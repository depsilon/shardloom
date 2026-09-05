//! Actual source-chunk canonicalization and exact count work. Global state stays
//! caller-owned; its ordered weighted merge is measured separately. Every key
//! survives each partial, including candidates outside every chunk's top K.

use super::{
    AggregateSingleNumericKey, AggregateValueTransform, GroupedAggregateStates,
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
    aggregate_chunk_jobs::{AggregateChunkJobs, ChunkWorkerContext},
    numeric_count_partial::{self, OwnedNumericCounts},
    string_count_partial::{self, StringCountMerge, StringCountPartial},
};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::time::Instant;
use vortex::{
    array::{
        ArrayRef, VortexSessionExecute as _,
        arrays::{Constant, Dict, PrimitiveArray},
        dtype::{DType, Nullability, PType},
        validity::Validity,
    },
    session::VortexSession,
};

#[derive(Clone, Copy)]
enum KeyKind {
    Utf8,
    I64,
    U64,
}

#[derive(Default)]
struct NumericWork {
    canonicalization_nanos: u128,
    count_nanos: u128,
    native_constant: bool,
}

enum Partial {
    String(StringCountPartial),
    Signed(OwnedNumericCounts<i64>, NumericWork),
    Unsigned(OwnedNumericCounts<u64>, NumericWork),
}

/// A source-shape precheck only. Schema and existing physical state gates below
/// still decide admission before any worker contributes to an aggregate.
pub(super) fn request_may_be_admitted(request: &VortexQueryPrimitiveRequest) -> bool {
    let Ok(aggregate) = super::required_simple_aggregate(request) else {
        return false;
    };
    let columns = aggregate
        .projected_columns()
        .iter()
        .map(|column| column.as_str().to_owned())
        .collect::<Vec<_>>();
    aggregate.group_by.len() == 1
        && super::aggregate_group_expressions_are_reconstructable_constants(aggregate)
        && super::SimpleAggregateStates::new(aggregate, &columns)
            .is_ok_and(|states| states.is_count_star_only())
}

pub(super) struct CountWorkers {
    jobs: AggregateChunkJobs<Partial>,
    kind: KeyKind,
    group_index: usize,
    column: String,
    preserve_order: bool,
    session: VortexSession,
    string_merge: StringCountMerge,
    rows: u64,
    entries: u64,
    canonicalization_nanos: u128,
    count_nanos: u128,
    merge_nanos: u128,
    submit_nanos: u128,
    hashed_bytes: u64,
    equality_comparisons: u64,
    dictionary_chunks: u64,
    dictionary_values: u64,
    constant_chunks: u64,
    peak_partial_bytes: u64,
    max_parallelism: usize,
}

impl CountWorkers {
    pub(super) fn admit(
        states: &GroupedAggregateStates<'_>,
        dtype: &DType,
        columns: &[String],
        policy: VortexLocalPrimitiveExecutionPolicy,
        session: &VortexSession,
        memory: &LiveMemoryPool,
    ) -> Result<Option<Self>> {
        if !super::aggregate_group_key_dtypes_nonnullable(dtype, states.request) {
            return Ok(None);
        }
        let [group_index] = states.group_key_indices.as_slice() else {
            return Ok(None);
        };
        let key = &states.group_columns[*group_index];
        let Some(column) = columns.get(key.column_index) else {
            return Ok(None);
        };
        let DType::Struct(fields, _) = dtype else {
            return Ok(None);
        };
        let Some(key_dtype) = fields.field(column.as_str()) else {
            return Ok(None);
        };
        let kind = match key_dtype {
            DType::Utf8(Nullability::NonNullable)
                if string_count_partial::admitted_group_index(states) == Some(*group_index) =>
            {
                KeyKind::Utf8
            }
            DType::Primitive(ptype @ (PType::I64 | PType::U64), Nullability::NonNullable)
                if numeric_state_admitted(states) =>
            {
                if ptype == PType::I64 {
                    KeyKind::I64
                } else {
                    KeyKind::U64
                }
            }
            _ => return Ok(None),
        };
        let max_parallelism = policy
            .resource_envelope
            .max_parallelism
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        let window = max_parallelism.saturating_mul(2).clamp(1, 24);
        Ok(Some(Self {
            jobs: AggregateChunkJobs::new(
                max_parallelism,
                window,
                memory.snapshot().limit_bytes,
                memory.clone(),
            )?,
            kind,
            group_index: *group_index,
            column: column.clone(),
            preserve_order: states.request.order_by.is_empty(),
            session: session.clone(),
            string_merge: StringCountMerge::default(),
            rows: 0,
            entries: 0,
            canonicalization_nanos: 0,
            count_nanos: 0,
            merge_nanos: 0,
            submit_nanos: 0,
            hashed_bytes: 0,
            equality_comparisons: 0,
            dictionary_chunks: 0,
            dictionary_values: 0,
            constant_chunks: 0,
            peak_partial_bytes: 0,
            max_parallelism,
        }))
    }

    /// Called before advancing the source: completed results consume window
    /// slots until their ordered merge releases both payload and capacity.
    pub(super) fn before_next(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        if self.jobs.is_full() {
            self.merge_next(states)?;
        }
        Ok(())
    }

    /// Numeric Dict keeps its existing native accessor path. Drain all prior
    /// partials before that path mutates the same global state.
    pub(super) fn submit(
        &mut self,
        chunk: &ArrayRef,
        states: &mut GroupedAggregateStates<'_>,
    ) -> Result<bool> {
        let started = Instant::now();
        let array = super::logical_field_from_native_array(chunk, &self.column)?;
        if !matches!(self.kind, KeyKind::Utf8) && array.as_opt::<Dict>().is_some() {
            self.drain(states)?;
            return Ok(false);
        }
        let partial_bytes = match self.kind {
            KeyKind::Utf8 => string_count_partial::partial_bytes(&array)?,
            KeyKind::I64 | KeyKind::U64 => numeric_count_partial::partial_bytes::<u64>(
                if array.as_opt::<Constant>().is_some() {
                    usize::from(!array.is_empty())
                } else {
                    array.len()
                },
            )?,
        };
        let initial_bytes = partial_bytes
            .checked_add(std::mem::size_of::<Partial>() as u64)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<ArrayRef>() as u64))
            .ok_or_else(|| failed("task capacity overflowed"))?;
        // Release earlier completed capacity before denying the next known
        // allocation. In-flight provider allocation can still deny explicitly.
        while self.jobs.outstanding() > 0 && {
            let snapshot = self.jobs.memory().snapshot();
            snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes) < initial_bytes
        } {
            self.merge_next(states)?;
        }
        let session = self.session.clone();
        let kind = self.kind;
        let preserve_order = self.preserve_order;
        self.jobs.submit(initial_bytes, move |worker, lease| {
            if matches!(kind, KeyKind::Utf8) {
                let mut partial = string_count_partial::count_string_chunk(
                    &array,
                    session.create_execution_ctx(),
                    worker,
                    lease,
                )?;
                if preserve_order {
                    partial.preserve_existing_key_order();
                }
                Ok(Partial::String(partial))
            } else {
                count_numeric_chunk(&array, &session, kind, worker, lease)
            }
        })?;
        self.submit_nanos += started.elapsed().as_nanos();
        Ok(true)
    }

    fn merge_next(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        let expected = self.jobs.joined();
        let Some(completed) = self.jobs.join_next()? else {
            return Ok(());
        };
        if completed.ordinal() != expected {
            self.jobs.cancel();
            return Err(failed("completed chunks lost source order"));
        }
        let started = Instant::now();
        completed.consume(|partial| {
            let (rows, entries, bytes) = match partial {
                Partial::String(partial) => {
                    self.string_merge.merge(states, self.group_index, partial)?;
                    self.canonicalization_nanos += partial.work.canonicalization_nanos;
                    self.count_nanos += partial.work.count_nanos;
                    self.hashed_bytes += partial.work.utf8_bytes_hashed;
                    self.equality_comparisons += partial.work.equality_comparisons;
                    self.dictionary_chunks += u64::from(partial.work.native_dictionary);
                    self.dictionary_values += partial.work.dictionary_values;
                    self.constant_chunks += u64::from(partial.work.native_constant);
                    (
                        partial.work.rows,
                        partial.work.partial_entries,
                        partial.work.partial_capacity_bytes,
                    )
                }
                Partial::Signed(partial, work) => {
                    merge_numeric(
                        states,
                        partial.pairs().iter().map(|&(key, count)| {
                            (
                                AggregateSingleNumericKey {
                                    bits: u64::from_ne_bytes(key.to_ne_bytes()),
                                    signed: true,
                                },
                                count,
                            )
                        }),
                    )?;
                    self.observe_numeric(work);
                    (
                        partial.rows(),
                        partial.pairs().len() as u64,
                        partial.reserved_bytes(),
                    )
                }
                Partial::Unsigned(partial, work) => {
                    merge_numeric(
                        states,
                        partial.pairs().iter().map(|&(key, count)| {
                            (
                                AggregateSingleNumericKey {
                                    bits: key,
                                    signed: false,
                                },
                                count,
                            )
                        }),
                    )?;
                    self.observe_numeric(work);
                    (
                        partial.rows(),
                        partial.pairs().len() as u64,
                        partial.reserved_bytes(),
                    )
                }
            };
            self.rows = self
                .rows
                .checked_add(rows)
                .ok_or_else(|| failed("row evidence overflowed"))?;
            self.entries = self
                .entries
                .checked_add(entries)
                .ok_or_else(|| failed("entry evidence overflowed"))?;
            self.peak_partial_bytes = self.peak_partial_bytes.max(bytes);
            Ok(())
        })?;
        self.merge_nanos += started.elapsed().as_nanos();
        Ok(())
    }

    fn observe_numeric(&mut self, work: &NumericWork) {
        self.canonicalization_nanos += work.canonicalization_nanos;
        self.count_nanos += work.count_nanos;
        self.constant_chunks += u64::from(work.native_constant);
    }

    pub(super) fn drain(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        while self.jobs.outstanding() > 0 {
            self.merge_next(states)?;
        }
        Ok(())
    }

    pub(super) fn annotate_summary(&self, summary: &mut String) -> Result<()> {
        let mut payload: serde_json::Value =
            serde_json::from_str(summary).map_err(|error| failed(&error.to_string()))?;
        let object = payload
            .as_object_mut()
            .ok_or_else(|| failed("summary must be an object"))?;
        let pool = self.jobs.pool_snapshot();
        let memory = self.jobs.memory().snapshot();
        for (name, value) in [
            ("rows", u128::from(self.rows)),
            ("partial_entries", u128::from(self.entries)),
            ("submitted_chunks", u128::from(self.jobs.submitted())),
            ("completed_chunks", u128::from(self.jobs.joined())),
            ("outstanding_chunks", self.jobs.outstanding() as u128),
            (
                "peak_outstanding_chunks",
                self.jobs.peak_outstanding() as u128,
            ),
            ("cpu_ceiling", self.max_parallelism as u128),
            (
                "compute_threads",
                pool.map_or(0, |p| p.workers_created) as u128,
            ),
            ("provider_background_workers", 0),
            (
                "peak_active_workers",
                pool.map_or(0, |p| p.peak_active_workers) as u128,
            ),
            (
                "worker_busy_elapsed_nanos",
                u128::from(self.jobs.worker_busy_nanos()),
            ),
            ("inline_busy_elapsed_nanos", self.jobs.inline_busy_nanos()),
            ("canonicalization_work_nanos", self.canonicalization_nanos),
            ("count_work_nanos", self.count_nanos),
            ("caller_merge_nanos", self.merge_nanos),
            ("caller_join_wait_nanos", self.jobs.join_wait_nanos()),
            ("caller_submit_elapsed_nanos", self.submit_nanos),
            ("utf8_bytes_hashed", u128::from(self.hashed_bytes)),
            (
                "equality_comparisons",
                u128::from(self.equality_comparisons),
            ),
            (
                "native_dictionary_chunks",
                u128::from(self.dictionary_chunks),
            ),
            (
                "native_dictionary_values",
                u128::from(self.dictionary_values),
            ),
            ("native_constant_chunks", u128::from(self.constant_chunks)),
            (
                "peak_partial_capacity_bytes",
                u128::from(self.peak_partial_bytes),
            ),
            (
                "shared_live_peak_bytes",
                u128::from(memory.peak_reserved_bytes),
            ),
            ("shared_live_limit_bytes", u128::from(memory.limit_bytes)),
            (
                "string_pressure_transitions",
                u128::from(self.string_merge.pressure_transitions),
            ),
            (
                "new_global_strings",
                u128::from(self.string_merge.new_global_strings),
            ),
            (
                "peak_estimated_global_string_bytes",
                u128::from(self.string_merge.peak_estimated_global_bytes),
            ),
            (
                "released_histogram_entries",
                u128::from(self.string_merge.released_histogram_entries),
            ),
            (
                "retained_interner_values_on_pressure",
                u128::from(self.string_merge.retained_interner_values_on_pressure),
            ),
        ] {
            object.insert(
                format!("aggregate_workers_{name}"),
                u64::try_from(value).unwrap_or(u64::MAX).into(),
            );
        }
        object.insert("aggregate_workers_scope".into(), "source_ordered_all_key_chunk_partials;parallel_canonicalization_and_exact_count;caller_global_merge;no_local_topk;sum_worker_elapsed_not_cpu_or_exclusive_wall;submit_includes_inline_work_and_memory_pressure_drains;shared_capacity_covers_native_allocator_and_owned_partials_not_legacy_global_maps_or_process_rss;source_generation_validated_after_drain_and_refinement".into());
        *summary = payload.to_string();
        Ok(())
    }
}

fn numeric_state_admitted(states: &GroupedAggregateStates<'_>) -> bool {
    let Ok(alias) = states.single_numeric_count_order_alias() else {
        return false;
    };
    states.result_limit.is_some()
        && states.request.having.is_empty()
        && states.state_template.is_count_star_only()
        && states
            .state_template
            .count_star_alias()
            .is_ok_and(|count_alias| count_alias == alias)
        && states.group_columns.len() == 1
        && states.groups.is_empty()
        && states.group_order.is_empty()
        && states.numeric_pair_compact_groups.is_none()
        && states.numeric_pair_late_measure_count_groups.is_none()
        && states.numeric_minute_string_count_groups.is_none()
        && states.group_columns.iter().all(|column| {
            column.extra_column_indices.is_empty()
                && matches!(column.transform, AggregateValueTransform::Identity)
        })
}

fn merge_numeric(
    states: &mut GroupedAggregateStates<'_>,
    pairs: impl ExactSizeIterator<Item = (AggregateSingleNumericKey, u64)>,
) -> Result<()> {
    if !numeric_state_admitted(states) {
        return Err(failed(
            "numeric global state lost its admitted count contract",
        ));
    }
    let groups = states.single_numeric_count_groups.get_or_insert_default();
    super::reserve_hash_map_capacity(groups, pairs.len(), "worker numeric exact global count")?;
    for (key, weight) in pairs {
        if weight == 0 {
            return Err(failed("numeric partial contains zero weight"));
        }
        let count = groups.entry(key).or_insert(0_u64);
        *count = count
            .checked_add(weight)
            .ok_or_else(|| failed("global COUNT(*) overflowed u64"))?;
    }
    states.count_star_direct_updates = true;
    states.single_numeric_count_direct_updates = true;
    states
        .aggregate_accessor_summary
        .insert("native_integer_owned_all_key_count_partial".into());
    Ok(())
}

fn count_numeric_chunk(
    array: &ArrayRef,
    session: &VortexSession,
    kind: KeyKind,
    worker: &ChunkWorkerContext,
    lease: &mut MemoryLease,
) -> Result<Partial> {
    worker.check_cancelled()?;
    let ptype = match kind {
        KeyKind::I64 => PType::I64,
        KeyKind::U64 => PType::U64,
        KeyKind::Utf8 => return Err(failed("string job entered numeric worker")),
    };
    if array.dtype() != &DType::Primitive(ptype, Nullability::NonNullable) {
        return Err(failed("numeric chunk lost its admitted nonnullable dtype"));
    }
    let rows = u64::try_from(array.len()).map_err(|_| failed("numeric row count overflowed"))?;
    let native_constant = array.as_opt::<Constant>().is_some();
    let started = Instant::now();
    let input = if native_constant {
        array
            .slice(0..usize::from(rows > 0))
            .map_err(super::vortex_error)?
    } else {
        array.clone()
    };
    let values = input
        .execute::<PrimitiveArray>(&mut session.create_execution_ctx())
        .map_err(super::vortex_error)?;
    let expected_values = if native_constant {
        usize::from(rows > 0)
    } else {
        array.len()
    };
    if values.len() != expected_values
        || values.ptype() != ptype
        || !matches!(
            vortex::array::arrays::primitive::PrimitiveArrayExt::validity(&values),
            Validity::NonNullable | Validity::AllValid
        )
    {
        return Err(failed(
            "numeric canonicalization changed length, dtype or validity",
        ));
    }
    let canonicalization_nanos = started.elapsed().as_nanos();
    let started = Instant::now();
    let mut work = NumericWork {
        canonicalization_nanos,
        native_constant,
        ..NumericWork::default()
    };
    match kind {
        KeyKind::I64 => {
            let slice = values.as_slice::<i64>();
            let counts = if native_constant && rows > 0 {
                numeric_count_partial::count_numeric_constant(
                    *slice.first().ok_or_else(|| failed("empty constant"))?,
                    rows,
                    worker,
                    lease,
                )?
            } else {
                numeric_count_partial::count_numeric_values(slice, worker, lease)?
            };
            work.count_nanos = started.elapsed().as_nanos();
            Ok(Partial::Signed(counts, work))
        }
        KeyKind::U64 => {
            let slice = values.as_slice::<u64>();
            let counts = if native_constant && rows > 0 {
                numeric_count_partial::count_numeric_constant(
                    *slice.first().ok_or_else(|| failed("empty constant"))?,
                    rows,
                    worker,
                    lease,
                )?
            } else {
                numeric_count_partial::count_numeric_values(slice, worker, lease)?
            };
            work.count_nanos = started.elapsed().as_nanos();
            Ok(Partial::Unsigned(counts, work))
        }
        KeyKind::Utf8 => unreachable!("validated numeric key kind"),
    }
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local Vortex aggregate workers {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "aggregate_count_workers_tests.rs"]
mod tests;
