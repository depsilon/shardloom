//! Bounded scalar COUNT(DISTINCT col) ingestion and exact partition union across
//! UTF-8, integer/float primitive, and boolean columns (including Dict, Constant,
//! nullable parent Structs, and pushed-down filters). The caller owns source progress;
//! workers never retain provider arrays in global state. An admitted failure aborts;
//! no partial cardinality is exposed.

use super::{
    AggregateDirectColumnAccessor, AggregateUtf8DictionarySource, AggregateValueTransform,
    SimpleAggregateStates, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveResourceEnvelope, VortexQueryPrimitiveRequest,
    aggregate_chunk_jobs::{AggregateChunkJobs, ChunkWorkerContext, SubmitOutcome},
    scalar_distinct_partitions::{NumericDistinctPartitions, Partitions, failed},
    utf8_chunk_dictionary::{Utf8ChunkDictionary, exact_row_vec},
    vortex_error,
};
use shardloom_core::Result;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{hash::Hasher as _, sync::Arc, time::Instant};
use vortex::array::{
    ArrayRef, VortexSessionExecute as _,
    arrays::{
        BoolArray, Dict, PrimitiveArray, Struct, VarBinViewArray, bool::BoolArrayExt,
        dict::DictArraySlotsExt as _, primitive::PrimitiveArrayExt, struct_::StructArrayExt as _,
        varbinview::VarBinViewArrayExt as _,
    },
    dtype::{DType, PType, half::f16},
    scalar::{PValue, ScalarValue},
};
use vortex::mask::Mask;
use vortex::session::VortexSession;

const NUMERIC_WORKER_BATCH_ROWS: usize = 16_384;

#[derive(Clone, Copy, PartialEq, Eq)]
enum TargetKind {
    Utf8,
    Primitive(PType),
    Bool,
}

impl TargetKind {
    const fn from_dtype(dtype: &DType) -> Option<Self> {
        match dtype {
            DType::Utf8(_) => Some(Self::Utf8),
            DType::Primitive(ptype, _) => Some(Self::Primitive(*ptype)),
            DType::Bool(_) => Some(Self::Bool),
            _ => None,
        }
    }

    fn matches_dtype(self, dtype: &DType) -> bool {
        match (self, dtype) {
            (Self::Utf8, DType::Utf8(_)) | (Self::Bool, DType::Bool(_)) => true,
            (Self::Primitive(expected), DType::Primitive(actual, _)) => expected == *actual,
            _ => false,
        }
    }

    const fn strategy(self) -> &'static str {
        match self {
            Self::Utf8 => "bounded_native_utf8_partition_exact",
            Self::Primitive(_) | Self::Bool => "bounded_native_numeric_partition_exact",
        }
    }
}

#[derive(Clone)]
enum PartitionBackend {
    Utf8(Arc<Partitions>),
    Numeric(Arc<NumericDistinctPartitions>),
}

#[derive(Default)]
struct Work {
    rows: u64,
    entries: u64,
    copied_bytes: u64,
    dictionary_chunks: u64,
    provider_nanos: u64,
    dictionary_nanos: u64,
    mark_nanos: u64,
    union_nanos: u64,
}

impl Work {
    fn add(&mut self, other: &Self) -> Result<()> {
        macro_rules! add {
            ($($field:ident),+ $(,)?) => { $(self.$field = self.$field.checked_add(other.$field)
                .ok_or_else(|| failed("work counter overflow"))?;)+ };
        }
        add!(
            rows,
            entries,
            copied_bytes,
            dictionary_chunks,
            provider_nanos,
            dictionary_nanos,
            mark_nanos,
            union_nanos
        );
        Ok(())
    }
}

enum Input {
    ConstantUtf8(Arc<str>),
    ConstantNumeric(u64),
    Utf8Canonical(VarBinViewArray),
    Utf8Dictionary {
        values: VarBinViewArray,
        codes: Vec<u32>,
        nulls: Option<Vec<bool>>,
    },
    NumericCanonical {
        values: PrimitiveArray,
        valid: Mask,
        struct_valid: Option<Mask>,
    },
    NumericDictionary {
        values: PrimitiveArray,
        valid: Mask,
        codes: Vec<u32>,
        nulls: Option<Vec<bool>>,
    },
    BoolCanonical {
        values: BoolArray,
        valid: Mask,
        struct_valid: Option<Mask>,
    },
    BoolDictionary {
        values: BoolArray,
        valid: Mask,
        codes: Vec<u32>,
        nulls: Option<Vec<bool>>,
    },
}

fn utf8_input_capacity_bytes(rows: usize, values_nbytes: u64, code_bytes: u64) -> Result<u64> {
    let scratch = Utf8ChunkDictionary::bounded_capacity_bytes(rows)?;
    let entries = rows
        .checked_mul(size_of::<(u64, usize)>() + size_of::<bool>())
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| failed("task scratch overflow"))?;
    scratch
        .checked_add(entries)
        .and_then(|n| n.checked_add(code_bytes))
        .and_then(|n| n.checked_add(values_nbytes))
        .ok_or_else(|| failed("task bytes overflow"))
}

fn dictionary_code_bytes(
    codes_capacity: usize,
    nulls_capacity: usize,
    extra_bytes: usize,
) -> Result<u64> {
    codes_capacity
        .checked_mul(size_of::<u32>())
        .and_then(|n| n.checked_add(nulls_capacity))
        .and_then(|n| n.checked_add(extra_bytes))
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| failed("code capacity overflow"))
}

impl Input {
    fn capacity_bytes(&self) -> Result<u64> {
        let base = match self {
            Self::ConstantUtf8(text) => size_of::<Self>()
                .checked_add(text.len())
                .and_then(|n| u64::try_from(n).ok())
                .ok_or_else(|| failed("task bytes overflow"))?,
            Self::ConstantNumeric(_) => u64::try_from(size_of::<Self>())
                .ok()
                .ok_or_else(|| failed("task bytes overflow"))?,
            Self::Utf8Canonical(values) => {
                utf8_input_capacity_bytes(values.len(), values.nbytes(), 0)?
            }
            Self::Utf8Dictionary {
                values,
                codes,
                nulls,
            } => {
                let code_bytes = dictionary_code_bytes(
                    codes.capacity(),
                    nulls.as_ref().map_or(0, Vec::capacity),
                    0,
                )?;
                utf8_input_capacity_bytes(values.len(), values.nbytes(), code_bytes)?
            }
            Self::NumericCanonical { values, .. } => {
                let batch_scratch = values
                    .len()
                    .min(NUMERIC_WORKER_BATCH_ROWS)
                    .checked_mul(size_of::<u64>())
                    .and_then(|n| u64::try_from(n).ok())
                    .ok_or_else(|| failed("task scratch overflow"))?;
                values
                    .nbytes()
                    .checked_add(batch_scratch)
                    .ok_or_else(|| failed("task bytes overflow"))?
            }
            Self::NumericDictionary {
                values,
                codes,
                nulls,
                ..
            } => {
                let mark_and_batch = values
                    .len()
                    .checked_mul(size_of::<bool>())
                    .and_then(|n| {
                        n.checked_add(
                            values
                                .len()
                                .min(NUMERIC_WORKER_BATCH_ROWS)
                                .checked_mul(size_of::<u64>())?,
                        )
                    })
                    .and_then(|n| u64::try_from(n).ok())
                    .ok_or_else(|| failed("task scratch overflow"))?;
                let code_bytes = dictionary_code_bytes(
                    codes.capacity(),
                    nulls.as_ref().map_or(0, Vec::capacity),
                    0,
                )?;
                values
                    .nbytes()
                    .checked_add(mark_and_batch)
                    .and_then(|n| n.checked_add(code_bytes))
                    .ok_or_else(|| failed("task bytes overflow"))?
            }
            Self::BoolCanonical { values, .. } => values
                .nbytes()
                .checked_add(16)
                .ok_or_else(|| failed("task bytes overflow"))?,
            Self::BoolDictionary {
                values,
                codes,
                nulls,
                ..
            } => {
                let code_bytes = dictionary_code_bytes(
                    codes.capacity(),
                    nulls.as_ref().map_or(0, Vec::capacity),
                    values.len().saturating_mul(size_of::<bool>()),
                )?;
                values
                    .nbytes()
                    .checked_add(code_bytes)
                    .ok_or_else(|| failed("task bytes overflow"))?
            }
        };
        base.checked_add(size_of::<Work>() as u64)
            .ok_or_else(|| failed("task bytes overflow"))
    }
}

pub(super) fn request_may_be_admitted(request: &VortexQueryPrimitiveRequest) -> bool {
    let Ok(aggregate) = super::required_simple_aggregate(request) else {
        return false;
    };
    if request.source_order_limit.is_some()
        || !aggregate.group_by.is_empty()
        || !aggregate.group_expressions.is_empty()
        || aggregate.spill.is_some()
    {
        return false;
    }
    let columns = aggregate
        .projected_columns()
        .iter()
        .map(|v| v.as_str().to_owned())
        .collect::<Vec<_>>();
    SimpleAggregateStates::new(aggregate, &columns).is_ok_and(|states| shape(&states, &columns))
}

pub(super) fn request_schema_may_be_admitted(
    request: &VortexQueryPrimitiveRequest,
    dtype: &DType,
    envelope: VortexLocalPrimitiveResourceEnvelope,
) -> bool {
    if !request_may_be_admitted(request) || envelope.max_parallelism < 2 {
        return false;
    }
    let Ok(aggregate) = super::required_simple_aggregate(request) else {
        return false;
    };
    let Ok(aggregate_plan) = super::rewrite_simple_aggregate_for_embedded_derived_columns(
        dtype,
        aggregate,
        request.predicate.as_ref(),
    ) else {
        return false;
    };
    let residual = aggregate_plan.predicate.as_ref().and_then(|predicate| {
        super::split_predicate_for_vortex_pushdown(predicate, request.kind).1
    });
    if residual.is_some() {
        return false;
    }
    let DType::Struct(fields, _) = dtype else {
        return false;
    };
    let projected = aggregate_plan.aggregate.projected_columns();
    if projected.len() != 1 {
        return false;
    }
    fields
        .field(projected[0].as_str())
        .is_some_and(|field_dtype| TargetKind::from_dtype(&field_dtype).is_some())
}

fn shape(states: &SimpleAggregateStates, columns: &[String]) -> bool {
    columns.len() == 1
        && states
            .single_count_distinct_alias_and_column()
            .is_some_and(|(_, index)| index == 0)
        && states.states[0].argument_offset.is_none()
        && matches!(
            states.states[0].value_transform,
            AggregateValueTransform::Identity
        )
        && states.states[0].distinct_values.is_empty()
        && !states.partition_distinct_completed
}

pub(super) struct ScalarDistinctWorkers {
    jobs: AggregateChunkJobs<Work>,
    partitions: Option<PartitionBackend>,
    target: TargetKind,
    column: String,
    session: VortexSession,
    parallelism: usize,
    work: Work,
    cardinality: Option<u64>,
    peak_reserved: u64,
    _metadata: MemoryLease,
}

impl ScalarDistinctWorkers {
    #[cfg(test)]
    pub(super) fn admit(
        states: &SimpleAggregateStates,
        dtype: &DType,
        columns: &[String],
        policy: VortexLocalPrimitiveExecutionPolicy,
        session: &VortexSession,
        memory: &LiveMemoryPool,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Option<Self>> {
        Self::admit_with_pool(
            states,
            dtype,
            columns,
            policy,
            session,
            memory,
            cancellation,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn admit_with_pool(
        states: &SimpleAggregateStates,
        dtype: &DType,
        columns: &[String],
        policy: VortexLocalPrimitiveExecutionPolicy,
        session: &VortexSession,
        memory: &LiveMemoryPool,
        cancellation: Option<&CancellationToken>,
        shared_pool: Option<&std::sync::Arc<shardloom_exec::compute_pool::ComputePool>>,
    ) -> Result<Option<Self>> {
        if !shape(states, columns) {
            return Ok(None);
        }
        let DType::Struct(fields, _) = dtype else {
            return Ok(None);
        };
        let Some(field_dtype) = fields.field(columns[0].as_str()) else {
            return Ok(None);
        };
        let Some(target) = TargetKind::from_dtype(&field_dtype) else {
            return Ok(None);
        };
        let parallelism = policy
            .resource_envelope
            .max_parallelism
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        if parallelism < 2 {
            return Ok(None);
        }
        let window = parallelism.saturating_mul(2).clamp(2, 24);
        let metadata_bytes = size_of::<Self>()
            .checked_add(window * 256)
            .and_then(|n| n.checked_add(columns[0].len()))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| failed("worker metadata overflow"))?;
        let Ok(metadata) = memory.reserve(metadata_bytes) else {
            return Ok(None);
        };
        let partitions = match target {
            TargetKind::Utf8 => {
                let Ok(parts) = Partitions::new(memory) else {
                    return Ok(None);
                };
                PartitionBackend::Utf8(Arc::new(parts))
            }
            TargetKind::Primitive(_) | TargetKind::Bool => {
                let Ok(parts) = NumericDistinctPartitions::new(memory) else {
                    return Ok(None);
                };
                PartitionBackend::Numeric(Arc::new(parts))
            }
        };
        Ok(Some(Self {
            jobs: AggregateChunkJobs::for_operation_with_pool(
                parallelism,
                window,
                memory.snapshot().limit_bytes,
                memory.clone(),
                cancellation,
                shared_pool,
            )?,
            partitions: Some(partitions),
            target,
            column: columns[0].clone(),
            session: session.clone(),
            parallelism,
            work: Work::default(),
            cardinality: None,
            peak_reserved: 0,
            _metadata: metadata,
        }))
    }

    pub(super) fn before_next(&mut self) -> Result<()> {
        self.jobs.check_cancelled()?;
        if self.jobs.is_full() {
            self.consume_next()?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    pub(super) fn submit(&mut self, chunk: &ArrayRef) -> Result<()> {
        self.jobs.check_cancelled()?;
        let started = Instant::now();
        if chunk.is_empty() {
            return Ok(());
        }
        let mut ctx = super::native_numeric_execution_ctx(&self.session);
        let (array, struct_valid) = if let Some(structure) = chunk.as_opt::<Struct>() {
            let struct_valid = match structure
                .struct_validity()
                .execute_mask(chunk.len(), &mut ctx)
                .map_err(vortex_error)?
            {
                Mask::AllTrue(_) => None,
                Mask::AllFalse(_) => {
                    self.work.rows = self
                        .work
                        .rows
                        .checked_add(chunk.len() as u64)
                        .ok_or_else(|| failed("work counter overflow"))?;
                    self.work.provider_nanos = self
                        .work
                        .provider_nanos
                        .checked_add(nanos(started))
                        .ok_or_else(|| failed("work counter overflow"))?;
                    return Ok(());
                }
                mask @ Mask::Values(_) => Some(mask),
            };
            let field = structure
                .unmasked_field_by_name(&self.column)
                .cloned()
                .map_err(vortex_error)?;
            let resolved =
                super::encoded_numeric_reduction::resolve_structural_projection(&field, &mut ctx)?;
            (resolved, struct_valid)
        } else {
            let field = super::logical_field_from_native_array(chunk, &self.column)?;
            let resolved =
                super::encoded_numeric_reduction::resolve_structural_projection(&field, &mut ctx)?;
            (resolved, None)
        };
        if !self.target.matches_dtype(array.dtype()) || array.len() != chunk.len() {
            return Err(failed("source changed admitted dtype or row count"));
        }

        // Fast path for Constant chunks: avoid materializing per-row vectors or scratch leases.
        let (input, preparation_lease) = if let Some(scalar) = array.as_constant() {
            let any_valid = struct_valid
                .as_ref()
                .is_none_or(|mask| mask.true_count() > 0);
            if !any_valid || scalar.is_null() {
                self.work.rows = self
                    .work
                    .rows
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| failed("work counter overflow"))?;
                self.work.provider_nanos = self
                    .work
                    .provider_nanos
                    .checked_add(nanos(started))
                    .ok_or_else(|| failed("work counter overflow"))?;
                return Ok(());
            }
            let input = match (self.target, scalar.value()) {
                (TargetKind::Utf8, Some(ScalarValue::Utf8(text))) => {
                    Input::ConstantUtf8(Arc::<str>::from(text.as_str()))
                }
                (TargetKind::Bool, Some(ScalarValue::Bool(value))) => {
                    Input::ConstantNumeric(u64::from(*value))
                }
                (TargetKind::Primitive(_), Some(ScalarValue::Primitive(pvalue))) => {
                    Input::ConstantNumeric(pvalue_key(*pvalue))
                }
                _ => {
                    return Err(failed(
                        "constant source scalar differed from admitted dtype",
                    ));
                }
            };
            (input, None)
        } else {
            // Bound code/remapping vectors while the caller prepares the native
            // owner. Keep this lease through task admission, including its overlap.
            let preparation_bytes = array
                .len()
                .checked_mul(32)
                .and_then(|n| n.checked_add(size_of::<Input>()))
                .and_then(|n| n.checked_add(self.column.len()))
                .and_then(|n| u64::try_from(n).ok())
                .ok_or_else(|| failed("preparation capacity overflow"))?;
            while self.jobs.outstanding() != 0 && self.available_bytes() < preparation_bytes {
                self.consume_next()?;
            }
            let lease = self.jobs.memory().reserve(preparation_bytes)?;
            let input = if let Some(dict) = array.as_opt::<Dict>()
                && let Some((mut codes, mut nulls)) =
                    super::direct_u32_codes_with_nulls_from_vortex_array(dict.codes(), &mut ctx)?
            {
                if let Some(struct_mask) = struct_valid.as_ref() {
                    let combined = nulls.get_or_insert_with(|| vec![false; codes.len()]);
                    if combined.len() != codes.len() {
                        return Err(failed("dictionary validity length mismatch"));
                    }
                    for (row, is_null) in combined.iter_mut().enumerate() {
                        if !struct_mask.value(row) {
                            *is_null = true;
                        }
                    }
                }
                let selected = super::dictionary_handoff::referenced_values(
                    dict.values(),
                    &mut codes,
                    nulls.as_deref(),
                )?;
                match self.target {
                    TargetKind::Utf8 => {
                        let values = selected
                            .execute::<VarBinViewArray>(&mut ctx)
                            .map_err(vortex_error)?;
                        Input::Utf8Dictionary {
                            values,
                            codes,
                            nulls,
                        }
                    }
                    TargetKind::Primitive(expected_ptype) => {
                        let values = selected
                            .execute::<PrimitiveArray>(&mut ctx)
                            .map_err(vortex_error)?;
                        if values.ptype() != expected_ptype {
                            return Err(failed("dictionary primitive values changed ptype"));
                        }
                        let valid = PrimitiveArrayExt::validity(&values)
                            .execute_mask(values.len(), &mut ctx)
                            .map_err(vortex_error)?;
                        Input::NumericDictionary {
                            values,
                            valid,
                            codes,
                            nulls,
                        }
                    }
                    TargetKind::Bool => {
                        let values = selected
                            .execute::<BoolArray>(&mut ctx)
                            .map_err(vortex_error)?;
                        let valid = BoolArrayExt::validity(&values)
                            .execute_mask(values.len(), &mut ctx)
                            .map_err(vortex_error)?;
                        Input::BoolDictionary {
                            values,
                            valid,
                            codes,
                            nulls,
                        }
                    }
                }
            } else {
                match self.target {
                    TargetKind::Utf8 => {
                        let values = if struct_valid.is_some() {
                            // Non-Dict nullable StructArray folds parent validity onto the UTF-8 array.
                            super::logical_field_from_native_array(chunk, &self.column)?
                                .execute::<VarBinViewArray>(&mut ctx)
                                .map_err(vortex_error)?
                        } else {
                            array
                                .clone()
                                .execute::<VarBinViewArray>(&mut ctx)
                                .map_err(vortex_error)?
                        };
                        if values.len() != chunk.len() {
                            return Err(failed("canonical source changed row count"));
                        }
                        Input::Utf8Canonical(values)
                    }
                    TargetKind::Primitive(expected_ptype) => {
                        let values = array
                            .clone()
                            .execute::<PrimitiveArray>(&mut ctx)
                            .map_err(vortex_error)?;
                        if values.len() != chunk.len() || values.ptype() != expected_ptype {
                            return Err(failed("canonical source changed row count or ptype"));
                        }
                        let valid = PrimitiveArrayExt::validity(&values)
                            .execute_mask(values.len(), &mut ctx)
                            .map_err(vortex_error)?;
                        Input::NumericCanonical {
                            values,
                            valid,
                            struct_valid,
                        }
                    }
                    TargetKind::Bool => {
                        let values = array
                            .clone()
                            .execute::<BoolArray>(&mut ctx)
                            .map_err(vortex_error)?;
                        if values.len() != chunk.len() {
                            return Err(failed("canonical source changed row count"));
                        }
                        let valid = BoolArrayExt::validity(&values)
                            .execute_mask(values.len(), &mut ctx)
                            .map_err(vortex_error)?;
                        Input::BoolCanonical {
                            values,
                            valid,
                            struct_valid,
                        }
                    }
                }
            };
            (input, Some(lease))
        };

        let work = Work {
            rows: chunk.len() as u64,
            provider_nanos: nanos(started),
            ..Work::default()
        };
        let bytes = input
            .capacity_bytes()?
            .checked_add(self.column.len() as u64)
            .ok_or_else(|| failed("task column metadata overflow"))?;
        // Completed receipts still own their task leases. Release them before
        // admitting another task, never hold a partition lock while joining.
        while self.jobs.outstanding() != 0 && self.available_bytes() < bytes {
            self.consume_next()?;
        }
        let partitions = self
            .partitions
            .as_ref()
            .ok_or_else(|| failed("union already finalized"))?
            .clone();
        let column = self.column.clone();
        let session = self.session.clone();
        let rotate_seed = usize::try_from(self.jobs.submitted()).unwrap_or(0);
        match self.jobs.try_submit(bytes, move |worker, _lease| {
            process(
                input,
                &column,
                &session,
                &partitions,
                worker,
                work,
                rotate_seed,
            )
        })? {
            SubmitOutcome::Submitted(_) => {}
            SubmitOutcome::InitialCapacityDenied(error) => return Err(error),
        }
        drop(preparation_lease);
        self.peak_reserved = self
            .peak_reserved
            .max(self.jobs.memory().snapshot().peak_reserved_bytes);
        Ok(())
    }

    fn consume_next(&mut self) -> Result<()> {
        let Some(done) = self.jobs.join_next()? else {
            return Err(failed("missing completed chunk"));
        };
        done.consume(|work| self.work.add(work))
    }

    fn available_bytes(&self) -> u64 {
        let snapshot = self.jobs.memory().snapshot();
        snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes)
    }

    pub(super) fn finish(&mut self, states: &mut SimpleAggregateStates) -> Result<()> {
        while self.jobs.outstanding() != 0 {
            self.consume_next()?;
        }
        self.jobs.retire()?;
        if !shape(states, std::slice::from_ref(&self.column)) {
            return Err(failed("scalar state changed before finalization"));
        }
        let parts = self
            .partitions
            .take()
            .ok_or_else(|| failed("union already finalized"))?;
        let cardinality = match &parts {
            PartitionBackend::Utf8(parts) => parts.cardinality()?,
            PartitionBackend::Numeric(parts) => parts.cardinality()?,
        };
        self.peak_reserved = self
            .peak_reserved
            .max(self.jobs.memory().snapshot().peak_reserved_bytes);
        drop(parts);
        states.states[0].count = cardinality;
        states.partition_distinct_completed = true;
        states.partition_distinct_strategy = Some(self.target.strategy());
        states.direct_scalar_updates = true;
        states.direct_distinct_updates = true;
        self.cardinality = Some(cardinality);
        Ok(())
    }

    pub(super) fn annotate_summary(&self, summary: &mut String) -> Result<()> {
        let mut value: serde_json::Value = serde_json::from_str(summary).map_err(vortex_error)?;
        value["aggregate_scalar_distinct_workers"] = serde_json::json!({
            "strategy": self.target.strategy(), "chunks": self.jobs.joined(),
            "rows": self.work.rows, "partial_entries": self.work.entries,
            "cardinality": self.cardinality, "copied_persistent_bytes": self.work.copied_bytes,
            "copy_scope": "copied_persistent_bytes_counts_unique_key_bytes_on_insert;arena_resize_copies_excluded",
            "native_dictionary_chunks": self.work.dictionary_chunks,
            "provider_nanos": self.work.provider_nanos, "dictionary_nanos": self.work.dictionary_nanos,
            "used_code_mark_nanos": self.work.mark_nanos, "union_nanos": self.work.union_nanos,
            "parallelism_including_caller": self.parallelism, "peak_outstanding": self.jobs.peak_outstanding(),
            "worker_busy_nanos": self.jobs.worker_busy_nanos(), "join_wait_nanos": self.jobs.join_wait_nanos(),
            "shared_pool_peak_reserved_bytes": self.peak_reserved,
            "memory_scope": "owned_native_buffers_and_reserved_task_partition_capacity;provider_allocations_outside_HostAllocator_excluded;not_RSS",
            "cpu_scope": "one_source_caller_plus_admitted_compute_workers;no_provider_background_pool",
            "timing_scope": "provider_nanos_is_caller_preparation_including_admission;dictionary_mark_union_are_summed_worker_elapsed_and_can_overlap_caller_and_each_other;not_CPU_or_additive_query_wall_time",
        });
        *summary = value.to_string();
        Ok(())
    }
}

#[allow(clippy::too_many_lines)]
fn process(
    input: Input,
    column: &str,
    session: &VortexSession,
    parts: &PartitionBackend,
    worker: &ChunkWorkerContext,
    mut work: Work,
    rotate_seed: usize,
) -> Result<Work> {
    worker.check_cancelled()?;
    match (input, parts) {
        (Input::ConstantUtf8(text), PartitionBackend::Utf8(parts)) => {
            let mut entries = vec![(hash(text.as_bytes()), 0_usize)];
            work.entries = 1;
            let started = Instant::now();
            work.copied_bytes = parts.union(&mut entries, |_| text.as_bytes(), worker)?;
            work.union_nanos = nanos(started);
        }
        (Input::ConstantNumeric(key), PartitionBackend::Numeric(parts)) => {
            let mut keys = [key];
            work.entries = 1;
            let started = Instant::now();
            parts.insert_in_place(&mut keys, worker, rotate_seed)?;
            work.union_nanos = nanos(started);
        }
        (Input::Utf8Canonical(values), PartitionBackend::Utf8(parts)) => {
            let started = Instant::now();
            let mut dictionary_work = super::native_numeric_accessor::Utf8AccessorWork::default();
            let dictionary = Utf8ChunkDictionary::with_row_capacity(values.len())?;
            let accessor = super::aggregate_utf8_accessor_from_canonical(
                column,
                &values,
                AggregateUtf8DictionarySource::HostUtf8ChunkDictionary,
                &mut dictionary_work,
                dictionary,
                &|| worker.check_cancelled(),
                &mut session.create_execution_ctx(),
            )?;
            work.dictionary_nanos = nanos(started);
            let AggregateDirectColumnAccessor::Utf8Dictionary { values, .. } = accessor else {
                return Err(failed("chunk dictionary missing"));
            };
            // Constructed dictionaries contain only referenced nonnull values;
            // no second row/code pass or per-key occurrence count is needed.
            let mut entries =
                hashed_entries(values.len(), |index| values[index].as_bytes(), worker)?;
            work.entries = entries.len() as u64;
            let started = Instant::now();
            work.copied_bytes =
                parts.union(&mut entries, |index| values[index].as_bytes(), worker)?;
            work.union_nanos = nanos(started);
        }
        (
            Input::Utf8Dictionary {
                values,
                codes,
                nulls,
            },
            PartitionBackend::Utf8(parts),
        ) => {
            work.dictionary_chunks = 1;
            let started = Instant::now();
            let mut ctx = super::native_numeric_execution_ctx(session);
            let valid = values
                .varbinview_validity()
                .execute_mask(values.len(), &mut ctx)
                .map_err(vortex_error)?;
            let used = mark_used_dictionary_codes(&codes, nulls.as_deref(), &valid, worker)?;
            work.mark_nanos = nanos(started);
            let mut entries = exact_row_vec(values.len())?;
            for (index, used) in used.into_iter().enumerate() {
                if index.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                if used {
                    let text = values.utf8_slice_at(index).map_err(|error| {
                        failed(&format!("used dictionary value has invalid UTF8: {error}"))
                    })?;
                    entries.push((hash(text.as_bytes()), index));
                }
            }
            work.entries = entries.len() as u64;
            let started = Instant::now();
            work.copied_bytes = parts.union(
                &mut entries,
                |index| super::native_utf8::borrowed_bytes(&values, index),
                worker,
            )?;
            work.union_nanos = nanos(started);
        }
        (
            Input::NumericCanonical {
                values,
                valid,
                struct_valid,
            },
            PartitionBackend::Numeric(parts),
        ) => {
            if valid.len() != values.len()
                || struct_valid
                    .as_ref()
                    .is_some_and(|m| m.len() != values.len())
            {
                return Err(failed("numeric validity length mismatch"));
            }
            let total_rows = values.len();
            let mut keys = Vec::with_capacity(total_rows.min(NUMERIC_WORKER_BATCH_ROWS));
            let mut offset = 0_usize;
            while offset < total_rows {
                let end = (offset + NUMERIC_WORKER_BATCH_ROWS).min(total_rows);
                keys.clear();
                primitive_range_to_keys(
                    &values,
                    &valid,
                    struct_valid.as_ref(),
                    offset..end,
                    &mut keys,
                    worker,
                )?;
                work.entries = work
                    .entries
                    .checked_add(keys.len() as u64)
                    .ok_or_else(|| failed("work counter overflow"))?;
                let started = Instant::now();
                parts.insert_in_place(&mut keys, worker, rotate_seed)?;
                work.union_nanos = work.union_nanos.saturating_add(nanos(started));
                offset = end;
            }
        }
        (
            Input::NumericDictionary {
                values,
                valid,
                codes,
                nulls,
            },
            PartitionBackend::Numeric(parts),
        ) => {
            work.dictionary_chunks = 1;
            let started = Instant::now();
            let used = mark_used_dictionary_codes(&codes, nulls.as_deref(), &valid, worker)?;
            work.mark_nanos = nanos(started);
            let mut keys = Vec::with_capacity(used.len().min(NUMERIC_WORKER_BATCH_ROWS));
            for (index, is_used) in used.into_iter().enumerate() {
                if index.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                if !is_used {
                    continue;
                }
                keys.push(primitive_key_at(&values, index)?);
                if keys.len() == NUMERIC_WORKER_BATCH_ROWS {
                    work.entries = work
                        .entries
                        .checked_add(keys.len() as u64)
                        .ok_or_else(|| failed("work counter overflow"))?;
                    let started = Instant::now();
                    parts.insert_in_place(&mut keys, worker, rotate_seed)?;
                    work.union_nanos = work.union_nanos.saturating_add(nanos(started));
                    keys.clear();
                }
            }
            if !keys.is_empty() {
                work.entries = work
                    .entries
                    .checked_add(keys.len() as u64)
                    .ok_or_else(|| failed("work counter overflow"))?;
                let started = Instant::now();
                parts.insert_in_place(&mut keys, worker, rotate_seed)?;
                work.union_nanos = work.union_nanos.saturating_add(nanos(started));
            }
        }
        (
            Input::BoolCanonical {
                values,
                valid,
                struct_valid,
            },
            PartitionBackend::Numeric(parts),
        ) => {
            if valid.len() != values.len()
                || struct_valid
                    .as_ref()
                    .is_some_and(|m| m.len() != values.len())
            {
                return Err(failed("boolean validity length mismatch"));
            }
            let bits = values.bit_buffer_view();
            let mut saw_false = false;
            let mut saw_true = false;
            for row in 0..values.len() {
                if row.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                if !valid.value(row) || struct_valid.as_ref().is_some_and(|m| !m.value(row)) {
                    continue;
                }
                if bits.value(row) {
                    saw_true = true;
                } else {
                    saw_false = true;
                }
                if saw_false && saw_true {
                    break;
                }
            }
            let mut keys = Vec::with_capacity(2);
            if saw_false {
                keys.push(0);
            }
            if saw_true {
                keys.push(1);
            }
            work.entries = keys.len() as u64;
            let started = Instant::now();
            parts.insert_in_place(&mut keys, worker, rotate_seed)?;
            work.union_nanos = nanos(started);
        }
        (
            Input::BoolDictionary {
                values,
                valid,
                codes,
                nulls,
            },
            PartitionBackend::Numeric(parts),
        ) => {
            work.dictionary_chunks = 1;
            let started = Instant::now();
            let used = mark_used_dictionary_codes(&codes, nulls.as_deref(), &valid, worker)?;
            work.mark_nanos = nanos(started);
            let bits = values.bit_buffer_view();
            let mut saw_false = false;
            let mut saw_true = false;
            for (index, is_used) in used.into_iter().enumerate() {
                if index.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                if !is_used {
                    continue;
                }
                if bits.value(index) {
                    saw_true = true;
                } else {
                    saw_false = true;
                }
                if saw_false && saw_true {
                    break;
                }
            }
            let mut keys = Vec::with_capacity(2);
            if saw_false {
                keys.push(0);
            }
            if saw_true {
                keys.push(1);
            }
            work.entries = keys.len() as u64;
            let started = Instant::now();
            parts.insert_in_place(&mut keys, worker, rotate_seed)?;
            work.union_nanos = nanos(started);
        }
        _ => {
            return Err(failed(
                "partition backend did not match prepared chunk input",
            ));
        }
    }
    worker.check_cancelled()?;
    Ok(work)
}

fn mark_used_dictionary_codes(
    codes: &[u32],
    nulls: Option<&[bool]>,
    valid: &Mask,
    worker: &ChunkWorkerContext,
) -> Result<Vec<bool>> {
    let values_len = valid.len();
    if nulls.is_some_and(|v| v.len() != codes.len()) {
        return Err(failed("dictionary validity length mismatch"));
    }
    let mut used = exact_row_vec::<bool>(values_len)?;
    used.resize(values_len, false);
    for (row, &code) in codes.iter().enumerate() {
        if row.is_multiple_of(4096) {
            worker.check_cancelled()?;
        }
        if nulls.is_some_and(|v| v[row]) {
            continue;
        }
        let index = code as usize;
        let slot = used
            .get_mut(index)
            .ok_or_else(|| failed("dictionary code exceeds values"))?;
        if valid.value(index) {
            *slot = true;
        }
    }
    Ok(used)
}

#[inline]
const fn canonical_float_bits(bits: u64) -> u64 {
    if bits << 1 == 0 { 0 } else { bits }
}

fn pvalue_key(value: PValue) -> u64 {
    match value {
        PValue::U8(v) => u64::from(v),
        PValue::U16(v) => u64::from(v),
        PValue::U32(v) => u64::from(v),
        PValue::U64(v) => v,
        PValue::I8(v) => i64::from(v).cast_unsigned(),
        PValue::I16(v) => i64::from(v).cast_unsigned(),
        PValue::I32(v) => i64::from(v).cast_unsigned(),
        PValue::I64(v) => v.cast_unsigned(),
        PValue::F16(v) => canonical_float_bits(
            shardloom_core::float_policy::widen_f16_bits(v.to_bits()).to_bits(),
        ),
        PValue::F32(v) => {
            canonical_float_bits(shardloom_core::float_policy::widen_f32(v).to_bits())
        }
        PValue::F64(v) => canonical_float_bits(v.to_bits()),
    }
}

fn primitive_key_at(values: &PrimitiveArray, index: usize) -> Result<u64> {
    if index >= values.len() {
        return Err(failed("dictionary index exceeds primitive values"));
    }
    Ok(match values.ptype() {
        PType::U8 => u64::from(values.as_slice::<u8>()[index]),
        PType::U16 => u64::from(values.as_slice::<u16>()[index]),
        PType::U32 => u64::from(values.as_slice::<u32>()[index]),
        PType::U64 => values.as_slice::<u64>()[index],
        PType::I8 => i64::from(values.as_slice::<i8>()[index]).cast_unsigned(),
        PType::I16 => i64::from(values.as_slice::<i16>()[index]).cast_unsigned(),
        PType::I32 => i64::from(values.as_slice::<i32>()[index]).cast_unsigned(),
        PType::I64 => values.as_slice::<i64>()[index].cast_unsigned(),
        PType::F16 => canonical_float_bits(
            shardloom_core::float_policy::widen_f16_bits(values.as_slice::<f16>()[index].to_bits())
                .to_bits(),
        ),
        PType::F32 => canonical_float_bits(
            shardloom_core::float_policy::widen_f32(values.as_slice::<f32>()[index]).to_bits(),
        ),
        PType::F64 => canonical_float_bits(values.as_slice::<f64>()[index].to_bits()),
    })
}

fn primitive_range_to_keys(
    values: &PrimitiveArray,
    valid: &Mask,
    struct_valid: Option<&Mask>,
    range: std::ops::Range<usize>,
    out: &mut Vec<u64>,
    worker: &ChunkWorkerContext,
) -> Result<()> {
    let all_valid = valid.all_true() && struct_valid.is_none_or(Mask::all_true);
    macro_rules! push_slice {
        ($slice:expr, |$v:ident| $encode:expr) => {{
            let slice = &$slice[range.clone()];
            if all_valid {
                for (offset, &$v) in slice.iter().enumerate() {
                    if offset.is_multiple_of(4096) {
                        worker.check_cancelled()?;
                    }
                    out.push($encode);
                }
            } else {
                for (offset, &$v) in slice.iter().enumerate() {
                    if offset.is_multiple_of(4096) {
                        worker.check_cancelled()?;
                    }
                    let row = range.start + offset;
                    if valid.value(row) && struct_valid.is_none_or(|m| m.value(row)) {
                        out.push($encode);
                    }
                }
            }
        }};
    }
    match values.ptype() {
        PType::U8 => push_slice!(values.as_slice::<u8>(), |v| u64::from(v)),
        PType::U16 => push_slice!(values.as_slice::<u16>(), |v| u64::from(v)),
        PType::U32 => push_slice!(values.as_slice::<u32>(), |v| u64::from(v)),
        PType::U64 => push_slice!(values.as_slice::<u64>(), |v| v),
        PType::I8 => push_slice!(values.as_slice::<i8>(), |v| i64::from(v).cast_unsigned()),
        PType::I16 => push_slice!(values.as_slice::<i16>(), |v| i64::from(v).cast_unsigned()),
        PType::I32 => push_slice!(values.as_slice::<i32>(), |v| i64::from(v).cast_unsigned()),
        PType::I64 => push_slice!(values.as_slice::<i64>(), |v| v.cast_unsigned()),
        PType::F16 => push_slice!(values.as_slice::<f16>(), |v| {
            canonical_float_bits(
                shardloom_core::float_policy::widen_f16_bits(v.to_bits()).to_bits(),
            )
        }),
        PType::F32 => push_slice!(values.as_slice::<f32>(), |v| {
            canonical_float_bits(shardloom_core::float_policy::widen_f32(v).to_bits())
        }),
        PType::F64 => push_slice!(values.as_slice::<f64>(), |v| {
            canonical_float_bits(v.to_bits())
        }),
    }
    Ok(())
}

fn hashed_entries<'a>(
    len: usize,
    bytes: impl Fn(usize) -> &'a [u8],
    worker: &ChunkWorkerContext,
) -> Result<Vec<(u64, usize)>> {
    let mut entries = exact_row_vec(len)?;
    for index in 0..len {
        if index.is_multiple_of(4096) {
            worker.check_cancelled()?;
        }
        entries.push((hash(bytes(index)), index));
    }
    Ok(entries)
}

fn hash(bytes: &[u8]) -> u64 {
    let mut hasher = rustc_hash::FxHasher::default();
    hasher.write(bytes);
    hasher.finish()
}
fn nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "scalar_distinct_workers_tests.rs"]
mod tests;
