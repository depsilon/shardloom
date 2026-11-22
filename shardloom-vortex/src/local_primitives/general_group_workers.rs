//! Parallel general grouped aggregation across nullable keys, nullable struct
//! roots, multi-measure aggregates, post-merge `HAVING`, 4+ group keys, and
//! unordered `GROUP BY ... LIMIT K` locked-key admission.

use super::{
    AggregateDistinctValue, AggregateGroupKey, AggregateGroupRuntimeColumn,
    AggregateValueTransform, GroupedAggregateState, GroupedAggregateStates,
    NativeNumericAccessorWork, SimpleAggregateFunction, SimpleAggregateState,
    SimpleAggregateStates, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveResourceEnvelope, VortexQueryPrimitiveRequest,
    aggregate_chunk_jobs::{AggregateChunkJobs, ChunkWorkerContext, SubmitOutcome},
    aggregate_direct_column_accessors_from_chunk, aggregate_group_column_stat_value_for_direct_row,
    native_numeric_execution_ctx, required_simple_aggregate,
    rewrite_simple_aggregate_for_embedded_derived_columns, split_predicate_for_vortex_pushdown,
    usize_to_u64, vortex_error,
};
use rustc_hash::{FxHashMap, FxHashSet};
use shardloom_core::{Result, ShardLoomError, StatValue};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{
    collections::hash_map::Entry,
    sync::{Arc, OnceLock},
    time::Instant,
};
use vortex::{
    array::{
        ArrayRef,
        arrays::{Struct, struct_::StructArrayExt as _},
        dtype::{DType, Nullability, PType},
    },
    mask::Mask,
    session::VortexSession,
};

const MAX_ROWS: usize = 262_144;

#[cfg(test)]
thread_local! {
    pub(super) static SUBMIT_TEST_PRESSURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn failed(message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native general group workers: {message}; no fallback execution was attempted"
    ))
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct OwnedGroupKey(AggregateGroupKey);

impl OwnedGroupKey {
    fn from_group_values(values: &[StatValue], key_indices: &[usize]) -> Self {
        Self(AggregateGroupKey::from_indices(values, key_indices))
    }

    fn estimated_bytes(&self) -> usize {
        self.0.estimated_storage_bytes()
    }
}

fn scalar_dtype_admitted(dtype: &DType) -> bool {
    matches!(
        dtype,
        DType::Primitive(..) | DType::Bool(_) | DType::Utf8(_) | DType::Null
    )
}

fn float_identity_additive_measure(
    function: SimpleAggregateFunction,
    transform: AggregateValueTransform,
    column_dtype: Option<&DType>,
) -> bool {
    matches!(
        function,
        SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg
    ) && matches!(transform, AggregateValueTransform::Identity)
        && column_dtype.is_some_and(|dtype| {
            matches!(
                dtype,
                DType::Primitive(PType::F16 | PType::F32 | PType::F64, _)
            )
        })
}

pub(super) fn request_may_be_admitted(
    request: &VortexQueryPrimitiveRequest,
    _envelope: VortexLocalPrimitiveResourceEnvelope,
) -> bool {
    let Ok(aggregate) = required_simple_aggregate(request) else {
        return false;
    };
    if aggregate.spill.is_some() {
        return false;
    }
    !aggregate.group_by.is_empty() || !aggregate.group_expressions.is_empty()
}

pub(super) fn request_schema_may_be_admitted(
    request: &VortexQueryPrimitiveRequest,
    dtype: &DType,
    envelope: VortexLocalPrimitiveResourceEnvelope,
) -> bool {
    if !request_may_be_admitted(request, envelope) {
        return false;
    }
    let Ok(aggregate) = required_simple_aggregate(request) else {
        return false;
    };
    let Ok(aggregate_plan) = rewrite_simple_aggregate_for_embedded_derived_columns(
        dtype,
        aggregate,
        request.predicate.as_ref(),
    ) else {
        return false;
    };
    let residual = aggregate_plan
        .predicate
        .as_ref()
        .and_then(|predicate| split_predicate_for_vortex_pushdown(predicate, request.kind).1);
    if residual.is_some() {
        return false;
    }
    let DType::Struct(fields, _) = dtype else {
        return false;
    };
    let aggregate = &aggregate_plan.aggregate;
    let projected = aggregate.projected_columns();
    let projected_names = projected
        .iter()
        .map(|column| column.as_str().to_owned())
        .collect::<Vec<_>>();
    for column in &projected_names {
        let Some(field_dtype) = fields.field(column.as_str()) else {
            return false;
        };
        if !scalar_dtype_admitted(&field_dtype) {
            return false;
        }
    }
    for expression in &aggregate.group_expressions {
        if AggregateValueTransform::from_expression(expression).is_err() {
            return false;
        }
    }
    if SimpleAggregateStates::new(aggregate, &projected_names).is_err() {
        return false;
    }
    for measure in &aggregate.measures {
        let Ok(function) = SimpleAggregateFunction::parse(&measure.function) else {
            return false;
        };
        let Ok(transform) =
            AggregateValueTransform::from_measure_transform(measure.value_transform.as_deref())
        else {
            return false;
        };
        let column_dtype = measure
            .column
            .as_ref()
            .and_then(|column| fields.field(column.as_str()));
        if float_identity_additive_measure(function, transform, column_dtype.as_ref()) {
            return false;
        }
    }
    true
}

struct Recipe {
    declared_columns: Vec<String>,
    group_columns: Vec<AggregateGroupRuntimeColumn>,
    group_key_indices: Vec<usize>,
    state_template: SimpleAggregateStates,
    locked_keys: Arc<OnceLock<FxHashSet<OwnedGroupKey>>>,
    session: VortexSession,
}

struct GroupPartial {
    key: OwnedGroupKey,
    group_values: Vec<StatValue>,
    states: SimpleAggregateStates,
}

struct ChunkPartial {
    rows: usize,
    groups: Vec<GroupPartial>,
    accessor_work: NativeNumericAccessorWork,
    skipped_for_locked_keys: bool,
    worker_nanos: u128,
}

impl Recipe {
    #[allow(clippy::too_many_lines)]
    fn process_chunk(
        &self,
        chunk: &ArrayRef,
        worker: &ChunkWorkerContext,
        lease: &mut MemoryLease,
    ) -> Result<ChunkPartial> {
        let started = Instant::now();
        worker.check_cancelled()?;
        let rows = chunk.len();
        if rows == 0 {
            return Ok(ChunkPartial {
                rows: 0,
                groups: Vec::new(),
                accessor_work: NativeNumericAccessorWork::default(),
                skipped_for_locked_keys: false,
                worker_nanos: started.elapsed().as_nanos(),
            });
        }
        let mut ctx = native_numeric_execution_ctx(&self.session);
        let struct_valid_mask = if let Some(structure) = chunk.as_opt::<Struct>() {
            match structure
                .struct_validity()
                .execute_mask(rows, &mut ctx)
                .map_err(vortex_error)?
            {
                Mask::AllTrue(_) => None,
                mask => Some(mask),
            }
        } else if chunk.dtype().nullability() == Nullability::Nullable {
            match chunk
                .validity()
                .map_err(vortex_error)?
                .execute_mask(rows, &mut ctx)
                .map_err(vortex_error)?
            {
                Mask::AllTrue(_) => None,
                mask => Some(mask),
            }
        } else {
            None
        };
        let accessors =
            aggregate_direct_column_accessors_from_chunk(chunk, &self.declared_columns, &mut ctx)?;
        let locked_keys = self.locked_keys.get();
        let mut index_by_key = FxHashMap::<OwnedGroupKey, usize>::default();
        let mut groups = Vec::<GroupPartial>::new();
        let mut skipped_for_locked_keys = false;
        let mut tracked_bytes = size_of::<ChunkPartial>()
            .saturating_add(usize::try_from(chunk.nbytes()).unwrap_or(usize::MAX));
        let base_group_bytes = group_entry_bytes(&self.group_columns, &self.state_template);

        for row_index in 0..rows {
            if row_index.is_multiple_of(4096) {
                worker.check_cancelled()?;
            }
            let row_valid = struct_valid_mask
                .as_ref()
                .is_none_or(|mask| mask.value(row_index));
            let group_values = if row_valid {
                let mut values = Vec::with_capacity(self.group_columns.len());
                for group_column in &self.group_columns {
                    values.push(aggregate_group_column_stat_value_for_direct_row(
                        group_column,
                        &accessors.values,
                        row_index,
                    )?);
                }
                values
            } else {
                vec![StatValue::Null; self.group_columns.len()]
            };
            let key = OwnedGroupKey::from_group_values(&group_values, &self.group_key_indices);
            if let Some(locked) = locked_keys
                && !locked.contains(&key)
            {
                skipped_for_locked_keys = true;
                continue;
            }
            let entry_index = match index_by_key.entry(key) {
                Entry::Occupied(entry) => *entry.get(),
                Entry::Vacant(entry) => {
                    let index = groups.len();
                    let inserted_key = entry.key().clone();
                    let extra_bytes = base_group_bytes
                        .saturating_add(inserted_key.estimated_bytes())
                        .saturating_add(stat_values_estimated_bytes(&group_values));
                    tracked_bytes = tracked_bytes.saturating_add(extra_bytes);
                    let needed =
                        u64::try_from(tracked_bytes).map_err(|_| failed("task size overflow"))?;
                    if needed > lease.bytes() {
                        lease.resize(needed)?;
                    }
                    entry.insert(index);
                    groups.push(GroupPartial {
                        key: inserted_key,
                        group_values,
                        states: self.state_template.clone(),
                    });
                    index
                }
            };
            let target = &mut groups[entry_index].states;
            if row_valid {
                target.update_general_from_accessors_row(&accessors.values, row_index)?;
            } else {
                update_null_struct_row_measures(target, row_index, rows)?;
            }
            if target.has_count_distinct() {
                // Keep distinct sets metered when high-cardinality distinct values grow inside a chunk.
                let distinct_growth = target
                    .states
                    .iter()
                    .map(|state| {
                        state
                            .distinct_values
                            .len()
                            .saturating_mul(size_of::<AggregateDistinctValue>() + 16)
                    })
                    .sum::<usize>();
                let with_distinct = u64::try_from(tracked_bytes.saturating_add(distinct_growth))
                    .map_err(|_| failed("distinct task size overflow"))?;
                if with_distinct > lease.bytes() {
                    lease.resize(with_distinct)?;
                }
            }
        }
        Ok(ChunkPartial {
            rows,
            groups,
            accessor_work: accessors.numeric_work,
            skipped_for_locked_keys,
            worker_nanos: started.elapsed().as_nanos(),
        })
    }
}

fn update_null_struct_row_measures(
    states: &mut SimpleAggregateStates,
    row_index: usize,
    chunk_rows: usize,
) -> Result<()> {
    for state in &mut states.states {
        if state.column_index.is_none() {
            state.update_count_star_direct_row(row_index, chunk_rows)?;
        } else {
            let transformed = state.value_transform.apply(&StatValue::Null)?;
            state.update_transformed_value(&transformed)?;
        }
    }
    Ok(())
}

fn stat_values_estimated_bytes(values: &[StatValue]) -> usize {
    values
        .iter()
        .map(|value| match value {
            StatValue::Utf8(text) => size_of::<StatValue>().saturating_add(text.len()),
            _ => size_of::<StatValue>(),
        })
        .sum()
}

fn group_entry_bytes(
    group_columns: &[AggregateGroupRuntimeColumn],
    state_template: &SimpleAggregateStates,
) -> usize {
    let alias_bytes = state_template
        .states
        .iter()
        .map(|state| state.alias.len())
        .sum::<usize>();
    size_of::<GroupPartial>()
        .saturating_add(size_of::<(OwnedGroupKey, usize)>())
        .saturating_add(group_columns.len().saturating_mul(size_of::<StatValue>()))
        .saturating_add(
            state_template
                .states
                .len()
                .saturating_mul(size_of::<SimpleAggregateState>())
                .saturating_add(alias_bytes),
        )
        .saturating_add(64)
}

fn initial_task_bytes(
    chunk: &ArrayRef,
    rows: usize,
    columns: usize,
    group_columns: &[AggregateGroupRuntimeColumn],
    state_template: &SimpleAggregateStates,
) -> Result<u64> {
    let per_group = group_entry_bytes(group_columns, state_template);
    let estimated_groups = rows.clamp(1, 1024);
    let bytes = estimated_groups
        .checked_mul(per_group)
        .and_then(|bytes| {
            bytes.checked_add(rows.saturating_mul(columns.saturating_mul(16).max(16)))
        })
        .and_then(|bytes| bytes.checked_add(usize::try_from(chunk.nbytes()).unwrap_or(usize::MAX)))
        .and_then(|bytes| bytes.checked_add(size_of::<ChunkPartial>() + 4096))
        .ok_or_else(|| failed("task reservation overflow"))?;
    u64::try_from(bytes).map_err(|_| failed("task reservation exceeds u64"))
}

pub(super) struct GeneralGroupWorkers {
    jobs: AggregateChunkJobs<ChunkPartial>,
    recipe: Arc<Recipe>,
    active: bool,
    merged_any: bool,
    source_order_limit: Option<usize>,
    parallelism: usize,
    rows: u64,
    merged_groups: u64,
    peak_task_bytes: u64,
    worker_nanos: u128,
    merge_nanos: u128,
    _metadata: MemoryLease,
}

impl GeneralGroupWorkers {
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) fn admit_with_pool(
        states: &GroupedAggregateStates<'_>,
        dtype: &DType,
        columns: &[String],
        policy: VortexLocalPrimitiveExecutionPolicy,
        session: &VortexSession,
        memory: &LiveMemoryPool,
        cancellation: Option<&CancellationToken>,
        shared_pool: Option<&Arc<shardloom_exec::compute_pool::ComputePool>>,
    ) -> Result<Option<Self>> {
        let parallelism = policy
            .resource_envelope
            .max_parallelism
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        if parallelism < 2 {
            return Ok(None);
        }
        if states.group_columns.is_empty()
            || states.request.spill.is_some()
            || states.numeric_pair_late_measure_enabled
            || states.string_count_topk_heavy_hitter_enabled
            || states.string_count_distinct_topk_heavy_hitter_enabled
            || states.numeric_utf8_topk_heavy_hitter_enabled
            || states.transformed_dictionary_dense_general_groups.is_some()
            || states.single_numeric_count_groups.is_some()
            || states.numeric_pair_compact_groups.is_some()
            || states.numeric_minute_string_count_groups.is_some()
            || states.state_template.partition_distinct_completed
        {
            return Ok(None);
        }
        let DType::Struct(fields, _) = dtype else {
            return Ok(None);
        };
        for column in columns {
            let Some(field_dtype) = fields.field(column.as_str()) else {
                return Ok(None);
            };
            if !scalar_dtype_admitted(&field_dtype) {
                return Ok(None);
            }
        }
        for state in &states.state_template.states {
            let column_dtype = state
                .column_index
                .and_then(|index| columns.get(index))
                .and_then(|column| fields.field(column.as_str()));
            if float_identity_additive_measure(
                state.function,
                state.value_transform,
                column_dtype.as_ref(),
            ) {
                return Ok(None);
            }
        }

        let window = parallelism.saturating_mul(2).clamp(2, 24);
        let state_bytes = group_entry_bytes(&states.group_columns, &states.state_template);
        let column_bytes = columns
            .iter()
            .try_fold(0usize, |total, col| {
                total
                    .checked_add(size_of::<String>())
                    .and_then(|n| n.checked_add(col.len()))
            })
            .ok_or_else(|| failed("worker column metadata overflow"))?;
        let metadata_bytes = size_of::<Self>()
            .checked_add(size_of::<Recipe>())
            .and_then(|n| n.checked_add(column_bytes))
            .and_then(|n| n.checked_add(state_bytes))
            .and_then(|n| n.checked_add(window.saturating_mul(256)))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| failed("worker metadata overflow"))?;
        let Ok(metadata) = memory.reserve(metadata_bytes) else {
            return Ok(None);
        };
        let locked_keys = Arc::new(OnceLock::new());
        let source_order_limit = states.source_order_group_admission_limit();
        if source_order_limit == Some(0) || states.source_order_group_admission_closed() {
            let _ = locked_keys.set(states.groups.keys().cloned().map(OwnedGroupKey).collect());
        }
        Ok(Some(Self {
            jobs: AggregateChunkJobs::for_operation_with_pool(
                parallelism,
                window,
                memory.snapshot().limit_bytes,
                memory.clone(),
                cancellation,
                shared_pool,
            )?,
            recipe: Arc::new(Recipe {
                declared_columns: columns.to_vec(),
                group_columns: states.group_columns.clone(),
                group_key_indices: states.group_key_indices.clone(),
                state_template: states.state_template.clone(),
                locked_keys,
                session: session.clone(),
            }),
            active: true,
            merged_any: false,
            source_order_limit,
            parallelism,
            rows: 0,
            merged_groups: 0,
            peak_task_bytes: 0,
            worker_nanos: 0,
            merge_nanos: 0,
            _metadata: metadata,
        }))
    }

    pub(super) fn before_next(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        self.jobs.check_cancelled()?;
        if self.jobs.is_full() {
            self.merge_next(states)?;
        }
        Ok(())
    }

    fn available_bytes(&self) -> u64 {
        let snapshot = self.jobs.memory().snapshot();
        snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes)
    }

    pub(super) fn submit(
        &mut self,
        chunk: &ArrayRef,
        states: &mut GroupedAggregateStates<'_>,
    ) -> Result<bool> {
        if !self.active {
            return Ok(false);
        }
        self.jobs.check_cancelled()?;
        if chunk.len() > MAX_ROWS {
            self.retire(states)?;
            return Ok(false);
        }
        let bytes = initial_task_bytes(
            chunk,
            chunk.len(),
            self.recipe.declared_columns.len(),
            &self.recipe.group_columns,
            &self.recipe.state_template,
        )?;
        #[cfg(test)]
        let bytes = if SUBMIT_TEST_PRESSURE.with(std::cell::Cell::take) {
            u64::MAX
        } else {
            bytes
        };
        while self.jobs.outstanding() != 0 && self.available_bytes() < bytes {
            self.merge_next(states)?;
        }
        if self.available_bytes() < bytes {
            self.retire(states)?;
            return Ok(false);
        }
        let recipe = Arc::clone(&self.recipe);
        let chunk = chunk.clone();
        let outcome = self.jobs.try_submit(bytes, move |worker, lease| {
            recipe.process_chunk(&chunk, worker, lease)
        })?;
        if let SubmitOutcome::InitialCapacityDenied(_) = outcome {
            self.retire(states)?;
            return Ok(false);
        }
        self.peak_task_bytes = self.peak_task_bytes.max(bytes);
        Ok(true)
    }

    fn merge_next(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        let Some(completed) = self.jobs.join_next()? else {
            return Ok(());
        };
        let started = Instant::now();
        let record_source_order = states.request.order_by.is_empty();
        let merged_any = &mut self.merged_any;
        let rows_total = &mut self.rows;
        let worker_nanos_total = &mut self.worker_nanos;
        let merged_groups_total = &mut self.merged_groups;
        let locked_keys = &self.recipe.locked_keys;
        let has_limit = self.source_order_limit.is_some();
        completed.consume_owned(|partial| {
            if !*merged_any {
                states.begin_general_worker_merge();
                *merged_any = true;
            }
            *rows_total = rows_total
                .checked_add(usize_to_u64(partial.rows)?)
                .ok_or_else(|| failed("worker row count overflow"))?;
            *worker_nanos_total = worker_nanos_total.saturating_add(partial.worker_nanos);
            states
                .native_numeric_accessor_work
                .add(&partial.accessor_work)?;
            if partial.skipped_for_locked_keys {
                states.source_order_limited_group_admission = true;
            }
            let partial_groups = partial.groups.len();
            states.groups.try_reserve(partial_groups).map_err(|error| {
                failed(format!("global group state reservation failed: {error}"))
            })?;
            if record_source_order {
                states
                    .group_order
                    .try_reserve(partial_groups)
                    .map_err(|error| failed(format!("group order reservation failed: {error}")))?;
            }
            for group in partial.groups {
                if states.source_order_group_admission_closed() {
                    if has_limit {
                        let _ = locked_keys.get_or_init(|| {
                            states.groups.keys().cloned().map(OwnedGroupKey).collect()
                        });
                    }
                    states.source_order_limited_group_admission = true;
                    if let Some(existing) = states.groups.get_mut(&group.key.0) {
                        existing
                            .general_states_mut()?
                            .merge_preaggregated_from(&group.states)?;
                    }
                    continue;
                }
                match states.groups.entry(group.key.0) {
                    Entry::Occupied(entry) => {
                        entry
                            .into_mut()
                            .general_states_mut()?
                            .merge_preaggregated_from(&group.states)?;
                    }
                    Entry::Vacant(entry) => {
                        if record_source_order {
                            states.group_order.push(entry.key().clone());
                        }
                        let mut target = GroupedAggregateState::new_general(
                            group.group_values,
                            states.state_template.clone(),
                        );
                        target
                            .general_states_mut()?
                            .merge_preaggregated_from(&group.states)?;
                        entry.insert(target);
                        *merged_groups_total = merged_groups_total.saturating_add(1);
                    }
                }
            }
            if has_limit && states.source_order_group_admission_closed() {
                let _ = locked_keys
                    .get_or_init(|| states.groups.keys().cloned().map(OwnedGroupKey).collect());
            }
            Ok(())
        })?;
        self.merge_nanos = self
            .merge_nanos
            .saturating_add(started.elapsed().as_nanos());
        Ok(())
    }

    pub(super) fn finish(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        while self.jobs.outstanding() != 0 {
            self.merge_next(states)?;
        }
        self.jobs.check_cancelled()
    }

    fn retire(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        self.finish(states)?;
        self.jobs.retire()?;
        self.active = false;
        Ok(())
    }

    pub(super) fn retired(&self) -> bool {
        !self.active
    }

    pub(super) fn cancel(&self) {
        self.jobs.cancel();
    }

    #[cfg(test)]
    pub(super) fn has_committed_groups(&self) -> bool {
        self.merged_groups != 0
    }

    pub(super) fn annotate_summary(&self, summary: &mut String) -> Result<()> {
        let mut value: serde_json::Value = serde_json::from_str(summary).map_err(failed)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| failed("summary is not an object"))?;
        object.insert(
            "aggregate_workers_family".into(),
            "general_group_workers".into(),
        );
        object.insert(
            "aggregate_workers_submitted_chunks".into(),
            self.jobs.submitted().into(),
        );
        object.insert(
            "aggregate_workers_completed_chunks".into(),
            self.jobs.joined().into(),
        );
        object.insert(
            "aggregate_workers_outstanding_chunks".into(),
            self.jobs.outstanding().into(),
        );
        object.insert("aggregate_workers_rows".into(), self.rows.into());
        object.insert("aggregate_workers_groups".into(), self.merged_groups.into());
        object.insert("aggregate_workers_retired".into(), (!self.active).into());
        object.insert(
            "aggregate_workers_provider_background_workers".into(),
            0.into(),
        );
        object.insert(
            "aggregate_general_group_workers".into(),
            serde_json::json!({
                "rows": self.rows,
                "submitted_chunks": self.jobs.submitted(),
                "completed_chunks": self.jobs.joined(),
                "peak_outstanding_chunks": self.jobs.peak_outstanding(),
                "cpu_ceiling": self.parallelism,
                "merged_groups": self.merged_groups,
                "retired_to_same_serial_consumer": !self.active,
                "locked_key_filter_activated": self.recipe.locked_keys.get().is_some(),
                "peak_task_reservation_bytes": self.peak_task_bytes,
                "worker_nanos": u64::try_from(self.worker_nanos).unwrap_or(u64::MAX),
                "ordered_merge_caller_nanos": u64::try_from(self.merge_nanos).unwrap_or(u64::MAX),
                "join_wait_caller_nanos": u64::try_from(self.jobs.join_wait_nanos()).unwrap_or(u64::MAX),
            }),
        );
        *summary = value.to_string();
        Ok(())
    }
}
