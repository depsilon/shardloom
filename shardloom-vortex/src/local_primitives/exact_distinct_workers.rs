//! Bounded complete-pair jobs on the existing caller-plus-worker CPU budget.
//! Full pair identity survives pressure handoff; only EOF produces final counts.

use super::super::{
    AggregateDistinctValue, AggregateGroupKey, AggregateIntegerKeyPart, AggregateValueTransform,
    GroupedAggregateState, GroupedAggregateStates, SimpleAggregateFunction,
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
    aggregate_chunk_jobs::{AggregateChunkJobs, SubmitOutcome},
    logical_field_from_native_array, reserve_hash_map_capacity, reserve_hash_set_capacity,
};
use super::{
    CountOutcome, Pair, PairPartial, count_admitted, failed,
    partitions::{Evidence, ExactDistinctPartitions, FinalGroupCounts, Receipt},
};
use shardloom_core::{Result, StatValue};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use vortex::{
    array::{
        ArrayRef, VortexSessionExecute as _,
        dtype::{DType, Nullability},
    },
    session::VortexSession,
};

enum Completed {
    Reduced(Receipt),
    Retry([ArrayRef; 2]),
    ExactPartial(PairPartial),
    Groups(Option<Arc<FinalGroupCounts>>),
}

#[derive(Clone, Copy)]
struct Roles {
    group: usize,
    value: usize,
}

pub(in super::super) struct ExactDistinctWorkers {
    jobs: AggregateChunkJobs<Completed>,
    partitions: Option<Arc<ExactDistinctPartitions>>,
    deferred: Vec<Arc<PairPartial>>,
    retries: Vec<[ArrayRef; 2]>,
    result: Option<ExactDistinctResult>,
    cancelled: AtomicBool,
    session: VortexSession,
    columns: [String; 2],
    group_limit: usize,
    retained_limit: usize,
    parallelism: usize,
    rows: u64,
    partial_pairs: u64,
    handoffs: u64,
    retry_jobs: u64,
    group_jobs: u64,
    native_execution_nanos: u128,
    count_nanos: u128,
    peak_partial_bytes: u64,
    partial_comparisons: u64,
    merge_nanos: u128,
    submit_nanos: u128,
    evidence: Option<Evidence>,
    _metadata: MemoryLease,
}

pub(in super::super) struct ExactDistinctResult {
    groups: Arc<FinalGroupCounts>,
}
impl ExactDistinctResult {
    /// All contributions met before global selection. Visit the best bounded
    /// prefix in native comparator order; rendering applies offset and limit.
    pub(in super::super) fn visit(
        &self,
        visit: impl FnMut(AggregateIntegerKeyPart, u64) -> Result<()>,
    ) -> Result<()> {
        self.groups.visit(visit)
    }
    pub(in super::super) fn group_count(&self) -> usize {
        self.groups.group_count
    }
    pub(in super::super) fn retained_count(&self) -> usize {
        self.groups.retained_count()
    }
    pub(in super::super) fn reserved_bytes(&self) -> u64 {
        self.groups.reserved_bytes()
    }

    pub(in super::super) fn result_summary(
        &self,
        states: &GroupedAggregateStates<'_>,
        limit: Option<usize>,
    ) -> Result<(usize, String)> {
        let limit = limit.ok_or_else(|| failed("final distinct counts require bounded output"))?;
        let group = states
            .group_columns
            .first()
            .ok_or_else(|| failed("final distinct group is absent"))?;
        let measure = states
            .state_template
            .states
            .first()
            .filter(|measure| measure.function == SimpleAggregateFunction::CountDistinct)
            .ok_or_else(|| failed("final distinct aggregate contract changed"))?;
        let mut rows = Vec::new();
        let mut ordinal = 0_usize;
        self.visit(|key, count| {
            if ordinal >= states.request.offset && rows.len() < limit {
                let mut row = serde_json::Map::new();
                row.insert(
                    group.name.clone(),
                    super::super::integer_key_json_value(key.bits, key.signed),
                );
                row.insert(measure.alias.clone(), count.into());
                rows.push(serde_json::Value::Object(row));
            }
            ordinal += 1;
            Ok(())
        })?;
        let row_count = rows.len();
        let payload = serde_json::json!({
            "rows": row_count, "group_by": group.name, "functions": states.state_template.functions_summary(),
            "aggregate_key_encoding_mode": "typed_complete_integer_pair_keys",
            "aggregate_update_strategy": "complete_integer_pair_partition_distinct",
            "expression_fusion_strategy": states.expression_fusion_strategy(),
            "expression_plan_fingerprint_status": states.expression_plan_fingerprint_status(),
            "aggregate_accessor_summary": states.aggregate_accessor_summary(),
            "aggregate_accessor_materialization_status": states.aggregate_accessor_materialization_status(),
            "aggregate_vortex_dictionary_accessor_columns": states.aggregate_vortex_dictionary_accessor_columns(),
            "aggregate_chunk_dictionary_accessor_columns": states.aggregate_chunk_dictionary_accessor_columns(),
            "aggregate_primitive_accessor_columns": states.aggregate_primitive_accessor_columns(),
            "aggregate_materialized_accessor_columns": states.aggregate_materialized_accessor_columns(),
            "aggregate_accessor_blockers": states.aggregate_accessor_blockers(),
            "distinct_state_strategy": "complete_pair_partition_dedup_then_complete_group_count",
            "group_output_strategy": "bounded_heap_after_complete_distinct_group_reduction",
            "candidate_groups": self.group_count(), "retained_candidate_groups": self.retained_count(),
            "exact_distinct_final_reserved_bytes": self.reserved_bytes(),
            "exact_distinct_final_reservation_scope": "retained_numeric_group_keys_counts_vector_and_owner_metadata;JSON_output_excluded",
            "compact_group_state_strategy": "finalized_exact_distinct_counts",
            "group_state_mode": "complete_pair_state_released_after_EOF_group_reduction",
            "group_key_storage": "owned_native_integer_pair_bits", "group_key_comparison_strategy": "typed_value_comparator",
            "source_order_key_retention": "ordered_route_source_order_keys_elided",
            "topk_retention_after_update": self.retained_count(), "evicted_or_spilled_group_count": 0,
            "materialized_group_value_count": row_count, "decoded_string_count": 0,
            "estimated_group_key_storage_bytes": states.estimated_group_key_storage_bytes(),
            "estimated_group_string_storage_bytes": 0,
            "uniqueness_proof_status": "complete_group_value_pairs_all_contributions_before_group_count_and_selection",
            "spill_state": "not_spilled", "offset": states.request.offset,
            "order_by": states.request.order_by.iter().map(crate::VortexAggregateOrderExpr::summary).collect::<Vec<_>>().join(","),
            "values": rows,
        });
        Ok((row_count, payload.to_string()))
    }
}

impl ExactDistinctWorkers {
    pub(in super::super) fn admit(
        states: &GroupedAggregateStates<'_>,
        dtype: &DType,
        columns: &[String],
        policy: VortexLocalPrimitiveExecutionPolicy,
        session: &VortexSession,
        memory: &LiveMemoryPool,
    ) -> Result<Option<Self>> {
        let Some(roles) = roles(states, dtype, columns) else {
            return Ok(None);
        };
        let retained_limit = states
            .request
            .offset
            .checked_add(states.result_limit.expect("roles checked limit"))
            .ok_or_else(|| failed("distinct output offset plus limit overflowed"))?;
        let parallelism = policy
            .resource_envelope
            .max_parallelism
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        let window = parallelism.saturating_mul(2).clamp(1, 24);
        let metadata = (window * (size_of::<Arc<PairPartial>>() + size_of::<[ArrayRef; 2]>()))
            .checked_add(size_of::<Self>())
            .and_then(|bytes| bytes.checked_add(columns[roles.group].len()))
            .and_then(|bytes| bytes.checked_add(columns[roles.value].len()))
            .ok_or_else(|| failed("worker metadata size overflowed"))?;
        let Ok(lease) = memory.reserve(metadata as u64) else {
            return Ok(None);
        };
        let group_limit = policy.resource_envelope.group_state_soft_item_budget;
        let Some(partitions) = ExactDistinctPartitions::try_new(memory, group_limit)? else {
            return Ok(None);
        };
        let mut deferred = Vec::new();
        let mut retries = Vec::new();
        deferred
            .try_reserve_exact(window)
            .map_err(|error| failed(&error.to_string()))?;
        retries
            .try_reserve_exact(window)
            .map_err(|error| failed(&error.to_string()))?;
        if deferred.capacity() > window || retries.capacity() > window {
            return Err(failed("deferred owner capacity exceeded its reservation"));
        }
        Ok(Some(Self {
            jobs: AggregateChunkJobs::new(
                parallelism,
                window,
                memory.snapshot().limit_bytes,
                memory.clone(),
            )?,
            partitions: Some(partitions),
            deferred,
            retries,
            result: None,
            cancelled: AtomicBool::new(false),
            session: session.clone(),
            columns: [columns[roles.group].clone(), columns[roles.value].clone()],
            group_limit,
            retained_limit,
            parallelism,
            rows: 0,
            partial_pairs: 0,
            handoffs: 0,
            retry_jobs: 0,
            group_jobs: 0,
            native_execution_nanos: 0,
            count_nanos: 0,
            peak_partial_bytes: 0,
            partial_comparisons: 0,
            merge_nanos: 0,
            submit_nanos: 0,
            evidence: None,
            _metadata: lease,
        }))
    }

    pub(in super::super) fn has_active_partitions(&self) -> bool {
        self.partitions.is_some()
    }
    #[cfg(test)]
    pub(in super::super) fn has_committed_groups(&self) -> bool {
        self.partitions.as_ref().is_some_and(|partitions| {
            partitions
                .evidence()
                .is_ok_and(|evidence| evidence.committed_rows != 0)
        })
    }
    pub(in super::super) fn cancel_for_source_replay(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(partitions) = &self.partitions {
            partitions.request_pressure();
        }
        self.jobs.cancel();
    }
    pub(in super::super) fn before_next(
        &mut self,
        states: &mut GroupedAggregateStates<'_>,
    ) -> Result<()> {
        self.ensure_running()?;
        if self
            .partitions
            .as_ref()
            .is_some_and(|partitions| partitions.pressured())
        {
            self.handoff(states)?;
        }
        if self.jobs.is_full() {
            self.merge_next(states)?;
        }
        Ok(())
    }
    pub(in super::super) fn submit(
        &mut self,
        chunk: &ArrayRef,
        states: &mut GroupedAggregateStates<'_>,
    ) -> Result<bool> {
        self.ensure_running()?;
        if self.partitions.is_none() {
            return Ok(false);
        }
        let started = Instant::now();
        let group = logical_field_from_native_array(chunk, &self.columns[0])?;
        let value = logical_field_from_native_array(chunk, &self.columns[1])?;
        let bytes = (size_of::<Completed>() + 2 * size_of::<ArrayRef>()) as u64;
        while self.jobs.outstanding() > 0 && self.available_bytes() < bytes {
            self.merge_next(states)?;
        }
        if self.available_bytes() < bytes
            || self
                .partitions
                .as_ref()
                .is_some_and(|partitions| partitions.pressured())
        {
            self.handoff(states)?;
            self.submit_nanos += started.elapsed().as_nanos();
            return Ok(false);
        }
        let partitions = Arc::clone(
            self.partitions
                .as_ref()
                .expect("active exact-pair partitions"),
        );
        let session = self.session.clone();
        let memory = self.jobs.memory().clone();
        let submitted = self.jobs.try_submit(bytes, move |worker, _lease| {
            let denied_before = memory.snapshot().denied_reservations;
            match count_admitted(
                &group,
                &value,
                session.create_execution_ctx(),
                worker,
                &memory,
            )? {
                CountOutcome::Counted(partial) => {
                    partitions.reduce(partial, worker).map(Completed::Reduced)
                }
                CountOutcome::RetryCapacity(error) => {
                    worker.check_cancelled()?;
                    if memory.snapshot().denied_reservations <= denied_before {
                        return Err(error);
                    }
                    partitions.request_pressure();
                    Ok(Completed::Retry([group, value]))
                }
            }
        })?;
        if let SubmitOutcome::InitialCapacityDenied(_) = submitted {
            self.handoff(states)?;
            self.submit_nanos += started.elapsed().as_nanos();
            return Ok(false);
        }
        self.submit_nanos += started.elapsed().as_nanos();
        Ok(true)
    }
    fn available_bytes(&self) -> u64 {
        let memory = self.jobs.memory().snapshot();
        memory.limit_bytes - memory.reserved_bytes
    }
    fn ensure_running(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(failed("worker family was cancelled"));
        }
        Ok(())
    }
    fn record_numeric_work(
        &self,
        states: &mut GroupedAggregateStates<'_>,
        rows: u64,
        work: &[super::NumericWork; 2],
    ) -> Result<()> {
        states
            .aggregate_accessor_summary
            .insert("native_complete_integer_pair_owned_distinct_partial".into());
        for (column, work) in self.columns.iter().zip(work) {
            if work.required_execution {
                states.native_numeric_accessor_work.record_native_owner(
                    column,
                    rows,
                    work.source_bytes,
                    work.canonical_bytes,
                    work.elapsed_nanos,
                )?;
            }
        }
        Ok(())
    }
    fn merge_next(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        let expected = self.jobs.joined();
        let Some(completed) = self.jobs.join_next()? else {
            return Ok(());
        };
        if completed.ordinal() != expected {
            self.jobs.cancel();
            return Err(failed("complete pair completion lost source order"));
        }
        let started = Instant::now();
        completed.consume(|completed| {
            match completed {
                Completed::Reduced(receipt) => {
                    self.record_numeric_work(states, receipt.source_rows, &receipt.numeric_work)?;
                    self.native_execution_nanos += receipt.native_execution_nanos;
                    self.count_nanos += receipt.count_nanos;
                    self.peak_partial_bytes = self.peak_partial_bytes.max(receipt.capacity_bytes);
                    self.partial_comparisons = self
                        .partial_comparisons
                        .checked_add(receipt.comparisons)
                        .ok_or_else(|| failed("partial comparison counter overflowed"))?;
                    self.rows = self
                        .rows
                        .checked_add(receipt.source_rows)
                        .ok_or_else(|| failed("source row count overflowed"))?;
                    self.partial_pairs = self
                        .partial_pairs
                        .checked_add(receipt.partial_pairs as u64)
                        .ok_or_else(|| failed("partial pair counter overflowed"))?;
                    if let Some(partial) = &receipt.deferred {
                        if self.deferred.len() == self.deferred.capacity() {
                            return Err(failed("deferred pair window exceeded admission"));
                        }
                        self.deferred.push(Arc::clone(partial));
                    }
                }
                Completed::Retry(arrays) => {
                    if self.retries.len() == self.retries.capacity() {
                        return Err(failed("retry pair window exceeded admission"));
                    }
                    self.retries.push(arrays.clone());
                }
                Completed::ExactPartial(partial) => {
                    self.record_numeric_work(states, partial.rows, &partial.numeric_work)?;
                    self.native_execution_nanos += partial.native_execution_nanos;
                    self.count_nanos += partial.count_nanos;
                    self.peak_partial_bytes = self.peak_partial_bytes.max(partial.capacity_bytes);
                    self.partial_comparisons = self
                        .partial_comparisons
                        .checked_add(partial.comparisons)
                        .ok_or_else(|| failed("retry comparison counter overflowed"))?;
                    partial.visit(|pair, weight| replay_pair(states, pair, weight))?;
                    self.rows = self
                        .rows
                        .checked_add(partial.rows)
                        .ok_or_else(|| failed("retry row count overflowed"))?;
                    self.partial_pairs = self
                        .partial_pairs
                        .checked_add(partial.len() as u64)
                        .ok_or_else(|| failed("retry pair counter overflowed"))?;
                }
                Completed::Groups(groups) => {
                    self.result = groups.as_ref().map(|groups| ExactDistinctResult {
                        groups: Arc::clone(groups),
                    });
                }
            }
            Ok(())
        })?;
        self.merge_nanos += started.elapsed().as_nanos();
        Ok(())
    }
    fn drain(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        while self.jobs.outstanding() != 0 {
            self.merge_next(states)?;
        }
        Ok(())
    }
    fn handoff(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        self.ensure_running()?;
        let Some(partitions) = self.partitions.as_ref().map(Arc::clone) else {
            return Ok(());
        };
        partitions.request_pressure();
        self.drain(states)?;
        let mut replayed = 0_u64;
        partitions.replay_and_release(|pair, weight| {
            replay_pair(states, pair, weight)?;
            replayed = replayed
                .checked_add(weight)
                .ok_or_else(|| failed("handoff pair weight overflowed"))?;
            Ok(())
        })?;
        self.evidence = Some(partitions.evidence()?);
        if replayed
            != self
                .evidence
                .as_ref()
                .expect("captured pair evidence")
                .committed_rows
        {
            return Err(failed("committed pair replay weight diverged"));
        }
        for partial in self.deferred.drain(..) {
            partial.visit(|pair, weight| {
                replay_pair(states, pair, weight)?;
                replayed = replayed
                    .checked_add(weight)
                    .ok_or_else(|| failed("suffix pair weight overflowed"))?;
                Ok(())
            })?;
        }
        if replayed != self.rows {
            return Err(failed(
                "complete pair handoff lost or duplicated input weight",
            ));
        }
        self.partitions = None;
        drop(partitions);
        self.handoffs += 1;
        while let Some([group, value]) = self.retries.pop() {
            let session = self.session.clone();
            let memory = self.jobs.memory().clone();
            let bytes = (size_of::<Completed>() + 2 * size_of::<ArrayRef>()) as u64;
            self.jobs.submit(bytes, move |worker, _lease| {
                match count_admitted(
                    &group,
                    &value,
                    session.create_execution_ctx(),
                    worker,
                    &memory,
                )? {
                    CountOutcome::Counted(partial) => Ok(Completed::ExactPartial(partial)),
                    CountOutcome::RetryCapacity(error) => Err(error),
                }
            })?;
            self.retry_jobs += 1;
            self.merge_next(states)?;
        }
        Ok(())
    }
    pub(in super::super) fn finish(
        &mut self,
        states: &mut GroupedAggregateStates<'_>,
    ) -> Result<()> {
        self.ensure_running()?;
        self.drain(states)?;
        if self
            .partitions
            .as_ref()
            .is_some_and(|partitions| partitions.pressured())
        {
            return self.handoff(states);
        }
        let Some(partitions) = self.partitions.as_ref().map(Arc::clone) else {
            return Ok(());
        };
        let owned = Arc::clone(&partitions);
        let limit = self.group_limit;
        let retained = self.retained_limit;
        let submitted =
            self.jobs
                .try_submit(size_of::<Completed>() as u64, move |worker, _lease| {
                    let groups = owned.finish_groups(limit, worker)?;
                    let selected = groups
                        .map(|groups| groups.select(retained, worker))
                        .transpose()?
                        .flatten();
                    Ok(Completed::Groups(selected.map(Arc::new)))
                })?;
        if let SubmitOutcome::InitialCapacityDenied(_) = submitted {
            drop(partitions);
            return self.handoff(states);
        }
        self.group_jobs += 1;
        self.merge_next(states)?;
        if self.result.is_none() {
            drop(partitions);
            return self.handoff(states);
        }
        let evidence = partitions.evidence()?;
        if evidence.committed_rows != self.rows {
            return Err(failed("EOF pair row accounting diverged"));
        }
        self.evidence = Some(evidence);
        self.partitions = None;
        Ok(())
    }
    pub(in super::super) fn take_exact_result(&mut self) -> Option<ExactDistinctResult> {
        self.result.take()
    }

    pub(in super::super) fn annotate_summary(&self, summary: &mut String) -> Result<()> {
        let mut payload: serde_json::Value =
            serde_json::from_str(summary).map_err(|error| failed(&error.to_string()))?;
        let object = payload
            .as_object_mut()
            .ok_or_else(|| failed("distinct summary is not an object"))?;
        let memory = self.jobs.memory().snapshot();
        for (key, value) in [
            ("rows", u128::from(self.rows)),
            ("partial_entries", u128::from(self.partial_pairs)),
            (
                "submitted_chunks",
                u128::from(self.jobs.submitted() - self.retry_jobs - self.group_jobs),
            ),
            (
                "completed_chunks",
                u128::from(self.jobs.joined() - self.retry_jobs - self.group_jobs),
            ),
            ("outstanding_chunks", self.jobs.outstanding() as u128),
            (
                "peak_outstanding_chunks",
                self.jobs.peak_outstanding() as u128,
            ),
            ("cpu_ceiling", self.parallelism as u128),
            (
                "compute_threads",
                self.jobs
                    .pool_snapshot()
                    .map_or(0, |pool| pool.workers_created) as u128,
            ),
            ("provider_background_workers", 0),
            (
                "worker_busy_elapsed_nanos",
                u128::from(self.jobs.worker_busy_nanos()),
            ),
            ("inline_busy_elapsed_nanos", self.jobs.inline_busy_nanos()),
            ("canonicalization_work_nanos", self.native_execution_nanos),
            ("count_work_nanos", self.count_nanos),
            ("equality_comparisons", u128::from(self.partial_comparisons)),
            ("caller_merge_nanos", self.merge_nanos),
            ("caller_join_wait_nanos", self.jobs.join_wait_nanos()),
            ("caller_submit_elapsed_nanos", self.submit_nanos),
            (
                "peak_partial_capacity_bytes",
                u128::from(self.peak_partial_bytes),
            ),
            (
                "shared_live_peak_bytes",
                u128::from(memory.peak_reserved_bytes),
            ),
            ("shared_live_limit_bytes", u128::from(memory.limit_bytes)),
            ("partition_native_handoffs", u128::from(self.handoffs)),
            ("partition_retry_jobs", u128::from(self.retry_jobs)),
            ("distinct_group_reduction_jobs", u128::from(self.group_jobs)),
        ] {
            object.insert(
                format!("aggregate_workers_{key}"),
                u64::try_from(value).unwrap_or(u64::MAX).into(),
            );
        }
        if let Some(evidence) = &self.evidence {
            for (key, value) in [
                ("complete_pairs", evidence.pairs as u64),
                ("committed_rows", evidence.committed_rows),
                ("partition_comparisons", evidence.comparisons),
                ("partition_lock_wait_nanos", evidence.lock_wait_nanos),
                ("partition_reconcile_nanos", evidence.reconcile_nanos),
                ("group_reduce_nanos", evidence.group_reduce_nanos),
                ("entry_credit_claims", evidence.entry_claims),
                ("entry_credit_returns", evidence.entry_returns),
            ] {
                object.insert(
                    format!("aggregate_workers_exact_distinct_{key}"),
                    value.into(),
                );
            }
        }
        object.insert("aggregate_workers_exact_distinct_scope".into(), "complete_nonnull_integer_group_value_pairs;all_eight_original_width_native_owners;full_bits_and_signedness_equality;all_pair_contributions_before_EOF_group_reduction;no_local_group_topk;typed_provider_or_owned_capacity_denial_retries_untouched_arrays_once_after_partition_release;pressure_replays_full_identities_into_existing_exact_sets;worker_elapsed_includes_failed_attempts_but_partial_counters_include_successful_attempts_only;provider_bypass_allocations_legacy_handoff_maps_output_maps_and_RSS_excluded".into());
        *summary = payload.to_string();
        Ok(())
    }
}

fn replay_pair(states: &mut GroupedAggregateStates<'_>, pair: Pair, weight: u64) -> Result<()> {
    let group = pair.group();
    let value = pair.value();
    let distinct = |key: AggregateIntegerKeyPart| {
        if key.signed {
            AggregateDistinctValue::Int64(i64::from_ne_bytes(key.bits.to_ne_bytes()))
        } else {
            AggregateDistinctValue::UInt64(key.bits)
        }
    };
    let key = AggregateGroupKey::Single(distinct(group));
    if !states.groups.contains_key(&key) {
        reserve_hash_map_capacity(&mut states.groups, 1, "complete pair handoff group")?;
        let group_value = if group.signed {
            StatValue::Int64(i64::from_ne_bytes(group.bits.to_ne_bytes()))
        } else {
            StatValue::UInt64(group.bits)
        };
        states.groups.insert(
            key.clone(),
            GroupedAggregateState::new_general(vec![group_value], states.state_template.clone()),
        );
    }
    let state = &mut states
        .groups
        .get_mut(&key)
        .expect("admitted group")
        .general_states_mut()?
        .states[0];
    let value = distinct(value);
    if !state.distinct_values.contains(&value) {
        reserve_hash_set_capacity(
            &mut state.distinct_values,
            1,
            "complete pair handoff distinct identity",
        )?;
        state.distinct_values.insert(value);
    }
    state.count = state
        .count
        .checked_add(weight)
        .ok_or_else(|| failed("handoff group row count overflowed"))?;
    states.general_direct_updates = true;
    states.general_direct_count_distinct_updates = true;
    Ok(())
}

fn roles(states: &GroupedAggregateStates<'_>, dtype: &DType, columns: &[String]) -> Option<Roles> {
    if dtype.is_nullable()
        || states.group_key_indices.len() != 1
        || states.group_columns.len() != 1
        || states.state_template.states.len() != 1
        || states.result_limit.is_none()
        || !states.request.having.is_empty()
        || !states.groups.is_empty()
        || !states.group_order.is_empty()
        || states
            .request
            .offset
            .checked_add(states.result_limit?)
            .is_none()
    {
        return None;
    }
    let group = states.group_columns.get(states.group_key_indices[0])?;
    let measure = &states.state_template.states[0];
    if !matches!(group.transform, AggregateValueTransform::Identity)
        || !group.extra_column_indices.is_empty()
        || measure.function != SimpleAggregateFunction::CountDistinct
        || !matches!(measure.value_transform, AggregateValueTransform::Identity)
        || measure.argument_offset.is_some()
        || !matches!(states.request.order_by.as_slice(), [order] if order.descending && order.column == measure.alias)
    {
        return None;
    }
    let value = measure.column_index?;
    let fields = dtype.as_struct_fields_opt()?;
    if [group.column_index, value].into_iter().any(|index| {
        !matches!(columns.get(index).and_then(|column| fields.field(column.as_str())), Some(DType::Primitive(ptype, Nullability::NonNullable)) if ptype.is_int())
    }) { return None; }
    Some(Roles {
        group: group.column_index,
        value,
    })
}

pub(in super::super) fn request_may_be_admitted(request: &VortexQueryPrimitiveRequest) -> bool {
    let Ok(aggregate) = super::super::required_simple_aggregate(request) else {
        return false;
    };
    if aggregate.group_by.len() != 1
        || !aggregate.group_expressions.is_empty()
        || aggregate.measures.len() != 1
        || !aggregate.having.is_empty()
        || request
            .source_order_limit
            .is_none_or(|limit| aggregate.offset.checked_add(limit).is_none())
    {
        return false;
    }
    let columns = aggregate
        .projected_columns()
        .iter()
        .map(|column| column.as_str().to_owned())
        .collect::<Vec<_>>();
    let Ok(states) = super::super::SimpleAggregateStates::new(aggregate, &columns) else {
        return false;
    };
    let measure = &states.states[0];
    measure.function == SimpleAggregateFunction::CountDistinct
        && measure.column_index.is_some()
        && matches!(measure.value_transform, AggregateValueTransform::Identity)
        && measure.argument_offset.is_none()
        && matches!(aggregate.order_by.as_slice(), [order] if order.descending && order.column == measure.alias)
}

pub(in super::super) fn request_schema_may_be_admitted(
    request: &VortexQueryPrimitiveRequest,
    dtype: &DType,
) -> bool {
    let Ok(aggregate) = super::super::required_simple_aggregate(request) else {
        return false;
    };
    let Ok(plan) = super::super::rewrite_simple_aggregate_for_embedded_derived_columns(
        dtype,
        aggregate,
        request.predicate.as_ref(),
    ) else {
        return false;
    };
    if plan.predicate.as_ref().is_some_and(|predicate| {
        super::super::split_predicate_for_vortex_pushdown(predicate, request.kind)
            .1
            .is_some()
    }) {
        return false;
    }
    let columns = plan
        .aggregate
        .projected_columns()
        .iter()
        .map(|column| column.as_str().to_owned())
        .collect::<Vec<_>>();
    let Ok(states) = GroupedAggregateStates::new_with_resource_envelope(
        &plan.aggregate,
        request.source_order_limit,
        &columns,
        false,
        false,
        super::super::VortexLocalPrimitiveResourceEnvelope::default_single_threaded(),
    ) else {
        return false;
    };
    roles(&states, dtype, &columns).is_some()
}

#[cfg(test)]
#[path = "exact_distinct_workers_tests.rs"]
mod tests;

#[cfg(all(test, feature = "vortex-write", unix))]
#[path = "exact_distinct_public_tests.rs"]
mod public_tests;
