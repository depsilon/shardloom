//! Bounded native compound-key COUNT jobs and exact pressure handoff. The same
//! retained file/session owns scanning, partial work, and any existing recount.

use super::{
    AggregateNumericUtf8InternedKey, AggregateValueTransform, GroupedAggregateStates,
    NumericUtf8GroupRoles, NumericUtf8TopKHeavyHitterSketch, VortexLocalPrimitiveExecutionPolicy,
    aggregate_chunk_jobs::{AggregateChunkJobs, SubmitOutcome},
    compound_count_partial::{self, CompoundPartial, CountOutcome, Key, failed},
    compound_count_partitions::{CompoundPartitions, Evidence, PARTITIONS, Receipt, Selection},
};
use shardloom_core::Result;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    sync::{Arc, atomic::Ordering},
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
    Count(Receipt),
    Retry([ArrayRef; 2]),
    Unpartitioned(CompoundPartial),
    Selected(Selection),
}
pub(super) struct CompoundWorkers {
    jobs: AggregateChunkJobs<Completed>,
    partitions: Option<Arc<CompoundPartitions>>,
    deferred: Vec<Arc<CompoundPartial>>,
    retries: Vec<[ArrayRef; 2]>,
    _handoff: MemoryLease,
    session: VortexSession,
    columns: [String; 2],
    roles: NumericUtf8GroupRoles,
    evidence: Option<Evidence>,
    rows: u64,
    partial_entries: u64,
    dictionary_chunks: u64,
    bytes_hashed: u64,
    comparisons: u64,
    canonicalization: u128,
    count: u128,
    merge: u128,
    submit_time: u128,
    selection_jobs: u64,
    retry_jobs: u64,
    handoffs: u64,
    peak_partial: u64,
    parallelism: usize,
    #[cfg(test)]
    deny_next_initial_reservation: bool,
}

impl CompoundWorkers {
    pub(super) fn admit(
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
        let Some(retained) = states
            .request
            .offset
            .checked_add(states.result_limit.unwrap_or(0))
            .filter(|cap| *cap > 0)
        else {
            return Ok(None);
        };
        let parallelism = policy
            .resource_envelope
            .max_parallelism
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        let window = parallelism.saturating_mul(2).clamp(1, 24);
        let Ok(handoff) = memory.reserve(
            (window * (size_of::<Arc<CompoundPartial>>() + size_of::<[ArrayRef; 2]>())
                + size_of::<Self>()) as u64,
        ) else {
            return Ok(None);
        };
        let Some(partitions) = CompoundPartitions::try_new(
            memory,
            policy.resource_envelope.group_state_soft_item_budget,
            retained,
            roles.numeric_group < roles.utf8_group,
        )?
        else {
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
            return Err(failed("deferred owner capacity exceeds reserved window"));
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
            _handoff: handoff,
            session: session.clone(),
            columns: [
                columns[roles.numeric_column].clone(),
                columns[roles.utf8_column].clone(),
            ],
            roles,
            evidence: None,
            rows: 0,
            partial_entries: 0,
            dictionary_chunks: 0,
            bytes_hashed: 0,
            comparisons: 0,
            canonicalization: 0,
            count: 0,
            merge: 0,
            submit_time: 0,
            selection_jobs: 0,
            retry_jobs: 0,
            handoffs: 0,
            peak_partial: 0,
            parallelism,
            #[cfg(test)]
            deny_next_initial_reservation: false,
        }))
    }
    pub(super) fn has_active_partitions(&self) -> bool {
        self.partitions.is_some()
    }
    #[cfg(test)]
    pub(super) fn has_committed_groups(&self) -> bool {
        self.partitions
            .as_ref()
            .is_some_and(|partitions| partitions.groups() != 0)
    }
    #[cfg(test)]
    pub(super) fn deny_next_initial_reservation_for_test(&mut self) {
        self.deny_next_initial_reservation = true;
    }
    pub(super) fn cancel_for_source_replay(&self) {
        if let Some(partitions) = &self.partitions {
            partitions.request_pressure();
        }
        self.jobs.cancel();
    }
    pub(super) fn before_next(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
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
    pub(super) fn submit(
        &mut self,
        chunk: &ArrayRef,
        states: &mut GroupedAggregateStates<'_>,
    ) -> Result<bool> {
        if self.partitions.is_none() {
            return Ok(false);
        }
        let started = Instant::now();
        let numeric = super::logical_field_from_native_array(chunk, &self.columns[0])?;
        let text = super::logical_field_from_native_array(chunk, &self.columns[1])?;
        let bytes = CompoundPartial::bytes(chunk.len())?
            .checked_add(CompoundPartial::dictionary_hash_bytes(&text)?)
            .and_then(|bytes| {
                bytes.checked_add((size_of::<Completed>() + 2 * size_of::<ArrayRef>()) as u64)
            })
            .ok_or_else(|| failed("compound task capacity overflowed"))?;
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
            // This chunk has not contributed any weight. The caller's existing
            // exact native route consumes it once after the weighted handoff.
            self.submit_time += started.elapsed().as_nanos();
            return Ok(false);
        }
        let partitions = Arc::clone(
            self.partitions
                .as_ref()
                .expect("active compound partitions"),
        );
        let session = self.session.clone();
        let memory = self.jobs.memory().clone();
        states.numeric_utf8_topk_group_roles = Some(self.roles);
        states.numeric_utf8_topk_heavy_hitter_enabled = true;
        // Simulate a competing owner between the availability snapshot and the
        // atomic reservation. The actual pool denial follows production code.
        #[cfg(test)]
        let reservation_race = if std::mem::take(&mut self.deny_next_initial_reservation) {
            Some(self.jobs.memory().reserve(self.available_bytes())?)
        } else {
            None
        };
        let submitted = self.jobs.try_submit(bytes, move |worker, lease| {
            let denied_before = memory.snapshot().denied_reservations;
            let partial = match compound_count_partial::count_admitted(
                &numeric,
                &text,
                session.create_execution_ctx(),
                worker,
                lease,
            )? {
                CountOutcome::Counted(partial) => partial,
                CountOutcome::OwnedAllocationDenied(error) => {
                    worker.check_cancelled()?;
                    if memory.snapshot().denied_reservations <= denied_before {
                        return Err(error);
                    }
                    partitions.request_pressure();
                    return Ok(Completed::Retry([numeric, text]));
                }
            };
            partitions.reduce(partial, worker).map(Completed::Count)
        });
        #[cfg(test)]
        drop(reservation_race);
        if let SubmitOutcome::InitialCapacityDenied(_) = submitted? {
            self.handoff(states)?;
            self.submit_time += started.elapsed().as_nanos();
            return Ok(false);
        }
        self.submit_time += started.elapsed().as_nanos();
        Ok(true)
    }
    fn available_bytes(&self) -> u64 {
        let snapshot = self.jobs.memory().snapshot();
        snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes)
    }
    fn record_numeric_work(
        &self,
        states: &mut GroupedAggregateStates<'_>,
        work: &compound_count_partial::Work,
    ) -> Result<()> {
        if work.numeric_required_execution {
            states.native_numeric_accessor_work.record_native_owner(
                &self.columns[0],
                work.rows,
                work.numeric_source_bytes,
                work.numeric_canonical_bytes,
                work.numeric_execution_nanos,
            )?;
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
            return Err(failed("compound completion lost source order"));
        }
        let started = Instant::now();
        completed.consume(|completed| {
            match completed {
                Completed::Count(receipt) => {
                    if let Some(partial) = &receipt.deferred {
                        if self.deferred.len() == self.deferred.capacity() {
                            return Err(failed("compound deferred owners exceed window"));
                        }
                        self.deferred.push(Arc::clone(partial));
                    }
                    let work = &receipt.work;
                    self.record_numeric_work(states, work)?;
                    self.rows = self
                        .rows
                        .checked_add(work.rows)
                        .ok_or_else(|| failed("compound row count overflowed"))?;
                    self.partial_entries = self
                        .partial_entries
                        .checked_add(work.entries)
                        .ok_or_else(|| failed("compound entry count overflowed"))?;
                    self.bytes_hashed = self
                        .bytes_hashed
                        .checked_add(work.bytes_hashed)
                        .ok_or_else(|| failed("compound hashed bytes overflowed"))?;
                    self.comparisons = self
                        .comparisons
                        .checked_add(work.comparisons)
                        .ok_or_else(|| failed("compound comparisons overflowed"))?;
                    self.canonicalization += work.canonicalization_nanos;
                    self.count += work.count_nanos;
                    self.peak_partial = self.peak_partial.max(work.capacity_bytes);
                    self.dictionary_chunks += u64::from(work.dictionary);
                }
                Completed::Retry(arrays) => {
                    if self.retries.len() == self.retries.capacity() {
                        return Err(failed("retry owners exceed reserved window"));
                    }
                    self.retries.push(arrays.clone());
                }
                Completed::Unpartitioned(partial) => {
                    partial
                        .visit(|key, value, count| install_weighted(states, key, value, count))?;
                    let work = &partial.work;
                    self.record_numeric_work(states, work)?;
                    self.rows = self
                        .rows
                        .checked_add(work.rows)
                        .ok_or_else(|| failed("retry row count overflowed"))?;
                    self.partial_entries = self
                        .partial_entries
                        .checked_add(work.entries)
                        .ok_or_else(|| failed("retry entry count overflowed"))?;
                    self.bytes_hashed = self
                        .bytes_hashed
                        .checked_add(work.bytes_hashed)
                        .ok_or_else(|| failed("retry hashed bytes overflowed"))?;
                    self.comparisons = self
                        .comparisons
                        .checked_add(work.comparisons)
                        .ok_or_else(|| failed("retry comparisons overflowed"))?;
                    self.canonicalization += work.canonicalization_nanos;
                    self.count += work.count_nanos;
                    self.peak_partial = self.peak_partial.max(work.capacity_bytes);
                    self.dictionary_chunks += u64::from(work.dictionary);
                }
                Completed::Selected(selection) => {
                    self.partitions
                        .as_ref()
                        .ok_or_else(|| failed("selection lost partition owner"))?
                        .visit_selected(selection, |key, value, count| {
                            install_exact(states, key, value, count)
                        })?;
                }
            }
            Ok(())
        })?;
        self.merge += started.elapsed().as_nanos();
        Ok(())
    }
    pub(super) fn drain(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        while self.jobs.outstanding() > 0 {
            self.merge_next(states)?;
        }
        Ok(())
    }
    fn handoff(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        let Some(partitions) = self.partitions.clone() else {
            return Ok(());
        };
        partitions.request_pressure();
        self.drain(states)?;
        states.numeric_utf8_topk_heavy_hitter_enabled = true;
        states.numeric_utf8_topk_group_roles = Some(self.roles);
        states.numeric_utf8_topk_heavy_hitter_sketch =
            Some(NumericUtf8TopKHeavyHitterSketch::new_with_exact_mirror(
                states.numeric_utf8_topk_heavy_hitter_capacity(),
                0,
            ));
        let started = Instant::now();
        let mut committed = 0_u64;
        partitions.replay_and_release(|key, value, count| {
            install_weighted(states, key, value, count)?;
            committed = committed
                .checked_add(count)
                .ok_or_else(|| failed("compound replay weight overflowed"))?;
            Ok(())
        })?;
        if committed != partitions.rows.load(Ordering::Acquire) {
            return Err(failed("compound replay differs from committed weight"));
        }
        let mut suffix = 0_u64;
        for partial in self.deferred.drain(..) {
            partial.visit(|key, value, count| {
                install_weighted(states, key, value, count)?;
                suffix = suffix
                    .checked_add(count)
                    .ok_or_else(|| failed("compound suffix weight overflowed"))?;
                Ok(())
            })?;
        }
        if committed.checked_add(suffix) != Some(self.rows) {
            return Err(failed(
                "compound prefix/suffix differs from completed input weight",
            ));
        }
        self.merge += started.elapsed().as_nanos();
        self.handoffs += 1;
        self.evidence = Some(partitions.evidence()?);
        self.partitions = None;
        drop(partitions);
        // No failed attempt contributed rows. Retry each exact immutable leaf
        // pair once after every partition/job releases its state. A second
        // provider denial is an explicit error, never another retry.
        while let Some([numeric, text]) = self.retries.pop() {
            let bytes = CompoundPartial::bytes(numeric.len())?
                .checked_add(CompoundPartial::dictionary_hash_bytes(&text)?)
                .and_then(|bytes| {
                    bytes.checked_add((size_of::<Completed>() + 2 * size_of::<ArrayRef>()) as u64)
                })
                .ok_or_else(|| failed("retry capacity overflowed"))?;
            let session = self.session.clone();
            self.jobs.submit(bytes, move |worker, lease| {
                match compound_count_partial::count_admitted(
                    &numeric,
                    &text,
                    session.create_execution_ctx(),
                    worker,
                    lease,
                )? {
                    CountOutcome::Counted(partial) => Ok(Completed::Unpartitioned(partial)),
                    CountOutcome::OwnedAllocationDenied(error) => Err(error),
                }
            })?;
            self.retry_jobs += 1;
            self.merge_next(states)?;
        }
        states.count_star_direct_updates = true;
        states
            .aggregate_accessor_summary
            .insert("native_compound_owned_all_key_count_partial".into());
        Ok(())
    }
    pub(super) fn finish(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        self.drain(states)?;
        if self
            .partitions
            .as_ref()
            .is_some_and(|partitions| partitions.pressured())
        {
            return self.handoff(states);
        }
        let Some(partitions) = self.partitions.clone() else {
            return Ok(());
        };
        if partitions.rows.load(Ordering::Acquire) != self.rows {
            return Err(failed(
                "compound completed partitions differ from source rows",
            ));
        }
        states.numeric_utf8_topk_group_roles = Some(self.roles);
        states.numeric_utf8_topk_heavy_hitter_enabled = true;
        states.numeric_utf8_topk_exact_counts = Some(rustc_hash::FxHashMap::default());
        for index in 0..PARTITIONS {
            if self.jobs.is_full() {
                self.merge_next(states)?;
            }
            let partitions = Arc::clone(&partitions);
            self.jobs.submit(0, move |worker, _lease| {
                partitions.select(index, worker).map(Completed::Selected)
            })?;
            self.selection_jobs += 1;
        }
        self.drain(states)?;
        states.numeric_utf8_topk_first_pass_exact_counts = true;
        states.numeric_utf8_topk_exact_counts_source = Some("complete_key_partition_topk");
        states.numeric_utf8_topk_total_weight = self.rows;
        states.complete_key_partition_group_count = Some(partitions.groups());
        states.count_star_direct_updates = true;
        states.chunk_dictionary_direct_updates = self.dictionary_chunks != 0;
        states
            .aggregate_accessor_summary
            .insert("native_compound_complete_key_partition_counts".into());
        partitions.release()?;
        self.evidence = Some(partitions.evidence()?);
        self.partitions = None;
        Ok(())
    }
    // One flat evidence mapping keeps the runtime counters and their scope
    // together, independently of query execution.
    #[allow(clippy::too_many_lines)]
    pub(super) fn annotate_summary(&self, summary: &mut String) -> Result<()> {
        let mut payload: serde_json::Value =
            serde_json::from_str(summary).map_err(|error| failed(&error.to_string()))?;
        let object = payload
            .as_object_mut()
            .ok_or_else(|| failed("compound summary is not an object"))?;
        let memory = self.jobs.memory().snapshot();
        let pool = self.jobs.pool_snapshot();
        for (key, value) in [
            ("rows", u128::from(self.rows)),
            ("partial_entries", u128::from(self.partial_entries)),
            (
                "submitted_chunks",
                u128::from(self.jobs.submitted() - self.selection_jobs - self.retry_jobs),
            ),
            (
                "completed_chunks",
                u128::from(self.jobs.joined() - self.selection_jobs - self.retry_jobs),
            ),
            ("outstanding_chunks", self.jobs.outstanding() as u128),
            (
                "peak_outstanding_chunks",
                self.jobs.peak_outstanding() as u128,
            ),
            ("cpu_ceiling", self.parallelism as u128),
            (
                "compute_threads",
                pool.map_or(0, |pool| pool.workers_created) as u128,
            ),
            ("provider_background_workers", 0),
            (
                "worker_busy_elapsed_nanos",
                u128::from(self.jobs.worker_busy_nanos()),
            ),
            ("inline_busy_elapsed_nanos", self.jobs.inline_busy_nanos()),
            ("canonicalization_work_nanos", self.canonicalization),
            ("count_work_nanos", self.count),
            ("caller_merge_nanos", self.merge),
            ("caller_join_wait_nanos", self.jobs.join_wait_nanos()),
            ("caller_submit_elapsed_nanos", self.submit_time),
            ("utf8_bytes_hashed", u128::from(self.bytes_hashed)),
            ("equality_comparisons", u128::from(self.comparisons)),
            (
                "native_dictionary_chunks",
                u128::from(self.dictionary_chunks),
            ),
            ("peak_partial_capacity_bytes", u128::from(self.peak_partial)),
            (
                "shared_live_peak_bytes",
                u128::from(memory.peak_reserved_bytes),
            ),
            ("shared_live_limit_bytes", u128::from(memory.limit_bytes)),
            ("partition_native_handoffs", u128::from(self.handoffs)),
            ("partition_selection_jobs", u128::from(self.selection_jobs)),
            ("partition_retry_jobs", u128::from(self.retry_jobs)),
        ] {
            object.insert(
                format!("aggregate_workers_{key}"),
                u64::try_from(value).unwrap_or(u64::MAX).into(),
            );
        }
        if let Some(evidence) = &self.evidence {
            for (key, value) in [
                ("partition_count", PARTITIONS as u64),
                ("partition_complete_groups", evidence.groups as u64),
                ("partition_committed_rows", evidence.rows),
                ("partition_lock_wait_nanos", evidence.lock_wait_nanos),
                ("partition_reconcile_work_nanos", evidence.reconcile_nanos),
                ("partition_selection_work_nanos", evidence.selection_nanos),
                ("partition_equality_comparisons", evidence.comparisons),
                ("partition_entry_credit_claim_calls", evidence.credit_claims),
                (
                    "partition_entry_credit_return_calls",
                    evidence.credit_returns,
                ),
                ("compound_partition_strings", evidence.strings),
                (
                    "compound_partition_utf8_bytes_copied",
                    evidence.string_bytes_copied,
                ),
            ] {
                object.insert(format!("aggregate_workers_{key}"), value.into());
            }
            if self.handoffs == 0 {
                object.insert("candidate_groups".into(), evidence.groups.into());
                object.insert(
                    "group_output_strategy".into(),
                    "complete_numeric_utf8_key_partition_exact_topk".into(),
                );
                object.insert(
                    "uniqueness_proof_status".into(),
                    "complete_numeric_utf8_key_partition_all_contributions_before_selection".into(),
                );
                object.insert(
                    "group_key_storage".into(),
                    "owned_original_width_integer_partial_and_partition_utf8_domains".into(),
                );
            }
        }
        object.insert("aggregate_workers_compound_key_scope".into(), "all_eight_integer_widths_native_owners;exact_signedness_numeric_bits_and_utf8_bytes;dictionary_codes_bound_to_retained_native_values;all_keys_in_each_partial;complete_key_partition_selection_only_after_eof;weighted_committed_prefix_and_unconsumed_suffix_once_on_pressure;no_local_topk".into());
        object.insert("aggregate_workers_scope".into(), "compound_native_count_and_partition_reconciliation_on_same_bounded_workers;sum_worker_elapsed_not_cpu_or_exclusive_wall;caller_merge_includes_pressure_replay;owned_capacity_includes_partial_tables_deferred_owners_partition_domains_group_tables_selection_and_old_plus_new_growth;provider_allocations_bypassing_host_allocator_legacy_handoff_output_maps_and_process_rss_excluded;retained_source_generation_validated_after_drain_and_refinement".into());
        *summary = payload.to_string();
        Ok(())
    }
}

fn roles(
    states: &GroupedAggregateStates<'_>,
    dtype: &DType,
    columns: &[String],
) -> Option<NumericUtf8GroupRoles> {
    if states.group_key_indices.len() != 2
        || states.group_columns.len() != 2
        || !states.state_template.is_count_star_only()
        || states.result_limit.is_none()
        || !states.request.having.is_empty()
        || !states.groups.is_empty()
        || !states.group_order.is_empty()
        || !matches!(states.request.order_by.as_slice(), [order] if order.descending && states.state_template.count_star_alias().is_ok_and(|alias| order.column == alias))
    {
        return None;
    }
    let DType::Struct(fields, Nullability::NonNullable) = dtype else {
        return None;
    };
    let mut numeric = None;
    let mut text = None;
    for &index in &states.group_key_indices {
        let group = states.group_columns.get(index)?;
        if !matches!(group.transform, AggregateValueTransform::Identity)
            || !group.extra_column_indices.is_empty()
        {
            return None;
        }
        let dtype = fields.field(columns.get(group.column_index)?.as_str())?;
        match dtype {
            DType::Primitive(ptype, Nullability::NonNullable)
                if ptype.is_int() && numeric.is_none() =>
            {
                numeric = Some((index, group.column_index));
            }
            DType::Utf8(Nullability::NonNullable) if text.is_none() => {
                text = Some((index, group.column_index));
            }
            _ => return None,
        }
    }
    let (numeric_group, numeric_column) = numeric?;
    let (utf8_group, utf8_column) = text?;
    Some(NumericUtf8GroupRoles {
        numeric_group,
        numeric_column,
        utf8_group,
        utf8_column,
    })
}

/// The source is already open: use exactly the grouped request rewrite and
/// native predicate split that precede worker admission, without reopening it.
pub(super) fn request_schema_may_be_admitted(
    request: &super::VortexQueryPrimitiveRequest,
    dtype: &DType,
) -> bool {
    let Ok(aggregate) = super::required_simple_aggregate(request) else {
        return false;
    };
    let Ok(plan) = super::rewrite_simple_aggregate_for_embedded_derived_columns(
        dtype,
        aggregate,
        request.predicate.as_ref(),
    ) else {
        return false;
    };
    if plan.predicate.as_ref().is_some_and(|predicate| {
        super::split_predicate_for_vortex_pushdown(predicate, request.kind)
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
        super::VortexLocalPrimitiveResourceEnvelope::default_single_threaded(),
    ) else {
        return false;
    };
    roles(&states, dtype, &columns).is_some()
}
fn intern(
    states: &mut GroupedAggregateStates<'_>,
    key: Key,
    value: &str,
) -> Result<AggregateNumericUtf8InternedKey> {
    Ok(AggregateNumericUtf8InternedKey::new(
        key.part(),
        states.string_interner.intern(value)?,
    ))
}
fn install_weighted(
    states: &mut GroupedAggregateStates<'_>,
    key: Key,
    value: &str,
    count: u64,
) -> Result<()> {
    let key = intern(states, key, value)?;
    states.update_numeric_utf8_topk_weighted_key(key, count)
}
fn install_exact(
    states: &mut GroupedAggregateStates<'_>,
    key: Key,
    value: &str,
    count: u64,
) -> Result<()> {
    let key = intern(states, key, value)?;
    let counts = states
        .numeric_utf8_topk_exact_counts
        .as_mut()
        .ok_or_else(|| failed("compound exact output state missing"))?;
    super::reserve_hash_map_capacity(counts, 1, "compound final candidate union")?;
    if counts.insert(key, count).is_some() {
        return Err(failed("complete key appeared in two final partitions"));
    }
    Ok(())
}
