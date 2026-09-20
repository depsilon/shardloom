//! Actual source-chunk canonicalization and exact count work. Global state stays
//! caller-owned; its ordered weighted merge is measured separately. Every key
//! survives each partial, including candidates outside every chunk's top K.

use super::{
    AggregateSingleNumericKey, AggregateValueTransform, GroupedAggregateStates,
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
    aggregate_chunk_jobs::{AggregateChunkJobs, ChunkWorkerContext},
    numeric_count_partial::{self, NumericCountKey, OwnedNumericCounts},
    numeric_count_partitions::{self, NumericPartition, NumericPartitions},
    string_count_partial::{self, StringCountMerge, StringCountPartial},
    string_count_partitions::{self, PartitionReceipt, PartitionSelection, StringCountPartitions},
};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Instant,
};
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
    I32,
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
    Partitioned(PartitionReceipt),
    RetryString(ArrayRef),
    Selected(PartitionSelection),
    NumericSelected(numeric_count_partitions::Selection),
    Signed32(OwnedNumericCounts<i32>, NumericWork),
    Signed(OwnedNumericCounts<i64>, NumericWork),
    Unsigned(OwnedNumericCounts<u64>, NumericWork),
}

// One caller-owned coordinator per query; keep it inline instead of introducing
// an additional heap owner just to make the two private variants equally sized.
#[allow(clippy::large_enum_variant)]
pub(super) enum CountWorkers {
    PairPartitions(super::pair_partition_workers::PairWorkers),
    Triple(super::triple_count_workers::TripleWorkers),
    Single(SingleCountWorkers),
    Compound(super::compound_count_workers::CompoundWorkers),
    ExactDistinct(super::exact_distinct_pairs::workers::ExactDistinctWorkers),
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
        #[cfg(test)]
        let _admission_pressure = ADMISSION_TEST_PRESSURE
            .with(std::cell::Cell::take)
            .then(|| {
                let snapshot = memory.snapshot();
                memory
                    .reserve(snapshot.limit_bytes - snapshot.reserved_bytes)
                    .expect("one-shot pressure reserves only currently available query credit")
            });
        if let Some(workers) = super::pair_partition_workers::PairWorkers::admit(
            states, dtype, columns, policy, memory,
        )? {
            return Ok(Some(Self::PairPartitions(workers)));
        }
        if let Some(workers) = super::triple_count_workers::TripleWorkers::admit(
            states, dtype, columns, policy, memory,
        )? {
            return Ok(Some(Self::Triple(workers)));
        }
        if let Some(workers) = super::exact_distinct_pairs::workers::ExactDistinctWorkers::admit(
            states, dtype, columns, policy, session, memory,
        )? {
            return Ok(Some(Self::ExactDistinct(workers)));
        }
        if let Some(workers) = super::compound_count_workers::CompoundWorkers::admit(
            states, dtype, columns, policy, session, memory,
        )? {
            return Ok(Some(Self::Compound(workers)));
        }
        SingleCountWorkers::admit(states, dtype, columns, policy, session, memory)
            .map(|workers| workers.map(Self::Single))
    }
    pub(super) fn before_next(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        match self {
            Self::PairPartitions(workers) => workers.before_next(),
            Self::Triple(workers) => workers.before_next(),
            Self::Single(workers) => workers.before_next(states),
            Self::Compound(workers) => workers.before_next(states),
            Self::ExactDistinct(workers) => workers.before_next(states),
        }
    }
    pub(super) fn submit(
        &mut self,
        chunk: &ArrayRef,
        states: &mut GroupedAggregateStates<'_>,
    ) -> Result<bool> {
        match self {
            Self::PairPartitions(workers) => workers.submit(chunk, states),
            Self::Triple(workers) => workers.submit(chunk, states),
            Self::Single(workers) => workers.submit(chunk, states),
            Self::Compound(workers) => workers.submit(chunk, states),
            Self::ExactDistinct(workers) => workers.submit(chunk, states),
        }
    }
    pub(super) fn finish(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        match self {
            Self::PairPartitions(workers) => workers.finish(states),
            Self::Triple(workers) => workers.finish(states),
            Self::Single(workers) => workers.finish(states),
            Self::Compound(workers) => workers.finish(states),
            Self::ExactDistinct(workers) => {
                workers.finish(states)?;
                states.finalized_distinct_counts = workers.take_exact_result();
                Ok(())
            }
        }
    }
    pub(super) fn annotate_summary(&self, summary: &mut String) -> Result<()> {
        match self {
            Self::PairPartitions(workers) => workers.annotate_summary(summary),
            Self::Triple(workers) => workers.annotate_summary(summary),
            Self::Single(workers) => workers.annotate_summary(summary),
            Self::Compound(workers) => workers.annotate_summary(summary),
            Self::ExactDistinct(workers) => workers.annotate_summary(summary),
        }
    }
    pub(super) fn has_active_partitions(&self) -> bool {
        match self {
            // Pair/triple state has no certified bounded serial/spill destination.
            // It must fail and release owners on source pressure, never replay.
            Self::PairPartitions(_) | Self::Triple(_) => false,
            Self::Single(workers) => workers.has_active_partitions(),
            Self::Compound(workers) => workers.has_active_partitions(),
            Self::ExactDistinct(workers) => workers.has_active_partitions(),
        }
    }
    pub(super) fn cancel_for_source_replay(&self) {
        match self {
            Self::PairPartitions(workers) => workers.cancel(),
            Self::Triple(workers) => workers.cancel(),
            Self::Single(workers) => workers.cancel_for_source_replay(),
            Self::Compound(workers) => workers.cancel_for_source_replay(),
            Self::ExactDistinct(workers) => workers.cancel_for_source_replay(),
        }
    }
    #[cfg(test)]
    pub(super) fn inject_scan_fault_for_test(
        &self,
        memory: &LiveMemoryPool,
        chunks: usize,
    ) -> Option<vortex::error::VortexResult<ArrayRef>> {
        use vortex::array::memory::HostAllocator as _;

        let committed = match self {
            Self::PairPartitions(workers) => workers.has_committed_input(),
            Self::Triple(workers) => workers.has_committed_groups(),
            Self::Single(workers) => return workers.inject_scan_fault_for_test(memory, chunks),
            Self::Compound(workers) => workers.has_committed_groups(),
            Self::ExactDistinct(workers) => workers.has_committed_groups(),
        };
        if chunks == 0 || !committed {
            return None;
        }
        let fault = SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::take)?;
        let snapshot = memory.snapshot();
        // Workers may release reservations after this snapshot. Request the
        // whole limit so alignment capacity guarantees denial even at zero use.
        let len = usize::try_from(snapshot.limit_bytes).ok()?;
        let denied = crate::owned_buffers::ReservedHostAllocator::new(memory.clone())
            .allocate(len, vortex::buffer::Alignment::DEFAULT_ALIGNMENT)
            .expect_err("native allocator denial includes alignment capacity");
        Some(Err(match fault {
            SourceScanTestFault::OwnedDenial => denied,
            SourceScanTestFault::CorruptionWithConcurrentDenial => {
                vortex::error::vortex_err!(InvalidArgument: "injected source corruption")
            }
        }))
    }

    /// True only after this family's job pool has been retired. Other families
    /// retain their existing CPU ownership and never request this transition.
    pub(super) fn provider_restore_requested(&self) -> bool {
        matches!(self, Self::PairPartitions(workers) if workers.provider_restore_requested())
    }
}

/// A source-shape precheck only. Schema and existing physical state gates below
/// still decide admission before any worker contributes to an aggregate.
pub(super) fn request_may_be_admitted(request: &VortexQueryPrimitiveRequest) -> bool {
    if super::pair_partition_workers::request_may_be_admitted(request) {
        return true;
    }
    if super::triple_count_workers::request_may_be_admitted(request) {
        return true;
    }
    if super::exact_distinct_pairs::workers::request_may_be_admitted(request) {
        return true;
    }
    let Ok(aggregate) = super::required_simple_aggregate(request) else {
        return false;
    };
    let columns = aggregate
        .projected_columns()
        .iter()
        .map(|column| column.as_str().to_owned())
        .collect::<Vec<_>>();
    let ordinary = matches!(aggregate.group_by.len(), 1 | 2)
        && super::aggregate_group_expressions_are_reconstructable_constants(aggregate)
        && super::SimpleAggregateStates::new(aggregate, &columns)
            .is_ok_and(|states| states.is_count_star_only());
    if ordinary {
        return true;
    }
    // The request precheck must not discard the physical-key proof before
    // schema admission. Construct the same lowering state, without any source
    // access or worker allocation. Actual admission still checks native dtype.
    let Ok(envelope) = super::VortexLocalPrimitiveResourceEnvelope::new(1, 1) else {
        return false;
    };
    GroupedAggregateStates::new_with_resource_envelope(
        aggregate,
        request.source_order_limit,
        &columns,
        false,
        false,
        envelope,
    )
    .is_ok_and(|states| numeric_state_admitted(&states))
}

/// Multi-role shapes must restore provider CPU drivers when their original
/// source schema or residual predicate cannot use the owned worker family.
pub(super) fn restore_provider_drivers(
    request: &VortexQueryPrimitiveRequest,
    dtype: &DType,
) -> bool {
    if super::pair_partition_workers::request_may_be_admitted(request) {
        return !super::pair_partition_workers::request_schema_may_be_admitted(request, dtype);
    }
    if super::triple_count_workers::request_may_be_admitted(request) {
        return !super::triple_count_workers::request_schema_may_be_admitted(request, dtype);
    }
    if super::exact_distinct_pairs::workers::request_may_be_admitted(request) {
        return !super::exact_distinct_pairs::workers::request_schema_may_be_admitted(
            request, dtype,
        ) && !super::compound_count_workers::request_schema_may_be_admitted(request, dtype);
    }
    super::required_simple_aggregate(request).is_ok_and(|aggregate| aggregate.group_by.len() == 2)
        && !super::compound_count_workers::request_schema_may_be_admitted(request, dtype)
}

pub(super) struct SingleCountWorkers {
    jobs: AggregateChunkJobs<Partial>,
    kind: KeyKind,
    group_index: usize,
    column: String,
    preserve_order: bool,
    session: VortexSession,
    string_merge: StringCountMerge,
    partitions: Option<Arc<StringCountPartitions>>,
    partition_evidence: Option<string_count_partitions::PartitionEvidence>,
    deferred: Vec<Arc<StringCountPartial>>,
    retry_arrays: Vec<ArrayRef>,
    _handoff_lease: MemoryLease,
    partition_handoffs: u64,
    partition_groups: usize,
    selection_jobs: u64,
    retry_jobs: u64,
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
    numeric_dictionary_bypass: bool,
    numeric_partitions: Option<NumericPartitions>,
    numeric_partition_evidence: Option<numeric_count_partitions::Evidence>,
    peak_partial_bytes: u64,
    max_parallelism: usize,
}

impl SingleCountWorkers {
    #[cfg(test)]
    pub(super) fn inject_scan_fault_for_test(
        &self,
        memory: &LiveMemoryPool,
        chunks: usize,
    ) -> Option<vortex::error::VortexResult<ArrayRef>> {
        use vortex::array::memory::HostAllocator as _;
        if chunks == 0
            || (self
                .partitions
                .as_ref()
                .is_none_or(|partitions| partitions.group_count() == 0)
                && self
                    .numeric_partitions
                    .as_ref()
                    .is_none_or(|p| p.evidence().rows == 0))
        {
            return None;
        }
        let fault = SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::take)?;
        let snapshot = memory.snapshot();
        // Do not base fault injection on concurrently changing free capacity.
        // The allocator charges alignment in addition to this entire limit.
        let len = usize::try_from(snapshot.limit_bytes).ok()?;
        let allocator = crate::owned_buffers::ReservedHostAllocator::new(memory.clone());
        let denied = allocator
            .allocate(len, vortex::buffer::Alignment::DEFAULT_ALIGNMENT)
            .expect_err("native allocator denial must include alignment capacity");
        Some(Err(match fault {
            SourceScanTestFault::OwnedDenial => denied,
            SourceScanTestFault::CorruptionWithConcurrentDenial => {
                vortex::error::vortex_err!(InvalidArgument: "injected source corruption")
            }
        }))
    }

    #[allow(clippy::too_many_lines)]
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
            DType::Primitive(
                ptype @ (PType::I32 | PType::I64 | PType::U64),
                Nullability::NonNullable,
            ) if numeric_state_admitted(states) => match ptype {
                PType::I32 => KeyKind::I32,
                PType::I64 => KeyKind::I64,
                PType::U64 => KeyKind::U64,
                _ => unreachable!("admitted integer dtype"),
            },
            _ => return Ok(None),
        };
        let max_parallelism = policy
            .resource_envelope
            .max_parallelism
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        let window = max_parallelism.saturating_mul(2).clamp(1, 24);
        let handoff_bytes = window
            .checked_mul(size_of::<Arc<StringCountPartial>>() + size_of::<ArrayRef>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| failed("handoff capacity overflowed"))?;
        let mut handoff_lease = memory
            .reserve(handoff_bytes)
            .or_else(|_| memory.reserve(0))?;
        let mut deferred = Vec::new();
        let mut retry_arrays = Vec::new();
        let handoff_slots = if handoff_lease.bytes() == handoff_bytes {
            window
        } else {
            0
        };
        deferred
            .try_reserve_exact(handoff_slots)
            .map_err(|error| failed(&error.to_string()))?;
        retry_arrays
            .try_reserve_exact(handoff_slots)
            .map_err(|error| failed(&error.to_string()))?;
        if deferred.capacity() > window || retry_arrays.capacity() > window {
            return Err(failed("handoff capacity exceeds admitted window"));
        }
        let partitions = if handoff_slots != 0
            && matches!(kind, KeyKind::Utf8)
            && states.request.having.is_empty()
            && states.result_limit.is_some()
            && matches!(states.request.order_by.as_slice(), [order] if order.descending && states.state_template.count_star_alias().is_ok_and(|alias| order.column == alias))
        {
            if let Some(cap) = states
                .request
                .offset
                .checked_add(states.result_limit.unwrap_or(0))
                .filter(|cap| *cap != 0)
            {
                StringCountPartitions::try_new(
                    memory,
                    if states.string_count_topk_heavy_hitter_enabled {
                        states.string_count_topk_first_pass_exact_histogram_entry_budget
                    } else {
                        usize::MAX
                    },
                    cap,
                )?
            } else {
                None
            }
        } else {
            None
        };
        if partitions.is_none() {
            deferred = Vec::new();
            retry_arrays = Vec::new();
            handoff_lease.resize(0)?;
        }
        let numeric_partitions = if !matches!(kind, KeyKind::Utf8)
            && states.request.spill.is_none()
            && states.result_limit.is_some_and(|limit| {
                limit > 0
                    && states
                        .request
                        .offset
                        .checked_add(limit)
                        .is_some_and(|cap| cap <= 128)
            }) {
            let ptype = match kind {
                KeyKind::I32 => PType::I32,
                KeyKind::I64 => PType::I64,
                KeyKind::U64 => PType::U64,
                KeyKind::Utf8 => unreachable!("numeric admission"),
            };
            NumericPartitions::try_new(ptype, memory)?
        } else {
            None
        };
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
            partitions,
            partition_evidence: None,
            deferred,
            retry_arrays,
            _handoff_lease: handoff_lease,
            partition_handoffs: 0,
            partition_groups: 0,
            selection_jobs: 0,
            retry_jobs: 0,
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
            numeric_dictionary_bypass: false,
            numeric_partitions,
            numeric_partition_evidence: None,
            peak_partial_bytes: 0,
            max_parallelism,
        }))
    }

    /// Called before advancing the source: completed results consume window
    /// slots until their ordered merge releases both payload and capacity.
    pub(super) fn before_next(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        if self
            .partitions
            .as_ref()
            .is_some_and(|partitions| partitions.pressure_requested())
        {
            self.handoff_partitions(states)?;
        }
        if self.jobs.is_full() {
            self.merge_next(states)?;
        }
        Ok(())
    }

    pub(super) fn has_active_partitions(&self) -> bool {
        self.partitions.is_some()
    }

    pub(super) fn cancel_for_source_replay(&self) {
        self.jobs.cancel();
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
            self.handoff_numeric_partitions(states)?;
            self.numeric_dictionary_bypass = true;
            return Ok(false);
        }
        let numeric_rows = if array.as_opt::<Constant>().is_some() {
            usize::from(!array.is_empty())
        } else {
            array.len()
        };
        let partial_bytes = match self.kind {
            KeyKind::Utf8 => string_count_partial::partial_bytes(&array)?,
            KeyKind::I32 => numeric_count_partial::partial_bytes::<i32>(numeric_rows)?,
            KeyKind::I64 | KeyKind::U64 => {
                numeric_count_partial::partial_bytes::<u64>(numeric_rows)?
            }
        };
        let deferred_metadata = if self.partitions.is_some() {
            StringCountPartial::deferred_metadata_bytes()
        } else {
            0
        };
        let mut initial_bytes = partial_bytes
            .checked_add(std::mem::size_of::<Partial>() as u64)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<ArrayRef>() as u64))
            .and_then(|bytes| bytes.checked_add(deferred_metadata))
            .ok_or_else(|| failed("task capacity overflowed"))?;
        // Release earlier completed capacity before denying the next known
        // allocation. In-flight provider allocation can still deny explicitly.
        while self.jobs.outstanding() > 0 && {
            let snapshot = self.jobs.memory().snapshot();
            snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes) < initial_bytes
        } {
            self.merge_next(states)?;
        }
        if self
            .partitions
            .as_ref()
            .is_some_and(|partitions| partitions.pressure_requested())
            || self.partitions.is_some() && {
                let snapshot = self.jobs.memory().snapshot();
                snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes) < initial_bytes
            }
        {
            self.handoff_partitions(states)?;
        }
        if self.partitions.is_none() {
            initial_bytes -= deferred_metadata;
        }
        let session = self.session.clone();
        let kind = self.kind;
        let preserve_order = self.preserve_order;
        let partitions = self.partitions.clone();
        self.jobs.submit(initial_bytes, move |worker, lease| {
            if matches!(kind, KeyKind::Utf8) {
                let denied_before = partitions
                    .as_ref()
                    .map(|partitions| partitions.denied_reservations());
                let counted = string_count_partial::count_string_chunk(
                    &array,
                    session.create_execution_ctx(),
                    worker,
                    lease,
                );
                let mut partial = match counted {
                    Ok(partial) => partial,
                    Err(error) => {
                        // Retry this same immutable native leaf once after releasing
                        // partition state. A repeated failure is the ordinary C4 error.
                        if let Some(partitions) = partitions.as_ref()
                            && denied_before
                                .is_some_and(|before| partitions.denied_reservations() > before)
                            && error.to_string().contains("memory reservation denied:")
                        {
                            worker.check_cancelled()?;
                            partitions.request_pressure();
                            return Ok(Partial::RetryString(array));
                        }
                        return Err(error);
                    }
                };
                if preserve_order {
                    partial.preserve_existing_key_order();
                }
                if let Some(partitions) = partitions {
                    partitions
                        .reduce(partial, worker, lease)
                        .map(Partial::Partitioned)
                } else {
                    Ok(Partial::String(partial))
                }
            } else {
                count_numeric_chunk(&array, &session, kind, worker, lease)
            }
        })?;
        self.submit_nanos += started.elapsed().as_nanos();
        Ok(true)
    }

    #[allow(clippy::too_many_lines)]
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
                Partial::Partitioned(receipt) => {
                    if let Some(deferred) = receipt.deferred.as_ref() {
                        if self.deferred.len() == self.deferred.capacity() {
                            return Err(failed("deferred chunks exceeded source window"));
                        }
                        self.deferred.push(Arc::clone(deferred));
                    }
                    self.observe_string(&receipt.work);
                    (
                        receipt.work.rows,
                        receipt.work.partial_entries,
                        receipt.work.partial_capacity_bytes,
                    )
                }
                Partial::RetryString(array) => {
                    if self.retry_arrays.len() == self.retry_arrays.capacity() {
                        return Err(failed("retry chunks exceeded source window"));
                    }
                    self.retry_arrays.push(array.clone());
                    return Ok(());
                }
                Partial::Selected(selection) => {
                    let partitions = self
                        .partitions
                        .as_ref()
                        .ok_or_else(|| failed("selection lost its complete state"))?;
                    partitions.visit_selection(selection, |value, count| {
                        install_weighted_string(states, self.group_index, value, count, false)
                    })?;
                    return Ok(());
                }
                Partial::NumericSelected(selection) => {
                    merge_numeric(
                        states,
                        selection
                            .retained
                            .iter()
                            .map(|candidate| (candidate.key, candidate.count)),
                    )?;
                    let evidence = self
                        .numeric_partition_evidence
                        .as_mut()
                        .ok_or_else(|| failed("numeric selection lost its evidence owner"))?;
                    evidence.groups = evidence
                        .groups
                        .checked_add(selection.groups)
                        .ok_or_else(|| failed("complete numeric group count overflow"))?;
                    evidence.reduced_rows = evidence
                        .reduced_rows
                        .checked_add(selection.rows)
                        .ok_or_else(|| failed("complete numeric weight overflow"))?;
                    evidence.sort_nanos += selection.sort_nanos;
                    evidence.reduce_nanos += selection.reduce_nanos;
                    return Ok(());
                }
                Partial::String(partial) => {
                    self.string_merge.merge(states, self.group_index, partial)?;
                    self.observe_string(&partial.work);
                    (
                        partial.work.rows,
                        partial.work.partial_entries,
                        partial.work.partial_capacity_bytes,
                    )
                }
                Partial::Signed32(partial, work) => {
                    let partitions = match self.numeric_partitions.as_mut() {
                        Some(NumericPartitions::Signed32(p)) => Some(p),
                        None => None,
                        _ => return Err(failed("numeric partitions lost i32 dtype")),
                    };
                    merge_numeric_partial(states, partial, partitions, || {
                        self.jobs.check_cancelled()
                    })?;
                    self.observe_numeric(work);
                    (
                        partial.rows(),
                        partial.pairs().len() as u64,
                        partial.reserved_bytes(),
                    )
                }
                Partial::Signed(partial, work) => {
                    let partitions = match self.numeric_partitions.as_mut() {
                        Some(NumericPartitions::Signed(p)) => Some(p),
                        None => None,
                        _ => return Err(failed("numeric partitions lost i64 dtype")),
                    };
                    merge_numeric_partial(states, partial, partitions, || {
                        self.jobs.check_cancelled()
                    })?;
                    self.observe_numeric(work);
                    (
                        partial.rows(),
                        partial.pairs().len() as u64,
                        partial.reserved_bytes(),
                    )
                }
                Partial::Unsigned(partial, work) => {
                    let partitions = match self.numeric_partitions.as_mut() {
                        Some(NumericPartitions::Unsigned(p)) => Some(p),
                        None => None,
                        _ => return Err(failed("numeric partitions lost u64 dtype")),
                    };
                    merge_numeric_partial(states, partial, partitions, || {
                        self.jobs.check_cancelled()
                    })?;
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

    fn observe_string(&mut self, work: &string_count_partial::StringCountPartialWork) {
        self.canonicalization_nanos += work.canonicalization_nanos;
        self.count_nanos += work.count_nanos;
        self.hashed_bytes += work.utf8_bytes_hashed;
        self.equality_comparisons += work.equality_comparisons;
        self.dictionary_chunks += u64::from(work.native_dictionary);
        self.dictionary_values += work.dictionary_values;
        self.constant_chunks += u64::from(work.native_constant);
    }

    pub(super) fn drain(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        while self.jobs.outstanding() > 0 {
            self.merge_next(states)?;
        }
        Ok(())
    }

    fn handoff_partitions(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        let Some(partitions) = self.partitions.clone() else {
            return Ok(());
        };
        partitions.request_pressure();
        self.drain(states)?;
        self.partitions = None;
        let started = Instant::now();
        let heavy = states.string_count_topk_heavy_hitter_enabled;
        if heavy {
            states.string_count_topk_first_pass_exact_histogram_counts = None;
            states.string_count_topk_first_pass_exact_histogram_disabled = true;
            states.string_count_topk_heavy_hitter_sketch = Some(
                super::StringCountTopKHeavyHitterSketch::new_with_exact_mirror(
                    states.string_count_topk_heavy_hitter_capacity(),
                    0,
                ),
            );
        }
        let mut replayed = 0_u64;
        partitions.replay_and_release(|value, count| {
            install_weighted_string(states, self.group_index, value, count, heavy)?;
            replayed = replayed
                .checked_add(count)
                .ok_or_else(|| failed("partition replay weight overflowed"))?;
            Ok(())
        })?;
        if replayed != partitions.committed_rows.load(Ordering::Acquire) {
            return Err(failed("partition replay differs from committed weight"));
        }
        if heavy {
            states.string_count_topk_total_weight = replayed;
            states.string_count_topk_first_pass_exact_histogram_input_rows = replayed;
        }
        for partial in self.deferred.drain(..) {
            self.string_merge
                .merge(states, self.group_index, &partial)?;
        }
        self.merge_nanos += started.elapsed().as_nanos();
        self.partition_handoffs += 1;
        self.partition_evidence = Some(partitions.evidence()?);
        drop(partitions);
        // Existing successful receipts already contributed their complete input
        // weights. Failed count attempts contribute only on this successful retry.
        while let Some(array) = self.retry_arrays.pop() {
            let bytes = string_count_partial::partial_bytes(&array)?
                .checked_add(size_of::<Partial>() as u64)
                .and_then(|bytes| bytes.checked_add(size_of::<ArrayRef>() as u64))
                .ok_or_else(|| failed("retry task capacity overflowed"))?;
            let session = self.session.clone();
            self.jobs.submit(bytes, move |worker, lease| {
                string_count_partial::count_string_chunk(
                    &array,
                    session.create_execution_ctx(),
                    worker,
                    lease,
                )
                .map(Partial::String)
            })?;
            self.retry_jobs += 1;
            self.merge_next(states)?;
        }
        Ok(())
    }

    /// Final complete-key selection is deliberately separate from intermediate
    /// drains: only EOF makes partition-local top-K safe.
    pub(super) fn finish(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        self.drain(states)?;
        if self.numeric_partitions.is_some() {
            return self.finish_numeric_partitions(states);
        }
        if self.numeric_dictionary_bypass && states.group_columns.len() > 1 {
            // Dictionary chunks deliberately retain their established native
            // path. Validate every observed physical key before result pruning,
            // including keys contributed by that non-worker path.
            for key in states.groups.keys() {
                validate_numeric_group_reconstruction(states, key)?;
            }
        }
        if self
            .partitions
            .as_ref()
            .is_some_and(|partitions| partitions.pressure_requested())
        {
            return self.handoff_partitions(states);
        }
        let Some(partitions) = self.partitions.clone() else {
            return Ok(());
        };
        if partitions.committed_rows.load(Ordering::Acquire) != self.rows {
            return Err(failed("complete partition weight differs from source rows"));
        }
        self.partition_groups = partitions.group_count();
        for index in 0..string_count_partitions::PARTITIONS {
            if self.jobs.is_full() {
                self.merge_next(states)?;
            }
            let partitions = Arc::clone(&partitions);
            self.jobs.submit(0, move |worker, _lease| {
                partitions.select(index, worker).map(Partial::Selected)
            })?;
            self.selection_jobs += 1;
        }
        self.drain(states)?;
        states.complete_key_partition_group_count = Some(self.partition_groups);
        if states.string_count_topk_heavy_hitter_enabled {
            states.string_count_topk_total_weight = self.rows;
            states.string_count_topk_first_pass_exact_histogram_input_rows = self.rows;
            if !states.promote_string_count_topk_first_pass_exact_histogram_if_possible()
                && self.rows != 0
            {
                return Err(failed("complete partition candidates were not promotable"));
            }
            states.string_count_topk_exact_counts_source = Some("complete_key_partition_topk");
        }
        states.count_star_direct_updates = true;
        states.chunk_dictionary_direct_updates = self.dictionary_chunks != 0;
        states
            .aggregate_accessor_summary
            .insert("native_complete_key_partition_counts".into());
        // Selected keys now have independent output-state owners. Full partition
        // storage can be released before formatting the bounded result.
        partitions.release_storage()?;
        self.partitions = None;
        self.partition_evidence = Some(partitions.evidence()?);
        Ok(())
    }

    fn handoff_numeric_partitions(
        &mut self,
        states: &mut GroupedAggregateStates<'_>,
    ) -> Result<()> {
        let Some(partitions) = self.numeric_partitions.take() else {
            return Ok(());
        };
        let started = Instant::now();
        let mut entries = 0_u64;
        partitions.visit_batches(|pairs| {
            self.jobs.check_cancelled()?;
            entries += u64::try_from(pairs.len()).map_err(|_| failed("handoff length overflow"))?;
            merge_numeric(states, pairs)?;
            Ok(())
        })?;
        self.merge_nanos += started.elapsed().as_nanos();
        let mut evidence = partitions.evidence();
        if entries != evidence.entries {
            return Err(failed("numeric handoff lost entries"));
        }
        evidence.dictionary_handoff = true;
        self.numeric_partition_evidence = Some(evidence);
        Ok(())
    }

    fn finish_numeric_partitions(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        let mut partitions = self
            .numeric_partitions
            .take()
            .ok_or_else(|| failed("numeric finish lost its partitions"))?;
        let started = Instant::now();
        let evidence = partitions.evidence();
        if evidence.rows != self.rows {
            return Err(failed("numeric partition weight differs from source rows"));
        }
        self.numeric_partition_evidence = Some(evidence);
        let cap = states
            .result_limit
            .and_then(|limit| states.request.offset.checked_add(limit))
            .filter(|&cap| cap > 0 && cap <= 128)
            .ok_or_else(|| failed("numeric selection lost its retained bound"))?;
        while let Some(partition) = partitions.pop() {
            self.jobs.check_cancelled()?;
            if self.jobs.is_full() {
                self.merge_next(states)?;
            }
            self.jobs.submit(
                NumericPartition::output_bytes(cap)? + size_of::<Partial>() as u64,
                move |worker, _| partition.reduce(cap, worker).map(Partial::NumericSelected),
            )?;
            self.selection_jobs += 1;
        }
        self.drain(states)?;
        self.jobs.check_cancelled()?;
        let evidence = self
            .numeric_partition_evidence
            .as_mut()
            .ok_or_else(|| failed("numeric selection lost its evidence"))?;
        if evidence.reduced_rows != evidence.rows {
            return Err(failed("numeric reduction lost input weight"));
        }
        evidence.finish_nanos = started.elapsed().as_nanos();
        states.complete_key_partition_group_count = Some(evidence.groups);
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
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
        if let Some(evidence) = self.partition_evidence.as_ref() {
            for (name, value) in [
                (
                    "partition_count",
                    string_count_partitions::PARTITIONS as u64,
                ),
                ("partition_complete_groups", evidence.groups as u64),
                ("partition_committed_rows", evidence.rows),
                ("partition_lock_wait_nanos", evidence.lock_wait_nanos),
                ("partition_reconcile_work_nanos", evidence.reconcile_nanos),
                ("partition_arrange_work_nanos", evidence.arrange_nanos),
                ("partition_selection_work_nanos", evidence.selection_nanos),
                (
                    "partition_equality_comparisons",
                    evidence.equality_comparisons,
                ),
                (
                    "partition_comparison_publish_calls",
                    evidence.comparison_publish_calls,
                ),
                (
                    "partition_entry_credit_claim_calls",
                    evidence.entry_credit_claim_calls,
                ),
                (
                    "partition_entry_credit_granted_entries",
                    evidence.entry_credit_granted_entries,
                ),
                (
                    "partition_entry_credit_return_calls",
                    evidence.entry_credit_return_calls,
                ),
                (
                    "partition_entry_credit_refunded_entries",
                    evidence.entry_credit_refunded_entries,
                ),
                (
                    "partition_entry_credit_wait_calls",
                    evidence.entry_credit_wait_calls,
                ),
                (
                    "partition_entry_credit_reserved_entries",
                    evidence.entry_credit_reserved_entries as u64,
                ),
                (
                    "partition_entry_credit_block_entries",
                    evidence.entry_credit_block_entries as u64,
                ),
                ("partition_native_handoffs", self.partition_handoffs),
                ("partition_selection_jobs", self.selection_jobs),
                ("partition_retry_jobs", self.retry_jobs),
            ] {
                object.insert(format!("aggregate_workers_{name}"), value.into());
            }
            object.insert("aggregate_workers_partition_entry_credit_scope".into(), "hard_distinct_limit_includes_committed_and_reserved_entries;consume_locally_publish_and_refund_at_block_or_partition_exit;wait_only_without_partition_guard_or_unused_credits;group_count_lower_bound_while_blocks_active_exact_after_drain_and_preserved_after_storage_release;final_outstanding_credits_zero;byte_reservations_remain_independent".into());
            object.insert("aggregate_workers_partition_comparison_scope".into(), "checked_local_matching_hash_byte_comparisons_summed_at_partition_or_refill_or_error_boundaries;includes_rechecks_after_credit_wait;no_per_key_shared_counter_update".into());
            if self.partition_handoffs == 0 {
                object.insert("candidate_groups".into(), self.partition_groups.into());
                object.insert(
                    "group_output_strategy".into(),
                    "complete_key_partition_exact_topk".into(),
                );
                object.insert(
                    "group_key_storage".into(),
                    "owned_utf8_complete_key_partitions".into(),
                );
                object.insert(
                    "aggregate_workers_new_global_strings".into(),
                    self.partition_groups.into(),
                );
            }
        }
        object.insert("aggregate_workers_scope".into(), "source_ordered_all_key_chunk_partials;parallel_canonicalization_and_exact_count;caller_global_merge;no_local_topk;sum_worker_elapsed_not_cpu_or_exclusive_wall;submit_includes_inline_work_and_memory_pressure_drains;shared_capacity_covers_native_allocator_and_owned_partials_not_legacy_global_maps_or_process_rss;source_generation_validated_after_drain_and_refinement".into());
        if self.partition_evidence.is_some() {
            object.insert("aggregate_workers_scope".into(), "all_key_chunk_counts_then_complete_key_partition_reconciliation_on_same_workers;partition_lock_wait_separate_from_work;final_partition_topk_only_after_source_drains;caller_bounded_candidate_union_or_explicit_native_pressure_handoff;sum_worker_elapsed_not_cpu_or_exclusive_wall;shared_capacity_covers_native_allocator_partials_partition_vectors_and_growth_overlap_not_legacy_handoff_or_output_maps_or_process_rss;source_generation_validated_after_drain_and_refinement".into());
        }
        if let Some(evidence) = &self.numeric_partition_evidence {
            object.insert(
                "aggregate_workers_integer_partition_rows".into(),
                evidence.rows.into(),
            );
            object.insert(
                "aggregate_workers_integer_partition_entries".into(),
                evidence.entries.into(),
            );
            object.insert(
                "aggregate_workers_integer_partition_capacity_peak_bytes".into(),
                evidence.capacity_peak.into(),
            );
            object.insert(
                "aggregate_workers_integer_partition_growth_overlap_peak_bytes".into(),
                evidence.growth_overlap_peak.into(),
            );
            object.insert(
                "aggregate_workers_integer_dictionary_handoff".into(),
                evidence.dictionary_handoff.into(),
            );
            if !evidence.dictionary_handoff {
                object.insert("candidate_groups".into(), evidence.groups.into());
                object.insert(
                    "group_output_strategy".into(),
                    "complete_weighted_integer_partition_topk".into(),
                );
                object.insert(
                    "group_state_mode".into(),
                    "complete_weighted_integer_partitions".into(),
                );
                object.insert(
                    "aggregate_workers_integer_partition_selection_jobs".into(),
                    self.selection_jobs.into(),
                );
                for (name, nanos) in [
                    ("sort", evidence.sort_nanos),
                    ("reduce", evidence.reduce_nanos),
                    ("finish", evidence.finish_nanos),
                ] {
                    object.insert(
                        format!("aggregate_workers_integer_partition_{name}_nanos"),
                        u64::try_from(nanos).unwrap_or(u64::MAX).into(),
                    );
                }
                object.insert("aggregate_workers_scope".into(), "native_all_key_chunk_counts_then_leased_complete_integer_partitions;caller_weighted_routing;parallel_complete_partition_sort_and_checked_reduction;bounded_candidate_union_after_EOF;worker_spans_overlap;source_chunk_counts_exclude_selection_jobs;capacity_covers_vectors_growth_overlap_and_task_results_not_legacy_output_maps_or_RSS;numeric_vector_failure_and_ordinary_source_pressure_cancel_join_fail_without_replay;native_Dict_transfers_all_entries_to_existing_state".into());
            }
        }
        *summary = payload.to_string();
        Ok(())
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) enum SourceScanTestFault {
    OwnedDenial,
    CorruptionWithConcurrentDenial,
}

#[cfg(test)]
thread_local! {
    pub(super) static SOURCE_SCAN_TEST_FAULT: std::cell::Cell<Option<SourceScanTestFault>> = const { std::cell::Cell::new(None) };
    // Scoped to the calling test thread and consumed once. The actual worker
    // reservation fails; the pressure lease refunds before the native scan.
    pub(super) static ADMISSION_TEST_PRESSURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn install_weighted_string(
    states: &mut GroupedAggregateStates<'_>,
    group_index: usize,
    value: &str,
    count: u64,
    sketch: bool,
) -> Result<()> {
    if sketch {
        let owned: Arc<str> = Arc::from(value);
        states
            .string_count_topk_heavy_hitter_sketch
            .as_mut()
            .ok_or_else(|| failed("native pressure sketch is absent"))?
            .update_lazy_utf8_value(&owned, count, &mut states.string_interner)?;
    } else {
        states
            .string_interner
            .reserve(1, "complete partition output identity")?;
        let id = states.string_interner.intern(value)?;
        if states.string_count_topk_heavy_hitter_enabled {
            let counts = states
                .string_count_topk_first_pass_exact_histogram_counts
                .as_mut()
                .ok_or_else(|| failed("complete partition output histogram is absent"))?;
            super::reserve_hash_map_capacity(counts, 1, "complete partition output counts")?;
            let previous = counts.entry(id).or_insert(0);
            *previous = previous
                .checked_add(count)
                .ok_or_else(|| failed("partition output count overflowed"))?;
        } else {
            let key =
                super::AggregateGroupKey::single(super::AggregateDistinctValue::Utf8Interned(id));
            super::reserve_hash_map_capacity(
                &mut states.groups,
                1,
                "complete partition output groups",
            )?;
            states
                .groups
                .entry(key)
                .or_insert_with(|| super::GroupedAggregateState::new_compact_count_star(None))
                .increment_count_star_by(count)?;
        }
    }
    states.count_star_direct_updates = true;
    if states.string_count_topk_heavy_hitter_enabled {
        states.string_count_topk_string_group_index = Some(group_index);
        states.string_count_topk_heavy_hitter_direct_updates = true;
    }
    states
        .aggregate_accessor_summary
        .insert("native_complete_key_partition_counts".into());
    Ok(())
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
        && states.group_key_indices.len() == 1
        && states.can_reconstruct_count_star_group_values_from_key()
        && (states.group_columns.len() > 1 || states.groups.is_empty())
        && (states.group_columns.len() == 1 || states.single_numeric_count_groups.is_none())
        && states.group_order.is_empty()
        && states.numeric_pair_compact_groups.is_none()
        && states.numeric_pair_late_measure_count_groups.is_none()
        && states.numeric_minute_string_count_groups.is_none()
        && states.group_key_indices.iter().all(|index| {
            states.group_columns.get(*index).is_some_and(|column| {
                column.extra_column_indices.is_empty()
                    && matches!(column.transform, AggregateValueTransform::Identity)
            })
        })
}

fn validate_numeric_group_reconstruction(
    states: &GroupedAggregateStates<'_>,
    key: &super::AggregateGroupKey,
) -> Result<()> {
    // These transforms depend only on the complete physical key. Checking one
    // weighted key therefore preserves errors without repeating work per row,
    // and cannot hide overflow in a group that loses top-K selection.
    for index in 0..states.group_columns.len() {
        if states.key_position_for_group_index(index).is_none() {
            states.reconstruct_group_value_from_key(key, index)?;
        }
    }
    Ok(())
}

fn merge_numeric_partial<K: NumericCountKey>(
    states: &mut GroupedAggregateStates<'_>,
    partial: &OwnedNumericCounts<K>,
    partitions: Option<&mut numeric_count_partitions::Partitions<K>>,
    check: impl Fn() -> Result<()>,
) -> Result<()> {
    let Some(partitions) = partitions else {
        return merge_numeric(
            states,
            partial
                .pairs()
                .iter()
                .map(|&(key, weight)| (key.aggregate_key(), weight)),
        );
    };
    if !numeric_state_admitted(states) {
        return Err(failed("numeric partition lost its key proof"));
    }
    // OwnedNumericCounts is sorted and contains only observed keys. The admitted
    // dependent expressions are constants or checked AddOffset of this identity
    // key. Their domain is an interval, so both extrema prove every group, even
    // losing groups. Validate minimum first with the existing diagnostic order.
    if let Some(&(key, _)) = partial.pairs().first() {
        validate_numeric_group_reconstruction(states, &key.aggregate_key().aggregate_group_key())?;
    }
    if let Some(&(key, _)) = partial.pairs().last() {
        validate_numeric_group_reconstruction(states, &key.aggregate_key().aggregate_group_key())?;
    }
    partitions.append(partial.pairs(), check)
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
    if states.group_columns.len() > 1 {
        // The existing generic COUNT renderer reconstructs all dependent output
        // columns and preserves its complete tie comparator. The single-numeric
        // renderer intentionally emits only an identity key and is not reused.
        super::reserve_hash_map_capacity(
            &mut states.groups,
            pairs.len(),
            "worker physical-key exact global count",
        )?;
        for (key, weight) in pairs {
            if weight == 0 {
                return Err(failed("numeric partial contains zero weight"));
            }
            let key = key.aggregate_group_key();
            validate_numeric_group_reconstruction(states, &key)?;
            states
                .groups
                .entry(key)
                .or_insert_with(|| super::GroupedAggregateState::new_compact_count_star(None))
                .increment_count_star_by(weight)?;
        }
        states.count_star_direct_updates = true;
        states
            .aggregate_accessor_summary
            .insert("native_integer_owned_all_key_count_partial".into());
        return Ok(());
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

#[allow(clippy::too_many_lines)]
fn count_numeric_chunk(
    array: &ArrayRef,
    session: &VortexSession,
    kind: KeyKind,
    worker: &ChunkWorkerContext,
    lease: &mut MemoryLease,
) -> Result<Partial> {
    worker.check_cancelled()?;
    let ptype = match kind {
        KeyKind::I32 => PType::I32,
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
        KeyKind::I32 => {
            let slice = values.as_slice::<i32>();
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
            Ok(Partial::Signed32(counts, work))
        }
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

#[cfg(test)]
#[path = "aggregate_count_physical_key_tests.rs"]
mod physical_key_tests;
