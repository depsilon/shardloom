//! One query-reserved weighted COUNT accumulator. No worker pool or additional
//! runtime is created. A returned owner retains the full parent reservation.

use super::AggregateIntegerKeyPart;
use super::{
    NativeNumericOwner, logical_field_from_native_array,
    native_numeric_accessor::NativeNumericAccessorWork,
    vortex_error,
    weighted_count_spill::{Policy, SpilledCountResult, WeightedCountSpill},
    weighted_count_spill_admission::{Contract, MAX_KEY_BYTES, failed},
    weighted_count_spill_intake::TextInput,
};
use crate::VortexAggregateSpillPolicy;
use shardloom_core::Result;
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
        ArrayRef, ExecutionCtx, VortexSessionExecute as _,
        arrays::{Primitive, PrimitiveArray},
        memory::MemorySessionExt as _,
    },
    io::runtime::BlockingRuntime,
    session::VortexSession,
};

#[derive(Default)]
pub(super) struct SourceWork {
    pub numeric: NativeNumericAccessorWork,
    pub source_batches: u64,
    pub dictionary_batches: u64,
    pub utf8_native_value_bytes: u64,
    pub drained_epochs: u64,
    pub committed_weight: u64,
    pub deferred_weight: u64,
    pub worker_jobs: u64,
    pub worker_peak_jobs: usize,
    pub workers_created: usize,
    pub fitted_partition_selection: bool,
}
pub(super) struct Accumulator {
    spill: WeightedCountSpill,
    operator_memory: LiveMemoryPool,
    run_session: VortexSession,
    source_ctx: ExecutionCtx,
    contract: Contract,
    cancellation: Arc<AtomicBool>,
    source_rows: u64,
    work: SourceWork,
    failed: bool,
    metadata: MemoryLease,
    // Parent credit drops after all child buffers/contexts and metadata.
    envelope: MemoryLease,
}
pub(super) struct OwnedResult {
    pub result: SpilledCountResult,
    pub contract: Contract,
    pub source_work: SourceWork,
    metadata: MemoryLease,
    envelope: MemoryLease,
}
impl OwnedResult {
    pub(super) fn reserved_bytes(&self) -> u64 {
        self.envelope.bytes()
    }
    pub(super) fn selected_reserved_bytes(&self) -> u64 {
        self.result.reserved_bytes() + self.metadata.bytes()
    }
}
type Visit<'a> = dyn FnMut(Option<AggregateIntegerKeyPart>, &str, u64) -> Result<()> + 'a;

impl Accumulator {
    pub(super) fn new(
        policy: &VortexAggregateSpillPolicy,
        contract: Contract,
        query_memory: &LiveMemoryPool,
        session: &VortexSession,
    ) -> Result<Self> {
        cancelled(&policy.cancellation)?;
        if policy.memory_bytes < 4 << 20
            || !policy.workspace.is_absolute()
            || policy.quota_bytes < 32 << 10
        {
            return Err(failed(
                "requires an absolute explicit workspace, 32 KiB quota and at least 4 MiB operator memory",
            ));
        }
        let workspace = std::fs::symlink_metadata(&policy.workspace)
            .map_err(|error| failed(&format!("workspace admission failed: {error}")))?;
        if !workspace.is_dir() || workspace.file_type().is_symlink() {
            return Err(failed("workspace must be an existing real directory"));
        }
        let envelope = query_memory.reserve(policy.memory_bytes)?;
        let operator_memory = LiveMemoryPool::new(policy.memory_bytes)?;
        let metadata_bytes = contract
            .metadata_bytes()?
            .checked_add(
                u64::try_from(
                    size_of::<Self>() + size_of::<OwnedResult>() + 2 * size_of::<usize>(),
                )
                .map_err(|_| failed("owner metadata exceeds u64"))?,
            )
            .ok_or_else(|| failed("owner metadata overflowed"))?;
        let metadata = operator_memory.reserve(metadata_bytes)?;
        let run_session = session.clone().with_allocator(Arc::new(
            crate::owned_buffers::ReservedHostAllocator::new(operator_memory.clone()),
        ));
        let spill = WeightedCountSpill::new(
            Policy {
                workspace: policy.workspace.clone(),
                quota_bytes: policy.quota_bytes,
                memory_bytes: policy.memory_bytes,
                max_key_bytes: MAX_KEY_BYTES,
                cancellation: Arc::clone(&policy.cancellation),
            },
            operator_memory.clone(),
            contract.order,
            contract.retained,
        )?;
        Ok(Self {
            spill,
            operator_memory,
            run_session,
            source_ctx: session.create_execution_ctx(),
            contract,
            cancellation: Arc::clone(&policy.cancellation),
            source_rows: 0,
            work: SourceWork::default(),
            failed: false,
            metadata,
            envelope,
        })
    }
    fn check(&self) -> Result<()> {
        cancelled(&self.cancellation)?;
        if self.failed {
            Err(failed(
                "failed accumulator cannot accept or publish a source prefix",
            ))
        } else {
            Ok(())
        }
    }
    pub(super) fn worker_memory(&self) -> &LiveMemoryPool {
        &self.operator_memory
    }
    pub(super) fn worker_contract(&self) -> &Contract {
        &self.contract
    }
    pub(super) fn worker_session(&self) -> &VortexSession {
        &self.run_session
    }
    pub(super) fn record_workers(
        &mut self,
        jobs: u64,
        peak: usize,
        created: usize,
        batches: u64,
        dictionaries: u64,
    ) -> Result<()> {
        self.check()?;
        self.work.worker_jobs = jobs;
        self.work.worker_peak_jobs = peak;
        self.work.workers_created = created;
        self.work.source_batches = self
            .work
            .source_batches
            .checked_add(batches)
            .ok_or_else(|| failed("worker source batches overflowed"))?;
        self.work.dictionary_batches = self
            .work
            .dictionary_batches
            .checked_add(dictionaries)
            .ok_or_else(|| failed("worker dictionary batches overflowed"))?;
        Ok(())
    }
    pub(super) fn push_source(
        &mut self,
        chunk: &ArrayRef,
        runtime: &impl BlockingRuntime,
    ) -> Result<()> {
        let result = self.push_source_inner(chunk, runtime);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    /// An admitted worker could not finish native canonicalization. No partial
    /// from this immutable UTF8 leaf was committed; consume it once after the
    /// drained epoch releases its partition/partial owners.
    pub(super) fn push_untouched_text(
        &mut self,
        array: &ArrayRef,
        runtime: &impl BlockingRuntime,
    ) -> Result<()> {
        let result = (|| {
            self.check()?;
            if self.contract.numeric_index.is_some()
                || self.contract.dtypes.get(self.contract.text_index) != Some(array.dtype())
            {
                return Err(failed("untouched worker leaf changed its UTF8 contract"));
            }
            let text = TextInput::new(array, &mut self.source_ctx)?;
            text.visit(None, |row, key, value| {
                if row.is_multiple_of(4096) {
                    cancelled(&self.cancellation)?;
                }
                self.spill.push(key, value, 1, runtime, &self.run_session)
            })?;
            self.source_rows = self
                .source_rows
                .checked_add(array.len() as u64)
                .ok_or_else(|| failed("untouched worker rows overflowed"))?;
            self.work.source_batches = self
                .work
                .source_batches
                .checked_add(1)
                .ok_or_else(|| failed("untouched worker batches overflowed"))?;
            self.work.dictionary_batches = self
                .work
                .dictionary_batches
                .checked_add(u64::from(text.dictionary()))
                .ok_or_else(|| failed("untouched dictionary batches overflowed"))?;
            self.work.utf8_native_value_bytes = self
                .work
                .utf8_native_value_bytes
                .checked_add(text.native_value_bytes())
                .ok_or_else(|| failed("untouched native UTF8 bytes overflowed"))?;
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn source_column(&self, chunk: &ArrayRef, index: usize) -> Result<ArrayRef> {
        let name = self
            .contract
            .groups
            .get(index)
            .ok_or_else(|| failed("source role index changed"))?;
        let dtype = self
            .contract
            .dtypes
            .get(index)
            .ok_or_else(|| failed("source role dtype changed"))?;
        let array = logical_field_from_native_array(chunk, name)?;
        if array.dtype() != dtype || array.len() != chunk.len() {
            return Err(failed(
                "source projection changed admitted dtype or row count",
            ));
        }
        Ok(array)
    }
    fn push_source_inner(
        &mut self,
        chunk: &ArrayRef,
        runtime: &impl BlockingRuntime,
    ) -> Result<()> {
        self.check()?;
        let text = self.source_column(chunk, self.contract.text_index)?;
        let numeric = self
            .contract
            .numeric_index
            .map(|index| -> Result<_> {
                let array = self.source_column(chunk, index)?;
                let started = Instant::now();
                let primitive = array
                    .clone()
                    .execute::<PrimitiveArray>(&mut self.source_ctx)
                    .map_err(vortex_error)?;
                if primitive.dtype() != array.dtype() || primitive.len() != array.len() {
                    return Err(failed("native integer execution changed admitted shape"));
                }
                let canonical_bytes = primitive.nbytes();
                let owner = NativeNumericOwner::new(primitive, &mut self.source_ctx)?;
                if !array.is::<Primitive>() {
                    self.work.numeric.record_native_owner(
                        &self.contract.groups[index],
                        u64::try_from(array.len()).map_err(|_| failed("source rows exceed u64"))?,
                        array.nbytes(),
                        canonical_bytes,
                        started.elapsed().as_nanos(),
                    )?;
                }
                Ok(owner)
            })
            .transpose()?;
        let text = TextInput::new(&text, &mut self.source_ctx)?;
        text.visit(numeric.as_ref(), |row, key, value| {
            if row.is_multiple_of(4096) {
                cancelled(&self.cancellation)?;
            }
            self.spill.push(key, value, 1, runtime, &self.run_session)
        })?;
        let count = u64::try_from(chunk.len()).map_err(|_| failed("source rows exceed u64"))?;
        self.source_rows = self
            .source_rows
            .checked_add(count)
            .ok_or_else(|| failed("source rows overflowed"))?;
        self.work.source_batches = self
            .work
            .source_batches
            .checked_add(1)
            .ok_or_else(|| failed("source batch counter overflowed"))?;
        self.work.dictionary_batches = self
            .work
            .dictionary_batches
            .checked_add(u64::from(text.dictionary()))
            .ok_or_else(|| failed("dictionary batch counter overflowed"))?;
        self.work.utf8_native_value_bytes = self
            .work
            .utf8_native_value_bytes
            .checked_add(text.native_value_bytes())
            .ok_or_else(|| failed("native UTF8 byte counter overflowed"))?;
        Ok(())
    }

    /// The caller has stopped admission and joined all epoch jobs. Each visitor
    /// must enumerate complete keys, never selected/top-K output. The exact
    /// combined weight is verified before source input can resume. Any visitor
    /// failure is terminal, even if earlier partitions released their credits.
    pub(super) fn transfer_drained_epoch(
        &mut self,
        expected_rows: u64,
        committed: impl FnOnce(&mut Visit<'_>) -> Result<()>,
        deferred: impl FnOnce(&mut Visit<'_>) -> Result<()>,
        runtime: &impl BlockingRuntime,
    ) -> Result<()> {
        let result = self.transfer_inner(expected_rows, committed, deferred, runtime);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn transfer_inner(
        &mut self,
        expected_rows: u64,
        committed: impl FnOnce(&mut Visit<'_>) -> Result<()>,
        deferred: impl FnOnce(&mut Visit<'_>) -> Result<()>,
        runtime: &impl BlockingRuntime,
    ) -> Result<()> {
        self.check()?;
        let mut prefix = 0_u64;
        let mut suffix = 0_u64;
        committed(&mut |key, text, weight| {
            self.spill
                .push(key, text, weight, runtime, &self.run_session)?;
            prefix = prefix
                .checked_add(weight)
                .ok_or_else(|| failed("committed epoch weight overflowed"))?;
            Ok(())
        })?;
        deferred(&mut |key, text, weight| {
            self.spill
                .push(key, text, weight, runtime, &self.run_session)?;
            suffix = suffix
                .checked_add(weight)
                .ok_or_else(|| failed("deferred epoch weight overflowed"))?;
            Ok(())
        })?;
        if prefix.checked_add(suffix) != Some(expected_rows) {
            return Err(failed(
                "drained committed prefix plus untouched suffix differs from source epoch weight",
            ));
        }
        self.source_rows = self
            .source_rows
            .checked_add(expected_rows)
            .ok_or_else(|| failed("source rows overflowed"))?;
        self.work.drained_epochs = self
            .work
            .drained_epochs
            .checked_add(1)
            .ok_or_else(|| failed("drained epoch counter overflowed"))?;
        self.work.committed_weight = self
            .work
            .committed_weight
            .checked_add(prefix)
            .ok_or_else(|| failed("committed weight counter overflowed"))?;
        self.work.deferred_weight = self
            .work
            .deferred_weight
            .checked_add(suffix)
            .ok_or_else(|| failed("deferred weight counter overflowed"))?;
        Ok(())
    }
    pub(super) fn finish_fitted(
        mut self,
        expected_rows: u64,
        groups: u64,
        visit: impl FnOnce(&mut dyn FnMut(&str, u64) -> Result<()>) -> Result<()>,
    ) -> Result<OwnedResult> {
        self.check()?;
        if self.source_rows != 0 || self.contract.numeric_index.is_some() {
            return Err(failed(
                "fitted finalization requires one untouched UTF8 source epoch",
            ));
        }
        let result = self.spill.finish_fitted(expected_rows, groups, visit)?;
        self.work.fitted_partition_selection = true;
        if self.operator_memory.snapshot().reserved_bytes > self.envelope.bytes() {
            return Err(failed(
                "fitted child credits exceed retained parent envelope",
            ));
        }
        Ok(OwnedResult {
            result,
            contract: self.contract,
            source_work: self.work,
            metadata: self.metadata,
            envelope: self.envelope,
        })
    }
    pub(super) fn finish(self, runtime: &impl BlockingRuntime) -> Result<OwnedResult> {
        self.check()?;
        let result = self.spill.finish(runtime, &self.run_session)?;
        if result.evidence.source_weight != self.source_rows {
            return Err(failed(
                "final complete-key weights differ from accepted source rows",
            ));
        }
        if self.operator_memory.snapshot().reserved_bytes > self.envelope.bytes() {
            return Err(failed("child credits exceed retained parent envelope"));
        }
        Ok(OwnedResult {
            result,
            contract: self.contract,
            source_work: self.work,
            metadata: self.metadata,
            envelope: self.envelope,
        })
    }
}
fn cancelled(cancellation: &AtomicBool) -> Result<()> {
    if cancellation.load(Ordering::Acquire) {
        Err(failed("execution cancelled"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
#[path = "weighted_count_spill_accumulator_tests.rs"]
mod tests;
