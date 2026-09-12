//! Explicit-spill adapter for the retained all-key string partials, partitions
//! and AggregateChunkJobs. It owns no additional scheduler or partition format.
//! Source and spill continue on the same caller-driven native runtime.

use super::super::{
    aggregate_chunk_jobs::{AggregateChunkJobs, SubmitOutcome},
    logical_field_from_native_array,
    string_count_partial::{self, CountOutcome, StringCountPartial},
    string_count_partitions::{PARTITIONS, PartitionReceipt, StringCountPartitions},
    weighted_count_spill_accumulator::Accumulator,
    weighted_count_spill_admission::{Contract, MAX_KEY_BYTES, failed},
};
use shardloom_core::Result;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use vortex::{
    array::{ArrayRef, VortexSessionExecute as _},
    io::runtime::BlockingRuntime,
    session::VortexSession,
};

enum Outcome {
    Counted(PartitionReceipt),
    Untouched(ArrayRef),
}

pub(super) struct Workers {
    jobs: AggregateChunkJobs<Outcome>,
    partitions: Arc<StringCountPartitions>,
    deferred: Vec<Arc<StringCountPartial>>,
    retries: Vec<ArrayRef>,
    session: VortexSession,
    column: String,
    accepted_rows: u64,
    counted_rows: u64,
    batches: u64,
    dictionaries: u64,
    created: usize,
    cancellation: Arc<AtomicBool>,
    _handoff: MemoryLease,
    #[cfg(test)]
    deny_next_initial: bool,
}

impl Workers {
    pub(super) fn try_new(
        contract: &Contract,
        parallelism: usize,
        entry_limit: usize,
        memory: &LiveMemoryPool,
        session: &VortexSession,
        cancellation: Arc<AtomicBool>,
    ) -> Result<Option<Self>> {
        if contract.numeric_index.is_some() || contract.groups.len() != 1 {
            return Ok(None);
        }
        let parallelism = parallelism
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        let window = parallelism.saturating_mul(2).clamp(1, 24);
        let bytes = size_of::<Self>()
            .checked_add(contract.groups[0].len())
            .and_then(|bytes| {
                bytes.checked_add(
                    window * (size_of::<Arc<StringCountPartial>>() + size_of::<ArrayRef>()),
                )
            })
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| failed("worker handoff capacity overflowed"))?;
        let Ok(handoff) = memory.reserve(bytes) else {
            return Ok(None);
        };
        let Some(partitions) =
            StringCountPartitions::try_new(memory, entry_limit, contract.retained)?
        else {
            return Ok(None);
        };
        let mut deferred = Vec::new();
        let mut retries = Vec::new();
        deferred
            .try_reserve_exact(window)
            .map_err(|_| failed("deferred capacity allocation failed"))?;
        retries
            .try_reserve_exact(window)
            .map_err(|_| failed("retry capacity allocation failed"))?;
        if deferred.capacity() > window || retries.capacity() > window {
            return Err(failed("worker handoff allocation exceeds reserved window"));
        }
        let jobs = AggregateChunkJobs::with_cancellation(
            parallelism,
            window,
            memory.snapshot().limit_bytes,
            memory.clone(),
            CancellationToken::from_shared_flag(Arc::clone(&cancellation)),
        )?;
        let created = jobs
            .pool_snapshot()
            .map_or(0, |snapshot| snapshot.workers_created);
        Ok(Some(Self {
            jobs,
            partitions,
            deferred,
            retries,
            session: session.clone(),
            column: contract.groups[0].clone(),
            accepted_rows: 0,
            counted_rows: 0,
            batches: 0,
            dictionaries: 0,
            created,
            cancellation,
            _handoff: handoff,
            #[cfg(test)]
            deny_next_initial: false,
        }))
    }

    pub(super) fn pressured(&self) -> bool {
        self.partitions.pressure_requested()
    }

    pub(super) fn before_next(&mut self) -> Result<()> {
        if self.jobs.is_full() {
            self.join_next()?;
        }
        Ok(())
    }

    /// False means this source chunk has not been admitted or counted at all.
    /// The caller drains this epoch before passing that chunk to native runs.
    pub(super) fn submit(&mut self, chunk: &ArrayRef) -> Result<bool> {
        let array = logical_field_from_native_array(chunk, &self.column)?;
        let bytes = string_count_partial::partial_bytes(&array)?
            .checked_add((size_of::<Outcome>() + size_of::<ArrayRef>()) as u64)
            .and_then(|bytes| bytes.checked_add(StringCountPartial::deferred_metadata_bytes()))
            .ok_or_else(|| failed("count worker input capacity overflowed"))?;
        while self.jobs.outstanding() > 0 && self.available() < bytes {
            self.join_next()?;
        }
        if self.pressured() || self.available() < bytes {
            self.partitions.request_pressure();
            return Ok(false);
        }
        let session = self.session.clone();
        let partitions = Arc::clone(&self.partitions);
        #[cfg(test)]
        let race = if std::mem::take(&mut self.deny_next_initial) {
            Some(self.jobs.memory().reserve(self.available())?)
        } else {
            None
        };
        let submitted = self.jobs.try_submit(bytes, move |worker, lease| {
            let partial = match string_count_partial::count_admitted(
                &array,
                session.create_execution_ctx(),
                worker,
                lease,
            )? {
                CountOutcome::Counted(partial) => partial,
                CountOutcome::OwnedAllocationDenied(_error) => {
                    worker.check_cancelled()?;
                    partitions.request_pressure();
                    return Ok(Outcome::Untouched(array));
                }
            };
            // Validate every referenced key, including keys outside final K.
            let mut inspected = 0_usize;
            partial.for_each_count(|text, _| {
                if inspected.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                inspected += 1;
                if text.len() > MAX_KEY_BYTES {
                    return Err(failed("worker UTF8 key exceeds admitted byte bound"));
                }
                Ok(())
            })?;
            partitions
                .reduce(partial, worker, lease)
                .map(Outcome::Counted)
        });
        #[cfg(test)]
        drop(race);
        match submitted? {
            SubmitOutcome::Submitted(_) => {
                self.accepted_rows = self
                    .accepted_rows
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| failed("worker accepted rows overflowed"))?;
                Ok(true)
            }
            SubmitOutcome::InitialCapacityDenied(_) => {
                self.partitions.request_pressure();
                Ok(false)
            }
        }
    }

    fn available(&self) -> u64 {
        let snapshot = self.jobs.memory().snapshot();
        snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes)
    }

    fn join_next(&mut self) -> Result<()> {
        let expected = self.jobs.joined();
        let Some(completed) = self.jobs.join_next()? else {
            return Ok(());
        };
        if completed.ordinal() != expected {
            return Err(failed("weighted COUNT worker receipt lost source order"));
        }
        completed.consume(|outcome| {
            match outcome {
                Outcome::Counted(receipt) => {
                    if let Some(partial) = &receipt.deferred {
                        if self.deferred.len() == self.deferred.capacity() {
                            return Err(failed(
                                "weighted COUNT deferred owners exceed source window",
                            ));
                        }
                        self.deferred.push(Arc::clone(partial));
                    }
                    self.counted_rows = self
                        .counted_rows
                        .checked_add(receipt.work.rows)
                        .ok_or_else(|| failed("worker counted rows overflowed"))?;
                    self.batches = self
                        .batches
                        .checked_add(1)
                        .ok_or_else(|| failed("worker batch count overflowed"))?;
                    self.dictionaries = self
                        .dictionaries
                        .checked_add(u64::from(receipt.work.native_dictionary))
                        .ok_or_else(|| failed("worker dictionary count overflowed"))?;
                }
                Outcome::Untouched(array) => {
                    if self.retries.len() == self.retries.capacity() {
                        return Err(failed("weighted COUNT retry owners exceed source window"));
                    }
                    self.retries.push(array.clone());
                }
            }
            Ok(())
        })
    }

    pub(super) fn drain(&mut self) -> Result<()> {
        while self.jobs.outstanding() != 0 {
            self.join_next()?;
        }
        let retries = self.retries.iter().try_fold(0_u64, |rows, array| {
            rows.checked_add(array.len() as u64)
                .ok_or_else(|| failed("retry rows overflowed"))
        })?;
        if self.counted_rows.checked_add(retries) != Some(self.accepted_rows) {
            return Err(failed(
                "worker completed and untouched rows differ from admission",
            ));
        }
        // Drained evidence rejects outstanding partition entry credits.
        self.partitions.evidence()?;
        Ok(())
    }

    pub(super) fn stop_for_transfer(&mut self) -> Result<()> {
        self.partitions.request_pressure();
        self.drain()?;
        self.jobs.retire()
    }

    pub(super) fn record(&self, accumulator: &mut Accumulator) -> Result<()> {
        if self.jobs.joined() != self.jobs.submitted() {
            return Err(failed(
                "worker evidence requires every source receipt to be joined",
            ));
        }
        accumulator.record_workers(
            self.jobs.submitted(),
            self.jobs.peak_outstanding(),
            self.created,
            self.batches,
            self.dictionaries,
        )
    }

    /// Workers are already joined and retired. The reserved run scratch can now
    /// coexist with one visited partition and every untouched suffix owner.
    pub(super) fn transfer(
        mut self,
        accumulator: &mut Accumulator,
        runtime: &impl BlockingRuntime,
    ) -> Result<()> {
        self.record(accumulator)?;
        accumulator.transfer_drained_epoch(
            self.counted_rows,
            |visit| {
                self.partitions
                    .replay_and_release(|text, count| visit(None, text, count))
            },
            |visit| {
                for partial in self.deferred.drain(..) {
                    partial.for_each_count(|text, count| visit(None, text, count))?;
                }
                Ok(())
            },
            runtime,
        )?;
        for array in self.retries.drain(..) {
            accumulator.push_untouched_text(&array, runtime)?;
        }
        Ok(())
    }

    pub(super) fn fitted_rows_and_groups(&self) -> Result<(u64, u64)> {
        if self.pressured()
            || !self.deferred.is_empty()
            || !self.retries.is_empty()
            || self.jobs.outstanding() != 0
            || self.partitions.committed_rows.load(Ordering::Acquire) != self.accepted_rows
        {
            return Err(failed(
                "fitted finalization requires the complete drained epoch",
            ));
        }
        Ok((self.accepted_rows, self.partitions.group_count() as u64))
    }

    /// Select only after EOF, using the existing exact per-partition heaps. The
    /// caller's pre-reserved global heap receives at most PARTITIONS * K keys.
    pub(super) fn select_fitted(
        &mut self,
        visit: &mut dyn FnMut(&str, u64) -> Result<()>,
    ) -> Result<()> {
        self.fitted_rows_and_groups()?;
        // No new pool: these are the retained exact partition selectors. Running
        // on the caller also avoids retaining another set of completed payloads.
        let token = CancellationToken::from_shared_flag(Arc::clone(&self.cancellation));
        let worker = super::super::aggregate_chunk_jobs::ChunkWorkerContext::Inline(token);
        for index in 0..PARTITIONS {
            let selected = self.partitions.select(index, &worker)?;
            self.partitions.visit_selection(&selected, &mut *visit)?;
        }
        self.partitions.release_storage()?;
        self.jobs.retire()
    }
}

#[cfg(test)]
#[path = "weighted_count_spill_workers_tests.rs"]
mod tests;
