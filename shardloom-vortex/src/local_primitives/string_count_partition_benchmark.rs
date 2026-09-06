//! Test-only access to the actual retained complete-key reducer. No copied
//! probing, insertion, credit, or reconciliation implementation lives here.

use super::super::{
    aggregate_chunk_jobs::ChunkWorkerContext,
    compact_string_state::benchmark::{RetainedRun, WeightedText, nanos},
};
use super::{Slot, StringCountPartial, StringCountPartitions, failed};
use shardloom_core::Result;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{collections::BTreeMap, time::Instant};

pub(in super::super) fn run(input: &[WeightedText<'_>], memory_bytes: u64) -> Result<RetainedRun> {
    let source_memory = LiveMemoryPool::new(memory_bytes)?;
    let tuples = input
        .iter()
        .map(|entry| (entry.value, entry.hash, entry.count))
        .collect::<Vec<_>>();
    let partial = StringCountPartial::benchmark_weighted(&tuples, &source_memory)?;
    let mut task_lease = source_memory.reserve(StringCountPartial::deferred_metadata_bytes())?;
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let memory = LiveMemoryPool::new(memory_bytes)?;
    let started = Instant::now();
    // No selection storage is needed: every occupied slot is verified below.
    let partitions = StringCountPartitions::try_new(&memory, input.len(), 0)?
        .ok_or_else(|| failed("benchmark retained constructor not admitted"))?;
    let construction_nanos = nanos(started)?;
    let started = Instant::now();
    let receipt = partitions.reduce(partial, &worker, &mut task_lease)?;
    let kernel_nanos = nanos(started)?;
    if receipt.deferred.is_some() || partitions.pressure_requested() {
        return Err(failed("benchmark retained input not completely admitted"));
    }
    let evidence = partitions.evidence()?;
    let mut complete_groups = BTreeMap::new();
    let mut payload_bytes_copied = 0_u64;
    let mut table_owned_bytes = 0_u64;
    for partition in &partitions.partitions {
        let partition = partition
            .lock()
            .map_err(|_| failed("benchmark partition lock poisoned"))?;
        payload_bytes_copied = payload_bytes_copied
            .checked_add(partition.benchmark_payload_bytes_copied)
            .ok_or_else(|| failed("benchmark copy sum overflowed"))?;
        table_owned_bytes = table_owned_bytes
            .checked_add(partition.slots_lease.bytes())
            .and_then(|bytes| bytes.checked_add(partition.bytes_lease.bytes()))
            .ok_or_else(|| failed("benchmark table capacity sum overflowed"))?;
        for slot in &partition.slots {
            if slot.count == 0 {
                continue;
            }
            let value = std::str::from_utf8(&partition.bytes[slot.offset..slot.offset + slot.len])
                .map_err(|_| failed("benchmark retained UTF8 invalid"))?;
            if complete_groups
                .insert(value.to_owned(), slot.count)
                .is_some()
            {
                return Err(failed(
                    "benchmark duplicate complete group across partitions",
                ));
            }
        }
    }
    if complete_groups.len() != evidence.groups || evidence.entry_credit_reserved_entries != 0 {
        return Err(failed("benchmark complete groups or entry refunds differ"));
    }
    let snapshot = memory.snapshot();
    let started = Instant::now();
    drop(partitions);
    let drop_nanos = nanos(started)?;
    drop(receipt);
    drop(task_lease);
    if memory.snapshot().reserved_bytes != 0 || source_memory.snapshot().reserved_bytes != 0 {
        return Err(failed("benchmark retained owner did not refund all bytes"));
    }
    Ok(RetainedRun {
        complete_groups,
        construction_nanos,
        kernel_nanos,
        drop_nanos,
        owned_bytes: snapshot.reserved_bytes,
        peak_owned_bytes: snapshot.peak_reserved_bytes,
        table_owned_bytes,
        slot_bytes: size_of::<Slot>(),
        probes: None,
        equality_comparisons: evidence.equality_comparisons,
        payload_bytes_copied,
        rows: evidence.rows,
        arrange_nanos: evidence.arrange_nanos,
        reconcile_nanos: evidence.reconcile_nanos,
        credit_claims: evidence.entry_credit_claim_calls,
        credit_returns: evidence.entry_credit_return_calls,
        live_bytes_after_drop: memory.snapshot().reserved_bytes,
        entry_credits_outstanding: evidence.entry_credit_reserved_entries,
    })
}
