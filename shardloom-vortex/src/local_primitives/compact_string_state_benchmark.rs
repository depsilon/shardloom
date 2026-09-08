//! Actual retained reducer versus the isolated compact table, under the same
//! 64-way hash partitioning and finite global entry/owned-byte limits.
//! This is a single-caller reducer-lifecycle/adapter experiment, not a pure
//! table-kernel comparison or public query latency. Native
//! input preparation, hashing, the oracle and full result export are untimed.
//! Retained reduce includes its production routing/locking/evidence work;
//! candidate routing is separately timed. Neither side performs top-K.

use super::super::{
    string_count_entry_credits::{Claim, EntryBlock, EntryCredits},
    string_count_partial::partition_index,
    string_count_partitions::{PARTITIONS, benchmark as retained},
};
use super::{Admission, CompactCountWork, CompactStringCounts, failed};
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::{collections::BTreeMap, sync::Mutex, time::Instant};

#[derive(Clone, Copy)]
pub(crate) struct WeightedText<'a> {
    pub(crate) value: &'a str,
    pub(crate) hash: u64,
    pub(crate) count: u64,
}

#[derive(Default)]
pub(crate) struct RetainedRun {
    pub(crate) complete_groups: BTreeMap<String, u64>,
    pub(crate) construction_nanos: u64,
    pub(crate) kernel_nanos: u64,
    pub(crate) drop_nanos: u64,
    pub(crate) owned_bytes: u64,
    pub(crate) peak_owned_bytes: u64,
    pub(crate) table_owned_bytes: u64,
    pub(crate) slot_bytes: usize,
    pub(crate) probes: Option<u64>,
    pub(crate) equality_comparisons: u64,
    pub(crate) payload_bytes_copied: u64,
    pub(crate) rows: u64,
    pub(crate) arrange_nanos: u64,
    pub(crate) reconcile_nanos: u64,
    pub(crate) credit_claims: u64,
    pub(crate) credit_returns: u64,
    pub(crate) live_bytes_after_drop: u64,
    pub(crate) entry_credits_outstanding: usize,
}

pub(crate) struct CandidateRun {
    // Match the retained export lifetime across both cases in either order.
    pub(crate) complete_groups: BTreeMap<String, u64>,
    pub(crate) construction_nanos: u64,
    pub(crate) kernel_nanos: u64,
    pub(crate) arrange_nanos: u64,
    pub(crate) drop_nanos: u64,
    pub(crate) work: CompactCountWork,
    pub(crate) owned_bytes: u64,
    pub(crate) peak_owned_bytes: u64,
    pub(crate) credit_claims: u64,
    pub(crate) credit_returns: u64,
    pub(crate) live_bytes_after_drop: u64,
    pub(crate) entry_credits_outstanding: usize,
}

pub(crate) struct PairedRun {
    pub(crate) order: PairOrder,
    pub(crate) retained_source: String,
    pub(crate) retained: RetainedRun,
    pub(crate) candidate: CandidateRun,
    pub(crate) complete_values_verified: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PairOrder {
    RetainedFirst,
    CandidateFirst,
}

pub(crate) fn compare_retained(
    input: &[WeightedText<'_>],
    retained_source: &str,
    memory_bytes: u64,
    slab_bytes: usize,
    order: PairOrder,
    retained: impl FnOnce(&[WeightedText<'_>]) -> Result<RetainedRun>,
) -> Result<PairedRun> {
    if retained_source.is_empty() {
        return Err(failed("paired control source identity is required"));
    }
    let oracle = oracle(input)?;
    let (control, candidate) = match order {
        PairOrder::RetainedFirst => (
            retained(input)?,
            run_candidate(input, &oracle, memory_bytes, slab_bytes)?,
        ),
        PairOrder::CandidateFirst => {
            let candidate = run_candidate(input, &oracle, memory_bytes, slab_bytes)?;
            (retained(input)?, candidate)
        }
    };
    if control.complete_groups != oracle || candidate.complete_groups != oracle {
        return Err(failed("complete-group mismatch"));
    }
    if control.live_bytes_after_drop != 0 || control.entry_credits_outstanding != 0 {
        return Err(failed(
            "retained owners or entry credits remain outstanding",
        ));
    }
    Ok(PairedRun {
        order,
        retained_source: retained_source.to_owned(),
        retained: control,
        candidate,
        complete_values_verified: true,
    })
}

fn oracle(input: &[WeightedText<'_>]) -> Result<BTreeMap<String, u64>> {
    let mut checked = BTreeMap::<String, (u64, u64)>::new();
    for entry in input {
        if entry.count == 0 {
            return Err(failed("zero-weight benchmark input"));
        }
        let (hash, count) = checked
            .entry(entry.value.to_owned())
            .or_insert((entry.hash, 0));
        if *hash != entry.hash {
            return Err(failed("equal benchmark values have inconsistent hashes"));
        }
        *count = count
            .checked_add(entry.count)
            .ok_or_else(|| failed("oracle count overflow"))?;
    }
    Ok(checked
        .into_iter()
        .map(|(key, (_, count))| (key, count))
        .collect())
}

fn run_candidate(
    input: &[WeightedText<'_>],
    oracle: &BTreeMap<String, u64>,
    memory_bytes: u64,
    slab_bytes: usize,
) -> Result<CandidateRun> {
    let started = Instant::now();
    let mut routed: [Vec<WeightedText<'_>>; PARTITIONS] = std::array::from_fn(|_| Vec::new());
    for entry in input {
        routed[partition_index::<PARTITIONS>(entry.hash)].push(*entry);
    }
    let arrange_nanos = nanos(started)?;
    let memory = LiveMemoryPool::new(memory_bytes)?;
    let started = Instant::now();
    let credits = EntryCredits::new(input.len());
    let metadata_bytes = PARTITIONS
        .checked_mul(size_of::<Mutex<CompactStringCounts>>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| failed("candidate metadata size overflow"))?;
    let metadata = memory.reserve(metadata_bytes)?;
    let mut tables = Vec::new();
    tables
        .try_reserve_exact(PARTITIONS)
        .map_err(|_| failed("candidate partition allocation failed"))?;
    if tables.capacity() != PARTITIONS {
        return Err(failed("candidate partition capacity exceeds reservation"));
    }
    for _ in 0..PARTITIONS {
        tables.push(Mutex::new(CompactStringCounts::new(&memory, slab_bytes)?));
    }
    let construction_nanos = nanos(started)?;
    let started = Instant::now();
    for (entries, table) in routed.iter().zip(&tables) {
        let mut table = table
            .lock()
            .map_err(|_| failed("candidate partition lock poisoned"))?;
        candidate_updates(entries, &mut table, &credits)?;
    }
    let kernel_nanos = nanos(started)?;
    let mut values = BTreeMap::new();
    let mut work = CompactCountWork::default();
    for table in &tables {
        let table = table
            .lock()
            .map_err(|_| failed("candidate partition lock poisoned"))?;
        add_work(&mut work, table.evidence())?;
        for (key, count) in table.entries() {
            if values.insert(key.to_owned(), count).is_some() {
                return Err(failed("duplicate candidate complete group"));
            }
        }
    }
    if &values != oracle {
        return Err(failed("candidate complete-group mismatch"));
    }
    let credit = credits.evidence()?;
    if credit.reserved != 0 || credit.committed != oracle.len() {
        return Err(failed("candidate entry credit mismatch"));
    }
    let snapshot = memory.snapshot();
    let started = Instant::now();
    drop(tables);
    drop(metadata);
    let drop_nanos = nanos(started)?;
    if memory.snapshot().reserved_bytes != 0 {
        return Err(failed("candidate retained bytes after drop"));
    }
    Ok(CandidateRun {
        complete_groups: values,
        construction_nanos,
        kernel_nanos,
        arrange_nanos,
        drop_nanos,
        work,
        owned_bytes: snapshot.reserved_bytes,
        peak_owned_bytes: snapshot.peak_reserved_bytes,
        credit_claims: credit.claim_calls,
        credit_returns: credit.return_calls,
        live_bytes_after_drop: memory.snapshot().reserved_bytes,
        entry_credits_outstanding: credit.reserved,
    })
}

fn candidate_updates(
    input: &[WeightedText<'_>],
    table: &mut CompactStringCounts,
    credits: &EntryCredits,
) -> Result<()> {
    let mut block: Option<EntryBlock<'_>> = None;
    for (index, entry) in input.iter().enumerate() {
        if block.as_ref().is_none_or(|block| block.remaining() == 0) {
            drop(block.take());
            let Claim::Block(granted) = credits.claim(input.len() - index, || Ok(true))? else {
                return Err(failed("candidate entry limit reached; no paired result"));
            };
            block = Some(granted);
        }
        let before = table.groups;
        if table.update(entry.value, entry.hash, entry.count, || Ok(()))? != Admission::Ready(()) {
            return Err(failed(
                "candidate not admitted; no paired performance result",
            ));
        }
        if table.groups != before {
            block
                .as_mut()
                .ok_or_else(|| failed("candidate insertion without entry credits"))?
                .consume_one()?;
        }
    }
    Ok(())
}

fn add_work(total: &mut CompactCountWork, value: CompactCountWork) -> Result<()> {
    macro_rules! add { ($($field:ident),+) => { $(total.$field = total.$field.checked_add(value.$field)
        .ok_or_else(|| failed("candidate work sum overflow"))?;)+ }; }
    add!(
        rows,
        updates,
        probes,
        full_hash_comparisons,
        rehashed_slots,
        payload_bytes_copied,
        directory_owner_moves,
        slabs,
        groups,
        owned_bytes
    );
    total.slot_bytes = value.slot_bytes;
    Ok(())
}

pub(crate) fn nanos(started: Instant) -> Result<u64> {
    u64::try_from(started.elapsed().as_nanos()).map_err(|_| failed("elapsed overflow"))
}

#[test]
fn hook_rejects_incomplete_control_instead_of_scoring_only_local_winners() {
    let input = [
        WeightedText {
            value: "a",
            hash: 0,
            count: 2,
        },
        WeightedText {
            value: "b",
            hash: 0,
            count: 3,
        },
    ];
    let result = compare_retained(
        &input,
        "synthetic negative hook test",
        1 << 20,
        64,
        PairOrder::RetainedFirst,
        |_| {
            Ok(RetainedRun {
                complete_groups: BTreeMap::from([("b".to_owned(), 3)]),
                ..RetainedRun::default()
            })
        },
    );
    assert!(result.is_err());
}

#[test]
fn actual_retained_reducer_matches_compact_collisions_weights_and_refunds() {
    let input = [
        WeightedText {
            value: "λ\0東京",
            hash: 0,
            count: 2,
        },
        WeightedText {
            value: "",
            hash: 0,
            count: 3,
        },
        WeightedText {
            value: "λ\0東京",
            hash: 0,
            count: 9,
        },
        WeightedText {
            value: "different",
            hash: 0,
            count: 7,
        },
    ];
    for order in [PairOrder::RetainedFirst, PairOrder::CandidateFirst] {
        let report = compare_retained(
            &input,
            "actual retained reducer correctness test",
            1 << 20,
            64,
            order,
            |entries| retained::run(entries, 1 << 20),
        )
        .unwrap();
        assert_eq!(report.order, order);
        assert!(report.complete_values_verified);
        assert_eq!(report.retained.complete_groups["λ\0東京"], 11);
        assert_eq!(report.retained.complete_groups[""], 3);
        assert_eq!(report.retained.rows, 21);
        assert_eq!(report.candidate.work.rows, 21);
        assert_eq!(report.retained.probes, None);
        assert!(report.retained.equality_comparisons > 0);
        assert_eq!(
            report.retained.payload_bytes_copied,
            report.candidate.work.payload_bytes_copied
        );
        assert!(report.retained.owned_bytes >= report.retained.table_owned_bytes);
        assert_eq!(report.retained.live_bytes_after_drop, 0);
        assert_eq!(report.candidate.live_bytes_after_drop, 0);
        assert_eq!(report.retained.entry_credits_outstanding, 0);
        assert_eq!(report.candidate.entry_credits_outstanding, 0);
    }
}

#[test]
fn actual_retained_copy_counter_includes_arena_relocations_and_rejects_pressure() {
    let values = (0..128)
        .map(|index| format!("long-unicode-東京-{index:06}-abcdefghijklmnopqrstuvwxyz"))
        .collect::<Vec<_>>();
    let input = values
        .iter()
        .map(|value| WeightedText {
            value,
            hash: 0,
            count: 2,
        })
        .collect::<Vec<_>>();
    let report = compare_retained(
        &input,
        "actual retained arena copy test",
        1 << 20,
        128,
        PairOrder::RetainedFirst,
        |entries| retained::run(entries, 1 << 20),
    )
    .unwrap();
    let unique_bytes = u64::try_from(values.iter().map(String::len).sum::<usize>()).unwrap();
    assert_eq!(report.candidate.work.payload_bytes_copied, unique_bytes);
    assert!(report.retained.payload_bytes_copied > unique_bytes);
    assert!(retained::run(&input, 1).is_err());
    let invalid = [
        WeightedText {
            value: "a",
            hash: 1,
            count: 1,
        },
        WeightedText {
            value: "a",
            hash: 2,
            count: 1,
        },
    ];
    assert!(
        compare_retained(
            &invalid,
            "invalid hash contract",
            1 << 20,
            128,
            PairOrder::RetainedFirst,
            |_| panic!("invalid input must be rejected before retained execution")
        )
        .is_err()
    );
}

#[path = "compact_string_state_paired_benchmark.rs"]
mod paired;
