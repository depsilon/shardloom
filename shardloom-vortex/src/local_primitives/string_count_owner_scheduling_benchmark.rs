//! Private paired scheduler timing. Root records host, binary, revision and
//! serial invocation; this runner provides exact compiled sources and inputs.

use super::{Input, MAX_ROWS, PARTITIONS, QUEUE_PER_WORKER, Report, Scheduling, run};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{collections::BTreeMap, hash::Hasher as _};

fn digest(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 15)]));
    }
    output
}

fn source_sha() -> String {
    let mut hash = Sha256::new();
    for bytes in [
        include_bytes!("string_count_owner_scheduling.rs").as_slice(),
        include_bytes!("string_count_owner_scheduling_benchmark.rs").as_slice(),
        include_bytes!("string_count_partitions.rs").as_slice(),
        include_bytes!("string_count_partial.rs").as_slice(),
        include_bytes!("string_count_entry_credits.rs").as_slice(),
        include_bytes!("../../../shardloom-exec/src/compute_pool.rs").as_slice(),
    ] {
        hash.update(u64::try_from(bytes.len()).unwrap().to_le_bytes());
        hash.update(bytes);
    }
    digest(hash.finalize())
}

fn output_sha(values: &BTreeMap<String, u64>) -> String {
    let mut hash = Sha256::new();
    for (key, count) in values {
        hash.update(u64::try_from(key.len()).unwrap().to_le_bytes());
        hash.update(key.as_bytes());
        hash.update(count.to_le_bytes());
    }
    digest(hash.finalize())
}

fn summary(report: &Report) -> Value {
    json!({
        "construction_nanos": report.construction_nanos,
        "execution_with_submit_and_drain_nanos": report.execution_nanos,
        "joined_worker_and_state_drop_nanos": report.drop_nanos,
        "reducer_lock_acquisition_nanos": report.lock_wait_nanos,
        "reducer_reconcile_nanos": report.reconcile_nanos,
        "queue_enqueue_nanos": report.enqueue_wait_nanos,
        "queue_wait_and_dequeue_nanos": report.dequeue_wait_nanos,
        "peak_queued_ranges": report.peak_queued_ranges,
        "peak_active_ranges": report.peak_active_ranges,
        "completed_ranges": report.completed_ranges,
        "peak_reserved_bytes": report.peak_reserved_bytes,
        "table_arena_reserved_bytes": report.table_reserved_bytes,
        "utf8_bytes_copied_including_relocation": report.copied_bytes,
        "full_hash_equality_comparisons": report.equality_comparisons,
        "entry_credit_claim_calls": report.credit_claims,
        "entry_credit_return_calls": report.credit_returns,
        "rows": report.rows, "groups": report.values.len(),
        "background_workers": report.background_workers,
        "owned_state_bytes_after_drop": 0,
        "entry_credits_outstanding_after_drain": 0,
        "complete_output_sha256": output_sha(&report.values),
    })
}

fn paired(
    input: &Input,
    lanes: usize,
    owner_first: bool,
    expected: &BTreeMap<String, u64>,
) -> (Report, Report) {
    let execute = |mode| {
        let memory = LiveMemoryPool::new(128 << 20).unwrap();
        let report = run(
            input,
            mode,
            lanes,
            &memory,
            input.entries,
            CancellationToken::default(),
            None,
        )
        .unwrap();
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert!(report.peak_active_ranges <= lanes);
        assert!(report.peak_queued_ranges <= (lanes - 1) * QUEUE_PER_WORKER);
        report
    };
    // Both maps remain alive through the second case in either order.
    let (dynamic, owner) = if owner_first {
        let owner = execute(Scheduling::Owner);
        (execute(Scheduling::Dynamic), owner)
    } else {
        let dynamic = execute(Scheduling::Dynamic);
        (dynamic, execute(Scheduling::Owner))
    };
    assert_eq!(&dynamic.values, expected);
    assert_eq!(&owner.values, expected);
    assert_eq!(dynamic.rows, owner.rows);
    assert_eq!(dynamic.completed_ranges, owner.completed_ranges);
    (dynamic, owner)
}

#[test]
#[ignore = "requires isolated release timing; root owns serial benchmark gate"]
fn owner_scheduling_actual_retained_paired_release() {
    assert!(
        !std::hint::black_box(cfg!(debug_assertions)),
        "use a release test binary"
    );
    let source_sha = source_sha();
    for distribution in ["repeated", "skew", "high_cardinality"] {
        let strings = (0..MAX_ROWS)
            .map(|index| {
                let group = match distribution {
                    "repeated" => index % 257,
                    "skew" => {
                        if index.is_multiple_of(32) {
                            index / 32
                        } else {
                            0
                        }
                    }
                    _ => (index * 4051) % (MAX_ROWS / 2),
                };
                if group == 0 {
                    String::new()
                } else {
                    format!("cohort/東京/λ\0{group:08}/abcdefghijklmnopqrstuvwxyz")
                }
            })
            .collect::<Vec<_>>();
        let rows = strings
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let mut hash = rustc_hash::FxHasher::default();
                hash.write(value.as_bytes());
                (
                    value.as_str(),
                    hash.finish(),
                    u64::try_from(index % 7 + 1).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let mut input_hash = Sha256::new();
        let mut expected = BTreeMap::<String, u64>::new();
        for (key, hash, count) in &rows {
            input_hash.update(u64::try_from(key.len()).unwrap().to_le_bytes());
            input_hash.update(key.as_bytes());
            input_hash.update(hash.to_le_bytes());
            input_hash.update(count.to_le_bytes());
            let entry = expected.entry((*key).to_owned()).or_default();
            *entry = entry.checked_add(*count).unwrap();
        }
        let input_sha = digest(input_hash.finalize());
        let source_memory = LiveMemoryPool::new(64 << 20).unwrap();
        let input = Input::prepare(&rows, 4096, &source_memory).unwrap();
        for lanes in [1, 2, 4, 8] {
            drop(paired(&input, lanes, false, &expected));
            for pair in 0..7 {
                let owner_first = pair % 2 == 1;
                let (dynamic, owner) = paired(&input, lanes, owner_first, &expected);
                println!(
                    "{}",
                    json!({
                        "schema": "shardloom.owner_partition_scheduling_experiment.v1",
                        "source_sha256": source_sha, "input_sha256": input_sha,
                        "distribution": distribution, "partial_entries": input.entries,
                        "source_chunk_rows": 4096, "logical_partitions": PARTITIONS,
                        "requested_cpu_lanes_including_caller": lanes,
                        "queue_range_capacity": (lanes - 1) * QUEUE_PER_WORKER,
                        "pair": pair, "owner_first": owner_first,
                        "dynamic": summary(&dynamic), "owner": summary(&owner),
                        "exact_complete_values_verified": true,
                        "scope": "actual retained partition reducer, identical prepared native ranges; source construction/hash/arrange and full oracle/export excluded; prior full output retained symmetrically; no compact-state or public-query claim",
                        "memory_scope": "actual table/arena credits plus reserved range queue/control envelopes; fixture provider buffers, OS thread stacks, ComputePool internal bookkeeping and output oracle maps excluded",
                        "timing_scope": "wall clocks; dequeue includes idle waits and lock acquisition includes uncontended acquisition; no CPU-time or cache-cold claim"
                    })
                );
            }
        }
        drop(input);
        assert_eq!(source_memory.snapshot().reserved_bytes, 0);
    }
}
