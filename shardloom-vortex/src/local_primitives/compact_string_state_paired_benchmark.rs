//! Ignored, bounded release measurements. The test emits compact JSON records;
//! the root runner owns binary/revision/machine identity and serial execution.

use super::{PairOrder, PairedRun, WeightedText, compare_retained, oracle, retained};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::hash::Hasher as _;

const ROWS: usize = 131_072;
const MEMORY_BYTES: u64 = 128 << 20;
const PAIRS: usize = 7;

#[derive(Clone, Copy, Debug)]
enum Distribution {
    Repeated,
    Skew,
    HighCardinality,
}

struct Fixture {
    values: Vec<String>,
    hashes: Vec<u64>,
    weights: Vec<u64>,
    payload_bytes: usize,
}

impl Fixture {
    fn new(distribution: Distribution, rows: usize) -> Self {
        assert!(rows <= ROWS);
        let values = (0..rows).map(|index| {
            let key = match distribution {
                Distribution::Repeated => index % 257,
                Distribution::Skew => if index.is_multiple_of(32) { index / 32 } else { 0 },
                Distribution::HighCardinality => (index * 4051) % (ROWS / 2),
            };
            if key == 0 { String::new() } else {
                format!("not-a-URL/東京/λ\0{key:08}/abcdefghijklmnopqrstuvwxyz-ABCDEFGHIJKLMNOPQRSTUVWXYZ-0123456789")
            }
        }).collect::<Vec<_>>();
        let payload_bytes = values.iter().map(String::len).sum::<usize>();
        assert!(payload_bytes <= 16 << 20);
        let hashes = values
            .iter()
            .map(|value| {
                let mut hash = rustc_hash::FxHasher::default();
                hash.write(value.as_bytes());
                hash.finish()
            })
            .collect();
        let weights = (0..rows)
            .map(|index| u64::try_from(index % 7 + 1).unwrap())
            .collect();
        Self {
            values,
            hashes,
            weights,
            payload_bytes,
        }
    }

    fn input(&self) -> Vec<WeightedText<'_>> {
        self.values
            .iter()
            .zip(&self.hashes)
            .zip(&self.weights)
            .map(|((value, hash), count)| WeightedText {
                value,
                hash: *hash,
                count: *count,
            })
            .collect()
    }
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = digest.as_ref();
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 15)]));
    }
    out
}

fn source_identity() -> String {
    let mut digest = Sha256::new();
    for source in [
        include_bytes!("string_count_partitions.rs").as_slice(),
        include_bytes!("string_count_partial.rs").as_slice(),
        include_bytes!("string_count_entry_credits.rs").as_slice(),
        include_bytes!("string_count_partition_benchmark.rs").as_slice(),
        include_bytes!("compact_string_state.rs").as_slice(),
        include_bytes!("compact_string_state_benchmark.rs").as_slice(),
        include_bytes!("compact_string_state_paired_benchmark.rs").as_slice(),
    ] {
        digest.update(u64::try_from(source.len()).unwrap().to_le_bytes());
        digest.update(source);
    }
    hex_digest(digest.finalize())
}

fn fixture_hashes(input: &[WeightedText<'_>]) -> (String, String, usize, u64) {
    let mut digest = Sha256::new();
    for entry in input {
        digest.update(u64::try_from(entry.value.len()).unwrap().to_le_bytes());
        digest.update(entry.value.as_bytes());
        digest.update(entry.hash.to_le_bytes());
        digest.update(entry.count.to_le_bytes());
    }
    let input_sha = hex_digest(digest.finalize());
    let expected = oracle(input).unwrap();
    let rows = expected
        .values()
        .try_fold(0_u64, |total, count| total.checked_add(*count))
        .unwrap();
    let mut digest = Sha256::new();
    for (key, count) in &expected {
        digest.update(u64::try_from(key.len()).unwrap().to_le_bytes());
        digest.update(key.as_bytes());
        digest.update(count.to_le_bytes());
    }
    (
        input_sha,
        hex_digest(digest.finalize()),
        expected.len(),
        rows,
    )
}

fn retained_json(report: &PairedRun) -> Value {
    let control = &report.retained;
    json!({
        "construction_nanos": control.construction_nanos,
        "reduce_nanos": control.kernel_nanos,
        "arrange_nanos_in_reduce": control.arrange_nanos,
        "reconcile_nanos_in_reduce": control.reconcile_nanos,
        "drop_nanos": control.drop_nanos,
        "owned_bytes": control.owned_bytes,
        "peak_owned_bytes": control.peak_owned_bytes,
        "table_arena_owned_bytes": control.table_owned_bytes,
        "slot_bytes": control.slot_bytes,
        "lookup_probes": control.probes,
        "full_hash_equality_comparisons": control.equality_comparisons,
        "utf8_bytes_copied_new_and_relocation": control.payload_bytes_copied,
        "rows": control.rows,
        "entry_credit_claim_calls": control.credit_claims,
        "entry_credit_return_calls": control.credit_returns,
        "entry_credits_outstanding": control.entry_credits_outstanding,
        "owned_bytes_after_drop": control.live_bytes_after_drop,
    })
}

fn candidate_json(report: &PairedRun) -> Value {
    let candidate = &report.candidate;
    let work = &candidate.work;
    json!({
        "construction_nanos": candidate.construction_nanos,
        "partition_input_arrange_nanos": candidate.arrange_nanos,
        "update_nanos": candidate.kernel_nanos,
        "drop_nanos": candidate.drop_nanos,
        "owned_bytes": candidate.owned_bytes,
        "peak_owned_bytes": candidate.peak_owned_bytes,
        "table_arena_owned_bytes": work.owned_bytes,
        "slot_bytes": work.slot_bytes,
        "lookup_probes_including_empty_slots_excluding_rehash": work.probes,
        "full_hash_equality_comparisons": work.full_hash_comparisons,
        "utf8_bytes_copied_new_only": work.payload_bytes_copied,
        "table_slots_rehashed": work.rehashed_slots,
        "directory_owner_moves": work.directory_owner_moves,
        "slabs": work.slabs,
        "groups": work.groups,
        "rows": work.rows,
        "updates": work.updates,
        "entry_credit_claim_calls": candidate.credit_claims,
        "entry_credit_return_calls": candidate.credit_returns,
        "entry_credits_outstanding": candidate.entry_credits_outstanding,
        "owned_bytes_after_drop": candidate.live_bytes_after_drop,
    })
}

#[test]
fn paired_fixture_full_values_hash_weights_and_cardinality_are_independent() {
    for distribution in [
        Distribution::Repeated,
        Distribution::Skew,
        Distribution::HighCardinality,
    ] {
        let fixture = Fixture::new(distribution, 8192);
        let input = fixture.input();
        let (hash, values_hash, groups, rows) = fixture_hashes(&input);
        assert_eq!(hash.len(), 64);
        assert_eq!(values_hash.len(), 64);
        assert_ne!(hash, values_hash);
        assert_eq!(rows, input.iter().map(|entry| entry.count).sum::<u64>());
        match distribution {
            Distribution::Repeated => assert_eq!(groups, 257),
            Distribution::Skew => assert_eq!(groups, 256),
            Distribution::HighCardinality => assert_eq!(groups, 8192),
        }
        let source = source_identity();
        let report = compare_retained(
            &input,
            &source,
            MEMORY_BYTES,
            16 << 10,
            PairOrder::CandidateFirst,
            |entries| retained::run(entries, MEMORY_BYTES),
        )
        .unwrap();
        assert_eq!(report.retained.complete_groups.len(), groups);
        assert_eq!(report.candidate.work.groups, groups);
        assert_eq!(report.retained.rows, rows);
        assert_eq!(report.candidate.work.rows, rows);
    }
}

#[test]
#[ignore = "paired release state benchmark; run serially with --release --ignored --nocapture"]
fn compact_string_state_actual_retained_paired_release() {
    assert!(
        !std::hint::black_box(cfg!(debug_assertions)),
        "paired measurements require a release test binary"
    );
    let source = source_identity();
    for distribution in [
        Distribution::Repeated,
        Distribution::Skew,
        Distribution::HighCardinality,
    ] {
        let fixture = Fixture::new(distribution, ROWS);
        let input = fixture.input();
        let (input_sha, output_sha, groups, rows) = fixture_hashes(&input);
        for slab_bytes in [16 << 10, 64 << 10, 256 << 10] {
            // One complete warm pair precedes seven actually alternating pairs.
            let warm = compare_retained(
                &input,
                &source,
                MEMORY_BYTES,
                slab_bytes,
                PairOrder::CandidateFirst,
                |entries| retained::run(entries, MEMORY_BYTES),
            )
            .unwrap();
            assert!(warm.complete_values_verified);
            drop(warm);
            for pair in 0..PAIRS {
                let order = if pair.is_multiple_of(2) {
                    PairOrder::RetainedFirst
                } else {
                    PairOrder::CandidateFirst
                };
                let report = compare_retained(
                    &input,
                    &source,
                    MEMORY_BYTES,
                    slab_bytes,
                    order,
                    |entries| retained::run(entries, MEMORY_BYTES),
                )
                .unwrap();
                assert_eq!(report.retained.rows, rows);
                assert_eq!(report.candidate.work.rows, rows);
                assert_eq!(report.retained.complete_groups.len(), groups);
                let control = retained_json(&report);
                let candidate = candidate_json(&report);
                println!(
                    "{}",
                    json!({
                        "benchmark": "actual_retained_complete_string_reducer_vs_compact_state",
                        "compiled_state_source_sha256": report.retained_source,
                        "distribution": format!("{distribution:?}"), "slab_bytes": slab_bytes,
                        "pair": pair, "pairs_per_profile": PAIRS, "order": format!("{:?}", report.order),
                        "input_entries": input.len(), "input_utf8_payload_bytes": fixture.payload_bytes,
                        "input_weighted_rows": rows, "complete_groups": groups,
                        "input_values_hashes_weights_sha256": input_sha,
                        "complete_values_sha256": output_sha,
                        "partition_count": super::PARTITIONS, "caller_threads": 1, "background_workers": 0,
                        "owned_state_budget_bytes": MEMORY_BYTES, "global_entry_limit": input.len(),
                        "complete_values_verified": report.complete_values_verified,
                        "retained": control, "candidate": candidate,
                        "timer_scope": "retained actual reducer lifecycle including routing/locks/credit publication and consumed native partial disposal; candidate test adapter updates plus separately reported routing; native fixture/hash/oracle/export construction excluded; not isolated table-kernel or public query latency",
                        "input_disposal_scope": "retained reduce consumes/disposes native value/count partial inside reduce clock; candidate borrowed routing vectors dispose outside update clock; no direct table-speed attribution",
                        "export_lifetime_scope": "both complete independently produced BTreeMaps retained across the paired kernels in either order; construction and verification outside state clocks",
                        "memory_scope": "explicit state table/arena and partition metadata capacities; source/partial/input routing/oracle/output/provider/allocator overhead and RSS excluded",
                        "counter_scope": "retained copy sites are cfg(test) instrumented; retained probes unavailable; full-hash equality is not total probes; candidate lookup excludes rehash probes",
                    })
                );
            }
        }
    }
}
