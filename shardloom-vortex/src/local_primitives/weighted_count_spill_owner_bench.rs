//! Bounded public-call screen of duplicate merge-key ownership, outside normal CI.

use super::*;
use sha2::{Digest as _, Sha256};
use std::{fmt::Write as _, time::Instant};

fn digest(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(&mut result, "{byte:02x}").unwrap();
    }
    result
}

#[test]
#[ignore = "bounded spill ownership screen; guarded local TMPDIR, release and serial execution required"]
#[allow(clippy::assertions_on_constants)]
fn complete_spill_key_owner_workflows() {
    assert!(!cfg!(debug_assertions));
    let root =
        PathBuf::from(std::env::var_os("SHARDLOOM_PERF_UAT_ROOT").expect("guarded UAT root"))
            .canonicalize()
            .unwrap();
    let temporary = std::env::temp_dir().canonicalize().unwrap();
    assert!(temporary.starts_with(&root) && temporary != root);
    let mut samples = Vec::new();
    for (name, rows, width, unique, groups) in [
        (
            "integer_text_repeated",
            65_536,
            128,
            false,
            ["cohort_renamed", "label_renamed"],
        ),
        (
            "text_integer_repeated",
            65_536,
            128,
            false,
            ["label_renamed", "cohort_renamed"],
        ),
        (
            "integer_text_unique",
            131_072,
            64,
            true,
            ["cohort_renamed", "label_renamed"],
        ),
    ] {
        let fixture = Fixture::with_unique_keys(rows, width, true, false, unique);
        assert!(fixture.directory.starts_with(&temporary));
        let source = std::fs::read(fixture.path()).unwrap();
        let source_bytes = source.len();
        let source_sha256 = digest(&source);
        drop(source);
        let expected = fixture.expected(&groups, 7, 20, 0);
        let request = fixture.query(&groups, 7, 20);
        for repetition in 1..=3 {
            let start = Instant::now();
            let report = execute_vortex_local_primitive_with_policy(
                &request,
                VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
            )
            .unwrap();
            let call_nanos = start.elapsed().as_nanos();
            let output = summary(&report);
            assert_eq!(output["values"], expected);
            assert_eq!(report.rows_selected, Some(rows as u64));
            let evidence = report
                .state_budget
                .native_weighted_count_spill
                .as_ref()
                .unwrap();
            assert!(evidence.runs_written > 0 && evidence.merge_passes > 0);
            assert_eq!(evidence.runs_written, evidence.runs_validated);
            assert!(evidence.owned_cleanup_completed);
            assert!(evidence.peak_reserved_bytes <= evidence.memory_bytes);
            assert!(evidence.peak_disk_bytes <= evidence.quota_bytes);
            assert!(!report.arrow_converted && !report.fallback_execution_allowed);
            assert!(
                local_primitive_native_io_certificate(&request, &report)
                    .unwrap()
                    .is_certified()
            );
            fixture.empty();
            let drop_start = Instant::now();
            drop(report);
            let report_drop_nanos = drop_start.elapsed().as_nanos();
            samples.push(serde_json::json!({
                "case": name, "repetition": repetition, "source_rows": rows,
                "key_width": width, "source_bytes": source_bytes,
                "source_sha256": source_sha256,
                "public_call_nanos": call_nanos,
                "report_drop_nanos": report_drop_nanos,
                "complete_nanos": call_nanos + report_drop_nanos,
                "complete_values": output["values"],
                "spill_evidence": output["weighted_count_spill"],
                "certified": true, "workspace_empty": true,
            }));
        }
        assert_eq!(
            digest(&std::fs::read(fixture.path()).unwrap()),
            source_sha256
        );
    }
    println!(
        "SHARDLOOM_SPILL_OWNER_SCREEN={}",
        serde_json::json!({
            "schema_version": "shardloom.spill_key_owner_screen.v1",
            "timing_boundary": "fresh public native call through complete report plus report release; fixture/request creation, oracle verification and observer output excluded",
            "memory_scope": "process RSS includes fixture and oracle construction; native reservations are separate",
            "samples": samples,
        })
    );
}
