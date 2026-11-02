//! Primitive spill permission and once-only replay survive the public writer.

use super::{complete_result, run_facade, unique_vortex_binding_dir};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let root = unique_vortex_binding_dir("primitive-fanout");
        fs::create_dir(&root).unwrap();
        fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex"),
            root.join("source.vortex"),
        )
        .unwrap();
        fs::create_dir(root.join("runs")).unwrap();
        fs::write(root.join("runs/keep.txt"), "unrelated").unwrap();
        Self(root)
    }

    fn assert_cleanup(&self) {
        assert_eq!(
            fs::read_to_string(self.0.join("runs/keep.txt")).unwrap(),
            "unrelated"
        );
        assert_eq!(fs::read_dir(self.0.join("runs")).unwrap().count(), 1);
        assert!(!fs::read_dir(&self.0).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".shardloom-fanout-")
        }));
    }

    fn paths(&self) -> [PathBuf; 3] {
        ["jsonl", "vortex", "parquet"].map(|format| self.0.join(format!("result.{format}")))
    }

    fn run(&self, surface: &str, aggregate: bool, quota: Option<u64>) -> (bool, Value) {
        let paths = self.paths();
        let mut declaration = if aggregate {
            json!({"group_by":["value"], "measures":[{"function":"count_distinct","column":"metric","alias":"frequency"}], "order_by":[{"column":"frequency","descending":true},{"column":"value","descending":false}], "offset":1})
        } else {
            json!({"order_by":[{"column":"metric","descending":true}], "offset":1})
        };
        if let Some(quota) = quota {
            declaration["spill"] = json!({"workspace":self.0.join("runs"), "quota_bytes":quota, "memory_bytes":2 << 20});
        }
        let mut args = vec![
            "run".into(),
            surface.into(),
            "--input".into(),
            self.0.join("source.vortex").display().to_string(),
            "--input-format".into(),
            "vortex".into(),
            "--request".into(),
            "write_jsonl".into(),
            "--output".into(),
            paths[0].display().to_string(),
            "--fanout-output".into(),
            format!("vortex={}", paths[1].display()),
            "--fanout-output".into(),
            format!("parquet={}", paths[2].display()),
            "--bounded".into(),
            "true".into(),
            "--vortex-primitive".into(),
            if aggregate { "aggregate" } else { "sort_rows" }.into(),
            if aggregate {
                "--vortex-aggregate"
            } else {
                "--vortex-sort-rows"
            }
            .into(),
            declaration.to_string(),
            "--vortex-source-order-limit".into(),
            "2".into(),
            "--memory-gb".into(),
            "1".into(),
            "--max-parallelism".into(),
            "1".into(),
            "--format".into(),
            "json".into(),
        ];
        if !aggregate {
            args.extend(["--vortex-columns".into(), "value,metric".into()]);
        }
        let (ok, stdout) = run_facade(&args.iter().map(String::as_str).collect::<Vec<_>>());
        (
            ok,
            serde_json::from_str(&stdout)
                .unwrap_or_else(|error| panic!("invalid completed report: {error}: {stdout}")),
        )
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn assert_unique(report: &Value) {
    let mut keys = BTreeSet::new();
    for entry in report["fields"].as_array().unwrap() {
        assert!(
            keys.insert(entry["key"].as_str().unwrap()),
            "duplicate field: {}",
            entry["key"]
        );
    }
}

fn reopen(path: &Path) -> Vec<Value> {
    let query = format!("SELECT * FROM '{}'", path.display());
    let (ok, stdout) = run_facade(&[
        "run",
        "sql",
        "--sql",
        &query,
        "--request",
        "collect",
        "--bounded",
        "true",
        "--memory-gb",
        "1",
        "--max-parallelism",
        "1",
        "--format",
        "json",
    ]);
    assert!(ok, "{stdout}");
    let report = serde_json::from_str(&stdout).unwrap();
    assert_unique(&report);
    complete_result::rows(&report)
}

#[test]
fn public_primitive_fanout_preserves_explicit_spill_permission_and_complete_results() {
    const QUOTA: u64 = 64 << 20;
    for surface in ["cli", "dataframe"] {
        for aggregate in [false, true] {
            for spill in [false, true] {
                let fixture = Fixture::new();
                let (ok, report) = fixture.run(surface, aggregate, spill.then_some(QUOTA));
                assert!(
                    ok,
                    "{surface}/aggregate={aggregate}/spill={spill}: {report}"
                );
                assert_unique(&report);
                assert_replay_decisions(&report);
                for (key, value) in [
                    ("native_vortex_result_export_execution_count", "1"),
                    (
                        "native_vortex_result_export_execution_model",
                        "native_query_once_with_credited_result_replay",
                    ),
                    ("native_vortex_result_export_replay_performed", "true"),
                    ("native_vortex_result_export_replay_count", "3"),
                    ("native_vortex_result_export_replay_rows_detached", "2"),
                    ("native_vortex_result_export_all_targets_committed", "true"),
                    ("fallback_attempted", "false"),
                ] {
                    assert_eq!(complete_result::field_value(&report, key), value, "{key}");
                }
                assert_eq!(
                    complete_result::field_value(
                        &report,
                        "native_vortex_result_export_replay_spill_admitted"
                    ),
                    spill.to_string()
                );
                if spill {
                    assert_eq!(
                        complete_result::field_value(
                            &report,
                            "native_vortex_result_export_replay_spill_quota_bytes"
                        ),
                        QUOTA.to_string()
                    );
                    assert_eq!(
                        complete_result::field_value(
                            &report,
                            "native_vortex_result_export_replay_spill_owned_cleanup_completed"
                        ),
                        "true"
                    );
                    let peak = complete_result::field_value(
                        &report,
                        "native_vortex_result_export_replay_shared_spill_peak_disk_bytes",
                    )
                    .parse::<u64>()
                    .unwrap();
                    assert!(peak <= QUOTA);
                    if aggregate {
                        // The admitted DISTINCT accumulator stays resident for
                        // these five pairs. Permission does not imply disk I/O.
                        assert_eq!(peak, 0);
                    } else {
                        assert!(peak > 0);
                    }
                }
                let expected = if aggregate {
                    vec![
                        json!({"value":2,"frequency":1}),
                        json!({"value":3,"frequency":1}),
                    ]
                } else {
                    vec![
                        json!({"value":4,"metric":40}),
                        json!({"value":3,"metric":30}),
                    ]
                };
                let paths = fixture.paths();
                let rows = fs::read_to_string(&paths[0])
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str::<Value>(line).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(rows, expected);
                for path in &paths[1..] {
                    assert_eq!(reopen(path), expected);
                }
                fixture.assert_cleanup();
            }
        }
    }
}

fn assert_replay_decisions(report: &Value) {
    for (key, value) in [
        (
            "native_vortex_result_export_replay_decision_native_replay_considered",
            "3",
        ),
        (
            "native_vortex_result_export_replay_decision_native_replay_executed",
            "3",
        ),
        (
            "native_vortex_result_export_replay_decision_native_replay_observed_output_rows",
            "6",
        ),
        (
            "native_vortex_result_export_replay_decision_native_replay_admission_reasons",
            "resident_retention",
        ),
    ] {
        assert_eq!(complete_result::field_value(report, key), value, "{key}");
    }
}

#[test]
fn public_primitive_fanout_rejects_the_original_quota_before_publication() {
    for aggregate in [false, true] {
        let fixture = Fixture::new();
        let (ok, report) = fixture.run("dataframe", aggregate, Some(1));
        assert!(!ok, "{report}");
        assert!(report.to_string().contains("quota"), "{report}");
        assert_unique(&report);
        assert!(fixture.paths().iter().all(|path| !path.exists()));
        fixture.assert_cleanup();
    }
}
