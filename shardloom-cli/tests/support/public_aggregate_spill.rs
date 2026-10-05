use super::{complete_result, field, run_facade, run_route, unique_vortex_binding_dir};
use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[test]
#[allow(clippy::too_many_lines)] // One bounded fixture proves both public front doors and effect denial.
fn public_aggregate_spill_sql_dataframe_exact_values_cleanup_and_effect_admission() {
    const ROWS: usize = 131_072;
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = unique_vortex_binding_dir("native-aggregate-spill");
    std::fs::create_dir(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let workspace = root.join("workspace");
    let source = root.join("renamed.vortex");
    let ipc = root.join("source.arrow");
    let pairs = (0..ROWS)
        .map(|row| {
            let logical = row % 65_536;
            (
                i64::try_from(logical % 257).unwrap() - 128,
                (1_u64 << 61) + u64::try_from(logical).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![
        Field::new("member", DataType::UInt64, false),
        Field::new("cohort", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                pairs.iter().map(|pair| pair.1),
            )),
            Arc::new(Int64Array::from_iter_values(
                pairs.iter().map(|pair| pair.0),
            )),
        ],
    )
    .unwrap();
    let mut writer =
        arrow_ipc::writer::FileWriter::try_new(std::fs::File::create(&ipc).unwrap(), &schema)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let columnar = shardloom_vortex::read_flat_arrow_ipc_columnar_source(&ipc, ROWS).unwrap();
    shardloom_vortex::write_flat_columnar_vortex_prepared_state(
        shardloom_vortex::VortexPreparedStateColumnarWriteRequest::new(&source, columnar),
    )
    .unwrap();
    let mut oracle = BTreeMap::<i64, BTreeSet<u64>>::new();
    for (group, member) in pairs {
        oracle.entry(group).or_default().insert(member);
    }
    let mut oracle = oracle
        .into_iter()
        .map(|(group, members)| (group, members.len()))
        .collect::<Vec<_>>();
    oracle.sort_unstable_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let expected = oracle
        .into_iter()
        .skip(123)
        .take(7)
        .map(|(group, count)| serde_json::json!({"cohort":group,"members":count}))
        .collect::<Vec<_>>();
    let payload = serde_json::json!({"group_by":["cohort"],"measures":[{"function":"count_distinct","column":"member","alias":"members"}],
        "order_by":[{"column":"members","descending":true,"nulls":"first"},{"column":"cohort","descending":false,"nulls":"last"}],"offset":123,
        "spill":{"workspace":workspace,"quota_bytes":67_108_864_u64,"memory_bytes":4_194_304_u64}}).to_string();
    let sql = format!(
        "SELECT cohort, COUNT(DISTINCT member) AS members FROM '{}' GROUP BY cohort ORDER BY members DESC NULLS FIRST, cohort ASC NULLS LAST LIMIT 7 OFFSET 123",
        source.display()
    );
    let stdout = run_route(&[
        "route",
        "sql",
        "--input",
        source.to_str().unwrap(),
        "--input-format",
        "vortex",
        "--sql",
        &sql,
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--vortex-aggregate",
        &payload,
        "--format",
        "json",
    ]);
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(!workspace.exists());
    std::fs::create_dir(&workspace).unwrap();
    for surface in ["sql", "dataframe"] {
        let mut args = vec![
            "run",
            surface,
            "--input",
            source.to_str().unwrap(),
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--vortex-aggregate",
            &payload,
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ];
        if surface == "sql" {
            args.extend(["--sql", &sql]);
        } else {
            args.extend([
                "--vortex-primitive",
                "aggregate",
                "--vortex-source-order-limit",
                "7",
            ]);
        }
        let (ok, stdout) = run_facade(&args);
        assert!(ok, "{surface}: {stdout}");
        let envelope: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(envelope["status"], "success");
        assert_eq!(complete_result::rows(&envelope), expected);
        assert!(stdout.contains(&field("result_payload_complete", "true")));
        assert!(stdout.contains(&field(
            "result_materialization_boundary",
            "bounded_native_batches_to_jsonl"
        )));
        let runs_written = complete_result::field_value(
            &envelope,
            "local_primitive_native_aggregate_spill_runs_written",
        )
        .parse::<u64>()
        .unwrap();
        assert!(runs_written >= 4);
        assert_eq!(
            runs_written,
            complete_result::field_value(
                &envelope,
                "local_primitive_native_aggregate_spill_runs_validated"
            )
            .parse::<u64>()
            .unwrap()
        );
        assert_eq!(
            complete_result::field_value(
                &envelope,
                "local_primitive_native_aggregate_spill_complete_pairs"
            ),
            "65536"
        );
        assert_eq!(
            complete_result::field_value(
                &envelope,
                "local_primitive_native_aggregate_spill_owned_cleanup_completed"
            ),
            "true"
        );
        assert!(
            complete_result::field_value(
                &envelope,
                "local_primitive_native_aggregate_spill_peak_reserved_bytes"
            )
            .parse::<u64>()
            .unwrap()
                <= 4_194_304
        );
        assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
        assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
        assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
    }
    for (format, request, extension) in [
        ("jsonl", "write_jsonl", "jsonl"),
        ("vortex", "write_vortex", "vortex"),
    ] {
        let output = root.join(format!("aggregate-output.{extension}"));
        let (ok, stdout) = run_facade(&[
            "run",
            "sql",
            "--input",
            source.to_str().unwrap(),
            "--input-format",
            "vortex",
            "--sql",
            &sql,
            "--request",
            request,
            "--output",
            output.to_str().unwrap(),
            "--execution-policy",
            "native_vortex",
            "--bounded",
            "true",
            "--allow-overwrite",
            "--vortex-aggregate",
            &payload,
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ]);
        assert!(ok, "{request}: {stdout}");
        assert!(stdout.contains(&field(
            "public_workflow_route_id",
            "native_vortex_primitive_row_export"
        )));
        assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
        assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
        let envelope: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        let result_fields = envelope["fields"].as_array().unwrap();
        let spill_runs = result_fields
            .iter()
            .find(|field| field["key"] == "local_primitive_native_aggregate_spill_runs_written")
            .unwrap()["value"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert!(spill_runs >= 4, "{request}: {stdout}");
        assert!(stdout.contains(&field(
            "local_primitive_native_aggregate_spill_owned_cleanup_completed",
            "true"
        )));
        assert!(output.is_file(), "{request}: {}", output.display());
        assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
        if format == "jsonl" {
            let rows = std::fs::read_to_string(&output)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(rows, expected, "{request}");
        } else {
            let output_sql = format!("SELECT * FROM '{}' LIMIT 10", output.display());
            let (ok, stdout) = run_facade(&[
                "run",
                "sql",
                "--input",
                output.to_str().unwrap(),
                "--input-format",
                "vortex",
                "--sql",
                &output_sql,
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--memory-gb",
                "1",
                "--max-parallelism",
                "1",
                "--format",
                "json",
            ]);
            assert!(ok, "read back {}: {stdout}", output.display());
            let envelope: serde_json::Value = serde_json::from_str(&stdout).unwrap();
            assert_eq!(
                complete_result::rows(&envelope),
                expected,
                "read back {}",
                output.display()
            );
        }
        assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
    }
    let missing = root.join("absent.vortex");
    let (ok, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        missing.to_str().unwrap(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--materialization-policy",
        "zero_decode",
        "--vortex-primitive",
        "aggregate",
        "--vortex-source-order-limit",
        "7",
        "--vortex-aggregate",
        &payload,
        "--format",
        "json",
    ]);
    assert!(!ok);
    assert!(
        stdout.contains("aggregate compute requires admitted native array decoding"),
        "{stdout}"
    );
    assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
}
