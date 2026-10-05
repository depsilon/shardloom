use super::{field, run_facade, run_route, unique_vortex_binding_dir};
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::{collections::BTreeMap, sync::Arc};

#[test]
#[allow(clippy::too_many_lines)] // The same complete-value fixture covers both public front doors.
fn public_weighted_count_spill_sql_dataframe_full_values_typed_evidence_and_lazy_route() {
    const ROWS: usize = 16_384;
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = unique_vortex_binding_dir("native-weighted-count-spill");
    std::fs::create_dir(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let workspace = root.join("workspace");
    let source = root.join("categories.vortex");
    let ipc = root.join("source.arrow");
    let values = (0..ROWS)
        .map(|index| format!("{:08}{}", index % 8192, "x".repeat(120)))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "category",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from_iter_values(values.iter()))],
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
    let mut counts = BTreeMap::new();
    for text in values {
        *counts.entry(text).or_insert(0_u64) += 1;
    }
    let mut expected = counts.into_iter().collect::<Vec<_>>();
    expected
        .sort_unstable_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let expected = expected
        .into_iter()
        .skip(7)
        .take(7)
        .map(|(text, count)| serde_json::json!({"category":text,"frequency":count}))
        .collect::<Vec<_>>();
    let payload = serde_json::json!({"group_by":["category"],"measures":[{"function":"count","alias":"frequency"}],"order_by":[{"column":"frequency","descending":true,"nulls":"first"},{"column":"category","descending":false,"nulls":"last"}],"offset":7,"spill":{"workspace":workspace,"quota_bytes":67_108_864_u64,"memory_bytes":8_388_608_u64}}).to_string();
    let sql = format!(
        "SELECT category, COUNT(*) AS frequency FROM '{}' GROUP BY category ORDER BY frequency DESC NULLS FIRST, category ASC NULLS LAST LIMIT 7 OFFSET 7",
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
        assert_eq!(super::complete_result::rows(&envelope), expected);
        assert!(stdout.contains(&field("result_payload_complete", "true")));
        assert!(stdout.contains(&field(
            "result_materialization_boundary",
            "bounded_native_batches_to_jsonl"
        )));
        let spill_field = |name| {
            super::complete_result::field_value(
                &envelope,
                &format!("local_primitive_native_weighted_count_spill_{name}"),
            )
        };
        let runs_written = spill_field("runs_written").parse::<u64>().unwrap();
        assert!(runs_written >= 2);
        assert_eq!(
            runs_written,
            spill_field("runs_validated").parse::<u64>().unwrap()
        );
        assert!(spill_field("min_run_block_rows").parse::<u64>().unwrap() > 1);
        assert_eq!(
            spill_field("source_rows").parse::<u64>().unwrap(),
            ROWS as u64
        );
        assert_eq!(spill_field("quota_bytes"), "67108864");
        assert_eq!(spill_field("memory_bytes"), "8388608");
        assert!(spill_field("peak_disk_bytes").parse::<u64>().unwrap() <= 67_108_864);
        assert!(spill_field("peak_reserved_bytes").parse::<u64>().unwrap() <= 8_388_608);
        assert_eq!(spill_field("owned_cleanup_completed"), "true");
        assert_eq!(
            spill_field("family"),
            "weighted_complete_utf8_grouped_count"
        );
        assert!(!envelope["fields"].as_array().unwrap().iter().any(|item| {
            item["key"] == "local_primitive_native_aggregate_spill_complete_pairs"
        }));
        assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
        assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
        assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
    }
}
