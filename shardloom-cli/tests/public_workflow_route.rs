use std::process::Command;

#[cfg(all(
    unix,
    feature = "vortex-local-primitives",
    feature = "vortex-write",
    feature = "universal-format-io"
))]
#[path = "support/public_aggregate_spill.rs"]
mod aggregate_spill;

#[cfg(all(
    unix,
    feature = "vortex-local-primitives",
    feature = "vortex-write",
    feature = "universal-format-io"
))]
#[path = "support/public_weighted_count_spill.rs"]
mod weighted_count_spill;

#[cfg(all(
    unix,
    feature = "vortex-local-primitives",
    feature = "vortex-write",
    feature = "universal-format-io"
))]
#[test]
#[allow(clippy::too_many_lines)]
fn public_native_array_sink_reopens_full_nullable_projection_and_filter_values() {
    use arrow_array::{Int64Array, RecordBatch, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    const ROWS: usize = 40_000;
    const LIMIT: usize = 10_003;
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = unique_vortex_binding_dir("native-array-sink");
    std::fs::create_dir(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let ipc = root.join("source.arrow");
    let source = root.join("shipments.vortex");
    let schema = Arc::new(Schema::new(vec![
        Field::new("priority", DataType::Int64, false),
        Field::new("shipment_sequence", DataType::UInt64, false),
        Field::new("destination", DataType::Utf8, true),
    ]));
    let labels = (0..ROWS)
        .map(|index| (!index.is_multiple_of(7)).then(|| format!("港-{index}")))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..ROWS).map(|index| i64::try_from(index % 97).unwrap() - 48),
            )),
            Arc::new(UInt64Array::from_iter_values(
                0..u64::try_from(ROWS).unwrap(),
            )),
            Arc::new(StringArray::from(labels)),
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
    let projection = r#"{"structured_columns":[{"name":"renamed_port","source":"destination"},{"name":"shipment_sequence","source":"shipment_sequence"}]}"#;
    for primitive in ["expression_project", "project", "filter_project"] {
        let output = root.join(format!("{primitive}.vortex"));
        let mut args = vec![
            "run",
            "dataframe",
            "--input",
            source.to_str().unwrap(),
            "--input-format",
            "vortex",
            "--request",
            "write_vortex",
            "--output",
            output.to_str().unwrap(),
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--vortex-primitive",
            primitive,
            "--vortex-source-order-limit",
            "10003",
            "--max-parallelism",
            "1",
            "--format",
            "json",
        ];
        if primitive == "expression_project" {
            args.extend([
                "--vortex-expression-projection",
                projection,
                "--vortex-columns",
                "destination,shipment_sequence",
                "--vortex-predicate",
                "gte:priority:0",
            ]);
        } else {
            args.extend(["--vortex-columns", "destination,shipment_sequence"]);
            if primitive == "filter_project" {
                args.extend(["--vortex-predicate", "gte:priority:0"]);
            }
        }
        let (ok, stdout) = run_facade(&args);
        assert!(ok, "{primitive}: {stdout}");
        let envelope: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(envelope["status"], "success", "{stdout}");
        for (key, value) in [
            (
                "native_vortex_result_export_kind",
                "owned_native_array_stream",
            ),
            ("native_vortex_array_sink_adapter_payload_bytes_copied", "0"),
            ("native_vortex_array_sink_scalar_values_materialized", "0"),
            (
                "native_vortex_array_sink_source_generation_validated",
                "true",
            ),
            (
                "native_vortex_array_sink_dtype_and_row_count_validated",
                "true",
            ),
            ("arrow_converted", "false"),
            ("row_read", "false"),
            (
                "decode_materialization_boundary",
                "native_scan_arrays_to_native_flat_writer;no_adapter_scalar_or_arrow_conversion;provider_decode_may_occur",
            ),
            (
                "native_vortex_result_export_target_commit_modes",
                "primary:vortex:atomic_create_if_absent_hard_link_same_directory",
            ),
            (
                "native_vortex_result_export_fanout_atomicity_contract",
                "single_target_atomic_create_if_absent_hard_link_same_directory",
            ),
            ("public_workflow_fallback_attempted", "false"),
            ("public_workflow_external_engine_invoked", "false"),
        ] {
            assert!(stdout.contains(&field(key, value)), "{key}: {stdout}");
        }
        assert!(!stdout.contains("native_vortex_columnar_compatibility_sink_arrow_batches"));
        // Decode every persisted value through the existing explicit JSONL boundary,
        // then compare to a Rust reference that never uses the native predicate.
        let decoded = root.join(format!("{primitive}.jsonl"));
        let request = shardloom_vortex::VortexQueryPrimitiveRequest::project(
            shardloom_core::DatasetUri::new(output.display().to_string()).unwrap(),
            shardloom_plan::ProjectionRequest::All,
        );
        let report = shardloom_vortex::execute_vortex_local_primitive_row_export_with_policy(
            &request,
            &decoded,
            shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Jsonl,
            false,
            shardloom_vortex::VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
        assert_eq!(report.rows_written, u64::try_from(LIMIT).unwrap());
        let actual = std::fs::read_to_string(decoded)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let name = if primitive == "expression_project" {
            "renamed_port"
        } else {
            "destination"
        };
        let expected = (0..ROWS)
            .filter(|index| primitive == "project" || index % 97 >= 48)
            .take(LIMIT)
            .map(|index| serde_json::json!({name:(!index.is_multiple_of(7)).then(|| format!("港-{index}")),"shipment_sequence":index}))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "{primitive}");
    }
}

#[cfg(all(
    unix,
    feature = "vortex-local-primitives",
    feature = "vortex-write",
    feature = "universal-format-io"
))]
mod columnar_compatibility {
    use super::{field, run_facade, unique_vortex_binding_dir};
    use arrow_array::{Array as _, Int64Array, RecordBatch, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use serde_json::{Value, json};
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::Arc,
    };

    const ROWS: usize = 4097;
    const PROJECTION: &str = r#"{"structured_columns":[{"name":"note","source":"text_value"},{"name":"id","source":"exact_identifier"},{"name":"position","source":"row_ordinal"}]}"#;
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let root = unique_vortex_binding_dir("columnar-compatibility");
            fs::create_dir(&root).unwrap();
            let fixture = Self(root);
            let schema = Arc::new(Schema::new(vec![
                Field::new("row_ordinal", DataType::UInt64, false),
                Field::new("exact_identifier", DataType::Int64, false),
                Field::new("text_value", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(UInt64Array::from_iter_values(
                        (0..ROWS).map(|row| u64::try_from(row).unwrap()),
                    )),
                    Arc::new(Int64Array::from_iter_values((0..ROWS).map(identifier))),
                    Arc::new(StringArray::from((0..ROWS).map(label).collect::<Vec<_>>())),
                ],
            )
            .unwrap();
            let ipc = fixture.0.join("source.arrow");
            let mut writer =
                arrow_ipc::writer::FileWriter::try_new(fs::File::create(&ipc).unwrap(), &schema)
                    .unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
            drop(writer);
            let columnar =
                shardloom_vortex::read_flat_arrow_ipc_columnar_source(&ipc, ROWS).unwrap();
            shardloom_vortex::write_flat_columnar_vortex_prepared_state(
                shardloom_vortex::VortexPreparedStateColumnarWriteRequest::new(
                    fixture.source(),
                    columnar,
                ),
            )
            .unwrap();
            fixture
        }
        fn source(&self) -> PathBuf {
            self.0.join("source.vortex")
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn identifier(row: usize) -> i64 {
        match row {
            0 => i64::MIN,
            1 => i64::MAX,
            _ => (1_i64 << 60) + i64::try_from(row).unwrap(),
        }
    }
    fn label(row: usize) -> Option<String> {
        (!row.is_multiple_of(7)).then(|| {
            if row.is_multiple_of(13) {
                String::new()
            } else {
                format!("港-東京-λ-{row}-literal%_\\")
            }
        })
    }
    fn oracle(row: usize) -> Value {
        json!({"note":label(row), "id":identifier(row), "position":row})
    }
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("note", DataType::Utf8, true),
            Field::new("id", DataType::Int64, false),
            Field::new("position", DataType::UInt64, false),
        ]))
    }
    fn read(path: &Path, format: &str) -> Vec<Value> {
        let (actual_schema, batches) = if format == "arrow-ipc" {
            let reader =
                arrow_ipc::reader::FileReader::try_new(fs::File::open(path).unwrap(), None)
                    .unwrap();
            (
                reader.schema(),
                reader.collect::<Result<Vec<_>, _>>().unwrap(),
            )
        } else {
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                fs::File::open(path).unwrap(),
            )
            .unwrap();
            (
                Arc::clone(builder.schema()),
                builder
                    .build()
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
            )
        };
        assert_eq!(actual_schema, schema());
        let mut rows = Vec::new();
        for batch in batches {
            let note = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let id = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let position = batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(!id.is_null(row) && !position.is_null(row));
                rows.push(json!({"note":(!note.is_null(row)).then(|| note.value(row)),
                    "id":id.value(row), "position":position.value(row)}));
            }
        }
        rows
    }
    fn number(envelope: &Value, name: &str) -> u64 {
        let key = format!("native_vortex_columnar_compatibility_sink_{name}");
        envelope["result"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .find(|field| field["key"] == key)
            .unwrap_or_else(|| panic!("missing {key}: {envelope}"))["value"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap()
    }
    fn export(
        source: &Path,
        output: &Path,
        format: &str,
        predicate: Option<&str>,
        limit: Option<&str>,
    ) -> String {
        let request = if format == "arrow-ipc" {
            "write_arrow_ipc"
        } else {
            "write_parquet"
        };
        let mut args = vec![
            "run",
            "dataframe",
            "--input",
            source.to_str().unwrap(),
            "--input-format",
            "vortex",
            "--request",
            request,
            "--output",
            output.to_str().unwrap(),
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--vortex-primitive",
            "expression_project",
            "--vortex-expression-projection",
            PROJECTION,
            "--vortex-columns",
            "text_value,exact_identifier,row_ordinal",
            "--max-parallelism",
            "1",
            "--format",
            "json",
        ];
        if let Some(predicate) = predicate {
            args.extend(["--vortex-predicate", predicate]);
        }
        if let Some(limit) = limit {
            args.extend(["--vortex-source-order-limit", limit]);
        }
        let (ok, stdout) = run_facade(&args);
        assert!(ok, "{format}: {stdout}");
        assert_eq!(
            serde_json::from_str::<Value>(&stdout).unwrap()["status"],
            "success"
        );
        stdout
    }
    fn assert_boundary(stdout: &str, format: &str, scanned: bool, converted: bool) {
        for (key, value) in [
            (
                "native_vortex_result_export_kind",
                "owned_native_array_stream",
            ),
            (
                "decode_materialization_boundary",
                "native_scan_arrays_to_explicit_arrow_compatibility_writer;no_scalar_row_bridge;provider_decode_or_copy_may_occur",
            ),
            ("row_read", "false"),
            ("data_read", if scanned { "true" } else { "false" }),
            ("data_decoded", if scanned { "true" } else { "false" }),
            ("data_materialized", if scanned { "true" } else { "false" }),
            (
                "upstream_vortex_scan_called",
                if scanned { "true" } else { "false" },
            ),
            ("arrow_converted", if converted { "true" } else { "false" }),
            ("native_vortex_array_sink_adapter_payload_bytes_copied", "0"),
            ("native_vortex_array_sink_scalar_values_materialized", "0"),
            (
                "native_vortex_array_sink_source_generation_validated",
                "true",
            ),
            (
                "native_vortex_array_sink_dtype_and_row_count_validated",
                "true",
            ),
            ("public_workflow_fallback_attempted", "false"),
            ("public_workflow_external_engine_invoked", "false"),
            (
                "native_vortex_result_export_fanout_atomicity_contract",
                "single_target_atomic_create_if_absent_hard_link_same_directory",
            ),
        ] {
            assert!(stdout.contains(&field(key, value)), "{key}: {stdout}");
        }
        let contract = if format == "arrow-ipc" {
            "native_vortex_arrays_to_arrow_ipc_compatibility_sink"
        } else {
            "native_vortex_arrays_to_parquet_compatibility_sink"
        };
        assert!(stdout.contains(&field("typed_sink_contract", contract)));
        assert!(stdout.contains(&field(
            "native_vortex_result_export_target_commit_modes",
            &format!("primary:{format}:atomic_create_if_absent_hard_link_same_directory")
        )));
        assert!(stdout.contains("read_decode_materialize_flags_conservative_scan_scope"));
        assert!(!stdout.contains("native_scan_arrays_to_native_flat_writer"));
    }
    #[test]
    fn public_columnar_compatibility_reopens_complete_nullable_values_and_filter_limits() {
        let fixture = Fixture::new();
        for format in ["arrow-ipc", "parquet"] {
            for (name, predicate, limit, start, count) in [
                ("complete", None, None, 0, ROWS),
                ("filtered", Some("gte:row_ordinal:3"), Some("2051"), 3, 2051),
            ] {
                let output = fixture.0.join(format!("{name}.{format}"));
                let stdout = export(&fixture.source(), &output, format, predicate, limit);
                assert_boundary(&stdout, format, true, true);
                assert_eq!(
                    read(&output, format),
                    (start..start + count).map(oracle).collect::<Vec<_>>()
                );
                let envelope = serde_json::from_str::<Value>(&stdout).unwrap();
                assert!(number(&envelope, "arrow_batches") > 0);
                assert_eq!(
                    number(&envelope, "arrow_batches"),
                    number(&envelope, "native_batches")
                );
                assert!(number(&envelope, "native_logical_bytes") > 0);
                assert!(number(&envelope, "admitted_arrow_expansion_bytes") > 0);
                assert!(number(&envelope, "max_arrow_batch_bytes") > 0);
                assert!(number(&envelope, "writer_reserved_bytes") > 0);
                assert_eq!(
                    number(&envelope, "output_bytes"),
                    fs::metadata(&output).unwrap().len()
                );
                if format == "parquet" {
                    assert!(number(&envelope, "max_observed_parquet_in_progress_bytes") > 0);
                } else {
                    assert_eq!(
                        number(&envelope, "max_observed_parquet_in_progress_bytes"),
                        0
                    );
                }
            }
        }
    }
    #[test]
    fn public_columnar_compatibility_distinguishes_pruned_and_unprunable_empty_scans() {
        let fixture = Fixture::new();
        for format in ["arrow-ipc", "parquet"] {
            for (name, predicate, scanned) in [
                ("pruned", "gte:row_ordinal:10000", false),
                // Seventeen is absent but lies between the exact signed extrema.
                ("unprunable", "eq:exact_identifier:17", true),
            ] {
                let output = fixture.0.join(format!("{name}.{format}"));
                let stdout = export(&fixture.source(), &output, format, Some(predicate), None);
                assert_boundary(&stdout, format, scanned, false);
                assert!(read(&output, format).is_empty());
                let envelope = serde_json::from_str::<Value>(&stdout).unwrap();
                for name in [
                    "native_batches",
                    "arrow_batches",
                    "native_logical_bytes",
                    "admitted_arrow_expansion_bytes",
                    "max_arrow_batch_bytes",
                    "max_observed_parquet_in_progress_bytes",
                ] {
                    assert_eq!(number(&envelope, name), 0, "{name}");
                }
                assert_eq!(
                    number(&envelope, "output_bytes"),
                    fs::metadata(output).unwrap().len()
                );
                assert!(number(&envelope, "writer_reserved_bytes") > 0);
                assert!(stdout.contains(&field("native_vortex_result_export_rows_written", "0")));
            }
        }
    }
}

#[cfg(all(
    feature = "vortex-local-primitives",
    feature = "vortex-write",
    feature = "universal-format-io"
))]
#[test]
#[allow(clippy::too_many_lines)]
fn public_numeric_sort_spill_sql_and_dataframe_return_complete_values_and_cleanup() {
    use arrow_array::{Int64Array, RecordBatch, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    const ROWS: usize = 131_072;
    const OFFSET: usize = 123_456;
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = unique_vortex_binding_dir("native-sort-spill");
    std::fs::create_dir(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let workspace = root.join("workspace");
    let ipc = root.join("source.arrow");
    let source = root.join("shipments.vortex");
    let keys = (0..ROWS)
        .map(|index| (1_i64 << 60) + i64::try_from((index * 37) % 997).unwrap())
        .collect::<Vec<_>>();
    let labels = (0..ROWS)
        .map(|index| format!("港-{index}-shipment"))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![
        Field::new("priority", DataType::Int64, false),
        Field::new("shipment_sequence", DataType::UInt64, false),
        Field::new("destination", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(keys.clone())),
            Arc::new(UInt64Array::from_iter_values(
                0..u64::try_from(ROWS).unwrap(),
            )),
            Arc::new(StringArray::from(labels.clone())),
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
    let payload = serde_json::json!({"order_by":[{"column":"priority","descending":true}],"offset":OFFSET,"spill":{"workspace":workspace,"memory_bytes":4_194_304,"quota_bytes":33_554_432}}).to_string();
    let sql = format!(
        "SELECT * FROM '{}' ORDER BY priority DESC LIMIT 7 OFFSET {OFFSET}",
        source.display()
    );
    // Inspecting the route must not create even the caller's workspace.
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
        "--vortex-sort-rows",
        &payload,
        "--format",
        "json",
    ]);
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(!workspace.exists());
    std::fs::create_dir(&workspace).unwrap();
    let mut order = (0..ROWS).collect::<Vec<_>>();
    order.sort_unstable_by(|left, right| {
        keys[*right].cmp(&keys[*left]).then_with(|| left.cmp(right))
    });
    let expected = order[OFFSET..OFFSET + 7].iter().map(|index| serde_json::json!({"priority":keys[*index],"shipment_sequence":index,"destination":labels[*index]})).collect::<Vec<_>>();
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
            "--vortex-sort-rows",
            &payload,
            "--max-parallelism",
            "1",
            "--format",
            "json",
        ];
        if surface == "sql" {
            args.extend(["--sql", &sql]);
        } else {
            args.extend([
                "--vortex-primitive",
                "sort_rows",
                "--vortex-source-order-limit",
                "7",
            ]);
        }
        let (ok, stdout) = run_facade(&args);
        assert!(ok, "{surface}: {stdout}");
        let envelope: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(envelope["status"], "success");
        let summary = envelope["human_text"]
            .as_str()
            .unwrap()
            .lines()
            .find(|line| line.starts_with("result summary: "))
            .unwrap();
        let values: serde_json::Value =
            serde_json::from_str(summary.split_once(" values=").unwrap().1).unwrap();
        assert_eq!(values["values"], serde_json::json!(expected));
        assert!(
            values["native_sort_spill"]["runs_written"]
                .as_u64()
                .unwrap()
                > 8
        );
        assert!(
            values["native_sort_spill"]["owned_cleanup_completed"]
                .as_bool()
                .unwrap()
        );
        assert!(
            values["native_sort_spill"]["merge_passes"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(values["native_sort_spill"]["run_block_rows"], 1024);
        assert_eq!(values["native_sort_spill"]["merge_fan_in"], 8);
        assert_eq!(
            values["native_sort_spill"]["runs_written"],
            values["native_sort_spill"]["runs_validated"]
        );
        assert!(
            values["native_sort_spill"]["peak_reserved_bytes"]
                .as_u64()
                .unwrap()
                <= 4_194_304
        );
        assert!(
            values["native_sort_spill"]["peak_disk_bytes"]
                .as_u64()
                .unwrap()
                <= 33_554_432
        );
        assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
        assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
        assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
    }
    let invalid = serde_json::json!({"order_by":[{"column":"priority","descending":true}],"spill":{"workspace":workspace,"memory_bytes":4_194_304,"quota_bytes":32_769}}).to_string();
    let (ok, stdout) = run_facade(&[
        "run",
        "dataframe",
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
        "--vortex-primitive",
        "sort_rows",
        "--vortex-sort-rows",
        &invalid,
        "--vortex-source-order-limit",
        "7",
        "--format",
        "json",
    ]);
    assert!(!ok, "{stdout}");
    assert!(stdout.contains("quota"), "{stdout}");
    assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
}

#[cfg(feature = "vortex-local-primitives")]
fn local_primitive_struct_fixture() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("shardloom-vortex")
        .join("tests")
        .join("fixtures")
        .join("local_primitive_struct_five.vortex")
        .display()
        .to_string()
}

#[cfg(feature = "vortex-local-primitives")]
fn unique_vortex_binding_dir(name: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    unique_vortex_binding_dir_at(name, nanos)
}

#[cfg(feature = "vortex-local-primitives")]
fn unique_vortex_binding_dir_at(name: &str, nanos: u128) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    // Clock precision does not guarantee different readings in parallel tests.
    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
    let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "shardloom-public-{name}-{}-{nanos}-{sequence}",
        std::process::id()
    ))
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn vortex_binding_directories_are_unique_for_parallel_calls_in_one_clock_tick() {
    let paths = std::thread::scope(|scope| {
        let workers = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    (0..32)
                        .map(|_| unique_vortex_binding_dir_at("same-clock-tick", 42))
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    let count = paths.len();
    let unique = paths.into_iter().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(count, 256);
    assert_eq!(unique.len(), count);
}

#[cfg(feature = "vortex-local-primitives")]
fn copy_partitioned_vortex_fixture(name: &str) -> std::path::PathBuf {
    let dir = unique_vortex_binding_dir(name);
    std::fs::create_dir_all(&dir).expect("create partitioned fixture dir");
    let fixture = std::path::PathBuf::from(local_primitive_struct_fixture());
    std::fs::copy(&fixture, dir.join("part-000.vortex")).expect("copy first partition");
    std::fs::copy(&fixture, dir.join("part-001.vortex")).expect("copy second partition");
    dir
}

fn field(key: &str, value: &str) -> String {
    format!("\"key\":\"{key}\",\"value\":\"{value}\"")
}

fn run_route(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("run shardloom route");
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn run_facade(args: &[&str]) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("run shardloom facade");
    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout is utf8"),
    )
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
#[allow(clippy::too_many_lines)]
fn public_run_native_vortex_aggregate_emits_state_budget_and_pulseweave_evidence() {
    let fixture = local_primitive_struct_fixture();
    let aggregate = r#"{"measures":[{"function":"sum","column":"metric","alias":"sum_metric"},{"function":"count","alias":"rows"}]}"#;
    let (ok, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        &fixture,
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--vortex-primitive",
        "aggregate",
        "--vortex-aggregate",
        aggregate,
        "--memory-gb",
        "4",
        "--max-parallelism",
        "2",
        "--format",
        "json",
    ]);

    assert!(ok, "{stdout}");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "native_vortex_aggregate"
    )));
    assert!(stdout.contains(&field("public_workflow_timing_surface", "hot_runtime")));
    assert!(stdout.contains(&field(
        "public_workflow_actual_evidence_tier",
        "metadata_sink"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_timing_claim_boundary",
        "runtime_route_evidence_only_no_benchmark_or_publication_claim"
    )));
    assert!(stdout.contains(&field("timing_surface", "hot_runtime")));
    assert!(stdout.contains(&field("actual_evidence_tier", "metadata_sink")));
    assert!(stdout.contains(&field("route_total_timing_reported", "false")));
    assert!(stdout.contains(&field(
        "result_sink_timing_included_in_route_total",
        "false"
    )));
    assert!(stdout.contains(&field(
        "evidence_render_timing_included_in_route_total",
        "false"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_state_budget_schema_version",
        "shardloom.local_vortex_state_budget.v2"
    )));
    assert!(stdout.contains(&field("local_primitive_state_budget_required", "true")));
    assert!(stdout.contains(&field(
        "local_primitive_state_family",
        "scalar_aggregate_state+direct_dictionary_or_typed"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_state_pressure_class",
        "low_cardinality_pressure"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_state_budget_status",
        "bounded_in_memory_low_pressure_spill_not_required"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_physical_policy_schema_version",
        "shardloom.local_vortex_physical_policy.v1"
    )));
    assert!(stdout.contains("local_primitive_physical_policy_summary"));
    assert!(stdout.contains("selected_max_parallelism=2"));
    assert!(stdout.contains(&field(
        "local_primitive_physical_policy_route_family",
        "stateless_scan_count"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_physical_policy_state_pressure_reason",
        "scalar_aggregate_without_group_state"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_physical_policy_requested_max_parallelism",
        "2"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_physical_policy_selected_max_parallelism",
        "2"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_physical_policy_selected_scan_concurrency_per_worker",
        "2"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_physical_policy_selected_group_state_soft_item_budget",
        "33554432"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_physical_policy_selected_string_topk_heavy_hitter_capacity",
        "65536"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_physical_policy_selected_numeric_utf8_topk_heavy_hitter_capacity",
        "65536"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_physical_policy_rejected_alternatives",
        "state_heavy_hitter_capacity,row_ref_retention"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_capillary_work_units",
        "vortex_scan,aggregate_state,dictionary_or_typed_direct_scalar_aggregate"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_pulseweave_pressure_signals",
        "aggregate_measure_count,aggregate_input_rows,row_materialization_bypass"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_spill_policy",
        "fail_closed_before_uncertified_spill"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_fail_closed_if_spill_required",
        "true"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_native_vortex_directory_count_uses_partitioned_binding() {
    let dir = copy_partitioned_vortex_fixture("directory-count");
    let (ok, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        &dir.display().to_string(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--vortex-primitive",
        "count",
        "--format",
        "json",
    ]);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(ok, "{stdout}");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "native_vortex_count_all"
    )));
    assert!(stdout.contains(&field(
        "native_vortex_input_binding_schema_version",
        "shardloom.native_vortex_input_binding.v1"
    )));
    assert!(stdout.contains(&field(
        "native_vortex_input_binding_mode",
        "local_directory"
    )));
    assert!(stdout.contains(&field("native_vortex_input_binding_count", "2")));
    assert!(stdout.contains(&field(
        "native_vortex_input_binding_strategy",
        "sequential_capillary_parts"
    )));
    assert!(stdout.contains(&field("native_vortex_partitioned_input_binding", "true")));
    assert!(stdout.contains(&field("local_primitive_rows_scanned", "10")));
    assert!(stdout.contains(&field("local_primitive_rows_selected", "10")));
    assert!(stdout.contains(&field("local_primitive_mode", "metadata_preserving_count")));
    assert!(stdout.contains(&field(
        "local_primitive_metadata_elimination_stage",
        "after_vortex_normalization_before_operator_execution"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_metadata_elimination_outcome",
        "metadata_answered_without_row_scan"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_metadata_elimination_attempted",
        "true"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_planner_consumption_status",
        "partitioned_metadata_row_count_consumed"
    )));
    assert!(stdout.contains(&field("local_primitive_decode_avoided_by_metadata", "true")));
    assert!(stdout.contains(&field(
        "local_primitive_physical_policy_route_family",
        "stateless_scan_count"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_physical_policy_state_pressure_reason",
        "metadata_or_scan_pushdown_without_unbounded_state"
    )));
    assert!(stdout.contains(&field("data_read", "false")));
    assert!(stdout.contains(&field("data_decoded", "false")));
    assert!(stdout.contains(&field("data_materialized", "false")));
    assert!(stdout.contains(&field("upstream_vortex_scan_called", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_native_vortex_directory_count_accepts_vtx_parts() {
    let dir = copy_partitioned_vortex_fixture("directory-count-vtx");
    std::fs::rename(dir.join("part-001.vortex"), dir.join("part-001.vtx"))
        .expect("rename partition to .vtx");
    let (ok, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        &dir.display().to_string(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--vortex-primitive",
        "count",
        "--format",
        "json",
    ]);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(ok, "{stdout}");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "native_vortex_input_binding_mode",
        "local_directory"
    )));
    assert!(stdout.contains(&field("native_vortex_input_binding_count", "2")));
    assert!(stdout.contains(&field("local_primitive_rows_scanned", "10")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_native_vortex_manifest_aggregate_uses_partitioned_state() {
    let dir = copy_partitioned_vortex_fixture("manifest-aggregate");
    let manifest = dir.join("parts.vortex-manifest");
    std::fs::write(
        &manifest,
        r#"{"inputs":["part-000.vortex","part-001.vortex"]}"#,
    )
    .expect("write manifest");
    let expected_sources = format!(
        "{},{}",
        dir.join("part-000.vortex").display(),
        dir.join("part-001.vortex").display()
    );
    let aggregate = r#"{"measures":[{"function":"sum","column":"metric","alias":"sum_metric"},{"function":"count","alias":"rows"}]}"#;
    let (ok, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        &manifest.display().to_string(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--vortex-primitive",
        "aggregate",
        "--vortex-aggregate",
        aggregate,
        "--format",
        "json",
    ]);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(ok, "{stdout}");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "native_vortex_aggregate"
    )));
    assert!(stdout.contains(&field("native_vortex_input_binding_mode", "manifest")));
    assert!(stdout.contains(&field("native_vortex_input_binding_count", "2")));
    assert!(stdout.contains(&field(
        "native_vortex_input_binding_sources",
        &expected_sources
    )));
    assert!(stdout.contains(&field(
        "native_vortex_input_binding_strategy",
        "sequential_capillary_parts"
    )));
    assert!(stdout.contains(&field("native_vortex_partitioned_input_binding", "true")));
    assert!(stdout.contains(&field("local_primitive_rows_scanned", "10")));
    assert!(stdout.contains(&field("local_primitive_rows_selected", "10")));
    assert!(stdout.contains(&field("local_primitive_rows_projected", "1")));
    assert!(stdout.contains(&field(
        "local_primitive_capillary_work_units",
        "partitioned_vortex_source,vortex_scan,aggregate_state,dictionary_or_typed_direct_scalar_aggregate"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_pulseweave_pressure_signals",
        "partition_count,aggregate_measure_count,aggregate_input_rows,row_materialization_bypass"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_native_vortex_manifest_rejects_duplicate_entries() {
    let dir = copy_partitioned_vortex_fixture("manifest-duplicate");
    let manifest = dir.join("parts.vortex-manifest");
    std::fs::write(
        &manifest,
        r#"{"inputs":["part-000.vortex","part-000.vortex"]}"#,
    )
    .expect("write manifest");
    let (ok, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        &manifest.display().to_string(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--vortex-primitive",
        "count",
        "--format",
        "json",
    ]);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(!ok, "{stdout}");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("native Vortex input manifest contains duplicate entry"));
    assert!(stdout.contains("no fallback execution was attempted"));
}

#[test]
fn public_route_routes_local_file_vortex_middle_without_direct_runtime() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/input.csv",
        "--input-format",
        "csv",
        "--plan",
        "read_csv(target/input.csv) -> select(id) -> limit(10)",
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_schema_version",
        "shardloom.public_workflow_route.v1"
    )));
    if cfg!(all(
        feature = "vortex-write",
        feature = "vortex-local-primitives"
    )) {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "local_file_prepare_once_first_query")));
        assert!(stdout.contains(&field("route_support_status", "global_runtime_supported")));
        assert!(stdout.contains(&field("native_vortex_plan_contract_status", "admitted")));
        assert!(stdout.contains(&field(
            "native_vortex_plan_route_family",
            "native_vortex_unified_plan"
        )));
        assert!(stdout.contains(&field(
            "native_vortex_plan_payload_kind",
            "prepared_compatibility_source"
        )));
        assert!(stdout.contains(&field(
            "resolved_internal_command",
            "vortex-prepare->vortex-production-runtime-run"
        )));
        assert!(stdout.contains(&field(
            "underlying_runtime_command",
            "vortex-prepare->vortex-production-runtime-run"
        )));
        assert!(stdout.contains(&field("start_state", "compatibility_local_source")));
        assert!(stdout.contains(&field("vortex_normalization_point", "VortexPreparedState")));
        assert!(stdout.contains(&field("vortex_middle_status", "prepared_vortex_state")));
        assert!(stdout.contains(&field("execution_mode", "prepared_vortex")));
        assert!(stdout.contains(&field("preparation_included", "true")));
        assert!(stdout.contains(&field("query_timing_starts_after_preparation", "true")));
        assert!(stdout.contains(&field("blocker_id", "none")));
        assert!(stdout.contains(&field(
            "local_workflow_runtime_profile",
            "product_local_workflow"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        let expected_blocker = if cfg!(feature = "vortex-write") {
            "cg21.route.local_file_vortex_primitive_feature_gated"
        } else {
            "cg21.route.local_file_vortex_ingest_feature_gated"
        };
        assert!(stdout.contains(&field("blocker_id", expected_blocker)));
        assert!(stdout.contains(&field(
            "native_vortex_plan_contract_status",
            "blocked_before_execution"
        )));
        assert!(stdout.contains(&field(
            "native_vortex_plan_route_family",
            "native_vortex_unified_plan"
        )));
        assert!(stdout.contains(&field("route_support_status", "unsupported_boundary")));
        assert!(stdout.contains(&field("resolved_internal_command", "not_resolved")));
        assert!(stdout.contains(&field("underlying_runtime_command", "not_resolved")));
        assert!(stdout.contains(&field("start_state", "blocked")));
        assert!(stdout.contains(&field("vortex_normalization_point", "not_applicable")));
        assert!(stdout.contains(&field("vortex_middle_status", "blocked_or_unsupported")));
        assert!(stdout.contains(&field("execution_mode", "blocked")));
        assert!(stdout.contains(&field("preparation_included", "false")));
        assert!(stdout.contains(&field("query_timing_starts_after_preparation", "false")));
        assert!(stdout.contains(&field("local_workflow_runtime_profile", "not_applicable")));
    }
    assert!(stdout.contains(&field("surface", "dataframe")));
    assert!(stdout.contains(&field("source_format", "csv")));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("source_io_performed", "false")));
    assert!(stdout.contains(&field("output_io_performed", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_blocks_explicit_direct_local_file_policy() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/input.csv",
        "--input-format",
        "csv",
        "--plan",
        "read_csv(target/input.csv) -> select(id) -> limit(10)",
        "--request",
        "collect",
        "--execution-policy",
        "direct",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field("blocker_id", "cg21.route.direct_local_file_blocked")));
    assert!(stdout.contains(&field("resolved_internal_command", "not_resolved")));
    assert!(stdout.contains(&field("underlying_runtime_command", "not_resolved")));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_blocks_unbounded_collect_before_execution() {
    let stdout = run_route(&[
        "route",
        "python",
        "--input",
        "target/input.csv",
        "--input-format",
        "csv",
        "--plan",
        "read_csv(target/input.csv)",
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field("blocker_id", "cg21.route.unbounded_collect_blocked")));
    assert!(stdout.contains(&field("resolved_internal_command", "not_resolved")));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_does_not_infer_scalar_path_literals_as_sources() {
    let stdout = run_route(&[
        "route",
        "sql",
        "--sql",
        "SELECT 'target/input.csv' AS label LIMIT 1",
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field("blocker_id", "cg21.route.input_not_declared")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_ignores_limit_inside_sql_comments() {
    let stdout = run_route(&[
        "route",
        "sql",
        "--sql",
        "SELECT id FROM 'target/input.csv' -- LIMIT 1\nWHERE id > 0",
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field("blocker_id", "cg21.route.unbounded_collect_blocked")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_blocks_unresolved_newline_from_source_without_declared_input() {
    let stdout = run_route(&[
        "route",
        "sql",
        "--sql",
        "SELECT *\nFROM events",
        "--request",
        "write_vortex",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field("blocker_id", "cg21.route.input_not_declared")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_rejects_native_vortex_sql_non_where_limit_tail() {
    let stdout = run_route(&[
        "route",
        "sql",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--sql",
        "SELECT id FROM 'target/fact.vortex' OFFSET 10 LIMIT 2",
        "--request",
        "collect",
        "--execution-policy",
        "native_vortex",
        "--bounded",
        "true",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(!stdout.contains(&field("route_id", "native_vortex_project")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_requires_vortex_input_for_native_vortex_policy() {
    let stdout = run_route(&[
        "route",
        "cli",
        "--input",
        "target/input.csv",
        "--input-format",
        "csv",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field(
        "blocker_id",
        "cg21.route.native_vortex_input_required"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_blocks_collect_fanout_before_execution() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/input.csv",
        "--input-format",
        "csv",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--fanout-output",
        "csv=target/out.csv",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field("blocker_id", "cg21.route.collect_fanout_blocked")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_admits_native_vortex_primitive_row_export_payloads() {
    let _ = std::fs::remove_file("target/native-vortex-output.jsonl");
    let stdout = run_route(&[
        "route",
        "cli",
        "--input",
        "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
        "--input-format",
        "vortex",
        "--request",
        "write_jsonl",
        "--execution-policy",
        "native_vortex",
        "--output",
        "target/native-vortex-output.jsonl",
        "--bounded",
        "true",
        "--vortex-primitive",
        "project",
        "--vortex-columns",
        "metric",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_primitive_row_export")));
        assert!(stdout.contains(&field(
            "resolved_internal_command",
            "vortex-local-primitive-row-export"
        )));
        assert!(stdout.contains(&field("native_vortex_operation_family", "sink")));
        assert!(stdout.contains(&field(
            "typed_sink_contract",
            "native_vortex_primitive_row_stream_to_jsonl_csv_compatibility_sink"
        )));
        assert!(stdout.contains(&field(
            "decode_materialization_boundary",
            "native_vortex_scan_pushdown_then_selected_column_decode_at_compatibility_sink"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        )));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_admits_native_vortex_tail_row_export_payloads() {
    let stdout = run_route(&[
        "route",
        "cli",
        "--input",
        "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
        "--input-format",
        "vortex",
        "--request",
        "write_jsonl",
        "--execution-policy",
        "native_vortex",
        "--output",
        "target/native-vortex-tail-output.jsonl",
        "--bounded",
        "true",
        "--vortex-primitive",
        "tail",
        "--vortex-columns",
        "metric",
        "--vortex-source-order-limit",
        "2",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_primitive_row_export")));
        assert!(stdout.contains(&field("vortex_primitive", "tail")));
        assert!(stdout.contains(&field("vortex_source_order_limit", "2")));
        assert!(stdout.contains(&field(
            "typed_sink_contract",
            "native_vortex_primitive_row_stream_to_jsonl_csv_compatibility_sink"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        )));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_blocks_native_vortex_tail_collect_without_explicit_count() {
    let stdout = run_route(&[
        "route",
        "cli",
        "--input",
        "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--execution-policy",
        "native_vortex",
        "--bounded",
        "true",
        "--vortex-primitive",
        "tail",
        "--vortex-columns",
        "metric",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field(
        "blocker_id",
        "cg21.route.native_vortex_payload_invalid"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_admits_native_vortex_sample_row_export_payloads() {
    let stdout = run_route(&[
        "route",
        "cli",
        "--input",
        "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
        "--input-format",
        "vortex",
        "--request",
        "write_csv",
        "--execution-policy",
        "native_vortex",
        "--output",
        "target/native-vortex-sample-output.csv",
        "--bounded",
        "true",
        "--vortex-primitive",
        "sample",
        "--vortex-columns",
        "metric",
        "--vortex-source-order-limit",
        "2",
        "--vortex-sample-seed",
        "7",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_primitive_row_export")));
        assert!(stdout.contains(&field("vortex_primitive", "sample")));
        assert!(stdout.contains(&field("vortex_source_order_limit", "2")));
        assert!(stdout.contains(&field("vortex_sample_seed", "7")));
        assert!(stdout.contains(&field("vortex_sample_fraction", "none")));
        assert!(stdout.contains(&field(
            "typed_sink_contract",
            "native_vortex_primitive_row_stream_to_jsonl_csv_compatibility_sink"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        )));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_admits_native_vortex_sort_row_export_payloads() {
    let _ = std::fs::remove_file("target/native-vortex-sort-output.jsonl");
    let stdout = run_route(&[
        "route",
        "cli",
        "--input",
        "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
        "--input-format",
        "vortex",
        "--request",
        "write_jsonl",
        "--execution-policy",
        "native_vortex",
        "--output",
        "target/native-vortex-sort-output.jsonl",
        "--bounded",
        "true",
        "--vortex-primitive",
        "sort_rows",
        "--vortex-columns",
        "value,metric",
        "--vortex-source-order-limit",
        "2",
        "--vortex-sort-rows",
        r#"{"order_by":[{"column":"metric","descending":true}]}"#,
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_primitive_row_export")));
        assert!(stdout.contains(&field("vortex_primitive", "sort_rows")));
        assert!(stdout.contains(&field("vortex_source_order_limit", "2")));
        assert!(stdout.contains(&field("vortex_sort_rows_present", "true")));
        assert!(stdout.contains(&field(
            "typed_sink_contract",
            "native_vortex_primitive_row_stream_to_jsonl_csv_compatibility_sink"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        )));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_admits_native_vortex_sample_fraction_payloads() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--execution-policy",
        "native_vortex",
        "--bounded",
        "true",
        "--vortex-primitive",
        "sample",
        "--vortex-columns",
        "metric",
        "--vortex-sample-fraction",
        "0.5",
        "--vortex-sample-seed",
        "7",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_sample")));
        assert!(stdout.contains(&field("vortex_primitive", "sample")));
        assert!(stdout.contains(&field("vortex_source_order_limit", "none")));
        assert!(stdout.contains(&field("vortex_sample_fraction", "0.5")));
        assert!(stdout.contains(&field("vortex_sample_seed", "7")));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        )));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-production-runtime")]
#[test]
fn public_route_admits_provider_backed_native_vortex_jsonl_result_sink() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--plan",
        "read_vortex(target/fact.vortex) -> with_column(amount_float,CAST(dirty_numeric AS float64)) -> filter(amount_float >= 0) -> limit(1000)",
        "--request",
        "write_jsonl",
        "--execution-policy",
        "native_vortex",
        "--output",
        "target/native-provider-result.jsonl",
        "--allow-overwrite",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("route_id", "native_vortex_user_sink")));
    assert!(stdout.contains(&field(
        "resolved_internal_command",
        "vortex-production-runtime-run"
    )));
    assert!(stdout.contains(&field("requested_output", "write_jsonl")));
    assert!(stdout.contains(&field("native_vortex_operation_family", "sink")));
    assert!(stdout.contains(&field(
        "native_vortex_provider_scenario",
        "clean-cast-filter-write"
    )));
    assert!(stdout.contains(&field(
        "typed_sink_contract",
        "native_vortex_provider_result_json_export_with_workspace_safe_sink"
    )));
    assert!(stdout.contains(&field(
        "decode_materialization_boundary",
        "native_vortex_zero_decode_runtime_with_bounded_result_json_sink_materialization"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-production-runtime")]
#[test]
fn public_route_admits_provider_backed_native_vortex_cast_collect_shapes() {
    for (plan, scenario) in [
        (
            "read_vortex(target/fact.vortex) -> with_column(amount_float,CAST(dirty_numeric AS float64)) -> filter(amount_float >= 0) -> limit(1000)",
            "clean-cast-filter-write",
        ),
        (
            "read_vortex(target/fact.vortex) -> with_column(event_day,CAST(raw_event_time AS date32)) -> limit(1000)",
            "malformed-timestamp-dirty-csv",
        ),
    ] {
        let stdout = run_route(&[
            "route",
            "dataframe",
            "--input",
            "target/fact.vortex",
            "--input-format",
            "vortex",
            "--plan",
            plan,
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--format",
            "json",
        ]);

        assert!(stdout.contains("\"command\":\"route\""));
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_user_cast")));
        assert!(stdout.contains(&field(
            "resolved_internal_command",
            "vortex-production-runtime-run"
        )));
        assert!(stdout.contains(&field("native_vortex_operation_family", "cast")));
        assert!(stdout.contains(&field("native_vortex_provider_scenario", scenario)));
        assert!(stdout.contains(&field(
            "route_support_status",
            "production_admitted_local_workflow"
        )));
        assert!(stdout.contains(&field("fallback_attempted", "false")));
        assert!(stdout.contains(&field("external_engine_invoked", "false")));
    }
}

#[test]
fn public_route_infers_native_vortex_distinct_without_smoke_middle() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--plan",
        "read_vortex(target/fact.vortex) -> select(id,group_key) -> distinct() -> limit(10)",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_distinct")));
        assert!(stdout.contains(&field("native_vortex_operation_family", "distinct")));
        assert!(stdout.contains(&field("resolved_internal_command", "vortex-run")));
        assert!(stdout.contains(&field("vortex_primitive", "distinct")));
        assert!(stdout.contains(&field("vortex_columns", "id,group_key")));
        assert!(stdout.contains(&field("vortex_source_order_limit", "10")));
        assert!(stdout.contains(&field(
            "route_support_status",
            "production_admitted_local_workflow"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        )));
        assert!(stdout.contains(&field(
            "native_vortex_required_feature_gate",
            "vortex-local-primitives"
        )));
        assert!(stdout.contains(&field("native_vortex_capability_status", "feature_gated")));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_infers_native_vortex_sample_without_smoke_middle() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--plan",
        "read_vortex(target/fact.vortex) -> filter(gte:value:3) -> select(id,group_key) -> sample(n=10,seed=7)",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_sample")));
        assert!(stdout.contains(&field("native_vortex_operation_family", "sample")));
        assert!(stdout.contains(&field("resolved_internal_command", "vortex-run")));
        assert!(stdout.contains(&field("vortex_primitive", "sample")));
        assert!(stdout.contains(&field("vortex_predicate", "gte:value:3")));
        assert!(stdout.contains(&field("vortex_columns", "id,group_key")));
        assert!(stdout.contains(&field("vortex_source_order_limit", "10")));
        assert!(stdout.contains(&field("vortex_sample_seed", "7")));
        assert!(stdout.contains(&field(
            "route_support_status",
            "production_admitted_local_workflow"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        )));
        assert!(stdout.contains(&field(
            "native_vortex_required_feature_gate",
            "vortex-local-primitives"
        )));
        assert!(stdout.contains(&field("native_vortex_capability_status", "feature_gated")));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_infers_native_vortex_sample_fraction_without_smoke_middle() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--plan",
        "read_vortex(target/fact.vortex) -> filter(gte:value:3) -> select(id,group_key) -> sample(fraction,0.5,7)",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_sample")));
        assert!(stdout.contains(&field("native_vortex_operation_family", "sample")));
        assert!(stdout.contains(&field("resolved_internal_command", "vortex-run")));
        assert!(stdout.contains(&field("vortex_primitive", "sample")));
        assert!(stdout.contains(&field("vortex_predicate", "gte:value:3")));
        assert!(stdout.contains(&field("vortex_columns", "id,group_key")));
        assert!(stdout.contains(&field("vortex_source_order_limit", "none")));
        assert!(stdout.contains(&field("vortex_sample_fraction", "0.5")));
        assert!(stdout.contains(&field("vortex_sample_seed", "7")));
        assert!(stdout.contains(&field(
            "route_support_status",
            "production_admitted_local_workflow"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        )));
        assert!(stdout.contains(&field(
            "native_vortex_required_feature_gate",
            "vortex-local-primitives"
        )));
        assert!(stdout.contains(&field("native_vortex_capability_status", "feature_gated")));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_ignores_scoped_set_index_metadata_for_native_vortex_shape() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--plan",
        "read_vortex(target/fact.vortex) -> select(id,group_key) -> set_index(id) -> limit(10)",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("route_id", "native_vortex_project")));
    assert!(stdout.contains(&field(
        "native_vortex_operation_family",
        "filter_project_limit"
    )));
    assert!(stdout.contains(&field("resolved_internal_command", "vortex-project")));
    assert!(stdout.contains(&field("vortex_primitive", "project")));
    assert!(stdout.contains(&field("vortex_columns", "id,group_key")));
    assert!(stdout.contains(&field("vortex_source_order_limit", "10")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_admits_payloadless_native_vortex_metadata_profile_without_smoke_middle() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--plan",
        "read_vortex(target/fact.vortex)",
        "--request",
        "profile",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("route_id", "native_vortex_user_profile")));
    assert!(stdout.contains(&field("route_status", "admitted")));
    assert!(stdout.contains(&field(
        "resolved_internal_command",
        "vortex-metadata-summary"
    )));
    assert!(stdout.contains(&field(
        "vortex_middle_status",
        "native_vortex_metadata_profile"
    )));
    assert!(stdout.contains(&field("native_vortex_operation_family", "profile")));
    assert!(stdout.contains(&field("native_vortex_capability_status", "supported")));
    assert!(stdout.contains(&field("native_vortex_required_feature_gate", "default")));
    assert!(stdout.contains(&field(
        "typed_result_contract",
        "metadata_first_native_vortex_profile_summary"
    )));
    assert!(stdout.contains(&field(
        "decode_materialization_boundary",
        "metadata_only_no_decode_materialization"
    )));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_native_vortex_profile_marks_projected_metadata_scope() {
    let (_success, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--plan",
        "read_vortex(target/fact.vortex) -> select(id,label)",
        "--request",
        "profile",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "native_vortex_user_profile"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_profile_projection_scope",
        "selected_columns"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_profile_projected_columns",
        "id,label"
    )));
    assert!(stdout.contains(&field(
        "metadata_summary_projection_scope",
        "selected_columns"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_routes_local_sql_vortex_middle_without_direct_runtime() {
    let workspace = std::path::Path::new("target/public-workflow-run-facade");
    let _ = std::fs::remove_dir_all(workspace);
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let input = workspace.join("fact.csv");
    std::fs::write(&input, "id,label\n1,alpha\n2,beta\n3,gamma\n").expect("write csv");
    let statement = format!("SELECT id,label FROM '{}' LIMIT 2", input.display());
    let (success, stdout) = run_facade(&[
        "run",
        "sql",
        "--sql",
        &statement,
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains(&field(
        "public_workflow_facade_schema_version",
        "shardloom.public_workflow_execution_facade.v1"
    )));
    assert!(stdout.contains(&field("public_workflow_route_attached", "true")));
    if cfg!(all(
        feature = "vortex-write",
        feature = "vortex-local-primitives"
    )) {
        assert!(success);
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("public_workflow_route_id", "native_vortex_project")));
        assert!(stdout.contains(&field(
            "public_workflow_resolved_internal_command",
            "vortex-project"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_local_source_route_id",
            "local_file_prepare_once_first_query"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_local_source_vortex_ingest_performed",
            "true"
        )));
        assert!(stdout.contains(&field("project_local_execution_status", "executed")));
        assert!(stdout.contains(&field(
            "project_local_execution_data_decoded",
            if cfg!(unix) { "true" } else { "false" }
        )));
        assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
        assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
    } else {
        assert!(!success);
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("public_workflow_route_id", "blocked")));
        assert!(stdout.contains(&field(
            "public_workflow_blocker_id",
            "cg21.route.local_file_vortex_ingest_feature_gated"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_resolved_internal_command",
            "not_resolved"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_underlying_runtime_command",
            "not_resolved"
        )));
        assert!(stdout.contains(&field("runtime_execution", "false")));
        assert!(stdout.contains(&field("fallback_attempted", "false")));
        assert!(stdout.contains(&field("external_engine_invoked", "false")));
    }
    assert!(stdout.contains(&field(
        "public_workflow_local_workflow_runtime_profile",
        "not_applicable"
    )));
}

#[test]
fn public_run_blocks_extensionless_local_sql_source_but_preserves_declared_format() {
    let workspace = std::path::Path::new("target/public-workflow-extensionless-source");
    let _ = std::fs::remove_dir_all(workspace);
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let input = workspace.join("fact");
    std::fs::write(&input, "id,label\n1,alpha\n2,beta\n").expect("write extensionless csv");
    let statement = format!("SELECT id,label FROM '{}' LIMIT 1", input.display());
    let (success, stdout) = run_facade(&[
        "run",
        "sql",
        "--sql",
        &statement,
        "--input-format",
        "csv",
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    if cfg!(all(
        feature = "vortex-write",
        feature = "vortex-local-primitives"
    )) {
        assert!(success);
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("public_workflow_route_id", "native_vortex_project")));
        assert!(stdout.contains(&field(
            "public_workflow_local_source_vortex_ingest_performed",
            "true"
        )));
        assert!(stdout.contains(&field("project_local_execution_status", "executed")));
        assert!(stdout.contains(&field(
            "project_local_execution_data_decoded",
            if cfg!(unix) { "true" } else { "false" }
        )));
        assert!(stdout.contains(&field("public_workflow_local_source_format", "csv")));
        assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
        assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
    } else {
        assert!(!success);
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("public_workflow_route_id", "blocked")));
        assert!(stdout.contains(&field(
            "public_workflow_blocker_id",
            "cg21.route.local_file_vortex_ingest_feature_gated"
        )));
        assert!(stdout.contains(&field("runtime_execution", "false")));
        assert!(stdout.contains(&field("public_workflow_source_format", "csv")));
        assert!(stdout.contains(&field("fallback_attempted", "false")));
        assert!(stdout.contains(&field("external_engine_invoked", "false")));
    }
}

#[test]
fn public_run_executes_local_write_through_prepared_vortex_row_export() {
    let workspace = std::path::Path::new("target/public-workflow-write-facade");
    let _ = std::fs::remove_dir_all(workspace);
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let input = workspace.join("fact.csv");
    let output = workspace.join("out.csv");
    let _ = std::fs::remove_file(&output);
    std::fs::write(&input, "id,label\n1,alpha\n2,beta\n").expect("write csv");
    let statement = format!("SELECT id,label FROM '{}' LIMIT 2", input.display());
    let (success, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        input.to_str().expect("utf8 input path"),
        "--input-format",
        "csv",
        "--sql",
        &statement,
        "--plan",
        "read_csv(fact.csv) -> select(id,label) -> limit(2)",
        "--request",
        "write_csv",
        "--output",
        output.to_str().expect("utf8 output path"),
        "--allow-overwrite",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    if cfg!(all(
        feature = "vortex-write",
        feature = "vortex-local-primitives"
    )) {
        assert!(success, "{stdout}");
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field(
            "public_workflow_route_id",
            "native_vortex_primitive_row_export"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_local_source_route_id",
            "local_file_prepare_once_first_query"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_local_source_vortex_ingest_performed",
            "true"
        )));
        assert!(stdout.contains(&field("public_workflow_requested_output", "write_csv")));
        assert!(stdout.contains(&field("native_vortex_result_export_format", "csv")));
        assert!(stdout.contains(&field("native_vortex_result_export_rows_written", "2")));
        assert!(stdout.contains(&field("native_vortex_result_export_target_count", "1")));
        assert!(stdout.contains(&field("data_decoded", "true")));
        assert!(stdout.contains(&field("upstream_vortex_scan_called", "true")));
        assert_eq!(
            std::fs::read_to_string(&output).expect("read csv output"),
            "id,label\n1,alpha\n2,beta\n"
        );
    } else {
        assert!(!success);
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("public_workflow_route_id", "blocked")));
        let expected_blocker = if cfg!(feature = "vortex-write") {
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        } else {
            "cg21.route.local_file_vortex_ingest_feature_gated"
        };
        assert!(stdout.contains(&field("public_workflow_blocker_id", expected_blocker)));
        assert!(stdout.contains(&field("runtime_execution", "false")));
        assert!(stdout.contains(&field("output_io_performed", "false")));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_executes_local_fanout_through_prepared_vortex_row_export() {
    let workspace = std::path::Path::new("target/public-workflow-fanout-facade");
    let _ = std::fs::remove_dir_all(workspace);
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let input = workspace.join("fact.csv");
    let primary = workspace.join("out.jsonl");
    let fanout = workspace.join("out.csv");
    let _ = std::fs::remove_file(&primary);
    let _ = std::fs::remove_file(&fanout);
    std::fs::write(&input, "id,label\n1,alpha\n2,beta\n").expect("write csv");
    let statement = format!("SELECT id,label FROM '{}' LIMIT 2", input.display());
    let fanout_arg = format!("csv={}", fanout.to_str().expect("utf8 fanout path"));
    let (success, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        input.to_str().expect("utf8 input path"),
        "--input-format",
        "csv",
        "--sql",
        &statement,
        "--plan",
        "read_csv(fact.csv) -> select(id,label) -> limit(2)",
        "--request",
        "write_jsonl",
        "--output",
        primary.to_str().expect("utf8 primary path"),
        "--fanout-output",
        &fanout_arg,
        "--allow-overwrite",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    if cfg!(all(
        feature = "vortex-write",
        feature = "vortex-local-primitives"
    )) {
        assert!(success, "{stdout}");
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field(
            "public_workflow_route_id",
            "native_vortex_primitive_row_export"
        )));
        assert!(stdout.contains(&field("public_workflow_requested_output", "write_jsonl")));
        assert!(stdout.contains(&field("public_workflow_fanout_output_count", "1")));
        assert!(stdout.contains(&field("public_workflow_fanout_outputs", &fanout_arg)));
        assert!(stdout.contains(&field("native_vortex_result_export_target_count", "2")));
        assert!(stdout.contains(&field("native_vortex_result_export_fanout_count", "1")));
        assert!(stdout.contains(&field(
            "native_vortex_result_export_fanout_performed",
            "true"
        )));
        assert!(stdout.contains(&field(
            "native_vortex_result_export_target_formats",
            "jsonl,csv"
        )));
        assert!(stdout.contains(&field(
            "native_vortex_result_export_target_rows_written",
            "2,2"
        )));
        assert_eq!(
            std::fs::read_to_string(&primary).expect("read jsonl output"),
            "{\"id\":1,\"label\":\"alpha\"}\n{\"id\":2,\"label\":\"beta\"}\n"
        );
        assert_eq!(
            std::fs::read_to_string(&fanout).expect("read csv fanout"),
            "id,label\n1,alpha\n2,beta\n"
        );
    } else {
        assert!(!success);
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("public_workflow_route_id", "blocked")));
        let expected_blocker = if cfg!(feature = "vortex-write") {
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        } else {
            "cg21.route.local_file_vortex_ingest_feature_gated"
        };
        assert!(stdout.contains(&field("public_workflow_blocker_id", expected_blocker)));
        assert!(stdout.contains(&field(
            "public_workflow_underlying_runtime_command",
            "not_resolved"
        )));
        assert!(stdout.contains(&field("runtime_execution", "false")));
        assert!(stdout.contains(&field("output_io_performed", "false")));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(all(feature = "vortex-write", feature = "vortex-local-primitives"))]
#[test]
fn public_run_executes_local_file_vortex_middle_through_prepared_vortex_primitive() {
    let workspace = std::path::Path::new("target/public-workflow-local-vortex-facade");
    let _ = std::fs::remove_dir_all(workspace);
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let input = workspace.join("fact.csv");
    std::fs::write(&input, "id,value,metric\n1,2,1.5\n2,4,2.5\n3,6,3.5\n").expect("write csv");
    let plan = format!(
        "read_csv({}) -> filter(gte:value:3) -> select(metric,value) -> limit(2)",
        input.display()
    );
    let stdout = run_route(&[
        "run",
        "dataframe",
        "--input",
        input.to_str().expect("utf8 input path"),
        "--input-format",
        "csv",
        "--plan",
        &plan,
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "native_vortex_filter_project"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_local_source_route_id",
        "local_file_prepare_once_first_query"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_local_source_vortex_ingest_performed",
        "true"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_local_source_execution_mode",
        "prepared_vortex_then_native_vortex"
    )));
    assert!(stdout.contains(&field("public_workflow_source_format", "vortex")));
    assert!(stdout.contains(&field("public_workflow_vortex_primitive", "filter_project")));
    assert!(stdout.contains(&field("public_workflow_vortex_predicate", "gte:value:3")));
    assert!(stdout.contains(&field("public_workflow_vortex_columns", "metric,value")));
    assert!(stdout.contains(&field("filter_project_local_execution_status", "executed")));
    assert!(stdout.contains(&field(
        "filter_project_local_execution_data_decoded",
        if cfg!(unix) { "true" } else { "false" }
    )));
    assert!(stdout.contains(&field(
        "filter_project_local_execution_data_materialized",
        if cfg!(unix) { "true" } else { "false" }
    )));
    assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
    assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
    assert!(stdout.contains(&field(
        "filter_project_local_execution_fallback_attempted",
        "false"
    )));
}

#[test]
fn public_run_executes_generated_user_rows_with_attached_route_envelope() {
    let workspace = std::path::Path::new("target/public-workflow-generated-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let output = workspace.join("user-rows.jsonl");
    let _ = std::fs::remove_file(&output);
    let stdout = run_route(&[
        "run",
        "python",
        "--request",
        "write_jsonl",
        "--output",
        output.to_str().expect("utf8 output path"),
        "--bounded",
        "true",
        "--allow-overwrite",
        "--generated-source-kind",
        "user_rows",
        "--generated-schema",
        "id:int64,label:utf8",
        "--generated-rows",
        "id=1,label=alpha",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "generated_user_rows_direct_output"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "generated-source-user-rows"
    )));
    assert!(stdout.contains(&field("public_workflow_generated_source_kind", "user_rows")));
    assert!(stdout.contains(&field("public_workflow_requested_output", "write_jsonl")));
    assert!(stdout.contains(&field("public_workflow_allow_overwrite", "true")));
    assert!(stdout.contains(&field("generated_source_kind", "user_rows")));
    assert!(stdout.contains(&field("generated_source_row_count", "1")));
    assert!(stdout.contains(&field("output_format", "jsonl")));
    assert!(stdout.contains(&field(
        "output_path",
        output.to_str().expect("utf8 output path")
    )));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_forwards_generated_fanout_payload_with_attached_route_envelope() {
    let workspace = std::path::Path::new("target/public-workflow-generated-fanout-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let primary = workspace.join("user-rows.jsonl");
    let fanout = workspace.join("user-rows.csv");
    let _ = std::fs::remove_file(&primary);
    let _ = std::fs::remove_file(&fanout);
    let fanout_arg = format!("csv={}", fanout.to_str().expect("utf8 fanout path"));
    let stdout = run_route(&[
        "run",
        "python",
        "--request",
        "write_jsonl",
        "--output",
        primary.to_str().expect("utf8 primary path"),
        "--fanout-output",
        &fanout_arg,
        "--bounded",
        "true",
        "--allow-overwrite",
        "--generated-source-kind",
        "user_rows",
        "--generated-schema",
        "id:int64,label:utf8",
        "--generated-rows",
        "id=1,label=alpha",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "generated_user_rows_direct_output"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "generated-source-user-rows"
    )));
    assert!(stdout.contains(&field("public_workflow_generated_source_kind", "user_rows")));
    assert!(stdout.contains(&field("public_workflow_fanout_output_count", "1")));
    assert!(stdout.contains(&field("public_workflow_fanout_outputs", &fanout_arg)));
    assert!(stdout.contains(&field("generated_source_kind", "user_rows")));
    assert!(stdout.contains(&field("output_fanout_performed", "true")));
    assert!(stdout.contains(&field("fanout_output_count", "1")));
    assert!(stdout.contains(&field("fanout_output_formats", "csv")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_executes_generated_range_with_attached_route_envelope() {
    let workspace = std::path::Path::new("target/public-workflow-generated-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let output = workspace.join("range.csv");
    let _ = std::fs::remove_file(&output);
    let stdout = run_route(&[
        "run",
        "python",
        "--request",
        "write_csv",
        "--output",
        output.to_str().expect("utf8 output path"),
        "--bounded",
        "true",
        "--allow-overwrite",
        "--generated-source-kind",
        "range",
        "--generated-range-start",
        "1",
        "--generated-range-end",
        "4",
        "--generated-range-step",
        "1",
        "--generated-range-column",
        "id",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "generated_range_direct_output"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "generated-source-range"
    )));
    assert!(stdout.contains(&field("public_workflow_generated_source_kind", "range")));
    assert!(stdout.contains(&field("public_workflow_requested_output", "write_csv")));
    assert!(stdout.contains(&field("generated_source_kind", "range")));
    assert!(stdout.contains(&field("generated_source_range_start", "1")));
    assert!(stdout.contains(&field("generated_source_range_end", "4")));
    assert!(stdout.contains(&field("generated_source_range_step", "1")));
    assert!(stdout.contains(&field("generated_source_range_column", "id")));
    assert!(stdout.contains(&field("generated_source_row_count", "3")));
    assert!(stdout.contains(&field("output_format", "csv")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_executes_generated_sequence_with_attached_route_envelope() {
    let workspace = std::path::Path::new("target/public-workflow-generated-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let output = workspace.join("sequence.jsonl");
    let _ = std::fs::remove_file(&output);
    let stdout = run_route(&[
        "run",
        "python",
        "--request",
        "write_jsonl",
        "--output",
        output.to_str().expect("utf8 output path"),
        "--bounded",
        "true",
        "--allow-overwrite",
        "--generated-source-kind",
        "sequence",
        "--generated-range-start",
        "1",
        "--generated-range-end",
        "6",
        "--generated-range-step",
        "2",
        "--generated-range-column",
        "seq",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "generated_sequence_direct_output"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "generated-source-sequence"
    )));
    assert!(stdout.contains(&field("public_workflow_generated_source_kind", "sequence")));
    assert!(stdout.contains(&field("generated_source_kind", "sequence")));
    assert!(stdout.contains(&field("generated_source_range_start", "1")));
    assert!(stdout.contains(&field("generated_source_range_end", "6")));
    assert!(stdout.contains(&field("generated_source_range_step", "2")));
    assert!(stdout.contains(&field("generated_source_range_column", "seq")));
    assert!(stdout.contains(&field("generated_source_row_count", "3")));
    assert!(stdout.contains(&field("output_format", "jsonl")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_admits_native_vortex_filter_project_payload() {
    let stdout = run_route(&[
        "route",
        "cli",
        "--input",
        "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--execution-policy",
        "native_vortex",
        "--materialization-policy",
        "zero_decode",
        "--bounded",
        "true",
        "--vortex-primitive",
        "filter_project",
        "--vortex-predicate",
        "gte:value:3",
        "--vortex-columns",
        "metric,value",
        "--vortex-source-order-limit",
        "2",
        "--memory-gb",
        "3",
        "--max-parallelism",
        "2",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("route_id", "native_vortex_filter_project")));
    assert!(stdout.contains(&field(
        "native_vortex_plan_route_family",
        "native_vortex_unified_plan"
    )));
    assert!(stdout.contains(&field(
        "native_vortex_plan_payload_kind",
        "primitive_operator"
    )));
    assert!(stdout.contains(&field("resolved_internal_command", "vortex-filter-project")));
    assert!(stdout.contains(&field("start_state", "native_vortex_file")));
    assert!(stdout.contains(&field("execution_mode", "native_vortex")));
    assert!(stdout.contains(&field("vortex_primitive", "filter_project")));
    assert!(stdout.contains(&field("vortex_predicate", "gte:value:3")));
    assert!(stdout.contains(&field("vortex_columns", "metric,value")));
    assert!(stdout.contains(&field("vortex_source_order_limit", "2")));
    assert!(stdout.contains(&field("memory_gb", "3")));
    assert!(stdout.contains(&field("max_parallelism", "2")));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("source_io_performed", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_blocks_native_vortex_missing_required_payload() {
    let stdout = run_route(&[
        "route",
        "cli",
        "--input",
        "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--execution-policy",
        "native_vortex",
        "--bounded",
        "true",
        "--vortex-primitive",
        "count_where",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field(
        "blocker_id",
        "cg21.route.native_vortex_payload_invalid"
    )));
    assert!(stdout.contains(&field("vortex_primitive", "count_where")));
    assert!(stdout.contains("\"feature\":\"public_workflow_route.vortex_predicate\""));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_executes_native_vortex_filter_project_payload_with_attached_route_envelope() {
    let fixture = local_primitive_struct_fixture();
    let stdout = run_route(&[
        "run",
        "cli",
        "--input",
        fixture.as_str(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--execution-policy",
        "native_vortex",
        "--materialization-policy",
        "bounded",
        "--evidence-level",
        "runtime_smoke",
        "--bounded",
        "true",
        "--vortex-primitive",
        "filter_project",
        "--vortex-predicate",
        "gte:value:3",
        "--vortex-columns",
        "metric",
        "--vortex-source-order-limit",
        "2",
        "--memory-gb",
        "1",
        "--max-parallelism",
        "2",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("public_workflow_route_attached", "true")));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "native_vortex_filter_project"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "vortex-filter-project"
    )));
    assert!(stdout.contains(&field("public_workflow_vortex_primitive", "filter_project")));
    assert!(stdout.contains(&field("public_workflow_vortex_predicate", "gte:value:3")));
    assert!(stdout.contains(&field("public_workflow_vortex_columns", "metric")));
    assert!(stdout.contains(&field("public_workflow_vortex_source_order_limit", "2")));
    assert!(stdout.contains(&field("mode", "vortex_filter_project")));
    assert!(stdout.contains(&field("primitive", "filter_and_project")));
    assert!(stdout.contains(&field(
        "filter_project_local_execution_projected_columns",
        "metric"
    )));
    assert!(stdout.contains(&field(
        "filter_project_local_execution_source_order_limit_requested",
        "2"
    )));
    assert!(stdout.contains(&field(
        "filter_project_local_execution_source_order_limit_applied",
        "true"
    )));
    assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
    assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
    assert!(stdout.contains(&field(
        "filter_project_local_execution_fallback_attempted",
        "false"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_execution_certificate_fallback_attempted",
        "false"
    )));
    #[cfg(unix)]
    {
        let envelope: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        let summary = envelope["human_text"].as_str().unwrap();
        let values: serde_json::Value =
            serde_json::from_str(summary.split_once(" values=").unwrap().1.trim()).unwrap();
        assert_eq!(
            values,
            serde_json::json!({"rows": 2, "values": [{"metric": 30}, {"metric": 40}]})
        );
        assert!(stdout.contains(&field(
            "local_primitive_native_io_certificate_status",
            "certified"
        )));
    }
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[test]
fn json_collect_rejects_zero_decode_instead_of_returning_a_descriptor() {
    let fixture = local_primitive_struct_fixture();
    let (success, stdout) = run_facade(&[
        "run",
        "cli",
        "--input",
        fixture.as_str(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--materialization-policy",
        "zero_decode",
        "--bounded",
        "true",
        "--vortex-primitive",
        "project",
        "--vortex-columns",
        "metric",
        "--format",
        "json",
    ]);
    assert!(!success);
    assert!(stdout.contains("JSON collect requires scalar decoding"));
    assert!(!stdout.contains("result summary:"));
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[test]
fn zero_decode_aggregate_rejects_before_source_open_but_metadata_count_remains_admitted() {
    let missing = std::env::temp_dir().join(format!(
        "shardloom-zero-decode-missing-{}.vortex",
        std::process::id()
    ));
    assert!(!missing.exists());
    let (success, stdout) = run_facade(&[
        "run",
        "sql",
        "--input",
        missing.to_str().unwrap(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--sql",
        "SELECT SUM(renamed_measure) FROM hits",
        "--bounded",
        "true",
        "--materialization-policy",
        "zero_decode",
        "--format",
        "json",
    ]);
    assert!(!success);
    assert!(
        stdout.contains("aggregate compute requires admitted native array decoding"),
        "{stdout}"
    );
    assert!(!stdout.contains("failed to open"), "{stdout}");
    assert!(!missing.exists());
    let fixture = local_primitive_struct_fixture();
    let (success, stdout) = run_facade(&[
        "run",
        "cli",
        "--input",
        &fixture,
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--vortex-primitive",
        "count",
        "--materialization-policy",
        "zero_decode",
        "--bounded",
        "true",
        "--format",
        "json",
    ]);
    assert!(success, "{stdout}");
    assert!(
        stdout.contains(&field("fallback_attempted", "false")),
        "{stdout}"
    );
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_executes_native_vortex_tail_payload_with_attached_route_envelope() {
    let fixture = local_primitive_struct_fixture();
    let stdout = run_route(&[
        "run",
        "cli",
        "--input",
        fixture.as_str(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--execution-policy",
        "native_vortex",
        "--materialization-policy",
        "bounded",
        "--evidence-level",
        "runtime_smoke",
        "--bounded",
        "true",
        "--vortex-primitive",
        "tail",
        "--vortex-columns",
        "metric",
        "--vortex-source-order-limit",
        "2",
        "--memory-gb",
        "3",
        "--max-parallelism",
        "1",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("public_workflow_route_attached", "true")));
    assert!(stdout.contains(&field("public_workflow_route_id", "native_vortex_tail")));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "vortex-run"
    )));
    assert!(stdout.contains(&field("public_workflow_vortex_primitive", "tail")));
    assert!(stdout.contains(&field("public_workflow_vortex_columns", "metric")));
    assert!(stdout.contains(&field("public_workflow_vortex_source_order_limit", "2")));
    assert!(stdout.contains(&field("mode", "native_vortex_primitive")));
    assert!(stdout.contains(&field("primitive", "tail")));
    assert!(stdout.contains(&field("execution", "local_vortex_tail_primitive_performed")));
    assert!(stdout.contains(&field("local_primitive_source_order_limit_requested", "2")));
    assert!(stdout.contains(&field("local_primitive_source_order_limit_applied", "true")));
    assert!(stdout.contains(&field("public_workflow_memory_gb", "3")));
    assert!(stdout.contains(&field("local_primitive_resource_memory_gb", "3")));
    assert!(stdout.contains(&field("data_decoded", "true")));
    assert!(stdout.contains(&field("data_materialized", "true")));
    assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
    assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_executes_native_vortex_sample_payload_with_attached_route_envelope() {
    let fixture = local_primitive_struct_fixture();
    let stdout = run_route(&[
        "run",
        "cli",
        "--input",
        fixture.as_str(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--execution-policy",
        "native_vortex",
        "--materialization-policy",
        "bounded",
        "--evidence-level",
        "runtime_smoke",
        "--bounded",
        "true",
        "--vortex-primitive",
        "sample",
        "--vortex-predicate",
        "gte:value:3",
        "--vortex-columns",
        "metric",
        "--vortex-source-order-limit",
        "2",
        "--vortex-sample-seed",
        "7",
        "--memory-gb",
        "1",
        "--max-parallelism",
        "1",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("public_workflow_route_attached", "true")));
    assert!(stdout.contains(&field("public_workflow_route_id", "native_vortex_sample")));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "vortex-run"
    )));
    assert!(stdout.contains(&field("public_workflow_vortex_primitive", "sample")));
    assert!(stdout.contains(&field("public_workflow_vortex_predicate", "gte:value:3")));
    assert!(stdout.contains(&field("public_workflow_vortex_columns", "metric")));
    assert!(stdout.contains(&field("public_workflow_vortex_source_order_limit", "2")));
    assert!(stdout.contains(&field("public_workflow_vortex_sample_seed", "7")));
    assert!(stdout.contains(&field("public_workflow_vortex_sample_fraction", "none")));
    assert!(stdout.contains(&field("mode", "native_vortex_primitive")));
    assert!(stdout.contains(&field("primitive", "sample")));
    assert!(stdout.contains(&field(
        "execution",
        "local_vortex_sample_primitive_performed"
    )));
    assert!(stdout.contains(&field("local_primitive_source_order_limit_requested", "2")));
    assert!(stdout.contains(&field("local_primitive_source_order_limit_applied", "true")));
    assert!(stdout.contains(&field(
        "local_primitive_native_io_certificate_status",
        "certified"
    )));
    assert!(stdout.contains(&field("local_primitive_native_io_certified", "true")));
    assert!(stdout.contains(&field("data_decoded", "true")));
    assert!(stdout.contains(&field("data_materialized", "true")));
    assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
    assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_executes_native_vortex_sample_fraction_payload() {
    let fixture = local_primitive_struct_fixture();
    let stdout = run_route(&[
        "run",
        "cli",
        "--input",
        fixture.as_str(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--execution-policy",
        "native_vortex",
        "--materialization-policy",
        "bounded",
        "--evidence-level",
        "runtime_smoke",
        "--bounded",
        "true",
        "--vortex-primitive",
        "sample",
        "--vortex-predicate",
        "gte:value:3",
        "--vortex-columns",
        "metric",
        "--vortex-sample-fraction",
        "0.5",
        "--vortex-sample-seed",
        "7",
        "--memory-gb",
        "1",
        "--max-parallelism",
        "1",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("public_workflow_route_id", "native_vortex_sample")));
    assert!(stdout.contains(&field("public_workflow_vortex_primitive", "sample")));
    assert!(stdout.contains(&field("public_workflow_vortex_source_order_limit", "none")));
    assert!(stdout.contains(&field("public_workflow_vortex_sample_fraction", "0.5")));
    assert!(stdout.contains(&field("public_workflow_vortex_sample_seed", "7")));
    assert!(stdout.contains(&field("mode", "native_vortex_primitive")));
    assert!(stdout.contains(&field("primitive", "sample")));
    assert!(stdout.contains(&field(
        "execution",
        "local_vortex_sample_primitive_performed"
    )));
    assert!(stdout.contains(&field("output_row_count", "2")));
    assert!(stdout.contains(&field(
        "local_primitive_native_io_certificate_status",
        "certified"
    )));
    assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
    assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
}

#[test]
fn public_run_executes_source_free_values_with_attached_route_envelope() {
    let workspace = std::path::Path::new("target/public-workflow-generated-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let output = workspace.join("values.jsonl");
    let _ = std::fs::remove_file(&output);
    let stdout = run_route(&[
        "run",
        "sql",
        "--sql",
        "VALUES (1, 'alpha')",
        "--request",
        "write_jsonl",
        "--output",
        output.to_str().expect("utf8 output path"),
        "--allow-overwrite",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "source_free_generated_output"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "generated-source-sql"
    )));
    assert!(stdout.contains(&field("public_workflow_requested_output", "write_jsonl")));
    assert!(stdout.contains(&field("generated_source_kind", "sql_values")));
    assert!(stdout.contains(&field("generated_source_row_count", "1")));
    assert!(stdout.contains(&field("output_format", "jsonl")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_executes_source_free_range_sql_with_attached_route_envelope() {
    let workspace = std::path::Path::new("target/public-workflow-generated-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let output = workspace.join("range-sql.jsonl");
    let _ = std::fs::remove_file(&output);
    let stdout = run_route(&[
        "run",
        "sql",
        "--sql",
        "SELECT value AS id FROM range(1, 5, 1) WHERE value >= 2 LIMIT 2",
        "--request",
        "write_jsonl",
        "--output",
        output.to_str().expect("utf8 output path"),
        "--allow-overwrite",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "source_free_generated_output"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "generated-source-sql"
    )));
    assert!(stdout.contains(&field("generated_source_kind", "sql_generate_series_range")));
    assert!(stdout.contains(&field("generated_source_row_count", "2")));
    assert!(stdout.contains(&field("generated_source_sql_generator_function", "range")));
    assert!(stdout.contains(&field("sql_source_free_filter_runtime_execution", "true")));
    assert!(stdout.contains(&field("sql_source_free_limit_runtime_execution", "true")));
    assert!(stdout.contains(&field("output_format", "jsonl")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_prepare_attaches_route_envelope_to_ingest_path_or_gate() {
    let workspace = std::path::Path::new("target/public-workflow-prepare-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let input = workspace.join("fact.csv");
    let output = workspace.join("fact.vortex");
    let _ = std::fs::remove_file(&output);
    std::fs::write(&input, "id,label\n1,alpha\n2,beta\n").expect("write csv");
    let (_success, stdout) = run_facade(&[
        "prepare",
        "dataframe",
        "--input",
        input.to_str().expect("utf8 input path"),
        "--input-format",
        "csv",
        "--output",
        output.to_str().expect("utf8 output path"),
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"prepare\""));
    if cfg!(feature = "vortex-write") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field(
            "public_workflow_facade_schema_version",
            "shardloom.public_workflow_execution_facade.v1"
        )));
        assert!(stdout.contains(&field("public_workflow_route_attached", "true")));
        assert!(stdout.contains(&field(
            "public_workflow_route_id",
            "local_file_prepare_once"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_resolved_internal_command",
            "vortex-prepare"
        )));
        assert!(stdout.contains(&field("public_workflow_preparation_included", "true")));
        assert!(stdout.contains(&field(
            "public_workflow_preparation_vortex_ingest_performed",
            "true"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_preparation_local_workflow_input_row_cap",
            "none_synthetic_row_cap_disabled"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_preparation_local_workflow_synthetic_input_row_cap_enabled",
            "false"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field(
            "public_workflow_facade_schema_version",
            "shardloom.public_workflow_execution_facade.v1"
        )));
        assert!(stdout.contains(&field("public_workflow_route_attached", "true")));
        assert!(stdout.contains(&field("public_workflow_route_id", "blocked")));
        assert!(stdout.contains(&field(
            "public_workflow_blocker_id",
            "cg21.route.local_file_vortex_ingest_feature_gated"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_resolved_internal_command",
            "not_resolved"
        )));
        assert!(stdout.contains(&field("public_workflow_preparation_included", "false")));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}
