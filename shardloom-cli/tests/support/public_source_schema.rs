use super::*;

#[test]
fn declared_json_numeric_schema_rejects_incompatible_values_before_publication() {
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = unique_vortex_binding_dir("json-schema-rejection");
    std::fs::create_dir(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    for (case, value, dtype) in [
        ("negative", "-1", "uint64"),
        ("fraction", "1.25", "int64"),
        ("boolean", "true", "int64"),
    ] {
        let source = root.join(format!("{case}.json"));
        let output = root.join(format!("{case}.vortex"));
        std::fs::write(&source, format!(r#"[{{"value":{value}}}]"#)).unwrap();
        let statement = format!("SELECT value FROM '{}'", source.display());
        let schema = format!("value:{dtype}");
        let (success, report) = run_facade(&[
            "run",
            "sql",
            "--sql",
            &statement,
            "--source-schema",
            &schema,
            "--request",
            "write_vortex",
            "--output",
            output.to_str().unwrap(),
            "--memory-gb",
            "1",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ]);
        assert!(!success, "{case}: {report}");
        assert!(
            report.contains("schema-declared JSON input"),
            "{case}: {report}"
        );
        assert!(
            report.contains(&format!("declared {dtype}")),
            "{case}: {report}"
        );
        assert!(!output.exists(), "{case}");
    }
}

#[test]
#[allow(clippy::too_many_lines)] // Verify count/aggregate producers and complete typed rows across every sink.
fn compatibility_sql_reuses_native_producers_and_preserves_path_values_through_every_sink() {
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = unique_vortex_binding_dir("format-strategy");
    std::fs::create_dir(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let source = root.join("input.csv");
    let source = source.to_str().unwrap();
    std::fs::write(
        source,
        format!("label,amount\n{source},10\nelsewhere,20\n{source},30\n"),
    )
    .unwrap();
    for (case, sql, expected, route, schema) in [
        (
            "aggregate",
            format!(
                "SELECT COUNT(*) AS n,SUM(amount) AS total FROM '{source}' WHERE label = '{source}'"
            ),
            vec![serde_json::json!({"n":2,"total":40.0})],
            "native_vortex_aggregate",
            "n:uint64,total:float64",
        ),
        (
            "count",
            format!("SELECT COUNT(*) FROM '{source}'"),
            vec![serde_json::json!({"count_all":3})],
            "native_vortex_count_all",
            "count_all:uint64",
        ),
        (
            "count_where",
            format!("SELECT COUNT(*) FROM '{source}' WHERE label = '{source}'"),
            vec![serde_json::json!({"count_all":2})],
            "native_vortex_count_where",
            "count_all:uint64",
        ),
    ] {
        let common = [
            "run",
            "sql",
            "--input",
            source,
            "--input-format",
            "csv",
            "--source-schema",
            "label:utf8,amount:int64",
            "--sql",
            &sql,
            "--bounded",
            "true",
            "--memory-gb",
            "1",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ];
        let mut collect = common.to_vec();
        collect.extend(["--request", "collect"]);
        let report: serde_json::Value = serde_json::from_str(&run_route(&collect)).unwrap();
        assert_eq!(complete_result::rows(&report), expected);
        assert_eq!(
            complete_result::field_value(&report, "public_workflow_route_id"),
            route,
        );
        assert_eq!(
            complete_result::field_value(&report, "public_workflow_local_source_format"),
            "csv",
        );
        for output in [
            "vortex",
            "parquet",
            "arrow_ipc",
            "avro",
            "orc",
            "csv",
            "json",
            "jsonl",
        ] {
            let target = root.join(format!("{case}.{output}"));
            let target = target.to_str().unwrap();
            let write = format!("write_{output}");
            let mut arguments = common.to_vec();
            arguments.extend(["--request", &write, "--output", target]);
            let report: serde_json::Value = serde_json::from_str(&run_route(&arguments)).unwrap();
            assert_eq!(
                complete_result::field_value(&report, "public_workflow_route_id"),
                "native_vortex_primitive_row_export",
                "{output}: {report}",
            );
            if case == "count" {
                assert_eq!(
                    complete_result::field_value(&report, "upstream_vortex_scan_called"),
                    "false"
                );
                assert_eq!(complete_result::field_value(&report, "data_read"), "false");
            }
            let reopen_sql = format!("SELECT * FROM '{target}'");
            let input_format = output.replace('_', "-");
            let mut reopen = vec![
                "run",
                "sql",
                "--input",
                target,
                "--input-format",
                &input_format,
                "--sql",
                &reopen_sql,
                "--request",
                "collect",
                "--bounded",
                "true",
                "--memory-gb",
                "1",
                "--max-parallelism",
                "2",
                "--format",
                "json",
            ];
            if matches!(output, "csv" | "json" | "jsonl") {
                reopen.extend(["--source-schema", schema]);
            }
            let reopened: serde_json::Value = serde_json::from_str(&run_route(&reopen)).unwrap();
            assert_eq!(complete_result::rows(&reopened), expected, "{output}");
            for report in [&report, &reopened] {
                assert_eq!(
                    complete_result::field_value(report, "public_workflow_fallback_attempted"),
                    "false"
                );
                assert_eq!(
                    complete_result::field_value(report, "public_workflow_external_engine_invoked"),
                    "false"
                );
            }
        }
    }
}

#[test]
#[allow(clippy::too_many_lines)] // Compare complete independent rows through both raw and prepared formats.
fn wildcard_queries_ignore_physical_preparation_metadata_for_public_sources() {
    use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::{fs::File, sync::Arc};

    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    let root = unique_vortex_binding_dir("source-schema");
    std::fs::create_dir(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let expected = serde_json::json!([
        {"label":"north","EventTime":61_000_000,"amount":10},
        {"label":null,"EventTime":null,"amount":20},
        {"label":"","EventTime":121_000_000,"amount":30}
    ]);

    for (format, extension) in [("parquet", "parquet"), ("orc", "orc")] {
        // The ORC provider cannot write timestamps. Both fixtures exercise
        // private UTF8 metadata; Parquet also exercises timestamp metadata.
        let times = vec![Some(61_000_000), None, Some(121_000_000)];
        let time: arrow_array::ArrayRef = if format == "parquet" {
            Arc::new(TimestampMicrosecondArray::from(times))
        } else {
            Arc::new(Int64Array::from(times))
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("label", DataType::Utf8, true),
            Field::new("EventTime", time.data_type().clone(), true),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("north"), None, Some("")])),
                time,
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let input = root.join(format!("source.{extension}"));
        if format == "parquet" {
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                File::create(&input).unwrap(),
                Arc::clone(&schema),
                None,
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        } else {
            let mut writer = orc_rust::ArrowWriterBuilder::new(
                File::create(&input).unwrap(),
                Arc::clone(&schema),
            )
            .try_build()
            .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let prepared = root.join(format!("{format}.vortex"));
        run_route(&[
            "prepare",
            "dataframe",
            "--input",
            input.to_str().unwrap(),
            "--input-format",
            format,
            "--output",
            prepared.to_str().unwrap(),
            "--memory-gb",
            "1",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ]);
        for (source, input_format) in [(&input, format), (&prepared, "vortex")] {
            let source = source.to_str().unwrap();
            for (sql, rows) in [
                (format!("SELECT * FROM '{source}'"), expected.clone()),
                (
                    format!("SELECT label,EventTime,amount FROM '{source}'"),
                    expected.clone(),
                ),
                (
                    format!("SELECT * FROM (SELECT * FROM '{source}') AS q ORDER BY amount"),
                    expected.clone(),
                ),
                (
                    format!("SELECT * FROM (SELECT * FROM '{source}') AS q WHERE amount < 0"),
                    serde_json::json!([]),
                ),
            ] {
                let stdout = run_route(&[
                    "run",
                    "sql",
                    "--input",
                    source,
                    "--input-format",
                    input_format,
                    "--sql",
                    &sql,
                    "--request",
                    "collect",
                    "--bounded",
                    "true",
                    "--memory-gb",
                    "1",
                    "--max-parallelism",
                    "2",
                    "--format",
                    "json",
                ]);
                let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
                assert_eq!(
                    complete_result::rows(&report),
                    rows.as_array().unwrap().clone(),
                    "format={format} source={input_format} sql={sql}"
                );
                for key in [
                    "public_workflow_fallback_attempted",
                    "public_workflow_external_engine_invoked",
                ] {
                    assert_eq!(
                        complete_result::field_value(&report, key),
                        "false",
                        "{stdout}"
                    );
                }
            }
        }
    }
}
