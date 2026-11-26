//! Public SQL source demand across admitted local compatibility formats.
use super::*;
use shardloom_core::ScalarValue;
use std::{
    fs,
    path::Path,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-public-format-demand-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn rows() -> (Vec<String>, Vec<Vec<(String, ScalarValue)>>) {
        let columns = vec!["id".into(), "value".into(), "unused".into()];
        let rows = (0..4)
            .map(|n| {
                vec![
                    ("id".into(), ScalarValue::Int64(n)),
                    ("value".into(), ScalarValue::Int64(100 + n)),
                    ("unused".into(), ScalarValue::Utf8(format!("unused-{n}"))),
                ]
            })
            .collect();
        (columns, rows)
    }

    fn bytes(format: &str) -> Vec<u8> {
        let (columns, rows) = Self::rows();
        match format {
            "arrow-ipc" => shardloom_vortex::encode_flat_arrow_ipc_rows(&columns, &rows).unwrap(),
            "avro" => shardloom_vortex::encode_flat_avro_rows(&columns, &rows).unwrap(),
            "orc" => shardloom_vortex::encode_flat_orc_rows(&columns, &rows).unwrap(),
            "csv" => b"id,value,unused\n0,100,unused-0\n1,101,unused-1\n2,102,unused-2\n3,103,unused-3\n".to_vec(),
            "tsv" => b"id\tvalue\tunused\n0\t100\tunused-0\n1\t101\tunused-1\n2\t102\tunused-2\n3\t103\tunused-3\n".to_vec(),
            "json" => br#"[{"id":0,"value":100,"unused":"unused-0"},{"id":1,"value":101,"unused":"unused-1"},{"id":2,"value":102,"unused":"unused-2"},{"id":3,"value":103,"unused":"unused-3"}]"#.to_vec(),
            "jsonl" | "ndjson" => b"{\"id\":0,\"value\":100,\"unused\":\"unused-0\"}\n{\"id\":1,\"value\":101,\"unused\":\"unused-1\"}\n{\"id\":2,\"value\":102,\"unused\":\"unused-2\"}\n{\"id\":3,\"value\":103,\"unused\":\"unused-3\"}\n".to_vec(),
            _ => unreachable!("fixture format"),
        }
    }

    fn write(path: &Path, format: &str) {
        fs::write(path, Self::bytes(format)).unwrap();
    }

    fn request(path: &Path, format: &str, sql: impl Into<String>) -> PublicWorkflowRouteRequest {
        let mut request = PublicWorkflowRouteRequest::new("sql".into());
        request.resources.memory_bytes = Some((64_u64 << 20).to_string());
        request.resources.max_parallelism = Some("1".into());
        request.input_uri = Some(path.display().to_string());
        request.input_format = Some(format.into());
        request.sql_statement = Some(sql.into());
        request.bounded = true;
        request
    }

    fn prepare(
        request: &PublicWorkflowRouteRequest,
    ) -> Result<PreparedVortexRelational, ShardLoomError> {
        prepare_with_source(
            request.sql_statement.as_ref().unwrap(),
            request,
            None,
            PublicSourcePreparations::default(),
        )
        .map(|(operation, _)| operation)
    }

    fn target(path: &Path, format: &str) -> PathBuf {
        auto_prepared_vortex_target_path_with_schema(path.to_str().unwrap(), format, None)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

const FORMATS: [&str; 8] = [
    "arrow-ipc",
    "avro",
    "orc",
    "csv",
    "tsv",
    "json",
    "jsonl",
    "ndjson",
];

#[test]
fn public_json_duplicate_fields_resolve_before_inferred_and_declared_coercion() {
    let fixture = Fixture::new();
    for format in ["jsonl", "ndjson", "json"] {
        for declared in [false, true] {
            let source = fixture.0.join(format!("duplicate.{format}"));
            let records = [
                r#"{"v":"discarded","a":1,"v":1.5}"#,
                r#"{"a":2,"v":false,"v":2.5}"#,
                r#"{"a":3,"v":[1,2],"v":3.5}"#,
            ];
            let content = if format == "json" {
                format!("[{}]", records.join(","))
            } else {
                records.join("\n")
            };
            fs::write(&source, content).unwrap();
            let mut request = Fixture::request(
                &source,
                format,
                format!("SELECT a,v FROM '{}'", source.display()),
            );
            if declared {
                request.source_schema = Some("a:int64,v:float64".into());
            }
            let prepared = Fixture::prepare(&request).unwrap();
            let initial = prepared.snapshot().memory.reserved_bytes;
            for _ in 0..2 {
                let result = prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap();
                let rows = result
                    .result_jsonl
                    .value()
                    .lines()
                    .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    rows,
                    [
                        serde_json::json!({"a":1,"v":1.5}),
                        serde_json::json!({"a":2,"v":2.5}),
                        serde_json::json!({"a":3,"v":3.5})
                    ]
                );
                drop(result);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
            }
            // Discarded values are still parsed: duplicate resolution does not
            // admit malformed scalar or nested syntax.
            for invalid in [
                r#"{"a":1,"v":broken,"v":1.5}"#,
                r#"{"a":1,"v":[broken],"v":1.5}"#,
            ] {
                fs::write(
                    &source,
                    if format == "json" {
                        format!("[{invalid}]")
                    } else {
                        invalid.into()
                    },
                )
                .unwrap();
                assert!(
                    Fixture::prepare(&request).is_err(),
                    "{format}, declared={declared}, invalid={invalid}"
                );
            }
        }
    }
}

#[test]
fn public_tsv_preserves_quoted_tabs_newlines_bom_and_empty_declared_schema() {
    let fixture = Fixture::new();
    let source = fixture.0.join("source.tsv");
    fs::write(
        &source,
        "\u{feff}\"id\"\t\"label\"\r\n1\t\"東京\t\"\"quote\"\"\nnext🙂\"\r\n2\tplain\r\n",
    )
    .unwrap();
    let mut request = Fixture::request(
        &source,
        "tsv",
        format!("SELECT id,label FROM '{}' ORDER BY id", source.display()),
    );
    request.source_schema = Some("id:int64,label:utf8".into());
    let result = Fixture::prepare(&request)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let rows = result
        .result_jsonl
        .value()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            serde_json::json!({"id":1,"label":"東京\t\"quote\"\nnext🙂"}),
            serde_json::json!({"id":2,"label":"plain"})
        ]
    );
    for malformed in [
        "id\tlabel\n1\tbad\"quote\n",
        "id\tlabel\n1\t\"closed\"suffix\n",
        "id\tlabel\n1\t\"unclosed\n",
    ] {
        fs::write(&source, malformed).unwrap();
        assert!(Fixture::prepare(&request).is_err());
    }
    for (format, content) in [("tsv", "id\tlabel\n"), ("jsonl", "")] {
        let source = fixture.0.join(format!("empty.{format}"));
        fs::write(&source, content).unwrap();
        let mut request = Fixture::request(
            &source,
            format,
            format!("SELECT COUNT(*) AS n FROM '{}'", source.display()),
        );
        request.source_schema = Some("id:int64,label:utf8".into());
        let result = Fixture::prepare(&request)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(result.result_jsonl.value().trim(), r#"{"n":0}"#);
    }
}

#[test]
fn public_delimited_sources_preserve_all_null_rows_and_skip_empty_lines() {
    let fixture = Fixture::new();
    for (format, delimiter) in [("csv", ','), ("tsv", '\t')] {
        for ending in ["\n", "\r\n", ""] {
            let source = fixture.0.join(format!("all-null.{format}"));
            fs::write(
                &source,
                format!("a{delimiter}b\r\n1{delimiter}2\n\n\r\n{delimiter}\r\n\"\"{delimiter}\"\"\n{delimiter}{ending}"),
            )
            .unwrap();
            let request = Fixture::request(
                &source,
                format,
                format!("SELECT a,b FROM '{}'", source.display()),
            );
            let operation = Fixture::prepare(&request).unwrap();
            for _ in 0..2 {
                let result = operation
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap();
                let rows = result
                    .result_jsonl
                    .value()
                    .lines()
                    .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    rows,
                    [
                        serde_json::json!({"a":1,"b":2}),
                        serde_json::json!({"a":null,"b":null}),
                        serde_json::json!({"a":null,"b":null}),
                        serde_json::json!({"a":null,"b":null}),
                    ],
                    "{format}, ending={ending:?}"
                );
            }
            let request = Fixture::request(
                &source,
                format,
                format!(
                    "SELECT COUNT(*) AS rows,COUNT(a) AS present FROM '{}'",
                    source.display()
                ),
            );
            let result = Fixture::prepare(&request)
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(result.result_jsonl.value()).unwrap(),
                serde_json::json!({"rows":4,"present":1})
            );
        }
    }
}

#[test]
fn inferred_text_numeric_promotion_never_rounds_large_integer_values() {
    let fixture = Fixture::new();
    for (format, contents) in [
        ("csv", "id\n-1\n18446744073709551615\n"),
        ("jsonl", "{\"id\":-1}\n{\"id\":18446744073709551615}\n"),
    ] {
        let source = fixture.0.join(format!("numbers.{format}"));
        fs::write(&source, contents).unwrap();
        let request = Fixture::request(
            &source,
            format,
            format!("SELECT id FROM '{}'", source.display()),
        );
        let operation = Fixture::prepare(&request).unwrap();
        let error = operation
            .collect_jsonl(&CancellationToken::default())
            .err()
            .unwrap()
            .to_string();
        assert!(
            error.contains("lose integer precision"),
            "{format}: {error}"
        );
    }
}

#[test]
fn public_compatibility_formats_demand_project_reexecute_and_do_not_persist() {
    let fixture = Fixture::new();
    for format in FORMATS {
        let source = fixture.0.join(format!("source.{format}"));
        Fixture::write(&source, format);
        let request = Fixture::request(
            &source,
            format,
            format!("SELECT value FROM '{}' WHERE id=2", source.display()),
        );
        let operation = Fixture::prepare(&request).unwrap_or_else(|e| panic!("{format}: {e}"));
        assert!(!Fixture::target(&source, format).exists(), "{format}");
        for _ in 0..2 {
            let result = operation
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(
                result.result_jsonl.value().trim(),
                r#"{"value":102}"#,
                "{format}"
            );
            let input = result.execution.input.as_ref().unwrap();
            assert_eq!(input.columnar_sources, 1, "{format}");
            assert_eq!(input.columnar_projected_columns, 2, "{format}");
            assert_eq!(input.columnar_available_columns, 3, "{format}");
            assert_eq!(
                input.columnar_source_formats,
                [if format == "ndjson" { "jsonl" } else { format }.to_string()],
                "{format}"
            );
            assert!(
                !result
                    .execution
                    .native_io_certificate
                    .side_effects
                    .fallback_attempted
            );
            assert!(!Fixture::target(&source, format).exists(), "{format}");
        }
        let mut sum_request = Fixture::request(
            &source,
            format,
            format!("SELECT SUM(value) AS total FROM '{}'", source.display()),
        );
        sum_request.bounded = true;
        let result = Fixture::prepare(&sum_request)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            result.result_jsonl.value().trim(),
            r#"{"total":406.0}"#,
            "{format}"
        );
        assert_eq!(
            result
                .execution
                .input
                .as_ref()
                .unwrap()
                .columnar_projected_columns,
            1,
            "{format}"
        );
        assert!(!fixture.0.join(".shardloom").exists(), "{format}");
    }
}

#[test]
fn public_compatibility_format_demand_invalidates_replaced_source_on_every_execution() {
    let fixture = Fixture::new();
    for format in FORMATS {
        let source = fixture.0.join(format!("replace.{format}"));
        Fixture::write(&source, format);
        let request = Fixture::request(
            &source,
            format,
            format!("SELECT value FROM '{}'", source.display()),
        );
        let operation = Fixture::prepare(&request).unwrap_or_else(|e| panic!("{format}: {e}"));
        let replacement = fixture.0.join(format!("replacement-{format}"));
        fs::write(&replacement, b"changed-generation").unwrap();
        fs::rename(&replacement, &source).unwrap();
        for _ in 0..2 {
            let error = operation
                .collect_jsonl(&CancellationToken::default())
                .err()
                .expect("changed generation must fail")
                .to_string();
            assert!(
                error.contains("changed") || error.contains("invalidated"),
                "{format}: {error}"
            );
        }
    }
}

#[test]
fn public_compatibility_format_directories_count_only_admitted_metadata() {
    let fixture = Fixture::new();
    for format in FORMATS {
        let directory = fixture.0.join(format!("parts-{format}"));
        fs::create_dir(&directory).unwrap();
        for name in ["a", "b"] {
            let extension = if format == "arrow-ipc" {
                "arrow"
            } else {
                format
            };
            Fixture::write(&directory.join(format!("{name}.{extension}")), format);
        }
        let single = directory.join(format!(
            "a.{}",
            if format == "arrow-ipc" {
                "arrow"
            } else {
                format
            }
        ));
        for (path, expected_rows) in [(directory.clone(), 8_u64), (single, 4_u64)] {
            let request = Fixture::request(
                &path,
                format,
                format!("SELECT COUNT(*) AS rows FROM '{}'", path.display()),
            );
            let result = Fixture::prepare(&request)
                .unwrap_or_else(|e| panic!("{format}: {e}"))
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(
                result.result_jsonl.value().trim(),
                format!(r#"{{"rows":{expected_rows}}}"#),
                "{format}"
            );
            let input = result.execution.input.as_ref().unwrap();
            let metadata_count = format == "orc";
            assert_eq!(
                input.metadata_count_sources,
                u64::from(metadata_count),
                "{format}"
            );
            assert_eq!(
                input.columnar_rows_decoded,
                if metadata_count { 0 } else { expected_rows },
                "{format}"
            );
        }
    }
}

#[test]
fn public_binary_compatibility_formats_reject_source_schema_hints() {
    let fixture = Fixture::new();
    for format in ["arrow-ipc", "avro", "orc"] {
        let source = fixture.0.join(format!("schema.{format}"));
        Fixture::write(&source, format);
        let mut request = Fixture::request(
            &source,
            format,
            format!("SELECT value FROM '{}'", source.display()),
        );
        request.source_schema = Some("id:int64,value:int64,unused:utf8".into());
        let error = Fixture::prepare(&request)
            .err()
            .expect("binary schema hint must fail")
            .to_string();
        assert!(
            error.contains("schema") || error.contains("embedded"),
            "{format}: {error}"
        );
    }
}

#[test]
fn public_streaming_json_preserves_exact_uint64_and_schema_bearing_empty_inputs() {
    let fixture = Fixture::new();
    let source = fixture.0.join("exact.json");
    fs::write(
        &source,
        r#"[{"id":18446744073709551615},{"id":9223372036854775808}]"#,
    )
    .unwrap();
    let request = Fixture::request(
        &source,
        "json",
        format!("SELECT id FROM '{}' ORDER BY id", source.display()),
    );
    let result = Fixture::prepare(&request)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        result.result_jsonl.value().trim(),
        "{\"id\":9223372036854775808}\n{\"id\":18446744073709551615}"
    );
    drop(result);
    for (content, expected) in [("[]", 0), ("[{}]", 1)] {
        fs::write(&source, content).unwrap();
        let mut request = Fixture::request(
            &source,
            "json",
            format!("SELECT COUNT(*) AS rows FROM '{}'", source.display()),
        );
        request.source_schema = Some("id:uint64,text:utf8".into());
        let result = Fixture::prepare(&request)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            result.result_jsonl.value().trim(),
            format!("{{\"rows\":{expected}}}")
        );
        assert_eq!(
            result
                .execution
                .input
                .as_ref()
                .unwrap()
                .metadata_count_sources,
            0
        );
    }
    for content in [
        r#"[{"id":18446744073709551616}]"#,
        r#"[{"id":1}] malformed"#,
    ] {
        fs::write(&source, content).unwrap();
        let request = Fixture::request(
            &source,
            "json",
            format!("SELECT id FROM '{}' LIMIT 1", source.display()),
        );
        assert!(Fixture::prepare(&request).is_err());
    }
}

#[test]
#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss
)]
fn public_parquet_parallel_aggregate_and_early_join_filter_evidence() {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::{
        arrow::ArrowWriter,
        file::properties::{EnabledStatistics, WriterProperties},
    };
    use std::collections::BTreeMap;
    use std::sync::Arc;

    let fixture = Fixture::new();
    let fact_path = fixture.0.join("fact.parquet");
    let dim_path = fixture.0.join("dim.parquet");
    let total_rows = 16_384_usize;

    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("grp", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
        Field::new("wide", DataType::Utf8, false),
    ]));
    let fact_batch = RecordBatch::try_new(
        Arc::clone(&fact_schema),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..total_rows).map(|i| (i % 512) as i64),
            )),
            Arc::new(Int64Array::from_iter_values(
                (0..total_rows).map(|i| (i % 4) as i64),
            )),
            Arc::new(Int64Array::from_iter_values(
                (0..total_rows).map(|i| (i % 10) as i64),
            )),
            Arc::new(StringArray::from_iter_values(
                (0..total_rows).map(|i| format!("wide-row-{i:05}-{}", "w".repeat(48))),
            )),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4_096))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();
    let file = fs::File::create(&fact_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, fact_schema, Some(props)).unwrap();
    writer.write(&fact_batch).unwrap();
    writer.close().unwrap();

    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tier", DataType::Utf8, false),
        Field::new("region", DataType::Int64, false),
    ]));
    let dim_batch = RecordBatch::try_new(
        Arc::clone(&dim_schema),
        vec![
            Arc::new(Int64Array::from_iter_values((0..256_usize).map(|i| {
                if i < 80 {
                    (i * 5) as i64
                } else {
                    (i + 1000) as i64
                }
            }))),
            Arc::new(StringArray::from_iter_values(
                (0..256_usize).map(|i| if i < 80 { "keep" } else { "drop" }),
            )),
            Arc::new(Int64Array::from_iter_values(
                (0..256_usize).map(|i| (i % 2) as i64),
            )),
        ],
    )
    .unwrap();
    let dim_file = fs::File::create(&dim_path).unwrap();
    let mut dim_writer = ArrowWriter::try_new(dim_file, dim_schema, None).unwrap();
    dim_writer.write(&dim_batch).unwrap();
    dim_writer.close().unwrap();

    // 1. Track A1+A2 through public SQL command + CLI report fields.
    let agg_sql = format!(
        "SELECT grp, COUNT(*) AS cnt, SUM(val) AS sum_v, MIN(val) AS min_v, MAX(val) AS max_v \
         FROM '{}' GROUP BY grp ORDER BY grp",
        fact_path.display()
    );
    let mut agg_req = Fixture::request(&fact_path, "parquet", &agg_sql);
    agg_req.resources.memory_bytes = Some((128_u64 << 20).to_string());
    agg_req.resources.max_parallelism = Some("4".into());
    let agg_collected = Fixture::prepare(&agg_req)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let mut expected_agg: BTreeMap<i64, (i64, i64, i64, i64)> = BTreeMap::new();
    for i in 0..total_rows {
        let g = (i % 4) as i64;
        let v = (i % 10) as i64;
        let entry = expected_agg.entry(g).or_insert((0, 0, v, v));
        entry.0 += 1;
        entry.1 += v;
        entry.2 = entry.2.min(v);
        entry.3 = entry.3.max(v);
    }
    let actual_agg: Vec<serde_json::Value> = agg_collected
        .result_jsonl
        .value()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(actual_agg.len(), 4);
    for (row, (g, (cnt, sum_v, min_v, max_v))) in actual_agg.iter().zip(&expected_agg) {
        assert_eq!(row["grp"].as_i64(), Some(*g));
        assert_eq!(row["cnt"].as_i64(), Some(*cnt));
        assert_eq!(row["sum_v"].as_f64(), Some(*sum_v as f64));
        assert_eq!(row["min_v"].as_i64(), Some(*min_v));
        assert_eq!(row["max_v"].as_i64(), Some(*max_v));
    }
    let agg_fields: BTreeMap<String, String> =
        crate::native::execution_fields(&agg_collected.execution, false)
            .into_iter()
            .collect();
    assert_eq!(
        agg_fields.get("fallback_attempted").map(String::as_str),
        Some("false")
    );
    assert_eq!(
        agg_fields
            .get("external_engine_invoked")
            .map(String::as_str),
        Some("false")
    );
    let workers: u64 = agg_fields["native_input_parquet_parallel_workers"]
        .parse()
        .unwrap();
    let states: u64 = agg_fields["native_input_parquet_persistent_aggregate_states"]
        .parse()
        .unwrap();
    let merged: u64 = agg_fields["native_input_parquet_aggregate_partial_groups_merged"]
        .parse()
        .unwrap();
    assert!(workers >= 2, "workers={workers}");
    assert!(states >= 2, "states={states}");
    assert!(merged >= 1, "merged={merged}");
    assert_eq!(
        agg_fields
            .get("native_source_demand_rows_decoded")
            .map(String::as_str),
        Some("16384")
    );

    // 2. Also exercise full public CLI `command_json` SQL route for Track A1+A2.
    let cli_json = crate::native::command_json(vec![
        "run".into(),
        "sql".into(),
        "--sql".into(),
        agg_sql,
        "--input".into(),
        fact_path.display().to_string(),
        "--input-format".into(),
        "parquet".into(),
        "--request".into(),
        "collect".into(),
        "--bounded".into(),
        "true".into(),
        "--memory-bytes".into(),
        (128_u64 << 20).to_string(),
        "--max-parallelism".into(),
        "4".into(),
    ])
    .unwrap();
    let cli_env: serde_json::Value = serde_json::from_str(&cli_json).unwrap();
    assert_eq!(cli_env["status"], "success");
    assert_eq!(cli_env["fallback"]["attempted"], false);
    let cli_fields: BTreeMap<String, String> = cli_env["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            (
                entry["key"].as_str().unwrap().to_string(),
                entry["value"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert!(
        cli_fields["native_input_parquet_parallel_workers"]
            .parse::<u64>()
            .unwrap()
            >= 2
    );
    assert!(
        cli_fields["native_input_parquet_persistent_aggregate_states"]
            .parse::<u64>()
            .unwrap()
            >= 2
    );

    // 3. Track B1+B2 through public CLI join_aggregate SQL route.
    let join_sql = format!(
        "SELECT d.region AS region, SUM(f.val) AS sum_v \
         FROM '{}' AS f INNER JOIN '{}' AS d ON f.id = d.id \
         WHERE d.tier = 'keep' GROUP BY d.region ORDER BY d.region",
        fact_path.display(),
        dim_path.display()
    );
    let mut join_req = Fixture::request(&fact_path, "parquet", &join_sql);
    join_req.resources.memory_bytes = Some((128_u64 << 20).to_string());
    join_req.resources.max_parallelism = Some("4".into());
    join_req.source_bindings = BTreeMap::from([
        (
            fact_path.display().to_string(),
            super::super::super::PublicSourceBinding {
                input_format: "parquet".into(),
                source_schema: None,
                memory_input: None,
                table_snapshot: None,
            },
        ),
        (
            dim_path.display().to_string(),
            super::super::super::PublicSourceBinding {
                input_format: "parquet".into(),
                source_schema: None,
                memory_input: None,
                table_snapshot: None,
            },
        ),
    ]);
    let join_collected = Fixture::prepare(&join_req)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let mut expected_join = [0_i64; 2];
    for i in 0..total_rows {
        let id = i % 512;
        if id <= 395 && id.is_multiple_of(5) {
            let reg = (id / 5) % 2;
            expected_join[reg] += (i % 10) as i64;
        }
    }
    let actual_join: Vec<serde_json::Value> = join_collected
        .result_jsonl
        .value()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(actual_join.len(), 2);
    for r in [0_usize, 1] {
        assert_eq!(actual_join[r]["region"].as_i64(), Some(r as i64));
        assert_eq!(
            actual_join[r]["sum_v"].as_f64(),
            Some(expected_join[r] as f64)
        );
    }
    let join_fields: BTreeMap<String, String> =
        crate::native::execution_fields(&join_collected.execution, false)
            .into_iter()
            .collect();
    assert_eq!(
        join_fields
            .get("native_input_parquet_key_filter_rows_decoded")
            .map(String::as_str),
        Some("16384")
    );
    let payload_decoded: u64 = join_fields["native_input_parquet_payload_rows_decoded"]
        .parse()
        .unwrap();
    assert!(
        payload_decoded < total_rows as u64,
        "payload_decoded={payload_decoded}"
    );
    let join_work: serde_json::Value =
        serde_json::from_str(&join_fields["local_primitive_native_io_join_work_json"]).unwrap();
    assert!(
        join_work["direct_aggregate"]["probe_filters_pushed"]
            .as_u64()
            .unwrap()
            >= 1
    );

    // 4. Non-Parquet arrow-ipc control: omit native_input_parquet_* while preserving native_source_demand_*.
    let ipc_path = fixture.0.join("control.arrow-ipc");
    Fixture::write(&ipc_path, "arrow-ipc");
    let mut ipc_req = Fixture::request(
        &ipc_path,
        "arrow-ipc",
        format!(
            "SELECT id, SUM(value) AS sum_v FROM '{}' GROUP BY id ORDER BY id",
            ipc_path.display()
        ),
    );
    ipc_req.resources.max_parallelism = Some("4".into());
    let ipc_collected = Fixture::prepare(&ipc_req)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let ipc_fields: BTreeMap<String, String> =
        crate::native::execution_fields(&ipc_collected.execution, false)
            .into_iter()
            .collect();
    assert!(
        ipc_fields
            .keys()
            .all(|k| !k.starts_with("native_input_parquet_"))
    );
    assert_eq!(
        ipc_fields
            .get("native_source_demand_rows_decoded")
            .map(String::as_str),
        Some("4")
    );
}
