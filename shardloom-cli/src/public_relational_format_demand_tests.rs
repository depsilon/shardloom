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
fn public_compatibility_format_directories_count_only_parquet_as_metadata() {
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
            assert_eq!(input.metadata_count_sources, 0, "{format}");
            assert_eq!(input.columnar_rows_decoded, expected_rows, "{format}");
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
