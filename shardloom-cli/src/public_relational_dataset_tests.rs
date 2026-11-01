//! Collection, glob, manifest and schema-union public SQL regressions.
use super::*;
use shardloom_core::ScalarValue;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-public-dataset-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn extension(format: &str) -> &str {
        match format {
            "arrow-ipc" => "arrow",
            "jsonl" => "jsonl",
            other => other,
        }
    }

    fn write_rows(
        path: &Path,
        format: &str,
        columns: &[String],
        rows: &[Vec<(String, ScalarValue)>],
    ) {
        let bytes = match format {
            "parquet" => shardloom_vortex::encode_flat_parquet_rows(columns, rows).unwrap(),
            "arrow-ipc" => shardloom_vortex::encode_flat_arrow_ipc_rows(columns, rows).unwrap(),
            "avro" => shardloom_vortex::encode_flat_avro_rows(columns, rows).unwrap(),
            "orc" => shardloom_vortex::encode_flat_orc_rows(columns, rows).unwrap(),
            "csv" => {
                let mut output = columns.join(",");
                output.push('\n');
                for row in rows {
                    let values = row
                        .iter()
                        .map(|(_, value)| match value {
                            ScalarValue::Null => String::new(),
                            ScalarValue::Int64(value) => value.to_string(),
                            ScalarValue::Utf8(value) => value.clone(),
                            other => panic!("unexpected CSV fixture value {other:?}"),
                        })
                        .collect::<Vec<_>>();
                    output.push_str(&values.join(","));
                    output.push('\n');
                }
                output.into_bytes()
            }
            "json" => {
                let values = rows
                    .iter()
                    .map(|row| {
                        let fields = row
                            .iter()
                            .map(|(name, value)| {
                                (
                                    name.clone(),
                                    match value {
                                        ScalarValue::Null => serde_json::Value::Null,
                                        ScalarValue::Int64(value) => serde_json::json!(value),
                                        ScalarValue::Utf8(value) => serde_json::json!(value),
                                        other => panic!("unexpected JSON fixture value {other:?}"),
                                    },
                                )
                            })
                            .collect::<serde_json::Map<_, _>>();
                        serde_json::Value::Object(fields)
                    })
                    .collect::<Vec<_>>();
                serde_json::to_vec(&values).unwrap()
            }
            "jsonl" => rows
                .iter()
                .map(|row| {
                    let fields = row
                        .iter()
                        .map(|(name, value)| {
                            (
                                name.clone(),
                                match value {
                                    ScalarValue::Null => serde_json::Value::Null,
                                    ScalarValue::Int64(value) => serde_json::json!(value),
                                    ScalarValue::Utf8(value) => serde_json::json!(value),
                                    other => panic!("unexpected JSONL fixture value {other:?}"),
                                },
                            )
                        })
                        .collect::<serde_json::Map<_, _>>();
                    serde_json::to_string(&fields).unwrap() + "\n"
                })
                .collect::<String>()
                .into_bytes(),
            _ => unreachable!("test format"),
        };
        fs::write(path, bytes).unwrap();
    }

    fn row(
        values: impl IntoIterator<Item = (&'static str, ScalarValue)>,
    ) -> Vec<(String, ScalarValue)> {
        values
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect()
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

    fn manifest(
        path: &Path,
        format: &str,
        files: &[&str],
        schema: Option<&str>,
        partitioning: Option<&str>,
    ) {
        let mut value = serde_json::json!({"version": 1, "format": format, "files": files});
        if let Some(schema) = schema {
            value["schema"] = schema.into();
        }
        if let Some(partitioning) = partitioning {
            value["partitioning"] = partitioning.into();
        }
        fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

const FORMATS: [&str; 7] = [
    "parquet",
    "arrow-ipc",
    "avro",
    "orc",
    "csv",
    "json",
    "jsonl",
];

#[test]
fn public_collection_directory_reads_sorted_files_and_rejects_membership_changes() {
    let fixture = Fixture::new();
    for format in FORMATS {
        let root = fixture.0.join(format!("directory-{format}"));
        fs::create_dir_all(root.join("nested")).unwrap();
        let columns = vec!["id".to_string()];
        Fixture::write_rows(
            &root.join(format!("a.{}", Fixture::extension(format))),
            format,
            &columns,
            &[Fixture::row([("id", ScalarValue::Int64(1))])],
        );
        Fixture::write_rows(
            &root
                .join("nested")
                .join(format!("b.{}", Fixture::extension(format))),
            format,
            &columns,
            &[Fixture::row([("id", ScalarValue::Int64(2))])],
        );
        let request = Fixture::request(
            &root,
            format,
            format!("SELECT id FROM '{}' ORDER BY id", root.display()),
        );
        let operation = Fixture::prepare(&request).unwrap_or_else(|e| panic!("{format}: {e}"));
        let result = operation
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            result.result_jsonl.value().lines().collect::<Vec<_>>(),
            [r#"{"id":1}"#, r#"{"id":2}"#],
            "{format}"
        );
        assert_eq!(
            result.execution.input.as_ref().unwrap().columnar_files,
            2,
            "{format}"
        );
        let extra = root.join(format!("c.{}", Fixture::extension(format)));
        Fixture::write_rows(
            &extra,
            format,
            &columns,
            &[Fixture::row([("id", ScalarValue::Int64(3))])],
        );
        for attempt in 0..2 {
            let error = operation
                .collect_jsonl(&CancellationToken::default())
                .err()
                .expect("collection membership change must invalidate execution")
                .to_string();
            assert!(
                error.contains(if attempt == 0 {
                    "changed"
                } else {
                    "invalidated"
                }),
                "{format}: {error}"
            );
        }
        assert!(!fixture.0.join(".shardloom").exists(), "{format}");
    }
}

#[test]
fn public_csv_globs_distinguish_top_level_and_recursive_patterns() {
    let fixture = Fixture::new();
    let nested = fixture.0.join("nested");
    fs::create_dir(&nested).unwrap();
    fs::write(fixture.0.join("a.csv"), "id\n1\n").unwrap();
    fs::write(nested.join("b.csv"), "id\n2\n").unwrap();
    for (pattern, expected) in [
        (fixture.0.join("*.csv"), vec![r#"{"id":1}"#]),
        (
            fixture.0.join("**/*.csv"),
            vec![r#"{"id":1}"#, r#"{"id":2}"#],
        ),
    ] {
        let request = Fixture::request(
            &pattern,
            "csv",
            format!("SELECT id FROM '{}' ORDER BY id", pattern.display()),
        );
        let result = Fixture::prepare(&request)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            result.result_jsonl.value().lines().collect::<Vec<_>>(),
            expected
        );
    }
}

#[test]
fn public_dataset_manifests_union_by_name_and_reject_implicit_or_incompatible_unions() {
    let fixture = Fixture::new();
    for format in FORMATS {
        let root = fixture.0.join(format!("manifest-{format}"));
        fs::create_dir(&root).unwrap();
        let ext = Fixture::extension(format);
        let columns_a = vec!["id".into(), "left".into()];
        let rows_a = vec![Fixture::row([
            ("id", ScalarValue::Int64(1)),
            ("left", ScalarValue::Int64(10)),
        ])];
        let columns_b = vec!["id".into(), "right".into()];
        let rows_b = vec![Fixture::row([
            ("id", ScalarValue::Int64(2)),
            ("right", ScalarValue::Int64(20)),
        ])];
        Fixture::write_rows(&root.join(format!("a.{ext}")), format, &columns_a, &rows_a);
        Fixture::write_rows(&root.join(format!("b.{ext}")), format, &columns_b, &rows_b);
        let manifest = root.join("dataset.manifest");
        let names = [format!("a.{ext}"), format!("b.{ext}")];
        Fixture::manifest(
            &manifest,
            format,
            &[&names[0], &names[1]],
            Some("union_by_name"),
            None,
        );
        let statement = format!(
            "SELECT id,left,right FROM '{}' ORDER BY id",
            manifest.display()
        );
        let request = Fixture::request(&manifest, format, statement.clone());
        let operation =
            Fixture::prepare(&request).unwrap_or_else(|e| panic!("{format} union: {e}"));
        let result = operation
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            result.result_jsonl.value().lines().collect::<Vec<_>>(),
            [
                r#"{"id":1,"left":10,"right":null}"#,
                r#"{"id":2,"left":null,"right":20}"#,
            ],
            "{format}"
        );

        Fixture::manifest(&manifest, format, &[&names[0], &names[1]], None, None);
        let error = Fixture::prepare(&Fixture::request(&manifest, format, statement.clone()))
            .err()
            .expect("schema drift without explicit union must fail")
            .to_string();
        assert!(
            error.contains("incompatible") || error.contains("union_by_name"),
            "{format}: {error}"
        );

        let bad_rows = vec![Fixture::row([
            ("id", ScalarValue::Utf8("not-an-integer".into())),
            ("right", ScalarValue::Int64(30)),
        ])];
        Fixture::write_rows(
            &root.join(format!("b.{ext}")),
            format,
            &columns_b,
            &bad_rows,
        );
        Fixture::manifest(
            &manifest,
            format,
            &[&names[0], &names[1]],
            Some("union_by_name"),
            None,
        );
        let error = Fixture::prepare(&Fixture::request(&manifest, format, statement))
            .err()
            .expect("union must reject incompatible id types")
            .to_string();
        assert!(
            error.contains("type") || error.contains("schema"),
            "{format}: {error}"
        );
    }
}

#[test]
fn public_csv_hive_manifests_add_nullable_partition_columns_and_detect_collisions() {
    let fixture = Fixture::new();
    let root = fixture.0.join("hive");
    let west = root.join("region=west");
    let missing = root.join("region=__HIVE_DEFAULT_PARTITION__");
    fs::create_dir_all(&west).unwrap();
    fs::create_dir_all(&missing).unwrap();
    fs::write(west.join("a.csv"), "id\n1\n").unwrap();
    fs::write(missing.join("b.csv"), "id\n2\n").unwrap();
    let manifest = root.join("hive.manifest");
    let entries = [
        "region=west/a.csv",
        "region=__HIVE_DEFAULT_PARTITION__/b.csv",
    ];
    Fixture::manifest(&manifest, "csv", &entries, None, Some("hive"));
    let request = Fixture::request(
        &manifest,
        "csv",
        format!("SELECT id,region FROM '{}' ORDER BY id", manifest.display()),
    );
    let result = Fixture::prepare(&request)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        result.result_jsonl.value().lines().collect::<Vec<_>>(),
        [r#"{"id":1,"region":"west"}"#, r#"{"id":2,"region":null}"#,]
    );

    fs::write(west.join("collision.csv"), "id,region\n3,stored\n").unwrap();
    let collision_entries = [
        "region=west/collision.csv",
        "region=__HIVE_DEFAULT_PARTITION__/b.csv",
    ];
    Fixture::manifest(&manifest, "csv", &collision_entries, None, Some("hive"));
    let error = Fixture::prepare(&Fixture::request(
        &manifest,
        "csv",
        format!("SELECT id,region FROM '{}'", manifest.display()),
    ))
    .err()
    .expect("Hive key collision must fail")
    .to_string();
    assert!(error.contains("collides"), "{error}");
}

#[test]
fn public_manifest_generation_replacement_invalidates_repeated_execution() {
    let fixture = Fixture::new();
    let root = fixture.0.join("manifest-replace");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("a.csv"), "id\n1\n").unwrap();
    fs::write(root.join("b.csv"), "id\n2\n").unwrap();
    let manifest = root.join("source.manifest");
    Fixture::manifest(&manifest, "csv", &["a.csv", "b.csv"], None, None);
    let request = Fixture::request(
        &manifest,
        "csv",
        format!("SELECT id FROM '{}' ORDER BY id", manifest.display()),
    );
    let operation = Fixture::prepare(&request).unwrap();
    let replacement = root.join("replacement.manifest");
    Fixture::manifest(&replacement, "csv", &["b.csv", "a.csv"], None, None);
    fs::rename(replacement, &manifest).unwrap();
    for attempt in 0..2 {
        let error = operation
            .collect_jsonl(&CancellationToken::default())
            .err()
            .expect("manifest generation change must fail")
            .to_string();
        assert!(
            error.contains(if attempt == 0 {
                "changed"
            } else {
                "invalidated"
            }),
            "{error}"
        );
    }
}

#[test]
fn public_dataset_admission_rejects_bad_manifests_empty_globs_and_delta_tables() {
    let fixture = Fixture::new();
    let root = fixture.0.join("invalid");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("a.csv"), "id\n1\n").unwrap();

    let duplicate = root.join("duplicate.manifest");
    Fixture::manifest(&duplicate, "csv", &["a.csv", "./a.csv"], None, None);
    let unknown = root.join("unknown.manifest");
    fs::write(
        &unknown,
        br#"{"version":1,"format":"csv","files":["a.csv"],"extra":true}"#,
    )
    .unwrap();
    let absent_glob = root.join("absent").join("*.csv");
    let empty_glob = root.join("empty-*.csv");
    for (path, statement, expected) in [
        (
            &duplicate,
            format!("SELECT id FROM '{}'", duplicate.display()),
            Some("duplicate"),
        ),
        (
            &unknown,
            format!("SELECT id FROM '{}'", unknown.display()),
            Some("unknown field"),
        ),
        (
            &empty_glob,
            format!("SELECT id FROM '{}'", empty_glob.display()),
            Some("no admitted files"),
        ),
    ] {
        let error = Fixture::prepare(&Fixture::request(path, "csv", statement))
            .err()
            .expect("invalid dataset must fail admission")
            .to_string();
        assert!(
            expected.is_some_and(|expected| error.contains(expected)),
            "expected {expected:?}, got {error}"
        );
        assert!(!root.join(".shardloom").exists());
    }
    let error = Fixture::prepare(&Fixture::request(
        &absent_glob,
        "csv",
        format!("SELECT id FROM '{}'", absent_glob.display()),
    ))
    .err()
    .expect("glob rooted in a missing directory must fail")
    .to_string();
    assert_ne!(error, "");
    assert!(!root.join(".shardloom").exists());

    let table = fixture.0.join("table");
    fs::create_dir_all(table.join("_delta_log")).unwrap();
    fs::write(table.join("part.csv"), "id\n1\n").unwrap();
    let error = Fixture::prepare(&Fixture::request(
        &table,
        "csv",
        format!("SELECT id FROM '{}'", table.display()),
    ))
    .err()
    .expect("raw Delta table directory must fail")
    .to_string();
    assert!(error.contains("table metadata"), "{error}");
    assert!(!fixture.0.join(".shardloom").exists());
}
