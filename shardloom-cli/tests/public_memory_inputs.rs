use std::process::Command;

#[test]
fn obsolete_source_evaluators_are_not_commands() {
    for command in [
        "generated-source-user-rows",
        "generated-source-range",
        "generated-source-sequence",
        "generated-source-sql",
        "generated-source-user-rows-smoke",
        "generated-source-range-smoke",
        "generated-source-sequence-smoke",
        "generated-source-sql-smoke",
        "traditional-analytics-run",
        "traditional-analytics-vortex-run",
        "traditional-analytics-vortex-batch-run",
        "traditional-analytics-prepare-batch-run",
        "vortex-production-runtime-run",
        "local-source-runtime",
        "operator-microkernel-benchmark",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .arg(command)
            .output()
            .unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8(output.stderr).unwrap();
        assert!(
            error.contains("unknown argument/value:"),
            "{command}: {error}"
        );
    }
}

#[cfg(all(unix, feature = "vortex-local-primitives"))]
mod native {
    use super::*;
    use serde_json::{Value, json};

    fn run(surface: &str, sql: &str, output: &str, args: &[String]) -> (bool, Value) {
        let result = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args([
                "run",
                surface,
                "--sql",
                sql,
                "--request",
                output,
                "--bounded",
                "true",
                "--memory-gb",
                "1",
                "--max-parallelism",
                "1",
                "--format",
                "json",
            ])
            .args(args)
            .output()
            .unwrap();
        let envelope: Value = serde_json::from_slice(&result.stdout).unwrap_or_else(|_| {
            panic!(
                "stdout={} stderr={}",
                String::from_utf8_lossy(&result.stdout),
                String::from_utf8_lossy(&result.stderr)
            )
        });
        let mut keys = std::collections::BTreeSet::new();
        for field in envelope["fields"].as_array().expect("envelope fields") {
            let key = field["key"].as_str().expect("field key");
            assert!(keys.insert(key), "duplicate report field {key}: {envelope}");
        }
        (result.status.success(), envelope)
    }

    fn field<'a>(envelope: &'a Value, key: &str) -> Option<&'a str> {
        envelope["fields"]
            .as_array()?
            .iter()
            .find(|entry| entry["key"].as_str() == Some(key))?["value"]
            .as_str()
    }

    fn rows(envelope: &Value) -> Value {
        assert_eq!(envelope["status"], "success", "{envelope}");
        assert_eq!(field(envelope, "fallback_attempted"), Some("false"));
        assert_eq!(field(envelope, "external_engine_invoked"), Some("false"));
        let jsonl = field(envelope, "result_jsonl")
            .or_else(|| field(envelope, "local_primitive_result_jsonl"))
            .unwrap_or_else(|| panic!("complete rows missing: {envelope}"));
        Value::Array(
            jsonl
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect(),
        )
    }

    #[test]
    fn source_free_inputs_use_the_same_public_engine_on_every_surface() {
        let cases = [
            (
                "SELECT 9223372036854775807 AS n, 'λ,;''%' AS label, NULL AS absent",
                json!([{"n":i64::MAX,"label":"λ,;'%","absent":null}]),
            ),
            ("SELECT 1 + 2 AS n WHERE FALSE", json!([])),
            (
                "SELECT column_1 * 2 AS n FROM (VALUES (1), (3), (2)) AS v WHERE column_1 > 1 ORDER BY n DESC",
                json!([{"n":6},{"n":4}]),
            ),
            (
                "SELECT value * 2 AS n FROM range(1, 5) WHERE value > 1 ORDER BY n DESC LIMIT 2",
                json!([{"n":8},{"n":6}]),
            ),
            (
                "SELECT value AS n FROM generate_series(5, 1, -2) ORDER BY n",
                json!([{"n":1},{"n":3},{"n":5}]),
            ),
            (
                "SELECT l.value AS n FROM range(1, 4) AS l JOIN generate_series(2, 4) AS r ON l.value = r.value ORDER BY n",
                json!([{"n":2},{"n":3}]),
            ),
        ];
        for surface in ["sql", "python", "dataframe", "cli"] {
            for (sql, expected) in &cases {
                let (success, envelope) = run(surface, sql, "collect", &[]);
                assert!(success, "{surface}: {sql}: {envelope}");
                assert_eq!(rows(&envelope), *expected);
                assert_eq!(field(&envelope, "resident_source_opens"), Some("0"));
                assert_eq!(field(&envelope, "source_io_performed"), Some("false"));
                assert_eq!(
                    field(&envelope, "resident_footer_open_performed_this_call"),
                    Some("false")
                );
                assert_eq!(
                    field(&envelope, "public_workflow_route_id"),
                    Some("native_vortex_relational_collect")
                );
            }
        }
    }

    #[test]
    fn declared_memory_rows_compose_with_sql_range_and_validate_before_output() {
        let binding = json!({"memory://inline": {"input_format":"memory", "memory_input":{
            "kind":"rows", "schema":[["n","int64"],["label","utf8"]], "rows":[["1","alpha"],["3","λ"],["2","beta"]]
        }}}).to_string();
        let args = ["--source-bindings".into(), binding];
        let sql = "SELECT l.label AS label,l.n * 2 AS doubled FROM 'memory://inline' AS l JOIN range(2, 4) AS r ON l.n = r.value ORDER BY doubled DESC";
        let (success, envelope) = run("dataframe", sql, "collect", &args);
        assert!(success, "{envelope}");
        assert_eq!(
            rows(&envelope),
            json!([{"label":"λ","doubled":6},{"label":"beta","doubled":4}])
        );
        assert_eq!(field(&envelope, "resident_source_opens"), Some("0"));
        for sql in [
            "SELECT value FROM range(1, 5, 0)",
            "SELECT value FROM generate_series(-9223372036854775808, 9223372036854775807)",
            "VALUES (1), ('wrong')",
            "VALUES (9007199254740993), (1.5)",
            "SELECT unknown",
        ] {
            let (success, envelope) = run("sql", sql, "collect", &[]);
            assert!(!success, "{sql}: {envelope}");
            assert_ne!(envelope["status"], "success");
            assert_eq!(envelope["fallback"]["attempted"], false);
        }
    }

    #[test]
    fn generated_range_grows_while_small_result_collection_keeps_its_bound() {
        for surface in ["sql", "python", "dataframe", "cli"] {
            let (success, envelope) = run(
                surface,
                "SELECT COUNT(*) AS n, MIN(value) AS first, MAX(value) AS last FROM range(0, 1000017)",
                "collect",
                &[],
            );
            assert!(success, "{surface}: {envelope}");
            assert_eq!(
                rows(&envelope),
                json!([{"n":1_000_017,"first":0,"last":1_000_016}])
            );
            assert_eq!(field(&envelope, "resident_source_opens"), Some("0"));
            assert_eq!(field(&envelope, "source_io_performed"), Some("false"));
        }
        let (success, envelope) = run("sql", "SELECT value FROM range(0, 1000017)", "collect", &[]);
        assert!(!success, "{envelope}");
        assert_ne!(envelope["status"], "success");
        assert_eq!(envelope["fallback"]["attempted"], false);
        assert!(envelope.to_string().contains("65,536"), "{envelope}");
    }

    #[test]
    fn typed_nullable_and_empty_memory_inputs_keep_native_semantics() {
        for (input, sql, expected) in [
            (
                json!({"kind":"rows","schema":[["n","int64"],["label","utf8"]],
                    "rows":[[null,"null"],["9223372036854775807",null],["-9223372036854775808","λ,;%="]]}),
                "SELECT n,label FROM 'memory://input' ORDER BY n ASC NULLS FIRST",
                json!([{"n":null,"label":"null"},{"n":i64::MIN,"label":"λ,;%="},{"n":i64::MAX,"label":null}]),
            ),
            (
                json!({"kind":"rows","schema":[["n","int64"]],"rows":[]}),
                "SELECT n + 1 AS next FROM 'memory://input'",
                json!([]),
            ),
            (
                json!({"kind":"rows","schema":[["n","int64"]],"rows":[]}),
                "SELECT COUNT(*) AS rows,COUNT(n) AS present,SUM(n) AS total FROM 'memory://input'",
                json!([{"rows":0,"present":0,"total":null}]),
            ),
            (
                json!({"kind":"rows","schema":[["n","int64"]],"rows":[[null],[null]]}),
                "SELECT COUNT(*) AS rows,COUNT(n) AS present,SUM(n) AS total FROM 'memory://input'",
                json!([{"rows":2,"present":0,"total":null}]),
            ),
        ] {
            let args = [
                "--source-bindings".into(),
                json!({"memory://input":{
                    "input_format":"memory","memory_input":input,
                }})
                .to_string(),
            ];
            let (success, envelope) = run("python", sql, "collect", &args);
            assert!(success, "{envelope}");
            assert_eq!(rows(&envelope), expected);
            assert_eq!(field(&envelope, "source_io_performed"), Some("false"));
        }
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn memory_query_fanout_reopens_all_eight_formats_through_the_public_engine() {
        use std::{
            fs,
            path::PathBuf,
            time::{SystemTime, UNIX_EPOCH},
        };
        struct Cleanup(PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let root = std::env::temp_dir().join(format!(
            "shardloom-public-memory-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        let _cleanup = Cleanup(root.clone());
        let formats = [
            "vortex",
            "json",
            "jsonl",
            "csv",
            "parquet",
            "arrow_ipc",
            "avro",
            "orc",
        ];
        let paths = formats.map(|format| root.join(format!("output-{format}")));
        let mut args = vec!["--output".into(), paths[0].display().to_string()];
        for (format, path) in formats.iter().zip(&paths).skip(1) {
            args.extend([
                "--fanout-output".into(),
                format!("{format}={}", path.display()),
            ]);
        }
        let (success, envelope) = run(
            "sql",
            "SELECT value * 2 AS n,'λ' AS label FROM range(1, 4) ORDER BY n DESC",
            "write_vortex",
            &args,
        );
        assert!(success, "{envelope}");
        assert_eq!(
            field(&envelope, "native_vortex_result_export_target_count"),
            Some("8")
        );
        assert_eq!(
            field(&envelope, "native_vortex_result_export_execution_count"),
            Some("8")
        );
        assert_eq!(
            field(
                &envelope,
                "native_vortex_result_export_all_targets_committed"
            ),
            Some("true")
        );
        for (format, path) in formats.iter().zip(&paths) {
            assert!(path.is_file());
            let mut args = vec![
                "--input".into(),
                path.display().to_string(),
                "--input-format".into(),
                (*format).into(),
            ];
            if *format == "csv" {
                args.extend(["--source-schema".into(), "n:int64,label:utf8".into()]);
            }
            let (success, result) = run(
                "sql",
                &format!("SELECT n,label FROM '{}' ORDER BY n DESC", path.display()),
                "collect",
                &args,
            );
            assert!(success, "{format}: {result}");
            assert_eq!(
                rows(&result),
                json!([{"n":6,"label":"λ"},{"n":4,"label":"λ"},{"n":2,"label":"λ"}])
            );
        }
        let first = root.join("not-published");
        let (success, envelope) = run(
            "sql",
            "SELECT 1 AS n",
            "write_jsonl",
            &[
                "--output".into(),
                first.display().to_string(),
                "--fanout-output".into(),
                format!("vortex={}", paths[0].display()),
                "--allow-overwrite".into(),
            ],
        );
        assert!(!success, "{envelope}");
        assert!(!first.exists());
        assert!(paths[0].is_file());
    }
}
