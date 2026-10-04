use super::*;
use serde_json::{Value, json};
use shardloom_exec::compute_pool::CancellationToken;

#[path = "sql_native_relational_aggregate_expression_tests.rs"]
mod aggregate_expression_tests;
#[path = "sql_native_relational_dynamic_tests.rs"]
mod dynamic_tests;
#[path = "sql_native_relational_unary_tests.rs"]
mod unary_tests;

fn fixture() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex")
        .display()
        .to_string()
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-sql-composition-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn verify(statement: &str, expected: &Value) {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    policy.resource_envelope.memory_budget_bytes = 32 << 20;
    let prepared = prepare(statement, policy, |path| {
        DatasetUri::new(path.path.to_string_lossy().into_owned())
    })
    .unwrap_or_else(|error| panic!("{statement}: {error}"));
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
    for execution in 1..=2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
        let rows = result
            .result_jsonl
            .value()
            .lines()
            .map(|row| serde_json::from_str::<Value>(row).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(json!(rows), *expected, "{statement}");
        assert_eq!(result.execution.runtime.prepared_source_opens, 1);
        assert_eq!(result.execution.runtime.completed_executions, execution);
    }
}

#[test]
fn native_relational_sql_column_null_selection_preserves_values_and_bind_errors() {
    let source = fixture();
    verify(
        &format!(
            "SELECT value,CASE WHEN value=1 THEN metric ELSE value END AS chosen,value>=2 AS matched,NULLIF(value,value) AS erased,COALESCE(value,metric) AS restored FROM (SELECT * FROM '{source}' LIMIT 2) AS input"
        ),
        &json!([
            {"value":1,"chosen":10,"matched":false,"erased":null,"restored":1},
            {"value":2,"chosen":2,"matched":true,"erased":null,"restored":2},
        ]),
    );
    verify(
        &format!(
            "SELECT value,COALESCE(value,metric) AS chosen,NULLIF(value,value) AS erased,NULLIF(value,metric) AS original FROM '{source}' LIMIT 2"
        ),
        &json!([
            {"value":1,"chosen":1,"erased":null,"original":1},
            {"value":2,"chosen":2,"erased":null,"original":2},
        ]),
    );
    let statement = format!("SELECT COALESCE(value,absent) AS chosen FROM '{source}' LIMIT 0");
    assert!(
        prepare(
            &statement,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            |path| DatasetUri::new(path.path.to_string_lossy().into_owned())
        )
        .is_err()
    );
    let statement = "SELECT COALESCE(first,second) AS chosen FROM '/missing/native.vortex' LIMIT 0";
    assert!(is_plain_select(statement).unwrap());
}

#[test]
fn native_relational_sql_trailing_offset_preserves_order_and_empty_binding() {
    let source = fixture();
    verify(
        &format!("SELECT value FROM '{source}' ORDER BY value DESC LIMIT 2 OFFSET 1"),
        &json!([{"value":4},{"value":3}]),
    );
    verify(
        &format!("SELECT value FROM '{source}' ORDER BY value DESC LIMIT 2 OFFSET 99"),
        &json!([]),
    );
    verify(
        &format!("SELECT value FROM '{source}' ORDER BY value DESC LIMIT 0 OFFSET 1"),
        &json!([]),
    );
    assert_eq!(
        source_count("SELECT value FROM '/missing/offset.vortex' LIMIT 1 OFFSET 2").unwrap(),
        1
    );
    for suffix in ["OFFSET -1", "OFFSET 1.5", "OFFSET 1 OFFSET 2"] {
        assert!(
            parsed_native_query(&format!("SELECT value FROM '{source}' LIMIT 2 {suffix}")).is_err()
        );
    }
    assert!(
        prepare(
            &format!("SELECT absent FROM '{source}' LIMIT 0 OFFSET 1"),
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            |path| DatasetUri::new(path.path.to_string_lossy().into_owned())
        )
        .is_err()
    );
}

#[test]
fn native_relational_sql_replace_or_add_binds_the_previous_schema() {
    let source = fixture();
    verify(
        &format!(
            "SELECT * REPLACE OR ADD (value + 10 AS value,value + 1 AS original_plus_one) FROM '{source}' LIMIT 2"
        ),
        &json!([{"value":11,"metric":10,"original_plus_one":2},{"value":12,"metric":20,"original_plus_one":3}]),
    );
    verify(
        &format!(
            "SELECT * REPLACE OR ADD (value + 1 AS value) FROM (SELECT * REPLACE OR ADD (value * 2 AS value) FROM '{source}') AS doubled LIMIT 2"
        ),
        &json!([{"value":3,"metric":10},{"value":5,"metric":20}]),
    );
    let prepared = prepare(
        &format!(
            "SELECT * REPLACE OR ADD (value + 1 AS a) FROM (SELECT value AS a,value AS b FROM '{source}') AS renamed"
        ),
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        |path| DatasetUri::new(path.path.to_string_lossy().into_owned()),
    );
    assert!(
        prepared
            .err()
            .expect("unknown input must fail even when replaced")
            .to_string()
            .contains("not present")
    );
    for projection in [
        "value",
        "*",
        "value AS x,value AS x",
        "SUM(value) AS total",
        "ROW_NUMBER() OVER (ORDER BY value) AS rn",
    ] {
        assert!(
            is_relational(&format!(
                "SELECT * REPLACE OR ADD ({projection}) FROM 'missing.vortex'"
            ))
            .is_err(),
            "{projection}"
        );
    }
}

#[test]
fn native_relational_sql_derived_stages_preserve_limit_and_join_order() {
    let source = fixture();
    verify(
        &format!(
            "SELECT value FROM (SELECT value FROM '{source}' ORDER BY value DESC LIMIT 2) AS q WHERE value < 5"
        ),
        &json!([{"value":4}]),
    );
    verify(
        &format!(
            "SELECT l.value AS n,r.value AS m FROM (SELECT value FROM '{source}' ORDER BY value DESC LIMIT 2) AS l LEFT JOIN (SELECT value FROM '{source}' WHERE value < 5) AS r ON l.value = r.value ORDER BY n ASC"
        ),
        &json!([{"n":4,"m":4},{"n":5,"m":null}]),
    );
    verify(
        &format!(
            "SELECT q.n FROM (SELECT l.value AS n FROM '{source}' AS l JOIN '{source}' AS r ON l.value = r.value WHERE r.value > 3) AS q ORDER BY q.n DESC"
        ),
        &json!([{"q.n":5},{"q.n":4}]),
    );
}

#[test]
fn native_relational_sql_derived_windows_aggregates_and_set_branches_compose() {
    let source = fixture();
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE EXISTS (SELECT 1 FROM (SELECT value FROM '{source}' ORDER BY value DESC LIMIT 2) AS q WHERE value = outer.value) ORDER BY value ASC"
        ),
        &json!([{"value":4},{"value":5}]),
    );
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE value IN (SELECT value FROM (SELECT * FROM (SELECT value FROM '{source}' LIMIT 1) AS a UNION ALL SELECT * FROM (SELECT value FROM '{source}' ORDER BY value DESC LIMIT 1) AS b) AS combined) ORDER BY value ASC"
        ),
        &json!([{"value":1},{"value":5}]),
    );
    verify(
        &format!(
            "SELECT value FROM (SELECT value, ROW_NUMBER() OVER (ORDER BY value DESC) AS rn FROM '{source}') AS ranked WHERE rn <= 2 ORDER BY value ASC"
        ),
        &json!([{"value":4},{"value":5}]),
    );
    verify(
        &format!(
            "SELECT total FROM (SELECT SUM(value) AS total FROM (SELECT value FROM '{source}' LIMIT 3) AS first_rows) AS totals WHERE total > 5.0"
        ),
        &json!([{"total":6.0}]),
    );
    verify(
        &format!(
            "SELECT value FROM (SELECT value FROM (SELECT value FROM '{source}' ORDER BY value DESC LIMIT 2) AS a UNION ALL SELECT value FROM (SELECT value FROM '{source}' LIMIT 2) AS b) AS combined WHERE value != 4"
        ),
        &json!([{"value":5},{"value":1},{"value":2}]),
    );
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE value IN (SELECT value FROM (SELECT value FROM '{source}' ORDER BY value DESC LIMIT 2) AS q) ORDER BY value ASC"
        ),
        &json!([{"value":4},{"value":5}]),
    );
}

#[test]
fn native_relational_sql_derived_discovery_and_binding_are_scope_aware() {
    let statement = "SELECT x.value FROM (SELECT value FROM 'missing-a.vortex' UNION ALL SELECT value FROM 'missing-b.vortex') AS x WHERE value IN (SELECT value FROM (SELECT value FROM 'missing-c.vortex') AS y)";
    assert!(is_relational(statement).unwrap());
    assert_eq!(source_count(statement).unwrap(), 3);
    let source = fixture();
    for (sql, message) in [
        (
            format!("SELECT * FROM (SELECT value AS renamed FROM '{source}') AS q WHERE value > 0"),
            "not present",
        ),
        (
            format!("SELECT value FROM '{source}' AS l JOIN '{source}' AS r ON l.value = r.value"),
            "ambiguous",
        ),
        (
            format!(
                "SELECT l.value FROM (SELECT l.value AS n FROM '{source}' AS l JOIN '{source}' AS r ON l.value = r.value) AS q"
            ),
            "not present",
        ),
    ] {
        let error = prepare(
            &sql,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            |path| DatasetUri::new(path.path.to_string_lossy().into_owned()),
        )
        .err()
        .expect("invalid scope must fail");
        assert!(error.to_string().contains(message), "{sql}: {error}");
    }
}

#[test]
fn native_relational_sql_derived_cancellation_consumer_failure_and_generation_cleanup() {
    let directory = TestDirectory::new();
    let left = directory.0.join("left.vortex");
    let right = directory.0.join("right.vortex");
    fs::copy(fixture(), &left).unwrap();
    fs::copy(fixture(), &right).unwrap();
    let sql = format!(
        "SELECT value FROM (SELECT l.value AS value FROM (SELECT value FROM '{}' LIMIT 2) AS l JOIN (SELECT value FROM '{}') AS r ON l.value = r.value) AS q",
        left.display(),
        right.display()
    );
    let prepared = prepare(
        &sql,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        |path| DatasetUri::new(path.path.to_string_lossy().into_owned()),
    )
    .unwrap();
    assert_eq!(prepared.snapshot().prepared_source_opens, 2);
    let reserved = prepared.snapshot().memory.reserved_bytes;
    let pre_cancel = CancellationToken::default();
    pre_cancel.cancel();
    assert!(prepared.collect_jsonl(&pre_cancel).is_err());
    let mut consumed = false;
    let failure = prepared
        .for_each_batch(&CancellationToken::default(), |_, _| {
            consumed = true;
            Err(unsupported_sql_error("test consumer failed"))
        })
        .err()
        .unwrap();
    assert!(consumed && failure.to_string().contains("test consumer failed"));
    assert_eq!(prepared.snapshot().memory.reserved_bytes, reserved);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    let mid_cancel = CancellationToken::default();
    assert!(
        prepared
            .for_each_batch(&mid_cancel, |_, _| {
                mid_cancel.cancel();
                Ok(())
            })
            .is_err()
    );
    assert_eq!(prepared.snapshot().memory.reserved_bytes, reserved);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    let collected = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(collected.execution.output_columns, ["value"]);
    assert_eq!(collected.execution.output_rows, 2);
    assert_eq!(prepared.snapshot().completed_executions, 1);
    drop(collected);
    let mut replaced = false;
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                let replacement = directory.0.join("replacement.vortex");
                fs::copy(&right, &replacement).unwrap();
                fs::rename(replacement, &right).unwrap();
                replaced = true;
                Ok(())
            })
            .is_err()
    );
    assert!(replaced);
    assert_eq!(prepared.snapshot().completed_executions, 1);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, reserved);
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
}

#[test]
fn native_relational_sql_derived_resource_denial_and_empty_schema_are_explicit() {
    let source = fixture();
    let sql = format!(
        "SELECT * FROM (SELECT value AS renamed FROM '{source}' ORDER BY value DESC) AS q LIMIT 0"
    );
    let prepared = prepare(
        &sql,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        |path| DatasetUri::new(path.path.to_string_lossy().into_owned()),
    )
    .unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(result.execution.output_columns, ["renamed"]);
    assert_eq!(result.execution.output_rows, 0);
    let mut policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    policy.resource_envelope.memory_budget_bytes = 1;
    assert!(
        prepare(&sql, policy, |path| DatasetUri::new(
            path.path.to_string_lossy().into_owned()
        ))
        .is_err()
    );
}

#[test]
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn native_relational_sql_every_writer_protects_nested_source_aliases() {
    use shardloom_vortex::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    let directory = TestDirectory::new();
    let left = directory.0.join("left.vortex");
    let right = directory.0.join("right.vortex");
    let link = directory.0.join("right-hardlink.vortex");
    let symlink = directory.0.join("right-symlink.vortex");
    fs::copy(fixture(), &left).unwrap();
    fs::copy(fixture(), &right).unwrap();
    fs::hard_link(&right, &link).unwrap();
    std::os::unix::fs::symlink(&right, &symlink).unwrap();
    let sql = format!(
        "SELECT value FROM (SELECT value FROM '{}' UNION ALL SELECT value FROM (SELECT value FROM '{}') AS nested) AS combined",
        left.display(),
        right.display()
    );
    let prepared = prepare(
        &sql,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        |path| DatasetUri::new(path.path.to_string_lossy().into_owned()),
    )
    .unwrap();
    let original = fs::read(&right).unwrap();
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    for format in [
        Format::Vortex,
        Format::Parquet,
        Format::ArrowIpc,
        Format::Avro,
        Format::Orc,
        Format::Json,
        Format::Jsonl,
        Format::Csv,
    ] {
        for path in [&left, &right, &link, &symlink] {
            let error = prepared.write(path, format, true).err().unwrap();
            assert!(error.to_string().contains("source"));
        }
        let destination = directory.0.join(format!("cancelled.{}", format.as_str()));
        assert!(
            prepared
                .write_controlled(&destination, format, false, &cancelled)
                .is_err()
        );
        assert!(!destination.exists());
    }
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(fs::read(&right).unwrap(), original);
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 4);
}

#[test]
fn native_relational_sql_join_predicates_and_grouped_having_execute_shared_native_nodes() {
    let source = fixture();
    verify(
        &format!(
            "SELECT l.value, r.value FROM '{source}' AS l LEFT JOIN '{source}' AS r ON l.value < r.value WHERE l.value >= 4 LIMIT 20"
        ),
        &json!([
            {"l.value":4,"r.value":5},{"l.value":5,"r.value":null}
        ]),
    );
    verify(
        &format!(
            "SELECT l.value, COUNT(*) AS rows FROM '{source}' AS l JOIN '{source}' AS r ON l.value < r.value GROUP BY l.value HAVING COUNT(*) > 2 ORDER BY rows DESC LIMIT 20"
        ),
        &json!([
            {"l.value":1,"rows":4},{"l.value":2,"rows":3}
        ]),
    );
    verify(
        &format!(
            "SELECT l.value FROM '{source}' AS l LEFT ANTI JOIN '{source}' AS r ON l.value < r.value LIMIT 20"
        ),
        &json!([{"l.value":5}]),
    );
}

#[test]
fn native_relational_sql_literal_on_precedes_full_join_null_extension() {
    let source = fixture();
    verify(
        &format!(
            "SELECT l.value,r.value FROM '{source}' AS l FULL JOIN '{source}' AS r ON l.value = r.value AND l.value > 3"
        ),
        &json!([
            {"l.value":1,"r.value":null},{"l.value":2,"r.value":null},
            {"l.value":3,"r.value":null},{"l.value":4,"r.value":4},
            {"l.value":5,"r.value":5},{"l.value":null,"r.value":1},
            {"l.value":null,"r.value":2},{"l.value":null,"r.value":3}
        ]),
    );
}

#[test]
fn native_relational_sql_extracts_only_unconditional_cross_input_equality_keys() {
    for (condition, expected) in [
        (
            "left.value = right.value AND left.value > 3",
            vec![("value", "value")],
        ),
        (
            "right.value = left.value AND right.value > 3",
            vec![("value", "value")],
        ),
        (
            "left.value = right.value AND (left.other = right.other OR left.value > 3)",
            vec![("value", "value")],
        ),
        ("left.value = right.value OR left.value > 3", vec![]),
        ("left.value = left.other AND left.value > 3", vec![]),
        (
            "left.value = right.value AND right.value = left.value AND left.other = right.other AND left.value > 3",
            vec![("value", "value"), ("other", "other")],
        ),
    ] {
        let sql_condition = condition.replace("left.", "l.").replace("right.", "r.");
        let parsed = parse_sql_local_source_statement(&format!(
            "SELECT l.value FROM '/missing/left.vortex' AS l JOIN '/missing/right.vortex' AS r ON {sql_condition} LIMIT 10"
        )).unwrap();
        let mut expression = parsed
            .join
            .unwrap()
            .on_predicate
            .unwrap()
            .to_expression()
            .unwrap();
        map_columns(&mut expression, &mut |name| {
            Ok(name.replacen("l.", "left.", 1).replacen("r.", "right.", 1))
        })
        .unwrap();
        let mut keys = Vec::new();
        append_equality_keys(&expression, &mut keys).unwrap();
        assert_eq!(
            keys.iter()
                .map(|key| (key.left.as_str(), key.right.as_str()))
                .collect::<Vec<_>>(),
            expected,
            "{condition}"
        );
    }
    let source = fixture();
    verify(
        &format!(
            "SELECT l.value,r.value FROM '{source}' AS l JOIN '{source}' AS r ON l.value = r.value OR l.value = 1"
        ),
        &json!([
            {"l.value":1,"r.value":1},{"l.value":1,"r.value":2},
            {"l.value":1,"r.value":3},{"l.value":1,"r.value":4},
            {"l.value":1,"r.value":5},{"l.value":2,"r.value":2},
            {"l.value":3,"r.value":3},{"l.value":4,"r.value":4},
            {"l.value":5,"r.value":5}
        ]),
    );
}

#[test]
fn native_relational_sql_column_aliases_preserve_source_values_without_arithmetic() {
    let source = fixture();
    verify(
        &format!(
            "SELECT l.value AS left_value,r.value AS right_value FROM '{source}' AS l LEFT JOIN '{source}' AS r ON l.value < r.value WHERE l.value >= 4"
        ),
        &json!([{"left_value":4,"right_value":5},{"left_value":5,"right_value":null}]),
    );
}

#[test]
fn native_relational_sql_scalar_aggregate_subquery_needs_no_grouping_or_having() {
    let source = fixture();
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE value IN (SELECT COUNT(*) AS n FROM '{source}' WHERE value <= outer.value)"
        ),
        &json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]),
    );
}

#[test]
fn native_relational_sql_limit_admission_does_not_expand_reference_materialization() {
    let source = fixture();
    let statement = format!(
        "SELECT value FROM '{source}' WHERE value IN (SELECT value FROM '{source}' LIMIT 64) LIMIT 10"
    );
    verify(
        &statement,
        &json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]),
    );
    let parsed = parse_sql_local_source_statement(&statement).unwrap();
    let ParsedPredicate::InSubquery { mut subquery, .. } = parsed.predicate else {
        panic!("expected IN");
    };
    let rejected = materialize_in_subquery(
        &mut subquery,
        None,
        SqlLocalSourceRuntimeProfile::Smoke.read_limits(),
    )
    .unwrap_err();
    assert!(
        rejected
            .to_string()
            .contains("decoded reference subquery LIMIT")
    );
}

#[test]
fn native_relational_sql_exists_binds_selected_columns_before_reducing_to_presence() {
    let source = fixture();
    let statement =
        format!("SELECT value FROM '{source}' WHERE EXISTS (SELECT absent_column FROM '{source}')");
    let result = prepare(
        &statement,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        |path| DatasetUri::new(path.path.to_string_lossy().into_owned()),
    );
    assert!(result.is_err());
    assert!(result.err().unwrap().to_string().contains("absent_column"));
}

#[test]
fn native_relational_sql_sets_align_names_preserve_multiplicity_and_apply_only_global_limit() {
    let source = fixture();
    for (operator, expected) in [
        (
            "UNION ALL",
            json!([{"value":1},{"value":2},{"value":3},{"value":3},{"value":4},{"value":5}]),
        ),
        (
            "UNION",
            json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]),
        ),
        ("INTERSECT", json!([{"value":3}])),
        ("EXCEPT", json!([{"value":1},{"value":2}])),
    ] {
        verify(
            &format!(
                "SELECT value FROM '{source}' WHERE value <= 3 {operator} SELECT value + 0 AS renamed FROM '{source}' WHERE value >= 3 ORDER BY value ASC LIMIT 20"
            ),
            &expected,
        );
    }
    verify(
        &format!(
            "SELECT DISTINCT l.value FROM '{source}' AS l JOIN '{source}' AS r ON l.value < r.value ORDER BY l.value DESC LIMIT 2"
        ),
        &json!([{"l.value":4},{"l.value":3}]),
    );
}

#[test]
fn native_relational_sql_windows_keep_input_order_and_can_order_by_the_result_alias() {
    let source = fixture();
    verify(
        &format!(
            "SELECT value, ROW_NUMBER() OVER (ORDER BY value DESC) AS position FROM '{source}' LIMIT 10"
        ),
        &json!([
            {"value":1,"position":5},{"value":2,"position":4},{"value":3,"position":3},{"value":4,"position":2},{"value":5,"position":1}
        ]),
    );
    verify(
        &format!(
            "SELECT value, LEAD(value, 2) OVER (ORDER BY value ASC) AS next FROM '{source}' ORDER BY next ASC NULLS LAST LIMIT 10"
        ),
        &json!([
            {"value":1,"next":3},{"value":2,"next":4},{"value":3,"next":5},{"value":4,"next":null},{"value":5,"next":null}
        ]),
    );
}

#[test]
fn native_relational_sql_subqueries_lower_relations_instead_of_unfilled_value_caches() {
    let source = fixture();
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE value IN (SELECT value FROM '{source}' WHERE value > 3) LIMIT 10"
        ),
        &json!([{"value":4},{"value":5}]),
    );
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE value > ALL (SELECT value FROM '{source}' WHERE value < 3) LIMIT 10"
        ),
        &json!([{"value":3},{"value":4},{"value":5}]),
    );
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE EXISTS (SELECT 1 FROM '{source}' WHERE value > outer.value) LIMIT 10"
        ),
        &json!([{"value":1},{"value":2},{"value":3},{"value":4}]),
    );
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE value IN (SELECT value FROM '{source}' WHERE value <= outer.value ORDER BY value DESC LIMIT 1) LIMIT 10"
        ),
        &json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]),
    );
}

#[test]
fn native_relational_sql_projected_correlated_aggregate_and_predicate_projection_compose() {
    let source = fixture();
    verify(
        &format!(
            "SELECT value FROM '{source}' WHERE value IN (SELECT COUNT(*) AS n FROM '{source}' WHERE value <= outer.value HAVING COUNT(*) >= 1) LIMIT 10"
        ),
        &json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]),
    );
    verify(
        &format!(
            "SELECT value, CASE WHEN value IN (SELECT value FROM '{source}' WHERE value > 3) THEN 'yes' ELSE 'no' END AS membership FROM '{source}' LIMIT 10"
        ),
        &json!([
            {"value":1,"membership":"no"},{"value":2,"membership":"no"},{"value":3,"membership":"no"},{"value":4,"membership":"yes"},{"value":5,"membership":"yes"}
        ]),
    );
}

#[test]
fn native_relational_sql_shape_inspection_does_not_open_missing_sources() {
    assert!(is_relational("SELECT l.value FROM '/missing/left.vortex' AS l JOIN '/missing/right.vortex' AS r ON l.value = r.value").unwrap());
    assert!(is_relational("SELECT value FROM '/missing/left.vortex' UNION SELECT value FROM '/missing/right.vortex'").unwrap());
    assert!(!is_relational("SELECT value FROM '/missing/left.vortex' LIMIT 1").unwrap());
    assert_eq!(source_count("SELECT value FROM '/missing/left.csv' WHERE value IN (SELECT value FROM '/missing/right.vortex' WHERE value IN (SELECT value FROM '/missing/nested.jsonl'))").unwrap(), 3);
    assert_eq!(source_count("SELECT value FROM '/missing/left.vortex' UNION SELECT value FROM '/missing/left.vortex' UNION SELECT value FROM '/missing/right.vortex'").unwrap(), 2);
    assert_eq!(source_count("SELECT CASE WHEN EXISTS (SELECT l.value FROM '/missing/left.vortex' AS l JOIN '/missing/right.vortex' AS r ON l.value = r.value) THEN 1 ELSE 0 END AS present FROM '/missing/outer.vortex'").unwrap(), 3);
}
