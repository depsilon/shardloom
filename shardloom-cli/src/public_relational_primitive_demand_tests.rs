//! Explicit payloads retain their meaning through the ordinary direct source owner.
use super::*;

fn collect_primitive(
    fixture: &Fixture,
    primitive: &str,
    options: &[(&str, &str)],
) -> serde_json::Value {
    try_collect_primitive(fixture, primitive, options).unwrap()
}

fn try_collect_primitive(
    fixture: &Fixture,
    primitive: &str,
    options: &[(&str, &str)],
) -> Result<serde_json::Value, ShardLoomError> {
    let mut arguments = vec![
        "sql".to_owned(),
        "--input".into(),
        fixture.path().display().to_string(),
        "--input-format".into(),
        "parquet".into(),
        "--request".into(),
        "collect".into(),
        "--bounded".into(),
        "true".into(),
        "--memory-bytes".into(),
        (32_u64 << 20).to_string(),
        "--max-parallelism".into(),
        "1".into(),
        "--vortex-primitive".into(),
        primitive.into(),
    ];
    for (flag, value) in options {
        arguments.extend([(*flag).into(), (*value).into()]);
    }
    let request = PublicWorkflowRouteRequest::parse(arguments.into_iter()).unwrap();
    let plan = super::super::super::super::plan_public_workflow_route(&request);
    assert_eq!(
        plan.route_id, "native_vortex_relational_collect",
        "{primitive}"
    );
    let policy = native_vortex_materializing_policy(&request).unwrap();
    let session = shardloom_vortex::resident_session::ResidentVortexSession::new(
        policy.resource_envelope.memory_budget_bytes,
        policy.max_parallelism,
    )
    .unwrap();
    let (operation, reusable, normalized) =
        prepare_primitive_in_session(&request, &session, &CancellationToken::default())?;
    assert!(reusable);
    assert_eq!(normalized, 1);
    let mut observations = Vec::new();
    for _ in 0..2 {
        let result = operation.collect_jsonl(&CancellationToken::default())?;
        let rows = result
            .result_jsonl
            .value()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(result.execution.input.as_ref().unwrap().columnar_sources, 1);
        assert!(
            !result
                .execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
        assert!(!Fixture::target(&fixture.path()).exists());
        observations.push(serde_json::json!(rows));
    }
    assert_eq!(observations[0], observations[1]);
    Ok(observations.remove(0))
}

#[test]
fn explicit_primitive_direct_source_keeps_payload_and_never_creates_preparation() {
    let fixture = Fixture::new();
    assert_eq!(
        collect_primitive(&fixture, "count", &[]),
        serde_json::json!([{"count_all":4}])
    );
    assert_eq!(
        collect_primitive(
            &fixture,
            "project",
            &[
                ("--vortex-columns", "value"),
                ("--vortex-source-order-limit", "2"),
                ("--sql", "SELECT unused FROM source.parquet"),
            ]
        ),
        serde_json::json!([{"value":100},{"value":101}])
    );
    assert_eq!(
        collect_primitive(
            &fixture,
            "tail",
            &[
                ("--vortex-columns", "id"),
                ("--vortex-source-order-limit", "2"),
            ]
        ),
        serde_json::json!([{"id":2},{"id":3}])
    );
    assert_eq!(
        collect_primitive(
            &fixture,
            "aggregate",
            &[(
                "--vortex-aggregate",
                r#"{"measures":[{"function":"sum","column":"value","alias":"total"}]}"#
            ),]
        ),
        serde_json::json!([{"total":406.0}])
    );
    assert_eq!(
        collect_primitive(
            &fixture,
            "sort_rows",
            &[
                ("--vortex-columns", "id,value"),
                (
                    "--vortex-sort-rows",
                    r#"{"order_by":[{"column":"id","descending":true}],"offset":1}"#
                ),
                ("--vortex-source-order-limit", "2"),
            ]
        ),
        serde_json::json!([{"id":2,"value":102},{"id":1,"value":101}])
    );
}

#[test]
fn primitive_aggregate_direct_source_keeps_group_transforms_offsets_and_having() {
    let fixture = Fixture::new();
    let aggregate = r#"{"group_expressions":[{"column":"unused","function":"length","alias":"width"}],"measures":[{"function":"sum","column":"value","alias":"total","argument_offset":2}],"having":[{"column":"total","op":"gt","value":"400"}],"order_by":[{"column":"total","descending":true}]}"#;
    assert_eq!(
        collect_primitive(&fixture, "aggregate", &[("--vortex-aggregate", aggregate)]),
        serde_json::json!([{"width":8,"total":414.0}])
    );
}

#[test]
fn primitive_sort_direct_source_preserves_each_tie_policy() {
    let fixture = Fixture::new();
    let names = vec!["id".into(), "key".into()];
    let rows = [1, 0, 0, 0, 2]
        .into_iter()
        .enumerate()
        .map(|(id, key)| {
            vec![
                ("id".into(), ScalarValue::Int64(i64::try_from(id).unwrap())),
                ("key".into(), ScalarValue::Int64(key)),
            ]
        })
        .collect::<Vec<_>>();
    fs::write(
        fixture.path(),
        shardloom_vortex::encode_flat_parquet_rows(&names, &rows).unwrap(),
    )
    .unwrap();
    for (policy, expected) in [
        (
            "first",
            serde_json::json!([{"id":1,"key":0},{"id":2,"key":0}]),
        ),
        (
            "last",
            serde_json::json!([{"id":3,"key":0},{"id":2,"key":0}]),
        ),
        (
            "all",
            serde_json::json!([{"id":1,"key":0},{"id":2,"key":0},{"id":3,"key":0}]),
        ),
    ] {
        let sort = format!(
            r#"{{"order_by":[{{"column":"key","descending":false}}],"tie_policy":"{policy}"}}"#
        );
        for projection in ["id,key", "id"] {
            let mut expected = expected.clone();
            if projection == "id" {
                for row in expected.as_array_mut().unwrap() {
                    row.as_object_mut().unwrap().remove("key");
                }
            }
            assert_eq!(
                collect_primitive(
                    &fixture,
                    "sort_rows",
                    &[
                        ("--vortex-columns", projection),
                        ("--vortex-sort-rows", &sort),
                        ("--vortex-source-order-limit", "2"),
                    ]
                ),
                expected,
                "{policy}, {projection}"
            );
        }
    }
}

#[test]
#[allow(clippy::cast_precision_loss)] // Independent expected values reproduce the declared Float64 promotion.
fn primitive_measure_offsets_keep_full_width_numeric_conversion_and_nulls() {
    let fixture = Fixture::new();
    let values = [
        ScalarValue::UInt64((1_u64 << 53) + 1),
        ScalarValue::Null,
        ScalarValue::UInt64(u64::MAX),
    ];
    let rows = values
        .into_iter()
        .map(|value| vec![("value".into(), value)])
        .collect::<Vec<_>>();
    fs::write(
        fixture.path(),
        shardloom_vortex::encode_flat_parquet_rows(&["value".into()], &rows).unwrap(),
    )
    .unwrap();
    let total = (((1_u64 << 53) + 1) as f64 + 2.0) + (u64::MAX as f64 + 2.0);
    for (function, expected) in [("sum", total), ("avg", total / 2.0)] {
        let spec = serde_json::json!({"measures":[{"function":function,"column":"value","alias":"value","argument_offset":2}]}).to_string();
        assert_eq!(
            collect_primitive(&fixture, "aggregate", &[("--vortex-aggregate", &spec)]),
            serde_json::json!([{"value":expected}]),
        );
    }
}

#[test]
fn primitive_conditional_grouping_keeps_required_null_errors_and_lazy_referer() {
    let fixture = Fixture::new();
    let spec = r#"{"group_expressions":[{"column":"referer","function":"case_search_adv_zero_referer_else_empty","extra_columns":["search","advertising"],"alias":"key"}],"measures":[{"function":"count","alias":"n"}],"order_by":[{"column":"key"}]}"#;
    let write = |values: Vec<[ScalarValue; 3]>| {
        let names = vec!["referer".into(), "search".into(), "advertising".into()];
        let rows = values
            .into_iter()
            .map(|row| names.iter().cloned().zip(row).collect())
            .collect::<Vec<_>>();
        fs::write(
            fixture.path(),
            shardloom_vortex::encode_flat_parquet_rows(&names, &rows).unwrap(),
        )
        .unwrap();
    };
    write(vec![
        [
            ScalarValue::Utf8("selected".into()),
            ScalarValue::Int64(0),
            ScalarValue::Int64(0),
        ],
        [
            ScalarValue::Null,
            ScalarValue::Int64(1),
            ScalarValue::Int64(0),
        ],
        [
            ScalarValue::Null,
            ScalarValue::Int64(0),
            ScalarValue::Int64(1),
        ],
    ]);
    assert_eq!(
        collect_primitive(&fixture, "aggregate", &[("--vortex-aggregate", spec)]),
        serde_json::json!([{"key":"","n":2},{"key":"selected","n":1}])
    );
    for values in [
        [
            ScalarValue::Utf8("unused".into()),
            ScalarValue::Null,
            ScalarValue::Int64(1),
        ],
        [
            ScalarValue::Utf8("unused".into()),
            ScalarValue::Int64(1),
            ScalarValue::Null,
        ],
        [
            ScalarValue::Null,
            ScalarValue::Int64(0),
            ScalarValue::Int64(0),
        ],
        [
            ScalarValue::Utf8("unused".into()),
            ScalarValue::UInt64(u64::MAX),
            ScalarValue::Int64(1),
        ],
    ] {
        write(vec![values]);
        assert!(
            try_collect_primitive(&fixture, "aggregate", &[("--vortex-aggregate", spec)]).is_err()
        );
        assert!(!Fixture::target(&fixture.path()).exists());
    }
}

#[test]
fn primitive_group_alias_does_not_replace_a_measure_input() {
    let fixture = Fixture::new();
    let spec = r#"{"group_expressions":[{"column":"id","function":"add_offset","argument_offset":1,"alias":"value"}],"measures":[{"function":"sum","column":"value","alias":"total"}],"order_by":[{"column":"value"}]}"#;
    assert_eq!(
        collect_primitive(&fixture, "aggregate", &[("--vortex-aggregate", spec)]),
        serde_json::json!([
            {"value":1,"total":100.0}, {"value":2,"total":101.0}, {"value":3,"total":102.0}, {"value":4,"total":103.0},
        ])
    );
}

#[test]
fn primitive_group_offsets_preserve_unsigned_and_signed_boundary_values() {
    let fixture = Fixture::new();
    let rows = [i64::MAX.unsigned_abs() + 1, i64::MAX.unsigned_abs() + 2]
        .into_iter()
        .map(|value| vec![("value".into(), ScalarValue::UInt64(value))])
        .collect::<Vec<_>>();
    fs::write(
        fixture.path(),
        shardloom_vortex::encode_flat_parquet_rows(&["value".into()], &rows).unwrap(),
    )
    .unwrap();
    for (function, offset, expected) in [
        (
            "add_offset",
            -2,
            serde_json::json!([{"key":i64::MAX.unsigned_abs() - 1,"n":1},{"key":i64::MAX.unsigned_abs(),"n":1}]),
        ),
        (
            "add_signed_offset",
            -2,
            serde_json::json!([{"key":i64::MAX - 1,"n":1},{"key":i64::MAX,"n":1}]),
        ),
    ] {
        let spec = serde_json::json!({"group_expressions":[{"column":"value","function":function,"argument_offset":offset,"alias":"key"}],"measures":[{"function":"count","alias":"n"}],"order_by":[{"column":"key"}]}).to_string();
        assert_eq!(
            collect_primitive(&fixture, "aggregate", &[("--vortex-aggregate", &spec)]),
            expected,
            "{function}"
        );
    }
}

#[test]
#[allow(clippy::cast_precision_loss)] // Primitive HAVING historically compares mixed numbers as Float64.
fn primitive_having_preserves_null_mixed_family_and_large_float_ordering() {
    let fixture = Fixture::new();
    let rows = vec![
        vec![
            ("id".into(), ScalarValue::Int64(0)),
            ("value".into(), ScalarValue::Null),
        ],
        vec![
            ("id".into(), ScalarValue::Int64(1)),
            ("value".into(), ScalarValue::UInt64(u64::MAX)),
        ],
    ];
    fs::write(
        fixture.path(),
        shardloom_vortex::encode_flat_parquet_rows(&["id".into(), "value".into()], &rows).unwrap(),
    )
    .unwrap();
    let null = r#"{"group_by":["id"],"measures":[{"function":"min","column":"value","alias":"value"}],"having":[{"column":"value","op":"lt","value":"0"}]}"#;
    assert_eq!(
        collect_primitive(&fixture, "aggregate", &[("--vortex-aggregate", null)]),
        serde_json::json!([{"id":0,"value":null}])
    );
    for (op, value, included) in [
        ("eq", "18446744073709551615", true),
        ("lt", "text", true),
        ("gt", "true", true),
        ("gt", "text", false),
    ] {
        let spec = serde_json::json!({"measures":[{"function":"sum","column":"value","alias":"total"}],"having":[{"column":"total","op":op,"value":value}]}).to_string();
        let expected = if included {
            serde_json::json!([{"total":u64::MAX as f64}])
        } else {
            serde_json::json!([])
        };
        assert_eq!(
            collect_primitive(&fixture, "aggregate", &[("--vortex-aggregate", &spec)]),
            expected,
            "{op} {value}"
        );
    }
}
