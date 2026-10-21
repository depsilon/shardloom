//! Exact projection normalization preserves validation and complete native output.

use super::*;

fn fixture(rows: usize) -> Fixture {
    Fixture::new(
        StructArray::try_new(
            FieldNames::from(["z", "a"]),
            vec![
                PrimitiveArray::from_option_iter([Some(7_i64), None, Some(-2)])
                    .into_array()
                    .slice(0..rows)
                    .unwrap(),
                VarBinViewArray::from_iter(
                    [Some("first"), Some("second"), None],
                    DType::Utf8(Nullability::Nullable),
                )
                .into_array()
                .slice(0..rows)
                .unwrap(),
            ],
            rows,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        1,
    )
}

fn root(prepared: &PreparedVortexRelational) -> &Node {
    let PreparedRoot::Bound(root) = &prepared.root else {
        panic!("static projection must stay statically bound")
    };
    root
}

#[test]
fn native_identity_projection_normalizes_nested_nullable_and_empty_input() {
    for rows in [0, 3] {
        let fixture = fixture(rows);
        let mut plan = fixture.scan();
        for _ in 0..2 {
            plan = project(plan, vec![("z", col("z")), ("a", col("a"))]);
        }
        let prepared = prepare_relational(&plan, policy()).unwrap();
        assert!(matches!(root(&prepared).kind, NodeKind::Scan { .. }));
        assert_eq!(
            root(&prepared).fields,
            vec![
                (
                    "z".into(),
                    DType::Primitive(PType::I64, Nullability::Nullable)
                ),
                ("a".into(), DType::Utf8(Nullability::Nullable)),
            ]
        );
        let expected = [
            serde_json::json!({"z":7,"a":"first"}),
            serde_json::json!({"z":null,"a":"second"}),
            serde_json::json!({"z":-2,"a":null}),
        ];
        let memory = prepared.session.memory().clone();
        for _ in 0..2 {
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(json_rows(&result), expected[..rows]);
            assert!(result.execution.native_io_certificate.is_certified());
        }
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 0);
    }
}

#[test]
fn native_identity_projection_keeps_reordered_renamed_subset_and_computed_fields() {
    let fixture = fixture(3);
    let original = collected(&fixture.scan());
    for (expressions, names, expected) in [
        (
            vec![("a", col("a")), ("z", col("z"))],
            vec!["a", "z"],
            original.clone(),
        ),
        (
            vec![("renamed", col("z")), ("a", col("a"))],
            vec!["renamed", "a"],
            original
                .iter()
                .map(|row| serde_json::json!({"renamed":row["z"],"a":row["a"]}))
                .collect(),
        ),
        (
            vec![("z", col("z"))],
            vec!["z"],
            original
                .iter()
                .map(|row| serde_json::json!({"z":row["z"]}))
                .collect(),
        ),
        (
            vec![("z", col("z")), ("a", col("z"))],
            vec!["z", "a"],
            original
                .iter()
                .map(|row| serde_json::json!({"z":row["z"],"a":row["z"]}))
                .collect(),
        ),
        (
            vec![
                (
                    "z",
                    binary(col("z"), BinaryOp::Add, lit(ScalarValue::Int64(0))),
                ),
                ("a", col("a")),
            ],
            vec!["z", "a"],
            original.clone(),
        ),
        (
            vec![
                (
                    "z",
                    expression(ExpressionKind::Cast {
                        expr: Box::new(col("z")),
                        target_dtype: shardloom_core::LogicalDType::Int64,
                    }),
                ),
                ("a", col("a")),
            ],
            vec!["z", "a"],
            original.clone(),
        ),
    ] {
        let prepared = prepare_relational(&project(fixture.scan(), expressions), policy()).unwrap();
        assert!(matches!(root(&prepared).kind, NodeKind::Project { .. }));
        assert_eq!(
            root(&prepared)
                .fields
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            names
        );
        let memory = prepared.session.memory().clone();
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected);
        drop((result, prepared));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_identity_projection_preserves_null_carrier_and_runtime_errors() {
    let fixture = fixture(3);
    let plan = project(
        fixture.scan(),
        vec![("z", lit(ScalarValue::Null)), ("a", col("a"))],
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert!(matches!(root(&prepared).kind, NodeKind::Project { .. }));
    assert_eq!(
        root(&prepared).fields[0].1,
        DType::Bool(Nullability::Nullable)
    );
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![
            serde_json::json!({"z":null,"a":"first"}),
            serde_json::json!({"z":null,"a":"second"}),
            serde_json::json!({"z":null,"a":null}),
        ]
    );
    drop((result, prepared));

    let plan = project(
        fixture.scan(),
        vec![
            (
                "z",
                binary(col("z"), BinaryOp::Divide, lit(ScalarValue::Int64(0))),
            ),
            ("a", col("a")),
        ],
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert!(matches!(root(&prepared).kind, NodeKind::Project { .. }));
    let memory = prepared.session.memory().clone();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .err()
            .unwrap()
            .to_string()
            .contains("division by zero")
    );
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_identity_projection_does_not_skip_invalid_binding() {
    let fixture = fixture(0);
    for expressions in [
        vec![("z", col("missing")), ("a", col("a"))],
        vec![("z", col("z")), ("z", col("a"))],
        vec![
            ("z", function("not_a_native_function", vec![col("z")])),
            ("a", col("a")),
        ],
    ] {
        assert!(prepare_relational(&project(fixture.scan(), expressions), policy()).is_err());
    }
}
