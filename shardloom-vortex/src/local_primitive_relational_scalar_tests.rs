use super::*;
use crate::relational_query::{
    VortexRelationalProject, VortexRelationalSubquery, VortexRelationalSubqueryKind,
};
use shardloom_core::{ExprId, Expression, ScalarValue};
use vortex::array::arrays::{
    DecimalArray, ExtensionArray, FixedSizeListArray, ListViewArray, VarBinArray,
};
use vortex::array::dtype::DecimalDType;
use vortex::array::extension::datetime::{Date, TimeUnit, Timestamp};

fn scalar(
    input: VortexRelationalPlan,
    relation: VortexRelationalPlan,
    correlated: bool,
    guard: Option<Expression>,
) -> VortexRelationalPlan {
    let query = Box::new(VortexRelationalSubquery {
        input,
        relation,
        kind: VortexRelationalSubqueryKind::Scalar,
        correlation: vec![],
        output_column: "scalar".into(),
        evaluation_guard: guard,
        negated: false,
    });
    if correlated {
        VortexRelationalPlan::CorrelatedSubquery(query)
    } else {
        VortexRelationalPlan::Subquery(query)
    }
}

fn column(name: &str) -> Expression {
    Expression::column(ExprId::new(name).unwrap(), ColumnRef::new(name).unwrap())
}

fn scalar_values() -> Vec<ArrayRef> {
    let list = ListViewArray::try_new(
        PrimitiveArray::from_option_iter([Some(7i64), None]).into_array(),
        PrimitiveArray::from_iter([0u64]).into_array(),
        PrimitiveArray::from_iter([2u64]).into_array(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    vec![
        PrimitiveArray::from_iter([u64::MAX]).into_array(),
        PrimitiveArray::from_iter([i64::MIN]).into_array(),
        PrimitiveArray::from_option_iter([None::<i16>]).into_array(),
        DecimalArray::from_option_iter(
            [Some(123_456_789_012_345_678_901_234_567_890_i128)],
            DecimalDType::new(34, 6),
        )
        .into_array(),
        VarBinArray::from(vec!["東京"]).into_array(),
        VarBinArray::from(vec![&b"\0\xff\x01"[..]]).into_array(),
        ExtensionArray::new(
            Date::new(TimeUnit::Days, Nullability::NonNullable).erased(),
            PrimitiveArray::from_iter([-1i32]).into_array(),
        )
        .into_array(),
        ExtensionArray::new(
            Timestamp::new(TimeUnit::Microseconds, Nullability::NonNullable).erased(),
            PrimitiveArray::from_iter([-1_234_567i64]).into_array(),
        )
        .into_array(),
        FixedSizeListArray::try_new(
            PrimitiveArray::from_option_iter([Some(7i64), None]).into_array(),
            2,
            Validity::NonNullable,
            1,
        )
        .unwrap()
        .into_array(),
        list,
        single(
            "child",
            PrimitiveArray::from_option_iter([Some(7i16)]).into_array(),
        ),
    ]
}

#[test]
fn native_scalar_subquery_keeps_exact_nullable_dtype_for_empty_and_one_row_values() {
    let outer = Fixture::new(
        single("id", PrimitiveArray::from_iter([1u32, 2]).into_array()),
        1,
    );
    for value in scalar_values() {
        for empty in [false, true] {
            let input = if empty {
                value.slice(0..0).unwrap()
            } else {
                value.clone()
            };
            let inner = Fixture::new(single("value", input), 1);
            let prepared =
                prepare_relational(&scalar(outer.scan(), inner.scan(), false, None), policy())
                    .unwrap();
            let memory = prepared.session.memory().clone();
            let baseline = memory.snapshot().reserved_bytes;
            let mut retained = Vec::new();
            prepared
                .for_each_batch(&CancellationToken::default(), |batch, _| {
                    let result =
                        crate::local_primitives::logical_field_from_native_array(&batch, "scalar")?;
                    assert_eq!(result.dtype(), &value.dtype().as_nullable());
                    retained.push(result);
                    Ok(())
                })
                .unwrap();
            assert_eq!(retained.iter().map(ArrayRef::len).sum::<usize>(), 2);
            let session = VortexSession::default();
            let mut execution = session.create_execution_ctx();
            for array in &retained {
                for row in 0..array.len() {
                    let actual = array.execute_scalar(row, &mut execution).unwrap();
                    if empty {
                        assert!(actual.is_null());
                    } else {
                        assert_eq!(
                            actual,
                            value
                                .execute_scalar(0, &mut execution)
                                .unwrap()
                                .cast(&value.dtype().as_nullable())
                                .unwrap()
                        );
                    }
                }
            }
            assert!(memory.snapshot().reserved_bytes > baseline);
            drop(prepared);
            assert!(memory.snapshot().reserved_bytes > 0);
            drop(retained);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_scalar_subquery_binds_arity_guard_and_static_schema_for_unused_empty_input() {
    let outer = Fixture::new(
        single(
            "id",
            PrimitiveArray::from_iter(Vec::<i64>::new()).into_array(),
        ),
        1,
    );
    let inner = Fixture::new(
        single("value", PrimitiveArray::from_iter([7i64]).into_array()),
        1,
    );
    let false_guard =
        || Expression::literal(ExprId::new("guard").unwrap(), ScalarValue::Boolean(false));
    let wide = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: inner.scan(),
        expressions: vec![
            ("first".into(), column("value")),
            ("second".into(), column("value")),
        ],
    }));
    for (relation, guard, message) in [
        (wide, false_guard(), "exactly one output column"),
        (
            inner.scan(),
            Expression::literal(ExprId::new("guard").unwrap(), ScalarValue::Int64(1)),
            "Boolean expression",
        ),
    ] {
        let error = prepare_relational(
            &scalar(outer.scan(), relation, false, Some(guard)),
            policy(),
        )
        .err()
        .expect("unused scalar declarations still require admission");
        assert!(error.to_string().contains(message), "{error}");
    }
    let outer_source = outer.scan();
    let prepared = prepare_relational_with_dynamic_schema(
        &[DatasetUri::new(outer.path().to_string_lossy()).unwrap()],
        policy(),
        65_536,
        move |schemas| {
            let relation = schemas.defer_subquery(65_536, |_| {
                panic!("rejected scalar schema must never execute")
            })?;
            Ok(scalar(
                outer_source.clone(),
                relation,
                true,
                Some(false_guard()),
            ))
        },
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let error = prepared
        .collect_jsonl(&CancellationToken::default())
        .err()
        .expect("deferred scalar schemas require explicit admission");
    assert!(
        error.to_string().contains("statically bound output schema"),
        "{error}"
    );
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(prepared.snapshot().completed_executions, 0);
}

#[test]
fn native_scalar_subquery_cardinality_counts_duplicate_rows_across_batches_and_releases_state() {
    let outer = Fixture::new(
        single("id", PrimitiveArray::from_iter([1u32, 2]).into_array()),
        1,
    );
    for values in [vec![7i64, 7], vec![7, 8, 9]] {
        for chunk_rows in [1, 3] {
            let inner = Fixture::new(
                single(
                    "value",
                    PrimitiveArray::from_iter(values.iter().copied()).into_array(),
                ),
                chunk_rows,
            );
            let prepared =
                prepare_relational(&scalar(outer.scan(), inner.scan(), false, None), policy())
                    .unwrap();
            let baseline = prepared.snapshot().memory.reserved_bytes;
            for _ in 0..2 {
                let error = prepared
                    .collect_jsonl(&CancellationToken::default())
                    .err()
                    .unwrap();
                assert!(
                    error.to_string().contains("scalar subquery cardinality"),
                    "{error}"
                );
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                assert_eq!(prepared.snapshot().completed_executions, 0);
            }
        }
    }
    let inner = Fixture::new(
        single("value", PrimitiveArray::from_iter([7i64, 8]).into_array()),
        1,
    );
    let false_guard =
        Expression::literal(ExprId::new("guard").unwrap(), ScalarValue::Boolean(false));
    let prepared = prepare_relational(
        &scalar(outer.scan(), inner.scan(), false, Some(false_guard)),
        policy(),
    )
    .unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        [
            serde_json::json!({"id":1,"scalar":null}),
            serde_json::json!({"id":2,"scalar":null})
        ]
    );
}

#[test]
fn native_scalar_subquery_rejects_duplicate_values_in_separate_union_batches() {
    use crate::relational_query::VortexRelationalSetKind;
    let input = Fixture::new(
        single("value", PrimitiveArray::from_iter([7i64]).into_array()),
        1,
    );
    let relation = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: input.scan(),
        right: input.scan(),
        kind: VortexRelationalSetKind::UnionAll,
    }));
    let mut batches = Vec::new();
    prepare_relational(&relation, policy())
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |array, _| {
            batches.push(array.len());
            Ok(())
        })
        .unwrap();
    assert_eq!(batches, [1, 1]);
    let prepared =
        prepare_relational(&scalar(input.scan(), relation, false, None), policy()).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let error = prepared
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("scalar subquery cardinality"),
        "{error}"
    );
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(prepared.snapshot().completed_executions, 0);
}

#[test]
fn native_scalar_subquery_first_demand_is_once_per_execution_and_correlation_is_per_row() {
    use vortex::array::arrays::BoolArray;
    let count = BATCH_ROWS + 2;
    let input = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "selected"]),
            vec![
                PrimitiveArray::from_iter(vec![2i64; count]).into_array(),
                BoolArray::from_iter((0..count).map(|row| Some(row >= BATCH_ROWS))).into_array(),
            ],
            count,
            Validity::NonNullable,
        )
        .into_array(),
        BATCH_ROWS,
    );
    let inner = Fixture::new(
        single("value", PrimitiveArray::from_iter([7i64]).into_array()),
        1,
    );
    for (correlated, selected, inner_reads) in
        [(false, false, 0), (false, true, 1), (true, true, 2)]
    {
        let guard = if selected {
            column("selected")
        } else {
            Expression::literal(ExprId::new("guard").unwrap(), ScalarValue::Boolean(false))
        };
        let prepared = prepare_relational(
            &scalar(input.scan(), inner.scan(), correlated, Some(guard)),
            policy(),
        )
        .unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for execution in 1..=2 {
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let rows = json_rows(&result);
            assert_eq!(rows.len(), count);
            assert!(rows[..BATCH_ROWS].iter().all(|row| row["scalar"].is_null()));
            for row in &rows[BATCH_ROWS..] {
                assert_eq!(
                    row["scalar"],
                    if selected {
                        serde_json::json!(7)
                    } else {
                        serde_json::Value::Null
                    }
                );
            }
            assert_eq!(
                result.execution.scan_rows_delivered,
                (count + inner_reads) as u64
            );
            assert_eq!(prepared.snapshot().completed_executions, execution);
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

#[test]
fn native_scalar_subquery_fresh_parameters_recover_after_cancellation_and_failed_consumer() {
    let input = Fixture::new(
        single(
            "id",
            PrimitiveArray::from_option_iter([Some(2i64), Some(2), None, Some(3)]).into_array(),
        ),
        1,
    );
    let relation = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: VortexRelationalPlan::Outer,
        expressions: vec![("value".into(), column("id"))],
    }));
    let prepared =
        prepare_relational(&scalar(input.scan(), relation, true, None), policy()).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for fail in [false, true] {
        let token = CancellationToken::default();
        let result = prepared.for_each_batch(&token, |_, _| {
            if fail {
                return Err(failed("intentional scalar consumer failure"));
            }
            token.cancel();
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(prepared.snapshot().completed_executions, 0);
    }
    for execution in 1..=2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            [
                serde_json::json!({"id":2,"scalar":2}),
                serde_json::json!({"id":2,"scalar":2}),
                serde_json::json!({"id":null,"scalar":null}),
                serde_json::json!({"id":3,"scalar":3})
            ]
        );
        assert_eq!(prepared.snapshot().completed_executions, execution);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    input.replace();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn native_scalar_subquery_nullable_guard_skips_value_errors_and_recovers_after_memory_denial() {
    use shardloom_core::{BinaryOp, ExpressionKind};
    use vortex::array::arrays::BoolArray;
    let input = Fixture::new(
        StructArray::try_new(
            FieldNames::from(["value", "selected"]),
            vec![
                PrimitiveArray::from_iter([0i64, 0, 2, 4]).into_array(),
                BoolArray::from_iter([Some(false), None, Some(true), Some(true)]).into_array(),
            ],
            4,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        2,
    );
    let divide = Expression::new(
        ExprId::new("divide").unwrap(),
        ExpressionKind::Binary {
            left: Box::new(Expression::literal(
                ExprId::new("numerator").unwrap(),
                ScalarValue::Int64(8),
            )),
            op: BinaryOp::Divide,
            right: Box::new(column("value")),
        },
    );
    let relation = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: VortexRelationalPlan::Outer,
        expressions: vec![("result".into(), divide)],
    }));
    let prepared = prepare_relational(
        &scalar(input.scan(), relation, true, Some(column("selected"))),
        policy(),
    )
    .unwrap();
    let memory = prepared.session.memory();
    let baseline = memory.snapshot();
    let occupied = memory
        .reserve(baseline.limit_bytes - baseline.reserved_bytes)
        .unwrap();
    let error = prepared
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap();
    assert!(error.to_string().contains("memory"), "{error}");
    assert_eq!(memory.snapshot().reserved_bytes, baseline.limit_bytes);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    drop(occupied);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        [
            serde_json::json!({"value":0,"selected":false,"scalar":null}),
            serde_json::json!({"value":0,"selected":null,"scalar":null}),
            serde_json::json!({"value":2,"selected":true,"scalar":4}),
            serde_json::json!({"value":4,"selected":true,"scalar":2}),
        ]
    );
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, baseline.reserved_bytes);
}

fn late_cardinality_plan(input: &Fixture) -> VortexRelationalPlan {
    use crate::relational_query::{VortexRelationalFilter, VortexRelationalSetKind};
    use shardloom_core::{ComparisonOp, ExpressionKind};
    let first = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: VortexRelationalPlan::Outer,
        expressions: vec![("value".into(), column("id"))],
    }));
    let second = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: first.clone(),
        predicate: Expression::new(
            ExprId::new("second").unwrap(),
            ExpressionKind::Compare {
                left: Box::new(column("value")),
                op: ComparisonOp::Gt,
                right: Box::new(Expression::literal(
                    ExprId::new("one").unwrap(),
                    ScalarValue::Int64(1),
                )),
            },
        ),
    }));
    scalar(
        input.scan(),
        VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
            left: first,
            right: second,
            kind: VortexRelationalSetKind::UnionAll,
        })),
        true,
        None,
    )
}

#[test]
fn native_scalar_subquery_late_cardinality_error_keeps_provisional_rows_unsuccessful() {
    let input = Fixture::new(
        single(
            "id",
            PrimitiveArray::from_iter(std::iter::repeat_n(1i64, BATCH_ROWS).chain([2]))
                .into_array(),
        ),
        BATCH_ROWS,
    );
    let prepared = prepare_relational(&late_cardinality_plan(&input), policy()).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut provisional = 0;
    let error = prepared
        .for_each_batch(&CancellationToken::default(), |array, _| {
            provisional += array.len();
            Ok(())
        })
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("scalar subquery cardinality"),
        "{error}"
    );
    assert_eq!(provisional, BATCH_ROWS);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn native_scalar_subquery_all_writers_read_back_typed_nulls_and_clean_late_failures() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    let input = Fixture::new(
        single("id", PrimitiveArray::from_iter([1i64, 2]).into_array()),
        1,
    );
    for (values, expected, csv, label) in [
        (
            vec![7i64],
            vec![
                serde_json::json!({"id":1,"scalar":7}),
                serde_json::json!({"id":2,"scalar":7}),
            ],
            "id,scalar\n1,7\n2,7\n",
            "one",
        ),
        (
            vec![],
            vec![
                serde_json::json!({"id":1,"scalar":null}),
                serde_json::json!({"id":2,"scalar":null}),
            ],
            "id,scalar\n1,\n2,\n",
            "empty",
        ),
    ] {
        let inner = Fixture::new(
            single("value", PrimitiveArray::from_iter(values).into_array()),
            1,
        );
        super::writer_tests::verify_writers(
            &input,
            &scalar(input.scan(), inner.scan(), false, None),
            label,
            &expected,
            csv,
        );
    }
    let late = Fixture::new(
        single(
            "id",
            PrimitiveArray::from_iter(std::iter::repeat_n(1i64, BATCH_ROWS).chain([2]))
                .into_array(),
        ),
        BATCH_ROWS,
    );
    let prepared = prepare_relational(&late_cardinality_plan(&late), policy()).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let before = fs::read_dir(&late.0).unwrap().count();
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
        let path = late.0.join(format!("failed.{}", format.as_str()));
        let error = prepared.write(&path, format, false).err().unwrap();
        assert!(
            error.to_string().contains("scalar subquery cardinality"),
            "{format:?}: {error}"
        );
        assert!(!path.exists(), "{format:?}");
        assert_eq!(fs::read_dir(&late.0).unwrap().count(), before);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}
