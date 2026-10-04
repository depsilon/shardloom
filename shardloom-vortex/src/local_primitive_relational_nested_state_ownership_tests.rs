use super::*;
use crate::local_primitives::{prepared_unary::BoundUnary, result_batch};
use crate::query_primitive::{VortexDuplicateKeepPolicy as Keep, VortexQueryPrimitiveKind as Kind};
use std::borrow::Cow;
use vortex::array::memory::MemorySessionExt as _;

fn hidden_children_fixture() -> (ArrayRef, DType) {
    use vortex::array::arrays::ConstantArray;
    let hidden = ListViewArray::try_new(
        ConstantArray::new("unobserved", 1_000_000).into_array(),
        PrimitiveArray::from_iter([0u64, 0]).into_array(),
        PrimitiveArray::from_iter([1_000_000u64, 0]).into_array(),
        Validity::from_iter([false, true]),
    )
    .unwrap()
    .into_array();
    let alternative = ListViewArray::try_new(
        VarBinArray::from(vec!["kept"]).into_array(),
        PrimitiveArray::from_iter([0u64, 1]).into_array(),
        PrimitiveArray::from_iter([1u64, 0]).into_array(),
        Validity::AllValid,
    )
    .unwrap()
    .into_array();
    let dtype = hidden.dtype().clone();
    let input = StructArray::new(
        FieldNames::from(["hidden", "alternative"]),
        vec![hidden, alternative],
        2,
        Validity::NonNullable,
    )
    .into_array();
    (input, dtype)
}

#[test]
fn native_nested_parent_validity_does_not_prepare_hidden_child_keys() {
    use crate::local_primitives::{
        SimpleAggregateFunction,
        native_relational_aggregate::{Aggregate, Measure, Spec},
        native_relational_expression::{Expression as NativeExpression, Kind as NativeKind},
    };
    let (input, dtype) = hidden_children_fixture();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let column = |name: &str| NativeExpression {
                dtype: dtype.clone(),
                kind: NativeKind::Column(name.into()),
            };
            let mut execution = context.native_session().create_execution_ctx();
            for (op, expected) in [
                (shardloom_core::UnaryOp::IsNull, [true, false]),
                (shardloom_core::UnaryOp::IsNotNull, [false, true]),
            ] {
                let expression = NativeExpression {
                    dtype: DType::Bool(Nullability::NonNullable),
                    kind: NativeKind::Unary(op, Box::new(column("hidden"))),
                };
                let result = expression.evaluate(&input, context)?;
                for (row, value) in expected.into_iter().enumerate() {
                    assert_eq!(
                        result.execute_scalar(row, &mut execution).unwrap(),
                        value.into()
                    );
                }
            }
            let count_dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
            let spec = Spec {
                fields: vec![("n".into(), count_dtype.clone())],
                groups: vec![],
                group_names: vec![],
                measures: vec![Measure {
                    function: SimpleAggregateFunction::Count,
                    column: Some("hidden".into()),
                    dtype: count_dtype,
                    distinct_fields: vec![],
                    distinct_names: vec![],
                }],
            };
            let mut count = Aggregate::new(&spec, &memory)?;
            count.consume(&input, context, 2)?;
            count.finish(context, 2, &mut |array| {
                let value = crate::local_primitives::logical_field_from_native_array(&array, "n")?;
                assert_eq!(
                    value.execute_scalar(0, &mut execution).unwrap(),
                    1u64.into()
                );
                Ok(())
            })?;
            let coalesce = NativeExpression {
                dtype: dtype.clone(),
                kind: NativeKind::Coalesce(vec![column("hidden"), column("alternative")]),
            };
            let output = coalesce.evaluate(&input, context)?;
            assert_eq!(
                output
                    .execute_scalar(0, &mut execution)
                    .unwrap()
                    .as_list()
                    .elements()
                    .unwrap(),
                vec!["kept".into()]
            );
            assert_eq!(
                output
                    .execute_scalar(1, &mut execution)
                    .unwrap()
                    .as_list()
                    .elements()
                    .unwrap(),
                [] as [vortex::array::scalar::Scalar; 0]
            );
            Ok(())
        })
        .unwrap();
    drop((input, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn large_nested(context: &crate::resident_session::NativeExecutionContext<'_>) -> ArrayRef {
    let huge = vec![255u8; 2 << 20];
    let values = result_batch::build_column(
        &DType::Binary(Nullability::NonNullable),
        2,
        &context.native_session().allocator(),
        |row| {
            Ok(result_batch::Value::Binary(Cow::Borrowed(if row == 0 {
                &huge
            } else {
                b"\x00\xfe"
            })))
        },
    )
    .unwrap();
    let lists = ListViewArray::try_new(
        values,
        PrimitiveArray::from_iter([1u64, 1, 1]).into_array(),
        PrimitiveArray::from_iter([1u64, 1, 1]).into_array(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    StructArray::new(
        FieldNames::from(["payload"]),
        vec![lists],
        3,
        Validity::NonNullable,
    )
    .into_array()
}

#[test]
fn native_nested_state_releases_unselected_children_before_completion_and_keeps_output_credits() {
    for kind in [
        Kind::TailRows,
        Kind::SampleRows,
        Kind::DropDuplicateRows,
        Kind::DistinctRows,
    ] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let projection = shardloom_plan::ProjectionRequest::columns(vec![column("payload")]);
        let mut request =
            VortexQueryPrimitiveRequest::for_relational_input(kind, projection.clone());
        if kind == Kind::DropDuplicateRows {
            request.duplicate_keep = Keep::Last;
            request.deduplicate_key_projection = Some(projection);
        } else if matches!(kind, Kind::TailRows | Kind::SampleRows) {
            request.source_order_limit = Some(1);
        }
        request.sample_seed = (kind == Kind::SampleRows).then_some(7);
        let dtype = DType::struct_(
            [(
                "payload",
                DType::List(
                    std::sync::Arc::new(DType::Binary(Nullability::NonNullable)),
                    Nullability::NonNullable,
                ),
            )],
            Nullability::NonNullable,
        );
        let bound = BoundUnary::for_relation(&request, &dtype, &memory).unwrap();
        let mut output = Vec::new();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                bound.consume_relation(
                    context,
                    None,
                    1,
                    |consume| {
                        let input = large_nested(context);
                        assert!(memory.snapshot().reserved_bytes >= 2 << 20);
                        consume(input)?;
                        assert!(
                            memory.snapshot().reserved_bytes < 1 << 20,
                            "{kind:?} retained the unselected native child domain"
                        );
                        Ok(())
                    },
                    &mut |array| {
                        output.push(array);
                        Ok(())
                    },
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(output.iter().map(ArrayRef::len).sum::<usize>(), 1);
        assert!(output.iter().map(ArrayRef::nbytes).sum::<u64>() < 64 << 10);
        let clone = output[0].clone();
        let slice = clone.slice(0..1).unwrap();
        drop((output, bound, session));
        assert!(memory.snapshot().reserved_bytes > 0);
        let field =
            crate::local_primitives::logical_field_from_native_array(&slice, "payload").unwrap();
        let mut execution = VortexSession::default().create_execution_ctx();
        assert_eq!(
            field
                .execute_scalar(0, &mut execution)
                .unwrap()
                .as_list()
                .elements()
                .unwrap(),
            vec![vortex::array::scalar::Scalar::from(&b"\x00\xfe"[..])]
        );
        drop((field, clone));
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(slice);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_nested_state_failure_cancellation_and_source_replacement_release_every_owner() {
    use crate::query_primitive::{
        VortexExpressionProjectionRequest, VortexExpressionRewrite as Rewrite,
    };
    use crate::relational_query::VortexRelationalUnary;
    let fixture = fixture();
    let mut request = VortexQueryPrimitiveRequest::for_relational_input(
        Kind::ExpressionProjectRows,
        shardloom_plan::ProjectionRequest::All,
    );
    request.expression_projection = Some(VortexExpressionProjectionRequest::new(vec![
        Rewrite::ForwardFillNull {
            target_column: column("items"),
            limit: None,
        },
    ]));
    let plan = VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary {
        input: fixture.scan(),
        request,
    }));
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    let blocker = memory
        .reserve(memory.snapshot().limit_bytes - baseline - 1024)
        .unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    drop(blocker);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    for cancel in [false, true] {
        let token = CancellationToken::default();
        let mut calls = 0;
        let error = prepared
            .for_each_batch(&token, |_, _| {
                calls += 1;
                if cancel {
                    token.cancel();
                    Ok(())
                } else {
                    Err(failed("nested consumer stopped"))
                }
            })
            .err()
            .unwrap();
        assert!(
            error.to_string().contains(if cancel {
                "cancel"
            } else {
                "nested consumer stopped"
            }),
            "{error}"
        );
        assert_eq!(calls, 1);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
    }
    let result = prepared.execute_owned().unwrap();
    let retained = result.result.arrays()[0].clone();
    drop(result);
    fixture.replace();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
    drop(prepared);
    assert!(memory.snapshot().reserved_bytes > 0);
    let mut execution = VortexSession::default().create_execution_ctx();
    let field =
        crate::local_primitives::logical_field_from_native_array(&retained, "items").unwrap();
    assert_eq!(
        field
            .execute_scalar(0, &mut execution)
            .unwrap()
            .as_list()
            .elements()
            .unwrap(),
        vec![
            9i64.into(),
            vortex::array::scalar::Scalar::null(DType::Primitive(
                PType::I64,
                Nullability::Nullable
            ))
        ]
    );
    drop((field, retained));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
