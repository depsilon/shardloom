use super::*;
use crate::local_primitives::prepared_unary::BoundUnary;
use crate::resident_session::NativeExecutionContext;
use vortex::array::{arrays::DictArray, memory::MemorySessionExt as _};

fn request_for(kind: Kind) -> VortexQueryPrimitiveRequest {
    if kind == Kind::PivotRows {
        decimal_pivot_tests::pivot("mean", true)
    } else {
        decimal_rolling_tests::rolling("mean", 3, 1, true)
    }
}

fn dtype() -> DType {
    DType::struct_(
        [
            ("group", DType::Utf8(Nullability::NonNullable)),
            ("domain", DType::Utf8(Nullability::NonNullable)),
            (
                "decimal",
                DType::Decimal(DecimalDType::new(38, 2), Nullability::NonNullable),
            ),
        ],
        Nullability::NonNullable,
    )
}

fn dictionary(context: &NativeExecutionContext<'_>) -> ArrayRef {
    let values = result_batch::build_column(
        &DType::Decimal(DecimalDType::new(38, 2), Nullability::NonNullable),
        131_076,
        &context.native_session().allocator(),
        |row| {
            Ok(result_batch::Value::Decimal(
                if row < 4 {
                    (row as i128 + 1) * 100
                } else {
                    10i128.pow(37)
                },
                DecimalDType::new(38, 2),
            ))
        },
    )
    .unwrap();
    StructArray::new(
        FieldNames::from(["group", "domain", "decimal"]),
        vec![
            VarBinArray::from(vec!["a", "a", "b", "b"]).into_array(),
            VarBinArray::from(vec!["x", "y", "x", "y"]).into_array(),
            DictArray::try_new(
                PrimitiveArray::from_iter([0u32, 1, 2, 3]).into_array(),
                values,
            )
            .unwrap()
            .into_array(),
        ],
        4,
        Validity::NonNullable,
    )
    .into_array()
}

fn deliver(
    bound: &BoundUnary,
    kind: Kind,
    context: &NativeExecutionContext<'_>,
    produce: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    if kind == Kind::PivotRows {
        let completed = bound.complete_relation_pivot(context, produce)?;
        completed.emit(bound, context, 1, consume)
    } else {
        bound
            .consume_relation(context, None, 1, produce, consume)
            .map(|_| ())
    }
}

#[test]
fn native_decimal_unary_dictionary_state_releases_source_and_retains_output_credit() {
    for kind in [Kind::PivotRows, Kind::RollingWindowRows] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let bound = BoundUnary::for_relation(&request_for(kind), &dtype(), &memory).unwrap();
        let mut output = Vec::new();
        let mut calls = 0;
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                deliver(
                    &bound,
                    kind,
                    context,
                    |consume| {
                        calls += 1;
                        let array = dictionary(context);
                        assert!(memory.snapshot().reserved_bytes > 2 << 20);
                        consume(array)?;
                        assert!(memory.snapshot().reserved_bytes < 1 << 20, "{kind:?}");
                        Ok(())
                    },
                    &mut |array| {
                        output.push(array);
                        Ok(())
                    },
                )
            })
            .unwrap();
        assert_eq!(calls, 1);
        assert_eq!(
            output.iter().map(ArrayRef::len).sum::<usize>(),
            if kind == Kind::PivotRows { 3 } else { 4 }
        );
        let retained = output[0].clone();
        let slice = retained.slice(0..1).unwrap();
        drop((output, bound, session, retained));
        assert!(memory.snapshot().reserved_bytes > 0);
        let name = if kind == Kind::PivotRows {
            "pivot_total"
        } else {
            "value"
        };
        let field = crate::local_primitives::logical_field_from_native_array(&slice, name).unwrap();
        let mut context = VortexSession::default().create_execution_ctx();
        assert_eq!(
            result_batch::scalar_value(&field, 0, &mut context)
                .unwrap()
                .into_json()
                .unwrap(),
            json!("decimal128(38,6):1500000")
        );
        drop((field, slice, context));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_decimal_unary_cancellation_consumer_failure_and_denied_grants_release_state() {
    for kind in [Kind::PivotRows, Kind::RollingWindowRows] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let bound = BoundUnary::for_relation(&request_for(kind), &dtype(), &memory).unwrap();
        let baseline = memory.snapshot().reserved_bytes;
        for cancel_in_producer in [false, true] {
            let token = CancellationToken::default();
            let error = session
                .with_native_execution_context(&token, |context| {
                    deliver(
                        &bound,
                        kind,
                        context,
                        |consume| {
                            consume(dictionary(context))?;
                            if cancel_in_producer {
                                token.cancel();
                            }
                            Ok(())
                        },
                        &mut |_| {
                            if cancel_in_producer {
                                Ok(())
                            } else {
                                Err(crate::local_primitives::vortex_error(
                                    "decimal consumer stopped",
                                ))
                            }
                        },
                    )
                })
                .unwrap_err();
            assert!(
                error.to_string().contains(if cancel_in_producer {
                    "cancel"
                } else {
                    "decimal consumer stopped"
                }),
                "{kind:?}: {error}"
            );
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
        }
        let blocked = memory
            .reserve(memory.snapshot().limit_bytes - baseline - 1024)
            .unwrap();
        let error = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                deliver(
                    &bound,
                    kind,
                    context,
                    |_| panic!("state grant must fail before source production"),
                    &mut |_| Ok(()),
                )
            })
            .unwrap_err();
        assert!(error.to_string().contains("reservation"), "{error}");
        drop(blocked);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        drop((bound, session));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_decimal_unary_unknown_cardinality_and_pivot_growth_obey_grants() {
    use crate::local_primitives::prepared_unary::prepare_unary_in_session;
    let values = vec![Some(100); 500];
    let keys = (0..500).map(|n| format!("{n:03}")).collect::<Vec<_>>();
    let fixture = decimal_pivot_tests::fixture(
        &keys.iter().map(String::as_str).collect::<Vec<_>>(),
        &vec!["x"; 500],
        &values,
        8,
        2,
        31,
    );
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let mut req = decimal_pivot_tests::pivot("sum", true);
    req.source_uri = Some(DatasetUri::new(fixture.path().display().to_string()).unwrap());
    let prepared = prepare_unary_in_session(&req, policy(), &session).unwrap();
    let baseline = session.snapshot().memory.reserved_bytes;
    for _ in 0..2 {
        let error = prepared.execute_owned().err().unwrap();
        assert!(error.to_string().contains("reservation"), "{error}");
        assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    }
    drop(prepared);
    let req = decimal_rolling_tests::rolling("sum", 20_000, 1, true);
    let bound = BoundUnary::for_relation(&req, &dtype(), session.memory()).unwrap();
    let baseline = session.snapshot().memory.reserved_bytes;
    let error = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            deliver(
                &bound,
                Kind::RollingWindowRows,
                context,
                |_| panic!("unknown cardinality window must reserve before production"),
                &mut |_| Ok(()),
            )
        })
        .unwrap_err();
    assert!(error.to_string().contains("reservation"), "{error}");
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
}
