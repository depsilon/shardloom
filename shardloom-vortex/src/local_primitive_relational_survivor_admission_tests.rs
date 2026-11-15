//! Admission counterexamples and work witnesses, never a production strategy.

use super::*;
use crate::{
    local_primitives::{
        native_payload,
        native_relational_batch::Batch,
        native_relational_expression::Kind,
        native_relational_keys::Cell,
        native_relational_observation::{Session, record as observe},
        native_relational_output_buffer::OutputBuffer,
        result_batch,
    },
    resident_session::ResidentVortexSession,
};
use shardloom_core::BinaryOp;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    IntoArray as _,
    arrays::ChunkedArray,
    dtype::{DType, Nullability, PType},
    memory::HostMemorySessionExt as _,
    scalar::Scalar,
};

fn dtype() -> DType {
    DType::Primitive(PType::I64, Nullability::NonNullable)
}

fn record(rows: usize, value: i64, context: &NativeExecutionContext<'_>) -> Result<ArrayRef> {
    let column = result_batch::build_column(
        &dtype(),
        rows,
        &context.native_session().host_allocator(),
        |_| Ok(result_batch::Value::Int(value)),
    )?;
    StructArray::try_new(
        FieldNames::from(["n"]),
        vec![column],
        rows,
        Validity::NonNullable,
    )
    .map(vortex::array::IntoArray::into_array)
    .map_err(vortex_error)
}

fn column() -> native_relational_expression::Expression {
    native_relational_expression::Expression::new(dtype(), Kind::Column("n".into()))
}

fn arithmetic(op: BinaryOp, right: i64) -> native_relational_expression::Expression {
    native_relational_expression::Expression::new(
        dtype(),
        Kind::Binary(
            Box::new(column()),
            op,
            Box::new(native_relational_expression::Expression::new(
                dtype(),
                Kind::Literal(Scalar::from(right)),
            )),
        ),
    )
}

fn emit(record: &serde_json::Value) {
    println!("SURVIVOR_ADMISSION {record}");
}

#[test]
fn survivor_admission_compaction_changes_projection_failure_order() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            // For n=1 the second output overflows; for n=-1 the first output
            // divides by zero. Batch-major and expression-major visits differ.
            let divisor = arithmetic(BinaryOp::Add, 1);
            let divide = native_relational_expression::Expression::new(
                dtype(),
                Kind::Binary(Box::new(column()), BinaryOp::Divide, Box::new(divisor)),
            );
            let expressions = [divide, arithmetic(BinaryOp::Add, i64::MAX)];
            let fields = [("division".into(), dtype()), ("overflow".into(), dtype())];
            let direct = project_array(&fields, &expressions, &record(1, 1, context)?, context)
                .err()
                .unwrap()
                .to_string();
            assert!(
                direct.contains("signed integer arithmetic overflow"),
                "{direct}"
            );
            let mut buffer = OutputBuffer::new(
                record(0, 0, context)?.dtype().clone(),
                8,
                native_payload::CopyPolicy::ValidateValues,
                context,
            )?;
            for value in [1, -1] {
                buffer.push(record(1, value, context)?, context, &mut |_| {
                    panic!("unexpected flush")
                })?;
            }
            let compact = buffer
                .flush(context, &mut |array| {
                    project_array(&fields, &expressions, &array, context).map(|_| ())
                })
                .err()
                .unwrap()
                .to_string();
            assert!(compact.contains("division by zero"), "{compact}");
            assert_ne!(direct, compact);
            emit(
                &serde_json::json!({"case":"projection_failure_order", "direct":direct,
            "compacted":compact, "candidate_preserves_error":false}),
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn survivor_admission_flushing_after_source_error_cannot_undo_extra_callbacks() {
    let mut observed = Vec::new();
    for buffered in [false, true] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let (calls, error) = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut calls = 0;
                let mut buffer = OutputBuffer::new(
                    record(0, 0, context)?.dtype().clone(),
                    8,
                    native_payload::CopyPolicy::ValidateValues,
                    context,
                )?;
                let mut consume = |array: ArrayRef| {
                    let expression = arithmetic(BinaryOp::Divide, 0);
                    expression.evaluate(&array, context).map(|_| ())
                };
                let produce = (|| {
                    for index in 0..2 {
                        calls += 1;
                        if index == 1 {
                            return Err(failed("later source callback failure"));
                        }
                        let array = record(1, 1, context)?;
                        if buffered {
                            buffer.push(array, context, &mut consume)?;
                        } else {
                            consume(array)?;
                        }
                    }
                    Ok(())
                })();
                // Give the repair its strongest case: pending earlier work gets
                // a chance to fail before the later producer error is propagated.
                let result = if buffered {
                    buffer.flush(context, &mut consume).and(produce)
                } else {
                    produce
                };
                Ok((calls, result.err().unwrap().to_string()))
            })
            .unwrap();
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        observed.push((calls, error));
    }
    assert_eq!(observed[0].1, observed[1].1);
    assert_eq!((observed[0].0, observed[1].0), (1, 2));
    emit(
        &serde_json::json!({"case":"source_callback_order", "direct_calls":observed[0].0,
        "flush_on_error_calls":observed[1].0, "same_final_error":true,
        "direct_error":observed[0].1, "flush_on_error_error":observed[1].1,
        "candidate_preserves_callbacks":false}),
    );
}

#[test]
fn survivor_admission_actual_key_preparation_requires_one_real_consumer_batch() {
    let mut reports = Vec::new();
    for strategy in ["direct", "list_forward", "chunked", "physical_compaction"] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let chunks = (0..16)
                    .map(|value| record(1, value, context))
                    .collect::<Result<Vec<_>>>()?;
                let dtype = chunks[0].dtype().clone();
                let observer = Session::start();
                let mut actual = Vec::new();
                let mut invocations = 0;
                let mut consumer_preparations = 0;
                let mut consume = |array: ArrayRef| {
                    invocations += 1;
                    let mut before = 0;
                    observe(|counts| before = counts.key_columns);
                    let batch = Batch::with_names(array, ["n"], context)?;
                    observe(|counts| consumer_preparations += counts.key_columns - before);
                    for row in 0..batch.array.len() {
                        actual.push(batch.raw_cell(row, 0)?);
                    }
                    Ok(())
                };
                match strategy {
                    "direct" | "list_forward" => {
                        // Retaining an array list changes no preparation boundary.
                        for array in chunks {
                            consume(array)?;
                        }
                    }
                    "chunked" => consume(
                        ChunkedArray::try_new(chunks, dtype)
                            .map_err(vortex_error)?
                            .into_array(),
                    )?,
                    "physical_compaction" => {
                        let mut buffer = OutputBuffer::new(
                            dtype,
                            64,
                            native_payload::CopyPolicy::ValidateValues,
                            context,
                        )?;
                        for array in chunks {
                            buffer.push(array, context, &mut consume)?;
                        }
                        buffer.flush(context, &mut consume)?;
                    }
                    _ => unreachable!(),
                }
                assert_eq!(
                    actual,
                    (0..16).map(Cell::NonnegativeInteger).collect::<Vec<_>>()
                );
                let counts = observer.finish();
                match strategy {
                    "direct" | "list_forward" => assert_eq!(invocations, 16),
                    "chunked" => assert_eq!(invocations, 1),
                    // OutputBuffer also charges per-fragment schema estimates
                    // against its byte ceiling. It need not emit one batch.
                    "physical_compaction" => assert!((1..16).contains(&invocations)),
                    _ => unreachable!(),
                }
                assert_eq!(consumer_preparations, invocations);
                assert!(counts.key_columns >= consumer_preparations);
                if matches!(strategy, "chunked" | "physical_compaction") {
                    assert!(counts.key_columns < 16);
                }
                reports.push(
                    serde_json::json!({"strategy":strategy, "consumer_invocations":invocations,
                "consumer_key_preparations":consumer_preparations, "all_key_preparations":counts.key_columns,
                "compaction_key_preparations":counts.key_columns - consumer_preparations,
                "ordered_values":actual.iter().map(|value| match value {
                    Cell::NonnegativeInteger(value) => *value, _ => unreachable!(),
                }).collect::<Vec<_>>(),
                "complete_ordered_values_match":true}),
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
    emit(
        &serde_json::json!({"case":"preparation_boundary", "observations":reports,
        "engine_timing":null}),
    );
}

#[test]
fn survivor_admission_small_views_keep_full_credited_sources_live() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    session.with_native_execution_context(&CancellationToken::default(), |context| {
        let mut buffer = OutputBuffer::new(record(0, 0, context)?.dtype().clone(), 64,
            native_payload::CopyPolicy::ValidateValues, context)?;
        let initial = context.memory().snapshot().reserved_bytes;
        let mut visible = 0;
        for value in 0..8 {
            let source = record(32_768, value, context)?;
            let selected = source.slice(0..1).map_err(vortex_error)?;
            visible += selected.nbytes();
            drop(source);
            buffer.push(selected, context, &mut |_| panic!("unexpected early flush"))?;
        }
        let pinned = context.memory().snapshot().reserved_bytes - initial;
        assert_eq!(visible, 8 * 8);
        assert!(pinned >= 8 * 32_768 * 8);
        let mut rows = 0;
        buffer.flush(context, &mut |array| { rows += array.len(); Ok(()) })?;
        assert_eq!(rows, 8);
        drop(buffer);
        // The operation's encoded-compare observer retains its initial credit
        // until the context ends. It is not a pinned survivor allocation.
        assert_eq!(context.memory().snapshot().reserved_bytes, initial);
        emit(&serde_json::json!({"case":"pinned_source_allocations", "visible_slice_bytes":visible,
            "live_credit_increase_bytes":pinned, "original_payload_bytes":8 * 32_768 * 8,
            "complete_rows":rows, "released_after_flush":true,
            "operation_context_credit_bytes":initial,
            "generic_backing_size_api":null}));
        Ok(())
    }).unwrap();
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn survivor_admission_pending_slices_release_after_consumer_error_and_cancellation() {
    for cancel in [false, true] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let token = CancellationToken::default();
        let result = session.with_native_execution_context(&token, |context| {
            let mut buffer = OutputBuffer::new(
                record(0, 0, context)?.dtype().clone(),
                64,
                native_payload::CopyPolicy::ValidateValues,
                context,
            )?;
            for value in 0..4 {
                let source = record(8192, value, context)?;
                buffer.push(
                    source.slice(0..1).map_err(vortex_error)?,
                    context,
                    &mut |_| panic!("unexpected early flush"),
                )?;
            }
            if cancel {
                token.cancel();
            }
            buffer.flush(context, &mut |_| {
                Err(failed("consumer refuses pending records"))
            })
        });
        assert!(result.is_err());
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
    emit(
        &serde_json::json!({"case":"pending_failure_cleanup", "consumer_error":true,
        "cancellation":true, "remaining_credits_bytes":0}),
    );
}
