use super::*;
use crate::local_primitives::prepared_unary::BoundUnary;
use std::borrow::Cow;
use vortex::array::{
    arrays::{DictArray, FilterArray, MaskedArray, SliceArray},
    memory::MemorySessionExt as _,
};

#[test]
fn typed_unary_lazy_field_wrappers_never_evaluate_unselected_struct_scalars() {
    let source = source_array();
    let projection = vortex::expr::get_item("bytes", vortex::expr::root())
        .bind(source.dtype())
        .unwrap();
    let lazy = source.clone().apply_bound(&projection).unwrap();
    let wrappers = [
        (
            SliceArray::new(lazy.clone(), 1..5).into_array(),
            vec![json!(""), json!("00ff"), Value::Null, json!("")],
        ),
        (
            FilterArray::new(
                lazy,
                vortex::mask::Mask::from_iter([true, false, true, false, false, true]),
            )
            .into_array(),
            vec![json!("00ff"), json!("00ff"), json!("0a00")],
        ),
        (
            MaskedArray::try_new(
                source
                    .slice(0..3)
                    .unwrap()
                    .apply_bound(&projection)
                    .unwrap(),
                Validity::from_iter([true, false, true]),
            )
            .unwrap()
            .into_array(),
            vec![json!("00ff"), Value::Null, json!("00ff")],
        ),
    ];
    for (array, expected) in wrappers {
        let name = array.encoding_id();
        let input = StructArray::new(
            FieldNames::from(["payload"]),
            vec![array.clone()],
            array.len(),
            Validity::NonNullable,
        )
        .into_array();
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let mut req = request(Kind::TailRows, &["payload"]);
        req.source_order_limit = Some(array.len());
        let bound = BoundUnary::for_relation(&req, input.dtype(), session.memory()).unwrap();
        let mut actual = Vec::new();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut execution = context.native_session().create_execution_ctx();
                bound
                    .consume_relation(context, None, 2, |consume| consume(input), &mut |array| {
                        let column = crate::local_primitives::logical_field_from_native_array(
                            &array, "payload",
                        )?;
                        for row in 0..array.len() {
                            actual.push(
                                result_batch::scalar_value(&column, row, &mut execution)?
                                    .into_json()?,
                            );
                        }
                        Ok(())
                    })
                    .map(|_| ())
            })
            .unwrap();
        assert_eq!(actual, expected, "{name}");
    }
}

fn large_dictionary(context: &crate::resident_session::NativeExecutionContext<'_>) -> ArrayRef {
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
    let selected = DictArray::try_new(PrimitiveArray::from_iter([1u32, 1, 1]).into_array(), values)
        .unwrap()
        .into_array();
    StructArray::new(
        FieldNames::from(["payload"]),
        vec![selected],
        3,
        Validity::NonNullable,
    )
    .into_array()
}

#[test]
fn typed_unary_retained_state_releases_unselected_native_dictionary_before_completion() {
    for kind in [Kind::TailRows, Kind::SampleRows, Kind::DropDuplicateRows] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let mut req = request(kind, &["payload"]);
        if kind == Kind::DropDuplicateRows {
            req.duplicate_keep = Keep::Last;
            req.deduplicate_key_projection = Some(columns(&["payload"]));
        } else {
            req.source_order_limit = Some(1);
        }
        req.sample_seed = (kind == Kind::SampleRows).then_some(7);
        let dtype = DType::struct_(
            [("payload", DType::Binary(Nullability::NonNullable))],
            Nullability::NonNullable,
        );
        let bound = BoundUnary::for_relation(&req, &dtype, &memory).unwrap();
        let mut output = Vec::new();
        let mut producer_calls = 0;
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                bound.consume_relation(
                    context,
                    None,
                    1,
                    |consume| {
                        producer_calls += 1;
                        let array = large_dictionary(context);
                        assert!(memory.snapshot().reserved_bytes >= 2 << 20);
                        consume(array)?;
                        // The producer has dropped its only source owner; retained
                        // rows still exist, but their tiny payload must be independent.
                        assert!(memory.snapshot().reserved_bytes < 1 << 20, "{kind:?}");
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
        assert_eq!(producer_calls, 1);
        assert_eq!(output.iter().map(ArrayRef::len).sum::<usize>(), 1);
        assert!(output.iter().map(ArrayRef::nbytes).sum::<u64>() < 64 << 10);
        let clone = output[0].clone();
        let slice = clone.slice(0..1).unwrap();
        drop((output, bound, session));
        assert!(memory.snapshot().reserved_bytes > 0);
        let mut context = VortexSession::default().create_execution_ctx();
        let field =
            crate::local_primitives::logical_field_from_native_array(&slice, "payload").unwrap();
        assert_eq!(
            result_batch::scalar_value(&field, 0, &mut context)
                .unwrap()
                .into_json()
                .unwrap(),
            json!("00fe")
        );
        drop((field, clone));
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(slice);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn typed_unary_repeated_failure_cancellation_and_source_change_release_execution_state() {
    let fixture = source();
    let req = rewritten(
        TYPED
            .iter()
            .map(|name| Rewrite::ForwardFillNull {
                target_column: ColumnRef::new(*name).unwrap(),
                limit: None,
            })
            .collect(),
    );
    let prepared = prepared_composed(&fixture, &req).unwrap();
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
                    Err(crate::local_primitives::vortex_error(
                        "typed consumer stopped",
                    ))
                }
            })
            .err()
            .expect("cancellation or consumer failure must abort execution");
        assert!(
            error.to_string().contains(if cancel {
                "cancel"
            } else {
                "typed consumer stopped"
            }),
            "{error}"
        );
        assert_eq!(calls, 1);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
    }
    let result = prepared.execute_owned().unwrap();
    let retained = result.result.arrays()[0].clone();
    assert_eq!(result.result.dtype(), &source_array().dtype().clone());
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
    let mut context = VortexSession::default().create_execution_ctx();
    let field =
        crate::local_primitives::logical_field_from_native_array(&retained, "instant").unwrap();
    assert_eq!(
        result_batch::scalar_value(&field, 3, &mut context)
            .unwrap()
            .into_json()
            .unwrap(),
        json!(i64::MIN)
    );
    drop((field, retained));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
