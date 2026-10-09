use super::*;
use crate::local_primitives::result_batch;
use std::borrow::Cow;
use vortex::array::memory::MemorySessionExt as _;

fn large_unused_children(context: &NativeExecutionContext<'_>) -> ArrayRef {
    let huge = vec![255u8; 2 << 20];
    let values = result_batch::build_column(
        &DType::Binary(Nullability::NonNullable),
        2,
        &context.native_session().allocator(),
        |row| {
            Ok(Value::Binary(Cow::Borrowed(if row == 0 {
                &huge
            } else {
                b"\x00\xfe"
            })))
        },
    )
    .unwrap();
    let selected = ListViewArray::try_new(
        values,
        PrimitiveArray::from_iter([1u64; 3]).into_array(),
        PrimitiveArray::from_iter([1u64; 3]).into_array(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    table(selected.clone(), selected.clone(), selected)
}

#[test]
fn unary_nested_pivot_compacts_all_retained_roles_and_output_credits_survive_session_drop() {
    for aggregate in ["first", "first_unique", "count", "min", "max"] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let nested = DType::List(
            std::sync::Arc::new(DType::Binary(Nullability::NonNullable)),
            Nullability::NonNullable,
        );
        let dtype = DType::struct_(
            ["entity", "category", "amount"]
                .into_iter()
                .map(|name| (name, nested.clone())),
            Nullability::NonNullable,
        );
        let bound =
            BoundUnary::for_relation(&relation_request(aggregate), &dtype, &memory).unwrap();
        let mut output = Vec::new();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let completed = bound.complete_relation_pivot(context, None, |accept| {
                    let input = large_unused_children(context);
                    assert!(memory.snapshot().reserved_bytes >= 2 << 20);
                    accept(input)?;
                    assert!(
                        memory.snapshot().reserved_bytes < 1 << 20,
                        "{aggregate} retained an unselected child domain"
                    );
                    Ok(())
                })?;
                completed.emit(&bound, context, 1, None, &mut |array| {
                    output.push(array);
                    Ok(())
                })
            })
            .unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].len(), 1);
        assert!(output[0].nbytes() < 4096);
        let clone = output[0].clone();
        let slice = clone.slice(0..1).unwrap();
        drop((output, bound, session));
        assert!(memory.snapshot().reserved_bytes > 0);
        let field = runtime::logical_field_from_native_array(&slice, "entity").unwrap();
        let mut execution = VortexSession::default().create_execution_ctx();
        assert_eq!(
            field
                .execute_scalar(0, &mut execution)
                .unwrap()
                .as_list()
                .elements()
                .unwrap(),
            vec![Scalar::from(&b"\x00\xfe"[..])]
        );
        drop((field, clone));
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(slice);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

fn one_large_value(text: &str) -> ArrayRef {
    table(
        VarBinArray::from(vec!["a"]).into_array(),
        VarBinArray::from(vec!["x"]).into_array(),
        ListViewArray::try_new(
            VarBinArray::from(vec![text]).into_array(),
            PrimitiveArray::from_iter([0u64]).into_array(),
            PrimitiveArray::from_iter([1u64]).into_array(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
    )
}

#[test]
fn unary_nested_pivot_unchanged_cells_avoid_payload_copies_and_replacements_charge_overlap() {
    for (aggregate, old, new, replace) in [
        ("first", 'z', 'a', false),
        ("first_unique", 'z', 'z', false),
        ("min", 'a', 'z', false),
        ("max", 'z', 'a', false),
        ("min", 'z', 'a', true),
        ("max", 'a', 'z', true),
    ] {
        let old = old.to_string().repeat(128 << 10);
        let new = new.to_string().repeat(128 << 10);
        let initial = one_large_value(&old);
        let next = one_large_value(&new);
        let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let bound =
            BoundUnary::for_relation(&relation_request(aggregate), initial.dtype(), &memory)
                .unwrap();
        let baseline = memory.snapshot().reserved_bytes;
        let result =
            session.with_native_execution_context(&CancellationToken::default(), |context| {
                bound.complete_relation_pivot(context, None, |accept| {
                    accept(initial)?;
                    let retained = memory.snapshot().reserved_bytes;
                    assert!(retained > baseline + (128 << 10));
                    let blocker =
                        memory.reserve(memory.snapshot().limit_bytes - retained - (64 << 10))?;
                    let result = accept(next);
                    drop(blocker);
                    result
                })
            });
        if replace {
            let error = result
                .err()
                .expect("replacement must reserve while the old value remains owned");
            assert!(
                error.to_string().contains("reservation"),
                "{aggregate}: {error}"
            );
            assert!(memory.snapshot().denied_reservations > 0);
        } else {
            drop(result.unwrap());
            assert_eq!(memory.snapshot().denied_reservations, 0);
        }
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        drop((bound, session));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn unary_nested_pivot_failures_cancellation_and_source_change_release_state_and_staging() {
    let fixture = Fixture::from_array(core_array("list_index_and_cells"), 1);
    let prepared = prepare(&request(&fixture, "min"));
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let memory = prepared.session.memory().clone();
    let blocker = memory
        .reserve(memory.snapshot().limit_bytes - baseline - 1024)
        .unwrap();
    let target = fixture.0.join("pressure.vortex");
    assert!(prepared.write(&target, Format::Vortex, false).is_err());
    assert!(!target.exists());
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
    drop(blocker);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    for cancel in [false, true] {
        let token = CancellationToken::default();
        let mut delivered = 0;
        let error = prepared
            .for_each_batch(&token, |_, _| {
                delivered += 1;
                if cancel {
                    token.cancel();
                    Ok(())
                } else {
                    Err(failed("nested pivot consumer stopped"))
                }
            })
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains(if cancel { "cancel" } else { "consumer stopped" }),
            "{error}"
        );
        assert_eq!(delivered, 1);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
    }
    let token = CancellationToken::default();
    token.cancel();
    assert!(
        prepared
            .write_controlled(&target, Format::Vortex, false, &token)
            .is_err()
    );
    assert!(!target.exists());
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let error = prepared
        .for_each_batch(&CancellationToken::default(), |_, _| {
            fixture.replace();
            Ok(())
        })
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("generation") || error.to_string().contains("changed"),
        "{error}"
    );
    assert!(prepared.execute_owned().is_err());
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
}

#[test]
fn unary_nested_pivot_discovery_cancellation_drops_selected_payloads() {
    let input = core_array("list_index_and_cells");
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let bound = BoundUnary::for_relation(&relation_request("min"), input.dtype(), &memory).unwrap();
    let baseline = memory.snapshot().reserved_bytes;
    let token = CancellationToken::default();
    let mut accepted = false;
    let result = session.with_native_execution_context(&token, |context| {
        bound.complete_relation_pivot(context, None, |accept| {
            accept(input)?;
            accepted = true;
            token.cancel();
            Ok(())
        })
    });
    assert!(accepted);
    assert!(result.is_err());
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
}
