use super::*;
use crate::local_primitives::result_batch::Value;
use crate::{
    local_primitives::{
        native_payload,
        native_relational_batch::{index_array, take_column},
    },
    resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    ArrayParts,
    arrays::{VarBin, VarBinArray, VarBinViewArray},
    dtype::Nullability,
};

#[test]
fn dictionary_gather_preserves_width_values_nulls_duplicates_and_selected_order() {
    macro_rules! check {
        ($type:ty) => {{
            let source = DictArray::try_new(
                PrimitiveArray::from_option_iter([
                    Some(3 as $type),
                    None,
                    Some(0),
                    Some(1),
                    Some(3),
                    Some(2),
                ])
                .into_array(),
                VarBinViewArray::from_iter_nullable_str([
                    Some(" 東京 "),
                    None,
                    Some("İßΣΟΣ"),
                    Some("same"),
                ])
                .into_array(),
            )
            .unwrap()
            .into_array();
            let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
            let memory = session.memory().clone();
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let positions = [
                        Some(5),
                        Some(0),
                        None,
                        Some(1),
                        Some(3),
                        Some(2),
                        Some(5),
                        Some(4),
                    ];
                    let indices =
                        index_array(positions.len(), true, context, |row| Ok(positions[row]))?;
                    let output = try_copy(&source, &indices, source.dtype(), context)?.unwrap();
                    let dictionary = output.as_::<Dict>();
                    assert_eq!(
                        dictionary.codes().dtype(),
                        source.as_::<Dict>().codes().dtype()
                    );
                    assert_eq!(dictionary.values().len(), 4);
                    let reference = native_payload::take_with_policy(
                        &source,
                        &indices,
                        source.dtype(),
                        native_payload::CopyPolicy::ValidateValues,
                        context,
                    )?;
                    let expected = [
                        Some("İßΣΟΣ"),
                        Some("same"),
                        None,
                        None,
                        None,
                        Some(" 東京 "),
                        Some("İßΣΟΣ"),
                        Some("same"),
                    ];
                    let mut execution = context.native_session().create_execution_ctx();
                    for (row, expected) in expected.into_iter().enumerate() {
                        let actual = result_batch::scalar(&output, row, &mut execution)?;
                        assert_eq!(
                            actual,
                            result_batch::scalar(&reference, row, &mut execution)?
                        );
                        assert_eq!(actual, vortex::array::scalar::Scalar::from(expected));
                    }
                    Ok(())
                })
                .unwrap();
            drop((session, source));
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }};
    }
    check!(u8);
    check!(u16);
    check!(u32);
    check!(u64);
    check!(i8);
    check!(i16);
    check!(i32);
    check!(i64);
}

#[test]
fn dictionary_gather_detaches_source_and_keeps_receiver_credit_in_child_aliases() {
    let producer = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let receiver = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let producer_memory = producer.memory().clone();
    let receiver_memory = receiver.memory().clone();
    let source = producer
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let allocator = context.native_session().host_allocator();
            let codes = result_batch::build_column(
                &DType::Primitive(PType::U16, Nullability::NonNullable),
                256,
                &allocator,
                |row| Ok(Value::UInt((row % 2) as u64)),
            )?;
            let unused = "UNSELECTED 東京 ".repeat(8192);
            let values = result_batch::build_column(
                &DType::Utf8(Nullability::NonNullable),
                3,
                &allocator,
                |slot| {
                    Ok(Value::Text(
                        ["kept", "also kept", unused.as_str()][slot].into(),
                    ))
                },
            )?;
            Ok(DictArray::try_new(codes, values).unwrap().into_array())
        })
        .unwrap();
    let output = receiver
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = index_array(128, false, context, |row| Ok(Some(row)))?;
            try_copy(&source, &indices, source.dtype(), context)?
                .ok_or_else(|| failed("test dictionary declined"))
        })
        .unwrap();
    assert_eq!(output.as_::<Dict>().values().len(), 2);
    let codes = output.as_::<Dict>().codes().clone();
    let values = output.as_::<Dict>().values().clone();
    let retained = output.slice(1..127).unwrap();
    drop((source, producer));
    assert_eq!(producer_memory.snapshot().reserved_bytes, 0);
    drop((output, receiver));
    assert!(receiver_memory.snapshot().reserved_bytes > 0);
    let mut execution = vortex::session::VortexSession::empty().create_execution_ctx();
    assert_eq!(
        result_batch::scalar(&retained, 0, &mut execution).unwrap(),
        vortex::array::scalar::Scalar::from("also kept")
    );
    drop((retained, execution));
    assert!(receiver_memory.snapshot().reserved_bytes > 0);
    drop((codes, values));
    assert_eq!(receiver_memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dictionary_gather_keeps_selected_utf8_errors_and_does_not_observe_unused_text() {
    let binary = VarBinArray::from(vec![&b"valid"[..], &b"\xff"[..]]);
    let slots = binary.slots().iter().cloned().collect();
    let values = VarBinArray::try_from_parts(ArrayParts::new(
        VarBin,
        DType::Utf8(Nullability::NonNullable),
        2,
        binary.into_data(),
        slots,
    ))
    .unwrap()
    .into_array();
    let source = DictArray::try_new(
        PrimitiveArray::from_iter([0_u8, 1, 0, 0]).into_array(),
        values,
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let valid = index_array(4, false, context, |_| Ok(Some(0)))?;
            let actual = try_copy(&source, &valid, source.dtype(), context)?.unwrap();
            assert_eq!(actual.as_::<Dict>().values().len(), 1);
            let reference = native_payload::take_with_policy(
                &source,
                &valid,
                source.dtype(),
                native_payload::CopyPolicy::ValidateValues,
                context,
            )?;
            let mut execution = context.native_session().create_execution_ctx();
            assert_eq!(
                result_batch::scalar(&actual, 0, &mut execution)?,
                result_batch::scalar(&reference, 0, &mut execution)?
            );
            let invalid = index_array(4, false, context, |row| Ok(Some(row)))?;
            assert!(
                try_copy(&source, &invalid, source.dtype(), context)
                    .unwrap_err()
                    .to_string()
                    .contains("utf-8")
            );
            assert!(
                native_payload::take_with_policy(
                    &source,
                    &invalid,
                    source.dtype(),
                    native_payload::CopyPolicy::ValidateValues,
                    context
                )
                .is_err()
            );
            let invalid_position =
                index_array(4, false, context, |row| Ok(Some([1, 0, 4, 0][row])))?;
            assert!(
                try_copy(&source, &invalid_position, source.dtype(), context)
                    .unwrap_err()
                    .to_string()
                    .contains("gather index")
            );
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dictionary_gather_declines_narrowing_and_nonreduction_and_handles_all_nulls() {
    let source = DictArray::try_new(
        PrimitiveArray::from_option_iter([Some(0_u8), None, Some(1), Some(0)]).into_array(),
        VarBinViewArray::from_iter_nullable_str([None::<&str>, None]).into_array(),
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let full = index_array(4, false, context, |row| Ok(Some(row)))?;
            assert!(
                try_copy(
                    &source,
                    &full,
                    &DType::Utf8(Nullability::NonNullable),
                    context
                )?
                .is_none()
            );
            let small = index_array(1, false, context, |_| Ok(Some(0)))?;
            assert!(try_copy(&source, &small, source.dtype(), context)?.is_none());
            let output = take_column(&source, &full, source.dtype(), context)?;
            assert!(output.is::<Dict>());
            assert_eq!(output.as_::<Dict>().values().len(), 2);
            let mut execution = context.native_session().create_execution_ctx();
            for row in 0..4 {
                assert!(matches!(
                    result_batch::scalar_value(&output, row, &mut execution)?,
                    Value::Null
                ));
            }
            let empty = index_array(0, false, context, |_| unreachable!())?;
            assert!(take_column(&source, &empty, source.dtype(), context)?.is_empty());
            Ok(())
        })
        .unwrap();
}

#[test]
fn dictionary_gather_optional_capacity_declines_and_errors_release_temporary_owners() {
    let source = DictArray::try_new(
        PrimitiveArray::from_iter([0_u8; 128]).into_array(),
        VarBinViewArray::from_iter_str(["valid"]).into_array(),
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = index_array(128, false, context, |row| Ok(Some(row)))?;
            let before = memory.snapshot();
            let held = memory.reserve(before.limit_bytes - before.reserved_bytes - 1500)?;
            let reserved = memory.snapshot().reserved_bytes;
            assert!(try_copy(&source, &indices, source.dtype(), context)?.is_none());
            assert_eq!(memory.snapshot().reserved_bytes, reserved);
            drop(held);
            Ok(())
        })
        .unwrap();
    let token = CancellationToken::default();
    let cancelled = session.with_native_execution_context(&token, |context| {
        let indices = index_array(128, false, context, |row| Ok(Some(row)))?;
        token.cancel();
        assert!(try_copy(&source, &indices, source.dtype(), context).is_err());
        Ok(())
    });
    assert!(cancelled.unwrap_err().to_string().contains("cancelled"));
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dictionary_gather_rejects_selected_bad_codes_and_all_null_codes_keep_no_values() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    for codes in [
        PrimitiveArray::from_iter([0_u64, 0, 0, u64::MAX]).into_array(),
        PrimitiveArray::from_iter([0_i64, 0, 0, -1]).into_array(),
        PrimitiveArray::from_iter([0_u8, 0, 0, 2]).into_array(),
    ] {
        let source = DictArray::try_new(
            codes,
            VarBinViewArray::from_iter_str(["valid", "unused"]).into_array(),
        )
        .unwrap()
        .into_array();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let selected = index_array(4, false, context, |_| Ok(Some(0)))?;
                assert!(try_copy(&source, &selected, source.dtype(), context)?.is_some());
                let full = index_array(4, false, context, |row| Ok(Some(row)))?;
                let reserved = memory.snapshot().reserved_bytes;
                assert!(try_copy(&source, &full, source.dtype(), context).is_err());
                assert_eq!(memory.snapshot().reserved_bytes, reserved);
                Ok(())
            })
            .unwrap();
    }
    let source = DictArray::try_new(
        PrimitiveArray::from_option_iter([None::<u8>; 16]).into_array(),
        VarBinViewArray::from_iter_str(["unused"]).into_array(),
    )
    .unwrap()
    .into_array();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = index_array(16, false, context, |row| Ok(Some(row)))?;
            let output = try_copy(&source, &indices, source.dtype(), context)?.unwrap();
            assert!(output.as_::<Dict>().values().is_empty());
            let mut execution = context.native_session().create_execution_ctx();
            for row in 0..16 {
                assert!(result_batch::scalar(&output, row, &mut execution)?.is_null());
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dictionary_gather_required_value_allocation_failure_refunds_admitted_bookkeeping() {
    let long = "東京".repeat(8192);
    let source = DictArray::try_new(
        PrimitiveArray::from_iter([0_u8; 128]).into_array(),
        VarBinViewArray::from_iter_str([long.as_str()]).into_array(),
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = index_array(128, false, context, |row| Ok(Some(row)))?;
            let before = memory.snapshot();
            let held = memory.reserve(before.limit_bytes - before.reserved_bytes - 4096)?;
            let reserved = memory.snapshot().reserved_bytes;
            assert!(try_copy(&source, &indices, source.dtype(), context).is_err());
            assert_eq!(memory.snapshot().reserved_bytes, reserved);
            drop(held);
            let output = try_copy(&source, &indices, source.dtype(), context)?.unwrap();
            assert_eq!(output.len(), 128);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
