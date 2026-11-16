use super::super::{Cell, KeyColumn, Preparation};
use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_core::FloatPolicy;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::sync::Arc;
use vortex::array::{
    ArrayParts, VortexSessionExecute as _,
    arrays::{DictArray, PrimitiveArray, VarBin, VarBinArray, VarBinViewArray},
    dtype::{DType, Nullability},
};

fn checked_values() -> ArrayRef {
    VarBinArray::from(vec![Some("東京\0 long value"), None, Some("")])
        .into_array()
        .execute::<VarBinViewArray>(
            &mut vortex::session::VortexSession::empty().create_execution_ctx(),
        )
        .unwrap()
        .into_array()
}

fn selected(codes: ArrayRef, indices: ArrayRef, values: ArrayRef) -> ArrayRef {
    let dictionary = DictArray::try_new(codes, values).unwrap().into_array();
    DictArray::try_new(indices, dictionary)
        .unwrap()
        .into_array()
}

#[test]
fn selected_dictionary_values_preserve_all_code_widths_nulls_duplicates_and_order() {
    macro_rules! check {
        ($type:ty) => {{
            let codes = PrimitiveArray::from_option_iter([
                Some(2 as $type),
                Some(0),
                Some(1),
                None,
                Some(2),
                Some(0),
                Some(1),
                Some(2),
                Some(0),
                Some(1),
                Some(2),
                Some(0),
            ])
            .into_array();
            let positions = [
                Some(11_u16),
                None,
                Some(0),
                Some(2),
                Some(3),
                Some(8),
                Some(4),
                Some(11),
            ];
            for indices in [
                PrimitiveArray::from_option_iter(positions).into_array(),
                PrimitiveArray::from_option_iter(positions.map(|position| position.map(u64::from)))
                    .into_array(),
            ] {
                let input = selected(codes.clone(), indices, checked_values());
                let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
                let memory = session.memory().clone();
                session
                    .with_native_execution_context(&CancellationToken::default(), |context| {
                        let mut execution = context.native_session().create_execution_ctx();
                        let reference = input
                            .clone()
                            .execute::<VarBinViewArray>(&mut execution)
                            .map_err(vortex_error)?
                            .into_array();
                        let actual = KeyColumn::values(
                            &input,
                            &mut execution,
                            &memory,
                            &CancellationToken::default(),
                        )?;
                        let expected = KeyColumn::values(
                            &reference,
                            &mut execution,
                            &memory,
                            &CancellationToken::default(),
                        )?;
                        let KeyColumn::Variable(values) = &actual else {
                            panic!("text column missing")
                        };
                        let (prepared_codes, domain) = values.dictionary_parts().unwrap();
                        assert_eq!(prepared_codes.ptype(), codes.as_::<Primitive>().ptype());
                        assert_eq!(domain, 3);
                        for row in 0..input.len() {
                            assert_eq!(actual.cell(row)?, expected.cell(row)?);
                            assert_eq!(
                                actual.compare_at(row, &expected, row)?,
                                std::cmp::Ordering::Equal
                            );
                        }
                        for purpose in [Preparation::Hash, Preparation::HashOnDemand] {
                            let prepared = KeyColumn::prepare(
                                &input,
                                &mut execution,
                                &memory,
                                &CancellationToken::default(),
                                purpose,
                                FloatPolicy::Strict,
                            )?;
                            let KeyColumn::Variable(values) = &prepared else {
                                panic!("text column missing")
                            };
                            assert_eq!(values.dictionary_parts().unwrap().1, 12);
                        }
                        Ok(())
                    })
                    .unwrap();
                drop(session);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
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
fn selected_dictionary_empty_and_all_null_domains_keep_null_semantics() {
    let mut execution = vortex::session::VortexSession::empty().create_execution_ctx();
    for values in [
        VarBinViewArray::from_iter_nullable_str([] as [Option<&str>; 0]).into_array(),
        checked_values(),
    ] {
        let input = selected(
            PrimitiveArray::from_option_iter([None::<u8>; 16]).into_array(),
            PrimitiveArray::from_iter([15_u16, 0, 7, 1, 3, 9, 15, 0]).into_array(),
            values,
        );
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let prepared = KeyColumn::values(
            &input,
            &mut execution,
            &memory,
            &CancellationToken::default(),
        )
        .unwrap();
        let KeyColumn::Variable(values) = &prepared else {
            panic!("text column missing")
        };
        assert!(values.dictionary_parts().unwrap().1 < input.len());
        for row in 0..input.len() {
            assert_eq!(prepared.cell(row).unwrap(), Cell::Null);
        }
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        let empty = input.slice(0..0).unwrap();
        assert!(
            compose(&empty, &mut execution, &CancellationToken::default())
                .unwrap()
                .is_none()
        );
        assert!(
            KeyColumn::values(
                &empty,
                &mut execution,
                &memory,
                &CancellationToken::default()
            )
            .is_ok()
        );
    }
}

#[test]
fn selected_dictionary_does_not_bypass_unobserved_inner_codes_or_outer_errors() {
    let mut execution = vortex::session::VortexSession::empty().create_execution_ctx();
    for codes in [
        PrimitiveArray::from_iter([0_u64, 0, 0, u64::MAX]).into_array(),
        PrimitiveArray::from_iter([0_i64, 0, 0, -1]).into_array(),
        PrimitiveArray::from_iter([0_u8, 0, 0, 3]).into_array(),
    ] {
        // The bad inner row is deliberately absent from the outer selection.
        let input = selected(
            codes,
            PrimitiveArray::from_iter([0_u16; 8]).into_array(),
            checked_values(),
        );
        assert!(
            compose(&input, &mut execution, &CancellationToken::default())
                .unwrap()
                .is_none()
        );
    }
    let input = selected(
        PrimitiveArray::from_iter([0_u8; 8]).into_array(),
        PrimitiveArray::from_iter([0_u16, 0, 0, 8]).into_array(),
        checked_values(),
    );
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    assert!(
        compose(&input, &mut execution, &CancellationToken::default())
            .unwrap()
            .is_none()
    );
    let prepared = KeyColumn::values(
        &input,
        &mut execution,
        &memory,
        &CancellationToken::default(),
    )
    .unwrap();
    assert!(prepared.cell(0).is_ok());
    assert!(
        prepared
            .cell(3)
            .unwrap_err()
            .to_string()
            .contains("dictionary code exceeds")
    );
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn selected_dictionary_unproved_text_keeps_existing_value_validation_boundary() {
    let binary = VarBinArray::from(vec![&b"valid"[..], &b"\xff"[..]]);
    let slots = binary.slots().iter().cloned().collect();
    let malformed = VarBinArray::try_from_parts(ArrayParts::new(
        VarBin,
        DType::Utf8(Nullability::NonNullable),
        2,
        binary.into_data(),
        slots,
    ))
    .unwrap()
    .into_array();
    let mut execution = vortex::session::VortexSession::empty().create_execution_ctx();
    for domain in [
        malformed,
        VarBinViewArray::from_iter_str(["valid", "also valid"]).into_array(),
    ] {
        let input = selected(
            PrimitiveArray::from_iter([0_u8; 8]).into_array(),
            PrimitiveArray::from_iter([0_u16; 4]).into_array(),
            domain,
        );
        // No canonicalization or UTF-8 inspection is used to obtain admission.
        assert!(
            compose(&input, &mut execution, &CancellationToken::default())
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn selected_dictionary_keeps_opaque_source_credit_until_prepared_owner_drops() {
    let producer = LiveMemoryPool::new(1 << 20).unwrap();
    let credit = Arc::new(producer.reserve(8192).unwrap());
    let input = selected(
        PrimitiveArray::from_iter([0_u8; 16]).into_array(),
        PrimitiveArray::from_iter([15_u16, 1, 7, 0, 3, 6, 9, 15]).into_array(),
        checked_values(),
    )
    .with_retained_owner(credit)
    .unwrap();
    let receiver = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = receiver.memory().clone();
    let prepared = receiver
        .with_native_execution_context(&CancellationToken::default(), |context| {
            KeyColumn::values(
                &input,
                &mut context.native_session().create_execution_ctx(),
                &memory,
                &CancellationToken::default(),
            )
        })
        .unwrap();
    drop(input);
    assert_eq!(producer.snapshot().reserved_bytes, 8192);
    assert_eq!(
        prepared.cell(7).unwrap(),
        Cell::Utf8(vortex::buffer::ByteBuffer::from(
            "東京\0 long value".as_bytes().to_vec()
        ))
    );
    drop((prepared, receiver));
    assert_eq!(producer.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn selected_dictionary_pressure_cancellation_and_retry_release_composition_buffers() {
    let input = selected(
        PrimitiveArray::from_iter([0_u8; 4096]).into_array(),
        PrimitiveArray::from_iter((0..2048).map(|row| u16::try_from(row * 2).unwrap()))
            .into_array(),
        checked_values(),
    );
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut execution = context.native_session().create_execution_ctx();
            let before = memory.snapshot();
            let occupied = memory.reserve(before.limit_bytes - before.reserved_bytes)?;
            assert!(
                KeyColumn::values(
                    &input,
                    &mut execution,
                    &memory,
                    &CancellationToken::default()
                )
                .is_err()
            );
            assert_eq!(memory.snapshot().reserved_bytes, before.limit_bytes);
            drop(occupied);
            let prepared = KeyColumn::values(
                &input,
                &mut execution,
                &memory,
                &CancellationToken::default(),
            )?;
            assert!(prepared.cell(2047).is_ok());
            drop(prepared);
            assert_eq!(memory.snapshot().reserved_bytes, before.reserved_bytes);
            let token = CancellationToken::default();
            token.cancel();
            assert!(compose(&input, &mut execution, &token).is_err());
            assert_eq!(memory.snapshot().reserved_bytes, before.reserved_bytes);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
