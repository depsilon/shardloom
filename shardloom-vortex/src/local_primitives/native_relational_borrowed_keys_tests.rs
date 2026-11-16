use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use std::hash::Hasher as _;
use vortex::array::{
    IntoArray as _,
    arrays::{ChunkedArray, DictArray, PrimitiveArray, StructArray, VarBinArray, VarBinViewArray},
    dtype::FieldNames,
};

fn hash(key: &KeyColumn, row: usize) -> Result<(bool, u64)> {
    let mut hash = rustc_hash::FxHasher::default();
    let valid = key.hash_into(row, &mut hash)?;
    Ok((valid, hash.finish()))
}

#[test]
fn direct_join_aggregate_borrowed_domain_preserves_ordinary_key_footprint() {
    // Every retained batch pays for its key owners, including unrelated numeric
    // window/grid inputs. Borrowed offsets must not enlarge that shared slot.
    assert_eq!(size_of::<VariableDomain>(), size_of::<VarBinViewArray>());
}

#[test]
fn direct_join_aggregate_borrowed_offsets_reserve_refund_and_decline() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let array = VarBinArray::from(vec![Some("first"), None, Some("東京")]).into_array();
            let memory = context.memory();
            let initial = memory.snapshot().reserved_bytes;
            let key = KeyColumn::try_borrow(&array, context)?.unwrap();
            assert!(memory.snapshot().reserved_bytes > initial);
            assert_eq!(
                key.cell(2)?,
                super::super::Cell::Utf8(vortex::buffer::ByteBuffer::copy_from("東京".as_bytes()))
            );
            drop(key);
            assert_eq!(memory.snapshot().reserved_bytes, initial);
            let pressure = memory.reserve(memory.snapshot().limit_bytes - initial)?;
            assert!(KeyColumn::try_borrow(&array, context)?.is_none());
            assert_eq!(
                memory.snapshot().reserved_bytes,
                memory.snapshot().limit_bytes
            );
            drop(pressure);
            assert_eq!(memory.snapshot().reserved_bytes, initial);
            Ok(())
        })
        .unwrap();
}

#[test]
fn direct_join_aggregate_borrowed_dictionary_keeps_cross_codebook_value_identity() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            for views in [false, true] {
                let domain = |values: Vec<Option<&str>>| {
                    let array = VarBinArray::from(values).into_array();
                    if views {
                        array
                            .execute::<VarBinViewArray>(
                                &mut context.native_session().create_execution_ctx(),
                            )
                            .unwrap()
                            .into_array()
                    } else {
                        array
                    }
                };
                let first = DictArray::try_new(
                    PrimitiveArray::from_option_iter([Some(0_u8), Some(2), None, Some(1), Some(0)])
                        .into_array(),
                    domain(vec![Some("shared\0東京"), None, Some("")]),
                )
                .unwrap()
                .into_array();
                let second = DictArray::try_new(
                    PrimitiveArray::from_option_iter([
                        Some(2_u64),
                        Some(0),
                        None,
                        Some(1),
                        Some(2),
                    ])
                    .into_array(),
                    domain(vec![Some(""), None, Some("shared\0東京")]),
                )
                .unwrap()
                .into_array();
                let actual = KeyColumn::try_borrow(&first, context)?.unwrap();
                let other = KeyColumn::try_borrow(&second, context)?.unwrap();
                let reference = KeyColumn::new_with_float_policy(
                    &first,
                    &mut context.native_session().create_execution_ctx(),
                    context.memory(),
                    context.cancellation(),
                    context.float_policy(),
                )?;
                for row in 0..5 {
                    actual.validate_at(row)?;
                    assert_eq!(hash(&actual, row)?, hash(&reference, row)?);
                    assert_eq!(hash(&actual, row)?, hash(&other, row)?);
                    assert!(actual.equals_at(row, &reference, row, true)?);
                    assert!(actual.equals_at(row, &other, row, true)?);
                }
                let wrapped = ChunkedArray::try_new(vec![first.clone()], first.dtype().clone())
                    .unwrap()
                    .into_array();
                let slice = wrapped.slice(1..5).unwrap().slice(1..3).unwrap();
                let selected = KeyColumn::try_borrow(&slice, context)?.unwrap();
                assert_eq!(selected.len(), 2);
                for row in 0..2 {
                    assert!(selected.equals_at(row, &reference, row + 2, true)?);
                }
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn direct_join_aggregate_borrowing_does_not_observe_unselected_bad_codes_or_floats() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let dictionary = DictArray::try_new(
                PrimitiveArray::from_iter([0_u8, 99]).into_array(),
                VarBinArray::from(vec![Some("valid")]).into_array(),
            )
            .unwrap()
            .into_array();
            let values = KeyColumn::try_borrow(&dictionary, context)?.unwrap();
            values.validate_at(0)?;
            assert!(
                values
                    .validate_at(1)
                    .unwrap_err()
                    .to_string()
                    .contains("dictionary code exceeds")
            );
            let floats = PrimitiveArray::from_iter([2.0_f64, f64::NAN]).into_array();
            let values = KeyColumn::try_borrow(&floats, context)?.unwrap();
            values.validate_at(0)?;
            assert!(
                values
                    .validate_at(1)
                    .unwrap_err()
                    .to_string()
                    .contains("nonfinite")
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn direct_join_aggregate_nullable_parent_and_encoded_numeric_decline_without_execution() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let array = StructArray::try_new(
                FieldNames::from(["value"]),
                vec![PrimitiveArray::from_iter([1_i64, 2]).into_array()],
                2,
                Validity::AllInvalid,
            )
            .unwrap()
            .into_array();
            let logical =
                crate::local_primitives::logical_field_from_native_array(&array, "value")?;
            assert!(KeyColumn::try_borrow(&logical, context)?.is_none());
            let encoded = DictArray::try_new(
                PrimitiveArray::from_iter([0_u8, 99]).into_array(),
                PrimitiveArray::from_iter([1_i64]).into_array(),
            )
            .unwrap()
            .into_array();
            assert!(KeyColumn::try_borrow(&encoded, context)?.is_none());
            Ok(())
        })
        .unwrap();
}
