//! Selected encoded leaves retain their exact types, validity and output owners.

use super::*;
use crate::{
    local_primitives::native_relational_batch::{index_array, take_column},
    resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    arrays::{ConstantArray, DictArray, ExtensionArray},
    dtype::FieldNames,
    extension::datetime::{TimeUnit, Timestamp},
};

const DOMAIN: [Option<i64>; 8] = [
    Some(i64::MIN),
    Some(i64::MAX),
    None,
    Some(-1),
    Some(0),
    Some(1),
    Some(i64::MIN + 1),
    Some(i64::MAX - 1),
];

fn dictionary(values: ArrayRef, rows: usize) -> ArrayRef {
    DictArray::try_new(
        PrimitiveArray::from_iter((0..rows).map(|row| u8::try_from(row % 8).unwrap())).into_array(),
        values,
    )
    .unwrap()
    .into_array()
}

fn check_numeric_take(array: &ArrayRef, context: &NativeExecutionContext<'_>) -> Result<ArrayRef> {
    let mut execution = context.native_session().create_execution_ctx();
    let rows = [Some(3), None, Some(0), Some(2), Some(1), Some(3)];
    let indices = index_array(rows.len(), true, context, |row| Ok(rows[row]))?;
    let dtype = array.dtype().as_nullable();
    let output = take_column(array, &indices, &dtype, context)?;
    assert_eq!(output.dtype(), &dtype);
    assert_eq!(output.len(), rows.len());
    for (row, selected) in rows.iter().enumerate() {
        let expected = selected.map_or(Ok(Value::Null), |row| {
            result_batch::scalar_value(array, row, &mut execution)
        })?;
        let actual = result_batch::scalar_value(&output, row, &mut execution)?;
        match (actual, expected) {
            (Value::Null, Value::Null) => {}
            (Value::Int(actual), Value::Int(expected)) => assert_eq!(actual, expected),
            (Value::UInt(actual), Value::UInt(expected)) => assert_eq!(actual, expected),
            (Value::Float(actual), Value::Float(expected)) => {
                assert_eq!(actual.to_bits(), expected.to_bits());
            }
            _ => panic!("numeric gather changed selected value, type or validity"),
        }
    }
    Ok(output)
}

#[test]
fn relational_numeric_take_preserves_widths_encodings_selection_and_owned_credits() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let cancellation = CancellationToken::default();
    let output = session
        .with_native_execution_context(&cancellation, |context| {
            let mut arrays = Vec::new();
            macro_rules! integer {
                ($ty:ty) => {
                    let zero: $ty = 0;
                    let one: $ty = 1;
                    arrays.push(
                        PrimitiveArray::from_option_iter([
                            Some(<$ty>::MIN),
                            Some(<$ty>::MAX),
                            None,
                            Some(one),
                        ])
                        .into_array(),
                    );
                    arrays.push(
                        PrimitiveArray::from_iter([<$ty>::MIN, <$ty>::MAX, zero, one]).into_array(),
                    );
                };
            }
            integer!(u8);
            integer!(u16);
            integer!(u32);
            integer!(u64);
            integer!(i8);
            integer!(i16);
            integer!(i32);
            integer!(i64);
            arrays.push(
                PrimitiveArray::from_option_iter([
                    Some(-0.0_f32),
                    Some(f32::MAX),
                    None,
                    Some(f32::MIN_POSITIVE),
                ])
                .into_array(),
            );
            arrays.push(
                PrimitiveArray::from_option_iter([
                    Some(-0.0_f64),
                    Some(f64::MAX),
                    None,
                    Some(f64::MIN_POSITIVE),
                ])
                .into_array(),
            );
            for array in arrays {
                let mut execution = context.native_session().create_execution_ctx();
                let constant = ConstantArray::new(
                    array
                        .execute_scalar(0, &mut execution)
                        .map_err(vortex_error)?,
                    4,
                )
                .into_array();
                let dictionary = DictArray::try_new(
                    PrimitiveArray::from_iter([3_u8, 1, 2, 0, 1]).into_array(),
                    array.clone(),
                )
                .map_err(vortex_error)?
                .into_array();
                let chunked = ChunkedArray::try_new(
                    vec![
                        array.slice(0..1).map_err(vortex_error)?,
                        array.slice(1..4).map_err(vortex_error)?,
                    ],
                    array.dtype().clone(),
                )
                .map_err(vortex_error)?
                .into_array();
                for source in [
                    array,
                    constant,
                    dictionary.slice(1..5).map_err(vortex_error)?,
                    chunked,
                ] {
                    check_numeric_take(&source, context)?;
                    let empty = index_array(0, false, context, |_| unreachable!())?;
                    assert_eq!(
                        take_column(&source, &empty, source.dtype(), context)?.len(),
                        0
                    );
                }
            }
            check_numeric_take(&timestamps(8), context)?;
            let all_null = PrimitiveArray::from_option_iter([None::<i64>; 4]).into_array();
            check_numeric_take(&all_null, context)
        })
        .unwrap();
    assert!(session.memory().snapshot().reserved_bytes > 0);
    let child = output.slice(1..3).unwrap();
    drop(output);
    assert!(session.memory().snapshot().reserved_bytes > 0);
    drop(child);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn relational_numeric_take_keeps_selected_finite_checks_and_failure_cleanup() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let cancellation = CancellationToken::default();
    let mut checked_cancelled_copy = false;
    let error = session.with_native_execution_context(&cancellation, |context| {
        let source = PrimitiveArray::from_iter([f64::NAN, -0.0, f64::INFINITY, 2.0]).into_array();
        let indices = index_array(2, false, context, |row| Ok(Some(row * 2 + 1)))?;
        let output = take_column(&source, &indices, source.dtype(), context)?;
        let mut execution = context.native_session().create_execution_ctx();
        assert!(matches!(result_batch::scalar_value(&output, 0, &mut execution)?, Value::Float(value) if value.to_bits() == (-0.0_f64).to_bits()));
        for row in [0, 2] {
            let bad = index_array(1, false, context, |_| Ok(Some(row)))?;
            let before = context.memory().snapshot().reserved_bytes;
            assert_eq!(take_column(&source, &bad, source.dtype(), context).unwrap_err().to_string(),
                "native result batch: numeric value differs from its finite declared dtype; no fallback execution was attempted");
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
        }
        let before = context.memory().snapshot().reserved_bytes;
        let hold = context.memory().reserve((1 << 20) - before)?;
        assert!(take_column(&source, &indices, source.dtype(), context).is_err());
        drop(hold);
        assert_eq!(context.memory().snapshot().reserved_bytes, before);
        cancellation.cancel();
        assert!(take_column(&source, &indices, source.dtype(), context).is_err());
        assert_eq!(context.memory().snapshot().reserved_bytes, before);
        checked_cancelled_copy = true;
        Ok(())
    }).unwrap_err();
    assert!(checked_cancelled_copy);
    assert_eq!(
        error.to_string(),
        "execution cancelled; fallback execution was not attempted"
    );
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

fn timestamps(rows: usize) -> ArrayRef {
    ExtensionArray::new(
        Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
        dictionary(PrimitiveArray::from_option_iter(DOMAIN).into_array(), rows),
    )
    .into_array()
}

fn source(rows: usize) -> ArrayRef {
    let nested = StructArray::new(
        FieldNames::from(["timestamp", "signed", "unsigned", "items"]),
        vec![
            timestamps(rows),
            dictionary(PrimitiveArray::from_iter(-4_i8..4).into_array(), rows),
            dictionary(
                PrimitiveArray::from_iter((0..8).map(|value| u64::MAX - value)).into_array(),
                rows,
            ),
            ListArray::new(
                timestamps(rows * 2),
                PrimitiveArray::from_iter((0..=rows).map(|row| (row * 2) as u64)).into_array(),
                Validity::from_iter((0..rows).map(|row| row % 3 != 0)),
            )
            .into_array(),
        ],
        rows,
        Validity::from_iter((0..rows).map(|row| row % 13 != 0)),
    )
    .into_array();
    // Dictionary parents compose with the subsequent arbitrary-row selection.
    dictionary(nested.slice(0..8).unwrap(), rows)
}

#[test]
fn native_payload_numeric_compaction_preserves_encoded_nested_selection_and_owners() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let cancellation = CancellationToken::default();
    let output = session
        .with_native_execution_context(&cancellation, |context| {
            let source = source(259);
            let indices = index_array(4099, true, context, |row| {
                Ok((row % 7 != 0).then_some((row * 17) % source.len()))
            })?;
            let output = take(&source, &indices, source.dtype(), context)?;
            assert_eq!(output.dtype(), source.dtype());
            assert_eq!(output.len(), 4099);
            let mut execution = context.native_session().create_execution_ctx();
            let structure = output
                .clone()
                .execute::<StructArray>(&mut execution)
                .map_err(vortex_error)?;
            let items =
                native_list::Column::new(structure.unmasked_field(3).clone(), &mut execution)?;
            for row in 0..4099 {
                let original = ((row * 17) % source.len()) % 8;
                let valid = row % 7 != 0 && original != 0;
                assert_eq!(
                    structure
                        .struct_validity()
                        .execute_is_valid(row, &mut execution)
                        .map_err(vortex_error)?,
                    valid
                );
                if !valid {
                    continue;
                }
                let timestamp =
                    result_batch::scalar_value(structure.unmasked_field(0), row, &mut execution)?;
                match (timestamp, DOMAIN[original]) {
                    (Value::Null, None) => {}
                    (Value::Int(actual), Some(expected)) => assert_eq!(actual, expected),
                    _ => panic!("timestamp changed type or validity"),
                }
                assert!(matches!(
                    result_batch::scalar_value(structure.unmasked_field(1), row, &mut execution)?,
                    Value::Int(value) if value == i64::try_from(original).unwrap() - 4
                ));
                assert!(matches!(
                    result_batch::scalar_value(structure.unmasked_field(2), row, &mut execution)?,
                    Value::UInt(value) if value == u64::MAX - original as u64
                ));
                let coordinates = items.coordinates(row, &mut execution)?;
                if original.is_multiple_of(3) {
                    assert_eq!(coordinates, None);
                    continue;
                }
                let (start, count) = coordinates.unwrap();
                assert_eq!(count, 2);
                for offset in 0..count {
                    match (
                        result_batch::scalar_value(
                            &items.elements,
                            start + offset,
                            &mut execution,
                        )?,
                        DOMAIN[(original * 2 + offset) % 8],
                    ) {
                        (Value::Null, None) => {}
                        (Value::Int(actual), Some(expected)) => assert_eq!(actual, expected),
                        _ => panic!("list timestamp changed type or validity"),
                    }
                }
            }
            let before = context.memory().snapshot().reserved_bytes;
            let hold = context.memory().reserve((16 << 20) - before - 1)?;
            assert!(take(&source, &indices, source.dtype(), context).is_err());
            drop(hold);
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            drop((structure, items, indices, source));
            Ok(output)
        })
        .unwrap();
    assert!(session.memory().snapshot().reserved_bytes > 0);
    let child = output
        .as_::<vortex::array::arrays::Struct>()
        .unmasked_field(0)
        .slice(1..2)
        .unwrap();
    drop(output);
    assert!(session.memory().snapshot().reserved_bytes > 0);
    drop(child);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
