//! Selected encoded leaves retain their exact types, validity and output owners.

use super::*;
use crate::{
    local_primitives::native_relational_batch::index_array, resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    arrays::{DictArray, ExtensionArray},
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
