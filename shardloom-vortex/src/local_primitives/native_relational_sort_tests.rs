use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_core::ColumnRef;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{IntoArray as _, arrays::PrimitiveArray};

fn spec(array: &ArrayRef, names: &[&str]) -> Spec {
    let DType::Struct(fields, _) = array.dtype() else {
        panic!("expected struct")
    };
    Spec {
        fields: fields
            .names()
            .iter()
            .zip(fields.fields())
            .map(|(name, dtype)| (name.to_string(), dtype))
            .collect(),
        keys: names
            .iter()
            .map(|name| VortexRelationalOrderKey {
                column: ColumnRef::new(*name).unwrap(),
                descending: false,
                nulls: Some(NullOrder::First),
            })
            .collect(),
        names: names.iter().map(|name| (*name).to_owned()).collect(),
        copy_policy: CopyPolicy::ValidateValues,
    }
}

#[test]
fn native_relational_sort_resolves_each_row_once_for_tied_multikey_comparisons() {
    let values: Vec<_> = (0..257)
        .map(|row| {
            (
                (row % 19 != 0).then_some(i64::from(row % 3)),
                (row % 13 != 0).then_some(i64::from(row % 7)),
            )
        })
        .collect();
    let array = StructArray::try_new(
        FieldNames::from(["a", "b"]),
        vec![
            PrimitiveArray::from_option_iter(values.iter().map(|value| value.0)).into_array(),
            PrimitiveArray::from_option_iter(values.iter().map(|value| value.1)).into_array(),
        ],
        values.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let binding = spec(&array, &["a", "b"]);
    let mut expected: Vec<_> = (0..values.len()).collect();
    expected.sort_by_key(|row| (values[*row], *row));
    let mut comparison_count = 0;
    let mut reference: Vec<_> = (0..values.len()).collect();
    native_relational_order::sort(&mut reference, &CancellationToken::default(), |a, b| {
        comparison_count += 1;
        Ok((values[a], a).cmp(&(values[b], b)))
    })
    .unwrap();
    assert_eq!(reference, expected);
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut sort = Sort::new(&binding, context.memory())?;
            for range in [0..0, 0..1, 1..64, 64..64, 64..256, 256..257, 257..257] {
                sort.build(array.slice(range).map_err(vortex_error)?, context)?;
            }
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            let actual = sort.ordered_rows(context)?;
            #[cfg(debug_assertions)]
            assert_eq!(
                observation.finish().row_location_searches,
                comparison_count * 2
            );
            assert_eq!(actual.values, expected);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_relational_sort_validates_payload_after_the_consumer_has_enough_rows() {
    let array = StructArray::try_new(
        FieldNames::from(["key", "payload"]),
        vec![
            PrimitiveArray::from_iter([0i64, 1]).into_array(),
            PrimitiveArray::from_iter([1.0f64, f64::NAN]).into_array(),
        ],
        2,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let binding = spec(&array, &["key"]);
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut sort = Sort::new(&binding, context.memory())?;
            sort.build(array, context)?;
            let mut emitted = 0;
            let error = sort
                .finish(context, 1, &mut |batch| {
                    // A downstream LIMIT has no remaining output demand after
                    // this row, but the next payload must still be validated.
                    assert_eq!(batch.len(), 1);
                    emitted += batch.len();
                    Ok(())
                })
                .unwrap_err();
            assert_eq!(emitted, 1);
            assert!(
                error.to_string().contains("finite declared dtype"),
                "{error}"
            );
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_relational_sort_validates_every_key_before_ordering() {
    let array = StructArray::try_new(
        FieldNames::from(["first", "later"]),
        vec![
            PrimitiveArray::from_iter([0i64, 1]).into_array(),
            PrimitiveArray::from_option_iter([Some(0.0f64), Some(f64::NAN)]).into_array(),
        ],
        2,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let binding = spec(&array, &["first", "later"]);
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut sort = Sort::new(&binding, context.memory())?;
            let error = sort.build(array, context).unwrap_err();
            assert!(error.to_string().contains("nonfinite"), "{error}");
            assert_eq!(sort.table.rows(), 0);
            Ok(())
        })
        .unwrap();
}
