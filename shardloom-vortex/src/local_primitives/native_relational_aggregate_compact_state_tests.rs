//! Representation migration, ordered failures, and charged page ownership.

use super::super::{compact_tests::binding, keys, update};
use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    IntoArray as _,
    arrays::{PrimitiveArray, StructArray},
    dtype::{DecimalDType, FieldNames},
    validity::Validity,
};

fn numeric(table: &Table, group: usize, index: usize) -> Numeric {
    let Slot::Numeric(index) = table.slots.values[index] else {
        panic!("numeric slot expected")
    };
    table.numbers[group * table.numeric_width + index]
}

fn assert_numeric(actual: Numeric, expected: &State) {
    assert_eq!(actual.count, expected.count);
    assert!(numeric_reuse::same_sum(actual.sum, expected.sum));
}

#[test]
fn compact_migration_preserves_compensation_signed_zero_and_new_group_identity() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let spec = binding();
            let mut old = ReservedVec::new(context.memory())?;
            old.reserve(6)?;
            old.values.resize_with(6, State::default);
            old.values[0].count = u64::MAX;
            old.values[1].count = 7;
            old.values[1].sum = NumericAggregateSum::Integer {
                sum: 1e18,
                correction: 7.0,
            };
            old.values[2].count = 1;
            old.values[2].sum = NumericAggregateSum::Ordered(-0.0);
            old.values[3].count = 13;
            old.values[4].count = 3;
            old.values[4].sum = NumericAggregateSum::Integer {
                sum: -1e18,
                correction: -1.0,
            };
            let table = Table::promote(&spec, &old.values, 8193, context)?.unwrap();
            for group in 0..2 {
                assert_eq!(table.counts[group], old.values[group * 3].count);
                for index in 1..3 {
                    assert_numeric(
                        numeric(&table, group, index),
                        &old.values[group * 3 + index],
                    );
                }
            }
            for group in 2..8193 {
                assert!(matches!(
                    table.finish(0, group, Function::Count)?,
                    Value::UInt(0)
                ));
                assert!(matches!(
                    table.finish(1, group, Function::Sum)?,
                    Value::Null
                ));
                assert!(matches!(
                    table.finish(2, group, Function::Avg)?,
                    Value::Null
                ));
            }
            drop(old);
            assert_eq!(table.counts[0], u64::MAX);
            assert_eq!(
                numeric(&table, 0, 2).sum.total().to_bits(),
                (-0.0f64).to_bits()
            );
            assert_eq!(
                memory.snapshot().reserved_bytes,
                table.counts.reserved_bytes()
                    + table.numbers.reserved_bytes()
                    + (table.slots.values.capacity() * size_of::<Slot>()) as u64
            );
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn compact_admission_keeps_small_scalar_distinct_extrema_and_decimal_contracts() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut spec = binding();
            let limit = MIN_GENERIC_BYTES / (size_of::<State>() * spec.measures.len());
            assert!(Table::promote(&spec, &[], limit, context)?.is_none());
            assert!(Table::promote(&spec, &[], limit + 1, context)?.is_some());
            for function in [
                Function::Min,
                Function::Max,
                Function::CountDistinct,
                Function::SumDistinct,
                Function::AvgDistinct,
            ] {
                spec.measures[1].function = function;
                assert!(Table::promote(&spec, &[], 8193, context)?.is_none());
            }
            spec.measures[1].function = Function::Sum;
            spec.measures[1].decimal_source = Some(DecimalDType::new(18, 2));
            assert!(Table::promote(&spec, &[], 8193, context)?.is_none());
            spec.measures[1].decimal_source = None;
            spec.groups.clear();
            spec.group_names.clear();
            assert!(Table::promote(&spec, &[], 8193, context)?.is_none());
            spec.measures.clear();
            assert!(Table::promote(&spec, &[], 8193, context)?.is_none());
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn compact_denials_preserve_old_owners_and_partial_page_growth_is_retryable() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let spec = binding();
            let mut old = ReservedVec::new(context.memory())?;
            old.reserve(3)?;
            old.values.resize_with(3, State::default);
            old.values[0].count = 17;
            let before = memory.snapshot().reserved_bytes;
            let held = memory.reserve((8 << 20) - before)?;
            assert!(Table::promote(&spec, &old.values, 8192, context).is_err());
            assert_eq!(old.values[0].count, 17);
            drop(held);
            assert_eq!(memory.snapshot().reserved_bytes, before);
            let mut table = Table::promote(&spec, &old.values, 8192, context)?.unwrap();
            drop(old);
            let held = memory.reserve((8 << 20) - memory.snapshot().reserved_bytes - 12_000)?;
            assert!(table.reserve_groups(8193, context).is_err());
            assert_eq!(table.groups, 8192);
            assert_eq!(
                table.counts.len(),
                8193,
                "COUNT page admitted before numeric denial"
            );
            assert_eq!(table.numbers.len(), 8192 * 2);
            assert_eq!(table.counts[0], 17);
            drop(held);
            table.reserve_groups(8193, context)?;
            assert_eq!(table.groups, 8193);
            assert_eq!(table.counts[8192], 0);
            assert_eq!(table.numbers.len(), 8193 * 2);
            assert!(table.reserve_groups(8192, context).is_err());
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn compact_numeric_failures_match_ordered_reference_and_preserve_partial_state() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let spec = binding();
            for (column, initial) in [
                (
                    PrimitiveArray::from_option_iter([None, Some(1i64), Some(2)]).into_array(),
                    u64::MAX,
                ),
                (
                    PrimitiveArray::from_iter([f64::MAX, f64::MAX, 1.0]).into_array(),
                    0,
                ),
                (
                    PrimitiveArray::from_iter([1.0f64, f64::INFINITY, 3.0]).into_array(),
                    0,
                ),
                (
                    PrimitiveArray::from_iter([
                        1_000_000_000_000_000_000i64,
                        1,
                        -1_000_000_000_000_000_000,
                    ])
                    .into_array(),
                    0,
                ),
            ] {
                let values = keys(&column, context)?;
                let mut table = Table::promote(&spec, &[], 8193, context)?.unwrap();
                table.numbers[0].count = initial;
                let mut expected = State {
                    count: initial,
                    ..State::default()
                };
                let mut reference_error = None;
                for row in 0..column.len() {
                    if let Err(error) = values.raw_cell(row).and_then(|cell| {
                        update(&mut expected, &spec.measures[1], Some(cell), context)
                    }) {
                        reference_error = Some(error.to_string());
                        break;
                    }
                }
                let actual = table.consume(
                    1,
                    &spec.measures[1],
                    &GroupOrdinals::Scalar(column.len()),
                    Some(&values),
                    None,
                    context,
                );
                assert_eq!(actual.err().map(|error| error.to_string()), reference_error);
                assert_numeric(numeric(&table, 0, 1), &expected);
            }
            let mut table = Table::promote(&spec, &[], 8193, context)?.unwrap();
            table.counts[0] = u64::MAX;
            let error = table
                .consume(
                    0,
                    &spec.measures[0],
                    &GroupOrdinals::Scalar(1),
                    None,
                    None,
                    context,
                )
                .unwrap_err()
                .to_string();
            assert!(error.contains("aggregate count overflow"), "{error}");
            assert_eq!(table.counts[0], u64::MAX);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn compact_cancelled_growth_migration_and_null_updates_release_every_credit() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let token = CancellationToken::default();
    let result = session.with_native_execution_context(&token, |context| {
        let spec = binding();
        let mut table = Table::promote(&spec, &[], 8193, context)?.unwrap();
        let column =
            PrimitiveArray::from_option_iter(std::iter::repeat_n(None::<i64>, 2053)).into_array();
        let values = keys(&column, context)?;
        token.cancel();
        assert!(table.reserve_groups(9217, context).is_err());
        assert!(Table::promote(&spec, &[], 8193, context).is_err());
        assert!(
            table
                .consume(
                    1,
                    &spec.measures[1],
                    &GroupOrdinals::Scalar(2053),
                    Some(&values),
                    None,
                    context
                )
                .is_err()
        );
        assert_eq!(numeric(&table, 0, 1).count, 0);
        context.check_cancelled()
    });
    assert!(result.unwrap_err().to_string().contains("cancel"));
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn compact_reuse_compares_exact_starts_and_declines_small_later_batches() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut spec = binding();
            spec.measures[2].column = Some("amount".into());
            let values = PrimitiveArray::from_iter((0..16386).map(|_| 1i64)).into_array();
            let input = StructArray::try_new(
                FieldNames::from(["amount"]),
                vec![values.clone()],
                values.len(),
                Validity::NonNullable,
            )
            .unwrap()
            .into_array();
            let mut table = Table::promote(&spec, &[], 8193, context)?.unwrap();
            assert!(table.admit_next(&spec, 1, &input, &values, context)?);
            table.numbers[1].count = 1;
            assert!(!table.admit_next(&spec, 1, &input, &values, context)?);
            table.numbers[1].count = 0;
            table.numbers[0].sum = NumericAggregateSum::Ordered(-0.0);
            assert!(!table.admit_next(&spec, 1, &input, &values, context)?);
            table.numbers[0].sum = NumericAggregateSum::Integer {
                sum: 1e18,
                correction: 1.0,
            };
            table.numbers[1].sum = NumericAggregateSum::Integer {
                sum: 1e18,
                correction: 2.0,
            };
            assert_eq!(table.numbers[0].sum.total(), table.numbers[1].sum.total());
            assert!(!table.admit_next(&spec, 1, &input, &values, context)?);
            table.numbers[1] = table.numbers[0];
            let small = input.slice(0..1).unwrap();
            assert!(!table.admit_next(&spec, 1, &small, &values, context)?);
            table.copy_completed(1, 2, values.len(), context)?;
            assert!(numeric_reuse::same_sum(
                table.numbers[0].sum,
                table.numbers[1].sum
            ));
            assert!(table.copy_completed(0, 1, values.len(), context).is_err());
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
