use super::*;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{DictArray, ExtensionArray, VarBinArray},
    dtype::Nullability,
    extension::datetime::{Date, TimeUnit, Timestamp},
    validity::Validity,
};

fn key(array: &ArrayRef, memory: &LiveMemoryPool) -> KeyColumn {
    KeyColumn::new(
        array,
        &mut vortex::array::legacy_session().create_execution_ctx(),
        memory,
        &CancellationToken::default(),
    )
    .unwrap()
}

fn hash(column: &KeyColumn, row: usize) -> (bool, u64) {
    let mut hash = rustc_hash::FxHasher::default();
    (column.hash_into(row, &mut hash).unwrap(), hash.finish())
}

#[test]
fn native_typed_keys_binary_dictionary_domains_preserve_bytes_nulls_and_credits() {
    let memory = LiveMemoryPool::new(4096).unwrap();
    let a = DictArray::try_new(
        PrimitiveArray::from_iter([2u8, 1, 0, 3, 2]).into_array(),
        VarBinArray::from(vec![
            None,
            Some(&b""[..]),
            Some(&b"\xff\0"[..]),
            Some(&b"\0"[..]),
        ])
        .into_array(),
    )
    .unwrap()
    .into_array();
    let b = DictArray::try_new(
        PrimitiveArray::from_iter([0u16, 3, 1, 2, 0]).into_array(),
        VarBinArray::from(vec![
            Some(&b"\xff\0"[..]),
            None,
            Some(&b"\0"[..]),
            Some(&b""[..]),
        ])
        .into_array(),
    )
    .unwrap()
    .into_array();
    let left = key(&a, &memory);
    let right = key(&b, &memory);
    drop((a, b));
    for row in 0..5 {
        assert_eq!(left.cell(row).unwrap(), right.cell(row).unwrap());
        assert_eq!(hash(&left, row), hash(&right, row));
        assert!(left.equals_at(row, &right, row, true).unwrap());
    }
    assert!(!left.equals_at(2, &right, 2, false).unwrap());
    assert_eq!(left.compare_at(1, &right, 3).unwrap(), Ordering::Less);
    assert_eq!(left.compare_at(3, &right, 0).unwrap(), Ordering::Less);
    assert_eq!(memory.snapshot().reserved_bytes, 8 * 8);
    let text = key(&VarBinViewArray::from_iter_str([""]).into_array(), &memory);
    assert!(left.compare_at(1, &text, 0).is_err());
    assert_ne!(hash(&left, 1), hash(&text, 0));
    drop((left, right, text));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_typed_keys_decimal_equality_is_exact_across_physical_widths() {
    let memory = LiveMemoryPool::new(4096).unwrap();
    let dtype = DecimalDType::new(3, 1);
    let narrow = key(
        &DecimalArray::from_option_iter([Some(-99i16), Some(0), None, Some(99)], dtype)
            .into_array(),
        &memory,
    );
    let wide = key(
        &DecimalArray::from_option_iter([Some(-99i128), Some(0), None, Some(99)], dtype)
            .into_array(),
        &memory,
    );
    for row in 0..4 {
        assert_eq!(narrow.cell(row).unwrap(), wide.cell(row).unwrap());
        assert_eq!(hash(&narrow, row), hash(&wide, row));
    }
    assert_eq!(narrow.compare_at(0, &wide, 3).unwrap(), Ordering::Less);
    let scale = key(
        &DecimalArray::from_option_iter([Some(0i128)], DecimalDType::new(3, 2)).into_array(),
        &memory,
    );
    let precision = key(
        &DecimalArray::from_option_iter([Some(0i128)], DecimalDType::new(4, 1)).into_array(),
        &memory,
    );
    for different in [scale, precision] {
        assert!(narrow.compare_at(1, &different, 0).is_err());
        assert_ne!(hash(&narrow, 1), hash(&different, 0));
    }
    let edge = 10i128.pow(38) - 1;
    let extremes = key(
        &DecimalArray::from_option_iter([Some(-edge), Some(edge)], DecimalDType::new(38, 6))
            .into_array(),
        &memory,
    );
    assert_eq!(
        extremes.compare_at(0, &extremes, 1).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        extremes.cell(1).unwrap(),
        Cell::Decimal(edge, DecimalDType::new(38, 6))
    );
}

#[test]
fn native_typed_keys_invalid_decimal_is_fallible_and_hidden_null_storage_is_ignored() {
    let memory = LiveMemoryPool::new(4096).unwrap();
    let values = DecimalArray::new(
        vec![9i128, 10, i128::MIN].into(),
        DecimalDType::new(1, 0),
        Validity::from_iter([true, true, false]),
    )
    .into_array();
    let column = key(&values, &memory);
    assert_eq!(
        column.cell(0).unwrap(),
        Cell::Decimal(9, DecimalDType::new(1, 0))
    );
    assert!(
        column
            .cell(1)
            .unwrap_err()
            .to_string()
            .contains("declared precision")
    );
    assert_eq!(column.cell(2).unwrap(), Cell::Null);
    assert!(!hash(&column, 2).0);
    assert!(column.cell(3).is_err());
}

#[test]
fn native_typed_keys_temporal_domains_keep_full_ranges_and_logical_identity() {
    let memory = LiveMemoryPool::new(4096).unwrap();
    let days = key(
        &ExtensionArray::new(
            Date::new(TimeUnit::Days, Nullability::Nullable).erased(),
            PrimitiveArray::from_option_iter([
                Some(i32::MIN),
                Some(-1),
                Some(0),
                Some(i32::MAX),
                None,
            ])
            .into_array(),
        )
        .into_array(),
        &memory,
    );
    let micros = key(
        &ExtensionArray::new(
            Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
            PrimitiveArray::from_option_iter([
                Some(i64::MIN),
                Some(-1),
                Some(0),
                Some(i64::MAX),
                None,
            ])
            .into_array(),
        )
        .into_array(),
        &memory,
    );
    assert_eq!(days.cell(0).unwrap(), Cell::Date(i32::MIN));
    assert_eq!(days.cell(3).unwrap(), Cell::Date(i32::MAX));
    assert_eq!(micros.cell(0).unwrap(), Cell::Timestamp(i64::MIN));
    assert_eq!(micros.cell(3).unwrap(), Cell::Timestamp(i64::MAX));
    for column in [&days, &micros] {
        for row in 0..3 {
            assert_eq!(
                column.compare_at(row, column, row + 1).unwrap(),
                Ordering::Less
            );
            assert!(column.equals_at(row, column, row, false).unwrap());
        }
        assert_eq!(column.cell(4).unwrap(), Cell::Null);
    }
    let integers = key(&PrimitiveArray::from_iter([0i64]).into_array(), &memory);
    assert!(days.compare_at(2, &micros, 2).is_err());
    assert!(days.compare_at(2, &integers, 0).is_err());
    assert!(micros.compare_at(2, &integers, 0).is_err());
    assert_ne!(hash(&days, 2), hash(&micros, 2));
    assert_ne!(hash(&micros, 2), hash(&integers, 0));
}

#[test]
fn native_typed_keys_admission_precedes_work_and_releases_failed_reservations() {
    let mut context = vortex::array::legacy_session().create_execution_ctx();
    let cancellation = CancellationToken::default();
    let memory = LiveMemoryPool::new(15).unwrap();
    let binary = VarBinArray::from(vec![Some(&b"\xff"[..]), Some(&b""[..])]).into_array();
    assert!(KeyColumn::new(&binary, &mut context, &memory, &cancellation).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let memory = LiveMemoryPool::new(4096).unwrap();
    for array in [
        DecimalArray::from_option_iter([] as [Option<i128>; 0], DecimalDType::new(39, 0))
            .into_array(),
        ExtensionArray::new(
            Timestamp::new(TimeUnit::Nanoseconds, Nullability::NonNullable).erased(),
            PrimitiveArray::from_iter([] as [i64; 0]).into_array(),
        )
        .into_array(),
        ExtensionArray::new(
            Timestamp::new_with_tz(
                TimeUnit::Microseconds,
                Some("UTC".into()),
                Nullability::NonNullable,
            )
            .erased(),
            PrimitiveArray::from_iter([] as [i64; 0]).into_array(),
        )
        .into_array(),
    ] {
        assert!(KeyColumn::new(&array, &mut context, &memory, &cancellation).is_err());
    }
    cancellation.cancel();
    assert!(KeyColumn::new(&binary, &mut context, &memory, &cancellation).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
