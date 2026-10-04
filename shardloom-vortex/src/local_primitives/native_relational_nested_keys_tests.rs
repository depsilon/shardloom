use super::*;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{
        ChunkedArray, ConstantArray, DictArray, FixedSizeListArray, ListViewArray, StructArray,
        VarBinArray,
    },
    dtype::{FieldNames, Nullability},
    scalar::Scalar,
    validity::Validity,
};

fn key(array: &ArrayRef, memory: &LiveMemoryPool, cancellation: &CancellationToken) -> KeyColumn {
    KeyColumn::new(
        array,
        &mut vortex::array::legacy_session().create_execution_ctx(),
        memory,
        cancellation,
    )
    .unwrap()
}

fn hash(column: &KeyColumn, row: usize) -> (bool, u64) {
    let mut hash = rustc_hash::FxHasher::default();
    (column.hash_into(row, &mut hash).unwrap(), hash.finish())
}

fn oracle_rows() -> Vec<Option<Vec<Option<i64>>>> {
    (0..73)
        .map(|row| {
            (row % 7 != 0).then(|| {
                (0..row % 5)
                    .map(|child| {
                        ((row + child) % 3 != 0)
                            .then_some((i64::from(row) + i64::from(child)) % 9 - 4)
                    })
                    .collect()
            })
        })
        .collect()
}

fn lists(rows: &[Option<Vec<Option<i64>>>], dictionary: bool) -> ArrayRef {
    let mut children = Vec::new();
    let mut offsets = Vec::new();
    let mut lengths = Vec::new();
    for row in rows {
        offsets.push(children.len() as u64);
        lengths.push(row.as_ref().map_or(0, Vec::len) as u64);
        if let Some(values) = row {
            children.extend(values.iter().copied());
        }
    }
    let children = if dictionary {
        let domain = PrimitiveArray::from_option_iter([
            None,
            Some(-4i64),
            Some(-3),
            Some(-2),
            Some(-1),
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
        ])
        .into_array();
        let codes = PrimitiveArray::from_iter(
            children
                .iter()
                .map(|value| value.map_or(0, |value| u8::try_from(value + 5).unwrap())),
        )
        .into_array();
        DictArray::try_new(codes, domain).unwrap().into_array()
    } else {
        PrimitiveArray::from_option_iter(children).into_array()
    };
    ListViewArray::try_new(
        children,
        PrimitiveArray::from_iter(offsets).into_array(),
        PrimitiveArray::from_iter(lengths).into_array(),
        Validity::from_iter(rows.iter().map(Option::is_some)),
    )
    .unwrap()
    .into_array()
}

#[test]
fn native_nested_keys_match_independent_all_pairs_across_dictionary_and_chunks() {
    let rows = oracle_rows();
    let plain = lists(&rows, false);
    let encoded = ChunkedArray::try_new(
        vec![lists(&rows[..31], true), lists(&rows[31..], true)],
        plain.dtype().clone(),
    )
    .unwrap()
    .into_array();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let left = key(&plain, &memory, &CancellationToken::default());
    let right = key(&encoded, &memory, &CancellationToken::default());
    drop((plain, encoded));
    for (i, a) in rows.iter().enumerate() {
        for (j, b) in rows.iter().enumerate() {
            assert_eq!(
                left.compare_at(i, &right, j).unwrap(),
                a.cmp(b),
                "{i} vs {j}"
            );
            assert_eq!(left.equals_at(i, &right, j, true).unwrap(), a == b);
            assert_eq!(
                left.equals_at(i, &right, j, false).unwrap(),
                a.is_some() && b.is_some() && a == b
            );
            if a == b {
                assert_eq!(hash(&left, i), hash(&right, j));
            }
        }
        assert_eq!(hash(&left, i).0, a.is_some());
    }
    assert!(left.cell(0).is_err());
    assert!(left.compare_at(rows.len(), &right, 0).is_err());
    assert!(memory.snapshot().reserved_bytes > 0);
    drop((left, right));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_nested_keys_structs_ignore_hidden_children_and_keep_logical_schema() {
    let child = PrimitiveArray::from_iter([f64::NAN, -0.0, 0.0, 2.0]).into_array();
    let a = StructArray::try_new(
        FieldNames::from(["value"]),
        vec![child],
        4,
        Validity::from_iter([false, true, true, true]),
    )
    .unwrap()
    .into_array();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let column = key(&a, &memory, &CancellationToken::default());
    assert!(!hash(&column, 0).0);
    assert_eq!(column.compare_at(0, &column, 3).unwrap(), Ordering::Less);
    assert_eq!(column.compare_at(1, &column, 2).unwrap(), Ordering::Equal);
    assert_eq!(hash(&column, 1), hash(&column, 2));
    let mut negative = String::new();
    let mut positive = String::new();
    let cancellation = CancellationToken::default();
    column
        .write_exact_key(1, &mut negative, &cancellation)
        .unwrap();
    column
        .write_exact_key(2, &mut positive, &cancellation)
        .unwrap();
    assert_ne!(
        negative, positive,
        "unary identity must preserve signed zero"
    );
    let other = StructArray::try_new(
        FieldNames::from(["different"]),
        vec![PrimitiveArray::from_iter([0.0]).into_array()],
        1,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let other = key(&other, &memory, &CancellationToken::default());
    assert!(column.compare_at(1, &other, 0).is_err());
    assert_ne!(hash(&column, 1), hash(&other, 0));
    let same_nonnullable = StructArray::try_new(
        FieldNames::from(["value"]),
        vec![PrimitiveArray::from_option_iter([Some(0.0)]).into_array()],
        1,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let same = key(&same_nonnullable, &memory, &CancellationToken::default());
    assert_eq!(column.compare_at(1, &same, 0).unwrap(), Ordering::Equal);
    assert_eq!(hash(&column, 1), hash(&same, 0));
}

#[test]
fn native_nested_keys_fixed_lists_preserve_width_child_nulls_and_cancellation() {
    let input = FixedSizeListArray::try_new(
        VarBinArray::from(vec![
            Some("ignored"),
            Some("hidden"),
            None,
            Some("東京"),
            None,
            Some("東京"),
        ])
        .into_array(),
        2,
        Validity::from_iter([false, true, true]),
        3,
    )
    .unwrap()
    .into_array();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let cancellation = CancellationToken::default();
    let column = key(&input, &memory, &cancellation);
    assert!(column.equals_at(1, &column, 2, false).unwrap());
    assert_eq!(hash(&column, 1), hash(&column, 2));
    let different = FixedSizeListArray::try_new(
        VarBinArray::from(vec![None::<&str>]).into_array(),
        1,
        Validity::NonNullable,
        1,
    )
    .unwrap()
    .into_array();
    let different = key(&different, &memory, &CancellationToken::default());
    assert!(column.compare_at(1, &different, 0).is_err());
    cancellation.cancel();
    assert!(column.compare_at(1, &column, 2).is_err());
    assert!(
        column
            .hash_into(1, &mut rustc_hash::FxHasher::default())
            .is_err()
    );
    drop((column, different));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_nested_keys_schema_and_memory_are_admitted_before_provider_execution() {
    let memory = LiveMemoryPool::new(128).unwrap();
    let mut context = vortex::array::legacy_session().create_execution_ctx();
    let small = lists(&[Some(vec![Some(1)])], false);
    assert!(KeyColumn::new(&small, &mut context, &memory, &CancellationToken::default()).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let mut dtype = DType::Utf8(Nullability::Nullable);
    for _ in 0..26 {
        dtype = DType::List(std::sync::Arc::new(dtype), Nullability::Nullable);
    }
    let too_deep = ConstantArray::new(Scalar::null(dtype), 0).into_array();
    let error = KeyColumn::new(
        &too_deep,
        &mut context,
        &memory,
        &CancellationToken::default(),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("depth 24"));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_nested_keys_repeated_overlapping_coordinates_and_parent_dictionaries_are_logical() {
    let input = ListViewArray::try_new(
        PrimitiveArray::from_option_iter([None, Some(2i64), Some(3), Some(4)]).into_array(),
        PrimitiveArray::from_iter([1u64, 1, 0, 3, 4]).into_array(),
        PrimitiveArray::from_iter([2u64, 2, 2, 1, 0]).into_array(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let encoded = DictArray::try_new(
        PrimitiveArray::from_iter([4u8, 2, 0, 3, 1]).into_array(),
        input.clone(),
    )
    .unwrap()
    .into_array();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let cancellation = CancellationToken::default();
    let plain = key(&input, &memory, &cancellation);
    let encoded = key(&encoded, &memory, &cancellation);
    let oracle = [
        vec![Some(2), Some(3)],
        vec![Some(2), Some(3)],
        vec![None, Some(2)],
        vec![Some(4)],
        vec![],
    ];
    for (left, value) in oracle.iter().enumerate() {
        for (right, original) in [4, 2, 0, 3, 1].into_iter().enumerate() {
            assert_eq!(
                plain.compare_at(left, &encoded, right).unwrap(),
                value.cmp(&oracle[original])
            );
            if value == &oracle[original] {
                assert_eq!(hash(&plain, left), hash(&encoded, right));
            }
        }
    }
}

#[test]
fn native_nested_keys_cancel_inside_large_binary_and_unicode_leaves() {
    struct Writer {
        token: CancellationToken,
        bytes: usize,
    }
    impl std::fmt::Write for Writer {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            self.bytes += value.len();
            if self.bytes > 8192 {
                self.token.cancel();
            }
            Ok(())
        }
    }
    let bytes = vec![255u8; 1 << 18];
    let text = "東京🦀".repeat(1 << 15);
    for leaf in [
        VarBinArray::from(vec![bytes.as_slice()]).into_array(),
        VarBinArray::from(vec![text.as_str()]).into_array(),
    ] {
        let input = StructArray::new(
            FieldNames::from(["leaf"]),
            vec![leaf],
            1,
            Validity::NonNullable,
        )
        .into_array();
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let cancellation = CancellationToken::default();
        let column = key(&input, &memory, &cancellation);
        let mut writer = Writer {
            token: cancellation.clone(),
            bytes: 0,
        };
        let error = column
            .write_exact_key(0, &mut writer, &cancellation)
            .unwrap_err();
        assert!(error.to_string().contains("cancel"), "{error}");
        assert!(
            writer.bytes < 16384,
            "cancellation traversed the complete leaf"
        );
        drop(column);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
