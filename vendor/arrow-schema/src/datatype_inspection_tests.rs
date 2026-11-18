// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

use crate::datatype_structural::tests::{cases, deep};
use crate::retained::FieldStorage;
use crate::structural_reference as original;
use crate::traversal_test_utils::small_stack;
use crate::{DataType, Field, FieldRef, Fields, UnionFields, UnionMode};

// This bounded reference delta is separate from the production iterative
// traversal. Reference fixtures have exact String capacity and no credits.
fn footprint_delta(dtype: &DataType) -> i128 {
    fn field(field: &Field) -> i128 {
        size_of::<Field>() as i128
            - size_of::<DataType>() as i128
            - size_of::<original::Field>() as i128
            + size_of::<original::DataType>() as i128
            + footprint_delta(field.data_type())
    }
    size_of::<DataType>() as i128 - size_of::<original::DataType>() as i128
        + match dtype {
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::ListView(f)
            | DataType::LargeListView(f)
            | DataType::FixedSizeList(f, _)
            | DataType::Map(f, _) => field(f),
            DataType::Struct(fs) => {
                fs.storage_size() as i128 - (fs.len() * size_of::<FieldRef>()) as i128
                    + fs.iter().map(|f| field(f)).sum::<i128>()
            }
            DataType::Union(fs, _) => {
                fs.storage_size() as i128 - (fs.len() * size_of::<(i8, FieldRef)>()) as i128
                    + fs.iter().map(|(_, f)| field(f)).sum::<i128>()
            }
            DataType::Dictionary(k, v) => footprint_delta(k) + footprint_delta(v),
            DataType::RunEndEncoded(k, v) => field(k) + field(v),
            _ => 0,
        }
}

#[test]
fn footprint_preserves_original_formula_with_explicit_new_storage() {
    for (index, dtype) in cases().iter().enumerate() {
        assert_eq!(
            dtype.size() as i128 - footprint_delta(dtype),
            original::DataType::from(dtype).size() as i128,
            "footprint fixture {index}"
        );
    }
    let owner_slot = size_of::<Arc<dyn std::any::Any + Send + Sync>>();
    let mut name = String::with_capacity(91);
    name.push_str("field");
    let mut metadata = HashMap::with_capacity(7);
    let mut key = String::with_capacity(37);
    key.push('k');
    let mut value = String::with_capacity(59);
    value.push('v');
    let metadata_bytes =
        metadata.capacity() * size_of::<(String, String)>() + key.capacity() + value.capacity();
    metadata.insert(key, value);
    let field_bytes = size_of::<Field>() + name.capacity() + metadata_bytes;
    let field = Field::new(name, DataType::Int32, false).with_metadata(metadata);
    assert_eq!(field.size(), field_bytes);
    let field = Arc::new(field.with_retained_owner(Arc::new([0_u8; 4096])));
    // Opaque contents and Arc allocator headers are intentionally not inferred.
    assert_eq!(field.size(), field_bytes + owner_slot);

    let mut values = Vec::with_capacity(11);
    values.push(field.clone());
    let expected = size_of::<FieldStorage<FieldRef>>()
        + values.capacity() * size_of::<FieldRef>()
        + field.size()
        + owner_slot;
    let fields = Fields::from(values).with_retained_owner(Arc::new(()));
    assert_eq!(fields.size(), expected);
    assert_eq!(
        DataType::Struct(fields).size(),
        size_of::<DataType>() + expected
    );
    assert_eq!(Fields::empty().size(), size_of::<FieldStorage<FieldRef>>());

    // try_from_fields explicitly reserves all 128 nonnegative tags.
    let union = UnionFields::try_from_fields([field.clone()])
        .unwrap()
        .with_retained_owner(Arc::new(()));
    let expected = size_of::<FieldStorage<(i8, FieldRef)>>()
        + 128 * size_of::<(i8, FieldRef)>()
        + field.size()
        + owner_slot;
    assert_eq!(union.size(), expected);
    assert_eq!(
        DataType::Union(union, UnionMode::Sparse).size(),
        size_of::<DataType>() + expected
    );
    assert_eq!(
        UnionFields::empty().size(),
        size_of::<FieldStorage<(i8, FieldRef)>>()
    );
}

#[test]
fn relation_rules_keep_union_direction_and_run_end_equality() {
    let nullable = Arc::new(Field::new("item", DataType::Int32, true));
    let required = Arc::new(Field::new("item", DataType::Int32, false));
    let left = DataType::Union(
        UnionFields::try_new([2], [nullable.clone()]).unwrap(),
        UnionMode::Dense,
    );
    let right = DataType::Union(
        UnionFields::try_new([7, 2], [required.clone(), required.clone()]).unwrap(),
        UnionMode::Dense,
    );
    assert!(left.contains(&right));
    assert!(!right.contains(&left));
    assert!(!left.equals_datatype(&right));

    let left = DataType::RunEndEncoded(required.clone(), nullable);
    let right = DataType::RunEndEncoded(required.clone(), required);
    assert!(!left.contains(&right));
    assert!(!left.equals_datatype(&right));
}

#[test]
fn deep_compatibility_classification_and_size() {
    small_stack(
        "datatype_inspection_tests::deep_compatibility_classification_and_size",
        || {
            let a = deep(DataType::Int64, 8_192);
            let b = deep(DataType::Int64, 8_192);
            assert!(a.is_nested());
            assert_eq!(a.size(), b.size());
            assert!(a.size() > 8_192 * size_of::<DataType>());
            let mut a = DataType::Int64;
            let mut b = DataType::Int64;
            let mut nested = DataType::new_list(DataType::Int32, false);
            for level in 0..8_192 {
                if level % 2 == 0 {
                    a = DataType::Dictionary(Box::new(DataType::Int8), Box::new(a));
                    nested = DataType::Dictionary(Box::new(DataType::Int8), Box::new(nested));
                } else {
                    a = DataType::RunEndEncoded(
                        Arc::new(Field::new("ends", DataType::Int16, false)),
                        Arc::new(Field::new("values", a, false)),
                    );
                    nested = DataType::RunEndEncoded(
                        Arc::new(Field::new("ends", DataType::Int16, false)),
                        Arc::new(Field::new("values", nested, false)),
                    );
                }
                b = DataType::Dictionary(Box::new(b), Box::new(DataType::Int8));
            }
            assert!(!a.is_nested());
            assert!(nested.is_nested());
            // Dictionary keys still contribute their full footprint and relation.
            assert_eq!(b.size(), (1 + 2 * 8_192) * size_of::<DataType>());
            assert!(b.equals_datatype(&b.clone()));
            assert!(b.contains(&b.clone()));
            let mut required = DataType::Int32;
            let mut nullable = DataType::Int32;
            for _ in 0..8_192 {
                required = DataType::new_list(required, false);
                nullable = DataType::new_list(nullable, true);
            }
            assert!(nullable.contains(&required));
            assert!(!required.contains(&nullable));
            assert!(!nullable.equals_datatype(&required));
        },
    );
}
