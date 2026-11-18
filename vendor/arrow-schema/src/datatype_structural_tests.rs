// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::structural_reference as original;
use crate::traversal_test_utils::small_stack;
use crate::{DataType, Field, Fields, IntervalUnit, TimeUnit, UnionFields, UnionMode};

#[derive(Debug, Default, PartialEq)]
struct HashTrace(Vec<Vec<u8>>);

impl Hasher for HashTrace {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0.push(bytes.to_vec());
    }
}

fn hash_trace(value: &impl Hash) -> HashTrace {
    let mut trace = HashTrace::default();
    value.hash(&mut trace);
    trace
}

#[allow(deprecated)]
pub(crate) fn cases() -> Vec<DataType> {
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let mut cases = vec![
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("America/Chicago".into())),
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Duration(TimeUnit::Millisecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::Binary,
        DataType::FixedSizeBinary(1),
        DataType::FixedSizeBinary(7),
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::List(field.clone()),
        DataType::ListView(field.clone()),
        DataType::FixedSizeList(field.clone(), 0),
        DataType::FixedSizeList(field.clone(), 4),
        DataType::LargeList(field.clone()),
        DataType::LargeListView(field.clone()),
        DataType::Struct(Fields::empty()),
        DataType::Struct(Fields::from([field.clone()])),
        DataType::Union(UnionFields::empty(), UnionMode::Dense),
        DataType::Union(
            UnionFields::try_new([7], [field.clone()]).unwrap(),
            UnionMode::Dense,
        ),
        DataType::Union(
            UnionFields::try_new([7], [field.clone()]).unwrap(),
            UnionMode::Sparse,
        ),
        DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::List(field.clone())),
        ),
        DataType::Decimal32(9, -3),
        DataType::Decimal32(8, 3),
        DataType::Decimal64(18, 2),
        DataType::Decimal128(38, 9),
        DataType::Decimal256(76, 12),
        DataType::Map(field.clone(), false),
        DataType::Map(field.clone(), true),
        DataType::RunEndEncoded(field.clone(), field.clone()),
    ];
    for name in ["item", "named"] {
        for nullable in [false, true] {
            for (dict_id, ordered) in [(0, false), (42, false), (42, true)] {
                for metadata in [
                    HashMap::new(),
                    HashMap::from([("a".into(), "x".into())]),
                    HashMap::from([("a".into(), "y".into()), ("z".into(), "q".into())]),
                ] {
                    let field = Arc::new(
                        Field::new_dict(name, DataType::Int32, nullable, dict_id, ordered)
                            .with_metadata(metadata),
                    );
                    cases.push(DataType::List(field.clone()));
                    cases.push(DataType::FixedSizeList(field.clone(), 7));
                    cases.push(DataType::Struct(Fields::from([field.clone(), field])));
                }
            }
        }
    }
    let left = Arc::new(Field::new("x", DataType::List(field.clone()), false));
    let right = Arc::new(Field::new("y", DataType::FixedSizeList(field, 2), true));
    cases.push(DataType::Union(
        UnionFields::try_new([3, 1], [left.clone(), right.clone()]).unwrap(),
        UnionMode::Dense,
    ));
    cases.push(DataType::Union(
        UnionFields::try_new([1, 3], [right.clone(), left.clone()]).unwrap(),
        UnionMode::Dense,
    ));
    cases.push(DataType::Struct(Fields::from([
        left.clone(),
        right.clone(),
    ])));
    cases.push(DataType::Struct(Fields::from([right, left])));
    let aliases = cases.clone();
    cases.extend(aliases);
    cases
}

#[test]
fn schema_traits_match_the_frozen_original() {
    let cases = cases();
    assert_eq!(
        cases.iter().map(super::rank).collect::<HashSet<_>>().len(),
        41
    );
    let originals: Vec<_> = cases.iter().map(original::DataType::from).collect();
    for (index, (case, old)) in cases.iter().zip(&originals).enumerate() {
        assert_eq!(case.is_nested(), old.is_nested(), "nested {index}");
        assert_eq!(
            hash_trace(case),
            hash_trace(old),
            "hash writes for fixture {index}"
        );
        for (other_index, (other, old_other)) in cases.iter().zip(&originals).enumerate() {
            assert_eq!(
                case == other,
                old == old_other,
                "equality {index}/{other_index}"
            );
            assert_eq!(
                case.cmp(other),
                old.cmp(old_other),
                "ordering {index}/{other_index}"
            );
            assert_eq!(
                case.equals_datatype(other),
                old.equals_datatype(old_other),
                "type-only equality {index}/{other_index}"
            );
            assert_eq!(
                case.contains(other),
                old.contains(old_other),
                "contains {index}/{other_index}"
            );
        }
    }
}

pub(crate) fn deep(mut dtype: DataType, depth: usize) -> DataType {
    for level in 0..depth {
        dtype = match level % 8 {
            0 => DataType::List(Arc::new(Field::new("item", dtype, true))),
            1 => DataType::FixedSizeList(Arc::new(Field::new("item", dtype, false)), 3),
            2 => DataType::Struct(Fields::from([Arc::new(Field::new("member", dtype, true))])),
            3 => DataType::Dictionary(Box::new(DataType::Int16), Box::new(dtype)),
            4 => DataType::RunEndEncoded(
                Arc::new(Field::new("ends", DataType::Int32, false)),
                Arc::new(Field::new("values", dtype, true)),
            ),
            5 => DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from([
                        Arc::new(Field::new("key", DataType::Int64, false)),
                        Arc::new(Field::new("value", dtype, true)),
                    ])),
                    false,
                )),
                true,
            ),
            6 => DataType::Union(
                UnionFields::try_new([5], [Field::new("choice", dtype, true)]).unwrap(),
                UnionMode::Sparse,
            ),
            _ => DataType::ListView(Arc::new(Field::new("item", dtype, false))),
        };
    }
    dtype
}

#[test]
fn deep_schema_comparison_and_hashing() {
    small_stack(
        "datatype_structural::tests::deep_schema_comparison_and_hashing",
        || {
            let a = deep(DataType::Int64, 8_192);
            let b = deep(DataType::Int64, 8_192);
            let c = deep(DataType::Int32, 8_192);
            assert!(a == b);
            assert!(a.equals_datatype(&b));
            assert!(a.contains(&b));
            assert_eq!(a.cmp(&b), Ordering::Equal);
            assert!(a != c);
            assert!(!a.equals_datatype(&c));
            assert!(!a.contains(&c));
            assert_eq!(a.cmp(&c), Ordering::Greater);
            assert_eq!(c.cmp(&a), Ordering::Less);
            assert!(hash_trace(&a) == hash_trace(&b));
            assert!(hash_trace(&a) != hash_trace(&c));
        },
    );
}

#[test]
fn wide_schema_last_sibling_and_shared_subtrees() {
    small_stack(
        "datatype_structural::tests::wide_schema_last_sibling_and_shared_subtrees",
        || {
            let fields: Vec<_> = (0..4_096)
                .map(|index| Arc::new(Field::new(format!("f{index}"), DataType::Int32, false)))
                .collect();
            let mut different = fields.clone();
            different[4_095] = Arc::new(different[4_095].as_ref().clone().with_nullable(true));
            let a = DataType::Struct(Fields::from(fields));
            let b = DataType::Struct(Fields::from(different));
            assert!(a != b);
            assert!(!a.equals_datatype(&b));
            assert!(!a.contains(&b));
            assert!(b.contains(&a));
            assert_eq!(a.cmp(&b), Ordering::Less);
            assert!(hash_trace(&a) != hash_trace(&b));

            let mut a = DataType::Int64;
            let mut b = DataType::Int64;
            for _ in 0..12 {
                let shared = Arc::new(Field::new("item", a, true));
                a = DataType::Struct(Fields::from([shared.clone(), shared]));
                b = DataType::Struct(Fields::from([
                    Arc::new(Field::new("item", b.clone(), true)),
                    Arc::new(Field::new("item", b, true)),
                ]));
            }
            assert!(a == b);
            assert_eq!(a.cmp(&b), Ordering::Equal);
            assert!(hash_trace(&a) == hash_trace(&b));
        },
    );
}
