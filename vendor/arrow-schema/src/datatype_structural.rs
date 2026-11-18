// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Preserve the pinned schema trait contracts without recursively entering
//! nested Field/DataType trait implementations.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::{DataType, Field, FieldRef, UnionFields, UnionMode};

enum Compare<'a> {
    Type(&'a DataType, &'a DataType),
    Field(&'a Field, &'a Field),
    Fields(&'a [FieldRef], &'a [FieldRef]),
    Union(&'a UnionFields, &'a UnionFields, usize),
    FieldTail(&'a Field, &'a Field),
    Size(i32, i32),
    Sorted(bool, bool),
    Mode(UnionMode, UnionMode),
}

pub(crate) fn equal(left: &DataType, right: &DataType) -> bool {
    let mut current = Compare::Type(left, right);
    let mut pending = Vec::new();
    loop {
        match current {
            Compare::Type(a, b) => match (a, b) {
                (DataType::Null, DataType::Null)
                | (DataType::Boolean, DataType::Boolean)
                | (DataType::Int8, DataType::Int8)
                | (DataType::Int16, DataType::Int16)
                | (DataType::Int32, DataType::Int32)
                | (DataType::Int64, DataType::Int64)
                | (DataType::UInt8, DataType::UInt8)
                | (DataType::UInt16, DataType::UInt16)
                | (DataType::UInt32, DataType::UInt32)
                | (DataType::UInt64, DataType::UInt64)
                | (DataType::Float16, DataType::Float16)
                | (DataType::Float32, DataType::Float32)
                | (DataType::Float64, DataType::Float64)
                | (DataType::Date32, DataType::Date32)
                | (DataType::Date64, DataType::Date64)
                | (DataType::Binary, DataType::Binary)
                | (DataType::LargeBinary, DataType::LargeBinary)
                | (DataType::BinaryView, DataType::BinaryView)
                | (DataType::Utf8, DataType::Utf8)
                | (DataType::LargeUtf8, DataType::LargeUtf8)
                | (DataType::Utf8View, DataType::Utf8View) => {}
                (DataType::Timestamp(au, az), DataType::Timestamp(bu, bz)) => {
                    if au != bu || az != bz {
                        return false;
                    }
                }
                (DataType::Time32(a), DataType::Time32(b))
                | (DataType::Time64(a), DataType::Time64(b))
                | (DataType::Duration(a), DataType::Duration(b)) => {
                    if a != b {
                        return false;
                    }
                }
                (DataType::Interval(a), DataType::Interval(b)) => {
                    if a != b {
                        return false;
                    }
                }
                (DataType::FixedSizeBinary(a), DataType::FixedSizeBinary(b)) => {
                    if a != b {
                        return false;
                    }
                }
                (DataType::Decimal32(ap, as_), DataType::Decimal32(bp, bs))
                | (DataType::Decimal64(ap, as_), DataType::Decimal64(bp, bs))
                | (DataType::Decimal128(ap, as_), DataType::Decimal128(bp, bs))
                | (DataType::Decimal256(ap, as_), DataType::Decimal256(bp, bs)) => {
                    if ap != bp || as_ != bs {
                        return false;
                    }
                }
                (DataType::List(a), DataType::List(b))
                | (DataType::ListView(a), DataType::ListView(b))
                | (DataType::LargeList(a), DataType::LargeList(b))
                | (DataType::LargeListView(a), DataType::LargeListView(b)) => {
                    if !Arc::ptr_eq(a, b) {
                        current = Compare::Field(a, b);
                        continue;
                    }
                }
                (DataType::FixedSizeList(a, an), DataType::FixedSizeList(b, bn)) => {
                    pending.push(Compare::Size(*an, *bn));
                    current = Compare::Field(a, b);
                    continue;
                }
                (DataType::Map(a, an), DataType::Map(b, bn)) => {
                    pending.push(Compare::Sorted(*an, *bn));
                    current = Compare::Field(a, b);
                    continue;
                }
                (DataType::Struct(a), DataType::Struct(b)) => {
                    if a.len() != b.len() {
                        return false;
                    }
                    if !a.same_owner(b) {
                        current = Compare::Fields(a, b);
                        continue;
                    }
                }
                (DataType::Union(a, am), DataType::Union(b, bm)) => {
                    if a.len() != b.len() {
                        return false;
                    }
                    pending.push(Compare::Mode(*am, *bm));
                    if !a.same_owner(b) {
                        current = Compare::Union(a, b, 0);
                        continue;
                    }
                }
                (DataType::Dictionary(ak, av), DataType::Dictionary(bk, bv)) => {
                    pending.push(Compare::Type(av, bv));
                    current = Compare::Type(ak, bk);
                    continue;
                }
                (DataType::RunEndEncoded(ak, av), DataType::RunEndEncoded(bk, bv)) => {
                    pending.push(Compare::Field(av, bv));
                    current = Compare::Field(ak, bk);
                    continue;
                }
                _ => return false,
            },
            Compare::Field(a, b) => {
                if !std::ptr::eq(a, b) {
                    if a.name() != b.name() {
                        return false;
                    }
                    pending.push(Compare::FieldTail(a, b));
                    current = Compare::Type(a.data_type(), b.data_type());
                    continue;
                }
            }
            Compare::Fields(a, b) => {
                if let Some((first, tail)) = a.split_first() {
                    pending.push(Compare::Fields(tail, &b[1..]));
                    current = Compare::Field(first, &b[0]);
                    continue;
                }
            }
            Compare::Union(a, b, index) => {
                if index < a.len() {
                    if a[index].0 != b[index].0 {
                        return false;
                    }
                    pending.push(Compare::Union(a, b, index + 1));
                    current = Compare::Field(&a[index].1, &b[index].1);
                    continue;
                }
            }
            Compare::FieldTail(a, b) => {
                if a.is_nullable() != b.is_nullable() || a.metadata() != b.metadata() {
                    return false;
                }
            }
            Compare::Size(a, b) => {
                if a != b {
                    return false;
                }
            }
            Compare::Sorted(a, b) => {
                if a != b {
                    return false;
                }
            }
            Compare::Mode(a, b) => {
                if a != b {
                    return false;
                }
            }
        }
        match pending.pop() {
            Some(next) => current = next,
            None => return true,
        }
    }
}

// Declaration order of the pinned public enum, as used by its derived Ord.
fn rank(dtype: &DataType) -> u8 {
    match dtype {
        DataType::Null => 0,
        DataType::Boolean => 1,
        DataType::Int8 => 2,
        DataType::Int16 => 3,
        DataType::Int32 => 4,
        DataType::Int64 => 5,
        DataType::UInt8 => 6,
        DataType::UInt16 => 7,
        DataType::UInt32 => 8,
        DataType::UInt64 => 9,
        DataType::Float16 => 10,
        DataType::Float32 => 11,
        DataType::Float64 => 12,
        DataType::Timestamp(_, _) => 13,
        DataType::Date32 => 14,
        DataType::Date64 => 15,
        DataType::Time32(_) => 16,
        DataType::Time64(_) => 17,
        DataType::Duration(_) => 18,
        DataType::Interval(_) => 19,
        DataType::Binary => 20,
        DataType::FixedSizeBinary(_) => 21,
        DataType::LargeBinary => 22,
        DataType::BinaryView => 23,
        DataType::Utf8 => 24,
        DataType::LargeUtf8 => 25,
        DataType::Utf8View => 26,
        DataType::List(_) => 27,
        DataType::ListView(_) => 28,
        DataType::FixedSizeList(_, _) => 29,
        DataType::LargeList(_) => 30,
        DataType::LargeListView(_) => 31,
        DataType::Struct(_) => 32,
        DataType::Union(_, _) => 33,
        DataType::Dictionary(_, _) => 34,
        DataType::Decimal32(_, _) => 35,
        DataType::Decimal64(_, _) => 36,
        DataType::Decimal128(_, _) => 37,
        DataType::Decimal256(_, _) => 38,
        DataType::Map(_, _) => 39,
        DataType::RunEndEncoded(_, _) => 40,
    }
}

pub(crate) fn compare(left: &DataType, right: &DataType) -> Ordering {
    let mut current = Compare::Type(left, right);
    let mut pending = Vec::new();
    macro_rules! compare {
        ($value:expr) => {
            match $value {
                Ordering::Equal => {}
                other => return other,
            }
        };
    }
    loop {
        match current {
            Compare::Type(a, b) => {
                compare!(rank(a).cmp(&rank(b)));
                match (a, b) {
                    (DataType::Timestamp(au, az), DataType::Timestamp(bu, bz)) => {
                        compare!(au.cmp(bu));
                        compare!(az.cmp(bz));
                    }
                    (DataType::Time32(a), DataType::Time32(b))
                    | (DataType::Time64(a), DataType::Time64(b))
                    | (DataType::Duration(a), DataType::Duration(b)) => compare!(a.cmp(b)),
                    (DataType::Interval(a), DataType::Interval(b)) => compare!(a.cmp(b)),
                    (DataType::FixedSizeBinary(a), DataType::FixedSizeBinary(b)) => {
                        compare!(a.cmp(b))
                    }
                    (DataType::Decimal32(ap, as_), DataType::Decimal32(bp, bs))
                    | (DataType::Decimal64(ap, as_), DataType::Decimal64(bp, bs))
                    | (DataType::Decimal128(ap, as_), DataType::Decimal128(bp, bs))
                    | (DataType::Decimal256(ap, as_), DataType::Decimal256(bp, bs)) => {
                        compare!(ap.cmp(bp));
                        compare!(as_.cmp(bs));
                    }
                    (DataType::List(a), DataType::List(b))
                    | (DataType::ListView(a), DataType::ListView(b))
                    | (DataType::LargeList(a), DataType::LargeList(b))
                    | (DataType::LargeListView(a), DataType::LargeListView(b)) => {
                        current = Compare::Field(a, b);
                        continue;
                    }
                    (DataType::FixedSizeList(a, an), DataType::FixedSizeList(b, bn)) => {
                        pending.push(Compare::Size(*an, *bn));
                        current = Compare::Field(a, b);
                        continue;
                    }
                    (DataType::Map(a, an), DataType::Map(b, bn)) => {
                        pending.push(Compare::Sorted(*an, *bn));
                        current = Compare::Field(a, b);
                        continue;
                    }
                    (DataType::Struct(a), DataType::Struct(b)) => {
                        current = Compare::Fields(a, b);
                        continue;
                    }
                    (DataType::Union(a, am), DataType::Union(b, bm)) => {
                        pending.push(Compare::Mode(*am, *bm));
                        current = Compare::Union(a, b, 0);
                        continue;
                    }
                    (DataType::Dictionary(ak, av), DataType::Dictionary(bk, bv)) => {
                        pending.push(Compare::Type(av, bv));
                        current = Compare::Type(ak, bk);
                        continue;
                    }
                    (DataType::RunEndEncoded(ak, av), DataType::RunEndEncoded(bk, bv)) => {
                        pending.push(Compare::Field(av, bv));
                        current = Compare::Field(ak, bk);
                        continue;
                    }
                    // Equal-ranked variants without owning/scalar fields.
                    _ => {}
                }
            }
            Compare::Field(a, b) => {
                if !std::ptr::eq(a, b) {
                    compare!(a.name().cmp(b.name()));
                    pending.push(Compare::FieldTail(a, b));
                    current = Compare::Type(a.data_type(), b.data_type());
                    continue;
                }
            }
            Compare::Fields(a, b) => match (a.split_first(), b.split_first()) {
                (Some((a, at)), Some((b, bt))) => {
                    pending.push(Compare::Fields(at, bt));
                    current = Compare::Field(a, b);
                    continue;
                }
                _ => compare!(a.len().cmp(&b.len())),
            },
            Compare::Union(a, b, index) => {
                if index < a.len() && index < b.len() {
                    compare!(a[index].0.cmp(&b[index].0));
                    pending.push(Compare::Union(a, b, index + 1));
                    current = Compare::Field(&a[index].1, &b[index].1);
                    continue;
                }
                compare!(a.len().cmp(&b.len()));
            }
            Compare::FieldTail(a, b) => {
                compare!(a.is_nullable().cmp(&b.is_nullable()));
                compare!(metadata_cmp(a.metadata(), b.metadata()));
            }
            Compare::Size(a, b) => compare!(a.cmp(&b)),
            Compare::Sorted(a, b) => compare!(a.cmp(&b)),
            Compare::Mode(a, b) => compare!(a.cmp(&b)),
        }
        match pending.pop() {
            Some(next) => current = next,
            None => return Ordering::Equal,
        }
    }
}

pub(crate) fn metadata_cmp(
    left: &std::collections::HashMap<String, String>,
    right: &std::collections::HashMap<String, String>,
) -> Ordering {
    // Preserve Field::cmp's sorted union of keys, including duplicate keys and
    // its original ordering for a key present on only one side.
    let mut keys: Vec<&String> = left.keys().chain(right.keys()).collect();
    keys.sort();
    for key in keys {
        match (left.get(key), right.get(key)) {
            (None, None) => {}
            (Some(_), None) => return Ordering::Less,
            (None, Some(_)) => return Ordering::Greater,
            (Some(a), Some(b)) => match a.cmp(b) {
                Ordering::Equal => {}
                other => return other,
            },
        }
    }
    Ordering::Equal
}

enum HashWork<'a> {
    Type(&'a DataType),
    Field(&'a Field),
    Fields(&'a [FieldRef]),
    Union(&'a UnionFields, usize),
    FieldTail(&'a Field),
    Size(i32),
    Sorted(bool),
    Mode(UnionMode),
}

pub(crate) fn hash<H: Hasher>(dtype: &DataType, state: &mut H) {
    let mut current = HashWork::Type(dtype);
    let mut pending = Vec::new();
    loop {
        match current {
            HashWork::Type(dtype) => {
                // This is the same discriminant Hash operation as derive(Hash),
                // without inventing an on-wire tag or a new hashing profile.
                std::mem::discriminant(dtype).hash(state);
                match dtype {
                    DataType::Timestamp(unit, timezone) => {
                        unit.hash(state);
                        timezone.hash(state);
                    }
                    DataType::Time32(unit) | DataType::Time64(unit) | DataType::Duration(unit) => {
                        unit.hash(state)
                    }
                    DataType::Interval(unit) => unit.hash(state),
                    DataType::FixedSizeBinary(size) => size.hash(state),
                    DataType::Decimal32(precision, scale)
                    | DataType::Decimal64(precision, scale)
                    | DataType::Decimal128(precision, scale)
                    | DataType::Decimal256(precision, scale) => {
                        precision.hash(state);
                        scale.hash(state);
                    }
                    DataType::List(field)
                    | DataType::ListView(field)
                    | DataType::LargeList(field)
                    | DataType::LargeListView(field) => {
                        current = HashWork::Field(field);
                        continue;
                    }
                    DataType::FixedSizeList(field, size) => {
                        pending.push(HashWork::Size(*size));
                        current = HashWork::Field(field);
                        continue;
                    }
                    DataType::Map(field, sorted) => {
                        pending.push(HashWork::Sorted(*sorted));
                        current = HashWork::Field(field);
                        continue;
                    }
                    DataType::Struct(fields) => {
                        fields.len().hash(state);
                        current = HashWork::Fields(fields);
                        continue;
                    }
                    DataType::Union(fields, mode) => {
                        fields.len().hash(state);
                        pending.push(HashWork::Mode(*mode));
                        current = HashWork::Union(fields, 0);
                        continue;
                    }
                    DataType::Dictionary(key, value) => {
                        pending.push(HashWork::Type(value));
                        current = HashWork::Type(key);
                        continue;
                    }
                    DataType::RunEndEncoded(run_ends, values) => {
                        pending.push(HashWork::Field(values));
                        current = HashWork::Field(run_ends);
                        continue;
                    }
                    DataType::Null
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Binary
                    | DataType::LargeBinary
                    | DataType::BinaryView
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View => {}
                }
            }
            HashWork::Field(field) => {
                field.name().hash(state);
                pending.push(HashWork::FieldTail(field));
                current = HashWork::Type(field.data_type());
                continue;
            }
            HashWork::Fields(fields) => {
                if let Some((first, tail)) = fields.split_first() {
                    pending.push(HashWork::Fields(tail));
                    current = HashWork::Field(first);
                    continue;
                }
            }
            HashWork::Union(fields, index) => {
                if let Some((id, field)) = fields.get(index) {
                    id.hash(state);
                    pending.push(HashWork::Union(fields, index + 1));
                    current = HashWork::Field(field);
                    continue;
                }
            }
            HashWork::FieldTail(field) => {
                field.is_nullable().hash(state);
                hash_metadata(field.metadata(), state);
            }
            HashWork::Size(size) => size.hash(state),
            HashWork::Sorted(sorted) => sorted.hash(state),
            HashWork::Mode(mode) => mode.hash(state),
        }
        match pending.pop() {
            Some(next) => current = next,
            None => return,
        }
    }
}

pub(crate) fn hash_metadata<H: Hasher>(
    metadata: &std::collections::HashMap<String, String>,
    state: &mut H,
) {
    let mut keys: Vec<&String> = metadata.keys().collect();
    keys.sort();
    for key in keys {
        key.hash(state);
        metadata.get(key).expect("key valid").hash(state);
    }
}

#[cfg(test)]
#[path = "datatype_structural_tests.rs"]
pub(crate) mod tests;
