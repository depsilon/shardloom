// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Only dictionary boxes are recursively owned by DataType::clone. Every
//! field/container Arc keeps the original shallow clone and pointer identity.

use crate::DataType;

enum Frame<'a> {
    Key { value: &'a DataType },
    Value { key: Box<DataType> },
}

pub(crate) fn clone(dtype: &DataType) -> DataType {
    let mut current = dtype;
    let mut pending = Vec::new();
    'descend: loop {
        let mut value = match current {
            DataType::Dictionary(key, value) => {
                pending.push(Frame::Key { value });
                current = key;
                continue;
            }
            DataType::Null => DataType::Null,
            DataType::Boolean => DataType::Boolean,
            DataType::Int8 => DataType::Int8,
            DataType::Int16 => DataType::Int16,
            DataType::Int32 => DataType::Int32,
            DataType::Int64 => DataType::Int64,
            DataType::UInt8 => DataType::UInt8,
            DataType::UInt16 => DataType::UInt16,
            DataType::UInt32 => DataType::UInt32,
            DataType::UInt64 => DataType::UInt64,
            DataType::Float16 => DataType::Float16,
            DataType::Float32 => DataType::Float32,
            DataType::Float64 => DataType::Float64,
            DataType::Timestamp(unit, timezone) => DataType::Timestamp(*unit, timezone.clone()),
            DataType::Date32 => DataType::Date32,
            DataType::Date64 => DataType::Date64,
            DataType::Time32(unit) => DataType::Time32(*unit),
            DataType::Time64(unit) => DataType::Time64(*unit),
            DataType::Duration(unit) => DataType::Duration(*unit),
            DataType::Interval(unit) => DataType::Interval(*unit),
            DataType::Binary => DataType::Binary,
            DataType::FixedSizeBinary(size) => DataType::FixedSizeBinary(*size),
            DataType::LargeBinary => DataType::LargeBinary,
            DataType::BinaryView => DataType::BinaryView,
            DataType::Utf8 => DataType::Utf8,
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8View => DataType::Utf8View,
            DataType::List(field) => DataType::List(field.clone()),
            DataType::ListView(field) => DataType::ListView(field.clone()),
            DataType::FixedSizeList(field, size) => DataType::FixedSizeList(field.clone(), *size),
            DataType::LargeList(field) => DataType::LargeList(field.clone()),
            DataType::LargeListView(field) => DataType::LargeListView(field.clone()),
            DataType::Struct(fields) => DataType::Struct(fields.clone()),
            DataType::Union(fields, mode) => DataType::Union(fields.clone(), *mode),
            DataType::Decimal32(precision, scale) => DataType::Decimal32(*precision, *scale),
            DataType::Decimal64(precision, scale) => DataType::Decimal64(*precision, *scale),
            DataType::Decimal128(precision, scale) => DataType::Decimal128(*precision, *scale),
            DataType::Decimal256(precision, scale) => DataType::Decimal256(*precision, *scale),
            DataType::Map(field, sorted) => DataType::Map(field.clone(), *sorted),
            DataType::RunEndEncoded(run_ends, values) => {
                DataType::RunEndEncoded(run_ends.clone(), values.clone())
            }
        };
        loop {
            match pending.pop() {
                Some(Frame::Key { value: next }) => {
                    pending.push(Frame::Value {
                        key: Box::new(value),
                    });
                    current = next;
                    continue 'descend;
                }
                Some(Frame::Value { key }) => {
                    value = DataType::Dictionary(key, Box::new(value));
                }
                None => return value,
            }
        }
    }
}
