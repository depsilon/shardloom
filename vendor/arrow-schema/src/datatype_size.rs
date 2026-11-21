// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Logical schema footprint, retaining the pinned counting conventions plus
//! the actual field-container and opaque-owner-vector storage introduced here.

use crate::{DataType, Field, FieldRef, Fields, UnionFields};

enum Work<'a> {
    Type(&'a DataType),
    Field(&'a Field),
    Fields(&'a [FieldRef]),
    Union(&'a UnionFields, usize),
}

pub(crate) fn dtype(value: &DataType) -> usize {
    size(Work::Type(value), 0)
}

pub(crate) fn field(value: &Field) -> usize {
    size(Work::Field(value), 0)
}

pub(crate) fn fields(value: &Fields) -> usize {
    size(Work::Fields(value), value.storage_size())
}

pub(crate) fn union(value: &UnionFields) -> usize {
    size(Work::Union(value, 0), value.storage_size())
}

fn size(mut current: Work<'_>, mut total: usize) -> usize {
    let mut pending = Vec::new();
    loop {
        match current {
            Work::Type(value) => {
                total += std::mem::size_of_val(value);
                match value {
                    DataType::Timestamp(_, timezone) => {
                        total += timezone.as_ref().map_or(0, |s| s.len());
                    }
                    DataType::List(field)
                    | DataType::ListView(field)
                    | DataType::FixedSizeList(field, _)
                    | DataType::LargeList(field)
                    | DataType::LargeListView(field)
                    | DataType::Map(field, _) => {
                        current = Work::Field(field);
                        continue;
                    }
                    DataType::Struct(fields) => {
                        total += fields.storage_size();
                        current = Work::Fields(fields);
                        continue;
                    }
                    DataType::Union(fields, _) => {
                        total += fields.storage_size();
                        current = Work::Union(fields, 0);
                        continue;
                    }
                    DataType::Dictionary(key, value) => {
                        pending.push(Work::Type(value));
                        current = Work::Type(key);
                        continue;
                    }
                    DataType::RunEndEncoded(ends, values) => {
                        // Preserve the pinned RunEndEncoded convention.
                        total -= 2 * std::mem::size_of::<FieldRef>();
                        pending.push(Work::Field(values));
                        current = Work::Field(ends);
                        continue;
                    }
                    _ => {}
                }
            }
            Work::Field(value) => {
                total += value.storage_size();
                current = Work::Type(value.data_type());
                continue;
            }
            Work::Fields(values) => {
                if let Some((first, tail)) = values.split_first() {
                    pending.push(Work::Fields(tail));
                    current = Work::Field(first);
                    continue;
                }
            }
            Work::Union(values, index) => {
                if let Some((_, first)) = values.get(index) {
                    pending.push(Work::Union(values, index + 1));
                    current = Work::Field(first);
                    continue;
                }
            }
        }
        match pending.pop() {
            Some(next) => current = next,
            None => return total,
        }
    }
}
