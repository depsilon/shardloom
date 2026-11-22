// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Module containing functionality to compute array equality.
//! This module uses [ArrayData] and does not
//! depend on dynamic casting of `Array`.

use crate::data::ArrayData;
use arrow_buffer::{IntervalMonthDayNano, i256};
use arrow_schema::{DataType, IntervalUnit};
use half::f16;

mod boolean;
mod byte_view;
mod dictionary;
mod fixed_binary;
mod fixed_list;
mod list;
mod list_view;
mod null;
mod primitive;
mod run;
mod structure;
mod union;
mod utils;
mod variable_size;
#[cfg(test)]
mod equal_tests;

// these methods assume the same type, len and null count.
// For this reason, they are not exposed and are instead used
// to build the generic functions below (`equal`).
use self::run::{RunEqualStepper, push_run_equal, step_run_equal};
use crate::equal::list_view::{push_list_view_equal, step_list_view_rows_kind};
use boolean::boolean_equal;
use byte_view::byte_view_equal;
use dictionary::{DictionaryKeyKind, push_dictionary_equal, step_dictionary_rows};
use fixed_binary::fixed_binary_equal;
use fixed_list::{push_fixed_list_equal, step_fixed_list_null_rows};
use list::{push_list_equal_kind, step_list_null_rows_kind};
use null::null_equal;
use primitive::primitive_equal;
use structure::{push_struct_equal, step_struct_children, step_struct_null_rows};
use union::{push_union_equal, step_dense_union_rows, step_sparse_union_children};
use variable_size::variable_sized_equal;

#[derive(Clone, Copy)]
struct EqualSlice<'a> {
    lhs: &'a ArrayData,
    rhs: &'a ArrayData,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
}

enum EqualFrame<'a> {
    Range(EqualSlice<'a>),
    StructChildren {
        slice: EqualSlice<'a>,
        child_idx: usize,
    },
    StructNullRows {
        slice: EqualSlice<'a>,
        row_idx: usize,
        child_idx: usize,
    },
    ListNullRows {
        slice: EqualSlice<'a>,
        row_idx: usize,
        is_large: bool,
    },
    ListViewRows {
        slice: EqualSlice<'a>,
        row_idx: usize,
        has_nulls: bool,
        is_large: bool,
    },
    FixedListNullRows {
        slice: EqualSlice<'a>,
        size: usize,
        row_idx: usize,
    },
    DictionaryRows {
        slice: EqualSlice<'a>,
        row_idx: usize,
        has_nulls: bool,
        key_kind: DictionaryKeyKind,
    },
    DenseUnionRows {
        slice: EqualSlice<'a>,
        row_idx: usize,
    },
    SparseUnionChildren {
        slice: EqualSlice<'a>,
        child_idx: usize,
    },
    RunEndStepper(RunEqualStepper<'a>),
}

/// Compares the values of two [ArrayData] starting at `lhs_start` and `rhs_start` respectively
/// for `len` slots, pushing any nested child comparisons onto `stack`.
#[inline]
fn equal_values_step<'a>(
    slice: EqualSlice<'a>,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    let EqualSlice {
        lhs,
        rhs,
        lhs_start,
        rhs_start,
        len,
    } = slice;
    match lhs.data_type() {
        DataType::Null => null_equal(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Boolean => boolean_equal(lhs, rhs, lhs_start, rhs_start, len),
        DataType::UInt8 => primitive_equal::<u8>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::UInt16 => primitive_equal::<u16>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::UInt32 => primitive_equal::<u32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::UInt64 => primitive_equal::<u64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Int8 => primitive_equal::<i8>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Int16 => primitive_equal::<i16>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Int32 => primitive_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Int64 => primitive_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Float32 => primitive_equal::<f32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Float64 => primitive_equal::<f64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Decimal32(_, _) => primitive_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Decimal64(_, _) => primitive_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Decimal128(_, _) => primitive_equal::<i128>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Decimal256(_, _) => primitive_equal::<i256>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Date32 | DataType::Time32(_) | DataType::Interval(IntervalUnit::YearMonth) => {
            primitive_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::Date64
        | DataType::Interval(IntervalUnit::DayTime)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => primitive_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            primitive_equal::<IntervalMonthDayNano>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::Utf8 | DataType::Binary => {
            variable_sized_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            variable_sized_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::FixedSizeBinary(_) => fixed_binary_equal(lhs, rhs, lhs_start, rhs_start, len),
        DataType::BinaryView | DataType::Utf8View => {
            byte_view_equal(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::List(_) | DataType::Map(_, _) => push_list_equal_kind(slice, false, stack),
        DataType::LargeList(_) => push_list_equal_kind(slice, true, stack),
        DataType::ListView(_) => push_list_view_equal::<i32>(slice, false, stack),
        DataType::LargeListView(_) => push_list_view_equal::<i64>(slice, true, stack),
        DataType::FixedSizeList(_, _) => push_fixed_list_equal(slice, stack),
        DataType::Struct(_) => push_struct_equal(slice, stack),
        DataType::Union(_, _) => push_union_equal(slice, stack),
        DataType::Dictionary(data_type, _) => {
            let key_kind = match data_type.as_ref() {
                DataType::Int8 => DictionaryKeyKind::Int8,
                DataType::Int16 => DictionaryKeyKind::Int16,
                DataType::Int32 => DictionaryKeyKind::Int32,
                DataType::Int64 => DictionaryKeyKind::Int64,
                DataType::UInt8 => DictionaryKeyKind::UInt8,
                DataType::UInt16 => DictionaryKeyKind::UInt16,
                DataType::UInt32 => DictionaryKeyKind::UInt32,
                DataType::UInt64 => DictionaryKeyKind::UInt64,
                _ => unreachable!(),
            };
            push_dictionary_equal(slice, key_kind, stack)
        }
        DataType::Float16 => primitive_equal::<f16>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::RunEndEncoded(_, _) => push_run_equal(slice, stack),
    }
}

/// Logically compares two [ArrayData].
///
/// Two arrays are logically equal if and only if:
/// * their data types are equal
/// * their lengths are equal
/// * their null counts are equal
/// * their null bitmaps are equal
/// * each of their items are equal
///
/// Two items are equal when their in-memory representation is physically equal
/// (i.e. has the same bit content).
///
/// The physical comparison depend on the data type.
///
/// # Panics
///
/// This function may panic whenever any of the [ArrayData] does not follow the
/// Arrow specification. (e.g. wrong number of buffers, buffer `len` does not
/// correspond to the declared `len`)
pub fn equal(lhs: &ArrayData, rhs: &ArrayData) -> bool {
    if !(utils::base_equal(lhs, rhs) && lhs.null_count() == rhs.null_count()) {
        return false;
    }
    let mut stack = vec![EqualFrame::Range(EqualSlice {
        lhs,
        rhs,
        lhs_start: 0,
        rhs_start: 0,
        len: lhs.len(),
    })];
    while let Some(frame) = stack.pop() {
        let matched = match frame {
            EqualFrame::Range(slice) => {
                utils::equal_nulls(
                    slice.lhs,
                    slice.rhs,
                    slice.lhs_start,
                    slice.rhs_start,
                    slice.len,
                ) && equal_values_step(slice, &mut stack)
            }
            EqualFrame::StructChildren { slice, child_idx } => {
                step_struct_children(slice, child_idx, &mut stack)
            }
            EqualFrame::StructNullRows {
                slice,
                row_idx,
                child_idx,
            } => step_struct_null_rows(slice, row_idx, child_idx, &mut stack),
            EqualFrame::ListNullRows {
                slice,
                row_idx,
                is_large,
            } => step_list_null_rows_kind(slice, row_idx, is_large, &mut stack),
            EqualFrame::ListViewRows {
                slice,
                row_idx,
                has_nulls,
                is_large,
            } => step_list_view_rows_kind(slice, row_idx, has_nulls, is_large, &mut stack),
            EqualFrame::FixedListNullRows {
                slice,
                size,
                row_idx,
            } => step_fixed_list_null_rows(slice, size, row_idx, &mut stack),
            EqualFrame::DictionaryRows {
                slice,
                row_idx,
                has_nulls,
                key_kind,
            } => step_dictionary_rows(slice, row_idx, has_nulls, key_kind, &mut stack),
            EqualFrame::DenseUnionRows { slice, row_idx } => {
                step_dense_union_rows(slice, row_idx, &mut stack)
            }
            EqualFrame::SparseUnionChildren { slice, child_idx } => {
                step_sparse_union_children(slice, child_idx, &mut stack)
            }
            EqualFrame::RunEndStepper(stepper) => step_run_equal(stepper, &mut stack),
        };
        if !matched {
            return false;
        }
    }
    true
}

// See arrow/tests/array_equal.rs for tests
