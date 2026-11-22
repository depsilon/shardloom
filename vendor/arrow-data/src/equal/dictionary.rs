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

use crate::data::{ArrayData, contains_nulls};
use arrow_buffer::ArrowNativeType;

use super::{EqualFrame, EqualSlice};

#[derive(Clone, Copy)]
pub(super) enum DictionaryKeyKind {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
}

pub(super) fn push_dictionary_equal<'a>(
    slice: EqualSlice<'a>,
    key_kind: DictionaryKeyKind,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    let has_nulls = contains_nulls(slice.lhs.nulls(), slice.lhs_start, slice.len);
    step_dictionary_rows(slice, 0, has_nulls, key_kind, stack)
}

pub(super) fn step_dictionary_rows<'a>(
    slice: EqualSlice<'a>,
    row_idx: usize,
    has_nulls: bool,
    key_kind: DictionaryKeyKind,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    match key_kind {
        DictionaryKeyKind::Int8 => {
            step_dictionary_typed::<i8>(slice, row_idx, has_nulls, key_kind, stack)
        }
        DictionaryKeyKind::Int16 => {
            step_dictionary_typed::<i16>(slice, row_idx, has_nulls, key_kind, stack)
        }
        DictionaryKeyKind::Int32 => {
            step_dictionary_typed::<i32>(slice, row_idx, has_nulls, key_kind, stack)
        }
        DictionaryKeyKind::Int64 => {
            step_dictionary_typed::<i64>(slice, row_idx, has_nulls, key_kind, stack)
        }
        DictionaryKeyKind::UInt8 => {
            step_dictionary_typed::<u8>(slice, row_idx, has_nulls, key_kind, stack)
        }
        DictionaryKeyKind::UInt16 => {
            step_dictionary_typed::<u16>(slice, row_idx, has_nulls, key_kind, stack)
        }
        DictionaryKeyKind::UInt32 => {
            step_dictionary_typed::<u32>(slice, row_idx, has_nulls, key_kind, stack)
        }
        DictionaryKeyKind::UInt64 => {
            step_dictionary_typed::<u64>(slice, row_idx, has_nulls, key_kind, stack)
        }
    }
}

fn step_dictionary_typed<'a, T: ArrowNativeType>(
    slice: EqualSlice<'a>,
    row_idx: usize,
    has_nulls: bool,
    key_kind: DictionaryKeyKind,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    let EqualSlice {
        lhs,
        rhs,
        lhs_start,
        rhs_start,
        len,
    } = slice;
    let lhs_keys = lhs.buffer::<T>(0);
    let rhs_keys = rhs.buffer::<T>(0);

    let lhs_values = &lhs.child_data()[0];
    let rhs_values = &rhs.child_data()[0];

    if !has_nulls {
        return push_next_dict_row(
            slice, row_idx, has_nulls, key_kind, lhs_keys, rhs_keys, lhs_values, rhs_values, stack,
        );
    }

    let lhs_nulls = lhs.nulls().unwrap();
    let rhs_nulls = rhs.nulls().unwrap();
    for i in row_idx..len {
        let lhs_pos = lhs_start + i;
        let rhs_pos = rhs_start + i;
        let lhs_is_null = lhs_nulls.is_null(lhs_pos);
        let rhs_is_null = rhs_nulls.is_null(rhs_pos);
        if lhs_is_null {
            continue;
        }
        if lhs_is_null != rhs_is_null {
            return false;
        }
        return push_dict_pair(
            slice,
            i,
            has_nulls,
            key_kind,
            lhs_keys[lhs_pos].to_usize().unwrap(),
            rhs_keys[rhs_pos].to_usize().unwrap(),
            lhs_values,
            rhs_values,
            stack,
        );
    }
    true
}

#[allow(clippy::too_many_arguments)]
fn push_next_dict_row<'a, T: ArrowNativeType>(
    slice: EqualSlice<'a>,
    row_idx: usize,
    has_nulls: bool,
    key_kind: DictionaryKeyKind,
    lhs_keys: &[T],
    rhs_keys: &[T],
    lhs_values: &'a ArrayData,
    rhs_values: &'a ArrayData,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    if row_idx < slice.len {
        let lhs_pos = slice.lhs_start + row_idx;
        let rhs_pos = slice.rhs_start + row_idx;
        return push_dict_pair(
            slice,
            row_idx,
            has_nulls,
            key_kind,
            lhs_keys[lhs_pos].to_usize().unwrap(),
            rhs_keys[rhs_pos].to_usize().unwrap(),
            lhs_values,
            rhs_values,
            stack,
        );
    }
    true
}

#[allow(clippy::too_many_arguments)]
fn push_dict_pair<'a>(
    slice: EqualSlice<'a>,
    row_idx: usize,
    has_nulls: bool,
    key_kind: DictionaryKeyKind,
    lhs_key: usize,
    rhs_key: usize,
    lhs_values: &'a ArrayData,
    rhs_values: &'a ArrayData,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    if row_idx + 1 < slice.len {
        stack.push(EqualFrame::DictionaryRows {
            slice,
            row_idx: row_idx + 1,
            has_nulls,
            key_kind,
        });
    }
    stack.push(EqualFrame::Range(EqualSlice {
        lhs: lhs_values,
        rhs: rhs_values,
        lhs_start: lhs_key,
        rhs_start: rhs_key,
        len: 1,
    }));
    true
}
