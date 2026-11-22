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

use crate::data::count_nulls;
use arrow_buffer::ArrowNativeType;
use num_integer::Integer;

use super::{EqualFrame, EqualSlice};

pub(super) fn push_list_view_equal<'a, T: ArrowNativeType + Integer>(
    slice: EqualSlice<'a>,
    is_large: bool,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    let EqualSlice {
        lhs,
        rhs,
        lhs_start,
        rhs_start,
        len,
    } = slice;
    let lhs_offsets = lhs.buffer::<T>(0);
    let lhs_sizes = lhs.buffer::<T>(1);

    let rhs_offsets = rhs.buffer::<T>(0);
    let rhs_sizes = rhs.buffer::<T>(1);

    let lhs_null_count = count_nulls(lhs.nulls(), lhs_start, len);
    let rhs_null_count = count_nulls(rhs.nulls(), rhs_start, len);

    if lhs_null_count != rhs_null_count {
        return false;
    }

    let lhs_range_sizes = &lhs_sizes[lhs_start..lhs_start + len];
    let rhs_range_sizes = &rhs_sizes[rhs_start..rhs_start + len];
    let lhs_range_offsets = &lhs_offsets[lhs_start..lhs_start + len];
    let rhs_range_offsets = &rhs_offsets[rhs_start..rhs_start + len];

    if lhs_range_sizes.len() != rhs_range_sizes.len()
        || lhs_range_offsets.len() != rhs_range_offsets.len()
    {
        return false;
    }

    let has_nulls = lhs_null_count != 0;
    if !has_nulls && lhs_range_sizes != rhs_range_sizes {
        return false;
    }

    step_list_view_rows::<T>(slice, 0, has_nulls, is_large, stack)
}

pub(super) fn step_list_view_rows_kind<'a>(
    slice: EqualSlice<'a>,
    row_idx: usize,
    has_nulls: bool,
    is_large: bool,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    if is_large {
        step_list_view_rows::<i64>(slice, row_idx, has_nulls, true, stack)
    } else {
        step_list_view_rows::<i32>(slice, row_idx, has_nulls, false, stack)
    }
}

fn step_list_view_rows<'a, T: ArrowNativeType + Integer>(
    slice: EqualSlice<'a>,
    row_idx: usize,
    has_nulls: bool,
    is_large: bool,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    let EqualSlice {
        lhs,
        rhs,
        lhs_start,
        rhs_start,
        len,
    } = slice;
    let lhs_range_offsets = &lhs.buffer::<T>(0)[lhs_start..lhs_start + len];
    let lhs_range_sizes = &lhs.buffer::<T>(1)[lhs_start..lhs_start + len];
    let rhs_range_offsets = &rhs.buffer::<T>(0)[rhs_start..rhs_start + len];
    let rhs_range_sizes = &rhs.buffer::<T>(1)[rhs_start..rhs_start + len];
    let lhs_data = &lhs.child_data()[0];
    let rhs_data = &rhs.child_data()[0];

    if !has_nulls {
        if row_idx < len {
            let lhs_offset = lhs_range_offsets[row_idx].to_usize().unwrap();
            let rhs_offset = rhs_range_offsets[row_idx].to_usize().unwrap();
            let size = lhs_range_sizes[row_idx].to_usize().unwrap();
            if row_idx + 1 < len {
                stack.push(EqualFrame::ListViewRows {
                    slice,
                    row_idx: row_idx + 1,
                    has_nulls,
                    is_large,
                });
            }
            stack.push(EqualFrame::Range(EqualSlice {
                lhs: lhs_data,
                rhs: rhs_data,
                lhs_start: lhs_offset,
                rhs_start: rhs_offset,
                len: size,
            }));
        }
        return true;
    }

    let lhs_nulls = lhs.nulls().unwrap().slice(lhs_start, len);
    let rhs_nulls = rhs.nulls().unwrap().slice(rhs_start, len);
    for index in row_idx..len {
        let lhs_is_null = lhs_nulls.is_null(index);
        let rhs_is_null = rhs_nulls.is_null(index);
        if lhs_is_null != rhs_is_null {
            return false;
        }
        if lhs_is_null {
            continue;
        }
        let lhs_size = lhs_range_sizes[index];
        let rhs_size = rhs_range_sizes[index];
        if lhs_size != rhs_size {
            return false;
        }
        let lhs_offset = lhs_range_offsets[index].to_usize().unwrap();
        let rhs_offset = rhs_range_offsets[index].to_usize().unwrap();
        let size = lhs_size.to_usize().unwrap();
        if index + 1 < len {
            stack.push(EqualFrame::ListViewRows {
                slice,
                row_idx: index + 1,
                has_nulls,
                is_large,
            });
        }
        stack.push(EqualFrame::Range(EqualSlice {
            lhs: lhs_data,
            rhs: rhs_data,
            lhs_start: lhs_offset,
            rhs_start: rhs_offset,
            len: size,
        }));
        return true;
    }
    true
}
