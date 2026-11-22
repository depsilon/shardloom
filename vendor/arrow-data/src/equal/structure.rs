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

use crate::data::contains_nulls;

use super::{EqualFrame, EqualSlice};

pub(super) fn push_struct_equal<'a>(
    slice: EqualSlice<'a>,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    // Only checking one null mask here because by the time the control flow reaches
    // this point, the equality of the two masks would have already been verified.
    if !contains_nulls(slice.lhs.nulls(), slice.lhs_start, slice.len) {
        step_struct_children(slice, 0, stack)
    } else {
        step_struct_null_rows(slice, 0, 0, stack)
    }
}

pub(super) fn step_struct_children<'a>(
    slice: EqualSlice<'a>,
    child_idx: usize,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    let num_children = slice.lhs.child_data().len();
    if child_idx >= num_children {
        return true;
    }
    if child_idx + 1 < num_children {
        stack.push(EqualFrame::StructChildren {
            slice,
            child_idx: child_idx + 1,
        });
    }
    stack.push(EqualFrame::Range(EqualSlice {
        lhs: &slice.lhs.child_data()[child_idx],
        rhs: &slice.rhs.child_data()[child_idx],
        lhs_start: slice.lhs_start,
        rhs_start: slice.rhs_start,
        len: slice.len,
    }));
    true
}

pub(super) fn step_struct_null_rows<'a>(
    slice: EqualSlice<'a>,
    mut row_idx: usize,
    child_idx: usize,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    let lhs_nulls = slice.lhs.nulls().unwrap();
    let rhs_nulls = slice.rhs.nulls().unwrap();
    let num_children = slice.lhs.child_data().len();
    while row_idx < slice.len {
        let lhs_pos = slice.lhs_start + row_idx;
        let rhs_pos = slice.rhs_start + row_idx;
        if child_idx == 0 {
            let lhs_is_null = lhs_nulls.is_null(lhs_pos);
            let rhs_is_null = rhs_nulls.is_null(rhs_pos);
            if lhs_is_null != rhs_is_null {
                return false;
            }
            if lhs_is_null {
                row_idx += 1;
                continue;
            }
        }
        if child_idx < num_children {
            let (next_row, next_child) = if child_idx + 1 < num_children {
                (row_idx, child_idx + 1)
            } else {
                (row_idx + 1, 0)
            };
            if next_row < slice.len {
                stack.push(EqualFrame::StructNullRows {
                    slice,
                    row_idx: next_row,
                    child_idx: next_child,
                });
            }
            stack.push(EqualFrame::Range(EqualSlice {
                lhs: &slice.lhs.child_data()[child_idx],
                rhs: &slice.rhs.child_data()[child_idx],
                lhs_start: lhs_pos,
                rhs_start: rhs_pos,
                len: 1,
            }));
            return true;
        }
        row_idx += 1;
    }
    true
}
