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
use arrow_schema::DataType;

use super::{EqualFrame, EqualSlice};

pub(super) fn push_fixed_list_equal<'a>(
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
    let size = match lhs.data_type() {
        DataType::FixedSizeList(_, i) => *i as usize,
        _ => unreachable!(),
    };

    let lhs_values = &lhs.child_data()[0];
    let rhs_values = &rhs.child_data()[0];

    // Only checking one null mask here because by the time the control flow reaches
    // this point, the equality of the two masks would have already been verified.
    if !contains_nulls(lhs.nulls(), lhs_start, len) {
        stack.push(EqualFrame::Range(EqualSlice {
            lhs: lhs_values,
            rhs: rhs_values,
            lhs_start: (lhs_start + lhs.offset()) * size,
            rhs_start: (rhs_start + rhs.offset()) * size,
            len: size * len,
        }));
        true
    } else {
        step_fixed_list_null_rows(slice, size, 0, stack)
    }
}

pub(super) fn step_fixed_list_null_rows<'a>(
    slice: EqualSlice<'a>,
    size: usize,
    row_idx: usize,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    let EqualSlice {
        lhs,
        rhs,
        lhs_start,
        rhs_start,
        len,
    } = slice;
    let lhs_values = &lhs.child_data()[0];
    let rhs_values = &rhs.child_data()[0];
    // get a ref of the null buffer bytes, to use in testing for nullness
    let lhs_nulls = lhs.nulls().unwrap();
    let rhs_nulls = rhs.nulls().unwrap();
    // with nulls, we need to compare item by item whenever it is not null
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
        if i + 1 < len {
            stack.push(EqualFrame::FixedListNullRows {
                slice,
                size,
                row_idx: i + 1,
            });
        }
        stack.push(EqualFrame::Range(EqualSlice {
            lhs: lhs_values,
            rhs: rhs_values,
            lhs_start: (lhs_pos + lhs.offset()) * size,
            rhs_start: (rhs_pos + rhs.offset()) * size,
            len: size, // 1 * size since we are comparing a single entry
        }));
        return true;
    }
    true
}
