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

use arrow_schema::{DataType, UnionMode};

use super::{EqualFrame, EqualSlice};

pub(super) fn push_union_equal<'a>(
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
    let lhs_type_ids = lhs.buffer::<i8>(0);
    let rhs_type_ids = rhs.buffer::<i8>(0);

    let lhs_type_id_range = &lhs_type_ids[lhs_start..lhs_start + len];
    let rhs_type_id_range = &rhs_type_ids[rhs_start..rhs_start + len];

    match (lhs.data_type(), rhs.data_type()) {
        (DataType::Union(_, UnionMode::Dense), DataType::Union(_, UnionMode::Dense)) => {
            if lhs_type_id_range != rhs_type_id_range {
                return false;
            }
            step_dense_union_rows(slice, 0, stack)
        }
        (DataType::Union(_, UnionMode::Sparse), DataType::Union(_, UnionMode::Sparse)) => {
            if lhs_type_id_range != rhs_type_id_range {
                return false;
            }
            step_sparse_union_children(slice, 0, stack)
        }
        _ => unimplemented!(
            "Logical equality not yet implemented between dense and sparse union arrays"
        ),
    }
}

pub(super) fn step_dense_union_rows<'a>(
    slice: EqualSlice<'a>,
    row_idx: usize,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    if row_idx >= slice.len {
        return true;
    }
    let EqualSlice {
        lhs,
        rhs,
        lhs_start,
        rhs_start,
        len,
    } = slice;
    let (DataType::Union(lhs_fields, UnionMode::Dense), DataType::Union(rhs_fields, UnionMode::Dense)) =
        (lhs.data_type(), rhs.data_type())
    else {
        unreachable!()
    };
    let l_type_id = lhs.buffer::<i8>(0)[lhs_start + row_idx];
    let r_type_id = rhs.buffer::<i8>(0)[rhs_start + row_idx];
    let l_offset = lhs.buffer::<i32>(1)[lhs_start + row_idx];
    let r_offset = rhs.buffer::<i32>(1)[rhs_start + row_idx];
    let lhs_child_index = lhs_fields.iter().position(|(r, _)| r == l_type_id).unwrap();
    let rhs_child_index = rhs_fields.iter().position(|(r, _)| r == r_type_id).unwrap();
    if row_idx + 1 < len {
        stack.push(EqualFrame::DenseUnionRows {
            slice,
            row_idx: row_idx + 1,
        });
    }
    stack.push(EqualFrame::Range(EqualSlice {
        lhs: &lhs.child_data()[lhs_child_index],
        rhs: &rhs.child_data()[rhs_child_index],
        lhs_start: l_offset as usize,
        rhs_start: r_offset as usize,
        len: 1,
    }));
    true
}

pub(super) fn step_sparse_union_children<'a>(
    slice: EqualSlice<'a>,
    child_idx: usize,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    let EqualSlice {
        lhs,
        rhs,
        lhs_start,
        rhs_start,
        len,
    } = slice;
    let num_children = lhs.child_data().len();
    if child_idx >= num_children {
        return true;
    }
    if child_idx + 1 < num_children {
        stack.push(EqualFrame::SparseUnionChildren {
            slice,
            child_idx: child_idx + 1,
        });
    }
    stack.push(EqualFrame::Range(EqualSlice {
        lhs: &lhs.child_data()[child_idx],
        rhs: &rhs.child_data()[child_idx],
        lhs_start: lhs_start + lhs.offset(),
        rhs_start: rhs_start + rhs.offset(),
        len,
    }));
    true
}
