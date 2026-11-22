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

use super::{_MutableArrayData, ChildExtendNullsTask, ChildExtendTask, Extend};
use crate::ArrayData;
use arrow_schema::{ArrowError, DataType};

pub(super) fn build_extend_sparse(array: &ArrayData) -> Extend<'_> {
    let type_ids = array.buffer::<i8>(0);

    Box::new(
        move |mutable: &mut _MutableArrayData,
              index: usize,
              start: usize,
              len: usize,
              tasks: &mut Vec<ChildExtendTask>| {
            // extends type_ids
            mutable
                .buffer1
                .extend_from_slice(&type_ids[start..start + len]);

            for child_idx in 0..mutable.child_data.len() {
                tasks.push(ChildExtendTask {
                    child_idx,
                    array_idx: index,
                    start,
                    end: start + len,
                    wrap_struct_col: false,
                });
            }
            Ok(())
        },
    )
}

pub(super) fn build_extend_dense(array: &ArrayData) -> Extend<'_> {
    let type_ids = array.buffer::<i8>(0);
    let offsets = array.buffer::<i32>(1);
    let arrow_schema::DataType::Union(src_fields, _) = array.data_type() else {
        unreachable!();
    };

    Box::new(
        move |mutable: &mut _MutableArrayData,
              index: usize,
              start: usize,
              len: usize,
              tasks: &mut Vec<ChildExtendTask>| {
            // extends type_ids
            mutable
                .buffer1
                .extend_from_slice(&type_ids[start..start + len]);

            let mut child_lens: Vec<usize> =
                mutable.child_data.iter().map(|child| child.len()).collect();

            for i in start..start + len {
                let type_id = type_ids[i];
                let child_index = src_fields
                    .iter()
                    .position(|(r, _)| r == type_id)
                    .expect("invalid union type ID");
                let src_offset = offsets[i] as usize;
                let dst_offset = child_lens[child_index];
                child_lens[child_index] += 1;

                // Extend offsets
                mutable.buffer2.push(dst_offset as i32);
                tasks.push(ChildExtendTask {
                    child_idx: child_index,
                    array_idx: index,
                    start: src_offset,
                    end: src_offset + 1,
                    wrap_struct_col: false,
                });
            }
            Ok(())
        },
    )
}

pub(super) fn extend_nulls_dense(
    mutable: &mut _MutableArrayData,
    len: usize,
    tasks: &mut Vec<ChildExtendNullsTask>,
) -> Result<(), ArrowError> {
    let DataType::Union(fields, _) = &mutable.data_type else {
        unreachable!()
    };
    let first_type_id = fields
        .iter()
        .next()
        .expect("union must have at least one field")
        .0;

    // Extend type_ids buffer
    mutable.buffer1.extend_from_slice(&vec![first_type_id; len]);

    // Dense: extend offsets pointing into the first child, then extend nulls in that child
    let child_offset = mutable.child_data[0].len();
    let (start, end) = (child_offset as i32, (child_offset + len) as i32);
    mutable.buffer2.extend(start..end);
    tasks.push(ChildExtendNullsTask {
        child_idx: 0,
        len,
        wrap_struct_col: false,
    });
    Ok(())
}

pub(super) fn extend_nulls_sparse(
    mutable: &mut _MutableArrayData,
    len: usize,
    tasks: &mut Vec<ChildExtendNullsTask>,
) -> Result<(), ArrowError> {
    let DataType::Union(fields, _) = &mutable.data_type else {
        unreachable!()
    };
    let first_type_id = fields
        .iter()
        .next()
        .expect("union must have at least one field")
        .0;

    // Extend type_ids buffer
    mutable.buffer1.extend_from_slice(&vec![first_type_id; len]);

    // Sparse: extend nulls in ALL children
    for child_idx in 0..mutable.child_data.len() {
        tasks.push(ChildExtendNullsTask {
            child_idx,
            len,
            wrap_struct_col: false,
        });
    }
    Ok(())
}
