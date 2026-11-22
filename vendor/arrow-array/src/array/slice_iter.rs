// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Ordered iterative slicing for recursive built-in nested Arrow arrays.

use arrow_buffer::NullBuffer;
use arrow_schema::DataType;
use std::sync::Arc;

use super::{Array, ArrayRef, FixedSizeListArray, StructArray, UnionArray};

#[derive(Clone, Copy)]
struct SliceRange {
    offset: usize,
    len: usize,
}

enum SliceParent {
    Struct {
        len: usize,
        data_type: DataType,
        nulls: Option<NullBuffer>,
        range: SliceRange,
        fields: Vec<ArrayRef>,
        next: usize,
        built: Vec<ArrayRef>,
    },
    FixedSizeList {
        data_type: DataType,
        nulls: Option<NullBuffer>,
        value_length: i32,
        len: usize,
    },
    SparseUnion {
        data_type: DataType,
        type_ids: arrow_buffer::ScalarBuffer<i8>,
        range: SliceRange,
        fields: Vec<Option<ArrayRef>>,
        next: usize,
        built: Vec<Option<ArrayRef>>,
    },
}

fn needs_iterative_slice(array: &dyn Array) -> bool {
    let any = array.as_any();
    any.is::<StructArray>()
        || any.is::<FixedSizeListArray>()
        || any
            .downcast_ref::<UnionArray>()
            .is_some_and(|u| !u.is_dense())
}

fn slice_leaf_or_non_recursive(array: &dyn Array, offset: usize, len: usize) -> ArrayRef {
    array.slice(offset, len)
}

pub(crate) fn slice_struct(source: &StructArray, offset: usize, len: usize) -> StructArray {
    let (data_type, nulls, fields) = source.slice_parts();
    if fields.is_empty() || fields.iter().all(|f| !needs_iterative_slice(f.as_ref())) {
        return StructArray::from_sliced_parts(
            len,
            data_type.clone(),
            nulls.as_ref().map(|n| n.slice(offset, len)),
            fields.iter().map(|a| a.slice(offset, len)).collect(),
        );
    }

    let owned_fields = fields.to_vec();
    let first = owned_fields[0].clone();
    let range = SliceRange { offset, len };
    let frame = SliceParent::Struct {
        len,
        data_type: data_type.clone(),
        nulls: nulls.as_ref().map(|n| n.slice(offset, len)),
        range,
        built: Vec::with_capacity(owned_fields.len()),
        fields: owned_fields,
        next: 1,
    };
    let result = drive_slice(first, offset, len, frame);
    result
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("root slice must preserve StructArray")
        .clone()
}

pub(crate) fn slice_fixed_size_list(
    source: &FixedSizeListArray,
    offset: usize,
    len: usize,
) -> FixedSizeListArray {
    let (data_type, values, nulls, value_length) = source.slice_parts();
    let size = value_length as usize;
    let child_offset = offset * size;
    let child_len = len * size;
    if !needs_iterative_slice(values.as_ref()) {
        return FixedSizeListArray::from_sliced_parts(
            data_type.clone(),
            values.slice(child_offset, child_len),
            nulls.as_ref().map(|n| n.slice(offset, len)),
            value_length,
            len,
        );
    }

    let frame = SliceParent::FixedSizeList {
        data_type: data_type.clone(),
        nulls: nulls.as_ref().map(|n| n.slice(offset, len)),
        value_length,
        len,
    };
    let result = drive_slice(values.clone(), child_offset, child_len, frame);
    result
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .expect("root slice must preserve FixedSizeListArray")
        .clone()
}

pub(crate) fn slice_sparse_union(source: &UnionArray, offset: usize, length: usize) -> UnionArray {
    let (data_type, type_ids, fields) = source.sparse_slice_parts();
    if !fields
        .iter()
        .flatten()
        .any(|f| needs_iterative_slice(f.as_ref()))
    {
        return UnionArray::from_sparse_sliced_parts(
            data_type.clone(),
            type_ids.slice(offset, length),
            fields
                .iter()
                .map(|x| x.as_ref().map(|a| a.slice(offset, length)))
                .collect(),
        );
    }

    let owned_fields = fields.to_vec();
    let mut built = Vec::with_capacity(owned_fields.len());
    let mut first = None;
    let mut next_idx = 0;
    while next_idx < owned_fields.len() {
        if let Some(arr) = &owned_fields[next_idx] {
            first = Some(arr.clone());
            next_idx += 1;
            break;
        }
        built.push(None);
        next_idx += 1;
    }
    let Some(first) = first else {
        return UnionArray::from_sparse_sliced_parts(
            data_type.clone(),
            type_ids.slice(offset, length),
            built,
        );
    };
    let frame = SliceParent::SparseUnion {
        data_type: data_type.clone(),
        type_ids: type_ids.slice(offset, length),
        range: SliceRange {
            offset,
            len: length,
        },
        fields: owned_fields,
        next: next_idx,
        built,
    };
    let result = drive_slice(first, offset, length, frame);
    result
        .as_any()
        .downcast_ref::<UnionArray>()
        .expect("root slice must preserve UnionArray")
        .clone()
}

fn drive_slice(
    mut current: ArrayRef,
    mut offset: usize,
    mut cur_len: usize,
    root_frame: SliceParent,
) -> ArrayRef {
    let mut stack = vec![root_frame];

    'visit: loop {
        let mut completed: ArrayRef = loop {
            let any = current.as_any();
            if let Some(st) = any.downcast_ref::<StructArray>() {
                assert!(
                    offset.saturating_add(cur_len) <= st.len(),
                    "the length + offset of the sliced StructArray cannot exceed the existing length"
                );
                let (data_type, nulls, fields) = st.slice_parts();
                if let Some(first) = fields.first().cloned() {
                    let owned_fields = fields.to_vec();
                    stack.push(SliceParent::Struct {
                        len: cur_len,
                        data_type: data_type.clone(),
                        nulls: nulls.as_ref().map(|n| n.slice(offset, cur_len)),
                        range: SliceRange {
                            offset,
                            len: cur_len,
                        },
                        built: Vec::with_capacity(owned_fields.len()),
                        fields: owned_fields,
                        next: 1,
                    });
                    current = first;
                    continue;
                }
                break Arc::new(StructArray::from_sliced_parts(
                    cur_len,
                    data_type.clone(),
                    nulls.as_ref().map(|n| n.slice(offset, cur_len)),
                    Vec::new(),
                ));
            }
            if let Some(fsl) = any.downcast_ref::<FixedSizeListArray>() {
                assert!(
                    offset.saturating_add(cur_len) <= fsl.len(),
                    "the length + offset of the sliced FixedSizeListArray cannot exceed the existing length"
                );
                let (data_type, values, nulls, value_length) = fsl.slice_parts();
                let size = value_length as usize;
                stack.push(SliceParent::FixedSizeList {
                    data_type: data_type.clone(),
                    nulls: nulls.as_ref().map(|n| n.slice(offset, cur_len)),
                    value_length,
                    len: cur_len,
                });
                current = values.clone();
                offset *= size;
                cur_len *= size;
                continue;
            }
            if let Some(union_arr) = any.downcast_ref::<UnionArray>()
                && !union_arr.is_dense()
            {
                let (data_type, type_ids, fields) = union_arr.sparse_slice_parts();
                let owned_fields = fields.to_vec();
                let mut built = Vec::with_capacity(owned_fields.len());
                let mut first = None;
                let mut next_idx = 0;
                while next_idx < owned_fields.len() {
                    if let Some(arr) = &owned_fields[next_idx] {
                        first = Some(arr.clone());
                        next_idx += 1;
                        break;
                    }
                    built.push(None);
                    next_idx += 1;
                }
                if let Some(first) = first {
                    stack.push(SliceParent::SparseUnion {
                        data_type: data_type.clone(),
                        type_ids: type_ids.slice(offset, cur_len),
                        range: SliceRange {
                            offset,
                            len: cur_len,
                        },
                        fields: owned_fields,
                        next: next_idx,
                        built,
                    });
                    current = first;
                    continue;
                }
                break Arc::new(UnionArray::from_sparse_sliced_parts(
                    data_type.clone(),
                    type_ids.slice(offset, cur_len),
                    built,
                ));
            }
            break slice_leaf_or_non_recursive(current.as_ref(), offset, cur_len);
        };

        while let Some(frame) = stack.pop() {
            completed = match frame {
                SliceParent::Struct {
                    len: frame_len,
                    data_type,
                    nulls,
                    range,
                    fields,
                    next,
                    mut built,
                } => {
                    built.push(completed);
                    if next < fields.len() {
                        let next_field = fields[next].clone();
                        stack.push(SliceParent::Struct {
                            len: frame_len,
                            data_type,
                            nulls,
                            range,
                            fields,
                            next: next + 1,
                            built,
                        });
                        current = next_field;
                        offset = range.offset;
                        cur_len = range.len;
                        continue 'visit;
                    }
                    Arc::new(StructArray::from_sliced_parts(
                        frame_len, data_type, nulls, built,
                    ))
                }
                SliceParent::FixedSizeList {
                    data_type,
                    nulls,
                    value_length,
                    len: frame_len,
                } => Arc::new(FixedSizeListArray::from_sliced_parts(
                    data_type,
                    completed,
                    nulls,
                    value_length,
                    frame_len,
                )),
                SliceParent::SparseUnion {
                    data_type,
                    type_ids,
                    range,
                    fields,
                    mut next,
                    mut built,
                } => {
                    built.push(Some(completed));
                    let mut next_child = None;
                    while next < fields.len() {
                        if let Some(arr) = &fields[next] {
                            next_child = Some(arr.clone());
                            next += 1;
                            break;
                        }
                        built.push(None);
                        next += 1;
                    }
                    if let Some(next_child) = next_child {
                        stack.push(SliceParent::SparseUnion {
                            data_type,
                            type_ids,
                            range,
                            fields,
                            next,
                            built,
                        });
                        current = next_child;
                        offset = range.offset;
                        cur_len = range.len;
                        continue 'visit;
                    }
                    Arc::new(UnionArray::from_sparse_sliced_parts(
                        data_type, type_ids, built,
                    ))
                }
            };
        }

        return completed;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread::Builder;

    use arrow_buffer::ScalarBuffer;
    use arrow_schema::{DataType, Field, Fields, Schema, UnionFields, UnionMode};

    use crate::{
        Array, ArrayRef, FixedSizeListArray, Int32Array, RecordBatch, StructArray, UnionArray,
    };

    fn small_stack<F: FnOnce() + Send + 'static>(name: &str, f: F) {
        Builder::new()
            .name(name.to_owned())
            .stack_size(128 * 1024)
            .spawn(f)
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn deep_2048_struct_fsl_sparse_union_slice_on_128k_stack() {
        small_stack("arrow_deep_2048_slice", || {
            let mut cur: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30, 40]));
            let type_ids = ScalarBuffer::from(vec![0_i8; 4]);
            for level in 0..2_048 {
                cur = match level % 3 {
                    0 => {
                        let f = Arc::new(Field::new("inner", cur.data_type().clone(), false));
                        Arc::new(StructArray::new(Fields::from(vec![f]), vec![cur], None))
                    }
                    1 => {
                        let f = Arc::new(Field::new_list_field(cur.data_type().clone(), false));
                        Arc::new(FixedSizeListArray::new(f, 1, cur, None))
                    }
                    _ => {
                        let uf = UnionFields::try_new(
                            [0],
                            [Field::new("u", cur.data_type().clone(), false)],
                        )
                        .unwrap();
                        Arc::new(UnionArray::from_sparse_sliced_parts(
                            DataType::Union(uf, UnionMode::Sparse),
                            type_ids.clone(),
                            vec![Some(cur)],
                        ))
                    }
                };
            }
            let schema = Arc::new(Schema::new(vec![Field::new(
                "root",
                cur.data_type().clone(),
                false,
            )]));
            let batch = RecordBatch::try_new(schema, vec![cur]).unwrap();
            let sliced = batch.slice(1, 2);
            assert_eq!(sliced.num_rows(), 2);
            drop((sliced, batch));
        });
    }

    #[test]
    fn deep_128_public_union_try_new_slice_on_128k_stack() {
        small_stack("arrow_deep_128_public_union_slice", || {
            let mut cur: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30, 40]));
            let type_ids = ScalarBuffer::from(vec![0_i8; 4]);
            for level in 0..128 {
                cur = match level % 3 {
                    0 => {
                        let f = Arc::new(Field::new("inner", cur.data_type().clone(), false));
                        Arc::new(StructArray::new(Fields::from(vec![f]), vec![cur], None))
                    }
                    1 => {
                        let f = Arc::new(Field::new_list_field(cur.data_type().clone(), false));
                        Arc::new(FixedSizeListArray::new(f, 1, cur, None))
                    }
                    _ => {
                        let uf = UnionFields::try_new(
                            [0],
                            [Field::new("u", cur.data_type().clone(), false)],
                        )
                        .unwrap();
                        Arc::new(UnionArray::try_new(uf, type_ids.clone(), None, vec![cur]).unwrap())
                    }
                };
            }
            let sliced = cur.slice(1, 2);
            assert_eq!(sliced.len(), 2);
            drop((sliced, cur));
        });
    }

    #[test]
    fn deep_2048_pure_struct_slice_on_128k_stack() {
        small_stack("arrow_deep_2048_pure_struct_slice", || {
            let mut cur: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30, 40]));
            for _ in 0..2_048 {
                let f = Arc::new(Field::new("inner", cur.data_type().clone(), false));
                cur = Arc::new(StructArray::new(Fields::from(vec![f]), vec![cur], None));
            }
            let sliced = cur.slice(1, 2);
            assert_eq!(sliced.len(), 2);
            drop((sliced, cur));
        });
    }

    #[test]
    fn shallow_sparse_and_dense_union_slice_parity() {
        let a = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let b = Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef;
        let uf = UnionFields::try_new(
            [0, 1],
            [
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ],
        )
        .unwrap();
        let type_ids = ScalarBuffer::from(vec![0_i8, 1, 0]);
        let sparse = UnionArray::try_new(uf.clone(), type_ids.clone(), None, vec![a.clone(), b.clone()])
            .unwrap();
        let sliced_sparse = sparse.slice(1, 2);
        assert_eq!(sliced_sparse.len(), 2);
        assert_eq!(sliced_sparse.type_ids().as_ref(), &[1_i8, 0]);

        let offsets = ScalarBuffer::from(vec![0_i32, 0, 1]);
        let dense = UnionArray::try_new(uf, type_ids, Some(offsets), vec![a, b]).unwrap();
        assert_eq!(dense.data_type(), &DataType::Union(
            UnionFields::try_new(
                [0, 1],
                [
                    Field::new("a", DataType::Int32, false),
                    Field::new("b", DataType::Int32, false),
                ],
            ).unwrap(),
            UnionMode::Dense,
        ));
        let sliced_dense = dense.slice(1, 2);
        assert_eq!(sliced_dense.len(), 2);
    }
}
