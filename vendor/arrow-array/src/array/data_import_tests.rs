// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{DataType, Field, Fields, UnionFields, UnionMode};

use super::super::data_export::tests::reference_data;
use super::super::release::tests::{deep, small_stack, wrap};
use crate::types::*;
use crate::*;

#[rustfmt::skip]
fn reference_make_array(data: ArrayData) -> ArrayRef {
    match data.data_type() {
        DataType::List(_) => Arc::new({
            let (data_type, len, nulls, offset, mut buffers, mut child_data) = data.into_parts();
            let buffer = buffers.pop().unwrap();
            let values = reference_make_array(child_data.pop().unwrap());
            let scalar = if len == 0 && buffer.is_empty() {
                OffsetBuffer::new_empty()
            } else {
                unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::<i32>::new(buffer, offset, len + 1)) }
            };
            ListArray::from_parts(data_type, nulls, values.into(), scalar)
        }) as ArrayRef,
        DataType::LargeList(_) => Arc::new({
            let (data_type, len, nulls, offset, mut buffers, mut child_data) = data.into_parts();
            let buffer = buffers.pop().unwrap();
            let values = reference_make_array(child_data.pop().unwrap());
            let scalar = if len == 0 && buffer.is_empty() {
                OffsetBuffer::new_empty()
            } else {
                unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::<i64>::new(buffer, offset, len + 1)) }
            };
            LargeListArray::from_parts(data_type, nulls, values.into(), scalar)
        }) as ArrayRef,
        DataType::ListView(_) => Arc::new({
            let (data_type, len, nulls, offset, buffers, child_data) = data.into_parts();
            let [offsets_buffer, sizes_buffer]: [arrow_buffer::Buffer; 2] = buffers.try_into().unwrap();
            let [values]: [ArrayData; 1] = child_data.try_into().unwrap();
            ListViewArray::from_parts(
                data_type,
                nulls,
                reference_make_array(values).into(),
                ScalarBuffer::<i32>::new(offsets_buffer, offset, len),
                ScalarBuffer::<i32>::new(sizes_buffer, offset, len),
            )
        }) as ArrayRef,
        DataType::LargeListView(_) => Arc::new({
            let (data_type, len, nulls, offset, buffers, child_data) = data.into_parts();
            let [offsets_buffer, sizes_buffer]: [arrow_buffer::Buffer; 2] = buffers.try_into().unwrap();
            let [values]: [ArrayData; 1] = child_data.try_into().unwrap();
            LargeListViewArray::from_parts(
                data_type,
                nulls,
                reference_make_array(values).into(),
                ScalarBuffer::<i64>::new(offsets_buffer, offset, len),
                ScalarBuffer::<i64>::new(sizes_buffer, offset, len),
            )
        }) as ArrayRef,
        DataType::Struct(_) => Arc::new({
            let (data_type, len, nulls, offset, _buffers, child_data) = data.into_parts();
            let fields = child_data
                .into_iter()
                .map(|cd| {
                    let cd = if offset != 0 || len != cd.len() {
                        cd.slice(offset, len)
                    } else {
                        cd
                    };
                    reference_make_array(cd)
                })
                .collect::<Vec<_>>();
            StructArray::from_parts(len, data_type, nulls, fields.into())
        }) as ArrayRef,
        DataType::Map(_, _) => Arc::new({
            let (data_type, len, nulls, offset, mut buffers, mut child_data) = data.into_parts();
            let buffer = buffers.pop().unwrap();
            let entries = reference_make_array(child_data.pop().unwrap())
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .clone();
            let offsets = if len == 0 && buffer.is_empty() {
                OffsetBuffer::new_empty()
            } else {
                unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::<i32>::new(buffer, offset, len + 1)) }
            };
            MapArray::from_parts(data_type, nulls, entries, offsets)
        }) as ArrayRef,
        DataType::FixedSizeList(_, _) => Arc::new({
            let (data_type, len, nulls, offset, _buffers, child_data) = data.into_parts();
            let DataType::FixedSizeList(_, value_length) = data_type else { unreachable!() };
            let size = value_length as usize;
            let values = reference_make_array(child_data[0].slice(offset * size, len * size));
            FixedSizeListArray::from_parts(data_type, values.into(), nulls, value_length, len)
        }) as ArrayRef,
        DataType::Union(_, _) => Arc::new({
            let (data_type, len, _nulls, offset, buffers, child_data) = data.into_parts();
            let DataType::Union(fields, mode) = &data_type else { unreachable!() };
            let (type_ids, offsets) = match mode {
                UnionMode::Sparse => (ScalarBuffer::new(buffers[0].clone(), offset, len), None),
                UnionMode::Dense => (
                    ScalarBuffer::new(buffers[0].clone(), offset, len),
                    Some(ScalarBuffer::new(buffers[1].clone(), offset, len)),
                ),
            };
            let max_id = fields.iter().map(|(i, _)| i).max().unwrap_or_default() as usize;
            let mut boxed = vec![None; max_id + 1];
            for (cd, (id, _)) in child_data.into_iter().zip(fields.iter()) {
                let cd = match mode {
                    UnionMode::Sparse => cd.slice(offset, len),
                    UnionMode::Dense => cd,
                };
                boxed[id as usize] = Some(reference_make_array(cd));
            }
            UnionArray::from_parts(data_type, type_ids, offsets, boxed.into())
        }) as ArrayRef,
        _ => super::make_array(data),
    }
}

#[test]
fn original_descriptor_import_buffers_metadata_offsets_and_errors() {
    let leaf: ArrayRef = Arc::new(Int32Array::from(vec![Some(42)]));
    for shape in 0..10 {
        for depth in 0..6 {
            let source = deep(leaf.clone(), depth, Some(shape));
            let descriptor = source.to_data();
            let rebuilt = make_array(descriptor.clone());
            let expected = reference_make_array(descriptor);
            assert_eq!(rebuilt.as_ref(), expected.as_ref());
            assert_eq!(rebuilt.to_data(), reference_data(expected.as_ref()));
            for (offset, len) in [(0, 0), (0, 1), (1, 0)] {
                let slice_data = source.slice(offset, len).to_data();
                let rebuilt_slice = make_array(slice_data.clone());
                let expected_slice = reference_make_array(slice_data);
                assert_eq!(rebuilt_slice.as_ref(), expected_slice.as_ref());
            }
        }
    }

    // Sliced StructArray triggers child slicing inside Struct frame.
    let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
    let b: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), None, Some("z")]));
    let struct_arr = StructArray::try_from(vec![("a", a), ("b", b)]).unwrap();
    let sliced = struct_arr.slice(1, 2);
    let rebuilt_struct = StructArray::from(sliced.to_data());
    assert_eq!(rebuilt_struct, sliced);

    // Declared union order differs from numeric type-id slot order.
    for dense in [false, true] {
        let fields = UnionFields::try_new(
            [5, 2],
            [
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
            ],
        )
        .unwrap();
        let union = UnionArray::try_new(
            fields,
            vec![5_i8, 2, 5].into(),
            dense.then(|| vec![0_i32, 0, 1].into()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 9])),
                Arc::new(Int32Array::from(if dense {
                    vec![3, 4]
                } else {
                    vec![3, 4, 8]
                })),
            ],
        )
        .unwrap();
        let sliced_union = union.slice(1, 2);
        let rebuilt = UnionArray::from(sliced_union.to_data());
        assert_eq!(rebuilt.to_data(), sliced_union.to_data());
        assert_eq!(
            make_array(sliced_union.to_data()).as_ref(),
            reference_make_array(sliced_union.to_data()).as_ref()
        );
    }

    // All dictionary key widths and run-end widths reconstruct cleanly via From<ArrayData>.
    macro_rules! dictionary {
        ($($ty:ty),+) => {$(
            let dict = DictionaryArray::<$ty>::new(
                PrimitiveArray::<$ty>::from(vec![Some(0), None]),
                wrap(Arc::new(Int32Array::from(vec![42])), 0),
            );
            let rebuilt = DictionaryArray::<$ty>::from(dict.to_data());
            assert_eq!(rebuilt, dict);
            assert_eq!(make_array(dict.to_data()).as_ref(), &dict as &dyn Array);
        )+};
    }
    dictionary!(
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type
    );

    macro_rules! runs {
        ($($ty:ty),+) => {$(
            let values = wrap(Arc::new(Int32Array::from(vec![42])), 5);
            let run = RunArray::<$ty>::try_new(
                &PrimitiveArray::<$ty>::from(vec![3]),
                values.as_ref(),
            )
            .unwrap()
            .slice(1, 2);
            let rebuilt = RunArray::<$ty>::from(run.to_data());
            assert_eq!(rebuilt, run);
            assert_eq!(make_array(run.to_data()).as_ref(), &run as &dyn Array);
        )+};
    }
    runs!(Int16Type, Int32Type, Int64Type);

    // Invalid descriptor error parity for concrete From<ArrayData>.
    let bad_list = unsafe {
        ArrayDataBuilder::new(DataType::Int32)
            .len(1)
            .buffers(vec![arrow_buffer::Buffer::from_slice_ref([0_i32, 1])])
            .build_unchecked()
    };
    assert!(catch_unwind(AssertUnwindSafe(|| ListArray::from(bad_list))).is_err());
    let bad_fsl = unsafe { ArrayDataBuilder::new(DataType::Int32).len(1).build_unchecked() };
    assert!(catch_unwind(AssertUnwindSafe(|| FixedSizeListArray::from(bad_fsl))).is_err());
    let bad_map = unsafe { ArrayDataBuilder::new(DataType::Int32).len(1).build_unchecked() };
    assert!(catch_unwind(AssertUnwindSafe(|| MapArray::from(bad_map))).is_err());
}

#[test]
fn deep_descriptor_import_and_unwind_cleanup_on_small_stack() {
    small_stack(
        "array::data_import::tests::deep_descriptor_import_and_unwind_cleanup_on_small_stack",
        || {
            for shape in 0..11 {
                let leaf: ArrayRef = Arc::new(Int32Array::from(vec![Some(42)]));
                let weak_leaf = Arc::downgrade(&leaf);
                let depth = if shape == 8 { 512 } else { 8_192 };
                let source = deep(leaf, depth, (shape < 10).then_some(shape));
                let descriptor = source.to_data();
                drop(source);

                let rebuilt = make_array(descriptor.clone());
                drop(descriptor);
                assert_eq!(rebuilt.len(), 1);

                // Spot-check typed From<ArrayData> entrypoints at depth too.
                let typed = match shape {
                    0 => Arc::new(ListArray::from(rebuilt.to_data())) as ArrayRef,
                    1 => Arc::new(LargeListArray::from(rebuilt.to_data())) as ArrayRef,
                    2 => Arc::new(ListViewArray::from(rebuilt.to_data())) as ArrayRef,
                    3 => Arc::new(LargeListViewArray::from(rebuilt.to_data())) as ArrayRef,
                    4 => Arc::new(FixedSizeListArray::from(rebuilt.to_data())) as ArrayRef,
                    5 => Arc::new(StructArray::from(rebuilt.to_data())) as ArrayRef,
                    6 => Arc::new(MapArray::from(rebuilt.to_data())) as ArrayRef,
                    7 => Arc::new(UnionArray::from(rebuilt.to_data())) as ArrayRef,
                    8 => Arc::new(DictionaryArray::<Int32Type>::from(rebuilt.to_data())) as ArrayRef,
                    9 => Arc::new(RunArray::<Int32Type>::from(rebuilt.to_data())) as ArrayRef,
                    _ => rebuilt.clone(),
                };
                drop(rebuilt);
                assert_eq!(typed.len(), 1);

                // Descend iteratively through descriptors to verify leaf value [42].
                let exported = typed.to_data();
                exported.validate_full().unwrap();
                let mut current = &exported;
                while !current.child_data().is_empty() {
                    current = current.child_data().last().unwrap();
                }
                assert_eq!(current.buffer::<i32>(0), &[42]);

                // Independent child retention after outer descriptor and ArrayRef drop.
                let retained_child = make_array(exported.child_data().last().unwrap().clone());
                drop(exported);
                drop(typed);
                assert_eq!(retained_child.len(), 1);
                drop(retained_child);
                assert!(weak_leaf.upgrade().is_none(), "shape {shape}");
            }

            // Deep unwind cleanup: outer valid Struct has a deep left sibling +
            // failing right sibling (FixedSizeList referencing mismatched child type -> panic).
            let live_left: ArrayRef = deep(
                Arc::new(Int32Array::from(vec![7])),
                4_096,
                None,
            );
            let weak_left = Arc::downgrade(&live_left);
            let bad_inner = unsafe {
                ArrayDataBuilder::new(DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Int32, true)),
                    2,
                ))
                .len(1)
                // Missing child_data triggers index-out-of-bounds panic deep inside make_array.
                .build_unchecked()
            };
            let deep_bad = {
                let mut cur = bad_inner;
                for _ in 0..4_096 {
                    let field = Arc::new(Field::new("item", cur.data_type().clone(), true));
                    cur = unsafe {
                        ArrayDataBuilder::new(DataType::List(field))
                            .len(1)
                            .buffers(vec![arrow_buffer::Buffer::from_slice_ref([0_i32, 1])])
                            .child_data(vec![cur])
                            .build_unchecked()
                    };
                }
                cur
            };
            let root = unsafe {
                ArrayDataBuilder::new(DataType::Struct(Fields::from([
                    Arc::new(Field::new("ok", live_left.data_type().clone(), true)),
                    Arc::new(Field::new("bad", deep_bad.data_type().clone(), true)),
                ])))
                .len(1)
                .child_data(vec![live_left.to_data(), deep_bad])
                .build_unchecked()
            };
            drop(live_left);
            let panicked = catch_unwind(AssertUnwindSafe(|| make_array(root)));
            assert!(panicked.is_err());
            assert!(weak_left.upgrade().is_none());
        },
    );
}
