// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, Fields, UnionFields};

use super::super::release::tests::{deep, small_stack, wrap};
use crate::types::*;
use crate::*;

// This dispatcher is only a shallow test oracle for the concrete fixtures.
// Production dispatch uses the actual trait hook and never uses as_any.
pub(in super::super) fn reference_data(array: &dyn Array) -> ArrayData {
    macro_rules! concrete {
        ($ty:ty, $module:ident) => {
            if let Some(array) = array.as_any().downcast_ref::<$ty>() {
                return super::super::$module::reference_data(array.clone());
            }
        };
    }
    concrete!(ListArray, list_array);
    concrete!(LargeListArray, list_array);
    concrete!(ListViewArray, list_view_array);
    concrete!(LargeListViewArray, list_view_array);
    concrete!(FixedSizeListArray, fixed_size_list_array);
    concrete!(StructArray, struct_array);
    concrete!(MapArray, map_array);
    concrete!(UnionArray, union_array);
    concrete!(DictionaryArray<Int8Type>, dictionary_array);
    concrete!(DictionaryArray<Int16Type>, dictionary_array);
    concrete!(DictionaryArray<Int32Type>, dictionary_array);
    concrete!(DictionaryArray<Int64Type>, dictionary_array);
    concrete!(DictionaryArray<UInt8Type>, dictionary_array);
    concrete!(DictionaryArray<UInt16Type>, dictionary_array);
    concrete!(DictionaryArray<UInt32Type>, dictionary_array);
    concrete!(DictionaryArray<UInt64Type>, dictionary_array);
    concrete!(RunArray<Int16Type>, run_array);
    concrete!(RunArray<Int32Type>, run_array);
    concrete!(RunArray<Int64Type>, run_array);
    array.to_data()
}

fn same_descriptor(actual: &ArrayData, expected: &ArrayData) {
    let mut work = vec![(actual, expected)];
    while let Some((a, e)) = work.pop() {
        assert_eq!(a.data_type(), e.data_type());
        assert_eq!(
            (a.len(), a.offset(), a.child_data().len()),
            (e.len(), e.offset(), e.child_data().len())
        );
        assert_eq!(a.buffers().len(), e.buffers().len());
        for (a, e) in a.buffers().iter().zip(e.buffers()) {
            assert_eq!(
                (a.as_ptr(), a.len(), a.capacity()),
                (e.as_ptr(), e.len(), e.capacity())
            );
            assert_eq!(a.as_slice(), e.as_slice());
        }
        match (a.nulls(), e.nulls()) {
            (None, None) => {}
            (Some(a), Some(e)) => {
                assert_eq!(
                    (a.buffer().as_ptr(), a.offset(), a.len(), a.null_count()),
                    (e.buffer().as_ptr(), e.offset(), e.len(), e.null_count())
                );
            }
            _ => panic!("changed null representation"),
        }
        work.extend(a.child_data().iter().zip(e.child_data()));
    }
}

#[test]
fn original_descriptor_export_buffers_metadata_and_offsets() {
    let leaf: ArrayRef = Arc::new(Int32Array::from(vec![Some(42)]));
    for shape in 0..10 {
        for depth in 0..6 {
            let source = deep(leaf.clone(), depth, Some(shape));
            let actual = source.to_data();
            let expected = reference_data(source.as_ref());
            same_descriptor(&actual, &expected);
            actual.validate_full().unwrap();
            for (offset, len) in [(0, 0), (0, 1), (1, 0)] {
                let slice = source.slice(offset, len);
                same_descriptor(&slice.to_data(), &reference_data(slice.as_ref()));
            }
        }
    }
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    let field = Arc::new(
        Field::new("named", DataType::Int32, true)
            .with_metadata([("key".into(), "value".into())].into()),
    );
    let list = ListArray::new(
        field,
        OffsetBuffer::new(vec![0, 1, 3, 4].into()),
        values,
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let list = list.slice(1, 2);
    let expected = reference_data(&list);
    same_descriptor(&list.to_data(), &expected);
    same_descriptor(&list.into_data(), &expected);

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
            vec![5_i8, 2].into(),
            dense.then(|| vec![0_i32, 1].into()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![3, 4])),
            ],
        )
        .unwrap();
        same_descriptor(&union.to_data(), &reference_data(&union));
    }
}

#[test]
fn all_dictionary_key_and_run_end_widths_export() {
    macro_rules! dictionary {
        ($($ty:ty),+) => {$(
            let array = DictionaryArray::<$ty>::new(PrimitiveArray::<$ty>::from(vec![Some(0), None]),
                wrap(Arc::new(Int32Array::from(vec![42])), 0));
            same_descriptor(&array.to_data(), &reference_data(&array));
        )+};
    }
    dictionary!(
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type
    );
    macro_rules! runs {
        ($($ty:ty),+) => {$(
            let values = wrap(Arc::new(Int32Array::from(vec![42])), 5);
            let array = RunArray::<$ty>::try_new(&PrimitiveArray::<$ty>::from(vec![3]), values.as_ref()).unwrap().slice(1, 2);
            same_descriptor(&array.to_data(), &reference_data(&array));
            array.to_data().validate_full().unwrap();
        )+};
    }
    runs!(Int16Type, Int32Type, Int64Type);
}

#[test]
fn deep_descriptor_export_and_independently_retained_result() {
    small_stack(
        "array::data_export::tests::deep_descriptor_export_and_independently_retained_result",
        || {
            for shape in 0..11 {
                let leaf: ArrayRef = Arc::new(Int32Array::from(vec![42]));
                let source = deep(
                    leaf,
                    if shape == 8 { 512 } else { 8_192 },
                    (shape < 10).then_some(shape),
                );
                let descriptor = source.to_data();
                descriptor.validate_full().unwrap();
                drop(source);
                let mut current = &descriptor;
                while !current.child_data().is_empty() {
                    current = current.child_data().last().unwrap();
                }
                assert_eq!(current.buffer::<i32>(0), &[42]);
                let retained = descriptor.child_data().last().unwrap().clone();
                drop(descriptor);
                retained.validate_full().unwrap();
                drop(retained);
            }
        },
    );
}

#[derive(Debug)]
struct Callback {
    values: Int32Array,
    events: Arc<Mutex<Vec<&'static str>>>,
    name: &'static str,
    fail: bool,
}

// SAFETY: Every successful method forwards an actual Int32 array unchanged;
// the selected test callback may panic before returning a descriptor.
unsafe impl Array for Callback {
    fn as_any(&self) -> &dyn Any {
        &self.values
    }
    fn to_data(&self) -> ArrayData {
        self.events.lock().unwrap().push(self.name);
        assert!(!self.fail, "selected export callback");
        self.values.to_data()
    }
    fn into_data(self) -> ArrayData {
        self.to_data()
    }
    fn data_type(&self) -> &DataType {
        self.values.data_type()
    }
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.values.slice(offset, length))
    }
    fn len(&self) -> usize {
        self.values.len()
    }
    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    fn offset(&self) -> usize {
        self.values.offset()
    }
    fn nulls(&self) -> Option<&NullBuffer> {
        self.values.nulls()
    }
    fn get_buffer_memory_size(&self) -> usize {
        self.values.get_buffer_memory_size()
    }
    fn get_array_memory_size(&self) -> usize {
        self.values.get_array_memory_size()
    }
}

#[test]
fn custom_callbacks_keep_order_stop_and_forwarding_semantics() {
    small_stack(
        "array::data_export::tests::custom_callbacks_keep_order_stop_and_forwarding_semantics",
        || {
            for fail in [false, true] {
                let events = Arc::new(Mutex::new(Vec::new()));
                let arrays: Vec<ArrayRef> = ["first", "middle", "last"]
                    .into_iter()
                    .map(|name| {
                        Arc::new(Callback {
                            values: Int32Array::from(vec![42]),
                            events: events.clone(),
                            name,
                            fail: fail && name == "middle",
                        }) as ArrayRef
                    })
                    .collect();
                let witnesses: Vec<_> = arrays.iter().map(Arc::downgrade).collect();
                let fields: Fields = arrays
                    .iter()
                    .map(|a| Field::new("x", a.data_type().clone(), false))
                    .collect();
                let root: ArrayRef = Arc::new(StructArray::new(fields, arrays, None));
                let root = deep(root, 8_192, Some(0));
                let result = catch_unwind(AssertUnwindSafe(|| root.to_data()));
                assert_eq!(result.is_err(), fail);
                assert_eq!(
                    events.lock().unwrap().as_slice(),
                    if fail {
                        &["first", "middle"][..]
                    } else {
                        &["first", "middle", "last"][..]
                    }
                );
                drop(root);
                assert!(witnesses.iter().all(|w| w.upgrade().is_none()));
                drop(result);
            }
        },
    );
}
