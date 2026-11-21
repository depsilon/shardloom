// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::{Arc, Barrier, Mutex};

use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, UnionFields};

use crate::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use crate::*;

pub(in super::super) fn small_stack(case: &'static str, run: impl FnOnce() + Send + 'static) {
    const MARKER: &str = "SHARDLOOM_ARROW_ARRAY_SMALL_STACK";
    if std::env::var(MARKER).as_deref() != Ok(case) {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", case, "--nocapture"])
            .env(MARKER, case)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "small-stack child failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    std::thread::Builder::new()
        .stack_size(128 * 1024)
        .spawn(run)
        .unwrap()
        .join()
        .unwrap();
}

pub(in super::super) fn wrap(values: ArrayRef, shape: usize) -> ArrayRef {
    let field = Arc::new(Field::new("item", values.data_type().clone(), true));
    match shape {
        0 => Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(vec![0, 1].into()),
            values,
            None,
        )),
        1 => Arc::new(LargeListArray::new(
            field,
            OffsetBuffer::new(vec![0, 1].into()),
            values,
            None,
        )),
        2 => Arc::new(ListViewArray::new(
            field,
            vec![0].into(),
            vec![1].into(),
            values,
            None,
        )),
        3 => Arc::new(LargeListViewArray::new(
            field,
            vec![0].into(),
            vec![1].into(),
            values,
            None,
        )),
        4 => Arc::new(FixedSizeListArray::new(field, 1, values, None)),
        5 => Arc::new(StructArray::new(Fields::from([field]), vec![values], None)),
        6 => {
            let key: ArrayRef = Arc::new(Int32Array::from(vec![7]));
            let entries = StructArray::new(
                Fields::from([Arc::new(Field::new("key", DataType::Int32, false)), field]),
                vec![key, values],
                None,
            );
            Arc::new(MapArray::new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::new(vec![0, 1].into()),
                entries,
                None,
                false,
            ))
        }
        7 => Arc::new(UnionArray::release_test_child(values)),
        8 => Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![0]),
            values,
        )),
        9 => Arc::new(RunArray::<Int32Type>::release_test_child(values)),
        _ => unreachable!(),
    }
}

pub(in super::super) fn deep(mut array: ArrayRef, depth: usize, shape: Option<usize>) -> ArrayRef {
    for level in 0..depth {
        array = wrap(array, shape.unwrap_or(level % 10));
    }
    array
}

#[test]
fn deep_builtin_array_final_owners() {
    small_stack(
        "array::release::tests::deep_builtin_array_final_owners",
        || {
            for shape in 0..11 {
                let leaf: ArrayRef = Arc::new(Int32Array::from(vec![42]));
                let weak = Arc::downgrade(&leaf);
                // Pure dictionary nesting owns boxed schema copies at every level;
                // mixed nesting retains shared schema subtrees and reaches 8192.
                let depth = if shape == 8 { 512 } else { 8_192 };
                let array = deep(leaf, depth, (shape < 10).then_some(shape));
                drop(array);
                assert!(weak.upgrade().is_none(), "shape {shape}");
            }
        },
    );
}

#[test]
fn detached_children_clones_and_concurrent_last_owners() {
    small_stack(
        "array::release::tests::detached_children_clones_and_concurrent_last_owners",
        || {
            let leaf: ArrayRef = Arc::new(Int32Array::from(vec![42]));
            let weak = Arc::downgrade(&leaf);
            let array = deep(leaf, 8_192, None);
            let parent = ListArray::new(
                Arc::new(Field::new("item", array.data_type().clone(), true)),
                OffsetBuffer::new(vec![0, 1].into()),
                array,
                None,
            );
            let detached = parent.values().clone();
            let clone = parent.clone();
            assert!(Arc::ptr_eq(parent.values(), clone.values()));
            assert_eq!(parent.offsets().as_ptr(), clone.offsets().as_ptr());
            drop(parent);
            drop(clone);
            assert!(weak.upgrade().is_some());
            let weak_array = Arc::downgrade(&detached);
            let barrier = Arc::new(Barrier::new(4));
            let threads: Vec<_> = (0..4)
                .map(|_| {
                    let root = detached.clone();
                    let barrier = barrier.clone();
                    std::thread::Builder::new()
                        .stack_size(128 * 1024)
                        .spawn(move || {
                            barrier.wait();
                            drop(root);
                        })
                        .unwrap()
                })
                .collect();
            drop(detached);
            for thread in threads {
                thread.join().unwrap();
            }
            assert!(weak_array.upgrade().is_none());
            assert!(weak.upgrade().is_none());

            let retained: ArrayRef = deep(Arc::new(Int32Array::from(vec![9])), 8_192, None);
            let survivor = retained.clone();
            let root = wrap(retained, 5);
            drop(root);
            assert_eq!(survivor.len(), 1);
            drop(survivor);
        },
    );
}

#[derive(Debug)]
struct ObservedBuffer {
    values: Box<[i32]>,
    label: &'static str,
    events: Arc<Mutex<Vec<&'static str>>>,
    panic: bool,
}

impl Drop for ObservedBuffer {
    fn drop(&mut self) {
        self.events.lock().unwrap().push(self.label);
        assert!(!self.panic, "selected owner panic");
    }
}

fn observed(
    values: &[i32],
    label: &'static str,
    events: &Arc<Mutex<Vec<&'static str>>>,
    panic: bool,
) -> ScalarBuffer<i32> {
    let owner = Arc::new(ObservedBuffer {
        values: values.into(),
        label,
        events: events.clone(),
        panic,
    });
    let length = owner.values.len();
    let ptr = std::ptr::NonNull::new(owner.values.as_ptr() as *mut u8).unwrap();
    // SAFETY: the Arc retains this immutable, properly aligned i32 allocation
    // for the entire custom buffer lifetime; length covers exactly its bytes.
    let buffer = unsafe { Buffer::from_custom_allocation(ptr, length * 4, owner) };
    ScalarBuffer::new(buffer, 0, length)
}

#[test]
fn declaration_order_and_single_panic_unwind() {
    small_stack(
        "array::release::tests::declaration_order_and_single_panic_unwind",
        || {
            for panic in [false, true] {
                let events = Arc::new(Mutex::new(Vec::new()));
                let first: ArrayRef = Arc::new(Int32Array::new(
                    observed(&[1], "first", &events, panic),
                    None,
                ));
                let last: ArrayRef = Arc::new(Int32Array::new(
                    observed(&[2], "last", &events, false),
                    None,
                ));
                let fields = Fields::from([
                    Arc::new(Field::new("first", DataType::Int32, true)),
                    Arc::new(Field::new("last", DataType::Int32, true)),
                ]);
                let child: ArrayRef = Arc::new(StructArray::new(fields, vec![first, last], None));
                let child = deep(child, 2_048, Some(0));
                let parent: ArrayRef = Arc::new(ListArray::new(
                    Arc::new(Field::new("item", child.data_type().clone(), true)),
                    OffsetBuffer::new(observed(&[0, 1], "outer offsets", &events, false)),
                    child,
                    None,
                ));
                let result = catch_unwind(AssertUnwindSafe(|| drop(parent)));
                assert_eq!(result.is_err(), panic);
                assert_eq!(*events.lock().unwrap(), ["first", "last", "outer offsets"]);
            }
        },
    );
}

#[test]
fn public_parts_and_small_constructor_controls() {
    let leaf: ArrayRef = Arc::new(Int32Array::from(vec![5]));
    for shape in 0..10 {
        let array = wrap(leaf.clone(), shape);
        let cloned = make_array(array.to_data());
        assert_eq!(array.to_data(), cloned.to_data());
        assert_eq!(array.data_type(), cloned.data_type());
        if shape == 7 {
            let standard = UnionArray::try_new(
                UnionFields::try_new([0], [Field::new("item", DataType::Int32, true)]).unwrap(),
                vec![0_i8].into(),
                Some(vec![0_i32].into()),
                vec![leaf.clone()],
            )
            .unwrap();
            assert_eq!(array.to_data(), standard.to_data());
        }
        if shape == 9 {
            let standard =
                RunArray::<Int32Type>::try_new(&Int32Array::from(vec![1]), leaf.as_ref()).unwrap();
            assert_eq!(array.to_data(), standard.to_data());
        }
    }
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(vec![0, 1].into()),
        leaf.clone(),
        Some(NullBuffer::new_valid(1)),
    );
    let ptr = list.offsets().as_ptr();
    let (_, offsets, values, _) = list.into_parts();
    assert_eq!(ptr, offsets.as_ptr());
    assert!(Arc::ptr_eq(&leaf, &values));
    macro_rules! dictionary {
        ($key:ty) => {{
            let keys = PrimitiveArray::<$key>::from_iter_values([0]);
            let array = DictionaryArray::new(keys, leaf.clone());
            let (_, values) = array.into_parts();
            assert!(Arc::ptr_eq(&values, &leaf));
            drop(wrap(
                Arc::new(DictionaryArray::<$key>::new(
                    PrimitiveArray::<$key>::from_iter_values([0]),
                    values,
                )),
                0,
            ));
        }};
    }
    dictionary!(Int8Type);
    dictionary!(Int16Type);
    dictionary!(Int32Type);
    dictionary!(Int64Type);
    dictionary!(UInt8Type);
    dictionary!(UInt16Type);
    dictionary!(UInt32Type);
    dictionary!(UInt64Type);
    for root in [
        Arc::new(StructArray::new_empty_fields(0, None)) as ArrayRef,
        Arc::new(
            UnionArray::try_new(
                UnionFields::empty(),
                ScalarBuffer::<i8>::from(vec![]),
                None,
                vec![],
            )
            .unwrap(),
        ),
    ] {
        drop(root);
    }
}

#[test]
fn run_end_key_widths_and_null_buffer_drop_order() {
    small_stack(
        "array::release::tests::run_end_key_widths_and_null_buffer_drop_order",
        || {
            macro_rules! run {
                ($key:ty) => {{
                    let leaf: ArrayRef = Arc::new(Int32Array::from(vec![3]));
                    let weak = Arc::downgrade(&leaf);
                    let values = deep(leaf, 8_192, Some(0));
                    let run = RunArray::<$key>::release_test_child(values);
                    drop(wrap(Arc::new(run), 0));
                    assert!(weak.upgrade().is_none());
                }};
            }
            run!(Int16Type);
            run!(Int32Type);
            run!(Int64Type);
            for fixed in [false, true] {
                let events = Arc::new(Mutex::new(Vec::new()));
                let values: ArrayRef = Arc::new(Int32Array::new(
                    observed(&[7], "values", &events, false),
                    None,
                ));
                let nulls = NullBuffer::new(arrow_buffer::BooleanBuffer::new(
                    observed(&[0], "nulls", &events, false).into_inner(),
                    0,
                    1,
                ));
                let field = Arc::new(Field::new("item", DataType::Int32, true));
                let parent: ArrayRef = if fixed {
                    Arc::new(FixedSizeListArray::new(field, 1, values, Some(nulls)))
                } else {
                    Arc::new(ListArray::new(
                        field,
                        OffsetBuffer::new(observed(&[0, 1], "offsets", &events, false)),
                        values,
                        Some(nulls),
                    ))
                };
                drop(wrap(parent, 0));
                let expected = if fixed {
                    vec!["values", "nulls"]
                } else {
                    vec!["nulls", "values", "offsets"]
                };
                assert_eq!(*events.lock().unwrap(), expected);
            }
        },
    );
}

#[test]
fn constructor_error_and_forwarding_array_default_hook() {
    small_stack(
        "array::release::tests::constructor_error_and_forwarding_array_default_hook",
        || {
            let leaf: ArrayRef = Arc::new(Int32Array::from(vec![1]));
            let weak = Arc::downgrade(&leaf);
            let child = deep(leaf, 8_192, None);
            let field = Arc::new(Field::new("item", child.data_type().clone(), true));
            assert!(FixedSizeListArray::try_new(field, 2, child, None).is_err());
            assert!(weak.upgrade().is_none());
            let root = deep(Arc::new(Int32Array::from(vec![1])), 8_192, None);
            // Arc<dyn Array>'s as_any forwards to its inner array. Its allocation
            // is still an Arc wrapper and must use the default hook, not a raw cast.
            let forwarding: ArrayRef = Arc::new(root);
            drop(wrap(forwarding, 0));
        },
    );
}
