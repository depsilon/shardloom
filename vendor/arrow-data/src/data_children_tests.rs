// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::{Arc, Mutex};

use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_schema::{DataType, Field, Fields, UnionFields, UnionMode};

use super::super::ArrayData;

pub(in super::super) fn small_stack(case: &'static str, run: impl FnOnce() + Send + 'static) {
    const MARKER: &str = "SHARDLOOM_ARROW_DATA_SMALL_STACK";
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

pub(in super::super) fn leaf(value: i32) -> ArrayData {
    ArrayData::builder(DataType::Int32)
        .len(1)
        .add_buffer(Buffer::from_slice_ref([value]))
        .build()
        .unwrap()
}

// Valid raw descriptor fixtures isolate ownership from public recursive
// validation, which is a separate obligation. Shallow controls validate them.
pub(in super::super) fn wrap(child: ArrayData, shape: usize) -> ArrayData {
    let field = Arc::new(Field::new("item", child.data_type.clone(), true));
    let (data_type, buffers, children) = match shape {
        0 => (DataType::Struct(Fields::from([field])), vec![], vec![child]),
        1 => (
            DataType::List(field),
            vec![Buffer::from_slice_ref([0_i32, 1])],
            vec![child],
        ),
        2 => (
            DataType::LargeList(field),
            vec![Buffer::from_slice_ref([0_i64, 1])],
            vec![child],
        ),
        3 => (DataType::FixedSizeList(field, 1), vec![], vec![child]),
        4 => (
            DataType::ListView(field),
            vec![
                Buffer::from_slice_ref([0_i32]),
                Buffer::from_slice_ref([1_i32]),
            ],
            vec![child],
        ),
        5 => (
            DataType::LargeListView(field),
            vec![
                Buffer::from_slice_ref([0_i64]),
                Buffer::from_slice_ref([1_i64]),
            ],
            vec![child],
        ),
        6 => (
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(child.data_type.clone())),
            vec![Buffer::from_slice_ref([0_i8])],
            vec![child],
        ),
        7 => (
            DataType::Union(
                UnionFields::try_new([0], [field]).unwrap(),
                UnionMode::Dense,
            ),
            vec![
                Buffer::from_slice_ref([0_i8]),
                Buffer::from_slice_ref([0_i32]),
            ],
            vec![child],
        ),
        8 => (
            DataType::RunEndEncoded(Arc::new(Field::new("ends", DataType::Int32, false)), field),
            vec![],
            vec![leaf(1), child],
        ),
        9 => {
            let fields = Fields::from([Arc::new(Field::new("key", DataType::Int32, false)), field]);
            let entries = ArrayData {
                data_type: DataType::Struct(fields),
                len: 1,
                offset: 0,
                buffers: vec![],
                child_data: vec![leaf(7), child].into(),
                nulls: None,
            };
            (
                DataType::Map(
                    Arc::new(Field::new("entries", entries.data_type.clone(), false)),
                    false,
                ),
                vec![Buffer::from_slice_ref([0_i32, 1])],
                vec![entries],
            )
        }
        _ => unreachable!(),
    };
    ArrayData {
        data_type,
        len: 1,
        offset: 0,
        buffers,
        child_data: children.into(),
        nulls: None,
    }
}

pub(in super::super) fn deep(mut data: ArrayData, depth: usize, shape: Option<usize>) -> ArrayData {
    for level in 0..depth {
        data = wrap(data, shape.unwrap_or(level % 10));
    }
    data
}

pub(in super::super) fn same_tree_and_buffers(left: &ArrayData, right: &ArrayData) -> usize {
    let mut work = vec![(left, right)];
    let mut count = 0;
    while let Some((a, b)) = work.pop() {
        count += 1;
        assert!(a.data_type == b.data_type);
        assert_eq!(
            (a.len, a.offset, a.buffers.len(), a.child_data.len()),
            (b.len, b.offset, b.buffers.len(), b.child_data.len())
        );
        assert_eq!(b.child_data.capacity(), b.child_data.len());
        for (a, b) in a.buffers.iter().zip(&b.buffers) {
            assert_eq!(
                (a.as_ptr(), a.len(), a.capacity()),
                (b.as_ptr(), b.len(), b.capacity())
            );
        }
        match (&a.nulls, &b.nulls) {
            (Some(a), Some(b)) => assert_eq!(
                (a.buffer().as_ptr(), a.offset(), a.len()),
                (b.buffer().as_ptr(), b.offset(), b.len())
            ),
            (None, None) => {}
            _ => panic!("lost null owner"),
        }
        work.extend(a.child_data.iter().zip(b.child_data.iter()));
    }
    count
}

#[test]
fn deep_descriptor_clone_release_and_public_parts() {
    small_stack(
        "data::children::tests::deep_descriptor_clone_release_and_public_parts",
        || {
            for shape in 0..11 {
                let depth = if shape == 6 { 512 } else { 8_192 };
                let data = deep(leaf(42), depth, (shape < 10).then_some(shape));
                let copy = data.clone();
                assert!(same_tree_and_buffers(&data, &copy) > depth);
                drop(data);
                let (_, _, _, _, _, children) = copy.into_parts();
                drop(children);
            }
        },
    );
}

#[test]
fn shallow_descriptor_controls_and_wide_last_sibling() {
    for shape in 0..10 {
        let data = wrap(leaf(42), shape);
        data.validate_full().unwrap();
        let copy = data.clone();
        assert_eq!(copy, data);
        same_tree_and_buffers(&data, &copy);
    }
    small_stack(
        "data::children::tests::shallow_descriptor_controls_and_wide_last_sibling",
        || {
            let count = 4_096;
            let children: Vec<_> = (0..count).map(|i| leaf(i as i32)).collect();
            let fields: Fields = (0..count)
                .map(|i| Field::new(format!("f{i}"), DataType::Int32, true))
                .collect();
            let data = ArrayData {
                data_type: DataType::Struct(fields),
                len: 1,
                offset: 0,
                buffers: vec![],
                child_data: children.into(),
                nulls: None,
            };
            let copy = data.clone();
            assert_eq!(same_tree_and_buffers(&data, &copy), count + 1);
            assert_eq!(
                copy.child_data[count - 1].buffers[0].typed_data::<i32>(),
                &[4095]
            );
            let bad = deep(leaf(3), 8_192, None)
                .into_builder()
                .offset(usize::MAX)
                .len(2)
                .build();
            assert!(bad.is_err());
        },
    );
}

#[derive(Debug)]
struct Observed {
    values: Box<[i32]>,
    label: &'static str,
    events: Arc<Mutex<Vec<&'static str>>>,
    panic: bool,
}

impl Drop for Observed {
    fn drop(&mut self) {
        self.events.lock().unwrap().push(self.label);
        assert!(!self.panic, "selected buffer panic");
    }
}

fn buffer(label: &'static str, events: &Arc<Mutex<Vec<&'static str>>>, panic: bool) -> Buffer {
    let owner = Arc::new(Observed {
        values: vec![1].into_boxed_slice(),
        label,
        events: events.clone(),
        panic,
    });
    let ptr = std::ptr::NonNull::new(owner.values.as_ptr() as *mut u8).unwrap();
    // SAFETY: owner retains this aligned, immutable four-byte allocation for
    // the complete custom buffer lifetime.
    unsafe { Buffer::from_custom_allocation(ptr, 4, owner) }
}

#[test]
fn descriptor_panic_keeps_current_children_before_outer_tails() {
    small_stack(
        "data::children::tests::descriptor_panic_keeps_current_children_before_outer_tails",
        || {
            for panic in [false, true] {
                let events = Arc::new(Mutex::new(Vec::new()));
                let mut child = leaf(1);
                child.buffers[0] = buffer("child", &events, false);
                let mut inner = wrap(child, 0);
                // Ownership-only diagnostic fixture: the extra struct buffer is
                // deliberately never interpreted or admitted as an Arrow array.
                inner.buffers.push(buffer("prefix", &events, panic));
                inner.nulls = Some(NullBuffer::new(BooleanBuffer::new(
                    buffer("inner nulls", &events, false),
                    0,
                    1,
                )));
                let mut sibling = leaf(2);
                sibling.buffers[0] = buffer("sibling", &events, false);
                let mut root = ArrayData {
                    data_type: DataType::Struct(Fields::empty()),
                    len: 1,
                    offset: 0,
                    buffers: vec![],
                    child_data: vec![inner, sibling].into(),
                    nulls: None,
                };
                root.nulls = Some(NullBuffer::new(BooleanBuffer::new(
                    buffer("outer nulls", &events, false),
                    0,
                    1,
                )));
                let root = deep(root, 2_048, Some(0));
                assert_eq!(
                    catch_unwind(AssertUnwindSafe(|| drop(root))).is_err(),
                    panic
                );
                assert_eq!(
                    *events.lock().unwrap(),
                    ["prefix", "child", "inner nulls", "sibling", "outer nulls"]
                );
            }
        },
    );
}
