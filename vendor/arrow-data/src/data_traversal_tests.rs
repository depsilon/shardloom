// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use arrow_buffer::{Buffer, NullBuffer};
use arrow_schema::{ArrowError, DataType, Field, Fields, UnionFields, UnionMode};

use super::ArrayData;
use super::children::tests::{deep, leaf, same_tree_and_buffers, small_stack, wrap};

#[derive(Debug, PartialEq)]
enum Outcome {
    Ok,
    Error(String),
    Panic(String),
}

fn outcome(f: impl FnOnce() -> Result<(), ArrowError>) -> Outcome {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => Outcome::Ok,
        Ok(Err(e)) => Outcome::Error(format!("{e:?}")),
        Err(e) => Outcome::Panic(
            e.downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "non-string panic".into()),
        ),
    }
}

fn parity(data: &ArrayData) {
    assert_eq!(
        outcome(|| data.validate()),
        outcome(|| data.reference_validate())
    );
    assert_eq!(
        outcome(|| data.validate_data()),
        outcome(|| data.reference_validate_data())
    );
    assert_eq!(
        outcome(|| data.validate_full()),
        outcome(|| data.reference_validate_full())
    );
}

fn change(data: &mut ArrayData, mutation: usize) {
    match mutation {
        0 => {}
        1 => data.len = 0,
        2 => data.len = 2,
        3 => data.offset = 1,
        4 => data.offset = usize::MAX,
        5 => data.buffers.clear(),
        6 => data.buffers.push(Buffer::from_slice_ref([0_i32])),
        7 => data.child_data.clear(),
        8 => data.child_data.push(leaf(0)),
        9 => data.nulls = Some(NullBuffer::new_null(1)),
        10 => data.nulls = Some(NullBuffer::new_null(2)),
        11 => data.data_type = DataType::Null,
        _ => unreachable!(),
    }
}

fn struct_data(children: Vec<ArrayData>) -> ArrayData {
    let fields = children
        .iter()
        .enumerate()
        .map(|(i, data)| Arc::new(Field::new(format!("f{i}"), data.data_type.clone(), true)))
        .collect::<Fields>();
    ArrayData {
        data_type: DataType::Struct(fields),
        len: 1,
        offset: 0,
        buffers: vec![],
        child_data: children.into(),
        nulls: None,
    }
}

#[test]
fn original_validation_errors_and_competing_fault_order() {
    for shape in 0..10 {
        let base = wrap(wrap(leaf(42), (shape + 1) % 10), shape);
        parity(&base);
        for parent_change in 0..12 {
            for child_change in 0..12 {
                let mut data = base.clone();
                change(&mut data.child_data[0], child_change);
                change(&mut data, parent_change);
                parity(&data);
            }
        }
    }
    for first in 0..12 {
        for second in 0..12 {
            let mut data = struct_data(vec![leaf(1), leaf(2)]);
            change(&mut data.child_data[0], first);
            change(&mut data.child_data[1], second);
            parity(&data);
        }
    }
    // Type-specific checks that run before, between and after child validation.
    let field = Arc::new(Field::new("x", DataType::Int32, true));
    let types = [
        DataType::FixedSizeList(field.clone(), -1),
        DataType::Map(field.clone(), false),
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::empty()),
                false,
            )),
            true,
        ),
        DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Int32)),
        DataType::RunEndEncoded(field.clone(), field.clone()),
        DataType::Union(
            UnionFields::try_new([3], [field]).unwrap(),
            UnionMode::Sparse,
        ),
    ];
    for dtype in types {
        for shape in 0..10 {
            let mut data = wrap(leaf(1), shape);
            data.data_type = dtype.clone();
            parity(&data);
            data.child_data[0].buffers.clear();
            parity(&data);
        }
    }
}

fn invalid_utf8() -> ArrayData {
    ArrayData {
        data_type: DataType::Utf8,
        len: 1,
        offset: 0,
        buffers: vec![
            Buffer::from_slice_ref([0_i32, 1]),
            Buffer::from_slice_ref([0xff_u8]),
        ],
        child_data: vec![].into(),
        nulls: None,
    }
}

#[test]
fn full_validation_keeps_nested_error_wrappers_and_root_priority() {
    for shape in 0..10 {
        let data = deep(invalid_utf8(), 5, Some(shape));
        data.validate().unwrap();
        parity(&data);
        assert!(data.validate_full().is_err());
    }
    // A later cheap layout error must precede an earlier invalid value, since
    // the root's first cheap pass visits the complete descriptor tree.
    let mut later = leaf(4);
    later.buffers.clear();
    let data = struct_data(vec![invalid_utf8(), later]);
    parity(&data);
    let error = data.validate_full().unwrap_err().to_string();
    assert!(error.contains("Expected 1 buffers"));
    assert!(!error.contains("child #"));

    // Once cheap checks pass, parent value checks precede child value checks.
    let mut dictionary = wrap(invalid_utf8(), 6);
    dictionary.buffers[0] = Buffer::from_slice_ref([5_i8]);
    parity(&dictionary);
    assert!(
        dictionary
            .validate_full()
            .unwrap_err()
            .to_string()
            .contains("Value at position 0")
    );
}

#[test]
fn original_inspection_slice_and_alignment_contracts() {
    for shape in 0..10 {
        for depth in 0..5 {
            let data = deep(leaf(42), depth, Some(shape));
            assert_eq!(
                data.get_buffer_memory_size(),
                data.reference_get_buffer_memory_size()
            );
            assert_eq!(
                data.get_array_memory_size(),
                data.reference_get_array_memory_size()
            );
            assert_eq!(
                data.get_slice_memory_size().unwrap(),
                data.reference_get_slice_memory_size().unwrap()
            );
            for (offset, length) in [(0, 0), (0, 1), (1, 0)] {
                let actual = data.slice(offset, length);
                let expected = data.reference_slice(offset, length);
                same_tree_and_buffers(&actual, &expected);
            }
            for (offset, length) in [(usize::MAX, 1), (1, 1)] {
                assert_eq!(
                    outcome(|| {
                        data.slice(offset, length);
                        Ok(())
                    }),
                    outcome(|| {
                        data.reference_slice(offset, length);
                        Ok(())
                    })
                );
            }
        }
    }
    let mut unaligned = leaf(1);
    unaligned.buffers[0] = Buffer::from_slice_ref([0_u8; 5]).slice(1);
    let mut data = struct_data(vec![unaligned, leaf(2)]);
    let mut expected = data.clone();
    let unchanged = data.child_data[1].buffers[0].as_ptr();
    data.align_buffers();
    expected.reference_align_buffers();
    data.validate_full().unwrap();
    assert_eq!(data.child_data[1].buffers[0].as_ptr(), unchanged);
    for (a, b) in super::traversal::Walk::new(&data).zip(super::traversal::Walk::new(&expected)) {
        assert_eq!(a.data_type, b.data_type);
        assert_eq!(a.buffers, b.buffers);
        for buffer in &a.buffers {
            assert_eq!(buffer.as_ptr().align_offset(4), 0);
        }
    }
}

#[test]
fn deep_validation_inspection_alignment_and_slice() {
    small_stack(
        "data::traversal_tests::deep_validation_inspection_alignment_and_slice",
        || {
            for shape in 0..11 {
                let depth = if shape == 6 { 512 } else { 8_192 };
                let mut data = deep(leaf(42), depth, (shape < 10).then_some(shape));
                data.validate_full().unwrap();
                let buffer_bytes = data.get_buffer_memory_size();
                assert!(buffer_bytes > 0);
                assert!(data.get_array_memory_size() > buffer_bytes);
                assert!(data.get_slice_memory_size().unwrap() > 0);
                data.align_buffers();
                let sliced = data.slice(0, 1);
                same_tree_and_buffers(&data, &sliced);
                sliced.validate_full().unwrap();
                let empty = data.slice(0, 0);
                empty.validate_full().unwrap();
            }
            let mut broken = leaf(0);
            broken.buffers.clear();
            let data = deep(broken, 8_192, Some(0));
            assert_eq!(
                data.validate_full().unwrap_err().to_string(),
                "Invalid argument error: Expected 1 buffers in array of type Int32, got 0"
            );
            // Error text includes each ancestor's whole dtype. Keep this text test
            // finite while separately proving an 8192-level failure above.
            let data = deep(invalid_utf8(), 128, Some(0));
            assert!(
                data.validate_full()
                    .unwrap_err()
                    .to_string()
                    .contains("child #0 invalid")
            );
        },
    );
}

#[test]
fn wide_last_child_validation_and_struct_offset_propagation() {
    let data = struct_data((0..4096).map(leaf).collect());
    data.validate_full().unwrap();
    let actual = data.slice(0, 0);
    let expected = data.reference_slice(0, 0);
    same_tree_and_buffers(&actual, &expected);
    let mut broken = data.clone();
    broken.child_data.last_mut().unwrap().buffers.clear();
    parity(&broken);

    let mut values = leaf(0);
    values.len = 10;
    values.buffers[0] = Buffer::from_slice_ref((0..10).collect::<Vec<i32>>());
    let mut inner = struct_data(vec![values]);
    inner.len = 6;
    inner.offset = 2;
    inner.nulls = Some(NullBuffer::from(vec![true, false, true, true, true, true]));
    let mut outer = struct_data(vec![inner]);
    outer.len = 3;
    outer.offset = 1;
    let actual = outer.slice(1, 2).slice(1, 1);
    let expected = outer.reference_slice(1, 2).reference_slice(1, 1);
    same_tree_and_buffers(&actual, &expected);
    assert_eq!(actual.offset, 0);
    assert_eq!(actual.child_data[0].offset, 0);
    assert_eq!(actual.child_data[0].child_data[0].offset, 5);
}
