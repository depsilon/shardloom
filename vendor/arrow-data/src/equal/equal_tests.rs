// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::sync::Arc;

use arrow_buffer::{Buffer, NullBuffer};
use arrow_schema::{DataType, Field, Fields, UnionFields, UnionMode};

use super::equal;
use crate::ArrayData;

fn int32_leaf(values: Vec<i32>, nulls: Option<Vec<bool>>) -> ArrayData {
    let len = values.len();
    ArrayData::builder(DataType::Int32)
        .len(len)
        .buffers(vec![Buffer::from_slice_ref(values)])
        .nulls(nulls.map(NullBuffer::from))
        .build()
        .unwrap()
}

fn wrap_struct(child: ArrayData, nulls: Option<Vec<bool>>) -> ArrayData {
    let len = child.len();
    let field = Arc::new(Field::new("item", child.data_type().clone(), true));
    ArrayData::builder(DataType::Struct(vec![field].into()))
        .len(len)
        .child_data(vec![child])
        .nulls(nulls.map(NullBuffer::from))
        .build()
        .unwrap()
}

fn wrap_list(child: ArrayData, large: bool, nulls: Option<Vec<bool>>) -> ArrayData {
    let len = child.len();
    let field = Arc::new(Field::new("item", child.data_type().clone(), true));
    let (dtype, offsets) = if large {
        let offsets = (0..=len as i64).collect::<Vec<_>>();
        (DataType::LargeList(field), Buffer::from_slice_ref(offsets))
    } else {
        let offsets = (0..=len as i32).collect::<Vec<_>>();
        (DataType::List(field), Buffer::from_slice_ref(offsets))
    };
    ArrayData::builder(dtype)
        .len(len)
        .buffers(vec![offsets])
        .child_data(vec![child])
        .nulls(nulls.map(NullBuffer::from))
        .build()
        .unwrap()
}

fn wrap_list_view(child: ArrayData, large: bool, nulls: Option<Vec<bool>>) -> ArrayData {
    let len = child.len();
    let field = Arc::new(Field::new("item", child.data_type().clone(), true));
    let (dtype, offsets, sizes) = if large {
        let offsets = (0..len as i64).collect::<Vec<_>>();
        let sizes = vec![1_i64; len];
        (
            DataType::LargeListView(field),
            Buffer::from_slice_ref(offsets),
            Buffer::from_slice_ref(sizes),
        )
    } else {
        let offsets = (0..len as i32).collect::<Vec<_>>();
        let sizes = vec![1_i32; len];
        (
            DataType::ListView(field),
            Buffer::from_slice_ref(offsets),
            Buffer::from_slice_ref(sizes),
        )
    };
    ArrayData::builder(dtype)
        .len(len)
        .buffers(vec![offsets, sizes])
        .child_data(vec![child])
        .nulls(nulls.map(NullBuffer::from))
        .build()
        .unwrap()
}

fn wrap_fixed_list(child: ArrayData, nulls: Option<Vec<bool>>) -> ArrayData {
    let len = child.len();
    let field = Arc::new(Field::new("item", child.data_type().clone(), true));
    ArrayData::builder(DataType::FixedSizeList(field, 1))
        .len(len)
        .child_data(vec![child])
        .nulls(nulls.map(NullBuffer::from))
        .build()
        .unwrap()
}

fn wrap_map(child: ArrayData, nulls: Option<Vec<bool>>) -> ArrayData {
    let len = child.len();
    let key_field = Arc::new(Field::new("key", DataType::Int32, false));
    let val_field = Arc::new(Field::new("value", child.data_type().clone(), true));
    let keys = ArrayData::builder(DataType::Int32)
        .len(len)
        .buffers(vec![Buffer::from_slice_ref((0..len as i32).collect::<Vec<_>>())])
        .build()
        .unwrap();
    let entries_struct = ArrayData::builder(DataType::Struct(Fields::from(vec![
        key_field,
        val_field.clone(),
    ])))
    .len(len)
    .child_data(vec![keys, child])
    .build()
    .unwrap();
    let map_field = Arc::new(Field::new(
        "entries",
        entries_struct.data_type().clone(),
        false,
    ));
    let offsets = Buffer::from_slice_ref((0..=len as i32).collect::<Vec<_>>());
    ArrayData::builder(DataType::Map(map_field, false))
        .len(len)
        .buffers(vec![offsets])
        .child_data(vec![entries_struct])
        .nulls(nulls.map(NullBuffer::from))
        .build()
        .unwrap()
}

fn wrap_dict(child: ArrayData, nulls: Option<Vec<bool>>) -> ArrayData {
    let len = child.len();
    let keys = (0..len as i32).collect::<Vec<_>>();
    ArrayData::builder(DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(child.data_type().clone()),
    ))
    .len(len)
    .buffers(vec![Buffer::from_slice_ref(keys)])
    .child_data(vec![child])
    .nulls(nulls.map(NullBuffer::from))
    .build()
    .unwrap()
}

fn wrap_union(child: ArrayData, mode: UnionMode) -> ArrayData {
    let len = child.len();
    let fields: UnionFields = [(0_i8, Arc::new(Field::new("u", child.data_type().clone(), true)))]
        .into_iter()
        .collect();
    let type_ids = Buffer::from_slice_ref(vec![0_i8; len]);
    let buffers = match mode {
        UnionMode::Sparse => vec![type_ids],
        UnionMode::Dense => {
            let offsets = Buffer::from_slice_ref((0..len as i32).collect::<Vec<_>>());
            vec![type_ids, offsets]
        }
    };
    ArrayData::builder(DataType::Union(fields, mode))
        .len(len)
        .buffers(buffers)
        .child_data(vec![child])
        .build()
        .unwrap()
}

fn wrap_ree(child: ArrayData, offset_run: bool) -> ArrayData {
    let len = child.len();
    assert_eq!(len, 2);
    let run_ends_values = if offset_run {
        vec![1_i32, 2]
    } else {
        vec![2_i32]
    };
    let values = if offset_run {
        child
    } else {
        child.slice(0, 1)
    };
    let run_ends = ArrayData::builder(DataType::Int32)
        .len(run_ends_values.len())
        .buffers(vec![Buffer::from_slice_ref(run_ends_values)])
        .build()
        .unwrap();
    let re_field = Arc::new(Field::new("run_ends", DataType::Int32, false));
    let val_field = Arc::new(Field::new("values", values.data_type().clone(), true));
    ArrayData::builder(DataType::RunEndEncoded(re_field, val_field))
        .len(len)
        .child_data(vec![run_ends, values])
        .build()
        .unwrap()
}

#[test]
fn iterative_equal_shallow_nested_shapes_match_expected() {
    for nulls in [None, Some(vec![true, false])] {
        let a_leaf = int32_leaf(vec![10, 20], nulls.clone());
        let b_leaf_same = int32_leaf(vec![10, 20], nulls.clone());
        let b_leaf_diff = int32_leaf(vec![10, 99], nulls.clone());

        type WrapCtor = fn(ArrayData, Option<Vec<bool>>) -> ArrayData;
        let constructors: [WrapCtor; 8] = [
            wrap_struct,
            |c, n| wrap_list(c, false, n),
            |c, n| wrap_list(c, true, n),
            |c, n| wrap_list_view(c, false, n),
            |c, n| wrap_list_view(c, true, n),
            wrap_fixed_list,
            wrap_map,
            wrap_dict,
        ];
        for ctor in constructors {
            let lhs = ctor(a_leaf.clone(), nulls.clone());
            let rhs_same = ctor(b_leaf_same.clone(), nulls.clone());
            let rhs_diff = ctor(b_leaf_diff.clone(), nulls.clone());
            assert!(equal(&lhs, &rhs_same));
            assert!(!lhs.eq(&rhs_diff) ^ nulls.is_some());
            if nulls.is_none() {
                assert!(!equal(&lhs, &rhs_diff));
            } else {
                // Slot 1 is null in both, so equal even though underlying leaf slot 1 differs!
                assert!(equal(&lhs, &rhs_diff));
                let diff_valid = int32_leaf(vec![11, 20], nulls.clone());
                assert!(!equal(&lhs, &ctor(diff_valid, nulls.clone())));
            }
            assert!(equal(&lhs.slice(1, 1), &rhs_same.slice(1, 1)));
        }

        for mode in [UnionMode::Dense, UnionMode::Sparse] {
            let lhs = wrap_union(a_leaf.clone(), mode);
            let rhs_same = wrap_union(b_leaf_same.clone(), mode);
            assert!(equal(&lhs, &rhs_same));
            let diff_valid = int32_leaf(vec![11, 20], nulls.clone());
            assert!(!equal(&lhs, &wrap_union(diff_valid, mode)));
        }

        let ree_aligned_a = wrap_ree(int32_leaf(vec![10, 10], None), false);
        let ree_aligned_b = wrap_ree(int32_leaf(vec![10, 10], None), true);
        let ree_diff = wrap_ree(int32_leaf(vec![10, 99], None), true);
        assert!(equal(&ree_aligned_a, &ree_aligned_b));
        assert!(!equal(&ree_aligned_a, &ree_diff));

        // Explicit reversed ListView offsets test (logical index 0 -> child 1, index 1 -> child 0).
        let lv_child = int32_leaf(vec![10, 20], None);
        let lv_rev_a = ArrayData::builder(DataType::ListView(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))))
        .len(2)
        .buffers(vec![
            Buffer::from_slice_ref([1_i32, 0]),
            Buffer::from_slice_ref([1_i32, 1]),
        ])
        .child_data(vec![lv_child.clone()])
        .build()
        .unwrap();
        let lv_rev_b = ArrayData::builder(lv_rev_a.data_type().clone())
            .len(2)
            .buffers(vec![
                Buffer::from_slice_ref([1_i32, 0]),
                Buffer::from_slice_ref([1_i32, 1]),
            ])
            .child_data(vec![int32_leaf(vec![10, 20], None)])
            .build()
            .unwrap();
        let lv_rev_diff = ArrayData::builder(lv_rev_a.data_type().clone())
            .len(2)
            .buffers(vec![
                Buffer::from_slice_ref([1_i32, 0]),
                Buffer::from_slice_ref([1_i32, 1]),
            ])
            .child_data(vec![int32_leaf(vec![10, 99], None)])
            .build()
            .unwrap();
        assert!(equal(&lv_rev_a, &lv_rev_b));
        assert!(!equal(&lv_rev_a, &lv_rev_diff));
    }
}

#[test]
fn iterative_equal_deep_nested_on_small_stack() {
    let handle = std::thread::Builder::new()
        .name("arrow_data_equal::small_stack".into())
        .stack_size(128 * 1024)
        .spawn(|| {
            for with_nulls in [false, true] {
                let nulls = with_nulls.then(|| vec![true, false]);
                let mut lhs = int32_leaf(vec![10, 20], nulls.clone());
                let mut rhs_same = int32_leaf(vec![10, 20], nulls.clone());
                let mut rhs_diff = int32_leaf(vec![11, 20], nulls.clone());

                for depth in 0..2_048 {
                    let wrap = |node: ArrayData| match depth % 7 {
                        0 => wrap_struct(node, nulls.clone()),
                        1 => wrap_list(node, false, nulls.clone()),
                        2 => wrap_list(node, true, nulls.clone()),
                        3 => wrap_list_view(node, false, nulls.clone()),
                        4 => wrap_fixed_list(node, nulls.clone()),
                        5 => wrap_dict(node, nulls.clone()),
                        _ => wrap_union(node, UnionMode::Dense),
                    };
                    lhs = wrap(lhs);
                    rhs_same = wrap(rhs_same);
                    rhs_diff = wrap(rhs_diff);
                }

                assert!(equal(&lhs, &rhs_same));
                assert!(!equal(&lhs, &rhs_diff));
                assert!(equal(&lhs.slice(1, 1), &rhs_same.slice(1, 1)));
            }
        })
        .unwrap();
    handle.join().unwrap();
}
