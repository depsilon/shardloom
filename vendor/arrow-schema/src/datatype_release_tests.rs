// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Barrier, Mutex};

use crate::traversal_test_utils::small_stack;
use crate::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode};

const DEPTH: usize = 8_192;

fn wrap(dtype: DataType, shape: usize) -> DataType {
    let field = Arc::new(Field::new("item", dtype, true));
    match shape {
        0 => DataType::List(field),
        1 => DataType::ListView(field),
        2 => DataType::LargeList(field),
        3 => DataType::LargeListView(field),
        4 => DataType::FixedSizeList(field, 1),
        5 => DataType::Struct(Fields::from([field])),
        6 => DataType::Union(
            UnionFields::try_new([7], [field]).unwrap(),
            UnionMode::Dense,
        ),
        7 => DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from([
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    field,
                ])),
                false,
            )),
            false,
        ),
        8 => DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            field,
        ),
        _ => unreachable!(),
    }
}

#[test]
fn deep_schema_final_owners() {
    small_stack("datatype_release::tests::deep_schema_final_owners", || {
        for shape in 0..10 {
            let timezone: Arc<str> = Arc::from("UTC");
            let weak = Arc::downgrade(&timezone);
            let mut dtype = DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone));
            for level in 0..DEPTH {
                dtype = wrap(dtype, if shape == 9 { level % 9 } else { shape });
            }
            drop(dtype);
            assert!(
                weak.upgrade().is_none(),
                "shape {shape} retained its timezone"
            );
        }
    });
}

#[test]
fn deep_dictionary_clone_and_both_box_owners() {
    small_stack(
        "datatype_release::tests::deep_dictionary_clone_and_both_box_owners",
        || {
            for alternate_keys in [false, true] {
                // Alternating keys intentionally tests raw enum ownership, not
                // public dictionary-key type admission.
                let timezone: Arc<str> = Arc::from("UTC");
                let weak = Arc::downgrade(&timezone);
                let mut dtype = DataType::Timestamp(TimeUnit::Second, Some(timezone));
                for level in 0..DEPTH {
                    dtype = if alternate_keys && level % 2 == 0 {
                        DataType::Dictionary(Box::new(dtype), Box::new(DataType::Int32))
                    } else {
                        DataType::Dictionary(Box::new(DataType::Int32), Box::new(dtype))
                    };
                }
                let cloned = dtype.clone();
                let mut original = &dtype;
                let mut copy = &cloned;
                for level in (0..DEPTH).rev() {
                    let (DataType::Dictionary(ak, av), DataType::Dictionary(bk, bv)) =
                        (original, copy)
                    else {
                        panic!("lost a dictionary at level {level}")
                    };
                    assert!(!std::ptr::eq(ak.as_ref(), bk.as_ref()));
                    assert!(!std::ptr::eq(av.as_ref(), bv.as_ref()));
                    if alternate_keys && level % 2 == 0 {
                        assert_eq!(av.as_ref(), &DataType::Int32);
                        assert_eq!(bv.as_ref(), &DataType::Int32);
                        (original, copy) = (ak, bk);
                    } else {
                        assert_eq!(ak.as_ref(), &DataType::Int32);
                        assert_eq!(bk.as_ref(), &DataType::Int32);
                        (original, copy) = (av, bv);
                    }
                }
                let (DataType::Timestamp(a, Some(at)), DataType::Timestamp(b, Some(bt))) =
                    (original, copy)
                else {
                    panic!("lost the timestamp leaf")
                };
                assert_eq!(a, b);
                assert!(Arc::ptr_eq(at, bt));
                drop(dtype);
                assert!(weak.upgrade().is_some());
                drop(cloned);
                assert!(weak.upgrade().is_none());
            }
        },
    );
}

struct Release {
    id: usize,
    events: Arc<Mutex<Vec<usize>>>,
    panic: bool,
}

impl Drop for Release {
    fn drop(&mut self) {
        self.events.lock().unwrap().push(self.id);
        assert!(!self.panic, "deliberate retained-owner panic");
    }
}

fn owner(id: usize, events: &Arc<Mutex<Vec<usize>>>, panic: bool) -> Arc<Release> {
    Arc::new(Release {
        id,
        events: events.clone(),
        panic,
    })
}

#[test]
fn field_and_container_release_order_survives_one_panic() {
    small_stack(
        "datatype_release::tests::field_and_container_release_order_survives_one_panic",
        || {
            for panic in [false, true] {
                let events = Arc::new(Mutex::new(Vec::new()));
                let mut dtype = DataType::Int32;
                for level in 0..DEPTH {
                    let field = Field::new("named", dtype, true)
                        .with_metadata(HashMap::from([("key".into(), "value".into())]))
                        .with_retained_owner(owner(level * 2, &events, panic && level == 0));
                    let fields = Fields::from([Arc::new(field)]).with_retained_owner(owner(
                        level * 2 + 1,
                        &events,
                        false,
                    ));
                    dtype = DataType::Struct(fields);
                }
                let result = catch_unwind(AssertUnwindSafe(|| drop(dtype)));
                assert_eq!(result.is_err(), panic);
                assert_eq!(*events.lock().unwrap(), (0..DEPTH * 2).collect::<Vec<_>>());
            }
        },
    );
}

#[test]
fn detached_fields_and_concurrent_last_owners() {
    small_stack(
        "datatype_release::tests::detached_fields_and_concurrent_last_owners",
        || {
            let events = Arc::new(Mutex::new(Vec::new()));
            let mut dtype = DataType::Int32;
            for level in 0..DEPTH {
                dtype = wrap(dtype, level % 9);
            }
            let field = Arc::new(
                Field::new("retained", dtype, false).with_retained_owner(owner(0, &events, false)),
            );
            let weak = Arc::downgrade(&field);
            let fields =
                Fields::from([field.clone()]).with_retained_owner(owner(1, &events, false));
            let root = DataType::Struct(fields);
            let detached = field.clone();
            drop(field);
            let barrier = Arc::new(Barrier::new(4));
            let copies: Vec<_> = (0..4).map(|_| root.clone()).collect();
            drop(root);
            let threads: Vec<_> = copies
                .into_iter()
                .map(|copy| {
                    let barrier = barrier.clone();
                    std::thread::Builder::new()
                        .stack_size(128 * 1024)
                        .spawn(move || {
                            barrier.wait();
                            drop(copy);
                        })
                        .unwrap()
                })
                .collect();
            for thread in threads {
                thread.join().unwrap();
            }
            assert_eq!(*events.lock().unwrap(), vec![1]);
            assert!(weak.upgrade().is_some());
            drop(detached);
            assert!(weak.upgrade().is_none());
            assert_eq!(*events.lock().unwrap(), vec![1, 0]);
        },
    );
}

#[test]
fn concurrent_final_owners_release_the_deep_subtree() {
    small_stack(
        "datatype_release::tests::concurrent_final_owners_release_the_deep_subtree",
        || {
            let timezone: Arc<str> = Arc::from("UTC");
            let weak = Arc::downgrade(&timezone);
            let mut dtype = DataType::Timestamp(TimeUnit::Second, Some(timezone));
            for level in 0..DEPTH {
                dtype = wrap(dtype, level % 9);
            }
            let barrier = Arc::new(Barrier::new(4));
            let copies: Vec<_> = (0..4).map(|_| dtype.clone()).collect();
            drop(dtype);
            let threads: Vec<_> = copies
                .into_iter()
                .map(|copy| {
                    let barrier = barrier.clone();
                    std::thread::Builder::new()
                        .stack_size(128 * 1024)
                        .spawn(move || {
                            barrier.wait();
                            drop(copy);
                        })
                        .unwrap()
                })
                .collect();
            for thread in threads {
                thread.join().unwrap();
            }
            assert!(weak.upgrade().is_none());
        },
    );
}

#[test]
fn partial_schema_construction_releases_deep_children() {
    small_stack(
        "datatype_release::tests::partial_schema_construction_releases_deep_children",
        || {
            let events = Arc::new(Mutex::new(Vec::new()));
            let fields = (0..3)
                .map(|id| {
                    let mut dtype = DataType::Int32;
                    for level in 0..DEPTH {
                        dtype = wrap(dtype, level % 9);
                    }
                    Field::new("partial", dtype, true)
                        .with_retained_owner(owner(id, &events, false))
                })
                .collect::<Vec<_>>();
            let error = UnionFields::try_new([0, 0, 1], fields).unwrap_err();
            assert_eq!(
                error.to_string(),
                "Invalid argument error: duplicate type id: 0"
            );
            assert_eq!(*events.lock().unwrap(), vec![1, 0, 2]);

            let events = Arc::new(Mutex::new(Vec::new()));
            let result = catch_unwind(AssertUnwindSafe(|| {
                let mut partial = Vec::new();
                for id in 0..3 {
                    let mut dtype = DataType::Int32;
                    for level in 0..DEPTH {
                        dtype = wrap(dtype, level % 9);
                    }
                    partial.push(
                        Field::new("partial", dtype, false)
                            .with_retained_owner(owner(id, &events, false)),
                    );
                }
                panic!("deliberate construction failure");
            }));
            assert!(result.is_err());
            assert_eq!(*events.lock().unwrap(), vec![0, 1, 2]);
        },
    );
}

#[test]
fn shared_empty_schema_owners_and_logical_identity() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let fields = Fields::empty()
        .with_retained_owner(owner(0, &events, false))
        .with_retained_owner(owner(1, &events, false));
    let schema = Arc::new(Schema::new(fields.clone()));
    assert_eq!(fields, Fields::empty());
    assert_eq!(format!("{fields:?}"), "[]");
    #[cfg(feature = "serde")]
    assert_eq!(serde_json::to_string(&fields).unwrap(), "[]");
    drop(fields);
    assert!(events.lock().unwrap().is_empty());
    drop(schema);
    assert_eq!(*events.lock().unwrap(), vec![0, 1]);

    let events = Arc::new(Mutex::new(Vec::new()));
    let union = UnionFields::empty().with_retained_owner(owner(0, &events, false));
    let alias = union.clone().with_retained_owner(owner(1, &events, false));
    assert_eq!(union, alias);
    #[cfg(feature = "serde")]
    assert_eq!(serde_json::to_string(&alias).unwrap(), "[]");
    drop(union);
    assert!(events.lock().unwrap().is_empty());
    drop(alias);
    assert_eq!(*events.lock().unwrap(), vec![0, 1]);

    let events = Arc::new(Mutex::new(Vec::new()));
    let original = Field::new("payload", DataType::Int32, true)
        .with_metadata(HashMap::from([("key".into(), "value".into())]));
    let retained = original
        .clone()
        .with_retained_owner(owner(0, &events, false));
    assert_eq!(retained, original);
    assert_eq!(format!("{retained:?}"), format!("{original:?}"));
    assert_eq!(format!("{retained}"), format!("{original}"));
    #[cfg(feature = "serde")]
    {
        assert_eq!(
            serde_json::to_value(&retained).unwrap(),
            serde_json::to_value(&original).unwrap()
        );
        let restored: Field =
            serde_json::from_value(serde_json::to_value(&retained).unwrap()).unwrap();
        assert_eq!(restored, original);
        drop(restored);
        assert!(events.lock().unwrap().is_empty());
    }
    drop(retained);
    assert_eq!(*events.lock().unwrap(), vec![0]);
}

#[test]
fn dictionary_run_end_and_union_siblings_release_in_order() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let field = |id| {
        Arc::new(
            Field::new("item", DataType::Int32, false)
                .with_retained_owner(owner(id, &events, false)),
        )
    };
    let dictionary = DataType::Dictionary(
        Box::new(DataType::List(field(0))),
        Box::new(DataType::List(field(1))),
    );
    let run_end = DataType::RunEndEncoded(field(2), field(3));
    let union = UnionFields::try_new(
        [5, 2],
        [
            Field::new("dictionary", dictionary, true),
            Field::new("run_end", run_end, true),
        ],
    )
    .unwrap()
    .with_retained_owner(owner(4, &events, false));
    drop(DataType::Union(union, UnionMode::Sparse));
    assert_eq!(*events.lock().unwrap(), vec![0, 1, 2, 3, 4]);
}

#[test]
fn clone_preserves_all_variant_values_and_shared_fields() {
    let field = Arc::new(Field::new("child", DataType::Int32, true));
    let fields = Fields::from([field.clone()]);
    let union = UnionFields::try_new([3], [field.clone()]).unwrap();
    let timezone: Arc<str> = Arc::from("America/Chicago");
    let cases = vec![
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.clone())),
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Duration(TimeUnit::Millisecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::Binary,
        DataType::FixedSizeBinary(7),
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::List(field.clone()),
        DataType::ListView(field.clone()),
        DataType::LargeList(field.clone()),
        DataType::LargeListView(field.clone()),
        DataType::FixedSizeList(field.clone(), 3),
        DataType::Struct(fields),
        DataType::Union(union, UnionMode::Dense),
        DataType::Decimal32(7, -2),
        DataType::Decimal64(15, 4),
        DataType::Decimal128(30, 9),
        DataType::Decimal256(70, 20),
        DataType::Map(field.clone(), true),
        DataType::RunEndEncoded(field.clone(), field.clone()),
        DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::List(field.clone())),
        ),
    ];
    for original in cases {
        let cloned = original.clone();
        assert_eq!(original, cloned);
        match &cloned {
            DataType::List(f)
            | DataType::ListView(f)
            | DataType::LargeList(f)
            | DataType::LargeListView(f)
            | DataType::FixedSizeList(f, _)
            | DataType::Map(f, _) => {
                assert!(Arc::ptr_eq(f, &field));
            }
            DataType::Struct(fs) => assert!(Arc::ptr_eq(&fs[0], &field)),
            DataType::Union(fs, _) => assert!(Arc::ptr_eq(&fs[0].1, &field)),
            DataType::RunEndEncoded(a, b) => {
                assert!(Arc::ptr_eq(a, &field));
                assert!(Arc::ptr_eq(b, &field));
            }
            DataType::Timestamp(_, Some(tz)) => assert!(Arc::ptr_eq(tz, &timezone)),
            DataType::Dictionary(_, v) => {
                let DataType::List(f) = v.as_ref() else {
                    panic!("changed value type")
                };
                assert!(Arc::ptr_eq(f, &field));
            }
            _ => {}
        }
    }
}
