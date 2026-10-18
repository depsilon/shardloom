// SPDX-License-Identifier: Apache-2.0
// ShardLoom regression coverage for logical nullable structural scopes.

use std::ops::BitAnd;

use parking_lot::Mutex;
use vortex_array::ArrayRef;
use vortex_array::RecursiveCanonical;
use vortex_array::aggregate_fn::fns::all_non_distinct::all_non_distinct;
use vortex_array::arrays::DictArray;
use vortex_array::expr::fill_null;
use vortex_array::expr::is_not_null;
use vortex_array::expr::is_null;
use vortex_array::expr::merge;
use vortex_array::expr::select_exclude;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;

use super::*;
use crate::segments::SegmentFuture;
use crate::segments::SegmentId;

// Keep both native and canonical comparison coverage without printing array
// contents if a regression fails. These fixtures contain only synthetic values.
fn assert_nullable_arrays_eq(left: &ArrayRef, right: &ArrayRef) -> VortexResult<()> {
    assert!(
        left.dtype() == right.dtype(),
        "nullable array dtypes differ"
    );
    assert!(left.len() == right.len(), "nullable array lengths differ");
    let mut context = SESSION.create_execution_ctx();
    let canonical = left
        .clone()
        .execute::<RecursiveCanonical>(&mut context)?
        .0
        .into_array();
    assert!(
        all_non_distinct(left, right, &mut context)?,
        "nullable native array values differ"
    );
    assert!(
        all_non_distinct(&canonical, right, &mut context)?,
        "nullable canonical array values differ"
    );
    Ok(())
}

struct ObservedSegments {
    inner: Arc<TestSegments>,
    requested: Mutex<Vec<SegmentId>>,
}

impl SegmentSource for ObservedSegments {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.requested.lock().push(id);
        self.inner.request(id)
    }
}

fn write_nullable_layout(array: &ArrayRef) -> (Arc<ObservedSegments>, LayoutRef) {
    let segments = Arc::new(TestSegments::default());
    let sink = Arc::clone(&segments);
    let array = array.clone();
    let layout = block_on(|handle| async move {
        let session = new_session().with_handle(handle);
        let (ptr, eof) = SequenceId::root().split();
        TableStrategy::new(
            Arc::new(FlatLayoutStrategy::default()),
            Arc::new(FlatLayoutStrategy::default()),
        )
        .write_stream(
            ArrayContext::empty().into(),
            sink,
            array.to_array_stream().sequenced(ptr),
            eof,
            &session,
        )
        .await
    })
    .unwrap();
    (
        Arc::new(ObservedSegments {
            inner: segments,
            requested: Mutex::new(Vec::new()),
        }),
        layout,
    )
}

fn nullable_array(validity: Validity) -> ArrayRef {
    let dictionary = DictArray::try_new(
        buffer![0u8, 1, 2, 0, 1].into_array(),
        buffer![7i32, 2, 3].into_array(),
    )
    .unwrap()
    .into_array();
    let nested = StructArray::try_from_iter_with_validity(
        [("x", buffer![10i32, 20, 30, 40, 50].into_array())],
        Validity::from_iter([true, false, true, true, false]),
    )
    .unwrap()
    .into_array();
    StructArray::try_from_iter_with_validity(
        [
            ("a", dictionary),
            (
                "b",
                PrimitiveArray::from_option_iter([Some(4i32), None, Some(6), Some(8), None])
                    .into_array(),
            ),
            ("validity", buffer![1i32, 2, 3, 4, 5].into_array()),
            ("field:a", buffer![11i32, 12, 13, 14, 15].into_array()),
            ("nested", nested),
        ],
        validity,
    )
    .unwrap()
    .into_array()
}

fn projection_expressions() -> Vec<Expression> {
    vec![
        root(),
        col("a"),
        select(vec![FieldName::from("b"), FieldName::from("a")], root()),
        select(Vec::<FieldName>::new(), root()),
        select_exclude(vec![FieldName::from("a")], root()),
        select(vec![FieldName::from("x")], col("nested")),
        get_item("x", col("nested")),
        is_null(col("a")),
        is_null(root()),
        is_not_null(root()),
        fill_null(col("a"), lit(-1i32)),
        pack(
            [
                ("value", col("a")),
                ("null", is_null(col("a"))),
                ("literal", lit(42i32)),
            ],
            Nullability::NonNullable,
        ),
        merge([
            pack([("a", col("a"))], Nullability::NonNullable),
            pack([("literal", lit(42i32))], Nullability::NonNullable),
        ]),
        pack(
            [("left", col("validity")), ("right", col("field:a"))],
            Nullability::NonNullable,
        ),
        lit(42i32),
    ]
}

#[test]
fn nullable_projections_match_native_expressions_for_all_masks() -> VortexResult<()> {
    for validity in [
        Validity::AllValid,
        Validity::AllInvalid,
        Validity::from_iter([false, true, true, false, true]),
    ] {
        let array = nullable_array(validity);
        let (segments, layout) = write_nullable_layout(&array);
        let reader = layout.new_reader("".into(), segments, &SESSION, &Default::default())?;
        for expression in projection_expressions() {
            let bound = expression.bind(array.dtype())?;
            for range in [0..5, 1..4, 0..0] {
                let length = usize::try_from(range.end - range.start)
                    .vortex_expect("fixture range length fits usize");
                for selection in [
                    Mask::new_true(length),
                    Mask::new_false(length),
                    Mask::from_iter((0..length).map(|index| index % 2 == 0)),
                ] {
                    let expected = array
                        .slice(
                            usize::try_from(range.start)
                                .vortex_expect("fixture range start fits usize")
                                ..usize::try_from(range.end)
                                    .vortex_expect("fixture range end fits usize"),
                        )?
                        .filter(selection.clone())?
                        .apply_bound(&bound)?;
                    let actual = block_on(|_| {
                        reader
                            .projection_evaluation(&range, &bound, MaskFuture::ready(selection))
                            .unwrap()
                    })?;
                    assert_eq!(actual.dtype(), bound.dtype(), "{expression}, {range:?}");
                    assert_nullable_arrays_eq(&actual, &expected)?;
                }
            }
        }
    }
    Ok(())
}

#[test]
fn nullable_filters_and_pruning_preserve_parent_null_semantics() -> VortexResult<()> {
    for validity in [
        Validity::AllValid,
        Validity::AllInvalid,
        Validity::from_iter([false, true, true, false, true]),
    ] {
        let array = nullable_array(validity);
        let (segments, layout) = write_nullable_layout(&array);
        let reader = layout.new_reader("".into(), segments, &SESSION, &Default::default())?;
        for expression in [
            is_null(col("a")),
            is_not_null(col("a")),
            is_null(root()),
            eq(col("a"), lit(7i32)),
            eq(fill_null(col("a"), lit(-1i32)), lit(-1i32)),
            or(is_null(col("a")), gt(col("b"), lit(5i32))),
            is_null(get_item("x", col("nested"))),
            lit(true),
            lit(false),
        ] {
            let bound = expression.bind(array.dtype())?;
            for range in [0..5, 1..4, 0..0] {
                let length = usize::try_from(range.end - range.start)
                    .vortex_expect("fixture range length fits usize");
                for selection in [
                    Mask::new_true(length),
                    Mask::new_false(length),
                    Mask::from_iter((0..length).map(|index| index % 2 == 0)),
                ] {
                    let mut ctx = SESSION.create_execution_ctx();
                    let expected = array
                        .slice(
                            usize::try_from(range.start)
                                .vortex_expect("fixture range start fits usize")
                                ..usize::try_from(range.end)
                                    .vortex_expect("fixture range end fits usize"),
                        )?
                        .apply_bound(&bound)?
                        .null_as_false()
                        .execute(&mut ctx)?
                        .bitand(&selection);
                    let pruned = block_on(|_| {
                        reader
                            .pruning_evaluation(&range, &bound, selection.clone())
                            .unwrap()
                    })?;
                    assert_eq!(
                        pruned.clone().bitand(&expected).iter().collect::<Vec<_>>(),
                        expected.iter().collect::<Vec<_>>(),
                        "unsafe pruning: {expression}"
                    );
                    let actual = block_on(|_| {
                        reader
                            .filter_evaluation(&range, &bound, MaskFuture::ready(pruned))
                            .unwrap()
                    })?;
                    assert_eq!(
                        actual.iter().collect::<Vec<_>>(),
                        expected.iter().collect::<Vec<_>>(),
                        "{expression}, {range:?}"
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn nullable_projection_reads_only_its_field_and_validity() -> VortexResult<()> {
    let array = nullable_array(Validity::from_iter([false, true, true, false, true]));
    for expression in [
        col("a"),
        is_null(col("a")),
        is_null(root()),
        is_not_null(root()),
        lit(42i32),
    ] {
        let (segments, layout) = write_nullable_layout(&array);
        let source: Arc<dyn SegmentSource> = Arc::clone(&segments) as Arc<dyn SegmentSource>;
        let reader = layout.new_reader("".into(), source, &SESSION, &Default::default())?;
        let bound = expression.bind(array.dtype())?;
        let actual = block_on(|_| {
            reader
                .projection_evaluation(&(0..5), &bound, MaskFuture::new_true(5))
                .unwrap()
        })?;
        assert_nullable_arrays_eq(&actual, &array.clone().apply_bound(&bound)?)?;
        let requested = segments.requested.lock();
        if expression == lit(42i32) {
            assert!(
                requested.is_empty(),
                "a literal must not read input validity or fields"
            );
        } else {
            let slots = if expression == is_null(root()) || expression == is_not_null(root()) {
                vec![0]
            } else {
                vec![0, 1]
            };
            let allowed: Vec<_> = slots
                .into_iter()
                .flat_map(|slot| layout.slot(slot).unwrap().unwrap().segment_ids())
                .collect();
            assert!(!requested.is_empty());
            assert!(
                requested.iter().all(|id| allowed.contains(id)),
                "unreferenced field read: {requested:?}"
            );
        }
    }
    Ok(())
}

#[test]
fn empty_nullable_record_schema_and_root_validity_survive() -> VortexResult<()> {
    for (length, validity) in [
        (0, Validity::AllValid),
        (3, Validity::AllValid),
        (3, Validity::AllInvalid),
    ] {
        let array = StructArray::try_new(Vec::<FieldName>::new().into(), vec![], length, validity)?
            .into_array();
        let (segments, layout) = write_nullable_layout(&array);
        let reader = layout.new_reader("".into(), segments, &SESSION, &Default::default())?;
        let bound = root().bind(array.dtype())?;
        let actual = block_on(|_| {
            reader
                .projection_evaluation(
                    &(0..u64::try_from(length).unwrap()),
                    &bound,
                    MaskFuture::new_true(length),
                )
                .unwrap()
        })?;
        assert_nullable_arrays_eq(&actual, &array)?;
    }
    Ok(())
}
