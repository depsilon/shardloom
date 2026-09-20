use super::super::{GroupedAggregateStates, aggregate_timing::AggregateFirstPassTiming};
use crate::{VortexAggregateOrderExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use shardloom_core::ColumnRef;
use vortex::array::{
    ArrayRef, IntoArray as _,
    arrays::{DictArray, PrimitiveArray, StructArray, VarBinViewArray},
    dtype::FieldNames,
    validity::Validity,
};

fn request() -> VortexSimpleAggregateRequest {
    VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("n").unwrap(), ColumnRef::new("s").unwrap()],
        vec![VortexSimpleAggregateMeasure::new("count", None, "c".into())],
    )
}

fn chunk(numbers: &[i16], strings: &[&str], dict: bool) -> ArrayRef {
    let text = VarBinViewArray::from_iter_str(strings.iter().copied()).into_array();
    let text = if dict {
        DictArray::try_new(
            PrimitiveArray::new(
                (0..u32::try_from(strings.len()).unwrap()).collect::<Vec<_>>(),
                Validity::NonNullable,
            )
            .into_array(),
            text,
        )
        .unwrap()
        .into_array()
    } else {
        text
    };
    StructArray::try_new(
        FieldNames::from(["n", "s"]),
        vec![
            PrimitiveArray::new(numbers.to_vec(), Validity::NonNullable).into_array(),
            text,
        ],
        numbers.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn run(chunks: &[ArrayRef], offset: usize, profiled: bool) -> serde_json::Value {
    let request = request().with_offset(offset);
    let columns = vec!["n".into(), "s".into()];
    // Explicitly retain enough groups for this state-level offset fixture.
    let mut states =
        GroupedAggregateStates::new(&request, Some(2 + offset), &columns, false, false).unwrap();
    for chunk in chunks {
        assert!(if profiled {
            states
                .update_compact_direct_from_chunk_profiled(
                    chunk,
                    &columns,
                    None,
                    &mut AggregateFirstPassTiming::default(),
                )
                .unwrap()
        } else {
            states
                .update_count_star_direct_from_chunk(chunk, &columns, None)
                .unwrap()
        });
    }
    serde_json::from_str(&states.result_row_count_and_summary(Some(2)).unwrap().1).unwrap()
}

#[test]
fn late_duplicates_keep_complete_key_equality_and_source_order() {
    for dictionary in [false, true] {
        for profiled in [false, true] {
            let result = run(
                &[
                    chunk(&[-1, 2, 3], &["雪", "", "ignored"], dictionary),
                    chunk(
                        &[99, -1, 2, -1, 2],
                        &["reject", "雪", "", "wrong", ""],
                        dictionary,
                    ),
                    chunk(&[99, 100], &["reject", "reject"], dictionary),
                ],
                0,
                profiled,
            );
            assert_eq!(
                result["values"],
                serde_json::json!([
                    {"n": -1, "s": "雪", "c": 2}, {"n": 2, "s": "", "c": 3}
                ])
            );
            assert_eq!(result["source_order_candidate_filter"]["rows"], 7);
            assert_eq!(result["source_order_candidate_filter"]["retained_rows"], 4);
        }
    }
}

#[test]
fn offsets_keep_existing_execution_without_candidate_filtering() {
    let result = run(
        &[
            chunk(
                &[-1, 2, 3, 4],
                &["first", "second", "third", "fourth"],
                false,
            ),
            chunk(
                &[3, -1, 2, 5],
                &["third", "first", "second", "absent"],
                false,
            ),
        ],
        1,
        true,
    );
    assert_eq!(
        result["values"],
        serde_json::json!([
            {"n": 2, "s": "second", "c": 2}, {"n": 3, "s": "third", "c": 2}
        ])
    );
    assert_eq!(result["source_order_candidate_filter"]["retained_rows"], 0);
}

#[test]
fn empty_and_dense_chunks_preserve_counts() {
    let result = run(
        &[
            chunk(&[-1, 2], &["a", "b"], false),
            chunk(&[], &[], false),
            chunk(&[2, -1, -1], &["b", "a", "a"], false),
        ],
        0,
        true,
    );
    assert_eq!(
        result["values"],
        serde_json::json!([
            {"n": -1, "s": "a", "c": 3}, {"n": 2, "s": "b", "c": 2}
        ])
    );
}

#[test]
fn residual_selection_and_unclosed_state_do_not_admit() {
    let request = request();
    let columns = vec!["n".into(), "s".into()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let first = chunk(&[-1, 2], &["a", "b"], false);
    assert!(
        states
            .source_order_candidate_chunk(&first, &columns, None)
            .unwrap()
            .is_none()
    );
    states
        .update_count_star_direct_from_chunk(&first, &columns, None)
        .unwrap();
    assert!(
        states
            .source_order_candidate_chunk(&first, &columns, Some(&[0]))
            .unwrap()
            .is_none()
    );
}

#[test]
fn nullable_chunk_keeps_existing_semantics() {
    let request = request();
    let columns = vec!["n".into(), "s".into()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    states
        .update_count_star_direct_from_chunk(&chunk(&[-1, 2], &["a", "b"], false), &columns, None)
        .unwrap();
    let nullable = StructArray::try_new(
        FieldNames::from(["n", "s"]),
        vec![
            PrimitiveArray::new(vec![-1_i16, 2], Validity::NonNullable).into_array(),
            VarBinViewArray::from_iter_nullable_str([None, Some("b")]).into_array(),
        ],
        2,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    assert!(
        states
            .source_order_candidate_chunk(&nullable, &columns, None)
            .unwrap()
            .is_none()
    );
    states
        .update_count_star_direct_from_chunk(&nullable, &columns, None)
        .unwrap();
    let result: serde_json::Value =
        serde_json::from_str(&states.result_row_count_and_summary(Some(2)).unwrap().1).unwrap();
    assert_eq!(result["values"][0]["c"], 1);
    assert_eq!(result["values"][1]["c"], 2);
}

#[test]
fn ordered_count_does_not_admit_retained_source_order_filter() {
    let request = request().with_order_by(vec![VortexAggregateOrderExpr::new("c", true)]);
    let columns = vec!["n".into(), "s".into()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let first = chunk(&[-1, 2], &["a", "b"], false);
    states
        .update_count_star_direct_from_chunk(&first, &columns, None)
        .unwrap();
    assert!(
        states
            .source_order_candidate_chunk(&first, &columns, None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn selected_provider_errors_propagate_and_rejected_text_is_not_executed() {
    use vortex::{
        array::VortexSessionExecute as _,
        encodings::zstd::{Zstd, ZstdData},
    };
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let text = VarBinViewArray::from_iter_str((0..512).map(|n| format!("text-{n}")));
    let data = ZstdData::from_var_bin_view_without_dict(&text, 0, 1024, &mut ctx).unwrap();
    let mut parts = data.into_parts(Validity::NonNullable);
    let frame = parts.frames.pop().unwrap();
    assert!(frame.len() > 16);
    parts.frames.push(frame.slice(..frame.len() - 1));
    let data = ZstdData::new(parts.dictionary, parts.frames, parts.metadata, parts.n_rows);
    let corrupt = Zstd::try_new(text.dtype().clone(), data, parts.validity)
        .unwrap()
        .into_array();
    assert!(
        corrupt
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .is_err()
    );
    for selected in [false, true] {
        let request = request();
        let columns = vec!["n".into(), "s".into()];
        let mut states =
            GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
        states
            .update_count_star_direct_from_chunk(
                &chunk(&[-1, 2], &["a", "b"], false),
                &columns,
                None,
            )
            .unwrap();
        let mut numbers = vec![99_i16; 512];
        if selected {
            numbers[0] = -1;
        }
        let later = StructArray::try_new(
            FieldNames::from(["n", "s"]),
            vec![
                PrimitiveArray::new(numbers, Validity::NonNullable).into_array(),
                corrupt.clone(),
            ],
            512,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let result = states.update_compact_direct_from_chunk_profiled(
            &later,
            &columns,
            None,
            &mut AggregateFirstPassTiming::default(),
        );
        if selected {
            assert!(result.is_err(), "executed corrupt provider must fail");
        } else {
            assert!(result.unwrap(), "irrelevant text does not execute");
            assert_eq!(
                states.source_order_candidate_filter.summary()["retained_rows"],
                0
            );
        }
    }
}
