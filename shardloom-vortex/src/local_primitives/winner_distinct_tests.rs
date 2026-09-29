use super::*;
use crate::local_primitives::{AggregateDirectColumnAccessor, aggregate_direct_column_accessor};
use crate::{VortexAggregateOrderExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use shardloom_core::ColumnRef;
use vortex::array::{
    IntoArray as _,
    arrays::PrimitiveArray,
    dtype::{FieldNames, PType, StructFields},
    validity::Validity,
};

fn request() -> VortexSimpleAggregateRequest {
    let column = |name| Some(ColumnRef::new(name).unwrap());
    VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("bucket").unwrap()],
        vec![
            VortexSimpleAggregateMeasure::new("sum", column("measure"), "total".into()),
            VortexSimpleAggregateMeasure::new("count", None, "n".into()),
            VortexSimpleAggregateMeasure::new("avg", column("width"), "mean".into()),
            VortexSimpleAggregateMeasure::new(
                "count_distinct",
                column("identity"),
                "unique".into(),
            ),
        ],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("n", true)])
}

fn columns() -> Vec<String> {
    ["identity", "width", "bucket", "measure"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn dtype(nullable: Option<usize>) -> DType {
    DType::Struct(
        StructFields::new(
            FieldNames::from(["identity", "width", "bucket", "measure"]),
            [PType::U64, PType::U64, PType::I64, PType::I64]
                .into_iter()
                .enumerate()
                .map(|(index, kind)| {
                    DType::Primitive(
                        kind,
                        if nullable == Some(index) {
                            Nullability::Nullable
                        } else {
                            Nullability::NonNullable
                        },
                    )
                })
                .collect(),
        ),
        Nullability::NonNullable,
    )
}

type Row = (i64, i64, u64, u64);

fn accessors(rows: &[Row]) -> Vec<AggregateDirectColumnAccessor> {
    let arrays = [
        PrimitiveArray::new(
            rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array(),
        PrimitiveArray::new(
            rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array(),
        PrimitiveArray::new(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array(),
        PrimitiveArray::new(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array(),
    ];
    arrays
        .iter()
        .enumerate()
        .map(|(index, array)| aggregate_direct_column_accessor(&columns()[index], array).unwrap())
        .collect()
}

#[test]
fn winner_distinct_preserves_every_ordinary_row_across_chunks_ties_and_offsets() {
    let chunks = [
        vec![
            (i64::MIN, 1, 10, 0),
            (-1, 4, 40, u64::MAX),
            (i64::MIN, 2, 20, 0),
            (-1, 5, 50, u64::MAX),
            (0, 9, 90, 4),
            (i64::MAX, 8, 80, 7),
        ],
        vec![(-1, 6, 60, 1), (i64::MIN, 3, 30, 2), (0, 10, 100, 5)],
    ];
    let columns = columns();
    for (offset, limit) in [(0, 1), (1, 1), (1, 2), (0, 8), (8, 1)] {
        let mut request = request();
        request.offset = offset;
        let mut state =
            GroupedAggregateStates::new(&request, Some(limit), &columns, false, false).unwrap();
        let mut report = admit(&state, &dtype(None), &columns, 1_000_000, true).unwrap();
        state.winner_distinct_pass = MeasurePass::Ordinary;
        for rows in &chunks {
            assert!(
                state
                    .update_compact_direct_from_accessors(
                        &accessors(rows),
                        &columns,
                        None,
                        rows.len()
                    )
                    .unwrap()
            );
        }
        assert_eq!(state.count_distinct_state_entries().unwrap(), 0);
        assert_eq!(state.bound_numeric_recipe_chunks, 2);
        report.select(&mut state).unwrap();
        for rows in &chunks {
            assert!(
                state
                    .update_compact_direct_from_accessors(
                        &accessors(rows),
                        &columns,
                        None,
                        rows.len()
                    )
                    .unwrap()
            );
        }
        let (_, mut summary) = state.result_row_count_and_summary(Some(limit)).unwrap();
        report.annotate(&mut summary).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
        let expected = [
            serde_json::json!({"bucket": i64::MIN, "n": 3, "total": 6.0, "mean": 20.0, "unique": 2}),
            serde_json::json!({"bucket": -1, "n": 3, "total": 15.0, "mean": 50.0, "unique": 2}),
            serde_json::json!({"bucket": 0, "n": 2, "total": 19.0, "mean": 95.0, "unique": 2}),
            serde_json::json!({"bucket": i64::MAX, "n": 1, "total": 8.0, "mean": 80.0, "unique": 1}),
        ].into_iter().skip(offset).take(limit).collect::<Vec<_>>();
        assert_eq!(payload["values"], serde_json::json!(expected));
        assert_eq!(payload["aggregate_winner_distinct_candidate_groups"], 4);
        assert_eq!(
            state.bound_numeric_recipe_chunks, 2,
            "second pass must not update ordinary measures"
        );
        assert!(state.count_distinct_state_entries().unwrap() <= 7);
    }
}

#[test]
fn winner_distinct_admission_requires_selection_independent_of_distinct() {
    let columns = columns();
    let test = |request: &VortexSimpleAggregateRequest, limit, dtype: &DType, rows, unfiltered| {
        let states = GroupedAggregateStates::new(request, limit, &columns, false, false).unwrap();
        admit(&states, dtype, &columns, rows, unfiltered).is_some()
    };
    let mut request = request();
    assert!(test(&request, Some(10), &dtype(None), 1_000_000, true));
    request
        .order_by
        .push(VortexAggregateOrderExpr::new("bucket", false));
    assert!(test(&request, Some(10), &dtype(None), 1_000_000, true));
    for nullable in 0..4 {
        assert!(!test(
            &request,
            Some(10),
            &dtype(Some(nullable)),
            1_000_000,
            true
        ));
    }
    for limit in [None, Some(0), Some(129)] {
        assert!(!test(&request, limit, &dtype(None), 1_000_000, true));
    }
    assert!(!test(&request, Some(10), &dtype(None), 999_999, true));
    assert!(!test(&request, Some(10), &dtype(None), 1_000_000, false));
    request.offset = usize::MAX;
    assert!(!test(&request, Some(1), &dtype(None), 1_000_000, true));
    request.offset = 0;
    request.order_by = vec![VortexAggregateOrderExpr::new("unique", true)];
    assert!(!test(&request, Some(10), &dtype(None), 1_000_000, true));
    request.order_by = vec![VortexAggregateOrderExpr::new("n", true)];
    request.measures[0].argument_offset = Some(1);
    assert!(!test(&request, Some(10), &dtype(None), 1_000_000, true));
}

#[test]
fn winner_distinct_empty_state_is_exact_and_selected_rows_cannot_bypass_contract() {
    let request = request();
    let columns = columns();
    let mut states =
        GroupedAggregateStates::new(&request, Some(10), &columns, false, false).unwrap();
    let mut report = admit(&states, &dtype(None), &columns, 1_000_000, true).unwrap();
    states.winner_distinct_pass = MeasurePass::Ordinary;
    report.select(&mut states).unwrap();
    let (rows, _) = states.result_row_count_and_summary(Some(10)).unwrap();
    assert_eq!(rows, 0);
    let rows = [(1, 2, 3, 4)];
    assert!(
        states
            .update_grouped_count_distinct_integer_pair_preunion_from_accessors(
                &accessors(&rows),
                Some(&[0]),
                1
            )
            .is_err()
    );
}
