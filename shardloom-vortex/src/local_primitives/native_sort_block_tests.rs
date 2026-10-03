use super::*;
use crate::local_primitives::{
    materialized_sort_row_values_by_indices, row_export_columns_from_chunk,
    select_sort_rows_with_tie_policy, sort_materialized_rows,
};
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{DictArray, StructArray},
    validity::Validity,
};

fn names() -> Vec<String> {
    ["payload", "number", "言葉", "source_id"]
        .map(str::to_owned)
        .to_vec()
}

fn chunk(partition: usize) -> ArrayRef {
    let labels = if partition.is_multiple_of(2) {
        [
            Some("z\0long repeated payload"),
            Some("é"),
            None,
            Some("東京"),
        ]
    } else {
        [
            Some("東京"),
            None,
            Some("e\u{301}"),
            Some("z\0long repeated payload"),
        ]
    };
    let label_values = VarBinViewArray::from_iter(
        labels,
        vortex::array::dtype::DType::Utf8(vortex::array::dtype::Nullability::Nullable),
    )
    .into_array();
    let labels = DictArray::try_new(
        PrimitiveArray::new(vec![0_u8, 1, 2, 3, 1, 3, 0, 2], Validity::NonNullable).into_array(),
        label_values,
    )
    .unwrap()
    .into_array();
    let number = PrimitiveArray::from_option_iter([
        Some(i64::MAX),
        Some(7_i64),
        None,
        Some(i64::MIN),
        Some(7),
        Some(-3),
        Some(0),
        Some(7),
    ])
    .into_array();
    let payload =
        VarBinViewArray::from_iter_str((0..8).map(|i| format!("payload-{partition}-{i}")))
            .into_array();
    let ids = PrimitiveArray::new(
        (0..8)
            .map(|i| 1000_u64 + (partition * 30 + i * 3) as u64)
            .collect::<Vec<_>>(),
        Validity::NonNullable,
    )
    .into_array();
    StructArray::try_new(
        ["payload", "number", "言葉", "source_id"].into(),
        vec![payload, number, labels, ids],
        8,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

#[test]
fn native_sort_block_matches_complete_sort_across_epochs_ties_offsets_and_source_addresses() {
    for tie in [VortexSortTiePolicy::First, VortexSortTiePolicy::Last] {
        for number_desc in [false, true] {
            for text_desc in [false, true] {
                for hidden_id in [None, Some(3)] {
                    let order = [
                        crate::VortexAggregateOrderExpr::new("number", number_desc),
                        crate::VortexAggregateOrderExpr::new("言葉", text_desc),
                    ];
                    // Candidate order differs from scan order; payload remains exact.
                    let value_indices = [2, 0, 1];
                    let order_indices = [2, 0];
                    let mut actual = Vec::new();
                    let mut expected = Vec::new();
                    let mut work = Work::default();
                    for partition in 0..4 {
                        let native = chunk(partition);
                        let old_columns = row_export_columns_from_chunk(&native, &names()).unwrap();
                        for row in 0..8 {
                            expected.push(SortRowCandidate {
                                ordinal: partition * 8 + row,
                                source_partition_index: partition,
                                source_ordinal: if hidden_id.is_some() {
                                    1000 + partition * 30 + row * 3
                                } else {
                                    50 + row
                                },
                                values: materialized_sort_row_values_by_indices(
                                    &old_columns,
                                    row,
                                    &value_indices,
                                )
                                .unwrap(),
                            });
                        }
                        work.add(
                            &append(
                                &native,
                                &names(),
                                &value_indices,
                                &order_indices,
                                &order,
                                tie,
                                5,
                                partition * 8,
                                partition,
                                50,
                                hidden_id,
                                &mut actual,
                                &mut vortex::array::legacy_session().create_execution_ctx(),
                            )
                            .unwrap()
                            .expect("native admission"),
                        )
                        .unwrap();
                    }
                    sort_materialized_rows(&mut actual, &order, &order_indices, tie);
                    sort_materialized_rows(&mut expected, &order, &order_indices, tie);
                    let actual =
                        select_sort_rows_with_tie_policy(&actual, &order_indices, 2, 3, tie);
                    let expected =
                        select_sort_rows_with_tie_policy(&expected, &order_indices, 2, 3, tie);
                    assert_eq!(actual.len(), 3);
                    for (actual, expected) in actual.iter().zip(expected) {
                        assert_eq!(actual.values, expected.values);
                        assert_eq!(actual.ordinal, expected.ordinal);
                        assert_eq!(
                            actual.source_partition_index,
                            expected.source_partition_index
                        );
                        assert_eq!(actual.source_ordinal, expected.source_ordinal);
                    }
                    assert_eq!(work.rows, 32);
                    assert!(work.candidate_rows < work.rows);
                }
            }
        }
    }
}

#[test]
fn native_sort_block_explicit_null_order_survives_cutoff_and_stable_ties() {
    use crate::relational_query::VortexRelationalNullOrder::{First, Last};
    // Independent source ordinals, frozen before the candidate algorithm runs.
    // Every block has MAX, 7, null, MIN, 7, -3, 0, 7. The second and later
    // blocks encounter an existing cutoff, so a wrong null rule loses winners.
    for (descending, nulls, first, last) in [
        (false, First, [18, 26, 3], [10, 2, 27]),
        (true, First, [18, 26, 0], [10, 2, 24]),
        (false, Last, [19, 27, 5], [11, 3, 29]),
        (true, Last, [16, 24, 1], [8, 0, 31]),
    ] {
        for (tie, expected) in [
            (VortexSortTiePolicy::First, first),
            (VortexSortTiePolicy::Last, last),
        ] {
            let order =
                [crate::VortexAggregateOrderExpr::new("number", descending).with_nulls(nulls)];
            let mut actual = Vec::new();
            let mut work = Work::default();
            for partition in 0..4 {
                work.add(
                    &append(
                        &chunk(partition),
                        &names(),
                        &[1],
                        &[0],
                        &order,
                        tie,
                        5,
                        partition * 8,
                        partition,
                        0,
                        None,
                        &mut actual,
                        &mut vortex::array::legacy_session().create_execution_ctx(),
                    )
                    .unwrap()
                    .unwrap(),
                )
                .unwrap();
            }
            sort_materialized_rows(&mut actual, &order, &[0], tie);
            let selected = select_sort_rows_with_tie_policy(&actual, &[0], 2, 3, tie);
            assert_eq!(
                selected.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
                expected
            );
            assert!(work.candidate_rows < work.rows, "cutoff must execute");
        }
    }
}

#[test]
fn native_sort_block_explicit_null_order_preserves_borrowed_utf8_cutoff() {
    use crate::relational_query::VortexRelationalNullOrder::{First, Last};
    let native = VarBinViewArray::from_iter_nullable_str([
        Some("z\0"),
        None,
        Some("a"),
        Some("東京"),
        None,
        Some("a"),
        Some(""),
    ])
    .into_array();
    for (descending, nulls, expected) in [
        (false, First, [8, 11, 6]),
        (true, First, [8, 11, 3]),
        (false, Last, [2, 5, 9]),
        (true, Last, [0, 7, 2]),
    ] {
        let order = [crate::VortexAggregateOrderExpr::new("text", descending).with_nulls(nulls)];
        let mut actual = Vec::new();
        for partition in 0..2 {
            append(
                &native,
                &["text".into()],
                &[0],
                &[0],
                &order,
                VortexSortTiePolicy::First,
                5,
                partition * 7,
                partition,
                0,
                None,
                &mut actual,
                &mut vortex::array::legacy_session().create_execution_ctx(),
            )
            .unwrap()
            .unwrap();
        }
        sort_materialized_rows(&mut actual, &order, &[0], VortexSortTiePolicy::First);
        let selected =
            select_sort_rows_with_tie_policy(&actual, &[0], 2, 3, VortexSortTiePolicy::First);
        assert_eq!(
            selected.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
            expected
        );
    }
}

#[test]
fn native_sort_block_preserves_nullable_parent_and_narrow_unsigned_extrema() {
    let array = StructArray::try_new(
        ["small", "large"].into(),
        vec![
            PrimitiveArray::new(vec![u8::MAX, 0, 5], Validity::NonNullable).into_array(),
            PrimitiveArray::new(vec![u64::MAX, 0, (1_u64 << 53) + 1], Validity::NonNullable)
                .into_array(),
        ],
        3,
        Validity::from_iter([true, false, true]),
    )
    .unwrap()
    .into_array();
    let names = ["small".to_owned(), "large".to_owned()];
    let mut rows = Vec::new();
    append(
        &array,
        &names,
        &[0, 1],
        &[0],
        &[crate::VortexAggregateOrderExpr::new("small", false)],
        VortexSortTiePolicy::First,
        3,
        0,
        0,
        0,
        None,
        &mut rows,
        &mut vortex::array::legacy_session().create_execution_ctx(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        rows[0].values,
        vec![StatValue::UInt64(255), StatValue::UInt64(u64::MAX)]
    );
    assert_eq!(rows[1].values, vec![StatValue::Null, StatValue::Null]);
    assert_eq!(
        rows[2].values,
        vec![StatValue::UInt64(5), StatValue::UInt64((1_u64 << 53) + 1)]
    );
}

#[test]
fn native_sort_block_does_not_reinterpret_float_nan_all_ties_or_invalid_indices() {
    let native = PrimitiveArray::new(vec![f64::NAN, 0.0], Validity::NonNullable).into_array();
    let integer = PrimitiveArray::new(vec![1_i64, 0], Validity::NonNullable).into_array();
    let mut rows = Vec::new();
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let names = ["n".to_owned()];
    let order = [crate::VortexAggregateOrderExpr::new("n", false)];
    for (array, tie) in [
        (&native, VortexSortTiePolicy::First),
        (&integer, VortexSortTiePolicy::All),
    ] {
        assert!(
            append(
                array,
                &names,
                &[0],
                &[0],
                &order,
                tie,
                1,
                0,
                0,
                0,
                None,
                &mut rows,
                &mut ctx
            )
            .unwrap()
            .is_none()
        );
        assert!(rows.is_empty());
    }
    assert!(
        append(
            &integer,
            &names,
            &[2],
            &[0],
            &order,
            VortexSortTiePolicy::First,
            1,
            0,
            0,
            0,
            None,
            &mut rows,
            &mut ctx
        )
        .is_err()
    );
    assert!(rows.is_empty());
    assert!(
        append(
            &integer,
            &names,
            &[0],
            &[0],
            &order,
            VortexSortTiePolicy::First,
            1,
            usize::MAX,
            0,
            0,
            None,
            &mut rows,
            &mut ctx
        )
        .is_err()
    );
    assert!(rows.is_empty());
    let empty = PrimitiveArray::new(Vec::<i64>::new(), Validity::NonNullable).into_array();
    let work = append(
        &empty,
        &names,
        &[0],
        &[0],
        &order,
        VortexSortTiePolicy::First,
        1,
        0,
        0,
        0,
        None,
        &mut rows,
        &mut ctx,
    )
    .unwrap()
    .unwrap();
    assert_eq!(work.rows, 0);
    assert!(rows.is_empty());
}

#[test]
fn native_sort_block_rejects_malformed_losing_utf8_but_ignores_null_payload_bytes() {
    use vortex::array::dtype::Nullability;
    for malformed in [&[0xff_u8][..], &b"long external malformed value\xff"[..]] {
        let make = |validity| {
            // Build valid binary buffers, then inject malformed UTF8 through the
            // native buffer-handle boundary. The UTF8 builder validates eagerly in
            // debug builds, before this fixture can reach the sort consumer.
            let binary = VarBinViewArray::from_iter_bin([malformed, &b"valid"[..]]);
            let invalid = VarBinViewArray::new_handle(
                binary.views_handle().clone(),
                binary.data_buffers().to_vec().into(),
                DType::Utf8(Nullability::NonNullable),
                Validity::NonNullable,
            )
            .into_array();
            StructArray::try_new(
                ["metric", "text"].into(),
                vec![
                    PrimitiveArray::new(vec![100_i64, 200], Validity::NonNullable).into_array(),
                    invalid,
                ],
                2,
                validity,
            )
            .unwrap()
            .into_array()
        };
        let names = ["metric".to_owned(), "text".to_owned()];
        let order = [crate::VortexAggregateOrderExpr::new("metric", false)];
        let mut candidates = vec![SortRowCandidate {
            ordinal: 0,
            source_partition_index: 0,
            source_ordinal: 0,
            values: vec![StatValue::Int64(0), StatValue::Utf8("cutoff".into())],
        }];
        let error = append(
            &make(Validity::NonNullable),
            &names,
            &[0, 1],
            &[0],
            &order,
            VortexSortTiePolicy::First,
            1,
            1,
            0,
            0,
            None,
            &mut candidates,
            &mut vortex::array::legacy_session().create_execution_ctx(),
        )
        .err()
        .expect("losing-row malformed UTF8 must fail");
        assert!(error.to_string().contains("invalid UTF8"));
        assert_eq!(candidates.len(), 1);
        let masked = make(Validity::from_iter([false, true]));
        let mut ctx = vortex::array::legacy_session().create_execution_ctx();
        let prepared = prepare(&masked, &names, VortexSortTiePolicy::First, 1, &mut ctx)
            .unwrap()
            .unwrap();
        // Hidden malformed payloads do not add copied-string scratch, including
        // when validity is inherited from the nullable parent struct.
        let bytes = 64 * 1024 + 2 * 2 * 256 + 4 * 5;
        assert_eq!(prepared.scratch_bytes().unwrap(), bytes);
        assert_eq!(
            super::super::sort_output_stream::scratch_bytes(&masked, &names, &mut ctx).unwrap(),
            bytes
        );
        let work = append(
            &masked,
            &names,
            &[0, 1],
            &[0],
            &order,
            VortexSortTiePolicy::First,
            1,
            1,
            0,
            0,
            None,
            &mut candidates,
            &mut vortex::array::legacy_session().create_execution_ctx(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(work.candidate_rows, 1);
        assert_eq!(candidates[1].values, vec![StatValue::Null, StatValue::Null]);
    }
}

#[test]
fn native_sort_borrows_sliced_inline_and_external_views_but_retains_independent_results() {
    use vortex::array::{arrays::varbinview::BinaryView, dtype::Nullability};
    use vortex::buffer::{Buffer, ByteBuffer};

    let first = "a-shared-prefix-東京-first";
    let second = "z-shared-prefix-東京-second";
    let values = ["outside", "123456789012", first, second, first, "", "tail"];
    let seed = "m-cutoff-long-seed-東京";
    for descending in [false, true] {
        let mut candidates = vec![SortRowCandidate {
            ordinal: 0,
            source_partition_index: 0,
            source_ordinal: 0,
            values: vec![StatValue::Utf8(seed.to_owned())],
        }];
        let expected = values[1..6]
            .iter()
            .enumerate()
            .filter(|(_, value)| {
                if descending {
                    **value > seed
                } else {
                    **value < seed
                }
            })
            .map(|(row, value)| (row, (*value).to_owned()))
            .collect::<Vec<_>>();
        let work = {
            let buffers = vec![
                ByteBuffer::from(format!("pad{first}").into_bytes()),
                ByteBuffer::from(format!("padding{second}").into_bytes()),
            ];
            let views = Buffer::from(vec![
                BinaryView::new_inlined(values[0].as_bytes()),
                BinaryView::new_inlined(values[1].as_bytes()),
                BinaryView::make_view(first.as_bytes(), 0, 3),
                BinaryView::make_view(second.as_bytes(), 1, 7),
                BinaryView::make_view(first.as_bytes(), 0, 3),
                BinaryView::new_inlined(values[5].as_bytes()),
                BinaryView::new_inlined(values[6].as_bytes()),
            ]);
            let mut ctx = vortex::array::legacy_session().create_execution_ctx();
            let source = VarBinViewArray::try_new(
                views,
                buffers.into(),
                DType::Utf8(Nullability::NonNullable),
                Validity::NonNullable,
                &mut ctx,
            )
            .unwrap()
            .into_array();
            let sliced = source.slice(1..6).unwrap();
            append(
                &sliced,
                &["renamed_text".to_owned()],
                &[0],
                &[0],
                &[crate::VortexAggregateOrderExpr::new(
                    "renamed_text",
                    descending,
                )],
                VortexSortTiePolicy::First,
                1,
                1,
                2,
                50,
                None,
                &mut candidates,
                &mut ctx,
            )
            .unwrap()
            .unwrap()
        }; // Native arrays, sliced view, buffers and execution context are gone.
        assert_eq!(work.rows, 5);
        assert_eq!(work.candidate_rows, expected.len() as u64);
        assert_eq!(
            work.copied_utf8_bytes,
            expected.iter().map(|(_, s)| s.len() as u64).sum::<u64>()
        );
        assert_eq!(candidates.len(), expected.len() + 1);
        for (actual, (row, text)) in candidates[1..].iter().zip(expected) {
            assert_eq!(actual.values, vec![StatValue::Utf8(text)]);
            assert_eq!(actual.ordinal, row + 1);
            assert_eq!(actual.source_partition_index, 2);
            assert_eq!(actual.source_ordinal, row + 50);
        }
    }
}
