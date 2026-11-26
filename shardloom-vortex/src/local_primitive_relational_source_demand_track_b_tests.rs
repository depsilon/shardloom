// SPDX-License-Identifier: Apache-2.0
//! Track B unit tests for key-first early Parquet filtering, Chrysalis safety,
//! demanded-domain validation guarantees, and join-aggregate runtime pushdown.

use super::*;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalFilter, VortexRelationalJoin,
        VortexRelationalJoinColumn, VortexRelationalJoinKey, VortexRelationalJoinKind as JoinKind,
        VortexRelationalSide as Side,
    },
    resident_session::ResidentVortexSession,
};
use arrow_array::{Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use serde_json::json;
use shardloom_core::{ComparisonOp, ExprId, Expression, ExpressionKind, FloatPolicy, ScalarValue};
use std::collections::BTreeSet;
use vortex::array::{
    arrays::{PrimitiveArray, StructArray},
    dtype::FieldNames,
    validity::Validity,
};

fn prepare_pair(
    fixtures: &[&ParquetFixture],
    plan: &VortexRelationalPlan,
) -> PreparedVortexRelational {
    prepare_relational_with_schema(policy(), |preparation| {
        for fixture in fixtures {
            let source = LocalColumnarSource::admit(
                &fixture.path(),
                "parquet",
                &preparation.shared_memory_pool(),
                &preparation.cancellation_token(),
            )?;
            preparation.register_local_source(
                DatasetUri::new(fixture.path().display().to_string())?,
                source,
            )?;
        }
        Ok(plan.clone())
    })
    .unwrap()
}

fn prepare_in_session(
    session: &ResidentVortexSession,
    fixtures: &[&ParquetFixture],
    plan: &VortexRelationalPlan,
) -> PreparedVortexRelational {
    prepare_relational_with_schema_in_session(policy(), session, |preparation| {
        for fixture in fixtures {
            let source = LocalColumnarSource::admit(
                &fixture.path(),
                "parquet",
                &preparation.shared_memory_pool(),
                &preparation.cancellation_token(),
            )?;
            preparation.register_local_source(
                DatasetUri::new(fixture.path().display().to_string())?,
                source,
            )?;
        }
        Ok(plan.clone())
    })
    .unwrap()
}

fn eq_expr(col: &str, value: ScalarValue) -> Expression {
    Expression::new(
        ExprId::new("eq").unwrap(),
        ExpressionKind::Compare {
            left: Box::new(Expression::column(
                ExprId::new("col").unwrap(),
                ColumnRef::new(col).unwrap(),
            )),
            op: ComparisonOp::Eq,
            right: Box::new(Expression::literal(ExprId::new("lit").unwrap(), value)),
        },
    )
}

#[test]
#[allow(clippy::too_many_lines, clippy::cast_possible_wrap)]
fn track_b1_selective_bloom_pushdown_single_and_compound_keys_skip_unselected_payload_rows() {
    let rows = 2048_usize;
    let fact = ParquetFixture::with_properties(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("k2", DataType::Int64, false),
                Field::new("text", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|i| (i % 512) as i64),
                )),
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|i| ((i / 100) % 20) as i64),
                )),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|i| format!("payload-{i:04}-{}", "p".repeat(48))),
                )),
            ],
        )
        .unwrap(),
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(512))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_dictionary_enabled(false)
            .build(),
    );

    // 101 spaced build keys across [0, 500]: >64 keys forces ProbeBloomFilter, while
    // min/max [0, 500] overlaps almost all rows [0, 511] in every RG so stats cannot prune.
    let build_keys: Vec<i64> = (0..=100).map(|i| i * 5).collect();
    let expected_key_set: BTreeSet<i64> = build_keys.iter().copied().collect();
    let build = ParquetFixture::with_batch_rows(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("tag", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(build_keys.iter().copied())),
                Arc::new(StringArray::from_iter_values(
                    build_keys.iter().map(|k| format!("t-{k}")),
                )),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
        256,
    );

    for kind in [JoinKind::Inner, JoinKind::LeftSemi] {
        let mut columns = vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("n").unwrap(),
                output_column: "n".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("text").unwrap(),
                output_column: "text".into(),
            },
        ];
        if kind == JoinKind::Inner {
            columns.push(VortexRelationalJoinColumn {
                side: Side::Right,
                column: ColumnRef::new("tag").unwrap(),
                output_column: "tag".into(),
            });
        }
        let plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
            left: fact.scan(&[], None),
            right: build.scan(&[], None),
            kind,
            keys: vec![VortexRelationalJoinKey {
                left: ColumnRef::new("n").unwrap(),
                right: ColumnRef::new("n").unwrap(),
            }],
            condition: None,
            columns,
        }));
        let prepared = prepare_pair(&[&fact, &build], &plan);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let res = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let out = json_rows(&res);
        assert_eq!(out.len(), 101 * 4, "exact output rows for {kind:?}");
        for row in &out {
            let n = row["n"].as_i64().unwrap();
            assert!(expected_key_set.contains(&n));
        }
        let input = res.execution.input.as_ref().unwrap();
        assert_eq!(input.columnar_key_filter_rows_decoded, rows as u64);
        assert!(
            input.columnar_key_filter_rows_selected < 700,
            "expected Bloom Stage 1 to filter <700 rows, got {}",
            input.columnar_key_filter_rows_selected
        );
        assert_eq!(
            input.columnar_payload_rows_decoded,
            input.columnar_key_filter_rows_selected
        );
        assert!(
            !res.execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
        drop(res);
        prepared
            .session
            .shared_memory_pool()
            .reclaim_optional_memory();
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }

    // Compound 2-column join key (n, k2) with >64 distinct pairs (no key_predicate, compound Bloom only).
    let pairs: Vec<(i64, i64)> = (0..80).map(|i| ((i * 11) % 512, (i / 16) % 20)).collect();
    let pair_set: BTreeSet<(i64, i64)> = pairs.iter().copied().collect();
    let build_compound = ParquetFixture::with_batch_rows(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("k2", DataType::Int64, false),
                Field::new("tag", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(pairs.iter().map(|p| p.0))),
                Arc::new(Int64Array::from_iter_values(pairs.iter().map(|p| p.1))),
                Arc::new(StringArray::from_iter_values(
                    (0..pairs.len()).map(|i| format!("c-{i}")),
                )),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
        256,
    );
    let compound_plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: fact.scan(&[], None),
        right: build_compound.scan(&[], None),
        kind: JoinKind::Inner,
        keys: vec![
            VortexRelationalJoinKey {
                left: ColumnRef::new("n").unwrap(),
                right: ColumnRef::new("n").unwrap(),
            },
            VortexRelationalJoinKey {
                left: ColumnRef::new("k2").unwrap(),
                right: ColumnRef::new("k2").unwrap(),
            },
        ],
        condition: None,
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("n").unwrap(),
                output_column: "n".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("k2").unwrap(),
                output_column: "k2".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("text").unwrap(),
                output_column: "text".into(),
            },
        ],
    }));
    let prepared_cmp = prepare_pair(&[&fact, &build_compound], &compound_plan);
    let res_cmp = prepared_cmp
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let expected_cmp_rows = (0..rows)
        .filter(|&i| pair_set.contains(&((i % 512) as i64, ((i / 100) % 20) as i64)))
        .count();
    assert_eq!(json_rows(&res_cmp).len(), expected_cmp_rows);
    let cmp_in = res_cmp.execution.input.as_ref().unwrap();
    assert_eq!(cmp_in.columnar_key_filter_rows_decoded, rows as u64);
    assert!(
        cmp_in.columnar_payload_rows_decoded < 1024,
        "compound Bloom should skip >50% payload rows, decoded={}",
        cmp_in.columnar_payload_rows_decoded
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn track_b1_zero_survivor_row_groups_skip_corrupt_infallible_payload_and_surviving_corrupt_fails() {
    // 4 row groups x 64 rows: RG0 & RG2 alternate 0 and 1000 so min/max [0, 1000]
    // defeats metadata pruning vs build [100, 298], while Stage 1 leaves 0 survivors.
    let mut keys = Vec::with_capacity(256);
    let mut texts = Vec::with_capacity(256);
    for rg in 0..4 {
        for r in 0..64 {
            let k = match rg {
                0 | 2 => {
                    if r % 2 == 0 {
                        0
                    } else {
                        1000
                    }
                }
                1 => {
                    if r == 0 {
                        0
                    } else if r == 63 {
                        1000
                    } else {
                        100 + i64::from(r - 1) * 2
                    }
                }
                _ => {
                    if r == 0 {
                        0
                    } else if r == 63 {
                        1000
                    } else {
                        200 + i64::from(r - 1) * 2
                    }
                }
            };
            keys.push(k);
            texts.push(format!("rg{rg}-r{r}-k{k}"));
        }
    }
    let fact = ParquetFixture::with_properties(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("text", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(keys)),
                Arc::new(StringArray::from_iter_values(texts)),
            ],
        )
        .unwrap(),
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(64))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_dictionary_enabled(false)
            .build(),
    );
    let build_keys: Vec<i64> = (0..100).map(|i| 100 + i * 2).collect();
    let build = ParquetFixture::with_batch_rows(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from_iter_values(build_keys))],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
        128,
    );
    let plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: fact.scan(&[], None),
        right: build.scan(&[], None),
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("n").unwrap(),
            right: ColumnRef::new("n").unwrap(),
        }],
        condition: None,
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("n").unwrap(),
                output_column: "n".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("text").unwrap(),
                output_column: "text".into(),
            },
        ],
    }));

    let clean = prepare_pair(&[&fact, &build], &plan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let expected = json_rows(&clean);
    let clean_in = clean.execution.input.as_ref().unwrap();
    assert_eq!(clean_in.columnar_payload_columns_skipped_row_groups, 2);
    assert_eq!(clean_in.columnar_key_filter_rows_decoded, 256);

    // Corrupt payload column (1 = text) in 0-survivor row groups 0 and 2: MUST still succeed.
    fact.corrupt_chunk(0, 1);
    fact.corrupt_chunk(2, 1);
    let skipped = prepare_pair(&[&fact, &build], &plan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&skipped), expected);
    assert_eq!(
        skipped
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_payload_columns_skipped_row_groups,
        2
    );

    // Corrupt payload column (1 = text) in surviving row group 1: MUST fail and release memory.
    fact.corrupt_chunk(1, 1);
    let bad = prepare_pair(&[&fact, &build], &plan);
    let initial = bad.snapshot().memory.reserved_bytes;
    assert!(bad.collect_jsonl(&CancellationToken::default()).is_err());
    bad.session.shared_memory_pool().reclaim_optional_memory();
    assert_eq!(bad.snapshot().memory.reserved_bytes, initial);
}

#[test]
#[allow(clippy::too_many_lines)]
fn track_b1_strict_float_and_decimal_payload_preserve_demanded_domain_validation() {
    // Row 0 has n=9999 (unselected by join) and NaN float payload.
    let fact_float = ParquetFixture::with_properties(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("x", DataType::Float64, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..128).map(|i| if i == 0 { 9999 } else { i64::from(i % 100) }),
                )),
                Arc::new(Float64Array::from_iter_values(
                    (0..128).map(|i| if i == 0 { f64::NAN } else { f64::from(i) }),
                )),
            ],
        )
        .unwrap(),
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(64))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build(),
    );
    let build = ParquetFixture::with_batch_rows(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from_iter_values(0..80_i64))],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
        128,
    );
    let float_plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: fact_float.scan(&[], None),
        right: build.scan(&[], None),
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("n").unwrap(),
            right: ColumnRef::new("n").unwrap(),
        }],
        condition: None,
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("n").unwrap(),
                output_column: "n".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("x").unwrap(),
                output_column: "x".into(),
            },
        ],
    }));

    // Strict float policy MUST decline early payload skip and reject NaN even in an unselected row.
    let strict_session =
        ResidentVortexSession::new_with_float_policy(32 << 20, 1, FloatPolicy::Strict).unwrap();
    let strict_prep = prepare_in_session(&strict_session, &[&fact_float, &build], &float_plan);
    let strict_initial = strict_prep.snapshot().memory.reserved_bytes;
    assert!(
        strict_prep
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    strict_session
        .shared_memory_pool()
        .reclaim_optional_memory();
    assert_eq!(strict_prep.snapshot().memory.reserved_bytes, strict_initial);

    // Extended float policy admits Float64 early payload skip, skipping row 0 NaN and decoding finite survivors.
    let ext_session =
        ResidentVortexSession::new_with_float_policy(32 << 20, 1, FloatPolicy::Extended).unwrap();
    let ext_res = prepare_in_session(&ext_session, &[&fact_float, &build], &float_plan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let expected_float_matches = (1..128).filter(|&i| (i % 100) < 80).count();
    assert_eq!(json_rows(&ext_res).len(), expected_float_matches);
    let ext_in = ext_res.execution.input.as_ref().unwrap();
    assert_eq!(ext_in.columnar_key_filter_rows_decoded, 128);
    assert!(ext_in.columnar_payload_rows_decoded < 128);

    // Decimal128 payload declines early payload skip cleanly.
    let fact_dec = ParquetFixture::with_batch_rows(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("dec", DataType::Decimal128(10, 2), false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..128).map(|i| i64::from(i % 100)),
                )),
                Arc::new(
                    Decimal128Array::from_iter_values((0..128).map(|i| i128::from(i) * 100))
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
        64,
    );
    let dec_plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: fact_dec.scan(&[], None),
        right: build.scan(&[], None),
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("n").unwrap(),
            right: ColumnRef::new("n").unwrap(),
        }],
        condition: None,
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("n").unwrap(),
                output_column: "n".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("dec").unwrap(),
                output_column: "dec".into(),
            },
        ],
    }));
    let dec_res = prepare_pair(&[&fact_dec, &build], &dec_plan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&dec_res).len(), expected_float_matches + 1);
    assert_eq!(
        dec_res
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_key_filter_rows_decoded,
        0
    );
}

#[test]
fn track_b1_chrysalis_coverage_not_poisoned_by_sparse_filter_and_reused_when_complete() {
    let fact = ParquetFixture::with_batch_rows(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("other", DataType::Int64, false),
                Field::new("text", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..512).map(|i| i64::from(i % 256)),
                )),
                Arc::new(Int64Array::from_iter_values(0..512_i64)),
                Arc::new(StringArray::from_iter_values(
                    (0..512).map(|i| format!("row-{i}")),
                )),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
        256,
    );
    let build = ParquetFixture::with_batch_rows(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from_iter_values(0..80_i64))],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
        128,
    );
    let join_plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: fact.scan(&[], None),
        right: build.scan(&[], None),
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("n").unwrap(),
            right: ColumnRef::new("n").unwrap(),
        }],
        condition: None,
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("n").unwrap(),
                output_column: "n".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("text").unwrap(),
                output_column: "text".into(),
            },
        ],
    }));
    let full_scan = fact.scan(&["n", "text"], None);

    // Direction 1: Filtered -> Unfiltered in same session must not poison subsequent unfiltered scan.
    let session1 = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let res_filtered = prepare_in_session(&session1, &[&fact, &build], &join_plan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&res_filtered).len(), 160);
    let filtered_in = res_filtered.execution.input.as_ref().unwrap();
    assert_eq!(filtered_in.columnar_key_filter_rows_decoded, 512);
    assert!(filtered_in.columnar_payload_rows_decoded < 512);
    assert_eq!(filtered_in.columnar_native_coverage_field_rows, 0);

    let res_full = prepare_in_session(&session1, &[&fact], &full_scan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&res_full).len(), 512);
    let full_in = res_full.execution.input.as_ref().unwrap();
    assert_eq!(full_in.columnar_rows_decoded, 512);

    // Direction 2: Complete unfiltered -> Filtered in same session reuses complete Chrysalis coverage cleanly.
    let session2 = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let seed = prepare_in_session(&session2, &[&fact], &full_scan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&seed).len(), 512);
    let res_reused = prepare_in_session(&session2, &[&fact, &build], &join_plan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&res_reused).len(), 160);
    let reused_in = res_reused.execution.input.as_ref().unwrap();
    assert!(
        reused_in.columnar_native_coverage_regions >= 2,
        "expected complete Chrysalis reuse across both row groups, got {:?}",
        reused_in.columnar_native_coverage_regions
    );

    // Direction 3: Partial payload Chrysalis coverage without all demanded key columns still runs Track B1 Stage 1.
    let partial_scan = fact.scan(&["other", "text"], None);
    let session3 = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let partial_seed = prepare_in_session(&session3, &[&fact], &partial_scan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&partial_seed).len(), 512);
    let res_partial = prepare_in_session(&session3, &[&fact, &build], &join_plan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&res_partial).len(), 160);
    let partial_in = res_partial.execution.input.as_ref().unwrap();
    assert_eq!(partial_in.columnar_key_filter_rows_decoded, 512);
    assert!(partial_in.columnar_payload_rows_decoded < 512);
}

#[test]
#[allow(clippy::cast_possible_wrap)]
fn track_b1_high_selectivity_bailout_disables_early_filter_after_32k_rows() {
    let total_rows = 65_536_usize; // 4 row groups x 16,384 rows
    let fact = ParquetFixture::with_properties(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..total_rows).map(|i| (i % 200) as i64),
                )),
                Arc::new(Int64Array::from_iter_values(0..total_rows as i64)),
            ],
        )
        .unwrap(),
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(16_384))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build(),
    );
    // 185 / 200 = 92.5% match rate (>85% threshold) and >64 build keys.
    let build = ParquetFixture::with_batch_rows(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from_iter_values(0..185_i64))],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
        256,
    );
    let plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: fact.scan(&[], None),
        right: build.scan(&[], None),
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("n").unwrap(),
            right: ColumnRef::new("n").unwrap(),
        }],
        condition: None,
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("n").unwrap(),
                output_column: "n".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("v").unwrap(),
                output_column: "v".into(),
            },
        ],
    }));
    let res = prepare_pair(&[&fact, &build], &plan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let expected_rows = (0..total_rows).filter(|i| (i % 200) < 185).count();
    assert_eq!(json_rows(&res).len(), expected_rows);
    let input = res.execution.input.as_ref().unwrap();
    assert_eq!(
        input.columnar_key_filter_rows_decoded, 32_768,
        "early filter should evaluate 2 RGs (32,768 rows) then disable itself on >85% selectivity"
    );
}

#[test]
fn track_b1_negative_join_directions_do_not_prune_or_skip_unmatched_rows() {
    let fact = ParquetFixture::with_batch_rows(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("text", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(0..256_i64)),
                Arc::new(StringArray::from_iter_values(
                    (0..256).map(|i| format!("r-{i}")),
                )),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
        64,
    );
    let build = ParquetFixture::with_batch_rows(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("tag", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(0..80_i64)),
                Arc::new(StringArray::from_iter_values(
                    (0..80).map(|i| format!("t-{i}")),
                )),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
        128,
    );
    for (kind, expected_rows) in [(JoinKind::Left, 256), (JoinKind::LeftAnti, 176)] {
        let mut columns = vec![VortexRelationalJoinColumn {
            side: Side::Left,
            column: ColumnRef::new("n").unwrap(),
            output_column: "n".into(),
        }];
        if kind == JoinKind::Left {
            columns.push(VortexRelationalJoinColumn {
                side: Side::Right,
                column: ColumnRef::new("tag").unwrap(),
                output_column: "tag".into(),
            });
        }
        let plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
            left: fact.scan(&[], None),
            right: build.scan(&[], None),
            kind,
            keys: vec![VortexRelationalJoinKey {
                left: ColumnRef::new("n").unwrap(),
                right: ColumnRef::new("n").unwrap(),
            }],
            condition: None,
            columns,
        }));
        let res = prepare_pair(&[&fact, &build], &plan)
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&res).len(), expected_rows, "{kind:?}");
        assert_eq!(
            res.execution
                .input
                .as_ref()
                .unwrap()
                .columnar_key_filter_rows_decoded,
            0
        );
    }
}

#[test]
#[allow(clippy::too_many_lines, clippy::cast_possible_wrap)]
fn track_b2_join_aggregate_pushes_filters_across_selective_empty_duplicate_and_vortex_fact() {
    let rows = 2048_usize;
    let fact_pq = ParquetFixture::with_properties(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("other", DataType::Int64, false),
                Field::new("text", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|i| (i % 512) as i64),
                )),
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|i| (i as i64) * 10),
                )),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|i| format!("fact-{i:04}-{}", "p".repeat(32))),
                )),
            ],
        )
        .unwrap(),
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(512))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_dictionary_enabled(false)
            .build(),
    );

    // Dimension with 256 rows; tier == 1 keeps 80 spaced keys (0, 5, 10, ..., 395), >64 unique.
    let dim_unique = ParquetFixture::with_batch_rows(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("tier", DataType::Int64, false),
                Field::new("region", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..256_i64).map(|i| if i < 80 { i * 5 } else { i + 1000 }),
                )),
                Arc::new(Int64Array::from_iter_values((0..256).map(|i| {
                    if i < 80 {
                        1
                    } else if i == 255 {
                        10
                    } else {
                        0
                    }
                }))),
                Arc::new(StringArray::from_iter_values(
                    (0..256).map(|i| if i % 2 == 0 { "north" } else { "south" }.to_string()),
                )),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
        256,
    );

    let make_join_agg =
        |fact_scan: VortexRelationalPlan, dim_scan: VortexRelationalPlan, tier_val: i64| {
            let filtered_dim = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
                input: dim_scan,
                predicate: eq_expr("tier", ScalarValue::Int64(tier_val)),
            }));
            let joined = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
                left: fact_scan,
                right: filtered_dim,
                kind: JoinKind::Inner,
                keys: vec![VortexRelationalJoinKey {
                    left: ColumnRef::new("n").unwrap(),
                    right: ColumnRef::new("n").unwrap(),
                }],
                condition: None,
                columns: vec![
                    VortexRelationalJoinColumn {
                        side: Side::Right,
                        column: ColumnRef::new("region").unwrap(),
                        output_column: "region".into(),
                    },
                    VortexRelationalJoinColumn {
                        side: Side::Left,
                        column: ColumnRef::new("other").unwrap(),
                        output_column: "other".into(),
                    },
                ],
            }));
            VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
                input: joined,
                group_by: vec![ColumnRef::new("region").unwrap()],
                grouping_sets: None,
                measures: vec![VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("other").unwrap()),
                    "sum_o".into(),
                )],
            }))
        };

    // 1. Unique selective dimension -> direct join_aggregate + probe_filters_pushed >= 1 + payload reduction.
    let res_sel = prepare_pair(
        &[&fact_pq, &dim_unique],
        &make_join_agg(fact_pq.scan(&[], None), dim_unique.scan(&[], None), 1),
    )
    .collect_jsonl(&CancellationToken::default())
    .unwrap();
    let jw_sel = &res_sel
        .execution
        .native_io_certificate
        .join_work
        .direct_aggregate;
    assert_eq!(jw_sel.probe_filters_pushed, 1);
    assert!(jw_sel.source_batches_direct >= 1);
    assert_eq!(jw_sel.chains_declined_duplicates, 0);
    let in_sel = res_sel.execution.input.as_ref().unwrap();
    assert_eq!(in_sel.columnar_key_filter_rows_decoded, rows as u64);
    assert!(in_sel.columnar_payload_rows_decoded < 1024);
    let mut rows_sel = json_rows(&res_sel);
    rows_sel.sort_by(|a, b| a["region"].as_str().cmp(&b["region"].as_str()));
    assert_eq!(rows_sel.len(), 2);
    assert_eq!(rows_sel[0]["sum_o"].as_f64(), Some(1_540_800.0));
    assert_eq!(rows_sel[1]["sum_o"].as_f64(), Some(1_548_800.0));

    // 2. Empty build dimension (tier == 2 falls within [0, 10] dim stats so dim reads 1 RG, while fact_pq reads 0 RGs!).
    let res_empty_grp = prepare_pair(
        &[&fact_pq, &dim_unique],
        &make_join_agg(fact_pq.scan(&[], None), dim_unique.scan(&[], None), 2),
    )
    .collect_jsonl(&CancellationToken::default())
    .unwrap();
    assert_eq!(
        res_empty_grp
            .execution
            .native_io_certificate
            .join_work
            .direct_aggregate
            .probe_filters_pushed,
        1
    );
    assert_eq!(json_rows(&res_empty_grp), Vec::<serde_json::Value>::new());
    // dim_unique read 1 RG (256 rows) while all 4 RGs of fact_pq were skipped!
    let empty_grp_in = res_empty_grp.execution.input.as_ref().unwrap();
    assert_eq!(empty_grp_in.columnar_row_groups_read, 1);
    assert_eq!(empty_grp_in.columnar_rows_decoded, 256);

    let scalar_empty_plan = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input: match make_join_agg(fact_pq.scan(&[], None), dim_unique.scan(&[], None), 2) {
            VortexRelationalPlan::Aggregate(agg) => agg.input,
            _ => unreachable!(),
        },
        group_by: vec![],
        grouping_sets: None,
        measures: vec![VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "cnt".into(),
        )],
    }));
    let res_empty_scalar = prepare_pair(&[&fact_pq, &dim_unique], &scalar_empty_plan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&res_empty_scalar), vec![json!({"cnt": 0})]);
    let empty_scalar_in = res_empty_scalar.execution.input.as_ref().unwrap();
    assert_eq!(empty_scalar_in.columnar_row_groups_read, 1);
    assert_eq!(empty_scalar_in.columnar_rows_decoded, 256);

    // 3. Duplicate-key dimension -> chains_declined_duplicates == 1, yet probe_filters_pushed >= 1 still pushes early filter!
    let mut dup_keys: Vec<i64> = (0..80).map(|i| i * 5).collect();
    dup_keys.push(0); // duplicate key 0 in north region
    let dup_tier = vec![1_i64; dup_keys.len()];
    let dup_regions: Vec<&str> = (0..dup_keys.len())
        .map(|i| if i % 2 == 0 { "north" } else { "south" })
        .collect();
    let dim_dup = ParquetFixture::with_batch_rows(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("tier", DataType::Int64, false),
                Field::new("region", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(dup_keys)),
                Arc::new(Int64Array::from_iter_values(dup_tier)),
                Arc::new(StringArray::from_iter_values(dup_regions)),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
        256,
    );
    let res_dup = prepare_pair(
        &[&fact_pq, &dim_dup],
        &make_join_agg(fact_pq.scan(&[], None), dim_dup.scan(&[], None), 1),
    )
    .collect_jsonl(&CancellationToken::default())
    .unwrap();
    let jw_dup = &res_dup
        .execution
        .native_io_certificate
        .join_work
        .direct_aggregate;
    assert_eq!(jw_dup.chains_declined_duplicates, 1);
    assert_eq!(jw_dup.probe_filters_pushed, 1);
    let in_dup = res_dup.execution.input.as_ref().unwrap();
    assert_eq!(in_dup.columnar_key_filter_rows_decoded, rows as u64);
    assert!(in_dup.columnar_payload_rows_decoded < 1024);
    let mut rows_dup = json_rows(&res_dup);
    rows_dup.sort_by(|a, b| a["region"].as_str().cmp(&b["region"].as_str()));
    assert_eq!(rows_dup[0]["sum_o"].as_f64(), Some(1_571_520.0));
    assert_eq!(rows_dup[1]["sum_o"].as_f64(), Some(1_548_800.0));

    // 4. Prepared .vortex FileKernel fact control -> probe_filters_pushed == 1 through FileKernel node.
    let fact_vortex_arr = StructArray::try_new(
        FieldNames::from(["n", "other", "text"]),
        vec![
            PrimitiveArray::from_iter((0..rows).map(|i| (i % 512) as i64)).into_array(),
            PrimitiveArray::from_iter((0..rows).map(|i| (i as i64) * 10)).into_array(),
            PrimitiveArray::from_iter((0..rows).map(|i| i as i64)).into_array(),
        ],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let fact_vortex = super::super::Fixture::new(fact_vortex_arr, 512);
    let dim_vortex_arr = StructArray::try_new(
        FieldNames::from(["n", "tier", "region"]),
        vec![
            PrimitiveArray::from_iter((0..256_i64).map(|i| if i < 80 { i * 5 } else { i + 1000 }))
                .into_array(),
            PrimitiveArray::from_iter((0..256).map(|i| i64::from(i < 80))).into_array(),
            PrimitiveArray::from_iter((0..256_i64).map(|i| i % 2)).into_array(),
        ],
        256,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let dim_vortex = super::super::Fixture::new(dim_vortex_arr, 256);
    let filtered_dim_v = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: dim_vortex.scan(),
        predicate: eq_expr("tier", ScalarValue::Int64(1)),
    }));
    let v_joined = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: fact_vortex.scan(),
        right: filtered_dim_v,
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("n").unwrap(),
            right: ColumnRef::new("n").unwrap(),
        }],
        condition: None,
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Right,
                column: ColumnRef::new("region").unwrap(),
                output_column: "region".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("other").unwrap(),
                output_column: "other".into(),
            },
        ],
    }));
    let v_agg = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input: v_joined,
        group_by: vec![ColumnRef::new("region").unwrap()],
        grouping_sets: None,
        measures: vec![VortexSimpleAggregateMeasure::new(
            "sum",
            Some(ColumnRef::new("other").unwrap()),
            "sum_o".into(),
        )],
    }));
    let v_res = prepare_relational(&v_agg, policy())
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let mut v_rows = json_rows(&v_res);
    v_rows.sort_by(|a, b| a["region"].as_i64().cmp(&b["region"].as_i64()));
    assert_eq!(v_rows.len(), 2);
    assert_eq!(v_rows[0]["sum_o"].as_f64(), Some(1_540_800.0));
    assert_eq!(v_rows[1]["sum_o"].as_f64(), Some(1_548_800.0));
    assert_eq!(
        v_res
            .execution
            .native_io_certificate
            .join_work
            .direct_aggregate
            .probe_filters_pushed,
        1
    );
}
