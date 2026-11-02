//! Real ORC stripes exercise shared source facts, native evaluation and lifetime.

use super::count_tests::{assert_metadata, count};
use super::*;
use crate::relational_query::VortexRelationalLimit;
use serde_json::json;
use shardloom_core::ShardLoomError;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::path::Path;

#[path = "local_primitive_relational_source_demand_orc_fixture.rs"]
mod fixture;
use fixture::Fixture;

fn scan(path: &Path, predicate: Option<PredicateExpr>) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new(path.display().to_string()).unwrap(),
        projection: ProjectionRequest::Columns(vec![ColumnRef::new("n").unwrap()]),
        predicate,
    })
}

fn prepare(path: &Path, plan: VortexRelationalPlan) -> PreparedVortexRelational {
    prepare_relational_with_schema(policy(), |preparation| {
        let source = LocalColumnarSource::admit(
            path,
            "orc",
            &preparation.shared_memory_pool(),
            &preparation.cancellation_token(),
        )?;
        preparation.register_local_source(DatasetUri::new(path.display().to_string())?, source)?;
        Ok(plan)
    })
    .unwrap()
}

fn assert_work(result: &CollectedVortexRelational, groups: u64, rows: u64) {
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!(
        (input.columnar_row_groups_read, input.columnar_rows_decoded),
        (groups, rows)
    );
    assert_eq!(input.columnar_data_read_requests > 0, groups > 0);
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
}

#[test]
fn orc_source_demand_exact_counts_preserve_empty_aliased_and_repeated_execution() {
    for rows in [0, 16] {
        let fixture = Fixture::new(rows, false);
        let prepared = prepare(
            &fixture.path,
            count(project(scan(&fixture.path, None), &[("n", "alias")])),
        );
        let initial = prepared.snapshot().memory.reserved_bytes;
        for _ in 0..2 {
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(json_rows(&result), vec![json!({"rows":rows})]);
            assert_metadata(&result, rows as u64);
            assert!(
                result
                    .execution
                    .input
                    .as_ref()
                    .unwrap()
                    .columnar_source_formats
                    .iter()
                    .any(|format| format == "orc")
            );
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        }
    }
}

#[test]
fn orc_source_demand_missing_stats_retain_stripes_and_exact_stats_skip_before_open() {
    for statistics in [false, true] {
        let fixture = Fixture::new(16, statistics);
        let prepared = prepare(&fixture.path, scan(&fixture.path, Some(equal("n", 5))));
        let initial = prepared.snapshot().memory.reserved_bytes;
        for _ in 0..2 {
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(json_rows(&result), vec![json!({"n":5})]);
            assert_work(
                &result,
                if statistics { 1 } else { 4 },
                if statistics { 4 } else { 16 },
            );
            super::decisions_tests::assert_regions(
                &result,
                4,
                if statistics { 3 } else { 0 },
                shardloom_core::native_io_decisions::NativeIoDecisionReason::ExclusionNotProved,
            );
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        }
    }
    let fixture = Fixture::new(16, true);
    fixture.corrupt(0);
    for plan in [
        scan(&fixture.path, Some(equal("n", 5))),
        count(scan(&fixture.path, None)),
    ] {
        let prepared = prepare(&fixture.path, plan);
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert!(result.execution.native_io_certificate.is_certified());
    }
    let prepared = prepare(&fixture.path, scan(&fixture.path, None));
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
}

#[test]
fn orc_source_demand_shared_consumers_union_stripes_and_prefix_has_exact_end() {
    let fixture = Fixture::new(16, true);
    let plan = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: scan(&fixture.path, Some(equal("n", 1))),
        right: scan(&fixture.path, Some(equal("n", 14))),
        kind: VortexRelationalSetKind::UnionAll,
    }));
    let prepared = prepare(&fixture.path, plan);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [json!({"n":1}), json!({"n":14})]);
    assert_work(&result, 2, 8);
    super::decisions_tests::assert_regions(
        &result,
        4,
        2,
        shardloom_core::native_io_decisions::NativeIoDecisionReason::ExclusionNotProved,
    );
    let plan = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: scan(&fixture.path, None),
        offset: 3,
        count: 3,
    }));
    let prepared = prepare(&fixture.path, plan);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [3, 4, 5].map(|n| json!({"n":n})));
    assert_work(&result, 2, 6);
    assert!(
        result
            .execution
            .native_io_certificate
            .capability_decisions
            .get(shardloom_core::native_io_decisions::NativeIoCapability::RegionExclusion)
            .is_none()
    );
}

#[test]
fn orc_source_demand_missing_file_total_declines_and_disagreement_fails_without_leaks() {
    let fixture = Fixture::new(16, false);
    fixture.tail(None, false);
    let prepared = prepare(&fixture.path, count(scan(&fixture.path, None)));
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [json!({"rows":16})]);
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .metadata_count_sources,
        0
    );
    assert_work(&result, 4, 16);
    fixture.tail(Some(15), false);
    let memory = LiveMemoryPool::new(64 << 20).unwrap();
    let error =
        LocalColumnarSource::admit(&fixture.path, "orc", &memory, &CancellationToken::default())
            .err()
            .unwrap();
    assert!(error.to_string().contains("counts disagree"));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn orc_source_demand_hive_collections_keep_virtual_facts_and_source_membership() {
    let fixture = Fixture::new(16, false);
    for country in ["US", "CA"] {
        let directory = fixture.directory.0.join(format!("country={country}"));
        fs::create_dir(&directory).unwrap();
        fs::copy(&fixture.path, directory.join("data.orc")).unwrap();
    }
    let path = fixture.directory.0.join("data.manifest");
    fs::write(&path, json!({"version":1,"format":"orc","files":["country=US/data.orc","country=CA/data.orc"],"schema":"strict","partitioning":"hive"}).to_string()).unwrap();
    let predicate = PredicateExpr::Compare {
        column: ColumnRef::new("country").unwrap(),
        op: ComparisonOp::Eq,
        value: StatValue::Utf8("US".into()),
    };
    let prepared = prepare(&path, scan(&path, Some(predicate)));
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        (0..16).map(|n| json!({"n":n})).collect::<Vec<_>>()
    );
    assert_work(&result, 4, 16);
    let prepared = prepare(&path, count(scan(&path, None)));
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [json!({"rows":32})]);
    assert_metadata(&result, 32);
    fs::write(&path, b"{}").unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
}

#[test]
fn orc_source_demand_union_maps_reordered_fields_and_missing_values_before_stream_reads() {
    for statistics in [false, true] {
        let left = Fixture::new(8, statistics);
        let right = Fixture::from_batch(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("only_right", DataType::Int64, false),
                    Field::new("n", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(Int64Array::from_iter_values(900..908)),
                    Arc::new(Int64Array::from_iter_values(100..108)),
                ],
            )
            .unwrap(),
            statistics,
        );
        fs::copy(&right.path, left.directory.0.join("right.orc")).unwrap();
        let path = left.directory.0.join("union.manifest");
        fs::write(
            &path,
            json!({"version":1,"format":"orc","files":["right.orc","input.orc"],
                "schema":"union_by_name","partitioning":"none"})
            .to_string(),
        )
        .unwrap();
        let prepared = prepare(&path, count(project(scan(&path, None), &[("n", "alias")])));
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), [json!({"rows":16})]);
        assert_metadata(&result, 16);
        for (predicate, expected, groups, rows) in [
            (
                equal("n", 103),
                vec![103],
                if statistics { 1 } else { 4 },
                if statistics { 4 } else { 16 },
            ),
            (
                PredicateExpr::IsNotNull {
                    column: ColumnRef::new("only_right").unwrap(),
                },
                (100..108).collect(),
                2,
                8,
            ),
            (
                PredicateExpr::IsNull {
                    column: ColumnRef::new("only_right").unwrap(),
                },
                (0..8).collect(),
                if statistics { 2 } else { 4 },
                if statistics { 8 } else { 16 },
            ),
        ] {
            let prepared = prepare(&path, scan(&path, Some(predicate)));
            let initial = prepared.snapshot().memory.reserved_bytes;
            for _ in 0..2 {
                let result = prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap();
                assert_eq!(
                    json_rows(&result),
                    expected.iter().map(|n| json!({"n":n})).collect::<Vec<_>>()
                );
                assert_work(&result, groups, rows);
                drop(result);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
            }
        }
    }
}

#[test]
fn orc_source_demand_admission_cancellation_and_consumer_failures_release_credits() {
    let fixture = Fixture::new(16, true);
    let memory = LiveMemoryPool::new(256 << 10).unwrap();
    assert!(
        LocalColumnarSource::admit(&fixture.path, "orc", &memory, &CancellationToken::default())
            .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    for metadata_only in [false, true] {
        for cancel in [false, true] {
            let plan = scan(&fixture.path, None);
            let prepared = prepare(
                &fixture.path,
                if metadata_only { count(plan) } else { plan },
            );
            let initial = prepared.snapshot().memory.reserved_bytes;
            let token = CancellationToken::default();
            let mut callbacks = 0;
            let result = prepared.for_each_batch(&token, |_, context| {
                callbacks += 1;
                if cancel {
                    context.cancellation().cancel();
                    Ok(())
                } else {
                    Err(ShardLoomError::InvalidOperation(
                        "test consumer failure".into(),
                    ))
                }
            });
            assert!(result.is_err());
            assert_eq!(callbacks, 1);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
            let retried = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let expected = if metadata_only {
                vec![json!({"rows":16})]
            } else {
                (0..16).map(|n| json!({"n":n})).collect()
            };
            assert_eq!(json_rows(&retried), expected);
            drop(retried);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        }
    }
}
