//! Exact source metadata counts retain aggregate semantics and source ownership.

use super::*;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure as Measure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalLimit, VortexRelationalSpillPolicy,
    },
};
use serde_json::json;

fn measure(function: &str, column: Option<&str>, alias: &str) -> Measure {
    Measure::new(
        function,
        column.map(|name| ColumnRef::new(name).unwrap()),
        alias.into(),
    )
}

fn aggregate(input: VortexRelationalPlan, measures: Vec<Measure>) -> VortexRelationalPlan {
    VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input,
        group_by: vec![],
        grouping_sets: None,
        measures,
    }))
}

pub(super) fn count(input: VortexRelationalPlan) -> VortexRelationalPlan {
    aggregate(input, vec![measure("count", None, "rows")])
}

pub(super) fn assert_metadata(result: &CollectedVortexRelational, rows: u64) {
    let report = result.execution.input.as_ref().unwrap();
    assert_eq!(
        (report.metadata_count_sources, report.metadata_count_rows),
        (1, rows)
    );
    assert_eq!(
        (
            report.columnar_projected_columns,
            report.columnar_physical_leaf_columns
        ),
        (0, 0)
    );
    assert_eq!(
        (
            report.columnar_row_groups_read,
            report.columnar_rows_decoded
        ),
        (0, 0)
    );
    assert_eq!(
        (
            report.columnar_data_read_bytes,
            report.columnar_data_read_requests
        ),
        (0, 0)
    );
    assert_eq!(
        (
            report.payload_batches,
            report.rows,
            report.input_logical_bytes
        ),
        (0, 0, 0)
    );
    assert!(report.columnar_metadata_read_bytes > 0);
    assert!(report.end_of_input_observed && report.output_ownership_detached);
    let certificate = &result.execution.native_io_certificate;
    assert!(certificate.is_certified());
    // JSON collection reads/materializes the completed scalar result. Source
    // work is proved by the explicit input counters and metadata-only scope.
    assert!(
        certificate
            .source_pushdown_report
            .proof_basis
            .contains("no_payload_reader")
    );
    assert!(!certificate.side_effects.fallback_attempted);
    assert!(
        certificate
            .source_pushdown_report
            .accepted_operations
            .iter()
            .any(|value| value == "native_source_metadata_count_all")
    );
    assert!(
        certificate
            .source_pushdown_report
            .proof_basis
            .contains("metadata_validation_only_no_page_or_value_validation")
    );
}

#[test]
fn parquet_metadata_count_empty_nullable_aliased_and_repeated_measures_reexecute_exactly() {
    for rows in [0, 16] {
        let batch = ParquetFixture::batch().slice(0, rows);
        let fixture = ParquetFixture::with_batch(batch, EnabledStatistics::None);
        let input = project(
            project(fixture.scan(&[], None), &[("text", "nullable")]),
            &[("nullable", "alias")],
        );
        let prepared = fixture.prepare(&aggregate(
            input,
            vec![
                measure("count", None, "rows"),
                measure("count", None, "also"),
            ],
        ));
        let initial = prepared.snapshot().memory.reserved_bytes;
        for _ in 0..2 {
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(json_rows(&result), vec![json!({"rows":rows,"also":rows})]);
            assert_metadata(&result, rows as u64);
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        }
    }
}

#[test]
fn parquet_metadata_count_directory_and_fingerprint_preserve_metadata_only_scope() {
    for fingerprint in [false, true] {
        let fixture = ParquetFixture::new();
        fs::copy(fixture.path(), fixture.0.join("second.parquet")).unwrap();
        let uri = DatasetUri::new(fixture.0.display().to_string()).unwrap();
        let prepared = prepare_relational_with_schema(policy(), |preparation| {
            let source = LocalColumnarSource::admit_with_content_fingerprint(
                &fixture.0,
                "parquet",
                fingerprint,
                &preparation.shared_memory_pool(),
                &preparation.cancellation_token(),
            )?;
            preparation.register_local_source(uri.clone(), source)?;
            Ok(count(VortexRelationalPlan::Scan(VortexRelationalScan {
                source_uri: uri.clone(),
                projection: ProjectionRequest::All,
                predicate: None,
            })))
        })
        .unwrap();
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), vec![json!({"rows":32})]);
        assert_metadata(&result, 32);
        let report = result.execution.input.as_ref().unwrap();
        assert_eq!(
            (report.columnar_files, report.columnar_row_groups_total),
            (2, 8)
        );
        assert_eq!(report.columnar_fingerprint_read_bytes > 0, fingerprint);
        assert_eq!(
            report.columnar_content_fingerprints.len(),
            if fingerprint { 2 } else { 0 }
        );
        drop(result);
        let failed = prepared.for_each_batch(&CancellationToken::default(), |_, _| {
            fs::copy(fixture.path(), fixture.0.join("new.parquet")).unwrap();
            Ok(())
        });
        assert!(failed.is_err());
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
    }
}

#[test]
fn parquet_metadata_count_does_not_validate_undemanded_corrupt_pages() {
    let fixture = ParquetFixture::new();
    fixture.corrupt_chunk(0, 0);
    fixture.corrupt_chunk(3, 2);
    let prepared = fixture.prepare(&count(fixture.scan(&[], None)));
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![json!({"rows":16})]);
    assert_metadata(&result, 16);
    for plan in [
        aggregate(
            fixture.scan(&[], None),
            vec![measure("count", Some("text"), "present")],
        ),
        fixture.scan(&[], None),
    ] {
        let prepared = fixture.prepare(&plan);
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
    }
}

#[test]
fn parquet_metadata_count_declines_column_filtered_mixed_computed_and_range_shapes() {
    let fixture = ParquetFixture::new();
    let computed = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: fixture.scan(&[], None),
        expressions: vec![(
            "computed".into(),
            Expression::cast(
                ExprId::new("cast").unwrap(),
                column("n"),
                shardloom_core::LogicalDType::Float64,
            ),
        )],
    }));
    let limited = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: fixture.scan(&[], None),
        offset: 2,
        count: 3,
    }));
    for (plan, expected) in [
        (
            aggregate(
                fixture.scan(&[], None),
                vec![measure("count", Some("text"), "present")],
            ),
            json!({"present":12}),
        ),
        (
            count(fixture.scan(&[], Some(equal("n", 5)))),
            json!({"rows":1}),
        ),
        (
            aggregate(
                fixture.scan(&[], None),
                vec![
                    measure("count", None, "rows"),
                    measure("sum", Some("n"), "sum"),
                ],
            ),
            json!({"rows":16,"sum":120.0}),
        ),
        (count(computed), json!({"rows":16})),
        (count(limited), json!({"rows":3})),
    ] {
        let result = fixture
            .prepare(&plan)
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), vec![expected]);
        let report = result.execution.input.as_ref().unwrap();
        assert_eq!(report.metadata_count_sources, 0);
        assert!(report.columnar_rows_decoded > 0);
    }
    let fallible = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: fixture.scan(&[], None),
        expressions: vec![(
            "invalid".into(),
            Expression::cast(
                ExprId::new("cast").unwrap(),
                column("text"),
                shardloom_core::LogicalDType::Int64,
            ),
        )],
    }));
    assert!(
        fixture
            .prepare(&count(fallible))
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
}

#[test]
fn parquet_metadata_count_declines_repeated_sources_and_preserves_native_replay() {
    let fixture = ParquetFixture::new();
    let plan = count(fixture.scan(&[], None));
    let union = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: plan.clone(),
        right: plan,
        kind: VortexRelationalSetKind::UnionAll,
    }));
    let result = fixture
        .prepare(&union)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![json!({"rows":16}), json!({"rows":16})]
    );
    let report = result.execution.input.as_ref().unwrap();
    assert_eq!(report.metadata_count_sources, 0);
    assert_eq!(
        (
            report.columnar_rows_decoded,
            report.replay_sources,
            report.replay_passes
        ),
        (16, 1, 1)
    );
}

#[test]
fn parquet_metadata_count_generation_failures_before_and_after_output_poison_reuse() {
    for after_output in [false, true] {
        let fixture = ParquetFixture::new();
        let prepared = fixture.prepare(&count(fixture.scan(&[], None)));
        let initial = prepared.snapshot().memory.reserved_bytes;
        let replace = || {
            let replacement = fixture.0.join("replacement.tmp");
            fs::copy(fixture.path(), &replacement).unwrap();
            fs::rename(replacement, fixture.path()).unwrap();
        };
        if !after_output {
            replace();
        }
        let mut delivered = false;
        let result = prepared.for_each_batch(&CancellationToken::default(), |_, _| {
            delivered = true;
            replace();
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(delivered, after_output);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
    }
}

#[test]
fn parquet_metadata_count_cancellation_denial_and_consumer_failure_refund_and_retry() {
    let fixture = ParquetFixture::new();
    let prepared = fixture.prepare(&count(fixture.scan(&[], None)));
    let initial = prepared.snapshot().memory.reserved_bytes;
    let token = CancellationToken::default();
    token.cancel();
    assert!(prepared.collect_jsonl(&token).is_err());
    let token = CancellationToken::default();
    assert!(
        prepared
            .for_each_batch(&token, |_, _| {
                token.cancel();
                Ok(())
            })
            .is_err()
    );
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| Err(failed(
                "count consumer stopped"
            )))
            .err()
            .unwrap()
            .to_string()
            .contains("count consumer stopped")
    );
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    let pressure = prepared
        .session
        .memory()
        .reserve(prepared.snapshot().memory.limit_bytes - initial)
        .unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    drop(pressure);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_metadata(&result, 16);
}

#[test]
fn parquet_metadata_count_native_output_survives_source_drop_with_spill_configured() {
    let fixture = ParquetFixture::new();
    let prepared = fixture
        .prepare(&count(fixture.scan(&[], None)))
        .with_spill(VortexRelationalSpillPolicy::new(&fixture.0, 16 << 20, 1 << 20).unwrap())
        .unwrap();
    let completed = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        completed
            .execution
            .input
            .as_ref()
            .unwrap()
            .metadata_count_sources,
        1
    );
    assert_eq!(completed.execution.ordered_aggregate_stages, 0);
    assert!(
        !completed
            .execution
            .native_io_certificate
            .side_effects
            .data_read
    );
    assert!(
        !completed
            .execution
            .native_io_certificate
            .side_effects
            .data_decoded
    );
    assert_eq!(completed.execution.spill.as_ref().unwrap().runs_written, 0);
    let memory = prepared.session.memory().clone();
    let mut context = completed.result.create_execution_ctx();
    let arrays = completed.result.arrays().to_vec();
    drop((completed, prepared, fixture));
    assert!(memory.snapshot().reserved_bytes > 0);
    let column = crate::local_primitives::logical_field_from_native_array(&arrays[0], "rows")
        .unwrap()
        .execute::<PrimitiveArray>(&mut context)
        .unwrap();
    assert_eq!(column.as_slice::<u64>(), &[16]);
    drop((column, arrays, context));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn parquet_metadata_count_declines_grouped_and_aggregate_filter_domains() {
    let fixture = ParquetFixture::new();
    let grouped = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input: fixture.scan(&[], None),
        group_by: vec![ColumnRef::new("n").unwrap()],
        grouping_sets: None,
        measures: vec![measure("count", None, "rows")],
    }));
    let result = fixture
        .prepare(&grouped)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        (0..16).map(|n| json!({"n":n,"rows":1})).collect::<Vec<_>>()
    );
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .metadata_count_sources,
        0
    );
    let fixture =
        ParquetFixture::with_batch(lifecycle_tests::typed_batch(), EnabledStatistics::None);
    let filtered =
        measure("count", None, "rows").with_filter_column(ColumnRef::new("flag").unwrap());
    let result = fixture
        .prepare(&aggregate(fixture.scan(&[], None), vec![filtered]))
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![json!({"rows":2})]);
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .metadata_count_sources,
        0
    );
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_rows_decoded,
        4
    );
}
