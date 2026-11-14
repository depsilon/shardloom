//! Evidence must come from the executed scan and survive kernel composition.

use super::*;
use shardloom_core::native_file_scan::NativeFileScanRoute;

#[test]
fn native_file_scan_evidence_adaptive_join_reports_actual_shared_owner_work() {
    let keys = (0..128)
        .map(Some)
        .chain(std::iter::repeat_n(Some(1), 127))
        .collect::<Vec<_>>();
    let right = Fixture::new(
        keyed(
            &keys,
            &(0..u32::try_from(keys.len()).unwrap()).collect::<Vec<_>>(),
        ),
        17,
    );
    let left = Fixture::new(
        keyed(
            &[Some(u64::MAX), Some(1), Some(999), None],
            &[10, 20, 30, 40],
        ),
        2,
    );
    let plan = join(&left, &right, JoinKind::Full);
    let mut expected = vec![json!({"debit":10,"credit":null})];
    expected.push(json!({"debit":20,"credit":1}));
    expected.extend((128..255).map(|row| json!({"debit":20,"credit":row})));
    expected.extend([
        json!({"debit":30,"credit":null}),
        json!({"debit":40,"credit":null}),
    ]);
    expected.extend(
        (0..128)
            .filter(|row| *row != 1)
            .map(|row| json!({"debit":null,"credit":row})),
    );
    for compiled in [true, false] {
        let mut prepared = prepare_relational(&plan, policy()).unwrap();
        if !compiled {
            prepared.remove_file_kernels_for_reference().unwrap();
        }
        let baseline = prepared.snapshot().reserved_without_source_segment_cache();
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected);
        let certificate = &result.execution.native_io_certificate;
        assert_eq!(certificate.file_scans.sources.len(), 2);
        let work = &certificate.join_work;
        assert_eq!(work.runtime_filter.considered, 1);
        assert_eq!(work.runtime_filter.admitted, 1);
        assert_eq!(work.runtime_filter.build_keys, 128);
        assert_eq!(work.runtime_filter.probe_rows, 3);
        assert!(work.runtime_filter.rejected_rows > 0);
        assert_eq!(
            work.runtime_filter.index_lookups_avoided,
            work.runtime_filter.rejected_rows
        );
        assert_eq!(work.skew.hot_groups, 1);
        assert_eq!(work.skew.admitted_rows, 128);
        assert_eq!(work.skew.range_visits, 1);
        assert_eq!(work.skew.rows_visited, 128);
        drop(result);
        assert_eq!(
            prepared.snapshot().reserved_without_source_segment_cache(),
            baseline
        );
    }
}

#[test]
fn native_file_scan_evidence_preserves_aliases_and_source_column_names() {
    let fixture = Fixture::new(
        keyed(&[Some(2), None, Some(1), Some(3)], &[10, 20, 30, 40]),
        1,
    );
    let plan = transforms(
        fixture.scan(),
        vec![
            Stage::Project(vec![("key".into(), column("entity"))]),
            Stage::Filter(greater("key", 1)),
        ],
        true,
    );
    for compiled in [true, false] {
        let mut prepared = prepare_relational(&plan, policy()).unwrap();
        if !compiled {
            prepared.remove_file_kernels_for_reference().unwrap();
        }
        let baseline = prepared.snapshot().reserved_without_source_segment_cache();
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), [json!({"key":2}), json!({"key":3})]);
        let report = &result.execution.native_io_certificate.file_scans;
        assert_eq!(report.sources.len(), 1);
        assert_eq!(report.contracts.len(), 1);
        let source = &report.sources[0];
        assert!(source.completed_read_requests > 0);
        assert!(source.completed_read_bytes > 0);
        assert!(source.segment_requests > 0);
        assert_eq!(source.generation_fingerprint.len(), 64);
        let contract = &report.contracts[0];
        assert_eq!(contract.source, 0);
        assert_eq!(contract.source_rows, 4);
        assert_eq!(contract.available_columns, 2);
        assert_eq!(contract.invocations, 1);
        assert_eq!(contract.provider_scan_invocations, Some(1));
        for name in contract.projected_columns.as_ref().unwrap() {
            assert!(["entity", "amount"].contains(&name.as_str()));
        }
        if compiled {
            assert_eq!(contract.route, NativeFileScanRoute::Projection);
            assert_eq!(contract.projected_columns, Some(vec!["entity".into()]));
            // The provider emits the source name; Prepared::consume applies
            // the public alias after this measured scan boundary.
            assert_eq!(contract.output_columns, Some(vec!["entity".into()]));
            assert_eq!(
                contract.pushed_predicate_columns,
                Some(vec!["entity".into()])
            );
        } else {
            assert_eq!(contract.route, NativeFileScanRoute::RelationalScan);
        }
        drop(result);
        assert_eq!(
            prepared.snapshot().reserved_without_source_segment_cache(),
            baseline
        );
    }
}

#[test]
fn native_file_scan_evidence_metadata_counts_survive_two_nested_kernels() {
    let left = Fixture::new(keyed(&[Some(1), None, Some(2)], &[10, 20, 30]), 1);
    let right = Fixture::new(keyed(&[Some(1)], &[40]), 1);
    let prepared = prepare_relational(&count_join(&left, &right), policy()).unwrap();
    let baseline = prepared.snapshot().reserved_without_source_segment_cache();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [json!({"left_rows":3,"right_rows":1})]);
    let report = &result.execution.native_io_certificate.file_scans;
    assert_eq!(report.sources.len(), 2);
    assert_eq!(report.contracts.len(), 2);
    for source in &report.sources {
        assert_eq!(source.completed_read_requests, 0);
        assert_eq!(source.completed_read_bytes, 0);
        assert_eq!(source.segment_requests, 0);
    }
    for (ordinal, contract) in report.contracts.iter().enumerate() {
        assert_eq!(contract.source, ordinal);
        assert_eq!(contract.route, NativeFileScanRoute::MetadataCount);
        assert_eq!(contract.projected_columns, Some(vec![]));
        assert_eq!(contract.provider_scan_invocations, Some(0));
        assert_eq!(contract.delivered_rows, 0);
    }
    drop(result);
    assert_eq!(
        prepared.snapshot().reserved_without_source_segment_cache(),
        baseline
    );
}

#[test]
fn native_file_scan_evidence_pruned_count_reports_zero_reads_and_actual_predicate() {
    use vortex::array::expr::stats::Stat;
    for statistics in [vec![], vec![Stat::Min, Stat::Max, Stat::NullCount]] {
        let pruned = !statistics.is_empty();
        let fixture = Fixture::with_strategy_statistics(
            keyed(&[Some(1), Some(2)], &[10, 20]),
            1,
            vec![],
            crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(2),
            statistics,
        );
        let input = transforms(
            fixture.scan(),
            vec![Stage::Filter(greater("entity", 100))],
            true,
        );
        let prepared = prepare_relational(&count(input), policy()).unwrap();
        let baseline = prepared.snapshot().reserved_without_source_segment_cache();
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), [json!({"rows":0})]);
        let report = &result.execution.native_io_certificate.file_scans;
        assert_eq!(report.sources.len(), 1);
        assert_eq!(report.contracts.len(), 1);
        let contract = &report.contracts[0];
        assert_eq!(contract.route, NativeFileScanRoute::CountWhere);
        assert_eq!(
            contract.pushed_predicate_columns,
            Some(vec!["entity".into()])
        );
        assert_eq!(contract.whole_file_pruning_checks, 1);
        assert_eq!(contract.whole_file_exclusions, u64::from(pruned));
        assert_eq!(contract.provider_scan_invocations, Some(u64::from(!pruned)));
        assert_eq!(contract.delivered_rows, 0);
        assert_eq!(report.sources[0].completed_read_bytes == 0, pruned);
        assert_eq!(report.sources[0].segment_requests == 0, pruned);
        drop(result);
        assert_eq!(
            prepared.snapshot().reserved_without_source_segment_cache(),
            baseline
        );
    }
}

#[test]
fn native_file_scan_evidence_aggregate_separates_input_columns_from_output_aliases() {
    use crate::{
        query_primitive::VortexSimpleAggregateMeasure, relational_query::VortexRelationalAggregate,
    };
    let fixture = Fixture::new(keyed(&[Some(1), Some(1), Some(2)], &[10, 20, 5]), 1);
    let plan = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input: fixture.scan(),
        group_by: vec![ColumnRef::new("entity").unwrap()],
        grouping_sets: None,
        measures: vec![VortexSimpleAggregateMeasure::new(
            "sum",
            Some(ColumnRef::new("amount").unwrap()),
            "total".into(),
        )],
    }));
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert!(prepared.root_has_file_kernel());
    let baseline = prepared.snapshot().reserved_without_source_segment_cache();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let rows = json_rows(&result);
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .any(|row| row["entity"] == 1 && row["total"].as_f64() == Some(30.0))
    );
    assert!(
        rows.iter()
            .any(|row| row["entity"] == 2 && row["total"].as_f64() == Some(5.0))
    );
    let report = &result.execution.native_io_certificate.file_scans;
    assert_eq!(report.sources.len(), 1);
    assert_eq!(report.contracts.len(), 1);
    let contract = &report.contracts[0];
    assert_eq!(contract.route, NativeFileScanRoute::AggregatePass);
    assert_eq!(
        contract.projected_columns,
        Some(vec!["entity".into(), "amount".into()])
    );
    assert_eq!(
        contract.output_columns,
        Some(vec!["entity".into(), "amount".into()])
    );
    assert_eq!(contract.provider_scan_invocations, Some(1));
    assert_eq!(contract.delivered_rows, 3);
    assert!(report.sources[0].completed_read_bytes > 0);
    drop(result);
    assert_eq!(
        prepared.snapshot().reserved_without_source_segment_cache(),
        baseline
    );
}
