use super::*;

#[test]
fn constant_group_uses_required_columns_and_preserves_declared_key_order() {
    let fixture = fixture();
    for renamed in [false, true] {
        let key = if renamed { "paid" } else { "amount" };
        let plan = range(
            sort(
                aggregate(
                    project(
                        fixture.scan(),
                        vec![
                            ("constant", literal(ScalarValue::Int64(1))),
                            (key, column("amount")),
                        ],
                    ),
                    &["constant", key],
                    vec![measure("count", None, "rows")],
                ),
                key,
            ),
            3,
            0,
        );
        assert_provider_matches_general(&plan, VortexQueryPrimitiveKind::SimpleAggregate);
        let result = prepare_relational(&plan, policy())
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let report = result.execution.primitive_execution.as_ref().unwrap();
        assert_eq!(
            report.scan_columns.as_ref().unwrap().projection_columns,
            ["amount"],
            "constant keys must not read an unrelated source column"
        );
        assert_eq!(report.projected_columns, ["constant", key, "rows"]);
    }
}

#[test]
fn count_provider_reports_exact_footer_proof_without_column_inventory() {
    let fixture = fixture();
    let plan = aggregate(fixture.scan(), &[], vec![measure("count", None, "rows")]);
    assert_provider_matches_general(&plan, VortexQueryPrimitiveKind::CountAll);
    let result = prepare_relational(&plan, policy())
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let report = result.execution.primitive_execution.as_ref().unwrap();
    assert_eq!(report.embedded_layout.footer_row_count, 5);
    assert!(report.embedded_layout.metadata_persisted_in_artifact);
    assert_eq!(
        report.embedded_layout.per_column_metadata_contract,
        "not_required_for_exact_footer_count"
    );
    assert_eq!(report.embedded_layout.layout_inventory_nodes_inspected, 0);
    let scan = report.scan_columns.as_ref().unwrap();
    assert!(scan.metadata_completed);
    assert_eq!(scan.projection_columns, [] as [String; 0]);
    assert_eq!(scan.predicate_columns, [] as [String; 0]);
    assert_eq!(report.arrays_read_count, 0);
    assert!(!report.data_read);
    assert!(result.execution.native_io_certificate.is_certified());
}

#[test]
fn projection_provider_preserves_aliases_nulls_filter_order_and_limit() {
    let fixture = fixture();
    let plans = [
        project(
            fixture.scan(),
            vec![("alias", column("entity")), ("same", column("entity"))],
        ),
        range(
            project(
                filter(
                    fixture.scan(),
                    "entity",
                    ComparisonOp::Eq,
                    ScalarValue::UInt64(2),
                ),
                vec![("paid", column("amount")), ("key", column("entity"))],
            ),
            1,
            0,
        ),
        filter(
            fixture.scan(),
            "entity",
            ComparisonOp::Eq,
            ScalarValue::UInt64(55),
        ),
    ];
    for plan in plans {
        let prepared = prepare_relational(&plan, policy()).unwrap();
        assert!(prepared.provider.is_some());
        let mut general = prepare_relational(&plan, policy()).unwrap();
        general.provider = None;
        let expected = general
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for completed in 1..=2 {
            let actual = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(json_rows(&actual), json_rows(&expected));
            assert_eq!(
                actual.result_schema_json.value(),
                expected.result_schema_json.value()
            );
            assert!(actual.execution.primitive_execution.is_none());
            assert!(actual.execution.native_io_certificate.is_certified());
            assert_eq!(actual.execution.runtime.prepared_source_opens, 1);
            assert_eq!(actual.execution.runtime.completed_executions, completed);
            drop(actual);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

#[test]
fn projection_provider_respects_bounded_delivery_and_terminal_consumer_failure() {
    let fixture = fixture();
    let prepared = prepare_relational(&fixture.scan(), policy()).unwrap();
    assert!(prepared.provider.is_some());
    let mut rows = 0;
    prepared
        .for_each_json_batch(&CancellationToken::default(), 1, 4096, |batch| {
            assert!(batch.rows <= 1);
            rows += batch.rows;
            Ok(())
        })
        .unwrap();
    assert_eq!(rows, 5);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let token = CancellationToken::default();
    let error = prepared
        .for_each_batch(&token, |_, _| {
            token.cancel();
            Err(failed("projection consumer rejected the result"))
        })
        .err()
        .unwrap();
    assert!(error.to_string().contains("projection consumer rejected"));
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(prepared.snapshot().completed_executions, 1);
    let completed = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&completed).len(), 5);
}

#[test]
fn projection_provider_does_not_move_filter_across_range_or_skip_computation() {
    let fixture = fixture();
    let range_then_filter = filter(
        range(fixture.scan(), 1, 0),
        "entity",
        ComparisonOp::Eq,
        ScalarValue::UInt64(1),
    );
    let computed = project(
        project(
            fixture.scan(),
            vec![("unused", subtract("amount", 1)), ("key", column("entity"))],
        ),
        vec![("key", column("key"))],
    );
    for plan in [range_then_filter, computed, range(fixture.scan(), 1, 1)] {
        assert!(
            prepare_relational(&plan, policy())
                .unwrap()
                .provider
                .is_none()
        );
    }
}

#[test]
fn provider_admission_uses_its_own_capacity_and_releases_it_on_drop() {
    let fixture = fixture();
    let plan = aggregate(fixture.scan(), &[], vec![measure("count", None, "rows")]);
    let mut prepared = prepare_relational(&plan, policy()).unwrap();
    prepared.provider = None;
    let memory = prepared.session.memory();
    let baseline = memory.snapshot().reserved_bytes;
    let available = 128 * 1024;
    let PreparedVortexRelational {
        binding_metadata: bound_metadata,
        ..
    } = &prepared;
    assert!(bound_metadata.bytes() * 4 > available / 2);
    let pressure = memory
        .reserve(memory.snapshot().limit_bytes - baseline - available)
        .unwrap();
    let pressured = memory.snapshot().reserved_bytes;
    let PreparedRoot::Bound(root) = &prepared.root else {
        panic!("expected bound root");
    };
    let provider = provider::prepare(
        root,
        &prepared.sources[0],
        policy(),
        memory,
        shardloom_core::FloatPolicy::Strict,
    )
    .unwrap();
    assert!(
        provider.provider.is_some(),
        "the concrete provider fits the available credit"
    );
    assert!(memory.snapshot().reserved_bytes > pressured);
    drop(provider);
    assert_eq!(memory.snapshot().reserved_bytes, pressured);
    let remaining = memory.reserve(available - 1).unwrap();
    assert!(
        provider::prepare(
            root,
            &prepared.sources[0],
            policy(),
            memory,
            shardloom_core::FloatPolicy::Strict
        )
        .unwrap()
        .provider
        .is_none()
    );
    drop(remaining);
    drop(pressure);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
}

#[test]
fn grouped_count_provider_survives_alias_projection_before_filter_sort_and_range() {
    let fixture = fixture();
    let grouped = aggregate(
        fixture.scan(),
        &["entity"],
        vec![measure("count", None, "rows")],
    );
    let renamed = project(
        grouped,
        vec![("key", column("entity")), ("total", column("rows"))],
    );
    let plan = range(
        sort(
            filter(renamed, "total", ComparisonOp::Gt, ScalarValue::UInt64(1)),
            "key",
        ),
        2,
        0,
    );

    assert_provider_matches_general(&plan, VortexQueryPrimitiveKind::SimpleAggregate);
    let result = prepare_relational(&plan, policy())
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let report = result.execution.primitive_execution.as_ref().unwrap();
    let columns = report.scan_columns.as_ref().unwrap();
    assert_eq!(columns.projection_columns, ["entity"]);
    assert_eq!(columns.predicate_columns, [] as [String; 0]);
    assert_eq!(
        json_rows(&result),
        vec![
            serde_json::json!({"key": null, "total": 2}),
            serde_json::json!({"key": 2, "total": 2}),
        ]
    );
}

#[test]
fn grouped_count_provider_composes_two_alias_projections_and_renamed_filter() {
    let fixture = fixture();
    let grouped = aggregate(
        fixture.scan(),
        &["entity"],
        vec![measure("count", None, "rows")],
    );
    let first_aliases = project(
        grouped,
        vec![
            ("count_stage_one", column("rows")),
            ("key_stage_one", column("entity")),
        ],
    );
    let second_aliases = project(
        first_aliases,
        vec![
            ("key_stage_two", column("key_stage_one")),
            ("count_stage_two", column("count_stage_one")),
        ],
    );
    let filtered = filter(
        second_aliases,
        "count_stage_two",
        ComparisonOp::Gt,
        ScalarValue::UInt64(1),
    );
    let plan = sort(
        project(
            filtered,
            vec![
                ("key", column("key_stage_two")),
                ("total", column("count_stage_two")),
            ],
        ),
        "key",
    );

    assert_provider_matches_general(&plan, VortexQueryPrimitiveKind::SimpleAggregate);
    let result = prepare_relational(&plan, policy())
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![
            serde_json::json!({"key": null, "total": 2}),
            serde_json::json!({"key": 2, "total": 2}),
        ]
    );
}
