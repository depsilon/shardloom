//! The ordinary primitive dispatcher delivers one result to every adapter.

use super::*;
use crate::resident_session::read_observer::ReadObservationLimits;
use serde_json::{Value, json};

type Case = (
    &'static str,
    VortexQueryPrimitiveRequest,
    Vec<&'static str>,
    Vec<Value>,
);

#[allow(clippy::too_many_lines)] // Complete expected values for every existing provider branch.
fn cases(fixture: &Fixture) -> Vec<Case> {
    let predicate = shardloom_core::PredicateExpr::Compare {
        column: ColumnRef::new(KEY).unwrap(),
        op: shardloom_core::ComparisonOp::Gt,
        value: shardloom_core::StatValue::UInt64(1),
    };
    let mut sort =
        crate::VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new(VALUE, false)]);
    sort.offset = 1;
    vec![
        (
            "count",
            VortexQueryPrimitiveRequest::count_all(fixture.uri()),
            vec!["count_all"],
            vec![json!({"count_all":4})],
        ),
        (
            "count_where",
            VortexQueryPrimitiveRequest::count_where(fixture.uri(), predicate.clone()),
            vec!["count_all"],
            vec![json!({"count_all":2})],
        ),
        (
            "project",
            VortexQueryPrimitiveRequest::project(fixture.uri(), projection(&[VALUE])),
            vec![VALUE],
            vec![
                json!({VALUE:30}),
                json!({VALUE:99}),
                json!({VALUE:10}),
                json!({VALUE:20}),
            ],
        ),
        (
            "filter",
            VortexQueryPrimitiveRequest::filter(fixture.uri(), predicate.clone()),
            vec![KEY, VALUE],
            vec![json!({KEY:3,VALUE:30}), json!({KEY:2,VALUE:20})],
        ),
        (
            "filter_project",
            VortexQueryPrimitiveRequest::filter_and_project(
                fixture.uri(),
                predicate,
                projection(&[VALUE]),
            ),
            vec![VALUE],
            vec![json!({VALUE:30}), json!({VALUE:20})],
        ),
        (
            "structured",
            VortexQueryPrimitiveRequest::structured_project_rows(
                fixture.uri(),
                crate::VortexStructuredProjectionRequest::new(vec![
                    crate::VortexStructuredProjectionColumn::new(
                        "alias".into(),
                        crate::VortexStructuredProjectionExpr::SourceColumn(
                            ColumnRef::new(VALUE).unwrap(),
                        ),
                    ),
                ]),
            )
            .with_source_order_limit(2),
            vec!["alias"],
            vec![json!({"alias":30}), json!({"alias":99})],
        ),
        (
            "unary",
            VortexQueryPrimitiveRequest::expression_project_rows(
                fixture.uri(),
                projection(&[VALUE]),
                crate::VortexExpressionProjectionRequest::new(vec![
                    crate::VortexExpressionRewrite::NumericScalarArithmetic {
                        target_column: ColumnRef::new(VALUE).unwrap(),
                        operator: "+".into(),
                        operand: shardloom_core::ScalarValue::UInt64(1),
                    },
                ]),
            ),
            vec![VALUE],
            vec![
                json!({VALUE:31}),
                json!({VALUE:100}),
                json!({VALUE:11}),
                json!({VALUE:21}),
            ],
        ),
        (
            "aggregate",
            VortexQueryPrimitiveRequest::simple_aggregate(
                fixture.uri(),
                crate::VortexSimpleAggregateRequest::new(vec![
                    crate::VortexSimpleAggregateMeasure::new("count", None, "n".into()),
                    crate::VortexSimpleAggregateMeasure::new(
                        "sum",
                        Some(ColumnRef::new(VALUE).unwrap()),
                        "total".into(),
                    ),
                ]),
            ),
            vec!["n", "total"],
            vec![json!({"n":4,"total":159.0})],
        ),
        (
            "sort",
            VortexQueryPrimitiveRequest::sort_rows(
                fixture.uri(),
                projection(&[KEY, VALUE]),
                None,
                sort,
                2,
            ),
            vec![KEY, VALUE],
            vec![json!({KEY:2,VALUE:20}), json!({KEY:3,VALUE:30})],
        ),
    ]
}

#[test]
#[allow(clippy::too_many_lines)] // Compare complete values, source reads and ownership in one matrix.
fn primitive_fanout_all_providers_preserve_results_without_repeating_source_reads() {
    let fixture = Fixture::new(&[Some(3), None, Some(1), Some(2)], &[30, 99, 10, 20], 2);
    for grant in [16_u64 << 20, 64_u64 << 20] {
        for (name, request, columns, expected) in cases(&fixture) {
            let mut first_observation = None;
            for count in [2, 3] {
                let session = ResidentVortexSession::new(grant, 1).unwrap();
                let (source, observer) = session
                    .prepare_observed_file(&fixture.path(), ReadObservationLimits::default())
                    .unwrap();
                let prepared = observer.snapshot().unwrap();
                let mut policy =
                    VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
                policy.resource_envelope.memory_budget_bytes = grant;
                let targets = [Format::Jsonl, Format::Vortex, Format::Parquet]
                    .into_iter()
                    .take(count)
                    .map(|format| {
                        (
                            fixture
                                .0
                                .join(format!("{name}-{grant}-{count}-{}", format.as_str())),
                            format,
                        )
                    })
                    .collect::<Vec<_>>();
                let written = runtime::output_fanout::write_primitive_with_source(
                    &request,
                    &targets,
                    false,
                    policy,
                    &source,
                    &CancellationToken::default(),
                )
                .unwrap_or_else(|error| panic!("{name}/{grant}/{count}: {error}"));
                let replay = written.replay.as_ref().expect("multi-target replay");
                assert_eq!(replay.replays, count);
                assert_eq!(replay.rows_detached, expected.len() as u64);
                assert!(replay.spill.is_none());
                let reports = written.outputs;
                let observed = observer
                    .close_and_drain(std::time::Duration::from_secs(5))
                    .unwrap();
                let mut ranges = observed.completed_ranges[prepared.completed_ranges.len()..]
                    .iter()
                    .map(|range| (range.offset, range.length))
                    .collect::<Vec<_>>();
                ranges.sort_unstable();
                assert_eq!(observed.failed_read_calls, 0);
                assert_eq!(observed.pending_jobs, 0);
                assert_eq!(observed.rejected_requests, 0);
                if name == "count" {
                    assert!(
                        ranges.is_empty(),
                        "metadata count must not read source payload"
                    );
                } else {
                    assert!(!ranges.is_empty(), "{name} must execute its source work");
                }
                let observation = (ranges, session.snapshot().completed_executions);
                if let Some(first) = &first_observation {
                    assert_eq!(
                        &observation, first,
                        "{name}: an extra adapter must not rerun the producer"
                    );
                } else {
                    first_observation = Some(observation);
                }
                assert_eq!(session.snapshot().prepared_source_opens, 1);
                for (report, (path, format)) in reports.iter().zip(&targets) {
                    assert_eq!(report.primitive_kind, request.kind);
                    assert_eq!(report.rows_written, expected.len() as u64);
                    assert_eq!(report.projected_columns, columns);
                    assert!(!report.evidence.side_effects.fallback_attempted);
                    assert!(!report.has_errors());
                    if name == "count" {
                        assert!(!report.evidence.upstream_scan_called);
                        assert!(!report.evidence.side_effects.data_read);
                    }
                    let rows = if *format == Format::Jsonl {
                        fs::read_to_string(path)
                            .unwrap()
                            .lines()
                            .map(|line| serde_json::from_str::<Value>(line).unwrap())
                            .collect::<Vec<_>>()
                    } else {
                        read_binary(path, *format, &columns)
                    };
                    assert_eq!(rows, expected, "{name}/{grant}/{count}/{format:?}");
                }
                drop(source);
                drop(observer);
                assert_eq!(session.snapshot().memory.reserved_bytes, 0);
                assert!(session.snapshot().memory.peak_reserved_bytes <= grant);
            }
        }
    }
}

#[test]
fn primitive_fanout_empty_providers_deliver_the_schema_and_exact_scalar_counts() {
    let fixture = Fixture::new(&[], &[], 2);
    let grant = 16 << 20;
    for (name, request, columns, _) in cases(&fixture) {
        let session = ResidentVortexSession::new(grant, 1).unwrap();
        let mut policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
        policy.resource_envelope.memory_budget_bytes = grant;
        let expected = match name {
            "count" | "count_where" => vec![json!({"count_all":0})],
            "aggregate" => vec![json!({"n":0,"total":null})],
            _ => Vec::new(),
        };
        let targets = [Format::Jsonl, Format::Vortex].map(|format| {
            (
                fixture.0.join(format!("empty-{name}-{}", format.as_str())),
                format,
            )
        });
        let reports = runtime::output_fanout::write_primitive_in_session(
            &request,
            &targets,
            false,
            policy,
            &session,
            &CancellationToken::default(),
        )
        .unwrap_or_else(|error| panic!("empty {name}: {error}"));
        for (report, (path, format)) in reports.iter().zip(&targets) {
            assert_eq!(report.rows_written, expected.len() as u64);
            assert_eq!(report.projected_columns, columns);
            let rows = if *format == Format::Jsonl {
                fs::read_to_string(path)
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str::<Value>(line).unwrap())
                    .collect::<Vec<_>>()
            } else {
                read_binary(path, *format, &columns)
            };
            assert_eq!(rows, expected, "empty {name}/{format:?}");
        }
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}
