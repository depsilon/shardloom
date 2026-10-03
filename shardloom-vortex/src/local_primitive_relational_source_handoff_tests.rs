use super::*;
use crate::local_primitives::{collect as native_collect, prepared_dispatch};

#[test]
fn native_typed_source_handoff_reuses_the_reader_and_tracks_only_referenced_types() {
    let fixture = fixture();
    let uri = DatasetUri::new(fixture.path().display().to_string()).unwrap();
    let request =
        VortexQueryPrimitiveRequest::project(uri.clone(), shardloom_plan::ProjectionRequest::All);
    let source = prepared_dispatch::prepare_source(&request, policy()).unwrap();
    let session = source.retained_session();
    let memory = session.memory().clone();
    assert_eq!(session.snapshot().prepared_source_opens, 1);
    assert_eq!(session.snapshot().completed_executions, 0);
    assert!(!prepared_dispatch::requires_relational(&source, Some(&["id".into()])).unwrap());
    assert!(!prepared_dispatch::requires_relational(&source, Some(&[])).unwrap());
    assert!(prepared_dispatch::requires_relational(&source, None).unwrap());
    for field in ["payload", "amount", "day", "instant"] {
        assert!(prepared_dispatch::requires_relational(&source, Some(&[field.into()])).unwrap());
    }
    assert!(prepared_dispatch::requires_relational(&source, Some(&["missing".into()])).is_err());
    let prepared =
        prepare_relational_from_source(uri.clone(), source.clone(), policy(), |schemas| {
            assert_eq!(
                schemas.source_columns(&uri)?,
                ["id", "payload", "amount", "day", "instant"]
            );
            Ok(sort(fixture.scan(), "amount"))
        })
        .unwrap();
    let baseline = memory.snapshot().reserved_bytes;
    let expected_rows = expected();
    for execution in 1..=2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            [0, 3, 1, 2].map(|index| expected_rows[index].clone())
        );
        assert_eq!(result.execution.runtime.prepared_source_opens, 1);
        assert_eq!(result.execution.runtime.completed_executions, execution);
        drop(result);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
    }
    fixture.replace();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert!(prepared_dispatch::requires_relational(&source, None).is_err());
    assert_eq!(session.snapshot().prepared_source_opens, 1);
    drop(prepared);
    drop(source);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_typed_source_handoff_rejects_other_generations_and_wider_grants() {
    let first = fixture();
    let second = fixture();
    let uri = DatasetUri::new(first.path().display().to_string()).unwrap();
    let other_uri = DatasetUri::new(second.path().display().to_string()).unwrap();
    let request =
        VortexQueryPrimitiveRequest::project(uri.clone(), shardloom_plan::ProjectionRequest::All);
    let source = prepared_dispatch::prepare_source(&request, policy()).unwrap();
    let session = source.retained_session();
    let baseline = session.snapshot().memory.reserved_bytes;
    assert!(
        prepare_relational_from_source(other_uri.clone(), source.clone(), policy(), |_| {
            panic!("source identity must be checked before frontend binding")
        })
        .is_err()
    );
    let mut narrow = policy();
    narrow.resource_envelope.memory_budget_bytes = 1 << 20;
    assert!(
        prepare_relational_from_source(uri, source.clone(), narrow, |_| {
            panic!("grant must be checked before frontend binding")
        })
        .is_err()
    );
    let other =
        VortexQueryPrimitiveRequest::project(other_uri, shardloom_plan::ProjectionRequest::All);
    assert!(native_collect::prepare_rows_from_source(&other, source.clone()).is_err());
    assert_eq!(session.snapshot().prepared_source_opens, 1);
    assert_eq!(session.snapshot().completed_executions, 0);
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn native_source_handoff_selects_from_lowered_fields_without_output_aliases() {
    use crate::{
        VortexSimpleAggregateMeasure as Measure, VortexSimpleAggregateRequest as Aggregate,
    };
    let fixture = fixture();
    let uri = DatasetUri::new(fixture.path().display().to_string()).unwrap();
    let project = VortexQueryPrimitiveRequest::project(
        uri.clone(),
        shardloom_plan::ProjectionRequest::Columns(vec![ColumnRef::new("id").unwrap()]),
    );
    let source = prepared_dispatch::prepare_source(&project, policy()).unwrap();
    assert!(!prepared_dispatch::request_requires_relational(&source, &project).unwrap());
    for (column, extended) in [("id", false), ("amount", true)] {
        let request = VortexQueryPrimitiveRequest::simple_aggregate(
            uri.clone(),
            Aggregate::new(vec![Measure::new(
                "count_distinct",
                Some(ColumnRef::new(column).unwrap()),
                "amount".into(),
            )]),
        );
        assert_eq!(
            prepared_dispatch::request_requires_relational(&source, &request).unwrap(),
            extended
        );
    }
    let request = VortexQueryPrimitiveRequest::sort_rows(
        uri.clone(),
        project.projection.clone(),
        None,
        crate::VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("day", true)]),
        2,
    );
    assert!(prepared_dispatch::request_requires_relational(&source, &request).unwrap());
    let request = VortexQueryPrimitiveRequest::count_where(
        uri,
        shardloom_core::PredicateExpr::IsNull {
            column: ColumnRef::new("payload").unwrap(),
        },
    );
    assert!(prepared_dispatch::request_requires_relational(&source, &request).unwrap());
    assert_eq!(source.retained_session().snapshot().completed_executions, 0);
    assert_eq!(
        source.retained_session().snapshot().prepared_source_opens,
        1
    );
}

#[test]
fn native_scalar_projection_source_handoff_retains_complete_rows_without_reopening() {
    let fixture = fixture();
    let request = VortexQueryPrimitiveRequest::project(
        DatasetUri::new(fixture.path().display().to_string()).unwrap(),
        shardloom_plan::ProjectionRequest::Columns(vec![ColumnRef::new("id").unwrap()]),
    );
    let source = prepared_dispatch::prepare_source(&request, policy()).unwrap();
    let session = source.retained_session();
    let operation = native_collect::prepare_rows_from_source(&request, source).unwrap();
    for execution in 1..=2 {
        let result = operation.execute().unwrap();
        let rows = serde_json::from_str::<Value>(result.values_json.value()).unwrap();
        assert_eq!(
            rows,
            json!([
                json!({"id":1}),
                json!({"id":2}),
                json!({"id":3}),
                json!({"id":4})
            ])
        );
        assert_eq!(session.snapshot().prepared_source_opens, 1);
        assert_eq!(session.snapshot().completed_executions, execution);
    }
    fixture.replace();
    assert!(operation.execute().is_err());
    assert_eq!(session.snapshot().prepared_source_opens, 1);
}

#[test]
fn native_scalar_sort_source_handoff_preserves_values_and_rejects_replacement() {
    let fixture = fixture();
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(fixture.path().display().to_string()).unwrap(),
        shardloom_plan::ProjectionRequest::Columns(vec![ColumnRef::new("id").unwrap()]),
        None,
        crate::VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("id", true)]),
        2,
    );
    let source = prepared_dispatch::prepare_source(&request, policy()).unwrap();
    let session = source.retained_session();
    for _ in 0..2 {
        let report = prepared_dispatch::execute_sort(&request, policy(), &source).unwrap();
        assert!(!report.has_errors());
        assert_eq!(report.rows_selected, Some(2));
        let rows: Value = serde_json::from_str(
            report
                .result_summary
                .as_ref()
                .unwrap()
                .rsplit_once(" values=")
                .unwrap()
                .1,
        )
        .unwrap();
        assert_eq!(rows["values"], json!([{"id":4},{"id":3}]));
        assert_eq!(session.snapshot().prepared_source_opens, 1);
        assert!(
            crate::local_primitive_native_io_certificate(&request, &report)
                .unwrap()
                .is_certified()
        );
    }
    fixture.replace();
    assert!(prepared_dispatch::execute_sort(&request, policy(), &source).is_err());
    assert_eq!(session.snapshot().prepared_source_opens, 1);
}

#[test]
#[cfg(feature = "universal-format-io")]
#[allow(clippy::too_many_lines)] // One retained owner spans every writer and the final replacement.
fn native_source_handoff_writers_keep_one_reader_all_formats_and_generation() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    use shardloom_core::{ComparisonOp, PredicateExpr, StatValue};
    use std::fmt::Write as _;
    let fixture = fixture();
    let uri = DatasetUri::new(fixture.path().display().to_string()).unwrap();
    let projection =
        shardloom_plan::ProjectionRequest::Columns(vec![ColumnRef::new("id").unwrap()]);
    let mut limited_projection =
        VortexQueryPrimitiveRequest::project(uri.clone(), projection.clone());
    limited_projection.source_order_limit = Some(2);
    let mut limited_filter = VortexQueryPrimitiveRequest::filter_and_project(
        uri.clone(),
        PredicateExpr::Compare {
            column: ColumnRef::new("id").unwrap(),
            op: ComparisonOp::Gt,
            value: StatValue::UInt64(1),
        },
        projection.clone(),
    );
    limited_filter.source_order_limit = Some(1);
    let mut pruned_filter = limited_filter.clone();
    pruned_filter.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("id").unwrap(),
        op: ComparisonOp::Gt,
        value: StatValue::UInt64(4),
    });
    let requests = [
        (
            VortexQueryPrimitiveRequest::project(uri.clone(), projection.clone()),
            vec![1, 2, 3, 4],
        ),
        (
            VortexQueryPrimitiveRequest::filter_and_project(
                uri.clone(),
                PredicateExpr::Compare {
                    column: ColumnRef::new("id").unwrap(),
                    op: ComparisonOp::Gt,
                    value: StatValue::UInt64(2),
                },
                projection.clone(),
            ),
            vec![3, 4],
        ),
        (
            VortexQueryPrimitiveRequest::sort_rows(
                uri,
                projection,
                None,
                crate::VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new(
                    "id", true,
                )]),
                2,
            ),
            vec![4, 3],
        ),
        (limited_projection, vec![1, 2]),
        (limited_filter, vec![2]),
        (pruned_filter, vec![]),
    ];
    let source = prepared_dispatch::prepare_source(&requests[0].0, policy()).unwrap();
    let session = source.retained_session();
    let memory = session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    let dtype = DType::struct_(
        [("id", DType::Primitive(PType::U32, Nullability::NonNullable))],
        Nullability::NonNullable,
    );
    let formats = [
        Format::Vortex,
        Format::ArrowIpc,
        Format::Parquet,
        Format::Avro,
        Format::Orc,
        Format::Json,
        Format::Jsonl,
        Format::Csv,
    ];
    let mut executions = 0;
    for (index, (request, ids)) in requests.iter().enumerate() {
        let expected = ids.iter().map(|id| json!({"id":id})).collect::<Vec<_>>();
        for format in formats {
            let path = fixture
                .0
                .join(format!("handoff-{index}.{}", format.as_str()));
            let report = prepared_dispatch::try_write_source(
                request,
                &path,
                format,
                false,
                policy(),
                source.clone(),
            )
            .unwrap()
            .unwrap();
            assert!(!report.has_errors());
            assert_eq!(report.rows_written, ids.len() as u64);
            assert!(!report.evidence.side_effects.fallback_attempted);
            assert!(report.pre_limit_result_row_count >= report.rows_written);
            if !ids.is_empty() {
                assert!(report.evidence.upstream_scan_called);
                assert!(report.evidence.side_effects.data_read);
                assert!(report.evidence.side_effects.data_decoded);
            }
            if index == 3 {
                assert_eq!(report.pre_limit_result_row_count, 4);
                assert!(
                    report
                        .evidence
                        .native_array_sink
                        .as_ref()
                        .unwrap()
                        .pre_limit_result_row_count_exact
                );
            } else if index == 4 {
                assert!(
                    !report
                        .evidence
                        .native_array_sink
                        .as_ref()
                        .unwrap()
                        .pre_limit_result_row_count_exact
                );
            }
            if format == Format::Csv {
                let mut expected = String::from("id\n");
                for id in ids {
                    writeln!(expected, "{id}").unwrap();
                }
                assert_eq!(fs::read_to_string(&path).unwrap(), expected);
            } else {
                assert_eq!(super::io_tests::reopen(&path, format, &dtype), expected);
            }
            executions += 1;
            assert_eq!(session.snapshot().prepared_source_opens, 1);
            assert_eq!(session.snapshot().completed_executions, executions);
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
        }
    }
    fixture.replace();
    for (index, (request, _)) in requests.iter().enumerate() {
        let path = fixture.0.join(format!("replaced-{index}.vortex"));
        assert!(
            prepared_dispatch::try_write_source(
                request,
                &path,
                Format::Vortex,
                false,
                policy(),
                source.clone()
            )
            .is_err()
        );
        assert!(!path.exists());
    }
    assert_eq!(session.snapshot().prepared_source_opens, 1);
    drop(source);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
#[cfg(feature = "universal-format-io")]
fn native_source_handoff_projection_writer_streams_above_collection_limit() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    const ROWS: usize = 65_541;
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id"]),
            vec![PrimitiveArray::from_iter(0..ROWS as u64).into_array()],
            ROWS,
            Validity::NonNullable,
        )
        .into_array(),
        512,
    );
    let request = VortexQueryPrimitiveRequest::project(
        DatasetUri::new(fixture.path().display().to_string()).unwrap(),
        shardloom_plan::ProjectionRequest::All,
    );
    let source = prepared_dispatch::prepare_source(&request, policy()).unwrap();
    let session = source.retained_session();
    let baseline = session.snapshot().memory.reserved_bytes;
    for format in [Format::Jsonl, Format::ArrowIpc] {
        let path = fixture.0.join(format!("complete.{}", format.as_str()));
        let report = prepared_dispatch::try_write_source(
            &request,
            &path,
            format,
            false,
            policy(),
            source.clone(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(report.rows_written, ROWS as u64);
        assert_eq!(session.snapshot().prepared_source_opens, 1);
        assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
        if format == Format::Jsonl {
            let rows = fs::read_to_string(path)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                rows,
                (0..ROWS).map(|id| json!({"id":id})).collect::<Vec<_>>()
            );
        } else {
            let reader =
                arrow_ipc::reader::FileReader::try_new(fs::File::open(path).unwrap(), None)
                    .unwrap();
            assert_eq!(
                reader.schema().field(0).data_type(),
                &arrow_schema::DataType::UInt64
            );
            let mut count = 0;
            for batch in reader {
                let batch = batch.unwrap();
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::UInt64Array>()
                    .unwrap();
                for id in ids.values() {
                    assert_eq!(*id, count);
                    count += 1;
                }
            }
            assert_eq!(count, ROWS as u64);
        }
    }
}
