//! One executable plan across file kernels, native composition and direct inputs.

use super::*;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::{VortexRelationalAggregate, VortexRelationalSpillPolicy},
};
use serde_json::json;

#[path = "local_primitive_native_pipeline_coverage_tests.rs"]
mod coverage_tests;

fn count(input: VortexRelationalPlan) -> VortexRelationalPlan {
    VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input,
        group_by: vec![],
        grouping_sets: None,
        measures: vec![VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".into(),
        )],
    }))
}

fn count_join(left: &Fixture, right: &Fixture) -> VortexRelationalPlan {
    VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: count(left.scan()),
        right: count(right.scan()),
        kind: JoinKind::Cross,
        condition: None,
        keys: vec![],
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("rows").unwrap(),
                output_column: "left_rows".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Right,
                column: ColumnRef::new("rows").unwrap(),
                output_column: "right_rows".into(),
            },
        ],
    }))
}

fn pipeline(execution: &ExecutedVortexRelational, kernels: u64) {
    assert_eq!(
        execution.provider_selection_reason,
        "native_operator_pipeline"
    );
    let certificate = &execution.native_io_certificate;
    assert!(certificate.is_certified());
    assert!(!certificate.fallback_attempted);
    assert!(!certificate.side_effects.fallback_execution_allowed);
    assert!(
        certificate
            .source_pushdown_report
            .proof_basis
            .contains(&format!(
                "native_pipeline_file_kernel_invocations={kernels} "
            ))
    );
    assert!(
        certificate
            .source_pushdown_report
            .proof_basis
            .contains("native_pipeline_catch_all_execution=false")
    );
}

#[test]
fn native_pipeline_admits_both_file_sources_inside_a_join() {
    let left = Fixture::new(keyed(&[Some(2), None, Some(1)], &[10, 11, 12]), 1);
    let right = Fixture::new(keyed(&[Some(2), Some(1)], &[20, 21]), 1);
    let plan = count_join(&left, &right);
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let PreparedRoot::Bound(root) = &prepared.root else {
        panic!("bound plan")
    };
    let NodeKind::Join { left, right, .. } = &root.kind else {
        panic!("native join")
    };
    assert!(left.has_file_kernel());
    assert!(
        right.has_file_kernel(),
        "source index one must be admitted too"
    );
    let baseline = prepared.snapshot().reserved_without_source_segment_cache();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [json!({"left_rows":3,"right_rows":2})]);
    pipeline(&result.execution, 2);
    assert!(result.execution.primitive_execution.is_none());
    assert_eq!(result.execution.runtime.completed_executions, 1);
    assert_eq!(result.execution.runtime.prepared_source_opens, 2);
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .data_read
    );
    drop(result);
    assert_eq!(
        prepared.snapshot().reserved_without_source_segment_cache(),
        baseline
    );
    let mut reference = prepare_relational(&plan, policy()).unwrap();
    reference.remove_file_kernels_for_reference().unwrap();
    let result = reference
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [json!({"left_rows":3,"right_rows":2})]);
    pipeline(&result.execution, 0);
}

#[test]
fn native_pipeline_nested_aggregate_evidence_survives_window_composition() {
    use crate::relational_query::{
        VortexRelationalNullOrder, VortexRelationalOrderKey, VortexRelationalWindow,
        VortexRelationalWindowExpression, VortexRelationalWindowFunction,
    };
    let fixture = Fixture::new(keyed(&[Some(1), Some(1), Some(2)], &[10, 20, 5]), 1);
    let aggregate = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input: fixture.scan(),
        group_by: vec![ColumnRef::new("entity").unwrap()],
        grouping_sets: None,
        measures: vec![VortexSimpleAggregateMeasure::new(
            "sum",
            Some(ColumnRef::new("amount").unwrap()),
            "total".into(),
        )],
    }));
    let plan = VortexRelationalPlan::Window(Box::new(VortexRelationalWindow {
        input: aggregate,
        columns: vec![
            ColumnRef::new("entity").unwrap(),
            ColumnRef::new("total").unwrap(),
        ],
        expressions: vec![VortexRelationalWindowExpression {
            output_column: "position".into(),
            function: VortexRelationalWindowFunction::RowNumber,
            partition_by: vec![],
            order_by: vec![VortexRelationalOrderKey {
                column: ColumnRef::new("total").unwrap(),
                descending: false,
                nulls: Some(VortexRelationalNullOrder::Last),
            }],
            frame: None,
        }],
    }));
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let PreparedRoot::Bound(root) = &prepared.root else {
        panic!("bound plan")
    };
    let NodeKind::Window { input, .. } = &root.kind else {
        panic!("native window")
    };
    assert!(input.has_file_kernel());
    let actual = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let mut reference = prepare_relational(&plan, policy()).unwrap();
    reference.remove_file_kernels_for_reference().unwrap();
    let expected = reference
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&actual), json_rows(&expected));
    assert_eq!(
        actual.result_schema_json.value(),
        expected.result_schema_json.value()
    );
    let rows = json_rows(&actual);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| row["entity"] == 1
        && row["total"].as_f64() == Some(30.0)
        && row["position"] == 2));
    assert!(rows.iter().any(|row| row["entity"] == 2
        && row["total"].as_f64() == Some(5.0)
        && row["position"] == 1));
    pipeline(&actual.execution, 1);
    assert!(actual.execution.primitive_execution.is_none());
    let certificate = &actual.execution.native_io_certificate;
    assert!(certificate.side_effects.data_materialized);
    assert!(
        certificate
            .materialization_boundaries
            .iter()
            .any(|boundary| {
                boundary.boundary_id == "local_primitive.simple_aggregate.bounded_materialization"
                    && boundary.rows_materialized == 2
            })
    );
    assert_ne!(certificate.representation_transitions, []);
}

#[test]
fn native_pipeline_terminal_consumer_error_does_not_retry_a_composed_query() {
    let left = Fixture::new(keyed(&[Some(1), Some(2)], &[10, 11]), 1);
    let right = Fixture::new(keyed(&[Some(1)], &[20]), 1);
    let prepared = prepare_relational(&count_join(&left, &right), policy()).unwrap();
    let baseline = prepared.snapshot().reserved_without_source_segment_cache();
    let mut calls = 0;
    let error = prepared
        .for_each_batch(&CancellationToken::default(), |_, _| {
            calls += 1;
            Err(failed("deliberate final consumer failure"))
        })
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains("deliberate final consumer failure")
    );
    assert_eq!(calls, 1);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(
        prepared.snapshot().reserved_without_source_segment_cache(),
        baseline
    );
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [json!({"left_rows":2,"right_rows":1})]);
    pipeline(&result.execution, 2);
}

#[test]
fn native_pipeline_missing_admission_fails_before_result_delivery() {
    let fixture = Fixture::new(keyed(&[Some(1)], &[10]), 1);
    let mut prepared = prepare_relational(&fixture.scan(), policy()).unwrap();
    let PreparedRoot::Bound(root) = &mut prepared.root else {
        panic!("bound plan")
    };
    root.kind = NodeKind::Unadmitted;
    let error = native_pipeline::compile(
        root,
        &prepared.sources,
        &prepared.session,
        prepared.policy,
        true,
        &CancellationToken::default(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("native_pipeline_missing_operator")
    );
    let mut calls = 0;
    let error = prepared
        .for_each_batch(&CancellationToken::default(), |_, _| {
            calls += 1;
            Ok(())
        })
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains("native_pipeline_missing_operator")
    );
    assert_eq!(calls, 0);
    assert_eq!(prepared.snapshot().completed_executions, 0);
}

#[test]
fn native_pipeline_rejects_foreign_context_before_kernel_admission() {
    let fixture = Fixture::new(keyed(&[Some(1)], &[10]), 1);
    let mut prepared = prepare_relational(&count(fixture.scan()), policy()).unwrap();
    prepared.remove_file_kernels_for_reference().unwrap();
    let PreparedRoot::Bound(root) = &prepared.root else {
        panic!("bound plan")
    };
    let foreign =
        ResidentVortexSession::new(policy().resource_envelope.memory_budget_bytes, 1).unwrap();
    let baseline = prepared.snapshot().reserved_without_source_segment_cache();
    let error = foreign
        .with_native_execution_context(&CancellationToken::default(), |context| {
            provider::prepare(
                root,
                &prepared.sources[0],
                policy(),
                prepared.session.memory(),
                prepared.session.float_policy(),
                Some(context),
            )
        })
        .err()
        .unwrap();
    assert!(error.to_string().contains("different session"));
    assert_eq!(
        prepared.snapshot().reserved_without_source_segment_cache(),
        baseline
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(foreign.snapshot().completed_executions, 0);
}

#[test]
fn native_pipeline_compiles_deferred_and_retained_file_queries() {
    let fixture = Fixture::new(keyed(&[Some(1), None, Some(2)], &[10, 20, 30]), 1);
    let plan = count(fixture.scan());
    let uri = DatasetUri::new(fixture.path().display().to_string()).unwrap();
    let dynamic =
        prepare_relational_with_dynamic_schema(&[uri], policy(), 65_536, move |_| Ok(plan.clone()))
            .unwrap();
    assert!(dynamic.output_dtype().is_none());
    let result = dynamic
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [json!({"rows":3})]);
    pipeline(&result.execution, 1);
    assert_eq!(
        result
            .execution
            .primitive_execution
            .as_ref()
            .unwrap()
            .primitive_kind,
        VortexQueryPrimitiveKind::CountAll
    );
    let definition = std::sync::Arc::new(count(fixture.scan()));
    let plan = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: VortexRelationalPlan::Materialized(std::sync::Arc::clone(&definition)),
        right: VortexRelationalPlan::Materialized(definition),
        kind: SetKind::UnionAll,
    }));
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [json!({"rows":3}), json!({"rows":3})]);
    pipeline(&result.execution, 1);
    assert_eq!(result.execution.composition.materialized_relations, 1);
    assert_eq!(result.execution.composition.materialized_reuses, 1);
}

#[test]
#[allow(clippy::used_underscore_binding)] // Inspect the retained reservation, not just the resulting values.
fn native_pipeline_deferred_nested_evidence_owns_credit_until_result_release() {
    let left = Fixture::new(keyed(&[Some(1), Some(2)], &[10, 20]), 1);
    let right = Fixture::new(keyed(&[Some(1)], &[30]), 1);
    let plan = count_join(&left, &right);
    let sources = [&left, &right]
        .map(|fixture| DatasetUri::new(fixture.path().display().to_string()).unwrap());
    let prepared = prepare_relational_with_dynamic_schema(&sources, policy(), 65_536, move |_| {
        Ok(plan.clone())
    })
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [json!({"left_rows":2,"right_rows":1})]);
    pipeline(&result.execution, 2);
    let summary_credit: u64 = result
        .execution
        ._pipeline_metadata
        .iter()
        .flatten()
        .map(MemoryLease::bytes)
        .sum();
    assert!(summary_credit > 0);
    let retained = prepared.snapshot().memory.reserved_bytes;
    assert!(retained >= baseline + summary_credit);
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn native_pipeline_deferred_kernels_borrow_the_active_context() {
    use crate::relational_query::{
        VortexRelationalNullOrder, VortexRelationalOrderKey, VortexRelationalSort,
    };
    use shardloom_core::{ComparisonOp, PredicateExpr, StatValue};
    let fixture = Fixture::new(keyed(&[Some(2), None, Some(1)], &[30, 10, 20]), 1);
    let mut filtered = fixture.scan();
    let VortexRelationalPlan::Scan(scan) = &mut filtered else {
        unreachable!()
    };
    scan.predicate = Some(PredicateExpr::And(vec![
        PredicateExpr::Compare {
            column: ColumnRef::new("amount").unwrap(),
            op: ComparisonOp::GtEq,
            value: StatValue::UInt64(10),
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("amount").unwrap(),
            op: ComparisonOp::LtEq,
            value: StatValue::UInt64(20),
        },
    ]));
    let aggregate = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input: filtered.clone(),
        group_by: vec![],
        grouping_sets: None,
        measures: vec![VortexSimpleAggregateMeasure::new(
            "sum",
            Some(ColumnRef::new("amount").unwrap()),
            "sum".into(),
        )],
    }));
    let sorted = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: fixture.scan(),
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new("amount").unwrap(),
            descending: false,
            nulls: Some(VortexRelationalNullOrder::First),
        }],
    }));
    for plan in [
        count(fixture.scan()),
        count(filtered),
        aggregate,
        fixture.scan(),
        sorted,
    ] {
        let mut reference = prepare_relational(&plan, policy()).unwrap();
        reference.remove_file_kernels_for_reference().unwrap();
        let expected = reference
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let uri = DatasetUri::new(fixture.path().display().to_string()).unwrap();
        let dynamic = prepare_relational_with_dynamic_schema(&[uri], policy(), 65_536, move |_| {
            Ok(plan.clone())
        })
        .unwrap();
        let baseline = dynamic.snapshot().reserved_without_source_segment_cache();
        let actual = dynamic
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&actual), json_rows(&expected));
        assert_eq!(
            actual.result_schema_json.value(),
            expected.result_schema_json.value()
        );
        pipeline(&actual.execution, 1);
        assert_eq!(actual.execution.runtime.completed_executions, 1);
        assert_eq!(actual.execution.runtime.prepared_source_opens, 1);
        drop(actual);
        assert_eq!(
            dynamic.snapshot().reserved_without_source_segment_cache(),
            baseline
        );
    }
}

#[test]
fn native_pipeline_spill_policy_selects_operators_before_execution() {
    let left = Fixture::new(keyed(&[Some(1), Some(2)], &[10, 20]), 1);
    let right = Fixture::new(keyed(&[Some(1)], &[30]), 1);
    let workspace = left.0.join("spill");
    fs::create_dir(&workspace).unwrap();
    let prepared = prepare_relational(&count_join(&left, &right), policy())
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20).unwrap())
        .unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [json!({"left_rows":2,"right_rows":1})]);
    pipeline(&result.execution, 0);
    assert!(fs::read_dir(workspace).unwrap().next().is_none());
}

#[test]
fn native_pipeline_accepts_memory_without_source_preparation() {
    let uri = DatasetUri::new("memory://pipeline").unwrap();
    let prepared = prepare_relational_with_schema(policy(), |schemas| {
        schemas.register_memory_source(uri.clone(), |session| {
            ResidentMemorySource::from_int64_range(session, "value", 0, 1, 7)
        })?;
        Ok(count(VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri,
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        })))
    })
    .unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [json!({"rows":7})]);
    pipeline(&result.execution, 0);
    assert_eq!(result.execution.runtime.prepared_source_opens, 0);
    assert!(!result.execution.native_io_certificate.side_effects.write_io);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn native_pipeline_accepts_original_parquet_and_ipc_without_prepared_writes() {
    use crate::universal_format_io::demanded::LocalColumnarSource;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let directory = Fixture::new(keyed(&[], &[]), 1);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![3, 1, 2]))],
    )
    .unwrap();
    for format in ["parquet", "arrow_ipc"] {
        let path = directory.0.join(format!("original.{format}"));
        let file = fs::File::create(&path).unwrap();
        if format == "parquet" {
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        } else {
            let mut writer = arrow_ipc::writer::FileWriter::try_new(file, &batch.schema()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        let files = fs::read_dir(&directory.0).unwrap().count();
        let original = fs::read(&path).unwrap();
        let prepared = prepare_relational_with_schema(policy(), |schemas| {
            let source = LocalColumnarSource::admit(
                &path,
                format,
                &schemas.shared_memory_pool(),
                &schemas.cancellation_token(),
            )?;
            let uri = DatasetUri::new(path.display().to_string())?;
            schemas.register_local_source(uri.clone(), source)?;
            Ok(count(VortexRelationalPlan::Scan(VortexRelationalScan {
                source_uri: uri,
                projection: shardloom_plan::ProjectionRequest::All,
                predicate: None,
            })))
        })
        .unwrap();
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), [json!({"rows":3})]);
        pipeline(&result.execution, 0);
        assert!(!result.execution.native_io_certificate.side_effects.write_io);
        assert_eq!(result.execution.runtime.prepared_source_opens, 0);
        assert_eq!(fs::read(&path).unwrap(), original);
        assert_eq!(fs::read_dir(&directory.0).unwrap().count(), files);
    }
}
