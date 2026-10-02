use super::*;
use crate::{
    query_primitive::{
        VortexDuplicateKeepPolicy as Keep, VortexExpressionProjectionRequest,
        VortexExpressionRewrite as Rewrite, VortexMeltProjectionRequest,
        VortexQueryPrimitiveKind as Kind, VortexRollingWindowRequest,
    },
    relational_query::{
        VortexRelationalAggregate, VortexRelationalLimit, VortexRelationalNullOrder,
        VortexRelationalOrderKey, VortexRelationalProject, VortexRelationalSort,
        VortexRelationalUnary,
    },
};
use shardloom_core::{ExprId, Expression};
use shardloom_plan::ProjectionRequest;
use vortex::array::arrays::VarBinViewArray;

fn columns(names: &[&str]) -> ProjectionRequest {
    ProjectionRequest::columns(
        names
            .iter()
            .map(|name| ColumnRef::new(*name).unwrap())
            .collect(),
    )
}

fn request(kind: Kind, names: &[&str]) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::for_relational_input(kind, columns(names))
}

fn unary(
    input: VortexRelationalPlan,
    request: VortexQueryPrimitiveRequest,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary { input, request }))
}

fn project(input: VortexRelationalPlan, fields: &[(&str, &str)]) -> VortexRelationalPlan {
    VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input,
        expressions: fields
            .iter()
            .map(|(source, name)| {
                (
                    (*name).into(),
                    Expression::column(
                        ExprId::new(*name).unwrap(),
                        ColumnRef::new(*source).unwrap(),
                    ),
                )
            })
            .collect(),
    }))
}

fn sort(input: VortexRelationalPlan, column: &str, descending: bool) -> VortexRelationalPlan {
    VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input,
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new(column).unwrap(),
            descending,
            nulls: Some(VortexRelationalNullOrder::Last),
        }],
    }))
}

fn range(input: VortexRelationalPlan, offset: usize, count: usize) -> VortexRelationalPlan {
    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input,
        offset,
        count,
    }))
}

fn tail(input: VortexRelationalPlan, names: &[&str], count: usize) -> VortexRelationalPlan {
    let mut req = request(Kind::TailRows, names);
    req.source_order_limit = Some(count);
    unary(input, req)
}

fn collect(plan: &VortexRelationalPlan) -> Vec<serde_json::Value> {
    let prepared = prepare_relational(plan, policy()).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut prior = None;
    for run in 1..=2 {
        let output = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert!(output.execution.native_io_certificate.is_certified());
        assert!(
            !output
                .execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
        assert_eq!(output.execution.runtime.completed_executions, run);
        let rows = json_rows(&output);
        if let Some(prior) = &prior {
            assert_eq!(&rows, prior);
        }
        prior = Some(rows);
        drop(output);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    prior.unwrap()
}

fn transformed(fixture: &Fixture) -> VortexRelationalPlan {
    project(
        range(sort(fixture.scan(), "amount", true), 1, 4),
        &[("entity", "key"), ("amount", "payload")],
    )
}

#[test]
fn composed_unary_selectors_preserve_stage_positions_and_all_duplicate_policies() {
    let fixture = Fixture::new(
        keyed(
            &[Some(2), Some(1), Some(2), None, Some(1), Some(3)],
            &[10, 11, 12, 13, 14, 15],
        ),
        2,
    );
    let distinct = unary(transformed(&fixture), request(Kind::DistinctRows, &["key"]));
    assert_eq!(
        collect(&distinct),
        vec![
            serde_json::json!({"key":1}),
            serde_json::json!({"key":null}),
            serde_json::json!({"key":2})
        ]
    );
    assert_eq!(
        collect(&tail(distinct, &["key"], 2)),
        vec![
            serde_json::json!({"key":null}),
            serde_json::json!({"key":2})
        ]
    );
    for (keep, expected, masks) in [
        (
            Keep::First,
            vec![14, 13, 12],
            vec![false, false, false, true],
        ),
        (
            Keep::Last,
            vec![13, 12, 11],
            vec![true, false, false, false],
        ),
        (
            Keep::AllDuplicates,
            vec![13, 12],
            vec![true, false, false, true],
        ),
    ] {
        let mut req = request(Kind::DropDuplicateRows, &["key", "payload"]);
        req.deduplicate_key_projection = Some(columns(&["key"]));
        req.duplicate_keep = keep;
        let plan = unary(transformed(&fixture), req);
        let rows = collect(&plan);
        assert_eq!(
            rows.iter()
                .map(|r| r["payload"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(collect(&range(plan, 0, 2)), rows[..2]);
        let mut req = request(Kind::DuplicateMaskRows, &["key"]);
        req.duplicate_keep = keep;
        let plan = project(unary(transformed(&fixture), req), &[("duplicated", "mask")]);
        assert_eq!(
            collect(&plan),
            masks
                .iter()
                .map(|mask| serde_json::json!({"mask":mask}))
                .collect::<Vec<_>>()
        );
    }
    let suffix = tail(transformed(&fixture), &["payload"], 2);
    assert_eq!(
        collect(&range(suffix, 0, 1)),
        vec![serde_json::json!({"payload":12})]
    );
    assert_eq!(
        collect(&tail(range(transformed(&fixture), 0, 1), &["payload"], 2)),
        vec![serde_json::json!({"payload":14})]
    );
}

#[test]
fn composed_unary_sampling_preserves_seed_weight_fraction_replacement_and_hidden_columns() {
    let fixture = Fixture::new(
        keyed(
            &[Some(5), Some(4), Some(3), Some(2), Some(1)],
            &[50, 40, 30, 20, 10],
        ),
        2,
    );
    let input = sort(fixture.scan(), "entity", false);
    for (fraction, replacement, weighted, expected) in [
        (false, false, false, vec![2, 5]),
        (false, false, true, vec![2, 5]),
        (true, false, false, vec![2, 5]),
        (true, false, true, vec![2, 5]),
        (false, true, false, vec![4, 2, 5, 4, 4, 2, 5]),
        (false, true, true, vec![5, 2, 2, 5, 4, 4, 3]),
    ] {
        let mut req = request(Kind::SampleRows, &["entity"]);
        req.sample_fraction = fraction.then_some(0.4);
        req.source_order_limit = (!fraction).then_some(if replacement { 7 } else { 2 });
        req.sample_seed = Some(if replacement { 11 } else { 7 });
        req.sample_with_replacement = replacement;
        req.sample_weight_column = weighted.then(|| ColumnRef::new("amount").unwrap());
        let plan = project(unary(input.clone(), req), &[("entity", "selected")]);
        assert_eq!(
            collect(&plan),
            expected
                .iter()
                .map(|n| serde_json::json!({"selected":n}))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn composed_unary_melt_feeds_aggregate_with_native_nullable_widening() {
    let fixture = Fixture::new(
        keyed(
            &[Some(2), Some(1), Some(2), None, Some(1), Some(3)],
            &[10, 11, 12, 13, 14, 15],
        ),
        2,
    );
    let mut req = request(Kind::MeltRows, &["key", "payload"]);
    req.melt_projection = Some(VortexMeltProjectionRequest::new(
        vec![],
        vec![
            ColumnRef::new("key").unwrap(),
            ColumnRef::new("payload").unwrap(),
        ],
        "field".into(),
        "value".into(),
    ));
    let melted = unary(range(transformed(&fixture), 0, 2), req);
    assert_eq!(
        collect(&melted),
        serde_json::json!([
            {"field":"key","value":1},{"field":"payload","value":14},
            {"field":"key","value":null},{"field":"payload","value":13}
        ])
        .as_array()
        .unwrap()
        .clone()
    );
    let grouped = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input: melted,
        group_by: vec![ColumnRef::new("field").unwrap()],
        measures: vec![crate::query_primitive::VortexSimpleAggregateMeasure::new(
            "sum",
            Some(ColumnRef::new("value").unwrap()),
            "sum".into(),
        )],
    }));
    assert_eq!(
        collect(&sort(grouped, "field", false)),
        serde_json::json!([
            {"field":"key","sum":1.0},{"field":"payload","sum":27.0}
        ])
        .as_array()
        .unwrap()
        .clone()
    );
}

#[test]
fn composed_unary_rewrite_and_rolling_keep_state_across_native_batches() {
    let count = 4_105_u32;
    let fixture = Fixture::new(
        keyed(
            &(0..count)
                .map(|n| (n % 7 == 0).then_some(u64::from(n)))
                .collect::<Vec<_>>(),
            &(0..count).collect::<Vec<_>>(),
        ),
        333,
    );
    let input = project(fixture.scan(), &[("entity", "key"), ("amount", "payload")]);
    let mut req = request(Kind::ExpressionProjectRows, &["key", "payload"]);
    req.expression_projection = Some(VortexExpressionProjectionRequest::new(vec![
        Rewrite::ForwardFillNull {
            target_column: ColumnRef::new("key").unwrap(),
            limit: Some(4),
        },
        Rewrite::RowNumber {
            target_column: ColumnRef::new("ordinal").unwrap(),
            start: 17,
        },
    ]));
    let rewritten = unary(input, req);
    let rows = collect(&rewritten);
    assert_eq!(rows.len(), count as usize);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row["ordinal"].as_u64(), Some(index as u64 + 17));
        assert_eq!(row["payload"].as_u64(), Some(index as u64));
        assert_eq!(
            row["key"].as_u64(),
            (index % 7 <= 4).then_some((index / 7 * 7) as u64)
        );
    }
    let mut req = request(Kind::RollingWindowRows, &["ordinal"]);
    req.rolling_window = Some(
        VortexRollingWindowRequest::new(
            ColumnRef::new("ordinal").unwrap(),
            "total".into(),
            3,
            3,
            "sum".into(),
        )
        .with_center(true),
    );
    let rolled = project(unary(rewritten, req), &[("total", "sum")]);
    let rows = collect(&rolled);
    assert_eq!(rows.len(), count as usize - 2);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(
            row["sum"].as_f64(),
            Some(f64::from(u32::try_from(3 * (index + 18)).unwrap()))
        );
    }
}

#[test]
fn composed_unary_empty_binding_rejects_conflicts_before_execution() {
    let empty = Fixture::new(keyed(&[], &[]), 1);
    for kind in [
        Kind::DistinctRows,
        Kind::DropDuplicateRows,
        Kind::DuplicateMaskRows,
        Kind::TailRows,
        Kind::SampleRows,
    ] {
        let mut req = request(kind, &["entity"]);
        if matches!(kind, Kind::TailRows | Kind::SampleRows) {
            req.source_order_limit = Some(2);
        }
        if kind == Kind::DropDuplicateRows {
            req.deduplicate_key_projection = Some(columns(&["entity"]));
        }
        let plan = unary(project(empty.scan(), &[("entity", "entity")]), req.clone());
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let result = prepared.execute_owned().unwrap();
        assert_eq!(result.execution.output_rows, 0);
        assert_eq!(result.execution.output_batches, 1);
        req.source_uri = Some(DatasetUri::new("ignored.vortex").unwrap());
        assert!(prepare_relational(&unary(empty.scan(), req), policy()).is_err());
    }
    for kind in [Kind::ExplodeRows, Kind::PivotRows, Kind::CountAll] {
        assert!(
            prepare_relational(&unary(empty.scan(), request(kind, &["entity"])), policy()).is_err()
        );
    }
    assert!(
        prepare_relational(
            &unary(empty.scan(), request(Kind::DistinctRows, &["missing"])),
            policy()
        )
        .is_err()
    );
}

#[test]
fn composed_unary_tail_retains_owned_buffers_after_producer_and_source_drop() {
    let text = "é".repeat(8_000);
    let fixture = Fixture::new(
        StructArray::try_new(
            FieldNames::from(["id", "text"]),
            vec![
                PrimitiveArray::from_iter([1_u64, u64::MAX]).into_array(),
                VarBinViewArray::from_iter_str(["short", text.as_str()]).into_array(),
            ],
            2,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        1,
    );
    let plan = tail(
        project(fixture.scan(), &[("id", "id"), ("text", "text")]),
        &["id", "text"],
        1,
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let memory = prepared.session.memory().clone();
    let mut retained = Vec::new();
    prepared
        .for_each_batch(&CancellationToken::default(), |array, _| {
            retained.push(array);
            Ok(())
        })
        .unwrap();
    drop(prepared);
    drop(fixture);
    assert!(memory.snapshot().reserved_bytes >= text.len() as u64);
    let session = VortexSession::default();
    let mut execution = session.create_execution_ctx();
    let id = crate::local_primitives::logical_field_from_native_array(&retained[0], "id").unwrap();
    assert!(matches!(
        result_batch::scalar_value(&id, 0, &mut execution).unwrap(),
        result_batch::Value::UInt(u64::MAX)
    ));
    let column =
        crate::local_primitives::logical_field_from_native_array(&retained[0], "text").unwrap();
    let value = result_batch::scalar_value(&column, 0, &mut execution).unwrap();
    assert!(matches!(&value, result_batch::Value::SharedText(value) if value.as_str() == text));
    drop(value);
    drop(column);
    drop(id);
    drop(execution);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn composed_unary_pressure_consumer_failure_cancellation_and_source_change_release_state() {
    let fixture = Fixture::new(
        single("value", PrimitiveArray::from_iter(0..5000_u64).into_array()),
        256,
    );
    let plan = unary(
        project(fixture.scan(), &[("value", "value")]),
        request(Kind::DistinctRows, &["value"]),
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let block = prepared
        .session
        .memory()
        .reserve((32 << 20) - baseline - 128 * 1024)
        .unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    drop(block);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| Err(failed(
                "consumer stopped"
            )))
            .is_err()
    );
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(
        prepared
            .for_each_batch(&cancelled, |_, _| panic!("cancelled before delivery"))
            .is_err()
    );
    let token = CancellationToken::default();
    assert!(
        prepared
            .for_each_batch(&token, |_, _| {
                token.cancel();
                Ok(())
            })
            .is_err()
    );
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    let mut replaced = false;
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                if !replaced {
                    fixture.replace();
                    replaced = true;
                }
                Ok(())
            })
            .is_err()
    );
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(prepared.snapshot().completed_executions, 0);
}
