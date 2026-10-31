//! Release-only, resident-Parquet attribution; original complete references stay external.

use super::*;
use crate::{
    local_primitives::native_relational_grouped_cost::{self as cost, PHASES, Phase},
    query_primitive::VortexSimpleAggregateMeasure as Measure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalFilter, VortexRelationalNullOrder,
        VortexRelationalOrderKey, VortexRelationalSort,
    },
    universal_format_io::demanded::LocalColumnarSource,
};
use serde_json::{Value, json};
use shardloom_core::{ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue};

fn expression(kind: ExpressionKind) -> Expression {
    Expression::new(ExprId::new("grouped-cost").unwrap(), kind)
}

fn plan(job: &Value, uri: DatasetUri) -> VortexRelationalPlan {
    let value = job["value"].as_str().unwrap();
    assert!(["signed_value", "optional_value", "metric", "all_null"].contains(&value));
    let groups = match job["groups"].as_str().unwrap() {
        "group_key" => vec!["group_key"],
        "category" => vec!["category"],
        "group_key_category" => vec!["group_key", "category"],
        _ => panic!("undeclared diagnostic grouping"),
    };
    let functions = match job["measures"].as_str().unwrap() {
        "sum" => vec!["sum"],
        "mixed" => vec!["sum", "avg", "min", "max", "count"],
        _ => panic!("undeclared diagnostic measures"),
    };
    let mut input = VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: uri,
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    });
    match job["selection"].as_str().unwrap() {
        "all" => {}
        "one_percent" => {
            input = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
                input,
                predicate: expression(ExpressionKind::Compare {
                    left: Box::new(expression(ExpressionKind::Column(
                        ColumnRef::new("selection_key").unwrap(),
                    ))),
                    op: ComparisonOp::Lt,
                    right: Box::new(expression(ExpressionKind::Literal(ScalarValue::Int64(
                        10_000,
                    )))),
                }),
            }));
        }
        _ => panic!("undeclared diagnostic selection"),
    }
    let aggregate = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input,
        group_by: groups
            .iter()
            .map(|name| ColumnRef::new(*name).unwrap())
            .collect(),
        grouping_sets: None,
        measures: functions
            .iter()
            .map(|function| {
                Measure::new(
                    *function,
                    Some(ColumnRef::new(value).unwrap()),
                    format!("value_{function}"),
                )
            })
            .collect(),
    }));
    VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: aggregate,
        keys: groups
            .iter()
            .map(|name| VortexRelationalOrderKey {
                column: ColumnRef::new(*name).unwrap(),
                descending: false,
                nulls: Some(VortexRelationalNullOrder::Last),
            })
            .collect(),
    }))
}

#[test]
#[ignore = "frozen grouped-cost attribution; release build and guarded isolated execution only"]
#[allow(clippy::too_many_lines)] // One complete query owns its observation, source, values and refunds.
#[allow(clippy::assertions_on_constants)] // Debug builds compile but must refuse manual execution.
fn native_grouped_cost_attribution() {
    assert!(
        !cfg!(debug_assertions),
        "use release; per-row debug observations must be disabled"
    );
    let path = PathBuf::from(
        std::env::var_os("SHARDLOOM_GROUPED_COST_JOB").expect("frozen diagnostic job"),
    );
    assert!(path.is_absolute() && fs::metadata(&path).unwrap().len() <= 64 * 1024);
    let job: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(job["schema_version"], "shardloom.grouped_cost_job.v1");
    let source = PathBuf::from(job["source_path"].as_str().unwrap());
    assert!(source.is_absolute() && source.is_file());
    let uri = DatasetUri::new(source.display().to_string()).unwrap();
    let declaration = plan(&job, uri.clone());
    let resources = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 2).unwrap();
    let prepared = prepare_relational_with_schema(resources, |preparation| {
        let admitted = LocalColumnarSource::admit(
            &source,
            "parquet",
            &preparation.shared_memory_pool(),
            &preparation.cancellation_token(),
        )?;
        preparation.register_local_source(uri, admitted)?;
        Ok(declaration)
    })
    .unwrap();
    assert!(
        prepared.provider.is_none(),
        "diagnostic requires the shared relational owner"
    );
    let memory = prepared.session.memory().clone();
    let initial = memory.snapshot().reserved_bytes;
    let observation = cost::Session::start();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let phases = observation.finish();
    let engine_ns = result.timing.execution.as_nanos();
    let classified_ns: u128 = phases.nanos[..9].iter().sum();
    assert!(
        classified_ns <= engine_ns,
        "classified phases exceed engine execution"
    );
    for phase in [
        Phase::SourceIntake,
        Phase::GroupInterning,
        Phase::AggregateRemainder,
        Phase::Finalization,
    ] {
        assert!(
            phases.calls[phase as usize] > 0,
            "missing diagnostic activation: {phase:?}"
        );
    }
    let input = result
        .execution
        .input
        .as_ref()
        .expect("demanded source report");
    assert_eq!(input.columnar_sources, 1);
    assert!(input.end_of_input_observed && input.output_ownership_detached);
    let source_report = json!({
        "projected_columns":input.columnar_projected_columns,
        "rows_decoded":input.columnar_rows_decoded,
        "data_read_bytes":input.columnar_data_read_bytes,
        "data_read_requests":input.columnar_data_read_requests,
        "end_of_input_observed":input.end_of_input_observed,
        "output_ownership_detached":input.output_ownership_detached,
    });
    let certificate = &result.execution.native_io_certificate;
    assert!(certificate.is_certified() && !certificate.side_effects.fallback_attempted);
    let actual = json_rows(&result);
    let schema: Value = serde_json::from_str(result.result_schema_json.value()).unwrap();
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, initial);
    let snapshot = prepared.snapshot();
    assert_eq!(snapshot.completed_executions, 1);
    assert_eq!(snapshot.admitted_max_parallelism, 2);
    drop(prepared);
    let final_memory = memory.snapshot();
    assert_eq!(final_memory.reserved_bytes, 0);
    assert_eq!(final_memory.denied_reservations, 0);
    assert!(final_memory.peak_reserved_bytes <= 2 << 30);
    let phases: Vec<_> = PHASES
        .iter()
        .enumerate()
        .map(|(index, name)| {
            json!({
                "phase":name, "exclusive_nanos":phases.nanos[index], "calls":phases.calls[index],
                "engine_classified":index < 9,
            })
        })
        .collect();
    println!(
        "GROUPED_COST {}",
        json!({
            "schema_version":"shardloom.grouped_cost_observation.v1", "job":job,
            "actual_rows":actual, "actual_schema":schema, "complete_consumption":true,
            "engine_execution_ns":engine_ns, "classified_engine_ns":classified_ns,
            "unclassified_engine_ns":engine_ns-classified_ns, "phases":phases,
            "source":source_report, "native_io_certified":true,
            "fallback_attempted":false, "external_engine_invoked":false,
            "memory_grant_bytes":2_u64 << 30, "max_parallelism":2,
            "peak_reserved_bytes":final_memory.peak_reserved_bytes,
            "final_reserved_bytes":0, "denied_reservations":0,
            "scope":"instrumented native relational diagnostic; no SQL preparation or production speedup claim",
        })
    );
}
