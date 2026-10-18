//! Complete production scan/filter operations; run only as a frozen cost screen.

use super::*;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::{VortexRelationalAggregate, VortexRelationalFilter},
    resident_memory_source::{MemoryColumn, MemoryColumnValues, MemorySourceBounds},
};
use shardloom_core::{ExprId, Expression, ExpressionKind};
use std::time::Instant;

const ROWS: usize = 16_387;

#[allow(clippy::manual_is_multiple_of)] // Preserve the frozen baseline's measurement source.
fn keep(shape: &str, row: usize) -> Option<bool> {
    match shape {
        "unfiltered" | "all" => Some(true),
        "none" => Some(false),
        "band" => Some((1031..13_019).contains(&row)),
        "sparse" => Some(row % 37 == 3),
        "dense" => Some(row % 37 != 3),
        "nullable" => (row % 17 != 0).then_some(row % 3 != 0),
        _ => panic!("unknown frozen shape"),
    }
}

#[allow(clippy::too_many_lines)] // Keep the complete measured lifecycle and cleanup together.
fn run(shape: &str, count: bool) -> serde_json::Value {
    let values = (0..ROWS)
        .map(|row| Some(i64::try_from(row).unwrap() * 17 - 5))
        .collect::<Vec<_>>();
    let predicate = (0..ROWS).map(|row| keep(shape, row)).collect::<Vec<_>>();
    let expected = (0..ROWS)
        .filter(|row| shape == "unfiltered" || keep(shape, *row) == Some(true))
        .collect::<Vec<_>>();
    let uri = DatasetUri::new("memory://selection-cost").unwrap();
    let mut plan = VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: uri.clone(),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    });
    if shape != "unfiltered" {
        plan = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
            input: plan,
            predicate: Expression::new(
                ExprId::new("keep").unwrap(),
                ExpressionKind::Column(ColumnRef::new("keep").unwrap()),
            ),
        }));
    }
    if count {
        plan = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
            input: plan,
            group_by: vec![],
            measures: vec![VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "rows".into(),
            )],
        }));
    }
    let started = Instant::now();
    let prepared = prepare_relational_with_schema(policy(), |schemas| {
        schemas.register_memory_source(uri, |session| {
            ResidentMemorySource::from_columns(
                session,
                &[
                    MemoryColumn {
                        name: "n",
                        values: MemoryColumnValues::Int64(&values),
                    },
                    MemoryColumn {
                        name: "keep",
                        values: MemoryColumnValues::Bool(&predicate),
                    },
                ],
                MemorySourceBounds::default(),
            )
        })?;
        Ok(plan)
    })
    .unwrap();
    let memory = prepared.session.memory().clone();
    let mut delivered = 0;
    let execution = if count {
        let collected = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&collected),
            vec![serde_json::json!({"rows": expected.len()})]
        );
        collected.execution
    } else {
        prepared
            .for_each_batch(&CancellationToken::default(), |array, context| {
                let n = super::super::super::logical_field_from_native_array(&array, "n")?;
                let keep_values =
                    super::super::super::logical_field_from_native_array(&array, "keep")?;
                let mut ctx = context.native_session().create_execution_ctx();
                for row in 0..array.len() {
                    assert_eq!(
                        n.execute_scalar(row, &mut ctx).unwrap(),
                        vortex::scalar::Scalar::from(values[expected[delivered]].unwrap())
                            .cast(n.dtype())
                            .unwrap()
                    );
                    assert_eq!(
                        keep_values.execute_scalar(row, &mut ctx).unwrap(),
                        vortex::scalar::Scalar::from(predicate[expected[delivered]])
                            .cast(keep_values.dtype())
                            .unwrap()
                    );
                    delivered += 1;
                }
                Ok(())
            })
            .unwrap()
    };
    assert_eq!(
        execution.output_rows,
        if count { 1 } else { expected.len() as u64 }
    );
    assert!(execution.native_io_certificate.is_certified());
    assert!(
        !execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
    let peak = memory.snapshot().peak_reserved_bytes;
    drop((execution, prepared));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let seconds = started.elapsed().as_secs_f64();
    serde_json::json!({
        "shape": shape, "consumer": if count { "count" } else { "projection" },
        "input_rows": ROWS, "selected_rows": expected.len(),
        "peak_reserved_bytes": peak, "reserved_after_cleanup": 0,
        "complete_lifecycle_seconds": seconds,
    })
}

#[test]
#[ignore = "frozen complete-operation resource/cost screen; use serial guarded release execution"]
fn native_selection_complete_cost_screen() {
    for shape in [
        "unfiltered",
        "all",
        "none",
        "band",
        "sparse",
        "dense",
        "nullable",
    ] {
        for count in [false, true] {
            for observation in 0..3 {
                let mut record = run(shape, count);
                record["observation"] = observation.into();
                println!("selection_cost={record}");
            }
        }
    }
}
