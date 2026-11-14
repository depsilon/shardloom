//! Declared COUNT feasibility observations through the shared prepared owner.
//! Debug work counts and release attribution are distinct, unscored observations.

use super::*;
use crate::{
    local_primitives::native_relational_grouped_cost::{self as cost, PHASES},
    query_primitive::VortexSimpleAggregateMeasure as Measure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalFilter, VortexRelationalLimit,
        VortexRelationalNullOrder, VortexRelationalOrderKey, VortexRelationalProject,
        VortexRelationalSort,
    },
    resident_memory_source::{MemoryBatchSourceBuilder, MemoryColumn, MemoryColumnValues},
    universal_format_io::demanded::LocalColumnarSource,
};
use serde_json::{Value, json};
use shardloom_core::{BinaryOp, ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue};
use vortex::array::dtype::Nullability;

// This test-only decoder accepts the frozen model's literal COUNT subset.
// It constructs ordinary public declarations; no input or aggregate work runs here.
fn expression(value: &Value) -> Expression {
    let args = value["args"].as_array().unwrap();
    let kind = match value["op"].as_str().unwrap() {
        "column" => {
            ExpressionKind::Column(ColumnRef::new(value["value"].as_str().unwrap()).unwrap())
        }
        "literal" => {
            assert_eq!(value["dtype"]["name"], "int64");
            ExpressionKind::Literal(ScalarValue::Int64(value["value"].as_i64().unwrap()))
        }
        "call" => {
            assert_eq!(value["value"], "coalesce");
            ExpressionKind::FunctionCall {
                name: "coalesce".into(),
                args: args.iter().map(expression).collect(),
            }
        }
        op @ ("add" | "and") => {
            assert_eq!(args.len(), 2);
            ExpressionKind::Binary {
                left: Box::new(expression(&args[0])),
                op: if op == "add" {
                    BinaryOp::Add
                } else {
                    BinaryOp::And
                },
                right: Box::new(expression(&args[1])),
            }
        }
        op @ ("lt" | "ge") => {
            assert_eq!(args.len(), 2);
            ExpressionKind::Compare {
                left: Box::new(expression(&args[0])),
                op: if op == "lt" {
                    ComparisonOp::Lt
                } else {
                    ComparisonOp::GtEq
                },
                right: Box::new(expression(&args[1])),
            }
        }
        _ => panic!("undeclared COUNT diagnostic expression"),
    };
    Expression::new(ExprId::new("direct-count-diagnostic").unwrap(), kind)
}

fn plan(node: &Value, uri: &DatasetUri) -> VortexRelationalPlan {
    let args = node["args"].as_array().unwrap();
    let inputs = node["inputs"].as_array().unwrap();
    if node["op"] == "scan" {
        assert_eq!(inputs.as_slice(), &[] as &[Value]);
        assert_eq!(args, &[json!("fact")]);
        return VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri.clone(),
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        });
    }
    assert_eq!(inputs.len(), 1);
    let input = plan(&inputs[0], uri);
    match node["op"].as_str().unwrap() {
        "filter" => {
            assert_eq!(args.len(), 1);
            VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
                input,
                predicate: expression(&args[0]),
            }))
        }
        "project" => VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
            input,
            expressions: args
                .iter()
                .map(|value| {
                    assert_eq!(value.as_array().unwrap().len(), 2);
                    (value[0].as_str().unwrap().into(), expression(&value[1]))
                })
                .collect(),
        })),
        "aggregate" => {
            assert_eq!(args.len(), 2);
            let measures = args[1].as_array().unwrap();
            assert_eq!(measures.len(), 1);
            let measure = &measures[0];
            assert_eq!(measure["function"], "count");
            assert_eq!(measure["argument"]["op"], "column");
            VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
                input,
                group_by: args[0]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|name| ColumnRef::new(name.as_str().unwrap()).unwrap())
                    .collect(),
                grouping_sets: None,
                measures: vec![Measure::new(
                    "count",
                    Some(ColumnRef::new(measure["argument"]["value"].as_str().unwrap()).unwrap()),
                    measure["alias"].as_str().unwrap().into(),
                )],
            }))
        }
        "sort" => VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
            input,
            keys: args
                .iter()
                .map(|key| VortexRelationalOrderKey {
                    column: ColumnRef::new(key["column"].as_str().unwrap()).unwrap(),
                    descending: key["descending"].as_bool().unwrap(),
                    nulls: Some(match key["nulls"].as_str().unwrap() {
                        "first" => VortexRelationalNullOrder::First,
                        "last" => VortexRelationalNullOrder::Last,
                        _ => panic!("undeclared null order"),
                    }),
                })
                .collect(),
        })),
        "limit" => {
            assert_eq!(args.len(), 2);
            VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                input,
                count: usize::try_from(args[0].as_u64().unwrap()).unwrap(),
                offset: usize::try_from(args[1].as_u64().unwrap()).unwrap(),
            }))
        }
        _ => panic!("undeclared COUNT diagnostic operator"),
    }
}

fn output_dtype(schema: &Value) -> DType {
    DType::struct_(
        schema
            .as_array()
            .unwrap()
            .iter()
            .map(|field| {
                let name = field[0].as_str().unwrap().to_owned();
                let ptype = match field[1]["name"].as_str().unwrap() {
                    "int64" => PType::I64,
                    "uint64" => PType::U64,
                    _ => panic!("undeclared COUNT output dtype"),
                };
                let nullability = if field[1]["nullable"].as_bool().unwrap() {
                    Nullability::Nullable
                } else {
                    Nullability::NonNullable
                };
                (name, DType::Primitive(ptype, nullability))
            })
            .collect::<Vec<_>>(),
        Nullability::NonNullable,
    )
}

struct Input {
    ids: Vec<i64>,
    keys: Vec<i64>,
    nullable_keys: Vec<Option<i64>>,
    values: Vec<i64>,
    nullable_values: Vec<Option<i64>>,
    nullable_key: bool,
    nullable_value: bool,
}

impl Input {
    fn new(job: &Value) -> Self {
        let rows = job["rows"].as_u64().unwrap();
        let domain = job["domain"].as_u64().unwrap();
        assert!(rows <= 131_073 && domain > 0);
        let ids: Vec<_> = (0..rows).map(|row| i64::try_from(row).unwrap()).collect();
        let keys: Vec<_> = (0..rows)
            .map(|row| i64::try_from((row * 37 + 11) % domain).unwrap())
            .collect();
        let values: Vec<_> = ids.iter().map(|row| row % 19 - 9).collect();
        Self {
            nullable_keys: keys
                .iter()
                .enumerate()
                .map(|(row, &key)| (!row.is_multiple_of(13)).then_some(key))
                .collect(),
            nullable_values: values
                .iter()
                .enumerate()
                .map(|(row, &value)| (!row.is_multiple_of(7)).then_some(value))
                .collect(),
            ids,
            keys,
            values,
            nullable_key: job["nullable_key"].as_bool().unwrap(),
            nullable_value: job["nullable_value"].as_bool().unwrap(),
        }
    }

    fn columns(&self, range: std::ops::Range<usize>) -> [MemoryColumn<'_>; 3] {
        [
            MemoryColumn {
                name: "row_id",
                values: MemoryColumnValues::Int64NonNullable(&self.ids[range.clone()]),
            },
            MemoryColumn {
                name: "g",
                values: if self.nullable_key {
                    MemoryColumnValues::Int64(&self.nullable_keys[range.clone()])
                } else {
                    MemoryColumnValues::Int64NonNullable(&self.keys[range.clone()])
                },
            },
            MemoryColumn {
                name: "n",
                values: if self.nullable_value {
                    MemoryColumnValues::Int64(&self.nullable_values[range])
                } else {
                    MemoryColumnValues::Int64NonNullable(&self.values[range])
                },
            },
        ]
    }

    fn batch(
        &self,
        session: &ResidentVortexSession,
        range: std::ops::Range<usize>,
    ) -> Result<ResidentMemorySource> {
        assert!(range.len() <= 2048);
        ResidentMemorySource::from_batch_columns(session, &self.columns(range))
    }

    fn memory(&self, session: &ResidentVortexSession) -> Result<ResidentMemorySource> {
        // This control explicitly declares a resident source. Its bounded typed
        // intake uses the existing builder before the observed query begins.
        let mut builder = MemoryBatchSourceBuilder::new(session, CancellationToken::default())?;
        for start in (0..self.ids.len().max(1)).step_by(2048) {
            let end = (start + 2048).min(self.ids.len());
            builder.push_columns(&self.columns(start..end))?;
        }
        builder.finish()
    }
}

#[test]
#[ignore = "frozen direct COUNT feasibility; guarded isolated debug/release execution only"]
#[allow(clippy::too_many_lines)] // A complete request owns its observation, result, source and refunds.
fn native_direct_count_attribution() {
    let path = PathBuf::from(std::env::var_os("SHARDLOOM_DIRECT_COUNT_JOB").expect("declared job"));
    assert!(path.is_absolute() && fs::metadata(&path).unwrap().len() <= 256 * 1024);
    let job: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(
        job["schema_version"],
        "shardloom.direct_count_diagnostic_job.v2"
    );
    let format = job["source"]["format"].as_str().unwrap();
    let input = matches!(format, "memory" | "batch").then(|| Input::new(&job["source"]));
    let uri = DatasetUri::new(if input.is_some() {
        "memory://direct-count-diagnostic"
    } else {
        job["source"]["path"].as_str().unwrap()
    })
    .unwrap();
    let declaration = plan(&job["relation"], &uri);
    let expected_dtype = output_dtype(&job["relation"]["schema"]);
    let resources = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 2).unwrap();
    let prepared = prepare_relational_with_schema(resources, |preparation| {
        match format {
            "memory" => preparation.register_memory_source(uri.clone(), |session| {
                input.as_ref().unwrap().memory(session)
            })?,
            "batch" => preparation.register_batch_source(uri.clone(), |session| {
                input.as_ref().unwrap().batch(session, 0..0)
            })?,
            "vortex" => {}
            "parquet" | "arrow-ipc" => {
                let source = PathBuf::from(job["source"]["path"].as_str().unwrap());
                assert!(source.is_absolute() && source.is_file());
                let admitted = LocalColumnarSource::admit(
                    &source,
                    format,
                    &preparation.shared_memory_pool(),
                    &preparation.cancellation_token(),
                )?;
                preparation.register_local_source(uri.clone(), admitted)?;
            }
            _ => panic!("undeclared diagnostic source"),
        }
        Ok(declaration)
    })
    .unwrap();
    // The unified compiler may embed file kernels below an ordinary operator.
    // Root admission is not an inference about every aggregate in the tree.
    let root_file_kernel = prepared.root_has_file_kernel();
    assert_eq!(prepared.output_dtype().as_ref(), Some(&expected_dtype));
    let memory = prepared.session.memory().clone();
    let initial = memory.snapshot().reserved_bytes;
    let mut consumed = 0;
    let mut calls = 0;
    let mut ended = false;
    let mut producer = |session: &ResidentVortexSession| {
        assert!(!ended, "input was restarted after completion");
        calls += 1;
        let data = input.as_ref().unwrap();
        if consumed == data.ids.len() {
            ended = true;
            return Ok(None);
        }
        let batch_rows = usize::try_from(job["source"]["batch_rows"].as_u64().unwrap()).unwrap();
        assert!((1..=2048).contains(&batch_rows));
        let end = (consumed + batch_rows).min(data.ids.len());
        let batch = data.batch(session, consumed..end)?;
        consumed = end;
        Ok(Some(batch))
    };
    #[cfg(debug_assertions)]
    let work = crate::local_primitives::native_relational_observation::Session::start();
    let observation = cost::Session::start();
    let result = if format == "batch" {
        prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap()
    } else {
        prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap()
    };
    let phases = observation.finish();
    #[cfg(debug_assertions)]
    let counts = serde_json::to_value(work.finish()).unwrap();
    #[cfg(not(debug_assertions))]
    let counts = Value::Null;
    if format == "batch" {
        assert!(ended);
        assert_eq!(consumed, input.as_ref().unwrap().ids.len());
    }
    let actual = json_rows(&result);
    assert_eq!(json!(actual), job["expected_rows"], "complete ordered rows");
    let schema: Value = serde_json::from_str(result.result_schema_json.value()).unwrap();
    assert_eq!(schema, serde_json::to_value(&expected_dtype).unwrap());
    let certificate = &result.execution.native_io_certificate;
    let provider_selection_reason = result.execution.provider_selection_reason;
    let partial_work = certificate.count_partial_work;
    assert!(certificate.is_certified() && !certificate.side_effects.fallback_attempted);
    let engine_ns = result.timing.execution.as_nanos();
    let classified_ns: u128 = phases.nanos[..9].iter().sum();
    assert!(classified_ns <= engine_ns);
    let source_report = result
        .execution
        .input
        .as_ref()
        .map_or(Value::Null, |input| {
            assert!(input.end_of_input_observed && input.output_ownership_detached);
            json!({
                "columnar_sources":input.columnar_sources,
                "columnar_source_formats":input.columnar_source_formats,
                "projected_columns":input.columnar_projected_columns,
                "rows_decoded":input.columnar_rows_decoded,
                "data_read_bytes":input.columnar_data_read_bytes,
                "data_read_requests":input.columnar_data_read_requests,
                "sources":input.sources, "payload_batches":input.payload_batches,
                "rows":input.rows, "max_batch_rows":input.max_batch_rows,
                "end_of_input_observed":input.end_of_input_observed,
                "output_ownership_detached":input.output_ownership_detached,
            })
        });
    let scan_rows_delivered = result.execution.scan_rows_delivered;
    drop(result);
    let after_result = memory.snapshot().reserved_bytes;
    // A stored Vortex source retains its shared footer and lazily parsed DTypes.
    // Measure that owner's lifetime separately; a first execution need not leave
    // its preparation reservation unchanged. Other source paths refund exactly.
    if format != "vortex" {
        assert_eq!(after_result, initial);
    }
    assert_eq!(prepared.snapshot().completed_executions, 1);
    let retained_sources = prepared.sources.clone();
    let retained_source_count = retained_sources.len();
    drop(prepared);
    let after_prepared = memory.snapshot().reserved_bytes;
    drop(retained_sources);
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
        "DIRECT_COUNT {}",
        json!({
            "schema_version":"shardloom.direct_count_diagnostic_observation.v2",
            "job":job, "debug_work_observation":cfg!(debug_assertions),
            "root_file_kernel":root_file_kernel,
            "count_partial_work": {
                "considered_batches": partial_work.considered_batches,
                "admitted_batches": partial_work.admitted_batches,
                "input_rows": partial_work.input_rows,
                "reduced_groups": partial_work.reduced_groups,
                "constant_batches": partial_work.constant_batches,
                "declined_unique_batches": partial_work.declined_unique_batches,
                "declined_capacity_batches": partial_work.declined_capacity_batches,
                "partial_capacity_bytes": partial_work.partial_capacity_bytes,
            },
            "provider_selection_reason":provider_selection_reason,
            "actual_rows":actual, "actual_schema":schema, "counts":counts,
            "engine_execution_ns":engine_ns, "classified_engine_ns":classified_ns,
            "unclassified_engine_ns":engine_ns-classified_ns, "phases":phases,
        "source":source_report, "scan_rows_delivered":scan_rows_delivered,
        "producer_calls":calls, "producer_ended":ended,
            "producer_consumed_rows":consumed, "native_io_certified":true,
            "fallback_attempted":false, "external_engine_invoked":false,
            "initial_reserved_bytes":initial, "after_result_reserved_bytes":after_result,
            "after_prepared_reserved_bytes":after_prepared,
            "retained_source_count":retained_source_count,
            "memory_grant_bytes":2_u64 << 30, "peak_reserved_bytes":final_memory.peak_reserved_bytes,
            "final_reserved_bytes":0, "denied_reservations":0,
            "scope":"instrumented shared native COUNT feasibility; no production performance claim",
        })
    );
}
