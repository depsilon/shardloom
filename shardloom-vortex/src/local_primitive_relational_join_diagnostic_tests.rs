//! Frozen, test-only resident join observations through the ordinary typed owner.

use super::*;
use crate::{
    local_primitives::native_relational_join_cost::{self as cost, ENGINE_PHASES, PHASES},
    query_primitive::VortexSimpleAggregateMeasure as Measure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalFilter, VortexRelationalLimit,
        VortexRelationalNullOrder, VortexRelationalOrderKey, VortexRelationalSort,
    },
    universal_format_io::demanded::LocalColumnarSource,
};
use serde_json::{Value, json};
use shardloom_core::{BinaryOp, ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue};
use std::collections::BTreeMap;
use vortex::array::dtype::Nullability;

// The frozen model's declared subset, not a public plan importer or SQL parser.
// Inputs and aggregate work remain in the existing prepared/native execution.
fn expression(value: &Value) -> Expression {
    let args = value["args"].as_array().unwrap();
    let kind = match value["op"].as_str().unwrap() {
        "column" => {
            assert_eq!(args.as_slice(), &[] as &[Value]);
            ExpressionKind::Column(ColumnRef::new(value["value"].as_str().unwrap()).unwrap())
        }
        "literal" => {
            assert_eq!(args.as_slice(), &[] as &[Value]);
            ExpressionKind::Literal(match value["dtype"]["name"].as_str().unwrap() {
                "int64" => ScalarValue::Int64(value["value"].as_i64().unwrap()),
                "bool" => ScalarValue::Boolean(value["value"].as_bool().unwrap()),
                _ => panic!("undeclared join diagnostic literal"),
            })
        }
        op @ ("and" | "mod") => {
            assert_eq!(args.len(), 2);
            ExpressionKind::Binary {
                left: Box::new(expression(&args[0])),
                op: if op == "and" {
                    BinaryOp::And
                } else {
                    BinaryOp::Remainder
                },
                right: Box::new(expression(&args[1])),
            }
        }
        op @ ("eq" | "lt" | "ge") => {
            assert_eq!(args.len(), 2);
            ExpressionKind::Compare {
                left: Box::new(expression(&args[0])),
                op: match op {
                    "eq" => ComparisonOp::Eq,
                    "lt" => ComparisonOp::Lt,
                    _ => ComparisonOp::GtEq,
                },
                right: Box::new(expression(&args[1])),
            }
        }
        _ => panic!("undeclared join diagnostic expression"),
    };
    Expression::new(ExprId::new("resident-join-diagnostic").unwrap(), kind)
}

fn join_plan(
    args: &[Value],
    inputs: &[Value],
    sources: &BTreeMap<String, DatasetUri>,
) -> VortexRelationalPlan {
    assert_eq!(inputs.len(), 2);
    assert_eq!(args.len(), 3);
    let keys: Vec<_> = args[0]
        .as_array()
        .unwrap()
        .iter()
        .map(|pair| {
            assert_eq!(pair.as_array().unwrap().len(), 2);
            VortexRelationalJoinKey {
                left: ColumnRef::new(pair[0].as_str().unwrap()).unwrap(),
                right: ColumnRef::new(pair[1].as_str().unwrap()).unwrap(),
            }
        })
        .collect();
    assert!(!keys.is_empty(), "declared diagnostic requires equi keys");
    VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: plan(&inputs[0], sources),
        right: plan(&inputs[1], sources),
        kind: match args[2].as_str().unwrap() {
            "inner" => JoinKind::Inner,
            "left" => JoinKind::Left,
            "right" => JoinKind::Right,
            "full" => JoinKind::Full,
            "semi" => JoinKind::LeftSemi,
            "anti" => JoinKind::LeftAnti,
            _ => panic!("undeclared join diagnostic kind"),
        },
        condition: None,
        keys,
        columns: args[1]
            .as_array()
            .unwrap()
            .iter()
            .map(|column| {
                assert_eq!(column.as_array().unwrap().len(), 3);
                VortexRelationalJoinColumn {
                    side: match column[0].as_str().unwrap() {
                        "left" => Side::Left,
                        "right" => Side::Right,
                        _ => panic!("undeclared join diagnostic side"),
                    },
                    column: ColumnRef::new(column[1].as_str().unwrap()).unwrap(),
                    output_column: column[2].as_str().unwrap().into(),
                }
            })
            .collect(),
    }))
}

fn plan(node: &Value, sources: &BTreeMap<String, DatasetUri>) -> VortexRelationalPlan {
    let args = node["args"].as_array().unwrap();
    let inputs = node["inputs"].as_array().unwrap();
    if node["op"] == "scan" {
        assert_eq!(inputs.as_slice(), &[] as &[Value]);
        assert_eq!(args.len(), 1);
        return VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: sources[args[0].as_str().unwrap()].clone(),
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        });
    }
    if node["op"] == "join" {
        return join_plan(args, inputs, sources);
    }
    assert_eq!(inputs.len(), 1);
    let input = plan(&inputs[0], sources);
    match node["op"].as_str().unwrap() {
        "filter" => {
            assert_eq!(args.len(), 1);
            VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
                input,
                predicate: expression(&args[0]),
            }))
        }
        "aggregate" => {
            assert_eq!(args.len(), 2);
            VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
                input,
                group_by: args[0]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|name| ColumnRef::new(name.as_str().unwrap()).unwrap())
                    .collect(),
                grouping_sets: None,
                measures: args[1]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|measure| {
                        let function = measure["function"].as_str().unwrap();
                        assert!(
                            ["count", "count_distinct", "sum", "min", "max", "avg"]
                                .contains(&function)
                        );
                        let argument = &measure["argument"];
                        let column = if argument.is_null() {
                            None
                        } else {
                            assert_eq!(argument["op"], "column");
                            Some(ColumnRef::new(argument["value"].as_str().unwrap()).unwrap())
                        };
                        Measure::new(function, column, measure["alias"].as_str().unwrap().into())
                    })
                    .collect(),
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
                        _ => panic!("undeclared join diagnostic null order"),
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
        _ => panic!("undeclared join diagnostic operator"),
    }
}

fn output_dtype(schema: &Value) -> DType {
    DType::struct_(
        schema
            .as_array()
            .unwrap()
            .iter()
            .map(|field| {
                assert_eq!(
                    field[1]["parameters"].as_array().unwrap().as_slice(),
                    &[] as &[Value]
                );
                let nullability = if field[1]["nullable"].as_bool().unwrap() {
                    Nullability::Nullable
                } else {
                    Nullability::NonNullable
                };
                let dtype = match field[1]["name"].as_str().unwrap() {
                    "int64" => DType::Primitive(PType::I64, nullability),
                    "uint64" => DType::Primitive(PType::U64, nullability),
                    "float64" => DType::Primitive(PType::F64, nullability),
                    "bool" => DType::Bool(nullability),
                    "utf8" => DType::Utf8(nullability),
                    _ => panic!("undeclared join diagnostic output dtype"),
                };
                (field[0].as_str().unwrap().to_owned(), dtype)
            })
            .collect::<Vec<_>>(),
        Nullability::NonNullable,
    )
}

fn phases(nanos: &[u128; 16], calls: &[u64; 16]) -> Vec<Value> {
    PHASES
        .iter()
        .enumerate()
        .map(|(index, name)| {
            json!({
                "phase":name, "exclusive_nanos":nanos[index], "calls":calls[index],
                "engine_classified":index < ENGINE_PHASES,
            })
        })
        .collect()
}

fn source_report(result: &CollectedVortexRelational) -> Value {
    let input = result.execution.input.as_ref().map_or(Value::Null, |input| {
        assert!(input.end_of_input_observed && input.output_ownership_detached);
        json!({
            "columnar_sources":input.columnar_sources, "columnar_source_formats":input.columnar_source_formats,
            "projected_columns":input.columnar_projected_columns, "rows_decoded":input.columnar_rows_decoded,
            "data_read_bytes":input.columnar_data_read_bytes, "data_read_requests":input.columnar_data_read_requests,
            "sources":input.sources, "payload_batches":input.payload_batches, "rows":input.rows,
            "max_batch_rows":input.max_batch_rows, "end_of_input_observed":input.end_of_input_observed,
            "output_ownership_detached":input.output_ownership_detached,
        })
    });
    let scans = &result.execution.native_io_certificate.file_scans;
    let sources: Vec<_> = scans.sources.iter().map(|source| json!({
        "source_uri":source.source_uri, "generation_fingerprint":source.generation_fingerprint,
        "segment_requests":source.segment_requests, "completed_read_requests":source.completed_read_requests,
        "completed_read_bytes":source.completed_read_bytes,
    })).collect();
    let contracts: Vec<_> = scans.contracts.iter().map(|contract| json!({
        "source":contract.source, "route":contract.route.as_str(), "source_rows":contract.source_rows,
        "available_columns":contract.available_columns, "projected_columns":contract.projected_columns,
        "output_columns":contract.output_columns, "pushed_predicate_columns":contract.pushed_predicate_columns,
        "residual_predicate_columns":contract.residual_predicate_columns, "invocations":contract.invocations,
        "provider_scan_invocations":contract.provider_scan_invocations, "delivered_rows":contract.delivered_rows,
        "whole_file_pruning_checks":contract.whole_file_pruning_checks,
        "whole_file_exclusions":contract.whole_file_exclusions,
    })).collect();
    json!({"batch_input":input, "native_file_sources":sources, "native_file_contracts":contracts,
           "prepared_sources":result.execution.prepared_sources,
           "prepared_file_sources":result.execution.prepared_file_sources,
           "completed_executions":result.execution.runtime.completed_executions})
}

fn result_rows_jsonl(result: &CollectedVortexRelational) -> String {
    // Parsing into serde_json::Value and re-emitting can change an f64 by an
    // ULP without float_roundtrip. Preserve the engine's bytes for the external
    // result codec, which checks the unchanged exact or declared float policy.
    result.result_jsonl.value().to_owned()
}

#[test]
#[ignore = "frozen resident joins; guarded release attribution or debug work inventory only"]
#[allow(clippy::too_many_lines)] // Keep the complete result and source/owner teardown in one observation.
#[allow(clippy::assertions_on_constants)] // Compilation in debug is allowed; timed diagnostic execution is not.
fn native_resident_join_attribution() {
    let work_only = std::env::var_os("SHARDLOOM_RESIDENT_JOIN_WORK_ONLY").is_some();
    assert_eq!(
        cfg!(debug_assertions),
        work_only,
        "release attribution or explicitly enabled debug work inventory required"
    );
    let path = PathBuf::from(std::env::var_os("SHARDLOOM_RESIDENT_JOIN_JOB").expect("frozen job"));
    assert!(path.is_absolute() && fs::metadata(&path).unwrap().len() <= 1024 * 1024);
    let job: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(job["schema"], "shardloom.resident_join_diagnostic_job.v1");
    let format = job["input_format"].as_str().unwrap();
    assert!(["vortex", "parquet", "arrow-ipc"].contains(&format));
    let sources: BTreeMap<_, _> = job["sources"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, value)| {
            let path = PathBuf::from(value.as_str().unwrap());
            assert!(path.is_absolute() && path.is_file());
            (
                name.clone(),
                DatasetUri::new(path.to_str().unwrap()).unwrap(),
            )
        })
        .collect();
    let declaration = plan(&job["relation"], &sources);
    let expected_dtype = output_dtype(&job["relation"]["schema"]);
    let prepared = prepare_relational_with_schema(
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 2).unwrap(),
        |preparation| {
            if format != "vortex" {
                for uri in sources.values() {
                    let source = PathBuf::from(uri.as_str());
                    let admitted = LocalColumnarSource::admit(
                        &source,
                        format,
                        &preparation.shared_memory_pool(),
                        &preparation.cancellation_token(),
                    )?;
                    preparation.register_local_source(uri.clone(), admitted)?;
                }
            }
            Ok(declaration)
        },
    )
    .unwrap();
    assert!(!prepared.root_has_file_kernel());
    assert_eq!(prepared.output_dtype().as_ref(), Some(&expected_dtype));
    let memory = prepared.session.memory().clone();
    let initial = memory.snapshot().reserved_bytes;
    let observation = cost::Session::start();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let report = observation.finish();
    let engine_ns = result.timing.execution.as_nanos();
    let nodes: Vec<_> = report.nodes[..report.node_count].iter().enumerate().map(|(index, node)| {
        let work = &report.payload_work[index];
        json!({
        "node":index, "parent_attribution_context":node.parent, "kind":node.kind,
        "key_columns":node.key_columns, "membership_only":node.membership_only,
        "conditional":node.conditional, "phases":phases(&node.nanos, &node.calls),
        "build_batches":node.build_batches, "build_rows":node.build_rows,
        "probe_batches":node.probe_batches, "probe_rows":node.probe_rows,
        "output_batches":node.output_batches, "output_rows":node.output_rows,
        "output_cells":node.output_cells, "validation_only_cells":node.validation_only_cells,
        "payload_work":{
            "coordinate_batches":work.coordinate_batches,
            "coordinate_rows":work.coordinate_rows,
            "coordinate_chunks":work.coordinate_chunks,
            "direct_numeric_batches":work.direct_numeric_batches,
            "direct_numeric_rows":work.direct_numeric_rows,
            "direct_numeric_chunks":work.direct_numeric_chunks,
            "direct_numeric_single_chunk_rows":work.direct_numeric_single_chunk_rows,
            "selected_numeric_copy_batches":work.selected_numeric_copy_batches,
            "selected_numeric_copy_rows":work.selected_numeric_copy_rows,
            "variable_copy_rows":work.variable_copy_rows,
            "scalar_copy_rows":work.scalar_copy_rows,
            "dictionary_copy_rows":work.dictionary_copy_rows,
            "dictionary_copy_values":work.dictionary_copy_values,
        },
    })}).collect();
    let classified_ns: u128 = report.nodes[..report.node_count]
        .iter()
        .map(|node| node.nanos[..ENGINE_PHASES].iter().sum::<u128>())
        .sum();
    assert!(classified_ns <= engine_ns);
    let certificate = &result.execution.native_io_certificate;
    assert!(certificate.is_certified() && !certificate.side_effects.fallback_attempted);
    let filter = certificate.join_work.runtime_filter;
    let skew = certificate.join_work.skew;
    assert_eq!(
        certificate.join_work.cyclic.executions, 0,
        "declared resident diagnostic"
    );
    assert_eq!(
        report.node_count,
        job["expected_join_nodes"].as_array().unwrap().len()
    );
    for phase in [
        cost::Phase::OrderedExcluded,
        cost::Phase::AsOfExcluded,
        cost::Phase::CyclicExcluded,
    ] {
        assert_eq!(
            report.excluded_calls[phase as usize], 0,
            "unexpected non-resident join"
        );
    }
    let source = source_report(&result);
    if format == "vortex" {
        assert!(source["batch_input"].is_null());
        assert_eq!(certificate.file_scans.sources.len(), sources.len());
    } else {
        assert!(source["batch_input"].is_object());
        assert_eq!(certificate.file_scans.sources.len(), 0);
    }
    let actual_jsonl = result_rows_jsonl(&result);
    let schema: Value = serde_json::from_str(result.result_schema_json.value()).unwrap();
    assert_eq!(schema, serde_json::to_value(&expected_dtype).unwrap());
    let scan_rows_delivered = result.execution.scan_rows_delivered;
    drop(result);
    let after_result = memory.snapshot().reserved_bytes;
    if format != "vortex" {
        assert_eq!(after_result, initial);
    }
    assert_eq!(prepared.snapshot().completed_executions, 1);
    let retained_sources = prepared.sources.clone();
    drop(prepared);
    let after_prepared = memory.snapshot().reserved_bytes;
    drop(retained_sources);
    let final_memory = memory.snapshot();
    assert_eq!(final_memory.reserved_bytes, 0);
    assert_eq!(final_memory.denied_reservations, 0);
    assert!(final_memory.peak_reserved_bytes <= 2 << 30);
    println!(
        "RESIDENT_JOIN {}",
        json!({
            "schema_version":"shardloom.resident_join_diagnostic_observation.v2",
            "job":job, "debug_work_observation":cfg!(debug_assertions),
            "work_inventory_only":work_only,
            "actual_jsonl":actual_jsonl, "actual_schema":schema, "nodes":nodes,
            "excluded_phases":phases(&report.excluded_nanos, &report.excluded_calls),
            "engine_execution_ns":engine_ns, "classified_engine_ns":classified_ns,
            "unclassified_engine_ns":engine_ns-classified_ns,
            "runtime_filter":{
                "considered":filter.considered, "admitted":filter.admitted,
                "declined_small":filter.declined_small, "declined_memory":filter.declined_memory,
                "build_keys":filter.build_keys, "retained_bytes":filter.retained_bytes,
                "probe_rows":filter.probe_rows, "rejected_rows":filter.rejected_rows,
                "index_lookups_avoided":filter.index_lookups_avoided,
            },
            "skew":{
                "considered":skew.considered, "hot_groups":skew.hot_groups, "hot_rows":skew.hot_rows,
                "admitted_groups":skew.admitted_groups, "admitted_rows":skew.admitted_rows,
                "declined_memory":skew.declined_memory, "range_visits":skew.range_visits,
                "rows_visited":skew.rows_visited,
            },
            "source":source, "scan_rows_delivered":scan_rows_delivered,
            "native_io_certified":true, "fallback_attempted":false, "external_engine_invoked":false,
            "initial_reserved_bytes":initial, "after_result_reserved_bytes":after_result,
            "after_prepared_reserved_bytes":after_prepared,
            "memory_grant_bytes":2_u64 << 30, "max_parallelism":2,
            "peak_reserved_bytes":final_memory.peak_reserved_bytes,
            "final_reserved_bytes":0, "denied_reservations":0,
            "scope":"instrumented caller-thread attribution; no production speedup or admission claim",
        })
    );
}

#[test]
fn join_cost_declaration_decoder_executes_outer_nulls_and_explicit_projection() {
    let left = Fixture::new(keyed(&[Some(2), None, Some(1)], &[10, 11, 12]), 1);
    let right = Fixture::new(keyed(&[Some(2), Some(2), Some(3)], &[20, 21, 22]), 1);
    let sources = BTreeMap::from([
        (
            "l".into(),
            DatasetUri::new(left.path().display().to_string()).unwrap(),
        ),
        (
            "r".into(),
            DatasetUri::new(right.path().display().to_string()).unwrap(),
        ),
    ]);
    let node = json!({"op":"join", "args":[[["entity","entity"]],
        [["left","amount","debit"],["right","amount","credit"]],"full"],
        "inputs":[{"op":"scan","args":["l"],"inputs":[]},{"op":"scan","args":["r"],"inputs":[]}]});
    let prepared = prepare_relational(&plan(&node, &sources), policy()).unwrap();
    let memory = prepared.session.memory().clone();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let source = source_report(&result);
    assert!(source["batch_input"].is_null());
    assert_eq!(source["native_file_sources"].as_array().unwrap().len(), 2);
    assert_eq!(source["native_file_contracts"].as_array().unwrap().len(), 2);
    assert_eq!(source["prepared_sources"], 2);
    assert_eq!(source["completed_executions"], 1);
    assert_eq!(
        json!(json_rows(&result)),
        json!([
            {"debit":10,"credit":20},{"debit":10,"credit":21},{"debit":11,"credit":null},
            {"debit":12,"credit":null},{"debit":null,"credit":22},
        ])
    );
    drop(result);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn join_cost_result_transport_preserves_native_f64_text_exactly() {
    let value = f64::from_bits(0x3fde_af87_2ba4_a3e9);
    let input = StructArray::try_new(
        FieldNames::from(["average"]),
        vec![PrimitiveArray::from_iter([value]).into_array()],
        1,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let fixture = Fixture::new(input, 1);
    let prepared = prepare_relational(&fixture.scan(), policy()).unwrap();
    let memory = prepared.session.memory().clone();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let native_text = result_rows_jsonl(&result);
    assert_eq!(native_text.trim(), "{\"average\":0.47946337949775725}");
    let transported = serde_json::to_string(&json!({"actual_jsonl":native_text})).unwrap();
    let restored: Value = serde_json::from_str(&transported).unwrap();
    assert_eq!(
        restored["actual_jsonl"].as_str().unwrap(),
        result.result_jsonl.value()
    );
    drop(result);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
