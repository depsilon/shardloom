//! Finite prepared-operator comparison; the same harness runs in both builds.

use super::*;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure as Measure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalFilter, VortexRelationalLimit,
        VortexRelationalNullOrder as NullOrder, VortexRelationalOrderKey, VortexRelationalProject,
        VortexRelationalSort,
    },
    resident_memory_source::{MemoryColumn, MemoryColumnValues, MemorySourceBounds},
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use shardloom_core::{BinaryOp, ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue};
use std::{io::Write as _, path::Path, time::Instant};

#[path = "local_primitive_relational_engine_cost_data.rs"]
mod data;
use data::{CASES, Data, Spec};

const GRANT: u64 = 128 << 20;

fn expression(kind: ExpressionKind) -> Expression {
    Expression::new(ExprId::new("engine-cost").unwrap(), kind)
}
fn column(name: &str) -> Expression {
    expression(ExpressionKind::Column(ColumnRef::new(name).unwrap()))
}
fn literal(value: i64) -> Expression {
    expression(ExpressionKind::Literal(ScalarValue::Int64(value)))
}
fn binary(left: Expression, op: BinaryOp, right: Expression) -> Expression {
    expression(ExpressionKind::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}
fn comparison(name: &str, op: ComparisonOp, value: i64) -> Expression {
    expression(ExpressionKind::Compare {
        left: Box::new(column(name)),
        op,
        right: Box::new(literal(value)),
    })
}
fn predicate(single: bool) -> Expression {
    let greater = comparison("n", ComparisonOp::Gt, 10);
    if single {
        return greater;
    }
    binary(
        binary(
            greater,
            BinaryOp::And,
            comparison("n", ComparisonOp::Lt, 100),
        ),
        BinaryOp::Or,
        binary(
            comparison("id", ComparisonOp::Lt, 77),
            BinaryOp::And,
            comparison("id", ComparisonOp::GtEq, 3),
        ),
    )
}
fn scan(uri: &DatasetUri) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: uri.clone(),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    })
}

impl Spec {
    #[allow(clippy::too_many_lines)] // One explicit table fixes every compared public plan before measurement.
    fn plan(self, left: &DatasetUri, right: &DatasetUri) -> VortexRelationalPlan {
        let input = scan(left);
        match self.case {
            "value_text" | "value_numeric" | "predicate_projected" => {
                let value = match self.case {
                    "value_text" => expression(ExpressionKind::FunctionCall {
                        name: "length".into(),
                        args: vec![column("s")],
                    }),
                    "value_numeric" => binary(column("n"), BinaryOp::Add, literal(3)),
                    _ => predicate(false),
                };
                VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
                    input,
                    expressions: vec![("id".into(), column("id")), ("value".into(), value)],
                }))
            }
            case if case.starts_with("aggregate_") => {
                let functions: &[(&str, Option<&str>, &str)] = match case {
                    "aggregate_text" => &[
                        ("min", Some("s"), "a"),
                        ("max", Some("s"), "b"),
                        ("count", Some("s"), "c"),
                        ("min", Some("s"), "d"),
                        ("max", Some("s"), "e"),
                        ("count", Some("s"), "f"),
                        ("count", None, "rows"),
                    ],
                    "aggregate_numeric" => &[
                        ("sum", Some("n"), "a"),
                        ("avg", Some("n"), "b"),
                        ("min", Some("n"), "c"),
                        ("max", Some("n"), "d"),
                        ("count", Some("n"), "e"),
                        ("sum", Some("n"), "f"),
                    ],
                    "aggregate_interleaved" => &[
                        ("min", Some("s"), "a"),
                        ("min", Some("n"), "b"),
                        ("max", Some("s"), "c"),
                        ("max", Some("n"), "d"),
                        ("count", Some("s"), "e"),
                        ("count", Some("n"), "f"),
                        ("count", None, "rows"),
                    ],
                    "aggregate_distinct" => &[
                        ("count_distinct", Some("s"), "a"),
                        ("count_distinct", Some("n"), "b"),
                    ],
                    _ => &[("count", None, "rows")],
                };
                VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
                    input,
                    group_by: vec![],
                    measures: functions
                        .iter()
                        .map(|(function, name, alias)| {
                            Measure::new(
                                *function,
                                name.map(|name| ColumnRef::new(name).unwrap()),
                                (*alias).into(),
                            )
                        })
                        .collect(),
                }))
            }
            "sort_many" | "sort_single" | "sort_limit" => {
                let sorted = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
                    input,
                    keys: [
                        ("n", false, NullOrder::First),
                        ("s", true, NullOrder::Last),
                        ("id", false, NullOrder::Last),
                    ]
                    .into_iter()
                    .map(|(name, descending, nulls)| VortexRelationalOrderKey {
                        column: ColumnRef::new(name).unwrap(),
                        descending,
                        nulls: Some(nulls),
                    })
                    .collect(),
                }));
                if self.case == "sort_limit" {
                    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                        input: sorted,
                        offset: 17,
                        count: 23,
                    }))
                } else {
                    sorted
                }
            }
            "predicate_filter" | "predicate_single" => {
                VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
                    input,
                    predicate: predicate(self.case == "predicate_single"),
                }))
            }
            case if case.starts_with("join_") => {
                VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
                    left: input,
                    right: scan(right),
                    kind: if case.starts_with("join_semi_") {
                        JoinKind::LeftSemi
                    } else {
                        JoinKind::LeftAnti
                    },
                    keys: vec![VortexRelationalJoinKey {
                        left: ColumnRef::new("n").unwrap(),
                        right: ColumnRef::new("n").unwrap(),
                    }],
                    condition: None,
                    columns: ["id", "n"]
                        .into_iter()
                        .map(|name| VortexRelationalJoinColumn {
                            side: Side::Left,
                            column: ColumnRef::new(name).unwrap(),
                            output_column: name.into(),
                        })
                        .collect(),
                }))
            }
            _ => panic!("frozen engine cost case"),
        }
    }
}

fn digest(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write as _;
        write!(value, "{byte:02x}").unwrap();
    }
    value
}
fn witness(root: &Path, value: &Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap();
    let identity = digest(&bytes);
    let path = root.join("witnesses").join(format!("{identity}.json"));
    if path.exists() {
        assert_eq!(fs::read(path).unwrap(), bytes);
    } else {
        fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .unwrap()
            .write_all(&bytes)
            .unwrap();
    }
    identity
}

#[allow(clippy::too_many_lines)] // Keep source, exact answers, timing and ownership in one operation.
fn run(root: &Path, spec: Spec) -> Value {
    let left = Data::new(spec, false);
    let right = spec.join().then(|| Data::new(spec, true));
    let (expected, schema) = left.expected(right.as_ref());
    let expected_witness = witness(root, &json!(expected));
    let schema_witness = witness(root, &serde_json::to_value(&schema).unwrap());
    let input_identity = left.input_identity();
    let right_identity = right.as_ref().map(Data::input_identity);
    let partitions = spec.partitions();
    let left_uri = DatasetUri::new("memory://engine-cost-left").unwrap();
    let right_uri = DatasetUri::new("memory://engine-cost-right").unwrap();
    let plan = spec.plan(&left_uri, &right_uri);
    let mut resources = policy();
    resources.resource_envelope.memory_budget_bytes = GRANT;
    let preparation_start = Instant::now();
    let prepared = prepare_relational_with_schema(resources, |schemas| {
        if spec.streaming() {
            schemas.register_batch_source(left_uri.clone(), |session| {
                left.source(session, 0..0, true)
            })?;
        } else {
            schemas.register_memory_source(left_uri.clone(), |session| {
                left.source(session, 0..spec.rows, false)
            })?;
        }
        if let Some(right) = &right {
            schemas.register_batch_source(right_uri.clone(), |session| {
                right.source(session, 0..0, true)
            })?;
        }
        Ok(plan)
    })
    .unwrap();
    let preparation_ns = preparation_start.elapsed().as_nanos();
    assert_eq!(prepared.output_dtype().as_ref(), Some(&schema));
    let memory = prepared.session.memory().clone();
    let reserved_before_execution = memory.snapshot().reserved_bytes;
    let mut delivered = [0_u64; 2];
    let mut completed = [false; 2];
    let mut input = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
        let (side, data) = if uri == &left_uri {
            (0, &left)
        } else {
            assert_eq!(uri, &right_uri);
            (1, right.as_ref().unwrap())
        };
        assert!(!completed[side]);
        assert_eq!(ordinal, delivered[side]);
        delivered[side] += 1;
        let Some(&(start, end)) = partitions.get(usize::try_from(ordinal).unwrap()) else {
            completed[side] = true;
            return Ok(None);
        };
        data.source(session, start..end, true).map(Some)
    };
    #[cfg(debug_assertions)]
    let observation = crate::local_primitives::native_relational_observation::Session::start();
    let operation_start = Instant::now();
    let result = if spec.streaming() {
        prepared
            .with_batch_inputs(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap()
    } else {
        prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap()
    };
    let operation_ns = operation_start.elapsed().as_nanos();
    #[cfg(debug_assertions)]
    let observations = serde_json::to_value(observation.finish()).unwrap();
    #[cfg(not(debug_assertions))]
    let observations = Value::Null;
    let actual = json_rows(&result);
    assert_eq!(actual, expected, "{} complete values", spec.case);
    let actual_schema = serde_json::from_str::<Value>(result.result_schema_json.value()).unwrap();
    assert_eq!(actual_schema, serde_json::to_value(&schema).unwrap());
    if spec.streaming() {
        assert!(completed[0]);
        assert_eq!(delivered[0], partitions.len() as u64 + 1);
        if spec.join() {
            assert!(completed[1]);
            assert_eq!(delivered[1], delivered[0]);
        }
    }
    let certificate = &result.execution.native_io_certificate;
    assert!(certificate.is_certified());
    assert!(!certificate.side_effects.fallback_attempted);
    let actual_witness = witness(root, &json!(actual));
    assert_eq!(actual_witness, expected_witness);
    let engine_ns = result.timing.execution.as_nanos();
    let serialization_ns = result.timing.result_serialization.as_nanos();
    let runtime = format!("{:#?}", result.execution.runtime);
    let certificate = format!("{certificate:#?}");
    drop(result);
    let reserved_after_output_release = memory.snapshot().reserved_bytes;
    assert_eq!(reserved_after_output_release, reserved_before_execution);
    let snapshot = prepared.snapshot();
    assert_eq!(snapshot.completed_executions, 1);
    assert_eq!(snapshot.admitted_max_parallelism, 1);
    assert_eq!(snapshot.provider_background_workers, 0);
    drop(prepared);
    let final_memory = memory.snapshot();
    assert_eq!(final_memory.reserved_bytes, 0);
    assert_eq!(final_memory.denied_reservations, 0);
    assert!(final_memory.peak_reserved_bytes <= GRANT);
    json!({"case":spec.case,"rows":spec.rows,"input":input_identity,"right_input":right_identity,
        "partitions":if spec.streaming() { json!(partitions) } else { Value::Null },
        "expected_rows_witness":expected_witness,"actual_rows_witness":actual_witness,"schema_witness":schema_witness,
        "engine_execution_ns":engine_ns,"result_serialization_ns":serialization_ns,
        "preparation_ns":preparation_ns,"execution_and_consumption_ns":operation_ns,
        "memory_grant_bytes":GRANT,"reserved_before_execution":reserved_before_execution,
        "reserved_after_output_release":reserved_after_output_release,"peak_reserved_bytes":final_memory.peak_reserved_bytes,
        "reserved_after_cleanup":0,"denied_reservations":0,"observations":observations,
        "observation_enabled":cfg!(debug_assertions),"runtime":runtime,"native_io_certificate":certificate,
        "producer_calls":delivered,"producer_completed":completed,"complete_values_match":true,"complete_schema_match":true})
}

#[test]
#[ignore = "frozen native engine decision screen; guarded serial execution only"]
fn native_engine_optimization_cost_screen() {
    let variable = |name| std::env::var(name).unwrap();
    let root = PathBuf::from(variable("SHARDLOOM_ENGINE_COST_ROOT"));
    assert!(root.is_absolute() && root.join("witnesses").is_dir());
    let requested = variable("SHARDLOOM_ENGINE_COST_CASE");
    let case = CASES
        .iter()
        .copied()
        .find(|case| *case == requested)
        .expect("frozen case");
    let rows = variable("SHARDLOOM_ENGINE_COST_ROWS")
        .parse::<usize>()
        .unwrap();
    assert!(matches!(rows, 257 | 65_536));
    assert!(case != "sort_single" || rows == 257);
    let role = variable("SHARDLOOM_ENGINE_COST_ROLE");
    assert!(matches!(role.as_str(), "baseline" | "candidate"));
    let sample = variable("SHARDLOOM_ENGINE_COST_SAMPLE")
        .parse::<usize>()
        .unwrap();
    assert!(sample <= 3);
    let mode = if cfg!(debug_assertions) {
        "observe"
    } else {
        "measure"
    };
    let path = root.join(format!("{mode}-{case}-{rows}-{role}-{sample}.json"));
    assert!(!path.exists(), "preserve each observation");
    let mut record = run(&root, Spec { case, rows });
    record["protocol"] = json!("native-engine-cost-v1");
    record["role"] = json!(role);
    record["sample"] = json!(sample);
    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .unwrap()
        .write_all(&serde_json::to_vec_pretty(&record).unwrap())
        .unwrap();
    println!("engine_cost_record={}", path.display());
}
