//! Frozen bounded-local-index comparison, protocol compact-local-indices-762-v1.

use super::*;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::{VortexRelationalAggregate, VortexRelationalFilter},
    resident_memory_source::{MemoryColumn, MemoryColumnValues, MemorySourceBounds},
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use shardloom_core::{ExprId, Expression, ExpressionKind};
use std::{io::Write as _, path::Path, time::Instant};

const SHAPES: [&str; 7] = [
    "unfiltered",
    "all",
    "none",
    "band",
    "sparse",
    "dense",
    "nullable",
];

#[derive(Clone, Copy)]
struct Spec {
    rows: usize,
    shape: &'static str,
    count: bool,
    calls: usize,
}

impl Spec {
    fn consumer(self) -> &'static str {
        if self.count { "count" } else { "projection" }
    }

    fn id(self) -> String {
        format!(
            "{}-{}-{}-{}",
            self.rows,
            self.shape,
            self.consumer(),
            self.calls
        )
    }

    fn keep(self, row: usize) -> Option<bool> {
        match self.shape {
            "unfiltered" | "all" => Some(true),
            "none" => Some(false),
            "band" => Some((1031..13_019).contains(&row)),
            "sparse" => Some(row % 37 == 3),
            "dense" => Some(row % 37 != 3),
            "nullable" => (!row.is_multiple_of(17)).then_some(!row.is_multiple_of(3)),
            _ => panic!("unknown frozen selection shape"),
        }
    }

    fn source_dtype() -> DType {
        DType::struct_(
            vec![
                (
                    "n".to_owned(),
                    DType::Primitive(PType::I64, Nullability::Nullable),
                ),
                ("keep".to_owned(), DType::Bool(Nullability::Nullable)),
            ],
            Nullability::NonNullable,
        )
    }

    fn output_dtype(self) -> DType {
        if self.count {
            DType::struct_(
                vec![(
                    "rows".to_owned(),
                    DType::Primitive(PType::U64, Nullability::NonNullable),
                )],
                Nullability::NonNullable,
            )
        } else {
            Self::source_dtype()
        }
    }

    fn plan(self, uri: DatasetUri) -> VortexRelationalPlan {
        let mut plan = VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri,
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        });
        if self.shape != "unfiltered" {
            plan = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
                input: plan,
                predicate: Expression::new(
                    ExprId::new("keep").unwrap(),
                    ExpressionKind::Column(ColumnRef::new("keep").unwrap()),
                ),
            }));
        }
        if self.count {
            plan = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
                input: plan,
                group_by: vec![],
                grouping_sets: None,
                measures: vec![VortexSimpleAggregateMeasure::new(
                    "count",
                    None,
                    "rows".into(),
                )],
            }));
        }
        plan
    }
}

fn witness(root: &Path, value: &Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap();
    let mut identity = String::with_capacity(64);
    for byte in Sha256::digest(&bytes) {
        use std::fmt::Write as _;
        write!(identity, "{byte:02x}").unwrap();
    }
    let path = root.join("witnesses").join(format!("{identity}.json"));
    if path.exists() {
        assert_eq!(fs::read(&path).unwrap(), bytes);
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

fn matrix(rows: usize) -> Vec<Spec> {
    let mut result = Vec::new();
    for shape in SHAPES {
        for count in [false, true] {
            for calls in [1, 4] {
                result.push(Spec {
                    rows,
                    shape,
                    count,
                    calls,
                });
            }
        }
    }
    result
}

#[allow(clippy::too_many_lines)] // Freeze the complete lifecycle and exact witnesses together.
fn workflow(
    root: &Path,
    spec: Spec,
    role: &str,
    cohort: &str,
    sample: usize,
    warmup: bool,
) -> Value {
    let values = (0..spec.rows)
        .map(|row| (!row.is_multiple_of(11)).then(|| i64::try_from(row).unwrap() * 17 - 5))
        .collect::<Vec<_>>();
    let predicate = (0..spec.rows).map(|row| spec.keep(row)).collect::<Vec<_>>();
    let input = (0..spec.rows)
        .map(|row| json!({"n":values[row], "keep":predicate[row]}))
        .collect::<Vec<_>>();
    let selected = (0..spec.rows)
        .filter(|&row| spec.shape == "unfiltered" || predicate[row] == Some(true))
        .collect::<Vec<_>>();
    let expected = if spec.count {
        vec![json!({"rows":selected.len()})]
    } else {
        selected.iter().map(|&row| input[row].clone()).collect()
    };
    let expected = json!(expected);
    let source_witness = witness(root, &json!(input));
    let source_schema_witness = witness(root, &serde_json::to_value(Spec::source_dtype()).unwrap());
    let expected_witness = witness(root, &expected);
    let expected_dtype = spec.output_dtype();
    let schema_witness = witness(root, &serde_json::to_value(&expected_dtype).unwrap());
    let observer = super::super::selection_observation::Session::start();
    let started = Instant::now();
    let uri = DatasetUri::new("memory://compact-local-indices").unwrap();
    let plan = spec.plan(uri.clone());
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
    let preparation_ns = started.elapsed().as_nanos();
    let memory = prepared.session.memory().clone();
    assert_eq!(prepared.output_dtype().as_ref(), Some(&expected_dtype));
    let mut operations = Vec::new();
    let mut complete_ns = preparation_ns;
    for call in 0..spec.calls {
        let started = Instant::now();
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let operation_ns = started.elapsed().as_nanos();
        complete_ns += operation_ns;
        let actual = json!(json_rows(&result));
        assert_eq!(actual, expected, "{} call {call}", spec.id());
        let actual_schema =
            serde_json::from_str::<Value>(result.result_schema_json.value()).unwrap();
        assert_eq!(
            actual_schema,
            serde_json::to_value(&expected_dtype).unwrap()
        );
        let certificate = &result.execution.native_io_certificate;
        assert!(certificate.is_certified() && !certificate.side_effects.fallback_attempted);
        assert_eq!(
            result.execution.output_rows,
            expected.as_array().unwrap().len() as u64
        );
        let mut record = json!({
            "call":call, "rows":result.execution.output_rows,
            "actual_rows_witness":witness(root,&actual),
            "actual_schema_witness":witness(root,&actual_schema),
            "engine_execution_ns":result.timing.execution.as_nanos(),
            "result_serialization_ns":result.timing.result_serialization.as_nanos(),
            "execution_and_consumption_ns":operation_ns,
            "native_io_certificate":{
                "certified":certificate.is_certified(), "fallback_attempted":certificate.side_effects.fallback_attempted,
                "complete_debug":format!("{certificate:#?}"),
            },
            "completed_executions":prepared.snapshot().completed_executions,
        });
        let release = Instant::now();
        drop(result);
        let release_ns = release.elapsed().as_nanos();
        complete_ns += release_ns;
        record["output_release_ns"] = json!(release_ns);
        operations.push(record);
    }
    let snapshot = prepared.snapshot();
    assert_eq!(snapshot.completed_executions, spec.calls as u64);
    assert_eq!(snapshot.admitted_max_parallelism, 1);
    assert_eq!(snapshot.provider_background_workers, 0);
    let started = Instant::now();
    drop(prepared);
    let destruction_ns = started.elapsed().as_nanos();
    complete_ns += destruction_ns;
    let final_memory = memory.snapshot();
    assert_eq!(final_memory.reserved_bytes, 0);
    assert_eq!(final_memory.denied_reservations, 0);
    json!({
        "cell_id":spec.id(), "rows":spec.rows, "shape":spec.shape,
        "consumer":spec.consumer(), "calls":spec.calls,
        "role":role, "cohort":cohort, "sample":sample, "warmup":warmup,
        "source_rows_witness":source_witness, "source_schema_witness":source_schema_witness,
        "expected_rows_witness":expected_witness,"expected_schema_witness":schema_witness,
        "preparation_ns":preparation_ns, "destruction_ns":destruction_ns,
        "complete_workflow_ns":complete_ns, "operations":operations,
        "completed_executions":snapshot.completed_executions,
        "prepared_source_opens":snapshot.prepared_source_opens,
        "admitted_max_parallelism":snapshot.admitted_max_parallelism,
        "provider_background_workers":snapshot.provider_background_workers,
        "peak_reserved_bytes":final_memory.peak_reserved_bytes,
        "reserved_after_cleanup":final_memory.reserved_bytes,
        "denied_reservations":final_memory.denied_reservations,
        "selection":observer.finish(),
    })
}

#[test]
#[ignore = "frozen compact local indices protocol; serial guarded execution only"]
fn native_compact_local_indices_screen() {
    let root = PathBuf::from(std::env::var("SHARDLOOM_COMPACT_INDEX_ROOT").unwrap());
    assert!(root.is_absolute());
    fs::create_dir_all(root.join("witnesses")).unwrap();
    let mode = std::env::var("SHARDLOOM_COMPACT_INDEX_MODE").unwrap();
    let role = std::env::var("SHARDLOOM_COMPACT_INDEX_ROLE").unwrap();
    let cohort = std::env::var("SHARDLOOM_COMPACT_INDEX_COHORT").unwrap();
    assert!(matches!(mode.as_str(), "preflight" | "measure"));
    assert!(matches!(role.as_str(), "baseline" | "candidate"));
    assert!(matches!(cohort.as_str(), "preflight" | "A" | "B"));
    let path = root.join(format!("{mode}-{cohort}-{role}.jsonl"));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut workflows = 0;
    let mut operations = 0;
    let mut emit = |spec: Spec, sample: usize, warmup: bool| {
        let result = workflow(&root, spec, &role, &cohort, sample, warmup);
        serde_json::to_writer(&mut file, &result).unwrap();
        writeln!(file).unwrap();
        file.flush().unwrap();
        workflows += 1;
        operations += spec.calls;
    };
    if mode == "measure" {
        emit(matrix(16_387)[0], 0, true);
    }
    let rows: &[usize] = if mode == "preflight" {
        &[0, 32, 16_387]
    } else {
        &[16_387]
    };
    let mut specs = rows
        .iter()
        .flat_map(|&rows| matrix(rows))
        .collect::<Vec<_>>();
    if cohort == "B" {
        specs.reverse();
    }
    for spec in specs {
        for sample in 0..if mode == "measure" { 3 } else { 1 } {
            emit(spec, sample, false);
        }
    }
    assert_eq!(workflows, if mode == "measure" { 85 } else { 84 });
    assert_eq!(operations, if mode == "measure" { 211 } else { 210 });
    println!("compact_indices: {workflows} workflows, {operations} operations");
}
