//! Frozen public prepared-sampling workflow, identical in both compared builds.

use super::*;
use crate::{
    local_primitives::prepared_unary::sample_observation,
    query_primitive::{VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest},
    relational_query::VortexRelationalUnary,
    resident_memory_source::{MemoryColumn, MemoryColumnValues, MemorySourceBounds},
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::{io::Write as _, path::Path, time::Instant};

#[derive(Clone, Copy)]
struct Spec {
    name: &'static str,
    population: usize,
    draws: usize,
}

impl Spec {
    fn weight(self, row: usize) -> f64 {
        match self.name {
            "uniform" | "few_draws" | "unweighted" => 1.0,
            "periodic" => f64::from(u32::try_from(1 + row % 16).unwrap()),
            "shifted" => {
                if row < 32_768 {
                    1.0
                } else {
                    1024.0
                }
            }
            "hot_tail" => {
                if row == 65_535 {
                    2.0_f64.powi(40)
                } else {
                    1.0
                }
            }
            "tiny" => f64::from(u32::try_from(1 + row % 4).unwrap()),
            "rounded_ties" => {
                let first = f64::from_bits(0x3fd6_ba82_24f5_0f05);
                [first, f64::from_bits(1), f64::from_bits(1), 1.0 - first][row]
            }
            _ => panic!("unknown frozen weighted-search workload"),
        }
    }

    fn plan(self, uri: DatasetUri) -> VortexRelationalPlan {
        let input = VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri,
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        });
        let mut request = VortexQueryPrimitiveRequest::for_relational_input(
            VortexQueryPrimitiveKind::SampleRows,
            shardloom_plan::ProjectionRequest::columns(vec![ColumnRef::new("id").unwrap()]),
        );
        request.source_order_limit = Some(self.draws);
        request.sample_with_replacement = true;
        request.sample_seed = Some(7);
        request.sample_weight_column =
            (self.name != "unweighted").then(|| ColumnRef::new("weight").unwrap());
        VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary { input, request }))
    }
}

fn witness(root: &Path, value: &Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap();
    let mut hash = String::with_capacity(64);
    for byte in Sha256::digest(&bytes) {
        use std::fmt::Write as _;
        write!(hash, "{byte:02x}").unwrap();
    }
    let path = root.join("witnesses").join(format!("{hash}.json"));
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
    hash
}

#[allow(clippy::too_many_lines)] // Keep the frozen operation/lifetime record together.
fn workflow(root: &Path, spec: Spec, calls: usize, observe: bool) -> Value {
    let ids = (0..spec.population)
        .map(|row| i64::try_from(row).unwrap())
        .collect::<Vec<_>>();
    let weights = (0..spec.population)
        .map(|row| Some(spec.weight(row)))
        .collect::<Vec<_>>();
    let input = ids
        .iter()
        .zip(&weights)
        .map(|(id, weight)| json!({"id": id, "weight_bits": weight.unwrap().to_bits()}))
        .collect::<Vec<_>>();
    let source_dtype = DType::struct_(
        vec![
            (
                "id".to_owned(),
                DType::Primitive(PType::I64, Nullability::NonNullable),
            ),
            (
                "weight".to_owned(),
                DType::Primitive(PType::F64, Nullability::Nullable),
            ),
        ],
        Nullability::NonNullable,
    );
    let output_dtype = DType::struct_(
        vec![(
            "id".to_owned(),
            DType::Primitive(PType::I64, Nullability::NonNullable),
        )],
        Nullability::NonNullable,
    );
    let input_witness = witness(root, &json!(input));
    let source_schema_witness = witness(root, &serde_json::to_value(&source_dtype).unwrap());
    let output_schema_witness = witness(root, &serde_json::to_value(&output_dtype).unwrap());
    let mut policy = policy();
    policy.resource_envelope.memory_budget_bytes = 64 << 20;
    let started = Instant::now();
    let uri = DatasetUri::new("memory://prepared-sampling-comparison").unwrap();
    let plan = spec.plan(uri.clone());
    let prepared = prepare_relational_with_schema(policy, |schemas| {
        schemas.register_memory_source(uri, |session| {
            let source = ResidentMemorySource::from_columns(
                session,
                &[
                    MemoryColumn {
                        name: "id",
                        values: MemoryColumnValues::Int64NonNullable(&ids),
                    },
                    MemoryColumn {
                        name: "weight",
                        values: MemoryColumnValues::Float64(&weights),
                    },
                ],
                MemorySourceBounds::default(),
            )?;
            assert_eq!(source.dtype(), &source_dtype);
            Ok(source)
        })?;
        Ok(plan)
    })
    .unwrap();
    let preparation_ns = started.elapsed().as_nanos();
    let memory = prepared.session.memory().clone();
    assert_eq!(prepared.output_dtype().as_ref(), Some(&output_dtype));
    let mut operations = Vec::new();
    let mut previous = None;
    let mut complete_ns = preparation_ns;
    let reserved_after_preparation = memory.snapshot().reserved_bytes;
    for call in 0..calls {
        let observation = observe.then(sample_observation::Session::start);
        let started = Instant::now();
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let operation_ns = started.elapsed().as_nanos();
        let counts = observation.map(sample_observation::Session::finish);
        complete_ns += operation_ns;
        let actual = json!(json_rows(&result));
        let actual_schema =
            serde_json::from_str::<Value>(result.result_schema_json.value()).unwrap();
        assert_eq!(actual_schema, serde_json::to_value(&output_dtype).unwrap());
        assert_eq!(result.execution.output_rows, spec.draws as u64);
        if let Some(previous) = &previous {
            assert_eq!(&actual, previous, "{} repeated draw sequence", spec.name);
        }
        previous = Some(actual.clone());
        if let Some(counts) = &counts {
            assert_eq!(counts.populations, 1);
            assert_eq!(counts.population_entries, spec.population as u64);
            assert_eq!(counts.logical_draws, spec.draws as u64);
            if spec.name == "unweighted" {
                assert_eq!(counts.searches, 0);
                assert_eq!(counts.comparisons, 0);
            } else {
                assert!(counts.searches >= spec.draws as u64);
            }
        }
        let certificate = &result.execution.native_io_certificate;
        assert!(certificate.is_certified());
        assert!(!certificate.side_effects.fallback_attempted);
        let mut operation = json!({
            "call":call, "rows":result.execution.output_rows, "counts":counts,
            "actual_rows_witness":witness(root, &actual),
            "actual_schema_witness":witness(root, &actual_schema),
            "engine_execution_ns":result.timing.execution.as_nanos(),
            "result_serialization_ns":result.timing.result_serialization.as_nanos(),
            "execution_and_consumption_ns":operation_ns,
            "certified":certificate.is_certified(),
            "fallback_attempted":certificate.side_effects.fallback_attempted,
            "native_io_certificate_debug":format!("{certificate:#?}"),
            "completed_executions":prepared.snapshot().completed_executions,
        });
        let started = Instant::now();
        drop(result);
        let release_ns = started.elapsed().as_nanos();
        complete_ns += release_ns;
        operation["output_release_ns"] = json!(release_ns);
        operation["reserved_after_output_release"] = json!(memory.snapshot().reserved_bytes);
        operations.push(operation);
    }
    let snapshot = prepared.snapshot();
    assert_eq!(snapshot.completed_executions, calls as u64);
    assert_eq!(snapshot.admitted_max_parallelism, 1);
    assert_eq!(snapshot.provider_background_workers, 0);
    let started = Instant::now();
    drop(prepared);
    let destruction_ns = started.elapsed().as_nanos();
    complete_ns += destruction_ns;
    let final_memory = memory.snapshot();
    assert_eq!(final_memory.reserved_bytes, 0);
    assert_eq!(final_memory.denied_reservations, 0);
    assert!(final_memory.peak_reserved_bytes <= 64 << 20);
    json!({
        "cell":spec.name, "population":spec.population, "draws":spec.draws,
        "calls":calls, "sample_seed":7, "with_replacement":true,
        "weighted":spec.name != "unweighted", "source_rows_witness":input_witness,
        "source_schema_witness":source_schema_witness, "output_schema_witness":output_schema_witness,
        "reserved_after_preparation":reserved_after_preparation,
        "reserved_before_teardown":snapshot.memory.reserved_bytes,
        "preparation_ns":preparation_ns, "destruction_ns":destruction_ns,
        "complete_workflow_ns":complete_ns, "operations":operations,
        "completed_executions":snapshot.completed_executions,
        "prepared_source_opens":snapshot.prepared_source_opens,
        "admitted_max_parallelism":snapshot.admitted_max_parallelism,
        "provider_background_workers":snapshot.provider_background_workers,
        "memory_grant_bytes":64 << 20, "peak_reserved_bytes":final_memory.peak_reserved_bytes,
        "reserved_after_cleanup":final_memory.reserved_bytes,
        "denied_reservations":final_memory.denied_reservations,
        "observation_enabled":observe,
    })
}

#[test]
#[ignore = "frozen prepared-sampling protocol; serial guarded comparison only"]
fn native_prepared_sampling_cost_screen() {
    let variable = |name| std::env::var(name).unwrap();
    let root = PathBuf::from(variable("SHARDLOOM_WEIGHTED_COST_ROOT"));
    assert!(root.is_absolute() && root.join("witnesses").is_dir());
    let cell = variable("SHARDLOOM_WEIGHTED_COST_CELL");
    let calls = variable("SHARDLOOM_WEIGHTED_COST_CALLS")
        .parse::<usize>()
        .unwrap();
    assert!(matches!(calls, 1 | 4 | 16));
    let role = variable("SHARDLOOM_WEIGHTED_COST_ROLE");
    let cohort = variable("SHARDLOOM_WEIGHTED_COST_COHORT");
    let mode = variable("SHARDLOOM_WEIGHTED_COST_MODE");
    let sample = variable("SHARDLOOM_WEIGHTED_COST_SAMPLE")
        .parse::<usize>()
        .unwrap();
    assert!(matches!(role.as_str(), "baseline" | "candidate"));
    assert!(matches!(cohort.as_str(), "preflight" | "A" | "B"));
    assert!(matches!(mode.as_str(), "preflight" | "measure"));
    assert!(sample <= 7);
    assert_eq!(mode == "preflight", cohort == "preflight");
    assert!(mode != "preflight" || sample == 0);
    let (name, population, draws) = [
        ("uniform", 65_536, 65_536),
        ("periodic", 65_536, 65_536),
        ("shifted", 65_536, 65_536),
        ("hot_tail", 65_536, 65_536),
        ("few_draws", 65_536, 8),
        ("tiny", 32, 128),
        ("unweighted", 65_536, 65_536),
        ("rounded_ties", 4, 7),
    ]
    .into_iter()
    .find(|(name, _, _)| *name == cell)
    .expect("frozen cell");
    let path = root.join(format!(
        "{mode}-{cohort}-{cell}-n{calls}-{role}-{sample}.json"
    ));
    assert!(!path.exists());
    let mut record = workflow(
        &root,
        Spec {
            name,
            population,
            draws,
        },
        calls,
        mode == "preflight",
    );
    record["role"] = json!(role);
    record["cohort"] = json!(cohort);
    record["mode"] = json!(mode);
    record["sample"] = json!(sample);
    record["warmup"] = json!(mode == "measure" && sample == 0);
    let mut output = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    serde_json::to_writer(&mut output, &record).unwrap();
    writeln!(output).unwrap();
    output.flush().unwrap();
    println!("prepared sampling {mode}/{cohort}/{cell}/n{calls}/{role}/{sample}: complete");
}
