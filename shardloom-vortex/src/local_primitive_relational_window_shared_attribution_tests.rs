//! Complete native window requests with independent membership and exact integer moments.
//! Debug observations establish actual work; they are never release timings.

use super::*;
use crate::{
    local_primitives::native_relational_observation::Session,
    relational_query::VortexRelationalScan,
    resident_memory_source::{MemoryColumn, MemoryColumnValues},
};
use sha2::{Digest as _, Sha256};
use std::io::Write as _;
use vortex::array::dtype::Nullability;

const URI: &str = "memory://compatible-window-attribution";
const GRANT: u64 = 64 << 20;
const CASES: [&str; 16] = [
    "single_sum",
    "single_variance",
    "arithmetic",
    "arithmetic_count_first",
    "arithmetic_count_star_interrupted",
    "arithmetic_key_interrupted",
    "arithmetic_filter_interrupted",
    "arithmetic_frame_interrupted",
    "arithmetic_rank_interrupted",
    "arithmetic_filtered",
    "arithmetic_excluded",
    "arithmetic_all_null",
    "moments",
    "moments_key_interrupted",
    "moments_excluded",
    "moments_all_null",
];

#[derive(Clone)]
struct Output {
    function: &'static str,
    column: Option<&'static str>,
    filtered: bool,
    excluded: bool,
    preceding: usize,
}

fn outputs(case: &str) -> Vec<Output> {
    let functions: &[&str] = match case {
        "single_sum" => &["sum"],
        "single_variance" => &["var_pop"],
        "arithmetic_count_first" => &["count", "sum", "avg"],
        "arithmetic_count_star_interrupted" => &["sum", "count", "avg"],
        "arithmetic_rank_interrupted" => &["sum", "row_number", "avg"],
        name if name.starts_with("moments") => {
            &["var_pop", "var_samp", "stddev_pop", "stddev_samp"]
        }
        _ => &["sum", "avg", "count"],
    };
    let mut outputs: Vec<_> = functions
        .iter()
        .map(|&function| Output {
            function,
            column: Some("value"),
            filtered: case == "arithmetic_filtered",
            excluded: case.ends_with("excluded"),
            preceding: 7,
        })
        .collect();
    if case.ends_with("key_interrupted") {
        outputs[1].column = Some("other");
    }
    if case == "arithmetic_filter_interrupted" {
        outputs[1].filtered = true;
    }
    if case == "arithmetic_frame_interrupted" {
        outputs[1].preceding = 3;
    }
    if case == "arithmetic_count_star_interrupted" {
        outputs[1].column = None;
    }
    if case == "wide_arithmetic" {
        outputs.truncate(1);
        outputs.extend((0..1040).map(|_| Output {
            function: "avg",
            column: Some("value"),
            filtered: false,
            excluded: false,
            preceding: 7,
        }));
    }
    outputs
}

struct Data {
    ids: Vec<i64>,
    values: Vec<Option<i64>>,
    other: Vec<Option<i64>>,
    flags: Vec<Option<bool>>,
}

impl Data {
    fn new(case: &str, rows: usize) -> Self {
        Self {
            ids: (0..rows).map(|row| i64::try_from(row).unwrap()).collect(),
            values: (0..rows)
                .map(|row| {
                    if case.ends_with("all_null") || row.is_multiple_of(5) {
                        None
                    } else {
                        Some(i64::try_from(row % 17).unwrap() - 8)
                    }
                })
                .collect(),
            other: (0..rows)
                .map(|row| Some(i64::try_from(row % 13).unwrap() - 6))
                .collect(),
            flags: (0..rows)
                .map(|row| (!row.is_multiple_of(7)).then_some(row.is_multiple_of(3)))
                .collect(),
        }
    }

    fn source(
        &self,
        session: &ResidentVortexSession,
        range: std::ops::Range<usize>,
    ) -> Result<ResidentMemorySource> {
        ResidentMemorySource::from_batch_columns(
            session,
            &[
                MemoryColumn {
                    name: "id",
                    values: MemoryColumnValues::Int64NonNullable(&self.ids[range.clone()]),
                },
                MemoryColumn {
                    name: "value",
                    values: MemoryColumnValues::Int64(&self.values[range.clone()]),
                },
                MemoryColumn {
                    name: "other",
                    values: MemoryColumnValues::Int64(&self.other[range.clone()]),
                },
                MemoryColumn {
                    name: "keep",
                    values: MemoryColumnValues::Bool(&self.flags[range]),
                },
            ],
        )
    }
}

fn declaration(outputs: &[Output]) -> VortexRelationalPlan {
    let expressions = outputs
        .iter()
        .enumerate()
        .map(|(ordinal, output)| {
            let name = format!("o{ordinal}");
            if output.function == "row_number" {
                return Expression {
                    output_column: name,
                    function: VortexRelationalWindowFunction::RowNumber,
                    partition_by: vec![],
                    order_by: vec![Order {
                        column: ColumnRef::new("id").unwrap(),
                        descending: false,
                        nulls: None,
                    }],
                    frame: None,
                };
            }
            let mut measure = measure(output.function, output.column, &name);
            if output.filtered {
                measure = measure.with_filter_column(ColumnRef::new("keep").unwrap());
            }
            let mut expression = moving(measure);
            let frame = expression.frame.as_mut().unwrap();
            frame.start = Bound::Preceding(Offset::Number(shardloom_core::ScalarValue::UInt64(
                u64::try_from(output.preceding).unwrap(),
            )));
            frame.exclusion = if output.excluded {
                Exclusion::CurrentRow
            } else {
                Exclusion::NoOthers
            };
            expression
        })
        .collect();
    VortexRelationalPlan::Window(Box::new(Window {
        input: VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: DatasetUri::new(URI).unwrap(),
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        }),
        columns: vec![ColumnRef::new("id").unwrap()],
        expressions,
    }))
}

fn expected(data: &Data, outputs: &[Output]) -> (Vec<Value>, DType) {
    let mut fields = vec![(
        "id".to_owned(),
        DType::Primitive(PType::I64, Nullability::NonNullable),
    )];
    for (ordinal, output) in outputs.iter().enumerate() {
        let dtype = match output.function {
            "count" => DType::Primitive(PType::U64, Nullability::NonNullable),
            "row_number" => DType::Primitive(PType::I64, Nullability::NonNullable),
            _ => DType::Primitive(PType::F64, Nullability::Nullable),
        };
        fields.push((format!("o{ordinal}"), dtype));
    }
    let values = (0..data.ids.len())
        .map(|row| {
            let mut result = serde_json::Map::new();
            result.insert("id".into(), json!(data.ids[row]));
            for (ordinal, output) in outputs.iter().enumerate() {
                let mut selected = Vec::new();
                for member in row.saturating_sub(output.preceding)..=row {
                    if (output.excluded && member == row)
                        || (output.filtered && data.flags[member] != Some(true))
                    {
                        continue;
                    }
                    let value = match output.column {
                        Some("value") => data.values[member],
                        Some("other") => data.other[member],
                        None => Some(1),
                        _ => panic!("undeclared diagnostic column"),
                    };
                    if let Some(value) = value {
                        selected.push(value);
                    }
                }
                let count = i64::try_from(selected.len()).unwrap();
                let sum: i64 = selected.iter().sum();
                let squares: i64 = selected.iter().map(|value| value * value).sum();
                let exact = |value| f64::from(i32::try_from(value).unwrap());
                let value = match output.function {
                    "row_number" => json!(row + 1),
                    "count" => json!(count),
                    _ if count == 0 => Value::Null,
                    "sum" => json!(exact(sum)),
                    "avg" => json!(exact(sum) / exact(count)),
                    function => {
                        let sample = function.ends_with("samp");
                        if sample && count < 2 {
                            Value::Null
                        } else {
                            let numerator = count * squares - sum * sum;
                            assert!(numerator >= 0);
                            let variance =
                                exact(numerator) / exact(count * (count - i64::from(sample)));
                            json!(if function.starts_with("stddev") {
                                variance.sqrt()
                            } else {
                                variance
                            })
                        }
                    }
                };
                result.insert(format!("o{ordinal}"), value);
            }
            Value::Object(result)
        })
        .collect();
    (values, DType::struct_(fields, Nullability::NonNullable))
}

fn compare_values(actual: &[Value], expected: &[Value], outputs: &[Output]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.as_object().unwrap().len(), outputs.len() + 1);
        assert_eq!(actual["id"], expected["id"]);
        for (ordinal, output) in outputs.iter().enumerate() {
            let name = format!("o{ordinal}");
            if (output.function.starts_with("var_") || output.function.starts_with("stddev_"))
                && !expected[&name].is_null()
            {
                let actual = actual[&name].as_f64().unwrap();
                let expected = expected[&name].as_f64().unwrap();
                assert!(actual.is_finite());
                assert!((actual - expected).abs() <= 1e-9 + 2e-12 * expected.abs());
            } else {
                assert_eq!(actual[&name], expected[&name]);
            }
        }
    }
}

fn result_identity(rows: &[Value]) -> String {
    use std::fmt::Write as _;
    let mut value = String::with_capacity(64);
    for byte in Sha256::digest(serde_json::to_vec(rows).unwrap()) {
        write!(value, "{byte:02x}").unwrap();
    }
    value
}

fn observe(case: &str, rows: usize) -> Value {
    let data = Data::new(case, rows);
    let outputs = outputs(case);
    let (wanted, schema) = expected(&data, &outputs);
    let mut resources = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
    resources.resource_envelope.memory_budget_bytes = GRANT;
    let prepared = prepare_relational_with_schema(resources, |schemas| {
        schemas
            .register_batch_source(DatasetUri::new(URI)?, |session| data.source(session, 0..0))?;
        Ok(declaration(&outputs))
    })
    .unwrap();
    assert!(!prepared.root_has_file_kernel());
    assert_eq!(prepared.output_dtype().as_ref(), Some(&schema));
    let memory = prepared.session.memory().clone();
    let initial = memory.snapshot().reserved_bytes;
    let owner_thread = std::thread::current().id();
    let (mut calls, mut position, mut ended) = (0u64, 0usize, false);
    let mut input = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
        assert_eq!(std::thread::current().id(), owner_thread);
        assert_eq!(uri.as_str(), URI);
        assert_eq!(ordinal, calls);
        assert!(!ended);
        calls += 1;
        if ordinal == 0 {
            return data.source(session, 0..0).map(Some);
        }
        if position == rows {
            ended = true;
            return Ok(None);
        }
        let start = position;
        position = (start + 257).min(rows);
        data.source(session, start..position).map(Some)
    };
    let observation = Session::start();
    let result = prepared
        .with_batch_inputs(&mut input)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let counts = observation.finish();
    let actual = json_rows(&result);
    compare_values(&actual, &wanted, &outputs);
    assert_eq!(
        serde_json::from_str::<Value>(result.result_schema_json.value()).unwrap(),
        serde_json::to_value(&schema).unwrap()
    );
    assert!(ended && position == rows);
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
    assert_eq!(result.execution.ordered_window_stages, 0);
    assert!(counts.window_frame_bound_evaluations > 0);
    assert!(counts.window_state_admissions > 0);
    assert_eq!(
        counts.window_state_admissions,
        counts.window_state_construction_attempts
    );
    let framed = outputs
        .iter()
        .filter(|output| output.function != "row_number")
        .count();
    assert_eq!(
        counts.window_resident_frame_writes,
        u64::try_from(rows * framed).unwrap()
    );
    let actual_identity = result_identity(&actual);
    let expected_identity = result_identity(&wanted);
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, initial);
    assert_eq!(prepared.snapshot().completed_executions, 1);
    assert_eq!(prepared.snapshot().admitted_max_parallelism, 1);
    assert_eq!(prepared.snapshot().provider_background_workers, 0);
    drop(prepared);
    let final_memory = memory.snapshot();
    assert_eq!(final_memory.reserved_bytes, 0);
    assert_eq!(final_memory.denied_reservations, 0);
    assert!(final_memory.peak_reserved_bytes <= GRANT);
    json!({"case":case, "input_rows":rows, "output_rows":actual.len(), "output_columns":outputs.len()+1,
        "observations":counts, "expected_sha256":expected_identity, "actual_sha256":actual_identity,
        "complete_values_and_schema_match":true, "source_exhausted":ended,
        "peak_charged_bytes":final_memory.peak_reserved_bytes, "final_charged_bytes":0,
        "observation_scope":"debug-test caller thread; one admitted worker; zero provider background workers",
        "moments_comparison":{"absolute":1e-9,"relative":2e-12}, "engine_timing_claimed":false, "no_fallback":true})
}

#[test]
#[ignore = "guarded serial window attribution; never a release timing benchmark"]
fn native_compatible_window_complete_request_attribution() {
    let destination = PathBuf::from(std::env::var("SHARDLOOM_WINDOW_ATTRIBUTION_OUTPUT").unwrap());
    assert!(destination.is_absolute() && !destination.exists());
    let mut observations = Vec::new();
    for rows in [17, 4097] {
        for case in CASES {
            let observation = observe(case, rows);
            eprintln!("WINDOW_SHARED_WORK {observation}");
            observations.push(observation);
        }
    }
    let observation = observe("wide_arithmetic", 17);
    eprintln!("WINDOW_SHARED_WORK {observation}");
    observations.push(observation);
    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination)
        .unwrap()
        .write_all(
            &serde_json::to_vec_pretty(&json!({
                "schema":"shardloom.compatible_window_attribution.v1", "observations":observations
            }))
            .unwrap(),
        )
        .unwrap();
}

#[test]
fn wide_window_outputs_observe_cancellation_and_refund_before_publication() {
    let fixture = fixture(PrimitiveArray::from_iter([1_i32]).into_array());
    for functions in [
        ["sum", "avg", "count", "avg"],
        ["var_pop", "var_samp", "stddev_pop", "stddev_samp"],
    ] {
        let plan = VortexRelationalPlan::Window(Box::new(Window {
            input: fixture.scan(),
            columns: vec![ColumnRef::new("id").unwrap()],
            expressions: (0..2049)
                .map(|index| {
                    moving(measure(
                        functions[index % functions.len()],
                        Some("value"),
                        &format!("o{index}"),
                    ))
                })
                .collect(),
        }));
        let mut resources = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
        resources.resource_envelope.memory_budget_bytes = GRANT;
        let prepared = prepare_relational(&plan, resources).unwrap();
        assert!(!prepared.root_has_file_kernel());
        assert_eq!(prepared.snapshot().admitted_max_parallelism, 1);
        assert_eq!(prepared.snapshot().provider_background_workers, 0);
        let memory = prepared.session.memory().clone();
        let baseline = memory.snapshot().reserved_bytes;
        let cancel = CancellationToken::default();
        let observation = Session::start();
        Session::cancel_after_window_writes(1, &cancel);
        let mut published = 0;
        let error = prepared
            .for_each_batch(&cancel, |_, _| {
                published += 1;
                Ok(())
            })
            .err()
            .unwrap();
        let counts = observation.finish();
        assert!(cancel.is_cancelled());
        assert!(error.to_string().contains("execution cancelled"));
        assert_eq!(published, 0);
        assert!((1..=1025).contains(&counts.window_resident_frame_writes));
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);

        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let rows = json_rows(&result);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].as_object().unwrap().len(), 2050);
        assert_eq!(rows[0]["id"], json!(0));
        for index in 0..2049 {
            let expected = match functions[index % functions.len()] {
                "sum" | "avg" => json!(1.0),
                "count" => json!(1),
                "var_pop" | "stddev_pop" => json!(0.0),
                "var_samp" | "stddev_samp" => Value::Null,
                _ => unreachable!(),
            };
            assert_eq!(rows[0][format!("o{index}")], expected);
        }
        assert!(result.execution.native_io_certificate.is_certified());
        assert!(
            !result
                .execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
        drop(result);
        assert_eq!(prepared.snapshot().completed_executions, 1);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
