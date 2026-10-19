//! Typed sample payloads and retained ownership across actual native runs.

use super::super::super::membership_tests::typed_tests::{FIELDS, typed_source};
use super::*;
use crate::local_primitives::{
    native_relational_spill::{BEFORE_RUN_OPEN, State},
    prepared_unary::BoundUnary,
};
use crate::relational_query::VortexRelationalSpillPolicy;
use std::{cell::Cell, rc::Rc};
use vortex::array::arrays::VarBinViewArray;

fn cases() -> Vec<Value> {
    let reference: Value = serde_json::from_str(include_str!(
        "../tests/fixtures/native_sampling_population_reference.json"
    ))
    .unwrap();
    let cases = reference["typed_cases"].as_array().unwrap().clone();
    assert_eq!(cases.len(), 6);
    cases
}

fn typed_request(case: &Value, fields: &[&str]) -> VortexQueryPrimitiveRequest {
    let mut req = sample_request(case);
    req.projection = columns(fields);
    if case["weighted"].as_bool().unwrap() {
        req.sample_weight_column = Some(ColumnRef::new("id").unwrap());
        req.predicate = Some(PredicateExpr::Compare {
            column: ColumnRef::new("id").unwrap(),
            op: ComparisonOp::Gt,
            value: shardloom_core::StatValue::UInt64(0),
        });
    }
    req
}

fn expected(case: &Value, rows: &[Value]) -> Vec<Value> {
    case["expected"]
        .as_array()
        .unwrap()
        .iter()
        .map(|ordinal| rows[usize::try_from(ordinal.as_u64().unwrap()).unwrap()].clone())
        .collect()
}

// Raw arrays expose invalid unselected payloads that finite public construction
// rejects earlier. The separate pressure tests use the credited source boundary.
fn consume(array: &ArrayRef, req: &VortexQueryPrimitiveRequest, spill: bool) -> Result<Vec<Value>> {
    let (_fixture, directory) = workspace();
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let bound = BoundUnary::for_relation(req, array.dtype(), &memory).unwrap();
    let dtype = DType::struct_(bound.fields().to_vec(), Nullability::NonNullable);
    let mut retained = Vec::new();
    let mut calls = 0;
    let mut ended = false;
    let result = session.with_native_execution_context(&CancellationToken::default(), |context| {
        let mut output = Vec::new();
        let produce = |consumer: &mut dyn FnMut(ArrayRef) -> Result<()>| {
            calls += 1;
            for start in (0..array.len()).step_by(2) {
                consumer(array.slice(start..(start + 2).min(array.len())).unwrap())?;
            }
            ended = true;
            Ok(())
        };
        let mut consumer = |array: ArrayRef| {
            assert_eq!(array.dtype(), &dtype);
            output.extend(values(&array, context)?);
            retained.push(array);
            Ok(())
        };
        if spill {
            let policy = VortexRelationalSpillPolicy::new(&directory, 128 << 20, 1 << 20)?;
            let state = State::new(&policy, context)?;
            let result =
                bound.consume_relation_sampling(context, &state, 2, produce, &mut consumer);
            let report = state.finish()?;
            assert!(report.runs_written > 0);
            assert!(report.owned_cleanup_completed);
            result?;
        } else {
            bound.consume_relation(context, None, 2, produce, &mut consumer)?;
        }
        Ok(output)
    });
    assert_eq!(calls, 1);
    assert!(ended);
    clean(&directory);
    assert!(memory.snapshot().peak_reserved_bytes <= 64 << 20);
    drop((bound, dtype, session));
    if result.is_ok() {
        assert!(memory.snapshot().reserved_bytes > 0);
    }
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    result
}

#[test]
fn sampling_population_spilled_typed_dictionary_payloads_detach_and_retain_exact_values() {
    for dictionary in [false, true] {
        let (array, rows) = typed_source(dictionary);
        for case in cases() {
            for spill in [false, true] {
                let req = typed_request(&case, &FIELDS);
                assert_eq!(
                    consume(&array, &req, spill).unwrap(),
                    expected(&case, &rows),
                    "{dictionary}/{}/{spill}",
                    case["name"]
                );
            }
        }
    }
}

#[test]
fn sampling_population_spilled_scalar_and_nested_payloads_preserve_signed_zero_bits() {
    let zeros = [-0.0_f64, 0.0, -0.0, 0.0, 1.0, -1.0];
    let pad = "z".repeat(600 << 10);
    let array = StructArray::new(
        FieldNames::from(["id", "small", "wide", "nested", "pad"]),
        vec![
            PrimitiveArray::from_iter(0_u64..6).into_array(),
            PrimitiveArray::from_iter([-0.0_f32, 0.0, -0.0, 0.0, 1.0, -1.0]).into_array(),
            PrimitiveArray::from_iter(zeros).into_array(),
            StructArray::new(
                FieldNames::from(["value"]),
                vec![PrimitiveArray::from_iter(zeros).into_array()],
                6,
                Validity::NonNullable,
            )
            .into_array(),
            VarBinViewArray::from_iter_str(std::iter::repeat_n(pad.as_str(), 6)).into_array(),
        ],
        6,
        Validity::NonNullable,
    )
    .into_array();
    for case in cases() {
        let req = typed_request(&case, &["id", "small", "wide", "nested", "pad"]);
        for spill in [false, true] {
            let rows = consume(&array, &req, spill).unwrap();
            assert_eq!(rows.len(), case["expected"].as_array().unwrap().len());
            for (row, ordinal) in rows.iter().zip(case["expected"].as_array().unwrap()) {
                assert_eq!(row["id"], *ordinal);
                let value = zeros[usize::try_from(ordinal.as_u64().unwrap()).unwrap()];
                for selected in [&row["small"], &row["wide"], &row["nested"]["value"]] {
                    assert_eq!(selected.as_f64().unwrap().to_bits(), value.to_bits());
                }
                assert_eq!(row["pad"], pad);
            }
        }
    }
}

#[test]
fn sampling_population_spilled_nonfinite_payloads_keep_selected_observation_boundary() {
    let pad = "f".repeat(600 << 10);
    for replace in [false, true] {
        // Independent seed-7 oracle: fraction 1/4 chooses ordinal 1 from four
        // rows, while a single replacement draw chooses ordinal 3.
        let chosen = if replace { 3 } else { 1 };
        for invalid in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            for selected_invalid in [false, true] {
                let mut payload = [1.0; 4];
                payload[if selected_invalid { chosen } else { 0 }] = invalid;
                let array = StructArray::new(
                    FieldNames::from(["id", "value", "pad"]),
                    vec![
                        PrimitiveArray::from_iter(0_u64..4).into_array(),
                        PrimitiveArray::from_iter(payload).into_array(),
                        VarBinViewArray::from_iter_str(std::iter::repeat_n(pad.as_str(), 4))
                            .into_array(),
                    ],
                    4,
                    Validity::NonNullable,
                )
                .into_array();
                let mut req = request(Kind::SampleRows, &["id", "value", "pad"]);
                req.sample_fraction = Some(0.25);
                req.sample_seed = Some(7);
                req.sample_with_replacement = replace;
                for spill in [false, true] {
                    let result = consume(&array, &req, spill);
                    if selected_invalid {
                        assert!(result.unwrap_err().to_string().contains("finite"));
                    } else {
                        assert_eq!(
                            result.unwrap(),
                            [json!({"id":chosen,"value":1.0,"pad":pad})]
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn sampling_population_native_file_generation_failure_cleans_then_fresh_preparation_completes() {
    for case in cases().into_iter().filter(|case| case["replace"] == true) {
        let (array, rows) = typed_source(true);
        let fixture = Fixture::new(array, 2);
        let directory = fixture.0.join("unary-runs");
        fs::create_dir(&directory).unwrap();
        fs::write(directory.join("foreign"), b"preserve").unwrap();
        let mut resources = policy();
        resources.resource_envelope.memory_budget_bytes = 64 << 20;
        let plan = unary(fixture.scan(), typed_request(&case, &FIELDS));
        let prepared = with_spill(prepare_relational(&plan, resources).unwrap(), &directory);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let fired = Rc::new(Cell::new(false));
        let observed = Rc::clone(&fired);
        let source = fixture.path();
        BEFORE_RUN_OPEN.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move |_| {
                let replacement = source.with_extension("replacement");
                fs::copy(&source, &replacement).unwrap();
                fs::rename(replacement, source).unwrap();
                observed.set(true);
            }));
        });
        let error = prepared
            .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
            .err()
            .unwrap()
            .to_string();
        assert!(fired.get());
        assert!(error.contains("changed"), "{error}");
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert!(BEFORE_RUN_OPEN.with(|slot| slot.borrow().is_none()));
        clean(&directory);
        drop(prepared);

        let prepared = with_spill(prepare_relational(&plan, resources).unwrap(), &directory);
        let memory = prepared.session.memory().clone();
        let dtype = prepared.output_dtype().unwrap();
        let mut actual = Vec::new();
        let mut retained = Vec::new();
        let report = prepared
            .for_each_batch(&CancellationToken::default(), |array, context| {
                assert_eq!(array.dtype(), &dtype);
                actual.extend(values(&array, context)?);
                retained.push(array);
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected(&case, &rows));
        assert_eq!(report.spillable_sampling_stages, 1);
        assert!(report.spill.as_ref().unwrap().runs_written > 0);
        assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
        assert_eq!(report.runtime.completed_executions, 1);
        assert!(report.native_io_certificate.is_certified());
        clean(&directory);
        drop((report, prepared, dtype));
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(retained);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
