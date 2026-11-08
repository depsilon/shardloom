//! Frozen semantic facts selected before native implementation, through binding
//! and native arrays. Intentionally raw arrays isolate expression observation
//! rules from the separate public input-admission contract.

use super::*;
use serde_json::{Value as Json, json};
use vortex::array::arrays::{ExtensionArray, FixedSizeListArray};

#[path = "local_primitive_relational_vector_reference_fixture.rs"]
mod fixture;

fn corpus() -> Json {
    serde_json::from_str(include_str!(
        "../tests/fixtures/native_vector_semantic_reference_r1.json"
    ))
    .unwrap()
}

fn control(value: &Json) -> Input {
    lit(if value.is_null() {
        ScalarValue::Null
    } else if let Some(text) = value.as_str() {
        ScalarValue::Utf8(text.into())
    } else if let Some(signed) = value.as_i64() {
        ScalarValue::Int64(signed)
    } else {
        ScalarValue::UInt64(value.as_u64().unwrap())
    })
}

fn error_matches(error: &str, category: &str) -> bool {
    match category {
        "unequal_dimensions" => error.contains("dimensions differ"),
        "observed_nonfinite" => error.contains("nonfinite vector components"),
        "uniform_size" => error.contains("uniform_size"),
        "uniform_shape" => error.contains("uniform dimension"),
        "shape_product" => error.contains("shape product"),
        "shape_dimension" => error.contains("nonnegative") || error.contains("nonnull int32"),
        "embedding_space" => error.contains("space_id") || error.contains("explicit VECTOR_VALUES"),
        "embedding_identity" => error.contains("embedding") && error.contains("differs"),
        _ => panic!("unknown expected error class {category}"),
    }
}

fn bits(array: &ArrayRef, index: usize, execution: &mut vortex::array::ExecutionCtx) -> Json {
    let primitive = array.clone().execute::<PrimitiveArray>(execution).unwrap();
    if !array.is_valid(index, execution).unwrap() {
        return Json::Null;
    }
    match primitive.ptype() {
        PType::F16 => json!(format!(
            "{:04x}",
            primitive.to_buffer::<vortex::array::dtype::half::f16>()[index].to_bits()
        )),
        PType::F32 => json!(format!(
            "{:08x}",
            primitive.to_buffer::<f32>()[index].to_bits()
        )),
        PType::F64 => json!(format!(
            "{:016x}",
            primitive.to_buffer::<f64>()[index].to_bits()
        )),
        _ => panic!("floating component required"),
    }
}

fn assert_result(output: &ArrayRef, expected: &Json, id: &str) {
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    assert_eq!(output.len(), 1, "{id}");
    if expected["kind"] == "null" {
        assert!(!output.is_valid(0, &mut execution).unwrap(), "{id}");
        return;
    }
    assert!(output.is_valid(0, &mut execution).unwrap(), "{id}");
    match expected["kind"].as_str().unwrap() {
        "f64_bits" | "component_bits" => {
            assert_eq!(bits(output, 0, &mut execution), expected["bits"], "{id}");
        }
        "u64" => {
            assert_eq!(
                output
                    .execute_scalar(0, &mut execution)
                    .unwrap()
                    .as_primitive()
                    .as_::<u64>(),
                expected["value"].as_u64(),
                "{id}"
            );
        }
        "vector_bits" | "shape_u64" => {
            assert!(!matches!(output.dtype(), DType::Extension(_)), "{id}");
            if let Some(fixed) = expected["fixed"].as_u64() {
                assert!(
                    matches!(output.dtype(),DType::FixedSizeList(_,size,_) if u64::from(*size)==fixed),
                    "{id}"
                );
            } else {
                assert!(matches!(output.dtype(), DType::List(_, _)), "{id}");
            }
            let column =
                crate::local_primitives::native_list::Column::new(output.clone(), &mut execution)
                    .unwrap();
            let (from, count) = column.coordinates(0, &mut execution).unwrap().unwrap();
            let values = (from..from + count)
                .map(|index| {
                    if expected["kind"] == "shape_u64" {
                        json!(
                            column
                                .elements
                                .execute_scalar(index, &mut execution)
                                .unwrap()
                                .as_primitive()
                                .as_::<u64>()
                                .unwrap()
                        )
                    } else {
                        bits(&column.elements, index, &mut execution)
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(json!(values), expected["values"], "{id}");
        }
        other => panic!("unknown expected result {other}"),
    }
}

#[test]
fn native_vector_frozen_semantic_rows_match_under_both_float_policies() {
    let corpus = corpus();
    let rows = corpus["cases"].as_array().unwrap();
    assert_eq!(rows.len(), 85);
    for (policy, key) in [
        (FloatPolicy::Strict, "strict"),
        (FloatPolicy::Extended, "extended"),
    ] {
        let session = ResidentVortexSession::new_with_float_policy(32 << 20, 1, policy).unwrap();
        for case in rows {
            let id = case["id"].as_str().unwrap();
            let mut columns = Vec::new();
            let mut args = Vec::new();
            for (index, argument) in case["arguments"].as_array().unwrap().iter().enumerate() {
                if argument.is_object() {
                    let name = format!("v{index}");
                    args.push(col(&name));
                    columns.push((name, fixture::array(argument)));
                } else {
                    args.push(control(argument));
                }
            }
            let input = batch(
                &columns
                    .iter()
                    .map(|(name, array)| (name.as_str(), array.clone()))
                    .collect::<Vec<_>>(),
                1,
            );
            let output = evaluate(
                &session,
                &call(case["operation"].as_str().unwrap(), args),
                &input,
            );
            let expected = &case["expected"][key];
            if expected["kind"] == "error" {
                let error = output.unwrap_err().to_string();
                assert!(
                    error_matches(&error, expected["category"].as_str().unwrap()),
                    "{id} {key}: {error}"
                );
            } else {
                assert_result(
                    &output.unwrap_or_else(|error| panic!("{id} {key}: {error}")),
                    expected,
                    id,
                );
            }
            assert_eq!(session.memory().snapshot().reserved_bytes, 0, "{id} {key}");
        }
    }
}

#[test]
fn native_vector_frozen_identity_bindings_are_static_even_for_empty_input() {
    let corpus = corpus();
    let cases = corpus["binding_cases"].as_array().unwrap();
    assert_eq!(cases.len(), 19);
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    for case in cases {
        let id = case["id"].as_str().unwrap();
        let fields = [
            ("left".into(), fixture::array(&case["left"]).dtype().clone()),
            (
                "right".into(),
                fixture::array(&case["right"]).dtype().clone(),
            ),
        ];
        for operation in [
            "vector_dot",
            "vector_l2_distance",
            "vector_cosine_similarity",
        ] {
            let mut binder = Binder::new(&session).unwrap();
            let bound = binder.expression(
                &call(operation, vec![col("left"), col("right")]),
                &fields,
                0,
            );
            if case["accepted"] == true {
                assert!(bound.is_ok(), "{id} {operation}");
            } else {
                let error = match bound {
                    Ok(_) => panic!("{id} {operation} accepted"),
                    Err(error) => error.to_string(),
                };
                assert!(
                    error_matches(&error, case["error_category"].as_str().unwrap()),
                    "{id} {operation}: {error}"
                );
            }
        }
        assert_eq!(session.memory().snapshot().reserved_bytes, 0, "{id}");
    }
}
