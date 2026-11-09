//! Native binding, masks, widths and retained vector expression results.

use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_core::{ColumnRef, ExprId, FloatPolicy};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::VortexSessionDefault as _;
use vortex::array::{
    ArrayRef, IntoArray as _, VortexSessionExecute as _,
    arrays::{ChunkedArray, DictArray, ListViewArray, PrimitiveArray, StructArray},
    dtype::{FieldNames, extension::ExtDType},
    validity::Validity,
};

#[path = "local_primitive_relational_vector_inspector_tests.rs"]
mod inspectors;

#[path = "local_primitive_relational_vector_reference_tests.rs"]
mod reference;

#[path = "local_primitive_relational_vector_resource_tests.rs"]
mod resources;

fn expr(kind: ExpressionKind) -> Input {
    Input::new(ExprId::new("vector-test").unwrap(), kind)
}
fn col(name: &str) -> Input {
    expr(ExpressionKind::Column(ColumnRef::new(name).unwrap()))
}
fn lit(value: ScalarValue) -> Input {
    expr(ExpressionKind::Literal(value))
}
fn call(name: &str, args: Vec<Input>) -> Input {
    expr(ExpressionKind::FunctionCall {
        name: name.into(),
        args,
    })
}
fn list_literal(values: &[Option<f64>]) -> Input {
    expr(ExpressionKind::List {
        values: values
            .iter()
            .map(|value| lit(value.map_or(ScalarValue::Null, ScalarValue::Float64)))
            .collect(),
    })
}
fn batch(columns: &[(&str, ArrayRef)], rows: usize) -> ArrayRef {
    StructArray::try_new(
        FieldNames::from(columns.iter().map(|(name, _)| *name).collect::<Vec<_>>()),
        columns.iter().map(|(_, array)| array.clone()),
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}
fn list(values: ArrayRef, spans: &[(u64, u64)], valid: Validity) -> ArrayRef {
    ListViewArray::try_new(
        values,
        PrimitiveArray::from_iter(spans.iter().map(|span| span.0)).into_array(),
        PrimitiveArray::from_iter(spans.iter().map(|span| span.1)).into_array(),
        valid,
        &mut vortex::session::VortexSession::default().create_execution_ctx(),
    )
    .unwrap()
    .into_array()
}
fn evaluate(
    session: &ResidentVortexSession,
    expression: &Input,
    input: &ArrayRef,
) -> Result<ArrayRef> {
    let DType::Struct(fields, _) = input.dtype() else {
        panic!("record");
    };
    let fields = fields
        .names()
        .iter()
        .zip(fields.fields())
        .map(|(name, dtype)| (name.to_string(), dtype))
        .collect::<Vec<_>>();
    let mut binder = Binder::new(session)?;
    let bound = binder.expression(expression, &fields, 0)?;
    session.with_native_execution_context(&CancellationToken::default(), |context| {
        bound.evaluate(input, context)
    })
}
fn numbers(array: &ArrayRef) -> Vec<Option<f64>> {
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    (0..array.len())
        .map(|row| {
            array
                .execute_scalar(row, &mut execution)
                .unwrap()
                .as_primitive()
                .as_::<f64>()
        })
        .collect()
}
fn list_numbers(array: &ArrayRef) -> Vec<Option<Vec<Option<f64>>>> {
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    let list =
        crate::local_primitives::native_list::Column::new(array.clone(), &mut execution).unwrap();
    (0..array.len())
        .map(|row| {
            list.coordinates(row, &mut execution)
                .unwrap()
                .map(|(from, count)| {
                    (from..from + count)
                        .map(|index| {
                            list.elements
                                .execute_scalar(index, &mut execution)
                                .unwrap()
                                .as_primitive()
                                .as_::<f64>()
                        })
                        .collect()
                })
        })
        .collect()
}

#[test]
fn native_vector_widths_encoded_children_masks_and_duplicate_spans_agree() {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    for width in [PType::F16, PType::F32, PType::F64] {
        let values: ArrayRef = match width {
            PType::F16 => PrimitiveArray::from_option_iter(
                [Some(3.0), Some(4.0), None, Some(0.0)]
                    .map(|value| value.map(vortex::array::dtype::half::f16::from_f64)),
            )
            .into_array(),
            PType::F32 => {
                PrimitiveArray::from_option_iter([Some(3.0f32), Some(4.0), None, Some(0.0)])
                    .into_array()
            }
            _ => PrimitiveArray::from_option_iter([Some(3.0f64), Some(4.0), None, Some(0.0)])
                .into_array(),
        };
        let dictionary = DictArray::try_new(
            PrimitiveArray::from_iter([0u8, 1, 2, 3]).into_array(),
            values.clone(),
        )
        .unwrap()
        .into_array();
        let chunked = ChunkedArray::try_new(
            vec![values.slice(0..1).unwrap(), values.slice(1..4).unwrap()],
            values.dtype().clone(),
        )
        .unwrap()
        .into_array();
        for child in [values, dictionary, chunked] {
            let input = batch(
                &[
                    (
                        "v",
                        list(
                            child,
                            &[(0, 2), (2, 2), (0, 0), (0, 2), (0, 2)],
                            Validity::from_iter([true, true, true, false, true]),
                        ),
                    ),
                    (
                        "w",
                        list(
                            PrimitiveArray::from_iter([3.0f64, 4.0]).into_array(),
                            &[(0, 2), (0, 2), (0, 0), (0, 2), (0, 2)],
                            Validity::NonNullable,
                        ),
                    ),
                ],
                5,
            );
            for (name, expected) in [
                (
                    "vector_dot",
                    vec![Some(25.0), None, Some(0.0), None, Some(25.0)],
                ),
                (
                    "vector_l2_distance",
                    vec![Some(0.0), None, Some(0.0), None, Some(0.0)],
                ),
                (
                    "vector_cosine_similarity",
                    vec![Some(1.0), None, None, None, Some(1.0)],
                ),
            ] {
                assert_eq!(
                    numbers(
                        &evaluate(&session, &call(name, vec![col("v"), col("w")]), &input).unwrap()
                    ),
                    expected,
                    "{name} {width}"
                );
            }
            assert_eq!(
                numbers(&evaluate(&session, &call("vector_norm", vec![col("v")]), &input).unwrap()),
                vec![Some(5.0), None, Some(0.0), None, Some(5.0)]
            );
            assert_eq!(
                list_numbers(
                    &evaluate(&session, &call("vector_normalize", vec![col("v")]), &input).unwrap()
                ),
                vec![
                    Some(vec![Some(0.6), Some(0.8)]),
                    None,
                    None,
                    None,
                    Some(vec![Some(0.6), Some(0.8)])
                ]
            );
        }
    }
}

#[test]
fn native_vector_nulls_shield_payloads_but_not_dimensions_or_argument_errors() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let null_component = list(
        PrimitiveArray::from_option_iter([Some(f64::INFINITY), None]).into_array(),
        &[(0, 2)],
        Validity::NonNullable,
    );
    let two = list(
        PrimitiveArray::from_iter([1.0f64, 2.0]).into_array(),
        &[(0, 2)],
        Validity::NonNullable,
    );
    let input = batch(&[("v", null_component.clone()), ("w", two)], 1);
    assert_eq!(
        numbers(
            &evaluate(
                &session,
                &call("vector_dot", vec![col("v"), col("w")]),
                &input
            )
            .unwrap()
        ),
        vec![None]
    );
    let one = list(
        PrimitiveArray::from_iter([1.0f64]).into_array(),
        &[(0, 1)],
        Validity::NonNullable,
    );
    let mismatch = batch(&[("v", null_component), ("w", one)], 1);
    assert!(
        evaluate(
            &session,
            &call("vector_dot", vec![col("v"), col("w")]),
            &mismatch
        )
        .unwrap_err()
        .to_string()
        .contains("dimensions differ")
    );
    let null_parent = list(
        PrimitiveArray::from_iter([f64::INFINITY]).into_array(),
        &[(0, 1)],
        Validity::AllInvalid,
    );
    let input = batch(&[("v", null_parent)], 1);
    assert_eq!(
        numbers(&evaluate(&session, &call("vector_norm", vec![col("v")]), &input).unwrap()),
        vec![None]
    );
    let error = evaluate(
        &session,
        &call(
            "vector_dot",
            vec![col("v"), list_literal(&[Some(f64::INFINITY)])],
        ),
        &input,
    )
    .unwrap_err();
    assert!(error.to_string().contains("nonfinite floating literals"));
}

#[test]
fn native_vector_exact_cancellation_and_scaled_ratios_avoid_intermediate_overflow() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let input = batch(&[], 1);
    let a = list_literal(&[Some(f64::MAX), Some(f64::MAX)]);
    let b = list_literal(&[Some(f64::MAX), Some(-f64::MAX)]);
    assert_eq!(
        numbers(&evaluate(&session, &call("vector_dot", vec![a.clone(), b]), &input).unwrap()),
        vec![Some(0.0)]
    );
    assert_eq!(
        numbers(
            &evaluate(
                &session,
                &call("vector_l2_distance", vec![a.clone(), a.clone()]),
                &input
            )
            .unwrap()
        ),
        vec![Some(0.0)]
    );
    assert!(
        evaluate(&session, &call("vector_norm", vec![a.clone()]), &input)
            .unwrap_err()
            .to_string()
            .contains("nonfinite vector result")
    );
    assert_eq!(
        numbers(
            &evaluate(
                &session,
                &call("vector_cosine_similarity", vec![a.clone(), a.clone()]),
                &input
            )
            .unwrap()
        ),
        vec![Some(1.0)]
    );
    let values =
        list_numbers(&evaluate(&session, &call("vector_normalize", vec![a]), &input).unwrap());
    for value in values[0].as_ref().unwrap() {
        assert!((value.unwrap() - std::f64::consts::FRAC_1_SQRT_2).abs() < 4.0 * f64::EPSILON);
    }
    let tiny = list_literal(&[Some(f64::from_bits(1))]);
    assert_eq!(
        numbers(&evaluate(&session, &call("vector_norm", vec![tiny.clone()]), &input).unwrap()),
        vec![Some(f64::from_bits(1))]
    );
    assert_eq!(
        list_numbers(&evaluate(&session, &call("vector_normalize", vec![tiny]), &input).unwrap()),
        vec![Some(vec![Some(1.0)])]
    );
}

#[test]
fn native_vector_extended_classification_and_raw_selection_keep_distinct_contracts() {
    let nan = f64::from_bits(0xfff8_0000_0000_0042);
    for policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
        let session = ResidentVortexSession::new_with_float_policy(16 << 20, 1, policy).unwrap();
        let array = list(
            PrimitiveArray::from_iter([nan, -0.0, f64::INFINITY]).into_array(),
            &[(0, 3)],
            Validity::NonNullable,
        );
        let input = batch(&[("v", array)], 1);
        let selected = evaluate(
            &session,
            &call("vector_element", vec![col("v"), lit(ScalarValue::Int64(0))]),
            &input,
        )
        .unwrap();
        assert_eq!(numbers(&selected)[0].unwrap().to_bits(), nan.to_bits());
        let sliced = evaluate(
            &session,
            &call(
                "vector_slice",
                vec![
                    col("v"),
                    lit(ScalarValue::Int64(-3)),
                    lit(ScalarValue::UInt64(u64::MAX)),
                ],
            ),
            &input,
        )
        .unwrap();
        let values = list_numbers(&sliced);
        assert_eq!(
            values[0]
                .as_ref()
                .unwrap()
                .iter()
                .map(|v| v.unwrap().to_bits())
                .collect::<Vec<_>>(),
            vec![nan.to_bits(), (-0.0f64).to_bits(), f64::INFINITY.to_bits()]
        );
        let norm = evaluate(&session, &call("vector_norm", vec![col("v")]), &input);
        if policy == FloatPolicy::Strict {
            assert!(norm.is_err());
        } else {
            assert_eq!(
                numbers(&norm.unwrap())[0].unwrap().to_bits(),
                shardloom_core::float_policy::CANONICAL_F64_NAN_BITS
            );
        }
    }
}

#[test]
fn native_vector_literal_broadcast_retains_one_child_and_releases_last_owner() {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let input = batch(&[], 4000);
    let literal = list_literal(&vec![Some(0.25); 1536]);
    let result = evaluate(&session, &call("vector_values", vec![literal]), &input).unwrap();
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    let column =
        crate::local_primitives::native_list::Column::new(result.clone(), &mut execution).unwrap();
    assert_eq!(column.elements.len(), 1536);
    assert_eq!(
        column.coordinates(0, &mut execution).unwrap(),
        Some((0, 1536))
    );
    assert_eq!(
        column.coordinates(3999, &mut execution).unwrap(),
        Some((0, 1536))
    );
    let child = column
        .elements
        .clone()
        .execute::<PrimitiveArray>(&mut execution)
        .unwrap();
    let bytes = child.to_buffer::<f64>();
    drop((column, child, result, input, session));
    assert!(memory.snapshot().reserved_bytes > 0);
    assert!(memory.snapshot().reserved_bytes < 32768);
    assert!(
        bytes
            .iter()
            .all(|value| value.to_bits() == 0.25_f64.to_bits())
    );
    drop((bytes, execution));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_vector_binding_rejects_unadmitted_storage_and_conflicting_identity() {
    use crate::native_embedding::{Embedding, Metadata};
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let f64type = DType::Primitive(PType::F64, Nullability::Nullable);
    let embedding = |metadata: &str| {
        DType::Extension(
            ExtDType::<Embedding>::try_new(
                Metadata::parse(metadata).unwrap(),
                DType::FixedSizeList(f64type.clone().into(), 2, Nullability::Nullable),
            )
            .unwrap()
            .erased(),
        )
    };
    let bind = |name: &str, types: Vec<DType>| {
        let mut binder = Binder::new(&session).unwrap();
        let fields = types
            .into_iter()
            .enumerate()
            .map(|(i, t)| (format!("v{i}"), t))
            .collect::<Vec<_>>();
        let args = (0..fields.len()).map(|i| col(&format!("v{i}"))).collect();
        binder.expression(&call(name, args), &fields, 0)
    };
    let a = embedding(r#"{"version":1,"space_id":"Ω","model_id":"m"}"#);
    assert!(bind("vector_dot", vec![a.clone(), a.clone()]).is_ok());
    for metadata in [
        r#"{"version":1}"#,
        r#"{"version":1,"space_id":"Ω"}"#,
        r#"{"version":1,"space_id":"ω","model_id":"m"}"#,
    ] {
        assert!(bind("vector_dot", vec![a.clone(), embedding(metadata)]).is_err());
    }
    assert!(
        bind(
            "vector_dot",
            vec![embedding(r#"{"version":1}"#), embedding(r#"{"version":1}"#)]
        )
        .is_err()
    );
    assert!(bind("vector_norm", vec![embedding(r#"{"version":1}"#)]).is_ok());
    assert!(
        bind(
            "vector_dot",
            vec![
                a,
                DType::List(f64type.clone().into(), Nullability::Nullable)
            ]
        )
        .is_err()
    );
    assert!(
        bind(
            "vector_dot",
            vec![
                DType::FixedSizeList(f64type.clone().into(), 2, Nullability::Nullable),
                DType::FixedSizeList(f64type.into(), 3, Nullability::Nullable)
            ]
        )
        .is_err()
    );
    for child in [
        DType::Primitive(PType::I64, Nullability::Nullable),
        DType::Bool(Nullability::Nullable),
        DType::Utf8(Nullability::Nullable),
        DType::Decimal(DecimalDType::new(8, 2), Nullability::Nullable),
        DType::List(
            DType::Primitive(PType::F64, Nullability::Nullable).into(),
            Nullability::Nullable,
        ),
    ] {
        for name in [
            "vector_dimension",
            "vector_values",
            "vector_element",
            "vector_slice",
            "vector_dot",
            "vector_l2_distance",
            "vector_cosine_similarity",
            "vector_norm",
            "vector_normalize",
        ] {
            let mut args = vec![DType::List(child.clone().into(), Nullability::Nullable)];
            if matches!(name, "vector_element" | "vector_slice") {
                args.push(DType::Primitive(PType::I64, Nullability::Nullable));
            } else if matches!(
                name,
                "vector_dot" | "vector_l2_distance" | "vector_cosine_similarity"
            ) {
                args.push(DType::List(
                    DType::Primitive(PType::F64, Nullability::Nullable).into(),
                    Nullability::Nullable,
                ));
            }
            assert!(bind(name, args).is_err(), "{name} {child}");
        }
    }
}
