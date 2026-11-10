//! Independent value, null, representation and lifetime checks for Variant expressions.

use super::*;
use crate::resident_session::ResidentVortexSession;
use parquet_variant::{Variant as PqValue, VariantBuilder};
use shardloom_core::{ColumnRef, ExprId, FloatPolicy};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::chunked::ChunkedArrayExt as _,
        arrays::{
            ChunkedArray, ConstantArray, DictArray, PrimitiveArray, StructArray, VarBinArray,
            VariantArray,
        },
        dtype::FieldNames,
        validity::Validity,
    },
    encodings::parquet_variant::ParquetVariant,
};

fn expr(kind: ExpressionKind) -> Input {
    Input::new(ExprId::new("variant-test").unwrap(), kind)
}
fn col() -> Input {
    expr(ExpressionKind::Column(ColumnRef::new("v").unwrap()))
}
fn text(value: &str) -> Input {
    expr(ExpressionKind::Literal(ScalarValue::Utf8(value.into())))
}
fn call(name: &str, args: Vec<Input>) -> Input {
    expr(ExpressionKind::FunctionCall {
        name: name.into(),
        args,
    })
}
fn get(path: &str, target: Option<&str>) -> Input {
    let mut args = vec![col(), text(path)];
    if let Some(target) = target {
        args.push(text(target));
    }
    call(
        if target.is_some() {
            "try_variant_get"
        } else {
            "variant_get"
        },
        args,
    )
}
fn batch(value: &ArrayRef) -> ArrayRef {
    StructArray::new(
        FieldNames::from(["v"]),
        vec![value.clone()],
        value.len(),
        Validity::NonNullable,
    )
    .into_array()
}
fn bind(
    session: &ResidentVortexSession,
    expression: &Input,
    source: &ArrayRef,
) -> Result<Expression> {
    Binder::new(session)?.expression(expression, &[("v".into(), source.dtype().clone())], 0)
}
fn evaluate(
    session: &ResidentVortexSession,
    expression: &Input,
    source: &ArrayRef,
) -> Result<ArrayRef> {
    let bound = bind(session, expression, source)?;
    session.with_native_execution_context(&CancellationToken::default(), |context| {
        bound.evaluate(&batch(source), context)
    })
}
fn encoded(metadata: Vec<Vec<u8>>, values: Vec<Vec<u8>>, validity: Validity) -> ArrayRef {
    ParquetVariant::try_new(
        validity,
        VarBinArray::from_vec(metadata, DType::Binary(Nullability::NonNullable)).into_array(),
        Some(VarBinArray::from_vec(values, DType::Binary(Nullability::NonNullable)).into_array()),
        None,
    )
    .unwrap()
    .into_array()
}
fn raw(values: &[PqValue<'_, '_>], validity: Validity) -> ArrayRef {
    let (metadata, values) = values
        .iter()
        .map(|value| VariantBuilder::new().with_value(value.clone()).finish())
        .unzip();
    encoded(metadata, values, validity)
}
fn objects(values: &[PqValue<'_, '_>], validity: Validity) -> ArrayRef {
    let (metadata, values) = values
        .iter()
        .map(|value| {
            let mut builder = VariantBuilder::new();
            let mut object = builder.new_object();
            object.insert("a.b[猫]", value.clone());
            object.finish();
            builder.finish()
        })
        .unzip();
    encoded(metadata, values, validity)
}
fn integers(array: &ArrayRef) -> Vec<Option<i64>> {
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    (0..array.len())
        .map(|row| {
            array
                .execute_scalar(row, &mut execution)
                .unwrap()
                .as_primitive()
                .as_::<i64>()
        })
        .collect()
}
fn booleans(array: &ArrayRef) -> Vec<Option<bool>> {
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    (0..array.len())
        .map(|row| {
            array
                .execute_scalar(row, &mut execution)
                .unwrap()
                .as_bool()
                .value()
        })
        .collect()
}

#[test]
fn native_variant_expression_paths_null_kinds_and_wrapped_selection() {
    let source = objects(
        &[
            PqValue::Int64(i64::MIN),
            PqValue::Null,
            PqValue::Int64(7),
            PqValue::from("bad"),
        ],
        Validity::from_iter([true, true, false, true]),
    );
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_option_iter([Some(3_u32), Some(0), None, Some(1), Some(2)])
            .into_array(),
        source.clone(),
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let path = "$[\"a.b[猫]\"]";
    assert_eq!(
        integers(&evaluate(&session, &get(path, Some("bigint")), &source).unwrap()),
        vec![Some(i64::MIN), None, None, None]
    );
    assert_eq!(
        integers(&evaluate(&session, &get(path, Some("int64")), &dictionary).unwrap()),
        vec![None, Some(i64::MIN), None, None, None]
    );
    let chunks = ChunkedArray::try_new(
        vec![source.slice(0..1).unwrap(), source.slice(1..4).unwrap()],
        source.dtype().clone(),
    )
    .unwrap()
    .into_array();
    let nested_null = call("is_variant_null", vec![get(path, None)]);
    for input in [source, chunks] {
        assert_eq!(
            booleans(&evaluate(&session, &nested_null, &input).unwrap()),
            vec![Some(false), Some(true), Some(false), Some(false)]
        );
        let output = evaluate(&session, &get("$.absent", None), &input).unwrap();
        assert_eq!(output.dtype(), &DType::Variant(Nullability::Nullable));
        let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
        assert!((0..output.len()).all(|row| output.is_invalid(row, &mut execution).unwrap()));
        assert_eq!(
            integers(&evaluate(&session, &get("$[0]", Some("int64")), &input).unwrap()),
            vec![None; 4]
        );
    }
    assert_eq!(
        integers(
            &evaluate(
                &session,
                &get(path, Some("int64")),
                &dictionary.slice(1..4).unwrap()
            )
            .unwrap()
        ),
        vec![Some(i64::MIN), None, None]
    );
}

#[test]
fn native_variant_expression_raw_list_and_uuid_preserve_exact_domains() {
    // Format-v1 fixtures independent of VariantBuilder: a list containing an
    // embedded null and unrestricted 128-bit UUID, including non-RFC bits.
    let uuid: Vec<u8> = std::iter::once(0x50).chain(0_u8..16).collect();
    let mut list = vec![0x03, 2, 0, 1, 18, 0];
    list.extend(&uuid);
    let source = encoded(vec![vec![1, 0, 0]], vec![list], Validity::AllValid);
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let direct = evaluate(&session, &get("$[1]", Some("uuid")), &source).unwrap();
    let nested = call(
        "try_variant_get",
        vec![get("$[1]", None), text("$"), text("uuid")],
    );
    let carried = evaluate(&session, &nested, &source).unwrap();
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    for output in [direct, carried] {
        assert!(crate::native_uuid::is_uuid(output.dtype()));
        let values = crate::native_uuid::Values::new(&output, &mut execution).unwrap();
        assert_eq!(
            values.value(0).unwrap(),
            Some(std::array::from_fn(|i| u8::try_from(i).unwrap()))
        );
    }
    assert_eq!(
        booleans(
            &evaluate(
                &session,
                &call("is_variant_null", vec![get("$[0]", None)]),
                &source
            )
            .unwrap()
        ),
        vec![Some(true)]
    );
    assert_eq!(
        integers(&evaluate(&session, &get("$[2]", Some("int64")), &source).unwrap()),
        vec![None]
    );
}

#[test]
fn native_variant_expression_canonical_shredded_and_raw_children_agree() {
    // Both raw and shredded field names belong to the encoded dictionary.
    let name = "a.b[猫]".as_bytes();
    let mut metadata = vec![
        0x11,
        2,
        0,
        u8::try_from(name.len()).unwrap(),
        u8::try_from(name.len() + 5).unwrap(),
    ];
    metadata.extend(name);
    metadata.extend(b"score");
    let mut number = vec![0x18];
    number.extend(9_i64.to_le_bytes());
    let values = [number, vec![0]]
        .into_iter()
        .map(|value| {
            let mut object = vec![0x02, 1, 0, 0, u8::try_from(value.len()).unwrap()];
            object.extend(value);
            object
        })
        .collect();
    let core = encoded(vec![metadata; 2], values, Validity::AllValid);
    let typed = StructArray::new(
        FieldNames::from(["score"]),
        vec![PrimitiveArray::from_iter([7_i64, -8]).into_array()],
        2,
        Validity::NonNullable,
    )
    .into_array();
    let source = VariantArray::try_new(core, Some(typed))
        .unwrap()
        .into_array();
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    assert_eq!(
        integers(&evaluate(&session, &get("$.score", Some("int64")), &source).unwrap()),
        vec![Some(7), Some(-8)]
    );
    assert_eq!(
        integers(&evaluate(&session, &get("$[\"a.b[猫]\"]", Some("int64")), &source).unwrap()),
        vec![Some(9), None]
    );
    assert_eq!(
        booleans(
            &evaluate(
                &session,
                &call("is_variant_null", vec![get("$[\"a.b[猫]\"]", None)]),
                &source
            )
            .unwrap()
        ),
        vec![Some(false), Some(true)]
    );
}

#[test]
fn native_variant_expression_binding_refuses_invalid_controls_before_execution() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let source = raw(&[PqValue::Int64(5)], Validity::AllValid);
    for expression in [
        call("variant_get", vec![]),
        call("variant_get", vec![col()]),
        call("variant_get", vec![col(), col()]),
        call("try_variant_get", vec![col(), text("$"), col()]),
        call(
            "variant_get",
            vec![col(), expr(ExpressionKind::Literal(ScalarValue::Null))],
        ),
        get("$[-1]", None),
        get("$.*", None),
        get("$..x", None),
        get("$[1e3]", None),
        get("$", Some("duration(s)")),
        get("$", Some("struct<x:int>")),
        get("$", Some("decimal(40,2)")),
        get("$", Some("decimal(2,3)")),
    ] {
        let error = bind(&session, &expression, &source)
            .err()
            .expect("invalid control admitted");
        assert!(error.to_string().contains("no fallback"), "{error}");
    }
    assert!(
        bind(
            &session,
            &get("$", None),
            &PrimitiveArray::from_iter([1_i64]).into_array()
        )
        .is_err()
    );
    for target in [
        "bool",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float16",
        "float32",
        "float64",
        "string",
        "binary",
        "decimal(10,2)",
        "date",
        "timestamp",
        "timestamp(ns,'UTC')",
        "uuid",
        "variant",
    ] {
        let bound = bind(&session, &get("$", Some(target)), &source).unwrap();
        assert!(bound.dtype.is_nullable(), "{target}");
    }
}

#[test]
fn native_variant_expression_empty_sql_null_and_tolerant_casts() {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let nulls = ConstantArray::new(Scalar::null(DType::Null), 3).into_array();
    assert_eq!(
        booleans(&evaluate(&session, &call("is_variant_null", vec![col()]), &nulls).unwrap()),
        vec![Some(false); 3]
    );
    assert_eq!(
        integers(&evaluate(&session, &get("$", Some("int64")), &nulls).unwrap()),
        vec![None; 3]
    );
    let source = raw(
        &[
            PqValue::Int64(5),
            PqValue::Int64(128),
            PqValue::from("6"),
            PqValue::from("bad"),
            PqValue::Null,
        ],
        Validity::AllValid,
    );
    let empty = evaluate(
        &session,
        &get("$", Some("uuid")),
        &raw(&[], Validity::AllValid),
    )
    .unwrap();
    assert!(empty.is_empty());
    assert!(crate::native_uuid::is_uuid(empty.dtype()));
    let integers = evaluate(&session, &get("$", Some("int8")), &source).unwrap();
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    assert_eq!(
        (0..integers.len())
            .map(|row| integers
                .execute_scalar(row, &mut execution)
                .unwrap()
                .as_primitive()
                .as_::<i8>())
            .collect::<Vec<_>>(),
        vec![Some(5), None, None, None, None]
    );
}

#[test]
fn native_variant_expression_all_sql_null_keeps_typed_outputs() {
    let source = raw(&[PqValue::Null, PqValue::Null], Validity::AllInvalid);
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    for path in ["$", "$.missing", "$[0]"] {
        for target in [None, Some("variant"), Some("uuid"), Some("int64")] {
            let expression = get(path, target);
            let output = evaluate(&session, &expression, &source).unwrap();
            assert_eq!(
                output.dtype(),
                &bind(&session, &expression, &source).unwrap().dtype
            );
            assert_eq!(output.len(), source.len());
            assert!(output.all_invalid(&mut execution).unwrap());
        }
    }
    assert_eq!(
        booleans(&evaluate(&session, &call("is_variant_null", vec![col()]), &source).unwrap()),
        vec![Some(false); 2]
    );
}

#[test]
fn native_variant_expression_float_policy_observes_only_typed_extractions() {
    let source = raw(
        &[PqValue::Double(f64::INFINITY), PqValue::Double(-0.0)],
        Validity::AllValid,
    );
    for policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
        let session = ResidentVortexSession::new_with_float_policy(32 << 20, 1, policy).unwrap();
        assert!(evaluate(&session, &get("$", None), &source).is_ok());
        let typed = evaluate(&session, &get("$", Some("float64")), &source);
        if policy == FloatPolicy::Strict {
            assert!(typed.is_err());
        } else {
            let output = typed.unwrap();
            let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
            assert_eq!(
                output
                    .execute_scalar(0, &mut execution)
                    .unwrap()
                    .as_primitive()
                    .as_::<f64>(),
                Some(f64::INFINITY)
            );
            assert_eq!(
                output
                    .execute_scalar(1, &mut execution)
                    .unwrap()
                    .as_primitive()
                    .as_::<f64>()
                    .unwrap()
                    .to_bits(),
                (-0.0_f64).to_bits()
            );
        }
    }
}

#[test]
fn native_variant_expression_denial_cancellation_and_bare_result_owner() {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let source = raw(&vec![PqValue::Int64(7); 513], Validity::AllValid);
    let bound = bind(&session, &get("$", Some("int64")), &source).unwrap();
    let input = batch(&source);
    let baseline = memory.snapshot().reserved_bytes;
    for available in [0, 512, 8192] {
        let held = memory
            .reserve(memory.snapshot().limit_bytes - baseline - available)
            .unwrap();
        let before = memory.snapshot().reserved_bytes;
        let error = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                bound.evaluate(&input, context)
            })
            .unwrap_err();
        assert!(error.to_string().contains("memory"), "{error}");
        assert_eq!(memory.snapshot().reserved_bytes, before);
        drop(held);
    }
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    assert!(
        session
            .with_native_execution_context(&cancellation, |context| bound.evaluate(&input, context))
            .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            bound.evaluate(&input, context)
        })
        .unwrap();
    assert_eq!(integers(&output), vec![Some(7); 513]);
    let slice = output.slice(255..258).unwrap();
    let child = output
        .as_::<vortex::array::arrays::Chunked>()
        .chunk(0)
        .clone();
    drop((output, bound, session));
    assert!(memory.snapshot().reserved_bytes > 0);
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    // Retain an actual result buffer. Concatenating chunks in a fresh external
    // Vortex session would create a different, caller-owned allocation.
    let primitive = child.execute::<PrimitiveArray>(&mut execution).unwrap();
    let buffer = primitive.to_buffer::<i64>();
    drop((primitive, slice, execution));
    assert!(memory.snapshot().reserved_bytes > 0);
    assert_eq!(buffer.len(), 256);
    assert!(buffer.iter().all(|value| *value == 7));
    drop(buffer);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_variant_expression_raw_and_shredded_cast_rules_are_identical() {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let raw = raw(
        &[PqValue::from("6"), PqValue::from("bad")],
        Validity::AllValid,
    );
    let shredded = ParquetVariant::try_new(
        Validity::AllValid,
        VarBinArray::from_vec(
            vec![vec![1, 0, 0]; 2],
            DType::Binary(Nullability::NonNullable),
        )
        .into_array(),
        None,
        Some(
            VarBinArray::from_vec(vec!["6", "bad"], DType::Utf8(Nullability::NonNullable))
                .into_array(),
        ),
    )
    .unwrap()
    .into_array();
    let core = ParquetVariant::try_new(
        Validity::AllValid,
        VarBinArray::from_vec(
            vec![vec![1, 0, 0]; 2],
            DType::Binary(Nullability::NonNullable),
        )
        .into_array(),
        Some(
            ConstantArray::new(Scalar::null(DType::Binary(Nullability::Nullable)), 2).into_array(),
        ),
        None,
    )
    .unwrap()
    .into_array();
    let canonical = VariantArray::try_new(
        core,
        Some(
            VarBinArray::from_vec(vec!["6", "bad"], DType::Utf8(Nullability::NonNullable))
                .into_array(),
        ),
    )
    .unwrap()
    .into_array();
    // Physical typed strings must not invoke Arrow's broader string-to-number
    // cast when raw Variant strings would return NULL.
    for source in [raw, shredded, canonical] {
        assert_eq!(
            integers(&evaluate(&session, &get("$", Some("int64")), &source).unwrap()),
            vec![None, None]
        );
        let output = evaluate(&session, &get("$", Some("string")), &source).unwrap();
        let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
        assert_eq!(
            output
                .execute_scalar(0, &mut execution)
                .unwrap()
                .as_utf8()
                .value()
                .unwrap()
                .as_str(),
            "6"
        );
    }
}

#[test]
fn native_variant_expression_malformed_selected_value_fails_and_releases() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let source = encoded(vec![vec![1, 0, 0]], vec![vec![0x18, 1]], Validity::AllValid);
    for expression in [get("$.missing", None), call("is_variant_null", vec![col()])] {
        assert!(evaluate(&session, &expression, &source).is_err());
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_variant_expression_aliased_binary_values_are_admitted_at_expanded_size() {
    let bytes = vec![0x5a; 4096];
    let mut binary = vec![0x3c];
    binary.extend(u32::try_from(bytes.len()).unwrap().to_le_bytes());
    binary.extend(bytes);
    let plain = encoded(
        vec![vec![1, 0, 0]],
        vec![binary.clone()],
        Validity::AllValid,
    );
    // Eight distinct object keys alias one large binary payload. Valid format
    // offsets do not imply that scalar materialization will share those bytes.
    let mut metadata = vec![0x11, 8];
    metadata.extend(0_u8..=8);
    metadata.extend(b"abcdefgh");
    let mut object = vec![0x06, 8];
    object.extend(0_u8..8);
    object.extend([0_u8; 16]);
    object.extend(u16::try_from(binary.len()).unwrap().to_le_bytes());
    object.extend(binary);
    let aliases = encoded(vec![metadata], vec![object], Validity::AllValid);
    let inspect = call("is_variant_null", vec![col()]);
    let small = ResidentVortexSession::new(2 << 20, 1).unwrap();
    assert_eq!(
        booleans(&evaluate(&small, &inspect, &plain).unwrap()),
        vec![Some(false)]
    );
    let error = evaluate(&small, &inspect, &aliases).unwrap_err();
    assert!(error.to_string().contains("reservation denied"), "{error}");
    assert_eq!(small.memory().snapshot().reserved_bytes, 0);
    let large = ResidentVortexSession::new(16 << 20, 1).unwrap();
    assert_eq!(
        booleans(&evaluate(&large, &inspect, &aliases).unwrap()),
        vec![Some(false)]
    );
    assert!(large.memory().snapshot().peak_reserved_bytes > 2 << 20);
    assert_eq!(large.memory().snapshot().reserved_bytes, 0);
}
