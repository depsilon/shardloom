//! Admission witnesses for an encoded text consumer, never a performance claim.

use super::*;
use crate::{
    local_primitives::{
        contains_literal_like_pattern,
        native_relational_expression::{Expression, Kind, scalar::Function},
        native_relational_observation::Session,
    },
    resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _, VortexSessionExecute as _,
        arrays::{BoolArray, ConstantArray, PrimitiveArray, VarBinArray, bool::BoolArrayExt as _},
        dtype::{DType, Nullability},
        scalar::Scalar,
        scalar_fn::fns::like::{LikeKernel, LikeOptions},
    },
    buffer::Buffer,
    encodings::fsst::FSST,
    session::VortexSession,
};

#[cfg(feature = "universal-format-io")]
#[path = "local_primitive_relational_fsst_existing_inputs_tests.rs"]
mod existing_inputs;

#[test]
fn fsst_admission_observer_is_session_scoped() {
    use vortex::encodings::fsst::like_observation::LikeObservation;

    let first = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let second = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let first_work = LikeObservation::default();
    let second_work = LikeObservation::default();
    let array = encoded(&[Some(vec![0xff, b'a']), Some(vec![0xff, b'b'])], &[1, 1]);
    first
        .with_native_execution_context(&CancellationToken::default(), |context| {
            context.native_session().register(first_work.clone());
            assert!(direct(&array, "a", false, context)?.is_some());
            Ok(())
        })
        .unwrap();
    let first_snapshot = first_work.snapshot();
    assert_eq!(first_snapshot.calls, 1);
    assert_eq!(first_snapshot.rows, 2);
    assert!(!first_snapshot.overflowed);
    second
        .with_native_execution_context(&CancellationToken::default(), |context| {
            // An unregistered session must not affect another request's observer.
            assert!(direct(&array, "a", false, context)?.is_some());
            assert_eq!(first_work.snapshot(), first_snapshot);
            context.native_session().register(second_work.clone());
            // Unsupported patterns are not successful calls.
            assert!(direct(&array, &"x".repeat(255), false, context)?.is_none());
            assert_eq!(second_work.snapshot().calls, 0);
            assert!(direct(&array, "b", false, context)?.is_some());
            Ok(())
        })
        .unwrap();
    assert_eq!(second_work.snapshot(), first_snapshot);
    assert_eq!(first_work.snapshot(), first_snapshot);
}

fn encoded(codes: &[Option<Vec<u8>>], lengths: &[u32]) -> ArrayRef {
    let nullable = if codes.iter().any(Option::is_none) {
        Nullability::Nullable
    } else {
        Nullability::NonNullable
    };
    FSST::try_new(
        DType::Utf8(nullable),
        Buffer::empty(),
        Buffer::empty(),
        VarBinArray::from_iter(codes.iter().map(Option::as_deref), DType::Binary(nullable)),
        PrimitiveArray::from_iter(lengths.iter().copied()).into_array(),
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap()
    .into_array()
}

fn escaped(value: &[u8]) -> Vec<u8> {
    value.iter().flat_map(|byte| [u8::MAX, *byte]).collect()
}

fn fixture(values: &[Option<&str>]) -> ArrayRef {
    encoded(
        &values
            .iter()
            .map(|v| v.map(|v| escaped(v.as_bytes())))
            .collect::<Vec<_>>(),
        &values
            .iter()
            .map(|v| v.map_or(0, |v| u32::try_from(v.len()).unwrap()))
            .collect::<Vec<_>>(),
    )
}

fn shared(
    array: &ArrayRef,
    needle: &str,
    prefix: bool,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let input = StructArray::try_new(
        FieldNames::from(["value"]),
        vec![array.clone()],
        array.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    let expression = Expression::new(
        DType::Bool(array.dtype().nullability()),
        Kind::Function {
            function: if prefix {
                Function::StartsWith
            } else {
                Function::Contains
            },
            args: vec![
                Expression::new(array.dtype().clone(), Kind::Column("value".into())),
                Expression::new(
                    DType::Utf8(Nullability::NonNullable),
                    Kind::Literal(Scalar::from(needle)),
                ),
            ],
        },
    );
    assert!(expression.filter_recipe().is_none());
    expression.evaluate(&input, context)
}

fn direct(
    array: &ArrayRef,
    needle: &str,
    prefix: bool,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ArrayRef>> {
    let Some(encoded) = array.as_opt::<FSST>() else {
        return Ok(None);
    };
    let mut pattern = contains_literal_like_pattern(needle);
    if prefix {
        pattern.remove(0);
    }
    <FSST as LikeKernel>::like(
        encoded,
        &ConstantArray::new(pattern, array.len()).into_array(),
        LikeOptions {
            negated: false,
            case_insensitive: false,
        },
        &mut context.native_session().create_execution_ctx(),
    )
    .map_err(vortex_error)
}

fn booleans(array: ArrayRef, context: &NativeExecutionContext<'_>) -> Result<Vec<Option<bool>>> {
    let mut ctx = context.native_session().create_execution_ctx();
    let array = array.execute::<BoolArray>(&mut ctx).map_err(vortex_error)?;
    let valid = array
        .validity()
        .map_err(vortex_error)?
        .execute_mask(array.len(), &mut ctx)
        .map_err(vortex_error)?;
    let bits = array.to_bit_buffer();
    Ok((0..array.len())
        .map(|row| valid.value(row).then(|| bits.value(row)))
        .collect())
}

fn emit(value: &serde_json::Value) {
    println!("FSST_ADMISSION {value}");
}

#[test]
fn fsst_admission_raw_matching_does_not_preserve_shared_validation() {
    for (name, codes, lengths, diagnostic) in [
        (
            "understated_length",
            vec![Some(escaped(b"a"))],
            vec![0],
            "row length",
        ),
        (
            "overstated_length",
            vec![Some(escaped(b"a"))],
            vec![2],
            "row length",
        ),
        (
            "truncated_escape",
            vec![Some(vec![u8::MAX])],
            vec![0],
            "truncated escape",
        ),
        (
            "unpopulated_symbol",
            vec![Some(vec![0])],
            vec![0],
            "no populated symbol",
        ),
        ("invalid_utf8", vec![Some(escaped(&[0xff]))], vec![1], "utf"),
    ] {
        let array = encoded(&codes, &lengths);
        for prefix in [false, true] {
            let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let error = shared(&array, "a", prefix, context)
                        .unwrap_err()
                        .to_string();
                    assert!(error.to_lowercase().contains(diagnostic), "{name}: {error}");
                    let result = booleans(direct(&array, "a", prefix, context)?.unwrap(), context)?;
                    assert_eq!(result.len(), 1);
                    emit(
                        &serde_json::json!({"case":name,"prefix":prefix,"shared_error":error,
                    "raw_kernel_values":result,"preserves_shared_validation":false}),
                    );
                    Ok(())
                })
                .unwrap();
            assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        }
    }
}

#[test]
fn fsst_admission_literal_boundaries_nulls_and_selected_domain() {
    let long = "x".repeat(255);
    let values = [
        Some("the needle / 🧶☃ / 100%_ / ab"),
        None,
        Some(""),
        Some("ab\\100%_"),
        Some(long.as_str()),
    ];
    let array = fixture(&values);
    for (label, needle) in [
        ("ascii", "needle"),
        ("unicode", "🧶☃"),
        ("escaped", "100%_"),
        ("backslash", "\\100%_"),
        ("short", "ab"),
        ("empty", ""),
        ("limit253", &long[..253]),
        ("limit254", &long[..254]),
        ("limit255", long.as_str()),
    ] {
        for prefix in [false, true] {
            for range in [0..values.len(), 1..4, 0..0] {
                let selected = array.slice(range.clone()).unwrap();
                let expected = values[range.clone()]
                    .iter()
                    .map(|v| {
                        v.map(|v| {
                            if prefix {
                                v.starts_with(needle)
                            } else {
                                v.contains(needle)
                            }
                        })
                    })
                    .collect::<Vec<_>>();
                let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
                session.with_native_execution_context(&CancellationToken::default(), |context| {
                    let observation = Session::start();
                    let actual = booleans(shared(&selected, needle, prefix, context)?, context)?;
                    let work = observation.finish();
                    assert_eq!(actual, expected);
                    let direct = direct(&selected, needle, prefix, context)?;
                    let admitted = selected.is::<FSST>()
                        && needle.len() <= if prefix { 253 } else { 254 };
                    assert_eq!(direct.is_some(), admitted);
                    if let Some(direct) = direct { assert_eq!(booleans(direct, context)?, expected); }
                    emit(&serde_json::json!({"case":"literal_boundary", "pattern":label,"prefix":prefix,
                        "range":[range.start,range.end], "values":actual,"direct_admitted":admitted,"shared_work":work}));
                    Ok(())
                }).unwrap();
                assert_eq!(session.snapshot().memory.reserved_bytes, 0);
            }
        }
    }
    // A selected valid row does not owe UTF-8 validation for a row outside its domain.
    let mixed = encoded(&[Some(escaped(b"a")), Some(escaped(&[0xff]))], &[1, 1]);
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            assert_eq!(
                booleans(
                    shared(&mixed.slice(0..1).unwrap(), "a", false, context)?,
                    context
                )?,
                [Some(true)]
            );
            assert!(shared(&mixed, "a", false, context).is_err());
            Ok(())
        })
        .unwrap();
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn fsst_admission_raw_output_has_no_native_credit_but_shared_output_retains_credit() {
    let array = fixture(&[Some("needle"), Some("ordinary")]);
    let session = ResidentVortexSession::new(1, 1).unwrap();
    let retained = session.with_native_execution_context(&CancellationToken::default(), |context| {
        let error = shared(&array, "needle", false, context).unwrap_err().to_string();
        let direct = direct(&array, "needle", false, context)?.unwrap();
        assert_eq!(booleans(direct.clone(), context)?, [Some(true),Some(false)]);
        emit(&serde_json::json!({"case":"one_byte_grant", "shared_error":error,
            "raw_retained_reserved_bytes":context.memory().snapshot().reserved_bytes,
            "raw_output_bytes":direct.as_::<vortex::array::arrays::Bool>().to_bit_buffer().len().div_ceil(8)}));
        Ok(direct)
    }).unwrap();
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    drop(retained);
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let retained = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            shared(&array, "needle", false, context)
        })
        .unwrap();
    let reserved = session.snapshot().memory.reserved_bytes;
    assert!(reserved > 0);
    drop(retained);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    emit(
        &serde_json::json!({"case":"shared_retained_output", "retained_reserved_bytes":reserved,"final_reserved_bytes":0}),
    );
}

#[test]
fn fsst_admission_raw_kernel_has_no_shared_cancellation_boundary() {
    let array = fixture(&[Some("needle")]);
    let cancellation = CancellationToken::default();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let error = session.with_native_execution_context(&cancellation, |context| {
        cancellation.cancel();
        assert!(direct(&array, "needle", false, context)?.is_some());
        let error = shared(&array, "needle", false, context).unwrap_err().to_string();
        emit(&serde_json::json!({"case":"cancelled_after_entry", "shared_error":error,"raw_kernel_completed":true}));
        context.check_cancelled()
    }).unwrap_err();
    assert!(error.to_string().to_lowercase().contains("cancel"));
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}
