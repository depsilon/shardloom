use super::*;
use crate::{
    local_primitives::native_relational_batch::{index_array, take_column},
    resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    IntoArray as _,
    arrays::{ChunkedArray, ConstantArray, PrimitiveArray},
    dtype::Nullability,
    scalar::Scalar,
};

#[test]
fn numeric_gather_writes_original_width_nullable_duplicate_rows_into_owned_buffers() {
    macro_rules! check {
        ($type:ty, $values:expr) => {{
            let values: [$type; 4] = $values;
            let first = PrimitiveArray::from_option_iter([Some(values[0]), None]).into_array();
            let second =
                PrimitiveArray::from_option_iter([Some(values[2]), Some(values[3])]).into_array();
            let source = ChunkedArray::try_new(vec![first.clone(), second], first.dtype().clone())
                .unwrap()
                .into_array();
            let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
            let memory = session.memory().clone();
            let output = session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let indices = index_array(6, true, context, |row| {
                        Ok([Some(3), Some(0), None, Some(1), Some(2), Some(3)][row])
                    })?;
                    let output = try_copy(&source, &indices, source.dtype(), context)?
                        .expect("physical numeric leaves are admitted");
                    let mut execution = context.native_session().create_execution_ctx();
                    let owner = NativeNumericOwner::new(
                        output.as_opt::<Primitive>().unwrap().into_owned(),
                        &mut execution,
                    )?;
                    assert_eq!(output.dtype(), source.dtype());
                    assert_eq!(output.len(), 6);
                    assert!(!owner.validity().value(2));
                    assert!(!owner.validity().value(3));
                    let actual = owner.primitive().as_slice::<$type>();
                    assert_eq!(actual[0], values[3]);
                    assert_eq!(actual[1], values[0]);
                    assert_eq!(actual[4], values[2]);
                    assert_eq!(actual[5], values[3]);
                    Ok(output)
                })
                .unwrap();
            drop(source);
            drop(first);
            drop(session);
            assert!(memory.snapshot().reserved_bytes > 0);
            let retained = output.slice(0..1).unwrap();
            drop(output);
            assert!(memory.snapshot().reserved_bytes > 0);
            drop(retained);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }};
    }
    check!(u8, [1, 0, 255, 3]);
    check!(u16, [1, 0, 65_535, 3]);
    check!(u32, [1, 0, u32::MAX, 3]);
    check!(u64, [1, 0, u64::MAX, 3]);
    check!(i8, [-1, 0, i8::MIN, 3]);
    check!(i16, [-1, 0, i16::MIN, 3]);
    check!(i32, [-1, 0, i32::MIN, 3]);
    check!(i64, [-1, 0, i64::MIN, 3]);
    check!(
        f16,
        [
            f16::NEG_ZERO,
            f16::ZERO,
            f16::from_f64(1.25),
            f16::from_f64(-3.5)
        ]
    );
    check!(f32, [-0.0, 0.0, 1.25, -3.5]);
    check!(f64, [-0.0, 0.0, 1.25, -3.5]);
}

#[test]
fn numeric_gather_preserves_selected_nonfinite_errors_and_provider_index_precedence() {
    let source = PrimitiveArray::from_iter([1.0_f64, f64::NAN, 3.0]).into_array();
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = index_array(3, false, context, |row| Ok(Some([2, 0, 2][row])))?;
            assert!(try_copy(&source, &indices, source.dtype(), context)?.is_some());
            let invalid = index_array(1, false, context, |_| Ok(Some(1)))?;
            let error = try_copy(&source, &invalid, source.dtype(), context).unwrap_err();
            assert!(error.to_string().contains("finite declared dtype"));
            let invalid_index = index_array(2, false, context, |row| Ok(Some([1, 3][row])))?;
            let error = try_copy(&source, &invalid_index, source.dtype(), context).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("gather index exceeds native input")
            );
            let error = take_column(&source, &invalid_index, source.dtype(), context).unwrap_err();
            assert!(
                !error.to_string().contains("finite declared dtype"),
                "{error}"
            );
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn numeric_gather_declines_encoded_values_casts_and_nonnullable_narrowing() {
    let source = PrimitiveArray::from_option_iter([Some(1_i64), None]).into_array();
    let encoded = ConstantArray::new(Scalar::from(9_i64), 2).into_array();
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = index_array(1, false, context, |_| Ok(Some(0)))?;
            assert!(try_copy(&encoded, &indices, encoded.dtype(), context)?.is_none());
            assert!(
                try_copy(
                    &source,
                    &indices,
                    &DType::Primitive(PType::I64, Nullability::NonNullable),
                    context
                )?
                .is_none()
            );
            assert!(
                try_copy(
                    &source,
                    &indices,
                    &DType::Primitive(PType::F64, Nullability::Nullable),
                    context
                )?
                .is_none()
            );
            let empty = index_array(0, false, context, |_| unreachable!())?;
            let output = try_copy(&source, &empty, source.dtype(), context)?.unwrap();
            assert!(output.is_empty());
            Ok(())
        })
        .unwrap();
}
