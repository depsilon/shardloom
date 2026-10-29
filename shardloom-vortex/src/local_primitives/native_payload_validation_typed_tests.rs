//! The complete typed validator agrees with the prior scalar construction path.

use super::*;
use crate::local_primitives::native_relational_sort::{Spec, StreamingPrefix};
use crate::relational_query::VortexRelationalOrderKey;
use crate::resident_session::ResidentVortexSession;
use shardloom_core::ColumnRef;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    array::{
        arrays::{BoolArray, ConstantArray, DictArray, ExtensionArray, VarBinViewArray},
        dtype::FieldNames,
        extension::datetime::{Time, TimeUnit},
        scalar::Scalar,
    },
    buffer::Buffer,
};

fn record(fields: Vec<(&str, ArrayRef)>) -> ArrayRef {
    let names = FieldNames::from(fields.iter().map(|(name, _)| *name).collect::<Vec<_>>());
    let rows = fields[0].1.len();
    StructArray::try_new(
        names,
        fields.into_iter().map(|(_, array)| array),
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn fields(array: &ArrayRef) -> Vec<(String, DType)> {
    let fields = array.dtype().as_struct_fields();
    fields
        .names()
        .iter()
        .zip(fields.fields())
        .map(|(name, dtype)| (name.to_string(), dtype))
        .collect()
}

// Preserve the original flat validator here as an independent differential
// oracle. It still dispatches each numeric scalar and writes discarded scratch.
fn original(
    array: &ArrayRef,
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let mut execution = context.native_session().create_execution_ctx();
    for (name, dtype) in fields {
        let column = super::super::logical_field_from_native_array(array, name)?
            .execute::<Columnar>(&mut execution)
            .map(IntoArray::into_array)
            .map_err(vortex_error)?;
        let numeric = if matches!(dtype, DType::Primitive(..))
            || crate::native_payload_schema::temporal_storage(dtype).is_some()
        {
            Some(NativeNumericOwner::new(
                result_batch::scalar_storage(&column, &mut execution)?
                    .execute::<PrimitiveArray>(&mut execution)
                    .map_err(vortex_error)?,
                &mut execution,
            )?)
        } else {
            None
        };
        drop(result_batch::build_column_prefix(
            dtype,
            array.len(),
            0,
            &context.native_session().host_allocator(),
            |row| {
                check(row, context)?;
                if let Some(numeric) = &numeric {
                    numeric.stat_value(row).map(Value::from)
                } else {
                    result_batch::scalar_value(&column, row, &mut execution)
                }
            },
        )?);
    }
    context.check_cancelled()
}

fn compare(array: &ArrayRef, declared: &[(String, DType)], error: Option<&str>, scalar: bool) {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let before = context.memory().snapshot().reserved_bytes;
            let expected = original(array, declared, context).map_err(|e| e.to_string());
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            let actual = validate_record(array, declared, context).map_err(|e| e.to_string());
            assert_eq!(actual, expected);
            if let Some(error) = error {
                let actual = actual.unwrap_err();
                assert!(actual.contains(error), "expected {error:?} in {actual:?}");
            } else {
                actual.unwrap();
            }
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.payload_validation_scalar_calls > 0, scalar);
                assert_eq!(counts.numeric_output_copy_rows, 0);
                if !array.is_empty() && error.is_none() && !scalar {
                    assert_eq!(
                        counts.payload_numeric_validation_rows
                            + counts.payload_variable_validation_rows
                            + counts.payload_boolean_validation_rows,
                        (array.len() * declared.len()) as u64
                    );
                }
            }
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn typed_payload_validation_preserves_all_flat_widths_nulls_slices_and_values() {
    let mut arrays = Vec::new();
    macro_rules! integer {
        ($($t:ty),+) => { $(
            arrays.push(PrimitiveArray::from_option_iter((0..2053).map(|row| match row % 4 {
                0 => Some(<$t>::MIN), 1 => Some(<$t>::MAX), 2 => None, _ => Some(0),
            })).into_array());
        )+ };
    }
    integer!(u8, u16, u32, u64, i8, i16, i32, i64);
    arrays.push(
        PrimitiveArray::from_option_iter((0..2053).map(|row| {
            [
                None,
                Some(-0.0_f32),
                Some(f32::from_bits(1)),
                Some(-f32::from_bits(1)),
            ][row % 4]
        }))
        .into_array(),
    );
    arrays.push(
        PrimitiveArray::from_option_iter((0..2053).map(|row| {
            [
                None,
                Some(-0.0_f64),
                Some(f64::from_bits(1)),
                Some(-f64::from_bits(1)),
            ][row % 4]
        }))
        .into_array(),
    );
    arrays.push(
        BoolArray::from_iter((0..2053).map(|row| [None, Some(false), Some(true)][row % 3]))
            .into_array(),
    );
    arrays.push(
        VarBinViewArray::from_iter_nullable_str((0..2053).map(|row| {
            [
                None,
                Some(""),
                Some("東京\0é"),
                Some("longer than twelve bytes λ"),
            ][row % 4]
        }))
        .into_array(),
    );
    arrays.push(
        VarBinViewArray::from_iter_nullable_bin((0..2053).map(|row| {
            [
                None,
                Some(b"".as_slice()),
                Some(b"\0\xff"),
                Some(&[0xff; 31]),
            ][row % 4]
        }))
        .into_array(),
    );
    for source in arrays {
        for selection in [
            source.clone(),
            source.slice(1..2052).unwrap(),
            source.slice(1..1).unwrap(),
        ] {
            let array = record(vec![("payload", selection)]);
            compare(&array, &fields(&array), None, false);
        }
    }
}

#[test]
fn typed_payload_validation_keeps_declined_conversion_and_constant_contracts() {
    let array = record(vec![(
        "payload",
        PrimitiveArray::from_iter([127_i16, 128, 0]).into_array(),
    )]);
    compare(
        &array,
        &[(
            "payload".into(),
            DType::Primitive(PType::I8, Nullability::NonNullable),
        )],
        Some("fit in target type"),
        true,
    );
    let array = record(vec![(
        "payload",
        PrimitiveArray::from_option_iter([Some(1i64), None]).into_array(),
    )]);
    compare(
        &array,
        &[(
            "payload".into(),
            DType::Primitive(PType::I64, Nullability::NonNullable),
        )],
        Some("null in a nonnullable"),
        true,
    );
    for value in [Scalar::from("a long constant λ"), Scalar::from(true)] {
        let array = record(vec![(
            "payload",
            ConstantArray::new(value, 2053).into_array(),
        )]);
        compare(&array, &fields(&array), None, true);
    }
    let values = VarBinViewArray::from_iter_nullable_str([
        Some("λ"),
        None,
        Some("long value outside inline"),
    ])
    .into_array();
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_iter((0..2053).map(|row| u8::try_from(row % 3).unwrap())).into_array(),
        values,
    )
    .unwrap()
    .into_array();
    let array = record(vec![("payload", dictionary)]);
    compare(&array, &fields(&array), None, false);
}

#[test]
fn typed_payload_validation_preserves_finite_domain_and_first_field_errors() {
    let invalid = PrimitiveArray::from_iter([1.0_f64, f64::NAN]).into_array();
    let time = ExtensionArray::try_new(
        Time::new(TimeUnit::Milliseconds, Nullability::NonNullable).erased(),
        PrimitiveArray::from_iter([0_i32, 86_400_000]).into_array(),
    )
    .unwrap()
    .into_array();
    for (sources, error) in [
        (
            vec![("number", invalid.clone()), ("time", time.clone())],
            "finite declared dtype",
        ),
        (vec![("time", time), ("number", invalid)], "time-of-day"),
    ] {
        let array = record(sources);
        compare(&array, &fields(&array), Some(error), false);
    }
    let array = record(vec![(
        "masked",
        PrimitiveArray::new(
            Buffer::copy_from([f64::NAN, -0.0, f64::INFINITY]),
            Validity::from_iter([false, true, false]),
        )
        .into_array(),
    )]);
    compare(&array, &fields(&array), None, false);
}

#[test]
fn typed_payload_validation_checks_discarded_rows_before_ordering_keys() {
    for cap in [0, 1] {
        let array = record(vec![
            (
                "payload",
                PrimitiveArray::from_iter(
                    (0..2053).map(|row| if row == 2052 { f64::INFINITY } else { 0.0 }),
                )
                .into_array(),
            ),
            (
                "key",
                PrimitiveArray::from_option_iter(
                    (0..2053).map(|row| (row != 0).then_some(i64::from(row))),
                )
                .into_array(),
            ),
        ]);
        let spec = Spec {
            fields: fields(&array),
            keys: vec![VortexRelationalOrderKey {
                column: ColumnRef::new("key").unwrap(),
                descending: false,
                nulls: None,
            }],
            names: vec!["key".into()],
            copy_policy: CopyPolicy::ValidateValues,
        };
        let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut prefix = StreamingPrefix::try_new(&spec, Some(cap), context)?.unwrap();
                let error = prefix.build(array, context).err().unwrap();
                assert!(error.to_string().contains("finite declared dtype"));
                Ok(())
            })
            .unwrap();
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
}

#[test]
fn typed_payload_validation_does_not_allocate_numeric_scratch_and_cancels_in_order() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let token = CancellationToken::default();
    let result = session.with_native_execution_context(&token, |context| {
        let source = PrimitiveArray::from_iter(0..4097_i64);
        let mut execution = context.native_session().create_execution_ctx();
        let owner = NativeNumericOwner::new(source, &mut execution)?;
        let baseline = context.memory().snapshot().reserved_bytes;
        let held = context.memory().reserve((1 << 20) - baseline)?;
        let mut checks = 0;
        result_batch::validate_numeric_column(
            owner.primitive().dtype(),
            owner.primitive(),
            owner.validity(),
            &context.native_session().host_allocator(),
            || {
                checks += 1;
                context.check_cancelled()
            },
        )?;
        assert_eq!(checks, 5);
        assert_eq!(context.memory().snapshot().reserved_bytes, 1 << 20);
        drop(held);
        let mut checks = 0;
        let error = result_batch::validate_numeric_column(
            owner.primitive().dtype(),
            owner.primitive(),
            owner.validity(),
            &context.native_session().host_allocator(),
            || {
                checks += 1;
                if checks == 2 {
                    token.cancel();
                }
                context.check_cancelled()
            },
        )
        .unwrap_err();
        assert_eq!(checks, 2);
        assert!(error.to_string().contains("cancel"));
        assert_eq!(context.memory().snapshot().reserved_bytes, baseline);
        Ok(())
    });
    assert!(result.unwrap_err().to_string().contains("cancel"));
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
