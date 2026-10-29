use super::super::{Value, build_column, scalar, scalar_storage, vortex_error};
use super::*;
use crate::local_primitives::{
    native_numeric_owner::NativeNumericOwner,
    native_relational_batch::{index_array, take_column},
};
use crate::native_temporal::Duration;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    array::{
        IntoArray as _, VortexSessionExecute as _,
        arrays::{BoolArray, ChunkedArray, ConstantArray, DictArray, ExtensionArray},
        dtype::Nullability,
        extension::datetime::{Date, Time, TimeUnit, Timestamp},
        memory::HostMemorySessionExt as _,
        scalar::Scalar,
    },
    buffer::Buffer,
};

fn compare_copy(source: &ArrayRef, dtype: &DType) {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut execution = context.native_session().create_execution_ctx();
            let primitive = scalar_storage(source, &mut execution)?
                .execute::<PrimitiveArray>(&mut execution)
                .map_err(vortex_error)?;
            let owner = NativeNumericOwner::new(primitive, &mut execution)?;
            let allocator = context.native_session().host_allocator();
            let before = memory.snapshot().reserved_bytes;
            let expected = build_column(dtype, source.len(), &allocator, |row| {
                owner.stat_value(row).map(Value::from)
            });
            let held_before_validation = memory.snapshot().reserved_bytes;
            let mut validation_checks = 0;
            let validated = validate_column(
                dtype,
                owner.primitive(),
                owner.validity(),
                &allocator,
                || {
                    validation_checks += 1;
                    Ok(())
                },
            );
            assert_eq!(
                validated.as_ref().err().map(ToString::to_string),
                expected.as_ref().err().map(ToString::to_string)
            );
            if validated.is_ok() {
                assert_eq!(validation_checks, source.len().div_ceil(1024));
            }
            assert_eq!(memory.snapshot().reserved_bytes, held_before_validation);
            let mut checks = 0;
            let actual = copy_column(
                dtype,
                owner.primitive(),
                owner.validity(),
                &allocator,
                || {
                    checks += 1;
                    Ok(())
                },
            );
            assert_eq!(
                actual.as_ref().err().map(ToString::to_string),
                expected.as_ref().err().map(ToString::to_string)
            );
            if let (Ok(actual), Ok(expected)) = (actual, expected) {
                assert_eq!(checks, source.len().div_ceil(1024));
                assert_eq!(actual.dtype(), dtype);
                assert_eq!(actual.len(), source.len());
                let actual_storage = scalar_storage(&actual, &mut execution)?
                    .execute::<PrimitiveArray>(&mut execution)
                    .map_err(vortex_error)?;
                let expected_storage = scalar_storage(&expected, &mut execution)?
                    .execute::<PrimitiveArray>(&mut execution)
                    .map_err(vortex_error)?;
                // Byte equality also covers signed zero, subnormals and zeroed
                // NULL payload slots that value equality would not distinguish.
                assert_eq!(
                    actual_storage.buffer_handle().as_host(),
                    expected_storage.buffer_handle().as_host()
                );
                if !source.is_empty() {
                    assert_ne!(
                        actual_storage.buffer_handle().as_host().as_ptr(),
                        owner.primitive().buffer_handle().as_host().as_ptr()
                    );
                }
                for row in 0..source.len() {
                    assert_eq!(
                        scalar(&actual, row, &mut execution)?,
                        scalar(&expected, row, &mut execution)?
                    );
                }
            }
            assert_eq!(memory.snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn numeric_output_copy_matches_all_widths_empty_null_sliced_and_encoded_values() {
    macro_rules! check {
        ($ty:ty, $one:expr) => {{
            let array = PrimitiveArray::from_iter([<$ty>::MIN, $one, <$ty>::MAX]).into_array();
            compare_copy(&array, array.dtype());
            compare_copy(&array, &array.dtype().as_nullable());
            let array = PrimitiveArray::from_option_iter([
                Some(<$ty>::MAX),
                None,
                Some(<$ty>::MIN),
                Some($one),
                None,
            ])
            .into_array();
            compare_copy(&array, array.dtype());
            compare_copy(&array.slice(1..4).unwrap(), array.dtype());
            compare_copy(&array.slice(2..2).unwrap(), array.dtype());
            let nulls = PrimitiveArray::from_option_iter([None::<$ty>; 9]).into_array();
            compare_copy(&nulls, nulls.dtype());
            let chunks =
                ChunkedArray::try_new([array.clone(), array.clone()], array.dtype().clone())
                    .unwrap()
                    .into_array();
            compare_copy(&chunks, chunks.dtype());
            let dict = DictArray::try_new(
                PrimitiveArray::from_iter([4u8, 2, 1, 0, 2]).into_array(),
                array,
            )
            .unwrap()
            .into_array();
            compare_copy(&dict, dict.dtype());
        }};
    }
    check!(u8, 1);
    check!(u16, 1);
    check!(u32, 1);
    check!(u64, 1);
    check!(i8, 1);
    check!(i16, 1);
    check!(i32, 1);
    check!(i64, 1);
    check!(f32, -0.0);
    check!(f64, -0.0);
    let constant = ConstantArray::new(Scalar::from(u64::MAX), 2051).into_array();
    compare_copy(&constant, constant.dtype());
}

#[test]
fn numeric_output_copy_validates_only_nonnull_values_and_preserves_float_bits() {
    for values in [
        vec![-0.0_f64, 0.0, f64::from_bits(1), -f64::from_bits(1)],
        vec![1.0, f64::INFINITY],
        vec![f64::NEG_INFINITY],
        vec![f64::NAN],
    ] {
        let array = PrimitiveArray::from_iter(values).into_array();
        compare_copy(&array, array.dtype());
    }
    let hidden_invalid = PrimitiveArray::new(
        Buffer::copy_from([f64::NAN, -0.0, f64::INFINITY, f64::from_bits(1)]),
        Validity::Array(BoolArray::from_iter([false, true, false, true]).into_array()),
    )
    .into_array();
    compare_copy(&hidden_invalid, hidden_invalid.dtype());
    let array = PrimitiveArray::from_option_iter([None, Some(7i64)]).into_array();
    compare_copy(
        &array,
        &DType::Primitive(PType::I64, Nullability::NonNullable),
    );
}

#[test]
fn numeric_output_copy_preserves_temporal_metadata_tick_checks_and_null_payloads() {
    let nullable = Nullability::Nullable;
    let cases = [
        (
            Date::new(TimeUnit::Days, nullable).erased(),
            PrimitiveArray::from_option_iter([Some(i32::MIN), None, Some(i32::MAX)]).into_array(),
        ),
        (
            Date::new(TimeUnit::Milliseconds, nullable).erased(),
            PrimitiveArray::from_option_iter([Some(-86_400_000i64), None, Some(86_400_000)])
                .into_array(),
        ),
        (
            Date::new(TimeUnit::Milliseconds, nullable).erased(),
            PrimitiveArray::from_option_iter([Some(0i64), None, Some(1)]).into_array(),
        ),
        (
            Time::new(TimeUnit::Seconds, nullable).erased(),
            PrimitiveArray::from_option_iter([Some(0i32), None, Some(86_399)]).into_array(),
        ),
        (
            Time::new(TimeUnit::Nanoseconds, nullable).erased(),
            PrimitiveArray::from_option_iter([Some(0i64), None, Some(86_400_000_000_000)])
                .into_array(),
        ),
        (
            Timestamp::new(TimeUnit::Nanoseconds, nullable).erased(),
            PrimitiveArray::from_option_iter([Some(i64::MIN), None, Some(i64::MAX)]).into_array(),
        ),
        (
            Duration::new(
                shardloom_core::temporal::TemporalUnit::Microseconds,
                nullable,
            )
            .erased(),
            PrimitiveArray::from_option_iter([Some(i64::MIN), None, Some(i64::MAX)]).into_array(),
        ),
    ];
    for (extension, storage) in cases {
        let array = ExtensionArray::try_new(extension, storage)
            .unwrap()
            .into_array();
        compare_copy(&array, array.dtype());
    }
}

#[test]
fn numeric_output_copy_keeps_cancellation_error_order_and_releases_refused_buffers() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut execution = context.native_session().create_execution_ctx();
            let allocator = context.native_session().host_allocator();
            for invalid_row in [None, Some(1023), Some(1024)] {
                let mut values = vec![1.0; 2051];
                if let Some(row) = invalid_row {
                    values[row] = f64::INFINITY;
                }
                let primitive = PrimitiveArray::from_iter(values);
                let owner = NativeNumericOwner::new(primitive, &mut execution)?;
                let dtype = owner.primitive().dtype();
                let before = context.memory().snapshot().reserved_bytes;
                let expected = build_column(dtype, owner.len(), &allocator, |row| {
                    if row == 1024 {
                        return Err(failed("test cancellation"));
                    }
                    owner.stat_value(row).map(Value::from)
                })
                .unwrap_err();
                let mut checks = 0;
                let actual = copy_column(
                    dtype,
                    owner.primitive(),
                    owner.validity(),
                    &allocator,
                    || {
                        checks += 1;
                        if checks == 2 {
                            Err(failed("test cancellation"))
                        } else {
                            Ok(())
                        }
                    },
                )
                .unwrap_err();
                assert_eq!(actual.to_string(), expected.to_string());
                assert_eq!(checks, if invalid_row == Some(1023) { 1 } else { 2 });
                assert_eq!(context.memory().snapshot().reserved_bytes, before);

                let snapshot = context.memory().snapshot();
                let occupied = context
                    .memory()
                    .reserve(snapshot.limit_bytes - snapshot.reserved_bytes)?;
                let mut checked = false;
                let error = copy_column(
                    dtype,
                    owner.primitive(),
                    owner.validity(),
                    &allocator,
                    || {
                        checked = true;
                        Ok(())
                    },
                )
                .unwrap_err();
                assert!(!checked, "allocation refusal must precede value visitation");
                assert!(
                    error.to_string().contains("memory") || error.to_string().contains("budget")
                );
                assert_eq!(
                    context.memory().snapshot().reserved_bytes,
                    snapshot.limit_bytes
                );
                drop(occupied);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn numeric_output_copy_survives_producer_drop_and_releases_its_own_clone_slice_credits() {
    for rows in [0usize, 4099] {
        let producer = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let producer_memory = producer.memory().clone();
        let receiver = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let receiver_memory = receiver.memory().clone();
        let dtype = DType::Primitive(PType::U64, Nullability::Nullable);
        let source = producer
            .with_native_execution_context(&CancellationToken::default(), |context| {
                build_column(
                    &dtype,
                    rows,
                    &context.native_session().host_allocator(),
                    |row| {
                        Ok(if row % 3 == 1 {
                            Value::Null
                        } else {
                            Value::UInt(u64::MAX - row as u64)
                        })
                    },
                )
            })
            .unwrap();
        let output = receiver
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut execution = context.native_session().create_execution_ctx();
                let primitive = source
                    .clone()
                    .execute::<PrimitiveArray>(&mut execution)
                    .map_err(vortex_error)?;
                let owner = NativeNumericOwner::new(primitive, &mut execution)?;
                copy_column(
                    &dtype,
                    owner.primitive(),
                    owner.validity(),
                    &context.native_session().host_allocator(),
                    || context.check_cancelled(),
                )
            })
            .unwrap();
        drop(source);
        drop(producer);
        assert_eq!(producer_memory.snapshot().reserved_bytes, 0);
        assert!(receiver_memory.snapshot().reserved_bytes > 0);
        let clone = output.clone();
        let slice = output.slice(rows.saturating_sub(2)..rows).unwrap();
        drop(output);
        drop(receiver);
        let reserved = receiver_memory.snapshot().reserved_bytes;
        assert!(reserved > 0);
        drop(clone);
        assert!(receiver_memory.snapshot().reserved_bytes > 0);
        let mut execution = vortex::array::array_session().create_execution_ctx();
        for row in 0..slice.len() {
            let original = rows - slice.len() + row;
            let observed = scalar(&slice, row, &mut execution).unwrap();
            let expected = if original % 3 == 1 {
                Scalar::null(dtype.clone())
            } else {
                Scalar::from(u64::MAX - original as u64)
                    .cast(&dtype)
                    .unwrap()
            };
            assert_eq!(observed, expected);
        }
        drop(slice);
        assert_eq!(receiver_memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn numeric_output_take_preserves_duplicate_order_null_indices_and_selected_validation() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let values = PrimitiveArray::from_iter([u64::MAX, 3, 9, 0]).into_array();
            let source = DictArray::try_new(
                PrimitiveArray::from_iter([3u8, 0, 2, 1]).into_array(),
                values,
            )
            .map_err(vortex_error)?
            .into_array();
            let indices = index_array(6, true, context, |row| {
                Ok([Some(1), None, Some(3), Some(1), Some(0), Some(2)][row])
            })?;
            #[cfg(debug_assertions)]
            let observation =
                crate::local_primitives::native_relational_observation::Session::start();
            let output = take_column(&source, &indices, &source.dtype().as_nullable(), context)?;
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.numeric_output_copy_batches, 1);
                assert_eq!(counts.numeric_output_copy_rows, 6);
            }
            let mut execution = context.native_session().create_execution_ctx();
            for (row, expected) in [
                Some(u64::MAX),
                None,
                Some(3),
                Some(u64::MAX),
                Some(0),
                Some(9),
            ]
            .into_iter()
            .enumerate()
            {
                let expected = expected.map_or_else(
                    || Scalar::null(output.dtype().clone()),
                    |value| Scalar::from(value).cast(output.dtype()).unwrap(),
                );
                assert_eq!(scalar(&output, row, &mut execution)?, expected);
            }
            let source =
                PrimitiveArray::from_iter([f64::INFINITY, -0.0, f64::NAN, 7.0]).into_array();
            let indices = index_array(3, false, context, |row| Ok(Some([1, 3, 1][row])))?;
            let output = take_column(&source, &indices, source.dtype(), context)?;
            let storage = output
                .execute::<PrimitiveArray>(&mut execution)
                .map_err(vortex_error)?;
            assert_eq!(
                storage
                    .as_slice::<f64>()
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                vec![
                    (-0.0_f64).to_bits(),
                    7.0_f64.to_bits(),
                    (-0.0_f64).to_bits()
                ]
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
