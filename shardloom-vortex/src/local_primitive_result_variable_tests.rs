use super::super::{build_column, scalar};
use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    ArrayParts, Columnar, IntoArray as _, VortexSessionExecute as _,
    arrays::{ConstantArray, VarBin, VarBinArray, varbin::VarBinArraySlotsExt as _},
    dtype::Nullability,
    memory::HostMemorySessionExt as _,
    scalar::Scalar,
};

fn compare(source: &ArrayRef, dtype: &DType) {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut execution = context.native_session().create_execution_ctx();
            let values = VariableValues::try_new(source, &mut execution)?.unwrap();
            let allocator = context.native_session().host_allocator();
            let before = context.memory().snapshot().reserved_bytes;
            let expected = build_column(dtype, source.len(), &allocator, |row| values.value(row));
            let mut checks = 0;
            let actual = values.copy_column(dtype, source.len(), &allocator, || {
                checks += 1;
                Ok(())
            });
            assert_eq!(
                actual.as_ref().err().map(ToString::to_string),
                expected.as_ref().err().map(ToString::to_string)
            );
            if let (Ok(actual), Ok(expected)) = (actual, expected) {
                assert_eq!(checks, 2 * source.len().div_ceil(1024));
                assert_eq!(actual.dtype(), expected.dtype());
                assert_eq!(actual.len(), expected.len());
                assert_eq!(actual.nbytes(), expected.nbytes());
                assert_eq!(
                    actual.as_::<VarBin>().bytes(),
                    expected.as_::<VarBin>().bytes()
                );
                for row in 0..source.len() {
                    assert_eq!(
                        scalar(&actual, row, &mut execution)?,
                        scalar(&expected, row, &mut execution)?
                    );
                }
                let offsets = actual.as_::<VarBin>().offsets().clone();
                let expected_offsets = expected.as_::<VarBin>().offsets().clone();
                for row in 0..=source.len() {
                    assert_eq!(
                        scalar(&offsets, row, &mut execution)?,
                        scalar(&expected_offsets, row, &mut execution)?
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
fn variable_copy_matches_generic_bytes_offsets_nulls_constants_and_empty() {
    let long = "東京-😀-".repeat(4096);
    let sources = [
        VarBinViewArray::from_iter_str(["", "a", "123456789012", "1234567890123", &long])
            .into_array(),
        VarBinViewArray::from_iter_nullable_str([Some(""), None, Some("東京"), Some(&long), None])
            .into_array(),
        VarBinViewArray::from_iter_nullable_bin([Some(&b"\xff\x00\x80"[..]), None, Some(&b""[..])])
            .into_array(),
        VarBinViewArray::from_iter_nullable_str([None::<&str>; 9]).into_array(),
        ConstantArray::new(Scalar::from("東京-long-😀"), 2049).into_array(),
        ConstantArray::new(Scalar::null(DType::Utf8(Nullability::Nullable)), 17).into_array(),
        VarBinViewArray::from_iter_str(std::iter::empty::<&str>()).into_array(),
    ];
    for source in sources {
        compare(&source, source.dtype());
        compare(&source, &source.dtype().as_nullable());
        compare(&source, &source.dtype().as_nonnullable());
    }
}

fn invalid_text() -> ArrayRef {
    let binary = VarBinArray::from(vec![&b"ok"[..], &b"\xff"[..], &b"last"[..]]);
    let slots = binary.slots().iter().cloned().collect();
    VarBinArray::try_from_parts(ArrayParts::new(
        VarBin,
        DType::Utf8(Nullability::NonNullable),
        3,
        binary.into_data(),
        slots,
    ))
    .unwrap()
    .into_array()
}

#[test]
fn variable_copy_preserves_required_native_utf8_and_null_type_checks() {
    use crate::local_primitives::native_relational_batch::{index_array, take_column};
    let invalid = invalid_text();
    // Build the selected native view through the real provider. A view made
    // from unchecked VarBin parts still owes value validation at consumption.
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut execution = context.native_session().create_execution_ctx();
            let bad = index_array(1, false, context, |_| Ok(Some(1)))?;
            let selected = invalid
                .take(bad.clone())
                .map_err(vortex_error)?
                .execute::<Columnar>(&mut execution)
                .map_err(vortex_error)?
                .into_array();
            let values = VariableValues::try_new(&selected, &mut execution)?.unwrap();
            let expected = build_column(
                selected.dtype(),
                selected.len(),
                &context.native_session().host_allocator(),
                |row| values.value(row),
            )
            .err()
            .unwrap();
            let actual = take_column(&invalid, &bad, invalid.dtype(), context)
                .err()
                .unwrap();
            assert_eq!(actual.to_string(), expected.to_string());
            assert!(actual.to_string().contains("utf-8"));
            let good = index_array(1, false, context, |_| Ok(Some(0)))?;
            let selected = take_column(&invalid, &good, invalid.dtype(), context)?;
            assert!(matches!(
                scalar_value(&selected, 0, &mut execution)?,
                Value::SharedText(value) if value.as_str() == "ok"
            ));
            // The sizing pass must still discover bad observed text before
            // the following pass checks output nullability or logical kind.
            let nullable = index_array(2, true, context, |row| Ok((row == 1).then_some(1)))?;
            let selected = invalid
                .take(nullable)
                .map_err(vortex_error)?
                .execute::<Columnar>(&mut execution)
                .map_err(vortex_error)?
                .into_array();
            compare(&selected, &DType::Utf8(Nullability::NonNullable));
            compare(&selected, &DType::Binary(Nullability::Nullable));
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    let nullable = VarBinViewArray::from_iter_nullable_str([None, Some("text")]).into_array();
    compare(&nullable, &nullable.dtype().as_nonnullable());
    let text = VarBinViewArray::from_iter_str(["text"]).into_array();
    compare(&text, &DType::Binary(Nullability::NonNullable));
    let binary = VarBinViewArray::from_iter_bin([&b"\xff"[..]]).into_array();
    compare(&binary, &DType::Utf8(Nullability::NonNullable));
}

#[test]
fn variable_copy_preserves_allocation_refusals_and_refunds_before_validation() {
    let session = ResidentVortexSession::new(64 << 10, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut execution = context.native_session().create_execution_ctx();
            let allocator = context.native_session().host_allocator();
            let bad = crate::local_primitives::native_relational_batch::index_array(
                1,
                false,
                context,
                |_| Ok(Some(1)),
            )?;
            let invalid = invalid_text()
                .take(bad)
                .map_err(vortex_error)?
                .execute::<Columnar>(&mut execution)
                .map_err(vortex_error)?
                .into_array();
            for source in [
                VarBinViewArray::from_iter_str(["data", "more data"]).into_array(),
                VarBinViewArray::from_iter_nullable_str([Some("data"), None]).into_array(),
                invalid,
            ] {
                let values = VariableValues::try_new(&source, &mut execution)?.unwrap();
                let before = context.memory().snapshot().reserved_bytes;
                for (available, extra_row) in [
                    (1, false),
                    (64, false),
                    (128, false),
                    (256, false),
                    (4096, false),
                    (4096, true),
                ] {
                    let held = context.memory().reserve((64 << 10) - before - available)?;
                    let rows = source.len() + usize::from(extra_row);
                    let dtype = source.dtype().as_nonnullable();
                    let expected = build_column(&dtype, rows, &allocator, |row| values.value(row));
                    let error = expected.as_ref().err().map(ToString::to_string);
                    drop(expected);
                    let actual = values.copy_column(&dtype, rows, &allocator, || Ok(()));
                    assert_eq!(actual.as_ref().err().map(ToString::to_string), error);
                    drop(actual);
                    drop(held);
                    assert_eq!(context.memory().snapshot().reserved_bytes, before);
                }
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn variable_copy_cancellation_in_either_pass_releases_temporary_buffers() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let source =
                VarBinViewArray::from_iter_str(std::iter::repeat_n("東京", 2049)).into_array();
            let mut execution = context.native_session().create_execution_ctx();
            let values = VariableValues::try_new(&source, &mut execution)?.unwrap();
            let before = context.memory().snapshot().reserved_bytes;
            for at in [1, 2, 3, 4, 5, 6] {
                let cancellation = CancellationToken::default();
                let mut checks = 0;
                let actual = values.copy_column(
                    source.dtype(),
                    source.len(),
                    &context.native_session().host_allocator(),
                    || {
                        checks += 1;
                        if checks == at {
                            cancellation.cancel();
                        }
                        cancellation.check()
                    },
                );
                assert!(
                    actual
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("execution cancelled")
                );
                assert_eq!(checks, at);
                assert_eq!(context.memory().snapshot().reserved_bytes, before);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn variable_copy_output_child_keeps_only_receiving_buffer_credit() {
    let producer = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let source = producer
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let text = "large native text".repeat(4096);
            build_column(
                &DType::Utf8(Nullability::NonNullable),
                2,
                &context.native_session().host_allocator(),
                |row| {
                    Ok(Value::Text(
                        if row == 0 { text.as_str() } else { "small" }.into(),
                    ))
                },
            )?
            .execute::<VarBinViewArray>(&mut context.native_session().create_execution_ctx())
            .map(vortex::array::IntoArray::into_array)
            .map_err(vortex_error)
        })
        .unwrap();
    let receiver = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let output = receiver
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut execution = context.native_session().create_execution_ctx();
            let values = VariableValues::try_new(&source, &mut execution)?.unwrap();
            values.copy_column(
                source.dtype(),
                source.len(),
                &context.native_session().host_allocator(),
                || context.check_cancelled(),
            )
        })
        .unwrap();
    drop(source);
    assert_eq!(producer.memory().snapshot().reserved_bytes, 0);
    let memory = receiver.memory().clone();
    let child = output.slice(1..2).unwrap();
    drop(output);
    drop(receiver);
    assert!(memory.snapshot().reserved_bytes > 0);
    assert!(
        matches!(scalar_value(&child, 0, &mut vortex::session::VortexSession::empty().create_execution_ctx()).unwrap(),
        Value::SharedText(value) if value.as_str() == "small")
    );
    drop(child);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
#[cfg(debug_assertions)]
fn variable_copy_visits_each_selected_value_once_and_keeps_encoded_selection() {
    use crate::local_primitives::{
        native_relational_batch::{index_array, take_column},
        native_relational_observation::Session,
    };
    use vortex::array::arrays::{DictArray, PrimitiveArray};
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let source =
                VarBinViewArray::from_iter_nullable_str([Some("東京"), None, Some("text")])
                    .into_array();
            let mut execution = context.native_session().create_execution_ctx();
            let values = VariableValues::try_new(&source, &mut execution)?.unwrap();
            let allocator = context.native_session().host_allocator();
            let counted = Session::start();
            drop(build_column(
                source.dtype(),
                source.len(),
                &allocator,
                |row| values.value(row),
            )?);
            assert_eq!(counted.finish().variable_result_visits, 6);
            let counted = Session::start();
            drop(values.copy_column(source.dtype(), source.len(), &allocator, || Ok(()))?);
            assert_eq!(counted.finish().variable_result_visits, 3);
            // A selected dictionary whose domain is not smaller than this selection
            // keeps the existing provider path, then reaches the typed copy.
            let encoded =
                DictArray::try_new(PrimitiveArray::from_iter([2_u8, 0, 1]).into_array(), source)
                    .map_err(vortex_error)?
                    .into_array();
            let indices = index_array(3, false, context, |row| Ok(Some([1, 2, 0][row])))?;
            let counted = Session::start();
            let output = take_column(&encoded, &indices, encoded.dtype(), context)?;
            assert_eq!(counted.finish().variable_result_visits, 3);
            for (row, expected) in [Some("東京"), None, Some("text")].into_iter().enumerate() {
                match scalar_value(&output, row, &mut execution)? {
                    Value::SharedText(value) => assert_eq!(Some(value.as_str()), expected),
                    Value::Null => assert!(expected.is_none()),
                    _ => panic!("changed output type"),
                }
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
