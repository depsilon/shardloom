//! Variable width payload copies preserve bytes, validity and owned credits.

use super::*;
use crate::{
    local_primitives::native_payload,
    local_primitives::native_relational_batch::{index_array, take_column},
    resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::arrays::{ChunkedArray, ConstantArray, DictArray, VarBinViewArray};

const ROWS: [Option<usize>; 6] = [Some(3), None, Some(0), Some(2), Some(1), Some(3)];

fn check_variable_take(
    source: &ArrayRef,
    expected: &[Option<&[u8]>],
    utf8: bool,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let indices = index_array(ROWS.len(), true, context, |row| Ok(ROWS[row]))?;
    let dtype = source.dtype().as_nullable();
    let relational = take_column(source, &indices, &dtype, context)?;
    let copied = native_payload::take_with_policy(
        source,
        &indices,
        &dtype,
        native_payload::CopyPolicy::ValidateValues,
        context,
    )?;
    for output in [&relational, &copied] {
        assert_eq!(output.dtype(), &dtype);
        assert_eq!(output.len(), ROWS.len());
        let mut execution = context.native_session().create_execution_ctx();
        for (row, selected) in ROWS.iter().enumerate() {
            let reference = selected
                .map(|source_row| result_batch::scalar_value(source, source_row, &mut execution))
                .transpose()?;
            let actual = result_batch::scalar_value(output, row, &mut execution)?;
            let wanted = expected[row];
            if let Some(bytes) = wanted {
                let reference = reference.as_ref().expect("selected row has a source value");
                match reference {
                    Value::SharedText(value) => assert_eq!(value.as_bytes(), bytes),
                    Value::SharedBinary(value) => assert_eq!(value.as_ref(), bytes),
                    _ => panic!("source scalar has an unexpected variable-width type"),
                }
            } else {
                assert!(reference.is_none() || matches!(reference, Some(Value::Null)));
            }
            match actual {
                Value::Null => assert!(wanted.is_none()),
                Value::SharedText(value) if utf8 => {
                    assert_eq!(value.as_bytes(), wanted.unwrap());
                }
                Value::SharedBinary(value) if !utf8 => {
                    assert_eq!(value.as_ref(), wanted.unwrap());
                }
                _ => panic!("variable width take changed value kind or validity"),
            }
        }
    }
    let empty = index_array(0, false, context, |_| unreachable!())?;
    let empty_relational = take_column(source, &empty, source.dtype(), context)?;
    let empty_copied = native_payload::take_with_policy(
        source,
        &empty,
        source.dtype(),
        native_payload::CopyPolicy::ValidateValues,
        context,
    )?;
    for output in [&empty_relational, &empty_copied] {
        assert_eq!(output.dtype(), source.dtype());
        assert_eq!(output.len(), 0);
    }
    Ok(copied)
}

#[test]
#[allow(clippy::too_many_lines)] // One explicit encoding matrix shares the same selection and scalar oracle.
fn variable_text_and_binary_copies_cover_encoded_sources_and_ownership() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let cancellation = CancellationToken::default();
    let retained = session
        .with_native_execution_context(&cancellation, |context| {
            let text_values = ["短", "unused", "", "東京-long-value-😀"];
            let text_expected = [
                Some(text_values[3].as_bytes()),
                None,
                Some(text_values[0].as_bytes()),
                Some(text_values[2].as_bytes()),
                None,
                Some(text_values[3].as_bytes()),
            ];
            let text = VarBinViewArray::from_iter_nullable_str([
                Some(text_values[0]),
                None,
                Some(text_values[2]),
                Some(text_values[3]),
            ])
            .into_array();
            let mut execution = context.native_session().create_execution_ctx();
            let constant = ConstantArray::new(
                text.execute_scalar(0, &mut execution)
                    .map_err(vortex_error)?,
                4,
            )
            .into_array();
            let sliced_dictionary = DictArray::try_new(
                PrimitiveArray::from_iter([2_u8, 3, 2, 1, 0, 3]).into_array(),
                text.clone(),
            )
            .map_err(vortex_error)?
            .into_array()
            .slice(1..5)
            .map_err(vortex_error)?;
            let sliced_expected = [
                Some(text_values[0].as_bytes()),
                None,
                Some(text_values[3].as_bytes()),
                None,
                Some(text_values[2].as_bytes()),
                Some(text_values[0].as_bytes()),
            ];
            let nullable_codes = DictArray::try_new(
                PrimitiveArray::from_option_iter([Some(2_u8), None, Some(3), Some(0)]).into_array(),
                text.clone(),
            )
            .map_err(vortex_error)?
            .into_array();
            let nullable_codes_expected = [
                Some(text_values[0].as_bytes()),
                None,
                Some(text_values[2].as_bytes()),
                Some(text_values[3].as_bytes()),
                None,
                Some(text_values[0].as_bytes()),
            ];
            let chunked = ChunkedArray::try_new(
                vec![
                    text.slice(0..2).map_err(vortex_error)?,
                    text.slice(2..4).map_err(vortex_error)?,
                ],
                text.dtype().clone(),
            )
            .map_err(vortex_error)?
            .into_array();
            let mut retained = None;
            for (source, expected) in [
                (text.clone(), text_expected),
                (
                    constant,
                    [
                        Some(text_values[0].as_bytes()),
                        None,
                        Some(text_values[0].as_bytes()),
                        Some(text_values[0].as_bytes()),
                        Some(text_values[0].as_bytes()),
                        Some(text_values[0].as_bytes()),
                    ],
                ),
                (chunked, text_expected),
            ] {
                let output = check_variable_take(&source, &expected, true, context)?;
                if retained.is_none() {
                    retained = Some(output);
                }
            }
            check_variable_take(&sliced_dictionary, &sliced_expected, true, context)?;
            check_variable_take(&nullable_codes, &nullable_codes_expected, true, context)?;
            let nulls = ConstantArray::new(
                vortex::array::scalar::Scalar::null(DType::Utf8(Nullability::Nullable)),
                4,
            )
            .into_array();
            check_variable_take(&nulls, &[None; 6], true, context)?;

            let binary_rows: [&[u8]; 4] = [b"x", b"unused", b"", b"\xff\x80binary-long"];
            let binary = VarBinViewArray::from_iter_nullable_bin([
                Some(binary_rows[0]),
                None,
                Some(binary_rows[2]),
                Some(binary_rows[3]),
            ])
            .into_array();
            let binary_expected = [
                Some(binary_rows[3]),
                None,
                Some(binary_rows[0]),
                Some(binary_rows[2]),
                None,
                Some(binary_rows[3]),
            ];
            let binary_constant = ConstantArray::new(
                binary
                    .execute_scalar(0, &mut execution)
                    .map_err(vortex_error)?,
                4,
            )
            .into_array();
            let binary_dictionary = DictArray::try_new(
                PrimitiveArray::from_iter([0_u8, 1, 2, 3]).into_array(),
                binary.clone(),
            )
            .map_err(vortex_error)?
            .into_array();
            let binary_chunked = ChunkedArray::try_new(
                vec![
                    binary.slice(0..2).map_err(vortex_error)?,
                    binary.slice(2..4).map_err(vortex_error)?,
                ],
                binary.dtype().clone(),
            )
            .map_err(vortex_error)?
            .into_array();
            for (source, expected) in [
                (binary.clone(), binary_expected),
                (
                    binary_constant,
                    [
                        Some(binary_rows[0]),
                        None,
                        Some(binary_rows[0]),
                        Some(binary_rows[0]),
                        Some(binary_rows[0]),
                        Some(binary_rows[0]),
                    ],
                ),
                (binary_dictionary, binary_expected),
                (binary_chunked, binary_expected),
            ] {
                check_variable_take(&source, &expected, false, context)?;
            }
            let nulls = ConstantArray::new(
                vortex::array::scalar::Scalar::null(DType::Binary(Nullability::Nullable)),
                4,
            )
            .into_array();
            check_variable_take(&nulls, &[None; 6], false, context)?;
            let nonnullable = VarBinViewArray::from_iter_str([
                "inline",
                "",
                "embedded\0nul",
                "more-than-twelve-bytes",
            ])
            .into_array();
            let expected = [
                Some(b"more-than-twelve-bytes".as_slice()),
                None,
                Some(b"inline".as_slice()),
                Some(b"embedded\0nul".as_slice()),
                Some(b"".as_slice()),
                Some(b"more-than-twelve-bytes".as_slice()),
            ];
            check_variable_take(&nonnullable, &expected, true, context)?;
            let nonnullable_binary = VarBinViewArray::from_iter_bin([
                b"inline".as_slice(),
                b"".as_slice(),
                b"embedded\0nul".as_slice(),
                b"\xff\x80binary-long".as_slice(),
            ])
            .into_array();
            let expected = [
                Some(b"\xff\x80binary-long".as_slice()),
                None,
                Some(b"inline".as_slice()),
                Some(b"embedded\0nul".as_slice()),
                Some(b"".as_slice()),
                Some(b"\xff\x80binary-long".as_slice()),
            ];
            check_variable_take(&nonnullable_binary, &expected, false, context)?;
            Ok(retained.unwrap())
        })
        .unwrap();
    assert!(session.memory().snapshot().reserved_bytes > 0);
    let child = retained.slice(1..3).unwrap();
    drop(retained);
    assert!(session.memory().snapshot().reserved_bytes > 0);
    drop(child);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn variable_result_owner_borrows_selected_native_values_and_keeps_constants_compact() {
    use std::borrow::Cow;

    let session = ResidentVortexSession::new(64 << 10, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut execution = context.native_session().create_execution_ctx();
            for source in [
                VarBinViewArray::from_iter_nullable_str([Some("東京-long-😀"), None]).into_array(),
                VarBinViewArray::from_iter_nullable_bin([Some(b"\xff\x80binary-long"), None])
                    .into_array(),
            ] {
                let owner = result_batch::VariableValues::try_new(&source, &mut execution)?
                    .expect("canonical variable input");
                assert!(matches!(
                    owner.value(0)?,
                    Value::Text(Cow::Borrowed(_)) | Value::Binary(Cow::Borrowed(_))
                ));
                assert!(matches!(owner.value(1)?, Value::Null));
                assert!(owner.value(2).is_err());
                let empty = source.slice(0..0).map_err(vortex_error)?;
                let empty = result_batch::VariableValues::try_new(&empty, &mut execution)?
                    .expect("empty variable input");
                assert!(empty.value(0).is_err());
                let constant = ConstantArray::new(
                    source
                        .execute_scalar(0, &mut execution)
                        .map_err(vortex_error)?,
                    1_000_000,
                )
                .into_array();
                let before = context.memory().snapshot().reserved_bytes;
                #[cfg(debug_assertions)]
                let observation =
                    crate::local_primitives::native_relational_observation::Session::start();
                let constant = result_batch::VariableValues::try_new(&constant, &mut execution)?
                    .expect("constant variable input");
                assert!(matches!(
                    constant.value(999_999)?,
                    Value::Text(Cow::Borrowed(_)) | Value::Binary(Cow::Borrowed(_))
                ));
                assert_eq!(context.memory().snapshot().reserved_bytes, before);
                #[cfg(debug_assertions)]
                {
                    let counts = observation.finish();
                    assert_eq!(counts.variable_result_preparations, 1);
                    assert_eq!(counts.variable_result_constant_rows, 1_000_000);
                    assert_eq!(counts.variable_result_visits, 1);
                }
                let dictionary =
                    DictArray::try_new(PrimitiveArray::from_iter([0_u8]).into_array(), source)
                        .map_err(vortex_error)?
                        .into_array();
                assert!(
                    result_batch::VariableValues::try_new(&dictionary, &mut execution).is_err()
                );
            }
            assert!(
                result_batch::VariableValues::try_new(
                    &PrimitiveArray::from_iter([1_i64]).into_array(),
                    &mut execution,
                )?
                .is_none()
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn variable_gather_releases_source_storage_and_preserves_pressure_and_cancellation() {
    use vortex::array::VortexSessionExecute as _;

    for payload_copy in [false, true] {
        let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let retained = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let huge = "unselected".repeat(131_072);
                let source = result_batch::build_column(
                    &DType::Utf8(Nullability::NonNullable),
                    2,
                    &context.native_session().host_allocator(),
                    |row| Ok(Value::Text(if row == 0 { &huge } else { "東京" }.into())),
                )?;
                assert!(context.memory().snapshot().reserved_bytes > 1 << 20);
                let indices = index_array(2, false, context, |_| Ok(Some(1)))?;
                let gather = || {
                    if payload_copy {
                        native_payload::take(&source, &indices, source.dtype(), context)
                    } else {
                        take_column(&source, &indices, source.dtype(), context)
                    }
                };
                let before = context.memory().snapshot().reserved_bytes;
                let hold = context.memory().reserve((4 << 20) - before - 1)?;
                assert!(gather().is_err());
                drop(hold);
                assert_eq!(context.memory().snapshot().reserved_bytes, before);
                let output = gather()?;
                assert!(output.nbytes() < 1024);
                Ok(output)
            })
            .unwrap();
        assert!(memory.snapshot().reserved_bytes > 0);
        assert!(memory.snapshot().reserved_bytes < 16 << 10);
        let child = retained.slice(0..1).unwrap();
        drop(retained);
        drop(session);
        assert!(memory.snapshot().reserved_bytes > 0);
        let mut execution = vortex::array::array_session().create_execution_ctx();
        assert!(matches!(
            result_batch::scalar_value(&child, 0, &mut execution).unwrap(),
            Value::SharedText(value) if value.as_str() == "東京"
        ));
        drop(child);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let session = ResidentVortexSession::new(64 << 10, 1).unwrap();
    let cancellation = CancellationToken::default();
    let error = session
        .with_native_execution_context(&cancellation, |context| {
            let source = VarBinViewArray::from_iter_str(["東京"]).into_array();
            let indices = index_array(1, false, context, |_| Ok(Some(0)))?;
            let before = context.memory().snapshot().reserved_bytes;
            cancellation.cancel();
            assert!(take_column(&source, &indices, source.dtype(), context).is_err());
            assert!(native_payload::take(&source, &indices, source.dtype(), context).is_err());
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap_err();
    assert!(error.to_string().contains("execution cancelled"), "{error}");
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
