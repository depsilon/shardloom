use super::super::{build_column, scalar, scalar_value};
use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    VortexSessionExecute as _, dtype::Nullability, memory::HostMemorySessionExt as _,
};

#[test]
fn expression_variable_output_evaluates_once_and_matches_complete_native_values() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let long = "λ\0東京İ".repeat(2049);
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let allocator = context.native_session().host_allocator();
            for (dtype, values) in [
                (
                    DType::Utf8(Nullability::Nullable),
                    vec![
                        Value::Text("".into()),
                        Value::Null,
                        Value::Text(long.as_str().into()),
                        Value::Text("ß\n".into()),
                    ],
                ),
                (
                    DType::Binary(Nullability::Nullable),
                    vec![
                        Value::Binary(b"".as_slice().into()),
                        Value::Null,
                        Value::Binary(long.as_bytes().into()),
                        Value::Binary(b"\xff\0\xfe".as_slice().into()),
                    ],
                ),
            ] {
                for selected in [&values[..], &values[..0], &values[1..2], &values[2..3]] {
                    let before = context.memory().snapshot().reserved_bytes;
                    let expected = build_column(&dtype, selected.len(), &allocator, |row| {
                        Ok(selected[row].clone())
                    })?;
                    let mut visits = Vec::new();
                    let actual = build_once(&dtype, selected.len(), &allocator, |row| {
                        visits.push(row);
                        Ok(selected[row].clone())
                    })?;
                    assert_eq!(visits, (0..selected.len()).collect::<Vec<_>>());
                    assert_eq!(actual.dtype(), expected.dtype());
                    assert_eq!(actual.len(), selected.len());
                    let mut execution = context.native_session().create_execution_ctx();
                    for row in 0..selected.len() {
                        assert_eq!(
                            scalar(&actual, row, &mut execution)?,
                            scalar(&expected, row, &mut execution)?
                        );
                    }
                    drop((actual, expected));
                    assert_eq!(context.memory().snapshot().reserved_bytes, before);
                }
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn expression_variable_output_preserves_callback_error_precedence() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let allocator = context.native_session().host_allocator();
            let dtype = DType::Utf8(Nullability::NonNullable);
            for bad in [
                Value::Null,
                Value::UInt(5),
                Value::Binary(b"wrong type".as_slice().into()),
            ] {
                for failure in [None, Some(1), Some(1024)] {
                    let mut visits = Vec::new();
                    let value = |row| {
                        if failure == Some(row) {
                            Err(failed("test cancellation"))
                        } else if row == 0 {
                            Ok(bad.clone())
                        } else {
                            Ok(Value::Text("ok".into()))
                        }
                    };
                    let before = context.memory().snapshot().reserved_bytes;
                    let expected = build_column(&dtype, 2051, &allocator, value).unwrap_err();
                    let actual = build_once(&dtype, 2051, &allocator, |row| {
                        visits.push(row);
                        value(row)
                    })
                    .unwrap_err();
                    assert_eq!(actual.to_string(), expected.to_string());
                    assert_eq!(
                        visits,
                        (0..failure.map_or(2051, |row| row + 1)).collect::<Vec<_>>()
                    );
                    assert_eq!(context.memory().snapshot().reserved_bytes, before);
                }
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn expression_variable_output_admits_growth_overlap_and_releases_refused_storage() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let allocator = context.native_session().host_allocator();
            let before = context.memory().snapshot();
            // Native host allocations include 256-byte alignment capacity.
            // Admit the initial buffers and first payload, but refuse the
            // replacement while its predecessor is still independently owned.
            let occupied = context
                .memory()
                .reserve(before.limit_bytes - before.reserved_bytes - 1536)?;
            let occupied_bytes = context.memory().snapshot().reserved_bytes;
            let mut visits = Vec::new();
            let error = build_once(&DType::Utf8(Nullability::Nullable), 3, &allocator, |row| {
                visits.push(row);
                Ok(if row == 0 {
                    Value::Text("x".into())
                } else {
                    Value::Text("abc".repeat(100).into())
                })
            })
            .unwrap_err();
            assert_eq!(visits, [0, 1]);
            assert!(error.to_string().contains("memory") || error.to_string().contains("budget"));
            assert_eq!(context.memory().snapshot().reserved_bytes, occupied_bytes);
            drop(occupied);
            assert_eq!(
                context.memory().snapshot().reserved_bytes,
                before.reserved_bytes
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn expression_variable_output_is_independent_after_source_and_session_close() {
    for rows in [0, 3] {
        let producer = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let receiver = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let producer_memory = producer.memory().clone();
        let receiver_memory = receiver.memory().clone();
        let dtype = DType::Utf8(Nullability::Nullable);
        let source = producer
            .with_native_execution_context(&CancellationToken::default(), |context| {
                build_column(
                    &dtype,
                    rows,
                    &context.native_session().host_allocator(),
                    |row| {
                        Ok(if row == 1 {
                            Value::Null
                        } else {
                            Value::Text("independent λ".into())
                        })
                    },
                )
            })
            .unwrap();
        let output = receiver
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut execution = context.native_session().create_execution_ctx();
                build_once(
                    &dtype,
                    rows,
                    &context.native_session().host_allocator(),
                    |row| scalar_value(&source, row, &mut execution),
                )
            })
            .unwrap();
        drop(source);
        drop(producer);
        assert_eq!(producer_memory.snapshot().reserved_bytes, 0);
        let clone = output.clone();
        let slice = output.slice(rows.saturating_sub(2)..rows).unwrap();
        drop(output);
        drop(receiver);
        assert!(receiver_memory.snapshot().reserved_bytes > 0);
        drop(clone);
        assert!(receiver_memory.snapshot().reserved_bytes > 0);
        let mut execution = vortex::array::array_session().create_execution_ctx();
        for row in 0..slice.len() {
            let original = rows - slice.len() + row;
            let actual = scalar_value(&slice, row, &mut execution)
                .unwrap()
                .into_json()
                .unwrap();
            assert_eq!(
                actual,
                if original == 1 {
                    serde_json::Value::Null
                } else {
                    "independent λ".into()
                }
            );
        }
        drop(slice);
        assert_eq!(receiver_memory.snapshot().reserved_bytes, 0);
    }
}
