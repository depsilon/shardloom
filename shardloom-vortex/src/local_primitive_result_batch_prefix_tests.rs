use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    VortexSessionExecute as _,
    dtype::Nullability,
    extension::datetime::{Date, TimeUnit, Timestamp},
    memory::MemorySessionExt as _,
};

#[test]
#[allow(clippy::too_many_lines)] // One explicit dtype table exercises the same complete/prefix contract.
fn native_result_prefix_matches_complete_values_and_visitation_for_every_flat_family() {
    let nullable = Nullability::Nullable;
    let decimal = DecimalDType::new(8, 2);
    let mut cases = vec![
        (
            DType::Bool(nullable),
            vec![Value::Bool(true), Value::Null, Value::Bool(false)],
        ),
        (
            DType::Utf8(nullable),
            vec![
                Value::Text("λ\0".into()),
                Value::Null,
                Value::Text("東京".into()),
            ],
        ),
        (
            DType::Binary(nullable),
            vec![
                Value::Binary(b"\0\xff".as_slice().into()),
                Value::Null,
                Value::Binary(b"\xfe".as_slice().into()),
            ],
        ),
        (
            DType::Decimal(decimal, nullable),
            vec![
                Value::Decimal(-999, decimal),
                Value::Null,
                Value::Decimal(123, decimal),
            ],
        ),
        (
            DType::Extension(Date::new(TimeUnit::Days, nullable).erased()),
            vec![
                Value::Int(i64::from(i32::MIN)),
                Value::Null,
                Value::Int(i64::from(i32::MAX)),
            ],
        ),
        (
            DType::Extension(Timestamp::new(TimeUnit::Microseconds, nullable).erased()),
            vec![Value::Int(i64::MIN), Value::Null, Value::Int(i64::MAX)],
        ),
    ];
    for ptype in [PType::I8, PType::I16, PType::I32, PType::I64] {
        cases.push((
            DType::Primitive(ptype, nullable),
            vec![Value::Int(-1), Value::Null, Value::Int(127)],
        ));
    }
    for ptype in [PType::U8, PType::U16, PType::U32, PType::U64] {
        cases.push((
            DType::Primitive(ptype, nullable),
            vec![Value::UInt(0), Value::Null, Value::UInt(255)],
        ));
    }
    for ptype in [PType::F32, PType::F64] {
        cases.push((
            DType::Primitive(ptype, nullable),
            vec![Value::Float(-0.0), Value::Null, Value::Float(1.5)],
        ));
    }
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let allocator = context.native_session().allocator();
            for (dtype, values) in cases {
                let empty = build_column_prefix(&dtype, 0, 0, &allocator, |_| unreachable!())?;
                assert!(empty.is_empty());
                assert_eq!(empty.dtype(), &dtype);
                let mut visits = Vec::new();
                let complete = build_column(&dtype, values.len(), &allocator, |row| {
                    visits.push(row);
                    Ok(values[row].clone())
                })?;
                for retained in 0..=values.len() {
                    let mut prefix_visits = Vec::new();
                    let prefix =
                        build_column_prefix(&dtype, values.len(), retained, &allocator, |row| {
                            prefix_visits.push(row);
                            Ok(values[row].clone())
                        })?;
                    assert_eq!(prefix_visits, visits, "{dtype} {retained}");
                    assert_eq!(prefix.dtype(), &dtype);
                    assert_eq!(prefix.len(), retained);
                    let mut execution = context.native_session().create_execution_ctx();
                    for row in 0..retained {
                        assert_eq!(
                            scalar(&prefix, row, &mut execution)?,
                            scalar(&complete, row, &mut execution)?,
                            "{dtype} {retained} {row}"
                        );
                    }
                }
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_result_prefix_preserves_discarded_value_errors_and_releases_buffers() {
    let nn = Nullability::NonNullable;
    let decimal = DecimalDType::new(2, 0);
    let cases = [
        (DType::Bool(nn), Value::Int(1)),
        (DType::Bool(nn), Value::Null),
        (DType::Utf8(nn), Value::Binary(b"text".as_slice().into())),
        (DType::Binary(nn), Value::Text("text".into())),
        (DType::Primitive(PType::I8, nn), Value::Int(128)),
        (DType::Primitive(PType::U8, nn), Value::UInt(256)),
        (DType::Primitive(PType::F32, nn), Value::Float(0.1)),
        (DType::Primitive(PType::F64, nn), Value::Float(f64::NAN)),
        (
            DType::Primitive(PType::F64, nn),
            Value::Float(f64::INFINITY),
        ),
        (DType::Decimal(decimal, nn), Value::Decimal(100, decimal)),
        (
            DType::Decimal(decimal, nn),
            Value::Decimal(1, DecimalDType::new(3, 0)),
        ),
        (
            DType::Extension(Date::new(TimeUnit::Days, nn).erased()),
            Value::Int(i64::MAX),
        ),
    ];
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let allocator = context.native_session().allocator();
            for (dtype, invalid) in cases {
                let complete =
                    build_column(&dtype, 1, &allocator, |_| Ok(invalid.clone())).unwrap_err();
                let prefix = build_column_prefix(&dtype, 1, 0, &allocator, |_| Ok(invalid.clone()))
                    .unwrap_err();
                assert_eq!(complete.to_string(), prefix.to_string(), "{dtype}");
            }
            assert!(
                build_column_prefix(&DType::Bool(nn), 0, 1, &allocator, |_| unreachable!())
                    .is_err()
            );
            let cancel = CancellationToken::default();
            let mut calls = 0;
            assert!(
                build_column_prefix(
                    &DType::Primitive(PType::I64, nn),
                    4097,
                    1,
                    &allocator,
                    |row| {
                        calls += 1;
                        if row == 1024 {
                            cancel.cancel();
                        }
                        cancel.check()?;
                        Ok(Value::Int(0))
                    }
                )
                .is_err()
            );
            assert_eq!(calls, 1025);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
