use super::*;
use vortex::{
    VortexSessionDefault as _,
    array::{
        Canonical, VortexSessionExecute as _,
        arrays::{ChunkedArray, DictArray, VarBinArray},
        dtype::Nullability,
        memory::HostMemorySessionExt as _,
    },
    buffer::buffer,
    encodings::fsst::{fsst_compress, fsst_train_compressor},
};

fn session(memory: &LiveMemoryPool) -> VortexSession {
    let session = VortexSession::default().with_host_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ));
    install(&session, memory.clone());
    session
}

#[derive(Clone, Debug, Default)]
struct SuspendedCalls(Arc<std::sync::atomic::AtomicUsize>);

impl SessionVar for SuspendedCalls {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

fn suspended_only(
    child: &ArrayRef,
    parent: &ArrayRef,
    child_idx: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<ArrayRef>> {
    if child_idx != 0 || parent.slots()[0].is_some() {
        return Ok(None);
    }
    ctx.session()
        .get::<SuspendedCalls>()
        .0
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    chunked::execute_shared_dictionary(child, parent, child_idx, ctx)
}

/// Restrict the registry so eager take kernels cannot bypass `ExecuteSlot`.
/// Vortex itself detaches the slot; the test never constructs an incomplete
/// array or mutates the parent. The ordinary installed session is tested above.
fn suspended_session(memory: &LiveMemoryPool) -> VortexSession {
    let session = VortexSession::empty()
        .with_some(vortex::array::optimizer::kernels::KernelSession::empty())
        .with_host_allocator(Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
            memory.clone(),
        )));
    session.register(ProviderMemory {
        memory: memory.clone(),
    });
    session.kernels().register_execute_parent(
        Dict.id(),
        Chunked.id(),
        &[suspended_only as ExecuteParentFn],
    );
    session.kernels().register_execute_parent(
        Chunked.id(),
        Primitive.id(),
        &[chunked::execute as ExecuteParentFn],
    );
    session
}

fn assert_values(actual: &ArrayRef, expected: &ArrayRef) {
    assert_eq!(actual.dtype(), expected.dtype());
    assert_eq!(actual.len(), expected.len());
    let mut ctx = VortexSession::default().create_execution_ctx();
    for row in 0..expected.len() {
        assert_eq!(
            actual.execute_scalar(row, &mut ctx).unwrap(),
            expected.execute_scalar(row, &mut ctx).unwrap(),
            "row {row}",
        );
    }
}

fn code_chunks(rows: usize, nullable: bool) -> ArrayRef {
    let codes = (0..rows).map(|row| [2u32, 0, 2, 1][row % 4]);
    let codes = if nullable {
        PrimitiveArray::from_option_iter(
            codes
                .enumerate()
                .map(|(row, code)| (row % 4 != 1).then_some(code)),
        )
        .into_array()
    } else {
        PrimitiveArray::from_iter(codes).into_array()
    };
    chunks(vec![
        codes.slice(0..rows / 2).unwrap(),
        codes.slice(rows / 2..rows).unwrap(),
    ])
}

#[test]
fn native_dictionary_suspended_fixed_width_preserves_admission_and_last_owner() {
    use vortex::array::{
        arrays::{BoolArray, DecimalArray},
        dtype::DecimalDType,
    };
    let decimal = DecimalDType::new(18, 2);
    for rows in [1usize, 7, 8, 65] {
        for (values, expected, output_bytes, gather_bytes) in [
            (
                buffer![7i64, -17, 91].into_array(),
                PrimitiveArray::from_iter((0..rows).map(|row| [91i64, 7, 91, -17][row % 4]))
                    .into_array(),
                rows as u64 * 8 + 256,
                rows as u64 * 8 + 256,
            ),
            (
                BoolArray::from_iter([true, false, true]).into_array(),
                BoolArray::from_iter((0..rows).map(|row| row % 4 != 3)).into_array(),
                rows.div_ceil(8) as u64 + 256,
                rows.div_ceil(64) as u64 * 8 + 256 + 8,
            ),
            (
                DecimalArray::new(buffer![123i128, -456, 789], decimal, Validity::NonNullable)
                    .into_array(),
                DecimalArray::new(
                    (0..rows)
                        .map(|row| [789i128, 123, 789, -456][row % 4])
                        .collect(),
                    decimal,
                    Validity::NonNullable,
                )
                .into_array(),
                rows as u64 * 8 + 256,
                rows as u64 * 16 + 256,
            ),
        ] {
            // Independent bounds for output/finish, gather scratch and the
            // separately owned u32 code builder/finish, all live at the peak.
            let outer = output_bytes + 256;
            let grant = outer + gather_bytes + rows as u64 * 4 + 256 + 256;
            for shared in [false, true] {
                for limit in [outer - 1, grant - 1, grant] {
                    let input = DictArray::try_new(code_chunks(rows, false), values.clone())
                        .unwrap()
                        .into_array();
                    let held = shared.then(|| input.clone());
                    let memory = LiveMemoryPool::new(limit).unwrap();
                    let session = suspended_session(&memory);
                    let result = input.execute::<Canonical>(&mut session.create_execution_ctx());
                    assert_eq!(
                        session
                            .get::<SuspendedCalls>()
                            .0
                            .load(std::sync::atomic::Ordering::Relaxed),
                        1,
                    );
                    if limit < grant {
                        let error = result.unwrap_err();
                        assert!(
                            crate::owned_buffers::is_owned_reservation_denial(&error),
                            "{error}"
                        );
                        assert_eq!(memory.snapshot().reserved_bytes, 0);
                        assert_eq!(memory.snapshot().denied_reservations, 1);
                    } else {
                        let output = result.unwrap().into_array();
                        assert_values(&output, &expected);
                        assert_eq!(memory.snapshot().peak_reserved_bytes, grant);
                        assert_eq!(memory.snapshot().reserved_bytes, output_bytes);
                        let retained = output.buffers()[0].clone();
                        drop(output);
                        assert_eq!(memory.snapshot().reserved_bytes, output_bytes);
                        drop(retained);
                        assert_eq!(memory.snapshot().reserved_bytes, 0);
                    }
                    if let Some(original) = held {
                        assert_eq!(original.as_::<Dict>().codes().len(), rows);
                    }
                }
            }
        }
    }
}

#[test]
fn native_dictionary_suspended_nullable_and_empty_preserve_values_and_release() {
    for rows in [0usize, 1, 7, 65] {
        for nullable_codes in [false, true] {
            for values in [
                buffer![7i64, -17, 91].into_array(),
                PrimitiveArray::from_option_iter([Some(7i64), None, Some(91)]).into_array(),
                PrimitiveArray::from_option_iter([None::<i64>, None, None]).into_array(),
            ] {
                let input = DictArray::try_new(code_chunks(rows, nullable_codes), values.clone())
                    .unwrap()
                    .into_array();
                let mut reference = VortexSession::default().create_execution_ctx();
                let expected = input
                    .clone()
                    .execute::<Canonical>(&mut reference)
                    .unwrap()
                    .into_array();
                for limit in [1u64, 1 << 20] {
                    let memory = LiveMemoryPool::new(limit).unwrap();
                    let session = suspended_session(&memory);
                    let result = input
                        .clone()
                        .execute::<Canonical>(&mut session.create_execution_ctx());
                    assert_eq!(
                        session
                            .get::<SuspendedCalls>()
                            .0
                            .load(std::sync::atomic::Ordering::Relaxed),
                        usize::from(rows != 0),
                    );
                    if rows != 0 && limit == 1 {
                        let error = result.unwrap_err();
                        assert!(
                            crate::owned_buffers::is_owned_reservation_denial(&error),
                            "{error}"
                        );
                    } else {
                        let output = result.unwrap().into_array();
                        assert_values(&output, &expected);
                        drop(output);
                    }
                    assert_eq!(memory.snapshot().reserved_bytes, 0);
                }
            }
        }
    }
}

fn chunks(values: Vec<ArrayRef>) -> ArrayRef {
    let dtype = values[0].dtype().clone();
    ChunkedArray::try_new(values, dtype).unwrap().into_array()
}

#[test]
fn native_dictionary_suspended_text_codes_execute_without_parent_slot_access() {
    let source = VarBinArray::from_iter(
        [
            Some("first long dictionary value"),
            None,
            Some("third long dictionary value"),
        ],
        DType::Utf8(Nullability::Nullable),
    )
    .into_array();
    let mut setup = VortexSession::default().create_execution_ctx();
    let compressor = fsst_train_compressor(&source, &mut setup).unwrap();
    let values = fsst_compress(&source, &compressor, &mut setup)
        .unwrap()
        .into_array();
    let expected = [
        Some("third long dictionary value"),
        Some("first long dictionary value"),
        None,
        Some("third long dictionary value"),
    ];
    for shared in [false, true] {
        let input = DictArray::try_new(
            chunks(vec![
                buffer![2u32, 0].into_array(),
                buffer![1u32, 2].into_array(),
            ]),
            values.clone(),
        )
        .unwrap()
        .into_array();
        let held = shared.then(|| input.clone());
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let output = input
            .execute::<Canonical>(&mut session(&memory).create_execution_ctx())
            .unwrap()
            .into_array();
        assert_eq!(output.dtype(), source.dtype());
        assert_eq!(output.len(), expected.len());
        for (row, value) in expected.iter().enumerate() {
            assert_eq!(
                output.execute_scalar(row, &mut setup).unwrap(),
                vortex::array::scalar::Scalar::from(*value),
            );
        }
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        if let Some(original) = held {
            assert_eq!(original.as_::<Dict>().codes().len(), expected.len());
        }
    }
}
