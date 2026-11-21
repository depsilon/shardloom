//! Compare complete shallow native results with the original export preparation.

use super::super::reference;
use super::*;
use parquet_variant::{Variant as PqValue, VariantBuilder};
use shardloom_exec::live_memory::LiveMemoryPool;
use std::{
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayContext, VortexSessionExecute as _,
        arrays::list::ListArraySlotsExt as _,
        arrays::variant::VariantArraySlotsExt as _,
        arrays::{
            ChunkedArray, ConstantArray, List, ListView, MaskedArray, PrimitiveArray, Struct,
            VarBinArray, Variant, VariantArray,
        },
        dtype::{FieldNames, Nullability, extension::ExtDType},
        memory::HostMemorySessionExt as _,
        scalar::Scalar,
        serde::SerializeOptions,
        validity::Validity,
    },
    encodings::{
        parquet_variant::ParquetVariant,
        uuid::{Uuid, UuidMetadata},
    },
    session::VortexSession,
};

fn session(bytes: u64) -> (LiveMemoryPool, VortexSession) {
    let memory = LiveMemoryPool::new(bytes).unwrap();
    let allocator = Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
        memory.clone(),
    ));
    (
        memory,
        VortexSession::default().with_host_allocator(allocator),
    )
}

fn uuid() -> ArrayRef {
    let storage = FixedSizeListArray::try_new(
        PrimitiveArray::from_iter(0_u8..16).into_array(),
        16,
        Validity::AllValid,
        1,
    )
    .unwrap()
    .into_array();
    let dtype = ExtDType::try_with_vtable(Uuid, UuidMetadata::default(), storage.dtype().clone())
        .unwrap()
        .erased();
    ExtensionArray::new(dtype, storage).into_array()
}

fn record(names: &[&str], children: Vec<ArrayRef>, rows: usize) -> ArrayRef {
    StructArray::new(
        FieldNames::from_iter(names.iter().copied()),
        children,
        rows,
        Validity::NonNullable,
    )
    .into_array()
}

fn list(child: ArrayRef, offsets: &[u64], sizes: &[u64], ctx: &mut ExecutionCtx) -> ArrayRef {
    ListViewArray::try_new(
        child,
        PrimitiveArray::from_iter(offsets.iter().copied()).into_array(),
        PrimitiveArray::from_iter(sizes.iter().copied()).into_array(),
        Validity::AllValid,
        ctx,
    )
    .unwrap()
    .into_array()
}

fn parquet(rows: usize, value: Option<Vec<u8>>, typed: Option<ArrayRef>) -> ArrayRef {
    let (metadata, _) = VariantBuilder::new()
        .with_field_names(["a", "b"])
        .with_value(PqValue::Null)
        .finish();
    let binary = |bytes| {
        VarBinArray::from_vec(vec![bytes; rows], DType::Binary(Nullability::NonNullable))
            .into_array()
    };
    ParquetVariant::try_new(
        Validity::AllValid,
        binary(metadata),
        value.map(binary),
        typed,
    )
    .unwrap()
    .into_array()
}

fn wrapper(core: ArrayRef, shredded: Option<ArrayRef>) -> ArrayRef {
    VariantArray::try_new(core, shredded).unwrap().into_array()
}

#[derive(Default, Clone, Copy)]
enum Fault {
    #[default]
    None,
    Cancel(usize),
    Panic(usize),
}

#[derive(Debug, PartialEq)]
struct Wire {
    dtype: Vec<u8>,
    rows: usize,
    buffers_and_metadata: Vec<u8>,
    encodings: Vec<String>,
}

type Observation = (std::result::Result<Wire, String>, u64, usize);

fn observe(
    source: &ArrayRef,
    mode: Mode,
    bytes: u64,
    strings: usize,
    initial: u64,
    fault: Fault,
    old: bool,
) -> Observation {
    let (memory, session) = session(64 << 20);
    let calls = Cell::new(0);
    let check = || {
        calls.set(calls.get() + 1);
        match fault {
            Fault::Cancel(stop) if stop == calls.get() => Err(error("test cancellation")),
            Fault::Panic(stop) if stop == calls.get() => panic!("preparation callback"),
            _ => Ok(()),
        }
    };
    let limits = ConversionLimits {
        memory: &memory,
        check: &check,
        arrow_batch_bytes: bytes,
        string_bytes: strings,
    };
    let mut expanded = initial;
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut ctx = session.create_execution_ctx();
        let array = if old {
            match mode {
                Mode::Canonical => reference::canonical(source, &mut ctx, &limits, &mut expanded),
                Mode::Variant(depth) => {
                    reference::variant::prepare(source, &mut ctx, &limits, &mut expanded, depth)
                }
                Mode::LogicalShredded => {
                    reference::variant::logical_shredded(source, &mut ctx, &limits, &mut expanded)
                }
            }
        } else {
            prepare(source, &mut ctx, &limits, &mut expanded, mode, 64)
        }
        .map_err(|err| err.to_string())?;
        let arrays = ArrayContext::empty();
        let buffers_and_metadata = array
            .serialize(&arrays, &session, &SerializeOptions::default())
            .unwrap()
            .into_iter()
            .flatten()
            .collect();
        Ok(Wire {
            dtype: serde_json::to_vec(array.dtype()).unwrap(),
            rows: array.len(),
            buffers_and_metadata,
            encodings: arrays.to_ids().iter().map(ToString::to_string).collect(),
        })
    }))
    .unwrap_or_else(|panic| {
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"preparation callback"));
        Err("preparation callback panic".to_owned())
    });
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    (result, expanded, calls.get())
}

fn parity(
    source: &ArrayRef,
    mode: Mode,
    bytes: u64,
    strings: usize,
    initial: u64,
    fault: Fault,
) -> Observation {
    let expected = observe(source, mode, bytes, strings, initial, fault, true);
    assert_eq!(
        observe(source, mode, bytes, strings, initial, fault, false),
        expected
    );
    expected
}

#[test]
fn arrow_preparation_matches_original_values_errors_and_callback_order() {
    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();
    let text = VarBinArray::from_iter(
        [Some("λ"), None, Some("third")],
        DType::Utf8(Nullability::Nullable),
    )
    .into_array();
    let selected = text.slice(1..3).unwrap();
    let compact = list(selected.clone(), &[0, 1], &[1, 1], &mut ctx);
    let gap = list(text.clone(), &[0, 2], &[1, 1], &mut ctx);
    let fields = record(&["a", "b"], vec![compact.clone(), selected], 2);
    let raw = VariantBuilder::new()
        .with_value(PqValue::Int32(41))
        .finish()
        .1;
    let pq = parquet(2, Some(raw.clone()), None);
    let typed = parquet(2, None, Some(fields.clone()));
    let both = parquet(2, Some(raw), Some(fields.clone()));
    let core = ChunkedArray::try_new([pq.clone(), typed.clone()], pq.dtype().clone())
        .unwrap()
        .into_array();
    let masked = MaskedArray::try_new(
        core.clone(),
        Validity::from_iter([false, true, true, false]),
        &mut ctx,
    )
    .unwrap()
    .into_array();
    let shredded = wrapper(pq.clone(), Some(fields.clone()));
    let null =
        ConstantArray::new(Scalar::null(DType::Variant(Nullability::Nullable)), 2).into_array();
    let empty = record(&[], vec![], 0);
    let wide = StructArray::try_new(
        FieldNames::from_iter((0..257).map(|i| format!("field{i}"))),
        (0..257).map(|_| text.clone()),
        text.len(),
        Validity::from_iter([true, false, true]),
    )
    .unwrap()
    .into_array();
    for source in [
        uuid(),
        empty,
        wide,
        text.clone(),
        compact,
        fields,
        pq.clone(),
        typed,
        both,
        core,
        masked,
        null,
        wrapper(pq.clone(), None),
        shredded.clone(),
    ] {
        let (wire, _, _) = parity(
            &source,
            Mode::Canonical,
            u64::MAX,
            usize::MAX,
            17,
            Fault::None,
        );
        assert!(wire.is_ok(), "{wire:?}");
    }
    check_cancellation_and_budget(&shredded);
    check_malformed(&pq, gap, &text);
}

fn check_cancellation_and_budget(shredded: &ArrayRef) {
    let (ok, bytes, calls) = parity(
        shredded,
        Mode::Canonical,
        u64::MAX,
        usize::MAX,
        7,
        Fault::None,
    );
    assert!(ok.is_ok() && calls > 10);
    for stop in 1..=calls {
        let (err, _, at) = parity(
            shredded,
            Mode::Canonical,
            u64::MAX,
            usize::MAX,
            7,
            Fault::Cancel(stop),
        );
        assert!(err.unwrap_err().contains("test cancellation"));
        assert_eq!(at, stop);
    }
    let (panic, _, _) = parity(
        shredded,
        Mode::Canonical,
        u64::MAX,
        usize::MAX,
        7,
        Fault::Panic(calls - 1),
    );
    assert_eq!(panic.unwrap_err(), "preparation callback panic");
    for limit in [0, 129, 300, 900, bytes - 1] {
        assert!(
            parity(shredded, Mode::Canonical, limit, usize::MAX, 7, Fault::None)
                .0
                .is_err()
        );
    }
}

fn check_malformed(pq: &ArrayRef, gap: ArrayRef, text: &ArrayRef) {
    assert!(
        parity(text, Mode::Canonical, u64::MAX, 1, 0, Fault::None)
            .0
            .unwrap_err()
            .contains("string exceeds")
    );
    assert!(
        parity(
            text,
            Mode::Canonical,
            u64::MAX,
            usize::MAX,
            u64::MAX,
            Fault::None
        )
        .0
        .unwrap_err()
        .contains("overflow")
    );
    for source in [gap.clone(), wrapper(pq.clone(), Some(gap))] {
        let mode = if source.is::<ListView>() {
            Mode::LogicalShredded
        } else {
            Mode::Canonical
        };
        assert!(
            parity(&source, mode, u64::MAX, usize::MAX, 0, Fault::None)
                .0
                .unwrap_err()
                .contains("compact selected lists")
        );
    }
    for depth in [64, 65] {
        assert_eq!(
            parity(
                pq,
                Mode::Variant(depth),
                u64::MAX,
                usize::MAX,
                0,
                Fault::None
            )
            .0
            .is_ok(),
            depth == 64
        );
    }
    let malformed = parquet(2, Some(vec![3]), None);
    let competing = wrapper(
        malformed,
        Some(ConstantArray::new(Scalar::null(DType::Null), 2).into_array()),
    );
    assert!(
        parity(
            &competing,
            Mode::Canonical,
            u64::MAX,
            usize::MAX,
            0,
            Fault::None
        )
        .0
        .is_err()
    );
}

fn nested(mut child: ArrayRef, depth: usize, shape: usize, ctx: &mut ExecutionCtx) -> ArrayRef {
    for index in 0..depth {
        child = if shape == 0 || (shape == 2 && index % 2 == 0) {
            record(&["a"], vec![child], 1)
        } else {
            list(child, &[0], &[1], ctx)
        };
    }
    child
}

fn check_deep_failures(
    source: &ArrayRef,
    ctx: &mut ExecutionCtx,
    limits: &ConversionLimits<'_>,
    baseline: u64,
) {
    let memory = limits.memory;
    let held = memory
        .reserve(memory.snapshot().limit_bytes - baseline - (32 << 10))
        .unwrap();
    assert!(
        super::super::canonical(source, ctx, limits, &mut 0)
            .unwrap_err()
            .to_string()
            .contains("reservation denied")
    );
    drop(held);
    let invalid = nested(
        ConstantArray::new(Scalar::null(DType::Null), 1).into_array(),
        8192,
        2,
        ctx,
    );
    assert!(
        super::super::canonical(&invalid, ctx, limits, &mut 0)
            .unwrap_err()
            .to_string()
            .contains("changed its admitted dtype")
    );
    drop(invalid);
    for panic in [false, true] {
        let calls = Cell::new(0);
        let check = || {
            calls.set(calls.get() + 1);
            if calls.get() == 4096 {
                assert!(!panic, "deep preparation callback");
                return Err(error("test cancellation"));
            }
            Ok(())
        };
        let fault_limits = ConversionLimits {
            check: &check,
            ..*limits
        };
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            super::super::canonical(source, ctx, &fault_limits, &mut 0)
        }));
        if panic {
            assert!(outcome.is_err());
        } else {
            assert!(
                outcome
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .contains("test cancellation")
            );
        }
        assert_eq!(calls.get(), 4096);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
    }
    let late = record(
        &["good", "bad"],
        vec![
            source.clone(),
            ConstantArray::new(Scalar::null(DType::Null), 1).into_array(),
        ],
        1,
    );
    assert!(
        super::super::canonical(&late, ctx, limits, &mut 0)
            .unwrap_err()
            .to_string()
            .contains("changed its admitted dtype")
    );
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
}

fn deep_tree() {
    const DEPTH: usize = 8192;
    let (memory, session) = session(96 << 20);
    let mut ctx = session.create_execution_ctx();
    let limits = ConversionLimits {
        memory: &memory,
        check: &|| Ok(()),
        arrow_batch_bytes: u64::MAX,
        string_bytes: usize::MAX,
    };
    let raw = parquet(1, Some(vec![0]), None);
    for shape in 0..3 {
        let child = nested(uuid(), DEPTH, shape, &mut ctx);
        let field_count = [DEPTH, 0, DEPTH / 2][shape];
        assert_eq!(
            variant::names(child.dtype(), &memory).unwrap(),
            (field_count * 17) as u64
        );
        let source = wrapper(raw.clone(), Some(child));
        let baseline = memory.snapshot().reserved_bytes;
        let output = super::super::canonical(&source, &mut ctx, &limits, &mut 0).unwrap();
        let shredded = output.as_::<Variant>().shredded().unwrap().clone();
        drop(output);
        if shape != 0 {
            assert!(
                memory.snapshot().reserved_bytes > baseline,
                "retained logical list offsets lost their grant"
            );
        }
        let mut current = shredded;
        for index in (0..DEPTH).rev() {
            current = if shape == 0 || (shape == 2 && index % 2 == 0) {
                current.as_::<Struct>().unmasked_field(0).clone()
            } else {
                let list = current.as_::<List>();
                assert_eq!(
                    list.offsets()
                        .execute_scalar(1, &mut ctx)
                        .unwrap()
                        .as_primitive()
                        .as_::<u64>(),
                    Some(1)
                );
                list.elements().clone()
            };
        }
        assert_eq!(current.dtype(), uuid().dtype());
        assert_eq!(
            current.execute_scalar(0, &mut ctx).unwrap(),
            uuid().execute_scalar(0, &mut ctx).unwrap()
        );
        drop(current);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        if shape == 2 {
            check_deep_failures(&source, &mut ctx, &limits, baseline);
        }
        drop(source);
    }
    let mut wrapped = raw;
    for _ in 0..DEPTH {
        wrapped = wrapper(wrapped, None);
    }
    assert!(
        super::super::canonical(&wrapped, &mut ctx, &limits, &mut 0)
            .unwrap_err()
            .to_string()
            .contains("wrapper depth")
    );
    let mut expanded = 0;
    let output = prepare(
        &wrapped,
        &mut ctx,
        &limits,
        &mut expanded,
        Mode::Canonical,
        DEPTH,
    )
    .unwrap();
    let mut current = output;
    for _ in 0..DEPTH {
        current = current.as_::<Variant>().core_storage().clone();
    }
    assert!(current.is::<ParquetVariant>());
    drop((current, wrapped, ctx, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn arrow_preparation_deep_export_inputs_and_cleanup_on_small_stack() {
    const CHILD: &str = "SHARDLOOM_DEEP_ARROW_PREPARATION_CHILD";
    if std::env::var_os(CHILD).is_some() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(deep_tree)
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            concat!(
                module_path!(),
                "::arrow_preparation_deep_export_inputs_and_cleanup_on_small_stack"
            )
            .strip_prefix("shardloom_vortex::")
            .unwrap(),
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
