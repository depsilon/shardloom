use super::{reference, visit};
use crate::{
    local_primitives::vortex_error, native_binary_type as binary,
    owned_buffers::ReservedHostAllocator,
};
use shardloom_core::Result;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray, VortexSessionExecute as _,
        arrays::{
            BoolArray, ExtensionArray, FixedSizeListArray, ListViewArray, PrimitiveArray,
            StructArray, VarBinViewArray,
        },
        dtype::{DType, FieldNames, Nullability, PType, StructFields},
        flatbuffers::{FlatBuffer, WriteFlatBufferExt as _},
        memory::{HostMemoryReservation, HostMemorySessionExt as _},
        validity::Validity,
    },
    session::VortexSession,
};

fn session(pool: &LiveMemoryPool) -> VortexSession {
    let session = VortexSession::default()
        .with_host_allocator(Arc::new(ReservedHostAllocator::new(pool.clone())));
    binary::register(&session);
    session
}

fn record(fields: impl IntoIterator<Item = (&'static str, DType)>) -> DType {
    DType::Struct(StructFields::from_iter(fields), Nullability::NonNullable)
}

#[test]
fn binary_traversal_classifiers_keep_leaf_domains_and_lazy_first_errors() {
    let live = LiveMemoryPool::new(16 << 20).unwrap();
    let host = LiveMemoryPool::new(16 << 20).unwrap();
    let owner = session(&host);
    let object = binary::dtype(true).unwrap();
    let num = DType::Primitive(PType::I32, Nullability::NonNullable);
    let map = DType::map(num.clone(), object.clone(), false, Nullability::Nullable).unwrap();
    let list = DType::List(object.clone().into(), Nullability::Nullable);
    let fixed = DType::FixedSizeList(list.into(), 2, Nullability::Nullable);
    for src in [
        object.clone(),
        num.clone(),
        map,
        record([]),
        record([("a", num.clone()), ("b", fixed)]),
    ] {
        let lazy = DType::from_flatbuffer(
            FlatBuffer::from(src.write_flatbuffer_bytes().unwrap()),
            &owner,
        )
        .unwrap();
        for dtype in [&src, &lazy] {
            let expected = binary::reference::contains(dtype).unwrap();
            let before = host.snapshot().reserved_bytes;
            assert_eq!(
                binary::contains(dtype, (&live).into(), vortex_error).unwrap(),
                expected
            );
            assert_eq!(
                binary::contains_in_session(dtype, &owner, None).unwrap(),
                expected
            );
            assert_eq!(host.snapshot().reserved_bytes, before);
            assert_eq!(live.snapshot().reserved_bytes, 0);
        }
    }
    for match_first in [false, true] {
        let provider = LiveMemoryPool::new(1 << 20).unwrap();
        let lazy_owner = session(&provider);
        let other = record([("hidden", num.clone())]);
        let dtype = if match_first {
            record([("object", object.clone()), ("other", other)])
        } else {
            record([("other", other), ("object", object.clone())])
        };
        let lazy = DType::from_flatbuffer(
            FlatBuffer::from(dtype.write_flatbuffer_bytes().unwrap()),
            &lazy_owner,
        )
        .unwrap();
        if match_first {
            // Cache only the matched extension. The later lazy field must stay untouched.
            lazy.as_struct_fields()
                .try_field_dtypes()
                .next()
                .unwrap()
                .unwrap();
        }
        let pressure = provider
            .reserve((1 << 20) - provider.snapshot().reserved_bytes)
            .unwrap();
        let expected = binary::reference::contains(&lazy);
        let native = binary::contains(&lazy, (&live).into(), vortex_error);
        let external = binary::contains_in_session(&lazy, &owner, None);
        if match_first {
            assert!(expected.unwrap() && native.unwrap() && external.unwrap());
        } else {
            let expected = expected.unwrap_err();
            assert_eq!(external.unwrap_err().to_string(), expected.to_string());
            assert_eq!(
                native.unwrap_err().to_string(),
                vortex_error(expected).to_string()
            );
        }
        drop(pressure);
        drop(lazy);
        assert_eq!(provider.snapshot().reserved_bytes, 0);
    }
    assert_eq!(host.snapshot().reserved_bytes, 0);
    assert_eq!(live.snapshot().reserved_bytes, 0);
}

fn cells(bytes: &[Option<u8>]) -> ArrayRef {
    let raw = VarBinViewArray::from_iter_nullable_bin(bytes.iter().map(|byte| byte.map(|b| [b])))
        .into_array();
    let dtype = binary::dtype(raw.dtype().is_nullable()).unwrap();
    ExtensionArray::new(dtype.as_extension().clone(), raw).into_array()
}

fn fixture() -> ArrayRef {
    let mut ctx = VortexSession::default().create_execution_ctx();
    let list = ListViewArray::new(
        cells(&[Some(20), Some(21), None, Some(23), Some(24), Some(25)]),
        PrimitiveArray::from_iter([1u32, 0, 4]).into_array(),
        PrimitiveArray::from_iter([2u32, 4, 1]).into_array(),
        Validity::NonNullable,
        &mut ctx,
    )
    .into_array();
    let fixed = FixedSizeListArray::new(
        cells(&[Some(30), None, Some(32), Some(33), Some(34), Some(35)]),
        2,
        Validity::NonNullable,
        3,
    )
    .into_array();
    StructArray::new(
        FieldNames::from_iter(["ordinary", "direct", "list", "fixed"]),
        vec![
            PrimitiveArray::from_iter([1i32, 2, 3]).into_array(),
            cells(&[Some(10), Some(11), Some(12)]),
            list,
            fixed,
        ],
        3,
        Validity::Array(BoolArray::from_iter([true, false, true]).into_array()),
    )
    .into_array()
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    Error(usize),
    Panic(usize),
    Cancel(usize),
}

fn observe(
    array: &ArrayRef,
    original: bool,
    fault: Fault,
) -> (Vec<u8>, std::result::Result<(), String>) {
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let token = CancellationToken::default();
    let mut ctx = VortexSession::default().create_execution_ctx();
    let mut seen = Vec::new();
    let mut callback = |bytes: &[u8]| -> Result<()> {
        assert_eq!(bytes.len(), 1);
        seen.push(bytes[0]);
        match fault {
            Fault::Error(i) if i == seen.len() => Err(super::error("TEST", "callback failed")),
            Fault::Panic(i) if i == seen.len() => panic!("descriptor test callback"),
            Fault::Cancel(i) if i == seen.len() => {
                token.cancel();
                Ok(())
            }
            _ => Ok(()),
        }
    };
    let res = catch_unwind(AssertUnwindSafe(|| {
        if original {
            reference::visit(array, &mut ctx, &token, &mut callback)
        } else {
            visit(array, &mut ctx, &memory, &token, &mut callback)
        }
    }))
    .map_or_else(
        |panic| {
            assert_eq!(
                panic.downcast_ref::<&str>(),
                Some(&"descriptor test callback")
            );
            Err("callback panic".into())
        },
        |res| res.map_err(|e| e.to_string()),
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    (seen, res)
}

#[test]
fn binary_traversal_preserves_descriptor_order_null_ancestors_and_callback_failures() {
    let array = fixture();
    for (array, expected) in [
        (array.clone(), vec![10, 12, 21, 24, 30, 34, 35]),
        (array.slice(1..3).unwrap(), vec![12, 24, 34, 35]),
        (array.slice(0..0).unwrap(), vec![]),
    ] {
        assert_eq!(
            observe(&array, true, Fault::None),
            (expected.clone(), Ok(()))
        );
        assert_eq!(
            observe(&array, false, Fault::None),
            (expected.clone(), Ok(()))
        );
        for i in 1..=expected.len() {
            for fault in [Fault::Error(i), Fault::Panic(i), Fault::Cancel(i)] {
                assert_eq!(observe(&array, false, fault), observe(&array, true, fault));
            }
        }
    }
}

#[test]
fn binary_traversal_refunds_denied_replacement_capacity() {
    let mut dtype = binary::dtype(false).unwrap();
    for _ in 0..8 {
        dtype = DType::List(dtype.into(), Nullability::NonNullable);
    }
    let handle = size_of::<HostMemoryReservation>() as u64;
    let host_retained = 9 * 1024 + 8 * handle;
    let host_requested = 16 * handle;
    let host_limit = host_retained + host_requested - 1;
    let denied = LiveMemoryPool::new(host_limit).unwrap();
    let err = binary::contains_in_session(&dtype, &session(&denied), None).unwrap_err();
    assert!(err.to_string().contains(&format!(
        "requested={host_requested}, reserved={host_retained}, limit={host_limit}"
    )));
    assert_eq!(denied.snapshot().reserved_bytes, 0);
    let admitted = LiveMemoryPool::new(host_limit + 1).unwrap();
    assert!(binary::contains_in_session(&dtype, &session(&admitted), None).unwrap());
    assert_eq!(admitted.snapshot().reserved_bytes, 0);

    // Each parent has a directly matched object before its deeper sibling.
    // Classification stays shallow while descriptor continuations accumulate.
    let leaf = cells(&[Some(7)]);
    let mut array = leaf.clone();
    for _ in 0..70 {
        array = StructArray::new(
            FieldNames::from_iter(["object", "nested"]),
            vec![leaf.clone(), array],
            1,
            Validity::NonNullable,
        )
        .into_array();
    }
    let limit = 16 << 20;
    let memory = LiveMemoryPool::new(limit).unwrap();
    let mut ctx = VortexSession::default().create_execution_ctx();
    let mut seen = 0;
    let mut pressure = None;
    let mut overlap = 0;
    let error = visit(
        &array,
        &mut ctx,
        &memory,
        &CancellationToken::default(),
        &mut |bytes| {
            assert_eq!(bytes, [7]);
            seen += 1;
            if seen == 64 {
                let retained = memory.snapshot().reserved_bytes;
                overlap = 2 * retained;
                assert!(overlap >= 4096);
                pressure = Some(memory.reserve(limit - retained - overlap + 1)?);
            }
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(seen, 64);
    assert!(error.to_string().contains(&format!(
        "requested={overlap}, reserved={}, limit={limit}",
        limit - overlap + 1
    )));
    let pressure = pressure.unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, pressure.bytes());
    drop(pressure);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn deep() {
    let memory = LiveMemoryPool::new(64 << 20).unwrap();
    let owner = session(&memory);
    let mut ctx = owner.create_execution_ctx();
    let mut array = cells(&[Some(91)]);
    for depth in 0..8192 {
        array = match depth % 3 {
            0 => StructArray::new(
                FieldNames::from_iter(["x"]),
                vec![array],
                1,
                Validity::NonNullable,
            )
            .into_array(),
            1 => FixedSizeListArray::new(array, 1, Validity::NonNullable, 1).into_array(),
            _ => ListViewArray::new(
                array,
                PrimitiveArray::from_iter([0u32]).into_array(),
                PrimitiveArray::from_iter([1u32]).into_array(),
                Validity::NonNullable,
                &mut ctx,
            )
            .into_array(),
        };
    }
    let token = CancellationToken::default();
    let before = memory.snapshot().reserved_bytes;
    assert!(binary::contains(array.dtype(), (&memory, &token).into(), vortex_error).unwrap());
    assert!(binary::contains_in_session(array.dtype(), &owner, Some(&token)).unwrap());
    let narrow = LiveMemoryPool::new(32 << 10).unwrap();
    let narrow_owner = session(&narrow);
    let held = narrow.reserve(2048).unwrap();
    assert!(binary::contains(array.dtype(), (&narrow).into(), vortex_error).is_err());
    assert!(binary::contains_in_session(array.dtype(), &narrow_owner, None).is_err());
    assert!(
        visit(&array, &mut ctx, &narrow, &token, &mut |_| panic!(
            "denied before leaf"
        ))
        .is_err()
    );
    assert_eq!(narrow.snapshot().reserved_bytes, 2048);
    drop(held);
    let mut seen = Vec::new();
    visit(&array, &mut ctx, &memory, &token, &mut |bytes| {
        seen.extend_from_slice(bytes);
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, [91]);
    token.cancel();
    assert!(
        visit(&array, &mut ctx, &memory, &token, &mut |_| panic!(
            "cancelled before leaf"
        ))
        .is_err()
    );
    assert!(binary::contains_in_session(array.dtype(), &owner, Some(&token)).is_err());
    // Provider execution may cache array metadata in its original owner.
    assert!(memory.snapshot().reserved_bytes >= before);
    drop(array);
    drop(ctx);
    drop(owner);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(narrow.snapshot().reserved_bytes, 0);
}

#[test]
fn binary_traversal_deep_mixed_values_and_owner_release_on_small_stack() {
    const CHILD: &str = "SHARDLOOM_BINARY_DESCRIPTOR_DEEP";
    if std::env::var_os(CHILD).is_some() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(deep)
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let name = concat!(
        module_path!(),
        "::binary_traversal_deep_mixed_values_and_owner_release_on_small_stack"
    );
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            name.strip_prefix("shardloom_vortex::").unwrap(),
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
