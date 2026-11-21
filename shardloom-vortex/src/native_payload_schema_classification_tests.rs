use super::*;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        dtype::{Nullability, PType, StructFields, UnionVariants, extension::ExtDType},
        extension::datetime::{TimeUnit, Timestamp},
        flatbuffers::{FlatBuffer, WriteFlatBufferExt as _},
        memory::HostMemorySessionExt as _,
    },
    session::VortexSession,
};

fn record(fields: impl IntoIterator<Item = (&'static str, DType)>) -> DType {
    DType::Struct(StructFields::from_iter(fields), Nullability::Nullable)
}

fn number(ptype: PType) -> DType {
    DType::Primitive(ptype, Nullability::NonNullable)
}

fn extension_leaves() -> Vec<DType> {
    let element = number(PType::F16);
    let fixed = DType::FixedSizeList(element.clone().into(), 2, Nullability::Nullable);
    vec![
        DType::Extension(Timestamp::new(TimeUnit::Nanoseconds, Nullability::Nullable).erased()),
        DType::Extension(
            ExtDType::<crate::native_tensor::FixedShapeTensor>::try_new(
                crate::native_tensor::Metadata::parse(r#"{"shape":[2]}"#).unwrap(),
                fixed.clone(),
            )
            .unwrap()
            .erased(),
        ),
        DType::Extension(
            ExtDType::<crate::native_embedding::Embedding>::try_new(
                crate::native_embedding::Metadata::parse(r#"{"version":1}"#).unwrap(),
                fixed,
            )
            .unwrap()
            .erased(),
        ),
        DType::Extension(
            ExtDType::<crate::native_variable_tensor::VariableShapeTensor>::try_new(
                crate::native_variable_tensor::Metadata::parse("{}").unwrap(),
                record([
                    (
                        "data",
                        DType::List(element.clone().into(), Nullability::Nullable),
                    ),
                    (
                        "shape",
                        DType::FixedSizeList(
                            number(PType::I32).into(),
                            1,
                            Nullability::NonNullable,
                        ),
                    ),
                ]),
            )
            .unwrap()
            .erased(),
        ),
        crate::native_vector::dtype(
            crate::native_vector::Metadata::parse(
                crate::native_vector::Kind::Sparse,
                r#"{"version":1,"index_policy":"preserve"}"#,
            )
            .unwrap(),
            record([
                ("size", number(PType::U64)),
                (
                    "indices",
                    DType::List(number(PType::U32).into(), Nullability::Nullable),
                ),
                (
                    "values",
                    DType::List(element.clone().into(), Nullability::Nullable),
                ),
            ]),
        )
        .unwrap(),
        crate::native_vector::dtype(
            crate::native_vector::Metadata::parse(
                crate::native_vector::Kind::Quantized,
                r#"{"version":1,"parameter_mode":"per_vector"}"#,
            )
            .unwrap(),
            record([
                (
                    "codes",
                    DType::List(number(PType::I8).into(), Nullability::Nullable),
                ),
                ("scale", element),
                ("zero_point", number(PType::I8)),
            ]),
        )
        .unwrap(),
    ]
}

fn current(dtype: &DType, memory: &LiveMemoryPool) -> [bool; 6] {
    let scope = memory.into();
    [
        crate::native_variant::contains(dtype, scope).unwrap(),
        crate::native_vector::contains(dtype, scope).unwrap(),
        crate::native_tensor::contains(dtype, scope).unwrap(),
        crate::native_variable_tensor::contains(dtype, scope).unwrap(),
        crate::native_embedding::contains(dtype, scope).unwrap(),
        contains_type(dtype, scope, |dtype| {
            matches!(dtype, DType::Primitive(PType::F16, _))
        })
        .unwrap(),
    ]
}

fn original(dtype: &DType) -> [bool; 6] {
    [
        reference::variant::contains(dtype),
        reference::vector::contains(dtype),
        reference::tensor::contains(dtype),
        reference::variable_tensor::contains(dtype),
        reference::embedding::contains(dtype),
        reference::float16::contains_float16(dtype),
    ]
}

fn lazy(dtype: &DType, session: &VortexSession) -> DType {
    DType::from_flatbuffer(
        FlatBuffer::from(dtype.write_flatbuffer_bytes().unwrap()),
        session,
    )
    .unwrap()
}

#[test]
fn payload_classification_matches_six_original_predicates_without_crossing_leaf_domains() {
    let memory = LiveMemoryPool::new(32 << 20).unwrap();
    let mut leaves = extension_leaves();
    leaves.extend([
        DType::Variant(Nullability::Nullable),
        number(PType::F16),
        number(PType::I64),
        DType::Null,
        record([]),
        DType::map(
            number(PType::I64),
            DType::Variant(Nullability::Nullable),
            false,
            Nullability::Nullable,
        )
        .unwrap(),
        DType::Union(
            UnionVariants::new(
                ["variant", "half"].into(),
                vec![DType::Variant(Nullability::Nullable), number(PType::F16)],
            )
            .unwrap(),
            Nullability::Nullable,
        ),
    ]);
    let session = VortexSession::default();
    crate::native_extensions::register(&session);
    for leaf in leaves {
        for nullable in [Nullability::Nullable, Nullability::NonNullable] {
            if matches!(leaf, DType::Null) && nullable == Nullability::NonNullable {
                continue;
            }
            let leaf = leaf.with_nullability(nullable);
            let expected = original(&leaf);
            let mut dtype = leaf.clone();
            for depth in 0..7 {
                for source in [&dtype, &lazy(&dtype, &session)] {
                    assert_eq!(current(source, &memory), original(source));
                    assert_eq!(current(source, &memory), expected);
                    assert_eq!(memory.snapshot().reserved_bytes, 0);
                }
                dtype = match depth % 3 {
                    0 => DType::List(dtype.into(), nullable),
                    1 => DType::FixedSizeList(dtype.into(), 2, nullable),
                    _ => record([
                        ("first", number(PType::I64)),
                        ("nested", dtype),
                        ("last", number(PType::U64)),
                    ]),
                };
            }
        }
    }
    // An extension's F16 storage remains outside the Float16 classifier.
    for dtype in extension_leaves() {
        assert!(!current(&dtype, &memory)[5]);
    }
}

#[test]
fn payload_classification_preserves_lazy_short_circuit_and_propagates_owner_denial() {
    let provider = LiveMemoryPool::new(1 << 20).unwrap();
    let inspection = LiveMemoryPool::new(1 << 20).unwrap();
    let session = VortexSession::default().with_host_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(provider.clone()),
    ));
    for matched_first in [true, false] {
        let nested = record([("hidden", number(PType::I64))]);
        let variant = DType::Variant(Nullability::Nullable);
        let source = if matched_first {
            record([("match", variant), ("denied", nested)])
        } else {
            record([("denied", nested), ("match", variant)])
        };
        let dtype = lazy(&source, &session);
        let initial = provider.snapshot().reserved_bytes;
        let blocker = provider.reserve((1 << 20) - initial).unwrap();
        let result = crate::native_variant::contains(&dtype, (&inspection).into());
        if matched_first {
            assert!(result.unwrap());
            assert_eq!(provider.snapshot().denied_reservations, 0);
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("provider metadata inspection failed")
            );
            assert!(provider.snapshot().denied_reservations > 0);
        }
        assert_eq!(inspection.snapshot().reserved_bytes, 0);
        drop(blocker);
        drop(dtype);
        assert_eq!(provider.snapshot().reserved_bytes, 0);
    }
}

fn deep_child() {
    let mut dtype = DType::Variant(Nullability::Nullable);
    for depth in 0..8192 {
        dtype = match depth % 3 {
            0 => DType::List(dtype.into(), Nullability::Nullable),
            1 => DType::FixedSizeList(dtype.into(), 1, Nullability::Nullable),
            _ => record([("nested", dtype), ("sibling", number(PType::I64))]),
        };
    }
    let memory = LiveMemoryPool::new(64 << 20).unwrap();
    assert!(crate::native_variant::contains(&dtype, (&memory).into()).unwrap());
    assert!(!crate::native_vector::contains(&dtype, (&memory).into()).unwrap());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert!(
        super::super::metadata_bytes(&dtype, &memory)
            .unwrap_err()
            .to_string()
            .contains("depth 24")
    );
    let narrow = LiveMemoryPool::new(32 << 10).unwrap();
    let unrelated = narrow.reserve(4096).unwrap();
    assert!(
        crate::native_variant::contains(&dtype, (&narrow).into())
            .unwrap_err()
            .to_string()
            .contains("reservation denied")
    );
    assert_eq!(narrow.snapshot().reserved_bytes, unrelated.bytes());
    drop(unrelated);
    assert_eq!(narrow.snapshot().reserved_bytes, 0);

    let wide = DType::Struct(
        StructFields::from_iter((0..8192).map(|i| (format!("field_{i}"), number(PType::I64)))),
        Nullability::Nullable,
    );
    let exact = LiveMemoryPool::new(8193 * 1024 + 4096).unwrap();
    assert!(!crate::native_variant::contains(&wide, (&exact).into()).unwrap());
    assert_eq!(exact.snapshot().reserved_bytes, 0);
    let mut shared = number(PType::I64);
    for _ in 0..20 {
        shared = record([("left", shared.clone()), ("right", shared)]);
    }
    assert!(crate::native_variant::contains(&shared, (&narrow).into()).is_err());
    assert_eq!(narrow.snapshot().reserved_bytes, 0);

    let token = CancellationToken::default();
    let visits = Cell::new(0);
    let result = contains_type(&dtype, (&memory, &token).into(), |_| {
        visits.set(visits.get() + 1);
        if visits.get() == 4096 {
            token.cancel();
        }
        false
    });
    assert!(result.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(visits.get(), 4096);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert!(crate::native_variant::contains(&dtype, (&memory, &token).into()).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let visits = Cell::new(0);
    let panic = catch_unwind(AssertUnwindSafe(|| {
        contains_type(&dtype, (&memory).into(), |_| {
            visits.set(visits.get() + 1);
            assert_ne!(visits.get(), 4096, "test predicate unwind");
            false
        })
    }));
    assert!(panic.is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn payload_classification_deep_work_denial_cancellation_and_unwind_on_small_stack() {
    const CHILD: &str = "SHARDLOOM_DEEP_PAYLOAD_CLASSIFICATION_CHILD";
    if std::env::var_os(CHILD).is_some() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(deep_child)
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
                "::payload_classification_deep_work_denial_cancellation_and_unwind_on_small_stack"
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
