// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::cell::RefCell;
use std::env;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::thread;

use prost::Message;
use vortex_error::{VortexResult, vortex_err};
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use crate::dtype::extension::{ExtDType, ExtId, ExtVTable, ForeignExtDType};
use crate::dtype::proto::dtype::{self as pb, d_type::DtypeType};
use crate::dtype::session::DTypeSessionExt;
use crate::dtype::{DType, DecimalDType, Nullability, PType, StructFields, UnionVariants};
use crate::flatbuffers::{FlatBuffer, WriteFlatBufferExt};
use crate::proto::scalar as scalar_pb;
use crate::scalar::ScalarValue;

#[path = "proto_reference.rs"]
mod reference;

const KINDS: [&str; 7] = [
    "list",
    "fixed_list",
    "map_key",
    "map_value",
    "struct",
    "union",
    "extension",
];

fn wrap(kind: &str, child: DType) -> DType {
    let n = Nullability::NonNullable;
    match kind {
        "list" => DType::List(Arc::new(child), n),
        "fixed_list" => DType::FixedSizeList(Arc::new(child), 7, n),
        "map_key" => DType::map(child, DType::Bool(n), true, n).unwrap(),
        "map_value" => DType::map(DType::Bool(n), child, false, n).unwrap(),
        "struct" => DType::Struct(StructFields::from_iter([("child", child)]), n),
        "union" => DType::Union(
            UnionVariants::try_new(["child"].into(), vec![child], vec![255]).unwrap(),
            n,
        ),
        "extension" => {
            static ID: CachedId = CachedId::new("test.proto.foreign");
            DType::Extension(ForeignExtDType::from_parts(*ID, vec![0, 255, 7], child).unwrap())
        }
        _ => panic!("unknown protobuf test shape"),
    }
}

fn chain(kind: &str, depth: usize) -> DType {
    (0..depth).fold(DType::Bool(Nullability::NonNullable), |child, index| {
        wrap(
            if kind == "mixed" {
                KINDS[index % KINDS.len()]
            } else {
                kind
            },
            child,
        )
    })
}

fn message(kind: DtypeType) -> pb::DType {
    pb::DType {
        dtype_type: Some(kind),
    }
}

fn invalid() -> pb::DType {
    message(DtypeType::Primitive(pb::Primitive {
        r#type: 999,
        nullable: false,
    }))
}

fn outcome(operation: impl FnOnce() -> VortexResult<DType>) -> Result<DType, String> {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(result) => result.map_err(|error| format!("error: {error}")),
        Err(panic) => Err(format!(
            "panic: {}",
            panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic")
        )),
    }
}

#[test]
fn iterative_dtype_proto_matches_original_objects_bytes_and_lazy_fields() {
    let session = VortexSession::empty();
    session.allow_unknown();
    let mut fixtures = vec![
        DType::Null,
        DType::Struct(StructFields::empty(), Nullability::Nullable),
    ];
    for nullable in [Nullability::NonNullable, Nullability::Nullable] {
        fixtures.extend([
            DType::Bool(nullable),
            DType::Utf8(nullable),
            DType::Binary(nullable),
            DType::Variant(nullable),
            DType::Decimal(DecimalDType::try_new(38, -4).unwrap(), nullable),
        ]);
        for ptype in [
            PType::U8,
            PType::U16,
            PType::U32,
            PType::U64,
            PType::I8,
            PType::I16,
            PType::I32,
            PType::I64,
            PType::F16,
            PType::F32,
            PType::F64,
        ] {
            fixtures.push(DType::Primitive(ptype, nullable));
        }
        for kind in KINDS.into_iter().chain(["mixed"]) {
            fixtures.push(chain(kind, 4).with_nullability(nullable));
        }
        fixtures.push(DType::Struct(
            StructFields::from_iter([
                ("same\"é", DType::Bool(nullable)),
                ("same\"é", chain("mixed", 3)),
            ]),
            nullable,
        ));
        fixtures.push(DType::Union(
            UnionVariants::try_new(
                ["zero", "max", "gap"].into(),
                vec![
                    DType::Bool(nullable),
                    chain("map_value", 2),
                    chain("extension", 2),
                ],
                vec![0, 255, 7],
            )
            .unwrap(),
            nullable,
        ));
    }
    fixtures.push(DType::Struct(
        StructFields::from_iter(
            (0..2048).map(|i| (format!("field{i}"), DType::Bool(Nullability::NonNullable))),
        ),
        Nullability::NonNullable,
    ));
    for dtype in fixtures {
        let lazy = DType::from_flatbuffer(
            FlatBuffer::from(dtype.write_flatbuffer_bytes().unwrap()),
            &session,
        )
        .unwrap();
        for dtype in [dtype, lazy] {
            let expected = reference::write(&dtype).unwrap();
            let actual = pb::DType::try_from(&dtype).unwrap();
            assert_eq!(actual, expected);
            assert_eq!(actual.encode_to_vec(), expected.encode_to_vec());
            let converted = DType::from_proto(&actual, &session).unwrap();
            assert_eq!(converted, reference::read(&expected, &session).unwrap());
            assert_eq!(converted, dtype);
        }
    }
}

#[test]
fn iterative_dtype_proto_keeps_malformed_errors_and_constructor_panics() {
    let session = VortexSession::empty();
    let leaf = || message(DtypeType::Bool(pb::Bool { nullable: false }));
    let fixtures = vec![
        pb::DType::default(),
        invalid(),
        message(DtypeType::Decimal(pb::Decimal {
            precision: 256,
            scale: 0,
            nullable: false,
        })),
        message(DtypeType::Decimal(pb::Decimal {
            precision: 1,
            scale: 128,
            nullable: false,
        })),
        message(DtypeType::List(Box::default())),
        message(DtypeType::FixedSizeList(Box::default())),
        message(DtypeType::Map(Box::default())),
        message(DtypeType::Map(Box::new(pb::Map {
            key_type: Some(Box::new(invalid())),
            ..Default::default()
        }))),
        message(DtypeType::Map(Box::new(pb::Map {
            key_type: Some(Box::new(leaf())),
            ..Default::default()
        }))),
        message(DtypeType::Map(Box::new(pb::Map {
            key_type: Some(Box::new(message(DtypeType::Bool(pb::Bool {
                nullable: true,
            })))),
            value_type: Some(Box::new(leaf())),
            ..Default::default()
        }))),
        message(DtypeType::Struct(pb::Struct {
            names: vec!["missing".into()],
            ..Default::default()
        })),
        message(DtypeType::Union(pb::Union {
            names: vec!["x".into()],
            dtypes: vec![invalid()],
            type_ids: vec![256],
            nullable: false,
        })),
        message(DtypeType::Union(pb::Union {
            names: vec!["x".into()],
            dtypes: vec![leaf()],
            type_ids: vec![-1],
            nullable: false,
        })),
        message(DtypeType::Union(pb::Union {
            names: vec!["x".into()],
            dtypes: vec![leaf()],
            type_ids: vec![256],
            nullable: false,
        })),
        message(DtypeType::Union(pb::Union {
            names: vec!["x".into(), "y".into()],
            dtypes: vec![leaf(), leaf()],
            type_ids: vec![7, 7],
            nullable: false,
        })),
        message(DtypeType::Extension(Box::new(pb::Extension {
            id: "test.unknown".into(),
            ..Default::default()
        }))),
        message(DtypeType::Extension(Box::new(pb::Extension {
            id: "test.unknown".into(),
            storage_dtype: Some(Box::new(leaf())),
            metadata: None,
        }))),
    ];
    for (index, fixture) in fixtures.iter().enumerate() {
        let original = outcome(|| reference::read(fixture, &session));
        assert!(original.is_err(), "negative fixture {index}");
        assert_eq!(
            outcome(|| DType::from_proto(fixture, &session)),
            original,
            "fixture {index}"
        );
    }
}

thread_local! {
    static EVENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn events() -> Vec<String> {
    EVENTS.with(|events| std::mem::take(&mut *events.borrow_mut()))
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
struct Probe;

impl ExtVTable for Probe {
    type Metadata = u8;
    type NativeValue<'a> = &'a ScalarValue;
    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("test.proto.order");
        *ID
    }
    fn serialize_metadata(&self, id: &u8) -> VortexResult<Vec<u8>> {
        EVENTS.with(|events| events.borrow_mut().push(format!("write{id}")));
        if *id == 255 {
            return Err(vortex_err!("test metadata write failure"));
        }
        Ok(vec![*id])
    }
    fn deserialize_metadata(&self, bytes: &[u8]) -> VortexResult<u8> {
        let id = *bytes
            .first()
            .ok_or_else(|| vortex_err!("missing test metadata"))?;
        EVENTS.with(|events| events.borrow_mut().push(format!("read{id}")));
        if id == 255 {
            return Err(vortex_err!("test metadata read failure"));
        }
        Ok(id)
    }
    fn validate_dtype(dtype: &ExtDType<Self>) -> VortexResult<()> {
        EVENTS.with(|events| {
            events
                .borrow_mut()
                .push(format!("validate{}", dtype.metadata()))
        });
        Ok(())
    }
    fn unpack_native<'a>(
        _: &'a ExtDType<Self>,
        value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        Ok(value)
    }
}

fn probe(id: u8, child: DType) -> DType {
    DType::Extension(ExtDType::<Probe>::try_new(id, child).unwrap().erased())
}

fn proto_probe(id: u8, child: pb::DType) -> pb::DType {
    message(DtypeType::Extension(Box::new(pb::Extension {
        id: "test.proto.order".into(),
        storage_dtype: Some(Box::new(child)),
        metadata: Some(vec![id]),
    })))
}

#[test]
fn iterative_dtype_proto_preserves_metadata_and_lazy_failure_order() {
    let session = VortexSession::empty();
    session.dtypes().register(Probe);
    for fail in [false, true] {
        let second = if fail { 255 } else { 3 };
        let dtype = probe(
            1,
            DType::Struct(
                StructFields::from_iter([
                    ("first", probe(2, DType::Bool(Nullability::NonNullable))),
                    ("second", probe(second, DType::Utf8(Nullability::Nullable))),
                    ("third", probe(4, DType::Null)),
                ]),
                Nullability::NonNullable,
            ),
        );
        let mut snapshots = Vec::new();
        for original in [true, false] {
            events();
            let result = if original {
                reference::write(&dtype)
            } else {
                pb::DType::try_from(&dtype)
            };
            snapshots.push((
                result
                    .map(|value| value.encode_to_vec())
                    .map_err(|error| error.to_string()),
                events(),
            ));
        }
        assert_eq!(snapshots[0], snapshots[1]);
        let wire = proto_probe(
            1,
            message(DtypeType::Struct(pb::Struct {
                names: vec!["first".into(), "second".into(), "third".into()],
                dtypes: vec![
                    proto_probe(
                        2,
                        pb::DType::try_from(&DType::Bool(Nullability::NonNullable)).unwrap(),
                    ),
                    proto_probe(
                        second,
                        pb::DType::try_from(&DType::Utf8(Nullability::Nullable)).unwrap(),
                    ),
                    proto_probe(4, pb::DType::try_from(&DType::Null).unwrap()),
                ],
                nullable: false,
            })),
        );
        let mut snapshots = Vec::new();
        for original in [true, false] {
            events();
            let result = if original {
                reference::read(&wire, &session)
            } else {
                DType::from_proto(&wire, &session)
            };
            snapshots.push((result.map_err(|error| error.to_string()), events()));
        }
        assert_eq!(snapshots[0], snapshots[1]);
    }
    // Each lazy source is fresh: neither codec benefits from the other's cache.
    for is_map in [false, true] {
        let unknown = wrap("extension", DType::Bool(Nullability::NonNullable));
        let first = probe(2, DType::Bool(Nullability::NonNullable));
        let dtype = if is_map {
            DType::map(first, unknown, false, Nullability::NonNullable).unwrap()
        } else {
            DType::Struct(
                StructFields::from_iter([("first", first), ("bad", unknown)]),
                Nullability::NonNullable,
            )
        };
        let bytes = dtype.write_flatbuffer_bytes().unwrap();
        let mut snapshots = Vec::new();
        for original in [true, false] {
            let lazy = DType::from_flatbuffer(FlatBuffer::from(bytes.clone()), &session).unwrap();
            events();
            let result = catch_unwind(AssertUnwindSafe(|| {
                if original {
                    reference::write(&lazy)
                } else {
                    pb::DType::try_from(&lazy)
                }
            }));
            assert!(result.is_err());
            snapshots.push(events());
        }
        assert_eq!(snapshots[0], snapshots[1]);
        assert_eq!(
            snapshots[0]
                .iter()
                .filter(|event| event.starts_with("write"))
                .count(),
            usize::from(!is_map)
        );
    }
}

fn scalar_chain(kind: &str, depth: usize) -> scalar_pb::ScalarValue {
    use scalar_pb::scalar_value::Kind;
    (0..depth).fold(
        scalar_pb::ScalarValue {
            kind: Some(Kind::BoolValue(true)),
        },
        |child, index| scalar_pb::ScalarValue {
            kind: Some(match kind {
                "list" => Kind::ListValue(scalar_pb::ListValue {
                    values: vec![child],
                }),
                "union" => Kind::UnionValue(Box::new(scalar_pb::UnionValue {
                    type_id: 255,
                    value: Some(Box::new(child)),
                })),
                "variant" => Kind::VariantValue(Box::new(scalar_pb::Scalar {
                    dtype: Some(pb::DType::try_from(&chain("mixed", index % 7)).unwrap()),
                    value: Some(Box::new(child)),
                })),
                _ => unreachable!(),
            }),
        },
    )
}

#[test]
fn deep_dtype_proto_conversion_and_temporary_cleanup_on_small_stack() {
    const SELECTOR: &str = "SHARDLOOM_DEEP_DTYPE_PROTO";
    const COMPLETED: &str = "deep dtype proto completed";
    if env::var_os(SELECTOR).is_some() {
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(|| {
                let session = VortexSession::empty();
                session.allow_unknown();
                for kind in KINDS.into_iter().chain(["mixed"]) {
                    let native = chain(kind, 8192);
                    let wire = pb::DType::try_from(&native).unwrap();
                    let restored = DType::from_proto(&wire, &session).unwrap();
                    assert_eq!(native, restored);
                    drop((native, restored));
                    // Detached ownership and an earlier constructed deep sibling
                    // survive a later conversion failure without recursive drop.
                    let parent = message(DtypeType::Struct(pb::Struct {
                        names: vec!["built".into(), "bad".into()],
                        dtypes: vec![wire, invalid()],
                        nullable: false,
                    }));
                    assert!(DType::from_proto(&parent, &session).is_err());
                    drop(parent);
                    let detached = Box::new(pb::DType::try_from(&chain(kind, 8192)).unwrap());
                    drop(detached);
                    let bad = (0..8192).fold(
                        probe(255, DType::Bool(Nullability::NonNullable)),
                        |child, index| {
                            wrap(
                                if kind == "mixed" {
                                    KINDS[index % KINDS.len()]
                                } else {
                                    kind
                                },
                                child,
                            )
                        },
                    );
                    let partial = DType::Struct(
                        StructFields::from_iter([("built", chain(kind, 8192)), ("bad", bad)]),
                        Nullability::NonNullable,
                    );
                    let error = pb::DType::try_from(&partial).err().unwrap();
                    assert!(error.to_string().contains("test metadata write failure"));
                    drop(partial);
                }
                for kind in ["list", "union", "variant"] {
                    let value = scalar_chain(kind, 8192);
                    drop(Box::new(value));
                    let result = catch_unwind(AssertUnwindSafe(|| {
                        let _partial = scalar_chain(kind, 8192);
                        panic!("test partial protobuf cleanup");
                    }));
                    assert!(result.is_err());
                }
            })
            .unwrap()
            .join()
            .unwrap();
        println!("\n{COMPLETED}");
        return;
    }
    let output = Command::new(env::current_exe().unwrap()).args([
        "--exact", "dtype::serde::proto::iterative_tests::deep_dtype_proto_conversion_and_temporary_cleanup_on_small_stack",
        "--test-threads=1", "--nocapture",
    ]).env(SELECTOR, "1").output().unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line == COMPLETED)
    );
}
