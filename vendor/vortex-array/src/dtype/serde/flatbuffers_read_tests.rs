// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::cell::RefCell;
use std::env;
use std::fmt;
use std::process::Command;
use std::sync::Arc;
use std::thread;

use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer, UnionWIPOffset, WIPOffset};
use vortex_error::{VortexResult, vortex_err};
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use super::super::ViewedDType;
use crate::dtype::extension::{ExtDType, ExtId, ExtVTable, ForeignExtDType};
use crate::dtype::session::DTypeSessionExt;
use crate::dtype::{
    DType, DecimalDType, Nullability, PType, StructFields, UnionVariants, flatbuffers as fb,
};
use crate::flatbuffers::{FlatBuffer, WriteFlatBuffer};
use crate::scalar::ScalarValue;

#[path = "flatbuffers_read_reference.rs"]
mod reference;

type Offset<'a> = WIPOffset<fb::DType<'a>>;
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
    let nullable = Nullability::NonNullable;
    match kind {
        "list" => DType::List(Arc::new(child), nullable),
        "fixed_list" => DType::FixedSizeList(Arc::new(child), 7, nullable),
        "map_key" => DType::map(child, DType::Bool(nullable), true, nullable).unwrap(),
        "map_value" => DType::map(DType::Bool(nullable), child, false, nullable).unwrap(),
        "struct" => DType::Struct(StructFields::from_iter([("child", child)]), nullable),
        "union" => DType::Union(
            UnionVariants::try_new(["child"].into(), vec![child], vec![255]).unwrap(),
            nullable,
        ),
        "extension" => {
            static ID: CachedId = CachedId::new("test.dtype_reader_foreign");
            DType::Extension(ForeignExtDType::from_parts(*ID, vec![8, 1, 7], child).unwrap())
        }
        _ => panic!("unknown reader fixture kind"),
    }
}

fn chain(kind: &str, depth: usize, leaf: DType) -> DType {
    (0..depth).fold(leaf, |child, index| {
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

fn finish_view(
    mut builder: FlatBufferBuilder<'_>,
    offset: Offset<'_>,
    session: &VortexSession,
) -> ViewedDType {
    builder.finish_minimal(offset);
    let bytes = builder.finished_data();
    // The offset is returned by the generated writer, never inferred from
    // unverified input. Only these internal tests may skip the recursive verifier.
    let location = bytes.len() - offset.value() as usize;
    assert_eq!(
        location,
        u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize
    );
    ViewedDType::from_fb_loc(
        location,
        FlatBuffer::align_from(bytes.to_vec()),
        session.clone(),
    )
}

fn view(dtype: &DType, session: &VortexSession) -> ViewedDType {
    let mut builder = FlatBufferBuilder::new();
    let offset = dtype.write_flatbuffer(&mut builder).unwrap();
    finish_view(builder, offset, session)
}

fn root<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    tag: fb::Type,
    payload: WIPOffset<UnionWIPOffset>,
) -> Offset<'a> {
    fb::DType::create(
        builder,
        &fb::DTypeArgs {
            type_type: tag,
            type_: Some(payload),
        },
    )
}

fn extension<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    child: Option<Offset<'a>>,
    id: Option<&str>,
    metadata: Option<&[u8]>,
) -> Offset<'a> {
    let id = id.map(|id| builder.create_string(id));
    let metadata = metadata.map(|bytes| builder.create_vector(bytes));
    let payload = fb::Extension::create(
        builder,
        &fb::ExtensionArgs {
            id,
            storage_dtype: child,
            metadata,
        },
    );
    root(builder, fb::Type::Extension, payload.as_union_value())
}

#[test]
fn logical_reader_matches_recursive_reference_and_lazy_representation() {
    let session = VortexSession::empty();
    session.allow_unknown();
    let mut fixtures = vec![
        DType::Null,
        DType::Struct(StructFields::empty(), Nullability::Nullable),
    ];
    for nullable in [Nullability::NonNullable, Nullability::Nullable] {
        fixtures.extend([
            DType::Bool(nullable),
            DType::Binary(nullable),
            DType::Utf8(nullable),
            DType::Variant(nullable),
            DType::Decimal(DecimalDType::try_new(38, -4).unwrap(), nullable),
        ]);
        for primitive in [
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
            fixtures.push(DType::Primitive(primitive, nullable));
        }
        for kind in KINDS.into_iter().chain(["mixed"]) {
            fixtures.push(
                chain(kind, 7, DType::Bool(Nullability::NonNullable)).with_nullability(nullable),
            );
        }
    }
    fixtures.push(DType::Struct(
        StructFields::from_iter(
            (0..2048).map(|index| (format!("field{index}"), DType::Bool(Nullability::Nullable))),
        ),
        Nullability::NonNullable,
    ));
    fixtures.push(DType::Union(
        UnionVariants::try_new(
            ["zero", "sparse", "max"].into(),
            vec![
                DType::Null,
                DType::Bool(Nullability::NonNullable),
                DType::Utf8(Nullability::Nullable),
            ],
            vec![0, 7, 255],
        )
        .unwrap(),
        Nullability::Nullable,
    ));
    for (index, dtype) in fixtures.iter().enumerate() {
        let view = view(dtype, &session);
        // Shallow fixtures must also pass the real physical entry point.
        let public = DType::from_flatbuffer(view.buffer().clone(), &session).unwrap();
        let expected = reference::read_reference(view.clone()).unwrap();
        let actual = DType::try_from(view).unwrap();
        // Compare before equality can initialize additional lazy fields.
        assert_eq!(
            format!("{actual:?}"),
            format!("{expected:?}"),
            "raw view {index}"
        );
        assert!(
            actual == expected && actual == public && &actual == dtype,
            "fixture {index}"
        );
    }
}

fn bad_view(case: usize, wrap_storage: bool, session: &VortexSession) -> ViewedDType {
    let mut builder = FlatBufferBuilder::new();
    let invalid = fb::Primitive::create(
        &mut builder,
        &fb::PrimitiveArgs {
            ptype: fb::PType(255),
            nullable: false,
        },
    );
    let invalid = root(&mut builder, fb::Type::Primitive, invalid.as_union_value());
    let good = DType::Bool(Nullability::NonNullable)
        .write_flatbuffer(&mut builder)
        .unwrap();
    let mut offset = match case {
        0 => invalid,
        1 => {
            let payload = fb::Decimal::create(
                &mut builder,
                &fb::DecimalArgs {
                    precision: 0,
                    scale: 1,
                    nullable: false,
                },
            );
            root(&mut builder, fb::Type::Decimal, payload.as_union_value())
        }
        2 => {
            let payload = fb::List::create(&mut builder, &fb::ListArgs::default());
            root(&mut builder, fb::Type::List, payload.as_union_value())
        }
        3 => {
            let payload =
                fb::FixedSizeList::create(&mut builder, &fb::FixedSizeListArgs::default());
            root(
                &mut builder,
                fb::Type::FixedSizeList,
                payload.as_union_value(),
            )
        }
        4 | 5 => {
            let payload = fb::Map::create(
                &mut builder,
                &fb::MapArgs {
                    key_type: (case == 5).then_some(invalid),
                    ..fb::MapArgs::default()
                },
            );
            root(&mut builder, fb::Type::Map, payload.as_union_value())
        }
        6 => extension(&mut builder, Some(invalid), None, None),
        7 => extension(&mut builder, None, Some("test.dtype_reader_foreign"), None),
        8 => extension(
            &mut builder,
            Some(invalid),
            Some("test.dtype_reader_foreign"),
            None,
        ),
        9 => extension(
            &mut builder,
            Some(good),
            Some("test.dtype_reader_foreign"),
            None,
        ),
        10 => extension(
            &mut builder,
            Some(good),
            Some("test.dtype_reader_foreign"),
            Some(&[]),
        ),
        11 => {
            let payload = fb::Struct_::create(&mut builder, &fb::Struct_Args::default());
            root(&mut builder, fb::Type::Struct_, payload.as_union_value())
        }
        12 => {
            let payload = fb::Union::create(&mut builder, &fb::UnionArgs::default());
            root(&mut builder, fb::Type::Union, payload.as_union_value())
        }
        13 => {
            let nullable = DType::Bool(Nullability::Nullable)
                .write_flatbuffer(&mut builder)
                .unwrap();
            let payload = fb::Map::create(
                &mut builder,
                &fb::MapArgs {
                    key_type: Some(nullable),
                    value_type: Some(invalid),
                    ..fb::MapArgs::default()
                },
            );
            root(&mut builder, fb::Type::Map, payload.as_union_value())
        }
        14..=17 => {
            let first = builder.create_string("name");
            let second = builder.create_string(if case == 14 { "name" } else { "other" });
            let names = builder.create_vector(&[first, second]);
            let dtypes = if case == 15 {
                builder.create_vector(&[good])
            } else {
                builder.create_vector(&[good, good])
            };
            if case == 15 {
                let payload = fb::Struct_::create(
                    &mut builder,
                    &fb::Struct_Args {
                        names: Some(names),
                        dtypes: Some(dtypes),
                        nullable: false,
                    },
                );
                root(&mut builder, fb::Type::Struct_, payload.as_union_value())
            } else {
                let type_ids = if case == 16 {
                    builder.create_vector(&[0_i8])
                } else {
                    builder.create_vector(&[0_i8, if case == 17 { 0 } else { 1 }])
                };
                let payload = fb::Union::create(
                    &mut builder,
                    &fb::UnionArgs {
                        names: Some(names),
                        dtypes: Some(dtypes),
                        type_ids: Some(type_ids),
                        nullable: false,
                    },
                );
                root(&mut builder, fb::Type::Union, payload.as_union_value())
            }
        }
        _ => unreachable!(),
    };
    if wrap_storage {
        for _ in 0..3 {
            let payload = fb::Map::create(
                &mut builder,
                &fb::MapArgs {
                    key_type: Some(offset),
                    value_type: Some(good),
                    ..fb::MapArgs::default()
                },
            );
            offset = root(&mut builder, fb::Type::Map, payload.as_union_value());
            offset = extension(
                &mut builder,
                Some(offset),
                Some("test.dtype_reader_foreign"),
                Some(&[]),
            );
        }
    }
    let view = finish_view(builder, offset, session);
    flatbuffers::root::<fb::DType>(view.buffer()).unwrap();
    view
}

#[test]
fn logical_reader_preserves_errors_and_every_pending_storage_context() {
    let session = VortexSession::empty();
    for case in 0..18 {
        for wrapped in [false, true] {
            let view = bad_view(case, wrapped, &session);
            let expected = reference::read_reference(view.clone())
                .unwrap_err()
                .to_string();
            let actual = DType::try_from(view).unwrap_err().to_string();
            // Backtraces name the independent implementations; error messages
            // and all contexts must otherwise be identical.
            let expected = expected.split("\nBacktrace:").next().unwrap();
            let actual = actual.split("\nBacktrace:").next().unwrap();
            assert_eq!(actual, expected, "case {case}, wrapped={wrapped}");
            if wrapped {
                assert!(
                    actual
                        .matches("failed to create DType from fbs message")
                        .count()
                        >= 3
                );
            }
        }
    }
}

thread_local! { static EVENTS: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) }; }

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Metadata(u8);
impl fmt::Display for Metadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Recording;
impl ExtVTable for Recording {
    type Metadata = Metadata;
    type NativeValue<'a> = &'a ScalarValue;
    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("test.dtype_reader_order");
        *ID
    }
    fn serialize_metadata(&self, metadata: &Metadata) -> VortexResult<Vec<u8>> {
        Ok(vec![metadata.0])
    }
    fn deserialize_metadata(&self, metadata: &[u8]) -> VortexResult<Metadata> {
        let id = metadata[0];
        EVENTS.with_borrow_mut(|events| events.push(id));
        if id == 255 {
            return Err(vortex_err!("test metadata refusal"));
        }
        Ok(Metadata(id))
    }
    fn validate_dtype(_: &ExtDType<Self>) -> VortexResult<()> {
        Ok(())
    }
    fn unpack_native<'a>(
        _: &'a ExtDType<Self>,
        storage: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        Ok(storage)
    }
}

fn recorded_view(fail: bool, lazy: bool, session: &VortexSession) -> ViewedDType {
    let mut builder = FlatBufferBuilder::new();
    let good = DType::Bool(Nullability::NonNullable)
        .write_flatbuffer(&mut builder)
        .unwrap();
    let inner = extension(
        &mut builder,
        Some(good),
        Some("test.dtype_reader_order"),
        Some(&[if fail { 255 } else { 1 }]),
    );
    let middle = extension(
        &mut builder,
        Some(inner),
        Some("test.dtype_reader_order"),
        Some(&[2]),
    );
    let mut offset = extension(
        &mut builder,
        Some(middle),
        Some("test.dtype_reader_order"),
        Some(&[3]),
    );
    if lazy {
        let value = extension(
            &mut builder,
            Some(good),
            Some("test.dtype_reader_order"),
            Some(&[4]),
        );
        let payload = fb::Map::create(
            &mut builder,
            &fb::MapArgs {
                key_type: Some(offset),
                value_type: Some(value),
                ..fb::MapArgs::default()
            },
        );
        offset = root(&mut builder, fb::Type::Map, payload.as_union_value());
    }
    let view = finish_view(builder, offset, session);
    flatbuffers::root::<fb::DType>(view.buffer()).unwrap();
    view
}

#[test]
fn logical_reader_preserves_metadata_order_and_map_key_cache() {
    let session = VortexSession::empty();
    session.dtypes().register(Recording);
    for reference in [false, true] {
        for fail in [false, true] {
            EVENTS.with_borrow_mut(Vec::clear);
            let view = recorded_view(fail, true, &session);
            let result = if reference {
                reference::read_reference(view)
            } else {
                DType::try_from(view)
            };
            if fail {
                let error = result.unwrap_err().to_string();
                assert_eq!(
                    error
                        .matches("failed to create DType from fbs message")
                        .count(),
                    2
                );
                assert_eq!(EVENTS.with_borrow(Clone::clone), [255]);
            } else {
                assert_eq!(EVENTS.with_borrow(Clone::clone), [1, 2, 3]);
                let dtype = result.unwrap();
                let DType::Map(map, _) = &dtype else {
                    panic!("expected map")
                };
                map.try_key_dtype().unwrap();
                map.try_key_dtype().unwrap();
                assert_eq!(EVENTS.with_borrow(Clone::clone), [1, 2, 3]);
                map.try_value_dtype().unwrap();
                map.try_value_dtype().unwrap();
                assert_eq!(EVENTS.with_borrow(Clone::clone), [1, 2, 3, 4]);
            }
        }
    }
}

#[test]
fn logical_reader_keeps_container_children_lazy_and_failed_caches_retryable() {
    let session = VortexSession::empty();
    session.dtypes().register(Recording);
    for use_reference in [false, true] {
        for union in [false, true] {
            EVENTS.with_borrow_mut(Vec::clear);
            let mut builder = FlatBufferBuilder::new();
            let good = DType::Bool(Nullability::NonNullable)
                .write_flatbuffer(&mut builder)
                .unwrap();
            let first = extension(
                &mut builder,
                Some(good),
                Some("test.dtype_reader_order"),
                Some(&[1]),
            );
            let second = extension(
                &mut builder,
                Some(good),
                Some("test.dtype_reader_order"),
                Some(&[255]),
            );
            let dtypes = builder.create_vector(&[first, second]);
            let first_name = builder.create_string("first");
            let second_name = builder.create_string("second");
            let names = builder.create_vector(&[first_name, second_name]);
            let offset = if union {
                let type_ids = builder.create_vector(&[0_i8, 1]);
                let payload = fb::Union::create(
                    &mut builder,
                    &fb::UnionArgs {
                        names: Some(names),
                        dtypes: Some(dtypes),
                        type_ids: Some(type_ids),
                        nullable: false,
                    },
                );
                root(&mut builder, fb::Type::Union, payload.as_union_value())
            } else {
                let payload = fb::Struct_::create(
                    &mut builder,
                    &fb::Struct_Args {
                        names: Some(names),
                        dtypes: Some(dtypes),
                        nullable: false,
                    },
                );
                root(&mut builder, fb::Type::Struct_, payload.as_union_value())
            };
            let view = finish_view(builder, offset, &session);
            flatbuffers::root::<fb::DType>(view.buffer()).unwrap();
            let dtype = if use_reference {
                reference::read_reference(view)
            } else {
                DType::try_from(view)
            }
            .unwrap();
            assert!(EVENTS.with_borrow(|events| events.is_empty()));
            let mut children = dtype.try_child_dtypes();
            assert!(children.next().unwrap().is_ok());
            assert_eq!(EVENTS.with_borrow(Clone::clone), [1]);
            assert!(children.next().unwrap().is_err());
            assert_eq!(EVENTS.with_borrow(Clone::clone), [1, 255]);
            assert!(children.next().is_none());
            let mut retry = dtype.try_child_dtypes();
            assert!(retry.next().unwrap().is_ok());
            assert!(retry.next().unwrap().is_err());
            assert_eq!(EVENTS.with_borrow(Clone::clone), [1, 255, 255]);
        }
    }
}

#[test]
fn deep_logical_reader_on_small_stack() {
    const SELECTOR: &str = "VORTEX_TEST_DEEP_LOGICAL_READER";
    const COMPLETED: &str = "deep logical reader completed";
    if let Ok(kind) = env::var(SELECTOR) {
        let marker = format!("{COMPLETED} {kind}");
        let bytes = thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                let session = VortexSession::empty();
                session.allow_unknown();
                if kind == "invalid" {
                    let mut builder = FlatBufferBuilder::new();
                    let payload = fb::Primitive::create(
                        &mut builder,
                        &fb::PrimitiveArgs {
                            ptype: fb::PType(255),
                            nullable: false,
                        },
                    );
                    let mut offset =
                        root(&mut builder, fb::Type::Primitive, payload.as_union_value());
                    for index in 0..8192 {
                        offset = if index % 32 == 0 {
                            extension(
                                &mut builder,
                                Some(offset),
                                Some("test.dtype_reader_foreign"),
                                Some(&[]),
                            )
                        } else {
                            let payload = fb::List::create(
                                &mut builder,
                                &fb::ListArgs {
                                    element_type: Some(offset),
                                    nullable: false,
                                },
                            );
                            root(&mut builder, fb::Type::List, payload.as_union_value())
                        };
                    }
                    let view = finish_view(builder, offset, &session);
                    let bytes = view.buffer().clone();
                    let error = DType::try_from(view).unwrap_err().to_string();
                    assert_eq!(
                        error
                            .matches("failed to create DType from fbs message")
                            .count(),
                        256
                    );
                    assert!(
                        error.contains("flatbuffer Primitive ptype must be a known PType, got 255")
                    );
                    return bytes;
                }
                let dtype = chain(&kind, 8192, DType::Bool(Nullability::NonNullable));
                let view = view(&dtype, &session);
                let bytes = view.buffer().clone();
                let actual = DType::try_from(view).unwrap();
                assert!(dtype == actual);
                drop(dtype);
                drop(actual);
                bytes
            })
            .unwrap()
            .join()
            .unwrap();
        // This is a logical-reader test only. The public physical verifier is
        // unchanged, and must still reject the deep source.
        assert!(matches!(
            flatbuffers::root::<fb::DType>(&bytes),
            Err(InvalidFlatbuffer::DepthLimitReached)
        ));
        println!("\n{marker}");
        return;
    }
    for kind in KINDS.into_iter().chain(["mixed", "invalid"]) {
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "dtype::serde::flatbuffers::read::tests::deep_logical_reader_on_small_stack",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(SELECTOR, kind)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "reader {kind}: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line == format!("{COMPLETED} {kind}"))
        );
    }
}
