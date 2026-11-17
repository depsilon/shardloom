// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::env;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::thread;

use flatbuffers::FlatBufferBuilder;
use vortex_buffer::ByteBuffer;
use vortex_session::registry::CachedId;

use crate::dtype::extension::{ExtDTypeRef, ForeignExtDType};
use crate::dtype::session::DTypeSessionExt;
use crate::dtype::test::SESSION;
use crate::dtype::{
    DType, DecimalDType, FieldDType, FieldNames, MapDType, Nullability, PType, StructFields,
    UnionVariants,
};
use crate::flatbuffers::{FlatBuffer, WriteFlatBufferExt};

// An independent, shallow owned representation retains the original derived
// ordinary Hash/Eq contract. Its field order matches the original native enum;
// child storage has no native DType Eq/Hash dependency.
#[derive(PartialEq, Eq, Hash)]
enum Reference<'a> {
    Null,
    Bool(Nullability),
    Primitive(PType, Nullability),
    Decimal(DecimalDType, Nullability),
    Utf8(Nullability),
    Binary(Nullability),
    List(Box<Self>, Nullability),
    FixedSizeList(Box<Self>, u32, Nullability),
    Map(ReferenceMap<'a>, Nullability),
    Struct(ReferenceFields<'a>, Nullability),
    Union(ReferenceUnion<'a>, Nullability),
    Variant(Nullability),
    Extension(ReferenceExtension<'a>),
}

#[derive(PartialEq, Eq, Hash)]
struct ReferenceFields<'a> {
    names: FieldNames,
    dtypes: Vec<Reference<'a>>,
}
#[derive(PartialEq, Eq, Hash)]
struct ReferenceUnion<'a> {
    names: FieldNames,
    dtypes: Vec<Reference<'a>>,
    tags: Vec<u8>,
}
#[derive(PartialEq, Eq, Hash)]
struct ReferenceMap<'a> {
    key: Box<Reference<'a>>,
    value: Box<Reference<'a>>,
    sorted: bool,
}
struct ReferenceExtension<'a> {
    native: &'a ExtDTypeRef,
    storage: Box<Reference<'a>>,
}

impl PartialEq for ReferenceExtension<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.native.metadata_eq(other.native) && self.storage == other.storage
    }
}
impl Eq for ReferenceExtension<'_> {}
impl Hash for ReferenceExtension<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.native.hash_metadata(state);
        self.storage.hash(state);
    }
}

impl<'a> Reference<'a> {
    fn from_native(dtype: &'a DType, ignore: bool) -> Self {
        let null = |n: Nullability| if ignore { Nullability::NonNullable } else { n };
        let child = |d| Box::new(Self::from_native(d, ignore));
        let fields = |f: &'a [FieldDType]| {
            f.iter()
                .map(|f| Self::from_native(f.value_ref().unwrap(), ignore))
                .collect()
        };
        match dtype {
            DType::Null => Self::Null,
            DType::Bool(n) => Self::Bool(null(*n)),
            DType::Primitive(p, n) => Self::Primitive(*p, null(*n)),
            DType::Decimal(d, n) => Self::Decimal(*d, null(*n)),
            DType::Utf8(n) => Self::Utf8(null(*n)),
            DType::Binary(n) => Self::Binary(null(*n)),
            DType::List(d, n) => Self::List(child(d), null(*n)),
            DType::FixedSizeList(d, size, n) => Self::FixedSizeList(child(d), *size, null(*n)),
            DType::Map(map, n) => {
                let (key, value) = map.raw_fields();
                Self::Map(
                    ReferenceMap {
                        key: child(key.value_ref().unwrap()),
                        value: child(value.value_ref().unwrap()),
                        sorted: map.keys_sorted(),
                    },
                    null(*n),
                )
            }
            DType::Struct(f, n) => Self::Struct(
                ReferenceFields {
                    names: f.names().clone(),
                    dtypes: fields(f.raw_dtypes()),
                },
                null(*n),
            ),
            DType::Union(v, n) => Self::Union(
                ReferenceUnion {
                    names: v.names().clone(),
                    dtypes: fields(v.raw_dtypes()),
                    tags: v.type_ids().to_vec(),
                },
                null(*n),
            ),
            DType::Variant(n) => Self::Variant(null(*n)),
            DType::Extension(e) => Self::Extension(ReferenceExtension {
                native: e,
                storage: child(e.storage_dtype()),
            }),
        }
    }

    fn hash_ignoring_nullability<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null | Self::Bool(_) | Self::Utf8(_) | Self::Binary(_) | Self::Variant(_) => {}
            Self::Primitive(p, _) => p.hash(state),
            Self::Decimal(d, _) => d.hash(state),
            Self::List(d, _) => d.hash_ignoring_nullability(state),
            Self::FixedSizeList(d, size, _) => {
                d.hash_ignoring_nullability(state);
                size.hash(state);
            }
            Self::Map(m, _) => {
                m.key.hash_ignoring_nullability(state);
                m.value.hash_ignoring_nullability(state);
                m.sorted.hash(state);
            }
            Self::Struct(f, _) => {
                f.names.hash(state);
                for d in &f.dtypes {
                    d.hash_ignoring_nullability(state);
                }
            }
            Self::Union(v, _) => {
                v.names.hash(state);
                for d in &v.dtypes {
                    d.hash_ignoring_nullability(state);
                }
                v.tags.hash(state);
            }
            Self::Extension(e) => {
                e.native.hash_metadata(state);
                e.storage.hash_ignoring_nullability(state);
            }
        }
    }
}

#[derive(Default, Debug, PartialEq, Eq)]
struct FeedTrace(Vec<Vec<u8>>);
impl Hasher for FeedTrace {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0.push(bytes.to_vec());
    }
}

const KINDS: [&str; 6] = ["list", "fixed_list", "struct", "map", "union", "extension"];
fn wrap(kind: &str, child: DType, nullable: Nullability) -> DType {
    match kind {
        "list" => DType::List(Arc::new(child), nullable),
        "fixed_list" => DType::FixedSizeList(Arc::new(child), 3, nullable),
        "struct" => DType::Struct(StructFields::from_iter([("child", child)]), nullable),
        "map" => DType::map(
            DType::Primitive(PType::I64, Nullability::NonNullable),
            child,
            true,
            nullable,
        )
        .unwrap(),
        "union" => DType::Union(
            UnionVariants::try_new(["child"].into(), vec![child], vec![7]).unwrap(),
            nullable,
        ),
        "extension" => {
            static ID: CachedId = CachedId::new("test.structural");
            DType::Extension(ForeignExtDType::from_parts(*ID, vec![2, 3], child).unwrap())
        }
        _ => panic!("unknown shape"),
    }
}

fn chain(kind: &str, depth: usize, p: PType, n: Nullability) -> DType {
    (0..depth).fold(DType::Primitive(p, n), |child, index| {
        wrap(
            if kind == "mixed" {
                KINDS[index % KINDS.len()]
            } else {
                kind
            },
            child,
            n,
        )
    })
}

#[test]
fn native_structural_matches_recursive_reference_and_hash_feed() {
    static ID: CachedId = CachedId::new("test.structural");
    let session = vortex_session::VortexSession::empty();
    session.dtypes().register(ForeignExtDType::new(*ID));
    let mut fixtures = vec![
        DType::Null,
        DType::Struct(StructFields::empty(), Nullability::Nullable),
    ];
    for n in [Nullability::Nullable, Nullability::NonNullable] {
        fixtures.extend([
            DType::Bool(n),
            DType::Utf8(n),
            DType::Binary(n),
            DType::Variant(n),
            DType::Decimal(DecimalDType::try_new(38, 7).unwrap(), n),
        ]);
        for p in [
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
            fixtures.push(DType::Primitive(p, n));
        }
        for kind in KINDS.into_iter().chain(["mixed"]) {
            fixtures.push(chain(kind, 3, PType::I64, n));
        }
        fixtures.push(DType::Struct(
            StructFields::from_iter([
                ("duplicate", DType::Bool(n)),
                ("duplicate", chain("mixed", 3, PType::U64, n)),
            ]),
            n,
        ));
        fixtures.push(DType::Union(
            UnionVariants::try_new(
                ["first", "second"].into(),
                vec![DType::Bool(n), chain("mixed", 3, PType::U64, n)],
                vec![255, 4],
            )
            .unwrap(),
            n,
        ));
    }
    let viewed: Vec<_> = fixtures
        .iter()
        .map(|d| {
            DType::from_flatbuffer(
                FlatBuffer::from(d.write_flatbuffer_bytes().unwrap()),
                &session,
            )
            .unwrap()
        })
        .collect();
    fixtures.extend(viewed);
    for (index, a) in fixtures.iter().enumerate() {
        for ignore in [false, true] {
            let reference = Reference::from_native(a, ignore);
            let mut actual = FeedTrace::default();
            let mut expected = FeedTrace::default();
            if ignore {
                a.hash_ignore_nullability(&mut actual);
                reference.hash_ignoring_nullability(&mut expected);
            } else {
                a.hash(&mut actual);
                reference.hash(&mut expected);
            }
            assert_eq!(actual, expected, "fixture {index}, ignore={ignore}");
            for b in &fixtures {
                assert_eq!(
                    if ignore {
                        a.eq_ignore_nullability(b)
                    } else {
                        a == b
                    },
                    reference == Reference::from_native(b, ignore),
                    "fixture {index}, ignore={ignore}"
                );
            }
        }
    }
}

fn hash(dtype: &DType, ignore: bool) -> u64 {
    let mut state = DefaultHasher::new();
    if ignore {
        dtype.hash_ignore_nullability(&mut state);
    } else {
        dtype.hash(&mut state);
    }
    state.finish()
}

fn run_deep(kind: &str) {
    let a = chain(kind, 8192, PType::I64, Nullability::NonNullable);
    let b = chain(kind, 8192, PType::I64, Nullability::NonNullable);
    let nullable = chain(kind, 8192, PType::I64, Nullability::Nullable);
    let changed = chain(kind, 8192, PType::U64, Nullability::NonNullable);
    assert!(a == b);
    assert!(a != changed);
    assert!(a != nullable);
    assert!(a.eq_ignore_nullability(&nullable));
    assert!(!a.eq_ignore_nullability(&changed));
    assert_eq!(hash(&a, false), hash(&b, false));
    assert_eq!(hash(&a, true), hash(&nullable, true));
    // Root clones use the established composite Arc shortcuts where available.
    assert!(a == a.clone());
    let converted = a.as_nullable();
    assert!(converted.is_nullable());
    assert!(converted.as_nonnullable() == a);
    assert!(converted.eq_ignore_nullability(&a));
}

#[test]
fn deep_structural_operations_on_small_stack() {
    const SELECTOR: &str = "SHARDLOOM_STRUCTURAL_CHILD";
    const COMPLETED: &str = "SHARDLOOM_STRUCTURAL_COMPLETED";
    if let Ok(kind) = env::var(SELECTOR) {
        let marker = format!("{COMPLETED} {kind}");
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || run_deep(&kind))
            .unwrap()
            .join()
            .unwrap();
        println!("\n{marker}");
        return;
    }
    for kind in KINDS.into_iter().chain(["mixed"]) {
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "dtype::structural::tests::deep_structural_operations_on_small_stack",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(SELECTOR, kind)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "shape {kind}: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|l| l == format!("{COMPLETED} {kind}")),
            "missing completion for {kind}"
        );
    }
}

fn invalid_lazy_field() -> FieldDType {
    use crate::dtype::flatbuffers as fb;
    let mut fbb = FlatBufferBuilder::new();
    let name = fbb.create_string("bad");
    let names = fbb.create_vector(&[name]);
    // Physically valid union NONE, logically invalid as a native dtype. The
    // parent struct must remain lazy and parse successfully.
    let bad = fb::DType::create(&mut fbb, &fb::DTypeArgs::default());
    let dtypes = fbb.create_vector(&[bad]);
    let structure = fb::Struct_::create(
        &mut fbb,
        &fb::Struct_Args {
            names: Some(names),
            dtypes: Some(dtypes),
            nullable: false,
        },
    );
    let root = fb::DType::create(
        &mut fbb,
        &fb::DTypeArgs {
            type_type: fb::Type::Struct_,
            type_: Some(structure.as_union_value()),
        },
    );
    fbb.finish_minimal(root);
    let (bytes, start) = fbb.collapse();
    let end = bytes.len();
    let root = DType::from_flatbuffer(
        FlatBuffer::align_from(ByteBuffer::from(bytes).slice(start..end)),
        &SESSION,
    )
    .unwrap();
    root.as_struct_fields_opt().unwrap().raw_dtypes()[0].clone()
}

#[test]
fn lazy_structural_comparison_preserves_short_circuits_and_trailing_checks() {
    let make_struct = |first, name: &str| {
        DType::Struct(
            StructFields::from_fields(
                ["first", name].into(),
                vec![
                    FieldDType::from(DType::Primitive(first, Nullability::NonNullable)),
                    invalid_lazy_field(),
                ],
            ),
            Nullability::NonNullable,
        )
    };
    let a = make_struct(PType::I32, "bad");
    let b = make_struct(PType::I64, "bad");
    let names_differ = make_struct(PType::I32, "different");
    for ignore in [false, true] {
        assert!(!super::equal(&a, &b, ignore));
        assert!(!super::equal(&a, &names_differ, ignore));
        assert!(super::equal(&a, &a.clone(), ignore));
        let same = make_struct(PType::I32, "bad");
        assert!(catch_unwind(AssertUnwindSafe(|| super::equal(&a, &same, ignore))).is_err());
        let make_map = |key, sorted| {
            DType::Map(
                MapDType::try_from_fields(
                    DType::Primitive(key, Nullability::NonNullable).into(),
                    invalid_lazy_field(),
                    sorted,
                )
                .unwrap(),
                Nullability::NonNullable,
            )
        };
        let map = make_map(PType::I32, true);
        assert!(!super::equal(&map, &make_map(PType::I64, false), ignore));
        assert!(
            catch_unwind(AssertUnwindSafe(|| super::equal(
                &map,
                &make_map(PType::I32, false),
                ignore
            )))
            .is_err()
        );
        let make_union = |tag| {
            DType::Union(
                UnionVariants::try_from_fields(
                    ["bad"].into(),
                    vec![invalid_lazy_field()],
                    vec![tag],
                )
                .unwrap(),
                Nullability::NonNullable,
            )
        };
        assert!(
            catch_unwind(AssertUnwindSafe(|| super::equal(
                &make_union(1),
                &make_union(2),
                ignore
            )))
            .is_err()
        );
        assert!(catch_unwind(AssertUnwindSafe(|| hash(&a, ignore))).is_err());
    }
}

#[test]
fn structural_shared_subtrees_and_wide_siblings() {
    let mut a = DType::Bool(Nullability::NonNullable);
    let mut b = a.clone();
    for _ in 0..12 {
        a = DType::Struct(
            StructFields::from_iter([("left", a.clone()), ("right", a)]),
            Nullability::NonNullable,
        );
        b = DType::Struct(
            StructFields::from_iter([("left", b.clone()), ("right", b)]),
            Nullability::NonNullable,
        );
    }
    assert!(a == b);
    assert_eq!(hash(&a, false), hash(&b, false));
    let wide = |last| {
        DType::Struct(
            StructFields::from_iter((0..4096).map(|index| {
                (
                    format!("field{index}"),
                    if index == 4095 {
                        DType::Primitive(last, Nullability::Nullable)
                    } else {
                        DType::Bool(Nullability::NonNullable)
                    },
                )
            })),
            Nullability::NonNullable,
        )
    };
    assert!(wide(PType::I64) == wide(PType::I64));
    assert!(wide(PType::I64) != wide(PType::U64));
}

#[path = "structural_extension_tests.rs"]
mod extension_tests;
