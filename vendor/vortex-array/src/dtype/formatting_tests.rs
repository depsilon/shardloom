// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::env;
use std::fmt;
use std::fmt::Write;
use std::hash::{Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::thread;

use flatbuffers::FlatBufferBuilder;
use parking_lot::Mutex;
use vortex_error::VortexResult;
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use super::*;
use crate::dtype::extension::{ExtDType, ExtId, ExtVTable, ForeignExtDType};
use crate::dtype::session::DTypeSessionExt;
use crate::flatbuffers::{FlatBuffer, WriteFlatBufferExt};
use crate::scalar::ScalarValue;

#[path = "formatting_reference.rs"]
mod reference;

const KINDS: [&str; 7] = [
    "list",
    "fixed_list",
    "struct",
    "map_key",
    "map_value",
    "union",
    "extension",
];

fn wrap(kind: &str, child: DType) -> DType {
    let nn = Nullability::NonNullable;
    match kind {
        "list" => DType::List(Arc::new(child), nn),
        "fixed_list" => DType::FixedSizeList(Arc::new(child), 7, nn),
        "struct" => DType::Struct(StructFields::from_iter([("child", child)]), nn),
        "map_key" => DType::map(child, DType::Bool(nn), true, nn).unwrap(),
        "map_value" => DType::map(DType::Bool(nn), child, false, nn).unwrap(),
        "union" => DType::Union(
            UnionVariants::try_new(["child"].into(), vec![child], vec![255]).unwrap(),
            nn,
        ),
        "extension" => {
            static ID: CachedId = CachedId::new("test.dtype_format");
            DType::Extension(ForeignExtDType::from_parts(*ID, vec![8, 0, 255], child).unwrap())
        }
        _ => panic!("unknown formatting shape"),
    }
}

fn chain(kind: &str, depth: usize) -> DType {
    (0..depth).fold(
        DType::Primitive(PType::I64, Nullability::NonNullable),
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
    )
}

fn fixtures() -> Vec<DType> {
    let mut fixtures = vec![
        DType::Null,
        DType::Struct(StructFields::empty(), Nullability::NonNullable),
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
            PType::I8,
            PType::I16,
            PType::I32,
            PType::I64,
            PType::U8,
            PType::U16,
            PType::U32,
            PType::U64,
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
                ("first", chain("mixed", 4)),
                ("naïve\"\n🙂", chain("extension", 2)),
                ("", DType::Null),
            ]),
            nullable,
        ));
        for tags in [vec![0, 1], vec![255, 7]] {
            fixtures.push(DType::Union(
                UnionVariants::try_new(
                    ["x", "λ"].into(),
                    vec![chain("mixed", 3), DType::Utf8(nullable)],
                    tags,
                )
                .unwrap(),
                nullable,
            ));
        }
    }
    fixtures
}

macro_rules! compare_formats {
    ($actual:expr, $expected:expr) => {{
        let actual = $actual;
        let expected = $expected;
        assert_eq!(format!("{actual}"), format!("{expected}"));
        assert_eq!(format!("{actual:#}"), format!("{expected:#}"));
        assert_eq!(format!("{actual:>40.7}"), format!("{expected:>40.7}"));
        assert_eq!(format!("{actual:*^40}"), format!("{expected:*^40}"));
    }};
}

fn compare(dtype: &DType) {
    compare_formats!(dtype, reference::DTypeDisplay(dtype));
    match dtype {
        DType::Struct(fields, _) => compare_formats!(fields, reference::StructDisplay(fields)),
        DType::Map(map, _) => compare_formats!(map, reference::MapDisplay(map)),
        DType::Union(variants, _) => compare_formats!(variants, reference::UnionDisplay(variants)),
        DType::Extension(extension) => {
            compare_formats!(extension, reference::ExtensionDisplay(extension))
        }
        _ => {}
    }
}

#[test]
fn display_preserves_recursive_reference_bytes_for_eager_lazy_and_wide_schemas() {
    static ID: CachedId = CachedId::new("test.dtype_format");
    let session = VortexSession::empty();
    session.dtypes().register(ForeignExtDType::new(*ID));
    for dtype in fixtures() {
        compare(&dtype);
        let lazy =
            DType::from_flatbuffer(dtype.write_flatbuffer_bytes().unwrap(), &session).unwrap();
        compare(&lazy);
    }
    let wide = DType::Struct(
        StructFields::from_iter(
            (0..2048).map(|index| (format!("field{index}"), chain("mixed", 3))),
        ),
        Nullability::Nullable,
    );
    compare(&wide);
}

struct RejectingOutput {
    remaining: usize,
    text: String,
}

impl fmt::Write for RejectingOutput {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if value.len() > self.remaining {
            return Err(fmt::Error);
        }
        self.remaining -= value.len();
        self.text.push_str(value);
        Ok(())
    }
}

#[test]
fn display_preserves_output_errors_and_partial_text() {
    for dtype in fixtures() {
        for limit in 0..=dtype.to_string().len() {
            let mut actual = RejectingOutput {
                remaining: limit,
                text: String::new(),
            };
            let mut expected = RejectingOutput {
                remaining: limit,
                text: String::new(),
            };
            assert_eq!(
                write!(&mut actual, "{dtype}"),
                write!(&mut expected, "{}", reference::DTypeDisplay(&dtype))
            );
            assert_eq!(actual.text, expected.text);
        }
    }
}

#[derive(Clone)]
struct Metadata {
    id: u8,
    events: Arc<Mutex<Vec<String>>>,
}
impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Metadata {}
impl Hash for Metadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
impl fmt::Debug for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.events.lock().push(format!("debug{}", self.id));
        self.id.fmt(f)
    }
}
impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.events.lock().push(format!("display{}", self.id));
        self.id.fmt(f)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
struct Probe;
impl ExtVTable for Probe {
    type Metadata = Metadata;
    type NativeValue<'a> = &'a ScalarValue;
    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("test.format_probe");
        *ID
    }
    fn serialize_metadata(&self, _: &Metadata) -> VortexResult<Vec<u8>> {
        panic!("formatting must not serialize metadata")
    }
    fn deserialize_metadata(&self, _: &[u8]) -> VortexResult<Metadata> {
        panic!("no serialized probe metadata")
    }
    fn validate_dtype(_: &ExtDType<Self>) -> VortexResult<()> {
        Ok(())
    }
    fn unpack_native<'a>(
        _: &'a ExtDType<Self>,
        value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        Ok(value)
    }
}
fn probe(id: u8, child: DType, events: &Arc<Mutex<Vec<String>>>) -> DType {
    DType::Extension(
        ExtDType::<Probe>::try_new(
            Metadata {
                id,
                events: Arc::clone(events),
            },
            child,
        )
        .unwrap()
        .erased(),
    )
}

fn malformed_lazy_field(session: &VortexSession) -> FieldDType {
    use crate::dtype::flatbuffers as fb;
    let mut builder = FlatBufferBuilder::new();
    let list = fb::List::create(&mut builder, &fb::ListArgs::default());
    let root = fb::DType::create(
        &mut builder,
        &fb::DTypeArgs {
            type_type: fb::Type::List,
            type_: Some(list.as_union_value()),
        },
    );
    let name = builder.create_string("bad");
    let names = builder.create_vector(&[name]);
    let dtypes = builder.create_vector(&[root]);
    let fields = fb::Struct_::create(
        &mut builder,
        &fb::Struct_Args {
            names: Some(names),
            dtypes: Some(dtypes),
            nullable: false,
        },
    );
    let root = fb::DType::create(
        &mut builder,
        &fb::DTypeArgs {
            type_type: fb::Type::Struct_,
            type_: Some(fields.as_union_value()),
        },
    );
    builder.finish_minimal(root);
    let dtype = DType::from_flatbuffer(
        FlatBuffer::align_from(builder.finished_data().to_vec()),
        session,
    )
    .unwrap();
    let DType::Struct(fields, _) = &dtype else {
        unreachable!()
    };
    fields.raw_dtypes()[0].clone()
}

#[test]
fn display_retains_join_buffer_map_preparse_and_callback_order() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let leaf = || DType::Bool(Nullability::NonNullable);
    let dtype = DType::Struct(
        StructFields::from_iter([
            ("first", probe(1, probe(2, leaf(), &events), &events)),
            ("second", probe(3, leaf(), &events)),
        ]),
        Nullability::NonNullable,
    );
    for limit in 0..=dtype.to_string().len() {
        let mut actual = RejectingOutput {
            remaining: limit,
            text: String::new(),
        };
        let mut expected = RejectingOutput {
            remaining: limit,
            text: String::new(),
        };
        events.lock().clear();
        let actual_result = write!(&mut actual, "{dtype}");
        let actual_events = events.lock().clone();
        events.lock().clear();
        let expected_result = write!(&mut expected, "{}", reference::DTypeDisplay(&dtype));
        assert_eq!(actual_result, expected_result);
        assert_eq!(actual.text, expected.text);
        assert_eq!(actual_events, *events.lock());
        assert_eq!(actual_events, ["display1", "display2", "display3"]);
    }
    let session = VortexSession::empty();
    let fields = StructFields::from_fields(
        ["first", "bad"].into(),
        vec![
            probe(1, leaf(), &events).into(),
            malformed_lazy_field(&session),
        ],
    );
    let invalid_struct = DType::Struct(fields, Nullability::NonNullable);
    events.lock().clear();
    let mut output = String::from("existing");
    assert!(catch_unwind(AssertUnwindSafe(|| write!(&mut output, "{invalid_struct}"))).is_err());
    assert_eq!(output, "existing");
    assert_eq!(*events.lock(), ["display1"]);

    let map = MapDType::try_from_fields(
        probe(2, leaf(), &events).into(),
        malformed_lazy_field(&session),
        false,
    )
    .unwrap();
    events.lock().clear();
    assert!(catch_unwind(AssertUnwindSafe(|| write!(&mut output, "{map}"))).is_err());
    assert_eq!(output, "existing");
    assert!(events.lock().is_empty());
}

#[test]
fn debug_preserves_derived_flags_and_does_not_initialize_lazy_fields() {
    #[derive(Debug)]
    #[expect(dead_code, reason = "independent original-style derived Debug fixture")]
    enum Expected {
        FixedSizeList(Arc<Expected>, u32, Nullability),
        Primitive(PType, Nullability),
    }
    let dtype = DType::FixedSizeList(
        Arc::new(DType::Primitive(PType::U64, Nullability::Nullable)),
        31,
        Nullability::NonNullable,
    );
    let expected = Expected::FixedSizeList(
        Arc::new(Expected::Primitive(PType::U64, Nullability::Nullable)),
        31,
        Nullability::NonNullable,
    );
    macro_rules! check {
        ($format:literal) => {
            assert_eq!(format!($format, dtype), format!($format, expected));
        };
    }
    check!("{:?}");
    check!("{:#?}");
    check!("{:x?}");
    check!("{:X?}");
    check!("{:#x?}");
    check!("{:#X?}");
    check!("{:>40.7?}");
    check!("{:+#032x?}");
    let field = malformed_lazy_field(&VortexSession::empty());
    let dtype = DType::Struct(
        StructFields::from_fields(["bad"].into(), vec![field]),
        Nullability::NonNullable,
    );
    let DType::Struct(fields, _) = &dtype else {
        unreachable!()
    };
    assert!(fields.raw_dtypes()[0].initialized_ref().is_none());
    assert!(format!("{dtype:#?}").contains("View("));
    assert!(fields.raw_dtypes()[0].initialized_ref().is_none());
    assert!(fields.try_field_dtypes().next().unwrap().is_err());
    assert!(fields.raw_dtypes()[0].initialized_ref().is_none());
}

#[test]
fn debug_depth_boundary_and_metadata_callbacks_are_explicit() {
    let within = chain("list", DEBUG_CHILD_DEPTH);
    assert!(!format!("{within:?}").contains(".."));
    let beyond = chain("list", DEBUG_CHILD_DEPTH + 1);
    assert_eq!(format!("{beyond:?}"), "List(..)");
    let events = Arc::new(Mutex::new(Vec::new()));
    let dtype = probe(7, DType::Bool(Nullability::NonNullable), &events);
    drop(format!("{dtype:#x?}"));
    assert_eq!(*events.lock(), ["debug7"]);
    events.lock().clear();
    let deep = probe(8, chain("list", DEBUG_CHILD_DEPTH), &events);
    assert_eq!(format!("{deep:?}"), "Extension(..)");
    assert!(events.lock().is_empty());
}

#[test]
fn deep_display_and_bounded_debug_on_small_stack() {
    const SELECTOR: &str = "VORTEX_TEST_DEEP_DTYPE_FORMAT";
    const COMPLETE: &str = "deep dtype formatting completed";
    if let Ok(kind) = env::var(SELECTOR) {
        let marker = format!("{COMPLETE} {kind}");
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                let shallow = chain(&kind, DEBUG_CHILD_DEPTH);
                assert!(!format!("{shallow:?}").contains(".."));
                assert!(!format!("{shallow:#x?}").contains(".."));
                drop(shallow);
                let depth = 8192;
                let dtype = chain(&kind, depth);
                let pieces = KINDS.map(|shape| {
                    let template = wrap(
                        shape,
                        DType::Primitive(PType::I64, Nullability::NonNullable),
                    );
                    let text = reference::DTypeDisplay(&template).to_string();
                    let (before, after) = text.split_once("i64").unwrap();
                    assert!(!after.contains("i64"));
                    (before.to_owned(), after.to_owned())
                });
                let indices: Vec<_> = (0..depth)
                    .map(|index| {
                        if kind == "mixed" {
                            index % KINDS.len()
                        } else {
                            KINDS.iter().position(|shape| *shape == kind).unwrap()
                        }
                    })
                    .collect();
                let mut expected = String::new();
                for index in indices.iter().rev() {
                    expected.push_str(&pieces[*index].0);
                }
                expected.push_str("i64");
                for index in indices {
                    expected.push_str(&pieces[index].1);
                }
                assert_eq!(dtype.to_string(), expected);
                let debug = format!("{dtype:?}");
                assert!(debug.len() < 40 && debug.ends_with("(..)"));
                assert!(format!("{dtype:#x?}").len() < 60);
                drop(dtype);
            })
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
                "dtype::formatting::tests::deep_display_and_bounded_debug_on_small_stack",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(SELECTOR, kind)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "deep formatting {kind}: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line == format!("{COMPLETE} {kind}"))
        );
    }
}
