// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::fmt::{self, Write};
use std::hash::{Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::{env, thread};

use flatbuffers::FlatBufferBuilder;
use parking_lot::Mutex;
use vortex_error::{VortexResult, vortex_bail};
use vortex_session::{VortexSession, registry::CachedId};

use crate::dtype::extension::{ExtDType, ExtId, ExtVTable, ForeignExtDType};
use crate::dtype::half::f16;
use crate::dtype::{
    DType, DecimalDType, MapDType, Nullability, PType, StructFields, UnionVariants, i256,
};
use crate::extension::datetime::{Date, TimeUnit};
use crate::flatbuffers::FlatBuffer;
use crate::scalar::{DecimalValue, ListScalar, PValue, Scalar, ScalarValue, UnionValue};

#[path = "formatting_reference.rs"]
mod reference;
use Nullability::{NonNullable, Nullable};

fn wrap(kind: &str, child: Scalar) -> Scalar {
    if kind == "variant" {
        return Scalar {
            value: Some(ScalarValue::Variant(Box::new(child))),
            dtype: DType::Variant(NonNullable),
        };
    }
    let (dtype, value) = child.into_parts();
    let (dtype, value) = match kind {
        "list" => (
            DType::List(Arc::new(dtype), NonNullable),
            Some(ScalarValue::Tuple(vec![value])),
        ),
        "fixed" => (
            DType::FixedSizeList(Arc::new(dtype), 1, NonNullable),
            Some(ScalarValue::Tuple(vec![value])),
        ),
        "struct" => (
            DType::Struct(
                StructFields::new(["child"].into(), vec![dtype]),
                NonNullable,
            ),
            Some(ScalarValue::Tuple(vec![value])),
        ),
        "map-key" => (
            DType::Map(
                MapDType::try_new(dtype, DType::Bool(Nullable), false).unwrap(),
                NonNullable,
            ),
            Some(ScalarValue::Tuple(vec![Some(ScalarValue::Tuple(vec![
                value, None,
            ]))])),
        ),
        "map-value" => (
            DType::Map(
                MapDType::try_new(DType::Bool(NonNullable), dtype, false).unwrap(),
                NonNullable,
            ),
            Some(ScalarValue::Tuple(vec![Some(ScalarValue::Tuple(vec![
                Some(ScalarValue::Bool(true)),
                value,
            ]))])),
        ),
        "union" => (
            DType::Union(
                UnionVariants::try_new(["selected"].into(), vec![dtype], vec![7]).unwrap(),
                NonNullable,
            ),
            Some(ScalarValue::Union(UnionValue::new(7, value))),
        ),
        "foreign" => {
            static ID: CachedId = CachedId::new("test.scalar_formatting_foreign");
            (
                DType::Extension(ForeignExtDType::from_parts(*ID, vec![2, 3], dtype).unwrap()),
                value,
            )
        }
        _ => panic!("unknown scalar formatting shape"),
    };
    Scalar { value, dtype }
}

fn fixtures() -> Vec<Scalar> {
    let mut leaves = vec![
        Scalar::from(false),
        Scalar::from(true),
        Scalar::from(""),
        Scalar::from("λ🙂\n\"\\01234567890"),
        Scalar::binary(vec![0_u8, 255], NonNullable),
        Scalar::binary((0..32).collect::<Vec<u8>>(), NonNullable),
        Scalar::try_new(
            DType::Extension(Date::new(TimeUnit::Days, NonNullable).erased()),
            Some(ScalarValue::Primitive(PValue::I32(0))),
        )
        .unwrap(),
    ];
    for primitive in [
        PValue::I8(-1),
        PValue::I16(-2),
        PValue::I32(-3),
        PValue::I64(-4),
        PValue::U8(1),
        PValue::U16(2),
        PValue::U32(3),
        PValue::U64(4),
        PValue::F16(f16::from_bits(0x7e01)),
        PValue::F32(f32::NAN),
        PValue::F64(f64::INFINITY),
    ] {
        leaves.push(
            Scalar::try_new(
                DType::Primitive(primitive.ptype(), NonNullable),
                Some(ScalarValue::Primitive(primitive)),
            )
            .unwrap(),
        );
    }
    for decimal in [
        DecimalValue::I8(-9),
        DecimalValue::I16(99),
        DecimalValue::I32(-1000),
        DecimalValue::I64(1000),
        DecimalValue::I128(0),
        DecimalValue::I256(i256::from(1_i64)),
    ] {
        leaves.push(
            Scalar::try_new(
                DType::Decimal(DecimalDType::new(8, 2), NonNullable),
                Some(ScalarValue::Decimal(decimal)),
            )
            .unwrap(),
        );
    }
    let mut values = vec![Scalar::null(DType::Null)];
    for leaf in leaves {
        values.push(leaf.clone());
        for nullable in [false, true] {
            let child = if nullable {
                Scalar::null(leaf.dtype().as_nullable())
            } else {
                leaf.clone()
            };
            for kind in KINDS.into_iter().chain(["foreign"]) {
                if matches!(kind, "map-key" | "variant") && nullable {
                    continue;
                }
                let nested = wrap(kind, child.clone());
                values.push(Scalar::null(nested.dtype().as_nullable()));
                values.push(nested.clone());
                values.push(wrap("struct", wrap("list", nested)));
            }
        }
    }
    values.push(wrap("variant", Scalar::null(DType::Null)));
    values.push(
        Scalar::try_new(
            DType::List(Arc::new(DType::Bool(Nullable)), NonNullable),
            Some(ScalarValue::Tuple(vec![])),
        )
        .unwrap(),
    );
    values.push(
        Scalar::try_new(
            DType::Struct(StructFields::new(Default::default(), vec![]), NonNullable),
            Some(ScalarValue::Tuple(vec![])),
        )
        .unwrap(),
    );
    values.push(
        Scalar::try_new(
            DType::Map(
                MapDType::try_new(DType::Bool(NonNullable), DType::Bool(Nullable), false).unwrap(),
                NonNullable,
            ),
            Some(ScalarValue::Tuple(vec![])),
        )
        .unwrap(),
    );
    let names: Vec<_> = (0..128).map(|i| format!("f{i}")).collect();
    values.push(
        Scalar::try_new(
            DType::Struct(
                StructFields::new(
                    names.iter().map(String::as_str).collect(),
                    vec![DType::Bool(Nullable); 128],
                ),
                NonNullable,
            ),
            Some(ScalarValue::Tuple(
                (0..128)
                    .map(|i| (i % 3 != 0).then_some(ScalarValue::Bool(i % 2 == 0)))
                    .collect(),
            )),
        )
        .unwrap(),
    );
    values
}

struct Sink {
    value: String,
    remaining: usize,
}
impl Write for Sink {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if value.len() > self.remaining {
            return Err(fmt::Error);
        }
        self.remaining -= value.len();
        self.value.push_str(value);
        Ok(())
    }
}
fn render(value: &impl fmt::Display, limit: usize) -> (Result<Result<(), ()>, ()>, String) {
    let mut sink = Sink {
        value: String::new(),
        remaining: limit,
    };
    let outcome =
        catch_unwind(AssertUnwindSafe(|| write!(sink, "{value}").map_err(|_| ()))).map_err(|_| ());
    (outcome, sink.value)
}

fn compare(scalar: &Scalar) {
    let original = reference::Typed(scalar);
    assert_eq!(format!("{scalar}"), format!("{original}"));
    assert_eq!(format!("{scalar:#}"), format!("{original:#}"));
    assert_eq!(format!("{scalar:>48.3}"), format!("{original:>48.3}"));
    assert_eq!(format!("{scalar:+032}"), format!("{original:+032}"));
    let text = scalar.to_string();
    for limit in 0..=text.len() + 1 {
        assert_eq!(
            render(scalar, limit),
            render(&original, limit),
            "limit {limit}, {scalar:?}"
        );
    }
    if let Some(value) = scalar.value() {
        let original = reference::Raw(value);
        assert_eq!(format!("{value}"), format!("{original}"));
        assert_eq!(format!("{value:#}"), format!("{original:#}"));
        assert_eq!(format!("{value:>48.3}"), format!("{original:>48.3}"));
        for limit in 0..=value.to_string().len() + 1 {
            assert_eq!(render(value, limit), render(&original, limit));
        }
    }
    let view = match scalar.dtype() {
        DType::List(..) | DType::FixedSizeList(..) => scalar.as_list().to_string(),
        DType::Struct(..) => scalar.as_struct().to_string(),
        DType::Map(..) => scalar.as_map().to_string(),
        DType::Union(..) => scalar.as_union().to_string(),
        DType::Variant(_) => scalar.as_variant().to_string(),
        _ => return,
    };
    assert_eq!(view, text);
}

#[test]
fn typed_raw_and_view_display_preserve_reference_bytes_and_failed_output() {
    for scalar in fixtures() {
        compare(&scalar);
    }
}

type Events = Arc<Mutex<Vec<(&'static str, u8)>>>;
#[derive(Clone)]
struct Metadata {
    id: u8,
    fail: u8,
    events: Events,
}
impl fmt::Debug for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}
impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}
impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        (self.id, self.fail) == (other.id, other.fail)
    }
}
impl Eq for Metadata {}
impl Hash for Metadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.id, self.fail).hash(state);
    }
}
struct Native<'a>(&'a Metadata);
impl fmt::Display for Native<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.events.lock().push(("format", self.0.id));
        if self.0.fail == 2 {
            return Err(fmt::Error);
        }
        write!(f, "probe{}", self.0.id)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
struct Probe;
impl ExtVTable for Probe {
    type Metadata = Metadata;
    type NativeValue<'a> = Native<'a>;
    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("test.scalar_formatting");
        *ID
    }
    fn serialize_metadata(&self, _: &Metadata) -> VortexResult<Vec<u8>> {
        panic!("no metadata serialization")
    }
    fn deserialize_metadata(&self, _: &[u8]) -> VortexResult<Metadata> {
        panic!("no metadata deserialization")
    }
    fn validate_dtype(_: &ExtDType<Self>) -> VortexResult<()> {
        Ok(())
    }
    fn validate_scalar_value(dtype: &ExtDType<Self>, _: &ScalarValue) -> VortexResult<()> {
        dtype
            .metadata()
            .events
            .lock()
            .push(("validate", dtype.metadata().id));
        Ok(())
    }
    fn unpack_native<'a>(
        dtype: &'a ExtDType<Self>,
        _: &'a ScalarValue,
    ) -> VortexResult<Native<'a>> {
        let metadata = dtype.metadata();
        metadata.events.lock().push(("unpack", metadata.id));
        if metadata.fail == 1 {
            vortex_bail!("probe unpack error");
        }
        Ok(Native(metadata))
    }
}
fn probe(id: u8, fail: u8, events: &Events) -> Scalar {
    Scalar {
        value: Some(ScalarValue::Bool(true)),
        dtype: DType::Extension(
            ExtDType::<Probe>::try_new(
                Metadata {
                    id,
                    fail,
                    events: Arc::clone(events),
                },
                DType::Bool(NonNullable),
            )
            .unwrap()
            .erased(),
        ),
    }
}

#[test]
fn display_keeps_validation_unpack_format_and_join_error_order() {
    let events = Arc::default();
    for fail in 0..=2 {
        for kind in KINDS.into_iter().chain(["foreign"]) {
            let first = probe(1, 0, &events);
            let second = probe(2, fail, &events);
            let (first_type, first_value) = first.into_parts();
            let (second_type, second_value) = second.into_parts();
            let child = Scalar {
                value: Some(ScalarValue::Tuple(vec![first_value, second_value])),
                dtype: DType::Struct(
                    StructFields::new(["a", "b"].into(), vec![first_type, second_type]),
                    NonNullable,
                ),
            };
            let scalar = wrap(kind, child);
            for limit in 0..=100 {
                events.lock().clear();
                let expected = render(&reference::Typed(&scalar), limit);
                let expected_events = std::mem::take(&mut *events.lock());
                let actual = render(&scalar, limit);
                assert_eq!(actual, expected, "{kind}, failure {fail}, limit {limit}");
                assert_eq!(
                    *events.lock(),
                    expected_events,
                    "{kind}, failure {fail}, limit {limit}"
                );
            }
        }
        // A native formatter error at the root propagates; inside a formatted
        // struct field/map entry it panics as an infallible String operation.
        let scalar = probe(3, fail, &events);
        events.lock().clear();
        let expected = render(&reference::Typed(&scalar), 100);
        let expected_events = std::mem::take(&mut *events.lock());
        let actual = render(&scalar, 100);
        assert_eq!(actual, expected);
        assert_eq!(*events.lock(), expected_events);
    }
}

#[test]
fn display_preserves_legacy_child_conversion_and_lazy_failure_order() {
    let child = DType::Primitive(PType::F16, NonNullable);
    let dtype = DType::List(Arc::new(child), NonNullable);
    let value = Some(ScalarValue::Tuple(vec![Some(ScalarValue::Primitive(
        PValue::U64(0x3c00),
    ))]));
    let scalar = Scalar { value, dtype };
    assert_eq!(scalar.to_string(), reference::Typed(&scalar).to_string());
    assert_eq!(
        ListScalar::try_new(scalar.dtype(), scalar.value())
            .unwrap()
            .to_string(),
        scalar.to_string()
    );

    use crate::dtype::flatbuffers as fb;
    let mut builder = FlatBufferBuilder::new();
    let first = fb::Bool::create(&mut builder, &fb::BoolArgs { nullable: false });
    let first = fb::DType::create(
        &mut builder,
        &fb::DTypeArgs {
            type_type: fb::Type::Bool,
            type_: Some(first.as_union_value()),
        },
    );
    let bad = fb::DType::create(&mut builder, &fb::DTypeArgs::default());
    let first_name = builder.create_string("first");
    let bad_name = builder.create_string("bad");
    let names = builder.create_vector(&[first_name, bad_name]);
    let dtypes = builder.create_vector(&[first, bad]);
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
        &VortexSession::empty(),
    )
    .unwrap();
    let scalar = Scalar {
        value: Some(ScalarValue::Tuple(vec![
            Some(ScalarValue::Bool(true)),
            None,
        ])),
        dtype,
    };
    for limit in 0..40 {
        assert_eq!(
            render(&scalar, limit),
            render(&reference::Typed(&scalar), limit)
        );
    }
}

const KINDS: [&str; 7] = [
    "list",
    "fixed",
    "struct",
    "map-key",
    "map-value",
    "union",
    "variant",
];
fn chain(kind: &str, depth: usize) -> Scalar {
    (0..depth).fold(Scalar::from(true), |child, index| {
        if kind == "foreign-mixed" {
            wrap("foreign", wrap("variant", wrap("struct", child)))
        } else {
            wrap(
                if kind == "mixed" {
                    KINDS[index % KINDS.len()]
                } else {
                    kind
                },
                child,
            )
        }
    })
}

#[test]
fn deep_typed_raw_and_view_display_and_cleanup_use_small_stack() {
    const SELECTOR: &str = "VORTEX_TEST_DEEP_SCALAR_DISPLAY";
    const COMPLETE: &str = "deep scalar display completed";
    if let Ok(kind) = env::var(SELECTOR) {
        let marker = format!("{COMPLETE} {kind}");
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                let scalar = chain(&kind, 8192);
                let typed = scalar.to_string();
                assert!(typed.contains("true"));
                assert!(!typed.contains(".."));
                assert!(typed.len() > 8192 && typed.len() < 1024 * 1024);
                let raw = scalar.value().unwrap().to_string();
                assert!(raw.contains("true"));
                assert!(!raw.contains(".."));
                let view = match scalar.dtype() {
                    DType::List(..) | DType::FixedSizeList(..) => scalar.as_list().to_string(),
                    DType::Struct(..) => scalar.as_struct().to_string(),
                    DType::Map(..) => scalar.as_map().to_string(),
                    DType::Union(..) => scalar.as_union().to_string(),
                    DType::Variant(_) => scalar.as_variant().to_string(),
                    DType::Extension(_) => scalar.as_extension().to_string(),
                    _ => unreachable!(),
                };
                assert_eq!(typed, view);
                let failed = render(&scalar, 5);
                assert!(failed.0.unwrap().is_err());
                drop(scalar);
            })
            .unwrap()
            .join()
            .unwrap();
        println!("\n{marker}");
        return;
    }
    for kind in KINDS.into_iter().chain(["mixed", "foreign-mixed"]) {
        let output = Command::new(env::current_exe().unwrap()).args([
            "--exact", "scalar::formatting::tests::deep_typed_raw_and_view_display_and_cleanup_use_small_stack", "--test-threads=1", "--nocapture"
        ]).env(SELECTOR, kind).output().unwrap();
        assert!(
            output.status.success(),
            "{kind}: {}\n{}\n{}",
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
