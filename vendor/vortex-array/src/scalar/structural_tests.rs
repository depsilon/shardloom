// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::env;
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use bytes::Bytes;
use parking_lot::Mutex;
use vortex_buffer::{Alignment, BufferString, ByteBuffer};
use vortex_error::VortexResult;
use vortex_session::registry::CachedId;

use super::*;
use crate::dtype::extension::{ExtDType, ExtId, ExtVTable};
use crate::dtype::half::f16;
use crate::dtype::{Nullability, PType, UnionVariants, i256};
use crate::scalar::{DecimalValue, PValue};

// Independent shallow native-value model with the original variant/field order.
// These derives must not call ScalarValue's implementations for child values.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Reference {
    Bool(bool),
    Primitive(PValue),
    Decimal(DecimalValue),
    Utf8(BufferString),
    Binary(ByteBuffer),
    Tuple(Vec<Option<Reference>>),
    Union(ReferenceUnion),
    Variant(Box<ReferenceScalar>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ReferenceUnion {
    type_id: u8,
    value: Option<Box<Reference>>,
}

#[derive(Clone, Debug, Eq)]
struct ReferenceScalar {
    value: Option<Reference>,
    dtype: DType,
}

impl PartialEq for ReferenceScalar {
    fn eq(&self, other: &Self) -> bool {
        self.dtype.eq_ignore_nullability(&other.dtype) && self.value == other.value
    }
}

impl Hash for ReferenceScalar {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dtype.hash_ignore_nullability(state);
        self.value.hash(state);
    }
}

fn reference(value: &ScalarValue) -> Reference {
    match value {
        ScalarValue::Bool(value) => Reference::Bool(*value),
        ScalarValue::Primitive(value) => Reference::Primitive(*value),
        ScalarValue::Decimal(value) => Reference::Decimal(*value),
        ScalarValue::Utf8(value) => Reference::Utf8(value.clone()),
        ScalarValue::Binary(value) => Reference::Binary(value.clone()),
        ScalarValue::Tuple(values) => Reference::Tuple(
            values
                .iter()
                .map(|value| value.as_ref().map(reference))
                .collect(),
        ),
        ScalarValue::Union(value) => Reference::Union(ReferenceUnion {
            type_id: value.type_id(),
            value: value.child_value().map(reference).map(Box::new),
        }),
        ScalarValue::Variant(scalar) => Reference::Variant(Box::new(ReferenceScalar {
            value: scalar.value.as_ref().map(reference),
            dtype: scalar.dtype.clone(),
        })),
    }
}

#[derive(Default)]
struct Feed(Vec<Vec<u8>>);
impl Hasher for Feed {
    fn finish(&self) -> u64 {
        panic!("reference comparison inspects each write")
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0.push(bytes.to_vec());
    }
}
fn feed(value: &impl Hash) -> Vec<Vec<u8>> {
    let mut state = Feed::default();
    value.hash(&mut state);
    state.0
}
fn hash(value: &impl Hash) -> u64 {
    let mut state = DefaultHasher::new();
    value.hash(&mut state);
    state.finish()
}

fn fixtures() -> Vec<ScalarValue> {
    let mut values = vec![
        ScalarValue::Bool(false),
        ScalarValue::Bool(true),
        ScalarValue::Utf8(BufferString::from("")),
        ScalarValue::Utf8(BufferString::from("λ\0naïve🙂")),
        ScalarValue::Binary(ByteBuffer::from(vec![])),
        ScalarValue::Binary(ByteBuffer::from(vec![0, 255, 7])),
        ScalarValue::Tuple(vec![]),
        ScalarValue::Tuple(vec![None]),
        ScalarValue::Union(UnionValue::new(0, None)),
        ScalarValue::Union(UnionValue::new(255, None)),
        ScalarValue::Variant(Box::new(Scalar::null(DType::Null))),
    ];
    for primitive in [
        PValue::I8(i8::MIN),
        PValue::I16(i16::MIN),
        PValue::I32(i32::MIN),
        PValue::I64(i64::MIN),
        PValue::U8(u8::MAX),
        PValue::U16(u16::MAX),
        PValue::U32(u32::MAX),
        PValue::U64(u64::MAX),
        PValue::F16(f16::from_bits(0x7e01)),
        PValue::F32(f32::from_bits(0x7fc0_0001)),
        PValue::F64(f64::from_bits(0x7ff8_0000_0000_0001)),
        PValue::F64(f64::NEG_INFINITY),
        PValue::F64(f64::INFINITY),
        PValue::F64(0.0),
        PValue::F64(-0.0),
    ] {
        values.push(ScalarValue::Primitive(primitive));
    }
    for decimal in [
        DecimalValue::I8(-9),
        DecimalValue::I16(-999),
        DecimalValue::I32(-999_999),
        DecimalValue::I64(i64::MIN),
        DecimalValue::I128(i128::MIN),
        DecimalValue::I256(i256::MIN),
    ] {
        values.push(ScalarValue::Decimal(decimal));
    }
    for nullable in [Nullability::Nullable, Nullability::NonNullable] {
        values.push(ScalarValue::Variant(Box::new(Scalar::primitive(
            7_i32, nullable,
        ))));
        values.push(ScalarValue::Variant(Box::new(Scalar::primitive(
            7_i64, nullable,
        ))));
    }
    let leaves = values.clone();
    for value in leaves {
        let union = ScalarValue::Union(UnionValue::new(7, Some(value.clone())));
        values.push(union.clone());
        values.push(ScalarValue::Tuple(vec![
            None,
            Some(ScalarValue::Tuple(vec![Some(value), None])),
            Some(union),
            None,
        ]));
    }
    values
}

#[test]
fn structural_operations_match_independent_shallow_derives_and_hash_feed() {
    let values = fixtures();
    let originals: Vec<_> = values.iter().map(reference).collect();
    for (index, (value, original)) in values.iter().zip(&originals).enumerate() {
        assert_eq!(
            reference(&value.clone()),
            original.clone(),
            "clone fixture {index}"
        );
        assert_eq!(feed(value), feed(original), "hash fixture {index}");
        for (other, other_reference) in values.iter().zip(&originals) {
            // Raw PValue equality has pre-existing cross-domain upcast panics.
            // Preserve those as well; typed Scalar equality first checks dtype.
            let actual = catch_unwind(AssertUnwindSafe(|| value == other)).map_err(|_| ());
            let expected =
                catch_unwind(AssertUnwindSafe(|| original == other_reference)).map_err(|_| ());
            assert_eq!(actual, expected);
        }
    }
    let wide = ScalarValue::Tuple(
        (0..4096)
            .map(|index| (index % 7 != 0).then(|| values[index % values.len()].clone()))
            .collect(),
    );
    assert_eq!(reference(&wide.clone()), reference(&wide));
    assert_eq!(feed(&wide), feed(&reference(&wide)));
    assert!(wide == wide.clone());
}

#[test]
fn clone_preserves_float_bits_and_variant_dtype_nullability() {
    let bits = 0xfff8_0000_0000_0015;
    let value = ScalarValue::Primitive(PValue::F64(f64::from_bits(bits)));
    let copied = value.clone();
    let ScalarValue::Primitive(PValue::F64(number)) = &copied else {
        panic!("primitive clone changed representation")
    };
    assert_eq!(number.to_bits(), bits);
    assert_eq!(value, copied);
    let nullable = ScalarValue::Variant(Box::new(Scalar::primitive(7_i32, Nullability::Nullable)));
    let non_nullable =
        ScalarValue::Variant(Box::new(Scalar::primitive(7_i32, Nullability::NonNullable)));
    assert_eq!(nullable, non_nullable);
    assert_eq!(feed(&nullable), feed(&non_nullable));
    let copied = nullable.clone();
    let ScalarValue::Variant(scalar) = &copied else {
        panic!("variant clone changed representation")
    };
    assert_eq!(
        scalar.dtype,
        DType::Primitive(PType::I32, Nullability::Nullable)
    );
    assert_eq!(nullable, copied);
}

struct Owner {
    drops: Arc<AtomicUsize>,
    bytes: [u8; 4],
}
impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn leaf_buffer(value: &ScalarValue) -> &ByteBuffer {
    let mut value = value;
    loop {
        value = match value {
            ScalarValue::Binary(buffer) => return buffer,
            ScalarValue::Tuple(values) => values.iter().find_map(Option::as_ref).unwrap(),
            ScalarValue::Union(union) => union.child_value().unwrap(),
            ScalarValue::Variant(scalar) => scalar.value.as_ref().unwrap(),
            _ => panic!("expected binary leaf"),
        };
    }
}

const KINDS: [&str; 3] = ["tuple", "union", "variant"];
fn chain(kind: &str, depth: usize, bytes: ByteBuffer) -> Scalar {
    let scalar = Scalar {
        value: Some(ScalarValue::Binary(bytes)),
        dtype: DType::Binary(Nullability::Nullable),
    };
    (0..depth).fold(scalar, |child, index| {
        // Internally construct type/value-consistent fixtures. The public
        // constructor still has separate validation/cast recursion obligations.
        match if kind == "mixed" {
            KINDS[index % KINDS.len()]
        } else {
            kind
        } {
            "tuple" => {
                let (dtype, value) = child.into_parts();
                Scalar {
                    value: Some(ScalarValue::Tuple(vec![None, value, None])),
                    dtype: DType::List(Arc::new(dtype), Nullability::Nullable),
                }
            }
            "union" => {
                let (dtype, value) = child.into_parts();
                Scalar {
                    value: Some(ScalarValue::Union(UnionValue::new(7, value))),
                    dtype: DType::Union(
                        UnionVariants::try_new(["child"].into(), vec![dtype], vec![7]).unwrap(),
                        Nullability::Nullable,
                    ),
                }
            }
            "variant" => Scalar {
                value: Some(ScalarValue::Variant(Box::new(child))),
                dtype: DType::Variant(Nullability::Nullable),
            },
            _ => panic!("unknown structural shape"),
        }
    })
}

#[test]
fn deep_clone_compare_hash_and_release_keep_buffer_ownership_on_small_stack() {
    const SELECTOR: &str = "VORTEX_TEST_DEEP_SCALAR_STRUCTURAL";
    const COMPLETE: &str = "deep scalar structural completed";
    if let Ok(kind) = env::var(SELECTOR) {
        let marker = format!("{COMPLETE} {kind}");
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                let drops = Arc::new(AtomicUsize::new(0));
                let buffer = ByteBuffer::from_bytes_aligned(
                    Bytes::from_owner(Owner {
                        drops: Arc::clone(&drops),
                        bytes: *b"leaf",
                    }),
                    Alignment::none(),
                );
                let source = chain(&kind, 8192, buffer);
                let same = chain(&kind, 8192, ByteBuffer::from(vec![b'l', b'e', b'a', b'f']));
                let different = chain(&kind, 8192, ByteBuffer::from(vec![b'l', b'e', b'a', b'g']));
                let copied = source.clone();
                assert!(source == same && source == copied);
                assert!(source != different);
                assert!(source.value == same.value && source.value != different.value);
                if let Some(ScalarValue::Union(union)) = &source.value {
                    let copied_union = union.clone();
                    assert!(union == &copied_union);
                    assert_eq!(hash(union), hash(&copied_union));
                    drop(copied_union);
                }
                assert_eq!(hash(&source), hash(&same));
                assert_eq!(hash(&source), hash(&copied));
                assert_eq!(
                    leaf_buffer(source.value.as_ref().unwrap()).as_ptr(),
                    leaf_buffer(copied.value.as_ref().unwrap()).as_ptr()
                );
                drop(source);
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                drop(copied);
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                drop(same);
                drop(different);
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
                "scalar::structural::tests::deep_clone_compare_hash_and_release_keep_buffer_ownership_on_small_stack",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(SELECTOR, kind)
            .output()
            .unwrap();
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

#[derive(Clone)]
struct Metadata {
    id: u8,
    events: Arc<Mutex<Vec<u8>>>,
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
        self.events.lock().push(self.id);
        self.id == other.id
    }
}
impl Eq for Metadata {}
impl Hash for Metadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.events.lock().push(self.id);
        self.id.hash(state);
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
struct Probe;
impl ExtVTable for Probe {
    type Metadata = Metadata;
    type NativeValue<'a> = &'a ScalarValue;
    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("test.scalar_structural");
        *ID
    }
    fn serialize_metadata(&self, _: &Metadata) -> VortexResult<Vec<u8>> {
        panic!("structural operations must not serialize metadata")
    }
    fn deserialize_metadata(&self, _: &[u8]) -> VortexResult<Metadata> {
        panic!("no serialized metadata fixture")
    }
    fn validate_dtype(_: &ExtDType<Self>) -> VortexResult<()> {
        Ok(())
    }
    fn unpack_native<'a>(
        _: &'a ExtDType<Self>,
        value: &'a ScalarValue,
    ) -> VortexResult<&'a ScalarValue> {
        Ok(value)
    }
}
fn probe(id: u8, value: bool, events: &Arc<Mutex<Vec<u8>>>) -> ScalarValue {
    ScalarValue::Variant(Box::new(Scalar {
        value: Some(ScalarValue::Bool(value)),
        dtype: DType::Extension(
            ExtDType::<Probe>::try_new(
                Metadata {
                    id,
                    events: Arc::clone(events),
                },
                DType::Bool(Nullability::NonNullable),
            )
            .unwrap()
            .erased(),
        ),
    }))
}

#[test]
fn structural_short_circuits_and_metadata_order_match_native_fields() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let left = ScalarValue::Tuple(vec![
        Some(probe(1, true, &events)),
        Some(probe(2, false, &events)),
    ]);
    let right = ScalarValue::Tuple(vec![
        Some(probe(1, true, &events)),
        Some(probe(2, false, &events)),
    ]);
    assert!(left == right);
    assert_eq!(*events.lock(), [1, 2]);
    events.lock().clear();
    drop(left.clone());
    assert!(events.lock().is_empty());
    let changed = ScalarValue::Tuple(vec![
        Some(probe(1, false, &events)),
        Some(probe(2, false, &events)),
    ]);
    assert!(left != changed);
    assert_eq!(*events.lock(), [1]);
    events.lock().clear();
    assert!(left != ScalarValue::Tuple(vec![Some(probe(1, true, &events))]));
    assert!(events.lock().is_empty());
    let union_left = ScalarValue::Union(UnionValue::new(1, Some(probe(3, true, &events))));
    let union_right = ScalarValue::Union(UnionValue::new(2, Some(probe(3, true, &events))));
    assert!(union_left != union_right);
    assert!(events.lock().is_empty());
    drop(feed(&left));
    assert_eq!(*events.lock(), [1, 2]);
}
