use std::env;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;

use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::dtype::extension::{ExtDType, ExtId, ExtVTable};
use vortex::array::dtype::{DecimalDType, PType, StructFields, UnionVariants};
use vortex::array::flatbuffers::{FlatBuffer, WriteFlatBufferExt};
use vortex::array::scalar::ScalarValue;
use vortex::error::{VortexResult, vortex_err};
use vortex::session::VortexSession;

use super::*;

const KINDS: [&str; 7] = [
    "list",
    "fixed_list",
    "map_key",
    "map_value",
    "struct",
    "union",
    "extension",
];

fn wrap(kind: &str, child: DType, nullable: Nullability) -> DType {
    match kind {
        "list" => DType::List(child.into(), nullable),
        "fixed_list" => DType::FixedSizeList(child.into(), 17, nullable),
        "map_key" => DType::map(
            child.with_nullability(Nullability::NonNullable),
            DType::Bool(nullable),
            true,
            nullable,
        )
        .unwrap(),
        "map_value" => DType::map(
            DType::Bool(Nullability::NonNullable),
            child,
            false,
            nullable,
        )
        .unwrap(),
        "struct" => DType::Struct(
            StructFields::from_iter([("\"\\\n\t\u{0000}naïve😀", child)]),
            nullable,
        ),
        "union" => DType::Union(
            UnionVariants::try_new(["é\\\""].into(), vec![child], vec![255]).unwrap(),
            nullable,
        ),
        "extension" => DType::Extension(ExtDType::<Plain>::try_new(0, child).unwrap().erased()),
        _ => panic!("unknown native JSON test shape"),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
struct Plain;
impl ExtVTable for Plain {
    type Metadata = u8;
    type NativeValue<'a> = &'a ScalarValue;
    fn id(&self) -> ExtId {
        ExtId::new("test.native_json.é\"")
    }
    fn serialize_metadata(&self, _: &u8) -> VortexResult<Vec<u8>> {
        Ok(vec![0, 255, 13, 10])
    }
    fn deserialize_metadata(&self, _: &[u8]) -> VortexResult<u8> {
        Ok(0)
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

fn chain(kind: &str, depth: usize) -> DType {
    (0..depth).fold(DType::Bool(Nullability::NonNullable), |child, index| {
        wrap(
            if kind == "mixed" {
                KINDS[index % KINDS.len()]
            } else {
                kind
            },
            child,
            Nullability::NonNullable,
        )
    })
}

fn fixtures() -> Vec<DType> {
    let mut result = vec![
        DType::Null,
        DType::Struct(StructFields::empty(), Nullability::Nullable),
    ];
    for nullable in [Nullability::NonNullable, Nullability::Nullable] {
        result.extend([
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
            result.push(DType::Primitive(ptype, nullable));
        }
        for kind in KINDS {
            result.push(wrap(kind, chain("mixed", 3), nullable));
        }
    }
    result.push(DType::Struct(
        StructFields::from_iter(
            (0..2048).map(|index| (format!("field_{index}"), DType::Bool(Nullability::Nullable))),
        ),
        Nullability::NonNullable,
    ));
    result
}

#[test]
fn native_schema_json_matches_generic_serde_for_all_shapes_and_lazy_fields() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let session = VortexSession::empty();
    session.allow_unknown();
    for dtype in fixtures() {
        let expected = serde_json::to_vec(&dtype).unwrap();
        let lazy = DType::from_flatbuffer(
            FlatBuffer::from(dtype.write_flatbuffer_bytes().unwrap()),
            &session,
        )
        .unwrap();
        for dtype in [dtype, lazy] {
            let mut actual = Vec::new();
            write(&dtype, &memory, &mut actual).unwrap();
            assert_eq!(actual, expected);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[derive(Clone, Debug)]
struct Metadata {
    id: u8,
    fail: bool,
    events: Arc<Mutex<Vec<u8>>>,
}
impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.fail == other.fail
    }
}
impl Eq for Metadata {}
impl Hash for Metadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.fail.hash(state);
    }
}
impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
struct Recording;
impl ExtVTable for Recording {
    type Metadata = Metadata;
    type NativeValue<'a> = &'a ScalarValue;
    fn id(&self) -> ExtId {
        ExtId::new("test.native_json.order")
    }
    fn serialize_metadata(&self, metadata: &Metadata) -> VortexResult<Vec<u8>> {
        metadata.events.lock().unwrap().push(metadata.id);
        if metadata.fail {
            return Err(vortex_err!("metadata failure {}", metadata.id));
        }
        Ok(vec![metadata.id, 0, 255])
    }
    fn deserialize_metadata(&self, _: &[u8]) -> VortexResult<Metadata> {
        Err(vortex_err!("test has no metadata reader"))
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

fn recorded(events: &Arc<Mutex<Vec<u8>>>, fail: bool) -> DType {
    let extension = |id, child| {
        DType::Extension(
            ExtDType::<Recording>::try_new(
                Metadata {
                    id,
                    fail: fail && id == 3,
                    events: Arc::clone(events),
                },
                child,
            )
            .unwrap()
            .erased(),
        )
    };
    extension(
        1,
        DType::Struct(
            StructFields::from_iter([
                ("first", extension(2, DType::Bool(Nullability::NonNullable))),
                ("second", extension(3, chain("mixed", 2))),
                ("third", extension(4, DType::Utf8(Nullability::Nullable))),
            ]),
            Nullability::NonNullable,
        ),
    )
}

struct Sink {
    bytes: Vec<u8>,
    chunks: Vec<Vec<u8>>,
    remaining: usize,
}
impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.chunks.push(bytes.to_vec());
        if self.remaining == 0 {
            return Err(std::io::Error::other("test sink full"));
        }
        let len = bytes.len().min(self.remaining);
        self.remaining -= len;
        self.bytes.extend_from_slice(&bytes[..len]);
        Ok(len)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn native_schema_json_preserves_callbacks_and_every_failed_sink_boundary() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    for fail in [false, true] {
        let dtype = recorded(&events, fail);
        let size = serde_json::to_vec(&recorded(&events, false)).unwrap().len();
        for remaining in 0..=size + 1 {
            let mut snapshots = Vec::new();
            for original in [true, false] {
                events.lock().unwrap().clear();
                let mut sink = Sink {
                    bytes: Vec::new(),
                    chunks: Vec::new(),
                    remaining,
                };
                let result = if original {
                    serde_json::to_writer(&mut sink, &dtype)
                } else {
                    write(&dtype, &memory, &mut sink).map_err(|error| match error {
                        Error::Json(error) => error,
                        Error::Resource(error) => panic!("unexpected resource refusal: {error}"),
                    })
                };
                snapshots.push((
                    sink.bytes,
                    sink.chunks,
                    result.err().map(|error| error.to_string()),
                    events.lock().unwrap().clone(),
                ));
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
            assert_eq!(
                snapshots[0], snapshots[1],
                "failure={fail}, sink bytes={remaining}"
            );
        }
    }
}

#[test]
fn native_schema_json_charges_growth_and_refunds_on_denial_and_cancellation() {
    struct CancelSink(CancellationToken);
    impl Write for CancelSink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.cancel();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let dtype = chain("mixed", 35);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    write(&dtype, &memory, std::io::sink()).unwrap();
    let peak = memory.snapshot().peak_reserved_bytes;
    assert!(peak > size_of::<Frame<'_>>() as u64 * 35);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    for limit in [1, peak - 1] {
        let memory = LiveMemoryPool::new(limit + 17).unwrap();
        let existing = memory.reserve(17).unwrap();
        assert!(matches!(
            write(&dtype, &memory, std::io::sink()),
            Err(Error::Resource(_))
        ));
        assert!(memory.snapshot().denied_reservations > 0);
        assert_eq!(memory.snapshot().reserved_bytes, existing.bytes());
        drop(existing);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let token = CancellationToken::default();
    assert!(matches!(
        write(&dtype, (&memory, &token), CancelSink(token.clone())),
        Err(Error::Resource(_))
    ));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let mut untouched = Vec::new();
    assert!(write(&dtype, (&memory, &token), &mut untouched).is_err());
    assert_eq!(untouched.as_slice(), [] as [u8; 0]);
}

#[test]
fn result_and_application_schema_boundaries_keep_the_wire_contract() {
    let dtype = DType::Struct(
        StructFields::from_iter([("list", chain("list", 4)), ("record", chain("struct", 4))]),
        Nullability::NonNullable,
    );
    let expected = serde_json::to_string(&dtype).unwrap();
    let memory = LiveMemoryPool::new(32 << 20).unwrap();
    let result =
        crate::local_primitives::collect::serialize_result_schema(&dtype, &memory).unwrap();
    assert_eq!(result.value(), &expected);
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    crate::local_primitives::native_input_schema::validate_application_dtype(&dtype).unwrap();
    assert!(
        crate::local_primitives::collect::serialize_result_schema(&chain("list", 65), &memory)
            .is_err()
    );
    assert!(
        crate::local_primitives::native_input_schema::validate_application_dtype(&chain(
            "list", 25
        ))
        .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_schema_json_preserves_exact_eight_mib_wire_limits() {
    const LIMIT: usize = 8 << 20;
    let schema = |name: String| {
        DType::Struct(
            StructFields::from_iter([(name, DType::Bool(Nullability::NonNullable))]),
            Nullability::NonNullable,
        )
    };
    let overhead = serde_json::to_vec(&schema(String::new())).unwrap().len();
    let memory = LiveMemoryPool::new(32 << 20).unwrap();
    for size in [LIMIT - 1, LIMIT, LIMIT + 1] {
        let dtype = schema("a".repeat(size - overhead));
        let result = crate::local_primitives::collect::serialize_result_schema(&dtype, &memory);
        let application =
            crate::local_primitives::native_input_schema::validate_application_dtype(&dtype);
        if size <= LIMIT {
            assert_eq!(result.as_ref().unwrap().value().len(), size);
            application.unwrap();
        } else {
            assert!(
                result
                    .as_ref()
                    .unwrap_err()
                    .to_string()
                    .contains("collect exceeds 8 MiB")
            );
            assert!(
                application
                    .unwrap_err()
                    .to_string()
                    .contains("application schema exceeds 8 MiB")
            );
        }
        drop(result);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn deep_native_schema_json_on_small_stack() {
    const SELECTOR: &str = "SHARDLOOM_DEEP_DTYPE_JSON";
    const COMPLETED: &str = "deep native schema JSON completed";
    if env::var_os(SELECTOR).is_some() {
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(|| {
                for kind in KINDS.into_iter().chain(["mixed"]) {
                    let dtype = chain(kind, 8192);
                    let memory = LiveMemoryPool::new(8 << 20).unwrap();
                    let mut bytes = Vec::new();
                    write(&dtype, &memory, &mut bytes).unwrap();
                    assert!(bytes.len() > 8192);
                    assert_eq!(memory.snapshot().reserved_bytes, 0);
                    let mut sink = Sink {
                        bytes: Vec::new(),
                        chunks: Vec::new(),
                        remaining: bytes.len() / 2,
                    };
                    assert!(matches!(
                        write(&dtype, &memory, &mut sink),
                        Err(Error::Json(_))
                    ));
                    assert_eq!(sink.bytes, bytes[..bytes.len() / 2]);
                    assert_eq!(memory.snapshot().reserved_bytes, 0);
                    drop(dtype);
                }
            })
            .unwrap()
            .join()
            .unwrap();
        println!("\n{COMPLETED}");
        return;
    }
    let output = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "native_payload_schema::json::tests::deep_native_schema_json_on_small_stack",
            "--test-threads=1",
            "--nocapture",
        ])
        .env(SELECTOR, "1")
        .output()
        .unwrap();
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
