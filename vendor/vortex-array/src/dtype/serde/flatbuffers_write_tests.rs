// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::env;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::process::Command;
use std::sync::Arc;
use std::thread;

use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer};
use parking_lot::Mutex;
use vortex_error::{VortexResult, vortex_err};
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use crate::dtype::extension::{ExtDType, ExtId, ExtVTable, ForeignExtDType};
use crate::dtype::{
    DType, DecimalDType, Nullability, PType, StructFields, UnionVariants, flatbuffers as fb,
};
use crate::flatbuffers::{FlatBuffer, WriteFlatBuffer, WriteFlatBufferExt};
use crate::scalar::ScalarValue;

#[path = "flatbuffers_write_reference.rs"]
mod reference;

const KINDS: [&str; 6] = ["list", "fixed_list", "struct", "map", "union", "extension"];

fn wrap(kind: &str, child: DType, nullable: Nullability) -> DType {
    match kind {
        "list" => DType::List(Arc::new(child), nullable),
        "fixed_list" => DType::FixedSizeList(Arc::new(child), 4, nullable),
        "struct" => DType::Struct(StructFields::from_iter([("field", child)]), nullable),
        "map" => DType::map(
            DType::Primitive(PType::I64, Nullability::NonNullable),
            child,
            true,
            nullable,
        )
        .unwrap(),
        "union" => DType::Union(
            UnionVariants::try_new(["field"].into(), vec![child], vec![255]).unwrap(),
            nullable,
        ),
        "extension" => {
            static ID: CachedId = CachedId::new("test.iterative_dtype_write");
            DType::Extension(ForeignExtDType::from_parts(*ID, vec![8, 1, 7], child).unwrap())
        }
        _ => panic!("unknown writer test shape"),
    }
}

fn chain(kind: &str, depth: usize, nullability: Nullability) -> DType {
    (0..depth).fold(DType::Primitive(PType::I64, nullability), |child, index| {
        wrap(
            if kind == "mixed" {
                KINDS[index % KINDS.len()]
            } else {
                kind
            },
            child,
            nullability,
        )
    })
}

fn bytes(dtype: &DType, use_reference: bool, populated: bool) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    if populated {
        builder.force_defaults(true);
        let _prefix = builder.create_string("field");
        let _vector = builder.create_vector(&[3_u8, 255, 7]);
        let _prior =
            reference::write_reference(&DType::Bool(Nullability::Nullable), &mut builder).unwrap();
    }
    let offset = if use_reference {
        reference::write_reference(dtype, &mut builder)
    } else {
        dtype.write_flatbuffer(&mut builder)
    }
    .unwrap();
    builder.finish_minimal(offset);
    builder.finished_data().to_vec()
}

#[test]
fn iterative_schema_writer_matches_original_bytes_and_round_trips() {
    let session = VortexSession::empty();
    session.allow_unknown();
    let mut fixtures = vec![
        DType::Null,
        DType::Struct(StructFields::empty(), Nullability::NonNullable),
    ];
    for nullability in [Nullability::NonNullable, Nullability::Nullable] {
        fixtures.extend([
            DType::Bool(nullability),
            DType::Utf8(nullability),
            DType::Binary(nullability),
            DType::Variant(nullability),
            DType::Decimal(DecimalDType::try_new(38, -4).unwrap(), nullability),
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
            fixtures.push(DType::Primitive(ptype, nullability));
        }
        for kind in KINDS.into_iter().chain(["mixed"]) {
            fixtures.push(chain(kind, 7, nullability));
        }
        fixtures.push(DType::Struct(
            StructFields::from_iter([
                ("escape\"é", chain("mixed", 7, nullability)),
                ("escape\"é", DType::Bool(nullability)),
            ]),
            nullability,
        ));
        fixtures.push(DType::Union(
            UnionVariants::try_new(
                ["zero", "max", "sparse"].into(),
                vec![
                    DType::Bool(nullability),
                    chain("map", 3, nullability),
                    chain("extension", 3, nullability),
                ],
                vec![0, 255, 7],
            )
            .unwrap(),
            nullability,
        ));
        fixtures.push(
            DType::map(
                DType::Primitive(PType::U64, Nullability::NonNullable),
                DType::Utf8(nullability),
                false,
                nullability,
            )
            .unwrap(),
        );
    }
    fixtures.push(DType::Struct(
        StructFields::from_iter((0..2048).map(|index| {
            (
                format!("name{index}"),
                DType::Primitive(PType::I64, Nullability::NonNullable),
            )
        })),
        Nullability::NonNullable,
    ));
    let lazy: Vec<_> = fixtures
        .iter()
        .map(|dtype| {
            DType::from_flatbuffer(
                FlatBuffer::from(dtype.write_flatbuffer_bytes().unwrap()),
                &session,
            )
            .unwrap()
        })
        .collect();
    fixtures.extend(lazy);
    for (index, dtype) in fixtures.iter().enumerate() {
        for populated in [false, true] {
            let actual = bytes(dtype, false, populated);
            assert_eq!(
                actual,
                bytes(dtype, true, populated),
                "fixture {index}, populated={populated}"
            );
            let reopened =
                DType::from_flatbuffer(FlatBuffer::align_from(actual), &session).unwrap();
            assert!(dtype == &reopened, "round trip {index}");
        }
    }
}

#[test]
fn deep_schema_writer_on_small_stack() {
    const SELECTOR: &str = "SHARDLOOM_DTYPE_WRITER_CHILD";
    const COMPLETED: &str = "SHARDLOOM_DTYPE_WRITER_COMPLETED";
    if let Ok(kind) = env::var(SELECTOR) {
        let marker = format!("{COMPLETED} {kind}");
        let bytes = thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                let dtype = chain(&kind, 8192, Nullability::NonNullable);
                dtype.write_flatbuffer_bytes().unwrap()
            })
            .unwrap()
            .join()
            .unwrap();
        assert!(bytes.len() > 8192);
        // The unchanged generic reader still refuses this depth. Writer stack
        // safety is not advertised as a complete deep read/write lifecycle.
        assert!(matches!(
            flatbuffers::root::<fb::DType>(&bytes),
            Err(InvalidFlatbuffer::DepthLimitReached)
        ));
        println!("\n{marker}");
        return;
    }
    for kind in KINDS.into_iter().chain(["mixed"]) {
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "dtype::serde::flatbuffers::write::tests::deep_schema_writer_on_small_stack",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(SELECTOR, kind)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "writer {kind}: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line == format!("{COMPLETED} {kind}")),
            "writer child did not complete {kind}"
        );
    }
}

#[derive(Clone, Debug)]
struct Metadata {
    id: u8,
    fail: bool,
    events: Arc<Mutex<Vec<u8>>>,
}
impl PartialEq for Metadata {
    fn eq(&self, rhs: &Self) -> bool {
        self.id == rhs.id && self.fail == rhs.fail
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
        static ID: CachedId = CachedId::new("test.dtype_writer_order");
        *ID
    }
    fn serialize_metadata(&self, metadata: &Metadata) -> VortexResult<Vec<u8>> {
        metadata.events.lock().push(metadata.id);
        if metadata.fail {
            return Err(vortex_err!("metadata failure {}", metadata.id));
        }
        Ok(vec![metadata.id])
    }
    fn deserialize_metadata(&self, _: &[u8]) -> VortexResult<Metadata> {
        Err(vortex_err!("test metadata has no reader"))
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

#[test]
fn iterative_writer_preserves_metadata_callback_order_and_partial_error_bytes() {
    let events = Arc::new(Mutex::new(Vec::new()));
    for fail in [false, true] {
        let extension = |id, storage| {
            DType::Extension(
                ExtDType::<Recording>::try_new(
                    Metadata {
                        id,
                        fail: fail && id == 3,
                        events: Arc::clone(&events),
                    },
                    storage,
                )
                .unwrap()
                .erased(),
            )
        };
        let dtype = extension(
            1,
            DType::Struct(
                StructFields::from_iter([
                    ("first", extension(2, DType::Bool(Nullability::NonNullable))),
                    ("second", extension(3, DType::Utf8(Nullability::Nullable))),
                    ("third", extension(4, DType::Null)),
                ]),
                Nullability::NonNullable,
            ),
        );
        let mut snapshots = Vec::new();
        for use_reference in [true, false] {
            events.lock().clear();
            let mut builder = FlatBufferBuilder::new();
            let result = if use_reference {
                reference::write_reference(&dtype, &mut builder)
            } else {
                dtype.write_flatbuffer(&mut builder)
            };
            if fail {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("metadata failure 3")
                );
                assert_eq!(*events.lock(), [2, 3]);
            } else {
                builder.finish_minimal(result.unwrap());
                assert_eq!(*events.lock(), [2, 3, 4, 1]);
            }
            snapshots.push(builder.unfinished_data().to_vec());
        }
        assert_eq!(snapshots[0], snapshots[1]);
    }
}
