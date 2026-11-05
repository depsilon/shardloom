//! Versioned embedding identity over native fixed-size floating-list storage.
//! This codec admits opaque carriage, not vector comparison or arithmetic.

use serde::{
    Deserializer as _,
    de::{Error as _, MapAccess, Visitor},
};
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};
use vortex::{
    array::{
        dtype::{
            DType, PType,
            extension::{ExtDType, ExtId, ExtVTable},
        },
        memory::{HostAllocatorRef, HostMemoryReservation},
        scalar::ScalarValue,
    },
    error::{VortexResult, vortex_ensure, vortex_err},
    session::registry::CachedId,
};

pub(crate) const ID: &str = "shardloom.embedding.v1";
const MAX_METADATA_BYTES: usize = 8 * 1024 * 1024;
const IDENTITY_FIELDS: &[&str] = &[
    "space_id",
    "model_id",
    "model_version",
    "representation_version",
    "normalization",
    "preprocessing_id",
    "source_provenance",
];

/// Immutable canonical JSON. Unknown identity stays unknown and strings retain
/// their exact bytes. Dimension/type/nullability are owned by the storage dtype.
#[derive(Debug, Clone)]
pub(crate) struct Metadata(Arc<str>, Option<Arc<HostMemoryReservation>>);

impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for Metadata {}
impl Hash for Metadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Metadata {
    pub(crate) fn parse(raw: &str) -> VortexResult<Self> {
        vortex_ensure!(
            raw.len() <= MAX_METADATA_BYTES,
            "embedding metadata exceeds 8 MiB"
        );
        // Parse a concrete map of scalar values; duplicate keys must not be
        // silently overwritten by serde_json's generic Value deserializer.
        let mut parser = serde_json::Deserializer::from_str(raw);
        let value = parser
            .deserialize_map(MetadataVisitor)
            .map_err(|error| vortex_err!("invalid embedding metadata: {error}"))?;
        parser.end().map_err(|error| vortex_err!("{error}"))?;
        let canonical = serde_json::to_string(&value).map_err(|error| vortex_err!("{error}"))?;
        Ok(Self(canonical.into(), None))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

struct MetadataVisitor;

impl<'de> Visitor<'de> for MetadataVisitor {
    type Value = serde_json::Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("versioned embedding identity metadata")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut fields = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if fields.contains_key(&key) {
                return Err(A::Error::custom("duplicate embedding metadata field"));
            }
            let value = if key == "version" {
                if map.next_value::<u32>()? != 1 {
                    return Err(A::Error::custom("embedding metadata version must be 1"));
                }
                serde_json::Value::from(1)
            } else {
                if !IDENTITY_FIELDS.contains(&key.as_str()) {
                    return Err(A::Error::custom("unknown embedding metadata field"));
                }
                let value = map.next_value::<Option<String>>()?;
                if value.as_ref().is_some_and(String::is_empty) {
                    return Err(A::Error::custom(
                        "embedding identity fields require nonempty strings or null",
                    ));
                }
                value.map_or(serde_json::Value::Null, serde_json::Value::String)
            };
            fields.insert(key, value);
        }
        if !fields.contains_key("version") {
            return Err(A::Error::custom("embedding metadata version must be 1"));
        }
        Ok(serde_json::Value::Object(fields))
    }
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Embedding {
    allocator: Option<HostAllocatorRef>,
}

// Allocation ownership is session-local, not part of logical type identity.
impl PartialEq for Embedding {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for Embedding {}
impl Hash for Embedding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        ID.hash(state);
    }
}

impl ExtVTable for Embedding {
    type Metadata = Metadata;
    type NativeValue<'a> = &'a ScalarValue;

    fn id(&self) -> ExtId {
        static NAME: CachedId = CachedId::new(ID);
        *NAME
    }

    fn serialize_metadata(&self, metadata: &Metadata) -> VortexResult<Vec<u8>> {
        Ok(metadata.as_str().as_bytes().to_vec())
    }

    fn deserialize_metadata(&self, raw: &[u8]) -> VortexResult<Metadata> {
        vortex_ensure!(
            raw.len() <= MAX_METADATA_BYTES,
            "embedding metadata exceeds 8 MiB"
        );
        // The file footer invokes this registry directly, before operation-level
        // schema conversion. Admit both bounded JSON scratch and the immutable
        // string owner through the same session allocator. Inert caller metadata
        // and already-admitted Arrow/typed declarations use the default vtable.
        let workspace = raw
            .len()
            .checked_mul(16)
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(|| vortex_err!("embedding metadata workspace overflow"))?;
        let retained = raw
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(256))
            .ok_or_else(|| vortex_err!("embedding metadata retained size overflow"))?;
        let _workspace = self
            .allocator
            .as_ref()
            .map(|allocator| allocator.reserve_external(workspace))
            .transpose()?;
        let retained = self
            .allocator
            .as_ref()
            .map(|allocator| allocator.reserve_external(retained))
            .transpose()?;
        let raw = std::str::from_utf8(raw).map_err(|error| vortex_err!("{error}"))?;
        let mut metadata = Metadata::parse(raw)?;
        vortex_ensure!(
            metadata.as_str() == raw,
            "native embedding metadata requires its canonical encoding"
        );
        metadata.1 = retained.map(Arc::new);
        Ok(metadata)
    }

    fn validate_dtype(dtype: &ExtDType<Self>) -> VortexResult<()> {
        vortex_ensure!(
            matches!(dtype.storage_dtype(), DType::FixedSizeList(child, size, _)
            if *size > 0 && matches!(child.as_ref(), DType::Primitive(PType::F32 | PType::F64, _))),
            "embedding storage requires a positive fixed-size Float32/Float64 list"
        );
        Ok(())
    }

    fn unpack_native<'a>(
        _: &'a ExtDType<Self>,
        value: &'a ScalarValue,
    ) -> VortexResult<&'a ScalarValue> {
        Ok(value)
    }
}

pub(crate) fn storage(dtype: &DType) -> Option<&DType> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    extension.metadata_opt::<Embedding>()?;
    Some(extension.storage_dtype())
}

pub(crate) fn contains(dtype: &DType) -> bool {
    match dtype {
        DType::Extension(_) => storage(dtype).is_some(),
        DType::List(child, _) | DType::FixedSizeList(child, _, _) => contains(child),
        DType::Struct(fields, _) => fields.fields().any(|child| contains(&child)),
        _ => false,
    }
}

pub(crate) fn register(session: &vortex::session::VortexSession) {
    register_dtype(session);
    edition::register(session);
    #[cfg(feature = "universal-format-io")]
    arrow::register(session);
}

/// A spill session inherits logical policy but must use its own metadata grant.
pub(crate) fn register_dtype(session: &vortex::session::VortexSession) {
    use vortex::array::dtype::session::DTypeSessionExt as _;
    use vortex::array::memory::HostMemorySessionExt as _;
    session.dtypes().register(Embedding {
        allocator: Some(session.host_allocator()),
    });
}

#[path = "native_embedding_edition.rs"]
mod edition;

#[cfg(feature = "universal-format-io")]
#[path = "native_embedding_arrow.rs"]
pub(crate) mod arrow;

#[cfg(all(test, feature = "vortex-local-primitives"))]
mod tests {
    use super::*;
    use vortex::array::dtype::Nullability;

    #[test]
    fn dense_embedding_metadata_is_versioned_exact_and_canonical() {
        let metadata =
            Metadata::parse(r#" { "version": 1, "space_id": "未知 Ω", "normalization": null } "#)
                .unwrap();
        assert_eq!(
            metadata.as_str(),
            r#"{"normalization":null,"space_id":"未知 Ω","version":1}"#
        );
        let bytes = Embedding::default().serialize_metadata(&metadata).unwrap();
        assert_eq!(
            Embedding::default().deserialize_metadata(&bytes).unwrap(),
            metadata
        );
        for invalid in [
            "{}",
            r#"{"version":2}"#,
            r#"{"version":true}"#,
            r#"{"version":1.0}"#,
            r#"{"version":1,"version":1}"#,
            r#"{"version":1,"space_id":"a","space_id":"b"}"#,
            r#"{"version":1,"unknown":null}"#,
            r#"{"version":1,"space_id":""}"#,
            r#"{"version":1,"space_id":[]}"#,
            r#"{"version":1} false"#,
        ] {
            assert!(Metadata::parse(invalid).is_err(), "{invalid}");
        }
        assert!(
            Embedding::default()
                .deserialize_metadata(b"{ \"version\": 1 }")
                .is_err()
        );
        assert!(Embedding::default().deserialize_metadata(&[0xff]).is_err());
    }

    #[test]
    fn dense_embedding_received_metadata_owns_its_reservation_until_last_clone() {
        use shardloom_exec::live_memory::LiveMemoryPool;
        let memory = LiveMemoryPool::new(64 << 10).unwrap();
        let provider = Embedding {
            allocator: Some(Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
                memory.clone(),
            ))),
        };
        let metadata = provider
            .deserialize_metadata(br#"{"space_id":"test","version":1}"#)
            .unwrap();
        let retained = memory.snapshot().reserved_bytes;
        assert!(retained > 0 && retained < 1024);
        let clone = metadata.clone();
        drop(metadata);
        assert_eq!(memory.snapshot().reserved_bytes, retained);
        drop(clone);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        for raw in [
            br#"{"version":2}"#.to_vec(),
            format!(r#"{{"space_id":"{}","version":1}}"#, "x".repeat(8192)).into_bytes(),
        ] {
            assert!(provider.deserialize_metadata(&raw).is_err());
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }

    #[test]
    fn dense_embedding_storage_keeps_dimension_precision_and_both_nullabilities() {
        let metadata = Metadata::parse(r#"{"version":1}"#).unwrap();
        for ptype in [PType::F32, PType::F64] {
            for outer in [Nullability::Nullable, Nullability::NonNullable] {
                for component in [Nullability::Nullable, Nullability::NonNullable] {
                    for dimension in [1, 1536, 65537, u32::MAX] {
                        let storage = DType::FixedSizeList(
                            DType::Primitive(ptype, component).into(),
                            dimension,
                            outer,
                        );
                        let dtype = DType::Extension(
                            ExtDType::<Embedding>::try_new(metadata.clone(), storage.clone())
                                .unwrap()
                                .erased(),
                        );
                        assert_eq!(super::storage(&dtype), Some(&storage));
                        assert_eq!(dtype.nullability(), outer);
                        assert!(!crate::native_payload_schema::admitted_scalar(&dtype));
                    }
                }
            }
        }
        for storage in [
            DType::Primitive(PType::F32, Nullability::Nullable),
            DType::FixedSizeList(
                DType::Primitive(PType::F32, Nullability::Nullable).into(),
                0,
                Nullability::Nullable,
            ),
            DType::FixedSizeList(
                DType::Primitive(PType::F16, Nullability::Nullable).into(),
                3,
                Nullability::Nullable,
            ),
            DType::FixedSizeList(
                DType::Primitive(PType::I64, Nullability::Nullable).into(),
                3,
                Nullability::Nullable,
            ),
        ] {
            assert!(ExtDType::<Embedding>::try_new(metadata.clone(), storage).is_err());
        }
    }
}
