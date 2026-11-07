//! Checked fixed-shape tensor identity over native list storage.
//! Shape describes physical row-major storage; permutation only describes a view.

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

pub(crate) const ID: &str = "shardloom.fixed_shape_tensor.v1";
#[cfg(feature = "universal-format-io")]
pub(crate) const ARROW_ID: &str = "arrow.fixed_shape_tensor";
const MAX_METADATA_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct Metadata {
    json: Arc<str>,
    elements: u32,
    credit: Option<Arc<HostMemoryReservation>>,
}

impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        self.json == other.json
    }
}
impl Eq for Metadata {}
impl Hash for Metadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.json.hash(state);
    }
}
impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.json)
    }
}

impl Metadata {
    pub(crate) fn parse(raw: &str) -> VortexResult<Self> {
        vortex_ensure!(
            raw.len() <= MAX_METADATA_BYTES,
            "tensor metadata exceeds 8 MiB"
        );
        let mut parser = serde_json::Deserializer::from_str(raw);
        let fields = parser
            .deserialize_map(MetadataVisitor)
            .map_err(|error| vortex_err!("invalid tensor metadata: {error}"))?;
        parser.end().map_err(|error| vortex_err!("{error}"))?;
        let shape = fields
            .get("shape")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| vortex_err!("tensor metadata requires shape"))?;
        let rank = shape.len();
        for dimension in shape {
            vortex_ensure!(
                dimension.as_u64().is_some_and(|n| i64::try_from(n).is_ok()),
                "tensor shape dimensions must be nonnegative int64 values"
            );
        }
        let elements = if shape.iter().any(|n| n.as_u64() == Some(0)) {
            0
        } else {
            shape.iter().try_fold(1_u32, |product, n| {
                u32::try_from(n.as_u64().expect("validated dimension"))
                    .ok()
                    .and_then(|n| product.checked_mul(n))
                    .ok_or_else(|| {
                        vortex_err!("tensor shape product exceeds native uint32 list width")
                    })
            })?
        };
        if let Some(names) = fields.get("dim_names") {
            vortex_ensure!(
                names.as_array().is_some_and(|names| names.len() == rank),
                "tensor dimension names must match shape rank"
            );
        }
        if let Some(permutation) = fields.get("permutation") {
            let permutation = permutation.as_array().expect("parsed permutation array");
            vortex_ensure!(
                permutation.len() == rank,
                "tensor permutation must match shape rank"
            );
            let mut seen = vec![false; rank];
            for value in permutation {
                let index = value
                    .as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .filter(|n| *n < rank)
                    .ok_or_else(|| vortex_err!("tensor permutation index is out of range"))?;
                vortex_ensure!(
                    !seen[index],
                    "tensor permutation contains a duplicate index"
                );
                seen[index] = true;
            }
        }
        let json = serde_json::to_string(&fields).map_err(|error| vortex_err!("{error}"))?;
        Ok(Self {
            json: json.into(),
            elements,
            credit: None,
        })
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.json
    }
}

struct MetadataVisitor;
impl<'de> Visitor<'de> for MetadataVisitor {
    type Value = serde_json::Map<String, serde_json::Value>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("fixed tensor shape, dimension names and permutation")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut fields = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if fields.contains_key(&key) {
                return Err(A::Error::custom("duplicate tensor metadata field"));
            }
            let value = match key.as_str() {
                "shape" | "permutation" => serde_json::Value::from(map.next_value::<Vec<u64>>()?),
                "dim_names" => serde_json::Value::from(map.next_value::<Vec<String>>()?),
                _ => return Err(A::Error::custom("unknown tensor metadata field")),
            };
            fields.insert(key, value);
        }
        Ok(fields)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FixedShapeTensor {
    allocator: Option<HostAllocatorRef>,
}
impl PartialEq for FixedShapeTensor {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for FixedShapeTensor {}
impl Hash for FixedShapeTensor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        ID.hash(state);
    }
}

impl ExtVTable for FixedShapeTensor {
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
            "tensor metadata exceeds 8 MiB"
        );
        let workspace = raw
            .len()
            .checked_mul(128)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| vortex_err!("tensor metadata workspace overflow"))?;
        let retained = raw
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(256))
            .ok_or_else(|| vortex_err!("tensor metadata retained size overflow"))?;
        let _workspace = self
            .allocator
            .as_ref()
            .map(|a| a.reserve_external(workspace))
            .transpose()?;
        let credit = self
            .allocator
            .as_ref()
            .map(|a| a.reserve_external(retained))
            .transpose()?;
        let raw = std::str::from_utf8(raw).map_err(|error| vortex_err!("{error}"))?;
        let mut metadata = Metadata::parse(raw)?;
        vortex_ensure!(
            metadata.as_str() == raw,
            "native tensor metadata requires its canonical encoding"
        );
        metadata.credit = credit.map(Arc::new);
        Ok(metadata)
    }
    fn validate_dtype(dtype: &ExtDType<Self>) -> VortexResult<()> {
        vortex_ensure!(
            matches!(dtype.storage_dtype(), DType::FixedSizeList(child, size, _)
            if element_admitted(child) && *size == dtype.metadata().elements),
            "tensor storage requires a Boolean/integer/Float32/Float64 fixed-size list matching the shape product"
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

pub(crate) fn element_admitted(dtype: &DType) -> bool {
    matches!(dtype, DType::Bool(_))
        || matches!(dtype, DType::Primitive(ptype, _) if *ptype != PType::F16)
}

pub(crate) fn storage(dtype: &DType) -> Option<&DType> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    extension.metadata_opt::<FixedShapeTensor>()?;
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
pub(crate) fn register_dtype(session: &vortex::session::VortexSession) {
    use vortex::array::{dtype::session::DTypeSessionExt as _, memory::HostMemorySessionExt as _};
    session.dtypes().register(FixedShapeTensor {
        allocator: Some(session.host_allocator()),
    });
}

#[cfg(feature = "universal-format-io")]
#[path = "native_tensor_arrow.rs"]
pub(crate) mod arrow;

#[path = "native_tensor_edition.rs"]
pub(crate) mod edition;
#[cfg(all(test, feature = "vortex-local-primitives"))]
#[path = "native_tensor_tests.rs"]
mod tests;
