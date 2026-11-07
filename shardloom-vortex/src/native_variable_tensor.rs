//! Checked variable-shape tensor identity over native struct/list storage.

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

pub(crate) const ID: &str = "shardloom.variable_shape_tensor.v1";
#[cfg(feature = "universal-format-io")]
pub(crate) const ARROW_ID: &str = "arrow.variable_shape_tensor";
const MAX_METADATA_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct Metadata {
    json: Arc<str>,
    declared_rank: Option<usize>,
    uniform_shape: Option<Arc<[Option<i32>]>>,
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
        let mut parser =
            serde_json::Deserializer::from_str(if raw.is_empty() { "{}" } else { raw });
        let fields = parser
            .deserialize_map(MetadataVisitor)
            .map_err(|error| vortex_err!("invalid variable tensor metadata: {error}"))?;
        parser.end().map_err(|error| vortex_err!("{error}"))?;
        let mut declared_rank = None;
        for value in fields.values() {
            let rank = value.as_array().expect("parsed metadata array").len();
            vortex_ensure!(
                declared_rank.is_none_or(|expected| expected == rank),
                "variable tensor metadata arrays must have the same rank"
            );
            declared_rank = Some(rank);
        }
        if let Some(permutation) = fields.get("permutation") {
            let values = permutation.as_array().expect("parsed permutation");
            let mut seen = vec![false; values.len()];
            for value in values {
                let index = value
                    .as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .filter(|n| *n < values.len())
                    .ok_or_else(|| vortex_err!("tensor permutation index is out of range"))?;
                vortex_ensure!(
                    !seen[index],
                    "tensor permutation contains a duplicate index"
                );
                seen[index] = true;
            }
        }
        let uniform_shape = fields
            .get("uniform_shape")
            .map(|value| {
                value
                    .as_array()
                    .expect("parsed uniform shape")
                    .iter()
                    .map(|dimension| {
                        if dimension.is_null() {
                            return Ok(None);
                        }
                        dimension
                            .as_u64()
                            .and_then(|n| i32::try_from(n).ok())
                            .map(Some)
                            .ok_or_else(|| {
                                vortex_err!(
                                    "uniform tensor dimensions require nonnegative int32 or null"
                                )
                            })
                    })
                    .collect::<VortexResult<Vec<_>>>()
                    .map(Arc::from)
            })
            .transpose()?;
        let json = serde_json::to_string(&fields).map_err(|error| vortex_err!("{error}"))?;
        Ok(Self {
            json: json.into(),
            declared_rank,
            uniform_shape,
            credit: None,
        })
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.json
    }

    pub(crate) fn validate_rank(&self, rank: usize) -> VortexResult<()> {
        vortex_ensure!(
            self.declared_rank.is_none_or(|expected| expected == rank),
            "variable tensor metadata must match the storage shape rank"
        );
        Ok(())
    }

    /// The iterator reads shape coordinates only. Its caller owns visibility,
    /// resource admission and cancellation; component values are never visited.
    pub(crate) fn validate_dimensions(
        &self,
        rank: usize,
        dimensions: impl IntoIterator<Item = VortexResult<i32>>,
        elements: usize,
    ) -> VortexResult<()> {
        self.validate_rank(rank)?;
        let mut count = 0usize;
        let mut zero = false;
        let mut product = Some(1usize);
        for dimension in dimensions {
            vortex_ensure!(
                count < rank,
                "tensor shape has more dimensions than its declared rank"
            );
            let dimension = usize::try_from(dimension?)
                .map_err(|_| vortex_err!("tensor dimensions must be nonnegative int32 values"))?;
            if let Some(expected) = self.uniform_shape.as_ref().and_then(|shape| shape[count]) {
                vortex_ensure!(
                    dimension == usize::try_from(expected).expect("validated uniform dimension"),
                    "tensor shape violates a declared uniform dimension at index {count}"
                );
            }
            zero |= dimension == 0;
            product = product.and_then(|value| value.checked_mul(dimension));
            count += 1;
        }
        vortex_ensure!(
            count == rank,
            "tensor shape has fewer dimensions than its declared rank"
        );
        let product = if zero {
            0
        } else {
            product
                .ok_or_else(|| vortex_err!("tensor shape product exceeds native list capacity"))?
        };
        vortex_ensure!(
            product == elements,
            "tensor shape product {product} differs from data length {elements}"
        );
        Ok(())
    }
}

struct MetadataVisitor;
impl<'de> Visitor<'de> for MetadataVisitor {
    type Value = serde_json::Map<String, serde_json::Value>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("variable tensor dimension names, permutation and uniform shape")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut fields = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if fields.contains_key(&key) {
                return Err(A::Error::custom("duplicate tensor metadata field"));
            }
            let value = match key.as_str() {
                "permutation" => serde_json::Value::from(map.next_value::<Vec<u64>>()?),
                "dim_names" => serde_json::Value::from(map.next_value::<Vec<String>>()?),
                "uniform_shape" => serde_json::Value::from(map.next_value::<Vec<Option<u64>>>()?),
                _ => return Err(A::Error::custom("unknown variable tensor metadata field")),
            };
            fields.insert(key, value);
        }
        Ok(fields)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct VariableShapeTensor {
    allocator: Option<HostAllocatorRef>,
}
impl PartialEq for VariableShapeTensor {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for VariableShapeTensor {}
impl Hash for VariableShapeTensor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        ID.hash(state);
    }
}

impl ExtVTable for VariableShapeTensor {
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
        // Canonical JSON and the cached optional Int32 uniform dimensions share
        // this owner. The bound includes short numeric/null JSON representations.
        let retained = raw
            .len()
            .checked_mul(8)
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
        dtype
            .metadata()
            .validate_rank(rank(dtype.storage_dtype())? as usize)
    }
    fn unpack_native<'a>(
        _: &'a ExtDType<Self>,
        value: &'a ScalarValue,
    ) -> VortexResult<&'a ScalarValue> {
        Ok(value)
    }
}

pub(crate) fn rank(storage: &DType) -> VortexResult<u32> {
    let DType::Struct(fields, _) = storage else {
        return Err(vortex_err!(
            "variable tensor storage requires data and shape struct fields"
        ));
    };
    vortex_ensure!(
        fields.nfields() == 2
            && fields.names()[0].as_ref() == "data"
            && fields.names()[1].as_ref() == "shape",
        "variable tensor storage requires data then shape fields"
    );
    let mut children = fields.fields();
    let data = children.next().expect("two fields");
    let shape = children.next().expect("two fields");
    vortex_ensure!(
        matches!(&data, DType::List(child, _) if crate::native_tensor::element_admitted(child)),
        "variable tensor data requires a Boolean/integer/Float32/Float64 list"
    );
    let DType::FixedSizeList(child, rank, _) = shape else {
        return Err(vortex_err!(
            "variable tensor shape requires a fixed-size Int32 list"
        ));
    };
    vortex_ensure!(
        matches!(child.as_ref(), DType::Primitive(PType::I32, _)),
        "variable tensor shape dimensions require Int32 storage"
    );
    Ok(rank)
}

pub(crate) fn storage(dtype: &DType) -> Option<&DType> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    extension.metadata_opt::<VariableShapeTensor>()?;
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
    crate::native_tensor::edition::register_variable(session);
    #[cfg(feature = "universal-format-io")]
    arrow::register(session);
}
pub(crate) fn register_dtype(session: &vortex::session::VortexSession) {
    use vortex::array::{dtype::session::DTypeSessionExt as _, memory::HostMemorySessionExt as _};
    session.dtypes().register(VariableShapeTensor {
        allocator: Some(session.host_allocator()),
    });
}

#[cfg(feature = "universal-format-io")]
#[path = "native_variable_tensor_arrow.rs"]
pub(crate) mod arrow;

#[cfg(all(test, feature = "vortex-local-primitives"))]
#[path = "native_variable_tensor_tests.rs"]
mod tests;
