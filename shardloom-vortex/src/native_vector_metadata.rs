//! Bounded vector interpretation metadata and shared cross-field rules.

use super::{Kind, MAX_METADATA_BYTES};
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
    array::memory::{HostAllocatorRef, HostMemoryReservation},
    error::{VortexResult, vortex_ensure, vortex_err},
};

#[derive(Debug, Clone)]
pub(crate) struct Metadata {
    json: Arc<str>,
    kind: Kind,
    uniform_size: Option<u64>,
    strictly_increasing: bool,
    block_size: Option<u64>,
    credit: Option<Arc<HostMemoryReservation>>,
}
impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.json == other.json
    }
}
impl Eq for Metadata {}
impl Hash for Metadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.json.hash(state);
    }
}
impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.json)
    }
}

impl Metadata {
    pub(crate) fn parse(kind: Kind, raw: &str) -> VortexResult<Self> {
        vortex_ensure!(
            raw.len() <= MAX_METADATA_BYTES,
            "vector metadata exceeds 8 MiB"
        );
        let mut parser = serde_json::Deserializer::from_str(raw);
        let fields = parser
            .deserialize_map(Fields(kind))
            .map_err(|e| vortex_err!("invalid vector metadata: {e}"))?;
        parser.end().map_err(|e| vortex_err!("{e}"))?;
        let strictly_increasing = match kind {
            Kind::Sparse => match fields
                .get("index_policy")
                .and_then(serde_json::Value::as_str)
            {
                Some("preserve") => false,
                Some("strictly_increasing") => true,
                _ => {
                    return Err(vortex_err!(
                        "sparse index_policy requires preserve or strictly_increasing"
                    ));
                }
            },
            Kind::Quantized => false,
        };
        let block_size = fields.get("block_size").and_then(serde_json::Value::as_u64);
        if kind == Kind::Quantized {
            match fields
                .get("parameter_mode")
                .and_then(serde_json::Value::as_str)
            {
                Some("per_vector") => vortex_ensure!(
                    block_size.is_none(),
                    "per-vector quantization forbids block_size"
                ),
                Some("blocked") => vortex_ensure!(
                    block_size.is_some_and(|n| n > 0),
                    "blocked quantization requires positive block_size"
                ),
                _ => {
                    return Err(vortex_err!(
                        "quantized parameter_mode requires per_vector or blocked"
                    ));
                }
            }
        }
        let uniform_size = fields
            .get("uniform_size")
            .and_then(serde_json::Value::as_u64);
        let json = serde_json::to_string(&fields).map_err(|e| vortex_err!("{e}"))?;
        Ok(Self {
            json: json.into(),
            kind,
            uniform_size,
            strictly_increasing,
            block_size,
            credit: None,
        })
    }

    pub(super) fn receive(
        kind: Kind,
        raw: &[u8],
        allocator: Option<&HostAllocatorRef>,
    ) -> VortexResult<Self> {
        vortex_ensure!(
            raw.len() <= MAX_METADATA_BYTES,
            "vector metadata exceeds 8 MiB"
        );
        let workspace = raw
            .len()
            .checked_mul(16)
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(|| vortex_err!("vector metadata workspace overflow"))?;
        let retained = raw
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(256))
            .ok_or_else(|| vortex_err!("vector metadata retention overflow"))?;
        let _workspace = allocator
            .map(|a| a.reserve_external(workspace))
            .transpose()?;
        let credit = allocator
            .map(|a| a.reserve_external(retained))
            .transpose()?;
        let raw = std::str::from_utf8(raw).map_err(|e| vortex_err!("{e}"))?;
        let mut metadata = Self::parse(kind, raw)?;
        vortex_ensure!(
            metadata.as_str() == raw,
            "native vector metadata requires its canonical encoding"
        );
        metadata.credit = credit.map(Arc::new);
        Ok(metadata)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.json
    }
    pub(crate) fn kind(&self) -> Kind {
        self.kind
    }
    pub(crate) fn block_size(&self) -> Option<u64> {
        self.block_size
    }

    pub(crate) fn validate_size(&self, size: u64) -> VortexResult<()> {
        vortex_ensure!(
            self.uniform_size.is_none_or(|expected| expected == size),
            "vector size violates uniform_size"
        );
        Ok(())
    }

    /// The caller supplies exact integer registers and owns selected visibility,
    /// cancellation and resource grants. No dimension-sized scratch is required.
    pub(crate) fn validate_sparse(
        &self,
        size: u64,
        count: usize,
        values: usize,
        indices: impl IntoIterator<Item = VortexResult<i128>>,
    ) -> VortexResult<()> {
        vortex_ensure!(self.kind == Kind::Sparse, "expected sparse metadata");
        self.validate_size(size)?;
        vortex_ensure!(count == values, "sparse index and value lengths differ");
        let mut previous = None;
        let mut seen = 0usize;
        for index in indices {
            let index = index?;
            vortex_ensure!(
                seen < count && index >= 0 && index < i128::from(size),
                "sparse index is outside its declared dimension"
            );
            vortex_ensure!(
                !self.strictly_increasing || previous.is_none_or(|prior| prior < index),
                "sparse indices must be strictly increasing"
            );
            previous = Some(index);
            seen += 1;
        }
        vortex_ensure!(
            seen == count,
            "sparse index count changed during validation"
        );
        Ok(())
    }

    pub(crate) fn parameter_count(&self, codes: usize) -> VortexResult<usize> {
        vortex_ensure!(self.kind == Kind::Quantized, "expected quantized metadata");
        let codes = u64::try_from(codes).map_err(|e| vortex_err!("{e}"))?;
        self.validate_size(codes)?;
        let count = self
            .block_size
            .map_or(1, |block| codes / block + u64::from(codes % block != 0));
        usize::try_from(count).map_err(|e| vortex_err!("{e}"))
    }

    pub(crate) fn validate_scale(scale: Option<f64>) -> VortexResult<()> {
        vortex_ensure!(
            scale.is_some_and(|value| value.is_finite() && value > 0.0),
            "quantized scales must be nonnull, finite and positive"
        );
        Ok(())
    }

    pub(crate) fn validate_json(&self, value: &serde_json::Value) -> VortexResult<()> {
        let list = |name: &str| {
            value
                .get(name)
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| vortex_err!("present vector requires {name} list"))
        };
        match self.kind {
            Kind::Sparse => {
                let size = value
                    .get("size")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| vortex_err!("sparse size requires nonnull UInt64"))?;
                let indices = list("indices")?;
                self.validate_sparse(
                    size,
                    indices.len(),
                    list("values")?.len(),
                    indices.iter().map(|index| {
                        index
                            .as_i64()
                            .map(i128::from)
                            .or_else(|| index.as_u64().map(i128::from))
                            .ok_or_else(|| {
                                vortex_err!("sparse coordinates require nonnull integers")
                            })
                    }),
                )?;
            }
            Kind::Quantized => {
                let expected = self.parameter_count(list("codes")?.len())?;
                if self.block_size.is_some() {
                    let scales = list("scale")?;
                    let zeros = list("zero_point")?;
                    vortex_ensure!(
                        scales.len() == expected && zeros.len() == expected,
                        "quantized parameter counts differ from code blocks"
                    );
                    for (scale, zero) in scales.iter().zip(zeros) {
                        Self::validate_scale(scale.as_f64())?;
                        vortex_ensure!(!zero.is_null(), "quantized zero points must be nonnull");
                    }
                } else {
                    Self::validate_scale(value.get("scale").and_then(serde_json::Value::as_f64))?;
                    vortex_ensure!(
                        value.get("zero_point").is_some_and(|zero| !zero.is_null()),
                        "quantized zero points must be nonnull"
                    );
                }
            }
        }
        Ok(())
    }
}

struct Fields(Kind);
impl<'de> Visitor<'de> for Fields {
    type Value = serde_json::Map<String, serde_json::Value>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("versioned vector interpretation metadata")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut fields = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if fields.contains_key(&key) {
                return Err(A::Error::custom("duplicate vector metadata field"));
            }
            let value = match key.as_str() {
                "version" => {
                    if map.next_value::<u64>()? != 1 {
                        return Err(A::Error::custom("vector metadata version must be 1"));
                    }
                    serde_json::Value::from(1)
                }
                "uniform_size" => serde_json::Value::from(map.next_value::<u64>()?),
                "block_size" if self.0 == Kind::Quantized => {
                    serde_json::Value::from(map.next_value::<u64>()?)
                }
                "index_policy" if self.0 == Kind::Sparse => {
                    serde_json::Value::from(map.next_value::<String>()?)
                }
                "parameter_mode" if self.0 == Kind::Quantized => {
                    serde_json::Value::from(map.next_value::<String>()?)
                }
                _ if crate::native_embedding::IDENTITY_FIELDS.contains(&key.as_str()) => {
                    let value = map.next_value::<Option<String>>()?;
                    if value.as_ref().is_some_and(String::is_empty) {
                        return Err(A::Error::custom(
                            "embedding identity fields require nonempty strings or null",
                        ));
                    }
                    value.map_or(serde_json::Value::Null, serde_json::Value::String)
                }
                _ => return Err(A::Error::custom("unknown vector metadata field")),
            };
            fields.insert(key, value);
        }
        if !fields.contains_key("version") {
            return Err(A::Error::custom("vector metadata version must be 1"));
        }
        Ok(fields)
    }
}
