//! Canonical GeoArrow metadata with bounded parser and retained ownership.

use super::{Kind, MAX_METADATA_BYTES};
use serde::{
    Deserialize, Deserializer,
    de::{Error as _, MapAccess, SeqAccess, Visitor},
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
    planar: bool,
    credit: Option<Arc<HostMemoryReservation>>,
}
impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.json == other.json && self.planar == other.planar
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
            "spatial metadata exceeds 8 MiB"
        );
        let Unique(value) =
            serde_json::from_str(raw).map_err(|e| vortex_err!("invalid spatial metadata: {e}"))?;
        let fields = value
            .as_object()
            .ok_or_else(|| vortex_err!("spatial metadata requires a JSON object"))?;
        for key in fields.keys() {
            vortex_ensure!(
                matches!(key.as_str(), "crs" | "crs_type" | "edges"),
                "unsupported spatial metadata field {key}"
            );
        }
        if let Some(crs) = fields.get("crs") {
            vortex_ensure!(
                crs.is_object() || crs.as_str().is_some_and(|s| !s.is_empty()),
                "spatial crs requires an object or nonempty string"
            );
        }
        if let Some(kind) = fields.get("crs_type") {
            let kind = kind
                .as_str()
                .ok_or_else(|| vortex_err!("spatial crs_type requires a string"))?;
            let crs = fields
                .get("crs")
                .ok_or_else(|| vortex_err!("spatial crs_type requires crs"))?;
            vortex_ensure!(
                match kind {
                    "projjson" => crs.is_object(),
                    "wkt2:2019" | "authority_code" | "srid" => crs.is_string(),
                    _ => false,
                },
                "unsupported spatial crs_type or mismatched crs representation"
            );
        }
        let planar = !fields.contains_key("edges");
        if let Some(edges) = fields.get("edges") {
            vortex_ensure!(
                matches!(
                    edges.as_str(),
                    Some("spherical" | "vincenty" | "thomas" | "andoyer" | "karney")
                ),
                "unsupported spatial edge declaration; omit edges for planar coordinates"
            );
        }
        Ok(Self {
            json: serde_json::to_string(&value)
                .map_err(|e| vortex_err!("{e}"))?
                .into(),
            kind,
            planar,
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
            "spatial metadata exceeds 8 MiB"
        );
        let workspace = raw
            .len()
            .checked_mul(128)
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(|| vortex_err!("spatial metadata workspace overflow"))?;
        let retained = raw
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(256))
            .ok_or_else(|| vortex_err!("spatial metadata retention overflow"))?;
        let _workspace = allocator
            .map(|a| a.reserve_external(workspace))
            .transpose()?;
        let credit = allocator
            .map(|a| a.reserve_external(retained))
            .transpose()?;
        let raw = std::str::from_utf8(raw).map_err(|e| vortex_err!("{e}"))?;
        let mut result = Self::parse(kind, raw)?;
        // JSON has several equivalent numeric spellings across language
        // boundaries. Accept valid unique-key metadata and write one canonical
        // native encoding; never require a Python producer to emulate Rust's
        // floating formatter for an opaque CRS document.
        result.credit = credit.map(Arc::new);
        Ok(result)
    }
    pub(crate) fn kind(&self) -> Kind {
        self.kind
    }
    pub(crate) fn as_str(&self) -> &str {
        &self.json
    }
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn is_planar(&self) -> bool {
        self.planar
    }
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn same_coordinates(&self, other: &Self) -> bool {
        self.json == other.json
    }
}

// Do not silently discard duplicate keys inside an opaque PROJJSON object.
struct Unique(serde_json::Value);
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(UniqueVisitor)
    }
}
struct UniqueVisitor;
impl<'de> Visitor<'de> for UniqueVisitor {
    type Value = Unique;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JSON with unique object keys")
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Unique, E> {
        Ok(Unique(value.into()))
    }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Unique, E> {
        Ok(Unique(value.into()))
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Unique, E> {
        Ok(Unique(value.into()))
    }
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Unique, E> {
        serde_json::Number::from_f64(value)
            .map(|n| Unique(n.into()))
            .ok_or_else(|| E::custom("nonfinite JSON number"))
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Unique, E> {
        Ok(Unique(value.into()))
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Unique, E> {
        Ok(Unique(value.into()))
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Unique, E> {
        Ok(Unique(serde_json::Value::Null))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Unique, A::Error> {
        let mut values = Vec::new();
        while let Some(Unique(value)) = sequence.next_element()? {
            values.push(value);
        }
        Ok(Unique(values.into()))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Unique, A::Error> {
        let mut fields = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if fields.contains_key(&key) {
                return Err(A::Error::custom("duplicate spatial metadata field"));
            }
            let Unique(value) = map.next_value()?;
            fields.insert(key, value);
        }
        Ok(Unique(fields.into()))
    }
}
