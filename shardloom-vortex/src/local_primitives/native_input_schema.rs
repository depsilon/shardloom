//! Restricted pinned-DType parsing before native input construction.

use serde::de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize as _, Deserializer as _};
use serde_json::{Map, Value, value::RawValue};
use shardloom_core::Result;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use vortex::array::{
    dtype::{DType, DecimalDType, FieldNames, Nullability, PType, StructFields},
    extension::datetime::{Date, TimeUnit, Timestamp},
};

use super::{native_typed_input::failed, vortex_error};

pub(super) const MAX_JSON_BYTES: usize = 8 * 1024 * 1024;

/// Covers decoded JSON nodes, map/container growth, strings and conversion
/// overlap. This is conservative workspace admission, not process RSS accounting.
pub(super) fn reserve_json(memory: &LiveMemoryPool, bytes: usize) -> Result<MemoryLease> {
    if bytes > MAX_JSON_BYTES {
        return Err(failed("typed input JSON exceeds 8 MiB"));
    }
    let bytes = bytes
        .checked_mul(128)
        .and_then(|bytes| bytes.checked_add(4096))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| failed("typed input JSON workspace overflow"))?;
    memory.reserve(bytes)
}

pub(super) fn parse_dtype(raw: &str, memory: &LiveMemoryPool) -> Result<(DType, MemoryLease)> {
    let _workspace = reserve_json(memory, raw.len())?;
    let value = parse_json(raw)?;
    let dtype = dtype(&value, 0, &mut 0)?;
    let credit = memory.reserve(crate::native_payload_schema::metadata_bytes(&dtype)?)?;
    Ok((dtype, credit))
}

/// Call only while the corresponding `reserve_json` credit remains live.
pub(super) fn parse_json(raw: &str) -> Result<Value> {
    let mut parser = serde_json::Deserializer::from_str(raw);
    let value = JsonSeed(0)
        .deserialize(&mut parser)
        .map_err(|error| failed(&error.to_string()))?;
    parser.end().map_err(|error| failed(&error.to_string()))?;
    Ok(value)
}

struct JsonSeed(usize);

impl<'de> DeserializeSeed<'de> for JsonSeed {
    type Value = Value;

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        parser: D,
    ) -> std::result::Result<Value, D::Error> {
        if self.0 > 96 {
            return Err(D::Error::custom(
                "typed input JSON nesting exceeds 96 levels",
            ));
        }
        // RawValue validates JSON syntax while retaining the exact numeric
        // lexeme. Parsing a generic JSON Number first can round integer
        // overflow and, without float_roundtrip, change finite float bits.
        let raw = <&RawValue>::deserialize(parser)?;
        let raw = raw.get();
        match raw.as_bytes().first() {
            Some(b'{' | b'[') => serde_json::Deserializer::from_str(raw)
                .deserialize_any(self)
                .map_err(D::Error::custom),
            Some(b'-' | b'0'..=b'9') => number(raw).map_err(D::Error::custom),
            _ => serde_json::from_str(raw).map_err(D::Error::custom),
        }
    }
}

fn number(raw: &str) -> Result<Value> {
    if raw == "-0" || raw.contains(['.', 'e', 'E']) {
        let value = raw
            .parse::<f64>()
            .map_err(|error| failed(&error.to_string()))?;
        return serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| failed("typed input numbers must be finite"));
    }
    // Exact integer tokens have the same int64/uint64 domain as the admitted
    // primitive types. Wider floating values use a decimal/exponent literal.
    if raw.starts_with('-') {
        raw.parse::<i64>().map(Value::from)
    } else {
        raw.parse::<u64>().map(Value::from)
    }
    .map_err(|_| failed("typed input integer JSON exceeds its exact 64-bit domain"))
}

impl<'de> Visitor<'de> for JsonSeed {
    type Value = Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("finite JSON without duplicate object fields")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<Value, A::Error> {
        let mut result = Vec::new();
        while let Some(value) = sequence.next_element_seed(JsonSeed(self.0 + 1))? {
            result.push(value);
        }
        Ok(Value::Array(result))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Value, A::Error> {
        let mut result = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if result.contains_key(&key) {
                return Err(A::Error::custom("duplicate typed input JSON field"));
            }
            result.insert(key, map.next_value_seed(JsonSeed(self.0 + 1))?);
        }
        Ok(Value::Object(result))
    }
}

fn fields<'a>(value: &'a Value, keys: &[&str]) -> Result<&'a Map<String, Value>> {
    let fields = value
        .as_object()
        .ok_or_else(|| failed("invalid native dtype object"))?;
    if fields.len() != keys.len() || keys.iter().any(|key| !fields.contains_key(*key)) {
        return Err(failed("native dtype has unknown or missing fields"));
    }
    Ok(fields)
}

fn dtype(value: &Value, depth: usize, nodes: &mut usize) -> Result<DType> {
    *nodes += 1;
    if depth > 24 || *nodes > 4096 {
        return Err(failed("nested input dtype exceeds depth 24 or 4096 nodes"));
    }
    let object = value
        .as_object()
        .filter(|value| value.len() == 1)
        .ok_or_else(|| failed("native input dtype requires one variant"))?;
    let (kind, value) = object.iter().next().expect("one dtype variant");
    if matches!(kind.as_str(), "Bool" | "Utf8" | "Binary") {
        let nullable = nullability(value)?;
        return Ok(match kind.as_str() {
            "Bool" => DType::Bool(nullable),
            "Utf8" => DType::Utf8(nullable),
            _ => DType::Binary(nullable),
        });
    }
    if kind == "Extension" {
        return temporal_dtype(value);
    }
    let size = if kind == "FixedSizeList" { 3 } else { 2 };
    let parts = value
        .as_array()
        .filter(|parts| parts.len() == size)
        .ok_or_else(|| failed("invalid native input dtype parameters"))?;
    let nullable = nullability(&parts[size - 1])?;
    Ok(match kind.as_str() {
        "Primitive" => DType::Primitive(
            match parts[0].as_str() {
                Some("i8") => PType::I8,
                Some("i16") => PType::I16,
                Some("i32") => PType::I32,
                Some("i64") => PType::I64,
                Some("u8") => PType::U8,
                Some("u16") => PType::U16,
                Some("u32") => PType::U32,
                Some("u64") => PType::U64,
                Some("f32") => PType::F32,
                Some("f64") => PType::F64,
                _ => return Err(failed("unsupported native input primitive")),
            },
            nullable,
        ),
        "Decimal" => decimal_dtype(&parts[0], nullable)?,
        "List" => DType::List(dtype(&parts[0], depth + 1, nodes)?.into(), nullable),
        "FixedSizeList" => {
            let size = parts[1]
                .as_u64()
                .and_then(|size| u32::try_from(size).ok())
                .ok_or_else(|| failed("fixed-size input width must be uint32"))?;
            DType::FixedSizeList(dtype(&parts[0], depth + 1, nodes)?.into(), size, nullable)
        }
        "Struct" => {
            let fields = fields(&parts[0], &["names", "dtypes"])?;
            let names = fields["names"]
                .as_array()
                .filter(|names| (1..=1024).contains(&names.len()))
                .ok_or_else(|| failed("nested input structs require 1..=1024 fields"))?;
            let types = fields["dtypes"]
                .as_array()
                .filter(|types| types.len() == names.len())
                .ok_or_else(|| failed("nested input names and dtypes must have equal length"))?;
            let mut distinct = std::collections::BTreeSet::new();
            let names = names
                .iter()
                .map(|name| {
                    let name = name
                        .as_str()
                        .filter(|name| !name.is_empty())
                        .ok_or_else(|| {
                            failed("nested input field names must be nonempty strings")
                        })?;
                    if !distinct.insert(name) {
                        return Err(failed("nested input field names must be distinct"));
                    }
                    Ok(name)
                })
                .collect::<Result<Vec<_>>>()?;
            let types = types
                .iter()
                .map(|value| dtype(value, depth + 1, nodes))
                .collect::<Result<Vec<_>>>()?;
            DType::Struct(StructFields::new(FieldNames::from(names), types), nullable)
        }
        _ => return Err(failed("unsupported native input dtype")),
    })
}

fn decimal_dtype(value: &Value, nullable: Nullability) -> Result<DType> {
    let fields = fields(value, &["precision", "scale"])?;
    let precision = fields["precision"]
        .as_u64()
        .filter(|n| (1..=38).contains(n))
        .ok_or_else(|| failed("decimal input precision must be 1..=38"))?;
    let scale = fields["scale"]
        .as_u64()
        .filter(|n| *n <= precision)
        .ok_or_else(|| failed("decimal input scale must be 0..=precision"))?;
    Ok(DType::Decimal(
        DecimalDType::new(
            u8::try_from(precision).map_err(vortex_error)?,
            i8::try_from(scale).map_err(vortex_error)?,
        ),
        nullable,
    ))
}

fn temporal_dtype(value: &Value) -> Result<DType> {
    let extension = fields(value, &["id", "metadata", "storage_dtype"])?;
    let storage = fields(&extension["storage_dtype"], &["Primitive"])?;
    let storage = storage["Primitive"]
        .as_array()
        .filter(|parts| parts.len() == 2)
        .ok_or_else(|| failed("temporal input requires primitive storage"))?;
    let nullable = nullability(&storage[1])?;
    match (extension["id"].as_str(), storage[0].as_str()) {
        (Some("vortex.date"), Some("i32")) if extension["metadata"] == serde_json::json!([4]) => {
            Ok(DType::Extension(
                Date::new(TimeUnit::Days, nullable).erased(),
            ))
        }
        (Some("vortex.timestamp"), Some("i64"))
            if extension["metadata"] == serde_json::json!([1, 0, 0]) =>
        {
            Ok(DType::Extension(
                Timestamp::new(TimeUnit::Microseconds, nullable).erased(),
            ))
        }
        _ => Err(failed(
            "unsupported native input extension metadata or storage",
        )),
    }
}

fn nullability(value: &Value) -> Result<Nullability> {
    value
        .as_bool()
        .map(Nullability::from)
        .ok_or_else(|| failed("native input nullability must be a bool"))
}
