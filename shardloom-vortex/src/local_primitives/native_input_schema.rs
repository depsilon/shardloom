//! Restricted pinned-DType parsing before native input construction.

use serde_json::{Map, Value};
use shardloom_core::Result;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use vortex::array::{
    dtype::{
        DType, DecimalDType, FieldNames, Nullability, PType, StructFields,
        extension::{ExtDType, ExtVTable},
    },
    extension::datetime::{Date, Time, Timestamp},
};

use super::{native_typed_input::failed, vortex_error};

#[path = "native_input_json.rs"]
mod json;
pub(super) use json::{ParsedJson, parse as parse_json};
#[path = "native_input_dtype.rs"]
mod dtype;

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

pub(crate) fn parse_dtype(raw: &str, memory: &LiveMemoryPool) -> Result<(DType, MemoryLease)> {
    let _workspace = reserve_json(memory, raw.len())?;
    let value = parse_json(raw, memory)?;
    let dtype = dtype::convert(value.value(), memory)?;
    let credit = crate::native_payload_schema::reserve_metadata(&dtype, memory)?;
    Ok((dtype, credit))
}

/// Parse inert caller-owned application metadata before an operation exists.
/// This retains the ordinary depth and JSON byte bounds; query binding later
/// reserves the complete schema against its execution allocation. Top-level
/// record width follows shared schema admission rather than nested limits.
pub(crate) fn application_fields(fields: &[(String, String)]) -> Result<DType> {
    fields.iter().try_fold(0_usize, |bytes, (name, raw)| {
        bytes
            .checked_add(name.len())
            .and_then(|bytes| bytes.checked_add(raw.len()))
            .filter(|bytes| *bytes <= MAX_JSON_BYTES)
            .ok_or_else(|| failed("application schema exceeds 8 MiB"))
    })?;
    let memory = application_memory()?;
    let types = fields
        .iter()
        .map(|(_, raw)| {
            let _workspace = reserve_json(&memory, raw.len())?;
            let value = parse_json(raw, &memory)?;
            dtype::convert(value.value(), &memory)
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = DType::Struct(
        StructFields::new(
            FieldNames::from(
                fields
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
            ),
            types,
        ),
        Nullability::NonNullable,
    );
    validate_application_dtype(&schema)?;
    Ok(schema)
}

/// Inert caller metadata has no operation owner. Bound its validation workspace
/// by the existing JSON declaration contract, then count its exact wire bytes
/// without allocating another serialized schema. Binding still admits its own
/// persistent metadata against the caller's execution pool.
pub(crate) fn validate_application_dtype(dtype: &DType) -> Result<()> {
    struct WireSize(usize);
    impl std::io::Write for WireSize {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|bytes| *bytes <= MAX_JSON_BYTES)
                .ok_or_else(|| std::io::Error::other("application schema exceeds 8 MiB"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let validation = application_memory()?;
    let _workspace = crate::native_payload_schema::reserve_schema(dtype, &validation)?;
    crate::native_payload_schema::json::write(dtype, &validation, WireSize(0))
        .map_err(|error| error.into_error(|error| failed(&error.to_string())))
}

fn application_memory() -> Result<LiveMemoryPool> {
    let limit = (MAX_JSON_BYTES as u64)
        .checked_mul(512)
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or_else(|| failed("application schema workspace overflow"))?;
    LiveMemoryPool::new(limit)
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

fn fields<'a>(value: &'a Value, keys: &[&str]) -> Result<&'a Map<String, Value>> {
    let fields = value
        .as_object()
        .ok_or_else(|| failed("invalid native dtype object"))?;
    if fields.len() != keys.len() || keys.iter().any(|key| !fields.contains_key(*key)) {
        return Err(failed("native dtype has unknown or missing fields"));
    }
    Ok(fields)
}

#[cfg(feature = "native-binary-objects")]
fn binary_object_metadata(fields: &Map<String, Value>) -> Result<()> {
    let expected = b"{\"version\":1}";
    let raw = fields["metadata"]
        .as_array()
        .ok_or_else(|| failed("binary-object metadata requires UTF8 bytes"))?;
    if raw.len() != expected.len()
        || raw
            .iter()
            .zip(expected)
            .any(|(value, byte)| value.as_u64() != Some(u64::from(*byte)))
    {
        return Err(failed("invalid binary-object metadata"));
    }
    Ok(())
}

#[cfg(feature = "native-binary-objects")]
fn binary_object_dtype(storage: DType) -> Result<DType> {
    ExtDType::<crate::native_binary_type::BinaryObjectType>::try_new(
        crate::native_binary_type::Metadata,
        storage,
    )
    .map(|dtype| DType::Extension(dtype.erased()))
    .map_err(vortex_error)
}

fn embedding_dtype(extension: &Map<String, Value>, storage: DType) -> Result<DType> {
    let metadata = extension["metadata"]
        .as_array()
        .ok_or_else(|| failed("embedding metadata requires UTF8 bytes"))?
        .iter()
        .map(|byte| {
            byte.as_u64()
                .and_then(|n| u8::try_from(n).ok())
                .ok_or_else(|| failed("embedding metadata must contain bytes"))
        })
        .collect::<Result<Vec<_>>>()?;
    let metadata = crate::native_embedding::Embedding::default()
        .deserialize_metadata(&metadata)
        .map_err(vortex_error)?;
    Ok(DType::Extension(
        ExtDType::<crate::native_embedding::Embedding>::try_new(metadata, storage)
            .map_err(vortex_error)?
            .erased(),
    ))
}

fn tensor_dtype(extension: &Map<String, Value>, storage: DType) -> Result<DType> {
    let metadata = extension["metadata"]
        .as_array()
        .ok_or_else(|| failed("tensor metadata requires UTF8 bytes"))?
        .iter()
        .map(|byte| {
            byte.as_u64()
                .and_then(|n| u8::try_from(n).ok())
                .ok_or_else(|| failed("tensor metadata must contain bytes"))
        })
        .collect::<Result<Vec<_>>>()?;
    let metadata = crate::native_tensor::FixedShapeTensor::default()
        .deserialize_metadata(&metadata)
        .map_err(vortex_error)?;
    Ok(DType::Extension(
        ExtDType::<crate::native_tensor::FixedShapeTensor>::try_new(metadata, storage)
            .map_err(vortex_error)?
            .erased(),
    ))
}

fn variable_tensor_dtype(extension: &Map<String, Value>, storage: DType) -> Result<DType> {
    let metadata = extension["metadata"]
        .as_array()
        .ok_or_else(|| failed("tensor metadata requires UTF8 bytes"))?
        .iter()
        .map(|byte| {
            byte.as_u64()
                .and_then(|n| u8::try_from(n).ok())
                .ok_or_else(|| failed("tensor metadata must contain bytes"))
        })
        .collect::<Result<Vec<_>>>()?;
    let metadata = crate::native_variable_tensor::VariableShapeTensor::default()
        .deserialize_metadata(&metadata)
        .map_err(vortex_error)?;
    Ok(DType::Extension(
        ExtDType::<crate::native_variable_tensor::VariableShapeTensor>::try_new(metadata, storage)
            .map_err(vortex_error)?
            .erased(),
    ))
}

fn vector_dtype(
    kind: crate::native_vector::Kind,
    extension: &Map<String, Value>,
    storage: DType,
) -> Result<DType> {
    let bytes = extension["metadata"]
        .as_array()
        .ok_or_else(|| failed("vector metadata requires UTF8 bytes"))?
        .iter()
        .map(|value| {
            value
                .as_u64()
                .and_then(|n| u8::try_from(n).ok())
                .ok_or_else(|| failed("vector metadata must contain bytes"))
        })
        .collect::<Result<Vec<_>>>()?;
    let metadata = match kind {
        crate::native_vector::Kind::Sparse => {
            crate::native_vector::SparseVector::default().deserialize_metadata(&bytes)
        }
        crate::native_vector::Kind::Quantized => {
            crate::native_vector::QuantizedVector::default().deserialize_metadata(&bytes)
        }
    }
    .map_err(vortex_error)?;
    crate::native_vector::dtype(metadata, storage).map_err(vortex_error)
}

fn spatial_dtype(
    kind: crate::native_spatial::Kind,
    extension: &Map<String, Value>,
    storage: DType,
) -> Result<DType> {
    let bytes = extension["metadata"]
        .as_array()
        .ok_or_else(|| failed("spatial metadata requires UTF8 bytes"))?
        .iter()
        .map(|value| {
            value
                .as_u64()
                .and_then(|n| u8::try_from(n).ok())
                .ok_or_else(|| failed("spatial metadata must contain bytes"))
        })
        .collect::<Result<Vec<_>>>()?;
    let metadata = match kind {
        crate::native_spatial::Kind::Point => {
            crate::native_spatial::Point::default().deserialize_metadata(&bytes)
        }
        crate::native_spatial::Kind::LineString => {
            crate::native_spatial::LineString::default().deserialize_metadata(&bytes)
        }
        crate::native_spatial::Kind::Polygon => {
            crate::native_spatial::Polygon::default().deserialize_metadata(&bytes)
        }
    }
    .map_err(vortex_error)?;
    crate::native_spatial::dtype(metadata, storage).map_err(vortex_error)
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
    let storage = DType::Primitive(
        match storage[0].as_str() {
            Some("i32") => PType::I32,
            Some("i64") => PType::I64,
            _ => return Err(failed("temporal input requires signed native storage")),
        },
        nullable,
    );
    // Timestamp metadata is one unit byte, a u16 length and at most 255
    // timezone bytes. All other admitted temporal metadata has one byte.
    let metadata = extension["metadata"]
        .as_array()
        .filter(|bytes| !bytes.is_empty() && bytes.len() <= 258)
        .ok_or_else(|| failed("temporal metadata requires 1..=258 bytes"))?
        .iter()
        .map(|value| {
            value
                .as_u64()
                .and_then(|n| u8::try_from(n).ok())
                .ok_or_else(|| failed("temporal metadata must contain bytes"))
        })
        .collect::<Result<Vec<_>>>()?;
    macro_rules! decode {
        ($provider:expr, $ty:ty) => {{
            let decoded = $provider
                .deserialize_metadata(&metadata)
                .map_err(vortex_error)?;
            if $provider
                .serialize_metadata(&decoded)
                .map_err(vortex_error)?
                != metadata
            {
                return Err(failed(
                    "temporal metadata must use its exact canonical encoding",
                ));
            }
            DType::Extension(
                ExtDType::<$ty>::try_new(decoded, storage)
                    .map_err(vortex_error)?
                    .erased(),
            )
        }};
    }
    let dtype = match extension["id"].as_str() {
        Some("vortex.date") => decode!(Date, Date),
        Some("vortex.time") => decode!(Time, Time),
        Some("vortex.timestamp") => decode!(Timestamp, Timestamp),
        Some("shardloom.duration.v1") => decode!(
            crate::native_temporal::Duration,
            crate::native_temporal::Duration
        ),
        _ => return Err(failed("unsupported native input extension metadata")),
    };
    if crate::native_temporal::temporal_storage(&dtype).is_none() {
        return Err(failed(
            "unsupported native input temporal domain or timezone",
        ));
    }
    Ok(dtype)
}

fn nullability(value: &Value) -> Result<Nullability> {
    value
        .as_bool()
        .map(Nullability::from)
        .ok_or_else(|| failed("native input nullability must be a bool"))
}
