//! Explicit text parsing kernels over native UTF8 columns.

use super::{MemoryLease, Result, Value, failed, growth, reserve, typed, vortex_error};
use serde_json::value::RawValue;
use std::borrow::Cow;
use std::collections::BTreeMap;

pub(in crate::local_primitives) enum JsonStep {
    Field(String),
    Index(usize),
}

pub(in crate::local_primitives) struct JsonPath(Vec<JsonStep>);

impl JsonPath {
    pub(in crate::local_primitives) fn parse(path: &str) -> Result<Self> {
        if path.len() > 16_384 {
            return Err(failed("JSON path exceeds 16384 bytes"));
        }
        let mut tail = path
            .strip_prefix('$')
            .ok_or_else(|| failed("JSON path must begin with $"))?;
        let mut steps = Vec::new();
        while !tail.is_empty() {
            if steps.len() >= 128 {
                return Err(failed("JSON path exceeds 128 steps"));
            }
            if let Some(field) = tail.strip_prefix('.') {
                let end = field.find(['.', '[']).unwrap_or(field.len());
                let name = &field[..end];
                if name.is_empty()
                    || !name
                        .chars()
                        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
                {
                    return Err(failed(
                        "JSON paths admit $.identifier fields and nonnegative [index] steps",
                    ));
                }
                steps.push(JsonStep::Field(name.to_owned()));
                tail = &field[end..];
            } else if let Some(index) = tail.strip_prefix('[') {
                let close = index
                    .find(']')
                    .ok_or_else(|| failed("JSON array index requires a closing bracket"))?;
                let digits = &index[..close];
                if digits.is_empty() || !digits.bytes().all(|ch| ch.is_ascii_digit()) {
                    return Err(failed("JSON array index must be a nonnegative integer"));
                }
                steps.push(JsonStep::Index(digits.parse().map_err(vortex_error)?));
                tail = &index[close + 1..];
            } else {
                return Err(failed("JSON paths admit only field and array-index steps"));
            }
        }
        Ok(Self(steps))
    }

    pub(in crate::local_primitives) fn extract(
        &self,
        text: &str,
        scratch: &mut MemoryLease,
    ) -> Result<Value<'static>> {
        if text.len() > 1024 * 1024 {
            return Err(failed("JSON scalar input exceeds 1 MiB"));
        }
        // Reserve container indexes, keys and output before parsing. Values stay
        // borrowed JSON slices, so numbers never round through a float domain.
        let capacity = growth(text.len(), 256)?;
        reserve(scratch, capacity)?;
        let mut selected: &RawValue = serde_json::from_str(text).map_err(vortex_error)?;
        for step in &self.0 {
            let next = match step {
                JsonStep::Field(field) if selected.get().starts_with('{') => {
                    serde_json::from_str::<BTreeMap<String, &RawValue>>(selected.get())
                        .map_err(vortex_error)?
                        .get(field)
                        .copied()
                }
                JsonStep::Index(index) if selected.get().starts_with('[') => {
                    serde_json::from_str::<Vec<&RawValue>>(selected.get())
                        .map_err(vortex_error)?
                        .get(*index)
                        .copied()
                }
                _ => None,
            };
            let Some(next) = next else {
                return Ok(Value::Null);
            };
            selected = next;
        }
        let result = selected.get().to_owned();
        if result.capacity() > capacity {
            return Err(failed("JSON output exceeded its admitted scratch capacity"));
        }
        Ok(Value::Text(Cow::Owned(result)))
    }
}

pub(in crate::local_primitives) enum TimestampFormat {
    IsoUtc,
    DateTime,
    Date,
}

impl TimestampFormat {
    pub(in crate::local_primitives) fn parse(format: &str) -> Result<Self> {
        match format {
            "%Y-%m-%dT%H:%M:%SZ" => Ok(Self::IsoUtc),
            "%Y-%m-%d %H:%M:%S" => Ok(Self::DateTime),
            "%Y-%m-%d" => Ok(Self::Date),
            _ => Err(failed(
                "timestamp format requires %Y-%m-%dT%H:%M:%SZ, %Y-%m-%d %H:%M:%S or %Y-%m-%d; timezone database and other directives are not admitted",
            )),
        }
    }

    pub(in crate::local_primitives) fn timestamp(&self, value: &str) -> Result<i64> {
        if !value.is_ascii() {
            return Err(failed("timestamp input must be ASCII"));
        }
        match self {
            Self::IsoUtc if value.len() == 20 && value.ends_with('Z') => {
                typed::parse_iso_timestamp_micros(value)
            }
            Self::DateTime if value.len() == 19 && value.as_bytes()[10] == b' ' => {
                let mut bytes = [b'Z'; 20];
                bytes[..19].copy_from_slice(value.as_bytes());
                bytes[10] = b'T';
                typed::parse_iso_timestamp_micros(
                    std::str::from_utf8(&bytes).map_err(vortex_error)?,
                )
            }
            Self::Date if value.len() == 10 => {
                typed::parse_iso_date32(value).and_then(typed::date32_timestamp_micros)
            }
            _ => Err(failed("timestamp input does not match its declared format")),
        }
    }
}
