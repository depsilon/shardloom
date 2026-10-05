//! Inert source declarations normalize into the same admitted native memory owner.

use shardloom_core::ShardLoomError;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryValueType {
    Int64,
    Float64,
    Bool,
    Utf8,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(transparent)]
pub(crate) struct MemoryRow(pub(crate) Vec<Option<String>>);

impl<'de> serde::Deserialize<'de> for MemoryRow {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        bounded_vec::<_, _, 64>(deserializer).map(Self)
    }
}

fn bounded_vec<'de, D, T, const MAX: usize>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    struct Visitor<T, const MAX: usize>(std::marker::PhantomData<T>);
    impl<'de, T: serde::Deserialize<'de>, const MAX: usize> serde::de::Visitor<'de>
        for Visitor<T, MAX>
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "an array with at most {MAX} entries")
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let mut values = Vec::new();
            while let Some(value) = sequence.next_element()? {
                if values.len() == MAX {
                    return Err(serde::de::Error::custom(format!(
                        "native memory array exceeds {MAX} entries"
                    )));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Visitor::<T, MAX>(std::marker::PhantomData))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MemoryInput {
    #[cfg(any(test, all(feature = "vortex-local-primitives", unix)))]
    #[serde(skip_deserializing)]
    Unit,
    Rows {
        #[serde(deserialize_with = "bounded_vec::<_, _, 64>")]
        schema: Vec<(String, MemoryValueType)>,
        #[serde(deserialize_with = "bounded_vec::<_, _, 65_536>")]
        rows: Vec<MemoryRow>,
    },
    Range {
        start: i64,
        end: i64,
        step: i64,
        column: String,
        #[serde(default)]
        inclusive: bool,
    },
}

impl MemoryInput {
    pub(crate) fn validate(&self) -> Result<(), ShardLoomError> {
        match self {
            #[cfg(any(test, all(feature = "vortex-local-primitives", unix)))]
            Self::Unit => Ok(()),
            Self::Rows { schema, rows } => validate_rows(schema, rows),
            Self::Range {
                start,
                end,
                step,
                column,
                inclusive,
            } => {
                if column.is_empty() || column.len() > 256 {
                    return Err(failed("range field must contain 1..=256 bytes"));
                }
                range_len(*start, *end, *step, *inclusive).map(|_| ())
            }
        }
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn build(
        &self,
        session: &shardloom_vortex::resident_session::ResidentVortexSession,
    ) -> Result<shardloom_vortex::resident_memory_source::ResidentMemorySource, ShardLoomError>
    {
        self.validate()?;
        match self {
            Self::Unit => {
                shardloom_vortex::resident_memory_source::ResidentMemorySource::from_int64_range(
                    session,
                    "__shardloom_unit",
                    0,
                    1,
                    1,
                )
            }
            Self::Rows { schema, rows } => crate::native_memory_rows::build(schema, rows, session),
            Self::Range {
                start,
                end,
                step,
                column,
                inclusive,
            } => shardloom_vortex::resident_memory_source::ResidentMemorySource::from_int64_range(
                session,
                column,
                *start,
                *step,
                range_len(*start, *end, *step, *inclusive)?,
            ),
        }
    }
}

pub(crate) fn validate_rows(
    schema: &[(String, MemoryValueType)],
    rows: &[MemoryRow],
) -> Result<(), ShardLoomError> {
    if schema.is_empty() || schema.len() > 64 || rows.len() > 65_536 {
        return Err(failed("rows require 1..=64 fields and at most 65,536 rows"));
    }
    let mut bytes = 0usize;
    for (index, (name, _)) in schema.iter().enumerate() {
        if name.is_empty()
            || name.len() > 256
            || schema[..index].iter().any(|(prior, _)| prior == name)
        {
            return Err(failed(
                "row field names must be distinct and contain 1..=256 UTF8 bytes",
            ));
        }
        bytes += name.len();
    }
    for row in rows {
        if row.0.len() != schema.len() {
            return Err(failed("row field count must match the declared schema"));
        }
        for value in &row.0 {
            bytes = bytes.saturating_add(value.as_ref().map_or(1, String::len));
            if bytes > 8 * 1024 * 1024 {
                return Err(failed("row payload exceeds 8 MiB"));
            }
        }
    }
    Ok(())
}

fn range_len(start: i64, end: i64, step: i64, inclusive: bool) -> Result<usize, ShardLoomError> {
    if step == 0 {
        return Err(failed("range step must be nonzero"));
    }
    let distance = if step > 0 {
        i128::from(end) - i128::from(start)
    } else {
        i128::from(start) - i128::from(end)
    };
    let stride = i128::from(step).abs();
    let rows = if distance < 0 || (!inclusive && distance == 0) {
        0
    } else if inclusive {
        distance / stride + 1
    } else {
        (distance + stride - 1) / stride
    };
    usize::try_from(rows)
        .ok()
        .filter(|rows| *rows <= 1_000_000)
        .ok_or_else(|| failed("range exceeds one million input rows"))
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native memory input: {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn row_declarations_bound_shape_during_deserialization() {
        for declaration in [
            json!({"kind":"rows", "schema":vec![("n", "int64"); 65], "rows":[]}),
            json!({"kind":"rows", "schema":[["n", "int64"]], "rows":[vec![None::<String>; 65]]}),
            json!({"kind":"rows", "schema":[["n", "int64"]], "rows":vec![vec![None::<String>]; 65_537]}),
            json!({"kind":"rows", "schema":"n:int64", "rows":"n=1"}),
        ] {
            assert!(serde_json::from_value::<MemoryInput>(declaration).is_err());
        }
    }

    #[test]
    fn typed_empty_and_nullable_rows_validate_but_invalid_widths_do_not() {
        for rows in [json!([]), json!([[null], ["9223372036854775807"]])] {
            let input = serde_json::from_value::<MemoryInput>(json!({
                "kind":"rows", "schema":[["n", "int64"]], "rows":rows,
            }))
            .unwrap();
            input.validate().unwrap();
        }
        let input = serde_json::from_value::<MemoryInput>(json!({
            "kind":"rows", "schema":[["n", "int64"]], "rows":[[]],
        }))
        .unwrap();
        assert!(input.validate().is_err());
    }
}
