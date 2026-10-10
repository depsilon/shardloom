//! Inert source declarations normalize into the same admitted native memory owner.

use shardloom_core::ShardLoomError;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MemoryValueType {
    Int64,
    Float64,
    Bool,
    Utf8,
    Native { encoding: String, dtype: String },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub(crate) struct MemoryRow(pub(crate) Vec<Option<String>>);

pub(crate) fn bounded_vec<'de, D, T, const MAX: usize>(deserializer: D) -> Result<Vec<T>, D::Error>
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
            struct Reject;
            impl<'de> serde::de::DeserializeSeed<'de> for Reject {
                type Value = ();
                fn deserialize<D: serde::Deserializer<'de>>(self, _: D) -> Result<(), D::Error> {
                    Err(serde::de::Error::custom(
                        "native memory array exceeds its entry bound",
                    ))
                }
            }
            let mut values = Vec::new();
            while values.len() < MAX {
                let Some(value) = sequence.next_element()? else {
                    return Ok(values);
                };
                values.push(value);
            }
            // Reject the first excess entry before its payload is deserialized.
            sequence.next_element_seed(Reject)?;
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
        schema: Vec<(String, MemoryValueType)>,
        #[serde(deserialize_with = "bounded_vec::<_, _, 65_536>")]
        rows: Vec<MemoryRow>,
    },
    Batches {
        schema: Vec<(String, MemoryValueType)>,
        #[serde(default)]
        streaming: bool,
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
            Self::Batches { schema, .. } => validate_batch_rows(schema, &[]),
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
            Self::Batches { .. } => Err(failed("batch input requires the bounded batch transport")),
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
    validate_rows_with_limits(schema, rows, 65_536)
}

pub(crate) fn validate_batch_rows(
    schema: &[(String, MemoryValueType)],
    rows: &[MemoryRow],
) -> Result<(), ShardLoomError> {
    validate_rows_with_limits(schema, rows, 2048)
}

fn validate_rows_with_limits(
    schema: &[(String, MemoryValueType)],
    rows: &[MemoryRow],
    max_rows: usize,
) -> Result<(), ShardLoomError> {
    if schema.is_empty() || rows.len() > max_rows {
        return Err(failed(&format!(
            "rows require a declared schema and at most {max_rows} rows"
        )));
    }
    let mut bytes = 0usize;
    // Declarations are separately bounded by the 8-MiB control envelope. The
    // native source performs grant-backed uniqueness validation before intake;
    // this inert declaration check uses borrowed names only.
    let mut names = std::collections::BTreeSet::new();
    for (name, kind) in schema {
        if name.is_empty() || name.len() > 256 || !names.insert(name) {
            return Err(failed(
                "row field names must be distinct and contain 1..=256 UTF8 bytes",
            ));
        }
        bytes = bytes
            .checked_add(name.len())
            .filter(|bytes| *bytes <= 8 * 1024 * 1024)
            .ok_or_else(|| failed("row schema exceeds 8 MiB"))?;
        if let MemoryValueType::Native { encoding, dtype } = kind {
            if encoding != "vortex.dtype.serde.v1" {
                return Err(failed("unsupported native input schema encoding"));
            }
            bytes = bytes
                .checked_add(encoding.len())
                .and_then(|bytes| bytes.checked_add(dtype.len()))
                .filter(|bytes| *bytes <= 8 * 1024 * 1024)
                .ok_or_else(|| failed("row schema exceeds 8 MiB"))?;
        }
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
    usize::try_from(rows).map_err(|_| failed("range row count exceeds the platform index capacity"))
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
    fn rich_memory_schema_round_trips_without_changing_legacy_wire_tokens() {
        let schema = json!([
            ["legacy", "int64"],
            ["exact", {"native":{"encoding":"vortex.dtype.serde.v1",
                "dtype":"{\"Primitive\":[\"u64\",false]}"}}],
        ]);
        for declaration in [
            json!({"kind":"rows","schema":schema,"rows":[["1","18446744073709551615"]]}),
            json!({"kind":"batches","schema":schema,"streaming":true}),
        ] {
            let input: MemoryInput = serde_json::from_value(declaration.clone()).unwrap();
            input.validate().unwrap();
            assert_eq!(serde_json::to_value(input).unwrap(), declaration);
        }
    }

    #[test]
    fn rich_memory_schema_rejects_unknown_transport_fields_and_encoding() {
        for kind in [
            json!({"native":{"encoding":"vortex.dtype.serde.v1","dtype":"{}","extra":true}}),
            json!({"native":{"encoding":"vortex.dtype.serde.v1"}}),
            json!({"native":{"encoding":"vortex.dtype.serde.v1","dtype":{}}}),
            json!({"native":{"encoding":"vortex.dtype.serde.v1","dtype":"{}"},"bool":true}),
        ] {
            assert!(
                serde_json::from_value::<MemoryInput>(json!({
                    "kind":"rows","schema":[["v",kind]],"rows":[],
                }))
                .is_err()
            );
        }
        for (encoding, dtype) in [
            ("vortex.dtype.serde.v2", "{}".into()),
            ("vortex.dtype.serde.v1", "x".repeat(8 * 1024 * 1024)),
        ] {
            let input = MemoryInput::Batches {
                schema: vec![(
                    "v".into(),
                    MemoryValueType::Native {
                        encoding: encoding.into(),
                        dtype,
                    },
                )],
                streaming: true,
            };
            assert!(input.validate().is_err());
        }
    }

    #[test]
    fn generated_range_length_has_no_fixed_total_and_preserves_signed_endpoints() {
        assert_eq!(range_len(0, 1_000_017, 1, false).unwrap(), 1_000_017);
        assert_eq!(range_len(1_000_016, 0, -1, true).unwrap(), 1_000_017);
        assert_eq!(range_len(i64::MIN, i64::MAX, i64::MAX, true).unwrap(), 3);
        assert_eq!(range_len(i64::MAX, i64::MIN, i64::MIN, true).unwrap(), 2);
        assert_eq!(range_len(7, 7, 1, false).unwrap(), 0);
        assert_eq!(range_len(7, 7, -1, true).unwrap(), 1);
        assert_eq!(range_len(0, 1, -1, true).unwrap(), 0);
        assert!(range_len(0, 1, 0, false).is_err());
        assert!(range_len(i64::MIN, i64::MAX, 1, true).is_err());
    }

    #[test]
    fn batch_streaming_mode_is_explicit_and_defaults_to_resident() {
        for streaming in [None, Some(false), Some(true)] {
            let mut declaration = json!({"kind":"batches","schema":[["n","int64"]]});
            if let Some(streaming) = streaming {
                declaration["streaming"] = json!(streaming);
            }
            let input: MemoryInput = serde_json::from_value(declaration).unwrap();
            input.validate().unwrap();
            assert!(
                matches!(input, MemoryInput::Batches { streaming: actual, .. } if actual == streaming.unwrap_or(false))
            );
        }
        for value in [json!(null), json!(1), json!("true")] {
            assert!(
                serde_json::from_value::<MemoryInput>(json!({
                    "kind":"batches","schema":[["n","int64"]],"streaming":value,
                }))
                .is_err()
            );
        }
    }

    #[test]
    fn row_declarations_bound_shape_during_deserialization() {
        for declaration in [
            json!({"kind":"rows", "schema":[["n", "int64"]], "rows":vec![vec![None::<String>]; 65_537]}),
            json!({"kind":"rows", "schema":"n:int64", "rows":"n=1"}),
        ] {
            assert!(serde_json::from_value::<MemoryInput>(declaration).is_err());
        }
    }

    #[test]
    fn wide_declarations_validate_schema_and_exact_row_shape() {
        let schema = (0..1025)
            .map(|index| (format!("c{index}"), "int64"))
            .collect::<Vec<_>>();
        for declaration in [
            json!({"kind":"rows", "schema":schema, "rows":[vec![None::<String>;1025]]}),
            json!({"kind":"batches", "schema":schema, "streaming":true}),
        ] {
            serde_json::from_value::<MemoryInput>(declaration)
                .unwrap()
                .validate()
                .unwrap();
        }
        for declaration in [
            json!({"kind":"rows", "schema":vec![("n", "int64"); 1025], "rows":[]}),
            json!({"kind":"rows", "schema":schema, "rows":[vec![None::<String>;1024]]}),
        ] {
            assert!(
                serde_json::from_value::<MemoryInput>(declaration)
                    .unwrap()
                    .validate()
                    .is_err()
            );
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
