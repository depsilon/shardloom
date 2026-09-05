//! Explicit bounded generated-row adapter into the native resident memory source.
//! Parsing storage is a bounded compatibility boundary; the session allocator
//! owns the native value/offset/validity buffers and completed JSON capacity.

use super::{
    GeneratedRow, GeneratedValueType, ShardLoomError, UserRowsGeneratedSourceKind, parse_rows,
    parse_schema, validate_user_rows_source_kind_shape,
};
use shardloom_vortex::{
    local_primitives::collect::CollectedVortexRows,
    resident_memory_source::{
        MemoryColumn, MemoryColumnValues, MemorySourceBounds, ResidentMemorySource,
    },
    resident_session::ResidentVortexSession,
};

pub(crate) struct GeneratedMemoryCollect {
    pub collected: CollectedVortexRows,
    pub input_logical_bytes: usize,
}

/// Preserve the existing percent-encoded row/schema grammar. In particular,
/// UTF8 `null` remains a literal string; nullable typed intake is a separate Rust
/// API. No write path or prepared file participates in this completed collect.
pub(crate) fn collect_generated_rows_memory(
    source_kind: &str,
    schema_raw: &str,
    rows_raw: &str,
    session: &ResidentVortexSession,
) -> Result<GeneratedMemoryCollect, ShardLoomError> {
    if schema_raw.len().saturating_add(rows_raw.len()) > 8 * 1024 * 1024
        || schema_raw.split(',').count() > 64
        || rows_raw.split(';').count() > 65_536
    {
        return Err(adapter_error(
            "generated collect input exceeds 8 MiB, 64 columns, or 65,536 rows",
        ));
    }
    let kind = UserRowsGeneratedSourceKind::parse(source_kind)?;
    let schema = parse_schema(schema_raw)?;
    // Delimiters inside names/values are percent-escaped in this grammar.
    // Bound each raw row's field count before parse_rows builds owned maps;
    // otherwise one admitted byte frame could allocate millions of unknown keys.
    if rows_raw
        .split(';')
        .any(|row| row.split(',').take(schema.len() + 1).count() != schema.len())
    {
        return Err(adapter_error(
            "generated collect row field count must match the declared schema",
        ));
    }
    let rows = parse_rows(rows_raw, &schema)?;
    validate_user_rows_source_kind_shape(kind, &schema, &rows)?;
    let typed = schema
        .iter()
        .enumerate()
        .map(|(index, column)| TypedColumn::from_rows(column.value_type, index, &rows))
        .collect::<Result<Vec<_>, _>>()?;
    let columns = schema
        .iter()
        .zip(&typed)
        .map(|(column, values)| MemoryColumn {
            name: &column.name,
            values: values.borrowed(),
        })
        .collect::<Vec<_>>();
    let source =
        ResidentMemorySource::from_columns(session, &columns, MemorySourceBounds::default())?;
    let names = schema
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    let input_logical_bytes = source.input_logical_bytes();
    let mut collected = source.prepare_projection(&names, None, None)?.execute()?;
    drop(source);
    collected.runtime = session.snapshot();
    Ok(GeneratedMemoryCollect {
        collected,
        input_logical_bytes,
    })
}

enum TypedColumn<'a> {
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
    Utf8(Vec<Option<&'a str>>),
}

impl<'a> TypedColumn<'a> {
    fn from_rows(
        kind: GeneratedValueType,
        index: usize,
        rows: &'a [GeneratedRow],
    ) -> Result<Self, ShardLoomError> {
        let values = || rows.iter().map(|row| row.values[index].as_str());
        match kind {
            GeneratedValueType::Int64 => values()
                .map(|value| value.parse().map(Some))
                .collect::<Result<_, _>>()
                .map(Self::Int64)
                .map_err(|_| adapter_error("invalid typed int64")),
            GeneratedValueType::Float64 => values()
                .map(|value| value.parse().map(Some))
                .collect::<Result<_, _>>()
                .map(Self::Float64)
                .map_err(|_| adapter_error("invalid typed float64")),
            GeneratedValueType::Bool => values()
                .map(|value| value.parse().map(Some))
                .collect::<Result<_, _>>()
                .map(Self::Bool)
                .map_err(|_| adapter_error("invalid typed bool")),
            GeneratedValueType::Utf8 => Ok(Self::Utf8(values().map(Some).collect())),
        }
    }

    fn borrowed(&self) -> MemoryColumnValues<'_> {
        match self {
            Self::Int64(values) => MemoryColumnValues::Int64(values),
            Self::Float64(values) => MemoryColumnValues::Float64(values),
            Self::Bool(values) => MemoryColumnValues::Bool(values),
            Self::Utf8(values) => MemoryColumnValues::Utf8(values),
        }
    }
}

fn adapter_error(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{message}; no fallback execution was attempted"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_field_preflight_rejects_unknown_field_amplification_and_preserves_escapes() {
        let session = ResidentVortexSession::new(1024 * 1024, 1).unwrap();
        let unknown = format!(
            "id=1,{}",
            (0..4096)
                .map(|index| format!("unknown{index}=0"))
                .collect::<Vec<_>>()
                .join(",")
        );
        let error = collect_generated_rows_memory("user_rows", "id:int64", &unknown, &session)
            .err()
            .expect("unknown field amplification must be rejected");
        assert!(error.to_string().contains("field count must match"));
        assert_eq!(session.snapshot().memory.peak_reserved_bytes, 0);
        let result = collect_generated_rows_memory(
            "user_rows",
            "label%2Cname:utf8,id:int64",
            "label%2Cname=one%2Ctwo%3Bthree,id=1;label%2Cname=%3B%2C,id=2",
            &session,
        )
        .unwrap();
        let actual: serde_json::Value =
            serde_json::from_str(result.collected.values_json.value()).unwrap();
        assert_eq!(
            actual,
            serde_json::json!([
                {"label,name":"one,two;three","id":1}, {"label,name":";,","id":2}
            ])
        );
    }

    #[test]
    fn generated_collect_preserves_exact_values_without_opening_files() {
        let session = ResidentVortexSession::new(2 * 1024 * 1024, 1).unwrap();
        let result = collect_generated_rows_memory("user_rows", "n:int64,x:float64,b:bool,s:utf8",
            "n=9223372036854775807,x=1.25,b=true,s=%CE%BB%22%0A;n=-9223372036854775808,x=-0.0,b=false,s=null", &session).unwrap();
        let actual: serde_json::Value =
            serde_json::from_str(result.collected.values_json.value()).unwrap();
        assert_eq!(
            actual,
            serde_json::json!([
                {"n": i64::MAX, "x": 1.25, "b": true, "s": "λ\"\n"},
                {"n": i64::MIN, "x": -0.0, "b": false, "s": "null"}
            ])
        );
        assert!(result.input_logical_bytes > 0);
        assert_eq!(result.collected.runtime.prepared_source_opens, 0);
        assert_eq!(result.collected.runtime.completed_executions, 1);
        assert!(!result.collected.native_io_certificate.side_effects.write_io);
        assert!(
            !result
                .collected
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
    }

    #[test]
    fn generated_collect_rejects_invalid_input_and_budget_before_returning_values() {
        let session = ResidentVortexSession::new(1024 * 1024, 1).unwrap();
        for (kind, schema, rows) in [
            ("user_rows", "id:int64", "id=null"),
            ("user_rows", "x:float64", "x=NaN"),
            ("dataframe_projection", "x:int64", "x=1;x=2"),
            ("user_rows", "x:int64", "x=1,y=2"),
        ] {
            assert!(collect_generated_rows_memory(kind, schema, rows, &session).is_err());
            assert_eq!(session.snapshot().memory.peak_reserved_bytes, 0);
        }
        let oversized = "x:int64,".repeat(64) + "last:int64";
        assert!(collect_generated_rows_memory("user_rows", &oversized, "x=1", &session).is_err());
        let tight = ResidentVortexSession::new(8, 1).unwrap();
        assert!(collect_generated_rows_memory("user_rows", "x:int64", "x=1", &tight).is_err());
        assert_eq!(tight.snapshot().memory.reserved_bytes, 0);
    }
}
