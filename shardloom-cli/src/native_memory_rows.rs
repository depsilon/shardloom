// SPDX-License-Identifier: Apache-2.0
//! Typed input conversion only; execution and output use the shared native engine.

use crate::native_memory_input::{MemoryRow, MemoryValueType, validate_batch_rows, validate_rows};
use shardloom_core::ShardLoomError;
use shardloom_exec::live_memory::MemoryLease;
use shardloom_vortex::{
    resident_memory_source::{
        MemoryBatchSourceBuilder, MemoryColumn, MemoryColumnValues, MemorySourceBounds,
        ResidentMemorySource,
    },
    resident_session::ResidentVortexSession,
};

pub(crate) fn build(
    schema: &[(String, MemoryValueType)],
    rows: &[MemoryRow],
    session: &ResidentVortexSession,
) -> Result<ResidentMemorySource, ShardLoomError> {
    validate_rows(schema, rows)?;
    let _scratch = session.reserve_input_scratch(conversion_bytes(schema.len(), rows.len())?)?;
    let typed = schema
        .iter()
        .enumerate()
        .map(|(index, (_, kind))| TypedColumn::from_rows(*kind, index, rows))
        .collect::<Result<Vec<_>, _>>()?;
    let columns = schema
        .iter()
        .zip(&typed)
        .map(|((name, _), values)| MemoryColumn {
            name,
            values: values.borrowed(),
        })
        .collect::<Vec<_>>();
    ResidentMemorySource::from_columns(session, &columns, MemorySourceBounds::default())
}

pub(crate) fn append_batch(
    schema: &[(String, MemoryValueType)],
    rows: &[MemoryRow],
    builder: &mut MemoryBatchSourceBuilder,
) -> Result<(), ShardLoomError> {
    let scratch = builder.reserve_scratch(conversion_bytes(schema.len(), rows.len())?)?;
    append_admitted_batch(schema, rows, builder, &scratch)
}

pub(crate) fn append_admitted_batch(
    schema: &[(String, MemoryValueType)],
    rows: &[MemoryRow],
    builder: &mut MemoryBatchSourceBuilder,
    scratch: &MemoryLease,
) -> Result<(), ShardLoomError> {
    with_batch_columns(schema, rows, scratch, |columns| {
        builder.push_columns(columns)
    })
}

pub(crate) fn build_batch(
    schema: &[(String, MemoryValueType)],
    rows: &[MemoryRow],
    session: &ResidentVortexSession,
) -> Result<ResidentMemorySource, ShardLoomError> {
    let scratch = session.reserve_input_scratch(conversion_bytes(schema.len(), rows.len())?)?;
    build_admitted_batch(schema, rows, session, &scratch)
}

pub(crate) fn build_admitted_batch(
    schema: &[(String, MemoryValueType)],
    rows: &[MemoryRow],
    session: &ResidentVortexSession,
    scratch: &MemoryLease,
) -> Result<ResidentMemorySource, ShardLoomError> {
    with_batch_columns(schema, rows, scratch, |columns| {
        ResidentMemorySource::from_batch_columns(session, columns)
    })
}

/// Temporary typed columns and their descriptor vectors. Three capacities cover
/// geometric Vec growth with the old allocation still live; UTF8 payloads are
/// borrowed from the separately admitted input cells rather than copied here.
pub(crate) fn conversion_bytes(columns: usize, rows: usize) -> Result<u64, ShardLoomError> {
    let cell = std::mem::size_of::<Option<&str>>()
        .max(std::mem::size_of::<Option<i64>>())
        .max(std::mem::size_of::<Option<f64>>())
        .max(std::mem::size_of::<Option<bool>>());
    rows.checked_mul(cell)
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<TypedColumn<'_>>()))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<MemoryColumn<'_>>()))
        .and_then(|bytes| bytes.checked_mul(columns))
        .and_then(|bytes| bytes.checked_mul(3))
        .and_then(|bytes| bytes.checked_add(4096))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| adapter_error("typed input conversion size overflow"))
}

fn with_batch_columns<T>(
    schema: &[(String, MemoryValueType)],
    rows: &[MemoryRow],
    scratch: &MemoryLease,
    consume: impl FnOnce(&[MemoryColumn<'_>]) -> Result<T, ShardLoomError>,
) -> Result<T, ShardLoomError> {
    if scratch.bytes() < conversion_bytes(schema.len(), rows.len())? {
        return Err(adapter_error(
            "typed input conversion lacks its scratch reservation",
        ));
    }
    validate_batch_rows(schema, rows)?;
    let typed = schema
        .iter()
        .enumerate()
        .map(|(index, (_, kind))| TypedColumn::from_rows(*kind, index, rows))
        .collect::<Result<Vec<_>, _>>()?;
    let columns = schema
        .iter()
        .zip(&typed)
        .map(|((name, _), values)| MemoryColumn {
            name,
            values: values.borrowed(),
        })
        .collect::<Vec<_>>();
    consume(&columns)
}

enum TypedColumn<'a> {
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
    Utf8(Vec<Option<&'a str>>),
}

impl<'a> TypedColumn<'a> {
    fn from_rows(
        kind: MemoryValueType,
        index: usize,
        rows: &'a [MemoryRow],
    ) -> Result<Self, ShardLoomError> {
        let values = || rows.iter().map(|row| row.0[index].as_deref());
        match kind {
            MemoryValueType::Int64 => values()
                .map(|value| value.map(str::parse).transpose())
                .collect::<Result<_, _>>()
                .map(Self::Int64)
                .map_err(|_| adapter_error("invalid typed int64")),
            MemoryValueType::Float64 => values()
                .map(|value| value.map(str::parse).transpose())
                .collect::<Result<_, _>>()
                .map(Self::Float64)
                .map_err(|_| adapter_error("invalid typed float64")),
            MemoryValueType::Bool => values()
                .map(|value| value.map(str::parse).transpose())
                .collect::<Result<_, _>>()
                .map(Self::Bool)
                .map_err(|_| adapter_error("invalid typed bool")),
            MemoryValueType::Utf8 => Ok(Self::Utf8(values().collect())),
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
    fn invalid_rows_release_conversion_and_native_reservations() {
        let session = ResidentVortexSession::new(1024 * 1024, 1).unwrap();
        for (kind, values) in [
            (MemoryValueType::Int64, vec![Some("1".into()); 4096]),
            (
                MemoryValueType::Int64,
                vec![Some("9223372036854775808".into())],
            ),
            (MemoryValueType::Float64, vec![Some("NaN".into())]),
            (MemoryValueType::Bool, vec![Some("1".into())]),
        ] {
            assert!(build(&[("n".into(), kind)], &[MemoryRow(values)], &session).is_err());
            assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        }
    }

    #[test]
    fn native_rows_preserve_exact_values_nulls_and_typed_empty_input() {
        let session = ResidentVortexSession::new(2 * 1024 * 1024, 1).unwrap();
        let schema = vec![
            ("n".into(), MemoryValueType::Int64),
            ("text".into(), MemoryValueType::Utf8),
        ];
        let rows = vec![
            MemoryRow(vec![Some(i64::MAX.to_string()), Some("λ\"\n,;%=".into())]),
            MemoryRow(vec![Some(i64::MIN.to_string()), Some("null".into())]),
            MemoryRow(vec![None, None]),
        ];
        let TypedColumn::Int64(ints) =
            TypedColumn::from_rows(MemoryValueType::Int64, 0, &rows).unwrap()
        else {
            panic!("int64 schema must create native int64 values");
        };
        assert_eq!(ints, vec![Some(i64::MAX), Some(i64::MIN), None]);
        let TypedColumn::Utf8(text) =
            TypedColumn::from_rows(MemoryValueType::Utf8, 1, &rows).unwrap()
        else {
            panic!("utf8 schema must create native utf8 values");
        };
        assert_eq!(text, vec![Some("λ\"\n,;%="), Some("null"), None]);
        let source = build(&schema, &rows, &session).unwrap();
        assert_eq!(source.row_count(), 3);
        let empty = build(&schema, &[], &session).unwrap();
        assert_eq!(empty.row_count(), 0);
        assert_eq!(
            empty.dtype().as_struct_fields_opt().unwrap().names().len(),
            2
        );
    }
}
