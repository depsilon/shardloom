//! Existing scalar syntax/coercion bound to incremental native source ownership.

use super::{
    BoundTextTemporalColumn, LocalSourceReadPlan, coerce_schema_declared_json_scalar,
    parse_csv_scalar, parse_flat_json_object_with_plan, parse_schema_declared_text_scalar,
    parse_vortex_ingest_schema_hints, schema_declared_text_arrow_dtype, split_delimited_record,
    strip_utf8_bom_from_first_header_cell, unsupported_sql_error, validate_sql_identifier,
};
use arrow_schema::{DataType, Field, Schema};
use shardloom_core::{LogicalDType, Result, ScalarValue};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
#[cfg(all(feature = "vortex-local-primitives", unix))]
use shardloom_vortex::universal_format_io::demanded::LocalColumnarSource;
use shardloom_vortex::universal_format_io::text_stream::{
    TextFraming, TextRecordDecoder, TextSource, TextTypePolicy, read_header, resolve_record_fields,
};
use std::{path::Path, sync::Arc};

const CONTEXT: &str = "native demanded text source";

struct Hints {
    values: Vec<(String, LogicalDType)>,
    temporal: Vec<Option<BoundTextTemporalColumn>>,
    _metadata: MemoryLease,
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
pub(crate) fn admit(
    path: &Path,
    format: &str,
    schema: Option<&str>,
    fingerprint: bool,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<Arc<LocalColumnarSource>> {
    cancellation.check()?;
    let metadata = memory.reserve(schema.map_or(4096, |s| s.len() as u64 * 32 + 4096))?;
    let values = schema
        .map(parse_vortex_ingest_schema_hints)
        .transpose()?
        .unwrap_or_default();
    let temporal = values
        .iter()
        .map(|(_, dtype)| BoundTextTemporalColumn::bind(dtype, memory))
        .collect::<Result<Vec<_>>>()?;
    let hints = Arc::new(Hints {
        values,
        temporal,
        _metadata: metadata,
    });
    LocalColumnarSource::admit_text_with(
        path,
        format,
        fingerprint,
        memory,
        cancellation,
        &|file, memory, cancellation| prepare(file, format, &hints, None, memory, cancellation),
    )
}

pub(super) fn prepare_single(
    path: &Path,
    format: &str,
    hints: &[(String, LogicalDType)],
    max_rows: Option<usize>,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<Arc<TextSource>> {
    let bytes = hints.iter().try_fold(4096_u64, |sum, (name, _)| {
        sum.checked_add(name.len() as u64 * 32 + 4096)
            .ok_or_else(|| unsupported_sql_error("text hint metadata overflow"))
    })?;
    let metadata = memory.reserve(bytes)?;
    let temporal = hints
        .iter()
        .map(|(_, dtype)| BoundTextTemporalColumn::bind(dtype, memory))
        .collect::<Result<Vec<_>>>()?;
    let hints = Arc::new(Hints {
        values: hints.to_vec(),
        temporal,
        _metadata: metadata,
    });
    prepare(path, format, &hints, max_rows, memory, cancellation)
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[allow(clippy::too_many_arguments)]
pub(super) fn admit_parsed(
    path: &Path,
    format: &str,
    hints: &[(String, LogicalDType)],
    max_rows: Option<usize>,
    fingerprint: bool,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<Arc<LocalColumnarSource>> {
    let bytes = hints.iter().try_fold(4096_u64, |sum, (name, _)| {
        sum.checked_add(name.len() as u64 * 32 + 4096)
            .ok_or_else(|| unsupported_sql_error("text hint metadata overflow"))
    })?;
    let metadata = memory.reserve(bytes)?;
    let temporal = hints
        .iter()
        .map(|(_, dtype)| BoundTextTemporalColumn::bind(dtype, memory))
        .collect::<Result<Vec<_>>>()?;
    let hints = Arc::new(Hints {
        values: hints.to_vec(),
        temporal,
        _metadata: metadata,
    });
    LocalColumnarSource::admit_text_with(
        path,
        format,
        fingerprint,
        memory,
        cancellation,
        &|file, memory, cancellation| prepare(file, format, &hints, max_rows, memory, cancellation),
    )
}

fn prepare(
    path: &Path,
    format: &str,
    hints: &Arc<Hints>,
    max_rows: Option<usize>,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<Arc<TextSource>> {
    let (framing, delimiter) = match format {
        "csv" => (TextFraming::Csv, Some(',')),
        "tsv" => (TextFraming::Csv, Some('\t')),
        "json" => (TextFraming::JsonDocument, None),
        "jsonl" | "ndjson" => (TextFraming::JsonLines, None),
        _ => {
            return Err(unsupported_sql_error(
                "text source format has no incremental adapter",
            ));
        }
    };
    let (mut header, header_bytes) = if let Some(delimiter) = delimiter {
        let (record, bytes) = read_header(path, framing, memory, cancellation)?;
        let _parse = memory.reserve(record.value().len() as u64 * 32 + 4096)?;
        let mut header = split_delimited_record(
            std::str::from_utf8(record.value())
                .map_err(|e| unsupported_sql_error(&e.to_string()))?
                .trim_start_matches('\u{feff}')
                .trim_end_matches(['\r', '\n']),
            delimiter,
        )?;
        strip_utf8_bom_from_first_header_cell(&mut header);
        for name in &header {
            validate_sql_identifier(name)?;
        }
        (header, bytes)
    } else {
        (Vec::new(), 0)
    };
    let declared = if hints.values.is_empty() {
        None
    } else {
        Some(Arc::new(Schema::new(
            hints
                .values
                .iter()
                .map(|(name, dtype)| {
                    schema_declared_text_arrow_dtype(dtype, name, CONTEXT)
                        .map(|dtype| Field::new(name, dtype, true))
                })
                .collect::<Result<Vec<_>>>()?,
        )))
    };
    if delimiter.is_none() && !hints.values.is_empty() {
        header = hints.values.iter().map(|(name, _)| name.clone()).collect();
    }
    let decoder = decoder(delimiter, header.clone(), Arc::clone(hints));
    let policy = (format != "json").then_some(TextTypePolicy {
        merge: merge_types,
        coerce: coerce_inferred,
    });
    let mut source = TextSource::admit_adapted(
        path,
        framing,
        delimiter.is_some(),
        decoder,
        declared,
        max_rows,
        &header,
        policy,
        memory,
        cancellation,
    )?;
    TextSource::add_header_read_bytes(&mut source, header_bytes)?;
    Ok(source)
}

fn decoder(delimiter: Option<char>, header: Vec<String>, hints: Arc<Hints>) -> TextRecordDecoder {
    Arc::new(move |record| {
        if let Some(delimiter) = delimiter {
            let fields = split_delimited_record(record.trim_end_matches(['\r', '\n']), delimiter)?;
            if fields.len() != header.len() {
                return Err(unsupported_sql_error("CSV row width must match its header"));
            }
            header
                .iter()
                .zip(fields)
                .map(|(name, raw)| {
                    let value = if let Some(index) =
                        hints.values.iter().position(|(column, _)| column == name)
                    {
                        parse_schema_declared_text_scalar(
                            &raw,
                            &hints.values[index].1,
                            name,
                            CONTEXT,
                            hints.temporal[index].as_ref().map(|bound| &bound.codec),
                        )?
                    } else {
                        parse_csv_scalar(&raw)
                    };
                    Ok((name.clone(), value))
                })
                .collect()
        } else {
            if !hints.values.is_empty() && record.trim() == "{}" {
                return Ok(Vec::new());
            }
            let fields = resolve_record_fields(parse_flat_json_object_with_plan(
                record.trim_start_matches('\u{feff}'),
                &LocalSourceReadPlan::full("native_demanded_text_record"),
            )?);
            fields
                .into_iter()
                .map(|(name, value)| {
                    let value = if let Some(index) =
                        hints.values.iter().position(|(column, _)| column == &name)
                    {
                        coerce_schema_declared_json_scalar(
                            value,
                            &hints.values[index].1,
                            &name,
                            CONTEXT,
                            hints.temporal[index].as_ref().map(|bound| &bound.codec),
                        )?
                    } else {
                        value
                    };
                    Ok((name, value))
                })
                .collect()
        }
    })
}

// Keep the established CSV/JSONL promotion policy. Whole JSON retains strict
// homogeneous inference; explicit schema hints retain their exact typed codec.
#[allow(clippy::unnecessary_wraps)] // Shared policy callbacks may reject a type merge.
fn merge_types(left: &DataType, right: &DataType) -> Result<DataType> {
    let numeric = |dtype: &DataType| {
        matches!(
            dtype,
            DataType::Int64 | DataType::UInt64 | DataType::Float64
        )
    };
    Ok(if left == right {
        left.clone()
    } else if numeric(left) && numeric(right) {
        DataType::Float64
    } else {
        DataType::Utf8
    })
}

fn coerce_inferred(value: ScalarValue, dtype: &DataType, name: &str) -> Result<ScalarValue> {
    match dtype {
        DataType::Float64 => {
            // An integer is exact in f64 when its nonzero significant bit span
            // fits the 53-bit significand, including powers above 2^53.
            let exact_integer = |magnitude: u64| {
                (64 - magnitude.leading_zeros()).saturating_sub(53) <= magnitude.trailing_zeros()
            };
            let exact = match &value {
                ScalarValue::Int64(value) => exact_integer(value.unsigned_abs()),
                ScalarValue::UInt64(value) => exact_integer(*value),
                _ => true,
            };
            if !exact {
                return Err(unsupported_sql_error(&format!(
                    "column '{name}' inferred numeric promotion would lose integer precision; declare an exact source schema"
                )));
            }
            coerce_schema_declared_json_scalar(value, &LogicalDType::Float64, name, CONTEXT, None)
        }
        DataType::Utf8 => {
            coerce_schema_declared_json_scalar(value, &LogicalDType::Utf8, name, CONTEXT, None)
        }
        _ => Ok(value),
    }
}
