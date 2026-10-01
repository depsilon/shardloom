//! Text encoding is a terminal consumer of complete typed native batches.
//! No full row table, JSON query report, disk spool or query replay is required.

use super::{
    Result, ShardLoomError, StatValue, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveExecutionStatus, VortexLocalPrimitivePhysicalPolicyReport,
    VortexLocalPrimitiveRowExportFormat as Format, VortexLocalPrimitiveRowExportReport,
    VortexLocalPrimitiveStateBudgetReport, VortexNativeArraySinkEvidence,
    VortexQueryPrimitiveRequest, disabled_row_export_evidence, logical_field_from_native_array,
    native_sink::{ArrayProducer, NativeSinkPlan, OwnedOutput},
    usize_to_u64, vortex_error, vortex_scalar_to_stat_value,
};
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    io::{BufWriter, Write},
    path::Path,
};
use vortex::array::VortexSessionExecute as _;

const BATCH_ROWS: usize = 2048;
const BATCH_BYTES: u64 = 8 * 1024 * 1024;

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native text sink: {reason}; no fallback execution was attempted"
    ))
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn write(
    plan: NativeSinkPlan,
    request: &VortexQueryPrimitiveRequest,
    path: &Path,
    format: Format,
    overwrite: bool,
    policy: VortexLocalPrimitiveExecutionPolicy,
    producer: Option<&mut ArrayProducer<'_>>,
    cancellation: &CancellationToken,
) -> Result<VortexLocalPrimitiveRowExportReport> {
    if !matches!(format, Format::Json | Format::Jsonl | Format::Csv) {
        return Err(failed("requires JSON, JSONL or CSV output"));
    }
    cancellation.check()?;
    plan.source.validate_generation()?;
    let mut scratch = plan.session.memory().reserve(128 * 1024)?;
    let mut output = OwnedOutput::new(path, overwrite)?;
    let mut rows = 0_u64;
    let mut batches = 0_usize;
    let mut maximum_rows = 0_usize;
    let mut logical_bytes = 0_u64;
    let mut text_copies = 0_u64;
    plan.source
        .with_native_execution(cancellation, |file, context| {
            let mut scalar_context = context.native_session().create_execution_ctx();
            let mut writer = BufWriter::with_capacity(64 * 1024, &mut output.file);
            match format {
                Format::Json => writer.write_all(b"["),
                Format::Csv => {
                    for (index, name) in plan.columns.iter().enumerate() {
                        if index > 0 {
                            writer.write_all(b",").map_err(vortex_error)?;
                        }
                        csv_text(&mut writer, name)?;
                    }
                    writer.write_all(b"\n")
                }
                _ => Ok(()),
            }
            .map_err(vortex_error)?;
            plan.consume(file, context, BATCH_ROWS, producer, |array| {
                context.check_cancelled()?;
                if array.dtype() != &plan.dtype
                    || array.len() > BATCH_ROWS
                    || array.nbytes() > BATCH_BYTES
                {
                    return Err(failed("result batch changed its admitted schema or size"));
                }
                let bytes = array
                    .nbytes()
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(128 * 1024))
                    .ok_or_else(|| failed("text scratch reservation overflow"))?;
                if bytes > scratch.bytes() {
                    scratch.resize(bytes)?;
                }
                let columns = plan
                    .columns
                    .iter()
                    .map(|name| logical_field_from_native_array(&array, name))
                    .collect::<Result<Vec<_>>>()?;
                batches += 1;
                maximum_rows = maximum_rows.max(array.len());
                logical_bytes = logical_bytes
                    .checked_add(array.nbytes())
                    .ok_or_else(|| failed("logical byte counter overflow"))?;
                for row in 0..array.len() {
                    context.check_cancelled()?;
                    if format == Format::Json && rows > 0 {
                        writer.write_all(b",").map_err(vortex_error)?;
                    }
                    if format != Format::Csv {
                        writer.write_all(b"{").map_err(vortex_error)?;
                    }
                    for (index, (name, column)) in plan.columns.iter().zip(&columns).enumerate() {
                        if index > 0 {
                            writer.write_all(b",").map_err(vortex_error)?;
                        }
                        if format != Format::Csv {
                            serde_json::to_writer(&mut writer, name).map_err(vortex_error)?;
                            writer.write_all(b":").map_err(vortex_error)?;
                        }
                        let scalar = column
                            .execute_scalar(row, &mut scalar_context)
                            .map_err(vortex_error)?;
                        let value = if scalar.is_null() {
                            StatValue::Null
                        } else {
                            vortex_scalar_to_stat_value(&scalar)
                                .ok_or_else(|| failed("text output requires a flat scalar value"))?
                        };
                        if let StatValue::Utf8(text) = &value {
                            text_copies = text_copies
                                .checked_add(usize_to_u64(text.len())?)
                                .ok_or_else(|| failed("text copy counter overflow"))?;
                        }
                        write_value(&mut writer, value, format)?;
                    }
                    writer
                        .write_all(if format == Format::Csv { b"\n" } else { b"}\n" })
                        .map_err(vortex_error)?;
                    rows = rows
                        .checked_add(1)
                        .ok_or_else(|| failed("row counter overflow"))?;
                }
                Ok(true)
            })?;
            if format == Format::Json {
                writer.write_all(b"]\n").map_err(vortex_error)?;
            }
            writer.flush().map_err(vortex_error)?;
            context.check_cancelled()
        })?;
    output.file.sync_all().map_err(vortex_error)?;
    let checksum = output.checksum()?;
    cancellation.check()?;
    plan.source.validate_generation()?;
    output.commit()?;
    let mut evidence = disabled_row_export_evidence();
    evidence.side_effects.write_io = true;
    evidence.side_effects.data_materialized = rows > 0;
    evidence.materialization_boundary_reported = true;
    evidence.native_array_sink = Some(VortexNativeArraySinkEvidence {
        native_arrays_submitted: usize_to_u64(batches)?,
        native_array_logical_bytes: logical_bytes,
        adapter_payload_bytes_copied: text_copies,
        scalar_values_materialized: rows
            .checked_mul(usize_to_u64(plan.columns.len())?)
            .ok_or_else(|| failed("scalar counter overflow"))?,
        scan_row_bound: BATCH_ROWS,
        writer_input_batch_bound: 1,
        peak_reserved_bytes: plan.session.snapshot().memory.peak_reserved_bytes,
        metadata_reserved_bytes: 128 * 1024,
        pre_limit_result_row_count: rows,
        pre_limit_result_row_count_exact: true,
        source_generation_validated: true,
        dtype_and_row_count_validated: false,
        output_sha256: checksum,
        compatibility: None,
        metadata_fidelity: if format == Format::Csv {
            "terminal_CSV_encoding;header_and_row_order_preserved;nulls_emit_empty_cells;dtype_validity_encodings_layout_statistics_not_persisted;native_input_schema_checked;no_persisted_dtype_claim"
        } else {
            "terminal_JSON_encoding;field_names_nulls_values_and_row_order_preserved;integer_widths_native_encodings_layout_statistics_not_persisted;native_input_schema_checked;no_persisted_dtype_claim"
        },
    });
    Ok(VortexLocalPrimitiveRowExportReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        primitive_kind: request.kind,
        output_path: path.display().to_string(),
        output_format: format.as_str(),
        rows_scanned: plan.row_count,
        rows_written: rows,
        pre_limit_result_row_count: rows,
        projected_columns: plan.columns,
        arrays_read_count: batches,
        max_chunk_rows: maximum_rows,
        resource_envelope: policy.resource_envelope(),
        physical_policy: VortexLocalPrimitivePhysicalPolicyReport::not_selected(),
        max_parallelism_requested: policy.max_parallelism,
        scan_concurrency_per_worker: 1,
        source_order_limit_requested: plan.limit,
        state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
        evidence,
        diagnostics: Vec::new(),
    })
}

fn write_value(writer: &mut impl Write, value: StatValue, format: Format) -> Result<()> {
    if let StatValue::Float64(value) = &value
        && !value.is_finite()
    {
        return Err(failed("nonfinite text numbers are not admitted"));
    }
    if format != Format::Csv {
        return match value {
            StatValue::Null => writer.write_all(b"null").map_err(vortex_error),
            StatValue::Boolean(value) => {
                serde_json::to_writer(writer, &value).map_err(vortex_error)
            }
            StatValue::Int64(value) => serde_json::to_writer(writer, &value).map_err(vortex_error),
            StatValue::UInt64(value) => serde_json::to_writer(writer, &value).map_err(vortex_error),
            StatValue::Float64(value) => {
                serde_json::to_writer(writer, &value).map_err(vortex_error)
            }
            StatValue::Utf8(value) => serde_json::to_writer(writer, &value).map_err(vortex_error),
        };
    }
    match value {
        StatValue::Null => Ok(()),
        StatValue::Boolean(value) => write!(writer, "{value}").map_err(vortex_error),
        StatValue::Int64(value) => write!(writer, "{value}").map_err(vortex_error),
        StatValue::UInt64(value) => write!(writer, "{value}").map_err(vortex_error),
        StatValue::Float64(value) => write!(writer, "{value}").map_err(vortex_error),
        StatValue::Utf8(value) => csv_text(writer, &value),
    }
}

fn csv_text(writer: &mut impl Write, text: &str) -> Result<()> {
    if !text.is_empty() && !text.contains([',', '"', '\r', '\n']) {
        return writer.write_all(text.as_bytes()).map_err(vortex_error);
    }
    writer.write_all(b"\"").map_err(vortex_error)?;
    for (index, part) in text.split('"').enumerate() {
        if index > 0 {
            writer.write_all(b"\"\"").map_err(vortex_error)?;
        }
        writer.write_all(part.as_bytes()).map_err(vortex_error)?;
    }
    writer.write_all(b"\"").map_err(vortex_error)
}
