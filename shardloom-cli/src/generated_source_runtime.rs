//! Scoped generated-source runtime smoke handlers.
//!
//! This module implements deliberately narrow local generated-output smokes. It
//! accepts either rows already supplied by the user/API layer or one
//! ShardLoom-native range generator, writes a local JSONL sink, and emits
//! generated-source/output evidence. It does not read source datasets, parse
//! SQL, execute broad `DataFrame` expressions, touch object stores, invoke
//! Foundry, or call fallback engines.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
    time::Instant,
};

use shardloom_core::{CommandStatus, OutputFormat, ShardLoomError};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

const USER_ROWS_COMMAND: &str = "generated-source-user-rows-smoke";
const USER_ROWS_SCHEMA_VERSION: &str = "shardloom.generated_source_user_rows_smoke.v1";
const USER_ROWS_GENERATED_SOURCE_CERTIFICATE_ID: &str =
    "generated-source.user-rows.local-output.v1";
const USER_ROWS_OUTPUT_NATIVE_IO_CERTIFICATE_ID: &str =
    "generated-source.user-rows.local-output.native-io.v1";
const USER_ROWS_EXECUTION_CERTIFICATE_ID: &str =
    "generated-source.user-rows.local-output.execution.v1";

const RANGE_COMMAND: &str = "generated-source-range-smoke";
const RANGE_SCHEMA_VERSION: &str = "shardloom.generated_source_range_smoke.v1";
const RANGE_GENERATED_SOURCE_CERTIFICATE_ID: &str = "generated-source.range.local-output.v1";
const RANGE_OUTPUT_NATIVE_IO_CERTIFICATE_ID: &str =
    "generated-source.range.local-output.native-io.v1";
const RANGE_EXECUTION_CERTIFICATE_ID: &str = "generated-source.range.local-output.execution.v1";
const MAX_GENERATED_RANGE_ROWS: usize = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeneratedOutputFormat {
    Jsonl,
}

impl GeneratedOutputFormat {
    fn parse(value: &str) -> Result<Self, ShardLoomError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "jsonl" | "json-lines" | "ndjson" => Ok(Self::Jsonl),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unsupported generated-source output format {other:?}; scoped generated-source smokes support local JSONL only"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeneratedValueType {
    Int64,
    Float64,
    Bool,
    Utf8,
}

impl GeneratedValueType {
    fn parse(value: &str) -> Result<Self, ShardLoomError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "int64" | "int" | "integer" => Ok(Self::Int64),
            "float64" | "float" | "double" => Ok(Self::Float64),
            "bool" | "boolean" => Ok(Self::Bool),
            "utf8" | "string" | "str" => Ok(Self::Utf8),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unsupported generated-source column type {other:?}; supported types are int64,float64,bool,utf8"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Int64 => "int64",
            Self::Float64 => "float64",
            Self::Bool => "bool",
            Self::Utf8 => "utf8",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedColumn {
    name: String,
    value_type: GeneratedValueType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedRow {
    values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedUserRowsSmokeRequest {
    output_path: PathBuf,
    output_format: GeneratedOutputFormat,
    schema: Vec<GeneratedColumn>,
    rows: Vec<GeneratedRow>,
    allow_overwrite: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedUserRowsSmokeReport {
    output_path: PathBuf,
    output_format: GeneratedOutputFormat,
    schema: Vec<GeneratedColumn>,
    rows: Vec<GeneratedRow>,
    output_bytes: u64,
    output_digest: String,
    schema_digest: String,
    plan_digest: String,
    write_millis: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedRangeSmokeRequest {
    output_path: PathBuf,
    output_format: GeneratedOutputFormat,
    start: i64,
    end: i64,
    step: i64,
    column_name: String,
    allow_overwrite: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedRangeSmokeReport {
    output_path: PathBuf,
    output_format: GeneratedOutputFormat,
    start: i64,
    end: i64,
    step: i64,
    column_name: String,
    schema: Vec<GeneratedColumn>,
    rows: Vec<GeneratedRow>,
    output_bytes: u64,
    output_digest: String,
    schema_digest: String,
    plan_digest: String,
    write_millis: u128,
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_generated_source_user_rows_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(output_target) = args.next() else {
        eprintln!(
            "usage: shardloom {USER_ROWS_COMMAND} <local-output-path> <schema> <rows> [--output-format jsonl] [--allow-overwrite]"
        );
        return ExitCode::from(2);
    };
    let Some(schema_raw) = args.next() else {
        return emit_error(
            USER_ROWS_COMMAND,
            format,
            "generated-source smoke failed",
            &ShardLoomError::InvalidOperation(
                "generated-source user rows smoke requires a schema argument".to_string(),
            ),
        );
    };
    let Some(rows_raw) = args.next() else {
        return emit_error(
            USER_ROWS_COMMAND,
            format,
            "generated-source smoke failed",
            &ShardLoomError::InvalidOperation(
                "generated-source user rows smoke requires a rows argument".to_string(),
            ),
        );
    };

    let mut output_format = GeneratedOutputFormat::Jsonl;
    let mut allow_overwrite = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output-format" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        USER_ROWS_COMMAND,
                        format,
                        "generated-source smoke failed",
                        &ShardLoomError::InvalidOperation(
                            "--output-format requires a value".to_string(),
                        ),
                    );
                };
                output_format = match GeneratedOutputFormat::parse(&value) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return emit_error(
                            USER_ROWS_COMMAND,
                            format,
                            "generated-source smoke failed",
                            &error,
                        );
                    }
                };
            }
            "--allow-overwrite" => allow_overwrite = true,
            extra => {
                return emit_error(
                    USER_ROWS_COMMAND,
                    format,
                    "generated-source smoke failed",
                    &cli_unknown_arg_error(USER_ROWS_COMMAND, extra),
                );
            }
        }
    }

    let request = match GeneratedUserRowsSmokeRequest::parse(
        &output_target,
        output_format,
        &schema_raw,
        &rows_raw,
        allow_overwrite,
    ) {
        Ok(request) => request,
        Err(error) => {
            return emit_error(
                USER_ROWS_COMMAND,
                format,
                "generated-source smoke failed",
                &error,
            );
        }
    };

    let report = match run_generated_user_rows_smoke(&request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                USER_ROWS_COMMAND,
                format,
                "generated-source smoke failed",
                &error,
            );
        }
    };

    emit(
        USER_ROWS_COMMAND,
        format,
        CommandStatus::Success,
        format!(
            "generated user rows local-output smoke wrote {} row(s)",
            report.rows.len()
        ),
        report.to_text(),
        vec![],
        report.fields(),
    );
    ExitCode::SUCCESS
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_generated_source_range_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(output_target) = args.next() else {
        eprintln!(
            "usage: shardloom {RANGE_COMMAND} <local-output-path> <start> <end> [--step int] [--column name] [--output-format jsonl] [--allow-overwrite]"
        );
        return ExitCode::from(2);
    };
    let Some(start_raw) = args.next() else {
        return emit_error(
            RANGE_COMMAND,
            format,
            "generated-source range smoke failed",
            &ShardLoomError::InvalidOperation(
                "generated-source range smoke requires a start argument".to_string(),
            ),
        );
    };
    let Some(end_raw) = args.next() else {
        return emit_error(
            RANGE_COMMAND,
            format,
            "generated-source range smoke failed",
            &ShardLoomError::InvalidOperation(
                "generated-source range smoke requires an end argument".to_string(),
            ),
        );
    };

    let mut output_format = GeneratedOutputFormat::Jsonl;
    let mut allow_overwrite = false;
    let mut step = 1_i64;
    let mut column_name = "value".to_string();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output-format" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        RANGE_COMMAND,
                        format,
                        "generated-source range smoke failed",
                        &ShardLoomError::InvalidOperation(
                            "--output-format requires a value".to_string(),
                        ),
                    );
                };
                output_format = match GeneratedOutputFormat::parse(&value) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return emit_error(
                            RANGE_COMMAND,
                            format,
                            "generated-source range smoke failed",
                            &error,
                        );
                    }
                };
            }
            "--allow-overwrite" => allow_overwrite = true,
            "--step" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        RANGE_COMMAND,
                        format,
                        "generated-source range smoke failed",
                        &ShardLoomError::InvalidOperation("--step requires a value".to_string()),
                    );
                };
                step = match parse_i64_arg("step", &value) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return emit_error(
                            RANGE_COMMAND,
                            format,
                            "generated-source range smoke failed",
                            &error,
                        );
                    }
                };
            }
            "--column" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        RANGE_COMMAND,
                        format,
                        "generated-source range smoke failed",
                        &ShardLoomError::InvalidOperation("--column requires a value".to_string()),
                    );
                };
                column_name = match percent_decode(&value) {
                    Ok(parsed) if !parsed.trim().is_empty() => parsed,
                    Ok(_) => {
                        return emit_error(
                            RANGE_COMMAND,
                            format,
                            "generated-source range smoke failed",
                            &ShardLoomError::InvalidOperation(
                                "generated-source range column must not be empty".to_string(),
                            ),
                        );
                    }
                    Err(error) => {
                        return emit_error(
                            RANGE_COMMAND,
                            format,
                            "generated-source range smoke failed",
                            &error,
                        );
                    }
                };
            }
            extra => {
                return emit_error(
                    RANGE_COMMAND,
                    format,
                    "generated-source range smoke failed",
                    &cli_unknown_arg_error(RANGE_COMMAND, extra),
                );
            }
        }
    }

    let start = match parse_i64_arg("start", &start_raw) {
        Ok(parsed) => parsed,
        Err(error) => {
            return emit_error(
                RANGE_COMMAND,
                format,
                "generated-source range smoke failed",
                &error,
            );
        }
    };
    let end = match parse_i64_arg("end", &end_raw) {
        Ok(parsed) => parsed,
        Err(error) => {
            return emit_error(
                RANGE_COMMAND,
                format,
                "generated-source range smoke failed",
                &error,
            );
        }
    };
    let request = match GeneratedRangeSmokeRequest::parse(
        &output_target,
        output_format,
        start,
        end,
        step,
        column_name,
        allow_overwrite,
    ) {
        Ok(request) => request,
        Err(error) => {
            return emit_error(
                RANGE_COMMAND,
                format,
                "generated-source range smoke failed",
                &error,
            );
        }
    };

    let report = match run_generated_range_smoke(&request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                RANGE_COMMAND,
                format,
                "generated-source range smoke failed",
                &error,
            );
        }
    };

    emit(
        RANGE_COMMAND,
        format,
        CommandStatus::Success,
        format!(
            "generated range local-output smoke wrote {} row(s)",
            report.rows.len()
        ),
        report.to_text(),
        vec![],
        report.fields(),
    );
    ExitCode::SUCCESS
}

impl GeneratedUserRowsSmokeRequest {
    fn parse(
        output_target: &str,
        output_format: GeneratedOutputFormat,
        schema_raw: &str,
        rows_raw: &str,
        allow_overwrite: bool,
    ) -> Result<Self, ShardLoomError> {
        let output_path = normalize_local_output_path(output_target)?;
        let schema = parse_schema(schema_raw)?;
        let rows = parse_rows(rows_raw, &schema)?;
        if rows.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "generated-source user rows smoke requires at least one row".to_string(),
            ));
        }
        Ok(Self {
            output_path,
            output_format,
            schema,
            rows,
            allow_overwrite,
        })
    }
}

impl GeneratedUserRowsSmokeReport {
    #[allow(clippy::too_many_lines)]
    fn fields(&self) -> Vec<(String, String)> {
        vec![
            (
                "schema_version".to_string(),
                USER_ROWS_SCHEMA_VERSION.to_string(),
            ),
            (
                "generated_source_smoke_report_id".to_string(),
                USER_ROWS_GENERATED_SOURCE_CERTIFICATE_ID.to_string(),
            ),
            (
                "execution_mode".to_string(),
                "source_free_generated_output".to_string(),
            ),
            ("engine_mode".to_string(), "batch".to_string()),
            ("runtime_execution".to_string(), "true".to_string()),
            ("input_dataset_count".to_string(), "0".to_string()),
            ("source_io_performed".to_string(), "false".to_string()),
            (
                "source_native_io_certificate_status".to_string(),
                "not_applicable_no_source_dataset".to_string(),
            ),
            ("generated_source_created".to_string(), "true".to_string()),
            ("generated_source_kind".to_string(), "user_rows".to_string()),
            (
                "generated_source_schema_digest".to_string(),
                self.schema_digest.clone(),
            ),
            (
                "generated_source_schema".to_string(),
                canonical_schema(&self.schema),
            ),
            (
                "generated_source_row_count".to_string(),
                self.rows.len().to_string(),
            ),
            (
                "generated_source_plan_digest".to_string(),
                self.plan_digest.clone(),
            ),
            ("generated_source_seed".to_string(), "none".to_string()),
            ("generation_deterministic".to_string(), "true".to_string()),
            (
                "generated_source_certificate_status".to_string(),
                "present".to_string(),
            ),
            (
                "generated_source_certificate_id".to_string(),
                USER_ROWS_GENERATED_SOURCE_CERTIFICATE_ID.to_string(),
            ),
            ("output_io_performed".to_string(), "true".to_string()),
            ("write_io".to_string(), "true".to_string()),
            (
                "output_format".to_string(),
                self.output_format.as_str().to_string(),
            ),
            (
                "output_path".to_string(),
                self.output_path.display().to_string(),
            ),
            ("output_bytes".to_string(), self.output_bytes.to_string()),
            ("output_digest".to_string(), self.output_digest.clone()),
            (
                "output_native_io_certificate_status".to_string(),
                "certified_local_file_sink".to_string(),
            ),
            (
                "output_native_io_certificate_id".to_string(),
                USER_ROWS_OUTPUT_NATIVE_IO_CERTIFICATE_ID.to_string(),
            ),
            (
                "execution_certificate_status".to_string(),
                "certified".to_string(),
            ),
            (
                "execution_certificate_id".to_string(),
                USER_ROWS_EXECUTION_CERTIFICATE_ID.to_string(),
            ),
            ("correctness_digest".to_string(), self.output_digest.clone()),
            (
                "materialization_boundary".to_string(),
                "python_user_rows_to_local_jsonl_sink".to_string(),
            ),
            ("data_materialized".to_string(), "true".to_string()),
            ("data_decoded".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("network_probe".to_string(), "false".to_string()),
            ("catalog_probe".to_string(), "false".to_string()),
            ("foundry_runtime_invoked".to_string(), "false".to_string()),
            ("foundry_spark_invoked".to_string(), "false".to_string()),
            ("fallback_attempted".to_string(), "false".to_string()),
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("external_engine_invoked".to_string(), "false".to_string()),
            (
                "claim_gate_status".to_string(),
                "fixture_smoke_only".to_string(),
            ),
            (
                "claim_gate_reason".to_string(),
                "one_scoped_local_user_rows_generated_output_smoke".to_string(),
            ),
            ("performance_claim_allowed".to_string(), "false".to_string()),
            ("production_claim_allowed".to_string(), "false".to_string()),
            (
                "sql_dataframe_runtime_claim_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "object_store_lakehouse_claim_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "output_write_millis".to_string(),
                self.write_millis.to_string(),
            ),
        ]
    }

    fn to_text(&self) -> String {
        format!(
            "generated-source user rows smoke\nschema_version: {USER_ROWS_SCHEMA_VERSION}\nschema: {}\nrows: {}\noutput: {}\noutput format: {}\ngenerated source certificate: present\noutput Native I/O certificate: certified_local_file_sink\nfallback_attempted: false\nexternal_engine_invoked: false\nclaim_gate_status: fixture_smoke_only",
            canonical_schema(&self.schema),
            self.rows.len(),
            self.output_path.display(),
            self.output_format.as_str(),
        )
    }
}

impl GeneratedRangeSmokeRequest {
    fn parse(
        output_target: &str,
        output_format: GeneratedOutputFormat,
        start: i64,
        end: i64,
        step: i64,
        column_name: String,
        allow_overwrite: bool,
    ) -> Result<Self, ShardLoomError> {
        if step == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "generated-source range step must not be zero".to_string(),
            ));
        }
        if column_name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "generated-source range column must not be empty".to_string(),
            ));
        }
        let output_path = normalize_local_output_path(output_target)?;
        let row_count = range_row_count(start, end, step)?;
        if row_count > MAX_GENERATED_RANGE_ROWS {
            return Err(ShardLoomError::InvalidOperation(format!(
                "generated-source range row count {row_count} exceeds scoped smoke limit {MAX_GENERATED_RANGE_ROWS}"
            )));
        }
        Ok(Self {
            output_path,
            output_format,
            start,
            end,
            step,
            column_name,
            allow_overwrite,
        })
    }
}

impl GeneratedRangeSmokeReport {
    #[allow(clippy::too_many_lines)]
    fn fields(&self) -> Vec<(String, String)> {
        vec![
            (
                "schema_version".to_string(),
                RANGE_SCHEMA_VERSION.to_string(),
            ),
            (
                "generated_source_smoke_report_id".to_string(),
                RANGE_GENERATED_SOURCE_CERTIFICATE_ID.to_string(),
            ),
            (
                "execution_mode".to_string(),
                "source_free_generated_output".to_string(),
            ),
            ("engine_mode".to_string(), "batch".to_string()),
            ("runtime_execution".to_string(), "true".to_string()),
            ("input_dataset_count".to_string(), "0".to_string()),
            ("source_io_performed".to_string(), "false".to_string()),
            (
                "source_native_io_certificate_status".to_string(),
                "not_applicable_no_source_dataset".to_string(),
            ),
            ("generated_source_created".to_string(), "true".to_string()),
            ("generated_source_kind".to_string(), "range".to_string()),
            (
                "generated_source_range_start".to_string(),
                self.start.to_string(),
            ),
            (
                "generated_source_range_end".to_string(),
                self.end.to_string(),
            ),
            (
                "generated_source_range_step".to_string(),
                self.step.to_string(),
            ),
            (
                "generated_source_range_column".to_string(),
                self.column_name.clone(),
            ),
            (
                "generated_source_schema_digest".to_string(),
                self.schema_digest.clone(),
            ),
            (
                "generated_source_schema".to_string(),
                canonical_schema(&self.schema),
            ),
            (
                "generated_source_row_count".to_string(),
                self.rows.len().to_string(),
            ),
            (
                "generated_source_plan_digest".to_string(),
                self.plan_digest.clone(),
            ),
            ("generated_source_seed".to_string(), "none".to_string()),
            ("generation_deterministic".to_string(), "true".to_string()),
            (
                "generated_source_certificate_status".to_string(),
                "present".to_string(),
            ),
            (
                "generated_source_certificate_id".to_string(),
                RANGE_GENERATED_SOURCE_CERTIFICATE_ID.to_string(),
            ),
            ("output_io_performed".to_string(), "true".to_string()),
            ("write_io".to_string(), "true".to_string()),
            (
                "output_format".to_string(),
                self.output_format.as_str().to_string(),
            ),
            (
                "output_path".to_string(),
                self.output_path.display().to_string(),
            ),
            ("output_bytes".to_string(), self.output_bytes.to_string()),
            ("output_digest".to_string(), self.output_digest.clone()),
            (
                "output_native_io_certificate_status".to_string(),
                "certified_local_file_sink".to_string(),
            ),
            (
                "output_native_io_certificate_id".to_string(),
                RANGE_OUTPUT_NATIVE_IO_CERTIFICATE_ID.to_string(),
            ),
            (
                "execution_certificate_status".to_string(),
                "certified".to_string(),
            ),
            (
                "execution_certificate_id".to_string(),
                RANGE_EXECUTION_CERTIFICATE_ID.to_string(),
            ),
            ("correctness_digest".to_string(), self.output_digest.clone()),
            (
                "materialization_boundary".to_string(),
                "engine_native_range_generator_to_local_jsonl_sink".to_string(),
            ),
            ("data_materialized".to_string(), "true".to_string()),
            ("data_decoded".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("network_probe".to_string(), "false".to_string()),
            ("catalog_probe".to_string(), "false".to_string()),
            ("foundry_runtime_invoked".to_string(), "false".to_string()),
            ("foundry_spark_invoked".to_string(), "false".to_string()),
            ("fallback_attempted".to_string(), "false".to_string()),
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("external_engine_invoked".to_string(), "false".to_string()),
            (
                "claim_gate_status".to_string(),
                "fixture_smoke_only".to_string(),
            ),
            (
                "claim_gate_reason".to_string(),
                "one_scoped_local_range_generated_output_smoke".to_string(),
            ),
            ("performance_claim_allowed".to_string(), "false".to_string()),
            ("production_claim_allowed".to_string(), "false".to_string()),
            (
                "sql_dataframe_runtime_claim_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "object_store_lakehouse_claim_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "output_write_millis".to_string(),
                self.write_millis.to_string(),
            ),
        ]
    }

    fn to_text(&self) -> String {
        format!(
            "generated-source range smoke\nschema_version: {RANGE_SCHEMA_VERSION}\nrange: {}..{} step {}\ncolumn: {}\nrows: {}\noutput: {}\noutput format: {}\ngenerated source certificate: present\noutput Native I/O certificate: certified_local_file_sink\nfallback_attempted: false\nexternal_engine_invoked: false\nclaim_gate_status: fixture_smoke_only",
            self.start,
            self.end,
            self.step,
            self.column_name,
            self.rows.len(),
            self.output_path.display(),
            self.output_format.as_str(),
        )
    }
}

fn run_generated_user_rows_smoke(
    request: &GeneratedUserRowsSmokeRequest,
) -> Result<GeneratedUserRowsSmokeReport, ShardLoomError> {
    if request.output_path.exists() && !request.allow_overwrite {
        return Err(ShardLoomError::InvalidOperation(format!(
            "output path already exists: {}; pass --allow-overwrite to replace it",
            request.output_path.display()
        )));
    }
    if let Some(parent) = request.output_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|error| {
                ShardLoomError::Message(format!(
                    "failed to create local output directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
    }

    let start = Instant::now();
    let content = render_jsonl(&request.schema, &request.rows)?;
    fs::write(&request.output_path, content.as_bytes()).map_err(|error| {
        ShardLoomError::Message(format!(
            "failed to write local generated-source output {}: {error}",
            request.output_path.display()
        ))
    })?;
    let write_millis = start.elapsed().as_millis();

    let schema_text = canonical_schema(&request.schema);
    let canonical_rows = canonical_rows(&request.schema, &request.rows);
    let output_digest = fnv64_digest(&content);
    Ok(GeneratedUserRowsSmokeReport {
        output_path: request.output_path.clone(),
        output_format: request.output_format,
        schema: request.schema.clone(),
        rows: request.rows.clone(),
        output_bytes: u64::try_from(content.len()).unwrap_or(u64::MAX),
        output_digest: output_digest.clone(),
        schema_digest: fnv64_digest(&schema_text),
        plan_digest: fnv64_digest(&format!(
            "generated_source_kind=user_rows;output_format={};schema={schema_text};rows={canonical_rows}",
            request.output_format.as_str()
        )),
        write_millis,
    })
}

fn run_generated_range_smoke(
    request: &GeneratedRangeSmokeRequest,
) -> Result<GeneratedRangeSmokeReport, ShardLoomError> {
    if request.output_path.exists() && !request.allow_overwrite {
        return Err(ShardLoomError::InvalidOperation(format!(
            "output path already exists: {}; pass --allow-overwrite to replace it",
            request.output_path.display()
        )));
    }
    if let Some(parent) = request.output_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|error| {
                ShardLoomError::Message(format!(
                    "failed to create local output directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
    }

    let schema = vec![GeneratedColumn {
        name: request.column_name.clone(),
        value_type: GeneratedValueType::Int64,
    }];
    let rows = generated_range_rows(request.start, request.end, request.step)?;
    let start = Instant::now();
    let content = render_jsonl(&schema, &rows)?;
    fs::write(&request.output_path, content.as_bytes()).map_err(|error| {
        ShardLoomError::Message(format!(
            "failed to write local generated-source output {}: {error}",
            request.output_path.display()
        ))
    })?;
    let write_millis = start.elapsed().as_millis();

    let schema_text = canonical_schema(&schema);
    let output_digest = fnv64_digest(&content);
    Ok(GeneratedRangeSmokeReport {
        output_path: request.output_path.clone(),
        output_format: request.output_format,
        start: request.start,
        end: request.end,
        step: request.step,
        column_name: request.column_name.clone(),
        schema,
        rows,
        output_bytes: u64::try_from(content.len()).unwrap_or(u64::MAX),
        output_digest: output_digest.clone(),
        schema_digest: fnv64_digest(&schema_text),
        plan_digest: fnv64_digest(&format!(
            "generated_source_kind=range;output_format={};start={};end={};step={};column={}",
            request.output_format.as_str(),
            request.start,
            request.end,
            request.step,
            request.column_name
        )),
        write_millis,
    })
}

fn parse_i64_arg(name: &str, value: &str) -> Result<i64, ShardLoomError> {
    value.trim().parse::<i64>().map_err(|_| {
        ShardLoomError::InvalidOperation(format!(
            "generated-source range {name} value {value:?} is not a valid int64"
        ))
    })
}

fn range_row_count(start: i64, end: i64, step: i64) -> Result<usize, ShardLoomError> {
    if step == 0 {
        return Err(ShardLoomError::InvalidOperation(
            "generated-source range step must not be zero".to_string(),
        ));
    }
    if (step > 0 && start >= end) || (step < 0 && start <= end) {
        return Ok(0);
    }
    let mut current = start;
    let mut count = 0_usize;
    while (step > 0 && current < end) || (step < 0 && current > end) {
        count = count.checked_add(1).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "generated-source range row count overflowed".to_string(),
            )
        })?;
        if count > MAX_GENERATED_RANGE_ROWS {
            return Ok(count);
        }
        current = current.checked_add(step).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "generated-source range step overflows int64 before reaching end".to_string(),
            )
        })?;
    }
    Ok(count)
}

fn generated_range_rows(
    start: i64,
    end: i64,
    step: i64,
) -> Result<Vec<GeneratedRow>, ShardLoomError> {
    let row_count = range_row_count(start, end, step)?;
    if row_count > MAX_GENERATED_RANGE_ROWS {
        return Err(ShardLoomError::InvalidOperation(format!(
            "generated-source range row count {row_count} exceeds scoped smoke limit {MAX_GENERATED_RANGE_ROWS}"
        )));
    }
    let mut rows = Vec::with_capacity(row_count);
    let mut current = start;
    while (step > 0 && current < end) || (step < 0 && current > end) {
        rows.push(GeneratedRow {
            values: vec![current.to_string()],
        });
        current = current.checked_add(step).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "generated-source range step overflows int64 before reaching end".to_string(),
            )
        })?;
    }
    Ok(rows)
}

fn normalize_local_output_path(value: &str) -> Result<PathBuf, ShardLoomError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "generated-source output path must not be empty".to_string(),
        ));
    }
    if trimmed.contains("://") && !trimmed.starts_with("file://") {
        return Err(ShardLoomError::InvalidOperation(
            "scoped generated-source smokes support local file output only; object-store and remote URI writes remain blocked".to_string(),
        ));
    }
    let local = trimmed.strip_prefix("file://").unwrap_or(trimmed);
    if local.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "file:// generated-source output path must include a local path".to_string(),
        ));
    }
    Ok(Path::new(local).to_path_buf())
}

fn parse_schema(raw: &str) -> Result<Vec<GeneratedColumn>, ShardLoomError> {
    if raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "generated-source schema must not be empty".to_string(),
        ));
    }
    let mut columns = Vec::new();
    for token in raw.split(',') {
        let Some((name_raw, type_raw)) = token.split_once(':') else {
            return Err(ShardLoomError::InvalidOperation(format!(
                "invalid generated-source schema token {token:?}; expected name:type"
            )));
        };
        let name = percent_decode(name_raw)?;
        if name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "generated-source schema column names must not be empty".to_string(),
            ));
        }
        if columns
            .iter()
            .any(|column: &GeneratedColumn| column.name == name)
        {
            return Err(ShardLoomError::InvalidOperation(format!(
                "duplicate generated-source schema column {name:?}"
            )));
        }
        columns.push(GeneratedColumn {
            name,
            value_type: GeneratedValueType::parse(type_raw)?,
        });
    }
    Ok(columns)
}

fn parse_rows(raw: &str, schema: &[GeneratedColumn]) -> Result<Vec<GeneratedRow>, ShardLoomError> {
    if raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "generated-source rows must not be empty".to_string(),
        ));
    }
    raw.split(';')
        .map(|row_raw| parse_row(row_raw, schema))
        .collect()
}

fn parse_row(raw: &str, schema: &[GeneratedColumn]) -> Result<GeneratedRow, ShardLoomError> {
    if raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "generated-source row entries must not be empty".to_string(),
        ));
    }
    let mut values = BTreeMap::new();
    for pair in raw.split(',') {
        let Some((name_raw, value_raw)) = pair.split_once('=') else {
            return Err(ShardLoomError::InvalidOperation(format!(
                "invalid generated-source row token {pair:?}; expected name=value"
            )));
        };
        let name = percent_decode(name_raw)?;
        let value = percent_decode(value_raw)?;
        if values.insert(name.clone(), value).is_some() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "duplicate generated-source row value for column {name:?}"
            )));
        }
    }
    let mut ordered = Vec::with_capacity(schema.len());
    for column in schema {
        let Some(value) = values.remove(&column.name) else {
            return Err(ShardLoomError::InvalidOperation(format!(
                "generated-source row missing column {:?}",
                column.name
            )));
        };
        validate_value(&value, column.value_type)?;
        ordered.push(value);
    }
    if let Some(extra) = values.keys().next() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "generated-source row contains unknown column {extra:?}"
        )));
    }
    Ok(GeneratedRow { values: ordered })
}

fn validate_value(value: &str, value_type: GeneratedValueType) -> Result<(), ShardLoomError> {
    match value_type {
        GeneratedValueType::Int64 => {
            value.parse::<i64>().map_err(|_| {
                ShardLoomError::InvalidOperation(format!(
                    "generated-source int64 value {value:?} is invalid"
                ))
            })?;
        }
        GeneratedValueType::Float64 => {
            let parsed = value.parse::<f64>().map_err(|_| {
                ShardLoomError::InvalidOperation(format!(
                    "generated-source float64 value {value:?} is invalid"
                ))
            })?;
            if !parsed.is_finite() {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "generated-source float64 value {value:?} must be finite"
                )));
            }
        }
        GeneratedValueType::Bool => {
            if !matches!(value, "true" | "false") {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "generated-source bool value {value:?} must be true or false"
                )));
            }
        }
        GeneratedValueType::Utf8 => {}
    }
    Ok(())
}

fn render_jsonl(
    schema: &[GeneratedColumn],
    rows: &[GeneratedRow],
) -> Result<String, ShardLoomError> {
    let mut output = String::new();
    for row in rows {
        output.push('{');
        for (index, column) in schema.iter().enumerate() {
            if index > 0 {
                output.push(',');
            }
            output.push('"');
            output.push_str(&json_escape(&column.name));
            output.push_str("\":");
            let value = row.values.get(index).ok_or_else(|| {
                ShardLoomError::InvalidOperation("generated-source row/schema mismatch".to_string())
            })?;
            output.push_str(&render_json_value(value, column.value_type)?);
        }
        output.push_str("}\n");
    }
    Ok(output)
}

fn render_json_value(
    value: &str,
    value_type: GeneratedValueType,
) -> Result<String, ShardLoomError> {
    match value_type {
        GeneratedValueType::Int64 => Ok(value.parse::<i64>().expect("validated int64").to_string()),
        GeneratedValueType::Float64 => {
            let parsed = value.parse::<f64>().expect("validated float64");
            if !parsed.is_finite() {
                return Err(ShardLoomError::InvalidOperation(
                    "generated-source float64 value must be finite".to_string(),
                ));
            }
            Ok(parsed.to_string())
        }
        GeneratedValueType::Bool => Ok(value.to_string()),
        GeneratedValueType::Utf8 => Ok(format!("\"{}\"", json_escape(value))),
    }
}

fn canonical_schema(schema: &[GeneratedColumn]) -> String {
    schema
        .iter()
        .map(|column| format!("{}:{}", column.name, column.value_type.as_str()))
        .collect::<Vec<_>>()
        .join(",")
}

fn canonical_rows(schema: &[GeneratedColumn], rows: &[GeneratedRow]) -> String {
    rows.iter()
        .map(|row| {
            schema
                .iter()
                .zip(row.values.iter())
                .map(|(column, value)| format!("{}={value}", column.name))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn percent_decode(value: &str) -> Result<String, ShardLoomError> {
    let mut bytes = Vec::with_capacity(value.len());
    let raw = value.as_bytes();
    let mut index = 0;
    while index < raw.len() {
        if raw[index] == b'%' {
            if index + 2 >= raw.len() {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "invalid percent escape in generated-source token {value:?}"
                )));
            }
            let high = from_hex(raw[index + 1])?;
            let low = from_hex(raw[index + 2])?;
            bytes.push((high << 4) | low);
            index += 3;
        } else {
            bytes.push(raw[index]);
            index += 1;
        }
    }
    String::from_utf8(bytes).map_err(|_| {
        ShardLoomError::InvalidOperation(format!(
            "generated-source token {value:?} is not valid UTF-8 after percent decoding"
        ))
    })
}

fn from_hex(value: u8) -> Result<u8, ShardLoomError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(ShardLoomError::InvalidOperation(
            "invalid hex digit in generated-source percent escape".to_string(),
        )),
    }
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            '\u{08}' => escaped.push_str("\\b"),
            '\u{0C}' => escaped.push_str("\\f"),
            character if character.is_control() => {
                write!(&mut escaped, "\\u{:04x}", u32::from(character))
                    .expect("writing to String cannot fail");
            }
            character => escaped.push(character),
        }
    }
    escaped
}

fn fnv64_digest(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv64:{hash:016x}")
}
