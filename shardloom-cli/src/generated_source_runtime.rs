//! Scoped generated-source runtime smoke handlers.
//!
//! This module implements deliberately narrow local generated-output smokes. It
//! accepts either rows already supplied by the user/API layer or narrow
//! ShardLoom-native integer generators, writes a local JSONL sink, and emits
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
const SEQUENCE_COMMAND: &str = "generated-source-sequence-smoke";
const SEQUENCE_SCHEMA_VERSION: &str = "shardloom.generated_source_sequence_smoke.v1";
const SEQUENCE_GENERATED_SOURCE_CERTIFICATE_ID: &str = "generated-source.sequence.local-output.v1";
const SEQUENCE_OUTPUT_NATIVE_IO_CERTIFICATE_ID: &str =
    "generated-source.sequence.local-output.native-io.v1";
const SEQUENCE_EXECUTION_CERTIFICATE_ID: &str =
    "generated-source.sequence.local-output.execution.v1";
const MAX_GENERATED_RANGE_ROWS: usize = 1_000_000;

const SQL_COMMAND: &str = "generated-source-sql-smoke";
const SQL_SCHEMA_VERSION: &str = "shardloom.generated_source_sql_smoke.v1";
const SQL_GENERATED_SOURCE_CERTIFICATE_ID: &str = "generated-source.sql.local-output.v1";
const SQL_OUTPUT_NATIVE_IO_CERTIFICATE_ID: &str = "generated-source.sql.local-output.native-io.v1";
const SQL_EXECUTION_CERTIFICATE_ID: &str = "generated-source.sql.local-output.execution.v1";
const MAX_SQL_GENERATED_ROWS: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UserRowsGeneratedSourceKind {
    UserRows,
    LiteralTable,
    Calendar,
}

impl UserRowsGeneratedSourceKind {
    fn parse(value: &str) -> Result<Self, ShardLoomError> {
        match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "user_rows" | "rows" => Ok(Self::UserRows),
            "literal_table" | "literal" => Ok(Self::LiteralTable),
            "calendar" | "date_dimension" => Ok(Self::Calendar),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unsupported generated-source user rows source kind {other:?}; supported kinds are user_rows,literal_table,calendar"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::UserRows => "user_rows",
            Self::LiteralTable => "literal_table",
            Self::Calendar => "calendar",
        }
    }

    const fn materialization_boundary(self) -> &'static str {
        match self {
            Self::UserRows => "python_user_rows_to_local_jsonl_sink",
            Self::LiteralTable => "python_literal_table_to_local_jsonl_sink",
            Self::Calendar => "python_calendar_generator_to_local_jsonl_sink",
        }
    }

    const fn claim_gate_reason(self) -> &'static str {
        match self {
            Self::UserRows => "one_scoped_local_user_rows_generated_output_smoke",
            Self::LiteralTable => "one_scoped_local_literal_table_generated_output_smoke",
            Self::Calendar => "one_scoped_local_calendar_generated_output_smoke",
        }
    }
}

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
    source_kind: UserRowsGeneratedSourceKind,
    schema: Vec<GeneratedColumn>,
    rows: Vec<GeneratedRow>,
    allow_overwrite: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedUserRowsSmokeReport {
    output_path: PathBuf,
    output_format: GeneratedOutputFormat,
    source_kind: UserRowsGeneratedSourceKind,
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
    source_kind: RangeGeneratedSourceKind,
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
    source_kind: RangeGeneratedSourceKind,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeGeneratedSourceKind {
    Range,
    Sequence,
}

impl RangeGeneratedSourceKind {
    const fn command(self) -> &'static str {
        match self {
            Self::Range => RANGE_COMMAND,
            Self::Sequence => SEQUENCE_COMMAND,
        }
    }

    const fn schema_version(self) -> &'static str {
        match self {
            Self::Range => RANGE_SCHEMA_VERSION,
            Self::Sequence => SEQUENCE_SCHEMA_VERSION,
        }
    }

    const fn generated_source_certificate_id(self) -> &'static str {
        match self {
            Self::Range => RANGE_GENERATED_SOURCE_CERTIFICATE_ID,
            Self::Sequence => SEQUENCE_GENERATED_SOURCE_CERTIFICATE_ID,
        }
    }

    const fn output_native_io_certificate_id(self) -> &'static str {
        match self {
            Self::Range => RANGE_OUTPUT_NATIVE_IO_CERTIFICATE_ID,
            Self::Sequence => SEQUENCE_OUTPUT_NATIVE_IO_CERTIFICATE_ID,
        }
    }

    const fn execution_certificate_id(self) -> &'static str {
        match self {
            Self::Range => RANGE_EXECUTION_CERTIFICATE_ID,
            Self::Sequence => SEQUENCE_EXECUTION_CERTIFICATE_ID,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Range => "range",
            Self::Sequence => "sequence",
        }
    }

    const fn materialization_boundary(self) -> &'static str {
        match self {
            Self::Range => "engine_native_range_generator_to_local_jsonl_sink",
            Self::Sequence => "engine_native_sequence_generator_to_local_jsonl_sink",
        }
    }

    const fn claim_gate_reason(self) -> &'static str {
        match self {
            Self::Range => "one_scoped_local_range_generated_output_smoke",
            Self::Sequence => "one_scoped_local_sequence_generated_output_smoke",
        }
    }

    const fn summary_noun(self) -> &'static str {
        match self {
            Self::Range => "range",
            Self::Sequence => "sequence",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlGeneratedSourceKind {
    LiteralSelect,
    Values,
    GenerateSeriesRange,
}

impl SqlGeneratedSourceKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::LiteralSelect => "sql_literal_select",
            Self::Values => "sql_values",
            Self::GenerateSeriesRange => "sql_generate_series_range",
        }
    }

    const fn materialization_boundary(self) -> &'static str {
        match self {
            Self::LiteralSelect => "sql_literal_select_to_local_jsonl_sink",
            Self::Values => "sql_values_to_local_jsonl_sink",
            Self::GenerateSeriesRange => "sql_generate_series_range_to_local_jsonl_sink",
        }
    }

    const fn claim_gate_reason(self) -> &'static str {
        match self {
            Self::LiteralSelect => "one_scoped_local_sql_literal_select_generated_output_smoke",
            Self::Values => "one_scoped_local_sql_values_generated_output_smoke",
            Self::GenerateSeriesRange => {
                "one_scoped_local_sql_generate_series_range_generated_output_smoke"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedSqlRangeMetadata {
    function_name: String,
    start: i64,
    end: i64,
    step: i64,
    column_name: String,
    end_inclusive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedSqlSmokeRequest {
    output_path: PathBuf,
    output_format: GeneratedOutputFormat,
    statement: String,
    source_kind: SqlGeneratedSourceKind,
    schema: Vec<GeneratedColumn>,
    rows: Vec<GeneratedRow>,
    range: Option<GeneratedSqlRangeMetadata>,
    allow_overwrite: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedSqlSmokeReport {
    output_path: PathBuf,
    output_format: GeneratedOutputFormat,
    statement: String,
    source_kind: SqlGeneratedSourceKind,
    schema: Vec<GeneratedColumn>,
    rows: Vec<GeneratedRow>,
    range: Option<GeneratedSqlRangeMetadata>,
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
            "usage: shardloom {USER_ROWS_COMMAND} <local-output-path> <schema> <rows> [--source-kind user_rows|literal_table|calendar] [--output-format jsonl] [--allow-overwrite]"
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
    let mut source_kind = UserRowsGeneratedSourceKind::UserRows;
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
            "--source-kind" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        USER_ROWS_COMMAND,
                        format,
                        "generated-source smoke failed",
                        &ShardLoomError::InvalidOperation(
                            "--source-kind requires a value".to_string(),
                        ),
                    );
                };
                source_kind = match UserRowsGeneratedSourceKind::parse(&value) {
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
        source_kind,
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
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    handle_generated_source_range_like_smoke(args, format, RangeGeneratedSourceKind::Range)
}

pub(crate) fn handle_generated_source_sequence_smoke(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    handle_generated_source_range_like_smoke(args, format, RangeGeneratedSourceKind::Sequence)
}

#[allow(clippy::too_many_lines)]
fn handle_generated_source_range_like_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
    source_kind: RangeGeneratedSourceKind,
) -> ExitCode {
    let command = source_kind.command();
    let noun = source_kind.summary_noun();
    let Some(output_target) = args.next() else {
        eprintln!(
            "usage: shardloom {command} <local-output-path> <start> <end> [--step int] [--column name] [--output-format jsonl] [--allow-overwrite]"
        );
        return ExitCode::from(2);
    };
    let Some(start_raw) = args.next() else {
        return emit_error(
            command,
            format,
            &format!("generated-source {noun} smoke failed"),
            &ShardLoomError::InvalidOperation(format!(
                "generated-source {noun} smoke requires a start argument"
            )),
        );
    };
    let Some(end_raw) = args.next() else {
        return emit_error(
            command,
            format,
            &format!("generated-source {noun} smoke failed"),
            &ShardLoomError::InvalidOperation(format!(
                "generated-source {noun} smoke requires an end argument"
            )),
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
                        command,
                        format,
                        &format!("generated-source {noun} smoke failed"),
                        &ShardLoomError::InvalidOperation(
                            "--output-format requires a value".to_string(),
                        ),
                    );
                };
                output_format = match GeneratedOutputFormat::parse(&value) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return emit_error(
                            command,
                            format,
                            &format!("generated-source {noun} smoke failed"),
                            &error,
                        );
                    }
                };
            }
            "--allow-overwrite" => allow_overwrite = true,
            "--step" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        command,
                        format,
                        &format!("generated-source {noun} smoke failed"),
                        &ShardLoomError::InvalidOperation("--step requires a value".to_string()),
                    );
                };
                step = match parse_i64_arg("step", &value) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return emit_error(
                            command,
                            format,
                            &format!("generated-source {noun} smoke failed"),
                            &error,
                        );
                    }
                };
            }
            "--column" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        command,
                        format,
                        &format!("generated-source {noun} smoke failed"),
                        &ShardLoomError::InvalidOperation("--column requires a value".to_string()),
                    );
                };
                column_name = match percent_decode(&value) {
                    Ok(parsed) if !parsed.trim().is_empty() => parsed,
                    Ok(_) => {
                        return emit_error(
                            command,
                            format,
                            &format!("generated-source {noun} smoke failed"),
                            &ShardLoomError::InvalidOperation(format!(
                                "generated-source {noun} column must not be empty"
                            )),
                        );
                    }
                    Err(error) => {
                        return emit_error(
                            command,
                            format,
                            &format!("generated-source {noun} smoke failed"),
                            &error,
                        );
                    }
                };
            }
            extra => {
                return emit_error(
                    command,
                    format,
                    &format!("generated-source {noun} smoke failed"),
                    &cli_unknown_arg_error(command, extra),
                );
            }
        }
    }

    let start = match parse_i64_arg("start", &start_raw) {
        Ok(parsed) => parsed,
        Err(error) => {
            return emit_error(
                command,
                format,
                &format!("generated-source {noun} smoke failed"),
                &error,
            );
        }
    };
    let end = match parse_i64_arg("end", &end_raw) {
        Ok(parsed) => parsed,
        Err(error) => {
            return emit_error(
                command,
                format,
                &format!("generated-source {noun} smoke failed"),
                &error,
            );
        }
    };
    let request = match GeneratedRangeSmokeRequest::parse(
        &output_target,
        output_format,
        source_kind,
        start,
        end,
        step,
        column_name,
        allow_overwrite,
    ) {
        Ok(request) => request,
        Err(error) => {
            return emit_error(
                command,
                format,
                &format!("generated-source {noun} smoke failed"),
                &error,
            );
        }
    };

    let report = match run_generated_range_smoke(&request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                command,
                format,
                &format!("generated-source {noun} smoke failed"),
                &error,
            );
        }
    };

    emit(
        command,
        format,
        CommandStatus::Success,
        format!(
            "generated {noun} local-output smoke wrote {} row(s)",
            report.rows.len()
        ),
        report.to_text(),
        vec![],
        report.fields(),
    );
    ExitCode::SUCCESS
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_generated_source_sql_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(output_target) = args.next() else {
        eprintln!(
            "usage: shardloom {SQL_COMMAND} <local-output-path> <sql-statement> [--output-format jsonl] [--allow-overwrite]"
        );
        return ExitCode::from(2);
    };
    let Some(statement_raw) = args.next() else {
        return emit_error(
            SQL_COMMAND,
            format,
            "generated-source SQL smoke failed",
            &ShardLoomError::InvalidOperation(
                "generated-source SQL smoke requires a SQL statement argument".to_string(),
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
                        SQL_COMMAND,
                        format,
                        "generated-source SQL smoke failed",
                        &ShardLoomError::InvalidOperation(
                            "--output-format requires a value".to_string(),
                        ),
                    );
                };
                output_format = match GeneratedOutputFormat::parse(&value) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return emit_error(
                            SQL_COMMAND,
                            format,
                            "generated-source SQL smoke failed",
                            &error,
                        );
                    }
                };
            }
            "--allow-overwrite" => allow_overwrite = true,
            extra => {
                return emit_error(
                    SQL_COMMAND,
                    format,
                    "generated-source SQL smoke failed",
                    &cli_unknown_arg_error(SQL_COMMAND, extra),
                );
            }
        }
    }

    let request = match GeneratedSqlSmokeRequest::parse(
        &output_target,
        output_format,
        &statement_raw,
        allow_overwrite,
    ) {
        Ok(request) => request,
        Err(error) => {
            return emit_error(
                SQL_COMMAND,
                format,
                "generated-source SQL smoke failed",
                &error,
            );
        }
    };

    let report = match run_generated_sql_smoke(&request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                SQL_COMMAND,
                format,
                "generated-source SQL smoke failed",
                &error,
            );
        }
    };

    emit(
        SQL_COMMAND,
        format,
        CommandStatus::Success,
        format!(
            "generated SQL source-free local-output smoke wrote {} row(s)",
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
        source_kind: UserRowsGeneratedSourceKind,
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
            source_kind,
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
            (
                "generated_source_kind".to_string(),
                self.source_kind.as_str().to_string(),
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
                self.source_kind.materialization_boundary().to_string(),
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
                self.source_kind.claim_gate_reason().to_string(),
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
            "generated-source user rows smoke\nschema_version: {USER_ROWS_SCHEMA_VERSION}\ngenerated_source_kind: {}\nschema: {}\nrows: {}\noutput: {}\noutput format: {}\ngenerated source certificate: present\noutput Native I/O certificate: certified_local_file_sink\nfallback_attempted: false\nexternal_engine_invoked: false\nclaim_gate_status: fixture_smoke_only",
            self.source_kind.as_str(),
            canonical_schema(&self.schema),
            self.rows.len(),
            self.output_path.display(),
            self.output_format.as_str(),
        )
    }
}

impl GeneratedRangeSmokeRequest {
    #[allow(clippy::too_many_arguments)]
    fn parse(
        output_target: &str,
        output_format: GeneratedOutputFormat,
        source_kind: RangeGeneratedSourceKind,
        start: i64,
        end: i64,
        step: i64,
        column_name: String,
        allow_overwrite: bool,
    ) -> Result<Self, ShardLoomError> {
        if step == 0 {
            return Err(ShardLoomError::InvalidOperation(format!(
                "generated-source {} step must not be zero",
                source_kind.as_str()
            )));
        }
        if column_name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "generated-source {} column must not be empty",
                source_kind.as_str()
            )));
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
            source_kind,
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
                self.source_kind.schema_version().to_string(),
            ),
            (
                "generated_source_smoke_report_id".to_string(),
                self.source_kind
                    .generated_source_certificate_id()
                    .to_string(),
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
            (
                "generated_source_kind".to_string(),
                self.source_kind.as_str().to_string(),
            ),
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
                self.source_kind
                    .generated_source_certificate_id()
                    .to_string(),
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
                self.source_kind
                    .output_native_io_certificate_id()
                    .to_string(),
            ),
            (
                "execution_certificate_status".to_string(),
                "certified".to_string(),
            ),
            (
                "execution_certificate_id".to_string(),
                self.source_kind.execution_certificate_id().to_string(),
            ),
            ("correctness_digest".to_string(), self.output_digest.clone()),
            (
                "materialization_boundary".to_string(),
                self.source_kind.materialization_boundary().to_string(),
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
                self.source_kind.claim_gate_reason().to_string(),
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
            "generated-source {} smoke\nschema_version: {}\n{}: {}..{} step {}\ncolumn: {}\nrows: {}\noutput: {}\noutput format: {}\ngenerated source certificate: present\noutput Native I/O certificate: certified_local_file_sink\nfallback_attempted: false\nexternal_engine_invoked: false\nclaim_gate_status: fixture_smoke_only",
            self.source_kind.summary_noun(),
            self.source_kind.schema_version(),
            self.source_kind.summary_noun(),
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

impl GeneratedSqlSmokeRequest {
    fn parse(
        output_target: &str,
        output_format: GeneratedOutputFormat,
        statement_raw: &str,
        allow_overwrite: bool,
    ) -> Result<Self, ShardLoomError> {
        let output_path = normalize_local_output_path(output_target)?;
        let parsed = parse_source_free_sql(statement_raw)?;
        if parsed.rows.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "generated-source SQL smoke produced no rows; scoped SQL smokes require at least one row".to_string(),
            ));
        }
        if parsed.rows.len() > MAX_SQL_GENERATED_ROWS {
            return Err(ShardLoomError::InvalidOperation(format!(
                "generated-source SQL row count {} exceeds scoped smoke limit {MAX_SQL_GENERATED_ROWS}",
                parsed.rows.len()
            )));
        }
        Ok(Self {
            output_path,
            output_format,
            statement: parsed.statement,
            source_kind: parsed.source_kind,
            schema: parsed.schema,
            rows: parsed.rows,
            range: parsed.range,
            allow_overwrite,
        })
    }
}

impl GeneratedSqlSmokeReport {
    #[allow(clippy::too_many_lines)]
    fn fields(&self) -> Vec<(String, String)> {
        let mut fields = vec![
            ("schema_version".to_string(), SQL_SCHEMA_VERSION.to_string()),
            (
                "generated_source_smoke_report_id".to_string(),
                SQL_GENERATED_SOURCE_CERTIFICATE_ID.to_string(),
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
            ("sql_parser_executed".to_string(), "true".to_string()),
            ("sql_binder_executed".to_string(), "true".to_string()),
            ("sql_planner_executed".to_string(), "true".to_string()),
            ("sql_runtime_execution".to_string(), "true".to_string()),
            (
                "sql_statement_kind".to_string(),
                self.source_kind.as_str().to_string(),
            ),
            ("sql_statement".to_string(), self.statement.clone()),
            ("generated_source_created".to_string(), "true".to_string()),
            (
                "generated_source_kind".to_string(),
                self.source_kind.as_str().to_string(),
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
                SQL_GENERATED_SOURCE_CERTIFICATE_ID.to_string(),
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
                SQL_OUTPUT_NATIVE_IO_CERTIFICATE_ID.to_string(),
            ),
            (
                "execution_certificate_status".to_string(),
                "certified".to_string(),
            ),
            (
                "execution_certificate_id".to_string(),
                SQL_EXECUTION_CERTIFICATE_ID.to_string(),
            ),
            ("correctness_digest".to_string(), self.output_digest.clone()),
            (
                "materialization_boundary".to_string(),
                self.source_kind.materialization_boundary().to_string(),
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
                self.source_kind.claim_gate_reason().to_string(),
            ),
            (
                "sql_source_free_runtime_smoke_supported".to_string(),
                "true".to_string(),
            ),
            (
                "sql_production_runtime_claim_allowed".to_string(),
                "false".to_string(),
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
        ];
        if let Some(range) = &self.range {
            fields.extend([
                (
                    "generated_source_range_start".to_string(),
                    range.start.to_string(),
                ),
                (
                    "generated_source_range_end".to_string(),
                    range.end.to_string(),
                ),
                (
                    "generated_source_range_step".to_string(),
                    range.step.to_string(),
                ),
                (
                    "generated_source_range_column".to_string(),
                    range.column_name.clone(),
                ),
                (
                    "generated_source_sql_generator_function".to_string(),
                    range.function_name.clone(),
                ),
                (
                    "generated_source_range_end_inclusive".to_string(),
                    range.end_inclusive.to_string(),
                ),
            ]);
        }
        fields
    }

    fn to_text(&self) -> String {
        format!(
            "generated-source SQL smoke\nschema_version: {SQL_SCHEMA_VERSION}\nsql_statement_kind: {}\nschema: {}\nrows: {}\noutput: {}\noutput format: {}\ngenerated source certificate: present\noutput Native I/O certificate: certified_local_file_sink\nfallback_attempted: false\nexternal_engine_invoked: false\nclaim_gate_status: fixture_smoke_only",
            self.source_kind.as_str(),
            canonical_schema(&self.schema),
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
        source_kind: request.source_kind,
        schema: request.schema.clone(),
        rows: request.rows.clone(),
        output_bytes: u64::try_from(content.len()).unwrap_or(u64::MAX),
        output_digest: output_digest.clone(),
        schema_digest: fnv64_digest(&schema_text),
        plan_digest: fnv64_digest(&format!(
            "generated_source_kind={};output_format={};schema={schema_text};rows={canonical_rows}",
            request.source_kind.as_str(),
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
        source_kind: request.source_kind,
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
            "generated_source_kind={};output_format={};start={};end={};step={};column={}",
            request.source_kind.as_str(),
            request.output_format.as_str(),
            request.start,
            request.end,
            request.step,
            request.column_name
        )),
        write_millis,
    })
}

fn run_generated_sql_smoke(
    request: &GeneratedSqlSmokeRequest,
) -> Result<GeneratedSqlSmokeReport, ShardLoomError> {
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
            "failed to write local generated-source SQL output {}: {error}",
            request.output_path.display()
        ))
    })?;
    let write_millis = start.elapsed().as_millis();

    let schema_text = canonical_schema(&request.schema);
    let canonical_rows = canonical_rows(&request.schema, &request.rows);
    let output_digest = fnv64_digest(&content);
    Ok(GeneratedSqlSmokeReport {
        output_path: request.output_path.clone(),
        output_format: request.output_format,
        statement: request.statement.clone(),
        source_kind: request.source_kind,
        schema: request.schema.clone(),
        rows: request.rows.clone(),
        range: request.range.clone(),
        output_bytes: u64::try_from(content.len()).unwrap_or(u64::MAX),
        output_digest: output_digest.clone(),
        schema_digest: fnv64_digest(&schema_text),
        plan_digest: fnv64_digest(&format!(
            "generated_source_kind={};output_format={};statement={};schema={schema_text};rows={canonical_rows}",
            request.source_kind.as_str(),
            request.output_format.as_str(),
            request.statement
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
    let distance = if step > 0 {
        i128::from(end) - i128::from(start)
    } else {
        i128::from(start) - i128::from(end)
    };
    let stride = if step > 0 {
        i128::from(step)
    } else {
        -i128::from(step)
    };
    let count = (distance + stride - 1) / stride;
    usize::try_from(count).map_err(|_| {
        ShardLoomError::InvalidOperation("generated-source range row count overflowed".to_string())
    })
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
    for index in 0..row_count {
        rows.push(GeneratedRow {
            values: vec![current.to_string()],
        });
        if index + 1 < row_count {
            current = current.checked_add(step).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "generated-source range step overflows int64 before reaching end".to_string(),
                )
            })?;
        }
    }
    Ok(rows)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedSourceFreeSql {
    statement: String,
    source_kind: SqlGeneratedSourceKind,
    schema: Vec<GeneratedColumn>,
    rows: Vec<GeneratedRow>,
    range: Option<GeneratedSqlRangeMetadata>,
}

fn parse_source_free_sql(raw: &str) -> Result<ParsedSourceFreeSql, ShardLoomError> {
    let statement = normalize_sql_statement(raw)?;
    if keyword_prefix(&statement, "SELECT") {
        if let Some(parsed) = parse_sql_generate_series_range(&statement)? {
            return Ok(parsed);
        }
        parse_sql_literal_select(&statement)
    } else if keyword_prefix(&statement, "VALUES") {
        parse_sql_values(&statement)
    } else {
        Err(unsupported_sql_error(
            "source-free SQL smoke supports only SELECT literal expressions, VALUES clauses, and SELECT * FROM generate_series/range(...)",
        ))
    }
}

fn parse_sql_literal_select(statement: &str) -> Result<ParsedSourceFreeSql, ShardLoomError> {
    let select_list = statement["SELECT".len()..].trim();
    if select_list.is_empty() {
        return Err(unsupported_sql_error(
            "SQL literal SELECT requires at least one literal expression",
        ));
    }
    if contains_keyword_outside_quotes(select_list, "FROM") {
        return Err(unsupported_sql_error(
            "SQL literal SELECT smoke does not admit FROM clauses or input datasets",
        ));
    }
    if contains_outside_quotes(select_list, '(') || contains_outside_quotes(select_list, ')') {
        return Err(unsupported_sql_error(
            "SQL literal SELECT smoke does not admit functions, subqueries, or parenthesized expressions",
        ));
    }

    let items = split_sql_csv(select_list)?;
    if items.is_empty() {
        return Err(unsupported_sql_error(
            "SQL literal SELECT requires at least one literal expression",
        ));
    }
    let mut schema = Vec::with_capacity(items.len());
    let mut values = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let (literal, alias) = split_select_alias(item, index + 1)?;
        let (value_type, value) = parse_sql_literal(literal.trim())?;
        if schema
            .iter()
            .any(|column: &GeneratedColumn| column.name == alias)
        {
            return Err(unsupported_sql_error(
                "SQL literal SELECT aliases must be unique",
            ));
        }
        schema.push(GeneratedColumn {
            name: alias,
            value_type,
        });
        values.push(value);
    }
    Ok(ParsedSourceFreeSql {
        statement: statement.to_string(),
        source_kind: SqlGeneratedSourceKind::LiteralSelect,
        schema,
        rows: vec![GeneratedRow { values }],
        range: None,
    })
}

fn parse_sql_values(statement: &str) -> Result<ParsedSourceFreeSql, ShardLoomError> {
    let values_body = statement["VALUES".len()..].trim();
    if values_body.is_empty() {
        return Err(unsupported_sql_error(
            "SQL VALUES smoke requires at least one row tuple",
        ));
    }
    let raw_rows = parse_values_tuples(values_body)?;
    if raw_rows.is_empty() {
        return Err(unsupported_sql_error(
            "SQL VALUES smoke requires at least one row tuple",
        ));
    }
    if raw_rows.len() > MAX_SQL_GENERATED_ROWS {
        return Err(ShardLoomError::InvalidOperation(format!(
            "generated-source SQL row count {} exceeds scoped smoke limit {MAX_SQL_GENERATED_ROWS}",
            raw_rows.len()
        )));
    }

    let first_width = raw_rows[0].len();
    if first_width == 0 {
        return Err(unsupported_sql_error(
            "SQL VALUES row tuples must contain at least one literal",
        ));
    }
    let mut column_types: Vec<GeneratedValueType> = Vec::with_capacity(first_width);
    let mut parsed_rows: Vec<Vec<(GeneratedValueType, String)>> =
        Vec::with_capacity(raw_rows.len());
    for row in raw_rows {
        if row.len() != first_width {
            return Err(unsupported_sql_error(
                "SQL VALUES row tuples must all have the same width",
            ));
        }
        let parsed_row = row
            .iter()
            .map(|literal| parse_sql_literal(literal.trim()))
            .collect::<Result<Vec<_>, _>>()?;
        if column_types.is_empty() {
            column_types.extend(parsed_row.iter().map(|(value_type, _)| *value_type));
        } else {
            for (index, (value_type, _)) in parsed_row.iter().enumerate() {
                column_types[index] = unify_sql_value_type(column_types[index], *value_type)?;
            }
        }
        parsed_rows.push(parsed_row);
    }

    let schema = column_types
        .iter()
        .enumerate()
        .map(|(index, value_type)| GeneratedColumn {
            name: format!("column_{}", index + 1),
            value_type: *value_type,
        })
        .collect::<Vec<_>>();
    let rows = parsed_rows
        .into_iter()
        .map(|row| GeneratedRow {
            values: row.into_iter().map(|(_, value)| value).collect(),
        })
        .collect();
    Ok(ParsedSourceFreeSql {
        statement: statement.to_string(),
        source_kind: SqlGeneratedSourceKind::Values,
        schema,
        rows,
        range: None,
    })
}

fn parse_sql_generate_series_range(
    statement: &str,
) -> Result<Option<ParsedSourceFreeSql>, ShardLoomError> {
    let select_body = statement["SELECT".len()..].trim();
    let Some(after_star) = select_body.strip_prefix('*') else {
        return Ok(None);
    };
    let after_star = after_star.trim();
    if !keyword_prefix(after_star, "FROM") {
        return Ok(None);
    }
    let source_ref = after_star["FROM".len()..].trim();
    let Some(range) = parse_sql_range_function_ref(source_ref)? else {
        return Ok(None);
    };
    let schema = vec![GeneratedColumn {
        name: range.column_name.clone(),
        value_type: GeneratedValueType::Int64,
    }];
    let rows = if range.end_inclusive {
        generated_inclusive_series_rows(range.start, range.end, range.step)?
    } else {
        let row_count = range_row_count(range.start, range.end, range.step)?;
        if row_count > MAX_SQL_GENERATED_ROWS {
            return Err(ShardLoomError::InvalidOperation(format!(
                "generated-source SQL row count {row_count} exceeds scoped smoke limit {MAX_SQL_GENERATED_ROWS}"
            )));
        }
        generated_range_rows(range.start, range.end, range.step)?
    };
    Ok(Some(ParsedSourceFreeSql {
        statement: statement.to_string(),
        source_kind: SqlGeneratedSourceKind::GenerateSeriesRange,
        schema,
        rows,
        range: Some(range),
    }))
}

fn parse_sql_range_function_ref(
    raw: &str,
) -> Result<Option<GeneratedSqlRangeMetadata>, ShardLoomError> {
    let trimmed = raw.trim();
    for (function_name, end_inclusive) in [("generate_series", true), ("range", false)] {
        if let Some(rest) = strip_sql_identifier_prefix(trimmed, function_name) {
            let rest = rest.trim();
            if !rest.starts_with('(') || !rest.ends_with(')') {
                return Err(unsupported_sql_error(
                    "SQL source-free range generators require a single function call like generate_series(start, end[, step])",
                ));
            }
            let args = split_sql_csv(rest[1..rest.len() - 1].trim())?;
            if !(2..=3).contains(&args.len()) {
                return Err(unsupported_sql_error(
                    "SQL source-free range generators require start, end, and optional step arguments",
                ));
            }
            let start = parse_sql_range_i64("start", &args[0])?;
            let end = parse_sql_range_i64("end", &args[1])?;
            let step = if let Some(raw_step) = args.get(2) {
                parse_sql_range_i64("step", raw_step)?
            } else {
                1
            };
            if step == 0 {
                return Err(unsupported_sql_error(
                    "SQL source-free range generator step must not be zero",
                ));
            }
            return Ok(Some(GeneratedSqlRangeMetadata {
                function_name: function_name.to_string(),
                start,
                end,
                step,
                column_name: "value".to_string(),
                end_inclusive,
            }));
        }
    }
    Ok(None)
}

fn strip_sql_identifier_prefix<'a>(raw: &'a str, identifier: &str) -> Option<&'a str> {
    let prefix = raw.get(..identifier.len())?;
    if !prefix.eq_ignore_ascii_case(identifier) {
        return None;
    }
    let Some(next) = raw.as_bytes().get(identifier.len()) else {
        return Some(&raw[identifier.len()..]);
    };
    if next.is_ascii_alphanumeric() || *next == b'_' {
        return None;
    }
    Some(&raw[identifier.len()..])
}

fn parse_sql_range_i64(name: &str, raw: &str) -> Result<i64, ShardLoomError> {
    let value = raw.trim();
    if value.is_empty()
        || value.starts_with('+')
        || value.contains('.')
        || value.contains('e')
        || value.contains('E')
        || value.contains('\'')
        || contains_outside_quotes(value, '(')
        || contains_outside_quotes(value, ')')
    {
        return Err(unsupported_sql_error(
            "SQL source-free range generator arguments must be int64 literals",
        ));
    }
    value.parse::<i64>().map_err(|_| {
        unsupported_sql_error(&format!(
            "SQL source-free range generator {name} argument is not a valid int64"
        ))
    })
}

fn generated_inclusive_series_rows(
    start: i64,
    end: i64,
    step: i64,
) -> Result<Vec<GeneratedRow>, ShardLoomError> {
    let mut rows = Vec::new();
    if (step > 0 && start > end) || (step < 0 && start < end) {
        return Ok(rows);
    }
    let mut current = start;
    loop {
        if rows.len() >= MAX_SQL_GENERATED_ROWS {
            return Err(ShardLoomError::InvalidOperation(format!(
                "generated-source SQL row count exceeds scoped smoke limit {MAX_SQL_GENERATED_ROWS}"
            )));
        }
        rows.push(GeneratedRow {
            values: vec![current.to_string()],
        });
        if current == end {
            break;
        }
        let next = current.checked_add(step).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "SQL source-free range generator step overflows int64 before reaching end"
                    .to_string(),
            )
        })?;
        if (step > 0 && next > end) || (step < 0 && next < end) {
            break;
        }
        current = next;
    }
    Ok(rows)
}

fn normalize_sql_statement(raw: &str) -> Result<String, ShardLoomError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(unsupported_sql_error(
            "generated-source SQL statement must not be empty",
        ));
    }
    let mut semicolon_positions = Vec::new();
    let bytes = trimmed.as_bytes();
    let mut in_quote = false;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' => {
                if in_quote && index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                    index += 2;
                    continue;
                }
                in_quote = !in_quote;
            }
            b';' if !in_quote => semicolon_positions.push(index),
            _ => {}
        }
        index += 1;
    }
    if in_quote {
        return Err(unsupported_sql_error(
            "generated-source SQL statement has an unterminated string literal",
        ));
    }
    if semicolon_positions.len() > 1 {
        return Err(unsupported_sql_error(
            "generated-source SQL smoke accepts only one statement",
        ));
    }
    if let Some(position) = semicolon_positions.first().copied() {
        if trimmed[position + 1..].trim().is_empty() {
            Ok(trimmed[..position].trim().to_string())
        } else {
            Err(unsupported_sql_error(
                "generated-source SQL smoke rejects multiple statements",
            ))
        }
    } else {
        Ok(trimmed.to_string())
    }
}

fn keyword_prefix(statement: &str, keyword: &str) -> bool {
    let Some(prefix) = statement.get(..keyword.len()) else {
        return false;
    };
    prefix.eq_ignore_ascii_case(keyword)
        && statement
            .as_bytes()
            .get(keyword.len())
            .is_some_and(|value| value.is_ascii_whitespace() || *value == b'(')
}

fn split_sql_csv(raw: &str) -> Result<Vec<String>, ShardLoomError> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut paren_depth = 0_i32;
    let mut chars = raw.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                current.push('\'');
                if in_quote && chars.peek() == Some(&'\'') {
                    current.push('\'');
                    chars.next();
                    continue;
                }
                in_quote = !in_quote;
            }
            '(' if !in_quote => {
                paren_depth += 1;
                current.push('(');
            }
            ')' if !in_quote => {
                paren_depth -= 1;
                if paren_depth < 0 {
                    return Err(unsupported_sql_error(
                        "generated-source SQL has unmatched closing parenthesis",
                    ));
                }
                current.push(')');
            }
            ',' if !in_quote && paren_depth == 0 => {
                let part = current.trim();
                if part.is_empty() {
                    return Err(unsupported_sql_error(
                        "generated-source SQL list entries must not be empty",
                    ));
                }
                parts.push(part.to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if in_quote {
        return Err(unsupported_sql_error(
            "generated-source SQL has an unterminated string literal",
        ));
    }
    if paren_depth != 0 {
        return Err(unsupported_sql_error(
            "generated-source SQL has unmatched parentheses",
        ));
    }
    let part = current.trim();
    if part.is_empty() {
        return Err(unsupported_sql_error(
            "generated-source SQL list entries must not be empty",
        ));
    }
    parts.push(part.to_string());
    Ok(parts)
}

fn parse_values_tuples(raw: &str) -> Result<Vec<Vec<String>>, ShardLoomError> {
    let mut rows = Vec::new();
    let bytes = raw.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index >= bytes.len() {
            break;
        }
        if bytes[index] != b'(' {
            return Err(unsupported_sql_error(
                "SQL VALUES smoke expects row tuples like VALUES (1, 'a'), (2, 'b')",
            ));
        }
        index += 1;
        let start = index;
        let mut in_quote = false;
        while index < bytes.len() {
            match bytes[index] {
                b'\'' => {
                    if in_quote && index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                        index += 2;
                        continue;
                    }
                    in_quote = !in_quote;
                }
                b')' if !in_quote => break,
                b'(' if !in_quote => {
                    return Err(unsupported_sql_error(
                        "SQL VALUES smoke does not admit nested expressions or subqueries",
                    ));
                }
                _ => {}
            }
            index += 1;
        }
        if index >= bytes.len() || in_quote {
            return Err(unsupported_sql_error(
                "SQL VALUES smoke has an unterminated row tuple or string literal",
            ));
        }
        let row_body = raw[start..index].trim();
        if row_body.is_empty() {
            return Err(unsupported_sql_error(
                "SQL VALUES row tuples must contain at least one literal",
            ));
        }
        rows.push(split_sql_csv(row_body)?);
        index += 1;
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index < bytes.len() {
            if bytes[index] != b',' {
                return Err(unsupported_sql_error(
                    "SQL VALUES smoke expects commas between row tuples",
                ));
            }
            index += 1;
        }
    }
    Ok(rows)
}

fn split_select_alias(item: &str, column_index: usize) -> Result<(&str, String), ShardLoomError> {
    let mut as_position = None;
    let bytes = item.as_bytes();
    let mut in_quote = false;
    let mut index = 0;
    while index + 1 < bytes.len() {
        match bytes[index] {
            b'\'' => {
                if in_quote && index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                    index += 2;
                    continue;
                }
                in_quote = !in_quote;
            }
            b'a' | b'A' if !in_quote && bytes[index + 1].eq_ignore_ascii_case(&b's') => {
                let before_ok = index == 0 || bytes[index - 1].is_ascii_whitespace();
                let after_index = index + 2;
                let after_ok =
                    after_index >= bytes.len() || bytes[after_index].is_ascii_whitespace();
                if before_ok && after_ok && as_position.replace(index).is_some() {
                    return Err(unsupported_sql_error(
                        "SQL literal SELECT supports at most one AS alias per expression",
                    ));
                }
            }
            _ => {}
        }
        index += 1;
    }
    if in_quote {
        return Err(unsupported_sql_error(
            "SQL literal SELECT has an unterminated string literal",
        ));
    }
    if let Some(position) = as_position {
        let literal = item[..position].trim();
        let alias = item[position + 2..].trim();
        if literal.is_empty() || alias.is_empty() {
            return Err(unsupported_sql_error(
                "SQL literal SELECT AS aliases require both an expression and a name",
            ));
        }
        validate_sql_identifier(alias)?;
        Ok((literal, alias.to_string()))
    } else {
        Ok((item.trim(), format!("column_{column_index}")))
    }
}

fn parse_sql_literal(raw: &str) -> Result<(GeneratedValueType, String), ShardLoomError> {
    let text = raw.trim();
    if text.is_empty() {
        return Err(unsupported_sql_error(
            "SQL source-free literals must not be empty",
        ));
    }
    if text.starts_with('\'') {
        return parse_sql_string_literal(text).map(|value| (GeneratedValueType::Utf8, value));
    }
    if text.eq_ignore_ascii_case("true") {
        return Ok((GeneratedValueType::Bool, "true".to_string()));
    }
    if text.eq_ignore_ascii_case("false") {
        return Ok((GeneratedValueType::Bool, "false".to_string()));
    }
    if text.eq_ignore_ascii_case("null") {
        return Err(unsupported_sql_error(
            "SQL NULL literals are not admitted in the first source-free smoke; null semantics are tracked by the operator-semantics slice",
        ));
    }
    if !text.contains('.') && !text.contains('e') && !text.contains('E') {
        if let Ok(value) = text.parse::<i64>() {
            return Ok((GeneratedValueType::Int64, value.to_string()));
        }
    }
    if let Ok(value) = text.parse::<f64>() {
        if value.is_finite() {
            return Ok((GeneratedValueType::Float64, value.to_string()));
        }
    }
    Err(unsupported_sql_error(
        "SQL source-free smoke admits only int64, finite float64, bool, and single-quoted utf8 literals",
    ))
}

fn parse_sql_string_literal(raw: &str) -> Result<String, ShardLoomError> {
    if !raw.ends_with('\'') || raw.len() < 2 {
        return Err(unsupported_sql_error(
            "SQL string literals must be single-quoted",
        ));
    }
    let inner = &raw[1..raw.len() - 1];
    let mut value = String::new();
    let mut chars = inner.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if chars.peek() == Some(&'\'') {
                value.push('\'');
                chars.next();
            } else {
                return Err(unsupported_sql_error(
                    "SQL string literals must escape quotes as doubled single quotes",
                ));
            }
        } else {
            value.push(ch);
        }
    }
    Ok(value)
}

fn unify_sql_value_type(
    current: GeneratedValueType,
    next: GeneratedValueType,
) -> Result<GeneratedValueType, ShardLoomError> {
    match (current, next) {
        (left, right) if left == right => Ok(left),
        (GeneratedValueType::Int64, GeneratedValueType::Float64)
        | (GeneratedValueType::Float64, GeneratedValueType::Int64) => {
            Ok(GeneratedValueType::Float64)
        }
        _ => Err(unsupported_sql_error(
            "SQL VALUES smoke requires each column to have a single compatible literal type",
        )),
    }
}

fn validate_sql_identifier(value: &str) -> Result<(), ShardLoomError> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(unsupported_sql_error("SQL identifiers must not be empty"));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(unsupported_sql_error(
            "SQL aliases must start with a letter or underscore",
        ));
    }
    if chars.any(|ch| !(ch == '_' || ch.is_ascii_alphanumeric())) {
        return Err(unsupported_sql_error(
            "SQL aliases may contain only letters, numbers, and underscores",
        ));
    }
    Ok(())
}

fn contains_keyword_outside_quotes(raw: &str, keyword: &str) -> bool {
    let keyword_bytes = keyword.as_bytes();
    let bytes = raw.as_bytes();
    let mut in_quote = false;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' => {
                if in_quote && index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                    index += 2;
                    continue;
                }
                in_quote = !in_quote;
            }
            _ if !in_quote
                && index + keyword_bytes.len() <= bytes.len()
                && bytes[index..index + keyword_bytes.len()]
                    .eq_ignore_ascii_case(keyword_bytes) =>
            {
                let before_ok = index == 0 || !bytes[index - 1].is_ascii_alphanumeric();
                let after_index = index + keyword_bytes.len();
                let after_ok =
                    after_index >= bytes.len() || !bytes[after_index].is_ascii_alphanumeric();
                if before_ok && after_ok {
                    return true;
                }
            }
            _ => {}
        }
        index += 1;
    }
    false
}

fn contains_outside_quotes(raw: &str, needle: char) -> bool {
    let bytes = raw.as_bytes();
    let needle = needle as u8;
    let mut in_quote = false;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' => {
                if in_quote && index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                    index += 2;
                    continue;
                }
                in_quote = !in_quote;
            }
            byte if !in_quote && byte == needle => return true,
            _ => {}
        }
        index += 1;
    }
    false
}

fn unsupported_sql_error(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "{reason}; broader SQL remains blocked by GAR-RUNTIME-IMPL-1B and no fallback engine was invoked"
    ))
}

fn local_path_from_file_uri(rest: &str) -> Result<String, ShardLoomError> {
    if rest.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "file:// generated-source output path must include a local path".to_string(),
        ));
    }
    let local = if rest.starts_with('/') {
        rest.to_string()
    } else {
        let Some((authority, path)) = rest.split_once('/') else {
            return Err(ShardLoomError::InvalidOperation(
                "file:// generated-source output path must include a local path".to_string(),
            ));
        };
        if !authority.is_empty() && !authority.eq_ignore_ascii_case("localhost") {
            return Err(ShardLoomError::InvalidOperation(format!(
                "file:// generated-source output URI authority {authority:?} is not local; only empty authority or localhost is allowed"
            )));
        }
        format!("/{path}")
    };
    if cfg!(windows)
        && local.len() >= 3
        && local.as_bytes()[0] == b'/'
        && local.as_bytes()[2] == b':'
        && local.as_bytes()[1].is_ascii_alphabetic()
    {
        Ok(local[1..].to_string())
    } else {
        Ok(local)
    }
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
    let local = if let Some(rest) = trimmed.strip_prefix("file://") {
        local_path_from_file_uri(rest)?
    } else {
        trimmed.to_string()
    };
    if local.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "file:// generated-source output path must include a local path".to_string(),
        ));
    }
    Ok(Path::new(&local).to_path_buf())
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

#[cfg(test)]
mod tests {
    use super::{generated_range_rows, normalize_local_output_path, range_row_count};

    #[test]
    fn range_row_count_does_not_step_past_final_boundary_row() {
        let count = range_row_count(i64::MAX - 1, i64::MAX, 1).expect("range count");
        assert_eq!(count, 1);
        let rows = generated_range_rows(i64::MAX - 1, i64::MAX, 1).expect("range rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], (i64::MAX - 1).to_string());
    }

    #[test]
    fn generated_source_file_uri_rejects_remote_authority() {
        let error = normalize_local_output_path("file://example.com/tmp/out.jsonl")
            .expect_err("remote file authority must be rejected");
        assert!(
            error
                .to_string()
                .contains("only empty authority or localhost is allowed")
        );
    }

    #[test]
    fn generated_source_file_uri_allows_empty_authority_or_localhost() {
        assert!(normalize_local_output_path("file:///tmp/out.jsonl").is_ok());
        assert!(normalize_local_output_path("file://localhost/tmp/out.jsonl").is_ok());
    }
}
