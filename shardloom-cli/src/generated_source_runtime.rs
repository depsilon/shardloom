//! Generated-source local-output runtime handlers.
//!
//! This module implements local generated-output runtime. It
//! accepts either rows already supplied by the user/API layer or narrow
//! ShardLoom-native integer generators, writes local sinks, and emits
//! generated-source/output evidence. Default builds admit JSONL/CSV sinks; flat
//! scalar Parquet, Arrow IPC, Avro, and ORC sinks are gated behind
//! `universal-format-io`, and local Vortex output is gated behind
//! `vortex-write`. It does not read source datasets, parse broad SQL, execute
//! broad `DataFrame` expressions, touch object stores, invoke Foundry, or call
//! fallback engines.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
    time::Instant,
};

use sha2::{Digest, Sha256};
#[cfg(any(feature = "universal-format-io", feature = "vortex-write"))]
use shardloom_core::ScalarValue;
use shardloom_core::WorkspaceSafeLocalWriteReport;
use shardloom_core::{CommandStatus, OutputFormat, ShardLoomError};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

const USER_ROWS_COMMAND: &str = "generated-source-user-rows";
const USER_ROWS_SCHEMA_VERSION: &str = "shardloom.generated_source_user_rows_runtime.v1";
const USER_ROWS_GENERATED_SOURCE_CERTIFICATE_ID: &str =
    "generated-source.user-rows.local-output.v1";
const USER_ROWS_OUTPUT_NATIVE_IO_CERTIFICATE_ID: &str =
    "generated-source.user-rows.local-output.native-io.v1";
const USER_ROWS_EXECUTION_CERTIFICATE_ID: &str =
    "generated-source.user-rows.local-output.execution.v1";

const RANGE_COMMAND: &str = "generated-source-range";
const RANGE_SCHEMA_VERSION: &str = "shardloom.generated_source_range_runtime.v1";
const RANGE_GENERATED_SOURCE_CERTIFICATE_ID: &str = "generated-source.range.local-output.v1";
const RANGE_OUTPUT_NATIVE_IO_CERTIFICATE_ID: &str =
    "generated-source.range.local-output.native-io.v1";
const RANGE_EXECUTION_CERTIFICATE_ID: &str = "generated-source.range.local-output.execution.v1";
const SEQUENCE_COMMAND: &str = "generated-source-sequence";
const SEQUENCE_SCHEMA_VERSION: &str = "shardloom.generated_source_sequence_runtime.v1";
const SEQUENCE_GENERATED_SOURCE_CERTIFICATE_ID: &str = "generated-source.sequence.local-output.v1";
const SEQUENCE_OUTPUT_NATIVE_IO_CERTIFICATE_ID: &str =
    "generated-source.sequence.local-output.native-io.v1";
const SEQUENCE_EXECUTION_CERTIFICATE_ID: &str =
    "generated-source.sequence.local-output.execution.v1";
const MAX_GENERATED_RANGE_ROWS: usize = 1_000_000;

const SQL_COMMAND: &str = "generated-source-sql";
const SQL_SCHEMA_VERSION: &str = "shardloom.generated_source_sql_runtime.v1";
const SQL_GENERATED_SOURCE_CERTIFICATE_ID: &str = "generated-source.sql.local-output.v1";
const SQL_OUTPUT_NATIVE_IO_CERTIFICATE_ID: &str = "generated-source.sql.local-output.native-io.v1";
const SQL_EXECUTION_CERTIFICATE_ID: &str = "generated-source.sql.local-output.execution.v1";
const MAX_SQL_GENERATED_ROWS: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UserRowsGeneratedSourceKind {
    UserRows,
    LiteralTable,
    Calendar,
    DataFrameProjection,
    DataFrameGeneratedWithColumn,
}

impl UserRowsGeneratedSourceKind {
    fn parse(value: &str) -> Result<Self, ShardLoomError> {
        match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "user_rows" | "rows" => Ok(Self::UserRows),
            "literal_table" | "literal" => Ok(Self::LiteralTable),
            "calendar" | "date_dimension" => Ok(Self::Calendar),
            "dataframe_projection" | "dataframe_source_free_projection" => {
                Ok(Self::DataFrameProjection)
            }
            "dataframe_generated_with_column" | "generated_with_column" => {
                Ok(Self::DataFrameGeneratedWithColumn)
            }
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unsupported generated-source user rows source kind {other:?}; supported kinds are user_rows,literal_table,calendar,dataframe_source_free_projection,dataframe_generated_with_column"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::UserRows => "user_rows",
            Self::LiteralTable => "literal_table",
            Self::Calendar => "calendar",
            Self::DataFrameProjection => "dataframe_source_free_projection",
            Self::DataFrameGeneratedWithColumn => "dataframe_generated_with_column",
        }
    }

    fn materialization_boundary(self, output_format: GeneratedOutputFormat) -> String {
        let source = match self {
            Self::UserRows => "python_user_rows",
            Self::LiteralTable => "python_literal_table",
            Self::Calendar => "python_calendar_generator",
            Self::DataFrameProjection => "python_dataframe_source_free_projection",
            Self::DataFrameGeneratedWithColumn => "python_dataframe_generated_with_column",
        };
        format!("{source}_to_local_{}_sink", output_format.sink_label())
    }

    const fn claim_gate_reason(self) -> &'static str {
        match self {
            Self::UserRows => "scoped_local_user_rows_generated_output_runtime",
            Self::LiteralTable => "scoped_local_literal_table_generated_output_runtime",
            Self::Calendar => "scoped_local_calendar_generated_output_runtime",
            Self::DataFrameProjection => {
                "scoped_local_dataframe_source_free_projection_generated_output_runtime"
            }
            Self::DataFrameGeneratedWithColumn => {
                "scoped_local_dataframe_generated_with_column_generated_output_runtime"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeneratedOutputFormat {
    Jsonl,
    Csv,
    Parquet,
    ArrowIpc,
    Avro,
    Orc,
    Vortex,
}

impl GeneratedOutputFormat {
    fn parse(value: &str) -> Result<Self, ShardLoomError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "jsonl" | "json-lines" | "ndjson" => Ok(Self::Jsonl),
            "csv" => Ok(Self::Csv),
            "parquet" => Ok(Self::Parquet),
            "arrow" | "arrow-ipc" | "arrow_ipc" | "ipc" | "feather" => Ok(Self::ArrowIpc),
            "avro" => Ok(Self::Avro),
            "orc" => Ok(Self::Orc),
            "vortex" | "vtx" => Ok(Self::Vortex),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unsupported generated-source output format {other:?}; generated-source runtime supports local JSONL/CSV plus feature-gated Parquet/Arrow IPC/Avro/ORC/Vortex only"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Csv => "csv",
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow_ipc",
            Self::Avro => "avro",
            Self::Orc => "orc",
            Self::Vortex => "vortex",
        }
    }

    const fn sink_label(self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Csv => "csv",
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow_ipc",
            Self::Avro => "avro",
            Self::Orc => "orc",
            Self::Vortex => "vortex",
        }
    }

    #[cfg(not(feature = "universal-format-io"))]
    const fn display_name(self) -> &'static str {
        match self {
            Self::Jsonl => "JSONL",
            Self::Csv => "CSV",
            Self::Parquet => "Parquet",
            Self::ArrowIpc => "Arrow IPC",
            Self::Avro => "Avro",
            Self::Orc => "ORC",
            Self::Vortex => "Vortex",
        }
    }

    const fn certificate_status(self) -> &'static str {
        match self {
            Self::Jsonl | Self::Csv => "certified_local_file_sink",
            Self::Parquet => "certified_local_parquet_sink",
            Self::ArrowIpc => "certified_local_arrow_ipc_sink",
            Self::Avro => "certified_local_avro_sink",
            Self::Orc => "certified_local_orc_sink",
            Self::Vortex => "certified_local_vortex_sink",
        }
    }

    fn render_rows(
        self,
        schema: &[GeneratedColumn],
        rows: &[GeneratedRow],
    ) -> Result<Vec<u8>, ShardLoomError> {
        match self {
            Self::Jsonl => Ok(render_jsonl(schema, rows)?.into_bytes()),
            Self::Csv => Ok(render_csv(schema, rows)?.into_bytes()),
            Self::Parquet => encode_parquet_output_rows(schema, rows),
            Self::ArrowIpc => encode_arrow_ipc_output_rows(schema, rows),
            Self::Avro => encode_avro_output_rows(schema, rows),
            Self::Orc => encode_orc_output_rows(schema, rows),
            Self::Vortex => Err(ShardLoomError::InvalidOperation(
                "local Vortex generated-source output uses the Vortex writer path, not byte rendering"
                    .to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedOutputWriteReport {
    output_bytes: u64,
    output_digest: String,
    write_millis: u128,
    workspace_write_report: WorkspaceSafeLocalWriteReport,
    vortex_report: Option<shardloom_vortex::VortexPreparedStateWriteReport>,
    prepared_state_reuse: Option<shardloom_vortex::VortexPreparedStateReuseReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedOutputTarget {
    output_format: GeneratedOutputFormat,
    output_path: PathBuf,
}

#[derive(Clone, Copy)]
struct GeneratedOutputWriteRequest<'a> {
    output_path: &'a Path,
    output_format: GeneratedOutputFormat,
    fanout_outputs: &'a [GeneratedOutputTarget],
    schema: &'a [GeneratedColumn],
    rows: &'a [GeneratedRow],
    allow_overwrite: bool,
    output_label: &'a str,
    reuse_context: &'a GeneratedSourceReuseContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedSourceReuseContext {
    source_ref: String,
    source_format: String,
    source_content_digest: String,
    source_size_bytes: u64,
    source_schema_digest: String,
    parse_decode_plan_digest: String,
    selected_columns: String,
    output_policy: String,
    source_state_id: String,
    source_state_digest: String,
    certificate_refs: String,
}

#[derive(Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
struct GeneratedPreparedStateEvidence<'a> {
    created: bool,
    reused: bool,
    reuse_allowed: bool,
    reuse_hit: bool,
    scope: &'a str,
    manifest_path: &'a str,
    reason: &'a str,
    manifest_digest: &'a str,
    invalidation_reason: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedWrittenOutput {
    target: GeneratedOutputTarget,
    write_report: GeneratedOutputWriteReport,
    replay: GeneratedOutputReplayEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedOutputReplayEvidence {
    verified: bool,
    status: String,
    replay_millis: u128,
    fidelity_status: String,
    fidelity_loss: String,
}

#[derive(Debug, Clone, Copy)]
struct GeneratedPrimarySinkArtifact<'a> {
    output_format: GeneratedOutputFormat,
    output_path: &'a Path,
    output_bytes: u64,
    output_digest: &'a str,
    workspace_write_report: &'a WorkspaceSafeLocalWriteReport,
    replay: &'a GeneratedOutputReplayEvidence,
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
    fanout_outputs: Vec<GeneratedOutputTarget>,
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
    primary_replay: GeneratedOutputReplayEvidence,
    fanout_outputs: Vec<GeneratedWrittenOutput>,
    schema_digest: String,
    plan_digest: String,
    write_millis: u128,
    workspace_write_report: WorkspaceSafeLocalWriteReport,
    vortex_report: Option<shardloom_vortex::VortexPreparedStateWriteReport>,
    prepared_state_reuse: Option<shardloom_vortex::VortexPreparedStateReuseReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedRangeSmokeRequest {
    output_path: PathBuf,
    output_format: GeneratedOutputFormat,
    fanout_outputs: Vec<GeneratedOutputTarget>,
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
    primary_replay: GeneratedOutputReplayEvidence,
    fanout_outputs: Vec<GeneratedWrittenOutput>,
    schema_digest: String,
    plan_digest: String,
    write_millis: u128,
    workspace_write_report: WorkspaceSafeLocalWriteReport,
    vortex_report: Option<shardloom_vortex::VortexPreparedStateWriteReport>,
    prepared_state_reuse: Option<shardloom_vortex::VortexPreparedStateReuseReport>,
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

    fn materialization_boundary(self, output_format: GeneratedOutputFormat) -> String {
        let source = match self {
            Self::Range => "engine_native_range_generator",
            Self::Sequence => "engine_native_sequence_generator",
        };
        format!("{source}_to_local_{}_sink", output_format.sink_label())
    }

    const fn claim_gate_reason(self) -> &'static str {
        match self {
            Self::Range => "scoped_local_range_generated_output_runtime",
            Self::Sequence => "scoped_local_sequence_generated_output_runtime",
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

    fn materialization_boundary(self, output_format: GeneratedOutputFormat) -> String {
        let source = match self {
            Self::LiteralSelect => "sql_literal_select",
            Self::Values => "sql_values",
            Self::GenerateSeriesRange => "sql_generate_series_range",
        };
        format!("{source}_to_local_{}_sink", output_format.sink_label())
    }

    const fn claim_gate_reason(self) -> &'static str {
        match self {
            Self::LiteralSelect => "scoped_local_sql_literal_select_generated_output_runtime",
            Self::Values => "scoped_local_sql_values_generated_output_runtime",
            Self::GenerateSeriesRange => {
                "scoped_local_sql_generate_series_range_generated_output_runtime"
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
struct GeneratedSqlFilterMetadata {
    source_column: String,
    predicate: String,
    selected_row_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedSqlLimitMetadata {
    count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedSqlProjectionMetadata {
    source_column: String,
    columns: Vec<String>,
    expressions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedSqlOrderMetadata {
    keys: Vec<GeneratedSqlOrderKeyMetadata>,
    input_row_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedSqlOrderKeyMetadata {
    column: String,
    direction: GeneratedSqlSortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeneratedSqlSortDirection {
    Asc,
    Desc,
}

impl GeneratedSqlSortDirection {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }
}

impl GeneratedSqlOrderMetadata {
    fn columns_label(&self) -> String {
        self.keys
            .iter()
            .map(|key| key.column.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    fn directions_label(&self) -> String {
        self.keys
            .iter()
            .map(|key| key.direction.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    fn operator_family_label(&self, has_limit: bool) -> &'static str {
        match (self.keys.len(), has_limit) {
            (0, _) => "not_applicable",
            (1, true) => "single_key_int64_topn",
            (_, true) => "multi_key_int64_topn",
            (1, false) => "single_key_int64_sort",
            _ => "multi_key_int64_sort",
        }
    }
}

type ProjectedSqlRangeRows = (
    Vec<GeneratedColumn>,
    Vec<GeneratedRow>,
    Option<GeneratedSqlProjectionMetadata>,
);
type ParsedSqlRangeTail = (
    Option<SqlRangeProjectionPredicate>,
    Option<GeneratedSqlOrderMetadata>,
    Option<usize>,
);

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedSqlSmokeRequest {
    output_path: PathBuf,
    output_format: GeneratedOutputFormat,
    fanout_outputs: Vec<GeneratedOutputTarget>,
    statement: String,
    source_kind: SqlGeneratedSourceKind,
    schema: Vec<GeneratedColumn>,
    rows: Vec<GeneratedRow>,
    range: Option<GeneratedSqlRangeMetadata>,
    filter: Option<GeneratedSqlFilterMetadata>,
    limit: Option<GeneratedSqlLimitMetadata>,
    projection: Option<GeneratedSqlProjectionMetadata>,
    order_by: Option<GeneratedSqlOrderMetadata>,
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
    filter: Option<GeneratedSqlFilterMetadata>,
    limit: Option<GeneratedSqlLimitMetadata>,
    projection: Option<GeneratedSqlProjectionMetadata>,
    order_by: Option<GeneratedSqlOrderMetadata>,
    primary_replay: GeneratedOutputReplayEvidence,
    fanout_outputs: Vec<GeneratedWrittenOutput>,
    output_bytes: u64,
    output_digest: String,
    schema_digest: String,
    plan_digest: String,
    write_millis: u128,
    workspace_write_report: WorkspaceSafeLocalWriteReport,
    vortex_report: Option<shardloom_vortex::VortexPreparedStateWriteReport>,
    prepared_state_reuse: Option<shardloom_vortex::VortexPreparedStateReuseReport>,
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_generated_source_user_rows_runtime(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    handle_generated_source_user_rows_runtime_with_facade(
        args,
        format,
        USER_ROWS_COMMAND,
        Vec::new(),
    )
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_generated_source_user_rows_runtime_with_facade(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
    emit_command: &'static str,
    extra_fields: Vec<(String, String)>,
) -> ExitCode {
    let Some(output_target) = args.next() else {
        eprintln!(
            "usage: shardloom {USER_ROWS_COMMAND} <local-output-path> <schema> <rows> [--source-kind user_rows|literal_table|calendar|dataframe_source_free_projection|dataframe_generated_with_column] [--output-format jsonl|csv|parquet|arrow-ipc|avro|orc|vortex] [--fanout-output format=local-path] [--allow-overwrite]"
        );
        return ExitCode::from(2);
    };
    let Some(schema_raw) = args.next() else {
        return emit_error(
            emit_command,
            format,
            "generated-source runtime failed",
            &ShardLoomError::InvalidOperation(
                "generated-source user rows runtime requires a schema argument".to_string(),
            ),
        );
    };
    let Some(rows_raw) = args.next() else {
        return emit_error(
            emit_command,
            format,
            "generated-source runtime failed",
            &ShardLoomError::InvalidOperation(
                "generated-source user rows runtime requires a rows argument".to_string(),
            ),
        );
    };

    let mut output_format = GeneratedOutputFormat::Jsonl;
    let mut source_kind = UserRowsGeneratedSourceKind::UserRows;
    let mut allow_overwrite = false;
    let mut fanout_outputs = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output-format" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        "generated-source runtime failed",
                        &ShardLoomError::InvalidOperation(
                            "--output-format requires a value".to_string(),
                        ),
                    );
                };
                output_format = match GeneratedOutputFormat::parse(&value) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return emit_error(
                            emit_command,
                            format,
                            "generated-source runtime failed",
                            &error,
                        );
                    }
                };
            }
            "--fanout-output" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        "generated-source runtime failed",
                        &ShardLoomError::InvalidOperation(
                            "--fanout-output requires format=local-path".to_string(),
                        ),
                    );
                };
                match parse_generated_fanout_output(&value) {
                    Ok(target) => fanout_outputs.push(target),
                    Err(error) => {
                        return emit_error(
                            emit_command,
                            format,
                            "generated-source runtime failed",
                            &error,
                        );
                    }
                }
            }
            "--source-kind" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        "generated-source runtime failed",
                        &ShardLoomError::InvalidOperation(
                            "--source-kind requires a value".to_string(),
                        ),
                    );
                };
                source_kind = match UserRowsGeneratedSourceKind::parse(&value) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return emit_error(
                            emit_command,
                            format,
                            "generated-source runtime failed",
                            &error,
                        );
                    }
                };
            }
            "--allow-overwrite" => allow_overwrite = true,
            extra => {
                return emit_error(
                    emit_command,
                    format,
                    "generated-source runtime failed",
                    &cli_unknown_arg_error(USER_ROWS_COMMAND, extra),
                );
            }
        }
    }

    let request = match GeneratedUserRowsSmokeRequest::parse(
        &output_target,
        output_format,
        fanout_outputs,
        source_kind,
        &schema_raw,
        &rows_raw,
        allow_overwrite,
    ) {
        Ok(request) => request,
        Err(error) => {
            return emit_error(
                emit_command,
                format,
                "generated-source runtime failed",
                &error,
            );
        }
    };

    let report = match run_generated_user_rows_smoke(&request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                emit_command,
                format,
                "generated-source runtime failed",
                &error,
            );
        }
    };

    let mut fields = report.fields();
    fields.extend(extra_fields);

    emit(
        emit_command,
        format,
        CommandStatus::Success,
        format!(
            "generated user rows local-output runtime wrote {} row(s)",
            report.rows.len()
        ),
        report.to_text(),
        vec![],
        fields,
    );
    ExitCode::SUCCESS
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_generated_source_range_runtime(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    handle_generated_source_range_runtime_with_facade(args, format, RANGE_COMMAND, Vec::new())
}

pub(crate) fn handle_generated_source_sequence_runtime(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    handle_generated_source_sequence_runtime_with_facade(args, format, SEQUENCE_COMMAND, Vec::new())
}

pub(crate) fn handle_generated_source_range_runtime_with_facade(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
    emit_command: &'static str,
    extra_fields: Vec<(String, String)>,
) -> ExitCode {
    handle_generated_source_range_like_runtime(
        args,
        format,
        RangeGeneratedSourceKind::Range,
        emit_command,
        extra_fields,
    )
}

pub(crate) fn handle_generated_source_sequence_runtime_with_facade(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
    emit_command: &'static str,
    extra_fields: Vec<(String, String)>,
) -> ExitCode {
    handle_generated_source_range_like_runtime(
        args,
        format,
        RangeGeneratedSourceKind::Sequence,
        emit_command,
        extra_fields,
    )
}

#[allow(clippy::too_many_lines)]
fn handle_generated_source_range_like_runtime(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
    source_kind: RangeGeneratedSourceKind,
    emit_command: &'static str,
    extra_fields: Vec<(String, String)>,
) -> ExitCode {
    let command = source_kind.command();
    let noun = source_kind.summary_noun();
    let Some(output_target) = args.next() else {
        eprintln!(
            "usage: shardloom {command} <local-output-path> <start> <end> [--step int] [--column name] [--output-format jsonl|csv|parquet|arrow-ipc|avro|orc|vortex] [--fanout-output format=local-path] [--allow-overwrite]"
        );
        return ExitCode::from(2);
    };
    let Some(start_raw) = args.next() else {
        return emit_error(
            emit_command,
            format,
            &format!("generated-source {noun} runtime failed"),
            &ShardLoomError::InvalidOperation(format!(
                "generated-source {noun} runtime requires a start argument"
            )),
        );
    };
    let Some(end_raw) = args.next() else {
        return emit_error(
            emit_command,
            format,
            &format!("generated-source {noun} runtime failed"),
            &ShardLoomError::InvalidOperation(format!(
                "generated-source {noun} runtime requires an end argument"
            )),
        );
    };

    let mut output_format = GeneratedOutputFormat::Jsonl;
    let mut allow_overwrite = false;
    let mut fanout_outputs = Vec::new();
    let mut step = 1_i64;
    let mut column_name = "value".to_string();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output-format" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        &format!("generated-source {noun} runtime failed"),
                        &ShardLoomError::InvalidOperation(
                            "--output-format requires a value".to_string(),
                        ),
                    );
                };
                output_format = match GeneratedOutputFormat::parse(&value) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return emit_error(
                            emit_command,
                            format,
                            &format!("generated-source {noun} runtime failed"),
                            &error,
                        );
                    }
                };
            }
            "--fanout-output" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        &format!("generated-source {noun} runtime failed"),
                        &ShardLoomError::InvalidOperation(
                            "--fanout-output requires format=local-path".to_string(),
                        ),
                    );
                };
                match parse_generated_fanout_output(&value) {
                    Ok(target) => fanout_outputs.push(target),
                    Err(error) => {
                        return emit_error(
                            emit_command,
                            format,
                            &format!("generated-source {noun} runtime failed"),
                            &error,
                        );
                    }
                }
            }
            "--allow-overwrite" => allow_overwrite = true,
            "--step" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        &format!("generated-source {noun} runtime failed"),
                        &ShardLoomError::InvalidOperation("--step requires a value".to_string()),
                    );
                };
                step = match parse_i64_arg("step", &value) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return emit_error(
                            emit_command,
                            format,
                            &format!("generated-source {noun} runtime failed"),
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
                        &format!("generated-source {noun} runtime failed"),
                        &ShardLoomError::InvalidOperation("--column requires a value".to_string()),
                    );
                };
                column_name = match percent_decode(&value) {
                    Ok(parsed) if !parsed.trim().is_empty() => parsed,
                    Ok(_) => {
                        return emit_error(
                            command,
                            format,
                            &format!("generated-source {noun} runtime failed"),
                            &ShardLoomError::InvalidOperation(format!(
                                "generated-source {noun} column must not be empty"
                            )),
                        );
                    }
                    Err(error) => {
                        return emit_error(
                            command,
                            format,
                            &format!("generated-source {noun} runtime failed"),
                            &error,
                        );
                    }
                };
            }
            extra => {
                return emit_error(
                    emit_command,
                    format,
                    &format!("generated-source {noun} runtime failed"),
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
                &format!("generated-source {noun} runtime failed"),
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
                &format!("generated-source {noun} runtime failed"),
                &error,
            );
        }
    };
    let request = match GeneratedRangeSmokeRequest::parse(
        &output_target,
        output_format,
        fanout_outputs,
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
                emit_command,
                format,
                &format!("generated-source {noun} runtime failed"),
                &error,
            );
        }
    };

    let report = match run_generated_range_smoke(&request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                emit_command,
                format,
                &format!("generated-source {noun} runtime failed"),
                &error,
            );
        }
    };

    let mut fields = report.fields();
    fields.extend(extra_fields);

    emit(
        emit_command,
        format,
        CommandStatus::Success,
        format!(
            "generated {noun} local-output smoke wrote {} row(s)",
            report.rows.len()
        ),
        report.to_text(),
        vec![],
        fields,
    );
    ExitCode::SUCCESS
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_generated_source_sql_runtime(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    handle_generated_source_sql_runtime_with_facade(args, format, SQL_COMMAND, Vec::new())
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_generated_source_sql_runtime_with_facade(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
    emit_command: &'static str,
    extra_fields: Vec<(String, String)>,
) -> ExitCode {
    let Some(output_target) = args.next() else {
        eprintln!(
            "usage: shardloom {SQL_COMMAND} <local-output-path> <sql-statement> [--output-format jsonl|csv|parquet|arrow-ipc|avro|orc|vortex] [--fanout-output format=local-path] [--allow-overwrite]"
        );
        return ExitCode::from(2);
    };
    let Some(statement_raw) = args.next() else {
        return emit_error(
            emit_command,
            format,
            "generated-source SQL runtime failed",
            &ShardLoomError::InvalidOperation(
                "generated-source SQL runtime requires a SQL statement argument".to_string(),
            ),
        );
    };

    let mut output_format = GeneratedOutputFormat::Jsonl;
    let mut allow_overwrite = false;
    let mut fanout_outputs = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output-format" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        "generated-source SQL runtime failed",
                        &ShardLoomError::InvalidOperation(
                            "--output-format requires a value".to_string(),
                        ),
                    );
                };
                output_format = match GeneratedOutputFormat::parse(&value) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return emit_error(
                            emit_command,
                            format,
                            "generated-source SQL runtime failed",
                            &error,
                        );
                    }
                };
            }
            "--fanout-output" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        "generated-source SQL runtime failed",
                        &ShardLoomError::InvalidOperation(
                            "--fanout-output requires format=local-path".to_string(),
                        ),
                    );
                };
                match parse_generated_fanout_output(&value) {
                    Ok(target) => fanout_outputs.push(target),
                    Err(error) => {
                        return emit_error(
                            emit_command,
                            format,
                            "generated-source SQL runtime failed",
                            &error,
                        );
                    }
                }
            }
            "--allow-overwrite" => allow_overwrite = true,
            extra => {
                return emit_error(
                    emit_command,
                    format,
                    "generated-source SQL runtime failed",
                    &cli_unknown_arg_error(SQL_COMMAND, extra),
                );
            }
        }
    }

    let request = match GeneratedSqlSmokeRequest::parse(
        &output_target,
        output_format,
        fanout_outputs,
        &statement_raw,
        allow_overwrite,
    ) {
        Ok(request) => request,
        Err(error) => {
            return emit_error(
                emit_command,
                format,
                "generated-source SQL runtime failed",
                &error,
            );
        }
    };

    let report = match run_generated_sql_smoke(&request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                emit_command,
                format,
                "generated-source SQL runtime failed",
                &error,
            );
        }
    };
    let mut fields = report.fields();
    fields.extend(extra_fields);

    emit(
        emit_command,
        format,
        CommandStatus::Success,
        format!(
            "generated SQL source-free local-output runtime wrote {} row(s)",
            report.rows.len()
        ),
        report.to_text(),
        vec![],
        fields,
    );
    ExitCode::SUCCESS
}

impl GeneratedUserRowsSmokeRequest {
    fn parse(
        output_target: &str,
        output_format: GeneratedOutputFormat,
        fanout_outputs: Vec<GeneratedOutputTarget>,
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
                "generated-source user rows runtime requires at least one row".to_string(),
            ));
        }
        validate_user_rows_source_kind_shape(source_kind, &schema, &rows)?;
        Ok(Self {
            output_path,
            output_format,
            fanout_outputs,
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
        let mut fields = vec![
            (
                "schema_version".to_string(),
                USER_ROWS_SCHEMA_VERSION.to_string(),
            ),
            (
                "generated_source_runtime_report_id".to_string(),
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
            ("output_row_count".to_string(), self.rows.len().to_string()),
            ("output_bytes".to_string(), self.output_bytes.to_string()),
            ("output_digest".to_string(), self.output_digest.clone()),
            (
                "output_native_io_certificate_status".to_string(),
                self.output_format.certificate_status().to_string(),
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
                self.source_kind
                    .materialization_boundary(self.output_format),
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
                "not_claim_grade".to_string(),
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
        ];
        fields.extend(generated_output_sink_artifact_fields(
            GeneratedPrimarySinkArtifact {
                output_format: self.output_format,
                output_path: &self.output_path,
                output_bytes: self.output_bytes,
                output_digest: &self.output_digest,
                workspace_write_report: &self.workspace_write_report,
                replay: &self.primary_replay,
            },
            &self.fanout_outputs,
        ));
        fields.extend(self.workspace_write_report.evidence_fields("output"));
        fields.extend(generated_vortex_output_fields(
            self.vortex_report.as_ref(),
            self.prepared_state_reuse.as_ref(),
        ));
        fields.extend(generated_output_fanout_fields(
            self.output_format,
            &self.primary_replay,
            &self.fanout_outputs,
        ));
        fields
    }

    fn to_text(&self) -> String {
        format!(
            "generated-source user rows runtime\nschema_version: {USER_ROWS_SCHEMA_VERSION}\ngenerated_source_kind: {}\nschema: {}\nrows: {}\noutput: {}\noutput format: {}\ngenerated source certificate: present\noutput Native I/O certificate: {}\nfallback_attempted: false\nexternal_engine_invoked: false\nclaim_gate_status: not_claim_grade",
            self.source_kind.as_str(),
            canonical_schema(&self.schema),
            self.rows.len(),
            self.output_path.display(),
            self.output_format.as_str(),
            self.output_format.certificate_status(),
        )
    }
}

impl GeneratedRangeSmokeRequest {
    #[allow(clippy::too_many_arguments)]
    fn parse(
        output_target: &str,
        output_format: GeneratedOutputFormat,
        fanout_outputs: Vec<GeneratedOutputTarget>,
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
                "generated-source range row count {row_count} exceeds runtime row limit {MAX_GENERATED_RANGE_ROWS}"
            )));
        }
        Ok(Self {
            output_path,
            output_format,
            fanout_outputs,
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
        let mut fields = vec![
            (
                "schema_version".to_string(),
                self.source_kind.schema_version().to_string(),
            ),
            (
                "generated_source_runtime_report_id".to_string(),
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
            ("output_row_count".to_string(), self.rows.len().to_string()),
            ("output_bytes".to_string(), self.output_bytes.to_string()),
            ("output_digest".to_string(), self.output_digest.clone()),
            (
                "output_native_io_certificate_status".to_string(),
                self.output_format.certificate_status().to_string(),
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
                self.source_kind
                    .materialization_boundary(self.output_format),
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
                "not_claim_grade".to_string(),
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
        ];
        fields.extend(generated_output_sink_artifact_fields(
            GeneratedPrimarySinkArtifact {
                output_format: self.output_format,
                output_path: &self.output_path,
                output_bytes: self.output_bytes,
                output_digest: &self.output_digest,
                workspace_write_report: &self.workspace_write_report,
                replay: &self.primary_replay,
            },
            &self.fanout_outputs,
        ));
        fields.extend(self.workspace_write_report.evidence_fields("output"));
        fields.extend(generated_vortex_output_fields(
            self.vortex_report.as_ref(),
            self.prepared_state_reuse.as_ref(),
        ));
        fields.extend(generated_output_fanout_fields(
            self.output_format,
            &self.primary_replay,
            &self.fanout_outputs,
        ));
        fields
    }

    fn to_text(&self) -> String {
        format!(
            "generated-source {} runtime\nschema_version: {}\n{}: {}..{} step {}\ncolumn: {}\nrows: {}\noutput: {}\noutput format: {}\ngenerated source certificate: present\noutput Native I/O certificate: {}\nfallback_attempted: false\nexternal_engine_invoked: false\nclaim_gate_status: not_claim_grade",
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
            self.output_format.certificate_status(),
        )
    }
}

impl GeneratedSqlSmokeRequest {
    fn parse(
        output_target: &str,
        output_format: GeneratedOutputFormat,
        fanout_outputs: Vec<GeneratedOutputTarget>,
        statement_raw: &str,
        allow_overwrite: bool,
    ) -> Result<Self, ShardLoomError> {
        let output_path = normalize_local_output_path(output_target)?;
        let parsed = parse_source_free_sql(statement_raw)?;
        if parsed.rows.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "generated-source SQL runtime produced no rows; source-free SQL runtime requires at least one row".to_string(),
            ));
        }
        if parsed.rows.len() > MAX_SQL_GENERATED_ROWS {
            return Err(ShardLoomError::InvalidOperation(format!(
                "generated-source SQL row count {} exceeds runtime row limit {MAX_SQL_GENERATED_ROWS}",
                parsed.rows.len()
            )));
        }
        Ok(Self {
            output_path,
            output_format,
            fanout_outputs,
            statement: parsed.statement,
            source_kind: parsed.source_kind,
            schema: parsed.schema,
            rows: parsed.rows,
            range: parsed.range,
            filter: parsed.filter,
            limit: parsed.limit,
            projection: parsed.projection,
            order_by: parsed.order_by,
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
                "generated_source_runtime_report_id".to_string(),
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
            ("output_row_count".to_string(), self.rows.len().to_string()),
            ("output_bytes".to_string(), self.output_bytes.to_string()),
            ("output_digest".to_string(), self.output_digest.clone()),
            (
                "output_native_io_certificate_status".to_string(),
                self.output_format.certificate_status().to_string(),
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
                self.source_kind
                    .materialization_boundary(self.output_format),
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
                "not_claim_grade".to_string(),
            ),
            (
                "claim_gate_reason".to_string(),
                self.source_kind.claim_gate_reason().to_string(),
            ),
            (
                "sql_source_free_runtime_supported".to_string(),
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
        fields.extend(generated_output_sink_artifact_fields(
            GeneratedPrimarySinkArtifact {
                output_format: self.output_format,
                output_path: &self.output_path,
                output_bytes: self.output_bytes,
                output_digest: &self.output_digest,
                workspace_write_report: &self.workspace_write_report,
                replay: &self.primary_replay,
            },
            &self.fanout_outputs,
        ));
        fields.extend(self.workspace_write_report.evidence_fields("output"));
        fields.extend(generated_vortex_output_fields(
            self.vortex_report.as_ref(),
            self.prepared_state_reuse.as_ref(),
        ));
        fields.extend(generated_output_fanout_fields(
            self.output_format,
            &self.primary_replay,
            &self.fanout_outputs,
        ));
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
        if let Some(filter) = &self.filter {
            fields.extend([
                (
                    "sql_source_free_filter_runtime_execution".to_string(),
                    "true".to_string(),
                ),
                (
                    "sql_source_free_filter_source_column".to_string(),
                    filter.source_column.clone(),
                ),
                (
                    "sql_source_free_filter_predicate".to_string(),
                    filter.predicate.clone(),
                ),
                (
                    "sql_source_free_filter_selected_row_count".to_string(),
                    filter.selected_row_count.to_string(),
                ),
            ]);
        }
        if let Some(limit) = &self.limit {
            fields.extend([
                (
                    "sql_source_free_limit_runtime_execution".to_string(),
                    "true".to_string(),
                ),
                (
                    "sql_source_free_limit_count".to_string(),
                    limit.count.to_string(),
                ),
            ]);
        }
        if let Some(order_by) = &self.order_by {
            fields.extend([
                (
                    "sql_source_free_order_by_runtime_execution".to_string(),
                    "true".to_string(),
                ),
                (
                    "sql_source_free_top_n_runtime_execution".to_string(),
                    self.limit.is_some().to_string(),
                ),
                (
                    "sql_source_free_sort_operator_family".to_string(),
                    order_by
                        .operator_family_label(self.limit.is_some())
                        .to_string(),
                ),
                (
                    "sql_source_free_sort_keys".to_string(),
                    order_by.columns_label(),
                ),
                (
                    "sql_source_free_sort_direction".to_string(),
                    order_by.directions_label(),
                ),
                (
                    "sql_source_free_sort_input_row_count".to_string(),
                    order_by.input_row_count.to_string(),
                ),
                (
                    "sql_source_free_top_n_limit".to_string(),
                    self.limit
                        .as_ref()
                        .map_or_else(|| "0".to_string(), |limit| limit.count.to_string()),
                ),
            ]);
        }
        if let Some(projection) = &self.projection {
            fields.extend([
                (
                    "sql_source_free_projection_runtime_execution".to_string(),
                    "true".to_string(),
                ),
                (
                    "sql_source_free_projection_source_column".to_string(),
                    projection.source_column.clone(),
                ),
                (
                    "sql_source_free_projection_columns".to_string(),
                    projection.columns.join(","),
                ),
                (
                    "sql_source_free_projection_expressions".to_string(),
                    projection.expressions.join(","),
                ),
            ]);
        }
        fields
    }

    fn to_text(&self) -> String {
        format!(
            "generated-source SQL runtime\nschema_version: {SQL_SCHEMA_VERSION}\nsql_statement_kind: {}\nschema: {}\nrows: {}\noutput: {}\noutput format: {}\ngenerated source certificate: present\noutput Native I/O certificate: {}\nfallback_attempted: false\nexternal_engine_invoked: false\nclaim_gate_status: not_claim_grade",
            self.source_kind.as_str(),
            canonical_schema(&self.schema),
            self.rows.len(),
            self.output_path.display(),
            self.output_format.as_str(),
            self.output_format.certificate_status(),
        )
    }
}

fn run_generated_user_rows_smoke(
    request: &GeneratedUserRowsSmokeRequest,
) -> Result<GeneratedUserRowsSmokeReport, ShardLoomError> {
    let schema_text = canonical_schema(&request.schema);
    let canonical_rows = canonical_rows(&request.schema, &request.rows);
    let schema_digest = fnv64_digest(&schema_text);
    let plan_digest = fnv64_digest(&format!(
        "generated_source_kind={};output_format={};fanout={};schema={schema_text};rows={canonical_rows}",
        request.source_kind.as_str(),
        request.output_format.as_str(),
        generated_fanout_plan_digest_fragment(&request.fanout_outputs)
    ));
    let reuse_context = generated_source_reuse_context(
        request.source_kind.as_str(),
        &schema_text,
        &canonical_rows,
        &schema_digest,
        &plan_digest,
        USER_ROWS_GENERATED_SOURCE_CERTIFICATE_ID,
        USER_ROWS_OUTPUT_NATIVE_IO_CERTIFICATE_ID,
    );
    let (primary_output, fanout_outputs) = write_generated_outputs(GeneratedOutputWriteRequest {
        output_path: &request.output_path,
        output_format: request.output_format,
        fanout_outputs: &request.fanout_outputs,
        schema: &request.schema,
        rows: &request.rows,
        allow_overwrite: request.allow_overwrite,
        output_label: "generated-source output",
        reuse_context: &reuse_context,
    })?;

    Ok(GeneratedUserRowsSmokeReport {
        output_path: request.output_path.clone(),
        output_format: request.output_format,
        source_kind: request.source_kind,
        schema: request.schema.clone(),
        rows: request.rows.clone(),
        output_bytes: primary_output.write_report.output_bytes,
        output_digest: primary_output.write_report.output_digest.clone(),
        primary_replay: primary_output.replay,
        fanout_outputs,
        schema_digest,
        plan_digest,
        write_millis: primary_output.write_report.write_millis,
        workspace_write_report: primary_output.write_report.workspace_write_report,
        vortex_report: primary_output.write_report.vortex_report,
        prepared_state_reuse: primary_output.write_report.prepared_state_reuse,
    })
}

fn run_generated_range_smoke(
    request: &GeneratedRangeSmokeRequest,
) -> Result<GeneratedRangeSmokeReport, ShardLoomError> {
    let schema = vec![GeneratedColumn {
        name: request.column_name.clone(),
        value_type: GeneratedValueType::Int64,
    }];
    let rows = generated_range_rows(request.start, request.end, request.step)?;
    let schema_text = canonical_schema(&schema);
    let canonical_rows = canonical_rows(&schema, &rows);
    let schema_digest = fnv64_digest(&schema_text);
    let plan_digest = fnv64_digest(&format!(
        "generated_source_kind={};output_format={};fanout={};start={};end={};step={};column={}",
        request.source_kind.as_str(),
        request.output_format.as_str(),
        generated_fanout_plan_digest_fragment(&request.fanout_outputs),
        request.start,
        request.end,
        request.step,
        request.column_name
    ));
    let reuse_context = generated_source_reuse_context(
        request.source_kind.as_str(),
        &schema_text,
        &canonical_rows,
        &schema_digest,
        &plan_digest,
        request.source_kind.generated_source_certificate_id(),
        request.source_kind.output_native_io_certificate_id(),
    );
    let (primary_output, fanout_outputs) = write_generated_outputs(GeneratedOutputWriteRequest {
        output_path: &request.output_path,
        output_format: request.output_format,
        fanout_outputs: &request.fanout_outputs,
        schema: &schema,
        rows: &rows,
        allow_overwrite: request.allow_overwrite,
        output_label: "generated-source output",
        reuse_context: &reuse_context,
    })?;

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
        output_bytes: primary_output.write_report.output_bytes,
        output_digest: primary_output.write_report.output_digest.clone(),
        primary_replay: primary_output.replay,
        fanout_outputs,
        schema_digest,
        plan_digest,
        write_millis: primary_output.write_report.write_millis,
        workspace_write_report: primary_output.write_report.workspace_write_report,
        vortex_report: primary_output.write_report.vortex_report,
        prepared_state_reuse: primary_output.write_report.prepared_state_reuse,
    })
}

fn run_generated_sql_smoke(
    request: &GeneratedSqlSmokeRequest,
) -> Result<GeneratedSqlSmokeReport, ShardLoomError> {
    let schema_text = canonical_schema(&request.schema);
    let canonical_rows = canonical_rows(&request.schema, &request.rows);
    let schema_digest = fnv64_digest(&schema_text);
    let plan_digest = fnv64_digest(&format!(
        "generated_source_kind={};output_format={};fanout={};statement={};schema={schema_text};rows={canonical_rows}",
        request.source_kind.as_str(),
        request.output_format.as_str(),
        generated_fanout_plan_digest_fragment(&request.fanout_outputs),
        request.statement
    ));
    let reuse_context = generated_source_reuse_context(
        request.source_kind.as_str(),
        &schema_text,
        &canonical_rows,
        &schema_digest,
        &plan_digest,
        SQL_GENERATED_SOURCE_CERTIFICATE_ID,
        SQL_OUTPUT_NATIVE_IO_CERTIFICATE_ID,
    );
    let (primary_output, fanout_outputs) = write_generated_outputs(GeneratedOutputWriteRequest {
        output_path: &request.output_path,
        output_format: request.output_format,
        fanout_outputs: &request.fanout_outputs,
        schema: &request.schema,
        rows: &request.rows,
        allow_overwrite: request.allow_overwrite,
        output_label: "generated-source SQL output",
        reuse_context: &reuse_context,
    })?;

    Ok(GeneratedSqlSmokeReport {
        output_path: request.output_path.clone(),
        output_format: request.output_format,
        statement: request.statement.clone(),
        source_kind: request.source_kind,
        schema: request.schema.clone(),
        rows: request.rows.clone(),
        range: request.range.clone(),
        filter: request.filter.clone(),
        limit: request.limit.clone(),
        projection: request.projection.clone(),
        order_by: request.order_by.clone(),
        primary_replay: primary_output.replay,
        fanout_outputs,
        output_bytes: primary_output.write_report.output_bytes,
        output_digest: primary_output.write_report.output_digest.clone(),
        schema_digest,
        plan_digest,
        write_millis: primary_output.write_report.write_millis,
        workspace_write_report: primary_output.write_report.workspace_write_report,
        vortex_report: primary_output.write_report.vortex_report,
        prepared_state_reuse: primary_output.write_report.prepared_state_reuse,
    })
}

fn write_generated_output(
    output_path: &Path,
    output_format: GeneratedOutputFormat,
    schema: &[GeneratedColumn],
    rows: &[GeneratedRow],
    allow_overwrite: bool,
    output_label: &str,
    reuse_context: &GeneratedSourceReuseContext,
) -> Result<GeneratedOutputWriteReport, ShardLoomError> {
    match output_format {
        GeneratedOutputFormat::Vortex => {
            write_generated_vortex_output(output_path, schema, rows, allow_overwrite, reuse_context)
        }
        _ => write_generated_file_output(
            output_path,
            output_format,
            schema,
            rows,
            allow_overwrite,
            output_label,
        ),
    }
}

fn parse_generated_fanout_output(value: &str) -> Result<GeneratedOutputTarget, ShardLoomError> {
    let Some((format_raw, path_raw)) = value.split_once('=') else {
        return Err(ShardLoomError::InvalidOperation(
            "--fanout-output must use format=local-path, for example csv=out.csv".to_string(),
        ));
    };
    Ok(GeneratedOutputTarget {
        output_format: GeneratedOutputFormat::parse(format_raw)?,
        output_path: normalize_local_output_path(path_raw)?,
    })
}

fn normalized_generated_output_path_key(path: &Path) -> String {
    path.to_string_lossy()
        .replace('\\', "/")
        .to_ascii_lowercase()
}

fn canonical_generated_output_path_key(path: &Path) -> Result<String, ShardLoomError> {
    let workspace_root = shardloom_core::infer_local_output_workspace_root(path)?;
    let plan = shardloom_core::plan_workspace_safe_local_output(workspace_root, path, true)?;
    Ok(normalized_generated_output_path_key(&plan.target_path))
}

fn generated_fanout_plan_digest_fragment(fanout_outputs: &[GeneratedOutputTarget]) -> String {
    fanout_outputs
        .iter()
        .map(|target| {
            format!(
                "{}={}",
                target.output_format.sink_label(),
                target.output_path.display()
            )
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn preflight_generated_output_writes(
    output_path: &Path,
    output_format: GeneratedOutputFormat,
    fanout_outputs: &[GeneratedOutputTarget],
    allow_overwrite: bool,
) -> Result<(), ShardLoomError> {
    let mut paths = BTreeSet::new();
    validate_generated_output_format_available(output_format, output_path)?;
    paths.insert(canonical_generated_output_path_key(output_path)?);
    for target in fanout_outputs {
        validate_generated_output_format_available(target.output_format, &target.output_path)?;
        if !paths.insert(canonical_generated_output_path_key(&target.output_path)?) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "generated-source fanout output path is duplicated: {}; no fallback execution was attempted",
                target.output_path.display()
            )));
        }
    }
    for (path, format) in std::iter::once((output_path, output_format)).chain(
        fanout_outputs
            .iter()
            .map(|target| (target.output_path.as_path(), target.output_format)),
    ) {
        let workspace_root = shardloom_core::infer_local_output_workspace_root(path)?;
        let effective_allow_overwrite = allow_overwrite || format == GeneratedOutputFormat::Vortex;
        shardloom_core::plan_workspace_safe_local_output(
            workspace_root,
            path,
            effective_allow_overwrite,
        )?;
    }
    Ok(())
}

fn validate_generated_output_format_available(
    format: GeneratedOutputFormat,
    output_path: &Path,
) -> Result<(), ShardLoomError> {
    match format {
        GeneratedOutputFormat::Jsonl | GeneratedOutputFormat::Csv => Ok(()),
        GeneratedOutputFormat::Parquet
        | GeneratedOutputFormat::ArrowIpc
        | GeneratedOutputFormat::Avro
        | GeneratedOutputFormat::Orc => {
            #[cfg(feature = "universal-format-io")]
            {
                let _ = format;
                Ok(())
            }
            #[cfg(not(feature = "universal-format-io"))]
            {
                Err(structured_output_feature_error(format.display_name()))
            }
        }
        GeneratedOutputFormat::Vortex => validate_generated_vortex_output_available(output_path),
    }
}

#[cfg(feature = "vortex-write")]
fn validate_generated_vortex_output_available(output_path: &Path) -> Result<(), ShardLoomError> {
    validate_generated_vortex_target(output_path)
}

#[cfg(not(feature = "vortex-write"))]
fn validate_generated_vortex_output_available(_output_path: &Path) -> Result<(), ShardLoomError> {
    Err(ShardLoomError::InvalidOperation(
        "local Vortex generated-source output runtime requires building shardloom-cli with --features vortex-write; default builds expose Vortex generated-source output as a deterministic blocked sink; no fallback execution was attempted"
            .to_string(),
    ))
}

fn write_generated_outputs(
    request: GeneratedOutputWriteRequest<'_>,
) -> Result<(GeneratedWrittenOutput, Vec<GeneratedWrittenOutput>), ShardLoomError> {
    preflight_generated_output_writes(
        request.output_path,
        request.output_format,
        request.fanout_outputs,
        request.allow_overwrite,
    )?;
    let primary_target = GeneratedOutputTarget {
        output_format: request.output_format,
        output_path: request.output_path.to_path_buf(),
    };
    let primary = write_generated_output_target(
        primary_target,
        request.schema,
        request.rows,
        request.allow_overwrite,
        request.output_label,
        request.reuse_context,
    )?;
    let fanout = request
        .fanout_outputs
        .iter()
        .cloned()
        .map(|target| {
            write_generated_output_target(
                target,
                request.schema,
                request.rows,
                request.allow_overwrite,
                request.output_label,
                request.reuse_context,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((primary, fanout))
}

fn write_generated_output_target(
    target: GeneratedOutputTarget,
    schema: &[GeneratedColumn],
    rows: &[GeneratedRow],
    allow_overwrite: bool,
    output_label: &str,
    reuse_context: &GeneratedSourceReuseContext,
) -> Result<GeneratedWrittenOutput, ShardLoomError> {
    let write_report = write_generated_output(
        &target.output_path,
        target.output_format,
        schema,
        rows,
        allow_overwrite,
        output_label,
        reuse_context,
    )?;
    let replay = replay_generated_output(target.output_format, &target.output_path, &write_report)?;
    Ok(GeneratedWrittenOutput {
        target,
        write_report,
        replay,
    })
}

fn replay_generated_output(
    format: GeneratedOutputFormat,
    path: &Path,
    write_report: &GeneratedOutputWriteReport,
) -> Result<GeneratedOutputReplayEvidence, ShardLoomError> {
    if format == GeneratedOutputFormat::Vortex {
        if let Some(report) = write_report.vortex_report.as_ref() {
            return Ok(GeneratedOutputReplayEvidence {
                verified: report.upstream_vortex_scan_called,
                status: if report.upstream_vortex_scan_called {
                    "verified_vortex_reopen_row_count".to_string()
                } else {
                    "blocked_missing_vortex_reopen_proof".to_string()
                },
                replay_millis: report.reopen_scan_micros / 1_000,
                fidelity_status: generated_output_fidelity_status(format).to_string(),
                fidelity_loss: generated_output_fidelity_loss(format).to_string(),
            });
        }
        if let Some(reuse_report) = write_report.prepared_state_reuse.as_ref() {
            return Ok(GeneratedOutputReplayEvidence {
                verified: reuse_report.hit,
                status: if reuse_report.hit {
                    "verified_vortex_reuse_manifest_artifact_digest".to_string()
                } else {
                    "blocked_missing_vortex_write_or_reuse_proof".to_string()
                },
                replay_millis: 0,
                fidelity_status: generated_output_fidelity_status(format).to_string(),
                fidelity_loss: generated_output_fidelity_loss(format).to_string(),
            });
        }
    }
    let replay_start = Instant::now();
    let bytes = fs::read(path).map_err(|error| {
        ShardLoomError::Message(format!(
            "failed to replay generated-source output {}: {error}",
            path.display()
        ))
    })?;
    let replay_digest = digest_bytes_for_algorithm(&bytes, &write_report.output_digest);
    if replay_digest != write_report.output_digest {
        return Err(ShardLoomError::InvalidOperation(format!(
            "generated-source output replay digest mismatch for {}: expected {}, got {replay_digest}",
            path.display(),
            write_report.output_digest
        )));
    }
    Ok(GeneratedOutputReplayEvidence {
        verified: true,
        status: "verified_local_file_digest".to_string(),
        replay_millis: replay_start.elapsed().as_millis(),
        fidelity_status: generated_output_fidelity_status(format).to_string(),
        fidelity_loss: generated_output_fidelity_loss(format).to_string(),
    })
}

fn generated_output_fidelity_status(format: GeneratedOutputFormat) -> &'static str {
    match format {
        GeneratedOutputFormat::Jsonl => "logical_rows_replay_verified",
        GeneratedOutputFormat::Csv => "logical_rows_replay_verified_type_metadata_not_preserved",
        GeneratedOutputFormat::Parquet
        | GeneratedOutputFormat::ArrowIpc
        | GeneratedOutputFormat::Avro
        | GeneratedOutputFormat::Orc => "flat_scalar_schema_replay_verified",
        GeneratedOutputFormat::Vortex => "native_vortex_reopen_row_count_verified",
    }
}

fn generated_output_fidelity_loss(format: GeneratedOutputFormat) -> &'static str {
    match format {
        GeneratedOutputFormat::Jsonl => "jsonl_text_roundtrip_not_full_type_metadata_fidelity",
        GeneratedOutputFormat::Csv => "csv_text_roundtrip_loses_static_type_metadata",
        GeneratedOutputFormat::Parquet
        | GeneratedOutputFormat::ArrowIpc
        | GeneratedOutputFormat::Avro
        | GeneratedOutputFormat::Orc => "flat_scalar_structured_schema_preserved",
        GeneratedOutputFormat::Vortex => "native_vortex_output_highest_fidelity",
    }
}

fn write_generated_file_output(
    output_path: &Path,
    output_format: GeneratedOutputFormat,
    schema: &[GeneratedColumn],
    rows: &[GeneratedRow],
    allow_overwrite: bool,
    output_label: &str,
) -> Result<GeneratedOutputWriteReport, ShardLoomError> {
    let start = Instant::now();
    let content = output_format.render_rows(schema, rows)?;
    let workspace_root = shardloom_core::infer_local_output_workspace_root(output_path)?;
    let workspace_write_report = shardloom_core::write_workspace_safe_bytes(
        workspace_root,
        output_path,
        allow_overwrite,
        output_label,
        &content,
    )?;
    Ok(GeneratedOutputWriteReport {
        output_bytes: workspace_write_report.bytes_written,
        output_digest: workspace_write_report.output_digest.clone(),
        write_millis: start.elapsed().as_millis(),
        workspace_write_report,
        vortex_report: None,
        prepared_state_reuse: None,
    })
}

fn generated_source_reuse_context(
    source_kind: &str,
    schema_text: &str,
    canonical_rows: &str,
    schema_digest: &str,
    plan_digest: &str,
    generated_source_certificate_id: &str,
    output_native_io_certificate_id: &str,
) -> GeneratedSourceReuseContext {
    let source_payload = format!("kind={source_kind};schema={schema_text};rows={canonical_rows}");
    let source_content_digest = fnv64_digest(&source_payload);
    let source_state_digest = fnv64_digest(&format!(
        "generated_source_state|{source_kind}|{schema_digest}|{source_content_digest}"
    ));
    GeneratedSourceReuseContext {
        source_ref: format!(
            "generated-source://{source_kind}/{}",
            source_content_digest.replace(':', "-")
        ),
        source_format: format!("generated_source:{source_kind}"),
        source_content_digest,
        source_size_bytes: source_payload.len() as u64,
        source_schema_digest: schema_digest.to_string(),
        parse_decode_plan_digest: plan_digest.to_string(),
        selected_columns: "all_columns".to_string(),
        output_policy: "caller_owned_generated_source_local_vortex_artifact".to_string(),
        source_state_id: format!(
            "generated-source-state-{}",
            source_state_digest.replace(':', "-")
        ),
        source_state_digest,
        certificate_refs: format!(
            "generated_source={generated_source_certificate_id};output_native_io={output_native_io_certificate_id}"
        ),
    }
}

#[cfg(not(feature = "vortex-write"))]
fn write_generated_vortex_output(
    _output_path: &Path,
    _schema: &[GeneratedColumn],
    _rows: &[GeneratedRow],
    _allow_overwrite: bool,
    _reuse_context: &GeneratedSourceReuseContext,
) -> Result<GeneratedOutputWriteReport, ShardLoomError> {
    Err(ShardLoomError::InvalidOperation(
        "local Vortex generated-source output runtime requires building shardloom-cli with --features vortex-write; default builds expose Vortex generated-source output as a deterministic blocked sink; no fallback execution was attempted"
            .to_string(),
    ))
}

#[cfg(feature = "vortex-write")]
fn write_generated_vortex_output(
    output_path: &Path,
    schema: &[GeneratedColumn],
    rows: &[GeneratedRow],
    allow_overwrite: bool,
    _reuse_context: &GeneratedSourceReuseContext,
) -> Result<GeneratedOutputWriteReport, ShardLoomError> {
    validate_generated_vortex_target(output_path)?;
    let start = Instant::now();
    let request = shardloom_vortex::VortexPreparedStateWriteRequest::new(
        output_path.to_path_buf(),
        generated_column_names(schema),
        generated_rows_to_scalar_rows(schema, rows)?,
    )
    .allow_overwrite(allow_overwrite);
    let report = shardloom_vortex::write_flat_scalar_vortex_prepared_state(request)?;
    let write_millis = start.elapsed().as_millis();
    Ok(GeneratedOutputWriteReport {
        output_bytes: report.bytes_written,
        output_digest: report.artifact_digest.clone(),
        write_millis,
        workspace_write_report: report.workspace_write_report.clone(),
        vortex_report: Some(report),
        prepared_state_reuse: None,
    })
}

#[cfg(feature = "vortex-write")]
fn validate_generated_vortex_target(output_path: &Path) -> Result<(), ShardLoomError> {
    let extension = output_path
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if extension != "vortex" {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local Vortex generated-source output target '{}' must use a .vortex extension; no fallback execution was attempted",
            output_path.display()
        )));
    }
    Ok(())
}

fn generated_vortex_output_fields(
    report: Option<&shardloom_vortex::VortexPreparedStateWriteReport>,
    reuse_report: Option<&shardloom_vortex::VortexPreparedStateReuseReport>,
) -> Vec<(String, String)> {
    let _ = reuse_report;
    match report {
        Some(report) => vortex_output_success_fields(report),
        None => default_vortex_output_fields(),
    }
}

fn generated_vortex_prepared_state_fields(
    evidence: GeneratedPreparedStateEvidence<'_>,
) -> Vec<(String, String)> {
    vec![
        (
            "prepared_state_created".to_string(),
            evidence.created.to_string(),
        ),
        (
            "prepared_state_reused".to_string(),
            evidence.reused.to_string(),
        ),
        (
            "prepared_state_reuse_allowed".to_string(),
            evidence.reuse_allowed.to_string(),
        ),
        (
            "prepared_state_reuse_hit".to_string(),
            evidence.reuse_hit.to_string(),
        ),
        (
            "prepared_state_reuse_scope".to_string(),
            evidence.scope.to_string(),
        ),
        (
            "prepared_state_reuse_manifest_path".to_string(),
            evidence.manifest_path.to_string(),
        ),
        (
            "prepared_state_reuse_policy".to_string(),
            generated_prepared_state_reuse_policy(evidence.scope).to_string(),
        ),
        (
            "prepared_state_reuse_reason".to_string(),
            evidence.reason.to_string(),
        ),
        (
            "prepared_state_reuse_manifest_digest".to_string(),
            evidence.manifest_digest.to_string(),
        ),
        (
            "prepared_state_reuse_manifest_digest_algorithm".to_string(),
            digest_algorithm(evidence.manifest_digest).to_string(),
        ),
        (
            "prepared_state_invalidation_reason".to_string(),
            evidence.invalidation_reason.to_string(),
        ),
    ]
}

fn generated_prepared_state_reuse_policy(scope: &str) -> &'static str {
    match scope {
        "single_vortex_artifact_no_sidecar" => "single_vortex_artifact_no_sidecar.v1",
        "not_applicable_non_vortex_generated_output" => {
            "not_applicable_non_vortex_generated_output"
        }
        _ => shardloom_vortex::VORTEX_PREPARED_STATE_REUSE_POLICY,
    }
}

fn default_vortex_output_fields() -> Vec<(String, String)> {
    let mut fields = generated_vortex_prepared_state_fields(GeneratedPreparedStateEvidence {
        created: false,
        reused: false,
        reuse_allowed: false,
        reuse_hit: false,
        scope: "not_applicable_non_vortex_generated_output",
        manifest_path: "not_applicable_non_vortex_generated_output",
        reason: "not_requested_non_vortex_generated_output",
        manifest_digest: "none",
        invalidation_reason: "not_applicable_non_vortex_generated_output",
    });
    fields.extend([
        (
            "vortex_output_runtime_execution".to_string(),
            "false".to_string(),
        ),
        (
            "vortex_output_reopen_verified".to_string(),
            "false".to_string(),
        ),
        (
            "vortex_output_timing_scope".to_string(),
            "not_applicable".to_string(),
        ),
        (
            "vortex_output_certification_level".to_string(),
            "not_applicable".to_string(),
        ),
        (
            "vortex_artifact_digest".to_string(),
            "not_applicable".to_string(),
        ),
        (
            "vortex_artifact_digest_source".to_string(),
            "not_applicable".to_string(),
        ),
        ("vortex_artifact_bytes".to_string(), "0".to_string()),
        (
            "upstream_vortex_write_called".to_string(),
            "false".to_string(),
        ),
        (
            "upstream_vortex_scan_called".to_string(),
            "false".to_string(),
        ),
    ]);
    fields.extend(default_vortex_output_writer_evidence_fields());
    fields
}

fn default_vortex_output_writer_evidence_fields() -> Vec<(String, String)> {
    vec![
        (
            "vortex_write_timing_split_schema_version".to_string(),
            "shardloom.vortex_write_timing_split.v1".to_string(),
        ),
        ("vortex_segment_write_millis".to_string(), "0".to_string()),
        ("vortex_workspace_stage_millis".to_string(), "0".to_string()),
        (
            "vortex_writer_context_open_millis".to_string(),
            "0".to_string(),
        ),
        (
            "vortex_writer_context_reuse_status".to_string(),
            "not_applicable".to_string(),
        ),
        (
            "vortex_writer_layout_strategy_applied".to_string(),
            "not_applicable".to_string(),
        ),
        (
            "vortex_writer_coalescing_policy_status".to_string(),
            "not_applicable".to_string(),
        ),
        (
            "vortex_writer_layout_row_block_size".to_string(),
            "0".to_string(),
        ),
        (
            "vortex_writer_layout_block_target_bytes".to_string(),
            "0".to_string(),
        ),
        (
            "vortex_writer_compression_policy".to_string(),
            "not_applicable".to_string(),
        ),
        (
            "vortex_writer_compression_concurrency".to_string(),
            "0".to_string(),
        ),
        (
            "vortex_writer_stats_concurrency".to_string(),
            "0".to_string(),
        ),
        (
            "vortex_writer_profile_selection_reason".to_string(),
            "not_applicable".to_string(),
        ),
        (
            "vortex_writer_profile_regression_guard".to_string(),
            "not_applicable".to_string(),
        ),
        (
            "vortex_layout_write_decision_applied".to_string(),
            "false".to_string(),
        ),
        (
            "vortex_layout_write_decision_strategy".to_string(),
            "not_applicable".to_string(),
        ),
        (
            "vortex_layout_write_decision_digest".to_string(),
            "not_applicable".to_string(),
        ),
        (
            "vortex_layout_write_decision_provider_admitted".to_string(),
            "false".to_string(),
        ),
        (
            "vortex_layout_write_decision_blocker".to_string(),
            "not_applicable".to_string(),
        ),
    ]
}

fn vortex_output_success_fields(
    report: &shardloom_vortex::VortexPreparedStateWriteReport,
) -> Vec<(String, String)> {
    let mut fields = generated_vortex_prepared_state_fields(GeneratedPreparedStateEvidence {
        created: true,
        reused: false,
        reuse_allowed: false,
        reuse_hit: false,
        scope: "single_vortex_artifact_no_sidecar",
        manifest_path: "not_applicable_single_vortex_artifact",
        reason: "generated_source_vortex_output_writes_single_vortex_artifact_without_sidecar",
        manifest_digest: "not_applicable_single_vortex_artifact",
        invalidation_reason: "not_applicable_single_vortex_artifact",
    });
    fields.extend(vortex_output_write_success_detail_fields(report));
    fields
}

#[allow(clippy::too_many_lines)]
fn vortex_output_write_success_detail_fields(
    report: &shardloom_vortex::VortexPreparedStateWriteReport,
) -> Vec<(String, String)> {
    vec![
        (
            "vortex_output_runtime_execution".to_string(),
            "true".to_string(),
        ),
        (
            "vortex_output_reopen_verified".to_string(),
            (report.reopen_row_count == report.row_count).to_string(),
        ),
        (
            "vortex_output_row_count".to_string(),
            report.row_count.to_string(),
        ),
        (
            "vortex_output_column_count".to_string(),
            report.column_count.to_string(),
        ),
        (
            "vortex_output_column_families".to_string(),
            report.column_family_summary(),
        ),
        (
            "vortex_artifact_digest".to_string(),
            report.artifact_digest.clone(),
        ),
        (
            "vortex_artifact_bytes".to_string(),
            report.bytes_written.to_string(),
        ),
        (
            "vortex_artifact_digest_source".to_string(),
            report.artifact_digest_source.clone(),
        ),
        (
            "vortex_writer_row_count".to_string(),
            report.writer_row_count.to_string(),
        ),
        (
            "vortex_reopen_row_count".to_string(),
            report.reopen_row_count.to_string(),
        ),
        (
            "vortex_write_millis".to_string(),
            micros_to_millis(report.write_micros).to_string(),
        ),
        (
            "vortex_write_timing_split_schema_version".to_string(),
            "shardloom.vortex_write_timing_split.v1".to_string(),
        ),
        (
            "vortex_segment_write_millis".to_string(),
            micros_to_millis(report.vortex_segment_write_micros).to_string(),
        ),
        (
            "vortex_workspace_stage_millis".to_string(),
            micros_to_millis(report.workspace_stage_micros).to_string(),
        ),
        (
            "vortex_writer_context_open_millis".to_string(),
            micros_to_millis(report.writer_context_open_micros).to_string(),
        ),
        (
            "vortex_writer_context_reuse_status".to_string(),
            report.writer_context_reuse_status.clone(),
        ),
        (
            "vortex_writer_layout_strategy_applied".to_string(),
            report.writer_layout_strategy_applied.clone(),
        ),
        (
            "vortex_writer_coalescing_policy_status".to_string(),
            report.writer_coalescing_policy_status.clone(),
        ),
        (
            "vortex_writer_layout_row_block_size".to_string(),
            report.writer_layout_row_block_size.to_string(),
        ),
        (
            "vortex_writer_layout_block_target_bytes".to_string(),
            report.writer_layout_block_target_bytes.to_string(),
        ),
        (
            "vortex_writer_compression_policy".to_string(),
            report.writer_compression_policy.clone(),
        ),
        (
            "vortex_writer_compression_concurrency".to_string(),
            report.writer_compression_concurrency.to_string(),
        ),
        (
            "vortex_writer_stats_concurrency".to_string(),
            report.writer_stats_concurrency.to_string(),
        ),
        (
            "vortex_writer_profile_selection_reason".to_string(),
            report.writer_profile_selection_reason.clone(),
        ),
        (
            "vortex_writer_profile_regression_guard".to_string(),
            report.writer_profile_regression_guard.clone(),
        ),
        (
            "vortex_layout_write_decision_applied".to_string(),
            report
                .layout_write_decision
                .runtime_decision_applied
                .to_string(),
        ),
        (
            "vortex_layout_write_decision_strategy".to_string(),
            report.layout_write_decision.selected_strategy.clone(),
        ),
        (
            "vortex_layout_write_decision_digest".to_string(),
            report
                .layout_write_decision
                .strategy_decision_digest
                .clone(),
        ),
        (
            "vortex_layout_write_decision_provider_admitted".to_string(),
            report.layout_write_decision.provider_admitted.to_string(),
        ),
        (
            "vortex_layout_write_decision_blocker".to_string(),
            report.layout_write_decision.blocker.clone(),
        ),
        (
            "vortex_digest_millis".to_string(),
            micros_to_millis(report.digest_micros).to_string(),
        ),
        (
            "vortex_reopen_verify_millis".to_string(),
            micros_to_millis(report.reopen_scan_micros).to_string(),
        ),
        (
            "vortex_output_timing_scope".to_string(),
            report.timing_scope.clone(),
        ),
        (
            "vortex_output_certification_level".to_string(),
            report.certification_level.clone(),
        ),
        (
            "vortex_output_layout_summary".to_string(),
            report.layout_summary(),
        ),
        (
            "vortex_output_encoding_summary".to_string(),
            report.encoding_summary(),
        ),
        (
            "vortex_output_statistics_summary".to_string(),
            report.statistics_summary(),
        ),
        (
            "vortex_prepared_olap_layout_inventory_summary".to_string(),
            report.prepared_olap_layout_inventory_summary(),
        ),
        (
            "vortex_prepared_olap_layout_inventory_status".to_string(),
            report.prepared_olap_layout_inventory.status.clone(),
        ),
        (
            "vortex_prepared_olap_layout_inventory_digest".to_string(),
            report
                .prepared_olap_layout_inventory
                .inventory_digest
                .clone(),
        ),
        (
            "vortex_prepared_olap_layout_footer_row_count".to_string(),
            report.prepared_olap_layout_inventory.row_count_field(),
        ),
        (
            "vortex_prepared_olap_layout_footer_segment_count".to_string(),
            report.prepared_olap_layout_inventory.segment_count_field(),
        ),
        (
            "vortex_prepared_olap_layout_footer_statistics_status".to_string(),
            report
                .prepared_olap_layout_inventory
                .statistics_status
                .clone(),
        ),
        (
            "vortex_prepared_olap_layout_footer_encoding_layout_status".to_string(),
            report
                .prepared_olap_layout_inventory
                .encoding_layout_status
                .clone(),
        ),
        (
            "vortex_prepared_olap_layout_footer_approx_bytes".to_string(),
            report
                .prepared_olap_layout_inventory
                .approx_footer_bytes_field(),
        ),
        (
            "vortex_prepared_olap_layout_footer_dtype_summary".to_string(),
            report.prepared_olap_layout_inventory.dtype_summary.clone(),
        ),
        (
            "vortex_prepared_olap_layout_metadata_persisted_in_artifact".to_string(),
            report
                .prepared_olap_layout_inventory
                .metadata_persisted_in_artifact
                .to_string(),
        ),
        (
            "upstream_vortex_write_called".to_string(),
            report.upstream_vortex_write_called.to_string(),
        ),
        (
            "upstream_vortex_scan_called".to_string(),
            report.upstream_vortex_scan_called.to_string(),
        ),
    ]
}

fn generated_output_sink_artifact_fields(
    primary: GeneratedPrimarySinkArtifact<'_>,
    fanout_outputs: &[GeneratedWrittenOutput],
) -> Vec<(String, String)> {
    vec![
        (
            "sink_artifact_count".to_string(),
            (fanout_outputs.len() + 1).to_string(),
        ),
        (
            "sink_artifact_ref".to_string(),
            generated_sink_artifact_ref(primary, fanout_outputs),
        ),
        (
            "sink_artifact_refs".to_string(),
            generated_sink_artifact_refs(primary, fanout_outputs),
        ),
        (
            "sink_artifact_digest".to_string(),
            generated_sink_artifact_digest(primary, fanout_outputs),
        ),
        (
            "sink_artifact_digests".to_string(),
            generated_sink_artifact_digests(primary, fanout_outputs),
        ),
        (
            "sink_artifact_formats".to_string(),
            generated_sink_artifact_formats(primary, fanout_outputs),
        ),
        (
            "sink_artifact_bytes".to_string(),
            generated_sink_artifact_bytes(primary, fanout_outputs),
        ),
        (
            "sink_artifact_replay_statuses".to_string(),
            generated_sink_artifact_replay_statuses(primary, fanout_outputs),
        ),
        (
            "sink_artifact_native_io_certificate_statuses".to_string(),
            generated_sink_artifact_certificate_statuses(primary, fanout_outputs),
        ),
        (
            "sink_artifact_workspace_path_safety_statuses".to_string(),
            generated_sink_artifact_workspace_safety_statuses(primary, fanout_outputs),
        ),
        (
            "sink_artifact_commit_modes".to_string(),
            generated_sink_artifact_commit_modes(primary, fanout_outputs),
        ),
        (
            "sink_artifact_manifest_status".to_string(),
            generated_sink_artifact_manifest_status(primary, fanout_outputs),
        ),
    ]
}

fn generated_sink_artifact_ref(
    primary: GeneratedPrimarySinkArtifact<'_>,
    fanout_outputs: &[GeneratedWrittenOutput],
) -> String {
    if fanout_outputs.is_empty() {
        primary.output_path.display().to_string()
    } else {
        generated_sink_artifact_refs(primary, fanout_outputs)
    }
}

fn generated_sink_artifact_refs(
    primary: GeneratedPrimarySinkArtifact<'_>,
    fanout_outputs: &[GeneratedWrittenOutput],
) -> String {
    generated_fanout_csv_or_not_applicable(
        std::iter::once(format!(
            "{}:{}",
            primary.output_format.sink_label(),
            primary.output_path.display()
        ))
        .chain(fanout_outputs.iter().map(|output| {
            format!(
                "{}:{}",
                output.target.output_format.sink_label(),
                output.target.output_path.display()
            )
        })),
    )
}

fn generated_sink_artifact_digest(
    primary: GeneratedPrimarySinkArtifact<'_>,
    fanout_outputs: &[GeneratedWrittenOutput],
) -> String {
    if fanout_outputs.is_empty() {
        primary.output_digest.to_string()
    } else {
        generated_sink_artifact_digests(primary, fanout_outputs)
    }
}

fn generated_sink_artifact_digests(
    primary: GeneratedPrimarySinkArtifact<'_>,
    fanout_outputs: &[GeneratedWrittenOutput],
) -> String {
    generated_fanout_csv_or_not_applicable(
        std::iter::once(format!(
            "{}:{}",
            primary.output_format.sink_label(),
            primary.output_digest
        ))
        .chain(fanout_outputs.iter().map(|output| {
            format!(
                "{}:{}",
                output.target.output_format.sink_label(),
                output.write_report.output_digest.as_str()
            )
        })),
    )
}

fn generated_sink_artifact_formats(
    primary: GeneratedPrimarySinkArtifact<'_>,
    fanout_outputs: &[GeneratedWrittenOutput],
) -> String {
    generated_fanout_csv_or_not_applicable(
        std::iter::once(primary.output_format.sink_label().to_string()).chain(
            fanout_outputs
                .iter()
                .map(|output| output.target.output_format.sink_label().to_string()),
        ),
    )
}

fn generated_sink_artifact_bytes(
    primary: GeneratedPrimarySinkArtifact<'_>,
    fanout_outputs: &[GeneratedWrittenOutput],
) -> String {
    generated_fanout_csv_or_not_applicable(
        std::iter::once(format!(
            "{}:{}",
            primary.output_format.sink_label(),
            primary.output_bytes
        ))
        .chain(fanout_outputs.iter().map(|output| {
            format!(
                "{}:{}",
                output.target.output_format.sink_label(),
                output.write_report.output_bytes
            )
        })),
    )
}

fn generated_sink_artifact_replay_statuses(
    primary: GeneratedPrimarySinkArtifact<'_>,
    fanout_outputs: &[GeneratedWrittenOutput],
) -> String {
    generated_fanout_csv_or_not_applicable(
        std::iter::once(format!(
            "{}:{}",
            primary.output_format.sink_label(),
            primary.replay.status.as_str()
        ))
        .chain(fanout_outputs.iter().map(|output| {
            format!(
                "{}:{}",
                output.target.output_format.sink_label(),
                output.replay.status.as_str()
            )
        })),
    )
}

fn generated_sink_artifact_certificate_statuses(
    primary: GeneratedPrimarySinkArtifact<'_>,
    fanout_outputs: &[GeneratedWrittenOutput],
) -> String {
    generated_fanout_csv_or_not_applicable(
        std::iter::once(format!(
            "{}:{}",
            primary.output_format.sink_label(),
            primary.output_format.certificate_status()
        ))
        .chain(fanout_outputs.iter().map(|output| {
            format!(
                "{}:{}",
                output.target.output_format.sink_label(),
                output.target.output_format.certificate_status()
            )
        })),
    )
}

fn generated_sink_artifact_workspace_safety_statuses(
    primary: GeneratedPrimarySinkArtifact<'_>,
    fanout_outputs: &[GeneratedWrittenOutput],
) -> String {
    generated_fanout_csv_or_not_applicable(
        std::iter::once(format!(
            "{}:{}",
            primary.output_format.sink_label(),
            primary.workspace_write_report.path_safety_report.accepted()
        ))
        .chain(fanout_outputs.iter().map(|output| {
            format!(
                "{}:{}",
                output.target.output_format.sink_label(),
                output
                    .write_report
                    .workspace_write_report
                    .path_safety_report
                    .accepted()
            )
        })),
    )
}

fn generated_sink_artifact_commit_modes(
    primary: GeneratedPrimarySinkArtifact<'_>,
    fanout_outputs: &[GeneratedWrittenOutput],
) -> String {
    generated_fanout_csv_or_not_applicable(
        std::iter::once(format!(
            "{}:{}",
            primary.output_format.sink_label(),
            primary.workspace_write_report.commit_mode.as_str()
        ))
        .chain(fanout_outputs.iter().map(|output| {
            format!(
                "{}:{}",
                output.target.output_format.sink_label(),
                output
                    .write_report
                    .workspace_write_report
                    .commit_mode
                    .as_str()
            )
        })),
    )
}

fn generated_sink_artifact_manifest_status(
    primary: GeneratedPrimarySinkArtifact<'_>,
    fanout_outputs: &[GeneratedWrittenOutput],
) -> String {
    let all_replayed =
        primary.replay.verified && fanout_outputs.iter().all(|output| output.replay.verified);
    if all_replayed {
        "verified_local_sink_artifacts".to_string()
    } else {
        "blocked_unverified_local_sink_artifact".to_string()
    }
}

fn generated_output_fanout_fields(
    primary_format: GeneratedOutputFormat,
    primary_replay: &GeneratedOutputReplayEvidence,
    fanout_outputs: &[GeneratedWrittenOutput],
) -> Vec<(String, String)> {
    let mut fields = generated_output_replay_fields(primary_format, primary_replay, fanout_outputs);
    fields.extend(generated_output_fanout_summary_fields(fanout_outputs));
    fields.extend(generated_output_fanout_detail_fields(fanout_outputs));
    fields
}

fn generated_output_replay_fields(
    primary_format: GeneratedOutputFormat,
    primary_replay: &GeneratedOutputReplayEvidence,
    fanout_outputs: &[GeneratedWrittenOutput],
) -> Vec<(String, String)> {
    let fanout_performed = !fanout_outputs.is_empty();
    let all_replayed =
        primary_replay.verified && fanout_outputs.iter().all(|output| output.replay.verified);
    vec![
        (
            "output_route".to_string(),
            if fanout_performed {
                "local_sink_and_fanout"
            } else {
                "local_sink"
            }
            .to_string(),
        ),
        (
            "result_reuse_for_fanout".to_string(),
            fanout_performed.to_string(),
        ),
        (
            "fanout_result_reuse_hit".to_string(),
            fanout_performed.to_string(),
        ),
        (
            "result_replay_verified".to_string(),
            all_replayed.to_string(),
        ),
        (
            "output_replay_status".to_string(),
            if all_replayed {
                "verified_local_sink_artifacts"
            } else {
                "blocked_unverified_local_sink_artifact"
            }
            .to_string(),
        ),
        (
            "output_replay_millis".to_string(),
            (primary_replay.replay_millis
                + fanout_outputs
                    .iter()
                    .map(|output| output.replay.replay_millis)
                    .sum::<u128>())
            .to_string(),
        ),
        (
            "output_fidelity_report_status".to_string(),
            if all_replayed {
                "scoped_local_output_fidelity_reported"
            } else {
                "blocked_unverified_output_replay"
            }
            .to_string(),
        ),
        (
            "output_fidelity_loss".to_string(),
            generated_fanout_csv_or_not_applicable(
                std::iter::once(format!(
                    "{}:{}",
                    primary_format.sink_label(),
                    primary_replay.fidelity_loss
                ))
                .chain(fanout_outputs.iter().map(|output| {
                    format!(
                        "{}:{}",
                        output.target.output_format.sink_label(),
                        output.replay.fidelity_loss
                    )
                })),
            ),
        ),
        (
            "output_fanout_performed".to_string(),
            fanout_performed.to_string(),
        ),
    ]
}

fn generated_output_fanout_summary_fields(
    fanout_outputs: &[GeneratedWrittenOutput],
) -> Vec<(String, String)> {
    vec![
        (
            "fanout_output_count".to_string(),
            fanout_outputs.len().to_string(),
        ),
        (
            "fanout_output_formats".to_string(),
            generated_fanout_csv_or_not_applicable(
                fanout_outputs
                    .iter()
                    .map(|output| output.target.output_format.sink_label().to_string()),
            ),
        ),
        (
            "fanout_output_paths".to_string(),
            generated_fanout_csv_or_not_applicable(
                fanout_outputs
                    .iter()
                    .map(|output| output.target.output_path.display().to_string()),
            ),
        ),
    ]
}

fn generated_output_fanout_detail_fields(
    fanout_outputs: &[GeneratedWrittenOutput],
) -> Vec<(String, String)> {
    vec![
        (
            "fanout_output_bytes".to_string(),
            generated_fanout_labeled_values(fanout_outputs, |output| {
                output.write_report.output_bytes.to_string()
            }),
        ),
        (
            "fanout_output_digests".to_string(),
            generated_fanout_labeled_values(fanout_outputs, |output| {
                output.write_report.output_digest.clone()
            }),
        ),
        (
            "fanout_output_native_io_certificate_statuses".to_string(),
            generated_fanout_labeled_values(fanout_outputs, |output| {
                output.target.output_format.certificate_status().to_string()
            }),
        ),
        (
            "fanout_output_write_millis".to_string(),
            fanout_outputs
                .iter()
                .map(|output| output.write_report.write_millis)
                .sum::<u128>()
                .to_string(),
        ),
        (
            "fanout_output_replay_statuses".to_string(),
            generated_fanout_labeled_values(fanout_outputs, |output| output.replay.status.clone()),
        ),
        (
            "fanout_output_fidelity_statuses".to_string(),
            generated_fanout_labeled_values(fanout_outputs, |output| {
                output.replay.fidelity_status.clone()
            }),
        ),
        (
            "fanout_output_fidelity_loss".to_string(),
            generated_fanout_labeled_values(fanout_outputs, |output| {
                output.replay.fidelity_loss.clone()
            }),
        ),
        (
            "fanout_output_workspace_path_safety_statuses".to_string(),
            generated_fanout_labeled_values(fanout_outputs, |output| {
                output
                    .write_report
                    .workspace_write_report
                    .path_safety_report
                    .accepted()
                    .to_string()
            }),
        ),
        (
            "fanout_output_commit_modes".to_string(),
            generated_fanout_labeled_values(fanout_outputs, |output| {
                output
                    .write_report
                    .workspace_write_report
                    .commit_mode
                    .clone()
            }),
        ),
    ]
}

fn generated_fanout_labeled_values(
    fanout_outputs: &[GeneratedWrittenOutput],
    value: impl Fn(&GeneratedWrittenOutput) -> String,
) -> String {
    generated_fanout_csv_or_not_applicable(fanout_outputs.iter().map(|output| {
        format!(
            "{}:{}",
            output.target.output_format.sink_label(),
            value(output)
        )
    }))
}

fn generated_fanout_csv_or_not_applicable(values: impl Iterator<Item = String>) -> String {
    let collected = values.collect::<Vec<_>>();
    if collected.is_empty() {
        "not_applicable".to_string()
    } else {
        collected.join(",")
    }
}

fn micros_to_millis(value: u128) -> u128 {
    value / 1_000
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
            "generated-source range row count {row_count} exceeds runtime row limit {MAX_GENERATED_RANGE_ROWS}"
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
    filter: Option<GeneratedSqlFilterMetadata>,
    limit: Option<GeneratedSqlLimitMetadata>,
    projection: Option<GeneratedSqlProjectionMetadata>,
    order_by: Option<GeneratedSqlOrderMetadata>,
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
            "source-free SQL runtime supports only SELECT literal expressions, VALUES clauses, and SELECT * FROM generate_series/range(...)",
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
            "SQL literal SELECT runtime does not admit FROM clauses or input datasets",
        ));
    }
    if contains_outside_quotes(select_list, '(') || contains_outside_quotes(select_list, ')') {
        return Err(unsupported_sql_error(
            "SQL literal SELECT runtime does not admit functions, subqueries, or parenthesized expressions",
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
        filter: None,
        limit: None,
        projection: None,
        order_by: None,
    })
}

fn parse_sql_values(statement: &str) -> Result<ParsedSourceFreeSql, ShardLoomError> {
    let values_body = statement["VALUES".len()..].trim();
    if values_body.is_empty() {
        return Err(unsupported_sql_error(
            "SQL VALUES runtime requires at least one row tuple",
        ));
    }
    let raw_rows = parse_values_tuples(values_body)?;
    if raw_rows.is_empty() {
        return Err(unsupported_sql_error(
            "SQL VALUES runtime requires at least one row tuple",
        ));
    }
    if raw_rows.len() > MAX_SQL_GENERATED_ROWS {
        return Err(ShardLoomError::InvalidOperation(format!(
            "generated-source SQL row count {} exceeds runtime row limit {MAX_SQL_GENERATED_ROWS}",
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
        filter: None,
        limit: None,
        projection: None,
        order_by: None,
    })
}

fn parse_sql_generate_series_range(
    statement: &str,
) -> Result<Option<ParsedSourceFreeSql>, ShardLoomError> {
    let select_body = statement["SELECT".len()..].trim();
    let Some((select_list, source_ref)) = split_sql_select_from_clause(select_body)? else {
        return Ok(None);
    };
    let Some(range_clause) = parse_sql_range_source_clause(source_ref)? else {
        return Ok(None);
    };
    let GeneratedSqlRangeClause {
        range,
        filter_predicate,
        order_by,
        limit,
    } = range_clause;
    let base_rows = generated_sql_range_rows(&range)?;
    let (filtered_rows, filter) = filter_sql_range_rows(&range, &base_rows, filter_predicate)?;
    let (schema, projected_rows, projection) =
        project_sql_range_rows(select_list, &range, &filtered_rows)?;
    let (ordered_rows, order_metadata) =
        order_sql_range_rows(&range, &schema, &filtered_rows, projected_rows, order_by)?;
    let (rows, limit_metadata) = limit_sql_range_rows(ordered_rows, limit)?;
    Ok(Some(ParsedSourceFreeSql {
        statement: statement.to_string(),
        source_kind: SqlGeneratedSourceKind::GenerateSeriesRange,
        schema,
        rows,
        range: Some(range),
        filter,
        limit: limit_metadata,
        projection,
        order_by: order_metadata,
    }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedSqlRangeClause {
    range: GeneratedSqlRangeMetadata,
    filter_predicate: Option<SqlRangeProjectionPredicate>,
    order_by: Option<GeneratedSqlOrderMetadata>,
    limit: Option<usize>,
}

fn split_sql_select_from_clause(raw: &str) -> Result<Option<(&str, &str)>, ShardLoomError> {
    let Some(position) = find_keyword_outside_quotes_and_parens(raw, "FROM") else {
        return Ok(None);
    };
    let select_list = raw[..position].trim();
    let source_ref = raw[position + "FROM".len()..].trim();
    if select_list.is_empty() || source_ref.is_empty() {
        return Err(unsupported_sql_error(
            "SQL source-free range projection requires both a SELECT list and a generator source",
        ));
    }
    Ok(Some((select_list, source_ref)))
}

fn parse_sql_range_source_clause(
    raw: &str,
) -> Result<Option<GeneratedSqlRangeClause>, ShardLoomError> {
    let trimmed = raw.trim();
    let clause_start = [
        find_keyword_outside_quotes_and_parens(trimmed, "WHERE"),
        find_keyword_outside_quotes_and_parens(trimmed, "ORDER BY"),
        find_keyword_outside_quotes_and_parens(trimmed, "LIMIT"),
    ]
    .into_iter()
    .flatten()
    .min();
    let (source_ref, tail) = if let Some(index) = clause_start {
        (trimmed[..index].trim(), trimmed[index..].trim())
    } else {
        (trimmed, "")
    };
    let Some(range) = parse_sql_range_function_ref(source_ref)? else {
        return Ok(None);
    };
    let (filter_predicate, order_by, limit) = parse_sql_range_ordered_tail(tail, &range)?;
    Ok(Some(GeneratedSqlRangeClause {
        range,
        filter_predicate,
        order_by,
        limit,
    }))
}

fn parse_sql_range_ordered_tail(
    raw: &str,
    range: &GeneratedSqlRangeMetadata,
) -> Result<ParsedSqlRangeTail, ShardLoomError> {
    let mut tail = raw.trim();
    let mut filter_predicate = None;
    let mut order_by = None;
    let mut limit = None;
    if tail.is_empty() {
        return Ok((None, None, None));
    }
    if keyword_prefix(tail, "WHERE") {
        let where_body = tail["WHERE".len()..].trim();
        let clause_index = first_sql_range_tail_clause_index(where_body, &["ORDER BY", "LIMIT"]);
        let (predicate_raw, remaining_tail) = if let Some(index) = clause_index {
            (where_body[..index].trim(), where_body[index..].trim())
        } else {
            (where_body, "")
        };
        filter_predicate = Some(parse_sql_range_case_projection_predicate(
            predicate_raw,
            &range.column_name,
        )?);
        tail = remaining_tail;
    }
    if !tail.is_empty() && keyword_prefix(tail, "ORDER BY") {
        let order_body = tail["ORDER BY".len()..].trim();
        let clause_index = first_sql_range_tail_clause_index(order_body, &["LIMIT"]);
        let (order_raw, remaining_tail) = if let Some(index) = clause_index {
            (order_body[..index].trim(), order_body[index..].trim())
        } else {
            (order_body, "")
        };
        order_by = Some(parse_sql_range_order_by(order_raw)?);
        tail = remaining_tail;
    }
    if !tail.is_empty() && keyword_prefix(tail, "LIMIT") {
        limit = Some(parse_sql_range_limit(tail["LIMIT".len()..].trim())?);
        tail = "";
    }
    if !tail.is_empty() {
        return Err(unsupported_sql_error(
            "SQL source-free range source admits only optional WHERE <range-column> <comparison> <int64>, ORDER BY <column> [ASC|DESC], and LIMIT <count> clauses in that order",
        ));
    }
    Ok((filter_predicate, order_by, limit))
}

fn first_sql_range_tail_clause_index(raw: &str, keywords: &[&str]) -> Option<usize> {
    keywords
        .iter()
        .filter_map(|keyword| find_keyword_outside_quotes_and_parens(raw, keyword))
        .min()
}

fn generated_sql_range_rows(
    range: &GeneratedSqlRangeMetadata,
) -> Result<Vec<GeneratedRow>, ShardLoomError> {
    if range.end_inclusive {
        generated_inclusive_series_rows(range.start, range.end, range.step)
    } else {
        let row_count = range_row_count(range.start, range.end, range.step)?;
        if row_count > MAX_SQL_GENERATED_ROWS {
            return Err(ShardLoomError::InvalidOperation(format!(
                "generated-source SQL row count {row_count} exceeds runtime row limit {MAX_SQL_GENERATED_ROWS}"
            )));
        }
        generated_range_rows(range.start, range.end, range.step)
    }
}

fn filter_sql_range_rows(
    range: &GeneratedSqlRangeMetadata,
    base_rows: &[GeneratedRow],
    predicate: Option<SqlRangeProjectionPredicate>,
) -> Result<(Vec<GeneratedRow>, Option<GeneratedSqlFilterMetadata>), ShardLoomError> {
    let Some(predicate) = predicate else {
        return Ok((base_rows.to_vec(), None));
    };
    let mut rows = Vec::new();
    for row in base_rows {
        let source_value = sql_range_row_value(row)?;
        if predicate.evaluate(source_value) {
            rows.push(row.clone());
        }
    }
    let filter = GeneratedSqlFilterMetadata {
        source_column: range.column_name.clone(),
        predicate: predicate.evidence_label(&range.column_name),
        selected_row_count: rows.len(),
    };
    Ok((rows, Some(filter)))
}

fn limit_sql_range_rows(
    mut rows: Vec<GeneratedRow>,
    limit: Option<usize>,
) -> Result<(Vec<GeneratedRow>, Option<GeneratedSqlLimitMetadata>), ShardLoomError> {
    let Some(count) = limit else {
        return Ok((rows, None));
    };
    if count > MAX_SQL_GENERATED_ROWS {
        return Err(ShardLoomError::InvalidOperation(format!(
            "generated-source SQL LIMIT {count} exceeds runtime row limit {MAX_SQL_GENERATED_ROWS}"
        )));
    }
    rows.truncate(count);
    Ok((rows, Some(GeneratedSqlLimitMetadata { count })))
}

fn sql_range_row_value(row: &GeneratedRow) -> Result<i64, ShardLoomError> {
    row.values
        .first()
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "SQL source-free range internal row is missing its source value".to_string(),
            )
        })?
        .parse::<i64>()
        .map_err(|_| {
            ShardLoomError::InvalidOperation(
                "SQL source-free range internal row has a non-int64 value".to_string(),
            )
        })
}

fn project_sql_range_rows(
    select_list: &str,
    range: &GeneratedSqlRangeMetadata,
    base_rows: &[GeneratedRow],
) -> Result<ProjectedSqlRangeRows, ShardLoomError> {
    if select_list == "*" {
        return Ok((
            vec![GeneratedColumn {
                name: range.column_name.clone(),
                value_type: GeneratedValueType::Int64,
            }],
            base_rows.to_vec(),
            None,
        ));
    }

    let items = split_sql_csv(select_list)?;
    let mut projection = Vec::with_capacity(items.len());
    let mut schema = Vec::with_capacity(items.len());
    let mut expression_labels = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let (raw_expression, mut alias, explicit_alias) =
            split_select_alias_with_explicit(item, index + 1)?;
        let expression = parse_sql_range_projection_expression(raw_expression, &range.column_name)?;
        if !explicit_alias {
            if expression == SqlRangeProjectionExpression::Column {
                alias.clone_from(&range.column_name);
            } else {
                return Err(unsupported_sql_error(
                    "SQL source-free range computed projections require an explicit AS alias",
                ));
            }
        }
        if schema
            .iter()
            .any(|column: &GeneratedColumn| column.name == alias)
        {
            return Err(unsupported_sql_error(
                "SQL source-free range projection aliases must be unique",
            ));
        }
        expression_labels.push(expression.evidence_label(&range.column_name));
        projection.push(expression);
        schema.push(GeneratedColumn {
            name: alias,
            value_type: GeneratedValueType::Int64,
        });
    }

    let mut rows = Vec::with_capacity(base_rows.len());
    for row in base_rows {
        let source_value = sql_range_row_value(row)?;
        let values = projection
            .iter()
            .map(|expression| {
                expression
                    .evaluate(source_value)
                    .map(|value| value.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        rows.push(GeneratedRow { values });
    }

    Ok((
        schema.clone(),
        rows,
        Some(GeneratedSqlProjectionMetadata {
            source_column: range.column_name.clone(),
            columns: schema.into_iter().map(|column| column.name).collect(),
            expressions: expression_labels,
        }),
    ))
}

fn order_sql_range_rows(
    range: &GeneratedSqlRangeMetadata,
    schema: &[GeneratedColumn],
    source_rows: &[GeneratedRow],
    projected_rows: Vec<GeneratedRow>,
    order_by: Option<GeneratedSqlOrderMetadata>,
) -> Result<(Vec<GeneratedRow>, Option<GeneratedSqlOrderMetadata>), ShardLoomError> {
    let Some(order_by) = order_by else {
        return Ok((projected_rows, None));
    };
    if source_rows.len() != projected_rows.len() {
        return Err(ShardLoomError::InvalidOperation(
            "SQL source-free range internal sort row count mismatch".to_string(),
        ));
    }
    let mut sortable = Vec::with_capacity(projected_rows.len());
    for (index, row) in projected_rows.into_iter().enumerate() {
        let source_value = sql_range_row_value(&source_rows[index])?;
        let keys = order_by
            .keys
            .iter()
            .map(|key| sql_range_sort_key_value(range, schema, &row, source_value, &key.column))
            .collect::<Result<Vec<_>, _>>()?;
        sortable.push((index, keys, row));
    }
    sortable.sort_by(|left, right| {
        for (key_index, key) in order_by.keys.iter().enumerate() {
            let ordering = left.1[key_index].cmp(&right.1[key_index]);
            let ordering = match key.direction {
                GeneratedSqlSortDirection::Asc => ordering,
                GeneratedSqlSortDirection::Desc => ordering.reverse(),
            };
            if !ordering.is_eq() {
                return ordering;
            }
        }
        left.0.cmp(&right.0)
    });
    let input_row_count = sortable.len();
    let rows = sortable.into_iter().map(|(_, _, row)| row).collect();
    Ok((
        rows,
        Some(GeneratedSqlOrderMetadata {
            keys: order_by.keys,
            input_row_count,
        }),
    ))
}

fn sql_range_sort_key_value(
    range: &GeneratedSqlRangeMetadata,
    schema: &[GeneratedColumn],
    projected_row: &GeneratedRow,
    source_value: i64,
    column: &str,
) -> Result<i64, ShardLoomError> {
    if let Some(index) = schema
        .iter()
        .position(|field| field.name.eq_ignore_ascii_case(column))
    {
        return projected_row
            .values
            .get(index)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "SQL source-free range internal sort row is missing a projected value"
                        .to_string(),
                )
            })?
            .parse::<i64>()
            .map_err(|_| {
                unsupported_sql_error(
                    "SQL source-free range ORDER BY keys currently admit only int64 sort values",
                )
            });
    }
    if column.eq_ignore_ascii_case(&range.column_name) {
        return Ok(source_value);
    }
    Err(unsupported_sql_error(
        "SQL source-free range ORDER BY keys must resolve to the range source column or projected int64 output aliases",
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlRangeProjectionExpression {
    Column,
    Literal(i64),
    Add(i64),
    Subtract(i64),
    Multiply(i64),
    Case {
        predicate: SqlRangeProjectionPredicate,
        then_value: i64,
        else_value: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SqlRangeProjectionPredicate {
    operator: SqlRangeProjectionPredicateOperator,
    rhs: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlRangeProjectionPredicateOperator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl SqlRangeProjectionExpression {
    fn evaluate(self, source_value: i64) -> Result<i64, ShardLoomError> {
        match self {
            Self::Column => Ok(source_value),
            Self::Literal(value) => Ok(value),
            Self::Add(rhs) => source_value.checked_add(rhs).ok_or_else(|| {
                unsupported_sql_error("SQL source-free range projection addition overflowed int64")
            }),
            Self::Subtract(rhs) => source_value.checked_sub(rhs).ok_or_else(|| {
                unsupported_sql_error(
                    "SQL source-free range projection subtraction overflowed int64",
                )
            }),
            Self::Multiply(rhs) => source_value.checked_mul(rhs).ok_or_else(|| {
                unsupported_sql_error(
                    "SQL source-free range projection multiplication overflowed int64",
                )
            }),
            Self::Case {
                predicate,
                then_value,
                else_value,
            } => {
                if predicate.evaluate(source_value) {
                    Ok(then_value)
                } else {
                    Ok(else_value)
                }
            }
        }
    }

    fn evidence_label(self, source_column: &str) -> String {
        match self {
            Self::Column => source_column.to_string(),
            Self::Literal(value) => format!("literal_int64({value})"),
            Self::Add(rhs) => format!("{source_column}+{rhs}"),
            Self::Subtract(rhs) => format!("{source_column}-{rhs}"),
            Self::Multiply(rhs) => format!("{source_column}*{rhs}"),
            Self::Case {
                predicate,
                then_value,
                else_value,
            } => format!(
                "case({}?{}:{})",
                predicate.evidence_label(source_column),
                then_value,
                else_value
            ),
        }
    }
}

impl SqlRangeProjectionPredicate {
    fn evaluate(self, source_value: i64) -> bool {
        match self.operator {
            SqlRangeProjectionPredicateOperator::Eq => source_value == self.rhs,
            SqlRangeProjectionPredicateOperator::NotEq => source_value != self.rhs,
            SqlRangeProjectionPredicateOperator::Lt => source_value < self.rhs,
            SqlRangeProjectionPredicateOperator::LtEq => source_value <= self.rhs,
            SqlRangeProjectionPredicateOperator::Gt => source_value > self.rhs,
            SqlRangeProjectionPredicateOperator::GtEq => source_value >= self.rhs,
        }
    }

    fn evidence_label(self, source_column: &str) -> String {
        format!("{}{}{}", source_column, self.operator.label(), self.rhs)
    }
}

impl SqlRangeProjectionPredicateOperator {
    const fn label(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "!=",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
        }
    }
}

fn parse_sql_range_projection_expression(
    raw: &str,
    source_column: &str,
) -> Result<SqlRangeProjectionExpression, ShardLoomError> {
    let expression = raw.trim();
    if expression.is_empty() {
        return Err(unsupported_sql_error(
            "SQL source-free range projection expression must not be empty",
        ));
    }
    if keyword_prefix(expression, "CASE") {
        return parse_sql_range_case_projection_expression(expression, source_column);
    }
    if contains_outside_quotes(expression, '(')
        || contains_outside_quotes(expression, ')')
        || expression.contains('\'')
    {
        return Err(unsupported_sql_error(
            "SQL source-free range projection admits only the range column, int64 literals, range-column +/-/* int64 expressions, and CASE WHEN range-column comparisons with int64 branches",
        ));
    }
    if let Some(rest) = strip_sql_identifier_prefix(expression, source_column) {
        let rest = rest.trim();
        if rest.is_empty() {
            return Ok(SqlRangeProjectionExpression::Column);
        }
        let (operator, rhs_raw) = rest.split_at(1);
        let rhs = parse_sql_range_i64("projection literal", rhs_raw.trim())?;
        return match operator {
            "+" => Ok(SqlRangeProjectionExpression::Add(rhs)),
            "-" => Ok(SqlRangeProjectionExpression::Subtract(rhs)),
            "*" => Ok(SqlRangeProjectionExpression::Multiply(rhs)),
            _ => Err(unsupported_sql_error(
                "SQL source-free range projection admits only +, -, and * int64 arithmetic over the range column",
            )),
        };
    }
    parse_sql_range_i64("projection literal", expression)
        .map(SqlRangeProjectionExpression::Literal)
        .map_err(|_| {
            unsupported_sql_error(
                "SQL source-free range projection admits only the range column, int64 literals, range-column +/-/* int64 expressions, and CASE WHEN range-column comparisons with int64 branches",
            )
        })
}

fn parse_sql_range_case_projection_expression(
    expression: &str,
    source_column: &str,
) -> Result<SqlRangeProjectionExpression, ShardLoomError> {
    let when_index = find_keyword_outside_quotes_and_parens(expression, "WHEN").ok_or_else(|| {
        unsupported_sql_error(
            "SQL source-free range CASE projections must use CASE WHEN <predicate> THEN <int64> ELSE <int64> END",
        )
    })?;
    if !expression[..when_index].trim().eq_ignore_ascii_case("CASE") {
        return Err(unsupported_sql_error(
            "SQL source-free range CASE projections must start with CASE WHEN",
        ));
    }
    let then_index =
        find_keyword_outside_quotes_and_parens(expression, "THEN").ok_or_else(|| {
            unsupported_sql_error("SQL source-free range CASE projections require a THEN branch")
        })?;
    let else_index =
        find_keyword_outside_quotes_and_parens(expression, "ELSE").ok_or_else(|| {
            unsupported_sql_error("SQL source-free range CASE projections require an ELSE branch")
        })?;
    let end_index = find_keyword_outside_quotes_and_parens(expression, "END").ok_or_else(|| {
        unsupported_sql_error("SQL source-free range CASE projections require an END marker")
    })?;
    if !(when_index < then_index && then_index < else_index && else_index < end_index) {
        return Err(unsupported_sql_error(
            "SQL source-free range CASE projections must use CASE WHEN <predicate> THEN <int64> ELSE <int64> END",
        ));
    }
    if !expression[end_index + "END".len()..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "SQL source-free range CASE projections must be a single CASE expression",
        ));
    }

    let predicate = parse_sql_range_case_projection_predicate(
        expression[when_index + "WHEN".len()..then_index].trim(),
        source_column,
    )?;
    let then_value = parse_sql_range_i64(
        "CASE THEN branch",
        expression[then_index + "THEN".len()..else_index].trim(),
    )
    .map_err(|_| {
        unsupported_sql_error(
            "SQL source-free range CASE projection THEN branch must be an int64 literal",
        )
    })?;
    let else_value = parse_sql_range_i64(
        "CASE ELSE branch",
        expression[else_index + "ELSE".len()..end_index].trim(),
    )
    .map_err(|_| {
        unsupported_sql_error(
            "SQL source-free range CASE projection ELSE branch must be an int64 literal",
        )
    })?;
    Ok(SqlRangeProjectionExpression::Case {
        predicate,
        then_value,
        else_value,
    })
}

fn parse_sql_range_case_projection_predicate(
    raw: &str,
    source_column: &str,
) -> Result<SqlRangeProjectionPredicate, ShardLoomError> {
    for (token, operator) in [
        (">=", SqlRangeProjectionPredicateOperator::GtEq),
        ("<=", SqlRangeProjectionPredicateOperator::LtEq),
        ("!=", SqlRangeProjectionPredicateOperator::NotEq),
        ("<>", SqlRangeProjectionPredicateOperator::NotEq),
        ("=", SqlRangeProjectionPredicateOperator::Eq),
        (">", SqlRangeProjectionPredicateOperator::Gt),
        ("<", SqlRangeProjectionPredicateOperator::Lt),
    ] {
        if let Some(index) = raw.find(token) {
            let left = raw[..index].trim();
            let right = raw[index + token.len()..].trim();
            if !left.eq_ignore_ascii_case(source_column) {
                return Err(unsupported_sql_error(
                    "SQL source-free range CASE projection predicate must compare the range column to an int64 literal",
                ));
            }
            return Ok(SqlRangeProjectionPredicate {
                operator,
                rhs: parse_sql_range_i64("CASE predicate literal", right).map_err(|_| {
                    unsupported_sql_error(
                        "SQL source-free range CASE projection predicate rhs must be an int64 literal",
                    )
                })?,
            });
        }
    }
    Err(unsupported_sql_error(
        "SQL source-free range CASE projection predicate must use =, !=, <>, <, <=, >, or >= against an int64 literal",
    ))
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

fn parse_sql_range_limit(raw: &str) -> Result<usize, ShardLoomError> {
    let value = raw.trim();
    if value.is_empty()
        || value.starts_with('+')
        || value.starts_with('-')
        || value.contains('.')
        || value.contains('e')
        || value.contains('E')
        || value.contains('\'')
        || contains_outside_quotes(value, '(')
        || contains_outside_quotes(value, ')')
        || value.split_whitespace().count() != 1
    {
        return Err(unsupported_sql_error(
            "SQL source-free range LIMIT requires a single non-negative integer literal",
        ));
    }
    value.parse::<usize>().map_err(|_| {
        unsupported_sql_error(
            "SQL source-free range LIMIT requires a single non-negative integer literal",
        )
    })
}

fn parse_sql_range_order_by(raw: &str) -> Result<GeneratedSqlOrderMetadata, ShardLoomError> {
    let text = raw.trim();
    if text.is_empty() {
        return Err(unsupported_sql_error(
            "SQL source-free range ORDER BY requires at least one sort key",
        ));
    }
    let parts = split_sql_csv(text)?;
    let mut seen = BTreeSet::new();
    let mut keys = Vec::with_capacity(parts.len());
    for part in parts {
        let tokens = part.split_whitespace().collect::<Vec<_>>();
        let (column, direction) = match tokens.as_slice() {
            [column] => (*column, GeneratedSqlSortDirection::Asc),
            [column, direction] if direction.eq_ignore_ascii_case("ASC") => {
                (*column, GeneratedSqlSortDirection::Asc)
            }
            [column, direction] if direction.eq_ignore_ascii_case("DESC") => {
                (*column, GeneratedSqlSortDirection::Desc)
            }
            _ => {
                return Err(unsupported_sql_error(
                    "SQL source-free range ORDER BY admits only <column> [ASC|DESC] keys",
                ));
            }
        };
        validate_sql_identifier(column)?;
        let normalized = column.to_ascii_lowercase();
        if !seen.insert(normalized) {
            return Err(unsupported_sql_error(
                "SQL source-free range ORDER BY keys must be unique",
            ));
        }
        keys.push(GeneratedSqlOrderKeyMetadata {
            column: column.to_string(),
            direction,
        });
    }
    Ok(GeneratedSqlOrderMetadata {
        keys,
        input_row_count: 0,
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
                "generated-source SQL row count exceeds runtime row limit {MAX_SQL_GENERATED_ROWS}"
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
            "generated-source SQL runtime accepts only one statement",
        ));
    }
    if let Some(position) = semicolon_positions.first().copied() {
        if trimmed[position + 1..].trim().is_empty() {
            Ok(trimmed[..position].trim().to_string())
        } else {
            Err(unsupported_sql_error(
                "generated-source SQL runtime rejects multiple statements",
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
                "SQL VALUES runtime expects row tuples like VALUES (1, 'a'), (2, 'b')",
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
                        "SQL VALUES runtime does not admit nested expressions or subqueries",
                    ));
                }
                _ => {}
            }
            index += 1;
        }
        if index >= bytes.len() || in_quote {
            return Err(unsupported_sql_error(
                "SQL VALUES runtime has an unterminated row tuple or string literal",
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
                    "SQL VALUES runtime expects commas between row tuples",
                ));
            }
            index += 1;
        }
    }
    Ok(rows)
}

fn split_select_alias(item: &str, column_index: usize) -> Result<(&str, String), ShardLoomError> {
    let (expression, alias, _explicit) = split_select_alias_with_explicit(item, column_index)?;
    Ok((expression, alias))
}

fn split_select_alias_with_explicit(
    item: &str,
    column_index: usize,
) -> Result<(&str, String, bool), ShardLoomError> {
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
        Ok((literal, alias.to_string(), true))
    } else {
        Ok((item.trim(), format!("column_{column_index}"), false))
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
            "SQL NULL literals are not admitted in the first source-free runtime; null semantics are tracked by the operator-semantics slice",
        ));
    }
    if !text.contains('.')
        && !text.contains('e')
        && !text.contains('E')
        && let Ok(value) = text.parse::<i64>()
    {
        return Ok((GeneratedValueType::Int64, value.to_string()));
    }
    if let Ok(value) = text.parse::<f64>()
        && value.is_finite()
    {
        return Ok((GeneratedValueType::Float64, value.to_string()));
    }
    Err(unsupported_sql_error(
        "SQL source-free runtime admits only int64, finite float64, bool, and single-quoted utf8 literals",
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
            "SQL VALUES runtime requires each column to have a single compatible literal type",
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
    find_keyword_outside_quotes_and_parens(raw, keyword).is_some()
}

fn find_keyword_outside_quotes_and_parens(raw: &str, keyword: &str) -> Option<usize> {
    let keyword_bytes = keyword.as_bytes();
    let bytes = raw.as_bytes();
    let mut in_quote = false;
    let mut paren_depth = 0_i32;
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
            b'(' if !in_quote => paren_depth += 1,
            b')' if !in_quote && paren_depth > 0 => paren_depth -= 1,
            _ if !in_quote
                && paren_depth == 0
                && index + keyword_bytes.len() <= bytes.len()
                && bytes[index..index + keyword_bytes.len()]
                    .eq_ignore_ascii_case(keyword_bytes) =>
            {
                let before_ok = index == 0 || !is_identifier_byte(bytes[index - 1]);
                let after_index = index + keyword_bytes.len();
                let after_ok =
                    after_index >= bytes.len() || !is_identifier_byte(bytes[after_index]);
                if before_ok && after_ok {
                    return Some(index);
                }
            }
            _ => {}
        }
        index += 1;
    }
    None
}

fn is_identifier_byte(byte: u8) -> bool {
    byte == b'_' || byte.is_ascii_alphanumeric()
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
            "scoped generated-source runtime supports local file output only; object-store and remote URI writes remain blocked".to_string(),
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

fn validate_user_rows_source_kind_shape(
    source_kind: UserRowsGeneratedSourceKind,
    schema: &[GeneratedColumn],
    rows: &[GeneratedRow],
) -> Result<(), ShardLoomError> {
    match source_kind {
        UserRowsGeneratedSourceKind::DataFrameProjection => {
            if rows.len() != 1 {
                return Err(ShardLoomError::InvalidOperation(
                    "dataframe_source_free_projection admits exactly one source-free generated row"
                        .to_string(),
                ));
            }
        }
        UserRowsGeneratedSourceKind::DataFrameGeneratedWithColumn => {
            if rows.len() != 1 || schema.len() != 1 {
                return Err(ShardLoomError::InvalidOperation(
                    "dataframe_generated_with_column admits exactly one source-free generated row with one literal column"
                        .to_string(),
                ));
            }
        }
        UserRowsGeneratedSourceKind::UserRows
        | UserRowsGeneratedSourceKind::LiteralTable
        | UserRowsGeneratedSourceKind::Calendar => {}
    }
    Ok(())
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

fn render_csv(schema: &[GeneratedColumn], rows: &[GeneratedRow]) -> Result<String, ShardLoomError> {
    let mut output = String::new();
    for (index, column) in schema.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        output.push_str(&csv_escape(&column.name));
    }
    output.push('\n');
    for row in rows {
        for (index, column) in schema.iter().enumerate() {
            if index > 0 {
                output.push(',');
            }
            let value = row.values.get(index).ok_or_else(|| {
                ShardLoomError::InvalidOperation("generated-source row/schema mismatch".to_string())
            })?;
            output.push_str(&csv_escape(&render_csv_value(value, column.value_type)?));
        }
        output.push('\n');
    }
    Ok(output)
}

#[cfg(any(feature = "universal-format-io", feature = "vortex-write"))]
fn generated_column_names(schema: &[GeneratedColumn]) -> Vec<String> {
    schema.iter().map(|column| column.name.clone()).collect()
}

#[cfg(any(feature = "universal-format-io", feature = "vortex-write"))]
fn generated_rows_to_scalar_rows(
    schema: &[GeneratedColumn],
    rows: &[GeneratedRow],
) -> Result<Vec<Vec<(String, ScalarValue)>>, ShardLoomError> {
    let mut scalar_rows = Vec::with_capacity(rows.len());
    for row in rows {
        if row.values.len() != schema.len() {
            return Err(ShardLoomError::InvalidOperation(
                "generated-source row/schema mismatch".to_string(),
            ));
        }
        let mut scalar_row = Vec::with_capacity(schema.len());
        for (column, value) in schema.iter().zip(&row.values) {
            scalar_row.push((
                column.name.clone(),
                generated_value_to_scalar(value, column.value_type)?,
            ));
        }
        scalar_rows.push(scalar_row);
    }
    Ok(scalar_rows)
}

#[cfg(any(feature = "universal-format-io", feature = "vortex-write"))]
fn generated_value_to_scalar(
    value: &str,
    value_type: GeneratedValueType,
) -> Result<ScalarValue, ShardLoomError> {
    match value_type {
        GeneratedValueType::Int64 => value.parse::<i64>().map(ScalarValue::Int64).map_err(|_| {
            ShardLoomError::InvalidOperation(format!(
                "generated-source int64 value {value:?} is invalid"
            ))
        }),
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
            Ok(ScalarValue::Float64(parsed))
        }
        GeneratedValueType::Bool => match value {
            "true" => Ok(ScalarValue::Boolean(true)),
            "false" => Ok(ScalarValue::Boolean(false)),
            _ => Err(ShardLoomError::InvalidOperation(format!(
                "generated-source bool value {value:?} must be true or false"
            ))),
        },
        GeneratedValueType::Utf8 => Ok(ScalarValue::Utf8(value.to_string())),
    }
}

#[cfg(feature = "universal-format-io")]
fn encode_parquet_output_rows(
    schema: &[GeneratedColumn],
    rows: &[GeneratedRow],
) -> Result<Vec<u8>, ShardLoomError> {
    shardloom_vortex::encode_flat_parquet_rows(
        &generated_column_names(schema),
        &generated_rows_to_scalar_rows(schema, rows)?,
    )
}

#[cfg(not(feature = "universal-format-io"))]
fn encode_parquet_output_rows(
    _schema: &[GeneratedColumn],
    _rows: &[GeneratedRow],
) -> Result<Vec<u8>, ShardLoomError> {
    Err(structured_output_feature_error("Parquet"))
}

#[cfg(feature = "universal-format-io")]
fn encode_arrow_ipc_output_rows(
    schema: &[GeneratedColumn],
    rows: &[GeneratedRow],
) -> Result<Vec<u8>, ShardLoomError> {
    shardloom_vortex::encode_flat_arrow_ipc_rows(
        &generated_column_names(schema),
        &generated_rows_to_scalar_rows(schema, rows)?,
    )
}

#[cfg(not(feature = "universal-format-io"))]
fn encode_arrow_ipc_output_rows(
    _schema: &[GeneratedColumn],
    _rows: &[GeneratedRow],
) -> Result<Vec<u8>, ShardLoomError> {
    Err(structured_output_feature_error("Arrow IPC"))
}

#[cfg(feature = "universal-format-io")]
fn encode_avro_output_rows(
    schema: &[GeneratedColumn],
    rows: &[GeneratedRow],
) -> Result<Vec<u8>, ShardLoomError> {
    shardloom_vortex::encode_flat_avro_rows(
        &generated_column_names(schema),
        &generated_rows_to_scalar_rows(schema, rows)?,
    )
}

#[cfg(not(feature = "universal-format-io"))]
fn encode_avro_output_rows(
    _schema: &[GeneratedColumn],
    _rows: &[GeneratedRow],
) -> Result<Vec<u8>, ShardLoomError> {
    Err(structured_output_feature_error("Avro"))
}

#[cfg(feature = "universal-format-io")]
fn encode_orc_output_rows(
    schema: &[GeneratedColumn],
    rows: &[GeneratedRow],
) -> Result<Vec<u8>, ShardLoomError> {
    shardloom_vortex::encode_flat_orc_rows(
        &generated_column_names(schema),
        &generated_rows_to_scalar_rows(schema, rows)?,
    )
}

#[cfg(not(feature = "universal-format-io"))]
fn encode_orc_output_rows(
    _schema: &[GeneratedColumn],
    _rows: &[GeneratedRow],
) -> Result<Vec<u8>, ShardLoomError> {
    Err(structured_output_feature_error("ORC"))
}

#[cfg(not(feature = "universal-format-io"))]
fn structured_output_feature_error(format_name: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local {format_name} generated-source output runtime requires building shardloom-cli \
         with --features universal-format-io; default builds expose {format_name} as a \
         deterministic blocked sink"
    ))
}

fn render_csv_value(value: &str, value_type: GeneratedValueType) -> Result<String, ShardLoomError> {
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
        GeneratedValueType::Bool | GeneratedValueType::Utf8 => Ok(value.to_string()),
    }
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
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
    fnv64_digest_bytes(value.as_bytes())
}

fn fnv64_digest_bytes(value: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv64:{hash:016x}")
}

fn sha256_digest_bytes(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let mut encoded = String::with_capacity("sha256:".len() + digest.len() * 2);
    encoded.push_str("sha256:");
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing digest hex cannot fail");
    }
    encoded
}

fn digest_bytes_for_algorithm(value: &[u8], expected_digest: &str) -> String {
    match digest_algorithm(expected_digest) {
        "sha256" => sha256_digest_bytes(value),
        _ => fnv64_digest_bytes(value),
    }
}

fn digest_algorithm(value: &str) -> &'static str {
    match value.split_once(':').map(|(algorithm, _)| algorithm) {
        Some("sha256") => "sha256",
        Some("fnv64") => "fnv64",
        Some("fnv1a64") => "fnv1a64",
        Some("external_baseline_only") => "external_baseline_only",
        Some("none") | None => "not_available",
        Some(_) => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GeneratedOutputFormat, GeneratedUserRowsSmokeRequest, UserRowsGeneratedSourceKind,
        generated_range_rows, normalize_local_output_path, range_row_count,
    };

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

    #[test]
    fn dataframe_projection_source_kind_requires_one_generated_row() {
        let error = GeneratedUserRowsSmokeRequest::parse(
            "target/out.jsonl",
            GeneratedOutputFormat::Jsonl,
            vec![],
            UserRowsGeneratedSourceKind::DataFrameProjection,
            "value:int64",
            "value=1;value=2",
            false,
        )
        .expect_err("dataframe projection must remain one scoped generated row");
        assert!(
            error
                .to_string()
                .contains("dataframe_source_free_projection admits exactly one")
        );
    }

    #[test]
    fn dataframe_generated_with_column_source_kind_requires_one_column() {
        let error = GeneratedUserRowsSmokeRequest::parse(
            "target/out.jsonl",
            GeneratedOutputFormat::Jsonl,
            vec![],
            UserRowsGeneratedSourceKind::DataFrameGeneratedWithColumn,
            "value:int64,label:utf8",
            "value=1,label=alpha",
            false,
        )
        .expect_err("generated with column must remain one literal column");
        assert!(
            error
                .to_string()
                .contains("dataframe_generated_with_column admits exactly one")
        );
    }

    #[test]
    fn dataframe_generated_with_column_source_kind_requires_one_generated_row() {
        let error = GeneratedUserRowsSmokeRequest::parse(
            "target/out.jsonl",
            GeneratedOutputFormat::Jsonl,
            vec![],
            UserRowsGeneratedSourceKind::DataFrameGeneratedWithColumn,
            "value:int64",
            "value=1;value=2",
            false,
        )
        .expect_err("generated with column must remain one scoped generated row");
        assert!(
            error
                .to_string()
                .contains("dataframe_generated_with_column admits exactly one")
        );
    }
}
