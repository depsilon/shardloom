//! SQL declarations and local input preparation for the shared native engine.
//!
//! Parsing retains complete expressions and relational structure for native
//! admission. Compatibility readers normalize inputs into Vortex; they do not
//! evaluate queries. Execution and output belong to `shardloom-vortex`.

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
mod whole_json_typed;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
    time::Instant,
};

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
use std::{
    collections::VecDeque,
    io::{BufRead, BufReader, Read},
    sync::Arc,
};

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
use arrow_array::{RecordBatch, RecordBatchReader};

use arrow_schema::DataType;
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
use arrow_schema::{ArrowError, Field, Schema, SchemaRef, TimeUnit};
use shardloom_core::{
    CommandStatus, ExecutionResources, ExpressionInputRow, LogicalDType, OutputFormat, ScalarValue,
    ShardLoomError, decimal128_dtype,
};
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
use shardloom_core::{parse_iso_date32, parse_iso_timestamp_micros};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
    execution_resources::ResourceArguments,
};
#[cfg(any(test, all(feature = "vortex-local-primitives", unix)))]
#[path = "sql_syntax.rs"]
mod syntax;
#[cfg(all(feature = "vortex-local-primitives", unix))]
pub(crate) use syntax::native_relational;

const VORTEX_PREPARE_COMMAND: &str = "vortex-prepare";

const ADMITTED_LOCAL_SOURCE_EXTENSIONS: &str =
    ".csv,.json,.jsonl,.ndjson,.parquet,.arrow,.ipc,.feather,.avro,.orc";
const VORTEX_PREPARE_SCHEMA_VERSION: &str = "shardloom.vortex_prepare.v1";
const LOCAL_SOURCE_STATE_SCHEMA_VERSION: &str = "shardloom.local_source_state.v1";
const LOCAL_INPUT_ADAPTER_REGISTRY_VERSION: &str = "shardloom.local_input_adapter_registry.v1";
const MAX_INPUT_ROWS: usize = 50_000;
const MAX_LIMIT_ROWS: usize = 10_000;
const MAX_JOIN_CANDIDATE_ROWS: usize = MAX_INPUT_ROWS;
const MAX_LOCAL_SOURCE_BYTES: u64 = 128 * 1024 * 1024;
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
const LOCAL_OLAP_MEDIUM_SOURCE_ROW_THRESHOLD: usize = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalSourceReadPlan {
    required_columns: Option<BTreeSet<String>>,
    reason: &'static str,
}

impl LocalSourceReadPlan {
    fn full(reason: &'static str) -> Self {
        Self {
            required_columns: None,
            reason,
        }
    }

    #[cfg(any(test, all(feature = "vortex-write", feature = "universal-format-io")))]
    fn required(columns: BTreeSet<String>, reason: &'static str) -> Self {
        Self {
            required_columns: Some(columns),
            reason,
        }
    }

    fn should_materialize(&self, column: &str) -> bool {
        match self.required_columns.as_ref() {
            Some(columns) => columns.contains(column),
            None => true,
        }
    }

    fn materialized_columns(&self, header: &[String]) -> Vec<String> {
        header
            .iter()
            .filter(|column| self.should_materialize(column))
            .cloned()
            .collect()
    }

    fn requested_columns(&self) -> String {
        self.required_columns.as_ref().map_or_else(
            || "all".to_string(),
            |columns| {
                if columns.is_empty() {
                    "none".to_string()
                } else {
                    columns.iter().cloned().collect::<Vec<_>>().join(",")
                }
            },
        )
    }

    const fn status(&self) -> &'static str {
        if self.required_columns.is_some() {
            "required_columns"
        } else {
            "full_columns"
        }
    }

    #[cfg(feature = "universal-format-io")]
    fn required_columns_vec(&self) -> Option<Vec<String>> {
        self.required_columns
            .as_ref()
            .map(|columns| columns.iter().cloned().collect())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlLocalSourceRuntimeProfile {
    Smoke,
    ProductLocalWorkflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalSourceReadLimits {
    input_rows: Option<usize>,
    source_bytes: Option<u64>,
    output_rows: Option<usize>,
    join_candidate_rows: Option<usize>,
}

impl SqlLocalSourceRuntimeProfile {
    const fn read_limits(self) -> LocalSourceReadLimits {
        match self {
            Self::Smoke => LocalSourceReadLimits {
                input_rows: Some(MAX_INPUT_ROWS),
                source_bytes: Some(MAX_LOCAL_SOURCE_BYTES),
                output_rows: Some(MAX_LIMIT_ROWS),
                join_candidate_rows: self.join_candidate_cap(),
            },
            Self::ProductLocalWorkflow => LocalSourceReadLimits {
                input_rows: None,
                source_bytes: None,
                output_rows: None,
                join_candidate_rows: self.join_candidate_cap(),
            },
        }
    }

    const fn input_row_cap_label(self) -> &'static str {
        match self {
            Self::Smoke => "50000",
            Self::ProductLocalWorkflow => "none_synthetic_row_cap_disabled",
        }
    }

    const fn synthetic_input_row_cap_enabled(self) -> bool {
        matches!(self, Self::Smoke)
    }

    const fn synthetic_source_byte_cap_enabled(self) -> bool {
        matches!(self, Self::Smoke)
    }

    const fn synthetic_output_row_cap_enabled(self) -> bool {
        matches!(self, Self::Smoke)
    }

    const fn join_candidate_cap(self) -> Option<usize> {
        match self {
            Self::Smoke => Some(MAX_JOIN_CANDIDATE_ROWS),
            Self::ProductLocalWorkflow => None,
        }
    }

    const fn synthetic_join_candidate_cap_enabled(self) -> bool {
        matches!(self, Self::Smoke)
    }

    const fn support_status(self) -> &'static str {
        match self {
            Self::Smoke => "fixture_smoke_supported",
            Self::ProductLocalWorkflow => "production_admitted_local_workflow",
        }
    }

    const fn source_adapter_status(self) -> &'static str {
        match self {
            Self::Smoke => "smoke_supported",
            Self::ProductLocalWorkflow => "production_admitted_local_workflow",
        }
    }

    const fn ingress_certification_level(self) -> &'static str {
        match self {
            Self::Smoke => "fixture_smoke",
            Self::ProductLocalWorkflow => "local_workflow_runtime",
        }
    }

    const fn certification_status(self) -> &'static str {
        match self {
            Self::Smoke => "fixture_smoke_certified",
            Self::ProductLocalWorkflow => "production_admitted_local_workflow_certified",
        }
    }

    const fn certification_blocker_id(self) -> &'static str {
        match self {
            Self::Smoke => "not_claim_grade_fixture_smoke",
            Self::ProductLocalWorkflow => "none_product_local_workflow_admitted",
        }
    }

    const fn source_adapter_blocker_id(self) -> &'static str {
        match self {
            Self::Smoke => "none_internal_local_source_runtime_diagnostic",
            Self::ProductLocalWorkflow => "none_product_local_workflow_route",
        }
    }

    const fn claim_gate_status(self) -> &'static str {
        match self {
            Self::Smoke => "fixture_smoke_only",
            Self::ProductLocalWorkflow => "local_workflow_runtime_supported",
        }
    }
}

fn vortex_prepare_usage() -> String {
    format!(
        "usage: shardloom {VORTEX_PREPARE_COMMAND} <local-source-path> <target.vortex> \
         [--input-format csv|json|jsonl|parquet|arrow-ipc|avro|orc|vortex] \
         [--schema name:dtype,...] [--allow-overwrite] \
         [--certification-level ingest_minimal|ingest_certified|ingest_full_replay] \
         [--source-fingerprint-policy metadata_only|content_digest] \
         [--memory-gb <n>] \
         [--max-parallelism <n>] \
         [--delta-source <local-source-path> --delta-target <delta.vortex> \
         [--delta-update-mode append-only|update|delete|upsert]] \
         [--internal-smoke-local-source] [--format text|json]"
    )
}

fn vortex_prepare_usage_error(missing: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "{}; missing {missing}; no fallback execution was attempted",
        vortex_prepare_usage()
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSourceFormat {
    Csv,
    Json,
    JsonLines,
    Parquet,
    ArrowIpc,
    Avro,
    Orc,
}

fn source_format_token_is_vortex(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().replace('_', "-").as_str(),
        "vortex" | "vtx"
    )
}

fn path_has_vortex_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .is_some_and(source_format_token_is_vortex)
}

impl LocalSourceFormat {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "csv" => Some(Self::Csv),
            "json" => Some(Self::Json),
            "jsonl" | "ndjson" => Some(Self::JsonLines),
            "parquet" => Some(Self::Parquet),
            "arrow-ipc" | "arrow" | "ipc" | "feather" => Some(Self::ArrowIpc),
            "avro" => Some(Self::Avro),
            "orc" => Some(Self::Orc),
            _ => None,
        }
    }

    fn from_extension(extension: &str) -> Option<Self> {
        match extension.to_ascii_lowercase().as_str() {
            "csv" => Some(Self::Csv),
            "json" => Some(Self::Json),
            "jsonl" | "ndjson" => Some(Self::JsonLines),
            "parquet" => Some(Self::Parquet),
            "arrow" | "ipc" | "feather" => Some(Self::ArrowIpc),
            "avro" => Some(Self::Avro),
            "orc" => Some(Self::Orc),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
            Self::JsonLines => "jsonl",
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow_ipc",
            Self::Avro => "avro",
            Self::Orc => "orc",
        }
    }

    const fn row_label(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::Json => "JSON",
            Self::JsonLines => "JSONL",
            Self::Parquet => "Parquet",
            Self::ArrowIpc => "Arrow IPC",
            Self::Avro => "Avro",
            Self::Orc => "ORC",
        }
    }

    const fn adapter_id(self) -> &'static str {
        match self {
            Self::Csv => "local_csv_input_adapter",
            Self::Json => "local_json_input_adapter",
            Self::JsonLines => "local_jsonl_input_adapter",
            Self::Parquet => "local_parquet_input_adapter",
            Self::ArrowIpc => "local_arrow_ipc_input_adapter",
            Self::Avro => "local_avro_input_adapter",
            Self::Orc => "local_orc_input_adapter",
        }
    }

    const fn adapter_registry_entry_id(self) -> &'static str {
        match self {
            Self::Csv => "shardloom.local_input_adapter.csv.v1",
            Self::Json => "shardloom.local_input_adapter.json.v1",
            Self::JsonLines => "shardloom.local_input_adapter.jsonl.v1",
            Self::Parquet => "shardloom.local_input_adapter.parquet.v1",
            Self::ArrowIpc => "shardloom.local_input_adapter.arrow_ipc.v1",
            Self::Avro => "shardloom.local_input_adapter.avro.v1",
            Self::Orc => "shardloom.local_input_adapter.orc.v1",
        }
    }

    const fn admitted_extensions(self) -> &'static str {
        match self {
            Self::Csv => ".csv",
            Self::Json => ".json",
            Self::JsonLines => ".jsonl,.ndjson",
            Self::Parquet => ".parquet",
            Self::ArrowIpc => ".arrow,.ipc,.feather",
            Self::Avro => ".avro",
            Self::Orc => ".orc",
        }
    }

    const fn feature_gate(self) -> &'static str {
        match self {
            Self::Csv | Self::Json | Self::JsonLines => "default",
            Self::Parquet | Self::ArrowIpc | Self::Avro | Self::Orc => "universal-format-io",
        }
    }

    const fn adapter_boundary(self) -> &'static str {
        match self {
            Self::Csv | Self::Json | Self::JsonLines => "local_text_source_state_adapter",
            Self::Parquet | Self::ArrowIpc | Self::Avro | Self::Orc => {
                "local_columnar_source_state_adapter"
            }
        }
    }

    const fn scalar_parse_normalization(self) -> &'static str {
        match self {
            Self::Csv | Self::Json | Self::JsonLines => "local_text_to_scalar_rows",
            Self::Parquet | Self::ArrowIpc | Self::Avro | Self::Orc => {
                "arrow_record_batch_to_scalar_rows"
            }
        }
    }

    fn projection_pushdown_status(
        self,
        read_plan: &LocalSourceReadPlan,
    ) -> LocalSourceProjectionPushdownStatus {
        if read_plan.required_columns.is_none() {
            return LocalSourceProjectionPushdownStatus::NotRequestedFullRead;
        }
        match self {
            Self::Csv | Self::Json | Self::JsonLines => {
                LocalSourceProjectionPushdownStatus::TextParserColumnPruning
            }
            Self::Parquet | Self::ArrowIpc | Self::Avro | Self::Orc => {
                LocalSourceProjectionPushdownStatus::ReaderLevelProjection
            }
        }
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    const fn preserves_columnar_vortex_ingest_source_state(self) -> bool {
        matches!(
            self,
            Self::Parquet | Self::ArrowIpc | Self::Avro | Self::Orc
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalInputAdapterSelection {
    source_format: LocalSourceFormat,
    source_extension: String,
    selection_kind: LocalInputAdapterSelectionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalInputAdapterSelectionKind {
    PathExtension,
    DeclaredInputFormat,
}

impl LocalInputAdapterSelection {
    fn infer_from_path(path: &Path) -> Result<Self, ShardLoomError> {
        let Some(extension) = path.extension().and_then(|value| value.to_str()) else {
            return Err(unsupported_sql_error(&format!(
                "local input adapter registry requires a supported file extension; admitted local source extensions are {ADMITTED_LOCAL_SOURCE_EXTENSIONS}"
            )));
        };
        let normalized_extension = extension.to_ascii_lowercase();
        let Some(source_format) = LocalSourceFormat::from_extension(&normalized_extension) else {
            return Err(unsupported_sql_error(&format!(
                "local input adapter registry cannot infer a supported source adapter from extension '.{normalized_extension}'; admitted local source extensions are {ADMITTED_LOCAL_SOURCE_EXTENSIONS}"
            )));
        };
        Ok(Self {
            source_format,
            source_extension: normalized_extension,
            selection_kind: LocalInputAdapterSelectionKind::PathExtension,
        })
    }

    fn select(
        path: &Path,
        source_format_override: Option<LocalSourceFormat>,
    ) -> Result<Self, ShardLoomError> {
        source_format_override.map_or_else(
            || Self::infer_from_path(path),
            |source_format| {
                Ok(Self {
                    source_format,
                    source_extension: "not_applicable".to_string(),
                    selection_kind: LocalInputAdapterSelectionKind::DeclaredInputFormat,
                })
            },
        )
    }

    const fn source_format_inferred(&self) -> bool {
        matches!(
            self.selection_kind,
            LocalInputAdapterSelectionKind::PathExtension
        )
    }

    const fn inference_kind(&self) -> &'static str {
        match self.selection_kind {
            LocalInputAdapterSelectionKind::PathExtension => "path_extension",
            LocalInputAdapterSelectionKind::DeclaredInputFormat => "declared_input_format",
        }
    }

    const fn inference_registry_route(&self) -> &'static str {
        match self.selection_kind {
            LocalInputAdapterSelectionKind::PathExtension => {
                "local_path_extension_adapter_registry"
            }
            LocalInputAdapterSelectionKind::DeclaredInputFormat => {
                "explicit_public_workflow_input_format"
            }
        }
    }

    const fn selection_reason(&self) -> &'static str {
        match self.selection_kind {
            LocalInputAdapterSelectionKind::PathExtension => "inferred_at_read_ingest_boundary",
            LocalInputAdapterSelectionKind::DeclaredInputFormat => {
                "declared_by_public_workflow_facade"
            }
        }
    }

    fn source_extension_field(&self) -> String {
        match self.selection_kind {
            LocalInputAdapterSelectionKind::PathExtension => format!(".{}", self.source_extension),
            LocalInputAdapterSelectionKind::DeclaredInputFormat => "not_applicable".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSourceProjectionPushdownStatus {
    NotRequestedFullRead,
    TextParserColumnPruning,
    ReaderLevelProjection,
}

impl LocalSourceProjectionPushdownStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NotRequestedFullRead => "not_requested_full_read",
            Self::TextParserColumnPruning => "local_text_parser_column_pruning",
            Self::ReaderLevelProjection => "reader_level_projection",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct LocalSourceReadContent {
    header: Vec<String>,
    column_dtypes: Vec<Option<LogicalDType>>,
    column_arrow_dtypes: Vec<Option<DataType>>,
    rows: Vec<ExpressionInputRow>,
    reader_projection_columns: Option<Vec<String>>,
    source_to_columnar_millis: u128,
    record_batch_count: usize,
    materialization_layout: &'static str,
    parse_normalization: &'static str,
    columnar_source_preserved: bool,
}

impl LocalSourceReadContent {
    fn text(
        source_format: LocalSourceFormat,
        header: Vec<String>,
        rows: Vec<ExpressionInputRow>,
    ) -> Self {
        let column_dtypes = vec![None; header.len()];
        let column_arrow_dtypes = vec![None; header.len()];
        Self {
            header,
            column_dtypes,
            column_arrow_dtypes,
            rows,
            reader_projection_columns: None,
            source_to_columnar_millis: 0,
            record_batch_count: 0,
            materialization_layout: "scalar_row_map",
            parse_normalization: source_format.scalar_parse_normalization(),
            columnar_source_preserved: false,
        }
    }

    #[cfg(feature = "universal-format-io")]
    fn columnar_then_scalar(
        header: Vec<String>,
        column_dtypes: Vec<Option<LogicalDType>>,
        column_arrow_dtypes: Vec<Option<DataType>>,
        rows: Vec<ExpressionInputRow>,
        reader_projection_columns: Vec<String>,
        source_to_columnar_millis: u128,
        record_batch_count: usize,
    ) -> Self {
        Self {
            header,
            column_dtypes,
            column_arrow_dtypes,
            rows,
            reader_projection_columns: Some(reader_projection_columns),
            source_to_columnar_millis,
            record_batch_count,
            materialization_layout: "arrow_record_batch_columnar_source_state_then_scalar_row_map",
            parse_normalization: "structured_reader_to_arrow_record_batches_then_scalar_rows",
            columnar_source_preserved: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CsvSourceData {
    source_adapter: LocalInputAdapterSelection,
    source_format: LocalSourceFormat,
    header: Vec<String>,
    column_dtypes: Vec<Option<LogicalDType>>,
    column_arrow_dtypes: Vec<Option<DataType>>,
    rows: Vec<ExpressionInputRow>,
    read_plan: LocalSourceReadPlan,
    materialized_columns: Vec<String>,
    reader_projection_columns: Vec<String>,
    projection_pushdown_status: LocalSourceProjectionPushdownStatus,
    source_bytes: u64,
    source_digest: String,
    source_metadata_scout_millis: u128,
    source_byte_acquisition_millis: u128,
    source_full_body_millis: u128,
    read_millis: u128,
    parse_millis: u128,
    source_to_columnar_millis: u128,
    record_batch_count: usize,
    materialization_layout: &'static str,
    parse_normalization: &'static str,
    columnar_source_preserved: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct SourceFingerprintEvidence {
    kind: String,
    policy: String,
    identity_source: String,
    content_requested: bool,
    content_performed: bool,
}

impl SourceFingerprintEvidence {
    #[cfg(test)]
    fn metadata_only() -> Self {
        Self {
            kind: "local_file_metadata_size_mtime".to_string(),
            policy: "metadata_only".to_string(),
            identity_source: "local_file_metadata_fast_prepare_identity".to_string(),
            content_requested: false,
            content_performed: false,
        }
    }

    #[cfg(not(all(feature = "vortex-write", feature = "universal-format-io")))]
    fn content_digest() -> Self {
        Self {
            kind: "local_file_content_digest".to_string(),
            policy: "content_digest".to_string(),
            identity_source: "local_file_explicit_proof_digest".to_string(),
            content_requested: true,
            content_performed: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct VortexIngestSourceData {
    source_adapter: LocalInputAdapterSelection,
    source_format: LocalSourceFormat,
    header: Vec<String>,
    column_arrow_dtypes: Vec<Option<DataType>>,
    read_plan: LocalSourceReadPlan,
    materialized_columns: Vec<String>,
    reader_projection_columns: Vec<String>,
    projection_pushdown_status: LocalSourceProjectionPushdownStatus,
    source_bytes: u64,
    source_digest: String,
    source_fingerprint: SourceFingerprintEvidence,
    row_count: usize,
    row_count_known: bool,
    source_split_row_ranges: Vec<(usize, usize)>,
    source_metadata_scout_millis: u128,
    source_byte_acquisition_millis: u128,
    source_full_body_millis: u128,
    read_millis: u128,
    compatibility_parse_millis: u128,
    source_to_columnar_millis: u128,
    record_batch_count: usize,
    source_stream_batch_size: usize,
    source_stream_unit_count_hint: Option<usize>,
    source_stream_unit_hint_kind: String,
    source_stream_policy: String,
    source_dictionary_preservation_status: String,
    ingest_executor_status: String,
    ingest_executor_kind: String,
    ingest_executor_requested_parallelism: usize,
    ingest_executor_applied_parallelism: usize,
    ingest_executor_unit_count_hint: Option<usize>,
    materialization_layout: &'static str,
    parse_normalization: &'static str,
    columnar_source_preserved: bool,
}

impl VortexIngestSourceData {
    #[cfg(not(all(feature = "vortex-write", feature = "universal-format-io")))]
    fn from_scalar_source(source: CsvSourceData) -> Self {
        let row_count = source.rows.len();
        Self {
            source_adapter: source.source_adapter.clone(),
            row_count,
            row_count_known: true,
            source_split_row_ranges: single_source_split_row_ranges(row_count),
            compatibility_parse_millis: source.parse_millis,
            source_to_columnar_millis: source.source_to_columnar_millis,
            record_batch_count: source.record_batch_count,
            source_stream_batch_size: 0,
            source_stream_unit_count_hint: None,
            source_stream_unit_hint_kind: "not_applicable_scalar_text_source".to_string(),
            source_stream_policy: "not_applicable_scalar_text_source".to_string(),
            source_dictionary_preservation_status: "not_applicable_scalar_text_source".to_string(),
            ingest_executor_status: "not_applicable_scalar_text_source".to_string(),
            ingest_executor_kind: "scalar_text_source_materialized_before_vortex_write".to_string(),
            ingest_executor_requested_parallelism: 1,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint: Some(source.record_batch_count),
            materialization_layout: source.materialization_layout,
            parse_normalization: source.parse_normalization,
            columnar_source_preserved: source.columnar_source_preserved,
            source_format: source.source_format,
            header: source.header,
            column_arrow_dtypes: source.column_arrow_dtypes,
            read_plan: source.read_plan,
            materialized_columns: source.materialized_columns,
            reader_projection_columns: source.reader_projection_columns,
            projection_pushdown_status: source.projection_pushdown_status,
            source_bytes: source.source_bytes,
            source_digest: source.source_digest,
            source_fingerprint: SourceFingerprintEvidence::content_digest(),
            source_metadata_scout_millis: source.source_metadata_scout_millis,
            source_byte_acquisition_millis: source.source_byte_acquisition_millis,
            source_full_body_millis: source.source_full_body_millis,
            read_millis: source.read_millis,
        }
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    fn from_columnar_stream_source(
        source_adapter: LocalInputAdapterSelection,
        columnar_source: &shardloom_vortex::FlatLocalColumnarStreamSource,
        scout: ColumnarSourceScoutEvidence,
        read_millis: u128,
        source_to_columnar_millis: u128,
    ) -> Self {
        let row_count = columnar_source.row_count_hint.unwrap_or(0);
        let source_split_row_ranges = source_unit_split_row_ranges(
            row_count,
            columnar_source.source_stream_unit_row_ranges.as_deref(),
        );
        Self {
            source_format: source_adapter.source_format,
            source_adapter,
            header: columnar_source.header.clone(),
            column_arrow_dtypes: columnar_source.column_arrow_dtypes.clone(),
            read_plan: LocalSourceReadPlan::full("streaming_columnar_source_state_default"),
            materialized_columns: columnar_source.materialized_columns.clone(),
            reader_projection_columns: columnar_source.reader_projection_columns.clone(),
            projection_pushdown_status: LocalSourceProjectionPushdownStatus::NotRequestedFullRead,
            source_bytes: scout.bytes,
            source_digest: scout.digest,
            source_fingerprint: SourceFingerprintEvidence {
                kind: scout.fingerprint_kind,
                policy: scout.fingerprint_policy,
                identity_source: scout.identity_source,
                content_requested: scout.content_fingerprint_requested,
                content_performed: scout.content_fingerprint_performed,
            },
            row_count,
            row_count_known: columnar_source.row_count_hint.is_some(),
            source_split_row_ranges,
            source_metadata_scout_millis: scout.metadata_scout_millis,
            source_byte_acquisition_millis: scout.byte_acquisition_millis,
            source_full_body_millis: scout.full_body_millis,
            read_millis,
            compatibility_parse_millis: 0,
            source_to_columnar_millis,
            record_batch_count: columnar_source.record_batch_count_hint.unwrap_or(0),
            source_stream_batch_size: columnar_source.source_stream_batch_size,
            source_stream_unit_count_hint: columnar_source.source_stream_unit_count_hint,
            source_stream_unit_hint_kind: columnar_source.source_stream_unit_hint_kind.clone(),
            source_stream_policy: columnar_source.source_stream_policy.clone(),
            source_dictionary_preservation_status: columnar_source
                .source_dictionary_preservation_status
                .clone(),
            ingest_executor_status: columnar_source.ingest_executor_status.clone(),
            ingest_executor_kind: columnar_source.ingest_executor_kind.clone(),
            ingest_executor_requested_parallelism: columnar_source
                .ingest_executor_requested_parallelism,
            ingest_executor_applied_parallelism: columnar_source
                .ingest_executor_applied_parallelism,
            ingest_executor_unit_count_hint: columnar_source.ingest_executor_unit_count_hint,
            materialization_layout: "streaming_arrow_record_batch_columnar_source_state",
            parse_normalization: "structured_reader_to_streaming_arrow_record_batches",
            columnar_source_preserved: true,
        }
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    fn with_observed_streaming_write(mut self, row_count: u64, record_batch_count: usize) -> Self {
        self.row_count = usize::try_from(row_count).unwrap_or(usize::MAX);
        self.row_count_known = true;
        self.source_split_row_ranges =
            source_unit_split_row_ranges(self.row_count, Some(&self.source_split_row_ranges));
        self.record_batch_count = record_batch_count;
        if record_batch_count > 0 {
            self.ingest_executor_applied_parallelism = self
                .ingest_executor_applied_parallelism
                .min(record_batch_count.max(1));
        }
        if self.ingest_executor_unit_count_hint.is_none() {
            self.ingest_executor_unit_count_hint = Some(record_batch_count);
        }
        self
    }

    fn materialized_columns_field(&self) -> String {
        if self.materialized_columns.is_empty() {
            "none".to_string()
        } else {
            self.materialized_columns.join(",")
        }
    }

    fn reader_projection_columns_field(&self) -> String {
        if self.reader_projection_columns.is_empty() {
            "none".to_string()
        } else {
            self.reader_projection_columns.join(",")
        }
    }

    fn source_stream_policy_evidence(&self, key: &str) -> String {
        semicolon_evidence_value(&self.source_stream_policy, key)
    }

    fn source_dictionary_evidence(&self, key: &str) -> String {
        semicolon_evidence_value(&self.source_dictionary_preservation_status, key)
    }

    fn pruned_column_count(&self) -> usize {
        self.header
            .len()
            .saturating_sub(self.materialized_columns.len())
    }

    fn column_pruning_applied(&self) -> bool {
        self.pruned_column_count() > 0
    }

    fn source_read_buffer_carry_status(&self) -> &'static str {
        if self.materialization_layout == "whole_json_typed_columns_with_batched_writer" {
            "read_once_buffer_carried_to_text_parser"
        } else if self.columnar_source_preserved {
            if self.source_fingerprint.content_performed {
                "streamed_source_fingerprint_reader_reopens_columnar_source"
            } else {
                "metadata_only_source_identity_columnar_reader_not_preopened"
            }
        } else {
            "read_once_buffer_carried_to_text_parser"
        }
    }

    fn source_read_scout_timing_split_status(&self) -> &'static str {
        if self.source_fingerprint.content_performed {
            "metadata_scout_and_content_fingerprint_split_recorded"
        } else {
            "metadata_scout_only_content_fingerprint_not_requested"
        }
    }

    fn source_read_mmap_eligibility_status(&self) -> &'static str {
        if self.materialization_layout == "whole_json_typed_columns_with_batched_writer" {
            "not_used_owned_text_buffer_default"
        } else if self.columnar_source_preserved {
            "not_used_columnar_reader_owns_buffer_lifetime"
        } else {
            "not_used_owned_text_buffer_default"
        }
    }

    fn adapter_id(&self) -> &'static str {
        self.source_format.adapter_id()
    }

    fn adapter_registry_entry_id(&self) -> &'static str {
        self.source_format.adapter_registry_entry_id()
    }

    fn adapter_admitted_extensions(&self) -> &'static str {
        self.source_format.admitted_extensions()
    }

    fn adapter_feature_gate(&self) -> &'static str {
        self.source_format.feature_gate()
    }

    fn adapter_boundary(&self) -> &'static str {
        self.source_format.adapter_boundary()
    }

    fn source_state_id(&self) -> String {
        format!(
            "local-{}-{}",
            self.source_format.as_str(),
            self.source_digest.replace(':', "-")
        )
    }

    fn source_state_digest(&self, source_schema_digest: &str) -> String {
        fnv64_digest(&format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            self.source_format.as_str(),
            self.source_adapter.source_extension,
            self.source_adapter.inference_kind(),
            self.source_digest,
            self.source_fingerprint.kind,
            self.source_fingerprint.policy,
            self.source_fingerprint.identity_source,
            self.source_fingerprint.content_performed,
            source_schema_digest,
            self.row_count,
            self.row_count_known,
            self.source_bytes,
            self.read_plan.requested_columns(),
            self.materialized_columns_field(),
            self.reader_projection_columns_field(),
            self.projection_pushdown_status.as_str(),
            self.materialization_layout,
            self.columnar_source_preserved,
            self.source_stream_batch_size,
            self.source_stream_unit_count_hint
                .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
            self.source_stream_unit_hint_kind,
            self.source_stream_policy,
            self.source_dictionary_preservation_status,
            self.ingest_executor_status,
            self.ingest_executor_kind,
            self.ingest_executor_applied_parallelism
        ))
    }

    fn preparation_spine_row_ranges(&self) -> Vec<(usize, usize)> {
        if self.source_split_row_ranges.is_empty() && self.row_count > 0 {
            single_source_split_row_ranges(self.row_count)
        } else {
            self.source_split_row_ranges.clone()
        }
    }

    fn preparation_spine_source_split_count(&self) -> usize {
        self.preparation_spine_row_ranges().len()
    }

    fn preparation_spine_source_split_refs(&self, source_state_id: &str) -> String {
        let ranges = self.preparation_spine_row_ranges();
        if ranges.is_empty() {
            return "none".to_string();
        }
        ranges
            .iter()
            .enumerate()
            .map(|(index, (start, end))| {
                format!(
                    "{source_state_id}:split={}:bytes=0..{}:rows={}..{}",
                    index + 1,
                    self.source_bytes,
                    start,
                    end
                )
            })
            .collect::<Vec<_>>()
            .join(";")
    }

    fn preparation_spine_source_byte_range_refs(&self, source_state_id: &str) -> String {
        let ranges = self.preparation_spine_row_ranges();
        if ranges.is_empty() {
            return "none".to_string();
        }
        ranges
            .iter()
            .enumerate()
            .map(|(index, _)| {
                format!(
                    "{source_state_id}:split={}:bytes=0..{}",
                    index + 1,
                    self.source_bytes
                )
            })
            .collect::<Vec<_>>()
            .join(";")
    }

    fn preparation_spine_source_row_range_refs(&self, source_state_id: &str) -> String {
        let ranges = self.preparation_spine_row_ranges();
        if ranges.is_empty() {
            return "none".to_string();
        }
        ranges
            .iter()
            .enumerate()
            .map(|(index, (start, end))| {
                format!(
                    "{source_state_id}:split={}:rows={}..{}",
                    index + 1,
                    start,
                    end
                )
            })
            .collect::<Vec<_>>()
            .join(";")
    }
}

fn single_source_split_row_ranges(row_count: usize) -> Vec<(usize, usize)> {
    if row_count == 0 {
        Vec::new()
    } else {
        vec![(0, row_count)]
    }
}

fn semicolon_evidence_value(raw: &str, key: &str) -> String {
    let prefix = format!("{key}=");
    raw.split(';')
        .find_map(|token| token.strip_prefix(&prefix))
        .filter(|value| !value.trim().is_empty())
        .map_or_else(|| "not_available".to_string(), ToString::to_string)
}

#[cfg_attr(
    not(all(feature = "vortex-write", feature = "universal-format-io")),
    allow(dead_code)
)]
fn source_unit_split_row_ranges(
    row_count: usize,
    exact_source_unit_row_ranges: Option<&[(usize, usize)]>,
) -> Vec<(usize, usize)> {
    if row_count == 0 {
        return Vec::new();
    }
    let Some(exact_source_unit_row_ranges) = exact_source_unit_row_ranges else {
        return single_source_split_row_ranges(row_count);
    };
    if exact_source_unit_row_ranges.is_empty() {
        return single_source_split_row_ranges(row_count);
    }
    let mut expected_start = 0usize;
    for &(start, end) in exact_source_unit_row_ranges {
        if start != expected_start || end <= start || end > row_count {
            return single_source_split_row_ranges(row_count);
        }
        expected_start = end;
    }
    if expected_start == row_count {
        exact_source_unit_row_ranges.to_vec()
    } else {
        single_source_split_row_ranges(row_count)
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
struct SchemaDeclaredTextRecordBatchReader {
    schema: SchemaRef,
    header: Vec<String>,
    column_dtypes: Vec<Option<LogicalDType>>,
    source_format: LocalSourceFormat,
    reader: BufReader<fs::File>,
    read_plan: LocalSourceReadPlan,
    column_indices: BTreeMap<String, usize>,
    batch_size: usize,
    max_input_rows: Option<usize>,
    next_row_number: usize,
    context: String,
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[derive(Clone, Copy)]
struct TextRecordBatchReaderConfig {
    max_input_rows: Option<usize>,
    batch_size: usize,
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
impl SchemaDeclaredTextRecordBatchReader {
    fn new(
        source_format: LocalSourceFormat,
        schema: SchemaRef,
        header: Vec<String>,
        column_dtypes: Vec<Option<LogicalDType>>,
        reader: BufReader<fs::File>,
        config: TextRecordBatchReaderConfig,
        context: String,
    ) -> Self {
        let required_columns = header.iter().cloned().collect::<BTreeSet<_>>();
        let column_indices = header
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, name)| (name, index))
            .collect();
        Self {
            schema,
            header,
            column_dtypes,
            source_format,
            reader,
            read_plan: LocalSourceReadPlan::required(
                required_columns,
                "schema_declared_text_stream_columns",
            ),
            column_indices,
            batch_size: config.batch_size,
            max_input_rows: config.max_input_rows,
            next_row_number: 0,
            context,
        }
    }

    fn next_record_batch(&mut self) -> Result<Option<RecordBatch>, ShardLoomError> {
        if self.source_format == LocalSourceFormat::JsonLines {
            return self.next_jsonl_record_batch();
        }
        let mut rows = Vec::<Vec<(String, ScalarValue)>>::with_capacity(self.batch_size.min(1024));
        let mut line = String::new();
        while rows.len() < self.batch_size {
            line.clear();
            let bytes_read = read_csv_record(&mut self.reader, &mut line).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "{} failed to read {} source row {}: {error}; no fallback execution was attempted",
                    self.context,
                    self.source_format.row_label(),
                    self.next_row_number + 1
                ))
            })?;
            if bytes_read == 0 {
                break;
            }
            if line.trim().is_empty() {
                continue;
            }
            self.next_row_number += 1;
            enforce_local_source_row_budget(
                self.next_row_number,
                self.max_input_rows,
                self.source_format.row_label(),
            )?;
            let row = match self.source_format {
                LocalSourceFormat::Csv => self.csv_row_from_line(&line)?,
                LocalSourceFormat::JsonLines
                | LocalSourceFormat::Json
                | LocalSourceFormat::Parquet
                | LocalSourceFormat::ArrowIpc
                | LocalSourceFormat::Avro
                | LocalSourceFormat::Orc => {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "{} only admits schema-declared CSV/JSONL text streams; no fallback execution was attempted",
                        self.context
                    )));
                }
            };
            rows.push(row);
        }
        if rows.is_empty() {
            return Ok(None);
        }
        shardloom_vortex::universal_format_io::flat_rows_to_record_batch_with_schema(
            Arc::clone(&self.schema),
            &self.header,
            &rows,
            &self.context,
        )
        .map(Some)
    }

    fn csv_row_from_line(&self, line: &str) -> Result<Vec<(String, ScalarValue)>, ShardLoomError> {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        let fields = split_csv_record(trimmed).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "{} CSV row {} is not admitted: {error}; no fallback execution was attempted",
                self.context, self.next_row_number
            ))
        })?;
        if fields.len() != self.header.len() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{} CSV row {} has {} fields, expected {}; no fallback execution was attempted",
                self.context,
                self.next_row_number,
                fields.len(),
                self.header.len()
            )));
        }
        self.header
            .iter()
            .zip(fields.iter())
            .zip(self.column_dtypes.iter())
            .map(|((column, raw), dtype)| {
                let dtype = dtype.as_ref().ok_or_else(|| {
                    ShardLoomError::InvalidOperation(format!(
                        "{} missing declared dtype for CSV column '{column}'; no fallback execution was attempted",
                        self.context
                    ))
                })?;
                Ok((
                    column.clone(),
                    parse_schema_declared_text_scalar(raw, dtype, column, &self.context)?,
                ))
            })
            .collect()
    }

    fn next_jsonl_record_batch(&mut self) -> Result<Option<RecordBatch>, ShardLoomError> {
        let mut builder = None;
        let mut values = vec![ScalarValue::Null; self.header.len()];
        let mut line = String::new();
        let mut row_count = 0;
        while row_count < self.batch_size {
            line.clear();
            let bytes_read = self.reader.read_line(&mut line).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "{} failed to read {} source row {}: {error}; no fallback execution was attempted",
                    self.context, self.source_format.row_label(), self.next_row_number + 1
                ))
            })?;
            if bytes_read == 0 {
                break;
            }
            if line.trim().is_empty() {
                continue;
            }
            self.next_row_number += 1;
            enforce_local_source_row_budget(
                self.next_row_number,
                self.max_input_rows,
                self.source_format.row_label(),
            )?;
            self.jsonl_values_from_line(&line, &mut values)?;
            // Allocate only for a nonempty batch. Keep no named scalar rows
            // alive while the native writer consumes the resulting columns.
            if builder.is_none() {
                builder = Some(
                    shardloom_vortex::universal_format_io::TextRecordBatchBuilder::new(
                        Arc::clone(&self.schema),
                        &self.header,
                        self.batch_size.min(1024),
                        &self.context,
                    )?,
                );
            }
            if let Some(builder) = builder.as_mut() {
                builder.append_row(&values)?;
            }
            row_count += 1;
        }
        builder
            .map(shardloom_vortex::universal_format_io::TextRecordBatchBuilder::finish)
            .transpose()
    }

    fn jsonl_values_from_line(
        &self,
        line: &str,
        values: &mut [ScalarValue],
    ) -> Result<(), ShardLoomError> {
        let line = if self.next_row_number == 1 {
            line.strip_prefix('\u{feff}').unwrap_or(line)
        } else {
            line
        };
        values.fill(ScalarValue::Null);
        visit_flat_json_object_with_plan(line.trim(), &self.read_plan, |column, value| {
            if let Some(&index) = self.column_indices.get(&column) {
                values[index] = value;
            }
        })
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "{} JSONL row {} is not admitted: {error}; no fallback execution was attempted",
                self.context, self.next_row_number
            ))
        })?;
        for ((column, dtype), value) in self.header.iter().zip(&self.column_dtypes).zip(values) {
            let dtype = dtype.as_ref().ok_or_else(|| {
                    ShardLoomError::InvalidOperation(format!(
                        "{} missing declared dtype for JSONL column '{column}'; no fallback execution was attempted",
                        self.context
                    ))
                })?;
            *value = coerce_schema_declared_json_scalar(
                std::mem::replace(value, ScalarValue::Null),
                dtype,
                column,
                &self.context,
            )?;
        }
        Ok(())
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
struct SchemaDeclaredTextStreamContract {
    source_format: LocalSourceFormat,
    header: Vec<String>,
    column_dtypes: Vec<Option<LogicalDType>>,
    column_arrow_dtypes: Vec<Option<DataType>>,
    reader: BufReader<fs::File>,
    source_stream_policy: String,
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn text_stream_record_batch_size(max_input_rows: Option<usize>) -> usize {
    max_input_rows.map_or(
        shardloom_vortex::universal_format_io::PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS,
        |_| shardloom_vortex::universal_format_io::PRODUCT_COLUMNAR_STREAM_RECORD_BATCH_ROWS,
    )
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn text_stream_policy_token(
    source_format: LocalSourceFormat,
    schema_kind: &str,
    batch_size: usize,
) -> String {
    let format = match source_format {
        LocalSourceFormat::Csv => "csv",
        LocalSourceFormat::JsonLines => "jsonl",
        LocalSourceFormat::Json
        | LocalSourceFormat::Parquet
        | LocalSourceFormat::ArrowIpc
        | LocalSourceFormat::Avro
        | LocalSourceFormat::Orc => "not_applicable",
    };
    format!("{schema_kind}_{format}_record_batch_stream_batch_size_{batch_size}_rows")
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn schema_declared_text_stream_contract(
    source_path: &Path,
    source_format: LocalSourceFormat,
    source_schema_hints: &[(String, LogicalDType)],
    max_input_rows: Option<usize>,
) -> Result<Option<SchemaDeclaredTextStreamContract>, ShardLoomError> {
    if source_schema_hints.is_empty()
        || !matches!(
            source_format,
            LocalSourceFormat::Csv | LocalSourceFormat::JsonLines
        )
        || source_schema_hints
            .iter()
            .any(|(_, dtype)| !schema_declared_text_dtype_supported(dtype))
    {
        return Ok(None);
    }
    let declared_header = source_schema_hints
        .iter()
        .map(|(name, _dtype)| name.clone())
        .collect::<Vec<_>>();
    let column_dtypes = source_schema_hints
        .iter()
        .map(|(_name, dtype)| Some(dtype.clone()))
        .collect::<Vec<_>>();
    let context = format!(
        "{} schema-declared Universal Ingest stream",
        source_format.row_label()
    );
    let column_arrow_dtypes = source_schema_hints
        .iter()
        .map(|(name, dtype)| schema_declared_text_arrow_dtype(dtype, name, &context).map(Some))
        .collect::<Result<Vec<_>, ShardLoomError>>()?;
    match source_format {
        LocalSourceFormat::Csv => {
            let file = fs::File::open(source_path).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "{context} failed to open {} for streaming read: {error}; no fallback execution was attempted",
                    source_path.display()
                ))
            })?;
            let mut reader = BufReader::new(file);
            let mut header_line = String::new();
            let bytes_read = read_csv_record(&mut reader, &mut header_line).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "{context} failed to read CSV header from {}: {error}; no fallback execution was attempted",
                    source_path.display()
                ))
            })?;
            if bytes_read == 0 {
                return Err(unsupported_sql_error(
                    "CSV source must include a header row",
                ));
            }
            let mut header = split_csv_record(header_line.trim_end_matches(['\r', '\n']))?;
            strip_utf8_bom_from_first_header_cell(&mut header);
            for column in &header {
                validate_sql_identifier(column)?;
            }
            if header != declared_header {
                return schema_hinted_inferred_text_stream_contract(
                    source_path,
                    source_format,
                    max_input_rows,
                    source_schema_hints,
                );
            }
            Ok(Some(SchemaDeclaredTextStreamContract {
                source_format,
                header,
                column_dtypes,
                column_arrow_dtypes,
                reader,
                source_stream_policy: text_stream_policy_token(
                    source_format,
                    "schema_declared",
                    text_stream_record_batch_size(max_input_rows),
                ),
            }))
        }
        LocalSourceFormat::JsonLines => schema_hinted_inferred_text_stream_contract(
            source_path,
            source_format,
            max_input_rows,
            source_schema_hints,
        ),
        LocalSourceFormat::Json
        | LocalSourceFormat::Parquet
        | LocalSourceFormat::ArrowIpc
        | LocalSourceFormat::Avro
        | LocalSourceFormat::Orc => Ok(None),
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn schema_hinted_inferred_text_stream_contract(
    source_path: &Path,
    source_format: LocalSourceFormat,
    max_input_rows: Option<usize>,
    source_schema_hints: &[(String, LogicalDType)],
) -> Result<Option<SchemaDeclaredTextStreamContract>, ShardLoomError> {
    let Some(mut contract) =
        inferred_text_stream_contract(source_path, source_format, max_input_rows)?
    else {
        return Ok(None);
    };
    let context = format!(
        "{} schema-hinted Universal Ingest stream",
        source_format.row_label()
    );
    apply_schema_hints_to_text_stream_contract(&mut contract, source_schema_hints, &context)?;
    let complete_declared_schema = contract.header.iter().all(|column| {
        source_schema_hints
            .iter()
            .any(|(hint, _dtype)| hint == column)
    });
    let schema_kind = if complete_declared_schema {
        "schema_declared"
    } else {
        "schema_hinted"
    };
    contract.source_stream_policy = text_stream_policy_token(
        source_format,
        schema_kind,
        text_stream_record_batch_size(max_input_rows),
    );
    Ok(Some(contract))
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn apply_schema_hints_to_text_stream_contract(
    contract: &mut SchemaDeclaredTextStreamContract,
    source_schema_hints: &[(String, LogicalDType)],
    context: &str,
) -> Result<(), ShardLoomError> {
    for (name, dtype) in source_schema_hints {
        let Some(index) = contract.header.iter().position(|column| column == name) else {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{context} schema column {name:?} is not present in source header {}; no fallback execution was attempted",
                contract.header.join(",")
            )));
        };
        contract.column_dtypes[index] = Some(dtype.clone());
        contract.column_arrow_dtypes[index] =
            Some(schema_declared_text_arrow_dtype(dtype, name, context)?);
    }
    Ok(())
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn text_stream_contract_schema_hints(
    contract: &SchemaDeclaredTextStreamContract,
) -> Vec<(String, LogicalDType)> {
    contract
        .header
        .iter()
        .zip(contract.column_dtypes.iter())
        .filter_map(|(name, dtype)| dtype.as_ref().map(|dtype| (name.clone(), dtype.clone())))
        .collect()
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InferredTextColumnKind {
    Null,
    Boolean,
    Int64,
    UInt64,
    Float64,
    Utf8,
    Binary,
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
impl InferredTextColumnKind {
    fn observe(self, value: &ScalarValue) -> Self {
        let candidate = match value {
            ScalarValue::Null => return self,
            ScalarValue::Boolean(_) => Self::Boolean,
            ScalarValue::Int64(_) => Self::Int64,
            ScalarValue::UInt64(_) => Self::UInt64,
            ScalarValue::Float64(_) => Self::Float64,
            ScalarValue::Utf8(_)
            | ScalarValue::Decimal128 { .. }
            | ScalarValue::Date32(_)
            | ScalarValue::TimestampMicros(_)
            | ScalarValue::List(_)
            | ScalarValue::Struct(_) => Self::Utf8,
            ScalarValue::Binary(_) => Self::Binary,
        };
        match (self, candidate) {
            (Self::Null, candidate) => candidate,
            (existing, candidate) if existing == candidate => existing,
            (
                Self::Int64 | Self::UInt64 | Self::Float64,
                Self::Int64 | Self::UInt64 | Self::Float64,
            ) => Self::Float64,
            _ => Self::Utf8,
        }
    }

    fn logical_dtype(self) -> LogicalDType {
        match self {
            Self::Null | Self::Utf8 => LogicalDType::Utf8,
            Self::Boolean => LogicalDType::Boolean,
            Self::Int64 => LogicalDType::Int64,
            Self::UInt64 => LogicalDType::UInt64,
            Self::Float64 => LogicalDType::Float64,
            Self::Binary => LogicalDType::Binary,
        }
    }

    fn arrow_dtype(self) -> DataType {
        match self {
            Self::Null | Self::Utf8 => DataType::Utf8,
            Self::Boolean => DataType::Boolean,
            Self::Int64 => DataType::Int64,
            Self::UInt64 => DataType::UInt64,
            Self::Float64 => DataType::Float64,
            Self::Binary => DataType::Binary,
        }
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn inferred_text_stream_contract(
    source_path: &Path,
    source_format: LocalSourceFormat,
    max_input_rows: Option<usize>,
) -> Result<Option<SchemaDeclaredTextStreamContract>, ShardLoomError> {
    if !matches!(
        source_format,
        LocalSourceFormat::Csv | LocalSourceFormat::JsonLines
    ) {
        return Ok(None);
    }
    let context = format!(
        "{} inferred-schema Universal Ingest stream",
        source_format.row_label()
    );
    let (header, inferred) = match source_format {
        LocalSourceFormat::Csv => infer_csv_text_stream_schema(source_path, max_input_rows)?,
        LocalSourceFormat::JsonLines => {
            infer_jsonl_text_stream_schema(source_path, max_input_rows)?
        }
        LocalSourceFormat::Json
        | LocalSourceFormat::Parquet
        | LocalSourceFormat::ArrowIpc
        | LocalSourceFormat::Avro
        | LocalSourceFormat::Orc => return Ok(None),
    };
    let column_dtypes = inferred
        .iter()
        .map(|kind| Some(kind.logical_dtype()))
        .collect::<Vec<_>>();
    let column_arrow_dtypes = inferred
        .iter()
        .map(|kind| Some(kind.arrow_dtype()))
        .collect::<Vec<_>>();
    let file = fs::File::open(source_path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "{context} failed to open {} for streaming read: {error}; no fallback execution was attempted",
            source_path.display()
        ))
    })?;
    let mut reader = BufReader::new(file);
    if source_format == LocalSourceFormat::Csv {
        let mut header_line = String::new();
        read_csv_record(&mut reader, &mut header_line).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "{context} failed to read CSV header from {}: {error}; no fallback execution was attempted",
                source_path.display()
            ))
        })?;
    }
    Ok(Some(SchemaDeclaredTextStreamContract {
        source_format,
        header,
        column_dtypes,
        column_arrow_dtypes,
        reader,
        source_stream_policy: text_stream_policy_token(
            source_format,
            "inferred",
            text_stream_record_batch_size(max_input_rows),
        ),
    }))
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn infer_csv_text_stream_schema(
    source_path: &Path,
    max_input_rows: Option<usize>,
) -> Result<(Vec<String>, Vec<InferredTextColumnKind>), ShardLoomError> {
    let context = "CSV inferred-schema Universal Ingest stream";
    let file = fs::File::open(source_path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "{context} failed to open {} for schema inference: {error}; no fallback execution was attempted",
            source_path.display()
        ))
    })?;
    let mut reader = BufReader::new(file);
    let mut header_line = String::new();
    let bytes_read = read_csv_record(&mut reader, &mut header_line).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "{context} failed to read CSV header from {}: {error}; no fallback execution was attempted",
            source_path.display()
        ))
    })?;
    if bytes_read == 0 {
        return Err(unsupported_sql_error(
            "CSV source must include a header row",
        ));
    }
    let mut header = split_csv_record(header_line.trim_end_matches(['\r', '\n']))?;
    strip_utf8_bom_from_first_header_cell(&mut header);
    if header.is_empty() {
        return Err(unsupported_sql_error("CSV source header must not be empty"));
    }
    for column in &header {
        validate_sql_identifier(column)?;
    }
    let mut inferred = vec![InferredTextColumnKind::Null; header.len()];
    let mut row_count = 0_usize;
    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = read_csv_record(&mut reader, &mut line).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "{context} failed to read CSV row {}: {error}; no fallback execution was attempted",
                row_count + 1
            ))
        })?;
        if bytes_read == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        row_count += 1;
        enforce_local_source_row_budget(row_count, max_input_rows, "CSV")?;
        let record = split_csv_record(line.trim_end_matches(['\r', '\n']))?;
        if record.len() != header.len() {
            return Err(unsupported_sql_error(
                "CSV row width must match the header width for the inferred-schema Universal Ingest stream",
            ));
        }
        for (index, raw) in record.iter().enumerate() {
            inferred[index] = inferred[index].observe(&parse_csv_scalar(raw));
        }
    }
    Ok((header, inferred))
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn infer_jsonl_text_stream_schema(
    source_path: &Path,
    max_input_rows: Option<usize>,
) -> Result<(Vec<String>, Vec<InferredTextColumnKind>), ShardLoomError> {
    let context = "JSONL inferred-schema Universal Ingest stream";
    let file = fs::File::open(source_path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "{context} failed to open {} for schema inference: {error}; no fallback execution was attempted",
            source_path.display()
        ))
    })?;
    let reader = BufReader::new(file);
    let read_plan = LocalSourceReadPlan::full("inferred_jsonl_text_stream_schema");
    let mut header = Vec::<String>::new();
    let mut inferred = Vec::<InferredTextColumnKind>::new();
    let mut row_count = 0_usize;
    for line in reader.lines() {
        let line = line.map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "{context} failed to read JSONL row {}: {error}; no fallback execution was attempted",
                row_count + 1
            ))
        })?;
        if line.trim().is_empty() {
            continue;
        }
        row_count += 1;
        enforce_local_source_row_budget(row_count, max_input_rows, "JSONL")?;
        let line = if row_count == 1 {
            line.strip_prefix('\u{feff}').unwrap_or(&line)
        } else {
            &line
        };
        let fields = parse_flat_json_object_with_plan(line, &read_plan).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "{context} row {row_count} is not admitted: {error}; no fallback execution was attempted"
            ))
        })?;
        for (name, value) in fields {
            let index = if let Some(index) = header.iter().position(|column| column == &name) {
                index
            } else {
                validate_sql_identifier(&name)?;
                header.push(name);
                inferred.push(InferredTextColumnKind::Null);
                inferred.len() - 1
            };
            inferred[index] = inferred[index].observe(&value);
        }
    }
    if row_count == 0 {
        return Err(unsupported_sql_error(
            "JSONL source must include at least one object row",
        ));
    }
    Ok((header, inferred))
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn schema_declared_text_dtype_supported(dtype: &LogicalDType) -> bool {
    matches!(
        dtype,
        LogicalDType::Boolean
            | LogicalDType::Int64
            | LogicalDType::UInt64
            | LogicalDType::Float64
            | LogicalDType::Utf8
            | LogicalDType::Binary
            | LogicalDType::Date32
            | LogicalDType::TimestampMicros
    )
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
impl Iterator for SchemaDeclaredTextRecordBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_record_batch()
            .transpose()
            .map(|result| result.map_err(|error| ArrowError::ComputeError(error.to_string())))
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
impl RecordBatchReader for SchemaDeclaredTextRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn parse_schema_declared_text_scalar(
    raw: &str,
    dtype: &LogicalDType,
    column: &str,
    context: &str,
) -> Result<ScalarValue, ShardLoomError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        return Ok(ScalarValue::Null);
    }
    match dtype {
        LogicalDType::Boolean => trimmed.parse::<bool>().map(ScalarValue::Boolean).map_err(|_| {
            ShardLoomError::InvalidOperation(format!(
                "{context} column '{column}' value {raw:?} is not a boolean; no fallback execution was attempted"
            ))
        }),
        LogicalDType::Int64 => trimmed.parse::<i64>().map(ScalarValue::Int64).map_err(|_| {
            ShardLoomError::InvalidOperation(format!(
                "{context} column '{column}' value {raw:?} is not int64; no fallback execution was attempted"
            ))
        }),
        LogicalDType::UInt64 => trimmed.parse::<u64>().map(ScalarValue::UInt64).map_err(|_| {
            ShardLoomError::InvalidOperation(format!(
                "{context} column '{column}' value {raw:?} is not uint64; no fallback execution was attempted"
            ))
        }),
        LogicalDType::Float64 => trimmed.parse::<f64>().map_err(|_| {
            ShardLoomError::InvalidOperation(format!(
                "{context} column '{column}' value {raw:?} is not float64; no fallback execution was attempted"
            ))
        }).and_then(|value| {
            if value.is_finite() {
                Ok(ScalarValue::Float64(value))
            } else {
                Err(ShardLoomError::InvalidOperation(format!(
                    "{context} column '{column}' value {raw:?} is not a finite float64; no fallback execution was attempted"
                )))
            }
        }),
        LogicalDType::Utf8 => Ok(ScalarValue::Utf8(raw.to_string())),
        LogicalDType::Binary => Ok(ScalarValue::Binary(raw.as_bytes().to_vec())),
        LogicalDType::Date32 => parse_iso_date32(trimmed).map(ScalarValue::Date32),
        LogicalDType::TimestampMicros => {
            parse_iso_timestamp_micros(trimmed).map(ScalarValue::TimestampMicros)
        }
        LogicalDType::Unknown | LogicalDType::List | LogicalDType::Struct | LogicalDType::Extension(_) => {
            Err(ShardLoomError::InvalidOperation(format!(
                "{context} column '{column}' declares unsupported schema dtype {}; no fallback execution was attempted",
                dtype.as_str()
            )))
        }
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[allow(clippy::cast_precision_loss)]
fn coerce_schema_declared_json_scalar(
    value: ScalarValue,
    dtype: &LogicalDType,
    column: &str,
    context: &str,
) -> Result<ScalarValue, ShardLoomError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    match (dtype, value) {
        (LogicalDType::Boolean, ScalarValue::Boolean(value)) => Ok(ScalarValue::Boolean(value)),
        (LogicalDType::Int64, ScalarValue::Int64(value)) => Ok(ScalarValue::Int64(value)),
        (LogicalDType::UInt64, ScalarValue::UInt64(value)) => Ok(ScalarValue::UInt64(value)),
        (LogicalDType::UInt64, ScalarValue::Int64(value)) if value >= 0 => {
            Ok(ScalarValue::UInt64(value.cast_unsigned()))
        }
        (LogicalDType::Float64, ScalarValue::Float64(value)) => Ok(ScalarValue::Float64(value)),
        (LogicalDType::Float64, ScalarValue::Int64(value)) => {
            Ok(ScalarValue::Float64(value as f64))
        }
        (LogicalDType::Utf8, ScalarValue::Utf8(value)) => Ok(ScalarValue::Utf8(value)),
        (LogicalDType::Utf8, ScalarValue::Boolean(value)) => {
            Ok(ScalarValue::Utf8(value.to_string()))
        }
        (LogicalDType::Utf8, ScalarValue::Int64(value)) => Ok(ScalarValue::Utf8(value.to_string())),
        (LogicalDType::Utf8, ScalarValue::UInt64(value)) => {
            Ok(ScalarValue::Utf8(value.to_string()))
        }
        (LogicalDType::Utf8, ScalarValue::Float64(value)) if value.is_finite() => {
            Ok(ScalarValue::Utf8(value.to_string()))
        }
        (LogicalDType::Binary, ScalarValue::Utf8(value)) => {
            Ok(ScalarValue::Binary(value.into_bytes()))
        }
        (LogicalDType::Date32, ScalarValue::Utf8(value)) => {
            parse_iso_date32(&value).map(ScalarValue::Date32)
        }
        (LogicalDType::TimestampMicros, ScalarValue::Utf8(value)) => {
            parse_iso_timestamp_micros(&value).map(ScalarValue::TimestampMicros)
        }
        (dtype, value) => Err(ShardLoomError::InvalidOperation(format!(
            "{context} column '{column}' declared {} but JSONL value was {}; no fallback execution was attempted",
            dtype.as_str(),
            value.summary()
        ))),
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn schema_declared_text_arrow_dtype(
    dtype: &LogicalDType,
    column: &str,
    context: &str,
) -> Result<DataType, ShardLoomError> {
    match dtype {
        LogicalDType::Boolean => Ok(DataType::Boolean),
        LogicalDType::Int64 => Ok(DataType::Int64),
        LogicalDType::UInt64 => Ok(DataType::UInt64),
        LogicalDType::Float64 => Ok(DataType::Float64),
        LogicalDType::Utf8 => Ok(DataType::Utf8),
        LogicalDType::Binary => Ok(DataType::Binary),
        LogicalDType::Date32 => Ok(DataType::Date32),
        LogicalDType::TimestampMicros => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        LogicalDType::Unknown
        | LogicalDType::List
        | LogicalDType::Struct
        | LogicalDType::Extension(_) => Err(ShardLoomError::InvalidOperation(format!(
            "{context} column '{column}' declares unsupported schema dtype {}; no fallback execution was attempted",
            dtype.as_str()
        ))),
    }
}

#[derive(Debug, Clone)]
struct VortexIngestRequest {
    source_path: PathBuf,
    source_format_override: Option<LocalSourceFormat>,
    target_path: PathBuf,
    allow_overwrite: bool,
    certification_level: shardloom_vortex::VortexIngestCertificationLevel,
    runtime_profile: SqlLocalSourceRuntimeProfile,
    resources: ExecutionResources,
    shared_memory_pool: Option<shardloom_exec::live_memory::LiveMemoryPool>,
    source_fingerprint_policy: SourceFingerprintPolicy,
    delta: Option<VortexIngestDeltaRequest>,
    // Retained by the request in every build; only the native writer consumes it.
    #[cfg_attr(
        not(all(feature = "vortex-write", feature = "universal-format-io")),
        allow(dead_code)
    )]
    prepared_source_binding: Option<String>,
}

impl VortexIngestRequest {
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    fn native_memory_pool(
        &self,
    ) -> Result<shardloom_exec::live_memory::LiveMemoryPool, ShardLoomError> {
        match &self.shared_memory_pool {
            Some(pool) => Ok(pool.clone()),
            None => shardloom_exec::live_memory::LiveMemoryPool::new(self.resources.memory_bytes()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VortexIngestDeltaRequest {
    source_path: PathBuf,
    target_path: PathBuf,
    update_mode: shardloom_vortex::VortexDifferentialUpdateMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceFingerprintPolicy {
    MetadataOnly,
    ContentDigest,
}

impl SourceFingerprintPolicy {
    const DEFAULT_PUBLIC_PREPARE: Self = Self::MetadataOnly;

    fn parse(value: &str) -> Result<Self, ShardLoomError> {
        match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "metadata" | "metadata_only" | "metadata_first" => Ok(Self::MetadataOnly),
            "content" | "content_digest" | "full_content_digest" => Ok(Self::ContentDigest),
            _ => Err(ShardLoomError::InvalidOperation(format!(
                "source fingerprint policy must be metadata_only or content_digest, got {value:?}; no fallback execution was attempted"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::ContentDigest => "content_digest",
        }
    }

    const fn fingerprint_kind(self) -> &'static str {
        match self {
            Self::MetadataOnly => "local_file_metadata_size_mtime",
            Self::ContentDigest => "local_file_content_digest",
        }
    }

    const fn identity_source(self) -> &'static str {
        match self {
            Self::MetadataOnly => "local_file_metadata_fast_prepare_identity",
            Self::ContentDigest => "local_file_explicit_proof_digest",
        }
    }

    const fn content_fingerprint_requested(self) -> bool {
        matches!(self, Self::ContentDigest)
    }
}

#[derive(Debug, Clone)]
struct VortexIngestReport {
    request: VortexIngestRequest,
    source: VortexIngestSourceData,
    source_schema_digest: String,
    source_state_id: String,
    source_state_digest: String,
    prepared_state_id: String,
    prepared_state_digest: String,
    prepare_once_total_millis: u128,
    prepared_olap_publication_millis: u128,
    evidence_render_millis: u128,
    vortex_report: shardloom_vortex::VortexPreparedStateWriteReport,
    scout_ingress: shardloom_vortex::VortexScoutIngressReport,
    layout_write_advisor: shardloom_vortex::VortexLayoutWriteAdvisorReport,
    capillary_preparation: shardloom_vortex::VortexCapillaryPreparationReport,
    copy_budget: shardloom_vortex::VortexCopyBudgetReport,
    differential_preparation: Option<shardloom_vortex::VortexDifferentialPreparationReport>,
    prepared_state_reuse: Option<shardloom_vortex::VortexPreparedStateReuseReport>,
    prepared_olap_state: Option<shardloom_vortex::VortexPreparedOlapStateReport>,
}

#[derive(Debug, Clone)]
enum VortexIngestOutcome {
    Prepared(Box<VortexIngestReport>),
}

#[derive(Debug)]
pub(crate) struct PublicWorkflowVortexPreparation {
    pub(crate) target_path: PathBuf,
    pub(crate) fields: Vec<(String, String)>,
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
    pub(crate) identity:
        Option<std::sync::Arc<shardloom_vortex::prepared_source_binding::LocalPreparationIdentity>>,
}

impl PublicWorkflowVortexPreparation {
    // Keep the same call boundary in builds that cannot prepare/reuse artifacts.
    #[cfg_attr(
        not(all(feature = "vortex-write", feature = "universal-format-io", unix)),
        allow(clippy::unused_self, clippy::unnecessary_wraps)
    )]
    pub(crate) fn validate_generation(&self) -> Result<(), ShardLoomError> {
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
        if let Some(identity) = &self.identity {
            identity.validate_generation()?;
        }
        Ok(())
    }
}

pub(crate) fn parse_vortex_ingest_schema_hints(
    raw: &str,
) -> Result<Vec<(String, LogicalDType)>, ShardLoomError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let mut seen = BTreeSet::new();
    let mut hints = Vec::new();
    for item in trimmed.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let (name_raw, dtype_raw) = item.split_once(':').ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "vortex_ingest schema item {item:?} must use name:dtype; no fallback execution was attempted"
            ))
        })?;
        let name = name_raw.trim().to_string();
        validate_sql_identifier(&name)?;
        if !seen.insert(name.clone()) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "vortex_ingest schema declares duplicate column {name:?}; no fallback execution was attempted"
            )));
        }
        hints.push((name, parse_cast_target_dtype(dtype_raw)?));
    }
    Ok(hints)
}

fn apply_vortex_ingest_schema_hints(
    source: &mut CsvSourceData,
    hints: &[(String, LogicalDType)],
) -> Result<(), ShardLoomError> {
    if hints.is_empty() {
        return Ok(());
    }
    if source.column_dtypes.len() != source.header.len() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "vortex_ingest source schema has {} dtype slots for {} columns; no fallback execution was attempted",
            source.column_dtypes.len(),
            source.header.len()
        )));
    }
    for (name, dtype) in hints {
        let Some(index) = source.header.iter().position(|column| column == name) else {
            return Err(ShardLoomError::InvalidOperation(format!(
                "vortex_ingest schema column {name:?} is not present in source header {}; no fallback execution was attempted",
                source.header.join(",")
            )));
        };
        source.column_dtypes[index] = Some(dtype.clone());
    }
    Ok(())
}

fn vortex_ingest_source_schema_digest(source: &CsvSourceData) -> String {
    if source.column_dtypes.iter().all(Option::is_none) {
        return fnv64_digest(&source.header.join(","));
    }
    fnv64_digest(
        &source
            .header
            .iter()
            .enumerate()
            .map(|(index, name)| {
                let dtype = source
                    .column_dtypes
                    .get(index)
                    .and_then(Option::as_ref)
                    .map_or("inferred", LogicalDType::as_str);
                format!("{name}:{dtype}")
            })
            .collect::<Vec<_>>()
            .join("|"),
    )
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_prepare(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    handle_vortex_prepare_with_facade(args, format, VORTEX_PREPARE_COMMAND, &[])
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_prepare_with_facade(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
    emit_command: &'static str,
    extra_fields: &[(String, String)],
) -> ExitCode {
    let Some(source_path_raw) = args.next() else {
        return emit_error(
            emit_command,
            format,
            "vortex prepare failed",
            &vortex_prepare_usage_error("local source path"),
        );
    };
    let Some(target_path_raw) = args.next() else {
        return emit_error(
            emit_command,
            format,
            "vortex prepare failed",
            &vortex_prepare_usage_error("target .vortex path"),
        );
    };
    let mut allow_overwrite = false;
    let mut source_format_override = None;
    let mut source_schema_hints = Vec::new();
    let mut source_format_is_native_vortex = false;
    let mut certification_level = shardloom_vortex::VortexIngestCertificationLevel::IngestCertified;
    let mut delta_source_path = None;
    let mut delta_target_path = None;
    let mut delta_update_mode = shardloom_vortex::VortexDifferentialUpdateMode::AppendOnly;
    let mut runtime_profile = SqlLocalSourceRuntimeProfile::ProductLocalWorkflow;
    let mut resource_arguments = ResourceArguments::default();
    let mut source_fingerprint_policy = SourceFingerprintPolicy::DEFAULT_PUBLIC_PREPARE;
    while let Some(arg) = args.next() {
        match resource_arguments.parse_flag(&arg, &mut args) {
            Ok(true) => continue,
            Ok(false) => {}
            Err(error) => return emit_error(emit_command, format, "vortex prepare failed", &error),
        }
        match arg.as_str() {
            "--allow-overwrite" => allow_overwrite = true,
            "--internal-smoke-local-source" => {
                runtime_profile = SqlLocalSourceRuntimeProfile::Smoke;
            }
            "--input-format" | "--source-format" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        "vortex prepare failed",
                        &ShardLoomError::InvalidOperation(format!(
                            "{arg} requires csv, json, jsonl, parquet, arrow-ipc, avro, or orc, or vortex"
                        )),
                    );
                };
                if source_format_token_is_vortex(&value) {
                    source_format_override = None;
                    source_format_is_native_vortex = true;
                    continue;
                }
                source_format_override = match LocalSourceFormat::parse(&value) {
                    Some(format) => Some(format),
                    None => {
                        return emit_error(
                            emit_command,
                            format,
                            "vortex prepare failed",
                            &ShardLoomError::InvalidOperation(format!(
                                "unsupported vortex_ingest input format: {value}; no fallback execution was attempted"
                            )),
                        );
                    }
                };
            }
            "--certification-level" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        "vortex prepare failed",
                        &ShardLoomError::InvalidOperation(
                            "--certification-level requires ingest_minimal, ingest_certified, or ingest_full_replay".to_string(),
                        ),
                    );
                };
                certification_level =
                    match shardloom_vortex::VortexIngestCertificationLevel::parse(&value) {
                        Ok(level) => level,
                        Err(error) => {
                            return emit_error(
                                emit_command,
                                format,
                                "vortex prepare failed",
                                &error,
                            );
                        }
                    };
            }
            "--source-fingerprint-policy" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        "vortex prepare failed",
                        &ShardLoomError::InvalidOperation(
                            "--source-fingerprint-policy requires metadata_only or content_digest"
                                .to_string(),
                        ),
                    );
                };
                source_fingerprint_policy = match SourceFingerprintPolicy::parse(&value) {
                    Ok(policy) => policy,
                    Err(error) => {
                        return emit_error(emit_command, format, "vortex prepare failed", &error);
                    }
                };
            }
            "--source-content-fingerprint" => {
                source_fingerprint_policy = SourceFingerprintPolicy::ContentDigest;
            }
            "--schema" | "--source-schema" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        "vortex prepare failed",
                        &ShardLoomError::InvalidOperation(
                            "--schema requires comma-separated name:dtype pairs".to_string(),
                        ),
                    );
                };
                source_schema_hints = match parse_vortex_ingest_schema_hints(&value) {
                    Ok(schema) => schema,
                    Err(error) => {
                        return emit_error(emit_command, format, "vortex prepare failed", &error);
                    }
                };
            }
            "--delta-source" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        "vortex prepare failed",
                        &ShardLoomError::InvalidOperation(
                            "--delta-source requires a local source path".to_string(),
                        ),
                    );
                };
                delta_source_path = Some(Path::new(value.trim()).to_path_buf());
            }
            "--delta-target" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        "vortex prepare failed",
                        &ShardLoomError::InvalidOperation(
                            "--delta-target requires a local .vortex path".to_string(),
                        ),
                    );
                };
                match normalize_local_vortex_ingest_target_path(&value) {
                    Ok(path) => delta_target_path = Some(path),
                    Err(error) => {
                        return emit_error(emit_command, format, "vortex prepare failed", &error);
                    }
                }
            }
            "--delta-update-mode" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        emit_command,
                        format,
                        "vortex prepare failed",
                        &ShardLoomError::InvalidOperation(
                            "--delta-update-mode requires append-only, update, delete, or upsert"
                                .to_string(),
                        ),
                    );
                };
                delta_update_mode =
                    match shardloom_vortex::VortexDifferentialUpdateMode::parse(&value) {
                        Ok(mode) => mode,
                        Err(error) => {
                            return emit_error(
                                emit_command,
                                format,
                                "vortex prepare failed",
                                &error,
                            );
                        }
                    };
            }
            extra => {
                return emit_error(
                    emit_command,
                    format,
                    "vortex prepare failed",
                    &cli_unknown_arg_error(VORTEX_PREPARE_COMMAND, extra),
                );
            }
        }
    }

    let resources = match resource_arguments.resolve() {
        Ok(resources) => resources,
        Err(error) => {
            return emit_error(
                emit_command,
                format,
                "vortex prepare requires execution resources",
                &error,
            );
        }
    };
    let source_path = Path::new(source_path_raw.trim()).to_path_buf();
    let target_path = match normalize_local_vortex_ingest_target_path(&target_path_raw) {
        Ok(path) => path,
        Err(error) => {
            return emit_error(emit_command, format, "vortex prepare failed", &error);
        }
    };
    let delta = match (delta_source_path, delta_target_path) {
        (Some(source_path), Some(target_path)) => Some(VortexIngestDeltaRequest {
            source_path,
            target_path,
            update_mode: delta_update_mode,
        }),
        (None, None) => None,
        (Some(_), None) => {
            return emit_error(
                emit_command,
                format,
                "vortex prepare failed",
                &ShardLoomError::InvalidOperation(
                    "vortex_ingest differential preparation requires --delta-target when --delta-source is provided; no fallback execution was attempted"
                        .to_string(),
                ),
            );
        }
        (None, Some(_)) => {
            return emit_error(
                emit_command,
                format,
                "vortex prepare failed",
                &ShardLoomError::InvalidOperation(
                    "vortex_ingest differential preparation requires --delta-source when --delta-target is provided; no fallback execution was attempted"
                        .to_string(),
                ),
            );
        }
    };
    let source_is_native_vortex =
        source_format_is_native_vortex || path_has_vortex_extension(&source_path);
    let request = VortexIngestRequest {
        source_path,
        source_format_override,
        target_path,
        allow_overwrite,
        certification_level,
        runtime_profile,
        resources,
        shared_memory_pool: None,
        source_fingerprint_policy,
        delta,
        prepared_source_binding: None,
    };

    if !shardloom_vortex::vortex_ingest_write_feature_enabled() {
        emit(
            emit_command,
            format,
            CommandStatus::Unsupported,
            "vortex_ingest feature gate is not enabled".to_string(),
            "local vortex_ingest runtime requires shardloom-cli --features vortex-write; fallback execution remains disabled"
                .to_string(),
            Vec::new(),
            fields_with_extra(vortex_ingest_feature_blocked_fields(&request), extra_fields),
        );
        return ExitCode::from(1);
    }

    if source_is_native_vortex {
        if request.delta.is_some() {
            return emit_error(
                emit_command,
                format,
                "vortex prepare failed",
                &ShardLoomError::InvalidOperation(
                    "native Vortex artifact prepare does not accept differential compatibility-source options; use the existing .vortex artifact directly or provide a compatibility delta source; no fallback execution was attempted"
                        .to_string(),
                ),
            );
        }
        let mut native_request =
            match shardloom_vortex::VortexNativeArtifactPrepareRequest::new_local(
                &request.source_path,
                &request.target_path,
                request.allow_overwrite,
                request.resources,
                shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION,
                "vortex-write",
                request.certification_level.as_str(),
            ) {
                Ok(request) => request,
                Err(error) => {
                    return emit_error(emit_command, format, "vortex prepare failed", &error);
                }
            };
        native_request
            .shared_memory_pool
            .clone_from(&request.shared_memory_pool);
        let report = match shardloom_vortex::prepare_native_vortex_artifact(&native_request) {
            Ok(report) => report,
            Err(error) => {
                return emit_error(emit_command, format, "vortex prepare failed", &error);
            }
        };
        emit(
            emit_command,
            format,
            CommandStatus::Success,
            report.summary(),
            report.to_text(),
            Vec::new(),
            fields_with_extra(report.evidence_fields(), extra_fields),
        );
        return ExitCode::SUCCESS;
    }

    let outcome = match run_vortex_prepare_with_schema(request.clone(), &source_schema_hints) {
        Ok(outcome) => outcome,
        Err(error) => {
            if let Some(fields) = vortex_ingest_scout_blocked_fields(&request, &error) {
                let blocked_source_path = vortex_ingest_scout_blocked_source_path(&request, &error);
                emit(
                    emit_command,
                    format,
                    CommandStatus::Unsupported,
                    "vortex_ingest scout ingress blocked source before preparation".to_string(),
                    format!(
                        "local vortex_ingest scout ingress blocked source {} before Vortex write: {error}; fallback execution remains disabled",
                        blocked_source_path.display()
                    ),
                    Vec::new(),
                    fields_with_extra(fields, extra_fields),
                );
                return ExitCode::from(1);
            }
            return emit_error(emit_command, format, "vortex prepare failed", &error);
        }
    };

    match outcome {
        VortexIngestOutcome::Prepared(report) => {
            let differential_blocked = report.differential_preparation_blocked();
            emit(
                emit_command,
                format,
                if differential_blocked {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                report.summary(),
                report.to_text(),
                Vec::new(),
                fields_with_extra(report.fields(), extra_fields),
            );
            if differential_blocked {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
    }
}

fn fields_with_extra(
    mut fields: Vec<(String, String)>,
    extra_fields: &[(String, String)],
) -> Vec<(String, String)> {
    fields.extend(extra_fields.iter().cloned());
    fields
}

#[cfg(all(test, feature = "vortex-write", feature = "universal-format-io", unix))]
pub(crate) fn prepare_local_source_as_vortex_for_public_workflow(
    source_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
    source_format: Option<&str>,
    allow_overwrite: bool,
    max_parallelism: usize,
    memory_gb: Option<u64>,
    source_fingerprint_policy: Option<&str>,
) -> Result<PublicWorkflowVortexPreparation, ShardLoomError> {
    let resources = ExecutionResources::resolve(
        shardloom_core::ExecutionResourceRequest {
            memory_gb,
            max_parallelism: Some(max_parallelism),
            ..shardloom_core::ExecutionResourceRequest::new(
                shardloom_core::ExecutionResourceOrigin::ExecutionCall,
            )
        },
        None,
        None,
    )?;
    prepare_local_source_as_vortex_for_public_workflow_with_schema(
        source_path,
        target_path,
        source_format,
        allow_overwrite,
        resources,
        None,
        source_fingerprint_policy,
        None,
    )
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(crate) fn prepare_local_source_as_vortex_for_public_workflow_with_schema(
    source_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
    source_format: Option<&str>,
    allow_overwrite: bool,
    resources: ExecutionResources,
    shared_memory_pool: Option<&shardloom_exec::live_memory::LiveMemoryPool>,
    source_fingerprint_policy: Option<&str>,
    source_schema: Option<&str>,
) -> Result<PublicWorkflowVortexPreparation, ShardLoomError> {
    if shared_memory_pool.is_some_and(|pool| pool.snapshot().limit_bytes > resources.memory_bytes())
    {
        return Err(ShardLoomError::new(
            "shared preparation memory owner exceeds the declared allocation",
        ));
    }
    let source_schema_hints = source_schema
        .map(parse_vortex_ingest_schema_hints)
        .transpose()?
        .unwrap_or_default();
    if !shardloom_vortex::vortex_ingest_write_feature_enabled() {
        return Err(ShardLoomError::NotImplemented(
            "vortex_ingest feature gate is not enabled".to_string(),
        ));
    }
    if source_format.is_some_and(source_format_token_is_vortex)
        || path_has_vortex_extension(source_path.as_ref())
    {
        if source_schema.is_some() {
            return Err(ShardLoomError::InvalidOperation(
                "source schema hints apply only to compatibility inputs; native Vortex input does not accept --source-schema; no fallback execution was attempted".into(),
            ));
        }
        let target_path =
            normalize_local_vortex_ingest_target_path(&target_path.as_ref().display().to_string())?;
        let request = shardloom_vortex::VortexNativeArtifactPrepareRequest::new_local(
            source_path.as_ref(),
            &target_path,
            allow_overwrite,
            resources,
            shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION,
            "vortex-write",
            shardloom_vortex::VortexIngestCertificationLevel::IngestCertified.as_str(),
        )?;
        let mut request = request;
        request.shared_memory_pool = shared_memory_pool.cloned();
        let report = shardloom_vortex::prepare_native_vortex_artifact(&request)?;
        let raw_fields = report.evidence_fields();
        return Ok(PublicWorkflowVortexPreparation {
            target_path,
            fields: public_workflow_preparation_fields(&raw_fields),
            #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
            identity: None,
        });
    }
    let source_format_override = match source_format {
        Some(value) => Some(LocalSourceFormat::parse(value).ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "unsupported vortex_ingest input format: {value}; no fallback execution was attempted"
            ))
        })?),
        None => None,
    };
    let source_fingerprint_policy = source_fingerprint_policy
        .map(SourceFingerprintPolicy::parse)
        .transpose()?
        .unwrap_or(SourceFingerprintPolicy::DEFAULT_PUBLIC_PREPARE);
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
    let binding_started = Instant::now();
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
    let prepared_source_binding = Some(public_preparation_source_binding(
        source_path.as_ref(),
        source_format_override,
        source_fingerprint_policy,
        source_schema,
    )?);
    #[cfg(not(all(feature = "vortex-write", feature = "universal-format-io", unix)))]
    let prepared_source_binding: Option<String> = None;
    let target_path =
        normalize_local_vortex_ingest_target_path(&target_path.as_ref().display().to_string())?;
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
    if !allow_overwrite && target_path.exists() {
        let binding = prepared_source_binding
            .as_deref()
            .expect("feature-admitted binding");
        let identity = shardloom_vortex::prepared_source_binding::local_preparation_identity(
            &target_path,
            binding,
        )?;
        return Ok(public_workflow_reused_preparation(
            target_path,
            source_fingerprint_policy,
            identity,
            binding_started.elapsed().as_millis(),
        ));
    }
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create prepared Vortex directory '{}': {error}; no fallback execution was attempted",
                parent.display()
            ))
        })?;
    }
    let request = VortexIngestRequest {
        source_path: source_path.as_ref().to_path_buf(),
        source_format_override,
        target_path: target_path.clone(),
        allow_overwrite,
        certification_level: shardloom_vortex::VortexIngestCertificationLevel::IngestCertified,
        runtime_profile: SqlLocalSourceRuntimeProfile::ProductLocalWorkflow,
        resources,
        shared_memory_pool: shared_memory_pool.cloned(),
        source_fingerprint_policy,
        delta: None,
        prepared_source_binding: prepared_source_binding.clone(),
    };
    let raw_fields = match run_vortex_prepare_with_schema(request, &source_schema_hints)? {
        VortexIngestOutcome::Prepared(report) => {
            if report.differential_preparation_blocked() {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "prepared Vortex lifecycle for '{}' was blocked by differential preparation policy; no fallback execution was attempted",
                    report.request.source_path.display()
                )));
            }
            report.fields()
        }
    };
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
    let (raw_fields, identity) = {
        let mut fields = raw_fields;
        let identity = if let Some(binding) = prepared_source_binding {
            let identity = shardloom_vortex::prepared_source_binding::local_preparation_identity(
                &target_path,
                &binding,
            )?;
            public_preparation_identity_fields(&mut fields, &identity, false);
            Some(std::sync::Arc::new(identity))
        } else {
            None
        };
        (fields, identity)
    };
    Ok(PublicWorkflowVortexPreparation {
        target_path,
        fields: public_workflow_preparation_fields(&raw_fields),
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
        identity,
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
fn public_workflow_reused_preparation(
    target_path: PathBuf,
    source_fingerprint_policy: SourceFingerprintPolicy,
    identity: shardloom_vortex::prepared_source_binding::LocalPreparationIdentity,
    elapsed_millis: u128,
) -> PublicWorkflowVortexPreparation {
    let mut raw_fields = vec![
        ("vortex_ingest_performed".into(), "false".into()),
        (
            "vortex_ingest_status".into(),
            "reused_embedded_source_binding".into(),
        ),
        ("prepared_state_created".into(), "false".into()),
        ("prepared_state_reused".into(), "true".into()),
        ("prepared_state_reuse_hit".into(), "true".into()),
        ("input_row_count".into(), identity.row_count.to_string()),
        (
            "target_vortex_path".into(),
            target_path.display().to_string(),
        ),
        (
            "prepared_artifact_ref".into(),
            target_path.display().to_string(),
        ),
        (
            "source_fingerprint_policy".into(),
            source_fingerprint_policy.as_str().into(),
        ),
        ("prepare_once_millis".into(), elapsed_millis.to_string()),
    ];
    public_preparation_identity_fields(&mut raw_fields, &identity, true);
    PublicWorkflowVortexPreparation {
        target_path,
        fields: public_workflow_preparation_fields(&raw_fields),
        identity: Some(std::sync::Arc::new(identity)),
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
fn public_preparation_identity_fields(
    fields: &mut Vec<(String, String)>,
    identity: &shardloom_vortex::prepared_source_binding::LocalPreparationIdentity,
    reused: bool,
) {
    // Public workflow identities describe the validated local generations. The
    // detailed ingest/capillary reports retain their own execution identities.
    for (key, value) in [
        (
            "source_state_id",
            format!("source-state-local-preparation-{}", identity.source_digest),
        ),
        ("source_state_digest", identity.source_digest.clone()),
        (
            "prepared_state_id",
            format!(
                "vortex-prepared-state-local-generation-{}",
                identity.prepared_digest
            ),
        ),
        ("prepared_state_digest", identity.prepared_digest.clone()),
        (
            "prepared_state_identity_policy",
            "validated_local_source_and_artifact_generations_v1".into(),
        ),
        ("prepared_state_reuse_allowed", "true".into()),
        (
            "prepared_state_reuse_scope",
            "embedded_source_binding".into(),
        ),
        (
            "prepared_state_reuse_manifest_path",
            "embedded:shardloom.prepared-source.v1".into(),
        ),
        (
            "prepared_state_reuse_manifest_digest",
            identity.source_digest.clone(),
        ),
        (
            "prepared_state_reuse_manifest_digest_algorithm",
            "sha256_source_binding_not_artifact_content".into(),
        ),
        (
            "prepared_state_reuse_policy",
            "unchanged_local_source_and_artifact_generation".into(),
        ),
        (
            "prepared_state_reuse_reason",
            if reused {
                "embedded_source_binding_matched"
            } else {
                "new_artifact_binding_validated"
            }
            .into(),
        ),
        ("prepared_state_invalidation_reason", "none".into()),
        ("fallback_attempted", "false".into()),
        ("external_engine_invoked", "false".into()),
    ] {
        set_cli_field(fields, key, value);
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
fn public_preparation_source_binding(
    path: &Path,
    format: Option<LocalSourceFormat>,
    policy: SourceFingerprintPolicy,
    source_schema: Option<&str>,
) -> Result<String, ShardLoomError> {
    let format = LocalInputAdapterSelection::select(path, format)?.source_format;
    let initial = shardloom_vortex::prepared_source_binding::local_preparation_binding_with_schema(
        path,
        format.as_str(),
        policy.as_str(),
        source_schema,
    )?;
    if policy == SourceFingerprintPolicy::MetadataOnly {
        return Ok(initial);
    }
    let scout = if path.is_dir() {
        scout_local_source_partition_files_with_budget(path, format, None, policy)?.evidence
    } else {
        fingerprint_local_source_file_with_budget_report(path, format.row_label(), None, policy)?
    };
    if shardloom_vortex::prepared_source_binding::local_preparation_binding_with_schema(
        path,
        format.as_str(),
        policy.as_str(),
        source_schema,
    )? != initial
    {
        return Err(ShardLoomError::InvalidOperation(
            "source changed during preparation fingerprint; no fallback execution was attempted"
                .into(),
        ));
    }
    shardloom_vortex::prepared_source_binding::local_preparation_binding_with_schema(
        path,
        format.as_str(),
        &scout.digest,
        source_schema,
    )
}

#[allow(clippy::too_many_lines)]
fn public_workflow_preparation_fields(raw_fields: &[(String, String)]) -> Vec<(String, String)> {
    const SELECTED_FIELDS: &[&str] = &[
        "vortex_ingest_performed",
        "vortex_ingest_status",
        "vortex_ingest_requested_memory_gb",
        "vortex_ingest_requested_max_parallelism",
        "source_format",
        "source_adapter_id",
        "source_adapter_boundary",
        "source_state_id",
        "source_state_digest",
        "source_state_contract_schema_version",
        "source_state_read_plan",
        "source_read_scout_schema_version",
        "source_read_scout_status",
        "source_read_scout_timing_split_status",
        "source_read_metadata_scout_millis",
        "source_read_byte_acquisition_millis",
        "source_read_full_body_millis",
        "source_read_buffer_carry_status",
        "source_read_mmap_eligibility_status",
        "source_read_many_small_file_batching_status",
        "source_fingerprint_kind",
        "source_fingerprint_policy",
        "source_fingerprint_identity_source",
        "source_content_fingerprint_requested",
        "source_content_fingerprint_performed",
        "source_state_projection_pushdown_status",
        "source_state_materialization_layout",
        "source_state_parse_normalization",
        "source_state_columnar_preserved",
        "source_state_record_batch_count",
        "source_state_stream_batch_size",
        "source_state_stream_unit_count_hint",
        "source_state_stream_unit_hint_kind",
        "source_state_stream_policy",
        "source_state_stream_unit_interface",
        "source_state_stream_unit_byte_range_count",
        "source_state_stream_unit_physical_bytes",
        "source_state_stream_unit_byte_range_sample",
        "source_state_stream_unit_physical_source",
        "source_state_stream_unit_scheduler_wait_status",
        "source_state_stream_unit_decode_wait_status",
        "source_state_stream_unit_writer_starvation_status",
        "source_state_stream_unit_physical_bandwidth_status",
        "source_state_parquet_extent_row_group_count",
        "source_state_parquet_extent_row_group_byte_range_count",
        "source_state_parquet_extent_column_chunk_count",
        "source_state_parquet_extent_column_chunk_byte_range_count",
        "source_state_parquet_extent_compressed_bytes",
        "source_state_parquet_extent_uncompressed_bytes",
        "source_state_parquet_extent_codec_summary",
        "source_state_parquet_extent_dictionary_page_count",
        "source_state_parquet_extent_statistics_count",
        "source_state_parquet_extent_row_group_summary",
        "source_state_dictionary_preservation_status",
        "source_state_ingest_executor_status",
        "source_state_ingest_executor_kind",
        "source_state_ingest_executor_requested_parallelism",
        "source_state_ingest_executor_applied_parallelism",
        "source_state_ingest_executor_unit_count_hint",
        "source_state_materialized_column_count",
        "source_state_materialized_columns",
        "source_state_reader_projection_column_count",
        "source_state_reader_projection_columns",
        "source_state_pruned_column_count",
        "source_state_column_pruning_applied",
        "prepared_state_id",
        "prepared_state_digest",
        "prepared_state_created",
        "prepared_state_reused",
        "prepared_state_reuse_hit",
        "prepared_state_identity_policy",
        "prepared_state_reuse_allowed",
        "prepared_state_reuse_scope",
        "prepared_state_reuse_manifest_path",
        "prepared_state_reuse_manifest_digest",
        "prepared_state_reuse_manifest_digest_algorithm",
        "prepared_state_reuse_policy",
        "prepared_state_reuse_reason",
        "prepared_state_invalidation_reason",
        "input_row_count",
        "target_vortex_path",
        "prepared_artifact_ref",
        "prepared_artifact_digest",
        "prepare_once_millis",
        "prepared_olap_publication_millis",
        "vortex_write_millis",
        "vortex_artifact_digest_source",
        "vortex_write_timing_split_schema_version",
        "vortex_writer_context_open_millis",
        "vortex_writer_context_reuse_status",
        "vortex_writer_runtime_kind",
        "vortex_writer_runtime_requested_parallelism",
        "vortex_writer_runtime_applied_parallelism",
        "vortex_writer_runtime_background_workers",
        "vortex_segment_write_millis",
        "vortex_compression_millis",
        "vortex_shared_native_memory_scope",
        "vortex_shared_native_memory_exclusions",
        "vortex_shared_native_memory_limit_bytes",
        "vortex_shared_native_memory_peak_reserved_bytes",
        "vortex_shared_native_memory_final_reserved_bytes",
        "vortex_shared_native_memory_denied_reservations",
        "vortex_shared_native_memory_max_source_batches",
        "vortex_opaque_arrow_owner_policy",
        "vortex_native_memory_physical_policy",
        "vortex_encode_write_millis",
        "vortex_workspace_stage_millis",
        "vortex_final_commit_millis",
        "vortex_digest_millis",
        "vortex_reopen_hot_path_status",
        "vortex_reopen_verify_millis",
        "vortex_copy_budget_status",
        "vortex_native_artifact_prepare_schema_version",
        "vortex_to_vortex_policy",
        "vortex_to_vortex_encoded_layout_preserved",
        "vortex_to_vortex_reencode_performed",
        "vortex_to_vortex_workspace_copy_performed",
        "vortex_to_vortex_same_artifact_passthrough",
        "vortex_to_vortex_upstream_vortex_scan_called",
        "vortex_to_vortex_upstream_vortex_write_called",
        "vortex_to_vortex_layout_rewrite_status",
        "vortex_to_vortex_source_digest_source",
        "vortex_to_vortex_target_digest_source",
        "vortex_copy_budget_buffer_reuse_status",
        "vortex_copy_budget_buffer_reuse_count",
        "vortex_capillary_preparation_max_parallelism",
        "vortex_capillary_preparation_prewrite_execution_window_size",
        "vortex_capillary_preparation_prewrite_external_engine_invoked",
        "universal_ingest_timing_split_schema_version",
        "universal_ingest_timing_split_status",
        "universal_ingest_source_read_millis",
        "universal_ingest_decode_derive_millis",
        "universal_ingest_decode_millis",
        "universal_ingest_derived_metadata_build_millis",
        "universal_ingest_arrow_to_vortex_convert_millis",
        "universal_ingest_encode_write_wall_millis",
        "universal_ingest_footer_register_millis",
        "universal_ingest_reopen_verify_millis",
        "universal_ingest_prepare_known_component_millis",
        "universal_ingest_prepare_unattributed_millis",
        "universal_ingest_prepare_attribution_status",
        "universal_ingest_prepare_attribution_policy",
        "universal_ingest_prepare_source_hydration_millis",
        "universal_ingest_prepare_nested_source_batch_production_millis",
        "universal_ingest_prepare_route_bookkeeping_millis",
        "universal_ingest_prepare_scheduler_wait_millis",
        "universal_ingest_prepare_writer_backpressure_or_timer_overlap_millis",
        "universal_ingest_prepare_evidence_emit_millis",
        "universal_ingest_prepare_residual_split_policy",
        "universal_ingest_stream_timing_overlap_policy",
        "vortex_array_build_millis",
        "vortex_array_build_provider_kind",
        "vortex_array_build_provider_surface",
        "vortex_array_build_strategy",
        "vortex_array_build_prefetch_window",
        "vortex_array_build_input_layout",
        "vortex_array_build_record_batch_count",
        "vortex_array_build_manual_scalar_copy_avoided",
        "vortex_layout_write_advisor_source_scale",
        "vortex_layout_write_advisor_profile_family",
        "vortex_layout_write_advisor_prepared_layout_family",
        "vortex_layout_write_advisor_text_domain_columns",
        "vortex_layout_write_advisor_time_bucket_columns",
        "vortex_layout_write_advisor_counter_columns",
        "vortex_layout_write_advisor_key_profile",
        "vortex_layout_write_advisor_dictionary_profile",
        "vortex_layout_write_advisor_writer_parallelism_budget",
        "vortex_layout_write_advisor_expected_read_tradeoff",
        "vortex_layout_write_advisor_expected_write_tradeoff",
        "vortex_writer_layout_strategy_applied",
        "vortex_writer_coalescing_policy_status",
        "vortex_writer_layout_row_block_size",
        "vortex_writer_layout_block_target_bytes",
        "vortex_writer_compression_policy",
        "vortex_writer_compression_field_count",
        "vortex_writer_compression_field_names",
        "vortex_writer_compression_decision_count",
        "vortex_writer_compression_decisions",
        "vortex_writer_compression_concurrency",
        "vortex_writer_stats_concurrency",
        "vortex_writer_profile_selection_reason",
        "vortex_writer_profile_regression_guard",
        "vortex_writer_physical_design_schema_version",
        "vortex_writer_physical_design_status",
        "vortex_writer_physical_design_planner",
        "vortex_writer_physical_design_selected_strategy",
        "vortex_writer_physical_design_decision_digest",
        "vortex_writer_physical_design_provider_decision",
        "vortex_writer_physical_design_provider_kind",
        "vortex_writer_physical_design_provider_surface",
        "vortex_writer_physical_design_admission_policy",
        "vortex_writer_physical_design_certification_level",
        "vortex_writer_physical_design_source_stage_plan",
        "vortex_writer_physical_design_derived_metadata_stage_plan",
        "vortex_writer_physical_design_array_build_stage_plan",
        "vortex_writer_physical_design_compression_layout_stage_plan",
        "vortex_writer_physical_design_writer_feed_stage_plan",
        "vortex_writer_physical_design_commit_stage_plan",
        "vortex_writer_physical_design_source_format",
        "vortex_writer_physical_design_source_stream_batch_size",
        "vortex_writer_physical_design_source_stream_unit_count_hint",
        "vortex_writer_physical_design_source_stream_unit_hint_kind",
        "vortex_writer_physical_design_source_stream_policy",
        "vortex_writer_physical_design_source_executor_status",
        "vortex_writer_physical_design_source_executor_kind",
        "vortex_writer_physical_design_source_executor_requested_parallelism",
        "vortex_writer_physical_design_source_executor_applied_parallelism",
        "vortex_writer_physical_design_source_executor_unit_count_hint",
        "vortex_writer_physical_design_array_build_prefetch_window",
        "vortex_writer_physical_design_array_build_worker_count",
        "vortex_writer_physical_design_array_build_input_layout",
        "vortex_writer_physical_design_writer_row_block_size",
        "vortex_writer_physical_design_writer_block_target_bytes",
        "vortex_writer_physical_design_writer_compression_policy",
        "vortex_writer_physical_design_writer_compression_field_count",
        "vortex_writer_physical_design_writer_compression_field_names",
        "vortex_writer_physical_design_writer_compression_decision_count",
        "vortex_writer_physical_design_writer_compression_decisions",
        "vortex_writer_physical_design_writer_compression_concurrency",
        "vortex_writer_physical_design_writer_stats_concurrency",
        "vortex_writer_physical_design_writer_runtime_kind",
        "vortex_writer_physical_design_writer_runtime_requested_parallelism",
        "vortex_writer_physical_design_writer_runtime_applied_parallelism",
        "vortex_writer_physical_design_writer_runtime_background_workers",
        "vortex_writer_physical_design_writer_queue_topology",
        "vortex_writer_physical_design_writer_backpressure_policy",
        "vortex_writer_physical_design_writer_profile_selection_reason",
        "vortex_writer_physical_design_writer_profile_regression_guard",
        "vortex_writer_physical_design_retained_ingest_baseline_seconds",
        "vortex_writer_physical_design_rejected_experimental_patch_seconds",
        "vortex_writer_physical_design_retention_status",
        "vortex_writer_physical_design_no_fallback_policy",
        "vortex_writer_physical_design_fallback_attempted",
        "vortex_writer_physical_design_external_engine_invoked",
        "vortex_segment_metadata_schema_version",
        "vortex_segment_metadata_status",
        "vortex_segment_metadata_primitive_id",
        "vortex_segment_metadata_source",
        "vortex_segment_metadata_inventory_status",
        "vortex_segment_metadata_inventory_digest",
        "vortex_segment_metadata_writer_physical_design_digest",
        "vortex_segment_metadata_row_count",
        "vortex_segment_metadata_segment_count",
        "vortex_segment_metadata_row_count_proven",
        "vortex_segment_metadata_segment_count_proven",
        "vortex_segment_metadata_statistics_status",
        "vortex_segment_metadata_row_range_coverage",
        "vortex_segment_metadata_physical_byte_range_status",
        "vortex_segment_metadata_null_count_status",
        "vortex_segment_metadata_min_max_status",
        "vortex_segment_metadata_byte_length_bounds_status",
        "vortex_segment_metadata_dictionary_membership_status",
        "vortex_segment_metadata_domain_absence_status",
        "vortex_segment_metadata_cardinality_sketch_status",
        "vortex_segment_metadata_row_position_locality_status",
        "vortex_segment_metadata_encoded_layout_status",
        "vortex_segment_metadata_per_column_contract",
        "vortex_segment_metadata_read_plan",
        "vortex_segment_metadata_query_use_policy",
        "vortex_segment_metadata_metadata_count_admission",
        "vortex_segment_metadata_predicate_pruning_admission",
        "vortex_segment_metadata_group_by_admission",
        "vortex_segment_metadata_topk_admission",
        "vortex_segment_metadata_dictionary_distinct_admission",
        "vortex_segment_metadata_no_false_negative_policy",
        "vortex_segment_metadata_query_answer_sidecar_status",
        "vortex_segment_metadata_digest",
        "vortex_segment_metadata_fallback_attempted",
        "vortex_segment_metadata_external_engine_invoked",
        "vortex_prepared_state_reuse_index_schema_version",
        "vortex_prepared_state_reuse_index_lookup_status",
        "vortex_prepared_state_reuse_index_cache_scope",
        "vortex_prepared_state_reuse_index_repair_status",
        "vortex_prepared_state_reuse_role_scoped_repair_status",
        "prepared_olap_state_allowed",
        "prepared_olap_state_status",
        "prepared_olap_state_evidence_persistence",
        "prepared_olap_state_external_manifest_written",
        "prepared_olap_state_query_time_contract",
        "prepared_olap_state_artifact_model",
        "prepared_olap_state_writer_layout_strategy",
        "prepared_olap_state_embedded_layout_statistics_contract",
        "prepared_olap_state_layout_inventory_status",
        "prepared_olap_state_layout_inventory_digest",
        "prepared_olap_state_layout_footer_row_count",
        "prepared_olap_state_layout_footer_segment_count",
        "prepared_olap_state_layout_footer_statistics_status",
        "prepared_olap_state_layout_footer_encoding_layout_status",
        "prepared_olap_state_layout_artifact_size_bytes",
        "prepared_olap_state_layout_footer_approx_bytes",
        "prepared_olap_state_layout_footer_dtype_summary",
        "prepared_olap_state_layout_metadata_persisted_in_artifact",
        "prepared_olap_state_layout_size_attribution",
        "prepared_olap_state_dictionary_metadata_policy",
        "prepared_olap_state_metadata_pruning_contract",
        "prepared_olap_state_query_answer_sidecar_status",
        "prepared_olap_state_admitted_query_families",
        "prepared_olap_state_exact_sidecar_family_count",
        "prepared_olap_state_sub_second_candidate",
        "prepared_olap_state_blocker_id",
        "vortex_prepared_olap_state_schema_version",
        "vortex_prepared_olap_state_status",
        "vortex_prepared_olap_state_policy",
        "vortex_prepared_olap_state_evidence_persistence",
        "vortex_prepared_olap_state_external_manifest_written",
        "vortex_prepared_olap_state_id",
        "vortex_prepared_olap_state_digest",
        "vortex_prepared_olap_state_prepared_artifact_ref",
        "vortex_prepared_olap_state_prepared_artifact_digest",
        "vortex_prepared_olap_state_artifact_model",
        "vortex_prepared_olap_state_writer_layout_strategy",
        "vortex_prepared_olap_state_embedded_layout_statistics_contract",
        "vortex_prepared_olap_state_layout_inventory_status",
        "vortex_prepared_olap_state_layout_inventory_digest",
        "vortex_prepared_olap_state_layout_footer_row_count",
        "vortex_prepared_olap_state_layout_footer_segment_count",
        "vortex_prepared_olap_state_layout_footer_statistics_status",
        "vortex_prepared_olap_state_layout_footer_encoding_layout_status",
        "vortex_prepared_olap_state_layout_artifact_size_bytes",
        "vortex_prepared_olap_state_layout_footer_approx_bytes",
        "vortex_prepared_olap_state_layout_footer_dtype_summary",
        "vortex_prepared_olap_state_layout_metadata_persisted_in_artifact",
        "vortex_prepared_olap_state_layout_size_attribution",
        "vortex_prepared_olap_state_dictionary_metadata_policy",
        "vortex_prepared_olap_state_metadata_pruning_contract",
        "vortex_prepared_olap_state_query_answer_sidecar_status",
        "vortex_prepared_olap_state_admitted_query_families",
        "vortex_prepared_olap_state_exact_sidecar_family_count",
        "vortex_prepared_olap_state_query_time_contract",
        "vortex_prepared_olap_state_sub_second_candidate",
        "vortex_prepared_olap_state_blocker_id",
        "query_timing_starts_after_preparation",
        "local_workflow_input_row_cap",
        "local_workflow_synthetic_input_row_cap_enabled",
        "local_workflow_synthetic_output_row_cap_enabled",
        "local_workflow_synthetic_source_byte_cap_enabled",
        "local_workflow_synthetic_join_candidate_cap_enabled",
        "fallback_attempted",
        "external_engine_invoked",
    ];
    let raw_map = raw_fields
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect::<BTreeMap<_, _>>();
    let mut selected: Vec<_> = SELECTED_FIELDS
        .iter()
        .filter_map(|key| {
            raw_map
                .get(key)
                .map(|value| (*value).to_string())
                .or_else(|| public_workflow_preparation_derived_field(&raw_map, key))
                .map(|value| (format!("public_workflow_preparation_{key}"), value))
        })
        .collect();
    for (key, value) in &raw_map {
        if key.starts_with("execution_resource_") {
            selected.push((
                format!("public_workflow_preparation_{key}"),
                (*value).to_string(),
            ));
        }
    }
    // Derive the allowlist from the typed report, but forward only observed
    // fields. A route without instrumentation must not manufacture zero work.
    for (key, _) in
        shardloom_vortex::vortex_ingest::VortexIngestStageReport::default().evidence_fields()
    {
        if let Some(value) = raw_map.get(key.as_str()) {
            selected.push((
                format!("public_workflow_preparation_{key}"),
                (*value).to_string(),
            ));
        }
    }
    selected
}

const STREAM_POLICY_DERIVED_FIELDS: &[(&str, &str)] = &[
    (
        "source_state_stream_unit_interface",
        "source_unit_interface",
    ),
    (
        "source_state_stream_unit_byte_range_count",
        "source_unit_byte_range_count",
    ),
    (
        "source_state_stream_unit_physical_bytes",
        "source_unit_physical_bytes",
    ),
    (
        "source_state_stream_unit_byte_range_sample",
        "source_unit_byte_range_sample",
    ),
    (
        "source_state_stream_unit_physical_source",
        "source_unit_physical_source",
    ),
    (
        "source_state_stream_unit_scheduler_wait_status",
        "source_unit_scheduler_wait_status",
    ),
    (
        "source_state_stream_unit_decode_wait_status",
        "source_unit_decode_wait_status",
    ),
    (
        "source_state_stream_unit_writer_starvation_status",
        "source_unit_writer_starvation_status",
    ),
    (
        "source_state_stream_unit_physical_bandwidth_status",
        "source_unit_physical_bandwidth_status",
    ),
    (
        "source_state_parquet_extent_row_group_count",
        "parquet_extent_row_groups",
    ),
    (
        "source_state_parquet_extent_row_group_byte_range_count",
        "parquet_extent_row_groups_with_byte_ranges",
    ),
    (
        "source_state_parquet_extent_column_chunk_count",
        "parquet_extent_column_chunks",
    ),
    (
        "source_state_parquet_extent_column_chunk_byte_range_count",
        "parquet_extent_column_chunks_with_byte_ranges",
    ),
    (
        "source_state_parquet_extent_compressed_bytes",
        "parquet_extent_compressed_bytes",
    ),
    (
        "source_state_parquet_extent_uncompressed_bytes",
        "parquet_extent_uncompressed_bytes",
    ),
    (
        "source_state_parquet_extent_codec_summary",
        "parquet_extent_codec_summary",
    ),
];

const DICTIONARY_STATUS_DERIVED_FIELDS: &[(&str, &str)] = &[
    (
        "source_state_parquet_extent_dictionary_page_count",
        "parquet_extent_dictionary_pages",
    ),
    (
        "source_state_parquet_extent_statistics_count",
        "parquet_extent_statistics",
    ),
    (
        "source_state_parquet_extent_row_group_summary",
        "parquet_extent_row_group_summary",
    ),
];

fn public_workflow_preparation_derived_field(
    raw_fields: &BTreeMap<&str, &str>,
    key: &str,
) -> Option<String> {
    STREAM_POLICY_DERIVED_FIELDS
        .iter()
        .find_map(|(field, evidence_key)| {
            (*field == key).then(|| {
                semicolon_evidence_value_opt(
                    raw_fields.get("source_state_stream_policy").copied(),
                    evidence_key,
                )
            })?
        })
        .or_else(|| {
            DICTIONARY_STATUS_DERIVED_FIELDS
                .iter()
                .find_map(|(field, evidence_key)| {
                    (*field == key).then(|| {
                        semicolon_evidence_value_opt(
                            raw_fields
                                .get("source_state_dictionary_preservation_status")
                                .copied(),
                            evidence_key,
                        )
                    })?
                })
        })
}

fn semicolon_evidence_value_opt(raw: Option<&str>, key: &str) -> Option<String> {
    let value = semicolon_evidence_value(raw?, key);
    (value != "not_available").then_some(value)
}

fn normalized_output_path_key(path: &Path) -> String {
    path.to_string_lossy()
        .replace('\\', "/")
        .to_ascii_lowercase()
}

fn canonical_output_path_key(path: &Path) -> Result<String, ShardLoomError> {
    let workspace_root = shardloom_core::infer_local_output_workspace_root(path)?;
    let plan = shardloom_core::plan_workspace_safe_local_output(workspace_root, path, true)?;
    Ok(normalized_output_path_key(&plan.target_path))
}

fn run_vortex_prepare_with_schema(
    request: VortexIngestRequest,
    source_schema_hints: &[(String, LogicalDType)],
) -> Result<VortexIngestOutcome, ShardLoomError> {
    let delta = request.delta.clone();
    if let Some(delta) = delta {
        preflight_vortex_ingest_differential_request(&request, &delta)?;
        let mut base_report = run_vortex_ingest_prepare_once_without_reuse_with_schema(
            VortexIngestRequest {
                delta: None,
                ..request
            },
            source_schema_hints,
        )?;
        let delta_report = run_vortex_ingest_prepare_once_without_reuse_with_schema(
            VortexIngestRequest {
                source_path: delta.source_path.clone(),
                source_format_override: base_report.request.source_format_override,
                target_path: delta.target_path.clone(),
                allow_overwrite: base_report.request.allow_overwrite,
                certification_level: base_report.request.certification_level,
                runtime_profile: base_report.request.runtime_profile,
                resources: base_report.request.resources,
                shared_memory_pool: base_report.request.shared_memory_pool.clone(),
                source_fingerprint_policy: base_report.request.source_fingerprint_policy,
                delta: None,
                prepared_source_binding: None,
            },
            source_schema_hints,
        )
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "vortex_ingest differential delta source '{}' failed preparation: {error}; no fallback execution was attempted",
                delta.source_path.display()
            ))
        })?;
        base_report.differential_preparation = Some(differential_preparation_report(
            &base_report,
            &delta_report,
            delta.update_mode,
        ));
        return Ok(VortexIngestOutcome::Prepared(Box::new(base_report)));
    }
    run_vortex_ingest_prepare_once_with_schema(request, source_schema_hints)
}

fn preflight_vortex_ingest_differential_request(
    request: &VortexIngestRequest,
    delta: &VortexIngestDeltaRequest,
) -> Result<(), ShardLoomError> {
    if request.certification_level
        != shardloom_vortex::VortexIngestCertificationLevel::IngestCertified
    {
        return Err(ShardLoomError::InvalidOperation(
            "vortex_ingest differential preparation requires ingest_certified replay evidence before any base or delta write; no fallback execution was attempted"
                .to_string(),
        ));
    }
    let base_target = canonical_output_path_key(&request.target_path)?;
    let delta_target = canonical_output_path_key(&delta.target_path)?;
    if base_target == delta_target {
        return Err(ShardLoomError::InvalidOperation(format!(
            "vortex_ingest differential preparation requires distinct base and delta targets; both resolved to {}; no fallback execution was attempted",
            request.target_path.display()
        )));
    }
    Ok(())
}

fn run_vortex_ingest_prepare_once_with_schema(
    request: VortexIngestRequest,
    source_schema_hints: &[(String, LogicalDType)],
) -> Result<VortexIngestOutcome, ShardLoomError> {
    let source_adapter =
        LocalInputAdapterSelection::select(&request.source_path, request.source_format_override)?;
    let mut report =
        run_vortex_ingest_prepare_once_with_adapter(request, source_adapter, source_schema_hints)?;
    let publication_start = Instant::now();
    report.prepared_olap_state = Some(write_prepared_olap_state_for_report(&report)?);
    report.prepared_olap_publication_millis = publication_start.elapsed().as_millis();
    Ok(VortexIngestOutcome::Prepared(Box::new(report)))
}

fn write_prepared_olap_state_for_report(
    report: &VortexIngestReport,
) -> Result<shardloom_vortex::VortexPreparedOlapStateReport, ShardLoomError> {
    let source_row_count = u64::try_from(report.source.row_count).map_err(|_| {
        ShardLoomError::InvalidOperation(
            "prepared OLAP state source row count overflowed u64; no fallback execution was attempted"
                .to_string(),
        )
    })?;
    let request = shardloom_vortex::VortexPreparedOlapStateWriteRequest::new_local(
        report.source_state_id.clone(),
        report.source_state_digest.clone(),
        report.source.source_format.as_str(),
        report.source.source_digest.clone(),
        report.source.source_bytes,
        report.source_schema_digest.clone(),
        source_row_count,
        &report.vortex_report.target_path,
        &report.vortex_report.target_path,
    )?
    .with_prepared_layout_policy("single_vortex_artifact_embedded_vortex_layout_statistics_v1")
    .with_segment_map_status(
        report
            .vortex_report
            .prepared_olap_layout_inventory
            .segment_membership_status
            .clone(),
    )
    .with_feature_gates(report.vortex_report.preparation_spine.feature_gate.clone())
    .with_certification_level(report.request.certification_level.as_str())
    .with_certificate_refs(format!(
        "source_state={};prepared_state={};artifact={};native_io={}",
        report.source_state_id,
        report.prepared_state_id,
        report.vortex_report.target_path.display(),
        report
            .vortex_report
            .preparation_spine
            .native_io_certificate_refs
    ));
    shardloom_vortex::write_vortex_prepared_olap_state_bundle(request)
}

fn run_vortex_ingest_prepare_once_without_reuse_with_schema(
    request: VortexIngestRequest,
    source_schema_hints: &[(String, LogicalDType)],
) -> Result<VortexIngestReport, ShardLoomError> {
    let source_adapter =
        LocalInputAdapterSelection::select(&request.source_path, request.source_format_override)?;
    run_vortex_ingest_prepare_once_with_adapter(request, source_adapter, source_schema_hints)
}

#[allow(clippy::needless_pass_by_value)]
fn run_vortex_ingest_prepare_once_with_adapter(
    request: VortexIngestRequest,
    source_adapter: LocalInputAdapterSelection,
    source_schema_hints: &[(String, LogicalDType)],
) -> Result<VortexIngestReport, ShardLoomError> {
    #[cfg(not(all(feature = "vortex-write", feature = "universal-format-io")))]
    let _ = &source_adapter;
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    {
        if source_adapter
            .source_format
            .preserves_columnar_vortex_ingest_source_state()
        {
            return run_columnar_vortex_prepare(request, source_adapter, source_schema_hints);
        }
    }
    run_scalar_vortex_prepare(request, source_adapter, source_schema_hints)
}

fn differential_preparation_report(
    base: &VortexIngestReport,
    delta: &VortexIngestReport,
    update_mode: shardloom_vortex::VortexDifferentialUpdateMode,
) -> shardloom_vortex::VortexDifferentialPreparationReport {
    let delta_byte_ranges = delta
        .source
        .preparation_spine_source_byte_range_refs(&delta.source_state_id);
    let delta_row_ranges = delta
        .source
        .preparation_spine_source_row_range_refs(&delta.source_state_id);
    let delta_segment_refs = delta.prepared_artifact_segment_refs();
    let delta_manifest_digest = fnv64_digest(&format!(
        "{}|{}|{}|{}|{}|{}",
        delta.source_state_digest,
        delta.prepared_state_digest,
        delta.vortex_report.artifact_digest,
        delta_byte_ranges,
        delta_row_ranges,
        update_mode.as_str()
    ));
    let native_io_certificate_refs = format!(
        "base_prepared_state={};delta_prepared_state={};delta_artifact={};reopen_row_count_scan",
        base.prepared_state_id,
        delta.prepared_state_id,
        delta.request.target_path.display()
    );

    shardloom_vortex::evaluate_vortex_differential_preparation(
        shardloom_vortex::VortexDifferentialPreparationInput {
            update_mode,
            base_source_state_id: base.source_state_id.clone(),
            base_source_state_digest: base.source_state_digest.clone(),
            base_prepared_state_id: base.prepared_state_id.clone(),
            base_prepared_state_digest: base.prepared_state_digest.clone(),
            base_row_count: base.vortex_report.row_count,
            base_schema_digest: base.source_schema_digest.clone(),
            base_column_family_summary: base.vortex_report.column_family_summary(),
            delta_source_state_id: delta.source_state_id.clone(),
            delta_source_state_digest: delta.source_state_digest.clone(),
            delta_row_count: delta.vortex_report.row_count,
            delta_schema_digest: delta.source_schema_digest.clone(),
            delta_column_family_summary: delta.vortex_report.column_family_summary(),
            delta_manifest_digest,
            changed_byte_range_refs: delta_byte_ranges,
            changed_row_range_refs: delta_row_ranges,
            changed_segment_refs: delta_segment_refs,
            delta_artifact_ref: delta.request.target_path.display().to_string(),
            delta_artifact_digest: delta.vortex_report.artifact_digest.clone(),
            native_io_certificate_refs,
        },
    )
}

fn scout_ingress_report(
    source: &VortexIngestSourceData,
    source_path: &Path,
    source_state_id: &str,
    source_state_digest: &str,
    source_schema_digest: &str,
) -> shardloom_vortex::VortexScoutIngressReport {
    shardloom_vortex::evaluate_vortex_scout_ingress(shardloom_vortex::VortexScoutIngressInput {
        source_state_id: source_state_id.to_string(),
        source_state_digest: source_state_digest.to_string(),
        source_format: source.source_format.as_str().to_string(),
        source_path: source_path.display().to_string(),
        source_schema_digest: source_schema_digest.to_string(),
        row_count: u64::try_from(source.row_count).unwrap_or(u64::MAX),
        source_byte_count: source.source_bytes,
        column_count: source.header.len(),
        read_plan: source.read_plan.status().to_string(),
        metadata_range_refs: source.preparation_spine_source_byte_range_refs(source_state_id),
        sampled_row_range_refs: source.preparation_spine_source_row_range_refs(source_state_id),
        anomaly_count: 0,
        anomaly_families: "none".to_string(),
        malformed_row_refs: "none".to_string(),
        schema_drift_status: "not_detected_no_prior_schema_baseline".to_string(),
        unsupported_shape_status: "not_detected".to_string(),
        nullability_status: "nullable_fields_admitted_as_scalar_nulls".to_string(),
        small_file_pathology_status: scout_small_file_pathology_status(source),
        quarantine_required: false,
        quarantine_output_plan_status: "not_required".to_string(),
        quarantine_output_ref: "not_requested".to_string(),
        quarantine_output_digest: "not_requested".to_string(),
        redaction_status: "malformed_row_refs_are_row_numbers_only".to_string(),
        unsupported_diagnostic_code: "none".to_string(),
        correctness_policy: "fail_closed_no_silent_repair_or_row_drop".to_string(),
        fallback_attempted: false,
        external_engine_invoked: false,
    })
}

fn scout_small_file_pathology_status(source: &VortexIngestSourceData) -> String {
    if source.source_bytes < 4096 {
        "observed_tiny_local_fixture_not_blocking".to_string()
    } else {
        "not_detected".to_string()
    }
}

fn layout_write_advisor_report(
    source: &VortexIngestSourceData,
    source_state_id: &str,
    source_state_digest: &str,
    source_schema_digest: &str,
    certification_level: shardloom_vortex::VortexIngestCertificationLevel,
    max_parallelism: usize,
) -> shardloom_vortex::VortexLayoutWriteAdvisorReport {
    shardloom_vortex::evaluate_vortex_layout_write_advisor(
        shardloom_vortex::VortexLayoutWriteAdvisorInput {
            source_state_id: source_state_id.to_string(),
            source_state_digest: source_state_digest.to_string(),
            source_format: source.source_format.as_str().to_string(),
            source_schema_digest: source_schema_digest.to_string(),
            row_count: u64::try_from(source.row_count).unwrap_or(u64::MAX),
            source_byte_count: source.source_bytes,
            column_count: source.header.len(),
            workload_constitution: layout_workload_constitution(source),
            source_statistics_status: layout_source_statistics_status(source),
            requested_pushdown_requirements: "none_prepare_once_full_source".to_string(),
            sink_requirements: "workspace_safe_local_vortex_file_sink".to_string(),
            layout_strategy: "single_vortex_artifact_embedded_olap_layout_statistics".to_string(),
            chunking_strategy: layout_chunking_strategy(source),
            segmentation_strategy: "upstream_vortex_writer_default_zoned_segments".to_string(),
            dictionary_strategy:
                "preserve_vortex_dictionary_and_encoding_metadata_when_writer_emits_it".to_string(),
            statistics_policy: "preserve_vortex_layout_footer_statistics_for_metadata_pruning"
                .to_string(),
            writer_provider_kind: layout_writer_provider_kind(source).to_string(),
            writer_provider_surface: layout_writer_provider_surface(source).to_string(),
            writer_admission_policy:
                shardloom_vortex::VORTEX_PRODUCT_LOCAL_INGEST_PREPARE_ONCE_ADMISSION_POLICY
                    .to_string(),
            writer_parallelism_budget: max_parallelism,
            writer_compression_candidate_fields: layout_writer_compression_candidate_fields(source),
            write_reopen_verification_depth: layout_verification_depth(certification_level)
                .to_string(),
            materialization_boundary_status: source.materialization_layout.to_string(),
            decode_boundary_status: source.parse_normalization.to_string(),
            expected_read_tradeoff: layout_expected_read_tradeoff(source).to_string(),
            expected_write_tradeoff: layout_expected_write_tradeoff(source).to_string(),
            strategy_admitted: true,
            unsupported_diagnostic_code: "none".to_string(),
            correctness_refs: "writer_row_count,reopen_row_count,artifact_digest".to_string(),
            benchmark_refs: "not_claim_grade_benchmark_refresh_deferred".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        },
    )
}

fn layout_workload_constitution(source: &VortexIngestSourceData) -> String {
    format!(
        "product_vortex_prepare_once;format={};scale={};adapter={};profile={};layout_family={};text_domain={};time_bucket={};counter={};key_profile={};dictionary={}",
        source.source_format.as_str(),
        layout_source_scale(source),
        if source.columnar_source_preserved {
            "streaming_columnar_source_state"
        } else {
            "typed_scalar_source_bridge"
        },
        layout_source_profile(source),
        layout_prepared_layout_family(source),
        layout_source_has_text_domain_columns(source),
        layout_source_has_time_bucket_columns(source),
        layout_source_has_counter_columns(source),
        layout_key_profile(source),
        layout_dictionary_profile(source),
    )
}

fn layout_source_scale(source: &VortexIngestSourceData) -> &'static str {
    if source.row_count >= 10_000_000 || source.source_bytes >= 1_073_741_824 {
        "large_olap"
    } else if source.row_count >= 1_000_000 || source.source_bytes >= 64 * 1024 * 1024 {
        "medium_olap"
    } else {
        "small_local"
    }
}

fn layout_source_profile(source: &VortexIngestSourceData) -> &'static str {
    match (
        layout_source_has_text_domain_columns(source),
        layout_source_has_time_bucket_columns(source),
        layout_source_has_counter_columns(source),
    ) {
        (true, true, true) => "url_time_counter_olap",
        (true, true, false) => "url_time_olap",
        (true, false, _) => "url_text_olap",
        (false, true, true) => "time_counter_olap",
        (false, true, false) => "time_olap",
        (false, false, true) => "counter_olap",
        (false, false, false) => "generic_columnar_olap",
    }
}

fn layout_source_has_text_domain_columns(source: &VortexIngestSourceData) -> bool {
    source
        .header
        .iter()
        .any(|column| layout_text_domain_column_name(column))
}

fn layout_text_domain_column_name(column: &str) -> bool {
    let lower = column.to_ascii_lowercase();
    lower.contains("url")
        || lower.contains("referer")
        || lower.contains("search")
        || lower.contains("phrase")
        || lower.contains("title")
        || lower.contains("utm")
        || lower.contains("param")
        || lower.contains("useragent")
}

fn layout_writer_compression_candidate_fields(source: &VortexIngestSourceData) -> Vec<String> {
    source
        .header
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            layout_writer_compression_candidate_field(
                column,
                source
                    .column_arrow_dtypes
                    .get(index)
                    .and_then(Option::as_ref),
            )
        })
        .collect()
}

fn layout_writer_compression_candidate_field(
    column: &str,
    arrow_dtype: Option<&DataType>,
) -> Option<String> {
    if column.starts_with("__shardloom_derived_") {
        return None;
    }
    match arrow_dtype {
        Some(dtype) if writer_candidate_arrow_dtype_is_utf8_like(dtype) => Some(column.to_string()),
        None if layout_text_domain_column_name(column) => Some(column.to_string()),
        Some(_) | None => None,
    }
}

fn writer_candidate_arrow_dtype_is_utf8_like(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::Dictionary(key, value) => {
            matches!(
                key.as_ref(),
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            ) && writer_candidate_arrow_dtype_is_utf8_like(value.as_ref())
        }
        _ => false,
    }
}

fn layout_source_has_time_bucket_columns(source: &VortexIngestSourceData) -> bool {
    source
        .header
        .iter()
        .any(|column| layout_time_bucket_column_name(column))
}

fn layout_time_bucket_column_name(column: &str) -> bool {
    let lower = column.to_ascii_lowercase();
    lower == "eventtime"
        || lower == "event_time"
        || lower.ends_with("_time")
        || lower.ends_with("time")
        || lower.contains("timestamp")
        || lower.contains("date")
}

fn layout_source_has_counter_columns(source: &VortexIngestSourceData) -> bool {
    source.header.iter().any(|column| {
        let lower = column.to_ascii_lowercase();
        lower.contains("id")
            || lower.contains("counter")
            || lower.contains("userid")
            || lower.contains("watchid")
            || lower.contains("clientip")
            || lower.contains("region")
            || lower.contains("browser")
    })
}

fn layout_prepared_layout_family(source: &VortexIngestSourceData) -> &'static str {
    match layout_source_profile(source) {
        "url_time_counter_olap" => "url_time_counter_dictionary_stats_layout",
        "url_time_olap" => "url_time_dictionary_stats_layout",
        "url_text_olap" => "url_domain_dictionary_length_layout",
        "time_counter_olap" => "time_counter_typed_stats_layout",
        "time_olap" => "time_bucket_typed_stats_layout",
        "counter_olap" => "counter_typed_stats_layout",
        _ => "generic_columnar_stats_layout",
    }
}

fn layout_key_profile(source: &VortexIngestSourceData) -> &'static str {
    match (
        layout_source_has_high_cardinality_key_columns(source),
        layout_source_has_text_domain_columns(source),
        layout_source_has_time_bucket_columns(source),
    ) {
        (true, true, true) => "high_cardinality_numeric_text_time_keys",
        (true, true, false) => "high_cardinality_numeric_text_keys",
        (true, false, true) => "high_cardinality_numeric_time_keys",
        (true, false, false) => "high_cardinality_numeric_keys",
        (false, true, true) => "dictionary_text_time_keys",
        (false, true, false) => "dictionary_text_keys",
        (false, false, true) => "typed_time_keys",
        (false, false, false) => "generic_typed_keys",
    }
}

fn layout_source_has_high_cardinality_key_columns(source: &VortexIngestSourceData) -> bool {
    source.header.iter().any(|column| {
        let lower = column.to_ascii_lowercase();
        matches!(
            lower.as_str(),
            "userid"
                | "watchid"
                | "clientip"
                | "urlhash"
                | "refererhash"
                | "titlehash"
                | "windowclientwidth"
                | "windowclientheight"
        ) || lower.ends_with("_id")
            || lower.ends_with("id")
    })
}

fn layout_dictionary_profile(source: &VortexIngestSourceData) -> &'static str {
    if source
        .source_dictionary_preservation_status
        .contains("dictionary")
    {
        "source_dictionary_or_derived_dictionary_evidence"
    } else if source.columnar_source_preserved {
        "columnar_dictionary_status_provider_dependent"
    } else {
        "text_typed_builders_no_source_dictionary"
    }
}

fn layout_expected_read_tradeoff(source: &VortexIngestSourceData) -> &'static str {
    match (layout_source_scale(source), layout_source_profile(source)) {
        ("large_olap", "url_time_counter_olap" | "url_time_olap" | "url_text_olap") => {
            "prefer_metadata_pruning_dictionary_domain_length_and_time_bucket_execution"
        }
        ("large_olap", "time_counter_olap" | "time_olap") => {
            "prefer_typed_time_bucket_counter_stats_and_late_materialization"
        }
        ("large_olap", "counter_olap") => {
            "prefer_typed_counter_stats_fast_load_and_late_materialization"
        }
        ("large_olap", _) => "prefer_large_source_typed_stats_and_late_materialization",
        ("medium_olap", "url_time_counter_olap" | "url_time_olap" | "url_text_olap") => {
            "prefer_medium_source_embedded_text_domain_metadata_when_exact"
        }
        ("medium_olap", _) => "prefer_medium_source_typed_stats_without_extra_layout_overhead",
        _ => "prefer_small_local_correctness_and_low_setup_overhead",
    }
}

fn layout_expected_write_tradeoff(source: &VortexIngestSourceData) -> &'static str {
    match (layout_source_scale(source), layout_source_profile(source)) {
        ("large_olap", "url_time_counter_olap" | "url_time_olap" | "url_text_olap") => {
            "prefer_column_family_fast_zstd_for_payload_text_and_embedded_layout_statistics"
        }
        ("large_olap", _) => "prefer_fast_load_uncompressed_layout_to_reduce_ingest_wall_time",
        ("medium_olap", "url_time_counter_olap" | "url_time_olap" | "url_text_olap") => {
            "balance_embedded_text_metadata_with_bounded_writer_overhead"
        }
        ("medium_olap", _) => "prefer_default_writer_layout_with_profile_evidence",
        _ => "prefer_small_local_default_writer_layout",
    }
}

fn layout_source_statistics_status(source: &VortexIngestSourceData) -> String {
    if source.source_bytes == 0 {
        "empty_source_file_stats_only".to_string()
    } else {
        "local_file_byte_row_column_stats_only".to_string()
    }
}

fn layout_chunking_strategy(source: &VortexIngestSourceData) -> String {
    if source.row_count <= 4096 {
        "single_chunk_for_product_local_small_source".to_string()
    } else {
        "upstream_vortex_writer_default_zoned_row_blocks".to_string()
    }
}

fn layout_writer_provider_kind(source: &VortexIngestSourceData) -> &'static str {
    if source.columnar_source_preserved
        && (layout_uses_streaming_columnar_source(source) || source.record_batch_count > 0)
    {
        "vortex_array_kernel"
    } else {
        "shardloom_kernel"
    }
}

fn layout_writer_provider_surface(source: &VortexIngestSourceData) -> &'static str {
    if source.columnar_source_preserved && layout_uses_streaming_columnar_source(source) {
        "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator;VortexSession::write_options().write(ArrayStream)"
    } else if source.columnar_source_preserved && source.record_batch_count > 0 {
        "ArrayRef::from_arrow(RecordBatch);VortexSession::write_options().write(ArrayStream)"
    } else if source.columnar_source_preserved {
        "shardloom_empty_columnar_struct_builder;VortexSession::write_options().write(ArrayStream)"
    } else {
        "shardloom_scalar_rows_to_vortex_struct;VortexSession::write_options().write(ArrayStream)"
    }
}

fn layout_uses_streaming_columnar_source(source: &VortexIngestSourceData) -> bool {
    // Product streaming writers reserve native memory even for empty inputs. They
    // therefore convert a schema-bearing empty Arrow batch through the same provider.
    matches!(
        source.materialization_layout,
        "streaming_arrow_record_batch_columnar_source_state"
            | "typed_text_rows_to_streaming_arrow_record_batch_source_state"
            | "schema_declared_text_to_streaming_arrow_record_batch_source_state"
            | "whole_json_typed_columns_with_batched_writer"
    ) || source
        .source_stream_policy
        .contains("record_batch_stream_batch_size")
}

fn layout_verification_depth(
    certification_level: shardloom_vortex::VortexIngestCertificationLevel,
) -> &'static str {
    match certification_level {
        shardloom_vortex::VortexIngestCertificationLevel::IngestMinimal => {
            "write_digest_only_no_reopen"
        }
        shardloom_vortex::VortexIngestCertificationLevel::IngestCertified => {
            "writer_and_reopen_metadata_row_count"
        }
        shardloom_vortex::VortexIngestCertificationLevel::IngestFullReplay => {
            "blocked_until_downstream_replay"
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn capillary_preparation_report(
    source: &VortexIngestSourceData,
    vortex_report: &shardloom_vortex::VortexPreparedStateWriteReport,
    certification_level: shardloom_vortex::VortexIngestCertificationLevel,
    source_state_id: &str,
    source_state_digest: &str,
    prepared_state_id: &str,
    prepared_state_digest: &str,
    memory_budget_bytes: u64,
    max_parallelism: usize,
) -> Result<shardloom_vortex::VortexCapillaryPreparationReport, ShardLoomError> {
    let native_io_certificate_status =
        if vortex_report.preparation_spine.native_io_certificate_status
            == "certified_local_vortex_preparation_spine"
        {
            "certified"
        } else {
            "missing"
        };
    let prepared_artifact_segment_refs = prepared_artifact_segment_refs_for(
        prepared_state_id,
        vortex_report.row_count,
        &vortex_report.artifact_digest,
    );
    let correctness_digest = fnv64_digest(&format!(
        "vortex_capillary_preparation|{}|{}|{}|{}|{}",
        source_state_digest,
        prepared_state_digest,
        vortex_report.writer_row_count,
        vortex_report.reopen_row_count,
        vortex_report.reopen_verification_status
    ));
    shardloom_vortex::evaluate_vortex_capillary_preparation(
        shardloom_vortex::VortexCapillaryPreparationInput {
            source_state_id: source_state_id.to_string(),
            source_state_digest: source_state_digest.to_string(),
            prepared_state_id: prepared_state_id.to_string(),
            prepared_state_digest: prepared_state_digest.to_string(),
            source_surface: vortex_report.preparation_spine.source_surface.clone(),
            sink_surface: vortex_report.preparation_spine.sink_surface.clone(),
            format_family: source.source_format.as_str().to_string(),
            operation_class: "vortex_ingest_prepare_once".to_string(),
            certification_depth: certification_level.as_str().to_string(),
            row_count: vortex_report.row_count,
            source_byte_count: source.source_bytes,
            column_count: source.header.len(),
            source_split_refs: source.preparation_spine_source_split_refs(source_state_id),
            source_byte_range_refs: source
                .preparation_spine_source_byte_range_refs(source_state_id),
            source_row_range_refs: source.preparation_spine_source_row_range_refs(source_state_id),
            projection_mask: source.materialized_columns_field(),
            filter_mask_status: "none".to_string(),
            prepared_artifact_ref: vortex_report.target_path.display().to_string(),
            prepared_artifact_digest: vortex_report.artifact_digest.clone(),
            prepared_artifact_segment_refs,
            writer_sink_refs: vortex_report.target_path.display().to_string(),
            materialization_boundary_status: vortex_report
                .preparation_spine
                .materialization_boundary_status
                .clone(),
            decode_boundary_status: vortex_report
                .preparation_spine
                .decode_boundary_status
                .clone(),
            native_io_certificate_status: native_io_certificate_status.to_string(),
            native_io_certificate_refs: vortex_report
                .preparation_spine
                .native_io_certificate_refs
                .clone(),
            correctness_digest,
            memory_budget_bytes,
            max_parallelism,
            result_sink_requested: false,
            result_sink_replay_verified: false,
            capillary_claim_evidence_requested: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
    )
}

fn capillary_prewrite_input(
    source: &VortexIngestSourceData,
    target_path: &Path,
    certification_level: shardloom_vortex::VortexIngestCertificationLevel,
    source_state_id: &str,
    source_state_digest: &str,
    memory_budget_bytes: u64,
    max_parallelism: usize,
) -> shardloom_vortex::VortexCapillaryPreparationInput {
    let prewrite_prepared_state_digest = fnv64_digest(&format!(
        "vortex_capillary_prewrite|{}|{}|{}|{}|{}",
        source_state_digest,
        target_path.display(),
        source.materialized_columns_field(),
        source.row_count,
        source.source_bytes
    ));
    let prewrite_prepared_state_id = format!(
        "vortex-prepared-state-prewrite-{}",
        prewrite_prepared_state_digest.replace(':', "-")
    );
    let source_split_refs = source.preparation_spine_source_split_refs(source_state_id);
    let source_byte_range_refs = source.preparation_spine_source_byte_range_refs(source_state_id);
    let source_row_range_refs = source.preparation_spine_source_row_range_refs(source_state_id);
    let correctness_digest = fnv64_digest(&format!(
        "vortex_capillary_prewrite_correctness|{}|{}|{}|{}",
        source_state_digest,
        prewrite_prepared_state_digest,
        source.row_count,
        source.materialized_columns_field()
    ));
    shardloom_vortex::VortexCapillaryPreparationInput {
        source_state_id: source_state_id.to_string(),
        source_state_digest: source_state_digest.to_string(),
        prepared_state_id: prewrite_prepared_state_id.clone(),
        prepared_state_digest: prewrite_prepared_state_digest.clone(),
        source_surface: if source.columnar_source_preserved {
            "local_columnar_source_state_arrow_record_batches"
        } else {
            "local_text_source_state_scalar_rows"
        }
        .to_string(),
        sink_surface: "workspace_safe_local_vortex_file_sink".to_string(),
        format_family: source.source_format.as_str().to_string(),
        operation_class: "vortex_ingest_prepare_once".to_string(),
        certification_depth: certification_level.as_str().to_string(),
        row_count: source.row_count as u64,
        source_byte_count: source.source_bytes,
        column_count: source.header.len(),
        source_split_refs,
        source_byte_range_refs,
        source_row_range_refs,
        projection_mask: source.materialized_columns_field(),
        filter_mask_status: "none".to_string(),
        prepared_artifact_ref: target_path.display().to_string(),
        prepared_artifact_digest: format!(
            "prewrite_pending_until_vortex_writer_certificate:{prewrite_prepared_state_digest}"
        ),
        prepared_artifact_segment_refs: format!(
            "{prewrite_prepared_state_id}:prewrite_rows={}:source_digest={}",
            source.row_count, source.source_digest
        ),
        writer_sink_refs: target_path.display().to_string(),
        materialization_boundary_status: if source.columnar_source_preserved {
            "columnar_source_state_preserved_before_vortex_array_provider"
        } else {
            "materialized_scalar_rows_before_vortex_write"
        }
        .to_string(),
        decode_boundary_status: if source.columnar_source_preserved {
            "structured_reader_to_arrow_record_batches"
        } else {
            "text_source_decoded_by_shardloom_adapter"
        }
        .to_string(),
        native_io_certificate_status: "certified".to_string(),
        native_io_certificate_refs:
            "prewrite_source_shape_local_sink_policy_final_certificate_required_after_write"
                .to_string(),
        correctness_digest,
        memory_budget_bytes,
        max_parallelism,
        result_sink_requested: false,
        result_sink_replay_verified: false,
        capillary_claim_evidence_requested: false,
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

fn prepared_artifact_segment_refs_for(
    prepared_state_id: &str,
    row_count: u64,
    artifact_digest: &str,
) -> String {
    format!("{prepared_state_id}:rows=0..{row_count}:digest={artifact_digest}")
}

fn copy_budget_report(
    source: &VortexIngestSourceData,
    vortex_report: &shardloom_vortex::VortexPreparedStateWriteReport,
    source_state_id: &str,
    source_state_digest: &str,
    prepared_state_id: &str,
    prepared_state_digest: &str,
) -> shardloom_vortex::VortexCopyBudgetReport {
    let source_read_copy_bytes = source.source_bytes;
    let writer_buffer_bytes = vortex_report.bytes_written;
    let total_measured_copy_bytes = source_read_copy_bytes.saturating_add(writer_buffer_bytes);
    let buffer_reuse_status = copy_buffer_reuse_status(source, vortex_report);
    shardloom_vortex::evaluate_vortex_copy_budget(shardloom_vortex::VortexCopyBudgetInput {
        source_state_id: source_state_id.to_string(),
        source_state_digest: source_state_digest.to_string(),
        prepared_state_id: prepared_state_id.to_string(),
        prepared_state_digest: prepared_state_digest.to_string(),
        source_format: source.source_format.as_str().to_string(),
        row_count: vortex_report.row_count,
        source_byte_count: source.source_bytes,
        column_count: source.header.len(),
        allocation_scope: "vortex_ingest_local_prepare_once".to_string(),
        copy_scope:
            "source_read,parse_normalization,columnar_handoff,vortex_array_build,writer,reopen,evidence"
                .to_string(),
        measurement_status: "reported_with_not_measured_segments".to_string(),
        source_read_copy_bytes: source_read_copy_bytes.to_string(),
        parse_normalization_copy_bytes: copy_parse_normalization_bytes(source),
        columnar_handoff_copy_bytes: copy_columnar_handoff_bytes(source),
        vortex_array_build_copy_bytes: copy_vortex_array_build_bytes(vortex_report),
        writer_buffer_bytes: writer_buffer_bytes.to_string(),
        reopen_verify_copy_bytes: copy_reopen_verify_bytes(vortex_report),
        evidence_render_copy_bytes: "not_measured".to_string(),
        total_measured_copy_bytes: total_measured_copy_bytes.to_string(),
        buffer_family: copy_buffer_family(source, vortex_report).to_string(),
        ownership_policy: "owned_buffers_no_borrowed_lifetime_reuse".to_string(),
        writer_buffering_status: "writer_bytes_reported_from_local_vortex_artifact".to_string(),
        buffer_reuse_status: buffer_reuse_status.to_string(),
        buffer_reuse_count: copy_buffer_reuse_count(buffer_reuse_status),
        unsafe_lifetime_shortcut_status: "blocked_no_unsafe_lifetime_shortcuts".to_string(),
        correctness_parity_refs:
            "source_state_digest,prepared_state_digest,writer_row_count,reopen_row_count"
                .to_string(),
        materialization_boundary_status: vortex_report
            .preparation_spine
            .materialization_boundary_status
            .clone(),
        decode_boundary_status: vortex_report
            .preparation_spine
            .decode_boundary_status
            .clone(),
        unsupported_diagnostic_code: "none".to_string(),
        fallback_attempted: false,
        external_engine_invoked: false,
    })
}

fn copy_buffer_reuse_status(
    source: &VortexIngestSourceData,
    vortex_report: &shardloom_vortex::VortexPreparedStateWriteReport,
) -> &'static str {
    let writer_count_verified = vortex_report.writer_row_count == vortex_report.row_count;
    let reopen_verified = vortex_report_reopen_row_count_verified(vortex_report);
    let minimal_digest_verified = vortex_report.reopen_verification_status
        == "not_performed_ingest_minimal"
        && vortex_report.artifact_digest.starts_with("sha256:");
    if !writer_count_verified || !(reopen_verified || minimal_digest_verified) {
        return "blocked_until_correctness_parity";
    }
    if source.columnar_source_preserved {
        "admitted_columnar_source_state_reuse_with_digest_and_row_count_proof"
    } else if source.source_read_buffer_carry_status() == "read_once_buffer_carried_to_text_parser"
    {
        "admitted_read_once_source_buffer_carry_with_digest_and_row_count_proof"
    } else {
        "reported_no_buffer_reuse"
    }
}

fn copy_buffer_reuse_count(status: &str) -> u64 {
    u64::from(status.starts_with("admitted"))
}

fn copy_parse_normalization_bytes(source: &VortexIngestSourceData) -> String {
    if source.columnar_source_preserved {
        "not_measured_structured_reader".to_string()
    } else {
        "not_measured_scalar_row_parse".to_string()
    }
}

fn copy_columnar_handoff_bytes(source: &VortexIngestSourceData) -> String {
    if source.columnar_source_preserved {
        "not_measured_arrow_record_batch_handoff".to_string()
    } else {
        "not_applicable_scalar_rows".to_string()
    }
}

fn copy_vortex_array_build_bytes(
    vortex_report: &shardloom_vortex::VortexPreparedStateWriteReport,
) -> String {
    if vortex_report.manual_scalar_copy_avoided {
        "not_measured_vortex_record_batch_conversion".to_string()
    } else {
        "not_measured_scalar_to_vortex_build".to_string()
    }
}

fn copy_reopen_verify_bytes(
    vortex_report: &shardloom_vortex::VortexPreparedStateWriteReport,
) -> String {
    if vortex_report_writer_summary_row_count_verified(vortex_report) {
        "not_performed_layout_inventory_deferred".to_string()
    } else if vortex_report_reopen_metadata_row_count_verified(vortex_report) {
        "not_measured_reopen_metadata".to_string()
    } else if vortex_report.upstream_vortex_scan_called {
        "not_measured_reopen_scan".to_string()
    } else {
        "not_performed".to_string()
    }
}

fn copy_buffer_family(
    source: &VortexIngestSourceData,
    vortex_report: &shardloom_vortex::VortexPreparedStateWriteReport,
) -> &'static str {
    if vortex_report_writer_summary_row_count_verified(vortex_report)
        && source.columnar_source_preserved
    {
        "source_bytes,arrow_record_batches,vortex_writer_buffer,layout_inventory_deferred"
    } else if vortex_report_writer_summary_row_count_verified(vortex_report) {
        "source_bytes,scalar_rows,vortex_writer_buffer,layout_inventory_deferred"
    } else if vortex_report_reopen_metadata_row_count_verified(vortex_report)
        && source.columnar_source_preserved
    {
        "source_bytes,arrow_record_batches,vortex_writer_buffer,reopen_metadata"
    } else if vortex_report_reopen_metadata_row_count_verified(vortex_report) {
        "source_bytes,scalar_rows,vortex_writer_buffer,reopen_metadata"
    } else if source.columnar_source_preserved {
        "source_bytes,arrow_record_batches,vortex_writer_buffer,reopen_scan_buffer"
    } else {
        "source_bytes,scalar_rows,vortex_writer_buffer,reopen_scan_buffer"
    }
}

fn vortex_report_reopen_row_count_verified(
    vortex_report: &shardloom_vortex::VortexPreparedStateWriteReport,
) -> bool {
    matches!(
        vortex_report.reopen_verification_status.as_str(),
        "reopen_row_count_verified"
            | "reopen_metadata_row_count_verified"
            | "writer_summary_row_count_verified_layout_inventory_deferred"
    )
}

fn vortex_report_reopen_metadata_row_count_verified(
    vortex_report: &shardloom_vortex::VortexPreparedStateWriteReport,
) -> bool {
    vortex_report.reopen_verification_status == "reopen_metadata_row_count_verified"
}

fn vortex_report_writer_summary_row_count_verified(
    vortex_report: &shardloom_vortex::VortexPreparedStateWriteReport,
) -> bool {
    vortex_report.reopen_verification_status
        == "writer_summary_row_count_verified_layout_inventory_deferred"
}

fn run_scalar_vortex_prepare(
    request: VortexIngestRequest,
    source_adapter: LocalInputAdapterSelection,
    source_schema_hints: &[(String, LogicalDType)],
) -> Result<VortexIngestReport, ShardLoomError> {
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    if source_adapter.source_format == LocalSourceFormat::Json
        && !request.source_path.is_dir()
        && source_schema_hints.is_empty()
    {
        return whole_json_typed::prepare(request, source_adapter);
    }
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    if let Some(report) = try_run_schema_declared_text_vortex_prepare(
        request.clone(),
        source_adapter.clone(),
        source_schema_hints,
    )? {
        return Ok(report);
    }
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    if let Some(report) =
        try_run_inferred_text_vortex_prepare(request.clone(), source_adapter.clone())?
    {
        return Ok(report);
    }

    let prepare_start = Instant::now();
    let mut source = read_local_source_with_plan_and_adapter(
        &request.source_path,
        &LocalSourceReadPlan::full("full_vortex_ingest_source_state"),
        source_adapter,
        request.runtime_profile.read_limits(),
    )?;
    apply_vortex_ingest_schema_hints(&mut source, source_schema_hints)?;
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    {
        run_text_streaming_vortex_prepare(request, source, prepare_start)
    }
    #[cfg(not(all(feature = "vortex-write", feature = "universal-format-io")))]
    {
        run_scalar_rows_vortex_prepare(request, &source, prepare_start)
    }
}

#[cfg(not(all(feature = "vortex-write", feature = "universal-format-io")))]
fn run_scalar_rows_vortex_prepare(
    request: VortexIngestRequest,
    source: &CsvSourceData,
    prepare_start: Instant,
) -> Result<VortexIngestReport, ShardLoomError> {
    let source_evidence = VortexIngestSourceData::from_scalar_source(source.clone());
    let source_schema_digest = vortex_ingest_source_schema_digest(source);
    let source_state_id = source_state_id_for_source(&source_evidence);
    let source_state_digest =
        source_state_digest_for_source(&source_evidence, &source_schema_digest);
    let scout_ingress = scout_ingress_report(
        &source_evidence,
        &request.source_path,
        &source_state_id,
        &source_state_digest,
        &source_schema_digest,
    );
    let layout_write_advisor = layout_write_advisor_report(
        &source_evidence,
        &source_state_id,
        &source_state_digest,
        &source_schema_digest,
        request.certification_level,
        request.resources.max_parallelism(),
    );
    let rows = ordered_source_rows(&source.header, &source.rows)?;
    let prewrite_input = capillary_prewrite_input(
        &source_evidence,
        &request.target_path,
        request.certification_level,
        &source_state_id,
        &source_state_digest,
        request.resources.memory_bytes(),
        request.resources.max_parallelism(),
    );
    let vortex_request = shardloom_vortex::VortexPreparedStateWriteRequest::new(
        &request.target_path,
        source.header.clone(),
        rows,
        request.resources,
    )
    .column_dtypes(source.column_dtypes.clone())
    .allow_overwrite(request.allow_overwrite)
    .certification_level(request.certification_level)
    .layout_write_advisor(layout_write_advisor.clone())
    .capillary_prewrite_input(prewrite_input);
    let vortex_report = shardloom_vortex::write_flat_scalar_vortex_prepared_state(vortex_request)?;
    let layout_write_advisor =
        layout_write_advisor.with_runtime_decision(&vortex_report.layout_write_decision);
    let prepare_once_total_millis = prepare_start.elapsed().as_millis();

    let evidence_start = Instant::now();
    let prepared_state_digest = fnv64_digest(&format!(
        "{}|{}|{}|{}",
        source_state_digest,
        vortex_report.artifact_digest,
        vortex_report.column_family_summary(),
        vortex_report.row_count
    ));
    let prepared_state_id = format!(
        "vortex-prepared-state-{}",
        prepared_state_digest.replace(':', "-")
    );
    let capillary_preparation = capillary_preparation_report(
        &source_evidence,
        &vortex_report,
        request.certification_level,
        &source_state_id,
        &source_state_digest,
        &prepared_state_id,
        &prepared_state_digest,
        request.resources.memory_bytes(),
        request.resources.max_parallelism(),
    )?;
    let copy_budget = copy_budget_report(
        &source_evidence,
        &vortex_report,
        &source_state_id,
        &source_state_digest,
        &prepared_state_id,
        &prepared_state_digest,
    );
    let evidence_render_millis = evidence_start.elapsed().as_millis();

    Ok(VortexIngestReport {
        request,
        source: source_evidence,
        source_schema_digest,
        source_state_id,
        source_state_digest,
        prepared_state_id,
        prepared_state_digest,
        prepare_once_total_millis,
        prepared_olap_publication_millis: 0,
        evidence_render_millis,
        vortex_report,
        scout_ingress,
        layout_write_advisor,
        capillary_preparation,
        copy_budget,
        differential_preparation: None,
        prepared_state_reuse: None,
        prepared_olap_state: None,
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[allow(clippy::too_many_lines)]
fn try_run_schema_declared_text_vortex_prepare(
    request: VortexIngestRequest,
    source_adapter: LocalInputAdapterSelection,
    source_schema_hints: &[(String, LogicalDType)],
) -> Result<Option<VortexIngestReport>, ShardLoomError> {
    if request.source_path.is_dir()
        || !matches!(
            source_adapter.source_format,
            LocalSourceFormat::Csv | LocalSourceFormat::JsonLines
        )
    {
        return Ok(None);
    }

    let source_format = source_adapter.source_format;
    let read_limits = request.runtime_profile.read_limits();
    let prepare_start = Instant::now();
    let read_start = Instant::now();
    let scout = fingerprint_local_source_file_with_budget_report(
        &request.source_path,
        source_format.row_label(),
        read_limits.source_bytes,
        request.source_fingerprint_policy,
    )?;
    let read_millis = read_start.elapsed().as_millis();
    let source_to_columnar_start = Instant::now();
    let Some(contract) = schema_declared_text_stream_contract(
        &request.source_path,
        source_format,
        source_schema_hints,
        read_limits.input_rows,
    )?
    else {
        return Ok(None);
    };
    let context = format!(
        "{} schema-declared Universal Ingest stream",
        contract.source_format.row_label()
    );
    let source_schema_hints = text_stream_contract_schema_hints(&contract);
    let fields = contract
        .header
        .iter()
        .zip(contract.column_arrow_dtypes.iter())
        .map(|(column, dtype)| {
            let data_type = dtype.clone().ok_or_else(|| {
                ShardLoomError::InvalidOperation(format!(
                    "{context} column '{column}' has no Arrow dtype; no fallback execution was attempted"
                ))
            })?;
            Ok(Field::new(column, data_type, true))
        })
        .collect::<Result<Vec<_>, ShardLoomError>>()?;
    let schema = Arc::new(Schema::new(fields));
    let reader_config = TextRecordBatchReaderConfig {
        max_input_rows: read_limits.input_rows,
        batch_size: text_stream_record_batch_size(read_limits.input_rows),
    };
    let batch_reader = SchemaDeclaredTextRecordBatchReader::new(
        contract.source_format,
        Arc::clone(&schema),
        contract.header.clone(),
        contract.column_dtypes.clone(),
        contract.reader,
        reader_config,
        context,
    );
    let columnar_source = shardloom_vortex::FlatLocalColumnarStreamSource {
        header: contract.header.clone(),
        column_dtypes: contract.column_dtypes.clone(),
        column_arrow_dtypes: contract.column_arrow_dtypes.clone(),
        materialized_columns: contract.header.clone(),
        reader_projection_columns: contract.header.clone(),
        row_count_hint: None,
        record_batch_count_hint: None,
        source_stream_batch_size: reader_config.batch_size,
        source_stream_unit_count_hint: None,
        source_stream_unit_row_ranges: None,
        source_stream_unit_hint_kind: "schema_declared_text_record_batch_stream".to_string(),
        source_stream_policy: contract.source_stream_policy.clone(),
        source_dictionary_preservation_status:
            "schema_declared_text_typed_builders_preserve_declared_scalar_types".to_string(),
        ingest_executor_status: "lazy_schema_declared_text_record_batch_builder".to_string(),
        ingest_executor_kind: "schema_declared_text_to_arrow_record_batch_reader".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: None,
        source_identities: Vec::new(),
        #[cfg(feature = "vortex-write")]
        ingest_runtime: None,
        embedded_derived_build_micros: shardloom_vortex::new_embedded_derived_build_micros_counter(
        ),
        reader: Box::new(batch_reader),
    };
    let columnar_source =
        shardloom_vortex::universal_format_io::with_embedded_derived_columns_columnar_stream_source(
            columnar_source,
        );
    let columnar_source = shardloom_vortex::with_capillary_prefetch_columnar_stream_source(
        columnar_source,
        request.resources.max_parallelism(),
    )?;
    let source_to_columnar_millis = source_to_columnar_start.elapsed().as_millis();
    let mut prewrite_source = VortexIngestSourceData::from_columnar_stream_source(
        source_adapter,
        &columnar_source,
        scout,
        read_millis,
        source_to_columnar_millis,
    );
    prewrite_source.read_plan =
        LocalSourceReadPlan::full("schema_declared_text_streaming_vortex_ingest_source_state");
    prewrite_source.projection_pushdown_status =
        LocalSourceProjectionPushdownStatus::TextParserColumnPruning;
    prewrite_source.materialization_layout =
        "schema_declared_text_to_streaming_arrow_record_batch_source_state";
    prewrite_source.parse_normalization = "schema_declared_text_to_record_batch_stream";
    prewrite_source
        .source_dictionary_preservation_status
        .clone_from(&columnar_source.source_dictionary_preservation_status);

    let source_schema_digest = fnv64_digest(&vortex_ingest_schema_digest_from_parts(
        &prewrite_source.header,
        &source_schema_hints,
    ));
    let prewrite_source_state_id = source_state_id_for_source(&prewrite_source);
    let prewrite_source_state_digest =
        source_state_digest_for_source(&prewrite_source, &source_schema_digest);
    let layout_write_advisor = layout_write_advisor_report(
        &prewrite_source,
        &prewrite_source_state_id,
        &prewrite_source_state_digest,
        &source_schema_digest,
        request.certification_level,
        request.resources.max_parallelism(),
    );
    let vortex_request = shardloom_vortex::VortexPreparedStateColumnarStreamWriteRequest::new(
        &request.target_path,
        columnar_source,
        request.resources,
    )
    .shared_native_memory_pool(request.native_memory_pool()?)
    .allow_overwrite(request.allow_overwrite)
    .certification_level(request.certification_level)
    .layout_write_advisor(layout_write_advisor.clone())
    .capillary_prewrite_input(capillary_prewrite_input(
        &prewrite_source,
        &request.target_path,
        request.certification_level,
        &prewrite_source_state_id,
        &prewrite_source_state_digest,
        request.resources.memory_bytes(),
        request.resources.max_parallelism(),
    ));
    let mut vortex_request = vortex_request;
    vortex_request
        .prepared_source_binding
        .clone_from(&request.prepared_source_binding);
    let vortex_report =
        shardloom_vortex::write_flat_columnar_vortex_prepared_state_streaming(vortex_request)?;
    let source = prewrite_source.with_observed_streaming_write(
        vortex_report.row_count,
        vortex_report.array_build_record_batch_count,
    );
    let source_state_id = prewrite_source_state_id;
    let source_state_digest = prewrite_source_state_digest;
    let scout_ingress = scout_ingress_report(
        &source,
        &request.source_path,
        &source_state_id,
        &source_state_digest,
        &source_schema_digest,
    );
    let layout_write_advisor =
        layout_write_advisor.with_runtime_decision(&vortex_report.layout_write_decision);
    let prepare_once_total_millis = prepare_start.elapsed().as_millis();

    let evidence_start = Instant::now();
    let prepared_state_digest = fnv64_digest(&format!(
        "{}|{}|{}|{}",
        source_state_digest,
        vortex_report.artifact_digest,
        vortex_report.column_family_summary(),
        vortex_report.row_count
    ));
    let prepared_state_id = format!(
        "vortex-prepared-state-{}",
        prepared_state_digest.replace(':', "-")
    );
    let capillary_preparation = capillary_preparation_report(
        &source,
        &vortex_report,
        request.certification_level,
        &source_state_id,
        &source_state_digest,
        &prepared_state_id,
        &prepared_state_digest,
        request.resources.memory_bytes(),
        request.resources.max_parallelism(),
    )?;
    let copy_budget = copy_budget_report(
        &source,
        &vortex_report,
        &source_state_id,
        &source_state_digest,
        &prepared_state_id,
        &prepared_state_digest,
    );
    let evidence_render_millis = evidence_start.elapsed().as_millis();

    Ok(Some(VortexIngestReport {
        request,
        source,
        source_schema_digest,
        source_state_id,
        source_state_digest,
        prepared_state_id,
        prepared_state_digest,
        prepare_once_total_millis,
        prepared_olap_publication_millis: 0,
        evidence_render_millis,
        vortex_report,
        scout_ingress,
        layout_write_advisor,
        capillary_preparation,
        copy_budget,
        differential_preparation: None,
        prepared_state_reuse: None,
        prepared_olap_state: None,
    }))
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[allow(clippy::too_many_lines)]
fn try_run_inferred_text_vortex_prepare(
    request: VortexIngestRequest,
    source_adapter: LocalInputAdapterSelection,
) -> Result<Option<VortexIngestReport>, ShardLoomError> {
    if request.source_path.is_dir()
        || !matches!(
            source_adapter.source_format,
            LocalSourceFormat::Csv | LocalSourceFormat::JsonLines
        )
    {
        return Ok(None);
    }

    let source_format = source_adapter.source_format;
    let read_limits = request.runtime_profile.read_limits();
    let prepare_start = Instant::now();
    let read_start = Instant::now();
    let scout = fingerprint_local_source_file_with_budget_report(
        &request.source_path,
        source_format.row_label(),
        read_limits.source_bytes,
        request.source_fingerprint_policy,
    )?;
    let read_millis = read_start.elapsed().as_millis();
    let source_to_columnar_start = Instant::now();
    let Some(contract) =
        inferred_text_stream_contract(&request.source_path, source_format, read_limits.input_rows)?
    else {
        return Ok(None);
    };
    let context = format!(
        "{} inferred-schema Universal Ingest stream",
        contract.source_format.row_label()
    );
    let source_schema_hints = text_stream_contract_schema_hints(&contract);
    let fields = contract
        .header
        .iter()
        .zip(contract.column_arrow_dtypes.iter())
        .map(|(column, dtype)| {
            let data_type = dtype.clone().ok_or_else(|| {
                ShardLoomError::InvalidOperation(format!(
                    "{context} column '{column}' has no Arrow dtype; no fallback execution was attempted"
                ))
            })?;
            Ok(Field::new(column, data_type, true))
        })
        .collect::<Result<Vec<_>, ShardLoomError>>()?;
    let schema = Arc::new(Schema::new(fields));
    let reader_config = TextRecordBatchReaderConfig {
        max_input_rows: read_limits.input_rows,
        batch_size: text_stream_record_batch_size(read_limits.input_rows),
    };
    let batch_reader = SchemaDeclaredTextRecordBatchReader::new(
        contract.source_format,
        Arc::clone(&schema),
        contract.header.clone(),
        contract.column_dtypes.clone(),
        contract.reader,
        reader_config,
        context,
    );
    let columnar_source = shardloom_vortex::FlatLocalColumnarStreamSource {
        header: contract.header.clone(),
        column_dtypes: contract.column_dtypes.clone(),
        column_arrow_dtypes: contract.column_arrow_dtypes.clone(),
        materialized_columns: contract.header.clone(),
        reader_projection_columns: contract.header.clone(),
        row_count_hint: None,
        record_batch_count_hint: None,
        source_stream_batch_size: reader_config.batch_size,
        source_stream_unit_count_hint: None,
        source_stream_unit_row_ranges: None,
        source_stream_unit_hint_kind: "inferred_text_record_batch_stream".to_string(),
        source_stream_policy: contract.source_stream_policy.clone(),
        source_dictionary_preservation_status:
            "inferred_text_typed_builders_preserve_inferred_scalar_types".to_string(),
        ingest_executor_status: "lazy_inferred_text_record_batch_builder".to_string(),
        ingest_executor_kind: "inferred_text_to_arrow_record_batch_reader".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: None,
        source_identities: Vec::new(),
        #[cfg(feature = "vortex-write")]
        ingest_runtime: None,
        embedded_derived_build_micros: shardloom_vortex::new_embedded_derived_build_micros_counter(
        ),
        reader: Box::new(batch_reader),
    };
    let columnar_source =
        shardloom_vortex::universal_format_io::with_embedded_derived_columns_columnar_stream_source(
            columnar_source,
        );
    let columnar_source = shardloom_vortex::with_capillary_prefetch_columnar_stream_source(
        columnar_source,
        request.resources.max_parallelism(),
    )?;
    let source_to_columnar_millis = source_to_columnar_start.elapsed().as_millis();
    let mut prewrite_source = VortexIngestSourceData::from_columnar_stream_source(
        source_adapter,
        &columnar_source,
        scout,
        read_millis,
        source_to_columnar_millis,
    );
    prewrite_source.read_plan =
        LocalSourceReadPlan::full("inferred_text_streaming_vortex_ingest_source_state");
    prewrite_source.projection_pushdown_status =
        LocalSourceProjectionPushdownStatus::TextParserColumnPruning;
    prewrite_source.materialization_layout =
        "inferred_text_to_streaming_arrow_record_batch_source_state";
    prewrite_source.parse_normalization = "inferred_text_to_record_batch_stream";
    prewrite_source
        .source_dictionary_preservation_status
        .clone_from(&columnar_source.source_dictionary_preservation_status);

    let source_schema_digest = fnv64_digest(&vortex_ingest_schema_digest_from_parts(
        &prewrite_source.header,
        &source_schema_hints,
    ));
    let prewrite_source_state_id = source_state_id_for_source(&prewrite_source);
    let prewrite_source_state_digest =
        source_state_digest_for_source(&prewrite_source, &source_schema_digest);
    let layout_write_advisor = layout_write_advisor_report(
        &prewrite_source,
        &prewrite_source_state_id,
        &prewrite_source_state_digest,
        &source_schema_digest,
        request.certification_level,
        request.resources.max_parallelism(),
    );
    let vortex_request = shardloom_vortex::VortexPreparedStateColumnarStreamWriteRequest::new(
        &request.target_path,
        columnar_source,
        request.resources,
    )
    .shared_native_memory_pool(request.native_memory_pool()?)
    .allow_overwrite(request.allow_overwrite)
    .certification_level(request.certification_level)
    .layout_write_advisor(layout_write_advisor.clone())
    .capillary_prewrite_input(capillary_prewrite_input(
        &prewrite_source,
        &request.target_path,
        request.certification_level,
        &prewrite_source_state_id,
        &prewrite_source_state_digest,
        request.resources.memory_bytes(),
        request.resources.max_parallelism(),
    ));
    let mut vortex_request = vortex_request;
    vortex_request
        .prepared_source_binding
        .clone_from(&request.prepared_source_binding);
    let vortex_report =
        shardloom_vortex::write_flat_columnar_vortex_prepared_state_streaming(vortex_request)?;
    let source = prewrite_source.with_observed_streaming_write(
        vortex_report.row_count,
        vortex_report.array_build_record_batch_count,
    );
    let source_state_id = prewrite_source_state_id;
    let source_state_digest = prewrite_source_state_digest;
    let scout_ingress = scout_ingress_report(
        &source,
        &request.source_path,
        &source_state_id,
        &source_state_digest,
        &source_schema_digest,
    );
    let layout_write_advisor =
        layout_write_advisor.with_runtime_decision(&vortex_report.layout_write_decision);
    let prepare_once_total_millis = prepare_start.elapsed().as_millis();

    let evidence_start = Instant::now();
    let prepared_state_digest = fnv64_digest(&format!(
        "{}|{}|{}|{}",
        source_state_digest,
        vortex_report.artifact_digest,
        vortex_report.column_family_summary(),
        vortex_report.row_count
    ));
    let prepared_state_id = format!(
        "vortex-prepared-state-{}",
        prepared_state_digest.replace(':', "-")
    );
    let capillary_preparation = capillary_preparation_report(
        &source,
        &vortex_report,
        request.certification_level,
        &source_state_id,
        &source_state_digest,
        &prepared_state_id,
        &prepared_state_digest,
        request.resources.memory_bytes(),
        request.resources.max_parallelism(),
    )?;
    let copy_budget = copy_budget_report(
        &source,
        &vortex_report,
        &source_state_id,
        &source_state_digest,
        &prepared_state_id,
        &prepared_state_digest,
    );
    let evidence_render_millis = evidence_start.elapsed().as_millis();

    Ok(Some(VortexIngestReport {
        request,
        source,
        source_schema_digest,
        source_state_id,
        source_state_digest,
        prepared_state_id,
        prepared_state_digest,
        prepare_once_total_millis,
        prepared_olap_publication_millis: 0,
        evidence_render_millis,
        vortex_report,
        scout_ingress,
        layout_write_advisor,
        capillary_preparation,
        copy_budget,
        differential_preparation: None,
        prepared_state_reuse: None,
        prepared_olap_state: None,
    }))
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn vortex_ingest_schema_digest_from_parts(
    header: &[String],
    hints: &[(String, LogicalDType)],
) -> String {
    if hints.is_empty() {
        header.join(",")
    } else {
        hints
            .iter()
            .map(|(name, dtype)| format!("{name}:{}", dtype.as_str()))
            .collect::<Vec<_>>()
            .join("|")
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
fn run_text_streaming_vortex_prepare(
    request: VortexIngestRequest,
    source: CsvSourceData,
    prepare_start: Instant,
) -> Result<VortexIngestReport, ShardLoomError> {
    let source_schema_digest = vortex_ingest_source_schema_digest(&source);
    let scout = ColumnarSourceScoutEvidence {
        bytes: source.source_bytes,
        digest: source.source_digest.clone(),
        fingerprint_kind: "local_file_content_digest".to_string(),
        fingerprint_policy: "content_digest".to_string(),
        identity_source: "local_file_explicit_proof_digest".to_string(),
        content_fingerprint_requested: true,
        content_fingerprint_performed: true,
        metadata_scout_millis: source.source_metadata_scout_millis,
        byte_acquisition_millis: source.source_byte_acquisition_millis,
        full_body_millis: source.source_full_body_millis,
    };
    let read_millis = source.read_millis;
    let compatibility_parse_millis = source.parse_millis;
    let source_to_columnar_start = Instant::now();
    let rows = ordered_source_rows(&source.header, &source.rows)?;
    let columnar_source =
        shardloom_vortex::universal_format_io::stream_flat_text_rows_columnar_source(
            source.header.clone(),
            source.column_dtypes.clone(),
            source.column_arrow_dtypes.clone(),
            source.materialized_columns.clone(),
            source.reader_projection_columns.clone(),
            rows,
            text_stream_record_batch_size(request.runtime_profile.read_limits().input_rows),
            source.source_format.row_label(),
        )?;
    let columnar_source = shardloom_vortex::with_capillary_prefetch_columnar_stream_source(
        columnar_source,
        request.resources.max_parallelism(),
    )?;
    let source_to_columnar_millis = source_to_columnar_start.elapsed().as_millis();
    let mut prewrite_source = VortexIngestSourceData::from_columnar_stream_source(
        source.source_adapter.clone(),
        &columnar_source,
        scout,
        read_millis,
        source_to_columnar_millis,
    );
    prewrite_source.compatibility_parse_millis = compatibility_parse_millis;
    prewrite_source.read_plan = source.read_plan.clone();
    prewrite_source.projection_pushdown_status = source.projection_pushdown_status;
    prewrite_source.materialization_layout =
        "typed_text_rows_to_streaming_arrow_record_batch_source_state";
    prewrite_source.parse_normalization = "text_adapter_to_typed_record_batch_stream";
    prewrite_source
        .source_dictionary_preservation_status
        .clone_from(&columnar_source.source_dictionary_preservation_status);

    finish_text_streaming_vortex_prepare(
        request,
        prewrite_source,
        columnar_source,
        source_schema_digest,
        prepare_start,
    )
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[allow(clippy::too_many_lines)]
fn finish_text_streaming_vortex_prepare(
    request: VortexIngestRequest,
    prewrite_source: VortexIngestSourceData,
    columnar_source: shardloom_vortex::FlatLocalColumnarStreamSource,
    source_schema_digest: String,
    prepare_start: Instant,
) -> Result<VortexIngestReport, ShardLoomError> {
    let prewrite_source_state_id = source_state_id_for_source(&prewrite_source);
    let prewrite_source_state_digest =
        source_state_digest_for_source(&prewrite_source, &source_schema_digest);
    let layout_write_advisor = layout_write_advisor_report(
        &prewrite_source,
        &prewrite_source_state_id,
        &prewrite_source_state_digest,
        &source_schema_digest,
        request.certification_level,
        request.resources.max_parallelism(),
    );
    let vortex_request = shardloom_vortex::VortexPreparedStateColumnarStreamWriteRequest::new(
        &request.target_path,
        columnar_source,
        request.resources,
    )
    .shared_native_memory_pool(request.native_memory_pool()?)
    .allow_overwrite(request.allow_overwrite)
    .certification_level(request.certification_level)
    .layout_write_advisor(layout_write_advisor.clone())
    .capillary_prewrite_input(capillary_prewrite_input(
        &prewrite_source,
        &request.target_path,
        request.certification_level,
        &prewrite_source_state_id,
        &prewrite_source_state_digest,
        request.resources.memory_bytes(),
        request.resources.max_parallelism(),
    ));
    let mut vortex_request = vortex_request;
    vortex_request
        .prepared_source_binding
        .clone_from(&request.prepared_source_binding);
    let vortex_report =
        shardloom_vortex::write_flat_columnar_vortex_prepared_state_streaming(vortex_request)?;
    let source = prewrite_source.with_observed_streaming_write(
        vortex_report.row_count,
        vortex_report.array_build_record_batch_count,
    );
    let source_state_id = prewrite_source_state_id;
    let source_state_digest = prewrite_source_state_digest;
    let scout_ingress = scout_ingress_report(
        &source,
        &request.source_path,
        &source_state_id,
        &source_state_digest,
        &source_schema_digest,
    );
    let layout_write_advisor =
        layout_write_advisor.with_runtime_decision(&vortex_report.layout_write_decision);
    let prepare_once_total_millis = prepare_start.elapsed().as_millis();

    let evidence_start = Instant::now();
    let prepared_state_digest = fnv64_digest(&format!(
        "{}|{}|{}|{}",
        source_state_digest,
        vortex_report.artifact_digest,
        vortex_report.column_family_summary(),
        vortex_report.row_count
    ));
    let prepared_state_id = format!(
        "vortex-prepared-state-{}",
        prepared_state_digest.replace(':', "-")
    );
    let capillary_preparation = capillary_preparation_report(
        &source,
        &vortex_report,
        request.certification_level,
        &source_state_id,
        &source_state_digest,
        &prepared_state_id,
        &prepared_state_digest,
        request.resources.memory_bytes(),
        request.resources.max_parallelism(),
    )?;
    let copy_budget = copy_budget_report(
        &source,
        &vortex_report,
        &source_state_id,
        &source_state_digest,
        &prepared_state_id,
        &prepared_state_digest,
    );
    let evidence_render_millis = evidence_start.elapsed().as_millis();

    Ok(VortexIngestReport {
        request,
        source,
        source_schema_digest,
        source_state_id,
        source_state_digest,
        prepared_state_id,
        prepared_state_digest,
        prepare_once_total_millis,
        prepared_olap_publication_millis: 0,
        evidence_render_millis,
        vortex_report,
        scout_ingress,
        layout_write_advisor,
        capillary_preparation,
        copy_budget,
        differential_preparation: None,
        prepared_state_reuse: None,
        prepared_olap_state: None,
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[allow(clippy::too_many_lines)]
fn run_columnar_vortex_prepare(
    request: VortexIngestRequest,
    source_adapter: LocalInputAdapterSelection,
    source_schema_hints: &[(String, LogicalDType)],
) -> Result<VortexIngestReport, ShardLoomError> {
    if !source_schema_hints.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "vortex_ingest --schema applies to local text SourceState adapters; columnar Parquet/Arrow IPC/Avro/ORC sources use their embedded schema evidence; no fallback execution was attempted"
                .to_string(),
        ));
    }
    reject_remote_source_path(&request.source_path)?;
    let source_format = source_adapter.source_format;
    let read_limits = request.runtime_profile.read_limits();
    let prepare_start = Instant::now();
    let read_start = Instant::now();
    let scout = if request.source_path.is_dir() {
        let partitions = scout_local_source_partition_files_with_budget(
            &request.source_path,
            source_format,
            read_limits.source_bytes,
            request.source_fingerprint_policy,
        )?;
        partitions.evidence
    } else {
        fingerprint_local_source_file_with_budget_report(
            &request.source_path,
            source_format.row_label(),
            read_limits.source_bytes,
            request.source_fingerprint_policy,
        )?
    };
    let read_millis = read_start.elapsed().as_millis();
    let source_to_columnar_start = Instant::now();
    let max_rows = read_limits.input_rows.unwrap_or(usize::MAX);
    let columnar_source = stream_columnar_vortex_ingest_source(
        source_format,
        &request.source_path,
        max_rows,
        read_limits.source_bytes,
        request.resources.max_parallelism(),
        request.source_fingerprint_policy,
        request.resources.memory_bytes()
            / 8
            / u64::try_from(shardloom_exec::compute_pool::bounded_cpu_parallelism(
                request.resources.max_parallelism(),
            ))
            .unwrap_or(u64::MAX),
    )?;
    let source_to_columnar_millis = source_to_columnar_start.elapsed().as_millis();
    // Embedded field names are already validated by the shared columnar source
    // reader. SQL output may contain qualified names such as `q.id`.
    let prewrite_source = VortexIngestSourceData::from_columnar_stream_source(
        source_adapter,
        &columnar_source,
        scout,
        read_millis,
        source_to_columnar_millis,
    );
    let source_schema_digest = fnv64_digest(&prewrite_source.header.join(","));
    let prewrite_source_state_id = source_state_id_for_source(&prewrite_source);
    let prewrite_source_state_digest =
        source_state_digest_for_source(&prewrite_source, &source_schema_digest);
    let layout_write_advisor = layout_write_advisor_report(
        &prewrite_source,
        &prewrite_source_state_id,
        &prewrite_source_state_digest,
        &source_schema_digest,
        request.certification_level,
        request.resources.max_parallelism(),
    );
    let vortex_request = shardloom_vortex::VortexPreparedStateColumnarStreamWriteRequest::new(
        &request.target_path,
        columnar_source,
        request.resources,
    )
    .shared_native_memory_pool(request.native_memory_pool()?)
    .allow_overwrite(request.allow_overwrite)
    .certification_level(request.certification_level)
    .layout_write_advisor(layout_write_advisor.clone())
    .capillary_prewrite_input(capillary_prewrite_input(
        &prewrite_source,
        &request.target_path,
        request.certification_level,
        &prewrite_source_state_id,
        &prewrite_source_state_digest,
        request.resources.memory_bytes(),
        request.resources.max_parallelism(),
    ));
    let mut vortex_request = vortex_request;
    vortex_request
        .prepared_source_binding
        .clone_from(&request.prepared_source_binding);
    let vortex_report =
        shardloom_vortex::write_flat_columnar_vortex_prepared_state_streaming(vortex_request)?;
    let source = prewrite_source.with_observed_streaming_write(
        vortex_report.row_count,
        vortex_report.array_build_record_batch_count,
    );
    let source_state_id = prewrite_source_state_id;
    let source_state_digest = prewrite_source_state_digest;
    let scout_ingress = scout_ingress_report(
        &source,
        &request.source_path,
        &source_state_id,
        &source_state_digest,
        &source_schema_digest,
    );
    let layout_write_advisor =
        layout_write_advisor.with_runtime_decision(&vortex_report.layout_write_decision);
    let prepare_once_total_millis = prepare_start.elapsed().as_millis();

    let evidence_start = Instant::now();
    let prepared_state_digest = fnv64_digest(&format!(
        "{}|{}|{}|{}",
        source_state_digest,
        vortex_report.artifact_digest,
        vortex_report.column_family_summary(),
        vortex_report.row_count
    ));
    let prepared_state_id = format!(
        "vortex-prepared-state-{}",
        prepared_state_digest.replace(':', "-")
    );
    let capillary_preparation = capillary_preparation_report(
        &source,
        &vortex_report,
        request.certification_level,
        &source_state_id,
        &source_state_digest,
        &prepared_state_id,
        &prepared_state_digest,
        request.resources.memory_bytes(),
        request.resources.max_parallelism(),
    )?;
    let copy_budget = copy_budget_report(
        &source,
        &vortex_report,
        &source_state_id,
        &source_state_digest,
        &prepared_state_id,
        &prepared_state_digest,
    );
    let evidence_render_millis = evidence_start.elapsed().as_millis();

    Ok(VortexIngestReport {
        request,
        source,
        source_schema_digest,
        source_state_id,
        source_state_digest,
        prepared_state_id,
        prepared_state_digest,
        prepare_once_total_millis,
        prepared_olap_publication_millis: 0,
        evidence_render_millis,
        vortex_report,
        scout_ingress,
        layout_write_advisor,
        capillary_preparation,
        copy_budget,
        differential_preparation: None,
        prepared_state_reuse: None,
        prepared_olap_state: None,
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn stream_columnar_vortex_ingest_source(
    source_format: LocalSourceFormat,
    path: &Path,
    max_rows: usize,
    source_byte_budget: Option<u64>,
    max_parallelism: usize,
    source_fingerprint_policy: SourceFingerprintPolicy,
    batch_budget_bytes: u64,
) -> Result<shardloom_vortex::FlatLocalColumnarStreamSource, ShardLoomError> {
    if path.is_dir() {
        return stream_columnar_vortex_ingest_partition_source(
            source_format,
            path,
            max_rows,
            source_byte_budget,
            max_parallelism,
            source_fingerprint_policy,
            batch_budget_bytes,
        );
    }
    let source = stream_columnar_vortex_ingest_file_source(
        source_format,
        path,
        max_rows,
        max_parallelism,
        batch_budget_bytes,
    )?;
    let source = with_layout_advised_embedded_derived_columns_columnar_stream_source(source);
    shardloom_vortex::with_capillary_prefetch_columnar_stream_source(source, max_parallelism)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn with_layout_advised_embedded_derived_columns_columnar_stream_source(
    source: shardloom_vortex::FlatLocalColumnarStreamSource,
) -> shardloom_vortex::FlatLocalColumnarStreamSource {
    if columnar_stream_source_prefers_full_embedded_text_metadata(&source) {
        shardloom_vortex::universal_format_io::with_embedded_derived_columns_columnar_stream_source(
            source,
        )
    } else if columnar_stream_source_prefers_lean_source_native_embedded_metadata(&source) {
        shardloom_vortex::with_source_native_lean_runtime_embedded_derived_columns_columnar_stream_source(
            source,
        )
    } else {
        shardloom_vortex::universal_format_io::with_source_native_embedded_derived_columns_columnar_stream_source(
            source,
        )
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_stream_source_prefers_lean_source_native_embedded_metadata(
    source: &shardloom_vortex::FlatLocalColumnarStreamSource,
) -> bool {
    let medium_or_larger = source
        .row_count_hint
        .is_some_and(|rows| rows >= LOCAL_OLAP_MEDIUM_SOURCE_ROW_THRESHOLD)
        || source.source_stream_batch_size
            >= shardloom_vortex::universal_format_io::PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS;
    let source_native_lean_text_metadata_available =
        source.source_dictionary_preservation_status.contains(
            "parquet_arrow_reader_requested_dictionary_preservation_for_string_derived_columns",
        ) || source.source_dictionary_preservation_status.contains(
            "parquet_arrow_reader_uses_plain_utf8_for_large_olap_text_zstd_artifact_size_guard",
        ) || source.source_dictionary_preservation_status.contains(
            "parquet_arrow_reader_uses_utf8_view_for_large_olap_text_zstd_artifact_size_guard",
        );
    medium_or_larger && source_native_lean_text_metadata_available
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_stream_source_prefers_full_embedded_text_metadata(
    source: &shardloom_vortex::FlatLocalColumnarStreamSource,
) -> bool {
    let text_adapter_without_source_dictionary = source
        .source_dictionary_preservation_status
        .contains("typed_builders")
        || source
            .source_dictionary_preservation_status
            .contains("no_source_dictionary");
    if !text_adapter_without_source_dictionary {
        return false;
    }
    let medium_or_larger = source
        .row_count_hint
        .is_some_and(|rows| rows >= LOCAL_OLAP_MEDIUM_SOURCE_ROW_THRESHOLD)
        || source.source_stream_batch_size
            >= shardloom_vortex::universal_format_io::PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS;
    medium_or_larger
        && (columnar_stream_source_has_missing_source_native_text_metadata(source)
            || columnar_stream_source_has_missing_source_native_time_metadata(source))
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_stream_source_has_missing_source_native_text_metadata(
    source: &shardloom_vortex::FlatLocalColumnarStreamSource,
) -> bool {
    source.header.iter().enumerate().any(|(index, column)| {
        layout_text_domain_column_name(column)
            && !source
                .column_arrow_dtypes
                .get(index)
                .and_then(Option::as_ref)
                .is_some_and(is_dictionary_utf8_arrow_dtype)
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_stream_source_has_missing_source_native_time_metadata(
    source: &shardloom_vortex::FlatLocalColumnarStreamSource,
) -> bool {
    source.header.iter().enumerate().any(|(index, column)| {
        layout_time_bucket_column_name(column)
            && !source
                .column_arrow_dtypes
                .get(index)
                .and_then(Option::as_ref)
                .is_some_and(is_source_native_extract_minute_arrow_dtype)
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn is_dictionary_utf8_arrow_dtype(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Dictionary(key, value)
            if is_arrow_dictionary_key_dtype(key.as_ref()) && is_utf8_arrow_dtype(value.as_ref())
    )
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn is_arrow_dictionary_key_dtype(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn is_utf8_arrow_dtype(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn is_source_native_extract_minute_arrow_dtype(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Timestamp(_, _)
    )
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn stream_columnar_vortex_ingest_file_source(
    source_format: LocalSourceFormat,
    path: &Path,
    max_rows: usize,
    max_parallelism: usize,
    batch_budget_bytes: u64,
) -> Result<shardloom_vortex::FlatLocalColumnarStreamSource, ShardLoomError> {
    match source_format {
        LocalSourceFormat::Parquet => {
            shardloom_vortex::universal_format_io::stream_flat_parquet_columnar_source_with_batch_budget(
                path,
                max_rows,
                max_parallelism,
                Some(batch_budget_bytes.max(1)),
            )
        }
        LocalSourceFormat::ArrowIpc => {
            shardloom_vortex::stream_flat_arrow_ipc_columnar_source(path, max_rows)
        }
        LocalSourceFormat::Avro => {
            shardloom_vortex::stream_flat_avro_columnar_source(path, max_rows)
        }
        LocalSourceFormat::Orc => shardloom_vortex::stream_flat_orc_columnar_source(path, max_rows),
        LocalSourceFormat::Csv | LocalSourceFormat::Json | LocalSourceFormat::JsonLines => {
            Err(ShardLoomError::InvalidOperation(format!(
                "local {} source does not have a columnar vortex_ingest SourceState route; no fallback execution was attempted",
                source_format.row_label()
            )))
        }
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
struct PartitionedColumnarStreamReader {
    schema: SchemaRef,
    readers: VecDeque<Box<dyn RecordBatchReader + Send>>,
    max_rows: usize,
    row_count: usize,
    source_label: &'static str,
    path: String,
    failed: bool,
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
impl Iterator for PartitionedColumnarStreamReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            let reader = self.readers.front_mut()?;
            match reader.next() {
                Some(Ok(batch)) => {
                    self.row_count =
                        if let Some(row_count) = self.row_count.checked_add(batch.num_rows()) {
                            row_count
                        } else {
                            self.failed = true;
                            return Some(Err(ArrowError::InvalidArgumentError(format!(
                                "local {} partition source '{}' row count overflowed usize",
                                self.source_label, self.path
                            ))));
                        };
                    if self.row_count > self.max_rows {
                        self.failed = true;
                        return Some(Err(ArrowError::InvalidArgumentError(format!(
                            "local {} partition source '{}' exceeds the configured local source row budget of {} across partition files",
                            self.source_label, self.path, self.max_rows
                        ))));
                    }
                    return Some(Ok(batch));
                }
                Some(Err(error)) => return Some(Err(error)),
                None => {
                    self.readers.pop_front();
                }
            }
        }
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
impl RecordBatchReader for PartitionedColumnarStreamReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[allow(clippy::too_many_lines)]
fn stream_columnar_vortex_ingest_partition_source(
    source_format: LocalSourceFormat,
    path: &Path,
    max_rows: usize,
    source_byte_budget: Option<u64>,
    max_parallelism: usize,
    source_fingerprint_policy: SourceFingerprintPolicy,
    batch_budget_bytes: u64,
) -> Result<shardloom_vortex::FlatLocalColumnarStreamSource, ShardLoomError> {
    let partition_files = scout_local_source_partition_files_with_budget(
        path,
        source_format,
        source_byte_budget,
        source_fingerprint_policy,
    )?;
    let Some(first_file) = partition_files.files.first() else {
        return Err(unsupported_sql_error(&format!(
            "local {} partition source {} did not contain any admitted partition files",
            source_format.row_label(),
            path.display()
        )));
    };

    let first_source = stream_columnar_vortex_ingest_file_source(
        source_format,
        first_file,
        max_rows,
        1,
        batch_budget_bytes,
    )?;
    let shardloom_vortex::FlatLocalColumnarStreamSource {
        header,
        column_dtypes,
        column_arrow_dtypes,
        materialized_columns,
        reader_projection_columns,
        row_count_hint,
        record_batch_count_hint,
        source_stream_batch_size,
        source_stream_unit_count_hint,
        source_stream_unit_row_ranges: _,
        source_stream_unit_hint_kind,
        source_stream_policy,
        source_dictionary_preservation_status,
        ingest_executor_unit_count_hint: _,
        ingest_executor_status: _,
        ingest_executor_kind: _,
        ingest_executor_requested_parallelism: _,
        ingest_executor_applied_parallelism: _,
        embedded_derived_build_micros,
        mut source_identities,
        ingest_runtime: _,
        reader,
    } = first_source;
    let schema = reader.schema();
    let mut readers = VecDeque::new();
    readers.push_back(reader);
    let mut combined_row_count_hint = row_count_hint;
    let mut combined_record_batch_count_hint = record_batch_count_hint;
    let mut combined_source_stream_unit_count_hint = source_stream_unit_count_hint;
    for file_path in partition_files.files.iter().skip(1) {
        let source = stream_columnar_vortex_ingest_file_source(
            source_format,
            file_path,
            max_rows,
            1,
            batch_budget_bytes,
        )?;
        validate_columnar_stream_partition_schema(
            &header,
            &column_dtypes,
            &column_arrow_dtypes,
            &materialized_columns,
            &reader_projection_columns,
            &source,
            file_path,
            source_format,
        )?;
        combined_row_count_hint = checked_sum_optional_usize(
            combined_row_count_hint,
            source.row_count_hint,
            "partition row count",
        )?;
        combined_record_batch_count_hint = checked_sum_optional_usize(
            combined_record_batch_count_hint,
            source.record_batch_count_hint,
            "partition RecordBatch count",
        )?;
        combined_source_stream_unit_count_hint = checked_sum_optional_usize(
            combined_source_stream_unit_count_hint,
            source.source_stream_unit_count_hint,
            "partition source stream unit count",
        )?;
        source_identities.extend(source.source_identities);
        readers.push_back(source.reader);
    }
    if let Some(row_count_hint) = combined_row_count_hint
        && row_count_hint > max_rows
    {
        return Err(unsupported_sql_error(&format!(
            "local {} partition source {} exceeds the configured local source row budget of {} across partition files",
            source_format.row_label(),
            path.display(),
            max_rows
        )));
    }
    let source = shardloom_vortex::FlatLocalColumnarStreamSource {
        header,
        column_dtypes,
        column_arrow_dtypes,
        materialized_columns,
        reader_projection_columns,
        row_count_hint: combined_row_count_hint,
        record_batch_count_hint: combined_record_batch_count_hint,
        source_stream_batch_size,
        source_stream_unit_count_hint: combined_source_stream_unit_count_hint,
        source_stream_unit_row_ranges: None,
        source_stream_unit_hint_kind: format!(
            "partition_directory_source_units;inner={source_stream_unit_hint_kind}"
        ),
        source_stream_policy: format!("partitioned_columnar_stream;inner={source_stream_policy}"),
        source_dictionary_preservation_status,
        ingest_executor_status: "serial_partition_pull_reader".to_string(),
        ingest_executor_kind: "partition_directory_record_batch_reader".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: combined_source_stream_unit_count_hint
            .or(combined_record_batch_count_hint)
            .or(Some(readers.len())),
        embedded_derived_build_micros,
        source_identities,
        // Partition readers were admitted at P1 and own no background drivers
        // or queued source tasks. The combined source receives the caller's
        // shared ingest runtime in the common wrapper below.
        ingest_runtime: None,
        reader: Box::new(PartitionedColumnarStreamReader {
            schema,
            readers,
            max_rows,
            row_count: 0,
            source_label: source_format.row_label(),
            path: path.display().to_string(),
            failed: false,
        }),
    };
    let source = with_layout_advised_embedded_derived_columns_columnar_stream_source(source);
    shardloom_vortex::with_capillary_prefetch_columnar_stream_source(source, max_parallelism)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[allow(clippy::too_many_arguments)]
fn validate_columnar_stream_partition_schema(
    expected_header: &[String],
    expected_column_dtypes: &[Option<LogicalDType>],
    expected_column_arrow_dtypes: &[Option<DataType>],
    expected_materialized_columns: &[String],
    expected_reader_projection_columns: &[String],
    candidate: &shardloom_vortex::FlatLocalColumnarStreamSource,
    path: &Path,
    source_format: LocalSourceFormat,
) -> Result<(), ShardLoomError> {
    if expected_header != candidate.header
        || expected_column_dtypes != candidate.column_dtypes
        || expected_column_arrow_dtypes != candidate.column_arrow_dtypes
        || expected_materialized_columns != candidate.materialized_columns
        || expected_reader_projection_columns != candidate.reader_projection_columns
    {
        return Err(unsupported_sql_error(&format!(
            "local {} partition file {} has a schema/projection mismatch with earlier partitions; no fallback execution was attempted",
            source_format.row_label(),
            path.display()
        )));
    }
    Ok(())
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn checked_sum_optional_usize(
    left: Option<usize>,
    right: Option<usize>,
    label: &str,
) -> Result<Option<usize>, ShardLoomError> {
    match (left, right) {
        (Some(left), Some(right)) => left.checked_add(right).map(Some).ok_or_else(|| {
            unsupported_sql_error(&format!(
                "local columnar {label} overflowed usize; no fallback execution was attempted"
            ))
        }),
        _ => Ok(None),
    }
}

#[allow(clippy::too_many_lines)]
impl VortexIngestReport {
    fn prepare_known_component_millis(&self) -> u128 {
        self.source
            .read_millis
            .saturating_add(self.vortex_report.write_micros.div_ceil(1000))
            .saturating_add(self.vortex_report.reopen_scan_micros.div_ceil(1000))
            .min(self.prepare_once_total_millis)
    }

    fn prepare_unattributed_millis(&self) -> u128 {
        self.prepare_once_total_millis
            .saturating_sub(self.prepare_known_component_millis())
    }

    fn prepare_attribution_status(&self) -> &'static str {
        if self.prepare_unattributed_millis() == 0 {
            "fully_attributed_by_source_read_write_wall_and_reopen_verify"
        } else {
            "residual_prepare_bucket_contains_source_hydration_batch_production_route_bookkeeping_scheduler_wait_writer_backpressure_or_timer_overlap"
        }
    }

    fn prepare_source_hydration_millis(&self) -> u128 {
        self.source.source_to_columnar_millis
    }

    fn prepare_nested_source_batch_production_millis(&self) -> u128 {
        self.vortex_report.stream_source_pull_micros.div_ceil(1000)
    }

    const fn prepare_route_bookkeeping_millis() -> u128 {
        0
    }

    const fn prepare_scheduler_wait_millis() -> u128 {
        0
    }

    fn prepare_evidence_emit_millis(&self) -> u128 {
        self.evidence_render_millis
    }

    fn prepare_writer_backpressure_or_timer_overlap_millis(&self) -> u128 {
        self.prepare_unattributed_millis()
            .saturating_sub(self.prepare_source_hydration_millis())
            .saturating_sub(Self::prepare_route_bookkeeping_millis())
            .saturating_sub(Self::prepare_scheduler_wait_millis())
            .saturating_sub(self.prepare_evidence_emit_millis())
    }

    fn fields(&self) -> Vec<(String, String)> {
        let certified_reopen = self.request.certification_level
            == shardloom_vortex::VortexIngestCertificationLevel::IngestCertified;
        let certification_status = if certified_reopen {
            self.request.runtime_profile.certification_status()
        } else {
            "minimal_ingest_evidence_reported"
        };
        let certification_blocker_id = if certified_reopen {
            self.request.runtime_profile.certification_blocker_id()
        } else {
            "not_claim_grade_ingest_minimal_no_reopen_or_replay"
        };
        let native_io_certificate_status = if certified_reopen {
            self.request.runtime_profile.certification_status()
        } else {
            "minimal_local_vortex_ingest_digest_only"
        };
        let source_backed_scan_evidence_status =
            if vortex_report_reopen_metadata_row_count_verified(&self.vortex_report) {
                "scoped_reopen_metadata_row_count"
            } else if certified_reopen {
                "scoped_reopen_row_count_scan"
            } else {
                "not_performed_ingest_minimal"
            };
        let source_backed_scan_provider_kind =
            if vortex_report_reopen_metadata_row_count_verified(&self.vortex_report) {
                "vortex_file_metadata"
            } else if certified_reopen {
                "vortex_scan"
            } else {
                "not_invoked"
            };
        let source_backed_scan_provider_surface =
            if vortex_report_reopen_metadata_row_count_verified(&self.vortex_report) {
                "VortexFile::row_count"
            } else if certified_reopen {
                "VortexFile::scan"
            } else {
                "not_invoked"
            };
        let claim_gate_status = if certified_reopen {
            self.request.runtime_profile.claim_gate_status()
        } else {
            "not_claim_grade"
        };
        let claim_gate_reason = if certified_reopen {
            self.request.runtime_profile.ingress_certification_level()
        } else {
            "ingest_minimal_records_artifact_digest_without_reopen_or_result_replay"
        };
        let mut fields = vec![
            (
                "schema_version".to_string(),
                VORTEX_PREPARE_SCHEMA_VERSION.to_string(),
            ),
            ("execution_mode".to_string(), "prepared_vortex".to_string()),
            (
                "selected_execution_mode".to_string(),
                "prepared_vortex".to_string(),
            ),
            ("engine_mode".to_string(), "batch".to_string()),
            ("runtime_execution".to_string(), "true".to_string()),
            (
                "support_status".to_string(),
                self.request.runtime_profile.support_status().to_string(),
            ),
            ("source_io_performed".to_string(), "true".to_string()),
            (
                "source_kind".to_string(),
                "local_non_vortex_file".to_string(),
            ),
            (
                "source_format".to_string(),
                self.source.source_format.as_str().to_string(),
            ),
            (
                "source_format_inferred".to_string(),
                self.source
                    .source_adapter
                    .source_format_inferred()
                    .to_string(),
            ),
            (
                "source_format_inference_kind".to_string(),
                self.source.source_adapter.inference_kind().to_string(),
            ),
            (
                "source_format_inference_extension".to_string(),
                self.source.source_adapter.source_extension_field(),
            ),
            (
                "source_format_inference_registry_route".to_string(),
                self.source
                    .source_adapter
                    .inference_registry_route()
                    .to_string(),
            ),
            (
                "source_adapter_id".to_string(),
                self.source.adapter_id().to_string(),
            ),
            (
                "source_adapter_registry_entry_id".to_string(),
                self.source.adapter_registry_entry_id().to_string(),
            ),
            (
                "source_adapter_status".to_string(),
                self.request
                    .runtime_profile
                    .source_adapter_status()
                    .to_string(),
            ),
            (
                "source_adapter_admitted_extensions".to_string(),
                self.source.adapter_admitted_extensions().to_string(),
            ),
            (
                "source_adapter_feature_gate".to_string(),
                self.source.adapter_feature_gate().to_string(),
            ),
            (
                "source_adapter_boundary".to_string(),
                self.source.adapter_boundary().to_string(),
            ),
            (
                "source_adapter_selection_reason".to_string(),
                self.source.source_adapter.selection_reason().to_string(),
            ),
            (
                "source_adapter_blocker_id".to_string(),
                self.request
                    .runtime_profile
                    .source_adapter_blocker_id()
                    .to_string(),
            ),
            (
                "local_workflow_input_row_cap".to_string(),
                self.request
                    .runtime_profile
                    .input_row_cap_label()
                    .to_string(),
            ),
            (
                "local_workflow_synthetic_input_row_cap_enabled".to_string(),
                self.request
                    .runtime_profile
                    .synthetic_input_row_cap_enabled()
                    .to_string(),
            ),
            (
                "local_workflow_synthetic_output_row_cap_enabled".to_string(),
                self.request
                    .runtime_profile
                    .synthetic_output_row_cap_enabled()
                    .to_string(),
            ),
            (
                "local_workflow_synthetic_source_byte_cap_enabled".to_string(),
                self.request
                    .runtime_profile
                    .synthetic_source_byte_cap_enabled()
                    .to_string(),
            ),
            (
                "local_workflow_synthetic_join_candidate_cap_enabled".to_string(),
                self.request
                    .runtime_profile
                    .synthetic_join_candidate_cap_enabled()
                    .to_string(),
            ),
            ("ingress_route".to_string(), "vortex_ingest".to_string()),
            (
                "ingress_route_label".to_string(),
                "Vortex ingest / prepare once route".to_string(),
            ),
            (
                "ingress_status".to_string(),
                self.request
                    .runtime_profile
                    .source_adapter_status()
                    .to_string(),
            ),
            (
                "ingress_certification_level".to_string(),
                self.vortex_report.certification_level.clone(),
            ),
            ("vortex_ingest_performed".to_string(), "true".to_string()),
            (
                "vortex_ingest_status".to_string(),
                "prepared_state_created".to_string(),
            ),
            (
                "vortex_ingest_blocker_id".to_string(),
                "none_local_vortex_prepare".to_string(),
            ),
            (
                "prepared_state_id".to_string(),
                self.prepared_state_id.clone(),
            ),
            (
                "prepared_state_digest".to_string(),
                self.prepared_state_digest.clone(),
            ),
            ("prepared_state_created".to_string(), "true".to_string()),
            ("prepared_state_reused".to_string(), "false".to_string()),
            (
                "prepared_state_reuse_allowed".to_string(),
                "false".to_string(),
            ),
            ("prepared_state_reuse_hit".to_string(), "false".to_string()),
            (
                "prepared_state_reuse_scope".to_string(),
                "single_vortex_artifact_no_sidecar".to_string(),
            ),
            (
                "prepared_state_reuse_manifest_path".to_string(),
                "not_applicable_single_vortex_artifact".to_string(),
            ),
            (
                "prepared_state_reuse_policy".to_string(),
                "single_vortex_artifact_no_sidecar.v1".to_string(),
            ),
            (
                "prepared_state_reuse_reason".to_string(),
                "public_prepare_rewrites_or_uses_explicit_vortex_artifact_without_sidecar"
                    .to_string(),
            ),
            (
                "prepared_state_reuse_manifest_digest".to_string(),
                "not_applicable_single_vortex_artifact".to_string(),
            ),
            (
                "prepared_state_reuse_manifest_digest_algorithm".to_string(),
                "not_applicable".to_string(),
            ),
            (
                "prepared_state_invalidation_reason".to_string(),
                "not_applicable_single_vortex_artifact".to_string(),
            ),
            (
                "invalidation_reason".to_string(),
                "not_applicable_single_vortex_artifact".to_string(),
            ),
            (
                "execution_route_label".to_string(),
                "Prepared Vortex route".to_string(),
            ),
            (
                "vortex_ingest_requested_max_parallelism".to_string(),
                self.request.resources.max_parallelism().to_string(),
            ),
            (
                "vortex_ingest_requested_memory_gb".to_string(),
                self.request.resources.whole_gib().map_or_else(|| "unavailable".into(), |value| value.to_string()),
            ),
            (
                "certification_policy".to_string(),
                format!(
                    "scoped_vortex_ingest_lifecycle_{}",
                    self.vortex_report.certification_level
                ),
            ),
            (
                "certification_status".to_string(),
                certification_status.to_string(),
            ),
            (
                "certification_blocker_id".to_string(),
                certification_blocker_id.to_string(),
            ),
            (
                "source_path".to_string(),
                self.request.source_path.display().to_string(),
            ),
            (
                "source_bytes".to_string(),
                self.source.source_bytes.to_string(),
            ),
            (
                "source_read_scout_schema_version".to_string(),
                "shardloom.local_source_read_scout.v1".to_string(),
            ),
            (
                "source_read_scout_status".to_string(),
                "source_read_scout_split_recorded".to_string(),
            ),
            (
                "source_read_scout_timing_split_status".to_string(),
                self.source
                    .source_read_scout_timing_split_status()
                    .to_string(),
            ),
            (
                "source_read_metadata_scout_millis".to_string(),
                self.source.source_metadata_scout_millis.to_string(),
            ),
            (
                "source_read_byte_acquisition_millis".to_string(),
                self.source.source_byte_acquisition_millis.to_string(),
            ),
            (
                "source_read_full_body_millis".to_string(),
                self.source.source_full_body_millis.to_string(),
            ),
            (
                "source_read_buffer_carry_status".to_string(),
                self.source.source_read_buffer_carry_status().to_string(),
            ),
            (
                "source_read_mmap_eligibility_status".to_string(),
                self.source
                    .source_read_mmap_eligibility_status()
                    .to_string(),
            ),
            (
                "source_read_many_small_file_batching_status".to_string(),
                if self.source.ingest_executor_status == "bounded_capillary_prefetch_active" {
                    "bounded_capillary_prefetch_batches"
                } else if self.source.preparation_spine_source_split_count() > 1 {
                    "partition_directory_sorted_batches"
                } else {
                    "not_applicable_single_file"
                }
                .to_string(),
            ),
            (
                "source_digest".to_string(),
                self.source.source_digest.clone(),
            ),
            (
                "source_fingerprint_kind".to_string(),
                self.source.source_fingerprint.kind.clone(),
            ),
            (
                "source_fingerprint_policy".to_string(),
                self.source.source_fingerprint.policy.clone(),
            ),
            (
                "source_fingerprint_identity_source".to_string(),
                self.source.source_fingerprint.identity_source.clone(),
            ),
            (
                "source_content_fingerprint_requested".to_string(),
                self.source.source_fingerprint.content_requested.to_string(),
            ),
            (
                "source_content_fingerprint_performed".to_string(),
                self.source.source_fingerprint.content_performed.to_string(),
            ),
            ("source_state_id".to_string(), self.source_state_id.clone()),
            (
                "source_state_digest".to_string(),
                self.source_state_digest.clone(),
            ),
            (
                "source_state_contract_schema_version".to_string(),
                LOCAL_SOURCE_STATE_SCHEMA_VERSION.to_string(),
            ),
            (
                "local_input_adapter_registry_version".to_string(),
                LOCAL_INPUT_ADAPTER_REGISTRY_VERSION.to_string(),
            ),
            (
                "source_state_read_plan".to_string(),
                self.source.read_plan.status().to_string(),
            ),
            (
                "source_state_read_plan_reason".to_string(),
                self.source.read_plan.reason.to_string(),
            ),
            (
                "source_state_requested_columns".to_string(),
                self.source.read_plan.requested_columns(),
            ),
            (
                "source_state_projection_pushdown_status".to_string(),
                self.source.projection_pushdown_status.as_str().to_string(),
            ),
            (
                "source_state_materialization_layout".to_string(),
                self.source.materialization_layout.to_string(),
            ),
            (
                "source_state_parse_normalization".to_string(),
                self.source.parse_normalization.to_string(),
            ),
            (
                "source_state_columnar_preserved".to_string(),
                self.source.columnar_source_preserved.to_string(),
            ),
            (
                "source_state_record_batch_count".to_string(),
                self.source.record_batch_count.to_string(),
            ),
            (
                "source_state_stream_batch_size".to_string(),
                self.source.source_stream_batch_size.to_string(),
            ),
            (
                "source_state_stream_unit_count_hint".to_string(),
                self.source.source_stream_unit_count_hint.map_or_else(
                    || "unknown".to_string(),
                    |unit_count| unit_count.to_string(),
                ),
            ),
            (
                "source_state_stream_unit_hint_kind".to_string(),
                self.source.source_stream_unit_hint_kind.clone(),
            ),
            (
                "source_state_stream_policy".to_string(),
                self.source.source_stream_policy.clone(),
            ),
            (
                "source_state_stream_unit_interface".to_string(),
                self.source
                    .source_stream_policy_evidence("source_unit_interface"),
            ),
            (
                "source_state_stream_unit_byte_range_count".to_string(),
                self.source
                    .source_stream_policy_evidence("source_unit_byte_range_count"),
            ),
            (
                "source_state_stream_unit_physical_bytes".to_string(),
                self.source
                    .source_stream_policy_evidence("source_unit_physical_bytes"),
            ),
            (
                "source_state_stream_unit_byte_range_sample".to_string(),
                self.source
                    .source_stream_policy_evidence("source_unit_byte_range_sample"),
            ),
            (
                "source_state_stream_unit_physical_source".to_string(),
                self.source
                    .source_stream_policy_evidence("source_unit_physical_source"),
            ),
            (
                "source_state_stream_unit_scheduler_wait_status".to_string(),
                self.source
                    .source_stream_policy_evidence("source_unit_scheduler_wait_status"),
            ),
            (
                "source_state_stream_unit_decode_wait_status".to_string(),
                self.source
                    .source_stream_policy_evidence("source_unit_decode_wait_status"),
            ),
            (
                "source_state_stream_unit_writer_starvation_status".to_string(),
                self.source
                    .source_stream_policy_evidence("source_unit_writer_starvation_status"),
            ),
            (
                "source_state_stream_unit_physical_bandwidth_status".to_string(),
                self.source
                    .source_stream_policy_evidence("source_unit_physical_bandwidth_status"),
            ),
            (
                "source_state_parquet_extent_row_group_count".to_string(),
                self.source
                    .source_stream_policy_evidence("parquet_extent_row_groups"),
            ),
            (
                "source_state_parquet_extent_row_group_byte_range_count".to_string(),
                self.source
                    .source_stream_policy_evidence("parquet_extent_row_groups_with_byte_ranges"),
            ),
            (
                "source_state_parquet_extent_column_chunk_count".to_string(),
                self.source
                    .source_stream_policy_evidence("parquet_extent_column_chunks"),
            ),
            (
                "source_state_parquet_extent_column_chunk_byte_range_count".to_string(),
                self.source
                    .source_stream_policy_evidence("parquet_extent_column_chunks_with_byte_ranges"),
            ),
            (
                "source_state_parquet_extent_compressed_bytes".to_string(),
                self.source
                    .source_stream_policy_evidence("parquet_extent_compressed_bytes"),
            ),
            (
                "source_state_parquet_extent_uncompressed_bytes".to_string(),
                self.source
                    .source_stream_policy_evidence("parquet_extent_uncompressed_bytes"),
            ),
            (
                "source_state_parquet_extent_codec_summary".to_string(),
                self.source
                    .source_stream_policy_evidence("parquet_extent_codec_summary"),
            ),
            (
                "source_state_parquet_extent_dictionary_page_count".to_string(),
                self.source
                    .source_dictionary_evidence("parquet_extent_dictionary_pages"),
            ),
            (
                "source_state_parquet_extent_statistics_count".to_string(),
                self.source
                    .source_dictionary_evidence("parquet_extent_statistics"),
            ),
            (
                "source_state_parquet_extent_row_group_summary".to_string(),
                self.source
                    .source_dictionary_evidence("parquet_extent_row_group_summary"),
            ),
            (
                "source_state_dictionary_preservation_status".to_string(),
                self.source.source_dictionary_preservation_status.clone(),
            ),
            (
                "source_state_ingest_executor_status".to_string(),
                self.source.ingest_executor_status.clone(),
            ),
            (
                "source_state_ingest_executor_kind".to_string(),
                self.source.ingest_executor_kind.clone(),
            ),
            (
                "source_state_ingest_executor_requested_parallelism".to_string(),
                self.source
                    .ingest_executor_requested_parallelism
                    .to_string(),
            ),
            (
                "source_state_ingest_executor_applied_parallelism".to_string(),
                self.source.ingest_executor_applied_parallelism.to_string(),
            ),
            (
                "source_state_ingest_executor_unit_count_hint".to_string(),
                self.source.ingest_executor_unit_count_hint.map_or_else(
                    || "unknown".to_string(),
                    |unit_count| unit_count.to_string(),
                ),
            ),
            (
                "source_state_materialized_column_count".to_string(),
                self.source.materialized_columns.len().to_string(),
            ),
            (
                "source_state_materialized_columns".to_string(),
                self.source.materialized_columns_field(),
            ),
            (
                "source_state_reader_projection_column_count".to_string(),
                self.source.reader_projection_columns.len().to_string(),
            ),
            (
                "source_state_reader_projection_columns".to_string(),
                self.source.reader_projection_columns_field(),
            ),
            (
                "source_state_pruned_column_count".to_string(),
                self.source.pruned_column_count().to_string(),
            ),
            (
                "source_state_column_pruning_applied".to_string(),
                self.source.column_pruning_applied().to_string(),
            ),
            (
                "source_state_reuse_allowed".to_string(),
                "false".to_string(),
            ),
            ("source_state_reuse_hit".to_string(), "false".to_string()),
            (
                "source_state_reuse_reason".to_string(),
                "created_for_vortex_prepare".to_string(),
            ),
            (
                "source_schema_digest".to_string(),
                self.source_schema_digest.clone(),
            ),
            (
                "source_column_count".to_string(),
                self.source.header.len().to_string(),
            ),
            ("source_columns".to_string(), self.source.header.join(",")),
            (
                "input_row_count".to_string(),
                self.source.row_count.to_string(),
            ),
            (
                "target_vortex_path".to_string(),
                self.request.target_path.display().to_string(),
            ),
            (
                "vortex_artifact_ref".to_string(),
                self.vortex_report.target_path.display().to_string(),
            ),
            (
                "vortex_artifact_digest".to_string(),
                self.vortex_report.artifact_digest.clone(),
            ),
            (
                "prepared_artifact_ref".to_string(),
                self.vortex_report.target_path.display().to_string(),
            ),
            (
                "prepared_artifact_digest".to_string(),
                self.vortex_report.artifact_digest.clone(),
            ),
            (
                "prepared_artifact_reuse_eligible".to_string(),
                "true".to_string(),
            ),
            (
                "layout_summary".to_string(),
                self.vortex_report.layout_summary(),
            ),
            (
                "encoding_summary".to_string(),
                self.vortex_report.encoding_summary(),
            ),
            (
                "statistics_summary".to_string(),
                self.vortex_report.statistics_summary(),
            ),
            (
                "prepared_olap_layout_inventory_summary".to_string(),
                self.vortex_report.prepared_olap_layout_inventory_summary(),
            ),
            (
                "vortex_prepared_olap_layout_inventory_status".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .status
                    .clone(),
            ),
            (
                "vortex_prepared_olap_layout_inventory_digest".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .inventory_digest
                    .clone(),
            ),
            (
                "vortex_prepared_olap_layout_footer_row_count".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .row_count_field(),
            ),
            (
                "vortex_prepared_olap_layout_footer_segment_count".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .segment_count_field(),
            ),
            (
                "vortex_prepared_olap_layout_footer_statistics_status".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .statistics_status
                    .clone(),
            ),
            (
                "vortex_prepared_olap_layout_footer_encoding_layout_status".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .encoding_layout_status
                    .clone(),
            ),
            (
                "vortex_prepared_olap_layout_footer_approx_bytes".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .approx_footer_bytes_field(),
            ),
            (
                "vortex_prepared_olap_layout_root_encoding".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .root_layout_encoding
                    .clone(),
            ),
            (
                "vortex_prepared_olap_layout_encoding_inventory".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .layout_encoding_inventory
                    .clone(),
            ),
            (
                "vortex_prepared_olap_layout_segment_membership_status".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .segment_membership_status
                    .clone(),
            ),
            (
                "vortex_prepared_olap_layout_domain_dictionary_status".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .domain_dictionary_status
                    .clone(),
            ),
            (
                "vortex_prepared_olap_layout_derived_stats_status".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .derived_layout_stats_status
                    .clone(),
            ),
            (
                "vortex_prepared_olap_layout_row_position_locality_status".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .row_position_locality_status
                    .clone(),
            ),
            (
                "vortex_prepared_olap_layout_reader_cache_status".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .layout_reader_cache_status
                    .clone(),
            ),
            (
                "vortex_prepared_olap_layout_metadata_persisted_in_artifact".to_string(),
                self.vortex_report
                    .prepared_olap_layout_inventory
                    .metadata_persisted_in_artifact
                    .to_string(),
            ),
            (
                "column_family_summary".to_string(),
                self.vortex_report.column_family_summary(),
            ),
            ("vortex_prepare_included".to_string(), "true".to_string()),
            (
                "vortex_write_reopen_included".to_string(),
                "true".to_string(),
            ),
            (
                "compatibility_import_included".to_string(),
                "false".to_string(),
            ),
            (
                "preparation_included_in_timing".to_string(),
                self.vortex_report.preparation_included.to_string(),
            ),
            (
                "query_timing_starts_after_preparation".to_string(),
                self.vortex_report
                    .query_timing_starts_after_preparation
                    .to_string(),
            ),
            (
                "timing_scope".to_string(),
                self.vortex_report.timing_scope.clone(),
            ),
            (
                "certification_level".to_string(),
                self.vortex_report.certification_level.clone(),
            ),
            (
                "warm_query_timing_included".to_string(),
                "false".to_string(),
            ),
            (
                "prepare_once_millis".to_string(),
                self.prepare_once_total_millis.to_string(),
            ),
            (
                "prepared_olap_publication_millis".to_string(),
                self.prepared_olap_publication_millis.to_string(),
            ),
            (
                "source_read_millis".to_string(),
                self.source.read_millis.to_string(),
            ),
            (
                "source_read_metadata_scout_millis".to_string(),
                self.source.source_metadata_scout_millis.to_string(),
            ),
            (
                "source_read_byte_acquisition_millis".to_string(),
                self.source.source_byte_acquisition_millis.to_string(),
            ),
            (
                "source_read_full_body_millis".to_string(),
                self.source.source_full_body_millis.to_string(),
            ),
            (
                "compatibility_parse_millis".to_string(),
                self.source.compatibility_parse_millis.to_string(),
            ),
            (
                "source_to_columnar_millis".to_string(),
                self.source.source_to_columnar_millis.to_string(),
            ),
            (
                "universal_ingest_timing_split_schema_version".to_string(),
                "shardloom.universal_ingest_timing_split.v1".to_string(),
            ),
            (
                "universal_ingest_timing_split_status".to_string(),
                self.vortex_report.stream_timing_split_status.clone(),
            ),
            (
                "universal_ingest_source_read_millis".to_string(),
                self.source.read_millis.to_string(),
            ),
            (
                "universal_ingest_decode_derive_millis".to_string(),
                self.vortex_report
                    .stream_source_pull_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "universal_ingest_decode_millis".to_string(),
                self.vortex_report
                    .stream_decode_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "universal_ingest_derived_metadata_build_millis".to_string(),
                self.vortex_report
                    .stream_derived_metadata_build_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "universal_ingest_arrow_to_vortex_convert_millis".to_string(),
                self.vortex_report
                    .stream_array_convert_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "universal_ingest_encode_write_wall_millis".to_string(),
                self.vortex_report.write_micros.div_ceil(1000).to_string(),
            ),
            (
                "universal_ingest_footer_register_millis".to_string(),
                self.vortex_report
                    .workspace_stage_micros
                    .saturating_add(self.vortex_report.digest_micros)
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "universal_ingest_reopen_verify_millis".to_string(),
                self.vortex_report
                    .reopen_scan_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "universal_ingest_prepare_known_component_millis".to_string(),
                self.prepare_known_component_millis().to_string(),
            ),
            (
                "universal_ingest_prepare_unattributed_millis".to_string(),
                self.prepare_unattributed_millis().to_string(),
            ),
            (
                "universal_ingest_prepare_attribution_status".to_string(),
                self.prepare_attribution_status().to_string(),
            ),
            (
                "universal_ingest_prepare_attribution_policy".to_string(),
                "source_read_plus_vortex_write_wall_plus_reopen_verify_are_non_overlapping_primary_buckets;stream_decode_derive_convert_are_nested_writer_components"
                    .to_string(),
            ),
            (
                "universal_ingest_prepare_source_hydration_millis".to_string(),
                self.prepare_source_hydration_millis().to_string(),
            ),
            (
                "universal_ingest_prepare_nested_source_batch_production_millis".to_string(),
                self.prepare_nested_source_batch_production_millis()
                    .to_string(),
            ),
            (
                "universal_ingest_prepare_route_bookkeeping_millis".to_string(),
                Self::prepare_route_bookkeeping_millis().to_string(),
            ),
            (
                "universal_ingest_prepare_scheduler_wait_millis".to_string(),
                Self::prepare_scheduler_wait_millis().to_string(),
            ),
            (
                "universal_ingest_prepare_writer_backpressure_or_timer_overlap_millis"
                    .to_string(),
                self.prepare_writer_backpressure_or_timer_overlap_millis()
                    .to_string(),
            ),
            (
                "universal_ingest_prepare_evidence_emit_millis".to_string(),
                self.prepare_evidence_emit_millis().to_string(),
            ),
            (
                "universal_ingest_prepare_residual_split_policy".to_string(),
                "non_overlapping_known_components_plus_source_hydration_and_evidence_emit;source_batch_production_is_nested_stream_timing;writer_backpressure_or_timer_overlap_is_residual_upper_bound"
                    .to_string(),
            ),
            (
                "universal_ingest_stream_timing_overlap_policy".to_string(),
                if self.vortex_report.array_build_prefetch_window > 0 {
                    "capillary_prefetch_may_overlap_decode_derive_with_encode_write_wall_time"
                } else {
                    "serial_stream_decode_derive_precedes_each_encode_write_pull"
                }
                .to_string(),
            ),
            (
                "vortex_array_build_millis".to_string(),
                self.vortex_report
                    .array_build_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "vortex_array_build_provider_kind".to_string(),
                self.vortex_report.array_build_provider_kind.clone(),
            ),
            (
                "vortex_array_build_provider_surface".to_string(),
                self.vortex_report.array_build_provider_surface.clone(),
            ),
            (
                "vortex_array_build_strategy".to_string(),
                self.vortex_report.array_build_strategy.clone(),
            ),
            (
                "vortex_array_build_prefetch_window".to_string(),
                self.vortex_report.array_build_prefetch_window.to_string(),
            ),
            (
                "vortex_array_build_input_layout".to_string(),
                self.vortex_report.array_build_input_layout.clone(),
            ),
            (
                "vortex_array_build_record_batch_count".to_string(),
                self.vortex_report
                    .array_build_record_batch_count
                    .to_string(),
            ),
            (
                "vortex_array_build_manual_scalar_copy_avoided".to_string(),
                self.vortex_report.manual_scalar_copy_avoided.to_string(),
            ),
            (
                "vortex_ingest_millis".to_string(),
                self.vortex_report.write_micros.div_ceil(1000).to_string(),
            ),
            (
                "vortex_write_millis".to_string(),
                self.vortex_report.write_micros.div_ceil(1000).to_string(),
            ),
            (
                "vortex_write_timing_split_schema_version".to_string(),
                "shardloom.vortex_write_timing_split.v1".to_string(),
            ),
            (
                "vortex_writer_context_open_millis".to_string(),
                self.vortex_report
                    .writer_context_open_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "vortex_writer_context_reuse_status".to_string(),
                self.vortex_report.writer_context_reuse_status.clone(),
            ),
            (
                "vortex_writer_runtime_kind".to_string(),
                self.vortex_report.writer_runtime_kind.clone(),
            ),
            (
                "vortex_writer_runtime_requested_parallelism".to_string(),
                self.vortex_report
                    .writer_runtime_requested_parallelism
                    .to_string(),
            ),
            (
                "vortex_writer_runtime_applied_parallelism".to_string(),
                self.vortex_report
                    .writer_runtime_applied_parallelism
                    .to_string(),
            ),
            (
                "vortex_writer_runtime_background_workers".to_string(),
                self.vortex_report
                    .writer_runtime_background_workers
                    .to_string(),
            ),
            (
                "vortex_writer_layout_strategy_applied".to_string(),
                self.vortex_report.writer_layout_strategy_applied.clone(),
            ),
            (
                "vortex_writer_coalescing_policy_status".to_string(),
                self.vortex_report.writer_coalescing_policy_status.clone(),
            ),
            (
                "vortex_writer_layout_row_block_size".to_string(),
                self.vortex_report.writer_layout_row_block_size.to_string(),
            ),
            (
                "vortex_writer_layout_block_target_bytes".to_string(),
                self.vortex_report
                    .writer_layout_block_target_bytes
                    .to_string(),
            ),
            (
                "vortex_writer_compression_policy".to_string(),
                self.vortex_report.writer_compression_policy.clone(),
            ),
            (
                "vortex_writer_compression_field_count".to_string(),
                self.vortex_report
                    .writer_compression_field_count()
                    .to_string(),
            ),
            (
                "vortex_writer_compression_field_names".to_string(),
                self.vortex_report.writer_compression_field_names(),
            ),
            (
                "vortex_writer_compression_decision_count".to_string(),
                self.vortex_report
                    .writer_compression_decision_count()
                    .to_string(),
            ),
            (
                "vortex_writer_compression_decisions".to_string(),
                self.vortex_report.writer_compression_decisions(),
            ),
            (
                "vortex_writer_compression_concurrency".to_string(),
                self.vortex_report
                    .writer_compression_concurrency
                    .to_string(),
            ),
            (
                "vortex_writer_stats_concurrency".to_string(),
                self.vortex_report.writer_stats_concurrency.to_string(),
            ),
            (
                "vortex_writer_profile_selection_reason".to_string(),
                self.vortex_report.writer_profile_selection_reason.clone(),
            ),
            (
                "vortex_writer_profile_regression_guard".to_string(),
                self.vortex_report.writer_profile_regression_guard.clone(),
            ),
            (
                "vortex_segment_write_millis".to_string(),
                self.vortex_report
                    .vortex_segment_write_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "vortex_compression_millis".to_string(),
                self.vortex_report
                    .vortex_compression_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "vortex_encode_write_millis".to_string(),
                self.vortex_report
                    .vortex_encode_write_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "vortex_workspace_stage_millis".to_string(),
                self.vortex_report
                    .workspace_stage_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "vortex_final_commit_millis".to_string(),
                self.vortex_report
                    .vortex_final_commit_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "vortex_digest_millis".to_string(),
                self.vortex_report.digest_micros.div_ceil(1000).to_string(),
            ),
            (
                "vortex_artifact_digest_source".to_string(),
                self.vortex_report.artifact_digest_source.clone(),
            ),
            (
                "vortex_reopen_millis".to_string(),
                self.vortex_report
                    .reopen_scan_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "vortex_reopen_verify_millis".to_string(),
                self.vortex_report
                    .reopen_scan_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "vortex_scan_millis".to_string(),
                self.vortex_report
                    .reopen_scan_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "vortex_reopen_hot_path_status".to_string(),
                if vortex_report_reopen_metadata_row_count_verified(&self.vortex_report) {
                    "performed_metadata_row_count_for_ingest_certification"
                } else if self.vortex_report.upstream_vortex_scan_called {
                    "performed_for_ingest_certification"
                } else {
                    "skipped_minimal_ingest_digest_path"
                }
                .to_string(),
            ),
            ("warm_query_millis".to_string(), "0".to_string()),
            (
                "evidence_render_millis".to_string(),
                self.evidence_render_millis.to_string(),
            ),
            (
                "total_runtime_millis".to_string(),
                self.prepare_once_total_millis.to_string(),
            ),
            (
                "writer_row_count".to_string(),
                self.vortex_report.writer_row_count.to_string(),
            ),
            (
                "reopen_row_count".to_string(),
                self.vortex_report.reopen_row_count.to_string(),
            ),
            (
                "reopen_verification_status".to_string(),
                self.vortex_report.reopen_verification_status.clone(),
            ),
            (
                "vortex_artifact_bytes".to_string(),
                self.vortex_report.bytes_written.to_string(),
            ),
            (
                "source_backed_scan_evidence_status".to_string(),
                source_backed_scan_evidence_status.to_string(),
            ),
            (
                "source_backed_scan_provider_kind".to_string(),
                source_backed_scan_provider_kind.to_string(),
            ),
            (
                "source_backed_scan_provider_surface".to_string(),
                source_backed_scan_provider_surface.to_string(),
            ),
            (
                "source_backed_scan_rows_scanned".to_string(),
                self.vortex_report.reopen_row_count.to_string(),
            ),
            (
                "materialization_boundary".to_string(),
                format!(
                    "local_{}_{}_to_vortex_prepared_state",
                    self.source.source_format.as_str(),
                    self.source.materialization_layout
                ),
            ),
            (
                "legacy_materialization_boundary".to_string(),
                format!(
                    "local_{}_row_materialization_to_vortex_prepared_state",
                    self.source.source_format.as_str()
                ),
            ),
            ("data_decoded".to_string(), "true".to_string()),
            ("data_materialized".to_string(), "true".to_string()),
            (
                "source_native_io_certificate_status".to_string(),
                "scoped_compatibility_source_certificate".to_string(),
            ),
            (
                "native_io_certificate_status".to_string(),
                native_io_certificate_status.to_string(),
            ),
            (
                "output_native_io_certificate_status".to_string(),
                "not_requested".to_string(),
            ),
            (
                "upstream_vortex_write_called".to_string(),
                self.vortex_report.upstream_vortex_write_called.to_string(),
            ),
            (
                "upstream_vortex_scan_called".to_string(),
                self.vortex_report.upstream_vortex_scan_called.to_string(),
            ),
            ("object_store_io".to_string(), "false".to_string()),
            ("fallback_attempted".to_string(), "false".to_string()),
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("external_engine_invoked".to_string(), "false".to_string()),
            (
                "claim_gate_status".to_string(),
                claim_gate_status.to_string(),
            ),
            (
                "claim_gate_reason".to_string(),
                claim_gate_reason.to_string(),
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
        ];
        crate::execution_resources::append_declaration_fields(
            &mut fields,
            self.vortex_report.resources,
        );
        crate::execution_resources::append_admission_fields(
            &mut fields,
            self.vortex_report
                .shared_native_memory
                .as_ref()
                .map_or(self.vortex_report.resources.memory_bytes(), |memory| {
                    memory.limit_bytes
                }),
            self.vortex_report.writer_runtime_applied_parallelism,
            "shared_native_ingest_runtime;integer_lanes_bounded_by_declared_allocation_and_local_capacity",
        );
        crate::execution_resources::append_spill_observation_fields(&mut fields, false, Some(0));
        fields.extend(self.scout_ingress.evidence_fields());
        fields.extend(self.layout_write_advisor.evidence_fields());
        fields.extend(self.vortex_report.writer_physical_design.evidence_fields());
        fields.extend(self.vortex_report.stage_work.evidence_fields());
        if let Some(memory) = &self.vortex_report.shared_native_memory {
            crate::execution_resources::append_memory_observation_fields(
                &mut fields,
                Some(memory.final_reserved_bytes),
                memory.peak_reserved_bytes,
                "shared_pool_lifetime_including_other_retained_native_owners;excludes_source_reader_internals_provider_bypass_allocations_and_process_rss",
            );
            fields.extend(memory.evidence_fields());
        }
        fields.extend(
            self.vortex_report
                .segment_metadata_primitive
                .evidence_fields(),
        );
        fields.extend(self.vortex_report.preparation_spine.evidence_fields());
        fields.extend(self.preparation_spine_source_fields(certified_reopen));
        fields.extend(
            self.vortex_report
                .workspace_write_report
                .evidence_fields("vortex_ingest_output"),
        );
        fields.extend(
            self.vortex_report
                .capillary_prewrite_control
                .evidence_fields(),
        );
        fields.extend(self.capillary_preparation.evidence_fields());
        fields.extend(self.copy_budget.evidence_fields());
        if let Some(report) = &self.differential_preparation {
            fields.extend(report.evidence_fields());
        }
        if let Some(report) = &self.prepared_state_reuse {
            apply_prepared_state_reuse_fields(&mut fields, report);
        }
        if let Some(report) = &self.prepared_olap_state {
            apply_prepared_olap_state_fields(&mut fields, report);
        }
        fields
    }

    fn summary(&self) -> String {
        if let Some(report) = &self.differential_preparation {
            return format!(
                "vortex_ingest prepared {} base row(s) into {} with scout status {}, layout/write status {}, capillary status {}, copy-budget status {}, and differential overlay status {}",
                self.vortex_report.row_count,
                self.request.target_path.display(),
                self.scout_ingress.status,
                self.layout_write_advisor.status,
                self.capillary_preparation.status,
                self.copy_budget.status,
                report.status
            );
        }
        format!(
            "vortex_ingest prepared {} local row(s) into {} with scout status {}, layout/write status {}, capillary status {}, and copy-budget status {}",
            self.vortex_report.row_count,
            self.request.target_path.display(),
            self.scout_ingress.status,
            self.layout_write_advisor.status,
            self.capillary_preparation.status,
            self.copy_budget.status
        )
    }

    fn differential_preparation_blocked(&self) -> bool {
        self.differential_preparation
            .as_ref()
            .is_some_and(|report| !report.is_admitted())
    }

    fn prepared_artifact_segment_refs(&self) -> String {
        prepared_artifact_segment_refs_for(
            &self.prepared_state_id,
            self.vortex_report.row_count,
            &self.vortex_report.artifact_digest,
        )
    }

    fn preparation_spine_source_fields(&self, certified_reopen: bool) -> Vec<(String, String)> {
        let prepared_artifact_segment_evidence_status =
            if vortex_report_writer_summary_row_count_verified(&self.vortex_report) {
                "writer_summary_row_count_verified_layout_inventory_deferred"
            } else if vortex_report_reopen_metadata_row_count_verified(&self.vortex_report) {
                "writer_and_reopen_metadata_row_count_verified"
            } else if certified_reopen {
                "writer_and_reopen_row_count_verified"
            } else {
                "writer_row_count_and_digest_recorded_ingest_minimal"
            };
        let fields = vec![
            (
                "vortex_preparation_spine_source_state_id".to_string(),
                self.source_state_id.clone(),
            ),
            (
                "vortex_preparation_spine_source_state_digest".to_string(),
                self.source_state_digest.clone(),
            ),
            (
                "vortex_preparation_spine_source_format".to_string(),
                self.source.source_format.as_str().to_string(),
            ),
            (
                "vortex_preparation_spine_source_split_count".to_string(),
                self.source
                    .preparation_spine_source_split_count()
                    .to_string(),
            ),
            (
                "vortex_preparation_spine_source_split_refs".to_string(),
                self.source
                    .preparation_spine_source_split_refs(&self.source_state_id),
            ),
            (
                "vortex_preparation_spine_source_byte_range_refs".to_string(),
                self.source
                    .preparation_spine_source_byte_range_refs(&self.source_state_id),
            ),
            (
                "vortex_preparation_spine_source_row_range_refs".to_string(),
                self.source
                    .preparation_spine_source_row_range_refs(&self.source_state_id),
            ),
            (
                "vortex_preparation_spine_source_byte_range_status".to_string(),
                "whole_local_file_range_reported".to_string(),
            ),
            (
                "vortex_preparation_spine_source_row_range_status".to_string(),
                "source_state_row_ranges_reported".to_string(),
            ),
            (
                "vortex_preparation_spine_source_projection_mask".to_string(),
                self.source.materialized_columns_field(),
            ),
            (
                "vortex_preparation_spine_source_filter_mask".to_string(),
                "none".to_string(),
            ),
            (
                "vortex_preparation_spine_sink_ref".to_string(),
                self.vortex_report.target_path.display().to_string(),
            ),
            (
                "vortex_preparation_spine_prepared_state_id".to_string(),
                self.prepared_state_id.clone(),
            ),
            (
                "vortex_preparation_spine_prepared_state_digest".to_string(),
                self.prepared_state_digest.clone(),
            ),
            (
                "vortex_preparation_spine_prepared_artifact_segment_refs".to_string(),
                self.prepared_artifact_segment_refs(),
            ),
            (
                "vortex_preparation_spine_prepared_artifact_segment_evidence_status".to_string(),
                prepared_artifact_segment_evidence_status.to_string(),
            ),
            (
                "vortex_preparation_spine_no_standalone_lane_status".to_string(),
                "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state".to_string(),
            ),
        ];
        fields
    }

    fn to_text(&self) -> String {
        let mut text = format!(
            "ShardLoom Vortex prepare\nsource: {}\ntarget: {}\nsource format: {}\nrows prepared: {}\ncolumns: {}\ncertification level: {}\nscout ingress: {}\nlayout/write advisor: {}\nreopen verification: {}\nprepared state: {}\ncapillary preparation: {}\ncopy budget: {}\npulseweave: {}\nfallback execution: disabled",
            self.request.source_path.display(),
            self.request.target_path.display(),
            self.source.source_format.as_str(),
            self.vortex_report.row_count,
            self.source.header.join(","),
            self.vortex_report.certification_level,
            self.scout_ingress.status,
            self.layout_write_advisor.status,
            self.vortex_report.reopen_verification_status,
            self.prepared_state_id,
            self.capillary_preparation.status,
            self.copy_budget.status,
            self.capillary_preparation.pulseweave_report.status
        );
        if let Some(report) = &self.differential_preparation {
            write!(
                text,
                "\ndifferential preparation: {}\nupdate mode: {}\ndelta rows: {}\noverlay applied: {}",
                report.status,
                report.update_mode.as_str(),
                report.delta_row_count,
                report.overlay_applied
            )
            .expect("write to string");
        }
        text
    }
}

fn apply_prepared_state_reuse_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexPreparedStateReuseReport,
) {
    set_cli_field(fields, "prepared_state_reuse_allowed", "true");
    set_cli_field(fields, "prepared_state_reuse_scope", report.scope.clone());
    set_cli_field(
        fields,
        "prepared_state_reuse_manifest_path",
        report.manifest_path.display().to_string(),
    );
    set_cli_field(fields, "prepared_state_reuse_policy", report.policy);
    set_cli_field(fields, "prepared_state_reuse_hit", report.hit.to_string());
    set_cli_field(fields, "prepared_state_reuse_reason", report.reason.clone());
    set_cli_field(
        fields,
        "prepared_state_reuse_manifest_digest",
        report.manifest_digest.clone(),
    );
    set_cli_field(
        fields,
        "prepared_state_reuse_manifest_digest_algorithm",
        digest_algorithm(&report.manifest_digest),
    );
    set_cli_field(
        fields,
        "prepared_state_invalidation_reason",
        report.invalidation_reason.clone(),
    );
    set_cli_field(
        fields,
        "invalidation_reason",
        report.invalidation_reason.clone(),
    );
    set_cli_field(fields, "prepared_state_reused", report.hit.to_string());
    fields.extend(report.evidence_fields());
}

#[allow(clippy::too_many_lines)]
fn apply_prepared_olap_state_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexPreparedOlapStateReport,
) {
    set_cli_field(fields, "prepared_olap_state_allowed", "true");
    set_cli_field(fields, "prepared_olap_state_status", report.status.clone());
    set_cli_field(
        fields,
        "prepared_olap_state_evidence_persistence",
        "embedded_in_single_prepared_vortex_artifact",
    );
    set_cli_field(
        fields,
        "prepared_olap_state_external_manifest_written",
        "false",
    );
    set_cli_field(
        fields,
        "prepared_olap_state_query_time_contract",
        report.query_time_contract.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_artifact_model",
        report.artifact_model.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_writer_layout_strategy",
        report.writer_layout_strategy.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_embedded_layout_statistics_contract",
        report.embedded_layout_statistics_contract.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_layout_inventory_status",
        report.layout_inventory_status.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_layout_inventory_digest",
        report.layout_inventory_digest.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_layout_footer_row_count",
        report.layout_footer_row_count.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_layout_footer_segment_count",
        report.layout_footer_segment_count.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_layout_footer_statistics_status",
        report.layout_footer_statistics_status.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_layout_footer_encoding_layout_status",
        report.layout_footer_encoding_layout_status.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_layout_artifact_size_bytes",
        report.layout_artifact_size_bytes.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_layout_footer_approx_bytes",
        report.layout_footer_approx_bytes.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_layout_footer_dtype_summary",
        report.layout_footer_dtype_summary.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_layout_metadata_persisted_in_artifact",
        report.layout_metadata_persisted_in_artifact.to_string(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_layout_size_attribution",
        report.layout_size_attribution.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_dictionary_metadata_policy",
        report.dictionary_metadata_policy.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_metadata_pruning_contract",
        report.metadata_pruning_contract.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_query_answer_sidecar_status",
        report.query_answer_sidecar_status.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_admitted_query_families",
        report.admitted_query_families.clone(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_exact_sidecar_family_count",
        report.exact_sidecar_family_count.to_string(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_sub_second_candidate",
        report.sub_second_candidate.to_string(),
    );
    set_cli_field(
        fields,
        "prepared_olap_state_blocker_id",
        report.blocker_id.clone(),
    );
    fields.extend(report.evidence_fields());
}

fn set_cli_field(fields: &mut Vec<(String, String)>, key: &str, value: impl Into<String>) {
    if let Some((_, existing)) = fields.iter_mut().find(|(field, _)| field == key) {
        *existing = value.into();
    } else {
        fields.push((key.to_string(), value.into()));
    }
}

#[allow(clippy::too_many_lines)]
fn vortex_ingest_feature_blocked_fields(request: &VortexIngestRequest) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "schema_version".to_string(),
            VORTEX_PREPARE_SCHEMA_VERSION.to_string(),
        ),
        ("execution_mode".to_string(), "prepared_vortex".to_string()),
        (
            "selected_execution_mode".to_string(),
            "prepared_vortex".to_string(),
        ),
        ("engine_mode".to_string(), "batch".to_string()),
        ("runtime_execution".to_string(), "false".to_string()),
        ("support_status".to_string(), "blocked".to_string()),
        (
            "source_path".to_string(),
            request.source_path.display().to_string(),
        ),
        (
            "target_vortex_path".to_string(),
            request.target_path.display().to_string(),
        ),
        ("source_io_performed".to_string(), "false".to_string()),
        ("ingress_route".to_string(), "vortex_ingest".to_string()),
        (
            "ingress_route_label".to_string(),
            "Vortex ingest / prepare once route".to_string(),
        ),
        ("vortex_ingest_performed".to_string(), "false".to_string()),
        (
            "vortex_ingest_status".to_string(),
            "blocked_feature_gate".to_string(),
        ),
        (
            "vortex_ingest_requested_memory_gb".to_string(),
            request
                .resources
                .whole_gib()
                .map_or_else(|| "unavailable".into(), |value| value.to_string()),
        ),
        (
            "vortex_ingest_requested_max_parallelism".to_string(),
            request.resources.max_parallelism().to_string(),
        ),
        (
            "certification_level".to_string(),
            request.certification_level.as_str().to_string(),
        ),
        (
            "certification_status".to_string(),
            "blocked_feature_gate".to_string(),
        ),
        (
            "vortex_ingest_blocker_id".to_string(),
            "vortex_ingest.requires_vortex_write_feature".to_string(),
        ),
    ];
    fields.extend(vortex_ingest_feature_blocked_scout_fields(request));
    fields.extend(vortex_ingest_feature_blocked_layout_write_advisor_fields(
        request,
    ));
    fields.extend(vortex_ingest_feature_blocked_spine_fields());
    fields.extend(vortex_ingest_feature_blocked_capillary_fields(request));
    fields.extend(vortex_ingest_feature_blocked_copy_budget_fields(request));
    fields.extend([
        ("prepared_state_created".to_string(), "false".to_string()),
        ("prepared_state_reused".to_string(), "false".to_string()),
        (
            "prepared_state_reuse_allowed".to_string(),
            "false".to_string(),
        ),
        ("prepared_state_reuse_hit".to_string(), "false".to_string()),
        (
            "prepared_state_reuse_scope".to_string(),
            "blocked_before_single_vortex_artifact_write".to_string(),
        ),
        (
            "prepared_state_reuse_manifest_path".to_string(),
            "not_available_feature_gate_blocked".to_string(),
        ),
        (
            "prepared_state_reuse_policy".to_string(),
            "single_vortex_artifact_no_sidecar.v1".to_string(),
        ),
        (
            "prepared_state_reuse_reason".to_string(),
            "vortex_write_feature_gate_blocked".to_string(),
        ),
        (
            "prepared_state_reuse_manifest_digest".to_string(),
            "none".to_string(),
        ),
        (
            "prepared_state_reuse_manifest_digest_algorithm".to_string(),
            "not_available".to_string(),
        ),
        (
            "prepared_state_invalidation_reason".to_string(),
            "vortex_write_feature_gate_blocked".to_string(),
        ),
        (
            "invalidation_reason".to_string(),
            "vortex_write_feature_gate_blocked".to_string(),
        ),
        ("timing_scope".to_string(), "ingest_only".to_string()),
        (
            "claim_gate_status".to_string(),
            "not_claim_grade".to_string(),
        ),
        ("fallback_attempted".to_string(), "false".to_string()),
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("external_engine_invoked".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        ("performance_claim_allowed".to_string(), "false".to_string()),
        ("production_claim_allowed".to_string(), "false".to_string()),
    ]);
    fields
}

#[allow(clippy::too_many_lines)]
fn vortex_ingest_scout_blocked_fields(
    request: &VortexIngestRequest,
    error: &ShardLoomError,
) -> Option<Vec<(String, String)>> {
    let error_text = error.to_string();
    let classifier = classify_vortex_ingest_scout_error(&error_text)?;
    let blocked_request = vortex_ingest_scout_blocked_request(request, &error_text);
    let source_adapter = LocalInputAdapterSelection::select(
        &blocked_request.source_path,
        blocked_request.source_format_override,
    )
    .ok();
    let source_format = source_adapter
        .as_ref()
        .map_or("unknown".to_string(), |adapter| {
            adapter.source_format.as_str().to_string()
        });
    let (source_bytes, source_digest) = read_local_source_bytes_with_budget(
        &blocked_request.source_path,
        &source_format,
        Some(MAX_LOCAL_SOURCE_BYTES),
    )
    .map_or_else(
        |_| (0, "not_available_source_read_failed".to_string()),
        |bytes| {
            (
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                fnv64_digest_bytes(&bytes),
            )
        },
    );
    let source_state_id = format!("local-{source_format}-{}", source_digest.replace(':', "-"));
    let source_state_digest = fnv64_digest(&format!(
        "{source_format}|{}|{}|{}",
        blocked_request.source_path.display(),
        source_digest,
        classifier.diagnostic_code
    ));
    let source_schema_digest = format!("blocked_before_schema_materialization:{source_digest}");
    let scout_report = shardloom_vortex::evaluate_vortex_scout_ingress(
        shardloom_vortex::VortexScoutIngressInput {
            source_state_id,
            source_state_digest,
            source_format,
            source_path: blocked_request.source_path.display().to_string(),
            source_schema_digest,
            row_count: 0,
            source_byte_count: source_bytes,
            column_count: 0,
            read_plan: "blocked_before_full_preparation".to_string(),
            metadata_range_refs: if source_bytes == 0 {
                "not_available_source_read_failed".to_string()
            } else {
                format!(
                    "{}:bytes=0..{}",
                    blocked_request.source_path.display(),
                    source_bytes
                )
            },
            sampled_row_range_refs: classifier.malformed_row_refs.clone(),
            anomaly_count: 1,
            anomaly_families: classifier.anomaly_family.to_string(),
            malformed_row_refs: classifier.malformed_row_refs,
            schema_drift_status: classifier.schema_drift_status.to_string(),
            unsupported_shape_status: classifier.unsupported_shape_status.to_string(),
            nullability_status: "not_evaluated_blocked_before_preparation".to_string(),
            small_file_pathology_status: "not_evaluated_blocked_before_preparation".to_string(),
            quarantine_required: true,
            quarantine_output_plan_status: "planned_not_emitted_no_quarantine_sink_requested"
                .to_string(),
            quarantine_output_ref: "not_emitted".to_string(),
            quarantine_output_digest: "not_emitted".to_string(),
            redaction_status: "malformed_row_refs_are_row_numbers_only".to_string(),
            unsupported_diagnostic_code: classifier.diagnostic_code.to_string(),
            correctness_policy: "fail_closed_no_silent_repair_or_row_drop".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        },
    );
    let mut fields = vec![
        (
            "schema_version".to_string(),
            VORTEX_PREPARE_SCHEMA_VERSION.to_string(),
        ),
        ("execution_mode".to_string(), "prepared_vortex".to_string()),
        (
            "selected_execution_mode".to_string(),
            "prepared_vortex".to_string(),
        ),
        ("engine_mode".to_string(), "batch".to_string()),
        ("runtime_execution".to_string(), "false".to_string()),
        ("support_status".to_string(), "blocked".to_string()),
        (
            "source_path".to_string(),
            blocked_request.source_path.display().to_string(),
        ),
        (
            "target_vortex_path".to_string(),
            blocked_request.target_path.display().to_string(),
        ),
        ("source_io_performed".to_string(), "true".to_string()),
        ("ingress_route".to_string(), "vortex_ingest".to_string()),
        (
            "ingress_route_label".to_string(),
            "Vortex ingest / prepare once route".to_string(),
        ),
        ("vortex_ingest_performed".to_string(), "false".to_string()),
        (
            "vortex_ingest_status".to_string(),
            "blocked_scout_ingress".to_string(),
        ),
        (
            "vortex_ingest_requested_memory_gb".to_string(),
            blocked_request
                .resources
                .whole_gib()
                .map_or_else(|| "unavailable".into(), |value| value.to_string()),
        ),
        (
            "vortex_ingest_requested_max_parallelism".to_string(),
            blocked_request.resources.max_parallelism().to_string(),
        ),
        (
            "certification_level".to_string(),
            blocked_request.certification_level.as_str().to_string(),
        ),
        (
            "certification_status".to_string(),
            "blocked_scout_ingress".to_string(),
        ),
        (
            "vortex_ingest_blocker_id".to_string(),
            classifier.diagnostic_code.to_string(),
        ),
    ];
    fields.extend(scout_report.evidence_fields());
    fields.extend(vortex_ingest_scout_blocked_layout_write_advisor_fields(
        &blocked_request,
        &scout_report,
        classifier.diagnostic_code,
    ));
    fields.extend(vortex_ingest_scout_blocked_copy_budget_fields(
        &scout_report,
        classifier.diagnostic_code,
    ));
    fields.extend([
        ("prepared_state_created".to_string(), "false".to_string()),
        ("prepared_state_reused".to_string(), "false".to_string()),
        (
            "prepared_state_reuse_allowed".to_string(),
            "false".to_string(),
        ),
        ("prepared_state_reuse_hit".to_string(), "false".to_string()),
        (
            "prepared_state_reuse_scope".to_string(),
            "blocked_before_single_vortex_artifact_write".to_string(),
        ),
        (
            "prepared_state_reuse_manifest_path".to_string(),
            "not_available_scout_blocked".to_string(),
        ),
        (
            "prepared_state_reuse_policy".to_string(),
            "single_vortex_artifact_no_sidecar.v1".to_string(),
        ),
        (
            "prepared_state_reuse_reason".to_string(),
            "scout_ingress_blocked_before_single_vortex_artifact_write".to_string(),
        ),
        (
            "prepared_state_reuse_manifest_digest".to_string(),
            "none".to_string(),
        ),
        (
            "prepared_state_invalidation_reason".to_string(),
            "scout_ingress_blocked_before_single_vortex_artifact_write".to_string(),
        ),
        (
            "invalidation_reason".to_string(),
            "scout_ingress_blocked_before_single_vortex_artifact_write".to_string(),
        ),
        ("timing_scope".to_string(), "scout_ingress_only".to_string()),
        (
            "claim_gate_status".to_string(),
            "not_claim_grade".to_string(),
        ),
        ("fallback_attempted".to_string(), "false".to_string()),
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("external_engine_invoked".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        ("performance_claim_allowed".to_string(), "false".to_string()),
        ("production_claim_allowed".to_string(), "false".to_string()),
    ]);
    Some(fields)
}

fn vortex_ingest_scout_blocked_source_path<'a>(
    request: &'a VortexIngestRequest,
    error: &ShardLoomError,
) -> &'a Path {
    if error
        .to_string()
        .contains("vortex_ingest differential delta source")
        && let Some(delta) = request.delta.as_ref()
    {
        return delta.source_path.as_path();
    }
    request.source_path.as_path()
}

fn vortex_ingest_scout_blocked_request(
    request: &VortexIngestRequest,
    error_text: &str,
) -> VortexIngestRequest {
    if error_text.contains("vortex_ingest differential delta source")
        && let Some(delta) = request.delta.as_ref()
    {
        return VortexIngestRequest {
            source_path: delta.source_path.clone(),
            source_format_override: request.source_format_override,
            target_path: delta.target_path.clone(),
            allow_overwrite: request.allow_overwrite,
            certification_level: request.certification_level,
            runtime_profile: request.runtime_profile,
            resources: request.resources,
            shared_memory_pool: request.shared_memory_pool.clone(),
            source_fingerprint_policy: request.source_fingerprint_policy,
            delta: None,
            prepared_source_binding: None,
        };
    }
    request.clone()
}

struct ScoutErrorClassifier {
    diagnostic_code: &'static str,
    anomaly_family: &'static str,
    malformed_row_refs: String,
    schema_drift_status: &'static str,
    unsupported_shape_status: &'static str,
}

fn classify_vortex_ingest_scout_error(error_text: &str) -> Option<ScoutErrorClassifier> {
    if error_text.contains("scalar values only")
        || error_text.contains("flat object")
        || error_text.contains("nested")
    {
        Some(ScoutErrorClassifier {
            diagnostic_code: "vortex_scout_ingress.unsupported_nested_shape",
            anomaly_family: "unsupported_nested_shape",
            malformed_row_refs: scout_error_row_refs(error_text),
            schema_drift_status: "not_detected_no_prior_schema_baseline",
            unsupported_shape_status: "blocked_unsupported_nested_shape",
        })
    } else if error_text.contains("row width")
        || error_text.contains("quoted field is not closed")
        || error_text.contains("not valid UTF-8")
        || error_text.contains("not admitted by this scoped source runtime")
    {
        Some(ScoutErrorClassifier {
            diagnostic_code: "vortex_scout_ingress.malformed_source",
            anomaly_family: "malformed_record",
            malformed_row_refs: scout_error_row_refs(error_text),
            schema_drift_status: "not_detected_no_prior_schema_baseline",
            unsupported_shape_status: "not_detected",
        })
    } else if error_text.contains("SQL identifiers") || error_text.contains("header") {
        Some(ScoutErrorClassifier {
            diagnostic_code: "vortex_scout_ingress.schema_drift",
            anomaly_family: "schema_drift",
            malformed_row_refs: "none".to_string(),
            schema_drift_status: "blocked_schema_or_header_drift",
            unsupported_shape_status: "not_detected",
        })
    } else if error_text.contains("failed to read local")
        || error_text.contains("local input adapter registry")
    {
        Some(ScoutErrorClassifier {
            diagnostic_code: "vortex_scout_ingress.source_admission",
            anomaly_family: "source_admission",
            malformed_row_refs: "none".to_string(),
            schema_drift_status: "not_evaluated_source_admission_blocked",
            unsupported_shape_status: "not_detected",
        })
    } else {
        None
    }
}

fn scout_error_row_refs(error_text: &str) -> String {
    let Some(row_pos) = error_text.find("row ") else {
        return "unknown_row".to_string();
    };
    let after_row = &error_text[row_pos + "row ".len()..];
    let row_number = after_row
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    if row_number.is_empty() {
        "unknown_row".to_string()
    } else {
        format!("row={row_number}")
    }
}

fn vortex_ingest_feature_blocked_scout_fields(
    request: &VortexIngestRequest,
) -> Vec<(String, String)> {
    let mut fields = shardloom_vortex::evaluate_vortex_scout_ingress(
        shardloom_vortex::VortexScoutIngressInput {
            source_state_id: "not_created_feature_gate_blocked".to_string(),
            source_state_digest: "not_created_feature_gate_blocked".to_string(),
            source_format: "unknown".to_string(),
            source_path: request.source_path.display().to_string(),
            source_schema_digest: "not_created_feature_gate_blocked".to_string(),
            row_count: 0,
            source_byte_count: 0,
            column_count: 0,
            read_plan: "not_started_feature_gate_blocked".to_string(),
            metadata_range_refs: "not_reported_feature_gate_blocked".to_string(),
            sampled_row_range_refs: "not_reported_feature_gate_blocked".to_string(),
            anomaly_count: 0,
            anomaly_families: "none".to_string(),
            malformed_row_refs: "none".to_string(),
            schema_drift_status: "not_evaluated_feature_gate_blocked".to_string(),
            unsupported_shape_status: "not_detected".to_string(),
            nullability_status: "not_evaluated_feature_gate_blocked".to_string(),
            small_file_pathology_status: "not_evaluated_feature_gate_blocked".to_string(),
            quarantine_required: false,
            quarantine_output_plan_status: "not_started_feature_gate_blocked".to_string(),
            quarantine_output_ref: "not_emitted".to_string(),
            quarantine_output_digest: "not_emitted".to_string(),
            redaction_status: "not_applicable_no_source_rows_read".to_string(),
            unsupported_diagnostic_code: "vortex_ingest.requires_vortex_write_feature".to_string(),
            correctness_policy: "blocked_before_scout_runtime".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        },
    )
    .evidence_fields();
    fields.extend([
        (
            "source_fingerprint_kind".to_string(),
            request
                .source_fingerprint_policy
                .fingerprint_kind()
                .to_string(),
        ),
        (
            "source_fingerprint_policy".to_string(),
            request.source_fingerprint_policy.as_str().to_string(),
        ),
        (
            "source_fingerprint_identity_source".to_string(),
            request
                .source_fingerprint_policy
                .identity_source()
                .to_string(),
        ),
        (
            "source_content_fingerprint_requested".to_string(),
            request
                .source_fingerprint_policy
                .content_fingerprint_requested()
                .to_string(),
        ),
        (
            "source_content_fingerprint_performed".to_string(),
            "false".to_string(),
        ),
    ]);
    fields
}

fn vortex_ingest_feature_blocked_layout_write_advisor_fields(
    request: &VortexIngestRequest,
) -> Vec<(String, String)> {
    shardloom_vortex::evaluate_vortex_layout_write_advisor(
        shardloom_vortex::VortexLayoutWriteAdvisorInput {
            source_state_id: "not_created_feature_gate_blocked".to_string(),
            source_state_digest: "not_created_feature_gate_blocked".to_string(),
            source_format: "unknown".to_string(),
            source_schema_digest: "not_created_feature_gate_blocked".to_string(),
            row_count: 0,
            source_byte_count: 0,
            column_count: 0,
            workload_constitution: "not_evaluated_feature_gate_blocked".to_string(),
            source_statistics_status: "not_evaluated_feature_gate_blocked".to_string(),
            requested_pushdown_requirements: "not_evaluated_feature_gate_blocked".to_string(),
            sink_requirements: "not_evaluated_feature_gate_blocked".to_string(),
            layout_strategy: "not_admitted_feature_gate_blocked".to_string(),
            chunking_strategy: "not_admitted_feature_gate_blocked".to_string(),
            segmentation_strategy: "not_admitted_feature_gate_blocked".to_string(),
            dictionary_strategy: "not_admitted_feature_gate_blocked".to_string(),
            statistics_policy: "not_admitted_feature_gate_blocked".to_string(),
            writer_provider_kind: "none_feature_gate_blocked".to_string(),
            writer_provider_surface: "none_feature_gate_blocked".to_string(),
            writer_admission_policy: "blocked_before_vortex_write_feature".to_string(),
            writer_parallelism_budget: request.resources.max_parallelism(),
            writer_compression_candidate_fields: Vec::new(),
            write_reopen_verification_depth: "not_started_feature_gate_blocked".to_string(),
            materialization_boundary_status: "not_started_feature_gate_blocked".to_string(),
            decode_boundary_status: "not_started_feature_gate_blocked".to_string(),
            expected_read_tradeoff: "not_evaluated_feature_gate_blocked".to_string(),
            expected_write_tradeoff: "not_evaluated_feature_gate_blocked".to_string(),
            strategy_admitted: false,
            unsupported_diagnostic_code: "vortex_ingest.requires_vortex_write_feature".to_string(),
            correctness_refs: "none_feature_gate_blocked".to_string(),
            benchmark_refs: "none_feature_gate_blocked".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        },
    )
    .evidence_fields()
}

fn vortex_ingest_feature_blocked_copy_budget_fields(
    _request: &VortexIngestRequest,
) -> Vec<(String, String)> {
    shardloom_vortex::evaluate_vortex_copy_budget(shardloom_vortex::VortexCopyBudgetInput {
        source_state_id: "not_created_feature_gate_blocked".to_string(),
        source_state_digest: "not_created_feature_gate_blocked".to_string(),
        prepared_state_id: "not_created_feature_gate_blocked".to_string(),
        prepared_state_digest: "not_created_feature_gate_blocked".to_string(),
        source_format: "unknown".to_string(),
        row_count: 0,
        source_byte_count: 0,
        column_count: 0,
        allocation_scope: "not_started_feature_gate_blocked".to_string(),
        copy_scope: "not_started_feature_gate_blocked".to_string(),
        measurement_status: "not_started_feature_gate_blocked".to_string(),
        source_read_copy_bytes: "not_started".to_string(),
        parse_normalization_copy_bytes: "not_started".to_string(),
        columnar_handoff_copy_bytes: "not_started".to_string(),
        vortex_array_build_copy_bytes: "not_started".to_string(),
        writer_buffer_bytes: "not_started".to_string(),
        reopen_verify_copy_bytes: "not_started".to_string(),
        evidence_render_copy_bytes: "not_started".to_string(),
        total_measured_copy_bytes: "0".to_string(),
        buffer_family: "none_feature_gate_blocked".to_string(),
        ownership_policy: "not_started_feature_gate_blocked".to_string(),
        writer_buffering_status: "not_started_feature_gate_blocked".to_string(),
        buffer_reuse_status: "not_started_feature_gate_blocked".to_string(),
        buffer_reuse_count: 0,
        unsafe_lifetime_shortcut_status: "blocked_no_unsafe_lifetime_shortcuts".to_string(),
        correctness_parity_refs: "none_feature_gate_blocked".to_string(),
        materialization_boundary_status: "not_started_feature_gate_blocked".to_string(),
        decode_boundary_status: "not_started_feature_gate_blocked".to_string(),
        unsupported_diagnostic_code: "vortex_ingest.requires_vortex_write_feature".to_string(),
        fallback_attempted: false,
        external_engine_invoked: false,
    })
    .evidence_fields()
}

fn vortex_ingest_scout_blocked_layout_write_advisor_fields(
    request: &VortexIngestRequest,
    scout_report: &shardloom_vortex::VortexScoutIngressReport,
    blocker_code: &str,
) -> Vec<(String, String)> {
    shardloom_vortex::evaluate_vortex_layout_write_advisor(
        shardloom_vortex::VortexLayoutWriteAdvisorInput {
            source_state_id: scout_report.source_state_id.clone(),
            source_state_digest: scout_report.source_state_digest.clone(),
            source_format: scout_report.source_format.clone(),
            source_schema_digest: scout_report.source_schema_digest_after.clone(),
            row_count: scout_report.row_count,
            source_byte_count: scout_report.source_byte_count,
            column_count: scout_report.column_count,
            workload_constitution: "blocked_by_scout_ingress".to_string(),
            source_statistics_status: "not_evaluated_scout_ingress_blocked".to_string(),
            requested_pushdown_requirements: "none_prepare_once_full_source".to_string(),
            sink_requirements: "workspace_safe_local_vortex_file_sink".to_string(),
            layout_strategy: "not_admitted_scout_ingress_blocked".to_string(),
            chunking_strategy: "not_admitted_scout_ingress_blocked".to_string(),
            segmentation_strategy: "not_admitted_scout_ingress_blocked".to_string(),
            dictionary_strategy: "not_admitted_scout_ingress_blocked".to_string(),
            statistics_policy: "not_admitted_scout_ingress_blocked".to_string(),
            writer_provider_kind: "none_scout_ingress_blocked".to_string(),
            writer_provider_surface: "none_scout_ingress_blocked".to_string(),
            writer_admission_policy: "blocked_before_vortex_write_by_scout_ingress".to_string(),
            writer_parallelism_budget: request.resources.max_parallelism(),
            writer_compression_candidate_fields: Vec::new(),
            write_reopen_verification_depth: "not_started_scout_ingress_blocked".to_string(),
            materialization_boundary_status: "not_started_scout_ingress_blocked".to_string(),
            decode_boundary_status: "not_started_scout_ingress_blocked".to_string(),
            expected_read_tradeoff: "not_evaluated_scout_ingress_blocked".to_string(),
            expected_write_tradeoff: "not_evaluated_scout_ingress_blocked".to_string(),
            strategy_admitted: false,
            unsupported_diagnostic_code: format!(
                "vortex_layout_write_advisor.blocked_by_scout_ingress:{blocker_code}"
            ),
            correctness_refs: "none_scout_ingress_blocked".to_string(),
            benchmark_refs: "none_scout_ingress_blocked".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        },
    )
    .evidence_fields()
}

fn vortex_ingest_scout_blocked_copy_budget_fields(
    scout_report: &shardloom_vortex::VortexScoutIngressReport,
    blocker_code: &str,
) -> Vec<(String, String)> {
    shardloom_vortex::evaluate_vortex_copy_budget(shardloom_vortex::VortexCopyBudgetInput {
        source_state_id: scout_report.source_state_id.clone(),
        source_state_digest: scout_report.source_state_digest.clone(),
        prepared_state_id: "not_created_scout_ingress_blocked".to_string(),
        prepared_state_digest: "not_created_scout_ingress_blocked".to_string(),
        source_format: scout_report.source_format.clone(),
        row_count: scout_report.row_count,
        source_byte_count: scout_report.source_byte_count,
        column_count: scout_report.column_count,
        allocation_scope: "blocked_before_copy_budget".to_string(),
        copy_scope: "blocked_before_copy_budget".to_string(),
        measurement_status: "blocked_before_copy_budget".to_string(),
        source_read_copy_bytes: scout_report.source_byte_count.to_string(),
        parse_normalization_copy_bytes: "blocked_before_parse_normalization".to_string(),
        columnar_handoff_copy_bytes: "blocked_before_columnar_handoff".to_string(),
        vortex_array_build_copy_bytes: "blocked_before_vortex_array_build".to_string(),
        writer_buffer_bytes: "not_started".to_string(),
        reopen_verify_copy_bytes: "not_started".to_string(),
        evidence_render_copy_bytes: "not_measured".to_string(),
        total_measured_copy_bytes: scout_report.source_byte_count.to_string(),
        buffer_family: "source_bytes_only_scout_ingress_blocked".to_string(),
        ownership_policy: "owned_source_bytes_no_prepared_buffers".to_string(),
        writer_buffering_status: "not_started_scout_ingress_blocked".to_string(),
        buffer_reuse_status: "blocked_scout_ingress".to_string(),
        buffer_reuse_count: 0,
        unsafe_lifetime_shortcut_status: "blocked_no_unsafe_lifetime_shortcuts".to_string(),
        correctness_parity_refs: "none_scout_ingress_blocked".to_string(),
        materialization_boundary_status: "not_started_scout_ingress_blocked".to_string(),
        decode_boundary_status: "not_started_scout_ingress_blocked".to_string(),
        unsupported_diagnostic_code: format!(
            "vortex_copy_budget.blocked_by_scout_ingress:{blocker_code}"
        ),
        fallback_attempted: false,
        external_engine_invoked: false,
    })
    .evidence_fields()
}

fn vortex_ingest_feature_blocked_spine_fields() -> Vec<(String, String)> {
    vec![
        (
            "vortex_preparation_spine_schema_version".to_string(),
            shardloom_vortex::VORTEX_PREPARATION_SPINE_SCHEMA_VERSION.to_string(),
        ),
        (
            "vortex_preparation_spine_status".to_string(),
            "blocked_feature_gate".to_string(),
        ),
        (
            "vortex_preparation_spine_vortex_first_decision".to_string(),
            "blocked_until_vortex_or_shardloom_evidence".to_string(),
        ),
        (
            "vortex_preparation_spine_feature_gate".to_string(),
            "vortex-write".to_string(),
        ),
        (
            "vortex_preparation_spine_shardloom_admission_policy".to_string(),
            "blocked_before_local_vortex_ingest_source_sink_split_prepare_once".to_string(),
        ),
        (
            "vortex_preparation_spine_split_ref_status".to_string(),
            "not_reported_feature_gate_blocked".to_string(),
        ),
        (
            "vortex_preparation_spine_claim_gate_status".to_string(),
            "not_claim_grade".to_string(),
        ),
        (
            "vortex_preparation_spine_fallback_attempted".to_string(),
            "false".to_string(),
        ),
        (
            "vortex_preparation_spine_external_engine_invoked".to_string(),
            "false".to_string(),
        ),
    ]
}

fn vortex_ingest_feature_blocked_capillary_fields(
    request: &VortexIngestRequest,
) -> Vec<(String, String)> {
    vec![
        (
            "vortex_capillary_preparation_schema_version".to_string(),
            shardloom_vortex::VORTEX_CAPILLARY_PREPARATION_SCHEMA_VERSION.to_string(),
        ),
        (
            "vortex_capillary_preparation_status".to_string(),
            "blocked_feature_gate".to_string(),
        ),
        (
            "vortex_capillary_preparation_activation_policy".to_string(),
            "not_applicable_feature_gate_blocked".to_string(),
        ),
        (
            "vortex_capillary_preparation_activation_result".to_string(),
            "blocked".to_string(),
        ),
        (
            "vortex_capillary_preparation_activation_reason".to_string(),
            "vortex_write_feature_gate_disabled".to_string(),
        ),
        (
            "vortex_capillary_preparation_execution_window_count".to_string(),
            "0".to_string(),
        ),
        (
            "vortex_capillary_preparation_execution_window_size".to_string(),
            "0".to_string(),
        ),
        (
            "vortex_capillary_preparation_memory_budget_bytes".to_string(),
            request.resources.memory_bytes().to_string(),
        ),
        (
            "vortex_capillary_preparation_max_parallelism".to_string(),
            request.resources.max_parallelism().to_string(),
        ),
        (
            "vortex_capillary_preparation_execution_window_ids".to_string(),
            "none".to_string(),
        ),
        (
            "vortex_capillary_preparation_scheduler_applied".to_string(),
            "false".to_string(),
        ),
        (
            "vortex_capillary_preparation_scheduler_application_reason".to_string(),
            "vortex_write_feature_gate_disabled".to_string(),
        ),
        (
            "vortex_capillary_preparation_no_standalone_lane_status".to_string(),
            "blocked_before_vortex_ingest_source_state_to_vortex_prepared_state".to_string(),
        ),
        (
            "vortex_capillary_preparation_fallback_attempted".to_string(),
            "false".to_string(),
        ),
        (
            "vortex_capillary_preparation_external_engine_invoked".to_string(),
            "false".to_string(),
        ),
    ]
}

fn ordered_source_rows(
    header: &[String],
    rows: &[ExpressionInputRow],
) -> Result<Vec<Vec<(String, ScalarValue)>>, ShardLoomError> {
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            header
                .iter()
                .map(|column| {
                    row.get(column)
                        .cloned()
                        .map(|value| (column.clone(), value))
                        .ok_or_else(|| {
                            ShardLoomError::InvalidOperation(format!(
                                "local vortex_ingest row {} is missing column '{column}'; no fallback execution was attempted",
                                row_index + 1
                            ))
                        })
                })
                .collect()
        })
        .collect()
}

fn source_state_id_for_source(source: &VortexIngestSourceData) -> String {
    source.source_state_id()
}

fn source_state_digest_for_source(
    source: &VortexIngestSourceData,
    source_schema_digest: &str,
) -> String {
    source.source_state_digest(source_schema_digest)
}

fn read_local_source_with_plan_and_adapter(
    path: &Path,
    read_plan: &LocalSourceReadPlan,
    source_adapter: LocalInputAdapterSelection,
    read_limits: LocalSourceReadLimits,
) -> Result<CsvSourceData, ShardLoomError> {
    reject_remote_source_path(path)?;
    if path.is_dir() {
        return read_local_source_directory_with_plan_and_adapter(
            path,
            read_plan,
            source_adapter,
            read_limits,
        );
    }
    let source_format = source_adapter.source_format;
    let byte_read = read_local_source_bytes_with_budget_report(
        path,
        source_format.row_label(),
        read_limits.source_bytes,
    )?;
    let read_millis = byte_read.total_read_millis();
    let source_metadata_scout_millis = byte_read.source_metadata_scout_millis;
    let source_byte_acquisition_millis = byte_read.source_byte_acquisition_millis;
    let source_full_body_millis = byte_read.source_full_body_millis;
    let bytes = byte_read.bytes;
    let source_bytes = u64::try_from(bytes.len()).map_err(|_| {
        ShardLoomError::InvalidOperation(format!(
            "{} source length does not fit in u64",
            source_format.row_label()
        ))
    })?;
    let source_digest = fnv64_digest_bytes(&bytes);
    let parse_start = Instant::now();
    let content = match source_format {
        LocalSourceFormat::Csv => {
            let content = decode_local_text_source(path, source_format, bytes)?;
            let (header, rows) =
                parse_csv_source_content_with_plan(&content, read_plan, read_limits.input_rows)?;
            LocalSourceReadContent::text(source_format, header, rows)
        }
        LocalSourceFormat::Json => {
            let content = decode_local_text_source(path, source_format, bytes)?;
            let (header, rows) =
                parse_json_source_content_with_plan(&content, read_plan, read_limits.input_rows)?;
            LocalSourceReadContent::text(source_format, header, rows)
        }
        LocalSourceFormat::JsonLines => {
            let content = decode_local_text_source(path, source_format, bytes)?;
            let (header, rows) =
                parse_jsonl_source_content_with_plan(&content, read_plan, read_limits.input_rows)?;
            LocalSourceReadContent::text(source_format, header, rows)
        }
        LocalSourceFormat::Parquet => {
            read_parquet_source_content(path, read_plan, read_limits.input_rows)?
        }
        LocalSourceFormat::ArrowIpc => {
            read_arrow_ipc_source_content(path, read_plan, read_limits.input_rows)?
        }
        LocalSourceFormat::Avro => {
            read_avro_source_content(path, read_plan, read_limits.input_rows)?
        }
        LocalSourceFormat::Orc => read_orc_source_content(path, read_plan, read_limits.input_rows)?,
    };
    let LocalSourceReadContent {
        header,
        column_dtypes,
        column_arrow_dtypes,
        mut rows,
        reader_projection_columns,
        source_to_columnar_millis,
        record_batch_count,
        materialization_layout,
        parse_normalization,
        columnar_source_preserved,
    } = content;
    prune_rows_to_read_plan(&mut rows, read_plan);
    let materialized_columns = read_plan.materialized_columns(&header);
    let reader_projection_columns =
        reader_projection_columns.unwrap_or_else(|| materialized_columns.clone());
    let projection_pushdown_status = source_format.projection_pushdown_status(read_plan);
    Ok(CsvSourceData {
        source_adapter,
        source_format,
        header,
        column_dtypes,
        column_arrow_dtypes,
        rows,
        read_plan: read_plan.clone(),
        materialized_columns,
        reader_projection_columns,
        projection_pushdown_status,
        source_bytes,
        source_digest,
        source_metadata_scout_millis,
        source_byte_acquisition_millis,
        source_full_body_millis,
        read_millis,
        parse_millis: parse_start.elapsed().as_millis(),
        source_to_columnar_millis,
        record_batch_count,
        materialization_layout,
        parse_normalization,
        columnar_source_preserved,
    })
}

fn read_local_source_directory_with_plan_and_adapter(
    path: &Path,
    read_plan: &LocalSourceReadPlan,
    source_adapter: LocalInputAdapterSelection,
    read_limits: LocalSourceReadLimits,
) -> Result<CsvSourceData, ShardLoomError> {
    let source_format = source_adapter.source_format;
    let read_start = Instant::now();
    let partition_files = read_local_source_partition_files_with_budget(
        path,
        source_format,
        read_limits.source_bytes,
    )?;
    let source_bytes = partition_files.source_bytes;
    let source_digest = partition_files.source_digest;
    let source_metadata_scout_millis = partition_files.source_metadata_scout_millis;
    let source_byte_acquisition_millis = partition_files.source_byte_acquisition_millis;
    let source_full_body_millis = partition_files.source_full_body_millis;
    let read_millis = read_start.elapsed().as_millis();
    let parse_start = Instant::now();
    let content = match source_format {
        LocalSourceFormat::Csv => {
            let (header, rows) = parse_csv_partition_files_with_plan(
                &partition_files.files,
                read_plan,
                read_limits.input_rows,
            )?;
            LocalSourceReadContent::text(source_format, header, rows)
        }
        LocalSourceFormat::JsonLines => {
            let (header, rows) = parse_jsonl_partition_files_with_plan(
                &partition_files.files,
                read_plan,
                read_limits.input_rows,
            )?;
            LocalSourceReadContent::text(source_format, header, rows)
        }
        LocalSourceFormat::Json => {
            let (header, rows) = parse_json_partition_files_with_plan(
                &partition_files.files,
                read_plan,
                read_limits.input_rows,
            )?;
            LocalSourceReadContent::text(source_format, header, rows)
        }
        LocalSourceFormat::Parquet
        | LocalSourceFormat::ArrowIpc
        | LocalSourceFormat::Avro
        | LocalSourceFormat::Orc => {
            return Err(unsupported_sql_error(&format!(
                "local {} partition directories are admitted through the universal-format Vortex preparation path, not the scalar local-source runtime reader",
                source_format.row_label()
            )));
        }
    };
    let LocalSourceReadContent {
        header,
        column_dtypes,
        column_arrow_dtypes,
        mut rows,
        reader_projection_columns,
        source_to_columnar_millis,
        record_batch_count,
        materialization_layout,
        parse_normalization,
        columnar_source_preserved,
    } = content;
    prune_rows_to_read_plan(&mut rows, read_plan);
    let materialized_columns = read_plan.materialized_columns(&header);
    let reader_projection_columns =
        reader_projection_columns.unwrap_or_else(|| materialized_columns.clone());
    Ok(CsvSourceData {
        source_adapter,
        source_format,
        header,
        column_dtypes,
        column_arrow_dtypes,
        rows,
        read_plan: read_plan.clone(),
        materialized_columns,
        reader_projection_columns,
        projection_pushdown_status: source_format.projection_pushdown_status(read_plan),
        source_bytes,
        source_digest,
        source_metadata_scout_millis,
        source_byte_acquisition_millis,
        source_full_body_millis,
        read_millis,
        parse_millis: parse_start.elapsed().as_millis(),
        source_to_columnar_millis,
        record_batch_count,
        materialization_layout,
        parse_normalization,
        columnar_source_preserved,
    })
}

fn decode_local_text_source(
    path: &Path,
    source_format: LocalSourceFormat,
    bytes: Vec<u8>,
) -> Result<String, ShardLoomError> {
    String::from_utf8(bytes).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "local {} source {} is not valid UTF-8: {error}",
            source_format.row_label(),
            path.display()
        ))
    })
}

fn prune_rows_to_read_plan(rows: &mut [ExpressionInputRow], read_plan: &LocalSourceReadPlan) {
    let Some(required_columns) = read_plan.required_columns.as_ref() else {
        return;
    };
    for row in rows {
        row.retain(|column, _value| required_columns.contains(column));
    }
}

#[cfg(feature = "universal-format-io")]
fn read_structured_columnar_source_content<ReadFull, ReadProjected>(
    path: &Path,
    read_plan: &LocalSourceReadPlan,
    source_format: LocalSourceFormat,
    read_full: ReadFull,
    read_projected: ReadProjected,
    max_input_rows: Option<usize>,
) -> Result<LocalSourceReadContent, ShardLoomError>
where
    ReadFull:
        FnOnce(&Path, usize) -> Result<shardloom_vortex::FlatLocalColumnarSource, ShardLoomError>,
    ReadProjected: FnOnce(
        &Path,
        usize,
        &[String],
    ) -> Result<shardloom_vortex::FlatLocalColumnarSource, ShardLoomError>,
{
    let source_to_columnar_start = Instant::now();
    let max_input_rows = max_input_rows.unwrap_or(usize::MAX);
    let columnar_source = if let Some(required_columns) = read_plan.required_columns_vec() {
        read_projected(path, max_input_rows, &required_columns)?
    } else {
        read_full(path, max_input_rows)?
    };
    let source_to_columnar_millis = source_to_columnar_start.elapsed().as_millis();
    // Preserve the schema admitted by the shared columnar reader, including
    // qualified result field names; SQL identifier rules apply to SQL syntax.
    let record_batch_count = columnar_source.batches.len();
    let table = shardloom_vortex::materialize_flat_columnar_source_to_scalar_table(
        &columnar_source,
        path,
        source_format.row_label(),
    )?;
    Ok(LocalSourceReadContent::columnar_then_scalar(
        table.header,
        table.column_dtypes,
        table.column_arrow_dtypes,
        table.rows,
        table.reader_projection_columns,
        source_to_columnar_millis,
        record_batch_count,
    ))
}

#[cfg(feature = "universal-format-io")]
fn read_parquet_source_content(
    path: &Path,
    read_plan: &LocalSourceReadPlan,
    max_input_rows: Option<usize>,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    read_structured_columnar_source_content(
        path,
        read_plan,
        LocalSourceFormat::Parquet,
        shardloom_vortex::read_flat_parquet_columnar_source,
        shardloom_vortex::read_flat_parquet_columnar_source_with_projection,
        max_input_rows,
    )
}

#[cfg(not(feature = "universal-format-io"))]
fn read_parquet_source_content(
    _path: &Path,
    _read_plan: &LocalSourceReadPlan,
    _max_input_rows: Option<usize>,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    Err(unsupported_sql_error(
        "local Parquet source runtime requires building shardloom-cli with --features universal-format-io; default builds expose Parquet as a deterministic blocked adapter",
    ))
}

#[cfg(feature = "universal-format-io")]
fn read_arrow_ipc_source_content(
    path: &Path,
    read_plan: &LocalSourceReadPlan,
    max_input_rows: Option<usize>,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    read_structured_columnar_source_content(
        path,
        read_plan,
        LocalSourceFormat::ArrowIpc,
        shardloom_vortex::read_flat_arrow_ipc_columnar_source,
        shardloom_vortex::read_flat_arrow_ipc_columnar_source_with_projection,
        max_input_rows,
    )
}

#[cfg(not(feature = "universal-format-io"))]
fn read_arrow_ipc_source_content(
    _path: &Path,
    _read_plan: &LocalSourceReadPlan,
    _max_input_rows: Option<usize>,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    Err(unsupported_sql_error(
        "local Arrow IPC source runtime requires building shardloom-cli with --features universal-format-io; default builds expose Arrow IPC as a deterministic blocked adapter",
    ))
}

#[cfg(feature = "universal-format-io")]
fn read_avro_source_content(
    path: &Path,
    read_plan: &LocalSourceReadPlan,
    max_input_rows: Option<usize>,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    read_structured_columnar_source_content(
        path,
        read_plan,
        LocalSourceFormat::Avro,
        shardloom_vortex::read_flat_avro_columnar_source,
        shardloom_vortex::read_flat_avro_columnar_source_with_projection,
        max_input_rows,
    )
}

#[cfg(not(feature = "universal-format-io"))]
fn read_avro_source_content(
    _path: &Path,
    _read_plan: &LocalSourceReadPlan,
    _max_input_rows: Option<usize>,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    Err(unsupported_sql_error(
        "local Avro source runtime requires building shardloom-cli with --features universal-format-io; default builds expose Avro as a deterministic blocked adapter",
    ))
}

#[cfg(feature = "universal-format-io")]
fn read_orc_source_content(
    path: &Path,
    read_plan: &LocalSourceReadPlan,
    max_input_rows: Option<usize>,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    read_structured_columnar_source_content(
        path,
        read_plan,
        LocalSourceFormat::Orc,
        shardloom_vortex::read_flat_orc_columnar_source,
        shardloom_vortex::read_flat_orc_columnar_source_with_projection,
        max_input_rows,
    )
}

#[cfg(not(feature = "universal-format-io"))]
fn read_orc_source_content(
    _path: &Path,
    _read_plan: &LocalSourceReadPlan,
    _max_input_rows: Option<usize>,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    Err(unsupported_sql_error(
        "local ORC source runtime requires building shardloom-cli with --features universal-format-io; default builds expose ORC as a deterministic blocked adapter",
    ))
}

fn parse_csv_source_content_with_plan(
    content: &str,
    read_plan: &LocalSourceReadPlan,
    max_input_rows: Option<usize>,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    let mut reader = std::io::Cursor::new(content.as_bytes());
    let mut line = String::new();
    let mut records = std::iter::from_fn(|| {
        loop {
            match read_csv_record(&mut reader, &mut line) {
                Ok(0) => return None,
                Ok(_) if line.trim().is_empty() => {}
                Ok(_) => return Some(split_csv_record(line.trim_end_matches(['\r', '\n']))),
                Err(error) => return Some(Err(error)),
            }
        }
    });
    let Some(header_record) = records.next() else {
        return Err(unsupported_sql_error(
            "CSV source must include a header row",
        ));
    };
    let mut header = header_record?;
    strip_utf8_bom_from_first_header_cell(&mut header);
    if header.is_empty() {
        return Err(unsupported_sql_error("CSV source header must not be empty"));
    }
    for column in &header {
        validate_sql_identifier(column)?;
    }
    let mut rows = Vec::new();
    for record in records {
        let record = record?;
        enforce_local_source_row_budget(rows.len() + 1, max_input_rows, "CSV")?;
        if record.len() != header.len() {
            return Err(unsupported_sql_error(
                "CSV row width must match the header width for this scoped SQL local-source runtime",
            ));
        }
        let mut row = ExpressionInputRow::new();
        for (column, value) in header.iter().zip(record) {
            if read_plan.should_materialize(column) {
                row.insert(column.clone(), parse_csv_scalar(&value));
            }
        }
        rows.push(row);
    }
    Ok((header, rows))
}

fn parse_csv_partition_files_with_plan(
    files: &[LocalSourcePartitionFile],
    read_plan: &LocalSourceReadPlan,
    max_input_rows: Option<usize>,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    let mut header: Option<Vec<String>> = None;
    let mut rows = Vec::new();
    for file in files {
        let content =
            decode_local_text_source(&file.path, LocalSourceFormat::Csv, file.bytes.clone())?;
        let (file_header, mut file_rows) =
            parse_csv_source_content_with_plan(&content, read_plan, None)?;
        if let Some(header) = &header {
            if header != &file_header {
                return Err(unsupported_sql_error(&format!(
                    "CSV partition file {} has header {}, expected {}; no fallback execution was attempted",
                    file.path.display(),
                    file_header.join(","),
                    header.join(",")
                )));
            }
        } else {
            header = Some(file_header);
        }
        rows.append(&mut file_rows);
        enforce_partition_row_budget(rows.len(), max_input_rows, "CSV")?;
    }
    Ok((header.unwrap_or_default(), rows))
}

fn parse_jsonl_partition_files_with_plan(
    files: &[LocalSourcePartitionFile],
    read_plan: &LocalSourceReadPlan,
    max_input_rows: Option<usize>,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    let mut header = Vec::new();
    let mut rows = Vec::new();
    for file in files {
        let content =
            decode_local_text_source(&file.path, LocalSourceFormat::JsonLines, file.bytes.clone())?;
        for (line_index, line) in content
            .lines()
            .filter(|line| !line.trim().is_empty())
            .enumerate()
        {
            let line = if line_index == 0 {
                line.strip_prefix('\u{feff}').unwrap_or(line)
            } else {
                line
            };
            let fields = parse_flat_json_object_with_plan(line, read_plan).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "JSONL partition file {} row {} is not admitted by this scoped source runtime: {error}",
                    file.path.display(),
                    line_index + 1
                ))
            })?;
            append_flat_json_fields_to_materialized_rows(
                &mut header,
                &mut rows,
                fields,
                read_plan,
            )?;
            enforce_partition_row_budget(rows.len(), max_input_rows, "JSONL")?;
        }
    }
    ensure_flat_json_rows_present("JSONL partition directory", &rows)?;
    Ok((header, rows))
}

fn parse_json_partition_files_with_plan(
    files: &[LocalSourcePartitionFile],
    read_plan: &LocalSourceReadPlan,
    max_input_rows: Option<usize>,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    let mut header = Vec::new();
    let mut rows = Vec::new();
    for file in files {
        let content =
            decode_local_text_source(&file.path, LocalSourceFormat::Json, file.bytes.clone())?;
        let (file_header, file_rows) =
            parse_json_source_content_with_plan(&content, read_plan, None)?;
        append_partition_rows_with_header_union(&mut header, &mut rows, &file_header, file_rows);
        enforce_partition_row_budget(rows.len(), max_input_rows, "JSON")?;
    }
    Ok((header, rows))
}

fn append_partition_rows_with_header_union(
    header: &mut Vec<String>,
    rows: &mut Vec<ExpressionInputRow>,
    file_header: &[String],
    file_rows: Vec<ExpressionInputRow>,
) {
    for column in file_header {
        if !header.contains(column) {
            header.push(column.clone());
            for row in rows.iter_mut() {
                row.insert(column.clone(), ScalarValue::Null);
            }
        }
    }
    for mut row in file_rows {
        for column in header.iter() {
            row.entry(column.clone()).or_insert(ScalarValue::Null);
        }
        rows.push(row);
    }
}

fn enforce_partition_row_budget(
    row_count: usize,
    max_input_rows: Option<usize>,
    source_label: &str,
) -> Result<(), ShardLoomError> {
    if let Some(max_input_rows) = max_input_rows
        && row_count > max_input_rows
    {
        return Err(unsupported_sql_error(&format!(
            "local partition source runtime profile supports at most {max_input_rows} {source_label} data rows"
        )));
    }
    Ok(())
}

fn enforce_local_source_row_budget(
    row_count: usize,
    max_input_rows: Option<usize>,
    source_label: &str,
) -> Result<(), ShardLoomError> {
    if let Some(max_input_rows) = max_input_rows
        && row_count > max_input_rows
    {
        return Err(unsupported_sql_error(&format!(
            "local source runtime profile supports at most {max_input_rows} {source_label} data rows"
        )));
    }
    Ok(())
}

fn parse_jsonl_source_content_with_plan(
    content: &str,
    read_plan: &LocalSourceReadPlan,
    max_input_rows: Option<usize>,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    let mut header = Vec::new();
    let mut rows = Vec::new();
    for (line_index, line) in content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
    {
        let line = if line_index == 0 {
            line.strip_prefix('\u{feff}').unwrap_or(line)
        } else {
            line
        };
        let fields = parse_flat_json_object_with_plan(line, read_plan).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "JSONL row {} is not admitted by this scoped source runtime: {error}",
                line_index + 1
            ))
        })?;
        append_flat_json_fields_to_materialized_rows(&mut header, &mut rows, fields, read_plan)?;
        enforce_local_source_row_budget(rows.len(), max_input_rows, "JSONL")?;
    }
    ensure_flat_json_rows_present("JSONL", &rows)?;
    Ok((header, rows))
}

#[cfg(test)]
fn parse_json_source_content(
    content: &str,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    parse_json_source_content_with_plan(
        content,
        &LocalSourceReadPlan::full("full_source_state_parse_test"),
        Some(MAX_INPUT_ROWS),
    )
}

fn parse_json_source_content_with_plan(
    content: &str,
    read_plan: &LocalSourceReadPlan,
    max_input_rows: Option<usize>,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    let mut raw_rows = Vec::new();
    visit_json_source_rows_with_plan(content, read_plan, |fields| {
        raw_rows.push(fields);
        Ok(())
    })?;
    materialize_flat_json_rows("JSON", raw_rows, read_plan, max_input_rows)
}

fn visit_json_source_rows_with_plan(
    content: &str,
    read_plan: &LocalSourceReadPlan,
    mut visitor: impl FnMut(Vec<(String, ScalarValue)>) -> Result<(), ShardLoomError>,
) -> Result<(), ShardLoomError> {
    let trimmed = content.trim_start_matches('\u{feff}').trim();
    if trimmed.is_empty() {
        return Err(unsupported_sql_error(
            "JSON source must include one flat object or an array of flat object rows",
        ));
    }
    let chars = trimmed.chars().collect::<Vec<_>>();
    let mut index = skip_json_ws(&chars, 0);
    match chars.get(index) {
        Some('{') => {
            let (fields, next_index) =
                parse_flat_json_object_at_with_plan(&chars, index, "JSON", read_plan)?;
            index = skip_json_ws(&chars, next_index);
            if index != chars.len() {
                return Err(unsupported_sql_error(
                    "JSON source must contain exactly one flat object or one array of flat objects",
                ));
            }
            visitor(fields)?;
        }
        Some('[') => {
            index += 1;
            loop {
                index = skip_json_ws(&chars, index);
                if chars.get(index) == Some(&']') {
                    index += 1;
                    break;
                }
                let (fields, next_index) =
                    parse_flat_json_object_at_with_plan(&chars, index, "JSON", read_plan)?;
                visitor(fields)?;
                index = skip_json_ws(&chars, next_index);
                match chars.get(index) {
                    Some(',') => index += 1,
                    Some(']') => {
                        index += 1;
                        break;
                    }
                    _ => {
                        return Err(unsupported_sql_error(
                            "JSON array rows must be separated by ','",
                        ));
                    }
                }
            }
            if skip_json_ws(&chars, index) != chars.len() {
                return Err(unsupported_sql_error(
                    "JSON source array must be the only top-level value",
                ));
            }
        }
        _ => {
            return Err(unsupported_sql_error(
                "JSON source must be a flat object or an array of flat object rows",
            ));
        }
    }
    Ok(())
}

fn materialize_flat_json_rows(
    source_label: &str,
    raw_rows: Vec<Vec<(String, ScalarValue)>>,
    read_plan: &LocalSourceReadPlan,
    max_input_rows: Option<usize>,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    let mut header = Vec::new();
    for fields in &raw_rows {
        for (name, _value) in fields {
            if !header.contains(name) {
                validate_sql_identifier(name)?;
                header.push(name.clone());
            }
        }
    }
    if raw_rows.is_empty() {
        return Err(unsupported_sql_error(&format!(
            "{source_label} source must include at least one object row"
        )));
    }
    if let Some(max_input_rows) = max_input_rows
        && raw_rows.len() > max_input_rows
    {
        return Err(unsupported_sql_error(&format!(
            "local source runtime profile supports at most {max_input_rows} {source_label} data rows"
        )));
    }
    let mut rows = Vec::with_capacity(raw_rows.len());
    for fields in raw_rows {
        let mut row = ExpressionInputRow::new();
        for column in &header {
            if read_plan.should_materialize(column) {
                row.insert(column.clone(), ScalarValue::Null);
            }
        }
        for (column, value) in fields {
            if read_plan.should_materialize(&column) {
                row.insert(column, value);
            }
        }
        rows.push(row);
    }
    Ok((header, rows))
}

fn ensure_flat_json_rows_present(
    source_label: &str,
    rows: &[ExpressionInputRow],
) -> Result<(), ShardLoomError> {
    if rows.is_empty() {
        return Err(unsupported_sql_error(&format!(
            "{source_label} source must include at least one object row"
        )));
    }
    Ok(())
}

fn append_flat_json_fields_to_materialized_rows(
    header: &mut Vec<String>,
    rows: &mut Vec<ExpressionInputRow>,
    fields: Vec<(String, ScalarValue)>,
    read_plan: &LocalSourceReadPlan,
) -> Result<(), ShardLoomError> {
    for (name, _value) in &fields {
        if !header.contains(name) {
            validate_sql_identifier(name)?;
            let materialized = read_plan.should_materialize(name);
            header.push(name.clone());
            if materialized {
                for row in rows.iter_mut() {
                    row.insert(name.clone(), ScalarValue::Null);
                }
            }
        }
    }

    let mut row = ExpressionInputRow::new();
    for column in header.iter() {
        if read_plan.should_materialize(column) {
            row.insert(column.clone(), ScalarValue::Null);
        }
    }
    for (column, value) in fields {
        if read_plan.should_materialize(&column) {
            row.insert(column, value);
        }
    }
    rows.push(row);
    Ok(())
}

fn parse_flat_json_object_with_plan(
    raw: &str,
    read_plan: &LocalSourceReadPlan,
) -> Result<Vec<(String, ScalarValue)>, ShardLoomError> {
    let mut fields = Vec::new();
    visit_flat_json_object_with_plan(raw, read_plan, |key, value| fields.push((key, value)))?;
    Ok(fields)
}

fn visit_flat_json_object_with_plan(
    raw: &str,
    read_plan: &LocalSourceReadPlan,
    mut visitor: impl FnMut(String, ScalarValue),
) -> Result<(), ShardLoomError> {
    let chars = raw.trim().chars().collect::<Vec<_>>();
    let index = visit_flat_json_object_at_with_plan(
        &chars,
        skip_json_ws(&chars, 0),
        "JSONL",
        read_plan,
        &mut visitor,
    )?;
    if skip_json_ws(&chars, index) != chars.len() {
        return Err(unsupported_sql_error(
            "JSONL rows must contain exactly one JSON object per line",
        ));
    }
    Ok(())
}

fn parse_flat_json_object_at_with_plan(
    chars: &[char],
    index: usize,
    source_label: &str,
    read_plan: &LocalSourceReadPlan,
) -> Result<(Vec<(String, ScalarValue)>, usize), ShardLoomError> {
    let mut fields = Vec::new();
    let index = visit_flat_json_object_at_with_plan(
        chars,
        index,
        source_label,
        read_plan,
        &mut |key, value| fields.push((key, value)),
    )?;
    Ok((fields, index))
}

fn visit_flat_json_object_at_with_plan(
    chars: &[char],
    mut index: usize,
    source_label: &str,
    read_plan: &LocalSourceReadPlan,
    visitor: &mut impl FnMut(String, ScalarValue),
) -> Result<usize, ShardLoomError> {
    if chars.get(index) != Some(&'{') {
        return Err(unsupported_sql_error(&format!(
            "{source_label} rows must be flat JSON objects"
        )));
    }
    index += 1;
    let mut has_fields = false;
    loop {
        index = skip_json_ws(chars, index);
        if chars.get(index) == Some(&'}') {
            index += 1;
            break;
        }
        let (key, next_index) = parse_json_string(chars, index)?;
        validate_sql_identifier(&key)?;
        index = skip_json_ws(chars, next_index);
        if chars.get(index) != Some(&':') {
            return Err(unsupported_sql_error(
                "JSON object fields must use ':' between key and value",
            ));
        }
        index += 1;
        if read_plan.should_materialize(&key) {
            let (value, next_index) = parse_json_value(chars, index)?;
            visitor(key, value);
            index = next_index;
        } else {
            index = skip_json_value(chars, index)?;
            visitor(key, ScalarValue::Null);
        }
        has_fields = true;
        index = skip_json_ws(chars, index);
        match chars.get(index) {
            Some(',') => index += 1,
            Some('}') => {
                index += 1;
                break;
            }
            _ => {
                return Err(unsupported_sql_error(
                    "JSON object fields must be separated by ','",
                ));
            }
        }
    }
    if !has_fields {
        return Err(unsupported_sql_error(&format!(
            "{source_label} object rows must include at least one field"
        )));
    }
    Ok(index)
}

fn parse_json_value(
    chars: &[char],
    mut index: usize,
) -> Result<(ScalarValue, usize), ShardLoomError> {
    index = skip_json_ws(chars, index);
    match chars.get(index) {
        Some('"') => {
            let (value, next_index) = parse_json_string(chars, index)?;
            Ok((ScalarValue::Utf8(value), next_index))
        }
        Some('{' | '[') => {
            let next_index = skip_json_value(chars, index)?;
            let raw = chars[index..next_index].iter().collect::<String>();
            let value = serde_json::from_str::<serde_json::Value>(&raw).map_err(|error| {
                unsupported_sql_error(&format!(
                    "JSON nested object/array value is not valid JSON: {error}"
                ))
            })?;
            let canonical = serde_json::to_string(&value).map_err(|error| {
                unsupported_sql_error(&format!(
                    "JSON nested object/array value could not be normalized as UTF-8 JSON payload: {error}"
                ))
            })?;
            Ok((ScalarValue::Utf8(canonical), next_index))
        }
        Some(_) => {
            let start = index;
            while let Some(ch) = chars.get(index) {
                if *ch == ',' || *ch == '}' {
                    break;
                }
                index += 1;
            }
            let token = chars[start..index]
                .iter()
                .collect::<String>()
                .trim()
                .to_string();
            if token.is_empty() {
                return Err(unsupported_sql_error(
                    "JSON scalar values must not be empty",
                ));
            }
            let value = parse_json_bare_scalar(&token)?;
            Ok((value, index))
        }
        None => Err(unsupported_sql_error("JSONL object value is missing")),
    }
}

fn skip_json_value(chars: &[char], mut index: usize) -> Result<usize, ShardLoomError> {
    index = skip_json_ws(chars, index);
    match chars.get(index) {
        Some('"') => skip_json_string_value(chars, index),
        Some('{') => skip_json_object_value(chars, index),
        Some('[') => skip_json_array_value(chars, index),
        Some(_) => {
            let start = index;
            while let Some(ch) = chars.get(index) {
                if *ch == ',' || *ch == '}' || *ch == ']' {
                    break;
                }
                index += 1;
            }
            let token = chars[start..index]
                .iter()
                .collect::<String>()
                .trim()
                .to_string();
            if token.is_empty() {
                return Err(unsupported_sql_error(
                    "JSON scalar values must not be empty",
                ));
            }
            parse_json_bare_scalar(&token)?;
            Ok(index)
        }
        None => Err(unsupported_sql_error("JSONL object value is missing")),
    }
}

fn skip_json_string_value(chars: &[char], mut index: usize) -> Result<usize, ShardLoomError> {
    if chars.get(index) != Some(&'"') {
        return Err(unsupported_sql_error(
            "JSON object keys and string values must be quoted strings",
        ));
    }
    index += 1;
    while let Some(ch) = chars.get(index).copied() {
        index += 1;
        match ch {
            '"' => return Ok(index),
            '\\' => {
                let Some(escaped) = chars.get(index).copied() else {
                    return Err(unsupported_sql_error("JSONL string escape is incomplete"));
                };
                index += 1;
                match escaped {
                    '"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't' => {}
                    'u' => {
                        for _ in 0..4 {
                            let Some(hex) = chars.get(index).copied() else {
                                return Err(unsupported_sql_error(
                                    "JSON unicode escape is incomplete",
                                ));
                            };
                            if !hex.is_ascii_hexdigit() {
                                return Err(unsupported_sql_error(
                                    "JSON unicode escape must contain four hex digits",
                                ));
                            }
                            index += 1;
                        }
                    }
                    _ => {
                        return Err(unsupported_sql_error(
                            "JSON string contains an unsupported escape sequence",
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    Err(unsupported_sql_error("JSON string is not closed"))
}

fn skip_json_object_value(chars: &[char], mut index: usize) -> Result<usize, ShardLoomError> {
    if chars.get(index) != Some(&'{') {
        return Err(unsupported_sql_error(
            "JSON object value must start with '{'",
        ));
    }
    index += 1;
    loop {
        index = skip_json_ws(chars, index);
        if chars.get(index) == Some(&'}') {
            return Ok(index + 1);
        }
        index = skip_json_string_value(chars, index)?;
        index = skip_json_ws(chars, index);
        if chars.get(index) != Some(&':') {
            return Err(unsupported_sql_error(
                "JSON object fields must use ':' between key and value",
            ));
        }
        index = skip_json_value(chars, index + 1)?;
        index = skip_json_ws(chars, index);
        match chars.get(index) {
            Some(',') => index += 1,
            Some('}') => return Ok(index + 1),
            _ => {
                return Err(unsupported_sql_error(
                    "JSON object fields must be separated by ','",
                ));
            }
        }
    }
}

fn skip_json_array_value(chars: &[char], mut index: usize) -> Result<usize, ShardLoomError> {
    if chars.get(index) != Some(&'[') {
        return Err(unsupported_sql_error(
            "JSON array value must start with '['",
        ));
    }
    index += 1;
    loop {
        index = skip_json_ws(chars, index);
        if chars.get(index) == Some(&']') {
            return Ok(index + 1);
        }
        index = skip_json_value(chars, index)?;
        index = skip_json_ws(chars, index);
        match chars.get(index) {
            Some(',') => index += 1,
            Some(']') => return Ok(index + 1),
            _ => {
                return Err(unsupported_sql_error(
                    "JSON array values must be separated by ','",
                ));
            }
        }
    }
}

fn parse_json_bare_scalar(token: &str) -> Result<ScalarValue, ShardLoomError> {
    if token == "null" {
        Ok(ScalarValue::Null)
    } else if token == "true" {
        Ok(ScalarValue::Boolean(true))
    } else if token == "false" {
        Ok(ScalarValue::Boolean(false))
    } else if let Ok(parsed) = token.parse::<i64>() {
        Ok(ScalarValue::Int64(parsed))
    } else if let Ok(parsed) = token.parse::<f64>() {
        if parsed.is_finite() {
            Ok(ScalarValue::Float64(parsed))
        } else {
            Err(unsupported_sql_error(
                "JSON numeric values must be finite int64 or float64 scalars",
            ))
        }
    } else {
        Err(unsupported_sql_error(
            "JSON bare values are limited to null, booleans, finite numbers, and quoted strings",
        ))
    }
}

fn parse_json_string(chars: &[char], index: usize) -> Result<(String, usize), ShardLoomError> {
    if chars.get(index) != Some(&'"') {
        return Err(unsupported_sql_error(
            "JSON object keys and string values must be quoted strings",
        ));
    }
    let next_index = skip_json_string_value(chars, index)?;
    let raw = chars[index..next_index].iter().collect::<String>();
    let value = serde_json::from_str::<String>(&raw).map_err(|error| {
        unsupported_sql_error(&format!(
            "JSON string value is not valid JSON text: {error}"
        ))
    })?;
    Ok((value, next_index))
}

fn skip_json_ws(chars: &[char], mut index: usize) -> usize {
    while chars.get(index).is_some_and(|ch| ch.is_whitespace()) {
        index += 1;
    }
    index
}

fn normalize_local_output_path(value: &str) -> Result<PathBuf, ShardLoomError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "SQL local-source output path must not be empty".to_string(),
        ));
    }
    if trimmed.contains("://") && !trimmed.starts_with("file://") {
        return Err(ShardLoomError::InvalidOperation(
            "scoped SQL local-source runtime supports local file output only; object-store and remote URI writes remain blocked".to_string(),
        ));
    }
    let local = if let Some(rest) = trimmed.strip_prefix("file://") {
        local_path_from_file_uri(rest)?
    } else {
        trimmed.to_string()
    };
    if local.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "file:// SQL local-source output path must include a local path".to_string(),
        ));
    }
    Ok(Path::new(&local).to_path_buf())
}

fn normalize_local_vortex_ingest_target_path(value: &str) -> Result<PathBuf, ShardLoomError> {
    let path = normalize_local_output_path(value)?;
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if !extension.eq_ignore_ascii_case("vortex") {
        return Err(ShardLoomError::InvalidOperation(
            "vortex prepare writes local .vortex targets only; object-store, table, and non-Vortex sinks remain blocked"
                .to_string(),
        ));
    }
    Ok(path)
}

#[derive(Debug)]
struct LocalSourcePartitionFile {
    path: PathBuf,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct LocalSourcePartitionFiles {
    files: Vec<LocalSourcePartitionFile>,
    source_bytes: u64,
    source_digest: String,
    source_metadata_scout_millis: u128,
    source_byte_acquisition_millis: u128,
    source_full_body_millis: u128,
}

#[derive(Debug)]
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
struct LocalSourcePartitionScout {
    files: Vec<PathBuf>,
    evidence: ColumnarSourceScoutEvidence,
}

#[derive(Debug)]
struct LocalSourceByteRead {
    bytes: Vec<u8>,
    source_metadata_scout_millis: u128,
    source_byte_acquisition_millis: u128,
    source_full_body_millis: u128,
}

impl LocalSourceByteRead {
    fn total_read_millis(&self) -> u128 {
        self.source_metadata_scout_millis
            .saturating_add(self.source_byte_acquisition_millis)
    }
}

#[derive(Debug)]
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
struct ColumnarSourceScoutEvidence {
    bytes: u64,
    digest: String,
    fingerprint_kind: String,
    fingerprint_policy: String,
    identity_source: String,
    content_fingerprint_requested: bool,
    content_fingerprint_performed: bool,
    metadata_scout_millis: u128,
    byte_acquisition_millis: u128,
    full_body_millis: u128,
}

fn strip_utf8_bom_from_first_header_cell(header: &mut [String]) {
    if let Some(first) = header.first_mut()
        && let Some(stripped) = first.strip_prefix('\u{feff}')
    {
        *first = stripped.to_string();
    }
}

fn local_path_from_file_uri(rest: &str) -> Result<String, ShardLoomError> {
    if rest.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "file:// SQL local-source output path must include a local path".to_string(),
        ));
    }
    let local = if rest.starts_with('/') {
        rest.to_string()
    } else {
        let Some((authority, path)) = rest.split_once('/') else {
            return Err(ShardLoomError::InvalidOperation(
                "file:// SQL local-source output path must include a local path".to_string(),
            ));
        };
        if !authority.is_empty() && !authority.eq_ignore_ascii_case("localhost") {
            return Err(ShardLoomError::InvalidOperation(format!(
                "file:// SQL local-source output URI authority {authority:?} is not local; only empty authority or localhost is allowed"
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

fn reject_remote_source_path(path: &Path) -> Result<(), ShardLoomError> {
    let value = path.to_string_lossy();
    if value.contains("://") || value.starts_with("s3:") || value.starts_with("gs:") {
        return Err(unsupported_sql_error(
            "SQL local-source runtime supports local CSV, JSONL/NDJSON, flat JSON, and feature-gated Parquet/Arrow IPC/Avro/ORC file paths only; object-store and remote URI reads remain blocked",
        ));
    }
    Ok(())
}

fn read_local_source_bytes_with_budget(
    path: &Path,
    source_label: &str,
    max_source_bytes: Option<u64>,
) -> Result<Vec<u8>, ShardLoomError> {
    Ok(read_local_source_bytes_with_budget_report(path, source_label, max_source_bytes)?.bytes)
}

fn read_local_source_bytes_with_budget_report(
    path: &Path,
    source_label: &str,
    max_source_bytes: Option<u64>,
) -> Result<LocalSourceByteRead, ShardLoomError> {
    reject_remote_source_path(path)?;
    let scout_start = Instant::now();
    let metadata = fs::metadata(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to inspect local {source_label} source {} before read: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(unsupported_sql_error(&format!(
            "local {source_label} source {} must be a regular file",
            path.display()
        )));
    }
    if max_source_bytes.is_some_and(|limit| metadata.len() > limit) {
        let limit = max_source_bytes.expect("checked source byte limit");
        return Err(unsupported_sql_error(&format!(
            "local {source_label} source {} is {} bytes; scoped local-source evidence reads admit at most {limit} bytes",
            path.display(),
            metadata.len()
        )));
    }
    let source_metadata_scout_millis = scout_start.elapsed().as_millis();
    let read_start = Instant::now();
    let bytes = fs::read(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read local {source_label} source {}: {error}",
            path.display()
        ))
    })?;
    let source_byte_acquisition_millis = read_start.elapsed().as_millis();
    let read_len = u64::try_from(bytes.len()).map_err(|_| {
        ShardLoomError::InvalidOperation(format!(
            "local {source_label} source {} length does not fit in u64",
            path.display()
        ))
    })?;
    if max_source_bytes.is_some_and(|limit| read_len > limit) {
        return Err(unsupported_sql_error(&format!(
            "local {source_label} source {} exceeded the scoped local-source evidence byte budget during read",
            path.display()
        )));
    }
    Ok(LocalSourceByteRead {
        bytes,
        source_metadata_scout_millis,
        source_byte_acquisition_millis,
        source_full_body_millis: source_byte_acquisition_millis,
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn fingerprint_local_source_file_with_budget_report(
    path: &Path,
    source_label: &str,
    max_source_bytes: Option<u64>,
    policy: SourceFingerprintPolicy,
) -> Result<ColumnarSourceScoutEvidence, ShardLoomError> {
    reject_remote_source_path(path)?;
    let scout_start = Instant::now();
    let metadata = fs::metadata(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to inspect local {source_label} source {} before fingerprint: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(unsupported_sql_error(&format!(
            "local {source_label} source {} must be a regular file",
            path.display()
        )));
    }
    if max_source_bytes.is_some_and(|limit| metadata.len() > limit) {
        let limit = max_source_bytes.expect("checked source byte limit");
        return Err(unsupported_sql_error(&format!(
            "local {source_label} source {} is {} bytes; scoped local-source evidence reads admit at most {limit} bytes",
            path.display(),
            metadata.len()
        )));
    }
    let source_metadata_scout_millis = scout_start.elapsed().as_millis();
    if policy == SourceFingerprintPolicy::MetadataOnly {
        return Ok(ColumnarSourceScoutEvidence {
            bytes: metadata.len(),
            digest: local_source_metadata_fingerprint(path, source_label, &metadata),
            fingerprint_kind: policy.fingerprint_kind().to_string(),
            fingerprint_policy: policy.as_str().to_string(),
            identity_source: policy.identity_source().to_string(),
            content_fingerprint_requested: policy.content_fingerprint_requested(),
            content_fingerprint_performed: policy.content_fingerprint_requested(),
            metadata_scout_millis: source_metadata_scout_millis,
            byte_acquisition_millis: 0,
            full_body_millis: 0,
        });
    }
    let read_start = Instant::now();
    let mut file = fs::File::open(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to open local {source_label} source {} for streaming fingerprint: {error}",
            path.display()
        ))
    })?;
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to fingerprint local {source_label} source {}: {error}",
                path.display()
            ))
        })?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).map_err(|_| {
                ShardLoomError::InvalidOperation(format!(
                    "local {source_label} source {} read length does not fit in u64",
                    path.display()
                ))
            })?)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(format!(
                    "local {source_label} source {} byte count overflowed u64; no fallback execution was attempted",
                    path.display()
                ))
            })?;
        if max_source_bytes.is_some_and(|limit| total > limit) {
            return Err(unsupported_sql_error(&format!(
                "local {source_label} source {} exceeded the scoped local-source evidence byte budget during streaming fingerprint",
                path.display()
            )));
        }
        hash = update_fnv64_hash(hash, &buffer[..read]);
    }
    let source_byte_acquisition_millis = read_start.elapsed().as_millis();
    Ok(ColumnarSourceScoutEvidence {
        bytes: total,
        digest: format!("fnv64:{hash:016x}"),
        fingerprint_kind: policy.fingerprint_kind().to_string(),
        fingerprint_policy: policy.as_str().to_string(),
        identity_source: policy.identity_source().to_string(),
        content_fingerprint_requested: policy.content_fingerprint_requested(),
        content_fingerprint_performed: policy.content_fingerprint_requested(),
        metadata_scout_millis: source_metadata_scout_millis,
        byte_acquisition_millis: source_byte_acquisition_millis,
        full_body_millis: 0,
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn local_source_metadata_fingerprint(
    path: &Path,
    source_label: &str,
    metadata: &fs::Metadata,
) -> String {
    let modified_ns = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or_else(
            || "unknown".to_string(),
            |duration| duration.as_nanos().to_string(),
        );
    fnv64_digest(&format!(
        "local_file_metadata_fingerprint.v1|{}|{}|{}|{}",
        source_label,
        path.display(),
        metadata.len(),
        modified_ns
    ))
}

fn sorted_local_source_partition_entries(
    path: &Path,
    source_format: LocalSourceFormat,
) -> Result<Vec<std::fs::DirEntry>, ShardLoomError> {
    let mut entries = fs::read_dir(path)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to list local {} partition source {}: {error}",
                source_format.row_label(),
                path.display()
            ))
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to read local {} partition directory entry under {}: {error}",
                source_format.row_label(),
                path.display()
            ))
        })?;
    entries.sort_by_key(std::fs::DirEntry::path);
    Ok(entries)
}

fn read_local_source_partition_files_with_budget(
    path: &Path,
    source_format: LocalSourceFormat,
    max_source_bytes: Option<u64>,
) -> Result<LocalSourcePartitionFiles, ShardLoomError> {
    reject_remote_source_path(path)?;
    let total_start = Instant::now();
    let mut source_byte_acquisition_millis = 0_u128;
    let metadata = fs::metadata(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to inspect local {} partition source {} before read: {error}",
            source_format.row_label(),
            path.display()
        ))
    })?;
    if !metadata.is_dir() {
        return Err(unsupported_sql_error(&format!(
            "local {} partition source {} must be a directory",
            source_format.row_label(),
            path.display()
        )));
    }
    let entries = sorted_local_source_partition_entries(path, source_format)?;

    let mut files = Vec::new();
    let mut digest_parts = Vec::new();
    let mut total_bytes = 0_u64;
    for entry in entries {
        let file_path = entry.path();
        let metadata = entry.metadata().map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to inspect local partition file {}: {error}",
                file_path.display()
            ))
        })?;
        if !metadata.is_file() || !partition_file_matches_source_format(&file_path, source_format) {
            continue;
        }
        total_bytes = total_bytes.checked_add(metadata.len()).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local partition source byte count overflowed u64; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
        if max_source_bytes.is_some_and(|limit| total_bytes > limit) {
            let limit = max_source_bytes.expect("checked source byte limit");
            return Err(unsupported_sql_error(&format!(
                "local {} partition source {} is {total_bytes} bytes; scoped local-source evidence reads admit at most {limit} bytes",
                source_format.row_label(),
                path.display()
            )));
        }
        let file_read_start = Instant::now();
        let bytes = fs::read(&file_path).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to read local partition file {}: {error}",
                file_path.display()
            ))
        })?;
        source_byte_acquisition_millis =
            source_byte_acquisition_millis.saturating_add(file_read_start.elapsed().as_millis());
        let file_digest = fnv64_digest_bytes(&bytes);
        digest_parts.push(format!(
            "{}:{}:{}",
            file_path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("<invalid-name>"),
            bytes.len(),
            file_digest
        ));
        files.push(LocalSourcePartitionFile {
            path: file_path,
            bytes,
        });
    }
    if files.is_empty() {
        return Err(unsupported_sql_error(&format!(
            "local {} partition source {} did not contain any admitted {} files",
            source_format.row_label(),
            path.display(),
            source_format.admitted_extensions()
        )));
    }
    let total_millis = total_start.elapsed().as_millis();
    Ok(LocalSourcePartitionFiles {
        files,
        source_bytes: total_bytes,
        source_digest: fnv64_digest(&format!(
            "partition_dir|{}|{}|{}",
            source_format.as_str(),
            path.display(),
            digest_parts.join(";")
        )),
        source_metadata_scout_millis: total_millis.saturating_sub(source_byte_acquisition_millis),
        source_byte_acquisition_millis,
        source_full_body_millis: source_byte_acquisition_millis,
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn scout_local_source_partition_files_with_budget(
    path: &Path,
    source_format: LocalSourceFormat,
    max_source_bytes: Option<u64>,
    policy: SourceFingerprintPolicy,
) -> Result<LocalSourcePartitionScout, ShardLoomError> {
    reject_remote_source_path(path)?;
    let total_start = Instant::now();
    let metadata = fs::metadata(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to inspect local {} partition source {} before scout: {error}",
            source_format.row_label(),
            path.display()
        ))
    })?;
    if !metadata.is_dir() {
        return Err(unsupported_sql_error(&format!(
            "local {} partition source {} must be a directory",
            source_format.row_label(),
            path.display()
        )));
    }
    let entries = sorted_local_source_partition_entries(path, source_format)?;

    let mut files = Vec::new();
    let mut digest_parts = Vec::new();
    let mut total_bytes = 0_u64;
    let mut byte_acquisition_millis = 0_u128;
    for entry in entries {
        let file_path = entry.path();
        let metadata = entry.metadata().map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to inspect local partition file {}: {error}",
                file_path.display()
            ))
        })?;
        if !metadata.is_file() || !partition_file_matches_source_format(&file_path, source_format) {
            continue;
        }
        total_bytes = total_bytes.checked_add(metadata.len()).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local partition source byte count overflowed u64; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
        if max_source_bytes.is_some_and(|limit| total_bytes > limit) {
            let limit = max_source_bytes.expect("checked source byte limit");
            return Err(unsupported_sql_error(&format!(
                "local {} partition source {} is {total_bytes} bytes; scoped local-source evidence reads admit at most {limit} bytes",
                source_format.row_label(),
                path.display()
            )));
        }
        let file_fingerprint = fingerprint_local_source_file_with_budget_report(
            &file_path,
            source_format.row_label(),
            None,
            policy,
        )?;
        byte_acquisition_millis =
            byte_acquisition_millis.saturating_add(file_fingerprint.byte_acquisition_millis);
        digest_parts.push(format!(
            "{}:{}:{}",
            file_path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("<invalid-name>"),
            metadata.len(),
            file_fingerprint.digest
        ));
        files.push(file_path);
    }
    if files.is_empty() {
        return Err(unsupported_sql_error(&format!(
            "local {} partition source {} did not contain any admitted {} files",
            source_format.row_label(),
            path.display(),
            source_format.admitted_extensions()
        )));
    }
    Ok(LocalSourcePartitionScout {
        files,
        evidence: ColumnarSourceScoutEvidence {
            bytes: total_bytes,
            digest: fnv64_digest(&format!(
                "partition_dir_{}_fingerprint.v1|{}|{}|{}",
                policy.as_str(),
                source_format.as_str(),
                path.display(),
                digest_parts.join(";")
            )),
            fingerprint_kind: format!("partition_directory_{}", policy.fingerprint_kind()),
            fingerprint_policy: policy.as_str().to_string(),
            identity_source: policy.identity_source().to_string(),
            content_fingerprint_requested: policy.content_fingerprint_requested(),
            content_fingerprint_performed: policy.content_fingerprint_requested(),
            metadata_scout_millis: total_start.elapsed().as_millis(),
            byte_acquisition_millis,
            full_body_millis: 0,
        },
    })
}

fn partition_file_matches_source_format(path: &Path, source_format: LocalSourceFormat) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .and_then(LocalSourceFormat::from_extension)
        .is_some_and(|candidate| candidate == source_format)
}

fn parse_cast_target_dtype(raw: &str) -> Result<LogicalDType, ShardLoomError> {
    let trimmed = raw.trim();
    if let Some((precision, scale)) = parse_decimal_cast_target_dtype(trimmed)? {
        return Ok(decimal128_dtype(precision, scale));
    }
    match trimmed.to_ascii_lowercase().as_str() {
        "int64" | "bigint" | "integer" | "int" => Ok(LogicalDType::Int64),
        "uint64" => Ok(LogicalDType::UInt64),
        "float64" | "double" | "float" => Ok(LogicalDType::Float64),
        "utf8" | "string" | "text" | "varchar" => Ok(LogicalDType::Utf8),
        "boolean" | "bool" => Ok(LogicalDType::Boolean),
        "date32" | "date" => Ok(LogicalDType::Date32),
        "timestamp_micros" | "timestamp" => Ok(LogicalDType::TimestampMicros),
        "binary" | "blob" | "varbinary" => Ok(LogicalDType::Binary),
        _ => Err(unsupported_sql_error(
            "CAST target dtype must be one of int64, float64, utf8, boolean, date32, timestamp_micros, binary, or scoped decimal128(<precision>,<scale>)",
        )),
    }
}

fn parse_decimal_cast_target_dtype(raw: &str) -> Result<Option<(u8, u8)>, ShardLoomError> {
    let normalized = raw
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    let Some((name, args)) = decimal_cast_target_parts(&normalized) else {
        return Ok(None);
    };
    if !matches!(name, "decimal128" | "decimal" | "numeric") {
        return Ok(None);
    }
    let Some(args) = args else {
        return Ok(Some((38, 0)));
    };
    let parts = args.split(',').collect::<Vec<_>>();
    let [precision_raw, scale_raw] = parts.as_slice() else {
        return Err(unsupported_sql_error(
            "decimal CAST targets must use decimal128(<precision>,<scale>)",
        ));
    };
    let precision = precision_raw.parse::<u8>().map_err(|_| {
        unsupported_sql_error("decimal CAST precision must be an integer between 1 and 38")
    })?;
    let scale = scale_raw.parse::<u8>().map_err(|_| {
        unsupported_sql_error("decimal CAST scale must be an integer between 0 and precision")
    })?;
    if precision == 0 || precision > 38 || scale > precision {
        return Err(unsupported_sql_error(
            "decimal CAST precision/scale must satisfy 1 <= precision <= 38 and scale <= precision",
        ));
    }
    Ok(Some((precision, scale)))
}

fn decimal_cast_target_parts(normalized: &str) -> Option<(&str, Option<&str>)> {
    if let Some(open_index) = normalized.find('(') {
        let without_close = normalized.strip_suffix(')')?;
        let name = &without_close[..open_index];
        let args = &without_close[open_index + 1..];
        return Some((name, Some(args)));
    }
    Some((normalized, None))
}

fn parse_csv_scalar(raw: &str) -> ScalarValue {
    let value = raw.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("null") {
        ScalarValue::Null
    } else if value.eq_ignore_ascii_case("true") {
        ScalarValue::Boolean(true)
    } else if value.eq_ignore_ascii_case("false") {
        ScalarValue::Boolean(false)
    } else if let Ok(parsed) = value.parse::<i64>() {
        ScalarValue::Int64(parsed)
    } else if let Some(parsed) = csv_decimal_exponent_integer(value) {
        ScalarValue::Int64(parsed)
    } else if let Ok(parsed) = value.parse::<f64>() {
        if parsed.is_finite() {
            ScalarValue::Float64(parsed)
        } else {
            ScalarValue::Utf8(raw.to_string())
        }
    } else {
        ScalarValue::Utf8(raw.to_string())
    }
}

fn csv_decimal_exponent_integer(raw: &str) -> Option<i64> {
    const MAX_EXPONENT_MAGNITUDE: u32 = 76;

    let trimmed = raw.trim();
    let (negative, body) = match trimmed.as_bytes().first() {
        Some(b'-') => (true, &trimmed[1..]),
        Some(b'+') => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };
    let marker = body.find(['e', 'E'])?;
    let mantissa = &body[..marker];
    let exponent_raw = &body[marker + 1..];
    let exponent = exponent_raw.parse::<i32>().ok()?;
    if exponent.unsigned_abs() > MAX_EXPONENT_MAGNITUDE {
        return None;
    }
    let parts = mantissa.split('.').collect::<Vec<_>>();
    let (integer, fraction) = match parts.as_slice() {
        [integer] => (*integer, ""),
        [integer, fraction] => (*integer, *fraction),
        _ => return None,
    };
    if integer.is_empty() && fraction.is_empty() {
        return None;
    }
    let digits = format!("{integer}{fraction}");
    if digits.is_empty() || !digits.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let integer_len = i32::try_from(integer.len()).ok()?;
    let digit_len = i32::try_from(digits.len()).ok()?;
    let decimal_position = integer_len.checked_add(exponent)?;
    let normalized = if decimal_position < digit_len {
        let split_index = usize::try_from(decimal_position).ok()?;
        let fractional = &digits[split_index..];
        if fractional.chars().any(|ch| ch != '0') {
            return None;
        }
        digits[..split_index].to_string()
    } else {
        let trailing_zero_count = usize::try_from(decimal_position - digit_len).ok()?;
        format!("{}{}", digits, "0".repeat(trailing_zero_count))
    };
    let normalized = normalized.trim_start_matches('0');
    let normalized = if normalized.is_empty() {
        "0"
    } else {
        normalized
    };
    let value = normalized.parse::<i64>().ok()?;
    if negative {
        value.checked_neg()
    } else {
        Some(value)
    }
}

// A CSV record may span physical lines. Quote parity treats escaped double
// quotes as a balanced pair and preserves embedded LF/CRLF bytes verbatim.
fn read_csv_record(
    reader: &mut impl std::io::BufRead,
    record: &mut String,
) -> Result<usize, ShardLoomError> {
    use std::io::{BufRead as _, Read as _};
    const MAX_RECORD_BYTES: usize = 8 * 1024 * 1024;
    record.clear();
    let mut quoted = false;
    loop {
        let start = record.len();
        let remaining = MAX_RECORD_BYTES.saturating_add(1).saturating_sub(start);
        let read = reader
            .by_ref()
            .take(remaining as u64)
            .read_line(record)
            .map_err(|error| unsupported_sql_error(&format!("CSV record read failed: {error}")))?;
        if record.len() > MAX_RECORD_BYTES {
            return Err(unsupported_sql_error("CSV record exceeds 8 MiB admission"));
        }
        if read == 0 {
            return if quoted {
                Err(unsupported_sql_error("CSV quoted field is not closed"))
            } else {
                Ok(record.len())
            };
        }
        for byte in &record.as_bytes()[start..] {
            if *byte == b'"' {
                quoted = !quoted;
            }
        }
        if !quoted {
            return Ok(record.len());
        }
    }
}

fn split_csv_record(raw: &str) -> Result<Vec<String>, ShardLoomError> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut chars = raw.chars().peekable();
    let mut in_quote = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quote && chars.peek() == Some(&'"') => {
                current.push('"');
                let _ = chars.next();
            }
            '"' => in_quote = !in_quote,
            ',' if !in_quote => {
                values.push(current);
                current = String::new();
            }
            _ => current.push(ch),
        }
    }
    if in_quote {
        return Err(unsupported_sql_error("CSV quoted field is not closed"));
    }
    values.push(current);
    Ok(values)
}

fn validate_sql_identifier(value: &str) -> Result<(), ShardLoomError> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(unsupported_sql_error("SQL identifiers must not be empty"));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(unsupported_sql_error(
            "SQL identifiers must start with an ASCII letter or underscore",
        ));
    }
    if !chars.all(is_identifier_char) {
        return Err(unsupported_sql_error(
            "SQL identifiers may contain only ASCII letters, numbers, and underscores",
        ));
    }
    Ok(())
}

fn is_identifier_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_identifier_char(ch: char) -> bool {
    is_identifier_start(ch) || ch.is_ascii_digit()
}

fn fnv64_digest(value: &str) -> String {
    fnv64_digest_bytes(value.as_bytes())
}

fn fnv64_digest_bytes(value: &[u8]) -> String {
    let hash = update_fnv64_hash(0xcbf2_9ce4_8422_2325_u64, value);
    format!("fnv64:{hash:016x}")
}

fn update_fnv64_hash(mut hash: u64, value: &[u8]) -> u64 {
    for byte in value {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
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

fn unsupported_sql_error(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "{reason}; no fallback execution was attempted and external_engine_invoked=false"
    ))
}

#[cfg(all(test, feature = "vortex-write", feature = "universal-format-io", unix))]
#[path = "public_io_route_tests.rs"]
mod public_io_route_tests;
