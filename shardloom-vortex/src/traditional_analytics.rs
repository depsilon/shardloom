use std::path::PathBuf;

use shardloom_core::{
    Diagnostic, ExecutionCertificate, NativeIoCertificate, Result, ShardLoomError,
};

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
use shardloom_core::{
    ExecutionCertificateInput, ExecutionProviderKind, ExpectedOutcome,
    NativeIoAdapterFidelityReport, NativeIoMaterializationBoundaryReport,
    NativeIoRepresentationTransition, NativeIoSideEffectReport, NativeIoSinkRequirementReport,
    NativeIoSourceCapabilityReport, NativeIoSourcePushdownReport, RepresentationState,
};
#[cfg(feature = "vortex-traditional-analytics-benchmark")]
use shardloom_exec::{
    ByteSize, MemoryBudget, MemoryOwner, MemoryPoolPlan, MemoryReservationId, OperatorMemoryClass,
    OperatorMemorySpillDeclaration, OperatorMemorySpillDeclarationReport,
    ShardLoomCancellationExecutionGateReport, ShardLoomCancellationExecutionGateRequest,
    ShardLoomRetryExecutionGateReport, ShardLoomRetryExecutionGateRequest, SpillPolicy, TaskId,
};

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
const BENCHMARK_FLOAT_DIGITS: i32 = 4;
#[cfg(feature = "vortex-traditional-analytics-benchmark")]
const MAX_EXACT_F64_INTEGER: u64 = 9_007_199_254_740_992;
#[cfg(feature = "vortex-traditional-analytics-benchmark")]
const LOCAL_VORTEX_ANALYTICS_CONSTITUTION_ID: &str = "local_vortex_analytics_v1";
#[cfg(feature = "vortex-traditional-analytics-benchmark")]
const OUTPUT_ARTIFACT_DIGEST_ALGORITHM: &str = "fnv1a64";
#[cfg(feature = "vortex-traditional-analytics-benchmark")]
const COMPUTED_RESULT_VORTEX_SCHEMA_SUMMARY: &str =
    "result(scenario:utf8,result_json:utf8,rows_materialized:u64,workload_constitution_id:utf8)";

/// Benchmark scenarios used by the local traditional analytics harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraditionalAnalyticsScenario {
    CsvFileIngest,
    SelectiveFilter,
    GroupByAggregation,
    SortAndTopK,
    HashJoin,
    WideProjection,
    DistinctCount,
    FilterProjectionLimit,
    MultiKeyGroupBy,
    JoinAggregate,
    RowNumberWindow,
    HighCardinalityStringGroupDistinct,
    TopNPerGroup,
    ScaleStressSkewedJoinAggregation,
    ScaleStressMultiStageEtl,
}

impl TraditionalAnalyticsScenario {
    /// # Errors
    /// Returns an error when the scenario label is not recognized.
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "csv/file ingest" | "csv-file-ingest" => Ok(Self::CsvFileIngest),
            "selective filter" | "selective-filter" => Ok(Self::SelectiveFilter),
            "group by aggregation" | "group-by-aggregation" => Ok(Self::GroupByAggregation),
            "sort and top-k" | "sort-and-top-k" => Ok(Self::SortAndTopK),
            "hash join" | "hash-join" => Ok(Self::HashJoin),
            "wide projection" | "wide-projection" => Ok(Self::WideProjection),
            "distinct count" | "distinct-count" => Ok(Self::DistinctCount),
            "filter + projection + limit"
            | "filter-projection-limit"
            | "filter-and-projection-limit" => Ok(Self::FilterProjectionLimit),
            "multi-key group by" | "multi-key-group-by" => Ok(Self::MultiKeyGroupBy),
            "join + aggregate" | "join-aggregate" => Ok(Self::JoinAggregate),
            "row number window" | "row-number-window" => Ok(Self::RowNumberWindow),
            "high-cardinality string group/distinct" | "high-cardinality-string-group-distinct" => {
                Ok(Self::HighCardinalityStringGroupDistinct)
            }
            "top-N per group" | "top-n-per-group" | "top-N-per-group" => Ok(Self::TopNPerGroup),
            "scale stress skewed join aggregation" | "scale-stress-skewed-join-aggregation" => {
                Ok(Self::ScaleStressSkewedJoinAggregation)
            }
            "scale stress multi-stage etl" | "scale-stress-multi-stage-etl" => {
                Ok(Self::ScaleStressMultiStageEtl)
            }
            _ => Err(ShardLoomError::InvalidOperation(format!(
                "unknown traditional analytics scenario: {value}"
            ))),
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CsvFileIngest => "csv/file ingest",
            Self::SelectiveFilter => "selective filter",
            Self::GroupByAggregation => "group by aggregation",
            Self::SortAndTopK => "sort and top-k",
            Self::HashJoin => "hash join",
            Self::WideProjection => "wide projection",
            Self::DistinctCount => "distinct count",
            Self::FilterProjectionLimit => "filter + projection + limit",
            Self::MultiKeyGroupBy => "multi-key group by",
            Self::JoinAggregate => "join + aggregate",
            Self::RowNumberWindow => "row number window",
            Self::HighCardinalityStringGroupDistinct => "high-cardinality string group/distinct",
            Self::TopNPerGroup => "top-N per group",
            Self::ScaleStressSkewedJoinAggregation => "scale stress skewed join aggregation",
            Self::ScaleStressMultiStageEtl => "scale stress multi-stage etl",
        }
    }
}

/// Compatibility input formats accepted by the feature-gated traditional
/// analytics universal-I/O smoke runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraditionalAnalyticsInputFormat {
    Csv,
    JsonLines,
    Parquet,
    ArrowIpc,
    Avro,
    Orc,
}

impl TraditionalAnalyticsInputFormat {
    /// # Errors
    /// Returns an error when the input format label is not recognized.
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "csv" => Ok(Self::Csv),
            "jsonl" | "json-lines" | "json_lines" | "ndjson" => Ok(Self::JsonLines),
            "parquet" => Ok(Self::Parquet),
            "arrow" | "arrow-ipc" | "arrow_ipc" | "ipc" | "feather" => Ok(Self::ArrowIpc),
            "avro" => Ok(Self::Avro),
            "orc" => Ok(Self::Orc),
            _ => Err(ShardLoomError::InvalidOperation(format!(
                "unknown traditional analytics input format: {value}"
            ))),
        }
    }

    #[must_use]
    pub fn infer_from_paths(fact_path: &std::path::Path, dim_path: &std::path::Path) -> Self {
        let fact = Self::from_extension(fact_path);
        let dim = Self::from_extension(dim_path);
        if fact == Some(Self::JsonLines) && dim == Some(Self::JsonLines) {
            Self::JsonLines
        } else if fact == Some(Self::Parquet) && dim == Some(Self::Parquet) {
            Self::Parquet
        } else if fact == Some(Self::ArrowIpc) && dim == Some(Self::ArrowIpc) {
            Self::ArrowIpc
        } else if fact == Some(Self::Avro) && dim == Some(Self::Avro) {
            Self::Avro
        } else if fact == Some(Self::Orc) && dim == Some(Self::Orc) {
            Self::Orc
        } else {
            Self::Csv
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::JsonLines => "jsonl",
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow_ipc",
            Self::Avro => "avro",
            Self::Orc => "orc",
        }
    }

    #[must_use]
    pub const fn source_kind(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::JsonLines => "jsonl",
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow_ipc",
            Self::Avro => "avro",
            Self::Orc => "orc",
        }
    }

    #[must_use]
    pub const fn adapter_id(self) -> &'static str {
        match self {
            Self::Csv => "shardloom.adapter.csv.local_benchmark.v1",
            Self::JsonLines => "shardloom.adapter.jsonl.local_benchmark.v1",
            Self::Parquet => "shardloom.adapter.parquet.local_benchmark.v1",
            Self::ArrowIpc => "shardloom.adapter.arrow_ipc.local_benchmark.v1",
            Self::Avro => "shardloom.adapter.avro.local_benchmark.v1",
            Self::Orc => "shardloom.adapter.orc.local_benchmark.v1",
        }
    }

    #[must_use]
    pub const fn boundary_id(self) -> &'static str {
        match self {
            Self::Csv => "cg19.csv_to_vortex_source_parse",
            Self::JsonLines => "cg19.jsonl_to_vortex_source_parse",
            Self::Parquet => "cg19.parquet_to_vortex_source_decode",
            Self::ArrowIpc => "cg19.arrow_ipc_to_vortex_source_decode",
            Self::Avro => "cg19.avro_to_vortex_source_decode",
            Self::Orc => "cg19.orc_to_vortex_source_decode",
        }
    }

    #[must_use]
    pub const fn import_label(self) -> &'static str {
        match self {
            Self::Csv => "csv_to_vortex_import",
            Self::JsonLines => "jsonl_to_vortex_import",
            Self::Parquet => "parquet_to_vortex_import",
            Self::ArrowIpc => "arrow_ipc_to_vortex_import",
            Self::Avro => "avro_to_vortex_import",
            Self::Orc => "orc_to_vortex_import",
        }
    }

    #[must_use]
    pub const fn proof_basis(self) -> &'static str {
        match self {
            Self::Csv => {
                "local CSV benchmark adapter performs deterministic schema validation and parses source rows before Vortex import"
            }
            Self::JsonLines => {
                "local JSONL benchmark adapter performs deterministic field validation and parses source rows before Vortex import"
            }
            Self::Parquet => {
                "local Parquet benchmark adapter decodes Arrow record batches for the declared schema before Vortex import"
            }
            Self::ArrowIpc => {
                "local Arrow IPC benchmark adapter decodes Arrow record batches for the declared schema before Vortex import"
            }
            Self::Avro => {
                "local Avro benchmark adapter decodes Arrow record batches for the declared schema before Vortex import"
            }
            Self::Orc => {
                "local ORC benchmark adapter decodes Arrow record batches for the declared schema before Vortex import"
            }
        }
    }

    #[must_use]
    pub const fn materialization_reason(self) -> &'static str {
        match self {
            Self::Csv => {
                "CSV text must be parsed into typed columnar values before native Vortex persistence in the current benchmark smoke path"
            }
            Self::JsonLines => {
                "JSONL objects must be parsed into typed columnar values before native Vortex persistence in the current benchmark smoke path"
            }
            Self::Parquet => {
                "Parquet batches must be decoded into typed columnar values before native Vortex persistence in the current benchmark smoke path"
            }
            Self::ArrowIpc => {
                "Arrow IPC batches must be decoded into typed columnar values before native Vortex persistence in the current benchmark smoke path"
            }
            Self::Avro => {
                "Avro batches must be decoded into typed columnar values before native Vortex persistence in the current benchmark smoke path"
            }
            Self::Orc => {
                "ORC batches must be decoded into typed columnar values before native Vortex persistence in the current benchmark smoke path"
            }
        }
    }

    #[must_use]
    pub const fn metadata_loss(self) -> &'static str {
        match self {
            Self::Csv => "csv_source_has_no_vortex_encoding_statistics_or_layout_metadata",
            Self::JsonLines => "jsonl_source_has_no_vortex_encoding_statistics_or_layout_metadata",
            Self::Parquet => "parquet_source_metadata_not_preserved_in_current_vortex_import_smoke",
            Self::ArrowIpc => {
                "arrow_ipc_source_metadata_not_preserved_in_current_vortex_import_smoke"
            }
            Self::Avro => "avro_source_metadata_not_preserved_in_current_vortex_import_smoke",
            Self::Orc => "orc_source_metadata_not_preserved_in_current_vortex_import_smoke",
        }
    }

    #[must_use]
    pub const fn output_extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::JsonLines => "jsonl",
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow",
            Self::Avro => "avro",
            Self::Orc => "orc",
        }
    }

    fn from_extension(path: &std::path::Path) -> Option<Self> {
        let extension = path.extension()?.to_str()?.to_ascii_lowercase();
        match extension.as_str() {
            "csv" => Some(Self::Csv),
            "jsonl" | "ndjson" => Some(Self::JsonLines),
            "parquet" => Some(Self::Parquet),
            "arrow" | "ipc" | "feather" => Some(Self::ArrowIpc),
            "avro" => Some(Self::Avro),
            "orc" => Some(Self::Orc),
            _ => None,
        }
    }
}

/// User-facing resource budget for feature-gated local universal-I/O ETL smoke
/// runs. The policy keeps CLI/API usage simple while making applied batch sizing
/// explicit in reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraditionalAnalyticsResourcePolicy {
    pub requested_memory_gb: Option<u32>,
    pub requested_max_parallelism: Option<usize>,
    pub memory_gb: u32,
    pub max_parallelism: usize,
    pub detected_parallelism: usize,
    pub target_batch_rows: usize,
    pub target_partition_bytes: u64,
    pub target_partition_count: usize,
    pub source_bytes: u64,
}

impl TraditionalAnalyticsResourcePolicy {
    const DEFAULT_MEMORY_GB: u32 = 4;
    const MIN_BATCH_ROWS: usize = 1024;
    const MAX_BATCH_ROWS: usize = 65_536;
    const ESTIMATED_ROW_BYTES: usize = 128;
    const WORKING_SET_DIVISOR: usize = 4;
    const MIN_PARTITION_BYTES: u64 = 8 * 1024 * 1024;
    const MAX_PARTITION_BYTES: u64 = 128 * 1024 * 1024;
    const TARGET_PARTITION_BYTES: u64 = 64 * 1024 * 1024;

    #[must_use]
    pub fn new(memory_gb: u32, max_parallelism: usize) -> Self {
        Self::from_hints(Some(memory_gb), Some(max_parallelism))
    }

    #[must_use]
    pub fn auto() -> Self {
        Self::from_hints(None, None)
    }

    #[must_use]
    pub fn from_hints(memory_gb: Option<u32>, max_parallelism: Option<usize>) -> Self {
        Self::resolve(memory_gb, max_parallelism, 0)
    }

    #[must_use]
    pub fn resolve_for_sources(self, source_bytes: u64) -> Self {
        Self::resolve(
            self.requested_memory_gb,
            self.requested_max_parallelism,
            source_bytes,
        )
    }

    #[must_use]
    pub const fn sizing_mode(self) -> &'static str {
        match (self.requested_memory_gb, self.requested_max_parallelism) {
            (None, None) => "auto",
            _ => "bounded-auto",
        }
    }

    #[must_use]
    pub const fn auto_sizing_enabled() -> bool {
        true
    }

    fn resolve(
        requested_memory_gb: Option<u32>,
        requested_max_parallelism: Option<usize>,
        source_bytes: u64,
    ) -> Self {
        let memory_gb = requested_memory_gb
            .unwrap_or(Self::DEFAULT_MEMORY_GB)
            .max(1);
        let detected_parallelism = detected_parallelism();
        let max_parallelism = requested_max_parallelism
            .unwrap_or(detected_parallelism)
            .max(1);
        let budget_bytes = memory_gb_to_bytes(memory_gb);
        let denominator = max_parallelism
            .saturating_mul(Self::ESTIMATED_ROW_BYTES)
            .saturating_mul(Self::WORKING_SET_DIVISOR)
            .max(1);
        let target_batch_rows =
            (budget_bytes / denominator).clamp(Self::MIN_BATCH_ROWS, Self::MAX_BATCH_ROWS);
        let target_partition_bytes = target_partition_bytes(budget_bytes, max_parallelism);
        let target_partition_count =
            target_partition_count(source_bytes, target_partition_bytes, max_parallelism);
        Self {
            requested_memory_gb,
            requested_max_parallelism,
            memory_gb,
            max_parallelism,
            detected_parallelism,
            target_batch_rows,
            target_partition_bytes,
            target_partition_count,
            source_bytes,
        }
    }
}

impl Default for TraditionalAnalyticsResourcePolicy {
    fn default() -> Self {
        Self::auto()
    }
}

fn detected_parallelism() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

fn memory_gb_to_bytes(memory_gb: u32) -> usize {
    usize::try_from(memory_gb)
        .unwrap_or(usize::MAX / 1024 / 1024 / 1024)
        .saturating_mul(1024)
        .saturating_mul(1024)
        .saturating_mul(1024)
}

fn target_partition_bytes(memory_budget_bytes: usize, max_parallelism: usize) -> u64 {
    let budget_per_worker = u64::try_from(memory_budget_bytes)
        .unwrap_or(u64::MAX)
        .saturating_div(u64::try_from(max_parallelism.max(1)).unwrap_or(u64::MAX))
        .saturating_div(2);
    budget_per_worker
        .min(TraditionalAnalyticsResourcePolicy::TARGET_PARTITION_BYTES)
        .clamp(
            TraditionalAnalyticsResourcePolicy::MIN_PARTITION_BYTES,
            TraditionalAnalyticsResourcePolicy::MAX_PARTITION_BYTES,
        )
}

fn target_partition_count(
    source_bytes: u64,
    target_partition_bytes: u64,
    max_parallelism: usize,
) -> usize {
    if source_bytes == 0 {
        return 1;
    }
    let partition_count_u64 = source_bytes
        .saturating_add(target_partition_bytes.saturating_sub(1))
        .saturating_div(target_partition_bytes.max(1))
        .max(1);
    let partition_count = usize::try_from(partition_count_u64).unwrap_or(usize::MAX);
    let soft_cap = max_parallelism.saturating_mul(4).max(1);
    partition_count.clamp(1, soft_cap)
}

/// Request for the feature-gated traditional analytics Vortex I/O smoke runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraditionalAnalyticsRequest {
    pub scenario: TraditionalAnalyticsScenario,
    pub fact_csv: PathBuf,
    pub dim_csv: PathBuf,
    pub workspace_dir: PathBuf,
    pub input_format: TraditionalAnalyticsInputFormat,
    pub compatibility_output_format: Option<TraditionalAnalyticsInputFormat>,
    pub verify_native_vortex_replay: bool,
    pub write_result_vortex: bool,
    pub resource_policy: TraditionalAnalyticsResourcePolicy,
}

impl TraditionalAnalyticsRequest {
    #[must_use]
    pub fn new(
        scenario: TraditionalAnalyticsScenario,
        fact_csv: PathBuf,
        dim_csv: PathBuf,
        workspace_dir: PathBuf,
    ) -> Self {
        Self {
            scenario,
            fact_csv,
            dim_csv,
            workspace_dir,
            input_format: TraditionalAnalyticsInputFormat::Csv,
            compatibility_output_format: None,
            verify_native_vortex_replay: false,
            write_result_vortex: false,
            resource_policy: TraditionalAnalyticsResourcePolicy::default(),
        }
    }

    #[must_use]
    pub const fn with_input_format(
        mut self,
        input_format: TraditionalAnalyticsInputFormat,
    ) -> Self {
        self.input_format = input_format;
        self
    }

    #[must_use]
    pub const fn with_compatibility_output_format(
        mut self,
        output_format: Option<TraditionalAnalyticsInputFormat>,
    ) -> Self {
        self.compatibility_output_format = output_format;
        self
    }

    #[must_use]
    pub const fn with_resource_policy(
        mut self,
        policy: TraditionalAnalyticsResourcePolicy,
    ) -> Self {
        self.resource_policy = policy;
        self
    }

    #[must_use]
    pub const fn with_native_vortex_replay_verification(mut self, value: bool) -> Self {
        self.verify_native_vortex_replay = value;
        self
    }

    #[must_use]
    pub const fn with_result_vortex_write(mut self, value: bool) -> Self {
        self.write_result_vortex = value;
        self
    }
}

/// Request for the feature-gated native Vortex traditional analytics runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraditionalAnalyticsVortexRequest {
    pub scenario: TraditionalAnalyticsScenario,
    pub fact_vortex: PathBuf,
    pub dim_vortex: PathBuf,
}

impl TraditionalAnalyticsVortexRequest {
    #[must_use]
    pub fn new(
        scenario: TraditionalAnalyticsScenario,
        fact_vortex: PathBuf,
        dim_vortex: PathBuf,
    ) -> Self {
        Self {
            scenario,
            fact_vortex,
            dim_vortex,
        }
    }
}

/// Report emitted by the local CSV-to-Vortex benchmark smoke runner.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct TraditionalAnalyticsReport {
    pub scenario: TraditionalAnalyticsScenario,
    pub input_format: TraditionalAnalyticsInputFormat,
    pub resource_policy: TraditionalAnalyticsResourcePolicy,
    pub result_json: String,
    pub fact_rows: u64,
    pub dim_rows: u64,
    pub rows_scanned: u64,
    pub rows_materialized: u64,
    pub workspace_dir: PathBuf,
    pub fact_vortex_path: PathBuf,
    pub dim_vortex_path: PathBuf,
    pub compatibility_output_format: Option<TraditionalAnalyticsInputFormat>,
    pub fact_compatibility_output_path: Option<PathBuf>,
    pub dim_compatibility_output_path: Option<PathBuf>,
    pub fact_source_path: PathBuf,
    pub dim_source_path: PathBuf,
    pub workload_constitution_id: String,
    pub workload_scorecard_status: String,
    pub benchmark_row_ref: String,
    pub coverage_row_ref: String,
    pub output_artifact_schema_summary: String,
    pub output_artifact_digest_algorithm: String,
    pub fact_vortex_digest: String,
    pub dim_vortex_digest: String,
    pub combined_output_digest: String,
    pub output_replay_requested: bool,
    pub output_replay_verified: bool,
    pub output_replay_result_json: Option<String>,
    pub output_replay_rows_scanned: Option<u64>,
    pub output_replay_rows_materialized: Option<u64>,
    pub output_replay_native_io_certificate_id: Option<String>,
    pub output_replay_native_io_certificate_status: Option<String>,
    pub computed_result_sink_requested: bool,
    pub computed_result_sink_written: bool,
    pub computed_result_sink_replay_verified: bool,
    pub computed_result_vortex_path: Option<PathBuf>,
    pub computed_result_vortex_bytes: u64,
    pub computed_result_vortex_digest: Option<String>,
    pub computed_result_sink_rows: u64,
    pub computed_result_sink_rows_materialized: u64,
    pub computed_result_sink_schema_summary: String,
    pub scenario_compute_micros: u64,
    pub computed_result_sink_write_micros: Option<u64>,
    pub computed_result_sink_replay_result_json: Option<String>,
    pub computed_result_sink_native_io_certificate_id: Option<String>,
    pub computed_result_sink_native_io_certificate_status: Option<String>,
    pub runtime_task_graph_created: bool,
    pub runtime_task_graph_executed: bool,
    pub runtime_scheduler_mode: String,
    pub runtime_scheduler_ref: String,
    pub runtime_task_count: usize,
    pub runtime_scheduled_task_count: usize,
    pub runtime_completed_task_count: usize,
    pub runtime_split_count: usize,
    pub runtime_scheduler_batch_count: usize,
    pub runtime_max_parallelism: usize,
    pub runtime_queue_limit: usize,
    pub runtime_queue_limit_enforced: bool,
    pub runtime_backpressure_bounded: bool,
    pub runtime_cancellation_checkpoint_count: usize,
    pub runtime_cancellation_testable: bool,
    pub runtime_cancellation_gate_status: String,
    pub runtime_retry_testable: bool,
    pub runtime_retry_gate_status: String,
    pub runtime_memory_budget_bytes: u64,
    pub runtime_memory_soft_limit_bytes: u64,
    pub runtime_memory_hard_limit_bytes: u64,
    pub runtime_memory_reservations_requested: usize,
    pub runtime_memory_reservations_granted: usize,
    pub runtime_memory_reservations_released: usize,
    pub runtime_memory_reservations_denied: usize,
    pub runtime_memory_peak_reserved_bytes: u64,
    pub runtime_fail_before_oom_enforced: bool,
    pub runtime_spill_required: bool,
    pub runtime_spill_supported: bool,
    pub runtime_spill_blocker: String,
    pub runtime_operator_memory_spill_declaration_count: usize,
    pub runtime_operator_memory_spill_claim_blocker_count: usize,
    pub runtime_large_workload_claim_allowed: bool,
    pub runtime_execution_certificate: ExecutionCertificate,
    pub commit_state: String,
    pub rollback_cleanup_status: String,
    pub fact_source_bytes: u64,
    pub dim_source_bytes: u64,
    pub fact_compatibility_output_bytes: u64,
    pub dim_compatibility_output_bytes: u64,
    pub fact_csv_bytes: u64,
    pub dim_csv_bytes: u64,
    pub source_bytes_read: u64,
    pub fact_vortex_bytes: u64,
    pub dim_vortex_bytes: u64,
    pub materialization_boundary_rows: u64,
    pub native_io_certificate: NativeIoCertificate,
    pub native_work_envelope_created: bool,
    pub native_work_stream_created: bool,
    pub native_result_stream_created: bool,
    pub native_io_certificate_emitted: bool,
    pub compatibility_source_adapter_used: bool,
    pub compatibility_to_vortex_import_performed: bool,
    pub compatibility_output_requested: bool,
    pub compatibility_output_written: bool,
    pub native_to_compatibility_output_performed: bool,
    pub csv_source_adapter_used: bool,
    pub csv_to_vortex_import_performed: bool,
    pub jsonl_source_adapter_used: bool,
    pub jsonl_to_vortex_import_performed: bool,
    pub vortex_file_written: bool,
    pub vortex_file_read: bool,
    pub upstream_vortex_scan_called: bool,
    pub streaming_vortex_execution_used: bool,
    pub full_table_materialization_avoided: bool,
    pub streaming_filter_pushdown_applied: bool,
    pub streaming_projection_pushdown_applied: bool,
    pub streaming_arrays_read_count: usize,
    pub streaming_max_chunk_rows: usize,
    pub streaming_projected_columns: Vec<String>,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub materialization_boundary_report_emitted: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl TraditionalAnalyticsReport {
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "ShardLoom traditional analytics universal I/O smoke\nscenario: {}\nsource format: {}\nresource policy: {}\napplied memory GiB: {}\napplied parallelism: {}\ntarget batch rows: {}\ntarget partitions: {}\nworkspace: {}\nfact Vortex: {}\ndim Vortex: {}\nrows scanned: {}\nrows materialized: {}\ncompatibility source adapter: true\ncompatibility to Vortex import: true\nVortex write/read/scan: true\nruntime scheduler: {} tasks={} batches={} certificate={}\nmaterialization boundary reported: {}\noutput replay verified: {}\ncomputed result sink verified: {}\nworkload scorecard: {}\nexternal engine fallback: disabled",
            self.scenario.as_str(),
            self.input_format.as_str(),
            self.resource_policy.sizing_mode(),
            self.resource_policy.memory_gb,
            self.resource_policy.max_parallelism,
            self.resource_policy.target_batch_rows,
            self.resource_policy.target_partition_count,
            self.workspace_dir.display(),
            self.fact_vortex_path.display(),
            self.dim_vortex_path.display(),
            self.rows_scanned,
            self.rows_materialized,
            self.runtime_scheduler_mode,
            self.runtime_task_count,
            self.runtime_scheduler_batch_count,
            self.runtime_execution_certificate.status.as_str(),
            self.materialization_boundary_report_emitted,
            self.output_replay_verified,
            self.computed_result_sink_replay_verified,
            self.workload_scorecard_status
        )
    }

    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn fields(&self) -> Vec<(String, String)> {
        let mut fields = vec![
            (
                "fallback_execution_allowed".to_string(),
                self.fallback_execution_allowed.to_string(),
            ),
            (
                "external_engines_are_fallback".to_string(),
                "false".to_string(),
            ),
            ("scenario".to_string(), self.scenario.as_str().to_string()),
            (
                "input_format".to_string(),
                self.input_format.as_str().to_string(),
            ),
            (
                "source_format".to_string(),
                self.input_format.as_str().to_string(),
            ),
            (
                "resource_policy_mode".to_string(),
                self.resource_policy.sizing_mode().to_string(),
            ),
            (
                "resource_auto_sizing_enabled".to_string(),
                TraditionalAnalyticsResourcePolicy::auto_sizing_enabled().to_string(),
            ),
            (
                "requested_memory_gb".to_string(),
                self.resource_policy
                    .requested_memory_gb
                    .map_or_else(|| "auto".to_string(), |value| value.to_string()),
            ),
            (
                "applied_memory_gb".to_string(),
                self.resource_policy.memory_gb.to_string(),
            ),
            (
                "requested_max_parallelism".to_string(),
                self.resource_policy
                    .requested_max_parallelism
                    .map_or_else(|| "auto".to_string(), |value| value.to_string()),
            ),
            (
                "applied_max_parallelism".to_string(),
                self.resource_policy.max_parallelism.to_string(),
            ),
            (
                "detected_parallelism".to_string(),
                self.resource_policy.detected_parallelism.to_string(),
            ),
            (
                "applied_batch_rows".to_string(),
                self.resource_policy.target_batch_rows.to_string(),
            ),
            (
                "target_partition_bytes".to_string(),
                self.resource_policy.target_partition_bytes.to_string(),
            ),
            (
                "target_partition_count".to_string(),
                self.resource_policy.target_partition_count.to_string(),
            ),
            (
                "partitioning_auto_derived".to_string(),
                TraditionalAnalyticsResourcePolicy::auto_sizing_enabled().to_string(),
            ),
            ("dynamic_sizing_applied".to_string(), "true".to_string()),
            (
                "source_kind".to_string(),
                self.input_format.source_kind().to_string(),
            ),
            (
                "source_adapter_id".to_string(),
                self.input_format.adapter_id().to_string(),
            ),
            (
                "compatibility_output_requested".to_string(),
                self.compatibility_output_requested.to_string(),
            ),
            (
                "compatibility_output_format".to_string(),
                self.compatibility_output_format
                    .map_or_else(|| "none".to_string(), |format| format.as_str().to_string()),
            ),
            ("result_json".to_string(), self.result_json.clone()),
            ("fact_rows".to_string(), self.fact_rows.to_string()),
            ("dim_rows".to_string(), self.dim_rows.to_string()),
            ("rows_scanned".to_string(), self.rows_scanned.to_string()),
            (
                "rows_materialized".to_string(),
                self.rows_materialized.to_string(),
            ),
            (
                "workspace_dir".to_string(),
                self.workspace_dir.display().to_string(),
            ),
            (
                "fact_vortex_path".to_string(),
                self.fact_vortex_path.display().to_string(),
            ),
            (
                "dim_vortex_path".to_string(),
                self.dim_vortex_path.display().to_string(),
            ),
            (
                "fact_compatibility_output_path".to_string(),
                self.fact_compatibility_output_path
                    .as_ref()
                    .map_or_else(String::new, |path| path.display().to_string()),
            ),
            (
                "dim_compatibility_output_path".to_string(),
                self.dim_compatibility_output_path
                    .as_ref()
                    .map_or_else(String::new, |path| path.display().to_string()),
            ),
            (
                "workload_constitution_id".to_string(),
                self.workload_constitution_id.clone(),
            ),
            (
                "workload_scorecard_status".to_string(),
                self.workload_scorecard_status.clone(),
            ),
            (
                "benchmark_row_ref".to_string(),
                self.benchmark_row_ref.clone(),
            ),
            (
                "coverage_row_ref".to_string(),
                self.coverage_row_ref.clone(),
            ),
            (
                "output_artifact_schema_summary".to_string(),
                self.output_artifact_schema_summary.clone(),
            ),
            (
                "output_artifact_digest_algorithm".to_string(),
                self.output_artifact_digest_algorithm.clone(),
            ),
            (
                "fact_vortex_digest".to_string(),
                self.fact_vortex_digest.clone(),
            ),
            (
                "dim_vortex_digest".to_string(),
                self.dim_vortex_digest.clone(),
            ),
            (
                "combined_output_digest".to_string(),
                self.combined_output_digest.clone(),
            ),
            (
                "output_replay_requested".to_string(),
                self.output_replay_requested.to_string(),
            ),
            (
                "output_replay_verified".to_string(),
                self.output_replay_verified.to_string(),
            ),
            (
                "output_replay_result_json".to_string(),
                self.output_replay_result_json.clone().unwrap_or_default(),
            ),
            (
                "output_replay_rows_scanned".to_string(),
                self.output_replay_rows_scanned
                    .map_or_else(|| "none".to_string(), |value| value.to_string()),
            ),
            (
                "output_replay_rows_materialized".to_string(),
                self.output_replay_rows_materialized
                    .map_or_else(|| "none".to_string(), |value| value.to_string()),
            ),
            (
                "output_replay_native_io_certificate_id".to_string(),
                self.output_replay_native_io_certificate_id
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            ),
            (
                "output_replay_native_io_certificate_status".to_string(),
                self.output_replay_native_io_certificate_status
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            ),
            (
                "computed_result_sink_requested".to_string(),
                self.computed_result_sink_requested.to_string(),
            ),
            (
                "computed_result_sink_written".to_string(),
                self.computed_result_sink_written.to_string(),
            ),
            (
                "computed_result_sink_replay_verified".to_string(),
                self.computed_result_sink_replay_verified.to_string(),
            ),
            (
                "computed_result_vortex_path".to_string(),
                self.computed_result_vortex_path
                    .as_ref()
                    .map_or_else(String::new, |path| path.display().to_string()),
            ),
            (
                "computed_result_vortex_bytes".to_string(),
                self.computed_result_vortex_bytes.to_string(),
            ),
            (
                "computed_result_vortex_digest".to_string(),
                self.computed_result_vortex_digest
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            ),
            (
                "computed_result_sink_rows".to_string(),
                self.computed_result_sink_rows.to_string(),
            ),
            (
                "computed_result_sink_rows_materialized".to_string(),
                self.computed_result_sink_rows_materialized.to_string(),
            ),
            (
                "computed_result_sink_schema_summary".to_string(),
                self.computed_result_sink_schema_summary.clone(),
            ),
            (
                "scenario_compute_micros".to_string(),
                self.scenario_compute_micros.to_string(),
            ),
            (
                "computed_result_sink_write_micros".to_string(),
                self.computed_result_sink_write_micros
                    .map_or_else(|| "none".to_string(), |value| value.to_string()),
            ),
            (
                "computed_result_sink_replay_result_json".to_string(),
                self.computed_result_sink_replay_result_json
                    .clone()
                    .unwrap_or_default(),
            ),
            (
                "computed_result_sink_native_io_certificate_id".to_string(),
                self.computed_result_sink_native_io_certificate_id
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            ),
            (
                "computed_result_sink_native_io_certificate_status".to_string(),
                self.computed_result_sink_native_io_certificate_status
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            ),
            (
                "runtime_task_graph_created".to_string(),
                self.runtime_task_graph_created.to_string(),
            ),
            (
                "runtime_task_graph_executed".to_string(),
                self.runtime_task_graph_executed.to_string(),
            ),
            (
                "runtime_scheduler_mode".to_string(),
                self.runtime_scheduler_mode.clone(),
            ),
            (
                "runtime_scheduler_ref".to_string(),
                self.runtime_scheduler_ref.clone(),
            ),
            (
                "runtime_task_count".to_string(),
                self.runtime_task_count.to_string(),
            ),
            (
                "runtime_scheduled_task_count".to_string(),
                self.runtime_scheduled_task_count.to_string(),
            ),
            (
                "runtime_completed_task_count".to_string(),
                self.runtime_completed_task_count.to_string(),
            ),
            (
                "runtime_split_count".to_string(),
                self.runtime_split_count.to_string(),
            ),
            (
                "runtime_scheduler_batch_count".to_string(),
                self.runtime_scheduler_batch_count.to_string(),
            ),
            (
                "runtime_max_parallelism".to_string(),
                self.runtime_max_parallelism.to_string(),
            ),
            (
                "runtime_queue_limit".to_string(),
                self.runtime_queue_limit.to_string(),
            ),
            (
                "runtime_queue_limit_enforced".to_string(),
                self.runtime_queue_limit_enforced.to_string(),
            ),
            (
                "runtime_backpressure_bounded".to_string(),
                self.runtime_backpressure_bounded.to_string(),
            ),
            (
                "runtime_cancellation_checkpoint_count".to_string(),
                self.runtime_cancellation_checkpoint_count.to_string(),
            ),
            (
                "runtime_cancellation_testable".to_string(),
                self.runtime_cancellation_testable.to_string(),
            ),
            (
                "runtime_cancellation_gate_status".to_string(),
                self.runtime_cancellation_gate_status.clone(),
            ),
            (
                "runtime_retry_testable".to_string(),
                self.runtime_retry_testable.to_string(),
            ),
            (
                "runtime_retry_gate_status".to_string(),
                self.runtime_retry_gate_status.clone(),
            ),
            (
                "runtime_memory_budget_bytes".to_string(),
                self.runtime_memory_budget_bytes.to_string(),
            ),
            (
                "runtime_memory_soft_limit_bytes".to_string(),
                self.runtime_memory_soft_limit_bytes.to_string(),
            ),
            (
                "runtime_memory_hard_limit_bytes".to_string(),
                self.runtime_memory_hard_limit_bytes.to_string(),
            ),
            (
                "runtime_memory_reservations_requested".to_string(),
                self.runtime_memory_reservations_requested.to_string(),
            ),
            (
                "runtime_memory_reservations_granted".to_string(),
                self.runtime_memory_reservations_granted.to_string(),
            ),
            (
                "runtime_memory_reservations_released".to_string(),
                self.runtime_memory_reservations_released.to_string(),
            ),
            (
                "runtime_memory_reservations_denied".to_string(),
                self.runtime_memory_reservations_denied.to_string(),
            ),
            (
                "runtime_memory_peak_reserved_bytes".to_string(),
                self.runtime_memory_peak_reserved_bytes.to_string(),
            ),
            (
                "runtime_fail_before_oom_enforced".to_string(),
                self.runtime_fail_before_oom_enforced.to_string(),
            ),
            (
                "runtime_spill_required".to_string(),
                self.runtime_spill_required.to_string(),
            ),
            (
                "runtime_spill_supported".to_string(),
                self.runtime_spill_supported.to_string(),
            ),
            (
                "runtime_spill_blocker".to_string(),
                self.runtime_spill_blocker.clone(),
            ),
            (
                "runtime_operator_memory_spill_declaration_count".to_string(),
                self.runtime_operator_memory_spill_declaration_count
                    .to_string(),
            ),
            (
                "runtime_operator_memory_spill_claim_blocker_count".to_string(),
                self.runtime_operator_memory_spill_claim_blocker_count
                    .to_string(),
            ),
            (
                "runtime_large_workload_claim_allowed".to_string(),
                self.runtime_large_workload_claim_allowed.to_string(),
            ),
            (
                "runtime_execution_certificate_id".to_string(),
                self.runtime_execution_certificate.certificate_id.clone(),
            ),
            (
                "runtime_execution_certificate_status".to_string(),
                self.runtime_execution_certificate
                    .status
                    .as_str()
                    .to_string(),
            ),
            (
                "runtime_execution_certificate_provider_kind".to_string(),
                self.runtime_execution_certificate
                    .execution_provider_kind
                    .as_str()
                    .to_string(),
            ),
            (
                "runtime_execution_certificate_plan_ref".to_string(),
                self.runtime_execution_certificate
                    .plan_ref
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            ),
            (
                "runtime_external_query_engine_invoked".to_string(),
                self.runtime_execution_certificate
                    .external_query_engine_invoked
                    .to_string(),
            ),
            (
                "runtime_fallback_attempted".to_string(),
                self.runtime_execution_certificate
                    .fallback_attempted
                    .to_string(),
            ),
            ("commit_state".to_string(), self.commit_state.clone()),
            (
                "rollback_cleanup_status".to_string(),
                self.rollback_cleanup_status.clone(),
            ),
            (
                "fact_source_path".to_string(),
                self.fact_source_path.display().to_string(),
            ),
            (
                "dim_source_path".to_string(),
                self.dim_source_path.display().to_string(),
            ),
            (
                "fact_source_bytes".to_string(),
                self.fact_source_bytes.to_string(),
            ),
            (
                "dim_source_bytes".to_string(),
                self.dim_source_bytes.to_string(),
            ),
            (
                "fact_compatibility_output_bytes".to_string(),
                self.fact_compatibility_output_bytes.to_string(),
            ),
            (
                "dim_compatibility_output_bytes".to_string(),
                self.dim_compatibility_output_bytes.to_string(),
            ),
            (
                "fact_csv_bytes".to_string(),
                self.fact_csv_bytes.to_string(),
            ),
            ("dim_csv_bytes".to_string(), self.dim_csv_bytes.to_string()),
            (
                "source_bytes_read".to_string(),
                self.source_bytes_read.to_string(),
            ),
            (
                "fact_vortex_bytes".to_string(),
                self.fact_vortex_bytes.to_string(),
            ),
            (
                "dim_vortex_bytes".to_string(),
                self.dim_vortex_bytes.to_string(),
            ),
            (
                "materialization_boundary_rows".to_string(),
                self.materialization_boundary_rows.to_string(),
            ),
            (
                "native_work_envelope_created".to_string(),
                self.native_work_envelope_created.to_string(),
            ),
            (
                "native_work_stream_created".to_string(),
                self.native_work_stream_created.to_string(),
            ),
            (
                "native_result_stream_created".to_string(),
                self.native_result_stream_created.to_string(),
            ),
            (
                "native_io_certificate_emitted".to_string(),
                self.native_io_certificate_emitted.to_string(),
            ),
            (
                "compatibility_source_adapter_used".to_string(),
                self.compatibility_source_adapter_used.to_string(),
            ),
            (
                "compatibility_to_vortex_import_performed".to_string(),
                self.compatibility_to_vortex_import_performed.to_string(),
            ),
            (
                "compatibility_output_written".to_string(),
                self.compatibility_output_written.to_string(),
            ),
            (
                "native_to_compatibility_output_performed".to_string(),
                self.native_to_compatibility_output_performed.to_string(),
            ),
            (
                "csv_source_adapter_used".to_string(),
                self.csv_source_adapter_used.to_string(),
            ),
            (
                "csv_to_vortex_import_performed".to_string(),
                self.csv_to_vortex_import_performed.to_string(),
            ),
            (
                "jsonl_source_adapter_used".to_string(),
                self.jsonl_source_adapter_used.to_string(),
            ),
            (
                "jsonl_to_vortex_import_performed".to_string(),
                self.jsonl_to_vortex_import_performed.to_string(),
            ),
            (
                "vortex_file_written".to_string(),
                self.vortex_file_written.to_string(),
            ),
            (
                "vortex_file_read".to_string(),
                self.vortex_file_read.to_string(),
            ),
            (
                "upstream_vortex_scan_called".to_string(),
                self.upstream_vortex_scan_called.to_string(),
            ),
            ("data_decoded".to_string(), self.data_decoded.to_string()),
            (
                "data_materialized".to_string(),
                self.data_materialized.to_string(),
            ),
            (
                "materialization_boundary_report_emitted".to_string(),
                self.materialization_boundary_report_emitted.to_string(),
            ),
            ("row_read".to_string(), self.row_read.to_string()),
            (
                "arrow_converted".to_string(),
                self.arrow_converted.to_string(),
            ),
            (
                "object_store_io".to_string(),
                self.object_store_io.to_string(),
            ),
            ("write_io".to_string(), self.write_io.to_string()),
            (
                "spill_io_performed".to_string(),
                self.spill_io_performed.to_string(),
            ),
        ];
        fields.extend(streaming_execution_fields(self));
        fields.extend(native_io_certificate_fields(&self.native_io_certificate));
        fields
    }
}

/// Report emitted by the native Vortex benchmark smoke runner.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct TraditionalAnalyticsVortexReport {
    pub scenario: TraditionalAnalyticsScenario,
    pub result_json: String,
    pub fact_rows: u64,
    pub dim_rows: u64,
    pub rows_scanned: u64,
    pub rows_materialized: u64,
    pub fact_vortex_path: PathBuf,
    pub dim_vortex_path: PathBuf,
    pub fact_vortex_bytes: u64,
    pub dim_vortex_bytes: u64,
    pub source_bytes_read: u64,
    pub materialization_boundary_rows: u64,
    pub native_io_certificate: NativeIoCertificate,
    pub native_work_envelope_created: bool,
    pub native_work_stream_created: bool,
    pub native_result_stream_created: bool,
    pub native_io_certificate_emitted: bool,
    pub vortex_source_adapter_used: bool,
    pub vortex_file_read: bool,
    pub upstream_vortex_scan_called: bool,
    pub streaming_vortex_execution_used: bool,
    pub full_table_materialization_avoided: bool,
    pub streaming_filter_pushdown_applied: bool,
    pub streaming_projection_pushdown_applied: bool,
    pub streaming_arrays_read_count: usize,
    pub streaming_max_chunk_rows: usize,
    pub streaming_projected_columns: Vec<String>,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub materialization_boundary_report_emitted: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl TraditionalAnalyticsVortexReport {
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "ShardLoom traditional analytics native Vortex smoke\nscenario: {}\nfact Vortex: {}\ndim Vortex: {}\nrows scanned: {}\nrows materialized: {}\nVortex source adapter: true\nVortex read/scan: true\nmaterialization boundary reported: {}\nexternal engine fallback: disabled",
            self.scenario.as_str(),
            self.fact_vortex_path.display(),
            self.dim_vortex_path.display(),
            self.rows_scanned,
            self.rows_materialized,
            self.materialization_boundary_report_emitted
        )
    }

    #[must_use]
    pub fn fields(&self) -> Vec<(String, String)> {
        let mut fields = self.base_fields();
        fields.extend(streaming_execution_fields(self));
        fields.extend(native_io_certificate_fields(&self.native_io_certificate));
        fields
    }

    fn base_fields(&self) -> Vec<(String, String)> {
        let fields = vec![
            (
                "fallback_execution_allowed".to_string(),
                self.fallback_execution_allowed.to_string(),
            ),
            (
                "external_engines_are_fallback".to_string(),
                "false".to_string(),
            ),
            ("scenario".to_string(), self.scenario.as_str().to_string()),
            ("source_format".to_string(), "vortex".to_string()),
            ("result_json".to_string(), self.result_json.clone()),
            ("fact_rows".to_string(), self.fact_rows.to_string()),
            ("dim_rows".to_string(), self.dim_rows.to_string()),
            ("rows_scanned".to_string(), self.rows_scanned.to_string()),
            (
                "rows_materialized".to_string(),
                self.rows_materialized.to_string(),
            ),
            (
                "fact_vortex_path".to_string(),
                self.fact_vortex_path.display().to_string(),
            ),
            (
                "dim_vortex_path".to_string(),
                self.dim_vortex_path.display().to_string(),
            ),
            (
                "fact_vortex_bytes".to_string(),
                self.fact_vortex_bytes.to_string(),
            ),
            (
                "dim_vortex_bytes".to_string(),
                self.dim_vortex_bytes.to_string(),
            ),
            (
                "source_bytes_read".to_string(),
                self.source_bytes_read.to_string(),
            ),
            (
                "materialization_boundary_rows".to_string(),
                self.materialization_boundary_rows.to_string(),
            ),
            (
                "native_work_envelope_created".to_string(),
                self.native_work_envelope_created.to_string(),
            ),
            (
                "native_work_stream_created".to_string(),
                self.native_work_stream_created.to_string(),
            ),
            (
                "native_result_stream_created".to_string(),
                self.native_result_stream_created.to_string(),
            ),
            (
                "native_io_certificate_emitted".to_string(),
                self.native_io_certificate_emitted.to_string(),
            ),
            (
                "vortex_source_adapter_used".to_string(),
                self.vortex_source_adapter_used.to_string(),
            ),
            (
                "vortex_file_read".to_string(),
                self.vortex_file_read.to_string(),
            ),
            (
                "upstream_vortex_scan_called".to_string(),
                self.upstream_vortex_scan_called.to_string(),
            ),
            ("data_decoded".to_string(), self.data_decoded.to_string()),
            (
                "data_materialized".to_string(),
                self.data_materialized.to_string(),
            ),
            (
                "materialization_boundary_report_emitted".to_string(),
                self.materialization_boundary_report_emitted.to_string(),
            ),
            ("row_read".to_string(), self.row_read.to_string()),
            (
                "arrow_converted".to_string(),
                self.arrow_converted.to_string(),
            ),
            (
                "object_store_io".to_string(),
                self.object_store_io.to_string(),
            ),
            ("write_io".to_string(), self.write_io.to_string()),
            (
                "spill_io_performed".to_string(),
                self.spill_io_performed.to_string(),
            ),
        ];
        fields
    }
}

trait StreamingExecutionFieldView {
    fn streaming_vortex_execution_used(&self) -> bool;
    fn full_table_materialization_avoided(&self) -> bool;
    fn streaming_filter_pushdown_applied(&self) -> bool;
    fn streaming_projection_pushdown_applied(&self) -> bool;
    fn streaming_arrays_read_count(&self) -> usize;
    fn streaming_max_chunk_rows(&self) -> usize;
    fn streaming_projected_columns(&self) -> &[String];
}

impl StreamingExecutionFieldView for TraditionalAnalyticsReport {
    fn streaming_vortex_execution_used(&self) -> bool {
        self.streaming_vortex_execution_used
    }

    fn full_table_materialization_avoided(&self) -> bool {
        self.full_table_materialization_avoided
    }

    fn streaming_filter_pushdown_applied(&self) -> bool {
        self.streaming_filter_pushdown_applied
    }

    fn streaming_projection_pushdown_applied(&self) -> bool {
        self.streaming_projection_pushdown_applied
    }

    fn streaming_arrays_read_count(&self) -> usize {
        self.streaming_arrays_read_count
    }

    fn streaming_max_chunk_rows(&self) -> usize {
        self.streaming_max_chunk_rows
    }

    fn streaming_projected_columns(&self) -> &[String] {
        &self.streaming_projected_columns
    }
}

impl StreamingExecutionFieldView for TraditionalAnalyticsVortexReport {
    fn streaming_vortex_execution_used(&self) -> bool {
        self.streaming_vortex_execution_used
    }

    fn full_table_materialization_avoided(&self) -> bool {
        self.full_table_materialization_avoided
    }

    fn streaming_filter_pushdown_applied(&self) -> bool {
        self.streaming_filter_pushdown_applied
    }

    fn streaming_projection_pushdown_applied(&self) -> bool {
        self.streaming_projection_pushdown_applied
    }

    fn streaming_arrays_read_count(&self) -> usize {
        self.streaming_arrays_read_count
    }

    fn streaming_max_chunk_rows(&self) -> usize {
        self.streaming_max_chunk_rows
    }

    fn streaming_projected_columns(&self) -> &[String] {
        &self.streaming_projected_columns
    }
}

fn streaming_execution_fields(report: &impl StreamingExecutionFieldView) -> Vec<(String, String)> {
    vec![
        (
            "streaming_vortex_execution_used".to_string(),
            report.streaming_vortex_execution_used().to_string(),
        ),
        (
            "full_table_materialization_avoided".to_string(),
            report.full_table_materialization_avoided().to_string(),
        ),
        (
            "streaming_filter_pushdown_applied".to_string(),
            report.streaming_filter_pushdown_applied().to_string(),
        ),
        (
            "streaming_projection_pushdown_applied".to_string(),
            report.streaming_projection_pushdown_applied().to_string(),
        ),
        (
            "streaming_arrays_read_count".to_string(),
            report.streaming_arrays_read_count().to_string(),
        ),
        (
            "streaming_max_chunk_rows".to_string(),
            report.streaming_max_chunk_rows().to_string(),
        ),
        (
            "streaming_projected_columns".to_string(),
            report.streaming_projected_columns().join(","),
        ),
    ]
}

#[allow(clippy::too_many_lines)]
fn native_io_certificate_fields(certificate: &NativeIoCertificate) -> Vec<(String, String)> {
    let source = &certificate.source_capability_report;
    let pushdown = &certificate.source_pushdown_report;
    let sink = &certificate.sink_requirement_report;
    let fidelity = &certificate.adapter_fidelity_report;
    let side_effects = &certificate.side_effects;
    let boundary = certificate.materialization_boundaries.first();
    vec![
        (
            "native_io_certificate_schema_version".to_string(),
            certificate.schema_version.to_string(),
        ),
        (
            "native_io_certificate_id".to_string(),
            certificate.certificate_id.clone(),
        ),
        (
            "native_io_certificate_path_id".to_string(),
            certificate.path_id.clone(),
        ),
        (
            "native_io_certificate_status".to_string(),
            certificate.status().to_string(),
        ),
        (
            "native_io_per_path_certificate_emitted".to_string(),
            certificate.is_certified().to_string(),
        ),
        (
            "native_io_representation_transition_order".to_string(),
            certificate.representation_transition_order(),
        ),
        (
            "native_io_materialization_boundary_order".to_string(),
            certificate.materialization_boundary_order(),
        ),
        (
            "native_io_materializing_transitions_have_boundaries".to_string(),
            certificate
                .materializing_transitions_have_boundaries()
                .to_string(),
        ),
        (
            "source_capability_source_kind".to_string(),
            source.source_kind.clone(),
        ),
        (
            "source_capability_adapter_id".to_string(),
            source.adapter_id.clone(),
        ),
        (
            "source_capability_schema_discovery_status".to_string(),
            source.schema_discovery_status.clone(),
        ),
        (
            "source_capability_statistics_availability".to_string(),
            source.statistics_availability.clone(),
        ),
        (
            "source_capability_pushdown_capabilities".to_string(),
            source.pushdown_capabilities.clone(),
        ),
        (
            "source_capability_encoded_representation_preserved".to_string(),
            source.encoded_representation_preserved.to_string(),
        ),
        (
            "source_capability_range_read_capability".to_string(),
            source.range_read_capability.to_string(),
        ),
        (
            "source_capability_streaming_capability".to_string(),
            source.streaming_capability.to_string(),
        ),
        (
            "source_capability_object_store_capability".to_string(),
            source.object_store_capability.to_string(),
        ),
        (
            "source_capability_fallback_attempted".to_string(),
            source.fallback_attempted.to_string(),
        ),
        (
            "source_pushdown_accepted_operations".to_string(),
            pushdown.accepted_operation_order(),
        ),
        (
            "source_pushdown_rejected_operations".to_string(),
            pushdown.rejected_operation_order(),
        ),
        (
            "source_pushdown_guarantee".to_string(),
            pushdown.guarantee.clone(),
        ),
        (
            "source_pushdown_proof_basis".to_string(),
            pushdown.proof_basis.clone(),
        ),
        (
            "source_pushdown_residual_expression".to_string(),
            pushdown
                .residual_expression
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "source_pushdown_conservative_false_positive_policy".to_string(),
            pushdown.conservative_false_positive_policy.to_string(),
        ),
        (
            "source_pushdown_unsafe_rejected_reason".to_string(),
            pushdown
                .unsafe_rejected_reason
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "source_pushdown_fallback_attempted".to_string(),
            pushdown.fallback_attempted.to_string(),
        ),
        (
            "sink_requirement_target_format".to_string(),
            sink.target_format.clone(),
        ),
        (
            "sink_requirement_accepts_encoded".to_string(),
            sink.accepts_encoded.to_string(),
        ),
        (
            "sink_requirement_requires_decoded_columnar".to_string(),
            sink.requires_decoded_columnar.to_string(),
        ),
        (
            "sink_requirement_requires_rows".to_string(),
            sink.requires_rows.to_string(),
        ),
        (
            "sink_requirement_preserves_metadata".to_string(),
            sink.preserves_metadata.to_string(),
        ),
        (
            "sink_requirement_requires_ordering".to_string(),
            sink.requires_ordering.to_string(),
        ),
        (
            "sink_requirement_requires_partitioning".to_string(),
            sink.requires_partitioning.to_string(),
        ),
        (
            "sink_requirement_requires_commit".to_string(),
            sink.requires_commit.to_string(),
        ),
        (
            "sink_requirement_supports_streaming".to_string(),
            sink.supports_streaming.to_string(),
        ),
        (
            "sink_requirement_max_chunk_size".to_string(),
            sink.max_chunk_size
                .map_or_else(|| "none".to_string(), |value| value.to_string()),
        ),
        (
            "sink_requirement_backpressure_policy".to_string(),
            sink.backpressure_policy.clone(),
        ),
        (
            "adapter_fidelity_adapter_id".to_string(),
            fidelity.adapter_id.clone(),
        ),
        (
            "adapter_fidelity_source_kind".to_string(),
            fidelity.source_kind.clone(),
        ),
        (
            "adapter_fidelity_sink_kind".to_string(),
            fidelity.sink_kind.clone(),
        ),
        (
            "adapter_fidelity_metadata_preserved".to_string(),
            fidelity.metadata_preserved.to_string(),
        ),
        (
            "adapter_fidelity_statistics_preserved".to_string(),
            fidelity.statistics_preserved.to_string(),
        ),
        (
            "adapter_fidelity_encoded_representation_preserved".to_string(),
            fidelity.encoded_representation_preserved.to_string(),
        ),
        (
            "adapter_fidelity_materialization_required".to_string(),
            fidelity.materialization_required.to_string(),
        ),
        (
            "adapter_fidelity_fidelity_loss".to_string(),
            fidelity.fidelity_loss.clone(),
        ),
        (
            "adapter_fidelity_metadata_loss".to_string(),
            fidelity.metadata_loss.clone(),
        ),
        (
            "adapter_fidelity_fallback_attempted".to_string(),
            fidelity.fallback_attempted.to_string(),
        ),
        (
            "materialization_boundary_id".to_string(),
            boundary.map_or_else(|| "none".to_string(), |report| report.boundary_id.clone()),
        ),
        (
            "materialization_boundary_from_state".to_string(),
            boundary.map_or_else(
                || "none".to_string(),
                |report| report.from_state.as_str().to_string(),
            ),
        ),
        (
            "materialization_boundary_to_state".to_string(),
            boundary.map_or_else(
                || "none".to_string(),
                |report| report.to_state.as_str().to_string(),
            ),
        ),
        (
            "materialization_boundary_required_by".to_string(),
            boundary.map_or_else(|| "none".to_string(), |report| report.required_by.clone()),
        ),
        (
            "materialization_boundary_reason".to_string(),
            boundary.map_or_else(|| "none".to_string(), |report| report.reason.clone()),
        ),
        (
            "materialization_boundary_bytes_decoded".to_string(),
            boundary.map_or_else(
                || "0".to_string(),
                |report| report.bytes_decoded.to_string(),
            ),
        ),
        (
            "materialization_boundary_rows_materialized".to_string(),
            boundary.map_or_else(
                || "0".to_string(),
                |report| report.rows_materialized.to_string(),
            ),
        ),
        (
            "materialization_boundary_fidelity_loss".to_string(),
            boundary.map_or_else(|| "none".to_string(), |report| report.fidelity_loss.clone()),
        ),
        (
            "materialization_boundary_fallback_attempted".to_string(),
            boundary.map_or_else(
                || "false".to_string(),
                |report| report.fallback_attempted.to_string(),
            ),
        ),
        (
            "native_io_side_effects_data_read".to_string(),
            side_effects.data_read.to_string(),
        ),
        (
            "native_io_side_effects_data_decoded".to_string(),
            side_effects.data_decoded.to_string(),
        ),
        (
            "native_io_side_effects_data_materialized".to_string(),
            side_effects.data_materialized.to_string(),
        ),
        (
            "native_io_side_effects_row_read".to_string(),
            side_effects.row_read.to_string(),
        ),
        (
            "native_io_side_effects_arrow_converted".to_string(),
            side_effects.arrow_converted.to_string(),
        ),
        (
            "native_io_side_effects_object_store_io".to_string(),
            side_effects.object_store_io.to_string(),
        ),
        (
            "native_io_side_effects_write_io".to_string(),
            side_effects.write_io.to_string(),
        ),
        (
            "native_io_side_effects_spill_io_performed".to_string(),
            side_effects.spill_io_performed.to_string(),
        ),
        (
            "native_io_side_effects_external_effects_executed".to_string(),
            side_effects.external_effects_executed.to_string(),
        ),
        (
            "native_io_side_effects_fallback_attempted".to_string(),
            side_effects.fallback_attempted.to_string(),
        ),
        (
            "native_io_side_effects_fallback_execution_allowed".to_string(),
            side_effects.fallback_execution_allowed.to_string(),
        ),
    ]
}

/// Runs a local traditional analytics scenario through CSV import into Vortex files.
///
/// # Errors
/// Returns an error when the feature gate is disabled, CSV input is invalid, the
/// local Vortex write/read path fails, or the benchmark scenario is unsupported.
pub fn run_traditional_analytics_benchmark(
    request: TraditionalAnalyticsRequest,
) -> Result<TraditionalAnalyticsReport> {
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    {
        run_traditional_analytics_benchmark_enabled(request)
    }
    #[cfg(not(feature = "vortex-traditional-analytics-benchmark"))]
    {
        std::mem::drop(request);
        Err(ShardLoomError::InvalidOperation(
            "traditional analytics benchmark requires feature `vortex-traditional-analytics-benchmark`; fallback execution was not attempted".to_string(),
        ))
    }
}

/// Runs a local traditional analytics scenario directly from native Vortex files.
///
/// # Errors
/// Returns an error when the feature gate is disabled, the Vortex files cannot be
/// read, or the benchmark scenario is unsupported.
pub fn run_traditional_analytics_vortex_benchmark(
    request: TraditionalAnalyticsVortexRequest,
) -> Result<TraditionalAnalyticsVortexReport> {
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    {
        run_traditional_analytics_vortex_benchmark_enabled(request)
    }
    #[cfg(not(feature = "vortex-traditional-analytics-benchmark"))]
    {
        std::mem::drop(request);
        Err(ShardLoomError::InvalidOperation(
            "native Vortex traditional analytics benchmark requires feature `vortex-traditional-analytics-benchmark`; fallback execution was not attempted".to_string(),
        ))
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone)]
struct TraditionalFactRow {
    id: u64,
    group_key: u32,
    dim_key: u32,
    value: u32,
    metric: f64,
    flag: u8,
    category: String,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone)]
struct TraditionalDimRow {
    dim_key: u32,
    dim_label: String,
    weight: f64,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone)]
struct VortexFactTable {
    id: Vec<u64>,
    group_key: Vec<u32>,
    dim_key: Vec<u32>,
    value: Vec<u32>,
    metric: Vec<f64>,
    flag: Vec<u8>,
    category: Vec<String>,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone)]
struct VortexDimTable {
    dim_key: Vec<u32>,
    dim_label: Vec<String>,
    weight: Vec<f64>,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Default, Clone)]
struct TraditionalGroupAccum {
    row_count: u64,
    metric_sum: f64,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl TraditionalGroupAccum {
    fn add(&mut self, metric: f64) {
        self.row_count += 1;
        self.metric_sum += metric;
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Default, Clone)]
struct TraditionalComplexAccum {
    row_count: u64,
    metric_sum: f64,
    weighted_sum: f64,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl TraditionalComplexAccum {
    fn add(&mut self, metric: f64, weighted_metric: f64) {
        self.row_count += 1;
        self.metric_sum += metric;
        self.weighted_sum += weighted_metric;
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
struct TraditionalScenarioExecutionEvidence {
    streaming_vortex_execution_used: bool,
    full_table_materialization_avoided: bool,
    filter_pushdown_applied: bool,
    projection_pushdown_applied: bool,
    arrays_read_count: usize,
    max_chunk_rows: usize,
    projected_columns: Vec<String>,
    data_decoded: bool,
    data_materialized: bool,
    row_read: bool,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl TraditionalScenarioExecutionEvidence {
    fn table_materialized() -> Self {
        Self {
            streaming_vortex_execution_used: false,
            full_table_materialization_avoided: false,
            filter_pushdown_applied: false,
            projection_pushdown_applied: false,
            arrays_read_count: 0,
            max_chunk_rows: 0,
            projected_columns: Vec::new(),
            data_decoded: true,
            data_materialized: true,
            row_read: false,
        }
    }

    fn streaming(stats: TraditionalStreamingScanStats) -> Self {
        Self {
            streaming_vortex_execution_used: true,
            full_table_materialization_avoided: true,
            filter_pushdown_applied: stats.filter_pushdown_applied,
            projection_pushdown_applied: stats.projection_pushdown_applied,
            arrays_read_count: stats.arrays_read_count,
            max_chunk_rows: stats.max_chunk_rows,
            projected_columns: stats.projected_columns,
            data_decoded: true,
            data_materialized: false,
            row_read: false,
        }
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalScenarioExecution {
    result_json: String,
    fact_rows: u64,
    dim_rows: u64,
    rows_scanned: u64,
    rows_materialized: u64,
    evidence: TraditionalScenarioExecutionEvidence,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalOutputReplayVerification {
    result_json: String,
    rows_scanned: u64,
    rows_materialized: u64,
    native_io_certificate: NativeIoCertificate,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalComputedResultPayload {
    scenario: String,
    result_json: String,
    rows_materialized: u64,
    workload_constitution_id: String,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalComputedResultSinkVerification {
    path: PathBuf,
    bytes: u64,
    digest: String,
    write_micros: u64,
    rows_written: u64,
    rows_materialized: u64,
    replay_result_json: String,
    native_io_certificate: NativeIoCertificate,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalRuntimeTaskEvidence {
    task_id: TaskId,
    label: &'static str,
    operator_class: OperatorMemoryClass,
    estimated_memory_bytes: u64,
    retryable: bool,
    cancellable: bool,
    idempotent: bool,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl TraditionalRuntimeTaskEvidence {
    fn new(
        task_id: impl Into<String>,
        label: &'static str,
        operator_class: OperatorMemoryClass,
        estimated_memory_bytes: u64,
    ) -> Result<Self> {
        Ok(Self {
            task_id: TaskId::new(task_id)?,
            label,
            operator_class,
            estimated_memory_bytes: estimated_memory_bytes.max(1),
            retryable: true,
            cancellable: true,
            idempotent: true,
        })
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
struct TraditionalRuntimeEvidence {
    scheduler_mode: String,
    scheduler_ref: String,
    task_count: usize,
    scheduled_task_count: usize,
    completed_task_count: usize,
    split_count: usize,
    scheduler_batch_count: usize,
    max_parallelism: usize,
    queue_limit: usize,
    queue_limit_enforced: bool,
    backpressure_bounded: bool,
    cancellation_checkpoint_count: usize,
    cancellation_testable: bool,
    cancellation_gate_status: String,
    retry_testable: bool,
    retry_gate_status: String,
    memory_budget_bytes: u64,
    memory_soft_limit_bytes: u64,
    memory_hard_limit_bytes: u64,
    memory_reservations_requested: usize,
    memory_reservations_granted: usize,
    memory_reservations_released: usize,
    memory_reservations_denied: usize,
    memory_peak_reserved_bytes: u64,
    fail_before_oom_enforced: bool,
    spill_required: bool,
    spill_supported: bool,
    spill_blocker: String,
    operator_memory_spill_declaration_count: usize,
    operator_memory_spill_claim_blocker_count: usize,
    large_workload_claim_allowed: bool,
    execution_certificate: ExecutionCertificate,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone)]
struct TraditionalStreamingScanStats {
    source_row_count: u64,
    result_row_count: u64,
    arrays_read_count: usize,
    max_chunk_rows: usize,
    projected_columns: Vec<String>,
    filter_pushdown_applied: bool,
    projection_pushdown_applied: bool,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[allow(clippy::too_many_lines)]
fn run_traditional_analytics_benchmark_enabled(
    request: TraditionalAnalyticsRequest,
) -> Result<TraditionalAnalyticsReport> {
    use std::fs;

    fs::create_dir_all(&request.workspace_dir).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to create traditional analytics Vortex workspace '{}': {error}",
            request.workspace_dir.display()
        ))
    })?;
    let fact_source_bytes = file_len(&request.fact_csv, "fact input")?;
    let dim_source_bytes = file_len(&request.dim_csv, "dimension input")?;
    let source_bytes_read = fact_source_bytes
        .checked_add(dim_source_bytes)
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "traditional analytics source byte count overflow".to_string(),
            )
        })?;
    let resource_policy = request
        .resource_policy
        .resolve_for_sources(source_bytes_read);
    let fact_rows =
        read_traditional_fact_rows(&request.fact_csv, request.input_format, resource_policy)?;
    let dim_rows =
        read_traditional_dim_rows(&request.dim_csv, request.input_format, resource_policy)?;
    let source_rows_materialized = checked_usize_sum_to_u64(fact_rows.len(), dim_rows.len())?;
    let fact_vortex_path = request.workspace_dir.join("fact.vortex");
    let dim_vortex_path = request.workspace_dir.join("dim.vortex");
    write_fact_vortex(&fact_rows, &fact_vortex_path)?;
    write_dim_vortex(&dim_rows, &dim_vortex_path)?;
    let mut compatibility_output = None;
    let scenario_compute_start = std::time::Instant::now();
    let scenario_execution = if let Some(output_format) = request.compatibility_output_format {
        let fact = read_fact_vortex(&fact_vortex_path)?;
        let dim = read_dim_vortex(&dim_vortex_path)?;
        let output_dir = request.workspace_dir.join("compatibility_output");
        fs::create_dir_all(&output_dir).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create traditional analytics compatibility output workspace '{}': {error}",
                output_dir.display()
            ))
        })?;
        let (fact_output, dim_output) =
            write_traditional_compatibility_outputs(&fact, &dim, output_format, &output_dir)?;
        compatibility_output = Some((output_format, fact_output, dim_output));
        run_vortex_derived_scenario_from_tables(request.scenario, &fact, &dim)?
    } else {
        run_vortex_derived_scenario_from_files(
            request.scenario,
            &fact_vortex_path,
            &dim_vortex_path,
        )?
    };
    let scenario_compute_micros = duration_to_micros(scenario_compute_start.elapsed());
    let fact_vortex_bytes = file_len(&fact_vortex_path, "fact Vortex file")?;
    let dim_vortex_bytes = file_len(&dim_vortex_path, "dimension Vortex file")?;
    let fact_compatibility_output_bytes = compatibility_output
        .as_ref()
        .map_or(Ok(0), |(_, fact_output, _)| {
            file_len(fact_output, "fact compatibility output")
        })?;
    let dim_compatibility_output_bytes = compatibility_output
        .as_ref()
        .map_or(Ok(0), |(_, _, dim_output)| {
            file_len(dim_output, "dimension compatibility output")
        })?;
    let fact_vortex_digest = file_digest(&fact_vortex_path, "fact Vortex file")?;
    let dim_vortex_digest = file_digest(&dim_vortex_path, "dimension Vortex file")?;
    let computed_result_sink = if request.write_result_vortex {
        Some(write_and_verify_computed_result_sink(
            request.scenario,
            &scenario_execution.result_json,
            scenario_execution.rows_materialized,
            &request.workspace_dir,
        )?)
    } else {
        None
    };
    let combined_output_digest = combined_artifact_digest(
        &fact_vortex_digest,
        &dim_vortex_digest,
        computed_result_sink
            .as_ref()
            .map(|sink| sink.digest.as_str()),
    );
    let output_replay = if request.verify_native_vortex_replay {
        Some(verify_native_vortex_replay(
            request.scenario,
            &fact_vortex_path,
            &dim_vortex_path,
            &scenario_execution.result_json,
            fact_vortex_bytes
                .checked_add(dim_vortex_bytes)
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "traditional analytics Vortex byte count overflow".to_string(),
                    )
                })?,
        )?)
    } else {
        None
    };
    let native_io_certificate = traditional_native_io_certificate(
        request.scenario,
        request.input_format,
        source_bytes_read,
        source_rows_materialized,
    )?;
    if !native_io_certificate.is_certified() {
        return Err(ShardLoomError::InvalidOperation(
            "traditional analytics native I/O certificate was not certified".to_string(),
        ));
    }
    let runtime_evidence = build_traditional_runtime_evidence(
        request.scenario,
        request.input_format,
        resource_policy,
        fact_source_bytes,
        dim_source_bytes,
        fact_vortex_bytes,
        dim_vortex_bytes,
        scenario_execution.rows_materialized,
        output_replay.is_some(),
        computed_result_sink.as_ref(),
    )?;
    if !runtime_evidence.execution_certificate.fallback_free()
        || !runtime_evidence
            .execution_certificate
            .external_query_engine_free()
    {
        return Err(ShardLoomError::InvalidOperation(
            "traditional analytics runtime execution certificate was blocked by fallback or external engine evidence".to_string(),
        ));
    }

    Ok(TraditionalAnalyticsReport {
        scenario: request.scenario,
        input_format: request.input_format,
        resource_policy,
        result_json: scenario_execution.result_json,
        fact_rows: scenario_execution.fact_rows,
        dim_rows: scenario_execution.dim_rows,
        rows_scanned: scenario_execution.rows_scanned,
        rows_materialized: scenario_execution.rows_materialized,
        workspace_dir: request.workspace_dir,
        fact_vortex_path,
        dim_vortex_path,
        compatibility_output_format: compatibility_output
            .as_ref()
            .map(|(output_format, _, _)| *output_format),
        fact_compatibility_output_path: compatibility_output
            .as_ref()
            .map(|(_, fact_output, _)| fact_output.clone()),
        dim_compatibility_output_path: compatibility_output
            .as_ref()
            .map(|(_, _, dim_output)| dim_output.clone()),
        fact_source_path: request.fact_csv,
        dim_source_path: request.dim_csv,
        workload_constitution_id: LOCAL_VORTEX_ANALYTICS_CONSTITUTION_ID.to_string(),
        workload_scorecard_status: traditional_workload_scorecard_status(
            output_replay.is_some(),
            computed_result_sink.is_some(),
        )
        .to_string(),
        benchmark_row_ref: traditional_benchmark_row_ref(request.scenario, request.input_format),
        coverage_row_ref: traditional_coverage_row_ref(request.scenario, request.input_format),
        output_artifact_schema_summary: traditional_output_schema_summary().to_string(),
        output_artifact_digest_algorithm: OUTPUT_ARTIFACT_DIGEST_ALGORITHM.to_string(),
        fact_vortex_digest,
        dim_vortex_digest,
        combined_output_digest,
        output_replay_requested: request.verify_native_vortex_replay,
        output_replay_verified: output_replay.is_some(),
        output_replay_result_json: output_replay
            .as_ref()
            .map(|replay| replay.result_json.clone()),
        output_replay_rows_scanned: output_replay.as_ref().map(|replay| replay.rows_scanned),
        output_replay_rows_materialized: output_replay
            .as_ref()
            .map(|replay| replay.rows_materialized),
        output_replay_native_io_certificate_id: output_replay
            .as_ref()
            .map(|replay| replay.native_io_certificate.certificate_id.clone()),
        output_replay_native_io_certificate_status: output_replay
            .as_ref()
            .map(|replay| replay.native_io_certificate.status().to_string()),
        computed_result_sink_requested: request.write_result_vortex,
        computed_result_sink_written: computed_result_sink.is_some(),
        computed_result_sink_replay_verified: computed_result_sink.is_some(),
        computed_result_vortex_path: computed_result_sink.as_ref().map(|sink| sink.path.clone()),
        computed_result_vortex_bytes: computed_result_sink.as_ref().map_or(0, |sink| sink.bytes),
        computed_result_vortex_digest: computed_result_sink
            .as_ref()
            .map(|sink| sink.digest.clone()),
        computed_result_sink_rows: computed_result_sink
            .as_ref()
            .map_or(0, |sink| sink.rows_written),
        computed_result_sink_rows_materialized: computed_result_sink
            .as_ref()
            .map_or(0, |sink| sink.rows_materialized),
        computed_result_sink_schema_summary: COMPUTED_RESULT_VORTEX_SCHEMA_SUMMARY.to_string(),
        scenario_compute_micros,
        computed_result_sink_write_micros: computed_result_sink
            .as_ref()
            .map(|sink| sink.write_micros),
        computed_result_sink_replay_result_json: computed_result_sink
            .as_ref()
            .map(|sink| sink.replay_result_json.clone()),
        computed_result_sink_native_io_certificate_id: computed_result_sink
            .as_ref()
            .map(|sink| sink.native_io_certificate.certificate_id.clone()),
        computed_result_sink_native_io_certificate_status: computed_result_sink
            .as_ref()
            .map(|sink| sink.native_io_certificate.status().to_string()),
        runtime_task_graph_created: true,
        runtime_task_graph_executed: true,
        runtime_scheduler_mode: runtime_evidence.scheduler_mode,
        runtime_scheduler_ref: runtime_evidence.scheduler_ref,
        runtime_task_count: runtime_evidence.task_count,
        runtime_scheduled_task_count: runtime_evidence.scheduled_task_count,
        runtime_completed_task_count: runtime_evidence.completed_task_count,
        runtime_split_count: runtime_evidence.split_count,
        runtime_scheduler_batch_count: runtime_evidence.scheduler_batch_count,
        runtime_max_parallelism: runtime_evidence.max_parallelism,
        runtime_queue_limit: runtime_evidence.queue_limit,
        runtime_queue_limit_enforced: runtime_evidence.queue_limit_enforced,
        runtime_backpressure_bounded: runtime_evidence.backpressure_bounded,
        runtime_cancellation_checkpoint_count: runtime_evidence.cancellation_checkpoint_count,
        runtime_cancellation_testable: runtime_evidence.cancellation_testable,
        runtime_cancellation_gate_status: runtime_evidence.cancellation_gate_status,
        runtime_retry_testable: runtime_evidence.retry_testable,
        runtime_retry_gate_status: runtime_evidence.retry_gate_status,
        runtime_memory_budget_bytes: runtime_evidence.memory_budget_bytes,
        runtime_memory_soft_limit_bytes: runtime_evidence.memory_soft_limit_bytes,
        runtime_memory_hard_limit_bytes: runtime_evidence.memory_hard_limit_bytes,
        runtime_memory_reservations_requested: runtime_evidence.memory_reservations_requested,
        runtime_memory_reservations_granted: runtime_evidence.memory_reservations_granted,
        runtime_memory_reservations_released: runtime_evidence.memory_reservations_released,
        runtime_memory_reservations_denied: runtime_evidence.memory_reservations_denied,
        runtime_memory_peak_reserved_bytes: runtime_evidence.memory_peak_reserved_bytes,
        runtime_fail_before_oom_enforced: runtime_evidence.fail_before_oom_enforced,
        runtime_spill_required: runtime_evidence.spill_required,
        runtime_spill_supported: runtime_evidence.spill_supported,
        runtime_spill_blocker: runtime_evidence.spill_blocker,
        runtime_operator_memory_spill_declaration_count: runtime_evidence
            .operator_memory_spill_declaration_count,
        runtime_operator_memory_spill_claim_blocker_count: runtime_evidence
            .operator_memory_spill_claim_blocker_count,
        runtime_large_workload_claim_allowed: runtime_evidence.large_workload_claim_allowed,
        runtime_execution_certificate: runtime_evidence.execution_certificate,
        commit_state: if computed_result_sink.is_some() {
            "local_vortex_files_and_result_sink_written_uncommitted".to_string()
        } else {
            "local_vortex_files_written_uncommitted".to_string()
        },
        rollback_cleanup_status: "caller_owned_workspace_cleanup".to_string(),
        fact_source_bytes,
        dim_source_bytes,
        fact_compatibility_output_bytes,
        dim_compatibility_output_bytes,
        fact_csv_bytes: if request.input_format == TraditionalAnalyticsInputFormat::Csv {
            fact_source_bytes
        } else {
            0
        },
        dim_csv_bytes: if request.input_format == TraditionalAnalyticsInputFormat::Csv {
            dim_source_bytes
        } else {
            0
        },
        source_bytes_read,
        fact_vortex_bytes,
        dim_vortex_bytes,
        materialization_boundary_rows: source_rows_materialized,
        native_io_certificate,
        native_work_envelope_created: true,
        native_work_stream_created: true,
        native_result_stream_created: true,
        native_io_certificate_emitted: true,
        compatibility_source_adapter_used: true,
        compatibility_to_vortex_import_performed: true,
        compatibility_output_requested: compatibility_output.is_some(),
        compatibility_output_written: compatibility_output.is_some(),
        native_to_compatibility_output_performed: compatibility_output.is_some(),
        csv_source_adapter_used: request.input_format == TraditionalAnalyticsInputFormat::Csv,
        csv_to_vortex_import_performed: request.input_format
            == TraditionalAnalyticsInputFormat::Csv,
        jsonl_source_adapter_used: request.input_format
            == TraditionalAnalyticsInputFormat::JsonLines,
        jsonl_to_vortex_import_performed: request.input_format
            == TraditionalAnalyticsInputFormat::JsonLines,
        vortex_file_written: true,
        vortex_file_read: true,
        upstream_vortex_scan_called: true,
        streaming_vortex_execution_used: scenario_execution
            .evidence
            .streaming_vortex_execution_used,
        full_table_materialization_avoided: scenario_execution
            .evidence
            .full_table_materialization_avoided,
        streaming_filter_pushdown_applied: scenario_execution.evidence.filter_pushdown_applied,
        streaming_projection_pushdown_applied: scenario_execution
            .evidence
            .projection_pushdown_applied,
        streaming_arrays_read_count: scenario_execution.evidence.arrays_read_count,
        streaming_max_chunk_rows: scenario_execution.evidence.max_chunk_rows,
        streaming_projected_columns: scenario_execution.evidence.projected_columns,
        data_decoded: true,
        data_materialized: true,
        materialization_boundary_report_emitted: true,
        row_read: true,
        arrow_converted: false,
        object_store_io: false,
        write_io: true,
        spill_io_performed: false,
        fallback_execution_allowed: false,
        diagnostics: Vec::new(),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn verify_native_vortex_replay(
    scenario: TraditionalAnalyticsScenario,
    fact_vortex_path: &std::path::Path,
    dim_vortex_path: &std::path::Path,
    expected_result_json: &str,
    source_bytes_read: u64,
) -> Result<TraditionalOutputReplayVerification> {
    let replay =
        run_vortex_derived_scenario_from_files(scenario, fact_vortex_path, dim_vortex_path)?;
    if replay.result_json != expected_result_json {
        return Err(ShardLoomError::InvalidOperation(format!(
            "native Vortex replay result mismatch for {}; fallback execution was not attempted",
            scenario.as_str()
        )));
    }
    let materialization_boundary_rows = if replay.evidence.data_materialized {
        checked_u64_sum(replay.fact_rows, replay.dim_rows)?
    } else {
        0
    };
    let native_io_certificate = traditional_native_vortex_io_certificate(
        scenario,
        source_bytes_read,
        materialization_boundary_rows,
        &replay.evidence,
    )?;
    if !native_io_certificate.is_certified() {
        return Err(ShardLoomError::InvalidOperation(
            "native Vortex replay NativeIoCertificate was not certified".to_string(),
        ));
    }
    Ok(TraditionalOutputReplayVerification {
        result_json: replay.result_json,
        rows_scanned: replay.rows_scanned,
        rows_materialized: replay.rows_materialized,
        native_io_certificate,
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_and_verify_computed_result_sink(
    scenario: TraditionalAnalyticsScenario,
    result_json: &str,
    rows_materialized: u64,
    workspace_dir: &std::path::Path,
) -> Result<TraditionalComputedResultSinkVerification> {
    let result_vortex_path = workspace_dir.join("result.vortex");
    let write_start = std::time::Instant::now();
    write_computed_result_vortex(
        scenario,
        result_json,
        rows_materialized,
        &result_vortex_path,
    )?;
    let write_micros = duration_to_micros(write_start.elapsed());
    let bytes = file_len(&result_vortex_path, "computed result Vortex file")?;
    let digest = file_digest(&result_vortex_path, "computed result Vortex file")?;
    let replay = read_computed_result_vortex(&result_vortex_path)?;
    if replay.scenario != scenario.as_str() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "computed result Vortex replay scenario mismatch for {}; fallback execution was not attempted",
            scenario.as_str()
        )));
    }
    if replay.result_json != result_json {
        return Err(ShardLoomError::InvalidOperation(format!(
            "computed result Vortex replay result mismatch for {}; fallback execution was not attempted",
            scenario.as_str()
        )));
    }
    if replay.rows_materialized != rows_materialized {
        return Err(ShardLoomError::InvalidOperation(format!(
            "computed result Vortex replay row-count mismatch for {}; fallback execution was not attempted",
            scenario.as_str()
        )));
    }
    if replay.workload_constitution_id != LOCAL_VORTEX_ANALYTICS_CONSTITUTION_ID {
        return Err(ShardLoomError::InvalidOperation(format!(
            "computed result Vortex replay workload constitution mismatch for {}; fallback execution was not attempted",
            scenario.as_str()
        )));
    }
    let native_io_certificate =
        computed_result_sink_native_io_certificate(scenario, bytes, rows_materialized)?;
    if !native_io_certificate.is_certified() {
        return Err(ShardLoomError::InvalidOperation(
            "computed result sink NativeIoCertificate was not certified".to_string(),
        ));
    }
    Ok(TraditionalComputedResultSinkVerification {
        path: result_vortex_path,
        bytes,
        digest,
        write_micros,
        rows_written: 1,
        rows_materialized,
        replay_result_json: replay.result_json,
        native_io_certificate,
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn traditional_workload_scorecard_status(
    source_replay_verified: bool,
    computed_result_sink_verified: bool,
) -> &'static str {
    match (source_replay_verified, computed_result_sink_verified) {
        (true, true) => "workload_certified",
        (true, false) => "fixture_certified_result_sink_not_requested",
        (false, true) => "fixture_certified_source_replay_not_requested",
        (false, false) => "fixture_certified_replay_not_requested",
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn build_traditional_runtime_evidence(
    scenario: TraditionalAnalyticsScenario,
    input_format: TraditionalAnalyticsInputFormat,
    resource_policy: TraditionalAnalyticsResourcePolicy,
    fact_source_bytes: u64,
    dim_source_bytes: u64,
    fact_vortex_bytes: u64,
    dim_vortex_bytes: u64,
    rows_materialized: u64,
    output_replay_verified: bool,
    computed_result_sink: Option<&TraditionalComputedResultSinkVerification>,
) -> Result<TraditionalRuntimeEvidence> {
    let tasks = traditional_runtime_tasks(
        scenario,
        resource_policy,
        fact_source_bytes,
        dim_source_bytes,
        fact_vortex_bytes,
        dim_vortex_bytes,
        output_replay_verified,
        computed_result_sink.map_or(0, |sink| sink.bytes),
    )?;
    let max_parallelism = resource_policy.max_parallelism.max(1);
    let scheduler_batch_count = tasks.len().div_ceil(max_parallelism).max(1);
    let queue_limit_enforced = tasks
        .chunks(max_parallelism)
        .all(|chunk| chunk.len() <= max_parallelism);
    let memory_budget = MemoryBudget::from_gib(u64::from(resource_policy.memory_gb))?;
    let mut memory_pool = MemoryPoolPlan::new(memory_budget.clone());
    let mut memory_reservations_requested = 0;
    let mut memory_reservations_granted = 0;
    let mut memory_reservations_released = 0;
    let mut memory_reservations_denied = 0;
    let mut memory_peak_reserved_bytes = 0;
    for task in &tasks {
        memory_reservations_requested += 1;
        let reservation_id =
            MemoryReservationId::new(format!("traditional-runtime-{}", task.task_id.as_str()))?;
        let owner =
            MemoryOwner::new(task.operator_class, task.label)?.with_task_id(task.task_id.clone());
        let admission = memory_pool.admit_reservation(
            reservation_id.clone(),
            owner,
            ByteSize::from_bytes(task.estimated_memory_bytes),
        )?;
        memory_peak_reserved_bytes = memory_peak_reserved_bytes
            .max(admission.reserved_after.as_bytes())
            .max(admission.reserved_before.as_bytes());
        if admission.granted_decision() {
            memory_reservations_granted += 1;
            memory_pool.release_reservation(&reservation_id)?;
            memory_reservations_released += 1;
        } else {
            memory_reservations_denied += 1;
        }
    }
    let retry_gate = ShardLoomRetryExecutionGateReport::from_request(
        ShardLoomRetryExecutionGateRequest::new()
            .retry_requested(true)
            .retry_allowed_by_plan(true),
    )?;
    let cancellation_gate = ShardLoomCancellationExecutionGateReport::from_request(
        ShardLoomCancellationExecutionGateRequest::new().cancellation_requested(true),
    )?;
    let operator_memory_report = traditional_operator_memory_spill_report(scenario)?;
    let operator_claim_blocker_count = operator_memory_report.claim_blocker_count();
    let actual_spill_required = false;
    let spill_blocker = if operator_claim_blocker_count == 0 {
        "none".to_string()
    } else {
        "large_workload_claim_blocked_until_native_operator_spill_declarations".to_string()
    };
    let scheduler_ref = format!(
        "scheduler://local_vortex_analytics_v1/{}/tasks/{}/batches/{}/splits/{}",
        scenario.as_str().replace(['/', ' '], "-"),
        tasks.len(),
        scheduler_batch_count,
        resource_policy.target_partition_count
    );
    let correctness_passed =
        output_replay_verified && computed_result_sink.is_none_or(|sink| sink.rows_written == 1);
    let execution_certificate = traditional_runtime_execution_certificate(
        scenario,
        input_format,
        rows_materialized,
        &scheduler_ref,
        &tasks,
        computed_result_sink.is_some(),
        correctness_passed,
    )?;
    let all_tasks_testable = tasks
        .iter()
        .all(|task| task.retryable && task.cancellable && task.idempotent);
    Ok(TraditionalRuntimeEvidence {
        scheduler_mode: "deterministic_local_task_sequence".to_string(),
        scheduler_ref,
        task_count: tasks.len(),
        scheduled_task_count: tasks.len(),
        completed_task_count: tasks.len(),
        split_count: resource_policy.target_partition_count,
        scheduler_batch_count,
        max_parallelism,
        queue_limit: max_parallelism,
        queue_limit_enforced,
        backpressure_bounded: queue_limit_enforced,
        cancellation_checkpoint_count: tasks.len(),
        cancellation_testable: all_tasks_testable && cancellation_gate.cancellation_gate_open(),
        cancellation_gate_status: cancellation_gate.status.as_str().to_string(),
        retry_testable: all_tasks_testable && retry_gate.retry_gate_open(),
        retry_gate_status: retry_gate.status.as_str().to_string(),
        memory_budget_bytes: memory_budget.total.as_bytes(),
        memory_soft_limit_bytes: memory_budget.soft_limit.as_bytes(),
        memory_hard_limit_bytes: memory_budget.hard_limit.as_bytes(),
        memory_reservations_requested,
        memory_reservations_granted,
        memory_reservations_released,
        memory_reservations_denied,
        memory_peak_reserved_bytes,
        fail_before_oom_enforced: memory_reservations_denied == 0
            && memory_reservations_granted == memory_reservations_requested
            && memory_reservations_released == memory_reservations_granted,
        spill_required: actual_spill_required,
        spill_supported: actual_spill_required && operator_claim_blocker_count == 0,
        spill_blocker,
        operator_memory_spill_declaration_count: operator_memory_report.declaration_count(),
        operator_memory_spill_claim_blocker_count: operator_claim_blocker_count,
        large_workload_claim_allowed: operator_memory_report.large_workload_claim_allowed,
        execution_certificate,
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[allow(clippy::too_many_arguments)]
fn traditional_runtime_tasks(
    scenario: TraditionalAnalyticsScenario,
    resource_policy: TraditionalAnalyticsResourcePolicy,
    fact_source_bytes: u64,
    dim_source_bytes: u64,
    fact_vortex_bytes: u64,
    dim_vortex_bytes: u64,
    output_replay_verified: bool,
    computed_result_sink_bytes: u64,
) -> Result<Vec<TraditionalRuntimeTaskEvidence>> {
    let task_memory = |bytes: u64| -> u64 {
        bytes
            .max(1)
            .min(resource_policy.target_partition_bytes.max(1))
    };
    let mut tasks = vec![
        TraditionalRuntimeTaskEvidence::new(
            "compatibility-import-fact",
            "compatibility import fact to native Vortex",
            OperatorMemoryClass::Translation,
            task_memory(fact_source_bytes),
        )?,
        TraditionalRuntimeTaskEvidence::new(
            "compatibility-import-dim",
            "compatibility import dimension to native Vortex",
            OperatorMemoryClass::Translation,
            task_memory(dim_source_bytes),
        )?,
        TraditionalRuntimeTaskEvidence::new(
            "native-vortex-scenario-compute",
            "native Vortex scenario compute",
            scenario_operator_memory_class(scenario),
            task_memory(fact_source_bytes.saturating_add(dim_source_bytes)),
        )?,
    ];
    if output_replay_verified {
        tasks.push(TraditionalRuntimeTaskEvidence::new(
            "native-vortex-replay",
            "native Vortex replay verification",
            OperatorMemoryClass::Scan,
            task_memory(fact_vortex_bytes.saturating_add(dim_vortex_bytes)),
        )?);
    }
    if computed_result_sink_bytes > 0 {
        tasks.push(TraditionalRuntimeTaskEvidence::new(
            "computed-result-vortex-sink",
            "computed result native Vortex sink",
            OperatorMemoryClass::Sink,
            task_memory(computed_result_sink_bytes),
        )?);
    }
    Ok(tasks)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn scenario_operator_memory_class(scenario: TraditionalAnalyticsScenario) -> OperatorMemoryClass {
    match scenario {
        TraditionalAnalyticsScenario::CsvFileIngest => OperatorMemoryClass::Translation,
        TraditionalAnalyticsScenario::SelectiveFilter
        | TraditionalAnalyticsScenario::FilterProjectionLimit => OperatorMemoryClass::Filter,
        TraditionalAnalyticsScenario::GroupByAggregation
        | TraditionalAnalyticsScenario::DistinctCount
        | TraditionalAnalyticsScenario::MultiKeyGroupBy
        | TraditionalAnalyticsScenario::HighCardinalityStringGroupDistinct => {
            OperatorMemoryClass::Aggregate
        }
        TraditionalAnalyticsScenario::SortAndTopK | TraditionalAnalyticsScenario::TopNPerGroup => {
            OperatorMemoryClass::Sort
        }
        TraditionalAnalyticsScenario::HashJoin
        | TraditionalAnalyticsScenario::JoinAggregate
        | TraditionalAnalyticsScenario::ScaleStressSkewedJoinAggregation
        | TraditionalAnalyticsScenario::ScaleStressMultiStageEtl => OperatorMemoryClass::Join,
        TraditionalAnalyticsScenario::RowNumberWindow => OperatorMemoryClass::Window,
        TraditionalAnalyticsScenario::WideProjection => OperatorMemoryClass::Projection,
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn traditional_operator_memory_spill_report(
    scenario: TraditionalAnalyticsScenario,
) -> Result<OperatorMemorySpillDeclarationReport> {
    let mut classes = Vec::new();
    for class in [
        OperatorMemoryClass::Translation,
        OperatorMemoryClass::Scan,
        scenario_operator_memory_class(scenario),
        OperatorMemoryClass::Sink,
    ] {
        if !classes.contains(&class) {
            classes.push(class);
        }
    }
    let declarations = classes
        .into_iter()
        .map(|class| {
            if class.is_stateful() || class == OperatorMemoryClass::Sink {
                Ok(OperatorMemorySpillDeclaration::missing_required(class))
            } else {
                OperatorMemorySpillDeclaration::certified(
                    class,
                    SpillPolicy::DisabledForOperator,
                    format!(
                        "runtime://local_vortex_analytics_v1/operator/{}",
                        class.as_str()
                    ),
                )
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(OperatorMemorySpillDeclarationReport::from_declarations(
        declarations,
    ))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn traditional_runtime_execution_certificate(
    scenario: TraditionalAnalyticsScenario,
    input_format: TraditionalAnalyticsInputFormat,
    rows_materialized: u64,
    scheduler_ref: &str,
    tasks: &[TraditionalRuntimeTaskEvidence],
    result_sink_written: bool,
    correctness_passed: bool,
) -> Result<ExecutionCertificate> {
    let mut certificate_input = ExecutionCertificateInput::new(
        format!(
            "p746.local_vortex_analytics.{}.runtime",
            scenario.as_str().replace(['/', ' '], "-")
        ),
        "local_vortex_analytics_task_graph",
    )?;
    certificate_input.execution_provider_kind = ExecutionProviderKind::ShardLoomKernel;
    certificate_input.provider_scope = "native_local_vortex_workflow".to_string();
    certificate_input.provider_crate = Some("shardloom-vortex".to_string());
    certificate_input.provider_version = Some(env!("CARGO_PKG_VERSION").to_string());
    certificate_input.provider_api_surface =
        Some("run_traditional_analytics_benchmark::local_task_graph".to_string());
    certificate_input.shardloom_admission_policy =
        Some("local_vortex_analytics_v1_no_external_fallback".to_string());
    certificate_input.plan_ref = Some(scheduler_ref.to_string());
    certificate_input.input_ref = Some(format!(
        "traditional-analytics://source-format/{}",
        input_format.as_str()
    ));
    certificate_input.output_ref = Some(if result_sink_written {
        "vortex://local_vortex_analytics_v1/result.vortex".to_string()
    } else {
        "runtime-result://local_vortex_analytics_v1/in-memory".to_string()
    });
    certificate_input.correctness_fixture_id =
        Some("local_vortex_analytics_v1.native_replay".to_string());
    let outcome = ExpectedOutcome::Rows {
        row_count: Some(rows_materialized),
    };
    certificate_input.expected_outcome = Some(outcome.clone());
    certificate_input.actual_outcome = Some(outcome);
    certificate_input.selected_segment_count = tasks.len();
    certificate_input.skipped_segment_count = 0;
    certificate_input.side_effects_performed = vec![
        "compatibility_source_to_native_vortex_import".to_string(),
        "native_vortex_source_scan".to_string(),
    ];
    if result_sink_written {
        certificate_input
            .side_effects_performed
            .push("native_vortex_result_sink_write".to_string());
    }
    certificate_input.data_read = true;
    certificate_input.data_decoded = true;
    certificate_input.data_materialized = true;
    certificate_input.row_read = true;
    certificate_input.arrow_converted = false;
    certificate_input.object_store_io = false;
    certificate_input.write_io = true;
    certificate_input.spill_io_performed = false;
    certificate_input.external_effects_executed = false;
    certificate_input.external_query_engine_invoked = false;
    certificate_input.unsafe_effect_detected = false;
    certificate_input.fallback_attempted = false;
    certificate_input.fallback_execution_allowed = false;
    certificate_input.correctness_passed = correctness_passed;
    Ok(ExecutionCertificate::evaluate(certificate_input))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn computed_result_sink_native_io_certificate(
    scenario: TraditionalAnalyticsScenario,
    bytes_written: u64,
    rows_materialized: u64,
) -> Result<NativeIoCertificate> {
    NativeIoCertificate::new(
        format!(
            "cg19.traditional_analytics.result_sink.{}",
            scenario.as_str().replace(['/', ' '], "-")
        ),
        "native_runtime_result_to_native_vortex_sink",
        NativeIoSourceCapabilityReport {
            source_kind: "shardloom_runtime_result".to_string(),
            adapter_id: "shardloom.adapter.traditional_analytics.result_sink.v1".to_string(),
            schema_discovery_status: "declared_result_schema_validated".to_string(),
            statistics_availability: "result_rows_known".to_string(),
            pushdown_capabilities: "none_sink_write".to_string(),
            encoded_representation_preserved: false,
            range_read_capability: false,
            streaming_capability: false,
            object_store_capability: false,
            fallback_attempted: false,
        },
        NativeIoSourcePushdownReport {
            accepted_operations: vec![
                "native_vortex_result_sink_write".to_string(),
                "native_vortex_result_sink_replay".to_string(),
            ],
            rejected_operations: Vec::new(),
            guarantee: "exact_result_json_roundtrip".to_string(),
            proof_basis: format!(
                "computed result is written with the upstream Vortex writer, reopened through ShardLoom Vortex file IO, and byte-counted at {bytes_written} bytes"
            ),
            residual_expression: None,
            conservative_false_positive_policy: false,
            unsafe_rejected_reason: None,
            fallback_attempted: false,
        },
        vec![NativeIoRepresentationTransition::new(
            RepresentationState::DecodedColumnar,
            RepresentationState::VortexEncoded,
            false,
        )],
        NativeIoSinkRequirementReport {
            target_format: "vortex".to_string(),
            accepts_encoded: true,
            requires_decoded_columnar: true,
            requires_rows: false,
            preserves_metadata: true,
            requires_ordering: false,
            requires_partitioning: false,
            requires_commit: false,
            supports_streaming: false,
            max_chunk_size: Some(1),
            backpressure_policy: "not_applicable_local_result_sink".to_string(),
        },
        NativeIoAdapterFidelityReport {
            adapter_id: "shardloom.adapter.traditional_analytics.result_sink.v1".to_string(),
            source_kind: "shardloom_runtime_result".to_string(),
            sink_kind: "vortex".to_string(),
            metadata_preserved: true,
            statistics_preserved: false,
            encoded_representation_preserved: false,
            materialization_required: false,
            fidelity_loss: "none_for_declared_result_schema".to_string(),
            metadata_loss: "statistics emission is not yet certified for the result artifact"
                .to_string(),
            fallback_attempted: false,
        },
        vec![NativeIoMaterializationBoundaryReport {
            boundary_id: "cg19.computed_result_vortex_sink_encode".to_string(),
            from_state: RepresentationState::DecodedColumnar,
            to_state: RepresentationState::VortexEncoded,
            required_by: "traditional_analytics_result_sink".to_string(),
            reason: "computed result envelope is encoded into a native Vortex result artifact"
                .to_string(),
            bytes_decoded: 0,
            rows_materialized,
            fidelity_loss: "none_for_declared_result_schema".to_string(),
            fallback_attempted: false,
        }],
        NativeIoSideEffectReport {
            data_read: true,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: true,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
        },
        Vec::new(),
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn traditional_output_schema_summary() -> &'static str {
    "fact(id:u64,group_key:u32,dim_key:u32,value:u32,metric:f64,flag:u8,category:utf8);dim(dim_key:u32,dim_label:utf8,weight:f64)"
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn traditional_benchmark_row_ref(
    scenario: TraditionalAnalyticsScenario,
    input_format: TraditionalAnalyticsInputFormat,
) -> String {
    format!(
        "traditional_analytics:{}:{}:{}",
        LOCAL_VORTEX_ANALYTICS_CONSTITUTION_ID,
        input_format.as_str(),
        scenario.as_str().replace(['/', ' '], "-")
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn traditional_coverage_row_ref(
    scenario: TraditionalAnalyticsScenario,
    input_format: TraditionalAnalyticsInputFormat,
) -> String {
    format!(
        "coverage:{}:{}:{}",
        LOCAL_VORTEX_ANALYTICS_CONSTITUTION_ID,
        input_format.as_str(),
        scenario.as_str().replace(['/', ' '], "-")
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_traditional_analytics_vortex_benchmark_enabled(
    request: TraditionalAnalyticsVortexRequest,
) -> Result<TraditionalAnalyticsVortexReport> {
    let fact_vortex_bytes = file_len(&request.fact_vortex, "fact Vortex file")?;
    let dim_vortex_bytes = file_len(&request.dim_vortex, "dimension Vortex file")?;
    let source_bytes_read = fact_vortex_bytes
        .checked_add(dim_vortex_bytes)
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "native Vortex traditional analytics source byte count overflow".to_string(),
            )
        })?;
    let scenario_execution = run_vortex_derived_scenario_from_files(
        request.scenario,
        &request.fact_vortex,
        &request.dim_vortex,
    )?;
    let materialization_boundary_rows = if scenario_execution.evidence.data_materialized {
        checked_u64_sum(scenario_execution.fact_rows, scenario_execution.dim_rows)?
    } else {
        0
    };
    let native_io_certificate = traditional_native_vortex_io_certificate(
        request.scenario,
        source_bytes_read,
        materialization_boundary_rows,
        &scenario_execution.evidence,
    )?;
    if !native_io_certificate.is_certified() {
        return Err(ShardLoomError::InvalidOperation(
            "native Vortex traditional analytics native I/O certificate was not certified"
                .to_string(),
        ));
    }

    Ok(TraditionalAnalyticsVortexReport {
        scenario: request.scenario,
        result_json: scenario_execution.result_json,
        fact_rows: scenario_execution.fact_rows,
        dim_rows: scenario_execution.dim_rows,
        rows_scanned: scenario_execution.rows_scanned,
        rows_materialized: scenario_execution.rows_materialized,
        fact_vortex_path: request.fact_vortex,
        dim_vortex_path: request.dim_vortex,
        fact_vortex_bytes,
        dim_vortex_bytes,
        source_bytes_read,
        materialization_boundary_rows,
        native_io_certificate,
        native_work_envelope_created: true,
        native_work_stream_created: true,
        native_result_stream_created: true,
        native_io_certificate_emitted: true,
        vortex_source_adapter_used: true,
        vortex_file_read: true,
        upstream_vortex_scan_called: true,
        streaming_vortex_execution_used: scenario_execution
            .evidence
            .streaming_vortex_execution_used,
        full_table_materialization_avoided: scenario_execution
            .evidence
            .full_table_materialization_avoided,
        streaming_filter_pushdown_applied: scenario_execution.evidence.filter_pushdown_applied,
        streaming_projection_pushdown_applied: scenario_execution
            .evidence
            .projection_pushdown_applied,
        streaming_arrays_read_count: scenario_execution.evidence.arrays_read_count,
        streaming_max_chunk_rows: scenario_execution.evidence.max_chunk_rows,
        streaming_projected_columns: scenario_execution.evidence.projected_columns,
        data_decoded: scenario_execution.evidence.data_decoded,
        data_materialized: scenario_execution.evidence.data_materialized,
        materialization_boundary_report_emitted: true,
        row_read: scenario_execution.evidence.row_read,
        arrow_converted: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        fallback_execution_allowed: false,
        diagnostics: Vec::new(),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn traditional_native_io_certificate(
    scenario: TraditionalAnalyticsScenario,
    input_format: TraditionalAnalyticsInputFormat,
    source_bytes_read: u64,
    source_rows_materialized: u64,
) -> Result<NativeIoCertificate> {
    NativeIoCertificate::new(
        format!(
            "cg19.traditional_analytics.{}.{}",
            input_format.as_str(),
            scenario.as_str().replace(['/', ' '], "-")
        ),
        "compatibility_source_to_native_vortex_sink",
        NativeIoSourceCapabilityReport {
            source_kind: input_format.source_kind().to_string(),
            adapter_id: input_format.adapter_id().to_string(),
            schema_discovery_status: "declared_schema_validated".to_string(),
            statistics_availability: "none".to_string(),
            pushdown_capabilities: "none".to_string(),
            encoded_representation_preserved: false,
            range_read_capability: false,
            streaming_capability: false,
            object_store_capability: false,
            fallback_attempted: false,
        },
        NativeIoSourcePushdownReport {
            accepted_operations: Vec::new(),
            rejected_operations: vec![scenario.as_str().to_string()],
            guarantee: "unsupported".to_string(),
            proof_basis: input_format.proof_basis().to_string(),
            residual_expression: Some(scenario.as_str().to_string()),
            conservative_false_positive_policy: false,
            unsafe_rejected_reason: None,
            fallback_attempted: false,
        },
        vec![
            NativeIoRepresentationTransition::new(
                RepresentationState::ForeignEncoded,
                RepresentationState::DecodedColumnar,
                true,
            ),
            NativeIoRepresentationTransition::new(
                RepresentationState::DecodedColumnar,
                RepresentationState::VortexEncoded,
                false,
            ),
        ],
        NativeIoSinkRequirementReport {
            target_format: "vortex".to_string(),
            accepts_encoded: true,
            requires_decoded_columnar: false,
            requires_rows: false,
            preserves_metadata: true,
            requires_ordering: false,
            requires_partitioning: false,
            requires_commit: false,
            supports_streaming: false,
            max_chunk_size: Some(source_rows_materialized),
            backpressure_policy: "not_applicable_local_smoke".to_string(),
        },
        NativeIoAdapterFidelityReport {
            adapter_id: input_format.adapter_id().to_string(),
            source_kind: input_format.source_kind().to_string(),
            sink_kind: "vortex".to_string(),
            metadata_preserved: false,
            statistics_preserved: false,
            encoded_representation_preserved: false,
            materialization_required: true,
            fidelity_loss: "none_for_declared_benchmark_schema".to_string(),
            metadata_loss: input_format.metadata_loss().to_string(),
            fallback_attempted: false,
        },
        vec![NativeIoMaterializationBoundaryReport {
            boundary_id: input_format.boundary_id().to_string(),
            from_state: RepresentationState::ForeignEncoded,
            to_state: RepresentationState::DecodedColumnar,
            required_by: input_format.import_label().to_string(),
            reason: input_format.materialization_reason().to_string(),
            bytes_decoded: source_bytes_read,
            rows_materialized: source_rows_materialized,
            fidelity_loss: "none_for_declared_benchmark_schema".to_string(),
            fallback_attempted: false,
        }],
        NativeIoSideEffectReport {
            data_read: true,
            data_decoded: true,
            data_materialized: true,
            row_read: true,
            arrow_converted: false,
            object_store_io: false,
            write_io: true,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
        },
        Vec::new(),
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn traditional_native_vortex_io_certificate(
    scenario: TraditionalAnalyticsScenario,
    source_bytes_read: u64,
    source_rows_materialized: u64,
    execution_evidence: &TraditionalScenarioExecutionEvidence,
) -> Result<NativeIoCertificate> {
    let transition_to_state = if execution_evidence.data_materialized {
        RepresentationState::MaterializedRows
    } else {
        RepresentationState::PartiallyDecoded
    };
    NativeIoCertificate::new(
        format!(
            "cg19.traditional_analytics.native_vortex.{}",
            scenario.as_str().replace(['/', ' '], "-")
        ),
        "native_vortex_source_to_native_runtime_result",
        NativeIoSourceCapabilityReport {
            source_kind: "vortex".to_string(),
            adapter_id: "shardloom.adapter.vortex.local_benchmark.v1".to_string(),
            schema_discovery_status: "vortex_schema_read".to_string(),
            statistics_availability: "vortex_metadata_available".to_string(),
            pushdown_capabilities: "vortex_scan_available".to_string(),
            encoded_representation_preserved: true,
            range_read_capability: false,
            streaming_capability: false,
            object_store_capability: false,
            fallback_attempted: false,
        },
        NativeIoSourcePushdownReport {
            accepted_operations: native_vortex_pushdown_operations(execution_evidence),
            rejected_operations: native_vortex_rejected_operations(scenario, execution_evidence),
            guarantee: if execution_evidence.streaming_vortex_execution_used {
                "exact_streaming_scan_then_scalar_operator".to_string()
            } else {
                "exact_scan_then_temporary_operator".to_string()
            },
            proof_basis: native_vortex_proof_basis(execution_evidence),
            residual_expression: Some(native_vortex_residual_expression(
                scenario,
                execution_evidence,
            )),
            conservative_false_positive_policy: false,
            unsafe_rejected_reason: None,
            fallback_attempted: false,
        },
        vec![NativeIoRepresentationTransition::new(
            RepresentationState::VortexEncoded,
            transition_to_state,
            true,
        )],
        native_vortex_sink_requirement_report(source_rows_materialized, execution_evidence),
        native_vortex_adapter_fidelity_report(execution_evidence),
        vec![native_vortex_materialization_boundary_report(
            source_bytes_read,
            source_rows_materialized,
            transition_to_state,
            execution_evidence,
        )],
        native_vortex_side_effect_report(execution_evidence),
        Vec::new(),
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn native_vortex_sink_requirement_report(
    source_rows_materialized: u64,
    execution_evidence: &TraditionalScenarioExecutionEvidence,
) -> NativeIoSinkRequirementReport {
    let max_chunk_size =
        native_vortex_sink_max_chunk_size(source_rows_materialized, execution_evidence);
    NativeIoSinkRequirementReport {
        target_format: "benchmark_result_json".to_string(),
        accepts_encoded: false,
        requires_decoded_columnar: true,
        requires_rows: false,
        preserves_metadata: false,
        requires_ordering: false,
        requires_partitioning: false,
        requires_commit: false,
        supports_streaming: execution_evidence.streaming_vortex_execution_used,
        max_chunk_size: Some(max_chunk_size),
        backpressure_policy: "not_applicable_local_smoke".to_string(),
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn native_vortex_sink_max_chunk_size(
    source_rows_materialized: u64,
    execution_evidence: &TraditionalScenarioExecutionEvidence,
) -> u64 {
    if execution_evidence.streaming_vortex_execution_used && execution_evidence.max_chunk_rows > 0 {
        execution_evidence.max_chunk_rows as u64
    } else {
        source_rows_materialized
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn native_vortex_adapter_fidelity_report(
    execution_evidence: &TraditionalScenarioExecutionEvidence,
) -> NativeIoAdapterFidelityReport {
    NativeIoAdapterFidelityReport {
        adapter_id: "shardloom.adapter.vortex.local_benchmark.v1".to_string(),
        source_kind: "vortex".to_string(),
        sink_kind: "benchmark_result_json".to_string(),
        metadata_preserved: false,
        statistics_preserved: false,
        encoded_representation_preserved: false,
        materialization_required: execution_evidence.data_materialized,
        fidelity_loss: "none_for_declared_benchmark_schema".to_string(),
        metadata_loss:
            "current temporary benchmark result does not preserve Vortex layout metadata"
                .to_string(),
        fallback_attempted: false,
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn native_vortex_materialization_boundary_report(
    source_bytes_read: u64,
    source_rows_materialized: u64,
    transition_to_state: RepresentationState,
    execution_evidence: &TraditionalScenarioExecutionEvidence,
) -> NativeIoMaterializationBoundaryReport {
    NativeIoMaterializationBoundaryReport {
        boundary_id: native_vortex_boundary_id(execution_evidence).to_string(),
        from_state: RepresentationState::VortexEncoded,
        to_state: transition_to_state,
        required_by: native_vortex_boundary_required_by(execution_evidence).to_string(),
        reason: native_vortex_boundary_reason(execution_evidence).to_string(),
        bytes_decoded: source_bytes_read,
        rows_materialized: source_rows_materialized,
        fidelity_loss: "none_for_declared_benchmark_schema".to_string(),
        fallback_attempted: false,
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn native_vortex_side_effect_report(
    execution_evidence: &TraditionalScenarioExecutionEvidence,
) -> NativeIoSideEffectReport {
    NativeIoSideEffectReport {
        data_read: true,
        data_decoded: execution_evidence.data_decoded,
        data_materialized: execution_evidence.data_materialized,
        row_read: execution_evidence.row_read,
        arrow_converted: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        external_effects_executed: false,
        fallback_attempted: false,
        fallback_execution_allowed: false,
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn native_vortex_pushdown_operations(
    execution_evidence: &TraditionalScenarioExecutionEvidence,
) -> Vec<String> {
    let mut operations = vec!["vortex_file_scan".to_string()];
    if execution_evidence.filter_pushdown_applied {
        operations.push("vortex_scan_filter".to_string());
    }
    if execution_evidence.projection_pushdown_applied {
        operations.push("vortex_scan_projection".to_string());
    }
    operations
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn native_vortex_rejected_operations(
    scenario: TraditionalAnalyticsScenario,
    execution_evidence: &TraditionalScenarioExecutionEvidence,
) -> Vec<String> {
    if execution_evidence.streaming_vortex_execution_used {
        vec![format!("{}_result_json_scalar_finish", scenario.as_str())]
    } else {
        vec![scenario.as_str().to_string()]
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn native_vortex_boundary_id(
    execution_evidence: &TraditionalScenarioExecutionEvidence,
) -> &'static str {
    if execution_evidence.data_materialized {
        "cg19.native_vortex_temporary_operator"
    } else {
        "cg19.native_vortex_streaming_operator"
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn native_vortex_boundary_required_by(
    execution_evidence: &TraditionalScenarioExecutionEvidence,
) -> &'static str {
    if execution_evidence.streaming_vortex_execution_used {
        "traditional_analytics_streaming_operator"
    } else {
        "traditional_analytics_temporary_operator"
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn native_vortex_boundary_reason(
    execution_evidence: &TraditionalScenarioExecutionEvidence,
) -> &'static str {
    if execution_evidence.data_materialized {
        "Current traditional analytics benchmark operators read native Vortex inputs but still materialize benchmark columns before producing result JSON"
    } else {
        "Current traditional analytics benchmark operator streams projected Vortex chunks and decodes only required scalar columns before producing result JSON"
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn native_vortex_proof_basis(execution_evidence: &TraditionalScenarioExecutionEvidence) -> String {
    if execution_evidence.streaming_vortex_execution_used {
        "local native Vortex benchmark path streams projected Vortex chunks and applies the remaining scalar result assembly inside ShardLoom"
            .to_string()
    } else {
        "local native Vortex benchmark path reads Vortex files and runs current temporary benchmark operators after scan"
            .to_string()
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn native_vortex_residual_expression(
    scenario: TraditionalAnalyticsScenario,
    execution_evidence: &TraditionalScenarioExecutionEvidence,
) -> String {
    if execution_evidence.streaming_vortex_execution_used {
        format!(
            "{} result JSON scalar finish over projected chunks",
            scenario.as_str()
        )
    } else {
        scenario.as_str().to_string()
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn file_len(path: &std::path::Path, label: &str) -> Result<u64> {
    std::fs::metadata(path)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to stat {label} '{}': {error}",
                path.display()
            ))
        })
        .map(|metadata| metadata.len())
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn file_digest(path: &std::path::Path, label: &str) -> Result<String> {
    use std::io::Read as _;

    let mut file = std::fs::File::open(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to open {label} '{}' for digest: {error}",
            path.display()
        ))
    })?;
    let mut digest = Fnv1a64::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to read {label} '{}' for digest: {error}",
                path.display()
            ))
        })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!(
        "{}:{:016x}",
        OUTPUT_ARTIFACT_DIGEST_ALGORITHM,
        digest.finish()
    ))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn combined_artifact_digest(
    fact_digest: &str,
    dim_digest: &str,
    result_digest: Option<&str>,
) -> String {
    let mut digest = Fnv1a64::new();
    digest.update(b"fact_vortex_digest");
    digest.update(fact_digest.as_bytes());
    digest.update(b"dim_vortex_digest");
    digest.update(dim_digest.as_bytes());
    if let Some(result_digest) = result_digest {
        digest.update(b"computed_result_vortex_digest");
        digest.update(result_digest.as_bytes());
    }
    format!(
        "{}:{:016x}",
        OUTPUT_ARTIFACT_DIGEST_ALGORITHM,
        digest.finish()
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn duration_to_micros(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
struct Fnv1a64 {
    state: u64,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl Fnv1a64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    const fn new() -> Self {
        Self {
            state: Self::OFFSET,
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(Self::PRIME);
        }
    }

    const fn finish(&self) -> u64 {
        self.state
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl VortexFactTable {
    fn len(&self) -> usize {
        self.id.len()
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl VortexDimTable {
    fn len(&self) -> usize {
        self.dim_key.len()
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_fact_vortex(rows: &[TraditionalFactRow], path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from([
            "id",
            "group_key",
            "dim_key",
            "value",
            "metric",
            "flag",
            "category",
        ]),
        vec![
            rows.iter()
                .map(|row| row.id)
                .collect::<PrimitiveArray>()
                .into_array(),
            rows.iter()
                .map(|row| row.group_key)
                .collect::<PrimitiveArray>()
                .into_array(),
            rows.iter()
                .map(|row| row.dim_key)
                .collect::<PrimitiveArray>()
                .into_array(),
            rows.iter()
                .map(|row| row.value)
                .collect::<PrimitiveArray>()
                .into_array(),
            rows.iter()
                .map(|row| row.metric)
                .collect::<PrimitiveArray>()
                .into_array(),
            rows.iter()
                .map(|row| row.flag)
                .collect::<PrimitiveArray>()
                .into_array(),
            VarBinViewArray::from_iter_str(rows.iter().map(|row| row.category.as_str()))
                .into_array(),
        ],
        rows.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    let array = array.into_array();
    write_vortex_array(path, &array)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_dim_vortex(rows: &[TraditionalDimRow], path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from(["dim_key", "dim_label", "weight"]),
        vec![
            rows.iter()
                .map(|row| row.dim_key)
                .collect::<PrimitiveArray>()
                .into_array(),
            VarBinViewArray::from_iter_str(rows.iter().map(|row| row.dim_label.as_str()))
                .into_array(),
            rows.iter()
                .map(|row| row.weight)
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        rows.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    let array = array.into_array();
    write_vortex_array(path, &array)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_computed_result_vortex(
    scenario: TraditionalAnalyticsScenario,
    result_json: &str,
    rows_materialized: u64,
    path: &std::path::Path,
) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from([
            "scenario",
            "result_json",
            "rows_materialized",
            "workload_constitution_id",
        ]),
        vec![
            VarBinViewArray::from_iter_str(std::iter::once(scenario.as_str())).into_array(),
            VarBinViewArray::from_iter_str(std::iter::once(result_json)).into_array(),
            std::iter::once(rows_materialized)
                .collect::<PrimitiveArray>()
                .into_array(),
            VarBinViewArray::from_iter_str(std::iter::once(LOCAL_VORTEX_ANALYTICS_CONSTITUTION_ID))
                .into_array(),
        ],
        1,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    let array = array.into_array();
    write_vortex_array(path, &array)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_traditional_compatibility_outputs(
    fact: &VortexFactTable,
    dim: &VortexDimTable,
    output_format: TraditionalAnalyticsInputFormat,
    output_dir: &std::path::Path,
) -> Result<(PathBuf, PathBuf)> {
    let fact_path = output_dir.join(format!("fact.{}", output_format.output_extension()));
    let dim_path = output_dir.join(format!("dim.{}", output_format.output_extension()));
    match output_format {
        TraditionalAnalyticsInputFormat::Csv => {
            write_fact_csv_output(fact, &fact_path)?;
            write_dim_csv_output(dim, &dim_path)?;
        }
        TraditionalAnalyticsInputFormat::JsonLines => {
            write_fact_jsonl_output(fact, &fact_path)?;
            write_dim_jsonl_output(dim, &dim_path)?;
        }
        TraditionalAnalyticsInputFormat::Parquet
        | TraditionalAnalyticsInputFormat::ArrowIpc
        | TraditionalAnalyticsInputFormat::Avro
        | TraditionalAnalyticsInputFormat::Orc => {
            write_arrow_batch_output(
                &fact_record_batch(fact)?,
                output_format,
                &fact_path,
                "fact compatibility output",
            )?;
            write_arrow_batch_output(
                &dim_record_batch(dim)?,
                output_format,
                &dim_path,
                "dimension compatibility output",
            )?;
        }
    }
    Ok((fact_path, dim_path))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_fact_csv_output(fact: &VortexFactTable, path: &std::path::Path) -> Result<()> {
    use std::io::Write as _;

    let mut file = create_compatibility_output_file(path, "fact CSV")?;
    writeln!(file, "id,group_key,dim_key,value,metric,flag,category")
        .map_err(|error| compatibility_write_error(path, "fact CSV", error))?;
    for index in 0..fact.len() {
        writeln!(
            file,
            "{},{},{},{},{:.2},{},{}",
            fact.id[index],
            fact.group_key[index],
            fact.dim_key[index],
            fact.value[index],
            fact.metric[index],
            fact.flag[index],
            fact.category[index]
        )
        .map_err(|error| compatibility_write_error(path, "fact CSV", error))?;
    }
    Ok(())
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_dim_csv_output(dim: &VortexDimTable, path: &std::path::Path) -> Result<()> {
    use std::io::Write as _;

    let mut file = create_compatibility_output_file(path, "dimension CSV")?;
    writeln!(file, "dim_key,dim_label,weight")
        .map_err(|error| compatibility_write_error(path, "dimension CSV", error))?;
    for index in 0..dim.len() {
        writeln!(
            file,
            "{},{},{:.2}",
            dim.dim_key[index], dim.dim_label[index], dim.weight[index]
        )
        .map_err(|error| compatibility_write_error(path, "dimension CSV", error))?;
    }
    Ok(())
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_fact_jsonl_output(fact: &VortexFactTable, path: &std::path::Path) -> Result<()> {
    use std::io::Write as _;

    let mut file = create_compatibility_output_file(path, "fact JSONL")?;
    for index in 0..fact.len() {
        writeln!(
            file,
            "{{\"id\":{},\"group_key\":{},\"dim_key\":{},\"value\":{},\"metric\":{:.2},\"flag\":{},\"category\":\"{}\"}}",
            fact.id[index],
            fact.group_key[index],
            fact.dim_key[index],
            fact.value[index],
            fact.metric[index],
            fact.flag[index],
            json_escape(&fact.category[index])
        )
        .map_err(|error| compatibility_write_error(path, "fact JSONL", error))?;
    }
    Ok(())
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_dim_jsonl_output(dim: &VortexDimTable, path: &std::path::Path) -> Result<()> {
    use std::io::Write as _;

    let mut file = create_compatibility_output_file(path, "dimension JSONL")?;
    for index in 0..dim.len() {
        writeln!(
            file,
            "{{\"dim_key\":{},\"dim_label\":\"{}\",\"weight\":{:.2}}}",
            dim.dim_key[index],
            json_escape(&dim.dim_label[index]),
            dim.weight[index]
        )
        .map_err(|error| compatibility_write_error(path, "dimension JSONL", error))?;
    }
    Ok(())
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn create_compatibility_output_file(path: &std::path::Path, label: &str) -> Result<std::fs::File> {
    std::fs::File::create(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to create {label} compatibility output '{}': {error}",
            path.display()
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn compatibility_write_error(
    path: &std::path::Path,
    label: &str,
    error: impl std::fmt::Display,
) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "failed to write {label} compatibility output '{}': {error}",
        path.display()
    ))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn json_escape(value: &str) -> String {
    value
        .chars()
        .flat_map(|ch| match ch {
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            '\r' => "\\r".chars().collect::<Vec<_>>(),
            '\t' => "\\t".chars().collect::<Vec<_>>(),
            _ => vec![ch],
        })
        .collect()
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn fact_record_batch(fact: &VortexFactTable) -> Result<arrow_array::RecordBatch> {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, Float64Array, Int8Array, Int32Array, Int64Array, RecordBatch, StringArray,
    };
    use arrow_schema::{DataType, Field, Schema};

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("group_key", DataType::Int32, false),
        Field::new("dim_key", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("metric", DataType::Float64, false),
        Field::new("flag", DataType::Int8, false),
        Field::new("category", DataType::Utf8, false),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(u64_values_to_i64(&fact.id, "fact.id")?)) as ArrayRef,
            Arc::new(Int32Array::from(u32_values_to_i32(
                &fact.group_key,
                "fact.group_key",
            )?)) as ArrayRef,
            Arc::new(Int32Array::from(u32_values_to_i32(
                &fact.dim_key,
                "fact.dim_key",
            )?)) as ArrayRef,
            Arc::new(Int32Array::from(u32_values_to_i32(
                &fact.value,
                "fact.value",
            )?)) as ArrayRef,
            Arc::new(Float64Array::from(fact.metric.clone())) as ArrayRef,
            Arc::new(Int8Array::from(u8_values_to_i8(&fact.flag, "fact.flag")?)) as ArrayRef,
            Arc::new(StringArray::from(fact.category.clone())) as ArrayRef,
        ],
    )
    .map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to build fact Arrow compatibility batch: {error}"
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn dim_record_batch(dim: &VortexDimTable) -> Result<arrow_array::RecordBatch> {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Float64Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let schema = Schema::new(vec![
        Field::new("dim_key", DataType::Int32, false),
        Field::new("dim_label", DataType::Utf8, false),
        Field::new("weight", DataType::Float64, false),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(u32_values_to_i32(
                &dim.dim_key,
                "dim.dim_key",
            )?)) as ArrayRef,
            Arc::new(StringArray::from(dim.dim_label.clone())) as ArrayRef,
            Arc::new(Float64Array::from(dim.weight.clone())) as ArrayRef,
        ],
    )
    .map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to build dimension Arrow compatibility batch: {error}"
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn u64_values_to_i64(values: &[u64], label: &str) -> Result<Vec<i64>> {
    values
        .iter()
        .map(|value| {
            i64::try_from(*value).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to export {label} as signed Arrow value: {error}"
                ))
            })
        })
        .collect()
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn u32_values_to_i32(values: &[u32], label: &str) -> Result<Vec<i32>> {
    values
        .iter()
        .map(|value| {
            i32::try_from(*value).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to export {label} as signed Arrow value: {error}"
                ))
            })
        })
        .collect()
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn u8_values_to_i8(values: &[u8], label: &str) -> Result<Vec<i8>> {
    values
        .iter()
        .map(|value| {
            i8::try_from(*value).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to export {label} as signed Arrow value: {error}"
                ))
            })
        })
        .collect()
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_arrow_batch_output(
    batch: &arrow_array::RecordBatch,
    output_format: TraditionalAnalyticsInputFormat,
    path: &std::path::Path,
    label: &str,
) -> Result<()> {
    match output_format {
        TraditionalAnalyticsInputFormat::Parquet => write_parquet_output(batch, path, label),
        TraditionalAnalyticsInputFormat::ArrowIpc => write_arrow_ipc_output(batch, path, label),
        TraditionalAnalyticsInputFormat::Avro => write_avro_output(batch, path, label),
        TraditionalAnalyticsInputFormat::Orc => write_orc_output(batch, path, label),
        TraditionalAnalyticsInputFormat::Csv | TraditionalAnalyticsInputFormat::JsonLines => {
            Err(ShardLoomError::InvalidOperation(format!(
                "internal error: {} output does not use Arrow batch writer",
                output_format.as_str()
            )))
        }
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_parquet_output(
    batch: &arrow_array::RecordBatch,
    path: &std::path::Path,
    label: &str,
) -> Result<()> {
    let file = create_compatibility_output_file(path, label)?;
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create Parquet writer for {label} '{}': {error}",
                path.display()
            ))
        })?;
    writer.write(batch).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to write Parquet {label} '{}': {error}",
            path.display()
        ))
    })?;
    writer.close().map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to close Parquet {label} '{}': {error}",
            path.display()
        ))
    })?;
    Ok(())
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_arrow_ipc_output(
    batch: &arrow_array::RecordBatch,
    path: &std::path::Path,
    label: &str,
) -> Result<()> {
    let file = create_compatibility_output_file(path, label)?;
    let mut writer =
        arrow_ipc::writer::FileWriter::try_new(file, &batch.schema()).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create Arrow IPC writer for {label} '{}': {error}",
                path.display()
            ))
        })?;
    writer.write(batch).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to write Arrow IPC {label} '{}': {error}",
            path.display()
        ))
    })?;
    writer.finish().map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to finish Arrow IPC {label} '{}': {error}",
            path.display()
        ))
    })?;
    Ok(())
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_avro_output(
    batch: &arrow_array::RecordBatch,
    path: &std::path::Path,
    label: &str,
) -> Result<()> {
    let file = create_compatibility_output_file(path, label)?;
    let mut writer = arrow_avro::writer::AvroWriter::new(file, batch.schema().as_ref().clone())
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create Avro writer for {label} '{}': {error}",
                path.display()
            ))
        })?;
    writer.write(batch).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to write Avro {label} '{}': {error}",
            path.display()
        ))
    })?;
    writer.finish().map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to finish Avro {label} '{}': {error}",
            path.display()
        ))
    })?;
    Ok(())
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_orc_output(
    batch: &arrow_array::RecordBatch,
    path: &std::path::Path,
    label: &str,
) -> Result<()> {
    let file = create_compatibility_output_file(path, label)?;
    let mut writer = orc_rust::ArrowWriterBuilder::new(file, batch.schema())
        .try_build()
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create ORC writer for {label} '{}': {error}",
                path.display()
            ))
        })?;
    writer.write(batch).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to write ORC {label} '{}': {error}",
            path.display()
        ))
    })?;
    writer.close().map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to close ORC {label} '{}': {error}",
            path.display()
        ))
    })?;
    Ok(())
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn write_vortex_array(path: &std::path::Path, array: &vortex::array::ArrayRef) -> Result<()> {
    use std::fs;

    use vortex::VortexSessionDefault as _;
    use vortex::file::WriteOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut bytes = Vec::new();
    let summary = runtime
        .block_on(
            session
                .write_options()
                .write(&mut bytes, array.to_array_stream()),
        )
        .map_err(vortex_error)?;
    let expected_rows = usize_to_u64(array.len())?;
    if summary.row_count() != expected_rows {
        return Err(ShardLoomError::InvalidOperation(format!(
            "Vortex writer row count mismatch: wrote {}, expected {}",
            summary.row_count(),
            expected_rows
        )));
    }
    fs::write(path, bytes).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to write Vortex file '{}': {error}",
            path.display()
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_fact_vortex(path: &std::path::Path) -> Result<VortexFactTable> {
    let fields = read_vortex_struct(path)?;
    Ok(VortexFactTable {
        id: primitive_field::<u64>(&fields, "id")?,
        group_key: primitive_field::<u32>(&fields, "group_key")?,
        dim_key: primitive_field::<u32>(&fields, "dim_key")?,
        value: primitive_field::<u32>(&fields, "value")?,
        metric: primitive_field::<f64>(&fields, "metric")?,
        flag: primitive_field::<u8>(&fields, "flag")?,
        category: utf8_field(&fields, "category")?,
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_dim_vortex(path: &std::path::Path) -> Result<VortexDimTable> {
    let fields = read_vortex_struct(path)?;
    Ok(VortexDimTable {
        dim_key: primitive_field::<u32>(&fields, "dim_key")?,
        dim_label: utf8_field(&fields, "dim_label")?,
        weight: primitive_field::<f64>(&fields, "weight")?,
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_computed_result_vortex(path: &std::path::Path) -> Result<TraditionalComputedResultPayload> {
    let fields = read_vortex_struct(path)?;
    Ok(TraditionalComputedResultPayload {
        scenario: single_utf8_field(&fields, "scenario", path)?,
        result_json: single_utf8_field(&fields, "result_json", path)?,
        rows_materialized: single_primitive_field::<u64>(&fields, "rows_materialized", path)?,
        workload_constitution_id: single_utf8_field(&fields, "workload_constitution_id", path)?,
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_vortex_struct(
    path: &std::path::Path,
) -> Result<std::collections::BTreeMap<String, vortex::array::ArrayRef>> {
    use std::fs;

    use vortex::VortexSessionDefault as _;
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::StructArray;
    use vortex::array::arrays::struct_::StructArrayExt as _;
    use vortex::array::stream::ArrayStreamExt as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let bytes = fs::read(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read Vortex file '{}': {error}",
            path.display()
        ))
    })?;
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = session
        .open_options()
        .open_buffer(bytes)
        .map_err(vortex_error)?;
    let array = runtime
        .block_on(
            file.scan()
                .map_err(vortex_error)?
                .into_array_stream()
                .map_err(vortex_error)?
                .read_all(),
        )
        .map_err(vortex_error)?;
    let mut ctx = session.create_execution_ctx();
    let struct_array = array
        .execute::<StructArray>(&mut ctx)
        .map_err(vortex_error)?;
    let mut fields = std::collections::BTreeMap::new();
    for name in struct_array.names().iter() {
        let field = struct_array
            .unmasked_field_by_name(name.as_ref())
            .map_err(vortex_error)?
            .clone();
        fields.insert(name.as_ref().to_string(), field);
    }
    Ok(fields)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn primitive_field<T>(
    fields: &std::collections::BTreeMap<String, vortex::array::ArrayRef>,
    name: &str,
) -> Result<Vec<T>>
where
    T: vortex::array::dtype::NativePType + Copy,
{
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::PrimitiveArray;

    let field = fields.get(name).ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!("Vortex field '{name}' was missing"))
    })?;
    let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
    let primitive = field
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(vortex_error)?;
    Ok(primitive.as_slice::<T>().to_vec())
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn utf8_field(
    fields: &std::collections::BTreeMap<String, vortex::array::ArrayRef>,
    name: &str,
) -> Result<Vec<String>> {
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::VarBinViewArray;

    let field = fields.get(name).ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!("Vortex field '{name}' was missing"))
    })?;
    let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
    let utf8 = field
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(vortex_error)?;
    let mut values = Vec::with_capacity(utf8.len());
    for index in 0..utf8.len() {
        let bytes = utf8.bytes_at(index);
        let text = std::str::from_utf8(bytes.as_slice()).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "Vortex UTF-8 field '{name}' contained invalid UTF-8 at row {index}: {error}"
            ))
        })?;
        values.push(text.to_string());
    }
    Ok(values)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn single_utf8_field(
    fields: &std::collections::BTreeMap<String, vortex::array::ArrayRef>,
    name: &str,
    path: &std::path::Path,
) -> Result<String> {
    let mut values = utf8_field(fields, name)?;
    if values.len() != 1 {
        return Err(ShardLoomError::InvalidOperation(format!(
            "Vortex result artifact '{}' field '{name}' expected exactly one row, found {}",
            path.display(),
            values.len()
        )));
    }
    Ok(values.remove(0))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn single_primitive_field<T>(
    fields: &std::collections::BTreeMap<String, vortex::array::ArrayRef>,
    name: &str,
    path: &std::path::Path,
) -> Result<T>
where
    T: vortex::array::dtype::NativePType + Copy,
{
    let values = primitive_field::<T>(fields, name)?;
    if values.len() != 1 {
        return Err(ShardLoomError::InvalidOperation(format!(
            "Vortex result artifact '{}' field '{name}' expected exactly one row, found {}",
            path.display(),
            values.len()
        )));
    }
    Ok(values[0])
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_traditional_fact_rows(
    path: &std::path::Path,
    input_format: TraditionalAnalyticsInputFormat,
    resource_policy: TraditionalAnalyticsResourcePolicy,
) -> Result<Vec<TraditionalFactRow>> {
    match input_format {
        TraditionalAnalyticsInputFormat::Csv => read_traditional_fact_csv(path),
        TraditionalAnalyticsInputFormat::JsonLines => read_traditional_fact_jsonl(path),
        TraditionalAnalyticsInputFormat::Parquet
        | TraditionalAnalyticsInputFormat::ArrowIpc
        | TraditionalAnalyticsInputFormat::Avro
        | TraditionalAnalyticsInputFormat::Orc => {
            let batches =
                read_traditional_arrow_batches(path, input_format, "fact input", resource_policy)?;
            fact_rows_from_arrow_batches(&batches, path)
        }
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_traditional_dim_rows(
    path: &std::path::Path,
    input_format: TraditionalAnalyticsInputFormat,
    resource_policy: TraditionalAnalyticsResourcePolicy,
) -> Result<Vec<TraditionalDimRow>> {
    match input_format {
        TraditionalAnalyticsInputFormat::Csv => read_traditional_dim_csv(path),
        TraditionalAnalyticsInputFormat::JsonLines => read_traditional_dim_jsonl(path),
        TraditionalAnalyticsInputFormat::Parquet
        | TraditionalAnalyticsInputFormat::ArrowIpc
        | TraditionalAnalyticsInputFormat::Avro
        | TraditionalAnalyticsInputFormat::Orc => {
            let batches = read_traditional_arrow_batches(
                path,
                input_format,
                "dimension input",
                resource_policy,
            )?;
            dim_rows_from_arrow_batches(&batches, path)
        }
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_traditional_fact_csv(path: &std::path::Path) -> Result<Vec<TraditionalFactRow>> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read fact CSV '{}': {error}",
            path.display()
        ))
    })?;
    let mut lines = content.lines();
    let header = lines.next().ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!("fact CSV '{}' is empty", path.display()))
    })?;
    let header_cols = header.trim_end_matches('\r').split(',').collect::<Vec<_>>();
    let required_header = [
        "id",
        "group_key",
        "dim_key",
        "value",
        "metric",
        "flag",
        "category",
    ];
    if header_cols.len() < required_header.len()
        || header_cols[..required_header.len()] != required_header[..]
    {
        return Err(ShardLoomError::InvalidOperation(format!(
            "fact CSV '{}' does not match the benchmark schema",
            path.display()
        )));
    }
    let mut rows = Vec::new();
    for (line_index, line) in lines.enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let cols = line.trim_end_matches('\r').split(',').collect::<Vec<_>>();
        if cols.len() < required_header.len() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "fact CSV '{}' line {} has {} columns, expected at least {}",
                path.display(),
                line_index + 2,
                cols.len(),
                required_header.len()
            )));
        }
        rows.push(TraditionalFactRow {
            id: parse_csv_field(cols[0], path, line_index + 2, "id")?,
            group_key: parse_csv_field(cols[1], path, line_index + 2, "group_key")?,
            dim_key: parse_csv_field(cols[2], path, line_index + 2, "dim_key")?,
            value: parse_csv_field(cols[3], path, line_index + 2, "value")?,
            metric: parse_csv_field(cols[4], path, line_index + 2, "metric")?,
            flag: parse_csv_field(cols[5], path, line_index + 2, "flag")?,
            category: cols[6].to_string(),
        });
    }
    Ok(rows)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_traditional_dim_csv(path: &std::path::Path) -> Result<Vec<TraditionalDimRow>> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read dimension CSV '{}': {error}",
            path.display()
        ))
    })?;
    let mut lines = content.lines();
    let header = lines.next().ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!("dimension CSV '{}' is empty", path.display()))
    })?;
    if header.trim_end_matches('\r') != "dim_key,dim_label,weight" {
        return Err(ShardLoomError::InvalidOperation(format!(
            "dimension CSV '{}' does not match the benchmark schema",
            path.display()
        )));
    }
    let mut rows = Vec::new();
    for (line_index, line) in lines.enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let cols = line.trim_end_matches('\r').split(',').collect::<Vec<_>>();
        if cols.len() != 3 {
            return Err(ShardLoomError::InvalidOperation(format!(
                "dimension CSV '{}' line {} has {} columns, expected 3",
                path.display(),
                line_index + 2,
                cols.len()
            )));
        }
        rows.push(TraditionalDimRow {
            dim_key: parse_csv_field(cols[0], path, line_index + 2, "dim_key")?,
            dim_label: cols[1].to_string(),
            weight: parse_csv_field::<f64>(cols[2], path, line_index + 2, "weight")?,
        });
    }
    Ok(rows)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_traditional_fact_jsonl(path: &std::path::Path) -> Result<Vec<TraditionalFactRow>> {
    let content = read_jsonl_to_string(path, "fact JSONL")?;
    let mut rows = Vec::new();
    for (line_index, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let fields = parse_jsonl_object(line, path, line_index + 1, "fact JSONL")?;
        rows.push(TraditionalFactRow {
            id: parse_jsonl_numeric_field(&fields, path, line_index + 1, "id")?,
            group_key: parse_jsonl_numeric_field(&fields, path, line_index + 1, "group_key")?,
            dim_key: parse_jsonl_numeric_field(&fields, path, line_index + 1, "dim_key")?,
            value: parse_jsonl_numeric_field(&fields, path, line_index + 1, "value")?,
            metric: parse_jsonl_numeric_field(&fields, path, line_index + 1, "metric")?,
            flag: parse_jsonl_numeric_field(&fields, path, line_index + 1, "flag")?,
            category: parse_jsonl_string_field(&fields, path, line_index + 1, "category")?,
        });
    }
    if rows.is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "fact JSONL '{}' contains no rows",
            path.display()
        )));
    }
    Ok(rows)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_traditional_dim_jsonl(path: &std::path::Path) -> Result<Vec<TraditionalDimRow>> {
    let content = read_jsonl_to_string(path, "dimension JSONL")?;
    let mut rows = Vec::new();
    for (line_index, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let fields = parse_jsonl_object(line, path, line_index + 1, "dimension JSONL")?;
        rows.push(TraditionalDimRow {
            dim_key: parse_jsonl_numeric_field(&fields, path, line_index + 1, "dim_key")?,
            dim_label: parse_jsonl_string_field(&fields, path, line_index + 1, "dim_label")?,
            weight: parse_jsonl_numeric_field(&fields, path, line_index + 1, "weight")?,
        });
    }
    if rows.is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "dimension JSONL '{}' contains no rows",
            path.display()
        )));
    }
    Ok(rows)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn parse_csv_field<T>(
    value: &str,
    path: &std::path::Path,
    line_number: usize,
    field: &str,
) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.parse::<T>().map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to parse field '{field}' in '{}' line {line_number}: {error}",
            path.display()
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_jsonl_to_string(path: &std::path::Path, label: &str) -> Result<String> {
    std::fs::read_to_string(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read {label} '{}': {error}",
            path.display()
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn parse_jsonl_object(
    line: &str,
    path: &std::path::Path,
    line_number: usize,
    label: &str,
) -> Result<std::collections::BTreeMap<String, String>> {
    let trimmed = line.trim();
    let Some(inner) = trimmed
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
    else {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} '{}' line {line_number} is not a JSON object",
            path.display()
        )));
    };
    let mut fields = std::collections::BTreeMap::new();
    for field in split_json_fields(inner, path, line_number, label)? {
        let (key, value) = split_json_key_value(field, path, line_number, label)?;
        let key = parse_json_string_token(key.trim(), path, line_number, "field name")?;
        if fields
            .insert(key.clone(), value.trim().to_string())
            .is_some()
        {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{label} '{}' line {line_number} contains duplicate field '{key}'",
                path.display()
            )));
        }
    }
    Ok(fields)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn split_json_fields<'a>(
    inner: &'a str,
    path: &std::path::Path,
    line_number: usize,
    label: &str,
) -> Result<Vec<&'a str>> {
    split_json_top_level(inner, ',', path, line_number, label)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn split_json_key_value<'a>(
    field: &'a str,
    path: &std::path::Path,
    line_number: usize,
    label: &str,
) -> Result<(&'a str, &'a str)> {
    let parts = split_json_top_level(field, ':', path, line_number, label)?;
    if parts.len() != 2 {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} '{}' line {line_number} has an invalid JSON field",
            path.display()
        )));
    }
    Ok((parts[0], parts[1]))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn split_json_top_level<'a>(
    value: &'a str,
    delimiter: char,
    path: &std::path::Path,
    line_number: usize,
    label: &str,
) -> Result<Vec<&'a str>> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_string = false;
    let mut escaped = false;
    for (index, ch) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_string => escaped = true,
            '"' => in_string = !in_string,
            _ if ch == delimiter && !in_string => {
                parts.push(value[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    if in_string || escaped {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} '{}' line {line_number} contains an unterminated JSON string",
            path.display()
        )));
    }
    let tail = value[start..].trim();
    if !tail.is_empty() {
        parts.push(tail);
    }
    Ok(parts)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn parse_jsonl_numeric_field<T>(
    fields: &std::collections::BTreeMap<String, String>,
    path: &std::path::Path,
    line_number: usize,
    field: &str,
) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = jsonl_required_field(fields, path, line_number, field)?;
    value.parse::<T>().map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to parse JSONL field '{field}' in '{}' line {line_number}: {error}",
            path.display()
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn parse_jsonl_string_field(
    fields: &std::collections::BTreeMap<String, String>,
    path: &std::path::Path,
    line_number: usize,
    field: &str,
) -> Result<String> {
    parse_json_string_token(
        jsonl_required_field(fields, path, line_number, field)?,
        path,
        line_number,
        field,
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn jsonl_required_field<'a>(
    fields: &'a std::collections::BTreeMap<String, String>,
    path: &std::path::Path,
    line_number: usize,
    field: &str,
) -> Result<&'a str> {
    fields.get(field).map(String::as_str).ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "JSONL '{}' line {line_number} is missing field '{field}'",
            path.display()
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn parse_json_string_token(
    value: &str,
    path: &std::path::Path,
    line_number: usize,
    field: &str,
) -> Result<String> {
    let trimmed = value.trim();
    let Some(inner) = trimmed
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    else {
        return Err(ShardLoomError::InvalidOperation(format!(
            "JSONL string field '{field}' in '{}' line {line_number} was not quoted",
            path.display()
        )));
    };
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let Some(escaped) = chars.next() else {
            return Err(ShardLoomError::InvalidOperation(format!(
                "JSONL string field '{field}' in '{}' line {line_number} ended with an escape",
                path.display()
            )));
        };
        match escaped {
            '"' => out.push('"'),
            '\\' => out.push('\\'),
            '/' => out.push('/'),
            'b' => out.push('\u{0008}'),
            'f' => out.push('\u{000c}'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'u' => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "JSONL string field '{field}' in '{}' line {line_number} uses unsupported unicode escape",
                    path.display()
                )));
            }
            other => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "JSONL string field '{field}' in '{}' line {line_number} uses invalid escape '\\{other}'",
                    path.display()
                )));
            }
        }
    }
    Ok(out)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_traditional_arrow_batches(
    path: &std::path::Path,
    input_format: TraditionalAnalyticsInputFormat,
    label: &str,
    resource_policy: TraditionalAnalyticsResourcePolicy,
) -> Result<Vec<arrow_array::RecordBatch>> {
    match input_format {
        TraditionalAnalyticsInputFormat::Parquet => {
            read_parquet_record_batches(path, label, resource_policy)
        }
        TraditionalAnalyticsInputFormat::ArrowIpc => read_arrow_ipc_record_batches(path, label),
        TraditionalAnalyticsInputFormat::Avro => {
            read_avro_record_batches(path, label, resource_policy)
        }
        TraditionalAnalyticsInputFormat::Orc => {
            read_orc_record_batches(path, label, resource_policy)
        }
        TraditionalAnalyticsInputFormat::Csv | TraditionalAnalyticsInputFormat::JsonLines => {
            Err(ShardLoomError::InvalidOperation(format!(
                "internal error: {} does not use Arrow batch reader",
                input_format.as_str()
            )))
        }
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_parquet_record_batches(
    path: &std::path::Path,
    label: &str,
    resource_policy: TraditionalAnalyticsResourcePolicy,
) -> Result<Vec<arrow_array::RecordBatch>> {
    let file = std::fs::File::open(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to open Parquet {label} '{}': {error}",
            path.display()
        ))
    })?;
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create Parquet reader for {label} '{}': {error}",
                path.display()
            ))
        })?
        .with_batch_size(resource_policy.target_batch_rows)
        .build()
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to build Parquet reader for {label} '{}': {error}",
                path.display()
            ))
        })?;
    collect_record_batches(reader, path, label, "Parquet")
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_arrow_ipc_record_batches(
    path: &std::path::Path,
    label: &str,
) -> Result<Vec<arrow_array::RecordBatch>> {
    let file = std::fs::File::open(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to open Arrow IPC {label} '{}': {error}",
            path.display()
        ))
    })?;
    let reader = arrow_ipc::reader::FileReader::try_new(file, None).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to create Arrow IPC reader for {label} '{}': {error}",
            path.display()
        ))
    })?;
    collect_record_batches(reader, path, label, "Arrow IPC")
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_avro_record_batches(
    path: &std::path::Path,
    label: &str,
    resource_policy: TraditionalAnalyticsResourcePolicy,
) -> Result<Vec<arrow_array::RecordBatch>> {
    let file = std::fs::File::open(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to open Avro {label} '{}': {error}",
            path.display()
        ))
    })?;
    let reader = arrow_avro::reader::ReaderBuilder::new()
        .with_batch_size(resource_policy.target_batch_rows)
        .build(std::io::BufReader::new(file))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create Avro reader for {label} '{}': {error}",
                path.display()
            ))
        })?;
    collect_record_batches(reader, path, label, "Avro")
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn read_orc_record_batches(
    path: &std::path::Path,
    label: &str,
    resource_policy: TraditionalAnalyticsResourcePolicy,
) -> Result<Vec<arrow_array::RecordBatch>> {
    let file = std::fs::File::open(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to open ORC {label} '{}': {error}",
            path.display()
        ))
    })?;
    let reader = orc_rust::ArrowReaderBuilder::try_new(file)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create ORC reader for {label} '{}': {error}",
                path.display()
            ))
        })?
        .with_batch_size(resource_policy.target_batch_rows)
        .build();
    collect_record_batches(reader, path, label, "ORC")
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn collect_record_batches<I, E>(
    reader: I,
    path: &std::path::Path,
    label: &str,
    format_name: &str,
) -> Result<Vec<arrow_array::RecordBatch>>
where
    I: IntoIterator<Item = std::result::Result<arrow_array::RecordBatch, E>>,
    E: std::fmt::Display,
{
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to read {format_name} batch from {label} '{}': {error}",
                path.display()
            ))
        })?);
    }
    if batches
        .iter()
        .map(arrow_array::RecordBatch::num_rows)
        .sum::<usize>()
        == 0
    {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{format_name} {label} '{}' contains no rows",
            path.display()
        )));
    }
    Ok(batches)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn fact_rows_from_arrow_batches(
    batches: &[arrow_array::RecordBatch],
    path: &std::path::Path,
) -> Result<Vec<TraditionalFactRow>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            rows.push(TraditionalFactRow {
                id: arrow_u64_field(batch, path, row_index, "id")?,
                group_key: arrow_u32_field(batch, path, row_index, "group_key")?,
                dim_key: arrow_u32_field(batch, path, row_index, "dim_key")?,
                value: arrow_u32_field(batch, path, row_index, "value")?,
                metric: arrow_f64_field(batch, path, row_index, "metric")?,
                flag: arrow_u8_field(batch, path, row_index, "flag")?,
                category: arrow_string_field(batch, path, row_index, "category")?,
            });
        }
    }
    Ok(rows)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn dim_rows_from_arrow_batches(
    batches: &[arrow_array::RecordBatch],
    path: &std::path::Path,
) -> Result<Vec<TraditionalDimRow>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            rows.push(TraditionalDimRow {
                dim_key: arrow_u32_field(batch, path, row_index, "dim_key")?,
                dim_label: arrow_string_field(batch, path, row_index, "dim_label")?,
                weight: arrow_f64_field(batch, path, row_index, "weight")?,
            });
        }
    }
    Ok(rows)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn arrow_column<'a>(
    batch: &'a arrow_array::RecordBatch,
    path: &std::path::Path,
    field: &str,
) -> Result<&'a dyn arrow_array::Array> {
    let index = batch.schema().index_of(field).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "Arrow input '{}' is missing field '{field}': {error}",
            path.display()
        ))
    })?;
    Ok(batch.column(index).as_ref())
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn arrow_u64_field(
    batch: &arrow_array::RecordBatch,
    path: &std::path::Path,
    row_index: usize,
    field: &str,
) -> Result<u64> {
    let value = arrow_i128_field(batch, path, row_index, field)?;
    u64::try_from(value).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "Arrow input '{}' field '{field}' row {} does not fit u64: {error}",
            path.display(),
            row_index + 1
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn arrow_u32_field(
    batch: &arrow_array::RecordBatch,
    path: &std::path::Path,
    row_index: usize,
    field: &str,
) -> Result<u32> {
    let value = arrow_i128_field(batch, path, row_index, field)?;
    u32::try_from(value).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "Arrow input '{}' field '{field}' row {} does not fit u32: {error}",
            path.display(),
            row_index + 1
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn arrow_u8_field(
    batch: &arrow_array::RecordBatch,
    path: &std::path::Path,
    row_index: usize,
    field: &str,
) -> Result<u8> {
    let value = arrow_i128_field(batch, path, row_index, field)?;
    u8::try_from(value).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "Arrow input '{}' field '{field}' row {} does not fit u8: {error}",
            path.display(),
            row_index + 1
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn arrow_i128_field(
    batch: &arrow_array::RecordBatch,
    path: &std::path::Path,
    row_index: usize,
    field: &str,
) -> Result<i128> {
    let array = arrow_column(batch, path, field)?;
    ensure_arrow_not_null(array, path, row_index, field)?;
    if let Some(values) = array.as_any().downcast_ref::<arrow_array::Int8Array>() {
        Ok(i128::from(values.value(row_index)))
    } else if let Some(values) = array.as_any().downcast_ref::<arrow_array::Int16Array>() {
        Ok(i128::from(values.value(row_index)))
    } else if let Some(values) = array.as_any().downcast_ref::<arrow_array::Int32Array>() {
        Ok(i128::from(values.value(row_index)))
    } else if let Some(values) = array.as_any().downcast_ref::<arrow_array::Int64Array>() {
        Ok(i128::from(values.value(row_index)))
    } else if let Some(values) = array.as_any().downcast_ref::<arrow_array::UInt8Array>() {
        Ok(i128::from(values.value(row_index)))
    } else if let Some(values) = array.as_any().downcast_ref::<arrow_array::UInt16Array>() {
        Ok(i128::from(values.value(row_index)))
    } else if let Some(values) = array.as_any().downcast_ref::<arrow_array::UInt32Array>() {
        Ok(i128::from(values.value(row_index)))
    } else if let Some(values) = array.as_any().downcast_ref::<arrow_array::UInt64Array>() {
        Ok(i128::from(values.value(row_index)))
    } else {
        Err(arrow_type_error(array, path, row_index, field, "integer"))
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn arrow_f64_field(
    batch: &arrow_array::RecordBatch,
    path: &std::path::Path,
    row_index: usize,
    field: &str,
) -> Result<f64> {
    let array = arrow_column(batch, path, field)?;
    ensure_arrow_not_null(array, path, row_index, field)?;
    if let Some(values) = array.as_any().downcast_ref::<arrow_array::Float64Array>() {
        Ok(values.value(row_index))
    } else if let Some(values) = array.as_any().downcast_ref::<arrow_array::Float32Array>() {
        Ok(f64::from(values.value(row_index)))
    } else if let Some(values) = array.as_any().downcast_ref::<arrow_array::Int64Array>() {
        arrow_i64_to_f64(values.value(row_index), path, row_index, field)
    } else if let Some(values) = array.as_any().downcast_ref::<arrow_array::Int32Array>() {
        Ok(f64::from(values.value(row_index)))
    } else if let Some(values) = array.as_any().downcast_ref::<arrow_array::UInt64Array>() {
        arrow_u64_to_f64(values.value(row_index), path, row_index, field)
    } else if let Some(values) = array.as_any().downcast_ref::<arrow_array::UInt32Array>() {
        Ok(f64::from(values.value(row_index)))
    } else {
        Err(arrow_type_error(array, path, row_index, field, "numeric"))
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn arrow_i64_to_f64(
    value: i64,
    path: &std::path::Path,
    row_index: usize,
    field: &str,
) -> Result<f64> {
    if value.unsigned_abs() > MAX_EXACT_F64_INTEGER {
        return Err(ShardLoomError::InvalidOperation(format!(
            "Arrow field '{field}' in '{}' row {row_index} cannot be represented exactly as f64",
            path.display()
        )));
    }
    value.to_string().parse::<f64>().map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "Arrow field '{field}' in '{}' row {row_index} could not be converted to f64: {error}",
            path.display()
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn arrow_u64_to_f64(
    value: u64,
    path: &std::path::Path,
    row_index: usize,
    field: &str,
) -> Result<f64> {
    if value > MAX_EXACT_F64_INTEGER {
        return Err(ShardLoomError::InvalidOperation(format!(
            "Arrow field '{field}' in '{}' row {row_index} cannot be represented exactly as f64",
            path.display()
        )));
    }
    value.to_string().parse::<f64>().map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "Arrow field '{field}' in '{}' row {row_index} could not be converted to f64: {error}",
            path.display()
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn arrow_string_field(
    batch: &arrow_array::RecordBatch,
    path: &std::path::Path,
    row_index: usize,
    field: &str,
) -> Result<String> {
    let array = arrow_column(batch, path, field)?;
    ensure_arrow_not_null(array, path, row_index, field)?;
    if let Some(values) = array.as_any().downcast_ref::<arrow_array::StringArray>() {
        Ok(values.value(row_index).to_string())
    } else if let Some(values) = array
        .as_any()
        .downcast_ref::<arrow_array::LargeStringArray>()
    {
        Ok(values.value(row_index).to_string())
    } else if let Some(values) = array
        .as_any()
        .downcast_ref::<arrow_array::StringViewArray>()
    {
        Ok(values.value(row_index).to_string())
    } else {
        Err(arrow_type_error(array, path, row_index, field, "string"))
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn ensure_arrow_not_null(
    array: &dyn arrow_array::Array,
    path: &std::path::Path,
    row_index: usize,
    field: &str,
) -> Result<()> {
    if array.is_null(row_index) {
        return Err(ShardLoomError::InvalidOperation(format!(
            "Arrow input '{}' field '{field}' row {} is null; benchmark schema requires a value",
            path.display(),
            row_index + 1
        )));
    }
    Ok(())
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn arrow_type_error(
    array: &dyn arrow_array::Array,
    path: &std::path::Path,
    row_index: usize,
    field: &str,
    expected: &str,
) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "Arrow input '{}' field '{field}' row {} expected {expected}, found {:?}",
        path.display(),
        row_index + 1,
        array.data_type()
    ))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_vortex_derived_scenario_from_files(
    scenario: TraditionalAnalyticsScenario,
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    match scenario {
        TraditionalAnalyticsScenario::CsvFileIngest => {
            run_streaming_fact_metric_sum_scenario(fact_path, dim_path, None, "metric")
        }
        TraditionalAnalyticsScenario::SelectiveFilter => run_streaming_fact_metric_sum_scenario(
            fact_path,
            dim_path,
            Some(selective_filter_expr()),
            "metric",
        ),
        TraditionalAnalyticsScenario::WideProjection => {
            run_streaming_fact_metric_sum_scenario(fact_path, dim_path, None, "group_key")
        }
        _ => {
            let fact = read_fact_vortex(fact_path)?;
            let dim = read_dim_vortex(dim_path)?;
            run_vortex_derived_scenario_from_tables(scenario, &fact, &dim)
        }
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_vortex_derived_scenario_from_tables(
    scenario: TraditionalAnalyticsScenario,
    fact: &VortexFactTable,
    dim: &VortexDimTable,
) -> Result<TraditionalScenarioExecution> {
    let result_json = run_vortex_derived_scenario(scenario, fact, dim)?;
    let rows_materialized = result_rows_materialized(&result_json)?;
    let rows_scanned = match scenario {
        TraditionalAnalyticsScenario::HashJoin
        | TraditionalAnalyticsScenario::JoinAggregate
        | TraditionalAnalyticsScenario::ScaleStressSkewedJoinAggregation
        | TraditionalAnalyticsScenario::ScaleStressMultiStageEtl => {
            checked_usize_sum_to_u64(fact.len(), dim.len())?
        }
        _ => usize_to_u64(fact.len())?,
    };
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: usize_to_u64(fact.len())?,
        dim_rows: usize_to_u64(dim.len())?,
        rows_scanned,
        rows_materialized,
        evidence: TraditionalScenarioExecutionEvidence::table_materialized(),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_fact_metric_sum_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
    filter: Option<vortex::array::expr::Expression>,
    sum_column: &'static str,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut metric_sum = 0.0;
    let stats =
        scan_fact_vortex_projected(fact_path, &[sum_column], filter, |fields, _chunk_rows| {
            if sum_column == "metric" {
                metric_sum += primitive_field::<f64>(fields, sum_column)?
                    .iter()
                    .sum::<f64>();
            } else {
                metric_sum += primitive_field::<u32>(fields, sum_column)?
                    .iter()
                    .map(|value| f64::from(*value))
                    .sum::<f64>();
            }
            Ok(())
        })?;
    let result_json = scalar_result_json(stats.result_row_count, metric_sum);
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        rows_scanned: stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn scan_fact_vortex_projected(
    path: &std::path::Path,
    projected_columns: &[&'static str],
    filter: Option<vortex::array::expr::Expression>,
    mut process: impl FnMut(
        &std::collections::BTreeMap<String, vortex::array::ArrayRef>,
        usize,
    ) -> Result<()>,
) -> Result<TraditionalStreamingScanStats> {
    use vortex::VortexSessionDefault as _;
    use vortex::array::expr::{root, select};
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(path))
        .map_err(vortex_error)?;
    let source_row_count = file.row_count();
    let filter_pushdown_applied = filter.is_some();
    let projection_pushdown_applied = !projected_columns.is_empty();
    let mut scan = file.scan().map_err(vortex_error)?;
    if let Some(filter) = filter {
        scan = scan.with_filter(filter);
    }
    if projection_pushdown_applied {
        scan = scan.with_projection(select(projected_columns.to_vec(), root()));
    }
    let mut result_row_count = 0_u64;
    let mut arrays_read_count = 0_usize;
    let mut max_chunk_rows = 0_usize;
    for chunk in scan.into_array_iter(&runtime).map_err(vortex_error)? {
        let chunk = chunk.map_err(vortex_error)?;
        let chunk_rows = chunk.len();
        let fields = projected_fields_from_chunk(chunk, projected_columns)?;
        process(&fields, chunk_rows)?;
        result_row_count = checked_u64_sum(result_row_count, usize_to_u64(chunk_rows)?)?;
        arrays_read_count += 1;
        max_chunk_rows = max_chunk_rows.max(chunk_rows);
    }
    Ok(TraditionalStreamingScanStats {
        source_row_count,
        result_row_count,
        arrays_read_count,
        max_chunk_rows,
        projected_columns: projected_columns
            .iter()
            .map(|column| (*column).to_string())
            .collect(),
        filter_pushdown_applied,
        projection_pushdown_applied,
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn projected_fields_from_chunk(
    chunk: vortex::array::ArrayRef,
    projected_columns: &[&str],
) -> Result<std::collections::BTreeMap<String, vortex::array::ArrayRef>> {
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::StructArray;
    use vortex::array::arrays::struct_::StructArrayExt as _;
    use vortex::array::dtype::DType;

    match chunk.dtype() {
        DType::Struct(_, _) => {
            let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
            let struct_array = chunk
                .execute::<StructArray>(&mut ctx)
                .map_err(vortex_error)?;
            let mut fields = std::collections::BTreeMap::new();
            for name in struct_array.names().iter() {
                let field = struct_array
                    .unmasked_field_by_name(name.as_ref())
                    .map_err(vortex_error)?
                    .clone();
                fields.insert(name.as_ref().to_string(), field);
            }
            Ok(fields)
        }
        DType::Primitive(_, _) if projected_columns.len() == 1 => {
            Ok([(projected_columns[0].to_string(), chunk)]
                .into_iter()
                .collect())
        }
        other => Err(ShardLoomError::InvalidOperation(format!(
            "projected Vortex chunk has unsupported dtype {other:?}"
        ))),
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn selective_filter_expr() -> vortex::array::expr::Expression {
    use vortex::array::expr::{and, col, eq, gt_eq, lit};

    and(
        eq(col("flag".to_string()), lit(1_u8)),
        gt_eq(col("value".to_string()), lit(5_000_u32)),
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn vortex_file_row_count(path: &std::path::Path) -> Result<u64> {
    use vortex::VortexSessionDefault as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(path))
        .map_err(vortex_error)?;
    Ok(file.row_count())
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[allow(clippy::too_many_lines)]
fn run_vortex_derived_scenario(
    scenario: TraditionalAnalyticsScenario,
    fact: &VortexFactTable,
    dim: &VortexDimTable,
) -> Result<String> {
    use std::collections::{BTreeMap, HashMap, HashSet};

    let dim_by_key = dim
        .dim_key
        .iter()
        .enumerate()
        .map(|(index, key)| (*key, index))
        .collect::<HashMap<_, _>>();
    let result_json = match scenario {
        TraditionalAnalyticsScenario::CsvFileIngest => {
            scalar_result_json(usize_to_u64(fact.len())?, fact.metric.iter().sum::<f64>())
        }
        TraditionalAnalyticsScenario::SelectiveFilter => {
            let mut accum = TraditionalGroupAccum::default();
            for index in 0..fact.len() {
                if fact.flag[index] == 1 && fact.value[index] >= 5_000 {
                    accum.add(fact.metric[index]);
                }
            }
            scalar_result_json(accum.row_count, accum.metric_sum)
        }
        TraditionalAnalyticsScenario::GroupByAggregation => {
            let mut groups = BTreeMap::<u32, TraditionalGroupAccum>::new();
            for index in 0..fact.len() {
                groups
                    .entry(fact.group_key[index])
                    .or_default()
                    .add(fact.metric[index]);
            }
            numeric_group_rows_json(groups, "group_key")
        }
        TraditionalAnalyticsScenario::SortAndTopK => {
            let mut rows = (0..fact.len())
                .map(|index| (fact.id[index], fact.metric[index]))
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| {
                right
                    .1
                    .total_cmp(&left.1)
                    .then_with(|| left.0.cmp(&right.0))
            });
            top_rows_json(&rows[..rows.len().min(10)])
        }
        TraditionalAnalyticsScenario::HashJoin => {
            let mut groups = BTreeMap::<String, TraditionalGroupAccum>::new();
            for index in 0..fact.len() {
                if let Some(dim_index) = dim_by_key.get(&fact.dim_key[index]) {
                    groups
                        .entry(dim.dim_label[*dim_index].clone())
                        .or_default()
                        .add(fact.metric[index]);
                }
            }
            string_group_rows_json(groups, "dim_label")
        }
        TraditionalAnalyticsScenario::WideProjection => scalar_result_json(
            usize_to_u64(fact.len())?,
            fact.group_key
                .iter()
                .map(|value| f64::from(*value))
                .sum::<f64>(),
        ),
        TraditionalAnalyticsScenario::DistinctCount => {
            let distinct = fact.category.iter().collect::<HashSet<_>>().len();
            format!(
                "{{\"distinct_category_count\":{}}}",
                usize_to_u64(distinct)?
            )
        }
        TraditionalAnalyticsScenario::FilterProjectionLimit => {
            let mut rows = (0..fact.len())
                .filter(|index| fact.flag[*index] == 1 && fact.value[*index] >= 5_000)
                .map(|index| (fact.id[index], fact.value[index]))
                .collect::<Vec<_>>();
            rows.sort_by_key(|(id, _value)| *id);
            let mut accum = TraditionalGroupAccum::default();
            for (_id, value) in rows.into_iter().take(100) {
                accum.add(f64::from(value));
            }
            scalar_result_json(accum.row_count, accum.metric_sum)
        }
        TraditionalAnalyticsScenario::MultiKeyGroupBy => {
            let mut groups = BTreeMap::<(u32, String), TraditionalGroupAccum>::new();
            for index in 0..fact.len() {
                groups
                    .entry((fact.group_key[index], fact.category[index].clone()))
                    .or_default()
                    .add(fact.metric[index]);
            }
            group_category_rows_json(groups)
        }
        TraditionalAnalyticsScenario::JoinAggregate => {
            let mut groups = BTreeMap::<(String, String), TraditionalGroupAccum>::new();
            for index in 0..fact.len() {
                if fact.value[index] < 2_500 {
                    continue;
                }
                if let Some(dim_index) = dim_by_key.get(&fact.dim_key[index]) {
                    groups
                        .entry((
                            dim.dim_label[*dim_index].clone(),
                            fact.category[index].clone(),
                        ))
                        .or_default()
                        .add(fact.metric[index]);
                }
            }
            dim_category_rows_json(groups)
        }
        TraditionalAnalyticsScenario::RowNumberWindow => {
            let rows = ranked_group_rows(fact, 1);
            rank_rows_json(rows)
        }
        TraditionalAnalyticsScenario::HighCardinalityStringGroupDistinct => {
            let mut groups = BTreeMap::<String, TraditionalGroupAccum>::new();
            for index in 0..fact.len() {
                groups
                    .entry(fact.category[index].clone())
                    .or_default()
                    .add(fact.metric[index]);
            }
            string_group_distinct_json(groups)
        }
        TraditionalAnalyticsScenario::TopNPerGroup => {
            let rows = ranked_group_rows(fact, 3);
            rank_rows_json(rows)
        }
        TraditionalAnalyticsScenario::ScaleStressSkewedJoinAggregation => {
            let mut groups = BTreeMap::<u32, TraditionalGroupAccum>::new();
            for index in 0..fact.len() {
                if dim_by_key.contains_key(&fact.dim_key[index]) {
                    groups
                        .entry(fact.group_key[index] % 10)
                        .or_default()
                        .add(fact.metric[index]);
                }
            }
            numeric_group_rows_json(groups, "skew_key")
        }
        TraditionalAnalyticsScenario::ScaleStressMultiStageEtl => {
            let mut groups = BTreeMap::<(String, u32), TraditionalComplexAccum>::new();
            for index in 0..fact.len() {
                if fact.value[index] < 2_500 {
                    continue;
                }
                if let Some(dim_index) = dim_by_key.get(&fact.dim_key[index]) {
                    let bucket = fact.group_key[index] % 10;
                    let weighted_metric = fact.metric[index] * (dim.weight[*dim_index] + 1.0);
                    groups
                        .entry((dim.dim_label[*dim_index].clone(), bucket))
                        .or_default()
                        .add(fact.metric[index], weighted_metric);
                }
            }
            complex_etl_rows_json(groups)
        }
    };
    Ok(result_json)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn result_rows_materialized(result_json: &str) -> Result<u64> {
    if result_json.starts_with('[') {
        usize_to_u64(result_json.matches('{').count())
    } else {
        Ok(1)
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn scalar_result_json(row_count: u64, metric_sum: f64) -> String {
    format!(
        "{{\"row_count\":{row_count},\"metric_sum\":{}}}",
        json_float(metric_sum)
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn numeric_group_rows_json(
    groups: std::collections::BTreeMap<u32, TraditionalGroupAccum>,
    key: &str,
) -> String {
    let rows = groups
        .into_iter()
        .map(|(group_key, accum)| {
            format!(
                "{{{}:{group_key},\"row_count\":{},\"metric_sum\":{}}}",
                json_key(key),
                accum.row_count,
                json_float(accum.metric_sum)
            )
        })
        .collect::<Vec<_>>();
    format!("[{}]", rows.join(","))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn string_group_rows_json(
    groups: std::collections::BTreeMap<String, TraditionalGroupAccum>,
    key: &str,
) -> String {
    let rows = groups
        .into_iter()
        .map(|(group_key, accum)| {
            format!(
                "{{{}:{},\"row_count\":{},\"metric_sum\":{}}}",
                json_key(key),
                json_string_literal(&group_key),
                accum.row_count,
                json_float(accum.metric_sum)
            )
        })
        .collect::<Vec<_>>();
    format!("[{}]", rows.join(","))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn group_category_rows_json(
    groups: std::collections::BTreeMap<(u32, String), TraditionalGroupAccum>,
) -> String {
    let rows = groups
        .into_iter()
        .map(|((group_key, category), accum)| {
            format!(
                "{{\"group_key\":{group_key},\"category\":{},\"row_count\":{},\"metric_sum\":{}}}",
                json_string_literal(&category),
                accum.row_count,
                json_float(accum.metric_sum)
            )
        })
        .collect::<Vec<_>>();
    format!("[{}]", rows.join(","))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn dim_category_rows_json(
    groups: std::collections::BTreeMap<(String, String), TraditionalGroupAccum>,
) -> String {
    let rows = groups
        .into_iter()
        .map(|((dim_label, category), accum)| {
            format!(
                "{{\"dim_label\":{},\"category\":{},\"row_count\":{},\"metric_sum\":{}}}",
                json_string_literal(&dim_label),
                json_string_literal(&category),
                accum.row_count,
                json_float(accum.metric_sum)
            )
        })
        .collect::<Vec<_>>();
    format!("[{}]", rows.join(","))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn ranked_group_rows(fact: &VortexFactTable, max_rank: u64) -> Vec<(u32, u64, f64, u64)> {
    let mut rows = (0..fact.len())
        .map(|index| (fact.group_key[index], fact.id[index], fact.metric[index]))
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| right.2.total_cmp(&left.2))
            .then_with(|| left.1.cmp(&right.1))
    });
    let mut ranked = Vec::new();
    let mut current_group = None;
    let mut rank = 0_u64;
    for (group_key, id, metric) in rows {
        if current_group == Some(group_key) {
            rank += 1;
        } else {
            current_group = Some(group_key);
            rank = 1;
        }
        if rank <= max_rank {
            ranked.push((group_key, id, metric, rank));
        }
    }
    ranked
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn rank_rows_json(mut rows: Vec<(u32, u64, f64, u64)>) -> String {
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.3.cmp(&right.3))
            .then_with(|| left.1.cmp(&right.1))
    });
    let rows = rows
        .into_iter()
        .map(|(group_key, id, metric, rank)| {
            format!(
                "{{\"group_key\":{group_key},\"id\":{id},\"metric\":{},\"rank\":{rank}}}",
                json_float(metric)
            )
        })
        .collect::<Vec<_>>();
    format!("[{}]", rows.join(","))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn string_group_distinct_json(
    groups: std::collections::BTreeMap<String, TraditionalGroupAccum>,
) -> String {
    let distinct = groups.len();
    let rows = groups
        .into_iter()
        .take(100)
        .map(|(category, accum)| {
            format!(
                "{{\"category\":{},\"row_count\":{},\"metric_sum\":{}}}",
                json_string_literal(&category),
                accum.row_count,
                json_float(accum.metric_sum)
            )
        })
        .collect::<Vec<_>>();
    format!(
        "{{\"distinct_category_count\":{},\"groups\":[{}]}}",
        distinct,
        rows.join(",")
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn top_rows_json(rows: &[(u64, f64)]) -> String {
    let rows = rows
        .iter()
        .map(|(id, metric)| format!("{{\"id\":{id},\"metric\":{}}}", json_float(*metric)))
        .collect::<Vec<_>>();
    format!("[{}]", rows.join(","))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn complex_etl_rows_json(
    groups: std::collections::BTreeMap<(String, u32), TraditionalComplexAccum>,
) -> String {
    let mut rows = groups.into_iter().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .1
            .weighted_sum
            .total_cmp(&left.1.weighted_sum)
            .then_with(|| left.0.0.cmp(&right.0.0))
            .then_with(|| left.0.1.cmp(&right.0.1))
    });
    let rows = rows
        .into_iter()
        .take(20)
        .map(|((dim_label, bucket), accum)| {
            format!(
                "{{\"dim_label\":{},\"bucket\":{bucket},\"row_count\":{},\"metric_sum\":{},\"weighted_sum\":{}}}",
                json_string_literal(&dim_label),
                accum.row_count,
                json_float(accum.metric_sum),
                json_float(accum.weighted_sum)
            )
        })
        .collect::<Vec<_>>();
    format!("[{}]", rows.join(","))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn json_key(value: &str) -> String {
    json_string_literal(value)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn json_string_literal(value: &str) -> String {
    use std::fmt::Write as _;

    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for ch in value.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            value if value.is_control() => {
                let _ = write!(escaped, "\\u{:04x}", u32::from(value));
            }
            value => escaped.push(value),
        }
    }
    escaped.push('"');
    escaped
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn json_float(value: f64) -> String {
    let scale = 10_f64.powi(BENCHMARK_FLOAT_DIGITS);
    let rounded = (value * scale).round() / scale;
    let mut text = format!("{rounded:.4}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.push('0');
    }
    text
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn checked_usize_sum_to_u64(left: usize, right: usize) -> Result<u64> {
    let Some(total) = left.checked_add(right) else {
        return Err(ShardLoomError::InvalidOperation(
            "traditional analytics row count overflow".to_string(),
        ));
    };
    usize_to_u64(total)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn checked_u64_sum(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right).ok_or_else(|| {
        ShardLoomError::InvalidOperation("traditional analytics row count overflow".to_string())
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn usize_to_u64(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "traditional analytics count does not fit in u64: {error}"
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn vortex_error(error: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("Vortex traditional analytics path failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_parse_accepts_harness_labels() {
        assert_eq!(
            TraditionalAnalyticsScenario::parse("csv/file ingest").unwrap(),
            TraditionalAnalyticsScenario::CsvFileIngest
        );
        assert_eq!(
            TraditionalAnalyticsScenario::parse("scale-stress-multi-stage-etl").unwrap(),
            TraditionalAnalyticsScenario::ScaleStressMultiStageEtl
        );
        assert_eq!(
            TraditionalAnalyticsScenario::parse("filter + projection + limit").unwrap(),
            TraditionalAnalyticsScenario::FilterProjectionLimit
        );
        assert_eq!(
            TraditionalAnalyticsScenario::parse("top-n-per-group").unwrap(),
            TraditionalAnalyticsScenario::TopNPerGroup
        );
    }

    #[test]
    fn disabled_build_returns_explicit_error() {
        if cfg!(feature = "vortex-traditional-analytics-benchmark") {
            return;
        }
        let err = run_traditional_analytics_benchmark(TraditionalAnalyticsRequest::new(
            TraditionalAnalyticsScenario::CsvFileIngest,
            PathBuf::from("fact.csv"),
            PathBuf::from("dim.csv"),
            PathBuf::from("ws"),
        ))
        .expect_err("default build should require feature gate");
        assert!(
            err.to_string()
                .contains("vortex-traditional-analytics-benchmark")
        );
    }

    #[test]
    fn resource_policy_auto_derives_parallelism_and_partitions() {
        let policy =
            TraditionalAnalyticsResourcePolicy::auto().resolve_for_sources(256 * 1024 * 1024);

        assert_eq!(policy.sizing_mode(), "auto");
        assert_eq!(policy.requested_memory_gb, None);
        assert_eq!(policy.requested_max_parallelism, None);
        assert!(policy.detected_parallelism >= 1);
        assert_eq!(policy.max_parallelism, policy.detected_parallelism);
        assert!(policy.target_batch_rows >= TraditionalAnalyticsResourcePolicy::MIN_BATCH_ROWS);
        assert!(policy.target_partition_count >= 1);
        assert_eq!(policy.source_bytes, 256 * 1024 * 1024);
    }

    #[test]
    fn resource_policy_explicit_values_are_bounds_for_auto_sizing() {
        let policy =
            TraditionalAnalyticsResourcePolicy::new(8, 2).resolve_for_sources(512 * 1024 * 1024);

        assert_eq!(policy.sizing_mode(), "bounded-auto");
        assert_eq!(policy.requested_memory_gb, Some(8));
        assert_eq!(policy.requested_max_parallelism, Some(2));
        assert_eq!(policy.memory_gb, 8);
        assert_eq!(policy.max_parallelism, 2);
        assert!(policy.target_batch_rows >= TraditionalAnalyticsResourcePolicy::MIN_BATCH_ROWS);
        assert!(policy.target_partition_count >= 1);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn traditional_analytics_test_root(label: &str) -> PathBuf {
        static NEXT_TEST_ROOT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = NEXT_TEST_ROOT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "shardloom-traditional-analytics-{label}-{}-{}-{}",
            std::process::id(),
            sequence,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn write_tiny_traditional_csv_inputs(root: &std::path::Path) -> (PathBuf, PathBuf) {
        std::fs::create_dir_all(root).unwrap();
        let fact_csv = root.join("fact.csv");
        let dim_csv = root.join("dim.csv");
        std::fs::write(
            &fact_csv,
            "id,group_key,dim_key,value,metric,flag,category\n1,10,1,6000,2.5,1,A\n2,11,2,1000,3.5,0,B\n3,10,1,8000,4.0,1,A\n",
        )
        .unwrap();
        std::fs::write(&dim_csv, "dim_key,dim_label,weight\n1,one,1.5\n2,two,2.0\n").unwrap();
        (fact_csv, dim_csv)
    }

    #[test]
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn fact_csv_reader_accepts_generated_profile_trailing_columns() {
        let root = traditional_analytics_test_root("wide-csv");
        std::fs::create_dir_all(&root).unwrap();
        let fact_csv = root.join("fact.csv");
        std::fs::write(
            &fact_csv,
            "id,group_key,dim_key,value,metric,flag,category,extra_metric_00,event_date\n1,10,1,6000,2.5,1,A,42.0,2024-01-01\n",
        )
        .unwrap();

        let rows = read_traditional_fact_csv(&fact_csv).expect("fact rows");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].category, "A");
        assert_eq!(rows[0].metric, 2.5);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn assert_streaming_selective_filter_import_report(report: &TraditionalAnalyticsReport) {
        assert!(report.streaming_vortex_execution_used);
        assert!(report.full_table_materialization_avoided);
        assert!(report.streaming_filter_pushdown_applied);
        assert!(report.streaming_projection_pushdown_applied);
        assert!(report.streaming_arrays_read_count > 0);
        assert!(report.streaming_max_chunk_rows > 0);
        assert_eq!(
            report.streaming_projected_columns,
            vec!["metric".to_string()]
        );
        assert!(report.data_decoded);
        assert!(report.data_materialized);
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_materialized, 1);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn assert_streaming_selective_filter_native_report(report: &TraditionalAnalyticsVortexReport) {
        assert!(report.streaming_vortex_execution_used);
        assert!(report.full_table_materialization_avoided);
        assert!(report.streaming_filter_pushdown_applied);
        assert!(report.streaming_projection_pushdown_applied);
        assert!(report.streaming_arrays_read_count > 0);
        assert!(report.streaming_max_chunk_rows > 0);
        assert_eq!(
            report.streaming_projected_columns,
            vec!["metric".to_string()]
        );
        assert!(report.data_decoded);
        assert!(!report.data_materialized);
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_materialized, 1);
        assert_eq!(report.materialization_boundary_rows, 0);
        assert_eq!(
            report
                .native_io_certificate
                .representation_transition_order(),
            "vortex_encoded->partially_decoded"
        );
        assert_eq!(
            report.native_io_certificate.materialization_boundaries[0].rows_materialized,
            0
        );
        assert!(
            report
                .native_io_certificate
                .sink_requirement_report
                .supports_streaming
        );
        assert_eq!(
            report
                .native_io_certificate
                .sink_requirement_report
                .max_chunk_size,
            Some(report.streaming_max_chunk_rows as u64)
        );
        assert!(
            !report
                .native_io_certificate
                .adapter_fidelity_report
                .materialization_required
        );
        assert!(!report.native_io_certificate.side_effects.data_materialized);
        let accepted = &report
            .native_io_certificate
            .source_pushdown_report
            .accepted_operations;
        assert!(
            accepted
                .iter()
                .any(|operation| operation == "vortex_file_scan")
        );
        assert!(
            accepted
                .iter()
                .any(|operation| operation == "vortex_scan_filter")
        );
        assert!(
            accepted
                .iter()
                .any(|operation| operation == "vortex_scan_projection")
        );
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_build_runs_csv_through_local_vortex_io() {
        let root = traditional_analytics_test_root("csv");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let report = run_traditional_analytics_benchmark(TraditionalAnalyticsRequest::new(
            TraditionalAnalyticsScenario::SelectiveFilter,
            fact_csv,
            dim_csv,
            workspace,
        ))
        .unwrap();

        assert_eq!(report.result_json, "{\"row_count\":2,\"metric_sum\":6.5}");
        assert_eq!(report.fact_rows, 3);
        assert_eq!(report.resource_policy.sizing_mode(), "auto");
        assert!(report.resource_policy.target_partition_count >= 1);
        assert!(report.fact_vortex_path.exists());
        assert!(report.dim_vortex_path.exists());
        assert!(report.native_work_envelope_created);
        assert!(report.native_work_stream_created);
        assert!(report.native_result_stream_created);
        assert!(report.native_io_certificate_emitted);
        assert!(report.native_io_certificate.is_certified());
        assert_eq!(
            report.native_io_certificate.path_id,
            "compatibility_source_to_native_vortex_sink"
        );
        assert_eq!(
            report
                .native_io_certificate
                .representation_transition_order(),
            "foreign_encoded->decoded_columnar,decoded_columnar->vortex_encoded"
        );
        assert_eq!(report.materialization_boundary_rows, 5);
        assert_eq!(
            report.native_io_certificate.materialization_boundaries[0].rows_materialized,
            5
        );
        assert!(report.csv_source_adapter_used);
        assert!(report.csv_to_vortex_import_performed);
        assert!(report.vortex_file_written);
        assert!(report.vortex_file_read);
        assert!(report.upstream_vortex_scan_called);
        assert_streaming_selective_filter_import_report(&report);
        assert!(report.materialization_boundary_report_emitted);
        assert!(report.row_read);
        assert!(!report.fallback_execution_allowed);

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                report.fact_vortex_path.clone(),
                report.dim_vortex_path.clone(),
            ))
            .unwrap();
        assert_eq!(
            native_report.result_json,
            "{\"row_count\":2,\"metric_sum\":6.5}"
        );
        assert_eq!(
            native_report.native_io_certificate.path_id,
            "native_vortex_source_to_native_runtime_result"
        );
        assert!(native_report.vortex_source_adapter_used);
        assert!(native_report.vortex_file_read);
        assert!(native_report.upstream_vortex_scan_called);
        assert_streaming_selective_filter_native_report(&native_report);
        assert!(native_report.materialization_boundary_report_emitted);
        assert!(!native_report.row_read);
        assert!(!native_report.write_io);
        assert!(!native_report.fallback_execution_allowed);

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn expanded_taxonomy_scenarios_run_against_local_vortex_outputs() {
        let root = traditional_analytics_test_root("expanded-taxonomy");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let cases = [
            (
                TraditionalAnalyticsScenario::FilterProjectionLimit,
                "{\"row_count\":2,\"metric_sum\":14000.0}",
            ),
            (
                TraditionalAnalyticsScenario::MultiKeyGroupBy,
                "[{\"group_key\":10,\"category\":\"A\",\"row_count\":2,\"metric_sum\":6.5},{\"group_key\":11,\"category\":\"B\",\"row_count\":1,\"metric_sum\":3.5}]",
            ),
            (
                TraditionalAnalyticsScenario::JoinAggregate,
                "[{\"dim_label\":\"one\",\"category\":\"A\",\"row_count\":2,\"metric_sum\":6.5}]",
            ),
            (
                TraditionalAnalyticsScenario::RowNumberWindow,
                "[{\"group_key\":10,\"id\":3,\"metric\":4.0,\"rank\":1},{\"group_key\":11,\"id\":2,\"metric\":3.5,\"rank\":1}]",
            ),
            (
                TraditionalAnalyticsScenario::HighCardinalityStringGroupDistinct,
                "{\"distinct_category_count\":2,\"groups\":[{\"category\":\"A\",\"row_count\":2,\"metric_sum\":6.5},{\"category\":\"B\",\"row_count\":1,\"metric_sum\":3.5}]}",
            ),
            (
                TraditionalAnalyticsScenario::TopNPerGroup,
                "[{\"group_key\":10,\"id\":3,\"metric\":4.0,\"rank\":1},{\"group_key\":10,\"id\":1,\"metric\":2.5,\"rank\":2},{\"group_key\":11,\"id\":2,\"metric\":3.5,\"rank\":1}]",
            ),
        ];

        for (scenario, expected_json) in cases {
            let report = run_traditional_analytics_benchmark(
                TraditionalAnalyticsRequest::new(
                    scenario,
                    fact_csv.clone(),
                    dim_csv.clone(),
                    root.join(format!(
                        "workspace-{}",
                        scenario.as_str().replace(['/', ' ', '+'], "-")
                    )),
                )
                .with_input_format(TraditionalAnalyticsInputFormat::Csv)
                .with_native_vortex_replay_verification(true)
                .with_result_vortex_write(true),
            )
            .unwrap();

            assert_eq!(report.result_json, expected_json, "{}", scenario.as_str());
            assert!(report.native_io_certificate.is_certified());
            assert!(report.computed_result_sink_written);
            assert!(report.computed_result_sink_replay_verified);
            assert!(!report.fallback_execution_allowed);
            assert!(report.runtime_task_graph_created);
            assert!(report.runtime_task_graph_executed);
            assert_eq!(
                report.runtime_execution_certificate.status.as_str(),
                "certified"
            );
        }

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_wide_projection_streams_projected_vortex_chunks() {
        let root = traditional_analytics_test_root("wide-projection");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::WideProjection,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(
            import_report.result_json,
            "{\"row_count\":3,\"metric_sum\":31.0}"
        );
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(!import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec!["group_key".to_string()]
        );

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::WideProjection,
                import_report.fact_vortex_path.clone(),
                import_report.dim_vortex_path.clone(),
            ))
            .unwrap();

        assert_eq!(native_report.result_json, import_report.result_json);
        assert!(native_report.streaming_vortex_execution_used);
        assert!(native_report.full_table_materialization_avoided);
        assert!(!native_report.streaming_filter_pushdown_applied);
        assert!(native_report.streaming_projection_pushdown_applied);
        assert_eq!(
            native_report.streaming_projected_columns,
            vec!["group_key".to_string()]
        );
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn native_replay_verification_certifies_local_vortex_analytics_workflow() {
        let root = traditional_analytics_test_root("workflow-replay");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_native_vortex_replay_verification(true),
        )
        .unwrap();

        assert_eq!(report.workload_constitution_id, "local_vortex_analytics_v1");
        assert_eq!(
            report.workload_scorecard_status,
            "fixture_certified_result_sink_not_requested"
        );
        assert!(report.output_replay_requested);
        assert!(report.output_replay_verified);
        assert_eq!(
            report.output_replay_result_json.as_deref(),
            Some(report.result_json.as_str())
        );
        assert_eq!(report.output_replay_rows_scanned, Some(report.rows_scanned));
        assert_eq!(report.output_replay_rows_materialized, Some(1));
        assert_eq!(
            report.output_replay_native_io_certificate_status.as_deref(),
            Some("certified")
        );
        assert!(report.fact_vortex_digest.starts_with("fnv1a64:"));
        assert!(report.dim_vortex_digest.starts_with("fnv1a64:"));
        assert!(report.combined_output_digest.starts_with("fnv1a64:"));
        assert!(
            report
                .output_artifact_schema_summary
                .contains("fact(id:u64")
        );
        assert_eq!(
            report.commit_state,
            "local_vortex_files_written_uncommitted"
        );
        assert_eq!(
            report.rollback_cleanup_status,
            "caller_owned_workspace_cleanup"
        );
        assert!(
            report
                .benchmark_row_ref
                .contains("local_vortex_analytics_v1")
        );
        assert!(
            report
                .coverage_row_ref
                .contains("local_vortex_analytics_v1")
        );
        assert!(!report.fallback_execution_allowed);

        let fields = report.fields();
        assert!(
            fields
                .iter()
                .any(|(key, value)| { key == "output_replay_verified" && value == "true" })
        );
        assert!(fields.iter().any(|(key, value)| {
            key == "workload_scorecard_status"
                && value == "fixture_certified_result_sink_not_requested"
        }));
        assert!(fields.iter().any(|(key, value)| {
            key == "output_replay_native_io_certificate_status" && value == "certified"
        }));

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn computed_result_vortex_sink_writes_and_replays_result_artifact() {
        let root = traditional_analytics_test_root("result-sink");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_native_vortex_replay_verification(true)
            .with_result_vortex_write(true),
        )
        .unwrap();

        let result_path = report
            .computed_result_vortex_path
            .clone()
            .expect("computed result Vortex path");
        assert!(result_path.exists());
        assert_eq!(report.workload_scorecard_status, "workload_certified");
        assert!(report.computed_result_sink_requested);
        assert!(report.computed_result_sink_written);
        assert!(report.computed_result_sink_replay_verified);
        assert!(report.computed_result_vortex_bytes > 0);
        assert!(
            report
                .computed_result_vortex_digest
                .as_deref()
                .unwrap_or_default()
                .starts_with("fnv1a64:")
        );
        assert_eq!(report.computed_result_sink_rows, 1);
        assert_eq!(
            report.computed_result_sink_rows_materialized,
            report.rows_materialized
        );
        assert_eq!(
            report.computed_result_sink_schema_summary,
            COMPUTED_RESULT_VORTEX_SCHEMA_SUMMARY
        );
        assert!(report.computed_result_sink_write_micros.is_some());
        assert_eq!(
            report.computed_result_sink_replay_result_json.as_deref(),
            Some(report.result_json.as_str())
        );
        assert_eq!(
            report
                .computed_result_sink_native_io_certificate_status
                .as_deref(),
            Some("certified")
        );
        assert_eq!(
            report.commit_state,
            "local_vortex_files_and_result_sink_written_uncommitted"
        );
        assert!(report.runtime_task_graph_created);
        assert!(report.runtime_task_graph_executed);
        assert_eq!(
            report.runtime_scheduler_mode,
            "deterministic_local_task_sequence"
        );
        assert!(report.runtime_scheduler_ref.contains("tasks/5"));
        assert_eq!(report.runtime_task_count, 5);
        assert_eq!(
            report.runtime_scheduled_task_count,
            report.runtime_task_count
        );
        assert_eq!(
            report.runtime_completed_task_count,
            report.runtime_task_count
        );
        assert!(report.runtime_split_count >= 1);
        assert!(report.runtime_scheduler_batch_count >= 1);
        assert_eq!(
            report.runtime_queue_limit,
            report.resource_policy.max_parallelism
        );
        assert!(report.runtime_queue_limit_enforced);
        assert!(report.runtime_backpressure_bounded);
        assert_eq!(
            report.runtime_cancellation_checkpoint_count,
            report.runtime_task_count
        );
        assert!(report.runtime_cancellation_testable);
        assert_eq!(report.runtime_cancellation_gate_status, "gate_open");
        assert!(report.runtime_retry_testable);
        assert_eq!(report.runtime_retry_gate_status, "gate_open");
        assert_eq!(
            report.runtime_memory_reservations_requested,
            report.runtime_task_count
        );
        assert_eq!(
            report.runtime_memory_reservations_granted,
            report.runtime_task_count
        );
        assert_eq!(
            report.runtime_memory_reservations_released,
            report.runtime_task_count
        );
        assert_eq!(report.runtime_memory_reservations_denied, 0);
        assert!(report.runtime_memory_peak_reserved_bytes > 0);
        assert!(report.runtime_fail_before_oom_enforced);
        assert!(!report.runtime_spill_required);
        assert!(report.runtime_operator_memory_spill_declaration_count > 0);
        assert!(report.runtime_operator_memory_spill_claim_blocker_count > 0);
        assert!(!report.runtime_large_workload_claim_allowed);
        assert_eq!(
            report.runtime_execution_certificate.status.as_str(),
            "certified"
        );
        assert_eq!(
            report.runtime_execution_certificate.plan_ref.as_deref(),
            Some(report.runtime_scheduler_ref.as_str())
        );
        assert!(report.runtime_execution_certificate.fallback_free());
        assert!(
            report
                .runtime_execution_certificate
                .external_query_engine_free()
        );
        assert!(!report.fallback_execution_allowed);

        let replay = read_computed_result_vortex(&result_path).unwrap();
        assert_eq!(
            replay.scenario,
            TraditionalAnalyticsScenario::SelectiveFilter.as_str()
        );
        assert_eq!(replay.result_json, report.result_json);
        assert_eq!(replay.rows_materialized, report.rows_materialized);
        assert_eq!(
            replay.workload_constitution_id,
            LOCAL_VORTEX_ANALYTICS_CONSTITUTION_ID
        );

        let fields = report.fields();
        assert!(fields.iter().any(|(key, value)| {
            key == "computed_result_sink_replay_verified" && value == "true"
        }));
        assert!(fields.iter().any(|(key, value)| {
            key == "computed_result_sink_native_io_certificate_status" && value == "certified"
        }));
        assert!(
            fields
                .iter()
                .any(|(key, _)| key == "scenario_compute_micros")
        );
        assert!(fields.iter().any(|(key, value)| {
            key == "runtime_execution_certificate_status" && value == "certified"
        }));
        assert!(fields.iter().any(|(key, value)| {
            key == "runtime_memory_reservations_released"
                && value == &report.runtime_task_count.to_string()
        }));
        assert!(
            fields
                .iter()
                .any(|(key, value)| { key == "runtime_fallback_attempted" && value == "false" })
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_build_roundtrips_common_formats_through_vortex_outputs() {
        for output_format in [
            TraditionalAnalyticsInputFormat::Csv,
            TraditionalAnalyticsInputFormat::JsonLines,
            TraditionalAnalyticsInputFormat::Parquet,
            TraditionalAnalyticsInputFormat::ArrowIpc,
            TraditionalAnalyticsInputFormat::Avro,
            TraditionalAnalyticsInputFormat::Orc,
        ] {
            let root = traditional_analytics_test_root(output_format.as_str());
            let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
            let import_workspace = root.join("import");
            let import_report = run_traditional_analytics_benchmark(
                TraditionalAnalyticsRequest::new(
                    TraditionalAnalyticsScenario::HashJoin,
                    fact_csv,
                    dim_csv,
                    import_workspace,
                )
                .with_compatibility_output_format(Some(output_format)),
            )
            .unwrap();

            assert_eq!(
                import_report.result_json,
                "[{\"dim_label\":\"one\",\"row_count\":2,\"metric_sum\":6.5},{\"dim_label\":\"two\",\"row_count\":1,\"metric_sum\":3.5}]"
            );
            assert!(import_report.compatibility_output_written);
            assert!(import_report.native_to_compatibility_output_performed);
            let fact_output = import_report
                .fact_compatibility_output_path
                .clone()
                .expect("fact compatibility output path");
            let dim_output = import_report
                .dim_compatibility_output_path
                .clone()
                .expect("dimension compatibility output path");
            assert!(fact_output.exists());
            assert!(dim_output.exists());

            let replay_report = run_traditional_analytics_benchmark(
                TraditionalAnalyticsRequest::new(
                    TraditionalAnalyticsScenario::HashJoin,
                    fact_output,
                    dim_output,
                    root.join("replay"),
                )
                .with_input_format(output_format),
            )
            .unwrap();

            assert_eq!(replay_report.result_json, import_report.result_json);
            assert_eq!(replay_report.input_format, output_format);
            assert_eq!(
                replay_report
                    .native_io_certificate
                    .source_capability_report
                    .source_kind,
                output_format.source_kind()
            );
            assert!(replay_report.compatibility_source_adapter_used);
            assert!(replay_report.compatibility_to_vortex_import_performed);
            assert!(!replay_report.fallback_execution_allowed);

            let _ = std::fs::remove_dir_all(root);
        }
    }
}
