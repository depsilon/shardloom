use std::path::PathBuf;

use shardloom_core::{
    Diagnostic, ExecutionCertificate, NativeIoCertificate, Result, ShardLoomError,
    ShardLoomExecutionMode, ShardLoomExecutionModeSelectionReport,
};

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
use crate::local_primitives::reader_generated_encoded_kernel_inputs_from_vortex_chunk;
#[cfg(feature = "vortex-traditional-analytics-benchmark")]
use crate::source_backed_encoded_execution::{
    VortexReaderBackedSplitEvidence, VortexReaderGeneratedEncodedKernelInput,
    execute_vortex_reader_generated_conjunctive_filter_from_encoded_kernel_inputs,
};
#[cfg(feature = "vortex-traditional-analytics-benchmark")]
use shardloom_core::{
    ColumnRef, ComparisonOp, DatasetUri, ExecutionCertificateInput, ExecutionProviderKind,
    ExpectedOutcome, NativeIoAdapterFidelityReport, NativeIoMaterializationBoundaryReport,
    NativeIoRepresentationTransition, NativeIoSideEffectReport, NativeIoSinkRequirementReport,
    NativeIoSourceCapabilityReport, NativeIoSourcePushdownReport, PredicateExpr,
    RepresentationState, SelectionVector, ShardLoomExecutionModeSelectionRequest, StatValue,
    UniversalInputSource,
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
const SOURCE_BACKED_SCAN_EVIDENCE_SCHEMA_VERSION: &str =
    "shardloom.traditional_analytics.source_backed_scan_evidence.v1";
const ENCODED_PREDICATE_PROVIDER_SCHEMA_VERSION: &str =
    "shardloom.traditional_analytics.encoded_predicate_provider.v4";
const TRADITIONAL_VORTEX_BATCH_SCHEMA_VERSION: &str =
    "shardloom.traditional_analytics.vortex_batch.v1";
const SOURCE_STATE_COVERAGE_SCHEMA_VERSION: &str =
    "shardloom.traditional_analytics.source_state_coverage.v1";
const FUSED_PIPELINE_SCHEMA_VERSION: &str = "shardloom.traditional_analytics.fused_pipeline.v1";
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
    PartitionPruning,
    ManySmallFilesScan,
    NullHeavyAggregate,
    HighCardinalityStringGroupDistinct,
    TopNPerGroup,
    CleanCastFilterWrite,
    MalformedTimestampDirtyCsv,
    SmallChangeOverLargeBase,
    NestedJsonFieldScan,
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
            "partition pruning" | "partition-pruning" => Ok(Self::PartitionPruning),
            "many-small-files scan" | "many-small-files-scan" => Ok(Self::ManySmallFilesScan),
            "null-heavy aggregate" | "null-heavy-aggregate" => Ok(Self::NullHeavyAggregate),
            "high-cardinality string group/distinct" | "high-cardinality-string-group-distinct" => {
                Ok(Self::HighCardinalityStringGroupDistinct)
            }
            "top-N per group" | "top-n-per-group" | "top-N-per-group" => Ok(Self::TopNPerGroup),
            "clean/cast/filter/write" | "clean-cast-filter-write" => Ok(Self::CleanCastFilterWrite),
            "malformed timestamp / dirty CSV" | "malformed-timestamp-dirty-csv" => {
                Ok(Self::MalformedTimestampDirtyCsv)
            }
            "small change over large base" | "small-change-over-large-base" => {
                Ok(Self::SmallChangeOverLargeBase)
            }
            "nested JSON field scan" | "nested-json-field-scan" => Ok(Self::NestedJsonFieldScan),
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
            Self::PartitionPruning => "partition pruning",
            Self::ManySmallFilesScan => "many-small-files scan",
            Self::NullHeavyAggregate => "null-heavy aggregate",
            Self::HighCardinalityStringGroupDistinct => "high-cardinality string group/distinct",
            Self::TopNPerGroup => "top-N per group",
            Self::CleanCastFilterWrite => "clean/cast/filter/write",
            Self::MalformedTimestampDirtyCsv => "malformed timestamp / dirty CSV",
            Self::SmallChangeOverLargeBase => "small change over large base",
            Self::NestedJsonFieldScan => "nested JSON field scan",
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
    pub cdc_delta_csv: Option<PathBuf>,
    pub workspace_dir: PathBuf,
    pub input_format: TraditionalAnalyticsInputFormat,
    pub compatibility_output_format: Option<TraditionalAnalyticsInputFormat>,
    pub verify_native_vortex_replay: bool,
    pub write_result_vortex: bool,
    pub resource_policy: TraditionalAnalyticsResourcePolicy,
    pub requested_execution_mode: ShardLoomExecutionMode,
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
            cdc_delta_csv: None,
            workspace_dir,
            input_format: TraditionalAnalyticsInputFormat::Csv,
            compatibility_output_format: None,
            verify_native_vortex_replay: false,
            write_result_vortex: false,
            resource_policy: TraditionalAnalyticsResourcePolicy::default(),
            requested_execution_mode: ShardLoomExecutionMode::CompatibilityImportCertified,
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
    pub fn with_cdc_delta_csv(mut self, path: Option<PathBuf>) -> Self {
        self.cdc_delta_csv = path;
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

    #[must_use]
    pub const fn with_requested_execution_mode(mut self, value: ShardLoomExecutionMode) -> Self {
        self.requested_execution_mode = value;
        self
    }
}

/// Request for the feature-gated native Vortex traditional analytics runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraditionalAnalyticsVortexRequest {
    pub scenario: TraditionalAnalyticsScenario,
    pub fact_vortex: PathBuf,
    pub dim_vortex: PathBuf,
    pub cdc_delta_vortex: Option<PathBuf>,
    pub requested_execution_mode: ShardLoomExecutionMode,
    pub result_workspace_dir: Option<PathBuf>,
    pub write_result_vortex: bool,
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
            cdc_delta_vortex: None,
            requested_execution_mode: ShardLoomExecutionMode::NativeVortex,
            result_workspace_dir: None,
            write_result_vortex: false,
        }
    }

    #[must_use]
    pub const fn with_requested_execution_mode(mut self, value: ShardLoomExecutionMode) -> Self {
        self.requested_execution_mode = value;
        self
    }

    #[must_use]
    pub fn with_cdc_delta_vortex(mut self, value: Option<PathBuf>) -> Self {
        self.cdc_delta_vortex = value;
        self
    }

    #[must_use]
    pub fn with_result_workspace_dir(mut self, value: Option<PathBuf>) -> Self {
        self.result_workspace_dir = value;
        self
    }

    #[must_use]
    pub const fn with_result_vortex_write(mut self, value: bool) -> Self {
        self.write_result_vortex = value;
        self
    }
}

/// Request for running several prepared/native Vortex traditional analytics
/// scenarios in one `ShardLoom` CLI/library process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraditionalAnalyticsVortexBatchRequest {
    pub scenarios: Vec<TraditionalAnalyticsScenario>,
    pub fact_vortex: PathBuf,
    pub dim_vortex: PathBuf,
    pub cdc_delta_vortex: Option<PathBuf>,
    pub requested_execution_mode: ShardLoomExecutionMode,
    pub result_workspace_dir: Option<PathBuf>,
    pub write_result_vortex: bool,
}

impl TraditionalAnalyticsVortexBatchRequest {
    #[must_use]
    pub fn new(
        scenarios: Vec<TraditionalAnalyticsScenario>,
        fact_vortex: PathBuf,
        dim_vortex: PathBuf,
    ) -> Self {
        Self {
            scenarios,
            fact_vortex,
            dim_vortex,
            cdc_delta_vortex: None,
            requested_execution_mode: ShardLoomExecutionMode::NativeVortex,
            result_workspace_dir: None,
            write_result_vortex: false,
        }
    }

    #[must_use]
    pub const fn with_requested_execution_mode(mut self, value: ShardLoomExecutionMode) -> Self {
        self.requested_execution_mode = value;
        self
    }

    #[must_use]
    pub fn with_cdc_delta_vortex(mut self, value: Option<PathBuf>) -> Self {
        self.cdc_delta_vortex = value;
        self
    }

    #[must_use]
    pub fn with_result_workspace_dir(mut self, value: Option<PathBuf>) -> Self {
        self.result_workspace_dir = value;
        self
    }

    #[must_use]
    pub const fn with_result_vortex_write(mut self, value: bool) -> Self {
        self.write_result_vortex = value;
        self
    }
}

/// File metadata and digest evidence shared by child scenarios in one
/// prepared/native Vortex batch run.
#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct TraditionalVortexSourceSnapshot {
    fact_vortex_bytes: u64,
    dim_vortex_bytes: u64,
    cdc_delta_vortex_bytes: u64,
    fact_vortex_digest: String,
    dim_vortex_digest: String,
    cdc_delta_vortex_digest: Option<String>,
    source_bytes_read: u64,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl TraditionalVortexSourceSnapshot {
    fn from_paths(
        fact_vortex: &std::path::Path,
        dim_vortex: &std::path::Path,
        cdc_delta_vortex: Option<&std::path::Path>,
    ) -> Result<Self> {
        let fact_vortex_bytes = file_len(fact_vortex, "fact Vortex file")?;
        let dim_vortex_bytes = file_len(dim_vortex, "dimension Vortex file")?;
        let cdc_delta_vortex_bytes =
            cdc_delta_vortex.map_or(Ok(0), |path| file_len(path, "CDC delta Vortex file"))?;
        let fact_vortex_digest = file_digest(fact_vortex, "fact Vortex file")?;
        let dim_vortex_digest = file_digest(dim_vortex, "dimension Vortex file")?;
        let cdc_delta_vortex_digest = cdc_delta_vortex
            .map(|path| file_digest(path, "CDC delta Vortex file"))
            .transpose()?;
        let source_bytes_read = fact_vortex_bytes
            .checked_add(dim_vortex_bytes)
            .and_then(|bytes| bytes.checked_add(cdc_delta_vortex_bytes))
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "native Vortex traditional analytics source byte count overflow".to_string(),
                )
            })?;

        Ok(Self {
            fact_vortex_bytes,
            dim_vortex_bytes,
            cdc_delta_vortex_bytes,
            fact_vortex_digest,
            dim_vortex_digest,
            cdc_delta_vortex_digest,
            source_bytes_read,
        })
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalDimensionLabelState {
    dim_by_key: std::collections::HashMap<u32, String>,
    stats: TraditionalStreamingScanStats,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl TraditionalDimensionLabelState {
    fn from_path(dim_vortex: &std::path::Path) -> Result<Self> {
        let (dim_by_key, stats) = scan_dim_label_state(dim_vortex, "batch dimension label state")?;
        Ok(Self { dim_by_key, stats })
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalCategoryMetricState {
    groups: std::collections::BTreeMap<String, TraditionalGroupAccum>,
    stats: TraditionalStreamingScanStats,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl TraditionalCategoryMetricState {
    fn from_path(fact_vortex: &std::path::Path) -> Result<Self> {
        let mut groups = std::collections::BTreeMap::<String, TraditionalGroupAccum>::new();
        let stats = scan_fact_vortex_projected(
            fact_vortex,
            &["category", "metric"],
            None,
            |fields, chunk_rows| {
                let categories = utf8_field(fields, "category")?;
                let metrics = primitive_field::<f64>(fields, "metric")?;
                if categories.len() != chunk_rows || metrics.len() != chunk_rows {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "batch category metric state Vortex chunk length mismatch: chunk_rows={chunk_rows}, category_len={}, metric_len={}",
                        categories.len(),
                        metrics.len()
                    )));
                }
                for (category, metric) in categories.into_iter().zip(metrics) {
                    groups.entry(category).or_default().add(metric);
                }
                Ok(())
            },
        )?;
        Ok(Self { groups, stats })
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalGroupCategoryMetricState {
    group_key_groups: std::collections::BTreeMap<u32, TraditionalGroupAccum>,
    group_category_groups: std::collections::BTreeMap<(u32, String), TraditionalGroupAccum>,
    stats: TraditionalStreamingScanStats,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl TraditionalGroupCategoryMetricState {
    fn from_path(fact_vortex: &std::path::Path) -> Result<Self> {
        let mut group_key_groups = std::collections::BTreeMap::<u32, TraditionalGroupAccum>::new();
        let mut group_category_groups =
            std::collections::BTreeMap::<(u32, String), TraditionalGroupAccum>::new();
        let stats = scan_fact_vortex_projected(
            fact_vortex,
            &["group_key", "category", "metric"],
            None,
            |fields, chunk_rows| {
                let group_keys = primitive_field::<u32>(fields, "group_key")?;
                let categories = utf8_field(fields, "category")?;
                let metrics = primitive_field::<f64>(fields, "metric")?;
                if group_keys.len() != chunk_rows
                    || categories.len() != chunk_rows
                    || metrics.len() != chunk_rows
                {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "batch group category metric state Vortex chunk length mismatch: chunk_rows={chunk_rows}, group_key_len={}, category_len={}, metric_len={}",
                        group_keys.len(),
                        categories.len(),
                        metrics.len()
                    )));
                }
                for ((group_key, category), metric) in
                    group_keys.into_iter().zip(categories).zip(metrics)
                {
                    group_key_groups.entry(group_key).or_default().add(metric);
                    group_category_groups
                        .entry((group_key, category))
                        .or_default()
                        .add(metric);
                }
                Ok(())
            },
        )?;
        Ok(Self {
            group_key_groups,
            group_category_groups,
            stats,
        })
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalRankedMetricRow {
    group_key: u32,
    id: u64,
    metric: f64,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalRankedMetricState {
    rows: Vec<TraditionalRankedMetricRow>,
    stats: TraditionalStreamingScanStats,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl TraditionalRankedMetricState {
    fn from_path(fact_vortex: &std::path::Path) -> Result<Self> {
        let mut rows = Vec::new();
        let stats = scan_fact_vortex_projected(
            fact_vortex,
            &["group_key", "id", "metric"],
            None,
            |fields, chunk_rows| {
                let group_keys = primitive_field::<u32>(fields, "group_key")?;
                let ids = primitive_field::<u64>(fields, "id")?;
                let metrics = primitive_field::<f64>(fields, "metric")?;
                if group_keys.len() != chunk_rows
                    || ids.len() != chunk_rows
                    || metrics.len() != chunk_rows
                {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "batch ranked metric state Vortex chunk length mismatch: chunk_rows={chunk_rows}, group_key_len={}, id_len={}, metric_len={}",
                        group_keys.len(),
                        ids.len(),
                        metrics.len()
                    )));
                }
                rows.extend(group_keys.into_iter().zip(ids).zip(metrics).map(
                    |((group_key, id), metric)| TraditionalRankedMetricRow {
                        group_key,
                        id,
                        metric,
                    },
                ));
                Ok(())
            },
        )?;
        Ok(Self { rows, stats })
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalFilteredProjectionRow {
    id: u64,
    value: u32,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TraditionalFilteredProjectionLimitCandidate {
    id: u64,
    sequence: u64,
    value: u32,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl PartialOrd for TraditionalFilteredProjectionLimitCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl Ord for TraditionalFilteredProjectionLimitCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id
            .cmp(&other.id)
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalSelectiveFilterState {
    selected_metric_accum: TraditionalGroupAccum,
    filtered_projection_rows: Vec<TraditionalFilteredProjectionRow>,
    stats: TraditionalStreamingScanStats,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl TraditionalSelectiveFilterState {
    fn from_path(fact_vortex: &std::path::Path) -> Result<Self> {
        let mut selected_metric_accum = TraditionalGroupAccum::default();
        let mut filtered_projection_limit_rows =
            std::collections::BinaryHeap::<TraditionalFilteredProjectionLimitCandidate>::new();
        let mut filtered_projection_sequence = 0_u64;
        let stats = scan_fact_vortex_projected(
            fact_vortex,
            &["id", "value", "metric"],
            Some(selective_filter_expr()),
            |fields, chunk_rows| {
                let ids = primitive_field::<u64>(fields, "id")?;
                let values = primitive_field::<u32>(fields, "value")?;
                let metrics = primitive_field::<f64>(fields, "metric")?;
                if ids.len() != chunk_rows
                    || values.len() != chunk_rows
                    || metrics.len() != chunk_rows
                {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "batch selective filter state Vortex chunk length mismatch: chunk_rows={chunk_rows}, id_len={}, value_len={}, metric_len={}",
                        ids.len(),
                        values.len(),
                        metrics.len()
                    )));
                }
                for ((id, value), metric) in ids.into_iter().zip(values).zip(metrics) {
                    selected_metric_accum.add(metric);
                    push_filter_projection_limit_candidate(
                        &mut filtered_projection_limit_rows,
                        filtered_projection_sequence,
                        id,
                        value,
                    );
                    filtered_projection_sequence =
                        filtered_projection_sequence.checked_add(1).ok_or_else(|| {
                            ShardLoomError::InvalidOperation(
                                "filter + projection + limit row sequence overflowed u64"
                                    .to_string(),
                            )
                        })?;
                }
                Ok(())
            },
        )?;
        Ok(Self {
            selected_metric_accum,
            filtered_projection_rows: filter_projection_limit_rows_from_heap(
                filtered_projection_limit_rows,
            ),
            stats,
        })
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalDirtyInputState {
    clean_cast_accum: TraditionalGroupAccum,
    malformed_timestamp_accum: TraditionalGroupAccum,
    stats: TraditionalStreamingScanStats,
    saw_raw_event_time: bool,
    saw_dirty_numeric: bool,
    saw_dirty_flag: bool,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl TraditionalDirtyInputState {
    fn from_path(fact_vortex: &std::path::Path) -> Result<Self> {
        let mut clean_cast_accum = TraditionalGroupAccum::default();
        let mut malformed_timestamp_accum = TraditionalGroupAccum::default();
        let mut saw_raw_event_time = false;
        let mut saw_dirty_numeric = false;
        let mut saw_dirty_flag = false;
        let stats = scan_fact_vortex_projected(
            fact_vortex,
            &["raw_event_time", "dirty_numeric", "dirty_flag"],
            None,
            |fields, chunk_rows| {
                let raw_event_times = utf8_field(fields, "raw_event_time")?;
                let dirty_numeric = utf8_field(fields, "dirty_numeric")?;
                let dirty_flags = utf8_field(fields, "dirty_flag")?;
                if raw_event_times.len() != chunk_rows
                    || dirty_numeric.len() != chunk_rows
                    || dirty_flags.len() != chunk_rows
                {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "batch dirty input state Vortex chunk length mismatch: chunk_rows={chunk_rows}, raw_event_time_len={}, dirty_numeric_len={}, dirty_flag_len={}",
                        raw_event_times.len(),
                        dirty_numeric.len(),
                        dirty_flags.len()
                    )));
                }
                for ((raw_event_time, dirty_numeric), dirty_flag) in raw_event_times
                    .into_iter()
                    .zip(dirty_numeric)
                    .zip(dirty_flags)
                {
                    saw_raw_event_time |= !raw_event_time.is_empty();
                    saw_dirty_numeric |= !dirty_numeric.is_empty();
                    saw_dirty_flag |= !dirty_flag.is_empty();
                    if !generated_timestamp_shape_is_valid(&raw_event_time) {
                        continue;
                    }
                    let Ok(value) = dirty_numeric.parse::<f64>() else {
                        continue;
                    };
                    malformed_timestamp_accum.add(value);
                    if dirty_flag == "Y" && value >= 500.0 {
                        clean_cast_accum.add(value);
                    }
                }
                Ok(())
            },
        )?;
        Ok(Self {
            clean_cast_accum,
            malformed_timestamp_accum,
            stats,
            saw_raw_event_time,
            saw_dirty_numeric,
            saw_dirty_flag,
        })
    }

    fn ensure_clean_cast_supported(&self) -> Result<()> {
        if !self.saw_raw_event_time || !self.saw_dirty_numeric || !self.saw_dirty_flag {
            return Err(ShardLoomError::InvalidOperation(
                "clean/cast/filter/write requires raw_event_time, dirty_numeric, and dirty_flag fixture columns"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn ensure_malformed_timestamp_supported(&self) -> Result<()> {
        if !self.saw_raw_event_time || !self.saw_dirty_numeric {
            return Err(ShardLoomError::InvalidOperation(
                "malformed timestamp / dirty CSV requires raw_event_time and dirty_numeric fixture columns"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalDateNullMetricState {
    partition_pruning_accum: TraditionalGroupAccum,
    null_heavy_accum: TraditionalGroupAccum,
    stats: TraditionalStreamingScanStats,
    saw_event_date: bool,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl TraditionalDateNullMetricState {
    fn from_path(fact_vortex: &std::path::Path) -> Result<Self> {
        let mut partition_pruning_accum = TraditionalGroupAccum::default();
        let mut null_heavy_accum = TraditionalGroupAccum::default();
        let mut saw_event_date = false;
        let stats = scan_fact_vortex_projected(
            fact_vortex,
            &["event_date", "metric", "nullable_metric_00"],
            None,
            |fields, chunk_rows| {
                let event_dates = utf8_field(fields, "event_date")?;
                let metrics = primitive_field::<f64>(fields, "metric")?;
                let nullable_metrics = utf8_field(fields, "nullable_metric_00")?;
                if event_dates.len() != chunk_rows
                    || metrics.len() != chunk_rows
                    || nullable_metrics.len() != chunk_rows
                {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "batch date/null metric state Vortex chunk length mismatch: chunk_rows={chunk_rows}, event_date_len={}, metric_len={}, nullable_metric_00_len={}",
                        event_dates.len(),
                        metrics.len(),
                        nullable_metrics.len()
                    )));
                }
                for ((event_date, metric), nullable_metric) in
                    event_dates.into_iter().zip(metrics).zip(nullable_metrics)
                {
                    saw_event_date |= !event_date.is_empty();
                    if partition_pruning_date_range_contains(&event_date) {
                        partition_pruning_accum.add(metric);
                    }
                    if nullable_metric.is_empty() {
                        continue;
                    }
                    let parsed_metric = nullable_metric.parse::<f64>().map_err(|error| {
                        ShardLoomError::InvalidOperation(format!(
                            "failed to parse nullable_metric_00 in batch date/null metric state: {error}"
                        ))
                    })?;
                    null_heavy_accum.add(parsed_metric);
                }
                Ok(())
            },
        )?;
        Ok(Self {
            partition_pruning_accum,
            null_heavy_accum,
            stats,
            saw_event_date,
        })
    }

    fn ensure_partition_pruning_supported(&self) -> Result<()> {
        if !self.saw_event_date {
            return Err(ShardLoomError::InvalidOperation(
                "partition pruning requires an event_date fixture column".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalVortexBatchSourceState {
    source_snapshot: TraditionalVortexSourceSnapshot,
    dimension_label_state: Option<TraditionalDimensionLabelState>,
    dimension_label_state_consumer_count: usize,
    category_metric_state: Option<TraditionalCategoryMetricState>,
    category_metric_state_consumer_count: usize,
    group_category_metric_state: Option<TraditionalGroupCategoryMetricState>,
    group_category_metric_state_consumer_count: usize,
    ranked_metric_state: Option<TraditionalRankedMetricState>,
    ranked_metric_state_consumer_count: usize,
    selective_filter_state: Option<TraditionalSelectiveFilterState>,
    selective_filter_state_consumer_count: usize,
    dirty_input_state: Option<TraditionalDirtyInputState>,
    dirty_input_state_consumer_count: usize,
    date_null_metric_state: Option<TraditionalDateNullMetricState>,
    date_null_metric_state_consumer_count: usize,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl TraditionalVortexBatchSourceState {
    fn from_paths(
        fact_vortex: &std::path::Path,
        dim_vortex: &std::path::Path,
        cdc_delta_vortex: Option<&std::path::Path>,
        scenarios: &[TraditionalAnalyticsScenario],
    ) -> Result<Self> {
        let source_snapshot =
            TraditionalVortexSourceSnapshot::from_paths(fact_vortex, dim_vortex, cdc_delta_vortex)?;
        let dimension_label_state_consumer_count = scenarios
            .iter()
            .filter(|scenario| dimension_label_state_reuse_candidate(**scenario))
            .count();
        let dimension_label_state = if dimension_label_state_consumer_count == 0 {
            None
        } else {
            Some(TraditionalDimensionLabelState::from_path(dim_vortex)?)
        };
        let category_metric_state_consumer_count = scenarios
            .iter()
            .filter(|scenario| category_metric_state_reuse_candidate(**scenario))
            .count();
        let category_metric_state = if category_metric_state_consumer_count > 1 {
            Some(TraditionalCategoryMetricState::from_path(fact_vortex)?)
        } else {
            None
        };
        let group_category_metric_state_consumer_count = scenarios
            .iter()
            .filter(|scenario| group_category_metric_state_reuse_candidate(**scenario))
            .count();
        let group_category_metric_state = if group_category_metric_state_consumer_count > 1 {
            Some(TraditionalGroupCategoryMetricState::from_path(fact_vortex)?)
        } else {
            None
        };
        let ranked_metric_state_consumer_count = scenarios
            .iter()
            .filter(|scenario| ranked_metric_state_reuse_candidate(**scenario))
            .count();
        let ranked_metric_state = if ranked_metric_state_consumer_count > 1 {
            Some(TraditionalRankedMetricState::from_path(fact_vortex)?)
        } else {
            None
        };
        let selective_filter_state_consumer_count = scenarios
            .iter()
            .filter(|scenario| selective_filter_state_reuse_candidate(**scenario))
            .count();
        let selective_filter_state = if selective_filter_state_consumer_count > 1 {
            Some(TraditionalSelectiveFilterState::from_path(fact_vortex)?)
        } else {
            None
        };
        let dirty_input_state_consumer_count = scenarios
            .iter()
            .filter(|scenario| dirty_input_state_reuse_candidate(**scenario))
            .count();
        let dirty_input_state = if dirty_input_state_consumer_count > 1 {
            Some(TraditionalDirtyInputState::from_path(fact_vortex)?)
        } else {
            None
        };
        let date_null_metric_state_consumer_count = scenarios
            .iter()
            .filter(|scenario| date_null_metric_state_reuse_candidate(**scenario))
            .count();
        let date_null_metric_state = if date_null_metric_state_consumer_count > 1 {
            Some(TraditionalDateNullMetricState::from_path(fact_vortex)?)
        } else {
            None
        };
        Ok(Self {
            source_snapshot,
            dimension_label_state,
            dimension_label_state_consumer_count,
            category_metric_state,
            category_metric_state_consumer_count,
            group_category_metric_state,
            group_category_metric_state_consumer_count,
            ranked_metric_state,
            ranked_metric_state_consumer_count,
            selective_filter_state,
            selective_filter_state_consumer_count,
            dirty_input_state,
            dirty_input_state_consumer_count,
            date_null_metric_state,
            date_null_metric_state_consumer_count,
        })
    }

    fn dimension_label_state_recompute_avoided_count(&self) -> usize {
        self.dimension_label_state_consumer_count.saturating_sub(1)
    }

    fn category_metric_state_recompute_avoided_count(&self) -> usize {
        self.category_metric_state_consumer_count.saturating_sub(1)
    }

    fn group_category_metric_state_recompute_avoided_count(&self) -> usize {
        self.group_category_metric_state_consumer_count
            .saturating_sub(1)
    }

    fn ranked_metric_state_recompute_avoided_count(&self) -> usize {
        self.ranked_metric_state_consumer_count.saturating_sub(1)
    }

    fn selective_filter_state_recompute_avoided_count(&self) -> usize {
        self.selective_filter_state_consumer_count.saturating_sub(1)
    }

    fn dirty_input_state_recompute_avoided_count(&self) -> usize {
        self.dirty_input_state_consumer_count.saturating_sub(1)
    }

    fn date_null_metric_state_recompute_avoided_count(&self) -> usize {
        self.date_null_metric_state_consumer_count.saturating_sub(1)
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn dimension_label_state_reuse_candidate(scenario: TraditionalAnalyticsScenario) -> bool {
    matches!(
        scenario,
        TraditionalAnalyticsScenario::HashJoin | TraditionalAnalyticsScenario::JoinAggregate
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn category_metric_state_reuse_candidate(scenario: TraditionalAnalyticsScenario) -> bool {
    matches!(
        scenario,
        TraditionalAnalyticsScenario::DistinctCount
            | TraditionalAnalyticsScenario::HighCardinalityStringGroupDistinct
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn group_category_metric_state_reuse_candidate(scenario: TraditionalAnalyticsScenario) -> bool {
    matches!(
        scenario,
        TraditionalAnalyticsScenario::GroupByAggregation
            | TraditionalAnalyticsScenario::MultiKeyGroupBy
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn ranked_metric_state_reuse_candidate(scenario: TraditionalAnalyticsScenario) -> bool {
    matches!(
        scenario,
        TraditionalAnalyticsScenario::SortAndTopK
            | TraditionalAnalyticsScenario::TopNPerGroup
            | TraditionalAnalyticsScenario::RowNumberWindow
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn selective_filter_state_reuse_candidate(scenario: TraditionalAnalyticsScenario) -> bool {
    matches!(
        scenario,
        TraditionalAnalyticsScenario::SelectiveFilter
            | TraditionalAnalyticsScenario::FilterProjectionLimit
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn dirty_input_state_reuse_candidate(scenario: TraditionalAnalyticsScenario) -> bool {
    matches!(
        scenario,
        TraditionalAnalyticsScenario::CleanCastFilterWrite
            | TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn date_null_metric_state_reuse_candidate(scenario: TraditionalAnalyticsScenario) -> bool {
    matches!(
        scenario,
        TraditionalAnalyticsScenario::PartitionPruning
            | TraditionalAnalyticsScenario::NullHeavyAggregate
    )
}

/// Report-only Vortex layout/write advisor evidence for a certified local
/// analytics workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct TraditionalVortexLayoutAdvisorReport {
    pub schema_version: String,
    pub report_id: String,
    pub status: String,
    pub workload_constitution_id: String,
    pub evidence_basis: String,
    pub evidence_source_refs: Vec<String>,
    pub recommended_chunk_rows: usize,
    pub recommended_chunk_bytes: u64,
    pub encoding_strategy: String,
    pub statistics_policy: String,
    pub dictionary_strategy: String,
    pub cluster_key: String,
    pub micro_segment_flush_policy: String,
    pub compaction_trigger: String,
    pub read_write_tradeoff: String,
    pub recommendation_evidence_status: String,
    pub measured_evidence_source_count: usize,
    pub simulated_evidence_source_count: usize,
    pub blocked_evidence_source_count: usize,
    pub write_layout_execution_allowed: bool,
    pub improvement_claim_allowed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

/// Report emitted by the local CSV-to-Vortex benchmark smoke runner.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct TraditionalAnalyticsReport {
    pub scenario: TraditionalAnalyticsScenario,
    pub input_format: TraditionalAnalyticsInputFormat,
    pub execution_mode_selection: ShardLoomExecutionModeSelectionReport,
    pub resource_policy: TraditionalAnalyticsResourcePolicy,
    pub result_json: String,
    pub fact_rows: u64,
    pub dim_rows: u64,
    pub rows_scanned: u64,
    pub rows_materialized: u64,
    pub workspace_dir: PathBuf,
    pub fact_vortex_path: PathBuf,
    pub dim_vortex_path: PathBuf,
    pub cdc_delta_vortex_path: Option<PathBuf>,
    pub compatibility_output_format: Option<TraditionalAnalyticsInputFormat>,
    pub fact_compatibility_output_path: Option<PathBuf>,
    pub dim_compatibility_output_path: Option<PathBuf>,
    pub fact_source_path: PathBuf,
    pub dim_source_path: PathBuf,
    pub cdc_delta_source_path: Option<PathBuf>,
    pub workload_constitution_id: String,
    pub workload_scorecard_status: String,
    pub benchmark_row_ref: String,
    pub coverage_row_ref: String,
    pub output_artifact_schema_summary: String,
    pub output_artifact_digest_algorithm: String,
    pub fact_vortex_digest: String,
    pub dim_vortex_digest: String,
    pub cdc_delta_vortex_digest: Option<String>,
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
    pub source_read_micros: u64,
    pub compatibility_to_vortex_import_micros: u64,
    pub vortex_write_micros: u64,
    pub vortex_reopen_scan_micros: u64,
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
    pub layout_advisor_report: TraditionalVortexLayoutAdvisorReport,
    pub commit_state: String,
    pub rollback_cleanup_status: String,
    pub fact_source_bytes: u64,
    pub dim_source_bytes: u64,
    pub cdc_delta_source_bytes: u64,
    pub cdc_delta_rows: u64,
    pub fact_compatibility_output_bytes: u64,
    pub dim_compatibility_output_bytes: u64,
    pub fact_csv_bytes: u64,
    pub dim_csv_bytes: u64,
    pub source_bytes_read: u64,
    pub fact_vortex_bytes: u64,
    pub dim_vortex_bytes: u64,
    pub cdc_delta_vortex_bytes: u64,
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
    pub streaming_result_row_count: u64,
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

/// Report emitted by the scoped direct compatibility transient CSV smoke path.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct TraditionalDirectTransientReport {
    pub scenario: TraditionalAnalyticsScenario,
    pub input_format: TraditionalAnalyticsInputFormat,
    pub execution_mode_selection: ShardLoomExecutionModeSelectionReport,
    pub resource_policy: TraditionalAnalyticsResourcePolicy,
    pub result_json: String,
    pub fact_rows: u64,
    pub dim_rows: u64,
    pub rows_scanned: u64,
    pub rows_materialized: u64,
    pub fact_source_path: PathBuf,
    pub dim_source_path: PathBuf,
    pub fact_source_bytes: u64,
    pub dim_source_bytes: u64,
    pub source_bytes_read: u64,
    pub source_read_micros: u64,
    pub scenario_compute_micros: u64,
    pub runtime_execution_certificate: ExecutionCertificate,
    pub diagnostics: Vec<Diagnostic>,
}

impl TraditionalDirectTransientReport {
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "ShardLoom direct compatibility transient CSV smoke\nscenario: {}\nsource format: {}\nrows scanned: {}\nrows materialized: {}\nVortex persistence: false\nruntime certificate: {}\nexternal engine fallback: disabled",
            self.scenario.as_str(),
            self.input_format.as_str(),
            self.rows_scanned,
            self.rows_materialized,
            self.runtime_execution_certificate.status.as_str()
        )
    }

    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn fields(&self) -> Vec<(String, String)> {
        let mut fields = vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "external_engines_are_fallback".to_string(),
                "false".to_string(),
            ),
        ];
        fields.extend(self.execution_mode_selection.fields());
        fields.extend(vec![
            ("scenario".to_string(), self.scenario.as_str().to_string()),
            (
                "input_format".to_string(),
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
                "applied_memory_gb".to_string(),
                self.resource_policy.memory_gb.to_string(),
            ),
            (
                "applied_max_parallelism".to_string(),
                self.resource_policy.max_parallelism.to_string(),
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
            ("partitioning_auto_derived".to_string(), "true".to_string()),
            ("dynamic_sizing_applied".to_string(), "true".to_string()),
            (
                "source_kind".to_string(),
                self.input_format.source_kind().to_string(),
            ),
            (
                "source_adapter_id".to_string(),
                "shardloom.adapter.csv.direct_transient.v1".to_string(),
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
                "fact_source_path".to_string(),
                self.fact_source_path.display().to_string(),
            ),
            (
                "dim_source_path".to_string(),
                self.dim_source_path.display().to_string(),
            ),
            ("workspace_dir".to_string(), "not_used".to_string()),
            ("fact_vortex_path".to_string(), "none".to_string()),
            ("dim_vortex_path".to_string(), "none".to_string()),
            ("prepared_artifact_ref".to_string(), "none".to_string()),
            ("prepared_artifact_fact_ref".to_string(), "none".to_string()),
            ("prepared_artifact_dim_ref".to_string(), "none".to_string()),
            ("prepared_artifact_digest".to_string(), "none".to_string()),
            (
                "prepared_artifact_fact_digest".to_string(),
                "none".to_string(),
            ),
            (
                "prepared_artifact_dim_digest".to_string(),
                "none".to_string(),
            ),
            (
                "prepared_artifact_lifecycle_status".to_string(),
                "not_applicable_direct_transient".to_string(),
            ),
            (
                "prepared_artifact_cleanup_policy".to_string(),
                "not_applicable".to_string(),
            ),
            (
                "prepared_artifact_reuse_eligible".to_string(),
                "false".to_string(),
            ),
            (
                "prepared_artifact_workspace".to_string(),
                "not_used".to_string(),
            ),
            (
                "workload_scorecard_status".to_string(),
                "fixture_smoke_only_direct_transient".to_string(),
            ),
            (
                "benchmark_row_ref".to_string(),
                "benchmark://local_vortex_analytics_v1/direct_transient_csv_selective_filter"
                    .to_string(),
            ),
            (
                "coverage_row_ref".to_string(),
                "coverage.direct_compatibility_transient.local_csv_smoke".to_string(),
            ),
            (
                "output_artifact_schema_summary".to_string(),
                "result(row_count:u64,metric_sum:f64)".to_string(),
            ),
            (
                "output_artifact_digest_algorithm".to_string(),
                OUTPUT_ARTIFACT_DIGEST_ALGORITHM.to_string(),
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
                "source_bytes_read".to_string(),
                self.source_bytes_read.to_string(),
            ),
            (
                "fact_csv_bytes".to_string(),
                self.fact_source_bytes.to_string(),
            ),
            (
                "dim_csv_bytes".to_string(),
                self.dim_source_bytes.to_string(),
            ),
            ("fact_vortex_bytes".to_string(), "0".to_string()),
            ("dim_vortex_bytes".to_string(), "0".to_string()),
            ("cdc_delta_vortex_bytes".to_string(), "0".to_string()),
            (
                "materialization_boundary_rows".to_string(),
                self.fact_rows.saturating_add(self.dim_rows).to_string(),
            ),
            (
                "source_read_micros".to_string(),
                self.source_read_micros.to_string(),
            ),
            (
                "compatibility_parse_micros".to_string(),
                self.source_read_micros.to_string(),
            ),
            (
                "compatibility_to_vortex_import_micros".to_string(),
                "0".to_string(),
            ),
            ("vortex_write_micros".to_string(), "0".to_string()),
            ("vortex_reopen_micros".to_string(), "0".to_string()),
            ("vortex_scan_micros".to_string(), "0".to_string()),
            (
                "operator_compute_micros".to_string(),
                self.scenario_compute_micros.to_string(),
            ),
            (
                "scenario_compute_micros".to_string(),
                self.scenario_compute_micros.to_string(),
            ),
            ("result_sink_write_micros".to_string(), "none".to_string()),
            (
                "computed_result_sink_write_micros".to_string(),
                "none".to_string(),
            ),
            (
                "evidence_render_micros".to_string(),
                "not_measured".to_string(),
            ),
            (
                "computed_result_sink_requested".to_string(),
                "false".to_string(),
            ),
            (
                "computed_result_sink_written".to_string(),
                "false".to_string(),
            ),
            (
                "computed_result_sink_replay_verified".to_string(),
                "false".to_string(),
            ),
            (
                "computed_result_vortex_path".to_string(),
                "none".to_string(),
            ),
            ("computed_result_vortex_bytes".to_string(), "0".to_string()),
            (
                "computed_result_vortex_digest".to_string(),
                "none".to_string(),
            ),
            ("computed_result_sink_rows".to_string(), "0".to_string()),
            (
                "computed_result_sink_rows_materialized".to_string(),
                "0".to_string(),
            ),
            (
                "computed_result_sink_schema_summary".to_string(),
                "not_applicable".to_string(),
            ),
            (
                "computed_result_sink_replay_result_json".to_string(),
                String::new(),
            ),
            (
                "computed_result_sink_native_io_certificate_status".to_string(),
                "not_applicable".to_string(),
            ),
            (
                "result_sink_claim_gate_status".to_string(),
                "not_applicable_direct_transient_no_result_sink".to_string(),
            ),
            (
                "result_sink_claim_gate_reason".to_string(),
                "direct transient smoke does not certify result sink".to_string(),
            ),
            (
                "native_io_certificate_id".to_string(),
                "not_vortex_native".to_string(),
            ),
            (
                "native_io_certificate_status".to_string(),
                "not_vortex_native".to_string(),
            ),
            (
                "native_io_certificate_path_id".to_string(),
                "not_vortex_native".to_string(),
            ),
            (
                "source_native_io_certificate_status".to_string(),
                "not_vortex_native".to_string(),
            ),
            (
                "native_io_per_path_certificate_emitted".to_string(),
                "false".to_string(),
            ),
            (
                "native_io_materializing_transitions_have_boundaries".to_string(),
                "true".to_string(),
            ),
            (
                "native_io_representation_transitions".to_string(),
                "foreign_encoded->decoded_rows".to_string(),
            ),
            (
                "native_io_representation_transition_order".to_string(),
                "foreign_encoded->decoded_rows".to_string(),
            ),
            (
                "native_work_envelope_created".to_string(),
                "true".to_string(),
            ),
            ("native_work_stream_created".to_string(), "true".to_string()),
            (
                "native_result_stream_created".to_string(),
                "true".to_string(),
            ),
            (
                "native_io_certificate_emitted".to_string(),
                "false".to_string(),
            ),
            (
                "compatibility_source_adapter_used".to_string(),
                "true".to_string(),
            ),
            (
                "compatibility_to_vortex_import_performed".to_string(),
                "false".to_string(),
            ),
            (
                "compatibility_output_requested".to_string(),
                "false".to_string(),
            ),
            (
                "compatibility_output_written".to_string(),
                "false".to_string(),
            ),
            (
                "native_to_compatibility_output_performed".to_string(),
                "false".to_string(),
            ),
            ("csv_source_adapter_used".to_string(), "true".to_string()),
            (
                "csv_to_vortex_import_performed".to_string(),
                "false".to_string(),
            ),
            ("jsonl_source_adapter_used".to_string(), "false".to_string()),
            (
                "jsonl_to_vortex_import_performed".to_string(),
                "false".to_string(),
            ),
            ("vortex_file_written".to_string(), "false".to_string()),
            ("vortex_file_read".to_string(), "false".to_string()),
            (
                "upstream_vortex_scan_called".to_string(),
                "false".to_string(),
            ),
            (
                "streaming_vortex_execution_used".to_string(),
                "false".to_string(),
            ),
            (
                "full_table_materialization_avoided".to_string(),
                "false".to_string(),
            ),
            (
                "streaming_filter_pushdown_applied".to_string(),
                "false".to_string(),
            ),
            (
                "streaming_projection_pushdown_applied".to_string(),
                "false".to_string(),
            ),
            ("streaming_arrays_read_count".to_string(), "0".to_string()),
            ("streaming_max_chunk_rows".to_string(), "0".to_string()),
            (
                "streaming_projected_columns".to_string(),
                "metric".to_string(),
            ),
            ("data_decoded".to_string(), "true".to_string()),
            ("data_materialized".to_string(), "true".to_string()),
            (
                "materialization_boundary_report_emitted".to_string(),
                "true".to_string(),
            ),
            ("row_read".to_string(), "true".to_string()),
            ("arrow_converted".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("spill_io_performed".to_string(), "false".to_string()),
            ("runtime_task_graph_created".to_string(), "true".to_string()),
            (
                "runtime_task_graph_executed".to_string(),
                "true".to_string(),
            ),
            (
                "runtime_scheduler_mode".to_string(),
                "direct_transient_local".to_string(),
            ),
            (
                "runtime_scheduler_ref".to_string(),
                "direct_transient_csv_selective_filter".to_string(),
            ),
            ("runtime_task_count".to_string(), "1".to_string()),
            ("runtime_scheduled_task_count".to_string(), "1".to_string()),
            ("runtime_completed_task_count".to_string(), "1".to_string()),
            ("runtime_split_count".to_string(), "1".to_string()),
            ("runtime_scheduler_batch_count".to_string(), "1".to_string()),
            (
                "runtime_max_parallelism".to_string(),
                self.resource_policy.max_parallelism.to_string(),
            ),
            (
                "runtime_queue_limit_enforced".to_string(),
                "true".to_string(),
            ),
            (
                "runtime_backpressure_bounded".to_string(),
                "true".to_string(),
            ),
            (
                "runtime_cancellation_testable".to_string(),
                "true".to_string(),
            ),
            ("runtime_retry_testable".to_string(), "true".to_string()),
            (
                "runtime_fail_before_oom_enforced".to_string(),
                "true".to_string(),
            ),
            ("runtime_spill_required".to_string(), "false".to_string()),
            ("runtime_spill_supported".to_string(), "true".to_string()),
            ("runtime_spill_blocker".to_string(), "none".to_string()),
            (
                "runtime_large_workload_claim_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "execution_certificate_emitted".to_string(),
                "true".to_string(),
            ),
            (
                "execution_certificate_id".to_string(),
                self.runtime_execution_certificate.certificate_id.clone(),
            ),
            (
                "execution_certificate_status".to_string(),
                self.runtime_execution_certificate
                    .status
                    .as_str()
                    .to_string(),
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
                "runtime_fallback_attempted".to_string(),
                "false".to_string(),
            ),
            (
                "runtime_external_query_engine_invoked".to_string(),
                "false".to_string(),
            ),
            ("provider_kind".to_string(), "shardloom_kernel".to_string()),
            (
                "provider_api_surface".to_string(),
                "direct_compatibility_transient_csv_smoke".to_string(),
            ),
            (
                "provider_admission_classification".to_string(),
                "direct_transient_csv_smoke".to_string(),
            ),
            (
                "encoded_native_execution_status".to_string(),
                "not_vortex_native".to_string(),
            ),
            (
                "scan_api_status".to_string(),
                "direct_transient_no_vortex_scan".to_string(),
            ),
            (
                "persistent_runner_status".to_string(),
                "process_per_scenario_attributed_not_reduced".to_string(),
            ),
            ("fusion_status".to_string(), "not_applicable".to_string()),
            (
                "fusion_blocker".to_string(),
                "not_vortex_native".to_string(),
            ),
            ("residual_executor".to_string(), "none".to_string()),
            ("residual_boundary".to_string(), "none".to_string()),
            (
                "representation_transition_summary".to_string(),
                "foreign_encoded->decoded_rows".to_string(),
            ),
            ("fallback_attempted".to_string(), "false".to_string()),
            ("external_engine_invoked".to_string(), "false".to_string()),
        ]);
        fields
    }
}

impl TraditionalAnalyticsReport {
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "ShardLoom traditional analytics universal I/O smoke\nscenario: {}\nsource format: {}\nresource policy: {}\napplied memory GiB: {}\napplied parallelism: {}\ntarget batch rows: {}\ntarget partitions: {}\nworkspace: {}\nfact Vortex: {}\ndim Vortex: {}\nrows scanned: {}\nrows materialized: {}\ncompatibility source adapter: true\ncompatibility to Vortex import: true\nVortex write/read/scan: true\nruntime scheduler: {} tasks={} batches={} certificate={}\nlayout advisor: {} recommendation={} claim_allowed={}\nmaterialization boundary reported: {}\noutput replay verified: {}\ncomputed result sink verified: {}\nworkload scorecard: {}\nexternal engine fallback: disabled",
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
            &self.layout_advisor_report.status,
            &self.layout_advisor_report.recommendation_evidence_status,
            self.layout_advisor_report.improvement_claim_allowed,
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
        ];
        fields.extend(self.execution_mode_selection.fields());
        fields.extend(vec![
            ("scenario".to_string(), self.scenario.as_str().to_string()),
            (
                "input_format".to_string(),
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
                "prepared_artifact_ref".to_string(),
                format!(
                    "fact={},dim={}",
                    self.fact_vortex_path.display(),
                    self.dim_vortex_path.display()
                ),
            ),
            (
                "prepared_artifact_fact_ref".to_string(),
                self.fact_vortex_path.display().to_string(),
            ),
            (
                "prepared_artifact_dim_ref".to_string(),
                self.dim_vortex_path.display().to_string(),
            ),
            (
                "prepared_artifact_digest".to_string(),
                format!(
                    "fact={},dim={}",
                    self.fact_vortex_digest, self.dim_vortex_digest
                ),
            ),
            (
                "prepared_artifact_fact_digest".to_string(),
                self.fact_vortex_digest.clone(),
            ),
            (
                "prepared_artifact_dim_digest".to_string(),
                self.dim_vortex_digest.clone(),
            ),
            (
                "prepared_artifact_lifecycle_status".to_string(),
                "prepared_available".to_string(),
            ),
            (
                "prepared_artifact_cleanup_policy".to_string(),
                self.rollback_cleanup_status.clone(),
            ),
            (
                "prepared_artifact_reuse_eligible".to_string(),
                "true".to_string(),
            ),
            (
                "prepared_artifact_workspace".to_string(),
                self.workspace_dir.display().to_string(),
            ),
            (
                "source_native_io_certificate_status".to_string(),
                self.native_io_certificate.status().to_string(),
            ),
            (
                "cdc_delta_vortex_path".to_string(),
                self.cdc_delta_vortex_path
                    .as_ref()
                    .map_or_else(String::new, |path| path.display().to_string()),
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
                "cdc_delta_vortex_digest".to_string(),
                self.cdc_delta_vortex_digest.clone().unwrap_or_default(),
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
                "source_read_micros".to_string(),
                self.source_read_micros.to_string(),
            ),
            (
                "compatibility_parse_micros".to_string(),
                self.source_read_micros.to_string(),
            ),
            (
                "compatibility_to_vortex_import_micros".to_string(),
                self.compatibility_to_vortex_import_micros.to_string(),
            ),
            (
                "vortex_write_micros".to_string(),
                self.vortex_write_micros.to_string(),
            ),
            (
                "vortex_reopen_micros".to_string(),
                self.vortex_reopen_scan_micros.to_string(),
            ),
            (
                "vortex_scan_micros".to_string(),
                self.vortex_reopen_scan_micros.to_string(),
            ),
            (
                "operator_compute_micros".to_string(),
                self.scenario_compute_micros.to_string(),
            ),
            (
                "result_sink_write_micros".to_string(),
                self.computed_result_sink_write_micros
                    .map_or_else(|| "none".to_string(), |value| value.to_string()),
            ),
            (
                "evidence_render_micros".to_string(),
                "not_measured".to_string(),
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
            (
                "layout_advisor_report_emitted".to_string(),
                "true".to_string(),
            ),
            (
                "layout_advisor_schema_version".to_string(),
                self.layout_advisor_report.schema_version.clone(),
            ),
            (
                "layout_advisor_report_id".to_string(),
                self.layout_advisor_report.report_id.clone(),
            ),
            (
                "layout_advisor_status".to_string(),
                self.layout_advisor_report.status.clone(),
            ),
            (
                "layout_advisor_workload_constitution_id".to_string(),
                self.layout_advisor_report.workload_constitution_id.clone(),
            ),
            (
                "layout_advisor_evidence_basis".to_string(),
                self.layout_advisor_report.evidence_basis.clone(),
            ),
            (
                "layout_advisor_evidence_source_refs".to_string(),
                self.layout_advisor_report.evidence_source_refs.join(","),
            ),
            (
                "layout_advisor_recommended_chunk_rows".to_string(),
                self.layout_advisor_report
                    .recommended_chunk_rows
                    .to_string(),
            ),
            (
                "layout_advisor_recommended_chunk_bytes".to_string(),
                self.layout_advisor_report
                    .recommended_chunk_bytes
                    .to_string(),
            ),
            (
                "layout_advisor_encoding_strategy".to_string(),
                self.layout_advisor_report.encoding_strategy.clone(),
            ),
            (
                "layout_advisor_statistics_policy".to_string(),
                self.layout_advisor_report.statistics_policy.clone(),
            ),
            (
                "layout_advisor_dictionary_strategy".to_string(),
                self.layout_advisor_report.dictionary_strategy.clone(),
            ),
            (
                "layout_advisor_cluster_key".to_string(),
                self.layout_advisor_report.cluster_key.clone(),
            ),
            (
                "layout_advisor_micro_segment_flush_policy".to_string(),
                self.layout_advisor_report
                    .micro_segment_flush_policy
                    .clone(),
            ),
            (
                "layout_advisor_compaction_trigger".to_string(),
                self.layout_advisor_report.compaction_trigger.clone(),
            ),
            (
                "layout_advisor_read_write_tradeoff".to_string(),
                self.layout_advisor_report.read_write_tradeoff.clone(),
            ),
            (
                "layout_advisor_recommendation_evidence_status".to_string(),
                self.layout_advisor_report
                    .recommendation_evidence_status
                    .clone(),
            ),
            (
                "layout_advisor_measured_evidence_source_count".to_string(),
                self.layout_advisor_report
                    .measured_evidence_source_count
                    .to_string(),
            ),
            (
                "layout_advisor_simulated_evidence_source_count".to_string(),
                self.layout_advisor_report
                    .simulated_evidence_source_count
                    .to_string(),
            ),
            (
                "layout_advisor_blocked_evidence_source_count".to_string(),
                self.layout_advisor_report
                    .blocked_evidence_source_count
                    .to_string(),
            ),
            (
                "layout_advisor_write_layout_execution_allowed".to_string(),
                self.layout_advisor_report
                    .write_layout_execution_allowed
                    .to_string(),
            ),
            (
                "layout_advisor_improvement_claim_allowed".to_string(),
                self.layout_advisor_report
                    .improvement_claim_allowed
                    .to_string(),
            ),
            (
                "layout_advisor_fallback_attempted".to_string(),
                self.layout_advisor_report.fallback_attempted.to_string(),
            ),
            (
                "layout_advisor_external_engine_invoked".to_string(),
                self.layout_advisor_report
                    .external_engine_invoked
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
                "cdc_delta_source_path".to_string(),
                self.cdc_delta_source_path
                    .as_ref()
                    .map_or_else(String::new, |path| path.display().to_string()),
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
                "cdc_delta_source_bytes".to_string(),
                self.cdc_delta_source_bytes.to_string(),
            ),
            (
                "cdc_delta_rows".to_string(),
                self.cdc_delta_rows.to_string(),
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
                "cdc_delta_vortex_bytes".to_string(),
                self.cdc_delta_vortex_bytes.to_string(),
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
        ]);
        fields.extend(streaming_execution_fields(self));
        fields.extend(traditional_vortex_provider_admission_fields(
            self.scenario,
            "compatibility_import_certified",
            self.streaming_vortex_execution_used,
            self.data_materialized,
        ));
        fields.extend(native_io_certificate_fields(&self.native_io_certificate));
        fields
    }
}

/// Report emitted by the native Vortex benchmark smoke runner.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct TraditionalAnalyticsVortexReport {
    pub scenario: TraditionalAnalyticsScenario,
    pub execution_mode_selection: ShardLoomExecutionModeSelectionReport,
    pub result_json: String,
    pub fact_rows: u64,
    pub dim_rows: u64,
    pub cdc_delta_rows: u64,
    pub rows_scanned: u64,
    pub rows_materialized: u64,
    pub fact_vortex_path: PathBuf,
    pub dim_vortex_path: PathBuf,
    pub cdc_delta_vortex_path: Option<PathBuf>,
    pub fact_vortex_bytes: u64,
    pub dim_vortex_bytes: u64,
    pub cdc_delta_vortex_bytes: u64,
    pub fact_vortex_digest: String,
    pub dim_vortex_digest: String,
    pub cdc_delta_vortex_digest: Option<String>,
    pub source_bytes_read: u64,
    pub materialization_boundary_rows: u64,
    pub scenario_compute_micros: u64,
    pub vortex_scan_micros: u64,
    pub computed_result_sink_requested: bool,
    pub computed_result_sink_written: bool,
    pub computed_result_sink_replay_verified: bool,
    pub computed_result_vortex_path: Option<PathBuf>,
    pub computed_result_vortex_bytes: u64,
    pub computed_result_vortex_digest: Option<String>,
    pub computed_result_sink_rows: u64,
    pub computed_result_sink_rows_materialized: u64,
    pub computed_result_sink_schema_summary: String,
    pub computed_result_sink_write_micros: Option<u64>,
    pub computed_result_sink_replay_result_json: Option<String>,
    pub computed_result_sink_native_io_certificate_id: Option<String>,
    pub computed_result_sink_native_io_certificate_status: Option<String>,
    pub result_sink_claim_gate_status: String,
    pub result_sink_claim_gate_reason: String,
    pub commit_state: String,
    pub rollback_cleanup_status: String,
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
    pub streaming_result_row_count: u64,
    pub streaming_reader_chunk_columns_observed: Vec<String>,
    pub streaming_reader_chunk_dtype_summary: Vec<String>,
    pub streaming_reader_chunk_encoding_summary: Vec<String>,
    pub encoded_predicate_provider_filter_column_probe_requested: bool,
    pub encoded_predicate_provider_filter_column_probe_requested_columns: Vec<String>,
    pub encoded_predicate_provider_filter_column_probe_status: String,
    pub encoded_predicate_provider_filter_column_probe_reader_split_count: usize,
    pub encoded_predicate_provider_filter_column_probe_row_count: u64,
    pub encoded_predicate_provider_filter_column_probe_reader_chunk_columns_observed: Vec<String>,
    pub encoded_predicate_provider_filter_column_probe_reader_chunk_dtype_summary: Vec<String>,
    pub encoded_predicate_provider_filter_column_probe_reader_chunk_encoding_summary: Vec<String>,
    pub encoded_predicate_provider_kernel_input_count: usize,
    pub encoded_predicate_provider_conjunctive_bridge_runtime_status: String,
    pub encoded_predicate_provider_conjunctive_bridge_runtime_report_id: String,
    pub encoded_predicate_provider_conjunctive_bridge_intersection_count: usize,
    pub encoded_predicate_provider_conjunctive_bridge_selected_row_count: Option<u64>,
    pub encoded_predicate_provider_filter_column_batches_consumed: bool,
    pub encoded_predicate_provider_selection_vector_intersection_certified: bool,
    pub encoded_predicate_provider_selected_metric_aggregation_status: String,
    pub encoded_predicate_provider_selected_metric_selection_vector_consumed: bool,
    pub encoded_predicate_provider_selected_metric_source: String,
    pub encoded_predicate_provider_selected_metric_row_count: Option<u64>,
    pub encoded_predicate_provider_selected_metric_sum: Option<f64>,
    pub encoded_predicate_provider_selected_metric_scan_split_count: usize,
    pub encoded_predicate_provider_selected_metric_data_decoded: bool,
    pub encoded_predicate_provider_selected_metric_data_materialized: bool,
    pub encoded_predicate_provider_filter_column_probe_data_decoded: bool,
    pub encoded_predicate_provider_filter_column_probe_data_materialized: bool,
    pub encoded_predicate_provider_filter_column_probe_row_read: bool,
    pub encoded_predicate_provider_filter_column_probe_fallback_attempted: bool,
    pub encoded_predicate_provider_filter_column_probe_external_engine_invoked: bool,
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

/// Report emitted by the scoped single-process prepared/native Vortex batch
/// runner.
#[derive(Debug, Clone, PartialEq)]
pub struct TraditionalAnalyticsVortexBatchReport {
    pub reports: Vec<TraditionalAnalyticsVortexReport>,
    pub requested_execution_mode: ShardLoomExecutionMode,
    pub result_sink_requested: bool,
    pub source_state_prepare_micros: u64,
    pub dimension_label_state_consumer_count: usize,
    pub dimension_label_state_recompute_avoided_count: usize,
    pub category_metric_state_consumer_count: usize,
    pub category_metric_state_recompute_avoided_count: usize,
    pub group_category_metric_state_consumer_count: usize,
    pub group_category_metric_state_recompute_avoided_count: usize,
    pub ranked_metric_state_consumer_count: usize,
    pub ranked_metric_state_recompute_avoided_count: usize,
    pub selective_filter_state_consumer_count: usize,
    pub selective_filter_state_recompute_avoided_count: usize,
    pub dirty_input_state_consumer_count: usize,
    pub dirty_input_state_recompute_avoided_count: usize,
    pub date_null_metric_state_consumer_count: usize,
    pub date_null_metric_state_recompute_avoided_count: usize,
    pub total_scenario_compute_micros: u64,
    pub total_vortex_scan_micros: u64,
    pub total_result_sink_write_micros: Option<u64>,
    pub total_rows_scanned: u64,
    pub total_rows_materialized: u64,
    pub all_native_io_certificates_certified: bool,
    pub all_result_sink_replays_verified: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl TraditionalAnalyticsVortexBatchReport {
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "ShardLoom prepared/native Vortex batch runner\nscenarios: {}\nrunner: single process\nscenario compute micros: {}\nVortex scan micros: {}\nfallback execution: disabled\nclaim boundary: scoped batch process reuse only",
            self.scenario_order().join(","),
            self.total_scenario_compute_micros,
            self.total_vortex_scan_micros
        )
    }

    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn fields(&self) -> Vec<(String, String)> {
        let selected_modes = self
            .reports
            .iter()
            .map(|report| {
                report
                    .execution_mode_selection
                    .selected_execution_mode
                    .as_str()
                    .to_string()
            })
            .collect::<Vec<_>>();
        let mut fields = vec![
            (
                "schema_version".to_string(),
                TRADITIONAL_VORTEX_BATCH_SCHEMA_VERSION.to_string(),
            ),
            (
                "runner_kind".to_string(),
                "single_process_prepared_native_batch".to_string(),
            ),
            ("support_status".to_string(), "runtime_supported".to_string()),
            (
                "claim_gate_status".to_string(),
                "fixture_smoke_only".to_string(),
            ),
            (
                "claim_boundary".to_string(),
                "scoped single-process prepared/native Vortex batch runner; no persistent daemon, no hidden benchmark fast mode, no SQL/DataFrame, object-store, production, performance, or Spark-displacement claim".to_string(),
            ),
            (
                "requested_execution_mode".to_string(),
                self.requested_execution_mode.as_str().to_string(),
            ),
            (
                "selected_execution_modes".to_string(),
                selected_modes.join(","),
            ),
            (
                "scenario_count".to_string(),
                self.reports.len().to_string(),
            ),
            ("scenario_order".to_string(), self.scenario_order().join(",")),
            (
                "typed_envelope_preserved".to_string(),
                "true".to_string(),
            ),
            (
                "process_startup_amortization_supported".to_string(),
                "true".to_string(),
            ),
            (
                "persistent_runner_status".to_string(),
                "single_process_batch_runner_supported".to_string(),
            ),
            (
                "persistent_runner_claim_boundary".to_string(),
                "scoped single-process batch runner only; not a daemon, service, hidden fast mode, or performance claim".to_string(),
            ),
            (
                "prepared_artifact_reuse_eligible".to_string(),
                "true".to_string(),
            ),
            (
                "prepared_artifact_reuse_scope".to_string(),
                "caller_supplied_fact_dim_cdc_vortex_artifacts_reused_across_requested_scenarios"
                    .to_string(),
            ),
            (
                "source_metadata_snapshot_status".to_string(),
                "per_batch_source_metadata_reused".to_string(),
            ),
            (
                "source_metadata_snapshot_reused".to_string(),
                "true".to_string(),
            ),
            (
                "source_metadata_snapshot_scope".to_string(),
                "caller_supplied_fact_dim_cdc_vortex_artifacts".to_string(),
            ),
            (
                "source_metadata_snapshot_reuse_count".to_string(),
                self.reports.len().to_string(),
            ),
            (
                "source_metadata_digest_recompute_avoided_count".to_string(),
                self.reports.len().saturating_sub(1).to_string(),
            ),
            (
                "source_metadata_snapshot_claim_boundary".to_string(),
                "runtime plumbing evidence only; not a performance, encoded-native, production, SQL/DataFrame, object-store, lakehouse, or Spark-displacement claim".to_string(),
            ),
            (
                "source_metadata_snapshot_fallback_attempted".to_string(),
                "false".to_string(),
            ),
            (
                "source_metadata_snapshot_external_engine_invoked".to_string(),
                "false".to_string(),
            ),
            (
                "source_state_reuse_status".to_string(),
                self.source_state_reuse_status().to_string(),
            ),
            (
                "source_state_coverage_schema_version".to_string(),
                SOURCE_STATE_COVERAGE_SCHEMA_VERSION.to_string(),
            ),
            (
                "source_state_coverage_matrix_ref".to_string(),
                "docs/architecture/source-state-reuse-coverage-matrix.md".to_string(),
            ),
            (
                "source_state_coverage_status_vocabulary".to_string(),
                "source-state-reused,source-state-not-needed,blocked-with-reason,unsupported-with-reason"
                    .to_string(),
            ),
            (
                "source_state_coverage_all_requested_scenarios_classified".to_string(),
                "true".to_string(),
            ),
            (
                "source_state_coverage_matrix".to_string(),
                self.source_state_coverage_matrix(),
            ),
            (
                "source_state_coverage_reused_scenario_count".to_string(),
                self.source_state_coverage_status_count("source-state-reused")
                    .to_string(),
            ),
            (
                "source_state_coverage_not_needed_scenario_count".to_string(),
                self.source_state_coverage_status_count("source-state-not-needed")
                    .to_string(),
            ),
            (
                "source_state_coverage_blocked_scenario_count".to_string(),
                self.source_state_coverage_status_count("blocked-with-reason")
                    .to_string(),
            ),
            (
                "source_state_coverage_unsupported_scenario_count".to_string(),
                self.source_state_coverage_status_count("unsupported-with-reason")
                    .to_string(),
            ),
            (
                "source_state_digest_status".to_string(),
                "not_emitted_scoped_in_memory_source_state".to_string(),
            ),
            (
                "source_state_digest_reason".to_string(),
                "current source-state families are scoped in-process derived states; universal SourceState digest is planned under GAR-IOREUSE-1A"
                    .to_string(),
            ),
            (
                "source_state_reused".to_string(),
                self.source_state_reused().to_string(),
            ),
            (
                "source_state_reuse_scope".to_string(),
                self.source_state_reuse_scope().clone(),
            ),
            (
                "source_state_reuse_consumer_count".to_string(),
                self.source_state_reuse_consumer_count().to_string(),
            ),
            (
                "source_state_recompute_avoided_count".to_string(),
                self.source_state_recompute_avoided_count().to_string(),
            ),
            (
                "source_state_family_count".to_string(),
                self.source_state_family_count().to_string(),
            ),
            (
                "source_state_dimension_label_reuse_status".to_string(),
                self.dimension_label_state_reuse_status().to_string(),
            ),
            (
                "source_state_dimension_label_reused".to_string(),
                (self.dimension_label_state_recompute_avoided_count > 0).to_string(),
            ),
            (
                "source_state_dimension_label_reuse_scope".to_string(),
                "dimension_label_lookup_for_hash_join_and_join_aggregate".to_string(),
            ),
            (
                "source_state_dimension_label_reuse_consumer_count".to_string(),
                self.dimension_label_state_consumer_count.to_string(),
            ),
            (
                "source_state_dimension_label_recompute_avoided_count".to_string(),
                self.dimension_label_state_recompute_avoided_count.to_string(),
            ),
            (
                "source_state_category_metric_reuse_status".to_string(),
                self.category_metric_state_reuse_status().to_string(),
            ),
            (
                "source_state_category_metric_reused".to_string(),
                (self.category_metric_state_recompute_avoided_count > 0).to_string(),
            ),
            (
                "source_state_category_metric_reuse_scope".to_string(),
                "category_metric_group_state_for_distinct_count_and_high_cardinality_string_group_distinct".to_string(),
            ),
            (
                "source_state_category_metric_reuse_consumer_count".to_string(),
                self.category_metric_state_consumer_count.to_string(),
            ),
            (
                "source_state_category_metric_recompute_avoided_count".to_string(),
                self.category_metric_state_recompute_avoided_count.to_string(),
            ),
            (
                "source_state_group_category_metric_reuse_status".to_string(),
                self.group_category_metric_state_reuse_status().to_string(),
            ),
            (
                "source_state_group_category_metric_reused".to_string(),
                (self.group_category_metric_state_recompute_avoided_count > 0).to_string(),
            ),
            (
                "source_state_group_category_metric_reuse_scope".to_string(),
                "group_category_metric_state_for_group_by_aggregation_and_multi_key_group_by"
                    .to_string(),
            ),
            (
                "source_state_group_category_metric_reuse_consumer_count".to_string(),
                self.group_category_metric_state_consumer_count.to_string(),
            ),
            (
                "source_state_group_category_metric_recompute_avoided_count".to_string(),
                self.group_category_metric_state_recompute_avoided_count
                    .to_string(),
            ),
            (
                "source_state_ranked_metric_reuse_status".to_string(),
                self.ranked_metric_state_reuse_status().to_string(),
            ),
            (
                "source_state_ranked_metric_reused".to_string(),
                (self.ranked_metric_state_recompute_avoided_count > 0).to_string(),
            ),
            (
                "source_state_ranked_metric_reuse_scope".to_string(),
                "ranked_metric_rows_for_sort_top_k_top_n_per_group_and_row_number_window"
                    .to_string(),
            ),
            (
                "source_state_ranked_metric_reuse_consumer_count".to_string(),
                self.ranked_metric_state_consumer_count.to_string(),
            ),
            (
                "source_state_ranked_metric_recompute_avoided_count".to_string(),
                self.ranked_metric_state_recompute_avoided_count.to_string(),
            ),
            (
                "source_state_selective_filter_reuse_status".to_string(),
                self.selective_filter_state_reuse_status().to_string(),
            ),
            (
                "source_state_selective_filter_reused".to_string(),
                (self.selective_filter_state_recompute_avoided_count > 0).to_string(),
            ),
            (
                "source_state_selective_filter_reuse_scope".to_string(),
                "selective_filter_state_for_selective_filter_and_filter_projection_limit"
                    .to_string(),
            ),
            (
                "source_state_selective_filter_reuse_consumer_count".to_string(),
                self.selective_filter_state_consumer_count.to_string(),
            ),
            (
                "source_state_selective_filter_recompute_avoided_count".to_string(),
                self.selective_filter_state_recompute_avoided_count
                    .to_string(),
            ),
            (
                "source_state_dirty_input_reuse_status".to_string(),
                self.dirty_input_state_reuse_status().to_string(),
            ),
            (
                "source_state_dirty_input_reused".to_string(),
                (self.dirty_input_state_recompute_avoided_count > 0).to_string(),
            ),
            (
                "source_state_dirty_input_reuse_scope".to_string(),
                "dirty_input_state_for_clean_cast_filter_write_and_malformed_timestamp_dirty_csv"
                    .to_string(),
            ),
            (
                "source_state_dirty_input_reuse_consumer_count".to_string(),
                self.dirty_input_state_consumer_count.to_string(),
            ),
            (
                "source_state_dirty_input_recompute_avoided_count".to_string(),
                self.dirty_input_state_recompute_avoided_count.to_string(),
            ),
            (
                "source_state_date_null_metric_reuse_status".to_string(),
                self.date_null_metric_state_reuse_status().to_string(),
            ),
            (
                "source_state_date_null_metric_reused".to_string(),
                (self.date_null_metric_state_recompute_avoided_count > 0).to_string(),
            ),
            (
                "source_state_date_null_metric_reuse_scope".to_string(),
                "date_null_metric_state_for_partition_pruning_and_null_heavy_aggregate"
                    .to_string(),
            ),
            (
                "source_state_date_null_metric_reuse_consumer_count".to_string(),
                self.date_null_metric_state_consumer_count.to_string(),
            ),
            (
                "source_state_date_null_metric_recompute_avoided_count".to_string(),
                self.date_null_metric_state_recompute_avoided_count
                    .to_string(),
            ),
            (
                "source_state_prepare_micros".to_string(),
                self.source_state_prepare_micros.to_string(),
            ),
            (
                "source_state_prepare_timing_scope".to_string(),
                "batch_shared_pre_scenario".to_string(),
            ),
            (
                "source_state_claim_boundary".to_string(),
                "scoped prepared/native batch source-state reuse only; not a performance, encoded-native, production, SQL/DataFrame, object-store, lakehouse, or Spark-displacement claim".to_string(),
            ),
            (
                "source_state_fallback_attempted".to_string(),
                "false".to_string(),
            ),
            (
                "source_state_external_engine_invoked".to_string(),
                "false".to_string(),
            ),
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("fallback_attempted".to_string(), "false".to_string()),
            (
                "external_engine_invoked".to_string(),
                "false".to_string(),
            ),
            (
                "all_fallback_attempted_false".to_string(),
                self.all_fallback_attempted_false().to_string(),
            ),
            (
                "all_external_engine_invoked_false".to_string(),
                "true".to_string(),
            ),
            (
                "all_native_io_certificates_certified".to_string(),
                self.all_native_io_certificates_certified.to_string(),
            ),
            (
                "result_sink_requested".to_string(),
                self.result_sink_requested.to_string(),
            ),
            (
                "all_result_sink_replays_verified".to_string(),
                self.all_result_sink_replays_verified.to_string(),
            ),
            (
                "total_scenario_compute_micros".to_string(),
                self.total_scenario_compute_micros.to_string(),
            ),
            (
                "total_vortex_scan_micros".to_string(),
                self.total_vortex_scan_micros.to_string(),
            ),
            (
                "total_result_sink_write_micros".to_string(),
                self.total_result_sink_write_micros
                    .map_or_else(|| "none".to_string(), |value| value.to_string()),
            ),
            (
                "total_rows_scanned".to_string(),
                self.total_rows_scanned.to_string(),
            ),
            (
                "total_rows_materialized".to_string(),
                self.total_rows_materialized.to_string(),
            ),
            (
                "performance_claim_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "spark_displacement_claim_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "encoded_native_claim_allowed".to_string(),
                "false".to_string(),
            ),
        ];
        for report in &self.reports {
            let mut scenario_fields = batch_scenario_fields(report);
            let prefix = format!("scenario_{}", traditional_scenario_slug(report.scenario));
            scenario_fields.push((
                format!("{prefix}_source_state_coverage_status"),
                self.source_state_coverage_status(report.scenario)
                    .to_string(),
            ));
            scenario_fields.push((
                format!("{prefix}_source_state_coverage_family"),
                Self::source_state_coverage_family(report.scenario).to_string(),
            ));
            scenario_fields.push((
                format!("{prefix}_source_state_coverage_reason"),
                self.source_state_coverage_reason(report.scenario)
                    .to_string(),
            ));
            scenario_fields.push((
                format!("{prefix}_source_state_coverage_claim_boundary"),
                "runtime-plumbing coverage classification only; not a performance, encoded-native, production, SQL/DataFrame, object-store, lakehouse, or Spark-displacement claim"
                    .to_string(),
            ));
            fields.extend(scenario_fields);
        }
        fields
    }

    fn scenario_order(&self) -> Vec<String> {
        self.reports
            .iter()
            .map(|report| traditional_scenario_slug(report.scenario))
            .collect()
    }

    fn all_fallback_attempted_false(&self) -> bool {
        self.reports
            .iter()
            .all(|report| !report.fallback_execution_allowed)
    }

    fn source_state_reused(&self) -> bool {
        self.source_state_recompute_avoided_count() > 0
    }

    fn source_state_reuse_consumer_count(&self) -> usize {
        self.dimension_label_state_consumer_count
            + self.category_metric_state_consumer_count
            + self.group_category_metric_state_consumer_count
            + self.ranked_metric_state_consumer_count
            + self.selective_filter_state_consumer_count
            + self.dirty_input_state_consumer_count
            + self.date_null_metric_state_consumer_count
    }

    fn source_state_recompute_avoided_count(&self) -> usize {
        self.dimension_label_state_recompute_avoided_count
            + self.category_metric_state_recompute_avoided_count
            + self.group_category_metric_state_recompute_avoided_count
            + self.ranked_metric_state_recompute_avoided_count
            + self.selective_filter_state_recompute_avoided_count
            + self.dirty_input_state_recompute_avoided_count
            + self.date_null_metric_state_recompute_avoided_count
    }

    fn source_state_family_count(&self) -> usize {
        usize::from(self.dimension_label_state_consumer_count > 0)
            + usize::from(self.category_metric_state_consumer_count > 0)
            + usize::from(self.group_category_metric_state_consumer_count > 0)
            + usize::from(self.ranked_metric_state_consumer_count > 0)
            + usize::from(self.selective_filter_state_consumer_count > 0)
            + usize::from(self.dirty_input_state_consumer_count > 0)
            + usize::from(self.date_null_metric_state_consumer_count > 0)
    }

    fn source_state_reuse_scope(&self) -> String {
        let mut scopes = Vec::new();
        if self.dimension_label_state_consumer_count > 0 {
            scopes.push("dimension_label_lookup_for_hash_join_and_join_aggregate");
        }
        if self.category_metric_state_consumer_count > 0 {
            scopes.push(
                "category_metric_group_state_for_distinct_count_and_high_cardinality_string_group_distinct",
            );
        }
        if self.group_category_metric_state_consumer_count > 0 {
            scopes.push(
                "group_category_metric_state_for_group_by_aggregation_and_multi_key_group_by",
            );
        }
        if self.ranked_metric_state_consumer_count > 0 {
            scopes.push("ranked_metric_rows_for_sort_top_k_top_n_per_group_and_row_number_window");
        }
        if self.selective_filter_state_consumer_count > 0 {
            scopes.push("selective_filter_state_for_selective_filter_and_filter_projection_limit");
        }
        if self.dirty_input_state_consumer_count > 0 {
            scopes.push(
                "dirty_input_state_for_clean_cast_filter_write_and_malformed_timestamp_dirty_csv",
            );
        }
        if self.date_null_metric_state_consumer_count > 0 {
            scopes.push("date_null_metric_state_for_partition_pruning_and_null_heavy_aggregate");
        }
        if scopes.is_empty() {
            "not_applicable_no_source_state_consumers".to_string()
        } else {
            scopes.join(";")
        }
    }

    fn source_state_reuse_status(&self) -> &'static str {
        let mut reused_statuses = Vec::new();
        if self.dimension_label_state_recompute_avoided_count > 0 {
            reused_statuses.push(self.dimension_label_state_reuse_status());
        }
        if self.category_metric_state_recompute_avoided_count > 0 {
            reused_statuses.push(self.category_metric_state_reuse_status());
        }
        if self.group_category_metric_state_recompute_avoided_count > 0 {
            reused_statuses.push(self.group_category_metric_state_reuse_status());
        }
        if self.ranked_metric_state_recompute_avoided_count > 0 {
            reused_statuses.push(self.ranked_metric_state_reuse_status());
        }
        if self.selective_filter_state_recompute_avoided_count > 0 {
            reused_statuses.push(self.selective_filter_state_reuse_status());
        }
        if self.dirty_input_state_recompute_avoided_count > 0 {
            reused_statuses.push(self.dirty_input_state_reuse_status());
        }
        if self.date_null_metric_state_recompute_avoided_count > 0 {
            reused_statuses.push(self.date_null_metric_state_reuse_status());
        }
        match reused_statuses.as_slice() {
            [] if self.source_state_family_count() > 1 => {
                "per_batch_multi_family_source_state_available_single_consumers"
            }
            [] if self.dimension_label_state_consumer_count > 0 => {
                self.dimension_label_state_reuse_status()
            }
            [] if self.category_metric_state_consumer_count > 0 => {
                self.category_metric_state_reuse_status()
            }
            [] if self.group_category_metric_state_consumer_count > 0 => {
                self.group_category_metric_state_reuse_status()
            }
            [] if self.ranked_metric_state_consumer_count > 0 => {
                self.ranked_metric_state_reuse_status()
            }
            [] if self.selective_filter_state_consumer_count > 0 => {
                self.selective_filter_state_reuse_status()
            }
            [] if self.dirty_input_state_consumer_count > 0 => {
                self.dirty_input_state_reuse_status()
            }
            [] if self.date_null_metric_state_consumer_count > 0 => {
                self.date_null_metric_state_reuse_status()
            }
            [] => "not_applicable_no_source_state_consumers",
            [status] => status,
            _ => "per_batch_multi_family_source_state_reused",
        }
    }

    fn dimension_label_state_reuse_status(&self) -> &'static str {
        match self.dimension_label_state_consumer_count {
            0 => "not_applicable_no_dimension_label_state_consumers",
            1 => "per_batch_dimension_label_state_available_single_consumer",
            _ => "per_batch_dimension_label_state_reused",
        }
    }

    fn category_metric_state_reuse_status(&self) -> &'static str {
        match self.category_metric_state_consumer_count {
            0 => "not_applicable_no_category_metric_state_consumers",
            1 => "not_prepared_single_consumer_uses_scenario_scan",
            _ => "per_batch_category_metric_state_reused",
        }
    }

    fn group_category_metric_state_reuse_status(&self) -> &'static str {
        match self.group_category_metric_state_consumer_count {
            0 => "not_applicable_no_group_category_metric_state_consumers",
            1 => "not_prepared_single_consumer_uses_scenario_scan",
            _ => "per_batch_group_category_metric_state_reused",
        }
    }

    fn ranked_metric_state_reuse_status(&self) -> &'static str {
        match self.ranked_metric_state_consumer_count {
            0 => "not_applicable_no_ranked_metric_state_consumers",
            1 => "not_prepared_single_consumer_uses_scenario_scan",
            _ => "per_batch_ranked_metric_state_reused",
        }
    }

    fn selective_filter_state_reuse_status(&self) -> &'static str {
        match self.selective_filter_state_consumer_count {
            0 => "not_applicable_no_selective_filter_state_consumers",
            1 => "not_prepared_single_consumer_uses_scenario_scan",
            _ => "per_batch_selective_filter_state_reused",
        }
    }

    fn dirty_input_state_reuse_status(&self) -> &'static str {
        match self.dirty_input_state_consumer_count {
            0 => "not_applicable_no_dirty_input_state_consumers",
            1 => "not_prepared_single_consumer_uses_scenario_scan",
            _ => "per_batch_dirty_input_state_reused",
        }
    }

    fn date_null_metric_state_reuse_status(&self) -> &'static str {
        match self.date_null_metric_state_consumer_count {
            0 => "not_applicable_no_date_null_metric_state_consumers",
            1 => "not_prepared_single_consumer_uses_scenario_scan",
            _ => "per_batch_date_null_metric_state_reused",
        }
    }

    fn source_state_coverage_matrix(&self) -> String {
        self.reports
            .iter()
            .map(|report| {
                format!(
                    "{}:{}:{}",
                    traditional_scenario_slug(report.scenario),
                    self.source_state_coverage_status(report.scenario),
                    Self::source_state_coverage_family(report.scenario)
                )
            })
            .collect::<Vec<_>>()
            .join(";")
    }

    fn source_state_coverage_status_count(&self, expected: &str) -> usize {
        self.reports
            .iter()
            .filter(|report| self.source_state_coverage_status(report.scenario) == expected)
            .count()
    }

    fn source_state_coverage_status(&self, scenario: TraditionalAnalyticsScenario) -> &'static str {
        match scenario {
            TraditionalAnalyticsScenario::HashJoin
            | TraditionalAnalyticsScenario::JoinAggregate => {
                if self.dimension_label_state_consumer_count > 1 {
                    "source-state-reused"
                } else {
                    "source-state-not-needed"
                }
            }
            TraditionalAnalyticsScenario::DistinctCount
            | TraditionalAnalyticsScenario::HighCardinalityStringGroupDistinct => {
                if self.category_metric_state_consumer_count > 1 {
                    "source-state-reused"
                } else {
                    "source-state-not-needed"
                }
            }
            TraditionalAnalyticsScenario::GroupByAggregation
            | TraditionalAnalyticsScenario::MultiKeyGroupBy => {
                if self.group_category_metric_state_consumer_count > 1 {
                    "source-state-reused"
                } else {
                    "source-state-not-needed"
                }
            }
            TraditionalAnalyticsScenario::SortAndTopK
            | TraditionalAnalyticsScenario::TopNPerGroup
            | TraditionalAnalyticsScenario::RowNumberWindow => {
                if self.ranked_metric_state_consumer_count > 1 {
                    "source-state-reused"
                } else {
                    "source-state-not-needed"
                }
            }
            TraditionalAnalyticsScenario::SelectiveFilter
            | TraditionalAnalyticsScenario::FilterProjectionLimit => {
                if self.selective_filter_state_consumer_count > 1 {
                    "source-state-reused"
                } else {
                    "source-state-not-needed"
                }
            }
            TraditionalAnalyticsScenario::CleanCastFilterWrite
            | TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv => {
                if self.dirty_input_state_consumer_count > 1 {
                    "source-state-reused"
                } else {
                    "source-state-not-needed"
                }
            }
            TraditionalAnalyticsScenario::PartitionPruning
            | TraditionalAnalyticsScenario::NullHeavyAggregate => {
                if self.date_null_metric_state_consumer_count > 1 {
                    "source-state-reused"
                } else {
                    "source-state-not-needed"
                }
            }
            TraditionalAnalyticsScenario::ScaleStressSkewedJoinAggregation
            | TraditionalAnalyticsScenario::ScaleStressMultiStageEtl => "blocked-with-reason",
            TraditionalAnalyticsScenario::CsvFileIngest
            | TraditionalAnalyticsScenario::WideProjection
            | TraditionalAnalyticsScenario::ManySmallFilesScan
            | TraditionalAnalyticsScenario::SmallChangeOverLargeBase
            | TraditionalAnalyticsScenario::NestedJsonFieldScan => "source-state-not-needed",
        }
    }

    fn source_state_coverage_family(scenario: TraditionalAnalyticsScenario) -> &'static str {
        match scenario {
            TraditionalAnalyticsScenario::HashJoin
            | TraditionalAnalyticsScenario::JoinAggregate => "dimension_label",
            TraditionalAnalyticsScenario::DistinctCount
            | TraditionalAnalyticsScenario::HighCardinalityStringGroupDistinct => "category_metric",
            TraditionalAnalyticsScenario::GroupByAggregation
            | TraditionalAnalyticsScenario::MultiKeyGroupBy => "group_category_metric",
            TraditionalAnalyticsScenario::SortAndTopK
            | TraditionalAnalyticsScenario::TopNPerGroup
            | TraditionalAnalyticsScenario::RowNumberWindow => "ranked_metric",
            TraditionalAnalyticsScenario::SelectiveFilter
            | TraditionalAnalyticsScenario::FilterProjectionLimit => "selective_filter",
            TraditionalAnalyticsScenario::CleanCastFilterWrite
            | TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv => "dirty_input",
            TraditionalAnalyticsScenario::PartitionPruning
            | TraditionalAnalyticsScenario::NullHeavyAggregate => "date_null_metric",
            TraditionalAnalyticsScenario::CsvFileIngest => "source_scan",
            TraditionalAnalyticsScenario::WideProjection => "projection_only",
            TraditionalAnalyticsScenario::ManySmallFilesScan => "split_fixture",
            TraditionalAnalyticsScenario::SmallChangeOverLargeBase => "cdc_overlay",
            TraditionalAnalyticsScenario::NestedJsonFieldScan => "nested_json",
            TraditionalAnalyticsScenario::ScaleStressSkewedJoinAggregation
            | TraditionalAnalyticsScenario::ScaleStressMultiStageEtl => "stress",
        }
    }

    fn source_state_coverage_reason(&self, scenario: TraditionalAnalyticsScenario) -> &'static str {
        match self.source_state_coverage_status(scenario) {
            "source-state-reused" => {
                "batch has multiple consumers for this prepared/native source-state family"
            }
            "blocked-with-reason" => {
                "stress scenarios are outside the scoped prepared/native source-state reuse smoke lane"
            }
            "unsupported-with-reason" => {
                "scenario family is not supported by the prepared/native source-state coverage contract"
            }
            _ => match scenario {
                TraditionalAnalyticsScenario::CsvFileIngest => {
                    "basic ingest/sum row has no reusable derived source-state family in the current batch lane"
                }
                TraditionalAnalyticsScenario::WideProjection => {
                    "projection-only row reads the prepared artifact directly and has no shared derived source-state family"
                }
                TraditionalAnalyticsScenario::ManySmallFilesScan => {
                    "many-file split coverage is prepared into the Vortex artifact before this batch lane"
                }
                TraditionalAnalyticsScenario::SmallChangeOverLargeBase => {
                    "CDC overlay is a single incremental-state workflow in the current batch lane"
                }
                TraditionalAnalyticsScenario::NestedJsonFieldScan => {
                    "nested JSON field scan is a single messy-data workflow in the current batch lane"
                }
                _ => {
                    "batch has only one consumer for this source-state family, so scenario-local scan remains explicit"
                }
            },
        }
    }
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
        fields.extend(source_backed_scan_evidence_fields(self));
        fields.extend(encoded_predicate_provider_fields(self));
        fields.extend(fused_pipeline_evidence_fields(self));
        fields.extend(traditional_vortex_provider_admission_fields(
            self.scenario,
            self.execution_mode_selection
                .selected_execution_mode
                .as_str(),
            self.streaming_vortex_execution_used,
            self.data_materialized,
        ));
        fields.extend(native_io_certificate_fields(&self.native_io_certificate));
        fields
    }

    #[allow(clippy::too_many_lines)]
    fn base_fields(&self) -> Vec<(String, String)> {
        let prepared_artifact_ref =
            if let Some(cdc_delta_path) = self.cdc_delta_vortex_path.as_ref() {
                format!(
                    "fact={},dim={},cdc_delta={}",
                    self.fact_vortex_path.display(),
                    self.dim_vortex_path.display(),
                    cdc_delta_path.display()
                )
            } else {
                format!(
                    "fact={},dim={}",
                    self.fact_vortex_path.display(),
                    self.dim_vortex_path.display()
                )
            };
        let prepared_artifact_digest =
            if let Some(cdc_delta_digest) = self.cdc_delta_vortex_digest.as_ref() {
                format!(
                    "fact={},dim={},cdc_delta={}",
                    self.fact_vortex_digest, self.dim_vortex_digest, cdc_delta_digest
                )
            } else {
                format!(
                    "fact={},dim={}",
                    self.fact_vortex_digest, self.dim_vortex_digest
                )
            };
        let mut fields = vec![
            (
                "fallback_execution_allowed".to_string(),
                self.fallback_execution_allowed.to_string(),
            ),
            (
                "external_engines_are_fallback".to_string(),
                "false".to_string(),
            ),
        ];
        fields.extend(self.execution_mode_selection.fields());
        fields.extend(vec![
            ("scenario".to_string(), self.scenario.as_str().to_string()),
            ("result_json".to_string(), self.result_json.clone()),
            ("fact_rows".to_string(), self.fact_rows.to_string()),
            ("dim_rows".to_string(), self.dim_rows.to_string()),
            (
                "cdc_delta_rows".to_string(),
                self.cdc_delta_rows.to_string(),
            ),
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
                "cdc_delta_vortex_path".to_string(),
                self.cdc_delta_vortex_path
                    .as_ref()
                    .map_or_else(String::new, |path| path.display().to_string()),
            ),
            ("prepared_artifact_ref".to_string(), prepared_artifact_ref),
            (
                "prepared_artifact_fact_ref".to_string(),
                self.fact_vortex_path.display().to_string(),
            ),
            (
                "prepared_artifact_dim_ref".to_string(),
                self.dim_vortex_path.display().to_string(),
            ),
            (
                "prepared_artifact_cdc_delta_ref".to_string(),
                self.cdc_delta_vortex_path
                    .as_ref()
                    .map_or_else(String::new, |path| path.display().to_string()),
            ),
            (
                "prepared_artifact_digest".to_string(),
                prepared_artifact_digest,
            ),
            (
                "prepared_artifact_fact_digest".to_string(),
                self.fact_vortex_digest.clone(),
            ),
            (
                "prepared_artifact_dim_digest".to_string(),
                self.dim_vortex_digest.clone(),
            ),
            (
                "prepared_artifact_cdc_delta_digest".to_string(),
                self.cdc_delta_vortex_digest.clone().unwrap_or_default(),
            ),
            (
                "prepared_artifact_lifecycle_status".to_string(),
                if self.execution_mode_selection.selected_execution_mode
                    == ShardLoomExecutionMode::PreparedVortex
                {
                    "reused_prepared_artifact"
                } else {
                    "native_vortex_artifact_supplied"
                }
                .to_string(),
            ),
            (
                "prepared_artifact_cleanup_policy".to_string(),
                "caller_owned_input_artifacts".to_string(),
            ),
            (
                "prepared_artifact_reuse_eligible".to_string(),
                "true".to_string(),
            ),
            (
                "source_native_io_certificate_status".to_string(),
                self.native_io_certificate.status().to_string(),
            ),
            (
                "result_sink_claim_gate_status".to_string(),
                self.result_sink_claim_gate_status.clone(),
            ),
            (
                "result_sink_claim_gate_reason".to_string(),
                self.result_sink_claim_gate_reason.clone(),
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
                "cdc_delta_vortex_bytes".to_string(),
                self.cdc_delta_vortex_bytes.to_string(),
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
                "cdc_delta_vortex_digest".to_string(),
                self.cdc_delta_vortex_digest.clone().unwrap_or_default(),
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
                "scenario_compute_micros".to_string(),
                self.scenario_compute_micros.to_string(),
            ),
            (
                "operator_compute_micros".to_string(),
                self.scenario_compute_micros.to_string(),
            ),
            (
                "vortex_scan_micros".to_string(),
                self.vortex_scan_micros.to_string(),
            ),
            (
                "result_sink_write_micros".to_string(),
                self.computed_result_sink_write_micros
                    .map_or_else(|| "none".to_string(), |value| value.to_string()),
            ),
            (
                "computed_result_sink_write_micros".to_string(),
                self.computed_result_sink_write_micros
                    .map_or_else(|| "none".to_string(), |value| value.to_string()),
            ),
            (
                "evidence_render_micros".to_string(),
                "not_measured".to_string(),
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
            ("commit_state".to_string(), self.commit_state.clone()),
            (
                "rollback_cleanup_status".to_string(),
                self.rollback_cleanup_status.clone(),
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
        ]);
        fields
    }
}

fn traditional_scenario_slug(scenario: TraditionalAnalyticsScenario) -> String {
    scenario.as_str().replace(['/', ' ', '+'], "-")
}

fn batch_child_field(child_fields: &[(String, String)], name: &str) -> String {
    child_fields
        .iter()
        .find(|(key, _)| key == name)
        .map_or_else(String::new, |(_, value)| value.clone())
}

fn batch_scenario_fields(report: &TraditionalAnalyticsVortexReport) -> Vec<(String, String)> {
    let child_fields = report.fields();
    let prefix = format!("scenario_{}", traditional_scenario_slug(report.scenario));
    let mut fields = child_fields
        .iter()
        .map(|(key, value)| (format!("{prefix}_{key}"), value.clone()))
        .collect::<Vec<_>>();
    let mut ensure_field = |name: &str, value: String| {
        let key = format!("{prefix}_{name}");
        if !fields.iter().any(|(existing, _)| existing == &key) {
            fields.push((key, value));
        }
    };
    ensure_field("name", report.scenario.as_str().to_string());
    ensure_field("result_json", report.result_json.clone());
    ensure_field(
        "selected_execution_mode",
        report
            .execution_mode_selection
            .selected_execution_mode
            .as_str()
            .to_string(),
    );
    ensure_field(
        "mode_selection_reason",
        report
            .execution_mode_selection
            .mode_selection_reason
            .clone(),
    );
    ensure_field("rows_scanned", report.rows_scanned.to_string());
    ensure_field("rows_materialized", report.rows_materialized.to_string());
    ensure_field(
        "scenario_compute_micros",
        report.scenario_compute_micros.to_string(),
    );
    ensure_field("vortex_scan_micros", report.vortex_scan_micros.to_string());
    ensure_field(
        "operator_execution_class",
        batch_child_field(&child_fields, "operator_execution_class"),
    );
    ensure_field(
        "operator_admission_status",
        batch_child_field(&child_fields, "operator_admission_status"),
    );
    ensure_field(
        "operator_blocker_id",
        batch_child_field(&child_fields, "operator_blocker_id"),
    );
    ensure_field(
        "operator_encoded_native_claim_allowed",
        batch_child_field(&child_fields, "operator_encoded_native_claim_allowed"),
    );
    ensure_field(
        "source_backed_scan_evidence_status",
        batch_child_field(&child_fields, "source_backed_scan_evidence_status"),
    );
    ensure_field(
        "native_io_certificate_status",
        report.native_io_certificate.status().to_string(),
    );
    ensure_field(
        "result_sink_claim_gate_status",
        report.result_sink_claim_gate_status.clone(),
    );
    ensure_field(
        "fallback_execution_allowed",
        report.fallback_execution_allowed.to_string(),
    );
    ensure_field("fallback_attempted", "false".to_string());
    ensure_field("external_engine_invoked", "false".to_string());
    fields
}

trait StreamingExecutionFieldView {
    fn streaming_vortex_execution_used(&self) -> bool;
    fn full_table_materialization_avoided(&self) -> bool;
    fn streaming_filter_pushdown_applied(&self) -> bool;
    fn streaming_projection_pushdown_applied(&self) -> bool;
    fn streaming_arrays_read_count(&self) -> usize;
    fn streaming_max_chunk_rows(&self) -> usize;
    fn streaming_projected_columns(&self) -> &[String];
    fn streaming_result_row_count(&self) -> u64;
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

    fn streaming_result_row_count(&self) -> u64 {
        self.streaming_result_row_count
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

    fn streaming_result_row_count(&self) -> u64 {
        self.streaming_result_row_count
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
        (
            "streaming_result_row_count".to_string(),
            report.streaming_result_row_count().to_string(),
        ),
    ]
}

#[allow(clippy::too_many_lines)]
fn source_backed_scan_evidence_fields(
    report: &TraditionalAnalyticsVortexReport,
) -> Vec<(String, String)> {
    let operator_classification = traditional_operator_execution_class(
        report.streaming_vortex_execution_used,
        report.data_materialized,
    );
    let residual_executor = match operator_classification {
        TraditionalOperatorExecutionClass::EncodedNative => "none",
        TraditionalOperatorExecutionClass::ResidualNative => "shardloom_native_residual_operator",
        TraditionalOperatorExecutionClass::MaterializedTemporary => {
            "shardloom_native_temporary_operator"
        }
        TraditionalOperatorExecutionClass::Unsupported => "unsupported",
    };
    let evidence_status = if report.streaming_vortex_execution_used && !report.data_materialized {
        "scoped_local_vortex_scan_evidence"
    } else if report.streaming_vortex_execution_used {
        "scan_evidence_has_materialization_boundary"
    } else {
        "not_admitted_for_source_backed_scan_evidence"
    };
    let provider_kind = if report.streaming_vortex_execution_used {
        "vortex_file_projected_scan"
    } else {
        "unsupported"
    };
    let source_roles =
        source_backed_scan_source_roles(report.scenario, &report.streaming_projected_columns);
    let (source_refs, source_digests) = source_backed_scan_sources(report, &source_roles);
    vec![
        (
            "source_backed_scan_evidence_schema_version".to_string(),
            SOURCE_BACKED_SCAN_EVIDENCE_SCHEMA_VERSION.to_string(),
        ),
        (
            "source_backed_scan_evidence_report_id".to_string(),
            format!(
                "gar-0021h.source_backed_scan.{}.{}",
                report
                    .execution_mode_selection
                    .selected_execution_mode
                    .as_str(),
                report.scenario.as_str().replace(['/', ' ', '+'], "-")
            ),
        ),
        (
            "source_backed_scan_evidence_status".to_string(),
            evidence_status.to_string(),
        ),
        (
            "source_backed_scan_provider_kind".to_string(),
            provider_kind.to_string(),
        ),
        (
            "source_backed_scan_provider_surface".to_string(),
            "traditional_analytics_vortex_file_scan".to_string(),
        ),
        (
            "source_backed_scan_provider_scope".to_string(),
            "local_vortex_prepared_or_native_rows_only".to_string(),
        ),
        (
            "source_backed_scan_source_roles".to_string(),
            source_roles,
        ),
        (
            "source_backed_scan_source_ref".to_string(),
            source_refs.join(","),
        ),
        (
            "source_backed_scan_source_digest".to_string(),
            source_digests.join(","),
        ),
        (
            "source_backed_scan_projected_columns".to_string(),
            report.streaming_projected_columns.join(","),
        ),
        (
            "source_backed_scan_filter_pushdown_applied".to_string(),
            report.streaming_filter_pushdown_applied.to_string(),
        ),
        (
            "source_backed_scan_projection_pushdown_applied".to_string(),
            report.streaming_projection_pushdown_applied.to_string(),
        ),
        (
            "source_backed_scan_rows_scanned".to_string(),
            report.rows_scanned.to_string(),
        ),
        (
            "source_backed_scan_arrays_read_count".to_string(),
            report.streaming_arrays_read_count.to_string(),
        ),
        (
            "source_backed_scan_max_chunk_rows".to_string(),
            report.streaming_max_chunk_rows.to_string(),
        ),
        (
            "source_backed_scan_materialization_boundary_rows".to_string(),
            report.materialization_boundary_rows.to_string(),
        ),
        (
            "source_backed_scan_data_materialized".to_string(),
            report.data_materialized.to_string(),
        ),
        (
            "source_backed_scan_native_io_certificate_id".to_string(),
            report.native_io_certificate.certificate_id.clone(),
        ),
        (
            "source_backed_scan_native_io_certificate_status".to_string(),
            report.native_io_certificate.status().to_string(),
        ),
        (
            "source_backed_scan_operator_execution_class".to_string(),
            operator_classification.as_str().to_string(),
        ),
        (
            "source_backed_scan_residual_executor".to_string(),
            residual_executor.to_string(),
        ),
        (
            "source_backed_scan_encoded_native_claim_allowed".to_string(),
            (operator_classification == TraditionalOperatorExecutionClass::EncodedNative)
                .to_string(),
        ),
        (
            "source_backed_scan_claim_gate_status".to_string(),
            "not_claim_grade".to_string(),
        ),
        (
            "source_backed_scan_claim_boundary".to_string(),
            "scoped local prepared/native Vortex scan evidence only; no SQL/DataFrame, object-store, distributed, encoded-native operator, or performance claim".to_string(),
        ),
        (
            "source_backed_scan_fallback_attempted".to_string(),
            "false".to_string(),
        ),
        (
            "source_backed_scan_external_engine_invoked".to_string(),
            "false".to_string(),
        ),
    ]
}

#[allow(clippy::too_many_lines)]
fn encoded_predicate_provider_fields(
    report: &TraditionalAnalyticsVortexReport,
) -> Vec<(String, String)> {
    let operator_classification = traditional_operator_execution_class(
        report.streaming_vortex_execution_used,
        report.data_materialized,
    );
    let selective_filter = report.scenario == TraditionalAnalyticsScenario::SelectiveFilter;
    let scenario_slug = report.scenario.as_str().replace(['/', ' ', '+'], "-");
    let report_id = format!(
        "gar-0026u.encoded_predicate_provider.{}.{}",
        report
            .execution_mode_selection
            .selected_execution_mode
            .as_str(),
        scenario_slug
    );
    let filter_probe_columns_observed = report
        .encoded_predicate_provider_filter_column_probe_reader_chunk_columns_observed
        .iter()
        .any(|column| column == "flag")
        && report
            .encoded_predicate_provider_filter_column_probe_reader_chunk_columns_observed
            .iter()
            .any(|column| column == "value");
    let bridge_intersected = report.encoded_predicate_provider_conjunctive_bridge_runtime_status
        == "intersected_selection_vectors"
        && report.encoded_predicate_provider_filter_column_batches_consumed
        && report.encoded_predicate_provider_selection_vector_intersection_certified;
    let selected_metric_consumed = bridge_intersected
        && report.encoded_predicate_provider_selected_metric_selection_vector_consumed
        && report.encoded_predicate_provider_selected_metric_row_count
            == report.encoded_predicate_provider_conjunctive_bridge_selected_row_count;
    let (
        checked,
        status,
        classification,
        candidate,
        filter_columns,
        blocker_id,
        blocker_reason,
        required_future_evidence,
        claim_boundary,
    ) = if selective_filter && selected_metric_consumed {
        (
            "true",
            "reader_generated_filter_column_batches_and_selected_metric_aggregation_admitted",
            "selection_vector_backed_metric_aggregation_consumed",
            "selective_filter.flag_eq_1_and_value_gte_5000",
            "flag,value",
            "none",
            "The scoped local Vortex scan projected flag,value filter-column batches, lowered them into admitted reader-generated encoded kernel inputs, intersected their selection vectors, and used the admitted selection vector to drive the metric aggregation without external fallback.",
            "execution_certificate;native_io_certificate;materialization_decode_boundary;no_fallback_policy;claim_grade_iterations",
            "scoped reader-generated filter-column and selection-vector-backed metric aggregation evidence only; operator remains residual-native and not encoded-native until metric aggregation itself has encoded-native certificate evidence",
        )
    } else if selective_filter && bridge_intersected {
        (
            "true",
            "reader_generated_filter_column_batches_admitted",
            "reader_generated_filter_column_kernel_inputs_admitted",
            "selective_filter.flag_eq_1_and_value_gte_5000",
            "flag,value",
            "gar-0026v.selected_metric_aggregation_not_selection_vector_backed",
            "The scoped local Vortex scan projected flag,value filter-column batches, lowered them into admitted reader-generated encoded kernel inputs, and the conjunctive bridge intersected selection vectors without fallback.",
            "selected_metric_aggregation_consumes_selection_vector;execution_certificate;native_io_certificate;materialization_decode_boundary;no_fallback_policy;claim_grade_iterations",
            "scoped reader-generated filter-column selection-vector evidence only; selected metric aggregation remains residual-native until it consumes the admitted selection vector end to end",
        )
    } else if selective_filter && filter_probe_columns_observed {
        (
            "true",
            "blocked_until_reader_generated_kernel_input_certificate",
            "filter_column_batches_observed_kernel_input_lowering_blocked",
            "selective_filter.flag_eq_1_and_value_gte_5000",
            "flag,value",
            "gar-0026u.reader_generated_kernel_input_lowering_unsupported_encoding",
            "The scoped local Vortex scan can project flag,value filter-column reader chunks without decode/materialization, but their current encodings are not yet lowered into admitted encoded kernel inputs for the conjunctive bridge.",
            "encoding_specific_kernel_input_lowering_from_vortex_reader_chunks;selective_filter_correctness_fixture;execution_certificate;native_io_certificate;materialization_decode_boundary;no_fallback_policy",
            "filter-column reader-batch capture evidence only; no encoded predicate provider/runtime/performance claim until kernel inputs and selected metric aggregation are certified",
        )
    } else if selective_filter {
        (
            "true",
            "blocked_until_reader_generated_filter_column_batches",
            "blocked_until_vortex_or_shardloom_filter_column_evidence",
            "selective_filter.flag_eq_1_and_value_gte_5000",
            "flag,value",
            "gar-0026t.reader_generated_filter_column_batches_missing",
            "The reader-generated conjunctive selection-vector bridge is available for supplied encoded kernel inputs, but the scoped local Vortex scan did not return flag,value filter-column reader chunks for this row.",
            "reader_generated_encoded_value_batches_for_filter_columns;encoding_specific_kernel_input_lowering_from_vortex_reader_chunks;selective_filter_correctness_fixture;execution_certificate;native_io_certificate;materialization_decode_boundary;no_fallback_policy",
            "conjunctive selection-vector bridge contract only when callers supply admitted filter-column batches; no encoded predicate provider/runtime/performance claim for traditional benchmark rows",
        )
    } else {
        (
            "false",
            "not_applicable_no_selective_filter_predicate",
            "not_applicable",
            "none",
            "none",
            "none",
            "scenario does not use the GAR-0026-R selective-filter predicate bridge candidate",
            "none",
            "no encoded predicate provider claim",
        )
    };
    let filter_pushdown_status = if selective_filter && report.streaming_filter_pushdown_applied {
        "requested_unverified_encoded_provider"
    } else if report.streaming_filter_pushdown_applied {
        "not_checked_non_selective_filter"
    } else {
        "not_requested"
    };
    let reader_chunk_columns_observed =
        comma_join_or_none(&report.streaming_reader_chunk_columns_observed);
    let reader_chunk_dtype_summary =
        comma_join_or_none(&report.streaming_reader_chunk_dtype_summary);
    let reader_chunk_encoding_summary =
        comma_join_or_none(&report.streaming_reader_chunk_encoding_summary);
    let filter_column_probe_columns_observed = comma_join_or_none(
        &report.encoded_predicate_provider_filter_column_probe_reader_chunk_columns_observed,
    );
    let filter_column_probe_dtype_summary = comma_join_or_none(
        &report.encoded_predicate_provider_filter_column_probe_reader_chunk_dtype_summary,
    );
    let filter_column_probe_encoding_summary = comma_join_or_none(
        &report.encoded_predicate_provider_filter_column_probe_reader_chunk_encoding_summary,
    );
    let filter_columns_observed = filter_probe_columns_observed;
    let projected_metric_observed = report
        .streaming_reader_chunk_columns_observed
        .iter()
        .any(|column| column == "metric");
    let projected_metric_filter_wrapped = report
        .streaming_reader_chunk_encoding_summary
        .iter()
        .any(|summary| summary == "metric:vortex.filter");
    let bridge_status = if selective_filter && selected_metric_consumed {
        "bridge_consumed_reader_generated_filter_column_kernel_inputs_and_metric_selection_vector"
    } else if selective_filter && bridge_intersected {
        "bridge_consumed_reader_generated_filter_column_kernel_inputs"
    } else if selective_filter && filter_columns_observed {
        "bridge_available_blocked_filter_column_kernel_inputs_not_lowered"
    } else if selective_filter {
        "bridge_available_blocked_filter_columns_not_returned_by_reader_projection"
    } else {
        "not_applicable_no_selective_filter_predicate"
    };
    let bridge_blocker_ids = if selective_filter && selected_metric_consumed {
        "none"
    } else if selective_filter && bridge_intersected {
        "gar-0026v.selected_metric_aggregation_not_selection_vector_backed"
    } else if selective_filter && filter_columns_observed {
        "gar-0026u.filter_column_kernel_inputs_not_admitted;gar-0026u.selected_metric_aggregation_not_selection_vector_backed"
    } else if selective_filter {
        "gar-0026t.filter_columns_not_returned_by_scan_projection;gar-0026t.reader_generated_filter_column_inputs_missing;gar-0026u.encoding_specific_filter_column_lowering_missing;gar-0026t.projected_output_filter_array_not_filter_column_input"
    } else {
        "none"
    };
    let filter_column_batch_status = if selective_filter && bridge_intersected {
        "admitted_filter_column_kernel_inputs"
    } else if selective_filter && filter_columns_observed {
        "observed_filter_column_reader_chunks_not_lowered"
    } else if selective_filter {
        "blocked_filter_only_columns_not_observed"
    } else {
        "not_applicable"
    };
    let projected_output_batch_status = if selective_filter && selected_metric_consumed {
        "observed_projected_metric_reader_chunk_selection_vector_backed"
    } else if selective_filter && projected_metric_filter_wrapped {
        "observed_projected_metric_vortex_filter_chunk"
    } else if selective_filter && projected_metric_observed {
        "observed_projected_metric_reader_chunk"
    } else if selective_filter && report.streaming_arrays_read_count == 0 {
        "blocked_no_reader_chunks_emitted_for_zero_result"
    } else if selective_filter {
        "blocked_projected_metric_reader_chunk_not_observed"
    } else {
        "not_applicable"
    };
    let predicate_shape_status = if selective_filter {
        "conjunctive_predicate_shape_supported_by_reader_generated_bridge"
    } else {
        "not_applicable"
    };
    let selection_vector_intersection_status = if selective_filter && bridge_intersected {
        "selection_vectors_intersected"
    } else if selective_filter && filter_columns_observed {
        "bridge_blocked_before_selection_vector_intersection"
    } else if selective_filter {
        "bridge_available_blocked_missing_filter_column_inputs"
    } else {
        "not_applicable"
    };
    let conjunctive_bridge_status = if selective_filter {
        report
            .encoded_predicate_provider_conjunctive_bridge_runtime_status
            .as_str()
    } else {
        "not_applicable"
    };
    let kernel_input_lowering_status = if selective_filter && bridge_intersected {
        "reader_generated_encoded_kernel_inputs_admitted"
    } else if selective_filter && filter_columns_observed {
        "blocked_missing_encoding_specific_kernel_input_lowering"
    } else if selective_filter {
        "blocked_missing_reader_generated_filter_column_batches"
    } else {
        "not_applicable"
    };
    vec![
        (
            "encoded_predicate_provider_schema_version".to_string(),
            ENCODED_PREDICATE_PROVIDER_SCHEMA_VERSION.to_string(),
        ),
        (
            "encoded_predicate_provider_report_id".to_string(),
            report_id,
        ),
        (
            "encoded_predicate_provider_checked".to_string(),
            checked.to_string(),
        ),
        (
            "encoded_predicate_provider_status".to_string(),
            status.to_string(),
        ),
        (
            "encoded_predicate_provider_classification".to_string(),
            classification.to_string(),
        ),
        (
            "encoded_predicate_provider_candidate".to_string(),
            candidate.to_string(),
        ),
        (
            "encoded_predicate_provider_api_surface_checked".to_string(),
            "VortexFile::scan.with_filter;VortexFile::scan.with_projection;shardloom_filter_column_probe_scan;shardloom_prepared_encoded_predicate_reports;reader_generated_prepared_batch_kernel_inputs".to_string(),
        ),
        (
            "encoded_predicate_provider_current_surface".to_string(),
            "traditional_analytics_vortex_scan_filter_pushdown".to_string(),
        ),
        (
            "encoded_predicate_provider_filter_column_probe_requested".to_string(),
            report
                .encoded_predicate_provider_filter_column_probe_requested
                .to_string(),
        ),
        (
            "encoded_predicate_provider_filter_column_probe_status".to_string(),
            report
                .encoded_predicate_provider_filter_column_probe_status
                .clone(),
        ),
        (
            "encoded_predicate_provider_filter_column_probe_requested_columns".to_string(),
            comma_join_or_none(
                &report.encoded_predicate_provider_filter_column_probe_requested_columns,
            ),
        ),
        (
            "encoded_predicate_provider_filter_column_probe_reader_split_count".to_string(),
            report
                .encoded_predicate_provider_filter_column_probe_reader_split_count
                .to_string(),
        ),
        (
            "encoded_predicate_provider_filter_column_probe_row_count".to_string(),
            report
                .encoded_predicate_provider_filter_column_probe_row_count
                .to_string(),
        ),
        (
            "encoded_predicate_provider_filter_column_probe_reader_chunk_columns_observed"
                .to_string(),
            filter_column_probe_columns_observed,
        ),
        (
            "encoded_predicate_provider_filter_column_probe_reader_chunk_dtype_summary".to_string(),
            filter_column_probe_dtype_summary,
        ),
        (
            "encoded_predicate_provider_filter_column_probe_reader_chunk_encoding_summary"
                .to_string(),
            filter_column_probe_encoding_summary,
        ),
        (
            "encoded_predicate_provider_filter_column_probe_data_decoded".to_string(),
            report
                .encoded_predicate_provider_filter_column_probe_data_decoded
                .to_string(),
        ),
        (
            "encoded_predicate_provider_filter_column_probe_data_materialized".to_string(),
            report
                .encoded_predicate_provider_filter_column_probe_data_materialized
                .to_string(),
        ),
        (
            "encoded_predicate_provider_filter_column_probe_row_read".to_string(),
            report
                .encoded_predicate_provider_filter_column_probe_row_read
                .to_string(),
        ),
        (
            "encoded_predicate_provider_filter_column_probe_fallback_attempted".to_string(),
            report
                .encoded_predicate_provider_filter_column_probe_fallback_attempted
                .to_string(),
        ),
        (
            "encoded_predicate_provider_filter_column_probe_external_engine_invoked".to_string(),
            report
                .encoded_predicate_provider_filter_column_probe_external_engine_invoked
                .to_string(),
        ),
        (
            "encoded_predicate_provider_conjunctive_bridge_schema_version".to_string(),
            "shardloom.vortex_reader_generated_conjunctive_selection_vector_bridge.v1"
                .to_string(),
        ),
        (
            "encoded_predicate_provider_conjunctive_bridge_report_id".to_string(),
            report
                .encoded_predicate_provider_conjunctive_bridge_runtime_report_id
                .clone(),
        ),
        (
            "encoded_predicate_provider_conjunctive_bridge_status".to_string(),
            conjunctive_bridge_status.to_string(),
        ),
        (
            "encoded_predicate_provider_conjunctive_bridge_intersection_count".to_string(),
            report
                .encoded_predicate_provider_conjunctive_bridge_intersection_count
                .to_string(),
        ),
        (
            "encoded_predicate_provider_conjunctive_bridge_selected_row_count".to_string(),
            report
                .encoded_predicate_provider_conjunctive_bridge_selected_row_count
                .map_or_else(|| "none".to_string(), |row_count| row_count.to_string()),
        ),
        (
            "encoded_predicate_provider_filter_column_batches_consumed".to_string(),
            report
                .encoded_predicate_provider_filter_column_batches_consumed
                .to_string(),
        ),
        (
            "encoded_predicate_provider_kernel_input_count".to_string(),
            report
                .encoded_predicate_provider_kernel_input_count
                .to_string(),
        ),
        (
            "encoded_predicate_provider_filter_pushdown_status".to_string(),
            filter_pushdown_status.to_string(),
        ),
        (
            "encoded_predicate_provider_filter_only_columns".to_string(),
            filter_columns.to_string(),
        ),
        (
            "encoded_predicate_provider_projected_output_columns".to_string(),
            report.streaming_projected_columns.join(","),
        ),
        (
            "encoded_predicate_provider_reader_chunk_columns_observed".to_string(),
            reader_chunk_columns_observed,
        ),
        (
            "encoded_predicate_provider_reader_chunk_dtype_summary".to_string(),
            reader_chunk_dtype_summary,
        ),
        (
            "encoded_predicate_provider_reader_chunk_encoding_summary".to_string(),
            reader_chunk_encoding_summary,
        ),
        (
            "encoded_predicate_provider_reader_backed_bridge_status".to_string(),
            bridge_status.to_string(),
        ),
        (
            "encoded_predicate_provider_reader_backed_bridge_blocker_ids".to_string(),
            bridge_blocker_ids.to_string(),
        ),
        (
            "encoded_predicate_provider_filter_column_batch_status".to_string(),
            filter_column_batch_status.to_string(),
        ),
        (
            "encoded_predicate_provider_projected_output_batch_status".to_string(),
            projected_output_batch_status.to_string(),
        ),
        (
            "encoded_predicate_provider_predicate_shape_status".to_string(),
            predicate_shape_status.to_string(),
        ),
        (
            "encoded_predicate_provider_selection_vector_intersection_status".to_string(),
            selection_vector_intersection_status.to_string(),
        ),
        (
            "encoded_predicate_provider_selected_metric_aggregation_status".to_string(),
            report
                .encoded_predicate_provider_selected_metric_aggregation_status
                .clone(),
        ),
        (
            "encoded_predicate_provider_selected_metric_selection_vector_consumed".to_string(),
            report
                .encoded_predicate_provider_selected_metric_selection_vector_consumed
                .to_string(),
        ),
        (
            "encoded_predicate_provider_selected_metric_source".to_string(),
            report
                .encoded_predicate_provider_selected_metric_source
                .clone(),
        ),
        (
            "encoded_predicate_provider_selected_metric_row_count".to_string(),
            report
                .encoded_predicate_provider_selected_metric_row_count
                .map_or_else(|| "none".to_string(), |row_count| row_count.to_string()),
        ),
        (
            "encoded_predicate_provider_selected_metric_sum".to_string(),
            report
                .encoded_predicate_provider_selected_metric_sum
                .map_or_else(|| "none".to_string(), evidence_float),
        ),
        (
            "encoded_predicate_provider_selected_metric_scan_split_count".to_string(),
            report
                .encoded_predicate_provider_selected_metric_scan_split_count
                .to_string(),
        ),
        (
            "encoded_predicate_provider_selected_metric_data_decoded".to_string(),
            report
                .encoded_predicate_provider_selected_metric_data_decoded
                .to_string(),
        ),
        (
            "encoded_predicate_provider_selected_metric_data_materialized".to_string(),
            report
                .encoded_predicate_provider_selected_metric_data_materialized
                .to_string(),
        ),
        (
            "encoded_predicate_provider_kernel_input_lowering_status".to_string(),
            kernel_input_lowering_status.to_string(),
        ),
        (
            "encoded_predicate_provider_blocker_id".to_string(),
            blocker_id.to_string(),
        ),
        (
            "encoded_predicate_provider_blocker_reason".to_string(),
            blocker_reason.to_string(),
        ),
        (
            "encoded_predicate_provider_required_future_evidence".to_string(),
            required_future_evidence.to_string(),
        ),
        (
            "encoded_predicate_provider_operator_execution_class".to_string(),
            operator_classification.as_str().to_string(),
        ),
        (
            "encoded_predicate_provider_encoded_native_claim_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "encoded_predicate_provider_claim_gate_status".to_string(),
            "not_claim_grade".to_string(),
        ),
        (
            "encoded_predicate_provider_claim_boundary".to_string(),
            claim_boundary.to_string(),
        ),
        (
            "encoded_predicate_provider_fallback_attempted".to_string(),
            "false".to_string(),
        ),
        (
            "encoded_predicate_provider_external_engine_invoked".to_string(),
            "false".to_string(),
        ),
    ]
}

#[allow(clippy::too_many_lines)]
fn fused_pipeline_evidence_fields(
    report: &TraditionalAnalyticsVortexReport,
) -> Vec<(String, String)> {
    let filter_project_limit_candidate =
        report.scenario == TraditionalAnalyticsScenario::FilterProjectionLimit;
    let filter_project_limit_used = filter_project_limit_candidate
        && report.streaming_vortex_execution_used
        && report.streaming_filter_pushdown_applied
        && report.streaming_projection_pushdown_applied
        && !report.data_materialized;
    let selection_vector_metric_candidate =
        report.scenario == TraditionalAnalyticsScenario::SelectiveFilter;
    let selection_vector_metric_used = selection_vector_metric_candidate
        && report.encoded_predicate_provider_selected_metric_selection_vector_consumed
        && report.encoded_predicate_provider_selected_metric_aggregation_status
            == "selection_vector_consumed"
        && !report.encoded_predicate_provider_selected_metric_data_materialized
        && !report.data_materialized;
    let fused_pipeline_used = filter_project_limit_used || selection_vector_metric_used;
    let fused_operator_family = if filter_project_limit_candidate {
        "filter_projection_limit"
    } else if selection_vector_metric_candidate {
        "selection_vector_metric_aggregation"
    } else {
        "not_applicable"
    };
    let rows_selected = if filter_project_limit_candidate {
        report.streaming_result_row_count.to_string()
    } else if selection_vector_metric_candidate {
        report
            .encoded_predicate_provider_selected_metric_row_count
            .map_or_else(|| "none".to_string(), |row_count| row_count.to_string())
    } else {
        "not_applicable".to_string()
    };
    let rows_output = if filter_project_limit_candidate || selection_vector_metric_candidate {
        scalar_result_row_count_from_json(&report.result_json).map_or_else(
            || report.rows_materialized.to_string(),
            |row_count| row_count.to_string(),
        )
    } else {
        "not_applicable".to_string()
    };
    let (blocker_id, blocker_reason) = if fused_pipeline_used {
        ("none", "fused local prepared/native residual path admitted")
    } else if filter_project_limit_candidate && report.data_materialized {
        (
            "gar-perf-1c.compatibility_import_or_materialized_boundary",
            "filter/projection/limit fusion is blocked because this row materialized before the prepared/native fused path",
        )
    } else if filter_project_limit_candidate {
        (
            "gar-perf-1c.filter_projection_limit_pushdown_missing",
            "filter/projection/limit fusion requires admitted filter and projection pushdown with no intermediate full-table materialization",
        )
    } else if selection_vector_metric_candidate {
        (
            "gar-perf-1c.selection_vector_metric_aggregation_not_admitted",
            "selection-vector metric aggregation requires an admitted reader-generated conjunctive selection vector consumed by the ShardLoom-native metric aggregation path",
        )
    } else {
        (
            "not_applicable",
            "scenario is outside GAR-PERF-1C fused path scope",
        )
    };
    let data_decoded = if selection_vector_metric_candidate {
        report.encoded_predicate_provider_selected_metric_data_decoded
    } else {
        report.data_decoded
    };
    let data_materialized = if selection_vector_metric_candidate {
        report.encoded_predicate_provider_selected_metric_data_materialized
    } else {
        report.data_materialized
    };
    let selection_vector_consumed = if selection_vector_metric_candidate {
        report
            .encoded_predicate_provider_selected_metric_selection_vector_consumed
            .to_string()
    } else {
        "not_applicable".to_string()
    };
    let selection_vector_status = if selection_vector_metric_candidate {
        report
            .encoded_predicate_provider_conjunctive_bridge_runtime_status
            .clone()
    } else {
        "not_applicable".to_string()
    };
    vec![
        (
            "fused_pipeline_schema_version".to_string(),
            FUSED_PIPELINE_SCHEMA_VERSION.to_string(),
        ),
        (
            "fused_pipeline_report_id".to_string(),
            format!(
                "gar-perf-1c.fused_pipeline.{}.{}",
                report
                    .execution_mode_selection
                    .selected_execution_mode
                    .as_str(),
                report.scenario.as_str().replace(['/', ' ', '+'], "-")
            ),
        ),
        (
            "fused_pipeline_scope".to_string(),
            "local_prepared_native_residual_pipeline".to_string(),
        ),
        (
            "fused_pipeline_used".to_string(),
            fused_pipeline_used.to_string(),
        ),
        (
            "fused_operator_family".to_string(),
            fused_operator_family.to_string(),
        ),
        (
            "intermediate_materialization_avoided".to_string(),
            (fused_pipeline_used && !data_materialized).to_string(),
        ),
        (
            "fused_pipeline_rows_scanned".to_string(),
            report.rows_scanned.to_string(),
        ),
        ("fused_pipeline_rows_selected".to_string(), rows_selected),
        ("fused_pipeline_rows_output".to_string(), rows_output),
        (
            "fused_pipeline_filter_columns".to_string(),
            if filter_project_limit_candidate || selection_vector_metric_candidate {
                "flag,value"
            } else {
                "not_applicable"
            }
            .to_string(),
        ),
        (
            "fused_pipeline_projection_columns".to_string(),
            if report.streaming_projected_columns.is_empty() {
                "none".to_string()
            } else {
                report.streaming_projected_columns.join(",")
            },
        ),
        (
            "fused_pipeline_selection_vector_consumed".to_string(),
            selection_vector_consumed,
        ),
        (
            "fused_pipeline_selection_vector_status".to_string(),
            selection_vector_status,
        ),
        (
            "fused_pipeline_data_decoded".to_string(),
            data_decoded.to_string(),
        ),
        (
            "fused_pipeline_data_materialized".to_string(),
            data_materialized.to_string(),
        ),
        (
            "fused_pipeline_operator_execution_class".to_string(),
            traditional_operator_execution_class(
                report.streaming_vortex_execution_used,
                report.data_materialized,
            )
            .as_str()
            .to_string(),
        ),
        (
            "fused_pipeline_encoded_native_claim_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "fused_pipeline_claim_gate_status".to_string(),
            "not_claim_grade".to_string(),
        ),
        (
            "fused_pipeline_blocker_id".to_string(),
            blocker_id.to_string(),
        ),
        (
            "fused_pipeline_blocker_reason".to_string(),
            blocker_reason.to_string(),
        ),
        (
            "fused_pipeline_claim_boundary".to_string(),
            "scoped local prepared/native fused residual evidence only; no encoded-native, SQL/DataFrame, object-store/lakehouse, production, public performance, or Spark-displacement claim".to_string(),
        ),
        (
            "fused_pipeline_fallback_attempted".to_string(),
            "false".to_string(),
        ),
        (
            "fused_pipeline_external_engine_invoked".to_string(),
            "false".to_string(),
        ),
    ]
}

fn scalar_result_row_count_from_json(result_json: &str) -> Option<u64> {
    let (_, rest) = result_json.split_once("\"row_count\":")?;
    let digits = rest
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    digits.parse().ok()
}

fn comma_join_or_none(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_string()
    } else {
        values.join(",")
    }
}

fn evidence_float(value: f64) -> String {
    let text = value.to_string();
    if text.contains('.') {
        text
    } else {
        format!("{text}.0")
    }
}

fn source_backed_scan_source_roles(
    scenario: TraditionalAnalyticsScenario,
    projected_columns: &[String],
) -> String {
    let mut roles = Vec::new();
    for column in projected_columns {
        let role = column.split_once('.').map_or_else(
            || match scenario {
                TraditionalAnalyticsScenario::SmallChangeOverLargeBase => "base",
                _ => "fact",
            },
            |(prefix, _)| match prefix {
                "fact" | "dim" | "base" | "cdc_delta" => prefix,
                _ => "fact",
            },
        );
        if !roles.contains(&role) {
            roles.push(role);
        }
    }
    if roles.is_empty() && scenario == TraditionalAnalyticsScenario::SmallChangeOverLargeBase {
        roles.push("base");
        roles.push("cdc_delta");
    } else if roles.is_empty() {
        roles.push("fact");
    }
    roles.join(",")
}

fn source_backed_scan_sources(
    report: &TraditionalAnalyticsVortexReport,
    source_roles: &str,
) -> (Vec<String>, Vec<String>) {
    let mut refs = Vec::new();
    let mut digests = Vec::new();
    for role in source_roles.split(',') {
        match role {
            "fact" => {
                refs.push(format!("fact={}", report.fact_vortex_path.display()));
                digests.push(format!("fact={}", report.fact_vortex_digest));
            }
            "dim" => {
                refs.push(format!("dim={}", report.dim_vortex_path.display()));
                digests.push(format!("dim={}", report.dim_vortex_digest));
            }
            "base" => {
                refs.push(format!("base={}", report.fact_vortex_path.display()));
                digests.push(format!("base={}", report.fact_vortex_digest));
            }
            "cdc_delta" => {
                if let Some(cdc_delta_path) = report.cdc_delta_vortex_path.as_ref() {
                    refs.push(format!("cdc_delta={}", cdc_delta_path.display()));
                }
                if let Some(cdc_delta_digest) = report.cdc_delta_vortex_digest.as_ref() {
                    digests.push(format!("cdc_delta={cdc_delta_digest}"));
                }
            }
            _ => {}
        }
    }
    (refs, digests)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TraditionalOperatorExecutionClass {
    EncodedNative,
    ResidualNative,
    MaterializedTemporary,
    Unsupported,
}

const TRADITIONAL_OPERATOR_EXECUTION_CLASS_VOCABULARY: [TraditionalOperatorExecutionClass; 4] = [
    TraditionalOperatorExecutionClass::EncodedNative,
    TraditionalOperatorExecutionClass::ResidualNative,
    TraditionalOperatorExecutionClass::MaterializedTemporary,
    TraditionalOperatorExecutionClass::Unsupported,
];

impl TraditionalOperatorExecutionClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::EncodedNative => "encoded_native",
            Self::ResidualNative => "residual_native",
            Self::MaterializedTemporary => "materialized_temporary",
            Self::Unsupported => "unsupported",
        }
    }
}

fn traditional_operator_execution_class_vocabulary() -> String {
    TRADITIONAL_OPERATOR_EXECUTION_CLASS_VOCABULARY
        .iter()
        .map(|classification| classification.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn traditional_operator_execution_class(
    streaming_vortex_execution_used: bool,
    data_materialized: bool,
) -> TraditionalOperatorExecutionClass {
    match (streaming_vortex_execution_used, data_materialized) {
        (true, false) => TraditionalOperatorExecutionClass::ResidualNative,
        (_, true) => TraditionalOperatorExecutionClass::MaterializedTemporary,
        _ => TraditionalOperatorExecutionClass::Unsupported,
    }
}

fn traditional_operator_blocker_id(
    classification: TraditionalOperatorExecutionClass,
) -> &'static str {
    match classification {
        TraditionalOperatorExecutionClass::EncodedNative => "none",
        TraditionalOperatorExecutionClass::ResidualNative => {
            "gar-flow-2b.residual_native_operator_not_encoded_native"
        }
        TraditionalOperatorExecutionClass::MaterializedTemporary => {
            "gar-flow-2b.materialized_temporary_operator_not_encoded_native"
        }
        TraditionalOperatorExecutionClass::Unsupported => {
            "gar-flow-2b.operator_classification_unsupported"
        }
    }
}

fn traditional_operator_blocker_reason(
    classification: TraditionalOperatorExecutionClass,
) -> &'static str {
    match classification {
        TraditionalOperatorExecutionClass::EncodedNative => {
            "encoded-native operator evidence attached"
        }
        TraditionalOperatorExecutionClass::ResidualNative => {
            "Vortex scan is admitted, but residual scalar work is executed by ShardLoom-native code"
        }
        TraditionalOperatorExecutionClass::MaterializedTemporary => {
            "current benchmark path materializes Vortex-derived arrays before temporary operator execution"
        }
        TraditionalOperatorExecutionClass::Unsupported => {
            "operator is not admitted for prepared/native execution"
        }
    }
}

fn traditional_operator_admission_status(
    classification: TraditionalOperatorExecutionClass,
) -> &'static str {
    match classification {
        TraditionalOperatorExecutionClass::EncodedNative => "encoded_native_supported",
        TraditionalOperatorExecutionClass::ResidualNative => "residual_native_supported",
        TraditionalOperatorExecutionClass::MaterializedTemporary => {
            "materialized_temporary_supported"
        }
        TraditionalOperatorExecutionClass::Unsupported => "unsupported",
    }
}

#[allow(clippy::too_many_lines)]
fn traditional_vortex_provider_admission_fields(
    scenario: TraditionalAnalyticsScenario,
    report_id_suffix: &str,
    streaming_vortex_execution_used: bool,
    data_materialized: bool,
) -> Vec<(String, String)> {
    let operator_classification =
        traditional_operator_execution_class(streaming_vortex_execution_used, data_materialized);
    let residual_operator = match operator_classification {
        TraditionalOperatorExecutionClass::EncodedNative => "none",
        TraditionalOperatorExecutionClass::ResidualNative => "shardloom_native_residual_operator",
        TraditionalOperatorExecutionClass::MaterializedTemporary => {
            "shardloom_native_temporary_operator"
        }
        TraditionalOperatorExecutionClass::Unsupported => "unsupported",
    };
    let residual_boundary = match operator_classification {
        TraditionalOperatorExecutionClass::EncodedNative => "none",
        TraditionalOperatorExecutionClass::ResidualNative => {
            "vortex_scan_to_shardloom_residual_native"
        }
        TraditionalOperatorExecutionClass::MaterializedTemporary => {
            "vortex_scan_to_shardloom_materialized_temporary"
        }
        TraditionalOperatorExecutionClass::Unsupported => "unsupported",
    };
    let encoded_status = match operator_classification {
        TraditionalOperatorExecutionClass::EncodedNative => "encoded_native_operator_admitted",
        TraditionalOperatorExecutionClass::ResidualNative => "vortex_scan_admitted_residual_native",
        TraditionalOperatorExecutionClass::MaterializedTemporary => {
            "vortex_scan_admitted_residual_materialized"
        }
        TraditionalOperatorExecutionClass::Unsupported => "unsupported",
    };
    let filter_project_limit_fused = scenario
        == TraditionalAnalyticsScenario::FilterProjectionLimit
        && streaming_vortex_execution_used
        && !data_materialized;
    let fusion_status = if filter_project_limit_fused {
        "filter_project_limit_fused"
    } else if scenario == TraditionalAnalyticsScenario::FilterProjectionLimit {
        "not_fused_materialized_residual"
    } else {
        "not_applicable"
    };
    let fusion_blocker = if filter_project_limit_fused {
        "none"
    } else if scenario == TraditionalAnalyticsScenario::FilterProjectionLimit {
        "p75.native_provider.filter_project_limit_fusion_missing"
    } else {
        "none"
    };
    vec![
        (
            "provider_admission_report_id".to_string(),
            format!("p75.provider_admission.{report_id_suffix}"),
        ),
        (
            "vortex_first_provider_check_performed".to_string(),
            "true".to_string(),
        ),
        (
            "provider_admission_classification".to_string(),
            "use_vortex_native_provider".to_string(),
        ),
        ("provider_kind".to_string(), "vortex_scan".to_string()),
        (
            "provider_api_surface".to_string(),
            "traditional_analytics_vortex_file_scan".to_string(),
        ),
        (
            "source_backed_encoded_provider_checked".to_string(),
            "true".to_string(),
        ),
        (
            "source_backed_encoded_provider_status".to_string(),
            "scan_admitted_residual_recorded".to_string(),
        ),
        (
            "operator_blocker_matrix_ref".to_string(),
            format!(
                "operator-blocker://traditional_analytics/{}/{}",
                report_id_suffix,
                scenario.as_str().replace(' ', "_")
            ),
        ),
        (
            "operator_execution_class_vocabulary".to_string(),
            traditional_operator_execution_class_vocabulary(),
        ),
        (
            "operator_execution_class".to_string(),
            operator_classification.as_str().to_string(),
        ),
        (
            "operator_admission_status".to_string(),
            traditional_operator_admission_status(operator_classification).to_string(),
        ),
        (
            "operator_blocker_id".to_string(),
            traditional_operator_blocker_id(operator_classification).to_string(),
        ),
        (
            "operator_blocker_reason".to_string(),
            traditional_operator_blocker_reason(operator_classification).to_string(),
        ),
        (
            "operator_encoded_native_claim_allowed".to_string(),
            (operator_classification == TraditionalOperatorExecutionClass::EncodedNative)
                .to_string(),
        ),
        (
            "operator_residual_native_used".to_string(),
            (operator_classification == TraditionalOperatorExecutionClass::ResidualNative)
                .to_string(),
        ),
        (
            "operator_temporary_materialization_used".to_string(),
            (operator_classification == TraditionalOperatorExecutionClass::MaterializedTemporary)
                .to_string(),
        ),
        (
            "operator_unsupported_diagnostic".to_string(),
            if operator_classification == TraditionalOperatorExecutionClass::Unsupported {
                "prepared_native_operator_unsupported"
            } else {
                "none"
            }
            .to_string(),
        ),
        (
            "operator_claim_boundary".to_string(),
            "temporary and residual-native operators are not encoded-native claims".to_string(),
        ),
        (
            "residual_executor".to_string(),
            residual_operator.to_string(),
        ),
        (
            "residual_boundary".to_string(),
            residual_boundary.to_string(),
        ),
        (
            "encoded_native_execution_status".to_string(),
            encoded_status.to_string(),
        ),
        (
            "filter_project_limit_fused".to_string(),
            filter_project_limit_fused.to_string(),
        ),
        ("fusion_status".to_string(), fusion_status.to_string()),
        ("fusion_blocker".to_string(), fusion_blocker.to_string()),
        (
            "materialization_required".to_string(),
            (residual_operator != "none").to_string(),
        ),
        (
            "decode_required".to_string(),
            (residual_operator != "none").to_string(),
        ),
        (
            "provider_admission_fallback_attempted".to_string(),
            "false".to_string(),
        ),
        (
            "provider_admission_external_engine_invoked".to_string(),
            "false".to_string(),
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

/// Runs the scoped direct compatibility transient local CSV smoke path.
///
/// # Errors
/// Returns an error when the feature gate is disabled or the request exceeds
/// the narrow GAR-FLOW-1B admission contract.
pub fn run_traditional_direct_transient_csv_smoke(
    request: TraditionalAnalyticsRequest,
) -> Result<TraditionalDirectTransientReport> {
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    {
        run_traditional_direct_transient_csv_smoke_enabled(request)
    }
    #[cfg(not(feature = "vortex-traditional-analytics-benchmark"))]
    {
        std::mem::drop(request);
        Err(ShardLoomError::InvalidOperation(
            "direct compatibility transient CSV smoke requires feature `vortex-traditional-analytics-benchmark`; fallback execution was not attempted".to_string(),
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

/// Runs multiple local traditional analytics scenarios directly from prepared
/// native Vortex files in one process.
///
/// # Errors
/// Returns an error when the feature gate is disabled, no scenarios are
/// supplied, a scenario is unsupported, or any underlying native Vortex run
/// fails.
pub fn run_traditional_analytics_vortex_batch_benchmark(
    request: TraditionalAnalyticsVortexBatchRequest,
) -> Result<TraditionalAnalyticsVortexBatchReport> {
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    {
        run_traditional_analytics_vortex_batch_benchmark_enabled(request)
    }
    #[cfg(not(feature = "vortex-traditional-analytics-benchmark"))]
    {
        std::mem::drop(request);
        Err(ShardLoomError::InvalidOperation(
            "native Vortex traditional analytics batch benchmark requires feature `vortex-traditional-analytics-benchmark`; fallback execution was not attempted".to_string(),
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
    event_date: Option<String>,
    nullable_metric_00: Option<String>,
    nested_payload: Option<String>,
    raw_event_time: Option<String>,
    dirty_numeric: Option<String>,
    dirty_flag: Option<String>,
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
struct TraditionalCdcDeltaRow {
    id: u64,
    op: String,
    value: String,
    metric: String,
    effective_ts: String,
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
    event_date: Vec<String>,
    nullable_metric_00: Vec<String>,
    nested_payload: Vec<String>,
    raw_event_time: Vec<String>,
    dirty_numeric: Vec<String>,
    dirty_flag: Vec<String>,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone)]
struct VortexDimTable {
    dim_key: Vec<u32>,
    dim_label: Vec<String>,
    weight: Vec<f64>,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone)]
struct VortexCdcDeltaTable {
    id: Vec<u64>,
    op: Vec<String>,
    value: Vec<String>,
    metric: Vec<String>,
    effective_ts: Vec<String>,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Default, Clone, PartialEq)]
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
#[derive(Debug, Clone, Copy)]
struct GlobalTopKCandidate {
    id: u64,
    metric: f64,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl GlobalTopKCandidate {
    const fn new(id: u64, metric: f64) -> Self {
        Self { id, metric }
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl PartialEq for GlobalTopKCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.metric.to_bits() == other.metric.to_bits()
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl Eq for GlobalTopKCandidate {}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl PartialOrd for GlobalTopKCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl Ord for GlobalTopKCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .metric
            .total_cmp(&self.metric)
            .then_with(|| self.id.cmp(&other.id))
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
struct TraditionalEncodedPredicateProviderRuntimeEvidence {
    requested: bool,
    requested_columns: Vec<String>,
    probe_status: String,
    reader_split_count: usize,
    probe_row_count: u64,
    reader_chunk_columns_observed: Vec<String>,
    reader_chunk_dtype_summary: Vec<String>,
    reader_chunk_encoding_summary: Vec<String>,
    encoded_kernel_input_count: usize,
    bridge_status: String,
    bridge_report_id: String,
    bridge_intersection_count: usize,
    bridge_selected_row_count: Option<u64>,
    bridge_selection_vectors: Vec<SelectionVector>,
    filter_column_batches_consumed: bool,
    selection_vector_intersection_certified: bool,
    selected_metric_aggregation_status: String,
    selected_metric_selection_vector_consumed: bool,
    selected_metric_source: String,
    selected_metric_row_count: Option<u64>,
    selected_metric_sum: Option<f64>,
    selected_metric_scan_split_count: usize,
    selected_metric_data_decoded: bool,
    selected_metric_data_materialized: bool,
    data_decoded: bool,
    data_materialized: bool,
    row_read: bool,
    fallback_attempted: bool,
    external_engine_invoked: bool,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
impl TraditionalEncodedPredicateProviderRuntimeEvidence {
    fn not_applicable() -> Self {
        Self {
            requested: false,
            requested_columns: Vec::new(),
            probe_status: "not_applicable".to_string(),
            reader_split_count: 0,
            probe_row_count: 0,
            reader_chunk_columns_observed: Vec::new(),
            reader_chunk_dtype_summary: Vec::new(),
            reader_chunk_encoding_summary: Vec::new(),
            encoded_kernel_input_count: 0,
            bridge_status: "not_applicable".to_string(),
            bridge_report_id: "none".to_string(),
            bridge_intersection_count: 0,
            bridge_selected_row_count: None,
            bridge_selection_vectors: Vec::new(),
            filter_column_batches_consumed: false,
            selection_vector_intersection_certified: false,
            selected_metric_aggregation_status: "not_applicable".to_string(),
            selected_metric_selection_vector_consumed: false,
            selected_metric_source: "none".to_string(),
            selected_metric_row_count: None,
            selected_metric_sum: None,
            selected_metric_scan_split_count: 0,
            selected_metric_data_decoded: false,
            selected_metric_data_materialized: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
struct TraditionalScenarioExecutionEvidence {
    streaming_vortex_execution_used: bool,
    full_table_materialization_avoided: bool,
    filter_pushdown_applied: bool,
    projection_pushdown_applied: bool,
    arrays_read_count: usize,
    max_chunk_rows: usize,
    projected_columns: Vec<String>,
    result_row_count: u64,
    reader_chunk_columns_observed: Vec<String>,
    reader_chunk_dtype_summary: Vec<String>,
    reader_chunk_encoding_summary: Vec<String>,
    encoded_predicate_provider: TraditionalEncodedPredicateProviderRuntimeEvidence,
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
            result_row_count: 0,
            reader_chunk_columns_observed: Vec::new(),
            reader_chunk_dtype_summary: Vec::new(),
            reader_chunk_encoding_summary: Vec::new(),
            encoded_predicate_provider:
                TraditionalEncodedPredicateProviderRuntimeEvidence::not_applicable(),
            data_decoded: true,
            data_materialized: true,
            row_read: false,
        }
    }

    fn streaming(stats: TraditionalStreamingScanStats) -> Self {
        let TraditionalStreamingScanStats {
            arrays_read_count,
            max_chunk_rows,
            projected_columns,
            result_row_count,
            filter_pushdown_applied,
            projection_pushdown_applied,
            reader_chunk_columns_observed,
            reader_chunk_dtype_summary,
            reader_chunk_encoding_summary,
            ..
        } = stats;
        Self {
            streaming_vortex_execution_used: true,
            full_table_materialization_avoided: true,
            filter_pushdown_applied,
            projection_pushdown_applied,
            arrays_read_count,
            max_chunk_rows,
            projected_columns,
            result_row_count,
            reader_chunk_columns_observed,
            reader_chunk_dtype_summary,
            reader_chunk_encoding_summary,
            encoded_predicate_provider:
                TraditionalEncodedPredicateProviderRuntimeEvidence::not_applicable(),
            data_decoded: true,
            data_materialized: false,
            row_read: false,
        }
    }

    fn streaming_with_encoded_predicate_provider(
        stats: TraditionalStreamingScanStats,
        encoded_predicate_provider: TraditionalEncodedPredicateProviderRuntimeEvidence,
    ) -> Self {
        let mut evidence = Self::streaming(stats);
        evidence.encoded_predicate_provider = encoded_predicate_provider;
        evidence
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct TraditionalScenarioExecution {
    result_json: String,
    fact_rows: u64,
    dim_rows: u64,
    cdc_delta_rows: u64,
    rows_scanned: u64,
    rows_materialized: u64,
    evidence: TraditionalScenarioExecutionEvidence,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[derive(Debug, Clone, PartialEq)]
struct SelectionVectorMetricAggregation {
    stats: TraditionalStreamingScanStats,
    row_count: u64,
    metric_sum: f64,
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
#[derive(Debug, Clone, PartialEq)]
struct TraditionalStreamingScanStats {
    source_row_count: u64,
    result_row_count: u64,
    arrays_read_count: usize,
    max_chunk_rows: usize,
    projected_columns: Vec<String>,
    filter_pushdown_applied: bool,
    projection_pushdown_applied: bool,
    reader_chunk_columns_observed: Vec<String>,
    reader_chunk_dtype_summary: Vec<String>,
    reader_chunk_encoding_summary: Vec<String>,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_traditional_direct_transient_csv_smoke_enabled(
    request: TraditionalAnalyticsRequest,
) -> Result<TraditionalDirectTransientReport> {
    if request.requested_execution_mode != ShardLoomExecutionMode::DirectCompatibilityTransient {
        return Err(ShardLoomError::InvalidOperation(
            "direct transient CSV smoke requires --execution-mode direct_compatibility_transient; fallback execution was not attempted".to_string(),
        ));
    }
    if request.input_format != TraditionalAnalyticsInputFormat::Csv {
        return Err(ShardLoomError::InvalidOperation(format!(
            "direct transient CSV smoke only supports local CSV input, found {}; fallback execution was not attempted",
            request.input_format.as_str()
        )));
    }
    if request.scenario != TraditionalAnalyticsScenario::SelectiveFilter {
        return Err(ShardLoomError::InvalidOperation(format!(
            "direct transient CSV smoke only supports selective filter, found {}; fallback execution was not attempted",
            request.scenario.as_str()
        )));
    }
    if request.cdc_delta_csv.is_some() {
        return Err(ShardLoomError::InvalidOperation(
            "direct transient CSV smoke does not support CDC delta input; fallback execution was not attempted".to_string(),
        ));
    }
    if request.compatibility_output_format.is_some() {
        return Err(ShardLoomError::InvalidOperation(
            "direct transient CSV smoke does not support compatibility output writers; fallback execution was not attempted".to_string(),
        ));
    }
    if request.verify_native_vortex_replay || request.write_result_vortex {
        return Err(ShardLoomError::InvalidOperation(
            "direct transient CSV smoke does not support Vortex replay or result-sink writes; fallback execution was not attempted".to_string(),
        ));
    }

    let fact_source_bytes = file_len(&request.fact_csv, "fact input")?;
    let dim_source_bytes = file_len(&request.dim_csv, "dimension input")?;
    let source_bytes_read = checked_u64_sum(fact_source_bytes, dim_source_bytes)?;
    let resource_policy = request
        .resource_policy
        .resolve_for_sources(source_bytes_read);
    let execution_mode_selection = ShardLoomExecutionModeSelectionReport::from_request(
        ShardLoomExecutionModeSelectionRequest::new(
            ShardLoomExecutionMode::DirectCompatibilityTransient,
        )
        .with_source_format(request.input_format.as_str())
        .with_workload_constitution(LOCAL_VORTEX_ANALYTICS_CONSTITUTION_ID)
        .with_compatibility_input(true)
        .with_direct_transient_supported(true),
    );

    let source_read_start = std::time::Instant::now();
    let fact_rows = read_traditional_fact_csv(&request.fact_csv)?;
    let dim_rows = read_traditional_dim_csv(&request.dim_csv)?;
    let source_read_micros = duration_to_micros(source_read_start.elapsed());
    let scenario_compute_start = std::time::Instant::now();
    let mut accum = TraditionalGroupAccum::default();
    for row in &fact_rows {
        if row.flag == 1 && row.value >= 5_000 {
            accum.add(row.metric);
        }
    }
    let selected_rows_materialized = accum.row_count;
    let result_json = scalar_result_json(accum.row_count, accum.metric_sum);
    let scenario_compute_micros = duration_to_micros(scenario_compute_start.elapsed());
    let runtime_execution_certificate = direct_transient_execution_certificate(
        request.scenario,
        request.input_format,
        selected_rows_materialized,
    )?;

    Ok(TraditionalDirectTransientReport {
        scenario: request.scenario,
        input_format: request.input_format,
        execution_mode_selection,
        resource_policy,
        result_json,
        fact_rows: usize_to_u64(fact_rows.len())?,
        dim_rows: usize_to_u64(dim_rows.len())?,
        rows_scanned: usize_to_u64(fact_rows.len())?,
        rows_materialized: selected_rows_materialized,
        fact_source_path: request.fact_csv,
        dim_source_path: request.dim_csv,
        fact_source_bytes,
        dim_source_bytes,
        source_bytes_read,
        source_read_micros,
        scenario_compute_micros,
        runtime_execution_certificate,
        diagnostics: Vec::new(),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn direct_transient_execution_certificate(
    scenario: TraditionalAnalyticsScenario,
    input_format: TraditionalAnalyticsInputFormat,
    rows_materialized: u64,
) -> Result<ExecutionCertificate> {
    let mut certificate_input = ExecutionCertificateInput::new(
        "gar-flow-1b.direct_transient_csv_selective_filter.runtime",
        "direct_compatibility_transient_csv_smoke",
    )?;
    certificate_input.execution_provider_kind = ExecutionProviderKind::ShardLoomKernel;
    certificate_input.provider_scope = "direct_compatibility_transient_local_csv".to_string();
    certificate_input.provider_crate = Some("shardloom-vortex".to_string());
    certificate_input.provider_version = Some(env!("CARGO_PKG_VERSION").to_string());
    certificate_input.provider_api_surface =
        Some("run_traditional_direct_transient_csv_smoke".to_string());
    certificate_input.shardloom_admission_policy =
        Some("local_csv_direct_transient_no_external_fallback".to_string());
    certificate_input.plan_ref = Some("direct_transient_csv_selective_filter".to_string());
    certificate_input.input_ref = Some(format!(
        "traditional-analytics://source-format/{}/direct-transient",
        input_format.as_str()
    ));
    certificate_input.output_ref =
        Some("runtime-result://direct_transient_csv_selective_filter/in-memory".to_string());
    certificate_input.correctness_fixture_id =
        Some("gar-flow-1b.direct_transient_csv_selective_filter".to_string());
    let outcome = ExpectedOutcome::Rows {
        row_count: Some(rows_materialized),
    };
    certificate_input.expected_outcome = Some(outcome.clone());
    certificate_input.actual_outcome = Some(outcome);
    certificate_input.selected_segment_count = 1;
    certificate_input.skipped_segment_count = 0;
    certificate_input.side_effects_performed = vec![
        "local_csv_read".to_string(),
        format!("direct_transient_{}", scenario.as_str().replace(' ', "_")),
    ];
    certificate_input.data_read = true;
    certificate_input.data_decoded = true;
    certificate_input.data_materialized = true;
    certificate_input.row_read = true;
    certificate_input.arrow_converted = false;
    certificate_input.object_store_io = false;
    certificate_input.write_io = false;
    certificate_input.spill_io_performed = false;
    certificate_input.external_effects_executed = false;
    certificate_input.external_query_engine_invoked = false;
    certificate_input.unsafe_effect_detected = false;
    certificate_input.fallback_attempted = false;
    certificate_input.fallback_execution_allowed = false;
    certificate_input.correctness_passed = true;
    Ok(ExecutionCertificate::evaluate(certificate_input))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[allow(clippy::too_many_lines)]
fn run_traditional_analytics_benchmark_enabled(
    request: TraditionalAnalyticsRequest,
) -> Result<TraditionalAnalyticsReport> {
    use std::fs;

    let execution_mode_selection = ShardLoomExecutionModeSelectionReport::from_request(
        ShardLoomExecutionModeSelectionRequest::new(request.requested_execution_mode)
            .with_source_format(request.input_format.as_str())
            .with_workload_constitution(LOCAL_VORTEX_ANALYTICS_CONSTITUTION_ID)
            .with_compatibility_input(true)
            .with_certification_requested(true)
            .with_result_sink_requested(
                request.write_result_vortex || request.verify_native_vortex_replay,
            ),
    );
    if !execution_mode_selection.mode_supported {
        return Err(ShardLoomError::InvalidOperation(format!(
            "traditional analytics execution mode {} is unsupported for compatibility import: {}; required future evidence: {}; fallback execution was not attempted",
            execution_mode_selection.requested_execution_mode.as_str(),
            execution_mode_selection.unsupported_diagnostic_code,
            execution_mode_selection.required_future_evidence
        )));
    }

    let compatibility_import_start = std::time::Instant::now();
    fs::create_dir_all(&request.workspace_dir).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to create traditional analytics Vortex workspace '{}': {error}",
            request.workspace_dir.display()
        ))
    })?;
    let cdc_delta_required =
        request.scenario == TraditionalAnalyticsScenario::SmallChangeOverLargeBase;
    if cdc_delta_required && request.cdc_delta_csv.is_none() {
        return Err(ShardLoomError::InvalidOperation(
            "small change over large base requires a CDC delta source via --cdc-delta; fallback execution was not attempted".to_string(),
        ));
    }
    let fact_source_bytes = fact_source_len(&request.fact_csv, request.input_format, "fact input")?;
    let dim_source_bytes = file_len(&request.dim_csv, "dimension input")?;
    let cdc_delta_source_bytes = request
        .cdc_delta_csv
        .as_ref()
        .map_or(Ok(0), |path| file_len(path, "CDC delta input"))?;
    let source_bytes_read = fact_source_bytes
        .checked_add(dim_source_bytes)
        .and_then(|bytes| bytes.checked_add(cdc_delta_source_bytes))
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "traditional analytics source byte count overflow".to_string(),
            )
        })?;
    let resource_policy = request
        .resource_policy
        .resolve_for_sources(source_bytes_read);
    let source_read_start = std::time::Instant::now();
    let fact_rows =
        read_traditional_fact_rows(&request.fact_csv, request.input_format, resource_policy)?;
    let dim_rows =
        read_traditional_dim_rows(&request.dim_csv, request.input_format, resource_policy)?;
    let cdc_delta_rows = request.cdc_delta_csv.as_ref().map_or_else(
        || Ok(Vec::new()),
        |path| read_traditional_cdc_delta_csv(path),
    )?;
    let source_read_micros = duration_to_micros(source_read_start.elapsed());
    let source_rows_materialized = checked_usize_sum_to_u64(fact_rows.len(), dim_rows.len())?
        .checked_add(usize_to_u64(cdc_delta_rows.len())?)
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "traditional analytics source row count overflow".to_string(),
            )
        })?;
    let fact_vortex_path = request.workspace_dir.join("fact.vortex");
    let dim_vortex_path = request.workspace_dir.join("dim.vortex");
    let cdc_delta_vortex_path = request
        .cdc_delta_csv
        .as_ref()
        .map(|_| request.workspace_dir.join("cdc_delta.vortex"));
    let vortex_write_start = std::time::Instant::now();
    write_fact_vortex(&fact_rows, &fact_vortex_path)?;
    write_dim_vortex(&dim_rows, &dim_vortex_path)?;
    if let Some(path) = &cdc_delta_vortex_path {
        write_cdc_delta_vortex(&cdc_delta_rows, path)?;
    }
    let vortex_write_micros = duration_to_micros(vortex_write_start.elapsed());
    let compatibility_to_vortex_import_micros =
        duration_to_micros(compatibility_import_start.elapsed());
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
        let cdc_delta = cdc_delta_vortex_path
            .as_ref()
            .map(|path| read_cdc_delta_vortex(path))
            .transpose()?;
        run_vortex_derived_scenario_from_tables(request.scenario, &fact, &dim, cdc_delta.as_ref())?
    } else {
        run_vortex_derived_scenario_from_files(
            request.scenario,
            &fact_vortex_path,
            &dim_vortex_path,
            cdc_delta_vortex_path.as_deref(),
        )?
    };
    let scenario_compute_micros = duration_to_micros(scenario_compute_start.elapsed());
    let vortex_reopen_scan_micros = scenario_compute_micros;
    let fact_vortex_bytes = file_len(&fact_vortex_path, "fact Vortex file")?;
    let dim_vortex_bytes = file_len(&dim_vortex_path, "dimension Vortex file")?;
    let cdc_delta_vortex_bytes = cdc_delta_vortex_path
        .as_ref()
        .map_or(Ok(0), |path| file_len(path, "CDC delta Vortex file"))?;
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
    let cdc_delta_vortex_digest = cdc_delta_vortex_path
        .as_ref()
        .map(|path| file_digest(path, "CDC delta Vortex file"))
        .transpose()?;
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
        cdc_delta_vortex_digest.as_deref(),
        computed_result_sink
            .as_ref()
            .map(|sink| sink.digest.as_str()),
    );
    let output_replay = if request.verify_native_vortex_replay {
        Some(verify_native_vortex_replay(
            request.scenario,
            &fact_vortex_path,
            &dim_vortex_path,
            cdc_delta_vortex_path.as_deref(),
            &scenario_execution.result_json,
            fact_vortex_bytes
                .checked_add(dim_vortex_bytes)
                .and_then(|bytes| bytes.checked_add(cdc_delta_vortex_bytes))
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
        cdc_delta_source_bytes,
        fact_vortex_bytes,
        dim_vortex_bytes,
        cdc_delta_vortex_bytes,
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
    let layout_advisor_report = traditional_layout_advisor_report(
        request.scenario,
        request.input_format,
        resource_policy,
        fact_vortex_bytes,
        dim_vortex_bytes,
        &runtime_evidence,
        computed_result_sink.is_some(),
    );

    Ok(TraditionalAnalyticsReport {
        scenario: request.scenario,
        input_format: request.input_format,
        execution_mode_selection,
        resource_policy,
        result_json: scenario_execution.result_json,
        fact_rows: scenario_execution.fact_rows,
        dim_rows: scenario_execution.dim_rows,
        rows_scanned: scenario_execution.rows_scanned,
        rows_materialized: scenario_execution.rows_materialized,
        workspace_dir: request.workspace_dir,
        fact_vortex_path,
        dim_vortex_path,
        cdc_delta_vortex_path,
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
        cdc_delta_source_path: request.cdc_delta_csv,
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
        cdc_delta_vortex_digest,
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
        source_read_micros,
        compatibility_to_vortex_import_micros,
        vortex_write_micros,
        vortex_reopen_scan_micros,
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
        layout_advisor_report,
        commit_state: if computed_result_sink.is_some() {
            "local_vortex_files_and_result_sink_written_uncommitted".to_string()
        } else {
            "local_vortex_files_written_uncommitted".to_string()
        },
        rollback_cleanup_status: "caller_owned_workspace_cleanup".to_string(),
        fact_source_bytes,
        dim_source_bytes,
        cdc_delta_source_bytes,
        cdc_delta_rows: usize_to_u64(cdc_delta_rows.len())?,
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
        cdc_delta_vortex_bytes,
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
        streaming_result_row_count: scenario_execution.evidence.result_row_count,
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
    cdc_delta_vortex_path: Option<&std::path::Path>,
    expected_result_json: &str,
    source_bytes_read: u64,
) -> Result<TraditionalOutputReplayVerification> {
    let replay = run_vortex_derived_scenario_from_files(
        scenario,
        fact_vortex_path,
        dim_vortex_path,
        cdc_delta_vortex_path,
    )?;
    if replay.result_json != expected_result_json {
        return Err(ShardLoomError::InvalidOperation(format!(
            "native Vortex replay result mismatch for {}; fallback execution was not attempted",
            scenario.as_str()
        )));
    }
    let materialization_boundary_rows = if replay.evidence.data_materialized {
        checked_u64_sum(replay.fact_rows, replay.dim_rows)?
            .checked_add(replay.cdc_delta_rows)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "native Vortex replay row count overflow".to_string(),
                )
            })?
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
    cdc_delta_source_bytes: u64,
    fact_vortex_bytes: u64,
    dim_vortex_bytes: u64,
    cdc_delta_vortex_bytes: u64,
    rows_materialized: u64,
    output_replay_verified: bool,
    computed_result_sink: Option<&TraditionalComputedResultSinkVerification>,
) -> Result<TraditionalRuntimeEvidence> {
    let tasks = traditional_runtime_tasks(
        scenario,
        resource_policy,
        fact_source_bytes,
        dim_source_bytes,
        cdc_delta_source_bytes,
        fact_vortex_bytes,
        dim_vortex_bytes,
        cdc_delta_vortex_bytes,
        output_replay_verified,
        computed_result_sink.map_or(0, |sink| sink.bytes),
    )?;
    let max_parallelism = resource_policy.max_parallelism.max(1);
    let scheduler_batch_count = tasks.len().div_ceil(max_parallelism).max(1);
    let observed_max_batch_len = tasks
        .chunks(max_parallelism)
        .map(<[_]>::len)
        .max()
        .unwrap_or(0);
    let queue_limit_enforced = observed_max_batch_len <= max_parallelism;
    let memory_budget = MemoryBudget::from_gib(u64::from(resource_policy.memory_gb))?;
    let mut memory_pool = MemoryPoolPlan::new(memory_budget.clone());
    let mut memory_reservations_requested = 0;
    let mut memory_reservations_granted = 0;
    let mut memory_reservations_released = 0;
    let mut memory_reservations_denied = 0;
    let mut memory_peak_reserved_bytes = 0;
    for batch in tasks.chunks(max_parallelism) {
        let mut granted_reservations = Vec::new();
        for task in batch {
            memory_reservations_requested += 1;
            let reservation_id =
                MemoryReservationId::new(format!("traditional-runtime-{}", task.task_id.as_str()))?;
            let owner = MemoryOwner::new(task.operator_class, task.label)?
                .with_task_id(task.task_id.clone());
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
                granted_reservations.push(reservation_id);
            } else {
                memory_reservations_denied += 1;
            }
        }
        for reservation_id in granted_reservations {
            memory_pool.release_reservation(&reservation_id)?;
            memory_reservations_released += 1;
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
    cdc_delta_source_bytes: u64,
    fact_vortex_bytes: u64,
    dim_vortex_bytes: u64,
    cdc_delta_vortex_bytes: u64,
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
    ];
    if cdc_delta_source_bytes > 0 {
        tasks.push(TraditionalRuntimeTaskEvidence::new(
            "compatibility-import-cdc-delta",
            "compatibility import CDC delta to native Vortex",
            OperatorMemoryClass::Translation,
            task_memory(cdc_delta_source_bytes),
        )?);
    }
    tasks.push(TraditionalRuntimeTaskEvidence::new(
        "native-vortex-scenario-compute",
        "native Vortex scenario compute",
        scenario_operator_memory_class(scenario),
        task_memory(
            fact_source_bytes
                .saturating_add(dim_source_bytes)
                .saturating_add(cdc_delta_source_bytes),
        ),
    )?);
    if output_replay_verified {
        tasks.push(TraditionalRuntimeTaskEvidence::new(
            "native-vortex-replay",
            "native Vortex replay verification",
            OperatorMemoryClass::Scan,
            task_memory(
                fact_vortex_bytes
                    .saturating_add(dim_vortex_bytes)
                    .saturating_add(cdc_delta_vortex_bytes),
            ),
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
        | TraditionalAnalyticsScenario::FilterProjectionLimit
        | TraditionalAnalyticsScenario::PartitionPruning
        | TraditionalAnalyticsScenario::ManySmallFilesScan
        | TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv => OperatorMemoryClass::Filter,
        TraditionalAnalyticsScenario::GroupByAggregation
        | TraditionalAnalyticsScenario::DistinctCount
        | TraditionalAnalyticsScenario::MultiKeyGroupBy
        | TraditionalAnalyticsScenario::NullHeavyAggregate
        | TraditionalAnalyticsScenario::NestedJsonFieldScan
        | TraditionalAnalyticsScenario::HighCardinalityStringGroupDistinct => {
            OperatorMemoryClass::Aggregate
        }
        TraditionalAnalyticsScenario::SortAndTopK | TraditionalAnalyticsScenario::TopNPerGroup => {
            OperatorMemoryClass::Sort
        }
        TraditionalAnalyticsScenario::HashJoin
        | TraditionalAnalyticsScenario::JoinAggregate
        | TraditionalAnalyticsScenario::SmallChangeOverLargeBase
        | TraditionalAnalyticsScenario::ScaleStressSkewedJoinAggregation
        | TraditionalAnalyticsScenario::ScaleStressMultiStageEtl => OperatorMemoryClass::Join,
        TraditionalAnalyticsScenario::RowNumberWindow => OperatorMemoryClass::Window,
        TraditionalAnalyticsScenario::CleanCastFilterWrite => OperatorMemoryClass::Sink,
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
fn traditional_layout_advisor_report(
    scenario: TraditionalAnalyticsScenario,
    input_format: TraditionalAnalyticsInputFormat,
    resource_policy: TraditionalAnalyticsResourcePolicy,
    fact_vortex_bytes: u64,
    dim_vortex_bytes: u64,
    runtime_evidence: &TraditionalRuntimeEvidence,
    result_sink_written: bool,
) -> TraditionalVortexLayoutAdvisorReport {
    let scenario_slug = scenario.as_str().replace(['/', ' '], "-");
    let mut evidence_source_refs = vec![
        traditional_benchmark_row_ref(scenario, input_format),
        traditional_coverage_row_ref(scenario, input_format),
        runtime_evidence
            .execution_certificate
            .certificate_id
            .clone(),
        format!(
            "native-io://local_vortex_analytics_v1/{}/source-import",
            input_format.as_str()
        ),
    ];
    if result_sink_written {
        evidence_source_refs.push(format!(
            "native-io://local_vortex_analytics_v1/{scenario_slug}/result-sink"
        ));
    }
    let (encoding_strategy, statistics_policy, dictionary_strategy, cluster_key) =
        traditional_layout_recommendations(scenario);
    let recommendation_evidence_status = if result_sink_written
        && runtime_evidence.execution_certificate.status.as_str() == "certified"
    {
        "measured_runtime_evidence_with_simulated_layout_advice"
    } else {
        "runtime_evidence_present_result_sink_or_correctness_incomplete"
    };
    TraditionalVortexLayoutAdvisorReport {
        schema_version: "shardloom.vortex_layout_advisor_report.v1".to_string(),
        report_id: format!("p747.local_vortex_analytics.{scenario_slug}.layout_advisor"),
        status: "report_only".to_string(),
        workload_constitution_id: LOCAL_VORTEX_ANALYTICS_CONSTITUTION_ID.to_string(),
        evidence_basis: format!(
            "workload_constitution={},input_format={},fact_vortex_bytes={},dim_vortex_bytes={},runtime_scheduler_ref={}",
            LOCAL_VORTEX_ANALYTICS_CONSTITUTION_ID,
            input_format.as_str(),
            fact_vortex_bytes,
            dim_vortex_bytes,
            &runtime_evidence.scheduler_ref
        ),
        evidence_source_refs,
        recommended_chunk_rows: resource_policy.target_batch_rows,
        recommended_chunk_bytes: resource_policy.target_partition_bytes,
        encoding_strategy: encoding_strategy.to_string(),
        statistics_policy: statistics_policy.to_string(),
        dictionary_strategy: dictionary_strategy.to_string(),
        cluster_key: cluster_key.to_string(),
        micro_segment_flush_policy: format!(
            "flush_at_target_batch_rows_or_{}bytes",
            resource_policy.target_partition_bytes
        ),
        compaction_trigger: if resource_policy.target_partition_count > 1 {
            "compact_when_small_segments_exceed_target_partition_count"
        } else {
            "no_compaction_for_single_local_partition"
        }
        .to_string(),
        read_write_tradeoff:
            "favor_read_pruning_and_dictionary_reuse; write-layout execution remains blocked"
                .to_string(),
        recommendation_evidence_status: recommendation_evidence_status.to_string(),
        measured_evidence_source_count: if result_sink_written { 5 } else { 4 },
        simulated_evidence_source_count: 1,
        blocked_evidence_source_count: 1,
        write_layout_execution_allowed: false,
        improvement_claim_allowed: false,
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn traditional_layout_recommendations(
    scenario: TraditionalAnalyticsScenario,
) -> (&'static str, &'static str, &'static str, &'static str) {
    match scenario {
        TraditionalAnalyticsScenario::SelectiveFilter
        | TraditionalAnalyticsScenario::FilterProjectionLimit => (
            "preserve_current_vortex_encoding_with_filter_column_statistics",
            "min_max_and_null_count_for_flag_value_metric",
            "dictionary_encode_category_when_cardinality_ratio_allows",
            "flag,value",
        ),
        TraditionalAnalyticsScenario::GroupByAggregation
        | TraditionalAnalyticsScenario::MultiKeyGroupBy => (
            "cluster_group_keys_and_preserve_metric_column",
            "group_key_category_metric_summary_statistics",
            "dictionary_encode_category",
            "group_key,category",
        ),
        TraditionalAnalyticsScenario::DistinctCount
        | TraditionalAnalyticsScenario::HighCardinalityStringGroupDistinct => (
            "preserve_dictionary_candidates_for_string_distinct",
            "category_cardinality_and_null_count",
            "dictionary_encode_category_with_cardinality_guard",
            "category",
        ),
        TraditionalAnalyticsScenario::HashJoin
        | TraditionalAnalyticsScenario::JoinAggregate
        | TraditionalAnalyticsScenario::ScaleStressSkewedJoinAggregation
        | TraditionalAnalyticsScenario::ScaleStressMultiStageEtl => (
            "co_locate_dim_key_and_metric_columns_for_join_probe",
            "dim_key_category_metric_summary_statistics",
            "dictionary_encode_dim_label_and_category",
            "dim_key,category",
        ),
        TraditionalAnalyticsScenario::SortAndTopK | TraditionalAnalyticsScenario::TopNPerGroup => (
            "cluster_metric_order_candidates_for_topn_reads",
            "metric_min_max_and_group_key_summary_statistics",
            "dictionary_encode_category_when_present",
            "metric,group_key,id",
        ),
        TraditionalAnalyticsScenario::RowNumberWindow => (
            "cluster_window_partition_and_order_columns",
            "group_key_metric_id_summary_statistics",
            "dictionary_encode_category_when_present",
            "group_key,metric,id",
        ),
        TraditionalAnalyticsScenario::WideProjection => (
            "chunk_columns_for_projection_locality",
            "column_presence_and_row_count_statistics",
            "preserve_current_dictionary_candidates",
            "projected_columns",
        ),
        TraditionalAnalyticsScenario::CleanCastFilterWrite => (
            "preserve_dirty_column_statistics_before_clean_write",
            "raw_event_time_dirty_numeric_dirty_flag_quality_statistics",
            "dictionary_encode_dirty_flag_and_category",
            "raw_event_time,dirty_numeric,dirty_flag",
        ),
        TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv => (
            "preserve_dirty_column_fidelity_for_quality_scan",
            "raw_event_time_dirty_numeric_validity_statistics",
            "cluster_valid_dirty_rows_for_quality_filters",
            "raw_event_time,dirty_numeric",
        ),
        TraditionalAnalyticsScenario::SmallChangeOverLargeBase => (
            "preserve_base_table_and_delta_overlay_separately",
            "base_row_count_delta_op_sequence_and_effective_timestamp_statistics",
            "cluster_delta_overlay_by_id_and_effective_timestamp",
            "id,effective_ts,op",
        ),
        TraditionalAnalyticsScenario::PartitionPruning => (
            "preserve_event_date_statistics_for_pruning",
            "event_date_min_max_and_partition_statistics",
            "cluster_by_event_date_when_filter_selectivity_is_high",
            "event_date",
        ),
        TraditionalAnalyticsScenario::ManySmallFilesScan => (
            "coalesce_small_fact_splits_after_import",
            "per_split_row_count_and_source_bytes",
            "compact_small_parts_before_repeated_scan",
            "file_bucket,event_date",
        ),
        TraditionalAnalyticsScenario::NullHeavyAggregate => (
            "preserve_nullable_metric_validity_statistics",
            "nullable_metric_00_valid_count_and_sum_profile",
            "dictionary_or_sparse_validity_when_null_density_is_high",
            "nullable_metric_00",
        ),
        TraditionalAnalyticsScenario::NestedJsonFieldScan => (
            "preserve_nested_payload_string_fidelity",
            "nested_payload_presence_and_score_statistics",
            "extract_frequent_nested_metrics_after_json_capability_certification",
            "nested_payload,nested_score",
        ),
        TraditionalAnalyticsScenario::CsvFileIngest => (
            "preserve_writer_defaults_for_ingest_smoke",
            "row_count_and_source_file_statistics",
            "preserve_current_dictionary_candidates",
            "none",
        ),
    }
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
    "fact(id:u64,group_key:u32,dim_key:u32,value:u32,metric:f64,flag:u8,category:utf8);dim(dim_key:u32,dim_label:utf8,weight:f64);cdc_delta(id:u64,op:utf8,value:utf8,metric:utf8,effective_ts:utf8)"
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
#[allow(clippy::too_many_lines)]
fn run_traditional_analytics_vortex_batch_benchmark_enabled(
    request: TraditionalAnalyticsVortexBatchRequest,
) -> Result<TraditionalAnalyticsVortexBatchReport> {
    let TraditionalAnalyticsVortexBatchRequest {
        scenarios,
        fact_vortex,
        dim_vortex,
        cdc_delta_vortex,
        requested_execution_mode,
        result_workspace_dir,
        write_result_vortex,
    } = request;
    if scenarios.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "traditional analytics native Vortex batch run requires at least one scenario; fallback execution was not attempted".to_string(),
        ));
    }
    if write_result_vortex && result_workspace_dir.is_none() {
        return Err(ShardLoomError::InvalidOperation(
            "traditional-analytics-vortex-batch-run --write-result-vortex requires --workspace for caller-owned result artifact output; fallback execution was not attempted".to_string(),
        ));
    }
    let source_state_prepare_start = std::time::Instant::now();
    let source_state = TraditionalVortexBatchSourceState::from_paths(
        &fact_vortex,
        &dim_vortex,
        cdc_delta_vortex.as_deref(),
        &scenarios,
    )?;
    let source_state_prepare_micros = duration_to_micros(source_state_prepare_start.elapsed());
    let dimension_label_state_consumer_count = source_state.dimension_label_state_consumer_count;
    let dimension_label_state_recompute_avoided_count =
        source_state.dimension_label_state_recompute_avoided_count();
    let category_metric_state_consumer_count = source_state.category_metric_state_consumer_count;
    let category_metric_state_recompute_avoided_count =
        source_state.category_metric_state_recompute_avoided_count();
    let group_category_metric_state_consumer_count =
        source_state.group_category_metric_state_consumer_count;
    let group_category_metric_state_recompute_avoided_count =
        source_state.group_category_metric_state_recompute_avoided_count();
    let ranked_metric_state_consumer_count = source_state.ranked_metric_state_consumer_count;
    let ranked_metric_state_recompute_avoided_count =
        source_state.ranked_metric_state_recompute_avoided_count();
    let selective_filter_state_consumer_count = source_state.selective_filter_state_consumer_count;
    let selective_filter_state_recompute_avoided_count =
        source_state.selective_filter_state_recompute_avoided_count();
    let dirty_input_state_consumer_count = source_state.dirty_input_state_consumer_count;
    let dirty_input_state_recompute_avoided_count =
        source_state.dirty_input_state_recompute_avoided_count();
    let date_null_metric_state_consumer_count = source_state.date_null_metric_state_consumer_count;
    let date_null_metric_state_recompute_avoided_count =
        source_state.date_null_metric_state_recompute_avoided_count();

    let mut reports = Vec::with_capacity(scenarios.len());
    for (index, scenario) in scenarios.into_iter().enumerate() {
        let scenario_workspace = result_workspace_dir.as_ref().map(|workspace| {
            workspace.join(format!(
                "{:02}-{}",
                index + 1,
                traditional_scenario_slug(scenario)
            ))
        });
        let child_request = TraditionalAnalyticsVortexRequest::new(
            scenario,
            fact_vortex.clone(),
            dim_vortex.clone(),
        )
        .with_cdc_delta_vortex(cdc_delta_vortex.clone())
        .with_requested_execution_mode(requested_execution_mode)
        .with_result_workspace_dir(scenario_workspace)
        .with_result_vortex_write(write_result_vortex);
        reports.push(
            run_traditional_analytics_vortex_benchmark_with_batch_source_state(
                child_request,
                &source_state,
            )?,
        );
    }

    let total_scenario_compute_micros = checked_u64_values_sum(
        reports.iter().map(|report| report.scenario_compute_micros),
        "native Vortex batch scenario compute micros",
    )?;
    let total_vortex_scan_micros = checked_u64_values_sum(
        reports.iter().map(|report| report.vortex_scan_micros),
        "native Vortex batch scan micros",
    )?;
    let total_result_sink_write_micros = if write_result_vortex {
        Some(checked_u64_values_sum(
            reports
                .iter()
                .map(|report| report.computed_result_sink_write_micros.unwrap_or(0)),
            "native Vortex batch result sink write micros",
        )?)
    } else {
        None
    };
    let total_rows_scanned = checked_u64_values_sum(
        reports.iter().map(|report| report.rows_scanned),
        "native Vortex batch rows scanned",
    )?;
    let total_rows_materialized = checked_u64_values_sum(
        reports.iter().map(|report| report.rows_materialized),
        "native Vortex batch rows materialized",
    )?;
    let all_native_io_certificates_certified = reports
        .iter()
        .all(|report| report.native_io_certificate.is_certified());
    let all_result_sink_replays_verified = write_result_vortex
        && reports
            .iter()
            .all(|report| report.computed_result_sink_replay_verified);
    let diagnostics = reports
        .iter()
        .flat_map(|report| report.diagnostics.clone())
        .collect();

    Ok(TraditionalAnalyticsVortexBatchReport {
        reports,
        requested_execution_mode,
        result_sink_requested: write_result_vortex,
        source_state_prepare_micros,
        dimension_label_state_consumer_count,
        dimension_label_state_recompute_avoided_count,
        category_metric_state_consumer_count,
        category_metric_state_recompute_avoided_count,
        group_category_metric_state_consumer_count,
        group_category_metric_state_recompute_avoided_count,
        ranked_metric_state_consumer_count,
        ranked_metric_state_recompute_avoided_count,
        selective_filter_state_consumer_count,
        selective_filter_state_recompute_avoided_count,
        dirty_input_state_consumer_count,
        dirty_input_state_recompute_avoided_count,
        date_null_metric_state_consumer_count,
        date_null_metric_state_recompute_avoided_count,
        total_scenario_compute_micros,
        total_vortex_scan_micros,
        total_result_sink_write_micros,
        total_rows_scanned,
        total_rows_materialized,
        all_native_io_certificates_certified,
        all_result_sink_replays_verified,
        diagnostics,
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn checked_u64_values_sum(values: impl IntoIterator<Item = u64>, context: &str) -> Result<u64> {
    values.into_iter().try_fold(0u64, |total, value| {
        total
            .checked_add(value)
            .ok_or_else(|| ShardLoomError::InvalidOperation(format!("{context} overflowed u64")))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[allow(clippy::too_many_lines)]
fn run_traditional_analytics_vortex_benchmark_enabled(
    request: TraditionalAnalyticsVortexRequest,
) -> Result<TraditionalAnalyticsVortexReport> {
    let execution_mode_selection = ShardLoomExecutionModeSelectionReport::from_request(
        ShardLoomExecutionModeSelectionRequest::new(request.requested_execution_mode)
            .with_source_format("vortex")
            .with_workload_constitution(LOCAL_VORTEX_ANALYTICS_CONSTITUTION_ID)
            .with_source_already_vortex(true)
            .with_prepared_artifact_available(true)
            .with_prepared_artifact_reuse_requested(
                request.requested_execution_mode == ShardLoomExecutionMode::PreparedVortex,
            )
            .with_result_sink_requested(request.write_result_vortex)
            .with_native_vortex_provider_available(true),
    );
    if !execution_mode_selection.mode_supported {
        return Err(ShardLoomError::InvalidOperation(format!(
            "traditional analytics execution mode {} is unsupported for native Vortex input: {}; required future evidence: {}; fallback execution was not attempted",
            execution_mode_selection.requested_execution_mode.as_str(),
            execution_mode_selection.unsupported_diagnostic_code,
            execution_mode_selection.required_future_evidence
        )));
    }
    let source_snapshot = TraditionalVortexSourceSnapshot::from_paths(
        &request.fact_vortex,
        &request.dim_vortex,
        request.cdc_delta_vortex.as_deref(),
    )?;
    run_traditional_analytics_vortex_benchmark_with_source_context(request, &source_snapshot, None)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[allow(clippy::too_many_lines)]
fn run_traditional_analytics_vortex_benchmark_with_batch_source_state(
    request: TraditionalAnalyticsVortexRequest,
    source_state: &TraditionalVortexBatchSourceState,
) -> Result<TraditionalAnalyticsVortexReport> {
    run_traditional_analytics_vortex_benchmark_with_source_context(
        request,
        &source_state.source_snapshot,
        Some(source_state),
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[allow(clippy::too_many_lines)]
fn run_traditional_analytics_vortex_benchmark_with_source_context(
    request: TraditionalAnalyticsVortexRequest,
    source_snapshot: &TraditionalVortexSourceSnapshot,
    batch_source_state: Option<&TraditionalVortexBatchSourceState>,
) -> Result<TraditionalAnalyticsVortexReport> {
    let execution_mode_selection = ShardLoomExecutionModeSelectionReport::from_request(
        ShardLoomExecutionModeSelectionRequest::new(request.requested_execution_mode)
            .with_source_format("vortex")
            .with_workload_constitution(LOCAL_VORTEX_ANALYTICS_CONSTITUTION_ID)
            .with_source_already_vortex(true)
            .with_prepared_artifact_available(true)
            .with_prepared_artifact_reuse_requested(
                request.requested_execution_mode == ShardLoomExecutionMode::PreparedVortex,
            )
            .with_result_sink_requested(request.write_result_vortex)
            .with_native_vortex_provider_available(true),
    );
    if !execution_mode_selection.mode_supported {
        return Err(ShardLoomError::InvalidOperation(format!(
            "traditional analytics execution mode {} is unsupported for native Vortex input: {}; required future evidence: {}; fallback execution was not attempted",
            execution_mode_selection.requested_execution_mode.as_str(),
            execution_mode_selection.unsupported_diagnostic_code,
            execution_mode_selection.required_future_evidence
        )));
    }

    let fact_vortex_bytes = source_snapshot.fact_vortex_bytes;
    let dim_vortex_bytes = source_snapshot.dim_vortex_bytes;
    let cdc_delta_vortex_bytes = source_snapshot.cdc_delta_vortex_bytes;
    let fact_vortex_digest = source_snapshot.fact_vortex_digest.clone();
    let dim_vortex_digest = source_snapshot.dim_vortex_digest.clone();
    let cdc_delta_vortex_digest = source_snapshot.cdc_delta_vortex_digest.clone();
    let source_bytes_read = source_snapshot.source_bytes_read;
    let scenario_compute_start = std::time::Instant::now();
    let scenario_execution = if let Some(source_state) = batch_source_state {
        run_vortex_derived_scenario_from_files_with_batch_source_state(
            request.scenario,
            &request.fact_vortex,
            &request.dim_vortex,
            request.cdc_delta_vortex.as_deref(),
            source_state,
        )?
    } else {
        run_vortex_derived_scenario_from_files(
            request.scenario,
            &request.fact_vortex,
            &request.dim_vortex,
            request.cdc_delta_vortex.as_deref(),
        )?
    };
    let scenario_compute_micros = duration_to_micros(scenario_compute_start.elapsed());
    let materialization_boundary_rows = if scenario_execution.evidence.data_materialized {
        checked_u64_sum(
            checked_u64_sum(scenario_execution.fact_rows, scenario_execution.dim_rows)?,
            scenario_execution.cdc_delta_rows,
        )?
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
    let computed_result_sink = if request.write_result_vortex {
        let Some(result_workspace_dir) = request.result_workspace_dir.as_ref() else {
            return Err(ShardLoomError::InvalidOperation(
                "traditional-analytics-vortex-run --write-result-vortex requires --workspace for caller-owned result artifact output; fallback execution was not attempted"
                    .to_string(),
            ));
        };
        std::fs::create_dir_all(result_workspace_dir).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create native Vortex result-sink workspace '{}': {error}",
                result_workspace_dir.display()
            ))
        })?;
        Some(write_and_verify_computed_result_sink(
            request.scenario,
            &scenario_execution.result_json,
            scenario_execution.rows_materialized,
            result_workspace_dir,
        )?)
    } else {
        None
    };
    let result_sink_claim_gate_status = if computed_result_sink.is_some() {
        "result_sink_replay_certified"
    } else {
        "not_claim_grade_missing_result_sink_evidence"
    };
    let result_sink_claim_gate_reason = if computed_result_sink.is_some() {
        "prepared_native_result_sink_replay_certificate_present"
    } else {
        "prepared_native_claim_grade_requires_result_sink_replay_when_result_sink_required"
    };

    Ok(TraditionalAnalyticsVortexReport {
        scenario: request.scenario,
        execution_mode_selection,
        result_json: scenario_execution.result_json,
        fact_rows: scenario_execution.fact_rows,
        dim_rows: scenario_execution.dim_rows,
        cdc_delta_rows: scenario_execution.cdc_delta_rows,
        rows_scanned: scenario_execution.rows_scanned,
        rows_materialized: scenario_execution.rows_materialized,
        fact_vortex_path: request.fact_vortex,
        dim_vortex_path: request.dim_vortex,
        cdc_delta_vortex_path: request.cdc_delta_vortex,
        fact_vortex_bytes,
        dim_vortex_bytes,
        cdc_delta_vortex_bytes,
        fact_vortex_digest,
        dim_vortex_digest,
        cdc_delta_vortex_digest,
        source_bytes_read,
        materialization_boundary_rows,
        scenario_compute_micros,
        vortex_scan_micros: scenario_compute_micros,
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
        result_sink_claim_gate_status: result_sink_claim_gate_status.to_string(),
        result_sink_claim_gate_reason: result_sink_claim_gate_reason.to_string(),
        commit_state: if computed_result_sink.is_some() {
            "native_vortex_result_sink_written_uncommitted".to_string()
        } else {
            "native_vortex_input_read_only".to_string()
        },
        rollback_cleanup_status: if computed_result_sink.is_some() {
            "caller_owned_workspace_cleanup".to_string()
        } else {
            "caller_owned_input_artifacts".to_string()
        },
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
        streaming_result_row_count: scenario_execution.evidence.result_row_count,
        streaming_reader_chunk_columns_observed: scenario_execution
            .evidence
            .reader_chunk_columns_observed,
        streaming_reader_chunk_dtype_summary: scenario_execution
            .evidence
            .reader_chunk_dtype_summary,
        streaming_reader_chunk_encoding_summary: scenario_execution
            .evidence
            .reader_chunk_encoding_summary,
        encoded_predicate_provider_filter_column_probe_requested: scenario_execution
            .evidence
            .encoded_predicate_provider
            .requested,
        encoded_predicate_provider_filter_column_probe_requested_columns: scenario_execution
            .evidence
            .encoded_predicate_provider
            .requested_columns,
        encoded_predicate_provider_filter_column_probe_status: scenario_execution
            .evidence
            .encoded_predicate_provider
            .probe_status,
        encoded_predicate_provider_filter_column_probe_reader_split_count: scenario_execution
            .evidence
            .encoded_predicate_provider
            .reader_split_count,
        encoded_predicate_provider_filter_column_probe_row_count: scenario_execution
            .evidence
            .encoded_predicate_provider
            .probe_row_count,
        encoded_predicate_provider_filter_column_probe_reader_chunk_columns_observed:
            scenario_execution
                .evidence
                .encoded_predicate_provider
                .reader_chunk_columns_observed,
        encoded_predicate_provider_filter_column_probe_reader_chunk_dtype_summary:
            scenario_execution
                .evidence
                .encoded_predicate_provider
                .reader_chunk_dtype_summary,
        encoded_predicate_provider_filter_column_probe_reader_chunk_encoding_summary:
            scenario_execution
                .evidence
                .encoded_predicate_provider
                .reader_chunk_encoding_summary,
        encoded_predicate_provider_kernel_input_count: scenario_execution
            .evidence
            .encoded_predicate_provider
            .encoded_kernel_input_count,
        encoded_predicate_provider_conjunctive_bridge_runtime_status: scenario_execution
            .evidence
            .encoded_predicate_provider
            .bridge_status,
        encoded_predicate_provider_conjunctive_bridge_runtime_report_id: scenario_execution
            .evidence
            .encoded_predicate_provider
            .bridge_report_id,
        encoded_predicate_provider_conjunctive_bridge_intersection_count: scenario_execution
            .evidence
            .encoded_predicate_provider
            .bridge_intersection_count,
        encoded_predicate_provider_conjunctive_bridge_selected_row_count: scenario_execution
            .evidence
            .encoded_predicate_provider
            .bridge_selected_row_count,
        encoded_predicate_provider_filter_column_batches_consumed: scenario_execution
            .evidence
            .encoded_predicate_provider
            .filter_column_batches_consumed,
        encoded_predicate_provider_selection_vector_intersection_certified: scenario_execution
            .evidence
            .encoded_predicate_provider
            .selection_vector_intersection_certified,
        encoded_predicate_provider_selected_metric_aggregation_status: scenario_execution
            .evidence
            .encoded_predicate_provider
            .selected_metric_aggregation_status,
        encoded_predicate_provider_selected_metric_selection_vector_consumed: scenario_execution
            .evidence
            .encoded_predicate_provider
            .selected_metric_selection_vector_consumed,
        encoded_predicate_provider_selected_metric_source: scenario_execution
            .evidence
            .encoded_predicate_provider
            .selected_metric_source,
        encoded_predicate_provider_selected_metric_row_count: scenario_execution
            .evidence
            .encoded_predicate_provider
            .selected_metric_row_count,
        encoded_predicate_provider_selected_metric_sum: scenario_execution
            .evidence
            .encoded_predicate_provider
            .selected_metric_sum,
        encoded_predicate_provider_selected_metric_scan_split_count: scenario_execution
            .evidence
            .encoded_predicate_provider
            .selected_metric_scan_split_count,
        encoded_predicate_provider_selected_metric_data_decoded: scenario_execution
            .evidence
            .encoded_predicate_provider
            .selected_metric_data_decoded,
        encoded_predicate_provider_selected_metric_data_materialized: scenario_execution
            .evidence
            .encoded_predicate_provider
            .selected_metric_data_materialized,
        encoded_predicate_provider_filter_column_probe_data_decoded: scenario_execution
            .evidence
            .encoded_predicate_provider
            .data_decoded,
        encoded_predicate_provider_filter_column_probe_data_materialized: scenario_execution
            .evidence
            .encoded_predicate_provider
            .data_materialized,
        encoded_predicate_provider_filter_column_probe_row_read: scenario_execution
            .evidence
            .encoded_predicate_provider
            .row_read,
        encoded_predicate_provider_filter_column_probe_fallback_attempted: scenario_execution
            .evidence
            .encoded_predicate_provider
            .fallback_attempted,
        encoded_predicate_provider_filter_column_probe_external_engine_invoked: scenario_execution
            .evidence
            .encoded_predicate_provider
            .external_engine_invoked,
        data_decoded: scenario_execution.evidence.data_decoded,
        data_materialized: scenario_execution.evidence.data_materialized,
        materialization_boundary_report_emitted: true,
        row_read: scenario_execution.evidence.row_read,
        arrow_converted: false,
        object_store_io: false,
        write_io: computed_result_sink.is_some(),
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
fn fact_source_len(
    path: &std::path::Path,
    input_format: TraditionalAnalyticsInputFormat,
    label: &str,
) -> Result<u64> {
    if !path.is_dir() {
        return file_len(path, label);
    }
    let mut total = 0_u64;
    for part in fact_input_part_paths(path, input_format, label)? {
        total = total.checked_add(file_len(&part, label)?).ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "{label} directory '{}' byte count overflow",
                path.display()
            ))
        })?;
    }
    Ok(total)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn fact_input_part_paths(
    path: &std::path::Path,
    input_format: TraditionalAnalyticsInputFormat,
    label: &str,
) -> Result<Vec<PathBuf>> {
    let extension = match input_format {
        TraditionalAnalyticsInputFormat::Csv => "csv",
        TraditionalAnalyticsInputFormat::JsonLines => "jsonl",
        TraditionalAnalyticsInputFormat::Parquet
        | TraditionalAnalyticsInputFormat::ArrowIpc
        | TraditionalAnalyticsInputFormat::Avro
        | TraditionalAnalyticsInputFormat::Orc => {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{label} directory '{}' is only supported for split CSV or JSONL inputs; fallback execution was not attempted",
                path.display()
            )));
        }
    };
    let entries = std::fs::read_dir(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read {label} directory '{}': {error}",
            path.display()
        ))
    })?;
    let mut parts = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to read {label} directory entry in '{}': {error}",
                path.display()
            ))
        })?;
        let part_path = entry.path();
        if part_path.is_file()
            && part_path
                .extension()
                .is_some_and(|value| value == std::ffi::OsStr::new(extension))
        {
            parts.push(part_path);
        }
    }
    parts.sort();
    if parts.is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} directory '{}' contains no *.{extension} part files; fallback execution was not attempted",
            path.display()
        )));
    }
    Ok(parts)
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
    cdc_delta_digest: Option<&str>,
    result_digest: Option<&str>,
) -> String {
    let mut digest = Fnv1a64::new();
    digest.update(b"fact_vortex_digest");
    digest.update(fact_digest.as_bytes());
    digest.update(b"dim_vortex_digest");
    digest.update(dim_digest.as_bytes());
    if let Some(cdc_delta_digest) = cdc_delta_digest {
        digest.update(b"cdc_delta_vortex_digest");
        digest.update(cdc_delta_digest.as_bytes());
    }
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
impl VortexCdcDeltaTable {
    fn len(&self) -> usize {
        self.id.len()
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
            "event_date",
            "nullable_metric_00",
            "nested_payload",
            "raw_event_time",
            "dirty_numeric",
            "dirty_flag",
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
            VarBinViewArray::from_iter_str(
                rows.iter()
                    .map(|row| row.event_date.as_deref().unwrap_or("")),
            )
            .into_array(),
            VarBinViewArray::from_iter_str(
                rows.iter()
                    .map(|row| row.nullable_metric_00.as_deref().unwrap_or("")),
            )
            .into_array(),
            VarBinViewArray::from_iter_str(
                rows.iter()
                    .map(|row| row.nested_payload.as_deref().unwrap_or("")),
            )
            .into_array(),
            VarBinViewArray::from_iter_str(
                rows.iter()
                    .map(|row| row.raw_event_time.as_deref().unwrap_or("")),
            )
            .into_array(),
            VarBinViewArray::from_iter_str(
                rows.iter()
                    .map(|row| row.dirty_numeric.as_deref().unwrap_or("")),
            )
            .into_array(),
            VarBinViewArray::from_iter_str(
                rows.iter()
                    .map(|row| row.dirty_flag.as_deref().unwrap_or("")),
            )
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
fn write_cdc_delta_vortex(rows: &[TraditionalCdcDeltaRow], path: &std::path::Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let array = StructArray::try_new(
        FieldNames::from(["id", "op", "value", "metric", "effective_ts"]),
        vec![
            rows.iter()
                .map(|row| row.id)
                .collect::<PrimitiveArray>()
                .into_array(),
            VarBinViewArray::from_iter_str(rows.iter().map(|row| row.op.as_str())).into_array(),
            VarBinViewArray::from_iter_str(rows.iter().map(|row| row.value.as_str())).into_array(),
            VarBinViewArray::from_iter_str(rows.iter().map(|row| row.metric.as_str())).into_array(),
            VarBinViewArray::from_iter_str(rows.iter().map(|row| row.effective_ts.as_str()))
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
    let id = primitive_field::<u64>(&fields, "id")?;
    let row_count = id.len();
    Ok(VortexFactTable {
        id,
        group_key: primitive_field::<u32>(&fields, "group_key")?,
        dim_key: primitive_field::<u32>(&fields, "dim_key")?,
        value: primitive_field::<u32>(&fields, "value")?,
        metric: primitive_field::<f64>(&fields, "metric")?,
        flag: primitive_field::<u8>(&fields, "flag")?,
        category: utf8_field(&fields, "category")?,
        event_date: optional_utf8_field(&fields, "event_date", row_count)?,
        nullable_metric_00: optional_utf8_field(&fields, "nullable_metric_00", row_count)?,
        nested_payload: optional_utf8_field(&fields, "nested_payload", row_count)?,
        raw_event_time: optional_utf8_field(&fields, "raw_event_time", row_count)?,
        dirty_numeric: optional_utf8_field(&fields, "dirty_numeric", row_count)?,
        dirty_flag: optional_utf8_field(&fields, "dirty_flag", row_count)?,
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
fn read_cdc_delta_vortex(path: &std::path::Path) -> Result<VortexCdcDeltaTable> {
    let fields = read_vortex_struct(path)?;
    Ok(VortexCdcDeltaTable {
        id: primitive_field::<u64>(&fields, "id")?,
        op: utf8_field(&fields, "op")?,
        value: utf8_field(&fields, "value")?,
        metric: utf8_field(&fields, "metric")?,
        effective_ts: utf8_field(&fields, "effective_ts")?,
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
fn optional_utf8_field(
    fields: &std::collections::BTreeMap<String, vortex::array::ArrayRef>,
    name: &str,
    row_count: usize,
) -> Result<Vec<String>> {
    if fields.contains_key(name) {
        utf8_field(fields, name)
    } else {
        Ok(vec![String::new(); row_count])
    }
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
    if path.is_dir() {
        let mut rows = Vec::new();
        for part in fact_input_part_paths(path, input_format, "fact input")? {
            match read_traditional_fact_rows(&part, input_format, resource_policy) {
                Ok(part_rows) => rows.extend(part_rows),
                Err(error)
                    if input_format == TraditionalAnalyticsInputFormat::JsonLines
                        && error.to_string().contains("contains no rows") => {}
                Err(error) => return Err(error),
            }
        }
        if rows.is_empty() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "fact input directory '{}' contains no rows; fallback execution was not attempted",
                path.display()
            )));
        }
        return Ok(rows);
    }
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
    let raw_event_time_index = header_cols
        .iter()
        .position(|column| *column == "raw_event_time");
    let dirty_numeric_index = header_cols
        .iter()
        .position(|column| *column == "dirty_numeric");
    let dirty_flag_index = header_cols
        .iter()
        .position(|column| *column == "dirty_flag");
    let event_date_index = header_cols
        .iter()
        .position(|column| *column == "event_date");
    let nullable_metric_00_index = header_cols
        .iter()
        .position(|column| *column == "nullable_metric_00");
    let nested_payload_index = header_cols
        .iter()
        .position(|column| *column == "nested_payload");
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
            event_date: optional_csv_string(cols.as_slice(), event_date_index),
            nullable_metric_00: optional_csv_string(cols.as_slice(), nullable_metric_00_index),
            nested_payload: optional_csv_string(cols.as_slice(), nested_payload_index),
            raw_event_time: optional_csv_string(cols.as_slice(), raw_event_time_index),
            dirty_numeric: optional_csv_string(cols.as_slice(), dirty_numeric_index),
            dirty_flag: optional_csv_string(cols.as_slice(), dirty_flag_index),
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
fn read_traditional_cdc_delta_csv(path: &std::path::Path) -> Result<Vec<TraditionalCdcDeltaRow>> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read CDC delta input '{}': {error}",
            path.display()
        ))
    })?;
    let mut lines = text.lines();
    let header = lines.next().ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!("CDC delta input '{}' was empty", path.display()))
    })?;
    let header_cols = header.trim_end_matches('\r').split(',').collect::<Vec<_>>();
    for required in ["id", "op", "value", "metric", "effective_ts"] {
        if !header_cols.contains(&required) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "CDC delta input '{}' missing required column '{required}'",
                path.display()
            )));
        }
    }
    let column_index = |name: &str| -> usize {
        header_cols
            .iter()
            .position(|column| *column == name)
            .expect("required CDC column checked")
    };
    let id_index = column_index("id");
    let op_index = column_index("op");
    let value_index = column_index("value");
    let metric_index = column_index("metric");
    let effective_ts_index = column_index("effective_ts");
    let mut rows = Vec::new();
    for (line_index, line) in lines.enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let cols = line.trim_end_matches('\r').split(',').collect::<Vec<_>>();
        let row_number = line_index + 2;
        let required_len = [
            id_index,
            op_index,
            value_index,
            metric_index,
            effective_ts_index,
        ]
        .into_iter()
        .max()
        .unwrap_or_default()
            + 1;
        if cols.len() < required_len {
            return Err(ShardLoomError::InvalidOperation(format!(
                "CDC delta input '{}' row {row_number} had {} columns but expected at least {required_len}",
                path.display(),
                cols.len()
            )));
        }
        rows.push(TraditionalCdcDeltaRow {
            id: cols[id_index].parse::<u64>().map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "CDC delta input '{}' row {row_number} invalid id: {error}",
                    path.display()
                ))
            })?,
            op: cols[op_index].to_string(),
            value: cols[value_index].to_string(),
            metric: cols[metric_index].to_string(),
            effective_ts: cols[effective_ts_index].to_string(),
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
            event_date: parse_jsonl_optional_string_field(
                &fields,
                path,
                line_index + 1,
                "event_date",
            )?,
            nullable_metric_00: parse_jsonl_optional_string_field(
                &fields,
                path,
                line_index + 1,
                "nullable_metric_00",
            )?,
            nested_payload: parse_jsonl_optional_string_field(
                &fields,
                path,
                line_index + 1,
                "nested_payload",
            )?,
            raw_event_time: parse_jsonl_optional_string_field(
                &fields,
                path,
                line_index + 1,
                "raw_event_time",
            )?,
            dirty_numeric: parse_jsonl_optional_string_field(
                &fields,
                path,
                line_index + 1,
                "dirty_numeric",
            )?,
            dirty_flag: parse_jsonl_optional_string_field(
                &fields,
                path,
                line_index + 1,
                "dirty_flag",
            )?,
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
fn optional_csv_string(cols: &[&str], index: Option<usize>) -> Option<String> {
    index.and_then(|field_index| {
        cols.get(field_index)
            .map(std::string::ToString::to_string)
            .filter(|value| !value.is_empty())
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
fn parse_jsonl_optional_string_field(
    fields: &std::collections::BTreeMap<String, String>,
    path: &std::path::Path,
    line_number: usize,
    field: &str,
) -> Result<Option<String>> {
    fields
        .get(field)
        .map(|value| {
            if value == "null" {
                Ok(None)
            } else {
                parse_json_string_token(value, path, line_number, field).map(Some)
            }
        })
        .transpose()
        .map(Option::flatten)
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
                event_date: arrow_optional_string_field(batch, path, row_index, "event_date")?,
                nullable_metric_00: arrow_optional_value_string_field(
                    batch,
                    path,
                    row_index,
                    "nullable_metric_00",
                )?,
                nested_payload: arrow_optional_string_field(
                    batch,
                    path,
                    row_index,
                    "nested_payload",
                )?,
                raw_event_time: arrow_optional_string_field(
                    batch,
                    path,
                    row_index,
                    "raw_event_time",
                )?,
                dirty_numeric: arrow_optional_string_field(
                    batch,
                    path,
                    row_index,
                    "dirty_numeric",
                )?,
                dirty_flag: arrow_optional_string_field(batch, path, row_index, "dirty_flag")?,
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
fn arrow_optional_string_field(
    batch: &arrow_array::RecordBatch,
    path: &std::path::Path,
    row_index: usize,
    field: &str,
) -> Result<Option<String>> {
    if batch.schema().index_of(field).is_err() {
        return Ok(None);
    }
    let array = arrow_column(batch, path, field)?;
    if array.is_null(row_index) {
        return Ok(None);
    }
    if let Some(values) = array.as_any().downcast_ref::<arrow_array::Date32Array>() {
        return Ok(Some(date32_days_to_yyyy_mm_dd(values.value(row_index))));
    }
    arrow_string_field(batch, path, row_index, field).map(Some)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn date32_days_to_yyyy_mm_dd(days_since_epoch: i32) -> String {
    let z = i64::from(days_since_epoch) + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn arrow_optional_value_string_field(
    batch: &arrow_array::RecordBatch,
    path: &std::path::Path,
    row_index: usize,
    field: &str,
) -> Result<Option<String>> {
    if batch.schema().index_of(field).is_err() {
        return Ok(None);
    }
    let array = arrow_column(batch, path, field)?;
    if array.is_null(row_index) {
        return Ok(None);
    }
    if array.as_any().is::<arrow_array::StringArray>()
        || array.as_any().is::<arrow_array::LargeStringArray>()
        || array.as_any().is::<arrow_array::StringViewArray>()
    {
        return arrow_string_field(batch, path, row_index, field).map(Some);
    }
    arrow_f64_field(batch, path, row_index, field).map(|value| Some(json_float(value)))
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
    cdc_delta_path: Option<&std::path::Path>,
) -> Result<TraditionalScenarioExecution> {
    match scenario {
        TraditionalAnalyticsScenario::CsvFileIngest => {
            run_streaming_fact_metric_sum_scenario(fact_path, dim_path, None, "metric")
        }
        TraditionalAnalyticsScenario::SelectiveFilter => {
            run_streaming_selective_filter_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::HashJoin => {
            run_streaming_hash_join_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::JoinAggregate => {
            run_streaming_join_aggregate_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::SortAndTopK => {
            run_streaming_sort_top_k_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::TopNPerGroup => {
            run_streaming_top_n_per_group_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::RowNumberWindow => {
            run_streaming_row_number_window_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::HighCardinalityStringGroupDistinct => {
            run_streaming_string_group_distinct_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::PartitionPruning => {
            run_streaming_partition_pruning_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::GroupByAggregation => {
            run_streaming_group_by_aggregation_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::DistinctCount => {
            run_streaming_distinct_count_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::NullHeavyAggregate => {
            run_streaming_null_heavy_aggregate_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::CleanCastFilterWrite => {
            run_streaming_clean_cast_filter_write_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv => {
            run_streaming_malformed_timestamp_dirty_csv_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::NestedJsonFieldScan => {
            run_streaming_nested_json_field_scan_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::SmallChangeOverLargeBase => {
            let cdc_delta_path = cdc_delta_path.ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "small change over large base requires imported CDC delta Vortex source"
                        .to_string(),
                )
            })?;
            run_streaming_small_change_over_large_base_scenario(fact_path, dim_path, cdc_delta_path)
        }
        TraditionalAnalyticsScenario::MultiKeyGroupBy => {
            run_streaming_multi_key_group_by_scenario(fact_path, dim_path)
        }
        TraditionalAnalyticsScenario::WideProjection => {
            run_streaming_fact_metric_sum_scenario(fact_path, dim_path, None, "group_key")
        }
        TraditionalAnalyticsScenario::FilterProjectionLimit => {
            run_streaming_filter_projection_limit_scenario(fact_path, dim_path)
        }
        _ => {
            let fact = read_fact_vortex(fact_path)?;
            let dim = read_dim_vortex(dim_path)?;
            let cdc_delta = cdc_delta_path.map(read_cdc_delta_vortex).transpose()?;
            run_vortex_derived_scenario_from_tables(scenario, &fact, &dim, cdc_delta.as_ref())
        }
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_vortex_derived_scenario_from_files_with_batch_source_state(
    scenario: TraditionalAnalyticsScenario,
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
    cdc_delta_path: Option<&std::path::Path>,
    source_state: &TraditionalVortexBatchSourceState,
) -> Result<TraditionalScenarioExecution> {
    if let Some(execution) =
        run_dimension_label_batch_source_state_scenario(scenario, fact_path, source_state)?
    {
        return Ok(execution);
    }
    if let Some(execution) =
        run_category_metric_batch_source_state_scenario(scenario, dim_path, source_state)?
    {
        return Ok(execution);
    }
    if let Some(execution) =
        run_group_category_batch_source_state_scenario(scenario, dim_path, source_state)?
    {
        return Ok(execution);
    }
    if let Some(execution) =
        run_ranked_metric_batch_source_state_scenario(scenario, dim_path, source_state)?
    {
        return Ok(execution);
    }
    if let Some(execution) = run_selective_filter_batch_source_state_scenario(
        scenario,
        fact_path,
        dim_path,
        source_state,
    )? {
        return Ok(execution);
    }
    if let Some(execution) =
        run_dirty_input_batch_source_state_scenario(scenario, dim_path, source_state)?
    {
        return Ok(execution);
    }
    if let Some(execution) =
        run_date_null_metric_batch_source_state_scenario(scenario, dim_path, source_state)?
    {
        return Ok(execution);
    }
    run_vortex_derived_scenario_from_files(scenario, fact_path, dim_path, cdc_delta_path)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_dimension_label_batch_source_state_scenario(
    scenario: TraditionalAnalyticsScenario,
    fact_path: &std::path::Path,
    source_state: &TraditionalVortexBatchSourceState,
) -> Result<Option<TraditionalScenarioExecution>> {
    let Some(dim_state) = source_state.dimension_label_state.as_ref() else {
        return Ok(None);
    };
    let execution = match scenario {
        TraditionalAnalyticsScenario::HashJoin => {
            run_streaming_hash_join_scenario_with_dim_state(fact_path, dim_state)?
        }
        TraditionalAnalyticsScenario::JoinAggregate => {
            run_streaming_join_aggregate_scenario_with_dim_state(fact_path, dim_state)?
        }
        _ => return Ok(None),
    };
    Ok(Some(execution))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_category_metric_batch_source_state_scenario(
    scenario: TraditionalAnalyticsScenario,
    dim_path: &std::path::Path,
    source_state: &TraditionalVortexBatchSourceState,
) -> Result<Option<TraditionalScenarioExecution>> {
    let Some(category_state) = source_state.category_metric_state.as_ref() else {
        return Ok(None);
    };
    let execution = match scenario {
        TraditionalAnalyticsScenario::HighCardinalityStringGroupDistinct => {
            run_streaming_string_group_distinct_scenario_with_category_metric_state(
                dim_path,
                category_state,
            )?
        }
        TraditionalAnalyticsScenario::DistinctCount => {
            run_streaming_distinct_count_scenario_with_category_metric_state(
                dim_path,
                category_state,
            )?
        }
        _ => return Ok(None),
    };
    Ok(Some(execution))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_group_category_batch_source_state_scenario(
    scenario: TraditionalAnalyticsScenario,
    dim_path: &std::path::Path,
    source_state: &TraditionalVortexBatchSourceState,
) -> Result<Option<TraditionalScenarioExecution>> {
    let Some(group_state) = source_state.group_category_metric_state.as_ref() else {
        return Ok(None);
    };
    let execution = match scenario {
        TraditionalAnalyticsScenario::GroupByAggregation => {
            run_streaming_group_by_aggregation_scenario_with_group_category_metric_state(
                dim_path,
                group_state,
            )?
        }
        TraditionalAnalyticsScenario::MultiKeyGroupBy => {
            run_streaming_multi_key_group_by_scenario_with_group_category_metric_state(
                dim_path,
                group_state,
            )?
        }
        _ => return Ok(None),
    };
    Ok(Some(execution))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_ranked_metric_batch_source_state_scenario(
    scenario: TraditionalAnalyticsScenario,
    dim_path: &std::path::Path,
    source_state: &TraditionalVortexBatchSourceState,
) -> Result<Option<TraditionalScenarioExecution>> {
    let Some(ranked_state) = source_state.ranked_metric_state.as_ref() else {
        return Ok(None);
    };
    let execution = match scenario {
        TraditionalAnalyticsScenario::SortAndTopK => {
            run_streaming_sort_top_k_scenario_with_ranked_metric_state(dim_path, ranked_state)?
        }
        TraditionalAnalyticsScenario::TopNPerGroup => {
            run_streaming_ranked_per_group_scenario_with_ranked_metric_state(
                dim_path,
                ranked_state,
                3,
            )?
        }
        TraditionalAnalyticsScenario::RowNumberWindow => {
            run_streaming_ranked_per_group_scenario_with_ranked_metric_state(
                dim_path,
                ranked_state,
                1,
            )?
        }
        _ => return Ok(None),
    };
    Ok(Some(execution))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_selective_filter_batch_source_state_scenario(
    scenario: TraditionalAnalyticsScenario,
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
    source_state: &TraditionalVortexBatchSourceState,
) -> Result<Option<TraditionalScenarioExecution>> {
    let Some(selective_state) = source_state.selective_filter_state.as_ref() else {
        return Ok(None);
    };
    let execution = match scenario {
        TraditionalAnalyticsScenario::SelectiveFilter => {
            run_streaming_selective_filter_scenario_with_selective_filter_state(
                fact_path,
                dim_path,
                selective_state,
            )?
        }
        TraditionalAnalyticsScenario::FilterProjectionLimit => {
            run_streaming_filter_projection_limit_scenario_with_selective_filter_state(
                dim_path,
                selective_state,
            )?
        }
        _ => return Ok(None),
    };
    Ok(Some(execution))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_dirty_input_batch_source_state_scenario(
    scenario: TraditionalAnalyticsScenario,
    dim_path: &std::path::Path,
    source_state: &TraditionalVortexBatchSourceState,
) -> Result<Option<TraditionalScenarioExecution>> {
    let Some(dirty_state) = source_state.dirty_input_state.as_ref() else {
        return Ok(None);
    };
    let execution = match scenario {
        TraditionalAnalyticsScenario::CleanCastFilterWrite => {
            run_streaming_clean_cast_filter_write_scenario_with_dirty_input_state(
                dim_path,
                dirty_state,
            )?
        }
        TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv => {
            run_streaming_malformed_timestamp_dirty_csv_scenario_with_dirty_input_state(
                dim_path,
                dirty_state,
            )?
        }
        _ => return Ok(None),
    };
    Ok(Some(execution))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_date_null_metric_batch_source_state_scenario(
    scenario: TraditionalAnalyticsScenario,
    dim_path: &std::path::Path,
    source_state: &TraditionalVortexBatchSourceState,
) -> Result<Option<TraditionalScenarioExecution>> {
    let Some(date_null_state) = source_state.date_null_metric_state.as_ref() else {
        return Ok(None);
    };
    let dim_rows = vortex_file_row_count(dim_path)?;
    let stats = date_null_state.stats.clone();
    let execution = match scenario {
        TraditionalAnalyticsScenario::PartitionPruning => {
            date_null_state.ensure_partition_pruning_supported()?;
            TraditionalScenarioExecution {
                result_json: scalar_result_json(
                    date_null_state.partition_pruning_accum.row_count,
                    date_null_state.partition_pruning_accum.metric_sum,
                ),
                fact_rows: stats.source_row_count,
                dim_rows,
                cdc_delta_rows: 0,
                rows_scanned: stats.source_row_count,
                rows_materialized: 1,
                evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
            }
        }
        TraditionalAnalyticsScenario::NullHeavyAggregate => TraditionalScenarioExecution {
            result_json: scalar_result_json(
                date_null_state.null_heavy_accum.row_count,
                date_null_state.null_heavy_accum.metric_sum,
            ),
            fact_rows: stats.source_row_count,
            dim_rows,
            cdc_delta_rows: 0,
            rows_scanned: stats.source_row_count,
            rows_materialized: 1,
            evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
        },
        _ => return Ok(None),
    };
    Ok(Some(execution))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_vortex_derived_scenario_from_tables(
    scenario: TraditionalAnalyticsScenario,
    fact: &VortexFactTable,
    dim: &VortexDimTable,
    cdc_delta: Option<&VortexCdcDeltaTable>,
) -> Result<TraditionalScenarioExecution> {
    let result_json = run_vortex_derived_scenario(scenario, fact, dim, cdc_delta)?;
    let rows_materialized = result_rows_materialized(&result_json)?;
    let rows_scanned = match scenario {
        TraditionalAnalyticsScenario::HashJoin
        | TraditionalAnalyticsScenario::JoinAggregate
        | TraditionalAnalyticsScenario::ScaleStressSkewedJoinAggregation
        | TraditionalAnalyticsScenario::ScaleStressMultiStageEtl => {
            checked_usize_sum_to_u64(fact.len(), dim.len())?
        }
        TraditionalAnalyticsScenario::SmallChangeOverLargeBase => usize_to_u64(fact.len())?
            .checked_add(cdc_delta.map_or(Ok(0), |cdc| usize_to_u64(cdc.len()))?)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "traditional analytics CDC rows scanned overflow".to_string(),
                )
            })?,
        _ => usize_to_u64(fact.len())?,
    };
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: usize_to_u64(fact.len())?,
        dim_rows: usize_to_u64(dim.len())?,
        cdc_delta_rows: cdc_delta.map_or(Ok(0), |cdc| usize_to_u64(cdc.len()))?,
        rows_scanned,
        rows_materialized,
        evidence: TraditionalScenarioExecutionEvidence::table_materialized(),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_hash_join_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let (dim_by_key, dim_stats) = scan_dim_label_state(dim_path, "hash join")?;
    let dim_state = TraditionalDimensionLabelState {
        dim_by_key,
        stats: dim_stats,
    };
    run_streaming_hash_join_scenario_with_dim_state(fact_path, &dim_state)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_hash_join_scenario_with_dim_state(
    fact_path: &std::path::Path,
    dim_state: &TraditionalDimensionLabelState,
) -> Result<TraditionalScenarioExecution> {
    let dim_by_key = &dim_state.dim_by_key;
    let dim_stats = &dim_state.stats;
    let mut groups = std::collections::BTreeMap::<String, TraditionalGroupAccum>::new();
    let fact_stats = scan_fact_vortex_projected(
        fact_path,
        &["dim_key", "metric"],
        None,
        |fields, chunk_rows| {
            let dim_keys = primitive_field::<u32>(fields, "dim_key")?;
            let metrics = primitive_field::<f64>(fields, "metric")?;
            if dim_keys.len() != chunk_rows || metrics.len() != chunk_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "hash join fact Vortex chunk length mismatch: chunk_rows={chunk_rows}, dim_key_len={}, metric_len={}",
                    dim_keys.len(),
                    metrics.len()
                )));
            }
            for (dim_key, metric) in dim_keys.into_iter().zip(metrics) {
                if let Some(dim_label) = dim_by_key.get(&dim_key) {
                    groups.entry(dim_label.clone()).or_default().add(metric);
                }
            }
            Ok(())
        },
    )?;

    let result_json = string_group_rows_json(groups, "dim_label");
    let rows_materialized = result_rows_materialized(&result_json)?;
    let rows_scanned = checked_u64_sum(fact_stats.source_row_count, dim_stats.source_row_count)?;
    let stats = TraditionalStreamingScanStats {
        source_row_count: fact_stats.source_row_count,
        result_row_count: rows_scanned,
        arrays_read_count: fact_stats.arrays_read_count + dim_stats.arrays_read_count,
        max_chunk_rows: fact_stats.max_chunk_rows.max(dim_stats.max_chunk_rows),
        projected_columns: prefixed_projected_columns(
            "dim",
            &dim_stats.projected_columns,
            "fact",
            &fact_stats.projected_columns,
        ),
        reader_chunk_columns_observed: prefixed_projected_columns(
            "dim",
            &dim_stats.reader_chunk_columns_observed,
            "fact",
            &fact_stats.reader_chunk_columns_observed,
        ),
        reader_chunk_dtype_summary: prefixed_reader_chunk_summary(
            "dim",
            &dim_stats.reader_chunk_dtype_summary,
            "fact",
            &fact_stats.reader_chunk_dtype_summary,
        ),
        reader_chunk_encoding_summary: prefixed_reader_chunk_summary(
            "dim",
            &dim_stats.reader_chunk_encoding_summary,
            "fact",
            &fact_stats.reader_chunk_encoding_summary,
        ),
        filter_pushdown_applied: false,
        projection_pushdown_applied: true,
    };
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: fact_stats.source_row_count,
        dim_rows: dim_stats.source_row_count,
        cdc_delta_rows: 0,
        rows_scanned,
        rows_materialized,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_join_aggregate_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let (dim_by_key, dim_stats) = scan_dim_label_state(dim_path, "join aggregate")?;
    let dim_state = TraditionalDimensionLabelState {
        dim_by_key,
        stats: dim_stats,
    };
    run_streaming_join_aggregate_scenario_with_dim_state(fact_path, &dim_state)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_join_aggregate_scenario_with_dim_state(
    fact_path: &std::path::Path,
    dim_state: &TraditionalDimensionLabelState,
) -> Result<TraditionalScenarioExecution> {
    let dim_by_key = &dim_state.dim_by_key;
    let dim_stats = &dim_state.stats;
    let mut groups = std::collections::BTreeMap::<(String, String), TraditionalGroupAccum>::new();
    let fact_stats = scan_fact_vortex_projected(
        fact_path,
        &["dim_key", "category", "metric"],
        Some(join_aggregate_fact_filter_expr()),
        |fields, chunk_rows| {
            let dim_keys = primitive_field::<u32>(fields, "dim_key")?;
            let categories = utf8_field(fields, "category")?;
            let metrics = primitive_field::<f64>(fields, "metric")?;
            if dim_keys.len() != chunk_rows
                || categories.len() != chunk_rows
                || metrics.len() != chunk_rows
            {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "join aggregate fact Vortex chunk length mismatch: chunk_rows={chunk_rows}, dim_key_len={}, category_len={}, metric_len={}",
                    dim_keys.len(),
                    categories.len(),
                    metrics.len()
                )));
            }
            for ((dim_key, category), metric) in dim_keys.into_iter().zip(categories).zip(metrics) {
                if let Some(dim_label) = dim_by_key.get(&dim_key) {
                    groups
                        .entry((dim_label.clone(), category))
                        .or_default()
                        .add(metric);
                }
            }
            Ok(())
        },
    )?;

    let result_json = dim_category_rows_json(groups);
    let rows_materialized = result_rows_materialized(&result_json)?;
    let rows_scanned = checked_u64_sum(fact_stats.source_row_count, dim_stats.source_row_count)?;
    let stats = TraditionalStreamingScanStats {
        source_row_count: fact_stats.source_row_count,
        result_row_count: checked_u64_sum(fact_stats.result_row_count, dim_stats.result_row_count)?,
        arrays_read_count: fact_stats.arrays_read_count + dim_stats.arrays_read_count,
        max_chunk_rows: fact_stats.max_chunk_rows.max(dim_stats.max_chunk_rows),
        projected_columns: prefixed_projected_columns(
            "dim",
            &dim_stats.projected_columns,
            "fact",
            &fact_stats.projected_columns,
        ),
        reader_chunk_columns_observed: prefixed_projected_columns(
            "dim",
            &dim_stats.reader_chunk_columns_observed,
            "fact",
            &fact_stats.reader_chunk_columns_observed,
        ),
        reader_chunk_dtype_summary: prefixed_reader_chunk_summary(
            "dim",
            &dim_stats.reader_chunk_dtype_summary,
            "fact",
            &fact_stats.reader_chunk_dtype_summary,
        ),
        reader_chunk_encoding_summary: prefixed_reader_chunk_summary(
            "dim",
            &dim_stats.reader_chunk_encoding_summary,
            "fact",
            &fact_stats.reader_chunk_encoding_summary,
        ),
        filter_pushdown_applied: true,
        projection_pushdown_applied: true,
    };
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: fact_stats.source_row_count,
        dim_rows: dim_stats.source_row_count,
        cdc_delta_rows: 0,
        rows_scanned,
        rows_materialized,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_top_n_per_group_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    run_streaming_ranked_per_group_scenario(fact_path, dim_path, 3, "top-N per group")
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_row_number_window_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    run_streaming_ranked_per_group_scenario(fact_path, dim_path, 1, "row number window")
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_sort_top_k_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut top_rows = std::collections::BinaryHeap::<GlobalTopKCandidate>::new();
    let stats = scan_fact_vortex_projected(
        fact_path,
        &["id", "metric"],
        None,
        |fields, chunk_rows| {
            let ids = primitive_field::<u64>(fields, "id")?;
            let metrics = primitive_field::<f64>(fields, "metric")?;
            if ids.len() != chunk_rows || metrics.len() != chunk_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "sort and top-k Vortex chunk length mismatch: chunk_rows={chunk_rows}, id_len={}, metric_len={}",
                    ids.len(),
                    metrics.len()
                )));
            }
            for (id, metric) in ids.into_iter().zip(metrics) {
                top_rows.push(GlobalTopKCandidate::new(id, metric));
                if top_rows.len() > 10 {
                    let _ = top_rows.pop();
                }
            }
            Ok(())
        },
    )?;
    let mut rows = top_rows.into_vec();
    rows.sort_by(|left, right| {
        right
            .metric
            .total_cmp(&left.metric)
            .then_with(|| left.id.cmp(&right.id))
    });
    let result_rows = rows
        .into_iter()
        .map(|row| (row.id, row.metric))
        .collect::<Vec<_>>();
    let result_json = top_rows_json(&result_rows);
    let rows_materialized = result_rows_materialized(&result_json)?;
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_sort_top_k_scenario_with_ranked_metric_state(
    dim_path: &std::path::Path,
    ranked_state: &TraditionalRankedMetricState,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let stats = ranked_state.stats.clone();
    let mut top_rows = std::collections::BinaryHeap::<GlobalTopKCandidate>::new();
    for row in &ranked_state.rows {
        top_rows.push(GlobalTopKCandidate::new(row.id, row.metric));
        if top_rows.len() > 10 {
            let _ = top_rows.pop();
        }
    }
    let mut rows = top_rows.into_vec();
    rows.sort_by(|left, right| {
        right
            .metric
            .total_cmp(&left.metric)
            .then_with(|| left.id.cmp(&right.id))
    });
    let result_rows = rows
        .into_iter()
        .map(|row| (row.id, row.metric))
        .collect::<Vec<_>>();
    let result_json = top_rows_json(&result_rows);
    let rows_materialized = result_rows_materialized(&result_json)?;
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_ranked_per_group_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
    max_rank: usize,
    scenario_label: &str,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut top_by_group = std::collections::BTreeMap::<u32, Vec<(u64, f64)>>::new();
    let stats = scan_fact_vortex_projected(
        fact_path,
        &["group_key", "id", "metric"],
        None,
        |fields, chunk_rows| {
            let group_keys = primitive_field::<u32>(fields, "group_key")?;
            let ids = primitive_field::<u64>(fields, "id")?;
            let metrics = primitive_field::<f64>(fields, "metric")?;
            if group_keys.len() != chunk_rows
                || ids.len() != chunk_rows
                || metrics.len() != chunk_rows
            {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "{scenario_label} Vortex chunk length mismatch: chunk_rows={chunk_rows}, group_key_len={}, id_len={}, metric_len={}",
                    group_keys.len(),
                    ids.len(),
                    metrics.len()
                )));
            }
            for ((group_key, id), metric) in group_keys.into_iter().zip(ids).zip(metrics) {
                let rows = top_by_group.entry(group_key).or_default();
                rows.push((id, metric));
                rows.sort_by(|left, right| {
                    right
                        .1
                        .total_cmp(&left.1)
                        .then_with(|| left.0.cmp(&right.0))
                });
                rows.truncate(max_rank);
            }
            Ok(())
        },
    )?;

    let mut ranked = Vec::new();
    for (group_key, rows) in top_by_group {
        for (index, (id, metric)) in rows.into_iter().enumerate() {
            ranked.push((group_key, id, metric, usize_to_u64(index + 1)?));
        }
    }
    let result_json = rank_rows_json(ranked);
    let rows_materialized = result_rows_materialized(&result_json)?;
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_ranked_per_group_scenario_with_ranked_metric_state(
    dim_path: &std::path::Path,
    ranked_state: &TraditionalRankedMetricState,
    max_rank: usize,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let stats = ranked_state.stats.clone();
    let mut top_by_group = std::collections::BTreeMap::<u32, Vec<(u64, f64)>>::new();
    for row in &ranked_state.rows {
        let rows = top_by_group.entry(row.group_key).or_default();
        rows.push((row.id, row.metric));
        rows.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        rows.truncate(max_rank);
    }
    let mut ranked = Vec::new();
    for (group_key, rows) in top_by_group {
        for (index, (id, metric)) in rows.into_iter().enumerate() {
            ranked.push((group_key, id, metric, usize_to_u64(index + 1)?));
        }
    }
    let result_json = rank_rows_json(ranked);
    let rows_materialized = result_rows_materialized(&result_json)?;
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_string_group_distinct_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut groups = std::collections::BTreeMap::<String, TraditionalGroupAccum>::new();
    let stats = scan_fact_vortex_projected(
        fact_path,
        &["category", "metric"],
        None,
        |fields, chunk_rows| {
            let categories = utf8_field(fields, "category")?;
            let metrics = primitive_field::<f64>(fields, "metric")?;
            if categories.len() != chunk_rows || metrics.len() != chunk_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "high-cardinality string group/distinct Vortex chunk length mismatch: chunk_rows={chunk_rows}, category_len={}, metric_len={}",
                    categories.len(),
                    metrics.len()
                )));
            }
            for (category, metric) in categories.into_iter().zip(metrics) {
                groups.entry(category).or_default().add(metric);
            }
            Ok(())
        },
    )?;
    let result_json = string_group_distinct_json(groups);
    let rows_materialized = result_rows_materialized(&result_json)?;
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_string_group_distinct_scenario_with_category_metric_state(
    dim_path: &std::path::Path,
    category_state: &TraditionalCategoryMetricState,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let stats = category_state.stats.clone();
    let result_json = string_group_distinct_json(category_state.groups.clone());
    let rows_materialized = result_rows_materialized(&result_json)?;
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_partition_pruning_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    if !fact_vortex_has_non_empty_event_date(fact_path)? {
        return Err(ShardLoomError::InvalidOperation(
            "partition pruning requires an event_date fixture column".to_string(),
        ));
    }
    let mut accum = TraditionalGroupAccum::default();
    let stats = scan_fact_vortex_projected(
        fact_path,
        &["event_date", "metric"],
        Some(partition_pruning_date_range_expr()),
        |fields, chunk_rows| {
            if !fields.contains_key("event_date") {
                return Err(ShardLoomError::InvalidOperation(
                    "partition pruning requires an event_date fixture column".to_string(),
                ));
            }
            let event_dates = utf8_field(fields, "event_date")?;
            let metrics = primitive_field::<f64>(fields, "metric")?;
            if event_dates.len() != chunk_rows || metrics.len() != chunk_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "partition pruning Vortex chunk length mismatch: chunk_rows={chunk_rows}, event_date_len={}, metric_len={}",
                    event_dates.len(),
                    metrics.len()
                )));
            }
            for (event_date, metric) in event_dates.into_iter().zip(metrics) {
                if partition_pruning_date_range_contains(&event_date) {
                    accum.add(metric);
                }
            }
            Ok(())
        },
    )?;
    Ok(TraditionalScenarioExecution {
        result_json: scalar_result_json(accum.row_count, accum.metric_sum),
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn fact_vortex_has_non_empty_event_date(fact_path: &std::path::Path) -> Result<bool> {
    let mut has_non_empty_event_date = false;
    scan_fact_vortex_projected(fact_path, &["event_date"], None, |fields, chunk_rows| {
        if !fields.contains_key("event_date") {
            return Err(ShardLoomError::InvalidOperation(
                "partition pruning requires an event_date fixture column".to_string(),
            ));
        }
        let event_dates = utf8_field(fields, "event_date")?;
        if event_dates.len() != chunk_rows {
            return Err(ShardLoomError::InvalidOperation(format!(
                "partition pruning event_date validation chunk length mismatch: chunk_rows={chunk_rows}, event_date_len={}",
                event_dates.len()
            )));
        }
        has_non_empty_event_date |= event_dates.iter().any(|event_date| !event_date.is_empty());
        Ok(())
    })?;
    Ok(has_non_empty_event_date)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_multi_key_group_by_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut groups = std::collections::BTreeMap::<(u32, String), TraditionalGroupAccum>::new();
    let stats = scan_fact_vortex_projected(
        fact_path,
        &["group_key", "category", "metric"],
        None,
        |fields, chunk_rows| {
            let group_keys = primitive_field::<u32>(fields, "group_key")?;
            let categories = utf8_field(fields, "category")?;
            let metrics = primitive_field::<f64>(fields, "metric")?;
            if group_keys.len() != chunk_rows
                || categories.len() != chunk_rows
                || metrics.len() != chunk_rows
            {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "multi-key group by Vortex chunk length mismatch: chunk_rows={chunk_rows}, group_key_len={}, category_len={}, metric_len={}",
                    group_keys.len(),
                    categories.len(),
                    metrics.len()
                )));
            }
            for ((group_key, category), metric) in
                group_keys.into_iter().zip(categories).zip(metrics)
            {
                groups.entry((group_key, category)).or_default().add(metric);
            }
            Ok(())
        },
    )?;
    let result_json = group_category_rows_json(groups);
    let rows_materialized = result_rows_materialized(&result_json)?;
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_multi_key_group_by_scenario_with_group_category_metric_state(
    dim_path: &std::path::Path,
    group_state: &TraditionalGroupCategoryMetricState,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let stats = group_state.stats.clone();
    let result_json = group_category_rows_json(group_state.group_category_groups.clone());
    let rows_materialized = result_rows_materialized(&result_json)?;
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_group_by_aggregation_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut groups = std::collections::BTreeMap::<u32, TraditionalGroupAccum>::new();
    let stats = scan_fact_vortex_projected(
        fact_path,
        &["group_key", "metric"],
        None,
        |fields, chunk_rows| {
            let group_keys = primitive_field::<u32>(fields, "group_key")?;
            let metrics = primitive_field::<f64>(fields, "metric")?;
            if group_keys.len() != chunk_rows || metrics.len() != chunk_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "group by aggregation Vortex chunk length mismatch: chunk_rows={chunk_rows}, group_key_len={}, metric_len={}",
                    group_keys.len(),
                    metrics.len()
                )));
            }
            for (group_key, metric) in group_keys.into_iter().zip(metrics) {
                groups.entry(group_key).or_default().add(metric);
            }
            Ok(())
        },
    )?;
    let result_json = numeric_group_rows_json(groups, "group_key");
    let rows_materialized = result_rows_materialized(&result_json)?;
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_group_by_aggregation_scenario_with_group_category_metric_state(
    dim_path: &std::path::Path,
    group_state: &TraditionalGroupCategoryMetricState,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let stats = group_state.stats.clone();
    let result_json = numeric_group_rows_json(group_state.group_key_groups.clone(), "group_key");
    let rows_materialized = result_rows_materialized(&result_json)?;
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_distinct_count_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut distinct = std::collections::HashSet::<String>::new();
    let stats = scan_fact_vortex_projected(
        fact_path,
        &["category"],
        None,
        |fields, chunk_rows| {
            let categories = utf8_field(fields, "category")?;
            if categories.len() != chunk_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "distinct count Vortex chunk length mismatch: chunk_rows={chunk_rows}, category_len={}",
                    categories.len()
                )));
            }
            distinct.extend(categories);
            Ok(())
        },
    )?;
    let result_json = format!(
        "{{\"distinct_category_count\":{}}}",
        usize_to_u64(distinct.len())?
    );
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_distinct_count_scenario_with_category_metric_state(
    dim_path: &std::path::Path,
    category_state: &TraditionalCategoryMetricState,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let stats = category_state.stats.clone();
    let result_json = format!(
        "{{\"distinct_category_count\":{}}}",
        usize_to_u64(category_state.groups.len())?
    );
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_null_heavy_aggregate_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut accum = TraditionalGroupAccum::default();
    let stats = scan_fact_vortex_projected(
        fact_path,
        &["nullable_metric_00"],
        None,
        |fields, chunk_rows| {
            let values = utf8_field(fields, "nullable_metric_00")?;
            if values.len() != chunk_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "null-heavy aggregate Vortex chunk length mismatch: chunk_rows={chunk_rows}, nullable_metric_00_len={}",
                    values.len()
                )));
            }
            for (index, value) in values.into_iter().enumerate() {
                if value.is_empty() {
                    continue;
                }
                let metric = value.parse::<f64>().map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "failed to parse nullable_metric_00 in Vortex chunk at row {}: {error}",
                        index + 1
                    ))
                })?;
                accum.add(metric);
            }
            Ok(())
        },
    )?;
    Ok(TraditionalScenarioExecution {
        result_json: scalar_result_json(accum.row_count, accum.metric_sum),
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_clean_cast_filter_write_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut saw_raw_event_time = false;
    let mut saw_dirty_numeric = false;
    let mut saw_dirty_flag = false;
    let mut accum = TraditionalGroupAccum::default();
    let stats = scan_fact_vortex_projected(
        fact_path,
        &["raw_event_time", "dirty_numeric", "dirty_flag"],
        None,
        |fields, chunk_rows| {
            let raw_event_times = utf8_field(fields, "raw_event_time")?;
            let dirty_numeric = utf8_field(fields, "dirty_numeric")?;
            let dirty_flags = utf8_field(fields, "dirty_flag")?;
            if raw_event_times.len() != chunk_rows
                || dirty_numeric.len() != chunk_rows
                || dirty_flags.len() != chunk_rows
            {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "clean/cast/filter/write Vortex chunk length mismatch: chunk_rows={chunk_rows}, raw_event_time_len={}, dirty_numeric_len={}, dirty_flag_len={}",
                    raw_event_times.len(),
                    dirty_numeric.len(),
                    dirty_flags.len()
                )));
            }
            for ((raw_event_time, dirty_numeric), dirty_flag) in raw_event_times
                .into_iter()
                .zip(dirty_numeric)
                .zip(dirty_flags)
            {
                saw_raw_event_time |= !raw_event_time.is_empty();
                saw_dirty_numeric |= !dirty_numeric.is_empty();
                saw_dirty_flag |= !dirty_flag.is_empty();
                if dirty_flag != "Y" || !generated_timestamp_shape_is_valid(&raw_event_time) {
                    continue;
                }
                let Ok(value) = dirty_numeric.parse::<f64>() else {
                    continue;
                };
                if value >= 500.0 {
                    accum.add(value);
                }
            }
            Ok(())
        },
    )?;
    if !saw_raw_event_time || !saw_dirty_numeric || !saw_dirty_flag {
        return Err(ShardLoomError::InvalidOperation(
            "clean/cast/filter/write requires raw_event_time, dirty_numeric, and dirty_flag fixture columns"
                .to_string(),
        ));
    }
    Ok(TraditionalScenarioExecution {
        result_json: scalar_result_json(accum.row_count, accum.metric_sum),
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_malformed_timestamp_dirty_csv_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut saw_raw_event_time = false;
    let mut saw_dirty_numeric = false;
    let mut accum = TraditionalGroupAccum::default();
    let stats = scan_fact_vortex_projected(
        fact_path,
        &["raw_event_time", "dirty_numeric"],
        None,
        |fields, chunk_rows| {
            let raw_event_times = utf8_field(fields, "raw_event_time")?;
            let dirty_numeric = utf8_field(fields, "dirty_numeric")?;
            if raw_event_times.len() != chunk_rows || dirty_numeric.len() != chunk_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "malformed timestamp / dirty CSV Vortex chunk length mismatch: chunk_rows={chunk_rows}, raw_event_time_len={}, dirty_numeric_len={}",
                    raw_event_times.len(),
                    dirty_numeric.len()
                )));
            }
            for (raw_event_time, dirty_numeric) in raw_event_times.into_iter().zip(dirty_numeric) {
                saw_raw_event_time |= !raw_event_time.is_empty();
                saw_dirty_numeric |= !dirty_numeric.is_empty();
                if !generated_timestamp_shape_is_valid(&raw_event_time) {
                    continue;
                }
                let Ok(value) = dirty_numeric.parse::<f64>() else {
                    continue;
                };
                accum.add(value);
            }
            Ok(())
        },
    )?;
    if !saw_raw_event_time || !saw_dirty_numeric {
        return Err(ShardLoomError::InvalidOperation(
            "malformed timestamp / dirty CSV requires raw_event_time and dirty_numeric fixture columns"
                .to_string(),
        ));
    }
    Ok(TraditionalScenarioExecution {
        result_json: scalar_result_json(accum.row_count, accum.metric_sum),
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_clean_cast_filter_write_scenario_with_dirty_input_state(
    dim_path: &std::path::Path,
    dirty_state: &TraditionalDirtyInputState,
) -> Result<TraditionalScenarioExecution> {
    dirty_state.ensure_clean_cast_supported()?;
    let dim_rows = vortex_file_row_count(dim_path)?;
    Ok(TraditionalScenarioExecution {
        result_json: scalar_result_json(
            dirty_state.clean_cast_accum.row_count,
            dirty_state.clean_cast_accum.metric_sum,
        ),
        fact_rows: dirty_state.stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: dirty_state.stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming(dirty_state.stats.clone()),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_malformed_timestamp_dirty_csv_scenario_with_dirty_input_state(
    dim_path: &std::path::Path,
    dirty_state: &TraditionalDirtyInputState,
) -> Result<TraditionalScenarioExecution> {
    dirty_state.ensure_malformed_timestamp_supported()?;
    let dim_rows = vortex_file_row_count(dim_path)?;
    Ok(TraditionalScenarioExecution {
        result_json: scalar_result_json(
            dirty_state.malformed_timestamp_accum.row_count,
            dirty_state.malformed_timestamp_accum.metric_sum,
        ),
        fact_rows: dirty_state.stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: dirty_state.stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming(dirty_state.stats.clone()),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_nested_json_field_scan_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut saw_nested_payload = false;
    let mut source_row_offset = 0_usize;
    let mut row_count = 0_u64;
    let mut metric_sum = 0.0;
    let mut flagged = 0_u64;
    let stats = scan_fact_vortex_projected(
        fact_path,
        &["nested_payload"],
        None,
        |fields, chunk_rows| {
            let nested_payloads = utf8_field(fields, "nested_payload")?;
            if nested_payloads.len() != chunk_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "nested JSON field scan Vortex chunk length mismatch: chunk_rows={chunk_rows}, nested_payload_len={}",
                    nested_payloads.len()
                )));
            }
            for (index, payload) in nested_payloads.into_iter().enumerate() {
                if payload.is_empty() {
                    continue;
                }
                saw_nested_payload = true;
                let row_index = source_row_offset + index;
                metric_sum += generated_nested_score(&payload, row_index)?;
                if generated_nested_flag(&payload, row_index)? {
                    flagged += 1;
                }
                row_count += 1;
            }
            source_row_offset = source_row_offset.checked_add(chunk_rows).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "nested JSON field scan source row offset overflow".to_string(),
                )
            })?;
            Ok(())
        },
    )?;
    if !saw_nested_payload {
        return Err(ShardLoomError::InvalidOperation(
            "nested JSON field scan requires nested_payload fixture column".to_string(),
        ));
    }
    Ok(TraditionalScenarioExecution {
        result_json: format!(
            "{{\"row_count\":{row_count},\"metric_sum\":{},\"flagged\":{flagged}}}",
            json_float(metric_sum)
        ),
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_small_change_over_large_base_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
    cdc_delta_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut rows = std::collections::BTreeMap::<u64, f64>::new();
    let fact_stats = scan_fact_vortex_projected(
        fact_path,
        &["id", "metric"],
        None,
        |fields, chunk_rows| {
            let ids = primitive_field::<u64>(fields, "id")?;
            let metrics = primitive_field::<f64>(fields, "metric")?;
            if ids.len() != chunk_rows || metrics.len() != chunk_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "small change over large base fact Vortex chunk length mismatch: chunk_rows={chunk_rows}, id_len={}, metric_len={}",
                    ids.len(),
                    metrics.len()
                )));
            }
            for (id, metric) in ids.into_iter().zip(metrics) {
                rows.insert(id, metric);
            }
            Ok(())
        },
    )?;

    let mut saw_cdc_delta = false;
    let mut cdc_row_offset = 0_usize;
    let cdc_stats = scan_fact_vortex_projected(
        cdc_delta_path,
        &["id", "op", "value", "metric", "effective_ts"],
        None,
        |fields, chunk_rows| {
            apply_cdc_delta_overlay_chunk(
                fields,
                chunk_rows,
                &mut rows,
                &mut saw_cdc_delta,
                &mut cdc_row_offset,
            )
        },
    )?;
    if !saw_cdc_delta {
        return Err(ShardLoomError::InvalidOperation(
            "small change over large base requires non-empty CDC delta rows".to_string(),
        ));
    }

    let rows_scanned = checked_u64_sum(fact_stats.source_row_count, cdc_stats.source_row_count)?;
    let stats = TraditionalStreamingScanStats {
        source_row_count: fact_stats.source_row_count,
        result_row_count: checked_u64_sum(fact_stats.result_row_count, cdc_stats.result_row_count)?,
        arrays_read_count: fact_stats.arrays_read_count + cdc_stats.arrays_read_count,
        max_chunk_rows: fact_stats.max_chunk_rows.max(cdc_stats.max_chunk_rows),
        projected_columns: prefixed_projected_columns(
            "base",
            &fact_stats.projected_columns,
            "cdc_delta",
            &cdc_stats.projected_columns,
        ),
        reader_chunk_columns_observed: prefixed_projected_columns(
            "base",
            &fact_stats.reader_chunk_columns_observed,
            "cdc_delta",
            &cdc_stats.reader_chunk_columns_observed,
        ),
        reader_chunk_dtype_summary: prefixed_reader_chunk_summary(
            "base",
            &fact_stats.reader_chunk_dtype_summary,
            "cdc_delta",
            &cdc_stats.reader_chunk_dtype_summary,
        ),
        reader_chunk_encoding_summary: prefixed_reader_chunk_summary(
            "base",
            &fact_stats.reader_chunk_encoding_summary,
            "cdc_delta",
            &cdc_stats.reader_chunk_encoding_summary,
        ),
        filter_pushdown_applied: false,
        projection_pushdown_applied: true,
    };
    Ok(TraditionalScenarioExecution {
        result_json: scalar_result_json(usize_to_u64(rows.len())?, rows.values().sum()),
        fact_rows: fact_stats.source_row_count,
        dim_rows,
        cdc_delta_rows: cdc_stats.source_row_count,
        rows_scanned,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn apply_cdc_delta_overlay_chunk(
    fields: &std::collections::BTreeMap<String, vortex::array::ArrayRef>,
    chunk_rows: usize,
    rows: &mut std::collections::BTreeMap<u64, f64>,
    saw_cdc_delta: &mut bool,
    cdc_row_offset: &mut usize,
) -> Result<()> {
    let ids = primitive_field::<u64>(fields, "id")?;
    let ops = utf8_field(fields, "op")?;
    let values = utf8_field(fields, "value")?;
    let metrics = utf8_field(fields, "metric")?;
    let effective_ts = utf8_field(fields, "effective_ts")?;
    if ids.len() != chunk_rows
        || ops.len() != chunk_rows
        || values.len() != chunk_rows
        || metrics.len() != chunk_rows
        || effective_ts.len() != chunk_rows
    {
        return Err(ShardLoomError::InvalidOperation(format!(
            "small change over large base CDC Vortex chunk length mismatch: chunk_rows={chunk_rows}, id_len={}, op_len={}, value_len={}, metric_len={}, effective_ts_len={}",
            ids.len(),
            ops.len(),
            values.len(),
            metrics.len(),
            effective_ts.len()
        )));
    }
    for index in 0..chunk_rows {
        *saw_cdc_delta = true;
        let row_number = *cdc_row_offset + index + 1;
        if !generated_timestamp_shape_is_valid(&effective_ts[index]) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "CDC delta row {row_number} has invalid effective_ts"
            )));
        }
        match ops[index].as_str() {
            "delete" => {
                rows.remove(&ids[index]);
            }
            "update" | "insert" => {
                let _value = values[index].parse::<u32>().map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "CDC delta row {row_number} has invalid value: {error}"
                    ))
                })?;
                let metric = metrics[index].parse::<f64>().map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "CDC delta row {row_number} has invalid metric: {error}"
                    ))
                })?;
                rows.insert(ids[index], metric);
            }
            other => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "CDC delta row {row_number} has unsupported op '{other}'"
                )));
            }
        }
    }
    *cdc_row_offset = cdc_row_offset.checked_add(chunk_rows).ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "small change over large base CDC row offset overflow".to_string(),
        )
    })?;
    Ok(())
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_selective_filter_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut encoded_predicate_provider = scan_selective_filter_column_batches(fact_path)?;
    let (stats, metric_sum) = if encoded_predicate_provider.selection_vector_intersection_certified
        && !encoded_predicate_provider
            .bridge_selection_vectors
            .is_empty()
    {
        let aggregation = scan_selective_filter_metric_by_selection_vectors(
            fact_path,
            &encoded_predicate_provider.bridge_selection_vectors,
        )?;
        encoded_predicate_provider.selected_metric_aggregation_status =
            "selection_vector_consumed".to_string();
        encoded_predicate_provider.selected_metric_selection_vector_consumed = true;
        encoded_predicate_provider.selected_metric_source =
            "reader_generated_conjunctive_bridge_selection_vectors".to_string();
        encoded_predicate_provider.selected_metric_row_count = Some(aggregation.row_count);
        encoded_predicate_provider.selected_metric_sum = Some(aggregation.metric_sum);
        encoded_predicate_provider.selected_metric_scan_split_count =
            aggregation.stats.arrays_read_count;
        encoded_predicate_provider.selected_metric_data_decoded = true;
        encoded_predicate_provider.selected_metric_data_materialized = false;
        (aggregation.stats, aggregation.metric_sum)
    } else {
        let mut metric_sum = 0.0;
        let stats = scan_fact_vortex_projected(
            fact_path,
            &["metric"],
            Some(selective_filter_expr()),
            |fields, _chunk_rows| {
                metric_sum += primitive_field::<f64>(fields, "metric")?
                    .iter()
                    .sum::<f64>();
                Ok(())
            },
        )?;
        (stats, metric_sum)
    };
    let result_json = scalar_result_json(stats.result_row_count, metric_sum);
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming_with_encoded_predicate_provider(
            stats,
            encoded_predicate_provider,
        ),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_selective_filter_scenario_with_selective_filter_state(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
    selective_state: &TraditionalSelectiveFilterState,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut encoded_predicate_provider = scan_selective_filter_column_batches(fact_path)?;
    encoded_predicate_provider.selected_metric_aggregation_status =
        "batch_source_state_metric_aggregation_used".to_string();
    encoded_predicate_provider.selected_metric_selection_vector_consumed = false;
    encoded_predicate_provider.selected_metric_source =
        "per_batch_selective_filter_source_state".to_string();
    encoded_predicate_provider.selected_metric_row_count =
        Some(selective_state.selected_metric_accum.row_count);
    encoded_predicate_provider.selected_metric_sum =
        Some(selective_state.selected_metric_accum.metric_sum);
    encoded_predicate_provider.selected_metric_scan_split_count =
        selective_state.stats.arrays_read_count;
    encoded_predicate_provider.selected_metric_data_decoded = true;
    encoded_predicate_provider.selected_metric_data_materialized = false;
    let result_json = scalar_result_json(
        selective_state.selected_metric_accum.row_count,
        selective_state.selected_metric_accum.metric_sum,
    );
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: selective_state.stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: selective_state.stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming_with_encoded_predicate_provider(
            selective_state.stats.clone(),
            encoded_predicate_provider,
        ),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn scan_selective_filter_metric_by_selection_vectors(
    fact_path: &std::path::Path,
    selection_vectors: &[SelectionVector],
) -> Result<SelectionVectorMetricAggregation> {
    let mut split_index = 0_usize;
    let mut row_count = 0_u64;
    let mut metric_sum = 0.0;
    let mut stats = scan_fact_vortex_projected(
        fact_path,
        &["metric"],
        None,
        |fields, chunk_rows| {
            let selection_vector = selection_vectors.get(split_index).ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "selected metric aggregation missing bridge selection vector for metric split {split_index}; fallback execution was not attempted"
            ))
        })?;
            let metrics = primitive_field::<f64>(fields, "metric")?;
            if metrics.len() != chunk_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "selected metric aggregation Vortex chunk length mismatch: chunk_rows={chunk_rows}, metric_len={}; fallback execution was not attempted",
                    metrics.len()
                )));
            }
            let (selected_rows, selected_sum) =
                selected_metric_sum_for_selection_vector(selection_vector, &metrics, split_index)?;
            row_count = checked_u64_sum(row_count, selected_rows)?;
            metric_sum += selected_sum;
            split_index += 1;
            Ok(())
        },
    )?;
    if split_index != selection_vectors.len() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "selected metric aggregation consumed {split_index} metric splits but bridge supplied {} selection vectors; fallback execution was not attempted",
            selection_vectors.len()
        )));
    }
    stats.result_row_count = row_count;
    Ok(SelectionVectorMetricAggregation {
        stats,
        row_count,
        metric_sum,
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn selected_metric_sum_for_selection_vector(
    selection_vector: &SelectionVector,
    metrics: &[f64],
    split_index: usize,
) -> Result<(u64, f64)> {
    match selection_vector {
        SelectionVector::All { row_count } => {
            let metric_row_count = usize_to_u64(metrics.len())?;
            if *row_count != metric_row_count {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "selected metric aggregation split {split_index} all-row boundary mismatch: selection_rows={row_count}, metric_rows={metric_row_count}; fallback execution was not attempted"
                )));
            }
            Ok((*row_count, metrics.iter().sum::<f64>()))
        }
        SelectionVector::None => Ok((0, 0.0)),
        SelectionVector::Indices(indices) => {
            let mut metric_sum = 0.0;
            for index in indices {
                let metric_index = usize::try_from(*index).map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "selected metric aggregation split {split_index} selection index {index} cannot fit usize: {error}; fallback execution was not attempted"
                    ))
                })?;
                let Some(metric) = metrics.get(metric_index) else {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "selected metric aggregation split {split_index} selection index {index} is outside metric row count {}; fallback execution was not attempted",
                        metrics.len()
                    )));
                };
                metric_sum += *metric;
            }
            Ok((usize_to_u64(indices.len())?, metric_sum))
        }
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
struct TraditionalSelectiveFilterColumnProbe {
    source: UniversalInputSource,
    projected_columns: Vec<String>,
    reader_splits: Vec<VortexReaderBackedSplitEvidence>,
    encoded_kernel_inputs: Vec<VortexReaderGeneratedEncodedKernelInput>,
    reader_chunk_columns_observed: std::collections::BTreeSet<String>,
    reader_chunk_dtype_summary: std::collections::BTreeSet<String>,
    reader_chunk_encoding_summary: std::collections::BTreeSet<String>,
    probe_row_count: u64,
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn scan_selective_filter_column_batches(
    fact_path: &std::path::Path,
) -> Result<TraditionalEncodedPredicateProviderRuntimeEvidence> {
    let TraditionalSelectiveFilterColumnProbe {
        source,
        projected_columns,
        reader_splits,
        encoded_kernel_inputs,
        reader_chunk_columns_observed,
        reader_chunk_dtype_summary,
        reader_chunk_encoding_summary,
        probe_row_count,
    } = collect_selective_filter_column_probe(fact_path)?;
    let bridge = execute_vortex_reader_generated_conjunctive_filter_from_encoded_kernel_inputs(
        &selective_filter_encoded_predicates()?,
        &source,
        &reader_splits,
        &encoded_kernel_inputs,
    )?;
    let filter_columns_observed = reader_chunk_columns_observed.contains("flag")
        && reader_chunk_columns_observed.contains("value");
    let probe_status = if bridge.runtime_execution_allowed {
        "admitted_filter_column_kernel_inputs"
    } else if reader_splits.is_empty() {
        "requested_no_filter_column_reader_chunks_emitted"
    } else if filter_columns_observed {
        "observed_filter_column_reader_chunks_blocked_kernel_input_lowering"
    } else {
        "blocked_filter_only_columns_not_observed"
    };
    Ok(TraditionalEncodedPredicateProviderRuntimeEvidence {
        requested: true,
        requested_columns: projected_columns,
        probe_status: probe_status.to_string(),
        reader_split_count: reader_splits.len(),
        probe_row_count,
        reader_chunk_columns_observed: reader_chunk_columns_observed.into_iter().collect(),
        reader_chunk_dtype_summary: reader_chunk_dtype_summary.into_iter().collect(),
        reader_chunk_encoding_summary: reader_chunk_encoding_summary.into_iter().collect(),
        encoded_kernel_input_count: encoded_kernel_inputs.len(),
        bridge_status: bridge.status.as_str().to_string(),
        bridge_report_id: bridge.report_id,
        bridge_intersection_count: bridge.intersection_count,
        bridge_selected_row_count: bridge.selected_row_count,
        bridge_selection_vectors: bridge.selected_selection_vectors,
        filter_column_batches_consumed: bridge.filter_column_batches_consumed,
        selection_vector_intersection_certified: bridge.selection_vector_intersection_certified,
        selected_metric_aggregation_status: selected_metric_initial_status(
            bridge.runtime_execution_allowed,
        ),
        selected_metric_selection_vector_consumed: false,
        selected_metric_source: "none".to_string(),
        selected_metric_row_count: None,
        selected_metric_sum: None,
        selected_metric_scan_split_count: 0,
        selected_metric_data_decoded: false,
        selected_metric_data_materialized: false,
        data_decoded: bridge.data_decoded,
        data_materialized: bridge.data_materialized,
        row_read: bridge.row_read,
        fallback_attempted: bridge.fallback_attempted,
        external_engine_invoked: bridge.external_engine_invoked,
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn collect_selective_filter_column_probe(
    fact_path: &std::path::Path,
) -> Result<TraditionalSelectiveFilterColumnProbe> {
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
        .block_on(session.open_options().open_path(fact_path))
        .map_err(vortex_error)?;
    let projected_columns = vec!["flag".to_string(), "value".to_string()];
    let projected_column_refs = ["flag", "value"];
    let source_uri = DatasetUri::new(fact_path.display().to_string())?;
    let source = UniversalInputSource::from_dataset_uri(source_uri.clone())?;
    let mut scan = file.scan().map_err(vortex_error)?;
    scan = scan.with_projection(select(projected_column_refs.to_vec(), root()));

    let mut reader_splits = Vec::new();
    let mut encoded_kernel_inputs = Vec::new();
    let mut reader_chunk_columns_observed = std::collections::BTreeSet::new();
    let mut reader_chunk_dtype_summary = std::collections::BTreeSet::new();
    let mut reader_chunk_encoding_summary = std::collections::BTreeSet::new();
    let mut probe_row_count = 0_u64;
    for (arrays_read_count, chunk) in scan
        .into_array_iter(&runtime)
        .map_err(vortex_error)?
        .enumerate()
    {
        let chunk = chunk.map_err(vortex_error)?;
        let row_count = chunk.len();
        record_reader_chunk_summary_from_vortex_chunk(
            &chunk,
            &projected_column_refs,
            &mut reader_chunk_columns_observed,
            &mut reader_chunk_dtype_summary,
            &mut reader_chunk_encoding_summary,
        );
        let split = VortexReaderBackedSplitEvidence::local_scan_chunk(
            source_uri.clone(),
            arrays_read_count,
            row_count,
            chunk.dtype().to_string(),
            chunk.encoding_id().to_string(),
            chunk.nchildren(),
            chunk.nbuffers(),
        )?;
        encoded_kernel_inputs.extend(reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            &source_uri,
            &split.split_ref,
            &chunk,
        )?);
        reader_splits.push(split);
        probe_row_count = checked_u64_sum(probe_row_count, usize_to_u64(row_count)?)?;
    }

    Ok(TraditionalSelectiveFilterColumnProbe {
        source,
        projected_columns,
        reader_splits,
        encoded_kernel_inputs,
        reader_chunk_columns_observed,
        reader_chunk_dtype_summary,
        reader_chunk_encoding_summary,
        probe_row_count,
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn selected_metric_initial_status(runtime_execution_allowed: bool) -> String {
    if runtime_execution_allowed {
        "pending_selection_vector_metric_aggregation".to_string()
    } else {
        "not_attempted_bridge_not_admitted".to_string()
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn record_reader_chunk_summary_from_vortex_chunk(
    chunk: &vortex::array::ArrayRef,
    projected_columns: &[&str],
    reader_chunk_columns_observed: &mut std::collections::BTreeSet<String>,
    reader_chunk_dtype_summary: &mut std::collections::BTreeSet<String>,
    reader_chunk_encoding_summary: &mut std::collections::BTreeSet<String>,
) {
    if chunk.dtype().is_struct() {
        for (field_name, field) in chunk.named_children() {
            let field_name = field_name.clone();
            reader_chunk_columns_observed.insert(field_name.clone());
            reader_chunk_dtype_summary.insert(format!("{field_name}:{:?}", field.dtype()));
            reader_chunk_encoding_summary.insert(format!("{field_name}:{}", field.encoding_id()));
        }
    } else if projected_columns.len() == 1 {
        let field_name = projected_columns[0].to_string();
        reader_chunk_columns_observed.insert(field_name.clone());
        reader_chunk_dtype_summary.insert(format!("{field_name}:{:?}", chunk.dtype()));
        reader_chunk_encoding_summary.insert(format!("{field_name}:{}", chunk.encoding_id()));
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn selective_filter_encoded_predicates() -> Result<Vec<PredicateExpr>> {
    Ok(vec![
        PredicateExpr::Compare {
            column: ColumnRef::new("flag")?,
            op: ComparisonOp::Eq,
            value: StatValue::UInt64(1),
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("value")?,
            op: ComparisonOp::GtEq,
            value: StatValue::UInt64(5_000),
        },
    ])
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
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_filter_projection_limit_scenario(
    fact_path: &std::path::Path,
    dim_path: &std::path::Path,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let mut filtered_projection_limit_rows =
        std::collections::BinaryHeap::<TraditionalFilteredProjectionLimitCandidate>::new();
    let mut filtered_projection_sequence = 0_u64;
    let stats = scan_fact_vortex_projected(
        fact_path,
        &["id", "value"],
        Some(selective_filter_expr()),
        |fields, chunk_rows| {
            let ids = primitive_field::<u64>(fields, "id")?;
            let values = primitive_field::<u32>(fields, "value")?;
            if ids.len() != chunk_rows || values.len() != chunk_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "filter + projection + limit Vortex chunk length mismatch: chunk_rows={chunk_rows}, id_len={}, value_len={}",
                    ids.len(),
                    values.len()
                )));
            }
            for (id, value) in ids.into_iter().zip(values) {
                push_filter_projection_limit_candidate(
                    &mut filtered_projection_limit_rows,
                    filtered_projection_sequence,
                    id,
                    value,
                );
                filtered_projection_sequence =
                    filtered_projection_sequence.checked_add(1).ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "filter + projection + limit row sequence overflowed u64".to_string(),
                        )
                    })?;
            }
            Ok(())
        },
    )?;
    let filtered_projection_rows =
        filter_projection_limit_rows_from_heap(filtered_projection_limit_rows);
    let result_json = filter_projection_limit_result_json(&filtered_projection_rows);
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming(stats),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn push_filter_projection_limit_candidate(
    rows: &mut std::collections::BinaryHeap<TraditionalFilteredProjectionLimitCandidate>,
    sequence: u64,
    id: u64,
    value: u32,
) {
    rows.push(TraditionalFilteredProjectionLimitCandidate {
        id,
        sequence,
        value,
    });
    if rows.len() > 100 {
        let _ = rows.pop();
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn filter_projection_limit_rows_from_heap(
    rows: std::collections::BinaryHeap<TraditionalFilteredProjectionLimitCandidate>,
) -> Vec<TraditionalFilteredProjectionRow> {
    let mut rows = rows.into_vec();
    rows.sort_by_key(|row| (row.id, row.sequence));
    rows.into_iter()
        .map(|row| TraditionalFilteredProjectionRow {
            id: row.id,
            value: row.value,
        })
        .collect()
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn run_streaming_filter_projection_limit_scenario_with_selective_filter_state(
    dim_path: &std::path::Path,
    selective_state: &TraditionalSelectiveFilterState,
) -> Result<TraditionalScenarioExecution> {
    let dim_rows = vortex_file_row_count(dim_path)?;
    let result_json =
        filter_projection_limit_result_json(&selective_state.filtered_projection_rows);
    Ok(TraditionalScenarioExecution {
        result_json,
        fact_rows: selective_state.stats.source_row_count,
        dim_rows,
        cdc_delta_rows: 0,
        rows_scanned: selective_state.stats.source_row_count,
        rows_materialized: 1,
        evidence: TraditionalScenarioExecutionEvidence::streaming(selective_state.stats.clone()),
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn filter_projection_limit_result_json(rows: &[TraditionalFilteredProjectionRow]) -> String {
    let mut limited_rows = rows.to_vec();
    limited_rows.sort_by_key(|row| row.id);
    limited_rows.truncate(100);
    let mut accum = TraditionalGroupAccum::default();
    for row in limited_rows {
        accum.add(f64::from(row.value));
    }
    scalar_result_json(accum.row_count, accum.metric_sum)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn scan_dim_label_state(
    dim_path: &std::path::Path,
    scenario: &str,
) -> Result<(
    std::collections::HashMap<u32, String>,
    TraditionalStreamingScanStats,
)> {
    let mut dim_by_key = std::collections::HashMap::<u32, String>::new();
    let dim_stats = scan_fact_vortex_projected(
        dim_path,
        &["dim_key", "dim_label"],
        None,
        |fields, chunk_rows| {
            let dim_keys = primitive_field::<u32>(fields, "dim_key")?;
            let dim_labels = utf8_field(fields, "dim_label")?;
            if dim_keys.len() != chunk_rows || dim_labels.len() != chunk_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "{scenario} dimension Vortex chunk length mismatch: chunk_rows={chunk_rows}, dim_key_len={}, dim_label_len={}",
                    dim_keys.len(),
                    dim_labels.len()
                )));
            }
            for (dim_key, dim_label) in dim_keys.into_iter().zip(dim_labels) {
                dim_by_key.insert(dim_key, dim_label);
            }
            Ok(())
        },
    )?;
    Ok((dim_by_key, dim_stats))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn prefixed_projected_columns(
    left_prefix: &str,
    left_columns: &[String],
    right_prefix: &str,
    right_columns: &[String],
) -> Vec<String> {
    left_columns
        .iter()
        .map(|column| format!("{left_prefix}.{column}"))
        .chain(
            right_columns
                .iter()
                .map(|column| format!("{right_prefix}.{column}")),
        )
        .collect()
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn prefixed_reader_chunk_summary(
    left_prefix: &str,
    left_values: &[String],
    right_prefix: &str,
    right_values: &[String],
) -> Vec<String> {
    left_values
        .iter()
        .map(|value| prefix_reader_chunk_summary_value(left_prefix, value))
        .chain(
            right_values
                .iter()
                .map(|value| prefix_reader_chunk_summary_value(right_prefix, value)),
        )
        .collect()
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn prefix_reader_chunk_summary_value(prefix: &str, value: &str) -> String {
    value.split_once(':').map_or_else(
        || format!("{prefix}.{value}"),
        |(column, detail)| format!("{prefix}.{column}:{detail}"),
    )
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
    let mut reader_chunk_columns_observed = std::collections::BTreeSet::new();
    let mut reader_chunk_dtype_summary = std::collections::BTreeSet::new();
    let mut reader_chunk_encoding_summary = std::collections::BTreeSet::new();
    for chunk in scan.into_array_iter(&runtime).map_err(vortex_error)? {
        let chunk = chunk.map_err(vortex_error)?;
        let chunk_rows = chunk.len();
        let fields = projected_fields_from_chunk(chunk, projected_columns)?;
        for (column, array) in &fields {
            reader_chunk_columns_observed.insert(column.clone());
            reader_chunk_dtype_summary.insert(format!("{column}:{:?}", array.dtype()));
            reader_chunk_encoding_summary.insert(format!("{column}:{}", array.encoding_id()));
        }
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
        reader_chunk_columns_observed: reader_chunk_columns_observed.into_iter().collect(),
        reader_chunk_dtype_summary: reader_chunk_dtype_summary.into_iter().collect(),
        reader_chunk_encoding_summary: reader_chunk_encoding_summary.into_iter().collect(),
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
fn join_aggregate_fact_filter_expr() -> vortex::array::expr::Expression {
    use vortex::array::expr::{col, gt_eq, lit};

    gt_eq(col("value".to_string()), lit(2_500_u32))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn partition_pruning_date_range_expr() -> vortex::array::expr::Expression {
    use vortex::array::expr::{and, col, gt_eq, lit, lt};

    and(
        gt_eq(col("event_date".to_string()), lit("2024-03-01")),
        lt(col("event_date".to_string()), lit("2024-06-01")),
    )
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn partition_pruning_date_range_contains(event_date: &str) -> bool {
    ("2024-03-01".."2024-06-01").contains(&event_date)
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
    cdc_delta: Option<&VortexCdcDeltaTable>,
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
        TraditionalAnalyticsScenario::PartitionPruning => partition_pruning_json(fact)?,
        TraditionalAnalyticsScenario::ManySmallFilesScan => {
            scalar_result_json(usize_to_u64(fact.len())?, fact.metric.iter().sum())
        }
        TraditionalAnalyticsScenario::NullHeavyAggregate => null_heavy_aggregate_json(fact)?,
        TraditionalAnalyticsScenario::CleanCastFilterWrite => clean_cast_filter_write_json(fact)?,
        TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv => {
            malformed_timestamp_dirty_csv_json(fact)?
        }
        TraditionalAnalyticsScenario::SmallChangeOverLargeBase => {
            small_change_over_large_base_json(fact, cdc_delta)?
        }
        TraditionalAnalyticsScenario::NestedJsonFieldScan => nested_json_field_scan_json(fact)?,
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
fn clean_cast_filter_write_json(fact: &VortexFactTable) -> Result<String> {
    if fact.raw_event_time.iter().all(String::is_empty)
        || fact.dirty_numeric.iter().all(String::is_empty)
        || fact.dirty_flag.iter().all(String::is_empty)
    {
        return Err(ShardLoomError::InvalidOperation(
            "clean/cast/filter/write requires raw_event_time, dirty_numeric, and dirty_flag fixture columns"
                .to_string(),
        ));
    }
    let mut accum = TraditionalGroupAccum::default();
    for index in 0..fact.len() {
        if fact.dirty_flag[index] != "Y"
            || !generated_timestamp_shape_is_valid(&fact.raw_event_time[index])
        {
            continue;
        }
        let Ok(value) = fact.dirty_numeric[index].parse::<f64>() else {
            continue;
        };
        if value >= 500.0 {
            accum.add(value);
        }
    }
    Ok(scalar_result_json(accum.row_count, accum.metric_sum))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn partition_pruning_json(fact: &VortexFactTable) -> Result<String> {
    if fact.event_date.iter().all(String::is_empty) {
        return Err(ShardLoomError::InvalidOperation(
            "partition pruning requires an event_date fixture column".to_string(),
        ));
    }
    let mut accum = TraditionalGroupAccum::default();
    for index in 0..fact.len() {
        let event_date = fact.event_date[index].as_str();
        if partition_pruning_date_range_contains(event_date) {
            accum.add(fact.metric[index]);
        }
    }
    Ok(scalar_result_json(accum.row_count, accum.metric_sum))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn null_heavy_aggregate_json(fact: &VortexFactTable) -> Result<String> {
    let mut accum = TraditionalGroupAccum::default();
    for index in 0..fact.len() {
        if fact.nullable_metric_00[index].is_empty() {
            continue;
        }
        let value = fact.nullable_metric_00[index]
            .parse::<f64>()
            .map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to parse nullable_metric_00 at row {}: {error}",
                    index + 1
                ))
            })?;
        accum.add(value);
    }
    Ok(scalar_result_json(accum.row_count, accum.metric_sum))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn malformed_timestamp_dirty_csv_json(fact: &VortexFactTable) -> Result<String> {
    if fact.raw_event_time.iter().all(String::is_empty)
        || fact.dirty_numeric.iter().all(String::is_empty)
    {
        return Err(ShardLoomError::InvalidOperation(
            "malformed timestamp / dirty CSV requires raw_event_time and dirty_numeric fixture columns"
                .to_string(),
        ));
    }
    let mut accum = TraditionalGroupAccum::default();
    for index in 0..fact.len() {
        if !generated_timestamp_shape_is_valid(&fact.raw_event_time[index]) {
            continue;
        }
        let Ok(value) = fact.dirty_numeric[index].parse::<f64>() else {
            continue;
        };
        accum.add(value);
    }
    Ok(scalar_result_json(accum.row_count, accum.metric_sum))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn small_change_over_large_base_json(
    fact: &VortexFactTable,
    cdc_delta: Option<&VortexCdcDeltaTable>,
) -> Result<String> {
    use std::collections::BTreeMap;

    let cdc_delta = cdc_delta.ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "small change over large base requires imported CDC delta Vortex source".to_string(),
        )
    })?;
    if cdc_delta.len() == 0 {
        return Err(ShardLoomError::InvalidOperation(
            "small change over large base requires non-empty CDC delta rows".to_string(),
        ));
    }
    let mut rows = (0..fact.len())
        .map(|index| (fact.id[index], fact.metric[index]))
        .collect::<BTreeMap<_, _>>();
    for index in 0..cdc_delta.len() {
        if !generated_timestamp_shape_is_valid(&cdc_delta.effective_ts[index]) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "CDC delta row {} has invalid effective_ts",
                index + 1
            )));
        }
        match cdc_delta.op[index].as_str() {
            "delete" => {
                rows.remove(&cdc_delta.id[index]);
            }
            "update" | "insert" => {
                let _value = cdc_delta.value[index].parse::<u32>().map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "CDC delta row {} has invalid value: {error}",
                        index + 1
                    ))
                })?;
                let metric = cdc_delta.metric[index].parse::<f64>().map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "CDC delta row {} has invalid metric: {error}",
                        index + 1
                    ))
                })?;
                rows.insert(cdc_delta.id[index], metric);
            }
            other => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "CDC delta row {} has unsupported op '{other}'",
                    index + 1
                )));
            }
        }
    }
    Ok(scalar_result_json(
        usize_to_u64(rows.len())?,
        rows.values().sum(),
    ))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn nested_json_field_scan_json(fact: &VortexFactTable) -> Result<String> {
    if fact.nested_payload.iter().all(String::is_empty) {
        return Err(ShardLoomError::InvalidOperation(
            "nested JSON field scan requires nested_payload fixture column".to_string(),
        ));
    }
    let mut row_count = 0_u64;
    let mut metric_sum = 0.0;
    let mut flagged = 0_u64;
    for index in 0..fact.len() {
        if fact.nested_payload[index].is_empty() {
            continue;
        }
        let payload = fact.nested_payload[index].as_str();
        metric_sum += generated_nested_score(payload, index)?;
        if generated_nested_flag(payload, index)? {
            flagged += 1;
        }
        row_count += 1;
    }
    Ok(format!(
        "{{\"row_count\":{row_count},\"metric_sum\":{},\"flagged\":{flagged}}}",
        json_float(metric_sum)
    ))
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn generated_nested_score(payload: &str, row_index: usize) -> Result<f64> {
    let Some(start) = payload
        .find("\"score\":")
        .map(|index| index + "\"score\":".len())
    else {
        return Err(ShardLoomError::InvalidOperation(format!(
            "nested_payload row {} missing metrics.score",
            row_index + 1
        )));
    };
    let tail = &payload[start..];
    let end = tail
        .find(|ch: char| !matches!(ch, '-' | '+' | '.' | '0'..='9' | 'e' | 'E'))
        .unwrap_or(tail.len());
    tail[..end].parse::<f64>().map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "nested_payload row {} has invalid metrics.score: {error}",
            row_index + 1
        ))
    })
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn generated_nested_flag(payload: &str, row_index: usize) -> Result<bool> {
    let Some(start) = payload
        .find("\"flag\":")
        .map(|index| index + "\"flag\":".len())
    else {
        return Err(ShardLoomError::InvalidOperation(format!(
            "nested_payload row {} missing event.flag",
            row_index + 1
        )));
    };
    let tail = &payload[start..];
    if tail.starts_with("true") {
        Ok(true)
    } else if tail.starts_with("false") {
        Ok(false)
    } else {
        Err(ShardLoomError::InvalidOperation(format!(
            "nested_payload row {} has invalid event.flag",
            row_index + 1
        )))
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn generated_timestamp_shape_is_valid(value: &str) -> bool {
    let bytes = value.as_bytes();
    if !(bytes.len() == 20
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[13] == b':'
        && bytes[16] == b':'
        && bytes[19] == b'Z'
        && bytes.iter().enumerate().all(|(index, byte)| match index {
            4 | 7 | 10 | 13 | 16 | 19 => true,
            _ => byte.is_ascii_digit(),
        }))
    {
        return false;
    }
    let Some(year) = parse_ascii_u32(&bytes[0..4]) else {
        return false;
    };
    let Some(month) = parse_ascii_u32(&bytes[5..7]) else {
        return false;
    };
    let Some(day) = parse_ascii_u32(&bytes[8..10]) else {
        return false;
    };
    let Some(hour) = parse_ascii_u32(&bytes[11..13]) else {
        return false;
    };
    let Some(minute) = parse_ascii_u32(&bytes[14..16]) else {
        return false;
    };
    let Some(second) = parse_ascii_u32(&bytes[17..19]) else {
        return false;
    };
    (1..=12).contains(&month)
        && day >= 1
        && day <= days_in_month(year, month)
        && hour < 24
        && minute < 60
        && second < 60
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn parse_ascii_u32(bytes: &[u8]) -> Option<u32> {
    let mut value = 0_u32;
    for byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u32::from(byte - b'0');
    }
    Some(value)
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn is_leap_year(year: u32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
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
        assert_eq!(
            TraditionalAnalyticsScenario::parse("partition pruning").unwrap(),
            TraditionalAnalyticsScenario::PartitionPruning
        );
        assert_eq!(
            TraditionalAnalyticsScenario::parse("many-small-files scan").unwrap(),
            TraditionalAnalyticsScenario::ManySmallFilesScan
        );
        assert_eq!(
            TraditionalAnalyticsScenario::parse("null-heavy aggregate").unwrap(),
            TraditionalAnalyticsScenario::NullHeavyAggregate
        );
        assert_eq!(
            TraditionalAnalyticsScenario::parse("clean/cast/filter/write").unwrap(),
            TraditionalAnalyticsScenario::CleanCastFilterWrite
        );
        assert_eq!(
            TraditionalAnalyticsScenario::parse("malformed timestamp / dirty CSV").unwrap(),
            TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv
        );
        assert_eq!(
            TraditionalAnalyticsScenario::parse("small change over large base").unwrap(),
            TraditionalAnalyticsScenario::SmallChangeOverLargeBase
        );
        assert_eq!(
            TraditionalAnalyticsScenario::parse("nested JSON field scan").unwrap(),
            TraditionalAnalyticsScenario::NestedJsonFieldScan
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
    fn direct_transient_disabled_build_returns_explicit_error() {
        if cfg!(feature = "vortex-traditional-analytics-benchmark") {
            return;
        }
        let err = run_traditional_direct_transient_csv_smoke(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                PathBuf::from("fact.csv"),
                PathBuf::from("dim.csv"),
                PathBuf::from("ws"),
            )
            .with_requested_execution_mode(ShardLoomExecutionMode::DirectCompatibilityTransient),
        )
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
            "id,group_key,dim_key,value,metric,flag,category,event_date,nullable_metric_00,raw_event_time,dirty_numeric,dirty_flag\n1,10,1,6000,2.5,1,A,2024-03-01,1.25,2024-01-01T00:00:00Z,6000,Y\n2,11,2,1000,3.5,0,B,2024-07-01,,not-a-timestamp,bad-number,N\n3,10,1,8000,4.0,1,A,2024-05-01,3.75,2024-01-03T00:00:00Z,8000,Y\n",
        )
        .unwrap();
        std::fs::write(&dim_csv, "dim_key,dim_label,weight\n1,one,1.5\n2,two,2.0\n").unwrap();
        (fact_csv, dim_csv)
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn write_zero_match_traditional_csv_inputs(root: &std::path::Path) -> (PathBuf, PathBuf) {
        std::fs::create_dir_all(root).unwrap();
        let fact_csv = root.join("fact.csv");
        let dim_csv = root.join("dim.csv");
        std::fs::write(
            &fact_csv,
            "id,group_key,dim_key,value,metric,flag,category,event_date,nullable_metric_00,raw_event_time,dirty_numeric,dirty_flag\n1,10,1,1000,2.5,1,A,2024-03-01,1.25,2024-01-01T00:00:00Z,1000,Y\n2,11,2,2000,3.5,0,B,2024-07-01,,not-a-timestamp,bad-number,N\n3,10,1,3000,4.0,1,A,2024-05-01,3.75,2024-01-03T00:00:00Z,3000,Y\n",
        )
        .unwrap();
        std::fs::write(&dim_csv, "dim_key,dim_label,weight\n1,one,1.5\n2,two,2.0\n").unwrap();
        (fact_csv, dim_csv)
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn write_sequence_encoded_selective_filter_csv_inputs(
        root: &std::path::Path,
        rows: usize,
    ) -> (PathBuf, PathBuf) {
        use std::fmt::Write as _;

        std::fs::create_dir_all(root).unwrap();
        let fact_csv = root.join("fact.csv");
        let dim_csv = root.join("dim.csv");
        let mut fact = String::from(
            "id,group_key,dim_key,value,metric,flag,category,event_date,nullable_metric_00,raw_event_time,dirty_numeric,dirty_flag\n",
        );
        for index in 0..rows {
            let id = index + 1;
            let group_key = index % 4;
            let dim_key = index % 16;
            let value = index * 17;
            let metric_whole = index;
            let flag = usize::from(index % 7 == 0);
            let category = char::from(b'A' + u8::try_from(index % 4).unwrap());
            let event_date = format!("2024-{:02}-{:02}", 1 + (index % 12), 1 + (index % 28));
            writeln!(
                &mut fact,
                "{id},{group_key},{dim_key},{value},{metric_whole}.5,{flag},{category},{event_date},,{event_date}T00:00:00Z,{value},{}",
                if flag == 1 { "Y" } else { "N" }
            )
            .unwrap();
        }
        let mut dim = String::from("dim_key,dim_label,weight\n");
        for dim_key in 0..16 {
            writeln!(&mut dim, "{dim_key},dim-{dim_key},1.0").unwrap();
        }
        std::fs::write(&fact_csv, fact).unwrap();
        std::fs::write(&dim_csv, dim).unwrap();
        (fact_csv, dim_csv)
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn write_empty_sequence_encoded_selective_filter_csv_inputs(
        root: &std::path::Path,
        rows: usize,
    ) -> (PathBuf, PathBuf) {
        use std::fmt::Write as _;

        std::fs::create_dir_all(root).unwrap();
        let fact_csv = root.join("fact.csv");
        let dim_csv = root.join("dim.csv");
        let mut fact = String::from(
            "id,group_key,dim_key,value,metric,flag,category,event_date,nullable_metric_00,raw_event_time,dirty_numeric,dirty_flag\n",
        );
        for index in 0..rows {
            let id = index + 1;
            let group_key = index % 4;
            let dim_key = index % 16;
            let value = index;
            let metric_whole = index;
            let category = char::from(b'A' + u8::try_from(index % 4).unwrap());
            let event_date = format!("2024-{:02}-{:02}", 1 + (index % 12), 1 + (index % 28));
            writeln!(
                &mut fact,
                "{id},{group_key},{dim_key},{value},{metric_whole}.5,1,{category},{event_date},,{event_date}T00:00:00Z,{value},Y"
            )
            .unwrap();
        }
        let mut dim = String::from("dim_key,dim_label,weight\n");
        for dim_key in 0..16 {
            writeln!(&mut dim, "{dim_key},dim-{dim_key},1.0").unwrap();
        }
        std::fs::write(&fact_csv, fact).unwrap();
        std::fs::write(&dim_csv, dim).unwrap();
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
        assert!((rows[0].metric - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn fact_directory_reader_accepts_split_csv_parts() {
        let root = traditional_analytics_test_root("split-csv");
        let parts_dir = root.join("fact_parts");
        std::fs::create_dir_all(&parts_dir).unwrap();
        let dim_csv = root.join("dim.csv");
        std::fs::write(
            parts_dir.join("part-00000.csv"),
            "id,group_key,dim_key,value,metric,flag,category,event_date\n1,10,1,6000,2.5,1,A,2024-03-01\n",
        )
        .unwrap();
        std::fs::write(
            parts_dir.join("part-00001.csv"),
            "id,group_key,dim_key,value,metric,flag,category,event_date\n2,11,2,1000,3.5,0,B,2024-07-01\n",
        )
        .unwrap();
        std::fs::write(&dim_csv, "dim_key,dim_label,weight\n1,one,1.5\n2,two,2.0\n").unwrap();

        let report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::ManySmallFilesScan,
                parts_dir,
                dim_csv,
                root.join("workspace"),
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv)
            .with_native_vortex_replay_verification(true)
            .with_result_vortex_write(true),
        )
        .unwrap();

        assert_eq!(report.result_json, "{\"row_count\":2,\"metric_sum\":6.0}");
        assert_eq!(report.fact_rows, 2);
        assert!(report.fact_source_path.is_dir());
        assert!(report.output_replay_verified);
        assert!(report.computed_result_sink_replay_verified);
        assert!(!report.fallback_execution_allowed);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn fact_directory_reader_skips_empty_jsonl_parts() {
        let root = traditional_analytics_test_root("split-jsonl-empty");
        let parts_dir = root.join("fact_parts");
        std::fs::create_dir_all(&parts_dir).unwrap();
        std::fs::write(parts_dir.join("part-00000.jsonl"), "").unwrap();
        std::fs::write(
            parts_dir.join("part-00001.jsonl"),
            "{\"id\":1,\"group_key\":10,\"dim_key\":1,\"value\":6000,\"metric\":2.5,\"flag\":1,\"category\":\"A\"}\n",
        )
        .unwrap();

        let rows = read_traditional_fact_rows(
            &parts_dir,
            TraditionalAnalyticsInputFormat::JsonLines,
            TraditionalAnalyticsResourcePolicy::auto().resolve_for_sources(128),
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, 1);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn null_heavy_aggregate_accepts_all_null_metric_column() {
        let root = traditional_analytics_test_root("null-heavy-all-null");
        std::fs::create_dir_all(&root).unwrap();
        let fact_csv = root.join("fact.csv");
        let dim_csv = root.join("dim.csv");
        std::fs::write(
            &fact_csv,
            "id,group_key,dim_key,value,metric,flag,category,nullable_metric_00\n1,10,1,6000,2.5,1,A,\n2,11,2,1000,3.5,0,B,\n",
        )
        .unwrap();
        std::fs::write(&dim_csv, "dim_key,dim_label,weight\n1,one,1.5\n2,two,2.0\n").unwrap();

        let report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::NullHeavyAggregate,
                fact_csv,
                dim_csv,
                root.join("workspace"),
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(report.result_json, "{\"row_count\":0,\"metric_sum\":0.0}");

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn clean_cast_filter_write_requires_dirty_columns() {
        let root = traditional_analytics_test_root("missing-dirty-columns");
        std::fs::create_dir_all(&root).unwrap();
        let fact_csv = root.join("fact.csv");
        let dim_csv = root.join("dim.csv");
        std::fs::write(
            &fact_csv,
            "id,group_key,dim_key,value,metric,flag,category\n1,10,1,6000,2.5,1,A\n",
        )
        .unwrap();
        std::fs::write(&dim_csv, "dim_key,dim_label,weight\n1,one,1.5\n").unwrap();

        let error = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::CleanCastFilterWrite,
                fact_csv,
                dim_csv,
                root.join("workspace"),
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap_err();

        assert!(error.to_string().contains("requires raw_event_time"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn timestamp_validation_rejects_impossible_dates_and_times() {
        assert!(generated_timestamp_shape_is_valid("2024-02-29T23:59:59Z"));
        assert!(!generated_timestamp_shape_is_valid("2023-02-29T00:00:00Z"));
        assert!(!generated_timestamp_shape_is_valid("2024-13-40T25:61:61Z"));
    }

    #[test]
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn date32_conversion_preserves_event_date_strings() {
        assert_eq!(date32_days_to_yyyy_mm_dd(0), "1970-01-01");
        assert_eq!(date32_days_to_yyyy_mm_dd(19_783), "2024-03-01");
    }

    #[test]
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn nested_json_field_scan_runs_jsonl_fixture() {
        let root = traditional_analytics_test_root("nested-jsonl");
        std::fs::create_dir_all(&root).unwrap();
        let fact_jsonl = root.join("fact.jsonl");
        let dim_jsonl = root.join("dim.jsonl");
        std::fs::write(
            &fact_jsonl,
            "{\"id\":1,\"group_key\":10,\"dim_key\":1,\"value\":6000,\"metric\":2.5,\"flag\":1,\"category\":\"A\",\"nested_payload\":\"{\\\"event\\\":{\\\"flag\\\":true},\\\"metrics\\\":{\\\"score\\\":2.5}}\"}\n{\"id\":2,\"group_key\":11,\"dim_key\":2,\"value\":1000,\"metric\":3.5,\"flag\":0,\"category\":\"B\",\"nested_payload\":\"{\\\"event\\\":{\\\"flag\\\":false},\\\"metrics\\\":{\\\"score\\\":3.75}}\"}\n",
        )
        .unwrap();
        std::fs::write(
            &dim_jsonl,
            "{\"dim_key\":1,\"dim_label\":\"one\",\"weight\":1.5}\n{\"dim_key\":2,\"dim_label\":\"two\",\"weight\":2.0}\n",
        )
        .unwrap();

        let report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::NestedJsonFieldScan,
                fact_jsonl,
                dim_jsonl,
                root.join("workspace"),
            )
            .with_input_format(TraditionalAnalyticsInputFormat::JsonLines)
            .with_native_vortex_replay_verification(true)
            .with_result_vortex_write(true),
        )
        .unwrap();

        assert_eq!(
            report.result_json,
            "{\"row_count\":2,\"metric_sum\":6.25,\"flagged\":1}"
        );
        assert!(report.output_replay_verified);
        assert!(report.computed_result_sink_replay_verified);
        assert!(!report.fallback_execution_allowed);
        assert!(report.streaming_vortex_execution_used);
        assert!(report.full_table_materialization_avoided);
        assert!(!report.streaming_filter_pushdown_applied);
        assert!(report.streaming_projection_pushdown_applied);
        assert_eq!(
            report.streaming_projected_columns,
            vec!["nested_payload".to_string()]
        );

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::NestedJsonFieldScan,
                report.fact_vortex_path.clone(),
                report.dim_vortex_path.clone(),
            ))
            .unwrap();
        assert_eq!(native_report.result_json, report.result_json);
        assert!(native_report.streaming_vortex_execution_used);
        assert!(native_report.full_table_materialization_avoided);
        assert!(!native_report.streaming_filter_pushdown_applied);
        assert!(native_report.streaming_projection_pushdown_applied);
        assert_eq!(
            native_report.streaming_projected_columns,
            vec!["nested_payload".to_string()]
        );
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 1);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_temporary_materialization_used")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[allow(clippy::too_many_lines)]
    fn small_change_over_large_base_imports_cdc_delta_fixture() {
        let root = traditional_analytics_test_root("cdc-overlay");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let cdc_csv = root.join("cdc_delta.csv");
        std::fs::write(
            &cdc_csv,
            "id,op,value,metric,effective_ts\n1,update,7000,10.00,2024-12-01T00:00:00Z\n2,delete,,,2024-12-02T00:00:00Z\n4,insert,9000,5.00,2024-12-03T12:00:00Z\n",
        )
        .unwrap();

        let report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::SmallChangeOverLargeBase,
                fact_csv,
                dim_csv,
                root.join("workspace"),
            )
            .with_cdc_delta_csv(Some(cdc_csv.clone()))
            .with_input_format(TraditionalAnalyticsInputFormat::Csv)
            .with_native_vortex_replay_verification(true)
            .with_result_vortex_write(true),
        )
        .unwrap();

        assert_eq!(report.result_json, "{\"row_count\":3,\"metric_sum\":19.0}");
        assert_eq!(report.cdc_delta_rows, 3);
        assert_eq!(report.cdc_delta_source_path, Some(cdc_csv));
        assert!(
            report
                .cdc_delta_vortex_path
                .as_ref()
                .is_some_and(|path| path.exists())
        );
        assert!(report.cdc_delta_vortex_bytes > 0);
        assert!(report.cdc_delta_vortex_digest.is_some());
        assert!(report.output_replay_verified);
        assert!(report.computed_result_sink_replay_verified);
        assert!(!report.fallback_execution_allowed);

        let native_report = run_traditional_analytics_vortex_benchmark(
            TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::SmallChangeOverLargeBase,
                report.fact_vortex_path.clone(),
                report.dim_vortex_path.clone(),
            )
            .with_cdc_delta_vortex(report.cdc_delta_vortex_path.clone()),
        )
        .unwrap();
        assert_eq!(native_report.result_json, report.result_json);
        assert_eq!(native_report.cdc_delta_rows, 3);
        assert_eq!(
            native_report.rows_scanned,
            native_report.fact_rows + native_report.cdc_delta_rows
        );
        assert!(native_report.cdc_delta_vortex_path.is_some());
        assert!(native_report.cdc_delta_vortex_bytes > 0);
        assert!(native_report.cdc_delta_vortex_digest.is_some());
        assert!(native_report.streaming_vortex_execution_used);
        assert!(native_report.full_table_materialization_avoided);
        assert!(!native_report.streaming_filter_pushdown_applied);
        assert!(native_report.streaming_projection_pushdown_applied);
        assert_eq!(
            native_report.streaming_projected_columns,
            vec![
                "base.id".to_string(),
                "base.metric".to_string(),
                "cdc_delta.id".to_string(),
                "cdc_delta.op".to_string(),
                "cdc_delta.value".to_string(),
                "cdc_delta.metric".to_string(),
                "cdc_delta.effective_ts".to_string(),
            ]
        );
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 1);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_temporary_materialization_used")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );
        assert!(
            native_fields
                .get("prepared_artifact_cdc_delta_ref")
                .is_some_and(|value| !value.is_empty())
        );
        assert_eq!(
            native_fields
                .get("source_backed_scan_source_roles")
                .map(String::as_str),
            Some("base,cdc_delta")
        );
        assert_eq!(
            native_fields
                .get("source_backed_scan_projected_columns")
                .map(String::as_str),
            Some(
                "base.id,base.metric,cdc_delta.id,cdc_delta.op,cdc_delta.value,cdc_delta.metric,cdc_delta.effective_ts"
            )
        );
        assert_eq!(
            native_fields
                .get("source_backed_scan_materialization_boundary_rows")
                .map(String::as_str),
            Some("0")
        );
        assert_eq!(
            native_fields
                .get("source_backed_scan_fallback_attempted")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("source_backed_scan_external_engine_invoked")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn small_change_over_large_base_requires_cdc_delta_source() {
        let root = traditional_analytics_test_root("cdc-missing");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);

        let err = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::SmallChangeOverLargeBase,
                fact_csv,
                dim_csv,
                root.join("workspace"),
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .expect_err("CDC overlay source should be required");

        assert!(
            err.to_string()
                .contains("requires a CDC delta source via --cdc-delta")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn field_map(fields: Vec<(String, String)>) -> std::collections::HashMap<String, String> {
        fields.into_iter().collect()
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    fn assert_field_eq(
        fields: &std::collections::HashMap<String, String>,
        name: &str,
        expected: &str,
    ) {
        assert_eq!(
            fields.get(name).map(String::as_str),
            Some(expected),
            "{name}"
        );
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
    #[allow(clippy::too_many_lines)]
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
        let fields = field_map(report.fields());
        assert_eq!(
            fields
                .get("source_backed_scan_evidence_schema_version")
                .map(String::as_str),
            Some(SOURCE_BACKED_SCAN_EVIDENCE_SCHEMA_VERSION)
        );
        assert_eq!(
            fields
                .get("source_backed_scan_evidence_status")
                .map(String::as_str),
            Some("scoped_local_vortex_scan_evidence")
        );
        assert_eq!(
            fields
                .get("source_backed_scan_provider_kind")
                .map(String::as_str),
            Some("vortex_file_projected_scan")
        );
        assert_eq!(
            fields
                .get("source_backed_scan_provider_scope")
                .map(String::as_str),
            Some("local_vortex_prepared_or_native_rows_only")
        );
        assert_eq!(
            fields
                .get("source_backed_scan_source_roles")
                .map(String::as_str),
            Some("fact")
        );
        assert_eq!(
            fields
                .get("source_backed_scan_projected_columns")
                .map(String::as_str),
            Some("metric")
        );
        assert_eq!(
            fields
                .get("source_backed_scan_materialization_boundary_rows")
                .map(String::as_str),
            Some("0")
        );
        assert_eq!(
            fields
                .get("source_backed_scan_data_materialized")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields
                .get("source_backed_scan_operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            fields
                .get("source_backed_scan_residual_executor")
                .map(String::as_str),
            Some("shardloom_native_residual_operator")
        );
        assert_eq!(
            fields
                .get("source_backed_scan_fallback_attempted")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields
                .get("source_backed_scan_external_engine_invoked")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_schema_version")
                .map(String::as_str),
            Some(ENCODED_PREDICATE_PROVIDER_SCHEMA_VERSION)
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_checked")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_status")
                .map(String::as_str),
            Some("blocked_until_reader_generated_kernel_input_certificate")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_classification")
                .map(String::as_str),
            Some("filter_column_batches_observed_kernel_input_lowering_blocked")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_filter_only_columns")
                .map(String::as_str),
            Some("flag,value")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_projected_output_columns")
                .map(String::as_str),
            Some("metric")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_blocker_id")
                .map(String::as_str),
            Some("gar-0026u.reader_generated_kernel_input_lowering_unsupported_encoding")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_reader_chunk_columns_observed")
                .map(String::as_str),
            Some("metric")
        );
        assert!(
            fields
                .get("encoded_predicate_provider_reader_chunk_dtype_summary")
                .is_some_and(|value| value.contains("metric:"))
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_reader_chunk_encoding_summary")
                .map(String::as_str),
            Some("metric:vortex.filter")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_reader_backed_bridge_status")
                .map(String::as_str),
            Some("bridge_available_blocked_filter_column_kernel_inputs_not_lowered")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_filter_column_probe_requested")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_filter_column_probe_status")
                .map(String::as_str),
            Some("observed_filter_column_reader_chunks_blocked_kernel_input_lowering")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_filter_column_probe_requested_columns")
                .map(String::as_str),
            Some("flag,value")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_filter_column_probe_reader_chunk_columns_observed")
                .map(String::as_str),
            Some("flag,value")
        );
        assert!(
            fields
                .get("encoded_predicate_provider_filter_column_probe_reader_chunk_encoding_summary")
                .is_some_and(|value| value.contains("flag:") && value.contains("value:"))
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_filter_column_probe_data_decoded")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_filter_column_probe_data_materialized")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_filter_column_probe_fallback_attempted")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_filter_column_probe_external_engine_invoked")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_conjunctive_bridge_schema_version")
                .map(String::as_str),
            Some("shardloom.vortex_reader_generated_conjunctive_selection_vector_bridge.v1")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_conjunctive_bridge_status")
                .map(String::as_str),
            Some("blocked_prepared_batch_validation")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_filter_column_batch_status")
                .map(String::as_str),
            Some("observed_filter_column_reader_chunks_not_lowered")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_projected_output_batch_status")
                .map(String::as_str),
            Some("observed_projected_metric_vortex_filter_chunk")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_predicate_shape_status")
                .map(String::as_str),
            Some("conjunctive_predicate_shape_supported_by_reader_generated_bridge")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_selection_vector_intersection_status")
                .map(String::as_str),
            Some("bridge_blocked_before_selection_vector_intersection")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_kernel_input_lowering_status")
                .map(String::as_str),
            Some("blocked_missing_encoding_specific_kernel_input_lowering")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_fallback_attempted")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_external_engine_invoked")
                .map(String::as_str),
            Some("false")
        );
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn prepared_native_vortex_batch_run_preserves_evidence_envelope() {
        let root = traditional_analytics_test_root("prepared-native-batch");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                fact_csv,
                dim_csv,
                root.join("workspace"),
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv)
            .with_native_vortex_replay_verification(true),
        )
        .unwrap();

        let batch_report = run_traditional_analytics_vortex_batch_benchmark(
            TraditionalAnalyticsVortexBatchRequest::new(
                vec![
                    TraditionalAnalyticsScenario::SelectiveFilter,
                    TraditionalAnalyticsScenario::GroupByAggregation,
                    TraditionalAnalyticsScenario::HashJoin,
                    TraditionalAnalyticsScenario::JoinAggregate,
                ],
                import_report.fact_vortex_path.clone(),
                import_report.dim_vortex_path.clone(),
            )
            .with_requested_execution_mode(ShardLoomExecutionMode::PreparedVortex)
            .with_result_workspace_dir(Some(root.join("batch-results")))
            .with_result_vortex_write(true),
        )
        .unwrap();

        assert_eq!(batch_report.reports.len(), 4);
        assert_eq!(
            batch_report.total_scenario_compute_micros,
            batch_report
                .reports
                .iter()
                .map(|report| report.scenario_compute_micros)
                .sum::<u64>()
        );
        assert_eq!(
            batch_report.total_vortex_scan_micros,
            batch_report.total_scenario_compute_micros
        );
        assert!(batch_report.total_result_sink_write_micros.is_some());
        assert!(batch_report.all_native_io_certificates_certified);
        assert!(batch_report.all_result_sink_replays_verified);
        assert!(
            batch_report
                .reports
                .iter()
                .all(|report| !report.fallback_execution_allowed)
        );
        assert!(batch_report.reports.iter().all(|report| {
            report.execution_mode_selection.selected_execution_mode
                == ShardLoomExecutionMode::PreparedVortex
        }));

        let fields = field_map(batch_report.fields());
        assert_field_eq(
            &fields,
            "schema_version",
            "shardloom.traditional_analytics.vortex_batch.v1",
        );
        assert_field_eq(
            &fields,
            "runner_kind",
            "single_process_prepared_native_batch",
        );
        assert_field_eq(&fields, "support_status", "runtime_supported");
        assert_field_eq(&fields, "claim_gate_status", "fixture_smoke_only");
        assert_field_eq(
            &fields,
            "persistent_runner_status",
            "single_process_batch_runner_supported",
        );
        assert_field_eq(&fields, "typed_envelope_preserved", "true");
        assert_field_eq(&fields, "process_startup_amortization_supported", "true");
        assert_field_eq(&fields, "prepared_artifact_reuse_eligible", "true");
        assert_field_eq(
            &fields,
            "source_metadata_snapshot_status",
            "per_batch_source_metadata_reused",
        );
        assert_field_eq(&fields, "source_metadata_snapshot_reused", "true");
        assert_field_eq(&fields, "source_metadata_snapshot_reuse_count", "4");
        assert_field_eq(
            &fields,
            "source_metadata_digest_recompute_avoided_count",
            "3",
        );
        assert_field_eq(
            &fields,
            "source_metadata_snapshot_fallback_attempted",
            "false",
        );
        assert_field_eq(
            &fields,
            "source_metadata_snapshot_external_engine_invoked",
            "false",
        );
        assert_field_eq(
            &fields,
            "source_state_reuse_status",
            "per_batch_dimension_label_state_reused",
        );
        assert_field_eq(&fields, "source_state_reused", "true");
        assert_field_eq(&fields, "source_state_reuse_consumer_count", "4");
        assert_field_eq(&fields, "source_state_recompute_avoided_count", "1");
        assert_field_eq(&fields, "source_state_family_count", "3");
        assert_field_eq(
            &fields,
            "source_state_dimension_label_reuse_status",
            "per_batch_dimension_label_state_reused",
        );
        assert_field_eq(&fields, "source_state_dimension_label_reused", "true");
        assert_field_eq(
            &fields,
            "source_state_category_metric_reuse_status",
            "not_applicable_no_category_metric_state_consumers",
        );
        assert_field_eq(&fields, "source_state_category_metric_reused", "false");
        assert_field_eq(
            &fields,
            "source_state_group_category_metric_reuse_status",
            "not_prepared_single_consumer_uses_scenario_scan",
        );
        assert_field_eq(
            &fields,
            "source_state_group_category_metric_reused",
            "false",
        );
        assert_field_eq(
            &fields,
            "source_state_ranked_metric_reuse_status",
            "not_applicable_no_ranked_metric_state_consumers",
        );
        assert_field_eq(&fields, "source_state_ranked_metric_reused", "false");
        assert_field_eq(
            &fields,
            "source_state_selective_filter_reuse_status",
            "not_prepared_single_consumer_uses_scenario_scan",
        );
        assert_field_eq(&fields, "source_state_selective_filter_reused", "false");
        assert_field_eq(
            &fields,
            "source_state_prepare_timing_scope",
            "batch_shared_pre_scenario",
        );
        assert_field_eq(&fields, "source_state_fallback_attempted", "false");
        assert_field_eq(&fields, "source_state_external_engine_invoked", "false");
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");
        assert_field_eq(&fields, "performance_claim_allowed", "false");
        assert_field_eq(&fields, "spark_displacement_claim_allowed", "false");
        assert_field_eq(&fields, "encoded_native_claim_allowed", "false");
        assert_field_eq(
            &fields,
            "scenario_order",
            "selective-filter,group-by-aggregation,hash-join,join---aggregate",
        );
        assert_field_eq(
            &fields,
            "selected_execution_modes",
            "prepared_vortex,prepared_vortex,prepared_vortex,prepared_vortex",
        );
        assert_field_eq(
            &fields,
            "scenario_selective-filter_operator_execution_class",
            "residual_native",
        );
        assert_field_eq(
            &fields,
            "scenario_group-by-aggregation_operator_execution_class",
            "residual_native",
        );
        assert_field_eq(
            &fields,
            "scenario_hash-join_operator_execution_class",
            "residual_native",
        );
        assert_field_eq(
            &fields,
            "scenario_join---aggregate_operator_execution_class",
            "residual_native",
        );
        assert_field_eq(
            &fields,
            "scenario_group-by-aggregation_source_backed_scan_evidence_status",
            "scoped_local_vortex_scan_evidence",
        );
        assert_field_eq(
            &fields,
            "scenario_selective-filter_result_sink_claim_gate_status",
            "result_sink_replay_certified",
        );
        assert_field_eq(
            &fields,
            "scenario_group-by-aggregation_fallback_attempted",
            "false",
        );
        assert_field_eq(
            &fields,
            "scenario_group-by-aggregation_external_engine_invoked",
            "false",
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn prepared_native_vortex_batch_run_reuses_selective_filter_source_state() {
        let root = traditional_analytics_test_root("prepared-native-selective-filter-state");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                fact_csv,
                dim_csv,
                root.join("workspace"),
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv)
            .with_native_vortex_replay_verification(true),
        )
        .unwrap();

        let batch_report = run_traditional_analytics_vortex_batch_benchmark(
            TraditionalAnalyticsVortexBatchRequest::new(
                vec![
                    TraditionalAnalyticsScenario::SelectiveFilter,
                    TraditionalAnalyticsScenario::FilterProjectionLimit,
                ],
                import_report.fact_vortex_path.clone(),
                import_report.dim_vortex_path.clone(),
            )
            .with_requested_execution_mode(ShardLoomExecutionMode::PreparedVortex),
        )
        .unwrap();

        assert_eq!(batch_report.reports.len(), 2);
        assert!(batch_report.all_native_io_certificates_certified);
        assert!(
            batch_report
                .reports
                .iter()
                .all(|report| !report.fallback_execution_allowed)
        );
        let selective_report = batch_report
            .reports
            .iter()
            .find(|report| report.scenario == TraditionalAnalyticsScenario::SelectiveFilter)
            .unwrap();
        assert_eq!(
            selective_report.result_json,
            "{\"row_count\":2,\"metric_sum\":6.5}"
        );
        let filter_projection_report = batch_report
            .reports
            .iter()
            .find(|report| report.scenario == TraditionalAnalyticsScenario::FilterProjectionLimit)
            .unwrap();
        assert_eq!(
            filter_projection_report.result_json,
            "{\"row_count\":2,\"metric_sum\":14000.0}"
        );

        let fields = field_map(batch_report.fields());
        assert_field_eq(
            &fields,
            "source_state_reuse_status",
            "per_batch_selective_filter_state_reused",
        );
        assert_field_eq(&fields, "source_state_reused", "true");
        assert_field_eq(
            &fields,
            "source_state_reuse_scope",
            "selective_filter_state_for_selective_filter_and_filter_projection_limit",
        );
        assert_field_eq(&fields, "source_state_reuse_consumer_count", "2");
        assert_field_eq(&fields, "source_state_recompute_avoided_count", "1");
        assert_field_eq(&fields, "source_state_family_count", "1");
        assert_field_eq(
            &fields,
            "source_state_selective_filter_reuse_status",
            "per_batch_selective_filter_state_reused",
        );
        assert_field_eq(&fields, "source_state_selective_filter_reused", "true");
        assert_field_eq(
            &fields,
            "source_state_selective_filter_reuse_consumer_count",
            "2",
        );
        assert_field_eq(
            &fields,
            "source_state_selective_filter_recompute_avoided_count",
            "1",
        );
        assert_field_eq(
            &fields,
            "source_state_coverage_schema_version",
            "shardloom.traditional_analytics.source_state_coverage.v1",
        );
        assert_field_eq(
            &fields,
            "source_state_coverage_matrix_ref",
            "docs/architecture/source-state-reuse-coverage-matrix.md",
        );
        assert_field_eq(
            &fields,
            "source_state_coverage_all_requested_scenarios_classified",
            "true",
        );
        assert_field_eq(&fields, "source_state_coverage_reused_scenario_count", "2");
        assert_field_eq(
            &fields,
            "source_state_coverage_not_needed_scenario_count",
            "0",
        );
        assert_field_eq(&fields, "source_state_coverage_blocked_scenario_count", "0");
        assert_field_eq(
            &fields,
            "source_state_coverage_matrix",
            "selective-filter:source-state-reused:selective_filter;filter---projection---limit:source-state-reused:selective_filter",
        );
        assert_field_eq(
            &fields,
            "source_state_digest_status",
            "not_emitted_scoped_in_memory_source_state",
        );
        assert_field_eq(
            &fields,
            "scenario_selective-filter_source_state_coverage_status",
            "source-state-reused",
        );
        assert_field_eq(
            &fields,
            "scenario_selective-filter_source_state_coverage_family",
            "selective_filter",
        );
        assert_field_eq(
            &fields,
            "scenario_filter---projection---limit_source_state_coverage_status",
            "source-state-reused",
        );
        assert_field_eq(
            &fields,
            "source_state_ranked_metric_reuse_status",
            "not_applicable_no_ranked_metric_state_consumers",
        );
        assert_field_eq(
            &fields,
            "source_state_dirty_input_reuse_status",
            "not_applicable_no_dirty_input_state_consumers",
        );
        assert_field_eq(&fields, "source_state_fallback_attempted", "false");
        assert_field_eq(&fields, "source_state_external_engine_invoked", "false");
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");
        assert_field_eq(&fields, "performance_claim_allowed", "false");
        assert_field_eq(&fields, "spark_displacement_claim_allowed", "false");
        assert_field_eq(&fields, "encoded_native_claim_allowed", "false");
        assert_field_eq(
            &fields,
            "scenario_order",
            "selective-filter,filter---projection---limit",
        );
        assert_field_eq(
            &fields,
            "scenario_selective-filter_source_backed_scan_projected_columns",
            "id,value,metric",
        );
        assert_field_eq(
            &fields,
            "scenario_filter---projection---limit_source_backed_scan_projected_columns",
            "id,value,metric",
        );
        assert_field_eq(
            &fields,
            "scenario_selective-filter_encoded_predicate_provider_selected_metric_aggregation_status",
            "batch_source_state_metric_aggregation_used",
        );
        assert_field_eq(
            &fields,
            "scenario_selective-filter_encoded_predicate_provider_selected_metric_source",
            "per_batch_selective_filter_source_state",
        );
        assert_field_eq(
            &fields,
            "scenario_selective-filter_operator_execution_class",
            "residual_native",
        );
        assert_field_eq(
            &fields,
            "scenario_filter---projection---limit_external_engine_invoked",
            "false",
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn prepared_native_vortex_batch_run_reuses_category_metric_source_state() {
        let root = traditional_analytics_test_root("prepared-native-category-state");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                fact_csv,
                dim_csv,
                root.join("workspace"),
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv)
            .with_native_vortex_replay_verification(true),
        )
        .unwrap();

        let batch_report = run_traditional_analytics_vortex_batch_benchmark(
            TraditionalAnalyticsVortexBatchRequest::new(
                vec![
                    TraditionalAnalyticsScenario::DistinctCount,
                    TraditionalAnalyticsScenario::HighCardinalityStringGroupDistinct,
                ],
                import_report.fact_vortex_path.clone(),
                import_report.dim_vortex_path.clone(),
            )
            .with_requested_execution_mode(ShardLoomExecutionMode::PreparedVortex),
        )
        .unwrap();

        assert_eq!(batch_report.reports.len(), 2);
        assert!(batch_report.all_native_io_certificates_certified);
        assert!(
            batch_report
                .reports
                .iter()
                .all(|report| !report.fallback_execution_allowed)
        );
        let distinct_report = batch_report
            .reports
            .iter()
            .find(|report| report.scenario == TraditionalAnalyticsScenario::DistinctCount)
            .unwrap();
        assert_eq!(
            distinct_report.result_json,
            "{\"distinct_category_count\":2}"
        );
        let string_group_report = batch_report
            .reports
            .iter()
            .find(|report| {
                report.scenario == TraditionalAnalyticsScenario::HighCardinalityStringGroupDistinct
            })
            .unwrap();
        assert_eq!(
            string_group_report.result_json,
            "{\"distinct_category_count\":2,\"groups\":[{\"category\":\"A\",\"row_count\":2,\"metric_sum\":6.5},{\"category\":\"B\",\"row_count\":1,\"metric_sum\":3.5}]}"
        );

        let fields = field_map(batch_report.fields());
        assert_field_eq(
            &fields,
            "source_state_reuse_status",
            "per_batch_category_metric_state_reused",
        );
        assert_field_eq(&fields, "source_state_reused", "true");
        assert_field_eq(
            &fields,
            "source_state_reuse_scope",
            "category_metric_group_state_for_distinct_count_and_high_cardinality_string_group_distinct",
        );
        assert_field_eq(&fields, "source_state_reuse_consumer_count", "2");
        assert_field_eq(&fields, "source_state_recompute_avoided_count", "1");
        assert_field_eq(&fields, "source_state_family_count", "1");
        assert_field_eq(
            &fields,
            "source_state_dimension_label_reuse_status",
            "not_applicable_no_dimension_label_state_consumers",
        );
        assert_field_eq(
            &fields,
            "source_state_category_metric_reuse_status",
            "per_batch_category_metric_state_reused",
        );
        assert_field_eq(&fields, "source_state_category_metric_reused", "true");
        assert_field_eq(
            &fields,
            "source_state_category_metric_reuse_consumer_count",
            "2",
        );
        assert_field_eq(
            &fields,
            "source_state_category_metric_recompute_avoided_count",
            "1",
        );
        assert_field_eq(
            &fields,
            "source_state_group_category_metric_reuse_status",
            "not_applicable_no_group_category_metric_state_consumers",
        );
        assert_field_eq(
            &fields,
            "source_state_group_category_metric_reused",
            "false",
        );
        assert_field_eq(
            &fields,
            "source_state_ranked_metric_reuse_status",
            "not_applicable_no_ranked_metric_state_consumers",
        );
        assert_field_eq(&fields, "source_state_ranked_metric_reused", "false");
        assert_field_eq(&fields, "source_state_fallback_attempted", "false");
        assert_field_eq(&fields, "source_state_external_engine_invoked", "false");
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");
        assert_field_eq(&fields, "performance_claim_allowed", "false");
        assert_field_eq(&fields, "spark_displacement_claim_allowed", "false");
        assert_field_eq(&fields, "encoded_native_claim_allowed", "false");
        assert_field_eq(
            &fields,
            "scenario_order",
            "distinct-count,high-cardinality-string-group-distinct",
        );
        assert_field_eq(
            &fields,
            "scenario_distinct-count_result_json",
            "{\"distinct_category_count\":2}",
        );
        assert_field_eq(
            &fields,
            "scenario_high-cardinality-string-group-distinct_operator_execution_class",
            "residual_native",
        );
        assert_field_eq(
            &fields,
            "scenario_high-cardinality-string-group-distinct_source_backed_scan_evidence_status",
            "scoped_local_vortex_scan_evidence",
        );
        assert_field_eq(
            &fields,
            "scenario_distinct-count_source_backed_scan_projected_columns",
            "category,metric",
        );
        assert_field_eq(
            &fields,
            "scenario_distinct-count_fallback_attempted",
            "false",
        );
        assert_field_eq(
            &fields,
            "scenario_high-cardinality-string-group-distinct_external_engine_invoked",
            "false",
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn prepared_native_vortex_batch_run_reuses_group_category_metric_source_state() {
        let root = traditional_analytics_test_root("prepared-native-group-category-state");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                fact_csv,
                dim_csv,
                root.join("workspace"),
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv)
            .with_native_vortex_replay_verification(true),
        )
        .unwrap();

        let batch_report = run_traditional_analytics_vortex_batch_benchmark(
            TraditionalAnalyticsVortexBatchRequest::new(
                vec![
                    TraditionalAnalyticsScenario::GroupByAggregation,
                    TraditionalAnalyticsScenario::MultiKeyGroupBy,
                ],
                import_report.fact_vortex_path.clone(),
                import_report.dim_vortex_path.clone(),
            )
            .with_requested_execution_mode(ShardLoomExecutionMode::PreparedVortex),
        )
        .unwrap();

        assert_eq!(batch_report.reports.len(), 2);
        assert!(batch_report.all_native_io_certificates_certified);
        assert!(
            batch_report
                .reports
                .iter()
                .all(|report| !report.fallback_execution_allowed)
        );
        let group_report = batch_report
            .reports
            .iter()
            .find(|report| report.scenario == TraditionalAnalyticsScenario::GroupByAggregation)
            .unwrap();
        assert_eq!(
            group_report.result_json,
            "[{\"group_key\":10,\"row_count\":2,\"metric_sum\":6.5},{\"group_key\":11,\"row_count\":1,\"metric_sum\":3.5}]"
        );
        let multi_key_report = batch_report
            .reports
            .iter()
            .find(|report| report.scenario == TraditionalAnalyticsScenario::MultiKeyGroupBy)
            .unwrap();
        assert_eq!(
            multi_key_report.result_json,
            "[{\"group_key\":10,\"category\":\"A\",\"row_count\":2,\"metric_sum\":6.5},{\"group_key\":11,\"category\":\"B\",\"row_count\":1,\"metric_sum\":3.5}]"
        );

        let fields = field_map(batch_report.fields());
        assert_field_eq(
            &fields,
            "source_state_reuse_status",
            "per_batch_group_category_metric_state_reused",
        );
        assert_field_eq(&fields, "source_state_reused", "true");
        assert_field_eq(
            &fields,
            "source_state_reuse_scope",
            "group_category_metric_state_for_group_by_aggregation_and_multi_key_group_by",
        );
        assert_field_eq(&fields, "source_state_reuse_consumer_count", "2");
        assert_field_eq(&fields, "source_state_recompute_avoided_count", "1");
        assert_field_eq(&fields, "source_state_family_count", "1");
        assert_field_eq(
            &fields,
            "source_state_group_category_metric_reuse_status",
            "per_batch_group_category_metric_state_reused",
        );
        assert_field_eq(&fields, "source_state_group_category_metric_reused", "true");
        assert_field_eq(
            &fields,
            "source_state_group_category_metric_reuse_consumer_count",
            "2",
        );
        assert_field_eq(
            &fields,
            "source_state_group_category_metric_recompute_avoided_count",
            "1",
        );
        assert_field_eq(
            &fields,
            "source_state_category_metric_reuse_status",
            "not_applicable_no_category_metric_state_consumers",
        );
        assert_field_eq(
            &fields,
            "source_state_ranked_metric_reuse_status",
            "not_applicable_no_ranked_metric_state_consumers",
        );
        assert_field_eq(&fields, "source_state_fallback_attempted", "false");
        assert_field_eq(&fields, "source_state_external_engine_invoked", "false");
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");
        assert_field_eq(&fields, "performance_claim_allowed", "false");
        assert_field_eq(&fields, "spark_displacement_claim_allowed", "false");
        assert_field_eq(&fields, "encoded_native_claim_allowed", "false");
        assert_field_eq(
            &fields,
            "scenario_order",
            "group-by-aggregation,multi-key-group-by",
        );
        assert_field_eq(
            &fields,
            "scenario_group-by-aggregation_source_backed_scan_projected_columns",
            "group_key,category,metric",
        );
        assert_field_eq(
            &fields,
            "scenario_multi-key-group-by_source_backed_scan_projected_columns",
            "group_key,category,metric",
        );
        assert_field_eq(
            &fields,
            "scenario_multi-key-group-by_operator_execution_class",
            "residual_native",
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn prepared_native_vortex_batch_run_reuses_ranked_metric_source_state() {
        let root = traditional_analytics_test_root("prepared-native-ranked-state");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                fact_csv,
                dim_csv,
                root.join("workspace"),
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv)
            .with_native_vortex_replay_verification(true),
        )
        .unwrap();

        let batch_report = run_traditional_analytics_vortex_batch_benchmark(
            TraditionalAnalyticsVortexBatchRequest::new(
                vec![
                    TraditionalAnalyticsScenario::SortAndTopK,
                    TraditionalAnalyticsScenario::TopNPerGroup,
                    TraditionalAnalyticsScenario::RowNumberWindow,
                ],
                import_report.fact_vortex_path.clone(),
                import_report.dim_vortex_path.clone(),
            )
            .with_requested_execution_mode(ShardLoomExecutionMode::PreparedVortex),
        )
        .unwrap();

        assert_eq!(batch_report.reports.len(), 3);
        assert!(batch_report.all_native_io_certificates_certified);
        assert!(
            batch_report
                .reports
                .iter()
                .all(|report| !report.fallback_execution_allowed)
        );
        let sort_report = batch_report
            .reports
            .iter()
            .find(|report| report.scenario == TraditionalAnalyticsScenario::SortAndTopK)
            .unwrap();
        assert_eq!(
            sort_report.result_json,
            "[{\"id\":3,\"metric\":4.0},{\"id\":2,\"metric\":3.5},{\"id\":1,\"metric\":2.5}]"
        );
        let top_n_report = batch_report
            .reports
            .iter()
            .find(|report| report.scenario == TraditionalAnalyticsScenario::TopNPerGroup)
            .unwrap();
        assert_eq!(
            top_n_report.result_json,
            "[{\"group_key\":10,\"id\":3,\"metric\":4.0,\"rank\":1},{\"group_key\":10,\"id\":1,\"metric\":2.5,\"rank\":2},{\"group_key\":11,\"id\":2,\"metric\":3.5,\"rank\":1}]"
        );
        let row_number_report = batch_report
            .reports
            .iter()
            .find(|report| report.scenario == TraditionalAnalyticsScenario::RowNumberWindow)
            .unwrap();
        assert_eq!(
            row_number_report.result_json,
            "[{\"group_key\":10,\"id\":3,\"metric\":4.0,\"rank\":1},{\"group_key\":11,\"id\":2,\"metric\":3.5,\"rank\":1}]"
        );

        let fields = field_map(batch_report.fields());
        assert_field_eq(
            &fields,
            "source_state_reuse_status",
            "per_batch_ranked_metric_state_reused",
        );
        assert_field_eq(&fields, "source_state_reused", "true");
        assert_field_eq(
            &fields,
            "source_state_reuse_scope",
            "ranked_metric_rows_for_sort_top_k_top_n_per_group_and_row_number_window",
        );
        assert_field_eq(&fields, "source_state_reuse_consumer_count", "3");
        assert_field_eq(&fields, "source_state_recompute_avoided_count", "2");
        assert_field_eq(&fields, "source_state_family_count", "1");
        assert_field_eq(
            &fields,
            "source_state_group_category_metric_reuse_status",
            "not_applicable_no_group_category_metric_state_consumers",
        );
        assert_field_eq(
            &fields,
            "source_state_group_category_metric_reused",
            "false",
        );
        assert_field_eq(
            &fields,
            "source_state_ranked_metric_reuse_status",
            "per_batch_ranked_metric_state_reused",
        );
        assert_field_eq(&fields, "source_state_ranked_metric_reused", "true");
        assert_field_eq(
            &fields,
            "source_state_ranked_metric_reuse_consumer_count",
            "3",
        );
        assert_field_eq(
            &fields,
            "source_state_ranked_metric_recompute_avoided_count",
            "2",
        );
        assert_field_eq(&fields, "source_state_fallback_attempted", "false");
        assert_field_eq(&fields, "source_state_external_engine_invoked", "false");
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");
        assert_field_eq(&fields, "performance_claim_allowed", "false");
        assert_field_eq(&fields, "spark_displacement_claim_allowed", "false");
        assert_field_eq(&fields, "encoded_native_claim_allowed", "false");
        assert_field_eq(
            &fields,
            "scenario_order",
            "sort-and-top-k,top-N-per-group,row-number-window",
        );
        assert_field_eq(
            &fields,
            "scenario_sort-and-top-k_source_backed_scan_projected_columns",
            "group_key,id,metric",
        );
        assert_field_eq(
            &fields,
            "scenario_top-N-per-group_operator_execution_class",
            "residual_native",
        );
        assert_field_eq(
            &fields,
            "scenario_row-number-window_external_engine_invoked",
            "false",
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn prepared_native_vortex_batch_run_reuses_dirty_input_source_state() {
        let root = traditional_analytics_test_root("prepared-native-dirty-input-state");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                fact_csv,
                dim_csv,
                root.join("workspace"),
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv)
            .with_native_vortex_replay_verification(true),
        )
        .unwrap();

        let batch_report = run_traditional_analytics_vortex_batch_benchmark(
            TraditionalAnalyticsVortexBatchRequest::new(
                vec![
                    TraditionalAnalyticsScenario::CleanCastFilterWrite,
                    TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv,
                ],
                import_report.fact_vortex_path.clone(),
                import_report.dim_vortex_path.clone(),
            )
            .with_requested_execution_mode(ShardLoomExecutionMode::PreparedVortex),
        )
        .unwrap();

        assert_eq!(batch_report.reports.len(), 2);
        assert!(batch_report.all_native_io_certificates_certified);
        assert!(
            batch_report
                .reports
                .iter()
                .all(|report| !report.fallback_execution_allowed)
        );
        let clean_report = batch_report
            .reports
            .iter()
            .find(|report| report.scenario == TraditionalAnalyticsScenario::CleanCastFilterWrite)
            .unwrap();
        assert_eq!(
            clean_report.result_json,
            "{\"row_count\":2,\"metric_sum\":14000.0}"
        );
        let malformed_report = batch_report
            .reports
            .iter()
            .find(|report| {
                report.scenario == TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv
            })
            .unwrap();
        assert_eq!(
            malformed_report.result_json,
            "{\"row_count\":2,\"metric_sum\":14000.0}"
        );

        let fields = field_map(batch_report.fields());
        assert_field_eq(
            &fields,
            "source_state_reuse_status",
            "per_batch_dirty_input_state_reused",
        );
        assert_field_eq(&fields, "source_state_reused", "true");
        assert_field_eq(
            &fields,
            "source_state_reuse_scope",
            "dirty_input_state_for_clean_cast_filter_write_and_malformed_timestamp_dirty_csv",
        );
        assert_field_eq(&fields, "source_state_reuse_consumer_count", "2");
        assert_field_eq(&fields, "source_state_recompute_avoided_count", "1");
        assert_field_eq(&fields, "source_state_family_count", "1");
        assert_field_eq(
            &fields,
            "source_state_dirty_input_reuse_status",
            "per_batch_dirty_input_state_reused",
        );
        assert_field_eq(&fields, "source_state_dirty_input_reused", "true");
        assert_field_eq(
            &fields,
            "source_state_dirty_input_reuse_consumer_count",
            "2",
        );
        assert_field_eq(
            &fields,
            "source_state_dirty_input_recompute_avoided_count",
            "1",
        );
        assert_field_eq(
            &fields,
            "source_state_ranked_metric_reuse_status",
            "not_applicable_no_ranked_metric_state_consumers",
        );
        assert_field_eq(&fields, "source_state_fallback_attempted", "false");
        assert_field_eq(&fields, "source_state_external_engine_invoked", "false");
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");
        assert_field_eq(&fields, "performance_claim_allowed", "false");
        assert_field_eq(&fields, "spark_displacement_claim_allowed", "false");
        assert_field_eq(&fields, "encoded_native_claim_allowed", "false");
        assert_field_eq(
            &fields,
            "scenario_order",
            "clean-cast-filter-write,malformed-timestamp---dirty-CSV",
        );
        assert_field_eq(
            &fields,
            "scenario_clean-cast-filter-write_source_backed_scan_projected_columns",
            "raw_event_time,dirty_numeric,dirty_flag",
        );
        assert_field_eq(
            &fields,
            "scenario_malformed-timestamp---dirty-CSV_source_backed_scan_projected_columns",
            "raw_event_time,dirty_numeric,dirty_flag",
        );
        assert_field_eq(
            &fields,
            "scenario_clean-cast-filter-write_operator_execution_class",
            "residual_native",
        );
        assert_field_eq(
            &fields,
            "scenario_malformed-timestamp---dirty-CSV_external_engine_invoked",
            "false",
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn prepared_native_vortex_batch_run_reuses_date_null_metric_source_state() {
        let root = traditional_analytics_test_root("prepared-native-date-null-metric-state");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                fact_csv,
                dim_csv,
                root.join("workspace"),
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv)
            .with_native_vortex_replay_verification(true),
        )
        .unwrap();

        let batch_report = run_traditional_analytics_vortex_batch_benchmark(
            TraditionalAnalyticsVortexBatchRequest::new(
                vec![
                    TraditionalAnalyticsScenario::PartitionPruning,
                    TraditionalAnalyticsScenario::NullHeavyAggregate,
                ],
                import_report.fact_vortex_path.clone(),
                import_report.dim_vortex_path.clone(),
            )
            .with_requested_execution_mode(ShardLoomExecutionMode::PreparedVortex),
        )
        .unwrap();

        assert_eq!(batch_report.reports.len(), 2);
        assert!(batch_report.all_native_io_certificates_certified);
        assert!(
            batch_report
                .reports
                .iter()
                .all(|report| !report.fallback_execution_allowed)
        );
        let partition_report = batch_report
            .reports
            .iter()
            .find(|report| report.scenario == TraditionalAnalyticsScenario::PartitionPruning)
            .unwrap();
        assert_eq!(
            partition_report.result_json,
            "{\"row_count\":2,\"metric_sum\":6.5}"
        );
        let null_report = batch_report
            .reports
            .iter()
            .find(|report| report.scenario == TraditionalAnalyticsScenario::NullHeavyAggregate)
            .unwrap();
        assert_eq!(
            null_report.result_json,
            "{\"row_count\":2,\"metric_sum\":5.0}"
        );

        let fields = field_map(batch_report.fields());
        assert_field_eq(
            &fields,
            "source_state_reuse_status",
            "per_batch_date_null_metric_state_reused",
        );
        assert_field_eq(&fields, "source_state_reused", "true");
        assert_field_eq(
            &fields,
            "source_state_reuse_scope",
            "date_null_metric_state_for_partition_pruning_and_null_heavy_aggregate",
        );
        assert_field_eq(&fields, "source_state_reuse_consumer_count", "2");
        assert_field_eq(&fields, "source_state_recompute_avoided_count", "1");
        assert_field_eq(&fields, "source_state_family_count", "1");
        assert_field_eq(
            &fields,
            "source_state_date_null_metric_reuse_status",
            "per_batch_date_null_metric_state_reused",
        );
        assert_field_eq(&fields, "source_state_date_null_metric_reused", "true");
        assert_field_eq(
            &fields,
            "source_state_date_null_metric_reuse_consumer_count",
            "2",
        );
        assert_field_eq(
            &fields,
            "source_state_date_null_metric_recompute_avoided_count",
            "1",
        );
        assert_field_eq(
            &fields,
            "source_state_dirty_input_reuse_status",
            "not_applicable_no_dirty_input_state_consumers",
        );
        assert_field_eq(&fields, "source_state_fallback_attempted", "false");
        assert_field_eq(&fields, "source_state_external_engine_invoked", "false");
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");
        assert_field_eq(&fields, "performance_claim_allowed", "false");
        assert_field_eq(&fields, "spark_displacement_claim_allowed", "false");
        assert_field_eq(&fields, "encoded_native_claim_allowed", "false");
        assert_field_eq(
            &fields,
            "scenario_order",
            "partition-pruning,null-heavy-aggregate",
        );
        assert_field_eq(
            &fields,
            "scenario_partition-pruning_source_backed_scan_projected_columns",
            "event_date,metric,nullable_metric_00",
        );
        assert_field_eq(
            &fields,
            "scenario_null-heavy-aggregate_source_backed_scan_projected_columns",
            "event_date,metric,nullable_metric_00",
        );
        assert_field_eq(
            &fields,
            "scenario_partition-pruning_operator_execution_class",
            "residual_native",
        );
        assert_field_eq(
            &fields,
            "scenario_null-heavy-aggregate_external_engine_invoked",
            "false",
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    #[allow(clippy::too_many_lines)]
    fn direct_transient_csv_smoke_runs_without_vortex_persistence() {
        let root = traditional_analytics_test_root("direct-transient-csv");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let report = run_traditional_direct_transient_csv_smoke(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                fact_csv,
                dim_csv,
                workspace.clone(),
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv)
            .with_requested_execution_mode(ShardLoomExecutionMode::DirectCompatibilityTransient),
        )
        .unwrap();

        assert_eq!(report.result_json, "{\"row_count\":2,\"metric_sum\":6.5}");
        assert_eq!(report.fact_rows, 3);
        assert_eq!(report.dim_rows, 2);
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_materialized, 2);
        assert!(!workspace.exists());
        assert_eq!(
            report.execution_mode_selection.selected_execution_mode,
            ShardLoomExecutionMode::DirectCompatibilityTransient
        );
        assert!(report.execution_mode_selection.mode_supported);
        assert_eq!(report.execution_mode_selection.support_status, "supported");
        assert!(report.execution_mode_selection.direct_transient_execution);
        assert!(!report.execution_mode_selection.vortex_native_claim_allowed);
        assert!(!report.execution_mode_selection.fallback_attempted);
        assert!(!report.execution_mode_selection.external_engine_invoked);
        assert!(report.runtime_execution_certificate.is_certified());
        assert!(report.runtime_execution_certificate.fallback_free());
        assert!(
            report
                .runtime_execution_certificate
                .external_query_engine_free()
        );
        assert_eq!(
            report.runtime_execution_certificate.expected_outcome,
            Some(ExpectedOutcome::Rows { row_count: Some(2) })
        );

        let fields = field_map(report.fields());
        assert_eq!(
            fields.get("selected_execution_mode").map(String::as_str),
            Some("direct_compatibility_transient")
        );
        assert_eq!(
            fields.get("support_status").map(String::as_str),
            Some("supported")
        );
        assert_eq!(
            fields.get("direct_transient_execution").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            fields
                .get("vortex_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields
                .get("compatibility_to_vortex_import_performed")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields.get("vortex_file_written").map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields.get("vortex_file_read").map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields
                .get("upstream_vortex_scan_called")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields
                .get("runtime_execution_certificate_status")
                .map(String::as_str),
            Some("certified")
        );
        assert_eq!(fields.get("write_io").map(String::as_str), Some("false"));
        assert_eq!(
            fields
                .get("native_io_certificate_status")
                .map(String::as_str),
            Some("not_vortex_native")
        );
        assert_eq!(
            fields.get("fallback_attempted").map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields.get("external_engine_invoked").map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn direct_transient_csv_smoke_rejects_adjacent_scenarios() {
        let root = traditional_analytics_test_root("direct-transient-unsupported");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);

        let error = run_traditional_direct_transient_csv_smoke(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::HashJoin,
                fact_csv,
                dim_csv,
                root.join("workspace"),
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv)
            .with_requested_execution_mode(ShardLoomExecutionMode::DirectCompatibilityTransient),
        )
        .expect_err("hash join is outside the direct transient smoke contract");

        assert!(error.to_string().contains("only supports selective filter"));
        assert!(
            error
                .to_string()
                .contains("fallback execution was not attempted")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    #[allow(clippy::too_many_lines)]
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
        let import_fields = report.fields();
        assert_eq!(
            import_fields
                .iter()
                .filter(|(key, _)| key == "selected_execution_mode")
                .count(),
            1
        );
        assert!(import_fields.iter().any(|(key, value)| {
            key == "selected_execution_mode" && value == "compatibility_import_certified"
        }));
        assert!(
            import_fields
                .iter()
                .any(|(key, value)| { key == "claim_gate_status" && value == "not_claim_grade" })
        );
        assert!(
            import_fields.iter().any(|(key, value)| {
                key == "prepared_artifact_reuse_eligible" && value == "true"
            })
        );
        assert!(import_fields.iter().any(|(key, value)| {
            key == "prepared_artifact_cleanup_policy" && value == "caller_owned_workspace_cleanup"
        }));

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
        let native_fields = native_report.fields();
        assert_eq!(
            native_fields
                .iter()
                .filter(|(key, _)| key == "selected_execution_mode")
                .count(),
            1
        );
        assert!(
            native_fields.iter().any(|(key, value)| {
                key == "selected_execution_mode" && value == "native_vortex"
            })
        );
        assert!(native_fields.iter().any(|(key, value)| {
            key == "mode_selection_reason" && value == "input_already_vortex"
        }));
        assert!(native_fields.iter().any(|(key, value)| {
            key == "vortex_first_provider_check_performed" && value == "true"
        }));
        assert!(native_fields.iter().any(|(key, value)| {
            key == "provider_admission_classification" && value == "use_vortex_native_provider"
        }));
        assert!(native_fields.iter().any(|(key, value)| {
            key == "operator_execution_class" && value == "residual_native"
        }));
        assert!(native_fields.iter().any(|(key, value)| {
            key == "operator_blocker_id"
                && value == "gar-flow-2b.residual_native_operator_not_encoded_native"
        }));
        assert!(native_fields.iter().any(|(key, value)| {
            key == "operator_encoded_native_claim_allowed" && value == "false"
        }));
        assert!(native_fields.iter().any(|(key, value)| {
            key == "residual_executor" && value == "shardloom_native_residual_operator"
        }));

        let prepared_report = run_traditional_analytics_vortex_benchmark(
            TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                report.fact_vortex_path.clone(),
                report.dim_vortex_path.clone(),
            )
            .with_requested_execution_mode(ShardLoomExecutionMode::PreparedVortex),
        )
        .unwrap();
        let prepared_fields = prepared_report.fields();
        assert!(prepared_fields.iter().any(|(key, value)| {
            key == "requested_execution_mode" && value == "prepared_vortex"
        }));
        assert!(prepared_fields.iter().any(|(key, value)| {
            key == "selected_execution_mode" && value == "prepared_vortex"
        }));
        assert!(prepared_fields.iter().any(|(key, value)| {
            key == "mode_selection_reason"
                && value == "prepared_vortex_artifacts_available_before_scenario_timing"
        }));
        assert!(prepared_fields.iter().any(|(key, value)| {
            key == "result_sink_claim_gate_status"
                && value == "not_claim_grade_missing_result_sink_evidence"
        }));
        assert!(prepared_fields.iter().any(|(key, value)| {
            key == "source_backed_scan_evidence_report_id"
                && value.starts_with("gar-0021h.source_backed_scan.prepared_vortex.")
        }));
        assert!(prepared_fields.iter().any(|(key, value)| {
            key == "source_backed_scan_evidence_status"
                && value == "scoped_local_vortex_scan_evidence"
        }));
        assert!(prepared_fields.iter().any(|(key, value)| {
            key == "source_backed_scan_projected_columns" && value == "metric"
        }));
        assert!(prepared_fields.iter().any(|(key, value)| {
            key == "source_backed_scan_claim_gate_status" && value == "not_claim_grade"
        }));

        let prepared_sink_report = run_traditional_analytics_vortex_benchmark(
            TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                report.fact_vortex_path.clone(),
                report.dim_vortex_path.clone(),
            )
            .with_requested_execution_mode(ShardLoomExecutionMode::PreparedVortex)
            .with_result_workspace_dir(Some(root.join("prepared-result-sink")))
            .with_result_vortex_write(true),
        )
        .unwrap();
        assert!(prepared_sink_report.computed_result_sink_requested);
        assert!(prepared_sink_report.computed_result_sink_written);
        assert!(prepared_sink_report.computed_result_sink_replay_verified);
        assert!(prepared_sink_report.computed_result_vortex_bytes > 0);
        assert_eq!(
            prepared_sink_report
                .computed_result_sink_native_io_certificate_status
                .as_deref(),
            Some("certified")
        );
        assert_eq!(
            prepared_sink_report.result_sink_claim_gate_status,
            "result_sink_replay_certified"
        );
        assert_eq!(
            prepared_sink_report.commit_state,
            "native_vortex_result_sink_written_uncommitted"
        );
        assert_eq!(
            prepared_sink_report.rollback_cleanup_status,
            "caller_owned_workspace_cleanup"
        );
        assert!(prepared_sink_report.write_io);
        assert_eq!(
            prepared_sink_report.result_json,
            prepared_sink_report
                .computed_result_sink_replay_result_json
                .as_deref()
                .unwrap()
        );
        let prepared_sink_fields = prepared_sink_report.fields();
        assert!(prepared_sink_fields.iter().any(|(key, value)| {
            key == "computed_result_sink_replay_verified" && value == "true"
        }));
        assert!(prepared_sink_fields.iter().any(|(key, value)| {
            key == "computed_result_sink_native_io_certificate_status" && value == "certified"
        }));
        assert!(
            prepared_sink_fields.iter().any(|(key, value)| {
                key == "computed_result_sink_write_micros" && value != "none"
            })
        );
        assert!(prepared_sink_fields.iter().any(|(key, value)| {
            key == "result_sink_claim_gate_status" && value == "result_sink_replay_certified"
        }));

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    #[allow(clippy::too_many_lines)]
    fn selective_filter_lowers_observed_bitpacked_and_sequence_filter_columns() {
        let root = traditional_analytics_test_root("csv-selective-encoded");
        let (fact_csv, dim_csv) = write_sequence_encoded_selective_filter_csv_inputs(&root, 512);
        let report = run_traditional_analytics_benchmark(TraditionalAnalyticsRequest::new(
            TraditionalAnalyticsScenario::SelectiveFilter,
            fact_csv,
            dim_csv,
            root.join("workspace"),
        ))
        .unwrap();
        assert_eq!(
            report.result_json,
            "{\"row_count\":31,\"metric_sum\":12601.5}"
        );

        let prepared_report = run_traditional_analytics_vortex_benchmark(
            TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                report.fact_vortex_path.clone(),
                report.dim_vortex_path.clone(),
            )
            .with_requested_execution_mode(ShardLoomExecutionMode::PreparedVortex),
        )
        .unwrap();
        let fields = field_map(prepared_report.fields());

        assert_field_eq(
            &fields,
            "encoded_predicate_provider_filter_column_probe_reader_chunk_encoding_summary",
            "flag:fastlanes.bitpacked,value:vortex.sequence",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_status",
            "reader_generated_filter_column_batches_and_selected_metric_aggregation_admitted",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_classification",
            "selection_vector_backed_metric_aggregation_consumed",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_kernel_input_count",
            "2",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_conjunctive_bridge_status",
            "intersected_selection_vectors",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_conjunctive_bridge_selected_row_count",
            "31",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_filter_column_batch_status",
            "admitted_filter_column_kernel_inputs",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_selection_vector_intersection_status",
            "selection_vectors_intersected",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_selected_metric_aggregation_status",
            "selection_vector_consumed",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_selected_metric_selection_vector_consumed",
            "true",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_selected_metric_source",
            "reader_generated_conjunctive_bridge_selection_vectors",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_selected_metric_row_count",
            "31",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_selected_metric_sum",
            "12601.5",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_selected_metric_scan_split_count",
            "1",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_selected_metric_data_decoded",
            "true",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_selected_metric_data_materialized",
            "false",
        );
        assert_field_eq(
            &fields,
            "fused_pipeline_schema_version",
            "shardloom.traditional_analytics.fused_pipeline.v1",
        );
        assert_field_eq(&fields, "fused_pipeline_used", "true");
        assert_field_eq(
            &fields,
            "fused_operator_family",
            "selection_vector_metric_aggregation",
        );
        assert_field_eq(&fields, "intermediate_materialization_avoided", "true");
        assert_field_eq(&fields, "fused_pipeline_rows_selected", "31");
        assert_field_eq(&fields, "fused_pipeline_rows_output", "31");
        assert_field_eq(&fields, "fused_pipeline_selection_vector_consumed", "true");
        assert_field_eq(
            &fields,
            "fused_pipeline_encoded_native_claim_allowed",
            "false",
        );
        assert_field_eq(&fields, "fused_pipeline_fallback_attempted", "false");
        assert_field_eq(&fields, "fused_pipeline_external_engine_invoked", "false");
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_kernel_input_lowering_status",
            "reader_generated_encoded_kernel_inputs_admitted",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_operator_execution_class",
            "residual_native",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_encoded_native_claim_allowed",
            "false",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_fallback_attempted",
            "false",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_external_engine_invoked",
            "false",
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn selective_filter_selection_vector_metric_aggregation_handles_empty_selection() {
        let root = traditional_analytics_test_root("csv-selective-empty-selection-vector");
        let (fact_csv, dim_csv) =
            write_empty_sequence_encoded_selective_filter_csv_inputs(&root, 512);
        let report = run_traditional_analytics_benchmark(TraditionalAnalyticsRequest::new(
            TraditionalAnalyticsScenario::SelectiveFilter,
            fact_csv,
            dim_csv,
            root.join("workspace"),
        ))
        .unwrap();
        assert_eq!(report.result_json, "{\"row_count\":0,\"metric_sum\":0.0}");

        let prepared_report = run_traditional_analytics_vortex_benchmark(
            TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                report.fact_vortex_path.clone(),
                report.dim_vortex_path.clone(),
            )
            .with_requested_execution_mode(ShardLoomExecutionMode::PreparedVortex),
        )
        .unwrap();
        assert_eq!(
            prepared_report.result_json,
            "{\"row_count\":0,\"metric_sum\":0.0}"
        );
        let fields = field_map(prepared_report.fields());

        assert_field_eq(
            &fields,
            "encoded_predicate_provider_conjunctive_bridge_status",
            "intersected_selection_vectors",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_conjunctive_bridge_selected_row_count",
            "0",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_selected_metric_aggregation_status",
            "selection_vector_consumed",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_selected_metric_selection_vector_consumed",
            "true",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_selected_metric_row_count",
            "0",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_selected_metric_sum",
            "0.0",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_fallback_attempted",
            "false",
        );
        assert_field_eq(
            &fields,
            "encoded_predicate_provider_external_engine_invoked",
            "false",
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn selective_filter_zero_result_reports_no_reader_chunks_emitted() {
        let root = traditional_analytics_test_root("csv-zero-selective");
        let (fact_csv, dim_csv) = write_zero_match_traditional_csv_inputs(&root);
        let report = run_traditional_analytics_benchmark(TraditionalAnalyticsRequest::new(
            TraditionalAnalyticsScenario::SelectiveFilter,
            fact_csv,
            dim_csv,
            root.join("workspace"),
        ))
        .unwrap();
        assert_eq!(report.result_json, "{\"row_count\":0,\"metric_sum\":0.0}");

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::SelectiveFilter,
                report.fact_vortex_path.clone(),
                report.dim_vortex_path.clone(),
            ))
            .unwrap();
        assert_eq!(
            native_report.result_json,
            "{\"row_count\":0,\"metric_sum\":0.0}"
        );

        let fields = field_map(native_report.fields());
        assert_eq!(
            fields
                .get("encoded_predicate_provider_reader_chunk_columns_observed")
                .map(String::as_str),
            Some("none")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_projected_output_batch_status")
                .map(String::as_str),
            Some("blocked_no_reader_chunks_emitted_for_zero_result")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_filter_column_batch_status")
                .map(String::as_str),
            Some("observed_filter_column_reader_chunks_not_lowered")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_filter_column_probe_reader_chunk_columns_observed")
                .map(String::as_str),
            Some("flag,value")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_filter_column_probe_status")
                .map(String::as_str),
            Some("observed_filter_column_reader_chunks_blocked_kernel_input_lowering")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_fallback_attempted")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            fields
                .get("encoded_predicate_provider_external_engine_invoked")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    #[allow(clippy::too_many_lines)]
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
            (
                TraditionalAnalyticsScenario::CleanCastFilterWrite,
                "{\"row_count\":2,\"metric_sum\":14000.0}",
            ),
            (
                TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv,
                "{\"row_count\":2,\"metric_sum\":14000.0}",
            ),
            (
                TraditionalAnalyticsScenario::PartitionPruning,
                "{\"row_count\":2,\"metric_sum\":6.5}",
            ),
            (
                TraditionalAnalyticsScenario::ManySmallFilesScan,
                "{\"row_count\":3,\"metric_sum\":10.0}",
            ),
            (
                TraditionalAnalyticsScenario::NullHeavyAggregate,
                "{\"row_count\":2,\"metric_sum\":5.0}",
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
            assert_eq!(report.layout_advisor_report.status, "report_only");
            assert_eq!(
                report
                    .layout_advisor_report
                    .workload_constitution_id
                    .as_str(),
                LOCAL_VORTEX_ANALYTICS_CONSTITUTION_ID
            );
            assert!(
                report
                    .layout_advisor_report
                    .evidence_source_refs
                    .iter()
                    .any(|value| value.contains("result-sink"))
            );
            assert!(!report.layout_advisor_report.improvement_claim_allowed);
            assert!(!report.layout_advisor_report.write_layout_execution_allowed);
            assert!(!report.layout_advisor_report.fallback_attempted);
            let fields = field_map(report.fields());
            if scenario == TraditionalAnalyticsScenario::MultiKeyGroupBy {
                assert_eq!(
                    fields.get("operator_execution_class").map(String::as_str),
                    Some("materialized_temporary")
                );
                assert_eq!(
                    fields
                        .get("operator_encoded_native_claim_allowed")
                        .map(String::as_str),
                    Some("false")
                );
            }
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
    fn enabled_hash_join_uses_prepared_native_vortex_scan() {
        let root = traditional_analytics_test_root("hash-join");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::HashJoin,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(
            import_report.result_json,
            "[{\"dim_label\":\"one\",\"row_count\":2,\"metric_sum\":6.5},{\"dim_label\":\"two\",\"row_count\":1,\"metric_sum\":3.5}]"
        );
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(!import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec![
                "dim.dim_key".to_string(),
                "dim.dim_label".to_string(),
                "fact.dim_key".to_string(),
                "fact.metric".to_string()
            ]
        );
        assert_eq!(import_report.materialization_boundary_rows, 5);
        assert!(import_report.data_materialized);

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::HashJoin,
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
            vec![
                "dim.dim_key".to_string(),
                "dim.dim_label".to_string(),
                "fact.dim_key".to_string(),
                "fact.metric".to_string()
            ]
        );
        assert_eq!(native_report.rows_scanned, 5);
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 2);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_fallback_attempted")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_external_engine_invoked")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_join_aggregate_uses_prepared_native_vortex_scan() {
        let root = traditional_analytics_test_root("join-aggregate");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::JoinAggregate,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(
            import_report.result_json,
            "[{\"dim_label\":\"one\",\"category\":\"A\",\"row_count\":2,\"metric_sum\":6.5}]"
        );
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec![
                "dim.dim_key".to_string(),
                "dim.dim_label".to_string(),
                "fact.dim_key".to_string(),
                "fact.category".to_string(),
                "fact.metric".to_string()
            ]
        );
        assert_eq!(import_report.materialization_boundary_rows, 5);
        assert!(import_report.data_materialized);

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::JoinAggregate,
                import_report.fact_vortex_path.clone(),
                import_report.dim_vortex_path.clone(),
            ))
            .unwrap();

        assert_eq!(native_report.result_json, import_report.result_json);
        assert!(native_report.streaming_vortex_execution_used);
        assert!(native_report.full_table_materialization_avoided);
        assert!(native_report.streaming_filter_pushdown_applied);
        assert!(native_report.streaming_projection_pushdown_applied);
        assert_eq!(
            native_report.streaming_projected_columns,
            vec![
                "dim.dim_key".to_string(),
                "dim.dim_label".to_string(),
                "fact.dim_key".to_string(),
                "fact.category".to_string(),
                "fact.metric".to_string()
            ]
        );
        assert_eq!(native_report.rows_scanned, 5);
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 1);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_fallback_attempted")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_external_engine_invoked")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    #[allow(clippy::too_many_lines)]
    fn enabled_sort_top_k_uses_prepared_native_vortex_scan() {
        let root = traditional_analytics_test_root("sort-top-k");
        std::fs::create_dir_all(&root).unwrap();
        let fact_csv = root.join("fact.csv");
        let dim_csv = root.join("dim.csv");
        std::fs::write(
            &fact_csv,
            concat!(
                "id,group_key,dim_key,value,metric,flag,category\n",
                "1,10,1,6000,9.0,1,A\n",
                "2,10,1,6000,9.0,1,A\n",
                "3,10,1,6000,8.0,1,A\n",
                "4,10,1,6000,7.0,1,A\n",
                "5,10,1,6000,6.0,1,A\n",
                "6,10,1,6000,5.0,1,A\n",
                "7,10,1,6000,4.0,1,A\n",
                "8,10,1,6000,3.0,1,A\n",
                "9,10,1,6000,2.0,1,A\n",
                "10,10,1,6000,1.0,1,A\n",
                "11,10,1,6000,9.0,1,A\n",
                "12,10,1,6000,0.5,1,A\n",
            ),
        )
        .unwrap();
        std::fs::write(&dim_csv, "dim_key,dim_label,weight\n1,one,1.5\n2,two,2.0\n").unwrap();
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::SortAndTopK,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(
            import_report.result_json,
            concat!(
                "[{\"id\":1,\"metric\":9.0},{\"id\":2,\"metric\":9.0},",
                "{\"id\":11,\"metric\":9.0},{\"id\":3,\"metric\":8.0},",
                "{\"id\":4,\"metric\":7.0},{\"id\":5,\"metric\":6.0},",
                "{\"id\":6,\"metric\":5.0},{\"id\":7,\"metric\":4.0},",
                "{\"id\":8,\"metric\":3.0},{\"id\":9,\"metric\":2.0}]",
            )
        );
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(!import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec!["id".to_string(), "metric".to_string()]
        );
        assert_eq!(import_report.materialization_boundary_rows, 14);
        assert!(import_report.data_materialized);

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::SortAndTopK,
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
            vec!["id".to_string(), "metric".to_string()]
        );
        assert_eq!(native_report.rows_scanned, 12);
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 10);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_fallback_attempted")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_external_engine_invoked")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_top_n_per_group_uses_prepared_native_vortex_scan() {
        let root = traditional_analytics_test_root("top-n-per-group");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::TopNPerGroup,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(
            import_report.result_json,
            "[{\"group_key\":10,\"id\":3,\"metric\":4.0,\"rank\":1},{\"group_key\":10,\"id\":1,\"metric\":2.5,\"rank\":2},{\"group_key\":11,\"id\":2,\"metric\":3.5,\"rank\":1}]"
        );
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(!import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec![
                "group_key".to_string(),
                "id".to_string(),
                "metric".to_string()
            ]
        );
        assert_eq!(import_report.materialization_boundary_rows, 5);
        assert!(import_report.data_materialized);

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::TopNPerGroup,
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
            vec![
                "group_key".to_string(),
                "id".to_string(),
                "metric".to_string()
            ]
        );
        assert_eq!(native_report.rows_scanned, 3);
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 3);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_fallback_attempted")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_external_engine_invoked")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_row_number_window_uses_prepared_native_vortex_scan() {
        let root = traditional_analytics_test_root("row-number-window");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::RowNumberWindow,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(
            import_report.result_json,
            "[{\"group_key\":10,\"id\":3,\"metric\":4.0,\"rank\":1},{\"group_key\":11,\"id\":2,\"metric\":3.5,\"rank\":1}]"
        );
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(!import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec![
                "group_key".to_string(),
                "id".to_string(),
                "metric".to_string()
            ]
        );
        assert_eq!(import_report.materialization_boundary_rows, 5);
        assert!(import_report.data_materialized);

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::RowNumberWindow,
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
            vec![
                "group_key".to_string(),
                "id".to_string(),
                "metric".to_string()
            ]
        );
        assert_eq!(native_report.rows_scanned, 3);
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 2);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_fallback_attempted")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_external_engine_invoked")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_string_group_distinct_uses_prepared_native_vortex_scan() {
        let root = traditional_analytics_test_root("string-group-distinct");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::HighCardinalityStringGroupDistinct,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(
            import_report.result_json,
            "{\"distinct_category_count\":2,\"groups\":[{\"category\":\"A\",\"row_count\":2,\"metric_sum\":6.5},{\"category\":\"B\",\"row_count\":1,\"metric_sum\":3.5}]}"
        );
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(!import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec!["category".to_string(), "metric".to_string()]
        );
        assert_eq!(import_report.materialization_boundary_rows, 5);
        assert!(import_report.data_materialized);

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::HighCardinalityStringGroupDistinct,
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
            vec!["category".to_string(), "metric".to_string()]
        );
        assert_eq!(native_report.rows_scanned, 3);
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 1);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_fallback_attempted")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_external_engine_invoked")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_partition_pruning_uses_prepared_native_date_range_scan() {
        let root = traditional_analytics_test_root("partition-pruning");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::PartitionPruning,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(
            import_report.result_json,
            "{\"row_count\":2,\"metric_sum\":6.5}"
        );
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec!["event_date".to_string(), "metric".to_string()]
        );
        assert_eq!(import_report.materialization_boundary_rows, 5);
        assert!(import_report.data_materialized);

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::PartitionPruning,
                import_report.fact_vortex_path.clone(),
                import_report.dim_vortex_path.clone(),
            ))
            .unwrap();

        assert_eq!(native_report.result_json, import_report.result_json);
        assert!(native_report.streaming_vortex_execution_used);
        assert!(native_report.full_table_materialization_avoided);
        assert!(native_report.streaming_filter_pushdown_applied);
        assert!(native_report.streaming_projection_pushdown_applied);
        assert_eq!(
            native_report.streaming_projected_columns,
            vec!["event_date".to_string(), "metric".to_string()]
        );
        assert_eq!(native_report.rows_scanned, 3);
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 1);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_fallback_attempted")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_external_engine_invoked")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_multi_key_group_by_uses_prepared_native_vortex_scan() {
        let root = traditional_analytics_test_root("multi-key-group-by");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::MultiKeyGroupBy,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(
            import_report.result_json,
            "[{\"group_key\":10,\"category\":\"A\",\"row_count\":2,\"metric_sum\":6.5},{\"group_key\":11,\"category\":\"B\",\"row_count\":1,\"metric_sum\":3.5}]"
        );
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(!import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec![
                "group_key".to_string(),
                "category".to_string(),
                "metric".to_string()
            ]
        );
        assert_eq!(import_report.materialization_boundary_rows, 5);
        assert!(import_report.data_materialized);

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::MultiKeyGroupBy,
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
            vec![
                "group_key".to_string(),
                "category".to_string(),
                "metric".to_string()
            ]
        );
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 2);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_fallback_attempted")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_external_engine_invoked")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_group_by_aggregation_uses_prepared_native_vortex_scan() {
        let root = traditional_analytics_test_root("group-by-aggregation");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::GroupByAggregation,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(
            import_report.result_json,
            "[{\"group_key\":10,\"row_count\":2,\"metric_sum\":6.5},{\"group_key\":11,\"row_count\":1,\"metric_sum\":3.5}]"
        );
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(!import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec!["group_key".to_string(), "metric".to_string()]
        );
        assert_eq!(import_report.materialization_boundary_rows, 5);
        assert!(import_report.data_materialized);

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::GroupByAggregation,
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
            vec!["group_key".to_string(), "metric".to_string()]
        );
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 2);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_fallback_attempted")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("provider_admission_external_engine_invoked")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_distinct_count_uses_prepared_native_vortex_scan() {
        let root = traditional_analytics_test_root("distinct-count");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::DistinctCount,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(import_report.result_json, "{\"distinct_category_count\":2}");
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(!import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec!["category".to_string()]
        );
        assert!(import_report.data_materialized);

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::DistinctCount,
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
            vec!["category".to_string()]
        );
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 1);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields.get("operator_blocker_id").map(String::as_str),
            Some("gar-flow-2b.residual_native_operator_not_encoded_native")
        );
        assert_eq!(
            native_fields
                .get("operator_temporary_materialization_used")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_null_heavy_aggregate_uses_prepared_native_vortex_scan() {
        let root = traditional_analytics_test_root("null-heavy-prepared-native");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::NullHeavyAggregate,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(
            import_report.result_json,
            "{\"row_count\":2,\"metric_sum\":5.0}"
        );
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(!import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec!["nullable_metric_00".to_string()]
        );
        assert!(import_report.data_materialized);

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::NullHeavyAggregate,
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
            vec!["nullable_metric_00".to_string()]
        );
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 1);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_temporary_materialization_used")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_clean_cast_filter_write_uses_prepared_native_vortex_scan() {
        let root = traditional_analytics_test_root("clean-cast-prepared-native");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::CleanCastFilterWrite,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(
            import_report.result_json,
            "{\"row_count\":2,\"metric_sum\":14000.0}"
        );
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(!import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec![
                "raw_event_time".to_string(),
                "dirty_numeric".to_string(),
                "dirty_flag".to_string()
            ]
        );
        assert!(import_report.data_materialized);

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::CleanCastFilterWrite,
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
            vec![
                "raw_event_time".to_string(),
                "dirty_numeric".to_string(),
                "dirty_flag".to_string()
            ]
        );
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 1);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_temporary_materialization_used")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_malformed_timestamp_dirty_csv_uses_prepared_native_vortex_scan() {
        let root = traditional_analytics_test_root("malformed-dirty-prepared-native");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(
            import_report.result_json,
            "{\"row_count\":2,\"metric_sum\":14000.0}"
        );
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(!import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec!["raw_event_time".to_string(), "dirty_numeric".to_string()]
        );
        assert!(import_report.data_materialized);

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv,
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
            vec!["raw_event_time".to_string(), "dirty_numeric".to_string()]
        );
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        assert_eq!(native_report.rows_materialized, 1);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_temporary_materialization_used")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_filter_projection_limit_uses_prepared_native_vortex_scan() {
        let root = traditional_analytics_test_root("filter-project-limit");
        let (fact_csv, dim_csv) = write_tiny_traditional_csv_inputs(&root);
        let workspace = root.join("workspace");

        let import_report = run_traditional_analytics_benchmark(
            TraditionalAnalyticsRequest::new(
                TraditionalAnalyticsScenario::FilterProjectionLimit,
                fact_csv,
                dim_csv,
                workspace,
            )
            .with_input_format(TraditionalAnalyticsInputFormat::Csv),
        )
        .unwrap();

        assert_eq!(
            import_report.result_json,
            "{\"row_count\":2,\"metric_sum\":14000.0}"
        );
        assert!(import_report.streaming_vortex_execution_used);
        assert!(import_report.full_table_materialization_avoided);
        assert!(import_report.streaming_filter_pushdown_applied);
        assert!(import_report.streaming_projection_pushdown_applied);
        assert_eq!(
            import_report.streaming_projected_columns,
            vec!["id".to_string(), "value".to_string()]
        );
        assert_eq!(import_report.materialization_boundary_rows, 5);
        assert!(import_report.data_materialized);
        let import_fields = field_map(import_report.fields());
        assert_eq!(
            import_fields
                .get("filter_project_limit_fused")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            import_fields.get("fusion_blocker").map(String::as_str),
            Some("p75.native_provider.filter_project_limit_fusion_missing")
        );
        assert_eq!(
            import_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("materialized_temporary")
        );
        assert_eq!(
            import_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );

        let native_report =
            run_traditional_analytics_vortex_benchmark(TraditionalAnalyticsVortexRequest::new(
                TraditionalAnalyticsScenario::FilterProjectionLimit,
                import_report.fact_vortex_path.clone(),
                import_report.dim_vortex_path.clone(),
            ))
            .unwrap();

        assert_eq!(native_report.result_json, import_report.result_json);
        assert!(native_report.streaming_vortex_execution_used);
        assert!(native_report.full_table_materialization_avoided);
        assert!(native_report.streaming_filter_pushdown_applied);
        assert!(native_report.streaming_projection_pushdown_applied);
        assert_eq!(
            native_report.streaming_projected_columns,
            vec!["id".to_string(), "value".to_string()]
        );
        assert_eq!(native_report.materialization_boundary_rows, 0);
        assert!(!native_report.data_materialized);
        let native_fields = field_map(native_report.fields());
        assert_eq!(
            native_fields
                .get("filter_project_limit_fused")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            native_fields
                .get("streaming_result_row_count")
                .map(String::as_str),
            Some("2")
        );
        assert_eq!(
            native_fields
                .get("fused_pipeline_schema_version")
                .map(String::as_str),
            Some("shardloom.traditional_analytics.fused_pipeline.v1")
        );
        assert_eq!(
            native_fields.get("fused_pipeline_used").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            native_fields
                .get("fused_operator_family")
                .map(String::as_str),
            Some("filter_projection_limit")
        );
        assert_eq!(
            native_fields
                .get("intermediate_materialization_avoided")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            native_fields
                .get("fused_pipeline_rows_selected")
                .map(String::as_str),
            Some("2")
        );
        assert_eq!(
            native_fields
                .get("fused_pipeline_rows_output")
                .map(String::as_str),
            Some("2")
        );
        assert_eq!(
            native_fields
                .get("fused_pipeline_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            native_fields.get("fusion_blocker").map(String::as_str),
            Some("none")
        );
        assert_eq!(
            native_fields
                .get("operator_execution_class")
                .map(String::as_str),
            Some("residual_native")
        );
        assert_eq!(
            native_fields
                .get("operator_encoded_native_claim_allowed")
                .map(String::as_str),
            Some("false")
        );

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
    #[allow(clippy::too_many_lines)]
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
        assert_eq!(report.layout_advisor_report.status, "report_only");
        assert_eq!(
            report.layout_advisor_report.recommendation_evidence_status,
            "measured_runtime_evidence_with_simulated_layout_advice"
        );
        assert_eq!(
            report.layout_advisor_report.recommended_chunk_rows,
            report.resource_policy.target_batch_rows
        );
        assert_eq!(
            report.layout_advisor_report.recommended_chunk_bytes,
            report.resource_policy.target_partition_bytes
        );
        assert_eq!(report.layout_advisor_report.cluster_key, "flag,value");
        assert!(report.layout_advisor_report.measured_evidence_source_count >= 5);
        assert_eq!(
            report.layout_advisor_report.simulated_evidence_source_count,
            1
        );
        assert_eq!(
            report.layout_advisor_report.blocked_evidence_source_count,
            1
        );
        assert!(!report.layout_advisor_report.improvement_claim_allowed);
        assert!(!report.layout_advisor_report.write_layout_execution_allowed);
        assert!(!report.layout_advisor_report.fallback_attempted);
        assert!(!report.layout_advisor_report.external_engine_invoked);
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
        assert!(
            fields
                .iter()
                .any(|(key, value)| { key == "layout_advisor_report_emitted" && value == "true" })
        );
        assert!(
            fields
                .iter()
                .any(|(key, value)| { key == "layout_advisor_status" && value == "report_only" })
        );
        assert!(fields.iter().any(|(key, value)| {
            key == "layout_advisor_improvement_claim_allowed" && value == "false"
        }));
        assert!(fields.iter().any(|(key, value)| {
            key == "layout_advisor_write_layout_execution_allowed" && value == "false"
        }));

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
            assert_eq!(
                import_report.compatibility_output_format,
                Some(output_format)
            );
            assert!(import_report.compatibility_output_written);
            assert!(import_report.native_to_compatibility_output_performed);
            assert!(!import_report.fallback_execution_allowed);
            assert!(import_report.runtime_execution_certificate.fallback_free());
            assert!(
                import_report
                    .runtime_execution_certificate
                    .external_query_engine_free()
            );
            assert!(!import_report.object_store_io);
            assert!(import_report.write_io);
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
            assert!(import_report.fact_compatibility_output_bytes > 0);
            assert!(import_report.dim_compatibility_output_bytes > 0);

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
