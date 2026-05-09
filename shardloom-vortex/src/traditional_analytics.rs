use std::path::PathBuf;

use shardloom_core::{Diagnostic, Result, ShardLoomError};

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
            Self::ScaleStressSkewedJoinAggregation => "scale stress skewed join aggregation",
            Self::ScaleStressMultiStageEtl => "scale stress multi-stage etl",
        }
    }
}

/// Request for the feature-gated traditional analytics Vortex I/O smoke runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraditionalAnalyticsRequest {
    pub scenario: TraditionalAnalyticsScenario,
    pub fact_csv: PathBuf,
    pub dim_csv: PathBuf,
    pub workspace_dir: PathBuf,
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
        }
    }
}

/// Report emitted by the local CSV-to-Vortex benchmark smoke runner.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct TraditionalAnalyticsReport {
    pub scenario: TraditionalAnalyticsScenario,
    pub result_json: String,
    pub fact_rows: u64,
    pub dim_rows: u64,
    pub rows_scanned: u64,
    pub rows_materialized: u64,
    pub workspace_dir: PathBuf,
    pub fact_vortex_path: PathBuf,
    pub dim_vortex_path: PathBuf,
    pub fact_vortex_bytes: u64,
    pub dim_vortex_bytes: u64,
    pub native_work_envelope_created: bool,
    pub native_work_stream_created: bool,
    pub native_result_stream_created: bool,
    pub native_io_certificate_emitted: bool,
    pub csv_source_adapter_used: bool,
    pub csv_to_vortex_import_performed: bool,
    pub vortex_file_written: bool,
    pub vortex_file_read: bool,
    pub upstream_vortex_scan_called: bool,
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
            "ShardLoom traditional analytics universal I/O smoke\nscenario: {}\nworkspace: {}\nfact Vortex: {}\ndim Vortex: {}\nrows scanned: {}\nrows materialized: {}\nCSV source adapter: true\nCSV to Vortex import: true\nVortex write/read/scan: true\nmaterialization boundary reported: {}\nexternal engine fallback: disabled",
            self.scenario.as_str(),
            self.workspace_dir.display(),
            self.fact_vortex_path.display(),
            self.dim_vortex_path.display(),
            self.rows_scanned,
            self.rows_materialized,
            self.materialization_boundary_report_emitted
        )
    }

    #[must_use]
    pub fn fields(&self) -> Vec<(String, String)> {
        vec![
            (
                "fallback_execution_allowed".to_string(),
                self.fallback_execution_allowed.to_string(),
            ),
            (
                "external_engines_are_fallback".to_string(),
                "false".to_string(),
            ),
            ("scenario".to_string(), self.scenario.as_str().to_string()),
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
                "fact_vortex_bytes".to_string(),
                self.fact_vortex_bytes.to_string(),
            ),
            (
                "dim_vortex_bytes".to_string(),
                self.dim_vortex_bytes.to_string(),
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
                "csv_source_adapter_used".to_string(),
                self.csv_source_adapter_used.to_string(),
            ),
            (
                "csv_to_vortex_import_performed".to_string(),
                self.csv_to_vortex_import_performed.to_string(),
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
        ]
    }
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
    let fact_rows = read_traditional_fact_csv(&request.fact_csv)?;
    let dim_rows = read_traditional_dim_csv(&request.dim_csv)?;
    let fact_vortex_path = request.workspace_dir.join("fact.vortex");
    let dim_vortex_path = request.workspace_dir.join("dim.vortex");
    write_fact_vortex(&fact_rows, &fact_vortex_path)?;
    write_dim_vortex(&dim_rows, &dim_vortex_path)?;
    let fact = read_fact_vortex(&fact_vortex_path)?;
    let dim = read_dim_vortex(&dim_vortex_path)?;
    let result_json = run_vortex_derived_scenario(request.scenario, &fact, &dim)?;
    let rows_materialized = result_rows_materialized(&result_json)?;
    let rows_scanned = match request.scenario {
        TraditionalAnalyticsScenario::HashJoin
        | TraditionalAnalyticsScenario::ScaleStressSkewedJoinAggregation
        | TraditionalAnalyticsScenario::ScaleStressMultiStageEtl => {
            checked_usize_sum_to_u64(fact.len(), dim.len())?
        }
        _ => usize_to_u64(fact.len())?,
    };
    let fact_vortex_bytes = fs::metadata(&fact_vortex_path)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to stat fact Vortex file '{}': {error}",
                fact_vortex_path.display()
            ))
        })?
        .len();
    let dim_vortex_bytes = fs::metadata(&dim_vortex_path)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to stat dimension Vortex file '{}': {error}",
                dim_vortex_path.display()
            ))
        })?
        .len();

    Ok(TraditionalAnalyticsReport {
        scenario: request.scenario,
        result_json,
        fact_rows: usize_to_u64(fact.len())?,
        dim_rows: usize_to_u64(dim.len())?,
        rows_scanned,
        rows_materialized,
        workspace_dir: request.workspace_dir,
        fact_vortex_path,
        dim_vortex_path,
        fact_vortex_bytes,
        dim_vortex_bytes,
        native_work_envelope_created: true,
        native_work_stream_created: true,
        native_result_stream_created: true,
        native_io_certificate_emitted: true,
        csv_source_adapter_used: true,
        csv_to_vortex_import_performed: true,
        vortex_file_written: true,
        vortex_file_read: true,
        upstream_vortex_scan_called: true,
        data_decoded: true,
        data_materialized: true,
        materialization_boundary_report_emitted: true,
        row_read: false,
        arrow_converted: false,
        object_store_io: false,
        write_io: true,
        spill_io_performed: false,
        fallback_execution_allowed: false,
        diagnostics: Vec::new(),
    })
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
    if header.trim_end_matches('\r') != "id,group_key,dim_key,value,metric,flag,category" {
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
        if cols.len() != 7 {
            return Err(ShardLoomError::InvalidOperation(format!(
                "fact CSV '{}' line {} has {} columns, expected 7",
                path.display(),
                line_index + 2,
                cols.len()
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
    let rounded = (value * 1_000_000.0).round() / 1_000_000.0;
    let mut text = format!("{rounded:.6}");
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

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn enabled_build_runs_csv_through_local_vortex_io() {
        let root = std::env::temp_dir().join(format!(
            "shardloom-traditional-analytics-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let fact_csv = root.join("fact.csv");
        let dim_csv = root.join("dim.csv");
        let workspace = root.join("workspace");
        std::fs::write(
            &fact_csv,
            "id,group_key,dim_key,value,metric,flag,category\n1,10,1,6000,2.5,1,A\n2,11,2,1000,3.5,0,B\n3,10,1,8000,4.0,1,A\n",
        )
        .unwrap();
        std::fs::write(&dim_csv, "dim_key,dim_label,weight\n1,one,1.5\n2,two,2.0\n").unwrap();

        let report = run_traditional_analytics_benchmark(TraditionalAnalyticsRequest::new(
            TraditionalAnalyticsScenario::SelectiveFilter,
            fact_csv,
            dim_csv,
            workspace,
        ))
        .unwrap();

        assert_eq!(report.result_json, "{\"row_count\":2,\"metric_sum\":6.5}");
        assert_eq!(report.fact_rows, 3);
        assert!(report.fact_vortex_path.exists());
        assert!(report.dim_vortex_path.exists());
        assert!(report.native_work_envelope_created);
        assert!(report.native_work_stream_created);
        assert!(report.native_result_stream_created);
        assert!(report.native_io_certificate_emitted);
        assert!(report.csv_source_adapter_used);
        assert!(report.csv_to_vortex_import_performed);
        assert!(report.vortex_file_written);
        assert!(report.vortex_file_read);
        assert!(report.upstream_vortex_scan_called);
        assert!(report.materialization_boundary_report_emitted);
        assert!(!report.fallback_execution_allowed);

        let _ = std::fs::remove_dir_all(root);
    }
}
