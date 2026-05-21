//! Feature-gated local `vortex_ingest` lifecycle helpers.
//!
//! This module intentionally exposes a narrow local prepare-once path for flat
//! scalar rows. It writes a local Vortex artifact, reopens/scans the artifact
//! for row-count proof, and returns evidence fields that callers can surface as
//! a `VortexPreparedState`. It is not a broad Vortex writer, object-store sink,
//! table commit path, or query-engine integration.

use std::path::PathBuf;

#[cfg(feature = "vortex-write")]
use std::{collections::BTreeSet, path::Path, time::Instant};

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
use std::collections::BTreeMap;

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
use arrow_array::{
    Array, ArrayRef as ArrowArrayRef, Date32Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
    TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use shardloom_core::{Result, ScalarValue, ShardLoomError, WorkspaceSafeLocalWriteReport};

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
use crate::universal_format_io::FlatLocalColumnarSource;

/// Request to write one flat scalar local source into a local Vortex artifact.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexPreparedStateWriteRequest {
    pub target_path: PathBuf,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<(String, ScalarValue)>>,
    pub allow_overwrite: bool,
    pub certification_level: VortexIngestCertificationLevel,
}

/// Request to write one flat columnar local source into a Vortex artifact.
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[derive(Debug, Clone, PartialEq)]
pub struct VortexPreparedStateColumnarWriteRequest {
    pub target_path: PathBuf,
    pub source: FlatLocalColumnarSource,
    pub allow_overwrite: bool,
    pub certification_level: VortexIngestCertificationLevel,
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
impl VortexPreparedStateColumnarWriteRequest {
    /// Create a request for a columnar local `VortexPreparedState` artifact write.
    #[must_use]
    pub fn new(target_path: impl Into<PathBuf>, source: FlatLocalColumnarSource) -> Self {
        Self {
            target_path: target_path.into(),
            source,
            allow_overwrite: false,
            certification_level: VortexIngestCertificationLevel::IngestCertified,
        }
    }

    /// Allow overwriting an existing local target artifact.
    #[must_use]
    pub const fn allow_overwrite(mut self, allow_overwrite: bool) -> Self {
        self.allow_overwrite = allow_overwrite;
        self
    }

    /// Set the requested ingest certification depth.
    #[must_use]
    pub const fn certification_level(
        mut self,
        certification_level: VortexIngestCertificationLevel,
    ) -> Self {
        self.certification_level = certification_level;
        self
    }
}

impl VortexPreparedStateWriteRequest {
    /// Create a request for a local `VortexPreparedState` artifact write.
    #[must_use]
    pub fn new(
        target_path: impl Into<PathBuf>,
        columns: Vec<String>,
        rows: Vec<Vec<(String, ScalarValue)>>,
    ) -> Self {
        Self {
            target_path: target_path.into(),
            columns,
            rows,
            allow_overwrite: false,
            certification_level: VortexIngestCertificationLevel::IngestCertified,
        }
    }

    /// Allow overwriting an existing local target artifact.
    #[must_use]
    pub const fn allow_overwrite(mut self, allow_overwrite: bool) -> Self {
        self.allow_overwrite = allow_overwrite;
        self
    }

    /// Set the requested ingest certification depth.
    #[must_use]
    pub const fn certification_level(
        mut self,
        certification_level: VortexIngestCertificationLevel,
    ) -> Self {
        self.certification_level = certification_level;
        self
    }
}

/// Certification depth for the scoped local `vortex_ingest` lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexIngestCertificationLevel {
    /// Write a local artifact and report bytes/digest/writer evidence only.
    IngestMinimal,
    /// Write and reopen/scan the artifact for row-count proof.
    IngestCertified,
    /// Requires downstream result replay/output evidence, so this prepare-only helper blocks it.
    IngestFullReplay,
}

impl VortexIngestCertificationLevel {
    /// Parse a command/API certification-depth token.
    ///
    /// # Errors
    /// Returns an error when the value is not one of the admitted certification
    /// depth tokens.
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim() {
            "ingest_minimal" => Ok(Self::IngestMinimal),
            "ingest_certified" => Ok(Self::IngestCertified),
            "ingest_full_replay" => Ok(Self::IngestFullReplay),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unknown vortex_ingest certification level '{other}'; expected ingest_minimal, ingest_certified, or ingest_full_replay; no fallback execution was attempted"
            ))),
        }
    }

    /// Return the canonical evidence token for this certification depth.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IngestMinimal => "ingest_minimal",
            Self::IngestCertified => "ingest_certified",
            Self::IngestFullReplay => "ingest_full_replay",
        }
    }
}

/// Evidence returned by the scoped local `vortex_ingest` helper.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexPreparedStateWriteReport {
    pub target_path: PathBuf,
    pub row_count: u64,
    pub column_count: usize,
    pub column_families: Vec<(String, String)>,
    pub bytes_written: u64,
    pub artifact_digest: String,
    pub digest_micros: u128,
    pub writer_row_count: u64,
    pub reopen_row_count: u64,
    pub array_build_micros: u128,
    pub write_micros: u128,
    pub reopen_scan_micros: u128,
    pub reopen_verification_status: String,
    pub timing_scope: String,
    pub certification_level: String,
    pub preparation_included: bool,
    pub query_timing_starts_after_preparation: bool,
    pub upstream_vortex_write_called: bool,
    pub upstream_vortex_scan_called: bool,
    pub array_build_provider_kind: String,
    pub array_build_provider_surface: String,
    pub array_build_strategy: String,
    pub array_build_input_layout: String,
    pub array_build_record_batch_count: usize,
    pub manual_scalar_copy_avoided: bool,
    pub workspace_write_report: WorkspaceSafeLocalWriteReport,
}

impl VortexPreparedStateWriteReport {
    /// Return a stable comma-separated column family summary.
    #[must_use]
    pub fn column_family_summary(&self) -> String {
        self.column_families
            .iter()
            .map(|(column, family)| format!("{column}:{family}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Return a compact schema/layout summary for evidence fields.
    #[must_use]
    pub fn layout_summary(&self) -> String {
        format!(
            "local_flat_struct;columns={};rows={}",
            self.column_count, self.row_count
        )
    }

    /// Return a compact encoding summary for evidence fields.
    #[must_use]
    pub fn encoding_summary(&self) -> String {
        format!(
            "upstream_vortex_writer_default;{}",
            self.column_family_summary()
        )
    }

    /// Return a compact statistics summary for evidence fields.
    #[must_use]
    pub fn statistics_summary(&self) -> String {
        format!(
            "writer_row_count={};reopen_row_count={};reopen_verification_status={};bytes_written={}",
            self.writer_row_count,
            self.reopen_row_count,
            self.reopen_verification_status,
            self.bytes_written
        )
    }
}

/// Whether local Vortex artifact writing is compiled into this crate.
#[must_use]
pub const fn vortex_ingest_write_feature_enabled() -> bool {
    cfg!(feature = "vortex-write")
}

/// Write flat scalar rows into a local Vortex artifact and reopen/scan it.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the feature gate is not
/// enabled, row shape/family support is outside the scoped contract, the target
/// already exists without overwrite permission, or upstream Vortex write/reopen
/// APIs fail.
#[cfg(not(feature = "vortex-write"))]
pub fn write_flat_scalar_vortex_prepared_state(
    _request: VortexPreparedStateWriteRequest,
) -> Result<VortexPreparedStateWriteReport> {
    Err(ShardLoomError::InvalidOperation(
        "local vortex_ingest runtime requires building shardloom-cli with --features vortex-write; default builds expose vortex_ingest as a deterministic blocked prepare-once route"
            .to_string(),
    ))
}

/// Write flat scalar rows into a local Vortex artifact and reopen/scan it.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when row shape/family support is
/// outside the scoped contract, the target already exists without overwrite
/// permission, or upstream Vortex write/reopen APIs fail.
#[cfg(feature = "vortex-write")]
pub fn write_flat_scalar_vortex_prepared_state(
    request: VortexPreparedStateWriteRequest,
) -> Result<VortexPreparedStateWriteReport> {
    if request.certification_level == VortexIngestCertificationLevel::IngestFullReplay {
        return Err(ShardLoomError::InvalidOperation(
            "local vortex_ingest ingest_full_replay requires downstream result replay/output evidence; use ingest_certified for prepare-once proof or run an output/replay workflow; no fallback execution was attempted"
                .to_string(),
        ));
    }

    let row_count = validate_flat_rows(&request.columns, &request.rows)?;
    let column_families = scalar_column_families(&request.columns, &request.rows)?;
    prepare_vortex_target(&request.target_path, request.allow_overwrite)?;
    let array_build_start = Instant::now();
    let array = flat_rows_to_vortex_struct(&request.columns, &request.rows, &column_families)?;
    let array_build_micros = array_build_start.elapsed().as_micros();
    finalize_vortex_prepared_state_write(VortexPreparedStateFinalizeInput {
        target_path: request.target_path,
        column_count: request.columns.len(),
        column_families,
        row_count,
        array: &array,
        array_build_micros,
        certification_level: request.certification_level,
        allow_overwrite: request.allow_overwrite,
        array_build_provider_kind: "shardloom_kernel",
        array_build_provider_surface: "shardloom_scalar_rows_to_vortex_struct",
        array_build_strategy: "scalar_rows_to_vortex_struct",
        array_build_input_layout: "materialized_rows",
        array_build_record_batch_count: 0,
        manual_scalar_copy_avoided: false,
    })
}

/// Write flat columnar Arrow batches into a local Vortex artifact and
/// reopen/scan it.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when columnar support is outside
/// the scoped contract, the target already exists without overwrite
/// permission, or upstream Vortex write/reopen APIs fail.
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
pub fn write_flat_columnar_vortex_prepared_state(
    request: VortexPreparedStateColumnarWriteRequest,
) -> Result<VortexPreparedStateWriteReport> {
    if request.certification_level == VortexIngestCertificationLevel::IngestFullReplay {
        return Err(ShardLoomError::InvalidOperation(
            "local vortex_ingest ingest_full_replay requires downstream result replay/output evidence; use ingest_certified for prepare-once proof or run an output/replay workflow; no fallback execution was attempted"
                .to_string(),
        ));
    }

    let source_shape = validate_flat_columnar_source_shape(&request.source)?;
    prepare_vortex_target(&request.target_path, request.allow_overwrite)?;
    let array_build_start = Instant::now();
    let array_build = flat_columnar_source_to_vortex_struct(&request.source, &source_shape)?;
    let array_build_micros = array_build_start.elapsed().as_micros();
    let row_count = usize_to_u64(request.source.row_count)?;
    finalize_vortex_prepared_state_write(VortexPreparedStateFinalizeInput {
        target_path: request.target_path,
        column_count: request.source.materialized_columns.len(),
        column_families: array_build.column_families,
        row_count,
        array: &array_build.array,
        array_build_micros,
        certification_level: request.certification_level,
        allow_overwrite: request.allow_overwrite,
        array_build_provider_kind: array_build.provider_kind,
        array_build_provider_surface: array_build.provider_surface,
        array_build_strategy: array_build.strategy,
        array_build_input_layout: "arrow_record_batch_columnar_source_state",
        array_build_record_batch_count: request.source.batches.len(),
        manual_scalar_copy_avoided: array_build.manual_scalar_copy_avoided,
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnarProjectedColumn {
    column: String,
    reader_index: usize,
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct FlatColumnarSourceShape {
    projected_columns: Vec<ColumnarProjectedColumn>,
}

#[cfg(feature = "vortex-write")]
fn prepare_vortex_target(target_path: &Path, allow_overwrite: bool) -> Result<()> {
    let workspace_root = shardloom_core::infer_local_output_workspace_root(target_path)?;
    shardloom_core::plan_workspace_safe_local_output(workspace_root, target_path, allow_overwrite)?;
    Ok(())
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn validate_flat_columnar_source_shape(
    source: &FlatLocalColumnarSource,
) -> Result<FlatColumnarSourceShape> {
    validate_flat_columns(&source.materialized_columns)?;
    validate_flat_columns(&source.reader_projection_columns)?;

    let reader_positions = source
        .reader_projection_columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let projected_columns = source
        .materialized_columns
        .iter()
        .map(|column| {
            let reader_index = reader_positions.get(column.as_str()).ok_or_else(|| {
                ShardLoomError::InvalidOperation(format!(
                    "local vortex_ingest columnar SourceState is missing projected column '{column}'; no fallback execution was attempted"
                ))
            })?;
            Ok(ColumnarProjectedColumn {
                column: column.clone(),
                reader_index: *reader_index,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut row_count = 0usize;
    for (batch_index, batch) in source.batches.iter().enumerate() {
        let expected_column_count = source.reader_projection_columns.len();
        if batch.num_columns() != expected_column_count {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest columnar SourceState batch {} has {} projected columns, expected {}; no fallback execution was attempted",
                batch_index + 1,
                batch.num_columns(),
                expected_column_count
            )));
        }
        let schema = batch.schema();
        if schema.fields().len() != expected_column_count {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest columnar SourceState batch {} schema has {} projected fields, expected {}; no fallback execution was attempted",
                batch_index + 1,
                schema.fields().len(),
                expected_column_count
            )));
        }
        for (column_index, expected_column) in source.reader_projection_columns.iter().enumerate() {
            let actual_column = schema.field(column_index).name();
            if actual_column != expected_column {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local vortex_ingest columnar SourceState batch {} projected field {} is '{}', expected '{}'; no fallback execution was attempted",
                    batch_index + 1,
                    column_index + 1,
                    actual_column,
                    expected_column
                )));
            }
        }
        row_count = row_count.checked_add(batch.num_rows()).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local vortex_ingest columnar SourceState row count overflowed usize; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
    }

    if row_count != source.row_count {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest columnar SourceState row count is {}, expected {}; no fallback execution was attempted",
            row_count, source.row_count
        )));
    }

    Ok(FlatColumnarSourceShape { projected_columns })
}

#[cfg(feature = "vortex-write")]
struct VortexPreparedStateFinalizeInput<'a> {
    target_path: PathBuf,
    column_count: usize,
    column_families: Vec<(String, String)>,
    row_count: u64,
    array: &'a vortex::array::ArrayRef,
    array_build_micros: u128,
    certification_level: VortexIngestCertificationLevel,
    allow_overwrite: bool,
    array_build_provider_kind: &'static str,
    array_build_provider_surface: &'static str,
    array_build_strategy: &'static str,
    array_build_input_layout: &'static str,
    array_build_record_batch_count: usize,
    manual_scalar_copy_avoided: bool,
}

#[cfg(feature = "vortex-write")]
fn finalize_vortex_prepared_state_write(
    input: VortexPreparedStateFinalizeInput<'_>,
) -> Result<VortexPreparedStateWriteReport> {
    let write_result = write_vortex_array(&input.target_path, input.array, input.allow_overwrite)?;

    let (
        reopen_row_count,
        reopen_scan_micros,
        reopen_verification_status,
        upstream_vortex_scan_called,
    ) = if input.certification_level == VortexIngestCertificationLevel::IngestCertified {
        let reopen_start = Instant::now();
        let reopen_row_count = reopen_vortex_row_count(&input.target_path)?;
        let reopen_scan_micros = reopen_start.elapsed().as_micros();
        if write_result.writer_row_count != input.row_count || reopen_row_count != input.row_count {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest row-count proof mismatch: source={} writer={} reopen={reopen_row_count}",
                input.row_count, write_result.writer_row_count
            )));
        }
        (
            reopen_row_count,
            reopen_scan_micros,
            "reopen_row_count_verified".to_string(),
            true,
        )
    } else {
        if write_result.writer_row_count != input.row_count {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest writer row count mismatch: source={} writer={}; no fallback execution was attempted",
                input.row_count, write_result.writer_row_count
            )));
        }
        (0, 0, "not_performed_ingest_minimal".to_string(), false)
    };

    Ok(VortexPreparedStateWriteReport {
        target_path: input.target_path,
        row_count: input.row_count,
        column_count: input.column_count,
        column_families: input.column_families,
        bytes_written: write_result.bytes_written,
        artifact_digest: write_result.artifact_digest,
        digest_micros: write_result.digest_micros,
        writer_row_count: write_result.writer_row_count,
        reopen_row_count,
        array_build_micros: input.array_build_micros,
        write_micros: write_result.write_micros,
        reopen_scan_micros,
        reopen_verification_status,
        timing_scope: "vortex_ingest_prepare_once".to_string(),
        certification_level: input.certification_level.as_str().to_string(),
        preparation_included: true,
        query_timing_starts_after_preparation: false,
        upstream_vortex_write_called: true,
        upstream_vortex_scan_called,
        array_build_provider_kind: input.array_build_provider_kind.to_string(),
        array_build_provider_surface: input.array_build_provider_surface.to_string(),
        array_build_strategy: input.array_build_strategy.to_string(),
        array_build_input_layout: input.array_build_input_layout.to_string(),
        array_build_record_batch_count: input.array_build_record_batch_count,
        manual_scalar_copy_avoided: input.manual_scalar_copy_avoided,
        workspace_write_report: write_result.workspace_write_report,
    })
}

#[cfg(feature = "vortex-write")]
fn validate_flat_rows(columns: &[String], rows: &[Vec<(String, ScalarValue)>]) -> Result<u64> {
    validate_flat_columns(columns)?;
    for (row_index, row) in rows.iter().enumerate() {
        if row.len() != columns.len() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest row {} has {} columns, expected {}; no fallback execution was attempted",
                row_index + 1,
                row.len(),
                columns.len()
            )));
        }
        for (column_index, (name, _value)) in row.iter().enumerate() {
            let expected = &columns[column_index];
            if name != expected {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local vortex_ingest row {} column {} is '{}', expected '{}'; no fallback execution was attempted",
                    row_index + 1,
                    column_index + 1,
                    name,
                    expected
                )));
            }
        }
    }
    usize_to_u64(rows.len())
}

#[cfg(feature = "vortex-write")]
fn validate_flat_columns(columns: &[String]) -> Result<()> {
    if columns.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "local vortex_ingest requires at least one column; no fallback execution was attempted"
                .to_string(),
        ));
    }
    let mut seen = BTreeSet::new();
    for column in columns {
        if column.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "local vortex_ingest column names must not be empty; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        if !seen.insert(column) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest contains duplicate column '{column}'; no fallback execution was attempted"
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "vortex-write")]
fn scalar_column_families(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<(String, String)>> {
    columns
        .iter()
        .enumerate()
        .map(|(column_index, column)| {
            let mut family: Option<&'static str> = None;
            for row in rows {
                let value = &row[column_index].1;
                let candidate = scalar_family(value).ok_or_else(|| {
                    ShardLoomError::InvalidOperation(format!(
                        "local vortex_ingest column '{column}' contains unsupported value {}; scoped Vortex ingest admits non-null int64, uint64, float64, utf8, date32, and timestamp_micros only; no fallback execution was attempted",
                        value.summary()
                    ))
                })?;
                if let Some(existing) = family {
                    if existing != candidate {
                        return Err(ShardLoomError::InvalidOperation(format!(
                            "local vortex_ingest column '{column}' mixes scalar families {existing} and {candidate}; no fallback execution was attempted"
                        )));
                    }
                } else {
                    family = Some(candidate);
                }
            }
            Ok((column.clone(), family.unwrap_or("utf8").to_string()))
        })
        .collect()
}

#[cfg(feature = "vortex-write")]
fn scalar_family(value: &ScalarValue) -> Option<&'static str> {
    match value {
        ScalarValue::Int64(_) => Some("int64"),
        ScalarValue::UInt64(_) => Some("uint64"),
        ScalarValue::Float64(value) if value.is_finite() => Some("float64"),
        ScalarValue::Utf8(_) => Some("utf8"),
        ScalarValue::Date32(_) => Some("date32"),
        ScalarValue::TimestampMicros(_) => Some("timestamp_micros"),
        ScalarValue::Null
        | ScalarValue::Boolean(_)
        | ScalarValue::Binary(_)
        | ScalarValue::Float64(_) => None,
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
struct ColumnarVortexArrayBuild {
    array: vortex::array::ArrayRef,
    column_families: Vec<(String, String)>,
    provider_kind: &'static str,
    provider_surface: &'static str,
    strategy: &'static str,
    manual_scalar_copy_avoided: bool,
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn flat_columnar_source_to_vortex_struct(
    source: &FlatLocalColumnarSource,
    source_shape: &FlatColumnarSourceShape,
) -> Result<ColumnarVortexArrayBuild> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::StructArray;
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let column_families = columnar_column_families(source, source_shape)?;
    if !source.batches.is_empty() {
        let array = flat_columnar_source_to_vortex_from_arrow_provider(source, source_shape)?;
        return Ok(ColumnarVortexArrayBuild {
            array,
            column_families,
            provider_kind: "vortex_array_kernel",
            provider_surface: "ArrayRef::from_arrow(RecordBatch)",
            strategy: "vortex_from_arrow_record_batch",
            manual_scalar_copy_avoided: true,
        });
    }

    let fields = column_families
        .iter()
        .zip(&source_shape.projected_columns)
        .map(|((column, family), projected_column)| {
            let arrays = source
                .batches
                .iter()
                .map(|batch| batch.column(projected_column.reader_index).clone())
                .collect::<Vec<_>>();
            columnar_column_to_vortex_array(column, family, &arrays)
        })
        .collect::<Result<Vec<_>>>()?;
    let array = StructArray::try_new(
        FieldNames::from(
            source
                .materialized_columns
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        ),
        fields,
        source.row_count,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    Ok(ColumnarVortexArrayBuild {
        array,
        column_families,
        provider_kind: "shardloom_kernel",
        provider_surface: "shardloom_empty_columnar_struct_builder",
        strategy: "empty_columnar_schema_to_vortex_struct",
        manual_scalar_copy_avoided: false,
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn flat_columnar_source_to_vortex_from_arrow_provider(
    source: &FlatLocalColumnarSource,
    source_shape: &FlatColumnarSourceShape,
) -> Result<vortex::array::ArrayRef> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::ChunkedArray;
    use vortex::array::arrow::FromArrowArray as _;

    let projection_indices = source_shape
        .projected_columns
        .iter()
        .map(|column| column.reader_index)
        .collect::<Vec<_>>();
    let chunks = source
        .batches
        .iter()
        .map(|batch| {
            let projected = batch.project(&projection_indices).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "local vortex_ingest Arrow RecordBatch projection failed: {error}; no fallback execution was attempted"
                ))
            })?;
            vortex::array::ArrayRef::from_arrow(projected, false).map_err(vortex_error)
        })
        .collect::<Result<Vec<_>>>()?;
    match chunks.as_slice() {
        [single] => Ok(single.clone()),
        [] => Err(ShardLoomError::InvalidOperation(
            "local vortex_ingest columnar SourceState contained no RecordBatch chunks; no fallback execution was attempted"
                .to_string(),
        )),
        _ => {
            let dtype = chunks[0].dtype().clone();
            Ok(ChunkedArray::try_new(chunks, dtype)
                .map_err(vortex_error)?
                .into_array())
        }
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_column_families(
    source: &FlatLocalColumnarSource,
    source_shape: &FlatColumnarSourceShape,
) -> Result<Vec<(String, String)>> {
    source_shape
        .projected_columns
        .iter()
        .map(|projected_column| {
            let mut family = None;
            for batch in &source.batches {
                let array = batch.column(projected_column.reader_index);
                reject_columnar_nulls(&projected_column.column, array.as_ref())?;
                let candidate = arrow_column_family(&projected_column.column, array.as_ref())?;
                if let Some(existing) = family {
                    if existing != candidate {
                        return Err(ShardLoomError::InvalidOperation(format!(
                            "local vortex_ingest column '{}' mixes columnar families {existing} and {candidate}; no fallback execution was attempted",
                            projected_column.column
                        )));
                    }
                } else {
                    family = Some(candidate);
                }
            }
            Ok((
                projected_column.column.clone(),
                family.unwrap_or("utf8").to_string(),
            ))
        })
        .collect()
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn reject_columnar_nulls(column: &str, array: &dyn Array) -> Result<()> {
    if array.null_count() > 0 {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest column '{column}' contains nulls; scoped Vortex ingest admits non-null int64, uint64, float64, utf8, date32, and timestamp_micros only; no fallback execution was attempted"
        )));
    }
    Ok(())
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn arrow_column_family(column: &str, array: &dyn Array) -> Result<&'static str> {
    if array.as_any().is::<Int8Array>()
        || array.as_any().is::<Int16Array>()
        || array.as_any().is::<Int32Array>()
        || array.as_any().is::<Int64Array>()
    {
        return Ok("int64");
    }
    if array.as_any().is::<UInt8Array>()
        || array.as_any().is::<UInt16Array>()
        || array.as_any().is::<UInt32Array>()
        || array.as_any().is::<UInt64Array>()
    {
        return Ok("uint64");
    }
    if array.as_any().is::<Float32Array>() || array.as_any().is::<Float64Array>() {
        return Ok("float64");
    }
    if array.as_any().is::<StringArray>()
        || array.as_any().is::<LargeStringArray>()
        || array.as_any().is::<StringViewArray>()
    {
        return Ok("utf8");
    }
    if array.as_any().is::<Date32Array>() {
        return Ok("date32");
    }
    if array.as_any().is::<TimestampMicrosecondArray>() {
        return Ok("timestamp_micros");
    }
    Err(ShardLoomError::InvalidOperation(format!(
        "local vortex_ingest column '{column}' has unsupported Arrow type {:?}; scoped Vortex ingest admits non-null int64, uint64, float64, utf8, date32, and timestamp_micros only; no fallback execution was attempted",
        array.data_type()
    )))
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_column_to_vortex_array(
    column: &str,
    family: &str,
    arrays: &[ArrowArrayRef],
) -> Result<vortex::array::ArrayRef> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, VarBinViewArray};

    match family {
        "int64" => Ok(columnar_int64_values(column, arrays)?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "uint64" => Ok(columnar_uint64_values(column, arrays)?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "float64" => Ok(columnar_float64_values(column, arrays)?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "utf8" => {
            let values = columnar_utf8_values(column, arrays)?;
            Ok(VarBinViewArray::from_iter_str(values.iter().map(String::as_str)).into_array())
        }
        "date32" => Ok(columnar_date32_values(column, arrays)?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "timestamp_micros" => Ok(columnar_timestamp_micros_values(column, arrays)?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest column '{column}' has unsupported columnar family {other}; no fallback execution was attempted"
        ))),
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_int64_values(column: &str, arrays: &[ArrowArrayRef]) -> Result<Vec<i64>> {
    let mut values = Vec::with_capacity(columnar_array_len(arrays));
    for array in arrays {
        if let Some(array) = array.as_any().downcast_ref::<Int8Array>() {
            values.extend((0..array.len()).map(|index| i64::from(array.value(index))));
        } else if let Some(array) = array.as_any().downcast_ref::<Int16Array>() {
            values.extend((0..array.len()).map(|index| i64::from(array.value(index))));
        } else if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
            values.extend((0..array.len()).map(|index| i64::from(array.value(index))));
        } else if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
            values.extend((0..array.len()).map(|index| array.value(index)));
        } else {
            return Err(unexpected_columnar_array(column, "int64", array.as_ref()));
        }
    }
    Ok(values)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_uint64_values(column: &str, arrays: &[ArrowArrayRef]) -> Result<Vec<u64>> {
    let mut values = Vec::with_capacity(columnar_array_len(arrays));
    for array in arrays {
        if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
            values.extend((0..array.len()).map(|index| u64::from(array.value(index))));
        } else if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
            values.extend((0..array.len()).map(|index| u64::from(array.value(index))));
        } else if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
            values.extend((0..array.len()).map(|index| u64::from(array.value(index))));
        } else if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
            values.extend((0..array.len()).map(|index| array.value(index)));
        } else {
            return Err(unexpected_columnar_array(column, "uint64", array.as_ref()));
        }
    }
    Ok(values)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_float64_values(column: &str, arrays: &[ArrowArrayRef]) -> Result<Vec<f64>> {
    let mut values = Vec::with_capacity(columnar_array_len(arrays));
    for array in arrays {
        if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
            for index in 0..array.len() {
                let value = array.value(index);
                if !value.is_finite() {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "local vortex_ingest column '{column}' contains non-finite float32; no fallback execution was attempted"
                    )));
                }
                values.push(f64::from(value));
            }
        } else if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
            for index in 0..array.len() {
                let value = array.value(index);
                if !value.is_finite() {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "local vortex_ingest column '{column}' contains non-finite float64; no fallback execution was attempted"
                    )));
                }
                values.push(value);
            }
        } else {
            return Err(unexpected_columnar_array(column, "float64", array.as_ref()));
        }
    }
    Ok(values)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_utf8_values(column: &str, arrays: &[ArrowArrayRef]) -> Result<Vec<String>> {
    let mut values = Vec::with_capacity(columnar_array_len(arrays));
    for array in arrays {
        if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
            values.extend((0..array.len()).map(|index| array.value(index).to_string()));
        } else if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
            values.extend((0..array.len()).map(|index| array.value(index).to_string()));
        } else if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
            values.extend((0..array.len()).map(|index| array.value(index).to_string()));
        } else {
            return Err(unexpected_columnar_array(column, "utf8", array.as_ref()));
        }
    }
    Ok(values)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_date32_values(column: &str, arrays: &[ArrowArrayRef]) -> Result<Vec<i32>> {
    let mut values = Vec::with_capacity(columnar_array_len(arrays));
    for array in arrays {
        if let Some(array) = array.as_any().downcast_ref::<Date32Array>() {
            values.extend((0..array.len()).map(|index| array.value(index)));
        } else {
            return Err(unexpected_columnar_array(column, "date32", array.as_ref()));
        }
    }
    Ok(values)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_timestamp_micros_values(column: &str, arrays: &[ArrowArrayRef]) -> Result<Vec<i64>> {
    let mut values = Vec::with_capacity(columnar_array_len(arrays));
    for array in arrays {
        if let Some(array) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            values.extend((0..array.len()).map(|index| array.value(index)));
        } else {
            return Err(unexpected_columnar_array(
                column,
                "timestamp_micros",
                array.as_ref(),
            ));
        }
    }
    Ok(values)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_array_len(arrays: &[ArrowArrayRef]) -> usize {
    arrays.iter().map(arrow_array::Array::len).sum()
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn unexpected_columnar_array(column: &str, family: &str, array: &dyn Array) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local vortex_ingest column '{column}' expected {family}, found Arrow type {:?}; no fallback execution was attempted",
        array.data_type()
    ))
}

#[cfg(feature = "vortex-write")]
fn flat_rows_to_vortex_struct(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
    column_families: &[(String, String)],
) -> Result<vortex::array::ArrayRef> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::StructArray;
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let fields = column_families
        .iter()
        .enumerate()
        .map(|(column_index, (_column, family))| {
            column_to_vortex_array(&columns[column_index], column_index, family, rows)
        })
        .collect::<Result<Vec<_>>>()?;

    let array = StructArray::try_new(
        FieldNames::from(columns.iter().map(String::as_str).collect::<Vec<_>>()),
        fields,
        rows.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    Ok(array)
}

#[cfg(feature = "vortex-write")]
fn column_to_vortex_array(
    column: &str,
    column_index: usize,
    family: &str,
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<vortex::array::ArrayRef> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, VarBinViewArray};

    match family {
        "int64" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::Int64(value) => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "uint64" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::UInt64(value) => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "float64" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::Float64(value) if value.is_finite() => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "utf8" => {
            let values = rows
                .iter()
                .map(|row| match &row[column_index].1 {
                    ScalarValue::Utf8(value) => Ok(value.as_str()),
                    value => Err(unexpected_vortex_ingest_value(column, family, value)),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(VarBinViewArray::from_iter_str(values).into_array())
        }
        "date32" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::Date32(value) => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "timestamp_micros" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::TimestampMicros(value) => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest column '{column}' has unsupported scalar family {other}; no fallback execution was attempted"
        ))),
    }
}

#[cfg(feature = "vortex-write")]
fn unexpected_vortex_ingest_value(
    column: &str,
    family: &str,
    value: &ScalarValue,
) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local vortex_ingest column '{column}' expected {family}, found {}; no fallback execution was attempted",
        value.summary()
    ))
}

#[cfg(feature = "vortex-write")]
struct LocalVortexWriteResult {
    writer_row_count: u64,
    bytes_written: u64,
    artifact_digest: String,
    digest_micros: u128,
    write_micros: u128,
    workspace_write_report: WorkspaceSafeLocalWriteReport,
}

#[cfg(feature = "vortex-write")]
fn write_vortex_array(
    path: &Path,
    array: &vortex::array::ArrayRef,
    allow_overwrite: bool,
) -> Result<LocalVortexWriteResult> {
    use vortex::VortexSessionDefault as _;
    use vortex::file::WriteOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut bytes = Vec::new();
    let write_start = Instant::now();
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
            "local vortex_ingest writer row count mismatch: wrote {}, expected {}; no fallback execution was attempted",
            summary.row_count(),
            expected_rows
        )));
    }
    let digest_start = Instant::now();
    let artifact_digest = fnv64_digest_bytes(&bytes);
    let digest_micros = digest_start.elapsed().as_micros();
    let bytes_written = usize_to_u64(bytes.len())?;
    let workspace_root = shardloom_core::infer_local_output_workspace_root(path)?;
    let workspace_write_report = shardloom_core::write_workspace_safe_bytes(
        workspace_root,
        path,
        allow_overwrite,
        "local vortex_ingest artifact",
        &bytes,
    )?;
    Ok(LocalVortexWriteResult {
        writer_row_count: summary.row_count(),
        bytes_written,
        artifact_digest,
        digest_micros,
        write_micros: write_start.elapsed().as_micros(),
        workspace_write_report,
    })
}

#[cfg(feature = "vortex-write")]
fn reopen_vortex_row_count(path: &Path) -> Result<u64> {
    use std::fs;

    use vortex::VortexSessionDefault as _;
    use vortex::array::stream::ArrayStreamExt as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let bytes = fs::read(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to reopen local vortex_ingest artifact '{}': {error}",
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
    usize_to_u64(array.len())
}

#[cfg(feature = "vortex-write")]
fn vortex_error(error: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local vortex_ingest upstream Vortex API failed: {error}; no fallback execution was attempted"
    ))
}

#[cfg(feature = "vortex-write")]
fn fnv64_digest_bytes(value: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv64:{hash:016x}")
}

#[cfg(feature = "vortex-write")]
fn usize_to_u64(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|_| {
        ShardLoomError::InvalidOperation(
            "local vortex_ingest value does not fit in u64".to_string(),
        )
    })
}

#[cfg(all(test, feature = "vortex-write"))]
mod tests {
    use super::*;

    #[test]
    fn local_flat_scalar_rows_write_and_reopen_vortex_artifact() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec!["id".to_string(), "label".to_string(), "metric".to_string()],
            vec![
                vec![
                    ("id".to_string(), ScalarValue::Int64(1)),
                    ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
                    ("metric".to_string(), ScalarValue::Float64(1.5)),
                ],
                vec![
                    ("id".to_string(), ScalarValue::Int64(2)),
                    ("label".to_string(), ScalarValue::Utf8("beta".to_string())),
                    ("metric".to_string(), ScalarValue::Float64(2.5)),
                ],
            ],
        );

        let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

        assert_eq!(report.row_count, 2);
        assert_eq!(report.reopen_row_count, 2);
        assert_eq!(
            report.reopen_verification_status,
            "reopen_row_count_verified"
        );
        assert!(report.artifact_digest.starts_with("fnv64:"));
        assert_eq!(report.timing_scope, "vortex_ingest_prepare_once");
        assert_eq!(report.certification_level, "ingest_certified");
        assert!(report.preparation_included);
        assert!(!report.query_timing_starts_after_preparation);
        assert_eq!(report.array_build_provider_kind, "shardloom_kernel");
        assert_eq!(
            report.array_build_provider_surface,
            "shardloom_scalar_rows_to_vortex_struct"
        );
        assert_eq!(report.array_build_strategy, "scalar_rows_to_vortex_struct");
        assert_eq!(report.array_build_input_layout, "materialized_rows");
        assert_eq!(report.array_build_record_batch_count, 0);
        assert!(!report.manual_scalar_copy_avoided);
        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[test]
    fn local_flat_scalar_minimal_ingest_skips_reopen_scan() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-minimal-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec!["id".to_string(), "label".to_string()],
            vec![vec![
                ("id".to_string(), ScalarValue::Int64(1)),
                ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
            ]],
        )
        .certification_level(VortexIngestCertificationLevel::IngestMinimal);

        let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

        assert_eq!(report.row_count, 1);
        assert_eq!(report.writer_row_count, 1);
        assert_eq!(report.reopen_row_count, 0);
        assert_eq!(
            report.reopen_verification_status,
            "not_performed_ingest_minimal"
        );
        assert_eq!(report.certification_level, "ingest_minimal");
        assert!(report.upstream_vortex_write_called);
        assert!(!report.upstream_vortex_scan_called);
        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    fn local_flat_columnar_source_writes_and_reopens_vortex_artifact() {
        use std::sync::Arc;

        use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-columnar-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let columns = vec!["id".to_string(), "label".to_string(), "metric".to_string()];
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("metric", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
                Arc::new(Float64Array::from(vec![1.5, 2.5])),
            ],
        )
        .expect("record batch");
        let source = FlatLocalColumnarSource {
            header: columns.clone(),
            materialized_columns: columns.clone(),
            reader_projection_columns: columns,
            batches: vec![batch],
            row_count: 2,
        };
        let request = VortexPreparedStateColumnarWriteRequest::new(&path, source);

        let report = write_flat_columnar_vortex_prepared_state(request).expect("write report");

        assert_eq!(report.row_count, 2);
        assert_eq!(report.reopen_row_count, 2);
        assert_eq!(
            report.column_family_summary(),
            "id:int64,label:utf8,metric:float64"
        );
        assert_eq!(
            report.reopen_verification_status,
            "reopen_row_count_verified"
        );
        assert!(report.upstream_vortex_write_called);
        assert!(report.upstream_vortex_scan_called);
        assert_eq!(report.array_build_provider_kind, "vortex_array_kernel");
        assert_eq!(
            report.array_build_provider_surface,
            "ArrayRef::from_arrow(RecordBatch)"
        );
        assert_eq!(
            report.array_build_strategy,
            "vortex_from_arrow_record_batch"
        );
        assert_eq!(
            report.array_build_input_layout,
            "arrow_record_batch_columnar_source_state"
        );
        assert_eq!(report.array_build_record_batch_count, 1);
        assert!(report.manual_scalar_copy_avoided);
        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    fn local_flat_columnar_source_rejects_short_batches_before_column_access() {
        use std::sync::Arc;

        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-columnar-short-batch-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
            ],
        )
        .expect("record batch");
        let columns = vec!["id".to_string(), "label".to_string(), "metric".to_string()];
        let source = FlatLocalColumnarSource {
            header: columns.clone(),
            materialized_columns: columns.clone(),
            reader_projection_columns: columns,
            batches: vec![batch],
            row_count: 2,
        };
        let request = VortexPreparedStateColumnarWriteRequest::new(&path, source);

        let error = write_flat_columnar_vortex_prepared_state(request)
            .expect_err("short batch must be rejected before column access");

        assert!(
            error
                .to_string()
                .contains("columnar SourceState batch 1 has 2 projected columns, expected 3")
        );
        assert!(
            error
                .to_string()
                .contains("no fallback execution was attempted")
        );
        assert!(!path.exists());
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    fn local_flat_columnar_source_rejects_row_count_mismatch() {
        use std::sync::Arc;

        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-columnar-row-count-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let columns = vec!["id".to_string(), "label".to_string()];
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
            ],
        )
        .expect("record batch");
        let source = FlatLocalColumnarSource {
            header: columns.clone(),
            materialized_columns: columns.clone(),
            reader_projection_columns: columns,
            batches: vec![batch],
            row_count: 3,
        };
        let request = VortexPreparedStateColumnarWriteRequest::new(&path, source);

        let error = write_flat_columnar_vortex_prepared_state(request)
            .expect_err("row count mismatch must be rejected");

        assert!(
            error
                .to_string()
                .contains("columnar SourceState row count is 2, expected 3")
        );
        assert!(
            error
                .to_string()
                .contains("no fallback execution was attempted")
        );
        assert!(!path.exists());
    }

    #[test]
    fn local_flat_scalar_full_replay_is_blocked_without_output_replay() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-full-replay-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec!["id".to_string(), "label".to_string()],
            vec![vec![
                ("id".to_string(), ScalarValue::Int64(1)),
                ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
            ]],
        )
        .certification_level(VortexIngestCertificationLevel::IngestFullReplay);

        let error = write_flat_scalar_vortex_prepared_state(request)
            .expect_err("full replay requires downstream output evidence");

        assert!(
            error
                .to_string()
                .contains("ingest_full_replay requires downstream result replay/output evidence")
        );
        assert!(!path.exists());
    }
}
