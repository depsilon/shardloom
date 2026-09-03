use std::{
    collections::hash_map::DefaultHasher,
    fmt::Write as _,
    hash::{Hash, Hasher},
};

use shardloom_core::{Diagnostic, DiagnosticCode, Result, ScalarValue, ShardLoomError};

use crate::VortexOutputPayloadContentDescriptor;

pub const VORTEX_COLUMNAR_RESULT_DATAPLANE_SCHEMA_VERSION: &str =
    "shardloom.vortex_columnar_result_dataplane.v1";
pub const VORTEX_COLUMNAR_RESULT_MATERIALIZATION_SCHEMA_VERSION: &str =
    "shardloom.vortex_columnar_result_materialization.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexColumnarResultSinkBoundary {
    NativeVortex,
    ArrowCompatible,
    Jsonl,
    Csv,
    CliText,
    PythonRows,
    RemoteDeliveryUnsupported,
}

impl VortexColumnarResultSinkBoundary {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NativeVortex => "native_vortex",
            Self::ArrowCompatible => "arrow_compatible",
            Self::Jsonl => "jsonl",
            Self::Csv => "csv",
            Self::CliText => "cli_text",
            Self::PythonRows => "python_rows",
            Self::RemoteDeliveryUnsupported => "remote_delivery_unsupported",
        }
    }

    #[must_use]
    pub const fn materializes_rows(self) -> bool {
        matches!(
            self,
            Self::Jsonl | Self::Csv | Self::CliText | Self::PythonRows
        )
    }

    #[must_use]
    pub const fn unsupported(self) -> bool {
        matches!(self, Self::RemoteDeliveryUnsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexColumnarResultOrdering {
    Unordered,
    SourceOrder,
    StableSort,
    StableTopK,
    GroupKeyOrder,
}

impl VortexColumnarResultOrdering {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unordered => "unordered",
            Self::SourceOrder => "source_order",
            Self::StableSort => "stable_sort",
            Self::StableTopK => "stable_topk",
            Self::GroupKeyOrder => "group_key_order",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VortexColumnarResultColumnStorage {
    ScalarValues(Vec<ScalarValue>),
    OpaqueEncoded {
        row_count: u64,
        encoding_layout: String,
        estimated_bytes: Option<u64>,
    },
    RetainedRowRefs(Vec<u64>),
}

impl VortexColumnarResultColumnStorage {
    #[must_use]
    pub fn row_count(&self) -> u64 {
        match self {
            Self::ScalarValues(values) => u64::try_from(values.len()).unwrap_or(u64::MAX),
            Self::OpaqueEncoded { row_count, .. } => *row_count,
            Self::RetainedRowRefs(row_refs) => u64::try_from(row_refs.len()).unwrap_or(u64::MAX),
        }
    }

    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ScalarValues(_) => "scalar_values",
            Self::OpaqueEncoded { .. } => "opaque_encoded",
            Self::RetainedRowRefs(_) => "retained_row_refs",
        }
    }

    #[must_use]
    pub const fn row_materialization_ready(&self) -> bool {
        matches!(self, Self::ScalarValues(_))
    }

    #[must_use]
    pub fn estimated_payload_bytes(&self) -> u64 {
        match self {
            Self::ScalarValues(values) => values.iter().map(scalar_value_payload_bytes).sum(),
            Self::OpaqueEncoded {
                row_count,
                estimated_bytes,
                ..
            } => estimated_bytes.unwrap_or(*row_count),
            Self::RetainedRowRefs(row_refs) => u64::try_from(row_refs.len())
                .unwrap_or(u64::MAX)
                .saturating_mul(8),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexColumnarResultColumn {
    pub name: String,
    pub logical_dtype: String,
    pub storage: VortexColumnarResultColumnStorage,
}

impl VortexColumnarResultColumn {
    /// # Errors
    /// Returns an error when the column name is empty.
    pub fn scalar_values(
        name: impl Into<String>,
        logical_dtype: impl Into<String>,
        values: Vec<ScalarValue>,
    ) -> Result<Self> {
        Self::new(
            name,
            logical_dtype,
            VortexColumnarResultColumnStorage::ScalarValues(values),
        )
    }

    /// # Errors
    /// Returns an error when the column name is empty.
    pub fn opaque_encoded(
        name: impl Into<String>,
        logical_dtype: impl Into<String>,
        row_count: u64,
        encoding_layout: impl Into<String>,
        estimated_bytes: Option<u64>,
    ) -> Result<Self> {
        Self::new(
            name,
            logical_dtype,
            VortexColumnarResultColumnStorage::OpaqueEncoded {
                row_count,
                encoding_layout: encoding_layout.into(),
                estimated_bytes,
            },
        )
    }

    /// # Errors
    /// Returns an error when the column name is empty.
    pub fn retained_row_refs(name: impl Into<String>, row_refs: Vec<u64>) -> Result<Self> {
        Self::new(
            name,
            "row_ref",
            VortexColumnarResultColumnStorage::RetainedRowRefs(row_refs),
        )
    }

    fn new(
        name: impl Into<String>,
        logical_dtype: impl Into<String>,
        storage: VortexColumnarResultColumnStorage,
    ) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "columnar result column name must not be empty".to_string(),
            ));
        }
        let logical_dtype = logical_dtype.into();
        if logical_dtype.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "columnar result logical dtype must not be empty".to_string(),
            ));
        }
        Ok(Self {
            name,
            logical_dtype,
            storage,
        })
    }

    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.storage.row_count()
    }

    #[must_use]
    pub fn estimated_payload_bytes(&self) -> u64 {
        self.storage.estimated_payload_bytes()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexRetainedRowSet {
    pub source_ref: String,
    pub row_refs: Vec<u64>,
    pub selected_columns: Vec<String>,
    pub ordering: VortexColumnarResultOrdering,
    pub tie_break_policy: String,
}

impl VortexRetainedRowSet {
    /// # Errors
    /// Returns an error when the source ref or tie-break policy is empty.
    pub fn new(
        source_ref: impl Into<String>,
        row_refs: Vec<u64>,
        selected_columns: Vec<String>,
        ordering: VortexColumnarResultOrdering,
        tie_break_policy: impl Into<String>,
    ) -> Result<Self> {
        let source_ref = source_ref.into();
        let tie_break_policy = tie_break_policy.into();
        if source_ref.trim().is_empty() || tie_break_policy.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "retained row set requires a source ref and tie-break policy".to_string(),
            ));
        }
        Ok(Self {
            source_ref,
            row_refs,
            selected_columns,
            ordering,
            tie_break_policy,
        })
    }

    #[must_use]
    pub fn row_count(&self) -> u64 {
        u64::try_from(self.row_refs.len()).unwrap_or(u64::MAX)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexColumnarResultBatch {
    pub schema_version: &'static str,
    pub batch_id: String,
    pub columns: Vec<VortexColumnarResultColumn>,
    pub retained_row_set: Option<VortexRetainedRowSet>,
    pub ordering: VortexColumnarResultOrdering,
    pub declared_sink_boundary: VortexColumnarResultSinkBoundary,
    pub row_count: u64,
}

impl VortexColumnarResultBatch {
    /// # Errors
    /// Returns an error when the batch id is empty or column/row-ref lengths disagree.
    pub fn new(
        batch_id: impl Into<String>,
        columns: Vec<VortexColumnarResultColumn>,
        retained_row_set: Option<VortexRetainedRowSet>,
        ordering: VortexColumnarResultOrdering,
        declared_sink_boundary: VortexColumnarResultSinkBoundary,
    ) -> Result<Self> {
        let batch_id = batch_id.into();
        if batch_id.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "columnar result batch id must not be empty".to_string(),
            ));
        }
        let row_count = infer_batch_row_count(&columns, retained_row_set.as_ref())?;
        Ok(Self {
            schema_version: VORTEX_COLUMNAR_RESULT_DATAPLANE_SCHEMA_VERSION,
            batch_id,
            columns,
            retained_row_set,
            ordering,
            declared_sink_boundary,
            row_count,
        })
    }

    /// # Errors
    /// Returns an error when row vectors have inconsistent widths or column definitions are invalid.
    pub fn from_rows(
        batch_id: impl Into<String>,
        column_names: Vec<String>,
        logical_dtypes: Vec<String>,
        rows: Vec<Vec<ScalarValue>>,
        ordering: VortexColumnarResultOrdering,
        declared_sink_boundary: VortexColumnarResultSinkBoundary,
    ) -> Result<Self> {
        if column_names.len() != logical_dtypes.len() {
            return Err(ShardLoomError::InvalidOperation(
                "columnar result row input requires one dtype per column".to_string(),
            ));
        }
        let mut values_by_column = vec![Vec::new(); column_names.len()];
        for row in rows {
            if row.len() != column_names.len() {
                return Err(ShardLoomError::InvalidOperation(
                    "columnar result row input contained inconsistent row width".to_string(),
                ));
            }
            for (index, value) in row.into_iter().enumerate() {
                values_by_column[index].push(value);
            }
        }
        let columns = column_names
            .into_iter()
            .zip(logical_dtypes)
            .zip(values_by_column)
            .map(|((name, dtype), values)| {
                VortexColumnarResultColumn::scalar_values(name, dtype, values)
            })
            .collect::<Result<Vec<_>>>()?;
        Self::new(batch_id, columns, None, ordering, declared_sink_boundary)
    }

    /// # Errors
    /// Returns an error when column definitions are invalid.
    pub fn opaque_encoded(
        batch_id: impl Into<String>,
        row_count: u64,
        columns: Vec<(String, String, String, Option<u64>)>,
        retained_row_set: Option<VortexRetainedRowSet>,
        ordering: VortexColumnarResultOrdering,
        declared_sink_boundary: VortexColumnarResultSinkBoundary,
    ) -> Result<Self> {
        let columns = columns
            .into_iter()
            .map(|(name, dtype, layout, estimated_bytes)| {
                VortexColumnarResultColumn::opaque_encoded(
                    name,
                    dtype,
                    row_count,
                    layout,
                    estimated_bytes,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Self::new(
            batch_id,
            columns,
            retained_row_set,
            ordering,
            declared_sink_boundary,
        )
    }

    #[must_use]
    pub fn selected_column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    }

    #[must_use]
    pub fn storage_summary(&self) -> String {
        self.columns
            .iter()
            .map(|column| format!("{}:{}", column.name, column.storage.as_str()))
            .collect::<Vec<_>>()
            .join(",")
    }

    #[must_use]
    pub fn estimated_payload_bytes(&self) -> u64 {
        self.columns
            .iter()
            .map(VortexColumnarResultColumn::estimated_payload_bytes)
            .fold(0_u64, u64::saturating_add)
    }

    #[must_use]
    pub fn row_values_ready(&self) -> bool {
        self.columns
            .iter()
            .all(|column| column.storage.row_materialization_ready())
    }

    #[must_use]
    pub fn checksum(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.schema_version.hash(&mut hasher);
        self.batch_id.hash(&mut hasher);
        self.row_count.hash(&mut hasher);
        self.ordering.as_str().hash(&mut hasher);
        self.declared_sink_boundary.as_str().hash(&mut hasher);
        for column in &self.columns {
            column.name.hash(&mut hasher);
            column.logical_dtype.hash(&mut hasher);
            column.storage.as_str().hash(&mut hasher);
            column.storage.row_count().hash(&mut hasher);
        }
        if let Some(row_set) = &self.retained_row_set {
            row_set.source_ref.hash(&mut hasher);
            row_set.row_refs.hash(&mut hasher);
            row_set.ordering.as_str().hash(&mut hasher);
            row_set.tie_break_policy.hash(&mut hasher);
        }
        hasher.finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexColumnarResultMaterializationStatus {
    ColumnarHandoffNoRowsMaterialized,
    MaterializedAtDeclaredSink,
    BlockedUnsupportedRemoteDelivery,
    BlockedRowsNotAvailableAtSink,
}

impl VortexColumnarResultMaterializationStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ColumnarHandoffNoRowsMaterialized => "columnar_handoff_no_rows_materialized",
            Self::MaterializedAtDeclaredSink => "materialized_at_declared_sink",
            Self::BlockedUnsupportedRemoteDelivery => "blocked_unsupported_remote_delivery",
            Self::BlockedRowsNotAvailableAtSink => "blocked_rows_not_available_at_sink",
        }
    }

    #[must_use]
    pub const fn is_error(self) -> bool {
        matches!(
            self,
            Self::BlockedUnsupportedRemoteDelivery | Self::BlockedRowsNotAvailableAtSink
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexColumnarResultMaterializationCertificate {
    pub schema_version: &'static str,
    pub batch_id: String,
    pub status: VortexColumnarResultMaterializationStatus,
    pub sink_boundary: VortexColumnarResultSinkBoundary,
    pub ordering: VortexColumnarResultOrdering,
    pub rows_considered: u64,
    pub rows_retained: u64,
    pub rows_materialized: u64,
    pub column_count: usize,
    pub columns_decoded: usize,
    pub payload_bytes_decoded: u64,
    pub materialized_before_declared_sink: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexColumnarResultMaterializationCertificate {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.materialized_before_declared_sink
            || self.fallback_attempted
            || self.fallback_execution_allowed
            || self.external_engine_invoked
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    shardloom_core::DiagnosticSeverity::Error
                        | shardloom_core::DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn evidence_fields(&self, prefix: &str) -> Vec<(String, String)> {
        vec![
            (
                format!("{prefix}_columnar_result_materialization_schema_version"),
                self.schema_version.to_string(),
            ),
            (
                format!("{prefix}_columnar_result_materialization_batch_id"),
                self.batch_id.clone(),
            ),
            (
                format!("{prefix}_columnar_result_materialization_status"),
                self.status.as_str().to_string(),
            ),
            (
                format!("{prefix}_columnar_result_materialization_sink_boundary"),
                self.sink_boundary.as_str().to_string(),
            ),
            (
                format!("{prefix}_columnar_result_materialization_ordering"),
                self.ordering.as_str().to_string(),
            ),
            (
                format!("{prefix}_columnar_result_materialization_rows_considered"),
                self.rows_considered.to_string(),
            ),
            (
                format!("{prefix}_columnar_result_materialization_rows_retained"),
                self.rows_retained.to_string(),
            ),
            (
                format!("{prefix}_columnar_result_materialization_rows_materialized"),
                self.rows_materialized.to_string(),
            ),
            (
                format!("{prefix}_columnar_result_materialization_column_count"),
                self.column_count.to_string(),
            ),
            (
                format!("{prefix}_columnar_result_materialization_columns_decoded"),
                self.columns_decoded.to_string(),
            ),
            (
                format!("{prefix}_columnar_result_materialization_payload_bytes_decoded"),
                self.payload_bytes_decoded.to_string(),
            ),
            (
                format!("{prefix}_columnar_result_materialization_before_declared_sink"),
                self.materialized_before_declared_sink.to_string(),
            ),
            (
                format!("{prefix}_columnar_result_materialization_fallback_attempted"),
                self.fallback_attempted.to_string(),
            ),
            (
                format!("{prefix}_columnar_result_materialization_external_engine_invoked"),
                self.external_engine_invoked.to_string(),
            ),
            (
                format!("{prefix}_columnar_result_materialization_fallback_execution_allowed"),
                self.fallback_execution_allowed.to_string(),
            ),
        ]
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexColumnarResultMaterializationReport {
    pub batch: VortexColumnarResultBatch,
    pub rows: Vec<Vec<(String, ScalarValue)>>,
    pub certificate: VortexColumnarResultMaterializationCertificate,
}

impl VortexColumnarResultMaterializationReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.certificate.has_errors()
    }

    #[must_use]
    pub fn evidence_fields(&self, prefix: &str) -> Vec<(String, String)> {
        let mut fields = vec![
            (
                format!("{prefix}_columnar_result_dataplane_schema_version"),
                self.batch.schema_version.to_string(),
            ),
            (
                format!("{prefix}_columnar_result_dataplane_batch_id"),
                self.batch.batch_id.clone(),
            ),
            (
                format!("{prefix}_columnar_result_dataplane_row_count"),
                self.batch.row_count.to_string(),
            ),
            (
                format!("{prefix}_columnar_result_dataplane_column_count"),
                self.batch.columns.len().to_string(),
            ),
            (
                format!("{prefix}_columnar_result_dataplane_selected_columns"),
                self.batch.selected_column_names().join(","),
            ),
            (
                format!("{prefix}_columnar_result_dataplane_storage_summary"),
                self.batch.storage_summary(),
            ),
            (
                format!("{prefix}_columnar_result_dataplane_ordering"),
                self.batch.ordering.as_str().to_string(),
            ),
            (
                format!("{prefix}_columnar_result_dataplane_declared_sink_boundary"),
                self.batch.declared_sink_boundary.as_str().to_string(),
            ),
            (
                format!("{prefix}_columnar_result_dataplane_estimated_payload_bytes"),
                self.batch.estimated_payload_bytes().to_string(),
            ),
            (
                format!("{prefix}_columnar_result_dataplane_checksum"),
                self.batch.checksum().to_string(),
            ),
            (
                format!("{prefix}_columnar_result_dataplane_fallback_attempted"),
                "false".to_string(),
            ),
            (
                format!("{prefix}_columnar_result_dataplane_external_engine_invoked"),
                "false".to_string(),
            ),
        ];
        fields.extend(self.certificate.evidence_fields(prefix));
        fields
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.batch.schema_version);
        let _ = writeln!(out, "batch: {}", self.batch.batch_id);
        let _ = writeln!(out, "rows: {}", self.batch.row_count);
        let _ = writeln!(
            out,
            "columns: {}",
            self.batch.selected_column_names().join(",")
        );
        let _ = writeln!(
            out,
            "sink boundary: {}",
            self.batch.declared_sink_boundary.as_str()
        );
        let _ = writeln!(out, "materialization: {}", self.certificate.status.as_str());
        let _ = writeln!(out, "fallback execution: disabled");
        out
    }
}

/// Plans or performs the declared sink-boundary materialization for a columnar result batch.
///
/// # Errors
/// Returns an error when row assembly finds inconsistent internal column lengths.
pub fn materialize_columnar_result_batch_for_sink(
    batch: VortexColumnarResultBatch,
    sink_boundary: VortexColumnarResultSinkBoundary,
) -> Result<VortexColumnarResultMaterializationReport> {
    if sink_boundary.unsupported() {
        return Ok(blocked_materialization(
            batch,
            sink_boundary,
            VortexColumnarResultMaterializationStatus::BlockedUnsupportedRemoteDelivery,
            "remote result delivery is not admitted for this local columnar result dataplane",
        ));
    }
    if !sink_boundary.materializes_rows() {
        return Ok(VortexColumnarResultMaterializationReport {
            certificate: materialization_certificate(
                &batch,
                sink_boundary,
                VortexColumnarResultMaterializationStatus::ColumnarHandoffNoRowsMaterialized,
                0,
                0,
                Vec::new(),
            ),
            batch,
            rows: Vec::new(),
        });
    }
    if !batch.row_values_ready() {
        return Ok(blocked_materialization(
            batch,
            sink_boundary,
            VortexColumnarResultMaterializationStatus::BlockedRowsNotAvailableAtSink,
            "row materialization requested at sink boundary but one or more columns are still opaque encoded or row-ref only",
        ));
    }

    let rows = materialized_rows(&batch)?;
    let rows_materialized = u64::try_from(rows.len()).unwrap_or(u64::MAX);
    let payload_bytes_decoded = rows
        .iter()
        .flat_map(|row| {
            row.iter()
                .map(|(_, value)| scalar_value_payload_bytes(value))
        })
        .fold(0_u64, u64::saturating_add);
    let columns_decoded = batch.columns.len();
    Ok(VortexColumnarResultMaterializationReport {
        certificate: materialization_certificate(
            &batch,
            sink_boundary,
            VortexColumnarResultMaterializationStatus::MaterializedAtDeclaredSink,
            rows_materialized,
            payload_bytes_decoded,
            Vec::new(),
        )
        .with_columns_decoded(columns_decoded),
        batch,
        rows,
    })
}

/// Converts a columnar result batch into a payload descriptor for local output planning.
///
/// # Errors
/// Returns an error when descriptor construction fails.
pub fn output_payload_descriptor_from_columnar_result_batch(
    batch: &VortexColumnarResultBatch,
) -> Result<VortexOutputPayloadContentDescriptor> {
    VortexOutputPayloadContentDescriptor::encoded_batch_payload(format!(
        "columnar_result_batch={} rows={} columns={} storage={} sink={}",
        batch.batch_id,
        batch.row_count,
        batch.columns.len(),
        batch.storage_summary(),
        batch.declared_sink_boundary.as_str()
    ))
    .map(|mut descriptor| {
        descriptor.logical_rows = Some(batch.row_count);
        descriptor.estimated_bytes = Some(batch.estimated_payload_bytes());
        descriptor.checksum = Some(batch.checksum());
        descriptor
    })
}

fn infer_batch_row_count(
    columns: &[VortexColumnarResultColumn],
    retained_row_set: Option<&VortexRetainedRowSet>,
) -> Result<u64> {
    let expected = columns
        .first()
        .map(VortexColumnarResultColumn::row_count)
        .or_else(|| retained_row_set.map(VortexRetainedRowSet::row_count))
        .unwrap_or(0);
    for column in columns {
        if column.row_count() != expected {
            return Err(ShardLoomError::InvalidOperation(format!(
                "columnar result column {} has {} rows but expected {}; no fallback execution was attempted",
                column.name,
                column.row_count(),
                expected
            )));
        }
    }
    if let Some(row_set) = retained_row_set {
        let row_ref_count = row_set.row_count();
        if row_ref_count != expected {
            return Err(ShardLoomError::InvalidOperation(format!(
                "columnar result retained row set has {row_ref_count} rows but expected {expected}; no fallback execution was attempted"
            )));
        }
    }
    Ok(expected)
}

fn blocked_materialization(
    batch: VortexColumnarResultBatch,
    sink_boundary: VortexColumnarResultSinkBoundary,
    status: VortexColumnarResultMaterializationStatus,
    reason: impl Into<String>,
) -> VortexColumnarResultMaterializationReport {
    let diagnostic = Diagnostic::unsupported(
        DiagnosticCode::NotImplemented,
        "vortex_columnar_result_dataplane",
        reason,
        Some("Use an admitted local sink boundary or keep result data columnar until a sink can consume it.".to_string()),
    );
    VortexColumnarResultMaterializationReport {
        certificate: materialization_certificate(
            &batch,
            sink_boundary,
            status,
            0,
            0,
            vec![diagnostic],
        ),
        batch,
        rows: Vec::new(),
    }
}

fn materialization_certificate(
    batch: &VortexColumnarResultBatch,
    sink_boundary: VortexColumnarResultSinkBoundary,
    status: VortexColumnarResultMaterializationStatus,
    rows_materialized: u64,
    payload_bytes_decoded: u64,
    diagnostics: Vec<Diagnostic>,
) -> VortexColumnarResultMaterializationCertificate {
    VortexColumnarResultMaterializationCertificate {
        schema_version: VORTEX_COLUMNAR_RESULT_MATERIALIZATION_SCHEMA_VERSION,
        batch_id: batch.batch_id.clone(),
        status,
        sink_boundary,
        ordering: batch.ordering,
        rows_considered: batch.row_count,
        rows_retained: batch
            .retained_row_set
            .as_ref()
            .map_or(batch.row_count, VortexRetainedRowSet::row_count),
        rows_materialized,
        column_count: batch.columns.len(),
        columns_decoded: 0,
        payload_bytes_decoded,
        materialized_before_declared_sink: false,
        fallback_attempted: false,
        fallback_execution_allowed: false,
        external_engine_invoked: false,
        diagnostics,
    }
}

trait WithColumnsDecoded {
    fn with_columns_decoded(self, columns_decoded: usize) -> Self;
}

impl WithColumnsDecoded for VortexColumnarResultMaterializationCertificate {
    fn with_columns_decoded(mut self, columns_decoded: usize) -> Self {
        self.columns_decoded = columns_decoded;
        self
    }
}

fn materialized_rows(batch: &VortexColumnarResultBatch) -> Result<Vec<Vec<(String, ScalarValue)>>> {
    let row_count = usize::try_from(batch.row_count).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "columnar result row count does not fit usize for sink materialization: {error}; no fallback execution was attempted"
        ))
    })?;
    let mut rows = Vec::with_capacity(row_count);
    for row_index in 0..row_count {
        let mut row = Vec::with_capacity(batch.columns.len());
        for column in &batch.columns {
            let VortexColumnarResultColumnStorage::ScalarValues(values) = &column.storage else {
                return Err(ShardLoomError::InvalidOperation(
                    "columnar result row materialization reached a non-scalar column; no fallback execution was attempted"
                        .to_string(),
                ));
            };
            let Some(value) = values.get(row_index) else {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "columnar result column {} missing row {}; no fallback execution was attempted",
                    column.name, row_index
                )));
            };
            row.push((column.name.clone(), value.clone()));
        }
        rows.push(row);
    }
    Ok(rows)
}

fn scalar_value_payload_bytes(value: &ScalarValue) -> u64 {
    match value {
        ScalarValue::Null => 0,
        ScalarValue::Boolean(_) => 1,
        ScalarValue::Int64(_)
        | ScalarValue::UInt64(_)
        | ScalarValue::Float64(_)
        | ScalarValue::TimestampMicros(_) => 8,
        ScalarValue::Utf8(value) => u64::try_from(value.len()).unwrap_or(u64::MAX),
        ScalarValue::Binary(value) => u64::try_from(value.len()).unwrap_or(u64::MAX),
        ScalarValue::Decimal128 { .. } => 16,
        ScalarValue::Date32(_) => 4,
        ScalarValue::List(values) => values
            .iter()
            .map(scalar_value_payload_bytes)
            .fold(0_u64, u64::saturating_add),
        ScalarValue::Struct(values) => values
            .iter()
            .map(|(key, value)| {
                u64::try_from(key.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(scalar_value_payload_bytes(value))
            })
            .fold(0_u64, u64::saturating_add),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VortexOutputPayloadContentKind;

    #[test]
    fn columnar_batch_materializes_rows_only_at_json_sink_boundary() {
        let batch = VortexColumnarResultBatch::from_rows(
            "grouped-topk-result",
            vec!["domain".to_string(), "hits".to_string()],
            vec!["utf8".to_string(), "uint64".to_string()],
            vec![
                vec![
                    ScalarValue::Utf8("example.com".to_string()),
                    ScalarValue::UInt64(3),
                ],
                vec![ScalarValue::Null, ScalarValue::UInt64(1)],
            ],
            VortexColumnarResultOrdering::StableTopK,
            VortexColumnarResultSinkBoundary::Jsonl,
        )
        .expect("batch");

        let report = materialize_columnar_result_batch_for_sink(
            batch,
            VortexColumnarResultSinkBoundary::Jsonl,
        )
        .expect("materialization");

        assert_eq!(
            report.certificate.status,
            VortexColumnarResultMaterializationStatus::MaterializedAtDeclaredSink
        );
        assert_eq!(report.certificate.rows_materialized, 2);
        assert_eq!(report.certificate.columns_decoded, 2);
        assert!(!report.certificate.materialized_before_declared_sink);
        assert!(!report.certificate.fallback_attempted);
        assert_eq!(
            report.rows[0][0].1,
            ScalarValue::Utf8("example.com".to_string())
        );
        assert_eq!(report.rows[1][0].1, ScalarValue::Null);
    }

    #[test]
    fn native_vortex_sink_keeps_batch_columnar() {
        let batch = VortexColumnarResultBatch::opaque_encoded(
            "native-vortex-result",
            8,
            vec![(
                "count".to_string(),
                "uint64".to_string(),
                "vortex.primitive".to_string(),
                Some(64),
            )],
            None,
            VortexColumnarResultOrdering::SourceOrder,
            VortexColumnarResultSinkBoundary::NativeVortex,
        )
        .expect("batch");

        let report = materialize_columnar_result_batch_for_sink(
            batch,
            VortexColumnarResultSinkBoundary::NativeVortex,
        )
        .expect("materialization");

        assert_eq!(
            report.certificate.status,
            VortexColumnarResultMaterializationStatus::ColumnarHandoffNoRowsMaterialized
        );
        assert!(report.rows.is_empty());
        assert_eq!(report.certificate.rows_materialized, 0);
        assert_eq!(report.certificate.columns_decoded, 0);
        assert!(!report.has_errors());
    }

    #[test]
    fn opaque_encoded_json_materialization_blocks_without_fallback() {
        let batch = VortexColumnarResultBatch::opaque_encoded(
            "opaque-json-result",
            3,
            vec![(
                "url".to_string(),
                "utf8".to_string(),
                "vortex.dict".to_string(),
                Some(24),
            )],
            None,
            VortexColumnarResultOrdering::StableTopK,
            VortexColumnarResultSinkBoundary::Jsonl,
        )
        .expect("batch");

        let report = materialize_columnar_result_batch_for_sink(
            batch,
            VortexColumnarResultSinkBoundary::Jsonl,
        )
        .expect("materialization");

        assert_eq!(
            report.certificate.status,
            VortexColumnarResultMaterializationStatus::BlockedRowsNotAvailableAtSink
        );
        assert!(report.rows.is_empty());
        assert!(report.has_errors());
        assert!(!report.certificate.fallback_attempted);
        assert!(!report.certificate.external_engine_invoked);
    }

    #[test]
    fn retained_row_refs_are_part_of_row_count_contract() {
        let row_set = VortexRetainedRowSet::new(
            "fact.vortex",
            vec![7, 2, 3],
            vec!["URL".to_string(), "EventTime".to_string()],
            VortexColumnarResultOrdering::StableTopK,
            "order_terms_then_source_ordinal",
        )
        .expect("row set");
        let batch = VortexColumnarResultBatch::new(
            "row-ref-topk",
            vec![
                VortexColumnarResultColumn::retained_row_refs("row_ref", vec![7, 2, 3])
                    .expect("row ref column"),
            ],
            Some(row_set),
            VortexColumnarResultOrdering::StableTopK,
            VortexColumnarResultSinkBoundary::NativeVortex,
        )
        .expect("batch");

        assert_eq!(batch.row_count, 3);
        assert_eq!(batch.storage_summary(), "row_ref:retained_row_refs");
        assert_eq!(
            batch
                .retained_row_set
                .as_ref()
                .expect("retained row set")
                .tie_break_policy,
            "order_terms_then_source_ordinal"
        );
    }

    #[test]
    fn output_payload_descriptor_tracks_columnar_batch_content() {
        let batch = VortexColumnarResultBatch::opaque_encoded(
            "payload-batch",
            1,
            vec![(
                "count".to_string(),
                "uint64".to_string(),
                "vortex.primitive".to_string(),
                Some(8),
            )],
            None,
            VortexColumnarResultOrdering::SourceOrder,
            VortexColumnarResultSinkBoundary::NativeVortex,
        )
        .expect("batch");

        let descriptor =
            output_payload_descriptor_from_columnar_result_batch(&batch).expect("descriptor");

        assert_eq!(
            descriptor.content_kind(),
            VortexOutputPayloadContentKind::EncodedBatchPayload
        );
        assert_eq!(descriptor.logical_rows(), Some(1));
        assert_eq!(descriptor.estimated_bytes(), Some(8));
        assert_eq!(descriptor.checksum(), Some(batch.checksum()));
        assert!(
            descriptor
                .summary()
                .contains("columnar_result_batch=payload-batch")
        );
    }
}
