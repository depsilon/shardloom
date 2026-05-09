#![allow(clippy::must_use_candidate)]

use std::fmt::Write as _;

#[cfg(feature = "vortex-local-primitives")]
use shardloom_core::UriScheme;
#[cfg(feature = "vortex-local-primitives")]
use shardloom_core::{
    ComparisonOp, DatasetUri, DiagnosticCode, PredicateExpr, ShardLoomError, StatValue,
};
use shardloom_core::{Diagnostic, Result};
#[cfg(feature = "vortex-local-primitives")]
use shardloom_plan::ProjectionRequest;

use crate::{VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest};

/// Feature-gated local Vortex primitive execution status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalPrimitiveExecutionStatus {
    FeatureDisabled,
    Executed,
    BlockedByUnsupportedInput,
    BlockedByUnsupportedPrimitive,
    BlockedByUnsupportedDType,
    Unsupported,
}
impl VortexLocalPrimitiveExecutionStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Executed => "executed",
            Self::BlockedByUnsupportedInput => "blocked_by_unsupported_input",
            Self::BlockedByUnsupportedPrimitive => "blocked_by_unsupported_primitive",
            Self::BlockedByUnsupportedDType => "blocked_by_unsupported_dtype",
            Self::Unsupported => "unsupported",
        }
    }

    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::BlockedByUnsupportedInput
                | Self::BlockedByUnsupportedPrimitive
                | Self::BlockedByUnsupportedDType
                | Self::Unsupported
        )
    }
}

/// Execution mode used by the local Vortex primitive executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalPrimitiveExecutionMode {
    FeatureDisabled,
    MetadataPreservingCount,
    VortexArrayPrimitive,
    Unsupported,
}
impl VortexLocalPrimitiveExecutionMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::MetadataPreservingCount => "metadata_preserving_count",
            Self::VortexArrayPrimitive => "vortex_array_primitive",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Report emitted by the narrow local Vortex primitive executor.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexLocalPrimitiveExecutionReport {
    pub status: VortexLocalPrimitiveExecutionStatus,
    pub mode: VortexLocalPrimitiveExecutionMode,
    pub primitive_kind: VortexQueryPrimitiveKind,
    pub result_summary: Option<String>,
    pub rows_scanned: u64,
    pub rows_selected: Option<u64>,
    pub rows_projected: Option<u64>,
    pub projected_columns: Vec<String>,
    pub arrays_read_count: usize,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub upstream_scan_called: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub materialization_boundary_reported: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexLocalPrimitiveExecutionReport {
    pub fn feature_disabled(primitive_kind: VortexQueryPrimitiveKind) -> Self {
        Self {
            status: VortexLocalPrimitiveExecutionStatus::FeatureDisabled,
            mode: VortexLocalPrimitiveExecutionMode::FeatureDisabled,
            primitive_kind,
            result_summary: None,
            rows_scanned: 0,
            rows_selected: None,
            rows_projected: None,
            projected_columns: Vec::new(),
            arrays_read_count: 0,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            upstream_scan_called: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            materialization_boundary_reported: false,
            diagnostics: Vec::new(),
        }
    }

    #[cfg(feature = "vortex-local-primitives")]
    fn blocked(
        primitive_kind: VortexQueryPrimitiveKind,
        status: VortexLocalPrimitiveExecutionStatus,
        diagnostic: Diagnostic,
    ) -> Self {
        let mut out = Self::feature_disabled(primitive_kind);
        out.status = status;
        out.mode = VortexLocalPrimitiveExecutionMode::Unsupported;
        out.diagnostics.push(diagnostic);
        out
    }

    pub const fn has_errors(&self) -> bool {
        self.status.is_error()
    }

    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "local primitive status: {}", self.status.as_str());
        let _ = writeln!(out, "local primitive mode: {}", self.mode.as_str());
        let _ = writeln!(out, "primitive kind: {}", self.primitive_kind.as_str());
        if let Some(summary) = &self.result_summary {
            let _ = writeln!(out, "result summary: {summary}");
        }
        let _ = writeln!(out, "rows scanned: {}", self.rows_scanned);
        let _ = writeln!(
            out,
            "rows selected: {}",
            self.rows_selected
                .map_or_else(|| "none".to_string(), |value| value.to_string())
        );
        let _ = writeln!(
            out,
            "rows projected: {}",
            self.rows_projected
                .map_or_else(|| "none".to_string(), |value| value.to_string())
        );
        let _ = writeln!(
            out,
            "projected columns: {}",
            self.projected_columns.join(",")
        );
        let _ = writeln!(out, "arrays read count: {}", self.arrays_read_count);
        let _ = writeln!(out, "data read: {}", self.data_read);
        let _ = writeln!(out, "data decoded: {}", self.data_decoded);
        let _ = writeln!(out, "data materialized: {}", self.data_materialized);
        let _ = writeln!(out, "upstream scan called: {}", self.upstream_scan_called);
        let _ = writeln!(out, "row read: {}", self.row_read);
        let _ = writeln!(out, "Arrow converted: {}", self.arrow_converted);
        let _ = writeln!(
            out,
            "materialization boundary reported: {}",
            self.materialization_boundary_reported
        );
        let _ = writeln!(out, "fallback execution disabled");
        out
    }
}

/// Executes a narrow local Vortex query primitive when the feature gate is enabled.
///
/// The executor is intentionally limited to local `.vortex` files. `CountAll`
/// reads Vortex arrays and sums lengths without decoding or row materialization.
/// `CountWhere`, `FilterPredicate`, and `ProjectColumns` operate over Vortex-
/// derived arrays and report decode/materialization boundaries explicitly.
///
/// # Errors
/// Returns an error only when internal report construction fails.
pub fn execute_vortex_local_primitive(
    request: &VortexQueryPrimitiveRequest,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    #[cfg(feature = "vortex-local-primitives")]
    {
        execute_vortex_local_primitive_enabled(request)
    }
    #[cfg(not(feature = "vortex-local-primitives"))]
    {
        Ok(VortexLocalPrimitiveExecutionReport::feature_disabled(
            request.kind,
        ))
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn execute_vortex_local_primitive_enabled(
    request: &VortexQueryPrimitiveRequest,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let Some(uri) = request.source_uri.as_ref() else {
        return Ok(VortexLocalPrimitiveExecutionReport::blocked(
            request.kind,
            VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedInput,
            Diagnostic::invalid_input(
                "vortex_local_primitive",
                "local primitive execution requires a source URI",
                "provide a local `.vortex` source URI",
            ),
        ));
    };
    let Some(path) = local_vortex_path(uri, request.kind)? else {
        return Ok(VortexLocalPrimitiveExecutionReport::blocked(
            request.kind,
            VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedInput,
            Diagnostic::invalid_input(
                "vortex_local_primitive",
                format!(
                    "unsupported local Vortex primitive target: {}",
                    uri.as_str()
                ),
                "provide an existing local path or file:// `.vortex` target",
            ),
        ));
    };
    let scan = read_local_vortex_array(&path, request.kind)?;
    match request.kind {
        VortexQueryPrimitiveKind::CountAll => Ok(count_all_report(request.kind, &scan)?),
        VortexQueryPrimitiveKind::CountWhere | VortexQueryPrimitiveKind::FilterPredicate => {
            let Some(predicate) = request.predicate.as_ref() else {
                return Ok(VortexLocalPrimitiveExecutionReport::blocked(
                    request.kind,
                    VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedPrimitive,
                    Diagnostic::invalid_input(
                        "vortex_local_primitive",
                        "predicate primitive was missing its predicate",
                        "use count-where:<predicate> or filter:<predicate>",
                    ),
                ));
            };
            predicate_report(request.kind, &scan, predicate)
        }
        VortexQueryPrimitiveKind::ProjectColumns => {
            projection_report(request.kind, &scan, &request.projection)
        }
        VortexQueryPrimitiveKind::FilterAndProject
        | VortexQueryPrimitiveKind::SimpleAggregate
        | VortexQueryPrimitiveKind::Unsupported => {
            Ok(VortexLocalPrimitiveExecutionReport::blocked(
                request.kind,
                VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedPrimitive,
                Diagnostic::unsupported(
                    DiagnosticCode::NotImplemented,
                    "vortex_local_primitive",
                    format!(
                        "local primitive execution does not yet support {}",
                        request.kind.as_str()
                    ),
                    Some("Fallback attempted: false".to_string()),
                ),
            ))
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
struct LocalVortexScan {
    array: vortex::array::ArrayRef,
    row_count: usize,
    arrays_read_count: usize,
}

#[cfg(feature = "vortex-local-primitives")]
fn local_vortex_path(
    target_uri: &DatasetUri,
    primitive_kind: VortexQueryPrimitiveKind,
) -> Result<Option<std::path::PathBuf>> {
    if !target_uri.looks_like_vortex() {
        return Ok(None);
    }
    let path = match target_uri.scheme() {
        UriScheme::LocalPath => std::path::PathBuf::from(target_uri.as_str()),
        UriScheme::File => std::path::PathBuf::from(
            target_uri
                .as_str()
                .strip_prefix("file://")
                .unwrap_or_else(|| target_uri.as_str()),
        ),
        UriScheme::S3 | UriScheme::Gcs | UriScheme::Adls | UriScheme::Other => return Ok(None),
    };
    if !path.exists() {
        return Ok(None);
    }
    if !path.is_file() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local primitive {} target is not a file: {}",
            primitive_kind.as_str(),
            path.display()
        )));
    }
    Ok(Some(path))
}

#[cfg(feature = "vortex-local-primitives")]
fn read_local_vortex_array(
    path: &std::path::Path,
    primitive_kind: VortexQueryPrimitiveKind,
) -> Result<LocalVortexScan> {
    use vortex::VortexSessionDefault as _;
    use vortex::array::stream::ArrayStreamExt as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(path))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to open local Vortex target for {}: {error}",
                primitive_kind.as_str()
            ))
        })?;
    let array = runtime
        .block_on(
            file.scan()
                .map_err(vortex_error)?
                .into_array_stream()
                .map_err(vortex_error)?
                .read_all(),
        )
        .map_err(vortex_error)?;
    Ok(LocalVortexScan {
        row_count: array.len(),
        array,
        arrays_read_count: 1,
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn count_all_report(
    primitive_kind: VortexQueryPrimitiveKind,
    scan: &LocalVortexScan,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let rows = usize_to_u64(scan.row_count)?;
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::MetadataPreservingCount,
        primitive_kind,
        result_summary: Some(rows.to_string()),
        rows_scanned: rows,
        rows_selected: Some(rows),
        rows_projected: None,
        projected_columns: Vec::new(),
        arrays_read_count: scan.arrays_read_count,
        data_read: true,
        data_decoded: false,
        data_materialized: false,
        upstream_scan_called: true,
        row_read: false,
        arrow_converted: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        external_effects_executed: false,
        fallback_execution_allowed: false,
        materialization_boundary_reported: false,
        diagnostics: Vec::new(),
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn predicate_report(
    primitive_kind: VortexQueryPrimitiveKind,
    scan: &LocalVortexScan,
    predicate: &PredicateExpr,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let selected_rows = evaluate_predicate(scan, predicate, primitive_kind)?;
    let rows_scanned = usize_to_u64(scan.row_count)?;
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexArrayPrimitive,
        primitive_kind,
        result_summary: Some(selected_rows.to_string()),
        rows_scanned,
        rows_selected: Some(selected_rows),
        rows_projected: None,
        projected_columns: Vec::new(),
        arrays_read_count: scan.arrays_read_count,
        data_read: true,
        data_decoded: true,
        data_materialized: true,
        upstream_scan_called: true,
        row_read: false,
        arrow_converted: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        external_effects_executed: false,
        fallback_execution_allowed: false,
        materialization_boundary_reported: true,
        diagnostics: Vec::new(),
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn projection_report(
    primitive_kind: VortexQueryPrimitiveKind,
    scan: &LocalVortexScan,
    projection: &ProjectionRequest,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let projected_columns = projected_column_names(scan, projection, primitive_kind)?;
    let rows = usize_to_u64(scan.row_count)?;
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexArrayPrimitive,
        primitive_kind,
        result_summary: Some(format!(
            "projected_columns={} rows={}",
            projected_columns.join(","),
            rows
        )),
        rows_scanned: rows,
        rows_selected: None,
        rows_projected: Some(rows),
        projected_columns,
        arrays_read_count: scan.arrays_read_count,
        data_read: true,
        data_decoded: true,
        data_materialized: true,
        upstream_scan_called: true,
        row_read: false,
        arrow_converted: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        external_effects_executed: false,
        fallback_execution_allowed: false,
        materialization_boundary_reported: true,
        diagnostics: Vec::new(),
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn projected_column_names(
    scan: &LocalVortexScan,
    projection: &ProjectionRequest,
    primitive_kind: VortexQueryPrimitiveKind,
) -> Result<Vec<String>> {
    match projection {
        ProjectionRequest::All => local_field_names(scan, primitive_kind),
        ProjectionRequest::Columns(columns) => {
            let available = local_field_names(scan, primitive_kind)?;
            let available_set = available
                .iter()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>();
            let mut out = Vec::new();
            for column in columns {
                if !available_set.contains(column.as_str()) {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "projection column '{}' was not found in local Vortex target",
                        column.as_str()
                    )));
                }
                out.push(column.as_str().to_string());
            }
            Ok(out)
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn local_field_names(
    scan: &LocalVortexScan,
    primitive_kind: VortexQueryPrimitiveKind,
) -> Result<Vec<String>> {
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::StructArray;
    use vortex::array::arrays::struct_::StructArrayExt as _;
    use vortex::array::dtype::DType;

    match scan.array.dtype() {
        DType::Struct(_, _) => {
            let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
            let struct_array = scan
                .array
                .clone()
                .execute::<StructArray>(&mut ctx)
                .map_err(vortex_error)?;
            Ok(struct_array
                .names()
                .iter()
                .map(|name| name.as_ref().to_string())
                .collect())
        }
        DType::Primitive(_, _) => Ok(vec!["value".to_string()]),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local primitive {} does not support top-level dtype {other:?}",
            primitive_kind.as_str()
        ))),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn evaluate_predicate(
    scan: &LocalVortexScan,
    predicate: &PredicateExpr,
    primitive_kind: VortexQueryPrimitiveKind,
) -> Result<u64> {
    match predicate {
        PredicateExpr::AlwaysTrue => usize_to_u64(scan.row_count),
        PredicateExpr::AlwaysFalse => Ok(0),
        PredicateExpr::IsNull { column } | PredicateExpr::IsNotNull { column } => {
            let field = local_field(scan, column.as_str(), primitive_kind)?;
            let validity = field.validity().map_err(vortex_error)?;
            let mut count = 0u64;
            for index in 0..field.len() {
                let is_valid = validity.is_valid(index).map_err(vortex_error)?;
                let selected = match predicate {
                    PredicateExpr::IsNull { .. } => !is_valid,
                    PredicateExpr::IsNotNull { .. } => is_valid,
                    _ => unreachable!("predicate arm is restricted above"),
                };
                if selected {
                    count = count.checked_add(1).ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local primitive predicate count overflowed u64".to_string(),
                        )
                    })?;
                }
            }
            Ok(count)
        }
        PredicateExpr::Compare { column, op, value } => {
            let field = local_field(scan, column.as_str(), primitive_kind)?;
            compare_field(&field, *op, value, primitive_kind)
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn local_field(
    scan: &LocalVortexScan,
    column: &str,
    primitive_kind: VortexQueryPrimitiveKind,
) -> Result<vortex::array::ArrayRef> {
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::StructArray;
    use vortex::array::arrays::struct_::StructArrayExt as _;
    use vortex::array::dtype::DType;

    match scan.array.dtype() {
        DType::Struct(_, _) => {
            let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
            let struct_array = scan
                .array
                .clone()
                .execute::<StructArray>(&mut ctx)
                .map_err(vortex_error)?;
            struct_array
                .unmasked_field_by_name(column)
                .cloned()
                .map_err(vortex_error)
        }
        DType::Primitive(_, _) if column == "value" => Ok(scan.array.clone()),
        DType::Primitive(_, _) => Err(ShardLoomError::InvalidOperation(format!(
            "top-level primitive Vortex arrays expose the implicit column `value`, not `{column}`"
        ))),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local primitive {} does not support predicate dtype {other:?}",
            primitive_kind.as_str()
        ))),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn compare_field(
    field: &vortex::array::ArrayRef,
    op: ComparisonOp,
    value: &StatValue,
    primitive_kind: VortexQueryPrimitiveKind,
) -> Result<u64> {
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::PrimitiveArray;
    use vortex::array::arrays::primitive::PrimitiveArrayExt as _;
    use vortex::array::dtype::PType;

    let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
    let primitive = field
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(vortex_error)?;
    match primitive.ptype() {
        PType::U8 => count_unsigned(primitive.as_slice::<u8>(), op, value),
        PType::U16 => count_unsigned(primitive.as_slice::<u16>(), op, value),
        PType::U32 => count_unsigned(primitive.as_slice::<u32>(), op, value),
        PType::U64 => count_unsigned(primitive.as_slice::<u64>(), op, value),
        PType::I8 => count_signed(primitive.as_slice::<i8>(), op, value),
        PType::I16 => count_signed(primitive.as_slice::<i16>(), op, value),
        PType::I32 => count_signed(primitive.as_slice::<i32>(), op, value),
        PType::I64 => count_signed(primitive.as_slice::<i64>(), op, value),
        PType::F32 => count_float(primitive.as_slice::<f32>(), op, value),
        PType::F64 => count_float(primitive.as_slice::<f64>(), op, value),
        other @ PType::F16 => Err(ShardLoomError::InvalidOperation(format!(
            "local primitive {} does not support predicate ptype {other:?}",
            primitive_kind.as_str()
        ))),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn count_unsigned<T>(values: &[T], op: ComparisonOp, literal: &StatValue) -> Result<u64>
where
    T: Copy + Into<u128>,
{
    let mut count = 0u64;
    for value in values {
        if compare_unsigned((*value).into(), op, literal)? {
            count = count.checked_add(1).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local primitive unsigned predicate count overflowed u64".to_string(),
                )
            })?;
        }
    }
    Ok(count)
}

#[cfg(feature = "vortex-local-primitives")]
fn count_signed<T>(values: &[T], op: ComparisonOp, literal: &StatValue) -> Result<u64>
where
    T: Copy + Into<i128>,
{
    let mut count = 0u64;
    for value in values {
        if compare_signed((*value).into(), op, literal)? {
            count = count.checked_add(1).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local primitive signed predicate count overflowed u64".to_string(),
                )
            })?;
        }
    }
    Ok(count)
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::cast_precision_loss)]
fn count_float<T>(values: &[T], op: ComparisonOp, literal: &StatValue) -> Result<u64>
where
    T: Copy + Into<f64>,
{
    let rhs = match literal {
        StatValue::Int64(value) => *value as f64,
        StatValue::UInt64(value) => *value as f64,
        StatValue::Float64(value) => *value,
        _ => {
            return Err(ShardLoomError::InvalidOperation(
                "local primitive float predicates require numeric literals".to_string(),
            ));
        }
    };
    let mut count = 0u64;
    for value in values {
        if compare_f64((*value).into(), op, rhs) {
            count = count.checked_add(1).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local primitive float predicate count overflowed u64".to_string(),
                )
            })?;
        }
    }
    Ok(count)
}

#[cfg(feature = "vortex-local-primitives")]
fn compare_unsigned(lhs: u128, op: ComparisonOp, literal: &StatValue) -> Result<bool> {
    let rhs = match literal {
        StatValue::UInt64(value) => Some(u128::from(*value)),
        StatValue::Int64(value) if *value >= 0 => Some(u128::try_from(*value).map_err(|_| {
            ShardLoomError::InvalidOperation(
                "local primitive unsigned predicate literal is negative".to_string(),
            )
        })?),
        StatValue::Int64(_) => None,
        _ => {
            return Err(ShardLoomError::InvalidOperation(
                "local primitive unsigned predicates require integer literals".to_string(),
            ));
        }
    };
    Ok(match rhs {
        Some(rhs) => compare_ord(lhs.cmp(&rhs), op),
        None => match op {
            ComparisonOp::Eq | ComparisonOp::Lt | ComparisonOp::LtEq => false,
            ComparisonOp::NotEq | ComparisonOp::Gt | ComparisonOp::GtEq => true,
        },
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn compare_signed(lhs: i128, op: ComparisonOp, literal: &StatValue) -> Result<bool> {
    let rhs = match literal {
        StatValue::Int64(value) => i128::from(*value),
        StatValue::UInt64(value) => i128::from(*value),
        _ => {
            return Err(ShardLoomError::InvalidOperation(
                "local primitive signed predicates require integer literals".to_string(),
            ));
        }
    };
    Ok(compare_ord(lhs.cmp(&rhs), op))
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::float_cmp)]
fn compare_f64(lhs: f64, op: ComparisonOp, rhs: f64) -> bool {
    match op {
        ComparisonOp::Eq => lhs == rhs,
        ComparisonOp::NotEq => lhs != rhs,
        ComparisonOp::Lt => lhs < rhs,
        ComparisonOp::LtEq => lhs <= rhs,
        ComparisonOp::Gt => lhs > rhs,
        ComparisonOp::GtEq => lhs >= rhs,
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn compare_ord(ordering: std::cmp::Ordering, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Eq => ordering.is_eq(),
        ComparisonOp::NotEq => !ordering.is_eq(),
        ComparisonOp::Lt => ordering.is_lt(),
        ComparisonOp::LtEq => ordering.is_lt() || ordering.is_eq(),
        ComparisonOp::Gt => ordering.is_gt(),
        ComparisonOp::GtEq => ordering.is_gt() || ordering.is_eq(),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn usize_to_u64(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|_| {
        ShardLoomError::InvalidOperation("local primitive row count exceeded u64".to_string())
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn vortex_error(error: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("Vortex local primitive failed: {error}"))
}

#[cfg(all(test, feature = "vortex-local-primitives"))]
mod tests {
    use super::*;
    use shardloom_core::ColumnRef;

    fn unique_vortex_path(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "shardloom-{name}-{}-{nanos}.vortex",
            std::process::id()
        ))
    }

    fn write_array(path: &std::path::Path, array: &vortex::array::ArrayRef) -> Result<()> {
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
        assert_eq!(
            summary.row_count(),
            u64::try_from(array.len()).expect("len")
        );
        std::fs::write(path, bytes).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to write test Vortex file '{}': {error}",
                path.display()
            ))
        })
    }

    fn write_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{PrimitiveArray, StructArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let array = StructArray::try_new(
            FieldNames::from(["value", "metric"]),
            vec![
                [1_u32, 2, 3, 4, 5]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                [10_i64, 20, 30, 40, 50]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
            ],
            5,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?;
        write_array(path, &array.into_array())
    }

    fn write_primitive_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::PrimitiveArray;

        let array = [7_u64, 8, 9]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        write_array(path, &array)
    }

    #[test]
    fn count_all_scans_local_vortex_without_decode() {
        let path = unique_vortex_path("count-all");
        write_primitive_fixture(&path).expect("fixture");
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let request = VortexQueryPrimitiveRequest::count_all(uri);

        let report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.result_summary.as_deref(), Some("3"));
        assert!(report.data_read);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn count_where_executes_over_local_vortex_values() {
        let path = unique_vortex_path("count-where");
        write_struct_fixture(&path).expect("fixture");
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let request = VortexQueryPrimitiveRequest::count_where(
            uri,
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.rows_selected, Some(3));
        assert_eq!(report.result_summary.as_deref(), Some("3"));
        assert!(report.data_read);
        assert!(report.data_decoded);
        assert!(report.data_materialized);
        assert!(report.materialization_boundary_reported);
        assert!(!report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn projection_reports_projected_columns_from_local_vortex() {
        let path = unique_vortex_path("project");
        write_struct_fixture(&path).expect("fixture");
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let request = VortexQueryPrimitiveRequest::project(
            uri,
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.rows_projected, Some(5));
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert!(report.data_read);
        assert!(report.data_decoded);
        assert!(report.data_materialized);
        assert!(report.materialization_boundary_reported);
        assert!(!report.fallback_execution_allowed);
    }
}
