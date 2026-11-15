//! Join executed native scan contracts to the current operation's reader owner.

use super::{LocalVortexScanPlan, bind_vortex_scan_expr, vortex_error};
use crate::resident_session::{NativeExecutionContext, PreparedVortexSource};
use shardloom_core::{
    Result,
    native_file_scan::{NativeFileScanContract, NativeFileScanRoute},
};
use shardloom_exec::live_memory::MemoryLease;
use vortex::{
    array::dtype::{DType, Field},
    expr::BoundExpression,
};

pub(crate) struct Observation {
    contract: NativeFileScanContract,
    temporary_metadata: MemoryLease,
}

impl Observation {
    pub(super) fn metadata(
        source: &PreparedVortexSource,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        Ok(Self {
            contract: NativeFileScanContract {
                source: 0,
                route: NativeFileScanRoute::MetadataCount,
                source_rows: source.file().row_count(),
                available_columns: source
                    .dtype()
                    .as_struct_fields_opt()
                    .map_or(1, |fields| fields.names().len() as u64),
                projected_columns: Some(Vec::new()),
                output_columns: Some(Vec::new()),
                pushed_predicate_columns: Some(Vec::new()),
                residual_predicate_columns: Some(Vec::new()),
                invocations: 1,
                provider_scan_invocations: Some(0),
                delivered_rows: 0,
                whole_file_pruning_checks: 0,
                whole_file_exclusions: 0,
            },
            temporary_metadata: context.memory().reserve(512)?,
        })
    }

    pub(super) fn plan(
        source: &PreparedVortexSource,
        plan: &LocalVortexScanPlan,
        route: NativeFileScanRoute,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        Self::file_plan(source.file(), plan, route, context)
    }

    pub(super) fn file_plan(
        file: &vortex::file::VortexFile,
        plan: &LocalVortexScanPlan,
        route: NativeFileScanRoute,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        // Admit expression binding and dependency/name scratch before invoking
        // the provider analysis. Retained contract storage is credited separately.
        let mut metadata = reserve_metadata(file.dtype(), context)?;
        for expression in [&plan.projection, &plan.filter].into_iter().flatten() {
            reserve_expression(expression, &mut metadata, context)?;
        }
        let projection = plan
            .projection
            .as_ref()
            .map(|expr| bind_vortex_scan_expr(file, expr))
            .transpose()?;
        let filter = plan
            .filter
            .as_ref()
            .map(|expr| bind_vortex_scan_expr(file, expr))
            .transpose()?;
        let mut result = Self::from_bound(
            file.dtype(),
            file.row_count(),
            projection.as_ref(),
            filter.as_ref(),
            route,
            metadata,
        )?;
        drop((projection, filter));
        result.residual(plan.residual_predicate.as_ref())?;
        if let Some(output) = &plan.output_columns {
            result.output_columns(output)?;
        }
        Ok(result)
    }

    pub(super) fn residual(&mut self, predicate: Option<&super::PredicateExpr>) -> Result<()> {
        if let Some(predicate) = predicate {
            reserve_predicate(predicate, &mut self.temporary_metadata)?;
            let mut columns = Vec::new();
            super::append_predicate_columns(predicate, &mut columns);
            reserve_names(
                columns.iter().map(shardloom_core::ColumnRef::as_str),
                &mut self.temporary_metadata,
            )?;
            self.contract.residual_predicate_columns = Some(
                columns
                    .into_iter()
                    .map(|column| column.as_str().to_owned())
                    .collect(),
            );
        }
        self.retain_contract_metadata()
    }

    pub(super) fn output_columns(&mut self, output: &[String]) -> Result<()> {
        reserve_names(
            output.iter().map(String::as_str),
            &mut self.temporary_metadata,
        )?;
        self.contract.output_columns = Some(output.to_vec());
        self.retain_contract_metadata()
    }

    fn retain_contract_metadata(&mut self) -> Result<()> {
        // Binding and dependency traversal have ended. Keep the copied names
        // and vectors credited, without retaining their larger analysis
        // workspace across the scan and downstream writer/operator work.
        self.temporary_metadata
            .resize(crate::resident_session::native_scan_contract_bytes(
                &self.contract,
            )?)
    }

    pub(crate) fn bound(
        source: &PreparedVortexSource,
        projection: Option<&BoundExpression>,
        filter: Option<&BoundExpression>,
        route: NativeFileScanRoute,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        Self::file_bound(source.file(), projection, filter, route, context)
    }

    pub(super) fn file_bound(
        file: &vortex::file::VortexFile,
        projection: Option<&BoundExpression>,
        filter: Option<&BoundExpression>,
        route: NativeFileScanRoute,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let mut metadata = reserve_metadata(file.dtype(), context)?;
        for expression in [projection, filter].into_iter().flatten() {
            reserve_bound_expression(expression, &mut metadata, context)?;
        }
        let mut result = Self::from_bound(
            file.dtype(),
            file.row_count(),
            projection,
            filter,
            route,
            metadata,
        )?;
        result.retain_contract_metadata()?;
        Ok(result)
    }

    fn from_bound(
        dtype: &DType,
        source_rows: u64,
        projection: Option<&BoundExpression>,
        filter: Option<&BoundExpression>,
        route: NativeFileScanRoute,
        mut metadata: MemoryLease,
    ) -> Result<Self> {
        let names = dtype
            .as_struct_fields_opt()
            .map(vortex::array::dtype::StructFields::names);
        if let Some(fields) =
            projection.and_then(|expression| expression.dtype().as_struct_fields_opt())
        {
            reserve_names(fields.names().iter().map(AsRef::as_ref), &mut metadata)?;
        }
        let projected = match projection {
            Some(expression) => columns(dtype, expression)?,
            None => names.map(|names| names.iter().map(ToString::to_string).collect()),
        };
        let output = projection
            .and_then(|expression| expression.dtype().as_struct_fields_opt())
            .map(|fields| fields.names().iter().map(ToString::to_string).collect())
            .or_else(|| projected.clone());
        Ok(Self {
            contract: NativeFileScanContract {
                source: 0,
                route,
                source_rows,
                available_columns: names.map_or(1, |names| names.len() as u64),
                projected_columns: projected,
                output_columns: output,
                pushed_predicate_columns: match filter {
                    Some(expression) => columns(dtype, expression)?,
                    None => Some(Vec::new()),
                },
                residual_predicate_columns: Some(Vec::new()),
                invocations: 1,
                provider_scan_invocations: Some(0),
                delivered_rows: 0,
                whole_file_pruning_checks: 0,
                whole_file_exclusions: 0,
            },
            temporary_metadata: metadata,
        })
    }

    pub(crate) fn complete(
        mut self,
        source: &PreparedVortexSource,
        context: &NativeExecutionContext<'_>,
        delivered_rows: u64,
        provider_scans: u64,
        pruning_checks: u64,
        exclusions: u64,
    ) -> Result<()> {
        self.contract.delivered_rows = delivered_rows;
        self.contract.provider_scan_invocations = Some(provider_scans);
        self.contract.whole_file_pruning_checks = pruning_checks;
        self.contract.whole_file_exclusions = exclusions;
        context.record_native_file_scan(source, self.contract)
    }

    pub(super) fn begin(
        mut self,
        source: &PreparedVortexSource,
        context: &NativeExecutionContext<'_>,
        pruning_checks: u64,
        exclusions: u64,
    ) -> Result<usize> {
        self.contract.provider_scan_invocations = Some(1);
        self.contract.whole_file_pruning_checks = pruning_checks;
        self.contract.whole_file_exclusions = exclusions;
        context.begin_native_file_scan(source, self.contract)
    }

    pub(super) fn begin_path(
        mut self,
        path: &std::path::Path,
        context: &NativeExecutionContext<'_>,
        pruning_checks: u64,
        exclusions: u64,
    ) -> Result<usize> {
        self.contract.provider_scan_invocations = Some(1);
        self.contract.whole_file_pruning_checks = pruning_checks;
        self.contract.whole_file_exclusions = exclusions;
        context.begin_native_file_scan_path(path, self.contract)
    }

    pub(super) fn complete_path(
        mut self,
        path: &std::path::Path,
        context: &NativeExecutionContext<'_>,
        delivered_rows: u64,
        provider_scans: u64,
        pruning_checks: u64,
        exclusions: u64,
    ) -> Result<()> {
        self.contract.delivered_rows = delivered_rows;
        self.contract.provider_scan_invocations = Some(provider_scans);
        self.contract.whole_file_pruning_checks = pruning_checks;
        self.contract.whole_file_exclusions = exclusions;
        context.record_native_file_scan_path(path, self.contract)
    }

    pub(super) fn aggregate(
        mut self,
        source: &PreparedVortexSource,
        context: &NativeExecutionContext<'_>,
        scan: &super::LocalVortexAggregateScan,
        provider_scans: u64,
    ) -> Result<()> {
        self.contract.delivered_rows = if scan.metadata_completed {
            0
        } else {
            scan.scan.pre_limit_result_row_count as u64
        };
        self.contract.provider_scan_invocations = Some(provider_scans);
        self.contract.whole_file_pruning_checks =
            u64::from(scan.scan.embedded_layout.metadata_first_pruning_consulted);
        self.contract.whole_file_exclusions = u64::from(scan.scan.metadata_pruned_entire_input());
        context.record_native_file_scan(source, self.contract)
    }
}

fn add_metadata(metadata: &mut MemoryLease, bytes: u64) -> Result<()> {
    metadata.resize(
        metadata.bytes().checked_add(bytes).ok_or_else(|| {
            super::native_relational_batch::failed("scan evidence metadata overflow")
        })?,
    )
}

fn reserve_names<'a>(
    names: impl Iterator<Item = &'a str>,
    metadata: &mut MemoryLease,
) -> Result<()> {
    for name in names {
        let bytes = (name.len() as u64)
            .checked_add(64)
            .and_then(|bytes| bytes.checked_mul(8))
            .ok_or_else(|| {
                super::native_relational_batch::failed("scan evidence metadata overflow")
            })?;
        add_metadata(metadata, bytes)?;
    }
    Ok(())
}

fn reserve_metadata(dtype: &DType, context: &NativeExecutionContext<'_>) -> Result<MemoryLease> {
    let mut metadata = context.memory().reserve(4096)?;
    if let Some(fields) = dtype.as_struct_fields_opt() {
        reserve_names(fields.names().iter().map(AsRef::as_ref), &mut metadata)?;
    }
    Ok(metadata)
}

fn reserve_expression(
    expression: &vortex::expr::Expression,
    metadata: &mut MemoryLease,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    context.check_cancelled()?;
    add_metadata(metadata, 1024)?;
    for child in expression.children() {
        reserve_expression(child, metadata, context)?;
    }
    Ok(())
}

fn reserve_predicate(predicate: &super::PredicateExpr, metadata: &mut MemoryLease) -> Result<()> {
    use super::PredicateExpr;
    add_metadata(metadata, 256)?;
    match predicate {
        PredicateExpr::AlwaysTrue | PredicateExpr::AlwaysFalse => Ok(()),
        PredicateExpr::And(predicates) => predicates
            .iter()
            .try_for_each(|child| reserve_predicate(child, metadata)),
        PredicateExpr::IsNull { column }
        | PredicateExpr::IsNotNull { column }
        | PredicateExpr::Compare { column, .. }
        | PredicateExpr::StringContains { column, .. }
        | PredicateExpr::InList { column, .. } => {
            reserve_names(std::iter::once(column.as_str()), metadata)
        }
    }
}

fn reserve_bound_expression(
    expression: &BoundExpression,
    metadata: &mut MemoryLease,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    context.check_cancelled()?;
    add_metadata(metadata, 1024)?;
    for child in expression.children() {
        reserve_bound_expression(child, metadata, context)?;
    }
    Ok(())
}

fn columns(dtype: &DType, expression: &BoundExpression) -> Result<Option<Vec<String>>> {
    let Some(fields) = dtype.as_struct_fields_opt() else {
        return Ok(None);
    };
    let paths = vortex::expr::analysis::referenced_field_paths(expression).map_err(vortex_error)?;
    let paths = paths.into_iter().collect::<Vec<_>>();
    if paths.iter().any(vortex::array::dtype::FieldPath::is_root) {
        return Ok(Some(
            fields.names().iter().map(ToString::to_string).collect(),
        ));
    }
    Ok(Some(
        fields
            .names()
            .iter()
            .filter(|name| {
                paths.iter().any(|path|
        matches!(path.parts().first(), Some(Field::Name(field)) if field == *name))
            })
            .map(ToString::to_string)
            .collect(),
    ))
}
