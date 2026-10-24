//! Owned declarations of the existing typed IR. This layer only binds sources;
//! expression, operator and storage semantics remain in the shared native engine.

use super::{
    CancellationToken, DatasetUri, ExecutionResources, PreparedVortexRelational,
    ResidentVortexSession, Result, SqlSourceBinding, failed, register_sources, resident_relational,
    sql_request, validate_sql_spill,
};
use shardloom_vortex::relational_query::{
    NATIVE_RECURSION_LIMIT, VortexRelationalPlan as Plan, VortexRelationalScan,
};
use std::{fmt::Write as _, sync::Arc};

#[path = "native_runtime_plan_expressions.rs"]
mod expressions;

/// An inert native relation declaration, retaining exactly the same typed tree
/// produced by SQL lowering. It never stores input rows or an external evaluator.
pub struct NativeRelationDeclaration {
    plan: Arc<Plan>,
    pub(crate) sources: Vec<DatasetUri>,
    pub(crate) metadata_bytes: usize,
    pub(crate) dynamic: bool,
    depth: usize,
}

impl NativeRelationDeclaration {
    /// Wrap an existing typed plan without inspecting a source. Recursive depth
    /// uses the existing binder guard; total node count is not a workload limit.
    /// # Errors
    /// Rejects execution-scoped references, recursive depth and size overflow.
    pub fn new(mut plan: Plan) -> Result<Self> {
        let mut sources = Vec::new();
        let mut nodes = 0_usize;
        let mut dynamic = false;
        let depth = inspect(&plan, 0, &mut nodes, &mut sources, &mut dynamic)?;
        expressions::visit(&mut plan, &mut nodes)?;
        // Counting formatting writes only a byte count, never a rendered plan.
        // The allowance covers descriptor containers and overlapping lowering
        // copies, separately from the binder's actual native state reservations.
        let mut description = CountBytes(0);
        write!(&mut description, "{plan:?}")
            .map_err(|_| failed("native plan metadata size overflow"))?;
        let metadata_bytes = description
            .0
            .checked_mul(8)
            .and_then(|bytes| nodes.checked_mul(4096).and_then(|n| bytes.checked_add(n)))
            .ok_or_else(|| failed("native plan metadata size overflow"))?;
        Ok(Self {
            plan: Arc::new(plan),
            sources,
            metadata_bytes,
            dynamic,
            depth,
        })
    }

    /// Declare a native scan. File compatibility and collections are resolved
    /// through the same source adapter used by SQL after resource admission.
    /// # Errors
    /// Rejects invalid source references.
    pub fn scan(reference: &str) -> Result<Self> {
        Self::new(Plan::Scan(VortexRelationalScan {
            source_uri: DatasetUri::new(reference)?,
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        }))
    }

    /// Borrow the existing typed tree for frontend composition.
    #[must_use]
    pub fn plan(&self) -> &Plan {
        &self.plan
    }

    /// Existing recursive binder depth of this declaration.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Apply an existing native unary operator to this relation. Options use
    /// the same bounded metadata grammar as SQL/DataFrame declarations: column
    /// lists for distinct/duplicates, a positive count for tail, and JSON for
    /// sample/rewrite/melt/rolling/explode/pivot. This performs no source I/O.
    /// # Errors
    /// Rejects unknown operations/options and excessive declaration depth.
    pub fn unary(&self, operation: &str, options: &str, keep: Option<&str>) -> Result<Self> {
        if self.depth >= NATIVE_RECURSION_LIMIT {
            return Err(failed("recursive plan exceeds 24 levels"));
        }
        let request =
            crate::sql_local_source_runtime::native_unary_request(operation, options, keep)?;
        Self::new(Plan::Unary(Box::new(
            shardloom_vortex::relational_query::VortexRelationalUnary {
                input: self.plan().clone(),
                request,
            },
        )))
    }

    /// Bind a typed declaration inside the supplied resource owner. The boolean
    /// reports whether its source membership permits retained preparation reuse.
    /// # Errors
    /// Propagates source, schema, policy, memory, spill and cancellation failures.
    pub fn prepare(
        &self,
        resources: ExecutionResources,
        session: &ResidentVortexSession,
        bindings: &[SqlSourceBinding],
        spill: Option<&str>,
        cancellation: &CancellationToken,
    ) -> Result<(PreparedVortexRelational, bool)> {
        cancellation.check()?;
        validate_sql_spill(spill, resources)?;
        let request = sql_request("", resources, bindings, spill)?;
        resident_relational::prepare_native_in_session(
            self,
            &request,
            session,
            |schemas| register_sources(schemas, bindings, cancellation),
            cancellation,
        )
    }
}

struct CountBytes(usize);
impl std::fmt::Write for CountBytes {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.0 = self.0.checked_add(value.len()).ok_or(std::fmt::Error)?;
        Ok(())
    }
}

fn inspect(
    plan: &Plan,
    depth: usize,
    nodes: &mut usize,
    sources: &mut Vec<DatasetUri>,
    dynamic: &mut bool,
) -> Result<usize> {
    if depth > NATIVE_RECURSION_LIMIT {
        return Err(failed("recursive plan exceeds 24 levels"));
    }
    *nodes = nodes
        .checked_add(1)
        .ok_or_else(|| failed("native plan size overflow"))?;
    let mut descend = |input: &Plan| inspect(input, depth + 1, nodes, sources, dynamic);
    Ok(match plan {
        Plan::Scan(scan) => {
            if !sources.iter().any(|uri| uri == &scan.source_uri) {
                sources.push(scan.source_uri.clone());
            }
            depth
        }
        Plan::Project(node) => descend(&node.input)?,
        Plan::Filter(node) => descend(&node.input)?,
        Plan::Sort(node) => descend(&node.input)?,
        Plan::Limit(node) => descend(&node.input)?,
        Plan::Aggregate(node) => descend(&node.input)?,
        Plan::Window(node) => descend(&node.input)?,
        Plan::Unary(node) => {
            let result = descend(&node.input)?;
            *dynamic |= node.request.kind == shardloom_vortex::VortexQueryPrimitiveKind::PivotRows;
            result
        }
        Plan::Join(node) => descend(&node.left)?.max(descend(&node.right)?),
        Plan::Set(node) => descend(&node.left)?.max(descend(&node.right)?),
        Plan::Subquery(node) | Plan::CorrelatedSubquery(node) => {
            descend(&node.input)?.max(descend(&node.relation)?)
        }
        Plan::Outer => depth,
        Plan::ExecutionResult(_) | Plan::DeferredSubquery(_) => {
            return Err(failed(
                "execution-scoped relations cannot be reused as declarations",
            ));
        }
    })
}
