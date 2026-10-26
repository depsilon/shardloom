//! Owned declarations of the existing typed IR. This layer only binds sources;
//! expression, operator and storage semantics remain in the shared native engine.

use super::{
    CancellationToken, DatasetUri, ExecutionResources, PreparedVortexRelational,
    ResidentVortexSession, Result, SqlSourceBinding, failed, register_sources, resident_relational,
    sql_request, validate_sql_spill,
};
use crate::sql_local_source_runtime::native_relational::{
    BoundRelations, Lowered, NativeSqlDeclaration, NativeSqlLowering, ParsedRelationLeaf,
    ResolvedSources,
};
use shardloom_vortex::relational_query::{
    NATIVE_RECURSION_LIMIT, VortexRelationalPlan as Plan, VortexRelationalScan,
};
use std::{collections::BTreeMap, fmt::Write as _, sync::Arc};

#[path = "native_runtime_plan_expressions.rs"]
mod expressions;
#[path = "native_runtime_plan_lowering.rs"]
mod lowering;

#[derive(Clone)]
enum Body {
    Typed(Arc<Plan>),
    Sql(Arc<NativeSqlDeclaration>),
}

/// An inert native relation declaration, retaining exactly the same typed tree
/// produced by SQL lowering. It never stores input rows or an external evaluator.
#[derive(Clone)]
pub struct NativeRelationDeclaration {
    body: Body,
    inputs: BoundRelations,
    pub(crate) sources: BTreeMap<ParsedRelationLeaf, u8>,
    pub(crate) metadata_bytes: usize,
    pub(crate) dynamic: bool,
    pub(crate) outer: bool,
    depth: usize,
    recursive_depth: usize,
    observation_names: std::collections::BTreeSet<String>,
}

impl NativeRelationDeclaration {
    /// Inspect the shared named-parameter contract without binding or executing.
    /// # Errors
    /// Rejects malformed quoting, parameter names and positional placeholders.
    pub fn parameter_names(raw: &str) -> Result<Vec<String>> {
        NativeSqlDeclaration::parameter_names(raw)
    }

    /// Wrap an existing typed plan without inspecting a source. Recursive depth
    /// uses the existing binder guard; total node count is not a workload limit.
    /// # Errors
    /// Rejects execution-scoped references, recursive depth and size overflow.
    pub fn new(plan: Plan) -> Result<Self> {
        Self::compose(plan, Vec::new())
    }

    /// Compose one typed operator template over immutable child declarations.
    /// Input slots are local to this template. Construction does not copy child
    /// trees, inspect sources, or retain execution state.
    /// # Errors
    /// Rejects unbound slots, execution-scoped references and metadata overflow.
    pub fn compose(plan: Plan, inputs: Vec<Arc<Self>>) -> Result<Self> {
        let mut plan = plan.compact_streaming();
        let mut inputs = inputs.into_iter().enumerate().collect::<BoundRelations>();
        let mut inspection = PlanInspection::default();
        let depth = inspect(&plan, &inputs, 0, &mut inspection)?;
        let PlanInspection {
            used_inputs,
            sources,
            mut nodes,
            dynamic,
            child_bytes,
            observation_names,
        } = inspection;
        inputs.retain(|index, _| used_inputs.contains(index));
        let outer = inputs.values().any(|input| input.outer);
        let recursive_depth = recursive_depth(&plan, &inputs)?;
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
            .and_then(|bytes| bytes.checked_add(child_bytes))
            .ok_or_else(|| failed("native plan metadata size overflow"))?;
        Ok(Self {
            body: Body::Typed(Arc::new(plan)),
            inputs,
            sources,
            metadata_bytes,
            dynamic,
            outer,
            depth,
            recursive_depth,
            observation_names,
        })
    }

    /// Parse SQL and snapshot its named relation inputs without inspecting data.
    /// Names bind syntax nodes, with case-insensitive ordinary identifiers and
    /// lexical WITH scopes. Repeated references share declarations, not results.
    /// # Errors
    /// Rejects malformed/duplicate names, unsupported SQL and metadata overflow.
    pub fn sql(raw: &str, relations: Vec<(String, Arc<Self>)>) -> Result<Self> {
        Self::sql_with_parameters(raw, relations, BTreeMap::new())
    }

    /// Bind exact named scalar values into a new immutable SQL declaration.
    /// Parameter names are case-sensitive; values never represent identifiers
    /// or relations. Every action uses fresh parameter-sensitive native state.
    /// # Errors
    /// Rejects missing, extra or malformed parameters and unsupported SQL/types.
    pub fn sql_with_parameters(
        raw: &str,
        relations: Vec<(String, Arc<Self>)>,
        parameters: BTreeMap<String, shardloom_core::ScalarValue>,
    ) -> Result<Self> {
        let parameters = parameters
            .into_iter()
            .map(|(name, value)| {
                Ok((
                    name.clone(),
                    shardloom_core::Expression::literal(
                        shardloom_core::ExprId::new(format!("parameter.{name}"))?,
                        value,
                    ),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Self::sql_with_bound_parameters(raw, relations, parameters)
    }

    /// Bind scalar literal nodes with optional explicit checked type casts.
    /// This preserves typed nulls and exact declared decimal/unsigned types.
    /// # Errors
    /// Rejects arbitrary expressions/identifiers as parameters, invalid names,
    /// unsupported SQL and metadata overflow. Execution never evaluates a host
    /// callback to produce a parameter value.
    pub fn sql_with_bound_parameters(
        raw: &str,
        relations: Vec<(String, Arc<Self>)>,
        parameters: BTreeMap<String, shardloom_core::Expression>,
    ) -> Result<Self> {
        let mut parameter_bytes = CountBytes(0);
        for value in parameters.values() {
            expressions::parameter(value, 0)?;
        }
        write!(&mut parameter_bytes, "{parameters:?}")
            .map_err(|_| failed("parameter metadata size overflow"))?;
        let names = relations
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let sql = NativeSqlDeclaration::new(raw, &names, parameters)?;
        let inputs = relations
            .into_iter()
            .enumerate()
            .filter(|(index, _)| sql.input_uses.contains_key(index))
            .map(|(index, (_, input))| (index, input))
            .collect::<BoundRelations>();
        sql.validate_inputs(&inputs)?;
        let outer = sql.references_outer(&inputs)?;
        let mut metadata_bytes = parameter_bytes
            .0
            .checked_mul(8)
            .and_then(|bytes| bytes.checked_add(sql.metadata_bytes))
            .ok_or_else(|| failed("parameter metadata size overflow"))?;
        let mut sources = sql.sources.clone();
        let mut dynamic = sql.dynamic;
        let mut depth = sql.depth;
        let mut recursive_depth = sql.recursive_depth;
        let mut observation_names = std::collections::BTreeSet::new();
        for (&index, &uses) in &sql.input_uses {
            let child = &inputs[&index];
            for name in &child.observation_names {
                if uses != 1 || !observation_names.insert(name.clone()) {
                    return Err(failed(
                        "observation names must be unique across the complete action",
                    ));
                }
            }
            metadata_bytes = child
                .metadata_bytes
                .checked_mul(uses)
                .and_then(|bytes| bytes.checked_add(metadata_bytes))
                .ok_or_else(|| failed("native plan metadata size overflow"))?;
            for (source, &count) in &child.sources {
                let total = sources.entry(source.clone()).or_default();
                *total = (*total + count * (u8::from(uses > 0) + u8::from(uses > 1))).min(2);
            }
            dynamic |= child.dynamic;
            depth = depth.max(
                sql.depth
                    .checked_add(child.depth)
                    .ok_or_else(|| failed("native plan depth overflow"))?,
            );
            recursive_depth = recursive_depth.max(
                sql.recursive_depth
                    .checked_add(child.recursive_depth)
                    .ok_or_else(|| failed("native plan depth overflow"))?,
            );
        }
        Ok(Self {
            body: Body::Sql(Arc::new(sql)),
            inputs,
            sources,
            metadata_bytes,
            dynamic,
            outer,
            depth,
            recursive_depth,
            observation_names,
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

    /// Logical operator depth, computed without walking shared children.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.depth
    }

    pub(crate) fn recursive_depth(&self) -> usize {
        self.recursive_depth
    }

    /// Bound input slots retained by this declaration. Unused supplied names
    /// retain neither declarations nor input owners.
    pub fn input_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.inputs.keys().copied()
    }

    /// Reserve lowering metadata before a caller opens an input producer.
    /// # Errors
    /// Rejects excessive depth, cancellation and insufficient shared capacity.
    pub fn admit(
        &self,
        session: &ResidentVortexSession,
        cancellation: &CancellationToken,
    ) -> Result<shardloom_exec::live_memory::MemoryLease> {
        cancellation.check()?;
        if self.recursive_depth > NATIVE_RECURSION_LIMIT
            || self.dynamic && self.depth > NATIVE_RECURSION_LIMIT
        {
            return Err(failed("recursive plan exceeds 24 levels"));
        }
        session.reserve_input_scratch(
            u64::try_from(self.metadata_bytes).map_err(|_| failed("plan metadata overflow"))?,
        )
    }

    /// Apply an existing native unary operator to this relation. Options use
    /// the same bounded metadata grammar as SQL/DataFrame declarations: column
    /// lists for distinct/duplicates, a positive count for tail, and JSON for
    /// sample/rewrite/melt/rolling/explode/pivot. This performs no source I/O.
    /// # Errors
    /// Rejects unknown operations/options and excessive declaration depth.
    pub fn unary(
        self: &Arc<Self>,
        operation: &str,
        options: &str,
        keep: Option<&str>,
    ) -> Result<Self> {
        let request =
            crate::sql_local_source_runtime::native_unary_request(operation, options, keep)?;
        Self::compose(
            Plan::Unary(Box::new(
                shardloom_vortex::relational_query::VortexRelationalUnary {
                    input: Plan::Input(0),
                    request,
                },
            )),
            vec![Arc::clone(self)],
        )
    }

    /// Observe native scalar aggregates at this plan position without changing
    /// its rows or evaluating another query. Metrics use the shared SQL binder.
    /// # Errors
    /// Rejects non-aggregate expressions, distinct state, subqueries, invalid
    /// names and metadata overflow. Construction performs no input effects.
    /// # Panics
    /// Panics if the SQL constructor stops returning a uniquely owned SQL body.
    pub fn observe(self: &Arc<Self>, name: &str, measures: &[String]) -> Result<Self> {
        if measures.is_empty() {
            return Err(failed("observation requires at least one aggregate"));
        }
        let mut declaration = Self::sql(
            &format!("SELECT {} FROM __sl_observed_input", measures.join(",")),
            vec![("__sl_observed_input".into(), Arc::clone(self))],
        )?;
        let Body::Sql(sql) = &mut declaration.body else {
            unreachable!("SQL constructor");
        };
        Arc::get_mut(sql)
            .expect("new SQL declaration")
            .observe(name)?;
        if !declaration
            .observation_names
            .insert(name.to_ascii_lowercase())
        {
            return Err(failed(
                "observation names must be unique across the complete action",
            ));
        }
        declaration.metadata_bytes = declaration
            .metadata_bytes
            .checked_add(
                name.len()
                    .checked_mul(8)
                    .ok_or_else(|| failed("observation metadata overflow"))?,
            )
            .ok_or_else(|| failed("observation metadata overflow"))?;
        // Observation state is flat and independent of the recursive binder.
        declaration.recursive_depth = self.recursive_depth;
        Ok(declaration)
    }

    /// Produce action-local operator descriptions only after the caller has
    /// reserved `metadata_bytes`. The explicit work stack also owns failure
    /// cleanup; composing a long declaration does not grow the call stack.
    fn instantiate(
        &self,
        lower_sql: &mut impl FnMut(&Self) -> Result<Plan>,
        cancellation: &CancellationToken,
        lowered_input: Option<(usize, &Plan)>,
    ) -> Result<Plan> {
        let Body::Typed(template) = &self.body else {
            return lower_sql(self);
        };
        let mut plan = template.as_ref().clone();
        let mut pending = vec![(&mut plan, self)];
        while let Some((node, declaration)) = pending.pop() {
            cancellation.check()?;
            match node {
                Plan::Input(index) => {
                    if let Some((slot, lowered)) = lowered_input
                        && *index == slot
                        && std::ptr::eq(declaration, self)
                    {
                        *node = lowered.clone();
                        continue;
                    }
                    let input = declaration
                        .inputs
                        .get(index)
                        .ok_or_else(|| failed("native declaration input is not bound"))?;
                    // Lower deep children as bounded pipelines before attaching
                    // them to a multi-input tree. Cancellation or an error must
                    // never drop a partially expanded recursive unary chain.
                    if input.depth > 12 {
                        *node = lower_sql(input)?;
                        continue;
                    }
                    match &input.body {
                        Body::Typed(template) => {
                            *node = template.as_ref().clone();
                            pending.push((node, input.as_ref()));
                        }
                        Body::Sql(_) => *node = lower_sql(input)?,
                    }
                }
                Plan::Project(node) => pending.push((&mut node.input, declaration)),
                Plan::Filter(node) => pending.push((&mut node.input, declaration)),
                Plan::Sort(node) => pending.push((&mut node.input, declaration)),
                Plan::Limit(node) => pending.push((&mut node.input, declaration)),
                Plan::Pipeline(node) => pending.push((&mut node.input, declaration)),
                Plan::Aggregate(node) => pending.push((&mut node.input, declaration)),
                Plan::Window(node) => pending.push((&mut node.input, declaration)),
                Plan::Unary(node) => pending.push((&mut node.input, declaration)),
                Plan::Join(node) => {
                    pending.push((&mut node.right, declaration));
                    pending.push((&mut node.left, declaration));
                }
                Plan::Set(node) => {
                    pending.push((&mut node.right, declaration));
                    pending.push((&mut node.left, declaration));
                }
                Plan::Subquery(node) | Plan::CorrelatedSubquery(node) => {
                    pending.push((&mut node.relation, declaration));
                    pending.push((&mut node.input, declaration));
                }
                Plan::Scan(_) | Plan::Outer => {}
                Plan::ExecutionResult(_) | Plan::DeferredSubquery(_) => {
                    return Err(failed(
                        "execution-scoped relation is not a reusable declaration",
                    ));
                }
            }
        }
        Ok(plan.compact_streaming())
    }

    pub(crate) fn lower(
        &self,
        schemas: &mut shardloom_vortex::local_primitives::prepared_relational::VortexRelationalPreparation<'_>,
        sources: &Arc<ResolvedSources>,
        cancellation: &CancellationToken,
    ) -> Result<Lowered> {
        self.lower_with_outer(schemas, sources, cancellation, None)
    }

    pub(crate) fn lower_with_outer(
        &self,
        schemas: &mut shardloom_vortex::local_primitives::prepared_relational::VortexRelationalPreparation<'_>,
        sources: &Arc<ResolvedSources>,
        cancellation: &CancellationToken,
        outer: Option<&[String]>,
    ) -> Result<Lowered> {
        let mut current = self;
        let mut pending = Vec::new();
        loop {
            cancellation.check()?;
            if current.inputs.len() != 1 {
                break;
            }
            let (&index, child) = current.inputs.first_key_value().expect("one input");
            // Correlated inputs acquire their scope at use, while dynamic
            // inputs may own single-use execution references. Neither is an
            // ordinary reusable static operator template.
            if child.outer || child.dynamic {
                break;
            }
            pending.push((current, index));
            current = child;
        }
        let mut lowered = current.lower_one(schemas, sources, cancellation, outer, None)?;
        for (parent, index) in pending.into_iter().rev() {
            cancellation.check()?;
            lowered = parent.lower_one(
                schemas,
                sources,
                cancellation,
                outer,
                Some((index, lowered)),
            )?;
        }
        Ok(lowered)
    }

    fn lower_one(
        &self,
        schemas: &mut shardloom_vortex::local_primitives::prepared_relational::VortexRelationalPreparation<'_>,
        sources: &Arc<ResolvedSources>,
        cancellation: &CancellationToken,
        outer: Option<&[String]>,
        lowered_input: Option<(usize, Lowered)>,
    ) -> Result<Lowered> {
        cancellation.check()?;
        if let Body::Sql(sql) = &self.body {
            return sql.lower(
                schemas,
                cancellation,
                NativeSqlLowering {
                    sources: sources.clone(),
                    inputs: self.inputs.clone(),
                    declaration_bytes: self.metadata_bytes,
                    outer: outer.map(<[String]>::to_vec),
                    lowered_input,
                },
            );
        }
        let mut lower_sql = |declaration: &Self| {
            declaration
                .lower_with_outer(schemas, sources, cancellation, outer)
                .map(|result| result.plan)
        };
        let mut plan = self.instantiate(
            &mut lower_sql,
            cancellation,
            lowered_input
                .as_ref()
                .map(|(index, lowered)| (*index, &lowered.plan)),
        )?;
        lowering::rebind(&mut plan, sources, cancellation)?;
        let columns = lowering::columns(&mut plan, schemas, cancellation)?;
        Ok(Lowered {
            plan,
            columns,
            qualifiers: BTreeMap::new(),
        })
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
        let admission = self.admit(session, cancellation)?;
        self.prepare_admitted(resources, session, bindings, spill, cancellation, admission)
    }

    /// Check that schema binding cannot trigger compatibility ingest or a
    /// data-dependent operator. This check performs no source inspection.
    /// # Errors
    /// Rejects sources without an admitted metadata-only schema path.
    pub fn validate_schema_inspection(&self, bindings: &[SqlSourceBinding]) -> Result<()> {
        if self.dynamic {
            return Err(failed(
                "output schema depends on data; inspect the completed Result schema",
            ));
        }
        for leaf in self.sources.keys() {
            if leaf.memory_input.is_some() {
                continue;
            }
            let reference = leaf.path.to_string_lossy();
            let binding = bindings
                .iter()
                .find(|binding| binding.reference == reference);
            let supported = match binding.map(|binding| &binding.source) {
                Some(super::SqlSource::File { format, .. }) => {
                    matches!(format.as_str(), "vortex" | "parquet" | "arrow-ipc")
                }
                Some(_) => true,
                None => matches!(
                    leaf.path.extension().and_then(|value| value.to_str()),
                    Some("vortex" | "parquet" | "arrow" | "ipc" | "feather")
                ),
            };
            if !supported {
                return Err(failed(
                    "schema inspection requires native Vortex metadata or a declared native input schema; compatibility schema inference is an explicit data action",
                ));
            }
        }
        Ok(())
    }

    /// Bind output schema from declared sources or self-describing file metadata.
    /// Inspection never starts compatibility ingestion, producers, or a query.
    /// # Errors
    /// Rejects data-dependent schemas, unavailable metadata, resource denial and
    /// normal native type/binding errors.
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub fn schema_json_admitted(
        &self,
        resources: ExecutionResources,
        session: &ResidentVortexSession,
        bindings: &[SqlSourceBinding],
        spill: Option<&str>,
        cancellation: &CancellationToken,
        admission: shardloom_exec::live_memory::MemoryLease,
    ) -> Result<shardloom_exec::live_memory::Budgeted<String>> {
        self.validate_schema_inspection(bindings)?;
        let mut bindings = bindings.to_vec();
        let mut metadata = Vec::new();
        let mut aliases = BTreeMap::new();
        let mut serial = 0usize;
        for leaf in self
            .sources
            .keys()
            .filter(|leaf| leaf.memory_input.is_none())
        {
            cancellation.check()?;
            let reference = leaf.path.to_string_lossy();
            let index = bindings
                .iter()
                .position(|binding| binding.reference == reference);
            let format = match index.map(|index| &bindings[index].source) {
                Some(super::SqlSource::File { format, .. }) => format.as_str(),
                Some(_) => continue,
                None => match leaf.path.extension().and_then(|value| value.to_str()) {
                    Some("parquet") => "parquet",
                    Some("arrow" | "ipc" | "feather") => "arrow-ipc",
                    _ => continue,
                },
            };
            if format == "vortex" {
                continue;
            }
            let schema = shardloom_vortex::universal_format_io::inspect_local_columnar_schema(
                &leaf.path,
                format,
                session,
                cancellation,
            )?;
            let source = super::SqlSource::ColumnarStream(Arc::clone(schema.value()));
            metadata.push(schema);
            let alias = loop {
                let name = format!("memory://schema-inspection/{serial}");
                serial += 1;
                if !self
                    .sources
                    .keys()
                    .any(|source| source.path.to_string_lossy() == name)
                {
                    break DatasetUri::new(name)?;
                }
            };
            aliases.insert(reference.to_string(), alias);
            if let Some(index) = index {
                bindings[index].source = source;
            } else {
                bindings.push(SqlSourceBinding {
                    reference: reference.into_owned(),
                    source,
                });
            }
        }
        validate_sql_spill(spill, resources)?;
        let request = sql_request("", resources, &bindings, spill)?;
        let (prepared, _) = resident_relational::prepare_native_in_session(
            self,
            &request,
            session,
            |schemas| {
                for binding in &mut bindings {
                    if let Some(alias) = aliases.get(&binding.reference) {
                        binding.reference = alias.as_str().to_owned();
                    }
                }
                register_sources(schemas, &bindings, cancellation)
            },
            cancellation,
            admission,
            Some(&aliases),
        )?;
        prepared.output_schema_json()?.ok_or_else(|| {
            failed("output schema depends on data; inspect the completed Result schema")
        })
    }

    /// Resolve remaining ordinary source identifiers against explicit source
    /// declarations. Child snapshots have already passed their own admission.
    /// # Errors
    /// Rejects unknown relation names before producers or file inspection.
    pub fn validate_named_sources<'a>(
        &self,
        names: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        let names = names.into_iter().collect::<std::collections::BTreeSet<_>>();
        for leaf in self.sources.keys().filter(|leaf| leaf.declared_identifier) {
            if self
                .inputs
                .values()
                .any(|child| child.sources.contains_key(leaf))
            {
                continue;
            }
            let name = leaf.path.to_string_lossy();
            if !names.iter().any(|known| known.eq_ignore_ascii_case(&name)) {
                return Err(failed(&format!(
                    "unknown relation {name:?}; register a plan or supply an explicit relation binding"
                )));
            }
        }
        Ok(())
    }

    /// Exact source references retained by the bound declaration. Unused
    /// catalog snapshots are excluded before any owner or producer is opened.
    pub fn source_references(&self) -> impl Iterator<Item = String> + '_ {
        self.sources
            .keys()
            .map(|leaf| leaf.path.to_string_lossy().into_owned())
    }

    /// Prepare using an admission retained while input adapters were opened.
    /// # Errors
    /// Rejects foreign/insufficient admission and normal preparation failures.
    pub fn prepare_admitted(
        &self,
        resources: ExecutionResources,
        session: &ResidentVortexSession,
        bindings: &[SqlSourceBinding],
        spill: Option<&str>,
        cancellation: &CancellationToken,
        admission: shardloom_exec::live_memory::MemoryLease,
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
            admission,
            None,
        )
    }
}

impl Drop for NativeRelationDeclaration {
    fn drop(&mut self) {
        let mut pending = std::mem::take(&mut self.inputs)
            .into_values()
            .collect::<Vec<_>>();
        while let Some(input) = pending.pop() {
            if let Ok(mut declaration) = Arc::try_unwrap(input) {
                pending.extend(std::mem::take(&mut declaration.inputs).into_values());
            }
        }
    }
}

struct CountBytes(usize);
impl std::fmt::Write for CountBytes {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.0 = self.0.checked_add(value.len()).ok_or(std::fmt::Error)?;
        Ok(())
    }
}

#[derive(Default)]
struct PlanInspection {
    nodes: usize,
    sources: BTreeMap<ParsedRelationLeaf, u8>,
    dynamic: bool,
    child_bytes: usize,
    used_inputs: std::collections::BTreeSet<usize>,
    observation_names: std::collections::BTreeSet<String>,
}

fn inspect(
    plan: &Plan,
    inputs: &BoundRelations,
    depth: usize,
    state: &mut PlanInspection,
) -> Result<usize> {
    if depth > NATIVE_RECURSION_LIMIT {
        return Err(failed("recursive plan exceeds 24 levels"));
    }
    state.nodes = state
        .nodes
        .checked_add(1)
        .ok_or_else(|| failed("native plan size overflow"))?;
    let mut descend = |input: &Plan| inspect(input, inputs, depth + 1, state);
    Ok(match plan {
        Plan::Input(index) => {
            let input = inputs
                .get(index)
                .ok_or_else(|| failed("native declaration input is not bound"))?;
            for name in &input.observation_names {
                if !state.observation_names.insert(name.clone()) {
                    return Err(failed(
                        "observation names must be unique across the complete action",
                    ));
                }
            }
            state.child_bytes = state
                .child_bytes
                .checked_add(input.metadata_bytes)
                .ok_or_else(|| failed("native plan metadata size overflow"))?;
            for (source, &count) in &input.sources {
                let total = state.sources.entry(source.clone()).or_default();
                *total = (*total + count).min(2);
            }
            state.dynamic |= input.dynamic;
            state.used_inputs.insert(*index);
            depth
                .checked_add(input.depth)
                .ok_or_else(|| failed("native plan depth overflow"))?
        }
        Plan::Scan(scan) => {
            let total = state
                .sources
                .entry(lowering::leaf(&scan.source_uri))
                .or_default();
            *total = (*total + 1).min(2);
            depth
        }
        Plan::Project(node) => descend(&node.input)?,
        Plan::Filter(node) => descend(&node.input)?,
        Plan::Sort(node) => descend(&node.input)?,
        Plan::Limit(node) => descend(&node.input)?,
        Plan::Pipeline(node) => {
            let depth = descend(&node.input)?;
            for stage in &node.stages {
                if let shardloom_vortex::relational_query::VortexRelationalStage::Observe(
                    observation,
                ) = stage
                    && !state.observation_names.insert(observation.name.clone())
                {
                    return Err(failed(
                        "observation names must be unique across the complete action",
                    ));
                }
            }
            depth
                .checked_add(node.stages.len())
                .ok_or_else(|| failed("native plan depth overflow"))?
        }
        Plan::Aggregate(node) => descend(&node.input)?,
        Plan::Window(node) => descend(&node.input)?,
        Plan::Unary(node) => {
            let result = descend(&node.input)?;
            state.dynamic |=
                node.request.kind == shardloom_vortex::VortexQueryPrimitiveKind::PivotRows;
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

fn recursive_depth(plan: &Plan, inputs: &BoundRelations) -> Result<usize> {
    let mut pending = vec![(plan, 0usize)];
    let mut maximum = 0usize;
    while let Some((plan, depth)) = pending.pop() {
        maximum = maximum.max(depth);
        let nested = depth
            .checked_add(1)
            .ok_or_else(|| failed("native plan depth overflow"))?;
        match plan {
            Plan::Input(index) => {
                maximum = maximum.max(
                    depth
                        .checked_add(
                            inputs
                                .get(index)
                                .ok_or_else(|| failed("native input is unbound"))?
                                .recursive_depth,
                        )
                        .ok_or_else(|| failed("native plan depth overflow"))?,
                );
            }
            Plan::Project(node) => pending.push((&node.input, depth)),
            Plan::Filter(node) => pending.push((&node.input, depth)),
            Plan::Limit(node) => pending.push((&node.input, depth)),
            Plan::Pipeline(node) => pending.push((&node.input, depth)),
            Plan::Sort(node) => pending.push((&node.input, nested)),
            Plan::Aggregate(node) => pending.push((&node.input, nested)),
            Plan::Window(node) => pending.push((&node.input, nested)),
            Plan::Unary(node) => pending.push((&node.input, nested)),
            Plan::Join(node) => pending.extend([(&node.left, nested), (&node.right, nested)]),
            Plan::Set(node) => pending.extend([(&node.left, nested), (&node.right, nested)]),
            Plan::Subquery(node) | Plan::CorrelatedSubquery(node) => {
                pending.extend([(&node.input, nested), (&node.relation, nested)]);
            }
            Plan::Scan(_) | Plan::Outer | Plan::ExecutionResult(_) | Plan::DeferredSubquery(_) => {}
        }
    }
    Ok(maximum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_vortex::relational_query::{
        VortexRelationalLimit, VortexRelationalSet, VortexRelationalSetKind,
    };

    #[test]
    fn native_declarations_share_children_and_instantiate_fresh_operator_trees() {
        let input = Arc::new(NativeRelationDeclaration::scan("absent.vortex").unwrap());
        let declaration = NativeRelationDeclaration::compose(
            Plan::Set(Box::new(VortexRelationalSet {
                left: Plan::Input(0),
                right: Plan::Input(1),
                kind: VortexRelationalSetKind::UnionAll,
            })),
            vec![Arc::clone(&input), Arc::clone(&input)],
        )
        .unwrap();
        assert!(Arc::ptr_eq(
            &declaration.inputs[&0],
            &declaration.inputs[&1]
        ));
        assert!(declaration.metadata_bytes > 2 * input.metadata_bytes);
        assert_eq!(
            declaration.sources.keys().collect::<Vec<_>>(),
            input.sources.keys().collect::<Vec<_>>()
        );
        assert!(declaration.sources.values().all(|count| *count == 2));
        let first = declaration
            .instantiate(
                &mut |_| panic!("no SQL in this template"),
                &CancellationToken::default(),
                None,
            )
            .unwrap();
        let second = declaration
            .instantiate(
                &mut |_| panic!("no SQL in this template"),
                &CancellationToken::default(),
                None,
            )
            .unwrap();
        assert_eq!(first, second);
        let (Plan::Set(first), Plan::Set(second)) = (&first, &second) else {
            panic!("set")
        };
        assert!(!std::ptr::eq(first.as_ref(), second.as_ref()));
        assert!(matches!(
            (&first.left, &first.right),
            (Plan::Scan(_), Plan::Scan(_))
        ));
        assert!(NativeRelationDeclaration::new(Plan::Input(0)).is_err());
    }

    #[test]
    fn native_declarations_construct_and_release_deep_chains_without_recursive_copies() {
        let source = Arc::new(NativeRelationDeclaration::scan("absent.vortex").unwrap());
        let mut declaration = Arc::clone(&source);
        for _ in 0..2048 {
            declaration = Arc::new(
                NativeRelationDeclaration::compose(
                    Plan::Limit(Box::new(VortexRelationalLimit {
                        input: Plan::Input(0),
                        offset: 0,
                        count: 1,
                    })),
                    vec![declaration],
                )
                .unwrap(),
            );
        }
        assert_eq!(declaration.depth(), 2048);
        assert_eq!(Arc::strong_count(&source), 2);
        drop(declaration);
        assert_eq!(Arc::strong_count(&source), 1);
    }

    #[test]
    fn native_declaration_multiplicity_is_admitted_without_expanding_the_graph() {
        let mut declaration = Arc::new(NativeRelationDeclaration::scan("absent.vortex").unwrap());
        let mut denied = false;
        for _ in 0..64 {
            let result = NativeRelationDeclaration::compose(
                Plan::Set(Box::new(VortexRelationalSet {
                    left: Plan::Input(0),
                    right: Plan::Input(1),
                    kind: VortexRelationalSetKind::UnionAll,
                })),
                vec![Arc::clone(&declaration), Arc::clone(&declaration)],
            );
            match result {
                Ok(next) => declaration = Arc::new(next),
                Err(error) => {
                    assert!(
                        error.to_string().contains("metadata size overflow"),
                        "{error}"
                    );
                    denied = true;
                    break;
                }
            }
        }
        assert!(denied);
    }

    fn collect(declaration: &NativeRelationDeclaration) -> serde_json::Value {
        use shardloom_core::ExecutionResourceOrigin;
        let resources =
            ExecutionResources::from_bytes(64 << 20, 2, ExecutionResourceOrigin::Context).unwrap();
        let session = ResidentVortexSession::new(64 << 20, 2).unwrap();
        let cancellation = CancellationToken::default();
        let (operation, reusable) = declaration
            .prepare(resources, &session, &[], None, &cancellation)
            .unwrap();
        assert!(reusable);
        cancellation.cancel();
        let mut observed = None;
        for _ in 0..2 {
            let action = CancellationToken::default();
            let result = operation.collect_jsonl(&action).unwrap();
            let values = serde_json::Value::Array(
                result
                    .result_jsonl
                    .value()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect(),
            );
            if let Some(previous) = &observed {
                assert_eq!(previous, &values);
            }
            observed = Some(values);
        }
        assert_eq!(session.snapshot().prepared_source_opens, 0);
        observed.unwrap()
    }

    #[test]
    fn native_declaration_unused_bindings_do_not_retain_inputs() {
        let unused = Arc::new(NativeRelationDeclaration::scan("absent.vortex").unwrap());
        let used = Arc::new(NativeRelationDeclaration::sql("SELECT 7 AS value", vec![]).unwrap());
        let relation = NativeRelationDeclaration::sql(
            "SELECT * FROM actual",
            vec![
                ("unused".into(), unused.clone()),
                ("actual".into(), used.clone()),
            ],
        )
        .unwrap();
        assert_eq!(Arc::strong_count(&unused), 1);
        assert_eq!(Arc::strong_count(&used), 2);
        assert_eq!(relation.input_indices().collect::<Vec<_>>(), vec![1]);
        assert_eq!(collect(&relation), serde_json::json!([{"value":7}]));
        let typed =
            NativeRelationDeclaration::compose(Plan::Input(1), vec![unused.clone(), used]).unwrap();
        assert_eq!(Arc::strong_count(&unused), 1);
        assert_eq!(collect(&typed), serde_json::json!([{"value":7}]));
    }

    #[test]
    fn native_declaration_sql_and_typed_plans_form_one_computation() {
        use shardloom_core::{ColumnRef, ExprId, Expression, ScalarValue};
        use shardloom_vortex::relational_query::VortexRelationalProject;
        let sql = Arc::new(NativeRelationDeclaration::sql("SELECT 3 AS x", vec![]).unwrap());
        let typed = Arc::new(
            NativeRelationDeclaration::compose(
                Plan::Project(Box::new(VortexRelationalProject {
                    input: Plan::Input(0),
                    expressions: vec![
                        (
                            "value".into(),
                            Expression::column(
                                ExprId::new("value").unwrap(),
                                ColumnRef::new("x").unwrap(),
                            ),
                        ),
                        (
                            "added".into(),
                            Expression::literal(
                                ExprId::new("added").unwrap(),
                                ScalarValue::Int64(9),
                            ),
                        ),
                    ],
                })),
                vec![sql],
            )
            .unwrap(),
        );
        let connected = NativeRelationDeclaration::sql(
            "WITH chosen AS (SELECT value+added AS total FROM named) SELECT a.total+b.total AS answer FROM chosen AS a CROSS JOIN chosen AS b",
            vec![("NAMED".into(), typed)],
        ).unwrap();
        assert_eq!(collect(&connected), serde_json::json!([{"answer":24}]));
        assert!(connected.sources.values().all(|count| *count == 2));
    }

    #[test]
    fn native_declaration_named_snapshots_preserve_scopes_and_subqueries() {
        let mut registered =
            Arc::new(NativeRelationDeclaration::sql("SELECT 5 AS value", vec![]).unwrap());
        let bound = NativeRelationDeclaration::sql(
            "SELECT (SELECT value FROM named) AS original FROM named WHERE value IN (SELECT value FROM named)",
            vec![("named".into(), registered.clone())],
        ).unwrap();
        registered =
            Arc::new(NativeRelationDeclaration::sql("SELECT 99 AS value", vec![]).unwrap());
        assert_eq!(collect(&bound), serde_json::json!([{"original":5}]));
        let shadowed = NativeRelationDeclaration::sql(
            "WITH named AS (SELECT 7 AS value) SELECT value FROM named",
            vec![("named".into(), registered)],
        )
        .unwrap();
        assert_eq!(collect(&shadowed), serde_json::json!([{"value":7}]));
        let child = Arc::new(NativeRelationDeclaration::scan("absent.vortex").unwrap());
        assert!(
            NativeRelationDeclaration::sql(
                "SELECT * FROM named",
                vec![
                    ("named".into(), child.clone()),
                    ("NAMED".into(), child.clone())
                ]
            )
            .is_err()
        );
        assert!(
            NativeRelationDeclaration::sql(
                "SELECT * FROM named",
                vec![("invalid name".into(), child)]
            )
            .is_err()
        );
    }

    #[test]
    fn native_declaration_parameters_bind_exact_values_without_sql_interpolation() {
        use shardloom_core::ScalarValue;
        let declaration = NativeRelationDeclaration::sql_with_parameters(
            "SELECT :amount AS money,CAST(:date AS utf8) AS day,:missing AS missing,:text AS text,:n+2 AS plus WHERE :n>1",
            vec![], BTreeMap::from([
                ("amount".into(), ScalarValue::Decimal128 { value: 12345, precision: 12, scale: 3 }),
                ("date".into(), ScalarValue::Date32(0)),
                ("missing".into(), ScalarValue::Null),
                ("text".into(), ScalarValue::Utf8("'; SELECT * FROM absent -- :fake".into())),
                ("n".into(), ScalarValue::Int64(3)),
            ]),
        ).unwrap();
        assert_eq!(
            collect(&declaration),
            serde_json::json!([{
                "money":"decimal128(12,3):12345", "day":"1970-01-01", "missing":null,
                "text":"'; SELECT * FROM absent -- :fake", "plus":5,
            }])
        );
    }

    #[test]
    fn native_declaration_parameter_failures_are_inert_and_values_rebind_freshly() {
        use shardloom_core::ScalarValue;
        for (sql, parameters, expected) in [
            (
                "SELECT :required AS n",
                BTreeMap::new(),
                "missing scalar parameter :required",
            ),
            (
                "SELECT 1 AS n",
                BTreeMap::from([("extra".into(), ScalarValue::Int64(1))]),
                "extra scalar parameter :extra",
            ),
            (
                "SELECT :Name AS n",
                BTreeMap::from([("name".into(), ScalarValue::Int64(1))]),
                "missing scalar parameter :Name",
            ),
            ("SELECT ? AS n", BTreeMap::new(), "positional parameters"),
            (
                "SELECT * FROM :table",
                BTreeMap::from([("table".into(), ScalarValue::Utf8("absent.vortex".into()))]),
                "cannot name a relation",
            ),
        ] {
            let error = NativeRelationDeclaration::sql_with_parameters(sql, vec![], parameters)
                .err()
                .expect("must reject during declaration");
            assert!(error.to_string().contains(expected), "{error}");
        }
        let input = Arc::new(NativeRelationDeclaration::sql("SELECT 5 AS value", vec![]).unwrap());
        for threshold in [2, 9, 1] {
            let plan = NativeRelationDeclaration::sql_with_parameters(
                "SELECT value FROM named WHERE value>:minimum",
                vec![("named".into(), input.clone())],
                BTreeMap::from([("minimum".into(), ScalarValue::Int64(threshold))]),
            )
            .unwrap();
            assert_eq!(
                collect(&plan),
                if threshold < 5 {
                    serde_json::json!([{"value":5}])
                } else {
                    serde_json::json!([])
                }
            );
        }
        let plan = NativeRelationDeclaration::sql_with_parameters(
            "WITH unused AS (SELECT :n AS n) SELECT ':ignored' AS text",
            vec![],
            BTreeMap::from([("n".into(), ScalarValue::Int64(3))]),
        )
        .unwrap();
        assert_eq!(collect(&plan), serde_json::json!([{"text":":ignored"}]));
    }

    #[test]
    fn native_declaration_dynamic_inputs_get_fresh_action_state_and_scalar_admission() {
        let options =
            r#"{"index":"entity","columns":"category","values":"amount","aggregate":"sum"}"#;
        let pivot = Arc::new(NativeRelationDeclaration::sql(
            &format!("SELECT * FROM PIVOT((SELECT 1 AS entity,'a' AS category,4 AS amount), '{options}') AS p"), vec![],
        ).unwrap());
        let relation = NativeRelationDeclaration::sql(
            "SELECT a.pivot_a+b.pivot_a AS total FROM named AS a CROSS JOIN named AS b",
            vec![("named".into(), pivot.clone())],
        )
        .unwrap();
        assert_eq!(collect(&relation), serde_json::json!([{"total":8.0}]));
        let error = NativeRelationDeclaration::sql(
            "SELECT (SELECT pivot_a FROM named) AS value",
            vec![("named".into(), pivot)],
        )
        .err()
        .expect("dynamic scalar shape fails before action");
        assert!(
            error.to_string().contains("statically bound output schema"),
            "{error}"
        );
    }
}
