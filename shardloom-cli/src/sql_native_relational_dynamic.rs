//! Data-dependent SQL lowering remains syntax/schema work; the native binder
//! owns every row, sparse pivot state and downstream execution.

use super::{
    BTreeMap, BTreeSet, DatasetUri, Lowerer, NativeResult, ParsedPredicate, ParsedRelationLeaf,
    ParsedRelationQuery, ParsedRelationSource, ParsedSqlLocalSource, PreparedVortexRelational,
    SourceResolver, VortexLocalPrimitiveExecutionPolicy, declared_query_sources,
    unsupported_sql_error,
};
use shardloom_vortex::local_primitives::prepared_relational::{
    VortexRelationalPreparation, prepare_relational_with_dynamic_inputs,
    prepare_relational_with_dynamic_inputs_in_session_controlled,
};

#[derive(Clone)]
pub(super) struct Declaration {
    pub(super) sources: std::sync::Arc<BTreeMap<ParsedRelationLeaf, Vec<DatasetUri>>>,
    pub(super) bytes: usize,
}

pub(super) fn required(query: &ParsedRelationQuery) -> bool {
    match query {
        ParsedRelationQuery::Select(parsed) => select_required(parsed),
        ParsedRelationQuery::Set(set) => set.branches.iter().any(select_required),
    }
}

pub(super) fn source_required(source: &ParsedRelationSource) -> bool {
    match source {
        ParsedRelationSource::Local(_) => false,
        ParsedRelationSource::Derived(query) => required(query),
        ParsedRelationSource::Unary(unary) => {
            unary.request.kind == shardloom_vortex::VortexQueryPrimitiveKind::PivotRows
                || required(&unary.input)
        }
    }
}

pub(super) fn select_required(parsed: &ParsedSqlLocalSource) -> bool {
    source_required(&parsed.source)
        || super::scalar::surfaces(parsed)
            .into_iter()
            .any(super::scalar::dynamic_required)
        || parsed
            .join
            .as_ref()
            .is_some_and(|join| source_required(&join.right_source))
        || parsed
            .predicate_surfaces()
            .into_iter()
            .any(predicate_required)
}

pub(super) fn predicate_required(predicate: &ParsedPredicate) -> bool {
    let (source, filter, projected) = match predicate {
        ParsedPredicate::GenericExpressionCompare { left, right, .. } => {
            return super::scalar::dynamic_required(left) || super::scalar::dynamic_required(right);
        }
        ParsedPredicate::Logical { left, right, .. } => {
            return predicate_required(left) || predicate_required(right);
        }
        ParsedPredicate::Not { inner } => return predicate_required(inner),
        ParsedPredicate::InSubquery { subquery, .. }
        | ParsedPredicate::QuantifiedSubquery { subquery, .. } => (
            &subquery.source,
            subquery.predicate.as_ref(),
            subquery.projected_plan.as_deref(),
        ),
        ParsedPredicate::RowValueInSubquery { subquery, .. } => (
            &subquery.source,
            subquery.predicate.as_ref(),
            subquery.projected_plan.as_deref(),
        ),
        ParsedPredicate::ExistsSubquery { subquery } => (
            &subquery.source,
            subquery.predicate.as_ref(),
            subquery.projected_plan.as_deref(),
        ),
        _ => return false,
    };
    source_required(source) || predicate_required(filter) || projected.is_some_and(select_required)
}

pub(super) fn prepare(
    parsed: ParsedRelationQuery,
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: Option<&shardloom_vortex::resident_session::ResidentVortexSession>,
    inputs: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> NativeResult<()>,
    resolve_source: &mut SourceResolver<'_>,
    cancellation: &shardloom_exec::compute_pool::CancellationToken,
) -> NativeResult<PreparedVortexRelational> {
    cancellation.check()?;
    let mut leaves = BTreeSet::new();
    declared_query_sources(&parsed, &mut leaves);
    if leaves.is_empty() || leaves.len() > 128 {
        return Err(unsupported_sql_error(
            "dynamic SQL requires between 1 and 128 source leaves",
        ));
    }
    // Retain the already parsed declaration, including nested statement copies.
    // Every execution only resolves schemas and lowers it; it does not reparse.
    let size = declaration_size(&parsed)?;
    let mut declaration_bytes = size.bytes;
    let shared_memory =
        session.map(shardloom_vortex::resident_session::ResidentVortexSession::shared_memory_pool);
    let mut sources = BTreeMap::new();
    let mut uses = BTreeMap::<String, (DatasetUri, u8)>::new();
    for leaf in leaves {
        cancellation.check()?;
        let uris = if leaf.memory_input.is_some() {
            vec![DatasetUri::new(leaf.path.to_string_lossy())?]
        } else {
            // A caller-owned session already has live inputs/results. Compatibility
            // preparation must compete with those owners for its existing grant.
            resolve_source(&leaf, shared_memory.as_ref())?
        };
        let uri_bytes = uris
            .iter()
            .try_fold(0usize, |bytes, uri| bytes.checked_add(uri.as_str().len()))
            .ok_or_else(|| unsupported_sql_error("dynamic SQL source metadata overflow"))?;
        declaration_bytes = leaf
            .path
            .as_os_str()
            .len()
            .checked_add(uri_bytes)
            .and_then(|bytes| bytes.checked_mul(8))
            .and_then(|bytes| bytes.checked_add(16_384))
            .and_then(|bytes| declaration_bytes.checked_add(bytes))
            .ok_or_else(|| unsupported_sql_error("dynamic SQL source metadata overflow"))?;
        let count = *size
            .source_uses
            .get(&leaf)
            .ok_or_else(|| unsupported_sql_error("dynamic SQL source occurrence is absent"))?;
        for uri in &uris {
            let entry = uses
                .entry(uri.as_str().to_owned())
                .or_insert_with(|| (uri.clone(), 0));
            entry.1 = (entry.1 + count).min(2);
        }
        sources.insert(leaf, uris);
    }
    let (uris, reused) = partition_source_uses(uses);
    let declaration = Declaration {
        sources: std::sync::Arc::new(sources),
        bytes: declaration_bytes,
    };
    let memory_parsed = parsed.clone();
    let initialize = |schemas: &mut VortexRelationalPreparation<'_>| {
        super::register_sql_memory_inputs(&memory_parsed, schemas)?;
        inputs(schemas)
    };
    let lower = move |schemas: &mut VortexRelationalPreparation<'_>| {
        // A pivot can execute before a later branch has even been lowered.
        // Declare complete-tree reuse now, combining aliases of each URI.
        for uri in &reused {
            schemas.declare_source_reuse(uri)?;
        }
        let mut resolver =
            |leaf: &ParsedRelationLeaf, _: Option<&shardloom_exec::live_memory::LiveMemoryPool>| {
                declaration.sources.get(leaf).cloned().ok_or_else(|| {
                    unsupported_sql_error("dynamic SQL referenced an undeclared source")
                })
            };
        let mut lowerer = Lowerer {
            schemas,
            serial: 0,
            resolve_source: &mut resolver,
            declaration: Some(declaration.clone()),
            outer: None,
        };
        let mut query = lowerer.query(&parsed)?;
        lowerer.prune(&mut query.plan, None, &mut BTreeSet::new())?;
        Ok(query.plan)
    };
    match session {
        Some(session) => prepare_relational_with_dynamic_inputs_in_session_controlled(
            &uris,
            policy,
            session,
            declaration_bytes,
            initialize,
            lower,
            cancellation,
        ),
        None => prepare_relational_with_dynamic_inputs(
            &uris,
            policy,
            declaration_bytes,
            initialize,
            lower,
        ),
    }
}

fn partition_source_uses(
    uses: BTreeMap<String, (DatasetUri, u8)>,
) -> (Vec<DatasetUri>, Vec<DatasetUri>) {
    let mut uris = Vec::with_capacity(uses.len());
    let mut reused = Vec::new();
    for (uri, count) in uses.into_values() {
        if count > 1 {
            reused.push(uri.clone());
        }
        uris.push(uri);
    }
    (uris, reused)
}

fn declaration_size(query: &ParsedRelationQuery) -> NativeResult<DeclarationSize<'_>> {
    let mut size = DeclarationSize {
        bytes: 65_536,
        queries: 0,
        predicates: 0,
        source_uses: BTreeMap::new(),
    };
    size.query(query, 0)?;
    Ok(size)
}

/// A conservative declaration credit, separate from native operator credits.
/// Count every retained normalized statement, even shared Arc children. The
/// per-byte allowance covers the parser's projection/predicate representations,
/// their Vec/String headers, and overlapping per-parameter lowering copies.
/// This is not a claim that the credit bounds total process RSS.
struct DeclarationSize<'a> {
    bytes: usize,
    queries: usize,
    predicates: usize,
    source_uses: BTreeMap<&'a ParsedRelationLeaf, u8>,
}

impl<'a> DeclarationSize<'a> {
    fn query(&mut self, query: &'a ParsedRelationQuery, depth: usize) -> NativeResult<()> {
        if depth > 24 {
            return Err(unsupported_sql_error(
                "dynamic declaration exceeds 24 levels",
            ));
        }
        match query {
            ParsedRelationQuery::Select(parsed) => self.select(parsed, depth),
            ParsedRelationQuery::Set(set) => {
                for branch in &set.branches {
                    self.select(branch, depth)?;
                }
                Ok(())
            }
        }
    }

    fn select(&mut self, parsed: &'a ParsedSqlLocalSource, depth: usize) -> NativeResult<()> {
        self.queries += 1;
        if self.queries > 128 || depth > 24 {
            return Err(unsupported_sql_error(
                "dynamic declaration exceeds 128 queries or 24 levels",
            ));
        }
        self.bytes = parsed
            .normalized_statement
            .len()
            .checked_mul(1024)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<ParsedSqlLocalSource>() * 16))
            .and_then(|bytes| self.bytes.checked_add(bytes))
            .ok_or_else(|| unsupported_sql_error("dynamic declaration metadata size overflow"))?;
        self.source(&parsed.source, depth + 1)?;
        if let Some(join) = &parsed.join {
            self.source(&join.right_source, depth + 1)?;
        }
        for predicate in parsed.predicate_surfaces() {
            self.predicate(predicate, depth + 1)?;
        }
        for value in super::scalar::surfaces(parsed) {
            self.scalar(value, depth + 1)?;
        }
        Ok(())
    }

    fn scalar(
        &mut self,
        value: &'a super::scalar_expression::ParsedScalarExpression,
        depth: usize,
    ) -> NativeResult<()> {
        for (_, binding) in &value.bindings {
            match binding {
                super::scalar_expression::RelationalBinding::Scalar(query) => {
                    self.query(query, depth)?;
                }
                super::scalar_expression::RelationalBinding::Predicate(predicate) => {
                    self.predicate(predicate, depth)?;
                }
            }
        }
        Ok(())
    }

    fn source(&mut self, source: &'a ParsedRelationSource, depth: usize) -> NativeResult<()> {
        match source {
            ParsedRelationSource::Local(leaf) => {
                let count = self.source_uses.entry(leaf).or_default();
                *count = (*count + 1).min(2);
                Ok(())
            }
            ParsedRelationSource::Derived(query) => self.query(query, depth),
            ParsedRelationSource::Unary(unary) => self.query(&unary.input, depth),
        }
    }

    fn predicate(&mut self, predicate: &'a ParsedPredicate, depth: usize) -> NativeResult<()> {
        self.predicates += 1;
        if depth > 24 || self.predicates > 4096 {
            return Err(unsupported_sql_error(
                "dynamic declaration exceeds 24 levels or 4096 predicates",
            ));
        }
        let (source, filter, projected) = match predicate {
            ParsedPredicate::GenericExpressionCompare { left, right, .. } => {
                self.scalar(left, depth + 1)?;
                self.scalar(right, depth + 1)?;
                return Ok(());
            }
            ParsedPredicate::Logical { left, right, .. } => {
                self.predicate(left, depth + 1)?;
                return self.predicate(right, depth + 1);
            }
            ParsedPredicate::Not { inner } => return self.predicate(inner, depth + 1),
            ParsedPredicate::InSubquery { subquery, .. }
            | ParsedPredicate::QuantifiedSubquery { subquery, .. } => (
                &subquery.source,
                subquery.predicate.as_ref(),
                subquery.projected_plan.as_deref(),
            ),
            ParsedPredicate::RowValueInSubquery { subquery, .. } => (
                &subquery.source,
                subquery.predicate.as_ref(),
                subquery.projected_plan.as_deref(),
            ),
            ParsedPredicate::ExistsSubquery { subquery } => (
                &subquery.source,
                subquery.predicate.as_ref(),
                subquery.projected_plan.as_deref(),
            ),
            _ => return Ok(()),
        };
        self.source(source, depth + 1)?;
        self.predicate(filter, depth + 1)?;
        if let Some(projected) = projected {
            self.select(projected, depth + 1)?;
        }
        Ok(())
    }
}
