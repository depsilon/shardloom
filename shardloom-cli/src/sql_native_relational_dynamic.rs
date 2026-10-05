//! Data-dependent SQL lowering remains syntax/schema work; the native binder
//! owns every row, sparse pivot state and downstream execution.

use super::{
    BTreeMap, BTreeSet, DatasetUri, Lowerer, NativeResult, ParsedPredicate, ParsedRelationLeaf,
    ParsedRelationQuery, ParsedRelationSource, ParsedSqlLocalSource, PreparedVortexRelational,
    VortexLocalPrimitiveExecutionPolicy, declared_query_sources, unsupported_sql_error,
};
use shardloom_vortex::local_primitives::prepared_relational::{
    VortexRelationalPreparation, prepare_relational_with_dynamic_inputs,
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
    inputs: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> NativeResult<()>,
    resolve_source: &mut dyn FnMut(&ParsedRelationLeaf) -> NativeResult<Vec<DatasetUri>>,
) -> NativeResult<PreparedVortexRelational> {
    let mut leaves = BTreeSet::new();
    declared_query_sources(&parsed, &mut leaves);
    if leaves.is_empty() || leaves.len() > 128 {
        return Err(unsupported_sql_error(
            "dynamic SQL requires between 1 and 128 source leaves",
        ));
    }
    // Retain the already parsed declaration, including nested statement copies.
    // Every execution only resolves schemas and lowers it; it does not reparse.
    let mut declaration_bytes = declaration_bytes(&parsed)?;
    let mut sources = BTreeMap::new();
    for leaf in leaves {
        let uris = if leaf.memory_input.is_some() {
            vec![DatasetUri::new(leaf.path.to_string_lossy())?]
        } else {
            resolve_source(&leaf)?
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
        sources.insert(leaf, uris);
    }
    let uris = sources.values().flatten().cloned().collect::<Vec<_>>();
    let declaration = Declaration {
        sources: std::sync::Arc::new(sources),
        bytes: declaration_bytes,
    };
    let memory_parsed = parsed.clone();
    prepare_relational_with_dynamic_inputs(
        &uris,
        policy,
        declaration_bytes,
        |schemas| {
            super::register_sql_memory_inputs(&memory_parsed, schemas)?;
            inputs(schemas)
        },
        move |schemas| {
            let mut resolver = |leaf: &ParsedRelationLeaf| {
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
        },
    )
}

fn declaration_bytes(query: &ParsedRelationQuery) -> NativeResult<usize> {
    let mut size = DeclarationSize {
        bytes: 65_536,
        queries: 0,
        predicates: 0,
    };
    size.query(query, 0)?;
    Ok(size.bytes)
}

/// A conservative declaration credit, separate from native operator credits.
/// Count every retained normalized statement, even shared Arc children. The
/// per-byte allowance covers the parser's projection/predicate representations,
/// their Vec/String headers, and overlapping per-parameter lowering copies.
/// This is not a claim that the credit bounds total process RSS.
struct DeclarationSize {
    bytes: usize,
    queries: usize,
    predicates: usize,
}

impl DeclarationSize {
    fn query(&mut self, query: &ParsedRelationQuery, depth: usize) -> NativeResult<()> {
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

    fn select(&mut self, parsed: &ParsedSqlLocalSource, depth: usize) -> NativeResult<()> {
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
        Ok(())
    }

    fn source(&mut self, source: &ParsedRelationSource, depth: usize) -> NativeResult<()> {
        match source {
            ParsedRelationSource::Local(_) => Ok(()),
            ParsedRelationSource::Derived(query) => self.query(query, depth),
            ParsedRelationSource::Unary(unary) => self.query(&unary.input, depth),
        }
    }

    fn predicate(&mut self, predicate: &ParsedPredicate, depth: usize) -> NativeResult<()> {
        self.predicates += 1;
        if depth > 24 || self.predicates > 4096 {
            return Err(unsupported_sql_error(
                "dynamic declaration exceeds 24 levels or 4096 predicates",
            ));
        }
        let (source, filter, projected) = match predicate {
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
