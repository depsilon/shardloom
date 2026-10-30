//! Data-dependent SQL lowering remains syntax/schema work; the native binder
//! owns every row, sparse pivot state and downstream execution.

use super::{
    BTreeMap, DatasetUri, ParsedPredicate, ParsedRelationLeaf, ParsedRelationQuery,
    ParsedRelationSource, ParsedSqlLocalSource,
};
#[cfg(test)]
use super::{
    BTreeSet, Lowerer, NativeResult, PreparedVortexRelational, SourceResolver,
    VortexLocalPrimitiveExecutionPolicy, declared_query_sources, unsupported_sql_error,
};
#[cfg(test)]
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
    super::visit::Node::Query(query).dynamic()
}

pub(super) fn source_required(
    source: &ParsedRelationSource,
    inputs: &super::BoundRelations,
) -> bool {
    super::visit::Node::Source(source).dynamic_bound(inputs)
}

pub(super) fn select_required(
    parsed: &ParsedSqlLocalSource,
    inputs: &super::BoundRelations,
) -> bool {
    super::visit::Node::Select(parsed).dynamic_bound(inputs)
}

pub(super) fn predicate_required(
    predicate: &ParsedPredicate,
    inputs: &super::BoundRelations,
) -> bool {
    super::visit::Node::Predicate(predicate).dynamic_bound(inputs)
}

#[cfg(test)]
#[allow(clippy::too_many_lines)] // Fixture preparation mirrors the production admission and dynamic binder.
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
    let size = declaration_size(&parsed, cancellation)?;
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
        let token = schemas.cancellation_token();
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
        let mut memo = super::LoweringMemo::new(schemas)?;
        let mut lowerer = Lowerer {
            schemas,
            memo: &mut memo,
            frontier: None,
            serial: 0,
            resolve_source: &mut resolver,
            declaration: Some(declaration.clone()),
            outer: None,
            bound_inputs: super::BoundRelations::new(),
            lowered_input: None,
            cancellation: token.clone(),
            parameters: BTreeMap::new(),
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

#[cfg(test)]
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

#[cfg(test)]
fn declaration_size<'a>(
    query: &'a ParsedRelationQuery,
    cancellation: &shardloom_exec::compute_pool::CancellationToken,
) -> NativeResult<super::visit::Admission<'a>> {
    super::visit::Admission::inspect(query, cancellation)
}
