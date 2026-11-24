//! Parsed SQL templates use the same immutable inputs as typed declarations.
//! Source names bind into syntax slots; no SQL string or data is substituted.

use super::{
    BTreeMap, BTreeSet, BoundRelations, DatasetUri, Expression, Lowered, Lowerer, NativeResult,
    ParsedRelationLeaf, ParsedRelationQuery, Plan, VortexRelationalPreparation, dynamic,
    observation, parsed_native_query, predicate, relation_sources, scalar, unsupported_sql_error,
    validate_sql_identifier, visit,
};
use shardloom_exec::compute_pool::CancellationToken;
use std::sync::Arc;

pub(crate) type ResolvedSources = BTreeMap<ParsedRelationLeaf, Vec<DatasetUri>>;

pub(crate) struct NativeSqlLowering {
    pub(crate) sources: Arc<ResolvedSources>,
    pub(crate) inputs: BoundRelations,
    pub(crate) declaration_bytes: usize,
    pub(crate) outer: Option<Vec<String>>,
    pub(crate) lowered_input: Option<(usize, Lowered)>,
}

enum SqlDeclarationBody {
    Legacy(ParsedRelationQuery),
    Canonical {
        plan: Box<shardloom_plan::sql_binder::BoundSqlPlan>,
        bound_names: Vec<String>,
        raw_sql: String,
    },
}

pub(crate) struct NativeSqlDeclaration {
    query: SqlDeclarationBody,
    pub(crate) metadata_bytes: usize,
    pub(crate) sources: BTreeMap<ParsedRelationLeaf, u8>,
    pub(crate) input_uses: BTreeMap<usize, usize>,
    pub(crate) depth: usize,
    pub(crate) recursive_depth: usize,
    pub(crate) dynamic: bool,
    parameters: BTreeMap<String, Expression>,
    observation: Option<String>,
}

impl NativeSqlDeclaration {
    pub(crate) fn parameter_names(raw: &str) -> NativeResult<Vec<String>> {
        relation_sources::validate_query_structure(raw)?;
        Ok(super::parameters::names(raw)?.into_iter().collect())
    }

    pub(crate) fn new(
        raw: &str,
        names: &[String],
        parameters: BTreeMap<String, Expression>,
    ) -> NativeResult<Self> {
        super::parameters::validate(raw, &parameters)?;
        let canonical_candidate = match super::canonical::classify_canonical_sql(raw) {
            super::canonical::CanonicalSqlRoute::Admitted(plan) => Some(plan),
            super::canonical::CanonicalSqlRoute::LegacyNative => None,
            super::canonical::CanonicalSqlRoute::Rejected(err) => return Err(err),
        };
        if let Some(plan) = canonical_candidate
            .as_ref()
            .filter(|plan| super::canonical::prefers_canonical_lowering(plan, names))
        {
            let summary = super::canonical::inspect_canonical_plan(plan, names)?;
            return Ok(Self {
                query: SqlDeclarationBody::Canonical {
                    plan: canonical_candidate.expect("checked Some above"),
                    bound_names: names.to_vec(),
                    raw_sql: raw.to_owned(),
                },
                metadata_bytes: summary.metadata_bytes,
                sources: summary.sources,
                input_uses: summary.input_uses,
                depth: summary.depth,
                recursive_depth: 0,
                dynamic: false,
                parameters,
                observation: None,
            });
        }
        let mut legacy = match parsed_native_query(raw) {
            Ok(query) => query,
            Err(legacy_err) => {
                let Some(plan) = canonical_candidate else {
                    return Err(legacy_err);
                };
                let summary = super::canonical::inspect_canonical_plan(&plan, names)?;
                return Ok(Self {
                    query: SqlDeclarationBody::Canonical {
                        plan,
                        bound_names: names.to_vec(),
                        raw_sql: raw.to_owned(),
                    },
                    metadata_bytes: summary.metadata_bytes,
                    sources: summary.sources,
                    input_uses: summary.input_uses,
                    depth: summary.depth,
                    recursive_depth: 0,
                    dynamic: false,
                    parameters,
                    observation: None,
                });
            }
        };
        relation_sources::scopes::bind_inputs(&mut legacy, names)?;
        let admission = visit::Admission::inspect(&legacy, &CancellationToken::default())?;
        let metadata_bytes = admission.bytes;
        let sources = admission
            .source_uses
            .into_iter()
            .map(|(leaf, count)| (leaf.clone(), count))
            .collect();
        let input_uses = admission.input_uses;
        let depth = admission.query_depth;
        let recursive_depth = admission.recursive_depth;
        let dynamic = dynamic::required(&legacy);
        Ok(Self {
            query: SqlDeclarationBody::Legacy(legacy),
            metadata_bytes,
            sources,
            input_uses,
            depth,
            recursive_depth,
            dynamic,
            parameters,
            observation: None,
        })
    }

    pub(crate) fn validate_inputs(&self, inputs: &BoundRelations) -> NativeResult<()> {
        match &self.query {
            SqlDeclarationBody::Legacy(query) => {
                super::retained::validate_query(query, inputs)?;
                scalar::validate_bound_query(query, inputs)
            }
            SqlDeclarationBody::Canonical { .. } => Ok(()),
        }
    }

    pub(crate) fn references_outer(&self, inputs: &BoundRelations) -> NativeResult<bool> {
        match &self.query {
            SqlDeclarationBody::Legacy(query) => predicate::query_outer(query, inputs),
            SqlDeclarationBody::Canonical { .. } => Ok(false),
        }
    }

    pub(crate) fn observe(&mut self, name: &str) -> NativeResult<()> {
        validate_sql_identifier(name)?;
        match &self.query {
            SqlDeclarationBody::Legacy(query) => {
                observation::validate(query)?;
                self.observation = Some(name.to_ascii_lowercase());
                Ok(())
            }
            SqlDeclarationBody::Canonical {
                bound_names,
                raw_sql,
                ..
            } => {
                let Ok(mut legacy) = parsed_native_query(raw_sql) else {
                    return Err(unsupported_sql_error(
                        "OBSERVE is not admitted on canonical multi-join plans",
                    ));
                };
                relation_sources::scopes::bind_inputs(&mut legacy, bound_names)?;
                observation::validate(&legacy)?;
                self.query = SqlDeclarationBody::Legacy(legacy);
                self.observation = Some(name.to_ascii_lowercase());
                Ok(())
            }
        }
    }

    pub(crate) fn lower(
        &self,
        schemas: &mut VortexRelationalPreparation<'_>,
        cancellation: &CancellationToken,
        state: NativeSqlLowering,
        memo: &mut super::LoweringMemo,
    ) -> NativeResult<Lowered> {
        let mut resolver =
            |leaf: &ParsedRelationLeaf, _: Option<&shardloom_exec::live_memory::LiveMemoryPool>| {
                state
                    .sources
                    .get(leaf)
                    .cloned()
                    .ok_or_else(|| unsupported_sql_error("bound SQL source was not declared"))
            };
        let mut lowerer = Lowerer {
            schemas,
            memo,
            frontier: None,
            serial: 0,
            resolve_source: &mut resolver,
            declaration: Some(dynamic::Declaration {
                sources: state.sources.clone(),
                bytes: state.declaration_bytes,
            }),
            outer: state.outer,
            bound_inputs: state.inputs,
            lowered_input: state.lowered_input,
            cancellation: cancellation.clone(),
            parameters: self.parameters.clone(),
        };
        match (&self.query, &self.observation) {
            (SqlDeclarationBody::Legacy(query), Some(name)) => lowerer.observation(query, name),
            (SqlDeclarationBody::Legacy(query), None) => lowerer.query(query),
            (
                SqlDeclarationBody::Canonical {
                    plan, bound_names, ..
                },
                None,
            ) => super::canonical::lower_bound_sql_plan(plan, bound_names, &mut lowerer),
            (SqlDeclarationBody::Canonical { .. }, Some(_)) => Err(unsupported_sql_error(
                "OBSERVE is not admitted on canonical multi-join plans",
            )),
        }
    }
}

impl Drop for NativeSqlDeclaration {
    fn drop(&mut self) {
        if let SqlDeclarationBody::Legacy(query) = std::mem::replace(
            &mut self.query,
            SqlDeclarationBody::Legacy(ParsedRelationQuery::Input(usize::MAX)),
        ) {
            relation_sources::scopes::drain_query(query);
        }
    }
}

pub(crate) fn prune_bound_plan(
    plan: &mut Plan,
    schemas: &mut VortexRelationalPreparation<'_>,
    cancellation: &CancellationToken,
) -> NativeResult<()> {
    let mut resolver =
        |_: &ParsedRelationLeaf, _: Option<&shardloom_exec::live_memory::LiveMemoryPool>| {
            Err(unsupported_sql_error(
                "source resolution is not part of bound-plan pruning",
            ))
        };
    let mut memo = super::LoweringMemo::new(schemas)?;
    let mut lowerer = Lowerer {
        schemas,
        memo: &mut memo,
        frontier: None,
        serial: 0,
        resolve_source: &mut resolver,
        declaration: None,
        outer: None,
        bound_inputs: BoundRelations::new(),
        lowered_input: None,
        cancellation: cancellation.clone(),
        parameters: BTreeMap::new(),
    };
    lowerer.prune(plan, None, &mut BTreeSet::new())
}
