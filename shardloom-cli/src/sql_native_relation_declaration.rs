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

pub(crate) struct NativeSqlDeclaration {
    query: ParsedRelationQuery,
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
        let mut declaration = Self {
            query: parsed_native_query(raw)?,
            metadata_bytes: 0,
            sources: BTreeMap::new(),
            input_uses: BTreeMap::new(),
            depth: 0,
            recursive_depth: 0,
            dynamic: false,
            parameters,
            observation: None,
        };
        relation_sources::scopes::bind_inputs(&mut declaration.query, names)?;
        let admission =
            visit::Admission::inspect(&declaration.query, &CancellationToken::default())?;
        let metadata_bytes = admission.bytes;
        let sources = admission
            .source_uses
            .into_iter()
            .map(|(leaf, count)| (leaf.clone(), count))
            .collect();
        let input_uses = admission.input_uses;
        let depth = admission.query_depth;
        let recursive_depth = admission.recursive_depth;
        declaration.dynamic = dynamic::required(&declaration.query);
        declaration.metadata_bytes = metadata_bytes;
        declaration.sources = sources;
        declaration.input_uses = input_uses;
        declaration.depth = depth;
        declaration.recursive_depth = recursive_depth;
        Ok(declaration)
    }

    pub(crate) fn validate_inputs(&self, inputs: &BoundRelations) -> NativeResult<()> {
        scalar::validate_bound_query(&self.query, inputs)
    }

    pub(crate) fn references_outer(&self, inputs: &BoundRelations) -> NativeResult<bool> {
        predicate::query_outer(&self.query, inputs)
    }

    pub(crate) fn observe(&mut self, name: &str) -> NativeResult<()> {
        validate_sql_identifier(name)?;
        observation::validate(&self.query)?;
        self.observation = Some(name.to_ascii_lowercase());
        Ok(())
    }

    pub(crate) fn lower(
        &self,
        schemas: &mut VortexRelationalPreparation<'_>,
        cancellation: &CancellationToken,
        state: NativeSqlLowering,
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
        match &self.observation {
            Some(name) => lowerer.observation(&self.query, name),
            None => lowerer.query(&self.query),
        }
    }
}

impl Drop for NativeSqlDeclaration {
    fn drop(&mut self) {
        relation_sources::scopes::drain_query(std::mem::replace(
            &mut self.query,
            ParsedRelationQuery::Input(usize::MAX),
        ));
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
    let mut lowerer = Lowerer {
        schemas,
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
