//! Lexical retained declarations lower once into shared immutable native plans.

use super::{
    BTreeMap, BTreeSet, FrontierBinding, Lowered, Lowerer, NativeResult,
    ParsedRelationQuery as Query, Plan, dynamic, predicate, unsupported_sql_error,
};
use shardloom_vortex::relational_query::{
    VortexRelationalRecursionId, VortexRelationalRecursive, VortexRelationalRecursiveInput,
};
use std::sync::Arc;

pub(super) fn validate_query(query: &Query, inputs: &super::BoundRelations) -> NativeResult<()> {
    for node in super::visit::Node::Query(query).walk() {
        if let super::visit::Node::Query(
            query @ (Query::Materialized { .. } | Query::Recursive { .. }),
        ) = node
            && (super::visit::Node::Query(query).dynamic_bound(inputs)
                || predicate::query_outer(query, inputs)?)
        {
            return Err(unsupported_sql_error(
                "retained definitions require static parameter-independent schemas",
            ));
        }
    }
    Ok(())
}

impl Lowerer<'_, '_> {
    pub(super) fn retained_query(&mut self, query: &Query) -> NativeResult<Lowered> {
        if let Query::RecursiveInput(identity) = query {
            let frontier = self
                .frontier
                .as_ref()
                .filter(|frontier| &frontier.identity == identity)
                .ok_or_else(|| {
                    unsupported_sql_error("recursive input is outside its lexical recursive term")
                })?;
            return Ok(Lowered {
                plan: Plan::RecursiveInput(VortexRelationalRecursiveInput {
                    identity: frontier.native.clone(),
                    columns: frontier.columns.clone(),
                }),
                columns: frontier.columns.clone(),
                qualifiers: BTreeMap::new(),
            });
        }
        if self.outer.is_some() {
            return Err(unsupported_sql_error(
                "retained definitions do not admit parameter-dependent lowering",
            ));
        }
        let (Query::Materialized { identity, .. } | Query::Recursive { identity, .. }) = query
        else {
            return Err(unsupported_sql_error(
                "expected a retained query declaration",
            ));
        };
        if let Some(lowered) = self.memo.get(&identity.0, 0, 3) {
            return Ok(lowered);
        }
        // This validation precedes lowerers that can discover pivot rows.
        if super::visit::Node::Query(query).dynamic_bound(&self.bound_inputs)
            || predicate::query_outer(query, &self.bound_inputs)?
        {
            return Err(unsupported_sql_error(
                "retained definitions require static parameter-independent schemas",
            ));
        }
        let prior = self.frontier.take();
        let lowered = self.retained_definition(query);
        self.frontier = prior;
        let lowered = lowered?;
        self.memo.insert(&identity.0, 0, 3, &lowered)?;
        Ok(lowered)
    }

    fn retained_definition(&mut self, query: &Query) -> NativeResult<Lowered> {
        match query {
            Query::Materialized { input, .. } => {
                let mut input = self.query(input)?;
                self.prune(&mut input.plan, None, &mut BTreeSet::new())?;
                Ok(Lowered {
                    plan: Plan::Materialized(Arc::new(input.plan.compact_streaming())),
                    columns: input.columns,
                    qualifiers: BTreeMap::new(),
                })
            }
            Query::Recursive {
                identity,
                anchor,
                step,
                columns,
                distinct,
            } => {
                if dynamic::required(anchor) || dynamic::required(step) {
                    return Err(unsupported_sql_error(
                        "recursive terms require static schemas",
                    ));
                }
                let mut anchor = self.query(anchor)?;
                if !columns.is_empty() {
                    anchor = Self::rename_columns(anchor, columns)?;
                }
                self.prune(&mut anchor.plan, None, &mut BTreeSet::new())?;
                let native = VortexRelationalRecursionId::default();
                self.frontier = Some(FrontierBinding {
                    identity: identity.clone(),
                    native: native.clone(),
                    columns: anchor.columns.clone(),
                });
                let step = self.query(step);
                self.frontier = None;
                let mut step = step?;
                if step.columns.len() != anchor.columns.len() {
                    return Err(unsupported_sql_error(
                        "recursive term must preserve the anchor field count",
                    ));
                }
                self.prune(&mut step.plan, None, &mut BTreeSet::new())?;
                Ok(Lowered {
                    plan: Plan::Recursive(Arc::new(VortexRelationalRecursive {
                        identity: native,
                        anchor: anchor.plan.compact_streaming(),
                        step: step.plan.compact_streaming(),
                        distinct: *distinct,
                    })),
                    columns: anchor.columns,
                    qualifiers: BTreeMap::new(),
                })
            }
            _ => Err(unsupported_sql_error(
                "expected a retained query definition",
            )),
        }
    }
}
