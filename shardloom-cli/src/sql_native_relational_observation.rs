//! Position-preserving metrics reuse ordinary scalar and aggregate lowering.

use super::{
    AggregateFunction, Lowered, Lowerer, NativeResult, ParsedAggregateArgument,
    ParsedProjectionOutput, ParsedRelationQuery, ParsedRelationSource, Plan, unsupported_sql_error,
};
use shardloom_vortex::relational_query::{
    VortexRelationalObservation, VortexRelationalPipeline, VortexRelationalStage,
};

pub(super) fn validate(query: &ParsedRelationQuery) -> NativeResult<()> {
    let ParsedRelationQuery::Select(parsed) = query else {
        return Err(unsupported_sql_error(
            "observation requires scalar aggregates",
        ));
    };
    let input = matches!(&parsed.source, ParsedRelationSource::Derived(input)
        if matches!(input.as_ref(), ParsedRelationQuery::Input(0)));
    if !input
        || parsed.join.is_some()
        || !parsed.group_by.is_empty()
        || parsed.grouping_sets.is_some()
        || !parsed.predicate.is_all()
        || !parsed.having.is_all()
        || parsed.order_by.is_some()
        || !parsed.limit_is_synthetic
        || parsed.offset != 0
        || parsed.distinct_projection
        || parsed.replace_or_add_projection
        || parsed.aggregates.is_empty()
        || parsed
            .projection_order
            .iter()
            .any(|output| !matches!(output, ParsedProjectionOutput::Aggregate(_)))
    {
        return Err(unsupported_sql_error(
            "observations require global aggregates over their declared input",
        ));
    }
    for aggregate in &parsed.aggregates {
        if aggregate.distinct
            || !matches!(
                aggregate.function,
                AggregateFunction::Count
                    | AggregateFunction::Sum
                    | AggregateFunction::Min
                    | AggregateFunction::Max
            )
        {
            return Err(unsupported_sql_error(
                "observations support COUNT, SUM, MIN and MAX without DISTINCT",
            ));
        }
        if let ParsedAggregateArgument::Computed { expression, .. } = &aggregate.argument {
            // No second relation, correlated scope or effectful source may hide
            // inside a metric. Ordinary typed expressions use the shared binder.
            expression.plain()?;
        }
        if let Some(ParsedAggregateArgument::Computed { expression, .. }) = &aggregate.filter {
            expression.plain()?;
        }
    }
    Ok(())
}

impl Lowerer<'_, '_> {
    pub(super) fn observation(
        &mut self,
        query: &ParsedRelationQuery,
        name: &str,
    ) -> NativeResult<Lowered> {
        validate(query)?;
        let ParsedRelationQuery::Select(parsed) = query else {
            unreachable!("validated select")
        };
        let ParsedRelationSource::Derived(source) = &parsed.source else {
            unreachable!("validated input")
        };
        let mut input = self.query(source)?;
        let plan = std::mem::replace(&mut input.plan, Plan::Input(usize::MAX));
        let columns = input.columns.clone();
        let qualifiers = input.qualifiers.clone();
        let (aggregate, _) = self.aggregate(input, parsed)?;
        let Plan::Aggregate(aggregate) = aggregate.plan else {
            unreachable!("aggregate lowering")
        };
        let projection = match aggregate.input {
            Plan::Input(usize::MAX) => vec![],
            Plan::Project(project) if matches!(project.input, Plan::Input(usize::MAX)) => {
                project.expressions
            }
            _ => {
                return Err(unsupported_sql_error(
                    "observation expression introduced another relation",
                ));
            }
        };
        Ok(Lowered {
            plan: Plan::Pipeline(Box::new(VortexRelationalPipeline {
                input: plan,
                stages: vec![VortexRelationalStage::Observe(
                    VortexRelationalObservation {
                        name: name.into(),
                        projection,
                        measures: aggregate.measures,
                    },
                )],
            }))
            .compact_streaming(),
            columns,
            qualifiers,
        })
    }
}
