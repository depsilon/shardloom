//! Only row-preserving, noncomputing views may use exact source cardinality.

use super::{
    Metrics, NativeExecutionContext, Node, NodeKind, PipelineKind, Result, ScanSource,
    native_relational_expression,
};

pub(super) fn source_count(
    mut node: &Node,
    metrics: &Metrics<'_>,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<u64>> {
    let Some(input) = metrics.input else {
        return Ok(None);
    };
    loop {
        match &node.kind {
            NodeKind::Project {
                input,
                expressions,
            } if direct(expressions) => node = input,
            NodeKind::Pipeline { input, stages }
                if stages.iter().all(|stage| {
                    matches!(&stage.kind, PipelineKind::Project(expressions) if direct(expressions))
                }) => node = input,
            NodeKind::Scan {
                source: ScanSource::Batch { index, .. },
                predicate: None,
                residual: None,
                plan,
                ..
            } if plan.filter.is_none()
                && plan.residual_predicate.is_none()
                && plan.embedded_derived_column_rewrites.is_empty()
                && plan.source_order_limit.is_none() => return input.metadata_count(*index, context),
            _ => return Ok(None),
        }
    }
}

fn direct(expressions: &[native_relational_expression::Expression]) -> bool {
    expressions.iter().all(|expression| {
        matches!(
            expression.kind,
            native_relational_expression::Kind::Column(_)
        )
    })
}
