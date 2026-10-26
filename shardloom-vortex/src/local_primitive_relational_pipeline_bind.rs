//! Bind each flat streaming stage through the ordinary native type/semantic binder.

use super::super::{BoundObservation, PipelineKind, PipelineStage};
use super::{Binder, Node, NodeKind, Result, failed, validate_name};
use crate::relational_query::{VortexRelationalPipeline, VortexRelationalStage};

impl Binder<'_> {
    pub(super) fn pipeline(
        &mut self,
        pipeline: &VortexRelationalPipeline,
        depth: usize,
    ) -> Result<Node> {
        self.charge_items(pipeline.stages.len(), 256)?;
        let mut node = self.bind(&pipeline.input, depth + 1)?;
        for stage in &pipeline.stages {
            self.cancellation.check()?;
            node = match stage {
                VortexRelationalStage::Project(expressions) => {
                    self.project_input(expressions, node)?
                }
                VortexRelationalStage::Filter(predicate) => self.filter_input(predicate, node)?,
                VortexRelationalStage::Limit { offset, count } => {
                    self.limit_input(*offset, *count, node)?
                }
                VortexRelationalStage::Observe(observation) => {
                    validate_name(&observation.name)?;
                    if self.parameterized_binding || self.outer_fields.is_some() {
                        return Err(failed(
                            "observations inside correlated execution are not admitted",
                        ));
                    }
                    self.charge(
                        observation
                            .name
                            .len()
                            .checked_mul(4)
                            .ok_or_else(|| failed("observation name capacity overflow"))?
                            + 256,
                    )?;
                    if !self.observation_names.insert(observation.name.clone()) {
                        return Err(failed(
                            "observation names must be unique across the complete action",
                        ));
                    }
                    // Bind private metric expressions without narrowing the
                    // actual pass-through input or introducing another scan.
                    let projection = if observation.projection.is_empty() {
                        None
                    } else {
                        self.charge_fields(node.fields.len())?;
                        let projected = self.project_input(
                            &observation.projection,
                            Node {
                                fields: node.fields.clone(),
                                kind: NodeKind::Outer,
                            },
                        )?;
                        match projected.kind {
                            NodeKind::Project { expressions, .. } => {
                                Some((projected.fields, expressions))
                            }
                            NodeKind::Outer => None,
                            _ => return Err(failed("observation projection changed its relation")),
                        }
                    };
                    let fields = projection
                        .as_ref()
                        .map_or(node.fields.as_slice(), |(fields, _)| fields);
                    let spec = self.aggregate_spec(fields, &[], &observation.measures)?;
                    if spec.measures.iter().any(|measure| {
                        !matches!(
                            measure.function,
                            crate::local_primitives::SimpleAggregateFunction::Count
                                | crate::local_primitives::SimpleAggregateFunction::Sum
                                | crate::local_primitives::SimpleAggregateFunction::Min
                                | crate::local_primitives::SimpleAggregateFunction::Max
                        )
                    }) {
                        return Err(failed(
                            "observations support COUNT, SUM, MIN and MAX without DISTINCT",
                        ));
                    }
                    self.charge_fields(node.fields.len())?;
                    let output_fields = node.fields.clone();
                    append(
                        node,
                        output_fields,
                        PipelineKind::Observe(Box::new(BoundObservation {
                            name: observation.name.clone(),
                            projection,
                            spec,
                        })),
                    )
                }
            };
            // Compact immediately, including on the path to a later error, so
            // neither successful ownership nor failed binding drops a deep tree.
            node = compact(node);
        }
        Ok(node)
    }
}

fn compact(node: Node) -> Node {
    let Node { fields, kind } = node;
    let (input, stage) = match kind {
        NodeKind::Project { input, expressions } => (input, PipelineKind::Project(expressions)),
        NodeKind::Filter { input, predicate } => (input, PipelineKind::Filter(predicate)),
        NodeKind::Limit {
            input,
            offset,
            count,
        } => (input, PipelineKind::Limit { offset, count }),
        kind => return Node { fields, kind },
    };
    append(*input, fields, stage)
}

fn append(input: Node, fields: Vec<(String, super::DType)>, kind: PipelineKind) -> Node {
    let stage = PipelineStage {
        fields: fields.clone(),
        kind,
    };
    let (input, mut stages) = match input.kind {
        NodeKind::Pipeline { input, stages } => (input, stages),
        _ => (Box::new(input), Vec::new()),
    };
    stages.push(stage);
    Node {
        fields,
        kind: NodeKind::Pipeline { input, stages },
    }
}
