//! A pure inner dimension chain can feed exact matches to its existing
//! aggregate. Physical or uniqueness declines reuse every completed build.

use super::{Metrics, Node, NodeKind, PipelineKind, PreparedVortexRelational, transform};
use crate::{
    local_primitives::{
        logical_field_from_native_array,
        native_capacity::ReservedVec,
        native_relational_aggregate::{Aggregate, Spec as AggregateSpec},
        native_relational_batch::{Batch, failed},
        native_relational_expression::Kind as ExpressionKind,
        native_relational_join::{Join, Spec},
        native_relational_keys::KeyColumn,
        native_relational_matches::{Frame, Input, Origin, Source},
    },
    relational_query::{VortexRelationalJoinKind as Kind, VortexRelationalSide as Side},
    resident_session::NativeExecutionContext,
};
use shardloom_core::{
    Result,
    native_join_work::{NativeJoinAggregateWork, NativeJoinWork},
};
use vortex::array::{
    ArrayRef,
    dtype::{DType, Nullability, PType},
};

struct Stage<'a> {
    node: &'a Node,
    right: &'a Node,
    spec: &'a Spec,
    probe: ReservedVec<Origin>,
    validation: ReservedVec<Origin>,
}

pub(super) struct Plan<'a> {
    input: &'a Node,
    fact: &'a Node,
    stages: ReservedVec<Stage<'a>>,
    groups: ReservedVec<Origin>,
    measure: Option<Origin>,
}

fn position(node: &Node, name: &str) -> Option<usize> {
    node.fields.iter().position(|(field, _)| field == name)
}

impl<'a> Plan<'a> {
    pub(super) fn admit(
        input: &'a Node,
        aggregate: &AggregateSpec,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<Self>> {
        if !aggregate.admits_matches() {
            return Ok(None);
        }
        let mut stages = ReservedVec::new(context.memory())?;
        let Some((fact, origins)) = bind(input, &mut stages, context)? else {
            return Ok(None);
        };
        if stages.values.is_empty() {
            return Ok(None);
        }
        let mut groups = ReservedVec::new(context.memory())?;
        groups.reserve(aggregate.group_names.len())?;
        for name in &aggregate.group_names {
            let Some(index) = position(input, name) else {
                return Ok(None);
            };
            groups.values.push(origins.values[index]);
        }
        let measure = if let Some(name) = &aggregate.measures[0].column {
            let Some(index) = position(input, name) else {
                return Ok(None);
            };
            let origin = origins.values[index];
            if origin.source != 0
                || !matches!(fact.fields[origin.column].1, DType::Primitive(ptype, _) if ptype != PType::F16)
            {
                return Ok(None);
            }
            Some(origin)
        } else {
            None
        };
        Ok(Some(Self {
            input,
            fact,
            stages,
            groups,
            measure,
        }))
    }

    pub(super) fn run(
        &self,
        prepared: &PreparedVortexRelational,
        aggregate: &mut Aggregate<'_>,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        count_partial: bool,
    ) -> Result<()> {
        let mut work = NativeJoinAggregateWork {
            chains_considered: 1,
            ..Default::default()
        };
        let mut joins = self.build_joins(prepared, context, metrics, batch_rows)?;
        let mut dimensions = ReservedVec::new(context.memory())?;
        dimensions.reserve(joins.values.len())?;
        let mut direct = joins.values.iter().all(Join::unique_inner);
        work.chains_declined_duplicates = u64::from(!direct);
        if direct {
            for (join, stage) in joins.values.iter().zip(&self.stages.values) {
                if let Some(source) = Source::try_new(
                    &stage.right.fields,
                    &join.right_table().batches().map(|(_, batch)| &batch.array),
                    context,
                )? {
                    dimensions.values.push(source);
                } else {
                    direct = false;
                    work.chains_declined_representation = 1;
                    break;
                }
            }
        }
        if !direct {
            dimensions.values.clear();
        }
        let dtype = DType::struct_(self.input.fields.clone(), Nullability::NonNullable);
        prepared.run(
            self.fact,
            context,
            metrics,
            batch_rows,
            None,
            &mut |array| {
                let fact = if direct {
                    Source::try_new(&self.fact.fields, &std::iter::once(&array), context)?
                } else {
                    None
                };
                if let Some(fact) = fact {
                    work.merge(NativeJoinAggregateWork {
                        source_batches_direct: 1,
                        ..Default::default()
                    })?;
                    let keys = self.probe_keys(&array, context)?;
                    self.matches(
                        0,
                        &mut joins.values,
                        &keys.values,
                        None,
                        &fact,
                        &dimensions.values,
                        aggregate,
                        &dtype,
                        context,
                        batch_rows,
                        &mut work,
                    )
                } else {
                    work.merge(NativeJoinAggregateWork {
                        source_batches_declined: 1,
                        ..Default::default()
                    })?;
                    self.ordinary(
                        self.input,
                        &mut joins.values,
                        array,
                        context,
                        batch_rows,
                        &mut |array| {
                            if count_partial {
                                aggregate.consume_reordered_count(&array, context, batch_rows)
                            } else {
                                aggregate.consume(&array, context, batch_rows)
                            }
                        },
                    )
                }
            },
        )?;
        for join in &joins.values {
            context.record_native_join_work(&join.join_work()?)?;
        }
        context.record_native_join_work(&NativeJoinWork {
            direct_aggregate: work,
            ..Default::default()
        })
    }

    fn build_joins(
        &self,
        prepared: &PreparedVortexRelational,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
    ) -> Result<ReservedVec<Join<'a>>> {
        // Outer build before inner build, exactly as recursive ordinary joins.
        let mut joins = ReservedVec::new(context.memory())?;
        joins.reserve(self.stages.values.len())?;
        for stage in self.stages.values.iter().rev() {
            let mut join = Join::new(stage.spec, context.memory())?;
            prepared.run(
                stage.right,
                context,
                metrics,
                batch_rows,
                None,
                &mut |array| {
                    join.build(
                        metrics.detach_join_build(array, context)?,
                        context,
                        batch_rows,
                    )
                },
            )?;
            join.finish_build(context)?;
            joins.values.push(join);
        }
        joins.values.reverse();
        Ok(joins)
    }

    fn probe_keys(
        &self,
        array: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ReservedVec<Batch>> {
        let mut keys = ReservedVec::new(context.memory())?;
        keys.reserve(self.stages.values.len())?;
        for (index, stage) in self.stages.values.iter().enumerate() {
            if index == 0 {
                // The first producer's complete key preparation is owed before
                // its first match. Later keys crossed the preceding validation.
                let names = stage
                    .probe
                    .values
                    .iter()
                    .map(|origin| self.fact.fields[origin.column].0.as_str());
                keys.values
                    .push(Batch::from_key_names(array.clone(), names, context)?);
                continue;
            }
            let mut columns = ReservedVec::new(context.memory())?;
            columns.reserve(stage.probe.values.len())?;
            for origin in &stage.probe.values {
                let column =
                    logical_field_from_native_array(array, &self.fact.fields[origin.column].0)?;
                columns.values.push(
                    KeyColumn::try_borrow(&column, context)?
                        .ok_or_else(|| failed("admitted probe representation changed"))?,
                );
            }
            keys.values
                .push(Batch::from_prepared_keys(array.clone(), columns));
        }
        Ok(keys)
    }

    #[allow(clippy::too_many_arguments)] // Borrowed source owners and coordinates live only for this join flush.
    fn matches(
        &self,
        index: usize,
        joins: &mut [Join<'_>],
        keys: &[Batch],
        parent: Option<&Frame>,
        fact: &Source,
        dimensions: &[Source],
        aggregate: &mut Aggregate<'_>,
        dtype: &DType,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        work: &mut NativeJoinAggregateWork,
    ) -> Result<()> {
        let (join, tail) = joins
            .split_first_mut()
            .ok_or_else(|| failed("direct join stage is absent"))?;
        let selected = parent.map(|frame| frame.source_rows(0)).transpose()?;
        join.match_unique(
            &keys[index],
            selected,
            context,
            batch_rows,
            &mut |parents, right| {
                let frame = Frame::extend(parent, parents, right, context)?;
                let input = Input {
                    fact,
                    dimensions,
                    frame: &frame,
                    groups: &self.groups.values,
                };
                input.validate(&self.stages.values[index].validation.values, context)?;
                let rows = u64::try_from(frame.len())
                    .map_err(|_| failed("direct join row count overflow"))?;
                let columns = u64::try_from(self.stages.values[index].validation.values.len())
                    .map_err(|_| failed("direct join column count overflow"))?;
                work.merge(NativeJoinAggregateWork {
                    match_batches: 1,
                    match_rows: rows,
                    validated_cells: rows
                        .checked_mul(columns)
                        .ok_or_else(|| failed("direct join validation count overflow"))?,
                    aggregate_rows: if tail.is_empty() { rows } else { 0 },
                    ..Default::default()
                })?;
                if tail.is_empty() {
                    aggregate.consume_matches(&input, self.measure, dtype, context, batch_rows)
                } else {
                    self.matches(
                        index + 1,
                        tail,
                        keys,
                        Some(&frame),
                        fact,
                        dimensions,
                        aggregate,
                        dtype,
                        context,
                        batch_rows,
                        work,
                    )
                }
            },
        )
    }

    fn ordinary(
        &self,
        node: &Node,
        joins: &mut [Join<'_>],
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        if std::ptr::eq(node, self.fact) {
            return consume(array);
        }
        match &node.kind {
            NodeKind::Join { left, .. } => {
                let index = self
                    .stages
                    .values
                    .iter()
                    .position(|stage| std::ptr::eq(stage.node, node))
                    .ok_or_else(|| failed("prepared join stage is absent"))?;
                let (before, rest) = joins.split_at_mut(index);
                self.ordinary(left, before, array, context, batch_rows, &mut |array| {
                    rest[0]
                        .consume(array, context, batch_rows, consume)
                        .map(|_| ())
                })
            }
            NodeKind::Project { input, expressions } => {
                self.ordinary(input, joins, array, context, batch_rows, &mut |array| {
                    consume(transform::project_array(
                        &node.fields,
                        expressions,
                        &array,
                        context,
                    )?)
                })
            }
            NodeKind::Pipeline { input, stages } => self.ordinary(
                input,
                joins,
                array,
                context,
                batch_rows,
                &mut |mut array| {
                    for stage in stages {
                        let PipelineKind::Project(expressions) = &stage.kind else {
                            return Err(failed("direct join projection contract changed"));
                        };
                        array =
                            transform::project_array(&stage.fields, expressions, &array, context)?;
                    }
                    consume(array)
                },
            ),
            _ => Err(failed("direct join source boundary changed")),
        }
    }
}

type Binding<'a> = Option<(&'a Node, ReservedVec<Origin>)>;

fn bind<'a>(
    node: &'a Node,
    stages: &mut ReservedVec<Stage<'a>>,
    context: &NativeExecutionContext<'_>,
) -> Result<Binding<'a>> {
    match &node.kind {
        NodeKind::Join { left, right, spec } if plain_inner(spec) => {
            bind_join(node, left, right, spec, stages, context)
        }
        NodeKind::Project { input, expressions } if transform::column_projection(expressions) => {
            let Some((fact, origins)) = bind(input, stages, context)? else {
                return Ok(None);
            };
            Ok(
                project_origins(&input.fields, expressions, &origins.values, context)?
                    .map(|origins| (fact, origins)),
            )
        }
        NodeKind::Pipeline {
            input,
            stages: pipeline,
        } if plain_projections(pipeline) => {
            let Some((fact, mut origins)) = bind(input, stages, context)? else {
                return Ok(None);
            };
            let mut fields = &input.fields;
            for stage in pipeline {
                let PipelineKind::Project(expressions) = &stage.kind else {
                    unreachable!();
                };
                let Some(next) = project_origins(fields, expressions, &origins.values, context)?
                else {
                    return Ok(None);
                };
                origins = next;
                fields = &stage.fields;
            }
            Ok(Some((fact, origins)))
        }
        _ => {
            let mut origins = ReservedVec::new(context.memory())?;
            origins.reserve(node.fields.len())?;
            origins
                .values
                .extend((0..node.fields.len()).map(|column| Origin { source: 0, column }));
            Ok(Some((node, origins)))
        }
    }
}

fn plain_inner(spec: &Spec) -> bool {
    spec.kind == Kind::Inner
        && spec.asof.is_none()
        && spec.condition.is_none()
        && !spec.left_keys.is_empty()
}

fn plain_projections(stages: &[super::PipelineStage]) -> bool {
    stages.iter().all(|stage| {
        matches!(&stage.kind, PipelineKind::Project(expressions)
            if transform::column_projection(expressions))
    })
}

fn bind_join<'a>(
    node: &'a Node,
    left: &'a Node,
    right: &'a Node,
    spec: &'a Spec,
    stages: &mut ReservedVec<Stage<'a>>,
    context: &NativeExecutionContext<'_>,
) -> Result<Binding<'a>> {
    let Some((fact, origins)) = bind(left, stages, context)? else {
        return Ok(None);
    };
    let mut probe = ReservedVec::new(context.memory())?;
    probe.reserve(spec.left_keys.len())?;
    for key in &spec.left_keys {
        let Some(index) = position(left, key) else {
            return Ok(None);
        };
        let origin = origins.values[index];
        if origin.source != 0 {
            return Ok(None);
        }
        probe.values.push(origin);
    }
    let source = stages.values.len() + 1;
    let mut validation = ReservedVec::new(context.memory())?;
    let mut output = ReservedVec::new(context.memory())?;
    validation.reserve(spec.columns.len())?;
    output.reserve(node.fields.len())?;
    for (index, (side, name)) in spec.columns.iter().enumerate() {
        let origin = match side {
            Side::Left => {
                let Some(index) = position(left, name) else {
                    return Ok(None);
                };
                origins.values[index]
            }
            Side::Right => {
                let Some(column) = position(right, name) else {
                    return Ok(None);
                };
                Origin { source, column }
            }
        };
        validation.values.push(origin);
        if spec.retains(index) {
            output.values.push(origin);
        }
    }
    if output.values.len() != node.fields.len() {
        return Ok(None);
    }
    stages.push(Stage {
        node,
        right,
        spec,
        probe,
        validation,
    })?;
    Ok(Some((fact, output)))
}

fn project_origins(
    fields: &[(String, DType)],
    expressions: &[crate::local_primitives::native_relational_expression::Expression],
    origins: &[Origin],
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ReservedVec<Origin>>> {
    let mut projected = ReservedVec::new(context.memory())?;
    projected.reserve(expressions.len())?;
    for expression in expressions {
        let ExpressionKind::Column(name) = &expression.kind else {
            return Ok(None);
        };
        let Some(index) = fields.iter().position(|(field, _)| field == name) else {
            return Ok(None);
        };
        projected.values.push(origins[index]);
    }
    Ok(Some(projected))
}
