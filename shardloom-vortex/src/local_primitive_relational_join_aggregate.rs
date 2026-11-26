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
use std::ops::Range;
use vortex::array::{
    ArrayRef,
    arrays::{Chunked, Slice, chunked::ChunkedArrayExt as _, slice::SliceArraySlotsExt as _},
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
    measures: ReservedVec<Option<Origin>>,
}

fn position(node: &Node, name: &str) -> Option<usize> {
    node.fields.iter().position(|(field, _)| field == name)
}

fn fact_chunk_subranges(
    array: &ArrayRef,
    fields: &[(String, DType)],
) -> Result<Option<Vec<Range<usize>>>> {
    if array.len() <= 1 || fields.is_empty() {
        return Ok(None);
    }
    let mut cuts = Vec::new();
    for (name, _) in fields {
        let mut leaf = logical_field_from_native_array(array, name)?;
        let mut range = 0..array.len();
        for _ in 0..16 {
            if range.end > leaf.len() {
                return Ok(None);
            }
            if let Some(slice) = leaf.as_opt::<Slice>() {
                let Some(start) = range.start.checked_add(slice.slice_range().start) else {
                    return Ok(None);
                };
                let Some(end) = range.end.checked_add(slice.slice_range().start) else {
                    return Ok(None);
                };
                range = start..end;
                leaf = slice.child().clone();
            } else if let Some(chunks) = leaf.as_opt::<Chunked>() {
                if chunks.nchunks() > 4096 {
                    return Ok(None);
                }
                let mut offset = 0usize;
                for idx in 0..chunks.nchunks() {
                    let Some(next) = offset.checked_add(chunks.chunk(idx).len()) else {
                        return Ok(None);
                    };
                    if next > range.start && next < range.end {
                        cuts.push(next - range.start);
                    }
                    offset = next;
                }
                break;
            } else {
                break;
            }
        }
    }
    if cuts.is_empty() {
        return Ok(None);
    }
    cuts.sort_unstable();
    cuts.dedup();
    if cuts.len() > 4096 {
        return Ok(None);
    }
    let mut ranges = Vec::with_capacity(cuts.len() + 1);
    let mut prev = 0usize;
    for cut in cuts {
        if cut > prev {
            ranges.push(prev..cut);
            prev = cut;
        }
    }
    if prev < array.len() {
        ranges.push(prev..array.len());
    }
    Ok((ranges.len() > 1).then_some(ranges))
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
        let mut measures = ReservedVec::new(context.memory())?;
        measures.reserve(aggregate.measures.len())?;
        for measure in &aggregate.measures {
            let origin = if let Some(name) = &measure.column {
                let Some(index) = position(input, name) else {
                    return Ok(None);
                };
                let origin = origins.values[index];
                let dtype = if origin.source == 0 {
                    fact.fields.get(origin.column).map(|(_, dtype)| dtype)
                } else {
                    stages
                        .values
                        .get(origin.source - 1)
                        .and_then(|stage| stage.right.fields.get(origin.column))
                        .map(|(_, dtype)| dtype)
                };
                if !matches!(dtype, Some(DType::Primitive(ptype, _)) if *ptype != PType::F16) {
                    return Ok(None);
                }
                Some(origin)
            } else {
                None
            };
            measures.values.push(origin);
        }
        Ok(Some(Self {
            input,
            fact,
            stages,
            groups,
            measures,
        }))
    }

    fn stage_validation_infallible(&self, stage: &Stage<'_>) -> bool {
        stage.validation.values.iter().all(|origin| {
            let dtype = if origin.source == 0 {
                self.fact.fields.get(origin.column).map(|(_, dtype)| dtype)
            } else {
                self.stages
                    .values
                    .get(origin.source - 1)
                    .and_then(|dim| dim.right.fields.get(origin.column))
                    .map(|(_, dtype)| dtype)
            };
            dtype.is_some_and(super::super::native_relational_probe_filter::is_infallible_key_dtype)
        })
    }

    #[allow(clippy::too_many_lines)]
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
        if direct {
            for origin in self.measures.values.iter().flatten() {
                if origin.source > 0
                    && dimensions.values[origin.source - 1]
                        .column(origin.column)?
                        .single()
                        .is_none()
                {
                    direct = false;
                    work.chains_declined_representation = 1;
                    break;
                }
            }
        }
        if !direct {
            dimensions.values.clear();
        }
        let mut fact_probe_filters = Vec::new();
        let target = super::fact_probe_filter_target(&self.fact.kind);
        if target != super::FactProbeFilterTarget::UnsupportedOrMemory {
            for (stage_idx, (join, stage)) in
                joins.values.iter().zip(&self.stages.values).enumerate()
            {
                if stage_idx > 0
                    && !self.stages.values[..stage_idx]
                        .iter()
                        .all(|earlier| self.stage_validation_infallible(earlier))
                {
                    break;
                }
                for filter in join.probe_runtime_filters(context)? {
                    let remapped = filter.remap(|left_name| {
                        let key_pos = stage
                            .spec
                            .left_keys
                            .iter()
                            .position(|key| key == left_name)?;
                        let origin = stage.probe.values.get(key_pos)?;
                        if origin.source != 0 {
                            return None;
                        }
                        let (fact_name, fact_dtype) = self.fact.fields.get(origin.column)?;
                        let stage_dtype = stage
                            .node
                            .fields
                            .iter()
                            .find_map(|(name, dtype)| (name == left_name).then_some(dtype))
                            .or(Some(fact_dtype))?;
                        (fact_dtype == stage_dtype).then(|| fact_name.clone())
                    });
                    if let Some(mut mapped) = remapped {
                        if direct && target == super::FactProbeFilterTarget::FileOrRetained {
                            mapped.bloom = None;
                        }
                        fact_probe_filters.push(mapped);
                    }
                }
            }
        }
        work.probe_filters_pushed = u64::try_from(fact_probe_filters.len()).unwrap_or(u64::MAX);
        let dtype = DType::struct_(self.input.fields.clone(), Nullability::NonNullable);
        prepared.run_with_probe_filters(
            self.fact,
            context,
            metrics,
            batch_rows,
            None,
            &fact_probe_filters,
            &mut |array| {
                let consume_with_fact = |batch: ArrayRef,
                                         fact: Option<Source>,
                                         work: &mut NativeJoinAggregateWork,
                                         joins: &mut [Join<'_>],
                                         aggregate: &mut Aggregate<'_>|
                 -> Result<()> {
                    if let Some(fact) = fact {
                        work.merge(NativeJoinAggregateWork {
                            source_batches_direct: 1,
                            ..Default::default()
                        })?;
                        let keys = self.probe_keys(&batch, context)?;
                        self.matches(
                            0,
                            joins,
                            &keys.values,
                            None,
                            &fact,
                            &dimensions.values,
                            aggregate,
                            &dtype,
                            context,
                            batch_rows,
                            work,
                        )
                    } else {
                        work.merge(NativeJoinAggregateWork {
                            source_batches_declined: 1,
                            ..Default::default()
                        })?;
                        self.ordinary(
                            self.input,
                            joins,
                            batch,
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
                };
                let fact = if direct {
                    Source::try_new(&self.fact.fields, &std::iter::once(&array), context)?
                } else {
                    None
                };
                if direct
                    && fact.is_none()
                    && let Some(ranges) = fact_chunk_subranges(&array, &self.fact.fields)?
                {
                    for sub in ranges {
                        context.check_cancelled()?;
                        let sliced = array.slice(sub).map_err(super::vortex_error)?;
                        let sub_batch =
                            super::project_selected_batch(&sliced, &self.fact.fields, context)?;
                        let sub_fact = Source::try_new(
                            &self.fact.fields,
                            &std::iter::once(&sub_batch),
                            context,
                        )?;
                        consume_with_fact(
                            sub_batch,
                            sub_fact,
                            &mut work,
                            &mut joins.values,
                            aggregate,
                        )?;
                    }
                    return Ok(());
                }
                consume_with_fact(array, fact, &mut work, &mut joins.values, aggregate)
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
                    aggregate.consume_matches(
                        &input,
                        &self.measures.values,
                        dtype,
                        context,
                        batch_rows,
                    )
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
