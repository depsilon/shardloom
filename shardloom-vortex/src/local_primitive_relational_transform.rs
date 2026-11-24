//! Streaming expression/projection/filter/range and retained native ordering.

use super::super::native_relational_keys::Cell as KeyCell;
use super::{
    ArrayRef, FieldNames, LocalSelection, Metrics, NativeExecutionContext, Node, NodeKind,
    PipelineKind, PipelineStage, PreparedVortexRelational, ReservedVec, Result, StructArray,
    Validity, add, failed, native_relational_expression, native_relational_sort, vortex_error,
};

#[cfg(all(test, debug_assertions, feature = "vortex-write"))]
#[path = "local_primitive_relational_survivor_admission_tests.rs"]
mod survivor_admission_tests;

#[cfg(all(test, debug_assertions, feature = "vortex-write"))]
#[path = "local_primitive_relational_fsst_admission_tests.rs"]
mod fsst_admission_tests;

pub(super) fn column_projection(expressions: &[native_relational_expression::Expression]) -> bool {
    expressions.iter().all(|expression| {
        matches!(
            expression.kind,
            native_relational_expression::Kind::Column(_)
        )
    })
}

fn remap_probe_filters(
    fields: &[(String, super::DType)],
    expressions: &[native_relational_expression::Expression],
    probe_filters: &[super::super::native_relational_probe_filter::ProbeRuntimeFilter],
) -> Vec<super::super::native_relational_probe_filter::ProbeRuntimeFilter> {
    if !column_projection(expressions) {
        return Vec::new();
    }
    probe_filters
        .iter()
        .filter_map(|filter| {
            filter.remap(|name| {
                fields
                    .iter()
                    .zip(expressions)
                    .find_map(|((out_name, _), expr)| {
                        if out_name == name
                            && let native_relational_expression::Kind::Column(source) = &expr.kind
                        {
                            Some(source.clone())
                        } else {
                            None
                        }
                    })
            })
        })
        .collect()
}

fn pipeline_probe_filters(
    stages: &[PipelineStage],
    probe_filters: &[super::super::native_relational_probe_filter::ProbeRuntimeFilter],
) -> Vec<super::super::native_relational_probe_filter::ProbeRuntimeFilter> {
    let mut current = probe_filters.to_vec();
    for stage in stages.iter().rev() {
        if current.is_empty() {
            break;
        }
        match &stage.kind {
            PipelineKind::Project(expressions) => {
                current = remap_probe_filters(&stage.fields, expressions, &current);
            }
            PipelineKind::Filter(predicate) if predicate.is_infallible_for_join_pushdown(true) => {}
            _ => {
                current.clear();
                break;
            }
        }
    }
    current
}

struct PipelineState<'a> {
    batch_rows: usize,
    ranges: ReservedVec<(usize, usize)>,
    observations: ReservedVec<(usize, super::native_relational_aggregate::Aggregate<'a>)>,
}

fn prepare_pipeline_state<'a>(
    stages: &'a [PipelineStage],
    metrics: &Metrics<'_>,
    batch_rows: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<PipelineState<'a>> {
    let mut ranges = ReservedVec::new(context.memory())?;
    let batch_rows = stages
        .iter()
        .fold(batch_rows, |rows, stage| match &stage.kind {
            PipelineKind::ApplicationBatch(spec) => rows.min(spec.max_batch_rows()),
            _ => rows,
        });
    ranges.reserve(stages.len())?;
    let mut observations = ReservedVec::new(context.memory())?;
    for (index, stage) in stages.iter().enumerate() {
        if let PipelineKind::ApplicationBatch(spec) = &stage.kind {
            spec.observation(metrics, context)?;
        }
        ranges.values.push(match stage.kind {
            PipelineKind::Limit { offset, count } => (offset, count),
            _ => (0, 0),
        });
        if let PipelineKind::Observe(observation) = &stage.kind {
            observations.push((
                index,
                super::native_relational_aggregate::Aggregate::new(
                    &observation.spec,
                    context.memory(),
                )?,
            ))?;
        }
    }
    Ok(PipelineState {
        batch_rows,
        ranges,
        observations,
    })
}

impl Node {
    /// Direct field selections preserve the producer's value-validation scope.
    pub(super) fn before_column_projections(&self) -> &Self {
        let mut node = self;
        loop {
            node = match node.semantic_kind() {
                NodeKind::Project { input, expressions } if column_projection(expressions) => input,
                NodeKind::Pipeline { input, stages } if stages.iter().all(|stage| {
                    matches!(&stage.kind, PipelineKind::Project(expressions) if column_projection(expressions))
                }) => input,
                _ => return node,
            };
        }
    }
}

impl PreparedVortexRelational {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn run_transform(
        &self,
        kind: &NodeKind,
        fields: &[(String, super::DType)],
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        parameter: Option<&ArrayRef>,
        probe_filters: &[super::super::native_relational_probe_filter::ProbeRuntimeFilter],
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        match kind {
            NodeKind::Project { input, expressions } => {
                let child_filters = if !probe_filters.is_empty() && column_projection(expressions) {
                    remap_probe_filters(fields, expressions, probe_filters)
                } else {
                    Vec::new()
                };
                self.run_with_probe_filters(
                    input,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &child_filters,
                    &mut |array| consume(project_array(fields, expressions, &array, context)?),
                )
            }
            NodeKind::Filter { input, predicate } => {
                let recipe = predicate.filter_recipe();
                let child_filters = if !probe_filters.is_empty()
                    && predicate.is_infallible_for_join_pushdown(true)
                {
                    probe_filters
                } else {
                    &[]
                };
                self.run_with_probe_filters(
                    input,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    child_filters,
                    &mut |array| {
                        let rows = filter_rows(predicate, recipe, &array, context)?;
                        rows.for_each_batch(&array, fields, array.len().max(1), context, consume)
                    },
                )
            }
            NodeKind::Sort { .. } => {
                self.run_sort(kind, context, metrics, batch_rows, parameter, consume)
            }
            NodeKind::Limit {
                input,
                offset,
                count,
            } => {
                // Streaming sources, observations and retained relations still
                // owe completion when no output rows are requested.
                if *count == 0 && metrics.input.is_none() && !input.requires_completion() {
                    return Ok(());
                }
                let mut skip = *offset;
                let mut remaining = *count;
                self.run(
                    input,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &mut |array| {
                        let start = skip.min(array.len());
                        skip -= start;
                        let count = remaining.min(array.len() - start);
                        remaining -= count;
                        if count == 0 {
                            return Ok(());
                        }
                        consume(array.slice(start..start + count).map_err(vortex_error)?)
                    },
                )
            }
            _ => Err(failed("transform dispatch received another operator")),
        }
    }

    #[allow(clippy::too_many_arguments)] // Same shared runtime/callback contract as run_transform.
    pub(super) fn run_pipeline(
        &self,
        input: &Node,
        stages: &[PipelineStage],
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        parameter: Option<&ArrayRef>,
        probe_filters: &[super::super::native_relational_probe_filter::ProbeRuntimeFilter],
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        if metrics.input.is_none()
            && !input.requires_completion()
            && !stages.iter().any(|stage| stage.kind.requires_completion())
            && stages
                .iter()
                .any(|stage| matches!(stage.kind, PipelineKind::Limit { count: 0, .. }))
        {
            return Ok(());
        }
        let child_filters = pipeline_probe_filters(stages, probe_filters);
        let PipelineState {
            batch_rows,
            mut ranges,
            mut observations,
        } = prepare_pipeline_state(stages, metrics, batch_rows, context)?;
        self.run_with_probe_filters(
            input,
            context,
            metrics,
            batch_rows,
            parameter,
            &child_filters,
            &mut |mut array| {
                let mut observation_index = 0;
                for (index, stage) in stages.iter().enumerate() {
                    context.check_cancelled()?;
                    array = match &stage.kind {
                        PipelineKind::ApplicationBatch(spec) => {
                            spec.execute(&array, &self.session, context, metrics)?
                        }
                        PipelineKind::Project(expressions) => {
                            project_array(&stage.fields, expressions, &array, context)?
                        }
                        PipelineKind::Filter(predicate) => {
                            let Some(selected) =
                                filter_array(predicate, &stage.fields, &array, context)?
                            else {
                                return Ok(());
                            };
                            selected
                        }
                        PipelineKind::Limit { .. } => {
                            let (skip, remaining) = &mut ranges.values[index];
                            let start = (*skip).min(array.len());
                            *skip -= start;
                            let count = (*remaining).min(array.len() - start);
                            *remaining -= count;
                            if count == 0 {
                                return Ok(());
                            }
                            array.slice(start..start + count).map_err(vortex_error)?
                        }
                        PipelineKind::Observe(observation) => {
                            let projected = observation
                                .projection
                                .as_ref()
                                .map(|(fields, expressions)| {
                                    project_array(fields, expressions, &array, context)
                                })
                                .transpose()?;
                            observations.values[observation_index].1.consume(
                                projected.as_ref().unwrap_or(&array),
                                context,
                                batch_rows,
                            )?;
                            observation_index += 1;
                            array
                        }
                    };
                }
                consume(array)
            },
        )?;
        for (index, aggregate) in &observations.values {
            if let PipelineKind::Observe(observation) = &stages[*index].kind {
                metrics.complete_observation(observation, aggregate, context)?;
            }
        }
        Ok(())
    }

    fn run_sort(
        &self,
        kind: &NodeKind,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        parameter: Option<&ArrayRef>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let NodeKind::Sort {
            input,
            spec,
            output_prefix,
        } = kind
        else {
            return Err(failed("sort dispatch received another operator"));
        };
        #[cfg(feature = "vortex-write")]
        if let Some(spill) = &metrics.spill {
            let mut sort = super::super::native_relational_spill::Ordering::new(
                spec, spill, batch_rows, context,
            )?;
            self.run(
                input,
                context,
                metrics,
                batch_rows,
                parameter,
                &mut |array| {
                    sort.build(
                        metrics.detach_ordering_input(array, &spec.unobserved_fields, context)?,
                        context,
                    )
                },
            )?;
            return sort.finish(context, batch_rows, consume);
        }
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        if spec.unobserved_fields.is_empty()
            && self.run_ranked_sort(kind, context, metrics, batch_rows, parameter, consume)?
        {
            return Ok(());
        }
        if output_prefix.is_some()
            && (metrics.input.is_some() || prepared_prefix_input(input, spec))
            && let Some(mut sort) =
                native_relational_sort::StreamingPrefix::try_new(spec, *output_prefix, context)?
        {
            self.run(
                input,
                context,
                metrics,
                batch_rows,
                parameter,
                &mut |array| {
                    let rows = array.len() as u64;
                    let copied = sort.build(array, context)?;
                    if copied.rows != 0 || copied.buffer_bytes != 0 {
                        metrics.record_ordering_copy(copied.rows, copied.buffer_bytes)?;
                    }
                    add(&metrics.ordering_prefix_batches, 1)?;
                    add(&metrics.ordering_prefix_rows_validated, rows)?;
                    metrics.ordering_prefix_peak_retained_buffer_bytes.set(
                        metrics
                            .ordering_prefix_peak_retained_buffer_bytes
                            .get()
                            .max(copied.retained_buffer_bytes),
                    );
                    Ok(())
                },
            )?;
            return sort.finish(context, batch_rows, consume);
        }
        let mut sort = native_relational_sort::Sort::new(spec, context.memory())?;
        self.run(
            input,
            context,
            metrics,
            batch_rows,
            parameter,
            &mut |array| {
                sort.build(
                    metrics.detach_ordering_input(array, &spec.unobserved_fields, context)?,
                    context,
                )
            },
        )?;
        sort.finish_prefix(
            context,
            batch_rows,
            *output_prefix,
            metrics.input.is_some(),
            consume,
        )
    }
}

fn prepared_prefix_input(input: &Node, spec: &native_relational_sort::Spec) -> bool {
    match input.before_column_projections().semantic_kind() {
        // These non-spilling owners validate every emitted payload before the
        // consumer sees it. Aggregate keys and join output share native gather;
        // computed aggregate/window values use the checked result builders.
        NodeKind::Window { .. } | NodeKind::Aggregate { .. } | NodeKind::Join { .. } => true,
        // There is no payload-only field here. The original sort already
        // requires every integer key from every row. StreamingPrefix checks
        // the complete record and takes independent ownership before pruning.
        NodeKind::Scan {
            source: super::ScanSource::File(_),
            ..
        } => spec.fields.iter().all(|(name, dtype)| {
            matches!(dtype, vortex::array::dtype::DType::Primitive(ptype, _) if ptype.is_int())
                && spec.names.contains(name)
        }),
        _ => false,
    }
}

fn filter_array(
    predicate: &native_relational_expression::Expression,
    fields: &[(String, super::DType)],
    array: &ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ArrayRef>> {
    let rows = filter_rows(predicate, predicate.filter_recipe(), array, context)?;
    let mut selected = None;
    // Selection cannot produce more rows than this input. Use one native
    // selection batch, preserving its ownership.
    rows.for_each_batch(array, fields, array.len().max(1), context, &mut |batch| {
        if selected.replace(batch).is_some() {
            return Err(failed("streaming selection exceeded its input batch"));
        }
        Ok(())
    })?;
    Ok(selected)
}

pub(super) fn project_array(
    fields: &[(String, super::DType)],
    expressions: &[native_relational_expression::Expression],
    array: &ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let mut columns = ReservedVec::new(context.memory())?;
    columns.reserve(expressions.len())?;
    for expression in expressions {
        columns
            .values
            .push(expression.evaluate_reusing(array, context, &columns.values)?);
    }
    let (columns, _ownership) = columns.into_parts();
    let output = StructArray::try_new(
        fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<FieldNames>(),
        columns,
        array.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    Ok(vortex::array::IntoArray::into_array(output))
}

fn filter_rows(
    predicate: &native_relational_expression::Expression,
    recipe: Option<&native_relational_expression::FilterRecipe>,
    array: &ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<LocalSelection> {
    if let Some(recipe) = recipe {
        let mask = recipe.evaluate_mask(array, context)?;
        return LocalSelection::from_mask(&mask, context);
    }
    // Keep the existing evaluation and ownership of unadmitted expressions,
    // including lazy/fallible branches and nullable Boolean projections.
    let predicate =
        native_relational_expression::keys(&predicate.evaluate(array, context)?, context)?;
    let mut rows = LocalSelection::new(array.len(), context)?;
    for row in 0..array.len() {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if predicate.cell(row)? == KeyCell::Boolean(true) {
            rows.push(row)?;
        }
    }
    Ok(rows)
}
