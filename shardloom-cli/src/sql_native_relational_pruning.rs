//! Backward column demand after SQL name resolution and before native binding.
//! Explicit expressions, grouping keys and set identity keep their full semantics.

use super::{
    AsOfJoin, BTreeSet, ColumnRef, Expression, Join, Lowerer, NativeResult, NativeWindowFunction,
    Plan, ProjectionRequest, Side, Subquery, SubqueryKind, Window, expression_source_columns,
    unsupported_sql_error,
};
use shardloom_vortex::relational_query::VortexRelationalStage as Stage;

type Columns = BTreeSet<String>;

impl Lowerer<'_, '_> {
    pub(super) fn prune(
        &mut self,
        plan: &mut Plan,
        required: Option<Columns>,
        outer: &mut Columns,
    ) -> NativeResult<()> {
        self.prune_at(plan, required, outer, 0)
    }

    #[allow(clippy::too_many_lines)] // Exhaustive demand transfer for the native operator variants.
    fn prune_at(
        &mut self,
        plan: &mut Plan,
        mut required: Option<Columns>,
        outer: &mut Columns,
        depth: usize,
    ) -> NativeResult<()> {
        if depth > 24 {
            return Err(unsupported_sql_error(
                "native relational plan exceeds 24 levels",
            ));
        }
        match plan {
            Plan::Input(_) => Err(unsupported_sql_error(
                "native declaration input must be bound before SQL pruning",
            )),
            Plan::ExecutionResult(_)
            | Plan::DeferredSubquery(_)
            | Plan::Materialized(_)
            | Plan::Recursive(_)
            | Plan::RecursiveInput(_) => Ok(()),
            Plan::Scan(scan) => {
                if let Some(mut required) = required {
                    // Native batches retain a row-bearing field even for COUNT(*)
                    // or literal-only EXISTS; no input row count is fabricated.
                    if required.is_empty() {
                        let columns = self.schemas.source_columns(&scan.source_uri)?;
                        required.insert(columns.first().cloned().ok_or_else(|| {
                            unsupported_sql_error("native SQL source has no row-bearing field")
                        })?);
                    }
                    if matches!(&scan.projection, ProjectionRequest::All) {
                        let columns = self.schemas.source_columns(&scan.source_uri)?;
                        if required.len() == columns.len()
                            && columns.iter().all(|column| required.contains(column))
                        {
                            // No column is removed. Preserve the source's field
                            // order so an identity SELECT stays identical when
                            // the native binder validates and normalizes it.
                            return Ok(());
                        }
                    }
                    scan.projection = ProjectionRequest::Columns(
                        required
                            .into_iter()
                            .map(ColumnRef::new)
                            .collect::<NativeResult<_>>()?,
                    );
                }
                Ok(())
            }
            Plan::Project(project) => {
                // Do not remove an explicitly written expression: all its names,
                // types and supported operations still require native admission.
                let needed = project
                    .expressions
                    .iter()
                    .flat_map(|(_, expression)| expression_source_columns(expression))
                    .collect();
                self.prune_at(&mut project.input, Some(needed), outer, depth + 1)
            }
            Plan::Filter(filter) => {
                add_expression(&mut required, &filter.predicate);
                self.prune_at(&mut filter.input, required, outer, depth + 1)
            }
            Plan::Sort(sort) => {
                if let Some(required) = &mut required {
                    required.extend(sort.keys.iter().map(|key| key.column.as_str().to_owned()));
                }
                self.prune_at(&mut sort.input, required, outer, depth + 1)
            }
            Plan::Limit(limit) => self.prune_at(&mut limit.input, required, outer, depth + 1),
            Plan::Pipeline(pipeline) => {
                for stage in pipeline.stages.iter().rev() {
                    self.cancellation.check()?;
                    pipeline_columns(stage, &mut required);
                }
                self.prune_at(&mut pipeline.input, required, outer, depth + 1)
            }
            Plan::Unary(unary) => {
                // Deduplication identity, rewrite dependencies and weight/reshape
                // inputs remain complete even when a later SELECT drops fields.
                self.prune_at(&mut unary.input, None, outer, depth + 1)
            }
            Plan::GapFill(fill) => {
                // Gap filling expands rows; keep complete input rows and leave
                // predicates, projections, and limits above the expansion.
                self.prune_at(&mut fill.input, None, outer, depth + 1)
            }
            Plan::Aggregate(aggregate) => {
                let needed =
                    aggregate
                        .group_by
                        .iter()
                        .chain(aggregate.measures.iter().flat_map(
                            shardloom_vortex::VortexSimpleAggregateMeasure::input_columns,
                        ))
                        .map(|column| column.as_str().to_owned())
                        .collect();
                self.prune_at(&mut aggregate.input, Some(needed), outer, depth + 1)
            }
            Plan::Join(join) => self.prune_join(join, required.as_ref(), outer, depth),
            Plan::AsOfJoin(join) => self.prune_asof(join, required.as_ref(), outer, depth),
            Plan::Set(set) => {
                // DISTINCT/INTERSECT/EXCEPT compare complete rows; pruning a set
                // key because a later projection drops it would change results.
                self.prune_at(&mut set.left, None, outer, depth + 1)?;
                self.prune_at(&mut set.right, None, outer, depth + 1)
            }
            Plan::Window(window) => self.prune_window(window, required.as_ref(), outer, depth),
            Plan::Subquery(query) => self.prune_subquery(query, required, outer, false, depth),
            Plan::CorrelatedSubquery(query) => {
                self.prune_subquery(query, required, outer, true, depth)
            }
            Plan::Outer => {
                outer.extend(required.ok_or_else(|| {
                    unsupported_sql_error(
                        "native outer-row projection requires explicit column demand",
                    )
                })?);
                Ok(())
            }
        }
    }

    fn prune_window(
        &mut self,
        window: &mut Window,
        required: Option<&Columns>,
        outer: &mut Columns,
        depth: usize,
    ) -> NativeResult<()> {
        if let Some(required) = required {
            window
                .columns
                .retain(|column| required.contains(column.as_str()));
        }
        let mut needed = window
            .columns
            .iter()
            .map(|column| column.as_str().to_owned())
            .collect::<Columns>();
        for expression in &window.expressions {
            needed.extend(
                expression
                    .partition_by
                    .iter()
                    .map(|column| column.as_str().to_owned()),
            );
            needed.extend(
                expression
                    .order_by
                    .iter()
                    .map(|key| key.column.as_str().to_owned()),
            );
            if let NativeWindowFunction::Lag { column, .. }
            | NativeWindowFunction::Lead { column, .. }
            | NativeWindowFunction::Fill { column, .. } = &expression.function
            {
                needed.insert(column.as_str().to_owned());
            }
            if let NativeWindowFunction::Framed(function) = &expression.function {
                needed.extend(
                    function
                        .input_columns()
                        .map(|column| column.as_str().to_owned()),
                );
            }
        }
        self.prune_at(&mut window.input, Some(needed), outer, depth + 1)
    }

    fn prune_asof(
        &mut self,
        join: &mut AsOfJoin,
        required: Option<&Columns>,
        outer: &mut Columns,
        depth: usize,
    ) -> NativeResult<()> {
        if let Some(required) = required {
            let row_field = join.columns.first().cloned();
            join.columns
                .retain(|column| required.contains(&column.output_column));
            if join.columns.is_empty() {
                join.columns.extend(row_field);
            }
        }
        let mut left = Columns::new();
        let mut right = Columns::new();
        for key in join.by.iter().chain(std::iter::once(&join.on)) {
            left.insert(key.left.as_str().to_owned());
            right.insert(key.right.as_str().to_owned());
        }
        for column in &join.columns {
            let needed = if column.side == Side::Left {
                &mut left
            } else {
                &mut right
            };
            needed.insert(column.column.as_str().to_owned());
        }
        self.prune_at(&mut join.left, Some(left), outer, depth + 1)?;
        self.prune_at(&mut join.right, Some(right), outer, depth + 1)
    }

    fn prune_join(
        &mut self,
        join: &mut Join,
        required: Option<&Columns>,
        outer: &mut Columns,
        depth: usize,
    ) -> NativeResult<()> {
        if let Some(required) = required {
            let row_field = join.columns.first().cloned();
            join.columns
                .retain(|column| required.contains(&column.output_column));
            if join.columns.is_empty() {
                join.columns.extend(row_field);
            }
        }
        let mut left = Columns::new();
        let mut right = Columns::new();
        for key in &join.keys {
            left.insert(key.left.as_str().to_owned());
            right.insert(key.right.as_str().to_owned());
        }
        for column in &join.columns {
            let needed = if column.side == Side::Left {
                &mut left
            } else {
                &mut right
            };
            needed.insert(column.column.as_str().to_owned());
        }
        if let Some(condition) = &join.condition {
            for name in expression_source_columns(condition) {
                if let Some(name) = name.strip_prefix("left.") {
                    left.insert(name.to_owned());
                } else if let Some(name) = name.strip_prefix("right.") {
                    right.insert(name.to_owned());
                } else {
                    return Err(unsupported_sql_error(
                        "native JOIN ON column has no input side",
                    ));
                }
            }
        }
        self.prune_at(&mut join.left, Some(left), outer, depth + 1)?;
        self.prune_at(&mut join.right, Some(right), outer, depth + 1)
    }

    fn prune_subquery(
        &mut self,
        query: &mut Subquery,
        mut required: Option<Columns>,
        outer: &mut Columns,
        parameterized: bool,
        depth: usize,
    ) -> NativeResult<()> {
        if matches!(query.relation, Plan::DeferredSubquery(_)) {
            // The inner tree binds after discovery for each parameter. Keep its
            // entire declared outer schema; pruning it here would invalidate
            // names captured by that deferred declaration.
            return self.prune_at(&mut query.input, None, outer, depth + 1);
        }
        let mut relation = Columns::new();
        let mut input = Columns::new();
        if let Some(guard) = &query.evaluation_guard {
            input.extend(expression_source_columns(guard));
        }
        let keys = match &query.kind {
            SubqueryKind::In { columns } => columns.as_slice(),
            SubqueryKind::Quantified { columns, .. } => std::slice::from_ref(columns),
            SubqueryKind::Exists | SubqueryKind::Scalar => &[],
        };
        for key in keys.iter().chain(&query.correlation) {
            input.insert(key.left.as_str().to_owned());
            relation.insert(key.right.as_str().to_owned());
        }
        let mut scoped_outer = Columns::new();
        self.prune_at(
            &mut query.relation,
            if matches!(query.kind, SubqueryKind::Scalar) {
                None
            } else {
                Some(relation)
            },
            &mut scoped_outer,
            depth + 1,
        )?;
        if parameterized {
            input.extend(scoped_outer);
        } else {
            outer.extend(scoped_outer);
        }
        if let Some(required) = &mut required {
            required.remove(&query.output_column);
            required.extend(input);
        }
        self.prune_at(&mut query.input, required, outer, depth + 1)
    }
}

fn pipeline_columns(stage: &Stage, required: &mut Option<Columns>) {
    match stage {
        Stage::Project(expressions) => {
            *required = Some(
                expressions
                    .iter()
                    .flat_map(|(_, expression)| expression_source_columns(expression))
                    .collect(),
            );
        }
        Stage::Filter(predicate) => add_expression(required, predicate),
        Stage::Observe(observation) => {
            if observation.projection.is_empty() {
                if let Some(required) = required {
                    required.extend(
                        observation
                            .measures
                            .iter()
                            .flat_map(shardloom_vortex::VortexSimpleAggregateMeasure::input_columns)
                            .map(|column| column.as_str().to_owned()),
                    );
                }
            } else {
                for (_, expression) in &observation.projection {
                    add_expression(required, expression);
                }
            }
        }
        Stage::Limit { .. } => {}
    }
}

fn add_expression(required: &mut Option<Columns>, expression: &Expression) {
    if let Some(required) = required {
        required.extend(expression_source_columns(expression));
    }
}
