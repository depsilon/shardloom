//! Projection/window lowering preserves output order and keeps sort helpers private.

use super::{
    Aggregate, AggregateFunction, ColumnRef, ExprId, Expression, ExpressionKind, Lowered, Lowerer,
    NativeResult, NativeWindowFunction, ParsedAggregate, ParsedAggregateArgument,
    ParsedProjectionOutput, ParsedSqlLocalSource, ParsedWindowProjection, Plan, Project, Window,
    WindowExpression, WindowFunction, append_ordered_projection_expression, column,
    find_projection_by_alias, map_columns, order_key, unsupported_sql_error,
};
use shardloom_vortex::relational_query::VortexRelationalFrameFunction as FrameFunction;

impl Lowerer<'_, '_> {
    fn projected_expressions(
        &mut self,
        mut input: Lowered,
        parsed: &ParsedSqlLocalSource,
        visible: &[String],
    ) -> NativeResult<(Lowered, Vec<(String, Expression)>)> {
        let mut expressions = Vec::new();
        for output in &parsed.projection_order {
            match output {
                ParsedProjectionOutput::GenericExpression(alias) => {
                    let projection = find_projection_by_alias(
                        &parsed.generic_expression_projections,
                        alias,
                        "scalar",
                    )?;
                    let (next, expression) = self.scalar(input, &projection.expression, None)?;
                    input = next;
                    expressions.push((alias.clone(), expression));
                }
                ParsedProjectionOutput::Predicate(alias) => {
                    let projection = find_projection_by_alias(
                        &parsed.predicate_projections,
                        alias,
                        "predicate",
                    )?;
                    let (next, expression) = self.predicate(input, &projection.predicate)?;
                    input = next;
                    expressions.push((alias.clone(), expression));
                }
                ParsedProjectionOutput::Conditional(alias) => {
                    let projection = find_projection_by_alias(
                        &parsed.conditional_projections,
                        alias,
                        "conditional",
                    )?;
                    let (next, predicate) = self.predicate(input, &projection.predicate)?;
                    input = next;
                    let expression = Expression::new(
                        ExprId::new(format!("native.case.{alias}"))?,
                        ExpressionKind::FunctionCall {
                            name: "case_when".into(),
                            args: vec![
                                predicate,
                                projection.then_branch.to_expression(ExprId::new("then")?)?,
                                projection.else_branch.to_expression(ExprId::new("else")?)?,
                            ],
                        },
                    );
                    expressions.push((alias.clone(), expression));
                }
                ParsedProjectionOutput::Window(alias) => {
                    expressions.push((alias.clone(), column(alias)?));
                }
                output => {
                    let mut lowered = Vec::new();
                    append_ordered_projection_expression(
                        &mut lowered,
                        parsed,
                        output,
                        visible,
                        "native",
                    )?;
                    for expression in lowered {
                        let name = match &expression.kind {
                            ExpressionKind::Alias { alias, .. } => alias.clone(),
                            ExpressionKind::Column(column) => column.as_str().to_owned(),
                            _ => {
                                return Err(unsupported_sql_error(
                                    "native projection requires a column or explicit alias",
                                ));
                            }
                        };
                        expressions.push((name, expression));
                    }
                }
            }
        }
        for (_, expression) in &mut expressions {
            map_columns(expression, &mut |name| input.resolve(name))?;
        }
        if parsed.replace_or_add_projection {
            // Resolve every expression against the preceding stage before replacing
            // slots. Multiple expressions in one SELECT never see one another's values.
            let added = expressions.split_off(visible.len());
            for (name, expression) in added {
                if let Some((_, existing)) =
                    expressions.iter_mut().find(|(column, _)| column == &name)
                {
                    *existing = expression;
                } else {
                    expressions.push((name, expression));
                }
            }
        }
        Ok((input, expressions))
    }

    pub(super) fn projection(
        &mut self,
        input: Lowered,
        parsed: &ParsedSqlLocalSource,
        visible: &[String],
    ) -> NativeResult<Lowered> {
        let (mut input, mut expressions) = self.projected_expressions(input, parsed, visible)?;
        let visible_names = expressions
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let mut order = parsed.order_by.clone();
        if let Some(order) = &mut order {
            for key in &mut order.keys {
                if visible_names.contains(&key.column) {
                    continue;
                }
                if parsed.distinct_projection {
                    return Err(unsupported_sql_error(
                        "SELECT DISTINCT requires ORDER BY columns to be projected",
                    ));
                }
                let resolved = input.resolve(&key.column)?;
                let hidden = self.fresh(
                    &expressions
                        .iter()
                        .map(|(name, _)| name.clone())
                        .collect::<Vec<_>>(),
                );
                expressions.push((hidden.clone(), column(&resolved)?));
                key.column = hidden;
            }
        }
        input = input.project(expressions);
        if parsed.distinct_projection {
            input.plan = Plan::Aggregate(Box::new(Aggregate {
                input: input.plan,
                group_by: input
                    .columns
                    .iter()
                    .map(|name| ColumnRef::new(name.clone()))
                    .collect::<NativeResult<_>>()?,
                measures: Vec::new(),
            }));
        }
        input = input.order(order.as_ref())?;
        if input.columns != visible_names {
            let expressions = visible_names
                .into_iter()
                .map(|name| Ok((name.clone(), column(&name)?)))
                .collect::<NativeResult<_>>()?;
            input = input.project(expressions);
        }
        Ok(input)
    }

    pub(super) fn windows(
        &mut self,
        input: Lowered,
        windows: &[ParsedWindowProjection],
    ) -> NativeResult<Lowered> {
        if windows.is_empty() {
            return Ok(input);
        }
        let mut windows = windows.to_vec();
        let mut input = self.window_scalars(input, &mut windows)?;
        let mut projection = input
            .columns
            .iter()
            .map(|name| Ok((name.clone(), column(name)?)))
            .collect::<NativeResult<Vec<_>>>()?;
        let mut names = input.columns.clone();
        names.extend(windows.iter().map(|window| window.alias.clone()));
        let expressions = windows
            .iter()
            .map(|window| {
                let function = match &window.function {
                    WindowFunction::RowNumber => NativeWindowFunction::RowNumber,
                    WindowFunction::Rank => NativeWindowFunction::Rank,
                    WindowFunction::DenseRank => NativeWindowFunction::DenseRank,
                    WindowFunction::Lag { column, offset } => NativeWindowFunction::Lag {
                        column: ColumnRef::new(input.resolve(column)?)?,
                        offset: *offset,
                    },
                    WindowFunction::Lead { column, offset } => NativeWindowFunction::Lead {
                        column: ColumnRef::new(input.resolve(column)?)?,
                        offset: *offset,
                    },
                    WindowFunction::Ntile { bucket_count } => NativeWindowFunction::Ntile {
                        buckets: *bucket_count,
                    },
                    WindowFunction::PercentRank => NativeWindowFunction::PercentRank,
                    WindowFunction::CumeDist => NativeWindowFunction::CumeDist,
                    WindowFunction::Aggregate(aggregate) => NativeWindowFunction::Framed(
                        self.window_aggregate(aggregate, &input, &mut projection, &mut names)?,
                    ),
                    WindowFunction::FirstValue(expression) => {
                        NativeWindowFunction::Framed(FrameFunction::FirstValue(
                            self.window_argument(expression, &input, &mut projection, &mut names)?,
                        ))
                    }
                    WindowFunction::LastValue(expression) => {
                        NativeWindowFunction::Framed(FrameFunction::LastValue(
                            self.window_argument(expression, &input, &mut projection, &mut names)?,
                        ))
                    }
                    WindowFunction::NthValue { expression, index } => {
                        NativeWindowFunction::Framed(FrameFunction::NthValue {
                            column: self.window_argument(
                                expression,
                                &input,
                                &mut projection,
                                &mut names,
                            )?,
                            index: *index,
                        })
                    }
                };
                Ok(WindowExpression {
                    output_column: window.alias.clone(),
                    function,
                    partition_by: window
                        .partition_by
                        .iter()
                        .map(|name| ColumnRef::new(input.resolve(name)?))
                        .collect::<NativeResult<_>>()?,
                    order_by: window
                        .order_by
                        .keys
                        .iter()
                        .map(|key| order_key(key, &input))
                        .collect::<NativeResult<_>>()?,
                    frame: window.frame.clone(),
                })
            })
            .collect::<NativeResult<_>>()?;
        if projection.len() > input.columns.len() {
            input.plan = Plan::Project(Box::new(Project {
                input: input.plan,
                expressions: projection,
            }));
        }
        let columns = input
            .columns
            .iter()
            .map(|name| ColumnRef::new(name.clone()))
            .collect::<NativeResult<_>>()?;
        input.plan = Plan::Window(Box::new(Window {
            input: input.plan,
            columns,
            expressions,
        }));
        input
            .columns
            .extend(windows.iter().map(|window| window.alias.clone()));
        Ok(input)
    }

    fn window_scalars(
        &mut self,
        mut input: Lowered,
        windows: &mut [ParsedWindowProjection],
    ) -> NativeResult<Lowered> {
        for window in windows {
            let value = match &mut window.function {
                WindowFunction::FirstValue(value)
                | WindowFunction::LastValue(value)
                | WindowFunction::NthValue {
                    expression: value, ..
                } => Some(value),
                WindowFunction::Aggregate(aggregate) => match &mut aggregate.argument {
                    ParsedAggregateArgument::Computed { expression, .. } => {
                        Some(expression.as_mut())
                    }
                    _ => None,
                },
                _ => None,
            };
            if let Some(value) = value {
                let (next, expression) = self.scalar(input, value, None)?;
                input = next;
                *value = expression.into();
            }
        }
        Ok(input)
    }

    fn window_aggregate(
        &mut self,
        aggregate: &ParsedAggregate,
        input: &Lowered,
        projection: &mut Vec<(String, Expression)>,
        names: &mut Vec<String>,
    ) -> NativeResult<FrameFunction> {
        let argument = match &aggregate.argument {
            ParsedAggregateArgument::All => None,
            ParsedAggregateArgument::Column(name) => Some(ColumnRef::new(input.resolve(name)?)?),
            ParsedAggregateArgument::Computed { expression, .. } => {
                Some(self.window_argument(expression, input, projection, names)?)
            }
        };
        Ok(match (aggregate.function, aggregate.distinct, argument) {
            (AggregateFunction::Count, false, None) => FrameFunction::CountAll,
            (AggregateFunction::Count, false, Some(column)) => FrameFunction::Count(column),
            (AggregateFunction::Count, true, Some(column)) => FrameFunction::CountDistinct(column),
            (AggregateFunction::Sum, false, Some(column)) => FrameFunction::Sum(column),
            (AggregateFunction::Avg, false, Some(column)) => FrameFunction::Avg(column),
            (AggregateFunction::Min, false, Some(column)) => FrameFunction::Min(column),
            (AggregateFunction::Max, false, Some(column)) => FrameFunction::Max(column),
            _ => {
                return Err(unsupported_sql_error(
                    "framed aggregate has incompatible arguments",
                ));
            }
        })
    }

    fn window_argument(
        &mut self,
        expression: &Expression,
        input: &Lowered,
        projection: &mut Vec<(String, Expression)>,
        names: &mut Vec<String>,
    ) -> NativeResult<ColumnRef> {
        if let ExpressionKind::Column(column) = &expression.kind {
            return ColumnRef::new(input.resolve(column.as_str())?);
        }
        let mut expression = expression.clone();
        map_columns(&mut expression, &mut |name| input.resolve(name))?;
        let name = self.fresh(names);
        names.push(name.clone());
        projection.push((name.clone(), expression));
        ColumnRef::new(name)
    }
}
