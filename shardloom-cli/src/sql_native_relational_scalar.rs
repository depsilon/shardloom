//! Resolve inert scalar declarations and preserve lazy relational demand.

use super::{
    BTreeSet, ColumnRef, ExprId, Expression, ExpressionKind, Lowered, Lowerer, NativeResult,
    ParsedAggregateArgument, ParsedPredicate, ParsedRelationLeaf, ParsedRelationQuery,
    ParsedRelationSource, ParsedSqlLocalSource, ScalarValue, SubqueryKind, UnaryOp, WindowFunction,
    dynamic, expression_source_columns, is_outer_correlation_ref, predicate, scalar_expression,
    unsupported_sql_error,
};
use scalar_expression::{ParsedScalarExpression, RelationalBinding};

pub(super) fn surfaces(parsed: &ParsedSqlLocalSource) -> Vec<&ParsedScalarExpression> {
    let mut values = parsed
        .generic_expression_projections
        .iter()
        .map(|item| &item.expression)
        .collect::<Vec<_>>();
    for aggregate in parsed.aggregates.iter().chain(&parsed.having_aggregates) {
        if let ParsedAggregateArgument::Computed { expression, .. } = &aggregate.argument {
            values.push(expression);
        }
    }
    for window in &parsed.window_projections {
        match &window.function {
            WindowFunction::FirstValue(expression)
            | WindowFunction::LastValue(expression)
            | WindowFunction::NthValue { expression, .. } => values.push(expression),
            WindowFunction::Aggregate(aggregate) => {
                if let ParsedAggregateArgument::Computed { expression, .. } = &aggregate.argument {
                    values.push(expression);
                }
            }
            _ => {}
        }
    }
    values
}

pub(super) fn declared_sources(
    value: &ParsedScalarExpression,
    paths: &mut BTreeSet<ParsedRelationLeaf>,
) {
    for (_, binding) in &value.bindings {
        match binding {
            RelationalBinding::Scalar(query) => super::declared_query_sources(query, paths),
            RelationalBinding::Predicate(value) => predicate::declared_sources(value, paths),
        }
    }
}

pub(super) fn direct_outer(value: &ParsedScalarExpression) -> bool {
    expression_source_columns(value)
        .iter()
        .any(|name| is_outer_correlation_ref(name))
        || value.bindings.iter().any(|(_, binding)| match binding {
            RelationalBinding::Predicate(value) => predicate::direct_outer(value),
            RelationalBinding::Scalar(_) => false,
        })
}

pub(super) fn dynamic_required(value: &ParsedScalarExpression) -> bool {
    value.bindings.iter().any(|(_, binding)| match binding {
        RelationalBinding::Scalar(query) => dynamic::required(query),
        RelationalBinding::Predicate(value) => dynamic::predicate_required(value),
    })
}

/// Reject data-dependent scalar schemas before either preparation route can
/// resolve sources or evaluate a pivot, including in a never-selected branch.
pub(super) fn validate_query(query: &ParsedRelationQuery) -> NativeResult<()> {
    match query {
        ParsedRelationQuery::Select(parsed) => validate_select(parsed),
        ParsedRelationQuery::Set(set) => set.branches.iter().try_for_each(validate_select),
    }
}

fn validate_select(parsed: &ParsedSqlLocalSource) -> NativeResult<()> {
    validate_source(&parsed.source)?;
    if let Some(join) = &parsed.join {
        validate_source(&join.right_source)?;
    }
    for predicate in parsed.predicate_surfaces() {
        validate_predicate(predicate)?;
    }
    surfaces(parsed)
        .into_iter()
        .try_for_each(validate_expression)
}

fn validate_source(source: &ParsedRelationSource) -> NativeResult<()> {
    match source {
        ParsedRelationSource::Local(_) => Ok(()),
        ParsedRelationSource::Derived(query) => validate_query(query),
        ParsedRelationSource::Unary(operation) => validate_query(&operation.input),
    }
}

fn require_static_schema(query: &ParsedRelationQuery) -> NativeResult<()> {
    if dynamic::required(query) {
        return Err(unsupported_sql_error(
            "scalar subquery requires a statically bound output schema; dynamic pivot is not admitted",
        ));
    }
    Ok(())
}

fn validate_expression(value: &ParsedScalarExpression) -> NativeResult<()> {
    for (_, binding) in &value.bindings {
        match binding {
            RelationalBinding::Scalar(query) => {
                require_static_schema(query)?;
                validate_query(query)?;
            }
            RelationalBinding::Predicate(predicate) => validate_predicate(predicate)?,
        }
    }
    Ok(())
}

fn validate_predicate(predicate: &ParsedPredicate) -> NativeResult<()> {
    let (source, filter, projected) = match predicate {
        ParsedPredicate::GenericExpressionCompare { left, right, .. } => {
            validate_expression(left)?;
            return validate_expression(right);
        }
        ParsedPredicate::Logical { left, right, .. } => {
            validate_predicate(left)?;
            return validate_predicate(right);
        }
        ParsedPredicate::Not { inner } => return validate_predicate(inner),
        ParsedPredicate::InSubquery { subquery, .. }
        | ParsedPredicate::QuantifiedSubquery { subquery, .. } => (
            &subquery.source,
            subquery.predicate.as_ref(),
            subquery.projected_plan.as_deref(),
        ),
        ParsedPredicate::RowValueInSubquery { subquery, .. } => (
            &subquery.source,
            subquery.predicate.as_ref(),
            subquery.projected_plan.as_deref(),
        ),
        ParsedPredicate::ExistsSubquery { subquery } => (
            &subquery.source,
            subquery.predicate.as_ref(),
            subquery.projected_plan.as_deref(),
        ),
        _ => return Ok(()),
    };
    validate_source(source)?;
    validate_predicate(filter)?;
    if let Some(parsed) = projected {
        validate_select(parsed)?;
    }
    Ok(())
}

impl Lowerer<'_, '_> {
    pub(super) fn scalar(
        &mut self,
        input: Lowered,
        value: &ParsedScalarExpression,
        guard: Option<&Expression>,
    ) -> NativeResult<(Lowered, Expression)> {
        self.scalar_expression(input, value.expression.clone(), &value.bindings, guard)
    }

    fn scalar_expression(
        &mut self,
        mut input: Lowered,
        mut expression: Expression,
        bindings: &[(ExprId, RelationalBinding)],
        guard: Option<&Expression>,
    ) -> NativeResult<(Lowered, Expression)> {
        match &mut expression.kind {
            ExpressionKind::RelationalValue { binding } => {
                let binding = bindings
                    .iter()
                    .find(|(id, _)| id == binding)
                    .map(|(_, value)| value)
                    .ok_or_else(|| unsupported_sql_error("scalar relational binding is absent"))?;
                return match binding {
                    RelationalBinding::Scalar(query) => {
                        self.scalar_subquery_value(input, query, guard)
                    }
                    RelationalBinding::Predicate(predicate) => {
                        self.guarded_predicate(input, predicate, guard)
                    }
                };
            }
            ExpressionKind::Column(column) => {
                *column = ColumnRef::new(input.resolve(column.as_str())?)?;
            }
            ExpressionKind::Alias { expr, .. }
            | ExpressionKind::Cast { expr, .. }
            | ExpressionKind::TryCast { expr, .. }
            | ExpressionKind::Unary { expr, .. } => {
                let (next, value) =
                    self.scalar_expression(input, (**expr).clone(), bindings, guard)?;
                input = next;
                **expr = value;
            }
            ExpressionKind::Binary { left, right, .. }
            | ExpressionKind::Compare { left, right, .. } => {
                let (next, value) =
                    self.scalar_expression(input, (**left).clone(), bindings, guard)?;
                let (next, other) =
                    self.scalar_expression(next, (**right).clone(), bindings, guard)?;
                input = next;
                **left = value;
                **right = other;
            }
            ExpressionKind::FunctionCall { name, args }
                if name.eq_ignore_ascii_case("case_when") && args.len() == 3 =>
            {
                let (next, condition) =
                    self.scalar_expression(input, args[0].clone(), bindings, guard)?;
                let yes = demand(guard, condition.clone(), true)?;
                let no = demand(guard, condition.clone(), false)?;
                let (next, selected) =
                    self.scalar_expression(next, args[1].clone(), bindings, Some(&yes))?;
                let (next, otherwise) =
                    self.scalar_expression(next, args[2].clone(), bindings, Some(&no))?;
                input = next;
                *args = vec![condition, selected, otherwise];
            }
            ExpressionKind::FunctionCall { name, args }
                if name.eq_ignore_ascii_case("coalesce") =>
            {
                let mut demand_guard = guard.cloned();
                for arg in args {
                    let (next, value) = self.scalar_expression(
                        input,
                        arg.clone(),
                        bindings,
                        demand_guard.as_ref(),
                    )?;
                    input = next;
                    let null = Expression::new(
                        ExprId::new("native.scalar.null")?,
                        ExpressionKind::Unary {
                            op: UnaryOp::IsNull,
                            expr: Box::new(value.clone()),
                        },
                    );
                    demand_guard = Some(demand(demand_guard.as_ref(), null, true)?);
                    *arg = value;
                }
            }
            ExpressionKind::FunctionCall { args, .. } | ExpressionKind::List { values: args } => {
                for arg in args {
                    let (next, value) =
                        self.scalar_expression(input, arg.clone(), bindings, guard)?;
                    input = next;
                    *arg = value;
                }
            }
            ExpressionKind::Struct { fields } => {
                for (_, field) in fields {
                    let (next, value) =
                        self.scalar_expression(input, field.clone(), bindings, guard)?;
                    input = next;
                    *field = value;
                }
            }
            ExpressionKind::Literal(_) | ExpressionKind::Unsupported { .. } => {}
        }
        Ok((input, expression))
    }

    fn scalar_subquery_value(
        &mut self,
        input: Lowered,
        query: &ParsedRelationQuery,
        guard: Option<&Expression>,
    ) -> NativeResult<(Lowered, Expression)> {
        require_static_schema(query)?;
        let parameterized = predicate::query_outer(query)?;
        let previous = std::mem::replace(
            &mut self.outer,
            parameterized.then(|| input.columns.clone()),
        );
        let relation = self.query(query);
        self.outer = previous;
        let relation = relation?;
        if relation.columns.len() != 1 {
            return Err(unsupported_sql_error(
                "scalar subquery requires exactly one output column",
            ));
        }
        self.append_subquery(
            input,
            relation.plan,
            SubqueryKind::Scalar,
            parameterized,
            guard,
        )
    }
}

fn demand(
    guard: Option<&Expression>,
    condition: Expression,
    selected: bool,
) -> NativeResult<Expression> {
    let boolean = |value| {
        Expression::literal(
            ExprId::new("native.scalar.demand").expect("fixed identifier"),
            ScalarValue::Boolean(value),
        )
    };
    let branch = Expression::new(
        ExprId::new("native.scalar.branch")?,
        ExpressionKind::FunctionCall {
            name: "case_when".into(),
            args: vec![condition, boolean(selected), boolean(!selected)],
        },
    );
    Ok(if let Some(guard) = guard {
        Expression::new(
            ExprId::new("native.scalar.guard")?,
            ExpressionKind::FunctionCall {
                name: "case_when".into(),
                args: vec![guard.clone(), branch, boolean(false)],
            },
        )
    } else {
        branch
    })
}
