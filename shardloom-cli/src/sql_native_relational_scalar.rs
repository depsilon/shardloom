//! Resolve inert scalar declarations and preserve lazy relational demand.

use super::{
    ColumnRef, ExprId, Expression, ExpressionKind, Lowered, Lowerer, NativeResult,
    ParsedAggregateArgument, ParsedRelationQuery, ParsedSqlLocalSource, ScalarValue, SubqueryKind,
    UnaryOp, WindowFunction, expression_source_columns, is_outer_correlation_ref, predicate,
    scalar_expression, unsupported_sql_error,
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

pub(super) fn direct_outer(value: &ParsedScalarExpression) -> bool {
    expression_source_columns(value)
        .iter()
        .any(|name| is_outer_correlation_ref(name))
        || value.bindings.iter().any(|(_, binding)| match binding {
            RelationalBinding::Predicate(value) => predicate::direct_outer(value),
            RelationalBinding::Scalar(_) | RelationalBinding::Parameter(_) => false,
        })
}

/// Reject data-dependent scalar schemas before either preparation route can
/// resolve sources or evaluate a pivot, including in a never-selected branch.
pub(super) fn validate_query(query: &ParsedRelationQuery) -> NativeResult<()> {
    validate_bound_query(query, &super::BoundRelations::new())
}

pub(super) fn validate_bound_query(
    query: &ParsedRelationQuery,
    inputs: &super::BoundRelations,
) -> NativeResult<()> {
    for node in super::visit::Node::Query(query).walk() {
        if let super::visit::Node::Scalar(value) = node {
            for (_, binding) in &value.bindings {
                if let RelationalBinding::Scalar(query) = binding {
                    require_static_schema(query, inputs)?;
                }
            }
        }
    }
    Ok(())
}

fn require_static_schema(
    query: &ParsedRelationQuery,
    inputs: &super::BoundRelations,
) -> NativeResult<()> {
    if super::visit::Node::Query(query).dynamic_bound(inputs) {
        return Err(unsupported_sql_error(
            "scalar subquery requires a statically bound output schema; dynamic pivot is not admitted",
        ));
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
                    RelationalBinding::Parameter(name) => {
                        Ok((input, self.parameter(name, expression.id)?))
                    }
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

    fn parameter(&self, name: &str, id: ExprId) -> NativeResult<Expression> {
        let mut value =
            self.parameters.get(name).cloned().ok_or_else(|| {
                unsupported_sql_error(&format!("missing scalar parameter :{name}"))
            })?;
        value.id = id;
        Ok(value)
    }

    fn scalar_subquery_value(
        &mut self,
        input: Lowered,
        query: &ParsedRelationQuery,
        guard: Option<&Expression>,
    ) -> NativeResult<(Lowered, Expression)> {
        require_static_schema(query, &self.bound_inputs)?;
        let parameterized = predicate::query_outer(query, &self.bound_inputs)?;
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
