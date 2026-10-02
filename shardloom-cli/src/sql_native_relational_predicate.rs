//! Subquery ASTs become relational operators, never empty cached literal lists.

use super::{
    ColumnRef, ComparisonOp, ExprId, Expression, ExpressionKind, JoinKey, Lowered, Lowerer,
    NativeResult, ParsedInSubquery, ParsedOrderBy, ParsedPredicate,
    ParsedQuantifiedSubqueryQuantifier, ParsedRelationSource, ParsedSqlLocalSource, Plan,
    Quantifier, ScalarValue, Subquery, SubqueryKind, UnaryOp, column, is_outer_correlation_ref,
    map_columns, unsupported_sql_error,
};

pub(super) fn declared_sources(
    predicate: &ParsedPredicate,
    paths: &mut std::collections::BTreeSet<super::ParsedRelationLeaf>,
) {
    let (source, filter, projected) = match predicate {
        ParsedPredicate::Logical { left, right, .. } => {
            declared_sources(left, paths);
            declared_sources(right, paths);
            return;
        }
        ParsedPredicate::Not { inner } => {
            declared_sources(inner, paths);
            return;
        }
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
        _ => return,
    };
    super::declared_relation_sources(source, paths);
    declared_sources(filter, paths);
    if let Some(projected) = projected {
        super::declared_sources(projected, paths);
    }
}

pub(super) fn has_subquery(predicate: &ParsedPredicate) -> bool {
    match predicate {
        ParsedPredicate::InSubquery { .. }
        | ParsedPredicate::RowValueInSubquery { .. }
        | ParsedPredicate::QuantifiedSubquery { .. }
        | ParsedPredicate::ExistsSubquery { .. } => true,
        ParsedPredicate::Logical { left, right, .. } => has_subquery(left) || has_subquery(right),
        ParsedPredicate::Not { inner } => has_subquery(inner),
        _ => false,
    }
}

fn direct_outer(predicate: &ParsedPredicate) -> bool {
    match predicate {
        ParsedPredicate::InSubquery { .. }
        | ParsedPredicate::RowValueInSubquery { .. }
        | ParsedPredicate::QuantifiedSubquery { .. }
        | ParsedPredicate::ExistsSubquery { .. } => false,
        ParsedPredicate::Logical { left, right, .. } => direct_outer(left) || direct_outer(right),
        ParsedPredicate::Not { inner } => direct_outer(inner),
        _ => predicate
            .columns()
            .into_iter()
            .any(is_outer_correlation_ref),
    }
}

#[derive(Clone, Copy)]
struct Inner<'a> {
    source: &'a ParsedRelationSource,
    selected: &'a [String],
    predicate: &'a ParsedPredicate,
    projected: Option<&'a ParsedSqlLocalSource>,
    order: Option<&'a ParsedOrderBy>,
    limit: Option<usize>,
}

impl Lowerer<'_, '_> {
    pub(super) fn predicate(
        &mut self,
        input: Lowered,
        predicate: &ParsedPredicate,
    ) -> NativeResult<(Lowered, Expression)> {
        match predicate {
            ParsedPredicate::Logical { left, right, op } => {
                let (input, left) = self.predicate(input, left)?;
                let (input, right) = self.predicate(input, right)?;
                Ok((
                    input,
                    Expression::new(
                        ExprId::new("native.logical")?,
                        ExpressionKind::Binary {
                            left: Box::new(left),
                            op: op.binary_op(),
                            right: Box::new(right),
                        },
                    ),
                ))
            }
            ParsedPredicate::Not { inner } => {
                let (input, inner) = self.predicate(input, inner)?;
                Ok((
                    input,
                    Expression::new(
                        ExprId::new("native.not")?,
                        ExpressionKind::Unary {
                            op: UnaryOp::Not,
                            expr: Box::new(inner),
                        },
                    ),
                ))
            }
            ParsedPredicate::InSubquery { column, subquery } => {
                let columns = vec![JoinKey {
                    left: ColumnRef::new(input.resolve(column)?)?,
                    right: ColumnRef::new(subquery.source_column.clone())?,
                }];
                self.scalar_subquery(input, subquery, SubqueryKind::In { columns })
            }
            ParsedPredicate::QuantifiedSubquery {
                column,
                subquery,
                comparison,
                quantifier,
            } => self.quantified(input, column, subquery, *comparison, *quantifier),
            ParsedPredicate::RowValueInSubquery { columns, subquery } => {
                if columns.len() != subquery.source_columns.len() {
                    return Err(unsupported_sql_error(
                        "row membership arity differs from its subquery",
                    ));
                }
                let columns = columns
                    .iter()
                    .zip(&subquery.source_columns)
                    .map(|(left, right)| {
                        Ok(JoinKey {
                            left: ColumnRef::new(input.resolve(left)?)?,
                            right: ColumnRef::new(right.clone())?,
                        })
                    })
                    .collect::<NativeResult<_>>()?;
                self.subquery(
                    input,
                    Inner {
                        source: &subquery.source,
                        selected: &subquery.source_columns,
                        predicate: &subquery.predicate,
                        projected: subquery.projected_plan.as_deref(),
                        order: subquery.order_by.as_ref(),
                        limit: subquery.limit,
                    },
                    SubqueryKind::In { columns },
                )
            }
            ParsedPredicate::ExistsSubquery { subquery } => self.subquery(
                input,
                Inner {
                    source: &subquery.source,
                    selected: &subquery.selected_columns,
                    predicate: &subquery.predicate,
                    projected: subquery.projected_plan.as_deref(),
                    order: subquery.order_by.as_ref(),
                    limit: subquery.limit,
                },
                SubqueryKind::Exists,
            ),
            ParsedPredicate::All => Ok((
                input,
                Expression::literal(ExprId::new("native.all")?, ScalarValue::Boolean(true)),
            )),
            _ => {
                let mut expression = predicate.to_expression()?;
                map_columns(&mut expression, &mut |name| input.resolve(name))?;
                Ok((input, expression))
            }
        }
    }

    fn quantified(
        &mut self,
        input: Lowered,
        column: &str,
        subquery: &ParsedInSubquery,
        comparison: ComparisonOp,
        quantifier: ParsedQuantifiedSubqueryQuantifier,
    ) -> NativeResult<(Lowered, Expression)> {
        let columns = JoinKey {
            left: ColumnRef::new(input.resolve(column)?)?,
            right: ColumnRef::new(subquery.source_column.clone())?,
        };
        self.scalar_subquery(
            input,
            subquery,
            SubqueryKind::Quantified {
                columns,
                comparison,
                quantifier: match quantifier {
                    ParsedQuantifiedSubqueryQuantifier::Any => Quantifier::Any,
                    ParsedQuantifiedSubqueryQuantifier::All => Quantifier::All,
                },
            },
        )
    }

    fn scalar_subquery(
        &mut self,
        input: Lowered,
        subquery: &ParsedInSubquery,
        kind: SubqueryKind,
    ) -> NativeResult<(Lowered, Expression)> {
        self.subquery(
            input,
            Inner {
                source: &subquery.source,
                selected: std::slice::from_ref(&subquery.source_column),
                predicate: &subquery.predicate,
                projected: subquery.projected_plan.as_deref(),
                order: subquery.order_by.as_ref(),
                limit: subquery.limit,
            },
            kind,
        )
    }

    fn subquery(
        &mut self,
        mut input: Lowered,
        inner: Inner<'_>,
        kind: SubqueryKind,
    ) -> NativeResult<(Lowered, Expression)> {
        let parameterized = inner.projected.map_or_else(
            || direct_outer(inner.predicate),
            |parsed| parsed.predicate_surfaces().into_iter().any(direct_outer),
        );
        let outer = parameterized.then_some(input.columns.as_slice());
        let relation = if let Some(parsed) = inner.projected {
            self.select(parsed, outer, !parsed.limit_is_synthetic)?
        } else {
            let relation = self.relation(inner.source)?;
            for selected in inner.selected {
                relation.resolve(selected)?;
            }
            let relation = Self::with_outer(relation, outer)?;
            let relation = self.filter(relation, inner.predicate)?.order(inner.order)?;
            if let Some(limit) = inner.limit {
                relation.limit(limit)
            } else {
                relation
            }
        };
        let selected = match &kind {
            SubqueryKind::In { columns } => columns
                .iter()
                .map(|key| key.right.as_str())
                .collect::<Vec<_>>(),
            SubqueryKind::Quantified { columns, .. } => vec![columns.right.as_str()],
            SubqueryKind::Exists => vec![],
        };
        let relation = if selected.is_empty() {
            // EXISTS needs only row presence, preserving count/group/limit semantics.
            relation.project(vec![(
                "__exists".into(),
                Expression::literal(ExprId::new("native.exists")?, ScalarValue::Boolean(true)),
            )])
        } else {
            let expressions = selected
                .iter()
                .map(|name| Ok(((*name).to_owned(), column(&relation.resolve(name)?)?)))
                .collect::<NativeResult<_>>()?;
            relation.project(expressions)
        };
        let output_column = self.fresh(&input.columns);
        let query = Box::new(Subquery {
            input: input.plan,
            relation: relation.plan,
            kind,
            correlation: vec![],
            output_column: output_column.clone(),
            negated: false,
        });
        input.plan = if parameterized {
            Plan::CorrelatedSubquery(query)
        } else {
            Plan::Subquery(query)
        };
        input.columns.push(output_column.clone());
        Ok((input, column(&output_column)?))
    }
}
