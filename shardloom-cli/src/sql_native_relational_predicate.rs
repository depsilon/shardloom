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

pub(super) fn select_direct_outer(parsed: &ParsedSqlLocalSource) -> bool {
    parsed.predicate_surfaces().into_iter().any(direct_outer)
}

fn source_outer(source: &ParsedRelationSource) -> bool {
    match source {
        ParsedRelationSource::Local(_) => false,
        ParsedRelationSource::Derived(query) => query_outer(query),
        ParsedRelationSource::Unary(unary) => query_outer(&unary.input),
    }
}

fn query_outer(query: &super::ParsedRelationQuery) -> bool {
    match query {
        super::ParsedRelationQuery::Select(parsed) => select_outer(parsed),
        super::ParsedRelationQuery::Set(set) => set.branches.iter().any(select_outer),
    }
}

fn select_outer(parsed: &ParsedSqlLocalSource) -> bool {
    select_direct_outer(parsed)
        || source_outer(&parsed.source)
        || parsed
            .join
            .as_ref()
            .is_some_and(|join| source_outer(&join.right_source))
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

struct OwnedInner {
    source: ParsedRelationSource,
    selected: Vec<String>,
    predicate: ParsedPredicate,
    projected: Option<ParsedSqlLocalSource>,
    order: Option<ParsedOrderBy>,
    limit: Option<usize>,
}

impl From<Inner<'_>> for OwnedInner {
    fn from(inner: Inner<'_>) -> Self {
        Self {
            source: inner.source.clone(),
            selected: inner.selected.to_vec(),
            predicate: inner.predicate.clone(),
            projected: inner.projected.cloned(),
            order: inner.order.cloned(),
            limit: inner.limit,
        }
    }
}

impl OwnedInner {
    fn as_inner(&self) -> Inner<'_> {
        Inner {
            source: &self.source,
            selected: &self.selected,
            predicate: &self.predicate,
            projected: self.projected.as_ref(),
            order: self.order.as_ref(),
            limit: self.limit,
        }
    }
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
        let parameterized = source_outer(inner.source)
            || direct_outer(inner.predicate)
            || inner.projected.is_some_and(select_outer);
        let dynamic = super::dynamic::source_required(inner.source)
            || super::dynamic::predicate_required(inner.predicate)
            || inner.projected.is_some_and(super::dynamic::select_required);
        let relation = if parameterized && dynamic {
            let declaration = self.declaration.clone().ok_or_else(|| {
                unsupported_sql_error("dynamic subquery requires an execution declaration")
            })?;
            let owned = OwnedInner::from(inner);
            let columns = input.columns.clone();
            let kind = kind.clone();
            self.schemas
                .defer_subquery(declaration.bytes, move |schemas| {
                    let mut resolver = |leaf: &super::ParsedRelationLeaf| {
                        declaration.sources.get(leaf).cloned().ok_or_else(|| {
                            unsupported_sql_error(
                                "dynamic subquery referenced an undeclared source",
                            )
                        })
                    };
                    let mut lowerer = Lowerer {
                        schemas,
                        serial: 0,
                        resolve_source: &mut resolver,
                        declaration: Some(declaration.clone()),
                        outer: Some(columns.clone()),
                    };
                    let mut relation = lowerer.inner_relation(owned.as_inner(), &kind)?;
                    lowerer.prune(
                        &mut relation.plan,
                        None,
                        &mut std::collections::BTreeSet::new(),
                    )?;
                    Ok(relation.plan)
                })?
        } else {
            let previous = self.outer.clone();
            if parameterized {
                self.outer = Some(input.columns.clone());
            }
            let relation = self.inner_relation(inner, &kind);
            self.outer = previous;
            relation?.plan
        };
        let output_column = self.fresh(&input.columns);
        let query = Box::new(Subquery {
            input: input.plan,
            relation,
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

    fn inner_relation(&mut self, inner: Inner<'_>, kind: &SubqueryKind) -> NativeResult<Lowered> {
        let relation = if let Some(parsed) = inner.projected {
            self.select(parsed, None, !parsed.limit_is_synthetic)?
        } else {
            let relation = self.relation(inner.source)?;
            for selected in inner.selected {
                relation.resolve(selected)?;
            }
            let outer = self
                .outer
                .as_deref()
                .filter(|_| direct_outer(inner.predicate));
            let relation = Self::with_outer(relation, outer)?;
            let relation = self.filter(relation, inner.predicate)?.order(inner.order)?;
            if let Some(limit) = inner.limit {
                relation.limit(limit)
            } else {
                relation
            }
        };
        let selected = match kind {
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
        Ok(relation)
    }
}
