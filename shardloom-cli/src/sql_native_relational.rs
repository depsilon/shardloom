//! Inert SQL parsing lowers into the shared prepared native relational executor.
//! The decoded reference preparation/evaluator is never called from this module.

use super::*;
use shardloom_core::DatasetUri;
use shardloom_plan::ProjectionRequest;
use shardloom_vortex::{
    VortexLocalPrimitiveExecutionPolicy,
    local_primitives::prepared_relational::{
        PreparedVortexRelational, VortexRelationalPreparation, prepare_relational_with_schema,
    },
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::{
        VortexRelationalAggregate as Aggregate, VortexRelationalFilter as Filter,
        VortexRelationalJoin as Join, VortexRelationalJoinColumn as JoinColumn,
        VortexRelationalJoinKey as JoinKey, VortexRelationalJoinKind as JoinKind,
        VortexRelationalLimit as Limit, VortexRelationalNullOrder as NullOrder,
        VortexRelationalOrderKey as OrderKey, VortexRelationalPlan as Plan,
        VortexRelationalProject as Project, VortexRelationalQuantifier as Quantifier,
        VortexRelationalScan as Scan, VortexRelationalSet as Set,
        VortexRelationalSetKind as SetKind, VortexRelationalSide as Side,
        VortexRelationalSort as Sort, VortexRelationalSubquery as Subquery,
        VortexRelationalSubqueryKind as SubqueryKind, VortexRelationalWindow as Window,
        VortexRelationalWindowExpression as WindowExpression,
        VortexRelationalWindowFunction as NativeWindowFunction,
    },
};

#[path = "sql_native_relational_predicate.rs"]
mod predicate;
#[path = "sql_native_relational_projection.rs"]
mod projection;
#[path = "sql_native_relational_pruning.rs"]
mod pruning;
#[cfg(test)]
#[path = "sql_native_relational_tests.rs"]
mod tests;

type NativeResult<T> = Result<T, ShardLoomError>;

pub(crate) fn prepare(
    raw: &str,
    policy: VortexLocalPrimitiveExecutionPolicy,
    mut resolve_source: impl FnMut(&Path) -> NativeResult<DatasetUri>,
) -> NativeResult<PreparedVortexRelational> {
    let statement = admitted_statement(raw)?;
    prepare_relational_with_schema(policy, |schemas| {
        let mut lowerer = Lowerer {
            schemas,
            serial: 0,
            resolve_source: &mut resolve_source,
        };
        let mut query = lowerer.statement(&statement)?;
        lowerer.prune(&mut query.plan, None, &mut BTreeSet::new())?;
        Ok(query.plan)
    })
}

/// Shape discovery is syntax-only. File/type/resource admission happens at prepare.
pub(crate) fn is_relational(raw: &str) -> NativeResult<bool> {
    let statement = admitted_statement(raw)?;
    if !top_level_sql_union_operators(&statement)?.is_empty() {
        parse_sql_local_source_union_statement(&statement)?;
        return Ok(true);
    }
    let parsed = parse_sql_local_source_statement(&statement)?;
    Ok(parsed.join.is_some()
        || !parsed.window_projections.is_empty()
        || parsed
            .predicate_surfaces()
            .into_iter()
            .any(predicate::has_subquery))
}

/// Count unique declared paths without opening inputs or preparing subqueries.
pub(crate) fn source_count(raw: &str) -> NativeResult<usize> {
    Ok(source_paths(raw)?.len())
}

pub(crate) fn source_paths(raw: &str) -> NativeResult<BTreeSet<PathBuf>> {
    let statement = admitted_statement(raw)?;
    let mut paths = BTreeSet::new();
    if top_level_sql_union_operators(&statement)?.is_empty() {
        declared_sources(&parse_sql_local_source_statement(&statement)?, &mut paths);
    } else {
        for branch in &parse_sql_local_source_union_statement(&statement)?.branches {
            declared_sources(branch, &mut paths);
        }
    }
    Ok(paths)
}

fn declared_sources(parsed: &ParsedSqlLocalSource, paths: &mut BTreeSet<PathBuf>) {
    paths.insert(parsed.source_path.clone());
    if let Some(join) = &parsed.join {
        paths.insert(join.right_source_path.clone());
    }
    for predicate in parsed.predicate_surfaces() {
        predicate::declared_sources(predicate, paths);
    }
}

fn admitted_statement(raw: &str) -> NativeResult<String> {
    if raw.len() > 256 * 1024 {
        return Err(unsupported_sql_error("native SQL exceeds 256 KiB"));
    }
    let mut quoted = false;
    let mut depth = 0usize;
    for ch in raw.chars() {
        match ch {
            '\'' => quoted = !quoted,
            '(' if !quoted => {
                depth += 1;
                if depth > 24 {
                    return Err(unsupported_sql_error(
                        "native SQL exceeds 24 nesting levels",
                    ));
                }
            }
            ')' if !quoted => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    let mut statement = normalize_sql_statement(raw)?;
    if top_level_keyword_indexes(&statement, "limit")?.is_empty() {
        write!(&mut statement, " LIMIT {}", usize::MAX).expect("String writes cannot fail");
    }
    Ok(statement)
}

struct Lowered {
    plan: Plan,
    columns: Vec<String>,
}

impl Lowered {
    fn resolve(&self, name: &str) -> NativeResult<String> {
        if self.columns.iter().any(|column| column == name) {
            return Ok(name.to_owned());
        }
        Err(unsupported_sql_error(&format!(
            "column {name:?} is not present in the native input schema"
        )))
    }

    fn project(self, expressions: Vec<(String, Expression)>) -> Self {
        Self {
            columns: expressions.iter().map(|(name, _)| name.clone()).collect(),
            plan: Plan::Project(Box::new(Project {
                input: self.plan,
                expressions,
            })),
        }
    }

    fn limit(mut self, count: usize) -> Self {
        self.plan = Plan::Limit(Box::new(Limit {
            input: self.plan,
            offset: 0,
            count,
        }));
        self
    }

    fn order(mut self, order: Option<&ParsedOrderBy>) -> NativeResult<Self> {
        if let Some(order) = order {
            let keys = order
                .keys
                .iter()
                .map(|key| order_key(key, &self))
                .collect::<NativeResult<_>>()?;
            self.plan = Plan::Sort(Box::new(Sort {
                input: self.plan,
                keys,
            }));
        }
        Ok(self)
    }
}

struct Lowerer<'a, 'session> {
    schemas: &'a mut VortexRelationalPreparation<'session>,
    serial: usize,
    resolve_source: &'a mut dyn FnMut(&Path) -> NativeResult<DatasetUri>,
}

impl Lowerer<'_, '_> {
    fn statement(&mut self, statement: &str) -> NativeResult<Lowered> {
        if top_level_sql_union_operators(statement)?.is_empty() {
            return self.select(&parse_sql_local_source_statement(statement)?, None, true);
        }
        let parsed = parse_sql_local_source_union_statement(statement)?;
        if parsed.branches.len() > 128 {
            return Err(unsupported_sql_error("native SQL exceeds 128 set branches"));
        }
        let kind = match parsed.mode {
            SqlUnionMode::All => SetKind::UnionAll,
            SqlUnionMode::Distinct => SetKind::UnionDistinct,
            SqlUnionMode::IntersectDistinct => SetKind::Intersect,
            SqlUnionMode::ExceptDistinct => SetKind::Except,
        };
        let mut branches = parsed.branches.iter();
        let first = branches
            .next()
            .ok_or_else(|| unsupported_sql_error("set operation requires branches"))?;
        // The legacy parser adds a synthetic branch LIMIT. It is not SQL's global LIMIT.
        let mut result = self.select(first, None, false)?;
        for branch in branches {
            let right = self.select(branch, None, false)?;
            result.plan = Plan::Set(Box::new(Set {
                left: result.plan,
                right: right.plan,
                kind,
            }));
        }
        Ok(result.order(parsed.order_by.as_ref())?.limit(parsed.limit))
    }

    fn scan(&mut self, path: &Path) -> NativeResult<Lowered> {
        let source_uri = (self.resolve_source)(path)?;
        let columns = self.schemas.source_columns(&source_uri)?;
        Ok(Lowered {
            plan: Plan::Scan(Scan {
                source_uri,
                projection: ProjectionRequest::All,
                predicate: None,
            }),
            columns,
        })
    }

    fn source(&mut self, parsed: &ParsedSqlLocalSource) -> NativeResult<Lowered> {
        let left = self.scan(&parsed.source_path)?;
        let Some(join) = &parsed.join else {
            return Ok(left);
        };
        let right = self.scan(&join.right_source_path)?;
        let left_alias = parsed
            .source_alias
            .as_deref()
            .ok_or_else(|| unsupported_sql_error("join left alias is absent"))?;
        let kind = match join.join_type {
            ParsedJoinType::InnerEqui => JoinKind::Inner,
            ParsedJoinType::LeftOuterEqui => JoinKind::Left,
            ParsedJoinType::RightOuterEqui => JoinKind::Right,
            ParsedJoinType::FullOuterEqui => JoinKind::Full,
            ParsedJoinType::LeftSemiEqui => JoinKind::LeftSemi,
            ParsedJoinType::LeftAntiEqui => JoinKind::LeftAnti,
            ParsedJoinType::Cross => JoinKind::Cross,
        };
        let mut columns = join_columns(&left.columns, Side::Left, Some(left_alias))?;
        if !matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
            columns.extend(join_columns(
                &right.columns,
                Side::Right,
                Some(&join.right_alias),
            )?);
        }
        let condition = join
            .on_predicate
            .as_ref()
            .map(|predicate| {
                if predicate::has_subquery(predicate) {
                    return Err(unsupported_sql_error(
                        "subqueries inside JOIN ON are not admitted",
                    ));
                }
                let mut expression = predicate.to_expression()?;
                map_columns(&mut expression, &mut |name| {
                    let qualified = parse_qualified_column_ref(name)?;
                    let side = if qualified.alias == left_alias {
                        "left"
                    } else if qualified.alias == join.right_alias {
                        "right"
                    } else {
                        return Err(unsupported_sql_error(
                            "JOIN ON uses an unknown source alias",
                        ));
                    };
                    Ok(format!("{side}.{}", qualified.column))
                })?;
                Ok(expression)
            })
            .transpose()?;
        let mut keys = join
            .key_pairs
            .iter()
            .map(|key| {
                Ok(JoinKey {
                    left: ColumnRef::new(key.left.column.clone())?,
                    right: ColumnRef::new(key.right.column.clone())?,
                })
            })
            .collect::<NativeResult<_>>()?;
        if let Some(condition) = &condition {
            append_equality_keys(condition, &mut keys)?;
        }
        Ok(Lowered {
            columns: columns
                .iter()
                .map(|column| column.output_column.clone())
                .collect(),
            plan: Plan::Join(Box::new(Join {
                left: left.plan,
                right: right.plan,
                kind,
                keys,
                condition,
                columns,
            })),
        })
    }

    fn with_outer(mut input: Lowered, outer: Option<&[String]>) -> NativeResult<Lowered> {
        let Some(outer) = outer else {
            return Ok(input);
        };
        let mut columns = join_columns(&input.columns, Side::Left, None)?;
        columns.extend(join_columns(outer, Side::Right, Some("outer"))?);
        input.columns = columns
            .iter()
            .map(|column| column.output_column.clone())
            .collect();
        input.plan = Plan::Join(Box::new(Join {
            left: input.plan,
            right: Plan::Outer,
            kind: JoinKind::Cross,
            keys: vec![],
            condition: None,
            columns,
        }));
        Ok(input)
    }

    fn select(
        &mut self,
        parsed: &ParsedSqlLocalSource,
        outer: Option<&[String]>,
        apply_limit: bool,
    ) -> NativeResult<Lowered> {
        let input = self.source(parsed)?;
        let visible = input.columns.clone();
        let input = Self::with_outer(input, outer)?;
        let mut input = self.filter(input, &parsed.predicate)?;
        let aggregate = !parsed.aggregates.is_empty()
            || !parsed.group_by.is_empty()
            || !parsed.having_aggregates.is_empty();
        if aggregate {
            input = Self::aggregate(input, parsed)?;
            input = self.filter(input, &parsed.having)?;
        } else if !parsed.having.is_all() {
            return Err(unsupported_sql_error(
                "HAVING requires grouped or scalar aggregation",
            ));
        }
        input = Self::windows(input, &parsed.window_projections)?;
        input = self.projection(input, parsed, &visible, aggregate)?;
        if apply_limit {
            input = input.limit(parsed.limit);
        }
        Ok(input)
    }

    fn filter(&mut self, input: Lowered, predicate: &ParsedPredicate) -> NativeResult<Lowered> {
        if predicate.is_all() {
            return Ok(input);
        }
        let (mut input, predicate) = self.predicate(input, predicate)?;
        input.plan = Plan::Filter(Box::new(Filter {
            input: input.plan,
            predicate,
        }));
        Ok(input)
    }

    fn aggregate(mut input: Lowered, parsed: &ParsedSqlLocalSource) -> NativeResult<Lowered> {
        let group_by = parsed
            .group_by
            .iter()
            .map(|name| ColumnRef::new(input.resolve(name)?))
            .collect::<NativeResult<Vec<_>>>()?;
        let measures = parsed
            .aggregates
            .iter()
            .chain(&parsed.having_aggregates)
            .map(|aggregate| {
                if aggregate.distinct && aggregate.function != AggregateFunction::Count {
                    return Err(unsupported_sql_error(
                        "only COUNT DISTINCT has an admitted native distinct aggregate",
                    ));
                }
                Ok(VortexSimpleAggregateMeasure::new(
                    if aggregate.distinct {
                        "count_distinct"
                    } else {
                        aggregate.function.as_str()
                    },
                    aggregate
                        .column
                        .as_deref()
                        .map(|name| ColumnRef::new(input.resolve(name)?))
                        .transpose()?,
                    aggregate.output_name(),
                ))
            })
            .collect::<NativeResult<Vec<_>>>()?;
        input.columns = group_by
            .iter()
            .map(|column| column.as_str().to_owned())
            .chain(measures.iter().map(|measure| measure.alias.clone()))
            .collect();
        input.plan = Plan::Aggregate(Box::new(Aggregate {
            input: input.plan,
            group_by,
            measures,
        }));
        Ok(input)
    }

    fn fresh(&mut self, columns: &[String]) -> String {
        loop {
            let name = format!("__shardloom_expression_{}", self.serial);
            self.serial += 1;
            if !columns.contains(&name) {
                return name;
            }
        }
    }
}

/// An equality conjunct can narrow the candidate set without moving any ON
/// predicate past outer null extension. Retain the complete condition for final
/// evaluation and never extract through OR, NOT, casts or computed expressions.
fn append_equality_keys(expression: &Expression, keys: &mut Vec<JoinKey>) -> NativeResult<()> {
    match &expression.kind {
        ExpressionKind::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            append_equality_keys(left, keys)?;
            append_equality_keys(right, keys)
        }
        ExpressionKind::Compare {
            left,
            op: ComparisonOp::Eq,
            right,
        } => {
            let (ExpressionKind::Column(left), ExpressionKind::Column(right)) =
                (&left.kind, &right.kind)
            else {
                return Ok(());
            };
            let pair = left
                .as_str()
                .strip_prefix("left.")
                .zip(right.as_str().strip_prefix("right."))
                .or_else(|| {
                    right
                        .as_str()
                        .strip_prefix("left.")
                        .zip(left.as_str().strip_prefix("right."))
                });
            if let Some((left, right)) = pair
                && !keys
                    .iter()
                    .any(|key| key.left.as_str() == left && key.right.as_str() == right)
            {
                keys.push(JoinKey {
                    left: ColumnRef::new(left)?,
                    right: ColumnRef::new(right)?,
                });
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn column(name: &str) -> NativeResult<Expression> {
    Ok(Expression::column(
        ExprId::new(format!("native.{name}"))?,
        ColumnRef::new(name)?,
    ))
}

fn join_columns(
    names: &[String],
    side: Side,
    prefix: Option<&str>,
) -> NativeResult<Vec<JoinColumn>> {
    names
        .iter()
        .map(|name| {
            Ok(JoinColumn {
                side,
                column: ColumnRef::new(name.clone())?,
                output_column: prefix
                    .map_or_else(|| name.clone(), |prefix| format!("{prefix}.{name}")),
            })
        })
        .collect()
}

fn order_key(key: &ParsedOrderKey, input: &Lowered) -> NativeResult<OrderKey> {
    Ok(OrderKey {
        column: ColumnRef::new(input.resolve(&key.column)?)?,
        descending: key.direction == SortDirection::Desc,
        nulls: key.null_ordering.map(|nulls| match nulls {
            SortNullOrdering::First => NullOrder::First,
            SortNullOrdering::Last => NullOrder::Last,
        }),
    })
}

fn map_columns(
    expression: &mut Expression,
    map: &mut impl FnMut(&str) -> NativeResult<String>,
) -> NativeResult<()> {
    match &mut expression.kind {
        ExpressionKind::Column(column) => *column = ColumnRef::new(map(column.as_str())?)?,
        ExpressionKind::Alias { expr, .. }
        | ExpressionKind::Cast { expr, .. }
        | ExpressionKind::TryCast { expr, .. }
        | ExpressionKind::Unary { expr, .. } => map_columns(expr, map)?,
        ExpressionKind::Binary { left, right, .. }
        | ExpressionKind::Compare { left, right, .. } => {
            map_columns(left, map)?;
            map_columns(right, map)?;
        }
        ExpressionKind::FunctionCall { args, .. } | ExpressionKind::List { values: args } => {
            for arg in args {
                map_columns(arg, map)?;
            }
        }
        ExpressionKind::Struct { fields } => {
            for (_, field) in fields {
                map_columns(field, map)?;
            }
        }
        ExpressionKind::Literal(_) | ExpressionKind::Unsupported { .. } => {}
    }
    Ok(())
}
