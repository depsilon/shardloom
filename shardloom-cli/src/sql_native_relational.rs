//! Inert SQL parsing lowers into the shared prepared native relational executor.
//! The decoded reference preparation/evaluator is never called from this module.

pub(crate) use super::relation_sources::ParsedRelationLeaf;
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
        VortexRelationalSubqueryKind as SubqueryKind, VortexRelationalUnary as Unary,
        VortexRelationalWindow as Window, VortexRelationalWindowExpression as WindowExpression,
        VortexRelationalWindowFunction as NativeWindowFunction,
    },
};

#[path = "sql_native_cte.rs"]
mod cte;
#[path = "sql_native_relational_dynamic.rs"]
mod dynamic;
#[path = "sql_native_relational_predicate.rs"]
mod predicate;
#[path = "sql_native_relational_projection.rs"]
mod projection;
#[path = "sql_native_relational_pruning.rs"]
mod pruning;
#[cfg(test)]
#[path = "sql_native_relational_tests.rs"]
mod tests;
#[path = "sql_native_relational_unary.rs"]
mod unary;

type NativeResult<T> = Result<T, ShardLoomError>;

#[cfg(test)]
pub(crate) fn prepare(
    raw: &str,
    policy: VortexLocalPrimitiveExecutionPolicy,
    mut resolve_source: impl FnMut(&ParsedRelationLeaf) -> NativeResult<DatasetUri>,
) -> NativeResult<PreparedVortexRelational> {
    prepare_with_inputs(
        raw,
        policy,
        |_| Ok(()),
        |leaf| resolve_source(leaf).map(|uri| vec![uri]),
    )
}

pub(crate) fn prepare_with_inputs(
    raw: &str,
    policy: VortexLocalPrimitiveExecutionPolicy,
    inputs: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> NativeResult<()>,
    mut resolve_source: impl FnMut(&ParsedRelationLeaf) -> NativeResult<Vec<DatasetUri>>,
) -> NativeResult<PreparedVortexRelational> {
    let (parsed, offset) = parsed_native_query(raw)?;
    if dynamic::required(&parsed) {
        if offset != 0 {
            return Err(unsupported_sql_error(
                "dynamic native SQL does not admit a trailing OFFSET",
            ));
        }
        return dynamic::prepare(parsed, policy, inputs, &mut resolve_source);
    }
    prepare_relational_with_schema(policy, |schemas| {
        register_sql_memory_inputs(&parsed, schemas)?;
        inputs(schemas)?;
        let mut lowerer = Lowerer {
            schemas,
            serial: 0,
            resolve_source: &mut resolve_source,
            declaration: None,
            outer: None,
        };
        let mut query = lowerer.query(&parsed)?.offset(offset)?;
        lowerer.prune(&mut query.plan, None, &mut BTreeSet::new())?;
        Ok(query.plan)
    })
}

/// Shape discovery is syntax-only. File/type/resource admission happens at prepare.
pub(crate) fn is_relational(raw: &str) -> NativeResult<bool> {
    let (parsed, _) = parsed_native_query(raw)?;
    let ParsedRelationQuery::Select(parsed) = parsed else {
        return Ok(true);
    };
    Ok(matches!(
        parsed.source,
        ParsedRelationSource::Derived(_) | ParsedRelationSource::Unary(_)
    ) || matches!(&parsed.source, ParsedRelationSource::Local(leaf) if leaf.memory_input.is_some())
        || parsed.replace_or_add_projection
        || parsed.join.is_some()
        || !parsed.window_projections.is_empty()
        || parsed
            .predicate_surfaces()
            .into_iter()
            .any(predicate::has_subquery))
}

/// Ordinary local SELECT discovery is inert, including on absent source paths.
pub(crate) fn is_plain_select(raw: &str) -> NativeResult<bool> {
    let (parsed, _) = parsed_native_query(raw)?;
    Ok(matches!(parsed,
        ParsedRelationQuery::Select(parsed) if matches!(parsed.source, ParsedRelationSource::Local(_))))
}

pub(crate) fn prepare_from_source(
    raw: &str,
    policy: VortexLocalPrimitiveExecutionPolicy,
    uri: DatasetUri,
    source: shardloom_vortex::resident_session::PreparedVortexSource,
    inputs: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> NativeResult<()>,
    mut resolve_source: impl FnMut(&ParsedRelationLeaf) -> NativeResult<Vec<DatasetUri>>,
) -> NativeResult<PreparedVortexRelational> {
    let (parsed, offset) = parsed_native_query(raw)?;
    if dynamic::required(&parsed) {
        return Err(unsupported_sql_error(
            "ordinary source dispatch cannot discover a dynamic schema",
        ));
    }
    shardloom_vortex::local_primitives::prepared_relational::prepare_relational_from_source(
        uri,
        source,
        policy,
        |schemas| {
            register_sql_memory_inputs(&parsed, schemas)?;
            inputs(schemas)?;
            let mut lowerer = Lowerer {
                schemas,
                serial: 0,
                resolve_source: &mut resolve_source,
                declaration: None,
                outer: None,
            };
            let mut query = lowerer.query(&parsed)?.offset(offset)?;
            lowerer.prune(&mut query.plan, None, &mut BTreeSet::new())?;
            Ok(query.plan)
        },
    )
}

/// Count unique declared paths without opening inputs or preparing subqueries.
pub(crate) fn source_count(raw: &str) -> NativeResult<usize> {
    Ok(source_leaves(raw)?
        .into_iter()
        .map(|leaf| leaf.path)
        .collect::<BTreeSet<_>>()
        .len())
}

pub(crate) fn source_leaves(raw: &str) -> NativeResult<BTreeSet<ParsedRelationLeaf>> {
    let (parsed, _) = parsed_native_query(raw)?;
    let mut paths = BTreeSet::new();
    declared_query_sources(&parsed, &mut paths);
    Ok(paths)
}

fn declared_query_sources(query: &ParsedRelationQuery, paths: &mut BTreeSet<ParsedRelationLeaf>) {
    match query {
        ParsedRelationQuery::Select(parsed) => declared_sources(parsed, paths),
        ParsedRelationQuery::Set(set) => {
            for branch in &set.branches {
                declared_sources(branch, paths);
            }
        }
    }
}

fn declared_relation_sources(
    source: &ParsedRelationSource,
    paths: &mut BTreeSet<ParsedRelationLeaf>,
) {
    match source {
        ParsedRelationSource::Local(path) => {
            paths.insert(path.clone());
        }
        ParsedRelationSource::Derived(query) => declared_query_sources(query, paths),
        ParsedRelationSource::Unary(operation) => declared_query_sources(&operation.input, paths),
    }
}

fn declared_sources(parsed: &ParsedSqlLocalSource, paths: &mut BTreeSet<ParsedRelationLeaf>) {
    declared_relation_sources(&parsed.source, paths);
    if let Some(join) = &parsed.join {
        declared_relation_sources(&join.right_source, paths);
    }
    for predicate in parsed.predicate_surfaces() {
        predicate::declared_sources(predicate, paths);
    }
}

fn register_sql_memory_inputs(
    parsed: &ParsedRelationQuery,
    schemas: &mut VortexRelationalPreparation<'_>,
) -> NativeResult<()> {
    let mut leaves = BTreeSet::new();
    declared_query_sources(parsed, &mut leaves);
    for leaf in leaves {
        if let Some(input) = leaf.memory_input {
            schemas.register_memory_source(
                DatasetUri::new(leaf.path.to_string_lossy())?,
                |session| input.build(session),
            )?;
        }
    }
    Ok(())
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
    let mut statement = cte::expand(normalize_sql_statement(raw)?)?;
    if top_level_keyword_indexes(&statement, "limit")?.is_empty() {
        write!(&mut statement, " LIMIT {}", usize::MAX).expect("String writes cannot fail");
    }
    Ok(statement)
}

fn parsed_native_query(raw: &str) -> NativeResult<(ParsedRelationQuery, usize)> {
    let statement = admitted_statement(raw)?;
    let limit = top_level_keyword_indexes(&statement, "limit")?
        .last()
        .copied()
        .ok_or_else(|| unsupported_sql_error("native SQL limit is absent"))?;
    let offsets = top_level_keyword_indexes(&statement, "offset")?
        .into_iter()
        .filter(|index| *index > limit)
        .collect::<Vec<_>>();
    match offsets.as_slice() {
        [] => Ok((ParsedRelationQuery::parse(&statement)?, 0)),
        [index] => {
            let offset = parse_limit(statement[index + "offset".len()..].trim())?;
            Ok((
                ParsedRelationQuery::parse(statement[..*index].trim())?,
                offset,
            ))
        }
        _ => Err(unsupported_sql_error(
            "native SQL admits one trailing OFFSET",
        )),
    }
}

struct Lowered {
    plan: Plan,
    columns: Vec<String>,
    qualifiers: BTreeMap<String, String>,
}

impl Lowered {
    fn offset(mut self, offset: usize) -> NativeResult<Self> {
        if offset != 0 {
            let Plan::Limit(limit) = &mut self.plan else {
                return Err(unsupported_sql_error(
                    "native SQL OFFSET requires an outer LIMIT",
                ));
            };
            limit.offset = offset;
        }
        Ok(self)
    }

    fn resolve(&self, name: &str) -> NativeResult<String> {
        if self.columns.iter().any(|column| column == name) {
            return Ok(name.to_owned());
        }
        if let Some(column) = self.qualifiers.get(name)
            && self.columns.contains(column)
        {
            return Ok(column.clone());
        }
        if !name.contains('.') {
            let mut matches = self.columns.iter().filter(|column| {
                column
                    .rsplit_once('.')
                    .is_some_and(|(_, suffix)| suffix == name)
            });
            if let Some(column) = matches.next() {
                if matches.next().is_some() {
                    return Err(unsupported_sql_error(&format!(
                        "column {name:?} is ambiguous; qualify it with a source alias"
                    )));
                }
                return Ok(column.clone());
            }
        }
        Err(unsupported_sql_error(&format!(
            "column {name:?} is not present in the native input schema"
        )))
    }

    fn project(self, expressions: Vec<(String, Expression)>) -> Self {
        Self {
            columns: expressions.iter().map(|(name, _)| name.clone()).collect(),
            qualifiers: BTreeMap::new(),
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
    resolve_source: &'a mut dyn FnMut(&ParsedRelationLeaf) -> NativeResult<Vec<DatasetUri>>,
    declaration: Option<dynamic::Declaration>,
    outer: Option<Vec<String>>,
}

impl Lowerer<'_, '_> {
    fn query(&mut self, query: &ParsedRelationQuery) -> NativeResult<Lowered> {
        let parsed = match query {
            ParsedRelationQuery::Select(parsed) => {
                return self.select(parsed, None, !parsed.limit_is_synthetic);
            }
            ParsedRelationQuery::Set(parsed) => parsed,
        };
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

    fn scan(&mut self, leaf: &ParsedRelationLeaf) -> NativeResult<Lowered> {
        let sources = if leaf.memory_input.is_some() {
            vec![DatasetUri::new(leaf.path.to_string_lossy())?]
        } else {
            (self.resolve_source)(leaf)?
        };
        let source_uri = sources
            .first()
            .ok_or_else(|| unsupported_sql_error("native source has no files"))?;
        let columns = if matches!(
            &leaf.memory_input,
            Some(crate::native_memory_input::MemoryInput::Unit)
        ) {
            Vec::new()
        } else {
            self.schemas.source_columns(source_uri)?
        };
        for uri in sources.iter().skip(1) {
            if self.schemas.source_columns(uri)? != columns {
                return Err(unsupported_sql_error(
                    "native source parts have different ordered columns",
                ));
            }
        }
        Ok(Lowered {
            plan: native_source_plan(&sources)?,
            columns,
            qualifiers: BTreeMap::new(),
        })
    }

    fn relation(&mut self, source: &ParsedRelationSource) -> NativeResult<Lowered> {
        match source {
            ParsedRelationSource::Local(path) => self.scan(path),
            ParsedRelationSource::Derived(query) => self.query(query),
            ParsedRelationSource::Unary(operation) => {
                let input = self.query(&operation.input)?;
                let request = unary::resolve(operation.request.clone(), &input)?;
                if request.kind == shardloom_vortex::VortexQueryPrimitiveKind::PivotRows {
                    let mut plan = Plan::Unary(Box::new(Unary {
                        input: input.plan,
                        request,
                    }));
                    self.prune(&mut plan, None, &mut BTreeSet::new())?;
                    let (plan, columns) = self.schemas.resolve_output(&plan)?;
                    return Ok(Lowered {
                        plan,
                        columns,
                        qualifiers: BTreeMap::new(),
                    });
                }
                let selected = match &request.projection {
                    ProjectionRequest::All => input.columns.clone(),
                    ProjectionRequest::Columns(columns) => columns
                        .iter()
                        .map(|column| input.resolve(column.as_str()))
                        .collect::<NativeResult<Vec<_>>>()?,
                };
                let columns = if let Some(melt) = &request.melt_projection {
                    melt.output_columns()
                } else if let Some(rolling) = &request.rolling_window {
                    rolling.output_columns()
                } else if let Some(rewrites) = &request.expression_projection {
                    rewrites.output_columns(&selected)
                } else if let Some(explode) = &request.explode_projection {
                    explode.output_columns(&selected)
                } else if request.kind
                    == shardloom_vortex::VortexQueryPrimitiveKind::DuplicateMaskRows
                {
                    vec!["duplicated".into()]
                } else {
                    selected
                };
                Ok(Lowered {
                    plan: Plan::Unary(Box::new(Unary {
                        input: input.plan,
                        request,
                    })),
                    columns,
                    qualifiers: BTreeMap::new(),
                })
            }
        }
    }

    fn source(&mut self, parsed: &ParsedSqlLocalSource) -> NativeResult<Lowered> {
        let mut left = self.relation(&parsed.source)?;
        let Some(join) = &parsed.join else {
            // A derived source establishes a fresh alias scope. Its output names
            // stay unchanged, so SELECT * and unqualified references agree.
            left.qualifiers.clear();
            if let Some(alias) = &parsed.source_alias {
                left.qualifiers = left
                    .columns
                    .iter()
                    .map(|name| (format!("{alias}.{name}"), name.clone()))
                    .collect();
            }
            return Ok(left);
        };
        let right = self.relation(&join.right_source)?;
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
            qualifiers: BTreeMap::new(),
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
        let outer = outer
            .or_else(|| {
                self.outer
                    .as_deref()
                    .filter(|_| predicate::select_direct_outer(parsed))
            })
            .map(<[String]>::to_vec);
        let input = Self::with_outer(input, outer.as_deref())?;
        let mut input = self.filter(input, &parsed.predicate)?;
        let aggregate = !parsed.aggregates.is_empty()
            || !parsed.group_by.is_empty()
            || !parsed.having_aggregates.is_empty();
        if aggregate && parsed.replace_or_add_projection {
            return Err(unsupported_sql_error(
                "REPLACE OR ADD must follow aggregation in a derived relation",
            ));
        }
        let mut grouped_projection = None;
        if aggregate {
            let (aggregated, aliases) = self.aggregate(input, parsed)?;
            input = aggregated;
            // An outer row is a constant parameter for this execution. Grouping
            // discards non-key input fields, so expose that same parameter again
            // before HAVING, including the one row of an empty scalar aggregate.
            if let Some(outer) = &outer {
                let missing = outer
                    .iter()
                    .filter(|name| {
                        !input
                            .columns
                            .iter()
                            .any(|column| column == &format!("outer.{name}"))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if !missing.is_empty() {
                    input = Self::with_outer(input, Some(&missing))?;
                }
            }
            input = self.filter(input, &parsed.having)?;
            if !aliases.is_empty() {
                let mut rewritten = parsed.clone();
                for output in &mut rewritten.projection_order {
                    if let Some(alias) = output.computed_alias()
                        && aliases.iter().any(|name| name == alias)
                    {
                        *output = ParsedProjectionOutput::Raw(alias.to_owned());
                    }
                }
                grouped_projection = Some(rewritten);
            }
        } else if !parsed.having.is_all() {
            return Err(unsupported_sql_error(
                "HAVING requires grouped or scalar aggregation",
            ));
        }
        let parsed = grouped_projection.as_ref().unwrap_or(parsed);
        input = self.windows(input, &parsed.window_projections)?;
        input = self.projection(input, parsed, &visible)?;
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

    fn group_aliases(
        mut input: Lowered,
        parsed: &ParsedSqlLocalSource,
    ) -> NativeResult<(Lowered, Vec<String>)> {
        let mut aliases = Vec::new();
        let mut keys = Vec::new();
        for name in &parsed.group_by {
            if input.qualifiers.contains_key(name)
                || input.columns.iter().any(|column| {
                    column == name
                        || (!name.contains('.')
                            && column
                                .rsplit_once('.')
                                .is_some_and(|(_, suffix)| suffix == name))
                })
            {
                input.resolve(name)?;
                continue;
            }
            let output = parsed
                .projection_order
                .iter()
                .find(|output| output.computed_alias() == Some(name))
                .ok_or_else(|| {
                    unsupported_sql_error(
                        "GROUP BY key is neither an input column nor a SELECT alias",
                    )
                })?;
            if matches!(
                output,
                ParsedProjectionOutput::Aggregate(_) | ParsedProjectionOutput::Window(_)
            ) {
                return Err(unsupported_sql_error(
                    "GROUP BY cannot reference an aggregate or window alias",
                ));
            }
            let mut expressions = Vec::new();
            append_ordered_projection_expression(
                &mut expressions,
                parsed,
                output,
                &input.columns,
                "native.group",
            )?;
            let mut expression = expressions
                .pop()
                .ok_or_else(|| unsupported_sql_error("GROUP BY alias expression is absent"))?;
            map_columns(&mut expression, &mut |column| input.resolve(column))?;
            aliases.push(name.clone());
            keys.push((name.clone(), expression));
        }
        if !keys.is_empty() {
            let mut expressions = input
                .columns
                .iter()
                .map(|name| Ok((name.clone(), column(name)?)))
                .collect::<NativeResult<Vec<_>>>()?;
            expressions.extend(keys);
            input.columns.extend(aliases.iter().cloned());
            input.plan = Plan::Project(Box::new(Project {
                input: input.plan,
                expressions,
            }));
        }
        Ok((input, aliases))
    }

    fn aggregate(
        &mut self,
        input: Lowered,
        parsed: &ParsedSqlLocalSource,
    ) -> NativeResult<(Lowered, Vec<String>)> {
        let (mut input, aliases) = Self::group_aliases(input, parsed)?;
        let group_by = parsed
            .group_by
            .iter()
            .map(|name| ColumnRef::new(input.resolve(name)?))
            .collect::<NativeResult<Vec<_>>>()?;
        let mut retained = group_by
            .iter()
            .map(|column| column.as_str().to_owned())
            .collect::<BTreeSet<_>>();
        for aggregate in parsed.aggregates.iter().chain(&parsed.having_aggregates) {
            if let ParsedAggregateArgument::Column(name) = &aggregate.argument {
                retained.insert(input.resolve(name)?);
            }
        }
        let mut projection = Vec::new();
        let mut names = input.columns.clone();
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
                let argument = match &aggregate.argument {
                    ParsedAggregateArgument::All => None,
                    ParsedAggregateArgument::Column(name) => {
                        Some(ColumnRef::new(input.resolve(name)?)?)
                    }
                    ParsedAggregateArgument::Computed { expression, .. } => {
                        if projection.is_empty() {
                            projection = input
                                .columns
                                .iter()
                                .filter(|name| retained.contains(*name))
                                .map(|name| Ok((name.clone(), column(name)?)))
                                .collect::<NativeResult<Vec<_>>>()?;
                        }
                        let mut expression = (**expression).clone();
                        map_columns(&mut expression, &mut |name| input.resolve(name))?;
                        let name = self.fresh(&names);
                        names.push(name.clone());
                        projection.push((name.clone(), expression));
                        Some(ColumnRef::new(name)?)
                    }
                };
                Ok(VortexSimpleAggregateMeasure::new(
                    if aggregate.distinct {
                        "count_distinct"
                    } else {
                        aggregate.function.as_str()
                    },
                    argument,
                    aggregate.output_name(),
                ))
            })
            .collect::<NativeResult<Vec<_>>>()?;
        if !projection.is_empty() {
            // These private arguments do not change the SQL qualification of
            // group keys. Retain only columns the reduction itself consumes.
            input.plan = Plan::Project(Box::new(Project {
                input: input.plan,
                expressions: projection,
            }));
        }
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
        Ok((input, aliases))
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

/// A file collection is one relation. Shared set/scan nodes keep all downstream
/// filtering, ordering, aggregation and writers independent of source shape.
fn native_source_plan(sources: &[DatasetUri]) -> NativeResult<Plan> {
    match sources {
        [] => Err(unsupported_sql_error("native source has no files")),
        [source_uri] => Ok(Plan::Scan(Scan {
            source_uri: source_uri.clone(),
            projection: ProjectionRequest::All,
            predicate: None,
        })),
        _ => {
            let (left, right) = sources.split_at(sources.len() / 2);
            Ok(Plan::Set(Box::new(Set {
                left: native_source_plan(left)?,
                right: native_source_plan(right)?,
                kind: SetKind::UnionAll,
            })))
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
