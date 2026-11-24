//! Lexical relation binding over shared syntax nodes. Binding never reads a
//! source or substitutes a query's text into another query.

use super::{
    ParsedAggregate, ParsedAggregateArgument, ParsedPredicate, ParsedRelationIdentity,
    ParsedRelationQuery as Query, ParsedRelationSource as Source, ParsedSqlLocalSource,
    ShardLoomError, WindowFunction, scalar_expression, unsupported_sql_error,
    validate_query_structure,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Default)]
struct Scope(BTreeMap<String, Arc<Query>>);

impl Scope {
    fn new() -> Self {
        Self::default()
    }
}

impl std::ops::Deref for Scope {
    type Target = BTreeMap<String, Arc<Query>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Scope {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        drain_shared(std::mem::take(&mut self.0).into_values());
    }
}

/// Release shared query ownership iteratively, including unused CTEs and
/// declarations rejected after parsing. Owned expression nesting remains under
/// its separate syntax guard.
pub(in super::super) fn drain_query(query: Query) {
    let mut pending = vec![query];
    let scope = Scope::new();
    let names = BTreeSet::new();
    while let Some(mut query) = pending.pop() {
        let mut binder = Binder {
            scope: &scope,
            names: &names,
            visited: BTreeMap::new(),
            discover: Some(Vec::new()),
            recursive: None,
        };
        let inspected = binder.query(&mut query);
        debug_assert!(
            inspected.is_ok(),
            "dependency enumeration cannot bind or reject names"
        );
        let children = binder.discover.take().expect("enumerating dependencies");
        drop(query);
        for child in children {
            if let Ok(child) = Arc::try_unwrap(child) {
                pending.push(child);
            }
        }
    }
}

fn drain_shared(queries: impl IntoIterator<Item = Arc<Query>>) {
    for query in queries {
        if let Ok(query) = Arc::try_unwrap(query) {
            drain_query(query);
        }
    }
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
pub(in super::super) fn bind_inputs(
    query: &mut Query,
    inputs: &[String],
) -> Result<(), ShardLoomError> {
    let mut scope = Scope::new();
    for (index, name) in inputs.iter().enumerate() {
        super::validate_sql_identifier(name)?;
        if scope
            .insert(name.to_ascii_lowercase(), Arc::new(Query::Input(index)))
            .is_some()
        {
            return Err(unsupported_sql_error(
                "relation bindings must be distinct ignoring case",
            ));
        }
    }
    bind(query, &scope, &BTreeSet::new())
}

pub(super) fn parse(statement: &str) -> Result<Query, ShardLoomError> {
    validate_query_structure(statement)?;
    let cte_parts = shardloom_plan::sql_binder::extract_cte_definitions(statement)?;
    let recursive = cte_parts.recursive;
    let names: BTreeSet<String> = cte_parts
        .definitions
        .iter()
        .map(|definition| definition.name.clone())
        .collect();
    let mut scope = Scope::new();
    for definition in cte_parts.definitions {
        let name = definition.name;
        let columns = definition.columns;
        let materialized = definition.materialized;
        let mut query = Query::parse(&definition.body_sql)?;
        let identity = ParsedRelationIdentity::default();
        if recursive {
            let mut step_scope = Scope::new();
            step_scope.extend(
                scope
                    .iter()
                    .map(|(name, query)| (name.clone(), Arc::clone(query))),
            );
            step_scope.insert(
                name.clone(),
                Arc::new(Query::RecursiveInput(identity.clone())),
            );
            bind(&mut query, &step_scope, &names)?;
        } else {
            bind(&mut query, &scope, &names)?;
        }
        let recursive = recursive && references_recursive(&mut query, &identity)?;
        if recursive {
            query = recursive_definition(query, identity, columns)?;
        } else if !columns.is_empty() {
            query = Query::Columns {
                input: Arc::new(query),
                names: columns,
            };
        }
        if materialized && !recursive {
            query = Query::Materialized {
                identity: ParsedRelationIdentity::default(),
                input: Arc::new(query),
            };
        }
        scope.insert(name, Arc::new(query));
    }
    let mut query = Query::parse(&cte_parts.tail_sql)?;
    bind(&mut query, &scope, &names)?;
    Ok(query)
}

fn recursive_definition(
    query: Query,
    identity: ParsedRelationIdentity,
    columns: Vec<String>,
) -> Result<Query, ShardLoomError> {
    let Query::Set(mut set) = query else {
        return Err(unsupported_sql_error(
            "recursive CTE requires a nonrecursive anchor UNION [ALL] one recursive term",
        ));
    };
    if set.branches.len() != 2
        || !matches!(
            set.mode,
            super::SqlUnionMode::All | super::SqlUnionMode::Distinct
        )
        || set.order_by.is_some()
        || set.limit != usize::MAX
        || set.offset != 0
    {
        return Err(unsupported_sql_error(
            "recursive CTE requires exactly two UNION or UNION ALL branches without a global ORDER BY, LIMIT or OFFSET",
        ));
    }
    let mut step = set.branches.pop().expect("two recursive branches");
    let mut anchor = set.branches.pop().expect("two recursive branches");
    // The set parser attaches a synthetic branch limit; recursive evaluation
    // completes both terms and cannot use that limit as a truncation policy.
    for branch in [&mut anchor, &mut step] {
        branch.limit = usize::MAX;
        branch.offset = 0;
        branch.limit_is_synthetic = true;
    }
    let mut anchor = Query::Select(Box::new(anchor));
    if references_recursive(&mut anchor, &identity)? {
        return Err(unsupported_sql_error(
            "recursive anchor must not reference itself",
        ));
    }
    Ok(Query::Recursive {
        identity,
        anchor: Arc::new(anchor),
        step: Arc::new(Query::Select(Box::new(step))),
        columns,
        distinct: set.mode == super::SqlUnionMode::Distinct,
    })
}

fn references_recursive(
    query: &mut Query,
    identity: &ParsedRelationIdentity,
) -> Result<bool, ShardLoomError> {
    let found = std::cell::Cell::new(false);
    let scope = Scope::new();
    let names = BTreeSet::new();
    Binder {
        scope: &scope,
        names: &names,
        visited: BTreeMap::new(),
        discover: None,
        recursive: Some((identity, &found)),
    }
    .query(query)?;
    Ok(found.get())
}

/// Bind only currently unresolved names. A relation inserted from this scope is
/// already a snapshot of its earlier lexical scope and is not rebound here.
fn bind(query: &mut Query, scope: &Scope, names: &BTreeSet<String>) -> Result<(), ShardLoomError> {
    Binder {
        scope,
        names,
        visited: BTreeMap::new(),
        discover: None,
        recursive: None,
    }
    .query(query)
}

struct Binder<'a> {
    scope: &'a Scope,
    names: &'a BTreeSet<String>,
    visited: BTreeMap<usize, Arc<Query>>,
    discover: Option<Vec<Arc<Query>>>,
    recursive: Option<(&'a ParsedRelationIdentity, &'a std::cell::Cell<bool>)>,
}

impl Drop for Binder<'_> {
    fn drop(&mut self) {
        drain_shared(std::mem::take(&mut self.visited).into_values());
    }
}

impl Binder<'_> {
    fn query(&mut self, query: &mut Query) -> Result<(), ShardLoomError> {
        match query {
            #[cfg(all(feature = "vortex-local-primitives", unix))]
            Query::Input(_) => Ok(()),
            Query::Select(select) => self.select(select),
            Query::Set(set) => set
                .branches
                .iter_mut()
                .try_for_each(|branch| self.select(branch)),
            Query::Columns { input, .. } | Query::Materialized { input, .. } => self.shared(input),
            Query::Recursive { anchor, step, .. } => {
                self.shared(anchor)?;
                self.shared(step)
            }
            Query::RecursiveInput(identity) => {
                if let Some((wanted, found)) = self.recursive
                    && wanted == identity
                {
                    found.set(true);
                }
                Ok(())
            }
        }
    }

    fn shared(&mut self, query: &mut Arc<Query>) -> Result<(), ShardLoomError> {
        if let Some(children) = &mut self.discover {
            children.push(Arc::clone(query));
            return Ok(());
        }
        let identity = Arc::as_ptr(query) as usize;
        if let Some(bound) = self.visited.get(&identity) {
            *query = Arc::clone(bound);
            return Ok(());
        }
        let mut pending = vec![(Arc::clone(query), false)];
        let mut originals = Vec::new();
        let mut active = BTreeSet::new();
        while let Some((input, finish)) = pending.pop() {
            let key = Arc::as_ptr(&input) as usize;
            if self.visited.contains_key(&key) {
                continue;
            }
            if finish {
                let mut bound = input.as_ref().clone();
                self.query(&mut bound)?;
                self.visited.insert(key, Arc::new(bound));
                active.remove(&key);
            } else {
                if !active.insert(key) {
                    return Err(unsupported_sql_error(
                        "relation declarations contain a cycle",
                    ));
                }
                originals.push(Arc::clone(&input));
                self.discover = Some(Vec::new());
                self.query(&mut input.as_ref().clone())?;
                let children = self.discover.take().expect("enumerating dependencies");
                pending.push((input, true));
                pending.extend(children.into_iter().rev().map(|input| (input, false)));
            }
        }
        *query = Arc::clone(&self.visited[&identity]);
        // Parent-first release keeps each child's separate owner alive while
        // its parent drops. Very deep lexical references never cascade-drop.
        for original in originals {
            drop(original);
        }
        Ok(())
    }

    fn source(&mut self, source: &mut Source) -> Result<Option<String>, ShardLoomError> {
        match source {
            Source::Local(_) if self.discover.is_some() => {}
            Source::Local(leaf) if leaf.declared_identifier => {
                let name = leaf.path.to_string_lossy().into_owned();
                let canonical = name.to_ascii_lowercase();
                if let Some(query) = self.scope.get(&canonical) {
                    *source = Source::Derived(Arc::clone(query));
                    return Ok(Some(name));
                }
                if self.names.contains(&canonical) {
                    return Err(unsupported_sql_error(
                        "CTE self-reference and forward references are not admitted",
                    ));
                }
            }
            Source::Local(_) => {}
            Source::Derived(query) => self.shared(query)?,
            Source::Unary(unary) => self.shared(&mut unary.input)?,
            Source::GapFill(fill) => self.shared(&mut fill.input)?,
        }
        Ok(None)
    }

    fn select(&mut self, select: &mut ParsedSqlLocalSource) -> Result<(), ShardLoomError> {
        let implicit = self.source(&mut select.source)?;
        if select.source_alias.is_none() {
            select.source_alias = implicit;
        }
        if let Some(join) = &mut select.join {
            self.source(&mut join.right_source)?;
            if let Some(predicate) = &mut join.on_predicate {
                self.predicate(predicate)?;
            }
        }
        self.predicate(&mut select.predicate)?;
        self.predicate(&mut select.having)?;
        for projection in &mut select.conditional_projections {
            self.predicate(&mut projection.predicate)?;
        }
        for projection in &mut select.predicate_projections {
            self.predicate(&mut projection.predicate)?;
        }
        for projection in &mut select.generic_expression_projections {
            self.scalar(&mut projection.expression)?;
        }
        for aggregate in select
            .aggregates
            .iter_mut()
            .chain(&mut select.having_aggregates)
        {
            self.aggregate(aggregate)?;
        }
        for window in &mut select.window_projections {
            match &mut window.function {
                WindowFunction::FirstValue(value)
                | WindowFunction::LastValue(value)
                | WindowFunction::NthValue {
                    expression: value, ..
                }
                | WindowFunction::Fill {
                    expression: value, ..
                } => self.scalar(value)?,
                WindowFunction::Aggregate(aggregate) => self.aggregate(aggregate)?,
                _ => {}
            }
        }
        Ok(())
    }

    fn aggregate(&mut self, value: &mut ParsedAggregate) -> Result<(), ShardLoomError> {
        if let Some(ParsedAggregateArgument::Computed { expression, .. }) = &mut value.filter {
            self.scalar(expression)?;
        }
        for argument in value.arguments_mut() {
            if let ParsedAggregateArgument::Computed { expression, .. } = argument {
                self.scalar(expression)?;
            }
        }
        Ok(())
    }

    fn scalar(
        &mut self,
        value: &mut scalar_expression::ParsedScalarExpression,
    ) -> Result<(), ShardLoomError> {
        for (_, binding) in &mut value.bindings {
            match binding {
                scalar_expression::RelationalBinding::Parameter(_) => {}
                scalar_expression::RelationalBinding::Scalar(query) => self.query(query)?,
                scalar_expression::RelationalBinding::Predicate(predicate) => {
                    self.predicate(predicate)?;
                }
            }
        }
        Ok(())
    }

    fn predicate(&mut self, predicate: &mut ParsedPredicate) -> Result<(), ShardLoomError> {
        let (source, qualifier, filter, projected) = match predicate {
            ParsedPredicate::GenericExpressionCompare { left, right, .. } => {
                self.scalar(left)?;
                return self.scalar(right);
            }
            ParsedPredicate::Logical { left, right, .. } => {
                self.predicate(left)?;
                return self.predicate(right);
            }
            ParsedPredicate::Not { inner } => return self.predicate(inner),
            ParsedPredicate::InSubquery { subquery, .. }
            | ParsedPredicate::QuantifiedSubquery { subquery, .. } => (
                &mut subquery.source,
                &mut subquery.source_qualifier,
                subquery.predicate.as_mut(),
                subquery.projected_plan.as_deref_mut(),
            ),
            ParsedPredicate::RowValueInSubquery { subquery, .. } => (
                &mut subquery.source,
                &mut subquery.source_qualifier,
                subquery.predicate.as_mut(),
                subquery.projected_plan.as_deref_mut(),
            ),
            ParsedPredicate::ExistsSubquery { subquery } => (
                &mut subquery.source,
                &mut subquery.source_qualifier,
                subquery.predicate.as_mut(),
                subquery.projected_plan.as_deref_mut(),
            ),
            _ => return Ok(()),
        };
        let implicit = self.source(source)?;
        if qualifier.is_none() {
            *qualifier = implicit;
        }
        self.predicate(filter)?;
        if let Some(select) = projected {
            self.select(select)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_names_share_the_same_inert_query_declaration() {
        let Query::Select(query) = Query::parse(
            "WITH value AS (SELECT 1 AS id) SELECT a.id FROM value AS a JOIN value AS b ON a.id=b.id"
        ).unwrap() else { panic!("select") };
        let Source::Derived(left) = &query.source else {
            panic!("left relation")
        };
        let Source::Derived(right) = &query.join.as_ref().unwrap().right_source else {
            panic!("right relation")
        };
        assert!(Arc::ptr_eq(left, right));
        assert!(
            query
                .normalized_statement
                .contains("FROM value AS a JOIN value AS b")
        );
        assert!(!query.normalized_statement.contains("SELECT 1 AS id"));
    }
}
