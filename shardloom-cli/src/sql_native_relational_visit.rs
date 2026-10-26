//! Iterative syntax traversal. A shared declaration is inspected once; its
//! occurrence count is propagated separately from its retained structure.

use super::{
    BTreeMap, BTreeSet, NativeResult, ParsedPredicate, ParsedRelationLeaf,
    ParsedRelationQuery as Query, ParsedRelationSource as Source, ParsedSqlLocalSource as Select,
    scalar, scalar_expression, unsupported_sql_error,
};
use shardloom_exec::compute_pool::CancellationToken;

#[derive(Clone, Copy)]
pub(super) enum Node<'a> {
    Query(&'a Query),
    Select(&'a Select),
    Source(&'a Source),
    Predicate(&'a ParsedPredicate),
    Scalar(&'a scalar_expression::ParsedScalarExpression),
}

type Key = (u8, usize);

impl<'a> Node<'a> {
    fn key(self) -> Key {
        match self {
            Self::Query(value) => (0, std::ptr::from_ref(value) as usize),
            Self::Select(value) => (1, std::ptr::from_ref(value) as usize),
            Self::Source(value) => (2, std::ptr::from_ref(value) as usize),
            Self::Predicate(value) => (3, std::ptr::from_ref(value) as usize),
            Self::Scalar(value) => (4, std::ptr::from_ref(value) as usize),
        }
    }

    fn children(self, pending: &mut Vec<Self>) {
        match self {
            Self::Query(Query::Input(_)) | Self::Source(Source::Local(_)) => {}
            Self::Query(Query::Select(select)) => pending.push(Self::Select(select)),
            Self::Query(Query::Set(set)) => {
                pending.extend(set.branches.iter().map(Self::Select));
            }
            Self::Query(Query::Columns { input, .. }) => pending.push(Self::Query(input)),
            Self::Select(select) => {
                pending.push(Self::Source(&select.source));
                if let Some(join) = &select.join {
                    pending.push(Self::Source(&join.right_source));
                }
                pending.extend(select.predicate_surfaces().into_iter().map(Self::Predicate));
                pending.extend(scalar::surfaces(select).into_iter().map(Self::Scalar));
            }
            Self::Source(Source::Derived(query)) => pending.push(Self::Query(query)),
            Self::Source(Source::Unary(unary)) => pending.push(Self::Query(&unary.input)),
            Self::Scalar(value) => {
                pending.extend(
                    value
                        .bindings
                        .iter()
                        .filter_map(|(_, binding)| match binding {
                            scalar_expression::RelationalBinding::Parameter(_) => None,
                            scalar_expression::RelationalBinding::Scalar(query) => {
                                Some(Self::Query(query))
                            }
                            scalar_expression::RelationalBinding::Predicate(value) => {
                                Some(Self::Predicate(value))
                            }
                        }),
                );
            }
            Self::Predicate(predicate) => {
                let (source, filter, projected) = match predicate {
                    ParsedPredicate::GenericExpressionCompare { left, right, .. } => {
                        pending.push(Self::Scalar(left));
                        pending.push(Self::Scalar(right));
                        return;
                    }
                    ParsedPredicate::Logical { left, right, .. } => {
                        pending.push(Self::Predicate(left));
                        pending.push(Self::Predicate(right));
                        return;
                    }
                    ParsedPredicate::Not { inner } => {
                        pending.push(Self::Predicate(inner));
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
                pending.push(Self::Source(source));
                pending.push(Self::Predicate(filter));
                if let Some(select) = projected {
                    pending.push(Self::Select(select));
                }
            }
        }
    }

    pub(super) fn walk(self) -> Walk<'a> {
        Walk {
            pending: vec![self],
            visited: BTreeSet::new(),
        }
    }

    pub(super) fn sources(self, paths: &mut BTreeSet<ParsedRelationLeaf>) {
        for node in self.walk() {
            if let Self::Source(Source::Local(leaf)) = node {
                paths.insert(leaf.clone());
            }
        }
    }

    pub(super) fn dynamic(self) -> bool {
        self.dynamic_bound(&super::BoundRelations::new())
    }

    pub(super) fn dynamic_bound(self, inputs: &super::BoundRelations) -> bool {
        self.walk().any(|node| {
            matches!(node,
            Self::Source(Source::Unary(unary))
                if unary.request.kind == shardloom_vortex::VortexQueryPrimitiveKind::PivotRows)
                || matches!(node, Self::Query(Query::Input(index)) if inputs.get(index).is_some_and(|input| input.dynamic))
        })
    }

    pub(super) fn sources_bound(
        self,
        inputs: &super::BoundRelations,
        paths: &mut BTreeSet<ParsedRelationLeaf>,
    ) {
        for node in self.walk() {
            match node {
                Self::Source(Source::Local(leaf)) => {
                    paths.insert(leaf.clone());
                }
                Self::Query(Query::Input(index)) => {
                    if let Some(input) = inputs.get(index) {
                        paths.extend(input.sources.keys().cloned());
                    }
                }
                _ => {}
            }
        }
    }

    fn local_bytes(self) -> NativeResult<usize> {
        let bytes = match self {
            Self::Select(select) => select
                .normalized_statement
                .len()
                .checked_mul(1024)
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Select>() * 16)),
            Self::Query(Query::Columns { names, .. }) => {
                names.iter().try_fold(4096usize, |bytes, name| {
                    name.len()
                        .checked_mul(8)
                        .and_then(|extra| bytes.checked_add(extra))
                })
            }
            _ => Some(256),
        };
        bytes.ok_or_else(|| unsupported_sql_error("SQL declaration metadata size overflow"))
    }
}

pub(super) struct Walk<'a> {
    pending: Vec<Node<'a>>,
    visited: BTreeSet<Key>,
}

impl<'a> Iterator for Walk<'a> {
    type Item = Node<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = self.pending.pop() {
            if self.visited.insert(node.key()) {
                node.children(&mut self.pending);
                return Some(node);
            }
        }
        None
    }
}

/// Credit includes the current lowerer's per-consumer operator descriptions.
/// Compute that work arithmetically over the DAG before building any copy or
/// resolving a source. Overflow is an admission failure, never tree expansion.
pub(super) struct Admission<'a> {
    pub(super) bytes: usize,
    pub(super) source_uses: BTreeMap<&'a ParsedRelationLeaf, u8>,
    pub(super) input_uses: BTreeMap<usize, usize>,
    pub(super) query_depth: usize,
    pub(super) recursive_depth: usize,
}

impl<'a> Admission<'a> {
    pub(super) fn inspect(
        query: &'a Query,
        cancellation: &CancellationToken,
    ) -> NativeResult<Self> {
        let root = Node::Query(query);
        let mut pending = vec![(root, false)];
        let mut states = BTreeMap::<Key, bool>::new();
        let mut sizes = BTreeMap::<Key, usize>::new();
        let mut depths = BTreeMap::<Key, usize>::new();
        let mut recursive_depths = BTreeMap::<Key, usize>::new();
        let mut order = Vec::new();
        let mut children = Vec::new();
        while let Some((node, finish)) = pending.pop() {
            cancellation.check()?;
            let key = node.key();
            if finish {
                children.clear();
                node.children(&mut children);
                let bytes = children
                    .iter()
                    .try_fold(node.local_bytes()?, |bytes, child| {
                        bytes
                            .checked_add(*sizes.get(&child.key()).ok_or_else(|| {
                                unsupported_sql_error("SQL declaration dependency was not admitted")
                            })?)
                            .ok_or_else(|| {
                                unsupported_sql_error("SQL declaration metadata size overflow")
                            })
                    })?;
                sizes.insert(key, bytes);
                let depth = children
                    .iter()
                    .map(|child| depths[&child.key()])
                    .max()
                    .unwrap_or(0)
                    + usize::from(matches!(node, Node::Query(_)));
                depths.insert(key, depth);
                recursive_depths.insert(
                    key,
                    children
                        .iter()
                        .map(|child| recursive_depths[&child.key()])
                        .max()
                        .unwrap_or(0)
                        .checked_add(node.recursive_depth())
                        .ok_or_else(|| unsupported_sql_error("SQL declaration depth overflow"))?,
                );
                states.insert(key, true);
                order.push(node);
            } else {
                match states.get(&key) {
                    Some(true) => continue,
                    Some(false) => {
                        return Err(unsupported_sql_error("SQL declaration contains a cycle"));
                    }
                    None => {}
                }
                states.insert(key, false);
                pending.push((node, true));
                children.clear();
                node.children(&mut children);
                pending.extend(children.iter().map(|child| (*child, false)));
            }
        }
        let bytes = sizes[&root.key()]
            .checked_add(65_536)
            .ok_or_else(|| unsupported_sql_error("SQL declaration metadata size overflow"))?;
        // Count execution occurrences without walking repeated subtrees. Two is
        // sufficient to declare replay; greater multiplicity does not alter it.
        let mut weights = BTreeMap::from([(root.key(), 1usize)]);
        let mut source_uses = BTreeMap::<&ParsedRelationLeaf, u8>::new();
        let mut input_uses = BTreeMap::<usize, usize>::new();
        for node in order.into_iter().rev() {
            cancellation.check()?;
            let weight = weights.get(&node.key()).copied().unwrap_or(0);
            if let Node::Source(Source::Local(leaf)) = node {
                let uses = source_uses.entry(leaf).or_default();
                *uses = (*uses + u8::try_from(weight.min(2)).expect("capped at two")).min(2);
            }
            if let Node::Query(Query::Input(index)) = node {
                let count = input_uses.entry(*index).or_default();
                *count = count
                    .checked_add(weight)
                    .ok_or_else(|| unsupported_sql_error("SQL relation multiplicity overflow"))?;
            }
            children.clear();
            node.children(&mut children);
            for child in &children {
                let count = weights.entry(child.key()).or_default();
                *count = count
                    .checked_add(weight)
                    .ok_or_else(|| unsupported_sql_error("SQL relation multiplicity overflow"))?;
            }
        }
        Ok(Self {
            bytes,
            source_uses,
            input_uses,
            query_depth: depths[&root.key()],
            recursive_depth: recursive_depths[&root.key()],
        })
    }
}

impl Node<'_> {
    fn recursive_depth(self) -> usize {
        match self {
            Self::Select(select) => {
                usize::from(select.join.is_some())
                    + usize::from(
                        !select.aggregates.is_empty()
                            || !select.group_by.is_empty()
                            || !select.having_aggregates.is_empty(),
                    )
                    + usize::from(!select.window_projections.is_empty())
                    + usize::from(select.order_by.is_some())
            }
            Self::Query(Query::Set(_)) | Self::Source(Source::Unary(_)) => 1,
            Self::Predicate(value) => usize::from(super::predicate::has_subquery(value)),
            Self::Scalar(value) => usize::from(value.bindings.iter().any(|(_, binding)| {
                !matches!(binding, scalar_expression::RelationalBinding::Parameter(_))
            })),
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repeated_query(depth: usize) -> String {
        let mut definitions = vec!["q0 AS (SELECT 1 AS value)".to_owned()];
        for index in 1..=depth {
            definitions.push(format!(
                "q{index} AS (SELECT a.value FROM q{} AS a CROSS JOIN q{} AS b)",
                index - 1,
                index - 1,
            ));
        }
        format!("WITH {} SELECT value FROM q{depth}", definitions.join(","))
    }

    #[test]
    fn shared_cte_inspection_is_linear_and_records_replay_multiplicity() {
        let query = super::super::parsed_native_query(&repeated_query(8)).unwrap();
        let admission = Admission::inspect(&query, &CancellationToken::default()).unwrap();
        assert_eq!(admission.source_uses.len(), 1);
        assert_eq!(
            admission.source_uses.values().copied().collect::<Vec<_>>(),
            [2]
        );
        // The unexpanded graph remains small even when the execution expression
        // has exponentially many occurrences. No source or row is evaluated.
        let wide = super::super::parsed_native_query(&repeated_query(64)).unwrap();
        assert!(Node::Query(&wide).walk().count() < 1024);
        assert!(!Node::Query(&wide).dynamic());
        assert_eq!(super::super::source_count(&repeated_query(64)).unwrap(), 1);
        let error = Admission::inspect(&wide, &CancellationToken::default())
            .err()
            .expect("expanded metadata cannot fit usize");
        assert!(
            error.to_string().contains("metadata size overflow"),
            "{error}"
        );
    }

    #[test]
    fn cte_metadata_denial_precedes_sources_and_releases_its_shared_credit() {
        use shardloom_vortex::{
            VortexLocalPrimitiveExecutionPolicy, resident_session::ResidentVortexSession,
        };
        let mut policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 2).unwrap();
        policy.resource_envelope.memory_budget_bytes = 32 << 20;
        let session = ResidentVortexSession::new(32 << 20, 2).unwrap();
        let before = session.shared_memory_pool().snapshot().reserved_bytes;
        let result = super::super::prepare_in_session(
            &repeated_query(20),
            policy,
            &session,
            |_| panic!("input registration preceded metadata admission"),
            |_, _| panic!("source resolution preceded metadata admission"),
            &CancellationToken::default(),
        );
        let error = result
            .err()
            .expect("expanded lowering exceeds the same shared grant");
        assert!(
            error.to_string().contains("memory reservation denied"),
            "{error}"
        );
        assert_eq!(
            session.shared_memory_pool().snapshot().reserved_bytes,
            before
        );
        assert_eq!(session.snapshot().prepared_source_opens, 0);
    }
}
