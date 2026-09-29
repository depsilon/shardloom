//! Deferred exact DISTINCT over winners selected solely by complete ordinary measures.
//! The typed chunk-group loop, numeric kernels and final comparator remain shared.

use super::{
    AggregateValueTransform, GroupedAggregateStates, Result, ShardLoomError,
    SimpleAggregateFunction, reserve_hash_set_capacity,
};
use vortex::array::dtype::{DType, Nullability};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MeasurePass {
    All,
    Ordinary,
    Distinct,
}

pub(super) fn contract_error() -> ShardLoomError {
    ShardLoomError::InvalidOperation(
        "native winner-only DISTINCT lost its admitted integer measure contract; no fallback execution was attempted".into(),
    )
}

pub(super) fn admit(
    states: &GroupedAggregateStates<'_>,
    dtype: &DType,
    columns: &[String],
    source_rows: u64,
    unfiltered: bool,
) -> Option<Report> {
    let retained_cap = states.request.offset.checked_add(states.result_limit?)?;
    if source_rows < 1_000_000
        || !unfiltered
        || dtype.is_nullable()
        || !(1..=128).contains(&retained_cap)
        || states.request.spill.is_some()
        || !states.request.having.is_empty()
        || !states.request.group_expressions.is_empty()
        || states.group_columns.len() != 1
        || states.group_key_indices.len() != 1
        || !states.groups.is_empty()
    {
        return None;
    }
    let (distinct_state, distinct_column) = states
        .state_template
        .single_count_distinct_state_index_and_column()?;
    let group = &states.group_columns[0];
    if !matches!(group.transform, AggregateValueTransform::Identity)
        || !group.extra_column_indices.is_empty()
    {
        return None;
    }
    let fields = dtype.as_struct_fields_opt()?;
    if [group.column_index, distinct_column]
        .into_iter()
        .any(|index| {
            !matches!(columns.get(index).and_then(|name| fields.field(name.as_str())),
            Some(DType::Primitive(kind, Nullability::NonNullable)) if kind.is_int())
        })
    {
        return None;
    }
    let count = states.state_template.states.iter().find(|state| {
        state.function == SimpleAggregateFunction::Count && state.column_index.is_none()
    })?;
    if !super::exact_distinct_pairs::workers::order_admitted(
        &states.request.order_by,
        &count.alias,
        &group.name,
    ) || !states
        .state_template
        .states
        .iter()
        .enumerate()
        .all(|(index, state)| {
            state.argument_offset.is_none()
                && matches!(state.value_transform, AggregateValueTransform::Identity)
                && (index == distinct_state
                    || match (state.function, state.column_index) {
                        (SimpleAggregateFunction::Count, None) => true,
                        (
                            SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg,
                            Some(column),
                        ) => {
                            matches!(
                                columns
                                    .get(column)
                                    .and_then(|name| fields.field(name.as_str())),
                                Some(DType::Primitive(_, Nullability::NonNullable))
                            )
                        }
                        _ => false,
                    })
        })
    {
        return None;
    }
    Some(Report {
        retained_cap,
        candidate_groups: 0,
        retained_groups: 0,
        second_pass_nanos: 0,
    })
}

pub(super) struct Report {
    retained_cap: usize,
    candidate_groups: usize,
    retained_groups: usize,
    pub second_pass_nanos: u128,
}

impl Report {
    pub(super) fn select(&mut self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        self.candidate_groups = states.groups.len();
        let mut candidates = states.ordered_candidates()?;
        states.capillary_select_ordered_candidates(&mut candidates, self.retained_cap);
        let mut retained = rustc_hash::FxHashSet::default();
        reserve_hash_set_capacity(&mut retained, candidates.len(), "winner-only DISTINCT keys")?;
        retained.extend(candidates.into_iter().map(|candidate| candidate.key));
        states.groups.retain(|key, _| retained.contains(key));
        self.retained_groups = states.groups.len();
        states.winner_distinct_pass = MeasurePass::Distinct;
        Ok(())
    }

    pub(super) fn annotate(&self, summary: &mut String) -> Result<()> {
        let mut value: serde_json::Value = serde_json::from_str(summary)
            .map_err(|error| ShardLoomError::InvalidOperation(error.to_string()))?;
        let object = value.as_object_mut().ok_or_else(contract_error)?;
        object.insert(
            "aggregate_winner_distinct_candidate_groups".into(),
            self.candidate_groups.into(),
        );
        object.insert(
            "aggregate_winner_distinct_retained_groups".into(),
            self.retained_groups.into(),
        );
        object.insert(
            "aggregate_winner_distinct_second_pass_nanos".into(),
            u64::try_from(self.second_pass_nanos)
                .unwrap_or(u64::MAX)
                .into(),
        );
        object.insert(
            "aggregate_winner_distinct_proof".into(),
            "complete_count_star_order_then_complete_key_ties_distinct_cannot_affect_selection"
                .into(),
        );
        *summary = value.to_string();
        Ok(())
    }
}

#[cfg(test)]
#[path = "winner_distinct_tests.rs"]
mod tests;
