//! The existing mixed-measure chunk recipe shared by serial and ordered workers.
//! DISTINCT pair suppression never suppresses an ordinary measure contribution.

use super::{
    AggregateCountDistinctPairPreunionKey, AggregateCountDistinctPreunionGroupKey,
    AggregateDirectColumnAccessor, AggregateDirectIntegerKeySlice, GroupedAggregateState,
    GroupedAggregateStates, SimpleAggregateStates, bound_numeric_updates,
    reserve_hash_map_capacity, reserve_hash_set_capacity, usize_to_u64,
};
use shardloom_core::{Result, ShardLoomError};

pub(super) struct MixedDistinctPartial {
    chunk_groups:
        rustc_hash::FxHashMap<AggregateCountDistinctPreunionGroupKey, SimpleAggregateStates>,
    chunk_group_order: Vec<AggregateCountDistinctPreunionGroupKey>,
    chunk_rows: usize,
    unique_pairs: u64,
    bound_recipe: bool,
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native mixed DISTINCT partial {reason}; no fallback execution was attempted"
    ))
}

impl MixedDistinctPartial {
    /// Conservative capacity model for the admitted integer-only retained
    /// collections. Hash tables need buckets and control bytes beyond their
    /// advertised entry capacity; two buckets per entry plus the control tail
    /// covers the pinned implementation. This is not an allocator/RSS meter.
    pub(super) fn retained_capacity_bytes(&self) -> Result<u64> {
        fn table(capacity: usize, entry: usize) -> Option<usize> {
            capacity
                .checked_mul(2)?
                .checked_mul(entry.checked_add(1)?)?
                .checked_add(16)
        }
        let bytes = table(
            self.chunk_groups.capacity(),
            size_of::<(
                AggregateCountDistinctPreunionGroupKey,
                SimpleAggregateStates,
            )>(),
        )
        .and_then(|bytes| {
            self.chunk_group_order
                .capacity()
                .checked_mul(size_of::<AggregateCountDistinctPreunionGroupKey>())
                .and_then(|order| bytes.checked_add(order))
        })
        .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
        .ok_or_else(|| failed("group capacity overflow"))?;
        let bytes = self
            .chunk_groups
            .values()
            .try_fold(bytes, |bytes, states| {
                let bytes = states
                    .states
                    .capacity()
                    .checked_mul(size_of::<super::SimpleAggregateState>())
                    .and_then(|extra| bytes.checked_add(extra))?;
                states.states.iter().try_fold(bytes, |bytes, state| {
                    bytes
                        .checked_add(state.alias.capacity())?
                        .checked_add(table(
                            state.distinct_values.capacity(),
                            size_of::<super::AggregateDistinctValue>(),
                        )?)
                })
            })
            .ok_or_else(|| failed("state capacity overflow"))?;
        u64::try_from(bytes).map_err(|_| failed("capacity exceeds u64"))
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) fn build(
        state_template: &SimpleAggregateStates,
        distinct_state_index: usize,
        group_keys: AggregateDirectIntegerKeySlice<'_>,
        distinct_keys: AggregateDirectIntegerKeySlice<'_>,
        accessors: &[AggregateDirectColumnAccessor],
        chunk_rows: usize,
        group_limit: Option<usize>,
        check_cancelled: &impl Fn() -> Result<()>,
    ) -> Result<Self> {
        let mut chunk_pairs =
            rustc_hash::FxHashSet::<AggregateCountDistinctPairPreunionKey>::default();
        reserve_hash_set_capacity(
            &mut chunk_pairs,
            chunk_rows,
            "grouped count-distinct pair preunion",
        )?;
        let mut chunk_groups = rustc_hash::FxHashMap::<
            AggregateCountDistinctPreunionGroupKey,
            SimpleAggregateStates,
        >::default();
        reserve_hash_map_capacity(
            &mut chunk_groups,
            chunk_rows.min(group_limit.unwrap_or(65_536)),
            "grouped count-distinct pair preunion chunk-group partials",
        )?;
        let mut chunk_group_order = Vec::new();
        chunk_group_order
            .try_reserve(chunk_rows.min(group_limit.unwrap_or(65_536)))
            .map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "local Vortex grouped count-distinct pair preunion chunk-group order reservation failed: {error}; no fallback execution was attempted"
                ))
            })?;
        let mut unique_pairs = 0_u64;
        let recipe = bound_numeric_updates::BoundNumericUpdates::bind(
            state_template,
            accessors,
            distinct_state_index,
            chunk_rows,
        );
        for row_index in 0..chunk_rows {
            if row_index.is_multiple_of(4096) {
                check_cancelled()?;
            }
            let pair_key = AggregateCountDistinctPairPreunionKey::from_integer_key_slices(
                group_keys,
                distinct_keys,
                row_index,
            )?;
            let pair_inserted = chunk_pairs.insert(pair_key);
            let group_key = pair_key.preunion_group_key();
            let group_states = match chunk_groups.entry(group_key) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    if group_limit.is_some_and(|limit| chunk_group_order.len() >= limit) {
                        return Err(failed("chunk exceeded its complete winner-key bound"));
                    }
                    chunk_group_order.push(group_key);
                    entry.insert(state_template.clone())
                }
            };
            if let Some(recipe) = &recipe {
                recipe.update(group_states, row_index)?;
            } else {
                group_states.update_direct_row_from_accessors_except_state(
                    accessors,
                    row_index,
                    chunk_rows,
                    distinct_state_index,
                )?;
            }
            if pair_inserted {
                group_states.update_count_distinct_preunion_value_at(
                    distinct_state_index,
                    pair_key.distinct_value(),
                )?;
                unique_pairs = unique_pairs.checked_add(1).ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex grouped count-distinct preunion unique-pair count overflowed u64"
                            .to_string(),
                    )
                })?;
            }
        }
        Ok(Self {
            chunk_groups,
            chunk_group_order,
            chunk_rows,
            unique_pairs,
            bound_recipe: recipe.is_some(),
        })
    }

    pub(super) fn merge_into(self, states: &mut GroupedAggregateStates<'_>) -> Result<()> {
        let Self {
            mut chunk_groups,
            chunk_group_order,
            chunk_rows,
            unique_pairs,
            bound_recipe,
        } = self;
        let chunk_group_count = chunk_groups.len();
        reserve_hash_map_capacity(
            &mut states.groups,
            chunk_group_count,
            "grouped count-distinct pair preunion aggregate",
        )?;
        if states.request.order_by.is_empty() {
            states.group_order
                .try_reserve(chunk_group_count)
                .map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "local Vortex grouped count-distinct pair preunion source-order reservation failed: {error}; no fallback execution was attempted"
                    ))
                })?;
        }
        let record_source_order = states.request.order_by.is_empty();
        for group_key in chunk_group_order {
            let partial_states = chunk_groups.remove(&group_key).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex grouped count-distinct pair preunion chunk partial was missing; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            let aggregate_group_key = group_key.aggregate_group_key();
            let group = match states.groups.entry(aggregate_group_key) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    if record_source_order {
                        states.group_order.push(entry.key().clone());
                    }
                    entry.insert(GroupedAggregateState::new_general(
                        vec![group_key.stat_value()],
                        states.state_template.clone(),
                    ))
                }
            };
            group
                .general_states_mut()?
                .merge_preaggregated_from(&partial_states)?;
        }
        states.general_direct_updates = true;
        states.general_direct_count_distinct_updates = true;
        states.general_direct_group_state_pre_reserved = true;
        states.grouped_count_distinct_pair_preunion_updates = true;
        if bound_recipe {
            states.bound_numeric_recipe_chunks += 1;
        }
        states.grouped_count_distinct_pair_preunion_input_rows = states
            .grouped_count_distinct_pair_preunion_input_rows
            .checked_add(usize_to_u64(chunk_rows)?)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex grouped count-distinct preunion input row count overflowed u64"
                        .to_string(),
                )
            })?;
        states.grouped_count_distinct_pair_preunion_unique_pairs = states
            .grouped_count_distinct_pair_preunion_unique_pairs
            .checked_add(unique_pairs)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex grouped count-distinct preunion unique-pair counter overflowed u64"
                        .to_string(),
                )
            })?;
        states.grouped_count_distinct_pair_preunion_chunk_group_partials = true;
        states.grouped_count_distinct_pair_preunion_chunk_groups = states
            .grouped_count_distinct_pair_preunion_chunk_groups
            .checked_add(usize_to_u64(chunk_group_count)?)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex grouped count-distinct preunion chunk-group counter overflowed u64"
                        .to_string(),
                )
            })?;
        Ok(())
    }
}
