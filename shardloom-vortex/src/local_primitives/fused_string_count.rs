//! Count canonical UTF-8 rows into the query's one exact global histogram.
//!
//! Native Dict arrays keep their existing domain-bound code path. This removes
//! only the transient dictionary/row-ID materialization for non-Dict strings.
//! Pressure uses the existing conservative per-entry estimate plus retained UTF-8
//! bytes; it is not allocator accounting, provider-buffer ownership, or an RSS cap.
//! Pressure replays the exact consumed prefix into the existing native sketch;
//! its unconsumed suffix and later refinement preserve supported exact execution.
//! Other failed updates poison this operation. Its partial state must never
//! be retried through another route or exposed as a completed result.

use super::{
    AggregateStringInterner, AggregateValueTransform, GroupedAggregateStates, Result,
    STRING_COUNT_TOPK_FIRST_PASS_EXACT_HISTOGRAM_BYTES_PER_ENTRY, ShardLoomError,
    StringCountTopKHeavyHitterSketch, replay_string_count_topk_sketch_from_exact_counts,
    reserve_hash_map_capacity, usize_to_u64, vortex_error,
};
use std::{sync::Arc, time::Instant};
use vortex::array::{
    ArrayRef, VortexSessionExecute as _,
    arrays::{Dict, VarBinViewArray, varbinview::VarBinViewArrayExt as _},
    dtype::{DType, Nullability},
    validity::Validity,
};

#[derive(Default)]
pub(super) struct FusedStringCountEvidence {
    pub chunks: u64,
    rows: u64,
    new_strings: u64,
    utf8_bytes_visited: u64,
    observed_interner_values: usize,
    retained_utf8_bytes: u64,
    peak_estimated_state_bytes: u64,
    pressure_prefix_rows: u64,
    pressure_suffix_rows: u64,
    pressure_histogram_entries_released: u64,
    pressure_interner_values_retained: u64,
    pressure_interner_utf8_bytes_retained: u64,
    pressure_reason: Option<&'static str>,
    poisoned: bool,
}

pub(super) struct FusedStringCountWork {
    pub rows: u64,
    pub canonicalization_nanos: u128,
}

impl FusedStringCountEvidence {
    pub(super) fn check_complete(&self) -> Result<()> {
        if self.poisoned {
            return Err(failed("cannot expose an invalidated partial result"));
        }
        Ok(())
    }

    pub(super) fn annotate_summary(&self, summary: &mut String) -> Result<()> {
        let mut payload: serde_json::Value = serde_json::from_str(summary)
            .map_err(|error| failed(&format!("invalid result summary: {error}")))?;
        let object = payload
            .as_object_mut()
            .ok_or_else(|| failed("result summary is not an object"))?;
        for (name, value) in [
            ("chunks", self.chunks),
            ("rows", self.rows),
            ("new_global_strings", self.new_strings),
            ("utf8_bytes_visited", self.utf8_bytes_visited),
            (
                "peak_estimated_state_bytes",
                self.peak_estimated_state_bytes,
            ),
            ("pressure_prefix_rows", self.pressure_prefix_rows),
            ("pressure_suffix_rows", self.pressure_suffix_rows),
            (
                "pressure_histogram_entries_released",
                self.pressure_histogram_entries_released,
            ),
            (
                "pressure_interner_values_retained",
                self.pressure_interner_values_retained,
            ),
            (
                "pressure_interner_utf8_bytes_retained",
                self.pressure_interner_utf8_bytes_retained,
            ),
        ] {
            object.insert(format!("aggregate_fused_string_count_{name}"), value.into());
        }
        object.insert("aggregate_fused_string_count_scope".into(),
            "nonnullable_count_star_native_canonical_utf8_to_exact_global_histogram;native_dictionary_codes_keep_existing_domain_path;no_local_topk_pruning;entry_plus_utf8_payload_pressure_estimate_not_allocator_or_rss_accounting".into());
        object.insert("aggregate_histogram_input_value_scope".into(),
            "existing_histogram_input_values_count_dictionary_bindings;fused_raw_utf8_rows_reported_separately".into());
        object.insert(
            "aggregate_fused_string_count_pressure_reason".into(),
            self.pressure_reason.unwrap_or("none").into(),
        );
        object.insert("aggregate_fused_string_count_pressure_release_scope".into(),
            "on_pressure_exact_histogram_map_released_after_native_sketch_replay;global_interner_values_and_utf8_payload_remain_owned_until_query_state_drop;no_full_byte_reclamation_claim".into());
        *summary = payload.to_string();
        Ok(())
    }

    fn refresh_interner(&mut self, interner: &AggregateStringInterner) -> Result<()> {
        let appended = interner
            .values
            .get(self.observed_interner_values..)
            .ok_or_else(|| failed("global interner identity changed"))?;
        // Native dictionary chunks may have added entries since the last fused
        // chunk. Inspect only that suffix, retaining one global identity domain.
        for value in appended {
            self.retained_utf8_bytes = self
                .retained_utf8_bytes
                .checked_add(usize_to_u64(value.len())?)
                .ok_or_else(|| failed("retained string bytes overflowed"))?;
        }
        self.observed_interner_values = interner.values.len();
        Ok(())
    }

    fn estimated_bytes(&self, value_count: usize, extra_string_bytes: u64) -> Result<u64> {
        usize_to_u64(value_count)?
            .checked_mul(STRING_COUNT_TOPK_FIRST_PASS_EXACT_HISTOGRAM_BYTES_PER_ENTRY)
            .and_then(|bytes| bytes.checked_add(self.retained_utf8_bytes))
            .and_then(|bytes| bytes.checked_add(extra_string_bytes))
            .ok_or_else(|| failed("retained-state byte estimate overflowed"))
    }
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local Vortex fused exact string count {reason}; no fallback execution was attempted"
    ))
}

fn admitted_group_index(states: &GroupedAggregateStates<'_>, selected: bool) -> Option<usize> {
    if selected
        || !states.string_count_topk_heavy_hitter_enabled
        || !states.state_template.is_count_star_only()
        || !states.string_count_topk_first_pass_exact_histogram_enabled
        || states.string_count_topk_first_pass_exact_histogram_disabled
        || states
            .string_count_topk_first_pass_exact_histogram_counts
            .is_none()
        || states.string_count_topk_heavy_hitter_sketch.is_some()
        || states.string_count_topk_exact_counts.is_some()
        || states.string_count_topk_candidate_ids.is_some()
        || !states.groups.is_empty()
        || !states.group_order.is_empty()
        || states.single_numeric_count_groups.is_some()
        || states.numeric_pair_compact_groups.is_some()
        || states.numeric_pair_late_measure_count_groups.is_some()
        || states.numeric_minute_string_count_groups.is_some()
        || states
            .string_count_distinct_topk_heavy_hitter_sketch
            .is_some()
        || states.string_count_distinct_topk_exact_sets.is_some()
        || states.numeric_utf8_topk_heavy_hitter_sketch.is_some()
        || states.numeric_utf8_topk_exact_counts.is_some()
        || states.result_limit.is_none()
        || !states.request.having.is_empty()
    {
        return None;
    }
    let [order] = states.request.order_by.as_slice() else {
        return None;
    };
    if !order.descending || states.state_template.count_star_measure_alias()? != order.column {
        return None;
    }
    let [group_index] = states.group_key_indices.as_slice() else {
        return None;
    };
    let group = states.group_columns.get(*group_index)?;
    (group.extra_column_indices.is_empty()
        && matches!(group.transform, AggregateValueTransform::Identity))
    .then_some(*group_index)
}

pub(super) fn try_update(
    states: &mut GroupedAggregateStates<'_>,
    chunk: &ArrayRef,
    declared_columns: &[String],
    row_indices: Option<&[usize]>,
) -> Result<Option<FusedStringCountWork>> {
    if states.fused_string_count.poisoned {
        return Err(failed(
            "operation was invalidated by an earlier failed update",
        ));
    }
    let result = try_update_inner(states, chunk, declared_columns, row_indices);
    if result.is_err() {
        states.fused_string_count.poisoned = true;
    }
    result
}

#[allow(clippy::too_many_lines)]
fn try_update_inner(
    states: &mut GroupedAggregateStates<'_>,
    chunk: &ArrayRef,
    declared_columns: &[String],
    row_indices: Option<&[usize]>,
) -> Result<Option<FusedStringCountWork>> {
    let Some(group_index) = admitted_group_index(states, row_indices.is_some()) else {
        return Ok(None);
    };
    let column_index = states.group_columns[group_index].column_index;
    let column = declared_columns
        .get(column_index)
        .ok_or_else(|| failed("group source column is missing"))?;
    if chunk.dtype().is_nullable() {
        return Err(failed("lost its admitted nonnullable source contract"));
    }
    let array = if chunk.dtype().is_struct() {
        super::logical_field_from_native_array(chunk, column)?
    } else {
        if declared_columns.len() != 1 || column_index != 0 {
            return Err(failed(
                "scalar chunk does not match declared source columns",
            ));
        }
        chunk.clone()
    };
    // Never canonicalize an already encoded dictionary into row strings. Its
    // numeric codes must be interpreted against its own dictionary values.
    if array.as_opt::<Dict>().is_some() {
        return Ok(None);
    }
    if !matches!(array.dtype(), DType::Utf8(Nullability::NonNullable)) {
        if matches!(array.dtype(), DType::Utf8(_)) {
            return Err(failed("lost its admitted nonnullable string contract"));
        }
        return Ok(None);
    }
    let started = Instant::now();
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let utf8 = array
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(vortex_error)?;
    let canonicalization_nanos = started.elapsed().as_nanos();
    if !matches!(
        utf8.varbinview_validity(),
        Validity::NonNullable | Validity::AllValid
    ) {
        return Err(failed("native canonicalization produced nullable rows"));
    }
    if utf8.len() != chunk.len() {
        return Err(failed(
            "canonical key length differs from scanned chunk rows",
        ));
    }
    let rows = usize_to_u64(utf8.len())?;
    let next_total = states
        .string_count_topk_total_weight
        .checked_add(rows)
        .ok_or_else(|| failed("total row count overflowed u64"))?;
    let next_input_rows = states
        .string_count_topk_first_pass_exact_histogram_input_rows
        .checked_add(rows)
        .ok_or_else(|| failed("histogram row count overflowed u64"))?;
    let next_chunks = states
        .fused_string_count
        .chunks
        .checked_add(1)
        .ok_or_else(|| failed("chunk count overflowed"))?;
    let next_rows = states
        .fused_string_count
        .rows
        .checked_add(rows)
        .ok_or_else(|| failed("fused row count overflowed"))?;
    let (estimated_bytes, pressure) = {
        let tracker = &mut states.fused_string_count;
        tracker.refresh_interner(&states.string_interner)?;
        let mut estimated_bytes = tracker.estimated_bytes(states.string_interner.len(), 0)?;
        let mut pressure = (estimated_bytes > states.resource_envelope.memory_budget_bytes)
            .then_some((0, "existing_retained_utf8_byte_estimate"));
        let interner = &mut states.string_interner;
        let histogram = states
            .string_count_topk_first_pass_exact_histogram_counts
            .as_mut()
            .ok_or_else(|| failed("exact histogram is missing"))?;
        for row in 0..utf8.len() {
            if pressure.is_some() {
                break;
            }
            let bytes = utf8.bytes_at(row);
            let value = std::str::from_utf8(bytes.as_slice()).map_err(|error| {
                failed(&format!(
                    "column '{column}' contains invalid UTF-8: {error}"
                ))
            })?;
            let existing_id = interner.ids.get(value).copied();
            if let Some(id) = existing_id
                && let Some(count) = histogram.get_mut(&id)
            {
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| failed("exact group count overflowed u64"))?;
                tracker.utf8_bytes_visited = tracker
                    .utf8_bytes_visited
                    .checked_add(usize_to_u64(value.len())?)
                    .ok_or_else(|| failed("visited string bytes overflowed"))?;
                continue;
            }
            if histogram.len() >= states.string_count_topk_first_pass_exact_histogram_entry_budget {
                pressure = Some((row, "exact_entry_budget"));
                break;
            }
            let id = if let Some(id) = existing_id {
                id
            } else {
                let next_count = interner
                    .len()
                    .checked_add(1)
                    .ok_or_else(|| failed("string entry count overflowed"))?;
                let string_bytes = usize_to_u64(value.len())?;
                let next_bytes = tracker.estimated_bytes(next_count, string_bytes)?;
                if next_bytes > states.resource_envelope.memory_budget_bytes {
                    pressure = Some((row, "retained_utf8_byte_estimate"));
                    break;
                }
                // Admission precedes the new key's Arc and container growth. This
                // preserves the existing conservative estimate, not an allocator cap.
                interner.reserve(1, "fused exact string identity")?;
                let id = usize_to_u64(interner.values.len())?;
                let owned: Arc<str> = Arc::from(value);
                interner.values.push(Arc::clone(&owned));
                interner.ids.insert(owned, id);
                tracker.retained_utf8_bytes = tracker
                    .retained_utf8_bytes
                    .checked_add(string_bytes)
                    .ok_or_else(|| failed("retained UTF-8 bytes overflowed"))?;
                tracker.observed_interner_values = interner.values.len();
                tracker.new_strings = tracker
                    .new_strings
                    .checked_add(1)
                    .ok_or_else(|| failed("new string count overflowed"))?;
                estimated_bytes = next_bytes;
                id
            };
            reserve_hash_map_capacity(histogram, 1, "fused exact string counts")?;
            histogram.insert(id, 1);
            tracker.utf8_bytes_visited = tracker
                .utf8_bytes_visited
                .checked_add(usize_to_u64(value.len())?)
                .ok_or_else(|| failed("visited string bytes overflowed"))?;
        }
        (estimated_bytes, pressure)
    };
    if let Some((prefix_rows, reason)) = pressure {
        replay_prefix_and_update_suffix(states, &utf8, prefix_rows, reason)?;
    }
    let tracker = &mut states.fused_string_count;
    tracker.chunks = next_chunks;
    tracker.rows = next_rows;
    tracker.peak_estimated_state_bytes = tracker.peak_estimated_state_bytes.max(estimated_bytes);
    states.string_count_topk_total_weight = next_total;
    states.string_count_topk_first_pass_exact_histogram_input_rows = next_input_rows;
    // input_values continues to count dictionary bindings from the original
    // encoded path. Raw UTF-8 visits have their own explicit row/byte counters.
    states.string_count_topk_string_group_index = Some(group_index);
    states.count_star_direct_updates = true;
    states.string_count_topk_heavy_hitter_direct_updates = true;
    states
        .aggregate_accessor_summary
        .insert(format!("{column}:native_canonical_utf8_fused_exact_count"));
    Ok(Some(FusedStringCountWork {
        rows,
        canonicalization_nanos,
    }))
}

fn replay_prefix_and_update_suffix(
    states: &mut GroupedAggregateStates<'_>,
    utf8: &VarBinViewArray,
    prefix_rows: usize,
    reason: &'static str,
) -> Result<()> {
    let mut sketch = StringCountTopKHeavyHitterSketch::new_with_exact_mirror(
        states.string_count_topk_heavy_hitter_capacity(),
        0,
    );
    let previous_counts = states
        .string_count_topk_first_pass_exact_histogram_counts
        .take()
        .ok_or_else(|| failed("pressure transition lost its exact prefix histogram"))?;
    let released_entries = usize_to_u64(previous_counts.len())?;
    // Includes all previous chunks plus current rows [0, prefix_rows). Neither
    // replay nor the suffix loop mutates total_weight; the whole chunk commits
    // its checked weight exactly once after both have completed successfully.
    replay_string_count_topk_sketch_from_exact_counts(&mut sketch, &previous_counts)?;
    drop(previous_counts);
    states.string_count_topk_first_pass_exact_histogram_disabled = true;
    for row in prefix_rows..utf8.len() {
        let bytes = utf8.bytes_at(row);
        let value = std::str::from_utf8(bytes.as_slice())
            .map_err(|error| failed(&format!("pressure suffix contains invalid UTF-8: {error}")))?;
        let owned = states.string_interner.id(value).map_or_else(
            || Ok(Arc::<str>::from(value)),
            |id| states.string_interner.value_arc(id),
        )?;
        sketch.update_lazy_utf8_value(&owned, 1, &mut states.string_interner)?;
        states.fused_string_count.utf8_bytes_visited = states
            .fused_string_count
            .utf8_bytes_visited
            .checked_add(usize_to_u64(value.len())?)
            .ok_or_else(|| failed("suffix string bytes overflowed"))?;
    }
    states.string_count_topk_heavy_hitter_sketch = Some(sketch);
    let tracker = &mut states.fused_string_count;
    tracker.refresh_interner(&states.string_interner)?;
    tracker.pressure_reason = Some(reason);
    tracker.pressure_prefix_rows = usize_to_u64(prefix_rows)?;
    tracker.pressure_suffix_rows = usize_to_u64(
        utf8.len()
            .checked_sub(prefix_rows)
            .ok_or_else(|| failed("pressure prefix exceeds chunk rows"))?,
    )?;
    tracker.pressure_histogram_entries_released = released_entries;
    tracker.pressure_interner_values_retained = usize_to_u64(states.string_interner.values.len())?;
    tracker.pressure_interner_utf8_bytes_retained = tracker.retained_utf8_bytes;
    Ok(())
}

#[cfg(test)]
#[path = "fused_string_count_tests.rs"]
mod tests;
