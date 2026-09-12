//! UTF8 key construction inside the shared completed aggregate finalizer.
//! Borrow complete native identities through selection; copy selected bytes once.

use super::{
    AggregateDistinctValue, AggregateGroupKey, ArrayRef, BinaryHeap, Buffer, DType, FieldNames,
    GroupedAggregateStates, MAX_BYTES, MAX_ROWS, Nullability, OwnedAggregateFinalizer,
    PrimitiveArray, Result, SimpleAggregateFunction, StructArray, Validity, completed_count,
    failed, vortex_error,
};
use vortex::{
    array::{IntoArray as _, arrays::VarBinArray},
    buffer::Alignment,
};

#[derive(Clone, Copy, Eq, PartialEq)]
struct RankedText<'a> {
    key: &'a str,
    count: u64,
}
impl PartialOrd for RankedText<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RankedText<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.count
            .cmp(&other.count)
            .reverse()
            .then_with(|| self.key.cmp(other.key))
    }
}

impl OwnedAggregateFinalizer {
    pub(super) fn finish_utf8(
        &mut self,
        states: &GroupedAggregateStates<'_>,
    ) -> Result<(usize, String)> {
        if self.function != SimpleAggregateFunction::Count
            || !states.state_template.is_count_star_only()
            || states.finalized_distinct_counts.is_some()
            || states.single_numeric_count_groups.is_some()
        {
            return Err(failed(
                "completed UTF8 state differs from admitted COUNT(*)",
            ));
        }
        let capacity = states.group_count().min(self.offset + self.limit);
        let selection_bytes = capacity
            .checked_mul(size_of::<RankedText<'_>>())
            .and_then(|bytes| bytes.checked_add(size_of::<BinaryHeap<RankedText<'_>>>()))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| failed("UTF8 selection capacity overflowed"))?;
        let _selection = self.memory.reserve(selection_bytes)?;
        let mut heap = BinaryHeap::new();
        heap.try_reserve_exact(capacity).map_err(vortex_error)?;
        if heap.capacity() > capacity {
            return Err(failed("UTF8 selection allocation exceeded its grant"));
        }
        visit_complete(states, |key, count| {
            let candidate = RankedText { key, count };
            if heap.len() < capacity {
                heap.push(candidate);
            } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                *heap.peek_mut().expect("nonempty bounded UTF8 heap") = candidate;
            }
            Ok(())
        })?;
        let retained = heap.into_sorted_vec();
        let begin = self.offset.min(retained.len());
        let selected = &retained[begin..retained.len().min(begin + self.limit)];
        let key_bytes = selected.iter().try_fold(0_usize, |bytes, row| {
            bytes
                .checked_add(row.key.len())
                .ok_or_else(|| failed("UTF8 data size overflowed"))
        })?;
        let array = self.build_utf8(selected, key_bytes)?;
        let mut summary = self.summary_payload(states, selected.len(), states.group_count());
        summary["aggregate_result_utf8_bytes"] = key_bytes.into();
        summary["aggregate_result_native_buffer_bytes"] = array.nbytes().into();
        summary["aggregate_result_json_rows"] = 0.into();
        summary["aggregate_result_stat_value_rows"] = 0.into();
        summary["aggregate_result_materialization_scope"] =
            "owned_output_only;selected_UTF8_bytes_copied_once;no_JSON_or_StatValue_output_rows;source_accessor_and_state_materialization_are_separate".into();
        summary["candidate_group_scope"] = if states.complete_key_partition_group_count.is_some() {
            "complete_key_partition_global_groups"
        } else if states.string_count_topk_exact_counts.is_some() {
            "completed_exact_candidate_set;not_a_global_distinct_group_total"
        } else {
            "complete_native_group_state"
        }
        .into();
        summary["aggregate_accessor_summary"] = states.aggregate_accessor_summary().into();
        summary["aggregate_accessor_materialization_status"] =
            states.aggregate_accessor_materialization_status().into();
        self.array = Some(array);
        Ok((selected.len(), summary.to_string()))
    }

    fn build_utf8(&self, selected: &[RankedText<'_>], key_bytes: usize) -> Result<ArrayRef> {
        let rows = selected.len();
        let (offset_bytes, _) = geometry(rows, key_bytes)?;
        // Keep earlier buffers and the selection live while admitting the next
        // buffer; an error drops all owners and restores their exact credits.
        let mut offsets = self
            .allocator
            .allocate(offset_bytes, Alignment::new(8))
            .map_err(vortex_error)?;
        let mut data = self
            .allocator
            .allocate(key_bytes, Alignment::none())
            .map_err(vortex_error)?;
        let mut counts = self
            .allocator
            .allocate(rows * 8, Alignment::new(8))
            .map_err(vortex_error)?;
        offsets.as_mut_slice()[..8].copy_from_slice(&0_u64.to_ne_bytes());
        let mut end = 0_usize;
        for (index, row) in selected.iter().enumerate() {
            let next = end
                .checked_add(row.key.len())
                .filter(|next| *next <= key_bytes)
                .ok_or_else(|| failed("UTF8 selected data changed size"))?;
            data.as_mut_slice()[end..next].copy_from_slice(row.key.as_bytes());
            end = next;
            offsets.as_mut_slice()[(index + 1) * 8..(index + 2) * 8]
                .copy_from_slice(&u64::try_from(end).map_err(vortex_error)?.to_ne_bytes());
            counts.as_mut_slice()[index * 8..(index + 1) * 8]
                .copy_from_slice(&row.count.to_ne_bytes());
        }
        if end != key_bytes {
            return Err(failed("UTF8 selected data ended before admitted size"));
        }
        let keys = VarBinArray::try_new(
            PrimitiveArray::new(
                Buffer::<u64>::from_byte_buffer(offsets.freeze()),
                Validity::NonNullable,
            )
            .into_array(),
            data.freeze(),
            DType::Utf8(Nullability::NonNullable),
            Validity::NonNullable,
        )
        .map_err(vortex_error)?
        .into_array();
        StructArray::try_new(
            FieldNames::from(self.columns.iter().map(String::as_str).collect::<Vec<_>>()),
            vec![
                keys,
                PrimitiveArray::new(
                    Buffer::<u64>::from_byte_buffer(counts.freeze()),
                    Validity::NonNullable,
                )
                .into_array(),
            ],
            rows,
            Validity::NonNullable,
        )
        .map(vortex::array::IntoArray::into_array)
        .map_err(vortex_error)
    }
}

fn geometry(rows: usize, key_bytes: usize) -> Result<(usize, usize)> {
    let offset_bytes = rows
        .checked_add(1)
        .and_then(|rows| rows.checked_mul(8))
        .ok_or_else(|| failed("UTF8 offset capacity overflowed"))?;
    let total = rows
        .checked_mul(8)
        .and_then(|counts| counts.checked_add(offset_bytes))
        .and_then(|bytes| bytes.checked_add(key_bytes))
        .ok_or_else(|| failed("UTF8 output byte bound overflowed"))?;
    if rows > MAX_ROWS || total > MAX_BYTES {
        return Err(failed(
            "completed UTF8 output exceeds row or byte admission",
        ));
    }
    Ok((offset_bytes, total))
}

fn visit_complete<'a>(
    states: &'a GroupedAggregateStates<'_>,
    mut visit: impl FnMut(&'a str, u64) -> Result<()>,
) -> Result<()> {
    if let Some(counts) = &states.string_count_topk_exact_counts {
        for (id, count) in counts {
            visit(states.string_interner.value(*id)?, *count)?;
        }
        return Ok(());
    }
    if states.string_count_topk_heavy_hitter_enabled && states.string_count_topk_total_weight != 0 {
        return Err(failed(
            "UTF8 heavy-hitter state has not completed exact refinement",
        ));
    }
    for (key, group) in &states.groups {
        let key = match key {
            AggregateGroupKey::Single(AggregateDistinctValue::Utf8Interned(id)) => {
                states.string_interner.value(*id)?
            }
            AggregateGroupKey::Single(AggregateDistinctValue::Utf8(value)) => value.as_ref(),
            _ => {
                return Err(failed(
                    "completed UTF8 group identity changed its admitted dtype",
                ));
            }
        };
        visit(key, completed_count(group, SimpleAggregateFunction::Count)?)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_utf8_count_geometry_includes_empty_offset_and_all_buffers() {
        assert_eq!(geometry(0, 0).unwrap(), (8, 8));
        assert_eq!(geometry(1, MAX_BYTES - 24).unwrap(), (16, MAX_BYTES));
        assert!(geometry(1, MAX_BYTES - 23).is_err());
        assert!(geometry(MAX_ROWS + 1, 0).is_err());
        assert!(geometry(usize::MAX, 0).is_err());
        assert!(geometry(1, usize::MAX).is_err());
    }

    #[cfg(unix)]
    fn output(session: &crate::resident_session::ResidentVortexSession) -> OwnedAggregateFinalizer {
        use crate::{
            VortexAggregateOrderExpr, VortexQueryPrimitiveRequest, VortexSimpleAggregateMeasure,
            VortexSimpleAggregateRequest,
        };
        use shardloom_core::{ColumnRef, DatasetUri};
        let request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new("/not-opened.vortex").unwrap(),
            VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new("labels").unwrap()],
                vec![VortexSimpleAggregateMeasure::new(
                    "count",
                    None,
                    "rows".into(),
                )],
            )
            .with_order_by(vec![VortexAggregateOrderExpr::new("rows", true)]),
        )
        .with_source_order_limit(3);
        let dtype = DType::struct_(
            [("labels", DType::Utf8(Nullability::NonNullable))],
            Nullability::NonNullable,
        );
        OwnedAggregateFinalizer::new(&request, &dtype, session).unwrap()
    }

    #[cfg(unix)]
    #[test]
    fn owned_utf8_count_each_buffer_denial_releases_earlier_grants() {
        let session = crate::resident_session::ResidentVortexSession::new(64 << 10, 1).unwrap();
        let memory = session.memory().clone();
        let output = output(&session);
        let baseline = memory.snapshot().reserved_bytes;
        let selected = [RankedText {
            key: "0123456789abcdef",
            count: u64::MAX,
        }];
        // Offsets need 16 bytes, text 16, counts 8. Exercise initial denial,
        // denial after offsets, and denial after offsets plus text coexist.
        for available in [0, 15, 16, 31, 32, 39] {
            let guard = memory
                .reserve(memory.snapshot().limit_bytes - baseline - available)
                .unwrap();
            assert!(output.build_utf8(&selected, 16).is_err());
            assert_eq!(memory.snapshot().reserved_bytes, baseline + guard.bytes());
            drop(guard);
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
        }
        let array = output.build_utf8(&selected, 16).unwrap();
        assert_eq!(array.nbytes(), 40);
        drop(array);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
