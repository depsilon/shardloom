//! Exact source-value counts feed the existing weighted domain accumulators.
//! Source order and dictionary domain order match the ordinary accessor route.

use super::*;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    ArrayRef,
    dtype::{DType, Nullability},
};

impl GroupedAggregateStates<'_> {
    pub(super) fn update_owned_weighted_string_chunk(
        &mut self,
        chunk: &ArrayRef,
        columns: &[String],
        timing: &mut aggregate_timing::AggregateFirstPassTiming,
    ) -> Result<bool> {
        let Some(memory) = self.weighted_string_memory.as_ref() else {
            return Ok(false);
        };
        if self.group_key_indices.as_slice() != [0]
            || self.group_columns.len() != 1
            || columns.len() != 1
            || self.source_order_group_admission_limit().is_some()
            || self.request.order_by.is_empty()
            || !self.groups.is_empty()
            || !self.group_order.is_empty()
        {
            return Ok(false);
        }
        let group = &self.group_columns[0];
        if group.column_index != 0
            || !group.extra_column_indices.is_empty()
            || group.transform != AggregateValueTransform::UrlDomain
        {
            return Ok(false);
        }
        let Some(plan) =
            TransformedDictionaryDenseGeneralPlan::from_template(&self.state_template, 0)
        else {
            return Ok(false);
        };
        let started = Instant::now();
        let array = if chunk.dtype().is_struct() {
            logical_field_from_native_array(chunk, &columns[0])?
        } else {
            chunk.clone()
        };
        if !matches!(array.dtype(), DType::Utf8(Nullability::NonNullable)) {
            return Ok(false);
        }
        // Keep direct Dict admission on its established provider path: it can
        // select domain order or canonical first-occurrence order depending on
        // code/value representation. Do not reassociate floating updates.
        if array.as_opt::<vortex::array::arrays::Dict>().is_some() {
            return Ok(false);
        }
        // Decline before contributing rows if exact partial capacity is unavailable.
        // Native provider buffers retain their shared-session allocator leases.
        let Ok(mut lease) = memory.reserve(string_count_partial::partial_bytes(&array)?) else {
            return Ok(false);
        };
        let mut partial = string_count_partial::count_string_chunk(
            &array,
            self.native_execution_ctx.clone(),
            &aggregate_chunk_jobs::ChunkWorkerContext::Inline(CancellationToken::default()),
            &mut lease,
        )?;
        partial.preserve_existing_key_order();
        self.aggregate_accessor_summary
            .insert("native_canonical_utf8_owned_all_key_count_partial".into());
        timing.accessor_nanos += started.elapsed().as_nanos();
        timing.accessor_chunks += 1;
        timing.accessor_rows += partial.work.rows;
        timing.weighted_string_rows += partial.work.rows;
        timing.weighted_string_entries += partial.work.partial_entries;
        timing.weighted_string_canonicalization_nanos += partial.work.canonicalization_nanos;
        timing.weighted_string_count_nanos += partial.work.count_nanos;
        timing.weighted_string_hash_bytes += partial.work.utf8_bytes_hashed;
        timing.weighted_string_peak_capacity = timing
            .weighted_string_peak_capacity
            .max(partial.work.partial_capacity_bytes);
        let started = Instant::now();
        self.consume_owned_weighted_string_partial(plan, &partial)?;
        timing.group_update_nanos += started.elapsed().as_nanos();
        Ok(true)
    }

    fn consume_owned_weighted_string_partial(
        &mut self,
        plan: TransformedDictionaryDenseGeneralPlan,
        partial: &string_count_partial::StringCountPartial,
    ) -> Result<()> {
        if self
            .transformed_dictionary_dense_general_plan
            .is_some_and(|existing| existing != plan)
        {
            return Err(ShardLoomError::InvalidOperation(
                "weighted string plan changed across chunks; no fallback execution was attempted"
                    .into(),
            ));
        }
        self.transformed_dictionary_dense_general_plan = Some(plan);
        let mut sampled = 0_usize;
        let mut domains = rustc_hash::FxHashSet::<String>::default();
        for index in 0..(partial.work.partial_entries as usize)
            .min(TRANSFORMED_DICTIONARY_DENSE_GENERAL_CHUNK_PARTIAL_SAMPLE_LIMIT)
        {
            let (bytes, _, _) = partial.entry(index)?;
            let value = std::str::from_utf8(bytes.as_slice())
                .map_err(|error| ShardLoomError::InvalidOperation(error.to_string()))?;
            sampled += 1;
            domains.insert(aggregate_url_domain_str(value).to_owned());
        }
        let chunk_partials = partial.value_domain_len()
            >= TRANSFORMED_DICTIONARY_DENSE_GENERAL_CHUNK_PARTIAL_MIN_SAMPLE
            && sampled >= TRANSFORMED_DICTIONARY_DENSE_GENERAL_CHUNK_PARTIAL_MIN_SAMPLE
            && domains.len().saturating_mul(4) <= sampled.saturating_mul(3);
        let groups = self
            .transformed_dictionary_dense_general_groups
            .get_or_insert_with(Default::default);
        if chunk_partials {
            let mut partials =
                rustc_hash::FxHashMap::<String, TransformedDictionaryDenseGeneralState>::default();
            reserve_hash_map_capacity(
                &mut partials,
                partial.work.partial_entries as usize,
                "owned weighted string chunk partial",
            )?;
            partial.for_each_count(|value, count| {
                let domain = aggregate_url_domain_str(value);
                if let Some(state) = partials.get_mut(domain) {
                    state.update_weighted_utf8_value(plan, value, count, || {
                        std::sync::Arc::from(value)
                    })?;
                } else {
                    let mut state = TransformedDictionaryDenseGeneralState::default();
                    state.update_weighted_utf8_value(plan, value, count, || {
                        std::sync::Arc::from(value)
                    })?;
                    partials.insert(domain.to_owned(), state);
                }
                Ok(())
            })?;
            reserve_hash_map_capacity(groups, partials.len(), "owned weighted domain groups")?;
            self.transformed_dictionary_dense_general_chunk_partials = true;
            self.transformed_dictionary_dense_general_chunk_partial_input_values +=
                partial.work.partial_entries;
            self.transformed_dictionary_dense_general_chunk_partial_groups += partials.len() as u64;
            for (domain, partial) in partials {
                let key = self.string_interner.intern(&domain)?;
                groups
                    .entry(key)
                    .or_default()
                    .merge_chunk_partial(plan, &partial)?;
            }
        } else {
            reserve_hash_map_capacity(
                groups,
                partial.value_domain_len(),
                "owned weighted domain groups",
            )?;
            partial.for_each_count(|value, count| {
                let key = self
                    .string_interner
                    .intern(aggregate_url_domain_str(value))?;
                groups.entry(key).or_default().update_weighted_utf8_value(
                    plan,
                    value,
                    count,
                    || std::sync::Arc::from(value),
                )
            })?;
        }
        self.count_star_direct_updates = self.state_template.has_count_star_measure();
        self.chunk_dictionary_direct_updates = true;
        self.transformed_dictionary_direct_updates = true;
        self.transformed_dictionary_dense_general_direct_updates = true;
        self.transformed_dictionary_dense_general_transform_fusion |= plan.fused_transform;
        self.transformed_dictionary_lazy_utf8_minmax_updates |= plan.needs_utf8_minmax();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_exec::live_memory::LiveMemoryPool;
    use vortex::array::{
        IntoArray as _,
        arrays::{ConstantArray, DictArray, PrimitiveArray, VarBinViewArray},
    };

    fn request() -> VortexSimpleAggregateRequest {
        VortexSimpleAggregateRequest::grouped(
            Vec::new(),
            vec![
                VortexSimpleAggregateMeasure::new("count", None, "n".into()),
                VortexSimpleAggregateMeasure::new(
                    "avg",
                    Some(ColumnRef::new("link").unwrap()),
                    "mean".into(),
                )
                .with_value_transform("length"),
                VortexSimpleAggregateMeasure::new(
                    "min",
                    Some(ColumnRef::new("link").unwrap()),
                    "first".into(),
                ),
                VortexSimpleAggregateMeasure::new(
                    "max",
                    Some(ColumnRef::new("link").unwrap()),
                    "last".into(),
                ),
            ],
        )
        .with_group_expressions(vec![crate::VortexAggregateExpression::new(
            "domain".into(),
            ColumnRef::new("link").unwrap(),
            "url_domain",
        )])
        .with_order_by(vec![crate::VortexAggregateOrderExpr::new("mean", true)])
    }

    #[test]
    fn weighted_string_partial_matches_existing_route_across_native_representations() {
        let request = request();
        let columns = vec!["link".into()];
        let memory = LiveMemoryPool::new(16 * 1024 * 1024).unwrap();
        let mut actual =
            GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
        actual.weighted_string_memory = Some(memory.clone());
        let mut expected =
            GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
        let mut timing = aggregate_timing::AggregateFirstPassTiming::default();
        let dictionary = DictArray::try_new(
            PrimitiveArray::from_iter([2_u32, 0, 2, 1, 0]).into_array(),
            VarBinViewArray::from_iter_str(["https://é.test/茶", "", "https://é.test/a", "unused"])
                .into_array(),
        )
        .unwrap()
        .into_array();
        let many: Vec<_> = (0..130)
            .map(|i| format!("https://same.test/{i:04}"))
            .collect();
        for array in [
            VarBinViewArray::from_iter_str([
                "http://x.test/z",
                "http://x.test/a",
                "http://x.test/z",
                "",
            ])
            .into_array(),
            dictionary,
            ConstantArray::new("https://constant.test/c", 17).into_array(),
            VarBinViewArray::from_iter_str(many.iter().map(String::as_str)).into_array(),
        ] {
            let accessors = aggregate_direct_column_accessors_from_chunk(
                &array,
                &columns,
                &mut expected.native_execution_ctx,
            )
            .unwrap();
            assert!(
                expected
                    .update_dense_general_direct_from_transformed_dictionary(&accessors, None)
                    .unwrap()
            );
            assert!(
                actual
                    .update_compact_direct_from_chunk_profiled(&array, &columns, None, &mut timing)
                    .unwrap()
            );
            assert_eq!(
                memory.snapshot().reserved_bytes,
                0,
                "partial owner releases its capacity after merge"
            );
        }
        let (_, expected) = expected.result_row_count_and_summary(None).unwrap();
        let (_, actual) = actual.result_row_count_and_summary(None).unwrap();
        let expected: serde_json::Value = serde_json::from_str(&expected).unwrap();
        let actual: serde_json::Value = serde_json::from_str(&actual).unwrap();
        assert_eq!(actual["values"], expected["values"]);
        assert!(timing.weighted_string_entries < timing.weighted_string_rows);
    }

    #[test]
    fn weighted_string_partial_declines_nulls_and_capacity_before_contributing() {
        let request = request();
        let columns = vec!["link".into()];
        let memory = LiveMemoryPool::new(1).unwrap();
        let mut states =
            GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
        states.weighted_string_memory = Some(memory.clone());
        let mut timing = aggregate_timing::AggregateFirstPassTiming::default();
        for array in [
            VarBinViewArray::from_iter_nullable_str([Some("https://x.test/a"), None]).into_array(),
            VarBinViewArray::from_iter_str(["https://x.test/a"]).into_array(),
        ] {
            assert!(
                !states
                    .update_owned_weighted_string_chunk(&array, &columns, &mut timing)
                    .unwrap()
            );
            assert!(states.transformed_dictionary_dense_general_groups.is_none());
        }
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
