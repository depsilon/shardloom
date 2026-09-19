//! Disjoint scopes on the caller thread; provider worker activity may overlap them.

use super::{Result, ShardLoomError};

#[derive(Default)]
pub(super) struct AggregateFirstPassTiming {
    pub scan_next_nanos: u128,
    pub reader_evidence_nanos: u128,
    pub accessor_nanos: u128,
    pub group_update_nanos: u128,
    pub finalization_nanos: u128,
    pub accessor_chunks: u64,
    pub accessor_rows: u64,
}

impl AggregateFirstPassTiming {
    pub(super) fn annotate_summary(&self, summary: &mut String) -> Result<()> {
        let mut payload: serde_json::Value = serde_json::from_str(summary)
            .map_err(|error| ShardLoomError::InvalidOperation(error.to_string()))?;
        let object = payload.as_object_mut().ok_or_else(|| {
            ShardLoomError::InvalidOperation("aggregate summary must be an object".into())
        })?;
        for (key, value) in [
            ("aggregate_first_pass_scan_next_nanos", self.scan_next_nanos),
            (
                "aggregate_first_pass_reader_evidence_nanos",
                self.reader_evidence_nanos,
            ),
            ("aggregate_first_pass_accessor_nanos", self.accessor_nanos),
            (
                "aggregate_first_pass_group_update_nanos",
                self.group_update_nanos,
            ),
            (
                "aggregate_result_finalization_nanos",
                self.finalization_nanos,
            ),
            (
                "aggregate_first_pass_accessor_chunks",
                u128::from(self.accessor_chunks),
            ),
            (
                "aggregate_first_pass_accessor_rows",
                u128::from(self.accessor_rows),
            ),
        ] {
            object.insert(key.into(), u64::try_from(value).unwrap_or(u64::MAX).into());
        }
        object.insert("aggregate_timing_scope".into(),
            "disjoint_caller_elapsed_scopes_first_pass_scan_next_reader_evidence_compact_group_accessors_and_updates_plus_final_result_render_not_cpu_or_complete_wall;scan_next_includes_provider_progress;residual_row_updates_and_later_passes_not_instrumented".into());
        *summary = payload.to_string();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::VortexAggregateOrderExpr;

    fn request() -> VortexSimpleAggregateRequest {
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("product_name").unwrap()],
            vec![VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "n".to_string(),
            )],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new("n", true)])
    }

    #[test]
    fn nullable_schema_keeps_mixed_chunks_in_one_exact_native_state() {
        use vortex::array::{
            IntoArray as _,
            arrays::VarBinViewArray,
            dtype::{DType, FieldNames, Nullability, StructFields},
        };
        let request = request();
        let columns = vec!["product_name".to_string()];
        let dtype = DType::Struct(
            StructFields::new(
                FieldNames::from(["product_name"]),
                vec![DType::Utf8(Nullability::Nullable)],
            ),
            Nullability::NonNullable,
        );
        let compact_admitted = aggregate_group_key_dtypes_nonnullable(&dtype, &request);
        assert!(!compact_admitted);
        let mut states = GroupedAggregateStates::new_with_resource_envelope(
            &request,
            Some(2),
            &columns,
            false,
            compact_admitted,
            VortexLocalPrimitiveResourceEnvelope::new(24, 12).unwrap(),
        )
        .unwrap();
        let mut timing = super::AggregateFirstPassTiming::default();
        for values in [
            vec![Some("tea"), Some("tea")],
            vec![None, None, None, Some("tea")],
        ] {
            let array = VarBinViewArray::from_iter_nullable_str(values).into_array();
            assert!(
                states
                    .update_compact_direct_from_chunk_profiled(&array, &columns, None, &mut timing)
                    .unwrap()
            );
        }
        assert!(
            states
                .string_count_topk_first_pass_exact_histogram_counts
                .is_none()
        );
        assert!(states.string_count_topk_heavy_hitter_sketch.is_none());
        let (rows, mut summary) = states.result_row_count_and_summary(Some(2)).unwrap();
        timing.annotate_summary(&mut summary).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(rows, 2);
        let values = payload["values"].as_array().unwrap();
        assert!(
            values
                .iter()
                .any(|value| value["product_name"].is_null() && value["n"] == 3)
        );
        assert!(
            values
                .iter()
                .any(|value| value["product_name"] == "tea" && value["n"] == 3)
        );
        assert_eq!(
            values
                .iter()
                .map(|value| value["n"].as_u64().unwrap())
                .sum::<u64>(),
            6
        );
        assert_eq!(payload["aggregate_first_pass_accessor_chunks"], 2);
        assert_eq!(payload["aggregate_first_pass_accessor_rows"], 6);
    }

    #[test]
    fn exact_histogram_interns_only_referenced_values_across_changed_dictionary_domains() {
        let request = request();
        let columns = vec!["product_name".to_string()];
        let mut states = GroupedAggregateStates::new_with_resource_envelope(
            &request,
            Some(2),
            &columns,
            false,
            true,
            VortexLocalPrimitiveResourceEnvelope::new(24, 12).unwrap(),
        )
        .unwrap();
        states
            .enable_string_count_topk_first_pass_exact_histogram()
            .unwrap();
        for (ids, values) in [
            (vec![0, 1, 1], ["tea", "coffee", "unused"]),
            (vec![0, 0, 1], ["coffee", "tea", "unused"]),
        ] {
            let accessor = AggregateDirectColumnAccessor::Utf8Dictionary {
                row_ids: ids,
                values: values
                    .into_iter()
                    .map(std::sync::Arc::<str>::from)
                    .collect(),
                value_nulls: None,
                row_nulls: None,
                source: AggregateUtf8DictionarySource::VortexDictArray,
            };
            assert!(
                states
                    .update_string_count_topk_heavy_hitter_from_accessors(&[accessor], None)
                    .unwrap()
            );
        }
        assert!(states.string_interner.id("unused").is_none());
        assert!(states.promote_string_count_topk_first_pass_exact_histogram_if_possible());
        let (_, summary) = states.result_row_count_and_summary(Some(2)).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(payload["values"][0]["product_name"], "coffee");
        assert_eq!(payload["values"][0]["n"], 4);
        assert_eq!(payload["values"][1]["product_name"], "tea");
        assert_eq!(payload["values"][1]["n"], 2);
    }
}
