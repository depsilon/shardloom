//! Fallible metadata copies for the original vector-returning Rust writer API.
//! Native payloads and query execution are shared by the fanout owner.

use super::super::{CompletedVortexRelationalObservation, ExecutedVortexRelational, Result};
use shardloom_exec::live_memory::Budgeted;

fn copy_text(value: &Budgeted<String>) -> Result<Budgeted<String>> {
    let lease = value.lease().pool().reserve(value.reserved_bytes())?;
    Ok(Budgeted::new(value.value().clone(), lease))
}

impl ExecutedVortexRelational {
    #[allow(clippy::too_many_lines, clippy::used_underscore_binding)] // Exhaustive report copy must duplicate the existing retained metadata leases.
    pub(super) fn duplicate_for_writer(&self) -> Result<Self> {
        let metadata = self._metadata.pool().reserve(self._metadata.bytes())?;
        let mut pipeline_metadata = [None, None, None, None];
        for (target, source) in pipeline_metadata.iter_mut().zip(&self._pipeline_metadata) {
            *target = source
                .as_ref()
                .map(|lease| lease.pool().reserve(lease.bytes()))
                .transpose()?;
        }
        let observation_metadata = self
            ._observation_metadata
            .as_ref()
            .map(|lease| lease.pool().reserve(lease.bytes()))
            .transpose()?;
        let mut observations = Vec::with_capacity(self.observations.len());
        for value in &self.observations {
            let metadata = value._metadata.pool().reserve(value._metadata.bytes())?;
            observations.push(CompletedVortexRelationalObservation {
                name: value.name.clone(),
                result_jsonl: copy_text(&value.result_jsonl)?,
                result_schema_json: copy_text(&value.result_schema_json)?,
                _metadata: metadata,
            });
        }
        Ok(Self {
            composition: self.composition.clone(),
            recursion_limit: self.recursion_limit,
            observations,
            _observation_metadata: observation_metadata,
            _pipeline_metadata: pipeline_metadata,
            primitive_execution: self.primitive_execution.clone(),
            provider_selection_reason: self.provider_selection_reason,
            resources: self.resources,
            float_policy: self.float_policy,
            output_rows: self.output_rows,
            output_batches: self.output_batches,
            max_output_batch_rows: self.max_output_batch_rows,
            output_buffer_bytes: self.output_buffer_bytes,
            scan_rows_delivered: self.scan_rows_delivered,
            scan_batches: self.scan_batches,
            prepared_sources: self.prepared_sources,
            prepared_file_sources: self.prepared_file_sources,
            schema_binding_deferred: self.schema_binding_deferred,
            dynamic_schema_stages: self.dynamic_schema_stages,
            spilled_pivot_stages: self.spilled_pivot_stages,
            spilled_pivot_input_rows: self.spilled_pivot_input_rows,
            spilled_pivot_index_rows: self.spilled_pivot_index_rows,
            spilled_pivot_domains: self.spilled_pivot_domains,
            spilled_pivot_cells: self.spilled_pivot_cells,
            spilled_pivot_lookup_blocks: self.spilled_pivot_lookup_blocks,
            spilled_pivot_reader_opens: self.spilled_pivot_reader_opens,
            ordered_aggregate_stages: self.ordered_aggregate_stages,
            ordered_aggregate_input_rows: self.ordered_aggregate_input_rows,
            ordered_aggregate_distinct_rows: self.ordered_aggregate_distinct_rows,
            ordered_aggregate_quantile_rows: self.ordered_aggregate_quantile_rows,
            ordered_set_stages: self.ordered_set_stages,
            ordered_set_input_rows: self.ordered_set_input_rows,
            ordered_set_groups: self.ordered_set_groups,
            ordered_set_output_rows: self.ordered_set_output_rows,
            spillable_unary_membership_stages: self.spillable_unary_membership_stages,
            spillable_unary_input_rows: self.spillable_unary_input_rows,
            spillable_unary_observed_rows: self.spillable_unary_observed_rows,
            spillable_unary_groups: self.spillable_unary_groups,
            spillable_unary_identity_rows: self.spillable_unary_identity_rows,
            spillable_unary_output_rows: self.spillable_unary_output_rows,
            spillable_unary_lookup_blocks: self.spillable_unary_lookup_blocks,
            spillable_unary_reader_opens: self.spillable_unary_reader_opens,
            spillable_sampling_stages: self.spillable_sampling_stages,
            spillable_sampling_input_rows: self.spillable_sampling_input_rows,
            spillable_sampling_population_rows: self.spillable_sampling_population_rows,
            spillable_sampling_output_rows: self.spillable_sampling_output_rows,
            spillable_sampling_replacement_draws: self.spillable_sampling_replacement_draws,
            spillable_sampling_lookup_blocks: self.spillable_sampling_lookup_blocks,
            spillable_subquery_stages: self.spillable_subquery_stages,
            spilled_subquery_stages: self.spilled_subquery_stages,
            spillable_subquery_input_rows: self.spillable_subquery_input_rows,
            spillable_subquery_indexed_rows: self.spillable_subquery_indexed_rows,
            spillable_subquery_probe_rows: self.spillable_subquery_probe_rows,
            spillable_subquery_candidate_rows: self.spillable_subquery_candidate_rows,
            spillable_subquery_lookup_blocks: self.spillable_subquery_lookup_blocks,
            ordered_join_stages: self.ordered_join_stages,
            ordered_join_build_rows: self.ordered_join_build_rows,
            ordered_join_probe_rows: self.ordered_join_probe_rows,
            ordered_join_candidate_rows: self.ordered_join_candidate_rows,
            ordered_join_match_records: self.ordered_join_match_records,
            ordered_join_lookup_blocks: self.ordered_join_lookup_blocks,
            ordered_window_stages: self.ordered_window_stages,
            ordered_window_input_rows: self.ordered_window_input_rows,
            ordered_window_groups: self.ordered_window_groups,
            ordered_window_partitions: self.ordered_window_partitions,
            ordered_window_peer_records: self.ordered_window_peer_records,
            ordered_window_bounds_rows: self.ordered_window_bounds_rows,
            ordered_window_distinct_intervals: self.ordered_window_distinct_intervals,
            ordered_window_distinct_events: self.ordered_window_distinct_events,
            ordered_window_extrema_summary_rows: self.ordered_window_extrema_summary_rows,
            ordered_window_lookup_blocks: self.ordered_window_lookup_blocks,
            output_columns: self.output_columns.clone(),
            bytes_decoded: self.bytes_decoded,
            native_io_certificate: self.native_io_certificate.clone(),
            runtime: self.runtime,
            spill: self.spill.clone(),
            input: self.input.clone(),
            _metadata: metadata,
        })
    }
}
