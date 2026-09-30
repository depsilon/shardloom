//! Select complete COUNT winners with the existing numeric-count consumer, then
//! push their keys into the existing Vortex scan and mixed-measure consumer.

use super::{
    AggregateSingleNumericKey, AggregateValueTransform, ColumnRef, DatasetUri,
    GroupedAggregateStates, PredicateExpr, Result, ShardLoomError, SimpleAggregateFunction,
    SingleNumericAggregateOrderCandidate, StatValue, VortexLocalPrimitiveExecutionPolicy,
    VortexQueryPrimitiveRequest, VortexReaderBackedSplitEvidence,
    aggregate_scan_runtime::AggregateScanRuntime, aggregate_scan_source::AggregateScanSource,
    aggregate_timing::AggregateFirstPassTiming, compare_single_numeric_candidates,
    native_numeric_execution_ctx, projection_request_from_declared_columns, projection_scan_plan,
    vortex_error,
};
use crate::{VortexAggregateOrderExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use rustc_hash::FxHashMap;
use std::time::Instant;
use vortex::{
    array::dtype::{DType, Nullability},
    session::VortexSession,
};

const SAMPLE_ROWS: u64 = 262_144;
const MAX_CHUNK_ROWS: usize = 262_144;
const MAX_COUNT_GROUPS: usize = 65_536;

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native winner-only DISTINCT {reason}; no fallback execution was attempted"
    ))
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
        count_request: VortexSimpleAggregateRequest::grouped(
            states.request.group_by.clone(),
            vec![VortexSimpleAggregateMeasure::new(
                "count",
                None,
                count.alias.clone(),
            )],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new(&count.alias, true)]),
        columns: vec![columns[group.column_index].clone()],
        retained_cap,
        source_rows,
        candidate_groups: 0,
        retained_groups: 0,
        count_rows: 0,
        retained_rows: 0,
        measure_rows: 0,
        sample_rows: 0,
        sample_retained_rows: 0,
        count_pass_nanos: 0,
        count_timing: AggregateFirstPassTiming::default(),
        decision: "not_started",
        filter: None,
    })
}

pub(super) struct Report {
    count_request: VortexSimpleAggregateRequest,
    columns: Vec<String>,
    retained_cap: usize,
    source_rows: u64,
    candidate_groups: usize,
    retained_groups: usize,
    count_rows: u64,
    retained_rows: u64,
    pub(super) measure_rows: u64,
    sample_rows: u64,
    sample_retained_rows: u64,
    count_pass_nanos: u128,
    count_timing: AggregateFirstPassTiming,
    decision: &'static str,
    pub(super) filter: Option<PredicateExpr>,
}

impl Report {
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) fn prepare(
        &mut self,
        file: AggregateScanSource<'_>,
        source_uri: &DatasetUri,
        request: &VortexQueryPrimitiveRequest,
        policy: VortexLocalPrimitiveExecutionPolicy,
        session: &VortexSession,
        runtime: &impl AggregateScanRuntime,
        check_cancelled: &impl Fn() -> Result<()>,
        uncached_retry: &mut Option<&mut dyn FnMut(&vortex::error::VortexError) -> bool>,
        reader_splits: &mut Vec<VortexReaderBackedSplitEvidence>,
        max_chunk_rows: &mut usize,
    ) -> Result<()> {
        let started = Instant::now();
        let mut states = GroupedAggregateStates::new_with_resource_envelope(
            &self.count_request,
            Some(self.retained_cap),
            &self.columns,
            false,
            false,
            policy.resource_envelope(),
        )?;
        states.native_execution_ctx = native_numeric_execution_ctx(session);
        let projection = projection_request_from_declared_columns(&self.columns)?;
        let mut plan = projection_scan_plan(file.dtype(), &projection, request.kind)?;
        if plan.projected_columns != self.columns {
            return Err(failed("count projection changed column order"));
        }
        let mut scan = file.scan(session).map_err(vortex_error)?;
        if let Some(projection) = plan.projection.take() {
            scan = scan.with_projection(file.bind(&projection)?);
        }
        let mut scan = scan
            .with_concurrency(policy.scan_concurrency_per_worker())
            .into_array_iter(runtime)
            .map_err(vortex_error)?;
        self.decision = "complete_count_then_selected_measures";
        loop {
            check_cancelled()?;
            let scan_started = Instant::now();
            let chunk = scan.next();
            self.count_timing.scan_next_nanos += scan_started.elapsed().as_nanos();
            let Some(chunk) = chunk else {
                break;
            };
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    // Preserve the enclosing prepared-source retry decision.
                    if let Some(retry) = uncached_retry.as_mut() {
                        retry(&error);
                    }
                    return Err(vortex_error(error));
                }
            };
            check_cancelled()?;
            let rows = chunk.len();
            reader_splits.push(VortexReaderBackedSplitEvidence::local_scan_chunk(
                source_uri.clone(),
                reader_splits.len(),
                rows,
                chunk.dtype().to_string(),
                chunk.encoding_id().to_string(),
                chunk.nchildren(),
                chunk.nbuffers(),
            )?);
            *max_chunk_rows = (*max_chunk_rows).max(rows);
            if rows > MAX_CHUNK_ROWS {
                self.decision = "declined_auxiliary_chunk_bound";
                break;
            }
            if !states.update_compact_direct_from_chunk_profiled(
                &chunk,
                &self.columns,
                None,
                &mut self.count_timing,
            )? {
                return Err(failed("lost the admitted numeric COUNT contract"));
            }
            self.count_rows = self
                .count_rows
                .checked_add(super::usize_to_u64(rows)?)
                .ok_or_else(|| failed("count row total overflowed"))?;
            let Some(groups) = states.single_numeric_count_groups.as_ref() else {
                if rows == 0 {
                    continue;
                }
                return Err(failed("numeric COUNT state is missing"));
            };
            self.candidate_groups = groups.len();
            if groups.len() > MAX_COUNT_GROUPS {
                self.decision = "declined_auxiliary_group_bound";
                break;
            }
            if self.sample_rows == 0 && self.count_rows >= SAMPLE_ROWS {
                self.sample_rows = self.count_rows;
                self.sample_retained_rows = selected(groups, self.retained_cap)
                    .iter()
                    .map(|candidate| candidate.count)
                    .sum();
                if high_winner_share(self.sample_retained_rows, self.sample_rows) {
                    self.decision = "declined_high_sample_winner_share";
                    break;
                }
            }
        }
        check_cancelled()?;
        let counts = states
            .single_numeric_count_groups
            .take()
            .unwrap_or_default();
        // Drop the native count consumer before measure state is populated.
        drop(states);
        if self.decision == "complete_count_then_selected_measures" {
            self.finish_counts(&counts)?;
        }
        self.count_pass_nanos = started.elapsed().as_nanos();
        Ok(())
    }

    fn finish_counts(&mut self, counts: &FxHashMap<AggregateSingleNumericKey, u64>) -> Result<()> {
        if self.count_rows != self.source_rows {
            return Err(failed("complete count weight differs from held source"));
        }
        self.candidate_groups = counts.len();
        let retained = selected(counts, self.retained_cap);
        self.retained_groups = retained.len();
        self.retained_rows = retained.iter().map(|candidate| candidate.count).sum();
        self.filter = Some(PredicateExpr::InList {
            column: ColumnRef::new(&self.columns[0])?,
            values: retained
                .iter()
                .map(|candidate| {
                    if candidate.key.signed {
                        StatValue::Int64(i64::from_ne_bytes(candidate.key.bits.to_ne_bytes()))
                    } else {
                        StatValue::UInt64(candidate.key.bits)
                    }
                })
                .collect(),
            negated: false,
        });
        Ok(())
    }

    pub(super) fn verify_measure_rows(&self) -> Result<()> {
        if self.measure_rows == self.retained_rows {
            Ok(())
        } else {
            Err(failed(
                "selected measure row weight differs from complete counts",
            ))
        }
    }

    pub(super) fn annotate(&self, summary: &mut String) -> Result<()> {
        let mut value: serde_json::Value = serde_json::from_str(summary)
            .map_err(|error| ShardLoomError::InvalidOperation(error.to_string()))?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| failed("summary is not an object"))?;
        object.insert("aggregate_winner_distinct".into(), serde_json::json!({
            "decision": self.decision,
            "strategy": "shared_numeric_count_then_native_in_filter_and_unchanged_mixed_measures",
            "proof": "complete_count_star_order_then_complete_key_ties_distinct_cannot_affect_selection",
            "candidate_groups": self.candidate_groups,
            "retained_groups": self.retained_groups,
            "retained_cap": self.retained_cap,
            "source_rows": self.source_rows,
            "count_rows": self.count_rows,
            "retained_rows": self.retained_rows,
            "measure_rows": self.measure_rows,
            "sample_rows": self.sample_rows,
            "sample_retained_rows": self.sample_retained_rows,
            "count_pass_nanos": u64::try_from(self.count_pass_nanos).unwrap_or(u64::MAX),
            "count_scan_next_nanos": u64::try_from(self.count_timing.scan_next_nanos).unwrap_or(u64::MAX),
            "count_accessor_nanos": u64::try_from(self.count_timing.accessor_nanos).unwrap_or(u64::MAX),
            "count_group_update_nanos": u64::try_from(self.count_timing.group_update_nanos).unwrap_or(u64::MAX),
            "count_accessor_chunks": self.count_timing.accessor_chunks,
            "count_accessor_rows": self.count_timing.accessor_rows,
            "count_columns": self.columns,
            "max_auxiliary_groups": MAX_COUNT_GROUPS,
            "max_auxiliary_chunk_rows": MAX_CHUNK_ROWS,
            "sample_winner_share_limit_percent": 70,
            "scope": "auxiliary_count_pass_in_addition_to_existing_measure_pass_timers_not_cpu_or_complete_wall",
        }));
        *summary = value.to_string();
        Ok(())
    }
}

fn selected(
    counts: &FxHashMap<AggregateSingleNumericKey, u64>,
    retained_cap: usize,
) -> Vec<SingleNumericAggregateOrderCandidate> {
    let mut candidates = counts
        .iter()
        .map(|(key, count)| SingleNumericAggregateOrderCandidate {
            key: *key,
            count: *count,
        })
        .collect();
    GroupedAggregateStates::capillary_select_single_numeric_candidates(
        &mut candidates,
        retained_cap,
    );
    candidates.sort_by(compare_single_numeric_candidates);
    candidates
}

fn high_winner_share(retained: u64, rows: u64) -> bool {
    u128::from(retained) * 10 >= u128::from(rows) * 7
}

#[cfg(test)]
#[path = "winner_distinct_tests.rs"]
mod tests;
