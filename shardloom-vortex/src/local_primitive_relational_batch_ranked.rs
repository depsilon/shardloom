//! Two-phase reads stay under the existing once-only source lifecycle owner.

use super::{
    Consumption, Execution, NativeExecutionContext, RankedConsumer, Result, checked_add, failed,
    verify_released,
};
use crate::universal_format_io::demanded::{ColumnarReadReport, PreparedColumnarSource};
use std::sync::Arc;

impl Execution<'_, '_> {
    pub(super) fn consume_ranked(
        &self,
        index: usize,
        columns: &[String],
        prefix: usize,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn RankedConsumer,
    ) -> Result<bool> {
        let state = self
            .states
            .values
            .get(index)
            .ok_or_else(|| failed("ranked input source index is absent"))?;
        let declaration = self
            .prepared
            .batch_sources
            .get(index)
            .ok_or_else(|| failed("ranked input declaration is absent"))?;
        if state.started.get() || !state.usage.get().admits_ranked() || prefix > 16_384 {
            return Ok(false);
        }
        let Some(source) = &declaration.local else {
            return Ok(false);
        };
        let Some(rank) = source.prepare_ranking(columns, prefix, context)? else {
            return Ok(false);
        };
        context.check_cancelled()?;
        if state.started.replace(true) {
            return Err(failed("a ranked producer cannot be opened twice"));
        }
        let ranked = self.rank_phase(&rank, context, &mut |array, first| {
            consume.rank(array, first, context)
        })?;
        // The previous reader and every original batch are closed before the
        // same held generation admits any selected-payload read.
        let positions = consume.select(context)?;
        if positions.len() > prefix {
            return Err(failed("ranked selection exceeds its admitted prefix"));
        }
        let selected_rows = positions.len() as u64;
        let payload = source.prepare_selected(positions, context)?;
        drop(rank);
        let mut selected = self.rank_phase(&payload, context, &mut |array, first| {
            consume.payload(array, first, context)
        })?;
        consume.complete()?;
        context.check_cancelled()?;
        source
            .source
            .validate_generation_controlled(context.cancellation())?;
        {
            let mut report = self.report.borrow_mut();
            checked_add(&mut report.ranked_sources, 1)?;
            checked_add(
                &mut report.ranked_rank_columns,
                ranked.projected_columns as u64,
            )?;
            checked_add(&mut report.ranked_rank_rows, ranked.rows_decoded)?;
            checked_add(&mut report.ranked_payload_rows, selected.rows_decoded)?;
            checked_add(
                &mut report.ranked_rank_groups,
                ranked.row_groups_read as u64,
            )?;
            checked_add(
                &mut report.ranked_payload_groups,
                selected.row_groups_read as u64,
            )?;
            checked_add(&mut report.ranked_rank_read_bytes, ranked.data_read_bytes)?;
            checked_add(
                &mut report.ranked_payload_read_bytes,
                selected.data_read_bytes,
            )?;
            checked_add(&mut report.ranked_selected_rows, selected_rows)?;
        }
        // Payload groups are a subset of rank groups; count distinct groups and
        // source/metadata facts once, but include every actual read and decode.
        selected.row_groups_read = ranked.row_groups_read;
        checked_add(&mut selected.rows_decoded, ranked.rows_decoded)?;
        checked_add(&mut selected.data_read_bytes, ranked.data_read_bytes)?;
        checked_add(&mut selected.data_read_requests, ranked.data_read_requests)?;
        self.record_columnar_input(selected)?;
        self.finish_producer(state, None, Consumption::Query)?;
        Ok(true)
    }

    fn rank_phase(
        &self,
        source: &Arc<PreparedColumnarSource>,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(&super::ArrayRef, u64) -> Result<()>,
    ) -> Result<ColumnarReadReport> {
        let mut reader = source.open(context)?;
        loop {
            context.check_cancelled()?;
            let owner = self
                .original_owner
                .try_borrow_mut()
                .map_err(|_| failed("ranked input requires releasing the original source batch"))?;
            let Some((batch, first)) =
                reader.next_native_located(&self.prepared.session, context)?
            else {
                return reader.report();
            };
            if !batch.belongs_to_session(&self.prepared.session) {
                return Err(failed("ranked input changed its native memory owner"));
            }
            let released = batch.batch_release_witness()?;
            self.record_input(&batch)?;
            let result = consume(batch.batch_array(context)?, first);
            drop(batch);
            result?;
            verify_released(&released)?;
            drop(owner);
        }
    }
}
