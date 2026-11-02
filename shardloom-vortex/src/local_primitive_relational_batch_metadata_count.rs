//! Metadata completion shares the once-only input and generation lifecycle.

use super::{Consumption, Execution, NativeExecutionContext, Result, checked_add, failed};
use shardloom_core::native_io_decisions::{
    NativeIoCapability, NativeIoDecisionOutcome, NativeIoDecisionReason, NativeIoObservedWork,
};

impl Execution<'_, '_> {
    pub(super) fn consume_metadata_count(
        &self,
        index: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<u64>> {
        let state = self
            .states
            .values
            .get(index)
            .ok_or_else(|| failed("metadata count input source index is absent"))?;
        let declaration = self
            .prepared
            .batch_sources
            .get(index)
            .ok_or_else(|| failed("metadata count input declaration is absent"))?;
        context.check_cancelled()?;
        if state.started.get() || !state.usage.get().admits_independent_read() {
            decline(context, NativeIoDecisionReason::SharedSourceUsage)?;
            return Ok(None);
        }
        let Some(source) = &declaration.local else {
            decline(context, NativeIoDecisionReason::SourceCapabilityUnavailable)?;
            return Ok(None);
        };
        let _owner = self
            .original_owner
            .try_borrow_mut()
            .map_err(|_| failed("metadata count requires releasing the original source batch"))?;
        let Some((rows, report)) = source.exact_metadata_count(context)? else {
            decline(context, NativeIoDecisionReason::SourceCapabilityUnavailable)?;
            return Ok(None);
        };
        context.check_cancelled()?;
        if state.started.replace(true) {
            return Err(failed("a metadata count source cannot be completed twice"));
        }
        self.record_columnar_input(report)?;
        {
            let mut report = self.report.borrow_mut();
            checked_add(&mut report.metadata_count_sources, 1)?;
            checked_add(&mut report.metadata_count_rows, rows)?;
        }
        self.finish_producer(state, None, Consumption::Query)?;
        context.record_capability_decision(
            NativeIoCapability::MetadataCount,
            NativeIoDecisionOutcome::Executed,
            NativeIoDecisionReason::ExactHeldMetadata,
            NativeIoObservedWork {
                input_rows: Some(rows),
                output_rows: Some(1),
                ..NativeIoObservedWork::default()
            },
        )?;
        Ok(Some(rows))
    }
}

fn decline(context: &NativeExecutionContext<'_>, reason: NativeIoDecisionReason) -> Result<()> {
    context.record_capability_decision(
        NativeIoCapability::MetadataCount,
        NativeIoDecisionOutcome::Declined,
        reason,
        NativeIoObservedWork::default(),
    )
}
