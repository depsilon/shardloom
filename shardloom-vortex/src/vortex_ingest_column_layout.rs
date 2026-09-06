//! Private paired-writer seam. The default remains the measured retained writer.

use super::*;
use crate::column_addressable_layout::{
    ColumnAddressableLayout, ColumnLayoutBounds, ColumnLayoutCounters,
};
use vortex::{array::dtype::DType, file::WriteOptionsSessionExt as _};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamFooterLayout {
    RetainedRows,
    ColumnAddressable,
}

// A frozen comparison candidate may change this one private selection. There is
// deliberately no public option or environment-dependent production dispatch.
pub(super) const DEFAULT_STREAM_FOOTER_LAYOUT: StreamFooterLayout =
    StreamFooterLayout::RetainedRows;

pub(super) struct StreamLayoutEvidence {
    pub(super) status: &'static str,
    pub(super) counters: Option<Arc<ColumnLayoutCounters>>,
}

impl StreamLayoutEvidence {
    pub(super) fn append_to(&self, applied: &mut String) {
        use std::fmt::Write as _;
        if let Some(counters) = &self.counters {
            let snapshot = counters.snapshot();
            // String formatting is infallible. These are completed writer
            // counts, not predictions based on the source's batch estimate.
            let _ = write!(
                applied,
                ";footer_layout={};actual_nonempty_groups={};empty_groups={};child_writer_calls={};transposed_column_references={};peak_owned_layout_reference_bytes={};reference_scope=footer_vectors_and_metadata_only",
                self.status,
                snapshot.input_groups,
                snapshot.empty_groups,
                snapshot.child_writer_calls,
                snapshot.transposed_references,
                snapshot.peak_reference_bytes
            );
        } else if self.status != "retained_source_batch_rows" {
            let _ = write!(applied, ";footer_layout={}", self.status);
        }
    }
}

pub(super) fn stream_options(
    context: &LocalVortexWriteContext,
    decision: &VortexLayoutWriteRuntimeDecision,
    timing: &VortexWriterStageTiming,
    memory: Option<&NativeIngestMemory>,
    dtype: &DType,
    requested: StreamFooterLayout,
) -> Result<(vortex::file::VortexWriteOptions, StreamLayoutEvidence)> {
    let Some(memory) = memory else {
        return Ok((
            context.write_options_for_decision(decision, timing),
            StreamLayoutEvidence {
                status: if requested == StreamFooterLayout::ColumnAddressable {
                    "retained_writer_candidate_requires_shared_memory"
                } else {
                    "retained_source_batch_rows"
                },
                counters: None,
            },
        ));
    };
    let bounds = ColumnLayoutBounds::default();
    let child = context.strategy_for_decision(decision, timing, &memory.session);
    if requested == StreamFooterLayout::ColumnAddressable
        && ColumnAddressableLayout::admits_dtype(dtype, bounds)
    {
        let strategy = ColumnAddressableLayout::new(child, memory.pool.clone(), bounds)
            .map_err(vortex_error)?;
        let counters = strategy.counters();
        return Ok((
            memory
                .session
                .write_options()
                .with_strategy(Arc::new(strategy)),
            StreamLayoutEvidence {
                status: "native_struct_column_chunked_preserved_subtrees",
                counters: Some(counters),
            },
        ));
    }
    // The original writer remains available for statically inadmissible schema.
    // Once an admitted candidate begins writing, a malformed native batch or
    // incompatible child layout is an error, never a mid-stream route change.
    Ok((
        memory.session.write_options().with_strategy(Arc::new(
            bounded_ingest_layout::BoundedIngestLayout::new(child, 0, memory.pool.reserve(0)?),
        )),
        StreamLayoutEvidence {
            status: if requested == StreamFooterLayout::ColumnAddressable {
                "retained_source_batch_rows_candidate_schema_not_admitted"
            } else {
                "retained_source_batch_rows"
            },
            counters: None,
        },
    ))
}
