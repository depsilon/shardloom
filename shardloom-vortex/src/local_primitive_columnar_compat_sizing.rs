//! Conversion geometry under the writer's existing retained workspace owner.

use super::{CompatibilityWork, PreparedCompatibilityExport, Result, Writer, error, usize_to_u64};
use crate::resident_session::NativeExecutionContext;
use shardloom_exec::adaptive_work::{WorkCost, WorkDecision, WorkLimits, WorkShape};
use shardloom_exec::live_memory::LiveMemorySnapshot;

const OWNER: &str = "compatibility_writer";

/// The prepared root dtype is shared through Vortex's `StructFields` owner.
/// Charge fresh child vectors/facades and nested canonical metadata, without
/// charging another root name/type tree that this conversion never constructs.
pub(super) fn canonical_metadata(
    dtype: &vortex::array::dtype::DType,
    context: &NativeExecutionContext<'_>,
) -> Result<u64> {
    let fields = dtype
        .as_struct_fields_opt()
        .ok_or_else(|| error("canonical output requires a struct"))?;
    let slots = usize_to_u64(fields.nfields())?
        .checked_mul(4 * std::mem::size_of::<vortex::array::ArrayRef>() as u64)
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or_else(|| error("canonical metadata size overflow"))?;
    fields.fields().try_fold(slots, |bytes, child| {
        bytes
            .checked_add(crate::native_payload_schema::metadata_bytes(
                &child, context,
            )?)
            .ok_or_else(|| error("canonical metadata size overflow"))
    })
}

pub(super) struct ConversionWindow<'a> {
    export: &'a PreparedCompatibilityExport,
    initial: u64,
    overhead: u64,
    source_rows: usize,
    schema_bytes: u64,
    gather_scratch: u64,
}

impl<'a> ConversionWindow<'a> {
    pub(super) fn new(
        export: &'a PreparedCompatibilityExport,
        initial: u64,
        source_rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        Ok(Self {
            export,
            initial,
            overhead: export.writer_overhead()?,
            source_rows,
            schema_bytes: crate::native_payload_schema::schema_bytes(&export.plan.dtype, context)?,
            gather_scratch: crate::local_primitives::native_payload::gather_scratch_per_row(
                &export.plan.dtype,
            )?,
        })
    }

    pub(super) fn required(&self, buffered: u64, conversion: u64, metadata: u64) -> Result<u64> {
        buffered
            .checked_add(conversion)
            .and_then(|bytes| bytes.checked_mul(8))
            .and_then(|bytes| bytes.checked_add(metadata))
            .and_then(|bytes| bytes.checked_add(self.overhead))
            .map(|bytes| bytes.max(self.initial))
            .ok_or_else(|| error("writer admission overflow"))
    }

    #[allow(clippy::too_many_arguments)] // All inputs identify the one retained writer envelope.
    pub(super) fn select(
        &self,
        writer: &mut Writer<'_>,
        work: &mut CompatibilityWork,
        prepaid: u64,
        maximum: usize,
        width: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<WorkShape> {
        context.check_cancelled()?;
        let mut decision = self.decide(
            writer,
            work.arrow_batches,
            prepaid,
            maximum,
            width,
            context.memory().snapshot(),
        )?;
        if matches!(decision, WorkDecision::Declined(_)) {
            context.memory().reclaim_optional_memory();
            decision = self.decide(
                writer,
                work.arrow_batches,
                prepaid,
                maximum,
                width,
                context.memory().snapshot(),
            )?;
        }
        if matches!(decision, WorkDecision::Declined(_)) && writer.buffered_input_bytes > 0 {
            // Flushing completes existing admitted state once. Selection never
            // retries a write, input producer or application callback.
            writer.flush(work, true)?;
            decision = self.decide(
                writer,
                work.arrow_batches,
                prepaid,
                maximum,
                width,
                context.memory().snapshot(),
            )?;
        }
        decision.require_selected(OWNER)
    }

    #[allow(clippy::too_many_arguments)]
    fn decide(
        &self,
        writer: &Writer<'_>,
        batches: u64,
        prepaid: u64,
        maximum: usize,
        width: u64,
        memory: LiveMemorySnapshot,
    ) -> Result<WorkDecision> {
        // Geometric candidates preserve the existing finite pre-write split
        // policy and include the exact incoming ceiling. No allocation or
        // provider operation takes place while inspecting these candidates.
        let candidates = std::iter::successors(Some(1_usize), move |rows| {
            (*rows < maximum).then(|| rows.saturating_mul(2).min(maximum))
        });
        writer.sizing.decide_candidates(
            OWNER,
            memory,
            WorkLimits {
                min_units: 1,
                max_units: usize_to_u64(maximum)?,
                max_in_flight: 1,
            },
            candidates.map(|rows| {
                Ok((
                    usize_to_u64(rows)?,
                    self.cost(writer, batches, prepaid, rows, width)?,
                ))
            }),
        )
    }

    pub(super) fn cost(
        &self,
        writer: &Writer<'_>,
        batches: u64,
        prepaid: u64,
        rows: usize,
        width: u64,
    ) -> Result<WorkCost> {
        let metadata = self
            .export
            .footer_metadata(writer.pending_units(batches, rows)?)?;
        let conversion = width
            .checked_mul(usize_to_u64(rows)?)
            .ok_or_else(|| error("conversion estimate overflow"))?;
        let additional = self
            .required(writer.buffered_input_bytes, conversion, metadata)?
            .saturating_sub(prepaid);
        // The original validation window is already held in the shared pool.
        // Whole-window conversion reuses it. A selected part may construct a
        // new schema, native views and compact nested child/index domains.
        let (fixed, bytes_per_unit) = if rows == self.source_rows {
            (0, 1)
        } else {
            (
                self.schema_bytes
                    .checked_add(4096)
                    .ok_or_else(|| error("conversion schema estimate overflow"))?,
                width
                    .checked_mul(2)
                    .and_then(|bytes| bytes.checked_add(self.gather_scratch))
                    .and_then(|bytes| bytes.checked_add(16))
                    .ok_or_else(|| error("conversion scratch estimate overflow"))?,
            )
        };
        Ok(WorkCost {
            fixed_bytes: fixed
                .checked_add(additional)
                .ok_or_else(|| error("conversion workspace estimate overflow"))?,
            bytes_per_unit,
        })
    }
}
