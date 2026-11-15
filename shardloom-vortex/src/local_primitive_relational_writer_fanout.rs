//! Every multi-output action shares one certified execution and its credited replay.

use super::super::{DType, Nullability, ReservedVec, batch_input, replay};
use super::{
    ArrayRef, CancellationToken, ExecutedVortexRelational, NativeExecutionContext,
    PreparedVortexRelational, Result, VortexLocalPrimitiveRowExportFormat,
    VortexLocalPrimitiveRowExportReport, failed, vortex_error,
};
use crate::local_primitives::{completed_result, output_fanout};
use shardloom_core::native_io_decisions::NativeIoCapabilityDecisions;
use std::{cell::RefCell, path::PathBuf};

// Replay merging overlaps the producing operator's final reader. Bound both
// the producer handoff and stored replay blocks before that overlap, rather
// than slicing a larger block only when an output adapter consumes it.
const OUTPUT_REPLAY_ROWS: usize = 256;

/// Native result retention used only when one execution feeds several writers.
/// Buffer bytes count copied native buffers, not capacity, decoded bytes or RSS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexRelationalOutputReplay {
    pub batches_detached: u64,
    pub rows_detached: u64,
    pub buffer_bytes_copied: u64,
    pub spilled: bool,
    pub replays: usize,
    /// The native run owner used by replay, including its other operator runs.
    pub spill: Option<crate::relational_query::VortexRelationalSpillReport>,
    /// Peak live run bytes across the producer and replay's shared quota scope.
    /// Overlapping executions using the same spill declaration also contribute.
    pub shared_spill_peak_disk_bytes: Option<u64>,
    /// Completed output retention and deliveries, separate from source replay
    /// and representation intake in the execution certificate.
    pub capability_decisions: NativeIoCapabilityDecisions,
}

impl VortexRelationalOutputReplay {
    pub(crate) fn record_capture(&mut self) -> Result<()> {
        use shardloom_core::native_io_decisions::{
            NativeIoCapability, NativeIoDecisionOutcome, NativeIoDecisionReason,
            NativeIoObservedWork,
        };
        if self.buffer_bytes_copied > 0 {
            self.capability_decisions.record(
                NativeIoCapability::RepresentationRetention,
                NativeIoDecisionOutcome::Declined,
                NativeIoDecisionReason::RequiredRepresentationCopy,
                NativeIoObservedWork {
                    input_rows: Some(self.rows_detached),
                    copied_buffer_bytes: Some(self.buffer_bytes_copied),
                    ..NativeIoObservedWork::default()
                },
            )?;
        }
        Ok(())
    }

    pub(crate) fn record_delivery(&mut self) -> Result<()> {
        use shardloom_core::native_io_decisions::{
            NativeIoCapability, NativeIoDecisionOutcome, NativeIoDecisionReason,
            NativeIoObservedWork,
        };
        self.capability_decisions.record(
            NativeIoCapability::NativeReplay,
            NativeIoDecisionOutcome::Executed,
            if self.spilled {
                NativeIoDecisionReason::AdmittedNativeSpill
            } else {
                NativeIoDecisionReason::ResidentRetention
            },
            NativeIoObservedWork {
                output_rows: Some(self.rows_detached),
                ..NativeIoObservedWork::default()
            },
        )
    }
}

/// One completed execution and every independently certified output adapter.
pub struct WrittenVortexRelationalFanout {
    pub execution: ExecutedVortexRelational,
    pub outputs: Vec<VortexLocalPrimitiveRowExportReport>,
    /// A single destination streams directly and has no retained output replay.
    pub replay: Option<VortexRelationalOutputReplay>,
}

impl PreparedVortexRelational {
    /// Execute once and deliver the completed native result to every destination.
    /// A single target streams directly; multiple targets use the existing
    /// credited native replay, with spill only under the caller's explicit policy.
    /// # Errors
    /// Propagates source, resource, cancellation, adapter and publication failures.
    pub fn write_many_shared_controlled(
        &self,
        targets: &[(PathBuf, VortexLocalPrimitiveRowExportFormat)],
        overwrite: bool,
        cancellation: &CancellationToken,
    ) -> Result<WrittenVortexRelationalFanout> {
        self.write_many_with_completion(targets, overwrite, cancellation, None, |_| Ok(()))
            .map(|(written, ())| written)
    }

    pub(in super::super) fn write_many_with_input(
        &self,
        targets: &[(PathBuf, VortexLocalPrimitiveRowExportFormat)],
        overwrite: bool,
        cancellation: &CancellationToken,
        input: batch_input::Provider<'_>,
    ) -> Result<WrittenVortexRelationalFanout> {
        self.write_many_with_completion(targets, overwrite, cancellation, Some(input), |_| Ok(()))
            .map(|(written, ())| written)
    }

    // The legacy vector-returning API prepares its duplicated *reports* before
    // publication. It never repeats computation, and metadata admission failure
    // cannot turn a successful publish into an unreported result.
    #[allow(clippy::too_many_lines)] // One execution/staging/publication lifecycle keeps failure cleanup explicit.
    pub(in super::super) fn write_many_with_completion<T>(
        &self,
        targets: &[(PathBuf, VortexLocalPrimitiveRowExportFormat)],
        overwrite: bool,
        cancellation: &CancellationToken,
        input: Option<batch_input::Provider<'_>>,
        prepare_reports: impl FnOnce(&ExecutedVortexRelational) -> Result<T>,
    ) -> Result<(WrittenVortexRelationalFanout, T)> {
        let sampling = self.sampling_failure_guard();
        self.validate_batch_provider(input.is_some())?;
        cancellation.check()?;
        for (path, _) in targets {
            self.validate_writer_destination(path, cancellation)?;
        }
        output_fanout::preflight_targets(targets, overwrite)?;
        if let [(path, format)] = targets {
            let written = self.write_with_input(path, *format, overwrite, cancellation, input)?;
            // Both callers use an infallible empty result for the single target.
            let reports = prepare_reports(&written.execution)?;
            sampling.complete();
            return Ok((
                WrittenVortexRelationalFanout {
                    execution: written.execution,
                    outputs: vec![written.output],
                    replay: None,
                },
                reports,
            ));
        }
        let sampling_call = self.begin_sampling_call()?;
        let (mut execution, (staged, mut replay)) = self.session.with_sources_execution(
            &self.sources, cancellation, |context| {
                self.with_bound_root(context, input, |root, metrics| {
                    let (_declaration_metadata, request) = self.writer_request(root, context)?;
                    let dtype = DType::struct_(root.fields.clone(), Nullability::NonNullable);
                    let mut writers = ReservedVec::new(context.memory())?;
                    for (_, format) in targets {
                        writers.push(completed_result::prepare_stream(
                            self.writer_plan(root)?, &request, *format, self.policy, cancellation,
                        )?)?;
                    }
                    let schema = replay::Schema::new(&dtype, context)?;
                    let recorder = RefCell::new(Some(replay::Recorder::with_batch_rows(
                        &schema, metrics, context, OUTPUT_REPLAY_ROWS,
                    )?));
                    let copied = RefCell::new(replay::CopyStats::default());
                    let (execution, completed) = self.consume_bound_with_completion(
                        root, context, metrics, OUTPUT_REPLAY_ROWS,
                        &mut |array| {
                            recorder.try_borrow_mut().map_err(|_| failed("output replay is active"))?
                                .as_mut().ok_or_else(|| failed("output replay is already complete"))?
                                .build(&array, &mut copied.borrow_mut(), context)
                        },
                        || {
                            let replay = recorder.try_borrow_mut().map_err(|_| failed("output replay is active"))?
                                .take().ok_or_else(|| failed("output replay is absent"))?.finish(context)?;
                            let copied = copied.borrow();
                            let mut report = VortexRelationalOutputReplay {
                                batches_detached: copied.batches,
                                rows_detached: copied.rows,
                                buffer_bytes_copied: copied.buffer_bytes,
                                spilled: replay.is_spilled(), replays: targets.len(),
                                spill: None, shared_spill_peak_disk_bytes: None,
                                capability_decisions: NativeIoCapabilityDecisions::default(),
                            };
                            report.record_capture()?;
                            let (writers, _writer_metadata) = writers.into_parts();
                            let mut writers = writers.into_iter();
                            let staged = output_fanout::stage(
                                targets, overwrite,
                                |path| self.validate_writer_destination(path, cancellation),
                                |path, _| {
                                    let writer = writers.next().ok_or_else(|| failed("prepared output adapter is absent"))?;
                                    let mut invoked = false;
                                    let mut producer = |context: &NativeExecutionContext<'_>, batch_rows: usize, consume: &mut dyn FnMut(ArrayRef) -> Result<bool>| {
                                        if std::mem::replace(&mut invoked, true) {
                                            return Err(failed("output replay was invoked twice"));
                                        }
                                        // Native run merging can combine small producer
                                        // batches. Preserve their bound at the adapter so
                                        // replay does not amplify its transient reservation.
                                        let batch_rows = batch_rows.min(copied.max_batch_rows.max(1));
                                        replay.produce(&dtype, metrics, context, batch_rows, report.rows_detached, consume)
                                    };
                                    let output = writer.write(&request, path, false, self.policy, &mut producer, cancellation, Some(context))?;
                                    if !invoked || output.rows_written != report.rows_detached {
                                        return Err(failed("output adapter did not consume the complete replay"));
                                    }
                                    report.record_delivery()?;
                                    Ok(output)
                                },
                            )?;
                            // Readers and payload owners disappear before State::finish cleans
                            // the shared native runs and issues the execution certificate.
                            drop(replay);
                            Ok((staged, report))
                        },
                    )?;
                    Ok((execution.finish_file_evidence(context)?, completed))
                })
            },
        )?;
        execution.runtime = self.snapshot();
        replay.spill.clone_from(&execution.spill);
        replay.shared_spill_peak_disk_bytes =
            execution.spill.as_ref().map(|spill| spill.peak_disk_bytes);
        let reports = prepare_reports(&execution)?;
        let mut outputs =
            staged.publish(|path| self.validate_writer_destination(path, cancellation))?;
        for (output, (path, _)) in outputs.iter_mut().zip(targets) {
            output.output_path = path.display().to_string();
            output.rows_scanned = execution.scan_rows_delivered;
            output.arrays_read_count =
                usize::try_from(execution.scan_batches).map_err(vortex_error)?;
            execution.annotate_output(output);
        }
        sampling_call.complete();
        sampling.complete();
        Ok((
            WrittenVortexRelationalFanout {
                execution,
                outputs,
                replay: Some(replay),
            },
            reports,
        ))
    }
}
