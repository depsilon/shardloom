//! Single-use input shares one certified execution and its credited native replay.

use super::super::{DType, Nullability, ReservedVec, batch_input, replay};
use super::{
    ArrayRef, BATCH_ROWS, CancellationToken, ExecutedVortexRelational, NativeExecutionContext,
    PreparedVortexRelational, Result, VortexLocalPrimitiveRowExportFormat,
    VortexLocalPrimitiveRowExportReport, failed, vortex_error,
};
use crate::local_primitives::{completed_result, output_fanout};
use std::{cell::RefCell, path::PathBuf};

/// Native result retention used only when one execution feeds several writers.
/// Buffer bytes count copied native buffers, not capacity, decoded bytes or RSS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexRelationalOutputReplay {
    pub batches_detached: u64,
    pub rows_detached: u64,
    pub buffer_bytes_copied: u64,
    pub spilled: bool,
    pub replays: usize,
}

/// One completed execution and every independently certified output adapter.
pub struct WrittenVortexRelationalFanout {
    pub execution: ExecutedVortexRelational,
    pub outputs: Vec<VortexLocalPrimitiveRowExportReport>,
    /// A single destination streams directly and has no retained output replay.
    pub replay: Option<VortexRelationalOutputReplay>,
}

impl PreparedVortexRelational {
    pub(in super::super) fn write_many_with_input(
        &self,
        targets: &[(PathBuf, VortexLocalPrimitiveRowExportFormat)],
        overwrite: bool,
        cancellation: &CancellationToken,
        input: batch_input::Provider<'_>,
    ) -> Result<WrittenVortexRelationalFanout> {
        self.validate_batch_provider(true)?;
        cancellation.check()?;
        output_fanout::preflight_targets(targets, overwrite)?;
        for (path, _) in targets {
            self.validate_writer_destination(path, cancellation)?;
        }
        if let [(path, format)] = targets {
            let written =
                self.write_with_input(path, *format, overwrite, cancellation, Some(input))?;
            return Ok(WrittenVortexRelationalFanout {
                execution: written.execution,
                outputs: vec![written.output],
                replay: None,
            });
        }
        let (mut execution, (staged, replay)) = self.session.with_sources_execution(
            &self.sources, cancellation, |context| {
                self.with_bound_root(context, Some(input), |root, metrics| {
                    let (_declaration_metadata, request) = self.writer_request(root, context)?;
                    let dtype = DType::struct_(root.fields.clone(), Nullability::NonNullable);
                    let mut writers = ReservedVec::new(context.memory())?;
                    for (_, format) in targets {
                        writers.push(completed_result::prepare_stream(
                            self.writer_plan(root)?, &request, *format, self.policy, cancellation,
                        )?)?;
                    }
                    let schema = replay::Schema::new(&dtype, context)?;
                    let recorder = RefCell::new(Some(replay::Recorder::new(&schema, metrics, context)?));
                    let copied = RefCell::new(replay::CopyStats::default());
                    self.consume_bound_with_completion(
                        root, context, metrics, BATCH_ROWS,
                        &mut |array| {
                            recorder.try_borrow_mut().map_err(|_| failed("output replay is active"))?
                                .as_mut().ok_or_else(|| failed("output replay is already complete"))?
                                .build(&array, &mut copied.borrow_mut(), context)
                        },
                        || {
                            let replay = recorder.try_borrow_mut().map_err(|_| failed("output replay is active"))?
                                .take().ok_or_else(|| failed("output replay is absent"))?.finish(context)?;
                            let copied = copied.borrow();
                            let report = VortexRelationalOutputReplay {
                                batches_detached: copied.batches,
                                rows_detached: copied.rows,
                                buffer_bytes_copied: copied.buffer_bytes,
                                spilled: replay.is_spilled(), replays: targets.len(),
                            };
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
                                    Ok(output)
                                },
                            )?;
                            // Readers and payload owners disappear before State::finish cleans
                            // the shared native runs and issues the execution certificate.
                            drop(replay);
                            Ok((staged, report))
                        },
                    )
                })
            },
        )?;
        execution.runtime = self.snapshot();
        let mut outputs =
            staged.publish(|path| self.validate_writer_destination(path, cancellation))?;
        for (output, (path, _)) in outputs.iter_mut().zip(targets) {
            output.output_path = path.display().to_string();
            output.rows_scanned = execution.scan_rows_delivered;
            output.arrays_read_count =
                usize::try_from(execution.scan_batches).map_err(vortex_error)?;
        }
        Ok(WrittenVortexRelationalFanout {
            execution,
            outputs,
            replay: Some(replay),
        })
    }
}
