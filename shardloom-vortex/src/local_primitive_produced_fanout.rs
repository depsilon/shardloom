//! Native primitive producers share the relational result replay and sink owners.
//! This adapter changes delivery only; it never rebinds or reruns a query.

use super::{
    ArrayRef, CancellationToken, DType, Metrics, NativeExecutionContext, Result, failed, replay,
};
use crate::{
    local_primitives::{
        VortexLocalPrimitiveExecutionPolicy, VortexLocalPrimitiveRowExportFormat as Format,
        VortexLocalPrimitiveRowExportReport, VortexQueryPrimitiveRequest, completed_result,
        native_sink::NativeSinkPlan, output_fanout,
    },
    resident_session::{PreparedVortexSource, ResidentVortexSession},
};
use std::{cell::OnceCell, path::PathBuf};

/// The original producer report and each terminal adapter's completed output.
pub struct WrittenNativeProducedFanout<T> {
    pub execution: T,
    pub outputs: Vec<VortexLocalPrimitiveRowExportReport>,
    pub replay: super::VortexRelationalOutputReplay,
}

/// Consume a native producer exactly once and replay its completed result to
/// several writers. Capture and delivery use the producer's existing memory
/// owner; this does not silently enable disk spill or cache results across calls.
/// Large results require sufficient memory or the ordinary relational fanout's
/// explicitly admitted native spill policy.
/// # Errors
/// Rejects unsafe destinations, changed sources, mismatched owners/schemas,
/// resource denial, cancellation, incomplete producers and adapter failures.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub fn write_produced_many_controlled<T>(
    session: &ResidentVortexSession,
    policy: VortexLocalPrimitiveExecutionPolicy,
    request: &VortexQueryPrimitiveRequest,
    sources: &[PreparedVortexSource],
    targets: &[(PathBuf, Format)],
    overwrite: bool,
    cancellation: &CancellationToken,
    produce: impl FnOnce(
        &mut dyn FnMut(ArrayRef, &NativeExecutionContext<'_>) -> Result<()>,
    ) -> Result<T>,
) -> Result<WrittenNativeProducedFanout<T>> {
    super::validate_session_policy(policy, session)?;
    let validate = |path: &std::path::Path| {
        cancellation.check()?;
        for source in sources {
            source.validate_generation()?;
            if source.aliases_file(path)? {
                return Err(failed("source and output must be different files"));
            }
        }
        cancellation.check()
    };
    output_fanout::preflight_targets(targets, overwrite)?;
    for (path, _) in targets {
        validate(path)?;
    }
    let dtype = OnceCell::<DType>::new();
    let schema = OnceCell::<replay::Schema>::new();
    let metrics = Metrics::default();
    let mut recorder = None;
    let mut copied = replay::CopyStats::default();
    let execution = produce(&mut |array, context| {
        session.validate_execution_context(context)?;
        cancellation.check()?;
        if let Some(dtype) = dtype.get() {
            if array.dtype() != dtype {
                return Err(failed("native output producer changed its schema"));
            }
        } else {
            schema
                .set(replay::Schema::new(array.dtype(), context)?)
                .map_err(|_| failed("native output schema was bound twice"))?;
            dtype
                .set(array.dtype().clone())
                .map_err(|_| failed("native output dtype was bound twice"))?;
        }
        if recorder.is_none() {
            recorder = Some(replay::Recorder::with_batch_rows(
                schema
                    .get()
                    .ok_or_else(|| failed("native output schema is absent"))?,
                &metrics,
                context,
                256,
            )?);
        }
        recorder
            .as_mut()
            .ok_or_else(|| failed("native output recorder is absent"))?
            .build(&array, &mut copied, context)
    })?;
    let dtype = dtype
        .get()
        .ok_or_else(|| failed("native producer did not deliver its schema"))?;
    let (staged, replay_report) =
        session.with_native_execution_context(cancellation, |context| {
            session.with_admitted_sources_execution(sources, context, |context| {
                let replay = recorder
                    .take()
                    .ok_or_else(|| failed("native producer did not complete"))?
                    .finish(context)?;
                let request = VortexQueryPrimitiveRequest::project(
                    request
                        .source_uri
                        .clone()
                        .ok_or_else(|| failed("native producer source is absent"))?,
                    shardloom_plan::ProjectionRequest::All,
                );
                let mut writers = super::ReservedVec::new(context.memory())?;
                for (_, format) in targets {
                    let plan = NativeSinkPlan::produced_sources(
                        session.clone(),
                        dtype.clone(),
                        copied.rows,
                        None,
                        sources.to_vec(),
                    )?;
                    writers.push(completed_result::prepare_stream(
                        plan,
                        &request,
                        *format,
                        policy,
                        cancellation,
                    )?)?;
                }
                let report = super::VortexRelationalOutputReplay {
                    batches_detached: copied.batches,
                    rows_detached: copied.rows,
                    buffer_bytes_copied: copied.buffer_bytes,
                    spilled: replay.is_spilled(),
                    replays: targets.len(),
                };
                let (writers, _metadata) = writers.into_parts();
                let mut writers = writers.into_iter();
                let staged = output_fanout::stage(targets, overwrite, validate, |path, _| {
                    let writer = writers
                        .next()
                        .ok_or_else(|| failed("native output adapter is absent"))?;
                    let mut invoked = false;
                    let mut producer = |context: &NativeExecutionContext<'_>, batch_rows: usize,
                    consume: &mut dyn FnMut(ArrayRef) -> Result<bool>| {
                    if std::mem::replace(&mut invoked, true) {
                        return Err(failed("native output replay was invoked twice"));
                    }
                    replay.produce(dtype, &metrics, context,
                        batch_rows.min(copied.max_batch_rows.max(1)), copied.rows, consume)
                };
                    let output = writer.write(
                        &request,
                        path,
                        false,
                        policy,
                        &mut producer,
                        cancellation,
                        Some(context),
                    )?;
                    if !invoked || output.rows_written != copied.rows {
                        return Err(failed(
                            "native output adapter did not consume its complete result",
                        ));
                    }
                    Ok(output)
                })?;
                drop(replay);
                Ok((staged, report))
            })
        })?;
    let mut outputs = staged.publish(validate)?;
    for (output, (path, _)) in outputs.iter_mut().zip(targets) {
        output.output_path = path.display().to_string();
    }
    Ok(WrittenNativeProducedFanout {
        execution,
        outputs,
        replay: replay_report,
    })
}
