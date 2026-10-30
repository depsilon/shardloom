//! One explicit application boundary inside the existing flat native pipeline.

use super::{
    ArrayRef, DType, MemoryLease, Metrics, NativeExecutionContext, ReservedVec,
    ResidentVortexSession, Result, failed, vortex_error,
};
use crate::application_batch::{ApplicationBatchFunction, ApplicationBatchOutput};
use shardloom_exec::live_memory::LiveMemoryPool;

pub(super) struct Spec {
    function: ApplicationBatchFunction,
    _metadata: MemoryLease,
}

pub(super) struct Observation {
    function: ApplicationBatchFunction,
    completed_batches: u64,
    rows: u64,
    native_input_buffer_bytes_copied: u64,
    native_output_buffer_bytes: u64,
    arrow_import_buffer_bytes_copied: u64,
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    export: crate::resident_session::NativeArrowExportReport,
}

pub(super) fn bind(
    function: &ApplicationBatchFunction,
    input: &[(String, DType)],
    memory: &LiveMemoryPool,
) -> Result<(Vec<(String, DType)>, Spec)> {
    let declaration = function.declaration();
    let mut metadata = crate::native_payload_schema::reserve_schema(&declaration.input, memory)?;
    let mut output_metadata =
        crate::native_payload_schema::reserve_schema(&declaration.output, memory)?;
    metadata.absorb(&mut output_metadata)?;
    let DType::Struct(input_schema, _) = &declaration.input else {
        return Err(failed("application input must be a record schema"));
    };
    if input_schema.nfields() != input.len()
        || input_schema
            .names()
            .iter()
            .zip(input_schema.fields())
            .zip(input)
            .any(|((name, dtype), (actual_name, actual_dtype))| {
                name.as_ref() != actual_name || &dtype != actual_dtype
            })
    {
        return Err(failed(&format!(
            "application batch function '{}' input schema differs from its declaration",
            function.name(),
        )));
    }
    let DType::Struct(output_schema, _) = &declaration.output else {
        return Err(failed("application output must be a record schema"));
    };
    let fields = output_schema
        .names()
        .iter()
        .zip(output_schema.fields())
        .map(|(name, dtype)| (name.to_string(), dtype))
        .collect();
    Ok((
        fields,
        Spec {
            function: function.clone(),
            _metadata: metadata,
        },
    ))
}

impl Spec {
    pub(super) fn max_batch_rows(&self) -> usize {
        self.function.declaration().limits.max_batch_rows
    }

    pub(super) fn execute(
        &self,
        input: &ArrayRef,
        session: &ResidentVortexSession,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
    ) -> Result<ArrayRef> {
        context.check_cancelled()?;
        let declaration = self.function.declaration();
        if input.dtype() != &declaration.input || input.len() > declaration.limits.max_batch_rows {
            return Err(failed(
                "application batch input exceeds its bound schema or row limit",
            ));
        }
        if input.is_empty() {
            return super::super::native_payload::empty_record(&declaration.output, context);
        }
        // Credit is acquired before validation/export or caller invocation and
        // remains alive until the foreign result has been independently owned.
        let _workspace = context
            .memory()
            .reserve(declaration.limits.workspace_bytes)?;
        let observation = self.observation(metrics, context)?;
        // A source may transfer a private batch whose aliases must be gone
        // before its next demand. Application code may retain its input, so
        // give it independently owned, credited buffers instead of weakening
        // that producer contract. All copies use the current operation grant.
        let owned_input = super::super::native_payload::detach(input, context)?;
        let invocation = self.function.invoke(&owned_input, session, context)?;
        // A caller's lazy native array can execute application code during
        // validation/intake too. Keep the reentry guard and panic boundary
        // through that whole ownership transition.
        let (output, copied) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.intake(invocation.output, input.len(), session, context)
        }))
        .map_err(|_| {
            failed(&format!(
                "application batch function '{}' output intake unwound with a panic",
                self.function.name()
            ))
        })??;
        context.check_cancelled()?;
        let mut observations = metrics.application.borrow_mut();
        let row = &mut observations.as_mut().expect("admitted observation").values[observation];
        increment(&mut row.completed_batches, 1)?;
        increment(&mut row.rows, output.len() as u64)?;
        increment(
            &mut row.native_input_buffer_bytes_copied,
            owned_input.nbytes(),
        )?;
        increment(&mut row.native_output_buffer_bytes, output.nbytes())?;
        increment(&mut row.arrow_import_buffer_bytes_copied, copied)?;
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        row.export.append(invocation.export)?;
        Ok(output)
    }

    fn intake(
        &self,
        returned: ApplicationBatchOutput,
        rows: usize,
        session: &ResidentVortexSession,
        context: &NativeExecutionContext<'_>,
    ) -> Result<(ArrayRef, u64)> {
        #[cfg(not(all(feature = "vortex-write", feature = "universal-format-io")))]
        let _ = session;
        let (output, copied) = match returned {
            ApplicationBatchOutput::Native(array) => {
                self.validate_output(&array, rows)?;
                (super::super::native_payload::detach(&array, context)?, 0)
            }
            #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
            ApplicationBatchOutput::Arrow(batch) => {
                if batch.num_rows() != rows
                    || batch.get_array_memory_size() as u64
                        > self.function.declaration().limits.max_output_bytes
                {
                    return Err(failed(
                        "application columnar output exceeds its row or byte contract",
                    ));
                }
                let source =
                    crate::resident_memory_source::ResidentMemorySource::from_arrow_in_context(
                        session, batch, context, true,
                    )?;
                let array = source.batch_array(context)?.clone();
                self.validate_output(&array, rows)?;
                (array, source.intake_columnar_buffer_bytes_copied())
            }
        };
        // Encoded caller output can expand during independent native intake.
        // Its final visible payload must also satisfy the declared bound; all
        // actual intake allocations already used the shared native grant.
        self.validate_output(&output, rows)?;
        Ok((output, copied))
    }

    fn validate_output(&self, output: &ArrayRef, rows: usize) -> Result<()> {
        let declaration = self.function.declaration();
        if output.dtype() != &declaration.output || output.len() != rows {
            return Err(failed(&format!(
                "application batch function '{}' changed its declared output schema or row count",
                self.function.name(),
            )));
        }
        if output.nbytes() > declaration.limits.max_output_bytes {
            return Err(failed(
                "application output exceeds its declared visible-byte limit",
            ));
        }
        Ok(())
    }

    pub(super) fn observation(
        &self,
        metrics: &Metrics<'_>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<usize> {
        let mut observations = metrics.application.borrow_mut();
        if observations.is_none() {
            *observations = Some(ReservedVec::new(context.memory())?);
        }
        let observations = observations.as_mut().expect("initialized observations");
        if let Some(index) = observations
            .values
            .iter()
            .position(|row| row.function == self.function)
        {
            return Ok(index);
        }
        let index = observations.values.len();
        observations.push(Observation {
            function: self.function.clone(),
            completed_batches: 0,
            rows: 0,
            native_input_buffer_bytes_copied: 0,
            native_output_buffer_bytes: 0,
            arrow_import_buffer_bytes_copied: 0,
            #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
            export: crate::resident_session::NativeArrowExportReport::default(),
        })?;
        Ok(index)
    }
}

fn increment(value: &mut u64, amount: u64) -> Result<()> {
    *value = value
        .checked_add(amount)
        .ok_or_else(|| failed("application observation overflow"))?;
    Ok(())
}

pub(super) fn append_certificate(
    certificate: &mut shardloom_core::NativeIoCertificate,
    metrics: &Metrics<'_>,
) -> Result<()> {
    use std::fmt::Write as _;
    let observations = metrics.application.borrow();
    let Some(observations) = observations.as_ref() else {
        return Ok(());
    };
    for row in &observations.values {
        let declaration = row.function.declaration();
        certificate
            .source_pushdown_report
            .accepted_operations
            .push(format!(
                "application_batch:{}@{}",
                row.function.name(),
                row.function.version(),
            ));
        write!(certificate.source_pushdown_report.proof_basis,
            " application_batch_name={:?} application_batch_version={:?} application_completed_batches={} application_rows={} application_native_input_buffer_bytes_copied={} application_native_output_buffer_bytes={} application_arrow_import_buffer_bytes_copied={} application_declared_workspace_bytes={} application_effect=pure_deterministic application_null_behavior=null_aware application_row_contract=rowwise_preserving application_trust=caller_trusted_local application_output_intake=validated_independent_native",
            row.function.name(), row.function.version(), row.completed_batches, row.rows,
            row.native_input_buffer_bytes_copied, row.native_output_buffer_bytes, row.arrow_import_buffer_bytes_copied,
            declaration.limits.workspace_bytes,
        ).map_err(vortex_error)?;
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        write!(certificate.source_pushdown_report.proof_basis,
            " application_arrow_export_batches={} application_arrow_shared_native_buffer_bytes={} application_arrow_converted_buffer_capacity_bytes={}",
            row.export.output_batches, row.export.shared_native_buffer_bytes,
            row.export.converted_buffer_capacity_bytes,
        ).map_err(vortex_error)?;
        if row.completed_batches > 0 {
            certificate.side_effects.data_materialized = true;
            certificate.adapter_fidelity_report.materialization_required = true;
        }
    }
    certificate.source_pushdown_report.guarantee.push_str(
        ";application_callback_contracts_are_caller_trusted_not_sandboxed;callback_private_allocations_are_not_process_memory_accounting;application_rows_schema_and_output_ownership_validated_before_publication",
    );
    Ok(())
}
