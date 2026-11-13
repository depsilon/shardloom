//! Embedded query ownership over the shared native operator pipeline.
//! SQL, typed declarations and workflows retain the same execution object.

use super::super as workflow;
use arrow_array::RecordBatch;
use shardloom_core::{ExecutionResources, Result};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_vortex::{
    VortexLocalPrimitiveRowExportFormat as Format,
    local_primitives::prepared_relational::{
        ExecutedVortexRelational, PreparedVortexRelational, VortexRelationalBatchInput,
    },
    resident_session::{NativeArrowExportReport, OwnedVortexResultBatch},
};
use std::path::{Path, PathBuf};

#[path = "native_runtime_workflow.rs"]
mod declaration;
pub use declaration::NativeWorkflowDeclaration;

type ArrowInput<'a> = &'a mut dyn FnMut() -> Result<Option<RecordBatch>>;

/// Borrowed input demand over the existing admitted Arrow boundary. Source-aware
/// callers keep independent producer order; the prepared plan owns native replay.
pub enum NativeArrowInput<'a> {
    /// Compatibility adapter for a plan declaring exactly one streaming source.
    Single(ArrowInput<'a>),
    /// Demand identifies a declared source and its next zero-based batch ordinal.
    Sources(&'a mut dyn FnMut(&shardloom_core::DatasetUri, u64) -> Result<Option<RecordBatch>>),
}

impl<'i> NativeArrowInput<'i> {
    fn bind<'p>(
        self,
        operation: &'p PreparedVortexRelational,
    ) -> Result<VortexRelationalBatchInput<'p, 'i>> {
        match self {
            Self::Single(input) => operation.with_arrow_batch_input(input),
            Self::Sources(input) => operation.with_arrow_batch_inputs(input),
        }
    }
}

/// One native operator plan with its original resource owner.
pub struct PreparedNativeOperation {
    resources: ExecutionResources,
    operation: Box<PreparedVortexRelational>,
    reusable: bool,
}

/// The complete pipeline report keeps its metadata owners alive with the result.
pub struct ExecutedNativeOperation {
    pub resources: ExecutionResources,
    report: Box<ExecutedVortexRelational>,
    timing: Option<shardloom_vortex::local_primitives::query_timing::QueryTiming>,
}

/// Complete native values and evidence. No data is rendered into the report.
pub struct ExecutedOwnedNativeOperation {
    pub execution: ExecutedNativeOperation,
    pub result: OwnedVortexResultBatch,
}

/// Completed native execution and its explicit columnar interchange observations.
pub struct ExecutedArrowNativeOperation {
    pub execution: ExecutedNativeOperation,
    pub export: NativeArrowExportReport,
}

impl PreparedNativeOperation {
    /// Collection membership is resolved for each complete call, just as in the
    /// CLI. Individual prepared files still validate their original generations.
    #[must_use]
    pub fn permits_reuse(&self) -> bool {
        self.reusable
    }

    #[must_use]
    pub fn relational(operation: PreparedVortexRelational, resources: ExecutionResources) -> Self {
        Self {
            resources,
            operation: Box::new(operation),
            reusable: true,
        }
    }

    pub(super) fn with_reuse(mut self, reusable: bool) -> Self {
        self.reusable &= reusable;
        self
    }

    /// Execute a complete result under the already admitted allocation.
    /// # Errors
    /// Propagates provider, source, pressure, cancellation and input failures.
    pub fn execute_native(
        &self,
        cancellation: &CancellationToken,
        input: Option<ArrowInput<'_>>,
    ) -> Result<ExecutedOwnedNativeOperation> {
        self.execute_native_with_inputs(cancellation, input.map(NativeArrowInput::Single))
    }

    /// Execute with independent demand for one or more declared input sources.
    /// # Errors
    /// Propagates provider, source, pressure, cancellation and input failures.
    pub fn execute_native_with_inputs(
        &self,
        cancellation: &CancellationToken,
        input: Option<NativeArrowInput<'_>>,
    ) -> Result<ExecutedOwnedNativeOperation> {
        let started = std::time::Instant::now();
        let operation = &self.operation;
        let completed = if let Some(input) = input {
            input.bind(operation)?.execute_native(cancellation)?
        } else {
            operation.execute_native(cancellation)?
        };
        let mut completed = ExecutedOwnedNativeOperation {
            execution: self.relational_execution(completed.execution),
            result: completed.result,
        };
        completed.execution.timing = Some(
            shardloom_vortex::local_primitives::query_timing::QueryTiming {
                execution: started.elapsed(),
                result_serialization: std::time::Duration::ZERO,
            },
        );
        Ok(completed)
    }

    /// Deliver batches directly from the native producer. Consumer backpressure
    /// applies before the producer advances; completion follows final validation.
    /// # Errors
    /// Propagates native, conversion, consumer, input and cancellation failures.
    pub fn for_each_arrow_batch(
        &self,
        cancellation: &CancellationToken,
        input: Option<ArrowInput<'_>>,
        consume: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<ExecutedArrowNativeOperation> {
        self.for_each_arrow_batch_with_inputs(
            cancellation,
            input.map(NativeArrowInput::Single),
            consume,
        )
    }

    /// Deliver native batches with independent source demand and backpressure.
    /// # Errors
    /// Propagates native, conversion, consumer, input and cancellation failures.
    pub fn for_each_arrow_batch_with_inputs(
        &self,
        cancellation: &CancellationToken,
        input: Option<NativeArrowInput<'_>>,
        consume: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<ExecutedArrowNativeOperation> {
        let operation = &self.operation;
        let completed = if let Some(input) = input {
            input
                .bind(operation)?
                .for_each_arrow_batch(cancellation, consume)?
        } else {
            operation.for_each_arrow_batch(cancellation, consume)?
        };
        Ok(ExecutedArrowNativeOperation {
            execution: self.relational_execution(completed.execution),
            export: completed.export,
        })
    }

    /// Select the finite output-window policy through the actual native plan.
    /// # Errors
    /// Propagates native policy, source, resource, input and consumer failures.
    pub fn for_each_arrow_batch_with_output_window(
        &self,
        cancellation: &CancellationToken,
        input: Option<NativeArrowInput<'_>>,
        requested: impl FnOnce() -> bool,
        initialize: impl FnOnce(
            Option<shardloom_vortex::local_primitives::prepared_relational::OutputCreditWindow>,
        ) -> Result<()>,
        consume: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<ExecutedArrowNativeOperation> {
        let operation = &self.operation;
        let completed = if let Some(input) = input {
            input
                .bind(operation)?
                .for_each_arrow_batch_with_output_window(
                    cancellation,
                    requested,
                    initialize,
                    consume,
                )?
        } else {
            operation.for_each_arrow_batch_with_output_window(
                cancellation,
                requested,
                initialize,
                consume,
            )?
        };
        Ok(ExecutedArrowNativeOperation {
            execution: self.relational_execution(completed.execution),
            export: completed.export,
        })
    }

    /// Execute and publish through the provider's existing native writer.
    /// # Errors
    /// Propagates validation, provider and publication errors without replay.
    #[allow(clippy::too_many_arguments)] // One complete output request and its original input owner.
    pub fn write_fields(
        &self,
        path: &Path,
        format: Format,
        overwrite: bool,
        cancellation: &CancellationToken,
        input: Option<ArrowInput<'_>>,
        reused: bool,
    ) -> Result<Vec<(String, String)>> {
        self.write_fields_with_inputs(
            path,
            format,
            overwrite,
            cancellation,
            input.map(NativeArrowInput::Single),
            reused,
        )
    }

    /// Execute one writer with independent demand for declared input sources.
    /// # Errors
    /// Propagates validation, provider and publication errors without replaying input.
    #[allow(clippy::too_many_arguments)] // One output request and its original input owners.
    pub fn write_fields_with_inputs(
        &self,
        path: &Path,
        format: Format,
        overwrite: bool,
        cancellation: &CancellationToken,
        input: Option<NativeArrowInput<'_>>,
        reused: bool,
    ) -> Result<Vec<(String, String)>> {
        let operation = &self.operation;
        let targets = [workflow::NativeVortexPrimitiveRowExportTarget {
            role: "primary",
            path: path.to_path_buf(),
            format,
        }];
        let written = if let Some(input) = input {
            input
                .bind(operation)?
                .write_controlled(path, format, overwrite, cancellation)?
        } else {
            operation.write_controlled(path, format, overwrite, cancellation)?
        };
        let mut fields = super::written_fields(&written, reused);
        workflow::append_native_vortex_primitive_row_export_target_fields(
            &mut fields,
            &targets,
            std::slice::from_ref(&written.output),
        );
        Ok(fields)
    }

    /// Stage and publish every target using the shared native fanout contract.
    /// # Errors
    /// Rejects invalid targets and propagates provider or publication failures.
    pub fn write_many_fields(
        &self,
        targets: &[(PathBuf, Format)],
        overwrite: bool,
        cancellation: &CancellationToken,
        reused: bool,
    ) -> Result<Vec<(String, String)>> {
        self.write_many_fields_with_input(targets, overwrite, cancellation, None, reused)
    }

    /// Single-use input executes once; each writer consumes its credited native result.
    /// # Errors
    /// Propagates source, replay, resource, cancellation and publication failures.
    pub fn write_many_fields_with_input(
        &self,
        targets: &[(PathBuf, Format)],
        overwrite: bool,
        cancellation: &CancellationToken,
        input: Option<ArrowInput<'_>>,
        reused: bool,
    ) -> Result<Vec<(String, String)>> {
        self.write_many_fields_with_inputs(
            targets,
            overwrite,
            cancellation,
            input.map(NativeArrowInput::Single),
            reused,
        )
    }

    /// Execute each declared producer once and replay its native result to all writers.
    /// # Errors
    /// Propagates source, replay, resource, cancellation and publication failures.
    pub fn write_many_fields_with_inputs(
        &self,
        targets: &[(PathBuf, Format)],
        overwrite: bool,
        cancellation: &CancellationToken,
        input: Option<NativeArrowInput<'_>>,
        reused: bool,
    ) -> Result<Vec<(String, String)>> {
        let operation = &self.operation;
        if let Some(input) = input {
            super::written_stream_fanout_fields(
                targets,
                input
                    .bind(operation)?
                    .write_many_controlled(targets, overwrite, cancellation)?,
                reused,
            )
        } else {
            super::written_stream_fanout_fields(
                targets,
                operation.write_many_shared_controlled(targets, overwrite, cancellation)?,
                reused,
            )
        }
    }

    fn relational_execution(&self, report: ExecutedVortexRelational) -> ExecutedNativeOperation {
        ExecutedNativeOperation {
            resources: self.resources,
            timing: None,
            report: Box::new(report),
        }
    }
}

impl ExecutedNativeOperation {
    /// Reuse public report serializers. Values remain in their native owner.
    /// # Errors
    /// Propagates report serialization failures.
    pub fn fields(&self, reused: bool) -> Result<Vec<(String, String)>> {
        let mut fields = super::execution_fields(&self.report, reused);
        // Collected values and completed streams retain their native array
        // owners. Keep the established payload contract across consolidation.
        fields.extend([
            ("result_payload_complete".into(), "true".into()),
            (
                "native_result_payload".into(),
                "retained_native_arrays".into(),
            ),
        ]);
        crate::execution_resources::append_query_timing_fields(&mut fields, self.timing);
        Ok(fields)
    }
}
