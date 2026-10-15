//! Complete embedded operations over the same native prepared providers as CLI.
//! This is dispatch and ownership only; it defines no expression or query semantics.

use super::super as workflow;
use arrow_array::RecordBatch;
use shardloom_core::{ExecutionResources, Result, ShardLoomError};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::MemoryLease};
use shardloom_vortex::{
    VortexLocalPrimitiveExecutionPolicy, VortexLocalPrimitiveRowExportFormat as Format,
    VortexLocalPrimitiveRowExportReport as OutputReport, VortexQueryPrimitiveRequest,
    local_primitives::{
        partitioned_native::{ExecutedVortexPartitioned, PartitionedNativeOperation},
        prepared_aggregate::{ExecutedVortexAggregate, PreparedVortexAggregate},
        prepared_count::{ExecutedVortexCountWhere, PreparedVortexCountWhere},
        prepared_dispatch::{self, ExecutedVortexSort},
        prepared_relational::{
            ExecutedVortexRelational, PreparedVortexRelational, VortexRelationalBatchInput,
            WrittenVortexRelational,
        },
        prepared_unary::{ExecutedVortexUnary, PreparedVortexUnary},
    },
    resident_session::{
        NativeArrowBatchDelivery, NativeArrowExportReport, OwnedVortexResultBatch,
        PreparedVortexCount, PreparedVortexSource, ResidentSessionSnapshot, ResidentVortexSession,
    },
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

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

/// One prepared operation, retaining providers and the original resource owner.
pub struct PreparedNativeOperation {
    resources: ExecutionResources,
    kind: PreparedKind,
    reusable: bool,
}

enum PreparedKind {
    Relational(Box<PreparedVortexRelational>),
    Primitive(Box<PreparedPrimitive>),
}

struct PreparedPrimitive {
    shared: Arc<PrimitiveMetadata>,
    session: ResidentVortexSession,
    provider: PrimitiveProvider,
}

struct PrimitiveMetadata {
    request: VortexQueryPrimitiveRequest,
    argument: String,
    binding: workflow::NativeVortexInputBinding,
    sources: Vec<PreparedVortexSource>,
    source_uris: Vec<shardloom_core::DatasetUri>,
    preparation_fields: Vec<(String, String)>,
    policy: VortexLocalPrimitiveExecutionPolicy,
    _metadata: MemoryLease,
}

enum PrimitiveProvider {
    Count(Vec<PreparedVortexCount>),
    CountWhere(Box<PreparedVortexCountWhere>),
    Unary(Box<PreparedVortexUnary>),
    Aggregate(Box<PreparedVortexAggregate>),
    Sort,
    Partitioned,
    Relational(Box<PreparedVortexRelational>),
}

/// The actual provider report keeps its metadata owners alive with the result.
pub struct ExecutedNativeOperation {
    pub resources: ExecutionResources,
    kind: ExecutedKind,
    timing: Option<shardloom_vortex::local_primitives::query_timing::QueryTiming>,
}

enum ExecutedKind {
    Relational(Box<ExecutedVortexRelational>),
    Primitive {
        report: PrimitiveExecution,
        shared: Arc<PrimitiveMetadata>,
    },
}

enum PrimitiveExecution {
    Count {
        count: u64,
        runtime: ResidentSessionSnapshot,
    },
    CountWhere(Box<ExecutedVortexCountWhere>),
    Unary(Box<ExecutedVortexUnary>),
    Aggregate(Box<ExecutedVortexAggregate>),
    Sort(Box<ExecutedVortexSort>),
    Partitioned(Box<ExecutedVortexPartitioned>),
    Relational(Box<ExecutedVortexRelational>),
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
            kind: PreparedKind::Relational(Box::new(operation)),
            reusable: true,
        }
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
        let mut completed = match &self.kind {
            PreparedKind::Relational(operation) => {
                let completed = if let Some(input) = input {
                    input.bind(operation)?.execute_native(cancellation)?
                } else {
                    operation.execute_native(cancellation)?
                };
                ExecutedOwnedNativeOperation {
                    execution: self.relational_execution(completed.execution),
                    result: completed.result,
                }
            }
            PreparedKind::Primitive(operation) => {
                no_streaming_input(input.is_some())?;
                let (report, result) = operation.execute_native(cancellation)?;
                ExecutedOwnedNativeOperation {
                    execution: self.primitive_execution(operation, report),
                    result,
                }
            }
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
        match &self.kind {
            PreparedKind::Relational(operation) => {
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
            PreparedKind::Primitive(operation) => {
                no_streaming_input(input.is_some())?;
                let (report, export) = operation.for_each_arrow_batch(cancellation, consume)?;
                Ok(ExecutedArrowNativeOperation {
                    execution: self.primitive_execution(operation, report),
                    export,
                })
            }
        }
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
        match &self.kind {
            PreparedKind::Relational(operation) => {
                let written = if let Some(input) = input {
                    input.bind(operation)?.write_controlled(
                        path,
                        format,
                        overwrite,
                        cancellation,
                    )?
                } else {
                    operation.write_controlled(path, format, overwrite, cancellation)?
                };
                Ok(super::written_fields(&written, reused))
            }
            PreparedKind::Primitive(operation) => {
                no_streaming_input(input.is_some())?;
                let report = operation.write(path, format, overwrite, cancellation)?;
                Ok(operation.written_fields(&report, self.resources, reused))
            }
        }
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
        match &self.kind {
            PreparedKind::Relational(operation) => {
                if let Some(input) = input {
                    super::written_stream_fanout_fields(
                        targets,
                        input.bind(operation)?.write_many_controlled(
                            targets,
                            overwrite,
                            cancellation,
                        )?,
                        reused,
                    )
                } else {
                    super::written_many_fields(
                        targets,
                        operation.write_many_controlled(targets, overwrite, cancellation)?,
                        reused,
                    )
                }
            }
            PreparedKind::Primitive(operation) => {
                no_streaming_input(input.is_some())?;
                let mut reports = shardloom_vortex::local_primitives::output_fanout::write(
                    targets,
                    overwrite,
                    |path| operation.validate_output(path, cancellation),
                    |path, format| operation.write(path, format, false, cancellation),
                )?;
                for (report, (path, _)) in reports.iter_mut().zip(targets) {
                    report.output_path = path.display().to_string();
                }
                let primary = reports
                    .first()
                    .ok_or_else(|| failed("fanout produced no output"))?;
                let mut fields = operation.written_fields(primary, self.resources, reused);
                let targets = targets
                    .iter()
                    .enumerate()
                    .map(
                        |(index, (path, format))| workflow::NativeVortexPrimitiveRowExportTarget {
                            role: if index == 0 { "primary" } else { "fanout" },
                            path: path.clone(),
                            format: *format,
                        },
                    )
                    .collect::<Vec<_>>();
                workflow::append_native_vortex_primitive_row_export_target_fields(
                    &mut fields,
                    &targets,
                    &reports,
                );
                Ok(fields)
            }
        }
    }

    fn relational_execution(&self, report: ExecutedVortexRelational) -> ExecutedNativeOperation {
        ExecutedNativeOperation {
            resources: self.resources,
            timing: None,
            kind: ExecutedKind::Relational(Box::new(report)),
        }
    }

    fn primitive_execution(
        &self,
        operation: &PreparedPrimitive,
        report: PrimitiveExecution,
    ) -> ExecutedNativeOperation {
        ExecutedNativeOperation {
            resources: self.resources,
            timing: None,
            kind: ExecutedKind::Primitive {
                report,
                shared: Arc::clone(&operation.shared),
            },
        }
    }
}

impl PreparedPrimitive {
    fn execute_native(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(PrimitiveExecution, OwnedVortexResultBatch)> {
        cancellation.check()?;
        Ok(match &self.provider {
            PrimitiveProvider::Count(counts) => {
                let mut count = 0_u64;
                for operation in counts {
                    count = count
                        .checked_add(operation.execute_with_cancellation(cancellation)?)
                        .ok_or_else(|| failed("combined native footer count overflow"))?;
                }
                let result = self.session.own_count_result(count, cancellation)?;
                self.validate_sources(cancellation)?;
                (
                    PrimitiveExecution::Count {
                        count,
                        runtime: self.session.snapshot(),
                    },
                    result,
                )
            }
            PrimitiveProvider::CountWhere(operation) => {
                let completed = operation.execute_native_result(cancellation)?;
                (
                    PrimitiveExecution::CountWhere(Box::new(completed.execution)),
                    completed.result,
                )
            }
            PrimitiveProvider::Unary(operation) => {
                let completed = operation.execute_native_result(cancellation)?;
                (
                    PrimitiveExecution::Unary(Box::new(completed.execution)),
                    completed.result,
                )
            }
            PrimitiveProvider::Aggregate(operation) => {
                let completed = operation.execute_native_result(cancellation)?;
                (
                    PrimitiveExecution::Aggregate(Box::new(completed.execution)),
                    completed.result,
                )
            }
            PrimitiveProvider::Sort => {
                let completed = prepared_dispatch::execute_sort_native_result(
                    &self.shared.request,
                    self.shared.policy,
                    self.source()?,
                    cancellation,
                )?;
                (
                    PrimitiveExecution::Sort(Box::new(completed.execution)),
                    completed.result,
                )
            }
            PrimitiveProvider::Partitioned => {
                let completed = self
                    .partitioned(cancellation)?
                    .execute_native_result(cancellation)?;
                (
                    PrimitiveExecution::Partitioned(Box::new(completed.execution)),
                    completed.result,
                )
            }
            PrimitiveProvider::Relational(operation) => {
                let completed = operation.execute_native(cancellation)?;
                (
                    PrimitiveExecution::Relational(Box::new(completed.execution)),
                    completed.result,
                )
            }
        })
    }

    fn for_each_arrow_batch(
        &self,
        cancellation: &CancellationToken,
        mut consume: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<(PrimitiveExecution, NativeArrowExportReport)> {
        if matches!(
            self.provider,
            PrimitiveProvider::Count(_) | PrimitiveProvider::CountWhere(_)
        ) {
            // A count owns exactly one scalar batch; this never collects a table
            // merely to simulate incremental delivery.
            let (execution, result) = self.execute_native(cancellation)?;
            let mut stream = Arc::new(result).arrow_batches(cancellation.clone())?;
            while let Some(batch) = stream.next_batch()? {
                consume(batch)?;
            }
            self.validate_sources(cancellation)?;
            return Ok((execution, stream.report()));
        }
        if let PrimitiveProvider::Relational(operation) = &self.provider {
            let completed = operation.for_each_arrow_batch(cancellation, consume)?;
            return Ok((
                PrimitiveExecution::Relational(Box::new(completed.execution)),
                completed.export,
            ));
        }
        let mut delivery = NativeArrowBatchDelivery::new(&self.session, cancellation, consume);
        let (report, rows) = match &self.provider {
            PrimitiveProvider::Unary(operation) => {
                let report = operation
                    .for_each_batch(cancellation, |array, context| delivery.push(array, context))?;
                let rows = output_rows(&report.report)?;
                (PrimitiveExecution::Unary(Box::new(report)), rows)
            }
            PrimitiveProvider::Aggregate(operation) => {
                let report = operation
                    .for_each_batch(cancellation, |array, context| delivery.push(array, context))?;
                let rows = output_rows(&report.report)?;
                (PrimitiveExecution::Aggregate(Box::new(report)), rows)
            }
            PrimitiveProvider::Sort => {
                let report = prepared_dispatch::for_each_sort_batch(
                    &self.shared.request,
                    self.shared.policy,
                    self.source()?,
                    cancellation,
                    |array, context| delivery.push(array, context),
                )?;
                let rows = output_rows(&report.report)?;
                (PrimitiveExecution::Sort(Box::new(report)), rows)
            }
            PrimitiveProvider::Partitioned => {
                let report = self
                    .partitioned(cancellation)?
                    .for_each_batch(cancellation, |array, context| delivery.push(array, context))?;
                let rows = output_rows(&report.report)?;
                (PrimitiveExecution::Partitioned(Box::new(report)), rows)
            }
            _ => return Err(failed("native batch provider was not selected")),
        };
        Ok((report, delivery.finish(rows)?))
    }

    fn source(&self) -> Result<&PreparedVortexSource> {
        self.shared
            .sources
            .first()
            .ok_or_else(|| failed("retained source is absent"))
    }

    fn partitioned(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<PartitionedNativeOperation<'_>> {
        PartitionedNativeOperation::new(
            &self.shared.request,
            self.shared.policy,
            &self.shared.source_uris,
            &self.shared.sources,
            cancellation,
        )
    }

    fn validate_sources(&self, cancellation: &CancellationToken) -> Result<()> {
        for source in &self.shared.sources {
            cancellation.check()?;
            source.validate_generation()?;
        }
        cancellation.check()
    }

    fn validate_output(&self, path: &Path, cancellation: &CancellationToken) -> Result<()> {
        self.validate_sources(cancellation)?;
        for source in &self.shared.sources {
            if source.aliases_file(path)? {
                return Err(failed("source and output must be different files"));
            }
        }
        Ok(())
    }

    fn write(
        &self,
        path: &Path,
        format: Format,
        overwrite: bool,
        cancellation: &CancellationToken,
    ) -> Result<OutputReport> {
        self.validate_output(path, cancellation)?;
        match &self.provider {
            PrimitiveProvider::Relational(operation) => operation
                .write_controlled(path, format, overwrite, cancellation)
                .map(WrittenVortexRelational::into_output_report),
            PrimitiveProvider::Unary(operation) => {
                operation.write_controlled(path, format, overwrite, cancellation)
            }
            PrimitiveProvider::Aggregate(operation) => {
                operation.write_controlled(path, format, overwrite, cancellation)
            }
            PrimitiveProvider::Sort => prepared_dispatch::try_write_source_controlled(
                &self.shared.request,
                path,
                format,
                overwrite,
                self.shared.policy,
                self.source()?.clone(),
                cancellation,
            )?
            .ok_or_else(|| failed("native sort writer was not admitted")),
            PrimitiveProvider::Partitioned => self.partitioned(cancellation)?.write_controlled(
                path,
                format,
                overwrite,
                cancellation,
            ),
            PrimitiveProvider::Count(_) | PrimitiveProvider::CountWhere(_) => {
                self.write_count(path, format, overwrite, cancellation)
            }
        }
    }

    fn write_count(
        &self,
        path: &Path,
        format: Format,
        overwrite: bool,
        cancellation: &CancellationToken,
    ) -> Result<OutputReport> {
        use shardloom_vortex::{
            relational_query::{VortexRelationalPlan, VortexRelationalScan},
            resident_memory_source::ResidentMemorySource,
        };
        // Preflight precedes the scalar operation, including existing targets.
        shardloom_vortex::local_primitives::output_fanout::preflight_targets(
            &[(path.to_path_buf(), format)],
            overwrite,
        )?;
        let (_, result) = self.execute_native(cancellation)?;
        let result = Arc::new(result);
        let source = ResidentMemorySource::from_completed_result(&result, cancellation)?;
        let uri = shardloom_core::DatasetUri::new("memory://native-count-result")?;
        let operation = shardloom_vortex::local_primitives::prepared_relational::prepare_relational_with_schema_in_session_controlled(
            self.shared.policy, &self.session,
            |schemas| {
                schemas.register_memory_source(uri.clone(), |_| Ok(source))?;
                Ok(VortexRelationalPlan::Scan(VortexRelationalScan { source_uri: uri, projection: shardloom_plan::ProjectionRequest::All, predicate: None }))
            }, cancellation,
        )?;
        // A scalar result no longer carries file readers into the memory scan.
        // Keep the original generations in the shared publication validator so
        // a source change during conversion cannot publish a stale count.
        let mut reports = shardloom_vortex::local_primitives::output_fanout::write(
            &[(path.to_path_buf(), format)],
            overwrite,
            |target| self.validate_output(target, cancellation),
            |staged, format| {
                operation
                    .write_controlled(staged, format, false, cancellation)
                    .map(WrittenVortexRelational::into_output_report)
            },
        )?;
        let mut report = reports
            .pop()
            .ok_or_else(|| failed("count writer produced no report"))?;
        report.output_path = path.display().to_string();
        Ok(report)
    }

    fn written_fields(
        &self,
        report: &OutputReport,
        resources: ExecutionResources,
        reused: bool,
    ) -> Vec<(String, String)> {
        let mut fields = self.shared.binding.evidence_fields();
        fields.extend(self.shared.preparation_fields.clone());
        workflow::append_native_vortex_primitive_row_export_fields(&mut fields, report);
        crate::execution_resources::append_declaration_fields(&mut fields, resources);
        crate::execution_resources::append_admission_fields(
            &mut fields,
            resources.memory_bytes(),
            resources.max_parallelism(),
            "shared_native_operation_owner",
        );
        crate::execution_resources::append_resident_snapshot_fields(
            &mut fields,
            &self.session.snapshot(),
        );
        fields.push((
            "resident_native_preparation_reused".into(),
            reused.to_string(),
        ));
        fields
    }
}

impl ExecutedNativeOperation {
    /// Reuse public report serializers. Values remain in their native owner.
    /// # Errors
    /// Rejects incomplete native footer-count evidence.
    #[allow(clippy::too_many_lines)] // Keep each native provider's report and runtime owner together.
    pub fn fields(&self, reused: bool) -> Result<Vec<(String, String)>> {
        let ExecutedKind::Primitive { report, shared } = &self.kind else {
            let ExecutedKind::Relational(report) = &self.kind else {
                unreachable!()
            };
            let mut fields = super::execution_fields(report, reused);
            crate::execution_resources::append_query_timing_fields(&mut fields, self.timing);
            return Ok(fields);
        };
        let mut fields = shared.binding.evidence_fields();
        fields.extend(shared.preparation_fields.clone());
        crate::execution_resources::append_declaration_fields(&mut fields, self.resources);
        crate::execution_resources::append_admission_fields(
            &mut fields,
            self.resources.memory_bytes(),
            self.resources.max_parallelism(),
            "shared_native_operation_owner",
        );
        let runtime = match report {
            PrimitiveExecution::Count { count, runtime } => {
                let certificate = workflow::resident_count::count_certificate(
                    &shared.binding,
                    *count,
                    *runtime,
                    !reused,
                )?;
                fields.extend(workflow::resident_count::count_fields(
                    *count, *runtime, !reused,
                ));
                workflow::resident_count::append_effect_fields(&mut fields);
                workflow::vortex_primitive_execution::append_vortex_footer_count_report_fields(
                    &mut fields,
                );
                workflow::vortex_primitive_execution::append_vortex_local_primitive_native_io_certificate_fields(&mut fields, Some(&certificate));
                workflow::vortex_primitive_execution::append_vortex_local_primitive_execution_certificate_fields(&mut fields, None);
                crate::execution_resources::append_spill_observation_fields(
                    &mut fields,
                    false,
                    Some(0),
                );
                runtime
            }
            PrimitiveExecution::CountWhere(report) => {
                primitive_fields(
                    &mut fields,
                    &shared.argument,
                    &report.report,
                    &report.native_io_certificate,
                );
                fields.push(("count".into(), report.count.to_string()));
                &report.runtime
            }
            PrimitiveExecution::Unary(report) => {
                primitive_fields(
                    &mut fields,
                    &shared.argument,
                    &report.report,
                    &report.native_io_certificate,
                );
                &report.runtime
            }
            PrimitiveExecution::Aggregate(report) => {
                primitive_fields(
                    &mut fields,
                    &shared.argument,
                    &report.report,
                    &report.native_io_certificate,
                );
                &report.runtime
            }
            PrimitiveExecution::Sort(report) => {
                primitive_fields(
                    &mut fields,
                    &shared.argument,
                    &report.report,
                    &report.native_io_certificate,
                );
                &report.runtime
            }
            PrimitiveExecution::Partitioned(report) => {
                primitive_fields(
                    &mut fields,
                    &shared.argument,
                    &report.report,
                    &report.native_io_certificate,
                );
                &report.runtime
            }
            PrimitiveExecution::Relational(report) => {
                fields.extend(super::execution_fields(report, reused));
                &report.runtime
            }
        };
        crate::execution_resources::append_resident_snapshot_fields(&mut fields, runtime);
        fields.extend([
            ("result_payload_complete".into(), "true".into()),
            (
                "native_result_payload".into(),
                "retained_native_arrays".into(),
            ),
            (
                "resident_native_preparation_reused".into(),
                reused.to_string(),
            ),
        ]);
        crate::execution_resources::append_query_timing_fields(&mut fields, self.timing);
        Ok(fields)
    }
}

fn primitive_fields(
    fields: &mut Vec<(String, String)>,
    argument: &str,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
    certificate: &shardloom_core::NativeIoCertificate,
) {
    workflow::append_native_vortex_materializing_primitive_fields(
        fields,
        report,
        argument,
        Some(certificate),
        None,
    );
}

fn output_rows(report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport) -> Result<u64> {
    report
        .rows_projected
        .ok_or_else(|| failed("native producer omitted its output cardinality"))
}

fn no_streaming_input(present: bool) -> Result<()> {
    if present {
        return Err(failed(
            "primitive file requests do not accept a separate streaming source",
        ));
    }
    Ok(())
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native operation: {message}; no fallback execution was attempted"
    ))
}
