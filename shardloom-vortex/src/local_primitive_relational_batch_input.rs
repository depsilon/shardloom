//! Single-use input under the existing admission, native operators and sinks.

use super::{
    ArrayRef, BATCH_ROWS, CancellationToken, CollectedVortexRelational, ExecutedVortexRelational,
    Metrics, NativeExecutionContext, Node, PreparedVortexRelational, ResidentMemorySource,
    ResidentVortexSession, Result, VortexRelationalPlan,
};

pub(super) type Provider<'a> =
    dyn FnMut(&ResidentVortexSession) -> Result<Option<ResidentMemorySource>> + 'a;

/// Successful completion evidence for a finite, single-use native batch source.
/// Logical bytes describe input values/offsets/validity/names, not capacity or RSS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutedVortexBatchInput {
    pub payload_batches: u64,
    pub rows: u64,
    pub input_logical_bytes: u64,
    pub intake_payload_bytes_copied: u64,
    pub max_batch_rows: usize,
    pub max_retained_input_batches: usize,
    pub max_retained_input_logical_bytes: u64,
    pub end_of_input_observed: bool,
    /// Every delivered native batch owns compact output independently of input.
    pub output_ownership_detached: bool,
}

impl Default for ExecutedVortexBatchInput {
    fn default() -> Self {
        Self {
            payload_batches: 0,
            rows: 0,
            input_logical_bytes: 0,
            intake_payload_bytes_copied: 0,
            max_batch_rows: 0,
            max_retained_input_batches: 0,
            max_retained_input_logical_bytes: 0,
            end_of_input_observed: false,
            output_ownership_detached: true,
        }
    }
}

/// An execution-scoped input provider. Each terminal method consumes this adapter;
/// the prepared plan retains only the declared schema, never a replayable source.
pub struct VortexRelationalBatchInput<'p, 'i> {
    prepared: &'p PreparedVortexRelational,
    input: &'i mut Provider<'i>,
}

impl PreparedVortexRelational {
    /// Attach one source to an already bound, admitted row-local plan. Each call
    /// must transfer a private `from_batch_columns` source from the supplied
    /// session; `None` proves end-of-input. The callback must not reenter query
    /// admission. Its errors prevent a successful final report/publication.
    /// # Errors
    /// Rejects a provider when the plan has no streaming source declaration.
    pub fn with_batch_input<'p, 'i>(
        &'p self,
        input: &'i mut Provider<'i>,
    ) -> Result<VortexRelationalBatchInput<'p, 'i>> {
        self.validate_batch_provider(true)?;
        Ok(VortexRelationalBatchInput {
            prepared: self,
            input,
        })
    }

    pub(super) fn validate_batch_provider(&self, supplied: bool) -> Result<()> {
        if self.batch_source.is_some() != supplied {
            return Err(failed(if supplied {
                "an input provider requires a declared streaming batch source"
            } else {
                "streaming input requires with_batch_input for this execution"
            }));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // One loop reuses the already admitted operator tree.
    pub(super) fn run_with_input(
        &self,
        root: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics,
        batch_rows: usize,
        input: Option<&mut Provider<'_>>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<Option<ExecutedVortexBatchInput>> {
        self.validate_batch_provider(input.is_some())?;
        let Some(input) = input else {
            self.run(root, context, metrics, batch_rows, None, consume)?;
            return Ok(None);
        };
        let (_, schema) = self
            .batch_source
            .as_ref()
            .expect("validated input declaration");
        let mut report = ExecutedVortexBatchInput::default();
        loop {
            context.check_cancelled()?;
            let next = input(&self.session)?;
            context.check_cancelled()?;
            let Some(source) = next else {
                report.end_of_input_observed = true;
                return Ok(Some(report));
            };
            if report.payload_batches == crate::resident_memory_source::MAX_BATCHES as u64 {
                return Err(failed("streaming source exceeds 4,096 payload batches"));
            }
            if !source.belongs_to_session(&self.session)
                || source.dtype() != schema.dtype()
                || source.row_count() > crate::resident_memory_source::MAX_BATCH_ROWS
                || source.input_logical_bytes() > 32 * 1024 * 1024
            {
                return Err(failed(
                    "input changed its declared schema, session or finite batch bounds",
                ));
            }
            let released = source.batch_release_witness()?;
            let rows = source.row_count();
            let bytes = source.input_logical_bytes() as u64;
            report.payload_batches += 1;
            checked_add(&mut report.rows, rows as u64)?;
            checked_add(&mut report.input_logical_bytes, bytes)?;
            checked_add(
                &mut report.intake_payload_bytes_copied,
                source.intake_payload_bytes_copied(),
            )?;
            report.max_batch_rows = report.max_batch_rows.max(rows);
            report.max_retained_input_batches = 1;
            report.max_retained_input_logical_bytes =
                report.max_retained_input_logical_bytes.max(bytes);
            *metrics.current_input.borrow_mut() = Some(source);
            let result = self.run(root, context, metrics, batch_rows, None, consume);
            drop(metrics.current_input.borrow_mut().take());
            result?;
            if released.strong_count() != 0 {
                return Err(failed(
                    "input aliases remain live after consumption; transfer private batches",
                ));
            }
        }
    }
}

impl VortexRelationalBatchInput<'_, '_> {
    /// Deliver independently owned native batches; success requires source end.
    /// # Errors
    /// Propagates input, consumer, cancellation, schema and resource failures.
    pub fn for_each_batch(
        self,
        cancellation: &CancellationToken,
        consume: impl FnMut(ArrayRef, &NativeExecutionContext<'_>) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        self.prepared
            .for_each_batch_with_input(cancellation, BATCH_ROWS, Some(self.input), consume)
    }

    /// Materialize bounded JSON batches through the same native operation.
    /// # Errors
    /// Rejects invalid delivery bounds and propagates every execution failure.
    pub fn for_each_json_batch(
        self,
        cancellation: &CancellationToken,
        batch_rows: usize,
        max_batch_bytes: usize,
        consume: impl FnMut(super::super::collect::SerializedVortexResultBatch) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        self.prepared.for_each_json_batch_with_input(
            cancellation,
            batch_rows,
            max_batch_bytes,
            Some(self.input),
            consume,
        )
    }

    /// Collect a small complete result, after observing all source input.
    /// # Errors
    /// Rejects output above 65,536 rows or 8 MiB and every execution failure.
    pub fn collect_jsonl(
        self,
        cancellation: &CancellationToken,
    ) -> Result<CollectedVortexRelational> {
        self.prepared
            .collect_jsonl_with_input(cancellation, Some(self.input))
    }

    /// Publish one native Vortex output only after input and sink completion.
    /// # Errors
    /// Rejects compatibility output before demand; propagates input/sink failures.
    #[cfg(feature = "vortex-write")]
    pub fn write_controlled(
        self,
        path: &std::path::Path,
        format: super::super::VortexLocalPrimitiveRowExportFormat,
        overwrite: bool,
        cancellation: &CancellationToken,
    ) -> Result<super::WrittenVortexRelational> {
        if format != super::super::VortexLocalPrimitiveRowExportFormat::Vortex {
            return Err(failed(
                "streaming input requires one native Vortex destination; choose explicit resident mode for compatibility exports",
            ));
        }
        self.prepared
            .write_with_input(path, format, overwrite, cancellation, Some(self.input))
    }
}

/// Check the complete lowered shape before any binding that could execute rows.
pub(super) fn classify(
    plan: &VortexRelationalPlan,
    uri: &shardloom_core::DatasetUri,
    depth: usize,
) -> Result<()> {
    if depth > 24 {
        return Err(failed("streaming plan exceeds 24 operator levels"));
    }
    let operator = match plan {
        VortexRelationalPlan::Scan(scan) if &scan.source_uri == uri => return Ok(()),
        VortexRelationalPlan::Project(project) => return classify(&project.input, uri, depth + 1),
        VortexRelationalPlan::Filter(filter) => return classify(&filter.input, uri, depth + 1),
        VortexRelationalPlan::Scan(_) => "another source",
        VortexRelationalPlan::Join(_) => "join/repeated source",
        VortexRelationalPlan::Set(_) => "set operation/repeated source",
        VortexRelationalPlan::Window(_) => "window",
        VortexRelationalPlan::Subquery(_) | VortexRelationalPlan::CorrelatedSubquery(_) => {
            "subquery"
        }
        VortexRelationalPlan::Outer => "correlated source",
        VortexRelationalPlan::Sort(_) => "sort",
        VortexRelationalPlan::Limit(_) => "limit/offset",
        VortexRelationalPlan::Aggregate(_) => "aggregate",
        VortexRelationalPlan::Unary(_) => "unary stateful operator",
        VortexRelationalPlan::ExecutionResult(_) | VortexRelationalPlan::DeferredSubquery(_) => {
            "dynamic schema"
        }
    };
    Err(failed(&format!(
        "streaming input does not admit {operator}; choose explicit resident mode"
    )))
}

fn checked_add(total: &mut u64, value: u64) -> Result<()> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| failed("input completion counter overflow"))?;
    Ok(())
}

pub(super) fn failed(reason: &str) -> shardloom_core::ShardLoomError {
    shardloom_core::ShardLoomError::InvalidOperation(format!(
        "SL-NATIVE-BATCH: {reason}; no fallback execution was attempted"
    ))
}
