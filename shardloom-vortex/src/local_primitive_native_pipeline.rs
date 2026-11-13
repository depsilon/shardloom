//! Compile file kernels into the same typed native operator tree as every source.
//!
//! A kernel's semantic declaration is retained for demand/ownership analysis and
//! pre-execution resource-policy changes. Execution never retries that declaration
//! after a kernel error. There is no whole-query provider-or-general dispatcher.

#[cfg(test)]
use super::PreparedRoot;
use super::{
    CancellationToken, Metrics, NativeExecutionContext, Node, NodeKind, PreparedVortexRelational,
    PreparedVortexSource, ResidentVortexSession, Result, ScanSource, SubqueryRelation,
    VortexLocalPrimitiveExecutionPolicy, add, failed, provider,
};
use shardloom_core::{
    NativeIoCertificate, NativeIoMaterializationBoundaryReport, NativeIoRepresentationTransition,
};
use shardloom_exec::live_memory::MemoryLease;
use std::cell::{Cell, RefCell};

pub(super) const NAME: &str = "native_operator_pipeline";

pub(super) struct FileKernel {
    pub(super) operation: provider::Prepared,
    pub(super) admission_reason: &'static str,
    declaration: Box<NodeKind>,
}

impl Node {
    /// The validated semantics are stable when the physical implementation changes.
    pub(super) fn semantic_kind(&self) -> &NodeKind {
        let mut kind = &self.kind;
        while let NodeKind::FileKernel(kernel) = kind {
            kind = &kernel.declaration;
        }
        kind
    }

    pub(super) fn has_file_kernel(&self) -> bool {
        matches!(self.kind, NodeKind::FileKernel(_))
    }
}

struct Compiler<'a> {
    sources: &'a [PreparedVortexSource],
    session: &'a ResidentVortexSession,
    policy: VortexLocalPrimitiveExecutionPolicy,
    file_kernels: bool,
    cancellation: &'a CancellationToken,
    context: Option<&'a NativeExecutionContext<'a>>,
}

/// Compile only after the complete declaration has passed ordinary type binding.
pub(super) fn compile(
    root: &mut Node,
    sources: &[PreparedVortexSource],
    session: &ResidentVortexSession,
    policy: VortexLocalPrimitiveExecutionPolicy,
    file_kernels: bool,
    cancellation: &CancellationToken,
) -> Result<()> {
    Compiler {
        sources,
        session,
        policy,
        file_kernels,
        cancellation,
        context: None,
    }
    .node(root)
}

impl Compiler<'_> {
    fn node(&self, node: &mut Node) -> Result<()> {
        self.cancellation.check()?;
        // This is policy compilation before execution, never failure recovery.
        if matches!(node.kind, NodeKind::FileKernel(_)) {
            if self.file_kernels {
                return Ok(());
            }
            let NodeKind::FileKernel(kernel) =
                std::mem::replace(&mut node.kind, NodeKind::Unadmitted)
            else {
                unreachable!("file kernel inspected above")
            };
            node.kind = *kernel.declaration;
        }
        if self.file_kernels
            && let Some(index) = single_file(node)
        {
            let source = self
                .sources
                .get(index)
                .ok_or_else(|| failed("native pipeline file source is not admitted"))?;
            let selection = provider::prepare(
                node,
                source,
                self.policy,
                self.session.memory(),
                self.session.float_policy(),
                self.context,
            )?;
            if let Some(operation) = selection.provider {
                let declaration = Box::new(std::mem::replace(&mut node.kind, NodeKind::Unadmitted));
                node.kind = NodeKind::FileKernel(Box::new(FileKernel {
                    operation,
                    admission_reason: selection.reason,
                    declaration,
                }));
                return Ok(());
            }
        }
        // Each accepted variant has a concrete native implementation. Adding a
        // declaration requires an explicit admission here and an execution arm;
        // an unknown operation cannot enter a generic interpreter.
        match &mut node.kind {
            NodeKind::Unadmitted => Err(failed(
                "native_pipeline_missing_operator: no native implementation was admitted",
            )),
            NodeKind::FileKernel(_) => unreachable!("compiled kernel returned above"),
            NodeKind::Scan { .. }
            | NodeKind::Outer
            | NodeKind::RecursiveInput(_)
            | NodeKind::CompletedPivot { .. } => Ok(()),
            NodeKind::Retained(binding) => self.node(&mut *binding.node()?),
            NodeKind::Recursive { anchor, step, .. } => {
                self.node(anchor)?;
                self.node(step)
            }
            NodeKind::Pipeline { input, .. }
            | NodeKind::Unary { input, .. }
            | NodeKind::Aggregate { input, .. }
            | NodeKind::Project { input, .. }
            | NodeKind::Filter { input, .. }
            | NodeKind::Sort { input, .. }
            | NodeKind::Limit { input, .. }
            | NodeKind::Window { input, .. }
            | NodeKind::GapFill { input, .. } => self.node(input),
            NodeKind::Join { left, right, .. } | NodeKind::Set { left, right, .. } => {
                self.node(left)?;
                self.node(right)
            }
            NodeKind::Subquery {
                input, relation, ..
            } => {
                self.node(input)?;
                match relation {
                    SubqueryRelation::Bound(relation) => self.node(relation),
                    SubqueryRelation::Dynamic(_) => Ok(()),
                }
            }
        }
    }
}

fn single_file(node: &Node) -> Option<usize> {
    match &node.kind {
        NodeKind::Scan {
            source: ScanSource::File(index),
            ..
        } => Some(*index),
        NodeKind::Project { input, .. }
        | NodeKind::Filter { input, .. }
        | NodeKind::Aggregate { input, .. }
        | NodeKind::Sort { input, .. }
        | NodeKind::Limit { input, .. } => single_file(input),
        _ => None,
    }
}

impl PreparedVortexRelational {
    pub(super) fn compile_native_node(
        &self,
        node: &mut Node,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        #[cfg(feature = "vortex-write")]
        let file_kernels = self.spill.is_none();
        #[cfg(not(feature = "vortex-write"))]
        let file_kernels = true;
        Compiler {
            sources: &self.sources,
            session: &self.session,
            policy: self.policy,
            file_kernels,
            cancellation: context.cancellation(),
            context: Some(context),
        }
        .node(node)
    }

    #[cfg(test)]
    pub(super) fn root_has_file_kernel(&self) -> bool {
        matches!(&self.root, PreparedRoot::Bound(root) if root.has_file_kernel())
    }

    /// Equivalence oracle only. Production has no alternate-executor switch.
    #[cfg(test)]
    pub(super) fn remove_file_kernels_for_reference(&mut self) -> Result<()> {
        if let PreparedRoot::Bound(root) = &mut self.root {
            compile(
                root,
                &self.sources,
                &self.session,
                self.policy,
                false,
                &CancellationToken::default(),
            )?;
        }
        Ok(())
    }
}

/// Bounded evidence regardless of correlated invocations or number of inputs.
#[derive(Default)]
pub(super) struct Evidence {
    pub(super) operators: Cell<u64>,
    pub(super) file_kernels: Cell<u64>,
    data_read: Cell<bool>,
    data_decoded: Cell<bool>,
    data_materialized: Cell<bool>,
    row_read: Cell<bool>,
    // Four admitted reporting kernels, independent of invocation/parameter count.
    // Summaries own credit even after a deferred operator has been released.
    summaries: RefCell<[Option<KernelSummary>; 4]>,
    keep_root_report: Cell<bool>,
    last_kernel: RefCell<Option<provider::Executed>>,
}

struct KernelSummary {
    transitions: Vec<NativeIoRepresentationTransition>,
    boundaries: Vec<NativeIoMaterializationBoundaryReport>,
    metadata: MemoryLease,
}

fn summary_bytes(
    previous: Option<&KernelSummary>,
    certificate: &NativeIoCertificate,
) -> Result<u64> {
    // Credit both vectors before a possible growth and copied boundary strings.
    // Counting an existing matching entry twice is a bounded conservative bound,
    // not a cumulative allocation per correlated invocation.
    let transitions = certificate.representation_transitions.len()
        + previous.map_or(0, |summary| summary.transitions.len());
    let bytes = transitions
        .checked_mul(std::mem::size_of::<NativeIoRepresentationTransition>())
        .ok_or_else(|| failed("native pipeline evidence capacity overflow"))?;
    let bytes = certificate
        .materialization_boundaries
        .iter()
        .chain(
            previous
                .into_iter()
                .flat_map(|summary| summary.boundaries.iter()),
        )
        .try_fold(bytes, |bytes, boundary| {
            [
                boundary.boundary_id.len(),
                boundary.required_by.len(),
                boundary.reason.len(),
                boundary.fidelity_loss.len(),
                std::mem::size_of_val(boundary),
            ]
            .into_iter()
            .try_fold(bytes, usize::checked_add)
        })
        .ok_or_else(|| failed("native pipeline evidence capacity overflow"))?;
    u64::try_from(bytes)
        .ok()
        .and_then(|bytes| bytes.checked_mul(4))
        .and_then(|bytes| bytes.checked_add(8192))
        .ok_or_else(|| failed("native pipeline evidence capacity overflow"))
}

impl Evidence {
    pub(super) fn begin_root(&self, root: &Node) {
        self.keep_root_report.set(root.has_file_kernel());
    }

    pub(super) fn record(
        &self,
        mut executed: provider::Executed,
        admission_reason: &str,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        add(&self.file_kernels, 1)?;
        if let Some(certificate) = &mut executed.certificate {
            if !certificate.is_certified() {
                return Err(failed(
                    "native_pipeline_uncertified_kernel: native file execution was not certified",
                ));
            }
            certificate
                .source_pushdown_report
                .proof_basis
                .push_str(";native_file_kernel_admission=");
            certificate
                .source_pushdown_report
                .proof_basis
                .push_str(admission_reason);
            let effects = &certificate.side_effects;
            self.data_read
                .set(self.data_read.get() || effects.data_read);
            self.data_decoded
                .set(self.data_decoded.get() || effects.data_decoded);
            self.data_materialized
                .set(self.data_materialized.get() || effects.data_materialized);
            self.row_read.set(self.row_read.get() || effects.row_read);
            if self.keep_root_report.get() {
                *self.last_kernel.borrow_mut() = Some(executed);
                return Ok(());
            }
            let index = match executed.report.as_ref().map(|report| report.primitive_kind) {
                Some(super::super::VortexQueryPrimitiveKind::CountAll) => 0,
                Some(super::super::VortexQueryPrimitiveKind::CountWhere) => 1,
                Some(super::super::VortexQueryPrimitiveKind::SimpleAggregate) => 2,
                Some(super::super::VortexQueryPrimitiveKind::SortRows) => 3,
                _ => {
                    return Err(failed(
                        "native pipeline kernel has no admitted evidence kind",
                    ));
                }
            };
            let mut summaries = self.summaries.borrow_mut();
            let bytes = summary_bytes(summaries[index].as_ref(), certificate)?;
            if let Some(summary) = &mut summaries[index] {
                summary.metadata.resize(bytes)?;
                // Current file kernels have stable boundary identities. Keep
                // varying execution counts, never an unbounded per-call ledger.
                for transition in &certificate.representation_transitions {
                    if !summary.transitions.contains(transition) {
                        summary.transitions.push(*transition);
                    }
                }
                for boundary in &certificate.materialization_boundaries {
                    if let Some(previous) = summary.boundaries.iter_mut().find(|previous| {
                        previous.boundary_id == boundary.boundary_id
                            && previous.from_state == boundary.from_state
                            && previous.to_state == boundary.to_state
                    }) {
                        previous.rows_materialized = previous
                            .rows_materialized
                            .checked_add(boundary.rows_materialized)
                            .ok_or_else(|| {
                                failed("native kernel materialization row count overflow")
                            })?;
                        previous.bytes_decoded = previous
                            .bytes_decoded
                            .checked_add(boundary.bytes_decoded)
                            .ok_or_else(|| failed("native kernel decode byte count overflow"))?;
                    } else {
                        summary.boundaries.push(boundary.clone());
                    }
                }
            } else {
                let metadata = context.memory().reserve(bytes)?;
                summaries[index] = Some(KernelSummary {
                    transitions: certificate.representation_transitions.clone(),
                    boundaries: certificate.materialization_boundaries.clone(),
                    metadata,
                });
            }
        }
        // A composed query has no use for a nested kernel's complete report.
        // Its bounded summary above owns everything retained for certification.
        Ok(())
    }

    pub(super) fn take_root_execution(&self, root: &Node) -> Option<provider::Executed> {
        let last = self.last_kernel.borrow_mut().take();
        root.has_file_kernel().then_some(last).flatten()
    }

    pub(super) fn append_certificate(
        &self,
        certificate: &mut NativeIoCertificate,
    ) -> [Option<MemoryLease>; 4] {
        use std::fmt::Write as _;
        write!(
            certificate.source_pushdown_report.proof_basis,
            " native_execution_pipeline={NAME} native_pipeline_operator_invocations={} native_pipeline_file_kernel_invocations={} native_pipeline_catch_all_execution=false",
            self.operators.get(),
            self.file_kernels.get(),
        )
        .expect("String writes cannot fail");
        certificate.side_effects.data_read |= self.data_read.get();
        certificate.side_effects.data_decoded |= self.data_decoded.get();
        certificate.side_effects.data_materialized |= self.data_materialized.get();
        certificate.side_effects.row_read |= self.row_read.get();
        certificate.adapter_fidelity_report.materialization_required |=
            self.data_materialized.get();
        let mut metadata = [None, None, None, None];
        for (index, slot) in self.summaries.borrow_mut().iter_mut().enumerate() {
            if let Some(summary) = slot.take() {
                for transition in summary.transitions {
                    if !certificate.representation_transitions.contains(&transition) {
                        certificate.representation_transitions.push(transition);
                    }
                }
                certificate
                    .materialization_boundaries
                    .extend(summary.boundaries);
                metadata[index] = Some(summary.metadata);
            }
        }
        metadata
    }
}

pub(super) fn record_operator(metrics: &Metrics<'_>) -> Result<()> {
    add(&metrics.pipeline.operators, 1)
}
