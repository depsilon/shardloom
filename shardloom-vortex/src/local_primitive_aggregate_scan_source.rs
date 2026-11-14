//! Source selection only; all aggregate lowering, updates and finalization stay shared.

use super::{
    Result, VortexLocalPrimitiveEmbeddedLayoutReport, VortexQueryPrimitiveKind, vortex_error,
};
#[cfg(unix)]
use super::{SimpleAggregateStates, VortexQueryPrimitiveRequest, footer_aggregate};
use vortex::{
    array::dtype::DType,
    error::VortexResult,
    expr::{BoundExpression, Expression},
    file::VortexFile,
    session::VortexSession,
};

#[path = "local_primitive_aggregate_scan_evidence.rs"]
mod evidence;
#[cfg(unix)]
pub(super) use evidence::FileEvidence;
use evidence::Scan;

#[derive(Clone, Copy)]
pub(super) enum AggregateScanSource<'a> {
    File(&'a VortexFile),
    #[cfg(unix)]
    ObservedFile(&'a VortexFile, &'a FileEvidence<'a>),
    #[cfg(all(feature = "vortex-write", unix))]
    Owned(&'a crate::owned_array_source::OwnedArraySource),
}

impl<'a> AggregateScanSource<'a> {
    pub(super) fn dtype(&self) -> &DType {
        match self {
            Self::File(file) => file.dtype(),
            #[cfg(unix)]
            Self::ObservedFile(file, _) => file.dtype(),
            #[cfg(all(feature = "vortex-write", unix))]
            Self::Owned(source) => source.dtype(),
        }
    }

    pub(super) fn row_count(self) -> u64 {
        match self {
            Self::File(file) => file.row_count(),
            #[cfg(unix)]
            Self::ObservedFile(file, _) => file.row_count(),
            #[cfg(all(feature = "vortex-write", unix))]
            Self::Owned(source) => source.row_count(),
        }
    }

    pub(super) fn scan(self, session: &VortexSession) -> VortexResult<Scan<'a>> {
        let _ = session; // File-only feature builds use their retained file session.
        let scan = match self {
            Self::File(file) => file.scan(),
            #[cfg(unix)]
            Self::ObservedFile(file, _) => file.scan(),
            #[cfg(all(feature = "vortex-write", unix))]
            Self::Owned(source) => Ok(source.scan(session)),
        }?;
        Ok(Scan::new(scan, self))
    }

    pub(super) fn bind(self, expr: &Expression) -> Result<BoundExpression> {
        expr.bind(self.dtype())
            .and_then(|expr| expr.optimize_recursive())
            .map_err(vortex_error)
    }

    pub(super) fn scan_reusing_text(
        self,
        session: &VortexSession,
        lowering: &super::aggregate_lowering::AggregateLowering,
        memory: Option<&shardloom_exec::live_memory::LiveMemoryPool>,
        cancellation: Option<&shardloom_exec::compute_pool::CancellationToken>,
    ) -> VortexResult<(
        Scan<'a>,
        Option<std::sync::Arc<super::text_scan_reuse::Stats>>,
    )> {
        let reused = match self {
            Self::File(file) => super::text_scan_reuse::admit(
                file,
                &lowering.plan,
                lowering.pushdown.as_ref(),
                memory,
                cancellation,
            )?,
            #[cfg(unix)]
            Self::ObservedFile(file, _) => super::text_scan_reuse::admit(
                file,
                &lowering.plan,
                lowering.pushdown.as_ref(),
                memory,
                cancellation,
            )?,
            #[cfg(all(feature = "vortex-write", unix))]
            Self::Owned(_) => None,
        };
        if let Some((scan, stats)) = reused {
            return Ok((Scan::new(scan, self), Some(stats)));
        }
        Ok((self.scan(session)?, None))
    }

    pub(super) fn can_prune(self, expr: &Expression) -> VortexResult<Option<bool>> {
        match self {
            Self::File(file) => file.can_prune(expr).map(Some),
            #[cfg(unix)]
            Self::ObservedFile(file, evidence) => {
                let excluded = file.can_prune(expr)?;
                evidence.pruning(excluded)?;
                Ok(Some(excluded))
            }
            #[cfg(all(feature = "vortex-write", unix))]
            Self::Owned(_) => Ok(None),
        }
    }

    pub(super) fn embedded_layout(
        self,
        kind: VortexQueryPrimitiveKind,
        filter: bool,
        projection: bool,
    ) -> VortexLocalPrimitiveEmbeddedLayoutReport {
        match self {
            Self::File(file) => {
                VortexLocalPrimitiveEmbeddedLayoutReport::from_file(file, kind, filter, projection)
            }
            #[cfg(unix)]
            Self::ObservedFile(file, _) => {
                VortexLocalPrimitiveEmbeddedLayoutReport::from_file(file, kind, filter, projection)
            }
            #[cfg(all(feature = "vortex-write", unix))]
            Self::Owned(_) => {
                let mut report = VortexLocalPrimitiveEmbeddedLayoutReport::not_available();
                report.status = "owned_array_source_no_file_layout".into();
                report
            }
        }
    }

    #[cfg(unix)]
    pub(super) fn complete(
        self,
        request: &VortexQueryPrimitiveRequest,
        states: &mut SimpleAggregateStates,
    ) -> Result<Option<footer_aggregate::Completion>> {
        match self {
            Self::File(file) => footer_aggregate::complete(file, request, states),
            Self::ObservedFile(file, _) => footer_aggregate::complete(file, request, states),
            #[cfg(feature = "vortex-write")]
            // Footer completion carries persisted-file proof fields. An owned
            // array must not fabricate those fields from its in-memory length.
            Self::Owned(_) => Ok(None),
        }
    }
}
