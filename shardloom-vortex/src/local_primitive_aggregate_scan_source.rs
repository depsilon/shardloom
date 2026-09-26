//! Source selection only; all aggregate lowering, updates and finalization stay shared.

use super::{
    Result, SimpleAggregateStates, VortexLocalPrimitiveEmbeddedLayoutReport,
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest, footer_aggregate, vortex_error,
};
use vortex::{
    array::{ArrayRef, dtype::DType},
    error::VortexResult,
    expr::{BoundExpression, Expression},
    file::VortexFile,
    layout::scan::scan_builder::ScanBuilder,
    session::VortexSession,
};

#[derive(Clone, Copy)]
pub(super) enum AggregateScanSource<'a> {
    File(&'a VortexFile),
    #[cfg(all(feature = "vortex-write", unix))]
    Owned(&'a crate::owned_array_source::OwnedArraySource),
}

impl AggregateScanSource<'_> {
    pub(super) fn dtype(&self) -> &DType {
        match self {
            Self::File(file) => file.dtype(),
            #[cfg(all(feature = "vortex-write", unix))]
            Self::Owned(source) => source.dtype(),
        }
    }

    pub(super) fn row_count(&self) -> u64 {
        match self {
            Self::File(file) => file.row_count(),
            #[cfg(all(feature = "vortex-write", unix))]
            Self::Owned(source) => source.row_count(),
        }
    }

    pub(super) fn scan(&self, session: &VortexSession) -> VortexResult<ScanBuilder<ArrayRef>> {
        let _ = session; // File-only feature builds use their retained file session.
        match self {
            Self::File(file) => file.scan(),
            #[cfg(all(feature = "vortex-write", unix))]
            Self::Owned(source) => Ok(source.scan(session)),
        }
    }

    pub(super) fn bind(&self, expr: &Expression) -> Result<BoundExpression> {
        expr.optimize_recursive(self.dtype())
            .and_then(|expr| expr.bind(self.dtype()))
            .map_err(vortex_error)
    }

    pub(super) fn can_prune(&self, expr: &Expression) -> VortexResult<Option<bool>> {
        match self {
            Self::File(file) => file.can_prune(expr).map(Some),
            #[cfg(all(feature = "vortex-write", unix))]
            Self::Owned(_) => Ok(None),
        }
    }

    pub(super) fn embedded_layout(
        &self,
        kind: VortexQueryPrimitiveKind,
        filter: bool,
        projection: bool,
    ) -> VortexLocalPrimitiveEmbeddedLayoutReport {
        match self {
            Self::File(file) => {
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
        &self,
        request: &VortexQueryPrimitiveRequest,
        states: &mut SimpleAggregateStates,
    ) -> Result<Option<footer_aggregate::Completion>> {
        match self {
            Self::File(file) => footer_aggregate::complete(file, request, states),
            #[cfg(feature = "vortex-write")]
            // Footer completion carries persisted-file proof fields. An owned
            // array must not fabricate those fields from its in-memory length.
            Self::Owned(_) => Ok(None),
        }
    }
}
