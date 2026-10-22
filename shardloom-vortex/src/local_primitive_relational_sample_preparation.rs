//! Admission and completion for scalar metadata on one immutable sampling root.

use super::{Node, NodeKind, PreparedRoot, PreparedVortexRelational, ScanSource};
use crate::local_primitives::prepared_unary::sample_preparation::{
    CallGuard, FailureGuard, Preparation,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;

impl Node {
    pub(super) fn admit_sampling_preparation(&mut self, memory: &LiveMemoryPool) -> Result<()> {
        let NodeKind::Unary { input, operation } = &mut self.kind else {
            return Ok(());
        };
        let NodeKind::Scan {
            source: ScanSource::Memory(source),
            plan,
            residual: None,
            ..
        } = &input.kind
        else {
            return Ok(());
        };
        if plan.filter.is_none()
            && plan.residual_predicate.is_none()
            && plan.source_order_limit.is_none()
        {
            operation.admit_sampling_preparation(source.source_rows(), &input.fields, memory)?;
        }
        Ok(())
    }
}

impl PreparedVortexRelational {
    pub(super) fn sampling_preparation(&self) -> Option<&Preparation> {
        let PreparedRoot::Bound(root) = &self.root else {
            return None;
        };
        let NodeKind::Unary { operation, .. } = &root.kind else {
            return None;
        };
        operation.sampling_preparation()
    }

    pub(super) fn disable_sampling_preparation(&mut self) {
        if let PreparedRoot::Bound(root) = &mut self.root
            && let NodeKind::Unary { operation, .. } = &mut root.kind
        {
            operation.disable_sampling_preparation();
        }
    }

    pub(super) fn begin_sampling_call(&self) -> Result<CallGuard<'_>> {
        CallGuard::begin(self.sampling_preparation(), self.session.memory())
    }

    pub(super) fn sampling_failure_guard(&self) -> FailureGuard<'_> {
        FailureGuard::new(self.sampling_preparation())
    }
}

#[cfg(test)]
type OwnershipFault = Box<dyn FnOnce(&mut Vec<super::ArrayRef>)>;

#[cfg(test)]
thread_local! {
    // Faults here exercise actual final ownership after the native call succeeds.
    pub(super) static BEFORE_RESULT_OWNERSHIP: std::cell::RefCell<Option<OwnershipFault>> =
        const { std::cell::RefCell::new(None) };
    pub(super) static BEFORE_JSON_COMPLETION: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    #[cfg(feature = "universal-format-io")]
    pub(super) static BEFORE_ARROW_COMPLETION: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}
