//! Explicit trusted application code at a native batch transformation boundary.
//!
//! A callback handle is local to its host process. It is never reconstructed by
//! importing a module, loading a library or executing code found in a plan.

use std::{cell::Cell, sync::Arc};

thread_local! {
    static CALLBACK_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

/// Reject nested native actions before they can wait on their own execution
/// grant. This protects same-thread reentry; trusted callbacks must also avoid
/// starting and awaiting an action on their active context from another thread.
///
/// # Errors
/// Returns an explicit no-fallback diagnostic during application invocation.
#[doc(hidden)]
pub fn check_application_callback_reentry() -> shardloom_core::Result<()> {
    if CALLBACK_ACTIVE.get() {
        return Err(failed(
            "an application batch callback cannot start another native action; compose the operation in the surrounding plan",
        ));
    }
    Ok(())
}

/// An immutable, trusted in-process function identity captured by a plan.
/// Cloning shares the callback declaration, never a computed answer. Replacing a
/// frontend registry name cannot change this handle's implementation.
#[derive(Clone)]
pub struct ApplicationBatchFunction {
    name: Arc<str>,
    version: Arc<str>,
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) definition: Arc<native::Definition>,
}

impl std::fmt::Debug for ApplicationBatchFunction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut description = formatter.debug_struct("ApplicationBatchFunction");
        description
            .field("name", &self.name)
            .field("version", &self.version);
        #[cfg(all(feature = "vortex-local-primitives", unix))]
        description.field("declaration", &self.definition.declaration);
        description.finish_non_exhaustive()
    }
}

impl PartialEq for ApplicationBatchFunction {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.name, &other.name)
    }
}

impl Eq for ApplicationBatchFunction {}

impl ApplicationBatchFunction {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }
}

fn failed(reason: &str) -> shardloom_core::ShardLoomError {
    shardloom_core::ShardLoomError::InvalidOperation(format!(
        "application batch function: {reason}; no fallback execution was attempted"
    ))
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "application_batch_native.rs"]
mod native;
#[cfg(all(feature = "vortex-local-primitives", unix))]
pub use native::{
    ApplicationBatchContext, ApplicationBatchDeclaration, ApplicationBatchLimits,
    ApplicationBatchOutput,
};
