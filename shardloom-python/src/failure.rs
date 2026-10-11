//! Freeze failure observations before releasing a complete native operation.

use pyo3::prelude::*;
use pyo3::types::PyDict;
use shardloom_core::{ExecutionResources, Result, ShardLoomError};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_runtime::native::EmbeddedExecutionOwner;
use shardloom_vortex::resident_session::{
    ConfigurableResidentSession, ResidentSessionSnapshot, ResidentVortexSession,
};

pub(super) struct NativeFailure {
    error: ShardLoomError,
    resources: ExecutionResources,
    admitted: Option<ResidentSessionSnapshot>,
}

impl NativeFailure {
    pub(super) fn into_python(self, map_error: impl FnOnce(ShardLoomError) -> PyErr) -> PyErr {
        let error = map_error(self.error);
        let fields = shardloom_runtime::native::failure_resource_fields(
            self.resources,
            self.admitted.as_ref(),
        );
        Python::attach(|py| {
            let attach = || -> PyResult<()> {
                let fields = super::result::fields_dict(py, fields)?;
                error
                    .value(py)
                    .getattr("__dict__")?
                    .cast::<PyDict>()?
                    .set_item("resource_fields", fields)?;
                Ok(())
            };
            // Producer exceptions keep their identity and original traceback.
            // An exception with inaccessible metadata must not be replaced by
            // a secondary attribute or allocation error while reporting it.
            let _ = attach();
            error
        })
    }
}

pub(super) fn with_resources<T>(
    owner: &EmbeddedExecutionOwner,
    resources: ExecutionResources,
    cancellation: &CancellationToken,
    operation: impl FnOnce(
        &ResidentVortexSession,
        ExecutionResources,
        &ConfigurableResidentSession,
    ) -> Result<T>,
) -> std::result::Result<T, Box<NativeFailure>> {
    let mut observed = None;
    let outcome = owner
        .controller(resources, cancellation)
        .and_then(|controller| {
            controller.with_resources(resources, cancellation, |session, admitted| {
                let outcome = operation(session, admitted, controller);
                if outcome.is_err() {
                    // The operation guard is still held. A later call cannot change
                    // the admitted grant or attach its own counters to this error.
                    observed = Some((admitted, session.snapshot()));
                }
                outcome
            })
        });
    outcome.map_err(|error| {
        Box::new(NativeFailure {
            error,
            resources: observed
                .as_ref()
                .map_or(resources, |(admitted, _)| *admitted),
            admitted: observed.map(|(_, snapshot)| snapshot),
        })
    })
}
