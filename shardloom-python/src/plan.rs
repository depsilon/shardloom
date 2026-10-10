use super::{
    native_error,
    resources::NativeResources,
    result::NativeResult,
    session::{SessionOwner, failed},
};
use pyo3::{prelude::*, types::PyDict};
use shardloom_core::{ExecutionResources, Result};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_runtime::native::{self, SqlSourceBinding};
use shardloom_vortex::local_primitives::prepared_relational::PreparedVortexRelational;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

struct Prepared {
    resources: ExecutionResources,
    operation: PreparedVortexRelational,
}

#[pyclass(name = "Plan", module = "shardloom._native", frozen)]
pub(super) struct NativePlan {
    pub(super) core: Arc<PlanCore>,
}

pub(super) struct PlanCore {
    session: Arc<SessionOwner>,
    statement: String,
    bindings: Vec<SqlSourceBinding>,
    prepared: Mutex<Option<Prepared>>,
}

impl std::ops::Deref for NativePlan {
    type Target = PlanCore;

    fn deref(&self) -> &PlanCore {
        &self.core
    }
}

impl NativePlan {
    pub(super) fn new(
        session: Arc<SessionOwner>,
        statement: String,
        bindings: Vec<SqlSourceBinding>,
    ) -> Self {
        Self {
            core: Arc::new(PlanCore {
                session,
                statement,
                bindings,
                prepared: Mutex::new(None),
            }),
        }
    }
}

impl PlanCore {
    pub(super) fn with_prepared<T>(
        &self,
        resources: ExecutionResources,
        cancellation: &CancellationToken,
        execute: impl FnOnce(&PreparedVortexRelational, bool) -> Result<T>,
    ) -> Result<T> {
        self.session
            .controller
            .with_resources(resources, cancellation, |session, resources| {
                cancellation.check()?;
                let mut prepared = self
                    .prepared
                    .lock()
                    .map_err(|_| failed("prepared plan ownership poisoned"))?;
                let reused = prepared
                    .as_ref()
                    .is_some_and(|entry| entry.resources == resources);
                if !reused {
                    *prepared = None;
                    let operation = native::prepare_sql(
                        &self.statement,
                        resources,
                        session,
                        &self.bindings,
                        cancellation,
                    )?;
                    *prepared = Some(Prepared {
                        resources,
                        operation,
                    });
                }
                let entry = prepared
                    .as_ref()
                    .ok_or_else(|| failed("prepared operation was not retained"))?;
                execute(&entry.operation, reused)
            })
    }

    pub(super) fn controller(
        &self,
    ) -> shardloom_vortex::resident_session::ConfigurableResidentSession {
        self.session.controller.clone()
    }
}

#[pymethods]
impl NativePlan {
    /// Demand-driven columnar execution. The worker starts on first iteration;
    /// completion evidence is published only after the final consumer demand.
    #[pyo3(signature = (*, resources=None))]
    fn iter_batches(
        &self,
        resources: Option<&NativeResources>,
    ) -> PyResult<super::stream::NativeExecutionStream> {
        let cancellation = self.session.begin().map_err(native_error)?;
        let resources = self.session.resources(resources);
        Ok(super::stream::NativeExecutionStream::new(
            Arc::clone(&self.core),
            resources,
            &cancellation,
        ))
    }

    #[pyo3(signature = (*, resources=None))]
    fn prepare(&self, py: Python<'_>, resources: Option<&NativeResources>) -> PyResult<()> {
        let cancellation = self.session.begin().map_err(native_error)?;
        let resources = self.session.resources(resources);
        py.detach(|| self.with_prepared(resources, &cancellation, |_, _| Ok(())))
            .map_err(native_error)
    }

    #[pyo3(signature = (*, resources=None))]
    fn collect(
        &self,
        py: Python<'_>,
        resources: Option<&NativeResources>,
    ) -> PyResult<NativeResult> {
        let cancellation = self.session.begin().map_err(native_error)?;
        let resources = self.session.resources(resources);
        py.detach(|| {
            self.with_prepared(resources, &cancellation, |operation, reused| {
                let completed = operation.execute_native(&cancellation)?;
                Ok(NativeResult::new(
                    completed,
                    self.session.controller.clone(),
                    reused,
                ))
            })
        })
        .map_err(native_error)
    }

    #[pyo3(signature = (path, format, *, overwrite=false, resources=None))]
    // PyO3 converts os.PathLike values into an owned PathBuf at this boundary.
    #[allow(clippy::needless_pass_by_value)]
    fn write(
        &self,
        py: Python<'_>,
        path: PathBuf,
        format: &str,
        overwrite: bool,
        resources: Option<&NativeResources>,
    ) -> PyResult<Py<PyDict>> {
        let cancellation = self.session.begin().map_err(native_error)?;
        let resources = self.session.resources(resources);
        let format = native::output_format(format).map_err(native_error)?;
        let fields = py
            .detach(|| {
                self.with_prepared(resources, &cancellation, |operation, reused| {
                    let written =
                        operation.write_controlled(&path, format, overwrite, &cancellation)?;
                    Ok(native::written_fields(&written, reused))
                })
            })
            .map_err(native_error)?;
        super::result::fields_dict(py, fields)
    }

    /// Drop retained preparation. The plan declaration and separately retained
    /// results remain usable; subsequent execution prepares the shared plan anew.
    fn release_prepared(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            let mut prepared = self
                .prepared
                .try_lock()
                .map_err(|_| failed("prepared plan is executing or its ownership is poisoned; finish or close its stream before releasing preparation"))?;
            *prepared = None;
            Ok(())
        })
        .map_err(native_error)
    }
}
