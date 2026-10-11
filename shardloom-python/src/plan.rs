use super::{
    batch_input::{self, BatchDeclaration, BatchReader},
    native_error,
    resources::NativeResources,
    result::NativeResult,
    session::{SessionOwner, failed},
};
use pyo3::{prelude::*, types::PyDict};
use shardloom_core::{ExecutionResources, Result};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_runtime::native::{self, SqlSource, SqlSourceBinding};
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
    batches: Vec<BatchDeclaration>,
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
        batches: Vec<BatchDeclaration>,
    ) -> Self {
        Self {
            core: Arc::new(PlanCore {
                session,
                statement,
                bindings,
                batches,
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
        execute: impl FnOnce(&PreparedVortexRelational, bool, Option<&mut BatchReader<'_>>) -> Result<T>,
    ) -> PyResult<T> {
        let errors = Arc::new(Mutex::new(None));
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
                    .is_some_and(|entry| entry.resources == resources)
                    && self.batches.iter().all(|input| input.streaming);
                if !reused {
                    *prepared = None;
                }
                let mut bindings = self.bindings.clone();
                let mut streaming = None;
                for declaration in &self.batches {
                    let reader = BatchReader::new(declaration, session, cancellation, &errors)?;
                    let source = if declaration.streaming {
                        if streaming.is_some() {
                            return Err(failed(
                                "one native operation admits one streaming input producer",
                            ));
                        }
                        let schema = reader.schema();
                        streaming = Some(reader);
                        SqlSource::ColumnarStream(schema)
                    } else {
                        SqlSource::Memory(reader.collect_resident()?)
                    };
                    bindings.push(SqlSourceBinding {
                        reference: declaration.reference.clone(),
                        source,
                    });
                }
                if !reused {
                    let operation = native::prepare_sql(
                        &self.statement,
                        resources,
                        session,
                        &bindings,
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
                execute(&entry.operation, reused, streaming.as_mut())
            })
            .map_err(|error| batch_input::map_failure(&errors, error))
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
    #[pyo3(signature = (*, resources=None, timeout=None))]
    fn iter_batches(
        &self,
        resources: Option<&NativeResources>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<super::stream::NativeExecutionStream> {
        let timeout = super::session::timeout_duration(timeout)?;
        let cancellation = self.session.begin().map_err(native_error)?;
        let resources = self.session.resources(resources);
        Ok(super::stream::NativeExecutionStream::new(
            Arc::clone(&self.core),
            resources,
            &cancellation,
            timeout,
        ))
    }

    #[pyo3(signature = (*, resources=None, timeout=None))]
    fn prepare(
        &self,
        py: Python<'_>,
        resources: Option<&NativeResources>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let cancellation = self.session.begin_operation(timeout)?;
        let resources = self.session.resources(resources);
        py.detach(|| self.with_prepared(resources, &cancellation, |_, _, _| Ok(())))
    }

    #[pyo3(signature = (*, resources=None, timeout=None))]
    fn collect(
        &self,
        py: Python<'_>,
        resources: Option<&NativeResources>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<NativeResult> {
        let cancellation = self.session.begin_operation(timeout)?;
        let resources = self.session.resources(resources);
        py.detach(|| {
            self.with_prepared(resources, &cancellation, |operation, reused, input| {
                let completed = if let Some(input) = input {
                    let mut next = || input.next();
                    operation
                        .with_arrow_batch_input(&mut next)?
                        .execute_native(&cancellation)?
                } else {
                    operation.execute_native(&cancellation)?
                };
                Ok(NativeResult::new(
                    completed,
                    self.session.controller.clone(),
                    reused,
                ))
            })
        })
    }

    #[pyo3(signature = (path, format, *, overwrite=false, resources=None, timeout=None))]
    // PyO3 converts os.PathLike values into an owned PathBuf at this boundary.
    #[allow(clippy::needless_pass_by_value)]
    fn write(
        &self,
        py: Python<'_>,
        path: PathBuf,
        format: &str,
        overwrite: bool,
        resources: Option<&NativeResources>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyDict>> {
        let cancellation = self.session.begin_operation(timeout)?;
        let resources = self.session.resources(resources);
        let format = native::output_format(format).map_err(native_error)?;
        let fields = py.detach(|| {
            self.with_prepared(resources, &cancellation, |operation, reused, input| {
                let written = if let Some(input) = input {
                    let mut next = || input.next();
                    operation
                        .with_arrow_batch_input(&mut next)?
                        .write_controlled(&path, format, overwrite, &cancellation)?
                } else {
                    operation.write_controlled(&path, format, overwrite, &cancellation)?
                };
                Ok(native::written_fields(&written, reused))
            })
        })?;
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
