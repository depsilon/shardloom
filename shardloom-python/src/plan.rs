use super::{
    batch_input::{self, BatchDeclaration, BatchReader, BatchReaders},
    native_error,
    resources::NativeResources,
    result::NativeResult,
    session::{SessionOwner, failed},
};
use pyo3::{prelude::*, types::PyDict};
use shardloom_core::{ExecutionResources, Result};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_runtime::native::{self, PreparedNativeOperation, SqlSource, SqlSourceBinding};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

struct Prepared {
    resources: ExecutionResources,
    operation: PreparedNativeOperation,
    reusable: bool,
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
    spill: Option<String>,
    workflow: Option<native::NativeWorkflowDeclaration>,
    relation: Option<Arc<native::NativeRelationDeclaration>>,
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
        spill: Option<String>,
    ) -> Self {
        Self {
            core: Arc::new(PlanCore {
                session,
                statement,
                bindings,
                batches,
                spill,
                workflow: None,
                relation: None,
                prepared: Mutex::new(None),
            }),
        }
    }

    pub(super) fn workflow(
        session: Arc<SessionOwner>,
        declaration: native::NativeWorkflowDeclaration,
    ) -> Self {
        Self {
            core: Arc::new(PlanCore {
                session,
                statement: String::new(),
                bindings: Vec::new(),
                batches: Vec::new(),
                spill: None,
                workflow: Some(declaration),
                relation: None,
                prepared: Mutex::new(None),
            }),
        }
    }

    pub(super) fn relation(
        session: Arc<SessionOwner>,
        declaration: Arc<native::NativeRelationDeclaration>,
        bindings: Vec<SqlSourceBinding>,
        batches: Vec<BatchDeclaration>,
        spill: Option<String>,
    ) -> Self {
        Self {
            core: Arc::new(PlanCore {
                session,
                statement: String::new(),
                bindings,
                batches,
                spill,
                workflow: None,
                relation: Some(declaration),
                prepared: Mutex::new(None),
            }),
        }
    }
}

impl PlanCore {
    /// A producer declaration remains effectful/one-shot even when preparation
    /// has collected its current values into a native memory source.
    pub(super) fn output_window_source_allowed(&self) -> bool {
        self.batches.is_empty()
    }

    pub(super) fn with_prepared<T>(
        &self,
        resources: ExecutionResources,
        cancellation: &CancellationToken,
        execute: impl FnOnce(&PreparedNativeOperation, bool, Option<&mut BatchReaders<'_>>) -> Result<T>,
    ) -> PyResult<T> {
        self.with_prepared_and_preflight(resources, cancellation, || Ok(()), execute)
    }

    fn with_prepared_and_preflight<T>(
        &self,
        resources: ExecutionResources,
        cancellation: &CancellationToken,
        preflight: impl FnOnce() -> Result<()>,
        execute: impl FnOnce(&PreparedNativeOperation, bool, Option<&mut BatchReaders<'_>>) -> Result<T>,
    ) -> PyResult<T> {
        let errors = Arc::new(Mutex::new(None));
        super::failure::with_resources(
            &self.session.runtime,
            resources,
            cancellation,
            |session, resources, _controller| {
                cancellation.check()?;
                native::validate_sql_spill(self.spill.as_deref(), resources)?;
                preflight()?;
                let mut prepared = self
                    .prepared
                    .lock()
                    .map_err(|_| failed("prepared plan ownership poisoned"))?;
                let reused = prepared
                    .as_ref()
                    .is_some_and(|entry| entry.resources == resources && entry.reusable)
                    && self.batches.iter().all(|input| input.streaming);
                if !reused {
                    *prepared = None;
                }
                let mut bindings = if reused {
                    Vec::new()
                } else {
                    self.bindings_in_session(session, cancellation)?
                };
                let mut streaming = BatchReaders::reserve(&self.batches, session)?;
                for declaration in &self.batches {
                    let reader = BatchReader::new(declaration, session, cancellation, &errors)?;
                    let source = if declaration.streaming {
                        let schema = reader.schema();
                        streaming
                            .as_mut()
                            .ok_or_else(|| failed("streaming reader ownership is absent"))?
                            .push(reader)?;
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
                    let (operation, reusable) = if let Some(declaration) = &self.relation {
                        let (operation, reusable) = declaration.prepare(
                            resources,
                            session,
                            &bindings,
                            self.spill.as_deref(),
                            cancellation,
                        )?;
                        (
                            PreparedNativeOperation::relational(operation, resources),
                            reusable,
                        )
                    } else if let Some(declaration) = &self.workflow {
                        (declaration.prepare(resources, session, cancellation)?, true)
                    } else {
                        (
                            native::prepare_sql_operation(
                                &self.statement,
                                resources,
                                session,
                                &bindings,
                                self.spill.as_deref(),
                                cancellation,
                            )?,
                            true,
                        )
                    };
                    *prepared = Some(Prepared {
                        resources,
                        reusable: reusable && operation.permits_reuse(),
                        operation,
                    });
                }
                let entry = prepared
                    .as_ref()
                    .ok_or_else(|| failed("prepared operation was not retained"))?;
                execute(&entry.operation, reused, streaming.as_mut())
            },
        )
        .map_err(|error| error.into_python(|error| batch_input::map_failure(&errors, error)))
    }

    fn bindings_in_session(
        &self,
        session: &shardloom_vortex::resident_session::ResidentVortexSession,
        cancellation: &CancellationToken,
    ) -> Result<Vec<SqlSourceBinding>> {
        let mut bindings = self.bindings.clone();
        for binding in &mut bindings {
            if let SqlSource::Memory(source) = &binding.source {
                binding.source = SqlSource::Memory(source.in_session(session, cancellation)?);
            }
        }
        Ok(bindings)
    }

    pub(super) fn controller(
        &self,
        resources: ExecutionResources,
        cancellation: &CancellationToken,
    ) -> Result<shardloom_vortex::resident_session::ConfigurableResidentSession> {
        self.session
            .runtime
            .controller(resources, cancellation)
            .cloned()
    }
}

#[pymethods]
impl NativePlan {
    /// Demand-driven columnar execution. The worker starts on first iteration;
    /// completion evidence is published only after the final consumer demand.
    #[pyo3(signature = (*, resources=None, timeout=None, output_window_bytes=0))]
    fn iter_batches(
        &self,
        resources: Option<&NativeResources>,
        timeout: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = super::stream::output_window_bytes)] output_window_bytes: u64,
    ) -> PyResult<super::stream::NativeExecutionStream> {
        let timeout = super::session::timeout_duration(timeout)?;
        let cancellation = self.session.begin().map_err(native_error)?;
        let resources = self.session.resources(resources).map_err(native_error)?;
        Ok(super::stream::NativeExecutionStream::new(
            Arc::clone(&self.core),
            resources,
            &cancellation,
            timeout,
            output_window_bytes != 0,
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
        let resources = self.session.resources(resources).map_err(native_error)?;
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
        let resources = self.session.resources(resources).map_err(native_error)?;
        py.detach(|| {
            self.with_prepared(resources, &cancellation, |operation, reused, input| {
                let completed = if let Some(input) = input {
                    let mut next =
                        |uri: &shardloom_core::DatasetUri, ordinal| input.next(uri, ordinal);
                    operation.execute_native_with_inputs(
                        &cancellation,
                        Some(native::NativeArrowInput::Sources(&mut next)),
                    )?
                } else {
                    operation.execute_native(&cancellation, None)?
                };
                Ok(NativeResult::new(
                    completed,
                    self.controller(resources, &cancellation)?,
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
        let resources = self.session.resources(resources).map_err(native_error)?;
        let format = native::output_format(format).map_err(native_error)?;
        let fields = py.detach(|| {
            self.with_prepared(resources, &cancellation, |operation, reused, input| {
                if let Some(input) = input {
                    let mut next =
                        |uri: &shardloom_core::DatasetUri, ordinal| input.next(uri, ordinal);
                    operation.write_fields_with_inputs(
                        &path,
                        format,
                        overwrite,
                        &cancellation,
                        Some(native::NativeArrowInput::Sources(&mut next)),
                        reused,
                    )
                } else {
                    operation.write_fields(&path, format, overwrite, &cancellation, None, reused)
                }
            })
        })?;
        super::result::fields_dict(py, fields)
    }

    /// Stage every declared output through the existing native fanout writer.
    /// Input preparation, all outputs and publication use one admitted operation.
    #[pyo3(signature = (outputs, *, overwrite=false, resources=None, timeout=None))]
    fn write_many(
        &self,
        py: Python<'_>,
        outputs: Vec<(PathBuf, String)>,
        overwrite: bool,
        resources: Option<&NativeResources>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyDict>> {
        let cancellation = self.session.begin_operation(timeout)?;
        let resources = self.session.resources(resources).map_err(native_error)?;
        let targets = outputs
            .into_iter()
            .map(|(path, format)| native::output_format(&format).map(|format| (path, format)))
            .collect::<Result<Vec<_>>>()
            .map_err(native_error)?;
        let fields = py.detach(|| {
            self.with_prepared_and_preflight(
                resources,
                &cancellation,
                || {
                    shardloom_vortex::local_primitives::output_fanout::preflight_targets(
                        &targets, overwrite,
                    )
                },
                |operation, reused, input| {
                    if let Some(input) = input {
                        let mut next =
                            |uri: &shardloom_core::DatasetUri, ordinal| input.next(uri, ordinal);
                        operation.write_many_fields_with_inputs(
                            &targets,
                            overwrite,
                            &cancellation,
                            Some(native::NativeArrowInput::Sources(&mut next)),
                            reused,
                        )
                    } else {
                        operation.write_many_fields(&targets, overwrite, &cancellation, reused)
                    }
                },
            )
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
