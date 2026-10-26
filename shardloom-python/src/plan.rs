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

#[path = "plan_inputs.rs"]
mod inputs;
use inputs::{InputOwners, Inputs};

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
    pub(super) session: Arc<SessionOwner>,
    statement: String,
    inputs: Arc<InputOwners>,
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
    pub(super) fn sql(
        session: Arc<SessionOwner>,
        statement: String,
        declaration: Arc<native::NativeRelationDeclaration>,
        bindings: Vec<SqlSourceBinding>,
        batches: Vec<BatchDeclaration>,
        parents: Vec<&NativePlan>,
        spill: Option<String>,
    ) -> PyResult<Self> {
        let inputs = InputOwners::new(
            bindings,
            batches,
            parents
                .into_iter()
                .map(|plan| Arc::clone(&plan.inputs))
                .collect(),
        )
        .map_err(native_error)?;
        Ok(Self {
            core: Arc::new(PlanCore {
                session,
                statement,
                inputs,
                spill,
                workflow: None,
                relation: Some(declaration),
                prepared: Mutex::new(None),
            }),
        })
    }

    pub(super) fn workflow(
        session: Arc<SessionOwner>,
        declaration: native::NativeWorkflowDeclaration,
    ) -> Self {
        Self {
            core: Arc::new(PlanCore {
                session,
                statement: String::new(),
                inputs: Arc::default(),
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
        parents: Vec<&NativePlan>,
        spill: Option<String>,
    ) -> PyResult<Self> {
        Self::sql(
            session,
            String::new(),
            declaration,
            bindings,
            batches,
            parents,
            spill,
        )
    }

    pub(super) fn declaration(
        &self,
        owner: &Arc<SessionOwner>,
    ) -> PyResult<Arc<native::NativeRelationDeclaration>> {
        if !Arc::ptr_eq(&self.session, owner) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "relation belongs to another context; bind plans from the same context",
            ));
        }
        self.session.begin().map_err(native_error)?;
        self.relation.clone().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "command workflow has no composable relation declaration",
            )
        })
    }
}

impl PlanCore {
    /// A producer declaration remains effectful/one-shot even when preparation
    /// has collected its current values into a native memory source.
    pub(super) fn output_window_source_allowed(&self) -> bool {
        !self.inputs.has_batches
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
                let inputs = self.inputs.inputs(cancellation)?;
                let mut prepared = self
                    .prepared
                    .lock()
                    .map_err(|_| failed("prepared plan ownership poisoned"))?;
                let reused = prepared
                    .as_ref()
                    .is_some_and(|entry| entry.resources == resources && entry.reusable)
                    && inputs.batches.iter().all(|input| input.streaming);
                if !reused {
                    *prepared = None;
                }
                let admission = if !reused && self.statement.is_empty() {
                    self.relation
                        .as_ref()
                        .map(|declaration| declaration.admit(session, cancellation))
                        .transpose()?
                } else {
                    None
                };
                let mut bindings = if reused {
                    Vec::new()
                } else {
                    Self::bindings_in_session(&inputs, session, cancellation)?
                };
                let mut streaming = BatchReaders::reserve(&inputs.batches, session)?;
                for declaration in &inputs.batches {
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
                    let (operation, reusable) = if !self.statement.is_empty() {
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
                    } else if let Some(declaration) = &self.relation {
                        let (operation, reusable) = declaration.prepare_admitted(
                            resources,
                            session,
                            &bindings,
                            self.spill.as_deref(),
                            cancellation,
                            admission
                                .ok_or_else(|| failed("relation metadata admission is absent"))?,
                        )?;
                        (
                            PreparedNativeOperation::relational(operation, resources),
                            reusable,
                        )
                    } else if let Some(declaration) = &self.workflow {
                        (declaration.prepare(resources, session, cancellation)?, true)
                    } else {
                        return Err(failed("plan has no native declaration"));
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
        inputs: &Inputs<'_>,
        session: &shardloom_vortex::resident_session::ResidentVortexSession,
        cancellation: &CancellationToken,
    ) -> Result<Vec<SqlSourceBinding>> {
        let mut bindings = inputs
            .bindings
            .iter()
            .map(|binding| (*binding).clone())
            .collect::<Vec<_>>();
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
    /// Direct input positions retained by native binding, before source flattening.
    /// These are indices in the supplied relation list, including source-free plans.
    #[getter]
    fn bound_input_indices(&self) -> PyResult<Vec<usize>> {
        Ok(self.declaration(&self.session)?.input_indices().collect())
    }

    /// Report retained source references without inspecting or opening them.
    #[getter]
    fn source_references(&self) -> PyResult<Vec<String>> {
        Ok(self
            .declaration(&self.session)?
            .source_references()
            .collect())
    }

    /// Retain the typed immutable relation, without preparing or reading inputs.
    #[staticmethod]
    fn parameter_names(statement: &str) -> PyResult<Vec<String>> {
        native::NativeRelationDeclaration::parameter_names(statement).map_err(native_error)
    }

    /// Retain the typed immutable relation, without preparing or reading inputs.
    fn as_relation(&self) -> PyResult<super::relation::NativeRelation> {
        Ok(super::relation::NativeRelation {
            declaration: self.declaration(&self.session)?,
        })
    }

    /// Add position-bound native metrics, retaining this plan's source owners.
    #[allow(clippy::needless_pass_by_value)] // PyO3 extracts an owned Python sequence.
    fn observe(&self, name: &str, measures: Vec<String>) -> PyResult<Self> {
        let declaration = self.declaration(&self.session)?;
        let observed = declaration.observe(name, &measures).map_err(native_error)?;
        Self::relation(
            self.session.clone(),
            Arc::new(observed),
            vec![],
            vec![],
            vec![self],
            self.spill.clone(),
        )
    }

    /// Inspect native output metadata without executing a producer or consumer.
    /// Prepared execution state is neither populated nor reused by inspection.
    #[pyo3(signature = (*, resources=None, timeout=None))]
    fn schema_json(
        &self,
        py: Python<'_>,
        resources: Option<&NativeResources>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<String> {
        let cancellation = self.session.begin_operation(timeout)?;
        let resources = self.session.resources(resources).map_err(native_error)?;
        let declaration = self.relation.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("command workflow has no relation schema")
        })?;
        py.detach(|| {
            super::failure::with_resources(
                &self.session.runtime,
                resources,
                &cancellation,
                |session, resources, _| {
                    let inputs = self.inputs.inputs(&cancellation)?;
                    let admission = declaration.admit(session, &cancellation)?;
                    let mut bindings =
                        PlanCore::bindings_in_session(&inputs, session, &cancellation)?;
                    let errors = Arc::new(Mutex::new(None));
                    // Readers retain only their declared schema. Their lazy
                    // factories are never invoked, including one-shot inputs.
                    let mut readers = Vec::with_capacity(inputs.batches.len());
                    for input in &inputs.batches {
                        let reader = BatchReader::new(input, session, &cancellation, &errors)?;
                        bindings.push(SqlSourceBinding {
                            reference: input.reference.clone(),
                            source: SqlSource::ColumnarStream(reader.schema()),
                        });
                        readers.push(reader);
                    }
                    let schema = declaration.schema_json_admitted(
                        resources,
                        session,
                        &bindings,
                        self.spill.as_deref(),
                        &cancellation,
                        admission,
                    )?;
                    cancellation.check()?;
                    Ok(schema.into_parts().0)
                },
            )
            .map_err(|error| error.into_python(native_error))
        })
    }

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
