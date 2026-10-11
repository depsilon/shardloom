use super::{native_error, plan::NativePlan, resources::NativeResources, result::NativeResult};
use arrow_array::{RecordBatch, RecordBatchReader, ffi_stream::ArrowArrayStreamReader};
use arrow_pyarrow::FromPyArrow as _;
use pyo3::{
    exceptions::{PyRuntimeError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyFloat, PyInt},
};
use shardloom_core::{ExecutionResources, Result, ShardLoomError};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_runtime::native::{EmbeddedExecutionOwner, SqlSource, SqlSourceBinding};
use shardloom_vortex::{
    resident_memory_source::{MemoryBatchSourceBuilder, ResidentMemorySource},
    resident_session::ConfigurableResidentSession,
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

struct Lifecycle {
    closed: bool,
    cancellation: CancellationToken,
}

pub(super) struct SessionOwner {
    pub(super) runtime: EmbeddedExecutionOwner,
    lifecycle: Mutex<Lifecycle>,
}

impl SessionOwner {
    pub(super) fn begin(&self) -> Result<CancellationToken> {
        let state = self
            .lifecycle
            .lock()
            .map_err(|_| failed("session lifecycle poisoned"))?;
        if state.closed {
            return Err(failed(
                "session is closed; retained completed results remain valid",
            ));
        }
        Ok(state.cancellation.clone())
    }

    pub(super) fn resources(
        &self,
        supplied: Option<&NativeResources>,
    ) -> Result<ExecutionResources> {
        self.runtime.resources(supplied.map(|value| value.value))
    }

    pub(super) fn begin_operation(
        &self,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<CancellationToken> {
        let timeout = timeout_duration(timeout)?;
        operation_cancellation(self.begin().map_err(native_error)?, timeout).map_err(native_error)
    }
}

pub(super) fn timeout_duration(timeout: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Duration>> {
    timeout
        .map(|value| {
            if value.is_instance_of::<PyBool>()
                || !(value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>())
            {
                return Err(PyTypeError::new_err(
                    "timeout must be a finite nonnegative number of seconds",
                ));
            }
            Duration::try_from_secs_f64(value.extract::<f64>()?).map_err(|_| {
                PyValueError::new_err("timeout must be a finite nonnegative number of seconds")
            })
        })
        .transpose()
}

pub(super) fn operation_cancellation(
    parent: CancellationToken,
    timeout: Option<Duration>,
) -> Result<CancellationToken> {
    let token = if let Some(timeout) = timeout {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| failed("timeout exceeds the monotonic clock's supported range"))?;
        parent.with_deadline(deadline)
    } else {
        parent
    };
    token.check()?;
    Ok(token)
}

#[pyclass(name = "Session", module = "shardloom._native", frozen)]
pub(super) struct NativeSession {
    owner: Arc<SessionOwner>,
}

#[pyclass(name = "Source", module = "shardloom._native", frozen)]
pub(super) struct NativeSource {
    pub(super) source: ResidentMemorySource,
    pub(super) _controller: ConfigurableResidentSession,
}

#[pymethods]
impl NativeSource {
    #[getter]
    fn schema_json(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyString>> {
        py.detach(|| {
            let schema = self.source.schema_json()?;
            Ok(Python::attach(|py| {
                pyo3::types::PyString::new(py, schema.value()).unbind()
            }))
        })
        .map_err(native_error)
    }

    #[getter]
    fn row_count(&self) -> usize {
        self.source.row_count()
    }

    #[getter]
    fn input_columnar_buffer_bytes_copied(&self) -> u64 {
        self.source.intake_columnar_buffer_bytes_copied()
    }
}

#[pymethods]
impl NativeSession {
    #[new]
    #[pyo3(signature = (resources=None, *, memory_limit_bytes=None, parallelism_limit=None))]
    fn new(
        py: Python<'_>,
        resources: Option<&NativeResources>,
        memory_limit_bytes: Option<&Bound<'_, PyAny>>,
        parallelism_limit: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let resources = resources.map(|value| value.value);
        let limits = super::resources::parse_limits(memory_limit_bytes, parallelism_limit)?;
        let runtime = py
            .detach(|| EmbeddedExecutionOwner::new(resources, limits))
            .map_err(native_error)?;
        Ok(Self {
            owner: Arc::new(SessionOwner {
                runtime,
                lifecycle: Mutex::new(Lifecycle {
                    closed: false,
                    cancellation: CancellationToken::default(),
                }),
            }),
        })
    }

    /// Invoke the shared command dispatcher with this session's ownership and
    /// cancellation. Each data handler parses and admits its own complete call.
    #[pyo3(signature = (args, *, timeout=None))]
    fn command(
        &self,
        py: Python<'_>,
        args: Vec<String>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<String> {
        let cancellation = self.owner.begin_operation(timeout)?;
        py.detach(|| {
            shardloom_runtime::native::command_json_in_owner(
                args,
                &self.owner.runtime,
                &cancellation,
            )
        })
        .map_err(native_error)
    }

    /// Construct a native plan declaration without inspecting any source.
    #[pyo3(signature = (arguments))]
    fn workflow(&self, py: Python<'_>, arguments: Vec<String>) -> PyResult<NativePlan> {
        self.owner.begin().map_err(native_error)?;
        let declaration = py
            .detach(|| shardloom_runtime::native::NativeWorkflowDeclaration::new(arguments))
            .map_err(native_error)?;
        Ok(NativePlan::workflow(Arc::clone(&self.owner), declaration))
    }

    /// Construct a native SQL declaration without inspecting any source.
    #[pyo3(signature = (statement, *, files=Vec::new(), sources=Vec::new(), batches=Vec::new(), spill=None))]
    fn sql(
        &self,
        statement: String,
        files: Vec<(String, String, Option<String>)>,
        sources: Vec<(String, PyRef<'_, NativeSource>)>,
        batches: Vec<super::batch_input::BatchArgument>,
        spill: Option<String>,
    ) -> PyResult<NativePlan> {
        self.owner.begin().map_err(native_error)?;
        Ok(NativePlan::new(
            Arc::clone(&self.owner),
            statement,
            source_bindings(files, sources),
            batches.into_iter().map(Into::into).collect(),
            spill,
        ))
    }

    /// Bind a typed native relation without rendering or parsing SQL text.
    #[pyo3(signature = (relation, *, files=Vec::new(), sources=Vec::new(), batches=Vec::new(), spill=None))]
    fn plan(
        &self,
        relation: &super::relation::NativeRelation,
        files: Vec<(String, String, Option<String>)>,
        sources: Vec<(String, PyRef<'_, NativeSource>)>,
        batches: Vec<super::batch_input::BatchArgument>,
        spill: Option<String>,
    ) -> PyResult<NativePlan> {
        self.owner.begin().map_err(native_error)?;
        Ok(NativePlan::relation(
            Arc::clone(&self.owner),
            Arc::clone(&relation.declaration),
            source_bindings(files, sources),
            batches.into_iter().map(Into::into).collect(),
            spill,
        ))
    }

    /// Copy columnar producer buffers into admitted native owners, without JSON
    /// cells or Python rows. Producer callbacks run only after resource admission.
    #[pyo3(name = "from_arrow", signature = (producer, *, resources=None, timeout=None))]
    fn import_arrow(
        &self,
        py: Python<'_>,
        producer: Py<PyAny>,
        resources: Option<&NativeResources>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<NativeSource> {
        let cancellation = self.owner.begin_operation(timeout)?;
        let resources = self.owner.resources(resources).map_err(native_error)?;
        let controller = py
            .detach(|| {
                self.owner
                    .runtime
                    .controller(resources, &cancellation)
                    .cloned()
            })
            .map_err(native_error)?;
        let native_result = producer
            .bind(py)
            .extract::<PyRef<'_, NativeResult>>()
            .ok()
            .and_then(|result| result.retained_for(&controller));
        py.detach(move || {
            let mut python_error = None;
            let source = controller.with_resources(resources, &cancellation, |session, _| {
                if let Some(result) = &native_result {
                    return ResidentMemorySource::from_completed_result(result, &cancellation);
                }
                let mut builder = MemoryBatchSourceBuilder::new(session, cancellation.clone())?;
                let imported = Python::attach(|py| {
                    let producer = producer.bind(py);
                    if !producer.hasattr("__arrow_c_stream__")? {
                        return Err(PyTypeError::new_err("native columnar input requires __arrow_c_stream__; no row conversion is implied"));
                    }
                    ArrowArrayStreamReader::from_pyarrow_bound(producer)
                });
                let mut reader = match imported {
                    Ok(reader) => reader,
                    Err(error) => {
                        python_error = Some(error);
                        return Err(failed("columnar producer did not supply an admitted Arrow stream"));
                    }
                };
                let schema = reader.schema();
                let mut batches = 0_u64;
                loop {
                    cancellation.check()?;
                    // A Python-backed producer may need the interpreter for its
                    // callback. Native copying/execution remains detached.
                    let batch = Python::attach(|_| reader.next());
                    let Some(batch) = batch else { break; };
                    let batch = batch.map_err(|error| failed(&format!("columnar producer: {error}")))?;
                    if batch.schema() != schema {
                        return Err(failed("columnar producer changed its declared schema"));
                    }
                    builder.push_arrow(batch)?;
                    batches = batches.checked_add(1).ok_or_else(|| failed("columnar input batch count overflow"))?;
                }
                if batches == 0 {
                    builder.push_arrow(RecordBatch::new_empty(schema))?;
                }
                cancellation.check()?;
                builder.finish()
            });
            match source {
                Ok(source) => Ok(NativeSource { source, _controller: controller }),
                Err(error) => Err(python_error.unwrap_or_else(|| native_error(error))),
            }
        })
    }

    #[pyo3(name = "from_range", signature = (name, start, step, rows, *, resources=None, timeout=None))]
    #[allow(clippy::too_many_arguments)] // Mirrors the public range and operation-control arguments.
    fn create_range(
        &self,
        py: Python<'_>,
        name: String,
        start: i64,
        step: i64,
        rows: usize,
        resources: Option<&NativeResources>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<NativeSource> {
        let cancellation = self.owner.begin_operation(timeout)?;
        let resources = self.owner.resources(resources).map_err(native_error)?;
        let controller = py
            .detach(|| {
                self.owner
                    .runtime
                    .controller(resources, &cancellation)
                    .cloned()
            })
            .map_err(native_error)?;
        py.detach(move || {
            let source = controller
                .with_resources(resources, &cancellation, |session, _| {
                    ResidentMemorySource::from_int64_range(session, &name, start, step, rows)
                })
                .map_err(native_error)?;
            Ok(NativeSource {
                source,
                _controller: controller,
            })
        })
    }

    /// Convert ordinary Python mappings in admitted native batches. No cell is
    /// encoded as JSON and no external library evaluates or converts the rows.
    #[pyo3(name = "from_rows", signature = (rows, *, fields=None, resources=None, timeout=None))]
    fn import_rows(
        &self,
        py: Python<'_>,
        rows: Py<PyAny>,
        fields: Option<Vec<(String, String)>>,
        resources: Option<&NativeResources>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<NativeSource> {
        let cancellation = self.owner.begin_operation(timeout)?;
        let resources = self.owner.resources(resources).map_err(native_error)?;
        let controller = py
            .detach(|| {
                self.owner
                    .runtime
                    .controller(resources, &cancellation)
                    .cloned()
            })
            .map_err(native_error)?;
        py.detach(move || {
            let mut python_error = None;
            let source = controller.with_resources(resources, &cancellation, |session, _| {
                let mut builder = MemoryBatchSourceBuilder::new(session, cancellation.clone())?;
                let converted = super::object_input::import_rows(
                    &rows,
                    fields.as_deref(),
                    &mut builder,
                    &cancellation,
                );
                if let Err(error) = converted {
                    python_error = Some(error);
                    return Err(failed("Python object input conversion failed"));
                }
                builder.finish()
            });
            match source {
                Ok(source) => Ok(NativeSource {
                    source,
                    _controller: controller,
                }),
                Err(error) => Err(python_error.unwrap_or_else(|| native_error(error))),
            }
        })
    }

    /// Cancel current and queued calls while allowing subsequent calls to reuse
    /// the session with a fresh cancellation owner.
    fn cancel(&self) -> PyResult<()> {
        let mut state = self
            .owner
            .lifecycle
            .lock()
            .map_err(|_| PyRuntimeError::new_err("session lifecycle poisoned"))?;
        state.cancellation.cancel();
        state.cancellation = CancellationToken::default();
        Ok(())
    }

    /// Revoke new session work and cancel current calls. Completed result owners
    /// retain their runtime, buffers and explicit export allocation.
    fn close(&self) -> PyResult<()> {
        let mut state = self
            .owner
            .lifecycle
            .lock()
            .map_err(|_| PyRuntimeError::new_err("session lifecycle poisoned"))?;
        state.closed = true;
        state.cancellation.cancel();
        Ok(())
    }

    /// Observe retained native reservations without admitting or executing work.
    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyDict>> {
        let observed = self.owner.runtime.snapshot();
        let values = pyo3::types::PyDict::new(py);
        values.set_item("native_owner_initialized", observed.is_some())?;
        values.set_item(
            "memory_limit_bytes",
            observed.as_ref().map(|value| value.memory.limit_bytes),
        )?;
        values.set_item(
            "reserved_bytes",
            observed.as_ref().map(|value| value.memory.reserved_bytes),
        )?;
        values.set_item(
            "peak_reserved_bytes",
            observed
                .as_ref()
                .map(|value| value.memory.peak_reserved_bytes),
        )?;
        values.set_item(
            "denied_reservations",
            observed
                .as_ref()
                .map(|value| value.memory.denied_reservations),
        )?;
        values.set_item(
            "admitted_max_parallelism",
            observed
                .as_ref()
                .map(|value| value.admitted_max_parallelism),
        )?;
        values.set_item(
            "prepared_source_opens",
            observed.as_ref().map(|value| value.prepared_source_opens),
        )?;
        values.set_item(
            "completed_executions",
            observed.as_ref().map(|value| value.completed_executions),
        )?;
        values.set_item(
            "scope",
            "shared_native_reservations;excludes_caller_memory_and_process_rss",
        )?;
        Ok(values.unbind())
    }
}

fn source_bindings(
    files: Vec<(String, String, Option<String>)>,
    sources: Vec<(String, PyRef<'_, NativeSource>)>,
) -> Vec<SqlSourceBinding> {
    files
        .into_iter()
        .map(|(reference, format, schema)| SqlSourceBinding {
            reference,
            source: SqlSource::File { format, schema },
        })
        .chain(
            sources
                .into_iter()
                .map(|(reference, source)| SqlSourceBinding {
                    reference,
                    source: SqlSource::Memory(source.source.clone()),
                }),
        )
        .collect()
}

pub(super) fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native Python boundary: {message}; no fallback execution was attempted"
    ))
}
