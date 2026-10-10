use super::{native_error, plan::NativePlan, resources::NativeResources, result::NativeResult};
use arrow_array::{RecordBatch, RecordBatchReader, ffi_stream::ArrowArrayStreamReader};
use arrow_pyarrow::FromPyArrow as _;
use pyo3::{
    exceptions::{PyRuntimeError, PyTypeError},
    prelude::*,
};
use shardloom_core::{ExecutionResources, Result, ShardLoomError};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_runtime::native::{SqlSource, SqlSourceBinding};
use shardloom_vortex::{
    resident_memory_source::{MemoryBatchSourceBuilder, ResidentMemorySource},
    resident_session::ConfigurableResidentSession,
};
use std::sync::{Arc, Mutex};

struct Lifecycle {
    closed: bool,
    cancellation: CancellationToken,
}

pub(super) struct SessionOwner {
    pub(super) controller: ConfigurableResidentSession,
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

    pub(super) fn resources(&self, supplied: Option<&NativeResources>) -> ExecutionResources {
        supplied.map_or_else(
            || self.controller.configured_resources(),
            |value| value.value,
        )
    }
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
    fn new(py: Python<'_>, resources: &NativeResources) -> PyResult<Self> {
        let resources = resources.value;
        let controller = py
            .detach(|| ConfigurableResidentSession::new(resources))
            .map_err(native_error)?;
        Ok(Self {
            owner: Arc::new(SessionOwner {
                controller,
                lifecycle: Mutex::new(Lifecycle {
                    closed: false,
                    cancellation: CancellationToken::default(),
                }),
            }),
        })
    }

    /// Construct a native plan declaration without inspecting any source.
    #[pyo3(signature = (statement, *, files=Vec::new(), sources=Vec::new()))]
    fn sql(
        &self,
        statement: String,
        files: Vec<(String, String, Option<String>)>,
        sources: Vec<(String, PyRef<'_, NativeSource>)>,
    ) -> PyResult<NativePlan> {
        self.owner.begin().map_err(native_error)?;
        let mut bindings = files
            .into_iter()
            .map(|(reference, format, schema)| SqlSourceBinding {
                reference,
                source: SqlSource::File { format, schema },
            })
            .collect::<Vec<_>>();
        bindings.extend(
            sources
                .into_iter()
                .map(|(reference, source)| SqlSourceBinding {
                    reference,
                    source: SqlSource::Memory(source.source.clone()),
                }),
        );
        Ok(NativePlan::new(
            Arc::clone(&self.owner),
            statement,
            bindings,
        ))
    }

    /// Copy columnar producer buffers into admitted native owners, without JSON
    /// cells or Python rows. Producer callbacks run only after resource admission.
    #[pyo3(name = "from_arrow", signature = (producer, *, resources=None))]
    fn import_arrow(
        &self,
        py: Python<'_>,
        producer: Py<PyAny>,
        resources: Option<&NativeResources>,
    ) -> PyResult<NativeSource> {
        let cancellation = self.owner.begin().map_err(native_error)?;
        let resources = self.owner.resources(resources);
        let controller = self.owner.controller.clone();
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

    #[pyo3(name = "from_range", signature = (name, start, step, rows, *, resources=None))]
    fn create_range(
        &self,
        py: Python<'_>,
        name: String,
        start: i64,
        step: i64,
        rows: usize,
        resources: Option<&NativeResources>,
    ) -> PyResult<NativeSource> {
        let cancellation = self.owner.begin().map_err(native_error)?;
        let resources = self.owner.resources(resources);
        let controller = self.owner.controller.clone();
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
        let observed = self.owner.controller.snapshot();
        let values = pyo3::types::PyDict::new(py);
        values.set_item("memory_limit_bytes", observed.memory.limit_bytes)?;
        values.set_item("reserved_bytes", observed.memory.reserved_bytes)?;
        values.set_item("peak_reserved_bytes", observed.memory.peak_reserved_bytes)?;
        values.set_item("denied_reservations", observed.memory.denied_reservations)?;
        values.set_item(
            "admitted_max_parallelism",
            observed.admitted_max_parallelism,
        )?;
        values.set_item("prepared_source_opens", observed.prepared_source_opens)?;
        values.set_item("completed_executions", observed.completed_executions)?;
        values.set_item(
            "scope",
            "shared_native_reservations;excludes_caller_memory_and_process_rss",
        )?;
        Ok(values.unbind())
    }
}

pub(super) fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native Python boundary: {message}; no fallback execution was attempted"
    ))
}
