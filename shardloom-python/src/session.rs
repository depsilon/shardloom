use super::{native_error, plan::NativePlan, resources::NativeResources, result::NativeResult};
use arrow_array::{RecordBatch, RecordBatchReader, ffi_stream::ArrowArrayStreamReader};
use arrow_pyarrow::FromPyArrow as _;
use pyo3::{
    exceptions::{PyRuntimeError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyBytes, PyFloat, PyInt},
};
use shardloom_core::{ExecutionResources, FloatPolicy, Result, ShardLoomError};
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

    #[getter]
    fn intake_transfer_report(&self) -> std::collections::BTreeMap<&'static str, u64> {
        let report = self.source.intake_transfer_report();
        [
            (
                "encoded_array_nodes_copied",
                report.encoded_array_nodes_copied,
            ),
            ("encoded_buffer_copies", report.encoded_buffer_copies),
            (
                "encoded_buffer_bytes_copied",
                report.encoded_buffer_bytes_copied,
            ),
            ("reused_array_references", report.reused_array_references),
            ("reused_buffer_references", report.reused_buffer_references),
            ("materialized_batches", report.materialized_batches),
            ("materialized_rows", report.materialized_rows),
            ("retained_metadata_bytes", report.retained_metadata_bytes),
            (
                "provider_scratch_peak_reserved_bytes",
                report.provider_scratch_peak_reserved_bytes,
            ),
            ("ipc_transport_bytes", report.ipc_transport_bytes),
            ("ipc_buffer_bytes_copied", report.ipc_buffer_bytes_copied),
            (
                "ipc_decode_workspace_peak_reserved_bytes",
                report.ipc_decode_workspace_peak_reserved_bytes,
            ),
            (
                "ipc_coordinate_arrays_executed",
                report.ipc_coordinate_arrays_executed,
            ),
            (
                "ipc_coordinate_values_checked",
                report.ipc_coordinate_values_checked,
            ),
        ]
        .into_iter()
        .collect()
    }
}

#[pymethods]
impl NativeSession {
    #[new]
    #[pyo3(signature = (resources=None, *, memory_limit_bytes=None, parallelism_limit=None, float_policy="strict"))]
    fn new(
        py: Python<'_>,
        resources: Option<&NativeResources>,
        memory_limit_bytes: Option<&Bound<'_, PyAny>>,
        parallelism_limit: Option<&Bound<'_, PyAny>>,
        float_policy: &str,
    ) -> PyResult<Self> {
        let resources = resources.map(|value| value.value);
        let limits = super::resources::parse_limits(memory_limit_bytes, parallelism_limit)?;
        let float_policy = FloatPolicy::parse(float_policy).map_err(native_error)?;
        let runtime = py
            .detach(|| {
                EmbeddedExecutionOwner::new_with_float_policy(resources, limits, float_policy)
            })
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

    #[getter]
    fn float_policy(&self) -> &'static str {
        self.owner.runtime.float_policy().as_str()
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
    #[allow(clippy::needless_pass_by_value)] // PyO3 sequences own their borrowed Python plan guards.
    #[pyo3(signature = (statement, *, files=Vec::new(), sources=Vec::new(), batches=Vec::new(), ranges=Vec::new(), relations=Vec::new(), parameters=Vec::new(), spill=None))]
    #[allow(clippy::too_many_arguments)] // Explicit bindings at the Python boundary.
    fn sql(
        &self,
        statement: String,
        files: Vec<(String, String, Option<String>)>,
        sources: Vec<(String, PyRef<'_, NativeSource>)>,
        batches: Vec<super::batch_input::BatchArgument>,
        ranges: Vec<(String, String, i64, i64, usize)>,
        relations: Vec<(String, PyRef<'_, NativePlan>)>,
        parameters: Vec<(String, PyRef<'_, super::expression::NativeExpression>)>,
        spill: Option<String>,
    ) -> PyResult<NativePlan> {
        self.owner.begin().map_err(native_error)?;
        let ordinary = relations.is_empty() && parameters.is_empty();
        let declarations = relations
            .iter()
            .map(|(name, plan)| Ok((name.clone(), plan.declaration(&self.owner)?)))
            .collect::<PyResult<Vec<_>>>()?;
        let mut values = std::collections::BTreeMap::new();
        for (name, expression) in parameters {
            if values.insert(name, (*expression.value).clone()).is_some() {
                return Err(PyValueError::new_err(
                    "SQL parameter names must be distinct",
                ));
            }
        }
        let declaration = Arc::new(
            shardloom_runtime::native::NativeRelationDeclaration::sql_with_bound_parameters(
                &statement,
                declarations,
                values,
            )
            .map_err(native_error)?,
        );
        let parents = declaration
            .input_indices()
            .map(|index| &*relations[index].1)
            .collect();
        let bindings = source_bindings(files, sources, ranges);
        declaration
            .validate_named_sources(
                bindings
                    .iter()
                    .map(|binding| binding.reference.as_str())
                    .chain(batches.iter().map(|batch| batch.0.as_str())),
            )
            .map_err(native_error)?;
        NativePlan::sql(
            Arc::clone(&self.owner),
            if ordinary && declaration.depth() <= 12 && statement.len() <= 256 * 1024 {
                statement
            } else {
                String::new()
            },
            declaration,
            bindings,
            batches.into_iter().map(Into::into).collect(),
            parents,
            spill,
        )
    }

    /// Bind a typed native relation without rendering or parsing SQL text.
    #[allow(clippy::needless_pass_by_value)] // PyO3 sequences own their borrowed Python plan guards.
    #[pyo3(signature = (relation, *, files=Vec::new(), sources=Vec::new(), batches=Vec::new(), ranges=Vec::new(), inputs=Vec::new(), spill=None))]
    #[allow(clippy::too_many_arguments)] // Explicit bindings at the Python boundary.
    fn plan(
        &self,
        relation: &super::relation::NativeRelation,
        files: Vec<(String, String, Option<String>)>,
        sources: Vec<(String, PyRef<'_, NativeSource>)>,
        batches: Vec<super::batch_input::BatchArgument>,
        ranges: Vec<(String, String, i64, i64, usize)>,
        inputs: Vec<PyRef<'_, NativePlan>>,
        spill: Option<String>,
    ) -> PyResult<NativePlan> {
        self.owner.begin().map_err(native_error)?;
        for input in &inputs {
            input.declaration(&self.owner)?;
        }
        NativePlan::relation(
            Arc::clone(&self.owner),
            Arc::clone(&relation.declaration),
            source_bindings(files, sources, ranges),
            batches.into_iter().map(Into::into).collect(),
            inputs.iter().map(|input| &**input).collect(),
            spill,
        )
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
        py.detach(move || {
            let mut python_error = None;
            let source = super::failure::with_resources(&self.owner.runtime, resources, &cancellation, |session, _, controller| {
                let native_result = Python::attach(|py| {
                    producer.bind(py).extract::<PyRef<'_, NativeResult>>()
                        .ok().map(|result| result.retained())
                });
                if let Some(result) = &native_result {
                    return Ok(NativeSource {
                        source: ResidentMemorySource::from_completed_result_in_session(session, result, &cancellation)?,
                        _controller: controller.clone(),
                    });
                }
                let native_source = Python::attach(|py| {
                    producer.bind(py).extract::<PyRef<'_, NativeSource>>().ok().map(|source| source.source.clone())
                });
                if let Some(source) = native_source {
                    return Ok(NativeSource {
                        source: source.in_session(session, &cancellation)?,
                        _controller: controller.clone(),
                    });
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
                Ok(NativeSource { source: builder.finish()?, _controller: controller.clone() })
            });
            source.map_err(|failure| failure.into_python(|error| python_error.unwrap_or_else(|| native_error(error))))
        })
    }

    /// Import complete pinned native messages. Immutable bytes remain borrowed
    /// through intake; no Python callback, capsule or implicit conversion runs.
    #[pyo3(name = "from_vortex_ipc", signature = (array_message, *, dtype_message, resources=None, timeout=None))]
    fn import_vortex_ipc(
        &self,
        py: Python<'_>,
        array_message: &Bound<'_, PyBytes>,
        dtype_message: &Bound<'_, PyBytes>,
        resources: Option<&NativeResources>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<NativeSource> {
        let cancellation = self.owner.begin_operation(timeout)?;
        let resources = self.owner.resources(resources).map_err(native_error)?;
        let array_message = array_message.as_bytes();
        let dtype_message = dtype_message.as_bytes();
        py.detach(|| {
            super::failure::with_resources(
                &self.owner.runtime,
                resources,
                &cancellation,
                |session, _, controller| {
                    Ok(NativeSource {
                        source: ResidentMemorySource::from_vortex_ipc(
                            session,
                            dtype_message,
                            array_message,
                            &cancellation,
                        )?,
                        _controller: controller.clone(),
                    })
                },
            )
            .map_err(|failure| failure.into_python(native_error))
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
        py.detach(move || {
            super::failure::with_resources(
                &self.owner.runtime,
                resources,
                &cancellation,
                |session, _, controller| {
                    Ok(NativeSource {
                        source: ResidentMemorySource::from_int64_range(
                            session, &name, start, step, rows,
                        )?,
                        _controller: controller.clone(),
                    })
                },
            )
            .map_err(|failure| failure.into_python(native_error))
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
        py.detach(move || {
            let mut python_error = None;
            let source = super::failure::with_resources(
                &self.owner.runtime,
                resources,
                &cancellation,
                |session, _, controller| {
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
                    Ok(NativeSource {
                        source: builder.finish()?,
                        _controller: controller.clone(),
                    })
                },
            );
            source.map_err(|failure| {
                failure.into_python(|error| python_error.unwrap_or_else(|| native_error(error)))
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
            "allocation_peak_reserved_bytes",
            observed
                .as_ref()
                .map(|value| value.memory.allocation_peak_reserved_bytes),
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
    ranges: Vec<(String, String, i64, i64, usize)>,
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
        .chain(
            ranges
                .into_iter()
                .map(|(reference, column, start, step, rows)| SqlSourceBinding {
                    reference,
                    source: SqlSource::Range {
                        column,
                        start,
                        step,
                        rows,
                    },
                }),
        )
        .collect()
}

pub(super) fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native Python boundary: {message}; no fallback execution was attempted"
    ))
}
