use super::{capsules, native_error, resources::NativeResources, session::NativeSource};
use pyo3::{
    prelude::*,
    types::{PyCapsule, PyDict, PyList, PyString},
};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_runtime::native;
use shardloom_vortex::{
    local_primitives::prepared_relational::{
        ExecutedOwnedVortexRelational, ExecutedVortexRelational,
    },
    resident_memory_source::ResidentMemorySource,
    resident_session::{ConfigurableResidentSession, OwnedVortexResultBatch},
};
use std::sync::Arc;

#[pyclass(name = "Result", module = "shardloom._native", frozen)]
pub(super) struct NativeResult {
    data: Arc<OwnedVortexResultBatch>,
    execution: ExecutedVortexRelational,
    controller: ConfigurableResidentSession,
    reused: bool,
}

impl NativeResult {
    pub(super) fn retained_for(
        &self,
        controller: &ConfigurableResidentSession,
    ) -> Option<Arc<OwnedVortexResultBatch>> {
        controller
            .has_same_owner(&self.controller)
            .then(|| Arc::clone(&self.data))
    }

    pub(super) fn new(
        completed: ExecutedOwnedVortexRelational,
        controller: ConfigurableResidentSession,
        reused: bool,
    ) -> Self {
        Self {
            data: Arc::new(completed.result),
            execution: completed.execution,
            controller,
            reused,
        }
    }
}

#[pymethods]
impl NativeResult {
    #[getter]
    fn row_count(&self) -> u64 {
        self.data.row_count()
    }

    #[getter]
    fn schema_json(&self, py: Python<'_>) -> PyResult<Py<PyString>> {
        py.detach(|| {
            self.controller.with_resources(
                self.execution.resources,
                &CancellationToken::default(),
                |_, _| {
                    let schema = self.data.schema_json()?;
                    Ok(Python::attach(|py| {
                        PyString::new(py, schema.value()).unbind()
                    }))
                },
            )
        })
        .map_err(native_error)
    }

    #[getter]
    fn report(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        fields_dict(py, native::execution_fields(&self.execution, self.reused))
    }

    /// Explicitly construct Python rows. Native operations and columnar export
    /// bypass this conversion; returned Python containers are caller-owned memory.
    #[pyo3(signature = (*, temporal_objects=true, wire_values=false, resources=None))]
    fn row_export(
        &self,
        py: Python<'_>,
        temporal_objects: bool,
        wire_values: bool,
        resources: Option<PyRef<'_, NativeResources>>,
    ) -> PyResult<(Py<PyList>, Py<PyDict>)> {
        let resources = resources.map_or(self.execution.resources, |value| value.value);
        let cancellation = CancellationToken::default();
        let (rows, observation) = py
            .detach(|| {
                self.controller
                    .with_resources(resources, &cancellation, |_, _| {
                        let rows = Python::attach(|py| PyList::empty(py).unbind());
                        let mut stream = self.data.arrow_batches(cancellation.clone())?;
                        while let Some(batch) = stream.next_batch()? {
                            if let Err(error) = Python::attach(|py| {
                                super::rows::append_batch(
                                    py,
                                    rows.bind(py),
                                    &batch,
                                    temporal_objects,
                                    wire_values,
                                    &cancellation,
                                )
                            }) {
                                return Ok(Err(error));
                            }
                        }
                        Ok(Ok((rows, stream.report())))
                    })
            })
            .map_err(native_error)??;
        let report = capsules::observation_dict(py, observation)?;
        report.set_item("python_rows_constructed", true)?;
        report.set_item(
            "python_object_memory_scope",
            "caller_owned;outside_native_reservations",
        )?;
        Ok((rows, report.unbind()))
    }

    /// Request a one-shot columnar export with its own conversion observations.
    #[pyo3(signature = (*, resources=None))]
    fn columnar_stream(
        &self,
        py: Python<'_>,
        resources: Option<PyRef<'_, NativeResources>>,
    ) -> PyResult<capsules::NativeColumnarStream> {
        capsules::columnar_stream(
            py,
            &self.data,
            self.controller.clone(),
            resources.map_or(self.execution.resources, |value| value.value),
        )
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        capsules::result_stream(
            py,
            &self.data,
            self.controller.clone(),
            self.execution.resources,
            requested_schema,
        )
    }

    #[pyo3(signature = (*, resources=None))]
    fn as_source(
        &self,
        py: Python<'_>,
        resources: Option<PyRef<'_, NativeResources>>,
    ) -> PyResult<NativeSource> {
        let resources = resources.map_or(self.execution.resources, |value| value.value);
        let cancellation = CancellationToken::default();
        py.detach(|| {
            let source = self
                .controller
                .with_resources(resources, &cancellation, |_, _| {
                    ResidentMemorySource::from_completed_result(&self.data, &cancellation)
                })
                .map_err(native_error)?;
            Ok(NativeSource {
                source,
                _controller: self.controller.clone(),
            })
        })
    }
}

pub(super) fn fields_dict(py: Python<'_>, fields: Vec<(String, String)>) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    for (key, value) in fields {
        result.set_item(key, value)?;
    }
    Ok(result.unbind())
}
