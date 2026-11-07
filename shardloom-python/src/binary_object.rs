//! Python handles over the shared native byte owners. Python byte copies are
//! explicit caller-owned results; neither schemas nor row export read payloads.

use super::{native_error, resources::NativeResources, session::SessionOwner};
use arrow_schema::Field;
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyBytes, PyDict, PyInt},
};
use shardloom_core::ExecutionResources;
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_vortex::{
    binary_object::{BinaryObject, BinaryObjectStore},
    resident_session::{ConfigurableResidentSession, ResidentVortexSession},
};
use std::{path::PathBuf, sync::Arc};

pub(super) fn field_is_object(field: &Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .is_some_and(|name| name == "shardloom.binary-object.v1")
}

/// The resource controller remains usable by retained readers after the public
/// session closes. Construction, in contrast, still checks SessionOwner.begin.
#[derive(Clone)]
pub(super) struct ObjectOwner {
    pub(super) controller: ConfigurableResidentSession,
    pub(super) resources: ExecutionResources,
    pub(super) session: ResidentVortexSession,
}

impl ObjectOwner {
    pub(super) fn resolve(
        &self,
        descriptor: &[u8],
        cancellation: &CancellationToken,
    ) -> PyResult<NativeBinaryObject> {
        Ok(NativeBinaryObject {
            value: self
                .session
                .binary_object_from_descriptor(descriptor, cancellation)
                .map_err(native_error)?,
            owner: self.clone(),
        })
    }
}

#[pyclass(name = "BinaryObject", module = "shardloom._native", frozen)]
pub(super) struct NativeBinaryObject {
    pub(super) value: BinaryObject,
    pub(super) owner: ObjectOwner,
}

#[pymethods]
impl NativeBinaryObject {
    #[getter]
    fn byte_length(&self) -> u64 {
        self.value.len()
    }

    #[getter]
    fn ownership(&self) -> &'static str {
        if self.value.is_managed() {
            "managed"
        } else {
            "external"
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "BinaryObject(ownership={:?}, byte_length={})",
            self.ownership(),
            self.value.len()
        )
    }

    /// Copy all bytes into an explicit caller-owned Python bytes value.
    #[pyo3(signature = (*, resources=None, timeout=None))]
    fn read(
        &self,
        py: Python<'_>,
        resources: Option<&NativeResources>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyBytes>> {
        self.copy_range(py, 0..self.value.len(), resources, timeout)
    }

    /// Read exactly [start, end); missing or corrupt content is an error.
    #[pyo3(signature = (start, end, *, resources=None, timeout=None))]
    fn read_range(
        &self,
        py: Python<'_>,
        start: &Bound<'_, PyAny>,
        end: &Bound<'_, PyAny>,
        resources: Option<&NativeResources>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyBytes>> {
        self.copy_range(
            py,
            unsigned(start, "start")?..unsigned(end, "end")?,
            resources,
            timeout,
        )
    }
}

impl NativeBinaryObject {
    fn copy_range(
        &self,
        py: Python<'_>,
        range: std::ops::Range<u64>,
        resources: Option<&NativeResources>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyBytes>> {
        let cancellation = super::session::operation_cancellation(
            CancellationToken::default(),
            super::session::timeout_duration(timeout)?,
        )
        .map_err(native_error)?;
        let resources = resources.map_or(self.owner.resources, |value| value.value);
        py.detach(|| {
            self.owner
                .controller
                .with_resources(resources, &cancellation, |_, _| {
                    let bytes = self.value.read_range(range, &cancellation)?;
                    Ok(Python::attach(|py| PyBytes::new(py, &bytes).unbind()))
                })
        })
        .map_err(native_error)
    }
}

#[pyclass(name = "BinaryObjectStore", module = "shardloom._native", frozen)]
pub(super) struct NativeBinaryObjectStore {
    pub(super) value: BinaryObjectStore,
    pub(super) owner: ObjectOwner,
    pub(super) session: Arc<SessionOwner>,
}

#[pymethods]
impl NativeBinaryObjectStore {
    #[getter]
    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let snapshot = self.value.snapshot();
        let result = PyDict::new(py);
        result.set_item("limit_bytes", snapshot.limit_bytes)?;
        result.set_item("retained_bytes", snapshot.retained_bytes)?;
        result.set_item("cleanup_failures", snapshot.cleanup_failures)?;
        Ok(result.unbind())
    }

    #[pyo3(signature = (path, *, timeout=None))]
    #[allow(clippy::needless_pass_by_value)] // PyO3 extracts an owned path for the detached native call.
    fn import_file(
        &self,
        py: Python<'_>,
        path: PathBuf,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<NativeBinaryObject> {
        let cancellation = self.session.begin_operation(timeout)?;
        let value = py
            .detach(|| {
                self.owner
                    .controller
                    .with_resources(self.owner.resources, &cancellation, |_, _| {
                        self.value.import_file(&path, &cancellation)
                    })
            })
            .map_err(native_error)?;
        Ok(NativeBinaryObject {
            value,
            owner: self.owner.clone(),
        })
    }

    #[pyo3(signature = (data, *, timeout=None))]
    fn put_bytes(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyBytes>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<NativeBinaryObject> {
        let cancellation = self.session.begin_operation(timeout)?;
        let bytes = data.as_bytes();
        let value = py
            .detach(|| {
                self.owner
                    .controller
                    .with_resources(self.owner.resources, &cancellation, |_, _| {
                        self.value.put_bytes(bytes, &cancellation)
                    })
            })
            .map_err(native_error)?;
        Ok(NativeBinaryObject {
            value,
            owner: self.owner.clone(),
        })
    }
}

pub(super) fn unsigned(value: &Bound<'_, PyAny>, name: &str) -> PyResult<u64> {
    if value.is_instance_of::<PyBool>() || !value.is_instance_of::<PyInt>() {
        return Err(PyTypeError::new_err(format!(
            "{name} must be an unsigned 64-bit integer"
        )));
    }
    value
        .extract()
        .map_err(|_| PyValueError::new_err(format!("{name} must fit an unsigned 64-bit integer")))
}
