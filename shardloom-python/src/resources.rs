use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyBool, PyInt},
};
use shardloom_core::{ExecutionResourceLimits, ExecutionResourceOrigin, ExecutionResources};

/// An explicit allocation. There are no memory, CPU, or environment defaults.
#[pyclass(name = "ExecutionResources", module = "shardloom._native", frozen)]
pub(super) struct NativeResources {
    pub(super) value: ExecutionResources,
}

#[pymethods]
impl NativeResources {
    #[new]
    #[pyo3(signature = (memory_bytes, max_parallelism, *, memory_origin="execution_call", parallelism_origin="execution_call", memory_limit_bytes=None, parallelism_limit=None))]
    fn new(
        memory_bytes: &Bound<'_, PyAny>,
        max_parallelism: &Bound<'_, PyAny>,
        memory_origin: &str,
        parallelism_origin: &str,
        memory_limit_bytes: Option<&Bound<'_, PyAny>>,
        parallelism_limit: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let memory_bytes = positive_u64(memory_bytes, "memory_bytes")?;
        let max_parallelism = positive_usize(max_parallelism, "max_parallelism")?;
        let memory_origin = memory_origin
            .parse::<ExecutionResourceOrigin>()
            .map_err(configuration)?;
        let parallelism_origin = parallelism_origin
            .parse::<ExecutionResourceOrigin>()
            .map_err(configuration)?;
        let memory_limit = memory_limit_bytes
            .map(|value| positive_u64(value, "memory_limit_bytes"))
            .transpose()?;
        let parallelism_limit = parallelism_limit
            .map(|value| positive_usize(value, "parallelism_limit"))
            .transpose()?;
        let limits = if memory_limit.is_some() || parallelism_limit.is_some() {
            Some(
                ExecutionResourceLimits::new(memory_limit, parallelism_limit)
                    .map_err(configuration)?,
            )
        } else {
            None
        };
        Ok(Self {
            value: ExecutionResources::from_declaration(
                memory_bytes,
                max_parallelism,
                memory_origin,
                parallelism_origin,
                limits,
            )
            .map_err(configuration)?,
        })
    }

    #[getter]
    fn memory_bytes(&self) -> u64 {
        self.value.memory_bytes()
    }

    #[getter]
    fn max_parallelism(&self) -> usize {
        self.value.max_parallelism()
    }
}

fn positive_u64(value: &Bound<'_, PyAny>, name: &str) -> PyResult<u64> {
    if value.is_instance_of::<PyBool>() || !value.is_instance_of::<PyInt>() {
        return Err(configuration(format!("{name} must be a positive integer")));
    }
    let parsed = value.extract::<u64>().map_err(|_| {
        configuration(format!(
            "{name} must fit unsigned 64-bit bytes or execution lanes"
        ))
    })?;
    if parsed == 0 {
        return Err(configuration(format!("{name} must be positive")));
    }
    Ok(parsed)
}

fn positive_usize(value: &Bound<'_, PyAny>, name: &str) -> PyResult<usize> {
    usize::try_from(positive_u64(value, name)?)
        .map_err(|_| configuration(format!("{name} exceeds the platform integer range")))
}

fn configuration(message: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(format!("SL_CONFIGURATION_ERROR: {message}"))
}
