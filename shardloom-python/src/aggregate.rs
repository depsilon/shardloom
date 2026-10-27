//! Inert aggregate descriptors shared by grouped and framed native reductions.

use super::relation::{column, order_keys};
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyFloat, PyInt},
};
use shardloom_vortex::query_primitive::{VortexQuantileLevel, VortexSimpleAggregateMeasure};

#[pyclass(name = "AggregateMeasure", module = "shardloom._native", frozen)]
pub(super) struct NativeAggregateMeasure {
    pub(super) value: VortexSimpleAggregateMeasure,
}

#[pymethods]
impl NativeAggregateMeasure {
    #[new]
    #[allow(clippy::too_many_arguments)] // Keyword-only fields declare one inert aggregate descriptor.
    #[pyo3(signature = (function, source_column, alias, *, secondary_column=None, filter_column=None, quantile_level=None, order_by=Vec::new(), separator=None))]
    fn new(
        function: String,
        source_column: Option<String>,
        alias: String,
        secondary_column: Option<String>,
        filter_column: Option<String>,
        quantile_level: Option<&Bound<'_, PyAny>>,
        order_by: Vec<(String, bool, Option<String>)>,
        separator: Option<String>,
    ) -> PyResult<Self> {
        let mut value = VortexSimpleAggregateMeasure::new(
            function,
            source_column.map(column).transpose()?,
            alias,
        );
        value.secondary_column = secondary_column.map(column).transpose()?;
        value.filter_column = filter_column.map(column).transpose()?;
        value.quantile_level = parse_quantile_level(&value.function, quantile_level)?;
        value.order_by = order_keys(order_by)?;
        value.separator = parse_separator(&value.function, separator)?;
        Ok(Self { value })
    }

    fn __repr__(&self) -> String {
        format!("AggregateMeasure({})", self.value.summary())
    }
}

pub(super) fn parse_separator(function: &str, value: Option<String>) -> PyResult<Option<String>> {
    let is_string_agg = function.trim().eq_ignore_ascii_case("string_agg");
    if is_string_agg != value.is_some() {
        return Err(PyValueError::new_err(
            "string_agg requires separator; other reducers do not accept it",
        ));
    }
    Ok(value)
}

pub(super) fn parse_quantile_level(
    function: &str,
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<VortexQuantileLevel>> {
    let required = matches!(
        function.trim().to_ascii_lowercase().as_str(),
        "quantile_cont" | "quantile_disc"
    );
    if required != value.is_some() {
        return Err(PyValueError::new_err(
            "quantile_cont and quantile_disc require quantile_level; other reducers do not accept it",
        ));
    }
    value
        .map(|value| {
            if !(value.is_exact_instance_of::<PyInt>() || value.is_exact_instance_of::<PyFloat>()) {
                return Err(PyTypeError::new_err(
                    "quantile level must be an int or float",
                ));
            }
            VortexQuantileLevel::new(value.extract::<f64>()?)
                .map_err(|error| PyValueError::new_err(error.to_string()))
        })
        .transpose()
}

pub(super) fn parse(value: &Bound<'_, PyAny>) -> PyResult<VortexSimpleAggregateMeasure> {
    if let Ok(value) = value.extract::<PyRef<'_, NativeAggregateMeasure>>() {
        return Ok(value.value.clone());
    }
    let (function, name, alias) = value
        .extract::<(String, Option<String>, String)>()
        .map_err(|_| {
            PyTypeError::new_err(
                "aggregate measure must be AggregateMeasure or a (function, column, alias) tuple",
            )
        })?;
    Ok(VortexSimpleAggregateMeasure::new(
        function,
        name.map(column).transpose()?,
        alias,
    ))
}
