//! Inert aggregate descriptors shared by grouped and framed native reductions.

use super::relation::{column, order_keys};
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyFloat, PyInt, PyString, PyTuple},
};
use shardloom_core::decimal_output::{DecimalOutput, DecimalRounding};
use shardloom_vortex::query_primitive::{VortexQuantileLevel, VortexSimpleAggregateMeasure};

#[pyclass(name = "AggregateMeasure", module = "shardloom._native", frozen)]
pub(super) struct NativeAggregateMeasure {
    pub(super) value: VortexSimpleAggregateMeasure,
}

#[pymethods]
impl NativeAggregateMeasure {
    #[new]
    #[allow(clippy::too_many_arguments)] // Keyword-only fields declare one inert aggregate descriptor.
    #[pyo3(signature = (function, source_column, alias, *, secondary_column=None, filter_column=None, quantile_level=None, order_by=Vec::new(), separator=None, decimal_output=None))]
    fn new(
        function: String,
        source_column: Option<String>,
        alias: String,
        secondary_column: Option<String>,
        filter_column: Option<String>,
        quantile_level: Option<&Bound<'_, PyAny>>,
        order_by: Vec<(String, bool, Option<String>)>,
        separator: Option<String>,
        decimal_output: Option<&Bound<'_, PyAny>>,
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
        value.decimal_output = parse_decimal_output(&value.function, decimal_output)?;
        Ok(Self { value })
    }

    fn __repr__(&self) -> String {
        format!("AggregateMeasure({})", self.value.summary())
    }
}

pub(super) fn parse_decimal_output(
    function: &str,
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<DecimalOutput>> {
    let function = function.trim().to_ascii_lowercase();
    let supported = matches!(
        function.as_str(),
        "sum"
            | "avg"
            | "mean"
            | "sum_distinct"
            | "avg_distinct"
            | "mean_distinct"
            | "median"
            | "quantile_cont"
    );
    let Some(value) = value else {
        return Ok(None);
    };
    if !supported {
        return Err(PyValueError::new_err(
            "decimal_output is only supported for sum, avg, sum_distinct, avg_distinct, median and quantile_cont",
        ));
    }
    if !value.is_exact_instance_of::<PyTuple>() {
        return Err(PyTypeError::new_err(
            "decimal_output must be a (precision, scale, rounding) tuple",
        ));
    }
    let tuple = value.cast::<PyTuple>().map_err(|_| {
        PyTypeError::new_err("decimal_output must be a (precision, scale, rounding) tuple")
    })?;
    if tuple.len() != 3 {
        return Err(PyTypeError::new_err(
            "decimal_output must be a (precision, scale, rounding) tuple",
        ));
    }
    let precision = tuple.get_item(0)?;
    let scale = tuple.get_item(1)?;
    let rounding = tuple.get_item(2)?;
    if !precision.is_exact_instance_of::<PyInt>() || !scale.is_exact_instance_of::<PyInt>() {
        return Err(PyTypeError::new_err(
            "decimal_output precision and scale must be exact Python ints",
        ));
    }
    if !rounding.is_exact_instance_of::<PyString>() {
        return Err(PyTypeError::new_err(
            "decimal_output rounding must be a string",
        ));
    }
    let precision = precision
        .extract::<u8>()
        .map_err(|_| PyValueError::new_err("decimal_output precision must be in 1..38"))?;
    let scale = scale
        .extract::<u8>()
        .map_err(|_| PyValueError::new_err("decimal_output scale must be in 0..precision"))?;
    let rounding = DecimalRounding::parse(&rounding.extract::<String>()?)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    DecimalOutput::new(precision, scale, rounding)
        .map(Some)
        .map_err(|error| PyValueError::new_err(error.to_string()))
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
