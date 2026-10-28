//! Metadata-only Arrow boundary schema. Data remains in its native owner.

use arrow_schema::{DataType, Field, Fields, TimeUnit};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyDict, PyList},
};

pub(super) fn fields(py: Python<'_>, fields: &Fields) -> PyResult<Py<PyList>> {
    let result = PyList::empty(py);
    for field in fields {
        result.append((field.name(), descriptor(py, field)?))?;
    }
    Ok(result.unbind())
}

fn descriptor(py: Python<'_>, field: &Field) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("nullable", field.is_nullable())?;
    let temporal = shardloom_vortex::temporal_arrow::dtype(field.data_type());
    let name = match field.data_type() {
        DataType::Boolean => "bool",
        DataType::Int8 => "int8",
        DataType::Int16 => "int16",
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::UInt8 => "uint8",
        DataType::UInt16 => "uint16",
        DataType::UInt32 => "uint32",
        DataType::UInt64 => "uint64",
        DataType::Float32 => "float32",
        DataType::Float64 => "float64",
        DataType::Utf8 => "utf8",
        DataType::Binary => "binary",
        DataType::Date32 => "date32",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_micros",
        _ if temporal.is_some() => temporal
            .as_ref()
            .expect("present temporal metadata")
            .as_str(),
        DataType::Decimal128(precision, scale) => {
            result.set_item("precision", precision)?;
            result.set_item("scale", scale)?;
            "decimal128"
        }
        DataType::List(child) => {
            result.set_item("item", descriptor(py, child)?)?;
            "list"
        }
        DataType::FixedSizeList(child, size) => {
            result.set_item("item", descriptor(py, child)?)?;
            result.set_item("size", size)?;
            "fixed_size_list"
        }
        DataType::Struct(children) => {
            result.set_item("fields", fields(py, children)?)?;
            "struct"
        }
        dtype => {
            return Err(PyValueError::new_err(format!(
                "native result schema does not admit {dtype}"
            )));
        }
    };
    result.set_item("type", name)?;
    Ok(result.unbind())
}
