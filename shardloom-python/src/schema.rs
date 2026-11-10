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
        DataType::Float16 => "float16",
        DataType::Float64 => "float64",
        DataType::Utf8 => "utf8",
        DataType::Binary if super::binary_object::field_is_object(field) => "binary_object",
        DataType::Binary => "binary",
        DataType::FixedSizeBinary(16) if is_uuid(field) => "uuid",
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
            if field
                .metadata()
                .get("ARROW:extension:name")
                .is_some_and(|name| name == "arrow.fixed_shape_tensor")
            {
                let metadata = field
                    .metadata()
                    .get("ARROW:extension:metadata")
                    .ok_or_else(|| PyValueError::new_err("tensor result is missing metadata"))?;
                result.set_item("metadata", metadata)?;
                result.set_item("type", "fixed_shape_tensor")?;
                return Ok(result.unbind());
            }
            if field
                .metadata()
                .get("ARROW:extension:name")
                .is_some_and(|name| name == "shardloom.embedding.v1")
            {
                let metadata = field
                    .metadata()
                    .get("ARROW:extension:metadata")
                    .ok_or_else(|| PyValueError::new_err("embedding result is missing metadata"))?;
                result.set_item("metadata", metadata)?;
                "embedding"
            } else {
                "fixed_size_list"
            }
        }
        DataType::Struct(children) => {
            result.set_item("fields", fields(py, children)?)?;
            structured_name(field, &result)?
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

pub(super) fn is_uuid(field: &Field) -> bool {
    *field.data_type() == DataType::FixedSizeBinary(16)
        && field
            .metadata()
            .get("ARROW:extension:name")
            .is_some_and(|name| name == "arrow.uuid")
        && field
            .metadata()
            .get("ARROW:extension:metadata")
            .is_none_or(String::is_empty)
}

fn structured_name(field: &Field, result: &Bound<'_, PyDict>) -> PyResult<&'static str> {
    let (name, missing) = match field
        .metadata()
        .get("ARROW:extension:name")
        .map(String::as_str)
    {
        Some("arrow.parquet.variant") => return Ok("variant"),
        Some("shardloom.sparse_vector.v1") => {
            ("sparse_vector", "vector result is missing metadata")
        }
        Some("shardloom.quantized_vector.v1") => {
            ("quantized_vector", "vector result is missing metadata")
        }
        Some("arrow.variable_shape_tensor") => (
            "variable_shape_tensor",
            "variable tensor result is missing metadata",
        ),
        _ => return Ok("struct"),
    };
    let metadata = field
        .metadata()
        .get("ARROW:extension:metadata")
        .ok_or_else(|| PyValueError::new_err(missing))?;
    result.set_item("metadata", metadata)?;
    Ok(name)
}
