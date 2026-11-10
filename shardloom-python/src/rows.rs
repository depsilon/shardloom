//! Explicit native-columnar to Python-object conversion. Query execution and
//! Arrow exchange never call this endpoint. Python containers are caller memory.

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    FixedSizeListArray, Float16Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, TimeUnit};
use pyo3::{
    IntoPyObjectExt as _,
    exceptions::{PyOverflowError, PyValueError},
    prelude::*,
    types::{PyBytes, PyDict, PyList, PyModule},
};
use shardloom_exec::compute_pool::CancellationToken;

pub(super) fn append_batch(
    py: Python<'_>,
    rows: &Bound<'_, PyList>,
    batch: &RecordBatch,
    temporal_objects: bool,
    wire_values: bool,
    cancellation: &CancellationToken,
    owner: &super::binary_object::ObjectOwner,
) -> PyResult<()> {
    let conversion = Conversion {
        py,
        temporal_objects,
        wire_values,
        datetime: PyModule::import(py, "datetime")?,
        decimal: PyModule::import(py, "decimal")?.getattr("Decimal")?,
        uuid: PyModule::import(py, "uuid")?.getattr("UUID")?,
        temporal_to_python: PyModule::import(py, "shardloom._result_schema")?
            .getattr("temporal_python_value")?,
        cancellation,
        owner,
    };
    for index in 0..batch.num_rows() {
        if index % 256 == 0 {
            cancellation.check().map_err(super::native_error)?;
            py.check_signals()?;
        }
        let row = PyDict::new(py);
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            row.set_item(
                field.name(),
                conversion.value(field, column.as_ref(), index)?,
            )?;
        }
        rows.append(row)?;
    }
    Ok(())
}

struct Conversion<'py> {
    py: Python<'py>,
    temporal_objects: bool,
    wire_values: bool,
    datetime: Bound<'py, PyModule>,
    decimal: Bound<'py, PyAny>,
    uuid: Bound<'py, PyAny>,
    temporal_to_python: Bound<'py, PyAny>,
    cancellation: &'py CancellationToken,
    owner: &'py super::binary_object::ObjectOwner,
}

impl Conversion<'_> {
    fn value(&self, field: &Field, array: &dyn Array, row: usize) -> PyResult<Py<PyAny>> {
        let py = self.py;
        if array.is_null(row) {
            return Ok(py.None());
        }
        macro_rules! scalar {
            ($array:ty) => {
                downcast::<$array>(array)?.value(row).into_py_any(py)
            };
        }
        match array.data_type() {
            DataType::Boolean => scalar!(BooleanArray),
            DataType::Int8 => scalar!(Int8Array),
            DataType::Int16 => scalar!(Int16Array),
            DataType::Int32 => scalar!(Int32Array),
            DataType::Int64 => scalar!(Int64Array),
            DataType::UInt8 => scalar!(UInt8Array),
            DataType::UInt16 => scalar!(UInt16Array),
            DataType::UInt32 => scalar!(UInt32Array),
            DataType::UInt64 => scalar!(UInt64Array),
            DataType::Float32 => {
                shardloom_core::float_policy::widen_f32(downcast::<Float32Array>(array)?.value(row))
                    .into_py_any(py)
            }
            DataType::Float16 => shardloom_core::float_policy::widen_f16_bits(
                downcast::<Float16Array>(array)?.value(row).to_bits(),
            )
            .into_py_any(py),
            DataType::Float64 => scalar!(Float64Array),
            DataType::Utf8 => scalar!(StringArray),
            DataType::Binary => self.binary(field, downcast::<BinaryArray>(array)?.value(row)),
            DataType::FixedSizeBinary(16) if super::schema::is_uuid(field) => {
                self.uuid_value(downcast::<FixedSizeBinaryArray>(array)?.value(row))
            }
            DataType::Decimal128(precision, scale) => self.decimal_value(
                downcast::<Decimal128Array>(array)?.value(row),
                *precision,
                *scale,
            ),
            DataType::Date32 => {
                self.temporal(i64::from(downcast::<Date32Array>(array)?.value(row)), true)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => self.temporal(
                downcast::<TimestampMicrosecondArray>(array)?.value(row),
                false,
            ),
            dtype if shardloom_vortex::temporal_arrow::dtype(dtype).is_some() => {
                use shardloom_core::temporal::TemporalKind;
                let temporal =
                    shardloom_vortex::temporal_arrow::dtype(dtype).expect("admitted temporal type");
                let ticks = shardloom_vortex::temporal_arrow::value(array, row)
                    .map_err(super::native_error)?
                    .expect("nonnull row");
                if !self.temporal_objects || self.wire_values {
                    return ticks.into_py_any(py);
                }
                let family = match temporal.kind() {
                    TemporalKind::Date32 => "date32",
                    TemporalKind::Date64 => "date64",
                    TemporalKind::Time(_) => "time",
                    TemporalKind::Duration(_) => "duration",
                    TemporalKind::Timestamp { .. } => "timestamp",
                };
                Ok(self
                    .temporal_to_python
                    .call1((
                        ticks,
                        (
                            family,
                            temporal
                                .unit()
                                .map(shardloom_core::temporal::TemporalUnit::as_str),
                            temporal.timezone(),
                        ),
                    ))?
                    .unbind())
            }
            DataType::List(child) => {
                self.list(child, downcast::<ListArray>(array)?.value(row).as_ref())
            }
            DataType::FixedSizeList(child, _) => self.list(
                child,
                downcast::<FixedSizeListArray>(array)?.value(row).as_ref(),
            ),
            DataType::Struct(fields) => {
                let structure = downcast::<StructArray>(array)?;
                let result = PyDict::new(py);
                for (field, column) in fields.iter().zip(structure.columns()) {
                    result.set_item(field.name(), self.value(field, column.as_ref(), row)?)?;
                }
                Ok(result.into_any().unbind())
            }
            dtype => Err(PyValueError::new_err(format!(
                "native Python row conversion does not admit {dtype}; no fallback execution was attempted",
            ))),
        }
    }

    fn decimal_value(&self, coefficient: i128, precision: u8, scale: i8) -> PyResult<Py<PyAny>> {
        if self.wire_values {
            return format!("decimal128({precision},{scale}):{coefficient}").into_py_any(self.py);
        }
        // Decimal(string) is exact and independent of the caller's
        // decimal arithmetic context; no JSON/text data bridge is used.
        Ok(self
            .decimal
            .call1((format!("{coefficient}e{}", -i16::from(scale)),))?
            .unbind())
    }

    fn uuid_value(&self, bytes: &[u8]) -> PyResult<Py<PyAny>> {
        let arguments = PyDict::new(self.py);
        arguments.set_item("bytes", PyBytes::new(self.py, bytes))?;
        let value = self.uuid.call((), Some(&arguments))?;
        if self.wire_values {
            Ok(value.str()?.into_any().unbind())
        } else {
            Ok(value.unbind())
        }
    }

    fn binary(&self, field: &Field, bytes: &[u8]) -> PyResult<Py<PyAny>> {
        let py = self.py;
        if super::binary_object::field_is_object(field) {
            let object = self.owner.resolve(bytes, self.cancellation)?;
            if self.wire_values {
                let metadata = PyDict::new(py);
                metadata.set_item("type", "binary_object")?;
                metadata.set_item(
                    "ownership",
                    if object.value.is_managed() {
                        "managed"
                    } else {
                        "external"
                    },
                )?;
                metadata.set_item("byte_length", object.value.len())?;
                return Ok(metadata.into_any().unbind());
            }
            return Ok(Py::new(py, object)?.into_any());
        }
        if self.wire_values {
            hexadecimal(bytes).into_py_any(py)
        } else {
            Ok(PyBytes::new(py, bytes).into_any().unbind())
        }
    }

    fn list(&self, field: &Field, array: &dyn Array) -> PyResult<Py<PyAny>> {
        let result = PyList::empty(self.py);
        for index in 0..array.len() {
            if index % 256 == 0 {
                self.cancellation.check().map_err(super::native_error)?;
                self.py.check_signals()?;
            }
            result.append(self.value(field, array, index)?)?;
        }
        Ok(result.into_any().unbind())
    }

    fn temporal(&self, value: i64, date: bool) -> PyResult<Py<PyAny>> {
        use pyo3::IntoPyObjectExt as _;
        if !self.temporal_objects || self.wire_values {
            return value.into_py_any(self.py);
        }
        let converted = || {
            let base = self
                .datetime
                .getattr(if date { "date" } else { "datetime" })?
                .call1((1970, 1, 1))?;
            let arguments = PyDict::new(self.py);
            arguments.set_item(if date { "days" } else { "microseconds" }, value)?;
            let delta = self
                .datetime
                .getattr("timedelta")?
                .call((), Some(&arguments))?;
            base.call_method1("__add__", (delta,)).map(Bound::unbind)
        };
        match converted() {
            Ok(value) => Ok(value),
            // Match the existing exact public temporal contract: Python's
            // narrower calendar never truncates full-domain epoch storage.
            Err(error) if error.is_instance_of::<PyOverflowError>(self.py) => {
                value.into_py_any(self.py)
            }
            Err(error) => Err(error),
        }
    }
}

fn hexadecimal(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|value| {
            [
                char::from(DIGITS[usize::from(value >> 4)]),
                char::from(DIGITS[usize::from(value & 15)]),
            ]
        })
        .collect()
}

fn downcast<T: 'static>(array: &dyn Array) -> PyResult<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        PyValueError::new_err("native Arrow representation disagrees with its declared type")
    })
}
