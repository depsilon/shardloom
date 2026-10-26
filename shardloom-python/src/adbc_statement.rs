//! Named typed parameters and demand-driven native batches for ADBC statements.

use super::{Bridge, call0, invalid, python_error, unsupported, unsupported_option_reads};
use adbc_core::{
    Optionable, PartitionedResult, Statement,
    error::Result,
    options::{OptionStatement, OptionValue},
};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_pyarrow::{FromPyArrow, ToPyArrow};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use pyo3::prelude::*;
use std::sync::Arc;

pub(super) struct LocalStatement {
    pub(super) bridge: Bridge,
    query: Option<String>,
    prepared: Option<Py<PyAny>>,
    parameters: Option<RecordBatch>,
    stream: Option<Bridge>,
}

impl LocalStatement {
    pub(super) fn new(bridge: Bridge) -> Self {
        Self {
            bridge,
            query: None,
            prepared: None,
            parameters: None,
            stream: None,
        }
    }
    fn prepared(&mut self) -> Result<&Py<PyAny>> {
        if self.prepared.is_none() {
            self.prepare()?;
        }
        self.prepared
            .as_ref()
            .ok_or_else(|| invalid("ADBC statement has no prepared SQL"))
    }
    fn stop(&mut self) -> Result<()> {
        if let Some(stream) = self.stream.take() {
            call0(&stream, "close")?;
        }
        Ok(())
    }
    fn parameter_object(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.parameters
            .as_ref()
            .map(|batch| batch.to_pyarrow(py).map(Bound::unbind))
            .transpose()
            .map(|value| value.unwrap_or_else(|| py.None()))
    }
}

impl Optionable for LocalStatement {
    type Option = OptionStatement;
    fn set_option(&mut self, key: OptionStatement, value: OptionValue) -> Result<()> {
        if key.as_ref() == "adbc.statement.bind_by_name"
            && matches!(value, OptionValue::String(ref value) if value == "true")
        {
            return Ok(());
        }
        Err(unsupported(key.as_ref()))
    }
    fn get_option_string(&self, key: OptionStatement) -> Result<String> {
        if key.as_ref() == "shardloom.execution_report" {
            let stream = self
                .stream
                .as_ref()
                .ok_or_else(|| invalid("ADBC statement has no completed execution"))?;
            return Python::attach(|py| {
                let report = stream.bind(py).getattr("report")?;
                if report.is_none() {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "ADBC execution has not completed successfully",
                    ));
                }
                let raw = report.getattr("envelope")?.getattr("raw")?;
                py.import("json")?.call_method1("dumps", (raw,))?.extract()
            })
            .map_err(python_error);
        }
        if key.as_ref() == "adbc.statement.bind_by_name" {
            Ok("true".into())
        } else {
            Err(unsupported(key.as_ref()))
        }
    }
    unsupported_option_reads!(OptionStatement);
}

impl Statement for LocalStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.stop()?;
        self.parameters = None;
        if batch.num_rows() != 1 || batch.get_array_memory_size() > 1 << 20 {
            return Err(invalid(
                "ADBC scalar binding requires exactly one row and at most 1 MiB of parameter buffers",
            ));
        }
        self.prepared()?;
        Python::attach(|py| {
            self.prepared
                .as_ref()
                .expect("prepared above")
                .bind(py)
                .call_method1("validate_parameters", (batch.to_pyarrow(py)?,))
                .map(|_| ())
        })
        .map_err(python_error)?;
        self.parameters = Some(batch);
        Ok(())
    }
    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        Err(unsupported(
            "parameter streams or bulk executemany; bind one named scalar row",
        ))
    }
    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.stop()?;
        self.prepared()?;
        // A binding is consumed by one execution, including a failed one. An
        // independent DB-API cursor must never accidentally reuse stale values.
        let parameters = self.parameters.take();
        let (schema, stream) = Python::attach(|py| -> PyResult<_> {
            let parameters = parameters
                .as_ref()
                .map(|batch| batch.to_pyarrow(py).map(Bound::unbind))
                .transpose()?
                .unwrap_or_else(|| py.None());
            let pair = self
                .prepared
                .as_ref()
                .expect("prepared above")
                .bind(py)
                .call_method1("execute", (parameters,))?;
            let schema = Schema::from_pyarrow_bound(&pair.get_item(0)?)?;
            Ok((Arc::new(schema), Arc::new(pair.get_item(1)?.unbind())))
        })
        .map_err(python_error)?;
        self.stream = Some(Arc::clone(&stream));
        Ok(Box::new(NativeReader {
            schema,
            stream,
            finished: false,
        }))
    }
    fn execute_schema(&mut self) -> Result<Schema> {
        self.prepared()?;
        Python::attach(|py| {
            let schema = self
                .prepared
                .as_ref()
                .expect("prepared above")
                .bind(py)
                .call_method1("schema", (self.parameter_object(py)?,))?;
            Schema::from_pyarrow_bound(&schema)
        })
        .map_err(python_error)
    }
    fn execute_update(&mut self) -> Result<Option<i64>> {
        Err(unsupported("SQL writes; use the explicit DataFrame writer"))
    }
    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(unsupported("partitioned remote execution"))
    }
    fn get_parameter_schema(&self) -> Result<Schema> {
        if let Some(parameters) = &self.parameters {
            return Ok(parameters.schema().as_ref().clone());
        }
        let prepared = self.prepared.as_ref().ok_or_else(|| {
            invalid("prepare the statement before requesting its parameter schema")
        })?;
        Python::attach(|py| {
            Schema::from_pyarrow_bound(&prepared.bind(py).call_method0("parameter_schema")?)
        })
        .map_err(python_error)
    }
    fn prepare(&mut self) -> Result<()> {
        self.stop()?;
        let query = self
            .query
            .as_ref()
            .ok_or_else(|| invalid("set SQL before preparing an ADBC statement"))?;
        self.prepared = Some(
            Python::attach(|py| {
                self.bridge
                    .bind(py)
                    .call_method1("prepare", (query,))
                    .map(Bound::unbind)
            })
            .map_err(python_error)?,
        );
        Ok(())
    }
    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.stop()?;
        self.query = Some(query.as_ref().into());
        self.prepared = None;
        self.parameters = None;
        Ok(())
    }
    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(unsupported("Substrait plan execution"))
    }
    fn cancel(&mut self) -> Result<()> {
        if let Some(stream) = &self.stream {
            call0(stream, "cancel")?;
        }
        Ok(())
    }
}

impl Drop for LocalStatement {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

struct NativeReader {
    schema: SchemaRef,
    stream: Bridge,
    finished: bool,
}

impl Iterator for NativeReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let result = Python::attach(|py| -> PyResult<Option<RecordBatch>> {
            match self.stream.bind(py).call_method0("__next__") {
                Ok(batch) => {
                    let native = batch.getattr("native_result")?;
                    let native =
                        native.extract::<PyRef<'_, super::super::stream::NativeColumnarBatch>>()?;
                    Ok(Some(native.batch.clone()))
                }
                Err(error) if error.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) => {
                    Ok(None)
                }
                Err(error) => Err(error),
            }
        });
        match result {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(error) => {
                self.finished = true;
                let _ = call0(&self.stream, "close");
                Some(Err(ArrowError::ExternalError(Box::new(python_error(
                    error,
                )))))
            }
        }
    }
}

impl RecordBatchReader for NativeReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
impl Drop for NativeReader {
    fn drop(&mut self) {
        if !self.finished {
            let _ = call0(&self.stream, "close");
        }
    }
}
