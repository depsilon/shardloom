use super::native_error;
use arrow_array::{RecordBatch, RecordBatchReader, ffi_stream::FFI_ArrowArrayStream};
use arrow_schema::{ArrowError, SchemaRef};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyCapsule, PyDict},
};
use shardloom_core::ExecutionResources;
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_vortex::resident_session::{
    ConfigurableResidentSession, NativeArrowBatchStream, NativeArrowExportReport,
    OwnedVortexResultBatch,
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Progress {
    observation: NativeArrowExportReport,
    exported: bool,
    closed: bool,
    error: Option<String>,
}

/// One-shot columnar export with a separately retained observation owner.
#[pyclass(name = "ColumnarStream", module = "shardloom._native", frozen)]
pub(super) struct NativeColumnarStream {
    reader: Mutex<Option<ResultReader>>,
    progress: Arc<Mutex<Progress>>,
    cancellation: CancellationToken,
}

struct ResultReader {
    stream: NativeArrowBatchStream,
    controller: ConfigurableResidentSession,
    resources: ExecutionResources,
    cancellation: CancellationToken,
    progress: Arc<Mutex<Progress>>,
}

impl Iterator for ResultReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let outcome = Python::attach(|py| {
            py.detach(|| {
                self.controller
                    .with_resources(self.resources, &self.cancellation, |_, _| {
                        self.stream.next_batch()
                    })
                    .map_err(|error| ArrowError::ExternalError(Box::new(error)))
            })
        });
        {
            let mut progress = self
                .progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            progress.observation = self.stream.report();
            if let Err(error) = &outcome {
                progress.error = Some(error.to_string());
            }
        }
        outcome.transpose()
    }
}

impl Drop for ResultReader {
    fn drop(&mut self) {
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
    }
}

#[pymethods]
impl NativeColumnarStream {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        validate_request(requested_schema)?;
        let reader = self.reader.lock().map_err(|_| PyValueError::new_err("columnar stream ownership poisoned"))?
            .take().ok_or_else(|| PyValueError::new_err("columnar stream already transferred or closed; request a new export from the retained result"))?;
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .exported = true;
        // The provider owns release callbacks and retained native buffer credits.
        PyCapsule::new(
            py,
            FFI_ArrowArrayStream::new(Box::new(reader)),
            Some(c"arrow_array_stream".into()),
        )
    }

    #[getter]
    fn report(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let progress = self
            .progress
            .lock()
            .map_err(|_| PyValueError::new_err("columnar report ownership poisoned"))?;
        let report = observation_dict(py, progress.observation)?;
        report.set_item("exported", progress.exported)?;
        report.set_item("closed", progress.closed)?;
        report.set_item("error", progress.error.as_deref())?;
        report.set_item("python_rows_constructed", false)?;
        Ok(report.unbind())
    }

    /// Stop future demand. Batches already held by a consumer remain valid.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.cancellation.cancel();
        py.detach(|| {
            self.reader
                .lock()
                .map_err(|_| PyValueError::new_err("columnar stream ownership poisoned"))?
                .take();
            self.progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .closed = true;
            Ok(())
        })
    }
}

pub(super) fn observation_dict(
    py: Python<'_>,
    observed: NativeArrowExportReport,
) -> PyResult<Bound<'_, PyDict>> {
    let report = PyDict::new(py);
    report.set_item("output_rows", observed.output_rows)?;
    report.set_item("output_batches", observed.output_batches)?;
    report.set_item(
        "referenced_arrow_buffer_bytes",
        observed.referenced_arrow_buffer_bytes,
    )?;
    report.set_item(
        "shared_native_buffer_bytes",
        observed.shared_native_buffer_bytes,
    )?;
    report.set_item(
        "converted_buffer_capacity_bytes",
        observed.converted_buffer_capacity_bytes,
    )?;
    report.set_item(
        "peak_conversion_work_reserved_bytes",
        observed.peak_conversion_work_reserved_bytes,
    )?;
    report.set_item(
        "provider_canonicalization_calls",
        observed.provider_canonicalization_calls,
    )?;
    report.set_item("complete", observed.complete)?;
    report.set_item("fallback_attempted", false)?;
    report.set_item("buffer_byte_scope", "referenced_regions;aliases_may_repeat;conversion_capacity_is_not_measured_decode_or_process_rss")?;
    Ok(report)
}

fn validate_request(requested_schema: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
    if requested_schema.is_some_and(|value| !value.is_none()) {
        return Err(PyValueError::new_err(
            "requested-schema casting is not admitted at the Arrow boundary; cast in the native plan",
        ));
    }
    Ok(())
}

impl RecordBatchReader for ResultReader {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

pub(super) fn result_stream<'py>(
    py: Python<'py>,
    source: &Arc<OwnedVortexResultBatch>,
    controller: ConfigurableResidentSession,
    resources: ExecutionResources,
    requested_schema: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyCapsule>> {
    validate_request(requested_schema)?;
    columnar_stream(py, source, controller, resources)?.__arrow_c_stream__(py, None)
}

pub(super) fn columnar_stream(
    py: Python<'_>,
    source: &Arc<OwnedVortexResultBatch>,
    controller: ConfigurableResidentSession,
    resources: ExecutionResources,
) -> PyResult<NativeColumnarStream> {
    let cancellation = CancellationToken::default();
    let stream = py
        .detach(|| {
            controller.with_resources(resources, &cancellation, |_, _| {
                source.arrow_batches(cancellation.clone())
            })
        })
        .map_err(native_error)?;
    let progress = Arc::new(Mutex::new(Progress::default()));
    let reader = ResultReader {
        stream,
        controller,
        resources,
        cancellation: cancellation.clone(),
        progress: Arc::clone(&progress),
    };
    Ok(NativeColumnarStream {
        reader: Mutex::new(Some(reader)),
        progress,
        cancellation,
    })
}
