//! One native operation with bounded columnar delivery and explicit demand.

use super::{capsules::observation_dict, native_error, plan::PlanCore, session::failed};
use arrow_array::{RecordBatch, RecordBatchIterator, ffi_stream::FFI_ArrowArrayStream};
use pyo3::{
    exceptions::PyRuntimeError,
    prelude::*,
    types::{PyCapsule, PyDict, PyList},
};
use shardloom_core::{ExecutionResources, Result};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_runtime::native;
use shardloom_vortex::local_primitives::prepared_relational::ExecutedVortexArrowRelational;
use std::{
    sync::{
        Arc, Mutex,
        atomic::AtomicBool,
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
    },
    thread::JoinHandle,
    time::Duration,
};

enum Event {
    Batch(RecordBatch),
    Finished(Box<PyResult<(ExecutedVortexArrowRelational, bool)>>),
}

struct Running {
    demand: Sender<()>,
    events: Option<Receiver<Event>>,
    worker: Option<JoinHandle<()>>,
}

struct State {
    plan: Arc<PlanCore>,
    resources: ExecutionResources,
    cancellation: CancellationToken,
    timeout: Option<Duration>,
    running: Option<Running>,
    completed: Option<(ExecutedVortexArrowRelational, bool)>,
    closed: bool,
}

#[pyclass(name = "ExecutionStream", module = "shardloom._native", frozen)]
pub(super) struct NativeExecutionStream {
    state: Mutex<State>,
    cancellation: CancellationToken,
}

/// A provisional columnar batch. Its retained native buffers remain valid after
/// stream close; the stream's final report determines transaction completion.
#[pyclass(name = "ColumnarBatch", module = "shardloom._native", frozen)]
pub(super) struct NativeColumnarBatch {
    batch: RecordBatch,
}

impl NativeExecutionStream {
    pub(super) fn new(
        plan: Arc<PlanCore>,
        resources: ExecutionResources,
        parent: &CancellationToken,
        timeout: Option<Duration>,
    ) -> Self {
        let cancellation = CancellationToken::from_shared_flag_with_parent(
            Arc::new(AtomicBool::new(false)),
            parent,
        );
        Self {
            state: Mutex::new(State {
                plan,
                resources,
                cancellation: cancellation.clone(),
                timeout,
                running: None,
                completed: None,
                closed: false,
            }),
            cancellation,
        }
    }
}

impl State {
    fn start(&mut self) -> Result<()> {
        // Declaration is lazy. The deadline starts at the first demand and
        // remains active across subsequent producer and consumer waits.
        self.cancellation =
            super::session::operation_cancellation(self.cancellation.clone(), self.timeout)?;
        let (demand, requested) = mpsc::channel();
        // At most one converted batch is queued; the producer then waits for
        // the consumer to acknowledge it by requesting its successor.
        let (outgoing, events) = mpsc::sync_channel(1);
        let plan = Arc::clone(&self.plan);
        let resources = self.resources;
        let cancellation = self.cancellation.clone();
        let worker = std::thread::Builder::new()
            .name("shardloom-python-stream".into())
            .spawn(move || {
                let outcome = demand_next(&requested, &cancellation)
                    .map_err(native_error)
                    .and_then(|()| {
                        let controller = plan
                            .controller(resources, &cancellation)
                            .map_err(native_error)?;
                        plan.with_prepared(resources, &cancellation, |operation, reused, input| {
                            let consume = |batch| {
                                controller.with_consumer_wait(|| {
                                    outgoing
                                        .send(Event::Batch(batch))
                                        .map_err(|_| failed("columnar consumer closed"))?;
                                    demand_next(&requested, &cancellation)
                                })
                            };
                            let executed = if let Some(input) = input {
                                let mut next = || input.next();
                                operation
                                    .with_arrow_batch_input(&mut next)?
                                    .for_each_arrow_batch(&cancellation, consume)?
                            } else {
                                operation.for_each_arrow_batch(&cancellation, consume)?
                            };
                            Ok((executed, reused))
                        })
                    });
                // A closed receiver deliberately discards provisional work and
                // does not turn a cancelled transaction into a success report.
                let _ = outgoing.send(Event::Finished(Box::new(outcome)));
            })
            .map_err(|error| failed(&format!("cannot start native stream: {error}")))?;
        self.running = Some(Running {
            demand,
            events: Some(events),
            worker: Some(worker),
        });
        Ok(())
    }

    fn next(&mut self) -> PyResult<Option<NativeColumnarBatch>> {
        if self.closed || self.completed.is_some() {
            return Ok(None);
        }
        if self.running.is_none() {
            self.start().map_err(native_error)?;
        }
        let running = self.running.as_ref().expect("started native stream");
        // If the worker already failed before first demand, its terminal event
        // remains authoritative even when the command receiver has dropped.
        let _ = running.demand.send(());
        loop {
            let outcome = running
                .events
                .as_ref()
                .expect("open event receiver")
                .recv_timeout(Duration::from_millis(25));
            match outcome {
                Ok(Event::Batch(batch)) => return Ok(Some(NativeColumnarBatch { batch })),
                Ok(Event::Finished(outcome)) => {
                    self.closed = true;
                    self.join().map_err(native_error)?;
                    match *outcome {
                        Ok(completed) => {
                            self.completed = Some(completed);
                            return Ok(None);
                        }
                        Err(error) => {
                            return Err(error);
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    if let Err(error) = check_python_signals() {
                        let _ = self.stop();
                        return Err(error);
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    self.closed = true;
                    self.join().map_err(native_error)?;
                    return Err(PyRuntimeError::new_err(
                        "native stream ended without completion evidence",
                    ));
                }
            }
        }
    }

    fn join(&mut self) -> Result<()> {
        if let Some(mut running) = self.running.take() {
            // Drop the receiver before joining so a failed/closed consumer can
            // never strand a producer in its bounded send.
            drop(running.events.take());
            if let Some(worker) = running.worker.take() {
                worker
                    .join()
                    .map_err(|_| failed("native stream worker panicked"))?;
            }
        }
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        self.cancellation.cancel();
        self.closed = true;
        self.join()
    }
}

// The method pointer does not satisfy attach's higher-ranked Python lifetime.
#[allow(clippy::redundant_closure_for_method_calls)]
fn check_python_signals() -> PyResult<()> {
    Python::attach(|py| py.check_signals())
}

fn demand_next(demand: &Receiver<()>, cancellation: &CancellationToken) -> Result<()> {
    loop {
        cancellation.check()?;
        match demand.recv_timeout(Duration::from_millis(5)) {
            Ok(()) => return cancellation.check(),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return Err(failed("columnar consumer closed")),
        }
    }
}

#[pymethods]
impl NativeExecutionStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<NativeColumnarBatch>> {
        py.detach(|| {
            self.state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("native stream state poisoned"))?
                .next()
        })
    }

    #[getter]
    fn report(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let fields = py.detach(|| {
            let state = self
                .state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("native stream state poisoned"))?;
            Ok::<_, PyErr>(state.completed.as_ref().map(|(completed, reused)| {
                (
                    native::execution_fields(&completed.execution, *reused),
                    completed.export,
                )
            }))
        })?;
        let Some((fields, export)) = fields else {
            return Ok(None);
        };
        let report = super::result::fields_dict(py, fields)?;
        let observation = observation_dict(py, export)?;
        observation.set_item("python_rows_constructed", false)?;
        report.bind(py).set_item("columnar_export", observation)?;
        Ok(Some(report))
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        // Cancellation is independent of the state mutex held by a blocking
        // next(), so a second Python thread can interrupt and drain that call.
        self.cancellation.cancel();
        py.detach(|| {
            self.state
                .lock()
                .map_err(|_| failed("native stream state poisoned"))?
                .stop()
        })
        .map_err(native_error)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _kind: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.close(py)
    }
}

impl Drop for NativeExecutionStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // The worker currently uses only native/file sources. Detach while
        // draining so unrelated Python work and future producer callbacks run.
        if Python::try_attach(|py| {
            py.detach(|| {
                let _ = state.stop();
            });
        })
        .is_none()
        {
            let _ = state.stop();
        }
    }
}

#[pymethods]
impl NativeColumnarBatch {
    #[getter]
    fn row_count(&self) -> usize {
        self.batch.num_rows()
    }

    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        super::schema::fields(py, self.batch.schema().fields())
    }

    fn slice(&self, offset: usize, length: usize) -> PyResult<Self> {
        if offset
            .checked_add(length)
            .is_none_or(|end| end > self.batch.num_rows())
        {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "native batch slice exceeds its row count",
            ));
        }
        Ok(Self {
            batch: self.batch.slice(offset, length),
        })
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        if requested_schema.is_some_and(|value| !value.is_none()) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "requested-schema casting is not admitted at the native batch boundary",
            ));
        }
        let reader = RecordBatchIterator::new(vec![Ok(self.batch.clone())], self.batch.schema());
        PyCapsule::new(
            py,
            FFI_ArrowArrayStream::new(Box::new(reader)),
            Some(c"arrow_array_stream".into()),
        )
    }

    #[pyo3(signature = (*, temporal_objects=true, wire_values=false))]
    fn row_export(
        &self,
        py: Python<'_>,
        temporal_objects: bool,
        wire_values: bool,
    ) -> PyResult<Py<PyList>> {
        let rows = PyList::empty(py);
        super::rows::append_batch(
            py,
            &rows,
            &self.batch,
            temporal_objects,
            wire_values,
            &CancellationToken::default(),
        )?;
        Ok(rows.unbind())
    }
}
