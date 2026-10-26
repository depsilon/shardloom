//! One native operation with bounded columnar delivery and explicit demand.

use super::{capsules::observation_dict, native_error, plan::PlanCore, session::failed};
use arrow_array::{RecordBatch, RecordBatchIterator, ffi_stream::FFI_ArrowArrayStream};
use pyo3::{
    exceptions::{PyRuntimeError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyCapsule, PyDict, PyInt, PyList},
};
use shardloom_core::{ExecutionResources, Result};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_runtime::native::ExecutedArrowNativeOperation;
use shardloom_vortex::local_primitives::prepared_relational::OutputCreditWindow;
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
    Batch { ordinal: usize, batch: RecordBatch },
    Finished(Box<PyResult<Completed>>),
}

struct Completed {
    operation: ExecutedArrowNativeOperation,
    reused: bool,
    output_window_policy: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Demand {
    Start,
    Acknowledge(usize),
}

struct Running {
    demand: Sender<Demand>,
    events: Option<Receiver<Event>>,
    worker: Option<JoinHandle<()>>,
}

#[cfg(feature = "output-window-observation")]
#[derive(Default)]
struct StreamObservation {
    report: Mutex<Option<String>>,
    // Bounded caller diagnostics, outside native memory accounting just like
    // Capture. Tests synchronize on actual sends instead of sleeping and
    // assuming that the native producer has reached a full window.
    sent_batches: std::sync::atomic::AtomicUsize,
}

struct State {
    plan: Arc<PlanCore>,
    resources: ExecutionResources,
    cancellation: CancellationToken,
    timeout: Option<Duration>,
    running: Option<Running>,
    completed: Option<Completed>,
    closed: bool,
    next_ordinal: usize,
    output_window: bool,
    #[cfg(feature = "output-window-observation")]
    observation: Arc<StreamObservation>,
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
    pub(super) batch: RecordBatch,
}

impl NativeExecutionStream {
    pub(super) fn new(
        plan: Arc<PlanCore>,
        resources: ExecutionResources,
        parent: &CancellationToken,
        timeout: Option<Duration>,
        output_window: bool,
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
                next_ordinal: 0,
                output_window,
                #[cfg(feature = "output-window-observation")]
                observation: Arc::new(StreamObservation::default()),
            }),
            cancellation,
        }
    }
}

impl State {
    #[allow(clippy::too_many_lines)] // One worker owns policy, delivery, cancellation and final evidence.
    fn start(&mut self) -> Result<()> {
        let output_window = self.output_window;
        #[cfg(feature = "output-window-observation")]
        let observation_mode = match std::env::var("SHARDLOOM_OUTPUT_WINDOW_OBSERVATION") {
            Ok(mode) if mode == "clock" => Some(false),
            Ok(mode) if mode == "trace" => Some(true),
            Err(std::env::VarError::NotPresent) => None,
            _ => return Err(failed("output-window observation requires clock or trace")),
        };
        #[cfg(feature = "output-window-observation")]
        let observation = Arc::clone(&self.observation);
        // Declaration is lazy. The deadline starts at the first demand and
        // remains active across subsequent producer and consumer waits.
        self.cancellation =
            super::session::operation_cancellation(self.cancellation.clone(), self.timeout)?;
        let (demand, requested) = mpsc::channel();
        // Keep one queued event. The optional policy separately bounds all
        // sent, unacknowledged batches, including the one held by the consumer.
        let (outgoing, events) = mpsc::sync_channel(1);
        let plan = Arc::clone(&self.plan);
        let resources = self.resources;
        let cancellation = self.cancellation.clone();
        let worker = std::thread::Builder::new()
            .name("shardloom-python-stream".into())
            .spawn(move || {
                let outcome = demand_next(&requested, &cancellation, Demand::Start)
                    .map_err(native_error)
                    .and_then(|()| {
                        let controller = plan
                            .controller(resources, &cancellation)
                            .map_err(native_error)?;
                        plan.with_prepared(resources, &cancellation, |operation, reused, input| {
                            #[cfg(feature = "output-window-observation")]
                            let capture = observation_mode.map(|detailed| {
                                shardloom_vortex::output_window_observation::Capture::begin_window(
                                    detailed, output_window,
                                )
                            }).transpose()?;
                            let window = std::cell::RefCell::new(None::<OutputCreditWindow>);
                            let initialize = |admission: Option<OutputCreditWindow>| {
                                #[cfg(feature = "output-window-observation")]
                                shardloom_vortex::output_window_observation::window_policy(
                                    admission.as_ref().map(OutputCreditWindow::expected_rows),
                                    admission.as_ref().map_or(0, OutputCreditWindow::metadata_bytes),
                                );
                                *window.borrow_mut() = admission;
                                Ok(())
                            };
                            let mut ordinal = 0;
                            let consume = |batch: RecordBatch| {
                                #[cfg(feature = "output-window-observation")]
                                shardloom_vortex::output_window_observation::converted(
                                    batch.num_rows(),
                                    batch.get_array_memory_size(),
                                );
                                if let Some(window) = window.borrow_mut().as_mut()
                                    && window.enqueue(batch.num_rows(), batch.get_array_memory_size())? != ordinal {
                                    return Err(failed("output window batch ordinal differs from producer"));
                                }
                                controller.with_consumer_wait(|| {
                                    cancellation.check()?;
                                    outgoing
                                        .send(Event::Batch { ordinal, batch })
                                        .map_err(|_| failed("columnar consumer closed"))?;
                                    #[cfg(feature = "output-window-observation")]
                                    shardloom_vortex::output_window_observation::sent();
                                    #[cfg(feature = "output-window-observation")]
                                    if observation_mode.is_some() {
                                        observation.sent_batches.store(
                                            ordinal + 1,
                                            std::sync::atomic::Ordering::Release,
                                        );
                                    }
                                    if let Some(window) = window.borrow_mut().as_mut() {
                                        while window.requires_acknowledgment() {
                                            let expected = window.oldest_ordinal()
                                                .ok_or_else(|| failed("output window barrier lacks an ordinal"))?;
                                            demand_next(&requested, &cancellation, Demand::Acknowledge(expected))?;
                                            window.acknowledge(expected)?;
                                            #[cfg(feature = "output-window-observation")]
                                            shardloom_vortex::output_window_observation::acknowledged_ordinal(expected);
                                        }
                                    } else {
                                        demand_next(&requested, &cancellation, Demand::Acknowledge(ordinal))?;
                                        #[cfg(feature = "output-window-observation")]
                                        shardloom_vortex::output_window_observation::acknowledged_ordinal(ordinal);
                                    }
                                    Ok(())
                                })?;
                                ordinal = ordinal.checked_add(1)
                                    .ok_or_else(|| failed("columnar batch ordinal overflowed"))?;
                                Ok(())
                            };
                            let executed = if let Some(input) = input {
                                let mut next = |uri: &shardloom_core::DatasetUri, ordinal| {
                                    input.next(uri, ordinal)
                                };
                                operation.for_each_arrow_batch_with_output_window(
                                    &cancellation,
                                    Some(shardloom_runtime::native::NativeArrowInput::Sources(&mut next)),
                                    || output_window && plan.output_window_source_allowed(), initialize, consume,
                                )
                            } else {
                                operation.for_each_arrow_batch_with_output_window(
                                    &cancellation, None,
                                    || output_window && plan.output_window_source_allowed(), initialize, consume,
                                )
                            };
                            let window_active = window.borrow().is_some();
                            let executed = {
                                let executed = executed.and_then(|execution| {
                                    if let Some(window) = window.borrow().as_ref() { window.finish()?; }
                                    Ok(execution)
                                });
                                drop(window.into_inner());
                                #[cfg(feature = "output-window-observation")]
                                shardloom_vortex::output_window_observation::window_released();
                                executed
                            };
                            #[cfg(feature = "output-window-observation")]
                            if let Some(capture) = capture {
                                *observation
                                    .report
                                    .lock()
                                    .map_err(|_| failed("output-window observation poisoned"))? =
                                    Some(capture.finish(executed.is_ok()));
                            }
                            Ok(Completed {
                                operation: executed?,
                                reused,
                                output_window_policy: if window_active {
                                    "two_batch_window"
                                } else if output_window {
                                    "ordinary_ineligible"
                                } else {
                                    "ordinary_not_requested"
                                },
                            })
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
        let demand = self
            .next_ordinal
            .checked_sub(1)
            .map_or(Demand::Start, Demand::Acknowledge);
        let _ = running.demand.send(demand);
        loop {
            let outcome = running
                .events
                .as_ref()
                .expect("open event receiver")
                .recv_timeout(Duration::from_millis(25));
            match outcome {
                Ok(Event::Batch { ordinal, batch }) => {
                    let checked = if ordinal != self.next_ordinal {
                        Err(failed(
                            "columnar batch ordinal is duplicate or out of order",
                        ))
                    } else if self.output_window {
                        self.cancellation.check()
                    } else {
                        Ok(())
                    };
                    if let Err(error) = checked {
                        drop(batch);
                        let _ = self.stop();
                        return Err(native_error(error));
                    }
                    self.next_ordinal = self
                        .next_ordinal
                        .checked_add(1)
                        .ok_or_else(|| native_error(failed("columnar batch ordinal overflowed")))?;
                    return Ok(Some(NativeColumnarBatch { batch }));
                }
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

fn demand_next(
    demand: &Receiver<Demand>,
    cancellation: &CancellationToken,
    expected: Demand,
) -> Result<()> {
    loop {
        cancellation.check()?;
        match demand.recv_timeout(Duration::from_millis(5)) {
            Ok(actual) if actual == expected => return cancellation.check(),
            Ok(_) => {
                return Err(failed(
                    "columnar acknowledgment is duplicate or out of order",
                ));
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return Err(failed("columnar consumer closed")),
        }
    }
}

pub(super) fn output_window_bytes(value: &Bound<'_, PyAny>) -> PyResult<u64> {
    if value.is_instance_of::<PyBool>() || !value.is_instance_of::<PyInt>() {
        return Err(PyTypeError::new_err(
            "output_window_bytes must be the integer 0 or 4194304",
        ));
    }
    let bytes = value.extract::<u64>().map_err(|_| {
        PyValueError::new_err("output_window_bytes must be the integer 0 or 4194304")
    })?;
    if bytes != 0 && bytes != 4_194_304 {
        return Err(PyValueError::new_err(
            "output_window_bytes must be 0 or 4194304",
        ));
    }
    Ok(bytes)
}

#[pymethods]
impl NativeExecutionStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Opt-in delivery diagnostics; never a successful query certificate.
    #[cfg(feature = "output-window-observation")]
    #[getter]
    fn output_window_observation(&self, py: Python<'_>) -> PyResult<Option<String>> {
        py.detach(|| {
            let state = self
                .state
                .lock()
                .map_err(|_| failed("native stream state poisoned"))?;
            let observation = state
                .observation
                .report
                .lock()
                .map_err(|_| failed("output-window observation poisoned"))?;
            Ok(observation.clone())
        })
        .map_err(native_error)
    }

    /// Opt-in synchronization evidence, never a completion certificate.
    #[cfg(feature = "output-window-observation")]
    #[getter]
    fn _output_window_sent_batches(&self, py: Python<'_>) -> PyResult<usize> {
        py.detach(|| {
            let state = self
                .state
                .lock()
                .map_err(|_| failed("native stream state poisoned"))?;
            Ok(state
                .observation
                .sent_batches
                .load(std::sync::atomic::Ordering::Acquire))
        })
        .map_err(native_error)
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
            state
                .completed
                .as_ref()
                .map(|completed| {
                    completed
                        .operation
                        .execution
                        .fields(completed.reused)
                        .map(|mut fields| {
                            fields.push((
                                "columnar_output_window_requested_bytes".into(),
                                if state.output_window { "4194304" } else { "0" }.into(),
                            ));
                            fields.push((
                                "columnar_output_window_selected_policy".into(),
                                completed.output_window_policy.into(),
                            ));
                            (fields, completed.operation.export)
                        })
                        .map_err(native_error)
                })
                .transpose()
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
        PyCapsule::new_with_value(
            py,
            FFI_ArrowArrayStream::new(Box::new(reader)),
            c"arrow_array_stream",
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
