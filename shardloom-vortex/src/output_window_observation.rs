//! Temporary, opt-in attribution of the unchanged native Python demand path.
//! Storage is bounded experiment infrastructure, not native allocation evidence.

use crate::local_primitives::query_timing::QueryTiming;
use shardloom_core::{Result, ShardLoomError};
use std::{cell::RefCell, rc::Rc, time::Instant};

const MAX_BATCHES: usize = 128;

thread_local! {
    static ACTIVE: RefCell<Option<Rc<RefCell<Data>>>> = const { RefCell::new(None) };
}

#[derive(Clone, serde::Serialize)]
struct Batch {
    rows: usize,
    callback_enter_ns: u64,
    converted_ready_ns: Option<u64>,
    send_complete_ns: Option<u64>,
    acknowledged_ns: Option<u64>,
    callback_exit_ns: Option<u64>,
    referenced_arrow_bytes: Option<usize>,
    callback_succeeded: bool,
}

#[derive(serde::Serialize)]
struct Window {
    requested: bool,
    initialized: bool,
    selected_policy: &'static str,
    eligible: Option<bool>,
    expected_rows: Option<usize>,
    metadata_reserved_bytes: u64,
    metadata_final_bytes: u64,
    released: bool,
    outstanding_batches: usize,
    outstanding_referenced_bytes: usize,
    maximum_unacknowledged_batches: usize,
    maximum_unacknowledged_referenced_bytes: usize,
    abandoned_batches: usize,
    abandoned_referenced_bytes: usize,
    #[serde(skip)]
    pending_bytes: [usize; 2],
    #[serde(skip)]
    head: usize,
}

impl Window {
    fn new(requested: bool) -> Self {
        Self {
            requested,
            initialized: false,
            selected_policy: "uninitialized",
            eligible: None,
            expected_rows: None,
            metadata_reserved_bytes: 0,
            metadata_final_bytes: 0,
            released: false,
            outstanding_batches: 0,
            outstanding_referenced_bytes: 0,
            maximum_unacknowledged_batches: 0,
            maximum_unacknowledged_referenced_bytes: 0,
            abandoned_batches: 0,
            abandoned_referenced_bytes: 0,
            pending_bytes: [0; 2],
            head: 0,
        }
    }

    fn active(&self) -> bool {
        self.selected_policy == "two_batch_window"
    }

    fn sent(&mut self, bytes: usize) -> bool {
        let limit = if self.active() { 2 } else { 1 };
        let Some(total) = self.outstanding_referenced_bytes.checked_add(bytes) else {
            return false;
        };
        if !self.initialized
            || self.released
            || self.outstanding_batches >= limit
            || (self.active() && (bytes > 2 << 20 || total > 4 << 20))
        {
            return false;
        }
        self.pending_bytes[(self.head + self.outstanding_batches) % 2] = bytes;
        self.outstanding_batches += 1;
        self.outstanding_referenced_bytes = total;
        self.maximum_unacknowledged_batches = self
            .maximum_unacknowledged_batches
            .max(self.outstanding_batches);
        self.maximum_unacknowledged_referenced_bytes =
            self.maximum_unacknowledged_referenced_bytes.max(total);
        true
    }
}

#[allow(clippy::struct_excessive_bools)] // Independent capture/scope/success facts must survive failed transitions.
struct Data {
    epoch: Instant,
    detailed: bool,
    error: Option<&'static str>,
    engine_calls: usize,
    engine_open: bool,
    engine_start: Option<u64>,
    engine_end: Option<u64>,
    engine_succeeded: bool,
    sink_open: bool,
    stage: u8,
    current_rows: usize,
    current_bytes: usize,
    batch_count: usize,
    output_rows: usize,
    ready_count: usize,
    sent_count: usize,
    acknowledged_count: usize,
    sink_ns: u64,
    max_referenced_bytes: usize,
    batches: Vec<Batch>,
    window: Option<Window>,
}

impl Data {
    fn stamp(&self, now: Instant) -> u64 {
        u64::try_from(now.duration_since(self.epoch).as_nanos())
            .expect("bounded attribution duration fits nanoseconds")
    }

    fn invalidate(&mut self, reason: &'static str) {
        self.error.get_or_insert(reason);
    }
}

/// One worker-local diagnostic capture. It deliberately cannot cross threads.
pub struct Capture {
    data: Rc<RefCell<Data>>,
}

impl Capture {
    /// Start one bounded capture after preparation, before native execution.
    /// # Errors
    /// Refuses nested capture instead of mixing independent operation evidence.
    pub fn begin(detailed: bool) -> Result<Self> {
        Self::begin_capture(detailed, None)
    }

    /// Observe the separate frozen candidate with actual indexed acknowledgments.
    /// # Errors
    /// Refuses nested observation just as the attribution capture does.
    pub fn begin_window(detailed: bool, requested: bool) -> Result<Self> {
        Self::begin_capture(detailed, Some(requested))
    }

    fn begin_capture(detailed: bool, window: Option<bool>) -> Result<Self> {
        ACTIVE.with(|active| {
            if active.borrow().is_some() {
                return Err(ShardLoomError::InvalidOperation(
                    "output-window attribution cannot nest captures; no fallback execution was attempted".into(),
                ));
            }
            let data = Rc::new(RefCell::new(Data {
                epoch: Instant::now(), detailed, error: None, engine_calls: 0,
                engine_open: false, engine_start: None, engine_end: None,
                engine_succeeded: false, sink_open: false, stage: 0, current_rows: 0, current_bytes: 0,
                batch_count: 0, output_rows: 0, ready_count: 0, sent_count: 0,
                acknowledged_count: 0, sink_ns: 0, max_referenced_bytes: 0,
                batches: Vec::with_capacity(if detailed { MAX_BATCHES } else { 0 }),
                window: window.map(Window::new),
            }));
            *active.borrow_mut() = Some(Rc::clone(&data));
            Ok(Self { data })
        })
    }

    /// Finish diagnostics separately from the public query completion report.
    #[must_use]
    pub fn finish(self, operation_succeeded: bool) -> String {
        let mut data = self.data.borrow_mut();
        if data.engine_calls != 1 || data.engine_open || data.sink_open {
            data.invalidate("native execution scopes are missing, nested or still open");
        }
        let elapsed = data
            .engine_end
            .zip(data.engine_start)
            .and_then(|(end, start)| end.checked_sub(start));
        if elapsed.is_none_or(|elapsed| data.sink_ns > elapsed) {
            data.invalidate("native sink spans exceed or lack the complete call");
        }
        if operation_succeeded
            && (!data.engine_succeeded
                || data.ready_count != data.batch_count
                || data.sent_count != data.batch_count
                || data.acknowledged_count != data.batch_count)
        {
            data.invalidate("successful native call lacks complete demand evidence");
        }
        if data.window.as_ref().is_some_and(|window| {
            !window.released
                || window.metadata_final_bytes != 0
                || window.outstanding_batches != 0
                || window.outstanding_referenced_bytes != 0
                || (operation_succeeded && (!window.initialized || window.abandoned_batches != 0))
        }) {
            data.invalidate(
                "output window lacks release or successful final acknowledgment evidence",
            );
        }
        let timing = elapsed
            .filter(|elapsed| data.sink_ns <= *elapsed)
            .map(|elapsed| {
                QueryTiming::from_elapsed(
                    std::time::Duration::from_nanos(elapsed),
                    std::time::Duration::from_nanos(data.sink_ns),
                )
            });
        let mut report = serde_json::json!({
            "schema_version":"shardloom.output_window_observation.v1",
            "mode":if data.detailed {"trace"} else {"clock"},
            "valid":data.error.is_none(), "error":data.error,
            "operation_succeeded":operation_succeeded,
            "engine_succeeded":data.engine_succeeded,
            "engine_calls":data.engine_calls,
            "engine_start_ns":data.engine_start, "engine_end_ns":data.engine_end,
            "native_call_elapsed_ns":elapsed,
            "engine_execution_ns":timing.map(|t|t.execution.as_nanos()),
            "synchronous_sink_ns":data.sink_ns,
            "batch_count":data.batch_count, "output_rows":data.output_rows,
            "ready_count":data.ready_count,"sent_count":data.sent_count,
            "acknowledged_count":data.acknowledged_count,
            "maximum_batch_referenced_arrow_bytes":data.max_referenced_bytes,
            "maximum_observation_batches":MAX_BATCHES,
            "batches":data.batches,
            "storage_scope":"bounded_caller_diagnostics_outside_native_grant_not_rss",
            "clock_boundary":"prepared_native_call_excluding_synchronous_conversion_and_consumer_wait",
        });
        if let Some(window) = &data.window {
            report["schema_version"] = "shardloom.output_window_observation.v2".into();
            report["window"] = serde_json::json!(window);
        }
        report.to_string()
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        ACTIVE.with(|active| {
            if active
                .borrow()
                .as_ref()
                .is_some_and(|data| Rc::ptr_eq(data, &self.data))
            {
                active.borrow_mut().take();
            }
        });
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Engine,
    Sink,
}

/// An elapsed span that records failure on early return or unwind.
pub struct Scope {
    data: Option<Rc<RefCell<Data>>>,
    kind: Kind,
    started: Option<Instant>,
    record: Option<usize>,
    succeeded: bool,
}

impl Scope {
    /// Mark successful completion immediately before the normal return.
    pub fn complete(&mut self) {
        self.succeeded = true;
    }
}

/// Observe the ordinary complete prepared native batch call.
#[must_use]
pub fn engine_scope() -> Scope {
    scope(Kind::Engine, 0)
}

/// Observe one complete synchronous conversion/consumer callback.
#[must_use]
pub fn sink_scope(rows: usize) -> Scope {
    scope(Kind::Sink, rows)
}

fn scope(kind: Kind, rows: usize) -> Scope {
    let active = ACTIVE.with(|active| active.borrow().clone());
    let mut result = Scope {
        data: None,
        kind,
        started: None,
        record: None,
        succeeded: false,
    };
    if let Some(active) = active {
        let now = Instant::now();
        let mut data = active.borrow_mut();
        let stamp = data.stamp(now);
        match kind {
            Kind::Engine => {
                data.engine_calls += 1;
                if data.engine_open || data.engine_calls != 1 {
                    data.invalidate("native engine scopes overlap or repeat");
                    return result;
                }
                data.engine_open = true;
                data.engine_start = Some(stamp);
            }
            Kind::Sink => {
                if !data.engine_open || data.sink_open {
                    data.invalidate("native callback is outside or overlaps its engine scope");
                    return result;
                }
                data.sink_open = true;
                data.stage = 0;
                data.current_rows = rows;
                data.batch_count += 1;
                data.output_rows += rows;
                if data.batch_count > MAX_BATCHES {
                    data.invalidate("output-window observation batch bound exceeded");
                } else if data.detailed {
                    result.record = Some(data.batches.len());
                    data.batches.push(Batch {
                        rows,
                        callback_enter_ns: stamp,
                        converted_ready_ns: None,
                        send_complete_ns: None,
                        acknowledged_ns: None,
                        callback_exit_ns: None,
                        referenced_arrow_bytes: None,
                        callback_succeeded: false,
                    });
                }
            }
        }
        drop(data);
        result.data = Some(active);
        result.started = Some(now);
    }
    result
}

impl Drop for Scope {
    fn drop(&mut self) {
        let (Some(active), Some(started)) = (&self.data, self.started) else {
            return;
        };
        let now = Instant::now();
        let mut data = active.borrow_mut();
        let stamp = data.stamp(now);
        match self.kind {
            Kind::Engine => {
                data.engine_open = false;
                data.engine_end = Some(stamp);
                data.engine_succeeded = self.succeeded;
            }
            Kind::Sink => {
                data.sink_open = false;
                data.sink_ns += u64::try_from(now.duration_since(started).as_nanos())
                    .expect("bounded callback duration fits nanoseconds");
                let final_stage = if data.window.as_ref().is_some_and(Window::active) {
                    2
                } else {
                    3
                };
                if self.succeeded && data.stage != final_stage {
                    data.invalidate("successful callback lacks ordered converted/send/ack marks");
                }
                if let Some(index) = self.record {
                    data.batches[index].callback_exit_ns = Some(stamp);
                    data.batches[index].callback_succeeded = self.succeeded;
                }
            }
        }
    }
}

/// Mark the actual converted batch before the unchanged send/wait callback.
pub fn converted(rows: usize, referenced_bytes: usize) {
    mark(1, rows, referenced_bytes);
}
/// Mark completion of the actual bounded send, not query completion.
pub fn sent() {
    mark(2, 0, 0);
}
/// Mark the actual demand acknowledgment, not query completion.
pub fn acknowledged() {
    mark(3, 0, 0);
}

/// Record actual native admission. This runs inside the complete native call.
pub fn window_policy(expected_rows: Option<usize>, metadata_bytes: u64) {
    ACTIVE.with(|active| {
        let active = active.borrow();
        let Some(active) = active.as_ref() else {
            return;
        };
        let mut data = active.borrow_mut();
        let engine_open = data.engine_open;
        let Some(window) = data.window.as_mut() else {
            data.invalidate("window policy lacks a candidate capture");
            return;
        };
        if !engine_open
            || window.initialized
            || window.released
            || (expected_rows.is_some() && (!window.requested || metadata_bytes != 1024))
            || (expected_rows.is_none() && metadata_bytes != 0)
        {
            data.invalidate("output window policy or metadata reservation is invalid");
            return;
        }
        window.initialized = true;
        window.expected_rows = expected_rows;
        window.eligible = window.requested.then_some(expected_rows.is_some());
        window.metadata_reserved_bytes = metadata_bytes;
        window.metadata_final_bytes = metadata_bytes;
        window.selected_policy = if expected_rows.is_some() {
            "two_batch_window"
        } else if window.requested {
            "ordinary_ineligible"
        } else {
            "ordinary_not_requested"
        };
    });
}

/// Record receipt of a unique FIFO acknowledgment, possibly for an earlier callback.
pub fn acknowledged_ordinal(ordinal: usize) {
    ACTIVE.with(|active| {
        let active = active.borrow();
        let Some(active) = active.as_ref() else {
            return;
        };
        let mut data = active.borrow_mut();
        if !data.engine_open
            || !data.sink_open
            || data.stage != 2
            || ordinal != data.acknowledged_count
            || ordinal >= data.sent_count
        {
            data.invalidate("output window acknowledgment is missing, duplicate or out of order");
            return;
        }
        let Some(window) = data.window.as_mut() else {
            data.invalidate("indexed acknowledgment lacks a candidate capture");
            return;
        };
        if window.outstanding_batches == 0 || window.released {
            data.invalidate("output window acknowledged an empty or released ledger");
            return;
        }
        window.outstanding_referenced_bytes -= window.pending_bytes[window.head];
        window.pending_bytes[window.head] = 0;
        window.head = (window.head + 1) % 2;
        window.outstanding_batches -= 1;
        let active_window = window.active();
        data.acknowledged_count += 1;
        if !active_window {
            data.stage = 3;
        }
        if data.detailed && ordinal < MAX_BATCHES {
            let stamp = data.stamp(Instant::now());
            data.batches[ordinal].acknowledged_ns = Some(stamp);
        }
    });
}

/// Called only after dropping the actual ledger. Abandoned slots are recorded
/// separately from acknowledgments; payload ownership is never inferred here.
pub fn window_released() {
    ACTIVE.with(|active| {
        let active = active.borrow();
        let Some(active) = active.as_ref() else {
            return;
        };
        let mut data = active.borrow_mut();
        if data.engine_open || data.sink_open {
            data.invalidate("output window was released before the native call returned");
            return;
        }
        let Some(window) = data.window.as_mut() else {
            return;
        };
        if window.released {
            data.invalidate("output window was released more than once");
            return;
        }
        window.abandoned_batches = window.outstanding_batches;
        window.abandoned_referenced_bytes = window.outstanding_referenced_bytes;
        window.outstanding_batches = 0;
        window.outstanding_referenced_bytes = 0;
        window.metadata_final_bytes = 0;
        window.released = true;
    });
}

fn mark(stage: u8, rows: usize, bytes: usize) {
    ACTIVE.with(|active| {
        let active = active.borrow();
        let Some(active) = active.as_ref() else {
            return;
        };
        let mut data = active.borrow_mut();
        if !data.sink_open || stage != data.stage + 1 {
            data.invalidate("output-window marks are out of order or outside callback");
            return;
        }
        if stage == 1 && rows != data.current_rows {
            data.invalidate("converted output cardinality differs from native callback");
        }
        data.stage = stage;
        match stage {
            1 => {
                data.ready_count += 1;
                data.current_bytes = bytes;
                data.max_referenced_bytes = data.max_referenced_bytes.max(bytes);
            }
            2 => {
                let bytes = data.current_bytes;
                if data
                    .window
                    .as_mut()
                    .is_some_and(|window| !window.sent(bytes))
                {
                    data.invalidate("output window send exceeds its observed credits");
                }
                data.sent_count += 1;
            }
            3 => data.acknowledged_count += 1,
            _ => unreachable!(),
        }
        if data.detailed && data.batch_count <= MAX_BATCHES {
            let stamp = data.stamp(Instant::now());
            if let Some(batch) = data.batches.last_mut() {
                match stage {
                    1 => {
                        batch.converted_ready_ns = Some(stamp);
                        batch.referenced_arrow_bytes = Some(bytes);
                    }
                    2 => batch.send_complete_ns = Some(stamp),
                    3 => batch.acknowledged_ns = Some(stamp),
                    _ => unreachable!(),
                }
            }
        }
    });
}

#[cfg(test)]
#[path = "output_window_observation_tests.rs"]
mod tests;
