//! Opt-in, bounded fixed-arrival evidence; never a production fairness claim.

use super::*;
use serde_json::{Value, json};
use std::sync::Mutex;

const REQUESTS: usize = 96;
const CLIENTS: usize = 8;
const DISPATCH_QUEUE: usize = 16;
const WRITER_REPEATS: usize = 8;

#[derive(Clone, Copy)]
enum Kind {
    Count,
    Projection,
    Writer,
}
impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Count => "metadata_count",
            Self::Projection => "native_projection_json",
            Self::Writer => "native_writer",
        }
    }
}

struct Job {
    id: usize,
    kind: Kind,
    scheduled: Duration,
    submitted: Duration,
}

fn micros(time: Duration) -> u64 {
    u64::try_from(time.as_micros()).unwrap()
}

fn quantiles(mut values: Vec<u64>) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    values.sort_unstable();
    let rank =
        |numerator: usize| values[(values.len() * numerator).div_ceil(100).saturating_sub(1)];
    json!({"n": values.len(), "min": values[0], "p50": rank(50), "p95": rank(95), "p99": rank(99), "max": values[values.len()-1]})
}

fn exact_json(json_text: &str, expected_rows: usize) {
    let rows: Value = serde_json::from_str(json_text).unwrap();
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), expected_rows);
    for (row, actual) in rows.iter().enumerate() {
        assert_eq!(
            *actual,
            json!({"renamed_exact_key": i64::MAX - i64::try_from(row % 4096).unwrap()})
        );
    }
}

fn execute(
    job: &Job,
    source: &PreparedVortexSource,
    fixture: &Fixture,
    session: &ResidentVortexSession,
    started: Instant,
) -> Result<(ResidentCallTiming, Duration)> {
    let cancellation = CancellationToken::default();
    match job.kind {
        Kind::Count => {
            let (rows, timing) = source.prepare_count().execute_timed(&cancellation)?;
            let delivered = started.elapsed();
            assert_eq!(rows, 4096);
            Ok((timing, delivered))
        }
        Kind::Projection => {
            let (result, timing) = source
                .prepare_projection(&["renamed_exact_key"], 4096, 1 << 20)?
                .execute_timed(&cancellation)?;
            // The result sink reacquires CPU admission. Its wait and work are
            // included in delivery, separately from first-stage native timing.
            let json = result.to_bounded_json(&["renamed_exact_key".to_owned()], 1 << 20)?;
            drop(result);
            let delivered = started.elapsed();
            exact_json(json.value(), 4096);
            drop(json);
            Ok((timing, delivered))
        }
        Kind::Writer => {
            let output = fixture.directory.join(format!("load-{}.vortex", job.id));
            let context = session.0.enter(CallClass::General, cancellation)?;
            source.with_admitted_native_execution(&context, |file, context| {
                shardloom_core::write_workspace_safe_bytes_with_producer(
                    &fixture.directory,
                    &output,
                    false,
                    "bounded serving load writer",
                    |sink| {
                        let _layout_references = context.memory().reserve(
                            (WRITER_REPEATS * size_of::<vortex::layout::LayoutRef>()) as u64,
                        )?;
                        let mut writer = context
                            .native_session()
                            .write_options()
                            .with_strategy(
                                crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(WRITER_REPEATS),
                            )
                            .blocking(context.runtime())
                            .writer(sink, file.dtype().clone());
                        for _ in 0..WRITER_REPEATS {
                            let scan = file
                                .scan()
                                .map_err(native_error)?
                                .with_concurrency(context.cpu_lanes())
                                .into_array_iter(context.runtime())
                                .map_err(native_error)?;
                            for array in scan {
                                let array = array.map_err(native_error)?;
                                context.check_cancelled()?;
                                writer.push(array).map_err(native_error)?;
                            }
                        }
                        writer.finish().map_err(native_error)?;
                        Ok(())
                    },
                )?;
                Ok(())
            })?;
            context.drain_io();
            source.validate_generation()?;
            let timing = context.timing();
            drop(context);
            let delivered = started.elapsed();
            // Independent complete-value readback is outside writer delivery.
            // It remains part of client occupancy and is separately timestamped.
            let written = session.prepare_file(&output)?;
            let rows = 4096 * WRITER_REPEATS;
            let values = written
                .prepare_projection(
                    &["renamed_exact_key"],
                    u64::try_from(rows).unwrap(),
                    8 << 20,
                )?
                .execute()?;
            let json = values.to_bounded_json(&["renamed_exact_key".to_owned()], 8 << 20)?;
            exact_json(json.value(), rows);
            Ok((timing, delivered))
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "Keep the bounded arrival schedule, client drain and complete receipt in one test lifecycle"
)]
fn run(serving: bool, interval: Duration, parallelism: usize) -> Value {
    let fixture = Fixture::new();
    let policy = ResidentServingPolicy {
        max_queued_calls: 16,
        ..Default::default()
    };
    let session = if serving {
        ResidentVortexSession::with_serving_policy(128 << 20, parallelism, policy)
    } else {
        ResidentVortexSession::new(128 << 20, parallelism)
    }
    .unwrap();
    let source = session.prepare_file(fixture.path()).unwrap();
    let memory = session.memory().clone();
    let (sender, receiver) = mpsc::sync_channel::<Job>(DISPATCH_QUEUE);
    let receiver = Mutex::new(receiver);
    let records = Mutex::new(Vec::with_capacity(REQUESTS));
    // Start only after every bounded client exists; schedule is independent of
    // completions and try_send records overload instead of delaying arrivals.
    let barrier = std::sync::Barrier::new(CLIENTS + 1);
    let epoch = std::sync::OnceLock::<std::time::Instant>::new();
    thread::scope(|threads| {
        for _ in 0..CLIENTS {
            let receiver = &receiver;
            let records = &records;
            let source = &source;
            let fixture = &fixture;
            let session = &session;
            let barrier = &barrier;
            let epoch = &epoch;
            threads.spawn(move || {
                barrier.wait();
                let started = *epoch.get().unwrap();
                loop {
                    let job = receiver.lock().unwrap().recv();
                    let Ok(job) = job else { break; };
                    let dispatched = started.elapsed();
                    let outcome = execute(&job, source, fixture, session, started);
                    let client_ready = started.elapsed();
                    let mut record = json!({
                        "id": job.id, "kind": job.kind.name(),
                        "scheduled_us": micros(job.scheduled), "submitted_us": micros(job.submitted),
                        "dispatched_us": micros(dispatched), "client_ready_us": micros(client_ready),
                    });
                    match outcome {
                        Ok((timing, delivered)) => {
                            record["status"] = json!("exact");
                            record["first_native_queue_us"] = json!(micros(timing.queue));
                            record["first_native_service_us"] = json!(micros(timing.service));
                            record["delivered_us"] = json!(micros(delivered));
                            record["scheduled_to_delivery_us"] = json!(micros(delivered.checked_sub(job.scheduled).unwrap()));
                        }
                        Err(error) => { record["status"] = json!("engine_error"); record["error"] = json!(error.to_string()); }
                    }
                    records.lock().unwrap().push(record);
                }
            });
        }
        let started = Instant::now();
        epoch.set(started).unwrap();
        barrier.wait();
        for id in 0..REQUESTS {
            let scheduled = interval * u32::try_from(id).unwrap();
            if let Some(remaining) = scheduled.checked_sub(started.elapsed()) {
                thread::sleep(remaining);
            }
            let kind = if id % 16 == 0 {
                Kind::Writer
            } else if id % 4 == 0 {
                Kind::Projection
            } else {
                Kind::Count
            };
            let submitted = started.elapsed();
            let job = Job {
                id,
                kind,
                scheduled,
                submitted,
            };
            if let Err(error) = sender.try_send(job) {
                let job = match error {
                    mpsc::TrySendError::Full(job) => job,
                    mpsc::TrySendError::Disconnected(_) => {
                        panic!("bounded load clients disconnected")
                    }
                };
                records.lock().unwrap().push(json!({"id": job.id, "kind": job.kind.name(), "scheduled_us": micros(job.scheduled), "submitted_us": micros(job.submitted), "status": "client_queue_rejected"}));
            }
        }
        drop(sender);
    });
    let elapsed = epoch.get().unwrap().elapsed();
    let mut records = records.into_inner().unwrap();
    records.sort_unstable_by_key(|value| value["id"].as_u64().unwrap());
    assert_eq!(records.len(), REQUESTS);
    for (id, record) in records.iter().enumerate() {
        assert_eq!(record["id"], id);
    }
    let errors = records
        .iter()
        .filter(|record| record["status"] == "engine_error")
        .count();
    let completed = records
        .iter()
        .filter(|record| record["status"] == "exact")
        .count();
    let rejected = REQUESTS - completed - errors;
    let families = [Kind::Count, Kind::Projection, Kind::Writer].map(|kind| {
        let values = records.iter().filter(|record| record["kind"] == kind.name() && record["status"] == "exact");
        json!({"kind": kind.name(),
            "scheduled_to_delivery_us": quantiles(values.clone().map(|record| record["scheduled_to_delivery_us"].as_u64().unwrap()).collect()),
            "first_native_queue_us": quantiles(values.clone().map(|record| record["first_native_queue_us"].as_u64().unwrap()).collect()),
            "first_native_service_us": quantiles(values.map(|record| record["first_native_service_us"].as_u64().unwrap()).collect()),
        })
    });
    let admission = session.admission_snapshot();
    let io = session.io_snapshot();
    if let Some(admission) = admission {
        assert_eq!(admission.active_cpu_lanes, 0);
        assert_eq!(admission.queued_calls, 0);
        assert!(admission.peak_active_cpu_lanes <= parallelism);
    }
    if let Some(io) = io {
        assert_eq!(io.active_requests, 0);
        assert_eq!(io.active_bytes, 0);
    }
    drop(source);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let report = json!({
        "mode": if serving {"serving"} else {"exclusive_batch"}, "requested_cpu_lanes": parallelism,
        "elapsed_us_including_oracle_and_drain": micros(elapsed), "scheduled_requests": REQUESTS,
        "exact_completions": completed, "client_queue_rejections": rejected, "engine_errors": errors,
        "exact_completions_per_second_including_oracle_and_drain": f64::from(u32::try_from(completed).unwrap()) / elapsed.as_secs_f64(),
        "admission": admission.map(|a| json!({"peak_active_calls": a.peak_active_calls, "peak_cpu_lanes": a.peak_active_cpu_lanes, "peak_queued_calls": a.peak_queued_calls, "peak_queued_metadata_bytes": a.peak_queued_call_bytes, "rejected_calls": a.rejected_calls})),
        "io": io.map(|io| json!({"peak_requests": io.peak_requests, "peak_bytes": io.peak_bytes, "rejected_requests": io.rejected_requests})),
        "final_reserved_bytes": memory.snapshot().reserved_bytes, "families": families, "requests": records,
    });
    println!("{}", serde_json::to_string(&report).unwrap());
    assert_eq!(errors, 0, "retain raw receipts when an engine call fails");
    report
}

#[test]
#[ignore = "explicit bounded fixed-arrival load run; prints complete JSON receipts"]
fn serving_fixed_arrival_load_receipt() {
    let interval_us = std::env::var("SHARDLOOM_SERVING_INTERVAL_US")
        .ok()
        .map_or(1000, |value| value.parse::<u64>().unwrap());
    assert!((100..=100_000).contains(&interval_us));
    let parallelism = thread::available_parallelism().unwrap().get().min(4);
    assert!(
        parallelism >= 2,
        "overlap evidence requires P >= 2; P1 behavior has deterministic tests"
    );
    println!(
        "{}",
        json!({"schema_version": 1, "fixture": "bounded_fixed_arrival_native_serving", "interval_us": interval_us, "clients": CLIENTS, "dispatch_queue": DISPATCH_QUEUE, "requests_per_mode": REQUESTS, "source_rows": 4096, "writer_rows": 4096 * WRITER_REPEATS, "host_available_cpu": thread::available_parallelism().unwrap().get(), "limitations": ["small local fixture; no production fairness claim", "first native timing excludes result-sink reacquisition", "oracle verification occupies clients and is timestamped after delivery", "queue byte counters cover admission metadata, not caller request payloads", "OS RSS and host contention require an external supervisor", "exclusive mode runs first; repeat/interleave externally for comparisons"]})
    );
    let interval = Duration::from_micros(interval_us);
    run(false, interval, parallelism);
    run(true, interval, parallelism);
}
