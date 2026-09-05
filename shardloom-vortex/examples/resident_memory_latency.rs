//! Complete bounded native-memory intake latency, with literal value checks.
//! Redirect raw JSON only into an admitted local UAT directory. This small
//! example writes no files and uses no other query engine.

#[cfg(unix)]
mod native {
    use shardloom_vortex::{
        local_primitives::collect::CollectedVortexRows,
        resident_memory_source::{
            MemoryColumn, MemoryColumnValues, MemorySourceBounds, ResidentMemorySource,
        },
        resident_session::ResidentVortexSession,
    };
    use std::{
        hint::black_box,
        sync::{
            Barrier,
            atomic::{AtomicBool, Ordering},
        },
        time::Instant,
    };
    use vortex::expr::{get_item, gt_eq, lit, root};

    type Error = Box<dyn std::error::Error>;

    struct Fixture {
        cohort: Vec<Option<i64>>,
        exact: Vec<Option<i64>>,
        text: Vec<Option<String>>,
        admitted: Vec<Option<bool>>,
        measure: Vec<Option<f64>>,
        expected: serde_json::Value,
    }

    impl Fixture {
        fn new() -> Self {
            let cohort = (0..32_i64).map(Some).collect();
            let exact = (0..32_i64).map(|row| Some((1_i64 << 60) + row)).collect();
            let text = (0..32)
                .map(|row| (row % 5 != 0).then(|| format!("λ\"\n東京 {row}")))
                .collect();
            let admitted = (0..32)
                .map(|row| (row % 5 != 0).then_some(row % 2 == 0))
                .collect();
            let measure = (0..32)
                .map(|row| (row % 5 != 0).then_some(f64::from(row) / 4.0))
                .collect();
            let mut fixture = Self {
                cohort,
                exact,
                text,
                admitted,
                measure,
                expected: serde_json::Value::Null,
            };
            fixture.expected = serde_json::Value::Array((24..32).map(|row| serde_json::json!({
                "cohort_key": row, "exact_identifier": fixture.exact[row],
                "nullable_label": fixture.text[row], "nullable_bool": fixture.admitted[row],
                "nullable_float": fixture.measure[row],
            })).collect());
            fixture
        }

        fn execute(&self, session: &ResidentVortexSession) -> Result<CollectedVortexRows, Error> {
            let labels = self.text.iter().map(Option::as_deref).collect::<Vec<_>>();
            let source = ResidentMemorySource::from_columns(
                session,
                &[
                    MemoryColumn {
                        name: "cohort_key",
                        values: MemoryColumnValues::Int64(&self.cohort),
                    },
                    MemoryColumn {
                        name: "exact_identifier",
                        values: MemoryColumnValues::Int64(&self.exact),
                    },
                    MemoryColumn {
                        name: "nullable_label",
                        values: MemoryColumnValues::Utf8(&labels),
                    },
                    MemoryColumn {
                        name: "nullable_bool",
                        values: MemoryColumnValues::Bool(&self.admitted),
                    },
                    MemoryColumn {
                        name: "nullable_float",
                        values: MemoryColumnValues::Float64(&self.measure),
                    },
                ],
                MemorySourceBounds::default(),
            )?;
            Ok(source
                .prepare_projection(
                    &[
                        "cohort_key",
                        "exact_identifier",
                        "nullable_label",
                        "nullable_bool",
                        "nullable_float",
                    ],
                    Some(gt_eq(get_item("cohort_key", root()), lit(24_i64))),
                    None,
                )?
                .execute()?)
        }

        fn verify(&self, result: &CollectedVortexRows) -> Result<(), Error> {
            if result.rows != 8
                || serde_json::from_str::<serde_json::Value>(result.values_json.value())?
                    != self.expected
                || result.native_io_certificate.side_effects.fallback_attempted
                || result.native_io_certificate.side_effects.write_io
                || result.runtime.prepared_source_opens != 0
            {
                return Err(
                    "complete memory result or no-fallback/no-file evidence disagrees".into(),
                );
            }
            Ok(())
        }
    }

    fn measure(
        fixture: &Fixture,
        session: &ResidentVortexSession,
        iterations: usize,
    ) -> Result<Vec<u64>, Error> {
        let mut samples = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let started = Instant::now();
            let result = black_box(fixture.execute(session)?);
            samples.push(u64::try_from(started.elapsed().as_nanos())?);
            fixture.verify(&result)?;
        }
        Ok(samples)
    }

    fn latency(samples: &[u64]) -> serde_json::Value {
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let percentile = |percent: usize| sorted[(sorted.len() * percent).div_ceil(100) - 1];
        serde_json::json!({ "raw_nanos": samples, "p50_nanos": percentile(50),
            "p95_nanos": percentile(95), "p99_nanos": percentile(99), "max_nanos": sorted.last(),
            "percentile_method": "nearest_rank" })
    }

    fn mixed(
        fixture: &Fixture,
        session: &ResidentVortexSession,
        iterations: usize,
    ) -> Result<serde_json::Value, Error> {
        let ids = (0..16_384_i64).map(Some).collect::<Vec<_>>();
        let source = ResidentMemorySource::from_columns(
            session,
            &[MemoryColumn {
                name: "background_key",
                values: MemoryColumnValues::Int64(&ids),
            }],
            MemorySourceBounds::default(),
        )?;
        let background = source.prepare_projection(
            &["background_key"],
            Some(gt_eq(get_item("background_key", root()), lit(8_192_i64))),
            None,
        )?;
        let active = AtomicBool::new(true);
        let barrier = Barrier::new(2);
        let (samples, completed) = std::thread::scope(|scope| -> Result<_, Error> {
            let worker = scope.spawn(|| -> Result<u64, String> {
                barrier.wait();
                let mut completed = 0;
                while active.load(Ordering::Acquire) {
                    let result = background
                        .execute_arrays()
                        .map_err(|error| error.to_string())?;
                    if result.row_count() != 8_192 {
                        return Err("mixed-load background row count mismatch".into());
                    }
                    completed += 1;
                    std::thread::yield_now();
                }
                Ok(completed)
            });
            barrier.wait();
            let samples = measure(fixture, session, iterations);
            active.store(false, Ordering::Release);
            let completed = worker.join().map_err(|_| "mixed-load worker panicked")??;
            Ok((samples?, completed))
        })?;
        if completed == 0 {
            return Err("mixed load completed no background operations".into());
        }
        Ok(
            serde_json::json!({ "foreground": latency(&samples), "background_completed_operations": completed,
            "background": "16384-row native nullable-int filter/projection into 8192 owned array rows; same resident session, admission gate and buffer budget",
            "background_validation": "exact selected row count; foreground validates every scalar against literal fixture" }),
        )
    }

    pub fn run() -> Result<(), Error> {
        let mut args = std::env::args().skip(1);
        let iterations: usize = args.next().map_or(Ok(1000), |value| value.parse())?;
        if !(100..=100_000).contains(&iterations) || args.next().is_some() {
            return Err("usage: resident_memory_latency [ITERATIONS 100..=100000]".into());
        }
        let fixture = Fixture::new();
        let started = Instant::now();
        let session = ResidentVortexSession::new(64 * 1024 * 1024, 2)?;
        let session_prepare_nanos = u64::try_from(started.elapsed().as_nanos())?;
        fixture.verify(&fixture.execute(&session)?)?;
        let isolated = measure(&fixture, &session, iterations)?;
        let mixed = mixed(&fixture, &session, iterations)?;
        let snapshot = session.snapshot();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "schema_version": "shardloom.resident_memory_latency.v1", "iterations": iterations,
                "provider_version": shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION,
                "timing_boundary": "borrowed view creation, validation, native buffer intake, expression binding, exact native filter/projection and complete JSON return; retained session construction, caller fixture creation, output verification and returned JSON drop excluded",
                "fixture": "32 renamed rows, nullable UTF8/bool/float64 and exact int64 above 2^53; selects final eight rows",
                "validation": "complete JSON values compared to independently constructed literal values on every sample; no fallback and no file opens",
                "session_prepare_nanos": session_prepare_nanos, "warmup_operations": 1,
                "isolated": latency(&isolated), "mixed": mixed,
                "fallback_attempted": false, "durable_publication": false,
                "completed_native_executions": snapshot.completed_executions,
                "memory": { "scope": "session allocator native value/offset/validity buffers and result JSON capacity; excludes caller/parser storage, array metadata, upstream scratch not using allocator, process RSS",
                    "limit_bytes": snapshot.memory.limit_bytes, "live_reserved_bytes_after_results_drop": snapshot.memory.reserved_bytes,
                    "peak_reserved_bytes": snapshot.memory.peak_reserved_bytes, "denied_reservations": snapshot.memory.denied_reservations }
            }))?
        );
        Ok(())
    }
}

#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    native::run()
}

#[cfg(not(unix))]
fn main() {
    eprintln!("resident memory example requires the Unix native collect feature surface");
}
