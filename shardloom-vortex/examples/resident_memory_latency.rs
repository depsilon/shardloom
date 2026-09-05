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

    struct ProfileSpec {
        name: &'static str,
        description: &'static str,
        rows: usize,
        columns: &'static [&'static str],
        filter_lower_bound: i64,
        raw_value_bytes: usize,
    }

    trait FixtureProfile {
        fn spec(&self) -> &ProfileSpec;
        fn source(&self, session: &ResidentVortexSession) -> Result<ResidentMemorySource, Error>;
        fn expected(&self) -> &serde_json::Value;

        fn execute(&self, session: &ResidentVortexSession) -> Result<CollectedVortexRows, Error> {
            let spec = self.spec();
            Ok(self
                .source(session)?
                .prepare_projection(
                    spec.columns,
                    Some(gt_eq(
                        get_item("cohort_key", root()),
                        lit(spec.filter_lower_bound),
                    )),
                    None,
                )?
                .execute()?)
        }

        fn verify(&self, result: &CollectedVortexRows) -> Result<(), Error> {
            if result.rows != 8
                || serde_json::from_str::<serde_json::Value>(result.values_json.value())?
                    != *self.expected()
                || result.native_io_certificate.side_effects.fallback_attempted
                || result.native_io_certificate.side_effects.write_io
                || result.native_io_certificate.side_effects.arrow_converted
                || result.runtime.prepared_source_opens != 0
            {
                return Err(
                    "complete memory result or no-fallback/no-file evidence disagrees".into(),
                );
            }
            Ok(())
        }
    }

    struct NullableFixture {
        spec: ProfileSpec,
        cohort: Vec<Option<i64>>,
        exact: Vec<Option<i64>>,
        text: Vec<Option<String>>,
        admitted: Vec<Option<bool>>,
        measure: Vec<Option<f64>>,
        expected: serde_json::Value,
    }

    impl NullableFixture {
        fn new() -> Self {
            let cohort = (0..32_i64).map(Some).collect();
            let exact = (0..32_i64).map(|row| Some((1_i64 << 60) + row)).collect();
            let text = (0..32)
                .map(|row| (row % 5 != 0).then(|| format!("λ\"\n東京 {row}")))
                .collect::<Vec<_>>();
            let admitted = (0..32)
                .map(|row| (row % 5 != 0).then_some(row % 2 == 0))
                .collect();
            let measure = (0..32)
                .map(|row| (row % 5 != 0).then_some(f64::from(row) / 4.0))
                .collect();
            let mut fixture = Self {
                spec: ProfileSpec {
                    name: "nullable32",
                    description: "32 renamed rows, nullable UTF8/bool/float64 and exact int64 above 2^53; selects final eight rows",
                    rows: 32,
                    columns: &[
                        "cohort_key",
                        "exact_identifier",
                        "nullable_label",
                        "nullable_bool",
                        "nullable_float",
                    ],
                    filter_lower_bound: 24,
                    raw_value_bytes: 32 * 8 * 3
                        + 32_usize.div_ceil(8)
                        + text.iter().flatten().map(String::len).sum::<usize>(),
                },
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
    }

    impl FixtureProfile for NullableFixture {
        fn spec(&self) -> &ProfileSpec {
            &self.spec
        }

        fn expected(&self) -> &serde_json::Value {
            &self.expected
        }

        fn source(&self, session: &ResidentVortexSession) -> Result<ResidentMemorySource, Error> {
            let labels = self.text.iter().map(Option::as_deref).collect::<Vec<_>>();
            Ok(ResidentMemorySource::from_columns(
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
            )?)
        }
    }

    struct Int64Fixture {
        cohort: Vec<i64>,
        exact: Vec<i64>,
        expected: serde_json::Value,
    }

    impl Int64Fixture {
        fn new() -> Self {
            Self {
                cohort: (0..4096_i64).collect(),
                exact: (0..4096_i64).map(|row| (1_i64 << 60) + row).collect(),
                expected: serde_json::Value::Array((4088..4096_i64).map(|row|
                    serde_json::json!({"cohort_key": row, "exact_identifier": (1_i64 << 60) + row})).collect()),
            }
        }
    }

    impl FixtureProfile for Int64Fixture {
        fn spec(&self) -> &ProfileSpec {
            &ProfileSpec {
                name: "int64_64k",
                description: "4096 rows of two nonnullable native Int64 columns; exactly 65536 raw value bytes plus 26 field-name bytes; selects final eight rows",
                rows: 4096,
                columns: &["cohort_key", "exact_identifier"],
                filter_lower_bound: 4088,
                raw_value_bytes: 4096 * 2 * size_of::<i64>(),
            }
        }

        fn expected(&self) -> &serde_json::Value {
            &self.expected
        }

        fn source(&self, session: &ResidentVortexSession) -> Result<ResidentMemorySource, Error> {
            Ok(ResidentMemorySource::from_columns(
                session,
                &[
                    MemoryColumn {
                        name: "cohort_key",
                        values: MemoryColumnValues::Int64NonNullable(&self.cohort),
                    },
                    MemoryColumn {
                        name: "exact_identifier",
                        values: MemoryColumnValues::Int64NonNullable(&self.exact),
                    },
                ],
                MemorySourceBounds::default(),
            )?)
        }
    }

    fn measure(
        fixture: &dyn FixtureProfile,
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

    #[derive(Clone, Copy, Debug)]
    struct OperationInterval {
        start: u64,
        end: u64,
    }

    fn interval(epoch: Instant, start: Instant, end: Instant) -> Result<OperationInterval, Error> {
        Ok(OperationInterval {
            start: u64::try_from(start.duration_since(epoch).as_nanos())?,
            end: u64::try_from(end.duration_since(epoch).as_nanos())?,
        })
    }

    fn require_mixed_overlap(
        foreground: &[OperationInterval],
        background: &[OperationInterval],
    ) -> Result<usize, Error> {
        let mut background_index = 0;
        let mut overlapping = 0;
        for sample in foreground {
            while background_index < background.len()
                && background[background_index].end <= sample.start
            {
                background_index += 1;
            }
            if sample.start < sample.end
                && background.get(background_index).is_some_and(|work| {
                    work.start < work.end && work.start < sample.end && sample.start < work.end
                })
            {
                overlapping += 1;
            }
        }
        if overlapping == 0 {
            return Err("no timed foreground operation overlapped a background operation".into());
        }
        Ok(overlapping)
    }

    fn measure_mixed_foreground(
        fixture: &dyn FixtureProfile,
        session: &ResidentVortexSession,
        iterations: usize,
        epoch: Instant,
    ) -> Result<(Vec<u64>, Vec<OperationInterval>), Error> {
        let mut samples = Vec::with_capacity(iterations);
        let mut intervals = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let started = Instant::now();
            let result = black_box(fixture.execute(session)?);
            let ended = Instant::now();
            samples.push(u64::try_from(ended.duration_since(started).as_nanos())?);
            intervals.push(interval(epoch, started, ended)?);
            fixture.verify(&result)?;
        }
        Ok((samples, intervals))
    }

    fn mixed(
        fixture: &dyn FixtureProfile,
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
        let reference = background.execute()?;
        let expected = serde_json::Value::Array(
            (8192..16_384_i64)
                .map(|value| serde_json::json!({"background_key": value}))
                .collect(),
        );
        if serde_json::from_str::<serde_json::Value>(reference.values_json.value())? != expected
            || reference
                .native_io_certificate
                .side_effects
                .fallback_attempted
            || reference.native_io_certificate.side_effects.arrow_converted
        {
            return Err("complete independent background values disagree".into());
        }
        drop(reference);
        drop(expected);
        let active = AtomicBool::new(true);
        let barrier = Barrier::new(2);
        let epoch = Instant::now();
        let max_background_intervals = iterations.saturating_mul(8).max(1024);
        let ((samples, foreground_intervals), background_intervals) =
            std::thread::scope(|scope| -> Result<_, Error> {
                let worker = scope.spawn(|| -> Result<Vec<OperationInterval>, String> {
                    let mut intervals = Vec::with_capacity(max_background_intervals);
                    barrier.wait();
                    while active.load(Ordering::Acquire) {
                        if intervals.len() == max_background_intervals {
                            return Err("mixed-load interval evidence bound exceeded".into());
                        }
                        let started = Instant::now();
                        let result = background
                            .execute_arrays()
                            .map_err(|error| error.to_string())?;
                        let ended = Instant::now();
                        if result.row_count() != 8_192 {
                            return Err("mixed-load background row count mismatch".into());
                        }
                        intervals.push(
                            interval(epoch, started, ended).map_err(|error| error.to_string())?,
                        );
                        std::thread::yield_now();
                    }
                    Ok(intervals)
                });
                barrier.wait();
                let samples = measure_mixed_foreground(fixture, session, iterations, epoch);
                active.store(false, Ordering::Release);
                let intervals = worker.join().map_err(|_| "mixed-load worker panicked")??;
                Ok((samples?, intervals))
            })?;
        let overlapping = require_mixed_overlap(&foreground_intervals, &background_intervals)?;
        let raw_intervals = |intervals: &[OperationInterval]| {
            intervals
                .iter()
                .map(|work| [work.start, work.end])
                .collect::<Vec<_>>()
        };
        Ok(
            serde_json::json!({ "foreground": latency(&samples), "background_completed_operations": background_intervals.len(),
            "foreground_intervals_nanos": raw_intervals(&foreground_intervals),
            "background_intervals_nanos": raw_intervals(&background_intervals),
            "interval_scope": "half-open monotonic operation intervals from one epoch; includes admission wait; excludes reference verification and result drop; overlap is concurrent in-flight calls, not simultaneous CPU execution",
            "foreground_operations_with_background_overlap": overlapping,
            "background_interval_capacity": max_background_intervals,
            "background": "16384-row native nullable-int filter/projection into 8192 owned array rows; same resident session, admission gate and buffer budget",
            "background_validation": "all 8192 values compared to an independent Rust sequence once before measured overlap; exact row count checked on each background operation; every foreground scalar checked on every sample" }),
        )
    }

    pub fn run() -> Result<(), Error> {
        let mut args = std::env::args().skip(1);
        let iterations: usize = args.next().map_or(Ok(1000), |value| value.parse())?;
        let profile = args.next().unwrap_or_else(|| "nullable32".into());
        if !(100..=100_000).contains(&iterations) || args.next().is_some() {
            return Err(
                "usage: resident_memory_latency [ITERATIONS 100..=100000] [nullable32|int64_64k]"
                    .into(),
            );
        }
        let fixture: Box<dyn FixtureProfile> = match profile.as_str() {
            "nullable32" => Box::new(NullableFixture::new()),
            "int64_64k" => Box::new(Int64Fixture::new()),
            _ => return Err("profile must be nullable32 or int64_64k".into()),
        };
        let spec = fixture.spec();
        let started = Instant::now();
        let session = ResidentVortexSession::new(64 * 1024 * 1024, 2)?;
        let session_prepare_nanos = u64::try_from(started.elapsed().as_nanos())?;
        let source = fixture.source(&session)?;
        let input_admission_bytes = source.input_logical_bytes();
        let intake_payload_bytes_copied = source.intake_payload_bytes_copied();
        let native_input_dtype = source.dtype().to_string();
        drop(source);
        fixture.verify(&fixture.execute(&session)?)?;
        let isolated = measure(fixture.as_ref(), &session, iterations)?;
        let mixed = mixed(fixture.as_ref(), &session, iterations)?;
        let snapshot = session.snapshot();
        if snapshot.memory.reserved_bytes != 0 || snapshot.memory.denied_reservations != 0 {
            return Err(
                "memory latency acceptance retained owned bytes or denied admission".into(),
            );
        }
        let memory = serde_json::json!({
            "scope": "session allocator native value/offset/validity buffers and result JSON capacity; excludes caller/parser storage, array metadata, upstream scratch not using allocator, process RSS",
            "limit_bytes": snapshot.memory.limit_bytes,
            "live_reserved_bytes_after_results_drop": snapshot.memory.reserved_bytes,
            "peak_reserved_bytes": snapshot.memory.peak_reserved_bytes,
            "denied_reservations": snapshot.memory.denied_reservations,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "schema_version": "shardloom.resident_memory_latency.v1", "iterations": iterations,
                "provider_version": shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION,
                "timing_boundary": "borrowed view creation, validation, native buffer intake, expression binding, exact native filter/projection and complete JSON return; retained session construction, caller fixture creation, output verification and returned JSON drop excluded",
                "fixture": spec.description, "profile": spec.name,
                "input_rows": spec.rows, "input_columns": spec.columns.len(), "output_rows": 8,
                "raw_value_payload_bytes": spec.raw_value_bytes,
                "raw_value_byte_scope": "primitive value buffers, UTF8 payload and packed boolean values; excludes validity, offsets, names, caller containers and metadata",
                "typed_input_admission_bytes": input_admission_bytes,
                "typed_input_admission_scope": "raw values plus conservative nullable-validity allowance, UTF8 offsets and field names; separate from actual allocation credits and RSS",
                "native_intake_payload_bytes_copied": intake_payload_bytes_copied,
                "intake_copy_scope": "numeric value and UTF8 payload copies; excludes bitmap, offset and name construction",
                "native_input_dtype": native_input_dtype,
                "latency_claim_scope": "only this measured input/profile/output and declared mixed background; no blanket service latency guarantee",
                "validation": "complete JSON values compared to independently constructed literal values on every sample; no fallback and no file opens",
                "session_prepare_nanos": session_prepare_nanos, "warmup_operations": 1,
                "isolated": latency(&isolated), "mixed": mixed,
                "fallback_attempted": false, "durable_publication": false,
                "completed_native_executions": snapshot.completed_executions,
                "memory": memory
            }))?
        );
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::{
            FixtureProfile as _, Int64Fixture, NullableFixture, OperationInterval,
            require_mixed_overlap,
        };
        use shardloom_vortex::{
            resident_memory_source::{
                MemoryColumn, MemoryColumnValues, MemorySourceBounds, ResidentMemorySource,
            },
            resident_session::ResidentVortexSession,
        };
        use vortex::array::dtype::{DType, Nullability, PType};

        #[test]
        fn mixed_acceptance_requires_overlap_inside_timed_operations() {
            let foreground = [
                OperationInterval { start: 10, end: 20 },
                OperationInterval { start: 30, end: 40 },
            ];
            for background in [
                vec![],
                vec![OperationInterval { start: 0, end: 10 }],
                vec![OperationInterval { start: 20, end: 30 }],
                vec![OperationInterval { start: 40, end: 50 }],
            ] {
                assert!(require_mixed_overlap(&foreground, &background).is_err());
            }
            assert_eq!(
                require_mixed_overlap(&foreground, &[OperationInterval { start: 15, end: 35 }])
                    .unwrap(),
                2
            );
            assert_eq!(
                require_mixed_overlap(&foreground, &[OperationInterval { start: 5, end: 15 }])
                    .unwrap(),
                1
            );
        }

        #[test]
        fn numeric_profile_is_exactly_64k_values_with_explicit_name_overhead_and_eight_results() {
            let fixture = Int64Fixture::new();
            assert_eq!(fixture.spec().rows, 4096);
            assert_eq!(fixture.spec().columns.len(), 2);
            assert_eq!(fixture.spec().raw_value_bytes, 65_536);
            assert_eq!(
                fixture.cohort.len() * size_of::<i64>() + fixture.exact.len() * size_of::<i64>(),
                65_536
            );
            let session = ResidentVortexSession::new(1024 * 1024, 1).unwrap();
            let source = fixture.source(&session).unwrap();
            assert_eq!(source.input_logical_bytes(), 65_562);
            assert_eq!(source.intake_payload_bytes_copied(), 65_536);
            for name in fixture.spec().columns {
                assert_eq!(
                    source.dtype().as_struct_fields().field(name),
                    Some(DType::Primitive(PType::I64, Nullability::NonNullable))
                );
            }
            assert!(
                ResidentMemorySource::from_columns(
                    &session,
                    &[
                        MemoryColumn {
                            name: "cohort_key",
                            values: MemoryColumnValues::Int64NonNullable(&fixture.cohort)
                        },
                        MemoryColumn {
                            name: "exact_identifier",
                            values: MemoryColumnValues::Int64NonNullable(&fixture.exact)
                        },
                    ],
                    MemorySourceBounds {
                        max_input_bytes: 65_536,
                        ..MemorySourceBounds::default()
                    }
                )
                .is_err()
            );
            let expected = fixture.expected().as_array().unwrap();
            assert_eq!(expected.len(), 8);
            assert_eq!(expected[0]["cohort_key"], 4088);
            assert_eq!(expected[7]["exact_identifier"], (1_i64 << 60) + 4095);
            fixture.verify(&fixture.execute(&session).unwrap()).unwrap();
        }

        #[test]
        fn default_nullable_profile_keeps_original_schema_nulls_and_complete_values() {
            let fixture = NullableFixture::new();
            assert_eq!(fixture.spec().name, "nullable32");
            assert_eq!(fixture.spec().rows, 32);
            assert_eq!(fixture.spec().columns.len(), 5);
            let session = ResidentVortexSession::new(1024 * 1024, 1).unwrap();
            let source = fixture.source(&session).unwrap();
            for name in fixture.spec().columns {
                assert_eq!(
                    source
                        .dtype()
                        .as_struct_fields()
                        .field(name)
                        .unwrap()
                        .nullability(),
                    Nullability::Nullable
                );
            }
            assert!(fixture.expected()[1]["nullable_label"].is_null());
            assert_eq!(fixture.expected()[0]["cohort_key"], 24);
            fixture.verify(&fixture.execute(&session).unwrap()).unwrap();
        }
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
