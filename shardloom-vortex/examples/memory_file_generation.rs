//! Explicit small generation/query/publication proof. Requires a new target and
//! --enable-write. Data and independent scalar references are constructed outside
//! measured boundaries. Large workloads and codec selection are not represented.

#[cfg(unix)]
mod native {
    use std::{hint::black_box, path::PathBuf, time::Instant};

    use serde_json::{Value, json};
    use shardloom_core::{ColumnRef, DatasetUri};
    use shardloom_plan::ProjectionRequest;
    use shardloom_vortex::{
        local_primitives::collect::{CollectedVortexRows, prepare_rows_in_session},
        memory_file_generation::{MemoryFileGeneration, MemoryFileGenerationBounds},
        query_primitive::VortexQueryPrimitiveRequest,
        resident_memory_source::{
            MemoryColumn, MemoryColumnValues, MemorySourceBounds, ResidentMemorySource,
        },
        resident_session::ResidentVortexSession,
    };
    use vortex::expr::{get_item, gt_eq, lit, root};

    type Error = Box<dyn std::error::Error>;
    const COLUMNS: [&str; 4] = [
        "renamed_label",
        "nullable_float",
        "exact_identifier",
        "cohort",
    ];

    struct Fixture {
        cohort: Vec<Option<i64>>,
        ids: Vec<Option<i64>>,
        labels: Vec<Option<String>>,
        measurements: Vec<Option<f64>>,
        expected_all: Value,
        expected_query: Value,
    }

    impl Fixture {
        fn new(rows: u32) -> Self {
            let cohort = (0..rows)
                .map(|row| Some(i64::from(row)))
                .collect::<Vec<_>>();
            let ids = (0..rows)
                .map(|row| Some((1_i64 << 60) + i64::from(row)))
                .collect::<Vec<_>>();
            let labels = (0..rows)
                .map(|row| (row % 5 != 0).then(|| format!("λ\"\n東京 {row}")))
                .collect::<Vec<_>>();
            let measurements = (0..rows)
                .map(|row| (row % 3 != 0).then_some(f64::from(row) / 4.0))
                .collect::<Vec<_>>();
            let expected = (0..rows as usize)
                .map(|row| {
                    json!({
                        "cohort": cohort[row], "exact_identifier": ids[row],
                        "renamed_label": labels[row], "nullable_float": measurements[row],
                    })
                })
                .collect::<Vec<_>>();
            let expected_query = Value::Array(
                expected
                    .iter()
                    .skip(rows as usize / 2)
                    .take(7)
                    .cloned()
                    .collect(),
            );
            Self {
                cohort,
                ids,
                labels,
                measurements,
                expected_all: Value::Array(expected),
                expected_query,
            }
        }

        fn source(&self, session: &ResidentVortexSession) -> Result<ResidentMemorySource, Error> {
            let labels = self.labels.iter().map(Option::as_deref).collect::<Vec<_>>();
            Ok(ResidentMemorySource::from_columns(
                session,
                &[
                    MemoryColumn {
                        name: "cohort",
                        values: MemoryColumnValues::Int64(&self.cohort),
                    },
                    MemoryColumn {
                        name: "exact_identifier",
                        values: MemoryColumnValues::Int64(&self.ids),
                    },
                    MemoryColumn {
                        name: "renamed_label",
                        values: MemoryColumnValues::Utf8(&labels),
                    },
                    MemoryColumn {
                        name: "nullable_float",
                        values: MemoryColumnValues::Float64(&self.measurements),
                    },
                ],
                MemorySourceBounds::default(),
            )?)
        }
    }

    fn verify(result: &CollectedVortexRows, expected: &Value) -> Result<(), Error> {
        if serde_json::from_str::<Value>(result.values_json.value())? != *expected
            || result.native_io_certificate.side_effects.fallback_attempted
            || result.native_io_certificate.side_effects.arrow_converted
        {
            return Err("complete scalar values or native execution evidence disagree".into());
        }
        Ok(())
    }

    fn latency(samples: &[u64]) -> Value {
        let mut ordered = samples.to_vec();
        ordered.sort_unstable();
        let percentile = |percent: usize| ordered[(ordered.len() * percent).div_ceil(100) - 1];
        json!({"raw_nanos": samples, "p50_nanos": percentile(50), "p95_nanos": percentile(95),
            "p99_nanos": percentile(99), "percentile_method": "nearest_rank"})
    }

    fn query_samples(
        source: &ResidentMemorySource,
        generation: &MemoryFileGeneration,
        fixture: &Fixture,
        rows: u32,
        iterations: usize,
    ) -> Result<(Vec<u64>, Vec<u64>), Error> {
        let mut ordinary_samples = Vec::with_capacity(iterations);
        let mut generation_samples = Vec::with_capacity(iterations);
        for sample in 0..=iterations {
            let order = if sample % 2 == 0 {
                [true, false]
            } else {
                [false, true]
            };
            for ordinary in order {
                let filter = || Some(gt_eq(get_item("cohort", root()), lit(i64::from(rows / 2))));
                let started = Instant::now();
                let result = black_box(if ordinary {
                    source
                        .prepare_projection(&COLUMNS, filter(), Some(7))?
                        .execute()?
                } else {
                    generation.collect(&COLUMNS, filter(), 7, 64 * 1024)?
                });
                let nanos = u64::try_from(started.elapsed().as_nanos())?;
                if sample != 0 {
                    if ordinary {
                        ordinary_samples.push(nanos);
                    } else {
                        generation_samples.push(nanos);
                    }
                }
                verify(&result, &fixture.expected_query)?;
            }
        }
        Ok((ordinary_samples, generation_samples))
    }

    pub fn run() -> Result<(), Error> {
        let args = std::env::args().skip(1).take(5).collect::<Vec<_>>();
        if args.len() != 4 || args[0] != "--enable-write" {
            return Err("usage: memory_file_generation --enable-write NEW_OUTPUT.vortex ROWS ITERATIONS; rows 1..16384, iterations 1..1000".into());
        }
        let target = PathBuf::from(&args[1]);
        let rows = args[2].parse::<u32>()?;
        let iterations = args[3].parse::<usize>()?;
        if !(1..=16_384).contains(&rows) || !(1..=1000).contains(&iterations) {
            return Err("rows or iterations exceed the small prototype bounds".into());
        }
        let fixture = Fixture::new(rows);
        let session = ResidentVortexSession::new(128 * 1024 * 1024, 2)?;
        let started = Instant::now();
        let source = fixture.source(&session)?;
        let intake_nanos = started.elapsed().as_nanos();
        let started = Instant::now();
        let generation = source.file_generation(MemoryFileGenerationBounds::default())?;
        let generation_nanos = started.elapsed().as_nanos();
        let (ordinary_samples, generation_samples) =
            query_samples(&source, &generation, &fixture, rows, iterations)?;
        let evidence = generation.evidence();
        if evidence.source_file_opens != 0 || session.snapshot().prepared_source_opens != 0 {
            return Err("memory generation unexpectedly opened a source file".into());
        }
        let started = Instant::now();
        let publication = generation.publish(&target)?;
        let publication_nanos = started.elapsed().as_nanos();
        if !publication.durable
            || target.metadata()?.len() != publication.file_bytes_written
            || publication.independent_readback_bytes != publication.file_bytes_written
        {
            return Err("durable publication byte evidence disagrees with final file".into());
        }
        let request = VortexQueryPrimitiveRequest::project(
            DatasetUri::new(target.to_string_lossy().into_owned())?,
            ProjectionRequest::columns(
                COLUMNS
                    .iter()
                    .map(|name| ColumnRef::new(*name))
                    .collect::<Result<_, _>>()?,
            ),
        )
        .with_source_order_limit(rows as usize);
        let started = Instant::now();
        let reopened = prepare_rows_in_session(&request, &session)?;
        let complete = reopened.execute()?;
        let reopen_collect_nanos = started.elapsed().as_nanos();
        verify(&complete, &fixture.expected_all)?;
        drop(complete);
        drop(reopened);
        drop(generation);
        drop(source);
        let snapshot = session.snapshot();
        if snapshot.memory.reserved_bytes != 0 || snapshot.memory.denied_reservations != 0 {
            return Err("generation acceptance retained owned bytes or denied admission".into());
        }
        let report = json!({
            "provider_version": shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION,
            "rows": rows, "iterations": iterations, "output": target,
            "query_warmups_per_variant": 1,
            "query_ordering": "alternating sequential ordinary/generation pairs; warmups excluded",
            "exact_query_and_reopened_values_verified": true, "fallback_attempted": false,
            "timing_boundary": "bind native filter/projection, execute and fully render bounded JSON; reference comparison and drop excluded",
            "typed_intake_nanos": intake_nanos, "one_native_file_generation_nanos": generation_nanos,
            "ordinary_memory_query": latency(&ordinary_samples), "memory_file_query": latency(&generation_samples),
            "durable_publish_with_hash_and_native_validation_nanos": publication_nanos,
            "reopen_and_complete_json_collect_nanos": reopen_collect_nanos,
            "generation": {
                "counter_scope": "snapshot after query warmups and samples, before publication and filesystem reopen",
                "input_logical_bytes": evidence.input_logical_bytes,
                "intake_payload_bytes_copied": evidence.intake_payload_bytes_copied,
                "segment_assembly_bytes_copied": evidence.segment_assembly_bytes_copied,
                "array_serializer_calls": evidence.array_serializer_calls,
                "dictionary_build_calls": evidence.dictionary_build_calls,
                "source_file_opens": evidence.source_file_opens,
                "memory_file_constructions": evidence.memory_file_constructions,
                "memory_segment_requests": evidence.memory_segment_requests,
                "memory_segment_bytes_returned": evidence.memory_segment_bytes_returned,
            },
            "publication": {
                "file_bytes_written": publication.file_bytes_written,
                "independent_readback_bytes": publication.independent_readback_bytes,
                "output_sha256": publication.output_sha256,
                "array_serializer_calls": publication.array_serializer_calls,
                "dictionary_build_calls": publication.dictionary_build_calls,
                "footer_serializer_calls": publication.footer_serializer_calls,
                "native_validation_file_opens": publication.validation_file_opens,
                "durable": publication.durable,
            },
            "peak_reserved_bytes": snapshot.memory.peak_reserved_bytes,
            "live_reserved_bytes_after_all_owned_results_and_sources_drop": snapshot.memory.reserved_bytes,
            "denied_reservations": snapshot.memory.denied_reservations,
            "encoding_policy": "one native Flat segment retaining typed primitive and nullable varbin encodings; no compression transform",
            "scope": "bounded immutable prototype; one segment assembly copy; no whole-file template; same segment bytes queried and persisted; provider-internal scratch/metadata and process RSS are not exhaustively counted; no speedup claim",
        });
        println!("{}", serde_json::to_string_pretty(&report)?);
        Ok(())
    }
}

#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    native::run()
}

#[cfg(not(unix))]
fn main() {
    eprintln!("immutable native memory generation currently requires Unix");
}
