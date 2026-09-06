//! Observed ingest scopes. Work spans overlap; they are not CPU or exclusive wall time.

const STAGE_NAMES: [&str; 11] = [
    "stream_validation",
    "stream_projection",
    "stream_arrow_conversion",
    "stream_reader_lock_wait",
    "stream_ordered_handoff_wait",
    "text_canonicalize",
    "text_compact",
    "text_zstd",
    "numeric_probe",
    "numeric_compress",
    "numeric_preserve",
];

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct WorkSpan {
    nanos: u64,
    calls: u64,
    rows: u64,
    input_bytes: u64,
    output_bytes: u64,
}

/// Measured scopes inside the native ingest implementation, independent of wall totals.
/// Bytes describe logical input/output buffers, not allocation or physical I/O traffic.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct VortexIngestStageReport {
    spans: [WorkSpan; 11],
    text_active_nanos: u64,
    identity_projection_batches: u64,
}

impl VortexIngestStageReport {
    /// Stable production evidence; no value here substitutes for end-to-end duration.
    #[must_use]
    pub fn evidence_fields(&self) -> Vec<(String, String)> {
        let mut fields = vec![
            ("vortex_ingest_stage_time_scope".into(), "summed_monotonic_elapsed_work_spans_may_overlap_not_cpu_or_exclusive_wall".into()),
            ("vortex_ingest_stage_byte_scope".into(), "provider_array_memory_size_estimates_may_share_storage_not_allocations_or_io_zero_where_not_measured".into()),
            ("vortex_ingest_stage_coverage".into(), "stream_validation_projection_conversion_reader_lock_ordered_wait_selected_text_codec_numeric_probe_and_post_coalescing_numeric_codec".into()),
            ("vortex_ingest_numeric_codec_scope".into(), "non_dict_primitive_data_after_coalescing;edition_admitted_btrblocks_without_integer_or_float_dict_selection;one_job_per_leaf;global_writer_concurrency_not_bounded_here;probe_result_not_reused".into()),
            ("vortex_ingest_dictionary_probe_scope".into(), "baseline_legacy_array_session_empty_edition_whitelist_preserved;built_in_canonical_or_constant_decisions_only;not_full_dictionary_or_text_scheme_selection".into()),
            ("vortex_ingest_legacy_encode_write_scope".into(), "measured_inclusive_provider_writer_wall_including_compression_not_exclusive_io".into()),
            ("vortex_ingest_legacy_encode_write_semantics".into(), "v2_inclusive_wall;historical_wall_minus_summed_compression_values_not_comparable".into()),
            ("vortex_ingest_legacy_stream_conversion_scope".into(), "validation_and_conversion_work_first_batch_also_includes_writer_admission_and_target_preparation".into()),
            ("vortex_ingest_legacy_stream_decode_scope".into(), "source_reader_pull_elapsed_minus_derived_build_includes_source_wait_not_decode_cpu".into()),
            ("vortex_ingest_text_active_scope".into(), "union_of_selected_text_canonicalize_compact_zstd_scopes_not_cpu_time_not_all_writer_work".into()),
            ("vortex_ingest_text_active_nanos".into(), self.text_active_nanos.to_string()),
            ("vortex_ingest_identity_projection_batches".into(), self.identity_projection_batches.to_string()),
        ];
        for (name, span) in STAGE_NAMES.iter().zip(&self.spans) {
            for (suffix, value) in [
                ("work_nanos", span.nanos),
                ("calls", span.calls),
                ("rows", span.rows),
                ("input_bytes", span.input_bytes),
                ("output_bytes", span.output_bytes),
            ] {
                fields.push((format!("vortex_ingest_{name}_{suffix}"), value.to_string()));
            }
        }
        fields
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub(super) fn with_stream(mut self, stream: &Self) -> Self {
        self.spans[..5].copy_from_slice(&stream.spans[..5]);
        self.identity_projection_batches = stream.identity_projection_batches;
        self
    }
}

#[test]
fn legacy_writer_field_declares_corrected_inclusive_wall_semantics() {
    let fields = VortexIngestStageReport::default()
        .evidence_fields()
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        fields["vortex_ingest_legacy_encode_write_scope"],
        "measured_inclusive_provider_writer_wall_including_compression_not_exclusive_io"
    );
    assert!(
        fields["vortex_ingest_legacy_encode_write_semantics"]
            .contains("historical_wall_minus_summed_compression_values_not_comparable")
    );
}

#[cfg(feature = "vortex-write")]
mod measured {
    use super::{VortexIngestStageReport, WorkSpan};
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, Instant},
    };

    #[derive(Clone, Copy)]
    #[cfg_attr(not(feature = "universal-format-io"), allow(dead_code))]
    pub(crate) enum Stage {
        Validation,
        Projection,
        ArrowConversion,
        ReaderLockWait,
        OrderedHandoffWait,
        TextCanonicalize,
        TextCompact,
        TextZstd,
        NumericProbe,
        NumericCompress,
        NumericPreserve,
    }

    #[derive(Debug, Default)]
    struct Counter {
        nanos: AtomicU64,
        calls: AtomicU64,
        rows: AtomicU64,
        input_bytes: AtomicU64,
        output_bytes: AtomicU64,
    }

    fn add(counter: &AtomicU64, value: u64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
            Some(old.saturating_add(value))
        });
    }

    #[derive(Debug, Default)]
    struct ActiveState {
        active: usize,
        since: Option<Instant>,
        nanos: u64,
    }

    impl ActiveState {
        fn begin(&mut self, now: Instant) {
            if self.active == 0 {
                self.since = Some(now);
            }
            self.active += 1;
        }
        fn end(&mut self, now: Instant) {
            self.active -= 1;
            if self.active == 0
                && let Some(start) = self.since.take()
            {
                self.nanos = self.nanos.saturating_add(nanos(now.duration_since(start)));
            }
        }
    }

    fn nanos(duration: Duration) -> u64 {
        u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
    }

    #[derive(Debug, Default, Clone)]
    pub(crate) struct IngestStageTimings {
        counters: Arc<[Counter; 11]>,
        active: Arc<Mutex<ActiveState>>,
        identities: Arc<AtomicU64>,
    }

    impl IngestStageTimings {
        pub(crate) fn record(
            &self,
            stage: Stage,
            elapsed: Duration,
            rows: u64,
            input_bytes: u64,
            output_bytes: u64,
        ) {
            let counter = &self.counters[stage as usize];
            add(&counter.nanos, nanos(elapsed));
            add(&counter.calls, 1);
            add(&counter.rows, rows);
            add(&counter.input_bytes, input_bytes);
            add(&counter.output_bytes, output_bytes);
        }
        #[cfg(feature = "universal-format-io")]
        pub(crate) fn identity_projection(&self) {
            add(&self.identities, 1);
        }
        pub(crate) fn text_scope(&self) -> ActiveGuard {
            self.active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .begin(Instant::now());
            ActiveGuard(Arc::clone(&self.active))
        }
        pub(crate) fn snapshot(&self) -> VortexIngestStageReport {
            VortexIngestStageReport {
                spans: std::array::from_fn(|index| {
                    let counter = &self.counters[index];
                    WorkSpan {
                        nanos: counter.nanos.load(Ordering::Relaxed),
                        calls: counter.calls.load(Ordering::Relaxed),
                        rows: counter.rows.load(Ordering::Relaxed),
                        input_bytes: counter.input_bytes.load(Ordering::Relaxed),
                        output_bytes: counter.output_bytes.load(Ordering::Relaxed),
                    }
                }),
                text_active_nanos: self
                    .active
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .nanos,
                identity_projection_batches: self.identities.load(Ordering::Relaxed),
            }
        }
    }

    pub(crate) struct ActiveGuard(Arc<Mutex<ActiveState>>);
    impl Drop for ActiveGuard {
        fn drop(&mut self) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .end(Instant::now());
        }
    }

    #[test]
    fn overlapping_work_is_not_subtracted_from_wall_time() {
        let now = Instant::now();
        let mut active = ActiveState::default();
        active.begin(now);
        active.begin(now + Duration::from_nanos(10));
        active.end(now + Duration::from_nanos(30));
        active.end(now + Duration::from_nanos(40));
        active.begin(now + Duration::from_nanos(70));
        active.end(now + Duration::from_nanos(80));
        assert_eq!(active.nanos, 50); // 40ns union plus 10ns; not 70ns summed work.
        assert_eq!(active.active, 0);
    }
}

#[cfg(feature = "vortex-write")]
pub(super) use measured::{IngestStageTimings, Stage};
