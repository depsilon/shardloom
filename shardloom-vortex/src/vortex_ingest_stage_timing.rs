//! Observed ingest scopes. Work spans overlap; they are not CPU or exclusive wall time.

const STAGE_NAMES: [&str; 13] = [
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
    "text_dictionary_preserve",
    "stream_arrow_ownership",
];

/// Exact copied/transferred regions and allocation charges for one batch admission.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ArrowOwnershipCounts {
    pub(crate) buffer_regions: u64,
    pub(crate) copied_buffer_bytes: u64,
    pub(crate) copied_allocation_bytes: u64,
    pub(crate) repeated_buffer_regions: u64,
    pub(crate) repeated_buffer_bytes: u64,
    pub(crate) transferred_buffer_regions: u64,
    pub(crate) transferred_buffer_bytes: u64,
    pub(crate) transferred_allocation_bytes: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct WorkSpan {
    nanos: u64,
    calls: u64,
    rows: u64,
    input_bytes: u64,
    output_bytes: u64,
}

/// Measured scopes inside the native ingest implementation, independent of wall totals.
/// Provider-stage bytes are logical estimates; the Arrow ownership span records actual
/// copied region bytes and full admitted allocation charges, not physical I/O traffic.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct VortexIngestStageReport {
    spans: [WorkSpan; 13],
    ownership_counts: ArrowOwnershipCounts,
    text_active_nanos: u64,
    identity_projection_batches: u64,
}

impl VortexIngestStageReport {
    /// Stable production evidence; no value here substitutes for end-to-end duration.
    #[must_use]
    pub fn evidence_fields(&self) -> Vec<(String, String)> {
        let mut fields = vec![
            ("vortex_ingest_stage_time_scope".into(), "summed_monotonic_elapsed_work_spans_may_overlap_not_cpu_or_exclusive_wall".into()),
            ("vortex_ingest_stage_byte_scope".into(), "provider_stage_bytes_are_array_memory_size_estimates_may_share_storage_not_allocations_or_io_zero_where_not_measured;stream_arrow_ownership_input_bytes_are_actual_copied_regions;output_bytes_are_full_new_copy_allocation_charges_including_alignment".into()),
            ("vortex_ingest_ownership_scope".into(), "copies_include_values_validity_and_children;full_new_copy_charges_include_alignment;controlled_parquet_transfer_requires_unique_standard_allocation_zero_pointer_offset_typed_alignment_and_full_retained_credit;generic_external_owners_keep_copies;repeated_regions_are_exact_held_pointer_and_length_within_one_batch;unadmitted_original_owners_and_provider_allocations_excluded;observer_map_metadata_not_allocator_charged;ownership_work_includes_observation_allocation_copy_transfer_and_validation_not_pure_memcpy".into()),
            ("vortex_ingest_ownership_buffer_regions".into(), self.ownership_counts.buffer_regions.to_string()),
            ("vortex_ingest_ownership_copied_buffer_bytes".into(), self.ownership_counts.copied_buffer_bytes.to_string()),
            ("vortex_ingest_ownership_copied_allocation_bytes".into(), self.ownership_counts.copied_allocation_bytes.to_string()),
            ("vortex_ingest_ownership_repeated_buffer_regions".into(), self.ownership_counts.repeated_buffer_regions.to_string()),
            ("vortex_ingest_ownership_repeated_buffer_bytes".into(), self.ownership_counts.repeated_buffer_bytes.to_string()),
            ("vortex_ingest_ownership_transferred_buffer_regions".into(), self.ownership_counts.transferred_buffer_regions.to_string()),
            ("vortex_ingest_ownership_transferred_buffer_bytes".into(), self.ownership_counts.transferred_buffer_bytes.to_string()),
            ("vortex_ingest_ownership_transferred_allocation_bytes".into(), self.ownership_counts.transferred_allocation_bytes.to_string()),
            ("vortex_ingest_stage_coverage".into(), "stream_validation_projection_conversion_and_nested_ownership_reader_lock_ordered_wait_selected_text_codec_numeric_probe_and_post_coalescing_numeric_codec".into()),
            ("vortex_ingest_numeric_codec_scope".into(), "non_dict_primitive_data_after_coalescing;edition_admitted_btrblocks_without_integer_or_float_dict_selection;one_job_per_leaf;global_writer_concurrency_not_bounded_here;probe_result_not_reused".into()),
            ("vortex_ingest_dictionary_probe_scope".into(), "baseline_legacy_array_session_empty_edition_whitelist_preserved;canonical_primitive_probe_omitted_when_dict_not_admitted;probe_counters_count_actual_provider_calls;other_inputs_keep_built_in_canonical_or_constant_decisions;not_full_dictionary_or_text_scheme_selection".into()),
            ("vortex_ingest_text_dictionary_preserve_scope".into(), "economical_existing_utf8_dict_within_row_block_before_repartition;native_code_compression;unchanged_value_owner_and_dictionary_epoch;one_zone_per_input;counter_excludes_statistics_and_serialization".into()),
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
        self.spans[Stage::ArrowOwnership as usize] = stream.spans[Stage::ArrowOwnership as usize];
        self.ownership_counts = stream.ownership_counts;
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
    use super::{ArrowOwnershipCounts, VortexIngestStageReport, WorkSpan};
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
        TextDictionaryPreserve,
        ArrowOwnership,
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
        let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
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
        counters: Arc<[Counter; 13]>,
        active: Arc<Mutex<ActiveState>>,
        identities: Arc<AtomicU64>,
        ownership_counts: Arc<Mutex<ArrowOwnershipCounts>>,
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
        #[cfg(any(test, feature = "universal-format-io"))]
        pub(crate) fn record_ownership(
            &self,
            elapsed: Duration,
            rows: u64,
            counts: ArrowOwnershipCounts,
        ) {
            self.record(
                Stage::ArrowOwnership,
                elapsed,
                rows,
                counts.copied_buffer_bytes,
                counts.copied_allocation_bytes,
            );
            let mut total = self
                .ownership_counts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            total.buffer_regions = total.buffer_regions.saturating_add(counts.buffer_regions);
            total.copied_buffer_bytes = total
                .copied_buffer_bytes
                .saturating_add(counts.copied_buffer_bytes);
            total.copied_allocation_bytes = total
                .copied_allocation_bytes
                .saturating_add(counts.copied_allocation_bytes);
            total.repeated_buffer_regions = total
                .repeated_buffer_regions
                .saturating_add(counts.repeated_buffer_regions);
            total.repeated_buffer_bytes = total
                .repeated_buffer_bytes
                .saturating_add(counts.repeated_buffer_bytes);
            total.transferred_buffer_regions = total
                .transferred_buffer_regions
                .saturating_add(counts.transferred_buffer_regions);
            total.transferred_buffer_bytes = total
                .transferred_buffer_bytes
                .saturating_add(counts.transferred_buffer_bytes);
            total.transferred_allocation_bytes = total
                .transferred_allocation_bytes
                .saturating_add(counts.transferred_allocation_bytes);
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
                ownership_counts: *self
                    .ownership_counts
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
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

    #[test]
    fn ownership_counts_aggregate_across_cloned_handles() {
        let timings = IngestStageTimings::default();
        let clone = timings.clone();
        timings.record_ownership(
            Duration::from_nanos(5),
            2,
            ArrowOwnershipCounts {
                buffer_regions: 3,
                copied_buffer_bytes: 40,
                copied_allocation_bytes: 48,
                repeated_buffer_regions: 1,
                repeated_buffer_bytes: 8,
                transferred_buffer_regions: 1,
                transferred_buffer_bytes: 16,
                transferred_allocation_bytes: 24,
            },
        );
        clone.record_ownership(
            Duration::from_nanos(7),
            4,
            ArrowOwnershipCounts {
                buffer_regions: 2,
                copied_buffer_bytes: 12,
                copied_allocation_bytes: 16,
                repeated_buffer_regions: 2,
                repeated_buffer_bytes: 6,
                ..ArrowOwnershipCounts::default()
            },
        );

        let report = timings.snapshot();
        assert_eq!(report.ownership_counts.buffer_regions, 5);
        assert_eq!(report.ownership_counts.copied_buffer_bytes, 52);
        assert_eq!(report.ownership_counts.copied_allocation_bytes, 64);
        assert_eq!(report.ownership_counts.repeated_buffer_regions, 3);
        assert_eq!(report.ownership_counts.repeated_buffer_bytes, 14);
        assert_eq!(report.ownership_counts.transferred_buffer_regions, 1);
        assert_eq!(report.ownership_counts.transferred_buffer_bytes, 16);
        assert_eq!(report.ownership_counts.transferred_allocation_bytes, 24);
        assert_eq!(report.spans[12].calls, 2);
        assert_eq!(report.spans[12].rows, 6);
        assert_eq!(report.spans[12].input_bytes, 52);
        assert_eq!(report.spans[12].output_bytes, 64);
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    fn with_stream_carries_ownership_without_replacing_writer_spans() {
        let writer = IngestStageTimings::default();
        writer.record(Stage::TextZstd, Duration::from_nanos(11), 9, 30, 20);
        let stream = IngestStageTimings::default();
        stream.record(Stage::Validation, Duration::from_nanos(3), 9, 10, 10);
        stream.record_ownership(
            Duration::from_nanos(5),
            9,
            ArrowOwnershipCounts {
                buffer_regions: 4,
                copied_buffer_bytes: 100,
                copied_allocation_bytes: 128,
                repeated_buffer_regions: 1,
                repeated_buffer_bytes: 16,
                ..ArrowOwnershipCounts::default()
            },
        );

        let report = writer.snapshot().with_stream(&stream.snapshot());
        assert_eq!(report.spans[0].calls, 1);
        assert_eq!(report.spans[7].calls, 1);
        assert_eq!(report.spans[7].output_bytes, 20);
        assert_eq!(report.spans[12].calls, 1);
        assert_eq!(report.spans[12].input_bytes, 100);
        assert_eq!(report.spans[12].output_bytes, 128);
        assert_eq!(report.ownership_counts.buffer_regions, 4);
        assert_eq!(report.ownership_counts.copied_buffer_bytes, 100);
        assert_eq!(report.ownership_counts.copied_allocation_bytes, 128);
        assert_eq!(report.ownership_counts.repeated_buffer_regions, 1);
        assert_eq!(report.ownership_counts.repeated_buffer_bytes, 16);
    }

    #[test]
    fn ownership_evidence_has_stable_values_and_scope() {
        let timings = IngestStageTimings::default();
        timings.record_ownership(
            Duration::from_nanos(5),
            1,
            ArrowOwnershipCounts {
                buffer_regions: 2,
                copied_buffer_bytes: 24,
                copied_allocation_bytes: 32,
                repeated_buffer_regions: 1,
                repeated_buffer_bytes: 8,
                ..ArrowOwnershipCounts::default()
            },
        );
        let fields = timings
            .snapshot()
            .evidence_fields()
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(fields["vortex_ingest_ownership_buffer_regions"], "2");
        assert_eq!(fields["vortex_ingest_ownership_copied_buffer_bytes"], "24");
        assert_eq!(
            fields["vortex_ingest_ownership_copied_allocation_bytes"],
            "32"
        );
        assert_eq!(
            fields["vortex_ingest_ownership_repeated_buffer_regions"],
            "1"
        );
        assert_eq!(fields["vortex_ingest_ownership_repeated_buffer_bytes"], "8");
        let scope = &fields["vortex_ingest_ownership_scope"];
        for token in [
            "copies_include_values_validity_and_children",
            "full_new_copy_charges_include_alignment",
            "repeated_regions_are_exact_held_pointer_and_length_within_one_batch",
            "original_owners_and_provider_allocations_excluded",
        ] {
            assert!(scope.contains(token));
        }
        assert!(fields["vortex_ingest_stage_byte_scope"].contains("actual_copied_regions"));
        assert!(
            fields["vortex_ingest_stage_byte_scope"]
                .contains("provider_stage_bytes_are_array_memory_size_estimates")
        );
    }
}

#[cfg(feature = "vortex-write")]
pub(super) use measured::{IngestStageTimings, Stage};
