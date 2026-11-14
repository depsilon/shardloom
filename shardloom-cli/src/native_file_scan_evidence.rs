//! Shared machine-readable serialization of completed native-file evidence.

use shardloom_core::native_file_scan::NativeFileScanEvidence;

const SEGMENT_REUSE_SCOPE: &str = "current_operation_polled_logical_segment_reuse;cross_call_hits_published_by_a_different_successfully_completed_native_operation;hit_segment_bytes_not_physical_io_bytes_saved";
const SESSION_SEGMENT_CACHE_SCOPE: &str = "cumulative_session_state;retained_capacity_includes_evicted_buffers_still_held_by_consumers;all_credits_included_in_memory;fixed_runtime_control_metadata_excluded";

pub(super) fn append(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    evidence: &NativeFileScanEvidence,
) {
    let sources = evidence
        .sources
        .iter()
        .enumerate()
        .map(|(source, row)| {
            serde_json::json!({
                "source": source,
                "source_uri": row.source_uri,
                "generation_fingerprint": row.generation_fingerprint,
                "segment_requests": row.segment_requests,
                "completed_read_requests": row.completed_read_requests,
                "completed_read_bytes": row.completed_read_bytes,
                "segment_reuse": {
                    "scope": SEGMENT_REUSE_SCOPE,
                    "lookups": row.segment_reuse.lookups,
                    "hits": row.segment_reuse.hits,
                    "cross_call_hits": row.segment_reuse.cross_call_hits,
                    "misses": row.segment_reuse.misses,
                    "hit_segment_bytes": row.segment_reuse.hit_segment_bytes,
                    "cross_call_hit_segment_bytes": row.segment_reuse.cross_call_hit_segment_bytes,
                    "stored_segments": row.segment_reuse.stored_segments,
                    "copied_segment_bytes": row.segment_reuse.copied_segment_bytes,
                    "retention_declines": row.segment_reuse.retention_declines,
                },
            })
        })
        .collect::<Vec<_>>();
    let contracts = evidence
        .contracts
        .iter()
        .map(|row| {
            serde_json::json!({
                "source": row.source,
                "route": row.route.as_str(),
                "source_rows": row.source_rows,
                "available_columns": row.available_columns,
                "projected_columns": row.projected_columns,
                "output_columns": row.output_columns,
                "pushed_predicate_columns": row.pushed_predicate_columns,
                "residual_predicate_columns": row.residual_predicate_columns,
                "invocations": row.invocations,
                "provider_scan_invocations": row.provider_scan_invocations,
                "delivered_rows": row.delivered_rows,
                "whole_file_pruning_checks": row.whole_file_pruning_checks,
                "whole_file_exclusions": row.whole_file_exclusions,
            })
        })
        .collect::<Vec<_>>();
    let report = serde_json::json!({
        "schema_version": "shardloom.native_file_scan_evidence.v1",
        "scope": "executed_logical_contracts_and_successful_positional_reads;read_bytes_include_repeats_and_coalescing_gaps;not_device_or_decoded_bytes;segment_requests_not_pruning_decisions;unknown_dependencies_are_null",
        "session_segment_cache_scope": SESSION_SEGMENT_CACHE_SCOPE,
        "session_segment_cache": evidence.session_segment_cache.map_or(
            serde_json::Value::Null,
            |snapshot| serde_json::json!({
                "scope": SESSION_SEGMENT_CACHE_SCOPE,
                "enabled": snapshot.enabled,
                "retained_entries": snapshot.retained_entries,
                "table_reserved_bytes": snapshot.table_reserved_bytes,
                "retained_capacity_bytes": snapshot.retained_capacity_bytes,
                "retention_limit_bytes": snapshot.retention_limit_bytes,
                "evictions": snapshot.evictions,
                "invalidations": snapshot.invalidations,
                "pressure_reclamations": snapshot.pressure_reclamations,
                "counters_overflowed": snapshot.counters_overflowed,
            }),
        ),
        "sources": sources,
        "contracts": contracts,
    });
    fields.push((format!("{prefix}_file_scans_json"), report.to_string()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::native_file_scan::{
        NativeFileScanContract, NativeFileScanRoute, NativeFileSourceObservation,
        NativeSourceSegmentCacheSnapshot, NativeSourceSegmentReuseWork,
    };

    #[test]
    fn native_file_scan_fields_preserve_names_unknown_dependencies_and_zero_reads() {
        let evidence = NativeFileScanEvidence {
            sources: vec![NativeFileSourceObservation {
                source_uri: "file:///a \"猫\".vortex".into(),
                generation_fingerprint: "ab".repeat(32),
                segment_requests: 0,
                completed_read_requests: 0,
                completed_read_bytes: 0,
                segment_reuse: NativeSourceSegmentReuseWork::default(),
            }],
            contracts: vec![NativeFileScanContract {
                source: 0,
                route: NativeFileScanRoute::MetadataCount,
                source_rows: 9,
                available_columns: 2,
                projected_columns: Some(vec![]),
                output_columns: Some(vec![]),
                pushed_predicate_columns: Some(vec![]),
                residual_predicate_columns: None,
                invocations: 1,
                provider_scan_invocations: Some(0),
                delivered_rows: 0,
                whole_file_pruning_checks: 0,
                whole_file_exclusions: 0,
            }],
            session_segment_cache: None,
        };
        let mut fields = Vec::new();
        append(&mut fields, "local_primitive_native_io", &evidence);
        assert_eq!(fields[0].0, "local_primitive_native_io_file_scans_json");
        let value: serde_json::Value = serde_json::from_str(&fields[0].1).unwrap();
        assert_eq!(
            value["sources"][0]["source_uri"],
            evidence.sources[0].source_uri
        );
        assert_eq!(value["sources"][0]["completed_read_bytes"], 0);
        assert_eq!(
            value["sources"][0]["segment_reuse"],
            serde_json::json!({
                "scope": SEGMENT_REUSE_SCOPE,
                "lookups": 0,
                "hits": 0,
                "cross_call_hits": 0,
                "misses": 0,
                "hit_segment_bytes": 0,
                "cross_call_hit_segment_bytes": 0,
                "stored_segments": 0,
                "copied_segment_bytes": 0,
                "retention_declines": 0,
            })
        );
        assert!(value["session_segment_cache"].is_null());
        assert_eq!(
            value["session_segment_cache_scope"],
            SESSION_SEGMENT_CACHE_SCOPE
        );
        assert_eq!(
            value["contracts"][0]["projected_columns"],
            serde_json::json!([])
        );
        assert!(value["contracts"][0]["residual_predicate_columns"].is_null());
        assert_eq!(value["contracts"][0]["provider_scan_invocations"], 0);
    }

    #[test]
    fn native_file_scan_serializes_nested_segment_reuse_and_session_cache_fields() {
        let evidence = NativeFileScanEvidence {
            sources: vec![NativeFileSourceObservation {
                source_uri: "file:///work.vortex".into(),
                generation_fingerprint: "cd".repeat(32),
                segment_requests: 12,
                completed_read_requests: 3,
                completed_read_bytes: 4096,
                segment_reuse: NativeSourceSegmentReuseWork {
                    lookups: 31,
                    hits: 32,
                    cross_call_hits: 38,
                    misses: 33,
                    hit_segment_bytes: 34,
                    cross_call_hit_segment_bytes: 39,
                    stored_segments: 35,
                    copied_segment_bytes: 36,
                    retention_declines: 37,
                },
            }],
            contracts: vec![],
            session_segment_cache: Some(NativeSourceSegmentCacheSnapshot {
                enabled: true,
                retained_entries: 41,
                table_reserved_bytes: 42,
                retained_capacity_bytes: 43,
                retention_limit_bytes: 44,
                evictions: 45,
                invalidations: 46,
                pressure_reclamations: 47,
                counters_overflowed: true,
            }),
        };
        let mut fields = Vec::new();

        append(&mut fields, "native_io", &evidence);

        let value: serde_json::Value = serde_json::from_str(&fields[0].1).unwrap();
        assert_eq!(
            value["sources"][0]["segment_reuse"],
            serde_json::json!({
                "scope": SEGMENT_REUSE_SCOPE,
                "lookups": 31,
                "hits": 32,
                "cross_call_hits": 38,
                "misses": 33,
                "hit_segment_bytes": 34,
                "cross_call_hit_segment_bytes": 39,
                "stored_segments": 35,
                "copied_segment_bytes": 36,
                "retention_declines": 37,
            })
        );
        assert_eq!(
            value["session_segment_cache"],
            serde_json::json!({
                "scope": SESSION_SEGMENT_CACHE_SCOPE,
                "enabled": true,
                "retained_entries": 41,
                "table_reserved_bytes": 42,
                "retained_capacity_bytes": 43,
                "retention_limit_bytes": 44,
                "evictions": 45,
                "invalidations": 46,
                "pressure_reclamations": 47,
                "counters_overflowed": true,
            })
        );
    }
}
