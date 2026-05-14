//! Global architecture runtime-claim gate.
//!
//! This report is a side-effect-free release/readiness surface. It aggregates
//! the distributed, object-store, and lakehouse claim boundaries that are too
//! broad for a single runtime implementation slice.

use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchitectureRuntimeClaimSurface {
    DistributedCoordinatorStartup,
    DistributedWorkerStartup,
    DistributedTaskExecution,
    ObjectStoreRangeRead,
    ObjectStoreFullFileRead,
    ObjectStoreWrite,
    ObjectStoreCommit,
    LakehouseCatalogMetadata,
    LakehouseTransactionCommit,
    CdcDeleteTombstoneExecution,
}

impl ArchitectureRuntimeClaimSurface {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::DistributedCoordinatorStartup => "distributed_coordinator_startup",
            Self::DistributedWorkerStartup => "distributed_worker_startup",
            Self::DistributedTaskExecution => "distributed_task_execution",
            Self::ObjectStoreRangeRead => "object_store_range_read",
            Self::ObjectStoreFullFileRead => "object_store_full_file_read",
            Self::ObjectStoreWrite => "object_store_write",
            Self::ObjectStoreCommit => "object_store_commit",
            Self::LakehouseCatalogMetadata => "lakehouse_catalog_metadata",
            Self::LakehouseTransactionCommit => "lakehouse_transaction_commit",
            Self::CdcDeleteTombstoneExecution => "cdc_delete_tombstone_execution",
        }
    }

    #[must_use]
    pub const fn claim_family(&self) -> &'static str {
        match self {
            Self::DistributedCoordinatorStartup
            | Self::DistributedWorkerStartup
            | Self::DistributedTaskExecution => "distributed",
            Self::ObjectStoreRangeRead
            | Self::ObjectStoreFullFileRead
            | Self::ObjectStoreWrite
            | Self::ObjectStoreCommit => "object_store",
            Self::LakehouseCatalogMetadata
            | Self::LakehouseTransactionCommit
            | Self::CdcDeleteTombstoneExecution => "lakehouse",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchitectureRuntimeClaimSupportStatus {
    Blocked,
    ReportOnly,
}

impl ArchitectureRuntimeClaimSupportStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Blocked => "blocked",
            Self::ReportOnly => "report_only",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ArchitectureRuntimeClaimGateRow {
    pub surface: ArchitectureRuntimeClaimSurface,
    pub support_status: ArchitectureRuntimeClaimSupportStatus,
    pub existing_gate_ref: &'static str,
    pub required_evidence: &'static str,
    pub unsupported_diagnostic_code: &'static str,
    pub blocker_id: &'static str,
    pub claim_gate_status: &'static str,
    pub runtime_execution_allowed: bool,
    pub credential_resolution_allowed: bool,
    pub data_read_allowed: bool,
    pub object_store_io_allowed: bool,
    pub table_catalog_io_allowed: bool,
    pub write_io_allowed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl ArchitectureRuntimeClaimGateRow {
    #[must_use]
    pub const fn blocked(
        surface: ArchitectureRuntimeClaimSurface,
        existing_gate_ref: &'static str,
        required_evidence: &'static str,
        unsupported_diagnostic_code: &'static str,
        blocker_id: &'static str,
    ) -> Self {
        Self {
            surface,
            support_status: ArchitectureRuntimeClaimSupportStatus::Blocked,
            existing_gate_ref,
            required_evidence,
            unsupported_diagnostic_code,
            blocker_id,
            claim_gate_status: "not_claim_grade",
            runtime_execution_allowed: false,
            credential_resolution_allowed: false,
            data_read_allowed: false,
            object_store_io_allowed: false,
            table_catalog_io_allowed: false,
            write_io_allowed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn report_only(
        surface: ArchitectureRuntimeClaimSurface,
        existing_gate_ref: &'static str,
        required_evidence: &'static str,
        unsupported_diagnostic_code: &'static str,
        blocker_id: &'static str,
    ) -> Self {
        Self {
            support_status: ArchitectureRuntimeClaimSupportStatus::ReportOnly,
            ..Self::blocked(
                surface,
                existing_gate_ref,
                required_evidence,
                unsupported_diagnostic_code,
                blocker_id,
            )
        }
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.runtime_execution_allowed
            && !self.credential_resolution_allowed
            && !self.data_read_allowed
            && !self.object_store_io_allowed
            && !self.table_catalog_io_allowed
            && !self.write_io_allowed
            && !self.fallback_attempted
            && !self.external_engine_invoked
    }

    #[must_use]
    pub fn deterministic_diagnostic_present(&self) -> bool {
        !self.unsupported_diagnostic_code.is_empty() && !self.blocker_id.is_empty()
    }

    #[must_use]
    pub fn not_claim_grade(&self) -> bool {
        self.claim_gate_status == "not_claim_grade"
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ArchitectureRuntimeClaimGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub docs_ref: &'static str,
    pub source_refs: &'static str,
    pub support_status_vocabulary: &'static str,
    pub claim_gate_status: &'static str,
    pub rows: Vec<ArchitectureRuntimeClaimGateRow>,
    pub existing_gate_refs: Vec<&'static str>,
    pub required_gate_refs: Vec<&'static str>,
    pub release_gate_required: bool,
    pub runtime_claim_allowed: bool,
    pub distributed_runtime_claim_allowed: bool,
    pub object_store_runtime_claim_allowed: bool,
    pub lakehouse_runtime_claim_allowed: bool,
    pub public_claim_allowed: bool,
    pub coordinator_worker_start_allowed: bool,
    pub task_execution_allowed: bool,
    pub credential_resolution_allowed: bool,
    pub object_store_io_allowed: bool,
    pub table_catalog_io_allowed: bool,
    pub lakehouse_commit_allowed: bool,
    pub data_read_allowed: bool,
    pub write_io_allowed: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
}

impl ArchitectureRuntimeClaimGateReport {
    #[must_use]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.global_architecture_runtime_claim_gate.v1",
            report_id: "gar0001a.global_architecture_runtime_claim_gate",
            docs_ref: "docs/architecture/global-architecture-review.md#rfc-0001---shardloom-architecture",
            source_refs: "docs/rfcs/0001-architecture.md,docs/rfcs/0008-object-store-runtime-distributed-tasks.md,docs/rfcs/0028-output-payloads-finalization-commit-lakehouse.md,docs/architecture/operational-evidence-policy-hardening.md",
            support_status_vocabulary: "unsupported,blocked,report_only",
            claim_gate_status: "not_claim_grade",
            rows: architecture_runtime_claim_gate_rows(),
            existing_gate_refs: architecture_existing_gate_refs(),
            required_gate_refs: architecture_required_gate_refs(),
            release_gate_required: true,
            runtime_claim_allowed: false,
            distributed_runtime_claim_allowed: false,
            object_store_runtime_claim_allowed: false,
            lakehouse_runtime_claim_allowed: false,
            public_claim_allowed: false,
            coordinator_worker_start_allowed: false,
            task_execution_allowed: false,
            credential_resolution_allowed: false,
            object_store_io_allowed: false,
            table_catalog_io_allowed: false,
            lakehouse_commit_allowed: false,
            data_read_allowed: false,
            write_io_allowed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.surface.as_str()).collect()
    }

    #[must_use]
    pub fn claim_families(&self) -> Vec<&'static str> {
        vec!["distributed", "object_store", "lakehouse"]
    }

    #[must_use]
    pub fn unsupported_diagnostic_codes(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .map(|row| row.unsupported_diagnostic_code)
            .collect()
    }

    #[must_use]
    pub fn blocker_ids(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.blocker_id).collect()
    }

    #[must_use]
    pub fn required_evidence(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.required_evidence).collect()
    }

    #[must_use]
    pub fn all_rows_side_effect_free(&self) -> bool {
        self.rows
            .iter()
            .all(ArchitectureRuntimeClaimGateRow::side_effect_free)
    }

    #[must_use]
    pub fn all_rows_not_claim_grade(&self) -> bool {
        self.rows
            .iter()
            .all(ArchitectureRuntimeClaimGateRow::not_claim_grade)
    }

    #[must_use]
    pub fn deterministic_diagnostics_present(&self) -> bool {
        self.rows
            .iter()
            .all(ArchitectureRuntimeClaimGateRow::deterministic_diagnostic_present)
    }

    #[must_use]
    pub fn all_runtime_claims_blocked(&self) -> bool {
        !self.runtime_claim_allowed
            && !self.distributed_runtime_claim_allowed
            && !self.object_store_runtime_claim_allowed
            && !self.lakehouse_runtime_claim_allowed
            && !self.public_claim_allowed
            && self.claim_gate_status == "not_claim_grade"
            && self.all_rows_not_claim_grade()
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.coordinator_worker_start_allowed
            && !self.task_execution_allowed
            && !self.credential_resolution_allowed
            && !self.object_store_io_allowed
            && !self.table_catalog_io_allowed
            && !self.lakehouse_commit_allowed
            && !self.data_read_allowed
            && !self.write_io_allowed
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && !self.external_engine_invoked
            && self.all_rows_side_effect_free()
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(out, "claim_gate_status: {}", self.claim_gate_status);
        let _ = writeln!(out, "runtime_claim_allowed: {}", self.runtime_claim_allowed);
        let _ = writeln!(out, "public_claim_allowed: {}", self.public_claim_allowed);
        let _ = writeln!(out, "release_gate_required: {}", self.release_gate_required);
        let _ = writeln!(out, "side_effect_free: {}", self.side_effect_free());
        let _ = writeln!(
            out,
            "existing_gate_refs: {}",
            self.existing_gate_refs.join(",")
        );
        let _ = writeln!(out, "rows:");
        for row in &self.rows {
            let _ = writeln!(
                out,
                "  - {} [{}] family={} existing_gate_ref={} blocker_id={} claim_gate_status={} runtime_execution_allowed={} object_store_io_allowed={} table_catalog_io_allowed={} write_io_allowed={} fallback_attempted={} external_engine_invoked={}",
                row.surface.as_str(),
                row.support_status.as_str(),
                row.surface.claim_family(),
                row.existing_gate_ref,
                row.blocker_id,
                row.claim_gate_status,
                row.runtime_execution_allowed,
                row.object_store_io_allowed,
                row.table_catalog_io_allowed,
                row.write_io_allowed,
                row.fallback_attempted,
                row.external_engine_invoked,
            );
        }
        out
    }
}

fn architecture_runtime_claim_gate_rows() -> Vec<ArchitectureRuntimeClaimGateRow> {
    vec![
        ArchitectureRuntimeClaimGateRow::blocked(
            ArchitectureRuntimeClaimSurface::DistributedCoordinatorStartup,
            "cg10.object_store_runtime_promotion_gate",
            "scheduler_policy,worker_identity,checkpoint_plan,retry_policy,execution_certificate,native_io_certificate,benchmark_evidence",
            "SL_BLOCKED_DISTRIBUTED_RUNTIME",
            "gar0001a.distributed.coordinator_runtime_blocked",
        ),
        ArchitectureRuntimeClaimGateRow::blocked(
            ArchitectureRuntimeClaimSurface::DistributedWorkerStartup,
            "cg10.object_store_runtime_promotion_gate",
            "worker_identity,credential_effect_policy,checkpoint_plan,attempt_records,execution_certificate,native_io_certificate,benchmark_evidence",
            "SL_BLOCKED_DISTRIBUTED_RUNTIME",
            "gar0001a.distributed.worker_runtime_blocked",
        ),
        ArchitectureRuntimeClaimGateRow::blocked(
            ArchitectureRuntimeClaimSurface::DistributedTaskExecution,
            "cg10.object_store_runtime_promotion_gate",
            "task_attempt_records,retry_policy,idempotency_keys,cleanup_policy,execution_certificate,native_io_certificate,benchmark_evidence",
            "SL_BLOCKED_DISTRIBUTED_RUNTIME",
            "gar0001a.distributed.task_execution_blocked",
        ),
        ArchitectureRuntimeClaimGateRow::report_only(
            ArchitectureRuntimeClaimSurface::ObjectStoreRangeRead,
            "cg10.object_store_request_planner.aggregate",
            "range_planning_evidence,request_budget_policy,provider_capability_policy,credential_effect_policy,native_io_certificate,execution_certificate",
            "SL_BLOCKED_OBJECT_STORE_RANGE_READ",
            "gar0001a.object_store.range_read_blocked",
        ),
        ArchitectureRuntimeClaimGateRow::blocked(
            ArchitectureRuntimeClaimSurface::ObjectStoreFullFileRead,
            "cg10.object_store_runtime_promotion_gate",
            "provider_capability_policy,credential_effect_policy,full_file_read_budget,native_io_certificate,execution_certificate,benchmark_evidence",
            "SL_BLOCKED_OBJECT_STORE_FULL_FILE_READ",
            "gar0001a.object_store.full_file_read_blocked",
        ),
        ArchitectureRuntimeClaimGateRow::blocked(
            ArchitectureRuntimeClaimSurface::ObjectStoreWrite,
            "shardloom.object_store_commit_protocol.v1",
            "write_intent,staging_policy,provider_capability_policy,credential_effect_policy,native_io_certificate,execution_certificate,atomic_commit_evidence",
            "SL_BLOCKED_OBJECT_STORE_WRITE",
            "gar0001a.object_store.write_blocked",
        ),
        ArchitectureRuntimeClaimGateRow::blocked(
            ArchitectureRuntimeClaimSurface::ObjectStoreCommit,
            "shardloom.object_store_commit_protocol.v1",
            "atomic_commit_evidence,idempotency_keys,cleanup_policy,recovery_plan,credential_effect_policy,native_io_certificate,execution_certificate",
            "SL_BLOCKED_OBJECT_STORE_COMMIT",
            "gar0001a.object_store.commit_blocked",
        ),
        ArchitectureRuntimeClaimGateRow::report_only(
            ArchitectureRuntimeClaimSurface::LakehouseCatalogMetadata,
            "cg9.catalog_metadata_integration_gate",
            "catalog_metadata_gate,table_compatibility_report,credential_policy,metadata_cache_policy,native_io_certificate,execution_certificate",
            "SL_BLOCKED_LAKEHOUSE_CATALOG_METADATA",
            "gar0001a.lakehouse.catalog_metadata_blocked",
        ),
        ArchitectureRuntimeClaimGateRow::blocked(
            ArchitectureRuntimeClaimSurface::LakehouseTransactionCommit,
            "shardloom.table_compatibility.v1",
            "table_compatibility_report,object_store_commit_protocol,manifest_finalization_evidence,atomic_commit_evidence,recovery_plan,native_io_certificate",
            "SL_BLOCKED_LAKEHOUSE_TRANSACTION_COMMIT",
            "gar0001a.lakehouse.transaction_commit_blocked",
        ),
        ArchitectureRuntimeClaimGateRow::blocked(
            ArchitectureRuntimeClaimSurface::CdcDeleteTombstoneExecution,
            "table-compat-plan delete-semantics",
            "delete_tombstone_semantics,cdc_incremental_plan,table_compatibility_report,correctness_evidence,execution_certificate,native_io_certificate",
            "SL_BLOCKED_CDC_DELETE_TOMBSTONE_EXECUTION",
            "gar0001a.lakehouse.cdc_delete_tombstone_blocked",
        ),
    ]
}

fn architecture_existing_gate_refs() -> Vec<&'static str> {
    vec![
        "cg9.catalog_metadata_integration_gate",
        "cg10.object_store_runtime_promotion_gate",
        "cg10.object_store_request_planner.aggregate",
        "shardloom.object_store_commit_protocol.v1",
        "shardloom.table_compatibility.v1",
        "table-compat-plan delete-semantics",
    ]
}

fn architecture_required_gate_refs() -> Vec<&'static str> {
    vec![
        "execution_certificate",
        "native_io_certificate",
        "policy_no_fallback",
        "credential_effect_policy",
        "benchmark_evidence",
        "object_store_commit_protocol",
        "catalog_metadata_gate",
        "table_compatibility_report",
        "release_readiness_gate",
    ]
}

#[must_use]
pub fn plan_global_architecture_runtime_claim_gate() -> ArchitectureRuntimeClaimGateReport {
    ArchitectureRuntimeClaimGateReport::report_only()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_architecture_runtime_claim_gate_blocks_broad_runtime_claims() {
        let report = plan_global_architecture_runtime_claim_gate();

        assert_eq!(
            report.schema_version,
            "shardloom.global_architecture_runtime_claim_gate.v1"
        );
        assert_eq!(report.rows.len(), 10);
        assert_eq!(
            report.row_order(),
            vec![
                "distributed_coordinator_startup",
                "distributed_worker_startup",
                "distributed_task_execution",
                "object_store_range_read",
                "object_store_full_file_read",
                "object_store_write",
                "object_store_commit",
                "lakehouse_catalog_metadata",
                "lakehouse_transaction_commit",
                "cdc_delete_tombstone_execution",
            ]
        );
        assert_eq!(
            report.claim_families(),
            vec!["distributed", "object_store", "lakehouse"]
        );
        assert!(report.release_gate_required);
        assert!(report.all_runtime_claims_blocked());
        assert!(report.side_effect_free());
        assert!(report.deterministic_diagnostics_present());
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
        assert!(!report.object_store_io_allowed);
        assert!(!report.table_catalog_io_allowed);
        assert!(!report.lakehouse_commit_allowed);
    }
}
