//! Table intelligence planning contracts.
//!
//! This module aggregates existing report-only table, schema, CDC, layout, and
//! compaction surfaces. It does not read catalogs, table metadata, object stores,
//! or data files, and it does not implement table-format runtime behavior.

#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::struct_excessive_bools
)]

use crate::{
    ByteRange, CatalogKind, CatalogRef, CdcEventKind, CdcEventSummary,
    CdcIncrementalPlanningReport, ChangeSet, ColumnRef, DatasetFormat, DatasetManifest, DatasetRef,
    DatasetUri, DeleteModel, Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity,
    EncodedSegment, EncodingKind, FallbackStatus, FieldId, FieldName, FieldPath, FileDescriptor,
    FileRole, LayoutKind, LogicalDType, ManifestId, ManifestSegment, Nullability, PartitionField,
    PartitionSpec, PartitionTransform, Result, SchemaDefinition, SchemaField, SchemaId,
    SchemaVersion, SegmentChange, SegmentChangeKind, SegmentId, SegmentLayout, SegmentStats,
    SnapshotId, SnapshotRef, evaluate_cdc_incremental_planning,
};
use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableIntelligenceSurfaceKind {
    SchemaEvolution,
    PartitionEvolution,
    DeleteTombstone,
    TableCompatibility,
    CdcIncremental,
    LayoutHealth,
    Compaction,
    SnapshotManifest,
    CatalogCompatibility,
    CommitRecovery,
}

impl TableIntelligenceSurfaceKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SchemaEvolution => "schema_evolution",
            Self::PartitionEvolution => "partition_evolution",
            Self::DeleteTombstone => "delete_tombstone",
            Self::TableCompatibility => "table_compatibility",
            Self::CdcIncremental => "cdc_incremental",
            Self::LayoutHealth => "layout_health",
            Self::Compaction => "compaction",
            Self::SnapshotManifest => "snapshot_manifest",
            Self::CatalogCompatibility => "catalog_compatibility",
            Self::CommitRecovery => "commit_recovery",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableIntelligenceSurfaceStatus {
    ReportOnlyAvailable,
    Planned,
    Deferred,
}

impl TableIntelligenceSurfaceStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnlyAvailable => "report_only_available",
            Self::Planned => "planned",
            Self::Deferred => "deferred",
        }
    }

    #[must_use]
    pub const fn is_available(&self) -> bool {
        matches!(self, Self::ReportOnlyAvailable)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableIntelligenceSurface {
    pub kind: TableIntelligenceSurfaceKind,
    pub status: TableIntelligenceSurfaceStatus,
    pub command: Option<&'static str>,
    pub schema_version: &'static str,
    pub required_for_cg9: bool,
    pub requires_snapshot_boundary: bool,
    pub performs_catalog_io: bool,
    pub performs_table_metadata_io: bool,
    pub performs_data_io: bool,
    pub performs_write_io: bool,
    pub fallback_execution_allowed: bool,
}

impl TableIntelligenceSurface {
    #[must_use]
    pub const fn report_only(
        kind: TableIntelligenceSurfaceKind,
        command: Option<&'static str>,
        schema_version: &'static str,
        required_for_cg9: bool,
        requires_snapshot_boundary: bool,
    ) -> Self {
        Self {
            kind,
            status: TableIntelligenceSurfaceStatus::ReportOnlyAvailable,
            command,
            schema_version,
            required_for_cg9,
            requires_snapshot_boundary,
            performs_catalog_io: false,
            performs_table_metadata_io: false,
            performs_data_io: false,
            performs_write_io: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn planned(
        kind: TableIntelligenceSurfaceKind,
        command: Option<&'static str>,
        schema_version: &'static str,
        required_for_cg9: bool,
        requires_snapshot_boundary: bool,
    ) -> Self {
        Self {
            kind,
            status: TableIntelligenceSurfaceStatus::Planned,
            command,
            schema_version,
            required_for_cg9,
            requires_snapshot_boundary,
            performs_catalog_io: false,
            performs_table_metadata_io: false,
            performs_data_io: false,
            performs_write_io: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.performs_catalog_io
            && !self.performs_table_metadata_io
            && !self.performs_data_io
            && !self.performs_write_io
            && !self.fallback_execution_allowed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableIntelligenceReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub surfaces: Vec<TableIntelligenceSurface>,
    pub compatibility_profiles: Vec<&'static str>,
    pub catalog_io_performed: bool,
    pub table_metadata_io_performed: bool,
    pub data_io_performed: bool,
    pub write_io_performed: bool,
    pub external_table_format_dependency_added: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl TableIntelligenceReport {
    #[must_use]
    pub fn report_only_foundation() -> Self {
        Self {
            schema_version: "shardloom.table_intelligence.v1",
            report_id: "cg9.table_intelligence.foundation",
            surfaces: vec![
                TableIntelligenceSurface::report_only(
                    TableIntelligenceSurfaceKind::SchemaEvolution,
                    Some("schema-plan evolution"),
                    "shardloom.schema_evolution_compatibility.v1",
                    true,
                    false,
                ),
                TableIntelligenceSurface::report_only(
                    TableIntelligenceSurfaceKind::PartitionEvolution,
                    Some("table-compat-plan partition-evolution"),
                    "shardloom.partition_evolution_compatibility.v1",
                    true,
                    true,
                ),
                TableIntelligenceSurface::report_only(
                    TableIntelligenceSurfaceKind::DeleteTombstone,
                    Some("table-compat-plan delete-semantics"),
                    "shardloom.delete_tombstone_compatibility.v1",
                    true,
                    true,
                ),
                TableIntelligenceSurface::report_only(
                    TableIntelligenceSurfaceKind::TableCompatibility,
                    Some("table-compat-plan aggregate"),
                    "shardloom.table_compatibility.v1",
                    true,
                    true,
                ),
                TableIntelligenceSurface::report_only(
                    TableIntelligenceSurfaceKind::CdcIncremental,
                    Some("incremental-plan cdc"),
                    "shardloom.cdc_incremental_planning.v1",
                    true,
                    true,
                ),
                TableIntelligenceSurface::report_only(
                    TableIntelligenceSurfaceKind::LayoutHealth,
                    Some("layout-health-plan"),
                    "shardloom.layout_health.v1",
                    true,
                    false,
                ),
                TableIntelligenceSurface::report_only(
                    TableIntelligenceSurfaceKind::Compaction,
                    Some("compaction-plan"),
                    "shardloom.compaction_planning.v1",
                    true,
                    false,
                ),
                TableIntelligenceSurface::planned(
                    TableIntelligenceSurfaceKind::SnapshotManifest,
                    Some("manifest-plan,incremental-plan"),
                    "shardloom.dataset_manifest.v1",
                    true,
                    true,
                ),
                TableIntelligenceSurface::planned(
                    TableIntelligenceSurfaceKind::CatalogCompatibility,
                    None,
                    "shardloom.catalog_compatibility.v1",
                    true,
                    true,
                ),
                TableIntelligenceSurface::planned(
                    TableIntelligenceSurfaceKind::CommitRecovery,
                    Some("recovery-plan"),
                    "shardloom.recovery_plan.v1",
                    true,
                    true,
                ),
            ],
            compatibility_profiles: vec![
                "native_vortex",
                "iceberg_compatible",
                "delta_compatible",
                "hudi_like",
                "hive_style_partitions",
            ],
            catalog_io_performed: false,
            table_metadata_io_performed: false,
            data_io_performed: false,
            write_io_performed: false,
            external_table_format_dependency_added: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn surface_order(&self) -> Vec<&'static str> {
        self.surfaces
            .iter()
            .map(|surface| surface.kind.as_str())
            .collect()
    }

    #[must_use]
    pub fn report_only_available_surface_count(&self) -> usize {
        self.surfaces
            .iter()
            .filter(|surface| surface.status.is_available())
            .count()
    }

    #[must_use]
    pub fn required_cg9_surface_count(&self) -> usize {
        self.surfaces
            .iter()
            .filter(|surface| surface.required_for_cg9)
            .count()
    }

    #[must_use]
    pub fn snapshot_boundary_surface_count(&self) -> usize {
        self.surfaces
            .iter()
            .filter(|surface| surface.requires_snapshot_boundary)
            .count()
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.catalog_io_performed
            && !self.table_metadata_io_performed
            && !self.data_io_performed
            && !self.write_io_performed
            && !self.external_table_format_dependency_added
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && self
                .surfaces
                .iter()
                .all(TableIntelligenceSurface::side_effect_free)
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(
            out,
            "compatibility profiles: {}",
            self.compatibility_profiles.join(",")
        );
        let _ = writeln!(out, "catalog io performed: {}", self.catalog_io_performed);
        let _ = writeln!(
            out,
            "table metadata io performed: {}",
            self.table_metadata_io_performed
        );
        let _ = writeln!(out, "data io performed: {}", self.data_io_performed);
        let _ = writeln!(out, "write io performed: {}", self.write_io_performed);
        let _ = writeln!(
            out,
            "external table-format dependency added: {}",
            self.external_table_format_dependency_added
        );
        let _ = writeln!(
            out,
            "fallback execution allowed: {}",
            self.fallback_execution_allowed
        );
        let _ = writeln!(out, "surfaces:");
        for surface in &self.surfaces {
            let _ = writeln!(
                out,
                "  - {} [{}] command={} schema={} cg9_required={} snapshot_boundary={} side_effect_free={}",
                surface.kind.as_str(),
                surface.status.as_str(),
                surface.command.unwrap_or("none"),
                surface.schema_version,
                surface.required_for_cg9,
                surface.requires_snapshot_boundary,
                surface.side_effect_free()
            );
        }
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableMaintenanceExecutionFamily {
    DeleteTombstone,
    Cdc,
    MaintenanceWrite,
}

impl TableMaintenanceExecutionFamily {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::DeleteTombstone => "delete_tombstone",
            Self::Cdc => "cdc",
            Self::MaintenanceWrite => "maintenance_write",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableMaintenanceExecutionOperation {
    FileLevelDeleteCompatibility,
    SegmentTombstoneExecution,
    RowLevelDeleteExecution,
    PositionDeleteExecution,
    EqualityDeleteExecution,
    CdcAppendOnlyPlanning,
    CdcMetadataOnlyPlanning,
    CdcUpdateDeleteTombstoneExecution,
    CompactionPlanning,
    CompactionExecutionWrite,
    TableMetadataWrite,
    TableMaintenanceCommit,
}

impl TableMaintenanceExecutionOperation {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FileLevelDeleteCompatibility => "file_level_delete_compatibility",
            Self::SegmentTombstoneExecution => "segment_tombstone_execution",
            Self::RowLevelDeleteExecution => "row_level_delete_execution",
            Self::PositionDeleteExecution => "position_delete_execution",
            Self::EqualityDeleteExecution => "equality_delete_execution",
            Self::CdcAppendOnlyPlanning => "cdc_append_only_planning",
            Self::CdcMetadataOnlyPlanning => "cdc_metadata_only_planning",
            Self::CdcUpdateDeleteTombstoneExecution => "cdc_update_delete_tombstone_execution",
            Self::CompactionPlanning => "compaction_planning",
            Self::CompactionExecutionWrite => "compaction_execution_write",
            Self::TableMetadataWrite => "table_metadata_write",
            Self::TableMaintenanceCommit => "table_maintenance_commit",
        }
    }

    #[must_use]
    pub const fn diagnostic_code(&self) -> DiagnosticCode {
        match self {
            Self::TableMaintenanceCommit => DiagnosticCode::CommitNotAtomic,
            _ => DiagnosticCode::NotImplemented,
        }
    }

    #[must_use]
    pub const fn diagnostic_category(&self) -> DiagnosticCategory {
        match self {
            Self::FileLevelDeleteCompatibility
            | Self::CdcAppendOnlyPlanning
            | Self::CdcMetadataOnlyPlanning
            | Self::CompactionPlanning => DiagnosticCategory::Planning,
            Self::TableMaintenanceCommit
            | Self::CompactionExecutionWrite
            | Self::TableMetadataWrite
            | Self::SegmentTombstoneExecution
            | Self::RowLevelDeleteExecution
            | Self::PositionDeleteExecution
            | Self::EqualityDeleteExecution
            | Self::CdcUpdateDeleteTombstoneExecution => DiagnosticCategory::Execution,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableMaintenanceExecutionStatus {
    ReportOnlyAvailable,
    UnsupportedUntilCertified,
}

impl TableMaintenanceExecutionStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnlyAvailable => "report_only_available",
            Self::UnsupportedUntilCertified => "unsupported_until_certified",
        }
    }

    #[must_use]
    pub const fn is_report_only(&self) -> bool {
        matches!(self, Self::ReportOnlyAvailable)
    }

    #[must_use]
    pub const fn is_unsupported(&self) -> bool {
        matches!(self, Self::UnsupportedUntilCertified)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMaintenanceExecutionMatrixRow {
    pub family: TableMaintenanceExecutionFamily,
    pub operation: TableMaintenanceExecutionOperation,
    pub status: TableMaintenanceExecutionStatus,
    pub existing_report_ref: Option<&'static str>,
    pub required_fixture: &'static str,
    pub required_commit_semantics: &'static str,
    pub required_evidence: &'static str,
    pub report_only_available: bool,
    pub runtime_execution_allowed: bool,
    pub delete_tombstone_execution_allowed: bool,
    pub cdc_execution_allowed: bool,
    pub maintenance_write_allowed: bool,
    pub catalog_io: bool,
    pub table_metadata_io: bool,
    pub data_io: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub support_status: &'static str,
    pub claim_gate_status: &'static str,
}

impl TableMaintenanceExecutionMatrixRow {
    #[must_use]
    pub const fn report_only(
        family: TableMaintenanceExecutionFamily,
        operation: TableMaintenanceExecutionOperation,
        existing_report_ref: &'static str,
        required_fixture: &'static str,
        required_commit_semantics: &'static str,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            family,
            operation,
            status: TableMaintenanceExecutionStatus::ReportOnlyAvailable,
            existing_report_ref: Some(existing_report_ref),
            required_fixture,
            required_commit_semantics,
            required_evidence,
            report_only_available: true,
            runtime_execution_allowed: false,
            delete_tombstone_execution_allowed: false,
            cdc_execution_allowed: false,
            maintenance_write_allowed: false,
            catalog_io: false,
            table_metadata_io: false,
            data_io: false,
            object_store_io: false,
            write_io: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            external_engine_invoked: false,
            support_status: "report_only",
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn unsupported(
        family: TableMaintenanceExecutionFamily,
        operation: TableMaintenanceExecutionOperation,
        required_fixture: &'static str,
        required_commit_semantics: &'static str,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            family,
            operation,
            status: TableMaintenanceExecutionStatus::UnsupportedUntilCertified,
            existing_report_ref: None,
            required_fixture,
            required_commit_semantics,
            required_evidence,
            report_only_available: false,
            runtime_execution_allowed: false,
            delete_tombstone_execution_allowed: false,
            cdc_execution_allowed: false,
            maintenance_write_allowed: false,
            catalog_io: false,
            table_metadata_io: false,
            data_io: false,
            object_store_io: false,
            write_io: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            external_engine_invoked: false,
            support_status: "unsupported",
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.runtime_execution_allowed
            && !self.delete_tombstone_execution_allowed
            && !self.cdc_execution_allowed
            && !self.maintenance_write_allowed
            && !self.catalog_io
            && !self.table_metadata_io
            && !self.data_io
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && !self.external_engine_invoked
    }

    #[must_use]
    pub fn to_diagnostic(&self) -> Option<Diagnostic> {
        if !self.status.is_unsupported() {
            return None;
        }
        Some(Diagnostic::new(
            self.operation.diagnostic_code(),
            DiagnosticSeverity::Info,
            self.operation.diagnostic_category(),
            format!("{} is unsupported until certified", self.operation.as_str()),
            Some(self.operation.as_str().to_string()),
            Some(format!(
                "{} requires fixture={}, commit_semantics={}, evidence={} before runtime promotion.",
                self.operation.as_str(),
                self.required_fixture,
                self.required_commit_semantics,
                self.required_evidence
            )),
            Some(
                "Keep this table operation report-only or unsupported until dedicated evidence lands."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMaintenanceExecutionMatrixReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub gar_id: &'static str,
    pub support_status: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub rows: Vec<TableMaintenanceExecutionMatrixRow>,
    pub existing_report_refs: Vec<&'static str>,
    pub delete_tombstone_compatibility_report_present: bool,
    pub cdc_incremental_planning_present: bool,
    pub layout_compaction_planning_present: bool,
    pub local_metadata_smoke_present: bool,
    pub local_delete_tombstone_smoke_present: bool,
    pub local_append_only_cdc_overlay_smoke_present: bool,
    pub local_table_append_commit_rehearsal_smoke_present: bool,
    pub fixture_metadata_required: bool,
    pub row_identity_required: bool,
    pub delete_tombstone_policy_required: bool,
    pub commit_semantics_required: bool,
    pub table_metadata_schema_required: bool,
    pub execution_certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub materialization_decode_evidence_required: bool,
    pub no_fallback_policy_required: bool,
    pub runtime_execution_allowed: bool,
    pub delete_tombstone_execution_allowed: bool,
    pub cdc_execution_allowed: bool,
    pub maintenance_write_allowed: bool,
    pub catalog_io_allowed: bool,
    pub table_metadata_io_allowed: bool,
    pub data_io_allowed: bool,
    pub object_store_io_allowed: bool,
    pub write_io_allowed: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub table_format_execution_claim_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl TableMaintenanceExecutionMatrixReport {
    #[must_use]
    pub fn planning_default() -> Self {
        let rows = table_maintenance_execution_matrix_rows();
        let diagnostics = rows
            .iter()
            .filter_map(TableMaintenanceExecutionMatrixRow::to_diagnostic)
            .collect();
        Self {
            schema_version: "shardloom.table_maintenance_execution_matrix.v1",
            report_id: "gar0020b.table_maintenance_execution_matrix",
            gar_id: "GAR-0020-B",
            support_status: "report_only_with_unsupported_runtime_paths",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "delete/tombstone, CDC, compaction, and maintenance-write execution are not table-format runtime claims; existing rows are compatibility/planning evidence only",
            rows,
            existing_report_refs: table_maintenance_execution_existing_report_refs(),
            delete_tombstone_compatibility_report_present: true,
            cdc_incremental_planning_present: true,
            layout_compaction_planning_present: true,
            local_metadata_smoke_present: true,
            local_delete_tombstone_smoke_present: true,
            local_append_only_cdc_overlay_smoke_present: true,
            local_table_append_commit_rehearsal_smoke_present: true,
            fixture_metadata_required: true,
            row_identity_required: true,
            delete_tombstone_policy_required: true,
            commit_semantics_required: true,
            table_metadata_schema_required: true,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            materialization_decode_evidence_required: true,
            no_fallback_policy_required: true,
            runtime_execution_allowed: false,
            delete_tombstone_execution_allowed: false,
            cdc_execution_allowed: false,
            maintenance_write_allowed: false,
            catalog_io_allowed: false,
            table_metadata_io_allowed: false,
            data_io_allowed: false,
            object_store_io_allowed: false,
            write_io_allowed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            external_engine_invoked: false,
            table_format_execution_claim_allowed: false,
            diagnostics,
        }
    }

    #[must_use]
    pub fn operation_count(&self) -> usize {
        self.rows.len()
    }

    #[must_use]
    pub fn report_only_operation_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.status.is_report_only())
            .count()
    }

    #[must_use]
    pub fn unsupported_operation_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.status.is_unsupported())
            .count()
    }

    #[must_use]
    pub fn operation_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.operation.as_str()).collect()
    }

    #[must_use]
    pub fn family_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.family.as_str()).collect()
    }

    #[must_use]
    pub fn runtime_promotions_blocked(&self) -> bool {
        !self.runtime_execution_allowed
            && !self.delete_tombstone_execution_allowed
            && !self.cdc_execution_allowed
            && !self.maintenance_write_allowed
            && !self.catalog_io_allowed
            && !self.table_metadata_io_allowed
            && !self.data_io_allowed
            && !self.object_store_io_allowed
            && !self.write_io_allowed
            && self
                .rows
                .iter()
                .all(TableMaintenanceExecutionMatrixRow::side_effect_free)
    }

    #[must_use]
    pub fn claim_blocked(&self) -> bool {
        !self.table_format_execution_claim_allowed && self.claim_gate_status == "not_claim_grade"
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        self.runtime_promotions_blocked()
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && !self.external_engine_invoked
    }

    #[must_use]
    pub fn unsupported_diagnostic_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.status.is_unsupported())
            .filter(|row| {
                self.diagnostics.iter().any(|diagnostic| {
                    diagnostic.code == row.operation.diagnostic_code()
                        && diagnostic.category == row.operation.diagnostic_category()
                        && diagnostic.severity == DiagnosticSeverity::Info
                        && diagnostic.feature.as_deref() == Some(row.operation.as_str())
                        && !diagnostic.fallback.attempted
                        && !diagnostic.fallback.allowed
                })
            })
            .count()
    }

    #[must_use]
    pub fn deterministic_unsupported_diagnostics_ready(&self) -> bool {
        self.unsupported_operation_count() > 0
            && self.unsupported_diagnostic_count() == self.unsupported_operation_count()
    }

    #[must_use]
    pub fn unsupported_diagnostic_code_order(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.status.is_unsupported())
            .map(|row| row.operation.diagnostic_code().as_str())
            .collect()
    }

    #[must_use]
    pub fn unsupported_diagnostic_category_order(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.status.is_unsupported())
            .map(|row| row.operation.diagnostic_category().as_str())
            .collect()
    }

    #[must_use]
    pub fn unsupported_diagnostic_severity_order(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.status.is_unsupported())
            .map(|_| DiagnosticSeverity::Info.as_str())
            .collect()
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || !self.claim_blocked()
            || !self.deterministic_unsupported_diagnostics_ready()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(out, "gar_id: {}", self.gar_id);
        let _ = writeln!(out, "support_status: {}", self.support_status);
        let _ = writeln!(out, "claim_gate_status: {}", self.claim_gate_status);
        let _ = writeln!(out, "claim_boundary: {}", self.claim_boundary);
        let _ = writeln!(
            out,
            "runtime promotions blocked: {}",
            self.runtime_promotions_blocked()
        );
        let _ = writeln!(
            out,
            "deterministic unsupported diagnostics ready: {}",
            self.deterministic_unsupported_diagnostics_ready()
        );
        let _ = writeln!(out, "side effect free: {}", self.side_effect_free());
        let _ = writeln!(out, "operations:");
        for row in &self.rows {
            let _ = writeln!(
                out,
                "  - {}:{} [{}] existing_ref={} required_fixture={} commit_semantics={} runtime_allowed=false fallback_attempted=false external_engine_invoked=false claim_gate_status={}",
                row.family.as_str(),
                row.operation.as_str(),
                row.status.as_str(),
                row.existing_report_ref.unwrap_or("none"),
                row.required_fixture,
                row.required_commit_semantics,
                row.claim_gate_status
            );
        }
        out
    }
}

fn table_maintenance_execution_matrix_rows() -> Vec<TableMaintenanceExecutionMatrixRow> {
    vec![
        TableMaintenanceExecutionMatrixRow::report_only(
            TableMaintenanceExecutionFamily::DeleteTombstone,
            TableMaintenanceExecutionOperation::FileLevelDeleteCompatibility,
            "shardloom.delete_tombstone_compatibility.v1",
            "declared_delete_model_fixture",
            "none_report_only",
            "delete_tombstone_compatibility_report,no_fallback_policy",
        ),
        TableMaintenanceExecutionMatrixRow::unsupported(
            TableMaintenanceExecutionFamily::DeleteTombstone,
            TableMaintenanceExecutionOperation::SegmentTombstoneExecution,
            "segment_tombstone_fixture,row_identity",
            "native_tombstone_filter_rule",
            "correctness_evidence,execution_certificate,native_io_certificate",
        ),
        TableMaintenanceExecutionMatrixRow::unsupported(
            TableMaintenanceExecutionFamily::DeleteTombstone,
            TableMaintenanceExecutionOperation::RowLevelDeleteExecution,
            "row_level_delete_fixture,row_identity",
            "native_row_delete_rule",
            "correctness_evidence,execution_certificate,native_io_certificate",
        ),
        TableMaintenanceExecutionMatrixRow::unsupported(
            TableMaintenanceExecutionFamily::DeleteTombstone,
            TableMaintenanceExecutionOperation::PositionDeleteExecution,
            "position_delete_fixture,row_position_identity",
            "native_position_delete_rule",
            "correctness_evidence,execution_certificate,native_io_certificate",
        ),
        TableMaintenanceExecutionMatrixRow::unsupported(
            TableMaintenanceExecutionFamily::DeleteTombstone,
            TableMaintenanceExecutionOperation::EqualityDeleteExecution,
            "equality_delete_fixture,equality_predicate",
            "native_equality_delete_rule",
            "correctness_evidence,execution_certificate,native_io_certificate",
        ),
        TableMaintenanceExecutionMatrixRow::report_only(
            TableMaintenanceExecutionFamily::Cdc,
            TableMaintenanceExecutionOperation::CdcAppendOnlyPlanning,
            "shardloom.cdc_incremental_planning.v1",
            "snapshot_pair,append_only_change_set",
            "none_report_only",
            "cdc_incremental_planning_report,no_fallback_policy",
        ),
        TableMaintenanceExecutionMatrixRow::report_only(
            TableMaintenanceExecutionFamily::Cdc,
            TableMaintenanceExecutionOperation::CdcMetadataOnlyPlanning,
            "shardloom.cdc_incremental_planning.v1",
            "snapshot_pair,metadata_only_change_set",
            "none_report_only",
            "cdc_incremental_planning_report,no_fallback_policy",
        ),
        TableMaintenanceExecutionMatrixRow::unsupported(
            TableMaintenanceExecutionFamily::Cdc,
            TableMaintenanceExecutionOperation::CdcUpdateDeleteTombstoneExecution,
            "snapshot_pair,row_identity,delete_tombstone_fixture",
            "cdc_transaction_and_delete_semantics",
            "correctness_evidence,execution_certificate,native_io_certificate,commit_protocol",
        ),
        TableMaintenanceExecutionMatrixRow::report_only(
            TableMaintenanceExecutionFamily::MaintenanceWrite,
            TableMaintenanceExecutionOperation::CompactionPlanning,
            "shardloom.compaction_planning.v1",
            "declared_layout_health_fixture",
            "none_report_only",
            "layout_health_report,compaction_planning_report,no_fallback_policy",
        ),
        TableMaintenanceExecutionMatrixRow::unsupported(
            TableMaintenanceExecutionFamily::MaintenanceWrite,
            TableMaintenanceExecutionOperation::CompactionExecutionWrite,
            "compaction_candidate_fixture,write_payload_fixture",
            "staged_output_and_atomic_commit_protocol",
            "correctness_evidence,execution_certificate,native_io_certificate,commit_recovery",
        ),
        TableMaintenanceExecutionMatrixRow::report_only(
            TableMaintenanceExecutionFamily::MaintenanceWrite,
            TableMaintenanceExecutionOperation::TableMetadataWrite,
            "gar-runtime-impl-4o.local_table_append_commit_rehearsal_smoke",
            "table_metadata_schema_fixture,catalog_ref",
            "local_manifest_staged_append_commit_rehearsal",
            "local_manifest_fixture_write,execution_certificate,native_io_certificate,no_fallback_policy",
        ),
        TableMaintenanceExecutionMatrixRow::report_only(
            TableMaintenanceExecutionFamily::MaintenanceWrite,
            TableMaintenanceExecutionOperation::TableMaintenanceCommit,
            "gar-runtime-impl-4o.local_table_append_commit_rehearsal_smoke",
            "manifest_delta_fixture,commit_marker_fixture",
            "local_manifest_sidecar_commit_record",
            "idempotency_key,rollback_cleanup,execution_certificate,native_io_certificate,no_fallback_policy",
        ),
    ]
}

fn table_maintenance_execution_existing_report_refs() -> Vec<&'static str> {
    vec![
        "cg9.table_intelligence.foundation",
        "shardloom.delete_tombstone_compatibility.v1",
        "shardloom.cdc_incremental_planning.v1",
        "shardloom.layout_health.v1",
        "shardloom.compaction_planning.v1",
        "gar0020c.local_manifest_table_metadata_read_smoke",
        "gar0020d.local_delete_tombstone_read_smoke",
        "gar0020e.local_append_only_cdc_overlay_smoke",
        "gar-runtime-impl-4o.local_table_append_commit_rehearsal_smoke",
        "gar0004a.cdc_manifest_transaction_gate",
        "shardloom.object_store_commit_protocol.v1",
    ]
}

#[must_use]
pub fn plan_table_maintenance_execution_matrix() -> TableMaintenanceExecutionMatrixReport {
    TableMaintenanceExecutionMatrixReport::planning_default()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogMetadataIntegrationSurface {
    TableIntelligenceFoundation,
    CatalogRefSkeleton,
    SnapshotManifestBoundary,
    CatalogTableResolution,
    TableMetadataRead,
    PartitionMetadataRead,
    DeleteTombstoneMetadataRead,
    CdcMetadataRead,
    TableFormatDependencyAdmission,
    CommitRecoveryMetadataBinding,
    MetadataCacheInvalidation,
}

impl CatalogMetadataIntegrationSurface {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::TableIntelligenceFoundation => "table_intelligence_foundation",
            Self::CatalogRefSkeleton => "catalog_ref_skeleton",
            Self::SnapshotManifestBoundary => "snapshot_manifest_boundary",
            Self::CatalogTableResolution => "catalog_table_resolution",
            Self::TableMetadataRead => "table_metadata_read",
            Self::PartitionMetadataRead => "partition_metadata_read",
            Self::DeleteTombstoneMetadataRead => "delete_tombstone_metadata_read",
            Self::CdcMetadataRead => "cdc_metadata_read",
            Self::TableFormatDependencyAdmission => "table_format_dependency_admission",
            Self::CommitRecoveryMetadataBinding => "commit_recovery_metadata_binding",
            Self::MetadataCacheInvalidation => "metadata_cache_invalidation",
        }
    }

    #[must_use]
    pub const fn diagnostic_code(&self) -> DiagnosticCode {
        match self {
            Self::SnapshotManifestBoundary
            | Self::TableMetadataRead
            | Self::PartitionMetadataRead
            | Self::DeleteTombstoneMetadataRead
            | Self::CdcMetadataRead
            | Self::MetadataCacheInvalidation
            | Self::CatalogTableResolution
            | Self::CommitRecoveryMetadataBinding
            | Self::TableIntelligenceFoundation
            | Self::CatalogRefSkeleton => DiagnosticCode::NotImplemented,
            Self::TableFormatDependencyAdmission => DiagnosticCode::ExternalEffectDisabled,
        }
    }

    #[must_use]
    pub const fn diagnostic_category(&self) -> DiagnosticCategory {
        match self {
            Self::SnapshotManifestBoundary
            | Self::CatalogTableResolution
            | Self::TableMetadataRead
            | Self::PartitionMetadataRead
            | Self::DeleteTombstoneMetadataRead
            | Self::CdcMetadataRead
            | Self::MetadataCacheInvalidation => DiagnosticCategory::Planning,
            Self::TableFormatDependencyAdmission => DiagnosticCategory::ExternalEffect,
            Self::CommitRecoveryMetadataBinding => DiagnosticCategory::Execution,
            Self::TableIntelligenceFoundation | Self::CatalogRefSkeleton => {
                DiagnosticCategory::Planning
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogMetadataIntegrationStatus {
    ExistingReportOnlyEvidence,
    BlockedUntilCertified,
}

impl CatalogMetadataIntegrationStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ExistingReportOnlyEvidence => "existing_report_only_evidence",
            Self::BlockedUntilCertified => "blocked_until_certified",
        }
    }

    #[must_use]
    pub const fn is_existing_evidence(&self) -> bool {
        matches!(self, Self::ExistingReportOnlyEvidence)
    }

    #[must_use]
    pub const fn is_blocked(&self) -> bool {
        matches!(self, Self::BlockedUntilCertified)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogMetadataIntegrationGateEntry {
    pub surface: CatalogMetadataIntegrationSurface,
    pub status: CatalogMetadataIntegrationStatus,
    pub existing_report_ref: Option<&'static str>,
    pub required_evidence: &'static str,
    pub requires_catalog_ref: bool,
    pub requires_snapshot_ref: bool,
    pub requires_table_metadata_io: bool,
    pub requires_catalog_io: bool,
    pub requires_object_store_io: bool,
    pub requires_dependency_approval: bool,
    pub requires_credential_policy: bool,
    pub requires_execution_certificate: bool,
    pub requires_native_io_certificate: bool,
    pub runtime_allowed: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogMetadataIntegrationRequirements {
    pub requires_catalog_ref: bool,
    pub requires_snapshot_ref: bool,
    pub requires_table_metadata_io: bool,
    pub requires_catalog_io: bool,
    pub requires_object_store_io: bool,
    pub requires_dependency_approval: bool,
    pub requires_credential_policy: bool,
}

impl CatalogMetadataIntegrationRequirements {
    pub const SNAPSHOT_MANIFEST_BOUNDARY: Self = Self {
        requires_catalog_ref: false,
        requires_snapshot_ref: true,
        requires_table_metadata_io: true,
        requires_catalog_io: false,
        requires_object_store_io: true,
        requires_dependency_approval: false,
        requires_credential_policy: false,
    };

    pub const CATALOG_TABLE_RESOLUTION: Self = Self {
        requires_catalog_ref: true,
        requires_snapshot_ref: true,
        requires_table_metadata_io: true,
        requires_catalog_io: true,
        requires_object_store_io: false,
        requires_dependency_approval: true,
        requires_credential_policy: true,
    };

    pub const CATALOG_BACKED_METADATA: Self = Self {
        requires_catalog_ref: true,
        requires_snapshot_ref: true,
        requires_table_metadata_io: true,
        requires_catalog_io: true,
        requires_object_store_io: true,
        requires_dependency_approval: true,
        requires_credential_policy: true,
    };

    pub const TABLE_FORMAT_DEPENDENCY_ADMISSION: Self = Self {
        requires_catalog_ref: true,
        requires_snapshot_ref: true,
        requires_table_metadata_io: false,
        requires_catalog_io: false,
        requires_object_store_io: false,
        requires_dependency_approval: true,
        requires_credential_policy: false,
    };
}

impl CatalogMetadataIntegrationGateEntry {
    #[must_use]
    pub const fn existing(
        surface: CatalogMetadataIntegrationSurface,
        existing_report_ref: &'static str,
    ) -> Self {
        Self {
            surface,
            status: CatalogMetadataIntegrationStatus::ExistingReportOnlyEvidence,
            existing_report_ref: Some(existing_report_ref),
            required_evidence: existing_report_ref,
            requires_catalog_ref: false,
            requires_snapshot_ref: false,
            requires_table_metadata_io: false,
            requires_catalog_io: false,
            requires_object_store_io: false,
            requires_dependency_approval: false,
            requires_credential_policy: false,
            requires_execution_certificate: false,
            requires_native_io_certificate: false,
            runtime_allowed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn blocked(
        surface: CatalogMetadataIntegrationSurface,
        requirements: CatalogMetadataIntegrationRequirements,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            surface,
            status: CatalogMetadataIntegrationStatus::BlockedUntilCertified,
            existing_report_ref: None,
            required_evidence,
            requires_catalog_ref: requirements.requires_catalog_ref,
            requires_snapshot_ref: requirements.requires_snapshot_ref,
            requires_table_metadata_io: requirements.requires_table_metadata_io,
            requires_catalog_io: requirements.requires_catalog_io,
            requires_object_store_io: requirements.requires_object_store_io,
            requires_dependency_approval: requirements.requires_dependency_approval,
            requires_credential_policy: requirements.requires_credential_policy,
            requires_execution_certificate: true,
            requires_native_io_certificate: true,
            runtime_allowed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.runtime_allowed
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && !self.external_engine_invoked
    }

    #[must_use]
    pub fn to_diagnostic(&self) -> Option<Diagnostic> {
        if !self.status.is_blocked() {
            return None;
        }
        Some(Diagnostic::new(
            self.surface.diagnostic_code(),
            DiagnosticSeverity::Info,
            self.surface.diagnostic_category(),
            format!(
                "{} is blocked until table/catalog metadata evidence is certified",
                self.surface.as_str()
            ),
            Some(self.surface.as_str().to_string()),
            Some(format!(
                "{} requires {} before runtime promotion.",
                self.surface.as_str(),
                self.required_evidence
            )),
            Some(
                "Keep this table/catalog lane report-only; do not perform catalog, metadata, data, credential, object-store, or write I/O."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogMetadataIntegrationGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub gar_id: &'static str,
    pub gate_status: &'static str,
    pub support_status: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub entries: Vec<CatalogMetadataIntegrationGateEntry>,
    pub existing_report_refs: Vec<&'static str>,
    pub compatibility_profiles: Vec<&'static str>,
    pub existing_table_intelligence_foundation_present: bool,
    pub existing_schema_partition_delete_compatibility_present: bool,
    pub existing_cdc_layout_compaction_planning_present: bool,
    pub existing_catalog_ref_skeleton_present: bool,
    pub local_manifest_table_metadata_smoke_supported: bool,
    pub local_manifest_table_metadata_smoke_command: &'static str,
    pub local_manifest_table_metadata_smoke_report_ref: &'static str,
    pub local_manifest_table_metadata_smoke_claim_gate_status: &'static str,
    pub local_manifest_table_metadata_smoke_claim_boundary: &'static str,
    pub snapshot_manifest_metadata_read_allowed: bool,
    pub catalog_resolution_allowed: bool,
    pub table_metadata_read_allowed: bool,
    pub catalog_io_allowed: bool,
    pub object_store_io_allowed: bool,
    pub data_io_allowed: bool,
    pub write_io_allowed: bool,
    pub external_table_format_dependency_allowed: bool,
    pub credential_resolution_allowed: bool,
    pub metadata_cache_runtime_allowed: bool,
    pub metadata_integration_claim_allowed: bool,
    pub table_intelligence_report_required: bool,
    pub catalog_ref_required: bool,
    pub snapshot_ref_required: bool,
    pub schema_digest_required: bool,
    pub partition_spec_required: bool,
    pub delete_tombstone_policy_required: bool,
    pub dependency_license_approval_required: bool,
    pub credential_policy_required: bool,
    pub effect_policy_required: bool,
    pub materialization_boundary_required: bool,
    pub execution_certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub benchmark_evidence_required: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl CatalogMetadataIntegrationGateReport {
    #[must_use]
    pub fn planning_default() -> Self {
        let entries = catalog_metadata_integration_entries();
        let diagnostics = entries
            .iter()
            .filter_map(CatalogMetadataIntegrationGateEntry::to_diagnostic)
            .collect();
        Self {
            schema_version: "shardloom.catalog_metadata_integration_gate.v1",
            report_id: "cg9.catalog_metadata_integration_gate",
            gar_id: "GAR-0020-A",
            gate_status: "report_only",
            support_status: "unsupported",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "catalog resolution, snapshot/manifest reads, table metadata reads, data reads, credentials, external table-format dependencies, and table/catalog runtime claims remain unsupported until certified",
            entries,
            existing_report_refs: catalog_metadata_existing_report_refs(),
            compatibility_profiles: catalog_metadata_compatibility_profiles(),
            existing_table_intelligence_foundation_present: true,
            existing_schema_partition_delete_compatibility_present: true,
            existing_cdc_layout_compaction_planning_present: true,
            existing_catalog_ref_skeleton_present: true,
            local_manifest_table_metadata_smoke_supported: true,
            local_manifest_table_metadata_smoke_command: "local-table-metadata-read-smoke",
            local_manifest_table_metadata_smoke_report_ref: "gar0020c.local_manifest_table_metadata_read_smoke",
            local_manifest_table_metadata_smoke_claim_gate_status: "scoped_local_metadata_smoke_only",
            local_manifest_table_metadata_smoke_claim_boundary: "one in-memory local manifest metadata smoke path only; broad catalog/table, object-store, credential, data-read, write, lakehouse, and production claims remain blocked",
            snapshot_manifest_metadata_read_allowed: false,
            catalog_resolution_allowed: false,
            table_metadata_read_allowed: false,
            catalog_io_allowed: false,
            object_store_io_allowed: false,
            data_io_allowed: false,
            write_io_allowed: false,
            external_table_format_dependency_allowed: false,
            credential_resolution_allowed: false,
            metadata_cache_runtime_allowed: false,
            metadata_integration_claim_allowed: false,
            table_intelligence_report_required: true,
            catalog_ref_required: true,
            snapshot_ref_required: true,
            schema_digest_required: true,
            partition_spec_required: true,
            delete_tombstone_policy_required: true,
            dependency_license_approval_required: true,
            credential_policy_required: true,
            effect_policy_required: true,
            materialization_boundary_required: true,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            benchmark_evidence_required: true,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            external_engine_invoked: false,
            diagnostics,
        }
    }

    #[must_use]
    pub fn surface_count(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn existing_evidence_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_existing_evidence())
            .count()
    }

    #[must_use]
    pub fn blocked_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_blocked())
            .count()
    }

    #[must_use]
    pub fn unsupported_surface_count(&self) -> usize {
        self.blocked_surface_count()
    }

    #[must_use]
    pub fn surface_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.surface.as_str())
            .collect()
    }

    #[must_use]
    pub fn runtime_promotions_blocked(&self) -> bool {
        !self.snapshot_manifest_metadata_read_allowed
            && !self.catalog_resolution_allowed
            && !self.table_metadata_read_allowed
            && !self.catalog_io_allowed
            && !self.object_store_io_allowed
            && !self.data_io_allowed
            && !self.write_io_allowed
            && !self.external_table_format_dependency_allowed
            && !self.credential_resolution_allowed
            && !self.metadata_cache_runtime_allowed
            && self
                .entries
                .iter()
                .all(CatalogMetadataIntegrationGateEntry::side_effect_free)
    }

    #[must_use]
    pub fn claim_blocked(&self) -> bool {
        !self.metadata_integration_claim_allowed && self.claim_gate_status == "not_claim_grade"
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        self.runtime_promotions_blocked()
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && !self.external_engine_invoked
            && self
                .entries
                .iter()
                .all(CatalogMetadataIntegrationGateEntry::side_effect_free)
    }

    #[must_use]
    pub fn unsupported_diagnostic_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_blocked())
            .filter(|entry| {
                self.diagnostics.iter().any(|diagnostic| {
                    diagnostic.code == entry.surface.diagnostic_code()
                        && diagnostic.category == entry.surface.diagnostic_category()
                        && diagnostic.severity == DiagnosticSeverity::Info
                        && diagnostic.feature.as_deref() == Some(entry.surface.as_str())
                        && !diagnostic.fallback.attempted
                        && !diagnostic.fallback.allowed
                })
            })
            .count()
    }

    #[must_use]
    pub fn deterministic_unsupported_diagnostics_ready(&self) -> bool {
        self.unsupported_surface_count() > 0
            && self.unsupported_diagnostic_count() == self.unsupported_surface_count()
    }

    #[must_use]
    pub fn unsupported_diagnostic_code_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_blocked())
            .map(|entry| entry.surface.diagnostic_code().as_str())
            .collect()
    }

    #[must_use]
    pub fn unsupported_diagnostic_category_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_blocked())
            .map(|entry| entry.surface.diagnostic_category().as_str())
            .collect()
    }

    #[must_use]
    pub fn unsupported_diagnostic_severity_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_blocked())
            .map(|_| DiagnosticSeverity::Info.as_str())
            .collect()
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || !self.claim_blocked()
            || !self.deterministic_unsupported_diagnostics_ready()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(out, "gar_id: {}", self.gar_id);
        let _ = writeln!(out, "gate_status: {}", self.gate_status);
        let _ = writeln!(out, "support_status: {}", self.support_status);
        let _ = writeln!(out, "claim_gate_status: {}", self.claim_gate_status);
        let _ = writeln!(out, "claim_boundary: {}", self.claim_boundary);
        let _ = writeln!(
            out,
            "compatibility profiles: {}",
            self.compatibility_profiles.join(",")
        );
        let _ = writeln!(
            out,
            "existing report refs: {}",
            self.existing_report_refs.join(",")
        );
        let _ = writeln!(
            out,
            "runtime promotions blocked: {}",
            self.runtime_promotions_blocked()
        );
        let _ = writeln!(out, "claim blocked: {}", self.claim_blocked());
        let _ = writeln!(
            out,
            "deterministic unsupported diagnostics ready: {}",
            self.deterministic_unsupported_diagnostics_ready()
        );
        let _ = writeln!(out, "side effect free: {}", self.side_effect_free());
        let _ = writeln!(out, "fallback attempted: {}", self.fallback_attempted);
        let _ = writeln!(
            out,
            "fallback execution allowed: {}",
            self.fallback_execution_allowed
        );
        let _ = writeln!(
            out,
            "external engine invoked: {}",
            self.external_engine_invoked
        );
        let _ = writeln!(out, "surfaces:");
        for entry in &self.entries {
            let _ = writeln!(
                out,
                "  - {} [{}] existing_ref={} required_evidence={} runtime_allowed={} requires_catalog_ref={} requires_snapshot_ref={} requires_table_metadata_io={} requires_catalog_io={} requires_object_store_io={} requires_dependency_approval={} requires_credential_policy={} requires_execution_certificate={} requires_native_io_certificate={} fallback_attempted={} fallback_execution_allowed={} external_engine_invoked={} claim_gate_status={}",
                entry.surface.as_str(),
                entry.status.as_str(),
                entry.existing_report_ref.unwrap_or("none"),
                entry.required_evidence,
                entry.runtime_allowed,
                entry.requires_catalog_ref,
                entry.requires_snapshot_ref,
                entry.requires_table_metadata_io,
                entry.requires_catalog_io,
                entry.requires_object_store_io,
                entry.requires_dependency_approval,
                entry.requires_credential_policy,
                entry.requires_execution_certificate,
                entry.requires_native_io_certificate,
                entry.fallback_attempted,
                entry.fallback_execution_allowed,
                entry.external_engine_invoked,
                entry.claim_gate_status
            );
        }
        out
    }
}

fn catalog_metadata_integration_entries() -> Vec<CatalogMetadataIntegrationGateEntry> {
    vec![
        CatalogMetadataIntegrationGateEntry::existing(
            CatalogMetadataIntegrationSurface::TableIntelligenceFoundation,
            "cg9.table_intelligence.foundation",
        ),
        CatalogMetadataIntegrationGateEntry::existing(
            CatalogMetadataIntegrationSurface::CatalogRefSkeleton,
            "catalog-plan",
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::SnapshotManifestBoundary,
            CatalogMetadataIntegrationRequirements::SNAPSHOT_MANIFEST_BOUNDARY,
            "snapshot_ref,manifest_location,object_store_provider_policy,native_io_certificate,execution_certificate",
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::CatalogTableResolution,
            CatalogMetadataIntegrationRequirements::CATALOG_TABLE_RESOLUTION,
            "catalog_ref,snapshot_ref,table_identifier,credential_policy,dependency_license_approval,effect_policy",
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::TableMetadataRead,
            CatalogMetadataIntegrationRequirements::CATALOG_BACKED_METADATA,
            "catalog_ref,snapshot_ref,table_metadata_schema,credential_policy,native_io_certificate,execution_certificate",
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::PartitionMetadataRead,
            CatalogMetadataIntegrationRequirements::CATALOG_BACKED_METADATA,
            "partition_spec,snapshot_ref,table_metadata_schema,credential_policy,native_io_certificate",
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::DeleteTombstoneMetadataRead,
            CatalogMetadataIntegrationRequirements::CATALOG_BACKED_METADATA,
            "delete_tombstone_policy,row_identity,snapshot_ref,credential_policy,native_io_certificate",
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::CdcMetadataRead,
            CatalogMetadataIntegrationRequirements::CATALOG_BACKED_METADATA,
            "cdc_manifest_schema,change_set_ref,snapshot_pair,credential_policy,native_io_certificate",
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::TableFormatDependencyAdmission,
            CatalogMetadataIntegrationRequirements::TABLE_FORMAT_DEPENDENCY_ADMISSION,
            "dependency_license_approval,feature_gate,version_record,policy_admission,no_fallback_policy",
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::CommitRecoveryMetadataBinding,
            CatalogMetadataIntegrationRequirements::CATALOG_BACKED_METADATA,
            "commit_protocol,recovery_certificate,conflict_detection,credential_policy,native_io_certificate",
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::MetadataCacheInvalidation,
            CatalogMetadataIntegrationRequirements::CATALOG_BACKED_METADATA,
            "cache_key_contract,snapshot_ref,invalidation_policy,credential_policy,execution_certificate",
        ),
    ]
}

fn catalog_metadata_existing_report_refs() -> Vec<&'static str> {
    vec![
        "cg9.table_intelligence.foundation",
        "shardloom.schema_evolution_compatibility.v1",
        "shardloom.partition_evolution_compatibility.v1",
        "shardloom.delete_tombstone_compatibility.v1",
        "shardloom.table_compatibility.v1",
        "shardloom.cdc_incremental_planning.v1",
        "shardloom.layout_health.v1",
        "shardloom.compaction_planning.v1",
        "catalog-plan",
    ]
}

fn catalog_metadata_compatibility_profiles() -> Vec<&'static str> {
    vec![
        "native_vortex",
        "iceberg_compatible",
        "delta_compatible",
        "hudi_like",
        "hive_style_partitions",
        "external_catalog_only",
    ]
}

#[must_use]
pub fn plan_catalog_metadata_integration_gate() -> CatalogMetadataIntegrationGateReport {
    CatalogMetadataIntegrationGateReport::planning_default()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalTableMetadataBlockedPath {
    ExternalCatalogResolution,
    ObjectStoreManifestRead,
    CredentialResolution,
    DataFileRead,
    TableMetadataWrite,
    CdcDeleteTombstoneExecution,
    ExternalTableFormatRuntime,
    LakehouseProductionClaim,
}

impl LocalTableMetadataBlockedPath {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ExternalCatalogResolution => "external_catalog_resolution",
            Self::ObjectStoreManifestRead => "object_store_manifest_read",
            Self::CredentialResolution => "credential_resolution",
            Self::DataFileRead => "data_file_read",
            Self::TableMetadataWrite => "table_metadata_write",
            Self::CdcDeleteTombstoneExecution => "cdc_delete_tombstone_execution",
            Self::ExternalTableFormatRuntime => "external_table_format_runtime",
            Self::LakehouseProductionClaim => "lakehouse_production_claim",
        }
    }

    #[must_use]
    pub const fn diagnostic_code(&self) -> DiagnosticCode {
        match self {
            Self::ObjectStoreManifestRead => DiagnosticCode::ObjectStoreUnsupported,
            Self::CredentialResolution | Self::ExternalTableFormatRuntime => {
                DiagnosticCode::ExternalEffectDisabled
            }
            Self::ExternalCatalogResolution
            | Self::DataFileRead
            | Self::TableMetadataWrite
            | Self::CdcDeleteTombstoneExecution
            | Self::LakehouseProductionClaim => DiagnosticCode::NotImplemented,
        }
    }

    #[must_use]
    pub const fn diagnostic_category(&self) -> DiagnosticCategory {
        match self {
            Self::ObjectStoreManifestRead => DiagnosticCategory::ObjectStore,
            Self::CredentialResolution | Self::ExternalTableFormatRuntime => {
                DiagnosticCategory::ExternalEffect
            }
            Self::DataFileRead | Self::TableMetadataWrite | Self::CdcDeleteTombstoneExecution => {
                DiagnosticCategory::Execution
            }
            Self::ExternalCatalogResolution | Self::LakehouseProductionClaim => {
                DiagnosticCategory::Planning
            }
        }
    }

    #[must_use]
    pub fn to_diagnostic(self) -> Diagnostic {
        Diagnostic::new(
            self.diagnostic_code(),
            DiagnosticSeverity::Info,
            self.diagnostic_category(),
            format!(
                "{} remains blocked outside GAR-0020-C's local metadata smoke scope",
                self.as_str()
            ),
            Some(self.as_str().to_string()),
            Some(
                "GAR-0020-C only reads typed metadata from the local manifest fixture; no broad table/catalog runtime is certified."
                    .to_string(),
            ),
            Some(
                "Use local-table-metadata-read-smoke for the fixture path, or keep the broader lane report-only until dedicated evidence lands."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalTableMetadataReadSmokeReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub gar_id: &'static str,
    pub support_status: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub catalog_kind: &'static str,
    pub catalog_ref_summary: String,
    pub dataset_uri: String,
    pub dataset_format: String,
    pub manifest_id: String,
    pub manifest_version: String,
    pub snapshot_id: String,
    pub schema_id: String,
    pub schema_version_number: u64,
    pub schema_field_count: usize,
    pub schema_has_field_ids: bool,
    pub partition_field_count: usize,
    pub is_partitioned: bool,
    pub manifest_file_count: usize,
    pub manifest_segment_count: usize,
    pub native_vortex_file_count: usize,
    pub metadata_capable_segment_count: usize,
    pub declared_row_count: u64,
    pub metadata_summary: String,
    pub metadata_summary_digest: String,
    pub correctness_refs: &'static str,
    pub benchmark_refs: &'static str,
    pub execution_certificate_refs: &'static str,
    pub native_io_certificate_refs: &'static str,
    pub materialization_decode_refs: &'static str,
    pub policy_refs: &'static str,
    pub dependency_boundary_refs: &'static str,
    pub local_catalog_ref_resolved: bool,
    pub local_manifest_metadata_read_performed: bool,
    pub table_metadata_summary_emitted: bool,
    pub table_metadata_read_performed: bool,
    pub catalog_io_performed: bool,
    pub table_metadata_file_io_performed: bool,
    pub object_store_io_performed: bool,
    pub data_file_read_performed: bool,
    pub write_io_performed: bool,
    pub credential_resolution_performed: bool,
    pub external_table_format_dependency_invoked: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub performance_claim_allowed: bool,
    pub production_table_catalog_claim_allowed: bool,
    pub lakehouse_claim_allowed: bool,
    pub blocked_paths: Vec<LocalTableMetadataBlockedPath>,
    pub diagnostics: Vec<Diagnostic>,
}

impl LocalTableMetadataReadSmokeReport {
    #[must_use]
    pub fn runtime_supported(&self) -> bool {
        self.support_status == "runtime_supported"
            && self.local_catalog_ref_resolved
            && self.local_manifest_metadata_read_performed
            && self.table_metadata_summary_emitted
            && self.table_metadata_read_performed
    }

    #[must_use]
    pub fn claim_scoped(&self) -> bool {
        self.claim_gate_status == "scoped_local_metadata_smoke_only"
            && !self.performance_claim_allowed
            && !self.production_table_catalog_claim_allowed
            && !self.lakehouse_claim_allowed
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.catalog_io_performed
            && !self.table_metadata_file_io_performed
            && !self.object_store_io_performed
            && !self.data_file_read_performed
            && !self.write_io_performed
            && !self.credential_resolution_performed
            && !self.external_table_format_dependency_invoked
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && !self.external_engine_invoked
    }

    #[must_use]
    pub fn unsupported_diagnostic_count(&self) -> usize {
        self.blocked_paths
            .iter()
            .filter(|path| {
                self.diagnostics.iter().any(|diagnostic| {
                    diagnostic.code == path.diagnostic_code()
                        && diagnostic.category == path.diagnostic_category()
                        && diagnostic.severity == DiagnosticSeverity::Info
                        && diagnostic.feature.as_deref() == Some(path.as_str())
                        && !diagnostic.fallback.attempted
                        && !diagnostic.fallback.allowed
                })
            })
            .count()
    }

    #[must_use]
    pub fn deterministic_unsupported_diagnostics_ready(&self) -> bool {
        !self.blocked_paths.is_empty()
            && self.unsupported_diagnostic_count() == self.blocked_paths.len()
    }

    #[must_use]
    pub fn blocked_path_order(&self) -> Vec<&'static str> {
        self.blocked_paths
            .iter()
            .map(LocalTableMetadataBlockedPath::as_str)
            .collect()
    }

    #[must_use]
    pub fn unsupported_diagnostic_code_order(&self) -> Vec<&'static str> {
        self.blocked_paths
            .iter()
            .map(|path| path.diagnostic_code().as_str())
            .collect()
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.runtime_supported()
            || !self.claim_scoped()
            || !self.side_effect_free()
            || !self.deterministic_unsupported_diagnostics_ready()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(out, "gar_id: {}", self.gar_id);
        let _ = writeln!(out, "support_status: {}", self.support_status);
        let _ = writeln!(out, "claim_gate_status: {}", self.claim_gate_status);
        let _ = writeln!(out, "claim_boundary: {}", self.claim_boundary);
        let _ = writeln!(out, "catalog: {}", self.catalog_ref_summary);
        let _ = writeln!(out, "dataset_uri: {}", self.dataset_uri);
        let _ = writeln!(
            out,
            "manifest: {} version={} snapshot={}",
            self.manifest_id, self.manifest_version, self.snapshot_id
        );
        let _ = writeln!(
            out,
            "schema: {} version={} fields={} has_field_ids={}",
            self.schema_id,
            self.schema_version_number,
            self.schema_field_count,
            self.schema_has_field_ids
        );
        let _ = writeln!(
            out,
            "partition_fields: {} manifest_files: {} manifest_segments: {} metadata_capable_segments: {} declared_rows: {}",
            self.partition_field_count,
            self.manifest_file_count,
            self.manifest_segment_count,
            self.metadata_capable_segment_count,
            self.declared_row_count
        );
        let _ = writeln!(
            out,
            "metadata_summary_digest: {}",
            self.metadata_summary_digest
        );
        let _ = writeln!(out, "side_effect_free: {}", self.side_effect_free());
        let _ = writeln!(out, "fallback_attempted: {}", self.fallback_attempted);
        let _ = writeln!(
            out,
            "external_engine_invoked: {}",
            self.external_engine_invoked
        );
        let _ = writeln!(
            out,
            "blocked_paths: {}",
            self.blocked_path_order().join(",")
        );
        out
    }
}

#[derive(Debug, Clone)]
struct LocalTableMetadataFixture {
    catalog: CatalogRef,
    manifest: DatasetManifest,
    schema: SchemaDefinition,
    partition_spec: PartitionSpec,
}

#[must_use]
fn local_table_metadata_blocked_paths() -> Vec<LocalTableMetadataBlockedPath> {
    vec![
        LocalTableMetadataBlockedPath::ExternalCatalogResolution,
        LocalTableMetadataBlockedPath::ObjectStoreManifestRead,
        LocalTableMetadataBlockedPath::CredentialResolution,
        LocalTableMetadataBlockedPath::DataFileRead,
        LocalTableMetadataBlockedPath::TableMetadataWrite,
        LocalTableMetadataBlockedPath::CdcDeleteTombstoneExecution,
        LocalTableMetadataBlockedPath::ExternalTableFormatRuntime,
        LocalTableMetadataBlockedPath::LakehouseProductionClaim,
    ]
}

fn local_table_metadata_fixture() -> Result<LocalTableMetadataFixture> {
    let catalog = CatalogRef::new(CatalogKind::LocalManifest, "gar0020c-local-manifest")?
        .with_namespace("fixtures.gar0020c")?;
    let manifest_id = ManifestId::new("gar0020c-local-manifest-v1")?;
    let snapshot_id = SnapshotId::new("gar0020c-snapshot-0001")?;
    let dataset_uri = DatasetUri::new("file://fixtures/gar0020c/orders.vortex")?;
    let dataset = DatasetRef::from_uri(dataset_uri)?
        .with_snapshot(snapshot_id.clone())
        .with_manifest(manifest_id.clone());
    let snapshot = SnapshotRef::new(snapshot_id.clone());
    let mut manifest = DatasetManifest::new(manifest_id, dataset, snapshot);
    let data_file = FileDescriptor::new(
        DatasetUri::new("file://fixtures/gar0020c/orders.vortex/part-000.vortex")?,
        DatasetFormat::Vortex,
        FileRole::NativeVortexData,
    )
    .with_size_bytes(4096);
    manifest.add_file(data_file.clone());
    let layout = SegmentLayout::new(
        EncodingKind::VortexNative("fixture_metadata_only".to_string()),
        LayoutKind::VortexNative("local_manifest_smoke".to_string()),
    )
    .with_byte_ranges(vec![ByteRange::new(0, 4096)]);
    let segment = EncodedSegment::new(
        SegmentId::new("gar0020c-segment-0001")?,
        ColumnRef::new("amount")?,
        LogicalDType::Int64,
        Nullability::Nullable,
        layout,
        SegmentStats::with_row_count(8),
    );
    manifest.add_segment(ManifestSegment::new(segment, data_file).with_snapshot(snapshot_id));

    let mut schema = SchemaDefinition::new(
        SchemaId::new("gar0020c-orders-schema")?,
        SchemaVersion::new(1)?,
    );
    schema.add_field(
        SchemaField::new(
            FieldName::new("order_id")?,
            LogicalDType::UInt64,
            Nullability::NonNullable,
        )
        .with_id(FieldId::new("field.order_id")?),
    );
    schema.add_field(
        SchemaField::new(
            FieldName::new("event_date")?,
            LogicalDType::Date32,
            Nullability::NonNullable,
        )
        .with_id(FieldId::new("field.event_date")?),
    );
    schema.add_field(
        SchemaField::new(
            FieldName::new("region")?,
            LogicalDType::Utf8,
            Nullability::Nullable,
        )
        .with_id(FieldId::new("field.region")?),
    );
    schema.add_field(
        SchemaField::new(
            FieldName::new("amount")?,
            LogicalDType::Int64,
            Nullability::Nullable,
        )
        .with_id(FieldId::new("field.amount")?),
    );

    let mut partition_spec = PartitionSpec::empty();
    partition_spec.add_field(PartitionField::new(
        FieldPath::from_dot_separated("event_date")?,
        PartitionTransform::Identity,
    ));

    Ok(LocalTableMetadataFixture {
        catalog,
        manifest,
        schema,
        partition_spec,
    })
}

fn declared_row_count(manifest: &DatasetManifest) -> u64 {
    manifest
        .segments
        .iter()
        .filter_map(|segment| segment.segment.stats.row_count)
        .sum()
}

fn local_table_metadata_summary(fixture: &LocalTableMetadataFixture, declared_rows: u64) -> String {
    format!(
        "catalog_kind={} catalog_name={} dataset={} manifest={} snapshot={} schema={} schema_version={} fields={} partition_fields={} files={} segments={} native_vortex_files={} metadata_capable_segments={} declared_rows={} fallback_attempted=false external_engine_invoked=false",
        fixture.catalog.kind.as_str(),
        fixture.catalog.name,
        fixture.manifest.dataset.uri.as_str(),
        fixture.manifest.id.as_str(),
        fixture.manifest.snapshot.id.as_str(),
        fixture.schema.id.as_str(),
        fixture.schema.version.as_u64(),
        fixture.schema.field_count(),
        fixture.partition_spec.field_count(),
        fixture.manifest.file_count(),
        fixture.manifest.segment_count(),
        fixture.manifest.native_vortex_file_count(),
        fixture.manifest.segments_with_metadata_count(),
        declared_rows
    )
}

fn stable_metadata_digest(input: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv1a64:{hash:016x}")
}

pub fn run_local_table_metadata_read_smoke() -> Result<LocalTableMetadataReadSmokeReport> {
    let fixture = local_table_metadata_fixture()?;
    let declared_rows = declared_row_count(&fixture.manifest);
    let metadata_summary = local_table_metadata_summary(&fixture, declared_rows);
    let blocked_paths = local_table_metadata_blocked_paths();
    let diagnostics = blocked_paths
        .iter()
        .map(|path| path.to_diagnostic())
        .collect();
    Ok(LocalTableMetadataReadSmokeReport {
        schema_version: "shardloom.local_table_metadata_read_smoke.v1",
        report_id: "gar0020c.local_manifest_table_metadata_read_smoke",
        gar_id: "GAR-0020-C",
        support_status: "runtime_supported",
        claim_gate_status: "scoped_local_metadata_smoke_only",
        claim_boundary: "one in-memory local manifest-backed metadata summary; no data file read, object-store read, credential resolution, write, CDC/delete execution, lakehouse, SQL/DataFrame, Foundry, production, or performance claim",
        catalog_kind: fixture.catalog.kind.as_str(),
        catalog_ref_summary: fixture.catalog.summary(),
        dataset_uri: fixture.manifest.dataset.uri.as_str().to_string(),
        dataset_format: fixture.manifest.dataset.format.as_str().to_string(),
        manifest_id: fixture.manifest.id.as_str().to_string(),
        manifest_version: fixture.manifest.version.as_str().to_string(),
        snapshot_id: fixture.manifest.snapshot.id.as_str().to_string(),
        schema_id: fixture.schema.id.as_str().to_string(),
        schema_version_number: fixture.schema.version.as_u64(),
        schema_field_count: fixture.schema.field_count(),
        schema_has_field_ids: fixture.schema.has_field_ids(),
        partition_field_count: fixture.partition_spec.field_count(),
        is_partitioned: fixture.partition_spec.is_partitioned(),
        manifest_file_count: fixture.manifest.file_count(),
        manifest_segment_count: fixture.manifest.segment_count(),
        native_vortex_file_count: fixture.manifest.native_vortex_file_count(),
        metadata_capable_segment_count: fixture.manifest.segments_with_metadata_count(),
        declared_row_count: declared_rows,
        metadata_summary_digest: stable_metadata_digest(&metadata_summary),
        metadata_summary,
        correctness_refs: "shardloom-core::table_intelligence::local_table_metadata_read_smoke",
        benchmark_refs: "not_required_fixture_smoke_no_performance_claim",
        execution_certificate_refs: "shardloom-cli/tests/local_table_metadata_read_smoke.rs",
        native_io_certificate_refs: "not_required_no_source_sink_or_file_io",
        materialization_decode_refs: "metadata_summary_only_no_data_decode_no_row_materialization",
        policy_refs: "fallback_attempted=false,external_engine_invoked=false,object_store_io=false,credential_resolution=false",
        dependency_boundary_refs: "no_external_table_format_dependency,no_catalog_adapter_dependency,no_runtime_js_or_external_engine",
        local_catalog_ref_resolved: true,
        local_manifest_metadata_read_performed: true,
        table_metadata_summary_emitted: true,
        table_metadata_read_performed: true,
        catalog_io_performed: false,
        table_metadata_file_io_performed: false,
        object_store_io_performed: false,
        data_file_read_performed: false,
        write_io_performed: false,
        credential_resolution_performed: false,
        external_table_format_dependency_invoked: false,
        fallback_attempted: false,
        fallback_execution_allowed: false,
        external_engine_invoked: false,
        performance_claim_allowed: false,
        production_table_catalog_claim_allowed: false,
        lakehouse_claim_allowed: false,
        blocked_paths,
        diagnostics,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalDeleteTombstoneBlockedModel {
    RowLevelDelete,
    PositionDelete,
    EqualityDelete,
    ExternalTableMetadata,
    CdcUpdateDeleteTombstone,
    ObjectStoreDeleteManifest,
    TableFormatDeleteRuntime,
}

impl LocalDeleteTombstoneBlockedModel {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::RowLevelDelete => "row_level_delete",
            Self::PositionDelete => "position_delete",
            Self::EqualityDelete => "equality_delete",
            Self::ExternalTableMetadata => "external_table_metadata",
            Self::CdcUpdateDeleteTombstone => "cdc_update_delete_tombstone",
            Self::ObjectStoreDeleteManifest => "object_store_delete_manifest",
            Self::TableFormatDeleteRuntime => "table_format_delete_runtime",
        }
    }

    #[must_use]
    pub const fn diagnostic_code(&self) -> DiagnosticCode {
        match self {
            Self::ObjectStoreDeleteManifest => DiagnosticCode::ObjectStoreUnsupported,
            Self::ExternalTableMetadata | Self::TableFormatDeleteRuntime => {
                DiagnosticCode::ExternalEffectDisabled
            }
            Self::RowLevelDelete
            | Self::PositionDelete
            | Self::EqualityDelete
            | Self::CdcUpdateDeleteTombstone => DiagnosticCode::NotImplemented,
        }
    }

    #[must_use]
    pub const fn diagnostic_category(&self) -> DiagnosticCategory {
        match self {
            Self::ObjectStoreDeleteManifest => DiagnosticCategory::ObjectStore,
            Self::ExternalTableMetadata | Self::TableFormatDeleteRuntime => {
                DiagnosticCategory::ExternalEffect
            }
            Self::RowLevelDelete
            | Self::PositionDelete
            | Self::EqualityDelete
            | Self::CdcUpdateDeleteTombstone => DiagnosticCategory::Execution,
        }
    }

    #[must_use]
    pub fn to_diagnostic(self) -> Diagnostic {
        Diagnostic::new(
            self.diagnostic_code(),
            DiagnosticSeverity::Info,
            self.diagnostic_category(),
            format!(
                "{} remains blocked outside GAR-0020-D's local delete/tombstone fixture scope",
                self.as_str()
            ),
            Some(self.as_str().to_string()),
            Some(
                "GAR-0020-D only applies file-level delete and segment tombstone admission to a declared in-memory local manifest fixture; no broad table-format runtime is certified."
                    .to_string(),
            ),
            Some(
                "Keep unsupported delete models blocked until dedicated row identity, position identity, equality predicate, CDC, object-store, and table-format evidence lands."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct LocalDeleteTombstoneReadSmokeReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub gar_id: &'static str,
    pub support_status: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub fixture_id: &'static str,
    pub catalog_kind: &'static str,
    pub catalog_ref_summary: String,
    pub dataset_uri: String,
    pub dataset_format: String,
    pub manifest_id: String,
    pub manifest_version: String,
    pub snapshot_id: String,
    pub schema_id: String,
    pub admitted_delete_model_order: Vec<&'static str>,
    pub unsupported_delete_model_order: Vec<&'static str>,
    pub delete_tombstone_admission_rule: &'static str,
    pub row_identity_rule: &'static str,
    pub base_row_count: usize,
    pub file_deleted_row_count: usize,
    pub segment_tombstoned_row_count: usize,
    pub effective_row_count: usize,
    pub manifest_file_count: usize,
    pub manifest_segment_count: usize,
    pub native_vortex_file_count: usize,
    pub admitted_file_delete_count: usize,
    pub admitted_segment_tombstone_count: usize,
    pub effective_row_ids: Vec<u64>,
    pub correctness_summary: String,
    pub correctness_digest: String,
    pub correctness_refs: &'static str,
    pub benchmark_refs: &'static str,
    pub execution_certificate_refs: &'static str,
    pub native_io_certificate_refs: &'static str,
    pub materialization_decode_refs: &'static str,
    pub policy_refs: &'static str,
    pub local_catalog_ref_resolved: bool,
    pub local_manifest_metadata_read_performed: bool,
    pub in_memory_fixture_rows_read: bool,
    pub delete_tombstone_rule_applied: bool,
    pub result_row_order_preserved: bool,
    pub table_metadata_write_performed: bool,
    pub data_file_read_performed: bool,
    pub object_store_io_performed: bool,
    pub write_io_performed: bool,
    pub credential_resolution_performed: bool,
    pub external_table_format_dependency_invoked: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub performance_claim_allowed: bool,
    pub table_format_execution_claim_allowed: bool,
    pub production_table_catalog_claim_allowed: bool,
    pub lakehouse_claim_allowed: bool,
    pub blocked_models: Vec<LocalDeleteTombstoneBlockedModel>,
    pub diagnostics: Vec<Diagnostic>,
}

impl LocalDeleteTombstoneReadSmokeReport {
    #[must_use]
    pub fn fixture_smoke_supported(&self) -> bool {
        self.support_status == "fixture_smoke_only"
            && self.local_catalog_ref_resolved
            && self.local_manifest_metadata_read_performed
            && self.in_memory_fixture_rows_read
            && self.delete_tombstone_rule_applied
            && self.base_row_count
                == self.file_deleted_row_count
                    + self.segment_tombstoned_row_count
                    + self.effective_row_count
    }

    #[must_use]
    pub fn claim_scoped(&self) -> bool {
        self.claim_gate_status == "scoped_local_delete_tombstone_smoke_only"
            && !self.performance_claim_allowed
            && !self.table_format_execution_claim_allowed
            && !self.production_table_catalog_claim_allowed
            && !self.lakehouse_claim_allowed
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.table_metadata_write_performed
            && !self.data_file_read_performed
            && !self.object_store_io_performed
            && !self.write_io_performed
            && !self.credential_resolution_performed
            && !self.external_table_format_dependency_invoked
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && !self.external_engine_invoked
    }

    #[must_use]
    pub fn unsupported_diagnostic_count(&self) -> usize {
        self.blocked_models
            .iter()
            .filter(|model| {
                self.diagnostics.iter().any(|diagnostic| {
                    diagnostic.code == model.diagnostic_code()
                        && diagnostic.category == model.diagnostic_category()
                        && diagnostic.severity == DiagnosticSeverity::Info
                        && diagnostic.feature.as_deref() == Some(model.as_str())
                        && !diagnostic.fallback.attempted
                        && !diagnostic.fallback.allowed
                })
            })
            .count()
    }

    #[must_use]
    pub fn deterministic_unsupported_diagnostics_ready(&self) -> bool {
        !self.blocked_models.is_empty()
            && self.unsupported_diagnostic_count() == self.blocked_models.len()
    }

    #[must_use]
    pub fn blocked_model_order(&self) -> Vec<&'static str> {
        self.blocked_models
            .iter()
            .map(LocalDeleteTombstoneBlockedModel::as_str)
            .collect()
    }

    #[must_use]
    pub fn unsupported_diagnostic_code_order(&self) -> Vec<&'static str> {
        self.blocked_models
            .iter()
            .map(|model| model.diagnostic_code().as_str())
            .collect()
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.fixture_smoke_supported()
            || !self.claim_scoped()
            || !self.side_effect_free()
            || !self.deterministic_unsupported_diagnostics_ready()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(out, "gar_id: {}", self.gar_id);
        let _ = writeln!(out, "support_status: {}", self.support_status);
        let _ = writeln!(out, "claim_gate_status: {}", self.claim_gate_status);
        let _ = writeln!(out, "claim_boundary: {}", self.claim_boundary);
        let _ = writeln!(out, "fixture_id: {}", self.fixture_id);
        let _ = writeln!(
            out,
            "admitted_delete_models: {}",
            self.admitted_delete_model_order.join(",")
        );
        let _ = writeln!(
            out,
            "rows: base={} file_deleted={} segment_tombstoned={} effective={}",
            self.base_row_count,
            self.file_deleted_row_count,
            self.segment_tombstoned_row_count,
            self.effective_row_count
        );
        let _ = writeln!(out, "correctness_digest: {}", self.correctness_digest);
        let _ = writeln!(out, "side_effect_free: {}", self.side_effect_free());
        let _ = writeln!(out, "fallback_attempted: {}", self.fallback_attempted);
        let _ = writeln!(
            out,
            "external_engine_invoked: {}",
            self.external_engine_invoked
        );
        let _ = writeln!(
            out,
            "blocked_models: {}",
            self.blocked_model_order().join(",")
        );
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalDeleteTombstoneFixtureRow {
    row_id: u64,
    segment_id: &'static str,
    file_uri: String,
}

#[derive(Debug, Clone)]
struct LocalDeleteTombstoneFixture {
    catalog: CatalogRef,
    manifest: DatasetManifest,
    schema: SchemaDefinition,
    rows: Vec<LocalDeleteTombstoneFixtureRow>,
    deleted_file_uri: String,
    tombstoned_segment_id: &'static str,
}

#[must_use]
fn local_delete_tombstone_blocked_models() -> Vec<LocalDeleteTombstoneBlockedModel> {
    vec![
        LocalDeleteTombstoneBlockedModel::RowLevelDelete,
        LocalDeleteTombstoneBlockedModel::PositionDelete,
        LocalDeleteTombstoneBlockedModel::EqualityDelete,
        LocalDeleteTombstoneBlockedModel::ExternalTableMetadata,
        LocalDeleteTombstoneBlockedModel::CdcUpdateDeleteTombstone,
        LocalDeleteTombstoneBlockedModel::ObjectStoreDeleteManifest,
        LocalDeleteTombstoneBlockedModel::TableFormatDeleteRuntime,
    ]
}

fn local_delete_tombstone_fixture() -> Result<LocalDeleteTombstoneFixture> {
    let catalog = CatalogRef::new(
        CatalogKind::LocalManifest,
        "gar0020d-local-delete-tombstone",
    )?
    .with_namespace("fixtures.gar0020d")?;
    let manifest_id = ManifestId::new("gar0020d-local-delete-tombstone-v1")?;
    let snapshot_id = SnapshotId::new("gar0020d-snapshot-0001")?;
    let dataset_uri = DatasetUri::new("file://fixtures/gar0020d/orders.vortex")?;
    let dataset = DatasetRef::from_uri(dataset_uri)?
        .with_snapshot(snapshot_id.clone())
        .with_manifest(manifest_id.clone());
    let snapshot = SnapshotRef::new(snapshot_id.clone());
    let mut manifest = DatasetManifest::new(manifest_id, dataset, snapshot);

    let (kept_file, deleted_file, tombstoned_file) = local_delete_tombstone_files()?;
    manifest.add_file(kept_file.clone());
    manifest.add_file(deleted_file.clone());
    manifest.add_file(tombstoned_file.clone());
    add_local_delete_tombstone_segments(
        &mut manifest,
        &snapshot_id,
        kept_file,
        deleted_file,
        tombstoned_file,
    )?;

    Ok(LocalDeleteTombstoneFixture {
        catalog,
        manifest,
        schema: local_delete_tombstone_schema()?,
        rows: local_delete_tombstone_rows(),
        deleted_file_uri: local_delete_deleted_file_uri().to_string(),
        tombstoned_segment_id: "gar0020d-segment-tombstone",
    })
}

fn local_delete_kept_file_uri() -> &'static str {
    "file://fixtures/gar0020d/orders.vortex/part-keep.vortex"
}

fn local_delete_deleted_file_uri() -> &'static str {
    "file://fixtures/gar0020d/orders.vortex/part-delete.vortex"
}

fn local_delete_tombstoned_file_uri() -> &'static str {
    "file://fixtures/gar0020d/orders.vortex/part-tombstone.vortex"
}

fn local_delete_tombstone_file(uri: &'static str, size_bytes: u64) -> Result<FileDescriptor> {
    Ok(FileDescriptor::new(
        DatasetUri::new(uri)?,
        DatasetFormat::Vortex,
        FileRole::NativeVortexData,
    )
    .with_size_bytes(size_bytes))
}

fn local_delete_tombstone_files() -> Result<(FileDescriptor, FileDescriptor, FileDescriptor)> {
    Ok((
        local_delete_tombstone_file(local_delete_kept_file_uri(), 2048)?,
        local_delete_tombstone_file(local_delete_deleted_file_uri(), 1024)?,
        local_delete_tombstone_file(local_delete_tombstoned_file_uri(), 1024)?,
    ))
}

fn add_local_delete_tombstone_segments(
    manifest: &mut DatasetManifest,
    snapshot_id: &SnapshotId,
    kept_file: FileDescriptor,
    deleted_file: FileDescriptor,
    tombstoned_file: FileDescriptor,
) -> Result<()> {
    for (segment_id, file, row_count) in [
        ("gar0020d-segment-keep", kept_file, 3_u64),
        ("gar0020d-segment-file-delete", deleted_file, 2_u64),
        ("gar0020d-segment-tombstone", tombstoned_file, 1_u64),
    ] {
        manifest.add_segment(
            local_delete_tombstone_segment(segment_id, file, row_count)?
                .with_snapshot(snapshot_id.clone()),
        );
    }
    Ok(())
}

fn local_delete_tombstone_segment(
    segment_id: &'static str,
    file: FileDescriptor,
    row_count: u64,
) -> Result<ManifestSegment> {
    let layout = SegmentLayout::new(
        EncodingKind::VortexNative("fixture_delete_tombstone_smoke".to_string()),
        LayoutKind::VortexNative("local_delete_tombstone_smoke".to_string()),
    )
    .with_byte_ranges(vec![ByteRange::new(0, file.size_bytes.unwrap_or(0))]);
    let segment = EncodedSegment::new(
        SegmentId::new(segment_id)?,
        ColumnRef::new("order_id")?,
        LogicalDType::UInt64,
        Nullability::NonNullable,
        layout,
        SegmentStats::with_row_count(row_count),
    );
    Ok(ManifestSegment::new(segment, file))
}

fn local_delete_tombstone_schema() -> Result<SchemaDefinition> {
    let mut schema = SchemaDefinition::new(
        SchemaId::new("gar0020d-orders-schema")?,
        SchemaVersion::new(1)?,
    );
    schema.add_field(
        SchemaField::new(
            FieldName::new("order_id")?,
            LogicalDType::UInt64,
            Nullability::NonNullable,
        )
        .with_id(FieldId::new("field.order_id")?),
    );
    schema.add_field(
        SchemaField::new(
            FieldName::new("segment_marker")?,
            LogicalDType::Utf8,
            Nullability::NonNullable,
        )
        .with_id(FieldId::new("field.segment_marker")?),
    );
    Ok(schema)
}

fn local_delete_tombstone_rows() -> Vec<LocalDeleteTombstoneFixtureRow> {
    let kept_file_uri = local_delete_kept_file_uri();
    let deleted_file_uri = local_delete_deleted_file_uri();
    let tombstoned_file_uri = local_delete_tombstoned_file_uri();
    vec![
        LocalDeleteTombstoneFixtureRow {
            row_id: 1001,
            segment_id: "gar0020d-segment-keep",
            file_uri: kept_file_uri.to_string(),
        },
        LocalDeleteTombstoneFixtureRow {
            row_id: 1002,
            segment_id: "gar0020d-segment-keep",
            file_uri: kept_file_uri.to_string(),
        },
        LocalDeleteTombstoneFixtureRow {
            row_id: 1003,
            segment_id: "gar0020d-segment-keep",
            file_uri: kept_file_uri.to_string(),
        },
        LocalDeleteTombstoneFixtureRow {
            row_id: 2001,
            segment_id: "gar0020d-segment-file-delete",
            file_uri: deleted_file_uri.to_string(),
        },
        LocalDeleteTombstoneFixtureRow {
            row_id: 2002,
            segment_id: "gar0020d-segment-file-delete",
            file_uri: deleted_file_uri.to_string(),
        },
        LocalDeleteTombstoneFixtureRow {
            row_id: 3001,
            segment_id: "gar0020d-segment-tombstone",
            file_uri: tombstoned_file_uri.to_string(),
        },
    ]
}

fn local_delete_tombstone_effective_rows(
    fixture: &LocalDeleteTombstoneFixture,
) -> Vec<&LocalDeleteTombstoneFixtureRow> {
    fixture
        .rows
        .iter()
        .filter(|row| {
            row.file_uri != fixture.deleted_file_uri
                && row.segment_id != fixture.tombstoned_segment_id
        })
        .collect()
}

fn local_delete_tombstone_summary(
    fixture: &LocalDeleteTombstoneFixture,
    effective_row_ids: &[u64],
    file_deleted_rows: usize,
    segment_tombstoned_rows: usize,
) -> String {
    let effective_row_ids = effective_row_ids
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "fixture_id=gar0020d-local-delete-tombstone catalog_kind={} catalog_name={} dataset={} manifest={} snapshot={} schema={} admitted_delete_models={},{} rule=local_manifest_file_delete_and_segment_tombstone_admission row_identity=stable_fixture_row_id base_rows={} file_deleted_rows={} segment_tombstoned_rows={} effective_rows={} effective_row_ids={} fallback_attempted=false external_engine_invoked=false",
        fixture.catalog.kind.as_str(),
        fixture.catalog.name,
        fixture.manifest.dataset.uri.as_str(),
        fixture.manifest.id.as_str(),
        fixture.manifest.snapshot.id.as_str(),
        fixture.schema.id.as_str(),
        DeleteModel::FileLevelDelete.as_str(),
        DeleteModel::SegmentLevelTombstone.as_str(),
        fixture.rows.len(),
        file_deleted_rows,
        segment_tombstoned_rows,
        effective_row_ids
            .split(',')
            .filter(|value| !value.is_empty())
            .count(),
        effective_row_ids
    )
}

pub fn run_local_delete_tombstone_read_smoke() -> Result<LocalDeleteTombstoneReadSmokeReport> {
    let fixture = local_delete_tombstone_fixture()?;
    let effective_rows = local_delete_tombstone_effective_rows(&fixture);
    let effective_row_ids = effective_rows
        .iter()
        .map(|row| row.row_id)
        .collect::<Vec<_>>();
    let file_deleted_row_count = fixture
        .rows
        .iter()
        .filter(|row| row.file_uri == fixture.deleted_file_uri)
        .count();
    let segment_tombstoned_row_count = fixture
        .rows
        .iter()
        .filter(|row| row.segment_id == fixture.tombstoned_segment_id)
        .count();
    let correctness_summary = local_delete_tombstone_summary(
        &fixture,
        &effective_row_ids,
        file_deleted_row_count,
        segment_tombstoned_row_count,
    );
    let blocked_models = local_delete_tombstone_blocked_models();
    let diagnostics = blocked_models
        .iter()
        .map(|model| model.to_diagnostic())
        .collect();

    Ok(LocalDeleteTombstoneReadSmokeReport {
        schema_version: "shardloom.local_delete_tombstone_read_smoke.v1",
        report_id: "gar0020d.local_delete_tombstone_read_smoke",
        gar_id: "GAR-0020-D",
        support_status: "fixture_smoke_only",
        claim_gate_status: "scoped_local_delete_tombstone_smoke_only",
        claim_boundary: "one in-memory local manifest fixture applying file-level delete and segment tombstone admission; no row/position/equality delete runtime, object-store, lakehouse/catalog, table-format execution, production, or performance claim",
        fixture_id: "gar0020d-local-delete-tombstone",
        catalog_kind: fixture.catalog.kind.as_str(),
        catalog_ref_summary: fixture.catalog.summary(),
        dataset_uri: fixture.manifest.dataset.uri.as_str().to_string(),
        dataset_format: fixture.manifest.dataset.format.as_str().to_string(),
        manifest_id: fixture.manifest.id.as_str().to_string(),
        manifest_version: fixture.manifest.version.as_str().to_string(),
        snapshot_id: fixture.manifest.snapshot.id.as_str().to_string(),
        schema_id: fixture.schema.id.as_str().to_string(),
        admitted_delete_model_order: vec![
            DeleteModel::FileLevelDelete.as_str(),
            DeleteModel::SegmentLevelTombstone.as_str(),
        ],
        unsupported_delete_model_order: blocked_models
            .iter()
            .map(LocalDeleteTombstoneBlockedModel::as_str)
            .collect(),
        delete_tombstone_admission_rule: "local_manifest_file_delete_and_segment_tombstone_admission",
        row_identity_rule: "stable_fixture_row_id",
        base_row_count: fixture.rows.len(),
        file_deleted_row_count,
        segment_tombstoned_row_count,
        effective_row_count: effective_row_ids.len(),
        manifest_file_count: fixture.manifest.file_count(),
        manifest_segment_count: fixture.manifest.segment_count(),
        native_vortex_file_count: fixture.manifest.native_vortex_file_count(),
        admitted_file_delete_count: 1,
        admitted_segment_tombstone_count: 1,
        effective_row_ids,
        correctness_digest: stable_metadata_digest(&correctness_summary),
        correctness_summary,
        correctness_refs: "shardloom-core::table_intelligence::local_delete_tombstone_read_smoke",
        benchmark_refs: "not_required_fixture_smoke_no_performance_claim",
        execution_certificate_refs: "shardloom-cli/tests/local_delete_tombstone_read_smoke.rs",
        native_io_certificate_refs: "not_required_no_vortex_file_read_or_source_sink_io",
        materialization_decode_refs: "in_memory_fixture_row_identity_only_no_file_decode_no_table_materialization",
        policy_refs: "fallback_attempted=false,external_engine_invoked=false,object_store_io=false,table_metadata_write=false",
        local_catalog_ref_resolved: true,
        local_manifest_metadata_read_performed: true,
        in_memory_fixture_rows_read: true,
        delete_tombstone_rule_applied: true,
        result_row_order_preserved: true,
        table_metadata_write_performed: false,
        data_file_read_performed: false,
        object_store_io_performed: false,
        write_io_performed: false,
        credential_resolution_performed: false,
        external_table_format_dependency_invoked: false,
        fallback_attempted: false,
        fallback_execution_allowed: false,
        external_engine_invoked: false,
        performance_claim_allowed: false,
        table_format_execution_claim_allowed: false,
        production_table_catalog_claim_allowed: false,
        lakehouse_claim_allowed: false,
        blocked_models,
        diagnostics,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalAppendOnlyCdcOverlayBlockedPath {
    CdcUpdate,
    CdcDelete,
    CdcTombstone,
    ManifestSerialization,
    ManifestWrite,
    TransactionExecution,
    ObjectStoreCommit,
    TableCatalogCommit,
    TableFormatCdcRuntime,
}

impl LocalAppendOnlyCdcOverlayBlockedPath {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CdcUpdate => "cdc_update",
            Self::CdcDelete => "cdc_delete",
            Self::CdcTombstone => "cdc_tombstone",
            Self::ManifestSerialization => "manifest_serialization",
            Self::ManifestWrite => "manifest_write",
            Self::TransactionExecution => "transaction_execution",
            Self::ObjectStoreCommit => "object_store_commit",
            Self::TableCatalogCommit => "table_catalog_commit",
            Self::TableFormatCdcRuntime => "table_format_cdc_runtime",
        }
    }

    #[must_use]
    pub const fn diagnostic_code(&self) -> DiagnosticCode {
        match self {
            Self::ObjectStoreCommit => DiagnosticCode::ObjectStoreUnsupported,
            Self::TableCatalogCommit | Self::TransactionExecution => {
                DiagnosticCode::CommitNotAtomic
            }
            Self::TableFormatCdcRuntime => DiagnosticCode::ExternalEffectDisabled,
            Self::CdcUpdate
            | Self::CdcDelete
            | Self::CdcTombstone
            | Self::ManifestSerialization
            | Self::ManifestWrite => DiagnosticCode::NotImplemented,
        }
    }

    #[must_use]
    pub const fn diagnostic_category(&self) -> DiagnosticCategory {
        match self {
            Self::ObjectStoreCommit => DiagnosticCategory::ObjectStore,
            Self::TableFormatCdcRuntime => DiagnosticCategory::ExternalEffect,
            Self::CdcUpdate
            | Self::CdcDelete
            | Self::CdcTombstone
            | Self::ManifestSerialization
            | Self::ManifestWrite
            | Self::TransactionExecution
            | Self::TableCatalogCommit => DiagnosticCategory::Execution,
        }
    }

    #[must_use]
    pub fn to_diagnostic(self) -> Diagnostic {
        Diagnostic::new(
            self.diagnostic_code(),
            DiagnosticSeverity::Info,
            self.diagnostic_category(),
            format!(
                "{} remains blocked outside GAR-0020-E's append-only local CDC overlay fixture",
                self.as_str()
            ),
            Some(self.as_str().to_string()),
            Some(
                "GAR-0020-E only overlays declared in-memory append rows on a base local snapshot; it performs no manifest serialization, transaction execution, object-store commit, or update/delete/tombstone CDC runtime."
                    .to_string(),
            ),
            Some(
                "Promote these paths only with dedicated row identity, delete/tombstone, manifest write, commit, object-store, and table-format evidence."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct LocalAppendOnlyCdcOverlaySmokeReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub gar_id: &'static str,
    pub support_status: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub fixture_id: &'static str,
    pub catalog_kind: &'static str,
    pub catalog_ref_summary: String,
    pub dataset_uri: String,
    pub dataset_format: String,
    pub base_manifest_id: String,
    pub delta_manifest_id: String,
    pub base_snapshot_id: String,
    pub delta_snapshot_id: String,
    pub schema_id: String,
    pub incremental_plan_report_ref: &'static str,
    pub incremental_status: &'static str,
    pub change_set_from_snapshot: String,
    pub change_set_to_snapshot: String,
    pub overlay_rule: &'static str,
    pub cdc_event_order: Vec<&'static str>,
    pub blocked_path_order: Vec<&'static str>,
    pub base_row_count: usize,
    pub append_row_count: usize,
    pub effective_row_count: usize,
    pub base_manifest_file_count: usize,
    pub delta_manifest_file_count: usize,
    pub base_manifest_segment_count: usize,
    pub delta_manifest_segment_count: usize,
    pub changed_segment_count: usize,
    pub insert_count: usize,
    pub update_count: usize,
    pub delete_count: usize,
    pub tombstone_count: usize,
    pub unsupported_change_count: usize,
    pub base_row_ids: Vec<u64>,
    pub appended_row_ids: Vec<u64>,
    pub effective_row_ids: Vec<u64>,
    pub correctness_summary: String,
    pub correctness_digest: String,
    pub correctness_refs: &'static str,
    pub benchmark_refs: &'static str,
    pub execution_certificate_refs: &'static str,
    pub native_io_certificate_refs: &'static str,
    pub materialization_decode_refs: &'static str,
    pub policy_refs: &'static str,
    pub local_catalog_ref_resolved: bool,
    pub local_base_snapshot_declared: bool,
    pub local_append_delta_declared: bool,
    pub cdc_incremental_plan_evaluated: bool,
    pub append_overlay_rule_applied: bool,
    pub result_row_order_preserved: bool,
    pub table_metadata_write_performed: bool,
    pub manifest_write_performed: bool,
    pub transaction_execution_performed: bool,
    pub commit_execution_performed: bool,
    pub data_file_read_performed: bool,
    pub object_store_io_performed: bool,
    pub write_io_performed: bool,
    pub credential_resolution_performed: bool,
    pub external_table_format_dependency_invoked: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub performance_claim_allowed: bool,
    pub production_incremental_claim_allowed: bool,
    pub lakehouse_claim_allowed: bool,
    pub blocked_paths: Vec<LocalAppendOnlyCdcOverlayBlockedPath>,
    pub diagnostics: Vec<Diagnostic>,
}

impl LocalAppendOnlyCdcOverlaySmokeReport {
    #[must_use]
    pub fn fixture_smoke_supported(&self) -> bool {
        self.support_status == "fixture_smoke_only"
            && self.incremental_status == "execute_changed_segments_only"
            && self.local_catalog_ref_resolved
            && self.local_base_snapshot_declared
            && self.local_append_delta_declared
            && self.cdc_incremental_plan_evaluated
            && self.append_overlay_rule_applied
            && self.base_row_count + self.append_row_count == self.effective_row_count
            && self.insert_count == self.append_row_count
            && self.unsupported_change_count == 0
    }

    #[must_use]
    pub fn claim_scoped(&self) -> bool {
        self.claim_gate_status == "scoped_append_only_cdc_overlay_smoke_only"
            && !self.performance_claim_allowed
            && !self.production_incremental_claim_allowed
            && !self.lakehouse_claim_allowed
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.table_metadata_write_performed
            && !self.manifest_write_performed
            && !self.transaction_execution_performed
            && !self.commit_execution_performed
            && !self.data_file_read_performed
            && !self.object_store_io_performed
            && !self.write_io_performed
            && !self.credential_resolution_performed
            && !self.external_table_format_dependency_invoked
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && !self.external_engine_invoked
    }

    #[must_use]
    pub fn unsupported_diagnostic_count(&self) -> usize {
        self.blocked_paths
            .iter()
            .filter(|path| {
                self.diagnostics.iter().any(|diagnostic| {
                    diagnostic.code == path.diagnostic_code()
                        && diagnostic.category == path.diagnostic_category()
                        && diagnostic.severity == DiagnosticSeverity::Info
                        && diagnostic.feature.as_deref() == Some(path.as_str())
                        && !diagnostic.fallback.attempted
                        && !diagnostic.fallback.allowed
                })
            })
            .count()
    }

    #[must_use]
    pub fn deterministic_unsupported_diagnostics_ready(&self) -> bool {
        !self.blocked_paths.is_empty()
            && self.unsupported_diagnostic_count() == self.blocked_paths.len()
    }

    #[must_use]
    pub fn blocked_path_order(&self) -> Vec<&'static str> {
        self.blocked_paths
            .iter()
            .map(LocalAppendOnlyCdcOverlayBlockedPath::as_str)
            .collect()
    }

    #[must_use]
    pub fn unsupported_diagnostic_code_order(&self) -> Vec<&'static str> {
        self.blocked_paths
            .iter()
            .map(|path| path.diagnostic_code().as_str())
            .collect()
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.fixture_smoke_supported()
            || !self.claim_scoped()
            || !self.side_effect_free()
            || !self.deterministic_unsupported_diagnostics_ready()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(out, "gar_id: {}", self.gar_id);
        let _ = writeln!(out, "support_status: {}", self.support_status);
        let _ = writeln!(out, "claim_gate_status: {}", self.claim_gate_status);
        let _ = writeln!(out, "fixture_id: {}", self.fixture_id);
        let _ = writeln!(out, "incremental_status: {}", self.incremental_status);
        let _ = writeln!(
            out,
            "rows: base={} appended={} effective={}",
            self.base_row_count, self.append_row_count, self.effective_row_count
        );
        let _ = writeln!(out, "overlay_rule: {}", self.overlay_rule);
        let _ = writeln!(out, "correctness_digest: {}", self.correctness_digest);
        let _ = writeln!(out, "side_effect_free: {}", self.side_effect_free());
        let _ = writeln!(out, "fallback_attempted: {}", self.fallback_attempted);
        let _ = writeln!(
            out,
            "external_engine_invoked: {}",
            self.external_engine_invoked
        );
        let _ = writeln!(
            out,
            "blocked_paths: {}",
            self.blocked_path_order().join(",")
        );
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalAppendOnlyCdcOverlayFixtureRow {
    row_id: u64,
    segment_id: &'static str,
    snapshot_role: &'static str,
}

#[derive(Debug, Clone)]
struct LocalAppendOnlyCdcOverlayFixture {
    catalog: CatalogRef,
    base_manifest: DatasetManifest,
    delta_manifest: DatasetManifest,
    schema: SchemaDefinition,
    base_rows: Vec<LocalAppendOnlyCdcOverlayFixtureRow>,
    appended_rows: Vec<LocalAppendOnlyCdcOverlayFixtureRow>,
}

#[must_use]
fn local_append_only_cdc_blocked_paths() -> Vec<LocalAppendOnlyCdcOverlayBlockedPath> {
    vec![
        LocalAppendOnlyCdcOverlayBlockedPath::CdcUpdate,
        LocalAppendOnlyCdcOverlayBlockedPath::CdcDelete,
        LocalAppendOnlyCdcOverlayBlockedPath::CdcTombstone,
        LocalAppendOnlyCdcOverlayBlockedPath::ManifestSerialization,
        LocalAppendOnlyCdcOverlayBlockedPath::ManifestWrite,
        LocalAppendOnlyCdcOverlayBlockedPath::TransactionExecution,
        LocalAppendOnlyCdcOverlayBlockedPath::ObjectStoreCommit,
        LocalAppendOnlyCdcOverlayBlockedPath::TableCatalogCommit,
        LocalAppendOnlyCdcOverlayBlockedPath::TableFormatCdcRuntime,
    ]
}

fn local_append_only_cdc_overlay_fixture() -> Result<LocalAppendOnlyCdcOverlayFixture> {
    let catalog = CatalogRef::new(CatalogKind::LocalManifest, "gar0020e-local-cdc-overlay")?
        .with_namespace("fixtures.gar0020e")?;
    let base_snapshot = SnapshotId::new("gar0020e-base-snapshot-0001")?;
    let delta_snapshot = SnapshotId::new("gar0020e-delta-snapshot-0002")?;
    Ok(LocalAppendOnlyCdcOverlayFixture {
        catalog,
        base_manifest: local_append_only_cdc_manifest(
            "gar0020e-base-manifest-v1",
            base_snapshot,
            local_append_only_cdc_base_file_uri(),
            "gar0020e-segment-base",
            3,
        )?,
        delta_manifest: local_append_only_cdc_manifest(
            "gar0020e-delta-manifest-v1",
            delta_snapshot,
            local_append_only_cdc_delta_file_uri(),
            "gar0020e-segment-append",
            2,
        )?,
        schema: local_append_only_cdc_schema()?,
        base_rows: local_append_only_cdc_base_rows(),
        appended_rows: local_append_only_cdc_appended_rows(),
    })
}

fn local_append_only_cdc_dataset_uri() -> &'static str {
    "file://fixtures/gar0020e/orders.vortex"
}

fn local_append_only_cdc_base_file_uri() -> &'static str {
    "file://fixtures/gar0020e/orders.vortex/base-part.vortex"
}

fn local_append_only_cdc_delta_file_uri() -> &'static str {
    "file://fixtures/gar0020e/orders.vortex/append-delta.vortex"
}

fn local_append_only_cdc_manifest(
    manifest_id: &'static str,
    snapshot_id: SnapshotId,
    file_uri: &'static str,
    segment_id: &'static str,
    row_count: u64,
) -> Result<DatasetManifest> {
    let manifest_id = ManifestId::new(manifest_id)?;
    let dataset = DatasetRef::from_uri(DatasetUri::new(local_append_only_cdc_dataset_uri())?)?
        .with_snapshot(snapshot_id.clone())
        .with_manifest(manifest_id.clone());
    let snapshot = SnapshotRef::new(snapshot_id.clone());
    let mut manifest = DatasetManifest::new(manifest_id, dataset, snapshot);
    let file = FileDescriptor::new(
        DatasetUri::new(file_uri)?,
        DatasetFormat::Vortex,
        FileRole::NativeVortexData,
    )
    .with_size_bytes(row_count * 512);
    manifest.add_file(file.clone());
    manifest.add_segment(
        local_append_only_cdc_segment(segment_id, row_count, file)?.with_snapshot(snapshot_id),
    );
    Ok(manifest)
}

fn local_append_only_cdc_segment(
    segment_id: &'static str,
    row_count: u64,
    file: FileDescriptor,
) -> Result<ManifestSegment> {
    let layout = SegmentLayout::new(
        EncodingKind::VortexNative("fixture_append_only_cdc_overlay".to_string()),
        LayoutKind::VortexNative("local_append_only_cdc_overlay".to_string()),
    )
    .with_byte_ranges(vec![ByteRange::new(0, file.size_bytes.unwrap_or(0))]);
    let segment = EncodedSegment::new(
        SegmentId::new(segment_id)?,
        ColumnRef::new("order_id")?,
        LogicalDType::UInt64,
        Nullability::NonNullable,
        layout,
        SegmentStats::with_row_count(row_count),
    );
    Ok(ManifestSegment::new(segment, file))
}

fn local_append_only_cdc_schema() -> Result<SchemaDefinition> {
    let mut schema = SchemaDefinition::new(
        SchemaId::new("gar0020e-orders-schema")?,
        SchemaVersion::new(1)?,
    );
    schema.add_field(
        SchemaField::new(
            FieldName::new("order_id")?,
            LogicalDType::UInt64,
            Nullability::NonNullable,
        )
        .with_id(FieldId::new("field.order_id")?),
    );
    schema.add_field(
        SchemaField::new(
            FieldName::new("snapshot_role")?,
            LogicalDType::Utf8,
            Nullability::NonNullable,
        )
        .with_id(FieldId::new("field.snapshot_role")?),
    );
    Ok(schema)
}

fn local_append_only_cdc_base_rows() -> Vec<LocalAppendOnlyCdcOverlayFixtureRow> {
    vec![
        LocalAppendOnlyCdcOverlayFixtureRow {
            row_id: 1001,
            segment_id: "gar0020e-segment-base",
            snapshot_role: "base",
        },
        LocalAppendOnlyCdcOverlayFixtureRow {
            row_id: 1002,
            segment_id: "gar0020e-segment-base",
            snapshot_role: "base",
        },
        LocalAppendOnlyCdcOverlayFixtureRow {
            row_id: 1003,
            segment_id: "gar0020e-segment-base",
            snapshot_role: "base",
        },
    ]
}

fn local_append_only_cdc_appended_rows() -> Vec<LocalAppendOnlyCdcOverlayFixtureRow> {
    vec![
        LocalAppendOnlyCdcOverlayFixtureRow {
            row_id: 4001,
            segment_id: "gar0020e-segment-append",
            snapshot_role: "append_delta",
        },
        LocalAppendOnlyCdcOverlayFixtureRow {
            row_id: 4002,
            segment_id: "gar0020e-segment-append",
            snapshot_role: "append_delta",
        },
    ]
}

fn local_append_only_cdc_change_set(
    fixture: &LocalAppendOnlyCdcOverlayFixture,
) -> Result<ChangeSet> {
    let mut change_set = ChangeSet::between(
        fixture.base_manifest.snapshot.id.clone(),
        fixture.delta_manifest.snapshot.id.clone(),
    );
    change_set.add_change(
        SegmentChange::new(
            SegmentChangeKind::Added,
            SegmentId::new("gar0020e-segment-append")?,
        )
        .with_reason("append-only CDC delta declared by local fixture"),
    );
    Ok(change_set)
}

fn local_append_only_cdc_effective_row_ids(fixture: &LocalAppendOnlyCdcOverlayFixture) -> Vec<u64> {
    fixture
        .base_rows
        .iter()
        .chain(fixture.appended_rows.iter())
        .map(|row| row.row_id)
        .collect()
}

fn local_append_only_cdc_summary(
    fixture: &LocalAppendOnlyCdcOverlayFixture,
    cdc_plan: &CdcIncrementalPlanningReport,
    effective_row_ids: &[u64],
) -> String {
    let base_row_ids = fixture
        .base_rows
        .iter()
        .map(|row| row.row_id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let appended_row_ids = fixture
        .appended_rows
        .iter()
        .map(|row| row.row_id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let base_row_segments = fixture
        .base_rows
        .iter()
        .map(|row| format!("{}:{}", row.row_id, row.segment_id))
        .collect::<Vec<_>>()
        .join(",");
    let appended_row_segments = fixture
        .appended_rows
        .iter()
        .map(|row| format!("{}:{}", row.row_id, row.segment_id))
        .collect::<Vec<_>>()
        .join(",");
    let row_roles = fixture
        .base_rows
        .iter()
        .chain(fixture.appended_rows.iter())
        .map(|row| format!("{}:{}", row.row_id, row.snapshot_role))
        .collect::<Vec<_>>()
        .join(",");
    let effective_row_ids = effective_row_ids
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "fixture_id=gar0020e-local-append-only-cdc-overlay catalog_kind={} catalog_name={} dataset={} base_manifest={} delta_manifest={} from_snapshot={} to_snapshot={} schema={} incremental_status={} changed_segments={} inserts={} updates={} deletes={} tombstones={} overlay_rule=base_snapshot_then_append_delta base_row_ids={} appended_row_ids={} base_row_segments={} appended_row_segments={} row_roles={} effective_row_ids={} fallback_attempted=false external_engine_invoked=false manifest_write=false transaction_execution=false",
        fixture.catalog.kind.as_str(),
        fixture.catalog.name,
        fixture.base_manifest.dataset.uri.as_str(),
        fixture.base_manifest.id.as_str(),
        fixture.delta_manifest.id.as_str(),
        fixture.base_manifest.snapshot.id.as_str(),
        fixture.delta_manifest.snapshot.id.as_str(),
        fixture.schema.id.as_str(),
        cdc_plan.status.as_str(),
        cdc_plan.changed_segment_count,
        cdc_plan.insert_count,
        cdc_plan.update_count,
        cdc_plan.delete_count,
        cdc_plan.tombstone_count,
        base_row_ids,
        appended_row_ids,
        base_row_segments,
        appended_row_segments,
        row_roles,
        effective_row_ids
    )
}

fn local_append_only_cdc_row_ids(
    fixture: &LocalAppendOnlyCdcOverlayFixture,
) -> (Vec<u64>, Vec<u64>, Vec<u64>) {
    let effective_row_ids = local_append_only_cdc_effective_row_ids(fixture);
    let base_row_ids = fixture
        .base_rows
        .iter()
        .map(|row| row.row_id)
        .collect::<Vec<_>>();
    let appended_row_ids = fixture
        .appended_rows
        .iter()
        .map(|row| row.row_id)
        .collect::<Vec<_>>();
    (base_row_ids, appended_row_ids, effective_row_ids)
}

pub fn run_local_append_only_cdc_overlay_smoke() -> Result<LocalAppendOnlyCdcOverlaySmokeReport> {
    let fixture = local_append_only_cdc_overlay_fixture()?;
    let change_set = local_append_only_cdc_change_set(&fixture)?;
    let cdc_plan = evaluate_cdc_incremental_planning(
        change_set,
        vec![CdcEventSummary::new(
            CdcEventKind::Insert,
            fixture.appended_rows.len(),
        )],
    );
    let (base_row_ids, appended_row_ids, effective_row_ids) =
        local_append_only_cdc_row_ids(&fixture);
    let correctness_summary =
        local_append_only_cdc_summary(&fixture, &cdc_plan, &effective_row_ids);
    let blocked_paths = local_append_only_cdc_blocked_paths();
    let diagnostics = blocked_paths
        .iter()
        .map(|path| path.to_diagnostic())
        .collect();

    Ok(LocalAppendOnlyCdcOverlaySmokeReport {
        schema_version: "shardloom.local_append_only_cdc_overlay_smoke.v1",
        report_id: "gar0020e.local_append_only_cdc_overlay_smoke",
        gar_id: "GAR-0020-E",
        support_status: "fixture_smoke_only",
        claim_gate_status: "scoped_append_only_cdc_overlay_smoke_only",
        claim_boundary: "one in-memory local append-only CDC overlay fixture combining a base snapshot and append delta; no update/delete/tombstone CDC runtime, manifest write, transaction, object-store, lakehouse/catalog, production incremental, or performance claim",
        fixture_id: "gar0020e-local-append-only-cdc-overlay",
        catalog_kind: fixture.catalog.kind.as_str(),
        catalog_ref_summary: fixture.catalog.summary(),
        dataset_uri: fixture.base_manifest.dataset.uri.as_str().to_string(),
        dataset_format: fixture.base_manifest.dataset.format.as_str().to_string(),
        base_manifest_id: fixture.base_manifest.id.as_str().to_string(),
        delta_manifest_id: fixture.delta_manifest.id.as_str().to_string(),
        base_snapshot_id: fixture.base_manifest.snapshot.id.as_str().to_string(),
        delta_snapshot_id: fixture.delta_manifest.snapshot.id.as_str().to_string(),
        schema_id: fixture.schema.id.as_str().to_string(),
        incremental_plan_report_ref: "shardloom.cdc_incremental_planning.v1",
        incremental_status: cdc_plan.status.as_str(),
        change_set_from_snapshot: fixture.base_manifest.snapshot.id.as_str().to_string(),
        change_set_to_snapshot: fixture.delta_manifest.snapshot.id.as_str().to_string(),
        overlay_rule: "base_snapshot_then_append_delta",
        cdc_event_order: vec![CdcEventKind::Insert.as_str()],
        blocked_path_order: blocked_paths
            .iter()
            .map(LocalAppendOnlyCdcOverlayBlockedPath::as_str)
            .collect(),
        base_row_count: fixture.base_rows.len(),
        append_row_count: fixture.appended_rows.len(),
        effective_row_count: effective_row_ids.len(),
        base_manifest_file_count: fixture.base_manifest.file_count(),
        delta_manifest_file_count: fixture.delta_manifest.file_count(),
        base_manifest_segment_count: fixture.base_manifest.segment_count(),
        delta_manifest_segment_count: fixture.delta_manifest.segment_count(),
        changed_segment_count: cdc_plan.changed_segment_count,
        insert_count: cdc_plan.insert_count,
        update_count: cdc_plan.update_count,
        delete_count: cdc_plan.delete_count,
        tombstone_count: cdc_plan.tombstone_count,
        unsupported_change_count: cdc_plan.unsupported_change_count,
        base_row_ids,
        appended_row_ids,
        effective_row_ids,
        correctness_digest: stable_metadata_digest(&correctness_summary),
        correctness_summary,
        correctness_refs: "shardloom-core::table_intelligence::local_append_only_cdc_overlay_smoke",
        benchmark_refs: "not_required_fixture_smoke_no_performance_claim",
        execution_certificate_refs: "shardloom-cli/tests/local_append_only_cdc_overlay_smoke.rs",
        native_io_certificate_refs: "not_required_no_vortex_file_read_or_source_sink_io",
        materialization_decode_refs: "in_memory_base_and_append_rows_only_no_file_decode_no_table_materialization",
        policy_refs: "fallback_attempted=false,external_engine_invoked=false,manifest_write=false,transaction_execution=false,object_store_io=false",
        local_catalog_ref_resolved: true,
        local_base_snapshot_declared: true,
        local_append_delta_declared: true,
        cdc_incremental_plan_evaluated: true,
        append_overlay_rule_applied: true,
        result_row_order_preserved: true,
        table_metadata_write_performed: false,
        manifest_write_performed: false,
        transaction_execution_performed: false,
        commit_execution_performed: false,
        data_file_read_performed: false,
        object_store_io_performed: false,
        write_io_performed: false,
        credential_resolution_performed: false,
        external_table_format_dependency_invoked: false,
        fallback_attempted: false,
        fallback_execution_allowed: false,
        external_engine_invoked: false,
        performance_claim_allowed: false,
        production_incremental_claim_allowed: false,
        lakehouse_claim_allowed: false,
        blocked_paths,
        diagnostics,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcManifestTransactionSurface {
    CdcReadIntent,
    CdcWriteIntent,
    ManifestSerialization,
    ManifestMetadataRead,
    ObjectStoreCommit,
    TableCatalogCommit,
    TransactionExecution,
    UnsupportedCommitDiagnostic,
}

impl CdcManifestTransactionSurface {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CdcReadIntent => "cdc_read_intent",
            Self::CdcWriteIntent => "cdc_write_intent",
            Self::ManifestSerialization => "manifest_serialization",
            Self::ManifestMetadataRead => "manifest_metadata_read",
            Self::ObjectStoreCommit => "object_store_commit",
            Self::TableCatalogCommit => "table_catalog_commit",
            Self::TransactionExecution => "transaction_execution",
            Self::UnsupportedCommitDiagnostic => "unsupported_commit_diagnostic",
        }
    }

    #[must_use]
    pub const fn diagnostic_code(&self) -> DiagnosticCode {
        match self {
            Self::ObjectStoreCommit => DiagnosticCode::ObjectStoreUnsupported,
            Self::TableCatalogCommit | Self::TransactionExecution => {
                DiagnosticCode::CommitNotAtomic
            }
            Self::CdcReadIntent
            | Self::CdcWriteIntent
            | Self::ManifestSerialization
            | Self::ManifestMetadataRead
            | Self::UnsupportedCommitDiagnostic => DiagnosticCode::NotImplemented,
        }
    }

    #[must_use]
    pub const fn diagnostic_category(&self) -> DiagnosticCategory {
        match self {
            Self::ObjectStoreCommit => DiagnosticCategory::ObjectStore,
            Self::ManifestSerialization => DiagnosticCategory::Translation,
            Self::TableCatalogCommit | Self::TransactionExecution => DiagnosticCategory::Execution,
            Self::CdcReadIntent
            | Self::CdcWriteIntent
            | Self::ManifestMetadataRead
            | Self::UnsupportedCommitDiagnostic => DiagnosticCategory::Planning,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcManifestTransactionStatus {
    ReportOnlyAvailable,
    UnsupportedUntilCertified,
}

impl CdcManifestTransactionStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnlyAvailable => "report_only_available",
            Self::UnsupportedUntilCertified => "unsupported_until_certified",
        }
    }

    #[must_use]
    pub const fn is_report_only(&self) -> bool {
        matches!(self, Self::ReportOnlyAvailable)
    }

    #[must_use]
    pub const fn is_unsupported(&self) -> bool {
        matches!(self, Self::UnsupportedUntilCertified)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct CdcManifestTransactionGateEntry {
    pub surface: CdcManifestTransactionSurface,
    pub status: CdcManifestTransactionStatus,
    pub existing_report_ref: Option<&'static str>,
    pub required_evidence: &'static str,
    pub report_only_available: bool,
    pub cdc_read_runtime_allowed: bool,
    pub cdc_write_runtime_allowed: bool,
    pub manifest_serialization_allowed: bool,
    pub manifest_metadata_read_allowed: bool,
    pub transaction_execution_allowed: bool,
    pub commit_execution_allowed: bool,
    pub data_read: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: &'static str,
}

impl CdcManifestTransactionGateEntry {
    #[must_use]
    pub const fn report_only(
        surface: CdcManifestTransactionSurface,
        existing_report_ref: &'static str,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            surface,
            status: CdcManifestTransactionStatus::ReportOnlyAvailable,
            existing_report_ref: Some(existing_report_ref),
            required_evidence,
            report_only_available: true,
            cdc_read_runtime_allowed: false,
            cdc_write_runtime_allowed: false,
            manifest_serialization_allowed: false,
            manifest_metadata_read_allowed: false,
            transaction_execution_allowed: false,
            commit_execution_allowed: false,
            data_read: false,
            object_store_io: false,
            write_io: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn unsupported(
        surface: CdcManifestTransactionSurface,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            surface,
            status: CdcManifestTransactionStatus::UnsupportedUntilCertified,
            existing_report_ref: None,
            required_evidence,
            report_only_available: false,
            cdc_read_runtime_allowed: false,
            cdc_write_runtime_allowed: false,
            manifest_serialization_allowed: false,
            manifest_metadata_read_allowed: false,
            transaction_execution_allowed: false,
            commit_execution_allowed: false,
            data_read: false,
            object_store_io: false,
            write_io: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.cdc_read_runtime_allowed
            && !self.cdc_write_runtime_allowed
            && !self.manifest_serialization_allowed
            && !self.manifest_metadata_read_allowed
            && !self.transaction_execution_allowed
            && !self.commit_execution_allowed
            && !self.data_read
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && !self.external_engine_invoked
    }

    #[must_use]
    pub fn to_diagnostic(&self) -> Option<Diagnostic> {
        if !self.status.is_unsupported() {
            return None;
        }
        Some(Diagnostic::new(
            self.surface.diagnostic_code(),
            DiagnosticSeverity::Info,
            self.surface.diagnostic_category(),
            format!("{} is unsupported until certified", self.surface.as_str()),
            Some(self.surface.as_str().to_string()),
            Some(format!(
                "{} requires {} before runtime promotion.",
                self.surface.as_str(),
                self.required_evidence
            )),
            Some(
                "Keep this lane report-only and attach evidence before enabling execution."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct CdcManifestTransactionGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub entries: Vec<CdcManifestTransactionGateEntry>,
    pub existing_report_refs: Vec<&'static str>,
    pub existing_cdc_planning_present: bool,
    pub existing_manifest_contract_present: bool,
    pub existing_object_store_commit_protocol_present: bool,
    pub existing_local_staged_manifest_helpers_present: bool,
    pub cdc_read_intent_report_only_available: bool,
    pub cdc_write_intent_allowed: bool,
    pub manifest_serialization_allowed: bool,
    pub manifest_metadata_read_allowed: bool,
    pub transaction_execution_allowed: bool,
    pub commit_execution_allowed: bool,
    pub object_store_io_allowed: bool,
    pub data_read_allowed: bool,
    pub write_io_allowed: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub cdc_transaction_claim_allowed: bool,
    pub claim_gate_status: &'static str,
    pub snapshot_pair_required: bool,
    pub row_identity_required: bool,
    pub manifest_schema_required: bool,
    pub manifest_serialization_evidence_required: bool,
    pub transaction_protocol_required: bool,
    pub commit_protocol_required: bool,
    pub object_store_provider_evidence_required: bool,
    pub execution_certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub no_fallback_policy_required: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl CdcManifestTransactionGateReport {
    #[must_use]
    pub fn planning_default() -> Self {
        let entries = cdc_manifest_transaction_gate_entries();
        let diagnostics = entries
            .iter()
            .filter_map(CdcManifestTransactionGateEntry::to_diagnostic)
            .collect();
        Self {
            schema_version: "shardloom.cdc_manifest_transaction_gate.v1",
            report_id: "gar0004a.cdc_manifest_transaction_gate",
            entries,
            existing_report_refs: cdc_manifest_transaction_existing_report_refs(),
            existing_cdc_planning_present: true,
            existing_manifest_contract_present: true,
            existing_object_store_commit_protocol_present: true,
            existing_local_staged_manifest_helpers_present: true,
            cdc_read_intent_report_only_available: true,
            cdc_write_intent_allowed: false,
            manifest_serialization_allowed: false,
            manifest_metadata_read_allowed: false,
            transaction_execution_allowed: false,
            commit_execution_allowed: false,
            object_store_io_allowed: false,
            data_read_allowed: false,
            write_io_allowed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            external_engine_invoked: false,
            cdc_transaction_claim_allowed: false,
            claim_gate_status: "not_claim_grade",
            snapshot_pair_required: true,
            row_identity_required: true,
            manifest_schema_required: true,
            manifest_serialization_evidence_required: true,
            transaction_protocol_required: true,
            commit_protocol_required: true,
            object_store_provider_evidence_required: true,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            no_fallback_policy_required: true,
            diagnostics,
        }
    }

    #[must_use]
    pub fn surface_count(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn report_only_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_report_only())
            .count()
    }

    #[must_use]
    pub fn unsupported_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_unsupported())
            .count()
    }

    #[must_use]
    pub fn surface_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.surface.as_str())
            .collect()
    }

    #[must_use]
    pub fn runtime_promotions_blocked(&self) -> bool {
        !self.cdc_write_intent_allowed
            && !self.manifest_serialization_allowed
            && !self.manifest_metadata_read_allowed
            && !self.transaction_execution_allowed
            && !self.commit_execution_allowed
            && !self.object_store_io_allowed
            && !self.data_read_allowed
            && !self.write_io_allowed
            && self
                .entries
                .iter()
                .all(CdcManifestTransactionGateEntry::side_effect_free)
    }

    #[must_use]
    pub fn claim_blocked(&self) -> bool {
        !self.cdc_transaction_claim_allowed && self.claim_gate_status == "not_claim_grade"
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        self.runtime_promotions_blocked()
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && !self.external_engine_invoked
    }

    #[must_use]
    pub fn unsupported_diagnostic_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_unsupported())
            .filter(|entry| {
                self.diagnostics.iter().any(|diagnostic| {
                    diagnostic.code == entry.surface.diagnostic_code()
                        && diagnostic.category == entry.surface.diagnostic_category()
                        && diagnostic.severity == DiagnosticSeverity::Info
                        && diagnostic.feature.as_deref() == Some(entry.surface.as_str())
                        && !diagnostic.fallback.attempted
                        && !diagnostic.fallback.allowed
                })
            })
            .count()
    }

    #[must_use]
    pub fn deterministic_unsupported_diagnostics_ready(&self) -> bool {
        self.unsupported_surface_count() > 0
            && self.unsupported_diagnostic_count() == self.unsupported_surface_count()
    }

    #[must_use]
    pub fn unsupported_diagnostic_code_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_unsupported())
            .map(|entry| entry.surface.diagnostic_code().as_str())
            .collect()
    }

    #[must_use]
    pub fn unsupported_diagnostic_category_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_unsupported())
            .map(|entry| entry.surface.diagnostic_category().as_str())
            .collect()
    }

    #[must_use]
    pub fn unsupported_diagnostic_severity_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_unsupported())
            .map(|_| DiagnosticSeverity::Info.as_str())
            .collect()
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || !self.claim_blocked()
            || !self.deterministic_unsupported_diagnostics_ready()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(
            out,
            "existing report refs: {}",
            self.existing_report_refs.join(",")
        );
        let _ = writeln!(
            out,
            "runtime promotions blocked: {}",
            self.runtime_promotions_blocked()
        );
        let _ = writeln!(out, "claim blocked: {}", self.claim_blocked());
        let _ = writeln!(
            out,
            "deterministic unsupported diagnostics ready: {}",
            self.deterministic_unsupported_diagnostics_ready()
        );
        let _ = writeln!(out, "side effect free: {}", self.side_effect_free());
        let _ = writeln!(out, "surfaces:");
        for entry in &self.entries {
            let _ = writeln!(
                out,
                "  - {} [{}] existing_ref={} report_only={} required_evidence={} runtime_allowed=false fallback_attempted=false external_engine_invoked=false claim_gate_status={}",
                entry.surface.as_str(),
                entry.status.as_str(),
                entry.existing_report_ref.unwrap_or("none"),
                entry.report_only_available,
                entry.required_evidence,
                entry.claim_gate_status
            );
        }
        out
    }
}

fn cdc_manifest_transaction_gate_entries() -> Vec<CdcManifestTransactionGateEntry> {
    vec![
        CdcManifestTransactionGateEntry::report_only(
            CdcManifestTransactionSurface::CdcReadIntent,
            "shardloom.cdc_incremental_planning.v1",
            "declared_change_set,cdc_event_summary,incremental-plan cdc",
        ),
        CdcManifestTransactionGateEntry::unsupported(
            CdcManifestTransactionSurface::CdcWriteIntent,
            "write_intent,staged_manifest,commit_protocol,recovery_certificate",
        ),
        CdcManifestTransactionGateEntry::unsupported(
            CdcManifestTransactionSurface::ManifestSerialization,
            "generalized_manifest_schema,artifact_write_policy,native_io_certificate",
        ),
        CdcManifestTransactionGateEntry::unsupported(
            CdcManifestTransactionSurface::ManifestMetadataRead,
            "snapshot_ref,catalog_or_manifest_location,object_store_provider_policy,native_io_certificate",
        ),
        CdcManifestTransactionGateEntry::unsupported(
            CdcManifestTransactionSurface::ObjectStoreCommit,
            "object_store_commit_protocol,atomicity_evidence,idempotency_key_contract,cleanup_policy",
        ),
        CdcManifestTransactionGateEntry::unsupported(
            CdcManifestTransactionSurface::TableCatalogCommit,
            "catalog_transaction_protocol,table_metadata_write_policy,commit_recovery_certificate",
        ),
        CdcManifestTransactionGateEntry::unsupported(
            CdcManifestTransactionSurface::TransactionExecution,
            "transaction_state_machine,conflict_detection,commit_recovery_certificate,no_fallback_policy",
        ),
        CdcManifestTransactionGateEntry::report_only(
            CdcManifestTransactionSurface::UnsupportedCommitDiagnostic,
            "gar0004a.cdc_manifest_transaction_gate",
            "stable_diagnostic_code,claim_gate_status,no_fallback_policy",
        ),
    ]
}

fn cdc_manifest_transaction_existing_report_refs() -> Vec<&'static str> {
    vec![
        "cg9.table_intelligence.foundation",
        "shardloom.cdc_incremental_planning.v1",
        "shardloom.dataset_manifest.v1",
        "shardloom.object_store_commit_protocol.v1",
        "cg10.object_store_request_planner.aggregate",
        "cg4.commit_execution_promotion_gate",
        "vortex-staged-manifest-file-plan",
    ]
}

#[must_use]
pub fn plan_cdc_manifest_transaction_gate() -> CdcManifestTransactionGateReport {
    CdcManifestTransactionGateReport::planning_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_only_foundation_aggregates_table_surfaces() {
        let report = TableIntelligenceReport::report_only_foundation();
        assert_eq!(report.schema_version, "shardloom.table_intelligence.v1");
        assert_eq!(report.surfaces.len(), 10);
        assert_eq!(report.required_cg9_surface_count(), 10);
        assert_eq!(report.report_only_available_surface_count(), 7);
        assert!(report.surface_order().contains(&"schema_evolution"));
        assert!(report.surface_order().contains(&"cdc_incremental"));
        assert!(report.surface_order().contains(&"catalog_compatibility"));
    }

    #[test]
    fn report_only_foundation_is_side_effect_free_and_no_fallback() {
        let report = TableIntelligenceReport::report_only_foundation();
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.catalog_io_performed);
        assert!(!report.table_metadata_io_performed);
        assert!(!report.data_io_performed);
        assert!(!report.write_io_performed);
        assert!(!report.external_table_format_dependency_added);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn unsafe_io_or_fallback_marks_report_error() {
        let mut report = TableIntelligenceReport::report_only_foundation();
        report.table_metadata_io_performed = true;
        assert!(!report.side_effect_free());
        assert!(report.has_errors());

        let mut fallback = TableIntelligenceReport::report_only_foundation();
        fallback.fallback_attempted = true;
        assert!(fallback.has_errors());
    }

    #[test]
    fn catalog_metadata_gate_keeps_runtime_surfaces_blocked() {
        let report = plan_catalog_metadata_integration_gate();
        assert_eq!(
            report.schema_version,
            "shardloom.catalog_metadata_integration_gate.v1"
        );
        assert_eq!(report.report_id, "cg9.catalog_metadata_integration_gate");
        assert_eq!(report.gar_id, "GAR-0020-A");
        assert_eq!(report.gate_status, "report_only");
        assert_eq!(report.support_status, "unsupported");
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert_eq!(report.surface_count(), 11);
        assert_eq!(report.existing_evidence_surface_count(), 2);
        assert_eq!(report.blocked_surface_count(), 9);
        assert_eq!(report.unsupported_surface_count(), 9);
        assert_eq!(
            report.surface_order(),
            vec![
                "table_intelligence_foundation",
                "catalog_ref_skeleton",
                "snapshot_manifest_boundary",
                "catalog_table_resolution",
                "table_metadata_read",
                "partition_metadata_read",
                "delete_tombstone_metadata_read",
                "cdc_metadata_read",
                "table_format_dependency_admission",
                "commit_recovery_metadata_binding",
                "metadata_cache_invalidation",
            ]
        );
        assert!(report.existing_table_intelligence_foundation_present);
        assert!(report.existing_schema_partition_delete_compatibility_present);
        assert!(report.existing_cdc_layout_compaction_planning_present);
        assert!(report.existing_catalog_ref_skeleton_present);
        assert!(report.runtime_promotions_blocked());
        assert!(report.claim_blocked());
        assert!(report.side_effect_free());
        assert!(report.deterministic_unsupported_diagnostics_ready());
        assert_eq!(
            report.unsupported_diagnostic_count(),
            report.unsupported_surface_count()
        );
        assert!(!report.has_errors());
    }

    #[test]
    fn catalog_metadata_gate_requires_evidence_before_catalog_runtime() {
        let report = plan_catalog_metadata_integration_gate();
        assert!(report.table_intelligence_report_required);
        assert!(report.catalog_ref_required);
        assert!(report.snapshot_ref_required);
        assert!(report.schema_digest_required);
        assert!(report.partition_spec_required);
        assert!(report.delete_tombstone_policy_required);
        assert!(report.dependency_license_approval_required);
        assert!(report.credential_policy_required);
        assert!(report.effect_policy_required);
        assert!(report.materialization_boundary_required);
        assert!(report.execution_certificate_required);
        assert!(report.native_io_certificate_required);
        assert!(report.benchmark_evidence_required);
        assert!(!report.catalog_resolution_allowed);
        assert!(!report.table_metadata_read_allowed);
        assert!(!report.catalog_io_allowed);
        assert!(!report.object_store_io_allowed);
        assert!(!report.external_table_format_dependency_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.external_engine_invoked);
        assert!(report.claim_boundary.contains("table metadata reads"));
        assert!(report.local_manifest_table_metadata_smoke_supported);
        assert_eq!(
            report.local_manifest_table_metadata_smoke_command,
            "local-table-metadata-read-smoke"
        );
        assert_eq!(
            report.local_manifest_table_metadata_smoke_report_ref,
            "gar0020c.local_manifest_table_metadata_read_smoke"
        );
        assert_eq!(
            report.local_manifest_table_metadata_smoke_claim_gate_status,
            "scoped_local_metadata_smoke_only"
        );
    }

    #[test]
    fn catalog_metadata_gate_emits_deterministic_unsupported_diagnostics() {
        let report = plan_catalog_metadata_integration_gate();
        assert_eq!(
            report.unsupported_diagnostic_code_order(),
            vec![
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_EXTERNAL_EFFECT_DISABLED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
            ]
        );
        assert_eq!(
            report.unsupported_diagnostic_category_order(),
            vec![
                "planning",
                "planning",
                "planning",
                "planning",
                "planning",
                "planning",
                "external_effect",
                "execution",
                "planning",
            ]
        );
        assert_eq!(
            report.unsupported_diagnostic_severity_order(),
            vec![
                "info", "info", "info", "info", "info", "info", "info", "info", "info",
            ]
        );
        let table_metadata = report
            .entries
            .iter()
            .find(|entry| entry.surface == CatalogMetadataIntegrationSurface::TableMetadataRead)
            .expect("table metadata row");
        assert_eq!(table_metadata.status.as_str(), "blocked_until_certified");
        assert!(
            table_metadata
                .required_evidence
                .contains("table_metadata_schema")
        );
        assert!(table_metadata.requires_catalog_ref);
        assert!(table_metadata.requires_table_metadata_io);
        assert!(table_metadata.requires_catalog_io);
        assert!(table_metadata.requires_object_store_io);
        assert!(!table_metadata.runtime_allowed);
        assert!(!table_metadata.fallback_attempted);
        assert!(!table_metadata.external_engine_invoked);
        assert_eq!(table_metadata.claim_gate_status, "not_claim_grade");
    }

    #[test]
    fn table_maintenance_execution_matrix_blocks_runtime_paths() {
        let report = plan_table_maintenance_execution_matrix();
        assert_eq!(
            report.schema_version,
            "shardloom.table_maintenance_execution_matrix.v1"
        );
        assert_eq!(
            report.report_id,
            "gar0020b.table_maintenance_execution_matrix"
        );
        assert_eq!(report.gar_id, "GAR-0020-B");
        assert_eq!(
            report.support_status,
            "report_only_with_unsupported_runtime_paths"
        );
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert_eq!(report.operation_count(), 12);
        assert_eq!(report.report_only_operation_count(), 6);
        assert_eq!(report.unsupported_operation_count(), 6);
        assert_eq!(
            report.operation_order(),
            vec![
                "file_level_delete_compatibility",
                "segment_tombstone_execution",
                "row_level_delete_execution",
                "position_delete_execution",
                "equality_delete_execution",
                "cdc_append_only_planning",
                "cdc_metadata_only_planning",
                "cdc_update_delete_tombstone_execution",
                "compaction_planning",
                "compaction_execution_write",
                "table_metadata_write",
                "table_maintenance_commit",
            ]
        );
        assert!(report.delete_tombstone_compatibility_report_present);
        assert!(report.cdc_incremental_planning_present);
        assert!(report.layout_compaction_planning_present);
        assert!(report.local_metadata_smoke_present);
        assert!(report.local_delete_tombstone_smoke_present);
        assert!(report.local_append_only_cdc_overlay_smoke_present);
        assert!(report.local_table_append_commit_rehearsal_smoke_present);
        assert!(report.fixture_metadata_required);
        assert!(report.row_identity_required);
        assert!(report.delete_tombstone_policy_required);
        assert!(report.commit_semantics_required);
        assert!(report.table_metadata_schema_required);
        assert!(report.execution_certificate_required);
        assert!(report.native_io_certificate_required);
        assert!(report.materialization_decode_evidence_required);
        assert!(report.no_fallback_policy_required);
        assert!(report.runtime_promotions_blocked());
        assert!(report.claim_blocked());
        assert!(report.side_effect_free());
        assert!(report.deterministic_unsupported_diagnostics_ready());
        assert_eq!(
            report.unsupported_diagnostic_count(),
            report.unsupported_operation_count()
        );
        assert!(!report.has_errors());
    }

    #[test]
    fn table_maintenance_execution_matrix_rows_are_deterministic_and_no_fallback() {
        let report = plan_table_maintenance_execution_matrix();
        let file_level_delete = report
            .rows
            .iter()
            .find(|row| {
                row.operation == TableMaintenanceExecutionOperation::FileLevelDeleteCompatibility
            })
            .expect("file-level delete row");
        assert_eq!(
            file_level_delete.status,
            TableMaintenanceExecutionStatus::ReportOnlyAvailable
        );
        assert_eq!(file_level_delete.support_status, "report_only");
        assert_eq!(
            file_level_delete.existing_report_ref,
            Some("shardloom.delete_tombstone_compatibility.v1")
        );
        assert!(!file_level_delete.runtime_execution_allowed);
        assert!(file_level_delete.side_effect_free());

        let cdc_delete = report
            .rows
            .iter()
            .find(|row| {
                row.operation
                    == TableMaintenanceExecutionOperation::CdcUpdateDeleteTombstoneExecution
            })
            .expect("cdc delete/tombstone row");
        assert_eq!(
            cdc_delete.status,
            TableMaintenanceExecutionStatus::UnsupportedUntilCertified
        );
        assert_eq!(cdc_delete.support_status, "unsupported");
        assert_eq!(
            cdc_delete.required_commit_semantics,
            "cdc_transaction_and_delete_semantics"
        );
        assert!(!cdc_delete.cdc_execution_allowed);
        assert!(!cdc_delete.fallback_attempted);
        assert!(!cdc_delete.external_engine_invoked);
        assert_eq!(cdc_delete.claim_gate_status, "not_claim_grade");

        assert_eq!(
            report.unsupported_diagnostic_code_order(),
            vec![
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
            ]
        );
    }

    #[test]
    fn local_table_metadata_smoke_reads_typed_fixture_metadata_only() {
        let report = run_local_table_metadata_read_smoke().expect("local metadata smoke");
        assert_eq!(
            report.schema_version,
            "shardloom.local_table_metadata_read_smoke.v1"
        );
        assert_eq!(
            report.report_id,
            "gar0020c.local_manifest_table_metadata_read_smoke"
        );
        assert_eq!(report.gar_id, "GAR-0020-C");
        assert_eq!(report.support_status, "runtime_supported");
        assert_eq!(report.claim_gate_status, "scoped_local_metadata_smoke_only");
        assert_eq!(report.catalog_kind, "local_manifest");
        assert_eq!(report.dataset_format, "vortex");
        assert_eq!(report.schema_field_count, 4);
        assert!(report.schema_has_field_ids);
        assert_eq!(report.partition_field_count, 1);
        assert!(report.is_partitioned);
        assert_eq!(report.manifest_file_count, 1);
        assert_eq!(report.manifest_segment_count, 1);
        assert_eq!(report.native_vortex_file_count, 1);
        assert_eq!(report.metadata_capable_segment_count, 1);
        assert_eq!(report.declared_row_count, 8);
        assert!(report.metadata_summary.contains("declared_rows=8"));
        assert!(report.metadata_summary_digest.starts_with("fnv1a64:"));
        assert!(report.runtime_supported());
        assert!(report.claim_scoped());
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn local_table_metadata_smoke_blocks_broad_runtime_and_claims() {
        let report = run_local_table_metadata_read_smoke().expect("local metadata smoke");
        assert!(report.local_catalog_ref_resolved);
        assert!(report.local_manifest_metadata_read_performed);
        assert!(report.table_metadata_summary_emitted);
        assert!(report.table_metadata_read_performed);
        assert!(!report.catalog_io_performed);
        assert!(!report.table_metadata_file_io_performed);
        assert!(!report.object_store_io_performed);
        assert!(!report.data_file_read_performed);
        assert!(!report.write_io_performed);
        assert!(!report.credential_resolution_performed);
        assert!(!report.external_table_format_dependency_invoked);
        assert!(!report.fallback_attempted);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.external_engine_invoked);
        assert!(!report.performance_claim_allowed);
        assert!(!report.production_table_catalog_claim_allowed);
        assert!(!report.lakehouse_claim_allowed);
        assert_eq!(
            report.blocked_path_order(),
            vec![
                "external_catalog_resolution",
                "object_store_manifest_read",
                "credential_resolution",
                "data_file_read",
                "table_metadata_write",
                "cdc_delete_tombstone_execution",
                "external_table_format_runtime",
                "lakehouse_production_claim",
            ]
        );
        assert_eq!(report.unsupported_diagnostic_count(), 8);
        assert!(report.deterministic_unsupported_diagnostics_ready());
        assert_eq!(
            report.unsupported_diagnostic_code_order(),
            vec![
                "SL_NOT_IMPLEMENTED",
                "SL_OBJECT_STORE_UNSUPPORTED",
                "SL_EXTERNAL_EFFECT_DISABLED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_EXTERNAL_EFFECT_DISABLED",
                "SL_NOT_IMPLEMENTED",
            ]
        );
    }

    #[test]
    fn local_delete_tombstone_smoke_applies_fixture_admission_rule() {
        let report = run_local_delete_tombstone_read_smoke().expect("local delete/tombstone smoke");
        assert_eq!(
            report.schema_version,
            "shardloom.local_delete_tombstone_read_smoke.v1"
        );
        assert_eq!(
            report.report_id,
            "gar0020d.local_delete_tombstone_read_smoke"
        );
        assert_eq!(report.gar_id, "GAR-0020-D");
        assert_eq!(report.support_status, "fixture_smoke_only");
        assert_eq!(
            report.claim_gate_status,
            "scoped_local_delete_tombstone_smoke_only"
        );
        assert_eq!(report.fixture_id, "gar0020d-local-delete-tombstone");
        assert_eq!(report.catalog_kind, "local_manifest");
        assert_eq!(report.dataset_format, "vortex");
        assert_eq!(
            report.admitted_delete_model_order,
            vec!["file_level_delete", "segment_level_tombstone"]
        );
        assert_eq!(
            report.delete_tombstone_admission_rule,
            "local_manifest_file_delete_and_segment_tombstone_admission"
        );
        assert_eq!(report.row_identity_rule, "stable_fixture_row_id");
        assert_eq!(report.base_row_count, 6);
        assert_eq!(report.file_deleted_row_count, 2);
        assert_eq!(report.segment_tombstoned_row_count, 1);
        assert_eq!(report.effective_row_count, 3);
        assert_eq!(report.manifest_file_count, 3);
        assert_eq!(report.manifest_segment_count, 3);
        assert_eq!(report.native_vortex_file_count, 3);
        assert_eq!(report.admitted_file_delete_count, 1);
        assert_eq!(report.admitted_segment_tombstone_count, 1);
        assert_eq!(report.effective_row_ids, vec![1001, 1002, 1003]);
        assert!(
            report
                .correctness_summary
                .contains("effective_row_ids=1001,1002,1003")
        );
        assert!(report.correctness_digest.starts_with("fnv1a64:"));
        assert!(report.fixture_smoke_supported());
        assert!(report.claim_scoped());
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn local_delete_tombstone_smoke_blocks_unsupported_models_and_claims() {
        let report = run_local_delete_tombstone_read_smoke().expect("local delete/tombstone smoke");
        assert!(report.local_catalog_ref_resolved);
        assert!(report.local_manifest_metadata_read_performed);
        assert!(report.in_memory_fixture_rows_read);
        assert!(report.delete_tombstone_rule_applied);
        assert!(report.result_row_order_preserved);
        assert!(!report.table_metadata_write_performed);
        assert!(!report.data_file_read_performed);
        assert!(!report.object_store_io_performed);
        assert!(!report.write_io_performed);
        assert!(!report.credential_resolution_performed);
        assert!(!report.external_table_format_dependency_invoked);
        assert!(!report.fallback_attempted);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.external_engine_invoked);
        assert!(!report.performance_claim_allowed);
        assert!(!report.table_format_execution_claim_allowed);
        assert!(!report.production_table_catalog_claim_allowed);
        assert!(!report.lakehouse_claim_allowed);
        assert_eq!(
            report.blocked_model_order(),
            vec![
                "row_level_delete",
                "position_delete",
                "equality_delete",
                "external_table_metadata",
                "cdc_update_delete_tombstone",
                "object_store_delete_manifest",
                "table_format_delete_runtime",
            ]
        );
        assert_eq!(report.unsupported_diagnostic_count(), 7);
        assert!(report.deterministic_unsupported_diagnostics_ready());
        assert_eq!(
            report.unsupported_diagnostic_code_order(),
            vec![
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_EXTERNAL_EFFECT_DISABLED",
                "SL_NOT_IMPLEMENTED",
                "SL_OBJECT_STORE_UNSUPPORTED",
                "SL_EXTERNAL_EFFECT_DISABLED",
            ]
        );
    }

    #[test]
    fn local_append_only_cdc_overlay_smoke_combines_base_and_append_rows() {
        let report =
            run_local_append_only_cdc_overlay_smoke().expect("local append-only CDC overlay smoke");
        assert_eq!(
            report.schema_version,
            "shardloom.local_append_only_cdc_overlay_smoke.v1"
        );
        assert_eq!(
            report.report_id,
            "gar0020e.local_append_only_cdc_overlay_smoke"
        );
        assert_eq!(report.gar_id, "GAR-0020-E");
        assert_eq!(report.support_status, "fixture_smoke_only");
        assert_eq!(
            report.claim_gate_status,
            "scoped_append_only_cdc_overlay_smoke_only"
        );
        assert_eq!(report.fixture_id, "gar0020e-local-append-only-cdc-overlay");
        assert_eq!(report.catalog_kind, "local_manifest");
        assert_eq!(report.dataset_format, "vortex");
        assert_eq!(report.incremental_status, "execute_changed_segments_only");
        assert_eq!(report.overlay_rule, "base_snapshot_then_append_delta");
        assert_eq!(report.cdc_event_order, vec!["insert"]);
        assert_eq!(report.base_row_count, 3);
        assert_eq!(report.append_row_count, 2);
        assert_eq!(report.effective_row_count, 5);
        assert_eq!(report.base_manifest_file_count, 1);
        assert_eq!(report.delta_manifest_file_count, 1);
        assert_eq!(report.base_manifest_segment_count, 1);
        assert_eq!(report.delta_manifest_segment_count, 1);
        assert_eq!(report.changed_segment_count, 1);
        assert_eq!(report.insert_count, 2);
        assert_eq!(report.update_count, 0);
        assert_eq!(report.delete_count, 0);
        assert_eq!(report.tombstone_count, 0);
        assert_eq!(report.unsupported_change_count, 0);
        assert_eq!(report.base_row_ids, vec![1001, 1002, 1003]);
        assert_eq!(report.appended_row_ids, vec![4001, 4002]);
        assert_eq!(report.effective_row_ids, vec![1001, 1002, 1003, 4001, 4002]);
        assert!(
            report
                .correctness_summary
                .contains("effective_row_ids=1001,1002,1003,4001,4002")
        );
        assert!(report.correctness_digest.starts_with("fnv1a64:"));
        assert!(report.fixture_smoke_supported());
        assert!(report.claim_scoped());
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn local_append_only_cdc_overlay_smoke_blocks_runtime_claims_and_writes() {
        let report =
            run_local_append_only_cdc_overlay_smoke().expect("local append-only CDC overlay smoke");
        assert!(report.local_catalog_ref_resolved);
        assert!(report.local_base_snapshot_declared);
        assert!(report.local_append_delta_declared);
        assert!(report.cdc_incremental_plan_evaluated);
        assert!(report.append_overlay_rule_applied);
        assert!(report.result_row_order_preserved);
        assert!(!report.table_metadata_write_performed);
        assert!(!report.manifest_write_performed);
        assert!(!report.transaction_execution_performed);
        assert!(!report.commit_execution_performed);
        assert!(!report.data_file_read_performed);
        assert!(!report.object_store_io_performed);
        assert!(!report.write_io_performed);
        assert!(!report.credential_resolution_performed);
        assert!(!report.external_table_format_dependency_invoked);
        assert!(!report.fallback_attempted);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.external_engine_invoked);
        assert!(!report.performance_claim_allowed);
        assert!(!report.production_incremental_claim_allowed);
        assert!(!report.lakehouse_claim_allowed);
        assert_eq!(
            report.blocked_path_order(),
            vec![
                "cdc_update",
                "cdc_delete",
                "cdc_tombstone",
                "manifest_serialization",
                "manifest_write",
                "transaction_execution",
                "object_store_commit",
                "table_catalog_commit",
                "table_format_cdc_runtime",
            ]
        );
        assert_eq!(report.unsupported_diagnostic_count(), 9);
        assert!(report.deterministic_unsupported_diagnostics_ready());
        assert_eq!(
            report.unsupported_diagnostic_code_order(),
            vec![
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_NOT_IMPLEMENTED",
                "SL_COMMIT_NOT_ATOMIC",
                "SL_OBJECT_STORE_UNSUPPORTED",
                "SL_COMMIT_NOT_ATOMIC",
                "SL_EXTERNAL_EFFECT_DISABLED",
            ]
        );
    }

    #[test]
    fn cdc_manifest_transaction_gate_reports_current_boundaries() {
        let report = plan_cdc_manifest_transaction_gate();
        assert_eq!(
            report.schema_version,
            "shardloom.cdc_manifest_transaction_gate.v1"
        );
        assert_eq!(report.report_id, "gar0004a.cdc_manifest_transaction_gate");
        assert_eq!(report.surface_count(), 8);
        assert_eq!(report.report_only_surface_count(), 2);
        assert_eq!(report.unsupported_surface_count(), 6);
        assert_eq!(
            report.surface_order(),
            vec![
                "cdc_read_intent",
                "cdc_write_intent",
                "manifest_serialization",
                "manifest_metadata_read",
                "object_store_commit",
                "table_catalog_commit",
                "transaction_execution",
                "unsupported_commit_diagnostic",
            ]
        );
        assert!(report.existing_cdc_planning_present);
        assert!(report.existing_manifest_contract_present);
        assert!(report.existing_object_store_commit_protocol_present);
        assert!(report.existing_local_staged_manifest_helpers_present);
        assert!(report.cdc_read_intent_report_only_available);
    }

    #[test]
    fn cdc_manifest_transaction_gate_blocks_runtime_io_commits_and_claims() {
        let report = plan_cdc_manifest_transaction_gate();
        assert!(report.runtime_promotions_blocked());
        assert!(report.claim_blocked());
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
        assert!(report.deterministic_unsupported_diagnostics_ready());
        assert_eq!(
            report.unsupported_diagnostic_count(),
            report.unsupported_surface_count()
        );
        assert!(!report.cdc_write_intent_allowed);
        assert!(!report.manifest_serialization_allowed);
        assert!(!report.manifest_metadata_read_allowed);
        assert!(!report.transaction_execution_allowed);
        assert!(!report.commit_execution_allowed);
        assert!(!report.object_store_io_allowed);
        assert!(!report.data_read_allowed);
        assert!(!report.write_io_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.external_engine_invoked);
        assert!(!report.cdc_transaction_claim_allowed);
        assert_eq!(report.claim_gate_status, "not_claim_grade");
    }
}
