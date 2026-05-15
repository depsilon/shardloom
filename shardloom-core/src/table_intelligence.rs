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

use crate::{Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, FallbackStatus};
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogMetadataIntegrationGateEntry {
    pub surface: CatalogMetadataIntegrationSurface,
    pub status: CatalogMetadataIntegrationStatus,
    pub existing_report_ref: Option<&'static str>,
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
    pub fallback_execution_allowed: bool,
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
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn blocked(
        surface: CatalogMetadataIntegrationSurface,
        requirements: CatalogMetadataIntegrationRequirements,
    ) -> Self {
        Self {
            surface,
            status: CatalogMetadataIntegrationStatus::BlockedUntilCertified,
            existing_report_ref: None,
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
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.runtime_allowed && !self.fallback_execution_allowed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogMetadataIntegrationGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub entries: Vec<CatalogMetadataIntegrationGateEntry>,
    pub existing_report_refs: Vec<&'static str>,
    pub compatibility_profiles: Vec<&'static str>,
    pub existing_table_intelligence_foundation_present: bool,
    pub existing_schema_partition_delete_compatibility_present: bool,
    pub existing_cdc_layout_compaction_planning_present: bool,
    pub existing_catalog_ref_skeleton_present: bool,
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
    pub diagnostics: Vec<Diagnostic>,
}

impl CatalogMetadataIntegrationGateReport {
    #[must_use]
    pub fn planning_default() -> Self {
        Self {
            schema_version: "shardloom.catalog_metadata_integration_gate.v1",
            report_id: "cg9.catalog_metadata_integration_gate",
            entries: catalog_metadata_integration_entries(),
            existing_report_refs: catalog_metadata_existing_report_refs(),
            compatibility_profiles: catalog_metadata_compatibility_profiles(),
            existing_table_intelligence_foundation_present: true,
            existing_schema_partition_delete_compatibility_present: true,
            existing_cdc_layout_compaction_planning_present: true,
            existing_catalog_ref_skeleton_present: true,
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
            diagnostics: Vec::new(),
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
            .filter(|entry| {
                matches!(
                    entry.status,
                    CatalogMetadataIntegrationStatus::BlockedUntilCertified
                )
            })
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
                .all(|entry| !entry.runtime_allowed && !entry.fallback_execution_allowed)
    }

    #[must_use]
    pub fn claim_blocked(&self) -> bool {
        !self.metadata_integration_claim_allowed
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        self.runtime_promotions_blocked()
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && self
                .entries
                .iter()
                .all(CatalogMetadataIntegrationGateEntry::side_effect_free)
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || !self.claim_blocked()
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
        let _ = writeln!(out, "side effect free: {}", self.side_effect_free());
        let _ = writeln!(out, "fallback attempted: {}", self.fallback_attempted);
        let _ = writeln!(
            out,
            "fallback execution allowed: {}",
            self.fallback_execution_allowed
        );
        let _ = writeln!(out, "surfaces:");
        for entry in &self.entries {
            let _ = writeln!(
                out,
                "  - {} [{}] existing_ref={} runtime_allowed={} requires_catalog_ref={} requires_snapshot_ref={} requires_table_metadata_io={} requires_catalog_io={} requires_object_store_io={} requires_dependency_approval={} requires_credential_policy={} requires_execution_certificate={} requires_native_io_certificate={} fallback_execution_allowed={}",
                entry.surface.as_str(),
                entry.status.as_str(),
                entry.existing_report_ref.unwrap_or("none"),
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
                entry.fallback_execution_allowed
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
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::CatalogTableResolution,
            CatalogMetadataIntegrationRequirements::CATALOG_TABLE_RESOLUTION,
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::TableMetadataRead,
            CatalogMetadataIntegrationRequirements::CATALOG_BACKED_METADATA,
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::PartitionMetadataRead,
            CatalogMetadataIntegrationRequirements::CATALOG_BACKED_METADATA,
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::DeleteTombstoneMetadataRead,
            CatalogMetadataIntegrationRequirements::CATALOG_BACKED_METADATA,
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::CdcMetadataRead,
            CatalogMetadataIntegrationRequirements::CATALOG_BACKED_METADATA,
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::TableFormatDependencyAdmission,
            CatalogMetadataIntegrationRequirements::TABLE_FORMAT_DEPENDENCY_ADMISSION,
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::CommitRecoveryMetadataBinding,
            CatalogMetadataIntegrationRequirements::CATALOG_BACKED_METADATA,
        ),
        CatalogMetadataIntegrationGateEntry::blocked(
            CatalogMetadataIntegrationSurface::MetadataCacheInvalidation,
            CatalogMetadataIntegrationRequirements::CATALOG_BACKED_METADATA,
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
        assert_eq!(report.surface_count(), 11);
        assert_eq!(report.existing_evidence_surface_count(), 2);
        assert_eq!(report.blocked_surface_count(), 9);
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
