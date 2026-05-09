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

use crate::{Diagnostic, DiagnosticSeverity};
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
}
