//! Extension dtype capability matrix.
//!
//! This keeps rich Vortex/ShardLoom data categories visible without implying
//! vector search, geospatial, raster, media, or model execution support.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionTypeSupportStatus {
    Declared,
    MetadataPreserved,
    ReportOnly,
    Deferred,
    UnsupportedBlocked,
}

impl ExtensionTypeSupportStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Declared => "declared",
            Self::MetadataPreserved => "metadata_preserved",
            Self::ReportOnly => "report_only",
            Self::Deferred => "deferred",
            Self::UnsupportedBlocked => "unsupported_blocked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExtensionTypeCapabilityRow {
    pub type_family: &'static str,
    pub dtype_recognition: ExtensionTypeSupportStatus,
    pub metadata_preservation: ExtensionTypeSupportStatus,
    pub scan_support: ExtensionTypeSupportStatus,
    pub expression_support: ExtensionTypeSupportStatus,
    pub write_support: ExtensionTypeSupportStatus,
    pub certified_execution: ExtensionTypeSupportStatus,
    pub no_external_engine_fallback: bool,
    pub fallback_attempted: bool,
}

impl ExtensionTypeCapabilityRow {
    fn declared(type_family: &'static str) -> Self {
        Self {
            type_family,
            dtype_recognition: ExtensionTypeSupportStatus::Declared,
            metadata_preservation: ExtensionTypeSupportStatus::ReportOnly,
            scan_support: ExtensionTypeSupportStatus::Deferred,
            expression_support: ExtensionTypeSupportStatus::Deferred,
            write_support: ExtensionTypeSupportStatus::Deferred,
            certified_execution: ExtensionTypeSupportStatus::UnsupportedBlocked,
            no_external_engine_fallback: true,
            fallback_attempted: false,
        }
    }

    fn reference(type_family: &'static str) -> Self {
        Self {
            type_family,
            dtype_recognition: ExtensionTypeSupportStatus::Declared,
            metadata_preservation: ExtensionTypeSupportStatus::MetadataPreserved,
            scan_support: ExtensionTypeSupportStatus::ReportOnly,
            expression_support: ExtensionTypeSupportStatus::Deferred,
            write_support: ExtensionTypeSupportStatus::Deferred,
            certified_execution: ExtensionTypeSupportStatus::UnsupportedBlocked,
            no_external_engine_fallback: true,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn execution_claim_allowed(&self) -> bool {
        matches!(
            self.certified_execution,
            ExtensionTypeSupportStatus::MetadataPreserved
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExtensionTypeCapabilityMatrix {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub rows: Vec<ExtensionTypeCapabilityRow>,
    pub vector_similarity_scan_separate_from_ann_topk: bool,
    pub media_model_work_is_effect_boundary: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl ExtensionTypeCapabilityMatrix {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.extension_type_capability_matrix.v1",
            report_id: "cg20.cg21.extension_type_capability_matrix",
            rows: vec![
                ExtensionTypeCapabilityRow::declared("vector"),
                ExtensionTypeCapabilityRow::declared("tensor_matrix"),
                ExtensionTypeCapabilityRow::declared("fixed_size_binary"),
                ExtensionTypeCapabilityRow::declared("map"),
                ExtensionTypeCapabilityRow::declared("variant_json"),
                ExtensionTypeCapabilityRow::declared("uuid"),
                ExtensionTypeCapabilityRow::declared("geospatial_wkb_geoarrow"),
                ExtensionTypeCapabilityRow::reference("raster_image_reference"),
                ExtensionTypeCapabilityRow::reference("embedding_reference"),
                ExtensionTypeCapabilityRow::reference("document_media_reference"),
            ],
            vector_similarity_scan_separate_from_ann_topk: true,
            media_model_work_is_effect_boundary: true,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn row(&self, type_family: &str) -> Option<&ExtensionTypeCapabilityRow> {
        self.rows.iter().find(|row| row.type_family == type_family)
    }

    #[must_use]
    pub fn type_family_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.type_family).collect()
    }

    #[must_use]
    pub fn certified_execution_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.execution_claim_allowed())
            .count()
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        !self.external_engine_invoked
            && !self.fallback_attempted
            && self
                .rows
                .iter()
                .all(|row| row.no_external_engine_fallback && !row.fallback_attempted)
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "extension type capability matrix\nschema_version: {}\nreport: {}\ntype families: {}\ncertified execution: {}\nmedia/model work: effect boundary\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.rows.len(),
            self.certified_execution_count(),
        )
    }
}

#[must_use]
pub fn plan_extension_type_capability_matrix() -> ExtensionTypeCapabilityMatrix {
    ExtensionTypeCapabilityMatrix::current()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_matrix_lists_rich_type_families() {
        let matrix = plan_extension_type_capability_matrix();

        assert_eq!(
            matrix.type_family_order(),
            vec![
                "vector",
                "tensor_matrix",
                "fixed_size_binary",
                "map",
                "variant_json",
                "uuid",
                "geospatial_wkb_geoarrow",
                "raster_image_reference",
                "embedding_reference",
                "document_media_reference"
            ]
        );
    }

    #[test]
    fn extension_matrix_distinguishes_metadata_from_execution() {
        let matrix = plan_extension_type_capability_matrix();
        let vector = matrix.row("vector").expect("vector row");
        let media = matrix
            .row("document_media_reference")
            .expect("media reference row");

        assert_eq!(
            vector.dtype_recognition,
            ExtensionTypeSupportStatus::Declared
        );
        assert_eq!(
            vector.certified_execution,
            ExtensionTypeSupportStatus::UnsupportedBlocked
        );
        assert_eq!(
            media.metadata_preservation,
            ExtensionTypeSupportStatus::MetadataPreserved
        );
        assert_eq!(matrix.certified_execution_count(), 0);
    }

    #[test]
    fn extension_matrix_keeps_vector_ann_and_media_effects_bounded() {
        let matrix = plan_extension_type_capability_matrix();

        assert!(matrix.vector_similarity_scan_separate_from_ann_topk);
        assert!(matrix.media_model_work_is_effect_boundary);
        assert!(matrix.all_rows_fallback_free());
        assert!(
            matrix
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
}
