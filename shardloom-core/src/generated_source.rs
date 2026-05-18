//! Source-free generated-output evidence contracts.
//!
//! These contracts distinguish no-dataset smoke from generated-output execution. They define the
//! fields future runtime slices must emit when `ShardLoom` creates output without reading an input
//! dataset, but they do not execute generators, parse SQL, materialize `DataFrames`, write outputs,
//! probe object stores, invoke Foundry, or call external fallback engines.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeneratedSourceCaseKind {
    NoDatasetSmoke,
    UserGeneratedSource,
    EngineNativeGeneratedSource,
}

impl GeneratedSourceCaseKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoDatasetSmoke => "no_dataset_smoke",
            Self::UserGeneratedSource => "user_generated_source",
            Self::EngineNativeGeneratedSource => "engine_native_generated_source",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeneratedSourceSupportStatus {
    SmokeOnly,
    FixtureSmokeSupported,
    ReportOnly,
    PlannedRuntime,
}

impl GeneratedSourceSupportStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SmokeOnly => "smoke_only",
            Self::FixtureSmokeSupported => "fixture_smoke_supported",
            Self::ReportOnly => "report_only",
            Self::PlannedRuntime => "planned_runtime",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeneratedSourceCertificateStatus {
    NotApplicableNoGeneratedRows,
    NotEmittedReportOnly,
    RequiredForRuntime,
}

impl GeneratedSourceCertificateStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotApplicableNoGeneratedRows => "not_applicable_no_generated_rows",
            Self::NotEmittedReportOnly => "not_emitted_report_only",
            Self::RequiredForRuntime => "required_for_runtime",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct GeneratedSourceCertificateContractRow {
    pub case_kind: GeneratedSourceCaseKind,
    pub support_status: GeneratedSourceSupportStatus,
    pub generated_source_certificate_status: GeneratedSourceCertificateStatus,
    pub input_dataset_count: u64,
    pub source_io_performed: bool,
    pub generated_source_created: bool,
    pub output_io_performed: bool,
    pub source_native_io_certificate_status: &'static str,
    pub output_native_io_certificate_status: &'static str,
    pub required_generator_kinds: &'static str,
    pub required_evidence_fields: &'static str,
    pub blocker_id: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl GeneratedSourceCertificateContractRow {
    #[must_use]
    pub const fn no_dataset_smoke() -> Self {
        Self {
            case_kind: GeneratedSourceCaseKind::NoDatasetSmoke,
            support_status: GeneratedSourceSupportStatus::SmokeOnly,
            generated_source_certificate_status:
                GeneratedSourceCertificateStatus::NotApplicableNoGeneratedRows,
            input_dataset_count: 0,
            source_io_performed: false,
            generated_source_created: false,
            output_io_performed: false,
            source_native_io_certificate_status: "not_applicable_no_source_dataset",
            output_native_io_certificate_status: "not_emitted_no_output_data",
            required_generator_kinds: "none",
            required_evidence_fields: "input_dataset_count,source_io_performed,generated_source_created,output_io_performed,generated_source_certificate_status,claim_gate_status",
            blocker_id: "gar-gen-1.no_dataset_smoke_not_generated_output",
            claim_gate_status: "smoke_only",
            claim_boundary: "No-dataset smoke is a status/capability proof only; it creates no generated rows, no source Native I/O certificate, and no output data claim.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn user_generated_source() -> Self {
        Self {
            case_kind: GeneratedSourceCaseKind::UserGeneratedSource,
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            generated_source_certificate_status:
                GeneratedSourceCertificateStatus::RequiredForRuntime,
            input_dataset_count: 0,
            source_io_performed: false,
            generated_source_created: false,
            output_io_performed: false,
            source_native_io_certificate_status: "not_applicable_no_source_dataset",
            output_native_io_certificate_status: "required_for_runtime_output",
            required_generator_kinds: "python_rows,literal_rows",
            required_evidence_fields: "generated_source_kind,generated_source_schema_digest,generated_source_row_count,generated_source_plan_digest,generation_deterministic,output_io_performed,output_native_io_certificate_status",
            blocker_id: "none_scoped_local_jsonl_smoke_only",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "User-generated source support is limited to the scoped local JSONL fixture smoke until broader runtime and sink evidence exists.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn engine_native_generated_source() -> Self {
        Self {
            case_kind: GeneratedSourceCaseKind::EngineNativeGeneratedSource,
            support_status: GeneratedSourceSupportStatus::FixtureSmokeSupported,
            generated_source_certificate_status:
                GeneratedSourceCertificateStatus::RequiredForRuntime,
            input_dataset_count: 0,
            source_io_performed: false,
            generated_source_created: false,
            output_io_performed: false,
            source_native_io_certificate_status: "not_applicable_no_source_dataset",
            output_native_io_certificate_status: "required_for_runtime_output",
            required_generator_kinds: "range(runtime_local_jsonl_smoke),sequence(report_only),values(report_only),literal_table(report_only),calendar(report_only),synthetic(report_only)",
            required_evidence_fields: "generated_source_kind,generated_source_schema_digest,generated_source_row_count,generated_source_plan_digest,generated_source_seed,generation_deterministic,output_io_performed,output_native_io_certificate_status",
            blocker_id: "none_scoped_local_range_jsonl_smoke_only",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "Engine-native generated source support is limited to the scoped local range JSONL fixture smoke; sequence, values, literal_table, calendar, synthetic, SQL, DataFrame, object-store, and Foundry runtime remain blocked/report-only.",
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.fallback_attempted && !self.external_engine_invoked
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct GeneratedSourceCertificateContractReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub generated_source_certificate_schema_version: &'static str,
    pub support_status_vocabulary: &'static str,
    pub required_field_order: Vec<&'static str>,
    pub rows: Vec<GeneratedSourceCertificateContractRow>,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub object_store_io_performed: bool,
    pub foundry_runtime_invoked: bool,
    pub broad_sql_dataframe_claim_allowed: bool,
    pub claim_gate_status: &'static str,
}

impl GeneratedSourceCertificateContractReport {
    #[must_use]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.generated_source_certificate_contract.v1",
            report_id: "gar-gen-1.generated_source_certificate_contract",
            generated_source_certificate_schema_version: "shardloom.generated_source_certificate.v1",
            support_status_vocabulary: "smoke_only,fixture_smoke_supported,report_only,planned_runtime",
            required_field_order: vec![
                "input_dataset_count",
                "source_io_performed",
                "generated_source_created",
                "generated_source_kind",
                "generated_source_schema_digest",
                "generated_source_row_count",
                "generated_source_plan_digest",
                "generated_source_seed",
                "generation_deterministic",
                "output_io_performed",
                "output_native_io_certificate_status",
                "generated_source_certificate_status",
                "fallback_attempted",
                "external_engine_invoked",
                "claim_gate_status",
            ],
            rows: vec![
                GeneratedSourceCertificateContractRow::no_dataset_smoke(),
                GeneratedSourceCertificateContractRow::user_generated_source(),
                GeneratedSourceCertificateContractRow::engine_native_generated_source(),
            ],
            fallback_attempted: false,
            external_engine_invoked: false,
            object_store_io_performed: false,
            foundry_runtime_invoked: false,
            broad_sql_dataframe_claim_allowed: false,
            claim_gate_status: "not_claim_grade",
        }
    }

    #[must_use]
    pub fn case_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.case_kind.as_str()).collect()
    }

    #[must_use]
    pub fn row_for(
        &self,
        case_kind: GeneratedSourceCaseKind,
    ) -> Option<&GeneratedSourceCertificateContractRow> {
        self.rows.iter().find(|row| row.case_kind == case_kind)
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        !self.fallback_attempted
            && !self.external_engine_invoked
            && self
                .rows
                .iter()
                .all(GeneratedSourceCertificateContractRow::fallback_free)
    }
}

#[cfg(test)]
mod tests {
    use super::{GeneratedSourceCaseKind, GeneratedSourceCertificateContractReport};

    #[test]
    fn report_only_contract_separates_no_dataset_smoke_from_generated_output() {
        let report = GeneratedSourceCertificateContractReport::report_only();
        assert_eq!(
            report.case_order(),
            vec![
                "no_dataset_smoke",
                "user_generated_source",
                "engine_native_generated_source"
            ]
        );
        assert!(report.all_rows_fallback_free());
        assert!(!report.broad_sql_dataframe_claim_allowed);
        assert!(!report.object_store_io_performed);
        assert!(!report.foundry_runtime_invoked);

        let smoke = report
            .row_for(GeneratedSourceCaseKind::NoDatasetSmoke)
            .expect("no-dataset smoke row");
        assert!(!smoke.generated_source_created);
        assert!(!smoke.output_io_performed);
        assert_eq!(
            smoke.generated_source_certificate_status.as_str(),
            "not_applicable_no_generated_rows"
        );

        let user_rows = report
            .row_for(GeneratedSourceCaseKind::UserGeneratedSource)
            .expect("user generated source row");
        assert_eq!(
            user_rows.generated_source_certificate_status.as_str(),
            "required_for_runtime"
        );
        assert_eq!(user_rows.support_status.as_str(), "fixture_smoke_supported");
        assert_eq!(user_rows.claim_gate_status, "fixture_smoke_only");

        let engine_range = report
            .row_for(GeneratedSourceCaseKind::EngineNativeGeneratedSource)
            .expect("engine-native generated source row");
        assert_eq!(
            engine_range.generated_source_certificate_status.as_str(),
            "required_for_runtime"
        );
        assert_eq!(
            engine_range.support_status.as_str(),
            "fixture_smoke_supported"
        );
        assert_eq!(engine_range.claim_gate_status, "fixture_smoke_only");
        assert!(engine_range.required_generator_kinds.contains("range("));
        assert!(engine_range.claim_boundary.contains("range JSONL"));
    }
}
