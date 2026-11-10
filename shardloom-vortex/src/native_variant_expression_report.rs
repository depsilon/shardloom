//! Actual operation-local Variant provider boundaries, without payload tracing.

use shardloom_core::{
    NativeIoCertificate, NativeIoMaterializationBoundaryReport, RepresentationState, Result,
};

#[derive(Default)]
pub(crate) struct ExpressionWork {
    batches: u64,
    rows: u64,
    normalized_rows: u64,
    format_bridge_rows: u64,
}

impl ExpressionWork {
    pub(crate) fn record(
        &mut self,
        rows: u64,
        normalized: bool,
        format_bridge: bool,
    ) -> Result<()> {
        let add = |left: u64, right: u64| {
            left.checked_add(right)
                .ok_or_else(|| super::failed("expression observation overflow"))
        };
        self.batches = add(self.batches, 1)?;
        self.rows = add(self.rows, rows)?;
        if normalized {
            self.normalized_rows = add(self.normalized_rows, rows)?;
        }
        if format_bridge {
            self.format_bridge_rows = add(self.format_bridge_rows, rows)?;
        }
        Ok(())
    }

    pub(crate) fn append(&self, certificate: &mut NativeIoCertificate) {
        use std::fmt::Write as _;
        if self.batches == 0 {
            return;
        }
        write!(certificate.source_pushdown_report.proof_basis,
            " variant_provider=vortex_0.93.0_VariantGet_ParquetVariant variant_provider_batches={} variant_provider_rows={} variant_normalized_rows={} variant_format_bridge_rows={};variant_workspace_reserved_before_provider;variant_output_detached_before_workspace_release;provider_decoder_bytes_unmeasured;no_external_engine",
            self.batches, self.rows, self.normalized_rows, self.format_bridge_rows).expect("String writes cannot fail");
        certificate
            .source_pushdown_report
            .accepted_operations
            .push("native_variant_expression".into());
        certificate.side_effects.data_materialized = true;
        certificate.adapter_fidelity_report.materialization_required = true;
        certificate.materialization_boundaries.push(NativeIoMaterializationBoundaryReport {
            boundary_id: "native_variant_expression_provider".into(),
            from_state: RepresentationState::VortexEncoded,
            to_state: RepresentationState::DecodedColumnar,
            required_by: "explicit_native_variant_expression".into(),
            reason: "Pinned Vortex VariantGet uses its encoded Parquet Variant provider and an internal Arrow format bridge for extraction and typed conversion; canonical shredded inputs are normalized when needed; null inspection uses native scalar access; bounded workspace and returned native buffer owners are separately credited; decoded byte volume is not instrumented and zero below is not a zero-decode claim".into(),
            bytes_decoded: 0,
            rows_materialized: self.rows,
            fidelity_loss: "none_for_selected_logical_values;physical_shredding_may_change;explicit_tolerant_target_casts_have_declared_semantics".into(),
            fallback_attempted: false,
        });
    }
}
