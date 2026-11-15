//! Completed text-expression work at the existing native dictionary boundary.

use shardloom_core::{NativeIoCertificate, Result, ShardLoomError};

#[derive(Default)]
pub(crate) struct Work {
    pub(crate) batches: u64,
    pub(crate) logical_rows: u64,
    pub(crate) dictionary_batches: u64,
    pub(crate) domain_slots: u64,
    pub(crate) referenced_values: u64,
    pub(crate) dictionary_kernel_calls: u64,
    pub(crate) code_rows_inspected: u64,
    pub(crate) code_rows_copied: u64,
    pub(crate) retained_dictionary_rows: u64,
    pub(crate) row_output_rows: u64,
    pub(crate) row_kernel_calls: u64,
    pub(crate) no_dictionary_batches: u64,
    pub(crate) no_reduction_batches: u64,
    pub(crate) capacity_declines: u64,
}

impl Work {
    pub(crate) fn merge(&mut self, other: &Self) -> Result<()> {
        macro_rules! add {
            ($($field:ident),+ $(,)?) => {$(
                self.$field = self.$field.checked_add(other.$field).ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "native dictionary expression observation overflow; no fallback execution was attempted".into(),
                    )
                })?;
            )+};
        }
        add!(
            batches,
            logical_rows,
            dictionary_batches,
            domain_slots,
            referenced_values,
            dictionary_kernel_calls,
            code_rows_inspected,
            code_rows_copied,
            retained_dictionary_rows,
            row_output_rows,
            row_kernel_calls,
            no_dictionary_batches,
            no_reduction_batches,
            capacity_declines
        );
        Ok(())
    }

    pub(crate) fn append(&self, certificate: &mut NativeIoCertificate) {
        use std::fmt::Write as _;
        if self.batches == 0 {
            return;
        }
        let proof = &mut certificate.source_pushdown_report.proof_basis;
        macro_rules! append {
            ($($field:ident),+ $(,)?) => {$(
                write!(proof, " dictionary_text_{}={}", stringify!($field), self.$field)
                    .expect("String writes cannot fail");
            )+};
        }
        append!(
            batches,
            logical_rows,
            dictionary_batches,
            domain_slots,
            referenced_values,
            dictionary_kernel_calls,
            code_rows_inspected,
            code_rows_copied,
            retained_dictionary_rows,
            row_output_rows,
            row_kernel_calls,
            no_dictionary_batches,
            no_reduction_batches,
            capacity_declines
        );
        proof.push_str(
            " dictionary_text_contract=v1;vortex_0.93.0_DictArray;strict_prepared_utf8_kernels;complete_domain_validation;referenced_values_only;independent_code_and_value_ownership;exact_consumer_value_equality;counts_are_owner_invocations_not_unique_query_rows;decoder_bytes_and_process_rss_unobserved;no_external_engine",
        );
        if self.dictionary_batches > 0 {
            certificate
                .source_pushdown_report
                .accepted_operations
                .push("native_dictionary_text_expression".into());
        }
    }
}
