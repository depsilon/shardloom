//! Fixed-size, operation-local work evidence recorded at native owner boundaries.

use shardloom_core::{Result, ShardLoomError};

#[derive(Clone, Copy)]
pub(crate) enum Counter {
    PreparedSelections,
    PreparedCoordinates,
    ScalarCoordinates,
    RebuiltSelections,
    RebuiltCoordinates,
    MissingFields,
    MissingRows,
    JoinBatches,
    JoinRows,
    JoinRetainedCells,
    JoinOmittedCells,
    SortGathers,
    SortSelectedCells,
    SortRetainedCells,
    SortOmittedCells,
    SortBufferBytes,
    TypedValidationColumns,
    TypedValidationRows,
}

impl Counter {
    const ALL: [Self; 18] = [
        Self::PreparedSelections,
        Self::PreparedCoordinates,
        Self::ScalarCoordinates,
        Self::RebuiltSelections,
        Self::RebuiltCoordinates,
        Self::MissingFields,
        Self::MissingRows,
        Self::JoinBatches,
        Self::JoinRows,
        Self::JoinRetainedCells,
        Self::JoinOmittedCells,
        Self::SortGathers,
        Self::SortSelectedCells,
        Self::SortRetainedCells,
        Self::SortOmittedCells,
        Self::SortBufferBytes,
        Self::TypedValidationColumns,
        Self::TypedValidationRows,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::PreparedSelections => "payload_prepared_selections",
            Self::PreparedCoordinates => "payload_prepared_coordinates",
            Self::ScalarCoordinates => "payload_scalar_coordinates",
            Self::RebuiltSelections => "payload_rebuilt_selections",
            Self::RebuiltCoordinates => "payload_rebuilt_coordinates",
            Self::MissingFields => "payload_missing_fields_validated",
            Self::MissingRows => "payload_missing_rows_validated",
            Self::JoinBatches => "join_payload_batches",
            Self::JoinRows => "join_payload_rows",
            Self::JoinRetainedCells => "join_payload_retained_cells",
            Self::JoinOmittedCells => "join_payload_omitted_cells",
            Self::SortGathers => "sort_payload_gathers",
            Self::SortSelectedCells => "sort_payload_selected_cells",
            Self::SortRetainedCells => "sort_payload_retained_cells",
            Self::SortOmittedCells => "sort_payload_omitted_cells",
            Self::SortBufferBytes => "sort_payload_buffer_bytes",
            Self::TypedValidationColumns => "payload_typed_validation_columns",
            Self::TypedValidationRows => "payload_typed_validation_rows",
        }
    }
}

#[derive(Default, Clone, Copy)]
pub(crate) struct Report([u64; Counter::ALL.len()]);

impl Report {
    pub(crate) fn record(&mut self, values: &[(Counter, u64)]) -> Result<()> {
        let mut next = self.0;
        for &(counter, count) in values {
            next[counter as usize] = next[counter as usize]
                .checked_add(count)
                .ok_or_else(overflow)?;
        }
        self.0 = next;
        Ok(())
    }

    pub(crate) fn append_certificate(&self, proof: &mut String) {
        use std::fmt::Write as _;
        if self.0.iter().all(|&count| count == 0) {
            return;
        }
        proof.push_str(" native_payload_work_version=1 native_payload_work_scope=completed_owner_batches native_payload_cells=logical_slots native_payload_bytes=visible_constructed_buffers");
        for counter in Counter::ALL {
            write!(proof, " {}={}", counter.name(), self.0[counter as usize])
                .expect("String writes cannot fail");
        }
    }
}

pub(crate) fn record_sort_gather(
    context: &crate::resident_session::NativeExecutionContext<'_>,
    rows: usize,
    fields: usize,
    output: &vortex::array::ArrayRef,
) -> Result<()> {
    let width = output.dtype().as_struct_fields().names().len();
    context.record_payload_work(&[
        (Counter::SortGathers, 1),
        (Counter::SortSelectedCells, logical_cells(rows, fields)?),
        (
            Counter::SortRetainedCells,
            logical_cells(output.len(), width)?,
        ),
        (
            Counter::SortOmittedCells,
            logical_cells(rows, fields.checked_sub(width).ok_or_else(overflow)?)?,
        ),
        (Counter::SortBufferBytes, output.nbytes()),
    ])
}

pub(crate) fn logical_cells(rows: usize, fields: usize) -> Result<u64> {
    (rows as u64)
        .checked_mul(fields as u64)
        .ok_or_else(overflow)
}

fn overflow() -> ShardLoomError {
    ShardLoomError::InvalidOperation(
        "native payload work counter overflow; no fallback execution was attempted".into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_work_reports_zero_counters_and_rejects_partial_overflow() {
        for (index, counter) in Counter::ALL.into_iter().enumerate() {
            assert_eq!(counter as usize, index);
        }
        assert_eq!(
            logical_cells(usize::MAX, usize::MAX).ok(),
            (usize::MAX as u64).checked_mul(usize::MAX as u64),
        );
        let mut report = Report::default();
        let mut proof = String::new();
        report.append_certificate(&mut proof);
        assert_eq!(proof, "");
        report
            .record(&[(Counter::PreparedCoordinates, u64::MAX)])
            .unwrap();
        assert!(
            report
                .record(&[
                    (Counter::PreparedSelections, 1),
                    (Counter::PreparedCoordinates, 1)
                ])
                .is_err()
        );
        report.append_certificate(&mut proof);
        assert!(proof.contains("payload_prepared_selections=0"));
        assert!(proof.contains(&format!("payload_prepared_coordinates={}", u64::MAX)));
        assert!(proof.contains("payload_scalar_coordinates=0"));
        assert!(proof.contains("sort_payload_buffer_bytes=0"));
    }
}
