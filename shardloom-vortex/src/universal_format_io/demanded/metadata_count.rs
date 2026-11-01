//! Exact cardinality is an optional held-source capability, not a row hint.

use super::{ColumnarReadReport, NativeExecutionContext, PreparedColumnarSource, Result, failed};
use parquet::file::metadata::ParquetMetaData;
use shardloom_exec::compute_pool::CancellationToken;

impl PreparedColumnarSource {
    pub(crate) fn exact_metadata_count(
        &self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<(u64, ColumnarReadReport)>> {
        context.check_cancelled()?;
        if self.max_rows.is_some() || self.row_groups.len() != self.source.row_groups_total {
            return Ok(None);
        }
        if !context.memory().owns(&self.metadata)
            || !context.memory().owns(&self.source.metadata_credit)
        {
            return Err(failed("metadata count belongs to a different memory owner"));
        }
        self.source
            .validate_generation_controlled(context.cancellation())?;
        let (rows, format) = if self.source.is_parquet() {
            (
                count_files(
                    self.source
                        .parquet_metadata()
                        .map(|metadata| metadata.metadata().as_ref()),
                    context.cancellation(),
                )?,
                "parquet",
            )
        } else if let Some(source) = &self.source.other
            && source.format == "orc"
        {
            let mut rows = 0_u64;
            for file in &source.files {
                context.check_cancelled()?;
                let metadata = file
                    .orc
                    .as_ref()
                    .ok_or_else(|| failed("ORC metadata owner missing"))?;
                let Some(count) = metadata.exact_number_of_rows().map_err(failed)? else {
                    return Ok(None);
                };
                rows = rows.checked_add(count).ok_or_else(|| {
                    failed("metadata count exceeds the unsigned 64-bit result domain")
                })?;
            }
            (rows, "orc")
        } else {
            return Ok(None);
        };
        self.source
            .validate_generation_controlled(context.cancellation())?;
        let report = ColumnarReadReport {
            format,
            parquet_selected_chunks: self.source.other.is_none(),
            projected_columns: 0,
            available_columns: self.source.schema().fields().len(),
            physical_leaf_columns: 0,
            row_groups_total: self.source.row_groups_total,
            row_groups_read: 0,
            rows_decoded: 0,
            adaptive_decode: shardloom_exec::adaptive_work::AdaptiveWorkReport::default(),
            requested_prefix_rows: None,
            files: self.source.collection.files.len(),
            fingerprint_read_bytes: self.source.fingerprint_read_bytes,
            content_fingerprints: self.source.content_fingerprints.clone(),
            metadata_read_bytes: self.source.metadata_read_bytes,
            schema_discovery_read_bytes: 0,
            data_read_bytes: 0,
            data_read_requests: 0,
        };
        Ok(Some((rows, report)))
    }
}

fn count_files<'a>(
    files: impl Iterator<Item = &'a ParquetMetaData>,
    cancellation: &CancellationToken,
) -> Result<u64> {
    let mut rows = 0_u64;
    for metadata in files {
        cancellation.check()?;
        rows = rows
            .checked_add(file_rows(metadata, cancellation)?)
            .ok_or_else(|| failed("metadata count exceeds the unsigned 64-bit result domain"))?;
    }
    cancellation.check()?;
    Ok(rows)
}

pub(super) fn file_rows(
    metadata: &ParquetMetaData,
    cancellation: &CancellationToken,
) -> Result<u64> {
    let declared = u64::try_from(metadata.file_metadata().num_rows())
        .map_err(|_| failed("metadata count requires a nonnegative file row count"))?;
    let mut rows = 0_u64;
    for group in metadata.row_groups() {
        cancellation.check()?;
        let count = u64::try_from(group.num_rows())
            .map_err(|_| failed("metadata count requires nonnegative row-group counts"))?;
        rows = rows
            .checked_add(count)
            .ok_or_else(|| failed("metadata row-group count overflow"))?;
    }
    if rows != declared {
        return Err(failed(
            "file and row-group counts disagree; metadata cannot prove an exact count",
        ));
    }
    cancellation.check()?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::{
        file::metadata::{FileMetaData, RowGroupMetaData},
        schema::types::{SchemaDescriptor, Type},
    };
    use std::sync::Arc;

    fn metadata(declared: i64, groups: &[i64]) -> ParquetMetaData {
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("counts").build().unwrap(),
        )));
        ParquetMetaData::new(
            FileMetaData::new(1, declared, None, None, schema.clone(), None),
            groups
                .iter()
                .map(|rows| {
                    RowGroupMetaData::builder(schema.clone())
                        .set_num_rows(*rows)
                        .build()
                        .unwrap()
                })
                .collect(),
        )
    }

    #[test]
    fn metadata_count_validates_exact_u64_collection_totals_without_float_rounding() {
        let token = CancellationToken::default();
        for (total, groups) in [
            (0, vec![]),
            (17, vec![3, 0, 14]),
            ((1 << 53) + 1, vec![1 << 53, 1]),
            (i64::MAX, vec![i64::MAX]),
        ] {
            assert_eq!(
                file_rows(&metadata(total, &groups), &token).unwrap(),
                u64::try_from(total).unwrap()
            );
        }
        let files = [
            metadata(i64::MAX, &[i64::MAX]),
            metadata(i64::MAX, &[i64::MAX]),
            metadata(1, &[1]),
        ];
        assert_eq!(count_files(files.iter(), &token).unwrap(), u64::MAX);
        let extra = metadata(1, &[1]);
        assert!(
            count_files(files.iter().chain(std::iter::once(&extra)), &token)
                .unwrap_err()
                .to_string()
                .contains("unsigned 64-bit")
        );
    }

    #[test]
    fn metadata_count_rejects_negative_inconsistent_overflowing_and_cancelled_metadata() {
        let token = CancellationToken::default();
        for (total, groups) in [
            (-1, vec![]),
            (1, vec![-1, 2]),
            (9, vec![4, 4]),
            (0, vec![1]),
            (1, vec![]),
            (i64::MAX, vec![i64::MAX, i64::MAX, 2]),
        ] {
            assert!(file_rows(&metadata(total, &groups), &token).is_err());
        }
        token.cancel();
        assert!(file_rows(&metadata(0, &[]), &token).is_err());
        assert!(count_files(std::iter::empty(), &token).is_err());
    }
}
