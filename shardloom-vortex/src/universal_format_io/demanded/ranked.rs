//! The adapter supplies generation-bound positions and whole-group selections.
//! Native relational operators, never this decoder, choose ranking winners.

use super::{Arc, NativeExecutionContext, PreparedColumnarSource, ProjectionMask, Result, failed};

impl PreparedColumnarSource {
    pub(crate) fn prepare_ranking(
        &self,
        columns: &[String],
        prefix: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<Arc<Self>>> {
        context.check_cancelled()?;
        if self.max_rows.is_some() || self.row_groups.len() <= 1 || columns.is_empty() {
            return Ok(None);
        }
        let fields = self.source.metadata().schema().fields();
        if columns.iter().any(|name| {
            !self
                .projection
                .iter()
                .any(|&index| fields[index].name() == name)
        }) {
            return Err(failed(
                "ranking demand references an unadmitted source column",
            ));
        }
        let credit = context.memory().reserve(self.metadata.bytes())?;
        let projection: Vec<_> = self
            .projection
            .iter()
            .copied()
            .filter(|&index| columns.iter().any(|name| fields[index].name() == name))
            .collect();
        if projection.len() == self.projection.len() {
            return Ok(None);
        }
        let mut original_bytes = 0_u64;
        let mut ranking_bytes = 0_u64;
        let mut payload_groups = Vec::with_capacity(self.row_groups.len());
        for group in &self.row_groups {
            context.check_cancelled()?;
            let metadata = &self.source.files[group.file].metadata;
            let rank = ProjectionMask::roots(metadata.parquet_schema(), projection.iter().copied());
            let full =
                ProjectionMask::roots(metadata.parquet_schema(), self.projection.iter().copied());
            let mut payload_bytes = 0_u64;
            for (index, column) in metadata
                .metadata()
                .row_group(group.index)
                .columns()
                .iter()
                .enumerate()
            {
                let bytes = u64::try_from(column.compressed_size()).map_err(failed)?;
                if full.leaf_included(index) {
                    payload_bytes = add(payload_bytes, bytes)?;
                }
                if rank.leaf_included(index) {
                    ranking_bytes = add(ranking_bytes, bytes)?;
                }
            }
            original_bytes = add(original_bytes, payload_bytes)?;
            payload_groups.push(payload_bytes);
        }
        // Each selected row can introduce at most one winning group. Count the
        // largest possible groups, including key rereads, before admitting work.
        payload_groups.sort_unstable_by(|left, right| right.cmp(left));
        let worst_bytes = payload_groups
            .iter()
            .take(prefix)
            .try_fold(ranking_bytes, |sum, bytes| add(sum, *bytes))?;
        context.check_cancelled()?;
        if worst_bytes >= original_bytes {
            return Ok(None);
        }
        self.source
            .validate_generation_controlled(context.cancellation())?;
        Ok(Some(Arc::new(Self {
            source: Arc::clone(&self.source),
            projection,
            row_groups: self.row_groups.clone(),
            max_rows: None,
            metadata: credit,
        })))
    }

    pub(crate) fn prepare_selected(
        &self,
        positions: &[u64],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Arc<Self>> {
        context.check_cancelled()?;
        self.source
            .validate_generation_controlled(context.cancellation())?;
        let credit = context.memory().reserve(self.metadata.bytes())?;
        let mut groups = Vec::with_capacity(self.row_groups.len().min(positions.len()));
        let mut group_index = 0;
        let mut previous = None;
        for &position in positions {
            context.check_cancelled()?;
            if previous.is_some_and(|old| old >= position) {
                return Err(failed(
                    "selected source positions must be strictly increasing",
                ));
            }
            while let Some(group) = self.row_groups.get(group_index) {
                if position < add(group.first_row, group.rows)? {
                    break;
                }
                group_index += 1;
            }
            let group = self
                .row_groups
                .get(group_index)
                .filter(|group| position >= group.first_row)
                .ok_or_else(|| failed("selected position is outside the admitted source groups"))?;
            if groups.last().is_none_or(|last: &super::SourceGroup| {
                last.file != group.file || last.index != group.index
            }) {
                groups.push(*group);
            }
            previous = Some(position);
        }
        Ok(Arc::new(Self {
            source: Arc::clone(&self.source),
            projection: self.projection.clone(),
            row_groups: groups,
            max_rows: None,
            metadata: credit,
        }))
    }
}

fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| failed("ranked source extent or byte count overflow"))
}
