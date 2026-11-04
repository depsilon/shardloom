//! Completed native window-owner work, without row values or per-row traces.

use shardloom_core::{Result, ShardLoomError};

const NAMES: [&str; 6] = [
    "window_reused_partition_spans",
    "window_reused_partition_rows",
    "window_peer_scan_partitions",
    "window_peer_scan_rows",
    "window_position_only_partitions",
    "window_position_only_rows",
];

#[derive(Default, Clone, Copy)]
pub(crate) struct Report([u64; NAMES.len()]);

impl Report {
    pub(crate) fn spans(partitions: usize, rows: usize) -> Self {
        Self([partitions as u64, rows as u64, 0, 0, 0, 0])
    }

    pub(crate) fn partitions(partitions: usize, rows: usize, peers: bool) -> Self {
        if peers {
            Self([0, 0, partitions as u64, rows as u64, 0, 0])
        } else {
            Self([0, 0, 0, 0, partitions as u64, rows as u64])
        }
    }

    pub(crate) fn record(&mut self, completed: Self) -> Result<()> {
        let mut next = self.0;
        for (total, count) in next.iter_mut().zip(completed.0) {
            *total = total.checked_add(count).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "native window work counter overflow; no fallback execution was attempted"
                        .into(),
                )
            })?;
        }
        self.0 = next;
        Ok(())
    }

    pub(crate) fn append_certificate(&self, proof: &mut String) {
        use std::fmt::Write as _;
        if self.0.iter().all(|&count| count == 0) {
            return;
        }
        proof.push_str(" native_window_work_version=1 native_window_work_scope=completed_groups");
        for (name, count) in NAMES.into_iter().zip(self.0) {
            write!(proof, " {name}={count}").expect("String writes cannot fail");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_work_keeps_peer_and_position_rows_separate_and_rejects_partial_overflow() {
        let mut report = Report::default();
        report.record(Report::spans(3, 37)).unwrap();
        report.record(Report::partitions(2, 20, true)).unwrap();
        report.record(Report::partitions(1, 17, false)).unwrap();
        assert_eq!(report.0, [3, 37, 2, 20, 1, 17]);
        let mut proof = String::new();
        report.append_certificate(&mut proof);
        assert!(proof.contains("window_reused_partition_spans=3"));
        assert!(proof.contains("window_peer_scan_rows=20"));
        assert!(proof.contains("window_position_only_rows=17"));
        let before = report.0;
        assert!(report.record(Report([1, u64::MAX, 0, 0, 0, 0])).is_err());
        assert_eq!(report.0, before);
    }
}
