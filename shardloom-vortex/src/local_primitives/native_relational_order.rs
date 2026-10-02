//! Fallible, cancellable ordinal ordering with no auxiliary allocation.

use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;
use std::cmp::Ordering;

/// Heap ordering is unstable; callers include source ordinals as the final key
/// when stable ties are required. Errors stop before any sorted output is used.
pub(super) fn sort(
    rows: &mut [usize],
    cancellation: &CancellationToken,
    mut compare: impl FnMut(usize, usize) -> Result<Ordering>,
) -> Result<()> {
    cancellation.check()?;
    let mut comparisons = 0usize;
    let mut compare = |left, right| {
        if comparisons.is_multiple_of(1024) {
            cancellation.check()?;
        }
        comparisons = comparisons.wrapping_add(1);
        compare(left, right)
    };
    for root in (0..rows.len() / 2).rev() {
        sift(rows, root, &mut compare)?;
    }
    for end in (1..rows.len()).rev() {
        rows.swap(0, end);
        sift(&mut rows[..end], 0, &mut compare)?;
    }
    cancellation.check()
}

fn sift(
    rows: &mut [usize],
    mut root: usize,
    compare: &mut impl FnMut(usize, usize) -> Result<Ordering>,
) -> Result<()> {
    // A node has a child exactly while root < floor(len / 2). This also
    // establishes the bounds for 2 * root + 1 without saturating arithmetic.
    while root < rows.len() / 2 {
        let mut child = root * 2 + 1;
        if child + 1 < rows.len() && compare(rows[child], rows[child + 1])?.is_lt() {
            child += 1;
        }
        if !compare(rows[root], rows[child])?.is_lt() {
            break;
        }
        rows.swap(root, child);
        root = child;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_relational_order_matches_total_order_and_propagates_comparison_failure_and_cancel() {
        for length in [0, 1, 2, 3, 31, 1024, 4097] {
            let mut rows = (0..length).rev().collect::<Vec<_>>();
            let mut expected = rows.clone();
            expected.sort_by_key(|row| (row % 17, *row));
            sort(&mut rows, &CancellationToken::default(), |a, b| {
                Ok((a % 17, a).cmp(&(b % 17, b)))
            })
            .unwrap();
            assert_eq!(rows, expected);
        }
        let mut rows: Vec<_> = (0..4097).collect();
        let mut calls = 0;
        assert!(
            sort(&mut rows, &CancellationToken::default(), |a, b| {
                calls += 1;
                if calls == 10 {
                    return Err(super::super::native_relational_batch::failed(
                        "comparison failed",
                    ));
                }
                Ok(a.cmp(&b))
            })
            .is_err()
        );
        assert_eq!(calls, 10);
        let cancel = CancellationToken::default();
        let mut calls = 0;
        assert!(
            sort(&mut rows, &cancel, |a, b| {
                calls += 1;
                cancel.cancel();
                Ok(a.cmp(&b))
            })
            .is_err()
        );
        assert!(calls <= 1024);
    }
}
