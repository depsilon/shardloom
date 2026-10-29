//! Charged order statistics with stable observation ordinals. AVL
//! height bounds update/selection work; departed nodes return to a free list.
//! Native spill owns partition/value files separately from this active index.
//! Numeric domains retain exact cells; native ordered domains compare source
//! ordinals through their existing owner and retain no copied key payload.

use super::{
    native_capacity::ReservedVec,
    native_quantile,
    native_relational_batch::failed,
    native_relational_keys::{Cell, compare_cells},
};
use shardloom_core::Result;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::cmp::Ordering;

pub(super) type Root = Option<usize>;

struct Node {
    value: Cell,
    ordinal: usize,
    left: Root,
    right: Root,
    height: u8,
    count: u64,
}

pub(super) struct Arena {
    nodes: ReservedVec<Node>,
    free: Root,
}

impl Arena {
    pub(super) fn new(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            nodes: ReservedVec::new(memory)?,
            free: None,
        })
    }

    pub(super) fn count(&self, root: Root) -> u64 {
        root.map_or(0, |index| self.nodes.values[index].count)
    }

    fn height(&self, root: Root) -> u8 {
        root.map_or(0, |index| self.nodes.values[index].height)
    }

    fn refresh(&mut self, index: usize) -> Result<()> {
        let node = &self.nodes.values[index];
        let count = self
            .count(node.left)
            .checked_add(self.count(node.right))
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| failed("quantile index count overflow"))?;
        let height = self
            .height(node.left)
            .max(self.height(node.right))
            .checked_add(1)
            .ok_or_else(|| failed("quantile index height overflow"))?;
        let node = &mut self.nodes.values[index];
        node.count = count;
        node.height = height;
        Ok(())
    }

    fn rotate_left(&mut self, index: usize) -> Result<usize> {
        let next = self.nodes.values[index]
            .right
            .ok_or_else(|| failed("quantile rotation lost its right child"))?;
        self.nodes.values[index].right = self.nodes.values[next].left;
        self.nodes.values[next].left = Some(index);
        self.refresh(index)?;
        self.refresh(next)?;
        Ok(next)
    }

    fn rotate_right(&mut self, index: usize) -> Result<usize> {
        let next = self.nodes.values[index]
            .left
            .ok_or_else(|| failed("quantile rotation lost its left child"))?;
        self.nodes.values[index].left = self.nodes.values[next].right;
        self.nodes.values[next].right = Some(index);
        self.refresh(index)?;
        self.refresh(next)?;
        Ok(next)
    }

    fn balance(&mut self, index: usize) -> Result<usize> {
        self.refresh(index)?;
        let node = &self.nodes.values[index];
        let delta = i16::from(self.height(node.left)) - i16::from(self.height(node.right));
        if delta > 1 {
            let child = node
                .left
                .ok_or_else(|| failed("quantile balance lost its left child"))?;
            let node = &self.nodes.values[child];
            if self.height(node.left) < self.height(node.right) {
                self.nodes.values[index].left = Some(self.rotate_left(child)?);
            }
            self.rotate_right(index)
        } else if delta < -1 {
            let child = node
                .right
                .ok_or_else(|| failed("quantile balance lost its right child"))?;
            let node = &self.nodes.values[child];
            if self.height(node.right) < self.height(node.left) {
                self.nodes.values[index].right = Some(self.rotate_right(child)?);
            }
            self.rotate_left(index)
        } else {
            Ok(index)
        }
    }

    fn compare(
        &self,
        index: usize,
        value: &Cell,
        ordinal: usize,
        compare: &mut impl FnMut(&Cell, usize, &Cell, usize) -> Result<Ordering>,
    ) -> Result<Ordering> {
        let node = &self.nodes.values[index];
        Ok(compare(value, ordinal, &node.value, node.ordinal)?
            .then_with(|| ordinal.cmp(&node.ordinal)))
    }

    fn allocate(&mut self, value: Cell, ordinal: usize) -> Result<usize> {
        let node = Node {
            value,
            ordinal,
            left: None,
            right: None,
            height: 1,
            count: 1,
        };
        if let Some(index) = self.free {
            self.free = self.nodes.values[index].left;
            self.nodes.values[index] = node;
            Ok(index)
        } else {
            let index = self.nodes.values.len();
            self.nodes.push(node)?;
            Ok(index)
        }
    }

    fn release(&mut self, index: usize) {
        self.nodes.values[index] = Node {
            value: Cell::Null,
            ordinal: 0,
            left: self.free,
            right: None,
            height: 0,
            count: 0,
        };
        self.free = Some(index);
    }

    #[cfg(test)]
    pub(super) fn insert(
        &mut self,
        root: Root,
        value: Cell,
        ordinal: usize,
        cancellation: &CancellationToken,
    ) -> Result<Root> {
        self.insert_with_policy(
            root,
            value,
            ordinal,
            cancellation,
            shardloom_core::FloatPolicy::Strict,
        )
    }

    pub(super) fn insert_with_policy(
        &mut self,
        root: Root,
        value: Cell,
        ordinal: usize,
        cancellation: &CancellationToken,
        policy: shardloom_core::FloatPolicy,
    ) -> Result<Root> {
        cancellation.check()?;
        native_quantile::validate(&value, policy)?;
        self.insert_inner(root, value, ordinal, &mut |left, _, right, _| {
            compare_cells(left.clone(), right.clone())
        })
        .map(Some)
    }

    /// Retain only a source ordinal under its owner's native comparison. Stable
    /// ordinal order breaks equal keys, just as for the numeric entry point.
    pub(super) fn insert_by(
        &mut self,
        root: Root,
        ordinal: usize,
        cancellation: &CancellationToken,
        mut compare: impl FnMut(usize, usize) -> Result<Ordering>,
    ) -> Result<Root> {
        cancellation.check()?;
        self.insert_inner(root, Cell::Null, ordinal, &mut |_, left, _, right| {
            compare(left, right)
        })
        .map(Some)
    }

    // Recursion follows an AVL path, bounded by the logarithm of a u64 count.
    fn insert_inner(
        &mut self,
        root: Root,
        value: Cell,
        ordinal: usize,
        compare: &mut impl FnMut(&Cell, usize, &Cell, usize) -> Result<Ordering>,
    ) -> Result<usize> {
        let Some(index) = root else {
            return self.allocate(value, ordinal);
        };
        match self.compare(index, &value, ordinal, compare)? {
            Ordering::Less => {
                let child =
                    self.insert_inner(self.nodes.values[index].left, value, ordinal, compare)?;
                self.nodes.values[index].left = Some(child);
            }
            Ordering::Greater => {
                let child =
                    self.insert_inner(self.nodes.values[index].right, value, ordinal, compare)?;
                self.nodes.values[index].right = Some(child);
            }
            Ordering::Equal => {
                return Err(failed("quantile observation ordinal was inserted twice"));
            }
        }
        self.balance(index)
    }

    pub(super) fn remove(
        &mut self,
        root: Root,
        value: &Cell,
        ordinal: usize,
        cancellation: &CancellationToken,
    ) -> Result<Root> {
        cancellation.check()?;
        self.remove_inner(root, value, ordinal, &mut |left, _, right, _| {
            compare_cells(left.clone(), right.clone())
        })
    }

    pub(super) fn remove_by(
        &mut self,
        root: Root,
        ordinal: usize,
        cancellation: &CancellationToken,
        mut compare: impl FnMut(usize, usize) -> Result<Ordering>,
    ) -> Result<Root> {
        cancellation.check()?;
        self.remove_inner(root, &Cell::Null, ordinal, &mut |_, left, _, right| {
            compare(left, right)
        })
    }

    fn remove_inner(
        &mut self,
        root: Root,
        value: &Cell,
        ordinal: usize,
        compare: &mut impl FnMut(&Cell, usize, &Cell, usize) -> Result<Ordering>,
    ) -> Result<Root> {
        let index = root.ok_or_else(|| failed("departed quantile observation is absent"))?;
        match self.compare(index, value, ordinal, compare)? {
            Ordering::Less => {
                self.nodes.values[index].left =
                    self.remove_inner(self.nodes.values[index].left, value, ordinal, compare)?;
            }
            Ordering::Greater => {
                self.nodes.values[index].right =
                    self.remove_inner(self.nodes.values[index].right, value, ordinal, compare)?;
            }
            Ordering::Equal => {
                let node = &self.nodes.values[index];
                match (node.left, node.right) {
                    (None, next) | (next, None) => {
                        self.release(index);
                        return Ok(next);
                    }
                    (Some(_), Some(right)) => {
                        let mut next = right;
                        while let Some(left) = self.nodes.values[next].left {
                            next = left;
                        }
                        let value = self.nodes.values[next].value.clone();
                        let ordinal = self.nodes.values[next].ordinal;
                        let child = self.remove_inner(Some(right), &value, ordinal, compare)?;
                        let node = &mut self.nodes.values[index];
                        node.value = value;
                        node.ordinal = ordinal;
                        node.right = child;
                    }
                }
            }
        }
        self.balance(index).map(Some)
    }

    pub(super) fn select(
        &self,
        root: Root,
        mut rank: u64,
        cancellation: &CancellationToken,
    ) -> Result<(&Cell, usize)> {
        cancellation.check()?;
        if rank >= self.count(root) {
            return Err(failed("quantile selected rank exceeds active observations"));
        }
        let mut next = root;
        while let Some(index) = next {
            let node = &self.nodes.values[index];
            let left = self.count(node.left);
            match rank.cmp(&left) {
                Ordering::Less => next = node.left,
                Ordering::Equal => return Ok((&node.value, node.ordinal)),
                Ordering::Greater => {
                    rank -= left + 1;
                    next = node.right;
                }
            }
        }
        Err(failed("quantile index lost its selected observation"))
    }

    /// Linear ordered output with logarithmic stack depth and no temporary
    /// row-sized traversal container. The caller owns and charges its output.
    pub(super) fn visit(
        &self,
        root: Root,
        cancellation: &CancellationToken,
        consume: &mut impl FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        cancellation.check()?;
        let Some(index) = root else {
            return Ok(());
        };
        let node = &self.nodes.values[index];
        self.visit(node.left, cancellation, consume)?;
        consume(node.ordinal)?;
        self.visit(node.right, cancellation, consume)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn validate(arena: &Arena, root: Root) -> (u64, u8) {
        let Some(index) = root else {
            return (0, 0);
        };
        let node = &arena.nodes.values[index];
        let (left, lh) = validate(arena, node.left);
        let (right, rh) = validate(arena, node.right);
        assert_eq!(node.count, left + right + 1);
        assert_eq!(node.height, lh.max(rh) + 1);
        assert!(lh.abs_diff(rh) <= 1);
        (node.count, node.height)
    }

    #[test]
    fn exact_quantile_index_matches_sorted_reference_and_reuses_departed_slots() {
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let cancellation = CancellationToken::default();
        let mut arena = Arena::new(&memory).unwrap();
        let mut root = None;
        let mut expected = BTreeSet::new();
        for ordinal in 0..8192usize {
            let value = u64::try_from((ordinal * 541) % 127).unwrap();
            root = arena
                .insert(
                    root,
                    Cell::NonnegativeInteger(value),
                    ordinal,
                    &cancellation,
                )
                .unwrap();
            expected.insert((value, ordinal));
            if ordinal >= 96 {
                let departed = ordinal - 96;
                let value = u64::try_from((departed * 541) % 127).unwrap();
                root = arena
                    .remove(
                        root,
                        &Cell::NonnegativeInteger(value),
                        departed,
                        &cancellation,
                    )
                    .unwrap();
                assert!(expected.remove(&(value, departed)));
            }
            validate(&arena, root);
            for (rank, &(value, ordinal)) in expected.iter().enumerate() {
                let (actual, observed) = arena
                    .select(root, u64::try_from(rank).unwrap(), &cancellation)
                    .unwrap();
                assert_eq!(actual, &Cell::NonnegativeInteger(value));
                assert_eq!(observed, ordinal);
            }
        }
        assert!(arena.nodes.values.len() <= 97);
        for (value, ordinal) in expected {
            root = arena
                .remove(
                    root,
                    &Cell::NonnegativeInteger(value),
                    ordinal,
                    &cancellation,
                )
                .unwrap();
            validate(&arena, root);
        }
        assert_eq!(root, None);
        drop(arena);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn exact_quantile_index_denies_growth_and_cancel_without_losing_existing_ranks() {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let cancellation = CancellationToken::default();
        let mut arena = Arena::new(&memory).unwrap();
        let mut root = None;
        let mut count = 0;
        while let Ok(next) = arena.insert(
            root,
            Cell::NonnegativeInteger(count),
            usize::try_from(count).unwrap(),
            &cancellation,
        ) {
            root = next;
            count += 1;
        }
        assert!(count > 0 && count < 128);
        assert_eq!(arena.count(root), count);
        validate(&arena, root);
        for rank in 0..count {
            assert_eq!(
                arena.select(root, rank, &cancellation).unwrap().0,
                &Cell::NonnegativeInteger(rank)
            );
        }
        let cancelled = CancellationToken::default();
        cancelled.cancel();
        assert!(
            arena
                .insert(root, Cell::NonnegativeInteger(0), 999, &cancelled)
                .is_err()
        );
        assert!(
            arena
                .remove(root, &Cell::NonnegativeInteger(0), 0, &cancelled)
                .is_err()
        );
        assert!(arena.select(root, 0, &cancelled).is_err());
        assert_eq!(arena.count(root), count);
        drop(arena);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn ordered_collection_index_matches_native_comparator_order_and_reuses_departed_slots() {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let cancellation = CancellationToken::default();
        let mut arena = Arena::new(&memory).unwrap();
        let mut root = None;
        let key = |row: usize| std::cmp::Reverse((row * 541) % 127);
        let compare = |left, right| Ok(key(left).cmp(&key(right)));
        let mut expected = BTreeSet::new();
        for ordinal in 0..4096 {
            if ordinal >= 64 {
                let departed = ordinal - 64;
                root = arena
                    .remove_by(root, departed, &cancellation, compare)
                    .unwrap();
                assert!(expected.remove(&(key(departed), departed)));
            }
            root = arena
                .insert_by(root, ordinal, &cancellation, compare)
                .unwrap();
            expected.insert((key(ordinal), ordinal));
            validate(&arena, root);
            let mut actual = Vec::new();
            arena
                .visit(root, &cancellation, &mut |row| {
                    actual.push(row);
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                actual,
                expected.iter().map(|(_, row)| *row).collect::<Vec<_>>()
            );
        }
        assert_eq!(arena.nodes.values.len(), 64);
        let count = arena.count(root);
        let fail = |_, _| Err(failed("injected native comparator failure"));
        assert!(arena.insert_by(root, 9999, &cancellation, fail).is_err());
        assert!(arena.remove_by(root, 4095, &cancellation, fail).is_err());
        assert_eq!(arena.count(root), count);
        validate(&arena, root);
        let cancelled = CancellationToken::default();
        cancelled.cancel();
        assert!(
            arena
                .visit(root, &cancelled, &mut |_| unreachable!())
                .is_err()
        );
        drop(arena);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
