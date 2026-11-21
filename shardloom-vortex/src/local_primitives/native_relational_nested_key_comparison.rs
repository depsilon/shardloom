//! Borrowed lexicographic comparison over prepared native key nodes.

use super::{Children, Column, Container, Frames, Value, failed};
use shardloom_core::Result;
use std::cmp::Ordering;

#[derive(Clone, Copy)]
struct Pair {
    left: usize,
    right: usize,
    row: usize,
    other_row: usize,
    root: bool,
}

enum Siblings {
    Struct {
        left: Option<usize>,
        right: Option<usize>,
        row: usize,
        other_row: usize,
    },
    List {
        left: usize,
        right: usize,
        start: usize,
        other_start: usize,
        next: usize,
        count: usize,
        length_order: Ordering,
    },
}

enum Compared {
    Value(Ordering),
    Children(Siblings),
}

impl Column {
    pub(in crate::local_primitives) fn compare_at(
        &self,
        row: usize,
        other: &Self,
        other_row: usize,
    ) -> Result<Ordering> {
        let mut frames = Frames::new(self.owner.pool())?;
        let mut next = Some(Pair {
            left: self.root,
            right: other.root,
            row,
            other_row,
            root: true,
        });
        loop {
            let mut siblings = if let Some(pair) = next.take() {
                match self.compare_pair(other, pair)? {
                    Compared::Value(Ordering::Equal) => continue,
                    Compared::Value(order) => return Ok(order),
                    Compared::Children(siblings) => siblings,
                }
            } else if let Some(siblings) = frames.pop() {
                siblings
            } else {
                return Ok(Ordering::Equal);
            };
            if let Some(pair) = siblings.next(self, other)? {
                if siblings.must_resume() {
                    frames.push(siblings)?;
                }
                next = Some(pair);
            } else if let Siblings::List { length_order, .. } = siblings
                && length_order != Ordering::Equal
            {
                return Ok(length_order);
            }
        }
    }

    fn compare_pair(&self, other: &Self, pair: Pair) -> Result<Compared> {
        let left = &self.nodes.values[pair.left].value;
        let right = &other.nodes.values[pair.right].value;
        let (left, right) = match (left, right) {
            (Value::Leaf(left), Value::Leaf(right)) => {
                return left
                    .compare_at(pair.row, right, pair.other_row)
                    .map(Compared::Value);
            }
            (Value::Container(left), Value::Container(right)) => (left, right),
            _ => return Err(failed("nested key structure changed after binding")),
        };
        if !pair.root && (pair.row >= left.rows || pair.other_row >= right.rows) {
            return Err(failed("comparison row index exceeds key owner"));
        }
        self.cancellation.check()?;
        other.cancellation.check()?;
        if !left.dtype.eq_ignore_nullability(&right.dtype) {
            return Err(failed(
                "nested keys require identical logical structure and leaf types",
            ));
        }
        match (left.is_null(pair.row)?, right.is_null(pair.other_row)?) {
            (true, true) => return Ok(Compared::Value(Ordering::Equal)),
            (true, false) => return Ok(Compared::Value(Ordering::Less)),
            (false, true) => return Ok(Compared::Value(Ordering::Greater)),
            (false, false) => {}
        }
        Siblings::for_pair(left, right, pair).map(Compared::Children)
    }
}

impl Siblings {
    fn for_pair(left: &Container, right: &Container, pair: Pair) -> Result<Self> {
        Ok(match (&left.children, &right.children) {
            (Children::Struct { first: left, .. }, Children::Struct { first: right, .. }) => {
                Self::Struct {
                    left: *left,
                    right: *right,
                    row: pair.row,
                    other_row: pair.other_row,
                }
            }
            (
                Children::List {
                    coordinates: left,
                    elements: left_elements,
                },
                Children::List {
                    coordinates: right,
                    elements: right_elements,
                },
            ) => {
                let (start, left_count) = left
                    .at(pair.row)?
                    .ok_or_else(|| failed("nested key validity changed"))?;
                let (other_start, right_count) = right
                    .at(pair.other_row)?
                    .ok_or_else(|| failed("nested key validity changed"))?;
                Self::List {
                    left: *left_elements,
                    right: *right_elements,
                    start,
                    other_start,
                    next: 0,
                    count: left_count.min(right_count),
                    length_order: left_count.cmp(&right_count),
                }
            }
            _ => return Err(failed("nested key structure changed after binding")),
        })
    }

    fn next(&mut self, left_owner: &Column, right_owner: &Column) -> Result<Option<Pair>> {
        Ok(Some(match self {
            Self::Struct {
                left,
                right,
                row,
                other_row,
            } => {
                let (Some(a), Some(b)) = (*left, *right) else {
                    return Ok(None);
                };
                left_owner.cancellation.check()?;
                *left = left_owner.nodes.values[a].next_sibling;
                *right = right_owner.nodes.values[b].next_sibling;
                Pair {
                    left: a,
                    right: b,
                    row: *row,
                    other_row: *other_row,
                    root: false,
                }
            }
            Self::List {
                left,
                right,
                start,
                other_start,
                next,
                count,
                ..
            } => {
                if *next == *count {
                    return Ok(None);
                }
                let index = *next;
                if index.is_multiple_of(1024) {
                    left_owner.cancellation.check()?;
                    right_owner.cancellation.check()?;
                }
                *next += 1;
                Pair {
                    left: *left,
                    right: *right,
                    row: *start + index,
                    other_row: *other_start + index,
                    root: false,
                }
            }
        }))
    }

    fn must_resume(&self) -> bool {
        match self {
            Self::Struct { left, right, .. } => left.is_some() && right.is_some(),
            Self::List {
                next,
                count,
                length_order,
                ..
            } => next < count || *length_order != Ordering::Equal,
        }
    }
}
