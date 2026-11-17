//! Borrowed schema continuations. A wide struct contributes one cursor, and
//! lazy child types are inspected only when their preceding sibling finishes.

use super::{DType, MemoryLease, Result, failed, provider_error};
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::dtype::DTypeChildren;
use vortex::error::{VortexError, VortexResult};

pub(super) struct Frame<'a> {
    children: DTypeChildren<'a>,
    depth: usize,
}

pub(super) struct Pending<'a> {
    cursor: Cursor<'a>,
    // Frames are released before their temporary capacity credit.
    credit: MemoryLease,
}

impl<'a> Pending<'a> {
    pub(super) fn new(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            cursor: Cursor::default(),
            credit: memory.reserve(0)?,
        })
    }

    pub(super) fn enter(
        &mut self,
        dtype: &'a DType,
        depth: usize,
    ) -> Result<Option<(&'a DType, usize)>> {
        self.cursor.enter(
            dtype,
            depth,
            |frames| reserve_frame(frames, Some(&mut self.credit)),
            &|error| provider_error(&error),
        )
    }

    pub(super) fn next(&mut self) -> Result<Option<(&'a DType, usize)>> {
        self.cursor.next().map_err(|error| provider_error(&error))
    }
}

#[derive(Default)]
pub(super) struct Cursor<'a> {
    frames: Vec<Frame<'a>>,
}

impl<'a> Cursor<'a> {
    pub(super) fn enter<E>(
        &mut self,
        dtype: &'a DType,
        depth: usize,
        grow: impl FnOnce(&mut Vec<Frame<'a>>) -> std::result::Result<(), E>,
        provider_error: &impl Fn(VortexError) -> E,
    ) -> std::result::Result<Option<(&'a DType, usize)>, E> {
        let mut children = dtype.try_child_dtypes();
        if children.len() > 1 {
            grow(&mut self.frames)?;
        }
        let first = children.next().transpose().map_err(provider_error)?;
        if children.len() > 0 {
            self.frames.push(Frame { children, depth });
        }
        Ok(first.map(|dtype| (dtype, depth)))
    }

    pub(super) fn next(&mut self) -> VortexResult<Option<(&'a DType, usize)>> {
        let Some(frame) = self.frames.last_mut() else {
            return Ok(None);
        };
        let child = frame
            .children
            .next()
            .expect("schema cursor stays within its borrowed fields")?;
        let depth = frame.depth;
        if frame.children.len() == 0 {
            self.frames.pop();
        }
        Ok(Some((child, depth)))
    }
}

/// Admit replacement overlap before growing traversal storage. Native callers
/// supply their live credit. Arrow also supports bounded inspection without a pool.
pub(crate) fn reserve_frame<T>(
    frames: &mut Vec<T>,
    mut credit: Option<&mut MemoryLease>,
) -> Result<()> {
    if frames.len() < frames.capacity() {
        return Ok(());
    }
    let capacity = frames
        .capacity()
        .max(4)
        .checked_mul(2)
        .ok_or_else(|| failed("schema traversal capacity overflow"))?;
    let bytes = capacity
        .checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| failed("schema traversal capacity overflow"))?;
    if let Some(owner) = credit.as_deref_mut() {
        owner.resize(
            owner
                .bytes()
                .checked_add(bytes)
                .ok_or_else(|| failed("schema traversal capacity overflow"))?,
        )?;
    }
    frames
        .try_reserve_exact(capacity - frames.len())
        .map_err(|_| failed("schema traversal allocation failed"))?;
    if frames.capacity() > capacity {
        return Err(failed("schema traversal exceeded admitted capacity"));
    }
    credit.map_or(Ok(()), |owner| owner.resize(bytes))
}
