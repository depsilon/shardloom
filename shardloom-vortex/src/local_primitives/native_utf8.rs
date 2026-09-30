//! Native UTF8 reads borrow; chunk retention and persistent keys own explicitly.

use std::sync::{Arc, OnceLock};

use vortex::{array::arrays::VarBinViewArray, buffer::BufferString};

#[cfg(test)]
thread_local! {
    pub(super) static SOURCE_PROMOTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Borrow a physical value while its native array remains alive. Callers handle
/// validity and UTF8 validation, just as they do for `VarBinViewArray::bytes_at`.
///
/// Vortex's `BinaryView::bytes` supports pre-resolved buffer slices. These callers
/// already own the array, so use its view/buffer APIs without allocating a second
/// buffer directory or cloning a buffer handle on each lookup. Escaping values
/// must instead acquire an owner or copy into independent persistent state.
#[inline]
pub(super) fn borrowed_bytes(source: &VarBinViewArray, row: usize) -> &[u8] {
    let view = &source.views()[row];
    if view.is_inlined() {
        view.as_inlined().value()
    } else {
        let reference = view.as_view();
        &source.buffer(reference.buffer_index as usize)[reference.as_range()]
    }
}

#[derive(Clone, Debug)]
pub(super) enum Utf8DictionaryValue {
    Owned(Arc<str>),
    Source {
        value: BufferString,
        independent: OnceLock<Arc<str>>,
    },
}

impl Utf8DictionaryValue {
    pub(super) fn source(value: BufferString) -> Self {
        Self::Source {
            value,
            independent: OnceLock::new(),
        }
    }

    /// Persistent state must not pin a complete provider buffer for one key.
    /// Repeated consumers of this dictionary entry share one independent copy,
    /// including duplicate DISTINCT rows and simultaneous MIN/MAX adoption.
    /// The cached Arc lives only as long as this entry or an escaping consumer.
    pub(super) fn to_owned_arc(&self) -> Arc<str> {
        match self {
            Self::Owned(value) => Arc::clone(value),
            Self::Source { value, independent } => Arc::clone(independent.get_or_init(|| {
                #[cfg(test)]
                SOURCE_PROMOTIONS.with(|count| count.set(count.get() + 1));
                Arc::from(value.as_str())
            })),
        }
    }
}

impl AsRef<str> for Utf8DictionaryValue {
    fn as_ref(&self) -> &str {
        match self {
            Self::Owned(value) => value,
            Self::Source { value, .. } => value.as_str(),
        }
    }
}

impl std::ops::Deref for Utf8DictionaryValue {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_ref()
    }
}

impl From<Arc<str>> for Utf8DictionaryValue {
    fn from(value: Arc<str>) -> Self {
        Self::Owned(value)
    }
}
