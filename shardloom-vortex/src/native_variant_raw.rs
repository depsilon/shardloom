//! Borrowed raw-byte walks inside the caller's validation workspace grant.

use super::{Result, Variant, VariantMetadata, failed, workspace};
use parquet_variant::{VariantList, VariantObject};

/// These walks run sequentially under the existing caller-held workspace. Each
/// descended container consumes at least four bytes. An 80-byte frame with
/// geometric growth has less than 60 bytes of old/new capacity per input byte;
/// the fixed allowance covers the initial capacity. Check the actual capacity
/// before/after every allocation so this is a bound, not a Vec growth guess.
struct Stack<T> {
    values: Vec<T>,
    allowance: usize,
}

impl<T> Stack<T> {
    fn new(bytes: usize) -> Result<Self> {
        Ok(Self {
            values: Vec::new(),
            allowance: usize::try_from(workspace(bytes)?).map_err(failed)?,
        })
    }

    fn push(&mut self, value: T) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            let capacity = self
                .values
                .capacity()
                .max(4)
                .checked_mul(2)
                .ok_or_else(|| failed("validation workspace overflow"))?;
            let overlap = self
                .values
                .capacity()
                .checked_add(capacity)
                .and_then(|count| count.checked_mul(size_of::<T>()))
                .ok_or_else(|| failed("validation workspace overflow"))?;
            if overlap > self.allowance {
                return Err(failed(
                    "validation continuation exceeds its workspace envelope",
                ));
            }
            self.values
                .try_reserve_exact(capacity - self.values.len())
                .map_err(failed)?;
            if self.values.capacity() > capacity {
                return Err(failed(
                    "validation continuation capacity exceeds its reservation",
                ));
            }
        }
        self.values.push(value);
        Ok(())
    }
}

pub(super) fn value<'m, 'v>(
    metadata: &'m [u8],
    value: &'v [u8],
    check: &dyn Fn() -> Result<()>,
    max_depth: usize,
) -> Result<Variant<'m, 'v>> {
    let mut remaining = value.len();
    envelope(value, &mut remaining, check, max_depth)?;
    let metadata = VariantMetadata::try_new(metadata).map_err(failed)?;
    let parsed = Variant::try_new_with_metadata(metadata, value).map_err(failed)?;
    unique_keys(&parsed, value.len(), check)?;
    Ok(parsed)
}

fn integer(bytes: &[u8], start: usize, width: usize) -> Result<usize> {
    let end = start
        .checked_add(width)
        .ok_or_else(|| failed("offset overflow"))?;
    let bytes = bytes
        .get(start..end)
        .ok_or_else(|| failed("truncated container framing"))?;
    Ok(bytes.iter().enumerate().fold(0, |value, (shift, byte)| {
        value | (usize::from(*byte) << (shift * 8))
    }))
}

struct Envelope<'a> {
    bytes: &'a [u8],
    values: &'a [u8],
    offsets: usize,
    width: usize,
    next: usize,
    count: usize,
    depth: usize,
    object: bool,
}

const _: () = assert!(size_of::<Envelope<'_>>() <= 80);

impl<'a> Envelope<'a> {
    fn child(&mut self) -> Result<Option<(&'a [u8], usize)>> {
        if self.next == self.count {
            return Ok(None);
        }
        let index = self.next;
        let start = integer(self.bytes, self.offsets + index * self.width, self.width)?;
        let end = if self.object {
            self.values.len()
        } else {
            integer(
                self.bytes,
                self.offsets + (index + 1) * self.width,
                self.width,
            )?
        };
        let child = self
            .values
            .get(start..end)
            .ok_or_else(|| failed("invalid child offsets"))?;
        self.next += 1;
        Ok(Some((child, self.depth)))
    }
}

fn framing<'a>(
    bytes: &'a [u8],
    depth: usize,
    remaining: &mut usize,
    check: &dyn Fn() -> Result<()>,
    max_depth: usize,
) -> Result<Option<Envelope<'a>>> {
    check()?;
    if depth > max_depth {
        return Err(failed("encoded value exceeds the current depth boundary"));
    }
    *remaining = remaining
        .checked_sub(1)
        .ok_or_else(|| failed("validation work exceeds its encoded byte envelope"))?;
    let header = *bytes.first().ok_or_else(|| failed("empty encoded value"))?;
    let basic = header & 3;
    if basic < 2 {
        return Ok(None);
    }
    let object = basic == 2;
    let count_width = if header & if object { 64 } else { 16 } != 0 {
        4
    } else {
        1
    };
    let count = integer(bytes, 1, count_width)?;
    let offset_width = usize::from((header >> 2) & 3) + 1;
    let id_width = if object {
        usize::from((header >> 4) & 3) + 1
    } else {
        0
    };
    let offsets = count
        .checked_mul(id_width)
        .and_then(|size| size.checked_add(1 + count_width))
        .ok_or_else(|| failed("container framing overflow"))?;
    let data = count
        .checked_add(1)
        .and_then(|count| count.checked_mul(offset_width))
        .and_then(|size| size.checked_add(offsets))
        .ok_or_else(|| failed("container framing overflow"))?;
    let values = bytes
        .get(data..)
        .ok_or_else(|| failed("truncated container framing"))?;
    Ok(Some(Envelope {
        bytes,
        values,
        offsets,
        width: offset_width,
        next: 0,
        count,
        depth: depth + 1,
        object,
    }))
}

fn envelope(
    bytes: &[u8],
    remaining: &mut usize,
    check: &dyn Fn() -> Result<()>,
    max_depth: usize,
) -> Result<()> {
    let mut parents = Stack::new(bytes.len())?;
    let mut next = Some((bytes, 0));
    loop {
        if let Some((bytes, depth)) = next.take()
            && let Some(parent) = framing(bytes, depth, remaining, check, max_depth)?
        {
            parents.push(parent)?;
        }
        loop {
            let Some(mut parent) = parents.values.pop() else {
                return Ok(());
            };
            if let Some(child) = parent.child()? {
                parents.values.push(parent);
                next = Some(child);
                break;
            }
        }
    }
}

enum Container<'m, 'v> {
    Object(VariantObject<'m, 'v>),
    List(VariantList<'m, 'v>),
}

struct Children<'m, 'v> {
    container: Container<'m, 'v>,
    next: usize,
}

const _: () = assert!(size_of::<Children<'_, '_>>() <= 80);

impl<'m, 'v> Children<'m, 'v> {
    fn child(&mut self) -> Result<Option<Variant<'m, 'v>>> {
        let index = self.next;
        let child = match &self.container {
            Container::Object(object) => {
                let Some(name) = object.field_name(index) else {
                    return Ok(None);
                };
                let child = object
                    .field(index)
                    .expect("existing field name has a value");
                // These headers/keys have already passed full validation. The
                // preceding name is a borrowed lookup, with no decoded map.
                if index > 0 && object.field_name(index - 1) == Some(name) {
                    return Err(failed("object contains duplicate field names"));
                }
                child
            }
            Container::List(list) => {
                let Some(child) = list.get(index) else {
                    return Ok(None);
                };
                child
            }
        };
        self.next += 1;
        Ok(Some(child))
    }
}

fn unique_keys(
    value: &Variant<'_, '_>,
    bytes: usize,
    check: &dyn Fn() -> Result<()>,
) -> Result<()> {
    let mut parents = Stack::new(bytes)?;
    let mut next = Some(value.clone());
    loop {
        if let Some(value) = next.take() {
            check()?;
            let container = match value {
                Variant::Object(object) => Some(Container::Object(object)),
                Variant::List(list) => Some(Container::List(list)),
                _ => None,
            };
            if let Some(container) = container {
                parents.push(Children { container, next: 0 })?;
            }
        }
        loop {
            let Some(mut parent) = parents.values.pop() else {
                return Ok(());
            };
            if let Some(child) = parent.child()? {
                parents.values.push(parent);
                next = Some(child);
                break;
            }
        }
    }
}
