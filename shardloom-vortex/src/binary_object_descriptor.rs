//! Allocation-free validation of versioned binary-object cell descriptors.

use super::{ObjectId, error};
use shardloom_core::Result;
use std::{ops::Range, path::Path};

const MAGIC: &[u8; 5] = b"SLBO\x01";
const MANAGED: u8 = 1;
const EXTERNAL: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Descriptor<'a> {
    Managed {
        id: ObjectId,
        length: u64,
    },
    External {
        path: &'a str,
        range: Range<u64>,
        generation: [u8; 32],
    },
}

#[cfg(test)]
impl Descriptor<'_> {
    pub(crate) fn length(&self) -> u64 {
        match self {
            Self::Managed { length, .. } => *length,
            Self::External { range, .. } => range.end - range.start,
        }
    }
}

pub(crate) fn parse(bytes: &[u8]) -> Result<Descriptor<'_>> {
    let mut cursor = Cursor(bytes);
    if cursor.take(MAGIC.len())? != MAGIC {
        return Err(error(
            "DESCRIPTOR",
            "unknown binary-object descriptor version",
        ));
    }
    let tag = cursor.take(1)?[0];
    let length = cursor.u64()?;
    let value = match tag {
        MANAGED => Descriptor::Managed {
            id: cursor.take(32)?.try_into().expect("checked fixed width"),
            length,
        },
        EXTERNAL => {
            let start = cursor.u64()?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| error("RANGE", "external object range overflows"))?;
            let generation = cursor.take(32)?.try_into().expect("checked fixed width");
            let count = u32::from_le_bytes(cursor.take(4)?.try_into().expect("checked fixed width"))
                as usize;
            let path = std::str::from_utf8(cursor.take(count)?)
                .map_err(|_| error("DESCRIPTOR", "external object path must be UTF-8"))?;
            if path.is_empty() || path.as_bytes().contains(&0) || !Path::new(path).is_absolute() {
                return Err(error(
                    "DESCRIPTOR",
                    "external object requires an absolute local path",
                ));
            }
            Descriptor::External {
                path,
                range: start..end,
                generation,
            }
        }
        _ => return Err(error("DESCRIPTOR", "unknown binary-object ownership tag")),
    };
    if !cursor.0.is_empty() {
        return Err(error(
            "DESCRIPTOR",
            "binary-object descriptor has trailing bytes",
        ));
    }
    Ok(value)
}

pub(crate) fn managed(id: &ObjectId, length: u64) -> [u8; 46] {
    let mut bytes = [0; 46];
    bytes[..5].copy_from_slice(MAGIC);
    bytes[5] = MANAGED;
    bytes[6..14].copy_from_slice(&length.to_le_bytes());
    bytes[14..].copy_from_slice(id);
    bytes
}

pub(crate) fn external(
    path: &str,
    range: Range<u64>,
    generation: &[u8; 32],
    bytes: &mut Vec<u8>,
) -> Result<()> {
    if range.start > range.end || !Path::new(path).is_absolute() || path.as_bytes().contains(&0) {
        return Err(error("DESCRIPTOR", "invalid external object path or range"));
    }
    let count = u32::try_from(path.len()).map_err(|_| {
        error(
            "DESCRIPTOR",
            "external object path exceeds descriptor width",
        )
    })?;
    bytes.extend_from_slice(MAGIC);
    bytes.push(EXTERNAL);
    bytes.extend_from_slice(&(range.end - range.start).to_le_bytes());
    bytes.extend_from_slice(&range.start.to_le_bytes());
    bytes.extend_from_slice(generation);
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(path.as_bytes());
    Ok(())
}

struct Cursor<'a>(&'a [u8]);
impl<'a> Cursor<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let (head, rest) = self
            .0
            .split_at_checked(count)
            .ok_or_else(|| error("DESCRIPTOR", "truncated binary-object descriptor"))?;
        self.0 = rest;
        Ok(head)
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("checked fixed width"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_descriptor_checks_every_prefix_version_and_trailing_bytes() {
        let encoded = managed(&[7; 32], 65_537);
        assert_eq!(
            parse(&encoded).unwrap(),
            Descriptor::Managed {
                id: [7; 32],
                length: 65_537
            }
        );
        for length in 0..encoded.len() {
            assert!(parse(&encoded[..length]).is_err(), "prefix {length}");
        }
        let mut invalid = encoded.to_vec();
        invalid.push(0);
        assert!(parse(&invalid).is_err());
        invalid.truncate(encoded.len());
        invalid[4] = 2;
        assert!(parse(&invalid).is_err());
        invalid[4] = 1;
        invalid[5] = 0;
        assert!(parse(&invalid).is_err());
    }

    #[test]
    fn external_descriptor_checks_bounds_paths_and_exact_length() {
        let mut encoded = Vec::new();
        external("/tmp/文字.bin", 3..19, &[9; 32], &mut encoded).unwrap();
        assert_eq!(
            parse(&encoded).unwrap(),
            Descriptor::External {
                path: "/tmp/文字.bin",
                range: 3..19,
                generation: [9; 32],
            }
        );
        for length in 0..encoded.len() {
            assert!(parse(&encoded[..length]).is_err(), "prefix {length}");
        }
        let mut invalid = encoded.clone();
        invalid[6..14].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(parse(&invalid).is_err());
        invalid = encoded.clone();
        invalid[58] = b'.';
        assert!(parse(&invalid).is_err());
        invalid = encoded.clone();
        invalid[58] = 0;
        assert!(parse(&invalid).is_err());
        encoded.push(0);
        assert!(parse(&encoded).is_err());
        assert!(external("relative", 0..0, &[0; 32], &mut Vec::new()).is_err());
    }
}
