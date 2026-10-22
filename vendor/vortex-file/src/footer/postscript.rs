// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-10-21: admit metadata names and locators once
// and share their owner through incremental parsing and footer handoff.

use flatbuffers::FlatBufferBuilder;
use flatbuffers::Follow;
use flatbuffers::WIPOffset;
use vortex_buffer::Alignment;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_flatbuffers::FlatBufferRoot;
use vortex_flatbuffers::ReadFlatBuffer;
use vortex_flatbuffers::WriteFlatBuffer;
use vortex_flatbuffers::footer as fb;
use vortex_session::VortexSession;

use super::MetadataSegments;
use super::SegmentSpec;

/// The postscript captures the locations and compression for the initial segments required for
/// reading a Vortex file.
pub(crate) struct Postscript {
    pub(crate) dtype: Option<PostscriptSegment>,
    pub(crate) layout: PostscriptSegment,
    pub(crate) statistics: Option<PostscriptSegment>,
    pub(crate) footer: PostscriptSegment,
    pub(crate) metadata: MetadataSegments,
}

impl FlatBufferRoot for Postscript {}

impl WriteFlatBuffer for Postscript {
    type Target<'a> = fb::Postscript<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> VortexResult<WIPOffset<Self::Target<'fb>>> {
        self.metadata.validate()?;
        self.write_flatbuffer_unchecked(fbb)
    }
}

impl Postscript {
    fn write_flatbuffer_unchecked<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> VortexResult<WIPOffset<fb::Postscript<'fb>>> {
        let dtype = self
            .dtype
            .as_ref()
            .map(|ps| ps.write_flatbuffer(fbb))
            .transpose()?;
        let layout = self.layout.write_flatbuffer(fbb)?;
        let statistics = self
            .statistics
            .as_ref()
            .map(|ps| ps.write_flatbuffer(fbb))
            .transpose()?;
        let footer = self.footer.write_flatbuffer(fbb)?;
        let metadata = if self.metadata.is_empty() {
            None
        } else {
            let mut metadata = Vec::with_capacity(self.metadata.len());
            for (key, segment) in self.metadata.iter() {
                let key = fbb.create_string(key);
                let segment = PostscriptSegment::from(segment).write_flatbuffer(fbb)?;
                metadata.push(fb::PostscriptMetadata::create(
                    fbb,
                    &fb::PostscriptMetadataArgs {
                        key: Some(key),
                        segment: Some(segment),
                    },
                ));
            }
            Some(fbb.create_vector(metadata.as_slice()))
        };
        Ok(fb::Postscript::create(
            fbb,
            &fb::PostscriptArgs {
                dtype,
                layout: Some(layout),
                statistics,
                footer: Some(footer),
                metadata,
            },
        ))
    }
}

impl Postscript {
    /// Read through the receiving allocator before constructing metadata storage.
    pub(crate) fn read_with_session(
        fb: &fb::Postscript<'_>,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        let entries = fb.metadata().unwrap_or_default();
        let metadata = MetadataSegments::try_from_fn(
            entries.len(),
            |index| entries.get(index).key(),
            |index| {
                PostscriptSegment::read_flatbuffer(&entries.get(index).segment())
                    .map(|segment| SegmentSpec::from(&segment))
            },
            session,
        )?;
        Ok(Self {
            dtype: fb
                .dtype()
                .map(|ps| PostscriptSegment::read_flatbuffer(&ps))
                .transpose()?,
            layout: PostscriptSegment::read_flatbuffer(
                &fb.layout()
                    .ok_or_else(|| vortex_err!("Postscript missing layout segment"))?,
            )?,
            statistics: fb
                .statistics()
                .map(|ps| PostscriptSegment::read_flatbuffer(&ps))
                .transpose()?,
            footer: PostscriptSegment::read_flatbuffer(
                &fb.footer()
                    .ok_or_else(|| vortex_err!("Postscript missing footer segment"))?,
            )?,
            metadata,
        })
    }
}

// Private format fixtures use the default allocator. Native file opens must
// supply their receiving session through read_with_session.
#[cfg(test)]
impl ReadFlatBuffer for Postscript {
    type Source<'a> = fb::Postscript<'a>;
    type Error = VortexError;

    fn read_flatbuffer<'buf>(
        fb: &<Self::Source<'buf> as Follow<'buf>>::Inner,
    ) -> Result<Self, Self::Error> {
        Self::read_with_session(fb, &VortexSession::empty())
    }
}

pub struct PostscriptSegment {
    pub(crate) offset: u64,
    pub(crate) length: u32,
    pub(crate) alignment: Alignment,
}

impl FlatBufferRoot for PostscriptSegment {}

impl WriteFlatBuffer for PostscriptSegment {
    type Target<'a> = fb::PostscriptSegment<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> VortexResult<WIPOffset<Self::Target<'fb>>> {
        Ok(fb::PostscriptSegment::create(
            fbb,
            &fb::PostscriptSegmentArgs {
                offset: self.offset,
                length: self.length,
                alignment_exponent: self.alignment.exponent(),
                _compression: None,
                _encryption: None,
            },
        ))
    }
}

impl ReadFlatBuffer for PostscriptSegment {
    type Source<'a> = fb::PostscriptSegment<'a>;
    type Error = VortexError;

    fn read_flatbuffer<'buf>(
        fb: &<Self::Source<'buf> as Follow<'buf>>::Inner,
    ) -> Result<Self, Self::Error> {
        Ok(PostscriptSegment {
            offset: fb.offset(),
            length: fb.length(),
            // The alignment exponent comes from the file and may be corrupt, so validate it rather
            // than panicking on a too-large shift (see issue #8819).
            alignment: Alignment::try_from_untrusted_exponent(fb.alignment_exponent())?,
        })
    }
}

#[cfg(test)]
mod tests {
    use vortex_buffer::ByteBuffer;
    use vortex_flatbuffers::FlatBuffer;
    use vortex_flatbuffers::ReadFlatBuffer;
    use vortex_flatbuffers::WriteFlatBufferExt;

    use super::*;
    use crate::MAX_POSTSCRIPT_SIZE;
    use crate::footer::MAX_METADATA_KEY_BYTES;
    use crate::footer::MAX_METADATA_SEGMENTS;

    fn metadata_entries(entries: Vec<(String, PostscriptSegment)>) -> MetadataSegments {
        MetadataSegments::from_unchecked(
            entries
                .into_iter()
                .map(|(key, segment)| (key, SegmentSpec::from(&segment)))
                .collect(),
        )
    }

    fn segment(offset: u64) -> PostscriptSegment {
        PostscriptSegment {
            offset,
            length: 1,
            alignment: Alignment::none(),
        }
    }

    fn write_postscript_bytes_unchecked(postscript: &Postscript) -> FlatBuffer {
        let mut fbb = FlatBufferBuilder::new();
        let root_offset = postscript.write_flatbuffer_unchecked(&mut fbb).unwrap();
        fbb.finish_minimal(root_offset);
        let (vec, start) = fbb.collapse();
        let end = vec.len();
        FlatBuffer::align_from(ByteBuffer::from(vec).slice(start..end))
    }

    fn read_postscript_error(postscript: &Postscript) -> String {
        let bytes = write_postscript_bytes_unchecked(postscript);
        match Postscript::read_flatbuffer_bytes(&bytes) {
            Ok(_) => panic!("expected postscript read to fail"),
            Err(err) => err.to_string(),
        }
    }

    fn write_postscript_error(postscript: &Postscript) -> String {
        match postscript.write_flatbuffer_bytes() {
            Ok(_) => panic!("expected postscript write to fail"),
            Err(err) => err.to_string(),
        }
    }

    fn key_limit_message() -> String {
        format!("at most {MAX_METADATA_KEY_BYTES} bytes")
    }

    fn segment_limit_message() -> String {
        format!("at most {MAX_METADATA_SEGMENTS} metadata segments")
    }

    #[test]
    fn metadata_limit_boundaries_roundtrip() {
        let metadata = (0..MAX_METADATA_SEGMENTS)
            .map(|idx| {
                (
                    format!("{idx:0>width$}", width = MAX_METADATA_KEY_BYTES),
                    segment(2 + idx as u64),
                )
            })
            .collect::<Vec<_>>();

        let bytes = Postscript {
            dtype: None,
            layout: segment(0),
            statistics: None,
            footer: segment(1),
            metadata: metadata_entries(metadata),
        }
        .write_flatbuffer_bytes()
        .unwrap();

        let postscript = Postscript::read_flatbuffer_bytes(&bytes).unwrap();
        assert_eq!(postscript.metadata.len(), MAX_METADATA_SEGMENTS);
        let mut total_key_bytes = 0;
        for (key, _) in postscript.metadata.iter() {
            assert_eq!(key.len(), MAX_METADATA_KEY_BYTES);
            total_key_bytes += key.len();
        }
        assert_eq!(
            total_key_bytes,
            MAX_METADATA_SEGMENTS * MAX_METADATA_KEY_BYTES
        );
        assert!(bytes.len() <= MAX_POSTSCRIPT_SIZE as usize);
    }

    #[test]
    fn duplicate_metadata_keys_are_rejected() {
        let postscript = Postscript {
            dtype: None,
            layout: segment(0),
            statistics: None,
            footer: segment(1),
            metadata: metadata_entries(vec![
                ("metadata".to_string(), segment(2)),
                ("metadata".to_string(), segment(3)),
            ]),
        };

        let read_err = read_postscript_error(&postscript);
        assert!(read_err.contains("duplicate metadata key"));
        assert!(read_err.contains(&key_limit_message()));
        assert!(read_err.contains(&segment_limit_message()));
        let write_err = write_postscript_error(&postscript);
        assert!(write_err.contains("duplicate metadata key"));
        assert!(write_err.contains(&key_limit_message()));
        assert!(write_err.contains(&segment_limit_message()));
    }

    #[test]
    fn empty_metadata_keys_are_rejected() {
        let postscript = Postscript {
            dtype: None,
            layout: segment(0),
            statistics: None,
            footer: segment(1),
            metadata: metadata_entries(vec![(String::new(), segment(2))]),
        };

        let read_err = read_postscript_error(&postscript);
        assert!(read_err.contains("metadata key must not be empty"));
        let write_err = write_postscript_error(&postscript);
        assert!(write_err.contains("metadata key must not be empty"));
    }

    #[test]
    fn long_metadata_keys_are_rejected() {
        let key = "é".repeat((MAX_METADATA_KEY_BYTES / "é".len()) + 1);
        let key_len = key.len();
        let postscript = Postscript {
            dtype: None,
            layout: segment(0),
            statistics: None,
            footer: segment(1),
            metadata: metadata_entries(vec![(key, segment(2))]),
        };

        for err in [
            read_postscript_error(&postscript),
            write_postscript_error(&postscript),
        ] {
            assert!(err.contains(&format!("{key_len} bytes")));
            assert!(err.contains(&key_limit_message()));
            assert!(err.contains(&segment_limit_message()));
        }
    }

    #[test]
    fn too_many_metadata_segments_are_rejected() {
        let postscript = Postscript {
            dtype: None,
            layout: segment(0),
            statistics: None,
            footer: segment(1),
            metadata: metadata_entries(
                (0..=MAX_METADATA_SEGMENTS)
                    .map(|idx| (format!("metadata-{idx}"), segment(2 + idx as u64)))
                    .collect(),
            ),
        };

        for err in [
            read_postscript_error(&postscript),
            write_postscript_error(&postscript),
        ] {
            assert!(err.contains(&segment_limit_message()));
            assert!(err.contains(&key_limit_message()));
        }
    }
}
