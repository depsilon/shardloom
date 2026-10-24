// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom: defer unused V2 tables and preserve source-scoped cache lifetimes.

//! This module defines the [`VortexFile`] struct, which represents a Vortex file on disk or in memory.
//!
//! The `VortexFile` provides methods for accessing file metadata, creating segment sources for reading
//! data from the file, and initiating scans to read the file's contents into memory as Vortex arrays.

use std::ops::Range;
use std::sync::Arc;
use std::sync::OnceLock;

use itertools::Itertools;
use vortex_array::ArrayRef;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldMask;
use vortex_array::expr::Expression;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_layout::LayoutReader;
use vortex_layout::scan::layout::LayoutReaderDataSource;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_layout::scan::split_by::SplitBy;
use vortex_layout::scan::v2::ScanCache;
use vortex_layout::scan::v2::ScanFile;
use vortex_layout::scan::v2::SegmentLocation;
use vortex_layout::segments::SegmentSource;
use vortex_scan::DataSourceRef;
use vortex_session::VortexSession;
use vortex_utils::aliases::hash_map::HashMap;

use crate::FileStatistics;
use crate::footer::Footer;
use crate::pruning::can_prune_file_stats;
use crate::v2::FileStatsLayoutReader;

/// Represents a Vortex file, providing access to its metadata and content.
///
/// A `VortexFile` is created by opening a Vortex file using [`VortexOpenOptions`](crate::VortexOpenOptions).
/// It provides methods for accessing file metadata (such as row count, data type, and statistics)
/// and for initiating scans to read the file's contents.
#[derive(Clone)]
pub struct VortexFile {
    /// The footer of the Vortex file, containing metadata and layout information.
    footer: Footer,
    /// The segment source used to read segments from this file.
    segment_source: Arc<dyn SegmentSource>,
    /// The Vortex session used to open this file.
    session: VortexSession,
    /// User-defined metadata values resolved for this file open.
    metadata: Arc<HashMap<String, ByteBuffer>>,
    /// None id LayoutReader caching is turned off
    layout_reader_cache: Option<OnceLock<Arc<dyn LayoutReader>>>,
    /// None if V2 scan caching is turned off; initialized only by an explicit V2 scan.
    scan_cache: Option<Arc<OnceLock<FileScanCache>>>,
}

/// What repeated V2 scans of a file share: its segment locations, read from the footer once, and
/// the plan state derived from them.
#[derive(Clone)]
struct FileScanCache {
    locations: Arc<[SegmentLocation]>,
    plans: ScanCache,
}

fn segment_locations(footer: &Footer) -> Arc<[SegmentLocation]> {
    footer
        .segment_specs_with_metadata()
        .iter()
        .map(|spec| SegmentLocation {
            offset: spec.offset,
            length: spec.length,
            alignment: spec.alignment,
        })
        .collect()
}

fn layout_reader(
    segment_source: Arc<dyn SegmentSource>,
    footer: &Footer,
    session: &VortexSession,
) -> VortexResult<Arc<dyn LayoutReader>> {
    let root_reader = footer
        .layout()
        // TODO(ngates): we may want to allow the user pass in a name here?
        .new_reader("".into(), segment_source, session, &Default::default())?;

    Ok(if let Some(stats) = footer.statistics().cloned() {
        Arc::new(FileStatsLayoutReader::new(
            root_reader,
            stats,
            session.clone(),
        ))
    } else {
        root_reader
    })
}

impl VortexFile {
    /// Creates a new `VortexFile` from the given footer, segment source, and session.
    pub fn new(
        footer: Footer,
        segment_source: Arc<dyn SegmentSource>,
        session: VortexSession,
    ) -> Self {
        Self {
            footer,
            segment_source,
            session,
            metadata: Arc::new(HashMap::new()),
            layout_reader_cache: None,
            scan_cache: None,
        }
    }

    pub(crate) fn with_metadata(mut self, metadata: Arc<HashMap<String, ByteBuffer>>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Enable layout reader and V2 scan caching.
    ///
    /// Repeated calls to [`layout_reader`](Self::layout_reader), [`scan`](Self::scan), and
    /// [`data_source`](Self::data_source) will share the same reader tree. Repeated V2 scans of
    /// [`scan_file`](Self::scan_file) will share its segment locations and a [`ScanCache`].
    /// Those V2 allocations are deferred until the first call to `scan_file`.
    pub fn with_caching(self) -> Self {
        Self {
            footer: self.footer,
            segment_source: self.segment_source,
            session: self.session,
            metadata: self.metadata,
            layout_reader_cache: Some(OnceLock::new()),
            scan_cache: Some(Arc::new(OnceLock::new())),
        }
    }

    /// Returns a reference to the file's footer, which contains metadata and layout information.
    pub fn footer(&self) -> &Footer {
        &self.footer
    }

    /// Returns the number of rows in the file.
    pub fn row_count(&self) -> u64 {
        self.footer.row_count()
    }

    /// Returns the data type of the file's contents.
    pub fn dtype(&self) -> &DType {
        self.footer.dtype()
    }

    /// Returns the file's statistics, if available.
    ///
    /// Statistics can be used for query optimization and data exploration.
    pub fn file_stats(&self) -> Option<&FileStatistics> {
        self.footer.statistics()
    }

    /// Returns the user-defined metadata segments loaded for this file.
    ///
    /// Metadata is only loaded when requested during open. Iteration order is unspecified.
    pub fn metadata_segments(&self) -> impl Iterator<Item = (&str, &ByteBuffer)> {
        self.metadata
            .iter()
            .map(|(key, metadata)| (key.as_str(), metadata))
    }

    /// Returns the loaded user-defined metadata segment for the given key.
    ///
    /// Returns `None` when the key is absent or metadata was not loaded.
    pub fn metadata_segment(&self, key: &str) -> Option<&ByteBuffer> {
        self.metadata.get(key)
    }

    /// Create a new segment source for reading from the file.
    ///
    /// This may spawn a background I/O driver that will exit when the returned segment source
    /// is dropped.
    pub fn segment_source(&self) -> Arc<dyn SegmentSource> {
        Arc::clone(&self.segment_source)
    }

    /// Replace the segment source used by this file.
    ///
    /// Any cached layout reader or V2 scan state is cleared so that subsequent scans read from
    /// the replacement source.
    pub fn with_segment_source(mut self, segment_source: Arc<dyn SegmentSource>) -> Self {
        self.segment_source = segment_source;
        if self.layout_reader_cache.is_some() {
            self.layout_reader_cache = Some(OnceLock::new());
        }
        if let Some(cache) = &mut self.scan_cache {
            // Source replacement must sever shared plan state even when this
            // handle was cloned before its first V2 scan. Immutable locations
            // may still be shared when they have already been initialized.
            *cache = Arc::new(match cache.get() {
                Some(previous) => OnceLock::from(FileScanCache {
                    locations: Arc::clone(&previous.locations),
                    plans: ScanCache::default(),
                }),
                None => OnceLock::new(),
            });
        }
        self
    }

    /// Returns a reference to the Vortex session used to open this file.
    pub fn session(&self) -> &VortexSession {
        &self.session
    }

    /// Create a new layout reader for the file.
    ///
    /// Wraps the root layout in a [`FileStatsLayoutReader`] if file stats are available.
    pub fn layout_reader(&self) -> VortexResult<Arc<dyn LayoutReader>> {
        match &self.layout_reader_cache {
            None => layout_reader(
                Arc::clone(&self.segment_source),
                &self.footer,
                &self.session,
            ),
            Some(reader) => {
                // get_or_try_init is unstable
                if let Some(val) = reader.get() {
                    Ok(Arc::clone(val))
                } else {
                    let inner = layout_reader(
                        Arc::clone(&self.segment_source),
                        &self.footer,
                        &self.session,
                    )?;
                    Ok(if let Err(val) = reader.set(Arc::clone(&inner)) {
                        val
                    } else {
                        inner
                    })
                }
            }
        }
    }

    /// Create a [`DataSource`](vortex_scan::DataSource) from this file for scanning.
    ///
    /// Wraps the file's layout reader with [`FileStatsLayoutReader`] (when file-level
    /// statistics are available) and [`LayoutReaderDataSource`].
    pub fn data_source(&self) -> VortexResult<DataSourceRef> {
        let reader = self.layout_reader()?;

        Ok(Arc::new(LayoutReaderDataSource::new(
            reader,
            self.session.clone(),
        )))
    }

    /// Initiate a scan of the file, returning a builder for projection, filtering, selection, and
    /// execution options.
    pub fn scan(&self) -> VortexResult<ScanBuilder<ArrayRef>> {
        Ok(ScanBuilder::new(
            self.session.clone(),
            self.layout_reader()?,
        ))
    }

    /// The parts of this file the planner-driven scan executor reads through: its root layout,
    /// where each of its segments lives, and its segment source.
    ///
    /// Pass this to [`vortex_layout::scan::v2::ScanBuilder::new`] to build a V2 scan explicitly.
    /// With [caching](Self::with_caching), every returned [`ScanFile`] shares one [`ScanCache`].
    pub fn scan_file(&self) -> ScanFile {
        let (locations, cache) = match &self.scan_cache {
            Some(cache) => {
                let cache = cache.get_or_init(|| FileScanCache {
                    locations: segment_locations(&self.footer),
                    plans: ScanCache::default(),
                });
                (Arc::clone(&cache.locations), Some(cache.plans.clone()))
            }
            None => (segment_locations(&self.footer), None),
        };
        ScanFile {
            layout: Arc::clone(self.footer.layout()),
            locations,
            segments: Arc::clone(&self.segment_source),
            cache,
        }
    }

    /// Returns `true` if file-level statistics prove the expression cannot
    /// match any rows in this file.
    ///
    /// Row-count-aware pruning predicates are evaluated with the file's total
    /// row count as their scope.
    pub fn can_prune(&self, filter: &Expression) -> VortexResult<bool> {
        let Some((stats, fields)) = self
            .footer
            .statistics()
            .zip(self.footer.dtype().as_struct_fields_opt())
        else {
            return Ok(false);
        };

        can_prune_file_stats(
            &filter.bind(self.footer.dtype())?,
            self.footer.row_count(),
            stats,
            fields,
            &self.session,
        )
    }

    /// Return the file's natural row splits as root-coordinate ranges.
    ///
    /// These are the ranges that the default [`SplitBy`] would use for an all-fields scan.
    pub fn splits(&self) -> VortexResult<Vec<Range<u64>>> {
        let reader = self.layout_reader()?;
        Ok(SplitBy::default()
            .splits(reader.as_ref(), &(0..reader.row_count()), &[FieldMask::All])?
            .into_iter()
            .tuple_windows()
            .map(|(start, end)| start..end)
            .collect())
    }
}

#[cfg(test)]
mod cache_lifetime_tests {
    use std::sync::Arc;

    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::array_session;
    use vortex_array::assert_arrays_eq;
    use vortex_array::stream::ArrayStreamAdapter;
    use vortex_array::stream::ArrayStreamExt;
    use vortex_buffer::Buffer;
    use vortex_buffer::ByteBufferMut;
    use vortex_error::VortexResult;
    use vortex_io::session::RuntimeSessionExt;
    use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
    use vortex_layout::scan::v2::ScanBuilder;
    use vortex_layout::session::LayoutSession;

    use super::VortexFile;
    use crate::OpenOptionsSessionExt;
    use crate::WriteOptionsSessionExt;

    async fn cached_file(start: i32) -> VortexResult<VortexFile> {
        let session = array_session().with::<LayoutSession>().with_tokio();
        crate::register_default_encodings(&session);
        crate::enable_all_registered_array_encodings(&session);
        let mut bytes = ByteBufferMut::empty();
        session
            .write_options()
            .with_strategy(Arc::new(FlatLayoutStrategy::default()))
            .with_file_statistics(Vec::new())
            .with_metadata_segment("fixture", vec![1u8, 2, 3])
            .write(
                &mut bytes,
                Buffer::from((start..start + 16).collect::<Vec<_>>())
                    .into_array()
                    .to_array_stream(),
            )
            .await?;
        session
            .open_options()
            .with_layout_reader_cache()
            .open_buffer(bytes)
    }

    async fn assert_v2_values(file: &VortexFile, start: i32) -> VortexResult<()> {
        let result = ArrayStreamAdapter::new(
            file.dtype().clone(),
            ScanBuilder::new(file.session().clone(), file.scan_file()).into_stream()?,
        )
        .read_all()
        .await?;
        assert_arrays_eq!(
            result,
            Buffer::from((start..start + 16).collect::<Vec<_>>()).into_array(),
            &mut file.session().create_execution_ctx()
        );
        Ok(())
    }

    #[tokio::test]
    async fn v1_scans_do_not_initialize_v2_tables() -> VortexResult<()> {
        let file = cached_file(0).await?;
        let cache = file.scan_cache.as_ref().unwrap();
        assert!(cache.get().is_none());
        let reader = file.layout_reader()?;
        for _ in 0..2 {
            let result = file.scan()?.into_array_stream()?.read_all().await?;
            assert_arrays_eq!(
                result,
                Buffer::from((0..16i32).collect::<Vec<_>>()).into_array(),
                &mut file.session().create_execution_ctx()
            );
            assert!(Arc::ptr_eq(&reader, &file.layout_reader()?));
            assert!(cache.get().is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn clones_share_lazily_initialized_v2_locations() -> VortexResult<()> {
        let file = cached_file(0).await?;
        let cloned = file.clone();
        assert!(Arc::ptr_eq(
            file.scan_cache.as_ref().unwrap(),
            cloned.scan_cache.as_ref().unwrap()
        ));
        assert!(file.scan_cache.as_ref().unwrap().get().is_none());
        let first = cloned.scan_file();
        let second = file.scan_file();
        assert!(Arc::ptr_eq(&first.locations, &second.locations));
        let specs = file.footer().segment_specs_with_metadata();
        assert_eq!(first.locations.len(), specs.iter().count());
        for (actual, expected) in first.locations.iter().zip(specs.iter()) {
            assert_eq!(actual.offset, expected.offset);
            assert_eq!(actual.length, expected.length);
            assert_eq!(actual.alignment, expected.alignment);
        }
        assert_v2_values(&cloned, 0).await?;
        assert_v2_values(&file, 0).await
    }

    #[tokio::test]
    async fn replacement_sources_reset_cold_and_initialized_caches() -> VortexResult<()> {
        let replacement = cached_file(100).await?;
        for initialized in [false, true] {
            let original = cached_file(0).await?;
            let reader = original.layout_reader()?;
            if initialized {
                assert_v2_values(&original, 0).await?;
            }
            let changed = original
                .clone()
                .with_segment_source(replacement.segment_source());
            assert!(!Arc::ptr_eq(
                original.scan_cache.as_ref().unwrap(),
                changed.scan_cache.as_ref().unwrap()
            ));
            assert_eq!(
                changed.scan_cache.as_ref().unwrap().get().is_some(),
                initialized
            );
            assert!(!Arc::ptr_eq(&reader, &changed.layout_reader()?));
            if initialized {
                assert!(Arc::ptr_eq(
                    &original.scan_file().locations,
                    &changed.scan_file().locations
                ));
            }
            let result = changed.scan()?.into_array_stream()?.read_all().await?;
            assert_arrays_eq!(
                result,
                Buffer::from((100..116i32).collect::<Vec<_>>()).into_array(),
                &mut changed.session().create_execution_ctx()
            );
            assert_v2_values(&changed, 100).await?;
            assert_v2_values(&original, 0).await?;
        }
        Ok(())
    }
}
