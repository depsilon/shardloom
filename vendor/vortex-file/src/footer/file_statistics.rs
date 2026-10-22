// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-10-21: admit shared file-statistics tables before
// construction and retain their credit through the final owner.

//! File statistics used for native metadata answers and conservative pruning.

use std::alloc::Layout as AllocationLayout;
use std::sync::Arc;

use flatbuffers::FlatBufferBuilder;
use flatbuffers::WIPOffset;
use vortex_array::dtype::DType;
use vortex_array::memory::HostMemoryReservation;
use vortex_array::memory::MemorySessionExt;
use vortex_array::stats::StatsSet;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_flatbuffers::FlatBufferRoot;
use vortex_flatbuffers::WriteFlatBuffer;
use vortex_flatbuffers::footer as fb;
use vortex_session::VortexSession;

/// Statistics and aligned field types for a native file.
///
/// Clones retain both tables without copying their entries. Parsed and fallibly
/// constructed tables retain allocator admission through their final owner.
/// Borrowed accessors do not export an unguarded shared allocation. Independently
/// copying a slice or cloning an entry creates caller-owned data; nested schema,
/// scalar payloads and statistics overflow storage have separate owners.
#[derive(Clone, Debug)]
pub struct FileStatistics {
    storage: FileStatisticsStorage,
}

#[derive(Clone, Debug)]
enum FileStatisticsStorage {
    Empty,
    CallerOwned {
        stats: Arc<[StatsSet]>,
        dtypes: Arc<[DType]>,
    },
    Admitted(Arc<OwnedFileStatistics>),
}

#[derive(Debug)]
struct OwnedFileStatistics {
    stats: Vec<StatsSet>,
    dtypes: Vec<DType>,
    // Release both vectors before their credit, including on the final clone.
    _reservation: HostMemoryReservation,
}

impl FileStatistics {
    /// Keep existing caller-owned tables without retroactive allocator admission.
    ///
    /// # Panics
    ///
    /// Panics if `stats` and `dtypes` have different lengths.
    pub fn new(stats: Arc<[StatsSet]>, dtypes: Arc<[DType]>) -> Self {
        assert_eq!(
            stats.len(),
            dtypes.len(),
            "stats and dtypes must have the same length"
        );
        Self {
            storage: FileStatisticsStorage::CallerOwned { stats, dtypes },
        }
    }

    /// Construct caller-owned type references for existing caller-owned statistics.
    ///
    /// Native producers with a session use [`Self::try_from_fn`] to admit table
    /// capacity before producing entries. This compatibility constructor does
    /// not claim admission for its type table or the supplied statistics.
    ///
    /// # Panics
    ///
    /// Panics unless `stats` has one entry per struct field, or one for a
    /// non-struct file type.
    pub fn new_with_dtype(stats: Arc<[StatsSet]>, file_dtype: &DType) -> Self {
        let dtypes = if let DType::Struct(fields, _) = file_dtype {
            assert_eq!(
                stats.len(),
                fields.nfields(),
                "stats length must match number of struct fields"
            );
            fields.fields().collect()
        } else {
            assert_eq!(
                stats.len(),
                1,
                "non-struct dtype must have exactly 1 statistic"
            );
            Arc::from([file_dtype.clone()])
        };
        Self::new(stats, dtypes)
    }

    /// Admit both table capacities, then produce entries in field-index order.
    ///
    /// Count overflow and allocation denial occur before `field` is invoked.
    /// Partial producer failure releases constructed entries and credit. Empty
    /// tables have no payload allocation and invoke neither producer nor allocator.
    /// The charge covers table capacity, not separately allocated entry payloads.
    pub fn try_from_fn(
        count: usize,
        mut field: impl FnMut(usize) -> VortexResult<(StatsSet, DType)>,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        let stats_layout = AllocationLayout::array::<StatsSet>(count)
            .map_err(|_| vortex_err!("File statistics allocation size overflow"))?;
        let dtypes_layout = AllocationLayout::array::<DType>(count)
            .map_err(|_| vortex_err!("File statistics allocation size overflow"))?;
        let bytes = stats_layout
            .size()
            .checked_add(dtypes_layout.size())
            .ok_or_else(|| vortex_err!("File statistics allocation size overflow"))?;
        if count == 0 {
            return Ok(Self {
                storage: FileStatisticsStorage::Empty,
            });
        }
        let reservation = session.allocator().reserve_external(bytes)?;
        let mut stats = Vec::with_capacity(count);
        let mut dtypes = Vec::with_capacity(count);
        for index in 0..count {
            let (set, dtype) = field(index)?;
            stats.push(set);
            dtypes.push(dtype);
        }
        debug_assert_eq!(stats.capacity(), count);
        debug_assert_eq!(dtypes.capacity(), count);
        Ok(Self {
            storage: FileStatisticsStorage::Admitted(Arc::new(OwnedFileStatistics {
                stats,
                dtypes,
                _reservation: reservation,
            })),
        })
    }

    /// Parse admitted tables directly from the native FlatBuffer field vector.
    ///
    /// Cardinality must match the struct fields, or one for a non-struct file.
    /// Validate it before admission or entry parsing. The serialized input and
    /// independently allocated scalar/schema payloads have separate ownership.
    pub fn from_flatbuffer<'a>(
        fb: &fb::FileStatistics<'a>,
        file_dtype: &DType,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        let field_stats = fb.field_stats().unwrap_or_default();
        let expected = file_dtype
            .as_struct_fields_opt()
            .map_or(1, |fields| fields.nfields());
        vortex_ensure_eq!(field_stats.len(), expected);
        Self::try_from_fn(
            expected,
            |index| {
                let dtype = match file_dtype {
                    DType::Struct(fields, _) => fields.field_by_index(index).ok_or_else(|| {
                        vortex_err!("Missing file statistics field type at {index}")
                    })?,
                    dtype => dtype.clone(),
                };
                let stats = StatsSet::from_flatbuffer(&field_stats.get(index), &dtype, session)?;
                Ok((stats, dtype))
            },
            session,
        )
    }

    /// Borrow the statistics table. Clone `FileStatistics` for independent retention.
    pub fn stats_sets(&self) -> &[StatsSet] {
        match &self.storage {
            FileStatisticsStorage::Empty => &[],
            FileStatisticsStorage::CallerOwned { stats, .. } => stats,
            FileStatisticsStorage::Admitted(owner) => &owner.stats,
        }
    }

    /// Borrow the field-type table. Clone `FileStatistics` for independent retention.
    pub fn dtypes(&self) -> &[DType] {
        match &self.storage {
            FileStatisticsStorage::Empty => &[],
            FileStatisticsStorage::CallerOwned { dtypes, .. } => dtypes,
            FileStatisticsStorage::Admitted(owner) => &owner.dtypes,
        }
    }

    /// Returns aligned statistics and type references for a field.
    ///
    /// # Panics
    ///
    /// Panics if `field_idx` is out of bounds.
    pub fn get(&self, field_idx: usize) -> (&StatsSet, &DType) {
        (&self.stats_sets()[field_idx], &self.dtypes()[field_idx])
    }
}

impl<'a> IntoIterator for &'a FileStatistics {
    type Item = (&'a StatsSet, &'a DType);
    type IntoIter = std::iter::Zip<std::slice::Iter<'a, StatsSet>, std::slice::Iter<'a, DType>>;

    fn into_iter(self) -> Self::IntoIter {
        self.stats_sets().iter().zip(self.dtypes().iter())
    }
}

impl FlatBufferRoot for FileStatistics {}

impl WriteFlatBuffer for FileStatistics {
    type Target<'a> = fb::FileStatistics<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> VortexResult<WIPOffset<Self::Target<'fb>>> {
        let field_stats = self
            .stats_sets()
            .iter()
            .map(|s| s.write_flatbuffer(fbb))
            .collect::<VortexResult<Vec<_>>>()?;
        let field_stats = fbb.create_vector(field_stats.as_slice());
        Ok(fb::FileStatistics::create(
            fbb,
            &fb::FileStatisticsArgs {
                field_stats: Some(field_stats),
            },
        ))
    }
}
