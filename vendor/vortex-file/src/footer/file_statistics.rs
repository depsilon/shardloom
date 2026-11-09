// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-11-08: admit shared statistics tables before
// construction, retain their final owner, and preserve lossless legacy slots.

//! This module defines the file statistics component of the Vortex file footer.
//!
//! File statistics provide metadata about the data in the file, such as min/max values,
//! null counts, and other statistical information that can be used for query optimization
//! and data exploration.
use std::hash::Hash;
use std::sync::Arc;

use flatbuffers::FlatBufferBuilder;
use flatbuffers::WIPOffset;
use prost::encoding::decode_varint;
use prost::encoding::encode_varint;
use vortex_array::aggregate_fn::AggregateFnId;
use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::aggregate_fn::session::AggregateFnSessionExt;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldPath;
use vortex_array::expr::stats::Stat;
use vortex_array::flatbuffers::FlatBufferRoot;
use vortex_array::flatbuffers::WriteFlatBuffer;
use vortex_array::memory::HostMemoryReservation;
use vortex_array::scalar::Scalar;
use vortex_array::scalar::ScalarValue;
use vortex_array::stats::StatsSet;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_layout::layouts::file_stats::AggregateStat;
use vortex_layout::layouts::file_stats::AggregateStats;
use vortex_layout::layouts::file_stats::FileStatsAccumulator;
use vortex_layout::layouts::file_stats::postorder_stats_layout;
use vortex_session::VortexSession;
use vortex_utils::aliases::hash_map::HashMap;

use crate::flatbuffers::footer as fb;

#[path = "file_statistics_storage.rs"]
mod storage;
use storage::OwnedStatistics;
use storage::StatisticsStorage;
use storage::layout_shape;
use storage::reserve_parsed_tables;
use storage::reserve_tables;

/// Contains statistical information about the data in a Vortex file.
///
/// Each entry holds the aggregates recorded for a field of the file. These statistics can be used
/// for query optimization and data exploration.
#[derive(Clone, Debug)]
pub struct FileStatistics {
    storage: StatisticsStorage,
    /// Whether serialization writes the nested layout (`entry_sets` and `partials`) alongside the
    /// legacy `field_stats`.
    write_nested: bool,
}

impl FileStatistics {
    fn admitted(
        aggregates: Vec<AggregateStats>,
        dtypes: Vec<DType>,
        paths: Vec<FieldPath>,
        legacy: Vec<StatsSet>,
        reservation: Option<HostMemoryReservation>,
        write_nested: bool,
    ) -> Self {
        Self {
            storage: StatisticsStorage::Admitted(Arc::new(OwnedStatistics {
                aggregates,
                dtypes,
                paths,
                legacy,
                _reservation: reservation,
            })),
            write_nested,
        }
    }

    /// Keep caller-owned legacy tables, including slots without an aggregate
    /// equivalent. This constructor does not claim native allocator admission.
    /// The tables have no field names; use `from_stats_with_dtype` for path lookup.
    pub fn from_stats(stats: Arc<[StatsSet]>, dtypes: Arc<[DType]>) -> Self {
        assert_eq!(
            stats.len(),
            dtypes.len(),
            "stats and dtypes must have the same length"
        );
        let aggregates = stats
            .iter()
            .zip(dtypes.iter())
            .map(|(stats, dtype)| {
                aggregates_from_stats(stats, dtype)
                    .vortex_expect("caller statistics must match their field types")
            })
            .collect();
        Self {
            storage: StatisticsStorage::CallerOwned {
                aggregates,
                dtypes,
                paths: Arc::from([]),
                legacy: stats,
            },
            write_nested: false,
        }
    }

    /// Caller-owned legacy tables with the top-level field paths of a file type.
    pub fn from_stats_with_dtype(stats: Arc<[StatsSet]>, file_dtype: &DType) -> Self {
        let count = legacy_count(file_dtype);
        assert_eq!(
            stats.len(),
            count,
            "legacy statistics length must match file fields"
        );
        let mut result = Self::from_stats(
            stats,
            (0..count)
                .map(|index| {
                    legacy_dtype(file_dtype, index).vortex_expect("checked legacy field index")
                })
                .collect(),
        );
        if let StatisticsStorage::CallerOwned { paths, .. } = &mut result.storage {
            *paths = (0..count)
                .map(|index| {
                    legacy_path(file_dtype, index).vortex_expect("checked legacy field index")
                })
                .collect();
        }
        result
    }

    /// Admit all table capacities before producing any legacy entry. Count
    /// overflow and admission denial precede `field`; partial failure releases
    /// tables and credit. Empty tables need no allocator support. Scalar/schema
    /// payloads and independently copied entries retain their separate contracts.
    pub fn try_from_fn(
        count: usize,
        mut field: impl FnMut(usize) -> VortexResult<(StatsSet, DType)>,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        let reservation = reserve_tables(count, count, Stat::all().count(), count, 0, session)?;
        let mut aggregates = Vec::with_capacity(count);
        let mut dtypes = Vec::with_capacity(count);
        let mut legacy = Vec::with_capacity(count);
        for index in 0..count {
            let (stats, dtype) = field(index)?;
            aggregates.push(aggregates_from_stats(&stats, &dtype)?);
            dtypes.push(dtype);
            legacy.push(stats);
        }
        Ok(Self::admitted(
            aggregates,
            dtypes,
            Vec::new(),
            legacy,
            reservation,
            false,
        ))
    }

    /// Snapshot native running statistics only after their table capacities are
    /// admitted. The accumulator uses one bounded tree; no snapshot or path
    /// table is allocated before this constructor's reservation succeeds.
    pub fn try_from_accumulator(
        accumulator: &FileStatsAccumulator,
        file_dtype: &DType,
        write_nested: bool,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        let legacy_count = legacy_count(file_dtype);
        let (count, path_parts) = if write_nested {
            layout_shape(file_dtype)?
        } else {
            (
                legacy_count,
                if file_dtype.is_struct() {
                    legacy_count
                } else {
                    0
                },
            )
        };
        let temporary = if write_nested {
            let legacy_capacity = storage::collection_capacity(legacy_count)?;
            let aggregate_capacity =
                storage::collection_capacity(accumulator.max_aggregate_count())?;
            // The legacy snapshot coexists with the nested one only on this
            // writer path. Legacy-only writes move it into the final owner;
            // readers never construct this extra aggregate table.
            let legacy_slots = legacy_capacity
                .checked_mul(aggregate_capacity)
                .ok_or_else(|| vortex_err!("File statistics allocation size overflow"))?;
            let legacy_tables = checked_table_bytes::<AggregateStats>(legacy_capacity)?;
            let legacy_tables = checked_table_add(legacy_tables, legacy_tables)?;
            let legacy_payload = checked_table_bytes::<AggregateStat>(legacy_slots)?;
            let legacy_growth = if legacy_count == 0 {
                0
            } else {
                checked_table_bytes::<AggregateStat>(aggregate_capacity)?
            };
            checked_table_add(
                checked_table_add(
                    checked_table_add(legacy_tables, legacy_payload)?,
                    legacy_growth,
                )?,
                // The post-order layout grows separately from the final
                // path/type tables; its old and new capacities can overlap.
                checked_table_bytes::<(FieldPath, DType)>(checked_table_add(
                    storage::collection_capacity(count)?,
                    storage::collection_capacity(count)?,
                )?)?,
            )?
        } else {
            checked_table_bytes::<&DType>(legacy_count)?
        };
        let reservation = reserve_tables(
            count,
            legacy_count,
            accumulator.max_aggregate_count(),
            path_parts,
            temporary,
            session,
        )?;
        let legacy_aggregates = accumulator.legacy_aggregate_stats()?;
        vortex_ensure_eq!(legacy_aggregates.len(), legacy_count);
        let legacy = legacy_aggregates
            .iter()
            .map(AggregateStats::to_stats_set)
            .collect();
        let (aggregates, dtypes, paths) = if write_nested {
            let aggregates = accumulator.aggregate_stats()?;
            vortex_ensure_eq!(aggregates.len(), count);
            let layout = if count == 0 {
                Vec::new()
            } else {
                postorder_stats_layout(file_dtype)
            };
            vortex_ensure_eq!(layout.len(), count);
            let (paths, dtypes) = layout.into_iter().unzip();
            (aggregates, dtypes, paths)
        } else {
            let dtypes = (0..count)
                .map(|index| legacy_dtype(file_dtype, index))
                .collect::<VortexResult<Vec<_>>>()?;
            let paths = (0..count)
                .map(|index| legacy_path(file_dtype, index))
                .collect::<VortexResult<Vec<_>>>()?;
            (legacy_aggregates, dtypes, paths)
        };
        Ok(Self::admitted(
            aggregates,
            dtypes,
            paths,
            legacy,
            reservation,
            write_nested,
        ))
    }

    /// Assemble caller-owned tables. Native parsing and production use the
    /// admitted constructors instead. Borrowed accessors never detach table credit.
    fn from_parts(
        aggregates: Arc<[AggregateStats]>,
        dtypes: Arc<[DType]>,
        paths: Arc<[FieldPath]>,
        legacy_aggregates: Arc<[AggregateStats]>,
        write_nested: bool,
    ) -> Self {
        Self {
            storage: StatisticsStorage::CallerOwned {
                aggregates,
                dtypes,
                paths,
                legacy: legacy_aggregates
                    .iter()
                    .map(AggregateStats::to_stats_set)
                    .collect(),
            },
            write_nested,
        }
    }

    /// Creates a new [`FileStatistics`] from the given aggregates, data types, and field paths.
    ///
    /// # Panics
    ///
    /// Panics if `aggregates`, `dtypes`, and `paths` have different lengths.
    pub fn new(
        aggregates: Arc<[AggregateStats]>,
        dtypes: Arc<[DType]>,
        paths: Arc<[FieldPath]>,
    ) -> Self {
        assert_eq!(
            aggregates.len(),
            dtypes.len(),
            "aggregates and dtypes must have the same length"
        );
        assert_eq!(
            aggregates.len(),
            paths.len(),
            "aggregates and paths must have the same length"
        );
        Self::from_parts(aggregates, dtypes, paths, Arc::new([]), true)
    }

    /// Creates a new [`FileStatistics`] from the given nested aggregates, legacy top-level-only
    /// aggregates, and file dtype.
    ///
    /// `aggregates` must follow the post-order nested-struct layout produced by
    /// [`postorder_stats_layout`] for `file_dtype`. `legacy_aggregates` must have one entry per
    /// top-level struct field (or a single entry for a non-struct root dtype); they are written
    /// in the legacy `ArrayStats` format for readers that predate nested field statistics.
    ///
    /// # Panics
    ///
    /// Panics if the number of entries doesn't match the expected number based on the dtype.
    pub fn new_with_dtype(
        aggregates: Arc<[AggregateStats]>,
        legacy_aggregates: Arc<[AggregateStats]>,
        file_dtype: &DType,
    ) -> Self {
        let layout = postorder_stats_layout(file_dtype);
        assert_eq!(
            aggregates.len(),
            layout.len(),
            "aggregates length must match the post-order stats layout for the file dtype"
        );

        let (paths, dtypes): (Vec<FieldPath>, Vec<DType>) = layout.into_iter().unzip();
        Self::from_parts(
            aggregates,
            dtypes.into(),
            paths.into(),
            legacy_aggregates,
            true,
        )
    }

    /// Creates a new [`FileStatistics`] that only serializes the legacy top-level-only layout
    /// (`field_stats`), omitting the nested layout.
    ///
    /// `legacy_aggregates` must have one entry per top-level struct field (or a single entry for a
    /// non-struct root dtype). The entries are exposed the way a reader sees them: keyed by the
    /// top-level field paths.
    ///
    /// # Panics
    ///
    /// Panics if the number of entries doesn't match the number of top-level fields.
    pub fn new_legacy_only(legacy_aggregates: Arc<[AggregateStats]>, file_dtype: &DType) -> Self {
        let (dtypes, paths): (Vec<DType>, Vec<FieldPath>) = match file_dtype.as_struct_fields_opt()
        {
            Some(struct_fields) => struct_fields
                .fields()
                .zip(struct_fields.names().iter())
                .map(|(dtype, name)| (dtype, FieldPath::from_name(name.clone())))
                .unzip(),
            None => (vec![file_dtype.clone()], vec![FieldPath::root()]),
        };
        assert_eq!(
            legacy_aggregates.len(),
            dtypes.len(),
            "legacy aggregates length must match the number of top-level fields"
        );
        Self::from_parts(
            Arc::clone(&legacy_aggregates),
            dtypes.into(),
            paths.into(),
            legacy_aggregates,
            false,
        )
    }

    /// Creates [`FileStatistics`] from a flatbuffers [`fb::FileStatistics<'a>`].
    ///
    /// Aggregates the session doesn't know are skipped: file statistics are optional, so a reader
    /// without an aggregate loses only what that aggregate could tell it.
    pub fn from_flatbuffer<'a>(
        fb: &fb::FileStatistics<'a>,
        file_dtype: &DType,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        let legacy_fields = fb.field_stats().unwrap_or_default();
        let expected_legacy = legacy_count(file_dtype);
        if let Some(entry_sets) = fb.entry_sets() {
            // Validate shape before requesting memory or parsing any scalar.
            let (count, path_parts) = layout_shape(file_dtype)?;
            vortex_ensure_eq!(
                entry_sets.len(),
                count,
                "Nested file statistics cardinality does not match file fields"
            );
            vortex_ensure!(
                legacy_fields.is_empty() || legacy_fields.len() == expected_legacy,
                "Legacy file statistics cardinality does not match file fields"
            );
            let specs = fb.aggregate_specs().unwrap_or_default();
            let sets = fb.aggregate_sets().unwrap_or_default();
            let mut aggregate_slots = 0;
            for set in entry_sets.iter() {
                vortex_ensure!(
                    usize::from(set) < sets.len(),
                    "aggregate set index {set} out of range for {} sets",
                    sets.len()
                );
                aggregate_slots =
                    checked_table_add(aggregate_slots, sets.get(usize::from(set)).specs().len())?;
            }
            let mut temporary = checked_table_bytes::<Option<AggregateFnRef>>(specs.len())?;
            temporary = checked_table_add(temporary, checked_table_bytes::<Vec<u16>>(sets.len())?)?;
            temporary = checked_table_add(
                temporary,
                checked_table_bytes::<(FieldPath, DType)>(checked_table_add(
                    storage::collection_capacity(count)?,
                    storage::collection_capacity(count)?,
                )?)?,
            )?;
            for set in sets.iter() {
                temporary =
                    checked_table_add(temporary, checked_table_bytes::<u16>(set.specs().len())?)?;
            }
            let reservation = reserve_parsed_tables(
                count,
                legacy_fields.len(),
                aggregate_slots,
                path_parts,
                temporary,
                session,
            )?;

            let mut aggregate_fns = Vec::with_capacity(specs.len());
            for spec in specs.iter() {
                aggregate_fns.push(aggregate_fn_from_spec(&spec, session)?);
            }
            let mut aggregate_sets = Vec::with_capacity(sets.len());
            for set in sets.iter() {
                let mut indices = Vec::with_capacity(set.specs().len());
                for spec in set.specs().iter() {
                    vortex_ensure!(
                        usize::from(spec) < aggregate_fns.len(),
                        "aggregate spec index {spec} out of range for {} specs",
                        aggregate_fns.len()
                    );
                    indices.push(spec);
                }
                aggregate_sets.push(indices);
            }
            let layout = if count == 0 {
                Vec::new()
            } else {
                postorder_stats_layout(file_dtype)
            };
            vortex_ensure_eq!(layout.len(), count);
            let mut partials =
                PartialsReader::new(fb.partials().map(|p| p.bytes()).unwrap_or_default());
            let mut aggregates = Vec::with_capacity(count);
            let mut dtypes = Vec::with_capacity(count);
            let mut paths = Vec::with_capacity(count);
            for (set, (path, dtype)) in entry_sets.iter().zip(layout) {
                let set = aggregate_sets.get(usize::from(set)).ok_or_else(|| {
                    vortex_err!(
                        "aggregate set index {set} out of range for {} sets",
                        aggregate_sets.len()
                    )
                })?;
                aggregates.push(aggregate_stats_from_partials(
                    set,
                    &aggregate_fns,
                    &mut partials,
                    &dtype,
                    session,
                )?);
                dtypes.push(dtype);
                paths.push(path);
            }
            partials.finish()?;
            let mut legacy = Vec::with_capacity(legacy_fields.len());
            for (index, field) in legacy_fields.iter().enumerate() {
                legacy.push(StatsSet::from_flatbuffer(
                    &field,
                    &legacy_dtype(file_dtype, index)?,
                    session,
                )?);
            }
            return Ok(Self::admitted(
                aggregates,
                dtypes,
                paths,
                legacy,
                reservation,
                true,
            ));
        }

        vortex_ensure_eq!(
            legacy_fields.len(),
            expected_legacy,
            "Legacy file statistics cardinality does not match file fields"
        );
        let aggregate_slots = legacy_fields.iter().try_fold(0, |total, field| {
            // StatsSet decoding may omit unsupported or null scalar slots.
            // Counting every present wire slot is a conservative upper bound
            // for aggregates_from_stats' exact stats.len() allocation.
            let slots = [
                field.is_constant().is_some(),
                field.is_sorted().is_some(),
                field.is_strict_sorted().is_some(),
                field.min().is_some(),
                field.max().is_some(),
                field.null_count().is_some(),
                field.uncompressed_size_in_bytes().is_some(),
                field.sum().is_some(),
                field.nan_count().is_some(),
            ]
            .into_iter()
            .filter(|present| *present)
            .count();
            checked_table_add(total, slots)
        })?;
        let reservation = reserve_parsed_tables(
            expected_legacy,
            expected_legacy,
            aggregate_slots,
            if file_dtype.is_struct() {
                expected_legacy
            } else {
                0
            },
            0,
            session,
        )?;
        let mut aggregates = Vec::with_capacity(expected_legacy);
        let mut dtypes = Vec::with_capacity(expected_legacy);
        let mut paths = Vec::with_capacity(expected_legacy);
        let mut legacy = Vec::with_capacity(expected_legacy);
        for (index, field) in legacy_fields.iter().enumerate() {
            let dtype = legacy_dtype(file_dtype, index)?;
            let stats = StatsSet::from_flatbuffer(&field, &dtype, session)?;
            aggregates.push(aggregates_from_stats(&stats, &dtype)?);
            dtypes.push(dtype);
            paths.push(legacy_path(file_dtype, index)?);
            legacy.push(stats);
        }
        Ok(Self::admitted(
            aggregates,
            dtypes,
            paths,
            legacy,
            reservation,
            false,
        ))
    }

    /// Returns a reference to the aggregates recorded for each entry.
    pub fn aggregates(&self) -> &[AggregateStats] {
        self.storage.aggregates()
    }

    /// Returns `true` if there is no statistical information at all, in either the nested or the
    /// legacy layout.
    ///
    /// These can disagree: a non-nullable struct field whose entire subtree is unsupported dtypes
    /// (e.g. all-`Variant`) contributes no entries to the nested post-order layout (every leaf is
    /// skipped, and there's no nullable struct along the way to emit an own entry), but still gets
    /// a legacy top-level entry with a real `NullCount`, since that stat is dtype-agnostic.
    pub fn is_empty(&self) -> bool {
        self.aggregates().is_empty() && self.stats_sets().is_empty()
    }

    /// Returns a reference to the data types.
    pub fn dtypes(&self) -> &[DType] {
        self.storage.dtypes()
    }

    /// Returns a reference to the field paths.
    pub fn paths(&self) -> &[FieldPath] {
        self.storage.paths()
    }

    /// Returns the aggregates and data type for a specific field.
    ///
    /// # Panics
    ///
    /// Panics if `field_idx` is out of bounds.
    pub fn get(&self, field_idx: usize) -> (&AggregateStats, &DType) {
        (&self.aggregates()[field_idx], &self.dtypes()[field_idx])
    }

    /// Returns the aggregates and data type for the field at the given path, if present.
    pub fn get_by_path(&self, path: &FieldPath) -> Option<(&AggregateStats, &DType)> {
        self.paths()
            .iter()
            .position(|entry| entry == path)
            .map(|idx| self.get(idx))
    }

    /// Borrow lossless top-level legacy statistics. Nested aggregate metadata
    /// does not replace or reinterpret the slots used by existing native readers.
    pub fn stats_sets(&self) -> &[StatsSet] {
        self.storage.legacy()
    }
}

fn legacy_count(dtype: &DType) -> usize {
    dtype
        .as_struct_fields_opt()
        .map_or(1, |fields| fields.nfields())
}

fn legacy_dtype(dtype: &DType, index: usize) -> VortexResult<DType> {
    match dtype {
        DType::Struct(fields, _) => fields
            .try_field_by_index(index)?
            .ok_or_else(|| vortex_err!("Missing file statistics field type at {index}")),
        dtype if index == 0 => Ok(dtype.clone()),
        _ => vortex_bail!("Missing file statistics field type at {index}"),
    }
}

fn legacy_path(dtype: &DType, index: usize) -> VortexResult<FieldPath> {
    match dtype {
        DType::Struct(fields, _) => fields
            .field_name(index)
            .map(|name| FieldPath::from_name(name.clone()))
            .ok_or_else(|| vortex_err!("Missing file statistics field name at {index}")),
        _ if index == 0 => Ok(FieldPath::root()),
        _ => vortex_bail!("Missing file statistics field name at {index}"),
    }
}

fn checked_table_bytes<T>(count: usize) -> VortexResult<usize> {
    std::alloc::Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| vortex_err!("File statistics allocation size overflow"))
}

fn checked_table_add(left: usize, right: usize) -> VortexResult<usize> {
    left.checked_add(right)
        .filter(|total| *total <= isize::MAX as usize)
        .ok_or_else(|| vortex_err!("File statistics allocation size overflow"))
}

/// Convert supported legacy slots without discarding a malformed typed value.
/// Slots without an aggregate equivalent remain in the separate legacy table.
fn aggregates_from_stats(stats: &StatsSet, dtype: &DType) -> VortexResult<AggregateStats> {
    let mut aggregates = Vec::with_capacity(stats.len());
    for (stat, value) in stats.iter() {
        let (Some(aggregate_fn), Some(stat_dtype)) = (stat.aggregate_fn(), stat.dtype(dtype))
        else {
            continue;
        };
        let value = value
            .clone()
            .map(|value| Scalar::try_new(stat_dtype.clone(), Some(value)))
            .transpose()?;
        aggregates.push(AggregateStat::from_value(aggregate_fn, value));
    }
    Ok(AggregateStats::new(aggregates))
}

/// Resolves an [`fb::AggregateSpec`]'s aggregate function, or `None` if the session doesn't
/// know it.
fn aggregate_fn_from_spec(
    spec: &fb::AggregateSpec<'_>,
    session: &VortexSession,
) -> VortexResult<Option<AggregateFnRef>> {
    #[expect(clippy::disallowed_methods, reason = "interning a dynamic id")]
    let id = AggregateFnId::new(spec.id());
    let Some(plugin) = session.aggregate_fns().find_plugin(&id) else {
        return Ok(None);
    };
    let options = spec
        .options()
        .map(|options| options.bytes())
        .unwrap_or_default();
    let aggregate_fn = plugin.deserialize(options, session)?;
    if aggregate_fn.id() != id {
        vortex_bail!(
            "Aggregate function ID mismatch: expected {}, got {}",
            id,
            aggregate_fn.id()
        );
    }
    Ok(Some(aggregate_fn))
}

/// Reads one entry's partial states, one per spec in `set`, from `partials`.
///
/// `set`'s spec indices must already be checked against `aggregate_fns`.
fn aggregate_stats_from_partials(
    set: &[u16],
    aggregate_fns: &[Option<AggregateFnRef>],
    partials: &mut PartialsReader<'_>,
    dtype: &DType,
    session: &VortexSession,
) -> VortexResult<AggregateStats> {
    let mut aggregates = Vec::with_capacity(set.len());
    for &spec in set {
        // Read the partial even for an unknown aggregate, to stay in step with the stream.
        let bytes = partials.next_partial()?;
        let Some(aggregate_fn) = &aggregate_fns[usize::from(spec)] else {
            continue;
        };
        let partial_dtype = aggregate_fn.state_dtype(dtype).ok_or_else(|| {
            vortex_err!("aggregate {aggregate_fn} does not support field dtype {dtype}")
        })?;
        let partial = ScalarValue::from_proto_bytes(bytes, &partial_dtype, session)?;
        let partial = Scalar::try_new(partial_dtype, partial)?;
        aggregates.push(AggregateStat::try_from_partial(
            aggregate_fn.clone(),
            partial,
            dtype,
        )?);
    }
    Ok(AggregateStats::new(aggregates))
}

/// Reads the length-prefixed partial states of `FileStatistics.partials` in order.
struct PartialsReader<'a> {
    remaining: &'a [u8],
}

impl<'a> PartialsReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    /// Returns the bytes of the next partial state.
    fn next_partial(&mut self) -> VortexResult<&'a [u8]> {
        let len = decode_varint(&mut self.remaining)
            .map_err(|error| vortex_err!("file statistics partial length: {error}"))?;
        let len = usize::try_from(len)
            .ok()
            .filter(|&len| len <= self.remaining.len())
            .ok_or_else(|| {
                vortex_err!(
                    "file statistics partial of {len} bytes exceeds the {} remaining bytes",
                    self.remaining.len()
                )
            })?;
        let (partial, rest) = self.remaining.split_at(len);
        self.remaining = rest;
        Ok(partial)
    }

    /// Fails unless every partial state has been read.
    fn finish(self) -> VortexResult<()> {
        vortex_ensure!(
            self.remaining.is_empty(),
            "{} unread bytes after the last file statistics partial",
            self.remaining.len()
        );
        Ok(())
    }
}

/// Assigns each distinct value a `u16` index in order of first appearance.
struct Dictionary<K> {
    values: Vec<K>,
    indices: HashMap<K, u16>,
    kind: &'static str,
}

impl<K: Clone + Eq + Hash> Dictionary<K> {
    fn new(kind: &'static str) -> Self {
        Self {
            values: Vec::new(),
            indices: HashMap::default(),
            kind,
        }
    }

    fn intern(&mut self, value: K) -> VortexResult<u16> {
        if let Some(&index) = self.indices.get(&value) {
            return Ok(index);
        }
        let index = u16::try_from(self.values.len()).map_err(|_| {
            vortex_err!("file statistics exceed {} distinct {}", u16::MAX, self.kind)
        })?;
        self.values.push(value.clone());
        self.indices.insert(value, index);
        Ok(index)
    }
}

impl<'a> IntoIterator for &'a FileStatistics {
    type Item = (&'a AggregateStats, &'a DType);
    type IntoIter =
        std::iter::Zip<std::slice::Iter<'a, AggregateStats>, std::slice::Iter<'a, DType>>;

    fn into_iter(self) -> Self::IntoIter {
        self.aggregates().iter().zip(self.dtypes().iter())
    }
}

impl FlatBufferRoot for FileStatistics {}

impl WriteFlatBuffer for FileStatistics {
    type Target<'a> = fb::FileStatistics<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> VortexResult<WIPOffset<Self::Target<'fb>>> {
        // The legacy `ArrayStats` format has a fixed slot per stat rather than open-ended
        // aggregates.
        let field_stats = self
            .stats_sets()
            .iter()
            .map(|stats| stats.write_flatbuffer(fbb))
            .collect::<VortexResult<Vec<_>>>()?;
        let field_stats = fbb.create_vector(field_stats.as_slice());

        if !self.write_nested {
            return Ok(fb::FileStatistics::create(
                fbb,
                &fb::FileStatisticsArgs {
                    field_stats: Some(field_stats),
                    ..Default::default()
                },
            ));
        }

        // Aggregate functions and the lists of them recorded per entry are dictionary-encoded, so
        // each distinct function and list is written once however many entries use it.
        let mut specs = Dictionary::<&AggregateFnRef>::new("aggregate functions");
        let mut sets = Dictionary::<Vec<u16>>::new("aggregate sets");
        let mut entry_sets = Vec::with_capacity(self.aggregates().len());
        let mut partials = Vec::new();
        for field_aggregates in self.aggregates().iter() {
            let mut set = Vec::new();
            // Values without a partial state (e.g. from legacy statistics) can't be written.
            for stat in field_aggregates.iter() {
                let Some(partial) = stat.partial() else {
                    continue;
                };
                set.push(specs.intern(stat.aggregate_fn())?);
                let partial = ScalarValue::to_proto_bytes::<Vec<u8>>(partial.value());
                encode_varint(partial.len() as u64, &mut partials);
                partials.extend_from_slice(&partial);
            }
            entry_sets.push(sets.intern(set)?);
        }

        let aggregate_specs = specs
            .values
            .into_iter()
            .map(|aggregate_fn| {
                let options = aggregate_fn.options().serialize()?.ok_or_else(|| {
                    vortex_err!(
                        "Aggregate function '{}' is not serializable",
                        aggregate_fn.id()
                    )
                })?;
                let id = fbb.create_string(aggregate_fn.id().as_ref());
                let options = (!options.is_empty()).then(|| fbb.create_vector(options.as_slice()));
                Ok(fb::AggregateSpec::create(
                    fbb,
                    &fb::AggregateSpecArgs {
                        id: Some(id),
                        options,
                    },
                ))
            })
            .collect::<VortexResult<Vec<_>>>()?;
        let aggregate_specs = fbb.create_vector(aggregate_specs.as_slice());
        let aggregate_sets = sets
            .values
            .iter()
            .map(|set| {
                let specs = fbb.create_vector(set.as_slice());
                fb::AggregateSet::create(fbb, &fb::AggregateSetArgs { specs: Some(specs) })
            })
            .collect::<Vec<_>>();
        let aggregate_sets = fbb.create_vector(aggregate_sets.as_slice());
        let entry_sets = fbb.create_vector(entry_sets.as_slice());
        let partials = fbb.create_vector(partials.as_slice());

        Ok(fb::FileStatistics::create(
            fbb,
            &fb::FileStatisticsArgs {
                field_stats: Some(field_stats),
                aggregate_specs: Some(aggregate_specs),
                aggregate_sets: Some(aggregate_sets),
                entry_sets: Some(entry_sets),
                partials: Some(partials),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use flatbuffers::FlatBufferBuilder;
    use rstest::rstest;
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::aggregate_fn::AggregateFnVTableExt;
    use vortex_array::aggregate_fn::EmptyOptions;
    use vortex_array::aggregate_fn::NumericalAggregateOpts;
    use vortex_array::aggregate_fn::fns::bounded_max::BoundedMax;
    use vortex_array::aggregate_fn::fns::bounded_max::BoundedMaxOptions;
    use vortex_array::aggregate_fn::fns::max::Max;
    use vortex_array::aggregate_fn::fns::min::Min;
    use vortex_array::aggregate_fn::fns::null_count::NullCount;
    use vortex_array::arrays::VarBinViewArray;
    use vortex_array::dtype::FieldPath;
    use vortex_array::dtype::Nullability;
    use vortex_array::dtype::PType;
    use vortex_array::expr::stats::Precision;
    use vortex_array::expr::stats::Stat;
    use vortex_array::flatbuffers::WriteFlatBufferExt;
    use vortex_array::scalar::ScalarValue;

    use super::*;

    fn i32_dtype() -> DType {
        DType::Primitive(PType::I32, Nullability::NonNullable)
    }

    /// An aggregate over a field of `dtype` whose partial state is `value` itself, as for `Min`
    /// and `NullCount`.
    fn stat(aggregate_fn: AggregateFnRef, value: Scalar, dtype: &DType) -> AggregateStat {
        let partial_dtype = aggregate_fn
            .state_dtype(dtype)
            .vortex_expect("aggregate supports dtype");
        let partial = value.cast(&partial_dtype).vortex_expect("castable partial");
        AggregateStat::try_from_partial(aggregate_fn, partial, dtype).vortex_expect("valid partial")
    }

    fn min_fn() -> AggregateFnRef {
        Min.bind(NumericalAggregateOpts::skip_nans())
    }

    /// The value `aggregates` resolves for `aggregate_fn`.
    fn value(aggregates: &AggregateStats, aggregate_fn: &AggregateFnRef) -> Precision<ScalarValue> {
        aggregates.get(aggregate_fn).and_then(Scalar::into_value)
    }

    fn min(value: i32) -> AggregateStat {
        stat(min_fn(), Scalar::from(value), &i32_dtype())
    }

    fn null_count(value: u64, dtype: &DType) -> AggregateStat {
        stat(NullCount.bind(EmptyOptions), Scalar::from(value), dtype)
    }

    fn read_back(file_stats: &FileStatistics, file_dtype: &DType) -> VortexResult<FileStatistics> {
        let bytes = file_stats.write_flatbuffer_bytes()?;
        let fb = flatbuffers::root::<fb::FileStatistics>(bytes.as_ref())
            .vortex_expect("valid flatbuffer");
        FileStatistics::from_flatbuffer(&fb, file_dtype, &VortexSession::empty())
    }

    #[test]
    fn nested_round_trip_resolves_by_path() -> VortexResult<()> {
        let inner = DType::struct_([("b", i32_dtype())], Nullability::Nullable);
        let file_dtype = DType::struct_([("a", inner.clone())], Nullability::NonNullable);

        // Layout: [a.b, a] (a's own null-count entry trails its child).
        let file_stats = FileStatistics::new_with_dtype(
            Arc::from([
                AggregateStats::new(vec![min(1)]),
                AggregateStats::new(vec![null_count(1, &inner)]),
            ]),
            Arc::from([AggregateStats::default()]),
            &file_dtype,
        );

        let read_back = read_back(&file_stats, &file_dtype)?;

        let (b, _) = read_back
            .get_by_path(&FieldPath::from_name("a").push("b"))
            .expect("a.b stats");
        assert_eq!(
            value(b, &min_fn()),
            Precision::exact(ScalarValue::from(1i32))
        );

        let (a, _) = read_back
            .get_by_path(&FieldPath::from_name("a"))
            .expect("a's own null-count stats");
        assert_eq!(
            value(a, &NullCount.bind(EmptyOptions)),
            Precision::exact(ScalarValue::from(1u64))
        );

        assert!(read_back.get_by_path(&FieldPath::root()).is_none());

        Ok(())
    }

    #[test]
    fn specs_and_sets_are_written_once() -> VortexResult<()> {
        let file_dtype = DType::struct_(
            [("a", i32_dtype()), ("b", i32_dtype()), ("c", i32_dtype())],
            Nullability::NonNullable,
        );
        let min_including_nans = Min.bind(NumericalAggregateOpts::include_nans());
        let file_stats = FileStatistics::new_with_dtype(
            Arc::from([
                AggregateStats::new(vec![min(1), null_count(0, &i32_dtype())]),
                AggregateStats::new(vec![min(2), null_count(0, &i32_dtype())]),
                // Same ID as `min`, different options: a spec of its own.
                AggregateStats::new(vec![stat(
                    min_including_nans.clone(),
                    Scalar::from(3i32),
                    &i32_dtype(),
                )]),
            ]),
            Arc::from([
                AggregateStats::default(),
                AggregateStats::default(),
                AggregateStats::default(),
            ]),
            &file_dtype,
        );

        let bytes = file_stats.write_flatbuffer_bytes()?;
        let fb = flatbuffers::root::<fb::FileStatistics>(bytes.as_ref())
            .vortex_expect("valid flatbuffer");
        let specs = fb
            .aggregate_specs()
            .vortex_expect("aggregate specs")
            .iter()
            .map(|spec| {
                (
                    spec.id().to_string(),
                    spec.options()
                        .map(|options| options.bytes().to_vec())
                        .unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>();
        let options = |aggregate_fn: &AggregateFnRef| -> VortexResult<Vec<u8>> {
            Ok(aggregate_fn.options().serialize()?.unwrap_or_default())
        };
        assert_eq!(
            specs,
            [
                ("vortex.min".to_string(), options(&min_fn())?),
                (
                    "vortex.null_count".to_string(),
                    options(&NullCount.bind(EmptyOptions))?
                ),
                ("vortex.min".to_string(), options(&min_including_nans)?),
            ]
        );

        // "a" and "b" record the same aggregates, so they share a set.
        let sets = fb
            .aggregate_sets()
            .vortex_expect("aggregate sets")
            .iter()
            .map(|set| set.specs().iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(sets, [vec![0, 1], vec![2]]);
        let entry_sets = fb
            .entry_sets()
            .vortex_expect("entry sets")
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(entry_sets, [0, 0, 1]);

        let read_back = read_back(&file_stats, &file_dtype)?;
        let (b, _) = read_back
            .get_by_path(&FieldPath::from_name("b"))
            .expect("b stats");
        assert_eq!(
            value(b, &min_fn()),
            Precision::exact(ScalarValue::from(2i32))
        );
        let (c, _) = read_back
            .get_by_path(&FieldPath::from_name("c"))
            .expect("c stats");
        assert_eq!(
            c.iter().next().map(AggregateStat::aggregate_fn),
            Some(&min_including_nans)
        );
        Ok(())
    }

    #[test]
    fn bounded_aggregate_round_trips_as_an_approximate_max() -> VortexResult<()> {
        let file_dtype = DType::struct_(
            [("s", DType::Utf8(Nullability::NonNullable))],
            Nullability::NonNullable,
        );
        let field_dtype = DType::Utf8(Nullability::NonNullable);
        let bounded_max = BoundedMax.bind(BoundedMaxOptions {
            max_bytes: NonZeroUsize::new(4).vortex_expect("non-zero"),
        });
        let mut accumulator = bounded_max.accumulator(&field_dtype)?;
        accumulator.accumulate(
            &VarBinViewArray::from_iter_str(["a value past the bound"]).into_array(),
            &mut VortexSession::empty().create_execution_ctx(),
        )?;
        let partial = accumulator.partial_scalar()?;
        let file_stats = FileStatistics::new_with_dtype(
            Arc::from([AggregateStats::new(vec![AggregateStat::try_from_partial(
                bounded_max.clone(),
                partial,
                &field_dtype,
            )?])]),
            Arc::from([AggregateStats::default()]),
            &file_dtype,
        );

        let read_back = read_back(&file_stats, &file_dtype)?;
        let path = FieldPath::from_name("s");
        let (aggregates, _) = read_back.get_by_path(&path).expect("s aggregates");
        assert!(
            aggregates
                .iter()
                .all(|stat| stat.aggregate_fn() == &bounded_max)
        );
        assert_eq!(
            value(aggregates, &Max.bind(NumericalAggregateOpts::skip_nans())),
            Precision::inexact(ScalarValue::from("a vb"))
        );
        Ok(())
    }

    /// Hand-assembles the aggregate-based sections of a `FileStatistics` flatbuffer, so tests can
    /// write footers this writer never would.
    fn raw_file_stats(
        specs: &[(&str, &[u8])],
        sets: &[&[u16]],
        entry_sets: &[u16],
        partials: &[u8],
    ) -> Vec<u8> {
        let mut fbb = FlatBufferBuilder::new();
        let specs = specs
            .iter()
            .map(|(id, options)| {
                let id = fbb.create_string(id);
                let options = fbb.create_vector(options);
                fb::AggregateSpec::create(
                    &mut fbb,
                    &fb::AggregateSpecArgs {
                        id: Some(id),
                        options: Some(options),
                    },
                )
            })
            .collect::<Vec<_>>();
        let specs = fbb.create_vector(&specs);
        let sets = sets
            .iter()
            .map(|set| {
                let specs = fbb.create_vector(set);
                fb::AggregateSet::create(&mut fbb, &fb::AggregateSetArgs { specs: Some(specs) })
            })
            .collect::<Vec<_>>();
        let sets = fbb.create_vector(&sets);
        let entry_sets = fbb.create_vector(entry_sets);
        let partials = fbb.create_vector(partials);
        let root = fb::FileStatistics::create(
            &mut fbb,
            &fb::FileStatisticsArgs {
                field_stats: None,
                aggregate_specs: Some(specs),
                aggregate_sets: Some(sets),
                entry_sets: Some(entry_sets),
                partials: Some(partials),
            },
        );
        fbb.finish_minimal(root);
        fbb.finished_data().to_vec()
    }

    fn read_raw(bytes: &[u8], file_dtype: &DType) -> VortexResult<FileStatistics> {
        let fb = flatbuffers::root::<fb::FileStatistics>(bytes).vortex_expect("valid flatbuffer");
        FileStatistics::from_flatbuffer(&fb, file_dtype, &VortexSession::empty())
    }

    /// The options of `min` and its partial state for `value`, length-prefixed as in `partials`.
    fn min_spec_and_partial(value: i32) -> VortexResult<(Vec<u8>, Vec<u8>)> {
        let known = min(value);
        let options = known
            .aggregate_fn()
            .options()
            .serialize()?
            .vortex_expect("serializable options");
        let partial = ScalarValue::to_proto_bytes::<Vec<u8>>(
            known.partial().vortex_expect("has a partial").value(),
        );
        let mut prefixed = Vec::new();
        encode_varint(partial.len() as u64, &mut prefixed);
        prefixed.extend_from_slice(&partial);
        Ok((options, prefixed))
    }

    #[test]
    fn unknown_aggregates_are_skipped_by_length() -> VortexResult<()> {
        let file_dtype = DType::struct_([("col", i32_dtype())], Nullability::NonNullable);
        let (options, known_partial) = min_spec_and_partial(7)?;

        // Spec 0 is an aggregate no session knows, recorded before the known `min`. Its partial is
        // bytes this reader can't interpret, which it has to skip to reach `min`'s.
        let mut partials = vec![3, 0xde, 0xad, 0xbe];
        partials.extend_from_slice(&known_partial);
        let bytes = raw_file_stats(
            &[("test.unknown", &[]), ("vortex.min", &options)],
            &[&[0, 1]],
            &[0],
            &partials,
        );

        let read_back = read_raw(&bytes, &file_dtype)?;
        let (aggregates, _) = read_back
            .get_by_path(&FieldPath::from_name("col"))
            .expect("col aggregates");
        assert_eq!(aggregates.iter().count(), 1);
        assert_eq!(
            value(aggregates, &min_fn()),
            Precision::exact(ScalarValue::from(7i32))
        );
        Ok(())
    }

    #[rstest]
    #[case::set_index_out_of_range(vec![vec![0]], vec![1], None, "aggregate set index 1 out of range")]
    #[case::spec_index_out_of_range(vec![vec![1]], vec![0], None, "aggregate spec index 1 out of range")]
    #[case::wrong_entry_count(vec![vec![0]], vec![0, 0], None, "")]
    #[case::partial_missing(vec![vec![0]], vec![0], Some(vec![]), "invalid varint")]
    #[case::truncated_varint(vec![vec![0]], vec![0], Some(vec![0x80]), "invalid varint")]
    #[case::overlong_varint(vec![vec![0]], vec![0], Some(vec![0x80; 11]), "invalid varint")]
    #[case::varint_overflows(
        vec![vec![0]],
        vec![0],
        Some(vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]),
        "invalid varint"
    )]
    #[case::length_past_end(vec![vec![0]], vec![0], Some(vec![5, 0]), "exceeds the 1 remaining bytes")]
    #[case::trailing_bytes(vec![vec![]], vec![0], Some(vec![0]), "1 unread bytes")]
    fn malformed_aggregate_statistics_are_rejected(
        #[case] sets: Vec<Vec<u16>>,
        #[case] entry_sets: Vec<u16>,
        #[case] partials: Option<Vec<u8>>,
        #[case] message: &str,
    ) -> VortexResult<()> {
        let file_dtype = DType::struct_([("col", i32_dtype())], Nullability::NonNullable);
        let (options, valid_partial) = min_spec_and_partial(7)?;
        let sets = sets.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let bytes = raw_file_stats(
            &[("vortex.min", &options)],
            &sets,
            &entry_sets,
            &partials.unwrap_or(valid_partial),
        );

        let error = read_raw(&bytes, &file_dtype)
            .err()
            .vortex_expect("malformed statistics must be rejected");
        assert!(
            error.to_string().contains(message),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn get_by_path_resolves_a_later_sibling_past_a_nullable_struct_field() {
        // Regression test: a nullable struct field ("s") contributes both an `s.b` entry and a
        // trailing entry for its own null count to the nested (post-order) layout, so that layout
        // has one more entry (3) than there are top-level fields (2: "s", "c"). Resolving the
        // later top-level field "c" by path must not be thrown off by "s"'s extra own-entry.
        let s_dtype = DType::struct_([("b", i32_dtype())], Nullability::Nullable);
        let file_dtype = DType::struct_(
            [("s", s_dtype.clone()), ("c", i32_dtype())],
            Nullability::NonNullable,
        );

        // Post-order layout: [s.b, s, c].
        let file_stats = FileStatistics::new_with_dtype(
            Arc::from([
                AggregateStats::default(),
                AggregateStats::new(vec![null_count(0, &s_dtype)]),
                AggregateStats::new(vec![min(42)]),
            ]),
            Arc::from([AggregateStats::default(), AggregateStats::default()]),
            &file_dtype,
        );

        let (c, _) = file_stats
            .get_by_path(&FieldPath::from_name("c"))
            .expect("c stats");
        assert_eq!(
            value(c, &min_fn()),
            Precision::exact(ScalarValue::from(42i32))
        );
    }

    #[test]
    fn legacy_non_nested_footer_still_parses() -> VortexResult<()> {
        // Simulates a footer written before aggregate-based stats existed: `entry_sets` is
        // absent, and `field_stats` holds one entry per top-level
        // struct field.
        let session = VortexSession::empty();
        let file_dtype = DType::struct_([("col", i32_dtype())], Nullability::NonNullable);

        let mut stats = StatsSet::default();
        stats.set(Stat::Min, Precision::exact(ScalarValue::from(7i32)));

        let mut fbb = FlatBufferBuilder::new();
        let array_stats = stats.write_flatbuffer(&mut fbb)?;
        let field_stats = fbb.create_vector(&[array_stats]);
        let root = fb::FileStatistics::create(
            &mut fbb,
            &fb::FileStatisticsArgs {
                field_stats: Some(field_stats),
                ..Default::default()
            },
        );
        fbb.finish_minimal(root);
        let bytes = fbb.finished_data().to_vec();

        let fb = flatbuffers::root::<fb::FileStatistics>(&bytes).vortex_expect("valid flatbuffer");
        assert!(fb.entry_sets().is_none());

        let read_back = FileStatistics::from_flatbuffer(&fb, &file_dtype, &session)?;
        let path = FieldPath::from_name("col");
        let (col, _) = read_back.get_by_path(&path).expect("col aggregates");
        assert_eq!(
            value(col, &min_fn()),
            Precision::exact(ScalarValue::from(7i32))
        );

        Ok(())
    }

    #[test]
    fn writer_still_emits_legacy_field_stats_matching_top_level_field_count() -> VortexResult<()> {
        // Regression test: an old reader (pre-nested-stats) only ever looks at `field_stats` and
        // requires its length to match the number of top-level struct fields. A nested/nullable
        // struct schema's post-order layout has a different length, so `field_stats` must keep
        // carrying the legacy top-level-only shape, not the nested one.
        let inner = DType::struct_([("b", i32_dtype())], Nullability::Nullable);
        let file_dtype =
            DType::struct_([("a", inner), ("c", i32_dtype())], Nullability::NonNullable);
        let struct_fields = file_dtype
            .as_struct_fields_opt()
            .vortex_expect("file_dtype is a struct");

        // Nested (post-order) layout: [a.b, a, c] - 3 entries, differs from the 2 top-level fields.
        let nested: Arc<[AggregateStats]> = Arc::from([
            AggregateStats::default(),
            AggregateStats::default(),
            AggregateStats::default(),
        ]);
        // Legacy layout: one entry per top-level field ("a", "c").
        let legacy: Arc<[AggregateStats]> =
            Arc::from([AggregateStats::default(), AggregateStats::default()]);

        let file_stats = FileStatistics::new_with_dtype(nested, legacy, &file_dtype);
        let bytes = file_stats.write_flatbuffer_bytes()?;
        let fb = flatbuffers::root::<fb::FileStatistics>(bytes.as_ref())
            .vortex_expect("valid flatbuffer");

        let field_stats_len = fb.field_stats().map_or(0, |field_stats| field_stats.len());
        assert_eq!(field_stats_len, struct_fields.nfields());

        Ok(())
    }

    #[test]
    fn is_empty_considers_legacy_stats_too() {
        // Regression test: the nested (post-order) layout can be empty while the legacy layout
        // still carries real content, e.g. a non-nullable struct field whose entire subtree is
        // unsupported dtypes contributes no nested entries, but still gets a legacy NullCount
        // (dtype-agnostic). `is_empty` must not report "nothing to write" in that case, or the
        // caller (the footer serializer) would silently drop the legacy stats too.
        // A non-nullable struct field whose only child is an unsupported dtype (`Variant`)
        // contributes zero entries to the nested post-order layout: the leaf is skipped, and the
        // struct itself is non-nullable so it gets no own entry either.
        let file_dtype = DType::struct_(
            [("a", DType::Variant(Nullability::NonNullable))],
            Nullability::NonNullable,
        );
        assert_eq!(postorder_stats_layout(&file_dtype), Vec::new());
        let legacy = AggregateStats::new(vec![null_count(
            0,
            &DType::Variant(Nullability::NonNullable),
        )]);

        let file_stats =
            FileStatistics::new_with_dtype(Arc::from([]), Arc::from([legacy]), &file_dtype);

        assert!(!file_stats.is_empty());
    }
}
