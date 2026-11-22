// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::mem::needs_drop;
use std::sync::Arc;

use vortex_error::VortexError;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;

use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::IntoArray;
use crate::array::ArrayView;
use crate::array::VTable;
use crate::array::probe::RepeatedArrayProbe;
use crate::array::probe::RepeatedState;
use crate::array::probe::repeated::child_probe;
use crate::array::probe::repeated::is_valid_scalar;
use crate::arrays::Chunked;
use crate::arrays::Dict;
use crate::arrays::Extension;
use crate::arrays::FixedSizeList;
use crate::arrays::List;
use crate::arrays::ListView;
use crate::arrays::Map;
use crate::arrays::Masked;
use crate::arrays::Primitive;
use crate::arrays::Slice;
use crate::arrays::Struct;
use crate::arrays::Union;
use crate::arrays::chunked::ChunkedArrayExt;
use crate::arrays::chunked::ChunkedSlots;
use crate::arrays::dict::DictSlots;
use crate::arrays::extension::ExtensionArrayExt;
use crate::arrays::extension::ExtensionSlots;
use crate::arrays::fixed_size_list::FixedSizeListArrayExt;
use crate::arrays::fixed_size_list::FixedSizeListSlots;
use crate::arrays::list::ListArrayExt;
use crate::arrays::list::ListSlots;
use crate::arrays::listview::ListViewArrayExt;
use crate::arrays::listview::ListViewSlots;
use crate::arrays::ListViewArray;
use crate::arrays::map::MapSlots;
use crate::arrays::masked::MaskedSlots;
use crate::arrays::slice::SliceArraySlotsExt;
use crate::arrays::struct_::StructArrayExt;
use crate::arrays::struct_::StructSlots;
use crate::arrays::union::UnionArrayExt;
use crate::arrays::union::UnionArraySlotsExt;
use crate::dtype::DType;
use crate::dtype::Nullability;
use crate::dtype::UnionVariants;
use crate::dtype::extension::ExtDTypeRef;
use crate::scalar::Scalar;
use crate::scalar::ScalarValue;
use crate::validity::Validity;
use crate::vtable::OperationsVTable;

/// A borrowed row accessor.
///
/// Either a one-off reader over a borrowed array, which retains nothing, or a borrow of a
/// [`RepeatedArrayProbe`] that keeps state between reads. Both variants hold only references,
/// so an `ArrayProbe` has no destructor and building one per read, including for every child
/// slot of a nested array, costs nothing.
pub enum ArrayProbe<'a> {
    /// A single read of a borrowed array; nothing outlives the call.
    Once(&'a ArrayRef),
    /// A read through a retained probe.
    Repeated(&'a mut RepeatedArrayProbe),
}

// Everything built per read must be free to drop, or the unwind path pins it in memory and
// blocks the tail call into the encoding. Keep these at compile time.
const _: () = assert!(!needs_drop::<ArrayProbe<'_>>());
const _: () = assert!(!needs_drop::<ProbeState<'_, Primitive>>());

impl ArrayProbe<'_> {
    /// The array this probe reads from.
    #[inline]
    pub fn array(&self) -> &ArrayRef {
        match self {
            Self::Once(array) => array,
            Self::Repeated(probe) => probe.array(),
        }
    }

    /// Read the scalar at `index`, including its nullness.
    #[inline]
    pub fn execute_scalar(&mut self, index: usize, ctx: &mut ExecutionCtx) -> VortexResult<Scalar> {
        match self {
            Self::Once(array) => execute_scalar_once(array, index, ctx),
            Self::Repeated(probe) => probe.execute_scalar(index, ctx),
        }
    }

    /// Whether the row at `index` is valid.
    #[inline]
    pub fn execute_is_valid(&mut self, index: usize, ctx: &mut ExecutionCtx) -> VortexResult<bool> {
        match self {
            Self::Once(array) => execute_is_valid_once(array, index, ctx),
            Self::Repeated(probe) => probe.execute_is_valid(index, ctx),
        }
    }

    /// Whether the row at `index` is null.
    #[inline]
    pub fn execute_is_invalid(
        &mut self,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<bool> {
        Ok(!self.execute_is_valid(index, ctx)?)
    }
}

enum ScalarFrame {
    Masked,
    Dict {
        dtype: DType,
    },
    Extension {
        ext_dtype: ExtDTypeRef,
    },
    Union {
        variants: UnionVariants,
        type_id: u8,
        nullability: Nullability,
    },
    Struct {
        dtype: DType,
        owner: ArrayRef,
        index: usize,
        next_field: usize,
        nfields: usize,
        values: Vec<Option<ScalarValue>>,
    },
    Sequence {
        element_dtype: DType,
        nullability: Nullability,
        fixed_size: bool,
        elements: ArrayRef,
        cur: usize,
        end: usize,
        items: Vec<Scalar>,
    },
    Map {
        dtype: DType,
        key_dtype: DType,
        val_dtype: DType,
        entries_el: ArrayRef,
        cur: usize,
        end: usize,
        pairs: Vec<(Scalar, Scalar)>,
    },
}

#[inline]
fn is_iterative_scalar_array(array: &ArrayRef) -> bool {
    array.is::<Struct>()
        || array.is::<List>()
        || array.is::<ListView>()
        || array.is::<FixedSizeList>()
        || array.is::<Map>()
        || array.is::<Extension>()
        || array.is::<Masked>()
        || array.is::<Slice>()
        || array.is::<Dict>()
        || array.is::<Chunked>()
        || array.is::<Union>()
}

/// One-off scalar read without call-stack recursion through compound containers.
#[inline]
fn execute_scalar_once(
    array: &ArrayRef,
    index: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Scalar> {
    if !is_iterative_scalar_array(array) {
        check_bounds(array, index)?;
        return check_dtype(
            array,
            array.dyn_array().probe_scalar_once(array, index, ctx),
        );
    }
    execute_scalar_iterative(array, index, ctx)
}

#[allow(clippy::cognitive_complexity)]
fn execute_scalar_iterative(
    root: &ArrayRef,
    root_index: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Scalar> {
    let mut frames: Vec<ScalarFrame> = Vec::new();
    let mut cur_array = root.clone();
    let mut cur_index = root_index;

    'visit: loop {
        let mut completed = loop {
            check_bounds(&cur_array, cur_index)?;

            if let Some(slice) = cur_array.as_opt::<Slice>() {
                cur_index += slice.slice_range().start;
                cur_array = slice.child().clone();
                continue;
            }
            if let Some(chunked) = cur_array.as_opt::<Chunked>() {
                let (chunk_idx, chunk_offset) = chunked.find_chunk_idx(cur_index)?;
                let chunk = child_of(&cur_array, ChunkedSlots::CHUNKS_OFFSET + chunk_idx)?
                    .ok_or_else(|| vortex_err!("Chunked chunk slot {chunk_idx} is absent"))?
                    .clone();
                cur_array = chunk;
                cur_index = chunk_offset;
                continue;
            }
            if cur_array.is::<Masked>() {
                if !is_valid_once(&cur_array, cur_index, ctx)? {
                    break Scalar::null(cur_array.dtype().clone());
                }
                let child = child_of(&cur_array, MaskedSlots::CHILD)?
                    .ok_or_else(|| vortex_err!("Masked child slot is missing"))?
                    .clone();
                frames.push(ScalarFrame::Masked);
                cur_array = child;
                continue;
            }
            if cur_array.is::<Dict>() {
                let codes = child_of(&cur_array, DictSlots::CODES)?
                    .ok_or_else(|| vortex_err!("Dict codes slot is missing"))?;
                let code = execute_scalar_once(codes, cur_index, ctx)?;
                let Some(dict_index) = code.as_primitive().as_::<usize>() else {
                    break Scalar::null(cur_array.dtype().clone());
                };
                let values = child_of(&cur_array, DictSlots::VALUES)?
                    .ok_or_else(|| vortex_err!("Dict values slot is missing"))?
                    .clone();
                frames.push(ScalarFrame::Dict {
                    dtype: cur_array.dtype().clone(),
                });
                cur_array = values;
                cur_index = dict_index;
                continue;
            }
            if let Some(ext) = cur_array.as_opt::<Extension>() {
                let ext_dtype = ext.ext_dtype().clone();
                let storage = child_of(&cur_array, ExtensionSlots::STORAGE)?
                    .ok_or_else(|| vortex_err!("Extension storage slot is missing"))?
                    .clone();
                frames.push(ScalarFrame::Extension { ext_dtype });
                cur_array = storage;
                continue;
            }
            if let Some(union_arr) = cur_array.as_opt::<Union>() {
                let type_id_scalar = union_arr.type_ids().execute_scalar(cur_index, ctx)?;
                let Some(type_id) = type_id_scalar.as_primitive().typed_value::<u8>() else {
                    break Scalar::null(cur_array.dtype().clone());
                };
                let variants = union_arr.variants().clone();
                let Some(child_idx) = variants.tag_to_child_index(type_id) else {
                    vortex_bail!("Unknown UnionArray type ID {type_id}");
                };
                let child = union_arr
                    .child(child_idx)
                    .ok_or_else(|| vortex_err!("UnionArray is missing child {child_idx}"))?
                    .clone();
                let nullability = cur_array.dtype().nullability();
                frames.push(ScalarFrame::Union {
                    variants,
                    type_id,
                    nullability,
                });
                cur_array = child;
                continue;
            }
            if let Some(st) = cur_array.as_opt::<Struct>() {
                if !is_valid_once(&cur_array, cur_index, ctx)? {
                    break Scalar::null(cur_array.dtype().clone());
                }
                let nfields = st.iter_unmasked_fields().len();
                let dtype = cur_array.dtype().clone();
                if nfields == 0 {
                    break unsafe { Scalar::new_unchecked(dtype, Some(ScalarValue::Tuple(Vec::new()))) };
                }
                let first = child_of(&cur_array, StructSlots::FIELDS_OFFSET)?
                    .ok_or_else(|| {
                        vortex_err!("Struct field slot {} is absent", StructSlots::FIELDS_OFFSET)
                    })?
                    .clone();
                frames.push(ScalarFrame::Struct {
                    dtype,
                    owner: cur_array,
                    index: cur_index,
                    next_field: 1,
                    nfields,
                    values: Vec::with_capacity(nfields),
                });
                cur_array = first;
                continue;
            }
            if let Some(list) = cur_array.as_opt::<List>() {
                if !is_valid_once(&cur_array, cur_index, ctx)? {
                    break Scalar::null(cur_array.dtype().clone());
                }
                let start = list.offset_at(cur_index, ctx)?;
                let end = list.offset_at(cur_index + 1, ctx)?;
                let elements = child_of(&cur_array, ListSlots::ELEMENTS)?
                    .ok_or_else(|| vortex_err!("List elements slot is missing"))?
                    .clone();
                let element_dtype = elements.dtype().clone();
                let nullability = cur_array.dtype().nullability();
                if start == end {
                    break Scalar::list(Arc::new(element_dtype), Vec::new(), nullability);
                }
                frames.push(ScalarFrame::Sequence {
                    element_dtype,
                    nullability,
                    fixed_size: false,
                    elements: elements.clone(),
                    cur: start + 1,
                    end,
                    items: Vec::with_capacity(end - start),
                });
                cur_array = elements;
                cur_index = start;
                continue;
            }
            if let Some(lv) = cur_array.as_opt::<ListView>() {
                if !is_valid_once(&cur_array, cur_index, ctx)? {
                    break Scalar::null(cur_array.dtype().clone());
                }
                let offset = lv.offset_at(cur_index, ctx);
                let size = lv.size_at(cur_index, ctx);
                let elements = child_of(&cur_array, ListViewSlots::ELEMENTS)?
                    .ok_or_else(|| vortex_err!("ListView elements slot is missing"))?
                    .clone();
                let element_dtype = elements.dtype().clone();
                let nullability = cur_array.dtype().nullability();
                if size == 0 {
                    break Scalar::list(Arc::new(element_dtype), Vec::new(), nullability);
                }
                frames.push(ScalarFrame::Sequence {
                    element_dtype,
                    nullability,
                    fixed_size: false,
                    elements: elements.clone(),
                    cur: offset + 1,
                    end: offset + size,
                    items: Vec::with_capacity(size),
                });
                cur_array = elements;
                cur_index = offset;
                continue;
            }
            if let Some(fsl) = cur_array.as_opt::<FixedSizeList>() {
                if !is_valid_once(&cur_array, cur_index, ctx)? {
                    break Scalar::null(cur_array.dtype().clone());
                }
                let list_size = fsl.list_size() as usize;
                let elements = child_of(&cur_array, FixedSizeListSlots::ELEMENTS)?
                    .ok_or_else(|| vortex_err!("FixedSizeList elements slot is missing"))?
                    .clone();
                let element_dtype = elements.dtype().clone();
                let nullability = cur_array.dtype().nullability();
                if list_size == 0 {
                    break Scalar::fixed_size_list(element_dtype, Vec::new(), nullability);
                }
                let start = list_size * cur_index;
                let end = start + list_size;
                frames.push(ScalarFrame::Sequence {
                    element_dtype,
                    nullability,
                    fixed_size: true,
                    elements: elements.clone(),
                    cur: start + 1,
                    end,
                    items: Vec::with_capacity(list_size),
                });
                cur_array = elements;
                cur_index = start;
                continue;
            }
            if cur_array.is::<Map>() {
                if !is_valid_once(&cur_array, cur_index, ctx)? {
                    break Scalar::null(cur_array.dtype().clone());
                }
                let entries = child_of(&cur_array, MapSlots::ENTRIES)?
                    .ok_or_else(|| vortex_err!("Map entries slot is missing"))?;
                let entries_owned;
                let (entries_ref, entries_lv) = if let Some(lv) = entries.as_opt::<ListView>() {
                    (entries, lv)
                } else {
                    entries_owned = entries.clone().execute::<ListViewArray>(ctx)?.into_array();
                    (&entries_owned, entries_owned.as_::<ListView>())
                };
                let offset = entries_lv.offset_at(cur_index, ctx);
                let size = entries_lv.size_at(cur_index, ctx);
                let entries_el = child_of(entries_ref, ListViewSlots::ELEMENTS)?
                    .ok_or_else(|| vortex_err!("ListView elements slot is missing"))?
                    .clone();
                let dtype = cur_array.dtype().clone();
                let Some(map_dtype) = dtype.as_map_opt() else {
                    vortex_bail!("MapArray requires a map dtype");
                };
                if size == 0 {
                    break Scalar::try_map(dtype, Vec::new())?;
                }
                let key_dtype = map_dtype.key_dtype().clone();
                let val_dtype = map_dtype.value_dtype().clone();
                frames.push(ScalarFrame::Map {
                    dtype,
                    key_dtype,
                    val_dtype,
                    entries_el: entries_el.clone(),
                    cur: offset + 1,
                    end: offset + size,
                    pairs: Vec::with_capacity(size),
                });
                cur_array = entries_el;
                cur_index = offset;
                continue;
            }

            break check_dtype(
                &cur_array,
                cur_array
                    .dyn_array()
                    .probe_scalar_once(&cur_array, cur_index, ctx),
            )?;
        };

        while let Some(frame) = frames.pop() {
            completed = match frame {
                ScalarFrame::Masked => completed.into_nullable(),
                ScalarFrame::Dict { dtype } => completed
                    .cast(&dtype)
                    .vortex_expect("Array dtype will only differ by nullability"),
                ScalarFrame::Extension { ext_dtype } => {
                    Scalar::try_extension_ref(ext_dtype, completed)?
                }
                ScalarFrame::Union {
                    variants,
                    type_id,
                    nullability,
                } => Scalar::union(variants, type_id, completed, nullability)?,
                ScalarFrame::Struct {
                    dtype,
                    owner,
                    index,
                    next_field,
                    nfields,
                    mut values,
                } => {
                    values.push(completed.into_value());
                    if next_field < nfields {
                        let slot = StructSlots::FIELDS_OFFSET + next_field;
                        let field_arr = child_of(&owner, slot)?
                            .ok_or_else(|| vortex_err!("Struct field slot {slot} is absent"))?
                            .clone();
                        frames.push(ScalarFrame::Struct {
                            dtype,
                            owner,
                            index,
                            next_field: next_field + 1,
                            nfields,
                            values,
                        });
                        cur_array = field_arr;
                        cur_index = index;
                        continue 'visit;
                    }
                    unsafe { Scalar::new_unchecked(dtype, Some(ScalarValue::Tuple(values))) }
                }
                ScalarFrame::Sequence {
                    element_dtype,
                    nullability,
                    fixed_size,
                    elements,
                    cur,
                    end,
                    mut items,
                } => {
                    items.push(completed);
                    if cur < end {
                        let next_elem = elements.clone();
                        frames.push(ScalarFrame::Sequence {
                            element_dtype,
                            nullability,
                            fixed_size,
                            elements,
                            cur: cur + 1,
                            end,
                            items,
                        });
                        cur_array = next_elem;
                        cur_index = cur;
                        continue 'visit;
                    }
                    if fixed_size {
                        Scalar::fixed_size_list(element_dtype, items, nullability)
                    } else {
                        Scalar::list(Arc::new(element_dtype), items, nullability)
                    }
                }
                ScalarFrame::Map {
                    dtype,
                    key_dtype,
                    val_dtype,
                    entries_el,
                    cur,
                    end,
                    mut pairs,
                } => {
                    let mut completed_val = completed.into_value();
                    let Some(ScalarValue::Tuple(tuple_vals)) = completed_val.as_mut() else {
                        vortex_bail!("Map entry scalar must be a two-field struct");
                    };
                    let mut pair_vals = std::mem::take(tuple_vals);
                    if pair_vals.len() != 2 {
                        vortex_bail!("Map entry struct must have key and value fields");
                    }
                    let val_raw = pair_vals.pop().vortex_expect("checked len == 2");
                    let key_raw = pair_vals.pop().vortex_expect("checked len == 2");
                    let key = unsafe { Scalar::new_unchecked(key_dtype.clone(), key_raw) };
                    let val = unsafe { Scalar::new_unchecked(val_dtype.clone(), val_raw) };
                    pairs.push((key, val));
                    if cur < end {
                        let next_el = entries_el.clone();
                        frames.push(ScalarFrame::Map {
                            dtype,
                            key_dtype,
                            val_dtype,
                            entries_el,
                            cur: cur + 1,
                            end,
                            pairs,
                        });
                        cur_array = next_el;
                        cur_index = cur;
                        continue 'visit;
                    }
                    Scalar::try_map(dtype, pairs)?
                }
            };
        }

        return check_dtype(root, Ok(completed));
    }
}

/// One-off validity read: nothing outlives the call.
#[inline]
fn execute_is_valid_once(
    array: &ArrayRef,
    index: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<bool> {
    check_bounds(array, index)?;
    is_valid_once(array, index, ctx)
}

#[inline]
fn is_valid_once(array: &ArrayRef, index: usize, ctx: &mut ExecutionCtx) -> VortexResult<bool> {
    if !array.dtype().is_nullable() {
        return Ok(true);
    }
    match array.validity()? {
        Validity::NonNullable | Validity::AllValid => Ok(true),
        Validity::AllInvalid => Ok(false),
        Validity::Array(validity) => is_valid_scalar(validity.execute_scalar(index, ctx)?, index),
    }
}

/// Pass an encoding's result through, checking its dtype in debug builds.
///
/// Unwrapping and re-wrapping the result here would cost an extra copy of the scalar on every
/// read.
#[inline]
pub(super) fn check_dtype(array: &ArrayRef, result: VortexResult<Scalar>) -> VortexResult<Scalar> {
    result.inspect(|scalar| {
        debug_assert_eq!(scalar.dtype(), array.dtype(), "Scalar dtype mismatch");
    })
}

#[inline]
pub(super) fn check_bounds(array: &ArrayRef, index: usize) -> VortexResult<()> {
    if index >= array.len() {
        return Err(out_of_bounds(index, array.len()));
    }
    Ok(())
}

/// Kept out of line so the error path, which captures a backtrace, does not count against the
/// hot probe bodies when the inliner sizes them.
#[cold]
#[inline(never)]
fn out_of_bounds(index: usize, len: usize) -> VortexError {
    vortex_err!("index {index} out of bounds from 0 to {len}")
}

/// The encoding state type of `V`'s operations vtable.
pub type EncodingProbeState<V> =
    <<V as VTable>::OperationsVTable as OperationsVTable<V>>::ProbeState;

/// Everything an encoding's `probe_scalar` runs with: the typed view of the array being read
/// and, for a repeated read, a borrow of the state its [`RepeatedArrayProbe`] keeps.
///
/// Passed to [`OperationsVTable::probe_scalar`].
/// A one-off read gets [`ProbeState::once`], which holds only the view. A repeated read borrows
/// the encoding's own state and the lazily created child probes. Neither owns anything, so
/// building one per read is free.
///
/// Encodings never inspect the policy: [`ProbeState::slot`] hands out a probe over a child under
/// whichever policy is in force, [`ProbeState::retained`] hands out the encoding state only when
/// it is kept, and [`ProbeState::split`] gives both at once.
pub struct ProbeState<'a, V: VTable> {
    array: ArrayView<'a, V>,
    retained: Option<&'a mut RepeatedState<EncodingProbeState<V>>>,
}

impl<'a, V: VTable> ProbeState<'a, V> {
    /// State for a single read of `array`.
    ///
    /// Encodings use it to run their `probe_scalar` path from the deprecated `scalar_at`; it
    /// goes away with `scalar_at`.
    #[inline]
    pub fn once(array: ArrayView<'a, V>) -> Self {
        Self {
            array,
            retained: None,
        }
    }

    /// State for a read through a [`RepeatedArrayProbe`], borrowing what it keeps.
    #[inline]
    pub(crate) fn repeated(
        array: ArrayView<'a, V>,
        retained: &'a mut RepeatedState<EncodingProbeState<V>>,
    ) -> Self {
        Self {
            array,
            retained: Some(retained),
        }
    }

    /// The typed view of the array being read.
    #[inline]
    pub fn array(&self) -> ArrayView<'a, V> {
        self.array
    }

    /// Whether the row at `index` of the array being read is valid.
    /// Will use the cached state to resolve the query.
    #[inline]
    pub fn is_valid(&mut self, index: usize, ctx: &mut ExecutionCtx) -> VortexResult<bool> {
        let array = self.array.array();
        match &mut self.retained {
            None => is_valid_once(array, index, ctx),
            Some(retained) => retained.is_valid(array, index, ctx),
        }
    }

    /// The encoding's retained state, or `None` for a one-off read.
    ///
    /// Encodings whose repeated algorithm differs from their one-off one branch on this. To hold
    /// the state while reading children, use [`Self::split`].
    #[inline]
    pub fn retained(&mut self) -> Option<&mut EncodingProbeState<V>> {
        self.retained.as_deref_mut().map(RepeatedState::state_mut)
    }

    /// The encoding's retained state and the array's children, borrowed apart, so a cache can
    /// stay borrowed while children are read.
    #[inline]
    pub fn split(&mut self) -> (Option<&mut EncodingProbeState<V>>, ProbeChildren<'_, 'a, V>) {
        let array = self.array;
        match &mut self.retained {
            None => (None, ProbeChildren { array, slots: None }),
            Some(repeated) => {
                let (state, slots) = repeated.split_mut();
                let children = ProbeChildren {
                    array,
                    slots: Some(slots),
                };
                (Some(state), children)
            }
        }
    }

    /// A probe over the array's child in `slot`, under this state's policy.
    ///
    /// Shorthand for [`Self::split`] followed by [`ProbeChildren::slot`]. `None` if the slot is
    /// absent, an error if it is out of bounds.
    #[inline]
    pub fn slot(&mut self, slot: usize) -> VortexResult<Option<ArrayProbe<'_>>> {
        let parent = self.array.array();
        match &mut self.retained {
            None => Ok(child_of(parent, slot)?.map(ArrayProbe::Once)),
            Some(repeated) => {
                Ok(child_probe(repeated.split_mut().1, parent, slot)?.map(ArrayProbe::Repeated))
            }
        }
    }
}

/// The child slots of the array a [`ProbeState`] reads, borrowed apart from the encoding state
/// by [`ProbeState::split`].
pub struct ProbeChildren<'s, 'a, V: VTable> {
    array: ArrayView<'a, V>,
    /// The retained slot table, or `None` for a one-off read.
    slots: Option<&'s mut Vec<Option<RepeatedArrayProbe>>>,
}

impl<V: VTable> ProbeChildren<'_, '_, V> {
    /// A probe over the child in `slot`, under the read's policy.
    ///
    /// For a one-off read this borrows the child; for a repeated read it is the
    /// [`RepeatedArrayProbe`] kept in the slot table, created on first use. `None` if the slot
    /// is absent, an error if it is out of bounds.
    #[inline]
    pub fn slot(&mut self, slot: usize) -> VortexResult<Option<ArrayProbe<'_>>> {
        let parent = self.array.array();
        match &mut self.slots {
            None => Ok(child_of(parent, slot)?.map(ArrayProbe::Once)),
            Some(slots) => Ok(child_probe(slots, parent, slot)?.map(ArrayProbe::Repeated)),
        }
    }
}

/// The child of `parent` in `slot`: `None` if the slot is absent, an error if it is out of bounds.
#[inline]
pub(super) fn child_of(parent: &ArrayRef, slot: usize) -> VortexResult<Option<&ArrayRef>> {
    Ok(parent
        .slots()
        .get(slot)
        .ok_or_else(|| vortex_err!("Probe slot {slot} is out of bounds"))?
        .as_ref())
}
#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use vortex_error::VortexResult;
    use vortex_error::vortex_err;
    use vortex_session::VortexSession;

    use super::*;
    use crate::VortexSessionExecute;
    use crate::array::IntoArray;
    use crate::arrays::BoolArray;
    use crate::arrays::DictArray;
    use crate::arrays::PrimitiveArray;
    use crate::arrays::Struct;
    use crate::arrays::StructArray;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(VortexSession::empty);

    fn nullable_ints() -> ArrayRef {
        PrimitiveArray::from_option_iter([Some(10i32), None, Some(30)]).into_array()
    }

    fn check_reads(mut probe: ArrayProbe<'_>, ctx: &mut ExecutionCtx) -> VortexResult<()> {
        assert!(probe.execute_scalar(3, ctx).is_err());
        assert!(probe.execute_is_valid(3, ctx).is_err());
        assert!(probe.execute_scalar(1, ctx)?.is_null());
        assert!(!probe.execute_is_valid(1, ctx)?);
        assert!(probe.execute_is_invalid(1, ctx)?);
        assert_eq!(probe.execute_scalar(2, ctx)?, Scalar::from(Some(30i32)));
        assert_eq!(probe.execute_scalar(0, ctx)?, Scalar::from(Some(10i32)));
        Ok(())
    }

    #[test]
    fn once_probe_checks_bounds_and_nulls() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let array = nullable_ints();
        check_reads(array.probe(), &mut ctx)
    }

    /// Bool has no `probe_scalar` of its own, so this reads through the default that checks
    /// validity before `scalar_at`.
    #[test]
    fn default_probe_scalar_resolves_nulls() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let array = BoolArray::from_iter([Some(true), None, Some(false)]).into_array();
        check_bool_reads(array.probe(), &mut ctx)?;
        check_bool_reads(array.repeated_probe().as_probe(), &mut ctx)
    }

    fn check_bool_reads(mut probe: ArrayProbe<'_>, ctx: &mut ExecutionCtx) -> VortexResult<()> {
        assert_eq!(probe.execute_scalar(0, ctx)?, Scalar::from(Some(true)));
        assert!(probe.execute_scalar(1, ctx)?.is_null());
        assert_eq!(probe.execute_scalar(2, ctx)?, Scalar::from(Some(false)));
        Ok(())
    }

    /// A null reached through a child comes back from the wrapper as a null of the wrapper's
    /// dtype, on both the one-off and the retained path.
    #[test]
    fn wrapper_passes_child_nulls_through() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let codes = PrimitiveArray::from_iter([0u32, 1, 0, 2]).into_array();
        let values = PrimitiveArray::from_option_iter([Some(7i64), None, Some(9)]).into_array();
        let array = DictArray::try_new(codes, values)?.into_array();
        let expected = [Some(7i64), None, Some(7), Some(9)];
        let mut once = array.probe();
        let mut repeated = array.repeated_probe();
        for (index, value) in expected.into_iter().enumerate() {
            assert_eq!(once.execute_scalar(index, &mut ctx)?, Scalar::from(value));
            assert_eq!(
                repeated.execute_scalar(index, &mut ctx)?,
                Scalar::from(value)
            );
        }
        Ok(())
    }

    /// Nullable codes and nullable values each produce nulls, and one retained probe answers
    /// validity and scalar reads on both, sliced or not.
    #[test]
    fn dict_with_nullable_codes_and_values() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let codes = PrimitiveArray::from_option_iter([Some(0u32), None, Some(1), Some(2), None])
            .into_array();
        let values = PrimitiveArray::from_option_iter([Some(7i64), None, Some(9)]).into_array();
        let array = DictArray::try_new(codes, values)?.into_array();
        let expected = [Some(7i64), None, None, Some(9), None];
        for (array, expected) in [
            (array.clone(), &expected[..]),
            (array.slice(1..4)?, &expected[1..4]),
        ] {
            let mut once = array.probe();
            let mut repeated = array.repeated_probe();
            for (index, value) in expected.iter().enumerate().rev() {
                assert_eq!(once.execute_is_valid(index, &mut ctx)?, value.is_some());
                assert_eq!(repeated.execute_is_valid(index, &mut ctx)?, value.is_some());
                assert_eq!(once.execute_scalar(index, &mut ctx)?, Scalar::from(*value));
                assert_eq!(
                    repeated.execute_scalar(index, &mut ctx)?,
                    Scalar::from(*value)
                );
            }
        }
        Ok(())
    }

    #[test]
    fn state_validity_follows_read_policy() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let array = nullable_ints();
        let typed = array
            .as_opt::<Primitive>()
            .ok_or_else(|| vortex_err!("expected a primitive"))?;

        let mut once = ProbeState::once(typed);
        assert!(once.is_valid(0, &mut ctx)?);
        assert!(!once.is_valid(1, &mut ctx)?);

        let mut repeated = RepeatedState::<()>::default();
        let mut state = ProbeState::repeated(typed, &mut repeated);
        assert!(!state.is_valid(1, &mut ctx)?);
        assert!(state.is_valid(2, &mut ctx)?);

        let all_valid = PrimitiveArray::from_option_iter([Some(1i32)]).into_array();
        let typed = all_valid
            .as_opt::<Primitive>()
            .ok_or_else(|| vortex_err!("expected a primitive"))?;
        assert!(ProbeState::once(typed).is_valid(0, &mut ctx)?);
        Ok(())
    }

    #[test]
    fn once_state_reads_children_without_retaining() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let array = struct_of_two_fields()?;
        let typed = array
            .as_opt::<Struct>()
            .ok_or_else(|| vortex_err!("expected a struct"))?;
        let mut state = ProbeState::once(typed);

        assert!(state.slot(5).is_err());
        // Slot 0 is the struct's absent validity.
        assert!(state.slot(0)?.is_none());
        {
            let mut field = state.slot(2)?.ok_or_else(|| vortex_err!("missing field"))?;
            assert!(matches!(field, ArrayProbe::Once(_)));
            assert_eq!(field.array().len(), 2);
            assert_eq!(field.execute_scalar(1, &mut ctx)?, Scalar::from(4i64));
            assert!(field.execute_is_valid(0, &mut ctx)?);
        }
        assert!(state.retained().is_none());
        let (retained, mut children) = state.split();
        assert!(retained.is_none());
        let mut field = children
            .slot(1)?
            .ok_or_else(|| vortex_err!("missing field"))?;
        assert_eq!(field.execute_scalar(1, &mut ctx)?, Scalar::from(2i32));
        Ok(())
    }

    fn struct_of_two_fields() -> VortexResult<ArrayRef> {
        Ok(StructArray::from_fields(&[
            ("a", PrimitiveArray::from_iter([1i32, 2]).into_array()),
            ("b", PrimitiveArray::from_iter([3i64, 4]).into_array()),
        ])?
        .into_array())
    }
}
