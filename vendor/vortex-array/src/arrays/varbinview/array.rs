// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-10-16: retain admitted buffer-table ownership.
// Modified by ShardLoom on 2026-10-17: carry checked native UTF-8 value validation.
// Modified by ShardLoom on 2026-10-28: preserve selected-row UTF-8 diagnostics.

use std::fmt::Display;
use std::fmt::Formatter;
use std::mem::size_of;
use std::sync::Arc;

use smallvec::smallvec;
use vortex_buffer::Alignment;
use vortex_buffer::Buffer;
use vortex_buffer::BufferAllocatorRef;
use vortex_buffer::BufferString;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_error::vortex_panic;
use vortex_mask::AllOr;
use vortex_mask::Mask;

use crate::ArrayRef;
use crate::ArraySlots;
use crate::ExecutionCtx;
use crate::array::Array;
use crate::array::ArrayParts;
use crate::array::TypedArrayRef;
use crate::array::child_to_validity;
use crate::array::validity_to_child;
use crate::array_slots;
use crate::arrays::VarBinView;
use crate::arrays::varbinview::BinaryView;
use crate::arrays::varbinview::ResolvedViews;
use crate::arrays::varbinview::metadata::MetadataReservation;
use crate::arrays::varbinview::metadata::MetadataVec;
use crate::buffer::BufferHandle;
use crate::builders::ArrayBuilder;
use crate::builders::VarBinViewBuilder;
use crate::dtype::DType;
use crate::dtype::Nullability;
use crate::memory::HostAllocatorRef;
use crate::validity::Validity;

#[array_slots(VarBinView)]
pub struct VarBinViewSlots {
    /// The validity bitmap indicating which elements are non-null.
    #[slot(0)]
    pub validity: Option<ArrayRef>,
}

/// A variable-length binary view array that stores strings and binary data efficiently.
///
/// This mirrors the Apache Arrow StringView/BinaryView array encoding and provides
/// an optimized representation for variable-length data with excellent performance
/// characteristics for both short and long strings.
///
/// ## Data Layout
///
/// The array uses a hybrid storage approach with two main components:
/// - **Views buffer**: Array of 16-byte `BinaryView` entries (one per logical element)
/// - **Data buffers**: Shared backing storage for strings longer than 12 bytes
///
/// ## View Structure
///
/// Commonly referred to as "German Strings", each 16-byte view entry contains either:
/// - **Inlined data**: For strings ≤ 12 bytes, the entire string is stored directly in the view
/// - **Reference data**: For strings > 12 bytes, contains:
///   - String length (4 bytes)
///   - First 4 bytes of string as prefix (4 bytes)
///   - Buffer index and offset (8 bytes total)
///
/// The following ASCII graphic is reproduced verbatim from the Arrow documentation:
///
/// ```text
///                         ┌──────┬────────────────────────┐
///                         │length│      string value      │
///    Strings (len <= 12)  │      │    (padded with 0)     │
///                         └──────┴────────────────────────┘
///                          0    31                      127
///
///                         ┌───────┬───────┬───────┬───────┐
///                         │length │prefix │  buf  │offset │
///    Strings (len > 12)   │       │       │ index │       │
///                         └───────┴───────┴───────┴───────┘
///                          0    31       63      95    127
/// ```
///
/// # Examples
///
/// ```
/// use vortex_array::arrays::VarBinViewArray;
/// use vortex_array::dtype::{DType, Nullability};
/// use vortex_array::IntoArray;
///
/// // Create from an Iterator<Item = &str>
/// let array = VarBinViewArray::from_iter_str([
///         "inlined",
///         "this string is outlined"
/// ]);
///
/// assert_eq!(array.len(), 2);
///
/// // Access individual strings
/// let first = array.bytes_at(0);
/// assert_eq!(first.as_slice(), b"inlined"); // "short"
///
/// let second = array.bytes_at(1);
/// assert_eq!(second.as_slice(), b"this string is outlined"); // Long string
/// ```
#[derive(Clone, Debug)]
pub struct VarBinViewData {
    pub(super) buffers: Arc<[BufferHandle]>,
    pub(super) views: BufferHandle,
    // The value type under which these immutable views were constructed.
    // Opaque device contents still require validation on host replacement.
    pub(super) utf8_values: bool,
    // Unlike the construction dtype, this receipt is established only by the
    // reviewed checked paths below. Public unchecked constructors and generic
    // builders do not establish it. Device bytes remain opaque until replaced.
    pub(super) utf8_validated: bool,
    // Keep last: each managed table reference must drop before its shared credit.
    pub(super) table_owner: Option<Arc<MetadataReservation>>,
}

impl Display for VarBinViewData {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

pub struct VarBinViewDataParts {
    pub dtype: DType,
    pub buffers: Arc<[BufferHandle]>,
    pub views: BufferHandle,
    pub validity: Validity,
}

// Walk invalid (null) runs of "mask" as [start, end) ranges
fn for_each_invalid_range(mask: &Mask, len: usize, mut f: impl FnMut(usize, usize)) {
    match mask.bit_buffer() {
        AllOr::All => {}
        AllOr::None => f(0, len),
        AllOr::Some(buffer) => {
            let mut prev = 0;
            for (start, end) in buffer.set_slices() {
                if start > prev {
                    f(prev, start);
                }
                prev = end;
            }
            if prev < len {
                f(prev, len);
            }
        }
    }
}

/// Calls `f` for each valid index of `mask`, and stops at the first error.
fn try_for_each_valid(
    mask: &Mask,
    mut f: impl FnMut(usize) -> VortexResult<()>,
) -> VortexResult<()> {
    match mask.bit_buffer() {
        AllOr::All => (0..mask.len()).try_for_each(f),
        AllOr::None => Ok(()),
        AllOr::Some(buffer) => buffer.set_indices().try_for_each(&mut f),
    }
}

/// Returns `true` if all the inlined bytes of `view` are ASCII.
fn is_inlined_ascii(view: &BinaryView) -> bool {
    const HIGH_BITS: u128 = u128::from_le_bytes([0x80; 16]);

    // The first 4 bytes hold the length, and the inlined bytes follow.
    let data_bits = ((1u128 << (8 * view.len())) - 1) << 32;
    view.as_u128() & data_bits & HIGH_BITS == 0
}

/// Returns `true` if `byte` starts a UTF-8 char, that is, it is not a continuation byte.
fn is_char_boundary(byte: u8) -> bool {
    // Continuation bytes have the form `0b10xx_xxxx`.
    byte & 0b1100_0000 != 0b1000_0000
}

impl VarBinViewData {
    fn dtype_parts(dtype: &DType) -> VortexResult<(bool, Nullability)> {
        match dtype {
            DType::Utf8(nullability) => Ok((true, *nullability)),
            DType::Binary(nullability) => Ok((false, *nullability)),
            _ => vortex_bail!(InvalidArgument: "invalid DType {dtype} for `VarBinViewArray`"),
        }
    }

    /// Build the slots vector for this array.
    pub(super) fn make_slots(validity: &Validity, len: usize) -> ArraySlots {
        smallvec![validity_to_child(validity, len)]
    }

    /// Creates a new `VarBinViewArray`.
    ///
    /// # Panics
    ///
    /// Panics if the provided components do not satisfy the invariants documented
    /// in `VarBinViewArray::new_unchecked`.
    pub fn new(
        views: Buffer<BinaryView>,
        buffers: Arc<[ByteBuffer]>,
        dtype: DType,
        validity: Validity,
        ctx: &mut ExecutionCtx,
    ) -> Self {
        Self::try_new(views, buffers, dtype, validity, ctx)
            .vortex_expect("VarBinViewArray construction failed")
    }

    /// Creates a new `VarBinViewArray` with device or host memory.
    ///
    /// # Panics
    ///
    /// Panics if the provided components do not satisfy the invariants documented
    /// in `VarBinViewArray::new_unchecked`.
    pub fn new_handle(
        views: BufferHandle,
        buffers: Arc<[BufferHandle]>,
        dtype: DType,
        validity: Validity,
    ) -> Self {
        Self::try_new_handle(views, buffers, dtype, validity)
            .vortex_expect("VarbinViewArray construction failed")
    }

    /// Constructs a new `VarBinViewArray`.
    ///
    /// See `VarBinViewArray::new_unchecked` for more information.
    ///
    /// # Errors
    ///
    /// Returns an error if the provided components do not satisfy the invariants documented in
    /// `VarBinViewArray::new_unchecked`.
    pub fn try_new(
        views: Buffer<BinaryView>,
        buffers: Arc<[ByteBuffer]>,
        dtype: DType,
        validity: Validity,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Self> {
        Self::try_new_from_buffers(views, &buffers, dtype, validity, ctx)
    }

    /// Validate host views and create an admitted handle table from borrowed
    /// buffers. The caller retains ownership of the input collection.
    pub fn try_new_from_buffers(
        views: Buffer<BinaryView>,
        buffers: &[ByteBuffer],
        dtype: DType,
        validity: Validity,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Self> {
        vortex_ensure!(
            validity.maybe_len().is_none_or(|len| len == views.len()),
            "VarBinView validity length differs from its views"
        );
        let views = Self::validate_and_fix(views, buffers, &dtype, &validity, ctx)?;
        // SAFETY: validation checked every non-null value against these exact
        // buffers and replaced null slots with valid empty views.
        unsafe {
            Self::try_new_handle_in_unchecked(
                BufferHandle::new_host(views.into_byte_buffer()),
                buffers.iter().cloned().map(BufferHandle::new_host),
                dtype,
                validity,
                &ctx.host_allocator(),
            )
        }
    }

    /// Construct an admitted table from host or device handles. Host views are
    /// checked against their buffers, including prefixes and UTF-8 contents.
    /// Null slots must also contain valid views because raw value access does
    /// not consult validity. Device contents remain opaque until host replacement,
    /// which performs the same validation without an implicit device copy here.
    /// The iterator's requested capacity and the final Arc slice overlap until
    /// conversion completes. Raw exported buffers do not carry this table credit.
    pub fn try_new_handle_in(
        views: BufferHandle,
        buffers: impl ExactSizeIterator<Item = BufferHandle>,
        dtype: DType,
        validity: Validity,
        allocator: &HostAllocatorRef,
    ) -> VortexResult<Self> {
        Self::check_handle_shape(&views, &dtype, &validity)?;
        let (buffers, owner) = MetadataVec::from_iter(buffers, allocator)?.into_arc()?;
        Self::from_admitted_handles(views, buffers, owner, dtype, validity)
    }

    /// Admit a table whose value contents have already been validated.
    ///
    /// # Safety
    ///
    /// Every host-readable view, including null slots, must satisfy the value
    /// invariants of `new_handle_unchecked` against these exact buffers and dtype.
    pub(crate) unsafe fn try_new_handle_in_unchecked(
        views: BufferHandle,
        buffers: impl ExactSizeIterator<Item = BufferHandle>,
        dtype: DType,
        validity: Validity,
        allocator: &HostAllocatorRef,
    ) -> VortexResult<Self> {
        Self::check_handle_shape(&views, &dtype, &validity)?;
        let (buffers, owner) = MetadataVec::from_iter(buffers, allocator)?.into_arc()?;
        // SAFETY: the caller established value invariants; table conversion
        // preserves the handles and their order, with all storage admitted.
        let utf8_validated = matches!(dtype, DType::Utf8(_));
        let mut data = unsafe {
            Self::from_admitted_handles_unchecked(views, buffers, owner, dtype, validity)?
        };
        // This crate-private helper's callers establish value validity with
        // validate_and_fix or the checked UTF-8 heap/character-boundary builder.
        data.utf8_validated = utf8_validated;
        Ok(data)
    }

    /// Check a dense UTF-8 heap once, then validate views against those exact immutable bytes.
    /// Inline values remain independently checked: an allocator's freeze operation cannot be
    /// used as evidence that previously written view or payload bytes stayed unchanged.
    pub(crate) fn try_new_utf8_buffer_in(
        views: Buffer<BinaryView>,
        buffer: ByteBuffer,
        validity: Validity,
        allocator: &HostAllocatorRef,
    ) -> VortexResult<Self> {
        let dtype = DType::Utf8(validity.nullability());
        let view_handle = BufferHandle::new_host(views.clone().into_byte_buffer());
        Self::check_handle_shape(&view_handle, &dtype, &validity)?;
        let text = simdutf8::basic::from_utf8(buffer.as_slice())
            .map_err(|_| vortex_err!("VarBinView text heap failed utf-8 validation"))?;
        let buffer_count = usize::from(!buffer.is_empty());
        for (index, view) in views.iter().enumerate() {
            Self::validate_view_with(
                index,
                view,
                buffer_count,
                |_| (buffer.len(), Some(buffer.as_slice())),
                true,
                Some(text),
            )?;
        }
        let buffer = (!buffer.is_empty()).then(|| BufferHandle::new_host(buffer));
        // SAFETY: shape, dtype, validity and every immutable view were checked above. Outlined
        // values reference this exact validated heap at character boundaries with matching
        // prefixes; inline values, including null slots, passed independent UTF-8 validation.
        unsafe {
            Self::try_new_handle_in_unchecked(
                view_handle,
                buffer.into_iter(),
                dtype,
                validity,
                allocator,
            )
        }
    }

    pub(crate) fn from_admitted_handles(
        views: BufferHandle,
        buffers: Arc<[BufferHandle]>,
        owner: Arc<MetadataReservation>,
        dtype: DType,
        validity: Validity,
    ) -> VortexResult<Self> {
        Self::try_new_handle(views, buffers, dtype, validity)?.with_table_owner(owner)
    }

    /// Attach an admitted table without re-reading already validated values.
    ///
    /// # Safety
    ///
    /// The view/value contents must satisfy `new_handle_unchecked`, including
    /// null slots. Shape, dtype, validity length and table credit remain checked.
    pub(crate) unsafe fn from_admitted_handles_unchecked(
        views: BufferHandle,
        buffers: Arc<[BufferHandle]>,
        owner: Arc<MetadataReservation>,
        dtype: DType,
        validity: Validity,
    ) -> VortexResult<Self> {
        Self::check_handle_shape(&views, &dtype, &validity)?;
        // SAFETY: the caller established contents; the remaining structural
        // requirements were checked above.
        unsafe { Self::new_handle_unchecked(views, buffers, dtype, validity) }
            .with_table_owner(owner)
    }

    fn with_table_owner(mut self, owner: Arc<MetadataReservation>) -> VortexResult<Self> {
        vortex_ensure!(
            owner.bytes
                == self
                    .buffers
                    .len()
                    .checked_mul(size_of::<BufferHandle>())
                    .ok_or_else(|| vortex_err!("VarBinView metadata capacity overflow"))?,
            "VarBinView retained table differs from its admission"
        );
        self.table_owner = Some(owner);
        Ok(self)
    }

    /// Constructs a new `VarBinViewArray`.
    ///
    /// See `VarBinViewArray::new_unchecked` for more information.
    ///
    /// # Errors
    ///
    /// Returns an error if the provided components do not satisfy the invariants documented in
    /// `VarBinViewArray::new_unchecked`.
    pub fn try_new_handle(
        views: BufferHandle,
        buffers: Arc<[BufferHandle]>,
        dtype: DType,
        validity: Validity,
    ) -> VortexResult<Self> {
        Self::validate_handle_contents(&views, &buffers, &dtype, &validity)?;

        // SAFETY: host values and structure have been validated. Device contents
        // cannot be accessed on the host; replacing them with host buffers checks
        // the contents again through this constructor.
        let utf8_validated = matches!(dtype, DType::Utf8(_));
        let mut data = unsafe { Self::new_handle_unchecked(views, buffers, dtype, validity) };
        data.utf8_validated = utf8_validated;
        Ok(data)
    }

    pub(super) fn validate_handle_contents(
        views: &BufferHandle,
        buffers: &[BufferHandle],
        dtype: &DType,
        validity: &Validity,
    ) -> VortexResult<()> {
        let check_utf8 = Self::check_handle_shape(views, dtype, validity)?;
        let Some(host_views) = views.as_host_opt() else {
            // This path retains opaque device handles only. It does not read,
            // copy, or certify their contents for execution on the host.
            return Ok(());
        };
        let host_views = Buffer::<BinaryView>::from_byte_buffer(host_views.clone());
        // Short outlined strings otherwise invoke scalar UTF-8 validation once
        // per row. For a dense, single host buffer, validate the heap together
        // and then check each view's character boundaries below. Bound this
        // extra scan to 64 bytes per view so a sparse slice cannot cause an
        // unbounded read of unrelated payload. No table or copied bytes are
        // needed. Invalid unused bytes (including codec record headers) merely
        // disable this optimization; the ordinary per-value checks still apply.
        let validated_utf8 = if check_utf8 && buffers.len() == 1 && host_views.len() >= 64 {
            buffers[0]
                .as_host_opt()
                .filter(|buffer| buffer.len() <= host_views.len().saturating_mul(64))
                .and_then(|buffer| simdutf8::basic::from_utf8(buffer.as_slice()).ok())
        } else {
            None
        };
        for (idx, view) in host_views.iter().enumerate() {
            Self::validate_view_with(
                idx,
                view,
                buffers.len(),
                |index| {
                    let buffer = &buffers[index];
                    (buffer.len(), buffer.as_host_opt().map(ByteBuffer::as_slice))
                },
                check_utf8,
                validated_utf8,
            )?;
        }
        Ok(())
    }

    pub(crate) fn check_handle_shape(
        views: &BufferHandle,
        dtype: &DType,
        validity: &Validity,
    ) -> VortexResult<bool> {
        Self::check_view_layout(views)?;
        let check_utf8 = Self::check_nullability(dtype, validity)?;
        vortex_ensure!(
            validity
                .maybe_len()
                .is_none_or(|len| len == views.len() / size_of::<BinaryView>()),
            "VarBinView validity length differs from its views"
        );
        Ok(check_utf8)
    }

    pub(super) fn check_view_layout(views: &BufferHandle) -> VortexResult<()> {
        let views_nbytes = views.len();
        vortex_ensure!(
            views_nbytes.is_multiple_of(size_of::<BinaryView>()),
            "Expected views buffer length ({views_nbytes}) to be a multiple of {}",
            size_of::<BinaryView>()
        );

        // TODO(aduffy): device validation.
        if let Some(host) = views.as_host_opt() {
            vortex_ensure!(
                host.is_aligned(Alignment::of::<BinaryView>()),
                "Views on host must be 16 byte aligned"
            );
        }

        Ok(())
    }

    /// Creates a new `VarBinViewArray` without validation from these components:
    ///
    /// * `views` is a buffer of 16-byte view entries (one per logical element).
    /// * `buffers` contains the backing storage for strings longer than 12 bytes.
    /// * `dtype` specifies whether this contains UTF-8 strings or binary data.
    /// * `validity` holds the null values.
    ///
    /// # Safety
    ///
    /// The caller must ensure all of the following invariants are satisfied:
    ///
    /// ## View Requirements
    ///
    /// - Views must be properly formatted 16-byte [`BinaryView`] entries.
    /// - Inlined views (length ≤ 12) must have valid data in the first `length` bytes.
    /// - Reference views (length > 12) must:
    ///   - Have a valid buffer index < `buffers.len()`.
    ///   - Have valid offsets that don't exceed the referenced buffer's bounds.
    ///   - Have a 4-byte prefix that matches the actual data at the referenced location.
    ///
    /// ## Type Requirements
    ///
    /// - `dtype` must be either [`DType::Utf8`] or [`DType::Binary`].
    /// - For [`DType::Utf8`], all string data (both inlined and referenced) must be valid UTF-8.
    ///
    /// ## Validity Requirements
    ///
    /// - The validity must have the same nullability as the dtype.
    /// - If validity is an array, its length must match `views.len()`.
    pub unsafe fn new_unchecked(
        views: Buffer<BinaryView>,
        buffers: Arc<[ByteBuffer]>,
        dtype: DType,
        validity: Validity,
    ) -> Self {
        let handles: Vec<BufferHandle> = buffers
            .iter()
            .cloned()
            .map(BufferHandle::new_host)
            .collect();

        let handles = Arc::from(handles);
        let view_handle = BufferHandle::new_host(views.into_byte_buffer());
        unsafe { Self::new_handle_unchecked(view_handle, handles, dtype, validity) }
    }

    /// Construct a new array from `BufferHandle`s without validation.
    ///
    /// # Safety
    ///
    /// See documentation in `new_unchecked`.
    pub unsafe fn new_handle_unchecked(
        views: BufferHandle,
        buffers: Arc<[BufferHandle]>,
        dtype: DType,
        _validity: Validity,
    ) -> Self {
        let (utf8_values, _) =
            Self::dtype_parts(&dtype).vortex_expect("VarBinViewArray dtype must be utf8 or binary");
        Self {
            buffers,
            views,
            utf8_values,
            utf8_validated: false,
            table_owner: None,
        }
    }

    /// Validates the components that would be used to create a `VarBinViewArray`.
    ///
    /// This function checks all the invariants required by `VarBinViewArray::new_unchecked`,
    /// validating only the views at non-null slots.
    pub fn validate(
        views: &Buffer<BinaryView>,
        buffers: &[ByteBuffer],
        dtype: &DType,
        validity: &Validity,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        let check_utf8 = Self::check_nullability(dtype, validity)?;
        let mask = validity.execute_mask(views.len(), ctx)?;
        Self::validate_views(views, &mask, buffers, check_utf8, &ctx.host_allocator())
    }

    /// Validates components like validate() and replaces views at null slots to empty views
    pub(crate) fn validate_and_fix(
        views: Buffer<BinaryView>,
        buffers: &[ByteBuffer],
        dtype: &DType,
        validity: &Validity,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Buffer<BinaryView>> {
        let check_utf8 = Self::check_nullability(dtype, validity)?;
        let mask = validity.execute_mask(views.len(), ctx)?;

        // With nulls and views we own, validate and replace in a single pass: a second pass over
        // many short null runs costs more than a branch per view.
        let views = match (mask.bit_buffer(), views.try_into_mut()) {
            (AllOr::Some(bits), Ok(mut views)) => {
                let buffers_utf8 =
                    Self::buffers_utf8(&mask, buffers, check_utf8, &ctx.host_allocator())?;
                let buffers_utf8 = buffers_utf8.as_deref();
                let empty = BinaryView::empty_view();
                let slice = views.as_mut_slice();
                for (idx, (view, valid)) in slice.iter_mut().zip(bits.iter()).enumerate() {
                    if valid {
                        Self::validate_view(idx, view, buffers, buffers_utf8)?;
                    } else {
                        *view = empty;
                    }
                }
                return Ok(views.freeze());
            }
            (_, Ok(views)) => views.freeze(),
            (_, Err(views)) => views,
        };

        Self::validate_views(&views, &mask, buffers, check_utf8, &ctx.host_allocator())?;
        Ok(Self::replace_null_views(views, &mask))
    }

    fn check_nullability(dtype: &DType, validity: &Validity) -> VortexResult<bool> {
        let (is_utf8, nullability) = Self::dtype_parts(dtype)?;
        vortex_ensure_eq!(
            validity.nullability(),
            nullability,
            InvalidArgument: "validity nullability is incompatible with dtype nullability",
        );
        Ok(is_utf8)
    }

    /// Replaces the views at null slots with empty views. Copies the views only if a null slot
    /// holds a non-empty view and the buffer is shared.
    fn replace_null_views(views: Buffer<BinaryView>, mask: &Mask) -> Buffer<BinaryView> {
        if mask.all_true() {
            return views;
        }

        let empty = BinaryView::empty_view();
        let len = views.len();

        let mut views = match views.try_into_mut() {
            Ok(views) => views,
            Err(views) => {
                let mut needs_replace = false;
                for_each_invalid_range(mask, len, |start, end| {
                    needs_replace |= views[start..end].iter().any(|view| *view != empty);
                });
                if !needs_replace {
                    return views;
                }
                views.into_mut()
            }
        };

        let slice = views.as_mut_slice();
        for_each_invalid_range(mask, len, |start, end| slice[start..end].fill(empty));
        views.freeze()
    }

    /// Validates the views at the valid slots of `mask`.
    ///
    /// A per-view UTF-8 check costs a function call per view, which dominates for short strings.
    /// To avoid it, each data buffer is first checked once as a whole. If a buffer is valid UTF-8,
    /// a view into it is valid UTF-8 if and only if it starts and ends on a char boundary.
    ///
    /// A buffer can hold bytes that no valid view references, for example after a slice, after a
    /// filter, or at null views. So the whole-buffer check is only done while the total size of
    /// the checked buffers stays within a fixed cost per valid view. The views into a buffer that
    /// is not checked, or that is not valid UTF-8, are checked one by one.
    fn validate_views(
        views: &[BinaryView],
        mask: &Mask,
        buffers: &[ByteBuffer],
        check_utf8: bool,
        allocator: &HostAllocatorRef,
    ) -> VortexResult<()> {
        let buffers_utf8 = Self::buffers_utf8(mask, buffers, check_utf8, allocator)?;
        let buffers_utf8 = buffers_utf8.as_deref();

        try_for_each_valid(mask, |idx| {
            Self::validate_view(idx, &views[idx], buffers, buffers_utf8)
        })
    }

    /// The approximate cost of a per-view UTF-8 check, in bytes of a whole-buffer check.
    const PER_VIEW_UTF8_COST: usize = 64;

    /// Returns `None` if the UTF-8 check is off. Otherwise, returns for each buffer `true` if the
    /// whole buffer was checked and is valid UTF-8.
    ///
    /// The budget only counts a fixed cost per valid view, not the bytes the views reference: for
    /// long strings, the call per view is cheap next to the string, and checking a buffer larger
    /// than the cache and then reading it again for the char boundaries is slower.
    fn buffers_utf8(
        mask: &Mask,
        buffers: &[ByteBuffer],
        check_utf8: bool,
        allocator: &HostAllocatorRef,
    ) -> VortexResult<Option<MetadataVec<bool>>> {
        if !check_utf8 {
            return Ok(None);
        }

        let mut budget = mask.true_count().saturating_mul(Self::PER_VIEW_UTF8_COST);

        MetadataVec::from_iter(
            buffers.iter().map(|buf| {
                if buf.len() > budget {
                    return false;
                }

                budget -= buf.len();
                simdutf8::basic::from_utf8(buf).is_ok()
            }),
            allocator,
        )
        .map(Some)
    }

    /// Checks the bounds, the prefix, and the UTF-8 of a view.
    ///
    /// `buffers_utf8` is `None` if the UTF-8 check is off. Otherwise, it tells which buffers are
    /// valid UTF-8 as a whole.
    fn validate_view(
        idx: usize,
        view: &BinaryView,
        buffers: &[ByteBuffer],
        buffers_utf8: Option<&[bool]>,
    ) -> VortexResult<()> {
        if view.is_inlined() {
            if buffers_utf8.is_some() && !is_inlined_ascii(view) {
                let bytes = view.as_inlined().value();
                vortex_ensure!(
                    simdutf8::basic::from_utf8(bytes).is_ok(),
                    InvalidArgument: "view at index {idx}: inlined bytes failed utf-8 validation"
                );
            }
            return Ok(());
        }

        let view = view.as_view();
        let buf_index = view.buffer_index as usize;
        let start_offset = view.offset as usize;
        let end_offset = start_offset.saturating_add(view.size as usize);

        let buf = buffers.get(buf_index).ok_or_else(||
            vortex_err!(InvalidArgument: "view at index {idx} references invalid buffer: {buf_index} out of bounds for VarBinViewData with {} buffers",
                buffers.len()))?;

        vortex_ensure!(
            start_offset < buf.len(),
            InvalidArgument: "start offset {start_offset} out of bounds for buffer {buf_index} with size {}",
            buf.len(),
        );

        vortex_ensure!(
            end_offset <= buf.len(),
            InvalidArgument: "end offset {end_offset} out of bounds for buffer {buf_index} with size {}",
            buf.len(),
        );

        // Make sure the prefix data matches the buffer data.
        vortex_ensure!(
            view.prefix == buf[start_offset..start_offset + 4],
            InvalidArgument: "VarBinView prefix does not match full string"
        );

        let Some(buffers_utf8) = buffers_utf8 else {
            return Ok(());
        };

        let valid_utf8 = if buffers_utf8[buf_index] {
            is_char_boundary(buf[start_offset])
                && (end_offset == buf.len() || is_char_boundary(buf[end_offset]))
        } else {
            simdutf8::basic::from_utf8(&buf[start_offset..end_offset]).is_ok()
        };

        vortex_ensure!(
            valid_utf8,
            InvalidArgument: "view at index {idx}: outlined bytes fails utf-8 validation"
        );

        Ok(())
    }

    fn validate_view_with<'a>(
        idx: usize,
        view: &BinaryView,
        buffer_count: usize,
        buffer_at: impl FnOnce(usize) -> (usize, Option<&'a [u8]>),
        check_utf8: bool,
        validated_utf8: Option<&str>,
    ) -> VortexResult<()> {
        let valid_utf8 = |bytes: &[u8]| !check_utf8 || simdutf8::basic::from_utf8(bytes).is_ok();
        if view.is_inlined() {
            // Validate the inline bytestring
            let bytes = &view.as_inlined().data[..view.len() as usize];
            vortex_ensure!(
                valid_utf8(bytes),
                InvalidArgument: "view at index {idx}: inlined bytes failed utf-8 validation"
            );
        } else {
            // Validate the view pointer
            let view = view.as_view();
            let buf_index = view.buffer_index as usize;
            let start_offset = view.offset as usize;
            let end_offset = view.offset.checked_add(view.size).ok_or_else(
                || vortex_err!(InvalidArgument: "view at index {idx}: end offset exceeds u32"),
            )? as usize;

            vortex_ensure!(
                buf_index < buffer_count,
                InvalidArgument: "view at index {idx} references invalid buffer: {buf_index} out of bounds for VarBinViewData with {buffer_count} buffers"
            );
            let (buffer_len, bytes) = buffer_at(buf_index);

            vortex_ensure!(
                start_offset < buffer_len,
                InvalidArgument: "start offset {start_offset} out of bounds for buffer {buf_index} with size {buffer_len}",
            );

            vortex_ensure!(
                end_offset <= buffer_len,
                InvalidArgument: "end offset {end_offset} out of bounds for buffer {buf_index} with size {buffer_len}",
            );

            let Some(bytes) = bytes else {
                // Device bytes cannot be read here. A later host replacement
                // must pass this validation before any host value access.
                return Ok(());
            };
            // Make sure the prefix data matches the buffer data.
            let bytes = &bytes[start_offset..end_offset];
            vortex_ensure!(
                view.prefix == bytes[..4],
                InvalidArgument: "VarBinView prefix does not match full string"
            );

            // Validate the full string
            // `validated_utf8`, when present, is this exact single buffer.
            // A substring of valid UTF-8 is valid iff both endpoints are
            // character boundaries; all index, range and prefix checks above
            // remain necessary, as do independent checks for inlined values.
            let value_is_utf8 = validated_utf8.map_or_else(
                || valid_utf8(bytes),
                |text| text.is_char_boundary(start_offset) && text.is_char_boundary(end_offset),
            );
            vortex_ensure!(
                value_is_utf8,
                InvalidArgument: "view at index {idx}: outlined bytes fails utf-8 validation"
            );
        }
        Ok(())
    }

    /// Returns the length of this array.
    pub fn len(&self) -> usize {
        self.views.len() / size_of::<BinaryView>()
    }

    /// Returns `true` if this array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Access to the primitive views buffer.
    ///
    /// Variable-sized binary view buffer contain a "view" child array, with 16-byte entries that
    /// contain either a pointer into one of the array's owned `buffer`s OR an inlined copy of
    /// the string (if the string has 12 bytes or fewer).
    #[inline]
    pub fn views(&self) -> &[BinaryView] {
        let host_views = self.views.as_host();
        let len = host_views.len() / size_of::<BinaryView>();

        // SAFETY: data alignment is checked for host buffers on construction
        unsafe { std::slice::from_raw_parts(host_views.as_ptr().cast(), len) }
    }

    /// Return the buffer handle backing the views.
    pub fn views_handle(&self) -> &BufferHandle {
        &self.views
    }

    /// Access value bytes at a given index
    ///
    /// Will return a `ByteBuffer` containing the data without performing a copy.
    #[inline]
    pub fn bytes_at(&self, index: usize) -> ByteBuffer {
        let views = self.views();
        let view = &views[index];
        // Expect this to be the common case: strings > 12 bytes.
        if !view.is_inlined() {
            let view_ref = view.as_view();
            self.buffer(view_ref.buffer_index as usize)
                .slice(view_ref.as_range())
        } else {
            // Return access to the range of bytes around it.
            self.views_handle()
                .as_host()
                .clone()
                .into_byte_buffer()
                .slice_ref(view.as_inlined().value())
        }
    }

    #[inline]
    fn checked_host_bytes_at(&self, index: usize) -> VortexResult<&[u8]> {
        vortex_ensure!(
            self.views.as_host_opt().is_some(),
            InvalidArgument: "VarBinView text access requires host views"
        );
        let view = self.views().get(index).ok_or_else(
            || vortex_err!(InvalidArgument: "VarBinView text index {index} is out of bounds"),
        )?;
        if view.is_inlined() {
            return Ok(view.as_inlined().value());
        }
        let reference = view.as_view();
        let buffer = self
            .buffers
            .get(reference.buffer_index as usize)
            .and_then(BufferHandle::as_host_opt)
            .ok_or_else(|| {
                vortex_err!(InvalidArgument: "VarBinView text access requires a host payload buffer")
            })?;
        buffer
            .get(reference.as_range())
            .ok_or_else(|| vortex_err!(InvalidArgument: "VarBinView text range is out of bounds"))
    }

    /// Access one of the backing data buffers.
    ///
    /// # Panics
    ///
    /// This method panics if the provided index is out of bounds for the set of buffers provided
    /// at construction time.
    #[inline]
    pub fn buffer(&self, idx: usize) -> &ByteBuffer {
        if idx >= self.data_buffers().len() {
            vortex_panic!(
                "{idx} buffer index out of bounds, there are {} buffers",
                self.data_buffers().len()
            );
        }
        self.buffers[idx].as_host()
    }

    /// The underlying raw data buffers, not including the views buffer.
    /// Cloning this raw Arc exports caller-owned storage; use the owner-preserving
    /// array reconstruction method for managed native transformations.
    #[inline]
    pub fn data_buffers(&self) -> &Arc<[BufferHandle]> {
        &self.buffers
    }

    /// Retained requested table element bytes, excluding payloads and Arc headers.
    pub fn metadata_capacity_bytes(&self) -> usize {
        self.table_owner.as_ref().map_or(0, |owner| owner.bytes)
    }

    /// Accumulate an iterable set of values into our type here.
    #[expect(
        clippy::same_name_method,
        reason = "intentionally named from_iter like Iterator::from_iter"
    )]
    pub fn from_iter<T: AsRef<[u8]>, I: IntoIterator<Item = Option<T>>>(
        iter: I,
        dtype: DType,
    ) -> Self {
        let iter = iter.into_iter();
        let mut builder = VarBinViewBuilder::with_capacity_in(
            dtype,
            iter.size_hint().0,
            BufferAllocatorRef::statically_allocated(),
        );

        for item in iter {
            match item {
                None => builder.append_null(),
                Some(v) => builder.append_value(v),
            }
        }

        builder.finish_into_varbinview().into_data()
    }

    pub fn from_iter_str<T: AsRef<str>, I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = VarBinViewBuilder::with_capacity_in(
            DType::Utf8(Nullability::NonNullable),
            iter.size_hint().0,
            BufferAllocatorRef::statically_allocated(),
        );

        for item in iter {
            builder.append_value(item.as_ref());
        }

        builder.finish_into_varbinview().into_data()
    }

    pub fn from_iter_nullable_str<T: AsRef<str>, I: IntoIterator<Item = Option<T>>>(
        iter: I,
    ) -> Self {
        let iter = iter.into_iter();
        let mut builder = VarBinViewBuilder::with_capacity_in(
            DType::Utf8(Nullability::Nullable),
            iter.size_hint().0,
            BufferAllocatorRef::statically_allocated(),
        );

        for item in iter {
            match item {
                None => builder.append_null(),
                Some(v) => builder.append_value(v.as_ref()),
            }
        }

        builder.finish_into_varbinview().into_data()
    }

    pub fn from_iter_bin<T: AsRef<[u8]>, I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = VarBinViewBuilder::with_capacity_in(
            DType::Binary(Nullability::NonNullable),
            iter.size_hint().0,
            BufferAllocatorRef::statically_allocated(),
        );

        for item in iter {
            builder.append_value(item.as_ref());
        }

        builder.finish_into_varbinview().into_data()
    }

    pub fn from_iter_nullable_bin<T: AsRef<[u8]>, I: IntoIterator<Item = Option<T>>>(
        iter: I,
    ) -> Self {
        let iter = iter.into_iter();
        let mut builder = VarBinViewBuilder::with_capacity_in(
            DType::Binary(Nullability::Nullable),
            iter.size_hint().0,
            BufferAllocatorRef::statically_allocated(),
        );

        for item in iter {
            match item {
                None => builder.append_null(),
                Some(v) => builder.append_value(v.as_ref()),
            }
        }

        builder.finish_into_varbinview().into_data()
    }
}

pub trait VarBinViewArrayExt: TypedArrayRef<VarBinView> {
    /// Borrow a physical UTF-8 value without cloning its payload owner.
    ///
    /// Checked native construction can reuse its immutable value validation.
    /// Other construction paths validate the selected bytes here. Dtype, bounds
    /// and host residency remain checked. This does not consult null validity;
    /// callers must handle nulls before treating the physical value as present.
    #[inline]
    fn utf8_slice_at(&self, index: usize) -> VortexResult<&str> {
        vortex_ensure!(
            matches!(self.as_ref().dtype(), DType::Utf8(_)),
            InvalidArgument: "VarBinView text access requires a UTF-8 dtype"
        );
        let bytes = self.checked_host_bytes_at(index)?;
        if self.utf8_validated {
            // SAFETY: reviewed construction validated every host-readable value,
            // including null slots, against these immutable views and payloads.
            // Selection and byte-identical replacement retain that receipt only
            // for unchanged values. The selected view/payload are on the host,
            // in bounds, and the outer logical dtype was checked above.
            Ok(unsafe { std::str::from_utf8_unchecked(bytes) })
        } else {
            simdutf8::basic::from_utf8(bytes).or_else(|_| {
                std::str::from_utf8(bytes)
                    .map_err(|detail| vortex_err!("invalid utf-8: {detail} at index {index}"))
            })
        }
    }

    /// Retain a physical UTF-8 value with the same payload owner as `bytes_at`.
    ///
    /// This copies no value bytes and retains no unrelated buffer-table or
    /// outer-array owner. Null validity is the caller's responsibility, as for
    /// [`Self::utf8_slice_at`]. Dtype, host residency and bounds errors are returned.
    #[inline]
    fn utf8_buffer_at(&self, index: usize) -> VortexResult<BufferString> {
        self.utf8_slice_at(index)?;
        // SAFETY: utf8_slice_at established UTF-8 for this exact immutable value.
        // bytes_at retains precisely those bytes, including inline view storage.
        Ok(unsafe { BufferString::new_unchecked(self.bytes_at(index)) })
    }

    /// Replace views, dtype nullability and validity while sharing the original
    /// table and its owner. Host views are validated against the original buffers,
    /// including null slots, so changing validity cannot expose malformed values.
    /// This preserves any independent outer array lifetime owner as well.
    fn with_views_and_validity(
        &self,
        views: BufferHandle,
        dtype: DType,
        validity: Validity,
    ) -> VortexResult<Array<VarBinView>> {
        vortex_ensure!(
            self.as_ref().dtype().eq_ignore_nullability(&dtype),
            "VarBinView reconstruction cannot change its value type"
        );
        let same_host_views = views
            .as_host_opt()
            .zip(self.views.as_host_opt())
            .is_some_and(|(new, original)| new.as_slice() == original.as_slice());
        if !same_host_views {
            VarBinViewData::validate_handle_contents(&views, &self.buffers, &dtype, &validity)?;
        }
        // SAFETY: either the bytes equal the original valid views against the
        // unchanged payload table, or all newly readable contents were checked.
        // Shape and validity are checked again by the reconstruction helper.
        unsafe { self.with_views_and_validity_unchecked(views, dtype, validity) }
    }

    /// Reconstruct a proven selection of valid views while retaining table credit.
    /// Shape, dtype, validity and the outer array remain checked.
    ///
    /// # Safety
    ///
    /// Every host-readable view, including null slots, must satisfy the value
    /// invariants of `VarBinViewData::new_handle_unchecked` against this array's
    /// unchanged payload table and value type. Native kernels may copy, repeat
    /// or slice valid views, or introduce valid empty views; arbitrary caller
    /// input must use `with_views_and_validity` instead.
    unsafe fn with_views_and_validity_unchecked(
        &self,
        views: BufferHandle,
        dtype: DType,
        validity: Validity,
    ) -> VortexResult<Array<VarBinView>> {
        vortex_ensure!(
            self.as_ref().dtype().eq_ignore_nullability(&dtype),
            "VarBinView reconstruction cannot change its value type"
        );
        VarBinViewData::check_handle_shape(&views, &dtype, &validity)?;
        let len = views.len() / size_of::<BinaryView>();
        let data = VarBinViewData {
            buffers: Arc::clone(&self.buffers),
            views,
            utf8_values: matches!(dtype, DType::Utf8(_)),
            utf8_validated: self.utf8_validated && matches!(dtype, DType::Utf8(_)),
            table_owner: self.table_owner.clone(),
        };
        let mut parts = ArrayParts::new(
            VarBinView,
            dtype,
            len,
            data,
            VarBinViewData::make_slots(&validity, len),
        );
        parts.retained_owner = self.as_ref().dyn_array().retained_owner().cloned();
        Array::try_from_parts(parts)
    }

    fn dtype_parts(&self) -> (bool, Nullability) {
        match self.dtype() {
            DType::Utf8(nullability) => (true, *nullability),
            DType::Binary(nullability) => (false, *nullability),
            _ => unreachable!("VarBinViewArrayExt requires a utf8 or binary dtype"),
        }
    }

    fn varbinview_validity(&self) -> Validity {
        child_to_validity(
            self.slots()[VarBinViewSlots::VALIDITY].as_ref(),
            self.dtype_parts().1,
        )
    }
}
impl<T: TypedArrayRef<VarBinView>> VarBinViewArrayExt for T {}

impl Array<VarBinView> {
    /// Resolve the data buffers of this array for per-row access.
    pub fn resolved_views(&self) -> ResolvedViews<'_> {
        ResolvedViews::new(self)
    }

    #[inline]
    fn from_prevalidated_data(dtype: DType, data: VarBinViewData, slots: ArraySlots) -> Self {
        let len = data.len();
        unsafe { Array::from_parts_unchecked(ArrayParts::new(VarBinView, dtype, len, data, slots)) }
    }

    /// Construct a `VarBinViewArray` from an iterator of optional byte slices.
    #[expect(
        clippy::same_name_method,
        reason = "intentionally named from_iter like Iterator::from_iter"
    )]
    pub fn from_iter<T: AsRef<[u8]>, I: IntoIterator<Item = Option<T>>>(
        iter: I,
        dtype: DType,
    ) -> Self {
        let iter = iter.into_iter();
        let mut builder = VarBinViewBuilder::with_capacity_in(
            dtype,
            iter.size_hint().0,
            BufferAllocatorRef::statically_allocated(),
        );
        for value in iter {
            match value {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            }
        }
        builder.finish_into_varbinview()
    }

    pub fn from_iter_str<T: AsRef<str>, I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = VarBinViewBuilder::with_capacity_in(
            DType::Utf8(Nullability::NonNullable),
            iter.size_hint().0,
            BufferAllocatorRef::statically_allocated(),
        );
        for value in iter {
            builder.append_value(value.as_ref());
        }
        builder.finish_into_varbinview()
    }

    pub fn from_iter_nullable_str<T: AsRef<str>, I: IntoIterator<Item = Option<T>>>(
        iter: I,
    ) -> Self {
        let iter = iter.into_iter();
        let mut builder = VarBinViewBuilder::with_capacity_in(
            DType::Utf8(Nullability::Nullable),
            iter.size_hint().0,
            BufferAllocatorRef::statically_allocated(),
        );
        for value in iter {
            match value {
                Some(value) => builder.append_value(value.as_ref()),
                None => builder.append_null(),
            }
        }
        builder.finish_into_varbinview()
    }

    pub fn from_iter_bin<T: AsRef<[u8]>, I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = VarBinViewBuilder::with_capacity_in(
            DType::Binary(Nullability::NonNullable),
            iter.size_hint().0,
            BufferAllocatorRef::statically_allocated(),
        );
        for value in iter {
            builder.append_value(value.as_ref());
        }
        builder.finish_into_varbinview()
    }

    pub fn from_iter_nullable_bin<T: AsRef<[u8]>, I: IntoIterator<Item = Option<T>>>(
        iter: I,
    ) -> Self {
        let iter = iter.into_iter();
        let mut builder = VarBinViewBuilder::with_capacity_in(
            DType::Binary(Nullability::Nullable),
            iter.size_hint().0,
            BufferAllocatorRef::statically_allocated(),
        );
        for value in iter {
            match value {
                Some(value) => builder.append_value(value.as_ref()),
                None => builder.append_null(),
            }
        }
        builder.finish_into_varbinview()
    }

    /// Creates a new `VarBinViewArray`.
    pub fn try_new(
        views: Buffer<BinaryView>,
        buffers: Arc<[ByteBuffer]>,
        dtype: DType,
        validity: Validity,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Self> {
        let data = VarBinViewData::try_new(views, buffers, dtype.clone(), validity.clone(), ctx)?;
        let slots = VarBinViewData::make_slots(&validity, data.len());
        Ok(Self::from_prevalidated_data(dtype, data, slots))
    }

    /// Validate host views and create a fresh admitted table from borrowed buffers.
    pub fn try_new_from_buffers(
        views: Buffer<BinaryView>,
        buffers: &[ByteBuffer],
        dtype: DType,
        validity: Validity,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Self> {
        let data = VarBinViewData::try_new_from_buffers(
            views,
            buffers,
            dtype.clone(),
            validity.clone(),
            ctx,
        )?;
        let slots = VarBinViewData::make_slots(&validity, data.len());
        Ok(Self::from_prevalidated_data(dtype, data, slots))
    }

    /// Construct an admitted buffer table, preserving native host/device handles.
    /// See [`VarBinViewData::try_new_handle_in`] for the validation boundary.
    pub fn try_new_handle_in(
        views: BufferHandle,
        buffers: impl ExactSizeIterator<Item = BufferHandle>,
        dtype: DType,
        validity: Validity,
        allocator: &HostAllocatorRef,
    ) -> VortexResult<Self> {
        let data = VarBinViewData::try_new_handle_in(
            views,
            buffers,
            dtype.clone(),
            validity.clone(),
            allocator,
        )?;
        let slots = VarBinViewData::make_slots(&validity, data.len());
        Self::try_from_parts(ArrayParts::new(VarBinView, dtype, data.len(), data, slots))
    }

    /// Creates a new `VarBinViewArray` without validation.
    ///
    /// # Safety
    ///
    /// See [`VarBinViewData::new_unchecked`].
    pub unsafe fn new_unchecked(
        views: Buffer<BinaryView>,
        buffers: Arc<[ByteBuffer]>,
        dtype: DType,
        validity: Validity,
    ) -> Self {
        let data = unsafe {
            VarBinViewData::new_unchecked(views, buffers, dtype.clone(), validity.clone())
        };
        let slots = VarBinViewData::make_slots(&validity, data.len());
        Self::from_prevalidated_data(dtype, data, slots)
    }

    /// Creates a new `VarBinViewArray` with device or host memory.
    pub fn new_handle(
        views: BufferHandle,
        buffers: Arc<[BufferHandle]>,
        dtype: DType,
        validity: Validity,
    ) -> Self {
        let data = VarBinViewData::new_handle(views, buffers, dtype.clone(), validity.clone());
        let slots = VarBinViewData::make_slots(&validity, data.len());
        Self::from_prevalidated_data(dtype, data, slots)
    }

    /// Construct a new array from `BufferHandle`s without validation.
    ///
    /// # Safety
    ///
    /// See [`VarBinViewData::new_handle_unchecked`].
    pub unsafe fn new_handle_unchecked(
        views: BufferHandle,
        buffers: Arc<[BufferHandle]>,
        dtype: DType,
        validity: Validity,
    ) -> Self {
        let data = unsafe {
            VarBinViewData::new_handle_unchecked(views, buffers, dtype.clone(), validity.clone())
        };
        let slots = VarBinViewData::make_slots(&validity, data.len());
        Self::from_prevalidated_data(dtype, data, slots)
    }

    /// Export raw caller-owned parts. Managed native transformations should use
    /// `with_views_and_validity` so the table reservation stays with the array.
    pub fn into_data_parts(self) -> VarBinViewDataParts {
        let dtype = self.dtype().clone();
        let validity = self.varbinview_validity();
        let data = self.into_data();
        VarBinViewDataParts {
            dtype,
            buffers: data.buffers,
            views: data.views,
            validity,
        }
    }
}

impl<'a> FromIterator<Option<&'a [u8]>> for VarBinViewData {
    fn from_iter<T: IntoIterator<Item = Option<&'a [u8]>>>(iter: T) -> Self {
        Self::from_iter_nullable_bin(iter)
    }
}

impl FromIterator<Option<Vec<u8>>> for VarBinViewData {
    fn from_iter<T: IntoIterator<Item = Option<Vec<u8>>>>(iter: T) -> Self {
        Self::from_iter_nullable_bin(iter)
    }
}

impl FromIterator<Option<String>> for VarBinViewData {
    fn from_iter<T: IntoIterator<Item = Option<String>>>(iter: T) -> Self {
        Self::from_iter_nullable_str(iter)
    }
}

impl<'a> FromIterator<Option<&'a str>> for VarBinViewData {
    fn from_iter<T: IntoIterator<Item = Option<&'a str>>>(iter: T) -> Self {
        Self::from_iter_nullable_str(iter)
    }
}

// --- FromIterator forwarding for Array<VarBinView> ---

impl<'a> FromIterator<Option<&'a [u8]>> for Array<VarBinView> {
    fn from_iter<T: IntoIterator<Item = Option<&'a [u8]>>>(iter: T) -> Self {
        Self::from_iter(iter, DType::Binary(Nullability::Nullable))
    }
}

impl FromIterator<Option<Vec<u8>>> for Array<VarBinView> {
    fn from_iter<T: IntoIterator<Item = Option<Vec<u8>>>>(iter: T) -> Self {
        Self::from_iter(iter, DType::Binary(Nullability::Nullable))
    }
}

impl FromIterator<Option<String>> for Array<VarBinView> {
    fn from_iter<T: IntoIterator<Item = Option<String>>>(iter: T) -> Self {
        Self::from_iter_nullable_str(iter)
    }
}

impl<'a> FromIterator<Option<&'a str>> for Array<VarBinView> {
    fn from_iter<T: IntoIterator<Item = Option<&'a str>>>(iter: T) -> Self {
        Self::from_iter_nullable_str(iter)
    }
}
