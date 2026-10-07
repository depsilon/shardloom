// SPDX-License-Identifier: Apache-2.0
// ShardLoom local provider extension; see RFC 0044 and the pinned source audit.

//! One-shot Zstandard 1.5.7 decoding in actual session-allocated workspaces.
//!
//! The private wrapper borrows its buffers and dictionary. It has no C-owned
//! allocations and must never call a C free function. Streaming, internal
//! dictionary loading and legacy decoding are deliberately unavailable.

use std::marker::PhantomData;
use std::ptr;

use vortex_array::memory::HostAllocator;
use vortex_buffer::Alignment;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use zstd::zstd_safe;
use zstd_sys::ZSTD_dictContentType_e::ZSTD_dct_auto;
use zstd_sys::ZSTD_dictLoadMethod_e::ZSTD_dlm_byRef;

const AUDITED_ZSTD_VERSION: u32 = 10507;
const WORKSPACE_ALIGNMENT: usize = 8;

pub(crate) fn decompress_frames(
    frames: &[&ByteBuffer],
    dictionary: Option<&[u8]>,
    output: &mut [u8],
    allocator: &dyn HostAllocator,
) -> VortexResult<usize> {
    if frames.is_empty() {
        return Ok(0);
    }
    let version = zstd_safe::version_number();
    vortex_ensure!(
        version == AUDITED_ZSTD_VERSION,
        "Native Zstd workspaces require audited provider version {AUDITED_ZSTD_VERSION}, found {version}"
    );
    for frame in frames {
        validate_members(frame.as_slice())?;
    }

    // SAFETY: The pinned estimators take no pointers and have no preconditions.
    // In 1.5.7 these sizes are sizeof(DCtx) and sizeof(DDict); byRef excludes a
    // dictionary-content copy. The linked version was checked above.
    let context_size = size_result(unsafe { zstd_sys::ZSTD_estimateDCtxSize() })?;
    let dictionary_size = dictionary
        .map(|dict| {
            size_result(unsafe { zstd_sys::ZSTD_estimateDDictSize(dict.len(), ZSTD_dlm_byRef) })
        })
        .transpose()?;
    let alignment = Alignment::new(WORKSPACE_ALIGNMENT);
    let mut context = allocator.allocate(context_size, alignment)?;
    let mut prepared = dictionary_size
        .map(|size| allocator.allocate(size, alignment))
        .transpose()?;
    let prepared = prepared
        .as_mut()
        .zip(dictionary)
        .zip(dictionary_size)
        .map(|((buffer, bytes), size)| (buffer.as_mut_slice(), bytes, size));
    let mut decoder = BorrowedDecoder::new(context.as_mut_slice(), context_size, prepared)?;
    let mut written = 0;
    for frame in frames {
        written += decoder.decompress(frame.as_slice(), &mut output[written..])?;
    }
    // All workspaces drop here, before the caller freezes/retains its output.
    Ok(written)
}

fn validate_members(mut bytes: &[u8]) -> VortexResult<()> {
    vortex_ensure!(!bytes.is_empty(), "Invalid empty Zstd frame");
    while !bytes.is_empty() {
        vortex_ensure!(bytes.len() >= 4, "Truncated Zstd member header");
        reject_legacy_header(bytes)?;
        let size = zstd_safe::find_frame_compressed_size(bytes).map_err(|error| {
            vortex_err!("Invalid Zstd member: {}", zstd_safe::get_error_name(error))
        })?;
        vortex_ensure!(
            size > 0 && size <= bytes.len(),
            "Invalid Zstd member extent"
        );
        bytes = &bytes[size..];
    }
    Ok(())
}

pub(crate) fn reject_legacy_header(bytes: &[u8]) -> VortexResult<()> {
    if let Some(magic) = bytes.first_chunk::<4>().copied().map(u32::from_le_bytes) {
        // Pinned legacy headers: v0.1 differs in byte order from v0.2..v0.7.
        // Reject before any provider header parser can enter legacy dispatch.
        vortex_ensure!(
            magic != 0x1EB5_2FFD && !(0xFD2F_B522..=0xFD2F_B527).contains(&magic),
            "Legacy Zstd frames are unsupported by the admitted native decoder"
        );
    }
    Ok(())
}

fn size_result(result: usize) -> VortexResult<usize> {
    // SAFETY: This pure error-code predicate accepts every size_t value.
    vortex_ensure!(
        unsafe { zstd_sys::ZSTD_isError(result) } == 0,
        "Native Zstd decoder: {}",
        zstd_safe::get_error_name(result)
    );
    Ok(result)
}

fn initialize_workspace(bytes: &mut [u8], required: usize) -> VortexResult<()> {
    // Check the actual slice, not an allocator's advertised length/alignment.
    vortex_ensure!(
        required > 0 && bytes.len() == required,
        "Native Zstd workspace has incorrect length: expected {required}, found {}",
        bytes.len()
    );
    vortex_ensure!(
        bytes.as_ptr().addr().is_multiple_of(WORKSPACE_ALIGNMENT),
        "Native Zstd workspace must be eight-byte aligned"
    );
    bytes.fill(0);
    Ok(())
}

struct BorrowedDecoder<'a> {
    context: *mut zstd_sys::ZSTD_DCtx,
    dictionary: *const zstd_sys::ZSTD_DDict,
    // Both exclusive workspaces and the immutable original dictionary outlive
    // every call. Raw pointers also prevent automatic Send/Sync implementations.
    _borrows: PhantomData<(&'a mut [u8], &'a [u8])>,
}

impl<'a> BorrowedDecoder<'a> {
    fn new(
        context: &'a mut [u8],
        context_size: usize,
        prepared: Option<(&'a mut [u8], &'a [u8], usize)>,
    ) -> VortexResult<Self> {
        initialize_workspace(context, context_size)?;
        // SAFETY: The caller checked the pinned version and supplies its exact
        // DCtx estimate. The initialized slice is sufficiently large, aligned,
        // exclusive, and borrowed for 'a; its allocation cannot move or freeze.
        let context =
            unsafe { zstd_sys::ZSTD_initStaticDCtx(context.as_mut_ptr().cast(), context.len()) };
        vortex_ensure!(
            !context.is_null(),
            "Native Zstd decoder workspace initialization failed"
        );
        let dictionary = if let Some((workspace, dictionary, size)) = prepared {
            initialize_workspace(workspace, size)?;
            // SAFETY: The same size/alignment/lifetime rules hold for DDict.
            // Its input pointer is non-null even for an empty Rust slice.
            // byRef borrows immutable dictionary bytes for 'a and never copies
            // them or allocates. auto preserves raw/trained dictionary semantics.
            let dictionary = unsafe {
                zstd_sys::ZSTD_initStaticDDict(
                    workspace.as_mut_ptr().cast(),
                    workspace.len(),
                    dictionary.as_ptr().cast(),
                    dictionary.len(),
                    ZSTD_dlm_byRef,
                    ZSTD_dct_auto,
                )
            };
            vortex_ensure!(!dictionary.is_null(), "Invalid native Zstd dictionary");
            dictionary
        } else {
            ptr::null()
        };
        Ok(Self {
            context,
            dictionary,
            _borrows: PhantomData,
        })
    }

    fn decompress(&mut self, input: &[u8], output: &mut [u8]) -> VortexResult<usize> {
        // SAFETY: Both objects are initialized static state with live exclusive
        // workspaces. The optional dictionary and its bytes remain borrowed.
        // Input/output are valid disjoint slices with their exact bounds. This
        // synchronous one-shot API resets prior output references before using
        // each modern frame; it retains no input/output access after returning.
        // Legacy members were rejected and no streaming/dynamic-dictionary API
        // is reachable. Neither static object is freed through C.
        let result = unsafe {
            zstd_sys::ZSTD_decompress_usingDDict(
                self.context,
                output.as_mut_ptr().cast(),
                output.len(),
                input.as_ptr().cast(),
                input.len(),
                self.dictionary,
            )
        };
        let written = size_result(result)?;
        vortex_ensure!(
            written <= output.len(),
            "Native Zstd decoder exceeded its output extent"
        );
        Ok(written)
    }
}
