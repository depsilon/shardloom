// SPDX-License-Identifier: Apache-2.0
// ShardLoom local provider extension; see RFC 0044 and the pinned source audit.

//! Preallocation admission for the unchanged, synchronous Zstandard 1.5.7 APIs.
//!
//! Foreign C storage has an explicit heap bound and an opaque reservation owner.
//! Input preparation and retained outputs use the caller's actual host buffers.
//! This module exposes no raw context pointers or allocation callbacks.

use std::mem::size_of;

use vortex_array::memory::HostAllocator;
use vortex_array::memory::HostMemoryReservation;
use vortex_buffer::Alignment;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use zstd::bulk::Compressor;
use zstd::zstd_safe;
use zstd_sys::ZSTD_dictLoadMethod_e::ZSTD_dlm_byCopy;

#[cfg(test)]
#[path = "compression_resources_tests.rs"]
mod tests;

const AUDITED_ZSTD_VERSION: u32 = 10507;
const AUDITED_DEFAULT_LEVEL: i32 = 3;

fn check_version() -> VortexResult<()> {
    let version = zstd_safe::version_number();
    vortex_ensure!(
        version == AUDITED_ZSTD_VERSION,
        "Native Zstd compression requires audited provider version {AUDITED_ZSTD_VERSION}, found {version}"
    );
    // SAFETY: This pointer-free getter returns the linked provider's default.
    // A same-version library compiled with another default needs a new bound
    // for both level-zero compression and the default dictionary trainer.
    let default_level = unsafe { zstd_sys::ZSTD_defaultCLevel() };
    vortex_ensure!(
        default_level == AUDITED_DEFAULT_LEVEL,
        "Native Zstd compression requires audited default level {AUDITED_DEFAULT_LEVEL}, found {default_level}"
    );
    Ok(())
}

fn size_result(result: usize) -> VortexResult<usize> {
    // SAFETY: The pinned pure error predicate accepts every size_t value.
    vortex_ensure!(
        unsafe { zstd_sys::ZSTD_isError(result) } == 0,
        "Native Zstd compression: {}",
        zstd_safe::get_error_name(result)
    );
    Ok(result)
}

pub(crate) fn compress_bound(bytes: usize) -> VortexResult<usize> {
    size_result(zstd_safe::compress_bound(bytes))
}

fn compressor_heap_bound(level: i32, dictionary_len: usize) -> VortexResult<usize> {
    check_version()?;
    // Match CCtx's setter, including its default and clamping semantics. In
    // particular, never let an untrusted positive i32 drive the estimator's loop.
    let level = if level == 0 {
        AUDITED_DEFAULT_LEVEL
    } else {
        level
    }
    .clamp(zstd_safe::min_c_level(), zstd_safe::max_c_level());
    // SAFETY: These pointer-free estimators accept the normalized level and a
    // bounded dictionary size. The checked 1.5.7 contract covers single-threaded
    // one-shot compress2. estimateCDictSize's by-copy allowance also covers the
    // local loader's dictionary copy plus its by-reference prepared dictionary.
    let context = size_result(unsafe { zstd_sys::ZSTD_estimateCCtxSize(level) })?;
    let dictionary = if dictionary_len == 0 {
        0
    } else {
        size_result(unsafe { zstd_sys::ZSTD_estimateCDictSize(dictionary_len, level) })?
    };
    context
        .checked_add(dictionary)
        .ok_or_else(|| vortex_err!("Native Zstd compressor reservation overflow"))
}

pub(crate) struct AdmittedCompressor {
    // Rust drops fields in declaration order: all C storage is freed before its
    // reservation. No method exposes the context or permits parameter changes.
    compressor: Compressor<'static>,
    _reservation: HostMemoryReservation,
    admitted_bytes: usize,
}

impl AdmittedCompressor {
    pub(crate) fn new(
        level: i32,
        dictionary: Option<&[u8]>,
        allocator: &dyn HostAllocator,
    ) -> VortexResult<Self> {
        let dictionary = dictionary.unwrap_or_default();
        let admitted_bytes = compressor_heap_bound(level, dictionary.len())?;
        let reservation = allocator.reserve_external(admitted_bytes)?;
        let compressor = Compressor::with_dictionary(level, dictionary)?;
        Ok(Self {
            compressor,
            _reservation: reservation,
            admitted_bytes,
        })
    }

    pub(crate) fn compress(&mut self, input: &[u8], output: &mut [u8]) -> VortexResult<usize> {
        let written = self
            .compressor
            .compress_to_buffer(input, output)
            .map_err(|error| VortexError::from(error).with_context("while compressing"))?;
        vortex_ensure!(
            written <= output.len(),
            "Native Zstd compressor exceeded its output extent"
        );
        // A defensive check of the pinned provider contract, not a substitute
        // for preallocation admission. Source audit covers intermediate overlap.
        vortex_ensure!(
            self.compressor.context_mut().sizeof() <= self.admitted_bytes,
            "Native Zstd compressor exceeded its admitted heap bound"
        );
        Ok(written)
    }
}

// Layout only: the pinned sequential FASTCOVER trial malloc has these fields.
// No Rust value or pointer of this type is passed to C.
#[repr(C)]
struct TrainingTrialLayout {
    context: *const (),
    best: *mut (),
    dictionary_capacity: usize,
    parameters: zstd_sys::ZDICT_cover_params_t,
}

fn training_heap_bound(
    input_len: usize,
    sample_sizes: &[usize],
    dictionary_capacity: usize,
) -> VortexResult<usize> {
    check_version()?;
    vortex_ensure!(
        sample_sizes.len() < u32::MAX as usize,
        "Native Zstd training sample count exceeds the provider's checked domain"
    );
    let sample_total = sample_sizes.iter().try_fold(0usize, |total, size| {
        total
            .checked_add(*size)
            .ok_or_else(|| vortex_err!("Native Zstd training sample size overflow"))
    })?;
    vortex_ensure!(
        sample_total == input_len,
        "Native Zstd training sample extents do not cover the input"
    );
    let max_input = if size_of::<usize>() == 8 {
        u32::MAX as usize
    } else {
        1 << 30
    };
    vortex_ensure!(
        input_len >= 8 && input_len < max_input,
        "Native Zstd training input exceeds the provider's checked size domain"
    );
    // Default FASTCOVER uses the first floor(3 * N / 4) samples for training.
    // Its d-mer count subtracts eight from this partition's byte length, even
    // though its own entry check only validates the complete input length.
    // Validate that arithmetic before entering C. This integer expression is
    // equivalent to the pinned 0.75 split without overflowing a 32-bit usize.
    let training_samples = sample_sizes.len() - sample_sizes.len().div_ceil(4);
    vortex_ensure!(
        training_samples >= 5 && training_samples < sample_sizes.len(),
        "Native Zstd training sample split is outside the provider's checked domain"
    );
    let training_bytes: usize = sample_sizes[..training_samples].iter().sum();
    vortex_ensure!(
        training_bytes >= 8,
        "Native Zstd training partition is outside the provider's checked size domain"
    );
    let offsets = sample_sizes
        .len()
        .checked_add(1)
        .and_then(|count| count.checked_mul(size_of::<usize>()))
        .ok_or_else(|| vortex_err!("Native Zstd training offset size overflow"))?;
    let dictionaries = dictionary_capacity
        .checked_mul(4)
        .ok_or_else(|| vortex_err!("Native Zstd training dictionary size overflow"))?;
    let parameters = zstd_sys::ZSTD_compressionParameters {
        windowLog: 21,
        chainLog: 16,
        hashLog: 17,
        searchLog: 2,
        minMatch: 4,
        targetLength: 0,
        strategy: zstd_sys::ZSTD_strategy::ZSTD_dfast,
    };
    // SAFETY: The pointer-free estimates receive valid parameters enclosing all
    // four default level-3 rows. Default FASTCOVER trains sequentially at level
    // 3; its entropy and trial compressors are unbuffered. The dictionary copy,
    // match state, workspace alignment and context objects are in these bounds.
    let context = size_result(unsafe { zstd_sys::ZSTD_estimateCCtxSize_usingCParams(parameters) })?;
    let dictionary = size_result(unsafe {
        zstd_sys::ZSTD_estimateCDictSize_advanced(dictionary_capacity, parameters, ZSTD_dlm_byCopy)
    })?;
    let trial_output = compress_bound(sample_sizes.iter().copied().max().unwrap_or_default())?;
    [
        offsets,
        10 << 20,
        dictionaries,
        size_of::<TrainingTrialLayout>(),
        context,
        dictionary,
        128 << 10,
        trial_output,
    ]
    .into_iter()
    .try_fold(0usize, |total, bytes| {
        total
            .checked_add(bytes)
            .ok_or_else(|| vortex_err!("Native Zstd training reservation overflow"))
    })
}

pub(crate) fn train_dictionary(
    input: &[u8],
    sample_sizes: &[usize],
    dictionary_capacity: usize,
    allocator: &dyn HostAllocator,
) -> VortexResult<ByteBuffer> {
    let bound = training_heap_bound(input.len(), sample_sizes, dictionary_capacity)?;
    let _reservation = allocator.reserve_external(bound)?;
    let mut dictionary = allocator.allocate(dictionary_capacity, Alignment::none())?;
    let bytes = dictionary.as_mut_slice();
    vortex_ensure!(
        bytes.len() == dictionary_capacity,
        "Native Zstd dictionary buffer has incorrect length"
    );
    // The safe wrapper preserves ZDICT_trainFromBuffer's default algorithm. Its
    // count conversion and sample-sum assertion were validated before admission.
    let written = zstd_safe::train_from_buffer(bytes, input, sample_sizes).map_err(|error| {
        vortex_err!(
            "Native Zstd dictionary training: {}",
            zstd_safe::get_error_name(error)
        )
    })?;
    vortex_ensure!(
        written > 0 && written <= dictionary_capacity,
        "Native Zstd dictionary output has invalid length"
    );
    let dictionary = dictionary.freeze();
    vortex_ensure!(
        dictionary.len() == dictionary_capacity,
        "Native Zstd dictionary freeze changed buffer length"
    );
    // Slicing retains the admitted capacity; all foreign training allocations
    // were freed before the safe training call returned.
    Ok(dictionary.slice(0..written))
}
