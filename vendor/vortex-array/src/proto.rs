// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Bindings generated from this crate's `proto` schemas, checked in under `generated/`.
//! Regenerate with `cargo run -p xtask -- generate-proto`.
//! This vendored subset has no xtask. After upstream regeneration, reapply the
//! two root trait overrides documented in `proto/generated/README.shardloom.md`.

mod decode;
mod owned_release;
mod structural;
mod wire;

#[cfg(test)]
mod decode_tests;

#[cfg(test)]
mod structural_tests;

#[cfg(test)]
mod wire_reference_fixtures;

#[cfg(test)]
mod wire_tests;

/// Data types.
#[allow(clippy::all)]
#[allow(clippy::absolute_paths)]
#[allow(clippy::nursery)]
#[allow(missing_docs)]
pub mod dtype {
    include!("proto/generated/vortex.dtype.rs");
}

/// Scalar values.
#[allow(clippy::all)]
#[allow(clippy::absolute_paths)]
#[allow(clippy::nursery)]
#[allow(missing_docs)]
pub mod scalar {
    include!("proto/generated/vortex.scalar.rs");
}

/// Expressions.
#[allow(clippy::all)]
#[allow(clippy::absolute_paths)]
#[allow(clippy::nursery)]
#[allow(missing_docs)]
pub mod expr {
    include!("proto/generated/vortex.expr.rs");
}
