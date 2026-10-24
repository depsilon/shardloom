// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-10-16: expose admitted buffer-table construction.

mod array;
pub use array::VarBinViewArrayExt;
pub use array::VarBinViewArraySlotsExt;
pub use array::VarBinViewData;
pub use array::VarBinViewDataParts;
pub use array::VarBinViewSlots;
pub use vtable::VarBinViewArray;

pub(crate) mod compact;

pub(crate) mod compute;

pub(crate) mod metadata;
pub use metadata::VarBinViewBufferBuilder;

mod vtable;
pub use vtable::VarBinView;

pub(crate) fn initialize(session: &vortex_session::VortexSession) {
    vtable::initialize(session);
}

pub mod build_views;

mod resolved;
pub use resolved::ResolvedViews;

mod view;
pub use view::BinaryView;
pub use view::Inlined;
pub use view::Ref;

#[cfg(test)]
mod tests;
