// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Ordered release of explicit buffer owners, without opaque nested Bytes owners.

use std::sync::Arc;

use crate::BufferBacking;

pub(crate) struct RetainedBufferOwner {
    pub(crate) backing: Option<Arc<BufferBacking>>,
    pub(crate) owner: Option<Arc<dyn Send + Sync>>,
}

#[derive(Default)]
struct PendingOwners(Vec<Arc<dyn Send + Sync>>);

impl PendingOwners {
    fn drain(&mut self) {
        while let Some(owner) = self.0.pop() {
            drop(owner);
        }
    }
}

impl Drop for PendingOwners {
    fn drop(&mut self) {
        // Also handles a single panic during the explicit normal-path drain.
        self.drain();
    }
}

impl Drop for RetainedBufferOwner {
    fn drop(&mut self) {
        let mut pending = PendingOwners::default();
        if let Some(owner) = self.owner.take() {
            pending.0.push(owner);
        }
        let mut current = self.backing.take();
        while let Some(backing) = current {
            // Unlike try_unwrap(...).ok(), exactly one concurrent final owner
            // extracts the value; every other retained link has this destructor.
            let Some(backing) = Arc::into_inner(backing) else {
                break;
            };
            match backing {
                BufferBacking::Retained(mut retained) => {
                    current = retained.backing.take();
                    if let Some(owner) = retained.owner.take() {
                        pending.0.push(owner);
                    }
                }
                backing => {
                    // Storage first, including one-panic cleanup of pending owners.
                    drop(backing);
                    break;
                }
            }
        }
        pending.drain();
    }
}

#[cfg(test)]
#[path = "retained_owner_tests.rs"]
mod tests;
