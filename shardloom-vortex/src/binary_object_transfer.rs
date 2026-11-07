//! Explicit transfer of object ownership with native source/result intake.
//!
//! Memory objects acquire receiving credits and copies. File objects share the
//! immutable file/cleanup owner while re-admitting their directory metadata.

use super::{
    Arena, Backing, BinaryObject, Chunk, Payload,
    descriptor::{self, Descriptor},
    error, reserved_vec,
};
use crate::resident_session::{NativeExecutionContext, ResidentVortexSession};
use shardloom_core::Result;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::Budgeted};
use std::sync::Arc;
use vortex::array::{ArrayRef, VortexSessionExecute as _};

pub(crate) struct Transfer(Budgeted<Vec<Arc<Payload>>>);

impl Transfer {
    pub(crate) fn prepare(
        origin: &ResidentVortexSession,
        target: &ResidentVortexSession,
        arrays: &[ArrayRef],
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        target.with_native_execution_context(cancellation, |context| {
            let source = origin.binary_object_arena()?;
            let receiving = target.binary_object_arena()?;
            let (objects, credit) = reserved_vec(context.memory(), 0)?;
            let mut objects = Budgeted::new(objects, credit);
            let mut ctx = context.native_session().create_execution_ctx();
            for array in arrays {
                super::writer::visit(array, &mut ctx, cancellation, &mut |raw| {
                    if let Descriptor::Managed { id, length } = descriptor::parse(raw)? {
                        if receiving
                            .lookup(&id, length)
                            .is_ok_and(|object| object.validate().is_ok())
                        {
                            return Ok(());
                        }
                        match objects
                            .value()
                            .binary_search_by_key(&id, |object: &Arc<Payload>| {
                                object.id.expect("managed transfer")
                            }) {
                            Ok(index) => {
                                if objects.value()[index].length != length {
                                    return Err(error(
                                        "DESCRIPTOR",
                                        "transferred object length differs from its owner",
                                    ));
                                }
                            }
                            Err(index) => {
                                let payload = copy(&source.lookup(&id, length)?, context)?;
                                let (mut replacement, credit) =
                                    reserved_vec(context.memory(), objects.value().len() + 1)?;
                                replacement.extend(objects.value().iter().cloned());
                                replacement.insert(index, payload);
                                objects = Budgeted::new(replacement, credit);
                            }
                        }
                    }
                    Ok(())
                })?;
            }
            Ok(Self(objects))
        })
    }

    /// Final fallible publication step after the receiving arrays are complete.
    pub(crate) fn publish(
        self,
        target: &ResidentVortexSession,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        target
            .binary_object_arena()?
            .insert_batch(self.0.value(), cancellation)
    }
}

fn copy(payload: &Arc<Payload>, context: &NativeExecutionContext<'_>) -> Result<Arc<Payload>> {
    context.check_cancelled()?;
    payload.validate()?;
    let (mut chunks, credit) =
        reserved_vec::<Chunk>(context.memory(), payload.chunks.value().len())?;
    chunks.extend_from_slice(payload.chunks.value());
    let metadata = context.memory().reserve(4096)?;
    let backing = match &payload.backing {
        Backing::Memory(bytes) => {
            let (mut copied, credit) = reserved_vec(context.memory(), bytes.value().len())?;
            for chunk in bytes.value().chunks(super::CHUNK_BYTES) {
                context.check_cancelled()?;
                copied.extend_from_slice(chunk);
            }
            Backing::Memory(Arc::new(Budgeted::new(copied, credit)))
        }
        Backing::File {
            identity,
            offset,
            _retention: retention,
        } => Backing::File {
            identity: Arc::clone(identity),
            offset: *offset,
            _retention: retention.clone(),
        },
    };
    payload.validate()?;
    context.check_cancelled()?;
    Ok(Arc::new(Payload {
        id: payload.id,
        length: payload.length,
        chunks: Budgeted::new(chunks, credit),
        backing,
        _metadata: metadata,
    }))
}

impl ResidentVortexSession {
    /// Transfer a byte object's actual owner to this session. Memory objects
    /// copy under the receiving budget; file objects retain their immutable
    /// backing and exact cleanup owner. No payload is read from a file.
    /// # Errors
    /// Rejects missing/changed content, denied memory or cancellation before
    /// publishing an object. The caller's original handle remains unchanged.
    pub fn import_binary_object(
        &self,
        object: &BinaryObject,
        cancellation: &CancellationToken,
    ) -> Result<BinaryObject> {
        if self.same_owner(&object.session) {
            cancellation.check()?;
            object.payload.validate()?;
            return Ok(object.clone());
        }
        let handle = self.with_native_execution_context(cancellation, |context| {
            let current = if let Some(id) = object.payload.id {
                Arena::get(context.native_session())?
                    .lookup(&id, object.len())
                    .ok()
                    .filter(|value| value.validate().is_ok())
            } else {
                None
            };
            let payload = match current {
                Some(payload) => payload,
                None => copy(&object.payload, context)?,
            };
            self.object_handle(payload, object.descriptor())
        })?;
        if handle.is_managed() {
            self.publish_binary_object(handle, cancellation)
        } else {
            Ok(handle)
        }
    }
}
