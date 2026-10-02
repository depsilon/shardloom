//! Yield shared conversion waits and drain the input outside provider tasks.

use std::{
    io::Write,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};

use futures::{Stream, io::AllowStdIo, stream::poll_fn};
use vortex::{
    array::{ArrayRef, iter::ArrayIterator, stream::ArrayStreamAdapter},
    error::VortexResult,
    file::{VortexWriteOptions, WriteSummary},
    io::{
        AsyncWriteAdapter, runtime::BlockingRuntime as _, runtime::current::CurrentThreadRuntime,
    },
};

struct InputOwner<I>(Arc<Mutex<Option<I>>>);

impl<I> Drop for InputOwner<I> {
    fn drop(&mut self) {
        // Remove the input even if the provider still holds an aborted stream.
        // Its eventual Drop then owns no source/conversion tasks to join.
        let input = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(input);
    }
}

pub(super) fn write<I, W>(
    runtime: &CurrentThreadRuntime,
    options: VortexWriteOptions,
    writer: W,
    input: I,
) -> VortexResult<WriteSummary>
where
    I: ArrayIterator + Stream<Item = VortexResult<ArrayRef>> + Unpin + Send + 'static,
    W: Write + Unpin,
{
    let dtype = input.dtype().clone();
    let owner = InputOwner(Arc::new(Mutex::new(Some(input))));
    let input = Arc::clone(&owner.0);
    let stream = ArrayStreamAdapter::new(
        dtype,
        poll_fn(move |cx| {
            let mut input = input
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            input
                .as_mut()
                .map_or(Poll::Ready(None), |input| Pin::new(input).poll_next(cx))
        }),
    );
    let result =
        runtime.block_on(options.write(AsyncWriteAdapter(AllowStdIo::new(writer)), stream));
    // This synchronous caller is outside the provider's executor poll stack.
    // Draining here preserves the source/array window without another queue.
    drop(owner);
    result
}
