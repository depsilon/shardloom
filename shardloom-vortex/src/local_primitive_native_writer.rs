//! Complete the bounded native writer task before returning a fallible result.
//!
//! Vortex 0.85's blocking push writer queues one array and spawns a layout task.
//! Dropping a healthy writer, or returning a sink error from its output future,
//! cancels that task without joining it. On an idle resident runtime its buffers
//! can remain alive. Keep the sequential Flat strategy and native serialization;
//! drain only already accepted input, never the producer, before releasing it.

use std::{cell::RefCell, io, rc::Rc, sync::Arc};
use vortex::{
    array::{ArrayRef, dtype::DType},
    error::{VortexResult, vortex_err},
    file::{BlockingWriter, WriteOptionsSessionExt as _, WriteSummary},
    io::runtime::BlockingRuntime,
    layout::LayoutStrategy,
    session::VortexSession,
};

type SinkFailure = Rc<RefCell<Option<io::Error>>>;

pub(super) struct NativeWriter<'runtime, 'sink, B: BlockingRuntime> {
    writer: Option<BlockingWriter<'runtime, 'sink, B>>,
    failure: SinkFailure,
}

impl<'runtime, 'sink, B: BlockingRuntime> NativeWriter<'runtime, 'sink, B> {
    pub(super) fn new(
        session: &VortexSession,
        runtime: &'runtime B,
        sink: impl io::Write + Unpin + 'sink,
        dtype: DType,
        strategy: Arc<dyn LayoutStrategy>,
    ) -> Self {
        let failure = Rc::new(RefCell::new(None));
        let writer = session
            .write_options()
            .with_strategy(strategy)
            .with_file_statistics(Vec::new())
            .blocking(runtime)
            .writer(
                DrainAfterFailure {
                    sink,
                    failure: Rc::clone(&failure),
                },
                dtype,
            );
        Self {
            writer: Some(writer),
            failure,
        }
    }

    pub(super) fn push(&mut self, array: ArrayRef) -> VortexResult<()> {
        let result = self
            .writer
            .as_mut()
            .ok_or_else(|| vortex_err!("native writer is already closed"))?
            .push(array);
        if result.is_err() {
            // A provider/layout error has consumed the fused terminal future.
            // Sink errors are handled below; never finish this future twice.
            self.writer.take();
        } else if self.failure.borrow().is_some() {
            self.drain();
        } else {
            return result;
        }
        match self.failure.borrow_mut().take() {
            Some(error) => Err(error.into()),
            None => result,
        }
    }

    pub(super) fn finish(mut self) -> VortexResult<WriteSummary> {
        let result = self
            .writer
            .take()
            .ok_or_else(|| vortex_err!("native writer is already closed"))?
            .finish();
        match self.failure.borrow_mut().take() {
            Some(error) => Err(error.into()),
            None => result,
        }
    }

    #[cfg(all(test, feature = "vortex-write", unix))]
    pub(super) fn buffered_bytes(&self) -> u64 {
        self.writer
            .as_ref()
            .map_or(0, BlockingWriter::buffered_bytes)
    }

    fn drain(&mut self) {
        if let Some(writer) = self.writer.take() {
            // Closing input joins the layout task; failed output is discarded by
            // the adapter. A producer error remains the caller's original error.
            let _ = writer.finish();
        }
    }
}

impl<B: BlockingRuntime> Drop for NativeWriter<'_, '_, B> {
    fn drop(&mut self) {
        // The fallible API guarantees cleanup. Do not re-poll a provider future
        // that panicked while unwinding and risk a second panic in destruction.
        if !std::thread::panicking() {
            self.drain();
        }
    }
}

/// Preserve the first real sink error, then consume only pending native output
/// without touching that sink. `NativeWriter` checks after each push and finish,
/// so failure cannot become a successful summary or demand more producer input.
struct DrainAfterFailure<W> {
    sink: W,
    failure: SinkFailure,
}

impl<W: io::Write> io::Write for DrainAfterFailure<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.failure.borrow().is_some() {
            return Ok(bytes.len());
        }
        match self.sink.write(bytes) {
            Ok(0) if !bytes.is_empty() => {
                *self.failure.borrow_mut() = Some(io::Error::from(io::ErrorKind::WriteZero));
                Ok(bytes.len())
            }
            Ok(written) => Ok(written),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => Err(error),
            Err(error) => {
                *self.failure.borrow_mut() = Some(error);
                Ok(bytes.len())
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.failure.borrow().is_none()
            && let Err(error) = self.sink.flush()
        {
            *self.failure.borrow_mut() = Some(error);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "local_primitive_native_writer_tests.rs"]
mod tests;
