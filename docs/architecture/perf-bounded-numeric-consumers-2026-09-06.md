# Bounded native numeric consumer experiment

This is an unregistered PERF-10 feasibility experiment following Constant/RunEnd
reducers. It has no production dispatch or accepted performance result yet.

Pinned Vortex 0.85's BitPacked slice provider addresses whole 1024-value packed
blocks and preserves the logical offset and validity. Its FoR slice provider
rewrites the encoded child. Use those existing providers to decode and consume
bounded windows through ShardLoom's existing typed scalar kernels. Do not copy
FastLanes code, implement unsafe unpacking, or introduce another engine.

The first admission is a host BitPacked leaf without patches, or a FoR leaf over
that representation. Validity is constant or a physical host Boolean array.
Unknown children, patches, device buffers, extra columns and transformed measures
miss before state changes. Native decode failures after admission propagate.
Provider allocations that bypass HostAllocator remain outside a memory bound;
the measured bound is the maximum canonical numeric rows in one window.

Preserve arbitrary selection order and duplicate multiplicity with bounded local
index scratch. A repeated selection may decode a block again; its actual work is
reported. Keep one additive measure's original ordered accumulator across every
window. Two SUM/AVG measures on the same column use the engine's existing fused
per-source-array arithmetic, so that shape is deliberately rejected rather than
silently reassociating it. Base-times-count arithmetic is not introduced.

The release experiment compares whole-array native decoding with 1024-, 8192-
and 32768-row windows using the same typed consumers and complete exact results.
Seven alternating pairs per window retain every timing. These are array decode
plus scalar-consumer times, not file, process, end-to-end ingest, or RSS evidence.
Measured regressions must reject automatic promotion or narrow its admission.
Independent native-width/null/selection/carried-sum tests precede measurement.
These ordinary native tests pass, including mixed-sign cancellation and distinct
COUNT(*)/COUNT(column) treatment. The ignored release experiment is unmeasured.
Fused additive measures, persisted-file structural admission and actual query
lifecycle measurement remain prerequisites for a broader implementation.
