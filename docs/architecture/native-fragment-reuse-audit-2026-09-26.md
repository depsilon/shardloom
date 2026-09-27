# Native fragment reuse — R9.a

Status: drop the duplicate prepare/seal/reuse proposal after source audit under
PERF-INTAKE / RFC 0044. No new runtime prototype or speedup is retained.

The audit uses `552c8cfe0805b9893dab2325657cd1f9a5fdc5b4` and pinned Vortex
0.85.0. The intake admits a new fragment representation only where a measured
duplicate remains. The active large-ingest path does not first seal a complete
payload and then serialize it again during publication.

| Boundary | Existing behavior |
| --- | --- |
| Stream finalizer | `vortex_ingest.rs::finalize_vortex_prepared_state_stream_write` passes its iterator once to `write_vortex_array_iterator`. |
| Native writer | `LocalVortexWriteContext::write_array_iterator` invokes one Vortex blocking write inside the workspace-safe staging producer. |
| Batch layout | `ingest_bounded_layout.rs::BoundedIngestLayout` invokes its child once per nonempty array and retains layout references for the final chunked root. These are metadata references, not a second serialized payload. |
| Leaf serialization | Pinned `vortex-layout/src/layouts/flat/writer.rs` serializes the leaf once and transfers its buffers to `SegmentSink::write`. |
| Artifact assembly | Pinned `vortex-file/src/writer.rs` streams those buffers to its output, then serializes the footer. ShardLoom hashes bytes during the staging write; `digest_micros=0` does not represent a deferred full-file hash. |
| Publication | `shardloom-core/src/security.rs` validates the writer result and source identities, flushes staging, and commits it. Reopen/schema/layout checks retain their separate correctness role. |

The bounded memory-generation route already implements encoded-segment reuse.
`GenerationBuilder` serializes arrays into immutable segments once; queries
read retained `ByteBuffer` handles and `MemoryFileGeneration::publish` writes
those same segments. Construction validates a serialized footer bound and
publication serializes that footer again. That is repeated metadata work, not
repeated array serialization, and there is no material-gain evidence for changing
that separate bounded contract. R5.a removes intermediate serialization entirely
for its admitted owned-array aggregate workflow.

This audit does not prove that every codec, statistic or canonicalization pass is
necessary. Native intake copies establish buffer ownership; text canonicalization,
compaction and compression have different representations; numeric probes and
post-coalescing encoding have distinct inputs. Removing any of those requires its
own identity, ownership and stage attribution. Required validation is not redundant
serialization. Ordinary streamed publication flushes and renames; its completion
is not a claim of file/parent-directory fsync durability.

Decision: do not introduce another fragment format or queue to duplicate these
mechanisms. Preserve existing serializers, source-generation checks, statistics,
native Vortex output and no-fallback execution. Reopen this candidate only with
an identified repeated payload pass and a material ingest/workflow opportunity.
R9.b separately investigates idle capacity within the existing sequential batch
writer; this decision does not settle that scheduling question.

Evidence is source inspection, including independent read-only inventory. No new
performance measurement is claimed, and no replacement ingest is required for
this documentation-only decision.
