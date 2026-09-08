# Prepared native arrays at the compatibility sink

This PERF-02/PERF-07 candidate implements the supplied performance plan's
explicit array-ownership and sink-boundary requirements. The integrated candidate
selects this route for admitted existing structured Arrow IPC/Parquet exports.
Validation and retention measurements remain pending. The retained native Vortex
writer is unchanged; it shares only source-plan preparation with this sink.

The existing structured compatibility exporter builds scalar row dictionaries
and then rebuilds Arrow arrays. The candidate reuses the native sink's prepared
file, generation identity, bound projection/filter and source-order limit.
Native arrays remain native until the requested Arrow IPC or Parquet boundary.
Pinned Vortex 0.85 `ArrowSession::execute_arrow` performs that conversion;
Arrow 58.3 `FileWriter` and Parquet 58.3 `ArrowWriter` serialize batches. These
are existing compatibility providers, not execution engines or new dependencies.

The first admission is a nonnullable root struct containing nullable or
nonnullable bool, integer, float and UTF8 fields. It admits existing source-field
projection/renaming, native filter pushdown and source-order limit. Constructed
lists/structs, residual predicates, extensions, Avro, multi-source plans and
aggregate result conversion remain outside this packet. Their existing paths
are not silently relabeled as this implementation.

The bounded profile caps source/output rows, fields, source-batch rows, output
batches, each string, expanded Arrow batch bytes and serialized output bytes.
Canonical native conversion uses the resident allocator. Before requesting
contiguous Arrow strings, the adapter checks lengths in canonical native views
without constructing scalar values or row dictionaries. Retained footer records
are reserved before writer creation. The shared conversion/write grant grows
from the checked expansion bound before Arrow conversion and remains at its
largest admitted size until the writer drops. Parquet uses PLAIN, uncompressed,
dictionary-disabled pages and flushes each batch as a row group. This physical
policy is explicit and must be measured against the existing writer. It avoids
an unbounded codec dictionary; it is not a universal best-codec recommendation.

Provider allocations bypassing the resident allocator remain a limitation.
The reservation is an admitted conversion/writer envelope and the counters name
their scope; it is not a general allocator intercept or process-RSS ceiling.
Arrow logical buffer bytes are not presented as physically copied bytes. The
adapter's zero scalar-row construction does not imply zero decode or zero copy
inside Arrow/Vortex. Native encodings, source layout statistics and user metadata
are not preserved by these compatibility outputs; logical dtype, order, names
and validity are preserved for admitted fields.

The typed work record distinguishes the summed conservative Arrow expansion
admission, observed maximum retained Arrow batch size and Parquet's measured
in-progress writer bytes after each batch and before its explicit flush. That
Parquet counter excludes closed row-group metadata and transient allocations
inside `write`; it is not a peak over every instant or every writer allocation.
IPC reopen validates all batch row counts; Parquet reopen validates its native
footer row count and Arrow schema. Complete independent value checks for both
formats are separate test/benchmark oracles.

Vortex 0.85 omits a scan task's array when its filter is all false. Consequently,
the returned native-batch count cannot establish zero reads or zero decoding.
The read/decode/materialize flags conservatively mark every nonempty-source scan
that was not pruned at file admission, including zero-match scans. They are
explicitly labeled scan-scope evidence, not measured payload bytes; lower layout
pruning can still avoid work. Arrow conversion counts only batches actually
converted. The regression includes an absent value inside the file's min/max
range and verifies complete empty output without misreporting the scan as free.

Source generation checks bracket actual native execution and final publication.
Safe staging uses the same `OwnedOutput` as the native sink: exclusive creation,
checked checksum readback and atomic create-if-absent publication. Existing
destinations are rejected even if overwrite is requested, because a preceding
generation check cannot make POSIX replacement conditional. Failures and
cancellation remove only owned staging; a published destination is preserved
if staging unlink fails. The output byte cap checks before forwarding writes.
No partial output is a successful result.

Acceptance requires complete independent IPC/Parquet reopen equality for empty,
nullable, renamed/reordered, filtered and limited results, including large
integers; denied conversion/output limits; cancellation and source mutation
after a real batch; output collision; and released owned credits. A repeated
prepared handle must execute and validate again without cached answers. Latency
comparison must include complete write/finish/sync/checksum/reopen/publication,
separate preparation and independent value validation, and actual output sizes.
The candidate is retained only after those gates and a matched existing-route
comparison. No unmeasured speedup or broad PERF completion follows from this note.

The ignored release lifecycle fixture uses 4,096 and 65,536 rows, both formats,
one warmup and seven alternating legacy/candidate pairs. It calls the legacy
exporter directly so the new public dispatch cannot replace the control. Fresh
candidate timing includes preparation and joined source teardown. Because the
legacy API omits the candidate's sync, checksum and reopen checks, the report
keeps original API elapsed separate from complete-artifact elapsed with those
checks added. Source-generation guarantees still differ. Complete independent
schema/value comparison runs outside both clocks. Repeated prepared writes
have separate preparation/close scope and must show one source open, advancing
execution counters and zero owned bytes after final drop. These are local warm
file measurements, not cold-device, transport or process-RSS claims.

Replay, under the repository's serial benchmark and local-artifact guards:

```bash
CARGO_TARGET_DIR=/Users/dylan/.cache/shardloom/cargo-target cargo test -p shardloom-vortex --release --features release-user-surfaces --lib local_primitives::columnar_compat_sink::tests::benchmark::columnar_compatibility_release_lifecycle -- --ignored --exact --nocapture --test-threads=1
```

The runner must pin source revision and test-binary SHA outside the measured
process. The fixture emits source-file SHA, provider version, actual feature,
all sample timings, typed work counters, output sizes and complete-value checks.
