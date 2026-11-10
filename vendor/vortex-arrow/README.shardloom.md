# ShardLoom Arrow storage boundaries

This directory contains the Apache-2.0 `vortex-arrow` 0.93.0 published registry
source. The provenance record binds its archive, complete member hashes and
upstream license. The archive's lock is recorded but omitted; the workspace and
reviewed standalone lockfiles control resolution. Publishing is disabled.

The local change adds `ArrowSession::with_preserved_decimal_storage`, a scoped
clone that retains decimal physical widths, raw value bits and shared Arrow
buffer owners. It still checks metadata, buffer layout and row validity, and
uses the existing recursive import machinery. It adds no unsafe operation.
Ordinary provider imports retain upstream precision validation and narrowing.

ShardLoom selects this option only at its admitted resident or local ingest
boundary. Its ancestor-aware checks validate every reachable decimal value;
null-parent children and values outside the visible slice remain unobserved.
Legacy and managed ingest retain this check at conversion as well. The option
does not grant decimal execution or admit additional Arrow types. Native scalar
evaluation remains fallible and validates precision when a value is observed.

Provider tests cover every Arrow decimal width, slices, nullability, unchanged
default behavior, buffer identity and nested policy propagation. Native intake
tests cover reachable invalid values, null parents, list/struct/fixed-list
shapes, slices and last-owner release. Arrow is a compatibility boundary;
execution remains Vortex-native with no external engine fallback.

The November 9 change dispatches Variant chunks separately before an extension
exporter can hide them in a canonical Variant core and repeatedly redispatch
that same wrapper. A canonical shredded child is sliced alongside each core
chunk. The existing registered exporter performs each conversion; the existing
Arrow `MutableArrayData` concatenates only the explicit boundary output. This
does not add a Variant representation, decode rows inside ShardLoom execution,
or bypass the provider's value conversion. Empty sequences still pass through
normal exporter and target dispatch through a zero-length native constant.

Standalone dispatch regressions cover one/multiple chunks, row order, canonical
shredded coordinates, empty nullable/nonnullable inputs and rejected targets.
ShardLoom's integration tests additionally exercise the actual
Parquet Variant provider, complete raw/shredded values and final-child ownership.
The consumer admits and retains the terminal conversion memory separately.
