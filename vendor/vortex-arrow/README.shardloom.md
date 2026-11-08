# ShardLoom Arrow decimal storage boundary

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
