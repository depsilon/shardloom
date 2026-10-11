# Vortex timestamp comparison validity patch

This is the reviewed Apache-2.0 `vortex-datetime-parts` 0.85.0 published source.
The focused change in `src/compute/compare.rs` preserves null timestamp values
when its encoded comparison kernel combines date/time components.

Timestamp validity belongs to the days child; seconds and subseconds are
non-nullable. Kleene Boolean AND/OR can therefore replace a null comparison
with false/true. `CompareKernel::compare` retains the existing encoded component
work and applies the days validity to its handled result through Vortex's native
mask operation. Unhandled comparisons retain their existing dispatch. The
array identity, compression algorithm, dtype, file representation and dependency
versions are unchanged. No Arrow conversion, new engine or independent pool is
introduced.

`upstream-provenance.json` records every original member hash and the published
archive identity. The archive is authoritative because its recorded VCS state is
dirty. Copyright headers remain in source files, and the modified file carries
an explicit change notice. `Cargo.toml.orig` remains unchanged; the normalized
manifest adds `publish = false`. The original crate-local lockfile is recorded
but omitted because the workspace owns resolution. The verified Apache-2.0
license is retained alongside this notice.

ShardLoom regression tests execute whole encoded comparison arrays as well as
actual compressed files. Scalar-at-a-time evaluation can bypass the affected
kernel. Tests cover full-domain timestamps, nulls, all-null/empty inputs,
statistics and filtered result equality; see the
[provider contract](../../docs/architecture/native-timestamp-provider-2026-10-11.md).

Remove this patch when an approved upstream provider preserves strict timestamp
comparison validity and passes the recorded source, file, resource and complete
workflow checks. External readers using an unpatched provider can retain this
defect; this patch does not certify them.
