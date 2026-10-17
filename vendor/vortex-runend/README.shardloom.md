# Vortex RunEnd buffer-table patch

This is the Apache-2.0 `vortex-runend` 0.85.0 published source. String and binary
expansion rebuilds views and validity through the original VarBinView array,
preserving its shared data-buffer table and allocator reservation. The existing
RunEnd decode loops, value semantics and native serialization are retained.

`upstream-provenance.json` records every original registry member, the archive
checksum and dirty VCS marker. The published archive is authoritative. The
original crate lockfile is recorded but omitted; the workspace controls
resolution. `Cargo.toml.orig` is unchanged and the normalized manifest disables
publication. The reviewed upstream Apache license is included.

`goldenfiles/runend.metadata` is supplemental test data from the recorded upstream
Git revision, omitted from the registry archive. Its Git object identity, bytes
and SHA-256 are recorded separately; the associated test source matches the
published crate exactly. The full library suite uses that fixture unchanged.

The [ownership contract](../../docs/architecture/native-varbin-metadata-resources-2026-10-16.md)
defines the finite claim and acceptance checks. No external execution engine or
new unsafe boundary is introduced. Retire this patch only after an approved
upstream provider satisfies the same ownership and complete-workflow checks.
