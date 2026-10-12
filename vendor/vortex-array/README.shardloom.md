# Vortex timestamp and statistics patches

This is the Apache-2.0 `vortex-array` 0.85.0 published source with a focused
correction for timezone-free microsecond timestamp scalars. Vortex's timestamp
extension uses Int64 storage, but the original scalar validator constructs a
calendar span that can panic on valid storage values. Its display path repeats
the conversion. This prevents safe extrema statistics and compression for the
full storage domain already admitted by ShardLoom.

The timestamp changes are confined to `src/extension/datetime/timestamp.rs`:

- `Timestamp::unpack_native` retains every Int64 value for microseconds without
  a timezone after the existing storage type validation.
- `TimestampValue::fmt` uses Jiff's checked direct microsecond conversion for
  that case. In-calendar display stays unchanged; other values render exactly
  as `<integer>us since Unix epoch`.

The persisted-output work also changes count and byte-size merges in
`src/stats/stats_set.rs` to checked `u64` addition. Overflow invalidates the
statistic instead of panicking or wrapping, and the representation is independent
of host pointer width. This is used by the bounded `vortex-layout` file collector.

Other temporal units and timezone policy are unchanged. The extension identity,
metadata serialization, comparison rules, native arrays and file format are
unchanged. There is no Arrow intermediate, new query engine, unchecked scalar
construction or dependency-version change. Ordinary calendar operations can
retain their own narrower domains; scalar storage admission is not a claim
that all calendar functions support every Int64 timestamp.

`upstream-provenance.json` records the reviewed archive, all original member
hashes, VCS metadata and verified Apache license. The archive is authoritative
because its recorded VCS state is dirty. `Cargo.toml.orig` remains unchanged;
the normalized manifest adds `publish = false`. The original crate-local lockfile
is recorded but not copied because the workspace owns dependency resolution.
Upstream copyright headers remain in every source file and each modified source
file has an explicit change notice. Two original trailing-whitespace lines are
also normalized: a blank documentation line in `src/arrays/patched/mod.rs`, and a
snapshot literal in `src/scalar_fn/fns/zip/mod.rs`. The latter uses an explicit
space escape in an ordinary string to preserve exactly the original expected
snapshot bytes. Neither normalization changes provider behavior.

The adapter regressions live in `shardloom-vortex/src/native_temporal.rs` and
the existing writer tests. The full acceptance contract is
[`native-timestamp-provider-2026-10-11.md`](../../docs/architecture/native-timestamp-provider-2026-10-11.md).
External readers built with the unpatched provider can still inherit its calendar
restriction; this patch does not certify them.

Remove this patch when an approved upstream release preserves the full storage
domain and passes scalar, compression, statistics, pruning, writer, resource,
package and complete-workflow checks. Keep the existing feature gates and shared
resource owners when changing the provider.
