# Arrow Python adapter compatibility patch

This is the Apache-2.0 `arrow-pyarrow` 58.3.0 published source, with the PyO3
0.29 compatibility change from Apache arrow-rs
[PR #10134](https://github.com/apache/arrow-rs/pull/10134) backported. The
workspace pins PyO3 0.29.3, outside RUSTSEC-2026-0176 and RUSTSEC-2026-0177's
affected ranges. No advisory waiver is used.

The patch changes the PyO3 dependency and explicitly converts an invalid-UTF-8
capsule-name error to `PyValueError`. It retains 58.3.0's aligned-buffer import
and the existing Arrow/Vortex dependency graph. It does not change operators or
introduce an execution engine. The crate remains optional through
`shardloom-python/python-native`.

`upstream-provenance.json` records the original registry archive and member
hashes, upstream patch identity, and exact-version Apache license and notice.
`Cargo.toml.orig` is the unmodified original manifest. The normalized manifest
is also marked `publish = false`; workspace `Cargo.lock` controls resolution.
The Python notice bundler includes both `LICENSE` and `NOTICE`.

Remove this patch when the approved Arrow/Vortex graph provides an adapter with
the patched PyO3 requirement. Validate native import/export, stream cleanup,
retained owners, invalid inputs, dependency policy, installed wheel behavior
and the Rust minimum version when changing the provider.
