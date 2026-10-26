<!-- SPDX-License-Identifier: Apache-2.0 -->

# Local ADBC provider review

October 26, 2026; connected programming package, RFCs 0023/0033/0044.
Implementation and independent-client local acceptance pass in the
[connected-programming record](../benchmarks/native-connected-programming-2026-10-26.md).
Hosted platform checks and publication are separate gates.

The optional Python native build uses Apache ADBC's released Rust
[`adbc_core` 0.24.0](https://docs.rs/crate/adbc_core/0.24.0/source/Cargo.toml)
and [`adbc_ffi` 0.24.0](https://docs.rs/crate/adbc_ffi/0.24.0/source/Cargo.toml).
Both are Apache-2.0, require Rust 1.85, and admit Arrow `>=58,<60`. The resolved
workspace retains its single Arrow 59.3.1 graph. These two crates add no query
engine or storage evaluator. Their published checksums are recorded in
`Cargo.lock`. No upstream implementation is copied into ShardLoom.

Both published crate archives identify upstream commit
`a3f81c6feb1133ba80600ac32e6d7c0b0b407e18`; their archive checksums match the lock.
The crate archives omit their root notice files, so the Python distribution
bundles that exact revision's [LICENSE.txt](https://github.com/apache/arrow-adbc/blob/a3f81c6feb1133ba80600ac32e6d7c0b0b407e18/LICENSE.txt)
and [NOTICE.txt](https://github.com/apache/arrow-adbc/blob/a3f81c6feb1133ba80600ac32e6d7c0b0b407e18/NOTICE.txt),
verified against their Git blob identities. The existing conservative notice
bundle retains all previously locked components and adds these two crates.

The optional Python consumer is Apache-2.0
[`adbc-driver-manager` 1.12.0](https://pypi.org/project/adbc-driver-manager/1.12.0/),
with PyArrow at the explicit scalar-parameter and result-export boundaries.
ShardLoom implements the released safe
[`Driver`/`Database`/`Connection`/`Statement` traits](https://docs.rs/crate/adbc_core/0.24.0/source/src/sync.rs).
The reviewed upstream
[`export_driver!` macro](https://docs.rs/crate/adbc_ffi/0.24.0/source/src/driver_exporter.rs)
owns the C ABI initialization, pointer validation, error/release callbacks and
Arrow C stream adaptation. Handwritten ShardLoom code contains no unsafe block;
the workspace's `unsafe_code = "forbid"` remains unchanged. Macro-generated and
dependency-internal unsafe code still forms a real FFI trust boundary.

An explicitly selected live context supplies a short-lived, one-use bootstrap
capability. A bounded pending map retains the Python bridge only until database
initialization or scope exit. It is not a global/default session. The database,
connection, prepared declaration and result stream retain their actual owners.
Closing a connection cancels its own streams without closing the borrowed
context. Native batch buffers retain their existing grants after export.

Named parameter batches contain exactly one row and at most 1 MiB of buffers.
Parameter conversion is explicit and bounded; result rows travel directly from
the existing native execution stream to Arrow batches. Each execution consumes
its parameter binding and replans parameter-sensitive work. Prepared statements
snapshot named relations without executing them. Schema/catalog inspection uses
the same metadata admission as ordinary DataFrames, never row sampling.

This is a local embedded adapter, not a standalone database server. Transaction
mode, SQL writes, parameter streams, bulk execution, remote partitions,
Substrait execution and unsupported catalog statistics return explicit errors.
No fallback provider or implicit context is available. DB-API uses the Apache
manager's standard interface; notebook SQL returns the ordinary lazy DataFrame.

Required acceptance includes independent manager loading, metadata/filter
shapes, nullable/exact parameters, partial fetch, rebind/no stale values,
catalog snapshot lifetimes, context/connection closure, retained buffers,
cancellation and late producer failures. A successful compilation alone does
not establish driver support.

Installed-package verification selects the connected-programming, scalar catalog,
observation and independent-client suites alongside the existing native binding
tests. Missing suites or skipped client tests fail that proof. Its offline test
environment uses hash-locked ADBC manager 1.12.0 and
[IPython 8.37.0](https://pypi.org/project/ipython/8.37.0/), whose Python 3.10 minimum
matches the package. The optional notebook extra may select a newer compatible
IPython; the development checks additionally exercise 9.17.1.

The [test-client intake](evidence/native-adapter-clients-2026-10-26.json) records
exact release metadata, dependencies, licenses and wheel hashes for the Linux
and macOS proof targets, including Python 3.10's exception-group dependency.
The closure uses Apache-2.0, BSD, MIT, ISC and PSF-2.0 licenses. These are external
test clients, not copied/bundled code or additional query execution providers;
ADBC's unrelated database and test extras are not installed. Wheel availability
and hash validation are separate from actual platform execution acceptance.
