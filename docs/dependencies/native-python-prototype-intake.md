# Native Python prototype dependency intake

Status: source-grounded direct dependency proposal; isolated graph resolution,
license inventory, compilation and local import tests pending. This document
does not approve a package release or alter the production Python dependency graph.
The unbuilt prototype and its standalone manifest remain on the local
`codex/perf-remaining-work` checkpoint `be2a69f3`, outside the retained-runtime
shipping batch. The paths below describe future validation of that checkpoint.

| Item | Exact proposal | Role | Provider license |
|---|---|---|---|
| `pyo3` | `=0.29.2`; default features disabled, `macros` enabled | Safe Python classes, explicit native calls and Python-owned result conversion | MIT OR Apache-2.0 |
| `pyo3-build-config` | `=0.29.2` | Platform-specific extension link arguments | MIT OR Apache-2.0 |
| `shardloom-core`, `shardloom-plan`, `shardloom-vortex` | Existing local paths | Existing admitted operators and retained Vortex result owners | Existing repository Apache-2.0 |

The exact provider source is [PyO3 tag v0.29.2](https://github.com/PyO3/pyo3/tree/v0.29.2).
Its [manifest](https://github.com/PyO3/pyo3/blob/v0.29.2/Cargo.toml) records the
dual license; the [Apache license text](https://github.com/PyO3/pyo3/blob/v0.29.2/LICENSE-APACHE)
permits the selected Apache-2.0 use. The proposal uses documented APIs and original
wrapper code, with no provider implementation copied into this repository.

The exact upstream [forbid-unsafe compile-pass test](https://github.com/PyO3/pyo3/blob/v0.29.2/tests/ui/forbid_unsafe.rs)
checks the same macro families used by this experiment. ShardLoom-owned code
continues to forbid unsafe Rust. FFI remains inside the reviewed provider; this
does not assert that upstream contains no unsafe implementation.

The provider [distribution documentation](https://pyo3.rs/v0.29.2/building-and-distribution.html)
defines the explicit `PYO3_PYTHON` interpreter choice and extension-link mode.
The proposal keeps `PYO3_BUILD_EXTENSION_MODULE=1` specific to the extension
build, avoiding that mode for the ordinary Rust control executable. It does not
introduce setuptools-rust, maturin, a wheel repair dependency, NumPy, pandas or an
external execution engine. Initial acceptance targets the root's exact local
CPython 3.13 build; ABI3, additional Python/platform support and distribution are
not established by provider support statements alone.

Before retaining the prototype, root must resolve the standalone manifest's
`Cargo.lock`, retain its hash and exact graph, check the license metadata of all
resolved new packages and confirm no fallback engine or incompatible duplicate
native provider entered the graph. Macros/build helpers are dependencies too.
The broad registry graph is not inferred from the two direct pins. A local
`cargo metadata --manifest-path experiments/python-native/Cargo.toml --format-version 1`
record and the lockfile provide the source for that check; neither has been run
by the implementing agent. Subsequent builds should use the reviewed lock.

No production manifest or lockfile is changed by the proposal. The adapter's
feature-gated result sink and metadata accessor themselves add no dependency.
Any future distribution must carry required dependency license/notice material
and pass the existing package-channel governance. The current artifact is an
unpublished local experiment with no new release claim.
