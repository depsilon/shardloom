# Compiler profile screens — C5.a/C5.b/C5.c

Status: bounded build experiments under PERF-INTAKE / RFC 0044. No optimized
profile is selected by this plan, and no new performance claim is established.
The C1 provider upgrade was declined at resource admission; these comparisons
hold the retained Vortex provider, ShardLoom runtime, input and policy fixed.

1. C5.a compares the existing `release-lto` profile (ThinLTO, one codegen unit)
   with ordinary `release` on the final C2 runtime.
2. C5.b compares trained `release-pgo` with the selected portable control. Use the
   guarded instrument/train/merge/use helper, matching Rust/LLVM tools, and a
   declared training corpus covering native ingest, queries and pressure. Retain
   separate untrained values/distributions for evaluation; training timing is
   not an evaluation result.
3. C5.c compares the existing `release-native-benchmark` with an explicit native
   CPU target against the selected portable control. Record the resolved CPU,
   architecture and flags. A local CPU-specific artifact is not a portable
   distribution binary.

Freeze each binary with source/content identity, dependency lock, feature set,
build command, toolchain, flags, build duration and size. Keep generated outputs
under the local-only Cargo/UAT roots with disk, log and child-cleanup guards.
Run local builds and measurements sequentially. Do not change source during a
build. Existing runtime tests remain applicable to unchanged source; every
candidate must independently validate complete results through its actual binary.

Use symmetric best-valid comparisons with every observation preserved. A broad
retained build needs complete paired Full43, guarded native ingest and disjoint
held-out acceptance, including resource/error behavior. Stage a bounded screen
first when it can reject a consistently slower variant without more bulk work.
No percentage cutoff discards a useful gain. Any default-profile or packaging
change requires its own final artifact verification; this plan changes neither.

These are compiler experiments over ShardLoom-native execution, with explicit
no-fallback evidence. They do not introduce JIT execution, a new provider or a
new operator family. Failed candidates leave the retained portable build intact.
PERF-13 and the broader CG-5/CG-6 obligations remain open beyond these screens.
