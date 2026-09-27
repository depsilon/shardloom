# Packed numeric predicate admission — C6

Decision: retain the existing Vortex provider and drop a duplicate packed
comparison kernel/new BitWeaving layout at this screen. No new runtime candidate
was timed and no speedup is claimed. This does not close every numeric decode or
reader-evidence obligation.

The pinned Vortex 0.85.0 BitPacked comparison provider accepts a matching integer
constant and all six comparison operators. It combines unpacking and comparison
into the result mask, including validity and patched values. ShardLoom already
lowers ordinary numeric predicates to Vortex expressions with constants cast to
the column's physical type, then binds them into the native scan filter. A new
scalar predicate over expanded integers would duplicate an existing provider.

The original [BitWeaving proposal](https://15721.courses.cs.cmu.edu/spring2016/papers/li-sigmod2013.pdf)
depends on a physical representation suited to its comparison mechanism.
FastLanes packing and BitWeaving layouts are not interchangeable. No measured
remaining boundary currently justifies adding conversion, storage and reader
contracts for another layout. No external implementation code or dependency is
imported.

Five new provider fixtures pass. The BitPacked fixtures assert that the kernel
itself returns an admitted result, then compare every mask value against
independent scalar expectations. They cover unsigned integers, signed maximum patches, nulls,
empty/all-null inputs, block tails, offset slices and all six comparisons. A
shared-lowering fixture verifies a renamed UInt16 field with a UInt64 frontend
literal. Negative integers are explicitly rejected by the pinned BitPacked
encoder; the fixture checks that error instead of constructing invalid packed
input. Frame-of-reference coverage includes negative values after wrapping.
These are provider and lowering proofs, not evidence that every
ClickBench numeric column executes that kernel.

Frame-of-reference remains a separate boundary. Its pinned specialized kernel
accepts equality/inequality but declines ordered comparisons because subtraction
can wrap. A fixture checks that restriction around signed overflow and verifies
the complete native expression's exact result. General Vortex execution may
materialize values; that is not an external query-engine fallback or an encoded
kernel claim. The [reviewed 0.86.1 provider](https://github.com/vortex-data/vortex/blob/0.86.1/encodings/fastlanes/src/for/compute/compare.rs)
retains this restriction, so a version
bump alone must not be credited with removing it.

One older ShardLoom bridge still expands unsigned, all-valid, unpatched BitPacked
arrays into `Vec<u64>` via `unpacked_chunks`, computes statistics, and labels the
portable batch `BitPackedUnsigned`. Native scans collect that bridge's evidence
after provider filtering; their aggregates consume the original native arrays.
The bridge's `values_mapped_without_decode` and `data_decoded` defaults do not
describe that expansion accurately. The bridge must not be used as evidence of
the fused native provider's activation. Replacing its value ownership and
correcting its evidence contract remain explicit follow-up work; silently
dropping the existing callable bridge is not this decision.

The retained Full43 packet lacks per-kernel activation and per-encoding evidence
expansion timing. Its total decode/copy counters cannot establish either the
cost of this bridge or savings from another numeric layout. Reopen C6 runtime
work when a remaining packed/FoR boundary has measured cost, exact provider
activation and complete-call evidence. No percentage cutoff applies.

Provider classification: `use_vortex_native_provider`, feature-gated by the
existing local-native surfaces and isolated in `shardloom-vortex`. No new public
route, persistent format, external execution fallback or package publication.
The phase queue and CG-5/CG-6 gates remain open beyond this bounded screen.

The [portable evidence](../benchmarks/lookup-packed-provider-admission-2026-09-27.json)
records source/provider identities, literal mask checks, required workspace/native
validation and the companion C3 attribution. Reproduce the focused proof with
`cargo test --offline -p shardloom-vortex --features release-user-surfaces packed_numeric_provider_tests`.
Only test modules and documentation change; the final C4 runtime's complete
Full43 UAT remains applicable. No repeated ingest or query timing is claimed.
