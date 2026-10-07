# Native join membership experiment

Status: **dropped** after the first frozen complete-operation screen. The
[report](../benchmarks/native-join-membership-2026-10-07.md) preserves its 6.88%
mostly-absent primary-score improvement and the disqualifying 4.17%/3.28%
high-match control regressions. All 941 accepted runtime assets are restored.
This document describes the removed test-only candidate, the second conditional
experiment after adaptive decimal. No public capability or grouped-directory
prototype is reopened.

## Contract and placement

The existing prepared relational executor completely consumes the right input
before starting the left input. A filter can therefore be sealed at that boundary
and owned by that single `Join`. It cannot outlive the build table, be shared with
another execution, or accept subsequent build rows. Existing source generation
validation still brackets the complete operation.

The first candidate admits only inner equijoins without a residual condition,
with 16,384 through 1,048,576 distinct exact-index entries. It consumes the existing
normalized key hashes, including compound keys. NULL keys remain excluded by the
existing join hash contract. Equal hash values are deduplicated for construction;
they never establish key equality. Every possible match enters the unchanged
exact index, equality comparison, duplicate chain and native output path.

Use an original three-segment, eight-bit binary fuse implementation based on the
[published algorithm and sizing formulas](https://arxiv.org/html/2201.01174v1).
Power-of-two segments, segment-ordered construction, singleton peeling and reverse
fingerprint assignment follow that algorithm. Deterministic seeded mixing and at
most four attempts bound construction. Verify membership for every distinct build
hash before publishing. Exhausted construction attempts disable the optimization;
they never publish a partial negative-answer structure. This uses no external
engine, copied implementation or new dependency.

Construction hashes, segment ordering, counts, XOR state, work stacks and final
fingerprints all retain shared memory credits. Skip the optional filter before
allocation when its conservative peak estimate exceeds a quarter of currently
available query credits. Check cancellation throughout construction, verification
and probing. Sorting has checks immediately before and after its bounded call.
The normal exact path remains authoritative when the filter is ineligible.

## Vortex-first provider decision

Decision: `implement_shardloom_kernel`. The pinned Vortex 0.85.0 array/file sources
and current provider inventory do not offer a static membership filter over
ShardLoom's normalized native join keys. The scalar-statistics documentation
mentions Bloom filters conceptually; it is not a callable join-membership provider.
Reuse `Batch`, `Table`, `RowIndex`, the native key normalization and `ReservedVec`.
No new layout, scan, source or result representation is introduced. The experiment
does not avoid input hashing or claim fewer decoded bytes. Native I/O certificates
and source checks stay on the existing prepared relational route.

## Screen and decision

The first screen selects an explicit per-prepared-operation test strategy within
one release test executable. There is no global switch or public strategy option.
Non-test production behavior remains unchanged until a successful screen warrants
a separately built production confirmation.

Freeze native fixture files, independent complete-output oracles, source hashes,
compiler/features, executable hash, helper hashes, memory grant and the paired
protocol before scored timing. The cohort includes mostly absent primitive,
long-string, compound and duplicate-preserving probes, plus 50% and 90% matching
controls and a small ineligible build. Include filter construction, exact index
construction, native input, every output row, output hashing, final validation
and owner release in each complete-operation clock. Fixture creation and external
oracle generation are outside it. Record prehashing and uncontrolled cache state.

Use three warmups per strategy followed by 15 alternating pairs, five complete
operations per member. The primary score is the geometric mean of per-cell median
paired ratios for the four mostly absent cells. Retention requires at least 3%
lower primary time, a stratified paired-bootstrap 95% upper bound below 1, at
least two primary cells at least 3% faster, and no cell more than 3% and 100 us
slower per operation. Confirm a passing screen with a fresh reversed-order cohort
and then the real production build; do not retune gates after observing results.

An untimed mechanism pass must count non-null probes, filter rejections, exact
lookups, construction attempts, distinct hashes, final bytes and credited peak
state. It must show no build-key rejection and complete oracle equality. Test
colliding hashes, duplicate keys, NULLs, empty/tiny sets, signed normalization,
resource denial, bounded construction failure, cancellation and late source
changes independently of performance. If the complete-operation gate fails,
preserve the candidate and raw evidence and restore the accepted runtime.

This is a join-workload decision under PERF-02/PERF-03/PERF-10/PERF-12 and CG-5/CG-6
evidence discipline. It is not a ClickBench or universal engine speedup. Metadata
pruning remains earlier; capillary batch ownership and dynamic memory admission
bound the experiment. PulseWeave scheduling is unchanged. Completion-aware input,
rematerialization and the other conditional-work tracks retain their own queues.
