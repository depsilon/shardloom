# Executable block recipes — C2.a

Status: bounded candidate in progress under PERF-INTAKE / RFC 0044. No speedup or
retention decision is established yet. R8's evidence merged in PR #1466.

Prepared aggregates already retain logical lowering and source identity. Native
accessors bind the current array owner, physical width and validity per block;
scalar numeric loops already dispatch outside their row loop. Reimplementing
those mechanisms or sharing mutable aggregate state is unnecessary.

The complete saved R9.b result envelopes include UTF8 timing omitted from the
flattened CLI fields. Q29's fastest candidate call records 6,250,928,539 ns in
the first-pass accessor span, including 2,475,530,654 ns of provider execution
and 3,771,199,276 ns of dictionary construction. The remaining 4,198,609 ns is
an inclusive remainder, not a pure binding timer or a bound on row-update cost.
The saved source is `paired43_20260926T234401225793Z/q29_completed.tar.xz`, member
`q29_run3_candidate.stdout.json`. This does not establish a cross-block accessor
cache opportunity.

The [baseline extraction](../benchmarks/recipe-route-attribution-2026-09-27.json)
preserves all three candidate runs for Q10/Q23/Q29, plus all 12 Q34/Q35
control/candidate route records. Its raw archive includes the original complete
envelopes, manifest, identities and extraction script. This closes the route
evidence archival gap raised in PR #1466; no new timing is claimed by extraction.

One distinct repeated operation remains in mixed grouped exact-DISTINCT updates:
the existing pair-preunion loop dispatches ordinary COUNT/SUM/AVG measures and
numeric physical types for each row. Screen a block-local recipe that binds
the current typed slice and validity once, then updates the existing per-group
states in original row/measure order. Keep exact pair preunion, group lookup,
DISTINCT state, merging, sorting and output unchanged.

Initial admission is COUNT(*) and identity SUM/AVG over retained native numeric
owners, without offsets; all other shapes retain the existing native update.
Bind afresh for every block. A recipe borrows that block's owners and cannot
escape or reuse another dictionary, validity mask or source generation. No cache,
JIT, new dependency, unsafe code, result reuse or external execution is introduced.

Vortex-first decision: use the existing Vortex 0.85 PrimitiveArray and native
validity provider through ShardLoom's current accessor boundary. The recipe only
specializes ShardLoom's aggregate consumer; it does not replace Vortex decoding
or claim zero decode. Fresh mutable state, resource admission and source checks
retain their current scope.

First verify numeric widths, nulls, changed blocks, overflow/nonfinite diagnostics,
unchanged error order and exact DISTINCT/ordinary measure multiplicities. Compare
the complete public operation against a frozen control with all samples and
complete result validation. Retain any useful measured gain under the maintainer's
policy; no one-second rejection cutoff applies. A positive candidate requires
full applicable UAT, required Rust/native checks and independent review before
merging. A failed candidate must be removed with its evidence preserved.
