<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native typed comparison and keys

Status: implementation contract under PERF-02/03/06/07/10/11/12 and
CG-3/5/19/20/21. The [phase plan](phased-execution-plan.md) owns sequencing.
This continues the accepted [typed payload contract](native-typed-payloads-2026-10-03.md)
and [universal workflow plan](universal-workflow-completion-2026-10-01.md).

## Decision and semantics

Extend the existing native relational key owner to Binary, Decimal128 with
precision 1–38 and scale 0–precision, Date32 and timezone-free microsecond
timestamps. SQL, DataFrame and Rust declarations lower to the same binder and
key owner. Do not create another join, group, sort, set or expression executor.

- Binary ordering is lexicographic unsigned-byte order; empty bytes are a value,
  distinct from NULL. Binary and UTF8 remain different logical types.
- Decimal comparison is exact signed i128 comparison only when precision and
  scale match. Never rescale, convert to floating point or compare to integer
  storage implicitly. Validate selected precision without provider panics.
- Date32 compares signed epoch days and timestamps compare signed microseconds,
  across their complete storage domains. Keep both types distinct from integers
  and from each other. Other units, timezones and extensions remain unadmitted.
- Preserve the existing operator-specific NULL rules: ordinary comparison
  propagates NULL; joins do not match null keys; sets and grouping identify nulls;
  ordering requires explicit null placement when a null is encountered.
- Equal admitted keys hash identically across physical encodings. Hashes only
  select candidates; equality compares complete typed values. Decimal metadata
  and the new logical type tags participate in hashing.

Admit these keys for existing relational joins, sets, grouping, ordering, window
partition/order and membership/correlation. Extend COUNT, COUNT DISTINCT, MIN
and MAX, preserving exact typed extrema. Same-type column comparisons, IS NULL,
IS NOT NULL, CASE, COALESCE and NULLIF use the existing expression machinery.
Typed literals, new casts, arithmetic/rescaling, retained unary state and nested
key equality remain separate semantic work under the same broader phase owners.
They must keep deterministic bind-time denial, including on empty input.

## Vortex-first provider check and reuse

Decision: `implement_shardloom_kernel` by extending the already admitted shared
key semantics. Vortex 0.85 supplies canonical DecimalArray, VarBinViewArray,
ExtensionArray, dictionary codes/values and validity. Its extension comparison
kernel also requires equal logical extension dtypes before comparing storage.
Its ArrayHash API is structural array identity, not this runtime's row-key hash
contract. Native array comparisons do not replace the existing arbitrary-row,
cross-batch hash/equality/order contract used by all relational state owners.
Retain that shared contract and reuse provider storage through the approved
`vortex-local-primitives` boundary; add no dependency or external engine.

| Existing component and callers | Shared extension |
| --- | --- |
| `native_relational_keys`: join, set, group, sort, windows, subqueries, expressions | Extend logical cells; reuse original-width numeric owners for temporal storage, canonical decimals for exact values and one dictionary-aware variable-byte owner for UTF8/Binary. |
| Relational binder and expression binder | Separate typed key/value selection admission from legacy arithmetic/cast/unary-state admission. Preserve exact metadata compatibility and reject empty unsupported plans. |
| Relational aggregate state and shared result builder | Reuse MIN/MAX comparison and reserved persistent byte ownership for binary extrema; emit existing exact native payload values. |
| Relational order spill and native query run store | Preserve the same typed schemas through run writing, merge comparisons and compact final delivery. No new spill protocol or independent budget. |
| Shared public writers and source declarations | Deliver computed typed results through the accepted payload/writer contracts; no format or frontend executor variants. |

Dictionary binary keys retain code/domain sharing and reserve hash capacity before
work. Escape from a source domain into persistent extrema requires compact owned
bytes under the operation allocator. Decimal and temporal comparisons read native
storage without Arrow or terminal JSON conversion. Existing canonicalization and
provider scratch boundaries remain explicit; this is not a zero-decode or complete
process-RSS claim.

## Resource and acceptance contract

Retain one operation grant, native leases, cancellation and state admission.
The existing opt-in relational ORDER BY spill path applies to the admitted typed
keys and payloads only after forced-run/merge tests prove exact values, nulls,
stable ties, dtype identity and cleanup. Group/join/window state spill is not
implied by sort-run support. No cap or resource guard is removed.

The comparator is a columnar-engine maintainer reviewing equality/hash consistency,
ordering laws, logical identity, precision, null policy and retained ownership.
Tests must independently specify expected values across nulls, duplicate values,
different dictionary domains, decimal precision/sign boundaries, full temporal
ranges and mixed-type denials. Verify typed empty/all-null outputs and lazy branch
selection, then compose join/set/group/window/subquery/ordering results into real
writers and reopen complete values and schemas. Include constrained grants,
cancellation, spill failure/cleanup and output above the collection boundary.

Run focused tests while implementation changes. At the coherent boundary, run
required workspace/native/Python/feature/documentation checks, freeze the public
matrix and all previous still-applicable cases, then run paired Full43 under the
existing serial storage/process guards and predeclared investigation thresholds.
Retire prior typed-key denial cases only when replaced by explicit positive and
incompatible-type cases; record that transition in the evidence. Availability is
accepted on correctness and resource proof, not a required speedup.

All execution and Native I/O certificates retain `fallback_attempted=false` and
`external_engine_invoked=false`. Hosted dependency completion and the inherited
website advisory decision stay separate. Paused large format/text performance
runs, native Python bindings, publication and broader PERF/CG completion are not
authorized by this finite implementation contract.
