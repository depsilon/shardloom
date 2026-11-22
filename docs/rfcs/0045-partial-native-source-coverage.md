<!-- SPDX-License-Identifier: Apache-2.0 -->

# RFC 0045: Exact partial native source coverage

## Status

Locally retained after bounded validation, isolated wheel proof and prospective three-role
comparative evaluation (November 21, 2026) under the
[current queue](../architecture/phased-execution-plan.md#current-execution). See the
[comparative report](../benchmarks/native-chrysalis-partial-coverage-2026-11-21.md) and
[portable evidence packet](../release/evidence/chrysalis-partial-coverage-2026-11-21/README.md).
Hosted CI acceptance remains pending. It does not establish a general speedup claim,
a durable fragment format, remote acceptance, package publication, or completion of items
19/20 or CG-1–CG-23. The
[original request](../architecture/native-chrysalis-resonance-cells-2026-11-16.md)
continues to govern the following Resonance investigation.

## Summary and context

Ordinary local Parquet execution already admits immutable file generations,
unions exact source demand, prunes row groups, decodes selected columns in
bounded batches, and hands owned native arrays to native operators. Separate
prepared operations can repeat that conversion for overlapping columns and
regions. The existing session segment store retains serialized **Vortex file
bytes**; it does not supply projected native Parquet values.

A Chrysalis Map may retain completed native representations of exact source
regions within one existing resident session. A lookup can combine previously
realized fields with newly demanded fields at identical original row positions.
It must not confuse a filtered result, an incomplete prefix, a decoded batch,
or a successful individual read with complete reusable source coverage.

## Goals and non-goals

- Start with one local Parquet file and its natural row groups. Reuse the
  current source-demand, generation, native intake, memory and operation owners.
- Avoid repeated selected-column reads and Arrow-to-Vortex intake when complete
  compatible native coverage is already retained. Report actual work separately
  from logical rows served.
- Preserve nullable values, field order, exact row coordinates, demand-scoped
  errors, per-call limits, cancellation, retained results and memory refusal.
- Use the same realization path for eligible ordinary execution and explicit
  in-session realization; ordinary users need no durable preparation command.

No automatic durable cache, sidecar, new file family, global cache, remote source,
mutable table, cross-session reuse, automatic request coalescing, new scheduler,
approximate result, external execution engine or package publication is included.
This is not a result cache. First realization of Parquet still decodes it; native
array reuse does not establish that the original source was zero-decode.

## Decision and provider review

Classification: `use_vortex_native_provider` for existing arrays and array
operations; `wrap_vortex_concept` for ShardLoom's exact coverage and ownership
records. No new value representation or query engine is required.

The pinned graph is Vortex **0.93.0** and Parquet **59.3.2**, with the repository's
recorded local provider patches. The normative API evidence is the source in
this checkout and `Cargo.lock`, not a claim that the latest online API is pinned.

| Existing owner/provider | Use and limitation |
| --- | --- |
| `universal_format_io/demanded` and `SourceGroup` | Preserve `(file, row_group, first_row, rows)`, projection and pruning. Native operators still evaluate predicates. |
| `SourceIdentity::same_generation` and `validate` | Compare fresh admissions with the held local generation; validate both path and descriptor. This is Unix metadata identity, not a filesystem snapshot or content checksum. |
| `ResidentMemorySource::from_local_arrow_with_ownership` | Reuse controlled Parquet intake, strict/extended floating policy, dtype checks and allocation credits. |
| Vortex `ArrayRef`, `StructArray`, array slicing and `ChunkedArray` | Retain native representations and compose matching logical coordinates without converting values back to Arrow. Preserve validity and extension identity. |
| Vortex `SegmentCache` / `SharedSegmentSource` | Reuse remains appropriate for serialized segments/live segment futures. Neither proves selected Parquet fields' native value coverage. |
| Vortex file layout/writer and current preparation transaction | Remain the explicit full durable preparation/export path. Do not encode partial dependencies as a complete Vortex table. |
| Existing `ResidentVortexSession` and optional segment reclaimer | Own optional retention and operation completion. One registered reclaim endpoint must drain all optional source owners; a second competing endpoint is prohibited. |

The upstream [array](https://docs.vortex.dev/concepts/arrays),
[layout](https://docs.vortex.dev/concepts/layouts) and
[I/O](https://docs.vortex.dev/developer-guide/internals/io) documentation supplies
conceptual context. The checked-in provider implements native array/segment
primitives; ShardLoom must supply its own policy, coverage proof and credits.
The existing `universal-format-io` / native execution feature gates apply.
Residual evaluation stays ShardLoom-native; unsupported behavior fails explicitly.
Certificates preserve `fallback_attempted=false` and `external_engine_invoked=false`.

## Detailed design

### Identity and coverage

The internal key comprises the held source generation, natural row-group index,
original row interval, source field index, authoritative dtype/nullability and
the session's immutable interpretation/validation policy. File path or field
name alone is never sufficient. Fresh bindings may match the held generation;
pointer equality between separately admitted source objects is insufficient.

A field realization contains ordered native fragments whose intervals partition
the **entire** admitted row group without gaps, duplicates, overlap or changed
ordering. Fragment boundaries may differ between fields because adaptive batch
sizing remains active. Readers intersect fragment intervals and slice by exact
offsets before constructing the demanded native struct. The first implementation
must bound fragments rather than assume all row groups fit one decoder batch.

Only demanded fields are read and validated. Nullability and raw values must
match current intake. Reuse never promotes regional facts into whole-source
uniqueness, ordering, complete statistics, filter completion or aggregate facts.
Selected-column metadata pruning remains valid, but a downstream selected or
ranked population cannot be registered as full source coverage. Prefix reads
may consume completed coverage but cannot publish a truncated row group.

The first implementation may decline retention for an unsupported field type,
directory/table input, excessive region/fragment count, missing ownership proof,
contention or optional memory refusal. The supported direct source route must
continue unchanged. A retention decline is not permission to retry a failed
decoder, suppress a validation error or change the demanded domain.

### State and bounded interfaces

Use internal typed interfaces under the source/session modules:

1. **Lookup:** validate source identity and policy, then return exact completed
   field coverage and the missing field set for a requested region. Empty demand
   returns no fabricated row-bearing coverage.
2. **Admission:** reserve map/fragment bookkeeping before allocation and obtain
   an epoch-bound realization ticket. Optional refusal leaves direct execution
   available without reserving a second mandatory execution grant.
3. **Realization:** run the existing selected-column decoder and native intake
   for missing fields. Record exact row intervals and actual read/decode work.
   Do not read ahead into an undemanded region solely to fill the map.
4. **Validation:** prove complete row-group extent, matching field identity,
   source generation, immutable policy and retained allocation ownership.
5. **Publication:** install a validated resident region atomically, conditional
   on its epoch and successful enclosing operation completion. A failed,
   cancelled or abandoned operation publishes no reusable new coverage.
6. **Acquisition/release:** return native fragment leases. Removing a map entry
   drops only the map's references; outstanding consumers retain their credits
   until the final buffer/metadata owner drops.

Distinguish `in_progress`, `validated_resident` and `committed_fragment` in the
design. Only the first two are admitted by this resident investigation.
`committed_fragment` remains unavailable: it requires an independently approved
format/storage policy and commit proof. In-progress/failed work is never a hit.
Same-operation readers retain their existing semantics; this design does not
replace live sharing with a completed cache.

### Ownership, budgets and reclamation

Do not put `ResidentMemorySource` directly inside a session-owned map: it retains
the session and would create a strong ownership cycle. Retain detached immutable
native arrays and their existing buffer/metadata credits. Reconstruction creates
an operation-owned source handle using the current validated session.

Map metadata, held source identity, field descriptors, fragment arrays and
native backing capacity all require explicit accounting. Use the intake's
copied/transferred **allocation** charges, not visible slices or `nbytes`, as
backing evidence. Additional provider allocations need their own proof; decline
retention when that proof is unavailable. Shared credits are not refunded when
only an entry is evicted. Conservative retention accounting may overcount shared
backing, but must not undercount it or claim to measure total process RSS.

Initial finite ceilings are at most 128 retained realization records and 128
fragments per realization, with optional retained backing limited to the smaller
of 32 MiB and one sixteenth of the session's current memory grant. Metadata is
included in the limit. These are investigation ceilings, not new public options.
Recheck a narrowed configurable grant before admission. Reclaim optional regions
through the existing segment store's single weak endpoint; avoid invoking it
while holding map locks. Mandatory execution must not block on cache contention.

Eviction, generation invalidation, memory pressure and session teardown drain
pending and completed map ownership. Epoch changes prevent an earlier in-flight
ticket from repopulating a reclaimed map. Existing caller results and leases may
outlive eviction; their original credits remain charged. Retention must not
depend on a caller keeping an earlier prepared plan alive.

### Explicit preparation and durable output

The candidate Rust method `LocalColumnarSource::realize_native_columns(session,
columns, cancellation)` uses the same admitted demand, `ColumnarReader`, native
conversion and enclosing-operation publication interfaces. It returns a
`ColumnarReadReport`; retention is optional and can decline or be reclaimed.
It requires one local Parquet file, rejects empty/unknown columns and foreign
memory ownership, and writes no durable artifact. It does not masquerade as a
source-independent persisted artifact. Its acceptance remains subject to the
same cancellation, completion, resource and package gates as ordinary execution.

Full durable preparation retains the existing single `.vortex` transaction,
complete-source validation and destination policy. Any later reuse of resident
regions by that writer must use these same coordinates/owners and charge missing
realization, encoding, validation, write and commit work. A self-contained export
must finish every source dependency. This RFC does not authorize storing a
partial map as a standalone table or changing the existing on-disk format.

### Observation and failure behavior

Keep physical `data_read_bytes`, `data_read_requests`, row groups decoded and
rows decoded separate from rows served from retained native coverage. Report
actual missing-field decoding and cache hits with their scope; repeated source
admission metadata remains a retained admission fact, not a fresh read claim.
Empty/pruned sources do not invent decoder work or useful reuse.

The candidate adds `native_source_coverage_field_rows`,
`native_source_coverage_regions`, `native_source_coverage_retained_bytes` and
`native_source_coverage_scope` to ordinary completed relational evidence.
Field-row counts identify exact values delivered from prior successful native
operations. Retained bytes are the maximum observed session map plus credited
backing during a reader, including pending regions; they are neither a final
residency snapshot nor RSS. Existing decoder and copied/transferred intake
counters count actual new work only. Delivery-owner release remains mandatory;
bounded optional source backing is reported separately and may survive a call.
The native I/O certificate carries the same coverage counts and owner scope.
A fully reused compatibility source enters this operation as decoded native
columns; operator materialization remains explicit. Its zero fresh source
decode and intake counts do not establish whole-query zero-decode or zero-copy.

Validate generation before lookup, between source pulls and before publication.
Path replacement, deletion, in-place change and sticky invalidation reject old
prepared operations. A newly admitted changed source can execute directly, but
cannot inherit old coverage. Existing source identity race limitations remain
explicit; this does not promise snapshot isolation against arbitrary writers.

Optional retention failures discard only optional state. Actual source errors,
bad values, cancellation and mandatory allocation failure remain observable in
the same demanded domain and order. Use an internal fault control to prove that
publication failure cannot expose partial coverage. Reopening in a new session
has no resident coverage and performs ordinary source admission/read work.

## Alternatives and risks

Full preparation is already available, but eagerly realizes undemanded fields
and writes a complete artifact. Serialized Parquet byte caching would avoid
some reads but would not avoid the native conversion targeted here. A
source-object-only cache misses separately bound SQL/DataFrame operations.
A global cache introduces identity/lifetime rights absent from current sessions.
Copying native arrays into another store gives simpler independent capacity
accounting but adds work; use only if measured complete-workflow benefit and
ownership proof justify it.

Primary risks are memory retained after a short query, false coverage after
LIMIT/filtering, mismatched fragment coordinates, stale source generations,
shifted validation/error order and overhead on single-use/disjoint workloads.
Explicit limits, operation-scoped publication, exact coordinate tests and a
prospective complete-cohort gate address these risks. They do not establish a
performance benefit before measurement.

## Compatibility and acceptance

Ordinary public declarations, result values/schema, output formats and source
failure semantics remain compatible. Internal terminology already exists in
the [canonical glossary](../architecture/canonical-terminology.md). Diagnostic
additions must be additive and describe actual implemented observations only.

Before runtime implementation, freeze a small affected workload and establish
actual repeated source reads/decodes, measured owner cost and credible removable
work. Do not derive source cost from whole-query time alone. Then compare these
complete workflows on the same immutable inputs:

- ordinary direct execution;
- explicit full preparation plus all declared execution;
- partial realization plus all declared execution, including first-use creation,
  validation and resident publication.

Retain only after complete values/schema, ownership, no-fallback and resource
gates pass and the prospective total improves under the existing gain/loss rule.
Keep query engine clocks consistent and separate from complete preparation and
publication clocks. A small reduction in work is not itself a measured speedup.
Preserve failures and every unchanged/slower control. No Full43/full10k is added.

## Verification plan and open questions

Test overlap, disjoint and full-width demand; different native fragment sizes;
empty/pruned/prefix/filtered sources; nullable fields; alias/order preservation;
changed/deleted/replaced sources; cancellation before/during/after realization;
late source failure; publication failure; fresh-session reopen; optional pressure,
grant narrowing and final-owner release. Verify complete results after source,
plan and session handles drop. Test a retained slice after map eviction.

Prove activation and physical work avoidance below SQL, DataFrame, native Python,
embedded Rust and CLI/worker surfaces where their session lifetimes permit it.
A fresh CLI process is an explicit no-reuse control. Run targeted source tests,
required workspace format/lint/tests, native feature checks and installed-package
acceptance before reporting implementation acceptance.

The frozen exposure and six-call source-owner screen admitted one bounded
candidate. Its retained families are metadata-free integer/float and UTF8
columns with proved backing capacity; unsupported retention keeps the ordinary
direct reader. Nonzero UTF8 offsets that require provider rebasing decline a
retention receipt. All-null native constants retain structural metadata even
without a value buffer. Natural regions, differing fragment boundaries, current
source generations and immutable session semantics remain authoritative.
Concurrent serving sessions decline this first cache; ordinary serialized
sessions use the existing single optional-memory reclaim endpoint.

Local validation, `NativeSession::close()` optional-memory reclamation, isolated installed-wheel
acceptance (`468/468` without skips) and the frozen three-role comparison
(`150,509,625 → 149,794,248` engine ns, `-0.48%`) pass with Chrysalis locally retained;
see the [comparative report](../benchmarks/native-chrysalis-partial-coverage-2026-11-21.md) and
[portable evidence packet](../release/evidence/chrysalis-partial-coverage-2026-11-21/README.md).
Hosted CI acceptance remains pending. Full durable preparation remains an independent
existing transaction in this comparison; it does not consume the resident map. Durable
fragments and Resonance live leases remain separate designs; this investigation does not
mark either delivered.
