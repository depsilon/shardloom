<!-- SPDX-License-Identifier: Apache-2.0 -->

Historical design record. The [final decision](native-multiway-join-2026-10-20.md)
does not retain this experiment; production code is restored. The prospective
protocol and observations below remain unchanged as provenance.

# Native multiway join investigation: workload and intake

Status: prerequisite observation, before candidate implementation or timing.
This is conditional row 14, `PERF-10.multiway_joins`, under RFC 0044 and
PERF-02/03/10/12. CG-5/20/21 evidence remains scoped; all CG-1 through CG-23
and the 37 required breadth substeps retain their own acceptance obligations.
The starting revision is `563c91260a526f6b02ab9bbb171dd9bfb8c4c2f0`.

## Contract and comparator

Compare an independently specified three-relation cyclic equijoin with the
existing prepared native binary join tree. A database execution maintainer
should be able to account for every input row, duplicate, null, intermediate,
native state owner and result-order requirement. A fast helper over precomputed
answers is not an acceptable comparison.

The public typed plan currently binds two binary `Join` nodes. Each builds its
right input, probes with its left input, and gathers bounded native output.
`native_relational_join.rs`, `native_relational_batch.rs`,
`native_relational_index.rs` and `local_primitive_prepared_relational.rs` own
these operations. Output chunking limits each batch, but does not eliminate
partial matches rejected by the second join. Existing binary and nested
outer-join tests do not establish this cyclic workload's complete semantics.

Ordinary execution order is T build, S build, then R probe. Required complete
output order is R source ordinal, then matching S ordinal, then matching T
ordinal. Preserve bag multiplicity, exact integer values and all nullable
fields. Equality with a null key is never true. No additional predicates,
outer/semi/anti joins, source effects or arbitrary expression movement belong
to the first candidate.

## Provider and reuse map

The pinned provider is Vortex 0.85.0. Its array take/filter and scan-selection
surfaces remain the native payload mechanisms. Its primitive ordered-search
adapter assumes an already sorted primitive array; rank-mask intersection
does not express the intersection of keys from different relations. A bounded
source search of `vortex-array`, `vortex-compute`, `vortex-scan` and
`vortex-mask` finds no relational multiway join provider. The source inventory
and hashes are retained with the experiment. This is not a claim about every
upstream crate.

Classification: `implement_shardloom_kernel`, only if the prerequisite cost
observation passes. Reuse `Batch`/`Table`, original-width `KeyColumn`
comparators, credited `ReservedVec`, cancellation-aware native ordinal sort,
and `Table::gather`/`take_column`. Do not introduce a second source API, scalar
row execution substrate, external engine, dependency or file format.

The proposed compatible-domain strategy narrows S's c values for R.b against
T's c values for R.a before producing an R/S intermediate. Sorted native row
ordinals may supply the two access paths. Exact comparisons prove membership;
all duplicate ordinals survive. Final gathering must restore the ordinary
R/S/T order and remain bounded. The design and quantitative comparison gates
will be frozen after the ordinary-path observation, before implementation.

Initial semantic admission is three immutable native memory scans, integer
equality keys and two inner equijoins. This permits original-width signed and
unsigned comparison without observing previously skipped floating failures.
Predicated/transformed/file/streaming inputs, floating and noninteger keys,
explicit spill and other join shapes retain ordinary native execution. Refusal
is eligibility for this strategy, not lack of existing binary support.

The existing prepared session is the source-generation owner. Repeated calls
may reuse its immutable source buffers and binding; index construction must be
charged on every call unless a separately specified generation-bound owner is
introduced. This experiment does not authorize a physical index cache.

## Independent finite workload

Every relation has nullable Int64 `x,y` and nonnullable Int64 `id`. IDs are
zero-based source ordinals. R maps x/y to a/b; S maps x/y to b/c; T maps x/y
to a/c. The normal inner join is R.y = S.x. The outer join requires
R.x = T.x AND S.y = T.y. Emit all six R/S fields with names
`ra,rb,rid,sb,sc,sid`, followed by `ta,tc,tid`. A binary control emits only
the first six. No aggregate or limit hides multiplicity or incomplete output.

Integer division is floor division. Formula inputs r/s/t are source ordinals.
Except for the explicitly nullable case, every key is present.

| Cell | R (x,y) | S (x,y) | T (x,y) | Role |
| --- | --- | --- | --- | --- |
| `cyclic_sparse` | 4096: `(r%128,r/128)` | 4096: `(s%32,s/32)` | 128: `(t,(13*t)%128)` | sparse compatible c domains |
| `cyclic_empty` | same as sparse | same as sparse | 128: `(t,128+(13*t)%128)` | no final matches |
| `cyclic_skew` | 1024: `(r%64, 0 if r%4 != 0 else 1+r%7)` | 2048: `(0 if s<1536 else 1+s%7,s%128)` | 64: `(t,(13*t)%128)` | duplicate-heavy hot b domain |
| `cyclic_duplicates` | 512: `((r/2)%32,(r/64)%8)` | 512: `((s/2)%8,(s/16)%32)` | 128: `((t/4)%32,(7*((t/2)%2)+t/4)%32)` | full duplicate products and nulls |
| `cyclic_dense` | 1024: `(r%32,r/32)` | 1024: `(s%32,s/32)` | 1024: `(t%32,t/32)` | nonselective control |
| `cyclic_small` | 16: `(r%4,r/4)` | 16: `(s%4,s/4)` | 16: `(t%4,t/4)` | preparation-dominated control |
| `star_control` | 4096: `(r%64,(r/64)%64)` | 64: `(s,17*s-5)` | 64: `(t,19*t-7)` | ordinary star: inner R.x=S.x, outer R.y=T.x |
| `binary_control` | 8192: `(17*r-3,r)` | 8192: `(s,19*s-5)` | absent | ordinary two-table control |

For `cyclic_duplicates`, replace R.x/R.y by NULL at ordinals divisible by
37/41 respectively, S.x/S.y by NULL at ordinals divisible by 43/47, and
T.x/T.y by NULL at ordinals divisible by 53/59. These rules include ordinal 0.

The external Python oracle independently regenerates the source values and
enumerates the relational equalities in source order. The small fixture is
also checked with a literal three-loop reference. Neither oracle participates
in measured execution. Every result row and dtype must match, including empty
results. The large target observation must establish at least 100,000 actual
inner output rows and a ratio of at least 16 inner rows per final row (or an
empty final result) in at least two target cells before candidate admission.
This is a materialized-work signal, not an exclusive-time attribution claim.

## Observation and safety

Use the actual public `prepare_relational_with_schema` and `collect_jsonl`
paths, a single shared session, one native CPU and a 64-MiB native grant.
Register R/S/T through the ordinary `ResidentMemorySource::from_columns`
boundary. Record preparation, the engine-owned execution clock, result
serialization and teardown separately. Inputs, reference generation and
post-result comparisons are outside those clocks. Repeated calls use the
same prepared generation; fresh workflows reconstruct all preparation.

Test-only observation records each binary stage's build/probe rows, exact key
comparisons, index capacity bytes, delivered intermediate/output rows and
array `nbytes`. Delivered bytes are representation traffic and may include
shared buffers; they are not unique allocation or an RSS claim. Shared-pool
peak and zero credits after teardown are separate measurements. Attributed
runs establish counts only. Candidate timing must use observation-disabled
runs, with symmetric mechanisms and ordinary controls.

Run builds and workloads serially through the current local workload guard,
which excludes competing underscore-named Rust test binaries and checks the
UAT lock. Preserve source/build/binary/fixture/protocol identities, host and
owned-process samples, complete output evidence and all failed attempts.
Use unique local-only output paths and unchanged storage limits. Retire
temporary payloads and obsolete executables after portable results verify.

PulseWeave and Capillary admission remain the common resource boundary. This
finite resident strategy changes avoided work and late gathering, not worker
topology, concurrency or distributed execution. Native Vortex payloads and
certificates remain authoritative; `fallback_attempted=false` and no external
query engine are required throughout. No public support, Full43 speed,
competitive superiority, release or publication claim follows from intake.
