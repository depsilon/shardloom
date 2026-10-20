<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite original-text structural representation — prospective design

Status: frozen design before candidate implementation or codec measurements.
This is row 12, `PERF-09.structural_payload`, in the
[conditional-first sequence](native-conditional-investigations-2026-10-19.md)
under RFC 0044. Required breadth resumes after that sequence. The
[phase plan](phased-execution-plan.md), all CG-1 through CG-23 and the separate
full-artifact lifecycle gates remain in force. This document admits a bounded
test-only provider screen, not a production encoding or release.

## Observed opportunity and provider decision

The existing [physical inventory](../benchmarks/native-ingest-text-size-2026-10-18.md)
attributes 44.63% of the accepted artifact to four original text fields. The
new descriptive intake reads all 28 original text fields, in schema order, in
the first 32,768 rows of source row groups 0, 113 and 225. All 84 field/region
summaries pass; their complete ordered validity/length/byte hashes are retained.
The intake is descriptive, with no codec or timing comparison. Its result hash
is `bba6620fc1a9e62884c7a849b4298efaffe9e11aa94613daf8b02e6448490add`.

The larger fields contain many repeated eight-byte prefixes and suffixes even
when complete values differ. Several other fields are short, sparse or entirely
empty. These fixed prefixes are not a representative sample of the whole source,
and repeated bytes do not establish storage savings. Field names identify
observations only; neither the candidate nor its admission rule uses them.

Classification: `use_vortex_native_provider`. Pinned `vortex-onpair` 0.85.0
already supplies `onpair_compress`, `OnPair::try_new_with_data`, native Vortex
serialization and native byte-length/equality/filter/slice consumers. It uses
`onpair` 0.2.0's learned adjacent substrings. The existing `vortex.onpair` tree
contains dictionary bytes, dictionary offsets, token codes, per-row code offsets,
decoded byte lengths and validity. Its dictionary includes every single byte,
so unmatched/irregular content remains exact. There is no new ShardLoom wire
format, parsed-URL substitution or raw-JSON normalization.

Both crates declare Apache-2.0 and are already locked through `vortex-file`.
The test adapter adds only a direct exact-version dev-dependency edge. Safe
`rustix` CPU clocks likewise use the already-locked package and a test-only
`time` feature. No provider implementation is copied. All upstream use remains
inside `shardloom-vortex`. The ordinary file session already registers OnPair;
the umbrella's default compressor does not select it without unstable features.
This experiment does not enable that broad feature set.

Use the provider default 12-bit dictionary budget, 0.15 training fraction and
seed 42. All four integer children are narrowed and compressed with the current
edition-filtered, dictionary-disabled numeric BtrBlocks policy. This is the
currently admitted numeric policy, not the upstream experimental OnPair scheme's
additional delta search. Preserve the original dictionary data and validity.
There is one candidate; no parameter sweep or favorable-column promotion.

## Fixed admission and semantics

The candidate reaches the existing text leaf after normal canonicalization and
compaction. A canonical UTF-8 chunk enters OnPair only when every condition holds:

- At least 1,024 nonempty values and at least 1 MiB of valid original payload.
- Mean nonempty value length is at least 32 bytes.
- At least 1,024 values have eight or more bytes, and at least three quarters
  of those values share their eight-byte prefix with another value in the chunk.

The size condition amortizes the dictionary's fixed table footprint; repetition
is an opportunity condition, not a predicted compression ratio. The whole
admission scan, prefix counting, training, child compression and reconstruction
are measured. Otherwise the existing exact Zstd text path is used with its
unchanged level and frame size. This is an internal representation choice,
with no external execution fallback. All 28 text fields remain in every screen
workflow, including ineligible controls.

The finite adapter admits at most 32,768 rows and 64 MiB of logical text per
chunk, checks arithmetic and the provider's signed-32-bit per-value length
before compression, and rejects unsupported input rather than truncating it.
These bounds avoid the low-level trainer's unchecked narrowing of its row
index to U32. They are experiment bounds, not a proposed public dataset ceiling.

Preserve the original UTF-8 dtype, validity, byte spelling and row order.
Fixtures include empty/all-null/nullable values, capitalization, percent escapes,
Unicode, embedded NUL, unusual delimiters, literal exceptions, and JSON with
whitespace, duplicate keys and distinct numeric spellings. No token code is
treated as lexical order. Byte length is not Unicode character length;
character-sensitive operations keep their exact existing native materialization
path. Substring tests include matches crossing learned token boundaries.
Reordering takes may materialize through native VarBinView; filtering and slicing
must preserve their exact selected rows and dictionaries where the provider does.

## Bounded screen workflow

The original resident Parquet has 99,997,497 rows, 105 fields, 226 row groups,
14,779,976,446 bytes and SHA-256
`a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`.
Select all 28 original UTF-8 fields in the three fixed regions above. Each
invocation reopens the source independently and uses the existing explicit
Parquet/Arrow input adapter and owned native conversion. No derived column or
precomputed query answer substitutes for original payload.

Write each field as a one-field native table using the actual current table,
zone, statistics, chunk and publication strategies. A test-only scope captures
the candidate leaf choice before asynchronous dispatch; unrelated runtime and
the non-test build remain unchanged. Current and candidate share one fresh
release executable. The measured complete-column write includes strategy/session
setup, exact owned conversion, admission, encoding, statistics, file writing,
digest/publication, required synchronization and owner release. Independent
inspection and oracle work remain separately identified diagnostic work.

The producer exits and its process group drains. The guard moves every complete
file to a new path after checking its identity. A fresh verifier independently
reopens the source and moved native files, checking every ordered byte and null,
dtype, schema, physical tree, region bounds, statistics and file identity. No
producer array, dictionary cache or process pointer may be needed to read it.
The verifier then writes the read result again with the ordinary native writer;
a second fresh verifier reopens that output and checks the same complete oracle.
Hash/digest checks apply to both writes. The first rewrite is part of lifecycle
diagnostics and has its own throughput control.

Each file has a first complete native read and three repeated complete reads,
with all results verified. Three native equality-count, inequality-count,
contains-count and negated-contains-count calls use fixed source-derived exact
literals or fixed bytes; NULL is excluded from both positive and negative
predicates. Each family has its own independent Arrow/Rust oracle and median.
The same public native executor is used for both arms when reporting query
execution. Full-value validation is independent of the timed query result.
Record native I/O and decode/materialization observations truthfully. These
per-column provider costs are not Full43 engine totals.

Before measurements, verify admission boundaries, unchanged ineligible control
encoding, deterministic output, native length/equality/filter/slice/gather,
producer-close/move/reopen/rewrite behavior, scope isolation across dispatched
tasks and after panic, output-cap refusal and native-credit cleanup. Candidate
activation must match the frozen data-property rule in every field/region.
Failed activation is invalid setup, not zero benefit.

Keep the existing 512-MiB native grant, 64-MiB per-file and 256-MiB live payload
caps, local-only source/storage checks, exclusive UAT lock, workload exclusion,
12-GiB free headroom, 100-GiB workspace ceiling, 192-MiB diagnostic ceiling and
8-MiB per-command output cap. Process groups must drain and owned reservations
return to zero. Arrow source buffers and unreviewed OnPair trainer/decoder
allocations remain outside the native allocator claim. Finite input bounds,
measured process peak memory and native credits are distinct. This early screen
cannot establish production memory safety; a passing candidate still needs a
reviewed shared-owner admission implementation and denial/cleanup proof.

## Frozen comparison and decision

Run six preflight workflows, then two complete cohorts. Each cohort has three
paired repeats of both profiles in all three regions, for 18 workflows per
cohort. Reverse profile order in the second repeat and symmetrically in cohort B.
Every original and rewritten file is independently checked and then retired;
compact raw observations remain. Complete both cohorts and the final full source
hash before any comparative scoring. Preserve all failures and original samples.

Use each field/region/profile median across three repeats, then sum medians.
Advance only if all of the following hold in both complete cohorts:

1. All exactness, activation, schema/statistics, source/build identity, file
   bounds, process cleanup and native ownership checks pass.
2. Summed physical text-column bytes and summed complete original file bytes
   each fall by at least 30%; neither byte total grows in any sampled region.
3. Summed complete original write process CPU is no more than 1.25 times current,
   both in each region and across the cohort. Summed ordinary rewrite process
   CPU has the same limit, including decoding needed by the ordinary writer.
4. For each complete first-read, repeated-read, equality, inequality, contains
   and negated-contains family independently, summed process CPU is no more
   than 1.05 times current, both in each region and across the cohort.
5. No individual field/region consumer or write has an increase exceeding both
   10% and 100 microseconds in both cohorts. Keep every control in the decision.

The storage and throughput bounds preserve the earlier text screen's criteria;
the new individual and two-cohort controls make regressions visible. Failing a
gate rejects this finite representation/profile. It does not prove that every
structural representation is unhelpful. Do not lower thresholds, remove fields
or relabel failed setup as a valid negative performance result.

## Production retention boundary

A passing screen permits a separately frozen complete bounded workflow, including
all source/derived fields and the real ordered writer. It does not permit runtime
retention on sample size or one faster operation. Resolve OnPair ownership and
untrusted-input/corruption admission before public production exposure, preserving
the exact native path and deterministic unsupported diagnostics.

Final retention needs the existing guarded full-size ingest, complete artifact
identity/size, first/repeated Full43 lifecycle comparisons, original consumer and
ingest controls, cancellation/EOF/publication races, required workspace/native
checks, public/installed-wheel and format acceptance. No new superiority claim
or release follows from the screen. If rejected, retain compact source/result
evidence and restore all experimental changes. `fallback_attempted=false` and
`external_engine_invoked=false` remain mandatory throughout.
