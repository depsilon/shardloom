<!-- SPDX-License-Identifier: Apache-2.0 -->

# Ingestion representation and scheduling: October 25 decision

Status: closed without a runtime change at the maintainer's request to advance
the 1.0.0 version train if no worthwhile additional change is established.
This is a source review and current-runtime observation, not a tested rejection
of every possible representation or scheduling improvement.

The starting and final runtime is merged main
`167ac248ba662ae7967a7801995e9b160b17bae5` (#1605). No item 16 engine code was
implemented. The [whole item 15 drop](native-workflow-drop-2026-10-25.md)
remains in force. The version train follows this decision regardless of the
absence of an accepted optimization.

## Current runtime observation

One isolated full ingestion through the standard guarded runner records
**49.303 engine seconds**: 42.115 seconds in native preparation followed by
7.188 seconds in prepared-artifact publication. These spans are sequential in
`sql_local_source_runtime.rs`; nested decode, conversion and compression timers
overlap and are not added to this total. Each original span has millisecond
precision. The prior same-clock observation is 49.781 seconds; the 0.9602%
difference between these single observations is not a causal speedup.

The output has 99,997,497 rows, 112 columns and 15,715,228,660 bytes, with SHA-256
`95a393d153a8c2e1ffeb5084b2656ac61465c6ef9dad435d032365844f1ac057`.
It is byte-identical to the accepted output whose entire 11,199,719,664-value
readback is recorded in the [ownership acceptance](native-ingest-ownership-2026-10-25.md).
This observation uses that exact-content bridge; it does not claim another
complete value-by-value readback. Source identity, publication and process
cleanup pass. Native peak reservation is 6,525,414,393 bytes within 24 GiB, with
no denied reservations. Provider allocations outside native credits retain their
existing scope exclusions.

The same restored-main runtime's single isolated Full43 verifies 43 queries,
381 complete rows against the independent reference and exact schemas at
**47.889862794 engine seconds**. Its executable SHA-256 is
`65b899c70fa2fbe9572f6b490b66092595646e855ebaed14c7c4e25147081ade`.
No subsequent engine change occurred, so the release transition reuses this
observation instead of repeating queries. It is pre-bump source evidence, not a
timing measurement of subsequently built 1.0.0 packages.

The [ingestion receipt](evidence/native-ingest-window-2026-10-25/acceptance.json)
and [compact source/run packet](evidence/native-ingest-window-2026-10-25/manifest.json)
preserve the original inputs, output identity, driver, build and raw report.
The [workflow-drop packet](evidence/native-workflow-drop-2026-10-25/README.md)
preserves the complete final Full43 and exact runtime-restoration evidence.

## Source review and finite scope

`parquet_dictionary_schema_plan` and
`parquet_reader_options_with_optional_schema_hint` already request Arrow
dictionaries for eligible small-source derived fields. Large imports request
UTF-8 views under the existing text-size policy. The shared reader metadata
carries these hints into both serial and parallel readers. Looking only at a
later `new_with_metadata` call would miss that policy.

`DictionaryPreservingStrategy` already preserves economical native dictionary
chunks, casts signed source codes through checked native U32 conversion and
retains each input chunk as a separate dictionary epoch and statistics zone.
The pinned Parquet 59.3.1 byte-array dictionary reader can retain source codes
when a batch remains within a compatible dictionary; mixed pages or epochs can
require rebuilding. These facts do not establish arbitrary Parquet-page reuse.
The [earlier codec decision](native-ingest-text-size-2026-10-18.md) preserves the
rejected broad source-dictionary and compression alternatives. Re-enabling the
same approach is not a new candidate.

The current run uses 3,120 batches of at most 32,768 rows and a ten-slot
conversion window under the existing shared CPU allocation. Source decoding,
conversion and writer work already share the executor. Derived-column work
inside the ordered reader is a possible scheduling lead, but the recorded
overlapping spans do not prove an idle recoverable tail or a complete-ingest
benefit. No new observer, batch-size search, worker reassignment, extra live
writer subtree or pool was added. No unmeasured change was retained.

The maintainer's release direction ends this investigation before a new
candidate/comparison cohort. There is consequently no candidate before/after
speed claim and no changed acceptance threshold. Further work needs a distinct,
source-backed mechanism and its own finite correctness, resource, output-quality
and complete-workflow benefit gates. CG-1 through CG-23 remain independent.
