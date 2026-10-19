# CPU investigation intake — October 18, 2026

Status: accepted into the existing roadmap as investigation and capability work. This records the maintainer's six-package brief; it does not authorize a version bump or claim an implementation, benchmark win or completed capability. The [phase plan](phased-execution-plan.md) remains the only execution queue. The [intake map](native-cpu-investigations-2026-10-18.json) records each package's disposition and existing owners.

The nullable-record correction in PR #1574 and the [fresh acceptance](../benchmarks/native-roadmap-fresh-uat-2026-10-18.md) are foundations. They are not work to repeat. The [earlier reconciliation](native-roadmap-reconciliation-2026-10-18.md) remains an immutable 54-step snapshot. This intake adds four conditional steps and extends existing obligations, producing 58 steps under the same 14 owners: 37 required and 21 conditional. The eight prior named investigations plus four new investigations make twelve; this count is distinct from the total number of conditional substeps.

## Disposition and sequence

| Package | Queue disposition | Canonical ownership |
| --- | --- | --- |
| A — compact selections and descriptors | New conditional investigation; preserve existing membership/gather and ownership contracts. | `PERF-10.compact_selections`, with `PERF-07.remaining_arrays` and `PERF-03.providers`. |
| B — reusable codec preparation | New conditional investigation within the current resource owner; cross-operation admission joins the existing cache owner only when justified. | `PERF-03.codec_preparation`, with `PERF-02.physical_cache`. |
| C — reversible structural payload encoding | New conditional investigation of original payload representation; distinct from derived columns and rejected codec profiles. | `PERF-09.structural_payload`, with `PERF-08.duplication` and `PERF-09.lifecycle`. |
| D — summaries and exact predicate exclusion | First-use summaries extend required existing work; reusable exclusion facts are a separate new conditional investigation. | `PERF-10.summaries`, `PERF-10.metadata_consumers`, `PERF-10.predicate_exclusion`. |
| E — partition-major external-memory state | Extend required membership/population capability and recovery; no duplicate spill project. | `PERF-06.membership`, `PERF-06.recovery`, `PERF-06.strategy`. |
| F — changed-generation reuse and safe publication | Refine the existing conditional reuse investigation and required publication/lifecycle work separately. | `PERF-02.changed_source`, `PERF-07.publication`, `PERF-09.lifecycle`. |

Start with a short assessment of A and B against current production callers, then choose the stronger measured cost signal for an implementation experiment. C is a separate original-payload compression experiment. D's first-use and repeat-query components may proceed independently. E and safe publication in F remain required capability work even if a proposed optimization is unprofitable. No package requires all the others to complete first; only its actual ownership, semantics, provider and recovery dependencies apply.

Before a candidate changes runtime behavior, record the production component and callers, observed cost, smallest shared extension, admitted Vortex provider or reason for a ShardLoom implementation, and exact verification selectors. Use the existing [owner verification contracts](native-roadmap-verification-contracts-2026-10-18.md); supplement their starting regressions with tests for the new behavior. The RFC 0044 program and relevant CG gates stay in force, including CG-5/6 for correctness/performance claims and CG-13/14/15/16/17/19/20/21 for the affected representation, resources, reuse and workflow boundaries. Preserve all CG-1 through CG-23 rather than treating these six packages as a replacement roadmap.

All six packages target ordinary CPU-native execution. Reuse the pinned Vortex array/selection/encoding/layout/statistics/scan/writer surfaces through approved ShardLoom boundaries. No GPU, hosted model, new service, query-engine integration or external execution fallback is required. No new dependency or physical format is admitted by this intake alone. Physical-format and publication changes need their provider/version/compatibility design before implementation.

## A — compact selections and execution descriptors

Current foundations include [selection resource ownership](native-selection-resources-2026-10-13.md), retained native arrays and [set consumers](../../shardloom-vortex/src/local_primitives/native_relational_set.rs). A descriptive `SelectionVector` or test-only expansion is not evidence of a production bottleneck. Locate an actual repeated expansion, reconstruction or descriptor traversal/allocation in admitted filter/count/gather consumers before selecting a representation.

Extend the existing native selection/descriptor path. Compact sparse/dense local regions may be useful for membership, while global positions remain checked and sufficiently wide. Local region width must not become a dataset-size limit. Preserve an explicit ordered gather representation for repeated and reordered positions; a membership bitmap cannot represent the sequence `17, 3, 17, 8` by itself.

Carry the representation through multiple real consumers, such as filtered counts and projected gathers. Include construction, conversion, iteration, selection composition, retained ownership and eventual output costs. A compact representation that immediately expands at every consumer does not establish the intended benefit.

Acceptance must cover all/none/empty selections, sparse/dense transitions, sliced bases, large global positions, duplicate/reordered gathers, null validity, cancellation and last-owner release. Compare complete operations and unaffected controls. Retain only the predeclared useful execution or memory improvement without a material throughput regression; a smaller descriptor alone is insufficient.

## B — reusable codec preparation

The [current Zstd contract](native-zstd-workspaces-2026-10-07.md) gives concurrent calls independent contexts and releases temporary workspace before returning retained output. [Compression ownership](native-compression-resources-2026-10-15.md) is also already accepted. A longer retained preparation lifetime is new work and must preserve or explicitly revise those lifecycle guarantees with evidence.

Separate three identities and lifetimes:

- Immutable coding/preparation information may be shared only where provider semantics and exact recipe/table identity permit it.
- Mutable decoder/compressor workspace requires an exclusive active lease, reset rules, failure cleanup and concurrency isolation.
- Data-derived prepared state needs source generation, schema/field, transform/expression and applicable deletion identities. A shared column name is not identity proof.

Begin within one operation or explicitly retained context. Extend across operations only after observing repeated expensive reconstruction; use `PERF-02.physical_cache` for that admission/eviction obligation. No hidden global cache or independent memory budget is allowed. Charge idle retained state to the shared owner and make it reclaimable before it blocks ordinary computation.

Persist portable recipe descriptions and required tables only, never process pointers or opaque mutable decoder memory. New artifacts must decode after the originating process exits. Measure setup plus first use and the complete later-use sequence, including lookup, synchronization, retention, eviction, reset and cleanup. Test changing inputs/recipes, concurrency, allocation denial and corrupt inputs. This is not a renamed rerun of the rejected stronger-Zstd/dictionary profiles.

The [October 19 finite investigation](native-codec-preparation-2026-10-19.md)
found a real repeated dictionary-setup signal and tested one idle prepared
dictionary under the existing owner. It passed focused correctness/resource and
fresh-process portability checks, but failed the frozen complete-workload and
control gates. All experimental code was archived and removed. Further codec
retention needs a materially different mechanism; physical-reader reconstruction
remains a separate investigation.

## C — reversible structural payload encoding

The [text-size inventory](../benchmarks/native-ingest-text-size-2026-10-18.md) attributes 44.63% of its measured artifact to four original text columns. That is an opportunity signal, not proof that a structural encoding will save space. Existing domain/length derived columns and the five rejected text profiles are different mechanisms.

Assess a small deterministic family of exact repeated prefixes/suffixes or templates, variable spans, literal exceptions, reconstruction metadata and original validity. Choose from actual data properties and cost, never a known column name or benchmark query. Keep the logical field type unchanged and prove exact reconstruction before adding specialized consumers.

Start native consumers with exact length under the requested semantics, equality, selection and projection where provider support permits. Do not treat dictionary codes as lexical order. Substrings may cross component boundaries; regex semantics must not be substituted with a general URL parser. Unsupported specialized consumers must continue through the existing exact native path with truthful decode/materialization evidence.

Preserve capitalization, percent encoding, delimiters, Unicode, embedded NULs, nulls and irregular values. Raw JSON payload must retain whitespace, duplicate-key spelling and numeric spelling. Test producer close, move and fresh-process reopen, different downstream operations, then write and reopen again.

Acceptance counts all templates/tables, residuals, offsets, exceptions, validity, reconstruction and finalization. Require useful whole-artifact and complete-workflow results against the retained writer and ordinary consumers. A smaller sample or one faster expression is insufficient when normal decoding becomes disproportionately expensive. Apply `PERF-09.lifecycle` and retain both rejected candidates and controls.

## D — first-use summaries and exact predicate-exclusion reuse

The existing [metadata summaries](../../shardloom-vortex/src/metadata_summary.rs), [predicate proof](../../shardloom-vortex/src/metadata_pruning.rs) and `PERF-10` consumers are the shared starting point. Keep the two components separately implemented and measured.

First-use summaries extend `PERF-10.summaries` and `PERF-10.metadata_consumers`. Find a real case where existing metadata cannot exclude irrelevant regions, then select the least expensive useful conservative summary: for example, longer bounds, an already-computed expression summary or dictionary-domain information. Charge creation, footer/storage and read costs. Do not enlarge every footer or recluster every output. Summaries must describe the actual transformed output and never inherit invalidated properties. Prove false-negative safety and useful pruning on the first query.

`PERF-10.predicate_exclusion` investigates an exact reusable fact: no covered row in an immutable region makes a particular predicate TRUE. It is not a cached count or grouped result. A complete count followed by a grouped sum using the same predicate is a useful cross-operation test.

Bind each fact to trustworthy source/region identity, complete coverage, field types, expression parameters, NULL/collation/timezone semantics, deletion state and applicable access context. Partial masks, sampling, early LIMIT, cancellation and failed evaluation cannot establish whole-region exclusion. Under three-valued logic, absence of TRUE for `P` does not establish the answer to `NOT P`.

Charge fact construction, lookup, retained state, eviction and invalidation to the complete related-query sequence. Test changed parameters/sources/deletions, nulls and unrelated workloads. With no valid fact, use ordinary exact native execution. Benchmark acceptance must not depend on an undisclosed warm-up or mandatory prior query.

## E — partition-major external-memory state

The [current batch-set pressure tests](../../shardloom-vortex/src/local_primitive_relational_batch_set_pressure_tests.rs) distinguish resident membership from source replay spill. Extend `PERF-06` through existing native partitioning, runs, exact comparisons, ordering, disk quotas and cleanup rather than adding a parallel spill subsystem.

Route complete keys and required ordinals, process an admitted partition, finalize its contribution, release its state, then reconcile complete output. First/last deduplication needs correct ordinals; remove-all needs complete multiplicity. Duplicate masks must return to original row positions. Membership and quantified predicates must retain empty-set and NULL behavior. Sampling and future-dependent operations must preserve their actual seeded/weighted/ordering algorithm rather than substituting an easier spill algorithm.

Repartitioning must make progress under skew. If it cannot, use or extend an exact sorted-run strategy; a fixed retry count must not become a dataset-size ceiling. Prove ownership and output reconstruction across empty, duplicate-heavy, all-unique and hot-key inputs.

Acceptance is exact completion when logical operator state exceeds the memory grant, with authorized temporary storage, cancellation, quota exhaustion, failure, corrupt/truncated runs and owned cleanup. Separate fresh restart from actual interrupted-execution resume. Preserve existing refusal when spill is disabled. This is required capability completion, so do not reject it for missing a 2% Full43 speedup; use workload-appropriate performance controls.

## F — changed-generation reuse and safe publication

The existing [append-only refinement](../../shardloom-vortex/src/vortex_ingest.rs), [native generation publisher](../../shardloom-vortex/src/memory_file_generation.rs) and [local destination contract](native-stream-destinations-2026-10-15.md) are foundations with distinct limits. Append-only prefix validation is not generic region reuse; new-target publication is not conditional replacement; existing multi-destination publication is not group-atomic.

`PERF-02.changed_source` remains conditional. Identify immutable files or independently addressable regions using trustworthy content/generation identity plus compatible schema, transforms, recipes, dictionaries and deletion semantics. A path, length and modification time alone do not prove unchanged content. Include all validation in the measured refresh lifecycle.

New regions prepare normally. A deletion change may permit payload reuse while invalidating summaries and exclusion facts. A recipe change may permit source-byte reuse without prepared-output reuse. An arbitrarily rewritten monolithic source uses normal validation/preparation unless trustworthy region identity exists. Copying reused encoded bytes into a new single-file artifact still counts as I/O. Compare the complete refresh with a correct rebuild, including validation and new metadata construction.

`PERF-07.publication` remains required, with `PERF-09.lifecycle` for durable artifacts. Stage and validate outputs, then expose a coherent generation through the actual publication contract. Managed multi-output datasets require one authoritative generation transition, not unrelated renames. Define the participating writers and use a real coordination mechanism appropriate to the local provider. Checking a generation and later renaming unconditionally is not compare-and-swap.

Prove atomic visibility and crash durability separately. Old readers retain their generation; retries must determine whether an ambiguously acknowledged commit succeeded. Test competing writers, old/new readers, cancellation on both sides of the commit point, crashes, retries and garbage collection. Cleanup must preserve external inputs and data retained by other readers. Safe replacement remains required even when a reuse optimization is dropped.

## Acceptance and claim discipline

For each selected mechanism, freeze the actual cost signal, source/build/workload identities, controls and retain/drop criteria before timing. Include preparation, conversion, reconstruction, synchronization, validation, retained state and cleanup in the relevant complete-operation/lifecycle evidence. Query comparisons use the same engine clock; routine Full43 communication remains one execution of each query summed from `engine_execution_seconds`. The existing prospective speed threshold applies when complete-workload speed is the selected target. A resource or artifact-size objective needs its own explicit acceptance and throughput controls; capability completion is evaluated on correctness and the supported envelope.

Retain exact values, schemas, NULL/bag/order semantics, source identity, resource/cleanup evidence and certificates with `fallback_attempted=false` and `external_engine_invoked=false`. Vortex remains native input/output; compatibility formats remain explicit boundaries. Rejected strategies do not become permanent workload exclusions. Keep the version bump held until a separate maintainer decision.
