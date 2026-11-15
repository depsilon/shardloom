# FSST queue and status document inventory

**Read-only source scope:** `/Users/dylan/LocalData/shardloom/optimization-investigation-queue-worktree/docs/architecture` (exactly five listed files). **Output:** `/Users/dylan/LocalData/shardloom/native-fsst-consumer-2026-11-15`.

## Complete file identities

| File | Bytes | SHA-256 |
|---|---:|---|
| `native-fsst-consumer-2026-11-15.md` | 11439 | `3eac59dda69dfa9cb13032bd7ab3538e8fe05c8a3d74a7a7623d7a3b89651f2b` |
| `native-metadata-encoded-consumer-investigations-2026-11-13.md` | 22031 | `a8a5e98cdf86acf31bd9571d4cb3a54c9535776ce56eee569d5634e8fc42fae2` |
| `phased-execution-plan.md` | 258943 | `3b739a295d5c11dbbb56983c8ce02c5aa394665ad5ab46aa9cc7a311db492844` |
| `native-shared-pipeline-investigations-2026-11-10.md` | 20407 | `8928ea076e94f27bb25c0cffca4506a20d56308e33dab9be0bc01895740a2939` |
| `native-structural-work-investigations-2026-11-11.md` | 19174 | `cd060345569e9361dffcee0c851e7366f535b175ceaa008717fa068b1c402517` |

## Passages requiring status reconciliation

### Status Passage Needing Reconciliation After Retained Prefilter Result

**Location:** `native-fsst-consumer-2026-11-15.md:5-7`

Labels the work active feasibility, says no performance result or retained candidate is admitted, which conflicts with the assigned new state (prefilter comparison complete and retained).

### Sequence Passage Needing Reconciliation

**Location:** `native-fsst-consumer-2026-11-15.md:162-170`

Frames freezing/comparing the prefilter as conditional future work if admission is feasible; this is stale for a completed retained prefilter result.

### Ordered Queue Passage Needing Reconciliation

**Location:** `native-metadata-encoded-consumer-investigations-2026-11-13.md:88-90`

Lists FSST candidate prefilter as next active work before separate matcher-lifetime work; the prefilter portion is completed under the assigned state, while matcher lifetime stays a separate open experiment.

### Phase Checklist Passage Needing Reconciliation

**Location:** `phased-execution-plan.md:411-413`

One unchecked item combines conservative FSST candidate prefilter and bounded exact-table matcher reuse. The assigned state says retain the prefilter result while leaving exact-table matcher lifetime open; passage currently does not represent that split.

### Still-Open Exact-Table Matcher Lifetime

**Location:** `native-fsst-consumer-2026-11-15.md:172-177; native-metadata-encoded-consumer-investigations-2026-11-13.md:88-90; phased-execution-plan.md:407-413`

Request-scoped matcher lifetime remains a distinct later experiment with exact identity, charged allocations/checks, invalidation and its own complete decision; not completed by the retained prefilter result.

### Still-Open Required Spatial Execution

**Location:** `native-fsst-consumer-2026-11-15.md:179-181; native-metadata-encoded-consumer-investigations-2026-11-13.md:89; phased-execution-plan.md:414-417; native-shared-pipeline-investigations-2026-11-10.md:51-52; native-structural-work-investigations-2026-11-11.md:10-11`

Required item-19 spatial execution remains open and has explicit independence from FSST success. Optional persisted spatial summaries follow exact spatial semantics/execution.

### Broader Open Queue Obligations To Preserve

**Location:** `native-metadata-encoded-consumer-investigations-2026-11-13.md:36-40, 89-105; phased-execution-plan.md:374-425, 427-453; native-shared-pipeline-investigations-2026-11-10.md:89-119, 277-278`

Broader I attribution/narrower batching, predicate-summary parent, wider constant-key support, codec/consumer choices, bounded CPU fusion, item-20 owner distinctions, recommendation-13 join-build reuse, D source pruning, deep traversal, and CG-1 through CG-23 remain separate/open as shown. No inference that prefilter closes these obligations.

### Unknown Linked Evidence

**Location:** `all five documents`

Linked survivor report, FSST screen/benchmark evidence, RFC 0044, item-19 detailed spatial contract, and other referenced evidence contents are outside this read-only five-file scope. Their actual content and link validity are unknown here.

## Exact source excerpts

Each excerpt is a byte-exact UTF-8 slice of the source file, recorded in JSON with inclusive line numbers, byte offsets, byte count, and SHA-256. The generator verified the slice bytes and line boundaries against each original file.

### native-fsst-consumer-2026-11-15.md:5–12 — current status and linked authorities

Excerpt SHA-256: `2455bcb978d96444ba95c5796beb5c70e9b741e360815482af9f6f8370d0142a`; 600 bytes.

```text
    5 | Status: active feasibility work under item 20/RFC 0044 and the
    6 | [ordered metadata/consumer queue](native-metadata-encoded-consumer-investigations-2026-11-13.md).
    7 | No performance result or retained production candidate is admitted yet.
    8 | The accepted source at intake is `873e60bca36f5030cec27c3baf59017ca49ad8ab`.
    9 | The [survivor screen](native-survivor-admission-2026-11-15.md) leaves existing
   10 | batch boundaries in force. The prior [FSST writer drop](fsst-consumer-screen-2026-09-26.md)
   11 | and the later [decoder validation correction](../benchmarks/native-fsst-admission-2026-10-07.md)
   12 | remain authoritative.
```

### native-fsst-consumer-2026-11-15.md:14–43 — provider boundary and initial classification

Excerpt SHA-256: `2a622a9cea60247b5bb6ea93dc38d36c4a2f5ab9b00412c81e828d974192901c`; 1887 bytes.

```text
   14 | ## Provider check and current boundary
   15 | 
   16 | The pinned Apache-2.0 Vortex 0.93.0 provider already has FSST arrays, symbol
   17 | tables, compressed codes, a native LIKE kernel and a symbol-aware exact matcher.
   18 | The matcher is private to `vortex-fsst` and is reconstructed by each LIKE call.
   19 | Literal contains and prefix patterns have finite byte-length limits; nullable
   20 | results and pattern escapes remain part of its contract. This work must reuse
   21 | or explicitly wrap that provider before considering another matcher.
   22 | 
   23 | The shared relational scalar evaluator currently prepares text arguments as
   24 | native UTF-8 views. Its value owner validates every valid value in the requested
   25 | domain. A separate legacy contains helper can call the compressed LIKE kernel.
   26 | Static call sites do not prove dispatch through a particular public request.
   27 | Vortex input and execution stay native; decoded native text is an explicit
   28 | existing route, not external query-engine fallback.
   29 | 
   30 | The native decoder admits integer metadata and checks every selected encoded
   31 | row's declared length, escape boundaries and populated symbols before payload
   32 | allocation. Generic text preparation then validates UTF-8. The raw LIKE kernel
   33 | does not invoke those decoder checks. Its temporary tables and Boolean output
   34 | are not proven to be entirely accounted by the existing two-row, zero-payload-
   35 | credit test. A new shared connection must not bypass validation or infer full
   36 | allocation enforcement from that narrow observation.
   37 | 
   38 | The initial classification is `blocked_until_vortex_or_shardloom_evidence` for
   39 | a new prefilter/reuse admission, while existing scoped provider calls keep
   40 | their present classification. No unsupported path is delegated to another
   41 | engine; `fallback_attempted=false` and `external_engine_invoked=false` remain
   42 | requirements. The source-based concerns below are hypotheses until the
   43 | executable screen records their outcomes.
```

### native-fsst-consumer-2026-11-15.md:45–83 — dispatch evidence and observer result

Excerpt SHA-256: `ae90a21753c0db7a8379e8dc1abd81e9c9540742703ce5aedf02df2ea65b6c2b`; 2640 bytes.

```text
   45 | ### Admission observations and source-pushdown correction
   46 | 
   47 | The finite diagnostic now passes 864 physical/shared declarations and 378
   48 | complete native-core requests over all 54 existing inputs. Independent replay
   49 | checks every output against the 288 frozen nullable Boolean references, exact
   50 | schemas, complete declaration keys and every reported owner-close refund. The
   51 | original failed harness and replay attempts are preserved. This is correctness
   52 | and route evidence without engine timing.
   53 | 
   54 | The source caller map corrects the scalar-only view above: `contains` filter
   55 | expressions lower to `PredicateExpr::StringContains` and an escaped Vortex LIKE
   56 | scan filter. Contains count also has a native scalar-count route. In contrast,
   57 | the selected prefix and projection requests prepare shared text. The diagnostic
   58 | observes 3,536 direct FSST LIKE calls, but these are explicit kernel witnesses,
   59 | not proof of invocation inside a public request. Zero legacy helper calls do
   60 | not mean zero upstream FSST calls. Raw malformed input mismatches rule out an
   61 | unvalidated projection bridge; they do not by themselves reject an optimization
   62 | inside the already-admitted source LIKE path.
   63 | 
   64 | Before selecting a candidate, add a debug-only, explicitly registered Vortex
   65 | session observer at the successful FSST LIKE return. It counts successful calls
   66 | and logical operand rows, with overflow detection; it is not a decode metric.
   67 | Use one fresh prepared session per request, no global mutable state, and read
   68 | counts only after the request drains. Its small harness-owned allocation is
   69 | outside the production resource claim and it compiles out of release builds.
   70 | The finite dispatch check selects both existing sizes, the selective profile,
   71 | and flat/FSST representations: seven complete consumers per input, 28 total.
   72 | Contains filter/count/scan requests are positive route candidates; projection,
   73 | prefix and flat inputs are controls. Reuse frozen values, schemas and owner
   74 | refund assertions. Do not rerun the full 864-declaration diagnostic for this
   75 | observer and do not record timing or choose a performance candidate from it.
   76 | 
   77 | The dispatch check passes all 28 complete requests. The six FSST contains
   78 | filter/count/scan requests invoke the upstream compressed kernel: 17 calls for
   79 | each filter/scan request and nine for each count request at both sizes. Each
   80 | route covers its 67 or 8,195 logical operand rows. All 22 flat, projection and
   81 | prefix controls record zero calls. Independent replay verifies complete results,
   82 | schemas, certificate identities and refunds; source and input identities are
   83 | preserved. These are logical call counts, without a physical decode claim.
```

### native-fsst-consumer-2026-11-15.md:85–131 — selected prefilter and admission gates

Excerpt SHA-256: `a6488db2431735f1ebcaddc248c36631e6f1039986adc72bed97b0883f34f2f4`; 3227 bytes.

```text
   85 | ## Selected prefilter experiment, before timing
   86 | 
   87 | Implement one conservative necessary-byte filter inside the existing Vortex
   88 | FSST LIKE provider. This is `use_vortex_native_provider` at the existing native
   89 | scan boundary, with a ShardLoom-authored local provider modification. It does not
   90 | admit a new scalar projection route, writer policy or persisted metadata format.
   91 | 
   92 | For positive, nonempty, supported literal contains patterns, inspect the actual
   93 | symbol table once. Choose the first pattern byte represented by the fewest
   94 | populated symbol codes, and admit the prefilter only when that number is at
   95 | most two. The two-code limit comes from the existing three-byte search primitive:
   96 | the escape code is always the third possible candidate. Every escape retains
   97 | exact verification, irrespective of its following byte. Raw byte search may
   98 | mistake escaped literals for symbol codes; that only adds false positives.
   99 | Symbol boundaries and Unicode cannot suppress a true match because every exact
  100 | match must contain the selected literal byte in a populated symbol or escape.
  101 | 
  102 | Use the already-locked `memchr` 2.8.3 provider (MIT option under
  103 | `Unlicense OR MIT`) for one-, two- or three-byte search. This adds a direct
  104 | dependency edge to the vendored FSST crate, not a new resolved package. Preserve
  105 | original registry provenance and update the local modification notice. No
  106 | external implementation code, new unsafe block or execution fallback is added.
  107 | 
  108 | The filter table uses bounded stack storage; retained state is at most two code
  109 | bytes and its discriminator. If the complete compressed backing has no candidate,
  110 | skip exact matcher construction. Otherwise preserve its construction and reject
  111 | individual rows only when the same necessary-byte test is negative. Execute
  112 | offset metadata and visit every row's original slice even for a complete backing
  113 | rejection, preserving bounds/error ordering. Shared text validation is untouched.
  114 | Prefix, empty, unsupported, nonconstant, case-insensitive and negated requests
  115 | retain the existing path. Dense candidates pay the filter and exact-match cost;
  116 | no data-trained thresholds, samples or speculative reuse are introduced.
  117 | 
  118 | Before measurement, require exhaustive small code/escape counterexamples against
  119 | the original exact matcher, every frozen existing-file result, nullable and
  120 | selected-slice behavior, malformed-value parity, resource denial/cancellation,
  121 | and actual candidate/construction counts. The existing explicit debug-only
  122 | session observer records these counts in the untimed admission screen; it adds
  123 | no production counter allocations or per-row counter work. Actual performance
  124 | requests retain their ordinary certificates, and must not be represented as
  125 | having measured per-request prefilter counters. Any later production counters
  126 | would require the exclusive-operation observer/credit contract. Existing
  127 | provider scratch exclusions remain explicit and
  128 | are not upgraded to a full memory-accounting claim. Freeze the complete public
  129 | comparison only after final validation and exact runtime/input/reference binding.
  130 | Reuse prior observations only with full identity equality; do not silently repeat
  131 | successful baseline requests. All regressions and failures remain in the decision.
```

### native-fsst-consumer-2026-11-15.md:133–181 — finite screen, subsequent experiments, open queue

Excerpt SHA-256: `4bea93313ff67d2234b00d3d9ec6a87aeb16f7bfc3a05547560263373635ad11`; 3004 bytes.

```text
  133 | ## Finite feasibility screen
  134 | 
  135 | First reuse the existing 18 FSST inputs from the necessary text-signature
  136 | experiment, selecting its ordinary layout without the declined summary metadata.
  137 | Their nine logical profiles and two sizes have complete independent readbacks
  138 | and source hashes. Flat and dictionary fixtures provide representation controls.
  139 | New fixtures are reserved for missing malformed, pattern-boundary and resource
  140 | cases; no repeated writer experiment or full10k/Full43 observation is requested.
  141 | 
  142 | Before selecting a performance candidate:
  143 | 
  144 | 1. Check complete results through the actual shared and source-predicate routes,
  145 |    identify the physical FSST inputs seen by each consumer, and distinguish
  146 |    direct kernel witnesses from public dispatch. Preserve nulls, Unicode,
  147 |    escaped literals, empty/over-limit patterns, selected slices and nonpredicate
  148 |    consumers. Record actual decode/preparation work where observed.
  149 | 2. Compare shared text behavior and direct compressed matching on incorrect
  150 |    row lengths, truncated escapes, unpopulated symbols and invalid UTF-8.
  151 |    Preserve the first required error and selected-domain validation. Successful
  152 |    Boolean output on malformed data does not admit a bypass.
  153 | 3. Bound matcher construction, metadata execution, output ownership and scratch
  154 |    overlap. Exercise denial, cancellation, empty/null input and retained output.
  155 |    A count of payload bytes avoided is not complete resource accounting.
  156 | 
  157 | Keep each original attempt, exact source, input identity and complete output.
  158 | This screen records no engine timing and cannot establish a performance win.
  159 | The expert comparator is a native encoded-execution maintainer checking errors,
  160 | allocation lifetime and actual dispatch before interpreting reduced work.
  161 | 
  162 | ## Subsequent independent experiments
  163 | 
  164 | If admission is feasible, freeze a conservative candidate prefilter followed by
  165 | the existing exact symbol-aware matcher. Compare it with direct compressed
  166 | matching and the admitted native decoded route. False negatives are forbidden;
  167 | false positives retain exact verification. Candidate selection must use actual
  168 | pattern/table/data characteristics and include construction, validation, scratch,
  169 | dense-candidate and nonpredicate costs. An empty or unsupported pattern retains
  170 | an explicitly admitted route rather than silently changing its semantics.
  171 | 
  172 | Only after that decision, separately evaluate request-scoped matcher lifetime
  173 | across existing slices. Exact symbol contents, lengths, pattern and options must
  174 | match, all retained allocations and identity checks must be charged, and changed
  175 | tables or requests must invalidate reuse. No global cache or writer default is
  176 | authorized. Each candidate gets its own complete comparison and retain/drop
  177 | decision; neither can inherit a kernel-only timing claim.
  178 | 
  179 | The parent queue, broader I, required item-19 spatial execution, item 20,
  180 | RFC 0044 and CG-1 through CG-23 remain open. Missing FSST admission must not
  181 | hold required spatial execution indefinitely.
```

### native-metadata-encoded-consumer-investigations-2026-11-13.md:34–46 — recent intake status including survivor screen

Excerpt SHA-256: `cb53a3a05eac40db7ef8b8a4f5978436f4a362c68259bb5742a7059a4079b5e9`; 856 bytes.

```text
   34 | [necessary text-signature experiment](native-text-signatures-2026-11-14.md) has
   35 | a complete drop disposition; accepted source is restored. The
   36 | [survivor admission screen](native-survivor-admission-2026-11-15.md) declines
   37 | unconditional callback batching before performance; the existing batch contract
   38 | remains in force for the next FSST work. Broader I attribution/narrower batching,
   39 | the predicate-summary parent,
   40 | broader constant-key support and coverage remain open.
   41 | 
   42 | The supplied research describes source checked at `72680743a34a5db9`, after
   43 | PR #1683. It is an intake, not fresh validation of later implementation. Recheck
   44 | the current source and pinned provider before selecting a design. Its historical
   45 | timings and earlier experiment results retain their original source attribution;
   46 | this queue update runs no benchmark and admits no performance claim.
```

### native-metadata-encoded-consumer-investigations-2026-11-13.md:48–69 — scope and no-fallback obligations

Excerpt SHA-256: `2a727812b3d07b780505293ea6065b0bac2422d327d5014607863a3595ba8150`; 1346 bytes.

```text
   48 | ## Decision and scope
   49 | 
   50 | Treat these as major investigations that can produce shared engine alterations,
   51 | provider integration, execution tests and complete public-operation evidence.
   52 | Finish each with an explicit implemented/retained or evidenced decline/drop
   53 | disposition. A literature review, declaration count, planning report or new
   54 | counter does not itself complete the work.
   55 | 
   56 | Use existing item-20/RFC-0044 source, aggregate, expression, key, selection,
   57 | memory and scheduling owners. Extend the existing
   58 | [structural investigations](native-structural-work-investigations-2026-11-11.md)
   59 | N for dictionary expressions and I for survivor batching; attach spatial work
   60 | to [item 19](native-rich-data-lifecycle-2026-10-24.md). Do not create duplicate
   61 | phase IDs or parallel copies of those investigations. This intake does not
   62 | automatically revive a previously dropped mechanism unchanged.
   63 | 
   64 | CLI SQL, embedded workflow, Python SQL, DataFrame and typed-plan interfaces must
   65 | converge on the same admitted engine implementation. Vortex remains native input, execution
   66 | representation and output. Other engines may supply independent correctness
   67 | references only. No Spark, DataFusion, DuckDB, Polars, Velox or Vortex
   68 | query-engine integration may execute residual work. Dependency and copied-code
   69 | provenance remain Apache-2.0-compatible and explicitly reviewed.
```

### native-metadata-encoded-consumer-investigations-2026-11-13.md:71–99 — ordered execution sequence and independent decisions

Excerpt SHA-256: `cd8f3f88f1872fcce5bbb79bd21dd54d075e64aece410ab5dd89444825e518de`; 4286 bytes.

```text
   71 | ## Sequence and deliverables
   72 | 
   73 | The maintainer's subsequent November 13 efficiency request orders this work by
   74 | dependencies and shared validation. The activation unit now has a valid accepted
   75 | baseline. Reconcile its new facts/counters with the existing
   76 | 10k inventory, adding separately versioned missing cases for mixed aggregates,
   77 | real dictionaries, actual FSST inputs and encoded-float semantics. This is a
   78 | delta review of existing evidence, not another whole-engine inventory phase.
   79 | 
   80 | | Execution order | Work and existing owner | Why it belongs here |
   81 | | --- | --- | --- |
   82 | | Completed finite experiments | Per-measure candidate dropped; exact scalar regional partials retained locally; constant-key candidate dropped | All use the existing aggregate state/finalizer. Regional coverage and the rejected grouped candidate's semantic/read proof remain useful evidence, but only the scalar regional implementation is retained. Wider constant-key support remains open. Keep each acceptance decision separate. |
   83 | | Completed finite experiment | Dictionary-domain text transforms and consumers, extending N, retained locally | Actual dictionary retention, exact consumer equality, 210 work witnesses, both complete engine totals and portable proof pass. The added dictionary/control subgroup's small engine increase and all other regressions remain explicit. |
   84 | | Completed finite experiment | Named exact source/domain facts and ALP/FastLanes predicate admission, extending C, retained locally | All 2,786 declarations per runtime, 620 finite-source admissions, 150 actual compressed-dispatch witnesses, separate lifecycles and portable proof pass. Both complete engine totals decrease; all regressions remain explicit. No general fact framework is introduced. |
   85 | | Completed finite admission review | Existing unversioned native Bloom provider declined before performance | Pinned persistence warning, four native signed-zero counterexamples, registration/resource review and portable proof are preserved. This is not a timing drop or a rejection of every Bloom design; the summary parent remains open. |
   86 | | Completed finite experiment | Necessary nonspatial text signatures declined; accepted source restored | All 9,040 declarations per runtime, lifecycles and 840 logical exclusions pass, with separate read/construction evidence. Complete fresh engine time increases despite the favorable new-text and lifecycle results. Preserve the complete drop; the summary parent remains open. |
   87 | | Completed finite admission screen | Unconditional survivor callback adapter declined, extending I | Eight complete requests and preparation/ownership counterexamples settle the prerequisite: existing batches remain in force. Error and callback order prevent unconditional buffering despite fewer preparations in the small control. Broader attribution and a separately admitted narrow route remain open. |
   88 | | Next active work | FSST candidate prefilter, then a separate exact-table matcher-lifetime experiment | Use the accepted encoded consumer and batch contract; retain construction, dense-candidate and nonpredicate controls. Neither result authorizes a writer default. |
   89 | | Next | Required item-19 coordinate/offset spatial execution, then optional persisted spatial summaries | Exact geometry/type/invalid-value semantics must precede a summary that can suppress work. Functional support does not depend on successful text, codec or fusion experiments. |
   90 | | Last | Consumer-specific representation/codec choices and any new bounded CPU fusion | Attribute the work remaining after accepted upstream reductions. Reuse the established consumer contract; keep the prior layout/fusion drops intact. |
   91 | 
   92 | The descriptive sections below retain each original contract; the table and
   93 | phase plan own execution order. Within a work unit, reuse fixture generation,
   94 | source bindings, independent references and narrow regression checks where their
   95 | identities match. Run required broad/native/package gates at the coherent source
   96 | boundary. Freeze and decide each distinct performance experiment independently;
   97 | do not add their savings or silently repeat measured declarations. An unavailable
   98 | proof requires an explicit bounded decline or missing-evidence disposition and
   99 | does not hold unrelated ready work indefinitely.
```

### native-metadata-encoded-consumer-investigations-2026-11-13.md:101–105 — owner separation and cross-obligation distinction

Excerpt SHA-256: `d5965b3e7545a746993e0f2fb9a0d7a4db35afd1144775c2f173450db064c328`; 382 bytes.

```text
  101 | Reconcile overlaps once: B remains the corrected direct COUNT bridge; C owns
  102 | reusable source/domain facts; N owns dictionary transforms; I owns survivor
  103 | batching. Active source-segment reuse is not join-build reuse, and post-key-
  104 | validation membership rejection is not source scan pruning. The older D and
  105 | recommendation-13 obligations therefore retain their distinct missing work.
```

### phased-execution-plan.md:366–425 — current coverage queue through FSST/spatial sequence

Excerpt SHA-256: `2d8dbb802814ba3610962d41c3c1fc7d7d9fdc223012414ef886c2a38adf18bf`; 4450 bytes.

```text
  366 |   The [necessary text-signature experiment](native-text-signatures-2026-11-14.md)
  367 |   has a complete drop disposition: all 9,040 declarations per runtime, 1,080
  368 |   lifecycle operations, 840 logical exclusion witnesses and portable checks
  369 |   pass, but complete fresh engine time rises from 682.167470629 to
  370 |   692.796513379 seconds. The accepted source is restored. The
  371 |   [survivor admission screen](native-survivor-admission-2026-11-15.md) declines
  372 |   unconditional callback batching before performance: eight complete requests,
  373 |   preparation/ownership witnesses, semantic counterexamples and portable replay
  374 |   pass. The existing batch contract remains in force for FSST. Broader I,
  375 |   the predicate-summary parent
  376 |   and broader constant-key support remain open.
  377 |   - [ ] Reconcile only changed activation/exposure against the accepted coverage
  378 |     baseline. Reuse B's corrected COUNT contract, C's exact fact ownership,
  379 |     N's dictionary owner and I's consumer boundaries; attach versioned missing
  380 |     encoding/selectivity controls without restarting the full inventory.
  381 |   - [ ] Complete individually proved aggregate measures from metadata, then
  382 |     exact regional partials and constant-group-key completion through the same
  383 |     state/finalizer. Keep each semantic admission distinct; missing region
  384 |     coverage proof must not block a separately complete per-measure result.
  385 |     The November 14 per-measure experiment has a complete drop disposition,
  386 |     and exact scalar regional partials have a complete local retain disposition.
  387 |     The fixed constant-key candidate has a complete drop disposition with
  388 |     semantic and read-avoidance proof. Wider constant-key support remains open;
  389 |     this parent stays open and does not request a repeat of the rejected experiment.
  390 |   - [x] Extend existing N with dictionary-domain text transforms retained into
  391 |     filter/group/distinct consumers and compatible shared outputs. The
  392 |     [finite November 14 unit](native-dictionary-domain-2026-11-14.md) is retained
  393 |     with complete comparison, lifecycle, ownership and portable evidence.
  394 |   - [x] Extend C with only the exact source/domain facts required by the named
  395 |     ALP/FastLanes predicate consumer. Reuse compatible aggregate proofs; preserve
  396 |     typed comparison and failure semantics without a speculative fact framework.
  397 |     The [finite November 14 unit](native-source-domain-facts-2026-11-14.md) is
  398 |     retained with complete comparison, lifecycle, ownership and portable proof.
  399 |   - [ ] Evaluate predicate-specific zone evidence: the existing unversioned
  400 |     native Bloom provider has a complete finite admission decline before performance.
  401 |     The finite necessary text-signature candidate has a complete drop
  402 |     disposition with logical-exclusion, separate read/construction and portable
  403 |     evidence preserved. This parent remains open; spatial summaries follow
  404 |     the exact spatial execution contract below.
  405 |   - [ ] Extend existing I with separately observed pruning/read/consumer units
  406 |     and survivor-driven batching. Settle its retained or declined batch/lifetime
  407 |     contract before tuning per-batch FSST matcher lifetime or codec choices.
  408 |     The finite unconditional-adapter screen is complete and declined with
  409 |     portable evidence; the existing batch contract is retained. Broader
  410 |     attribution and a separately admitted narrow route remain open.
  411 |   - [ ] Investigate a conservative FSST candidate prefilter and separately
  412 |     bounded exact-table matcher reuse on the resulting accepted consumer path,
  413 |     with selective/dense and hardware controls.
  414 |   - [ ] Advance item-19 spatial execution through native coordinate/offset
  415 |     arrays, conservative bounds and exact surviving-candidate kernels, then
  416 |     evaluate optional persisted spatial summaries. Required spatial support does
  417 |     not depend on a successful FSST, codec or CPU-fusion optimization.
  418 |   - [ ] Assess codec/consumer choices with the accepted consumer and batch
  419 |     contracts. Investigate distinct bounded CPU expression fusion last, only
  420 |     after attributing work left by the accepted upstream reductions and preserving
  421 |     the prior dropped candidate.
  422 |   Preserve all prior FSST, text-zoning, fusion and layout observations. Extend
  423 |   the fixed 10k with versioned encoding/metadata/selectivity controls where it
  424 |   lacks exposure. No speculative codec default, duplicate N/I/spatial queue,
  425 |   broad unmeasured rewrite, new fallback engine or competitive claim is admitted.
```

### phased-execution-plan.md:427–453 — broader item-20 order and open obligations

Excerpt SHA-256: `1846984353d694b7ce253db07c8f98197bac2742c05317a702aa7a89c0c2cc34`; 2148 bytes.

```text
  427 | - [ ] **Item 20 shared-pipeline and operator-state investigations — November 10–11 maintainer additions.**
  428 |   The higher-priority coverage and follow-on items above own the immediate
  429 |   sequence. Reconcile their outcomes into B/C/I/N here rather than implementing
  430 |   them twice. A post-validation join membership filter does not by itself close
  431 |   D's source-pruning obligation, and immutable source-segment reuse does not
  432 |   close recommendation 13's join-build reuse obligation. Keep those distinct
  433 |   residual contracts and the original acceptance records.
  434 |   - Scope: `v1_candidate_pending_feasibility`; the
  435 |     [shared execution contract](native-shared-pipeline-investigations-2026-11-10.md)
  436 |     records the initial seven research candidates in existing RFC 0044 owners.
  437 |     The later November 10 maintainer addition below preserves that program and
  438 |     changes the immediate order: complete the
  439 |     [dictionary repair's hosted integration](native-dictionary-execution-repair-2026-11-10.md)
  440 |     following the current-runtime observation (dictionary PR #1677, Variant and
  441 |     UUID hosted checks now pass), investigate recommendations 8 and 9 independently, then evaluate
  442 |     recommendation 10 under its separate resource objective. The November 11
  443 |     [structural-work intake](native-structural-work-investigations-2026-11-11.md)
  444 |     adds H–N. The complete 10,000-declaration exposure audit for H/I/J is recorded;
  445 |     physical activation and removable-work attribution remain prerequisites to
  446 |     their independent experiments. A is closed; B's original invalid baseline
  447 |     remains preserved, while its corrected bounded connection is retained in
  448 |     the November 14 activation unit without an independent COUNT speedup claim.
  449 |     H/I/J retain their structural contracts after the higher-priority sequence, when
  450 |     their exposure gates pass. K/L retain separate semantic gates, M remains
  451 |     topology research and N is dictionary-activation-gated. Select later extensions from
  452 |     demonstrated exposure and value; keep managed-object growth separate. Required useful
  453 |     spatial support and resource-admitted deep traversal remain in item 19.
```

### phased-execution-plan.md:454–463 — item-20 execution status and remaining gates

Excerpt SHA-256: `04707b8f8bb7fae8ba9ceadeea77635315d46798aef3e73a2c401d82b562792b`; 689 bytes.

```text
  454 |   - Execution checklist:
  455 |     - [x] Implement the November 12 requested
  456 |       [single native pipeline](native-pipeline-consolidation-2026-11-12.md)
  457 |       before further performance investigations. The frozen
  458 |       [original 10,000-case observation](../benchmarks/native-pipeline-consolidation-2026-11-12.md)
  459 |       passes every complete result. The November 13 correction admits minute
  460 |       EXTRACT/DATE_TRUNC SQL through the shared compiler with separate tests;
  461 |       it postdates that measured runtime. Hosted PR integration remains separate,
  462 |       and item 19, item 20 and CG-1 through CG-23 remain open.
  463 |     - [x] Complete hosted integration of the dictionary callback repair. PR #1677
```

### native-shared-pipeline-investigations-2026-11-10.md:45–59 — structural and item-19 open status

Excerpt SHA-256: `3fed074d38f8d6767cafe7e274b4cf08c164e047f37e306372db1944dd46a767`; 1016 bytes.

```text
   45 | The November 11
   46 | [structural-work addition](native-structural-work-investigations-2026-11-11.md)
   47 | adds H–N below. Its complete 10,000-declaration exposure audit identifies new
   48 | workload needs and candidate shapes; physical activation and removable-work
   49 | attribution remain open before independent experiments. The existing A/B sequence remains a foundation. Carry later mechanisms
   50 | only where those paths or separate attribution demonstrate a consumer benefit.
   51 | The separate required spatial and resource-admitted deep-traversal obligations
   52 | under item 19 remain open.
   53 | 
   54 | The maintainer's supplied research inspected `0cae77b` and considered UUID PR
   55 | #1675. It is an investigation input, not proof that a candidate is already
   56 | implemented, safe or faster. Reconcile each hypothesis against the accepted
   57 | source at experiment start, including the current Variant expression changes.
   58 | Check pinned Vortex array, selection, source and scan capabilities before
   59 | extending an owner. No external query engine executes ShardLoom work.
```

### native-shared-pipeline-investigations-2026-11-10.md:81–119 — shared owners, ordered program and distinct open work

Excerpt SHA-256: `3bfbf3e40a06b274c9cb50dd770d3fa71aeadded654c672bbb528df02c50ec65`; 2684 bytes.

```text
   81 | ## Intake dispositions and shared owners
   82 | 
   83 | The original A–G and November 11 H–N proposals are accepted for investigation
   84 | and attached to existing runtime obligations. B's bounded connection is now
   85 | retained within the November 14 activation unit; this does not independently
   86 | accept a COUNT speedup or any other lettered hypothesis. The earlier operator-state
   87 | recommendations 8–15 retain their own recorded dispositions.
   88 | 
   89 | The November 13 [phase-plan priority](phased-execution-plan.md#planned) now
   90 | places complete optimization coverage and its explicitly requested runtime
   91 | capabilities first. The subsequent
   92 | [metadata and encoded-consumer intake](native-metadata-encoded-consumer-investigations-2026-11-13.md)
   93 | follows that work, extending the same item-20 owners, existing N/I investigations
   94 | and item-19 spatial obligation. Preserve the original protocols and dispositions;
   95 | the newer priority does not admit the invalid B baseline or a dropped mechanism.
   96 | The later dependency review puts regional partials immediately after per-measure
   97 | metadata completion. The per-measure experiment is dropped and the distinct
   98 | [scalar regional unit](native-regional-aggregates-2026-11-14.md) is now retained
   99 | locally. The subsequent
  100 | [constant-key candidate](native-constant-key-aggregates-2026-11-14.md) has a
  101 | complete drop disposition after the full fresh engine total increases, despite
  102 | semantic, read-reduction and separate lifecycle proof. The subsequent
  103 | [dictionary-domain N unit](native-dictionary-domain-2026-11-14.md) and subsequent
  104 | [source-domain C unit](native-source-domain-facts-2026-11-14.md) are retained
  105 | locally. The [pinned Bloom admission review](native-bloom-admission-2026-11-14.md)
  106 | declines the current unversioned provider before performance. The subsequent
  107 | [necessary text-signature experiment](native-text-signatures-2026-11-14.md) is
  108 | declined with complete evidence and accepted source restored. The finite
  109 | unconditional-callback batching screen is complete and declined before
  110 | performance; the existing batch contract remains for FSST. The
  111 | [survivor admission report](native-survivor-admission-2026-11-15.md) records
  112 | this bounded result. Broader I read/consumer attribution and narrower batching
  113 | remain open. Broader constant-key support remains open. The sequence settles I's
  114 | batch/lifetime contract before FSST/codec tuning and places exact spatial
  115 | execution before optional spatial summaries.
  116 | Reconcile B/C/I/N outcomes from those active units once; the table below is an
  117 | index of hypothesis contracts rather than a second execution sequence. Source
  118 | segment reuse and a post-validation membership filter do not close join-build
  119 | reuse or D's source-pruning obligations.
```

### native-shared-pipeline-investigations-2026-11-10.md:271–278 — independent open gate statement

Excerpt SHA-256: `8619d2a6f8bb8868d8d3c497b3eb856862ba11a12c216a9dc738addeb3b3da63`; 573 bytes.

```text
  271 | The [October 25 workflow rejection](../benchmarks/native-workflow-drop-2026-10-25.md),
  272 | October 28 join-probe reuse outcome and
  273 | [#1657 selection-retention rejection](../benchmarks/native-selection-retention-2026-11-04.md)
  274 | remain dropped. A new mechanism needs a new protocol; do not rehabilitate an old
  275 | candidate by changing its denominator. Verify the supplied external research
  276 | against primary sources before adopting a design, and never transfer another
  277 | system's measured benefit to ShardLoom. Item 19, item 20, RFC 0044 and CG-1 through
  278 | CG-23 remain independently open.
```

### native-structural-work-investigations-2026-11-11.md:5–38 — structural program status and scope

Excerpt SHA-256: `3f69fa1ac106ac665b9fce83a30b6ea1a19b8446fb6548fa82b027fc2ad49a96`; 2328 bytes.

```text
    5 | Status: maintainer-requested investigations, November 11, 2026;
    6 | `v1_candidate_pending_feasibility`, `claim_gate_status=not_claim_grade`.
    7 | This extends the [shared-pipeline program](native-shared-pipeline-investigations-2026-11-10.md)
    8 | inside item 20 and RFC 0044. H–N below are hypothesis labels, not new phase or
    9 | competitive gate IDs. The [phased plan](phased-execution-plan.md#planned) alone
   10 | owns execution order. Item 19's spatial/deep-traversal obligations, the remaining
   11 | analytical capability/performance tracks, and CG-1 through CG-23 stay open.
   12 | 
   13 | ## Intake and evidence boundary
   14 | 
   15 | The supplied research checked accepted source
   16 | `591fdb42f6ffcd5e9003b4a5c130324d7a34a62c`. The source checks below were repeated
   17 | against that revision. They establish execution boundaries, not attributable
   18 | CPU shares, runtime activation or a performance result. Shared index PR #1678
   19 | and resident-window PR #1679 are integrated; their independent complete-cohort
   20 | results remain in their original packets. The original current-runtime 10k
   21 | observation remains invalid and the 517-case repair remains a separate cohort.
   22 | There is no new complete 10k total or additive savings claim.
   23 | 
   24 | The declaration exposure audit for H, I and J is complete below. Next match the
   25 | identified shapes to actual owner activation and removable work, then freeze
   26 | independent experiments. Declaration counts alone do not admit a candidate.
   27 | Rolling DISTINCT's first candidate was dropped and its hosted/retirement closeout
   28 | is complete. The [A join-mapping candidate](native-join-payload-mapping-2026-11-12.md)
   29 | also passes correctness but is dropped under the complete engine-time rule;
   30 | portable replay and all 30 corruption controls pass. PR #1681, all 58 PR and
   31 | 55 exact-main checks, clear review, verified retirement and post-retirement
   32 | replay are complete. The [B COUNT checkpoint](native-direct-count-bridge-2026-11-12.md)
   33 | records an invalid baseline with 60 failed controls, no candidate benchmark
   34 | execution and exact restoration of accepted runtime/test/CI sources. B remains
   35 | pending a versioned harness correction and complete comparison. These foundations
   36 | do not admit H/I/J by themselves. K and L have
   37 | separate semantic gates; M is conditional topology research; N requires actual
   38 | dictionary activation. Do not restart a previously rejected mechanism unchanged.
```

### native-structural-work-investigations-2026-11-11.md:104–145 — survivor batching status and remaining evidence

Excerpt SHA-256: `836c69c208dbc27b3a1ec95cca45c8ee29d0dad14cee6d814312dbefae35472c`; 2765 bytes.

```text
  104 | ## I: batches formed from surviving rows
  105 | 
  106 | The [November 15 admission screen](native-survivor-admission-2026-11-15.md)
  107 | declines an unconditional callback adapter before performance. Eight complete
  108 | requests establish sparse preparation exposure; four compaction controls reduce
  109 | actual preparations only when the consumer sees combined arrays. Projection
  110 | error order and source callback counterexamples prevent generic admission, and
  111 | small visible slices retain whole credited allocations. Existing batches remain
  112 | the accepted contract for FSST. Broader attribution and a separately proved
  113 | narrow source/consumer route remain open; this does not reject every form of
  114 | survivor batching.
  115 | 
  116 | The [transform path](../../shardloom-vortex/src/local_primitive_relational_transform.rs)
  117 | delivers selected arrays during each input callback. Ordinary
  118 | [join consumption](../../shardloom-vortex/src/local_primitives/native_relational_join.rs)
  119 | creates pending pairs per incoming batch and flushes its remainder at that batch's
  120 | end. Existing [replay capture](../../shardloom-vortex/src/local_primitive_relational_replay_capture.rs)
  121 | has a separate retained-fragment compaction boundary. None of these observations
  122 | by itself measures sparse downstream setup cost.
  123 | 
  124 | Separate source admission from consumer batching. At one admitted filter/join
  125 | boundary, retain bounded native selections/owners across input callbacks and
  126 | flush on useful-row capacity, credited backing-byte pressure, finite demand or
  127 | producer completion. Logical consolidation must remove actual downstream
  128 | preparation; an array list that still invokes each consumer separately is not
  129 | the proposed improvement. Count physical compaction and copying when necessary.
  130 | [Data Chunk Compaction in Vectorized Execution](https://people.iiis.tsinghua.edu.cn/~huanchen/publications/data-chunk-compaction-sigmod25.pdf)
  131 | studies this setup-versus-movement tradeoff and logical join compaction. Treat
  132 | its algorithms as research inputs, not measured benefits for this engine.
  133 | 
  134 | Charge pinned backing allocations, metadata, queues and overlap, not only selected
  135 | rows. Preserve row order/multiplicity, floating observation order, callbacks,
  136 | errors and bounded cancellation. Flush when demand can be met rather than pulling
  137 | an unnecessary suffix. Capillary owns hard admission; any PulseWeave feedback
  138 | uses completed work units and has a separate sampling-cost control.
  139 | 
  140 | Begin with selective filter → expression → aggregate and join → join workflows,
  141 | including dense, tiny, wide-payload and low-grant controls. Report actual survivor
  142 | distributions, consumer preparations/invocations, retained/peak backing bytes,
  143 | copies and complete engine time. Keep this distinct from rejected generic range
  144 | selection and parked ingestion topology.
  145 | 
```

## Scope limits

No source edits or external inspection occurred. This inventory does not validate linked reports, detailed spatial contracts, RFC content, or evidence packets outside the five assigned files. It records no proposed replacement wording.
