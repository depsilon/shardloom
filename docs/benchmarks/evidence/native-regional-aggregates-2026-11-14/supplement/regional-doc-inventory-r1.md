# Regional aggregate documentation integration inventory

Read-only inventory for `/Users/dylan/LocalData/shardloom/optimization-investigation-queue-worktree`. No worktree file was edited and no validator was run. The JSON companion contains exact source hashes and structured evidence.

## Disposition to reflect

Regional aggregates are **complete and retained locally**. Packet verification is still underway, and the source-preservation commit has not yet been made. Constant-key grouping is next; broader item-20/optimization coverage remains open. This is a local retain decision, not a broader completion or public performance claim.

Evidence facts supplied for documentation integration:

- 987 cases per role: 637 baseline observations reused, 350 new baseline observations, and 987 candidate observations executed once.
- Fresh engine time: 14.828472105 → 13.836163897 seconds; 429 individual regressions remain recorded.
- Ten two-call lifecycle plans per role: 0.399843288 → 0.221834044 engine seconds.
- All ten strict file-read witnesses, 350 regional coverage checks, and 34 negative controls passed.

## Current-status paragraphs to correct

| File and lines | Outdated status | Needed correction |
|---|---|---|
| `docs/architecture/phased-execution-plan.md:338-352` | Says regional partials are next and both regional and constant-key work remain open. | Record regional local retention with verification/source-preservation caveats; move constant-key grouping to next; leave broader queue open. Keep the per-measure drop unchanged. |
| `docs/architecture/native-metadata-encoded-consumer-investigations-2026-11-13.md:18-19` | Says regional partials are next, followed by constant grouping keys. | Record regional local retention and caveats; say constant grouping keys are next. |
| `docs/architecture/native-metadata-encoded-consumer-investigations-2026-11-13.md:61` | Sequence table calls exact regional partials “next.” | Update the row to reflect the completed local-retain disposition and next constant-key work; keep acceptance decisions distinct. |
| `docs/architecture/native-regional-aggregates-2026-11-14.md:5-8` | Calls the work an active experiment and says it does not claim a retained optimization or benchmark result. | Replace with complete/retained-local status and the provided cohort, lifecycle, read, coverage, and negative-control evidence. Preserve the verification, source-preservation, and broader-scope caveats. |
| `docs/architecture/native-metadata-measure-completion-2026-11-14.md:9-10` | Says regional work is next. | State that regional work is locally retained; constant-key grouping and the broader queue remain open. |
| `docs/architecture/native-metadata-measure-completion-2026-11-14.md:81-89` | Headed “Next boundary” and describes the regional proof as future work. | Reframe as a completed regional handoff while preserving the contract as evidence basis and retaining caveats. |
| `docs/architecture/native-shared-pipeline-investigations-2026-11-10.md:96-98` | Places regional partials immediately after per-measure completion. | Update the current dependency note: regional retained locally, constant-key grouping next; preserve unrelated I/spatial ordering. |

The exact literal source statements and their hashes are in the JSON companion.

## Reviewed documents that remain historically accurate

- `docs/benchmarks/native-metadata-measures-2026-11-14.md:3-54` reports the separate per-measure experiment's dropped result. Its measured facts and disposition remain unchanged.
- `docs/benchmarks/evidence/native-metadata-measures-2026-11-14/README.md:32-35` describes what that per-measure packet does not close; it remains accurate as a statement about that packet.
- `docs/benchmarks/native-optimization-activation-2026-11-14.md:3-24` remains the frozen activation-unit baseline record and does not state regional work is next.

A search of architecture, benchmark, release, and README docs found no additional canonical current-status paragraph explicitly placing regional work next beyond those listed above.

## Local documentation validators

- `scripts/check_release_architecture_tracker.py` — direct check for the phase-plan queue; do not pass `--allow-blocked` to mask the open broader queue.
- `scripts/check_public_status_docs.py` — public-status/claim-boundary regression check; it also runs public claim-language and v1-scope checks.
- `scripts/check_public_claim_language.py` — standalone claim wording check, already composed by the public-status check.
- `scripts/check_use_case_backlinks.py` — only applicable if use-case pages or their backlink ledger are changed; no such edits were identified.

No checks were run as part of this inventory. See `regional-doc-inventory-r1.json` for validator source hashes and invocation details.
