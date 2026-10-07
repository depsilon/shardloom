<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native Resource, Batch and Hardware Campaign Acceptance

Campaign date: October 6, 2026; final checks completed October 7 UTC.
Status: accepted and committed locally, with documentation completed after
engine acceptance. Hosted integration and package publication are separate.
The retained source has no external execution fallback.

## Accepted implementation

| Unit | Local commit | Result |
| --- | --- | --- |
| Provider resource ownership | `ae4e3398e07d58c059780d218c17b7eea7fa31ef` | Reviewed FSST/Zstd native buffers retain shared allocation credits |
| Batch input and incremental results | `3ec56331241f32c11046d575e967e5a874f5226f` | Public `from_batches` and `iter_batches` use the same native plan and lifecycle |
| Hardware-inspired campaign | `99e0a4b304c506a2b24f605eb5156859b356486a` | Two retained implementations; three other prototypes removed |

Contracts: [provider resources](../architecture/native-provider-resources-2026-10-06.md)
and [batch APIs](../architecture/native-bounded-adapters-2026-10-06.md).
The accepted combined source identity before hosted integration is
`7dab7afd05c9009de717896ddef43d9df947ca1e96577b95c8cfdb50bcf0f584`
over 935 runtime source assets. The accepted release executable SHA-256 is
`cf34826654b473eabd1a5f7773a52a5141e3fbcf20dc68b473a3d71d4b19deaf`.
Commit finalization verified that those accepted source bytes were unchanged.

## Five hardware-inspired decisions

The finite campaign adapted hardware-design ideas to the existing native CPU
engine. It added no second execution layer or external engine. Each decision
uses the frozen complete-operation targets and controls, including candidates
that were slower or selected an existing alternative strategy.

| Track | Disposition | Complete-operation evidence |
| --- | --- | --- |
| Reservation transitions | Drop | 0.9645% lower target median sum; below the predeclared 2% gate, despite five of seven paired wins |
| Grouped metadata rejection | Drop | 0.05328% lower target median sum; three of seven paired wins, failing both gates |
| Locality-aware scheduling | Drop | 2.7577% higher target median sum; zero of seven paired wins |
| Ingest lookahead | Retain | 4.9591% lower target median sum in the screen; 6.3334% in held-out confirmation; seven of seven wins in both |
| Pure predicate blocks | Retain | 77.7195% lower target median sum in the screen; 78.3458% in held-out confirmation; seven of seven wins in both |

These are separate workload-cohort comparisons on one host, not percentages
that can be added or applied to the whole engine. Failed prototypes and their
source snapshots remain evidence; their implementations are absent from the
accepted runtime source.

The ingest change recovers at most one absent lookahead slot using exact native
copy allocation bytes under unchanged memory and CPU grants. Existing positive
windows and synchronous P1 behavior remain unchanged. Its complete ingest
screen/confirmation contain 112/196 calls and 16/28 fresh complete reopens.
Target median sums are 1.361681 → 1.294153 seconds and
3.410108 → 3.194131 seconds respectively. The extra native input copy remains
reserved and leased through its lifetime. Observed RSS investigation flags were
reviewed against that finite overlap cost; the raw scores were not changed or
discarded. This is not a total RSS bound.

The expression change evaluates finite pure Int64 column/literal predicate
compositions in Boolean blocks with exact nullable truth tables, shared native
owners and reuse of identical ordered nodes. Arithmetic, casts, functions and
lazy/fallible branches retain their existing evaluators. The screen/confirmation
contain 98/126 complete calls; target median sums are
0.875590 → 0.195085 seconds and 1.385163 → 0.299946 seconds.
Fourteen preflight calls bring expression evidence to 238 complete calls.
Fresh value verification covers 46,137,538 rows. No control or RSS investigation
threshold crossed in these retained expression comparisons.

Instrumentation was kept separate from paired timing. CPU/RSS observations,
operation counts and static instruction inspection do not measure hardware
instruction counts or cache-coherence traffic. `xctrace` was unavailable; no
hardware-counter explanation or FPGA/ASIC/PIM claim is made.

## Current Full43 observation

The final source ran every ClickBench query three times in new native processes
against the retained 15,682,956,489-byte Vortex input. Each measurement includes
process startup, complete result output and exit. It excludes fresh ingest.
The resource policy requested 24 GiB and maximum parallelism 12 on the ten-CPU
macOS arm64 host. OS cache state was not controlled.

| Measurement | Seconds |
| --- | ---: |
| Sum of the 43 best-of-three query times | 63.616547 |
| Sum of the 43 query medians | 64.410554 |
| Sum of all 129 complete calls | 194.006782 |

Maximum observed native process RSS was 5,488,852,992 bytes. This is regression
and current-runtime evidence, not a paired whole-engine speedup or an observed
end-to-end ingest-plus-query duration. Earlier provider and adapter Full43
observations remain separately dated in their packets; comparing those totals
does not establish a controlled gain.

## Final validation

All 13 frozen source-check commands passed: formatting, default workspace
Clippy/tests, feature-gated native tests, CLI targets, examples, native Clippy
with/without writes, lean and Rust 1.96 compatibility checks, Python tests and
the release build. The final native suite records 2,348 passing and 23 ignored
tests; the Python suite records 613 passing tests. Ignored tests are not counted
as executed acceptance.

The same accepted binary passes:

- 27,373 public checks over 15,820,181 rows, retaining 54,733 raw envelopes.
- 202 direct checks over 131,734 rows, retaining 367 raw envelopes.
- 48 batch checks, with 44 raw envelopes, and 19 format checks, with 24 raw
  envelopes. Expected failures remain in those raw records.
- 145 admitted semantic stages, nine golden workflow stages and all 129
  complete Full43 calls with native route and no-fallback evidence.

The campaign packet additionally contains 4,360 raw files and 1,248 raw stdout
envelopes across the five experiment tracks. Finalization also reopened 516
members from the retained compacted prior-attempt archive. Earlier failures,
interrupted/incomplete attempts and superseded verifiers are preserved; they
are not silently promoted to passing runs. The independent stream inspector
passed 159 contract cases before checking the final compressed packet.

The batch/format contracts describe the original stream-frame retention and
independent-reader limits. Public query reports do not expose full-query final
live or denied-reservation counts; source ownership/denial tests and available
native peak reports remain distinct proof.

Documentation acceptance covers the user-surface index, public-status and
backlink checks, Astro checks/build, generated-site readiness and whitespace.
All three new Python examples produce their expected native rows; the final
Field Guide example was repeated after its formatting change. Desktop/mobile
inspection confirms readable pages, bounded code scrolling, resource navigation
and a working `from_batches` search result. The
[documentation receipt](evidence/native-documentation-acceptance-2026-10-06.json)
links final source/page identities, screenshots, logs and the original verifier
failures. Those failures were in verification setup, not the engine: one used
the wrong report attribute, and one omitted the import declared earlier on the
guide page. Corrected executions passed. At documentation acceptance, all 935
runtime source assets matched the accepted engine snapshot.

## Hosted integration corrections

The first run of [PR #1526](https://github.com/depsilon/shardloom/pull/1526)
exposed a test assumption: it expected all eight requested CPU lanes on a
four-CPU runner, where the runtime correctly admitted four. The test now checks
the available CPU cap and includes an over-capacity request on every host.
The original assertion was reproduced locally with an 11-lane request on a
ten-CPU host; all 11 pipeline pressure tests pass after the correction.

The website audit also detected
[GHSA-wq5f-xc86-pv6w](https://github.com/advisories/GHSA-wq5f-xc86-pv6w)
in its locked Sharp dependency. Updating Sharp from 0.35.4 to 0.35.5 and its
matching platform packages clears the audit. Website checks and the build pass,
with identical generated page bytes and no package-version change.

Formatting, default workspace Clippy/tests and native-feature Clippy pass after
these fixes. The source identity is now
`96e8ce44a67615aac0982410065b6d3df076f65725a3bd1aec7a67ea6849529b`:
934 of the 935 inventoried assets are unchanged, and the only changed source
asset is the test module behind `cfg(test)`. Production code and the measured
executable remain unchanged. The complete runtime and Full43 cohorts above
were therefore not repeated for these fixes. The
[integration receipt](evidence/native-engine-integration-fixes-2026-10-07.json)
retains both hosted failures, the local failing/passing evidence, complete
source hashes, patch and check logs.

All 39 hosted checks passed on `8873e261588ff29001253f4e73afb977e44c30cc`.
PR #1526 merged at `281056ca95daa86cc1376d4eb2c4d0b8714ae8ae`, whose tree
equals the tested head. Primary adversarial review passed; the hosted review
bot was account-limited and supplied no approval. No submitted reviews or
unresolved review threads were present. Production deployment succeeded, and
ordinary-browser checks verified the complete batch page text and linked
resource contract match the accepted preview. The
[hosted receipt](evidence/native-engine-hosted-2026-10-07.json) retains the
source/check snapshots and historical page observations. This completes the
finite packet's integration, without a new package release or broader roadmap
completion.

## Separate COUNT-reuse investigation

Both composed COUNT-reuse prototypes were dropped. All 2,700 query results
across five complete paired cohorts matched the frozen expected values, and
the eligible targets improved. The renamed grouped COUNT control at 1,048,576
rows nevertheless crossed the frozen regression gate in four cohorts. Its
candidate/baseline median ratios were 1.210270 and 1.227486 for the first
prototype's confirmation/repeat, then 1.254656 and 1.271089 for the second
prototype's two orderings. The first cohort's lack of a crossing is preserved.
Eligible gains do not override a failed unrelated-control gate.

The [decision index](evidence/native-composed-count-decisions-2026-10-06.json)
and [complete evidence archive](evidence/native-composed-count-decisions-2026-10-06.tar.xz)
retain five summaries, all raw query/preparation outputs, input fixtures,
frozen protocols, both source overlays, build records and diagnostic
counterevidence. All 927 baseline source assets were independently matched to
accepted commit `ae4e3398`, resolving the precommit build's dirty-parent label.
The archive has 3,322 source-path entries mapped to 1,418 unique payloads; every
member was reopened and hash-checked without extraction. Its compressed
SHA-256 is `f8236f3cdd59dc3f669b99b22434dbf545c3fe897f2ec97919da73e422f2e49d`.

Profiling did not establish the regression's source-level cause. A separate
whitespace-only alias admission gap was recorded without a runtime claim.
Candidate-2 DataFrame, full workspace, public and Full43 acceptance was not run
after retention failed. The archive records those omissions and identifies the
measured executables by hash; executable contents are not included. This closed
investigation is separate from the five-track hardware campaign.

## Durable evidence and reproduction

The [acceptance index](evidence/native-engine-acceptance-2026-10-06.json)
records each original acceptance, retention, local commit, source/binary hash,
packet hash, claim boundary and independent inspection. These exact compressed
packet copies retain frozen protocols, drivers, source identities, raw results,
timing/resource samples and failed attempts:

| Packet | Compressed bytes | SHA-256 |
| --- | ---: | --- |
| [Provider](evidence/native-provider-resources-2026-10-06.json.xz) | 43,390,448 | `9fc20b73f552a29d588062134e7a4db0aa63c8226b229abab4e16090d1aa36e6` |
| [Batch adapters](evidence/native-bounded-adapters-2026-10-06.json.xz) | 43,227,004 | `65efc2b5b080f3b27e2acd3fb62e120c32d9e341306d6cdaf546c0f617d4bb91` |
| [Combined hardware campaign](evidence/native-hardware-campaign-2026-10-06.json.xz) | 45,001,948 | `509787e4848c569cbec948de16e8bca678f6d1723c5a2a9cd4616f53410e0ec5` |

Separate independent inspection receipts are linked from the index. They verify
compressed/uncompressed identities, complete counts and the declared structural
contracts; archive inspection does not rerun the engine. Portable receipt paths
use `<repo>`, `<local-evidence>` and `<home>`. Original-file hashes still describe
the original bytes, not path-normalized copies.

Use the recorded source commits, feature sets, toolchain and frozen protocol
drivers when reproducing a cohort. Preserve paired ordering, targets, controls,
resource limits, complete output validation and host-load checks. Apply the
[local storage policy](../architecture/local-development-storage.md) and outer
timeout/process-cleanup guards before replay. Decompressed packets are several
GiB; inspect them as streams or in unsynced storage instead of loading them into
a document viewer. No replay should silently reclassify an old failed attempt.

## Remaining scope

All five decisions above are closed. The separate composed COUNT-reuse
investigation also dropped both prototypes after confirmed grouped-control
regressions; it is not an undecided sixth hardware track. Paused format
comparisons and large CSV/JSON/JSONL performance experiments remain outside this
campaign and would require further retain/drop testing if resumed.

Complete reader/codec scratch accounting, general operator spill/recovery,
other-platform runtime parity and broader SQL/format coverage retain their
existing owners. None of these finite acceptances closes whole PERF owners,
CG-1 through CG-23, stable production certification or public release gates.
