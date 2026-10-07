# FSST admission correction and input acceptance

Status: local engine acceptance and independent packet inspection pass for
corrected runtime `b7de216fe421012d3f9b632ad19e95f19b88dbe5`.
The engine correction passes all 39 hosted checks. Refreshed documentation,
native examples and desktop/mobile/search verification pass. Final hosted integration is in progress in
[PR #1530](https://github.com/depsilon/shardloom/pull/1530).
Published v0.4.0 is unchanged.

This supplements the [original input acceptance](native-input-completion-2026-10-07.md).
Its measurements and immutable packet describe `f14460ef`; they remain intact.
The FSST correction changes two of those 947 runtime source assets. All 945
other assets, public case declarations, exact oracles and fixture generators
remain unchanged. The corrected engine receives its own complete acceptance.

## Failure and correction

The [original hosted native Vortex job](https://github.com/depsilon/shardloom/actions/runs/37675738786/job/112978638267)
failed the existing malformed-metadata test when `fsst-rs` 0.6.0 asserted that
the decoded allocation was smaller than its lower bound. Local success had
not established that invalid metadata was safely rejected on the hosted path.
A fixed all-escape stream reproduced the assertion locally against the old
provider boundary; its nonzero exit and complete cleanup receipt are retained.

The existing feature-gated Vortex 0.85.0 provider remains the decoder. Before
requesting its decoded payload, ShardLoom now walks each selected row's encoded
codes and verifies that symbol expansion exactly matches the declared length.
Each escape must contain its literal byte within that row, and every symbol
reference must address a populated entry. Checking only the combined length
would miss incorrect row boundaries and cross-row escapes.

This validation adds no decoded payload allocation or replacement decoder.
Metadata execution, allocation admission and shared output leases retain their
existing boundaries. Errors leave no retained reservation in the tested denial
paths. Slices validate their selected encoded ranges, preserving nullable
Unicode and empty values even when unrelated retained prefix/suffix bytes are
malformed.

The three new regressions cover the deterministic former panic, seven malformed
row/code variants, and all 64 signed/unsigned integer-width combinations for
lengths and offsets. All 13 FSST tests pass in the focused run and in the clean
corrected native suite. The hosted correction also passes the formerly failing
native Vortex job. No upstream dependency, query-engine integration, fallback,
public API or input eligibility expansion is introduced by this correction.

## Frozen corrected runtime

| Identity | Value |
| --- | --- |
| Corrected runtime commit | `b7de216fe421012d3f9b632ad19e95f19b88dbe5` |
| Runtime assets | 947, verified against the commit |
| Source identity | `7f2100d0cda932636b024d1ec2f79f6fd3025c783a18da75b7c07d243781bff9` |
| Executable SHA-256 | `a0c1ae72701b09ece50db5df45a4ce666b5c9f3ece10a49a9e9eab9f91fa3323` |
| Executable bytes | 74,224,208 |
| Build | Rust 1.99.0, release, `release-user-surfaces`, macOS arm64 |

All 15 source gates pass, including the required workspace formatting, Clippy
and tests; native provider and CLI tests/Clippy; feature boundaries; Rust 1.96
compatibility; dependency checks; and 615 Python tests. The native provider
suite passes 2,389 tests with 24 explicitly ignored; the CLI suite passes 1,307.
The deterministic old-code failure remains a failure record, not a passing gate.

## Repeated input pressure observations

The five-case declaration is byte-identical to the original acceptance.
Each successful case consumes 1,152 batches of 1,024 rows with a 4,096-byte
UTF8 payload per row: 4.5 GiB of string payload, or 4,851,019,008 logical native
input bytes including the other declared fields and metadata. Every complete
selected result contains 1,152 exact rows. Streaming retains at most one input
batch, accounting for 4,210,954 logical native bytes.

| Observation | Native grant | Complete seconds | First provisional seconds | Peak tracked reservation bytes | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Streaming delivery under pressure | 1 GiB | 12.982265 | 0.562701 | 71,534,488 | Complete |
| Resident delivery under pressure | 1 GiB | 2.721065 | None | Not reported on denial | Expected denial after 239 batches |
| Streaming delivery with ample memory | 6 GiB | 12.222188 | 0.017981 | 71,534,488 | Complete |
| Resident delivery with ample memory | 6 GiB | 12.045921 | 11.772430 | 4,922,459,784 | Complete |
| Streaming native Vortex write under pressure | 1 GiB | 12.194011 | Not a delivery iterator | 81,103,316 | Complete, reopened |

These are separate single observations with producer generation, typed intake,
native execution, complete delivery and process exit inside the timer. Oracle
generation and file readback are outside it. Ample-memory streaming is slower
than its resident observation; neither these samples nor changes from the earlier
build establish a relative-speed claim. Tracked reservations are not process RSS.
Finite input/type/plan limits and separately admitted sink/output state remain.

The native output is 5,249,964 bytes with SHA-256
`c8dc8d744b1f93c67dd037a703025b653c03b2c665e4ca95539d6790e487c8a7`.
Its complete readback matches the independently generated values and the original
acceptance artifact. The failing resident control produces no successful prefix.

## Complete regression and evidence

Fresh checks pass 27,373 public cases over 15,820,181 complete rows, 202 direct
cases over 131,734 rows, 48 resident batch checks, 19 format checks, 145 admitted
semantic stages, nine golden stages and all 40 streaming conformance cases.
The declarations and independent public-case oracles remain unchanged. Expected
denials, cancellation, late producer failures and complete Vortex readback stay
part of the acceptance surface.

All 43 ClickBench queries run three times against the same 15,682,956,489-byte
native artifact with 99,997,497 rows. Every complete result matches the retained
native regression reference; floating comparisons keep its existing 1e-12
tolerance. This is a previously frozen regression reference, not a new
independently implemented ClickBench oracle. All three footer-only COUNT calls
retain native no-read/no-decode/no-row-materialization proof.

| Corrected-runtime Full43 observation | Value |
| --- | ---: |
| Sum of query minima | 69.221210 seconds |
| Sum of query medians | 70.299431 seconds |
| Sum of all 129 complete calls | 211.331544 seconds |
| Maximum observed native process RSS | 5,449,662,464 bytes |

The macOS arm64 host has ten logical CPUs and 16 GiB physical RAM; execution
policy allows 24 GiB with maximum parallelism 12. These are distinct values.
The clock includes process startup, complete output and exit, excluding ingest,
hashing and validation. Source prehashing warms the file cache; normal host/cache
activity is uncontrolled. This is an unpaired regression observation and supports
no speedup or slowdown attribution to the correction.

Two real preflight refusals preserve the unchanged 252-MiB log-admission limit.
Across two verified compactions, three completed historical cohorts retain all
1,548 original JSON/companion members and unchanged summaries; 5,582,848 accounted
log bytes are recovered. Only redundant completed log containers are removed.

The outer supervisor disappeared during the first corrected-runtime ClickBench
attempt. Its orphaned runner was stopped after 91 passing calls, the native children drained
and the UAT lock removed. Its complete partial files, log and interruption receipt
are preserved, with no acceptance credit. The subsequent pass starts from query
one and records all 129 results plus a successful supervisor/cleanup receipt.
Previously completed stages are reused only after verifying their receipts,
the executable and all 947 source assets. No failed or partial run is relabeled.

The finalizer reopens 54,733 public, 367 direct, 44 resident-batch, 24 format and
36 streaming envelopes, all ten malformed protocol traces, all five pressure
reports, all 129 ClickBench results and every source asset. The separately run
stream inspector verifies the complete decompressed hash, structural counts,
native-route and no-fallback claims after 168 mutation checks. All pass.

The [index](evidence/native-fsst-admission-2026-10-07.json) links the
[immutable packet](evidence/native-fsst-admission-2026-10-07.json.xz) and
[independent inspection](evidence/native-fsst-admission-2026-10-07-inspection.json).
The packet contains 44,377,572 compressed bytes with SHA-256
`2e66feb14889c3e6a777d4bca44db668c8883cadf1eef7add5e05e6f41ac8a17`.
Its 3,432,817,204 decompressed bytes have SHA-256
`6dd11954f583f1c72ae87767100930b66b6e7f882352569bafea6c19a1eb096f`.
Original-byte and portable-text identities remain distinct.

The [documentation acceptance](evidence/native-fsst-documentation-2026-10-07.json)
records seven source/site checks, three documentation examples executed against
the corrected native binary, and desktop/mobile/search review. All pass, with
no browser warnings or errors. The temporary preview process was drained.
Final hosted integration remains in progress; the PR records its final status.

The original packet remains byte-identical at SHA-256
`bc6560f39385f72a8f58688e136983b9243bf22b6d724ef5d31e82af763873d2`.
The corrected packet retains the original hosted failure, the fixed local
reproducer, the correction diff and both 947-asset commit checks alongside its
fresh complete results. The original source is not relabeled as unchanged.

This closes no additional spill, cache, rematerialization, multiway join or broad
PERF/CG obligation. No package publication, whole-process memory bound or engine
speedup follows from the correction.
