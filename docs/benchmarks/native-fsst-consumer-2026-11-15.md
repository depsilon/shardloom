<!-- SPDX-License-Identifier: Apache-2.0 -->

# Conservative FSST source prefilter: retained

The finite prefilter is **retained locally** at
`45be8e0bd1d9cef920c88fa9bf91811deade41c4`. Complete results, schemas, native
execution, source identities, ownership and resource checks pass. Both complete
engine totals decrease under the frozen gain/loss rule. The
[architecture contract](../architecture/native-fsst-consumer-2026-11-15.md) and
[portable packet](evidence/native-fsst-consumer-2026-11-15/README.md) preserve the
admission boundary, every observation and the remaining work.

## Complete engine comparison

| Population | Requests or operations per runtime | Accepted baseline, engine seconds | FSST candidate, engine seconds |
| --- | ---: | ---: | ---: |
| Complete fresh-session cohort | 9,040 requests | 682.167470629 | 663.612327731 |
| Original selected declarations | 4,150 requests | 675.799882008 | 657.388733689 |
| Dictionary and ordinary text controls | 1,380 requests | 2.208568271 | 2.186203195 |
| Text predicate and encoding controls | 3,510 requests | 4.159020350 | 4.037390847 |
| Separate prepared-plan lifecycles | 540 operations | 0.520543159 | 0.511136028 |

The fresh gain is 18.555142898 engine seconds. All 1,759 individual fresh
regressions remain in the raw ledgers and complete replay; their positive
candidate-minus-baseline differences sum to 1.675803154 engine seconds. Both
complete fresh and prepared totals must strictly decrease, with no minimum
percentage threshold. No subgroup result replaces those gates.

These sums use `engine_execution_seconds` for the frozen selected population,
not Full43 or a complete 10k pass. All baseline observations are reused, and
host/cache state is uncontrolled. Most of the absolute difference lies in the
original declarations. This single comparison does not establish causal
attribution of the total gain to FSST, an isolated per-kernel speedup, or general
performance superiority. Per-request release prefilter counters were not measured.

## Native mechanism and admission

The pinned Vortex 0.93.0 provider already implements exact compressed FSST LIKE.
ShardLoom's local modification adds a conservative necessary-byte test inside
its existing positive literal-contains route. It chooses the first pattern byte
represented by the fewest populated symbol codes, admitting at most two codes
plus the escape code. The already-resolved `memchr` 2.8.3 provider performs the
bounded code search. Every possible candidate retains the original exact DFA.

If the complete compressed backing contains no candidate, exact matcher
construction is avoided. Otherwise each row's necessary test may reject exact
matching. Escapes always survive the filter, so escaped payload bytes can add
false positives but cannot remove a match. Symbol boundaries and Unicode retain
the exact matcher contract. The filter uses bounded stack tables; it adds no
retained heap cache. Offset execution and every original row slice are still
visited, preserving bounds and error ordering.

Prefix, empty, over-limit, nonconstant, case-insensitive and negated patterns
retain the existing behavior. Shared projection text preparation is unchanged.
The earlier malformed-length, escape, symbol and UTF-8 counterexamples decline
an unvalidated scalar bridge; this change does not admit that bridge or broaden
raw LIKE validation. Existing provider scratch exclusions remain explicit.
The bounded stack filter does not establish full-provider allocation accounting.

The final untimed diagnostic has 1,999 records: 864 physical/shared declarations,
378 complete native-core requests, 378 refund records, 378 dispatch records and
one completion record. It replays against 288 frozen nullable Boolean vectors
over the existing 54 plain inputs: nine profiles, two sizes and flat/dictionary/
FSST representations. All source generations and full input hashes remain bound.

The actual route observer records 688 successful LIKE calls and 198,288 logical
operand rows. Of those, 638 calls admit the prefilter over 185,856 rows. It rejects
131,853 rows, while 66,435 rows reach exact matching across admitted and declined
calls. There are 452 matcher constructions and 236 whole-backing rejections.
The explicitly registered session observer compiles out of release builds;
these counts include repeated requests and null rows, not unique values or
decoded bytes. Dense, saturated, empty/null, projection and prefix controls stay
in the complete population.

## Execution and source binding

The 9,040 declarations are unchanged from the completed text-signature population.
Both roles use its plain inputs, without the declined summary metadata. All
9,040 baseline fresh results and 540 baseline prepared operations are reused
with exact request, worker, input, reference, runtime and consumer bindings.
Their earlier reuse chains remain preserved. Only a byte-identical pure fixture
module is relocated after its obsolete source-tree copy was removed; public
workers and requests are unchanged.

The candidate executes each fresh request once and each of 270 prepared plans
twice. There are no warmups, extra repeats, cache manipulation or timing-based
selection. Source/input hashing and recursive inventories occur outside query
execution. The existing lightweight supervisors retain deadlines, storage limits,
result validation and process cleanup. Builds, packaging, subagents and other
task work are idle during candidate query execution. No external engine runs.

The measured source fingerprint is
`f11e53979c13d7b4b253e92397e73813ab0db98507149a11199b892d13030ff1`.
All 5,794 captured source files match the preservation commit's blobs. The
actual build uses parent `873e60bca36f5030cec27c3baf59017ca49ad8ab` plus the
recorded source changes. The later source commit is not substituted for that
build identity. The release build freshly compiles `vortex-fsst`,
`shardloom-vortex`, `shardloom-cli` and `shardloom-python`:

```text
cargo build --offline --locked --release --features release-user-surfaces -p shardloom-cli -p shardloom-python
```

The only new dependency edge is `vortex-fsst -> memchr`; version 2.8.3 was
already resolved. Its MIT license option and original archive are verified.
Original Vortex provenance, local modification hashes and Python notices agree.
No external implementation is copied and no execution fallback is added.

## Validation and portable closeout

Required checks pass:

- Workspace formatting and strict default/native Clippy.
- Default workspace: 3,344 passed, zero failed or ignored.
- Native workspace: 7,266 passed, zero failed and 41 declared ignored. These
  comprise 38 opt-in diagnostics/fixtures and three Windows-only checks ignored
  on this macOS host; selected FSST diagnostics run separately and are retained.
- FSST provider: 113 passed, zero failed and one intentional ignore, plus strict
  provider lint. Focused exhaustive controls cover 37,448 small code streams.
- Minimal, all-feature, file/local reader boundaries and Rust 1.96.0 checks.
- Installed wheel: 455 passed, zero failures, errors or skips.

The packet preserves earlier admission/validator failures and four test-helper
lint corrections, with exact source snapshots. All scored candidate requests
complete without a failed or repeated query. Twelve synthetic acceptance
controls were frozen before measurement and never substitute for observations.

Portable replay checks all 18,080 complete fresh results and 1,080 prepared
operations, then recomputes the decision. All 57 packet corruption controls and
eight additional repair-provenance controls reject their mutations. Relocated
execution forbids reads from original source/runtime/evidence paths, native
engine imports, subprocesses and network access.

The first packaging attempt stops before writing at the unchanged storage floor.
Verified retirement removes only completed regenerable debug files; measured
inputs, runtimes, tested wheel and release cache remain unchanged. The first
relocated replay then exposes missing shared observation validators. The repair
adds their exact previously accepted bytes and provenance chain; all 62 local
imports pass. The nine original archive parts and every measurement remain
byte-identical, and all original failures remain recorded. Large native/input/
consumer-environment payloads are explicit identity-only exclusions, not required
to replay the complete saved values and references.

The prefilter's bounded decision is complete. Exact-table matcher lifetime
requires its own identity, retained-memory and complete-comparison proof. Broader
I attribution and narrower batching, required item-19 spatial execution, optional
spatial summaries, codec choices and bounded CPU fusion remain open in the
[ordered queue](../architecture/native-metadata-encoded-consumer-investigations-2026-11-13.md).
Item 20, RFC 0044 and CG-1 through CG-23 remain open where recorded. This local
disposition creates no merge, release, writer default or competitive claim.
