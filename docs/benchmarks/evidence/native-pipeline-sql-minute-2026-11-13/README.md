<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL minute correction validation

This packet records correctness checks for
`ab6dd9ed50c08030d0571254b40f068d03bd3fb2`. The later evidence/documentation
commit changes no tested production or test source. It follows the frozen
[10,000-case observation](../native-pipeline-consolidation-2026-11-12/README.md);
that packet and its measured runtime are unchanged. No benchmark, package
publication or release was performed for this correction.

Both `EXTRACT(minute FROM expression)` and `DATE_TRUNC('minute', expression)`
enter the shared native compiler and retain the existing integer-epoch-second
and timestamp-text contracts. The [architecture contract](../../../architecture/native-pipeline-consolidation-2026-11-12.md#november-13-sql-minute-correction)
records the type boundary, complete-expression matching and provider decision.

Final checks passed:

- Eight focused parser, admission and complete-result tests.
- `cargo fmt --all -- --check`.
- `cargo clippy --offline --locked --workspace --all-targets -- -D warnings`.
- `cargo test --offline --locked --workspace --all-targets`: 3,325 passed.
- Workspace strict lint with
  `shardloom-python/python-native,shardloom-vortex/native-encoded-exchange-experiment`.
- `cargo test --offline --locked -p shardloom-cli -p shardloom-python --all-targets --features release-user-surfaces`:
  1,567 passed, three existing ignored cases.
- Fresh `release-user-surfaces` build and isolated installed-wheel proof:
  442 passed with zero failures, errors or skips; uninstall verification passed.
- Contribution-governance validation and replay of the unchanged original
  observation packet, without executing any query.

The installed test `test_minute_sql_forms_collect_stream_and_persist_native_vortex`
checks both forms against explicit expected rows through collection, streaming
and Vortex write/reopen. Native Rust tests separately cover negative/positive
boundaries, nulls, empty inputs, unsigned epochs, text, nested/lazy operands,
predicates, aggregates, grouping, ordering, LIMIT/OFFSET, malformed syntax and
typed-timestamp rejection without reinterpreting units.

`validation.json` records exact commands, test counts, source/runtime identities
and the 53 archive-member identities. `observations.tar.gz` contains the original
receipts, logs and local guard/freeze/packaging helpers, including the first
test-fixture failure and a release build deliberately stopped for final syntax
review. Neither is relabeled as a completed successful check. The focused r3
receipt records the preceding HEAD because commit metadata advanced during
that check; its complete source inventory equals the final build. All final
broad checks and installed proof record the final source commit.

No runtime, wheel, environment or dataset payload is archived. Raw identities
are historical provenance; archived local paths are not portable prerequisites.
The ordinary Cargo commands and repository wheel-proof script are the
reproduction entrypoints. The guard/freeze helpers additionally document the
local supervision used for these observations.

Verify the packet bytes without native execution:

```sh
cd docs/benchmarks/evidence/native-pipeline-sql-minute-2026-11-13
shasum -a 256 -c checksums.sha256
```

The archive was reopened and every original member compared byte-for-byte
before packaging. Hosted PR checks are a separate validation surface.
