<!-- SPDX-License-Identifier: Apache-2.0 -->

# Combined dependency acceptance

This packet records local acceptance for flate2 1.1.10, tonic 0.14.9 and
tonic-prost 0.14.8 on the managed binary-object source base. The
[intake decision](../../../architecture/dependency-intake-2026-11-06.md) defines
the scope, original failures, optional-feature boundary and remaining hosted
acceptance.

Run the saved-evidence verifier with Python 3.11 or newer, from the repository
root:

```sh
python3 -B docs/benchmarks/evidence/dependency-followup-2026-11-06/verify.py --repo-root .
```

Omit `--repo-root .` to replay a relocated copy containing `acceptance.json`,
`verification.tar.xz` and `verify.py`. The verifier uses no network, extracts
no files and runs no engine queries, tests or benchmarks. It checks the archive
digest and all 441 member identities, the complete old/new published-source
comparisons, the 435-registry/13-vendor notice bundle, the required saved check
set, source continuity and installed-package results. With `--repo-root`, it
also compares the three current Cargo files to their accepted bytes.

The [corruption controls](corruption-controls.json) preserve an untouched-packet
pass and rejection of five isolated changes: a missing required stage, an
inflated Python run count, an installed-test skip, a changed source extent and
a wrong archive digest. The original packet bytes remain unchanged.

The saved checks include 3,319 default Rust tests, 6,947 native/optional-graph
tests with 37 existing ignores, strict lint and format, seven feature-matrix
stages, Rust 1.96 compatibility and all 319 installed-wheel integration tests
without skips. The core Python shard records 833 reported runs and 47 skip
records separately from its 1,175 discovered cases.

The original dependency proposals' failing logs, the hosted disk exhaustion,
the initial manifest merge conflict, source-inventory corrections and complete
legal-text provenance remain in the archive. The configured audit passes with
three unchanged informational warnings for paste, anyhow and event-listener;
this packet does not establish remediation or unreachable affected APIs.

The installed-wheel result is local macOS arm64 evidence. Hosted platform
checks, review, merge and main verification remain separate. This packet makes
no performance, package-publication or public transport-service claim.
