<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared native and combined workflow acceptance

`validation.json` records local acceptance across 18 source, artifact and package
checks. The default suite passes 3,351 tests; enabled-native passes 4,432 with
41 declared ignored; public CLI passes 1,256; focused Python passes 34; the fresh
installed wheel passes 466 with zero errors, failures or skips and verified
uninstall. These populations overlap and must not be added together.

`observations.json.gz` retains exact receipts, logs, build manifests, package
proof, the seven-family matrix, fixture/design notes and 68 source files. It
preserves the original focused failure and its original expected-schema source.
The corrected fixture requires the existing duration extension metadata without
weakening schema checks. The two runtime artifacts are byte-identical across
that correction. Earlier default checks have five explicitly identified native
test-only source differences; complete repository equality is not claimed.

The matrix contains 107 distinct passing tests and one ignored diagnostic with
separate isolated evidence. It does not establish the full Cartesian product of
interfaces, formats and operators. The
[worker observation](../shared-native-worker-observation-2026-11-17/README.md)
and [string delivery](../shared-native-string-delivery-2026-11-17/README.md)
packets retain all earlier diagnostic outcomes, failed attempts and the stopped
unoptimized native run. Their archive fingerprints are recorded here unchanged.

Replay with only the Python standard library:

```sh
python3 docs/release/evidence/shared-native-incorporation-2026-11-18/replay.py \
  docs/release/evidence/shared-native-incorporation-2026-11-18 --repository .
```

Copy this directory elsewhere and omit `--repository` to verify its retained
bytes and acceptance summaries independently of a checkout or build. Replay
does not import the native engine, execute a query, rebuild a package or repeat
the tests. With `--repository`, it additionally binds the 68 source files and
the two predecessor archive fingerprints to the selected checkout.

`verification.json` records a successful relocated replay and four rejected
controls: altered source bytes, an installed-test skip, an erased original
failure and a changed compiled-source identity. These checks execute no queries.

The [scope report](../../../architecture/native-shared-combined-acceptance-2026-11-18.md)
records the mixed document/portfolio workflow and five local analytical
workflows. Safe remote refresh stays with its deferred I/O/Foundry owner;
deep traversal remains after Foundry with unchanged public limits. No new
finite-feedback control was admitted by the bounded performance screens.
Hosted review/checks remain pending. No new benchmark, performance claim,
publication or competitive-gate closure follows from this packet.
