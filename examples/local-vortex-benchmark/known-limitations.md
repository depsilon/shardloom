<!-- SPDX-License-Identifier: Apache-2.0 -->

# Known Limitations

- The wrapper delegates to the shared harness and requires an already-built ShardLoom binary and
  a local-only workspace. It does not build the binary or create its own fixture/run directories.
- The example selects only the `selective filter` scenario. The pandas result is an independent
  correctness reference; pandas never executes unsupported ShardLoom work.
- This local comparison does not by itself establish benchmark acceptance, performance, public
  publication, production readiness, or object-store/table, broad SQL/DataFrame, live, hybrid, or
  Foundry support.
- `expected-output.json` and `expected-certificate-fields.json` are declarative example metadata;
  they are not captured results, certificates, or runtime proof.
