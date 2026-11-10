<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native Variant field-expression acceptance

Local acceptance passed on November 10, 2026. Hosted review, PR/main checks and
final retirement remain separate. The
[contract](../../../architecture/native-variant-expressions-2026-11-10.md)
admits literal-path `variant_get`, explicit typed `try_variant_get` and
`is_variant_null` through the shared native binder and pinned Vortex provider.
This finite unit leaves useful spatial support and resource-admitted deep
traversal required; it does not close item 19, item 20, RFC 0044 or CG-1–CG-23.

The packet binds 14 completed validation roles to source fingerprint
`e767ebc5e86cbcc9b0a7ba8050f7cd489dab4e869d579e0bc0aeeb583c08daab`:

- Formatting and default/native strict lint pass. Complete default/native
  workspace tests pass 3,323/7,065 cases, with 37 native opt-in ignores.
- Seven feature configurations and two supported-Rust configurations pass.
- All 13 new native/path/parser tests and six frozen independent fixture
  identities are verified. All 25 selected public/catalog/schema tests pass.
- The fresh release runtime and its exact installed wheel pass all 441
  integration tests with zero skips; uninstall succeeds. Source bytes inside
  the wheel match the final Python sources and runtime identities.
- Python core reports 868 runs from 1,301 declared cases, 63 optional/environment
  skip records and no failures or errors. Final documentation and repository
  contract checks pass against the complete final source inventory.
- Two positive controls replay the original and relocated packets. All 21
  negative controls reject changed archives, source/provider/lock identities,
  base adoption, tests, installed artifacts, frozen fixtures, missing failures,
  and invalid predecessor main/retirement evidence.

The 12 original preparation-role observations retain their original labels.
Their production, provider, lock/config and fixture bytes match the final source;
only four explicitly named status-prose files may differ. Fresh documentation
and contract roles require exact final-source equality. UUID PR #1675's accepted
merge, `8df722749e3993bb27a9cdd573de8a2a3860bb34`, has the same source tree as the
preparation base. Its 58 PR checks, 55 exact-main checks, clear review, verified
retirement and post-retirement replay are preserved separately. Base adoption
preserved the working diff, dirty-file identities and index.

Eight original failed or interrupted observations remain byte-bound in the
archive, with explicit dispositions. Preparation retirement retains its manifest,
closed-handle/identity checks and unchanged input/runtime evidence. No storage
floor was lowered and no payload backup was created. Earlier packets retain
their original status and observations.

## Replay

From the repository root:

```bash
python3 -I -B docs/release/evidence/native-variant-expressions-2026-11-10/verify.py --require-controls --repo-root .
```

For portable replay, copy `acceptance.json`, `verification.tar.xz`, `verify.py`
and `corruption-controls.json` together, then run:

```bash
python3 -I -B verify.py --require-controls
```

Replay verifies recorded source, result, ownership, process-cleanup and control
evidence without repeating native execution. The archive contains 5,783 members;
the acceptance manifest fixes every member identity and the complete archive
digest. Embedded earlier evidence is represented by exact replacement identities
instead of recursive archive copies.

## Claim boundary

Literal path/type controls, depth limits, SQL-null versus embedded-null behavior,
typed conversion pairs and failure behavior remain bounded by the contract.
The pinned provider's Arrow format bridge is admitted and reported; conservative
workspace reservations cover expansion and returned outputs enter credited
native ownership before temporary storage is released. Decoder bytes and total
process RSS remain unmeasured. No zero-copy, zero-decode, benchmark, performance,
package publication or broad production-readiness claim is made. No external
query engine executes ShardLoom work.
