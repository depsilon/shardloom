# Security Policy

ShardLoom is in early development. Please do not publicly disclose security vulnerabilities until
maintainers have had a reasonable opportunity to investigate and respond.

## Supported Versions

ShardLoom has not made a public stable release. Until a supported release line exists, maintainers
triage security reports against the current repository state and any published preview artifacts.

Future supported versions will be listed here by release series.

## Reporting A Vulnerability

Preferred reporting channel:

- Open a private security advisory through
  [GitHub Security Advisories](https://github.com/depsilon/shardloom/security/advisories/new)
  when available.
- If private advisories are unavailable, contact the maintainers through the project's listed
  security contact or repository owner channel.

Do not include credentials, private keys, production data, or governed dataset contents in the
initial report. Include enough detail for maintainers to reproduce the issue safely:

- affected version, commit, or artifact
- affected command/API/package path
- expected behavior
- observed behavior
- reproduction steps using synthetic data where possible
- whether an exploit is known or only suspected

## Response Targets

These targets are best-effort until ShardLoom has a formal release process:

- acknowledgement target: 3 business days
- initial triage target: 7 business days
- remediation target: depends on severity, exploitability, and release status

## Severity Categories

- Critical: remote code execution, credential exposure, package compromise, CI/publishing
  compromise, or a no-fallback bypass that executes an external engine as ShardLoom runtime.
- High: malicious input causes unsafe write, path traversal, artifact leak, deterministic crash in a
  supported path, or release artifact integrity failure.
- Medium: denial-of-service risk, malformed input panic in a non-release path, incomplete redaction,
  or dependency advisory with limited exploitability.
- Low: hardening gaps, documentation issues, or non-exploitable policy drift.

## Disclosure Policy

Maintainers may keep reports private while validating impact, preparing a fix, coordinating package
or dependency response, or avoiding active exploitation. Public disclosure should include affected
versions/artifacts, remediation guidance, and known limitations without exposing unnecessary exploit
details.

## Advisory And CVE Policy

For public releases, maintainers may publish GitHub Security Advisories and request CVEs for issues
that materially affect users. Preview-only issues may be documented through release notes or
repository advisories when appropriate.

## Security Release Policy

Security releases must be built from reviewed source and include:

- dependency/advisory audit status
- checksum manifest
- SBOM references
- provenance or attestation references where supported
- known unsupported paths
- no-fallback dependency statement

## User Notification Policy

Maintainers should notify users through GitHub Security Advisories, release notes, and repository
announcements. If a package artifact is affected, the package registry entry should be yanked,
deprecated, or otherwise marked where supported.

## Compromised Package Or Dependency Response

If a dependency, release artifact, package registry entry, CI workflow, or maintainer account is
suspected to be compromised, maintainers should:

1. Freeze publication and release workflows.
2. Disable or restrict affected workflow/environment paths.
3. Revoke or rotate credentials where applicable.
4. Identify affected commits, tags, artifacts, packages, SBOMs, and checksum manifests.
5. Verify source, package contents, checksums, SBOMs, and provenance.
6. Yank, deprecate, or mark affected package versions where supported.
7. Publish an advisory and remediation guidance.
8. Rebuild from known-good source before publication resumes.

## No-Fallback Security Invariant

Security response must not add Spark, DataFusion, DuckDB, Polars, pandas, Dask, Velox, Trino,
Snowflake, Databricks, BigQuery, Foundry compute, or any external engine as runtime fallback. Unsafe
or unsupported paths must remain blocked with deterministic diagnostics until ShardLoom-native
evidence exists.

## Related Documents

- [Security vulnerability, exploit, and supply-chain hardening RFC](docs/rfcs/0043-security-vulnerability-exploit-supply-chain-hardening.md)
- [Threat model](docs/security/threat-model.md)
- [Supply-chain response](docs/security/supply-chain-response.md)
- [OpenSSF Scorecard alert triage](docs/security/scorecard-alert-triage.md)
- [Dependency audit](docs/legal/dependency-audit.md)
- [SBOM generation plan](docs/release/sbom-generation-plan.md)
