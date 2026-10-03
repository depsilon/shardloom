import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { evaluateAudit } from "./audit-dependencies.mjs";

const policy = JSON.parse(readFileSync(new URL("../dependency-audit-exception.json", import.meta.url)));
const pinned = Buffer.from("reviewed file bytes");
const exception = {
  ...policy, approved: true,
  sha256: Object.fromEntries(Object.keys(policy.sha256).map(path =>
    [path, createHash("sha256").update(pinned).digest("hex")])),
};
const options = { now: new Date("2026-10-03T00:00:00Z"), readPinnedFile: () => pinned };
function findings() {
  const causes = {
    "http-cache-semantics": [{ url: policy.advisory, name: policy.package, dependency: policy.package }],
    astro: ["http-cache-semantics"],
    "@astrojs/mdx": ["astro"],
    "astro-expressive-code": ["astro"],
    "@astrojs/starlight": ["@astrojs/mdx", "astro", "astro-expressive-code"],
  };
  return {
    auditReportVersion: 2,
    vulnerabilities: Object.fromEntries(Object.entries(causes).map(([name, via]) => [name, { name, via }])),
    metadata: { vulnerabilities: { total: 5 } },
  };
}
test("retains an honest exception result for one advisory and its complete chain", () => {
  assert.deepEqual(evaluateAudit(findings(), exception, options), {
    status: "reviewed_exception", advisory: policy.advisory, expires: policy.expires,
    findings: 5, dependency_vulnerability_fixed: false,
  });
});
test("approval, expiration, file changes and missing installed files block acceptance", () => {
  assert.throws(() => evaluateAudit(findings(), { ...exception, approved: false }, options), /approval/);
  assert.throws(() => evaluateAudit(findings(), exception, { ...options, now: new Date(policy.expires) }), /expired/);
  assert.throws(() => evaluateAudit(findings(), exception, { ...options, readPinnedFile: () => Buffer.from("changed") }), /scope changed/);
  assert.throws(() => evaluateAudit(findings(), exception, { ...options, readPinnedFile: () => { throw Error("missing"); } }), /missing/);
});
test("new findings in an existing dependency remain blocking", () => {
  const report = findings();
  report.vulnerabilities.astro.via.push({ url: "https://github.com/advisories/NEW", name: "astro", dependency: "astro" });
  assert.throws(() => evaluateAudit(report, exception, options), /Unexcepted/);
});
test("replacing a reviewed scope file with an unrelated fingerprint is denied", () => {
  const changed = { ...exception, sha256: { ...exception.sha256 } };
  delete changed.sha256["wrangler.jsonc"];
  changed.sha256["unrelated.txt"] = createHash("sha256").update(pinned).digest("hex");
  assert.throws(() => evaluateAudit(findings(), changed, options), /missing its reviewed scope/);
});
test("new packages, missing links, empty causes and dependency cycles remain blocking", () => {
  for (const via of [["missing"], [], ["@astrojs/starlight"]]) {
    const report = findings();
    report.vulnerabilities.astro.via = via;
    assert.throws(() => evaluateAudit(report, exception, options), /Unresolved/);
  }
});
test("malformed, unavailable or inconsistent audit reports never pass", () => {
  for (const report of [null, {}, { error: {} }, { ...findings(), metadata: {} },
    ...[true, 1, "", []].map(vulnerabilities => ({
      auditReportVersion: 2, vulnerabilities, metadata: { vulnerabilities: { total: 0 } },
    })),
  ]) {
    assert.throws(() => evaluateAudit(report, exception, options));
  }
});
test("a clean audit requires no exception and does not pretend an accepted finding is fixed", () => {
  assert.deepEqual(evaluateAudit({ auditReportVersion: 2, vulnerabilities: {}, metadata: { vulnerabilities: { total: 0 } } }, null), { status: "clean", findings: 0 });
});
