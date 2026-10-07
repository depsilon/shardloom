import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

import { assertPagefindPolicy } from "./pagefind-policy.mjs";

const supported = "\n/*\n  Content-Security-Policy: default-src 'self'; script-src 'self' 'unsafe-inline' 'wasm-unsafe-eval'; worker-src 'self' blob:; frame-ancestors 'none'\n\n/assets/*\n  Cache-Control: public, max-age=3600\n";

test("the deployed source policy admits Pagefind without JavaScript eval", () => {
  const headers = fs.readFileSync(new URL("../../website-public/_headers", import.meta.url), "utf8");
  assertPagefindPolicy(headers);
});

test("same-origin WebAssembly and blob workers are admitted", () => {
  assertPagefindPolicy(supported);
  assertPagefindPolicy(supported.replaceAll("\n", "\r\n").replace("Content-Security-Policy", "content-security-policy"));
});

test("the original deployed policy blocks WebAssembly despite a working worker source", () => {
  assert.throws(() => assertPagefindPolicy(supported.replace(" 'wasm-unsafe-eval'", "")), /script-src/u);
});

test("JavaScript eval and additional script origins are rejected", () => {
  for (const extra of ["'unsafe-eval'", "https://example.com", "*", "blob:", "data:"]) {
    assert.throws(() => assertPagefindPolicy(supported.replace("script-src ", `script-src ${extra} `)), /script-src/u);
  }
});

test("a comment or a different directive cannot admit WebAssembly scripts", () => {
  const blocked = supported.replace(" 'wasm-unsafe-eval'", "");
  assert.throws(() => assertPagefindPolicy(`# 'wasm-unsafe-eval'\n${blocked}`), /script-src/u);
  assert.throws(() => assertPagefindPolicy(blocked.replace("frame-ancestors", "style-src 'wasm-unsafe-eval'; frame-ancestors")), /script-src/u);
});

test("the worker must retain both same-origin and blob admission", () => {
  for (const replacement of ["worker-src 'self'", "worker-src blob:", "worker-src *", "connect-src 'self'"]) {
    assert.throws(() => assertPagefindPolicy(supported.replace("worker-src 'self' blob:", replacement)), /worker-src/u);
  }
});

test("missing, repeated or route-specific policies require an explicit review", () => {
  for (const invalid of ["", supported + supported, supported.replace("/*\n", "/field-guide/*\n")]) {
    assert.throws(() => assertPagefindPolicy(invalid), /one reviewed global/u);
  }
  assert.throws(() => assertPagefindPolicy(supported.replace("frame-ancestors", "script-src 'self'; frame-ancestors")), /duplicate CSP/u);
});
