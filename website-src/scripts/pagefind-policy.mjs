// Pagefind runs its local WebAssembly index inside a blob worker.
// https://pagefind.app/docs/hosting/#content-security-policy-csp
export function assertPagefindPolicy(headers) {
  let route;
  const policies = [];
  for (const line of headers.split(/\r?\n/u)) {
    if (!line.trim() || line.trimStart().startsWith("#")) continue;
    if (!/^\s/u.test(line)) {
      route = line.trim();
      continue;
    }
    const match = /^\s+Content-Security-Policy:\s*(.*)$/iu.exec(line);
    if (match) policies.push({ route, value: match[1] });
  }
  if (policies.length !== 1 || policies[0].route !== "/*") {
    throw new Error("Pagefind requires one reviewed global Content-Security-Policy");
  }

  const directives = new Map();
  for (const value of policies[0].value.split(";")) {
    const [name, ...sources] = value.trim().split(/\s+/u);
    if (!name) continue;
    const key = name.toLowerCase();
    if (directives.has(key)) throw new Error(`duplicate CSP directive: ${key}`);
    directives.set(key, sources);
  }
  const required = {
    "script-src": ["'self'", "'unsafe-inline'", "'wasm-unsafe-eval'"],
    "worker-src": ["'self'", "blob:"],
  };
  for (const [name, expected] of Object.entries(required)) {
    const actual = directives.get(name) ?? [];
    if (actual.length !== expected.length || !expected.every((source) => actual.includes(source))) {
      throw new Error(`Pagefind ${name} must allow only ${expected.join(" ")}`);
    }
  }
}
