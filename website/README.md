# ShardLoom Website

This directory contains the static Cloudflare Workers Static Assets site for ShardLoom.

## Cloudflare Workers Static Assets

- Project name: `shardloom`
- Production branch: `main`
- Deploy command: `npx wrangler deploy`
- Static assets served from: `website/`
- Production domain: `shardloom.io`
- Custom domain: `shardloom.io`
- Repository: `depsilon/shardloom`
- Repository name: the repo does not need to be named `shardloom.io`

No separate repository is required. Deploy this directory from the existing ShardLoom repository.

Cloudflare setup:

1. Create application.
2. Continue with GitHub.
3. Select `depsilon/shardloom`.
4. Use deploy command `npx wrangler deploy`.
5. Use production branch `main`.
6. Serve static assets from `website/` through the root `wrangler.toml`.
7. Add production/custom domain `shardloom.io`.

The root `wrangler.toml` uses Workers Static Assets only. It does not define a Worker script,
Worker runtime JavaScript, or secrets.

The landing page includes a small first-party browser script at `assets/compute-flow.js`. It fetches
`docs/architecture/compute-engine-flow-reference.md` from the `main` branch on GitHub, parses the
flow tables and Mermaid node labels, and renders the current access, runtime, execution-lane,
batch/live/hybrid, I/O/downstream, timing, and guardrail summaries. If the fetch fails, the page
uses embedded claim-safe fallback copy.

## Content Rules

Website copy must stay claim-safe:

- ShardLoom is pre-release.
- ShardLoom is Vortex-native and no-fallback by design.
- Current evidence supports an evidence-certified local compute engine foundation.
- Do not claim ShardLoom is a Spark replacement.
- Do not claim production SQL or DataFrame runtime support.
- Do not claim production object-store, lakehouse, or Foundry support.
- Do not make performance, superiority, or best-default claims.

## Files

- `index.html`: main static page.
- `404.html`: custom 404 page.
- `robots.txt`: crawler policy.
- `sitemap.xml`: production sitemap for `https://shardloom.io/`.
- `_headers`: static security and cache headers where supported by the selected Cloudflare flow.
- `_redirects`: simple canonical redirects where supported by the selected Cloudflare flow.
- `BRAND.md`: ShardLoom brand asset guidance.
- `assets/logo/shardloom-logo.png`: full ShardLoom logo asset.
- `assets/logo/shardloom-logo-trim.png`: trimmed ShardLoom logo asset.
- `assets/logo/shardloom-favicon.png`: icon/favicon asset.
- `assets/compute-flow.js`: first-party parser for the canonical compute-engine flow reference.

Do not add a second license file under this directory. Project code remains under the repository
Apache-2.0 license. ShardLoom names, logos, and icons are brand assets; see `BRAND.md`.
