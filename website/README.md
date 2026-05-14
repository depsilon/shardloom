# ShardLoom Website

This directory contains the static Cloudflare Pages site for ShardLoom.

## Cloudflare Pages

- Project name: `shardloom`
- Production branch: `main`
- Build command: blank, or `exit 0`
- Build output directory: `website`
- Custom domain: `shardloom.io`
- Repository name: the repo does not need to be named `shardloom.io`

No separate repository is required. Deploy this directory from the existing ShardLoom repository.

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
- `404.html`: Cloudflare Pages custom 404.
- `robots.txt`: crawler policy.
- `sitemap.xml`: production sitemap for `https://shardloom.io/`.
- `_headers`: Cloudflare Pages security and cache headers.
- `_redirects`: simple canonical redirects.
- `BRAND.md`: ShardLoom brand asset guidance.
- `assets/logo/.gitkeep`: placeholder for future committed brand assets.

Do not add a second license file under this directory. Project code remains under the repository
Apache-2.0 license. ShardLoom names, logos, and icons are brand assets; see `BRAND.md`.
