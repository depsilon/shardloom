import { defineConfig } from "astro/config";
import mdx from "@astrojs/mdx";
import sitemap from "@astrojs/sitemap";
import starlight from "@astrojs/starlight";

export default defineConfig({
  site: "https://shardloom.io",
  output: "static",
  outDir: "../website",
  publicDir: "../website-public",
  trailingSlash: "never",
  integrations: [
    starlight({
      title: "ShardLoom",
      description: "Field Guide and docs for ShardLoom evidence-gated compute.",
      favicon: "/assets/logo/shardloom-favicon.png",
      customCss: ["./src/styles/starlight.css"],
      head: [
        {
          tag: "script",
          content:
            "try{if(!localStorage.getItem('starlight-theme'))localStorage.setItem('starlight-theme','light')}catch{}",
        },
        {
          tag: "link",
          attrs: { rel: "stylesheet", href: "/assets/site.css" },
        },
        {
          tag: "meta",
          attrs: { name: "robots", content: "index,follow" },
        },
      ],
      pagefind: true,
      social: [
        { icon: "github", label: "GitHub", href: "https://github.com/depsilon/shardloom" },
      ],
      sidebar: [
        {
          label: "Field Guide",
          items: [{ autogenerate: { directory: "field-guide" } }],
        },
        {
          label: "Public Pages",
          items: [
            { label: "Home", link: "/" },
            { label: "About", link: "/about" },
            { label: "Start", link: "/start" },
            { label: "Use Cases", link: "/use-cases" },
            { label: "Benchmarks", link: "/benchmarks" },
            { label: "Architecture", link: "/architecture" },
            { label: "Status", link: "/status" },
            { label: "Docs", link: "/docs" },
          ],
        },
      ],
    }),
    mdx(),
    sitemap(),
  ],
});
