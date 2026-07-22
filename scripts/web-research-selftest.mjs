import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";

import {
  __test,
  getWebResearchConfig,
  isPrivateOrReservedIp,
  normalizePublicUrl,
  runWebResearchCrawl,
  runWebResearchExtract,
  runWebResearchListings,
  runWebResearchMonitor
} from "../lib/web-research.js";

assert.throws(() => normalizePublicUrl("http://localhost/test"), /Lokale oder interne Hostnamen/);
assert.throws(() => normalizePublicUrl("http://127.0.0.1/test"), /Private oder reservierte/);
assert.throws(() => normalizePublicUrl("file:///etc/passwd"), /http\(s\)/);
assert.equal(isPrivateOrReservedIp("10.0.0.1"), true);
assert.equal(isPrivateOrReservedIp("169.254.1.1"), true);
assert.equal(isPrivateOrReservedIp("8.8.8.8"), false);
assert.equal(normalizePublicUrl("https://example.com/a#fragment"), "https://example.com/a");

const incompleteChromeExecutable = path.join(
  process.cwd(),
  ".cache",
  "puppeteer",
  "chrome",
  "linux-test-build",
  "chrome-linux64",
  "chrome"
);
assert.equal(
  __test.resolveIncompleteBrowserInstallDir({
    default: { executablePath: () => incompleteChromeExecutable }
  }),
  path.dirname(path.dirname(incompleteChromeExecutable))
);
assert.equal(
  __test.resolveIncompleteBrowserInstallDir({
    default: { executablePath: () => "/tmp/unrelated/chrome-linux64/chrome" }
  }),
  ""
);

const pages = new Map([
  [
    "https://example.com/",
    `<!doctype html><html><head>
      <title>Example Directory</title>
      <meta name="description" content="Verified directory">
    </head><body>
      <h1>Directory</h1>
      <article class="card"><a class="name" href="/alpha">Alpha GmbH</a><span class="city">Berlin</span></article>
      <article class="card"><a class="name" href="/beta">Beta GmbH</a><span class="city">Hamburg</span></article>
      <a class="next" href="/page/2">Next</a>
      <a href="https://other.example/out">External</a>
    </body></html>`
  ],
  [
    "https://example.com/page/2",
    `<!doctype html><html><head><title>Example Directory Page 2</title></head><body>
      <h1>Directory Page 2</h1>
      <article class="card"><a class="name" href="/beta">Beta GmbH</a><span class="city">Hamburg</span></article>
      <article class="card"><a class="name" href="/gamma">Gamma GmbH</a><span class="city">Koeln</span></article>
    </body></html>`
  ],
  [
    "https://example.com/alpha",
    "<html><head><title>Alpha</title></head><body><h1>Alpha GmbH</h1><p>Public profile</p></body></html>"
  ]
]);

const fakeFetcher = {
  async fetch(url) {
    const normalized = __test.canonicalizeUrl(url);
    const html = pages.get(normalized);
    if (!html) throw new Error(`fixture missing: ${normalized}`);
    return {
      requested_url: normalized,
      final_url: normalized,
      status: 200,
      content_type: "text/html",
      html,
      fetched_at: "2026-07-21T12:00:00.000Z",
      render_mode: "http"
    };
  },
  async close() {}
};

const robotsLoader = async (url) => ({
  parser: null,
  robots_url: `${new URL(url).origin}/robots.txt`,
  status: "not_present",
  fetched_at: "2026-07-21T12:00:00.000Z"
});

await assert.rejects(
  runWebResearchExtract(
    {
      agent_id: "vip-ai-research",
      purpose: "research",
      url: "https://example.com/",
      render_mode: "http"
    },
    {
      fetcher: fakeFetcher,
      robotsLoader: async (url) => ({
        parser: { isAllowed: () => false },
        robots_url: `${new URL(url).origin}/robots.txt`,
        status: "loaded"
      })
    }
  ),
  /robots\.txt untersagt/
);

const extracted = await runWebResearchExtract(
  {
    agent_id: "vip-ai-marketing",
    purpose: "marketing_audit",
    url: "https://example.com/",
    render_mode: "http",
    include: ["seo", "headings", "links", "text"],
    fields: [{ name: "company_names", selector: ".card .name", attribute: "text", multiple: true }]
  },
  { fetcher: fakeFetcher, robotsLoader }
);
assert.equal(extracted.extraction.seo.title, "Example Directory");
assert.deepEqual(extracted.extraction.fields.company_names, ["Alpha GmbH", "Beta GmbH"]);
assert.equal(extracted.policy.robots_txt_respected, true);
assert.match(extracted.extraction.evidence.html_sha256, /^[a-f0-9]{64}$/);

const listings = await runWebResearchListings(
  {
    agent_id: "vip-ai-sales",
    purpose: "sales_leads",
    start_url: "https://example.com/",
    render_mode: "http",
    item_selector: ".card",
    fields: [
      { name: "name", selector: ".name", attribute: "text", required: true },
      { name: "url", selector: ".name", attribute: "href", required: true },
      { name: "city", selector: ".city", attribute: "text" }
    ],
    next_page_selector: ".next",
    dedupe_by: "url",
    max_pages: 3,
    max_items: 10,
    delay_ms: 250
  },
  { fetcher: fakeFetcher, robotsLoader }
);
assert.equal(listings.summary.pages_fetched, 2);
assert.equal(listings.summary.records, 3);
assert.deepEqual(
  listings.records.map((record) => record.name),
  ["Alpha GmbH", "Beta GmbH", "Gamma GmbH"]
);
assert.equal(listings.records[0].url, "https://example.com/alpha");

const crawl = await runWebResearchCrawl(
  {
    agent_id: "vip-ai-research",
    purpose: "research",
    start_url: "https://example.com/",
    render_mode: "http",
    include_patterns: ["/", "/alpha"],
    exclude_patterns: [],
    max_pages: 2,
    max_depth: 1,
    delay_ms: 250,
    include: ["seo", "headings", "text"]
  },
  { fetcher: fakeFetcher, robotsLoader }
);
assert.equal(crawl.summary.pages_fetched, 2);
assert.deepEqual(
  crawl.pages.map((page) => page.url),
  ["https://example.com/", "https://example.com/alpha"]
);
assert.equal(crawl.pages.some((page) => page.url.startsWith("https://other.example")), false);

const baseline = await runWebResearchMonitor(
  {
    agent_id: "vip-ai-monitoring",
    purpose: "monitoring",
    url: "https://example.com/",
    render_mode: "http",
    fields: [{ name: "title", selector: "h1", attribute: "text" }]
  },
  { fetcher: fakeFetcher, robotsLoader }
);
assert.equal(baseline.comparison.baseline_created, true);

const unchanged = await runWebResearchMonitor(
  {
    agent_id: "vip-ai-monitoring",
    purpose: "monitoring",
    url: "https://example.com/",
    render_mode: "http",
    fields: [{ name: "title", selector: "h1", attribute: "text" }],
    previous_snapshot: baseline.snapshot
  },
  { fetcher: fakeFetcher, robotsLoader }
);
assert.equal(unchanged.comparison.changed, false);

const changedFetcher = {
  ...fakeFetcher,
  async fetch(url) {
    const page = await fakeFetcher.fetch(url);
    return { ...page, html: page.html.replace("<h1>Directory</h1>", "<h1>Updated Directory</h1>") };
  }
};
const changed = await runWebResearchMonitor(
  {
    agent_id: "vip-ai-monitoring",
    purpose: "monitoring",
    url: "https://example.com/",
    render_mode: "http",
    fields: [{ name: "title", selector: "h1", attribute: "text" }],
    previous_snapshot: baseline.snapshot
  },
  { fetcher: changedFetcher, robotsLoader }
);
assert.equal(changed.comparison.changed, true);
assert.equal(changed.comparison.field_changes[0].field, "title");

const config = getWebResearchConfig();
assert.equal(config.safety.public_http_https_only, true);
assert.equal(config.safety.authenticated_sessions_supported, false);
assert.equal(config.safety.non_get_browser_requests_blocked, true);
assert.equal(config.safety.browser_downloads_blocked, true);
assert.equal(config.browser_runtime_recovery_enabled, true);
assert.equal(config.browser_executable_resolution, "verified_explicit_path_v2");
assert.equal(
  config.browser_runtime_provider,
  process.platform === "linux" ? "sparticuz_chromium_138" : "puppeteer_managed"
);
assert.equal(config.mode, "synchronous_stateless_read_only");

const serverSource = await readFile(new URL("../server.js", import.meta.url), "utf8");
for (const toolName of [
  "web_research_check_config",
  "web_research_extract",
  "web_research_crawl",
  "web_research_listings",
  "web_research_monitor"
]) {
  assert.match(serverSource, new RegExp(`"${toolName}"`));
}

const packageJson = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
assert.equal(packageJson.scripts["test:web-research"], "node scripts/web-research-selftest.mjs");
assert.equal(packageJson.scripts.postinstall, "node scripts/install-browser-runtime.mjs");
assert.equal(packageJson.dependencies["@sparticuz/chromium"], "138.0.2");

const browserInstaller = await readFile(new URL("./install-browser-runtime.mjs", import.meta.url), "utf8");
assert.match(browserInstaller, /process\.platform === "linux"/);
assert.match(browserInstaller, /@sparticuz\/chromium/);

const puppeteerConfig = await readFile(new URL("../.puppeteerrc.cjs", import.meta.url), "utf8");
assert.match(puppeteerConfig, /\.cache.*puppeteer/);

console.log("web research self-test passed");
