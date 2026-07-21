import axios from "axios";
import * as cheerio from "cheerio";
import { createHash } from "node:crypto";
import { lookup as dnsLookupCallback } from "node:dns";
import { lookup as dnsLookup } from "node:dns/promises";
import http from "node:http";
import https from "node:https";
import net from "node:net";
import robotsParser from "robots-parser";

export const WEB_RESEARCH_PURPOSE_VALUES = [
  "sales_leads",
  "marketing_audit",
  "content_research",
  "business_development",
  "research",
  "monitoring",
  "finance_official_sources",
  "support_research",
  "general"
];

export const WEB_RESEARCH_RENDER_MODE_VALUES = ["http", "browser"];
export const WEB_RESEARCH_INCLUDE_VALUES = ["seo", "headings", "links", "images", "text", "html"];

const MAX_PAGES = Math.min(Math.max(Number(process.env.WEB_RESEARCH_MAX_PAGES || 20), 1), 50);
const MAX_BROWSER_PAGES = Math.min(
  Math.max(Number(process.env.WEB_RESEARCH_MAX_BROWSER_PAGES || 5), 1),
  10
);
const MAX_DEPTH = Math.min(Math.max(Number(process.env.WEB_RESEARCH_MAX_DEPTH || 2), 0), 4);
const MAX_ITEMS = Math.min(Math.max(Number(process.env.WEB_RESEARCH_MAX_ITEMS || 500), 1), 1000);
const MAX_RESPONSE_BYTES = Math.min(
  Math.max(Number(process.env.WEB_RESEARCH_MAX_RESPONSE_BYTES || 3_000_000), 100_000),
  10_000_000
);
const MAX_HTML_OUTPUT_CHARS = Math.min(
  Math.max(Number(process.env.WEB_RESEARCH_MAX_HTML_OUTPUT_CHARS || 50_000), 1_000),
  200_000
);
const MAX_TEXT_OUTPUT_CHARS = Math.min(
  Math.max(Number(process.env.WEB_RESEARCH_MAX_TEXT_OUTPUT_CHARS || 15_000), 1_000),
  50_000
);
const DEFAULT_DELAY_MS = Math.min(
  Math.max(Number(process.env.WEB_RESEARCH_DEFAULT_DELAY_MS || 500), 250),
  5_000
);
const DEFAULT_TIMEOUT_MS = Math.min(
  Math.max(Number(process.env.WEB_RESEARCH_TIMEOUT_MS || 25_000), 2_000),
  60_000
);
const ROBOTS_CACHE_TTL_MS = Math.min(
  Math.max(Number(process.env.WEB_RESEARCH_ROBOTS_CACHE_TTL_MS || 15 * 60 * 1000), 60_000),
  24 * 60 * 60 * 1000
);
const BROWSER_ENABLED = String(process.env.WEB_RESEARCH_BROWSER_ENABLED || "true").toLowerCase() !== "false";
const MAX_BROWSER_SESSIONS = Math.min(
  Math.max(Number(process.env.WEB_RESEARCH_MAX_BROWSER_SESSIONS || 1), 1),
  3
);
const MAX_BROWSER_REQUESTS_PER_PAGE = Math.min(
  Math.max(Number(process.env.WEB_RESEARCH_MAX_BROWSER_REQUESTS_PER_PAGE || 200), 20),
  1_000
);
const USER_AGENT =
  process.env.WEB_RESEARCH_USER_AGENT ||
  "VIP-Web-Research/1.0 (+https://vip-studios.de; public read-only research)";
const ROBOTS_USER_AGENT = "VIP-Web-Research";
const DEFAULT_INCLUDE = ["seo", "headings", "text"];
const robotsCache = new Map();
let activeBrowserSessions = 0;

const BINARY_PATH_PATTERN =
  /\.(?:7z|avi|bin|bmp|dmg|docx?|exe|gif|gz|ico|iso|jpe?g|mov|mp3|mp4|mpeg|pdf|png|pptx?|rar|svg|tar|tiff?|webm|webp|xlsx?|zip)(?:$|\?)/i;

function sha256(value) {
  return createHash("sha256").update(String(value || ""), "utf8").digest("hex");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeWhitespace(value) {
  return String(value || "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function truncate(value, maxChars) {
  const text = String(value || "");
  return text.length <= maxChars ? text : `${text.slice(0, maxChars)}…`;
}

function parseIpv4(address) {
  const parts = String(address || "").split(".").map(Number);
  if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) {
    return null;
  }
  return parts;
}

export function isPrivateOrReservedIp(address) {
  const normalized = String(address || "").toLowerCase().split("%")[0];
  if (normalized.startsWith("::ffff:")) return isPrivateOrReservedIp(normalized.slice(7));
  const ipVersion = net.isIP(normalized);
  if (ipVersion === 4) {
    const [a, b] = parseIpv4(normalized) || [];
    return (
      a === 0 ||
      a === 10 ||
      a === 127 ||
      (a === 100 && b >= 64 && b <= 127) ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 0) ||
      (a === 192 && b === 168) ||
      (a === 198 && (b === 18 || b === 19)) ||
      (a === 198 && b === 51) ||
      (a === 203 && b === 0) ||
      a >= 224
    );
  }
  if (ipVersion === 6) {
    return (
      normalized === "::" ||
      normalized === "::1" ||
      normalized.startsWith("fc") ||
      normalized.startsWith("fd") ||
      /^fe[89ab]/.test(normalized) ||
      normalized.startsWith("2001:db8")
    );
  }
  return true;
}

export function normalizePublicUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) throw new Error("URL darf nicht leer sein.");
  const withProtocol = /^https?:\/\//i.test(raw) ? raw : raw.startsWith("www.") ? `https://${raw}` : raw;
  let parsed;
  try {
    parsed = new URL(withProtocol);
  } catch {
    throw new Error(`Ungueltige URL: ${raw}`);
  }
  if (!["http:", "https:"].includes(parsed.protocol)) {
    throw new Error("Nur oeffentliche http(s)-URLs sind erlaubt.");
  }
  if (parsed.username || parsed.password) {
    throw new Error("URLs mit eingebetteten Zugangsdaten sind nicht erlaubt.");
  }
  const hostname = parsed.hostname.toLowerCase();
  if (
    hostname === "localhost" ||
    hostname.endsWith(".localhost") ||
    hostname.endsWith(".local") ||
    hostname === "metadata.google.internal" ||
    hostname.endsWith(".internal")
  ) {
    throw new Error("Lokale oder interne Hostnamen sind nicht erlaubt.");
  }
  if (net.isIP(hostname) && isPrivateOrReservedIp(hostname)) {
    throw new Error("Private oder reservierte IP-Adressen sind nicht erlaubt.");
  }
  parsed.hash = "";
  return parsed.href;
}

async function assertPublicDns(url) {
  const parsed = new URL(normalizePublicUrl(url));
  if (net.isIP(parsed.hostname)) return parsed.href;
  const addresses = await dnsLookup(parsed.hostname, { all: true, verbatim: true });
  if (!addresses.length || addresses.some(({ address }) => isPrivateOrReservedIp(address))) {
    throw new Error(`Hostname ${parsed.hostname} loest nicht ausschliesslich auf oeffentliche IP-Adressen auf.`);
  }
  return parsed.href;
}

function safeLookup(hostname, options, callback) {
  const normalizedOptions = typeof options === "object" && options ? options : {};
  dnsLookupCallback(hostname, { all: true, verbatim: true }, (error, addresses) => {
    if (error) return callback(error);
    const resolved = Array.isArray(addresses) ? addresses : [];
    if (!resolved.length || resolved.some(({ address }) => isPrivateOrReservedIp(address))) {
      return callback(new Error(`DNS-Aufloesung fuer ${hostname} enthaelt private oder reservierte Ziele.`));
    }
    let allowed = resolved;
    if (normalizedOptions.family === 4 || normalizedOptions.family === 6) {
      allowed = allowed.filter(({ family }) => family === normalizedOptions.family);
    }
    if (!allowed.length) return callback(new Error(`Keine passende oeffentliche DNS-Adresse fuer ${hostname}.`));
    if (normalizedOptions.all) return callback(null, allowed);
    return callback(null, allowed[0].address, allowed[0].family);
  });
}

const httpAgent = new http.Agent({ keepAlive: true, lookup: safeLookup });
const httpsAgent = new https.Agent({ keepAlive: true, lookup: safeLookup });

function canonicalizeUrl(value) {
  const parsed = new URL(normalizePublicUrl(value));
  for (const key of [...parsed.searchParams.keys()]) {
    if (/^(?:utm_.+|fbclid|gclid|mc_cid|mc_eid)$/i.test(key)) parsed.searchParams.delete(key);
  }
  parsed.hash = "";
  return parsed.href;
}

function resolveUrl(value, baseUrl) {
  try {
    return canonicalizeUrl(new URL(String(value || ""), baseUrl).href);
  } catch {
    return "";
  }
}

async function fetchHttpPage(url, { timeoutMs }) {
  const normalizedUrl = await assertPublicDns(url);
  const response = await axios.get(normalizedUrl, {
    responseType: "text",
    transformResponse: [(data) => data],
    timeout: timeoutMs,
    maxRedirects: 5,
    maxContentLength: MAX_RESPONSE_BYTES,
    maxBodyLength: MAX_RESPONSE_BYTES,
    validateStatus: () => true,
    proxy: false,
    httpAgent,
    httpsAgent,
    headers: {
      "User-Agent": USER_AGENT,
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.2"
    },
    beforeRedirect: (options) => {
      normalizePublicUrl(`${options.protocol}//${options.hostname}${options.path || "/"}`);
    }
  });
  const body = typeof response.data === "string" ? response.data : String(response.data || "");
  const finalUrl = canonicalizeUrl(response.request?.res?.responseUrl || normalizedUrl);
  await assertPublicDns(finalUrl);
  const contentType = String(response.headers["content-type"] || "");
  if (body && !/html|xhtml|xml|text|json/i.test(contentType)) {
    throw new Error(`Nicht unterstuetzter Content-Type: ${contentType || "unbekannt"}.`);
  }
  if (Buffer.byteLength(body, "utf8") > MAX_RESPONSE_BYTES) {
    throw new Error(`Antwort ueberschreitet das Limit von ${MAX_RESPONSE_BYTES} Bytes.`);
  }
  return {
    requested_url: normalizedUrl,
    final_url: finalUrl,
    status: response.status,
    content_type: contentType,
    html: body,
    fetched_at: new Date().toISOString(),
    render_mode: "http"
  };
}

async function createBrowserFetcher({ timeoutMs }) {
  if (!BROWSER_ENABLED) throw new Error("Browser-Modus ist per WEB_RESEARCH_BROWSER_ENABLED deaktiviert.");
  if (activeBrowserSessions >= MAX_BROWSER_SESSIONS) {
    throw new Error("Browser-Kapazitaet ist belegt; bitte den read-only Abruf spaeter erneut versuchen.");
  }
  activeBrowserSessions += 1;
  let browser;
  try {
    const puppeteer = await import("puppeteer");
    browser = await puppeteer.default.launch({
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--no-proxy-server"
      ]
    });
  } catch (error) {
    activeBrowserSessions -= 1;
    throw error;
  }
  let closed = false;
  return {
    async fetch(url) {
      const normalizedUrl = await assertPublicDns(url);
      const page = await browser.newPage();
      await page.setBypassServiceWorker(true);
      await page.setUserAgent(USER_AGENT);
      const cdp = await page.createCDPSession();
      await cdp.send("Page.setDownloadBehavior", { behavior: "deny" });
      await page.setRequestInterception(true);
      let requestCount = 0;
      page.on("request", async (request) => {
        const requestUrl = request.url();
        requestCount += 1;
        if (requestCount > MAX_BROWSER_REQUESTS_PER_PAGE) return request.abort("blockedbyclient");
        if (!["GET", "HEAD"].includes(request.method().toUpperCase())) return request.abort("blockedbyclient");
        if (["websocket", "media", "font"].includes(request.resourceType())) return request.abort("blockedbyclient");
        if (/^(?:data|blob|about):/i.test(requestUrl)) return request.continue();
        if (!/^https?:/i.test(requestUrl)) return request.abort("blockedbyclient");
        try {
          await assertPublicDns(requestUrl);
          return request.continue();
        } catch {
          return request.abort("blockedbyclient");
        }
      });
      try {
        const response = await page.goto(normalizedUrl, { waitUntil: "domcontentloaded", timeout: timeoutMs });
        const finalUrl = canonicalizeUrl(page.url());
        await assertPublicDns(finalUrl);
        const body = await page.content();
        if (Buffer.byteLength(body, "utf8") > MAX_RESPONSE_BYTES) {
          throw new Error(`Gerenderte Seite ueberschreitet das Limit von ${MAX_RESPONSE_BYTES} Bytes.`);
        }
        return {
          requested_url: normalizedUrl,
          final_url: finalUrl,
          status: response?.status() || 0,
          content_type: response?.headers()?.["content-type"] || "text/html",
          html: body,
          fetched_at: new Date().toISOString(),
          render_mode: "browser"
        };
      } finally {
        await page.close();
      }
    },
    async close() {
      if (closed) return;
      closed = true;
      try {
        await browser.close();
      } finally {
        activeBrowserSessions = Math.max(0, activeBrowserSessions - 1);
      }
    }
  };
}

async function createPageFetcher({ renderMode, timeoutMs }) {
  if (renderMode === "browser") return createBrowserFetcher({ timeoutMs });
  return {
    fetch: (url) => fetchHttpPage(url, { timeoutMs }),
    close: async () => {}
  };
}

async function loadRobotsPolicy(url) {
  const parsed = new URL(normalizePublicUrl(url));
  const cacheKey = parsed.origin;
  const cached = robotsCache.get(cacheKey);
  if (cached && cached.expires_at > Date.now()) return cached;
  const robotsUrl = `${parsed.origin}/robots.txt`;
  let entry = {
    parser: null,
    robots_url: robotsUrl,
    status: "unavailable",
    fetched_at: new Date().toISOString(),
    expires_at: Date.now() + ROBOTS_CACHE_TTL_MS
  };
  try {
    const response = await axios.get(await assertPublicDns(robotsUrl), {
      responseType: "text",
      transformResponse: [(data) => data],
      timeout: Math.min(DEFAULT_TIMEOUT_MS, 10_000),
      maxRedirects: 3,
      maxContentLength: 250_000,
      validateStatus: () => true,
      httpAgent,
      httpsAgent,
      headers: { "User-Agent": USER_AGENT, Accept: "text/plain,*/*;q=0.1" }
    });
    if (response.status >= 200 && response.status < 300) {
      const body = typeof response.data === "string" ? response.data : String(response.data || "");
      entry = {
        ...entry,
        parser: robotsParser(robotsUrl, body),
        status: "loaded",
        http_status: response.status,
        sha256: sha256(body)
      };
    } else {
      entry = { ...entry, status: "not_present", http_status: response.status };
    }
  } catch (error) {
    entry = { ...entry, error: error.message };
  }
  robotsCache.set(cacheKey, entry);
  return entry;
}

async function assertRobotsAllowed(url, robotsLoader = loadRobotsPolicy) {
  const policy = await robotsLoader(url);
  const allowed = policy.parser ? policy.parser.isAllowed(url, ROBOTS_USER_AGENT) !== false : true;
  if (!allowed) throw new Error(`robots.txt untersagt den Abruf fuer ${url}.`);
  return {
    robots_url: policy.robots_url,
    robots_status: policy.status,
    robots_sha256: policy.sha256 || null,
    allowed
  };
}

function extractValue($, root, field, baseUrl) {
  let selection;
  if (field.selector) selection = root.find(field.selector);
  else selection = root;
  if (!field.multiple) selection = selection.first();
  const values = [];
  selection.each((_, element) => {
    const node = $(element);
    let value;
    const attribute = String(field.attribute || "text").toLowerCase();
    if (attribute === "text") value = normalizeWhitespace(node.text());
    else if (attribute === "html") value = normalizeWhitespace(node.html() || "");
    else value = normalizeWhitespace(node.attr(attribute) || "");
    if (["href", "src", "action"].includes(attribute) && value) value = resolveUrl(value, baseUrl);
    if (value) values.push(truncate(value, field.max_chars || 2_000));
    if (values.length >= (field.max_values || 100)) return false;
    return undefined;
  });
  return field.multiple ? values : values[0] || "";
}

function extractCustomFields($, root, fields, baseUrl) {
  const output = {};
  const missingRequired = [];
  for (const field of fields || []) {
    const value = extractValue($, root, field, baseUrl);
    output[field.name] = value;
    if (field.required && (Array.isArray(value) ? !value.length : !value)) missingRequired.push(field.name);
  }
  return { fields: output, missing_required_fields: missingRequired };
}

function extractPageDocument(page, options = {}) {
  const $ = cheerio.load(page.html || "");
  $("script,style,noscript,template").remove();
  const include = new Set(options.include?.length ? options.include : DEFAULT_INCLUDE);
  const maxLinks = Math.min(Math.max(Number(options.max_links || 100), 0), 500);
  const maxImages = Math.min(Math.max(Number(options.max_images || 100), 0), 500);
  const maxTextChars = Math.min(Math.max(Number(options.text_max_chars || 5_000), 0), MAX_TEXT_OUTPUT_CHARS);
  const maxHtmlChars = Math.min(Math.max(Number(options.html_max_chars || 20_000), 0), MAX_HTML_OUTPUT_CHARS);
  const bodyText = normalizeWhitespace($("body").text() || $.root().text());
  const result = {
    requested_url: page.requested_url,
    final_url: page.final_url,
    status: page.status,
    ok: page.status >= 200 && page.status < 400,
    content_type: page.content_type,
    fetched_at: page.fetched_at,
    render_mode: page.render_mode,
    evidence: {
      source_url: page.final_url,
      fetched_at: page.fetched_at,
      html_sha256: sha256(page.html),
      normalized_text_sha256: sha256(bodyText),
      body_bytes: Buffer.byteLength(page.html || "", "utf8")
    }
  };
  if (include.has("seo")) {
    result.seo = {
      title: normalizeWhitespace($("title").first().text()),
      meta_description: normalizeWhitespace($("meta[name='description']").attr("content") || ""),
      canonical: resolveUrl($("link[rel~='canonical']").first().attr("href") || "", page.final_url),
      robots: normalizeWhitespace($("meta[name='robots']").attr("content") || ""),
      og_title: normalizeWhitespace($("meta[property='og:title']").attr("content") || ""),
      og_description: normalizeWhitespace($("meta[property='og:description']").attr("content") || ""),
      og_image: resolveUrl($("meta[property='og:image']").attr("content") || "", page.final_url)
    };
  }
  if (include.has("headings")) {
    result.headings = $("h1,h2,h3,h4,h5,h6")
      .slice(0, 100)
      .map((_, element) => ({
        level: Number(element.tagName.slice(1)),
        text: truncate(normalizeWhitespace($(element).text()), 1_000)
      }))
      .get()
      .filter((heading) => heading.text);
  }
  if (include.has("links")) {
    result.links = $("a[href]")
      .slice(0, maxLinks)
      .map((_, element) => ({
        text: truncate(normalizeWhitespace($(element).text()), 500),
        href: resolveUrl($(element).attr("href"), page.final_url),
        rel: normalizeWhitespace($(element).attr("rel") || "")
      }))
      .get()
      .filter((link) => link.href);
  }
  if (include.has("images")) {
    result.images = $("img")
      .slice(0, maxImages)
      .map((_, element) => ({
        src: resolveUrl($(element).attr("src") || $(element).attr("data-src") || "", page.final_url),
        alt: truncate(normalizeWhitespace($(element).attr("alt") || ""), 1_000),
        title: truncate(normalizeWhitespace($(element).attr("title") || ""), 1_000)
      }))
      .get()
      .filter((image) => image.src || image.alt);
  }
  if (include.has("text")) {
    result.text_excerpt = truncate(bodyText, maxTextChars);
    result.word_count_estimate = bodyText ? bodyText.split(/\s+/).length : 0;
  }
  if (include.has("html")) {
    result.html_source = truncate(page.html, maxHtmlChars);
    result.html_source_truncated = String(page.html || "").length > maxHtmlChars;
  }
  const custom = extractCustomFields($, $.root(), options.fields || [], page.final_url);
  if ((options.fields || []).length) {
    result.fields = custom.fields;
    result.missing_required_fields = custom.missing_required_fields;
  }
  return result;
}

function policyReceipt({ purpose, renderMode, robots }) {
  return {
    purpose,
    public_web_only: true,
    login_or_cookie_automation: false,
    write_actions: false,
    robots_txt_respected: true,
    render_mode: renderMode,
    robots
  };
}

async function withFetcher(options, dependencies, callback) {
  const renderMode = options.render_mode || "http";
  const timeoutMs = Math.min(Math.max(Number(options.timeout_ms || DEFAULT_TIMEOUT_MS), 2_000), 60_000);
  const fetcher = dependencies?.fetcher || (await createPageFetcher({ renderMode, timeoutMs }));
  try {
    return await callback({
      fetchPage: fetcher.fetch.bind(fetcher),
      robotsLoader: dependencies?.robotsLoader || loadRobotsPolicy,
      renderMode
    });
  } finally {
    if (!dependencies?.fetcher) await fetcher.close();
  }
}

export async function runWebResearchExtract(options, dependencies = {}) {
  return withFetcher(options, dependencies, async ({ fetchPage, robotsLoader, renderMode }) => {
    const url = canonicalizeUrl(options.url);
    const robots = await assertRobotsAllowed(url, robotsLoader);
    const page = await fetchPage(url);
    const extraction = extractPageDocument(page, options);
    return {
      agent_id: options.agent_id,
      purpose: options.purpose,
      policy: policyReceipt({ purpose: options.purpose, renderMode, robots }),
      extraction
    };
  });
}

function globToRegExp(pattern) {
  const escaped = String(pattern || "")
    .replace(/[.+^${}()|[\]\\]/g, "\\$&")
    .replace(/\*/g, ".*")
    .replace(/\?/g, ".");
  return new RegExp(`^${escaped}$`, "i");
}

function matchesPatterns(url, includePatterns = [], excludePatterns = []) {
  const candidate = `${url.pathname}${url.search}`;
  if (excludePatterns.some((pattern) => globToRegExp(pattern).test(candidate))) return false;
  return !includePatterns.length || includePatterns.some((pattern) => globToRegExp(pattern).test(candidate));
}

function discoverSameOriginLinks(page, origin, options) {
  const $ = cheerio.load(page.html || "");
  const links = [];
  const seen = new Set();
  $("a[href]").each((_, element) => {
    if (links.length >= 500) return false;
    const href = resolveUrl($(element).attr("href"), page.final_url);
    if (!href || BINARY_PATH_PATTERN.test(href)) return undefined;
    let parsed;
    try {
      parsed = new URL(href);
    } catch {
      return undefined;
    }
    if (parsed.origin !== origin || !matchesPatterns(parsed, options.include_patterns, options.exclude_patterns)) {
      return undefined;
    }
    if (!seen.has(href)) {
      seen.add(href);
      links.push(href);
    }
    return undefined;
  });
  return links;
}

export async function runWebResearchCrawl(options, dependencies = {}) {
  const requestedMaxPages = Math.min(Math.max(Number(options.max_pages || 10), 1), MAX_PAGES);
  const maxPages = options.render_mode === "browser" ? Math.min(requestedMaxPages, MAX_BROWSER_PAGES) : requestedMaxPages;
  const maxDepth = Math.min(Math.max(Number(options.max_depth || 1), 0), MAX_DEPTH);
  const delayMs = Math.min(Math.max(Number(options.delay_ms || DEFAULT_DELAY_MS), 250), 5_000);
  return withFetcher(options, dependencies, async ({ fetchPage, robotsLoader, renderMode }) => {
    const startUrl = canonicalizeUrl(options.start_url);
    const origin = new URL(startUrl).origin;
    const queue = [{ url: startUrl, depth: 0, discovered_from: null }];
    const queued = new Set([startUrl]);
    const pages = [];
    const errors = [];
    while (queue.length && pages.length < maxPages) {
      const current = queue.shift();
      try {
        const robots = await assertRobotsAllowed(current.url, robotsLoader);
        const page = await fetchPage(current.url);
        const extraction = extractPageDocument(page, {
          include: options.include || ["seo", "headings", "text"],
          text_max_chars: options.text_max_chars || 2_000,
          max_links: 0,
          max_images: 0
        });
        const discovered = current.depth < maxDepth ? discoverSameOriginLinks(page, origin, options) : [];
        pages.push({
          url: page.final_url,
          depth: current.depth,
          discovered_from: current.discovered_from,
          robots,
          ...extraction
        });
        for (const link of discovered) {
          if (queued.size >= maxPages * 20) break;
          if (!queued.has(link)) {
            queued.add(link);
            queue.push({ url: link, depth: current.depth + 1, discovered_from: page.final_url });
          }
        }
      } catch (error) {
        errors.push({ url: current.url, depth: current.depth, error: error.message });
      }
      if (queue.length && pages.length < maxPages) await sleep(delayMs);
    }
    return {
      agent_id: options.agent_id,
      purpose: options.purpose,
      policy: policyReceipt({ purpose: options.purpose, renderMode, robots: "checked_per_page" }),
      start_url: startUrl,
      same_origin_only: true,
      limits: { max_pages: maxPages, max_depth: maxDepth, delay_ms: delayMs },
      summary: {
        pages_fetched: pages.length,
        errors: errors.length,
        urls_discovered: queued.size,
        queue_remaining: queue.length
      },
      pages,
      errors
    };
  });
}

export async function runWebResearchListings(options, dependencies = {}) {
  const requestedMaxPages = Math.min(Math.max(Number(options.max_pages || 3), 1), MAX_PAGES);
  const maxPages = options.render_mode === "browser" ? Math.min(requestedMaxPages, MAX_BROWSER_PAGES) : requestedMaxPages;
  const maxItems = Math.min(Math.max(Number(options.max_items || 100), 1), MAX_ITEMS);
  const delayMs = Math.min(Math.max(Number(options.delay_ms || DEFAULT_DELAY_MS), 250), 5_000);
  return withFetcher(options, dependencies, async ({ fetchPage, robotsLoader, renderMode }) => {
    let nextUrl = canonicalizeUrl(options.start_url);
    const origin = new URL(nextUrl).origin;
    const visited = new Set();
    const records = [];
    const dedupe = new Set();
    const pages = [];
    const errors = [];
    while (nextUrl && visited.size < maxPages && records.length < maxItems) {
      if (visited.has(nextUrl)) break;
      visited.add(nextUrl);
      try {
        const robots = await assertRobotsAllowed(nextUrl, robotsLoader);
        const page = await fetchPage(nextUrl);
        const $ = cheerio.load(page.html || "");
        const items = $(options.item_selector).slice(0, maxItems - records.length);
        items.each((index, element) => {
          const extracted = extractCustomFields($, $(element), options.fields || [], page.final_url);
          const record = {
            ...extracted.fields,
            _source_url: page.final_url,
            _source_item_index: index,
            _missing_required_fields: extracted.missing_required_fields
          };
          const dedupeValue = options.dedupe_by ? record[options.dedupe_by] : sha256(JSON.stringify(record));
          const dedupeKey = Array.isArray(dedupeValue) ? dedupeValue.join("|") : String(dedupeValue || "");
          if (!dedupeKey || dedupe.has(dedupeKey)) return;
          dedupe.add(dedupeKey);
          records.push(record);
        });
        pages.push({
          url: page.final_url,
          status: page.status,
          fetched_at: page.fetched_at,
          html_sha256: sha256(page.html),
          robots,
          items_found: items.length
        });
        const nextHref = options.next_page_selector
          ? $(options.next_page_selector).first().attr("href") || ""
          : "";
        const candidate = nextHref ? resolveUrl(nextHref, page.final_url) : "";
        nextUrl = candidate && new URL(candidate).origin === origin && !visited.has(candidate) ? candidate : "";
      } catch (error) {
        errors.push({ url: nextUrl, error: error.message });
        nextUrl = "";
      }
      if (nextUrl && records.length < maxItems) await sleep(delayMs);
    }
    return {
      agent_id: options.agent_id,
      purpose: options.purpose,
      policy: policyReceipt({ purpose: options.purpose, renderMode, robots: "checked_per_page" }),
      start_url: canonicalizeUrl(options.start_url),
      same_origin_pagination_only: true,
      limits: { max_pages: maxPages, max_items: maxItems, delay_ms: delayMs },
      summary: {
        pages_fetched: pages.length,
        records: records.length,
        records_with_missing_required_fields: records.filter((record) => record._missing_required_fields.length).length,
        errors: errors.length
      },
      pages,
      records,
      errors
    };
  });
}

function normalizeSnapshotFields(fields = {}) {
  return Object.fromEntries(
    Object.entries(fields).map(([key, value]) => [key, Array.isArray(value) ? value.map(String) : [String(value || "")]])
  );
}

export async function runWebResearchMonitor(options, dependencies = {}) {
  const result = await runWebResearchExtract(
    {
      ...options,
      include: options.include || ["seo", "text"]
    },
    dependencies
  );
  const extraction = result.extraction;
  const currentFields = normalizeSnapshotFields(extraction.fields || {});
  const snapshot = {
    url: extraction.final_url,
    captured_at: extraction.fetched_at,
    html_sha256: extraction.evidence.html_sha256,
    normalized_text_sha256: extraction.evidence.normalized_text_sha256,
    fields: currentFields
  };
  const previous = options.previous_snapshot || null;
  const fieldChanges = [];
  if (previous) {
    const previousFields = normalizeSnapshotFields(previous.fields || {});
    for (const key of new Set([...Object.keys(previousFields), ...Object.keys(currentFields)])) {
      if (JSON.stringify(previousFields[key] || []) !== JSON.stringify(currentFields[key] || [])) {
        fieldChanges.push({ field: key, before: previousFields[key] || [], after: currentFields[key] || [] });
      }
    }
  }
  return {
    ...result,
    comparison: previous
      ? {
          changed:
            previous.normalized_text_sha256 !== snapshot.normalized_text_sha256 || fieldChanges.length > 0,
          html_changed: previous.html_sha256 !== snapshot.html_sha256,
          normalized_text_changed: previous.normalized_text_sha256 !== snapshot.normalized_text_sha256,
          field_changes: fieldChanges
        }
      : { changed: null, baseline_created: true, field_changes: [] },
    snapshot
  };
}

export function getWebResearchConfig() {
  return {
    service: "vip-web-research",
    mode: "synchronous_stateless_read_only",
    browser_enabled: BROWSER_ENABLED,
    user_agent: USER_AGENT,
    limits: {
      max_pages: MAX_PAGES,
      max_browser_pages: MAX_BROWSER_PAGES,
      max_browser_sessions: MAX_BROWSER_SESSIONS,
      max_browser_requests_per_page: MAX_BROWSER_REQUESTS_PER_PAGE,
      max_depth: MAX_DEPTH,
      max_items: MAX_ITEMS,
      max_response_bytes: MAX_RESPONSE_BYTES,
      max_html_output_chars: MAX_HTML_OUTPUT_CHARS,
      max_text_output_chars: MAX_TEXT_OUTPUT_CHARS,
      default_delay_ms: DEFAULT_DELAY_MS,
      default_timeout_ms: DEFAULT_TIMEOUT_MS
    },
    safety: {
      public_http_https_only: true,
      dns_private_range_block: true,
      robots_txt_mandatory: true,
      authenticated_sessions_supported: false,
      form_submissions_supported: false,
      non_get_browser_requests_blocked: true,
      browser_downloads_blocked: true,
      same_origin_crawl: true,
      same_origin_pagination: true,
      persistent_server_jobs: false
    },
    purposes: WEB_RESEARCH_PURPOSE_VALUES,
    render_modes: WEB_RESEARCH_RENDER_MODE_VALUES
  };
}

export const __test = {
  extractPageDocument,
  extractCustomFields,
  matchesPatterns,
  canonicalizeUrl,
  sha256
};
