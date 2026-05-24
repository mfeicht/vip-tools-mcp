import "dotenv/config";
import express from "express";
import axios from "axios";
import net from "net";
import tls from "tls";
import { createHash, createSign, randomUUID } from "crypto";
import { z } from "zod";

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

/* ---------------- CONFIG ---------------- */

const DEFAULT_AGENT_ID = "vip-ai-sales";

const ASANA_TOKEN_ENVS = {
  "vip-ai-sales": "ASANA_TOKEN_VIP_AI_SALES",
  "vip-ai-operations": "ASANA_TOKEN_VIP_AI_OPERATIONS",
  "vip-ai-content": "ASANA_TOKEN_VIP_AI_CONTENT",
  "vip-ai-support": "ASANA_TOKEN_VIP_AI_SUPPORT",
  "vip-ai-developer": "ASANA_TOKEN_VIP_AI_DEVELOPER",
  "vip-ai-marketing": "ASANA_TOKEN_VIP_AI_MARKETING",
  "vip-ai-office": "ASANA_TOKEN_VIP_AI_OFFICE",
  "vip-ai-design": "ASANA_TOKEN_VIP_AI_DESIGN",
  "vip-ai-social-media": "ASANA_TOKEN_VIP_AI_SOCIAL_MEDIA",
  "vip-ai-memory": "ASANA_TOKEN_VIP_AI_MEMORY",
  "vip-ai-review": "ASANA_TOKEN_VIP_AI_REVIEW",
  "vip-ai-strategy": "ASANA_TOKEN_VIP_AI_STRATEGY",
  "vip-ai-monitoring": "ASANA_TOKEN_VIP_AI_MONITORING",
  "vip-ai-communication": "ASANA_TOKEN_VIP_AI_COMMUNICATION",
  "vip-ai-research": "ASANA_TOKEN_VIP_AI_RESEARCH",
  "vip-ai-project-manager": "ASANA_TOKEN_VIP_AI_PROJECT_MANAGER",
  "vip-ai-business-developer": "ASANA_TOKEN_VIP_AI_BUSINESS_DEVELOPER",
  "vip-ai-finance": "ASANA_TOKEN_VIP_AI_FINANCE",
  "vip-ai-accounting": "ASANA_TOKEN_VIP_AI_ACCOUNTING",
  "rs-ai-sales": "ASANA_TOKEN_RS_AI_SALES",
  "rs-ai-content": "ASANA_TOKEN_RS_AI_CONTENT",
  "rs-ai-support": "ASANA_TOKEN_RS_AI_SUPPORT",
  "rs-ai-marketing": "ASANA_TOKEN_RS_AI_MARKETING",
  "rs-ai-office": "ASANA_TOKEN_RS_AI_OFFICE"
};

const agentIdSchema = z.enum(Object.keys(ASANA_TOKEN_ENVS)).optional().default(DEFAULT_AGENT_ID);
const sheetCellValueSchema = z.union([z.string(), z.number(), z.boolean(), z.null()]);
const GOOGLE_AGENT_FOLDER_ID =
  process.env.GOOGLE_DRIVE_AGENT_FOLDER_ID || "16BPopNsMWrI-FkvQtLkpYwb_cMFDpMRr";
const GOOGLE_ALLOWED_FOLDER_IDS = new Set(
  [GOOGLE_AGENT_FOLDER_ID, ...(process.env.GOOGLE_DRIVE_ALLOWED_FOLDER_IDS || "").split(",")]
    .map((value) => value.trim())
    .filter(Boolean)
);
const GOOGLE_SCOPES = [
  "https://www.googleapis.com/auth/drive",
  "https://www.googleapis.com/auth/spreadsheets"
];
const GOOGLE_SEO_OAUTH_PREFIXES = ["GOOGLE_SEO_OAUTH", "GOOGLE_ANALYTICS_SEARCH_OAUTH", "GOOGLE_OAUTH"];
const GOOGLE_SEO_REQUIRED_SCOPES = [
  "https://www.googleapis.com/auth/analytics.readonly",
  "https://www.googleapis.com/auth/webmasters.readonly"
];
const GOOGLE_ADS_OAUTH_PREFIXES = [
  "GOOGLE_ADS_OAUTH",
  "GOOGLE_SEO_OAUTH",
  "GOOGLE_ANALYTICS_SEARCH_OAUTH",
  "GOOGLE_OAUTH"
];
const GOOGLE_ADS_REQUIRED_SCOPES = ["https://www.googleapis.com/auth/adwords"];
const GOOGLE_ADS_API_BASE = (process.env.GOOGLE_ADS_API_BASE || "https://googleads.googleapis.com").replace(
  /\/$/,
  ""
);
const GOOGLE_ADS_API_VERSION = process.env.GOOGLE_ADS_API_VERSION || "v22";
const GOOGLE_ADS_TIMEOUT_MS = Number(process.env.GOOGLE_ADS_TIMEOUT_MS || 30_000);
const GOOGLE_ADS_MUTATE_SERVICE_VALUES = [
  "campaignBudgets",
  "campaigns",
  "adGroups",
  "adGroupAds",
  "campaignCriteria",
  "adGroupCriteria",
  "customerNegativeCriteria",
  "assets",
  "campaignAssets",
  "adGroupAssets"
];
const GOOGLE_ADS_RESPONSE_CONTENT_TYPE_VALUES = ["RESOURCE_NAME_ONLY", "MUTABLE_RESOURCE"];
const WP_IMPORT_URL =
  process.env.WORDPRESS_GK_IMPORT_URL || "https://app.goklever.de/wp-json/gk-mailpoet/v1/import-csv";
const RS_REDAKTIONSPLAN_SPREADSHEET_ID =
  process.env.RS_REDAKTIONSPLAN_SPREADSHEET_ID || "17BbIPRBGxcNUAYPNd6AD3wnvik6s26P8fnvPDeH1UrM";
const RS_REDAKTIONSPLAN_SHEET_NAME = process.env.RS_REDAKTIONSPLAN_SHEET_NAME || "Redaktionsplan";
const DEFAULT_EMAIL_ALLOWED_RECIPIENTS = "*";
const EMAIL_DRAFT_TTL_MS = Number(process.env.EMAIL_DRAFT_TTL_MS || 60 * 60 * 1000);
const SMTP_TIMEOUT_MS = Number(process.env.SMTP_TIMEOUT_MS || 15_000);
const EMAIL_HTTP_TIMEOUT_MS = Number(process.env.EMAIL_HTTP_TIMEOUT_MS || 20_000);
const IMAP_TIMEOUT_MS = Number(process.env.IMAP_TIMEOUT_MS || 15_000);
const DATAFORSEO_API_BASE = (process.env.DATAFORSEO_API_BASE || "https://api.dataforseo.com/v3").replace(/\/$/, "");
const DATAFORSEO_TIMEOUT_MS = Number(process.env.DATAFORSEO_TIMEOUT_MS || 30_000);
const DATAFORSEO_DEFAULT_LOCATION_CODE = Number(process.env.DATAFORSEO_DEFAULT_LOCATION_CODE || 2276);
const DATAFORSEO_DEFAULT_LANGUAGE_CODE = process.env.DATAFORSEO_DEFAULT_LANGUAGE_CODE || "de";
const DATAFORSEO_MAX_RESULTS = Math.min(Number(process.env.DATAFORSEO_MAX_RESULTS || 100), 1000);
const PEXELS_API_BASE = (process.env.PEXELS_API_BASE || "https://api.pexels.com").replace(/\/$/, "");
const PEXELS_TIMEOUT_MS = Number(process.env.PEXELS_TIMEOUT_MS || 20_000);
const TEMPLATED_API_BASE = (process.env.TEMPLATED_API_BASE || "https://api.templated.io").replace(/\/$/, "");
const TEMPLATED_TIMEOUT_MS = Number(process.env.TEMPLATED_TIMEOUT_MS || 30_000);
const FREEPIK_DEFAULT_API_BASE = (process.env.FREEPIK_API_BASE || "https://api.freepik.com").replace(/\/$/, "");
const MAGNIFIC_API_BASE = (process.env.MAGNIFIC_API_BASE || "https://api.magnific.com").replace(/\/$/, "");
const FREEPIK_TIMEOUT_MS = Number(process.env.FREEPIK_TIMEOUT_MS || 30_000);
const UNSPLASH_API_BASE = (process.env.UNSPLASH_API_BASE || "https://api.unsplash.com").replace(/\/$/, "");
const UNSPLASH_TIMEOUT_MS = Number(process.env.UNSPLASH_TIMEOUT_MS || 20_000);
const WEB_FETCH_TIMEOUT_MS = Number(process.env.WEB_FETCH_TIMEOUT_MS || 20_000);
const WEB_FETCH_MAX_BYTES = Number(process.env.WEB_FETCH_MAX_BYTES || 2_000_000);
const WEB_FETCH_USER_AGENT =
  process.env.WEB_FETCH_USER_AGENT ||
  "VIP-Tools-MCP/1.0 (+https://vip-studios.de; read-only metadata inspection)";
const PAGESPEED_API_BASE = (
  process.env.PAGESPEED_API_BASE || "https://www.googleapis.com/pagespeedonline/v5"
).replace(/\/$/, "");
const PAGESPEED_TIMEOUT_MS = Number(process.env.PAGESPEED_TIMEOUT_MS || 90_000);
const CRUX_API_BASE = (process.env.CRUX_API_BASE || "https://chromeuxreport.googleapis.com/v1").replace(/\/$/, "");
const CRUX_TIMEOUT_MS = Number(process.env.CRUX_TIMEOUT_MS || 30_000);
const PAGESPEED_CATEGORY_VALUES = ["performance", "accessibility", "best-practices", "seo", "pwa"];
const PAGESPEED_STRATEGY_VALUES = ["mobile", "desktop"];
const CRUX_FORM_FACTOR_VALUES = ["PHONE", "DESKTOP", "TABLET"];
const PEXELS_ORIENTATION_VALUES = ["landscape", "portrait", "square"];
const PEXELS_PHOTO_SIZE_VALUES = ["large", "medium", "small"];
const PEXELS_PHOTO_COLOR_VALUES = [
  "red",
  "orange",
  "yellow",
  "green",
  "turquoise",
  "blue",
  "violet",
  "pink",
  "brown",
  "black",
  "gray",
  "white"
];
const TEMPLATED_RENDER_FORMAT_VALUES = ["jpg", "png", "webp", "pdf", "mp4"];
const WEB_PAGE_SECTION_VALUES = [
  "seo",
  "meta",
  "headings",
  "links",
  "images",
  "paragraphs",
  "schema",
  "text",
  "html",
  "all"
];
const WEB_PAGE_DEFAULT_SECTIONS = ["seo", "headings", "links", "schema", "text"];
const ASANA_ATTACHMENT_DEFAULT_MAX_BYTES = 2 * 1024 * 1024;
const ASANA_ATTACHMENT_HARD_MAX_BYTES = 8 * 1024 * 1024;

const TOOL_READ_ONLY = Object.freeze({ readOnlyHint: true, openWorldHint: false });
const TOOL_EXTERNAL_READ = Object.freeze({ readOnlyHint: true, openWorldHint: true });
const TOOL_SAFE_WRITE = Object.freeze({
  readOnlyHint: false,
  destructiveHint: false,
  idempotentHint: false,
  openWorldHint: false
});
const TOOL_IDEMPOTENT_SAFE_WRITE = Object.freeze({
  readOnlyHint: false,
  destructiveHint: false,
  idempotentHint: true,
  openWorldHint: false
});
const TOOL_EXTERNAL_WRITE = Object.freeze({
  readOnlyHint: false,
  destructiveHint: false,
  idempotentHint: false,
  openWorldHint: true
});
const TOOL_DESTRUCTIVE_IDEMPOTENT_WRITE = Object.freeze({
  readOnlyHint: false,
  destructiveHint: true,
  idempotentHint: true,
  openWorldHint: false
});

const AGENT_EMAIL_DEFAULTS = {
  "vip-ai-sales": "sales-agent@vip-studios.de",
  "vip-ai-operations": "operations-agent@vip-studios.de",
  "vip-ai-content": "content-agent@vip-studios.de",
  "vip-ai-support": "support-agent@vip-studios.de",
  "vip-ai-developer": "developer-agent@vip-studios.de",
  "vip-ai-marketing": "marketing-agent@vip-studios.de",
  "vip-ai-office": "office-agent@vip-studios.de",
  "vip-ai-design": "design-agent@vip-studios.de",
  "vip-ai-social-media": "social-media-agent@vip-studios.de",
  "vip-ai-memory": "memory-agent@vip-studios.de",
  "vip-ai-review": "review-agent@vip-studios.de",
  "vip-ai-strategy": "strategy-agent@vip-studios.de",
  "vip-ai-monitoring": "monitoring-agent@vip-studios.de",
  "vip-ai-communication": "communication-agent@vip-studios.de",
  "vip-ai-research": "research-agent@vip-studios.de",
  "vip-ai-project-manager": "project-manager-agent@vip-studios.de",
  "vip-ai-business-developer": "business-developer-agent@vip-studios.de",
  "vip-ai-finance": "finance-agent@vip-studios.de",
  "vip-ai-accounting": "accounting-agent@vip-studios.de",
  "rs-ai-sales": "sales-agent@reise-stories.de",
  "rs-ai-content": "content-agent@reise-stories.de",
  "rs-ai-support": "support-agent@reise-stories.de",
  "rs-ai-marketing": "marketing-agent@reise-stories.de",
  "rs-ai-office": "office-agent@reise-stories.de"
};

let googleTokenCache = { accessToken: null, expiresAt: 0 };
let googleSeoTokenCache = { accessToken: null, expiresAt: 0, envPrefix: null };
let googleAdsTokenCache = { accessToken: null, expiresAt: 0, envPrefix: null };
const emailDraftCache = new Map();

function getAsana(agentId = DEFAULT_AGENT_ID) {
  const envName = ASANA_TOKEN_ENVS[agentId];
  if (!envName) throw new Error(`Unbekannter agent_id: ${agentId}`);

  const token = process.env[envName];
  if (!token) throw new Error(`Asana-Token fehlt fuer agent_id ${agentId} (${envName})`);

  return axios.create({
    baseURL: "https://app.asana.com/api/1.0",
    headers: {
      Authorization: `Bearer ${token}`,
      "Asana-Enable": "new_rich_text"
    }
  });
}

function out(data) {
  return {
    content: [{ type: "text", text: JSON.stringify(data, null, 2) }]
  };
}

function isTextLikeAttachment(contentType, fileName) {
  const type = String(contentType || "").toLowerCase();
  const name = String(fileName || "").toLowerCase();
  return (
    type.startsWith("text/") ||
    type.includes("json") ||
    type.includes("xml") ||
    type.includes("csv") ||
    /\.(txt|md|csv|json|xml|html?|css|js|ts)$/i.test(name)
  );
}

function decodeTextPreview(buffer, maxChars) {
  return buffer.toString("utf8").replace(/\u0000/g, "").slice(0, maxChars);
}

async function listAsanaTaskAttachments(asana, taskGid) {
  const res = await asana.get("/attachments", {
    params: {
      parent: taskGid,
      opt_fields:
        "gid,name,resource_subtype,created_at,download_url,view_url,permanent_url,parent.gid,parent.name"
    }
  });
  return res.data.data || [];
}

async function downloadAsanaAttachment(asana, attachmentGid, maxBytes) {
  const metaRes = await asana.get(`/attachments/${attachmentGid}`, {
    params: {
      opt_fields:
        "gid,name,resource_subtype,created_at,download_url,view_url,permanent_url,parent.gid,parent.name"
    }
  });
  const attachment = metaRes.data.data;
  if (!attachment?.download_url) {
    throw new Error(
      "Asana-Anhang hat keine download_url. Pruefe, ob der Anhang noch existiert und fuer den Agenten sichtbar ist."
    );
  }

  const downloadRes = await axios.get(attachment.download_url, {
    responseType: "arraybuffer",
    timeout: 30000,
    maxContentLength: maxBytes,
    maxBodyLength: maxBytes,
    validateStatus: () => true
  });
  if (downloadRes.status < 200 || downloadRes.status >= 300) {
    throw new Error(`Asana-Anhang-Download fehlgeschlagen: HTTP ${downloadRes.status}`);
  }

  const bytes = Buffer.from(downloadRes.data);
  if (bytes.length > maxBytes) {
    throw new Error(
      `Asana-Anhang ist zu gross (${bytes.length} Bytes, Limit ${maxBytes} Bytes). max_bytes erhoehen oder Anhang manuell/ueber Drive bereitstellen.`
    );
  }

  return {
    attachment,
    bytes,
    content_type: downloadRes.headers["content-type"] || "",
    content_length_header: downloadRes.headers["content-length"] || null
  };
}

function containsPlainMention(value) {
  return /(^|[\s(])@[A-Za-zÄÖÜäöüß]/.test(value);
}

function containsRawHtmlTag(value) {
  return /<\/?[A-Za-z][A-Za-z0-9:-]*(?:\s[^<>]*)?>/.test(String(value || ""));
}

function containsEscapedHtmlTag(value) {
  return /&lt;\s*\/?\s*[A-Za-z][A-Za-z0-9:-]*(?:\s|&gt;|>)/i.test(String(value || ""));
}

function assertSafeAsanaRequest(method, path, data) {
  const isStoryCreate = method === "POST" && /^\/tasks\/[^/]+\/stories\/?$/.test(path);
  const isStoryUpdate = method === "PUT" && /^\/stories\/[^/]+\/?$/.test(path);
  const isTaskComplete =
    method === "PUT" &&
    /^\/tasks\/[^/]+\/?$/.test(path) &&
    data &&
    Object.prototype.hasOwnProperty.call(data, "completed") &&
    data.completed === true;
  const isTaskAssign =
    method === "PUT" &&
    /^\/tasks\/[^/]+\/?$/.test(path) &&
    data &&
    Object.prototype.hasOwnProperty.call(data, "assignee");

  if (isTaskComplete) {
    throw new Error(
      "Asana-Aufgaben duerfen nicht per rohem asana_request abgeschlossen werden. Nutze asana_complete_task; es prueft Assignee, finalen Kommentar und Readback."
    );
  }

  if (isTaskAssign) {
    throw new Error(
      "Asana-Aufgaben duerfen nicht per rohem asana_request umgewiesen werden. Nutze asana_assign_task; es prueft Tags, blockiert Routine-Re-Assigns und verifiziert den Readback."
    );
  }

  if (!isStoryCreate && !isStoryUpdate) return;

  throw new Error(
    "Asana-Kommentare duerfen nicht mehr ueber asana_request erstellt oder geaendert werden. Nutze das enge Tool asana_comment; es baut valides Asana-Rich-Text-Markup und prueft den Readback."
  );
}

function escapeAsanaXml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function trimUrlTrailingPunctuation(value) {
  let url = String(value || "");
  let suffix = "";
  while (/[.,;:!?]$/.test(url)) {
    suffix = url.slice(-1) + suffix;
    url = url.slice(0, -1);
  }
  while (url.endsWith(")") && (url.match(/\)/g) || []).length > (url.match(/\(/g) || []).length) {
    suffix = ")" + suffix;
    url = url.slice(0, -1);
  }
  return { url, suffix };
}

function escapeAsanaTextWithLinks(value) {
  const text = String(value ?? "");
  const urlRegex = /(?:https?:\/\/|www\.)[^\s<>"']+/gi;
  let output = "";
  let lastIndex = 0;

  for (const match of text.matchAll(urlRegex)) {
    const rawMatch = match[0];
    const start = match.index ?? 0;
    const { url, suffix } = trimUrlTrailingPunctuation(rawMatch);
    const href = url.startsWith("www.") ? `https://${url}` : url;

    let parsedUrl;
    try {
      parsedUrl = new URL(href);
    } catch {
      continue;
    }
    if (!["http:", "https:"].includes(parsedUrl.protocol)) continue;

    output += escapeAsanaXml(text.slice(lastIndex, start));
    output += `<a href="${escapeAsanaXml(parsedUrl.href)}">${escapeAsanaXml(url)}</a>${escapeAsanaXml(suffix)}`;
    lastIndex = start + rawMatch.length;
  }

  output += escapeAsanaXml(text.slice(lastIndex));
  return output;
}

function ensureNoUnsafeAsanaText(value, label) {
  const text = String(value ?? "");
  if (!text.trim()) return;
  if (containsPlainMention(text)) {
    throw new Error(`${label} enthaelt eine Plain-Text-Mention. Nutze mention_user_gid statt @Name.`);
  }
  if (containsRawHtmlTag(text) || containsEscapedHtmlTag(text)) {
    throw new Error(`${label} enthaelt HTML oder escaped HTML. Uebergib nur Klartext; asana_comment baut das Markup selbst.`);
  }
}

function validateAsanaGid(value, label) {
  if (value && !/^\d+$/.test(String(value))) {
    throw new Error(`${label} muss eine Asana-GID sein.`);
  }
}

function asanaParagraph(text) {
  ensureNoUnsafeAsanaText(text, "Asana-Kommentarabsatz");
  return `${escapeAsanaTextWithLinks(text)}\n\n`;
}

function asanaSection(section) {
  const parts = [];
  if (section.title) {
    ensureNoUnsafeAsanaText(section.title, "Asana-Kommentarueberschrift");
    parts.push(`<strong>${escapeAsanaXml(section.title)}</strong>\n`);
  }
  for (const paragraph of section.paragraphs || []) {
    parts.push(asanaParagraph(paragraph));
  }
  if (section.bullets?.length) {
    parts.push(
      `<ul>${section.bullets
        .map((bullet) => {
          ensureNoUnsafeAsanaText(bullet, "Asana-Kommentarlistenpunkt");
          return `<li>${escapeAsanaTextWithLinks(bullet)}</li>`;
        })
        .join("")}</ul>\n`
    );
  }
  for (const codeBlock of section.code_blocks || []) {
    parts.push(`<code>${escapeAsanaXml(codeBlock)}</code>\n\n`);
  }
  return parts.join("");
}

function collectAsanaCodeBlocks(sections = []) {
  return sections.flatMap((section) => section.code_blocks || []).filter((value) => String(value).trim());
}

function buildAsanaCommentHtml({ greeting, sections, mention_user_gid, mention_text, effort_note }) {
  validateAsanaGid(mention_user_gid, "mention_user_gid");
  if (mention_text && !mention_user_gid) {
    throw new Error("mention_text braucht mention_user_gid, damit die Person in Asana korrekt verlinkt wird.");
  }
  const parts = [];

  if (greeting) {
    parts.push(asanaParagraph(greeting));
  }
  for (const section of sections || []) {
    parts.push(asanaSection(section));
  }
  if (mention_user_gid || mention_text) {
    ensureNoUnsafeAsanaText(mention_text, "Asana-Kommentar-Mentiontext");
    const mention = mention_user_gid ? `<a data-asana-gid="${mention_user_gid}"/>` : "";
    const text = mention_text ? ` ${escapeAsanaTextWithLinks(mention_text)}` : "";
    parts.push(`${mention}${text}\n\n`);
  }
  if (effort_note) {
    ensureNoUnsafeAsanaText(effort_note, "Asana-Kommentar-Aufwandshinweis");
    parts.push(`<em>${escapeAsanaXml(effort_note)}</em>`);
  }

  if (!parts.length) {
    throw new Error("asana_comment braucht mindestens greeting, sections, mention_text oder effort_note.");
  }

  const html = `<body>${parts.join("")}</body>`;
  assertGeneratedAsanaHtml(html);
  return html;
}

function assertGeneratedAsanaHtml(html) {
  if (!html.startsWith("<body>") || !html.endsWith("</body>")) {
    throw new Error("Generierter Asana-Kommentar entspricht nicht dem engen Rich-Text-Format.");
  }
  if (/<\/?p\b/i.test(html)) {
    throw new Error("Asana-Kommentar darf keine <p>-Tags verwenden; Asana-Stories escapen sie sichtbar.");
  }

  const allowedTags = new Set(["body", "strong", "em", "code", "ul", "li", "a"]);
  for (const tagMatch of html.matchAll(/<\/?([A-Za-z][A-Za-z0-9:-]*)(?:\s[^<>]*)?\/?>/g)) {
    const tag = tagMatch[1];
    const raw = tagMatch[0];
    if (!allowedTags.has(tag)) {
      throw new Error(`Generierter Asana-Kommentar enthaelt nicht erlaubtes Tag <${tag}>.`);
    }
    if (tag === "a") {
      if (/^<a data-asana-gid="\d+"\/>$/.test(raw)) {
        continue;
      }
      if (/^<a href="https?:\/\/[^"]+">$/.test(raw) || raw === "</a>") {
        continue;
      }
      throw new Error("Asana-Link muss als sichere Mention oder als http(s)-href erzeugt werden.");
    } else if (/\s[^<>]*=/.test(raw)) {
      throw new Error(`Asana-Kommentar enthaelt Attribute auf nicht erlaubtem Tag <${tag}>.`);
    }
  }

  const htmlWithoutCode = html.replace(/<code>[\s\S]*?<\/code>/gi, "");
  if (/<br\b/i.test(htmlWithoutCode) || containsPlainMention(htmlWithoutCode) || containsEscapedHtmlTag(htmlWithoutCode)) {
    throw new Error("Generierter Asana-Kommentar enthaelt blockierte Tags, Mentions oder escaped HTML.");
  }
}

function stripExpectedCodeSnippets(text, snippets = []) {
  let cleaned = String(text || "");
  for (const snippet of snippets) {
    const raw = String(snippet || "");
    if (!raw) continue;
    cleaned = cleaned.split(raw).join("");
    cleaned = cleaned.split(escapeAsanaXml(raw)).join("");
  }
  return cleaned;
}

function assertAsanaStoryReadbackLooksSafe(story, expectedCodeSnippets = []) {
  const visibleText = stripExpectedCodeSnippets(story?.text || "", expectedCodeSnippets);
  const richText = String(story?.html_text || "");
  const richTextWithoutCode = richText.replace(/<code>[\s\S]*?<\/code>/gi, "");
  if (containsRawHtmlTag(visibleText) || containsEscapedHtmlTag(visibleText)) {
    throw new Error("Asana-Readback enthaelt sichtbares body/html-Markup. Kommentar muss manuell geprueft/korrigiert werden.");
  }
  if (containsEscapedHtmlTag(richTextWithoutCode) || /<br\b/i.test(richTextWithoutCode)) {
    throw new Error("Asana-Readback enthaelt escaped oder inkompatibles Rich-Text-Markup. Kommentar muss manuell geprueft/korrigiert werden.");
  }
}

function normalizePrivateKey(value) {
  return value.replace(/\\n/g, "\n");
}

function parseGoogleServiceAccount() {
  const raw =
    process.env.GOOGLE_SERVICE_ACCOUNT_JSON ||
    (process.env.GOOGLE_SERVICE_ACCOUNT_JSON_BASE64
      ? Buffer.from(process.env.GOOGLE_SERVICE_ACCOUNT_JSON_BASE64, "base64").toString("utf8")
      : "");

  if (!raw) return null;

  const parsed = JSON.parse(raw);
  if (!parsed.client_email || !parsed.private_key) {
    throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON muss client_email und private_key enthalten.");
  }

  return {
    ...parsed,
    private_key: normalizePrivateKey(parsed.private_key)
  };
}

function getOAuthConfigFromPrefix(prefix) {
  const clientId = process.env[`${prefix}_CLIENT_ID`];
  const clientSecret = process.env[`${prefix}_CLIENT_SECRET`];
  const refreshToken = process.env[`${prefix}_REFRESH_TOKEN`];
  if (!clientId || !clientSecret || !refreshToken) return null;
  return { prefix, clientId, clientSecret, refreshToken };
}

function getFirstOAuthConfig(prefixes) {
  for (const prefix of prefixes) {
    const config = getOAuthConfigFromPrefix(prefix);
    if (config) return config;
  }
  return null;
}

function oauthEnvSummary(prefixes) {
  return Object.fromEntries(
    prefixes.map((prefix) => [
      prefix,
      {
        client_id_configured: Boolean(process.env[`${prefix}_CLIENT_ID`]),
        client_secret_configured: Boolean(process.env[`${prefix}_CLIENT_SECRET`]),
        refresh_token_configured: Boolean(process.env[`${prefix}_REFRESH_TOKEN`])
      }
    ])
  );
}

async function refreshGoogleOAuthAccessToken(oauthConfig, tokenCache) {
  const now = Math.floor(Date.now() / 1000);
  if (
    tokenCache.accessToken &&
    tokenCache.expiresAt > now + 60 &&
    (!tokenCache.envPrefix || tokenCache.envPrefix === oauthConfig.prefix)
  ) {
    return tokenCache.accessToken;
  }

  const params = new URLSearchParams({
    client_id: oauthConfig.clientId,
    client_secret: oauthConfig.clientSecret,
    refresh_token: oauthConfig.refreshToken,
    grant_type: "refresh_token"
  });

  const res = await axios.post("https://oauth2.googleapis.com/token", params.toString(), {
    headers: { "Content-Type": "application/x-www-form-urlencoded" }
  });

  tokenCache.accessToken = res.data.access_token;
  tokenCache.expiresAt = now + Number(res.data.expires_in || 3600);
  tokenCache.envPrefix = oauthConfig.prefix;
  return tokenCache.accessToken;
}

async function getGoogleAccessToken() {
  const now = Math.floor(Date.now() / 1000);
  if (googleTokenCache.accessToken && googleTokenCache.expiresAt > now + 60) {
    return googleTokenCache.accessToken;
  }

  if (
    process.env.GOOGLE_OAUTH_CLIENT_ID &&
    process.env.GOOGLE_OAUTH_CLIENT_SECRET &&
    process.env.GOOGLE_OAUTH_REFRESH_TOKEN
  ) {
    const params = new URLSearchParams({
      client_id: process.env.GOOGLE_OAUTH_CLIENT_ID,
      client_secret: process.env.GOOGLE_OAUTH_CLIENT_SECRET,
      refresh_token: process.env.GOOGLE_OAUTH_REFRESH_TOKEN,
      grant_type: "refresh_token"
    });

    const res = await axios.post("https://oauth2.googleapis.com/token", params.toString(), {
      headers: { "Content-Type": "application/x-www-form-urlencoded" }
    });

    googleTokenCache = {
      accessToken: res.data.access_token,
      expiresAt: now + Number(res.data.expires_in || 3600)
    };
    return googleTokenCache.accessToken;
  }

  const serviceAccount = parseGoogleServiceAccount();
  if (!serviceAccount) {
    throw new Error(
      "Google-Zugang fehlt. Setze GOOGLE_SERVICE_ACCOUNT_JSON(_BASE64) oder GOOGLE_OAUTH_CLIENT_ID/GOOGLE_OAUTH_CLIENT_SECRET/GOOGLE_OAUTH_REFRESH_TOKEN."
    );
  }

  const header = { alg: "RS256", typ: "JWT" };
  const claims = {
    iss: serviceAccount.client_email,
    scope: GOOGLE_SCOPES.join(" "),
    aud: "https://oauth2.googleapis.com/token",
    iat: now,
    exp: now + 3600
  };
  if (process.env.GOOGLE_WORKSPACE_IMPERSONATE_EMAIL) {
    claims.sub = process.env.GOOGLE_WORKSPACE_IMPERSONATE_EMAIL;
  }

  const encodedHeader = Buffer.from(JSON.stringify(header)).toString("base64url");
  const encodedClaims = Buffer.from(JSON.stringify(claims)).toString("base64url");
  const signature = createSign("RSA-SHA256")
    .update(`${encodedHeader}.${encodedClaims}`)
    .sign(serviceAccount.private_key, "base64url");
  const assertion = `${encodedHeader}.${encodedClaims}.${signature}`;

  const params = new URLSearchParams({
    grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
    assertion
  });

  const res = await axios.post("https://oauth2.googleapis.com/token", params.toString(), {
    headers: { "Content-Type": "application/x-www-form-urlencoded" }
  });

  googleTokenCache = {
    accessToken: res.data.access_token,
    expiresAt: now + Number(res.data.expires_in || 3600)
  };
  return googleTokenCache.accessToken;
}

async function getGoogleSeoAccessToken() {
  const oauthConfig = getFirstOAuthConfig(GOOGLE_SEO_OAUTH_PREFIXES);
  if (!oauthConfig) {
    throw new Error(
      `Google SEO OAuth-Zugang fehlt. Setze ${GOOGLE_SEO_OAUTH_PREFIXES.map(
        (prefix) => `${prefix}_CLIENT_ID/${prefix}_CLIENT_SECRET/${prefix}_REFRESH_TOKEN`
      ).join(" oder ")}.`
    );
  }
  return refreshGoogleOAuthAccessToken(oauthConfig, googleSeoTokenCache);
}

async function getGoogleAdsAccessToken() {
  const oauthConfig = getFirstOAuthConfig(GOOGLE_ADS_OAUTH_PREFIXES);
  if (!oauthConfig) {
    throw new Error(
      `Google-Ads-OAuth-Zugang fehlt. Setze ${GOOGLE_ADS_OAUTH_PREFIXES.map(
        (prefix) => `${prefix}_CLIENT_ID/${prefix}_CLIENT_SECRET/${prefix}_REFRESH_TOKEN`
      ).join(" oder ")}.`
    );
  }
  return refreshGoogleOAuthAccessToken(oauthConfig, googleAdsTokenCache);
}

async function googleSeoRequest(config) {
  const accessToken = await getGoogleSeoAccessToken();
  return axios.request({
    ...config,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      ...(config.headers || {})
    }
  });
}

function getGoogleAdsDeveloperToken() {
  return process.env.GOOGLE_ADS_DEVELOPER_TOKEN || process.env.ADWORDS_DEVELOPER_TOKEN || "";
}

function normalizeGoogleAdsCustomerId(customerId, fieldName = "customer_id") {
  const normalized = String(customerId || "").replace(/\D/g, "");
  if (!normalized) throw new Error(`${fieldName} darf nicht leer sein.`);
  return normalized;
}

function getGoogleAdsLoginCustomerId(loginCustomerId) {
  const raw =
    loginCustomerId ||
    process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID ||
    process.env.GOOGLE_ADS_MANAGER_CUSTOMER_ID ||
    "";
  return raw ? normalizeGoogleAdsCustomerId(raw, "login_customer_id") : null;
}

function compactAxiosError(error) {
  const responseData = error.response?.data;
  return {
    status: error.response?.status || null,
    message: responseData?.error?.message || responseData?.message || error.message,
    request_id: error.response?.headers?.["request-id"] || null,
    response: responseData || null
  };
}

async function googleAdsRequest({ method = "GET", path, data, params, login_customer_id, timeout = GOOGLE_ADS_TIMEOUT_MS }) {
  const developerToken = getGoogleAdsDeveloperToken();
  if (!developerToken) {
    throw new Error("Google-Ads-Developer-Token fehlt. Setze GOOGLE_ADS_DEVELOPER_TOKEN.");
  }
  const accessToken = await getGoogleAdsAccessToken();
  const loginCustomerId = getGoogleAdsLoginCustomerId(login_customer_id);
  return axios.request({
    method,
    url: `${GOOGLE_ADS_API_BASE}/${GOOGLE_ADS_API_VERSION}${path}`,
    data,
    params,
    timeout,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "developer-token": developerToken,
      ...(loginCustomerId ? { "login-customer-id": loginCustomerId } : {})
    }
  });
}

async function getGoogleTokenInfo(accessToken) {
  const res = await axios.get("https://www.googleapis.com/oauth2/v1/tokeninfo", {
    params: { access_token: accessToken },
    validateStatus: () => true
  });
  return res;
}

function normalizeGa4PropertyName(property) {
  const raw = String(property || "").trim();
  if (!raw) throw new Error("property_id darf nicht leer sein.");
  return raw.startsWith("properties/") ? raw : `properties/${raw}`;
}

async function googleRequest(config) {
  const accessToken = await getGoogleAccessToken();
  return axios.request({
    ...config,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      ...(config.headers || {})
    }
  });
}

async function getDriveFile(fileId, fields = "id,name,mimeType,parents,webViewLink") {
  const res = await googleRequest({
    method: "GET",
    url: `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    params: {
      fields,
      supportsAllDrives: true
    }
  });
  return res.data;
}

async function assertAllowedGoogleFolder(folderId) {
  if (GOOGLE_ALLOWED_FOLDER_IDS.has(folderId)) return;

  let currentId = folderId;
  for (let depth = 0; depth < 10; depth += 1) {
    const folder = await getDriveFile(currentId, "id,name,mimeType,parents");
    if (folder.mimeType !== "application/vnd.google-apps.folder") {
      throw new Error(`Google target_folder_id ist kein Ordner: ${folderId}`);
    }

    const parents = folder.parents || [];
    if (parents.some((parent) => GOOGLE_ALLOWED_FOLDER_IDS.has(parent))) return;
    if (!parents.length) break;
    currentId = parents[0];
  }

  throw new Error(
    `Google-Ordner ${folderId} liegt nicht im erlaubten Agenten-Drive-Bereich. Erlaubter Root: ${GOOGLE_AGENT_FOLDER_ID}`
  );
}

async function getSheetIdByName(spreadsheetId, sheetName) {
  const res = await googleRequest({
    method: "GET",
    url: `https://sheets.googleapis.com/v4/spreadsheets/${encodeURIComponent(spreadsheetId)}`,
    params: { fields: "sheets(properties(sheetId,title))" }
  });
  const sheet = (res.data.sheets || []).find((item) => item.properties?.title === sheetName);
  if (!sheet) throw new Error(`Sheet nicht gefunden: ${sheetName}`);
  return sheet.properties.sheetId;
}

function buildSheetRange(sheetName, range) {
  return `${sheetName}!${range}`;
}

async function getSheetValues(spreadsheetId, range, valueRenderOption = "FORMATTED_VALUE") {
  const res = await googleRequest({
    method: "GET",
    url: `https://sheets.googleapis.com/v4/spreadsheets/${encodeURIComponent(
      spreadsheetId
    )}/values/${encodeURIComponent(range)}`,
    params: { valueRenderOption }
  });
  return res.data.values || [];
}

function cleanCell(value) {
  return String(value ?? "").trim();
}

function envSuffixForAgent(agentId) {
  const asanaEnv = ASANA_TOKEN_ENVS[agentId];
  if (!asanaEnv) throw new Error(`Unbekannter agent_id: ${agentId}`);
  return asanaEnv.replace(/^ASANA_TOKEN_/, "");
}

function getEnvWithAgentFallback(baseName, agentId) {
  const suffix = envSuffixForAgent(agentId);
  return process.env[`${baseName}_${suffix}`] || process.env[baseName];
}

function getScopedEnvDetails(baseName, agentId) {
  const suffix = envSuffixForAgent(agentId);
  const agentEnvName = `${baseName}_${suffix}`;
  const globalEnvName = baseName;
  const agentValue = process.env[agentEnvName] || "";
  const globalValue = process.env[globalEnvName] || "";
  const selectedEnvName = agentValue ? agentEnvName : globalEnvName;
  const value = agentValue || globalValue;
  return {
    value,
    selectedEnvName,
    agentEnvName,
    globalEnvName,
    configured: Boolean(value),
    agentConfigured: Boolean(agentValue),
    globalConfigured: Boolean(globalValue)
  };
}

function parseBooleanEnv(value, defaultValue = false) {
  if (value === undefined || value === null || value === "") return defaultValue;
  return ["1", "true", "yes", "ja"].includes(String(value).trim().toLowerCase());
}

function parseRecipients(value) {
  const recipients = String(value || "")
    .split(/[;,]/)
    .map((item) => item.trim())
    .filter(Boolean);
  if (!recipients.length) throw new Error("Mindestens ein E-Mail-Empfaenger ist erforderlich.");
  for (const recipient of recipients) {
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(recipient)) {
      throw new Error(`Ungueltige E-Mail-Adresse: ${recipient}`);
    }
  }
  return recipients;
}

function assertAllowedEmailRecipients(recipients) {
  const rawAllowed = process.env.EMAIL_ALLOWED_RECIPIENTS || DEFAULT_EMAIL_ALLOWED_RECIPIENTS;
  if (rawAllowed.trim() === "*") return;

  const allowed = new Set(parseRecipients(rawAllowed).map((item) => item.toLowerCase()));
  const blocked = recipients.filter((recipient) => !allowed.has(recipient.toLowerCase()));
  if (blocked.length) {
    throw new Error(
      `E-Mail-Empfaenger nicht erlaubt: ${blocked.join(", ")}. Erlaubt per EMAIL_ALLOWED_RECIPIENTS: ${rawAllowed}`
    );
  }
}

function validateEmailPayload({ to, subject, body }) {
  const recipients = parseRecipients(to);
  assertAllowedEmailRecipients(recipients);

  const trimmedSubject = subject.trim();
  if (!trimmedSubject) throw new Error("subject darf nicht leer sein.");
  if (Buffer.byteLength(trimmedSubject, "utf8") > 300) {
    throw new Error("subject ist zu lang (>300 Bytes).");
  }
  if (!body.trim()) throw new Error("body darf nicht leer sein.");
  if (Buffer.byteLength(body, "utf8") > 2_000_000) {
    throw new Error("body ist zu gross fuer agent_email_send (>2 MB).");
  }

  return {
    recipients,
    subject: trimmedSubject,
    body_bytes: Buffer.byteLength(body, "utf8"),
    body_sha256: createHash("sha256").update(body, "utf8").digest("hex")
  };
}

function defaultSmtpHostForAddress(fromAddress) {
  if (/@vip-studios\.de$/i.test(fromAddress || "")) {
    return "vip-studios.vip-studios.de";
  }
  return "";
}

function publicSmtpConfig(config) {
  return {
    host: config.host,
    port: config.port,
    secure: config.secure,
    label: config.label || "primary"
  };
}

function getSmtpConfigDetails(agentId) {
  const suffix = envSuffixForAgent(agentId);
  const fromAddress =
    process.env[`EMAIL_ADDRESS_${suffix}`] || AGENT_EMAIL_DEFAULTS[agentId];
  const configuredHost = getEnvWithAgentFallback("SMTP_HOST", agentId);
  const defaultHost = defaultSmtpHostForAddress(fromAddress);
  const host = configuredHost || defaultHost;
  const rawPort = getEnvWithAgentFallback("SMTP_PORT", agentId);
  const port = Number(rawPort || 465);
  const rawSecure = getEnvWithAgentFallback("SMTP_SECURE", agentId);
  const secure = parseBooleanEnv(rawSecure, port === 465);
  const configuredUser = process.env[`SMTP_USER_${suffix}`];
  const user = configuredUser || fromAddress;
  const passwordEnvName = process.env[`SMTP_PASSWORD_${suffix}`]
    ? `SMTP_PASSWORD_${suffix}`
    : process.env[`EMAIL_PASSWORD_${suffix}`]
      ? `EMAIL_PASSWORD_${suffix}`
      : "";
  const password = passwordEnvName ? process.env[passwordEnvName] : "";

  return {
    suffix,
    config: { fromAddress, host, port, secure, user, password },
    summary: {
      agent_id: agentId,
      from: fromAddress,
      host_configured: Boolean(host),
      host_source: configuredHost ? "env" : defaultHost ? "default_vip_studios_domain" : "missing",
      port,
      port_source: rawPort ? "env" : "default",
      secure,
      secure_source: rawSecure ? "env" : "default",
      user_configured: Boolean(configuredUser),
      user,
      password_configured: Boolean(password),
      password_env_name: passwordEnvName || null,
      ready_for_send: Boolean(fromAddress && host && password)
    }
  };
}

function getSmtpConfigCandidates(agentId, { requireCredentials = true } = {}) {
  const { suffix, config, summary } = getSmtpConfigDetails(agentId);

  if (!config.fromAddress) throw new Error(`E-Mail-Adresse fuer agent_id ${agentId} fehlt.`);
  if (requireCredentials) {
    if (!config.host) throw new Error(`SMTP_HOST oder SMTP_HOST_${suffix} fehlt im MCP-Environment.`);
    if (!config.password) {
      throw new Error(`SMTP_PASSWORD_${suffix} oder EMAIL_PASSWORD_${suffix} fehlt im MCP-Environment.`);
    }
  }

  const candidates = [{ ...config, label: "primary" }];
  const hasExplicitPort = summary.port_source === "env";
  const isVipStudiosHost = /^vip-studios\.vip-studios\.de$/i.test(config.host || "");
  if (!hasExplicitPort && isVipStudiosHost && config.port === 465) {
    candidates.push({ ...config, port: 587, secure: false, label: "vip_studios_587_starttls_fallback" });
  }

  const uniqueCandidates = [];
  const seen = new Set();
  for (const candidate of candidates) {
    const key = `${candidate.host}:${candidate.port}:${candidate.secure}`;
    if (seen.has(key)) continue;
    seen.add(key);
    uniqueCandidates.push(candidate);
  }

  return {
    suffix,
    configs: uniqueCandidates,
    primaryConfig: uniqueCandidates[0],
    summary: {
      ...summary,
      smtp_timeout_ms: SMTP_TIMEOUT_MS,
      smtp_candidates: uniqueCandidates.map(publicSmtpConfig)
    }
  };
}

function getSmtpConfig(agentId, { requireCredentials = true } = {}) {
  return getSmtpConfigCandidates(agentId, { requireCredentials }).primaryConfig;
}

function publicImapConfig(config) {
  return {
    host: config.host,
    port: config.port,
    secure: config.secure,
    label: config.label || "primary"
  };
}

function getImapConfigDetails(agentId) {
  const suffix = envSuffixForAgent(agentId);
  const fromAddress =
    process.env[`EMAIL_ADDRESS_${suffix}`] || AGENT_EMAIL_DEFAULTS[agentId];
  const configuredHost =
    getEnvWithAgentFallback("IMAP_HOST", agentId) || getEnvWithAgentFallback("SMTP_HOST", agentId);
  const defaultHost = defaultSmtpHostForAddress(fromAddress);
  const host = configuredHost || defaultHost;
  const rawPort = getEnvWithAgentFallback("IMAP_PORT", agentId);
  const port = Number(rawPort || 993);
  const rawSecure = getEnvWithAgentFallback("IMAP_SECURE", agentId);
  const secure = parseBooleanEnv(rawSecure, port === 993);
  const configuredUser = process.env[`IMAP_USER_${suffix}`] || process.env[`SMTP_USER_${suffix}`];
  const user = configuredUser || fromAddress;
  const passwordEnvName = process.env[`IMAP_PASSWORD_${suffix}`]
    ? `IMAP_PASSWORD_${suffix}`
    : process.env[`EMAIL_PASSWORD_${suffix}`]
      ? `EMAIL_PASSWORD_${suffix}`
      : process.env[`SMTP_PASSWORD_${suffix}`]
        ? `SMTP_PASSWORD_${suffix}`
        : "";
  const password = passwordEnvName ? process.env[passwordEnvName] : "";

  return {
    suffix,
    config: { fromAddress, host, port, secure, user, password },
    summary: {
      agent_id: agentId,
      from: fromAddress,
      imap_host_configured: Boolean(host),
      imap_host_source: configuredHost ? "env" : defaultHost ? "default_vip_studios_domain" : "missing",
      imap_port: port,
      imap_port_source: rawPort ? "env" : "default",
      imap_secure: secure,
      imap_secure_source: rawSecure ? "env" : "default",
      imap_user_configured: Boolean(configuredUser),
      imap_user: user,
      imap_password_configured: Boolean(password),
      imap_password_env_name: passwordEnvName || null,
      imap_ready_for_read: Boolean(fromAddress && host && password)
    }
  };
}

function getImapConfigCandidates(agentId, { requireCredentials = true } = {}) {
  const { suffix, config, summary } = getImapConfigDetails(agentId);

  if (!config.fromAddress) throw new Error(`E-Mail-Adresse fuer agent_id ${agentId} fehlt.`);
  if (requireCredentials) {
    if (!config.host) throw new Error(`IMAP_HOST, SMTP_HOST oder Default-Host fuer ${suffix} fehlt im MCP-Environment.`);
    if (!config.password) {
      throw new Error(`IMAP_PASSWORD_${suffix}, EMAIL_PASSWORD_${suffix} oder SMTP_PASSWORD_${suffix} fehlt im MCP-Environment.`);
    }
  }

  const candidates = [{ ...config, label: "primary" }];
  const hasExplicitPort = summary.imap_port_source === "env";
  const isVipStudiosHost = /^vip-studios\.vip-studios\.de$/i.test(config.host || "");
  if (!hasExplicitPort && isVipStudiosHost && config.port === 993) {
    candidates.push({ ...config, port: 143, secure: false, label: "vip_studios_143_plain_fallback" });
  }

  const uniqueCandidates = [];
  const seen = new Set();
  for (const candidate of candidates) {
    const key = `${candidate.host}:${candidate.port}:${candidate.secure}`;
    if (seen.has(key)) continue;
    seen.add(key);
    uniqueCandidates.push(candidate);
  }

  return {
    suffix,
    configs: uniqueCandidates,
    summary: {
      ...summary,
      imap_timeout_ms: IMAP_TIMEOUT_MS,
      imap_candidates: uniqueCandidates.map(publicImapConfig)
    }
  };
}

function quoteImapString(value) {
  return `"${String(value ?? "").replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

function parseImapHeaderBlock(block) {
  const getHeader = (name) => {
    const match = new RegExp(`(?:^|\\r?\\n)${name}:\\s*([^\\r\\n]*)`, "i").exec(block);
    return match ? match[1].trim() : "";
  };
  return {
    from: getHeader("From"),
    subject: getHeader("Subject"),
    date: getHeader("Date"),
    message_id: getHeader("Message-ID")
  };
}

function cleanImapPreview(value, maxLength) {
  const compact = String(value || "")
    .replace(/\r?\n/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (compact.length <= maxLength) return compact;
  return `${compact.slice(0, Math.max(0, maxLength - 1)).trim()}…`;
}

async function openImapSocket(config) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const done = (error, socket) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (error) {
        if (socket && !socket.destroyed) socket.destroy();
        reject(error);
      } else {
        resolve(socket);
      }
    };

    const timer = setTimeout(() => {
      done(new Error(`IMAP connect timeout after ${IMAP_TIMEOUT_MS}ms (${config.host}:${config.port})`), socket);
    }, IMAP_TIMEOUT_MS);

    const socket = config.secure
      ? tls.connect({
          host: config.host,
          port: config.port,
          servername: config.host,
          rejectUnauthorized: !parseBooleanEnv(process.env.IMAP_TLS_ALLOW_UNAUTHORIZED, false)
        })
      : net.connect({ host: config.host, port: config.port });

    socket.once("secureConnect", () => done(null, socket));
    socket.once("connect", () => {
      if (!config.secure) done(null, socket);
    });
    socket.once("error", (error) => done(error, socket));
  });
}

async function readImapUntil(socket, state, matcher, label) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const cleanup = () => {
      clearTimeout(timer);
      socket.off("data", onData);
      socket.off("error", onError);
    };
    const finish = (error, value) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (error) reject(error);
      else resolve(value);
    };
    const check = () => {
      const match = matcher(state.buffer);
      if (match) finish(null, match);
    };
    const onData = (chunk) => {
      state.buffer += chunk;
      check();
    };
    const onError = (error) => finish(error);
    const timer = setTimeout(
      () => finish(new Error(`IMAP response timeout after ${IMAP_TIMEOUT_MS}ms (${label})`)),
      IMAP_TIMEOUT_MS
    );
    socket.on("data", onData);
    socket.once("error", onError);
    check();
  });
}

async function runImapReadUnseen(config, { limit, includeSnippets, snippetChars }) {
  const socket = await openImapSocket(config);
  socket.setEncoding("utf8");
  const state = { buffer: "" };
  let tagCounter = 1;

  const greeting = await readImapUntil(
    socket,
    state,
    (buffer) => {
      const end = buffer.indexOf("\n");
      if (end < 0) return null;
      const line = buffer.slice(0, end + 1);
      state.buffer = buffer.slice(end + 1);
      return line;
    },
    "greeting"
  );
  if (!/^\* OK/i.test(greeting)) {
    socket.destroy();
    throw new Error(`IMAP greeting nicht OK: ${cleanImapPreview(greeting, 200)}`);
  }

  const command = async (payload) => {
    const tag = `a${tagCounter++}`;
    socket.write(`${tag} ${payload}\r\n`);
    const response = await readImapUntil(
      socket,
      state,
      (buffer) => {
        const regex = new RegExp(`(?:^|\\r?\\n)${tag} (OK|NO|BAD)[^\\r\\n]*(?:\\r?\\n|$)`, "i");
        const match = regex.exec(buffer);
        if (!match) return null;
        const end = match.index + match[0].length;
        const value = buffer.slice(0, end);
        state.buffer = buffer.slice(end);
        return value;
      },
      payload.split(/\s+/, 1)[0]
    );
    if (!new RegExp(`(?:^|\\r?\\n)${tag} OK`, "i").test(response)) {
      throw new Error(`IMAP ${payload.split(/\s+/, 1)[0]} fehlgeschlagen: ${cleanImapPreview(response, 500)}`);
    }
    return response;
  };

  try {
    await command(`LOGIN ${quoteImapString(config.user)} ${quoteImapString(config.password)}`);
    await command("EXAMINE INBOX");
    const searchResponse = await command("UID SEARCH UNSEEN");
    const searchLine = searchResponse
      .split(/\r?\n/)
      .find((line) => /^\* SEARCH(?:[ \t]|$)/i.test(line));
    const uidText = searchLine ? searchLine.replace(/^\* SEARCH[ \t]*/i, "").trim() : "";
    const uids = uidText.split(/[ \t]+/).filter((value) => /^\d+$/.test(value));
    const selectedUids = uids.slice(0, limit);
    let messages = [];

    if (selectedUids.length) {
      const fetchItems = includeSnippets
        ? `(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE MESSAGE-ID)] BODY.PEEK[TEXT]<0.${snippetChars}>)`
        : "(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE MESSAGE-ID)])";
      const fetchResponse = await command(`UID FETCH ${selectedUids.join(",")} ${fetchItems}`);
      const uidMatches = [...fetchResponse.matchAll(/\bUID\s+(\d+)\b/g)];
      messages = uidMatches.map((match, index) => {
        const start = match.index || 0;
        const end = index + 1 < uidMatches.length ? uidMatches[index + 1].index || fetchResponse.length : fetchResponse.length;
        const segment = fetchResponse.slice(start, end);
        const headers = parseImapHeaderBlock(segment);
        return {
          uid: match[1],
          ...headers,
          preview: includeSnippets ? cleanImapPreview(segment, snippetChars) : undefined
        };
      });
    }

    await command("LOGOUT").catch(() => {});
    if (!socket.destroyed) socket.destroy();
    return {
      unseen_count: uids.length,
      returned_count: messages.length,
      messages
    };
  } catch (error) {
    if (!socket.destroyed) socket.destroy();
    throw error;
  }
}

async function readUnseenEmailWithFallback(configs, options) {
  const attempts = [];
  for (const config of configs) {
    try {
      const result = await runImapReadUnseen(config, options);
      return {
        ...result,
        host: config.host,
        port: config.port,
        secure: config.secure,
        label: config.label || "primary",
        attempts
      };
    } catch (error) {
      attempts.push({
        host: config.host,
        port: config.port,
        secure: config.secure,
        label: config.label || "primary",
        ok: false,
        error: String(error?.message || error)
      });
    }
  }
  const error = new Error(
    `IMAP read-only fehlgeschlagen: ${attempts.map((attempt) => `${attempt.label} ${attempt.host}:${attempt.port} ${attempt.error}`).join(" | ")}`
  );
  error.imap_attempts = attempts;
  throw error;
}

function normalizeEmailHttpProvider(value) {
  const provider = String(value || "").trim().toLowerCase();
  if (!provider || provider === "none" || provider === "smtp") return "";
  if (["resend", "brevo"].includes(provider)) return provider;
  throw new Error(`Unbekannter EMAIL_HTTP_PROVIDER: ${provider}. Erlaubt: resend, brevo.`);
}

function getEmailHttpConfigDetails(agentId, { requireCredentials = true } = {}) {
  const suffix = envSuffixForAgent(agentId);
  const fromAddress =
    process.env[`EMAIL_ADDRESS_${suffix}`] || AGENT_EMAIL_DEFAULTS[agentId];
  const provider = normalizeEmailHttpProvider(
    getEnvWithAgentFallback("EMAIL_HTTP_PROVIDER", agentId) ||
      getEnvWithAgentFallback("EMAIL_PROVIDER", agentId)
  );
  const fromName = getEnvWithAgentFallback("EMAIL_FROM_NAME", agentId) || "";
  const genericApiKey = getEnvWithAgentFallback("EMAIL_HTTP_API_KEY", agentId);
  let apiKeyEnvName = "";
  let apiKey = "";

  if (provider === "resend") {
    apiKeyEnvName = process.env[`RESEND_API_KEY_${suffix}`]
      ? `RESEND_API_KEY_${suffix}`
      : process.env.RESEND_API_KEY
        ? "RESEND_API_KEY"
        : genericApiKey
          ? process.env[`EMAIL_HTTP_API_KEY_${suffix}`]
            ? `EMAIL_HTTP_API_KEY_${suffix}`
            : "EMAIL_HTTP_API_KEY"
          : "";
    apiKey = apiKeyEnvName ? process.env[apiKeyEnvName] : "";
  } else if (provider === "brevo") {
    apiKeyEnvName = process.env[`BREVO_API_KEY_${suffix}`]
      ? `BREVO_API_KEY_${suffix}`
      : process.env.BREVO_API_KEY
        ? "BREVO_API_KEY"
        : genericApiKey
          ? process.env[`EMAIL_HTTP_API_KEY_${suffix}`]
            ? `EMAIL_HTTP_API_KEY_${suffix}`
            : "EMAIL_HTTP_API_KEY"
          : "";
    apiKey = apiKeyEnvName ? process.env[apiKeyEnvName] : "";
  }

  if (requireCredentials && provider && !apiKey) {
    throw new Error(`API-Key fuer EMAIL_HTTP_PROVIDER=${provider} fehlt im MCP-Environment.`);
  }

  return {
    config: { provider, apiKey, fromAddress, fromName },
    summary: {
      email_http_provider: provider || null,
      email_http_provider_configured: Boolean(provider),
      email_http_api_key_configured: Boolean(apiKey),
      email_http_api_key_env_name: apiKeyEnvName || null,
      email_http_from_name: fromName || null,
      email_http_ready_for_send: Boolean(provider && apiKey && fromAddress),
      email_http_timeout_ms: EMAIL_HTTP_TIMEOUT_MS
    }
  };
}

function formatHttpFrom(config) {
  if (!config.fromName) return config.fromAddress;
  return `${config.fromName} <${config.fromAddress}>`;
}

function sanitizeEmailHttpResponse(provider, response) {
  if (provider === "resend") {
    return { provider, id: response?.data?.id || null };
  }
  if (provider === "brevo") {
    return { provider, messageId: response?.data?.messageId || null };
  }
  return { provider };
}

function formatEmailHttpError(provider, error) {
  const status = error.response?.status;
  const data = error.response?.data;
  const message = typeof data === "string" ? data : data?.message || data?.error || error.message;
  return `${provider} HTTP send failed${status ? ` (${status})` : ""}: ${message}`;
}

async function sendEmailViaHttpProvider(config, { to, subject, body, idempotencyKey }) {
  if (config.provider === "resend") {
    const response = await axios.post(
      "https://api.resend.com/emails",
      {
        from: formatHttpFrom(config),
        to,
        subject,
        text: body
      },
      {
        timeout: EMAIL_HTTP_TIMEOUT_MS,
        headers: {
          Authorization: `Bearer ${config.apiKey}`,
          "Content-Type": "application/json",
          ...(idempotencyKey ? { "Idempotency-Key": idempotencyKey } : {})
        }
      }
    ).catch((error) => {
      throw new Error(formatEmailHttpError("resend", error));
    });
    return sanitizeEmailHttpResponse("resend", response);
  }

  if (config.provider === "brevo") {
    const response = await axios.post(
      "https://api.brevo.com/v3/smtp/email",
      {
        sender: {
          email: config.fromAddress,
          ...(config.fromName ? { name: config.fromName } : {})
        },
        to: to.map((email) => ({ email })),
        subject,
        textContent: body
      },
      {
        timeout: EMAIL_HTTP_TIMEOUT_MS,
        headers: {
          accept: "application/json",
          "api-key": config.apiKey,
          "content-type": "application/json"
        }
      }
    ).catch((error) => {
      throw new Error(formatEmailHttpError("brevo", error));
    });
    return sanitizeEmailHttpResponse("brevo", response);
  }

  throw new Error("Kein EMAIL_HTTP_PROVIDER konfiguriert.");
}

function cleanupExpiredEmailDrafts() {
  const now = Date.now();
  for (const [draftId, draft] of emailDraftCache.entries()) {
    if (draft.expires_at_ms <= now) {
      emailDraftCache.delete(draftId);
    }
  }
}

function storeEmailDraft({ agent_id, from, to, subject, body, body_bytes, body_sha256 }) {
  cleanupExpiredEmailDrafts();
  const draft_id = randomUUID();
  const expires_at_ms = Date.now() + EMAIL_DRAFT_TTL_MS;
  emailDraftCache.set(draft_id, {
    agent_id,
    from,
    to,
    subject,
    body,
    body_bytes,
    body_sha256,
    expires_at_ms
  });
  return { draft_id, expires_at_ms };
}

function getEmailDraft(draftId, agentId) {
  cleanupExpiredEmailDrafts();
  const draft = emailDraftCache.get(draftId);
  if (!draft) throw new Error("E-Mail-Draft nicht gefunden oder abgelaufen.");
  if (draft.agent_id !== agentId) {
    throw new Error("E-Mail-Draft gehoert zu einer anderen agent_id.");
  }
  return draft;
}

function createSmtpReader(socket) {
  let buffer = "";
  const waiters = [];

  socket.on("data", (chunk) => {
    buffer += chunk.toString("utf8");
    for (let index = waiters.length - 1; index >= 0; index -= 1) {
      waiters[index]();
      waiters.splice(index, 1);
    }
  });

  return async function readResponse() {
    for (;;) {
      const lines = buffer.split(/\r?\n/);
      for (let index = 0; index < lines.length; index += 1) {
        if (/^\d{3} /.test(lines[index])) {
          const responseLines = lines.slice(0, index + 1);
          buffer = lines.slice(index + 1).join("\r\n");
          const code = Number(lines[index].slice(0, 3));
          return { code, text: responseLines.join("\n") };
        }
      }
      await new Promise((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error(`SMTP response timeout after ${SMTP_TIMEOUT_MS}ms`)),
          SMTP_TIMEOUT_MS
        );
        waiters.push(() => {
          clearTimeout(timer);
          resolve();
        });
      });
    }
  };
}

function writeSmtp(socket, command) {
  socket.write(`${command}\r\n`);
}

async function expectSmtp(readResponse, expectedCodes, label) {
  const response = await readResponse();
  if (!expectedCodes.includes(response.code)) {
    throw new Error(`SMTP ${label} fehlgeschlagen (${response.code}): ${response.text}`);
  }
  return response;
}

async function createSmtpSocket(config) {
  if (config.secure) {
    return new Promise((resolve, reject) => {
      let settled = false;
      let timer;
      const done = (error, socket) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (error) reject(error);
        else resolve(socket);
      };
      const socket = tls.connect(
        {
          host: config.host,
          port: config.port,
          servername: config.host,
          rejectUnauthorized: !parseBooleanEnv(process.env.SMTP_TLS_ALLOW_UNAUTHORIZED, false)
        },
        () => done(null, socket)
      );
      timer = setTimeout(() => {
        socket.destroy();
        done(new Error(`SMTP connect timeout after ${SMTP_TIMEOUT_MS}ms (${config.host}:${config.port})`));
      }, SMTP_TIMEOUT_MS);
      socket.once("error", (error) => done(error));
    });
  }

  return new Promise((resolve, reject) => {
    let settled = false;
    let timer;
    const done = (error, socket) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (error) reject(error);
      else resolve(socket);
    };
    const socket = net.connect({ host: config.host, port: config.port }, () => done(null, socket));
    timer = setTimeout(() => {
      socket.destroy();
      done(new Error(`SMTP connect timeout after ${SMTP_TIMEOUT_MS}ms (${config.host}:${config.port})`));
    }, SMTP_TIMEOUT_MS);
    socket.once("error", (error) => done(error));
  });
}

function upgradeSmtpSocketToTls(socket, config) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let timer;
    const done = (error, tlsSocket) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (error) reject(error);
      else resolve(tlsSocket);
    };
    const tlsSocket = tls.connect(
      {
        socket,
        servername: config.host,
        rejectUnauthorized: !parseBooleanEnv(process.env.SMTP_TLS_ALLOW_UNAUTHORIZED, false)
      },
      () => done(null, tlsSocket)
    );
    timer = setTimeout(() => {
      tlsSocket.destroy();
      done(new Error(`SMTP STARTTLS timeout after ${SMTP_TIMEOUT_MS}ms (${config.host}:${config.port})`));
    }, SMTP_TIMEOUT_MS);
    tlsSocket.once("error", (error) => done(error));
  });
}

function encodeHeader(value) {
  if (/^[\x20-\x7E]*$/.test(value)) return value;
  return `=?UTF-8?B?${Buffer.from(value, "utf8").toString("base64")}?=`;
}

function dotStuff(value) {
  return String(value).replace(/\r?\n/g, "\r\n").replace(/^\./gm, "..");
}

function buildEmailMessage({ from, to, subject, body }) {
  const now = new Date().toUTCString();
  const messageIdHash = createHash("sha256").update(`${Date.now()}-${from}-${to.join(",")}`).digest("hex").slice(0, 24);
  const headers = [
    `From: <${from}>`,
    `To: ${to.map((recipient) => `<${recipient}>`).join(", ")}`,
    `Subject: ${encodeHeader(subject)}`,
    `Date: ${now}`,
    `Message-ID: <${messageIdHash}@vip-tools-mcp>`,
    "MIME-Version: 1.0",
    "Content-Type: text/plain; charset=utf-8",
    "Content-Transfer-Encoding: 8bit"
  ];
  return `${headers.join("\r\n")}\r\n\r\n${dotStuff(body)}`;
}

async function openAuthenticatedSmtpSession(config) {
  let socket;
  try {
    socket = await createSmtpSocket(config);
    let readResponse = createSmtpReader(socket);

    await expectSmtp(readResponse, [220], "connect");
    writeSmtp(socket, "EHLO vip-tools-mcp");
    await expectSmtp(readResponse, [250], "ehlo");

    if (!config.secure && config.port === 587) {
      writeSmtp(socket, "STARTTLS");
      await expectSmtp(readResponse, [220], "starttls");
      socket = await upgradeSmtpSocketToTls(socket, config);
      readResponse = createSmtpReader(socket);
      writeSmtp(socket, "EHLO vip-tools-mcp");
      await expectSmtp(readResponse, [250], "ehlo after starttls");
    }

    writeSmtp(socket, "AUTH LOGIN");
    await expectSmtp(readResponse, [334], "auth login");
    writeSmtp(socket, Buffer.from(config.user, "utf8").toString("base64"));
    await expectSmtp(readResponse, [334], "auth user");
    writeSmtp(socket, Buffer.from(config.password, "utf8").toString("base64"));
    await expectSmtp(readResponse, [235], "auth password");

    return { socket, readResponse };
  } catch (error) {
    if (socket && !socket.destroyed) socket.destroy();
    throw error;
  }
}

async function openAuthenticatedSmtpSessionWithFallback(configs) {
  const attempts = [];
  for (const config of configs) {
    try {
      const session = await openAuthenticatedSmtpSession(config);
      attempts.push({ ...publicSmtpConfig(config), ok: true });
      return { ...session, config, attempts };
    } catch (error) {
      attempts.push({ ...publicSmtpConfig(config), ok: false, error: String(error?.message || error) });
    }
  }

  const error = new Error(
    `SMTP-Authentifizierung fehlgeschlagen: ${attempts
      .map((attempt) => `${attempt.label} ${attempt.host}:${attempt.port} secure=${attempt.secure}: ${attempt.error}`)
      .join(" | ")}`
  );
  error.smtp_attempts = attempts;
  throw error;
}

function publicSmtpSessionResult(config, attempts) {
  return {
    host: config.host,
    port: config.port,
    secure: config.secure,
    label: config.label,
    attempts
  };
}

async function closeSmtpSession(socket, readResponse) {
  if (!socket || socket.destroyed) return;
  try {
    writeSmtp(socket, "QUIT");
    await expectSmtp(readResponse, [221], "quit");
  } catch {
    // The SMTP work is already done; a QUIT timeout must not turn a successful auth/send into a failure.
  } finally {
    socket.end();
  }
}

async function checkSmtpAuth(configs) {
  const { socket, readResponse, config, attempts } = await openAuthenticatedSmtpSessionWithFallback(configs);
  await closeSmtpSession(socket, readResponse);
  return publicSmtpSessionResult(config, attempts);
}

async function sendSmtpEmail(configs, { to, subject, body }) {
  const { socket, readResponse, config, attempts } = await openAuthenticatedSmtpSessionWithFallback(configs);

  try {
    writeSmtp(socket, `MAIL FROM:<${config.fromAddress}>`);
    await expectSmtp(readResponse, [250], "mail from");
    for (const recipient of to) {
      writeSmtp(socket, `RCPT TO:<${recipient}>`);
      await expectSmtp(readResponse, [250, 251], `rcpt to ${recipient}`);
    }
    writeSmtp(socket, "DATA");
    await expectSmtp(readResponse, [354], "data");
    socket.write(`${buildEmailMessage({ from: config.fromAddress, to, subject, body })}\r\n.\r\n`);
    await expectSmtp(readResponse, [250], "send data");
    await closeSmtpSession(socket, readResponse);
    return publicSmtpSessionResult(config, attempts);
  } catch (error) {
    if (!socket.destroyed) socket.destroy();
    throw error;
  }
}

function assertAsanaConfirmed(confirmedByAsana, asanaTaskGid, actionName) {
  if (!confirmedByAsana || !asanaTaskGid) {
    throw new Error(
      `${actionName} braucht eine klare Asana-Freigabe. Setze confirmed_by_asana=true und asana_task_gid.`
    );
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetryableAxiosError(error) {
  if (error.response?.status) {
    return error.response.status >= 500 && error.response.status < 600;
  }
  return Boolean(error.code && ["ECONNRESET", "ETIMEDOUT", "ENOTFOUND", "EAI_AGAIN"].includes(error.code));
}

function getDataForSeoConfigDetails(agentId, { requireCredentials = true } = {}) {
  const credentials = getDataForSeoCredentialSets(agentId);
  const primary = credentials.primary;
  const fallback = credentials.fallback;
  const readyCredentialSets = credentials.all.filter((credential) => credential.ready);

  if (requireCredentials && !readyCredentialSets.length) {
    throw new Error(
      `DataForSEO Zugangsdaten fehlen im MCP-Environment. Primaer: ${primary.loginEnvName}/${primary.passwordEnvName}; Fallback: ${fallback.loginEnvName}/${fallback.passwordEnvName}.`
    );
  }

  return {
    config: { login: primary.login, password: primary.password },
    credentialSets: readyCredentialSets,
    summary: {
      agent_id: agentId,
      dataforseo_api_base: DATAFORSEO_API_BASE,
      login_configured: primary.loginConfigured,
      login_env_name: primary.loginEnvName,
      password_configured: primary.passwordConfigured,
      password_env_name: primary.passwordConfigured ? primary.passwordEnvName : null,
      fallback_login_configured: fallback.loginConfigured,
      fallback_login_env_name: fallback.loginEnvName,
      fallback_password_configured: fallback.passwordConfigured,
      fallback_password_env_name: fallback.passwordConfigured ? fallback.passwordEnvName : null,
      primary_ready_for_live_calls: primary.ready,
      fallback_ready_for_live_calls: fallback.ready,
      ready_for_live_calls: Boolean(readyCredentialSets.length),
      fallback_available: fallback.ready,
      fallback_behavior: "Primaeren DataForSEO-Zugang nutzen; bei Auth-/Payment-/Limit-/Account-Blockern automatisch Fallback-Zugang versuchen.",
      default_location_code: DATAFORSEO_DEFAULT_LOCATION_CODE,
      default_language_code: DATAFORSEO_DEFAULT_LANGUAGE_CODE,
      max_results: DATAFORSEO_MAX_RESULTS,
      timeout_ms: DATAFORSEO_TIMEOUT_MS
    }
  };
}

function resolveDataForSeoCredentialSet(agentId, { fallback = false } = {}) {
  const suffix = envSuffixForAgent(agentId);
  const loginBaseName = fallback ? "DATAFORSEO_FALLBACK_LOGIN" : "DATAFORSEO_LOGIN";
  const passwordBaseName = fallback ? "DATAFORSEO_FALLBACK_PASSWORD" : "DATAFORSEO_PASSWORD";
  const agentLoginEnvName = `${loginBaseName}_${suffix}`;
  const agentPasswordEnvName = `${passwordBaseName}_${suffix}`;
  const loginEnvName = process.env[agentLoginEnvName] ? agentLoginEnvName : loginBaseName;
  const passwordEnvName = process.env[agentPasswordEnvName] ? agentPasswordEnvName : passwordBaseName;
  const login = process.env[loginEnvName] || "";
  const password = process.env[passwordEnvName] || "";

  return {
    label: fallback ? "fallback" : "primary",
    fallback,
    login,
    password,
    loginEnvName,
    passwordEnvName,
    loginConfigured: Boolean(login),
    passwordConfigured: Boolean(password),
    ready: Boolean(login && password)
  };
}

function getDataForSeoCredentialSets(agentId) {
  const primary = resolveDataForSeoCredentialSet(agentId);
  const fallback = resolveDataForSeoCredentialSet(agentId, { fallback: true });
  return {
    primary,
    fallback,
    all: [primary, fallback]
  };
}

function normalizeDataForSeoKeyword(value, { maxLength = 80, maxWords = 10 } = {}) {
  const keyword = String(value || "").trim();
  if (!keyword) throw new Error("Keyword darf nicht leer sein.");
  if (keyword.length > maxLength) {
    throw new Error(`Keyword ist zu lang (${keyword.length}/${maxLength} Zeichen): ${keyword.slice(0, 80)}`);
  }
  if (keyword.split(/\s+/).filter(Boolean).length > maxWords) {
    throw new Error(`Keyword hat zu viele Woerter (> ${maxWords}): ${keyword}`);
  }
  return keyword;
}

function normalizeDataForSeoKeywords(keywords, { maxCount, maxLength = 80, maxWords = 10 } = {}) {
  if (!Array.isArray(keywords) || !keywords.length) {
    throw new Error("keywords muss mindestens ein Keyword enthalten.");
  }
  if (keywords.length > maxCount) {
    throw new Error(`Zu viele Keywords: ${keywords.length}/${maxCount}.`);
  }
  const normalized = keywords.map((keyword) => normalizeDataForSeoKeyword(keyword, { maxLength, maxWords }));
  return [...new Set(normalized)];
}

function resolveDataForSeoTarget({ location_code, language_code }) {
  return {
    location_code: location_code || DATAFORSEO_DEFAULT_LOCATION_CODE,
    language_code: language_code || DATAFORSEO_DEFAULT_LANGUAGE_CODE
  };
}

function trimDataForSeoValue(value, maxItems) {
  if (Array.isArray(value)) {
    return value.slice(0, maxItems).map((item) => trimDataForSeoValue(item, maxItems));
  }
  if (!value || typeof value !== "object") return value;

  const output = {};
  for (const [key, nestedValue] of Object.entries(value)) {
    if (Array.isArray(nestedValue)) {
      output[key] = nestedValue.slice(0, maxItems).map((item) => trimDataForSeoValue(item, maxItems));
      if (nestedValue.length > maxItems) output[`${key}_truncated_count`] = nestedValue.length - maxItems;
    } else if (nestedValue && typeof nestedValue === "object") {
      output[key] = trimDataForSeoValue(nestedValue, maxItems);
    } else {
      output[key] = nestedValue;
    }
  }
  return output;
}

function maskSensitiveDataForSeoValue(value) {
  if (Array.isArray(value)) return value.map(maskSensitiveDataForSeoValue);
  if (!value || typeof value !== "object") return value;

  const output = {};
  for (const [key, nestedValue] of Object.entries(value)) {
    if (/password|token|secret|api[_-]?key/i.test(key)) {
      output[key] = nestedValue ? "[redacted]" : nestedValue;
    } else if (/login|email/i.test(key)) {
      output[key] = nestedValue ? "[configured]" : nestedValue;
    } else {
      output[key] = maskSensitiveDataForSeoValue(nestedValue);
    }
  }
  return output;
}

function compactDataForSeoResponse(response, { maxResults = DATAFORSEO_MAX_RESULTS } = {}) {
  return {
    version: response.version,
    status_code: response.status_code,
    status_message: response.status_message,
    time: response.time,
    cost: response.cost,
    credential_label: response._mcp_credential?.label,
    fallback_used: Boolean(response._mcp_credential?.fallback_used),
    tasks_count: response.tasks_count,
    tasks_error: response.tasks_error,
    tasks: (response.tasks || []).map((task) => ({
      id: task.id,
      status_code: task.status_code,
      status_message: task.status_message,
      time: task.time,
      cost: task.cost,
      result_count: task.result_count,
      path: task.path,
      data: task.data,
      result: trimDataForSeoValue(task.result || [], maxResults)
    }))
  };
}

function assertDataForSeoOk(response) {
  if (response.status_code !== 20000) {
    const error = new Error(`DataForSEO Fehler ${response.status_code}: ${response.status_message}`);
    error.dataForSeoResponse = response;
    error.dataForSeoStatusCode = response.status_code;
    throw error;
  }
  const failedTask = (response.tasks || []).find((task) => task.status_code && task.status_code !== 20000);
  if (failedTask) {
    const error = new Error(`DataForSEO Task-Fehler ${failedTask.status_code}: ${failedTask.status_message}`);
    error.dataForSeoResponse = response;
    error.dataForSeoTaskStatusCode = failedTask.status_code;
    throw error;
  }
}

async function dataForSeoRequest(agentId, { method = "POST", path, data }) {
  const { credentialSets } = getDataForSeoConfigDetails(agentId, { requireCredentials: true });
  let lastError;

  for (let index = 0; index < credentialSets.length; index += 1) {
    const credential = credentialSets[index];
    try {
      const res = await axios.request({
        method,
        url: `${DATAFORSEO_API_BASE}${path}`,
        auth: {
          username: credential.login,
          password: credential.password
        },
        headers: { "Content-Type": "application/json" },
        timeout: DATAFORSEO_TIMEOUT_MS,
        data
      });
      res.data._mcp_credential = {
        label: credential.label,
        fallback_used: credential.fallback
      };
      assertDataForSeoOk(res.data);
      return res.data;
    } catch (error) {
      lastError = error;
      const hasFallback = index < credentialSets.length - 1;
      if (!hasFallback || !shouldRetryDataForSeoWithFallback(error)) {
        throw error;
      }
    }
  }

  throw lastError;
}

function shouldRetryDataForSeoWithFallback(error) {
  const httpStatus = error.response?.status;
  if ([401, 402, 403, 429].includes(httpStatus)) return true;

  const statusCodes = [error.dataForSeoStatusCode, error.dataForSeoTaskStatusCode]
    .map((code) => Number(code))
    .filter(Number.isFinite);
  if (statusCodes.some((code) => [40100, 40200, 40201, 40202, 40300, 40301, 42900].includes(code))) {
    return true;
  }

  const message = `${error.message || ""} ${error.response?.data?.status_message || ""}`.toLowerCase();
  return /auth|login|credential|payment|required|cost.?limit|quota|paused|blocked|unusual|fraud|temporar/.test(message);
}

function summarizeDataForSeoRequests(requests) {
  return requests.map((request) => ({
    name: request.name,
    endpoint: request.path,
    max_results: request.maxResults,
    payload: request.data
  }));
}

function truncateString(value, maxChars) {
  const text = String(value ?? "");
  if (maxChars <= 0) return "";
  if (text.length <= maxChars) return text;
  return `${text.slice(0, Math.max(0, maxChars - 1))}…`;
}

function decodeHtmlEntities(value) {
  return String(value ?? "")
    .replace(/&#(\d+);/g, (match, code) => decodeHtmlCodePoint(Number(code), match))
    .replace(/&#x([0-9a-f]+);/gi, (match, code) => decodeHtmlCodePoint(parseInt(code, 16), match))
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");
}

function decodeHtmlCodePoint(codePoint, fallback) {
  if (!Number.isInteger(codePoint) || codePoint < 0 || codePoint > 0x10ffff) return fallback;
  try {
    return String.fromCodePoint(codePoint);
  } catch {
    return fallback;
  }
}

function stripHtml(value) {
  return decodeHtmlEntities(
    String(value ?? "")
      .replace(/<script\b[\s\S]*?<\/script>/gi, " ")
      .replace(/<style\b[\s\S]*?<\/style>/gi, " ")
      .replace(/<noscript\b[\s\S]*?<\/noscript>/gi, " ")
      .replace(/<!--[\s\S]*?-->/g, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim()
  );
}

function parseHtmlAttributes(tag) {
  const attrs = {};
  const openingTag = String(tag ?? "").match(/^<[^>]*>/)?.[0] || String(tag ?? "");
  const attrRegex = /([:\w-]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+)))?/g;
  for (const match of openingTag.matchAll(attrRegex)) {
    const name = match[1].toLowerCase();
    if (!name || name === "meta" || name === "link" || name === "script") continue;
    attrs[name] = decodeHtmlEntities(match[2] ?? match[3] ?? match[4] ?? "");
  }
  return attrs;
}

function firstTagText(html, tagName) {
  const match = new RegExp(`<${tagName}\\b[^>]*>([\\s\\S]*?)<\\/${tagName}>`, "i").exec(html);
  return match ? stripHtml(match[1]) : "";
}

function extractMetaTags(html) {
  const meta = {};
  const all = [];
  for (const match of html.matchAll(/<meta\b[^>]*>/gi)) {
    const attrs = parseHtmlAttributes(match[0]);
    const key = attrs.name || attrs.property || attrs["http-equiv"] || attrs.itemprop || "";
    const content = attrs.content || "";
    if (!key && !content) continue;
    const item = { key: key || null, content, attributes: attrs };
    all.push(item);
    if (key) {
      const normalizedKey = key.toLowerCase();
      if (meta[normalizedKey] === undefined) meta[normalizedKey] = content;
      else if (Array.isArray(meta[normalizedKey])) meta[normalizedKey].push(content);
      else meta[normalizedKey] = [meta[normalizedKey], content];
    }
  }
  return { meta, all };
}

function firstMetaValue(value) {
  if (Array.isArray(value)) return value.find((item) => String(item || "").trim()) || "";
  return value || "";
}

function extractLinkTags(html, finalUrl) {
  const links = [];
  for (const match of html.matchAll(/<link\b[^>]*>/gi)) {
    const attrs = parseHtmlAttributes(match[0]);
    if (!attrs.rel && !attrs.href) continue;
    links.push({
      rel: attrs.rel || "",
      href: absolutizeUrl(attrs.href || "", finalUrl),
      hreflang: attrs.hreflang || "",
      type: attrs.type || "",
      title: attrs.title || ""
    });
  }
  return links;
}

function extractHeadings(html, maxHeadings) {
  const headings = [];
  if (maxHeadings <= 0) return headings;
  for (const match of html.matchAll(/<h([1-6])\b[^>]*>([\s\S]*?)<\/h\1>/gi)) {
    headings.push({
      level: Number(match[1]),
      text: stripHtml(match[2])
    });
    if (headings.length >= maxHeadings) break;
  }
  return headings.filter((heading) => heading.text);
}

function absolutizeUrl(value, baseUrl) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  try {
    return new URL(raw, baseUrl).href;
  } catch {
    return raw;
  }
}

function extractPageLinks(html, finalUrl, maxLinks) {
  const links = [];
  if (maxLinks <= 0) return links;
  for (const match of html.matchAll(/<a\b[^>]*>([\s\S]*?)<\/a>/gi)) {
    const attrs = parseHtmlAttributes(match[0]);
    const href = absolutizeUrl(attrs.href || "", finalUrl);
    if (!href) continue;
    links.push({
      href,
      text: truncateString(stripHtml(match[1]), 140),
      rel: attrs.rel || ""
    });
    if (links.length >= maxLinks) break;
  }
  return links;
}

function extractJsonLd(html) {
  const scripts = [];
  for (const match of html.matchAll(/<script\b[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi)) {
    const raw = decodeHtmlEntities(match[1]).trim();
    if (!raw) continue;
    let parsed = null;
    let parse_error = null;
    try {
      parsed = JSON.parse(raw);
    } catch (error) {
      parse_error = String(error?.message || error);
    }
    scripts.push({
      parsed,
      parse_error,
      raw: parsed ? undefined : truncateString(raw, 4000)
    });
    if (scripts.length >= 20) break;
  }
  return scripts;
}

function parseSrcset(value, finalUrl, maxItems = 20) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .slice(0, maxItems)
    .map((item) => {
      const [url, descriptor] = item.split(/\s+/, 2);
      return {
        url: absolutizeUrl(url, finalUrl),
        descriptor: descriptor || ""
      };
    });
}

function extractImages(html, finalUrl, maxImages) {
  const images = [];
  if (maxImages <= 0) return images;
  for (const match of html.matchAll(/<img\b[^>]*>/gi)) {
    const attrs = parseHtmlAttributes(match[0]);
    const src = absolutizeUrl(attrs.src || attrs["data-src"] || "", finalUrl);
    const srcset = parseSrcset(attrs.srcset || attrs["data-srcset"] || "", finalUrl);
    if (!src && !srcset.length && !attrs.alt) continue;
    images.push({
      src,
      srcset,
      alt: attrs.alt || "",
      title: attrs.title || "",
      width: attrs.width || "",
      height: attrs.height || "",
      loading: attrs.loading || "",
      decoding: attrs.decoding || "",
      fetchpriority: attrs.fetchpriority || "",
      class: attrs.class || "",
      id: attrs.id || ""
    });
    if (images.length >= maxImages) break;
  }
  return images;
}

function extractParagraphs(html, maxParagraphs, paragraphMaxChars) {
  const paragraphs = [];
  if (maxParagraphs <= 0) return paragraphs;
  for (const match of html.matchAll(/<p\b[^>]*>([\s\S]*?)<\/p>/gi)) {
    const text = stripHtml(match[1]);
    if (!text) continue;
    paragraphs.push(truncateString(text, paragraphMaxChars));
    if (paragraphs.length >= maxParagraphs) break;
  }
  return paragraphs;
}

function extractSourceTags(html, finalUrl, maxSources = 40) {
  const sources = [];
  if (maxSources <= 0) return sources;
  for (const match of html.matchAll(/<source\b[^>]*>/gi)) {
    const attrs = parseHtmlAttributes(match[0]);
    const src = absolutizeUrl(attrs.src || "", finalUrl);
    const srcset = parseSrcset(attrs.srcset || "", finalUrl);
    if (!src && !srcset.length) continue;
    sources.push({
      src,
      srcset,
      media: attrs.media || "",
      type: attrs.type || "",
      sizes: attrs.sizes || ""
    });
    if (sources.length >= maxSources) break;
  }
  return sources;
}

function extractMicrodataHints(html, maxItems) {
  const hints = [];
  if (maxItems <= 0) return hints;
  for (const match of html.matchAll(/<([a-z][\w:-]*)\b[^>]*(?:itemscope|itemtype|itemprop)[^>]*>/gi)) {
    const attrs = parseHtmlAttributes(match[0]);
    hints.push({
      tag: match[1].toLowerCase(),
      itemtype: attrs.itemtype || "",
      itemprop: attrs.itemprop || "",
      itemscope: Object.prototype.hasOwnProperty.call(attrs, "itemscope"),
      content: attrs.content || "",
      href: attrs.href || "",
      src: attrs.src || ""
    });
    if (hints.length >= maxItems) break;
  }
  return hints;
}

function normalizeWebPageSections({ sections, include_html, include_text }) {
  const rawSections = Array.isArray(sections) && sections.length ? sections : WEB_PAGE_DEFAULT_SECTIONS;
  const selected = new Set(rawSections.map((section) => String(section).toLowerCase()));
  if (selected.has("all")) {
    selected.clear();
    for (const section of WEB_PAGE_SECTION_VALUES) {
      if (section !== "all") selected.add(section);
    }
  }
  if (include_html) selected.add("html");
  if (!include_text && (!Array.isArray(sections) || !sections.includes("text"))) selected.delete("text");
  return selected;
}

function normalizeWebUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) throw new Error("url darf nicht leer sein.");
  const withProtocol = /^https?:\/\//i.test(raw) ? raw : raw.startsWith("www.") ? `https://${raw}` : raw;
  let parsed;
  try {
    parsed = new URL(withProtocol);
  } catch {
    throw new Error(`Ungueltige URL: ${raw}`);
  }
  if (!["http:", "https:"].includes(parsed.protocol)) {
    throw new Error("Nur http(s)-URLs sind erlaubt.");
  }
  const hostname = parsed.hostname.toLowerCase();
  if (
    hostname === "localhost" ||
    hostname.endsWith(".localhost") ||
    hostname.endsWith(".local") ||
    hostname === "metadata.google.internal"
  ) {
    throw new Error("Lokale oder interne Hostnamen sind fuer web_page_inspect nicht erlaubt.");
  }
  const ipVersion = net.isIP(hostname);
  if (ipVersion === 4) {
    const parts = hostname.split(".").map(Number);
    const [a, b] = parts;
    if (
      a === 10 ||
      a === 127 ||
      a === 0 ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) ||
      (a === 169 && b === 254)
    ) {
      throw new Error("Private oder lokale IP-Adressen sind fuer web_page_inspect nicht erlaubt.");
    }
  }
  if (ipVersion === 6 && (hostname === "::1" || hostname.startsWith("fc") || hostname.startsWith("fd"))) {
    throw new Error("Private oder lokale IP-Adressen sind fuer web_page_inspect nicht erlaubt.");
  }
  return parsed.href;
}

function parseHttpHeaderLinks(headerValue) {
  const links = [];
  for (const part of String(headerValue || "").split(",")) {
    const match = /<([^>]+)>\s*;\s*rel="?([^";]+)"?/i.exec(part.trim());
    if (match) links.push({ href: match[1], rel: match[2] });
  }
  return links;
}

async function inspectWebPage({
  url,
  sections,
  include_html,
  html_max_chars,
  include_text,
  text_max_chars,
  max_headings,
  max_links,
  max_images,
  max_paragraphs,
  paragraph_max_chars,
  max_schema_items,
  timeout_ms
}) {
  const normalizedUrl = normalizeWebUrl(url);
  const res = await axios.get(normalizedUrl, {
    responseType: "text",
    transformResponse: [(data) => data],
    timeout: timeout_ms,
    maxContentLength: WEB_FETCH_MAX_BYTES,
    maxBodyLength: WEB_FETCH_MAX_BYTES,
    validateStatus: () => true,
    headers: {
      "User-Agent": WEB_FETCH_USER_AGENT,
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.5"
    }
  });

  const body = typeof res.data === "string" ? res.data : String(res.data || "");
  const finalUrl = res.request?.res?.responseUrl || normalizedUrl;
  const contentType = res.headers["content-type"] || "";
  const isHtml = /html|xml/i.test(contentType) || /<html[\s>]/i.test(body) || /<title[\s>]/i.test(body);
  const { meta, all } = isHtml ? extractMetaTags(body) : { meta: {}, all: [] };
  const linkTags = isHtml ? extractLinkTags(body, finalUrl) : [];
  const canonicalLink = linkTags.find((link) => /\bcanonical\b/i.test(link.rel));
  const alternateLinks = linkTags.filter((link) => /\balternate\b/i.test(link.rel));
  const robots = firstMetaValue(meta.robots) || firstMetaValue(meta["googlebot"]);
  const text = isHtml ? stripHtml(body) : String(body || "").replace(/\s+/g, " ").trim();
  const selectedSections = normalizeWebPageSections({ sections, include_html, include_text });
  const result = {
    requested_url: normalizedUrl,
    final_url: finalUrl,
    status: res.status,
    status_text: res.statusText,
    ok: res.status >= 200 && res.status < 400,
    fetched_at: new Date().toISOString(),
    content_type: contentType,
    content_length_header: res.headers["content-length"] || null,
    body_bytes: Buffer.byteLength(body, "utf8"),
    fetch_limit_bytes: WEB_FETCH_MAX_BYTES,
    extraction_sections: [...selectedSections],
    truncated_by_fetch_limit: false
  };

  if (selectedSections.has("seo")) {
    result.seo = {
      title: isHtml ? firstTagText(body, "title") : "",
      meta_description: firstMetaValue(meta.description),
      canonical: canonicalLink?.href || "",
      robots,
      viewport: firstMetaValue(meta.viewport),
      charset: /<meta\b[^>]*charset=["']?([^"'\s>]+)/i.exec(body)?.[1] || "",
      h1: isHtml ? extractHeadings(body, 10).filter((heading) => heading.level === 1).map((heading) => heading.text) : [],
      og_title: firstMetaValue(meta["og:title"]),
      og_description: firstMetaValue(meta["og:description"]),
      og_image: absolutizeUrl(firstMetaValue(meta["og:image"]), finalUrl),
      twitter_title: firstMetaValue(meta["twitter:title"]),
      twitter_description: firstMetaValue(meta["twitter:description"])
    };
  }
  if (selectedSections.has("meta")) {
    result.meta_tags = all;
    result.link_tags = linkTags.slice(0, 120);
  }
  if (selectedSections.has("headings")) {
    result.headings = isHtml ? extractHeadings(body, max_headings) : [];
  }
  if (selectedSections.has("links")) {
    result.alternates = alternateLinks.slice(0, 60);
    result.hreflang = alternateLinks.filter((link) => link.hreflang);
    result.http_link_headers = parseHttpHeaderLinks(res.headers.link);
    result.page_links = isHtml ? extractPageLinks(body, finalUrl, max_links) : [];
  }
  if (selectedSections.has("images")) {
    result.images = isHtml ? extractImages(body, finalUrl, max_images) : [];
    result.picture_sources = isHtml ? extractSourceTags(body, finalUrl, Math.min(max_images * 2, 80)) : [];
  }
  if (selectedSections.has("paragraphs")) {
    result.paragraphs = isHtml ? extractParagraphs(body, max_paragraphs, paragraph_max_chars) : [];
  }
  if (selectedSections.has("schema")) {
    const jsonLd = isHtml ? extractJsonLd(body).slice(0, max_schema_items) : [];
    const microdata = isHtml ? extractMicrodataHints(body, max_schema_items) : [];
    result.json_ld = jsonLd;
    result.schema = {
      json_ld: jsonLd,
      microdata
    };
  }
  if (selectedSections.has("text")) {
    result.text_excerpt = truncateString(text, text_max_chars);
    result.word_count_estimate = text ? text.split(/\s+/).filter(Boolean).length : 0;
  }
  if (selectedSections.has("html")) {
    result.html_source = truncateString(body, html_max_chars);
    result.html_source_truncated = body.length > html_max_chars;
  }

  return result;
}

function googleApiKeyFor(kind) {
  if (kind === "pagespeed") {
    return (
      process.env.PAGESPEED_API_KEY ||
      process.env.GOOGLE_PAGESPEED_API_KEY ||
      process.env.GOOGLE_API_KEY ||
      ""
    );
  }
  if (kind === "crux") {
    return process.env.CRUX_API_KEY || process.env.GOOGLE_CRUX_API_KEY || process.env.GOOGLE_API_KEY || "";
  }
  return process.env.GOOGLE_API_KEY || "";
}

function normalizePageSpeedCategory(category) {
  return String(category || "")
    .trim()
    .toLowerCase()
    .replace(/_/g, "-");
}

function pageSpeedCategoryParam(category) {
  return normalizePageSpeedCategory(category).toUpperCase().replace(/-/g, "_");
}

function compactAudit(audit) {
  if (!audit) return null;
  return {
    id: audit.id,
    title: audit.title,
    score: audit.score,
    score_display_mode: audit.scoreDisplayMode,
    display_value: audit.displayValue,
    numeric_value: audit.numericValue,
    numeric_unit: audit.numericUnit,
    description: truncateString(stripHtml(audit.description || ""), 500),
    details_type: audit.details?.type || null,
    overall_savings_ms: audit.details?.overallSavingsMs,
    overall_savings_bytes: audit.details?.overallSavingsBytes
  };
}

function compactPageSpeedResponse(data, { maxAudits = 20 } = {}) {
  const lighthouse = data?.lighthouseResult || {};
  const audits = lighthouse.audits || {};
  const categoryScores = {};
  for (const [key, category] of Object.entries(lighthouse.categories || {})) {
    categoryScores[key] = {
      title: category.title,
      score: typeof category.score === "number" ? Math.round(category.score * 100) : category.score
    };
  }

  const coreAuditIds = [
    "first-contentful-paint",
    "largest-contentful-paint",
    "interaction-to-next-paint",
    "total-blocking-time",
    "cumulative-layout-shift",
    "speed-index",
    "server-response-time",
    "render-blocking-resources",
    "unused-javascript",
    "unused-css-rules",
    "uses-optimized-images",
    "modern-image-formats"
  ];
  const coreAudits = coreAuditIds
    .map((id) => compactAudit({ id, ...(audits[id] || {}) }))
    .filter((audit) => audit?.title);

  const opportunities = Object.entries(audits)
    .map(([id, audit]) => compactAudit({ id, ...audit }))
    .filter((audit) => audit?.details_type === "opportunity")
    .sort((a, b) => (b.overall_savings_ms || 0) - (a.overall_savings_ms || 0))
    .slice(0, maxAudits);

  const failedAudits = Object.entries(audits)
    .map(([id, audit]) => compactAudit({ id, ...audit }))
    .filter((audit) => audit && audit.score !== null && audit.score !== undefined && audit.score < 1)
    .slice(0, maxAudits);

  return {
    requested_url: data?.id || null,
    final_url: lighthouse.finalUrl || lighthouse.requestedUrl || data?.id || null,
    analysis_utc: lighthouse.fetchTime || null,
    lighthouse_version: lighthouse.lighthouseVersion || null,
    user_agent: lighthouse.userAgent || null,
    category_scores: categoryScores,
    field_data: {
      loading_experience: data?.loadingExperience || null,
      origin_loading_experience: data?.originLoadingExperience || null
    },
    core_audits: coreAudits,
    opportunities,
    failed_audits: failedAudits
  };
}

function normalizeCruxTarget({ url, origin }) {
  if (url && origin) throw new Error("Bitte entweder url oder origin angeben, nicht beides.");
  if (url) return { url: normalizeWebUrl(url) };
  if (origin) {
    const normalized = normalizeWebUrl(origin);
    return { origin: new URL(normalized).origin };
  }
  throw new Error("Bitte url oder origin angeben.");
}

function buildCruxBody({ url, origin, form_factor, metrics, collection_period_count }) {
  const target = normalizeCruxTarget({ url, origin });
  const body = { ...target };
  if (form_factor) body.formFactor = form_factor;
  if (Array.isArray(metrics) && metrics.length) body.metrics = metrics;
  if (collection_period_count !== undefined) body.collectionPeriodCount = collection_period_count;
  return body;
}

async function callGoogleJsonApi({ method = "GET", url, params, body, timeout }) {
  const res = await axios.request({
    method,
    url,
    params,
    data: body,
    timeout,
    validateStatus: () => true,
    headers: {
      "User-Agent": WEB_FETCH_USER_AGENT,
      Accept: "application/json",
      "Content-Type": "application/json"
    }
  });
  return {
    ok: res.status >= 200 && res.status < 300,
    status: res.status,
    status_text: res.statusText,
    data: res.data
  };
}

function getPexelsConfigDetails(agentId, { requireCredentials = true } = {}) {
  const keyDetails = getScopedEnvDetails("PEXELS_API_KEY", agentId);
  if (requireCredentials && !keyDetails.value) {
    throw new Error(`Pexels API-Key fehlt. Setze ${keyDetails.agentEnvName} oder ${keyDetails.globalEnvName}.`);
  }

  return {
    config: { apiKey: keyDetails.value },
    summary: {
      agent_id: agentId,
      pexels_api_base: PEXELS_API_BASE,
      api_key_configured: keyDetails.configured,
      api_key_env_name: keyDetails.configured ? keyDetails.selectedEnvName : null,
      agent_api_key_env_name: keyDetails.agentEnvName,
      agent_api_key_configured: keyDetails.agentConfigured,
      global_api_key_env_name: keyDetails.globalEnvName,
      global_api_key_configured: keyDetails.globalConfigured,
      ready_for_read_calls: keyDetails.configured,
      timeout_ms: PEXELS_TIMEOUT_MS
    }
  };
}

async function pexelsRequest(agentId, { path, params = {}, timeout = PEXELS_TIMEOUT_MS }) {
  const { config } = getPexelsConfigDetails(agentId, { requireCredentials: true });
  const res = await axios.request({
    method: "GET",
    url: `${PEXELS_API_BASE}${path}`,
    params,
    timeout,
    validateStatus: () => true,
    headers: {
      Authorization: config.apiKey,
      Accept: "application/json",
      "User-Agent": WEB_FETCH_USER_AGENT
    }
  });

  return {
    ok: res.status >= 200 && res.status < 300,
    status: res.status,
    status_text: res.statusText,
    rate_limit: {
      limit: res.headers["x-ratelimit-limit"] || null,
      remaining: res.headers["x-ratelimit-remaining"] || null,
      reset: res.headers["x-ratelimit-reset"] || null
    },
    data: res.data
  };
}

function summarizePexelsPhoto(photo) {
  return {
    id: photo?.id,
    width: photo?.width,
    height: photo?.height,
    url: photo?.url,
    photographer: photo?.photographer,
    photographer_url: photo?.photographer_url,
    avg_color: photo?.avg_color,
    alt: photo?.alt,
    src: photo?.src
      ? {
          original: photo.src.original,
          large2x: photo.src.large2x,
          large: photo.src.large,
          medium: photo.src.medium,
          small: photo.src.small,
          portrait: photo.src.portrait,
          landscape: photo.src.landscape,
          tiny: photo.src.tiny
        }
      : undefined
  };
}

function getTemplatedConfigDetails(agentId, { requireCredentials = true } = {}) {
  const keyDetails = getScopedEnvDetails("TEMPLATED_API_KEY", agentId);
  if (requireCredentials && !keyDetails.value) {
    throw new Error(`Templated.io API-Key fehlt. Setze ${keyDetails.agentEnvName} oder ${keyDetails.globalEnvName}.`);
  }

  return {
    config: { apiKey: keyDetails.value },
    summary: {
      agent_id: agentId,
      templated_api_base: TEMPLATED_API_BASE,
      api_key_configured: keyDetails.configured,
      api_key_env_name: keyDetails.configured ? keyDetails.selectedEnvName : null,
      agent_api_key_env_name: keyDetails.agentEnvName,
      agent_api_key_configured: keyDetails.agentConfigured,
      global_api_key_env_name: keyDetails.globalEnvName,
      global_api_key_configured: keyDetails.globalConfigured,
      ready_for_read_calls: keyDetails.configured,
      ready_for_render_calls: keyDetails.configured,
      timeout_ms: TEMPLATED_TIMEOUT_MS
    }
  };
}

async function templatedRequest(agentId, { method = "GET", path, params, data, timeout = TEMPLATED_TIMEOUT_MS }) {
  const { config } = getTemplatedConfigDetails(agentId, { requireCredentials: true });
  const res = await axios.request({
    method,
    url: `${TEMPLATED_API_BASE}${path}`,
    params,
    data,
    timeout,
    validateStatus: () => true,
    headers: {
      Authorization: `Bearer ${config.apiKey}`,
      Accept: "application/json",
      "Content-Type": "application/json",
      "User-Agent": WEB_FETCH_USER_AGENT
    }
  });

  return {
    ok: res.status >= 200 && res.status < 300,
    status: res.status,
    status_text: res.statusText,
    data: res.data
  };
}

function getTemplatedItems(data) {
  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.data)) return data.data;
  if (Array.isArray(data?.templates)) return data.templates;
  if (Array.isArray(data?.items)) return data.items;
  if (Array.isArray(data?.results)) return data.results;
  return [];
}

function summarizeTemplatedTemplate(template) {
  return {
    id: template?.id,
    name: template?.name,
    description: template?.description,
    width: template?.width,
    height: template?.height,
    thumbnail: template?.thumbnail,
    layersCount: template?.layersCount ?? template?.layers_count,
    folderId: template?.folderId ?? template?.folder_id,
    tags: template?.tags
  };
}

function maybeSetField(target, key, value) {
  if (value !== undefined) target[key] = value;
}

function sha256Json(value) {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

function resolveFreepikKeyDetails(agentId) {
  const freepik = getScopedEnvDetails("FREEPIK_API_KEY", agentId);
  if (freepik.value) return { provider: "freepik", ...freepik };
  const magnific = getScopedEnvDetails("MAGNIFIC_API_KEY", agentId);
  if (magnific.value) return { provider: "magnific", ...magnific };
  return { provider: "freepik", ...freepik, magnific };
}

function getFreepikConfigDetails(agentId, { requireCredentials = true } = {}) {
  const keyDetails = resolveFreepikKeyDetails(agentId);
  if (requireCredentials && !keyDetails.value) {
    throw new Error(
      `Freepik API-Key fehlt. Setze ${keyDetails.agentEnvName} oder ${keyDetails.globalEnvName}; alternativ MAGNIFIC_API_KEY fuer die aktuelle Magnific/Freepik-Doku.`
    );
  }

  const apiBase =
    keyDetails.provider === "magnific" && !process.env.FREEPIK_API_BASE ? MAGNIFIC_API_BASE : FREEPIK_DEFAULT_API_BASE;
  const headerName =
    process.env.FREEPIK_API_HEADER ||
    process.env.FREEPIK_API_KEY_HEADER ||
    (keyDetails.provider === "magnific" ? "x-magnific-api-key" : "x-freepik-api-key");

  return {
    config: { apiKey: keyDetails.value, apiBase, headerName, provider: keyDetails.provider },
    summary: {
      agent_id: agentId,
      provider: keyDetails.provider,
      freepik_api_base: apiBase,
      api_key_configured: keyDetails.configured,
      api_key_env_name: keyDetails.configured ? keyDetails.selectedEnvName : null,
      api_key_header: headerName,
      agent_api_key_env_name: keyDetails.agentEnvName,
      agent_api_key_configured: keyDetails.agentConfigured,
      global_api_key_env_name: keyDetails.globalEnvName,
      global_api_key_configured: keyDetails.globalConfigured,
      magnific_fallback_supported: true,
      ready_for_read_calls: keyDetails.configured,
      ready_for_download_calls: keyDetails.configured,
      timeout_ms: FREEPIK_TIMEOUT_MS
    }
  };
}

async function freepikRequest(agentId, { method = "GET", path, params, data, timeout = FREEPIK_TIMEOUT_MS }) {
  const { config } = getFreepikConfigDetails(agentId, { requireCredentials: true });
  const res = await axios.request({
    method,
    url: `${config.apiBase}${path}`,
    params,
    data,
    timeout,
    validateStatus: () => true,
    headers: {
      [config.headerName]: config.apiKey,
      Accept: "application/json",
      "Content-Type": "application/json",
      "User-Agent": WEB_FETCH_USER_AGENT
    }
  });

  return {
    ok: res.status >= 200 && res.status < 300,
    status: res.status,
    status_text: res.statusText,
    data: res.data
  };
}

function getFreepikItems(data) {
  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.data)) return data.data;
  if (Array.isArray(data?.items)) return data.items;
  if (Array.isArray(data?.resources)) return data.resources;
  if (Array.isArray(data?.results)) return data.results;
  return [];
}

function summarizeFreepikResource(resource) {
  return {
    id: resource?.id,
    title: resource?.title,
    name: resource?.name,
    type: resource?.type,
    url: resource?.url,
    thumbnail: resource?.thumbnail || resource?.image?.source?.url || resource?.preview?.url,
    author: resource?.author
      ? {
          id: resource.author.id,
          name: resource.author.name,
          username: resource.author.username,
          url: resource.author.url
        }
      : undefined,
    license: resource?.license,
    premium: resource?.premium,
    width: resource?.width,
    height: resource?.height,
    tags: resource?.tags,
    available_formats: resource?.available_formats || resource?.formats
  };
}

function getUnsplashConfigDetails(agentId, { requireCredentials = true } = {}) {
  const keyDetails = getScopedEnvDetails("UNSPLASH_ACCESS_KEY", agentId);
  const secretDetails = getScopedEnvDetails("UNSPLASH_SECRET_KEY", agentId);
  if (requireCredentials && !keyDetails.value) {
    throw new Error(`Unsplash Access Key fehlt. Setze ${keyDetails.agentEnvName} oder ${keyDetails.globalEnvName}.`);
  }

  return {
    config: { accessKey: keyDetails.value, secretKey: secretDetails.value },
    summary: {
      agent_id: agentId,
      unsplash_api_base: UNSPLASH_API_BASE,
      access_key_configured: keyDetails.configured,
      access_key_env_name: keyDetails.configured ? keyDetails.selectedEnvName : null,
      agent_access_key_env_name: keyDetails.agentEnvName,
      agent_access_key_configured: keyDetails.agentConfigured,
      global_access_key_env_name: keyDetails.globalEnvName,
      global_access_key_configured: keyDetails.globalConfigured,
      secret_key_configured: secretDetails.configured,
      secret_key_env_name: secretDetails.configured ? secretDetails.selectedEnvName : null,
      ready_for_read_calls: keyDetails.configured,
      timeout_ms: UNSPLASH_TIMEOUT_MS
    }
  };
}

async function unsplashRequest(agentId, { method = "GET", path, params, data, timeout = UNSPLASH_TIMEOUT_MS }) {
  const { config } = getUnsplashConfigDetails(agentId, { requireCredentials: true });
  const res = await axios.request({
    method,
    url: `${UNSPLASH_API_BASE}${path}`,
    params,
    data,
    timeout,
    validateStatus: () => true,
    headers: {
      Authorization: `Client-ID ${config.accessKey}`,
      Accept: "application/json",
      "Content-Type": "application/json",
      "User-Agent": WEB_FETCH_USER_AGENT
    }
  });

  return {
    ok: res.status >= 200 && res.status < 300,
    status: res.status,
    status_text: res.statusText,
    rate_limit: {
      limit: res.headers["x-ratelimit-limit"] || null,
      remaining: res.headers["x-ratelimit-remaining"] || null
    },
    data: res.data
  };
}

function summarizeUnsplashPhoto(photo) {
  return {
    id: photo?.id,
    slug: photo?.slug,
    created_at: photo?.created_at,
    updated_at: photo?.updated_at,
    width: photo?.width,
    height: photo?.height,
    color: photo?.color,
    blur_hash: photo?.blur_hash,
    description: photo?.description,
    alt_description: photo?.alt_description,
    urls: photo?.urls
      ? {
          raw: photo.urls.raw,
          full: photo.urls.full,
          regular: photo.urls.regular,
          small: photo.urls.small,
          thumb: photo.urls.thumb
        }
      : undefined,
    links: photo?.links
      ? {
          self: photo.links.self,
          html: photo.links.html,
          download: photo.links.download,
          download_location: photo.links.download_location
        }
      : undefined,
    user: photo?.user
      ? {
          id: photo.user.id,
          username: photo.user.username,
          name: photo.user.name,
          portfolio_url: photo.user.portfolio_url,
          links: photo.user.links
            ? {
                html: photo.user.links.html
              }
            : undefined
        }
      : undefined
  };
}

/* ---------------- MCP SERVER FACTORY ---------------- */

function createServer() {
  const server = new McpServer({
    name: "vip-tools-remote",
    version: "1.0.0"
  });

  server.tool(
    "asana_list_agents",
    "Listet die im Remote-MCP bekannten Agenten und ob ein Token konfiguriert ist. Gibt keine Secret-Werte aus.",
    {},
    TOOL_READ_ONLY,
    async () => {
      return out(
        Object.entries(ASANA_TOKEN_ENVS).map(([agent_id, env_name]) => ({
          agent_id,
          env_name,
          token_configured: Boolean(process.env[env_name])
        }))
      );
    }
  );

  server.tool(
    "asana_whoami",
    "Prueft den Asana-Token eines Agenten und gibt den zugehoerigen User ohne Secret-Werte aus.",
    { agent_id: agentIdSchema },
    TOOL_READ_ONLY,
    async ({ agent_id }) => {
      const asana = getAsana(agent_id);
      const res = await asana.get("/users/me");
      return out({ agent_id, user: res.data.data });
    }
  );

  server.tool(
    "asana_get_my_tasks",
    "Liest offene Tasks",
    {
      agent_id: agentIdSchema,
      limit: z.number().optional().default(20)
    },
    TOOL_READ_ONLY,
    async ({ agent_id, limit }) => {
      const asana = getAsana(agent_id);
      const user = await asana.get("/users/me");
      const workspace = user.data.data.workspaces[0].gid;

      const res = await asana.get("/tasks", {
        params: {
          assignee: "me",
          workspace,
          completed_since: "now",
          limit
        }
      });

      return out({ agent_id, tasks: res.data.data });
    }
  );

  server.tool(
    "asana_comment",
    "Postet einen Asana-Kommentar ueber ein enges Rich-Text-Schema. Kein rohes HTML: Das Tool baut valides Asana-Rich-Text-Markup, echte GID-Mentions, Listen und bei Bedarf Code-Bloecke selbst und prueft den Readback.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string(),
      greeting: z.string().optional(),
      sections: z
        .array(
          z.object({
            title: z.string().optional(),
            paragraphs: z.array(z.string()).optional().default([]),
            bullets: z.array(z.string()).optional().default([]),
            code_blocks: z.array(z.string()).optional().default([])
          })
        )
        .optional()
        .default([]),
      mention_user_gid: z.string().optional(),
      mention_text: z.string().optional(),
      effort_note: z.string().optional(),
      dry_run: z.boolean().optional().default(false),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_SAFE_WRITE,
    async ({
      agent_id,
      task_gid,
      greeting,
      sections,
      mention_user_gid,
      mention_text,
      effort_note,
      dry_run,
      verify_after
    }) => {
      validateAsanaGid(task_gid, "task_gid");
      const html_text = buildAsanaCommentHtml({
        greeting,
        sections,
        mention_user_gid,
        mention_text,
        effort_note
      });
      const expectedCodeSnippets = collectAsanaCodeBlocks(sections);
      const html_sha256 = createHash("sha256").update(html_text, "utf8").digest("hex");

      if (dry_run) {
        return out({
          agent_id,
          dry_run: true,
          task_gid,
          html_text,
          html_bytes: Buffer.byteLength(html_text, "utf8"),
          html_sha256
        });
      }

      const asana = getAsana(agent_id);
      const res = await asana.post(
        `/tasks/${task_gid}/stories`,
        { data: { html_text } },
        { params: { opt_fields: "gid,text,html_text,created_at,created_by" } }
      );

      let verified_story;
      let verification_status = "not_requested";
      let verification_error;
      if (verify_after && res.data.data?.gid) {
        try {
          const verify = await asana.get(`/stories/${res.data.data.gid}`, {
            params: { opt_fields: "gid,text,html_text,created_at,created_by" }
          });
          verified_story = verify.data.data;
          assertAsanaStoryReadbackLooksSafe(verified_story, expectedCodeSnippets);
          verification_status = "ok";
        } catch (error) {
          verification_status = "posted_but_verification_failed_do_not_retry";
          verification_error = error?.message || String(error);
        }
      }

      return out({
        agent_id,
        task_gid,
        story: res.data.data,
        verified_story,
        verification_status,
        verification_error,
        must_not_retry_comment: verification_status === "posted_but_verification_failed_do_not_retry",
        html_bytes: Buffer.byteLength(html_text, "utf8"),
        html_sha256
      });
    }
  );

  server.tool(
    "asana_complete_task",
    "Schliesst eine Asana-Aufgabe kontrolliert ab. Nur fuer eigene zugewiesene Aufgaben nach erfolgreicher Bearbeitung; prueft Assignee, optional finalen Kommentar und Readback.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string(),
      completion_basis: z.string().min(20),
      final_comment_story_gid: z.string().optional(),
      require_final_comment: z.boolean().optional().default(true),
      dry_run: z.boolean().optional().default(false),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_IDEMPOTENT_SAFE_WRITE,
    async ({
      agent_id,
      task_gid,
      completion_basis,
      final_comment_story_gid,
      require_final_comment,
      dry_run,
      verify_after
    }) => {
      validateAsanaGid(task_gid, "task_gid");
      if (final_comment_story_gid) validateAsanaGid(final_comment_story_gid, "final_comment_story_gid");
      if (require_final_comment && !final_comment_story_gid) {
        throw new Error(
          "asana_complete_task braucht final_comment_story_gid, wenn require_final_comment=true ist. Poste zuerst einen Abschlusskommentar mit asana_comment."
        );
      }

      const asana = getAsana(agent_id);
      const me = await asana.get("/users/me");
      const meGid = me.data.data.gid;

      const taskRes = await asana.get(`/tasks/${task_gid}`, {
        params: {
          opt_fields: "gid,name,completed,completed_at,assignee.gid,assignee.name,tags.name,permalink_url"
        }
      });
      const task = taskRes.data.data;

      if (!task.assignee?.gid) {
        throw new Error("Asana-Aufgabe hat keinen Assignee; automatischer Abschluss ist nicht erlaubt.");
      }
      if (task.assignee.gid !== meGid) {
        throw new Error(
          `Asana-Aufgabe ist ${task.assignee.name || task.assignee.gid} zugewiesen, nicht dem ausfuehrenden Agenten ${meGid}.`
        );
      }

      let final_comment;
      if (final_comment_story_gid) {
        const storyRes = await asana.get(`/stories/${final_comment_story_gid}`, {
          params: { opt_fields: "gid,text,html_text,created_at,created_by.gid,created_by.name" }
        });
        final_comment = storyRes.data.data;
        if (!final_comment.created_by?.gid) {
          throw new Error("Finaler Asana-Kommentar hat keinen erkennbaren Autor.");
        }
        if (final_comment.created_by.gid !== meGid) {
          throw new Error("Finaler Asana-Kommentar wurde nicht vom ausfuehrenden Agenten erstellt.");
        }
        assertAsanaStoryReadbackLooksSafe(final_comment);
      }

      const basis_sha256 = createHash("sha256").update(completion_basis, "utf8").digest("hex");

      if (dry_run || task.completed) {
        return out({
          agent_id,
          dry_run,
          already_completed: Boolean(task.completed),
          allowed_to_complete: !task.completed,
          task,
          final_comment,
          completion_basis,
          completion_basis_sha256: basis_sha256
        });
      }

      const completeRes = await asana.put(
        `/tasks/${task_gid}`,
        { data: { completed: true } },
        { params: { opt_fields: "gid,name,completed,completed_at,assignee.gid,assignee.name,permalink_url" } }
      );

      let verified_task;
      let verification_status = "not_requested";
      if (verify_after) {
        const verify = await asana.get(`/tasks/${task_gid}`, {
          params: { opt_fields: "gid,name,completed,completed_at,assignee.gid,assignee.name,permalink_url" }
        });
        verified_task = verify.data.data;
        if (!verified_task.completed) {
          throw new Error("Asana-Readback zeigt completed=false nach Abschlussversuch.");
        }
        if (verified_task.assignee?.gid !== meGid) {
          throw new Error("Asana-Readback zeigt veraenderten Assignee nach Abschlussversuch.");
        }
        verification_status = "ok";
      }

      return out({
        agent_id,
        task_gid,
        completed: true,
        task: completeRes.data.data,
        verified_task,
        verification_status,
        final_comment,
        completion_basis,
        completion_basis_sha256: basis_sha256
      });
    }
  );

  server.tool(

      "asana_request",

      "Führt einen beliebigen Asana-API-Request mit dem Token des angegebenen Agenten aus. Ohne agent_id wird VIP AI-Sales verwendet.",

      {

        agent_id: agentIdSchema,

        method: z.enum(["GET", "POST", "PUT", "DELETE"]),

        path: z.string(),

        params: z.record(z.string(), z.any()).optional(),

        data: z.record(z.string(), z.any()).optional()

      },

      async ({ agent_id, method, path, params, data }) => {

        if (!path.startsWith("/")) {

          throw new Error("path muss mit / beginnen, z. B. /tasks");

        }

        assertSafeAsanaRequest(method, path, data);

        const asana = getAsana(agent_id);

        const res = await asana.request({

          method,

          url: path,

          params,

          data: data ? { data } : undefined

        });

        return out({ agent_id, response: res.data });

      }

    );

  server.tool(
    "asana_attachment_fetch",
    "Liest Asana-Anhaenge read-only ueber den Remote-MCP. Nutzt Asana-API und serverseitigen Download, damit lokale asanausercontent.com-DNS-Blocker nicht den Agentenlauf blockieren.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string().optional(),
      attachment_gid: z.string().optional(),
      attachment_name_contains: z.string().optional(),
      download_first_match: z.boolean().optional().default(false),
      include_base64: z.boolean().optional().default(true),
      include_text_preview: z.boolean().optional().default(true),
      text_preview_chars: z.number().int().min(100).max(20000).optional().default(4000),
      max_bytes: z
        .number()
        .int()
        .min(1024)
        .max(ASANA_ATTACHMENT_HARD_MAX_BYTES)
        .optional()
        .default(ASANA_ATTACHMENT_DEFAULT_MAX_BYTES)
    },
    TOOL_EXTERNAL_READ,
    async ({
      agent_id,
      task_gid,
      attachment_gid,
      attachment_name_contains,
      download_first_match,
      include_base64,
      include_text_preview,
      text_preview_chars,
      max_bytes
    }) => {
      if (!task_gid && !attachment_gid) {
        throw new Error("task_gid oder attachment_gid ist erforderlich.");
      }
      if (task_gid) validateAsanaGid(task_gid, "task_gid");
      if (attachment_gid) validateAsanaGid(attachment_gid, "attachment_gid");

      const asana = getAsana(agent_id);
      let attachments = [];
      let selectedAttachmentGid = attachment_gid;

      if (task_gid) {
        attachments = await listAsanaTaskAttachments(asana, task_gid);
        if (attachment_name_contains) {
          const needle = attachment_name_contains.toLowerCase();
          attachments = attachments.filter((attachment) =>
            String(attachment.name || "").toLowerCase().includes(needle)
          );
        }
        if (!selectedAttachmentGid && download_first_match && attachments.length) {
          selectedAttachmentGid = attachments[0].gid;
        }
      }

      if (!selectedAttachmentGid) {
        return out({
          agent_id,
          task_gid,
          attachment_name_contains: attachment_name_contains || null,
          returned_count: attachments.length,
          attachments,
          downloaded: false,
          next_step:
            "Mit attachment_gid erneut aufrufen oder download_first_match=true setzen, wenn der erste Treffer bewusst geladen werden soll."
        });
      }

      const { attachment, bytes, content_type, content_length_header } = await downloadAsanaAttachment(
        asana,
        selectedAttachmentGid,
        max_bytes
      );
      const sha256 = createHash("sha256").update(bytes).digest("hex");
      const textPreview =
        include_text_preview && isTextLikeAttachment(content_type, attachment.name)
          ? decodeTextPreview(bytes, text_preview_chars)
          : undefined;

      return out({
        agent_id,
        task_gid: task_gid || attachment.parent?.gid || null,
        attachment,
        downloaded: true,
        content_type,
        content_length_header,
        byte_length: bytes.length,
        sha256,
        text_preview: textPreview,
        content_base64: include_base64 ? bytes.toString("base64") : undefined,
        save_hint:
          "Base64-Inhalt in AGENT_WORKSPACE/temp speichern und danach mit passendem Dokument-/Tabellen-/PDF-Tool auswerten."
      });
    }
  );

  server.tool(
    "asana_assign_task",
    "Weist eine Asana-Aufgabe kontrolliert einem konkreten Asana-User per GID zu. Blockiert Routine-Aufgaben und verifiziert den Readback.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string(),
      assignee_gid: z.string(),
      dry_run: z.boolean().optional().default(false),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_IDEMPOTENT_SAFE_WRITE,
    async ({ agent_id, task_gid, assignee_gid, dry_run, verify_after }) => {
      validateAsanaGid(task_gid, "task_gid");
      validateAsanaGid(assignee_gid, "assignee_gid");
      const asana = getAsana(agent_id);

      const beforeRes = await asana.get(`/tasks/${task_gid}`, {
        params: { opt_fields: "gid,name,completed,assignee.gid,assignee.name,tags.name,permalink_url" }
      });
      const before_task = beforeRes.data.data;
      const tagNames = (before_task.tags || []).map((tag) => tag.name);
      if (tagNames.includes("Routine")) {
        throw new Error(
          "Routine-Aufgaben duerfen nicht umgewiesen werden. Assignee unveraendert lassen und Rueckfragen per asana_comment mit echter Mention klaeren."
        );
      }

      if (dry_run) {
        return out({
          agent_id,
          dry_run: true,
          task_gid,
          assignee_gid,
          before_task,
          allowed_to_assign: true
        });
      }

      const res = await asana.put(
        `/tasks/${task_gid}`,
        { data: { assignee: assignee_gid } },
        { params: { opt_fields: "gid,name,completed,assignee.gid,assignee.name,tags.name,permalink_url" } }
      );

      let verified_task;
      let verification_status = "not_requested";
      if (verify_after) {
        const verify = await asana.get(`/tasks/${task_gid}`, {
          params: { opt_fields: "gid,name,completed,assignee.gid,assignee.name,tags.name,permalink_url" }
        });
        verified_task = verify.data.data;
        if (verified_task.assignee?.gid !== assignee_gid) {
          throw new Error("Asana-Readback zeigt nicht den erwarteten Assignee nach Re-Assign.");
        }
        verification_status = "ok";
      }

      return out({
        agent_id,
        task_gid,
        assignee_gid,
        before_task,
        task: res.data.data,
        verified_task,
        verification_status
      });
    }
  );

  server.tool(
    "web_page_inspect",
    "Ruft eine oeffentliche Webseite read-only ab. Der Agent waehlt per sections gezielt aus: SEO/Core, Meta-/Link-Tags, H-Ueberschriften, Links/Hreflang, Bilder/ALT, Paragraphen, Schema/JSON-LD/Microdata, Textauszug oder begrenzten HTML-Quellcode.",
    {
      url: z.string(),
      sections: z.array(z.enum(WEB_PAGE_SECTION_VALUES)).optional(),
      include_html: z.boolean().optional().default(false),
      html_max_chars: z.number().int().min(0).max(200_000).optional().default(20_000),
      include_text: z.boolean().optional().default(true),
      text_max_chars: z.number().int().min(0).max(30_000).optional().default(5_000),
      max_headings: z.number().int().min(0).max(100).optional().default(40),
      max_links: z.number().int().min(0).max(200).optional().default(50),
      max_images: z.number().int().min(0).max(300).optional().default(80),
      max_paragraphs: z.number().int().min(0).max(200).optional().default(50),
      paragraph_max_chars: z.number().int().min(20).max(2000).optional().default(500),
      max_schema_items: z.number().int().min(0).max(80).optional().default(20),
      timeout_ms: z.number().int().min(1_000).max(30_000).optional().default(WEB_FETCH_TIMEOUT_MS)
    },
    TOOL_EXTERNAL_READ,
    async ({
      url,
      sections,
      include_html,
      html_max_chars,
      include_text,
      text_max_chars,
      max_headings,
      max_links,
      max_images,
      max_paragraphs,
      paragraph_max_chars,
      max_schema_items,
      timeout_ms
    }) => {
      const result = await inspectWebPage({
        url,
        sections,
        include_html,
        html_max_chars,
        include_text,
        text_max_chars,
        max_headings,
        max_links,
        max_images,
        max_paragraphs,
        paragraph_max_chars,
        max_schema_items,
        timeout_ms
      });
      return out(result);
    }
  );

  server.tool(
    "seo_google_api_check_config",
    "Prueft allgemeine Google-SEO-API-Konfigurationen fuer PageSpeed Insights und CrUX ohne URL-Abfrage. Gibt keine Secret-Werte aus.",
    {},
    TOOL_EXTERNAL_READ,
    async () => {
      return out({
        pagespeed: {
          api_base: PAGESPEED_API_BASE,
          timeout_ms: PAGESPEED_TIMEOUT_MS,
          key_configured: Boolean(googleApiKeyFor("pagespeed")),
          key_env_candidates: ["PAGESPEED_API_KEY", "GOOGLE_PAGESPEED_API_KEY", "GOOGLE_API_KEY"],
          note:
            "PageSpeed Insights kann je nach Google-Quota auch ohne Key funktionieren; fuer stabile Nutzung ist ein API-Key empfohlen."
        },
        crux: {
          api_base: CRUX_API_BASE,
          timeout_ms: CRUX_TIMEOUT_MS,
          key_configured: Boolean(googleApiKeyFor("crux")),
          key_env_candidates: ["CRUX_API_KEY", "GOOGLE_CRUX_API_KEY", "GOOGLE_API_KEY"],
          note: "CrUX API und CrUX History API benoetigen einen Google Cloud API-Key mit Chrome UX Report API."
        }
      });
    }
  );

  server.tool(
    "seo_pagespeed_api",
    "Ruft Google PageSpeed Insights fuer oeffentliche URLs ab und liefert kompakte Lighthouse-/CrUX-/Core-Web-Vitals-Daten. Read-only; jede URL wird einzeln analysiert.",
    {
      urls: z.array(z.string()).min(1).max(10),
      strategy: z.enum(PAGESPEED_STRATEGY_VALUES).optional().default("mobile"),
      categories: z
        .array(z.enum(PAGESPEED_CATEGORY_VALUES))
        .min(1)
        .max(PAGESPEED_CATEGORY_VALUES.length)
        .optional()
        .default(["performance", "accessibility", "best-practices", "seo"]),
      locale: z.string().min(2).max(20).optional().default("de"),
      timeout_ms: z.number().int().min(10_000).max(120_000).optional().default(PAGESPEED_TIMEOUT_MS),
      max_audits: z.number().int().min(0).max(100).optional().default(20)
    },
    TOOL_EXTERNAL_READ,
    async ({ urls, strategy, categories, locale, timeout_ms, max_audits }) => {
      const apiKey = googleApiKeyFor("pagespeed");
      const items = [];
      for (const inputUrl of urls) {
        const normalizedUrl = normalizeWebUrl(inputUrl);
        const params = new URLSearchParams();
        params.set("url", normalizedUrl);
        params.set("strategy", strategy);
        params.set("locale", locale);
        for (const category of categories) params.append("category", pageSpeedCategoryParam(category));
        if (apiKey) params.set("key", apiKey);

        const result = await callGoogleJsonApi({
          url: `${PAGESPEED_API_BASE}/runPagespeed?${params.toString()}`,
          timeout: timeout_ms
        });

        items.push({
          url: normalizedUrl,
          ok: result.ok,
          status: result.status,
          data: result.ok ? compactPageSpeedResponse(result.data, { maxAudits: max_audits }) : undefined,
          error: result.ok ? undefined : result.data
        });
      }

      return out({
        tool: "seo_pagespeed_api",
        input: {
          urls: urls.length,
          strategy,
          categories,
          locale,
          key_configured: Boolean(apiKey)
        },
        summary: {
          ok_count: items.filter((item) => item.ok).length,
          error_count: items.filter((item) => !item.ok).length
        },
        items
      });
    }
  );

  server.tool(
    "seo_crux_api",
    "Ruft die Chrome UX Report API fuer reale Web-Vitals-Felddaten einer oeffentlichen URL oder Origin ab. Benoetigt einen CrUX/Google API-Key.",
    {
      url: z.string().optional(),
      origin: z.string().optional(),
      form_factor: z.enum(CRUX_FORM_FACTOR_VALUES).optional(),
      metrics: z.array(z.string()).min(1).max(20).optional(),
      timeout_ms: z.number().int().min(5_000).max(60_000).optional().default(CRUX_TIMEOUT_MS)
    },
    TOOL_EXTERNAL_READ,
    async ({ url, origin, form_factor, metrics, timeout_ms }) => {
      const apiKey = googleApiKeyFor("crux");
      if (!apiKey) {
        throw new Error("CrUX API-Key fehlt. Setze CRUX_API_KEY, GOOGLE_CRUX_API_KEY oder GOOGLE_API_KEY.");
      }
      const body = buildCruxBody({ url, origin, form_factor, metrics });
      const result = await callGoogleJsonApi({
        method: "POST",
        url: `${CRUX_API_BASE}/records:queryRecord`,
        params: { key: apiKey },
        body,
        timeout: timeout_ms
      });

      return out({
        tool: "seo_crux_api",
        ok: result.ok,
        status: result.status,
        request: { ...body, key_configured: true },
        response: result.data,
        note: result.ok
          ? undefined
          : "Wenn status 404 oder keine Daten geliefert werden, hat CrUX fuer diese URL/Origin moeglicherweise nicht genug reale Nutzerdaten."
      });
    }
  );

  server.tool(
    "seo_crux_history",
    "Ruft die CrUX History API fuer Web-Vitals-Trends einer oeffentlichen URL oder Origin ab. Benoetigt einen CrUX/Google API-Key.",
    {
      url: z.string().optional(),
      origin: z.string().optional(),
      form_factor: z.enum(CRUX_FORM_FACTOR_VALUES).optional(),
      metrics: z.array(z.string()).min(1).max(20).optional(),
      collection_period_count: z.number().int().min(1).max(40).optional().default(25),
      timeout_ms: z.number().int().min(5_000).max(60_000).optional().default(CRUX_TIMEOUT_MS)
    },
    TOOL_EXTERNAL_READ,
    async ({ url, origin, form_factor, metrics, collection_period_count, timeout_ms }) => {
      const apiKey = googleApiKeyFor("crux");
      if (!apiKey) {
        throw new Error("CrUX API-Key fehlt. Setze CRUX_API_KEY, GOOGLE_CRUX_API_KEY oder GOOGLE_API_KEY.");
      }
      const body = buildCruxBody({ url, origin, form_factor, metrics, collection_period_count });
      const result = await callGoogleJsonApi({
        method: "POST",
        url: `${CRUX_API_BASE}/records:queryHistoryRecord`,
        params: { key: apiKey },
        body,
        timeout: timeout_ms
      });

      return out({
        tool: "seo_crux_history",
        ok: result.ok,
        status: result.status,
        request: { ...body, key_configured: true },
        response: result.data,
        note: result.ok
          ? undefined
          : "Wenn status 404 oder Luecken geliefert werden, hat CrUX fuer diese URL/Origin moeglicherweise nicht genug reale Nutzerdaten."
      });
    }
  );

  server.tool(
    "dataforseo_check_config",
    "Prueft die DataForSEO-Konfiguration ohne kostenpflichtige Keyword-Abfrage. Optional wird der kostenlose User-Data-Endpunkt abgefragt.",
    {
      agent_id: agentIdSchema,
      fetch_user_data: z.boolean().optional().default(false)
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, fetch_user_data }) => {
      const { summary } = getDataForSeoConfigDetails(agent_id, { requireCredentials: fetch_user_data });
      if (!fetch_user_data) {
        return out({ ...summary, fetch_user_data: false });
      }

      const response = await dataForSeoRequest(agent_id, {
        method: "GET",
        path: "/appendix/user_data"
      });

      return out({
        ...summary,
        fetch_user_data: true,
        user_data: maskSensitiveDataForSeoValue(compactDataForSeoResponse(response, { maxResults: 20 }))
      });
    }
  );

  server.tool(
    "pexels_check_config",
    "Prueft die Pexels-Konfiguration. Optional wird ein kleiner read-only Suchrequest ausgefuehrt, um den API-Key zu validieren.",
    {
      agent_id: agentIdSchema,
      fetch_test: z.boolean().optional().default(true),
      query: z.string().min(1).max(80).optional().default("design workspace")
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, fetch_test, query }) => {
      const { summary } = getPexelsConfigDetails(agent_id, { requireCredentials: fetch_test });
      if (!fetch_test) {
        return out({ ...summary, fetch_test: false });
      }

      const response = await pexelsRequest(agent_id, {
        path: "/v1/search",
        params: { query, per_page: 1, page: 1 }
      });

      return out({
        ...summary,
        fetch_test: true,
        ok: response.ok,
        status: response.status,
        rate_limit: response.rate_limit,
        query,
        returned_count: Array.isArray(response.data?.photos) ? response.data.photos.length : 0,
        sample_photo: response.data?.photos?.[0] ? summarizePexelsPhoto(response.data.photos[0]) : null,
        attribution_note: "Bei Verwendung nach Moeglichkeit Pexels und den Fotografen verlinken."
      });
    }
  );

  server.tool(
    "pexels_search_photos",
    "Sucht Pexels-Fotos fuer Design-/Layout-Arbeit. Gibt Bild-URLs, Fotografen und Attribution-Hinweise zurueck.",
    {
      agent_id: agentIdSchema,
      query: z.string().min(1).max(120),
      orientation: z.enum(PEXELS_ORIENTATION_VALUES).optional(),
      size: z.enum(PEXELS_PHOTO_SIZE_VALUES).optional(),
      color: z.enum(PEXELS_PHOTO_COLOR_VALUES).optional(),
      locale: z.string().min(2).max(10).optional(),
      page: z.number().int().min(1).max(1000).optional().default(1),
      per_page: z.number().int().min(1).max(30).optional().default(10)
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, query, orientation, size, color, locale, page, per_page }) => {
      const params = { query, page, per_page };
      maybeSetField(params, "orientation", orientation);
      maybeSetField(params, "size", size);
      maybeSetField(params, "color", color);
      maybeSetField(params, "locale", locale);

      const response = await pexelsRequest(agent_id, {
        path: "/v1/search",
        params
      });

      return out({
        agent_id,
        ok: response.ok,
        status: response.status,
        rate_limit: response.rate_limit,
        request: { ...params, api_key_configured: true },
        page: response.data?.page,
        per_page: response.data?.per_page,
        total_results: response.data?.total_results,
        next_page: response.data?.next_page,
        prev_page: response.data?.prev_page,
        photos: (response.data?.photos || []).map(summarizePexelsPhoto),
        attribution_note: "Bei Verwendung nach Moeglichkeit Pexels und den Fotografen verlinken."
      });
    }
  );

  server.tool(
    "templated_check_config",
    "Prueft die Templated.io-Konfiguration. Optional wird read-only eine Vorlage abgefragt, um den API-Key zu validieren.",
    {
      agent_id: agentIdSchema,
      fetch_templates: z.boolean().optional().default(true)
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, fetch_templates }) => {
      const { summary } = getTemplatedConfigDetails(agent_id, { requireCredentials: fetch_templates });
      if (!fetch_templates) {
        return out({ ...summary, fetch_templates: false });
      }

      const response = await templatedRequest(agent_id, {
        path: "/v1/templates",
        params: { page: 0, limit: 1, includeLayers: false, includePages: false }
      });
      const templates = getTemplatedItems(response.data);

      return out({
        ...summary,
        fetch_templates: true,
        ok: response.ok,
        status: response.status,
        template_count_returned: templates.length,
        sample_template: templates[0] ? summarizeTemplatedTemplate(templates[0]) : null
      });
    }
  );

  server.tool(
    "templated_list_templates",
    "Listet Templated.io-Vorlagen read-only, optional gefiltert nach Name, Dimensionen, Tags oder externer ID.",
    {
      agent_id: agentIdSchema,
      query: z.string().max(120).optional(),
      page: z.number().int().min(0).max(1000).optional().default(0),
      limit: z.number().int().min(1).max(50).optional().default(10),
      width: z.number().int().min(1).max(5000).optional(),
      height: z.number().int().min(1).max(5000).optional(),
      tags: z.string().max(300).optional(),
      external_id: z.string().max(200).optional(),
      include_layers: z.boolean().optional().default(false),
      include_pages: z.boolean().optional().default(false)
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, query, page, limit, width, height, tags, external_id, include_layers, include_pages }) => {
      const params = {
        page,
        limit,
        includeLayers: include_layers,
        includePages: include_pages
      };
      maybeSetField(params, "query", query);
      maybeSetField(params, "width", width);
      maybeSetField(params, "height", height);
      maybeSetField(params, "tags", tags);
      maybeSetField(params, "externalId", external_id);

      const response = await templatedRequest(agent_id, {
        path: "/v1/templates",
        params
      });
      const templates = getTemplatedItems(response.data);

      return out({
        agent_id,
        ok: response.ok,
        status: response.status,
        request: { ...params, api_key_configured: true },
        returned_count: templates.length,
        templates: include_layers || include_pages ? templates : templates.map(summarizeTemplatedTemplate),
        raw_meta:
          response.data && typeof response.data === "object" && !Array.isArray(response.data)
            ? Object.fromEntries(Object.entries(response.data).filter(([key]) => !["data", "templates", "items", "results"].includes(key)))
            : null
      });
    }
  );

  server.tool(
    "templated_render",
    "Erstellt einen Templated.io-Render aus einer Vorlage. Standard ist dry_run=true; echte Render brauchen klare Asana-Freigabe, weil Credits/Kosten entstehen koennen.",
    {
      agent_id: agentIdSchema,
      template: z.string().optional(),
      templates: z.array(z.string()).min(1).max(25).optional(),
      layers: z.record(z.string(), z.any()).optional().default({}),
      pages: z.array(z.record(z.string(), z.any())).optional(),
      format: z.enum(TEMPLATED_RENDER_FORMAT_VALUES).optional().default("jpg"),
      transparent: z.boolean().optional(),
      duration: z.number().int().min(1).max(90_000).optional(),
      fps: z.number().int().min(1).max(60).optional(),
      flatten: z.boolean().optional(),
      cmyk: z.boolean().optional(),
      name: z.string().max(200).optional(),
      background: z.string().max(80).optional(),
      width: z.number().int().min(100).max(5000).optional(),
      height: z.number().int().min(100).max(5000).optional(),
      scale: z.number().min(0.1).max(2).optional(),
      external_id: z.string().max(200).optional(),
      render_async: z.boolean().optional(),
      webhook_url: z.string().url().optional(),
      merge: z.boolean().optional(),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional()
    },
    TOOL_EXTERNAL_WRITE,
    async ({
      agent_id,
      template,
      templates,
      layers,
      pages,
      format,
      transparent,
      duration,
      fps,
      flatten,
      cmyk,
      name,
      background,
      width,
      height,
      scale,
      external_id,
      render_async,
      webhook_url,
      merge,
      dry_run,
      confirmed_by_asana,
      asana_task_gid
    }) => {
      if (!template && !templates?.length) {
        throw new Error("templated_render braucht entweder template oder templates.");
      }
      if (template && templates?.length) {
        throw new Error("Bitte entweder template oder templates setzen, nicht beides.");
      }

      const body = {};
      maybeSetField(body, "template", template);
      maybeSetField(body, "templates", templates);
      maybeSetField(body, "format", format);
      if (pages) maybeSetField(body, "pages", pages);
      else maybeSetField(body, "layers", layers || {});
      maybeSetField(body, "transparent", transparent);
      maybeSetField(body, "duration", duration);
      maybeSetField(body, "fps", fps);
      maybeSetField(body, "flatten", flatten);
      maybeSetField(body, "cmyk", cmyk);
      maybeSetField(body, "name", name);
      maybeSetField(body, "background", background);
      maybeSetField(body, "width", width);
      maybeSetField(body, "height", height);
      maybeSetField(body, "scale", scale);
      maybeSetField(body, "external_id", external_id);
      maybeSetField(body, "async", render_async);
      maybeSetField(body, "webhook_url", webhook_url);
      maybeSetField(body, "merge", merge);

      const { summary } = getTemplatedConfigDetails(agent_id, { requireCredentials: !dry_run });
      const payloadSummary = {
        has_template: Boolean(template),
        template_count: templates?.length || (template ? 1 : 0),
        format,
        layer_keys: pages ? [] : Object.keys(layers || {}),
        page_count: pages?.length || 0,
        payload_bytes: Buffer.byteLength(JSON.stringify(body), "utf8"),
        payload_sha256: sha256Json(body)
      };

      if (dry_run) {
        return out({
          ...summary,
          dry_run: true,
          endpoint: `${TEMPLATED_API_BASE}/v1/render`,
          payload_summary: payloadSummary,
          payload: body,
          note: "Kein Render wurde erstellt. Fuer Live-Render dry_run=false plus confirmed_by_asana=true und asana_task_gid setzen."
        });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "templated_render");
      const response = await templatedRequest(agent_id, {
        method: "POST",
        path: "/v1/render",
        data: body,
        timeout: format === "mp4" ? Math.max(TEMPLATED_TIMEOUT_MS, 120_000) : TEMPLATED_TIMEOUT_MS
      });

      return out({
        agent_id,
        dry_run: false,
        ok: response.ok,
        status: response.status,
        payload_summary: payloadSummary,
        response: response.data
      });
    }
  );

  server.tool(
    "freepik_check_config",
    "Prueft die Freepik/Magnific-API-Konfiguration. Optional wird ein kleiner read-only Ressourcen-Request ausgefuehrt, um den API-Key zu validieren.",
    {
      agent_id: agentIdSchema,
      fetch_test: z.boolean().optional().default(true),
      term: z.string().min(1).max(120).optional().default("travel")
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, fetch_test, term }) => {
      const { summary } = getFreepikConfigDetails(agent_id, { requireCredentials: fetch_test });
      if (!fetch_test) {
        return out({ ...summary, fetch_test: false });
      }

      const response = await freepikRequest(agent_id, {
        path: "/v1/resources",
        params: { term, limit: 1, page: 1 }
      });
      const resources = getFreepikItems(response.data);

      return out({
        ...summary,
        fetch_test: true,
        ok: response.ok,
        status: response.status,
        term,
        returned_count: resources.length,
        sample_resource: resources[0] ? summarizeFreepikResource(resources[0]) : null,
        raw_meta:
          response.data && typeof response.data === "object" && !Array.isArray(response.data)
            ? Object.fromEntries(Object.entries(response.data).filter(([key]) => !["data", "items", "resources", "results"].includes(key)))
            : null,
        usage_note:
          "Downloads und lizenzrelevante Nutzung nur gemaess Freepik-Lizenz und bei Asana-Aufgaben mit klarer Freigabe."
      });
    }
  );

  server.tool(
    "freepik_search_resources",
    "Sucht Freepik-Ressourcen read-only fuer Design-/Asset-Recherche. Gibt Metadaten, Thumbnails und Lizenz-/Autorhinweise zurueck, ohne Assets herunterzuladen.",
    {
      agent_id: agentIdSchema,
      term: z.string().min(1).max(120),
      page: z.number().int().min(1).max(1000).optional().default(1),
      limit: z.number().int().min(1).max(100).optional().default(20),
      order: z.string().max(50).optional(),
      filters: z.record(z.string(), z.any()).optional().default({}),
      accept_language: z.string().min(2).max(20).optional()
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, term, page, limit, order, filters, accept_language }) => {
      const params = { term, page, limit, ...filters };
      maybeSetField(params, "order", order);
      const response = await freepikRequest(agent_id, {
        path: "/v1/resources",
        params,
        timeout: FREEPIK_TIMEOUT_MS
      });
      const resources = getFreepikItems(response.data);

      return out({
        agent_id,
        ok: response.ok,
        status: response.status,
        request: {
          ...params,
          accept_language,
          api_key_configured: true
        },
        returned_count: resources.length,
        resources: resources.map(summarizeFreepikResource),
        raw_meta:
          response.data && typeof response.data === "object" && !Array.isArray(response.data)
            ? Object.fromEntries(Object.entries(response.data).filter(([key]) => !["data", "items", "resources", "results"].includes(key)))
            : null,
        usage_note: "Vor Download/Verwendung Lizenz und Aufgabenfreigabe pruefen."
      });
    }
  );

  server.tool(
    "freepik_get_resource",
    "Ruft Freepik-Ressourcen-Details read-only ab, ohne Asset-Download.",
    {
      agent_id: agentIdSchema,
      resource_id: z.union([z.string(), z.number()]),
      accept_language: z.string().min(2).max(20).optional()
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, resource_id, accept_language }) => {
      const response = await freepikRequest(agent_id, {
        path: `/v1/resources/${encodeURIComponent(String(resource_id))}`,
        params: accept_language ? { accept_language } : undefined
      });

      return out({
        agent_id,
        ok: response.ok,
        status: response.status,
        resource_id: String(resource_id),
        resource: response.data?.data ? summarizeFreepikResource(response.data.data) : summarizeFreepikResource(response.data),
        response: response.data,
        usage_note: "Vor Download/Verwendung Lizenz und Aufgabenfreigabe pruefen."
      });
    }
  );

  server.tool(
    "freepik_download_resource",
    "Bereitet einen Freepik-Ressourcen-Download vor oder fordert ihn an. Standard ist dry_run=true; echte Downloads brauchen klare Asana-Freigabe wegen Lizenz-/Kontonutzung.",
    {
      agent_id: agentIdSchema,
      resource_id: z.union([z.string(), z.number()]),
      resource_format: z.string().min(1).max(20).optional(),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional()
    },
    TOOL_EXTERNAL_WRITE,
    async ({ agent_id, resource_id, resource_format, dry_run, confirmed_by_asana, asana_task_gid }) => {
      const { summary } = getFreepikConfigDetails(agent_id, { requireCredentials: !dry_run });
      const path = resource_format
        ? `/v1/resources/${encodeURIComponent(String(resource_id))}/download/${encodeURIComponent(resource_format)}`
        : `/v1/resources/${encodeURIComponent(String(resource_id))}/download`;

      if (dry_run) {
        return out({
          ...summary,
          dry_run: true,
          resource_id: String(resource_id),
          resource_format: resource_format || null,
          endpoint: `${summary.freepik_api_base}${path}`,
          note: "Kein Asset wurde heruntergeladen. Fuer Live-Download dry_run=false plus confirmed_by_asana=true und asana_task_gid setzen."
        });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "freepik_download_resource");
      const response = await freepikRequest(agent_id, { path });

      return out({
        agent_id,
        dry_run: false,
        ok: response.ok,
        status: response.status,
        resource_id: String(resource_id),
        resource_format: resource_format || null,
        response: response.data,
        usage_note: "Lizenz-/Attribution-Hinweise aus der Aufgabe bzw. Freepik-Ressource bei finaler Uebergabe dokumentieren."
      });
    }
  );

  server.tool(
    "unsplash_check_config",
    "Prueft die Unsplash-Konfiguration. Optional wird ein kleiner read-only Suchrequest ausgefuehrt, um den Access Key zu validieren.",
    {
      agent_id: agentIdSchema,
      fetch_test: z.boolean().optional().default(true),
      query: z.string().min(1).max(120).optional().default("travel")
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, fetch_test, query }) => {
      const { summary } = getUnsplashConfigDetails(agent_id, { requireCredentials: fetch_test });
      if (!fetch_test) {
        return out({ ...summary, fetch_test: false });
      }

      const response = await unsplashRequest(agent_id, {
        path: "/search/photos",
        params: { query, per_page: 1, page: 1 }
      });
      const results = response.data?.results || [];

      return out({
        ...summary,
        fetch_test: true,
        ok: response.ok,
        status: response.status,
        rate_limit: response.rate_limit,
        query,
        returned_count: results.length,
        sample_photo: results[0] ? summarizeUnsplashPhoto(results[0]) : null,
        attribution_note:
          "Bei Verwendung Fotograf und Unsplash-Link dokumentieren; Downloads nach Unsplash-Regeln ueber download_location tracken."
      });
    }
  );

  server.tool(
    "unsplash_search_photos",
    "Sucht Unsplash-Fotos fuer Design-/Layout-Arbeit. Gibt Bild-URLs, Fotografen und Download-Location fuer regelkonformes Tracking zurueck.",
    {
      agent_id: agentIdSchema,
      query: z.string().min(1).max(120),
      page: z.number().int().min(1).max(1000).optional().default(1),
      per_page: z.number().int().min(1).max(30).optional().default(10),
      order_by: z.enum(["relevant", "latest"]).optional().default("relevant"),
      color: z.string().max(40).optional(),
      orientation: z.enum(["landscape", "portrait", "squarish"]).optional(),
      content_filter: z.enum(["low", "high"]).optional(),
      lang: z.string().min(2).max(10).optional()
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, query, page, per_page, order_by, color, orientation, content_filter, lang }) => {
      const params = { query, page, per_page, order_by };
      maybeSetField(params, "color", color);
      maybeSetField(params, "orientation", orientation);
      maybeSetField(params, "content_filter", content_filter);
      maybeSetField(params, "lang", lang);

      const response = await unsplashRequest(agent_id, {
        path: "/search/photos",
        params
      });

      return out({
        agent_id,
        ok: response.ok,
        status: response.status,
        rate_limit: response.rate_limit,
        request: { ...params, access_key_configured: true },
        total: response.data?.total,
        total_pages: response.data?.total_pages,
        photos: (response.data?.results || []).map(summarizeUnsplashPhoto),
        attribution_note:
          "Bei Verwendung Fotograf und Unsplash-Link dokumentieren; Downloads nach Unsplash-Regeln ueber download_location tracken."
      });
    }
  );

  server.tool(
    "unsplash_get_photo",
    "Ruft Unsplash-Foto-Details read-only ab, inklusive Download-Location fuer regelkonformes Tracking.",
    {
      agent_id: agentIdSchema,
      photo_id: z.string().min(1).max(120)
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, photo_id }) => {
      const response = await unsplashRequest(agent_id, {
        path: `/photos/${encodeURIComponent(photo_id)}`
      });

      return out({
        agent_id,
        ok: response.ok,
        status: response.status,
        rate_limit: response.rate_limit,
        photo_id,
        photo: summarizeUnsplashPhoto(response.data),
        response: response.data,
        attribution_note:
          "Bei Verwendung Fotograf und Unsplash-Link dokumentieren; Downloads nach Unsplash-Regeln ueber download_location tracken."
      });
    }
  );

  server.tool(
    "unsplash_track_download",
    "Ruft Unsplash download_location auf, um einen Download regelkonform zu tracken. Keine Datei wird heruntergeladen; es wird nur die von Unsplash gelieferte Download-URL zurueckgegeben.",
    {
      agent_id: agentIdSchema,
      download_location: z.string().url(),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional(),
      dry_run: z.boolean().optional().default(true)
    },
    TOOL_EXTERNAL_WRITE,
    async ({ agent_id, download_location, confirmed_by_asana, asana_task_gid, dry_run }) => {
      const { summary } = getUnsplashConfigDetails(agent_id, { requireCredentials: !dry_run });
      if (dry_run) {
        return out({
          ...summary,
          dry_run: true,
          download_location,
          note: "Kein Download-Tracking wurde ausgefuehrt. Fuer Nutzung dry_run=false plus confirmed_by_asana=true und asana_task_gid setzen."
        });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "unsplash_track_download");
      if (!download_location.startsWith(`${UNSPLASH_API_BASE}/`)) {
        throw new Error(`download_location muss vom konfigurierten Unsplash API Base kommen: ${UNSPLASH_API_BASE}`);
      }
      const path = download_location.slice(UNSPLASH_API_BASE.length);
      const response = await unsplashRequest(agent_id, { path });

      return out({
        agent_id,
        dry_run: false,
        ok: response.ok,
        status: response.status,
        rate_limit: response.rate_limit,
        download_location,
        response: response.data,
        usage_note: "Die gelieferte URL kann fuer den eigentlichen Asset-Download genutzt werden; Attribution dokumentieren."
      });
    }
  );

  server.tool(
    "google_seo_oauth_check_config",
    "Prueft die zentrale Google-OAuth-Konfiguration fuer GA4/Search Console read-only. Erzeugt testweise ein Access Token, gibt aber keine Secrets aus.",
    {},
    TOOL_EXTERNAL_READ,
    async () => {
      const selectedConfig = getFirstOAuthConfig(GOOGLE_SEO_OAUTH_PREFIXES);
      const summary = {
        env_prefix_candidates: GOOGLE_SEO_OAUTH_PREFIXES,
        env_configured: oauthEnvSummary(GOOGLE_SEO_OAUTH_PREFIXES),
        selected_env_prefix: selectedConfig?.prefix || null,
        required_scopes: GOOGLE_SEO_REQUIRED_SCOPES
      };
      if (!selectedConfig) {
        return out({
          ...summary,
          token_refresh_ok: false,
          error: "Kein vollstaendiges OAuth-Env-Set gefunden."
        });
      }

      const accessToken = await refreshGoogleOAuthAccessToken(selectedConfig, googleSeoTokenCache);
      const tokenInfo = await getGoogleTokenInfo(accessToken);
      const scopes = String(tokenInfo.data?.scope || "")
        .split(/\s+/)
        .filter(Boolean);
      return out({
        ...summary,
        token_refresh_ok: true,
        tokeninfo_status: tokenInfo.status,
        expires_in: tokenInfo.data?.expires_in,
        scopes,
        required_scopes_present: GOOGLE_SEO_REQUIRED_SCOPES.every((scope) => scopes.includes(scope)),
        audience: tokenInfo.data?.audience || tokenInfo.data?.aud || null
      });
    }
  );

  server.tool(
    "ga4_list_properties",
    "Listet GA4-Accounts und Properties, auf die der konfigurierte Google-OAuth-Account read-only Zugriff hat.",
    {
      page_size: z.number().int().min(1).max(200).optional().default(200),
      max_pages: z.number().int().min(1).max(20).optional().default(10)
    },
    TOOL_EXTERNAL_READ,
    async ({ page_size, max_pages }) => {
      const accountSummaries = [];
      let pageToken;
      for (let page = 0; page < max_pages; page += 1) {
        const res = await googleSeoRequest({
          method: "GET",
          url: "https://analyticsadmin.googleapis.com/v1beta/accountSummaries",
          params: {
            pageSize: page_size,
            pageToken
          }
        });
        accountSummaries.push(...(res.data.accountSummaries || []));
        pageToken = res.data.nextPageToken;
        if (!pageToken) break;
      }

      return out({
        account_count: accountSummaries.length,
        property_count: accountSummaries.reduce(
          (sum, account) => sum + Number(account.propertySummaries?.length || 0),
          0
        ),
        account_summaries: accountSummaries.map((account) => ({
          account: account.account,
          display_name: account.displayName,
          property_summaries: (account.propertySummaries || []).map((property) => ({
            property: property.property,
            display_name: property.displayName,
            property_type: property.propertyType,
            parent: property.parent
          }))
        }))
      });
    }
  );

  server.tool(
    "ga4_run_report",
    "Fuehrt einen GA4 Data API runReport read-only fuer eine Property aus. Nutzt nur Properties, auf die der OAuth-Account Zugriff hat.",
    {
      property_id: z.string(),
      date_start: z.string().optional().default("28daysAgo"),
      date_end: z.string().optional().default("today"),
      dimensions: z.array(z.string()).min(0).max(10).optional().default(["sessionDefaultChannelGroup"]),
      metrics: z.array(z.string()).min(1).max(10).optional().default(["sessions", "totalUsers"]),
      limit: z.number().int().min(1).max(10000).optional().default(1000),
      keep_empty_rows: z.boolean().optional().default(false)
    },
    TOOL_EXTERNAL_READ,
    async ({ property_id, date_start, date_end, dimensions, metrics, limit, keep_empty_rows }) => {
      const propertyName = normalizeGa4PropertyName(property_id);
      const res = await googleSeoRequest({
        method: "POST",
        url: `https://analyticsdata.googleapis.com/v1beta/${encodeURIComponent(propertyName)}:runReport`,
        data: {
          dateRanges: [{ startDate: date_start, endDate: date_end }],
          dimensions: dimensions.map((name) => ({ name })),
          metrics: metrics.map((name) => ({ name })),
          limit,
          keepEmptyRows: keep_empty_rows
        }
      });

      return out({
        property: propertyName,
        row_count: res.data.rowCount || 0,
        dimension_headers: res.data.dimensionHeaders || [],
        metric_headers: res.data.metricHeaders || [],
        rows: res.data.rows || [],
        totals: res.data.totals || [],
        metadata: res.data.metadata || null
      });
    }
  );

  server.tool(
    "gsc_list_sites",
    "Listet Search-Console-Properties, auf die der konfigurierte Google-OAuth-Account Zugriff hat.",
    {},
    TOOL_EXTERNAL_READ,
    async () => {
      const res = await googleSeoRequest({
        method: "GET",
        url: "https://www.googleapis.com/webmasters/v3/sites"
      });
      return out({
        site_count: res.data.siteEntry?.length || 0,
        sites: res.data.siteEntry || []
      });
    }
  );

  server.tool(
    "gsc_search_analytics",
    "Ruft Search-Console-Search-Analytics-Daten read-only fuer eine Property ab.",
    {
      site_url: z.string(),
      date_start: z.string(),
      date_end: z.string(),
      dimensions: z.array(z.string()).min(0).max(5).optional().default(["query", "page"]),
      row_limit: z.number().int().min(1).max(25000).optional().default(1000),
      start_row: z.number().int().min(0).optional().default(0),
      search_type: z.string().optional().default("web")
    },
    TOOL_EXTERNAL_READ,
    async ({ site_url, date_start, date_end, dimensions, row_limit, start_row, search_type }) => {
      const res = await googleSeoRequest({
        method: "POST",
        url: `https://www.googleapis.com/webmasters/v3/sites/${encodeURIComponent(site_url)}/searchAnalytics/query`,
        data: {
          startDate: date_start,
          endDate: date_end,
          dimensions,
          rowLimit: row_limit,
          startRow: start_row,
          type: search_type
        }
      });
      return out({
        site_url,
        date_start,
        date_end,
        dimensions,
        row_count: res.data.rows?.length || 0,
        rows: res.data.rows || []
      });
    }
  );

  server.tool(
    "gsc_url_inspection",
    "Prueft per Search Console URL Inspection API read-only den Indexierungsstatus einer URL innerhalb einer Property.",
    {
      site_url: z.string(),
      inspection_url: z.string(),
      language_code: z.string().optional().default("de-DE")
    },
    TOOL_EXTERNAL_READ,
    async ({ site_url, inspection_url, language_code }) => {
      const res = await googleSeoRequest({
        method: "POST",
        url: "https://searchconsole.googleapis.com/v1/urlInspection/index:inspect",
        data: {
          siteUrl: site_url,
          inspectionUrl: normalizeWebUrl(inspection_url),
          languageCode: language_code
        }
      });
      return out({
        site_url,
        inspection_url,
        response: res.data
      });
    }
  );

  server.tool(
    "google_ads_check_config",
    "Prueft die zentrale Google-Ads-API-Konfiguration ohne Secret-Ausgabe. Optional wird ein read-only ListAccessibleCustomers-Test ausgefuehrt.",
    {
      fetch_accessible_customers: z.boolean().optional().default(false),
      login_customer_id: z.string().optional()
    },
    TOOL_EXTERNAL_READ,
    async ({ fetch_accessible_customers, login_customer_id }) => {
      const selectedConfig = getFirstOAuthConfig(GOOGLE_ADS_OAUTH_PREFIXES);
      const summary = {
        env_prefix_candidates: GOOGLE_ADS_OAUTH_PREFIXES,
        env_configured: oauthEnvSummary(GOOGLE_ADS_OAUTH_PREFIXES),
        selected_env_prefix: selectedConfig?.prefix || null,
        api_version: GOOGLE_ADS_API_VERSION,
        developer_token_configured: Boolean(getGoogleAdsDeveloperToken()),
        default_login_customer_id_configured: Boolean(
          process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID || process.env.GOOGLE_ADS_MANAGER_CUSTOMER_ID
        ),
        requested_login_customer_id: login_customer_id ? normalizeGoogleAdsCustomerId(login_customer_id) : null,
        required_scopes: GOOGLE_ADS_REQUIRED_SCOPES
      };
      if (!selectedConfig) {
        return out({
          ...summary,
          token_refresh_ok: false,
          error: "Kein vollstaendiges OAuth-Env-Set gefunden."
        });
      }

      const accessToken = await refreshGoogleOAuthAccessToken(selectedConfig, googleAdsTokenCache);
      const tokenInfo = await getGoogleTokenInfo(accessToken);
      const scopes = String(tokenInfo.data?.scope || "")
        .split(/\s+/)
        .filter(Boolean);
      const requiredScopesPresent = GOOGLE_ADS_REQUIRED_SCOPES.every((scope) => scopes.includes(scope));
      const result = {
        ...summary,
        token_refresh_ok: true,
        tokeninfo_status: tokenInfo.status,
        expires_in: tokenInfo.data?.expires_in,
        scopes,
        required_scopes_present: requiredScopesPresent,
        audience: tokenInfo.data?.audience || tokenInfo.data?.aud || null
      };

      if (!fetch_accessible_customers) return out(result);
      try {
        const res = await googleAdsRequest({
          method: "GET",
          path: "/customers:listAccessibleCustomers",
          login_customer_id
        });
        return out({
          ...result,
          fetch_accessible_customers: true,
          list_accessible_customers_ok: true,
          request_id: res.headers?.["request-id"] || null,
          resource_names: res.data.resourceNames || []
        });
      } catch (error) {
        return out({
          ...result,
          fetch_accessible_customers: true,
          list_accessible_customers_ok: false,
          error: compactAxiosError(error)
        });
      }
    }
  );

  server.tool(
    "google_ads_list_customers",
    "Listet Google-Ads-Kundenkonten, auf die der konfigurierte OAuth-Account Zugriff hat.",
    {
      login_customer_id: z.string().optional()
    },
    TOOL_EXTERNAL_READ,
    async ({ login_customer_id }) => {
      const res = await googleAdsRequest({
        method: "GET",
        path: "/customers:listAccessibleCustomers",
        login_customer_id
      });
      return out({
        api_version: GOOGLE_ADS_API_VERSION,
        request_id: res.headers?.["request-id"] || null,
        customer_count: res.data.resourceNames?.length || 0,
        resource_names: res.data.resourceNames || []
      });
    }
  );

  server.tool(
    "google_ads_customer_clients",
    "Liest die Google-Ads-Konto-Hierarchie unter einem Manager- oder Kundenkonto read-only per GAQL aus.",
    {
      customer_id: z.string(),
      login_customer_id: z.string().optional(),
      level_max: z.number().int().min(0).max(10).optional().default(1),
      limit: z.number().int().min(1).max(10000).optional().default(1000)
    },
    TOOL_EXTERNAL_READ,
    async ({ customer_id, login_customer_id, level_max, limit }) => {
      const normalizedCustomerId = normalizeGoogleAdsCustomerId(customer_id);
      const query = `
        SELECT
          customer_client.client_customer,
          customer_client.descriptive_name,
          customer_client.manager,
          customer_client.level,
          customer_client.status,
          customer_client.currency_code,
          customer_client.time_zone
        FROM customer_client
        WHERE customer_client.level <= ${level_max}
        LIMIT ${limit}
      `;
      const res = await googleAdsRequest({
        method: "POST",
        path: `/customers/${normalizedCustomerId}/googleAds:search`,
        login_customer_id,
        data: { query, pageSize: Math.min(limit, 10000) }
      });
      return out({
        api_version: GOOGLE_ADS_API_VERSION,
        customer_id: normalizedCustomerId,
        login_customer_id: getGoogleAdsLoginCustomerId(login_customer_id),
        request_id: res.headers?.["request-id"] || null,
        row_count: res.data.results?.length || 0,
        next_page_token: res.data.nextPageToken || null,
        rows: res.data.results || []
      });
    }
  );

  server.tool(
    "google_ads_search",
    "Fuehrt eine freie Google-Ads-GAQL-Abfrage read-only aus. Nur fuer SELECT-Queries verwenden.",
    {
      customer_id: z.string(),
      query: z.string().min(1).max(10000),
      login_customer_id: z.string().optional(),
      page_size: z.number().int().min(1).max(10000).optional().default(1000),
      page_token: z.string().optional()
    },
    TOOL_EXTERNAL_READ,
    async ({ customer_id, query, login_customer_id, page_size, page_token }) => {
      const normalizedCustomerId = normalizeGoogleAdsCustomerId(customer_id);
      const trimmedQuery = query.trim();
      if (!/^select\s/i.test(trimmedQuery)) {
        throw new Error("google_ads_search erlaubt nur GAQL-SELECT-Queries.");
      }
      const res = await googleAdsRequest({
        method: "POST",
        path: `/customers/${normalizedCustomerId}/googleAds:search`,
        login_customer_id,
        data: {
          query: trimmedQuery,
          pageSize: page_size,
          ...(page_token ? { pageToken: page_token } : {})
        }
      });
      return out({
        api_version: GOOGLE_ADS_API_VERSION,
        customer_id: normalizedCustomerId,
        login_customer_id: getGoogleAdsLoginCustomerId(login_customer_id),
        request_id: res.headers?.["request-id"] || null,
        row_count: res.data.results?.length || 0,
        total_results_count: res.data.totalResultsCount || null,
        next_page_token: res.data.nextPageToken || null,
        field_mask: res.data.fieldMask || null,
        rows: res.data.results || []
      });
    }
  );

  server.tool(
    "google_ads_campaign_report",
    "Ruft einen standardisierten Google-Ads-Kampagnenbericht read-only ab.",
    {
      customer_id: z.string(),
      date_start: z.string(),
      date_end: z.string(),
      login_customer_id: z.string().optional(),
      limit: z.number().int().min(1).max(10000).optional().default(1000)
    },
    TOOL_EXTERNAL_READ,
    async ({ customer_id, date_start, date_end, login_customer_id, limit }) => {
      const normalizedCustomerId = normalizeGoogleAdsCustomerId(customer_id);
      const query = `
        SELECT
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.advertising_channel_type,
          campaign.optimization_score,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros,
          metrics.conversions,
          metrics.conversions_value,
          metrics.ctr,
          metrics.average_cpc
        FROM campaign
        WHERE segments.date BETWEEN '${date_start}' AND '${date_end}'
        ORDER BY metrics.impressions DESC
        LIMIT ${limit}
      `;
      const res = await googleAdsRequest({
        method: "POST",
        path: `/customers/${normalizedCustomerId}/googleAds:search`,
        login_customer_id,
        data: { query, pageSize: Math.min(limit, 10000) }
      });
      return out({
        api_version: GOOGLE_ADS_API_VERSION,
        customer_id: normalizedCustomerId,
        login_customer_id: getGoogleAdsLoginCustomerId(login_customer_id),
        date_start,
        date_end,
        request_id: res.headers?.["request-id"] || null,
        row_count: res.data.results?.length || 0,
        next_page_token: res.data.nextPageToken || null,
        rows: res.data.results || []
      });
    }
  );

  server.tool(
    "google_ads_mutate",
    "Fuehrt kontrollierte Google-Ads-Mutate-Operationen aus. Standard ist dry_run/validateOnly; echte Aenderungen nur mit klarer Asana-Freigabe.",
    {
      agent_id: agentIdSchema,
      customer_id: z.string(),
      service: z.enum(GOOGLE_ADS_MUTATE_SERVICE_VALUES),
      operations: z.array(z.record(z.string(), z.any())).min(1).max(50),
      login_customer_id: z.string().optional(),
      partial_failure: z.boolean().optional().default(false),
      response_content_type: z.enum(GOOGLE_ADS_RESPONSE_CONTENT_TYPE_VALUES).optional().default("RESOURCE_NAME_ONLY"),
      dry_run: z.boolean().optional().default(true),
      validate_only: z.boolean().optional().default(false),
      change_summary: z.string().optional(),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional()
    },
    TOOL_EXTERNAL_WRITE,
    async ({
      agent_id,
      customer_id,
      service,
      operations,
      login_customer_id,
      partial_failure,
      response_content_type,
      dry_run,
      validate_only,
      change_summary,
      confirmed_by_asana,
      asana_task_gid
    }) => {
      const normalizedCustomerId = normalizeGoogleAdsCustomerId(customer_id);
      const effectiveValidateOnly = dry_run || validate_only;
      if (!effectiveValidateOnly) {
        assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "google_ads_mutate");
        if (!String(change_summary || "").trim()) {
          throw new Error("google_ads_mutate braucht fuer echte Aenderungen eine change_summary.");
        }
      }

      const res = await googleAdsRequest({
        method: "POST",
        path: `/customers/${normalizedCustomerId}/${service}:mutate`,
        login_customer_id,
        data: {
          operations,
          partialFailure: partial_failure,
          validateOnly: effectiveValidateOnly,
          responseContentType: response_content_type
        }
      });

      return out({
        agent_id,
        api_version: GOOGLE_ADS_API_VERSION,
        customer_id: normalizedCustomerId,
        login_customer_id: getGoogleAdsLoginCustomerId(login_customer_id),
        service,
        dry_run,
        validate_only: effectiveValidateOnly,
        live_mutation_executed: !effectiveValidateOnly,
        change_summary: change_summary || null,
        request_id: res.headers?.["request-id"] || null,
        response: res.data
      });
    }
  );

  server.tool(
    "dataforseo_google_keyword_overview",
    "Ruft DataForSEO Labs Google Keyword Overview fuer konkrete Keywords ab. Live-Calls nur mit klarer Asana-Freigabe, da DataForSEO abrechnet.",
    {
      agent_id: agentIdSchema,
      keywords: z.array(z.string()).min(1).max(100),
      location_code: z.number().int().positive().optional(),
      language_code: z.string().min(2).max(10).optional(),
      include_serp_info: z.boolean().optional().default(false),
      include_clickstream_data: z.boolean().optional().default(false),
      max_results: z.number().int().positive().max(DATAFORSEO_MAX_RESULTS).optional().default(DATAFORSEO_MAX_RESULTS),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional()
    },
    TOOL_EXTERNAL_READ,
    async ({
      agent_id,
      keywords,
      location_code,
      language_code,
      include_serp_info,
      include_clickstream_data,
      max_results,
      dry_run,
      confirmed_by_asana,
      asana_task_gid
    }) => {
      const normalizedKeywords = normalizeDataForSeoKeywords(keywords, { maxCount: 100 });
      const target = resolveDataForSeoTarget({ location_code, language_code });
      const payload = [
        {
          keywords: normalizedKeywords,
          ...target,
          include_serp_info,
          include_clickstream_data
        }
      ];

      if (dry_run) {
        const { summary } = getDataForSeoConfigDetails(agent_id, { requireCredentials: false });
        return out({ agent_id, dry_run: true, endpoint: "/dataforseo_labs/google/keyword_overview/live", summary, payload });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "dataforseo_google_keyword_overview");
      const response = await dataForSeoRequest(agent_id, {
        path: "/dataforseo_labs/google/keyword_overview/live",
        data: payload
      });
      return out({ agent_id, endpoint: "/dataforseo_labs/google/keyword_overview/live", response: compactDataForSeoResponse(response, { maxResults: max_results }) });
    }
  );

  server.tool(
    "dataforseo_google_keyword_ideas",
    "Ruft DataForSEO Labs Google Keyword Ideas aus Seed-Keywords ab. Live-Calls nur mit klarer Asana-Freigabe, da DataForSEO abrechnet.",
    {
      agent_id: agentIdSchema,
      keywords: z.array(z.string()).min(1).max(20),
      location_code: z.number().int().positive().optional(),
      language_code: z.string().min(2).max(10).optional(),
      limit: z.number().int().positive().max(DATAFORSEO_MAX_RESULTS).optional().default(Math.min(50, DATAFORSEO_MAX_RESULTS)),
      offset: z.number().int().nonnegative().optional().default(0),
      include_serp_info: z.boolean().optional().default(false),
      include_seed_keyword: z.boolean().optional().default(true),
      filters: z.array(z.any()).optional(),
      order_by: z.array(z.string()).optional(),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional()
    },
    TOOL_EXTERNAL_READ,
    async ({
      agent_id,
      keywords,
      location_code,
      language_code,
      limit,
      offset,
      include_serp_info,
      include_seed_keyword,
      filters,
      order_by,
      dry_run,
      confirmed_by_asana,
      asana_task_gid
    }) => {
      const normalizedKeywords = normalizeDataForSeoKeywords(keywords, { maxCount: 20 });
      const target = resolveDataForSeoTarget({ location_code, language_code });
      const payload = [
        {
          keywords: normalizedKeywords,
          ...target,
          limit,
          offset,
          include_serp_info,
          include_seed_keyword,
          ...(filters ? { filters } : {}),
          ...(order_by ? { order_by } : {})
        }
      ];

      if (dry_run) {
        const { summary } = getDataForSeoConfigDetails(agent_id, { requireCredentials: false });
        return out({ agent_id, dry_run: true, endpoint: "/dataforseo_labs/google/keyword_ideas/live", summary, payload });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "dataforseo_google_keyword_ideas");
      const response = await dataForSeoRequest(agent_id, {
        path: "/dataforseo_labs/google/keyword_ideas/live",
        data: payload
      });
      return out({ agent_id, endpoint: "/dataforseo_labs/google/keyword_ideas/live", response: compactDataForSeoResponse(response, { maxResults: limit }) });
    }
  );

  server.tool(
    "dataforseo_google_search_volume",
    "Ruft DataForSEO Google Ads Search Volume fuer Keywordlisten ab. Live-Calls nur mit klarer Asana-Freigabe, da DataForSEO abrechnet.",
    {
      agent_id: agentIdSchema,
      keywords: z.array(z.string()).min(1).max(700),
      location_code: z.number().int().positive().optional(),
      language_code: z.string().min(2).max(10).optional(),
      search_partners: z.boolean().optional().default(false),
      date_from: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
      date_to: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
      max_results: z.number().int().positive().max(DATAFORSEO_MAX_RESULTS).optional().default(DATAFORSEO_MAX_RESULTS),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional()
    },
    TOOL_EXTERNAL_READ,
    async ({
      agent_id,
      keywords,
      location_code,
      language_code,
      search_partners,
      date_from,
      date_to,
      max_results,
      dry_run,
      confirmed_by_asana,
      asana_task_gid
    }) => {
      const normalizedKeywords = normalizeDataForSeoKeywords(keywords, { maxCount: 700 });
      const target = resolveDataForSeoTarget({ location_code, language_code });
      const payload = [
        {
          keywords: normalizedKeywords,
          ...target,
          search_partners,
          ...(date_from ? { date_from } : {}),
          ...(date_to ? { date_to } : {})
        }
      ];

      if (dry_run) {
        const { summary } = getDataForSeoConfigDetails(agent_id, { requireCredentials: false });
        return out({ agent_id, dry_run: true, endpoint: "/keywords_data/google_ads/search_volume/live", summary, payload });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "dataforseo_google_search_volume");
      const response = await dataForSeoRequest(agent_id, {
        path: "/keywords_data/google_ads/search_volume/live",
        data: payload
      });
      return out({ agent_id, endpoint: "/keywords_data/google_ads/search_volume/live", response: compactDataForSeoResponse(response, { maxResults: max_results }) });
    }
  );

  server.tool(
    "dataforseo_google_keyword_research",
    "Fuehrt eine gebuendelte DataForSEO-Keywordrecherche aus, damit Automationen nicht viele einzelne Live-Calls brauchen. Live nur mit klarer Asana-Freigabe.",
    {
      agent_id: agentIdSchema,
      seed_keywords: z.array(z.string()).min(1).max(20),
      overview_keywords: z.array(z.string()).max(100).optional().default([]),
      search_volume_keywords: z.array(z.string()).max(700).optional().default([]),
      location_code: z.number().int().positive().optional(),
      language_code: z.string().min(2).max(10).optional(),
      run_ideas: z.boolean().optional().default(true),
      run_overview: z.boolean().optional().default(true),
      run_search_volume: z.boolean().optional().default(false),
      ideas_limit: z.number().int().positive().max(DATAFORSEO_MAX_RESULTS).optional().default(Math.min(50, DATAFORSEO_MAX_RESULTS)),
      include_serp_info: z.boolean().optional().default(false),
      include_clickstream_data: z.boolean().optional().default(false),
      search_partners: z.boolean().optional().default(false),
      date_from: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
      date_to: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
      max_live_requests: z.number().int().positive().max(3).optional().default(3),
      max_results: z.number().int().positive().max(DATAFORSEO_MAX_RESULTS).optional().default(DATAFORSEO_MAX_RESULTS),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional()
    },
    TOOL_EXTERNAL_READ,
    async ({
      agent_id,
      seed_keywords,
      overview_keywords,
      search_volume_keywords,
      location_code,
      language_code,
      run_ideas,
      run_overview,
      run_search_volume,
      ideas_limit,
      include_serp_info,
      include_clickstream_data,
      search_partners,
      date_from,
      date_to,
      max_live_requests,
      max_results,
      dry_run,
      confirmed_by_asana,
      asana_task_gid
    }) => {
      const normalizedSeedKeywords = normalizeDataForSeoKeywords(seed_keywords, { maxCount: 20 });
      const target = resolveDataForSeoTarget({ location_code, language_code });
      const requests = [];

      if (run_ideas) {
        requests.push({
          name: "keyword_ideas",
          path: "/dataforseo_labs/google/keyword_ideas/live",
          maxResults: ideas_limit,
          data: [
            {
              keywords: normalizedSeedKeywords,
              ...target,
              limit: ideas_limit,
              offset: 0,
              include_serp_info,
              include_seed_keyword: true
            }
          ]
        });
      }

      if (run_overview) {
        const normalizedOverviewKeywords = normalizeDataForSeoKeywords(
          overview_keywords.length ? overview_keywords : normalizedSeedKeywords,
          { maxCount: 100 }
        );
        requests.push({
          name: "keyword_overview",
          path: "/dataforseo_labs/google/keyword_overview/live",
          maxResults: max_results,
          data: [
            {
              keywords: normalizedOverviewKeywords,
              ...target,
              include_serp_info,
              include_clickstream_data
            }
          ]
        });
      }

      if (run_search_volume) {
        const normalizedSearchVolumeKeywords = normalizeDataForSeoKeywords(
          search_volume_keywords.length ? search_volume_keywords : normalizedSeedKeywords,
          { maxCount: 700 }
        );
        requests.push({
          name: "search_volume",
          path: "/keywords_data/google_ads/search_volume/live",
          maxResults: max_results,
          data: [
            {
              keywords: normalizedSearchVolumeKeywords,
              ...target,
              search_partners,
              ...(date_from ? { date_from } : {}),
              ...(date_to ? { date_to } : {})
            }
          ]
        });
      }

      if (!requests.length) {
        throw new Error("Mindestens einer der Schalter run_ideas, run_overview oder run_search_volume muss true sein.");
      }
      if (requests.length > max_live_requests) {
        throw new Error(`Geplante Live-Requests (${requests.length}) ueberschreiten max_live_requests (${max_live_requests}).`);
      }

      const planned_requests = summarizeDataForSeoRequests(requests);
      if (dry_run) {
        const { summary } = getDataForSeoConfigDetails(agent_id, { requireCredentials: false });
        return out({
          agent_id,
          dry_run: true,
          summary,
          target,
          planned_live_requests_count: requests.length,
          requires_for_live: ["dry_run=false", "confirmed_by_asana=true", "asana_task_gid"],
          planned_requests
        });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "dataforseo_google_keyword_research");
      const responses = {};
      let total_cost = 0;
      for (const request of requests) {
        const response = await dataForSeoRequest(agent_id, {
          path: request.path,
          data: request.data
        });
        total_cost += Number(response.cost || 0);
        responses[request.name] = compactDataForSeoResponse(response, { maxResults: request.maxResults });
      }

      return out({
        agent_id,
        dry_run: false,
        asana_task_gid,
        target,
        live_requests_count: requests.length,
        live_requests: planned_requests.map(({ name, endpoint, max_results }) => ({ name, endpoint, max_results })),
        total_cost,
        responses
      });
    }
  );

  server.tool(
    "google_drive_create_file_in_agent_folder",
    "Erstellt eine neue Google-Datei direkt im erlaubten Agenten-Drive-Ordner. Keine Secrets ausgeben. Standard-Ziel ist der zentrale VIP-Agenten-Drive-Ordner.",
    {
      agent_id: agentIdSchema,
      title: z.string(),
      mime_type: z.enum([
        "application/vnd.google-apps.document",
        "application/vnd.google-apps.spreadsheet",
        "application/vnd.google-apps.presentation",
        "application/vnd.google-apps.folder"
      ]),
      target_folder_id: z.string().optional().default(GOOGLE_AGENT_FOLDER_ID)
    },
    TOOL_SAFE_WRITE,
    async ({ agent_id, title, mime_type, target_folder_id }) => {
      await assertAllowedGoogleFolder(target_folder_id);

      const res = await googleRequest({
        method: "POST",
        url: "https://www.googleapis.com/drive/v3/files",
        params: {
          fields: "id,name,mimeType,parents,webViewLink",
          supportsAllDrives: true
        },
        data: {
          name: title,
          mimeType: mime_type,
          parents: [target_folder_id]
        }
      });

      return out({ agent_id, file: res.data });
    }
  );

  server.tool(
    "google_drive_copy_file_to_agent_folder",
    "Kopiert eine bestehende Google-Drive-Datei als echte Drive-Dateikopie in den erlaubten Agenten-Drive-Ordner. Nutze dies fuer Vorlagen, wenn Formatierung, Tabs, Formeln, Filter, Validierungen oder Layout erhalten bleiben sollen.",
    {
      agent_id: agentIdSchema,
      source_file_id: z.string(),
      title: z.string().optional(),
      target_folder_id: z.string().optional().default(GOOGLE_AGENT_FOLDER_ID),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional(),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_SAFE_WRITE,
    async ({
      agent_id,
      source_file_id,
      title,
      target_folder_id,
      dry_run,
      confirmed_by_asana,
      asana_task_gid,
      verify_after
    }) => {
      await assertAllowedGoogleFolder(target_folder_id);

      const sourceFile = await getDriveFile(source_file_id, "id,name,mimeType,parents,webViewLink");
      if (sourceFile.mimeType === "application/vnd.google-apps.folder") {
        throw new Error("Google-Ordner koennen nicht per files.copy kopiert werden. Fuer Ordner braucht es einen separaten rekursiven Kopierprozess.");
      }

      const copyTitle = title?.trim() || `${sourceFile.name} - Kopie`;
      const plannedCopy = {
        source_file: sourceFile,
        target_folder_id,
        title: copyTitle,
        preserves_native_google_formatting: true
      };

      if (dry_run) {
        return out({
          agent_id,
          dry_run: true,
          ...plannedCopy,
          requires_for_live: ["dry_run=false", "confirmed_by_asana=true", "asana_task_gid"]
        });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "google_drive_copy_file_to_agent_folder");

      const res = await googleRequest({
        method: "POST",
        url: `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(source_file_id)}/copy`,
        params: {
          fields: "id,name,mimeType,parents,webViewLink",
          supportsAllDrives: true
        },
        data: {
          name: copyTitle,
          parents: [target_folder_id]
        }
      });

      const verifiedFile = verify_after
        ? await getDriveFile(res.data.id, "id,name,mimeType,parents,webViewLink")
        : undefined;
      const verificationStatus =
        !verify_after || verifiedFile?.parents?.includes(target_folder_id) ? "verified" : "folder_readback_mismatch";

      return out({
        agent_id,
        dry_run: false,
        asana_task_gid,
        source_file: sourceFile,
        copied_file: res.data,
        verified_file: verifiedFile,
        verification_status: verificationStatus,
        preserves_native_google_formatting: true
      });
    }
  );

  server.tool(
    "google_drive_move_file_to_agent_folder",
    "Verschiebt eine bestehende Google-Drive-Datei in den erlaubten Agenten-Drive-Ordner oder einen Unterordner davon. Entfernt standardmaessig alte Parents.",
    {
      agent_id: agentIdSchema,
      file_id: z.string(),
      target_folder_id: z.string().optional().default(GOOGLE_AGENT_FOLDER_ID),
      remove_previous_parents: z.boolean().optional().default(true)
    },
    TOOL_IDEMPOTENT_SAFE_WRITE,
    async ({ agent_id, file_id, target_folder_id, remove_previous_parents }) => {
      await assertAllowedGoogleFolder(target_folder_id);

      const file = await getDriveFile(file_id, "id,name,mimeType,parents,webViewLink");
      const previousParents = file.parents || [];

      const res = await googleRequest({
        method: "PATCH",
        url: `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(file_id)}`,
        params: {
          addParents: target_folder_id,
          removeParents: remove_previous_parents ? previousParents.join(",") : undefined,
          fields: "id,name,mimeType,parents,webViewLink",
          supportsAllDrives: true
        }
      });

      return out({
        agent_id,
        previous_parents: previousParents,
        file: res.data
      });
    }
  );

  server.tool(
    "google_sheets_append_rows",
    "Fuegt Zeilen in ein Google Sheet ein. Fuer Live-Daten nur nutzen, wenn die Asana-Aufgabe diese Aktion deckt.",
    {
      agent_id: agentIdSchema,
      spreadsheet_id: z.string(),
      sheet_name: z.string(),
      values: z.array(z.array(sheetCellValueSchema)),
      value_input_option: z.enum(["RAW", "USER_ENTERED"]).optional().default("USER_ENTERED")
    },
    TOOL_SAFE_WRITE,
    async ({ agent_id, spreadsheet_id, sheet_name, values, value_input_option }) => {
      if (!values.length) throw new Error("values darf nicht leer sein.");

      const range = `${sheet_name}!A1`;
      const res = await googleRequest({
        method: "POST",
        url: `https://sheets.googleapis.com/v4/spreadsheets/${encodeURIComponent(
          spreadsheet_id
        )}/values/${encodeURIComponent(range)}:append`,
        params: {
          valueInputOption: value_input_option,
          insertDataOption: "INSERT_ROWS"
        },
        data: { values }
      });

      return out({ agent_id, response: res.data });
    }
  );

  server.tool(
    "google_sheets_read_range",
    "Liest Werte aus einem bestehenden Google Sheet. Read-only; fuer gezielte Checks vor Sheet-Updates.",
    {
      agent_id: agentIdSchema,
      spreadsheet_id: z.string(),
      sheet_name: z.string(),
      range: z.string(),
      value_render_option: z
        .enum(["FORMATTED_VALUE", "UNFORMATTED_VALUE", "FORMULA"])
        .optional()
        .default("FORMATTED_VALUE")
    },
    TOOL_READ_ONLY,
    async ({ agent_id, spreadsheet_id, sheet_name, range, value_render_option }) => {
      const a1Range = buildSheetRange(sheet_name, range);
      const values = await getSheetValues(spreadsheet_id, a1Range, value_render_option);
      return out({ agent_id, spreadsheet_id, sheet_name, range, values });
    }
  );

  server.tool(
    "google_sheets_format_snapshot",
    "Liest eine begrenzte Format-/Struktur-Stichprobe aus einem Google Sheet. Read-only; fuer Template-/Kopie-Checks, wenn Formatierung erhalten bleiben muss.",
    {
      agent_id: agentIdSchema,
      spreadsheet_id: z.string(),
      sheet_name: z.string(),
      range: z.string().optional().default("A1:Z20")
    },
    TOOL_READ_ONLY,
    async ({ agent_id, spreadsheet_id, sheet_name, range }) => {
      const a1Range = buildSheetRange(sheet_name, range);
      const res = await googleRequest({
        method: "GET",
        url: `https://sheets.googleapis.com/v4/spreadsheets/${encodeURIComponent(spreadsheet_id)}`,
        params: {
          includeGridData: true,
          ranges: a1Range,
          fields:
            "spreadsheetId,properties(title),sheets(properties(sheetId,title,gridProperties),merges,conditionalFormats,protectedRanges,filterViews,basicFilter,data(rowData(values(formattedValue,userEnteredFormat,effectiveFormat,dataValidation,note,hyperlink))))"
        }
      });

      const sheet = res.data.sheets?.[0];
      if (!sheet) throw new Error(`Sheet nicht gefunden oder Range nicht lesbar: ${sheet_name}`);

      const sample_cells = (sheet.data?.[0]?.rowData || []).map((row) =>
        (row.values || []).map((cell) => ({
          formattedValue: cell.formattedValue,
          userEnteredFormat: cell.userEnteredFormat,
          effectiveFormat: cell.effectiveFormat,
          dataValidation: cell.dataValidation,
          note: cell.note,
          hyperlink: cell.hyperlink
        }))
      );

      return out({
        agent_id,
        spreadsheet_id,
        spreadsheet_title: res.data.properties?.title,
        sheet_name,
        range,
        sheet_properties: sheet.properties,
        structure_summary: {
          merges_count: sheet.merges?.length || 0,
          conditional_formats_count: sheet.conditionalFormats?.length || 0,
          protected_ranges_count: sheet.protectedRanges?.length || 0,
          filter_views_count: sheet.filterViews?.length || 0,
          has_basic_filter: Boolean(sheet.basicFilter)
        },
        sample_cells
      });
    }
  );

  server.tool(
    "google_sheets_update_range",
    "Aktualisiert eine explizite A1-Range in einem bestehenden Google Sheet und verifiziert optional per Readback. Live-Ausfuehrung nur mit klarer Asana-Freigabe.",
    {
      agent_id: agentIdSchema,
      spreadsheet_id: z.string(),
      sheet_name: z.string(),
      range: z.string(),
      values: z.array(z.array(sheetCellValueSchema)),
      value_input_option: z.enum(["RAW", "USER_ENTERED"]).optional().default("USER_ENTERED"),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional(),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_IDEMPOTENT_SAFE_WRITE,
    async ({
      agent_id,
      spreadsheet_id,
      sheet_name,
      range,
      values,
      value_input_option,
      dry_run,
      confirmed_by_asana,
      asana_task_gid,
      verify_after
    }) => {
      if (!values.length) throw new Error("values darf nicht leer sein.");
      const a1Range = buildSheetRange(sheet_name, range);

      if (dry_run) {
        return out({ agent_id, dry_run: true, spreadsheet_id, sheet_name, range, values });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "google_sheets_update_range");

      const res = await googleRequest({
        method: "PUT",
        url: `https://sheets.googleapis.com/v4/spreadsheets/${encodeURIComponent(
          spreadsheet_id
        )}/values/${encodeURIComponent(a1Range)}`,
        params: { valueInputOption: value_input_option },
        data: { values }
      });

      const verified_values = verify_after ? await getSheetValues(spreadsheet_id, a1Range) : undefined;
      return out({ agent_id, response: res.data, verified_values });
    }
  );

  server.tool(
    "rs_redaktionsplan_find_next_top10_row",
    "Findet im Reise-Stories-Redaktionsplan die naechste TOP-10-Zeile: Seitentyp TOP 10, Status Offline, Live-URL leer; Prioritaet Hoch vor Mittel vor Gering.",
    {
      agent_id: agentIdSchema,
      spreadsheet_id: z.string().optional().default(RS_REDAKTIONSPLAN_SPREADSHEET_ID),
      sheet_name: z.string().optional().default(RS_REDAKTIONSPLAN_SHEET_NAME),
      max_rows: z.number().int().positive().optional().default(12000)
    },
    TOOL_READ_ONLY,
    async ({ agent_id, spreadsheet_id, sheet_name, max_rows }) => {
      const range = buildSheetRange(sheet_name, `A1:J${max_rows}`);
      const values = await getSheetValues(spreadsheet_id, range);
      const candidates = [];

      for (let rowIndex = 2; rowIndex < values.length; rowIndex += 1) {
        const row = values[rowIndex] || [];
        const liveUrl = cleanCell(row[0]);
        const seitentyp = cleanCell(row[2]);
        const prioritaet = cleanCell(row[3]);
        const notiz = cleanCell(row[4]);
        const status = cleanCell(row[8]);
        const fokusKeyword = cleanCell(row[9]);

        if (seitentyp !== "TOP 10" || status !== "Offline" || liveUrl || !fokusKeyword) continue;
        candidates.push({
          row_number: rowIndex + 1,
          priority: prioritaet || "Gering",
          focus_keyword: fokusKeyword,
          notes: notiz,
          values: row
        });
      }

      const priorityOrder = ["Hoch", "Mittel", "Gering"];
      const selected =
        priorityOrder
          .map((priority) => candidates.find((candidate) => candidate.priority === priority))
          .find(Boolean) || candidates[0];

      return out({
        agent_id,
        spreadsheet_id,
        sheet_name,
        checked_range: range,
        candidate_count: candidates.length,
        selected: selected || null
      });
    }
  );

  server.tool(
    "rs_redaktionsplan_update_row",
    "Aktualisiert eine gefundene Reise-Stories-Redaktionsplan-Zeile fuer einen TOP-10-Artikel und verifiziert A:J danach. Live-Ausfuehrung nur mit klarer Asana-Freigabe.",
    {
      agent_id: agentIdSchema,
      spreadsheet_id: z.string().optional().default(RS_REDAKTIONSPLAN_SPREADSHEET_ID),
      sheet_name: z.string().optional().default(RS_REDAKTIONSPLAN_SHEET_NAME),
      row_number: z.number().int().min(3),
      live_url: z.string(),
      title: z.string(),
      publication_date: z.string(),
      region: z.string(),
      author: z.string().optional().default("Reise-Redaktion"),
      status: z.enum(["Online", "Entwurf", "Offline"]).optional().default("Online"),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional(),
      enforce_current_selection: z.boolean().optional().default(true)
    },
    TOOL_IDEMPOTENT_SAFE_WRITE,
    async ({
      agent_id,
      spreadsheet_id,
      sheet_name,
      row_number,
      live_url,
      title,
      publication_date,
      region,
      author,
      status,
      dry_run,
      confirmed_by_asana,
      asana_task_gid,
      enforce_current_selection
    }) => {
      if (!/^https:\/\/reise-stories\.de\/[a-z0-9-]+\/$/.test(live_url)) {
        throw new Error("live_url muss dem Format https://reise-stories.de/<slug>/ entsprechen.");
      }
      if (!/^\d{2}\.\d{2}\.\d{4}$/.test(publication_date)) {
        throw new Error("publication_date muss im Format TT.MM.JJJJ sein.");
      }

      const rowRange = buildSheetRange(sheet_name, `A${row_number}:J${row_number}`);
      const before = (await getSheetValues(spreadsheet_id, rowRange))[0] || [];

      if (enforce_current_selection) {
        const liveUrlBefore = cleanCell(before[0]);
        const seitentyp = cleanCell(before[2]);
        const currentStatus = cleanCell(before[8]);
        if (liveUrlBefore || seitentyp !== "TOP 10" || currentStatus !== "Offline") {
          throw new Error(
            `Zeile ${row_number} ist nicht mehr die erwartete Offline-TOP-10-Zeile mit leerer Live-URL.`
          );
        }
      }

      const updates = [
        { range: buildSheetRange(sheet_name, `A${row_number}:B${row_number}`), values: [[live_url, title]] },
        {
          range: buildSheetRange(sheet_name, `F${row_number}:I${row_number}`),
          values: [[publication_date, region, author, status]]
        }
      ];

      if (dry_run) {
        return out({ agent_id, dry_run: true, spreadsheet_id, sheet_name, row_number, before, updates });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "rs_redaktionsplan_update_row");

      const res = await googleRequest({
        method: "POST",
        url: `https://sheets.googleapis.com/v4/spreadsheets/${encodeURIComponent(
          spreadsheet_id
        )}/values:batchUpdate`,
        data: {
          valueInputOption: "USER_ENTERED",
          data: updates
        }
      });

      const verified = (await getSheetValues(spreadsheet_id, rowRange))[0] || [];
      return out({ agent_id, response: res.data, verified_row_range: rowRange, verified_values: verified });
    }
  );

  server.tool(
    "google_sheets_delete_duplicates",
    "Fuehrt die native Google-Sheets-deleteDuplicates-Operation kontrolliert aus. Live-Ausfuehrung nur mit klarer Asana-Freigabe; dry_run zeigt nur den geplanten Request.",
    {
      agent_id: agentIdSchema,
      spreadsheet_id: z.string(),
      sheet_name: z.string().optional(),
      sheet_id: z.number().optional(),
      start_row_index: z.number().int().nonnegative().optional().default(0),
      end_row_index: z.number().int().positive().optional(),
      start_column_index: z.number().int().nonnegative().optional().default(0),
      end_column_index: z.number().int().positive(),
      comparison_column_indexes: z.array(z.number().int().nonnegative()).optional().default([0]),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional()
    },
    TOOL_DESTRUCTIVE_IDEMPOTENT_WRITE,
    async ({
      agent_id,
      spreadsheet_id,
      sheet_name,
      sheet_id,
      start_row_index,
      end_row_index,
      start_column_index,
      end_column_index,
      comparison_column_indexes,
      dry_run,
      confirmed_by_asana,
      asana_task_gid
    }) => {
      if (typeof sheet_id !== "number" && !sheet_name) {
        throw new Error("google_sheets_delete_duplicates braucht sheet_id oder sheet_name.");
      }
      if (end_column_index <= start_column_index) {
        throw new Error("end_column_index muss groesser als start_column_index sein.");
      }
      if (end_row_index && end_row_index <= start_row_index) {
        throw new Error("end_row_index muss groesser als start_row_index sein.");
      }
      for (const index of comparison_column_indexes) {
        if (index < start_column_index || index >= end_column_index) {
          throw new Error("comparison_column_indexes muessen innerhalb der angegebenen Spalten-Range liegen.");
        }
      }

      const resolvedSheetId =
        typeof sheet_id === "number" ? sheet_id : await getSheetIdByName(spreadsheet_id, sheet_name);

      const request = {
        deleteDuplicates: {
          range: {
            sheetId: resolvedSheetId,
            startRowIndex: start_row_index,
            startColumnIndex: start_column_index,
            endColumnIndex: end_column_index,
            ...(end_row_index ? { endRowIndex: end_row_index } : {})
          },
          comparisonColumns: comparison_column_indexes.map((index) => ({
            sheetId: resolvedSheetId,
            dimension: "COLUMNS",
            startIndex: index,
            endIndex: index + 1
          }))
        }
      };

      if (dry_run) {
        return out({ agent_id, dry_run: true, request });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "google_sheets_delete_duplicates");

      const res = await googleRequest({
        method: "POST",
        url: `https://sheets.googleapis.com/v4/spreadsheets/${encodeURIComponent(
          spreadsheet_id
        )}:batchUpdate`,
        data: { requests: [request] }
      });

      return out({ agent_id, response: res.data });
    }
  );

  server.tool(
    "agent_email_check_config",
    "Prueft die SMTP-Konfiguration eines Agenten ohne E-Mail-Versand. Optional wird nur die SMTP-Anmeldung getestet; es werden keine Empfaenger, kein MAIL FROM und kein DATA gesendet.",
    {
      agent_id: agentIdSchema,
      check_smtp_auth: z.boolean().optional().default(false)
    },
    TOOL_READ_ONLY,
    async ({ agent_id, check_smtp_auth }) => {
      const { configs, summary } = getSmtpConfigCandidates(agent_id, { requireCredentials: check_smtp_auth });
      const { summary: httpSummary } = getEmailHttpConfigDetails(agent_id, { requireCredentials: false });
      const smtpReady = summary.ready_for_send;
      const result = {
        ...summary,
        ...httpSummary,
        smtp_ready_for_send: smtpReady,
        ready_for_send: httpSummary.email_http_provider_configured ? httpSummary.email_http_ready_for_send : smtpReady,
        active_email_transport: httpSummary.email_http_provider_configured
          ? httpSummary.email_http_ready_for_send
            ? httpSummary.email_http_provider
            : null
          : smtpReady
            ? "smtp"
            : null,
        check_smtp_auth,
        smtp_auth_ok: null,
        smtp_auth: null,
        smtp_auth_attempts: []
      };

      if (check_smtp_auth) {
        try {
          const auth = await checkSmtpAuth(configs);
          result.smtp_auth_ok = true;
          result.smtp_auth = {
            host: auth.host,
            port: auth.port,
            secure: auth.secure,
            label: auth.label
          };
          result.smtp_auth_attempts = auth.attempts;
        } catch (error) {
          result.smtp_auth_ok = false;
          result.smtp_auth_error = String(error?.message || error);
          result.smtp_auth_attempts = error.smtp_attempts || [];
        }
      }

      return out(result);
    }
  );

  server.tool(
    "agent_email_read_unseen",
    "Liest ungelesene E-Mails eines Agenten read-only per IMAP EXAMINE und BODY.PEEK. Markiert keine Nachrichten als gelesen und sendet nichts.",
    {
      agent_id: agentIdSchema,
      limit: z.number().int().min(0).max(25).optional().default(10),
      include_snippets: z.boolean().optional().default(false),
      snippet_chars: z.number().int().min(100).max(1200).optional().default(500)
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, limit, include_snippets, snippet_chars }) => {
      const { configs, summary } = getImapConfigCandidates(agent_id, { requireCredentials: true });
      const result = await readUnseenEmailWithFallback(configs, {
        limit,
        includeSnippets: include_snippets,
        snippetChars: snippet_chars
      });

      return out({
        ...summary,
        mode: "readonly_unseen_body_peek",
        connection: {
          host: result.host,
          port: result.port,
          secure: result.secure,
          label: result.label
        },
        unseen_count: result.unseen_count,
        returned_count: result.returned_count,
        messages: result.messages,
        imap_attempts: result.attempts
      });
    }
  );

  server.tool(
    "agent_email_prepare",
    "Bereitet eine Plain-Text-E-Mail serverseitig vor und gibt eine Draft-ID zurueck. Sendet nichts; empfohlen fuer grosse Payloads vor agent_email_send_prepared.",
    {
      agent_id: agentIdSchema,
      to: z.string(),
      subject: z.string(),
      body: z.string()
    },
    TOOL_SAFE_WRITE,
    async ({ agent_id, to, subject, body }) => {
      const validated = validateEmailPayload({ to, subject, body });
      const config = getSmtpConfig(agent_id, { requireCredentials: false });
      const stored = storeEmailDraft({
        agent_id,
        from: config.fromAddress,
        to: validated.recipients,
        subject: validated.subject,
        body,
        body_bytes: validated.body_bytes,
        body_sha256: validated.body_sha256
      });

      return out({
        agent_id,
        prepared: true,
        draft_id: stored.draft_id,
        expires_at: new Date(stored.expires_at_ms).toISOString(),
        from: config.fromAddress,
        to: validated.recipients,
        subject: validated.subject,
        body_bytes: validated.body_bytes,
        body_sha256: validated.body_sha256
      });
    }
  );

  server.tool(
    "agent_email_send_prepared",
    "Sendet eine zuvor mit agent_email_prepare vorbereitete E-Mail per SMTP. Live-Versand nur mit klarer Asana-Freigabe; Payload wird per Draft-ID referenziert.",
    {
      agent_id: agentIdSchema,
      draft_id: z.string(),
      body_sha256: z.string().optional(),
      confirmed_by_asana: z.boolean(),
      asana_task_gid: z.string()
    },
    TOOL_EXTERNAL_WRITE,
    async ({ agent_id, draft_id, body_sha256, confirmed_by_asana, asana_task_gid }) => {
      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "agent_email_send_prepared");
      const draft = getEmailDraft(draft_id, agent_id);
      if (body_sha256 && body_sha256 !== draft.body_sha256) {
        throw new Error("body_sha256 stimmt nicht mit dem vorbereiteten E-Mail-Draft ueberein.");
      }

      const { config: httpConfig } = getEmailHttpConfigDetails(agent_id, { requireCredentials: false });
      const { primaryConfig } = getSmtpConfigCandidates(agent_id, { requireCredentials: false });
      let transport;
      if (httpConfig.provider) {
        const { config: requiredHttpConfig } = getEmailHttpConfigDetails(agent_id, { requireCredentials: true });
        transport = {
          type: "http",
          ...(await sendEmailViaHttpProvider(requiredHttpConfig, {
            to: draft.to,
            subject: draft.subject,
            body: draft.body,
            idempotencyKey: draft_id
          }))
        };
      } else {
        const { configs } = getSmtpConfigCandidates(agent_id, { requireCredentials: true });
        transport = {
          type: "smtp",
          ...(await sendSmtpEmail(configs, { to: draft.to, subject: draft.subject, body: draft.body }))
        };
      }
      emailDraftCache.delete(draft_id);

      return out({
        agent_id,
        sent: true,
        draft_id,
        from: primaryConfig.fromAddress,
        to: draft.to,
        subject: draft.subject,
        body_bytes: draft.body_bytes,
        body_sha256: draft.body_sha256,
        transport
      });
    }
  );

  server.tool(
    "agent_email_send",
    "Sendet eine Plain-Text-E-Mail aus dem Agentenpostfach per SMTP. Live-Versand nur mit klarer Asana-Freigabe; Empfaenger sind standardmaessig frei, optional per EMAIL_ALLOWED_RECIPIENTS begrenzbar.",
    {
      agent_id: agentIdSchema,
      to: z.string(),
      subject: z.string(),
      body: z.string(),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional()
    },
    TOOL_EXTERNAL_WRITE,
    async ({ agent_id, to, subject, body, dry_run, confirmed_by_asana, asana_task_gid }) => {
      const validated = validateEmailPayload({ to, subject, body });
      const { config: httpConfig, summary: httpSummary } = getEmailHttpConfigDetails(agent_id, { requireCredentials: false });
      const { configs, primaryConfig, summary } = getSmtpConfigCandidates(agent_id, {
        requireCredentials: !dry_run && !httpConfig.provider
      });

      if (dry_run) {
        return out({
          agent_id,
          dry_run: true,
          from: primaryConfig.fromAddress,
          to: validated.recipients,
          subject: validated.subject,
          body_bytes: validated.body_bytes,
          body_sha256: validated.body_sha256,
          active_email_transport: httpSummary.email_http_provider_configured
            ? httpSummary.email_http_ready_for_send
              ? httpSummary.email_http_provider
              : null
            : summary.ready_for_send
              ? "smtp"
              : null,
          email_http_provider: httpSummary.email_http_provider,
          email_http_ready_for_send: httpSummary.email_http_ready_for_send,
          smtp_ready_for_send: summary.ready_for_send,
          smtp_candidates: summary.smtp_candidates
        });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "agent_email_send");
      let transport;
      if (httpConfig.provider) {
        const { config: requiredHttpConfig } = getEmailHttpConfigDetails(agent_id, { requireCredentials: true });
        transport = {
          type: "http",
          ...(await sendEmailViaHttpProvider(requiredHttpConfig, {
            to: validated.recipients,
            subject: validated.subject,
            body
          }))
        };
      } else {
        transport = {
          type: "smtp",
          ...(await sendSmtpEmail(configs, { to: validated.recipients, subject: validated.subject, body }))
        };
      }

      return out({
        agent_id,
        sent: true,
        from: primaryConfig.fromAddress,
        to: validated.recipients,
        subject: validated.subject,
        body_bytes: validated.body_bytes,
        body_sha256: validated.body_sha256,
        transport
      });
    }
  );

  server.tool(
    "wp_import_csv",
    "Importiert CSV-Inhalte in den Goklever-WordPress-Import-Endpunkt. Live-Ausfuehrung nur mit klarer Asana-Freigabe; dry_run prueft nur den Payload.",
    {
      agent_id: agentIdSchema,
      csv_text: z.string(),
      source: z.string().optional().default("vip-tools-mcp"),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional()
    },
    TOOL_EXTERNAL_WRITE,
    async ({ agent_id, csv_text, source, dry_run, confirmed_by_asana, asana_task_gid }) => {
      const trimmedCsv = csv_text.trim();
      if (!trimmedCsv) throw new Error("csv_text darf nicht leer sein.");
      if (Buffer.byteLength(trimmedCsv, "utf8") > 1_000_000) {
        throw new Error("csv_text ist zu gross fuer diesen MCP-Import (>1 MB).");
      }

      const lines = trimmedCsv.split(/\r?\n/).filter(Boolean);
      const preview = {
        line_count: lines.length,
        first_line: lines[0],
        source
      };

      if (dry_run) {
        return out({ agent_id, dry_run: true, preview });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "wp_import_csv");

      const apiKey = process.env.WORDPRESS_GK_API_KEY;
      if (!apiKey) throw new Error("WORDPRESS_GK_API_KEY fehlt im MCP-Environment.");

      let lastError;
      for (let attempt = 1; attempt <= 3; attempt += 1) {
        try {
          const res = await axios.post(
            WP_IMPORT_URL,
            { csv: trimmedCsv, source },
            {
              timeout: 60000,
              headers: {
                "Content-Type": "application/json",
                "x-gk-api-key": apiKey
              }
            }
          );

          return out({
            agent_id,
            status: res.status,
            response: res.data,
            attempts: attempt
          });
        } catch (error) {
          lastError = error;
          if (attempt >= 3 || !isRetryableAxiosError(error)) break;
          await sleep(2000 * attempt);
        }
      }

      if (lastError?.response) {
        return out({
          agent_id,
          ok: false,
          status: lastError.response.status,
          response: lastError.response.data
        });
      }

      throw lastError;
    }
  );

    return server;
  }

/* ---------------- EXPRESS ---------------- */

const app = express();

app.use(express.json());

app.all("/mcp", async (req, res) => {
  const server = createServer();

  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: undefined,
    enableJsonResponse: true
  });

  try {
    console.log("MCP REQUEST", req.method, req.body?.method);

    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (err) {
    console.error("MCP ERROR:", err);

    if (!res.headersSent) {
      res.status(500).send(err.message);
    }
  }
});

/* ---------------- START ---------------- */

const PORT = process.env.PORT || 3000;

const httpServer = app.listen(PORT, () => {
  console.log("MCP läuft auf Port " + PORT);
});
const keepAlive = setInterval(() => {}, 1 << 30);

process.on("SIGTERM", () => {
  clearInterval(keepAlive);
  httpServer.close(() => process.exit(0));
});
