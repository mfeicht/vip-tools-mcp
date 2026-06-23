import "dotenv/config";
import express from "express";
import axios from "axios";
import net from "net";
import tls from "tls";
import path from "path";
import { execFile } from "child_process";
import { createHash, createSign, randomUUID } from "crypto";
import { existsSync, readdirSync } from "fs";
import { promisify } from "util";
import { PDFParse } from "pdf-parse";
import { z } from "zod";

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

/* ---------------- CONFIG ---------------- */

const DEFAULT_AGENT_ID = "vip-ai-sales";
const ASANA_TIMEOUT_MS = Number(process.env.ASANA_TIMEOUT_MS || 12_000);
const ASANA_WRITE_TIMEOUT_MS = Number(process.env.ASANA_WRITE_TIMEOUT_MS || 30_000);
const ASANA_RETRY_ATTEMPTS = Math.max(1, Number(process.env.ASANA_RETRY_ATTEMPTS || 2));
const ASANA_RETRY_BASE_DELAY_MS = Math.max(0, Number(process.env.ASANA_RETRY_BASE_DELAY_MS || 250));
const execFileAsync = promisify(execFile);

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
const GOOGLE_DEFAULT_DRIVE_OAUTH_PREFIXES = ["GOOGLE_DRIVE_OAUTH"];
const GOOGLE_ACCOUNTING_DRIVE_OAUTH_PREFIXES = ["GOOGLE_ACCOUNTING_OAUTH", "GOOGLE_DRIVE_OAUTH"];
const GOOGLE_SEO_OAUTH_PREFIXES = ["GOOGLE_SEO_OAUTH", "GOOGLE_ANALYTICS_SEARCH_OAUTH", "GOOGLE_OAUTH"];
const GOOGLE_SEO_REQUIRED_SCOPES = [
  "https://www.googleapis.com/auth/analytics.readonly",
  "https://www.googleapis.com/auth/webmasters.readonly"
];
const GOOGLE_ADS_OAUTH_PREFIXES = [
  "GOOGLE_ADS_OAUTH"
];
const GOOGLE_ADS_REQUIRED_SCOPES = ["https://www.googleapis.com/auth/adwords"];
const GOOGLE_ADS_API_BASE = (process.env.GOOGLE_ADS_API_BASE || "https://googleads.googleapis.com").replace(
  /\/$/,
  ""
);
const GOOGLE_ADS_API_VERSION = process.env.GOOGLE_ADS_API_VERSION || "v22";
const GOOGLE_ADS_TIMEOUT_MS = Number(process.env.GOOGLE_ADS_TIMEOUT_MS || 30_000);
const GOOGLE_ADS_BUDGET_EXTRA_APPROVAL_PERCENT = Number(
  process.env.GOOGLE_ADS_BUDGET_EXTRA_APPROVAL_PERCENT || 12
);
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
const BUFFER_API_BASE = (process.env.BUFFER_API_BASE || "https://api.buffer.com").replace(/\/$/, "");
const BUFFER_TIMEOUT_MS = Number(process.env.BUFFER_TIMEOUT_MS || 30_000);
const CLOUDINARY_UPLOAD_TIMEOUT_MS = Number(process.env.CLOUDINARY_UPLOAD_TIMEOUT_MS || 45_000);
const CLOUDINARY_ADMIN_TIMEOUT_MS = Number(process.env.CLOUDINARY_ADMIN_TIMEOUT_MS || 30_000);
const CLOUDINARY_DEFAULT_FOLDER_PREFIX = process.env.CLOUDINARY_FOLDER_PREFIX || "vip-social-media";
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
const ASANA_ATTACHMENT_DEFAULT_CHUNK_BYTES = 24 * 1024;
const ASANA_ATTACHMENT_MAX_CHUNK_BYTES = 96 * 1024;
const ACCOUNTING_ATTACHMENT_HARD_MAX_BYTES = 16 * 1024 * 1024;
const ACCOUNTING_EMAIL_HARD_MAX_BYTES = 25 * 1024 * 1024;
const ACCOUNTING_ALLOWED_DRIVE_ROOT_IDS = new Set(
  [
    "1D7rxEBZM8gy7fqz0QY1OoaUSMoLC5i0w", // Rechnungen Kreditoren / 2026
    "1Y_H-m69sZNeOmV--u4Ibh7ymlI4bS9zp", // Einnahmen-Rechnungen
    ...(process.env.ACCOUNTING_DRIVE_ALLOWED_FOLDER_IDS || "").split(",")
  ]
    .map((value) => value.trim())
    .filter(Boolean)
);
const ACCOUNTING_ALLOWED_ATTACHMENT_EXTENSIONS = new Set([
  "pdf",
  "png",
  "jpg",
  "jpeg",
  "tif",
  "tiff",
  "webp",
  "heic",
  "heif"
]);
const ACCOUNTING_ALLOWED_ATTACHMENT_MIME_TYPES = new Set([
  "application/pdf",
  "image/png",
  "image/jpeg",
  "image/tiff",
  "image/webp",
  "image/heic",
  "image/heif"
]);
const ACCOUNTING_INLINE_INVOICE_PDF_MIME_TYPE = "application/pdf";
const ACCOUNTING_INLINE_EMAIL_RENDER_TIMEOUT_MS = Number(process.env.ACCOUNTING_INLINE_EMAIL_RENDER_TIMEOUT_MS || 30_000);
const ACCOUNTING_INLINE_EMAIL_RENDER_REMOTE_IMAGES =
  String(process.env.ACCOUNTING_INLINE_EMAIL_RENDER_REMOTE_IMAGES || "true").toLowerCase() !== "false";
const ACCOUNTING_INLINE_EMAIL_RUNTIME_CHROME_INSTALL =
  String(process.env.ACCOUNTING_INLINE_EMAIL_RUNTIME_CHROME_INSTALL || "true").toLowerCase() !== "false";
const ACCOUNTING_INLINE_EMAIL_CHROME_INSTALL_TIMEOUT_MS =
  Number(process.env.ACCOUNTING_INLINE_EMAIL_CHROME_INSTALL_TIMEOUT_MS || 180_000);
const ACCOUNTING_INLINE_EMAIL_CHROME_CACHE_DIR =
  process.env.PUPPETEER_CACHE_DIR || process.env.ACCOUNTING_INLINE_EMAIL_CHROME_CACHE_DIR || "/opt/render/.cache/puppeteer";
const ACCOUNTING_MORITZ_EMAILS = new Set(
  [
    "moritz.feichtmeyer@vip-studios.de",
    "moritz.feichtmeyer@gmail.com",
    "mf@vip-studios.de",
    ...(process.env.ACCOUNTING_MORITZ_EMAILS || "").split(",")
  ]
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
);
const ACCOUNTING_ARCHIVE_MAILBOX_CANDIDATES = [
  "Archive",
  "Archives",
  "Archiv",
  "INBOX.Archive",
  "INBOX.Archives",
  "INBOX.Archiv"
];
const ASANA_TASK_SCHEDULE_OPT_FIELDS =
  "gid,name,completed,due_on,due_at,start_on,start_at,permalink_url";
const ASANA_TASK_SCHEDULE_VERIFY_OPT_FIELDS =
  "gid,name,completed,assignee.gid,assignee.name,due_on,due_at,start_on,start_at,permalink_url,tags.gid,tags.name,followers.gid,followers.name,workspace.gid,memberships.project.gid,memberships.project.name,modified_at";
const ASANA_TASK_TAG_OPT_FIELDS = "gid,name,completed,tags.gid,tags.name,permalink_url";
const ASANA_TASK_TAG_WORKSPACE_OPT_FIELDS =
  "gid,name,completed,tags.gid,tags.name,workspace.gid,permalink_url";
const ASANA_TASK_CREATE_SOURCE_OPT_FIELDS =
  "gid,name,permalink_url,followers.gid,followers.name,workspace.gid,memberships.project.gid,memberships.project.name";
const ASANA_TASK_CREATE_VERIFY_OPT_FIELDS =
  "gid,name,completed,assignee.gid,assignee.name,due_on,due_at,permalink_url,followers.gid,followers.name,memberships.project.gid,memberships.project.name,custom_fields.gid,custom_fields.name,custom_fields.enum_value.gid,custom_fields.enum_value.name";
const ASANA_TASK_DESCRIPTION_OPT_FIELDS =
  "gid,name,completed,notes,html_notes,permalink_url,tags.gid,tags.name,assignee.gid,assignee.name,memberships.project.gid,memberships.project.name";
const ASANA_DEFAULT_SUPERVISOR_GID =
  process.env.ASANA_DEFAULT_SUPERVISOR_GID || "1108801330389276";
const ASANA_PROJECT_OPT_FIELDS = "gid,name,workspace.gid,permalink_url";
const ASANA_PROJECT_CUSTOM_FIELD_OPT_FIELDS =
  "gid,project.gid,custom_field.gid,custom_field.name,custom_field.resource_subtype,custom_field.enum_options.gid,custom_field.enum_options.name";

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

const AGENT_LOCK_MIN_TTL_SECONDS = 60;
const AGENT_LOCK_DEFAULT_TTL_SECONDS = Math.max(
  AGENT_LOCK_MIN_TTL_SECONDS,
  Number(process.env.AGENT_LOCK_DEFAULT_TTL_SECONDS) || 60 * 60
);
const AGENT_LOCK_MAX_TTL_SECONDS = Math.max(
  AGENT_LOCK_DEFAULT_TTL_SECONDS,
  Number(process.env.AGENT_LOCK_MAX_TTL_SECONDS) || 6 * 60 * 60
);
const AGENT_OPERATION_LOCKS = new Map();

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
    timeout: ASANA_TIMEOUT_MS,
    headers: {
      Authorization: `Bearer ${token}`,
      "Asana-Enable": "new_rich_text"
    }
  });
}

function isRetryableAsanaError(error) {
  const status = error?.response?.status;
  if (status && [408, 409, 425, 429, 500, 502, 503, 504].includes(status)) return true;
  const code = error?.code;
  if (code && ["ECONNRESET", "ETIMEDOUT", "ECONNABORTED", "EAI_AGAIN", "ENOTFOUND"].includes(code)) return true;
  return !error?.response;
}

async function asanaRequestWithRetry(asana, requestConfig, options = {}) {
  const attempts = Math.max(1, Number(options.attempts || ASANA_RETRY_ATTEMPTS));
  const baseDelayMs = Math.max(0, Number(options.baseDelayMs || ASANA_RETRY_BASE_DELAY_MS));

  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await asana.request(requestConfig);
    } catch (error) {
      lastError = error;
      if (attempt >= attempts || !isRetryableAsanaError(error)) throw error;
      await sleep(baseDelayMs * attempt);
    }
  }
  throw lastError;
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

async function resolveAsanaAttachmentSelection(
  asana,
  { taskGid, attachmentGid, attachmentNameContains, downloadFirstMatch = false }
) {
  let attachments = [];
  let selectedAttachmentGid = attachmentGid;

  if (taskGid) {
    attachments = await listAsanaTaskAttachments(asana, taskGid);
    if (attachmentNameContains) {
      const needle = String(attachmentNameContains).toLowerCase();
      attachments = attachments.filter((attachment) =>
        String(attachment.name || "").toLowerCase().includes(needle)
      );
    }
    if (!selectedAttachmentGid && downloadFirstMatch && attachments.length) {
      selectedAttachmentGid = attachments[0].gid;
    }
  }

  return { attachments, selectedAttachmentGid };
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
  const isTaskScheduleUpdate =
    method === "PUT" &&
    /^\/tasks\/[^/]+\/?$/.test(path) &&
    data &&
    ["due_on", "due_at", "start_on", "start_at"].some((field) =>
      Object.prototype.hasOwnProperty.call(data, field)
    );
  const isTaskRecurrenceUpdate =
    method === "PUT" &&
    /^\/tasks\/[^/]+\/?$/.test(path) &&
    data &&
    Object.prototype.hasOwnProperty.call(data, "recurrence");
  const isTaskTagUpdate = method === "POST" && /^\/tasks\/[^/]+\/(?:addTag|removeTag)\/?$/.test(path);
  const isTaskFollowerUpdate =
    method === "POST" && /^\/tasks\/[^/]+\/(?:addFollowers|removeFollowers)\/?$/.test(path);
  const isTaskCreate = method === "POST" && /^\/tasks\/?$/.test(path);

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

  if (isTaskScheduleUpdate) {
    throw new Error(
      "Asana-Faelligkeit/Startzeit darf nicht per rohem asana_request geaendert werden. Nutze asana_update_task_schedule; es validiert due_on/due_at und prueft den Readback."
    );
  }

  if (isTaskTagUpdate) {
    throw new Error(
      "Asana-Tags duerfen nicht per rohem asana_request geaendert werden. Nutze asana_update_task_tags; es ist idempotent und prueft den Readback."
    );
  }

  if (isTaskFollowerUpdate) {
    throw new Error(
      "Asana-Beteiligte/Follower duerfen nicht per rohem asana_request geaendert werden. Nutze asana_ensure_task_followers zum Hinzufuegen oder asana_leave_task_followers zum Entfernen; beide pruefen den Readback."
    );
  }

  if (isTaskRecurrenceUpdate) {
    throw new Error(
      "Asana-Wiederholungen duerfen nicht per rohem asana_request geaendert werden. Nutze asana_update_task_recurrence; es ist als experimenteller, enger Pfad markiert."
    );
  }

  if (isTaskCreate) {
    throw new Error(
      "Asana-Aufgaben duerfen nicht per rohem asana_request angelegt werden. Nutze asana_create_task; es erzwingt genau ein Projekt, Follow-up-Link, Standard-Faelligkeit, Prioritaet und Status."
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

function validateAsanaDate(value, label) {
  if (value != null && !/^\d{4}-\d{2}-\d{2}$/.test(String(value))) {
    throw new Error(`${label} muss im Format YYYY-MM-DD sein.`);
  }
}

function validateAsanaDateTime(value, label) {
  if (value == null) return;
  const text = String(value);
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?(?:Z|[+-]\d{2}:\d{2})$/.test(text)) {
    throw new Error(`${label} muss ein ISO-8601-Zeitpunkt mit Zeitzone sein, z. B. 2026-05-28T08:30:00.000Z.`);
  }
  if (!Number.isFinite(Date.parse(text))) {
    throw new Error(`${label} ist kein gueltiger ISO-8601-Zeitpunkt.`);
  }
}

function validateAsanaLocalTime(value, label) {
  if (value != null && !/^\d{2}:\d{2}(?::\d{2})?$/.test(String(value))) {
    throw new Error(`${label} muss im Format HH:mm oder HH:mm:ss sein.`);
  }
}

function getBerlinDateParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(date);
  const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return {
    year: Number(byType.year),
    month: Number(byType.month),
    day: Number(byType.day)
  };
}

function getBerlinDateString(date = new Date()) {
  const { year, month, day } = getBerlinDateParts(date);
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function getTimeZoneOffsetMinutes(timeZone, date) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    timeZoneName: "shortOffset",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  }).formatToParts(date);
  const timeZoneName = parts.find((part) => part.type === "timeZoneName")?.value || "GMT";
  if (timeZoneName === "GMT" || timeZoneName === "UTC") return 0;
  const match = /^GMT([+-])(\d{1,2})(?::?(\d{2}))?$/.exec(timeZoneName);
  if (!match) {
    throw new Error(`Zeitzonenoffset fuer ${timeZone} konnte nicht gelesen werden (${timeZoneName}).`);
  }
  const sign = match[1] === "-" ? -1 : 1;
  return sign * (Number(match[2]) * 60 + Number(match[3] || 0));
}

function berlinLocalDateTimeToUtcIso(dateString, timeString) {
  validateAsanaDate(dateString, "expected_due_date_berlin");
  validateAsanaLocalTime(timeString, "expected_due_time_berlin");
  const [year, month, day] = dateString.split("-").map(Number);
  const [hour, minute, second = 0] = String(timeString).split(":").map(Number);
  let utcMs = Date.UTC(year, month - 1, day, hour, minute, second, 0);
  for (let index = 0; index < 3; index += 1) {
    const offsetMinutes = getTimeZoneOffsetMinutes("Europe/Berlin", new Date(utcMs));
    utcMs = Date.UTC(year, month - 1, day, hour, minute, second, 0) - offsetMinutes * 60 * 1000;
  }
  return new Date(utcMs).toISOString();
}

function dueOnInBerlinDaysFromNow(days) {
  const offsetDays = Math.max(0, Number(days || 0));
  const { year, month, day } = getBerlinDateParts();
  const base = new Date(Date.UTC(year, month - 1, day));
  base.setUTCDate(base.getUTCDate() + offsetDays);
  return base.toISOString().slice(0, 10);
}

function getAsanaTaskDueGate(task, now = new Date()) {
  if (task?.due_at) {
    const dueAt = new Date(task.due_at);
    if (Number.isFinite(dueAt.getTime()) && now.getTime() < dueAt.getTime()) {
      return {
        blocked: true,
        mode: "due_at",
        reason: "task_due_at_is_in_future",
        due_at: task.due_at,
        now_iso: now.toISOString(),
        now_berlin_date: getBerlinDateString(now)
      };
    }
    return {
      blocked: false,
      mode: "due_at",
      due_at: task.due_at,
      now_iso: now.toISOString(),
      now_berlin_date: getBerlinDateString(now)
    };
  }

  if (task?.due_on) {
    const todayBerlin = getBerlinDateString(now);
    if (todayBerlin < task.due_on) {
      return {
        blocked: true,
        mode: "due_on",
        reason: "task_due_on_is_in_future",
        due_on: task.due_on,
        today_berlin: todayBerlin,
        now_iso: now.toISOString()
      };
    }
    return {
      blocked: false,
      mode: "due_on",
      due_on: task.due_on,
      today_berlin: todayBerlin,
      now_iso: now.toISOString()
    };
  }

  return {
    blocked: false,
    mode: "none",
    now_iso: now.toISOString(),
    now_berlin_date: getBerlinDateString(now)
  };
}

function getAsanaTaskDuePosition(task, now = new Date()) {
  if (task?.due_at) {
    const dueAt = new Date(task.due_at);
    const dueAtTime = dueAt.getTime();
    if (!Number.isFinite(dueAtTime)) {
      return {
        mode: "due_at",
        relation: "invalid",
        due_at: task.due_at,
        now_iso: now.toISOString(),
        now_berlin_date: getBerlinDateString(now)
      };
    }
    return {
      mode: "due_at",
      relation: dueAtTime > now.getTime() ? "future" : "past_or_now",
      due_at: task.due_at,
      now_iso: now.toISOString(),
      now_berlin_date: getBerlinDateString(now)
    };
  }

  if (task?.due_on) {
    const todayBerlin = getBerlinDateString(now);
    return {
      mode: "due_on",
      relation: task.due_on > todayBerlin ? "future" : "past_or_today",
      due_on: task.due_on,
      today_berlin: todayBerlin,
      now_iso: now.toISOString()
    };
  }

  return {
    mode: "none",
    relation: "none",
    now_iso: now.toISOString(),
    now_berlin_date: getBerlinDateString(now)
  };
}

function resolveNextFutureAsanaScheduleInput({
  next_future_due_on,
  next_future_due_at,
  next_future_due_date_berlin,
  next_future_due_time_berlin
}, now = new Date()) {
  validateAsanaDate(next_future_due_on, "next_future_due_on");
  validateAsanaDateTime(next_future_due_at, "next_future_due_at");
  validateAsanaDate(next_future_due_date_berlin, "next_future_due_date_berlin");
  validateAsanaLocalTime(next_future_due_time_berlin, "next_future_due_time_berlin");

  const hasBerlinLocalDue = Boolean(next_future_due_date_berlin || next_future_due_time_berlin);
  if (hasBerlinLocalDue && !(next_future_due_date_berlin && next_future_due_time_berlin)) {
    throw new Error("next_future_due_date_berlin und next_future_due_time_berlin muessen gemeinsam gesetzt werden.");
  }
  const setCount = [Boolean(next_future_due_on), Boolean(next_future_due_at), hasBerlinLocalDue].filter(Boolean).length;
  if (setCount !== 1) {
    throw new Error("Genau eines von next_future_due_on, next_future_due_at oder Berlin-Date/Time muss gesetzt sein.");
  }

  if (next_future_due_on) {
    const todayBerlin = getBerlinDateString(now);
    if (next_future_due_on <= todayBerlin) {
      throw new Error(
        `next_future_due_on muss nach heute liegen, damit kein Catch-up-Backlog entsteht (heute=${todayBerlin}).`
      );
    }
    return {
      mode: "due_on",
      data: { due_on: next_future_due_on },
      expected_due_on: next_future_due_on,
      expected_due_at: null,
      now_iso: now.toISOString(),
      today_berlin: todayBerlin
    };
  }

  const resolvedDueAt = next_future_due_at
    ? new Date(next_future_due_at).toISOString()
    : berlinLocalDateTimeToUtcIso(next_future_due_date_berlin, next_future_due_time_berlin);
  if (new Date(resolvedDueAt).getTime() <= now.getTime()) {
    throw new Error(
      `next_future_due_at muss nach jetzt liegen, damit kein Catch-up-Backlog entsteht (jetzt=${now.toISOString()}).`
    );
  }
  return {
    mode: "due_at",
    data: { due_at: resolvedDueAt },
    expected_due_on: null,
    expected_due_at: resolvedDueAt,
    now_iso: now.toISOString(),
    today_berlin: getBerlinDateString(now)
  };
}

function isRoutineLikeAsanaTask(task) {
  const tagNames = (task?.tags || []).map((tag) => String(tag.name || "").toLowerCase());
  return tagNames.includes("routine") || /^r\s*:/i.test(String(task?.name || "").trim());
}

function getAsanaTaskTagNames(task) {
  return (task?.tags || []).map((tag) => String(tag.name || ""));
}

function getAsanaTaskProjectGids(task) {
  return uniqueValues(
    (task?.memberships || [])
      .map((membership) => membership?.project?.gid)
      .filter(Boolean)
  );
}

function getAsanaTaskFollowerGids(task) {
  return uniqueValues((task?.followers || []).map((follower) => follower?.gid).filter(Boolean));
}

function isDefaultSupervisorFollowerGid(followerGid) {
  return String(followerGid || "") === String(ASANA_DEFAULT_SUPERVISOR_GID || "");
}

function isRoutineSupervisorDoNotReaddCandidate(task, followerGid) {
  return isRoutineLikeAsanaTask(task) && isDefaultSupervisorFollowerGid(followerGid);
}

function buildAsanaScheduleVerification({
  task,
  expected_due_on,
  expected_due_at,
  expected_due_date_berlin,
  expected_due_time_berlin,
  expect_due_time,
  expect_routine_tag,
  expected_assignee_gid,
  expected_project_gid,
  expected_follower_gid,
  expected_follower_gids,
  expected_name_contains,
  expected_completed,
  recurrence_expected,
  allow_unverifiable_native_recurrence,
  allow_routine_supervisor_do_not_readd
}) {
  validateAsanaDate(expected_due_on, "expected_due_on");
  validateAsanaDateTime(expected_due_at, "expected_due_at");
  validateAsanaDate(expected_due_date_berlin, "expected_due_date_berlin");
  validateAsanaLocalTime(expected_due_time_berlin, "expected_due_time_berlin");
  validateAsanaGid(expected_assignee_gid, "expected_assignee_gid");
  validateAsanaGid(expected_project_gid, "expected_project_gid");
  validateAsanaGid(expected_follower_gid, "expected_follower_gid");
  const followerExpectations = uniqueValues([
    ...(expected_follower_gids || []),
    ...(expected_follower_gid ? [expected_follower_gid] : [])
  ]);
  for (const followerGid of followerExpectations) validateAsanaGid(followerGid, "expected_follower_gids");

  const hasBerlinLocalExpectation = Boolean(expected_due_date_berlin || expected_due_time_berlin);
  if (hasBerlinLocalExpectation && !(expected_due_date_berlin && expected_due_time_berlin)) {
    throw new Error("expected_due_date_berlin und expected_due_time_berlin muessen gemeinsam gesetzt werden.");
  }
  if (expected_due_at && hasBerlinLocalExpectation) {
    throw new Error("expected_due_at nicht zusammen mit expected_due_date_berlin/expected_due_time_berlin setzen.");
  }
  if (expected_due_on && (expected_due_at || hasBerlinLocalExpectation)) {
    throw new Error("expected_due_on nicht zusammen mit erwarteter Uhrzeit setzen.");
  }

  const resolvedExpectedDueAt = expected_due_at || (hasBerlinLocalExpectation
    ? berlinLocalDateTimeToUtcIso(expected_due_date_berlin, expected_due_time_berlin)
    : undefined);
  const checks = [];
  const addCheck = ({ name, ok, expected, actual, severity = "error", note }) => {
    checks.push({ name, ok: Boolean(ok), expected, actual, severity, note });
  };

  if (expected_due_on) {
    addCheck({
      name: "due_on_exact",
      ok: task?.due_on === expected_due_on && !task?.due_at,
      expected: expected_due_on,
      actual: { due_on: task?.due_on || null, due_at: task?.due_at || null },
      note: "due_on ist ganztags; wenn eine Uhrzeit vorgesehen ist, expected_due_at oder Berlin-Date/Time nutzen."
    });
  }
  if (resolvedExpectedDueAt) {
    addCheck({
      name: "due_at_exact",
      ok: task?.due_at === resolvedExpectedDueAt,
      expected: resolvedExpectedDueAt,
      actual: task?.due_at || null,
      note: hasBerlinLocalExpectation
        ? `Berechnet aus Europe/Berlin ${expected_due_date_berlin} ${expected_due_time_berlin}.`
        : undefined
    });
  }
  if (expect_due_time) {
    addCheck({
      name: "due_time_present",
      ok: Boolean(task?.due_at),
      expected: "due_at gesetzt",
      actual: task?.due_at || null
    });
  }
  if (expect_routine_tag) {
    const tagNames = getAsanaTaskTagNames(task);
    addCheck({
      name: "routine_tag_present",
      ok: tagNames.some((name) => name.toLowerCase() === "routine"),
      expected: "Routine",
      actual: tagNames
    });
  }
  if (expected_assignee_gid) {
    addCheck({
      name: "assignee_exact",
      ok: task?.assignee?.gid === expected_assignee_gid,
      expected: expected_assignee_gid,
      actual: task?.assignee?.gid || null
    });
  }
  if (expected_project_gid) {
    const projectGids = getAsanaTaskProjectGids(task);
    addCheck({
      name: "project_present",
      ok: projectGids.includes(expected_project_gid),
      expected: expected_project_gid,
      actual: projectGids
    });
  }
  if (followerExpectations.length) {
    const followerGids = getAsanaTaskFollowerGids(task);
    const missingFollowerGids = followerExpectations.filter((followerGid) => !followerGids.includes(followerGid));
    const allowedMissingRoutineSupervisorGids = missingFollowerGids.filter((followerGid) =>
      allow_routine_supervisor_do_not_readd && isRoutineSupervisorDoNotReaddCandidate(task, followerGid)
    );
    const hardMissingFollowerGids = missingFollowerGids.filter(
      (followerGid) => !allowedMissingRoutineSupervisorGids.includes(followerGid)
    );
    addCheck({
      name: "followers_present",
      ok: hardMissingFollowerGids.length === 0,
      expected: followerExpectations,
      actual: followerGids,
      severity: hardMissingFollowerGids.length === 0 && allowedMissingRoutineSupervisorGids.length ? "warning" : "error",
      note:
        allowedMissingRoutineSupervisorGids.length
          ? "Routine-Aufgabe ohne Default-Supervisor-Follower wird als moeglicher do_not_readd-Fall behandelt; nicht automatisch readden, solange die Routine normal erfolgreich abgeschlossen werden kann."
          : "Pflicht fuer relevante Agenten-Aufgaben: Hauptvorgesetzter/Moritz muss als Beteiligter/Follower sichtbar sein."
    });
  }
  if (expected_name_contains) {
    addCheck({
      name: "name_contains",
      ok: String(task?.name || "").toLowerCase().includes(String(expected_name_contains).toLowerCase()),
      expected: expected_name_contains,
      actual: task?.name || null
    });
  }
  if (expected_completed !== undefined) {
    addCheck({
      name: "completed_exact",
      ok: Boolean(task?.completed) === Boolean(expected_completed),
      expected: Boolean(expected_completed),
      actual: Boolean(task?.completed)
    });
  }
  if (recurrence_expected) {
    addCheck({
      name: "native_recurrence_api_verification",
      ok: false,
      expected: "API-verifizierbare native Asana-Wiederholung",
      actual: "not_api_verifiable",
      severity: allow_unverifiable_native_recurrence ? "warning" : "error",
      note:
        "Native Asana-Wiederholung ist API-seitig nicht zuverlaessig auslesbar. Nicht als 100%-verifiziert behaupten; UI-Abnahme oder asana_verify_next_routine_instance nach Abschluss nutzen."
    });
  }

  const errors = checks.filter((check) => !check.ok && check.severity !== "warning");
  const warnings = checks.filter((check) => !check.ok && check.severity === "warning");
  const verification_status = errors.length ? "failed" : warnings.length ? "ok_with_warnings" : "ok";
  return {
    verification_status,
    resolved_expected_due_at: resolvedExpectedDueAt || null,
    checks,
    errors,
    warnings
  };
}

function clampAgentLockTtlSeconds(value) {
  const parsed = Number(value || AGENT_LOCK_DEFAULT_TTL_SECONDS);
  const fallback = Number.isFinite(parsed) ? parsed : AGENT_LOCK_DEFAULT_TTL_SECONDS;
  return Math.min(
    Math.max(Math.round(fallback), AGENT_LOCK_MIN_TTL_SECONDS),
    Math.max(AGENT_LOCK_MIN_TTL_SECONDS, AGENT_LOCK_MAX_TTL_SECONDS)
  );
}

function cleanupAgentOperationLocks(nowMs = Date.now()) {
  for (const [key, lock] of AGENT_OPERATION_LOCKS.entries()) {
    if (!lock?.expires_at_ms || lock.expires_at_ms <= nowMs) {
      AGENT_OPERATION_LOCKS.delete(key);
    }
  }
}

function buildAgentOperationLockKey({ scope, agent_id, task_gid, resource_key }) {
  if (scope === "task") {
    if (!task_gid) throw new Error("task_gid ist fuer scope=task erforderlich.");
    validateAsanaGid(task_gid, "task_gid");
    return `task:${task_gid}`;
  }
  if (scope === "agent") {
    return `agent:${agent_id}`;
  }
  if (scope === "resource") {
    const key = String(resource_key || "").trim();
    if (key.length < 3) {
      throw new Error("resource_key ist fuer scope=resource erforderlich und muss mindestens 3 Zeichen haben.");
    }
    return `resource:${key}`;
  }
  throw new Error(`Unbekannter Lock-Scope: ${scope}`);
}

function serializeAgentOperationLock(lock, nowMs = Date.now()) {
  if (!lock) return null;
  return {
    key: lock.key,
    scope: lock.scope,
    agent_id: lock.agent_id,
    task_gid: lock.task_gid || null,
    resource_key: lock.resource_key || null,
    run_id: lock.run_id,
    holder: lock.holder,
    purpose: lock.purpose,
    lock_token: lock.lock_token,
    acquired_at: new Date(lock.acquired_at_ms).toISOString(),
    renewed_at: new Date(lock.renewed_at_ms).toISOString(),
    expires_at: new Date(lock.expires_at_ms).toISOString(),
    seconds_remaining: Math.max(0, Math.ceil((lock.expires_at_ms - nowMs) / 1000))
  };
}

function normalizeAsanaLabel(value) {
  return String(value || "")
    .replace(/ß/g, "ss")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function uniqueValues(values = []) {
  return [...new Set((values || []).map((value) => String(value)).filter(Boolean))];
}

function uniqueObjectsByGid(values = []) {
  const seen = new Set();
  const result = [];
  for (const value of values || []) {
    if (!value?.gid || seen.has(value.gid)) continue;
    seen.add(value.gid);
    result.push(value);
  }
  return result;
}

function buildAsanaTaskHtmlNotes({ description, source_task, project_selection_reason }) {
  const parts = [];

  if (source_task?.permalink_url) {
    parts.push(
      `<strong>Bezug:</strong> <a href="${escapeAsanaXml(source_task.permalink_url)}">Ausgangsaufgabe ${escapeAsanaXml(
        source_task.gid
      )}</a>\n\n`
    );
  }

  if (description) {
    ensureNoUnsafeAsanaText(description, "Asana-Aufgabenbeschreibung");
    parts.push(`${escapeAsanaTextWithLinks(description)}\n\n`);
  }

  if (project_selection_reason) {
    ensureNoUnsafeAsanaText(project_selection_reason, "Asana-Projektbegruendung");
    parts.push(`<strong>Projektzuordnung:</strong> ${escapeAsanaTextWithLinks(project_selection_reason)}\n\n`);
  }

  if (!parts.length) {
    throw new Error("asana_create_task braucht description oder source_task_gid fuer eine nachvollziehbare Beschreibung.");
  }

  const html = `<body>${parts.join("")}</body>`;
  assertGeneratedAsanaHtml(html);
  return html;
}

function buildAsanaDescriptionAppendSection({ section_title, section_text, dedupe_key }) {
  ensureNoUnsafeAsanaText(section_title, "Asana-Aufgabenbeschreibung-Ueberschrift");
  ensureNoUnsafeAsanaText(section_text, "Asana-Aufgabenbeschreibung-Ergaenzung");
  ensureNoUnsafeAsanaText(dedupe_key, "Asana-Aufgabenbeschreibung-Dedupe-Key");

  const parts = [];
  if (dedupe_key) {
    parts.push(`<strong>Update-Key:</strong> ${escapeAsanaXml(dedupe_key)}\n`);
  }
  if (section_title) {
    parts.push(`<strong>${escapeAsanaXml(section_title)}</strong>\n`);
  }
  parts.push(`${escapeAsanaTextWithLinks(section_text)}\n`);

  const sectionHtml = parts.join("");
  assertGeneratedAsanaHtml(`<body>${sectionHtml}</body>`);
  return sectionHtml;
}

function appendAsanaHtmlNotesSection(existingHtmlNotes, sectionHtml) {
  const existing = String(existingHtmlNotes || "").trim();
  if (!existing) return `<body>${sectionHtml}</body>`;
  if (/^<body(?:\s[^>]*)?>[\s\S]*<\/body>\s*$/i.test(existing)) {
    return existing.replace(/<\/body>\s*$/i, `\n\n${sectionHtml}</body>`);
  }
  return `<body>${existing}\n\n${sectionHtml}</body>`;
}

function findProjectEnumField(fields, fieldAliases, valueAliases) {
  const fieldAliasSet = new Set(fieldAliases.map(normalizeAsanaLabel));
  const valueAliasSet = new Set(valueAliases.map(normalizeAsanaLabel));

  const matchingField = fields.find((field) => {
    if (!field?.gid || field.resource_subtype !== "enum") return false;
    return fieldAliasSet.has(normalizeAsanaLabel(field.name));
  });

  if (!matchingField) return { field: null, option: null };

  const option = (matchingField.enum_options || []).find((candidate) =>
    valueAliasSet.has(normalizeAsanaLabel(candidate.name))
  );
  return { field: matchingField, option: option || null };
}

async function resolveAsanaStandardCustomFields(
  asana,
  projectGid,
  { priorityValue = "Mittel", statusValue = "Todo", requireStandardCustomFields = true } = {}
) {
  const settingsRes = await asanaRequestWithRetry(asana, {
    method: "GET",
    url: `/projects/${projectGid}/custom_field_settings`,
    params: { limit: 100, opt_fields: ASANA_PROJECT_CUSTOM_FIELD_OPT_FIELDS }
  });
  const customFields = (settingsRes.data.data || [])
    .map((setting) => setting.custom_field)
    .filter(Boolean);

  const priorityAliases = [priorityValue, "Mittel", "Medium"];
  const statusAliases = [statusValue, "Todo", "To Do", "To-do"];
  const priority = findProjectEnumField(customFields, ["Prioritaet", "Prioritat", "Priority"], priorityAliases);
  const status = findProjectEnumField(customFields, ["Status"], statusAliases);

  const missing = [];
  const custom_fields = {};
  const resolved = {};

  if (priority.field?.gid && priority.option?.gid) {
    custom_fields[priority.field.gid] = priority.option.gid;
    resolved.priority = { field: priority.field, option: priority.option };
  } else {
    missing.push(`Prioritaet=${priorityValue}`);
  }

  if (status.field?.gid && status.option?.gid) {
    custom_fields[status.field.gid] = status.option.gid;
    resolved.status = { field: status.field, option: status.option };
  } else {
    missing.push(`Status=${statusValue}`);
  }

  if (missing.length && requireStandardCustomFields) {
    throw new Error(
      `Projekt ${projectGid} hat die benoetigten Standardfelder/-optionen nicht sichtbar: ${missing.join(
        ", "
      )}. Waehle ein passendes Projekt oder setze require_standard_custom_fields=false mit Projektbegruendung.`
    );
  }

  return { custom_fields, resolved, missing, settings: settingsRes.data.data || [] };
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

function googleDriveAuthSummary(agentId) {
  let serviceAccount = null;
  let serviceAccountError = null;
  try {
    serviceAccount = parseGoogleServiceAccount();
  } catch (error) {
    serviceAccountError = error?.message || String(error);
  }
  const oauthPrefixes = getGoogleDriveOAuthPrefixesForAgent(agentId);
  const authPriority = String(
    process.env.GOOGLE_DRIVE_AUTH_PRIORITY || process.env.GOOGLE_AUTH_PRIORITY || ""
  ).toLowerCase();
  return {
    agent_id: agentId,
    google_scopes: GOOGLE_SCOPES,
    service_account: {
      configured: Boolean(process.env.GOOGLE_SERVICE_ACCOUNT_JSON || process.env.GOOGLE_SERVICE_ACCOUNT_JSON_BASE64),
      parse_ok: Boolean(serviceAccount),
      client_email: serviceAccount?.client_email || null,
      error: serviceAccountError
    },
    oauth: oauthEnvSummary(oauthPrefixes),
    auth_priority: authPriority || (serviceAccount ? "service_account" : "oauth"),
    fallback_behavior:
      "Drive/Sheets nutzt nur Service Account oder Drive/Sheets-OAuth-Prefixe (GOOGLE_DRIVE_OAUTH, bei Accounting zusaetzlich GOOGLE_ACCOUNTING_OAUTH). GOOGLE_OAUTH ist fuer GA4/GSC reserviert und wird nicht fuer Drive/Sheets genutzt. Bei Drive-OAuth invalid_grant wird auf Service Account ausgewichen, sofern vorhanden."
  };
}

function getGoogleDriveOAuthPrefixesForAgent(agentId) {
  return agentId === "vip-ai-accounting"
    ? GOOGLE_ACCOUNTING_DRIVE_OAUTH_PREFIXES
    : GOOGLE_DEFAULT_DRIVE_OAUTH_PREFIXES;
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

  let res;
  try {
    res = await axios.post("https://oauth2.googleapis.com/token", params.toString(), {
      headers: { "Content-Type": "application/x-www-form-urlencoded" }
    });
  } catch (error) {
    const status = error?.response?.status;
    const data = error?.response?.data;
    let dataSnippet = "";
    try {
      if (data !== undefined) dataSnippet = JSON.stringify(data).slice(0, 2000);
    } catch {
      dataSnippet = String(data).slice(0, 2000);
    }
    throw new Error(
      `Google OAuth token refresh failed for prefix=${oauthConfig.prefix} (${status || "unknown"})` +
        (dataSnippet ? ` | response=${dataSnippet}` : "")
    );
  }

  tokenCache.accessToken = res.data.access_token;
  tokenCache.expiresAt = now + Number(res.data.expires_in || 3600);
  tokenCache.envPrefix = oauthConfig.prefix;
  return tokenCache.accessToken;
}

async function refreshGoogleServiceAccountAccessToken(serviceAccount) {
  const now = Math.floor(Date.now() / 1000);

  if (
    googleTokenCache.accessToken &&
    googleTokenCache.expiresAt > now + 60 &&
    googleTokenCache.envPrefix === "GOOGLE_SERVICE_ACCOUNT"
  ) {
    return googleTokenCache.accessToken;
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

  let res;
  try {
    res = await axios.post("https://oauth2.googleapis.com/token", params.toString(), {
      headers: { "Content-Type": "application/x-www-form-urlencoded" }
    });
  } catch (error) {
    const status = error?.response?.status;
    const data = error?.response?.data;
    let dataSnippet = "";
    try {
      if (data !== undefined) dataSnippet = JSON.stringify(data).slice(0, 2000);
    } catch {
      dataSnippet = String(data).slice(0, 2000);
    }
    const impersonate = process.env.GOOGLE_WORKSPACE_IMPERSONATE_EMAIL
      ? ` impersonate=${process.env.GOOGLE_WORKSPACE_IMPERSONATE_EMAIL}`
      : "";
    throw new Error(
      `Google Service-Account token request failed (${status || "unknown"})${impersonate}` +
        (dataSnippet ? ` | response=${dataSnippet}` : "")
    );
  }

  googleTokenCache = {
    accessToken: res.data.access_token,
    expiresAt: now + Number(res.data.expires_in || 3600),
    envPrefix: "GOOGLE_SERVICE_ACCOUNT"
  };
  return googleTokenCache.accessToken;
}

async function getGoogleAccessToken({ agent_id } = {}) {
  const oauthPrefixes = getGoogleDriveOAuthPrefixesForAgent(agent_id);
  const oauthConfig = getFirstOAuthConfig(oauthPrefixes);
  let serviceAccount = null;
  let serviceAccountParseError = null;
  try {
    serviceAccount = parseGoogleServiceAccount();
  } catch (error) {
    serviceAccountParseError = error;
  }

  const authPriority = String(
    process.env.GOOGLE_DRIVE_AUTH_PRIORITY || process.env.GOOGLE_AUTH_PRIORITY || ""
  ).toLowerCase();
  const preferServiceAccount = authPriority
    ? ["service", "service_account", "service-account", "sa"].includes(authPriority)
    : Boolean(serviceAccount);

  if (preferServiceAccount) {
    if (serviceAccount) {
      try {
        return await refreshGoogleServiceAccountAccessToken(serviceAccount);
      } catch (serviceError) {
        if (!oauthConfig) throw serviceError;
      }
    } else if (serviceAccountParseError && !oauthConfig) {
      throw serviceAccountParseError;
    }
  }

  if (oauthConfig) {
    try {
      return await refreshGoogleOAuthAccessToken(oauthConfig, googleTokenCache);
    } catch (oauthError) {
      if (serviceAccount) {
        return refreshGoogleServiceAccountAccessToken(serviceAccount);
      }
      throw oauthError;
    }
  }

  if (serviceAccount) {
    return refreshGoogleServiceAccountAccessToken(serviceAccount);
  }
  if (serviceAccountParseError) throw serviceAccountParseError;

  throw new Error(
    `Google-Zugang fehlt. Setze GOOGLE_SERVICE_ACCOUNT_JSON(_BASE64) oder ${oauthPrefixes.map(
      (prefix) => `${prefix}_CLIENT_ID/${prefix}_CLIENT_SECRET/${prefix}_REFRESH_TOKEN`
    ).join(" oder ")}.`
  );
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

function escapeGaqlString(value) {
  return String(value || "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function normalizeObjectKey(key) {
  return String(key || "").replace(/[_-]/g, "").toLowerCase();
}

function findByNormalizedKey(value, wantedKeys) {
  if (!value || typeof value !== "object") return undefined;
  const wanted = new Set(wantedKeys.map(normalizeObjectKey));
  for (const [key, nestedValue] of Object.entries(value)) {
    if (wanted.has(normalizeObjectKey(key))) return nestedValue;
  }
  return undefined;
}

function collectNormalizedKeys(value, prefix = "") {
  if (!value || typeof value !== "object") return [];
  const keys = [];
  for (const [key, nestedValue] of Object.entries(value)) {
    const path = prefix ? `${prefix}.${key}` : key;
    keys.push({ raw: path, normalized: normalizeObjectKey(path), value: nestedValue });
    if (nestedValue && typeof nestedValue === "object" && !Array.isArray(nestedValue)) {
      keys.push(...collectNormalizedKeys(nestedValue, path));
    }
  }
  return keys;
}

function googleAdsOperationType(operation) {
  if (!operation || typeof operation !== "object") return "unknown";
  if (operation.create) return "create";
  if (operation.update) return "update";
  if (operation.remove) return "remove";
  return "unknown";
}

function googleAdsOperationPayload(operation) {
  const type = googleAdsOperationType(operation);
  if (type === "create") return operation.create;
  if (type === "update") return operation.update;
  if (type === "remove") return operation.remove;
  return operation;
}

function numericMicros(value) {
  if (value === undefined || value === null || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function hasAnyNormalizedKey(payload, names) {
  const wanted = new Set(names.map(normalizeObjectKey));
  return collectNormalizedKeys(payload).some((item) => wanted.has(item.normalized));
}

function findNormalizedValue(payload, names) {
  const wanted = new Set(names.map(normalizeObjectKey));
  return collectNormalizedKeys(payload).find((item) => wanted.has(item.normalized))?.value;
}

function pushGoogleAdsRisk(risks, risk) {
  risks.push({
    severity: risk.severity || "high",
    code: risk.code,
    message: risk.message,
    operation_index: risk.operation_index,
    details: risk.details || {}
  });
}

async function lookupCampaignBudgetAmountMicros({ customerId, resourceName, loginCustomerId }) {
  const normalizedCustomerId = normalizeGoogleAdsCustomerId(customerId);
  const res = await googleAdsRequest({
    method: "POST",
    path: `/customers/${normalizedCustomerId}/googleAds:search`,
    login_customer_id: loginCustomerId,
    data: {
      query: `
        SELECT
          campaign_budget.resource_name,
          campaign_budget.amount_micros
        FROM campaign_budget
        WHERE campaign_budget.resource_name = '${escapeGaqlString(resourceName)}'
        LIMIT 1
      `,
      pageSize: 1
    }
  });
  return numericMicros(res.data.results?.[0]?.campaignBudget?.amountMicros);
}

async function assessGoogleAdsMutateSafety({ customerId, service, operations, loginCustomerId }) {
  const risks = [];
  const thresholdPercent = GOOGLE_ADS_BUDGET_EXTRA_APPROVAL_PERCENT;
  const highRiskCreateServices = new Set(["campaignBudgets", "campaigns", "adGroups", "adGroupAds"]);

  for (const [index, operation] of operations.entries()) {
    const type = googleAdsOperationType(operation);
    const payload = googleAdsOperationPayload(operation);

    if (type === "unknown") {
      pushGoogleAdsRisk(risks, {
        code: "unknown_operation_type",
        operation_index: index,
        message: "Operationstyp konnte nicht eindeutig erkannt werden."
      });
      continue;
    }

    if (type === "remove") {
      pushGoogleAdsRisk(risks, {
        code: "remove_operation",
        operation_index: index,
        message: "Entfernende Google-Ads-Operationen brauchen Extra-Freigabe."
      });
    }

    if (type === "create" && highRiskCreateServices.has(service)) {
      pushGoogleAdsRisk(risks, {
        code: "high_risk_create",
        operation_index: index,
        message: "Neue Budgets, Kampagnen, Anzeigengruppen oder Anzeigen brauchen Extra-Freigabe."
      });
    }

    const status = String(findNormalizedValue(payload, ["status"]) || "").toUpperCase();
    if (status === "ENABLED" || status === "REMOVED") {
      pushGoogleAdsRisk(risks, {
        code: "status_change_high_risk",
        operation_index: index,
        message: "Aktivieren oder Entfernen von Ads-Objekten braucht Extra-Freigabe.",
        details: { status }
      });
    }

    if (
      hasAnyNormalizedKey(payload, [
        "biddingStrategy",
        "biddingStrategyType",
        "targetCpaMicros",
        "targetRoas",
        "maximizeConversions",
        "maximizeConversionValue",
        "manualCpc",
        "cpcBidMicros",
        "percentCpcBidMicros"
      ])
    ) {
      pushGoogleAdsRisk(risks, {
        code: "bidding_or_bid_change",
        operation_index: index,
        message: "Gebots-, Ziel-CPA-/ROAS- oder Bidding-Strategie-Aenderungen brauchen Extra-Freigabe."
      });
    }

    if (hasAnyNormalizedKey(payload, ["finalUrls", "finalUrlSuffix", "trackingUrlTemplate"])) {
      pushGoogleAdsRisk(risks, {
        code: "landing_page_or_tracking_change",
        operation_index: index,
        message: "Aenderungen an Ziel-URLs oder Tracking-Templates brauchen Extra-Freigabe."
      });
    }

    const keyword = findByNormalizedKey(payload, ["keyword"]);
    const matchType = String(findByNormalizedKey(keyword, ["matchType"]) || "").toUpperCase();
    if (matchType === "BROAD") {
      pushGoogleAdsRisk(risks, {
        code: "broad_match_keyword",
        operation_index: index,
        message: "Broad-Match-Keyword-Aenderungen brauchen Extra-Freigabe."
      });
    }

    if (service === "campaignBudgets" && type === "update") {
      const newAmountMicros = numericMicros(findNormalizedValue(payload, ["amountMicros", "amount_micros"]));
      if (newAmountMicros !== null) {
        const resourceName = findNormalizedValue(payload, ["resourceName", "resource_name"]);
        let currentAmountMicros = null;
        let lookupError = null;
        if (resourceName) {
          try {
            currentAmountMicros = await lookupCampaignBudgetAmountMicros({
              customerId,
              resourceName,
              loginCustomerId
            });
          } catch (error) {
            lookupError = compactAxiosError(error);
          }
        }

        if (currentAmountMicros === null || currentAmountMicros <= 0) {
          pushGoogleAdsRisk(risks, {
            code: "budget_change_current_unknown",
            operation_index: index,
            message: "Budgetaenderung erkannt, aber aktuelles Budget konnte nicht sicher verglichen werden.",
            details: { resource_name: resourceName || null, new_amount_micros: newAmountMicros, lookup_error: lookupError }
          });
        } else {
          const increasePercent = ((newAmountMicros - currentAmountMicros) / currentAmountMicros) * 100;
          if (increasePercent > thresholdPercent) {
            pushGoogleAdsRisk(risks, {
              code: "budget_increase_over_threshold",
              operation_index: index,
              message: `Budgeterhoehung ueber ${thresholdPercent}% braucht Extra-Freigabe.`,
              details: {
                resource_name: resourceName,
                current_amount_micros: currentAmountMicros,
                new_amount_micros: newAmountMicros,
                increase_percent: Number(increasePercent.toFixed(2)),
                threshold_percent: thresholdPercent
              }
            });
          }
        }
      }
    }
  }

  return {
    requires_extra_approval: risks.length > 0,
    budget_extra_approval_threshold_percent: thresholdPercent,
    risks
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

async function googleRequest(config, googleContext = {}) {
  const { agent_id: configAgentId, ...requestConfig } = config || {};
  const accessToken = await getGoogleAccessToken({ agent_id: googleContext.agent_id || configAgentId });
  try {
    return await axios.request({
      ...requestConfig,
      headers: {
        Authorization: `Bearer ${accessToken}`,
        ...(requestConfig.headers || {})
      }
    });
  } catch (error) {
    const status = error?.response?.status;
    const statusText = error?.response?.statusText;
    const data = error?.response?.data;
    let dataSnippet = "";
    try {
      if (data !== undefined) dataSnippet = JSON.stringify(data).slice(0, 2000);
    } catch {
      dataSnippet = String(data).slice(0, 2000);
    }

    const method = String(requestConfig?.method || "GET").toUpperCase();
    const url = String(requestConfig?.url || "");
    throw new Error(
      `Google API request failed (${status || "unknown"}${statusText ? ` ${statusText}` : ""}) for ${method} ${url}` +
        (dataSnippet ? ` | response=${dataSnippet}` : "")
    );
  }
}

async function getDriveFile(fileId, fields = "id,name,mimeType,parents,webViewLink", googleContext = {}) {
  const res = await googleRequest({
    method: "GET",
    url: `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    params: {
      fields,
      supportsAllDrives: true
    }
  }, googleContext);
  return res.data;
}

async function assertAllowedGoogleFolder(folderId, googleContext = {}) {
  if (GOOGLE_ALLOWED_FOLDER_IDS.has(folderId)) return;

  let currentId = folderId;
  for (let depth = 0; depth < 10; depth += 1) {
    const folder = await getDriveFile(currentId, "id,name,mimeType,parents", googleContext);
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

async function uploadBufferToDrive({
  name,
  mimeType,
  targetFolderId,
  bytes,
  appProperties,
  assertFolder = assertAllowedGoogleFolder,
  googleContext = {}
}) {
  await assertFolder(targetFolderId, googleContext);
  const boundary = `vip-tools-${randomUUID()}`;
  const metadata = JSON.stringify({
    name,
    mimeType,
    parents: [targetFolderId],
    ...(appProperties && Object.keys(appProperties).length ? { appProperties } : {})
  });
  const body = Buffer.concat([
    Buffer.from(
      `--${boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n${metadata}\r\n` +
        `--${boundary}\r\nContent-Type: ${mimeType || "application/octet-stream"}\r\n\r\n`,
      "utf8"
    ),
    bytes,
    Buffer.from(`\r\n--${boundary}--`, "utf8")
  ]);

  const res = await googleRequest(
    {
      method: "POST",
      url: "https://www.googleapis.com/upload/drive/v3/files",
      params: {
        uploadType: "multipart",
        fields: "id,name,mimeType,parents,webViewLink,webContentLink",
        supportsAllDrives: true
      },
      headers: {
        "Content-Type": `multipart/related; boundary=${boundary}`,
        "Content-Length": String(body.length)
      },
      maxBodyLength: Infinity,
      data: body
    },
    googleContext
  );
  return res.data;
}

async function getSheetIdByName(spreadsheetId, sheetName, googleContext = {}) {
  const res = await googleRequest({
    method: "GET",
    url: `https://sheets.googleapis.com/v4/spreadsheets/${encodeURIComponent(spreadsheetId)}`,
    params: { fields: "sheets(properties(sheetId,title))" }
  }, googleContext);
  const sheet = (res.data.sheets || []).find((item) => item.properties?.title === sheetName);
  if (!sheet) throw new Error(`Sheet nicht gefunden: ${sheetName}`);
  return sheet.properties.sheetId;
}

function buildSheetRange(sheetName, range) {
  return `${sheetName}!${range}`;
}

async function getSheetValues(spreadsheetId, range, valueRenderOption = "FORMATTED_VALUE", googleContext = {}) {
  const res = await googleRequest({
    method: "GET",
    url: `https://sheets.googleapis.com/v4/spreadsheets/${encodeURIComponent(
      spreadsheetId
    )}/values/${encodeURIComponent(range)}`,
    params: { valueRenderOption }
  }, googleContext);
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

function extractEmailAddress(value) {
  const raw = String(value || "").trim();
  const bracket = /<([^>]+)>/.exec(raw);
  const candidate = bracket ? bracket[1] : raw;
  const match = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i.exec(candidate);
  return match ? match[0].toLowerCase() : "";
}

function normalizeEmailAllowlist(values) {
  return new Set((values || []).map(extractEmailAddress).filter(Boolean));
}

function decodeMimeWordText(text, encoding) {
  const enc = String(encoding || "").toUpperCase();
  if (enc === "B") return Buffer.from(String(text || "").replace(/\s+/g, ""), "base64").toString("utf8");
  if (enc === "Q") {
    const bytes = String(text || "")
      .replace(/_/g, " ")
      .replace(/=([A-F0-9]{2})/gi, (_, hex) => String.fromCharCode(Number.parseInt(hex, 16)));
    return Buffer.from(bytes, "binary").toString("utf8");
  }
  return String(text || "");
}

function decodeMimeHeaderValue(value) {
  return String(value || "").replace(/=\?([^?]+)\?([BQ])\?([^?]*)\?=/gi, (_full, charset, encoding, text) => {
    try {
      const decoded = decodeMimeWordText(text, encoding);
      return /utf-?8/i.test(charset) ? decoded : decoded;
    } catch {
      return _full;
    }
  });
}

function parseMimeHeaders(rawHeaders) {
  const headers = {};
  const unfolded = String(rawHeaders || "").replace(/\r?\n[ \t]+/g, " ");
  for (const line of unfolded.split(/\r?\n/)) {
    const index = line.indexOf(":");
    if (index <= 0) continue;
    const key = line.slice(0, index).trim().toLowerCase();
    const value = line.slice(index + 1).trim();
    headers[key] = headers[key] ? `${headers[key]}, ${value}` : value;
  }
  return headers;
}

function splitHeaderAndBody(raw) {
  const normalized = String(raw || "");
  const crlfIndex = normalized.indexOf("\r\n\r\n");
  if (crlfIndex >= 0) {
    return {
      headersText: normalized.slice(0, crlfIndex),
      body: normalized.slice(crlfIndex + 4)
    };
  }
  const lfIndex = normalized.indexOf("\n\n");
  if (lfIndex >= 0) {
    return {
      headersText: normalized.slice(0, lfIndex),
      body: normalized.slice(lfIndex + 2)
    };
  }
  return { headersText: normalized, body: "" };
}

function parseHeaderParams(headerValue) {
  const segments = String(headerValue || "")
    .split(";")
    .map((part) => part.trim())
    .filter(Boolean);
  const value = (segments.shift() || "").toLowerCase();
  const params = {};
  for (const segment of segments) {
    const index = segment.indexOf("=");
    if (index <= 0) continue;
    const key = segment.slice(0, index).trim().toLowerCase();
    let rawValue = segment.slice(index + 1).trim();
    if (rawValue.startsWith('"') && rawValue.endsWith('"')) {
      rawValue = rawValue.slice(1, -1).replace(/\\"/g, '"').replace(/\\\\/g, "\\");
    }
    params[key] = decodeMimeHeaderValue(rawValue);
  }
  return { value, params };
}

function decodeRfc2231Param(value) {
  const raw = String(value || "");
  const match = /^([^']*)'[^']*'(.*)$/.exec(raw);
  const encoded = match ? match[2] : raw;
  try {
    return decodeURIComponent(encoded);
  } catch {
    return encoded;
  }
}

function safeAttachmentFileName(value, fallback) {
  const decoded = decodeMimeHeaderValue(decodeRfc2231Param(value || ""));
  const cleaned = decoded
    .replace(/[\/\\:\u0000-\u001f]+/g, "_")
    .replace(/\s+/g, " ")
    .trim();
  return cleaned || fallback;
}

function splitMultipartBody(body, boundary) {
  const marker = `--${boundary}`;
  const segments = String(body || "").split(marker).slice(1);
  const parts = [];
  for (let segment of segments) {
    if (segment.startsWith("--")) break;
    segment = segment.replace(/^\r?\n/, "").replace(/\r?\n$/, "");
    if (segment.trim()) parts.push(segment);
  }
  return parts;
}

function decodeQuotedPrintableToBuffer(value) {
  const binary = String(value || "")
    .replace(/=\r?\n/g, "")
    .replace(/=([A-F0-9]{2})/gi, (_full, hex) => String.fromCharCode(Number.parseInt(hex, 16)));
  return Buffer.from(binary, "binary");
}

function decodeMimePartBody(body, transferEncoding) {
  const encoding = String(transferEncoding || "").trim().toLowerCase();
  if (encoding === "base64") return Buffer.from(String(body || "").replace(/\s+/g, ""), "base64");
  if (encoding === "quoted-printable") return decodeQuotedPrintableToBuffer(body);
  return Buffer.from(String(body || ""), "binary");
}

function decodeWindows1252Buffer(bytes) {
  const replacements = {
    0x80: 0x20ac,
    0x82: 0x201a,
    0x83: 0x0192,
    0x84: 0x201e,
    0x85: 0x2026,
    0x86: 0x2020,
    0x87: 0x2021,
    0x88: 0x02c6,
    0x89: 0x2030,
    0x8a: 0x0160,
    0x8b: 0x2039,
    0x8c: 0x0152,
    0x8e: 0x017d,
    0x91: 0x2018,
    0x92: 0x2019,
    0x93: 0x201c,
    0x94: 0x201d,
    0x95: 0x2022,
    0x96: 0x2013,
    0x97: 0x2014,
    0x98: 0x02dc,
    0x99: 0x2122,
    0x9a: 0x0161,
    0x9b: 0x203a,
    0x9c: 0x0153,
    0x9e: 0x017e,
    0x9f: 0x0178
  };
  return Array.from(bytes, (byte) => String.fromCodePoint(replacements[byte] || byte)).join("");
}

function decodeMimeTextBody(body, transferEncoding, charset) {
  const bytes = decodeMimePartBody(body, transferEncoding);
  const normalizedCharset = String(charset || "utf-8").trim().toLowerCase();
  if (["windows-1252", "cp1252"].includes(normalizedCharset)) {
    return decodeWindows1252Buffer(bytes);
  }
  if (["iso-8859-1", "latin1", "latin-1"].includes(normalizedCharset)) {
    return bytes.toString("latin1");
  }
  return bytes.toString("utf8");
}

function parseMimeMessageParts(raw, depth = 0) {
  if (depth > 20) return [];
  const { headersText, body } = splitHeaderAndBody(raw);
  const headers = parseMimeHeaders(headersText);
  const contentType = parseHeaderParams(headers["content-type"] || "text/plain");
  const disposition = parseHeaderParams(headers["content-disposition"] || "");

  if (contentType.value.startsWith("multipart/") && contentType.params.boundary) {
    return splitMultipartBody(body, contentType.params.boundary).flatMap((part) =>
      parseMimeMessageParts(part, depth + 1)
    );
  }

  const fileName =
    disposition.params.filename ||
    disposition.params["filename*"] ||
    contentType.params.name ||
    contentType.params["name*"] ||
    "";
  const isAttachment =
    disposition.value === "attachment" ||
    disposition.value === "inline" && Boolean(fileName) ||
    Boolean(fileName);
  if (!isAttachment) return [];

  const safeName = safeAttachmentFileName(fileName, `attachment-${randomUUID()}`);
  const bytes = decodeMimePartBody(body, headers["content-transfer-encoding"]);
  return [
    {
      filename: safeName,
      content_type: contentType.value || "application/octet-stream",
      content_disposition: disposition.value || null,
      transfer_encoding: headers["content-transfer-encoding"] || null,
      byte_length: bytes.length,
      bytes
    }
  ];
}

function parseMimeMessageTextParts(raw, depth = 0) {
  if (depth > 20) return [];
  const { headersText, body } = splitHeaderAndBody(raw);
  const headers = parseMimeHeaders(headersText);
  const contentType = parseHeaderParams(headers["content-type"] || "text/plain");
  const disposition = parseHeaderParams(headers["content-disposition"] || "");

  if (contentType.value.startsWith("multipart/") && contentType.params.boundary) {
    return splitMultipartBody(body, contentType.params.boundary).flatMap((part) =>
      parseMimeMessageTextParts(part, depth + 1)
    );
  }

  const type = contentType.value.toLowerCase();
  if (!["text/plain", "text/html"].includes(type)) return [];
  if (disposition.value === "attachment") return [];
  return [
    {
      content_type: type,
      charset: contentType.params.charset || null,
      text: decodeMimeTextBody(body, headers["content-transfer-encoding"], contentType.params.charset)
    }
  ];
}

function stripMimeContentId(value) {
  return String(value || "")
    .trim()
    .replace(/^<|>$/g, "")
    .trim();
}

function parseMimeMessageInlineResources(raw, depth = 0) {
  if (depth > 20) return [];
  const { headersText, body } = splitHeaderAndBody(raw);
  const headers = parseMimeHeaders(headersText);
  const contentType = parseHeaderParams(headers["content-type"] || "application/octet-stream");
  const disposition = parseHeaderParams(headers["content-disposition"] || "");

  if (contentType.value.startsWith("multipart/") && contentType.params.boundary) {
    return splitMultipartBody(body, contentType.params.boundary).flatMap((part) =>
      parseMimeMessageInlineResources(part, depth + 1)
    );
  }

  const mimeType = contentType.value.toLowerCase();
  const contentId = stripMimeContentId(headers["content-id"]);
  const contentLocation = decodeMimeHeaderValue(headers["content-location"] || "").trim();
  const isImage = mimeType.startsWith("image/");
  const isInlineResource = isImage && (contentId || contentLocation || disposition.value === "inline");
  if (!isInlineResource) return [];

  const bytes = decodeMimePartBody(body, headers["content-transfer-encoding"]);
  return [
    {
      content_id: contentId || null,
      content_location: contentLocation || null,
      content_type: mimeType,
      content_disposition: disposition.value || null,
      transfer_encoding: headers["content-transfer-encoding"] || null,
      byte_length: bytes.length,
      bytes
    }
  ];
}

function fileExtension(fileName) {
  const match = /\.([A-Za-z0-9]+)$/.exec(String(fileName || ""));
  return match ? match[1].toLowerCase() : "";
}

function isAccountingInvoiceAttachment(attachment) {
  const mimeType = String(attachment.content_type || "").split(";")[0].trim().toLowerCase();
  const ext = fileExtension(attachment.filename);
  return ACCOUNTING_ALLOWED_ATTACHMENT_MIME_TYPES.has(mimeType) || ACCOUNTING_ALLOWED_ATTACHMENT_EXTENSIONS.has(ext);
}

function inferAccountingMimeType(attachment) {
  const explicit = String(attachment.content_type || "").split(";")[0].trim().toLowerCase();
  if (ACCOUNTING_ALLOWED_ATTACHMENT_MIME_TYPES.has(explicit)) return explicit;
  const ext = fileExtension(attachment.filename);
  if (ext === "pdf") return "application/pdf";
  if (["jpg", "jpeg"].includes(ext)) return "image/jpeg";
  if (ext === "png") return "image/png";
  if (["tif", "tiff"].includes(ext)) return "image/tiff";
  if (ext === "webp") return "image/webp";
  if (ext === "heic") return "image/heic";
  if (ext === "heif") return "image/heif";
  return explicit || "application/octet-stream";
}

function publicAccountingAttachment(attachment) {
  return {
    filename: attachment.filename,
    content_type: attachment.content_type,
    content_disposition: attachment.content_disposition,
    transfer_encoding: attachment.transfer_encoding,
    byte_length: attachment.byte_length,
    allowed_invoice_attachment: isAccountingInvoiceAttachment(attachment),
    sha256: createHash("sha256").update(attachment.bytes).digest("hex"),
    generated_from_inline_invoice: Boolean(attachment.generated_from_inline_invoice),
    inline_invoice_vendor: attachment.inline_invoice_vendor || undefined,
    inline_invoice_source: attachment.inline_invoice_source || undefined,
    inline_invoice_render_mode: attachment.inline_invoice_render_mode || undefined,
    inline_invoice_render_engine: attachment.inline_invoice_render_engine || undefined,
    inline_invoice_render_remote_images: attachment.inline_invoice_render_remote_images ?? undefined,
    inline_invoice_render_error: attachment.inline_invoice_render_error || undefined
  };
}

function normalizeAccountingClassificationText(value) {
  return String(value || "")
    .replace(/\u00a0/g, " ")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function classifyAccountingTextAsInvoice(text) {
  const normalized = normalizeAccountingClassificationText(text);
  const hasInvoiceSignal = /\b(rechnung|invoice|tax invoice|credit note|gutschrift|facture|fattura|receipt|quittung)\b/i.test(normalized);
  const hasNonInvoiceSignal =
    /\b(mahnung|zahlungserinnerung|payment reminder|dunning|overdue|ubersicht|overview|report|kontoauszug|account statement|transaction overview|statement of account)\b/i.test(normalized);

  if (hasInvoiceSignal) {
    return {
      status: "invoice",
      invoice_signal: true,
      non_invoice_signal: hasNonInvoiceSignal,
      decision: "allow"
    };
  }
  if (hasNonInvoiceSignal) {
    return {
      status: "non_invoice",
      invoice_signal: false,
      non_invoice_signal: true,
      decision: "skip"
    };
  }
  return {
    status: "unclear",
    invoice_signal: false,
    non_invoice_signal: false,
    decision: "skip"
  };
}

function decodeAccountingHtmlEntities(value) {
  return String(value || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&euro;/gi, "€")
    .replace(/&auml;/gi, "ä")
    .replace(/&ouml;/gi, "ö")
    .replace(/&uuml;/gi, "ü")
    .replace(/&Auml;/g, "Ä")
    .replace(/&Ouml;/g, "Ö")
    .replace(/&Uuml;/g, "Ü")
    .replace(/&szlig;/gi, "ß")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&#x([0-9a-f]+);/gi, (_full, hex) => String.fromCodePoint(Number.parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_full, code) => String.fromCodePoint(Number.parseInt(code, 10)));
}

function escapeAccountingHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function extractAccountingHtmlBody(html) {
  const match = String(html || "").match(/<body\b[^>]*>([\s\S]*?)<\/body>/i);
  return match ? match[1] : String(html || "");
}

function extractAccountingHtmlStyleTags(html) {
  return Array.from(String(html || "").matchAll(/<style\b[^>]*>[\s\S]*?<\/style>/gi))
    .map((match) => match[0])
    .join("\n");
}

function decodeCidReference(value) {
  const raw = String(value || "").replace(/^cid:/i, "").trim();
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}

function resourceDataUri(resource) {
  if (!resource?.bytes?.length || !resource.content_type) return null;
  return `data:${resource.content_type};base64,${resource.bytes.toString("base64")}`;
}

function rewriteCidUrlsForAccountingHtml(html, inlineResources = []) {
  const byCid = new Map();
  const byLocation = new Map();
  for (const resource of inlineResources || []) {
    const contentId = stripMimeContentId(resource.content_id);
    if (contentId) byCid.set(contentId.toLowerCase(), resource);
    if (resource.content_location) byLocation.set(String(resource.content_location).trim(), resource);
  }
  if (!byCid.size && !byLocation.size) return String(html || "");
  return String(html || "")
    .replace(/cid:([^"'\s)>]+)/gi, (full, cid) => {
      const resource = byCid.get(decodeCidReference(cid).toLowerCase());
      const dataUri = resourceDataUri(resource);
      return dataUri || full;
    })
    .replace(/\b(src|background)\s*=\s*(["'])([^"']+)\2/gi, (full, attr, quote, url) => {
      const resource = byLocation.get(url.trim());
      const dataUri = resourceDataUri(resource);
      return dataUri ? `${attr}=${quote}${dataUri}${quote}` : full;
    })
    .replace(/url\((["']?)([^"')]+)\1\)/gi, (full, quote, url) => {
      const resource = byLocation.get(url.trim());
      const dataUri = resourceDataUri(resource);
      return dataUri ? `url(${quote}${dataUri}${quote})` : full;
    });
}

function htmlPartForAccountingPdf(message, fallbackText) {
  const htmlParts = (message.text_parts || [])
    .filter((part) => part.content_type === "text/html")
    .map((part) => part.text)
    .filter(Boolean);
  if (htmlParts.length) {
    return htmlParts
      .map((html) => `${extractAccountingHtmlStyleTags(html)}\n${extractAccountingHtmlBody(html)}`)
      .join('<hr style="border:0;border-top:1px solid #d0d7de;margin:18px 0;">');
  }
  return `<pre style="white-space:pre-wrap;font:14px/1.45 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">${escapeAccountingHtml(fallbackText)}</pre>`;
}

function buildInlineEmailHtmlDocument(message, bodyText) {
  const bodyHtml = rewriteCidUrlsForAccountingHtml(
    htmlPartForAccountingPdf(message, bodyText),
    message.inline_resources || []
  )
    .replace(/<\s*script\b[^>]*>[\s\S]*?<\s*\/\s*script\s*>/gi, "")
    .replace(/\son[a-z]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)/gi, "");
  return `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<style>
  @page { margin: 14mm; }
  html, body { margin: 0; padding: 0; background: #fff; color: #1f2328; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; font-size: 14px; line-height: 1.45; }
  img { max-width: 100%; height: auto; }
  table { max-width: 100%; border-collapse: collapse; }
  .vip-mail-header {
    border: 1px solid #d0d7de;
    border-radius: 6px;
    padding: 10px 12px;
    margin: 0 0 16px 0;
    background: #f6f8fa;
    font-size: 12px;
    color: #424a53;
  }
  .vip-mail-header div { margin: 2px 0; }
  .vip-mail-body { width: 100%; }
</style>
</head>
<body>
  <div class="vip-mail-header">
    <div><strong>Von:</strong> ${escapeAccountingHtml(message.from || message.from_email || "unbekannt")}</div>
    <div><strong>Betreff:</strong> ${escapeAccountingHtml(message.subject || "")}</div>
    <div><strong>Datum:</strong> ${escapeAccountingHtml(message.date || "")}</div>
  </div>
  <div class="vip-mail-body">${bodyHtml}</div>
</body>
</html>`;
}

function uniqueExistingPaths(paths) {
  return uniqueValues(paths.filter(Boolean).map((item) => String(item).trim()))
    .filter((item) => item && existsSync(item));
}

function findChromeExecutableInDirectory(root, maxDepth = 5) {
  if (!root || !existsSync(root) || maxDepth < 0) return null;
  const executableNames = new Set(["chrome", "google-chrome", "google-chrome-stable", "chromium", "chromium-browser"]);
  let entries = [];
  try {
    entries = readdirSync(root, { withFileTypes: true });
  } catch {
    return null;
  }
  for (const entry of entries) {
    const fullPath = path.join(root, entry.name);
    if (entry.isFile() && executableNames.has(entry.name) && existsSync(fullPath)) return fullPath;
  }
  if (maxDepth === 0) return null;
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const found = findChromeExecutableInDirectory(path.join(root, entry.name), maxDepth - 1);
    if (found) return found;
  }
  return null;
}

function resolveAccountingChromeExecutablePath(puppeteer) {
  const explicitPaths = uniqueExistingPaths([
    process.env.PUPPETEER_EXECUTABLE_PATH,
    process.env.CHROME_BIN,
    process.env.GOOGLE_CHROME_BIN,
    ...(process.env.ACCOUNTING_INLINE_EMAIL_CHROME_PATHS || "").split(",")
  ]);
  if (explicitPaths.length) return explicitPaths[0];

  try {
    const bundledPath = puppeteer.default.executablePath();
    if (bundledPath && existsSync(bundledPath)) return bundledPath;
  } catch {
    // Puppeteer throws here when the browser has not been installed yet.
  }

  const cacheRoots = uniqueExistingPaths([
    ACCOUNTING_INLINE_EMAIL_CHROME_CACHE_DIR,
    process.env.PUPPETEER_CACHE_DIR,
    path.join(process.cwd(), ".cache", "puppeteer"),
    path.join(process.cwd(), "node_modules", ".cache", "puppeteer"),
    process.env.HOME ? path.join(process.env.HOME, ".cache", "puppeteer") : ""
  ]);
  for (const root of cacheRoots) {
    const found = findChromeExecutableInDirectory(root);
    if (found) return found;
  }

  const systemPaths = uniqueExistingPaths([
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser"
  ]);
  return systemPaths[0] || "";
}

let accountingChromeInstallPromise = null;

async function installAccountingChromeRuntime() {
  if (!ACCOUNTING_INLINE_EMAIL_RUNTIME_CHROME_INSTALL) return;
  if (!accountingChromeInstallPromise) {
    accountingChromeInstallPromise = (async () => {
      const cliPath = path.join(process.cwd(), "node_modules", "puppeteer", "lib", "cjs", "puppeteer", "node", "cli.js");
      if (!existsSync(cliPath)) {
        throw new Error(`Puppeteer CLI nicht gefunden: ${cliPath}`);
      }
      await execFileAsync(process.execPath, [cliPath, "browsers", "install", "chrome"], {
        cwd: process.cwd(),
        env: {
          ...process.env,
          PUPPETEER_CACHE_DIR: ACCOUNTING_INLINE_EMAIL_CHROME_CACHE_DIR
        },
        timeout: ACCOUNTING_INLINE_EMAIL_CHROME_INSTALL_TIMEOUT_MS,
        maxBuffer: 2_000_000
      });
    })().catch((error) => {
      accountingChromeInstallPromise = null;
      throw error;
    });
  }
  await accountingChromeInstallPromise;
}

async function renderAccountingHtmlEmailPdfBuffer(html, { allowRemoteImages = ACCOUNTING_INLINE_EMAIL_RENDER_REMOTE_IMAGES } = {}) {
  if (!process.env.PUPPETEER_CACHE_DIR) {
    process.env.PUPPETEER_CACHE_DIR = ACCOUNTING_INLINE_EMAIL_CHROME_CACHE_DIR;
  }
  const puppeteer = await import("puppeteer");
  let executablePath = resolveAccountingChromeExecutablePath(puppeteer);
  if (!executablePath && ACCOUNTING_INLINE_EMAIL_RUNTIME_CHROME_INSTALL) {
    await installAccountingChromeRuntime();
    executablePath = resolveAccountingChromeExecutablePath(puppeteer);
  }
  const browser = await puppeteer.default.launch({
    headless: "new",
    ...(executablePath ? { executablePath } : {}),
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
  });
  try {
    const page = await browser.newPage();
    await page.setJavaScriptEnabled(false);
    await page.setViewport({ width: 1024, height: 1440, deviceScaleFactor: 1 });
    await page.emulateMediaType("screen");
    await page.setRequestInterception(true);
    page.on("request", (request) => {
      const url = request.url();
      const type = request.resourceType();
      if (/^(data|blob|about):/i.test(url)) {
        request.continue().catch(() => {});
        return;
      }
      if (allowRemoteImages && /^https?:/i.test(url) && ["image", "stylesheet", "font"].includes(type)) {
        request.continue().catch(() => {});
        return;
      }
      request.abort().catch(() => {});
    });
    await page.setContent(html, {
      waitUntil: "load",
      timeout: ACCOUNTING_INLINE_EMAIL_RENDER_TIMEOUT_MS
    });
    return Buffer.from(await page.pdf({
      format: "A4",
      printBackground: true,
      preferCSSPageSize: false,
      margin: { top: "12mm", right: "12mm", bottom: "12mm", left: "12mm" },
      timeout: ACCOUNTING_INLINE_EMAIL_RENDER_TIMEOUT_MS
    }));
  } finally {
    await browser.close().catch(() => {});
  }
}

async function createInlineAccountingEmailPdf({ message, bodyText, fallbackTextPdfTitle }) {
  const html = buildInlineEmailHtmlDocument(message, bodyText);
  try {
    const bytes = await renderAccountingHtmlEmailPdfBuffer(html);
    return {
      bytes,
      render_mode: "html_email_pdf",
      render_engine: "puppeteer",
      render_remote_images: ACCOUNTING_INLINE_EMAIL_RENDER_REMOTE_IMAGES
    };
  } catch (error) {
    const fallbackText = buildFullInlineEmailPdfText(message, bodyText);
    return {
      bytes: createSimpleAccountingPdfBuffer({
        title: fallbackTextPdfTitle,
        lines: fallbackText
      }),
      render_mode: "text_email_pdf_fallback",
      render_engine: "simple_pdf",
      render_error: String(error?.message || error),
      render_remote_images: false
    };
  }
}

function htmlToAccountingText(html) {
  return decodeAccountingHtmlEntities(
    String(html || "")
      .replace(/<\s*(script|style)[^>]*>[\s\S]*?<\s*\/\s*\1\s*>/gi, " ")
      .replace(/<\s*br\s*\/?>/gi, "\n")
      .replace(/<\s*\/\s*(p|div|tr|table|section|article|h[1-6]|li)\s*>/gi, "\n")
      .replace(/<[^>]+>/g, " ")
  )
    .replace(/\r/g, "")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/[ \t]{2,}/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function plainTextFromAccountingTextParts(textParts) {
  const htmlText = textParts
    .filter((part) => part.content_type === "text/html")
    .map((part) => htmlToAccountingText(part.text))
    .filter(Boolean);
  const plainText = textParts
    .filter((part) => part.content_type === "text/plain")
    .map((part) => part.text)
    .filter(Boolean);
  return [...htmlText, ...plainText].join("\n\n").trim();
}

function normalizePdfAsciiText(value) {
  return String(value || "")
    .replace(/€/g, " EUR")
    .replace(/–|—/g, "-")
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .replace(/ä/g, "ae")
    .replace(/ö/g, "oe")
    .replace(/ü/g, "ue")
    .replace(/Ä/g, "Ae")
    .replace(/Ö/g, "Oe")
    .replace(/Ü/g, "Ue")
    .replace(/ß/g, "ss")
    .replace(/[^\x09\x0a\x0d\x20-\x7e]/g, " ");
}

function escapePdfLiteralString(value) {
  return normalizePdfAsciiText(value)
    .replace(/\\/g, "\\\\")
    .replace(/\(/g, "\\(")
    .replace(/\)/g, "\\)");
}

function wrapAccountingPdfLine(line, maxChars = 96) {
  const words = normalizePdfAsciiText(line).split(/\s+/).filter(Boolean);
  if (!words.length) return [""];
  const lines = [];
  let current = "";
  for (const word of words) {
    if (!current) {
      current = word;
      continue;
    }
    if ((current.length + 1 + word.length) <= maxChars) {
      current += ` ${word}`;
    } else {
      lines.push(current);
      current = word;
    }
  }
  if (current) lines.push(current);
  return lines;
}

function createSimpleAccountingPdfBuffer({ title, lines }) {
  const pageWidth = 595;
  const pageHeight = 842;
  const marginLeft = 46;
  const firstY = 800;
  const lineHeight = 14;
  const maxLinesPerPage = 54;
  const normalizedLines = [
    title || "Inline-Rechnung",
    "",
    ...String(lines || "")
      .split(/\r?\n/)
      .flatMap((line) => wrapAccountingPdfLine(line))
  ];
  const pages = [];
  for (let index = 0; index < normalizedLines.length; index += maxLinesPerPage) {
    pages.push(normalizedLines.slice(index, index + maxLinesPerPage));
  }
  if (!pages.length) pages.push(["Inline-Rechnung"]);

  const objects = [];
  const addObject = (content) => {
    objects.push(content);
    return objects.length;
  };

  const catalogId = addObject("<< /Type /Catalog /Pages 2 0 R >>");
  const pagesId = addObject("");
  const fontId = addObject("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>");
  const pageIds = [];

  for (const pageLines of pages) {
    const textCommands = pageLines
      .map((line, index) => {
        const y = firstY - index * lineHeight;
        return `BT /F1 10 Tf ${marginLeft} ${y} Td (${escapePdfLiteralString(line)}) Tj ET`;
      })
      .join("\n");
    const stream = `${textCommands}\n`;
    const contentId = addObject(`<< /Length ${Buffer.byteLength(stream, "binary")} >>\nstream\n${stream}endstream`);
    const pageId = addObject(
      `<< /Type /Page /Parent ${pagesId} 0 R /MediaBox [0 0 ${pageWidth} ${pageHeight}] /Resources << /Font << /F1 ${fontId} 0 R >> >> /Contents ${contentId} 0 R >>`
    );
    pageIds.push(pageId);
  }

  objects[pagesId - 1] = `<< /Type /Pages /Kids [${pageIds.map((id) => `${id} 0 R`).join(" ")}] /Count ${pageIds.length} >>`;
  objects[catalogId - 1] = `<< /Type /Catalog /Pages ${pagesId} 0 R >>`;

  let pdf = "%PDF-1.4\n";
  const offsets = [0];
  for (let index = 0; index < objects.length; index += 1) {
    offsets.push(Buffer.byteLength(pdf, "binary"));
    pdf += `${index + 1} 0 obj\n${objects[index]}\nendobj\n`;
  }
  const xrefOffset = Buffer.byteLength(pdf, "binary");
  pdf += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n`;
  for (let index = 1; index < offsets.length; index += 1) {
    pdf += `${String(offsets[index]).padStart(10, "0")} 00000 n \n`;
  }
  pdf += `trailer\n<< /Size ${objects.length + 1} /Root ${catalogId} 0 R >>\nstartxref\n${xrefOffset}\n%%EOF\n`;
  return Buffer.from(pdf, "binary");
}

function compactInlineInvoiceText(text, maxLines = 90) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map((line) => line.replace(/\s+/g, " ").trim())
    .filter(Boolean);
  const startCandidates = [
    lines.findIndex((line) => /^rechnung$/i.test(line)),
    lines.findIndex((line) => /\bApple\b/i.test(line) && /\b(no_reply|no-reply|email\.apple\.com)\b/i.test(line)),
    lines.findIndex((line) => /\bDokument\s*:/i.test(line))
  ].filter((index) => index >= 0);
  const start = startCandidates.length ? Math.max(0, Math.min(...startCandidates) - 2) : 0;
  return lines.slice(start, start + maxLines).join("\n");
}

function buildFullInlineEmailPdfText(message, bodyText) {
  return [
    "Aus kompletter E-Mail erzeugtes PDF",
    `Von: ${message.from || message.from_email || "unbekannt"}`,
    `Absender-Adresse: ${message.from_email || "unbekannt"}`,
    `Betreff: ${message.subject || ""}`,
    `E-Mail-Datum: ${message.date || ""}`,
    message.message_id_hash ? `Message-ID-Hash: ${message.message_id_hash}` : null,
    "",
    "E-Mail-Inhalt:",
    "",
    bodyText || ""
  ].filter((line) => line !== null).join("\n");
}

function buildInlineInvoiceFileName({ vendor, invoiceDate, documentNumber, uid }) {
  const safeVendor = safeAttachmentFileName(vendor || "E-Mail-Rechnung", "E-Mail-Rechnung").replace(/\.[^.]+$/, "");
  const datePart = invoiceDate
    ? invoiceDate.replace(/^(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})$/, (_full, day, month, year) =>
        `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`
      )
    : "ohne-datum";
  const docPart = documentNumber
    ? `-${safeAttachmentFileName(documentNumber, "Dokument")}`
    : `-uid-${uid}`;
  return `${safeVendor}_E-Mail-Rechnung_${datePart}${docPart}.pdf`;
}

async function detectInlineAccountingInvoiceAttachment(message) {
  const bodyText = plainTextFromAccountingTextParts(message.text_parts || []);
  if (!bodyText) return null;

  const normalized = normalizeAccountingClassificationText(bodyText);
  const subject = normalizeAccountingClassificationText(message.subject || "");
  const isAppleInvoice =
    /\bapple\b/.test(normalized) &&
    (/\brechnung\b/.test(normalized) || /\brechnung\b/.test(subject)) &&
    /\b(dokument|bestellnummer|sequenz|icloud|mehrwertsteuer)\b/.test(normalized) &&
    /(\d+,\d{2})\s*(€|eur)/i.test(bodyText);

  if (!isAppleInvoice) return null;

  const invoiceText = compactInlineInvoiceText(bodyText);
  const invoiceExtraction = extractInvoiceAmountsFromPdfText(invoiceText || bodyText);
  const invoiceDate = invoiceExtraction.invoice_date || extractFirstGermanInvoiceDate(bodyText);
  const documentNumber = bodyText.match(/\bDokument\s*:\s*([A-Z0-9-]+)/i)?.[1] || null;
  const filename = buildInlineInvoiceFileName({
    vendor: "Apple",
    invoiceDate,
    documentNumber,
    uid: message.uid
  });
  const rendered = await createInlineAccountingEmailPdf({
    message,
    bodyText,
    fallbackTextPdfTitle: filename.replace(/\.pdf$/i, "")
  });

  return {
    filename,
    content_type: ACCOUNTING_INLINE_INVOICE_PDF_MIME_TYPE,
    content_disposition: "generated-inline",
    transfer_encoding: null,
    byte_length: rendered.bytes.length,
    bytes: rendered.bytes,
    generated_from_inline_invoice: true,
    inline_invoice_vendor: "Apple",
    inline_invoice_source: rendered.render_mode === "html_email_pdf" ? "rendered_full_email_html" : "full_email_body",
    inline_invoice_render_mode: rendered.render_mode,
    inline_invoice_render_engine: rendered.render_engine,
    inline_invoice_render_remote_images: rendered.render_remote_images,
    inline_invoice_render_error: rendered.render_error,
    inline_invoice_extraction: invoiceExtraction
  };
}

async function classifyAccountingAttachmentForUpload(attachment, message) {
  const ext = fileExtension(attachment.filename);
  const subjectAndName = `${message?.subject || ""}\n${attachment.filename || ""}`;
  if (attachment.generated_from_inline_invoice) {
    const extraction =
      attachment.inline_invoice_extraction || extractInvoiceAmountsFromPdfText(message?.inline_invoice_text || "");
    return {
      ...classifyAccountingTextAsInvoice(`${subjectAndName}\n${message?.subject || ""}`),
      status: "invoice",
      invoice_signal: true,
      non_invoice_signal: false,
      decision: "allow",
      source: "inline_email_body",
      reason: "recognized_inline_invoice_body",
      invoice_extraction: extraction
    };
  }
  if (ext !== "pdf" && String(attachment.content_type || "").toLowerCase() !== "application/pdf") {
    const metadataClassification = classifyAccountingTextAsInvoice(subjectAndName);
    return {
      ...metadataClassification,
      source: "attachment_metadata",
      reason:
        metadataClassification.decision === "allow"
          ? "invoice_signal_in_subject_or_filename"
          : "image_attachment_without_invoice_signal"
    };
  }

  const parser = new PDFParse({ data: attachment.bytes });
  let parsed;
  try {
    parsed = await parser.getText();
  } finally {
    await parser.destroy().catch(() => {});
  }
  const classification = classifyAccountingTextAsInvoice(`${subjectAndName}\n${parsed.text || ""}`);
  const invoiceExtraction =
    classification.decision === "allow" ? extractInvoiceAmountsFromPdfText(parsed.text || "") : null;
  return {
    ...classification,
    source: "pdf_text",
    reason:
      classification.decision === "allow"
        ? "invoice_signal_in_pdf_text"
        : classification.status === "non_invoice"
          ? "non_invoice_signal_in_pdf_text"
          : "missing_invoice_signal_in_pdf_text",
    invoice_extraction: invoiceExtraction
  };
}

async function parseRawEmail(raw, uid) {
  const { headersText } = splitHeaderAndBody(raw);
  const headers = parseMimeHeaders(headersText);
  const messageId = headers["message-id"] || "";
  const message = {
    uid,
    from: decodeMimeHeaderValue(headers.from || ""),
    from_email: extractEmailAddress(headers.from || ""),
    subject: decodeMimeHeaderValue(headers.subject || ""),
    date: headers.date || "",
    message_id: messageId,
    message_id_hash: messageId ? createHash("sha256").update(messageId).digest("hex") : "",
    attachments: parseMimeMessageParts(raw).filter(isAccountingInvoiceAttachment),
    text_parts: parseMimeMessageTextParts(raw),
    inline_resources: parseMimeMessageInlineResources(raw)
  };
  const inlineInvoiceAttachment = await detectInlineAccountingInvoiceAttachment(message);
  if (inlineInvoiceAttachment) {
    message.attachments.push(inlineInvoiceAttachment);
  }
  return message;
}

function extractImapLiteral(response) {
  const match = /BODY(?:\.PEEK)?\[\](?:[^\{]*)\{(\d+)\}\r?\n/i.exec(response);
  if (!match) return "";
  const start = match.index + match[0].length;
  const length = Number(match[1]);
  return response.slice(start, start + length);
}

async function runImapFetchUnseenRaw(config, {
  limit,
  maxEmailBytes,
  mailSearchMode = "unseen",
  fetchAllMatching = false,
  specificUid
}) {
  const socket = await openImapSocket(config);
  socket.setEncoding("binary");
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
    const searchCriterion = mailSearchMode === "inbox_all" ? "ALL" : "UNSEEN";
    const searchResponse = await command(`UID SEARCH ${searchCriterion}`);
    const searchLine = searchResponse
      .split(/\r?\n/)
      .find((line) => /^\* SEARCH(?:[ \t]|$)/i.test(line));
    const uidText = searchLine ? searchLine.replace(/^\* SEARCH[ \t]*/i, "").trim() : "";
    const uids = uidText.split(/[ \t]+/).filter((value) => /^\d+$/.test(value));
    const batchSize = Math.max(1, Number(limit || 25));
    const selectedUids = specificUid
      ? [String(specificUid)]
      : fetchAllMatching
        ? (mailSearchMode === "inbox_all" ? [...uids].reverse() : [...uids])
        : (mailSearchMode === "inbox_all" ? uids.slice(-batchSize).reverse() : uids.slice(0, batchSize));
    const messages = [];

    for (let index = 0; index < selectedUids.length; index += batchSize) {
      const batchUids = selectedUids.slice(index, index + batchSize);
      for (const uid of batchUids) {
        const fetchResponse = await command(`UID FETCH ${uid} (BODY.PEEK[])`);
        const raw = extractImapLiteral(fetchResponse);
        if (!raw) {
          messages.push({ uid, parse_error: "no_body_literal" });
          continue;
        }
        if (Buffer.byteLength(raw, "binary") > maxEmailBytes) {
          messages.push({ uid, parse_error: "message_too_large" });
          continue;
        }
        messages.push(await parseRawEmail(raw, uid));
      }
    }

    await command("LOGOUT").catch(() => {});
    if (!socket.destroyed) socket.destroy();
    return {
      mail_search_mode: mailSearchMode,
      search_criterion: searchCriterion,
      fetch_all_matching: Boolean(fetchAllMatching),
      specific_uid: specificUid || null,
      batch_size: batchSize,
      batch_count: Math.ceil(selectedUids.length / batchSize),
      matching_count: uids.length,
      unseen_count: mailSearchMode === "unseen" ? uids.length : null,
      returned_count: messages.length,
      messages
    };
  } catch (error) {
    if (!socket.destroyed) socket.destroy();
    throw error;
  }
}

async function fetchUnseenRawEmailWithFallback(configs, options) {
  const attempts = [];
  for (const config of configs) {
    try {
      const result = await runImapFetchUnseenRaw(config, options);
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
    `IMAP Attachment-Fetch fehlgeschlagen: ${attempts.map((attempt) => `${attempt.label} ${attempt.host}:${attempt.port} ${attempt.error}`).join(" | ")}`
  );
  error.imap_attempts = attempts;
  throw error;
}

function unquoteImapMailboxName(value) {
  const text = String(value || "").trim();
  if (!text.startsWith('"') || !text.endsWith('"')) return text;
  return text.slice(1, -1).replace(/\\(["\\])/g, "$1");
}

function parseImapListMailboxes(response) {
  const mailboxes = [];
  for (const line of String(response || "").split(/\r?\n/)) {
    if (!/^\* LIST(?:[ \t]|$)/i.test(line)) continue;
    const quoted = line.match(/"((?:[^"\\]|\\.)*)"\s*$/);
    if (quoted) {
      mailboxes.push(unquoteImapMailboxName(`"${quoted[1]}"`));
      continue;
    }
    const atom = line.trim().split(/\s+/).pop();
    if (atom && atom !== "NIL") mailboxes.push(unquoteImapMailboxName(atom));
  }
  return uniqueValues(mailboxes);
}

function resolveArchiveMailbox(mailboxes, preferredMailbox) {
  const available = uniqueValues(mailboxes);
  const exact = (candidate) =>
    available.find((mailbox) => mailbox.toLowerCase() === String(candidate || "").toLowerCase());
  if (preferredMailbox) {
    const found = exact(preferredMailbox);
    if (!found) {
      throw new Error(
        `Archivordner ${preferredMailbox} existiert nicht. Verfuegbar: ${available.join(", ") || "keine"}`
      );
    }
    return found;
  }
  for (const candidate of ACCOUNTING_ARCHIVE_MAILBOX_CANDIDATES) {
    const found = exact(candidate);
    if (found) return found;
  }
  throw new Error(
    `Kein bestehender Archivordner gefunden. Verfuegbar: ${available.join(", ") || "keine"}`
  );
}

function isAllowedAccountingArchiveMailboxName(value) {
  return ACCOUNTING_ARCHIVE_MAILBOX_CANDIDATES.some(
    (candidate) => candidate.toLowerCase() === String(value || "").trim().toLowerCase()
  );
}

async function runImapArchiveUid(config, {
  uid,
  archiveMailbox,
  createArchiveMailbox,
  expectedFromEmail,
  expectedMessageIdHash,
  dryRun,
  verifyAfter
}) {
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
    const listResponse = await command('LIST "" "*"');
    let mailboxes = parseImapListMailboxes(listResponse);
    let archiveMailboxCreated = false;
    let archiveMailboxCreatePlanned = false;
    let resolvedArchiveMailbox;
    try {
      resolvedArchiveMailbox = resolveArchiveMailbox(mailboxes, archiveMailbox);
    } catch (error) {
      if (!createArchiveMailbox || !archiveMailbox) throw error;
      if (!isAllowedAccountingArchiveMailboxName(archiveMailbox)) {
        throw new Error(
          `Archivordner ${archiveMailbox} darf nicht automatisch angelegt werden. Erlaubt: ${ACCOUNTING_ARCHIVE_MAILBOX_CANDIDATES.join(", ")}`
        );
      }
      if (dryRun) {
        resolvedArchiveMailbox = archiveMailbox;
        archiveMailboxCreatePlanned = true;
      } else {
        await command(`CREATE ${quoteImapString(archiveMailbox)}`);
        archiveMailboxCreated = true;
        const relistResponse = await command('LIST "" "*"');
        mailboxes = parseImapListMailboxes(relistResponse);
        resolvedArchiveMailbox = resolveArchiveMailbox(mailboxes, archiveMailbox);
      }
    }
    await command("SELECT INBOX");
    const fetchResponse = await command(`UID FETCH ${uid} (UID FLAGS BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE MESSAGE-ID)])`);
    const found = new RegExp(`\\bUID\\s+${uid}\\b`).test(fetchResponse);
    if (!found) {
      await command("LOGOUT").catch(() => {});
      if (!socket.destroyed) socket.destroy();
      return {
        status: "not_found",
        uid,
        archive_mailbox: resolvedArchiveMailbox,
        available_mailboxes: mailboxes
      };
    }

    const headers = parseImapHeaderBlock(fetchResponse);
    const fromEmail = extractEmailAddress(headers.from);
    const messageIdHash = headers.message_id
      ? createHash("sha256").update(headers.message_id).digest("hex")
      : "";

    if (expectedFromEmail && fromEmail !== extractEmailAddress(expectedFromEmail)) {
      throw new Error(`UID ${uid} Absender-Mismatch: erwartet ${expectedFromEmail}, gefunden ${fromEmail || "unbekannt"}.`);
    }
    if (expectedMessageIdHash && messageIdHash !== expectedMessageIdHash) {
      throw new Error(`UID ${uid} Message-ID-Hash-Mismatch.`);
    }

    const messageSummary = {
      uid,
      from_email: fromEmail || null,
      subject: headers.subject || null,
      date: headers.date || null,
      message_id_hash: messageIdHash || null
    };

    if (dryRun) {
      await command("LOGOUT").catch(() => {});
      if (!socket.destroyed) socket.destroy();
      return {
        status: "ready_for_archive",
        dry_run: true,
        archive_mailbox: resolvedArchiveMailbox,
        archive_mailbox_created: false,
        archive_mailbox_create_planned: archiveMailboxCreatePlanned,
        available_mailboxes: mailboxes,
        message: messageSummary
      };
    }

    const moveResponse = await command(`UID MOVE ${uid} ${quoteImapString(resolvedArchiveMailbox)}`);
    let inInboxAfterMove = null;
    if (verifyAfter) {
      const verifyResponse = await command(`UID FETCH ${uid} (UID FLAGS)`);
      inInboxAfterMove = new RegExp(`\\bUID\\s+${uid}\\b`).test(verifyResponse);
    }

    await command("LOGOUT").catch(() => {});
    if (!socket.destroyed) socket.destroy();
    return {
      status: inInboxAfterMove ? "archive_verification_failed" : "archived",
      dry_run: false,
      archive_mailbox: resolvedArchiveMailbox,
      archive_mailbox_created: archiveMailboxCreated,
      message: messageSummary,
      move_response_preview: cleanImapPreview(moveResponse, 300),
      in_inbox_after_move: inInboxAfterMove
    };
  } catch (error) {
    if (!socket.destroyed) socket.destroy();
    throw error;
  }
}

async function archiveImapUidWithFallback(configs, options) {
  const attempts = [];
  for (const config of configs) {
    try {
      const result = await runImapArchiveUid(config, options);
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
    `IMAP Archivierung fehlgeschlagen: ${attempts.map((attempt) => `${attempt.label} ${attempt.host}:${attempt.port} ${attempt.error}`).join(" | ")}`
  );
  error.imap_attempts = attempts;
  throw error;
}

async function assertAllowedAccountingFolder(folderId, googleContext = {}) {
  if (ACCOUNTING_ALLOWED_DRIVE_ROOT_IDS.has(folderId)) return;

  let currentId = folderId;
  for (let depth = 0; depth < 12; depth += 1) {
    const folder = await getDriveFile(currentId, "id,name,mimeType,parents", googleContext);
    if (folder.mimeType !== "application/vnd.google-apps.folder") {
      throw new Error(`Accounting target_folder_id ist kein Ordner: ${folderId}`);
    }
    const parents = folder.parents || [];
    if (parents.some((parent) => ACCOUNTING_ALLOWED_DRIVE_ROOT_IDS.has(parent))) return;
    if (!parents.length) break;
    currentId = parents[0];
  }

  throw new Error(
    `Accounting-Zielordner ${folderId} liegt nicht unter einem freigegebenen Rechnungsordner.`
  );
}

function escapeGoogleDriveQueryString(value) {
  return String(value || "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

async function findAccountingDriveDuplicate({ folderId, sha256, fileName }, googleContext = {}) {
  const query =
    `'${escapeGoogleDriveQueryString(folderId)}' in parents and trashed = false and ` +
    `(name = '${escapeGoogleDriveQueryString(fileName)}' or appProperties has { key='vip_sha256' and value='${escapeGoogleDriveQueryString(sha256)}' })`;
  const res = await googleRequest({
    method: "GET",
    url: "https://www.googleapis.com/drive/v3/files",
    params: {
      q: query,
      fields: "files(id,name,mimeType,parents,webViewLink,md5Checksum,appProperties,createdTime,modifiedTime)",
      supportsAllDrives: true,
      includeItemsFromAllDrives: true
    }
  }, googleContext);
  return res.data.files || [];
}

function parseEuroAmount(value) {
  if (!/\d/.test(String(value || ""))) return null;
  const normalized = String(value || "")
    .replace(/\s/g, "")
    .replace(/\./g, "")
    .replace(",", ".");
  const amount = Number(normalized);
  if (!Number.isFinite(amount)) return null;
  return Math.round(amount * 100) / 100;
}

function normalizeExtractedPdfText(text) {
  return String(text || "")
    .replace(/\u00a0/g, " ")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+/g, " ")
    .replace(/\n[ \t]+/g, "\n")
    .trim();
}

function getNormalizedPdfTextLines(rawText) {
  return normalizeExtractedPdfText(rawText)
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function extractMonthKeyFromGermanDate(dateText) {
  const match = String(dateText || "").match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
  if (!match) return null;
  return `${match[2]}/${match[3].slice(-2)}`;
}

function extractGermanMonthKey(text) {
  const monthNames = {
    jan: "01",
    januar: "01",
    feb: "02",
    februar: "02",
    maerz: "03",
    marz: "03",
    apr: "04",
    april: "04",
    mai: "05",
    jun: "06",
    juni: "06",
    jul: "07",
    juli: "07",
    aug: "08",
    august: "08",
    sep: "09",
    sept: "09",
    september: "09",
    okt: "10",
    oktober: "10",
    nov: "11",
    november: "11",
    dez: "12",
    dezember: "12"
  };
  const cleaned = String(text || "").toLowerCase().replace(/ä/g, "ae");
  const match = cleaned.match(/\b(?:1\.|01\.)\s*[–-]\s*(?:\d{1,2}\.)\s*([a-z]+)\.?\s+(\d{4})/i);
  if (!match) return null;
  const month = monthNames[match[1]] || monthNames[match[1].replace(/\.$/, "")];
  if (!month) return null;
  return `${month}/${String(match[2]).slice(-2)}`;
}

function germanMonthNameToNumber(value) {
  const monthNames = {
    jan: "01",
    januar: "01",
    feb: "02",
    februar: "02",
    maerz: "03",
    marz: "03",
    apr: "04",
    april: "04",
    mai: "05",
    jun: "06",
    juni: "06",
    jul: "07",
    juli: "07",
    aug: "08",
    august: "08",
    sep: "09",
    sept: "09",
    september: "09",
    okt: "10",
    oktober: "10",
    nov: "11",
    november: "11",
    dez: "12",
    dezember: "12"
  };
  const key = String(value || "").toLowerCase().replace(/ä/g, "ae").replace(/\.$/, "");
  return monthNames[key] || null;
}

function parseGermanTextualDateToNumeric(text) {
  const match = String(text || "").match(/\b(\d{1,2})\.\s*([A-Za-zÄÖÜäöü]+)\.?\s+(\d{4})\b/);
  if (!match) return null;
  const month = germanMonthNameToNumber(match[2]);
  if (!month) return null;
  return `${String(match[1]).padStart(2, "0")}.${month}.${match[3]}`;
}

function parseFirstEuroAmountFromText(text, patterns) {
  for (const pattern of patterns) {
    const amount = parseEuroAmount(String(text || "").match(pattern)?.[1]);
    if (amount !== null) return amount;
  }
  return null;
}

function extractFirstGermanInvoiceDate(text) {
  const value = String(text || "");
  return (
    value.match(/Rechnungsnr\.?:\s*[^\n]*?\bDatum\s+(\d{2}\.\d{2}\.\d{4})/i)?.[1] ||
    value.match(/\b(?:Rechnungsdatum|Datum der Rechnung|Belegdatum|Invoice Date|Datum)\b\s*:?\s*(\d{2}\.\d{2}\.\d{4})/i)?.[1] ||
    parseGermanTextualDateToNumeric(
      value.match(/\b(?:Rechnungsdatum|Datum der Rechnung|Belegdatum|Invoice Date|Datum)\b\s*:?\s*(\d{1,2}\.\s*[A-Za-zÄÖÜäöü]+\.?\s+\d{4})/i)?.[1]
    ) ||
    value.match(/\b(\d{2}\.\d{2}\.\d{4})\b/)?.[1] ||
    parseGermanTextualDateToNumeric(value) ||
    null
  );
}

function extractAccountingPdfRecipient(rawText) {
  const lines = getNormalizedPdfTextLines(rawText);
  const adsenseLine = lines.find((line) => /\bEinnahmen\b.*AdSense/i.test(line));
  if (adsenseLine) {
    const orgIndex = lines.findIndex((line) => /^Name der Organisation$/i.test(line));
    const aliasIndex = lines.findIndex((line) => /^Alias des Zahlungskontos$/i.test(line));
    return {
      recipient_name: "Google AdSense",
      recipient_address: [],
      recipient_firm_id: null,
      recipient_vat_id: null,
      recipient_confidence: "high",
      recipient_source: "adsense_statement",
      payment_organization: orgIndex >= 0 ? lines[orgIndex + 1] || null : null,
      payment_account_alias: aliasIndex >= 0 ? lines[aliasIndex + 1] || null : null
    };
  }

  const senderReferenceIndex = lines.findIndex((line) => /^VIP-Studios\s*\|\s*Feichtmeyer,/i.test(line));
  const invoiceIndex = lines.findIndex(
    (line, index) => index > senderReferenceIndex && /^Rechnungsnr\.?:/i.test(line)
  );
  const recipientLines =
    senderReferenceIndex >= 0 && invoiceIndex > senderReferenceIndex
      ? lines.slice(senderReferenceIndex + 1, invoiceIndex)
      : [];
  const firmLine = lines.find((line) => /^Firmen-ID:/i.test(line)) || "";

  return {
    recipient_name: recipientLines[0] || null,
    recipient_address: recipientLines.slice(1),
    recipient_firm_id: firmLine.match(/Firmen-ID:\s*([^\s]+)/i)?.[1] || null,
    recipient_vat_id: firmLine.match(/USt-ID:\s*([A-Z]{2}[A-Z0-9]+)/i)?.[1] || null,
    recipient_confidence: recipientLines[0] ? "high" : "low",
    recipient_source: recipientLines[0] ? "invoice_address_block" : "not_found",
    payment_organization: null,
    payment_account_alias: null
  };
}

function extractInvoiceAmountsFromPdfText(rawText) {
  const text = normalizeExtractedPdfText(rawText);
  const recipient = extractAccountingPdfRecipient(rawText);

  const adsenseAmount =
    parseEuroAmount(text.match(/\bEinnahmen\b[^\n€]*?(-?\s*[\d.]+,\d{2})\s*€/i)?.[1]) ??
    parseEuroAmount(text.match(/\bEinnahmen\s+[–-][^\n€]*?(-?\s*[\d.]+,\d{2})\s*€/i)?.[1]);
  if (/AdSense/i.test(text) && adsenseAmount !== null) {
    return {
      ...recipient,
      kind: "income_statement",
      status: "extracted",
      invoice_date: null,
      month_key: extractGermanMonthKey(text),
      net_amount: adsenseAmount,
      vat_amount: 0,
      gross_amount: adsenseAmount,
      confidence: "high"
    };
  }

  const invoiceDate = extractFirstGermanInvoiceDate(text);
  const rawNetInvoice = parseFirstEuroAmountFromText(text, [
    /\bSumme\s*:?\s*(-?\s*[\d.]+,\d{2})\s*(?:€|EUR)?/i,
    /\b(?:Nettobetrag|Netto(?:summe|betrag)?|Summe\s+netto|Zwischensumme)\b[^\n€]{0,120}(-?\s*[\d.]+,\d{2})\s*(?:€|EUR)?/i
  ]);
  const vatInvoice = parseFirstEuroAmountFromText(text, [
    /\bMwSt\.?\s*(?:\([^)]*\))?\s*(-?\s*[\d.]+,\d{2})\s*(?:€|EUR)?/i,
    /\b(?:USt\.?|Umsatzsteuer|Mehrwertsteuer|VAT)\b[^\n€]{0,120}(-?\s*[\d.]+,\d{2})\s*(?:€|EUR)?/i
  ]);
  const grossInvoice = parseFirstEuroAmountFromText(text, [
    /\bGesamtsumme\s*:?\s*(-?\s*[\d.]+,\d{2})\s*(?:€|EUR)?/i,
    /\b(?:Gesamtbetrag|Rechnungsbetrag|Betrag\s+zu\s+zahlen|Zu\s+zahlen|Amount\s+due|Total)\b[^\n€]{0,120}(-?\s*[\d.]+,\d{2})\s*(?:€|EUR)?/i
  ]);
  let netInvoice = rawNetInvoice;
  if (
    (netInvoice === null || (netInvoice === 0 && grossInvoice !== null && vatInvoice !== null && grossInvoice > vatInvoice)) &&
    grossInvoice !== null &&
    vatInvoice !== null
  ) {
    netInvoice = Math.round((grossInvoice - vatInvoice) * 100) / 100;
  }
  if (netInvoice !== null && grossInvoice === null && vatInvoice !== null) {
    const inferredGross = Math.round((netInvoice + vatInvoice) * 100) / 100;
    return {
      ...recipient,
      kind: "invoice",
      status: "extracted",
      invoice_date: invoiceDate,
      month_key: invoiceDate ? extractMonthKeyFromGermanDate(invoiceDate) : null,
      net_amount: netInvoice,
      vat_amount: vatInvoice,
      gross_amount: inferredGross,
      confidence: "medium"
    };
  }
  if (netInvoice === null && grossInvoice !== null && vatInvoice === null) {
    netInvoice = grossInvoice;
  }
  if (netInvoice !== null || vatInvoice !== null || grossInvoice !== null) {
    return {
      ...recipient,
      kind: "invoice",
      status: netInvoice !== null && vatInvoice !== null ? "extracted" : "partial",
      invoice_date: invoiceDate,
      month_key: invoiceDate ? extractMonthKeyFromGermanDate(invoiceDate) : null,
      net_amount: netInvoice,
      vat_amount: vatInvoice ?? 0,
      gross_amount: grossInvoice,
      confidence: netInvoice !== null && vatInvoice !== null && grossInvoice !== null ? "high" : "medium"
    };
  }

  return {
    ...recipient,
    kind: "unknown",
    status: "not_extracted",
    invoice_date: null,
    month_key: null,
    net_amount: null,
    vat_amount: null,
    gross_amount: null,
    confidence: "low"
  };
}

function getLimitedPdfTextDebugLines(rawText, limit) {
  return getNormalizedPdfTextLines(rawText).slice(0, limit);
}

async function listAccountingDriveFolderPdfFiles(folderId, maxFiles, googleContext = {}) {
  const files = [];
  let pageToken;
  do {
    const res = await googleRequest(
      {
        method: "GET",
        url: "https://www.googleapis.com/drive/v3/files",
        params: {
          q: `'${escapeGoogleDriveQueryString(folderId)}' in parents and trashed = false`,
          fields: "nextPageToken,files(id,name,mimeType,size,createdTime,modifiedTime,parents,webViewLink)",
          pageSize: Math.min(100, Math.max(1, maxFiles - files.length)),
          pageToken,
          orderBy: "name",
          supportsAllDrives: true,
          includeItemsFromAllDrives: true
        }
      },
      googleContext
    );
    files.push(...(res.data.files || []));
    pageToken = res.data.nextPageToken;
  } while (pageToken && files.length < maxFiles);

  return files.filter((file) => file.mimeType === "application/pdf").slice(0, maxFiles);
}

function isAccountingDriveInvoiceCandidateFile(file) {
  const mimeType = String(file?.mimeType || "").split(";")[0].trim().toLowerCase();
  const ext = fileExtension(file?.name || "");
  return ACCOUNTING_ALLOWED_ATTACHMENT_MIME_TYPES.has(mimeType) || ACCOUNTING_ALLOWED_ATTACHMENT_EXTENSIONS.has(ext);
}

function publicAccountingDriveMetadata(file, folderId, folderLabel = null) {
  return {
    id: file.id,
    folder_id: folderId,
    folder_label: folderLabel,
    name: file.name || null,
    mimeType: file.mimeType || null,
    size: file.size ? String(file.size) : null,
    md5Checksum: file.md5Checksum || null,
    createdTime: file.createdTime || null,
    modifiedTime: file.modifiedTime || null,
    webViewLink: file.webViewLink || null,
    vip_sha256: file.appProperties?.vip_sha256 || null,
    vip_source: file.appProperties?.vip_source || null
  };
}

async function listAccountingDriveFolderInvoiceFiles(folderId, maxFiles, googleContext = {}) {
  const files = [];
  let pageToken;
  do {
    const res = await googleRequest(
      {
        method: "GET",
        url: "https://www.googleapis.com/drive/v3/files",
        params: {
          q: `'${escapeGoogleDriveQueryString(folderId)}' in parents and trashed = false`,
          fields:
            "nextPageToken,files(id,name,mimeType,size,md5Checksum,appProperties,createdTime,modifiedTime,parents,webViewLink)",
          pageSize: Math.min(100, Math.max(1, maxFiles - files.length)),
          pageToken,
          orderBy: "modifiedTime desc",
          supportsAllDrives: true,
          includeItemsFromAllDrives: true
        }
      },
      googleContext
    );
    files.push(...(res.data.files || []));
    pageToken = res.data.nextPageToken;
  } while (pageToken && files.length < maxFiles);

  return files.filter(isAccountingDriveInvoiceCandidateFile).slice(0, maxFiles);
}

function normalizeAccountingKnownFile(file) {
  return {
    id: String(file?.id || ""),
    folder_id: file?.folder_id ? String(file.folder_id) : null,
    folder_label: file?.folder_label ? String(file.folder_label) : null,
    name: file?.name ? String(file.name) : null,
    mimeType: file?.mimeType ? String(file.mimeType) : null,
    size: file?.size === undefined || file?.size === null ? null : String(file.size),
    md5Checksum: file?.md5Checksum ? String(file.md5Checksum) : null,
    modifiedTime: file?.modifiedTime ? String(file.modifiedTime) : null,
    vip_sha256: file?.vip_sha256 ? String(file.vip_sha256) : null
  };
}

function compareAccountingDriveMetadata(previous, current) {
  const fields = ["folder_id", "name", "mimeType", "size", "md5Checksum", "modifiedTime", "vip_sha256"];
  const changed_fields = [];
  for (const field of fields) {
    const beforeValue = previous[field] ?? null;
    const afterValue = current[field] ?? null;
    if (beforeValue !== afterValue) {
      changed_fields.push({
        field,
        before: beforeValue,
        after: afterValue
      });
    }
  }
  return changed_fields;
}

async function downloadDrivePdfBuffer(fileId, maxBytes, googleContext = {}) {
  const res = await googleRequest(
    {
      method: "GET",
      url: `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
      params: {
        alt: "media",
        supportsAllDrives: true
      },
      responseType: "arraybuffer",
      maxContentLength: maxBytes,
      maxBodyLength: maxBytes
    },
    googleContext
  );
  const buffer = Buffer.from(res.data);
  if (buffer.length > maxBytes) {
    throw new Error(`PDF ${fileId} ist groesser als max_pdf_bytes.`);
  }
  return buffer;
}

function compactDriveAppProperty(value, maxLength = 124) {
  return String(value || "").slice(0, maxLength);
}

async function scanBufferWithClamd(buffer) {
  const host = process.env.CLAMD_HOST || process.env.CLAMAV_HOST || "";
  const port = Number(process.env.CLAMD_PORT || process.env.CLAMAV_PORT || 3310);
  if (!host) {
    return {
      status: "not_configured",
      clean: false,
      scanner: "clamd",
      detail: "CLAMD_HOST/CLAMAV_HOST ist nicht gesetzt."
    };
  }

  return new Promise((resolve, reject) => {
    const socket = net.connect({ host, port });
    const timeout = setTimeout(() => {
      socket.destroy();
      reject(new Error(`ClamAV scan timeout (${host}:${port})`));
    }, Number(process.env.CLAMD_TIMEOUT_MS || 30_000));
    const chunks = [];

    socket.once("connect", () => {
      socket.write(Buffer.from("zINSTREAM\0", "binary"));
      for (let offset = 0; offset < buffer.length; offset += 64 * 1024) {
        const chunk = buffer.subarray(offset, Math.min(offset + 64 * 1024, buffer.length));
        const size = Buffer.alloc(4);
        size.writeUInt32BE(chunk.length, 0);
        socket.write(size);
        socket.write(chunk);
      }
      const end = Buffer.alloc(4);
      socket.write(end);
    });
    socket.on("data", (chunk) => chunks.push(chunk));
    socket.once("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    socket.once("end", () => {
      clearTimeout(timeout);
      const response = Buffer.concat(chunks).toString("utf8").replace(/\u0000/g, "").trim();
      resolve({
        status: /:\s*OK$/i.test(response) || /^stream:\s*OK$/i.test(response) ? "clean" : "not_clean",
        clean: /:\s*OK$/i.test(response) || /^stream:\s*OK$/i.test(response),
        scanner: "clamd",
        response
      });
    });
  });
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

function assertMoritzAsanaConfirmed({ confirmedByAsana, asanaTaskGid, approvedBy, actionName }) {
  assertAsanaConfirmed(confirmedByAsana, asanaTaskGid, actionName);
  if (!/moritz\s+feichtmeyer/i.test(String(approvedBy || ""))) {
    throw new Error(`${actionName} braucht eine Asana-Freigabe von Moritz Feichtmeyer. Setze approved_by entsprechend.`);
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

function getWordPressImportConfigSummary() {
  let parsedUrl = null;
  try {
    parsedUrl = new URL(WP_IMPORT_URL);
  } catch {
    parsedUrl = null;
  }

  const apiKeyConfigured = Boolean(process.env.WORDPRESS_GK_API_KEY);
  return {
    wordpress_import_url_configured: Boolean(WP_IMPORT_URL),
    wordpress_import_url_valid: Boolean(parsedUrl),
    wordpress_import_host: parsedUrl?.host || null,
    wordpress_import_path: parsedUrl?.pathname || null,
    wordpress_api_key_env: "WORDPRESS_GK_API_KEY",
    wordpress_api_key_configured: apiKeyConfigured,
    ready_for_import: Boolean(parsedUrl && apiKeyConfigured)
  };
}

function summarizeWordPressImportResponse(responseData) {
  if (!responseData || typeof responseData !== "object") {
    return {
      success_count: null,
      error_count: null,
      subscriber_ids: null,
      list_id: null
    };
  }

  return {
    success_count: Number.isFinite(Number(responseData.success_count)) ? Number(responseData.success_count) : null,
    error_count: Number.isFinite(Number(responseData.error_count)) ? Number(responseData.error_count) : null,
    subscriber_ids: Array.isArray(responseData.subscriber_ids) ? responseData.subscriber_ids : null,
    list_id: responseData.list_id ?? null
  };
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

function normalizeBufferProjectKey(projectKey) {
  const normalized = String(projectKey || "").trim().toLowerCase();
  if (!/^[a-z0-9][a-z0-9_-]{1,80}$/.test(normalized)) {
    throw new Error("project_key muss aus Kleinbuchstaben, Zahlen, Bindestrichen oder Unterstrichen bestehen.");
  }
  return normalized;
}

function bufferProjectEnvSuffix(projectKey) {
  return normalizeBufferProjectKey(projectKey).replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "").toUpperCase();
}

function normalizeBufferChannelKey(channel) {
  const normalized = String(channel || "").trim().toLowerCase();
  const aliases = {
    ig: "instagram",
    insta: "instagram",
    instagram: "instagram",
    pin: "pinterest",
    pinterest: "pinterest",
    linkedin: "linkedin",
    linked_in: "linkedin",
    li: "linkedin"
  };
  const mapped = aliases[normalized] || normalized;
  if (!["instagram", "pinterest", "linkedin"].includes(mapped)) {
    throw new Error(`Unbekannter Buffer-Kanal: ${channel}. Erlaubt: instagram, pinterest, linkedin.`);
  }
  return mapped;
}

function bufferChannelEnvSuffix(channel) {
  return normalizeBufferChannelKey(channel).toUpperCase();
}

function getBufferProjectConfigDetails(projectKey, { requireApiKey = true, requireOrganizationId = false } = {}) {
  const normalizedProjectKey = normalizeBufferProjectKey(projectKey);
  const suffix = bufferProjectEnvSuffix(normalizedProjectKey);
  const apiKeyEnvNames = [`BUFFER_API_KEY_${suffix}`, `BUFFER_TOKEN_${suffix}`, `BUFFER_ACCESS_TOKEN_${suffix}`];
  const selectedApiKeyEnvName = apiKeyEnvNames.find((name) => Boolean(process.env[name])) || apiKeyEnvNames[0];
  const apiKey = process.env[selectedApiKeyEnvName] || "";
  const organizationEnvName = `BUFFER_ORGANIZATION_ID_${suffix}`;
  const organizationId = process.env[organizationEnvName] || "";

  if (requireApiKey && !apiKey) {
    throw new Error(`Buffer API-Key fehlt fuer ${normalizedProjectKey}. Setze ${apiKeyEnvNames.join(" oder ")}.`);
  }
  if (requireOrganizationId && !organizationId) {
    throw new Error(`Buffer Organization-ID fehlt fuer ${normalizedProjectKey}. Setze ${organizationEnvName}.`);
  }

  return {
    project_key: normalizedProjectKey,
    suffix,
    config: { apiKey, organizationId },
    summary: {
      project_key: normalizedProjectKey,
      buffer_api_base: BUFFER_API_BASE,
      api_key_configured: Boolean(apiKey),
      api_key_env_name: apiKey ? selectedApiKeyEnvName : null,
      api_key_candidate_env_names: apiKeyEnvNames,
      organization_id_configured: Boolean(organizationId),
      organization_id_env_name: organizationEnvName,
      ready_for_read_calls: Boolean(apiKey),
      ready_for_schedule_calls: Boolean(apiKey && organizationId),
      timeout_ms: BUFFER_TIMEOUT_MS
    }
  };
}

function getBufferChannelConfig(projectKey, channel, { requireChannelId = true } = {}) {
  const normalizedProjectKey = normalizeBufferProjectKey(projectKey);
  const projectSuffix = bufferProjectEnvSuffix(normalizedProjectKey);
  const normalizedChannel = normalizeBufferChannelKey(channel);
  const channelSuffix = bufferChannelEnvSuffix(normalizedChannel);
  const envName = `BUFFER_CHANNEL_ID_${projectSuffix}_${channelSuffix}`;
  const channelId = process.env[envName] || "";
  if (requireChannelId && !channelId) {
    throw new Error(`Buffer Channel-ID fehlt fuer ${normalizedProjectKey}/${normalizedChannel}. Setze ${envName}.`);
  }
  return {
    channel: normalizedChannel,
    channel_id: channelId || null,
    channel_id_configured: Boolean(channelId),
    channel_id_env_name: envName
  };
}

function getBufferConfiguredChannels(projectKey, channels = ["instagram", "pinterest", "linkedin"]) {
  return channels.map((channel) => getBufferChannelConfig(projectKey, channel, { requireChannelId: false }));
}

async function bufferGraphqlRequest(projectKey, { query, variables, timeout = BUFFER_TIMEOUT_MS }) {
  const { config } = getBufferProjectConfigDetails(projectKey, { requireApiKey: true });
  const res = await axios.post(
    BUFFER_API_BASE,
    { query, variables },
    {
      timeout,
      validateStatus: () => true,
      headers: {
        Authorization: `Bearer ${config.apiKey}`,
        "Content-Type": "application/json",
        Accept: "application/json",
        "User-Agent": WEB_FETCH_USER_AGENT
      }
    }
  );

  const ok = res.status >= 200 && res.status < 300 && !res.data?.errors;
  return {
    ok,
    status: res.status,
    status_text: res.statusText,
    data: res.data,
    errors: res.data?.errors || null
  };
}

function assertBufferGraphqlOk(response, context) {
  if (response.ok) return;
  const errors = response.errors ? JSON.stringify(response.errors) : JSON.stringify(response.data);
  throw new Error(`${context} fehlgeschlagen: HTTP ${response.status} ${response.status_text}; ${errors}`);
}

async function fetchBufferOrganizations(projectKey) {
  const response = await bufferGraphqlRequest(projectKey, {
    query: `query GetOrganizations {
      account {
        organizations {
          id
          name
          ownerEmail
        }
      }
    }`
  });
  assertBufferGraphqlOk(response, "Buffer Organization-Read");
  return response.data?.data?.account?.organizations || [];
}

async function fetchBufferChannels(projectKey, organizationId) {
  const response = await bufferGraphqlRequest(projectKey, {
    query: `query GetChannels {
      channels(input: { organizationId: "${escapeGraphqlString(organizationId)}" }) {
        id
        name
        displayName
        service
        avatar
        isQueuePaused
      }
    }`
  });
  assertBufferGraphqlOk(response, "Buffer Channel-Read");
  return response.data?.data?.channels || [];
}

async function fetchBufferScheduledPosts(projectKey, { organizationId, channelIds = [], first = 100 }) {
  const safeFirst = Math.min(Math.max(Number(first) || 20, 1), 100);
  const channelFilter = channelIds.length
    ? `, channelIds: [${channelIds.map((id) => `"${escapeGraphqlString(id)}"`).join(", ")}]`
    : "";
  const response = await bufferGraphqlRequest(projectKey, {
    query: `query GetScheduledPosts {
      posts(
        first: ${safeFirst}
        input: {
          organizationId: "${escapeGraphqlString(organizationId)}"
          sort: [{ field: dueAt, direction: asc }, { field: createdAt, direction: desc }]
          filter: { status: [scheduled]${channelFilter} }
        }
      ) {
        edges {
          node {
            id
            text
            dueAt
            createdAt
            channelId
            status
          }
        }
      }
    }`
  });
  assertBufferGraphqlOk(response, "Buffer Scheduled-Posts-Read");
  return (response.data?.data?.posts?.edges || []).map((edge) => edge.node).filter(Boolean);
}

function escapeGraphqlString(value) {
  return String(value || "").replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\n/g, "\\n").replace(/\r/g, "\\r");
}

function parseCloudinaryUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const parsed = new URL(raw);
  if (parsed.protocol !== "cloudinary:") {
    throw new Error("CLOUDINARY_URL muss mit cloudinary:// beginnen.");
  }
  return {
    cloudName: parsed.hostname,
    apiKey: decodeURIComponent(parsed.username || ""),
    apiSecret: decodeURIComponent(parsed.password || "")
  };
}

function getCloudinaryConfigDetails({ requireCredentials = true } = {}) {
  const urlConfig = process.env.CLOUDINARY_URL ? parseCloudinaryUrl(process.env.CLOUDINARY_URL) : null;
  const cloudName = process.env.CLOUDINARY_CLOUD_NAME || urlConfig?.cloudName || "";
  const apiKey = process.env.CLOUDINARY_API_KEY || urlConfig?.apiKey || "";
  const apiSecret = process.env.CLOUDINARY_API_SECRET || urlConfig?.apiSecret || "";
  const folderPrefix = CLOUDINARY_DEFAULT_FOLDER_PREFIX;

  if (requireCredentials && (!cloudName || !apiKey || !apiSecret)) {
    throw new Error(
      "Cloudinary-Konfiguration fehlt. Setze CLOUDINARY_URL oder CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY und CLOUDINARY_API_SECRET."
    );
  }

  return {
    config: { cloudName, apiKey, apiSecret, folderPrefix },
    summary: {
      cloudinary_url_configured: Boolean(process.env.CLOUDINARY_URL),
      cloud_name_configured: Boolean(cloudName),
      api_key_configured: Boolean(apiKey),
      api_secret_configured: Boolean(apiSecret),
      folder_prefix: folderPrefix,
      ready_for_uploads: Boolean(cloudName && apiKey && apiSecret),
      upload_timeout_ms: CLOUDINARY_UPLOAD_TIMEOUT_MS
    }
  };
}

function sanitizeCloudinaryPathPart(value, fallback = "asset") {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
  return normalized || fallback;
}

function cloudinarySignature(params, apiSecret) {
  const payload = Object.entries(params)
    .filter(([, value]) => value !== undefined && value !== null && value !== "")
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
  return createHash("sha1").update(`${payload}${apiSecret}`).digest("hex");
}

async function uploadImageToCloudinary({ projectKey, asanaTaskGid, attachment, bytes, contentType, index }) {
  const { config } = getCloudinaryConfigDetails({ requireCredentials: true });
  const taskPart = asanaTaskGid ? `asana-${sanitizeCloudinaryPathPart(asanaTaskGid)}` : `manual-${Date.now()}`;
  const folder = [config.folderPrefix, sanitizeCloudinaryPathPart(projectKey), taskPart].filter(Boolean).join("/");
  const publicId = `${String(index + 1).padStart(2, "0")}-${sanitizeCloudinaryPathPart(
    attachment?.name || attachment?.gid || randomUUID()
  )}-${randomUUID().slice(0, 8)}`;
  const timestamp = Math.floor(Date.now() / 1000);
  const signParams = { folder, public_id: publicId, timestamp };
  const signature = cloudinarySignature(signParams, config.apiSecret);
  const dataUri = `data:${contentType || "image/jpeg"};base64,${bytes.toString("base64")}`;
  const body = new URLSearchParams({
    file: dataUri,
    api_key: config.apiKey,
    timestamp: String(timestamp),
    folder,
    public_id: publicId,
    signature
  });

  const res = await axios.post(`https://api.cloudinary.com/v1_1/${encodeURIComponent(config.cloudName)}/image/upload`, body, {
    timeout: CLOUDINARY_UPLOAD_TIMEOUT_MS,
    validateStatus: () => true,
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "User-Agent": WEB_FETCH_USER_AGENT
    }
  });

  if (res.status < 200 || res.status >= 300 || !res.data?.secure_url) {
    throw new Error(`Cloudinary-Upload fehlgeschlagen: HTTP ${res.status}; ${JSON.stringify(res.data)}`);
  }

  return {
    attachment_gid: attachment?.gid,
    attachment_name: attachment?.name,
    bytes: bytes.length,
    content_type: contentType || null,
    cloudinary_public_id: res.data.public_id,
    secure_url: res.data.secure_url,
    width: res.data.width,
    height: res.data.height,
    format: res.data.format
  };
}

function normalizeCloudinaryCleanupPrefix({ folderPrefix, projectKey, targetPrefix }) {
  const basePrefix = [folderPrefix, sanitizeCloudinaryPathPart(projectKey)].filter(Boolean).join("/");
  const rawPrefix = String(targetPrefix || `${basePrefix}/`).trim().replace(/^\/+/, "");
  const prefix = rawPrefix.endsWith("/") ? rawPrefix : `${rawPrefix}/`;

  if (!basePrefix || !prefix.startsWith(`${basePrefix}/`)) {
    throw new Error(`Cloudinary-Cleanup-Prefix muss unter ${basePrefix}/ liegen.`);
  }
  if (prefix.length < 12 || prefix.split("/").filter(Boolean).length < 2) {
    throw new Error("Cloudinary-Cleanup-Prefix ist zu breit oder ungueltig.");
  }

  return prefix;
}

async function cloudinaryAdminRequest({ method = "GET", path: requestPath, params, data, config, timeout }) {
  const url = `https://api.cloudinary.com/v1_1/${encodeURIComponent(config.cloudName)}${requestPath}`;
  const res = await axios.request({
    url,
    method,
    params,
    data,
    timeout: timeout || CLOUDINARY_ADMIN_TIMEOUT_MS,
    validateStatus: () => true,
    auth: {
      username: config.apiKey,
      password: config.apiSecret
    },
    headers: {
      "User-Agent": WEB_FETCH_USER_AGENT,
      ...(data ? { "Content-Type": "application/x-www-form-urlencoded" } : {})
    }
  });

  if (res.status < 200 || res.status >= 300) {
    throw new Error(`Cloudinary-Admin-Request fehlgeschlagen: HTTP ${res.status}; ${JSON.stringify(res.data)}`);
  }
  return res.data || {};
}

async function listCloudinaryResourcesByPrefix({ config, prefix, maxResources }) {
  const resources = [];
  let nextCursor = null;

  do {
    const remaining = maxResources - resources.length;
    if (remaining <= 0) break;
    const data = await cloudinaryAdminRequest({
      config,
      path: "/resources/image/upload",
      params: {
        prefix,
        max_results: Math.min(500, remaining),
        fields: "public_id,asset_id,created_at,bytes,secure_url,resource_type,type",
        ...(nextCursor ? { next_cursor: nextCursor } : {})
      }
    });
    resources.push(...(data.resources || []));
    nextCursor = data.next_cursor || null;
  } while (nextCursor);

  return { resources, next_cursor: nextCursor };
}

async function deleteCloudinaryResourcesByPublicId({ config, publicIds, invalidate = false }) {
  const batches = [];
  for (let index = 0; index < publicIds.length; index += 100) {
    batches.push(publicIds.slice(index, index + 100));
  }

  const results = [];
  for (const batch of batches) {
    const body = new URLSearchParams();
    for (const publicId of batch) body.append("public_ids[]", publicId);
    body.set("invalidate", invalidate ? "true" : "false");

    const data = await cloudinaryAdminRequest({
      method: "DELETE",
      config,
      path: "/resources/image/upload",
      data: body
    });
    results.push(data);
  }
  return results;
}

function bufferAssetInputFromUrls(urls) {
  return urls.map((url) => `{ image: { url: "${escapeGraphqlString(url)}" } }`).join(", ");
}

function bufferMetadataInputForChannel({ channel, instagramType, instagramShouldShareToFeed }) {
  if (channel === "instagram") {
    return `\n          metadata: { instagram: { type: ${instagramType}, shouldShareToFeed: ${
      instagramShouldShareToFeed ? "true" : "false"
    } } }`;
  }
  return "";
}

function bufferCreatePostMutation({ text, channelId, dueAt, mediaUrls = [], metadataInput = "" }) {
  const assets = mediaUrls.length ? `\n          assets: [${bufferAssetInputFromUrls(mediaUrls)}]` : "";
  const dueAtLine = dueAt ? `\n          dueAt: "${escapeGraphqlString(dueAt)}"` : "";
  const mode = dueAt ? "customScheduled" : "addToQueue";
  return `mutation CreatePost {
    createPost(
      input: {
        text: "${escapeGraphqlString(text)}"
        channelId: "${escapeGraphqlString(channelId)}"
        schedulingType: automatic
        mode: ${mode}${dueAtLine}${assets}${metadataInput}
      }
    ) {
      ... on PostActionSuccess {
        post {
          id
          text
          dueAt
          createdAt
          channelId
          status
          assets {
            id
            mimeType
          }
        }
      }
      ... on MutationError {
        message
      }
    }
  }`;
}

async function createBufferPost(projectKey, { text, channelId, dueAt, mediaUrls, metadataInput }) {
  const response = await bufferGraphqlRequest(projectKey, {
    query: bufferCreatePostMutation({ text, channelId, dueAt, mediaUrls, metadataInput })
  });
  assertBufferGraphqlOk(response, "Buffer createPost");
  const result = response.data?.data?.createPost;
  if (result?.message && !result?.post) {
    throw new Error(`Buffer createPost MutationError: ${result.message}`);
  }
  if (!result?.post?.id) {
    throw new Error(`Buffer createPost lieferte keinen Post-Readback: ${JSON.stringify(response.data)}`);
  }
  return result.post;
}

function localDatePartsInTimeZone(date, timeZone) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  })
    .formatToParts(date)
    .reduce((acc, part) => {
      if (part.type !== "literal") acc[part.type] = part.value;
      return acc;
    }, {});
  return {
    year: Number(parts.year),
    month: Number(parts.month),
    day: Number(parts.day),
    hour: Number(parts.hour),
    minute: Number(parts.minute),
    dateKey: `${parts.year}-${parts.month}-${parts.day}`
  };
}

function zonedDateTimeToUtcIso({ dateKey, time, timeZone }) {
  const [year, month, day] = dateKey.split("-").map(Number);
  const [hour, minute] = time.split(":").map(Number);
  let guess = new Date(Date.UTC(year, month - 1, day, hour, minute, 0, 0));
  for (let i = 0; i < 4; i += 1) {
    const local = localDatePartsInTimeZone(guess, timeZone);
    const diffMinutes =
      (Date.UTC(local.year, local.month - 1, local.day, local.hour, local.minute) -
        Date.UTC(year, month - 1, day, hour, minute)) /
      60000;
    if (diffMinutes === 0) break;
    guess = new Date(guess.getTime() - diffMinutes * 60000);
  }
  return guess.toISOString();
}

function addDaysUtcDateKey(dateKey, days) {
  const [year, month, day] = dateKey.split("-").map(Number);
  const date = new Date(Date.UTC(year, month - 1, day + days, 12, 0, 0, 0));
  return date.toISOString().slice(0, 10);
}

function normalizePreferredBufferTimes(values) {
  const rawValues = Array.isArray(values) && values.length ? values : ["08:30", "17:15"];
  const normalized = rawValues.map((value) => String(value || "").trim()).filter(Boolean);
  for (const value of normalized) {
    if (!/^\d{2}:\d{2}$/.test(value)) {
      throw new Error(`Ungueltiger Buffer-Zeitwert: ${value}. Erwartet HH:MM, z. B. 08:30.`);
    }
    const [hour, minute] = value.split(":").map(Number);
    if (hour > 23 || minute > 59) throw new Error(`Ungueltiger Buffer-Zeitwert: ${value}.`);
  }
  return [...new Set(normalized)];
}

function resolveBufferAutoSlot({ scheduledPosts, timeZone, preferredTimes, startDate = new Date() }) {
  const occupiedDates = new Set(
    scheduledPosts
      .map((post) => post?.dueAt)
      .filter(Boolean)
      .map((dueAt) => localDatePartsInTimeZone(new Date(dueAt), timeZone).dateKey)
  );
  const todayKey = localDatePartsInTimeZone(startDate, timeZone).dateKey;
  const times = normalizePreferredBufferTimes(preferredTimes);

  for (let offset = 1; offset <= 120; offset += 1) {
    const dateKey = addDaysUtcDateKey(todayKey, offset);
    if (occupiedDates.has(dateKey)) continue;
    const localTime = times[0];
    return {
      due_at: zonedDateTimeToUtcIso({ dateKey, time: localTime, timeZone }),
      local_date: dateKey,
      local_time: localTime,
      timezone: timeZone,
      reason: "next_free_local_day_for_channel"
    };
  }
  throw new Error("Kein freier Buffer-Tag innerhalb der naechsten 120 Tage gefunden.");
}

function bufferTextForChannel({ channel, text, textByChannel }) {
  const candidate = textByChannel?.[channel] || textByChannel?.[normalizeBufferChannelKey(channel)] || text;
  const normalized = String(candidate || "").trim();
  if (!normalized) throw new Error(`Buffer-Text fehlt fuer Kanal ${channel}.`);
  if (normalized.length > 10_000) throw new Error(`Buffer-Text fuer ${channel} ist zu lang (${normalized.length}).`);
  return normalized;
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
    "agent_lock_acquire",
    "Setzt einen temporaeren Arbeits-Lock fuer eine Asana-Aufgabe, einen Agentenlauf oder eine Ressource, damit parallele Automationen nicht dieselbe Arbeit gleichzeitig ausfuehren. Vor tiefer Bearbeitung oder Schreibaktionen nutzen.",
    {
      agent_id: agentIdSchema,
      scope: z.enum(["task", "agent", "resource"]).optional().default("task"),
      task_gid: z.string().optional(),
      resource_key: z.string().optional(),
      run_id: z.string().min(3).max(160).optional(),
      lock_token: z.string().min(8).max(120).optional(),
      holder: z.string().max(160).optional(),
      purpose: z.string().max(500).optional(),
      ttl_seconds: z.number().int().min(AGENT_LOCK_MIN_TTL_SECONDS).max(AGENT_LOCK_MAX_TTL_SECONDS).optional(),
      dry_run: z.boolean().optional().default(false)
    },
    TOOL_IDEMPOTENT_SAFE_WRITE,
    async ({ agent_id, scope, task_gid, resource_key, run_id, lock_token, holder, purpose, ttl_seconds, dry_run }) => {
      const nowMs = Date.now();
      cleanupAgentOperationLocks(nowMs);
      const key = buildAgentOperationLockKey({ scope, agent_id, task_gid, resource_key });
      const ttl = clampAgentLockTtlSeconds(ttl_seconds);
      const existing = AGENT_OPERATION_LOCKS.get(key);
      const sameHolder =
        existing &&
        ((run_id && existing.run_id === run_id) || (lock_token && existing.lock_token === lock_token));

      if (existing && !sameHolder) {
        return out({
          agent_id,
          acquired: false,
          reason: "lock_already_held",
          requested: { key, scope, task_gid: task_gid || null, resource_key: resource_key || null, run_id: run_id || null },
          active_lock: serializeAgentOperationLock(existing, nowMs),
          retry_after_seconds: Math.max(1, Math.ceil((existing.expires_at_ms - nowMs) / 1000))
        });
      }

      const nextRunId = run_id || existing?.run_id || `run-${randomUUID()}`;
      const nextToken = existing?.lock_token || lock_token || randomUUID();
      const nextLock = {
        key,
        scope,
        agent_id,
        task_gid: task_gid || null,
        resource_key: resource_key || null,
        run_id: nextRunId,
        lock_token: nextToken,
        holder: holder || agent_id,
        purpose: purpose || "",
        acquired_at_ms: existing?.acquired_at_ms || nowMs,
        renewed_at_ms: nowMs,
        expires_at_ms: nowMs + ttl * 1000
      };

      if (!dry_run) {
        AGENT_OPERATION_LOCKS.set(key, nextLock);
      }

      return out({
        agent_id,
        acquired: true,
        dry_run,
        renewed: Boolean(existing),
        lock: serializeAgentOperationLock(nextLock, nowMs),
        note:
          "Lock ist temporaer und schuetzt vor paralleler Bearbeitung derselben Aufgabe/Ressource. Nach Abschluss agent_lock_release nutzen; bei Crash laeuft der Lock per TTL ab."
      });
    }
  );

  server.tool(
    "agent_lock_release",
    "Gibt einen zuvor per agent_lock_acquire gesetzten temporaeren Arbeits-Lock frei. Nur derselbe run_id oder lock_token darf freigeben.",
    {
      agent_id: agentIdSchema,
      scope: z.enum(["task", "agent", "resource"]).optional().default("task"),
      task_gid: z.string().optional(),
      resource_key: z.string().optional(),
      run_id: z.string().min(3).max(160).optional(),
      lock_token: z.string().min(8).max(120).optional(),
      dry_run: z.boolean().optional().default(false)
    },
    TOOL_IDEMPOTENT_SAFE_WRITE,
    async ({ agent_id, scope, task_gid, resource_key, run_id, lock_token, dry_run }) => {
      if (!run_id && !lock_token) {
        throw new Error("agent_lock_release braucht run_id oder lock_token, damit kein fremder Lock geloescht wird.");
      }
      const nowMs = Date.now();
      cleanupAgentOperationLocks(nowMs);
      const key = buildAgentOperationLockKey({ scope, agent_id, task_gid, resource_key });
      const existing = AGENT_OPERATION_LOCKS.get(key);

      if (!existing) {
        return out({ agent_id, released: false, reason: "no_active_lock", key, dry_run });
      }

      const sameHolder =
        (run_id && existing.run_id === run_id) || (lock_token && existing.lock_token === lock_token);
      if (!sameHolder) {
        return out({
          agent_id,
          released: false,
          reason: "lock_held_by_other_run",
          key,
          active_lock: serializeAgentOperationLock(existing, nowMs),
          dry_run
        });
      }

      if (!dry_run) {
        AGENT_OPERATION_LOCKS.delete(key);
      }

      return out({
        agent_id,
        released: true,
        dry_run,
        released_lock: serializeAgentOperationLock(existing, nowMs)
      });
    }
  );

  server.tool(
    "agent_lock_status",
    "Liest temporaere Arbeits-Locks read-only. Nutze es zur Diagnose, wenn ein Task oder Run bereits von einer anderen Automation bearbeitet wird.",
    {
      agent_id: agentIdSchema,
      scope: z.enum(["task", "agent", "resource"]).optional(),
      task_gid: z.string().optional(),
      resource_key: z.string().optional(),
      include_all: z.boolean().optional().default(false)
    },
    TOOL_READ_ONLY,
    async ({ agent_id, scope, task_gid, resource_key, include_all }) => {
      const nowMs = Date.now();
      cleanupAgentOperationLocks(nowMs);
      let key;
      if (scope) {
        key = buildAgentOperationLockKey({ scope, agent_id, task_gid, resource_key });
      }
      const locks = [...AGENT_OPERATION_LOCKS.values()]
        .filter((lock) => (key ? lock.key === key : include_all || lock.agent_id === agent_id))
        .map((lock) => serializeAgentOperationLock(lock, nowMs));

      return out({ agent_id, key: key || null, active_locks: locks });
    }
  );

  server.tool(
    "asana_whoami",
    "Prueft den Asana-Token eines Agenten und gibt den zugehoerigen User ohne Secret-Werte aus.",
    { agent_id: agentIdSchema },
    TOOL_READ_ONLY,
    async ({ agent_id }) => {
      const asana = getAsana(agent_id);
      const res = await asanaRequestWithRetry(asana, { method: "GET", url: "/users/me" });
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
      const user = await asanaRequestWithRetry(asana, { method: "GET", url: "/users/me" });
      const workspace = user.data.data.workspaces[0].gid;

      const res = await asanaRequestWithRetry(asana, {
        method: "GET",
        url: "/tasks",
        params: { assignee: "me", workspace, completed_since: "now", limit }
      });

      return out({ agent_id, tasks: res.data.data });
    }
  );

  server.tool(
    "asana_search_tasks",
    "Sucht Asana-Aufgaben im Workspace read-only ueber den korrekten GET-/tasks/search-Pfad. Vermeidet den bekannten 404 durch POST /tasks/search und ist der Standardpfad fuer API-Ersatz-Inbox/Follower-/Collaborator-Deltas.",
    {
      agent_id: agentIdSchema,
      workspace_gid: z.string().optional(),
      assignee_any: z.string().optional(),
      followers_any: z.string().optional(),
      involved_any: z.string().optional(),
      completed: z.boolean().optional(),
      modified_since: z.string().optional(),
      sort_by: z.string().optional().default("modified_at"),
      sort_ascending: z.boolean().optional(),
      limit: z.number().int().min(1).max(100).optional().default(20),
      opt_fields: z
        .string()
        .optional()
        .default(
          "gid,name,completed,assignee.gid,assignee.name,due_on,due_at,modified_at,permalink_url,tags.name,memberships.project.gid,memberships.project.name"
        ),
      extra_params: z.record(z.string(), z.any()).optional()
    },
    TOOL_READ_ONLY,
    async ({
      agent_id,
      workspace_gid,
      assignee_any,
      followers_any,
      involved_any,
      completed,
      modified_since,
      sort_by,
      sort_ascending,
      limit,
      opt_fields,
      extra_params
    }) => {
      if (workspace_gid) validateAsanaGid(workspace_gid, "workspace_gid");
      const asana = getAsana(agent_id);
      const me = await asanaRequestWithRetry(asana, { method: "GET", url: "/users/me" });
      const meGid = me.data.data.gid;
      const workspace = workspace_gid || me.data.data.workspaces?.[0]?.gid;
      if (!workspace) throw new Error("Kein Asana-Workspace gefunden.");

      const normalizeUserSelector = (value, label) => {
        if (!value) return undefined;
        if (value === "me") return meGid;
        validateAsanaGid(value, label);
        return value;
      };

      const params = {
        ...(extra_params || {}),
        limit,
        opt_fields,
        sort_by
      };
      if (sort_ascending !== undefined) params.sort_ascending = sort_ascending;
      if (completed !== undefined) params.completed = completed;
      if (modified_since) params.modified_since = modified_since;
      const assignee = normalizeUserSelector(assignee_any, "assignee_any");
      const followers = normalizeUserSelector(followers_any, "followers_any");
      const involved = normalizeUserSelector(involved_any, "involved_any");
      if (assignee) params["assignee.any"] = assignee;
      if (followers) params["followers.any"] = followers;
      if (involved) params["involved.any"] = involved;

      try {
        const res = await asanaRequestWithRetry(asana, {
          method: "GET",
          url: `/workspaces/${workspace}/tasks/search`,
          params
        });
        return out({
          agent_id,
          workspace_gid: workspace,
          user_gid: meGid,
          search_status: "ok",
          request_method: "GET",
          request_path: `/workspaces/${workspace}/tasks/search`,
          params,
          tasks: res.data.data || []
        });
      } catch (error) {
        if (error.response?.status === 404) {
          return out({
            agent_id,
            workspace_gid: workspace,
            user_gid: meGid,
            search_status: "not_available_or_not_supported",
            request_method: "GET",
            request_path: `/workspaces/${workspace}/tasks/search`,
            params,
            error: compactAxiosError(error),
            tasks: [],
            fallback:
              "Keine POST-Search-Retrys ausfuehren. Eigene Tasks mit asana_get_my_tasks lesen und bekannte/Follower-Tasks aus agentenspezifischem State gezielt per GET /tasks/{gid} pruefen."
          });
        }
        throw error;
      }
    }
  );

  server.tool(
    "asana_create_task",
    "Legt eine Asana-Aufgabe ueber einen engen Pfad an. Erzwingt genau ein Projekt, klaren Titel, Assignee, Faelligkeit, Standardfelder Prioritaet=Mittel/Status=Todo, Supervisor-Follower und bei Follow-ups einen Link zur Ausgangsaufgabe.",
    {
      agent_id: agentIdSchema,
      name: z.string().min(12).max(200),
      assignee_gid: z.string(),
      project_gid: z.string().optional(),
      source_task_gid: z.string().optional(),
      description: z.string().optional(),
      project_selection_reason: z.string().optional(),
      allow_different_project_than_source: z.boolean().optional().default(false),
      due_on: z.string().optional(),
      due_in_days: z.number().int().min(0).max(30).optional().default(3),
      priority_value: z.string().optional().default("Mittel"),
      status_value: z.string().optional().default("Todo"),
      require_standard_custom_fields: z.boolean().optional().default(true),
      ensure_supervisor_follower: z.boolean().optional().default(true),
      supervisor_follower_gid: z.string().optional(),
      confirmed_by_asana: z.boolean().optional().default(false),
      creation_basis: z.string().min(20),
      dry_run: z.boolean().optional().default(false),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_SAFE_WRITE,
    async ({
      agent_id,
      name,
      assignee_gid,
      project_gid,
      source_task_gid,
      description,
      project_selection_reason,
      allow_different_project_than_source,
      due_on,
      due_in_days,
      priority_value,
      status_value,
      require_standard_custom_fields,
      ensure_supervisor_follower,
      supervisor_follower_gid,
      confirmed_by_asana,
      creation_basis,
      dry_run,
      verify_after
    }) => {
      validateAsanaGid(assignee_gid, "assignee_gid");
      validateAsanaGid(project_gid, "project_gid");
      validateAsanaGid(source_task_gid, "source_task_gid");
      validateAsanaDate(due_on, "due_on");
      const finalSupervisorFollowerGid = supervisor_follower_gid || ASANA_DEFAULT_SUPERVISOR_GID;
      if (ensure_supervisor_follower) validateAsanaGid(finalSupervisorFollowerGid, "supervisor_follower_gid");
      ensureNoUnsafeAsanaText(name, "Asana-Aufgabentitel");
      ensureNoUnsafeAsanaText(creation_basis, "Asana-Aufgabenanlage-Begruendung");

      if (!source_task_gid && !confirmed_by_asana && !dry_run) {
        throw new Error(
          "asana_create_task braucht source_task_gid oder confirmed_by_asana=true, damit neue Aufgaben nicht ohne klare Arbeitsgrundlage entstehen."
        );
      }

      if (
        ["follow up", "followup", "todo", "aufgabe", "nacharbeit"].includes(normalizeAsanaLabel(name))
      ) {
        throw new Error("Der Aufgabentitel ist zu generisch. Waehle einen treffenden, konkreten Titel.");
      }

      const asana = getAsana(agent_id);
      let source_task;
      let source_projects = [];

      if (source_task_gid) {
        const sourceRes = await asanaRequestWithRetry(asana, {
          method: "GET",
          url: `/tasks/${source_task_gid}`,
          params: { opt_fields: ASANA_TASK_CREATE_SOURCE_OPT_FIELDS }
        });
        source_task = sourceRes.data.data;
        source_projects = uniqueObjectsByGid(
          (source_task.memberships || []).map((membership) => membership.project).filter(Boolean)
        );
      }

      let selectedProjectGid = project_gid;
      if (!selectedProjectGid) {
        if (source_projects.length === 1) {
          selectedProjectGid = source_projects[0].gid;
        } else if (source_projects.length > 1) {
          throw new Error(
            "Die Ausgangsaufgabe ist mehreren Projekten zugeordnet. project_gid explizit setzen, damit die neue Aufgabe genau einem Projekt zugeordnet wird."
          );
        } else {
          throw new Error(
            "Kein Projekt aus der Ausgangsaufgabe ableitbar. project_gid explizit setzen und project_selection_reason angeben."
          );
        }
      }

      const sourceProjectGids = new Set(source_projects.map((project) => project.gid));
      const differsFromVisibleSourceProject = source_projects.length > 0 && !sourceProjectGids.has(selectedProjectGid);
      if (differsFromVisibleSourceProject && !allow_different_project_than_source) {
        throw new Error(
          "Die Ausgangsaufgabe hat ein sichtbares Projekt. Neue Follow-up-Aufgaben muessen dieses Projekt nutzen, ausser allow_different_project_than_source=true und project_selection_reason erklaert die Ausnahme."
        );
      }
      if (
        (differsFromVisibleSourceProject || (source_task_gid && source_projects.length === 0)) &&
        String(project_selection_reason || "").trim().length < 20
      ) {
        throw new Error(
          "project_selection_reason ist erforderlich, wenn kein sichtbares Ausgangsprojekt genutzt werden kann oder bewusst ein anderes Projekt gewaehlt wird."
        );
      }

      const projectRes = await asanaRequestWithRetry(asana, {
        method: "GET",
        url: `/projects/${selectedProjectGid}`,
        params: { opt_fields: ASANA_PROJECT_OPT_FIELDS }
      });
      const project = projectRes.data.data;
      const finalDueOn = due_on || dueOnInBerlinDaysFromNow(due_in_days);

      const { custom_fields, resolved, missing } = await resolveAsanaStandardCustomFields(asana, selectedProjectGid, {
        priorityValue: priority_value,
        statusValue: status_value,
        requireStandardCustomFields: require_standard_custom_fields
      });

      const html_notes = buildAsanaTaskHtmlNotes({
        description,
        source_task,
        project_selection_reason
      });

      const data = {
        name,
        assignee: assignee_gid,
        workspace: project.workspace?.gid,
        projects: [selectedProjectGid],
        due_on: finalDueOn,
        html_notes
      };
      if (Object.keys(custom_fields).length) data.custom_fields = custom_fields;

      const basis_sha256 = createHash("sha256").update(creation_basis, "utf8").digest("hex");

      if (dry_run) {
        return out({
          agent_id,
          dry_run: true,
          data,
          project,
          source_task,
          source_projects,
          standard_fields: resolved,
          missing_standard_fields: missing,
          ensure_supervisor_follower,
          supervisor_follower_gid: ensure_supervisor_follower ? finalSupervisorFollowerGid : null,
          creation_basis,
          creation_basis_sha256: basis_sha256
        });
      }

      const res = await asanaRequestWithRetry(asana, {
        method: "POST",
        url: "/tasks",
        timeout: ASANA_WRITE_TIMEOUT_MS,
        data: { data },
        params: { opt_fields: ASANA_TASK_CREATE_VERIFY_OPT_FIELDS }
      });

      let supervisor_follower_add_result = null;
      if (ensure_supervisor_follower) {
        const followerRes = await asanaRequestWithRetry(asana, {
          method: "POST",
          url: `/tasks/${res.data.data.gid}/addFollowers`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          data: { data: { followers: [finalSupervisorFollowerGid] } }
        });
        supervisor_follower_add_result = followerRes.data;
      }

      let verified_task;
      let verification_status = "not_requested";
      if (verify_after) {
        const verify = await asanaRequestWithRetry(asana, {
          method: "GET",
          url: `/tasks/${res.data.data.gid}`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          params: { opt_fields: ASANA_TASK_CREATE_VERIFY_OPT_FIELDS }
        });
        verified_task = verify.data.data;
        const verifiedProjectGids = uniqueObjectsByGid(
          (verified_task.memberships || []).map((membership) => membership.project).filter(Boolean)
        ).map((item) => item.gid);
        if (verifiedProjectGids.length !== 1 || verifiedProjectGids[0] !== selectedProjectGid) {
          throw new Error(
            `Asana-Readback zeigt nicht genau das erwartete Projekt (${selectedProjectGid}), sondern ${verifiedProjectGids.join(
              ", "
            ) || "keins"}.`
          );
        }
        if (verified_task.assignee?.gid !== assignee_gid) {
          throw new Error("Asana-Readback zeigt nicht den erwarteten Assignee.");
        }
        if (verified_task.due_on !== finalDueOn) {
          throw new Error(`Asana-Readback due_on=${verified_task.due_on || "null"} statt erwartet ${finalDueOn}.`);
        }
        if (ensure_supervisor_follower) {
          const verifiedFollowerGids = getAsanaTaskFollowerGids(verified_task);
          if (!verifiedFollowerGids.includes(finalSupervisorFollowerGid)) {
            throw new Error(`Asana-Readback zeigt Supervisor-Follower ${finalSupervisorFollowerGid} nicht.`);
          }
        }
        for (const [fieldGid, enumGid] of Object.entries(custom_fields)) {
          const verifiedField = (verified_task.custom_fields || []).find((field) => field.gid === fieldGid);
          if (verifiedField?.enum_value?.gid !== enumGid) {
            throw new Error(`Asana-Readback zeigt Custom Field ${fieldGid} nicht mit erwarteter Enum ${enumGid}.`);
          }
        }
        verification_status = "ok";
      }

      return out({
        agent_id,
        task: res.data.data,
        supervisor_follower_add_result,
        ensure_supervisor_follower,
        supervisor_follower_gid: ensure_supervisor_follower ? finalSupervisorFollowerGid : null,
        verified_task,
        verification_status,
        project,
        source_task,
        source_projects,
        standard_fields: resolved,
        missing_standard_fields: missing,
        creation_basis,
        creation_basis_sha256: basis_sha256
      });
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
    "asana_update_task_description",
    "Ergaenzt eine Asana-Aufgabenbeschreibung kontrolliert und mit Readback. Standardpfad fuer dauerhafte Aenderungen an Routine-Aufgaben; Kommentare allein reichen dafuer nicht.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string(),
      section_title: z.string().min(3).max(140).optional().default("Dauerhafte Routine-Anweisung"),
      section_text: z.string().min(20).max(8000),
      dedupe_key: z.string().min(3).max(160).optional(),
      update_basis: z.string().min(20),
      require_routine_context: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      allow_completed_task: z.boolean().optional().default(false),
      dry_run: z.boolean().optional().default(false),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_SAFE_WRITE,
    async ({
      agent_id,
      task_gid,
      section_title,
      section_text,
      dedupe_key,
      update_basis,
      require_routine_context,
      confirmed_by_asana,
      allow_completed_task,
      dry_run,
      verify_after
    }) => {
      validateAsanaGid(task_gid, "task_gid");
      ensureNoUnsafeAsanaText(update_basis, "Asana-Aufgabenbeschreibung-Aenderungsgrund");

      if (!confirmed_by_asana && !dry_run) {
        throw new Error(
          "asana_update_task_description braucht confirmed_by_asana=true, damit dauerhafte Aufgabenbeschreibungen nur auf sichtbare Asana-/Moritz-Anweisung geaendert werden."
        );
      }

      const asana = getAsana(agent_id);
      const beforeRes = await asanaRequestWithRetry(asana, {
        method: "GET",
        url: `/tasks/${task_gid}`,
        params: { opt_fields: ASANA_TASK_DESCRIPTION_OPT_FIELDS }
      });
      const before_task = beforeRes.data.data;

      if (before_task.completed && !allow_completed_task) {
        throw new Error("Abgeschlossene Aufgabenbeschreibungen werden nicht geaendert, ausser allow_completed_task=true.");
      }

      const routine_like_task = isRoutineLikeAsanaTask(before_task);
      if (require_routine_context && !routine_like_task) {
        throw new Error("Aufgabe ist keine erkennbare Routine (Tag Routine oder Titelpraefix R: fehlt).");
      }

      const existingForDedupe = `${before_task.notes || ""}\n${before_task.html_notes || ""}`;
      if (dedupe_key && existingForDedupe.includes(dedupe_key)) {
        return out({
          agent_id,
          task_gid,
          no_change: true,
          reason: "dedupe_key_already_present",
          dedupe_key,
          before_task,
          routine_like_task
        });
      }

      const sectionHtml = buildAsanaDescriptionAppendSection({
        section_title,
        section_text,
        dedupe_key
      });
      const existingHtmlNotes = before_task.html_notes || escapeAsanaTextWithLinks(before_task.notes || "");
      const html_notes = appendAsanaHtmlNotesSection(existingHtmlNotes, sectionHtml);
      const html_sha256 = createHash("sha256").update(html_notes, "utf8").digest("hex");
      const update_basis_sha256 = createHash("sha256").update(update_basis, "utf8").digest("hex");

      if (dry_run) {
        return out({
          agent_id,
          dry_run: true,
          task_gid,
          routine_like_task,
          before_task,
          html_notes,
          html_bytes: Buffer.byteLength(html_notes, "utf8"),
          html_sha256,
          update_basis,
          update_basis_sha256
        });
      }

      const res = await asanaRequestWithRetry(asana, {
        method: "PUT",
        url: `/tasks/${task_gid}`,
        timeout: ASANA_WRITE_TIMEOUT_MS,
        data: { data: { html_notes } },
        params: { opt_fields: ASANA_TASK_DESCRIPTION_OPT_FIELDS }
      });

      let verified_task;
      let verification_status = "not_requested";
      if (verify_after) {
        const verify = await asanaRequestWithRetry(asana, {
          method: "GET",
          url: `/tasks/${task_gid}`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          params: { opt_fields: ASANA_TASK_DESCRIPTION_OPT_FIELDS }
        });
        verified_task = verify.data.data;
        const readback = `${verified_task.notes || ""}\n${verified_task.html_notes || ""}`;
        const snippet = String(section_text)
          .split(/\n+/)
          .map((line) => line.trim())
          .find((line) => line.length >= 8)
          ?.slice(0, 140);
        if (snippet && !readback.includes(snippet) && !readback.includes(escapeAsanaXml(snippet))) {
          throw new Error("Asana-Readback zeigt die neue Aufgabenbeschreibung-Ergaenzung nicht sicher.");
        }
        if (dedupe_key && !readback.includes(dedupe_key)) {
          throw new Error("Asana-Readback zeigt den Dedupe-Key der Aufgabenbeschreibung-Ergaenzung nicht.");
        }
        verification_status = "ok";
      }

      return out({
        agent_id,
        task_gid,
        task: res.data.data,
        before_task,
        verified_task,
        routine_like_task,
        verification_status,
        html_bytes: Buffer.byteLength(html_notes, "utf8"),
        html_sha256,
        update_basis,
        update_basis_sha256
      });
    }
  );

  server.tool(
    "asana_complete_task",
    "Schliesst eine Asana-Aufgabe kontrolliert ab. Nur fuer eigene zugewiesene Aufgaben nach erfolgreicher Bearbeitung; prueft Assignee, optional finalen Kommentar, Supervisor-Follower, Routine-Due-Gate und Readback. Bei bestehenden Routine-Aufgaben wird ein fehlender Default-Supervisor/Moritz nicht automatisch wieder hinzugefuegt, ausser allow_routine_supervisor_readd=true.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string(),
      completion_basis: z.string().min(20),
      final_comment_story_gid: z.string().optional(),
      require_final_comment: z.boolean().optional().default(true),
      ensure_supervisor_follower: z.boolean().optional().default(true),
      allow_routine_supervisor_readd: z.boolean().optional().default(false),
      supervisor_follower_gid: z.string().optional(),
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
      ensure_supervisor_follower,
      allow_routine_supervisor_readd,
      supervisor_follower_gid,
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
      const finalSupervisorFollowerGid = supervisor_follower_gid || ASANA_DEFAULT_SUPERVISOR_GID;
      if (ensure_supervisor_follower) validateAsanaGid(finalSupervisorFollowerGid, "supervisor_follower_gid");

      const asana = getAsana(agent_id);
      const me = await asana.get("/users/me");
      const meGid = me.data.data.gid;

      const taskRes = await asana.get(`/tasks/${task_gid}`, {
        params: {
          opt_fields:
            "gid,name,completed,completed_at,assignee.gid,assignee.name,due_on,due_at,tags.name,followers.gid,followers.name,permalink_url"
        }
      });
      const task = taskRes.data.data;
      const routine_like_task = isRoutineLikeAsanaTask(task);
      const due_gate = getAsanaTaskDueGate(task);
      const initialFollowerGids = getAsanaTaskFollowerGids(task);
      const supervisor_follower_present_initially = initialFollowerGids.includes(finalSupervisorFollowerGid);
      const supervisor_follower_readd_guarded =
        ensure_supervisor_follower &&
        !supervisor_follower_present_initially &&
        isRoutineSupervisorDoNotReaddCandidate(task, finalSupervisorFollowerGid) &&
        !allow_routine_supervisor_readd;
      const effective_ensure_supervisor_follower =
        ensure_supervisor_follower && !supervisor_follower_readd_guarded;
      const supervisor_follower_readd_guard_reason = supervisor_follower_readd_guarded
        ? "Routine-Aufgabe ohne Default-Supervisor-Follower: als moeglicher do_not_readd-Fall behandelt. Kein automatisches Re-Add bei normal erfolgreichem Abschluss; fuer Blocker/Rueckfrage explizit allow_routine_supervisor_readd=true setzen."
        : null;

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

      const blockedByFutureRoutineDue = routine_like_task && due_gate.blocked;
      if (blockedByFutureRoutineDue && !dry_run) {
        throw new Error(
          `Routine-Aufgabe darf nicht vor Faelligkeit abgeschlossen werden (${due_gate.reason}; due_on=${task.due_on || "-"}; due_at=${task.due_at || "-"}).`
        );
      }

      const basis_sha256 = createHash("sha256").update(completion_basis, "utf8").digest("hex");

      if (dry_run || task.completed) {
        return out({
          agent_id,
          dry_run,
          already_completed: Boolean(task.completed),
          allowed_to_complete: !task.completed && !blockedByFutureRoutineDue,
          ensure_supervisor_follower,
          effective_ensure_supervisor_follower,
          allow_routine_supervisor_readd,
          supervisor_follower_readd_guarded,
          supervisor_follower_readd_guard_reason,
          supervisor_follower_gid: effective_ensure_supervisor_follower ? finalSupervisorFollowerGid : null,
          supervisor_follower_present: ensure_supervisor_follower
            ? supervisor_follower_present_initially
            : null,
          routine_like_task,
          due_gate,
          task,
          final_comment,
          completion_basis,
          completion_basis_sha256: basis_sha256
        });
      }

      let supervisor_follower_add_result = null;
      if (effective_ensure_supervisor_follower && !supervisor_follower_present_initially) {
        const followerRes = await asanaRequestWithRetry(asana, {
          method: "POST",
          url: `/tasks/${task_gid}/addFollowers`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          data: { data: { followers: [finalSupervisorFollowerGid] } }
        });
        supervisor_follower_add_result = followerRes.data;
      }

      const completeRes = await asana.put(
        `/tasks/${task_gid}`,
        { data: { completed: true } },
        {
          timeout: ASANA_WRITE_TIMEOUT_MS,
          params: { opt_fields: "gid,name,completed,completed_at,assignee.gid,assignee.name,permalink_url" }
        }
      );

      let verified_task;
      let verification_status = "not_requested";
      if (verify_after) {
        const verify = await asana.get(`/tasks/${task_gid}`, {
          timeout: ASANA_WRITE_TIMEOUT_MS,
          params: { opt_fields: "gid,name,completed,completed_at,assignee.gid,assignee.name,followers.gid,followers.name,permalink_url" }
        });
        verified_task = verify.data.data;
        if (!verified_task.completed) {
          throw new Error("Asana-Readback zeigt completed=false nach Abschlussversuch.");
        }
        if (verified_task.assignee?.gid !== meGid) {
          throw new Error("Asana-Readback zeigt veraenderten Assignee nach Abschlussversuch.");
        }
        if (effective_ensure_supervisor_follower) {
          const verifiedFollowerGids = getAsanaTaskFollowerGids(verified_task);
          if (!verifiedFollowerGids.includes(finalSupervisorFollowerGid)) {
            throw new Error(`Asana-Readback zeigt Supervisor-Follower ${finalSupervisorFollowerGid} nicht.`);
          }
        }
        verification_status = "ok";
      }

      return out({
        agent_id,
        task_gid,
        completed: true,
        task: completeRes.data.data,
        supervisor_follower_add_result,
        ensure_supervisor_follower,
        effective_ensure_supervisor_follower,
        allow_routine_supervisor_readd,
        supervisor_follower_readd_guarded,
        supervisor_follower_readd_guard_reason,
        supervisor_follower_gid: effective_ensure_supervisor_follower ? finalSupervisorFollowerGid : null,
        verified_task,
        verification_status,
        final_comment,
        routine_like_task,
        due_gate,
        completion_basis,
        completion_basis_sha256: basis_sha256
      });
    }
  );

  server.tool(
    "asana_update_task_schedule",
    "Aendert Faelligkeitsdatum/-uhrzeit einer Asana-Aufgabe ueber einen engen Pfad. due_at ist verbindlich und muss als ISO-Zeitpunkt mit Zeitzone uebergeben werden; due_on und due_at sind gegenseitig exklusiv.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string(),
      due_on: z.string().optional(),
      due_at: z.string().optional(),
      clear_due: z.boolean().optional().default(false),
      dry_run: z.boolean().optional().default(false),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_IDEMPOTENT_SAFE_WRITE,
    async ({ agent_id, task_gid, due_on, due_at, clear_due, dry_run, verify_after }) => {
      validateAsanaGid(task_gid, "task_gid");
      validateAsanaDate(due_on, "due_on");
      validateAsanaDateTime(due_at, "due_at");

      const setCount = [Boolean(due_on), Boolean(due_at), Boolean(clear_due)].filter(Boolean).length;
      if (setCount !== 1) {
        throw new Error("Genau eines von due_on, due_at oder clear_due=true muss gesetzt sein.");
      }

      const data = clear_due ? { due_on: null, due_at: null } : due_at ? { due_at } : { due_on };
      const asana = getAsana(agent_id);
      const beforeRes = await asanaRequestWithRetry(asana, {
        method: "GET",
        url: `/tasks/${task_gid}`,
        params: { opt_fields: ASANA_TASK_SCHEDULE_OPT_FIELDS }
      });
      const before_task = beforeRes.data.data;

      if (dry_run) {
        return out({ agent_id, dry_run: true, task_gid, data, before_task });
      }

      const res = await asanaRequestWithRetry(asana, {
        method: "PUT",
        url: `/tasks/${task_gid}`,
        timeout: ASANA_WRITE_TIMEOUT_MS,
        data: { data },
        params: { opt_fields: ASANA_TASK_SCHEDULE_OPT_FIELDS }
      });

      let verified_task;
      let verification_status = "not_requested";
      if (verify_after) {
        const verify = await asanaRequestWithRetry(asana, {
          method: "GET",
          url: `/tasks/${task_gid}`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          params: { opt_fields: ASANA_TASK_SCHEDULE_OPT_FIELDS }
        });
        verified_task = verify.data.data;

        if (clear_due) {
          if (verified_task.due_on || verified_task.due_at) {
            throw new Error("Asana-Readback zeigt weiterhin due_on/due_at nach clear_due=true.");
          }
        } else if (due_at) {
          if (verified_task.due_at !== due_at) {
            throw new Error(`Asana-Readback due_at=${verified_task.due_at || "null"} statt erwartet ${due_at}.`);
          }
        } else if (due_on && verified_task.due_on !== due_on) {
          throw new Error(`Asana-Readback due_on=${verified_task.due_on || "null"} statt erwartet ${due_on}.`);
        }
        verification_status = "ok";
      }

      return out({
        agent_id,
        task_gid,
        data,
        before_task,
        task: res.data.data,
        verified_task,
        verification_status
      });
    }
  );

  server.tool(
    "asana_verify_task_schedule",
    "Verifiziert read-only, ob eine Asana-Aufgabe dem erwarteten Faelligkeits-/Routine-Vertrag entspricht. Nutze dieses Tool nach jeder neuen/geaenderten Routine, Faelligkeit, Uhrzeit oder Schedule-Annahme; es aendert keine Aufgabe. Fehlender Default-Supervisor/Moritz auf bestehenden Routinen wird standardmaessig als moeglicher do_not_readd-Fall nur gewarnt.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string(),
      expected_due_on: z.string().optional(),
      expected_due_at: z.string().optional(),
      expected_due_date_berlin: z.string().optional(),
      expected_due_time_berlin: z.string().optional(),
      expect_due_time: z.boolean().optional().default(false),
      expect_routine_tag: z.boolean().optional().default(false),
      expected_assignee_gid: z.string().optional(),
      expected_project_gid: z.string().optional(),
      expected_follower_gid: z.string().optional(),
      expected_follower_gids: z.array(z.string()).optional().default([]),
      expected_name_contains: z.string().optional(),
      expected_completed: z.boolean().optional(),
      recurrence_expected: z.boolean().optional().default(false),
      allow_unverifiable_native_recurrence: z.boolean().optional().default(false),
      allow_routine_supervisor_do_not_readd: z.boolean().optional().default(true)
    },
    TOOL_READ_ONLY,
    async ({
      agent_id,
      task_gid,
      expected_due_on,
      expected_due_at,
      expected_due_date_berlin,
      expected_due_time_berlin,
      expect_due_time,
      expect_routine_tag,
      expected_assignee_gid,
      expected_project_gid,
      expected_follower_gid,
      expected_follower_gids,
      expected_name_contains,
      expected_completed,
      recurrence_expected,
      allow_unverifiable_native_recurrence,
      allow_routine_supervisor_do_not_readd
    }) => {
      validateAsanaGid(task_gid, "task_gid");
      const asana = getAsana(agent_id);
      const res = await asanaRequestWithRetry(asana, {
        method: "GET",
        url: `/tasks/${task_gid}`,
        params: { opt_fields: ASANA_TASK_SCHEDULE_VERIFY_OPT_FIELDS }
      });
      const task = res.data.data;
      const verification = buildAsanaScheduleVerification({
        task,
        expected_due_on,
        expected_due_at,
        expected_due_date_berlin,
        expected_due_time_berlin,
        expect_due_time,
        expect_routine_tag,
        expected_assignee_gid,
        expected_project_gid,
        expected_follower_gid,
        expected_follower_gids,
        expected_name_contains,
        expected_completed,
        recurrence_expected,
        allow_unverifiable_native_recurrence,
        allow_routine_supervisor_do_not_readd
      });
      return out({
        agent_id,
        task_gid,
        task,
        ...verification,
        production_ready_claim_allowed: verification.verification_status === "ok",
        note:
          verification.verification_status === "ok"
            ? "Schedule-Vertrag ist API-seitig verifiziert."
            : "Nicht als vollstaendig korrekt/einsatzbereit behaupten, bis alle Fehler behoben sind. ok_with_warnings reicht nicht fuer 100%-Claims."
      });
    }
  );

  server.tool(
    "asana_verify_next_routine_instance",
    "Verifiziert read-only nach einem Routine-Abschluss, ob Asana eine passende naechste offene Routine-Instanz erzeugt hat. Nutze dieses Tool, bevor ein Agent behauptet, Wiederholung/Fortsetzung sei korrekt. Fehlender Default-Supervisor/Moritz auf Routine-Folgeinstanzen wird standardmaessig als moeglicher do_not_readd-Fall nur gewarnt.",
    {
      agent_id: agentIdSchema,
      source_task_gid: z.string(),
      expected_next_due_on: z.string().optional(),
      expected_next_due_at: z.string().optional(),
      expected_next_due_date_berlin: z.string().optional(),
      expected_next_due_time_berlin: z.string().optional(),
      expected_assignee_gid: z.string().optional(),
      expected_project_gid: z.string().optional(),
      expected_follower_gid: z.string().optional(),
      expected_follower_gids: z.array(z.string()).optional().default([]),
      expected_name_exact: z.string().optional(),
      expect_routine_tag: z.boolean().optional().default(true),
      allow_routine_supervisor_do_not_readd: z.boolean().optional().default(true),
      limit: z.number().int().min(1).max(100).optional().default(100)
    },
    TOOL_READ_ONLY,
    async ({
      agent_id,
      source_task_gid,
      expected_next_due_on,
      expected_next_due_at,
      expected_next_due_date_berlin,
      expected_next_due_time_berlin,
      expected_assignee_gid,
      expected_project_gid,
      expected_follower_gid,
      expected_follower_gids,
      expected_name_exact,
      expect_routine_tag,
      allow_routine_supervisor_do_not_readd,
      limit
    }) => {
      validateAsanaGid(source_task_gid, "source_task_gid");
      validateAsanaDate(expected_next_due_on, "expected_next_due_on");
      validateAsanaDateTime(expected_next_due_at, "expected_next_due_at");
      validateAsanaDate(expected_next_due_date_berlin, "expected_next_due_date_berlin");
      validateAsanaLocalTime(expected_next_due_time_berlin, "expected_next_due_time_berlin");
      validateAsanaGid(expected_assignee_gid, "expected_assignee_gid");
      validateAsanaGid(expected_project_gid, "expected_project_gid");
      validateAsanaGid(expected_follower_gid, "expected_follower_gid");
      for (const followerGid of expected_follower_gids || []) validateAsanaGid(followerGid, "expected_follower_gids");

      const asana = getAsana(agent_id);
      const sourceRes = await asanaRequestWithRetry(asana, {
        method: "GET",
        url: `/tasks/${source_task_gid}`,
        params: { opt_fields: ASANA_TASK_SCHEDULE_VERIFY_OPT_FIELDS }
      });
      const source_task = sourceRes.data.data;
      const projectGids = getAsanaTaskProjectGids(source_task);
      const selectedProjectGid = expected_project_gid || projectGids[0];
      if (!selectedProjectGid) {
        throw new Error("Kein Projekt aus source_task ableitbar. expected_project_gid setzen.");
      }
      const expectedName = expected_name_exact || source_task.name;
      const expectedAssignee = expected_assignee_gid || source_task.assignee?.gid;

      const tasksRes = await asanaRequestWithRetry(asana, {
        method: "GET",
        url: `/projects/${selectedProjectGid}/tasks`,
        params: {
          completed_since: "now",
          limit,
          opt_fields: ASANA_TASK_SCHEDULE_VERIFY_OPT_FIELDS
        }
      });
      const candidates = (tasksRes.data.data || []).filter((task) => {
        if (task.gid === source_task_gid) return false;
        if (String(task.name || "") !== String(expectedName || "")) return false;
        if (expectedAssignee && task.assignee?.gid !== expectedAssignee) return false;
        return true;
      });

      const candidate_results = candidates.map((task) => ({
        task,
        ...buildAsanaScheduleVerification({
          task,
          expected_due_on: expected_next_due_on,
          expected_due_at: expected_next_due_at,
          expected_due_date_berlin: expected_next_due_date_berlin,
          expected_due_time_berlin: expected_next_due_time_berlin,
          expect_due_time: Boolean(expected_next_due_at || expected_next_due_time_berlin),
          expect_routine_tag,
          expected_assignee_gid: expectedAssignee,
          expected_project_gid: selectedProjectGid,
          expected_follower_gid,
          expected_follower_gids,
          expected_name_contains: expectedName,
          expected_completed: false,
          recurrence_expected: false,
          allow_unverifiable_native_recurrence: false,
          allow_routine_supervisor_do_not_readd
        })
      }));
      const exactMatches = candidate_results.filter((candidate) => candidate.verification_status === "ok");
      const acceptableWarningMatches = candidate_results.filter(
        (candidate) => candidate.verification_status === "ok_with_warnings"
      );
      const acceptableMatches = [...exactMatches, ...acceptableWarningMatches];
      const verification_status =
        exactMatches.length === 1
          ? "ok"
          : exactMatches.length > 1
            ? "ambiguous"
            : acceptableMatches.length === 1
              ? "ok_with_warnings"
              : acceptableMatches.length > 1
                ? "ambiguous"
                : "failed";

      return out({
        agent_id,
        source_task_gid,
        source_task,
        selected_project_gid: selectedProjectGid,
        expected_name: expectedName,
        expected_assignee_gid: expectedAssignee || null,
        candidate_count: candidates.length,
        exact_match_count: exactMatches.length,
        acceptable_match_count: acceptableMatches.length,
        verification_status,
        production_ready_claim_allowed: verification_status === "ok",
        candidates: candidate_results,
        note:
          verification_status === "ok"
            ? "Naechste Routine-Instanz ist API-seitig gegen den erwarteten Vertrag verifiziert."
            : verification_status === "ok_with_warnings"
              ? "Naechste Routine-Instanz wurde gefunden, hat aber Warnungen. Reine Default-Supervisor/Moritz-Warnungen auf bestehenden Routinen sind kein automatischer Re-Add-Auftrag."
            : "Naechste Routine-Instanz nicht eindeutig korrekt verifiziert. Nicht behaupten, dass Wiederholung/Fortsetzung sauber ist."
      });
    }
  );

  server.tool(
    "asana_repair_routine_catchup_due",
    "Korrigiert nach verspaetetem Routine-Abschluss eine erzeugte Folgeinstanz, deren Faelligkeit noch in der Vergangenheit/heute liegt, auf den naechsten vorgesehenen Zeitpunkt nach jetzt. Verhindert Catch-up-Backlogs bei kurz getakteten Asana-Wiederholungen.",
    {
      agent_id: agentIdSchema,
      source_task_gid: z.string(),
      next_task_gid: z.string(),
      next_future_due_on: z.string().optional(),
      next_future_due_at: z.string().optional(),
      next_future_due_date_berlin: z.string().optional(),
      next_future_due_time_berlin: z.string().optional(),
      repair_basis: z.string().min(20),
      dry_run: z.boolean().optional().default(false),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_IDEMPOTENT_SAFE_WRITE,
    async ({
      agent_id,
      source_task_gid,
      next_task_gid,
      next_future_due_on,
      next_future_due_at,
      next_future_due_date_berlin,
      next_future_due_time_berlin,
      repair_basis,
      dry_run,
      verify_after
    }) => {
      validateAsanaGid(source_task_gid, "source_task_gid");
      validateAsanaGid(next_task_gid, "next_task_gid");
      if (source_task_gid === next_task_gid) {
        throw new Error("source_task_gid und next_task_gid muessen unterschiedliche Aufgaben sein.");
      }

      const now = new Date();
      const scheduleInput = resolveNextFutureAsanaScheduleInput(
        {
          next_future_due_on,
          next_future_due_at,
          next_future_due_date_berlin,
          next_future_due_time_berlin
        },
        now
      );
      const asana = getAsana(agent_id);
      const [sourceRes, nextRes] = await Promise.all([
        asanaRequestWithRetry(asana, {
          method: "GET",
          url: `/tasks/${source_task_gid}`,
          params: { opt_fields: ASANA_TASK_SCHEDULE_VERIFY_OPT_FIELDS }
        }),
        asanaRequestWithRetry(asana, {
          method: "GET",
          url: `/tasks/${next_task_gid}`,
          params: { opt_fields: ASANA_TASK_SCHEDULE_VERIFY_OPT_FIELDS }
        })
      ]);
      const source_task = sourceRes.data.data;
      const next_task = nextRes.data.data;

      if (!dry_run && !source_task.completed) {
        throw new Error("Catch-up-Reparatur ist erst nach abgeschlossenem source_task erlaubt.");
      }
      if (next_task.completed) {
        throw new Error("next_task ist bereits abgeschlossen; keine Catch-up-Reparatur moeglich.");
      }
      if (!isRoutineLikeAsanaTask(next_task)) {
        throw new Error("next_task ist keine Routine-Aufgabe (Tag Routine oder Titel R: fehlt).");
      }
      if (String(source_task.name || "") !== String(next_task.name || "")) {
        throw new Error("source_task und next_task haben unterschiedliche Titel; falsche Folgeinstanz vermutet.");
      }
      if (source_task.assignee?.gid && next_task.assignee?.gid !== source_task.assignee.gid) {
        throw new Error("source_task und next_task haben unterschiedliche Assignees; falsche Folgeinstanz vermutet.");
      }

      const sourceProjectGids = getAsanaTaskProjectGids(source_task);
      const nextProjectGids = getAsanaTaskProjectGids(next_task);
      const sharedProjectGids = sourceProjectGids.filter((projectGid) => nextProjectGids.includes(projectGid));
      if (!sharedProjectGids.length) {
        throw new Error("source_task und next_task teilen kein Projekt; falsche Folgeinstanz vermutet.");
      }

      const due_position = getAsanaTaskDuePosition(next_task, now);
      if (due_position.mode === "none") {
        throw new Error("next_task hat keine Faelligkeit; Catch-up-Reparatur braucht einen vorhandenen Due-Stand.");
      }
      if (due_position.relation === "invalid") {
        throw new Error("next_task hat eine ungueltige Faelligkeit; manuelle Klaerung noetig.");
      }
      const repairNeeded = due_position.relation !== "future";
      const repair_basis_sha256 = createHash("sha256").update(repair_basis, "utf8").digest("hex");

      if (!repairNeeded || dry_run) {
        return out({
          agent_id,
          dry_run,
          repair_needed: repairNeeded,
          no_action_reason: repairNeeded ? null : "next_task_due_is_already_after_now",
          source_task,
          next_task,
          due_position,
          proposed_schedule: scheduleInput,
          shared_project_gids: sharedProjectGids,
          repair_basis,
          repair_basis_sha256
        });
      }

      const updateRes = await asanaRequestWithRetry(asana, {
        method: "PUT",
        url: `/tasks/${next_task_gid}`,
        timeout: ASANA_WRITE_TIMEOUT_MS,
        data: { data: scheduleInput.data },
        params: { opt_fields: ASANA_TASK_SCHEDULE_OPT_FIELDS }
      });

      let verified_task = null;
      let verification_status = "not_requested";
      if (verify_after) {
        const verify = await asanaRequestWithRetry(asana, {
          method: "GET",
          url: `/tasks/${next_task_gid}`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          params: { opt_fields: ASANA_TASK_SCHEDULE_VERIFY_OPT_FIELDS }
        });
        verified_task = verify.data.data;

        if (scheduleInput.expected_due_at) {
          if (verified_task.due_at !== scheduleInput.expected_due_at) {
            throw new Error(
              `Asana-Readback due_at=${verified_task.due_at || "null"} statt erwartet ${scheduleInput.expected_due_at}.`
            );
          }
        } else if (scheduleInput.expected_due_on) {
          if (verified_task.due_on !== scheduleInput.expected_due_on || verified_task.due_at) {
            throw new Error(
              `Asana-Readback due_on=${verified_task.due_on || "null"}, due_at=${verified_task.due_at || "null"} statt erwartet ${scheduleInput.expected_due_on}.`
            );
          }
        }
        verification_status = "ok";
      }

      return out({
        agent_id,
        repaired: true,
        repair_needed: true,
        source_task,
        before_next_task: next_task,
        task: updateRes.data.data,
        verified_task,
        verification_status,
        previous_due_position: due_position,
        applied_schedule: scheduleInput,
        shared_project_gids: sharedProjectGids,
        repair_basis,
        repair_basis_sha256
      });
    }
  );

  server.tool(
    "asana_ensure_task_followers",
    "Stellt idempotent sicher, dass bestimmte Asana-Nutzer als Beteiligte/Follower einer Aufgabe gesetzt sind, und verifiziert den Readback. Standard fuer Hauptvorgesetzten-/Moritz-Beteiligung; nicht per rohem asana_request nutzen. Bei bestehenden Routine-Aufgaben wird ein fehlender Default-Supervisor/Moritz nicht automatisch wieder hinzugefuegt, ausser allow_routine_supervisor_readd=true.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string(),
      follower_gids: z.array(z.string()).min(1),
      ensure_basis: z.string().optional(),
      allow_routine_supervisor_readd: z.boolean().optional().default(false),
      dry_run: z.boolean().optional().default(false),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_IDEMPOTENT_SAFE_WRITE,
    async ({ agent_id, task_gid, follower_gids, ensure_basis, allow_routine_supervisor_readd, dry_run, verify_after }) => {
      validateAsanaGid(task_gid, "task_gid");
      const followersToEnsure = uniqueValues(follower_gids);
      for (const followerGid of followersToEnsure) validateAsanaGid(followerGid, "follower_gids");
      ensureNoUnsafeAsanaText(ensure_basis, "Asana-Follower-Begruendung");

      const asana = getAsana(agent_id);
      const beforeRes = await asanaRequestWithRetry(asana, {
        method: "GET",
        url: `/tasks/${task_gid}`,
        params: { opt_fields: ASANA_TASK_SCHEDULE_VERIFY_OPT_FIELDS }
      });
      const before_task = beforeRes.data.data;
      const currentFollowerGids = getAsanaTaskFollowerGids(before_task);
      const toAdd = followersToEnsure.filter((followerGid) => !currentFollowerGids.includes(followerGid));
      const guardedSkipFollowerGids = toAdd.filter(
        (followerGid) =>
          isRoutineSupervisorDoNotReaddCandidate(before_task, followerGid) && !allow_routine_supervisor_readd
      );
      const finalToAdd = toAdd.filter((followerGid) => !guardedSkipFollowerGids.includes(followerGid));
      const guardedSkipReason = guardedSkipFollowerGids.length
        ? "Routine-Aufgabe ohne Default-Supervisor-Follower: als moeglicher do_not_readd-Fall behandelt. Kein automatisches Re-Add; fuer Blocker/Rueckfrage explizit allow_routine_supervisor_readd=true setzen."
        : null;

      if (dry_run) {
        return out({
          agent_id,
          dry_run: true,
          task_gid,
          before_task,
          follower_gids: followersToEnsure,
          would_add: finalToAdd,
          guarded_skip_follower_gids: guardedSkipFollowerGids,
          guarded_skip_reason: guardedSkipReason,
          allow_routine_supervisor_readd,
          skipped_already_present: followersToEnsure.filter((followerGid) => currentFollowerGids.includes(followerGid)),
          ensure_basis: ensure_basis || null
        });
      }

      let add_result = null;
      if (finalToAdd.length) {
        const res = await asanaRequestWithRetry(asana, {
          method: "POST",
          url: `/tasks/${task_gid}/addFollowers`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          data: { data: { followers: finalToAdd } }
        });
        add_result = res.data;
      }

      let verified_task;
      let verification_status = "not_requested";
      if (verify_after) {
        const verify = await asanaRequestWithRetry(asana, {
          method: "GET",
          url: `/tasks/${task_gid}`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          params: { opt_fields: ASANA_TASK_SCHEDULE_VERIFY_OPT_FIELDS }
        });
        verified_task = verify.data.data;
        const verifiedFollowerGids = getAsanaTaskFollowerGids(verified_task);
        const followersToVerify = followersToEnsure.filter(
          (followerGid) => !guardedSkipFollowerGids.includes(followerGid)
        );
        for (const followerGid of followersToVerify) {
          if (!verifiedFollowerGids.includes(followerGid)) {
            throw new Error(`Asana-Readback zeigt erwarteten Follower ${followerGid} nicht.`);
          }
        }
        verification_status = "ok";
      }

      return out({
        agent_id,
        task_gid,
        before_task,
        follower_gids: followersToEnsure,
        added_follower_gids: finalToAdd,
        guarded_skip_follower_gids: guardedSkipFollowerGids,
        guarded_skip_reason: guardedSkipReason,
        allow_routine_supervisor_readd,
        skipped_already_present: followersToEnsure.filter((followerGid) => currentFollowerGids.includes(followerGid)),
        add_result,
        verified_task,
        verification_status,
        ensure_basis: ensure_basis || null
      });
    }
  );

  server.tool(
    "asana_leave_task_followers",
    "Entfernt Follower/Beteiligte aus einer Asana-Aufgabe ueber einen engen, verifizierten Pfad. Standard: der aufrufende Agent entfernt nur sich selbst als Nebenrollen-Follower, z. B. vor oder direkt nach einer Routine-Folgeinstanz. Andere Follower nur mit allow_remove_other_followers=true.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string(),
      follower_gid: z.string().optional(),
      leave_basis: z.string().min(12),
      allow_remove_other_followers: z.boolean().optional().default(false),
      dry_run: z.boolean().optional().default(false),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_IDEMPOTENT_SAFE_WRITE,
    async ({ agent_id, task_gid, follower_gid, leave_basis, allow_remove_other_followers, dry_run, verify_after }) => {
      validateAsanaGid(task_gid, "task_gid");
      if (follower_gid) validateAsanaGid(follower_gid, "follower_gid");
      ensureNoUnsafeAsanaText(leave_basis, "Asana-Follower-Entfernungsbegruendung");

      const asana = getAsana(agent_id);
      const meRes = await asanaRequestWithRetry(asana, { method: "GET", url: "/users/me" });
      const me = meRes.data.data;
      const targetFollowerGid = follower_gid || me.gid;
      validateAsanaGid(targetFollowerGid, "target_follower_gid");
      const removingSelf = String(targetFollowerGid) === String(me.gid);
      if (!removingSelf && !allow_remove_other_followers) {
        throw new Error(
          "Dieses Tool entfernt standardmaessig nur den aufrufenden Agenten selbst. Fuer andere Follower ist allow_remove_other_followers=true plus klare Begruendung noetig."
        );
      }

      const beforeRes = await asanaRequestWithRetry(asana, {
        method: "GET",
        url: `/tasks/${task_gid}`,
        params: { opt_fields: ASANA_TASK_SCHEDULE_VERIFY_OPT_FIELDS }
      });
      const before_task = beforeRes.data.data;
      if (String(before_task?.assignee?.gid || "") === String(targetFollowerGid)) {
        throw new Error(
          "Der aktuelle Assignee darf nicht ueber asana_leave_task_followers als Follower entfernt werden. Erst Verantwortlichkeit klaeren; Routine-Assignee bleibt unveraendert."
        );
      }
      const beforeFollowerGids = getAsanaTaskFollowerGids(before_task);
      const alreadyAbsent = !beforeFollowerGids.includes(targetFollowerGid);

      if (dry_run) {
        return out({
          agent_id,
          dry_run: true,
          task_gid,
          before_task,
          me,
          target_follower_gid: targetFollowerGid,
          removing_self: removingSelf,
          would_remove: !alreadyAbsent,
          skipped_already_absent: alreadyAbsent,
          routine_like: isRoutineLikeAsanaTask(before_task),
          leave_basis
        });
      }

      let remove_result = null;
      if (!alreadyAbsent) {
        const res = await asanaRequestWithRetry(asana, {
          method: "POST",
          url: `/tasks/${task_gid}/removeFollowers`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          data: { data: { followers: [targetFollowerGid] } }
        });
        remove_result = res.data;
      }

      let verified_task;
      let verification_status = "not_requested";
      if (verify_after) {
        const verify = await asanaRequestWithRetry(asana, {
          method: "GET",
          url: `/tasks/${task_gid}`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          params: { opt_fields: ASANA_TASK_SCHEDULE_VERIFY_OPT_FIELDS }
        });
        verified_task = verify.data.data;
        const verifiedFollowerGids = getAsanaTaskFollowerGids(verified_task);
        if (verifiedFollowerGids.includes(targetFollowerGid)) {
          throw new Error(`Asana-Readback zeigt entfernten Follower ${targetFollowerGid} weiterhin.`);
        }
        verification_status = "ok";
      }

      return out({
        agent_id,
        task_gid,
        before_task,
        me,
        target_follower_gid: targetFollowerGid,
        removing_self: removingSelf,
        removed_follower_gids: alreadyAbsent ? [] : [targetFollowerGid],
        skipped_already_absent: alreadyAbsent,
        routine_like: isRoutineLikeAsanaTask(before_task),
        remove_result,
        verified_task,
        verification_status,
        leave_basis
      });
    }
  );

  server.tool(
    "asana_find_tags",
    "Liest Tags eines Workspaces read-only, damit Agenten Tag-GIDs vor asana_update_task_tags sauber finden koennen.",
    {
      agent_id: agentIdSchema,
      workspace_gid: z.string().optional(),
      name_contains: z.string().optional(),
      limit: z.number().int().min(1).max(100).optional().default(50)
    },
    TOOL_READ_ONLY,
    async ({ agent_id, workspace_gid, name_contains, limit }) => {
      if (workspace_gid) validateAsanaGid(workspace_gid, "workspace_gid");
      const asana = getAsana(agent_id);
      let workspace = workspace_gid;
      if (!workspace) {
        const user = await asanaRequestWithRetry(asana, { method: "GET", url: "/users/me" });
        workspace = user.data.data.workspaces?.[0]?.gid;
      }
      if (!workspace) throw new Error("Kein Asana-Workspace gefunden.");

      const res = await asanaRequestWithRetry(asana, {
        method: "GET",
        url: `/workspaces/${workspace}/tags`,
        params: { limit, opt_fields: "gid,name,color,workspace.gid" }
      });
      const needle = String(name_contains || "").trim().toLowerCase();
      const tags = (res.data.data || []).filter((tag) =>
        needle ? String(tag.name || "").toLowerCase().includes(needle) : true
      );
      return out({ agent_id, workspace_gid: workspace, returned_count: tags.length, tags });
    }
  );

  server.tool(
    "asana_update_task_tags",
    "Fuegt Asana-Tags idempotent hinzu oder entfernt sie. Nutzt addTag/removeTag, prueft Routine-Tag-Schutz und verifiziert den Readback.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string(),
      add_tag_gids: z.array(z.string()).optional().default([]),
      remove_tag_gids: z.array(z.string()).optional().default([]),
      allow_routine_tag_change: z.boolean().optional().default(false),
      confirmed_by_asana: z.boolean().optional().default(false),
      dry_run: z.boolean().optional().default(false),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_IDEMPOTENT_SAFE_WRITE,
    async ({
      agent_id,
      task_gid,
      add_tag_gids,
      remove_tag_gids,
      allow_routine_tag_change,
      confirmed_by_asana,
      dry_run,
      verify_after
    }) => {
      validateAsanaGid(task_gid, "task_gid");
      const addTags = uniqueValues(add_tag_gids);
      const removeTags = uniqueValues(remove_tag_gids);
      for (const tagGid of [...addTags, ...removeTags]) validateAsanaGid(tagGid, "tag_gid");
      if (!addTags.length && !removeTags.length) {
        throw new Error("Mindestens ein Tag in add_tag_gids oder remove_tag_gids ist erforderlich.");
      }

      const asana = getAsana(agent_id);
      const beforeRes = await asanaRequestWithRetry(asana, {
        method: "GET",
        url: `/tasks/${task_gid}`,
        params: { opt_fields: ASANA_TASK_TAG_OPT_FIELDS }
      });
      const before_task = beforeRes.data.data;
      const currentTagGids = new Set((before_task.tags || []).map((tag) => tag.gid));
      const currentTagsByGid = new Map((before_task.tags || []).map((tag) => [tag.gid, tag]));
      const routineTagRemoval = removeTags.some(
        (tagGid) => String(currentTagsByGid.get(tagGid)?.name || "").toLowerCase() === "routine"
      );

      if (routineTagRemoval && !(allow_routine_tag_change && confirmed_by_asana)) {
        throw new Error(
          "Das Entfernen des Tags Routine ist gesperrt, ausser allow_routine_tag_change=true und confirmed_by_asana=true sind gesetzt."
        );
      }

      const toAdd = addTags.filter((tagGid) => !currentTagGids.has(tagGid));
      const toRemove = removeTags.filter((tagGid) => currentTagGids.has(tagGid));

      if (dry_run) {
        return out({
          agent_id,
          dry_run: true,
          task_gid,
          before_task,
          add_tag_gids: addTags,
          remove_tag_gids: removeTags,
          would_add: toAdd,
          would_remove: toRemove,
          skipped_already_present: addTags.filter((tagGid) => currentTagGids.has(tagGid)),
          skipped_not_present: removeTags.filter((tagGid) => !currentTagGids.has(tagGid))
        });
      }

      const add_results = [];
      for (const tagGid of toAdd) {
        const res = await asanaRequestWithRetry(asana, {
          method: "POST",
          url: `/tasks/${task_gid}/addTag`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          data: { data: { tag: tagGid } }
        });
        add_results.push({ tag_gid: tagGid, response: res.data });
      }

      const remove_results = [];
      for (const tagGid of toRemove) {
        const res = await asanaRequestWithRetry(asana, {
          method: "POST",
          url: `/tasks/${task_gid}/removeTag`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          data: { data: { tag: tagGid } }
        });
        remove_results.push({ tag_gid: tagGid, response: res.data });
      }

      let verified_task;
      let verification_status = "not_requested";
      if (verify_after) {
        const verify = await asanaRequestWithRetry(asana, {
          method: "GET",
          url: `/tasks/${task_gid}`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          params: { opt_fields: ASANA_TASK_TAG_OPT_FIELDS }
        });
        verified_task = verify.data.data;
        const verifiedGids = new Set((verified_task.tags || []).map((tag) => tag.gid));
        for (const tagGid of addTags) {
          if (!verifiedGids.has(tagGid)) throw new Error(`Asana-Readback zeigt hinzugefuegten Tag ${tagGid} nicht.`);
        }
        for (const tagGid of removeTags) {
          if (verifiedGids.has(tagGid)) throw new Error(`Asana-Readback zeigt entfernten Tag ${tagGid} weiterhin.`);
        }
        verification_status = "ok";
      }

      return out({
        agent_id,
        task_gid,
        before_task,
        add_results,
        remove_results,
        skipped_already_present: addTags.filter((tagGid) => currentTagGids.has(tagGid)),
        skipped_not_present: removeTags.filter((tagGid) => !currentTagGids.has(tagGid)),
        verified_task,
        verification_status
      });
    }
  );

  server.tool(
    "asana_update_task_recurrence",
    "Streng begrenzter experimenteller Pfad fuer native Asana-Wiederholungen. Native Recurrence ist API-seitig nicht zuverlaessig auslesbar; nutze fuer Produktionsfreigaben asana_verify_task_schedule und nach Abschluss asana_verify_next_routine_instance. Kein 100%-Verifikationspfad.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string(),
      recurrence: z.record(z.string(), z.any()).optional(),
      clear_recurrence: z.boolean().optional().default(false),
      confirmed_by_asana: z.boolean().optional().default(false),
      allow_unverified_native_recurrence_api: z.boolean().optional().default(false),
      dry_run: z.boolean().optional().default(false),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_IDEMPOTENT_SAFE_WRITE,
    async ({
      agent_id,
      task_gid,
      recurrence,
      clear_recurrence,
      confirmed_by_asana,
      allow_unverified_native_recurrence_api,
      dry_run,
      verify_after
    }) => {
      validateAsanaGid(task_gid, "task_gid");
      const setCount = [Boolean(recurrence), Boolean(clear_recurrence)].filter(Boolean).length;
      if (setCount !== 1) {
        throw new Error("Genau eines von recurrence oder clear_recurrence=true muss gesetzt sein.");
      }
      if (!dry_run && !confirmed_by_asana) {
        throw new Error(
          "asana_update_task_recurrence braucht confirmed_by_asana=true, weil Asana-Recurrence API-seitig nicht stabil dokumentiert ist."
        );
      }
      if (!dry_run && !allow_unverified_native_recurrence_api) {
        throw new Error(
          "Native Asana-Wiederholung ist API-seitig nicht 100% verifizierbar. Setze allow_unverified_native_recurrence_api=true nur als bewusste Ausnahme; fuer Standard-Routinen stattdessen UI-verifizieren oder nach Abschluss asana_verify_next_routine_instance nutzen."
        );
      }

      const data = clear_recurrence ? { recurrence: null } : { recurrence };
      const asana = getAsana(agent_id);
      const beforeRes = await asanaRequestWithRetry(asana, {
        method: "GET",
        url: `/tasks/${task_gid}`,
        params: { opt_fields: ASANA_TASK_TAG_WORKSPACE_OPT_FIELDS }
      });
      const before_task = beforeRes.data.data;
      let routine_tag;
      let routine_tag_result;

      if (!clear_recurrence) {
        const hasRoutineTag = (before_task.tags || []).some(
          (tag) => String(tag.name || "").toLowerCase() === "routine"
        );
        if (!hasRoutineTag) {
          const workspaceGid =
            before_task.workspace?.gid ||
            (await asanaRequestWithRetry(asana, { method: "GET", url: "/users/me" })).data.data.workspaces?.[0]?.gid;
          if (!workspaceGid) throw new Error("Kein Asana-Workspace fuer Routine-Tag-Suche gefunden.");
          const tagsRes = await asanaRequestWithRetry(asana, {
            method: "GET",
            url: `/workspaces/${workspaceGid}/tags`,
            params: { limit: 100, opt_fields: "gid,name,color,workspace.gid" }
          });
          routine_tag = (tagsRes.data.data || []).find(
            (tag) => String(tag.name || "").toLowerCase() === "routine"
          );
          if (!routine_tag) {
            throw new Error(
              "Wiederkehrende Aufgaben muessen den Tag Routine haben, aber im Workspace wurde kein Tag mit Name Routine gefunden."
            );
          }
        }
      }

      if (dry_run) {
        return out({
          agent_id,
          dry_run: true,
          experimental: true,
          production_ready_claim_allowed: false,
          task_gid,
          data,
          before_task,
          routine_tag,
          would_add_routine_tag: Boolean(routine_tag),
          note: "Recurrence ist in Asanas oeffentlicher API nicht stabil dokumentiert; Live-Call kann scheitern."
        });
      }

      if (routine_tag) {
        const tagRes = await asanaRequestWithRetry(asana, {
          method: "POST",
          url: `/tasks/${task_gid}/addTag`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          data: { data: { tag: routine_tag.gid } }
        });
        routine_tag_result = tagRes.data;
      }

      const res = await asanaRequestWithRetry(asana, {
        method: "PUT",
        url: `/tasks/${task_gid}`,
        timeout: ASANA_WRITE_TIMEOUT_MS,
        data: { data },
        params: { opt_fields: ASANA_TASK_SCHEDULE_OPT_FIELDS }
      });

      let verified_task;
      let verification_status = "api_response_only";
      let verification_note =
        "Asana gibt Recurrence nicht verlaesslich als dokumentiertes Feld zurueck; pruefe bei kritischen Aenderungen zusaetzlich im Asana-UI.";
      if (verify_after) {
        const verify = await asanaRequestWithRetry(asana, {
          method: "GET",
          url: `/tasks/${task_gid}`,
          timeout: ASANA_WRITE_TIMEOUT_MS,
          params: { opt_fields: ASANA_TASK_SCHEDULE_OPT_FIELDS }
        });
        verified_task = verify.data.data;
      }

      return out({
        agent_id,
        task_gid,
        experimental: true,
        production_ready_claim_allowed: false,
        data,
        before_task,
        routine_tag,
        routine_tag_result,
        task: res.data.data,
        verified_task,
        verification_status,
        verification_note
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

        const hasBody = data && typeof data === "object" && Object.keys(data).length > 0;
        const allowBody = method === "POST" || method === "PUT";

        const requestConfig = {
          method,
          url: path,
          params,
          // Asana kann GET/DELETE mit Body ablehnen; Body daher nur fuer POST/PUT und nur wenn nicht-leer.
          data: allowBody && hasBody ? { data } : undefined
        };
        const res = method === "GET" ? await asanaRequestWithRetry(asana, requestConfig) : await asana.request(requestConfig);

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
      const { attachments, selectedAttachmentGid } = await resolveAsanaAttachmentSelection(asana, {
        taskGid: task_gid,
        attachmentGid: attachment_gid,
        attachmentNameContains: attachment_name_contains,
        downloadFirstMatch: download_first_match
      });

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
    "asana_attachment_fetch_chunked",
    "Liest Asana-Anhaenge read-only in kleinen Base64-Chunks, damit Tool-Output-Limits den Inhalt nicht kuerzen. Standard fuer Screenshots, PDFs, ZIPs oder andere binaere Anhaenge, wenn asana_attachment_fetch zu gross ist.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string().optional(),
      attachment_gid: z.string().optional(),
      attachment_name_contains: z.string().optional(),
      download_first_match: z.boolean().optional().default(false),
      chunk_index: z.number().int().min(0).optional().default(0),
      chunk_size: z
        .number()
        .int()
        .min(512)
        .max(ASANA_ATTACHMENT_MAX_CHUNK_BYTES)
        .optional()
        .default(ASANA_ATTACHMENT_DEFAULT_CHUNK_BYTES),
      include_text_preview: z.boolean().optional().default(true),
      text_preview_chars: z.number().int().min(100).max(20000).optional().default(4000),
      max_bytes: z
        .number()
        .int()
        .min(1024)
        .max(ASANA_ATTACHMENT_HARD_MAX_BYTES)
        .optional()
        .default(ASANA_ATTACHMENT_HARD_MAX_BYTES)
    },
    TOOL_EXTERNAL_READ,
    async ({
      agent_id,
      task_gid,
      attachment_gid,
      attachment_name_contains,
      download_first_match,
      chunk_index,
      chunk_size,
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
      const { attachments, selectedAttachmentGid } = await resolveAsanaAttachmentSelection(asana, {
        taskGid: task_gid,
        attachmentGid: attachment_gid,
        attachmentNameContains: attachment_name_contains,
        downloadFirstMatch: download_first_match
      });

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
      const totalChunks = Math.max(1, Math.ceil(bytes.length / chunk_size));
      if (chunk_index >= totalChunks) {
        throw new Error(`chunk_index ${chunk_index} liegt ausserhalb der verfuegbaren Chunks 0..${totalChunks - 1}.`);
      }
      const start = chunk_index * chunk_size;
      const end = Math.min(start + chunk_size, bytes.length);
      const chunk = bytes.subarray(start, end);
      const sha256 = createHash("sha256").update(bytes).digest("hex");
      const chunkSha256 = createHash("sha256").update(chunk).digest("hex");
      const textPreview =
        include_text_preview && chunk_index === 0 && isTextLikeAttachment(content_type, attachment.name)
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
        chunk_index,
        chunk_size,
        chunk_start: start,
        chunk_end_exclusive: end,
        chunk_byte_length: chunk.length,
        chunk_sha256: chunkSha256,
        total_chunks: totalChunks,
        next_chunk_index: chunk_index + 1 < totalChunks ? chunk_index + 1 : null,
        content_base64_chunk: chunk.toString("base64"),
        text_preview: textPreview,
        reconstruct_hint:
          "Alle content_base64_chunk-Werte in Reihenfolge chunk_index konkatenieren und base64-dekodieren; sha256 gegen den Gesamtwert pruefen."
      });
    }
  );

  server.tool(
    "asana_attachment_copy_to_drive",
    "Kopiert einen Asana-Anhang serverseitig in den erlaubten Google-Drive-Agentenordner und gibt Drive-Link/ID zurueck. Standard, wenn lokale DNS-Downloads blockieren oder binaere Anhaenge nicht durch Tool-Output passen.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string().optional(),
      attachment_gid: z.string().optional(),
      attachment_name_contains: z.string().optional(),
      download_first_match: z.boolean().optional().default(false),
      title: z.string().optional(),
      target_folder_id: z.string().optional().default(GOOGLE_AGENT_FOLDER_ID),
      max_bytes: z
        .number()
        .int()
        .min(1024)
        .max(ASANA_ATTACHMENT_HARD_MAX_BYTES)
        .optional()
        .default(ASANA_ATTACHMENT_HARD_MAX_BYTES),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional(),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_SAFE_WRITE,
    async ({
      agent_id,
      task_gid,
      attachment_gid,
      attachment_name_contains,
      download_first_match,
      title,
      target_folder_id,
      max_bytes,
      dry_run,
      confirmed_by_asana,
      asana_task_gid,
      verify_after
    }) => {
      if (!task_gid && !attachment_gid) {
        throw new Error("task_gid oder attachment_gid ist erforderlich.");
      }
      if (task_gid) validateAsanaGid(task_gid, "task_gid");
      if (attachment_gid) validateAsanaGid(attachment_gid, "attachment_gid");
      if (asana_task_gid) validateAsanaGid(asana_task_gid, "asana_task_gid");
      await assertAllowedGoogleFolder(target_folder_id, { agent_id });

      const asana = getAsana(agent_id);
      const { attachments, selectedAttachmentGid } = await resolveAsanaAttachmentSelection(asana, {
        taskGid: task_gid,
        attachmentGid: attachment_gid,
        attachmentNameContains: attachment_name_contains,
        downloadFirstMatch: download_first_match
      });

      if (!selectedAttachmentGid) {
        return out({
          agent_id,
          task_gid,
          attachment_name_contains: attachment_name_contains || null,
          returned_count: attachments.length,
          attachments,
          downloaded: false,
          copied: false,
          next_step:
            "Mit attachment_gid erneut aufrufen oder download_first_match=true setzen, wenn der erste Treffer bewusst kopiert werden soll."
        });
      }

      const { attachment, bytes, content_type, content_length_header } = await downloadAsanaAttachment(
        asana,
        selectedAttachmentGid,
        max_bytes
      );
      const sha256 = createHash("sha256").update(bytes).digest("hex");
      const mimeType = String(content_type || "application/octet-stream").split(";")[0].trim() || "application/octet-stream";
      const fileName = title?.trim() || attachment.name || `asana-attachment-${attachment.gid}`;

      if (dry_run) {
        return out({
          agent_id,
          dry_run: true,
          task_gid: task_gid || attachment.parent?.gid || null,
          attachment,
          target_folder_id,
          planned_file_name: fileName,
          content_type,
          content_length_header,
          byte_length: bytes.length,
          sha256,
          requires_for_live: ["dry_run=false", "confirmed_by_asana=true", "asana_task_gid"]
        });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "asana_attachment_copy_to_drive");

      const uploaded = await uploadBufferToDrive({
        name: fileName,
        mimeType,
        targetFolderId: target_folder_id,
        bytes,
        googleContext: { agent_id }
      });
      const verifiedFile = verify_after
        ? await getDriveFile(uploaded.id, "id,name,mimeType,parents,webViewLink,webContentLink", { agent_id })
        : undefined;
      const verificationStatus =
        !verify_after || verifiedFile?.parents?.includes(target_folder_id) ? "verified" : "folder_readback_mismatch";

      return out({
        agent_id,
        dry_run: false,
        asana_task_gid,
        task_gid: task_gid || attachment.parent?.gid || null,
        attachment,
        target_folder_id,
        content_type,
        content_length_header,
        byte_length: bytes.length,
        sha256,
        copied_file: uploaded,
        verified_file: verifiedFile,
        verification_status: verificationStatus
      });
    }
  );

  server.tool(
    "accounting_drive_pdf_invoice_totals",
    "Accounting-Spezialpfad: liest PDF-Rechnungen in einem freigegebenen Accounting-Drive-Ordner read-only, extrahiert nur Netto/MwSt./Brutto und gibt keine vollstaendigen Rechnungstexte aus.",
    {
      agent_id: z.literal("vip-ai-accounting").optional().default("vip-ai-accounting"),
      folder_id: z.string(),
      expected_month_key: z.string().regex(/^\d{2}\/\d{2}$/).optional(),
      max_files: z.number().int().positive().max(300).optional().default(200),
      max_pdf_bytes: z
        .number()
        .int()
        .min(1024)
        .max(ACCOUNTING_ATTACHMENT_HARD_MAX_BYTES)
        .optional()
        .default(ACCOUNTING_ATTACHMENT_HARD_MAX_BYTES),
      include_file_details: z.boolean().optional().default(true),
      include_text_debug_lines: z.boolean().optional().default(false),
      text_debug_line_limit: z.number().int().positive().max(80).optional().default(30)
    },
    TOOL_READ_ONLY,
    async ({
      agent_id,
      folder_id,
      expected_month_key,
      max_files,
      max_pdf_bytes,
      include_file_details,
      include_text_debug_lines,
      text_debug_line_limit
    }) => {
      const googleContext = { agent_id };
      await assertAllowedAccountingFolder(folder_id, googleContext);
      const files = await listAccountingDriveFolderPdfFiles(folder_id, max_files, googleContext);
      const details = [];
      const totals = { net_amount: 0, vat_amount: 0, gross_amount: 0 };
      let extractedCount = 0;
      let partialCount = 0;
      let notExtractedCount = 0;
      let monthMismatchCount = 0;

      for (const file of files) {
        const fileSummary = {
          id: file.id,
          name: file.name,
          mimeType: file.mimeType,
          size: file.size ? Number(file.size) : null,
          createdTime: file.createdTime,
          modifiedTime: file.modifiedTime,
          webViewLink: file.webViewLink
        };

        try {
          if (file.size && Number(file.size) > max_pdf_bytes) {
            notExtractedCount += 1;
            details.push({
              ...fileSummary,
              status: "blocked",
              reason: "pdf_too_large",
              max_pdf_bytes
            });
            continue;
          }

          const buffer = await downloadDrivePdfBuffer(file.id, max_pdf_bytes, googleContext);
          const parser = new PDFParse({ data: buffer });
          let parsed;
          try {
            parsed = await parser.getText();
          } finally {
            await parser.destroy().catch(() => {});
          }
          const pdfText = parsed.text || "";
          const extracted = extractInvoiceAmountsFromPdfText(pdfText);
          const monthMismatch = Boolean(
            expected_month_key && extracted.month_key && extracted.month_key !== expected_month_key
          );

          if (extracted.status === "extracted") extractedCount += 1;
          else if (extracted.status === "partial") partialCount += 1;
          else notExtractedCount += 1;
          if (monthMismatch) monthMismatchCount += 1;

          if (extracted.net_amount !== null) totals.net_amount += extracted.net_amount;
          if (extracted.vat_amount !== null) totals.vat_amount += extracted.vat_amount;
          if (extracted.gross_amount !== null) totals.gross_amount += extracted.gross_amount;

          details.push({
            ...fileSummary,
            ...extracted,
            month_mismatch: monthMismatch,
            text_debug_lines: include_text_debug_lines
              ? getLimitedPdfTextDebugLines(pdfText, text_debug_line_limit)
              : undefined
          });
        } catch (error) {
          notExtractedCount += 1;
          details.push({
            ...fileSummary,
            status: "error",
            error: String(error?.message || error)
          });
        }
      }

      const round = (value) => Math.round(value * 100) / 100;
      return out({
        agent_id,
        folder_id,
        expected_month_key: expected_month_key || null,
        pdf_count: files.length,
        extracted_count: extractedCount,
        partial_count: partialCount,
        not_extracted_count: notExtractedCount,
        month_mismatch_count: monthMismatchCount,
        totals: {
          net_amount: round(totals.net_amount),
          vat_amount: round(totals.vat_amount),
          gross_amount: round(totals.gross_amount)
        },
        files: include_file_details ? details : undefined
      });
    }
  );

  server.tool(
    "accounting_drive_invoice_metadata_delta",
    "Accounting-Spezialpfad: prueft freigegebene Accounting-Drive-Ordner read-only und leichtgewichtig nur per Dateimetadaten auf neue, geaenderte oder fehlende Rechnungs-PDFs/Bilder. Laedt keine Dateien herunter.",
    {
      agent_id: z.literal("vip-ai-accounting").optional().default("vip-ai-accounting"),
      folders: z
        .array(
          z.object({
            folder_id: z.string(),
            folder_label: z.string().optional(),
            expected_month_key: z.string().regex(/^\d{2}\/\d{2}$/).optional()
          })
        )
        .min(1)
        .max(24),
      known_files: z
        .array(
          z.object({
            id: z.string(),
            folder_id: z.string().optional(),
            folder_label: z.string().optional(),
            name: z.string().optional(),
            mimeType: z.string().optional(),
            size: z.union([z.string(), z.number()]).optional().nullable(),
            md5Checksum: z.string().optional().nullable(),
            modifiedTime: z.string().optional().nullable(),
            vip_sha256: z.string().optional().nullable()
          })
        )
        .optional()
        .default([]),
      max_files_per_folder: z.number().int().positive().max(500).optional().default(300),
      include_unchanged_files: z.boolean().optional().default(false),
      include_current_snapshot: z.boolean().optional().default(true)
    },
    TOOL_READ_ONLY,
    async ({
      agent_id,
      folders,
      known_files,
      max_files_per_folder,
      include_unchanged_files,
      include_current_snapshot
    }) => {
      const googleContext = { agent_id };
      const requestedFolderIds = new Set(folders.map((folder) => folder.folder_id));
      const currentFiles = [];
      const folderSummaries = [];

      for (const folder of folders) {
        await assertAllowedAccountingFolder(folder.folder_id, googleContext);
        const files = await listAccountingDriveFolderInvoiceFiles(
          folder.folder_id,
          max_files_per_folder,
          googleContext
        );
        const publicFiles = files.map((file) =>
          publicAccountingDriveMetadata(file, folder.folder_id, folder.folder_label || folder.expected_month_key || null)
        );
        currentFiles.push(...publicFiles);
        folderSummaries.push({
          folder_id: folder.folder_id,
          folder_label: folder.folder_label || null,
          expected_month_key: folder.expected_month_key || null,
          invoice_candidate_count: publicFiles.length,
          newest_modifiedTime: publicFiles
            .map((file) => file.modifiedTime)
            .filter(Boolean)
            .sort()
            .at(-1) || null
        });
      }

      const knownById = new Map(
        known_files
          .map(normalizeAccountingKnownFile)
          .filter((file) => file.id)
          .map((file) => [file.id, file])
      );
      const currentById = new Map(currentFiles.map((file) => [file.id, normalizeAccountingKnownFile(file)]));
      const newFiles = [];
      const changedFiles = [];
      const unchangedFiles = [];
      const missingFiles = [];

      for (const currentFile of currentFiles) {
        const normalizedCurrent = normalizeAccountingKnownFile(currentFile);
        const previous = knownById.get(currentFile.id);
        if (!previous) {
          newFiles.push(currentFile);
          continue;
        }

        const changed_fields = compareAccountingDriveMetadata(previous, normalizedCurrent);
        if (changed_fields.length) {
          changedFiles.push({
            ...currentFile,
            previous,
            changed_fields
          });
        } else if (include_unchanged_files) {
          unchangedFiles.push(currentFile);
        }
      }

      for (const previous of knownById.values()) {
        const relevantFolder =
          !previous.folder_id || requestedFolderIds.has(previous.folder_id) || folders.length === 1;
        if (relevantFolder && !currentById.has(previous.id)) {
          missingFiles.push({
            ...previous,
            status: "missing_from_current_folder",
            note: "Datei wurde nicht mehr in den abgefragten Accounting-Ordnern gefunden; moeglich sind Loeschung, Papierkorb oder Verschiebung."
          });
        }
      }

      const needsReconcile = Boolean(newFiles.length || changedFiles.length || missingFiles.length);
      return out({
        agent_id,
        mode: "accounting_drive_invoice_metadata_delta",
        read_only: true,
        folders: folderSummaries,
        known_files_count: known_files.length,
        current_files_count: currentFiles.length,
        delta: {
          new_count: newFiles.length,
          changed_count: changedFiles.length,
          missing_count: missingFiles.length,
          unchanged_count: currentFiles.length - newFiles.length - changedFiles.length,
          new_files: newFiles,
          changed_files: changedFiles,
          missing_files: missingFiles,
          unchanged_files: include_unchanged_files ? unchangedFiles : undefined
        },
        current_snapshot: include_current_snapshot ? currentFiles : undefined,
        needs_reconcile: needsReconcile,
        next_step: needsReconcile
          ? "Nur neue/geaenderte/fehlende Dateien fachlich pruefen, betroffene PDF/Bild-Rechnungen neu auswerten und Sheet/Register abgleichen."
          : "Keine Metadaten-Aenderung gefunden; schwere PDF-Auswertung und Sheet-Betragskorrektur koennen uebersprungen werden."
      });
    }
  );

  server.tool(
    "accounting_mail_scan_upload_attachment",
    "Accounting-Spezialpfad: liest neue oder noch im Postfach liegende Rechnungen serverseitig, akzeptiert echte Rechnungs-PDFs/Bilder und erkannte Inline-Rechnungen wie Apple, rendert Inline-Rechnungen bevorzugt als visuelle HTML-Mail-PDFs mit Layout/Styles/Bildern und nutzt nur bei fehlender Browser-Runtime ein Text-PDF-Fallback, prueft optional eine Absender-Allowlist, scannt optional per ClamAV INSTREAM falls verfuegbar und laedt Dateien direkt in freigegebene Accounting-Drive-Ordner. Keine lokalen Zwischenkopien. Fuer Routinen process_all_matching=true nutzen; limit ist dann nur die interne Batchgroesse, kein Gesamtlimit.",
    {
      agent_id: z.literal("vip-ai-accounting").optional().default("vip-ai-accounting"),
      target_folder_id: z.string(),
      sender_allowlist: z.array(z.string()).optional().default([]),
      include_moritz_allowlist: z.boolean().optional().default(true),
      enforce_sender_allowlist: z.boolean().optional().default(true),
      invoice_only: z.boolean().optional().default(true),
      mail_search_mode: z.enum(["unseen", "inbox_all"]).optional().default("unseen"),
      uid: z.string().optional(),
      attachment_name_contains: z.string().optional(),
      download_first_match: z.boolean().optional().default(false),
      upload_all_matching: z.boolean().optional().default(false),
      process_all_matching: z.boolean().optional().default(false),
      limit: z.number().int().min(1).max(100).optional().default(25),
      max_email_bytes: z
        .number()
        .int()
        .min(20 * 1024)
        .max(ACCOUNTING_EMAIL_HARD_MAX_BYTES)
        .optional()
        .default(ACCOUNTING_EMAIL_HARD_MAX_BYTES),
      max_attachment_bytes: z
        .number()
        .int()
        .min(1024)
        .max(ACCOUNTING_ATTACHMENT_HARD_MAX_BYTES)
        .optional()
        .default(ACCOUNTING_ATTACHMENT_HARD_MAX_BYTES),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional(),
      approved_by: z.string().optional(),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_SAFE_WRITE,
    async ({
      agent_id,
      target_folder_id,
      sender_allowlist,
      include_moritz_allowlist,
      enforce_sender_allowlist,
      invoice_only,
      mail_search_mode,
      uid,
      attachment_name_contains,
      download_first_match,
      upload_all_matching,
      process_all_matching,
      limit,
      max_email_bytes,
      max_attachment_bytes,
      dry_run,
      confirmed_by_asana,
      asana_task_gid,
      approved_by,
      verify_after
    }) => {
      if (asana_task_gid) validateAsanaGid(asana_task_gid, "asana_task_gid");
      const googleContext = { agent_id };
      await assertAllowedAccountingFolder(target_folder_id, googleContext);
      if (!dry_run) {
        assertMoritzAsanaConfirmed({
          confirmedByAsana: confirmed_by_asana,
          asanaTaskGid: asana_task_gid,
          approvedBy: approved_by,
          actionName: "accounting_mail_scan_upload_attachment"
        });
      }

      const allowlist = normalizeEmailAllowlist(sender_allowlist);
      if (enforce_sender_allowlist && include_moritz_allowlist) {
        for (const email of ACCOUNTING_MORITZ_EMAILS) allowlist.add(email);
      }
      if (enforce_sender_allowlist && !allowlist.size) {
        throw new Error("sender_allowlist ist leer. Fuer Accounting-Mailanhaenge braucht es eine Moritz-Allowlist.");
      }

      const { configs, summary } = getImapConfigCandidates(agent_id, { requireCredentials: true });
      const mailResult = await fetchUnseenRawEmailWithFallback(configs, {
        limit,
        maxEmailBytes: max_email_bytes,
        mailSearchMode: mail_search_mode,
        fetchAllMatching: process_all_matching && !uid,
        specificUid: uid
      });
      const processed = [];

      for (const message of mailResult.messages) {
        if (uid && message.uid !== uid) continue;
        if (message.parse_error) {
          processed.push({ uid: message.uid, status: "blocked", reason: message.parse_error });
          continue;
        }

        const senderAllowed = !enforce_sender_allowlist || allowlist.has(message.from_email);
        const baseMessage = {
          uid: message.uid,
          from_email: message.from_email || null,
          date: message.date || null,
          message_id_hash: message.message_id_hash || null
        };
        if (!senderAllowed) {
          processed.push({
            ...baseMessage,
            status: "skipped",
            reason: "sender_not_allowlisted",
            sender_allowlist_enforced: true,
            attachment_count: message.attachments.length
          });
          continue;
        }

        let candidates = message.attachments;
        if (attachment_name_contains) {
          const needle = attachment_name_contains.toLowerCase();
          candidates = candidates.filter((attachment) => attachment.filename.toLowerCase().includes(needle));
        }

        if (!candidates.length) {
          processed.push({
            ...baseMessage,
            status: "skipped",
            reason: "no_matching_pdf_or_image_attachment",
            attachment_count: message.attachments.length
          });
          continue;
        }

        let selected = candidates;
        if (!upload_all_matching) {
          if (candidates.length > 1 && !download_first_match) {
            processed.push({
              ...baseMessage,
              status: "selection_required",
              reason: "multiple_matching_attachments",
              attachments: candidates.map(publicAccountingAttachment),
              next_step:
                "Mit attachment_name_contains weiter eingrenzen, download_first_match=true setzen oder upload_all_matching=true bewusst nutzen."
            });
            continue;
          }
          selected = [candidates[0]];
        }

        for (const attachment of selected) {
          const sha256 = createHash("sha256").update(attachment.bytes).digest("hex");
          const attachmentSummary = publicAccountingAttachment(attachment);
          if (attachment.bytes.length > max_attachment_bytes) {
            processed.push({
              ...baseMessage,
              status: "blocked",
              reason: "attachment_too_large",
              attachment: attachmentSummary,
              max_attachment_bytes
            });
            continue;
          }

          let invoiceClassification = null;
          if (invoice_only) {
            try {
              invoiceClassification = await classifyAccountingAttachmentForUpload(attachment, message);
            } catch (error) {
              processed.push({
                ...baseMessage,
                status: "blocked",
                reason: "invoice_classification_failed",
                attachment: attachmentSummary,
                error: String(error?.message || error)
              });
              continue;
            }
            if (invoiceClassification.decision !== "allow") {
              processed.push({
                ...baseMessage,
                status: "skipped",
                reason: "skipped_non_invoice_attachment",
                attachment: attachmentSummary,
                invoice_classification: invoiceClassification
              });
              continue;
            }
          }

          let scan;
          try {
            scan = await scanBufferWithClamd(attachment.bytes);
          } catch (error) {
            scan = {
              status: "error",
              clean: false,
              scanner: "clamd",
              error: String(error?.message || error)
            };
          }
          const scanBlocksUpload = scan.status === "not_clean";
          const scanWasSkipped = scan.status === "not_configured" || scan.status === "error";

          const duplicateFiles = await findAccountingDriveDuplicate({
            folderId: target_folder_id,
            sha256,
            fileName: attachment.filename
          }, googleContext);
          const duplicateSummary = duplicateFiles.map((file) => ({
            id: file.id,
            name: file.name,
            webViewLink: file.webViewLink,
            matched_by_hash: file.appProperties?.vip_sha256 === sha256,
            matched_by_name: file.name === attachment.filename
          }));

          if (dry_run) {
            processed.push({
              ...baseMessage,
              status: !scanBlocksUpload && duplicateFiles.length === 0 ? "ready_for_upload" : "blocked_or_duplicate",
              dry_run: true,
              target_folder_id,
              sender_allowlist_enforced: enforce_sender_allowlist,
              attachment: attachmentSummary,
              invoice_classification: invoiceClassification,
              scan,
              virus_scan_required: false,
              virus_scan_available: !scanWasSkipped,
              virus_scan_note: scanWasSkipped
                ? "ClamAV ist optional und aktuell nicht verfuegbar; Upload waere ohne Virenscan erlaubt."
                : undefined,
              duplicates: duplicateSummary
            });
            continue;
          }

          if (scanBlocksUpload) {
            processed.push({
              ...baseMessage,
              status: "blocked",
              reason: "virus_scan_not_clean",
              attachment: attachmentSummary,
              scan
            });
            continue;
          }

          if (duplicateFiles.length) {
            processed.push({
              ...baseMessage,
              status: "skipped_duplicate",
              attachment: attachmentSummary,
              duplicates: duplicateSummary
            });
            continue;
          }

          const uploaded = await uploadBufferToDrive({
            name: attachment.filename,
            mimeType: inferAccountingMimeType(attachment),
            targetFolderId: target_folder_id,
            bytes: attachment.bytes,
            assertFolder: assertAllowedAccountingFolder,
            googleContext,
            appProperties: {
              vip_source: "accounting_mail",
              vip_agent_id: agent_id,
              vip_sha256: sha256,
              vip_imap_uid: compactDriveAppProperty(message.uid),
              vip_message_hash: compactDriveAppProperty(message.message_id_hash),
              vip_sender_hash: compactDriveAppProperty(
                message.from_email ? createHash("sha256").update(message.from_email).digest("hex") : ""
              ),
              vip_original_name: compactDriveAppProperty(attachment.filename),
              vip_inline_invoice: attachment.generated_from_inline_invoice ? "true" : "false",
              vip_inline_vendor: compactDriveAppProperty(attachment.inline_invoice_vendor || "")
            }
          });
          const verifiedFile = verify_after
            ? await getDriveFile(
                uploaded.id,
                "id,name,mimeType,parents,webViewLink,webContentLink,md5Checksum,appProperties,createdTime,modifiedTime",
                googleContext
              )
            : undefined;
          const verificationStatus =
            !verify_after || verifiedFile?.parents?.includes(target_folder_id)
              ? "verified"
              : "folder_readback_mismatch";

          processed.push({
            ...baseMessage,
            status: "uploaded",
            dry_run: false,
            asana_task_gid,
            approved_by,
            target_folder_id,
            sender_allowlist_enforced: enforce_sender_allowlist,
            attachment: attachmentSummary,
            invoice_classification: invoiceClassification,
            scan,
            virus_scan_required: false,
            virus_scan_available: !scanWasSkipped,
            virus_scan_note: scanWasSkipped
              ? "ClamAV war nicht verfuegbar; Datei wurde gemaess optionalem Scan-Modus ohne Virenscan verarbeitet."
              : undefined,
            uploaded_file: uploaded,
            verified_file: verifiedFile,
            verification_status: verificationStatus
          });
        }
      }

      return out({
        agent_id,
        dry_run,
        ...summary,
        mode: "accounting_mail_scan_upload_attachment",
        connection: {
          host: mailResult.host,
          port: mailResult.port,
          secure: mailResult.secure,
          label: mailResult.label
        },
        target_folder_id,
        sender_allowlist_enforced: enforce_sender_allowlist,
        invoice_only,
        mail_search_mode,
        process_all_matching,
        batch_size: mailResult.batch_size,
        batch_count: mailResult.batch_count,
        matching_count: mailResult.matching_count,
        unseen_count: mailResult.unseen_count,
        returned_count: mailResult.returned_count,
        processed_count: processed.length,
        processed,
        imap_attempts: mailResult.attempts,
        requires_for_live: dry_run
          ? ["dry_run=false", "confirmed_by_asana=true", "asana_task_gid", "approved_by=Moritz Feichtmeyer"]
          : undefined
      });
    }
  );

  server.tool(
    "accounting_email_archive_processed",
    "Accounting-Spezialpfad: archiviert eine konkrete verarbeitete Accounting-Mail erst nach Drive-Ablage und verifiziertem Sheet-Readback. Verschiebt per IMAP UID MOVE in einen bestehenden Archivordner.",
    {
      agent_id: z.literal("vip-ai-accounting").optional().default("vip-ai-accounting"),
      uid: z.string().regex(/^\d+$/),
      archive_mailbox: z.string().optional(),
      create_archive_mailbox: z.boolean().optional().default(false),
      expected_from_email: z.string().optional(),
      expected_message_id_hash: z.string().optional(),
      drive_file_id: z.string().optional(),
      target_folder_id: z.string().optional(),
      sheet_verified: z.boolean().optional().default(false),
      sheet_range: z.string().optional(),
      dry_run: z.boolean().optional().default(true),
      confirmed_by_asana: z.boolean().optional().default(false),
      asana_task_gid: z.string().optional(),
      approved_by: z.string().optional(),
      verify_after: z.boolean().optional().default(true)
    },
    TOOL_SAFE_WRITE,
    async ({
      agent_id,
      uid,
      archive_mailbox,
      create_archive_mailbox,
      expected_from_email,
      expected_message_id_hash,
      drive_file_id,
      target_folder_id,
      sheet_verified,
      sheet_range,
      dry_run,
      confirmed_by_asana,
      asana_task_gid,
      approved_by,
      verify_after
    }) => {
      if (asana_task_gid) validateAsanaGid(asana_task_gid, "asana_task_gid");
      const googleContext = { agent_id };
      let verifiedDriveFile = null;

      if (target_folder_id) {
        await assertAllowedAccountingFolder(target_folder_id, googleContext);
      }
      if (!dry_run) {
        assertMoritzAsanaConfirmed({
          confirmedByAsana: confirmed_by_asana,
          asanaTaskGid: asana_task_gid,
          approvedBy: approved_by,
          actionName: "accounting_email_archive_processed"
        });
        if (!sheet_verified) {
          throw new Error("accounting_email_archive_processed braucht sheet_verified=true nach erfolgreichem Sheet-Readback.");
        }
        if (!drive_file_id) {
          throw new Error("accounting_email_archive_processed braucht drive_file_id der abgelegten Rechnung.");
        }
        if (create_archive_mailbox && !archive_mailbox) {
          throw new Error("create_archive_mailbox=true braucht archive_mailbox.");
        }
      }

      if (drive_file_id) {
        verifiedDriveFile = await getDriveFile(
          drive_file_id,
          "id,name,mimeType,parents,webViewLink,appProperties,createdTime,modifiedTime",
          googleContext
        );
        if (target_folder_id && !verifiedDriveFile.parents?.includes(target_folder_id)) {
          throw new Error(`Drive-Datei ${drive_file_id} liegt nicht im erwarteten Zielordner ${target_folder_id}.`);
        }
      }

      const { configs, summary } = getImapConfigCandidates(agent_id, { requireCredentials: true });
      const archiveResult = await archiveImapUidWithFallback(configs, {
        uid,
        archiveMailbox: archive_mailbox,
        createArchiveMailbox: create_archive_mailbox,
        expectedFromEmail: expected_from_email,
        expectedMessageIdHash: expected_message_id_hash,
        dryRun: dry_run,
        verifyAfter: verify_after
      });

      return out({
        agent_id,
        dry_run,
        ...summary,
        mode: "accounting_email_archive_processed",
        connection: {
          host: archiveResult.host,
          port: archiveResult.port,
          secure: archiveResult.secure,
          label: archiveResult.label
        },
        uid,
        create_archive_mailbox,
        asana_task_gid: dry_run ? undefined : asana_task_gid,
        approved_by: dry_run ? undefined : approved_by,
        sheet_verified,
        sheet_range: sheet_range || null,
        drive_file: verifiedDriveFile,
        result: archiveResult,
        imap_attempts: archiveResult.attempts,
        requires_for_live: dry_run
          ? [
              "dry_run=false",
              "confirmed_by_asana=true",
              "asana_task_gid",
              "approved_by=Moritz Feichtmeyer",
              "sheet_verified=true",
              "drive_file_id",
              "create_archive_mailbox=true nur mit archive_mailbox"
            ]
          : undefined
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
    "buffer_check_config",
    "Prueft Buffer- und Cloudinary-Konfiguration fuer ein Projekt ohne Secret-Werte auszugeben. Optional werden Organizations und Channels read-only aus Buffer gelesen.",
    {
      agent_id: agentIdSchema,
      project_key: z.string().min(2).max(80).optional().default("holzpunkt"),
      fetch_organizations: z.boolean().optional().default(false),
      fetch_channels: z.boolean().optional().default(false)
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, project_key, fetch_organizations, fetch_channels }) => {
      const bufferDetails = getBufferProjectConfigDetails(project_key, {
        requireApiKey: fetch_organizations || fetch_channels,
        requireOrganizationId: fetch_channels
      });
      const cloudinaryDetails = getCloudinaryConfigDetails({ requireCredentials: false });
      const configuredChannels = getBufferConfiguredChannels(project_key);
      const result = {
        agent_id,
        ...bufferDetails.summary,
        cloudinary: cloudinaryDetails.summary,
        channels: configuredChannels,
        fetch_organizations,
        fetch_channels
      };

      if (fetch_organizations) {
        result.organizations = await fetchBufferOrganizations(project_key);
      }
      if (fetch_channels) {
        const organizationId = bufferDetails.config.organizationId;
        result.buffer_channels = await fetchBufferChannels(project_key, organizationId);
      }

      return out(result);
    }
  );

  server.tool(
    "cloudinary_cleanup_old_uploads",
    "Raeumt alte Social-Media-Uploads aus dem eng begrenzten Cloudinary-Projektprefix auf. Standard ist dry_run=true; echte Loeschungen brauchen confirmed_cleanup=true.",
    {
      agent_id: agentIdSchema,
      project_key: z.string().min(2).max(80).optional().default("holzpunkt"),
      target_prefix: z.string().min(6).max(240).optional(),
      older_than_days: z.number().int().min(7).max(365).optional().default(30),
      max_scan: z.number().int().min(1).max(2000).optional().default(500),
      max_delete: z.number().int().min(0).max(500).optional().default(100),
      dry_run: z.boolean().optional().default(true),
      confirmed_cleanup: z.boolean().optional().default(false),
      invalidate_cdn: z.boolean().optional().default(false)
    },
    TOOL_EXTERNAL_WRITE,
    async ({
      agent_id,
      project_key,
      target_prefix,
      older_than_days,
      max_scan,
      max_delete,
      dry_run,
      confirmed_cleanup,
      invalidate_cdn
    }) => {
      if (!dry_run && !confirmed_cleanup) {
        throw new Error("cloudinary_cleanup_old_uploads braucht confirmed_cleanup=true fuer Live-Loeschung.");
      }

      const normalizedProjectKey = normalizeBufferProjectKey(project_key);
      const { config, summary } = getCloudinaryConfigDetails({ requireCredentials: true });
      const prefix = normalizeCloudinaryCleanupPrefix({
        folderPrefix: config.folderPrefix,
        projectKey: normalizedProjectKey,
        targetPrefix: target_prefix
      });
      const cutoff = new Date(Date.now() - older_than_days * 24 * 60 * 60 * 1000);
      const { resources, next_cursor } = await listCloudinaryResourcesByPrefix({
        config,
        prefix,
        maxResources: max_scan
      });

      const expired = [];
      const recent = [];
      const unparseable = [];
      for (const resource of resources) {
        const createdAt = new Date(resource.created_at);
        if (Number.isNaN(createdAt.getTime())) {
          unparseable.push(resource);
        } else if (createdAt.getTime() < cutoff.getTime()) {
          expired.push(resource);
        } else {
          recent.push(resource);
        }
      }

      expired.sort((a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime());
      const selected = expired.slice(0, max_delete);
      const publicIds = selected.map((resource) => resource.public_id).filter(Boolean);
      const deleteResults = !dry_run && publicIds.length
        ? await deleteCloudinaryResourcesByPublicId({
            config,
            publicIds,
            invalidate: invalidate_cdn
          })
        : [];

      const deletedCount = deleteResults.reduce((sum, result) => {
        const deleted = result?.deleted || {};
        return sum + Object.values(deleted).filter((status) => status === "deleted").length;
      }, 0);

      return out({
        agent_id,
        project_key: normalizedProjectKey,
        cloudinary: {
          ...summary,
          admin_timeout_ms: CLOUDINARY_ADMIN_TIMEOUT_MS
        },
        target_prefix: prefix,
        cutoff_iso: cutoff.toISOString(),
        older_than_days,
        max_scan,
        max_delete,
        dry_run,
        confirmed_cleanup,
        invalidate_cdn,
        scanned_count: resources.length,
        next_cursor_available: Boolean(next_cursor),
        expired_count: expired.length,
        recent_kept_count: recent.length,
        unparseable_created_at_count: unparseable.length,
        selected_delete_count: publicIds.length,
        deleted_count: dry_run ? 0 : deletedCount,
        candidates_sample: selected.slice(0, 20).map((resource) => ({
          public_id: resource.public_id,
          created_at: resource.created_at,
          bytes: resource.bytes || null,
          secure_url: resource.secure_url || null
        })),
        delete_batches: deleteResults.map((result) => ({
          deleted_count: Object.values(result?.deleted || {}).filter((status) => status === "deleted").length,
          partial: Boolean(result?.partial),
          next_cursor_available: Boolean(result?.next_cursor)
        })),
        note: dry_run
          ? "Dry-Run: keine Cloudinary-Ressourcen geloescht."
          : "Live: nur die ausgewaehlten Ressourcen unter dem geprueften Prefix wurden geloescht."
      });
    }
  );

  server.tool(
    "buffer_list_channels",
    "Liest Buffer-Channels fuer ein Projekt read-only. Gibt IDs, Namen und Plattformen aus, aber keine API-Keys.",
    {
      agent_id: agentIdSchema,
      project_key: z.string().min(2).max(80).optional().default("holzpunkt")
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, project_key }) => {
      const { config, summary } = getBufferProjectConfigDetails(project_key, {
        requireApiKey: true,
        requireOrganizationId: true
      });
      const channels = await fetchBufferChannels(project_key, config.organizationId);
      return out({
        agent_id,
        project_key: normalizeBufferProjectKey(project_key),
        organization_id_configured: summary.organization_id_configured,
        organization_id_env_name: summary.organization_id_env_name,
        channels
      });
    }
  );

  server.tool(
    "buffer_get_scheduled_posts",
    "Liest geplante Buffer-Posts fuer ein Projekt und optional einen oder mehrere Kanaele read-only. Standard fuer naechsten freien Tag/Slot.",
    {
      agent_id: agentIdSchema,
      project_key: z.string().min(2).max(80).optional().default("holzpunkt"),
      channels: z.array(z.enum(["instagram", "pinterest", "linkedin"])).min(1).max(3).optional(),
      first: z.number().int().min(1).max(100).optional().default(100)
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, project_key, channels, first }) => {
      const { config } = getBufferProjectConfigDetails(project_key, {
        requireApiKey: true,
        requireOrganizationId: true
      });
      const channelConfigs = (channels || ["instagram", "pinterest", "linkedin"]).map((channel) =>
        getBufferChannelConfig(project_key, channel, { requireChannelId: false })
      );
      const channelIds = channelConfigs.map((channel) => channel.channel_id).filter(Boolean);
      const posts = await fetchBufferScheduledPosts(project_key, {
        organizationId: config.organizationId,
        channelIds,
        first
      });
      return out({
        agent_id,
        project_key: normalizeBufferProjectKey(project_key),
        channels: channelConfigs,
        scheduled_count: posts.length,
        posts
      });
    }
  );

  server.tool(
    "buffer_schedule_post",
    "Plant Buffer-Posts kontrolliert fuer projektbezogene Kanaele. Laedt Asana-Bildanhaenge zuerst zu Cloudinary hoch und uebergibt stabile HTTPS-URLs an Buffer. Live-Ausfuehrung nur mit klarer Asana-Freigabe.",
    {
      agent_id: agentIdSchema,
      project_key: z.string().min(2).max(80).optional().default("holzpunkt"),
      asana_task_gid: z.string(),
      confirmed_by_asana: z.boolean().optional().default(false),
      dry_run: z.boolean().optional().default(true),
      channels: z.array(z.enum(["instagram", "pinterest", "linkedin"])).min(1).max(3),
      text: z.string().min(1).max(10000).optional(),
      text_by_channel: z.record(z.string(), z.string()).optional(),
      attachment_gids: z.array(z.string()).max(20).optional(),
      public_media_urls: z.array(z.string().url()).max(20).optional(),
      due_at: z.string().optional(),
      auto_next_free_slot: z.boolean().optional().default(true),
      preferred_times: z.array(z.string()).max(6).optional().default(["08:30", "17:15"]),
      timezone: z.string().min(3).max(80).optional().default("Europe/Berlin"),
      instagram_type: z.enum(["post", "story", "reel"]).optional().default("post"),
      instagram_should_share_to_feed: z.boolean().optional().default(true),
      max_attachment_bytes: z
        .number()
        .int()
        .min(1024)
        .max(ASANA_ATTACHMENT_HARD_MAX_BYTES)
        .optional()
        .default(ASANA_ATTACHMENT_HARD_MAX_BYTES)
    },
    TOOL_EXTERNAL_WRITE,
    async ({
      agent_id,
      project_key,
      asana_task_gid,
      confirmed_by_asana,
      dry_run,
      channels,
      text,
      text_by_channel,
      attachment_gids,
      public_media_urls,
      due_at,
      auto_next_free_slot,
      preferred_times,
      timezone,
      instagram_type,
      instagram_should_share_to_feed,
      max_attachment_bytes
    }) => {
      validateAsanaGid(asana_task_gid, "asana_task_gid");
      if (!dry_run && !confirmed_by_asana) {
        throw new Error("buffer_schedule_post braucht confirmed_by_asana=true fuer Live-Planung.");
      }
      if (!text && !text_by_channel) {
        throw new Error("buffer_schedule_post braucht text oder text_by_channel.");
      }
      const normalizedProjectKey = normalizeBufferProjectKey(project_key);
      const normalizedChannels = [...new Set(channels.map(normalizeBufferChannelKey))];
      const channelConfigs = normalizedChannels.map((channel) =>
        getBufferChannelConfig(normalizedProjectKey, channel, { requireChannelId: !dry_run })
      );
      const { config: bufferConfig } = getBufferProjectConfigDetails(normalizedProjectKey, {
        requireApiKey: !dry_run,
        requireOrganizationId: !dry_run || auto_next_free_slot
      });
      const cloudinary = getCloudinaryConfigDetails({ requireCredentials: !dry_run && !public_media_urls?.length });
      const asana = getAsana(agent_id);

      const attachmentSelection = [];
      if (attachment_gids?.length) {
        for (const gid of attachment_gids) {
          validateAsanaGid(gid, "attachment_gid");
          attachmentSelection.push(gid);
        }
      } else if (!public_media_urls?.length) {
        const attachments = await listAsanaTaskAttachments(asana, asana_task_gid);
        for (const attachment of attachments) {
          attachmentSelection.push(attachment.gid);
        }
      }

      if (!public_media_urls?.length && !attachmentSelection.length) {
        throw new Error("Keine Medien gefunden. Uebergib attachment_gids, public_media_urls oder Asana-Anhaenge am Task.");
      }

      const uploadedMedia = [];
      const mediaUrls = [...(public_media_urls || [])];
      if (!dry_run && !public_media_urls?.length) {
        for (let index = 0; index < attachmentSelection.length; index += 1) {
          const attachmentGid = attachmentSelection[index];
          const { attachment, bytes, content_type } = await downloadAsanaAttachment(
            asana,
            attachmentGid,
            max_attachment_bytes
          );
          if (!String(content_type || "").toLowerCase().startsWith("image/")) {
            throw new Error(`Asana-Anhang ${attachment.name || attachment.gid} ist kein Bild (${content_type || "unknown"}).`);
          }
          const upload = await uploadImageToCloudinary({
            projectKey: normalizedProjectKey,
            asanaTaskGid: asana_task_gid,
            attachment,
            bytes,
            contentType: content_type,
            index
          });
          uploadedMedia.push(upload);
          mediaUrls.push(upload.secure_url);
        }
      }

      const scheduledPostsByChannel = {};
      const planned = [];
      const created = [];
      for (const channelConfig of channelConfigs) {
        const channel = channelConfig.channel;
        const channelText = bufferTextForChannel({ channel, text, textByChannel: text_by_channel });
        let resolvedSchedule = null;
        if (due_at) {
          const date = new Date(due_at);
          if (Number.isNaN(date.getTime())) throw new Error(`due_at ist kein gueltiger ISO-Zeitpunkt: ${due_at}`);
          if (date.getTime() <= Date.now()) throw new Error(`due_at liegt nicht in der Zukunft: ${due_at}`);
          resolvedSchedule = {
            due_at: date.toISOString(),
            reason: "explicit_due_at",
            timezone
          };
        } else if (auto_next_free_slot) {
          const scheduledPosts = dry_run
            ? []
            : await fetchBufferScheduledPosts(normalizedProjectKey, {
                organizationId: bufferConfig.organizationId,
                channelIds: channelConfig.channel_id ? [channelConfig.channel_id] : [],
                first: 100
              });
          scheduledPostsByChannel[channel] = scheduledPosts;
          resolvedSchedule = resolveBufferAutoSlot({
            scheduledPosts,
            timeZone: timezone,
            preferredTimes: preferred_times
          });
        }

        const plan = {
          channel,
          channel_id: channelConfig.channel_id,
          channel_id_env_name: channelConfig.channel_id_env_name,
          text_chars: channelText.length,
          media_count: mediaUrls.length || attachmentSelection.length,
          due_at: resolvedSchedule?.due_at || null,
          schedule: resolvedSchedule,
          metadata: channel === "instagram"
            ? {
                instagram: {
                  type: instagram_type,
                  shouldShareToFeed: instagram_should_share_to_feed
                }
              }
            : null
        };
        planned.push(plan);

        if (!dry_run) {
          const metadataInput = bufferMetadataInputForChannel({
            channel,
            instagramType: instagram_type,
            instagramShouldShareToFeed: instagram_should_share_to_feed
          });
          const post = await createBufferPost(normalizedProjectKey, {
            text: channelText,
            channelId: channelConfig.channel_id,
            dueAt: resolvedSchedule?.due_at || null,
            mediaUrls,
            metadataInput
          });
          created.push({ channel, post });
        }
      }

      return out({
        agent_id,
        project_key: normalizedProjectKey,
        asana_task_gid,
        confirmed_by_asana,
        dry_run,
        buffer_api_key_configured: getBufferProjectConfigDetails(normalizedProjectKey, { requireApiKey: false }).summary
          .api_key_configured,
        cloudinary: cloudinary.summary,
        attachment_gids: attachmentSelection,
        provided_public_media_urls: public_media_urls?.length || 0,
        uploaded_media: uploadedMedia,
        planned,
        created,
        scheduled_read_counts: Object.fromEntries(
          Object.entries(scheduledPostsByChannel).map(([channel, posts]) => [channel, posts.length])
        ),
        note: dry_run
          ? "Dry-Run: keine Cloudinary-Uploads und keine Buffer-Posts erstellt."
          : "Live: Cloudinary-Uploads und Buffer-Planung wurden per Readback bestaetigt."
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
    "Fuehrt kontrollierte Google-Ads-Mutate-Operationen aus. Standard ist dry_run/validateOnly; echte Aenderungen brauchen Moritz-Asana-Freigabe, Hochrisiko-Aenderungen eine Extra-Freigabe.",
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
      asana_task_gid: z.string().optional(),
      approved_by: z.string().optional(),
      extra_confirmed_by_asana: z.boolean().optional().default(false),
      extra_asana_task_gid: z.string().optional(),
      extra_approval_note: z.string().optional()
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
      asana_task_gid,
      approved_by,
      extra_confirmed_by_asana,
      extra_asana_task_gid,
      extra_approval_note
    }) => {
      const normalizedCustomerId = normalizeGoogleAdsCustomerId(customer_id);
      const effectiveValidateOnly = dry_run || validate_only;
      const safety = await assessGoogleAdsMutateSafety({
        customerId: normalizedCustomerId,
        service,
        operations,
        loginCustomerId: login_customer_id
      });
      if (!effectiveValidateOnly) {
        assertMoritzAsanaConfirmed({
          confirmedByAsana: confirmed_by_asana,
          asanaTaskGid: asana_task_gid,
          approvedBy: approved_by,
          actionName: "google_ads_mutate"
        });
        if (!String(change_summary || "").trim()) {
          throw new Error("google_ads_mutate braucht fuer echte Aenderungen eine change_summary.");
        }
        if (safety.requires_extra_approval) {
          assertMoritzAsanaConfirmed({
            confirmedByAsana: extra_confirmed_by_asana,
            asanaTaskGid: extra_asana_task_gid,
            approvedBy: approved_by,
            actionName: "google_ads_mutate Hochrisiko-Aenderung"
          });
          if (!String(extra_approval_note || "").trim()) {
            throw new Error(
              "google_ads_mutate braucht fuer Hochrisiko-Aenderungen eine extra_approval_note mit Bezug auf die Extra-Freigabe."
            );
          }
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
        approved_by: approved_by || null,
        safety,
        extra_approval: {
          required: safety.requires_extra_approval,
          confirmed: Boolean(extra_confirmed_by_asana && extra_asana_task_gid),
          asana_task_gid: extra_asana_task_gid || null,
          note: extra_approval_note || null
        },
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
    "google_drive_check_config",
    "Prueft die Google-Drive/Sheets-Konfiguration fuer den Remote-MCP read-only. Gibt keine Secret-Werte aus und kann optional testweise ein Access Token erzeugen.",
    {
      agent_id: agentIdSchema,
      fetch_token: z.boolean().optional().default(false)
    },
    TOOL_READ_ONLY,
    async ({ agent_id, fetch_token }) => {
      const summary = googleDriveAuthSummary(agent_id);
      let token_check = { requested: Boolean(fetch_token), status: "not_requested" };
      if (fetch_token) {
        try {
          await getGoogleAccessToken({ agent_id });
          token_check = {
            requested: true,
            status: "ok",
            active_auth: googleTokenCache.envPrefix || null,
            expires_at: googleTokenCache.expiresAt
              ? new Date(googleTokenCache.expiresAt * 1000).toISOString()
              : null
          };
        } catch (error) {
          token_check = {
            requested: true,
            status: "failed",
            error: error?.message || String(error)
          };
        }
      }

      return out({
        agent_id,
        ...summary,
        token_check
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
      const googleContext = { agent_id };
      await assertAllowedGoogleFolder(target_folder_id, googleContext);

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
      }, googleContext);

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
      const googleContext = { agent_id };
      await assertAllowedGoogleFolder(target_folder_id, googleContext);

      const sourceFile = await getDriveFile(source_file_id, "id,name,mimeType,parents,webViewLink", googleContext);
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
      }, googleContext);

      const verifiedFile = verify_after
        ? await getDriveFile(res.data.id, "id,name,mimeType,parents,webViewLink", googleContext)
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
      const googleContext = { agent_id };
      await assertAllowedGoogleFolder(target_folder_id, googleContext);

      const file = await getDriveFile(file_id, "id,name,mimeType,parents,webViewLink", googleContext);
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
      }, googleContext);

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
      }, { agent_id });

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
      const values = await getSheetValues(spreadsheet_id, a1Range, value_render_option, { agent_id });
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
      }, { agent_id });

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
      }, { agent_id });

      const verified_values = verify_after ? await getSheetValues(spreadsheet_id, a1Range, "FORMATTED_VALUE", { agent_id }) : undefined;
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
        typeof sheet_id === "number" ? sheet_id : await getSheetIdByName(spreadsheet_id, sheet_name, { agent_id });

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
      }, { agent_id });

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
    "wp_import_check_config",
    "Prueft die Goklever-WordPress-Import-Konfiguration ohne CSV-Import. Optional wird der Endpunkt per HEAD read-only auf Erreichbarkeit geprueft.",
    {
      agent_id: agentIdSchema,
      check_endpoint: z.boolean().optional().default(false)
    },
    TOOL_EXTERNAL_READ,
    async ({ agent_id, check_endpoint }) => {
      const summary = getWordPressImportConfigSummary();
      const result = {
        agent_id,
        ...summary,
        check_endpoint,
        endpoint_check: null
      };

      if (!check_endpoint) {
        return out(result);
      }

      if (!summary.wordpress_import_url_valid) {
        return out({
          ...result,
          endpoint_check: {
            attempted: false,
            reachable: false,
            reason: "WORDPRESS_GK_IMPORT_URL ist keine gueltige URL."
          }
        });
      }

      try {
        const response = await axios.request({
          method: "HEAD",
          url: WP_IMPORT_URL,
          timeout: 15000,
          validateStatus: () => true
        });

        return out({
          ...result,
          endpoint_check: {
            attempted: true,
            method: "HEAD",
            reachable: true,
            status: response.status,
            status_text: response.statusText || null
          }
        });
      } catch (error) {
        return out({
          ...result,
          endpoint_check: {
            attempted: true,
            method: "HEAD",
            reachable: false,
            code: error.code || null,
            retryable: isRetryableAxiosError(error),
            message: error.message
          }
        });
      }
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
      const csvBytes = Buffer.byteLength(trimmedCsv, "utf8");
      const csvSha256 = createHash("sha256").update(trimmedCsv).digest("hex");
      const preview = {
        line_count: lines.length,
        first_line: lines[0],
        source,
        csv_bytes: csvBytes,
        csv_sha256: csvSha256
      };

      if (dry_run) {
        return out({ agent_id, dry_run: true, preview });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "wp_import_csv");

      const apiKey = process.env.WORDPRESS_GK_API_KEY;
      if (!apiKey) throw new Error("WORDPRESS_GK_API_KEY fehlt im MCP-Environment.");
      const importConfig = getWordPressImportConfigSummary();
      if (!importConfig.wordpress_import_url_valid) {
        throw new Error("WORDPRESS_GK_IMPORT_URL ist keine gueltige URL.");
      }

      let lastError;
      let attempts = 0;
      for (let attempt = 1; attempt <= 3; attempt += 1) {
        attempts = attempt;
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
            ok: true,
            status: res.status,
            attempts: attempt,
            source,
            csv_line_count: lines.length,
            csv_bytes: csvBytes,
            csv_sha256: csvSha256,
            response_summary: summarizeWordPressImportResponse(res.data),
            response: res.data,
            endpoint: {
              host: importConfig.wordpress_import_host,
              path: importConfig.wordpress_import_path
            }
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
          attempts,
          source,
          csv_line_count: lines.length,
          csv_bytes: csvBytes,
          csv_sha256: csvSha256,
          status: lastError.response.status,
          response_summary: summarizeWordPressImportResponse(lastError.response.data),
          response: lastError.response.data
        });
      }

      return out({
        agent_id,
        ok: false,
        attempts,
        source,
        csv_line_count: lines.length,
        csv_bytes: csvBytes,
        csv_sha256: csvSha256,
        endpoint: {
          host: importConfig.wordpress_import_host,
          path: importConfig.wordpress_import_path
        },
        network_error: {
          code: lastError?.code || null,
          retryable: lastError ? isRetryableAxiosError(lastError) : null,
          message: lastError?.message || "Unbekannter Netzwerkfehler."
        }
      });
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
