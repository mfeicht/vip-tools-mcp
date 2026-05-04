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
const WP_IMPORT_URL =
  process.env.WORDPRESS_GK_IMPORT_URL || "https://app.goklever.de/wp-json/gk-mailpoet/v1/import-csv";
const RS_REDAKTIONSPLAN_SPREADSHEET_ID =
  process.env.RS_REDAKTIONSPLAN_SPREADSHEET_ID || "17BbIPRBGxcNUAYPNd6AD3wnvik6s26P8fnvPDeH1UrM";
const RS_REDAKTIONSPLAN_SHEET_NAME = process.env.RS_REDAKTIONSPLAN_SHEET_NAME || "Redaktionsplan";
const DEFAULT_EMAIL_ALLOWED_RECIPIENTS = "*";
const EMAIL_DRAFT_TTL_MS = Number(process.env.EMAIL_DRAFT_TTL_MS || 60 * 60 * 1000);
const SMTP_TIMEOUT_MS = Number(process.env.SMTP_TIMEOUT_MS || 15_000);
const EMAIL_HTTP_TIMEOUT_MS = Number(process.env.EMAIL_HTTP_TIMEOUT_MS || 20_000);
const DATAFORSEO_API_BASE = (process.env.DATAFORSEO_API_BASE || "https://api.dataforseo.com/v3").replace(/\/$/, "");
const DATAFORSEO_TIMEOUT_MS = Number(process.env.DATAFORSEO_TIMEOUT_MS || 30_000);
const DATAFORSEO_DEFAULT_LOCATION_CODE = Number(process.env.DATAFORSEO_DEFAULT_LOCATION_CODE || 2276);
const DATAFORSEO_DEFAULT_LANGUAGE_CODE = process.env.DATAFORSEO_DEFAULT_LANGUAGE_CODE || "de";
const DATAFORSEO_MAX_RESULTS = Math.min(Number(process.env.DATAFORSEO_MAX_RESULTS || 100), 1000);

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
  "rs-ai-sales": "sales-agent@reise-stories.de",
  "rs-ai-content": "content-agent@reise-stories.de",
  "rs-ai-support": "support-agent@reise-stories.de",
  "rs-ai-marketing": "marketing-agent@reise-stories.de",
  "rs-ai-office": "office-agent@reise-stories.de"
};

let googleTokenCache = { accessToken: null, expiresAt: 0 };
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

  if (isTaskComplete) {
    throw new Error(
      "Asana-Aufgaben duerfen nicht per rohem asana_request abgeschlossen werden. Nutze asana_complete_task; es prueft Assignee, finalen Kommentar und Readback."
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
  return `${escapeAsanaXml(text)}\n\n`;
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
          return `<li>${escapeAsanaXml(bullet)}</li>`;
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
    const text = mention_text ? ` ${escapeAsanaXml(mention_text)}` : "";
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
      if (!/^<a data-asana-gid="\d+"\/>$/.test(raw)) {
        throw new Error("Asana-Mention muss exakt als <a data-asana-gid=\"...\"/> erzeugt werden.");
      }
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
  const suffix = envSuffixForAgent(agentId);
  const loginEnvName = process.env[`DATAFORSEO_LOGIN_${suffix}`] ? `DATAFORSEO_LOGIN_${suffix}` : "DATAFORSEO_LOGIN";
  const passwordEnvName = process.env[`DATAFORSEO_PASSWORD_${suffix}`]
    ? `DATAFORSEO_PASSWORD_${suffix}`
    : "DATAFORSEO_PASSWORD";
  const login = process.env[loginEnvName] || "";
  const password = process.env[passwordEnvName] || "";

  if (requireCredentials) {
    if (!login) throw new Error(`DataForSEO Login fehlt im MCP-Environment (${loginEnvName}).`);
    if (!password) throw new Error(`DataForSEO Passwort fehlt im MCP-Environment (${passwordEnvName}).`);
  }

  return {
    config: { login, password },
    summary: {
      agent_id: agentId,
      dataforseo_api_base: DATAFORSEO_API_BASE,
      login_configured: Boolean(login),
      login_env_name: loginEnvName,
      password_configured: Boolean(password),
      password_env_name: password ? passwordEnvName : null,
      ready_for_live_calls: Boolean(login && password),
      default_location_code: DATAFORSEO_DEFAULT_LOCATION_CODE,
      default_language_code: DATAFORSEO_DEFAULT_LANGUAGE_CODE,
      max_results: DATAFORSEO_MAX_RESULTS,
      timeout_ms: DATAFORSEO_TIMEOUT_MS
    }
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
    throw new Error(`DataForSEO Fehler ${response.status_code}: ${response.status_message}`);
  }
  const failedTask = (response.tasks || []).find((task) => task.status_code && task.status_code !== 20000);
  if (failedTask) {
    throw new Error(`DataForSEO Task-Fehler ${failedTask.status_code}: ${failedTask.status_message}`);
  }
}

async function dataForSeoRequest(agentId, { method = "POST", path, data }) {
  const { config } = getDataForSeoConfigDetails(agentId, { requireCredentials: true });
  const res = await axios.request({
    method,
    url: `${DATAFORSEO_API_BASE}${path}`,
    auth: {
      username: config.login,
      password: config.password
    },
    headers: { "Content-Type": "application/json" },
    timeout: DATAFORSEO_TIMEOUT_MS,
    data
  });
  assertDataForSeoOk(res.data);
  return res.data;
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
    "asana_assign_task",
    "Weist eine Asana-Aufgabe mit dem Token des angegebenen Agenten einem konkreten Asana-User per GID zu. Aendert keine weiteren Felder.",
    {
      agent_id: agentIdSchema,
      task_gid: z.string(),
      assignee_gid: z.string()
    },
    async ({ agent_id, task_gid, assignee_gid }) => {
      const asana = getAsana(agent_id);
      const res = await asana.put(`/tasks/${task_gid}`, {
        data: { assignee: assignee_gid }
      });

      return out({ agent_id, task: res.data.data });
    }
  );

  server.tool(
    "dataforseo_check_config",
    "Prueft die DataForSEO-Konfiguration ohne kostenpflichtige Keyword-Abfrage. Optional wird der kostenlose User-Data-Endpunkt abgefragt.",
    {
      agent_id: agentIdSchema,
      fetch_user_data: z.boolean().optional().default(false)
    },
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
    "google_drive_move_file_to_agent_folder",
    "Verschiebt eine bestehende Google-Drive-Datei in den erlaubten Agenten-Drive-Ordner oder einen Unterordner davon. Entfernt standardmaessig alte Parents.",
    {
      agent_id: agentIdSchema,
      file_id: z.string(),
      target_folder_id: z.string().optional().default(GOOGLE_AGENT_FOLDER_ID),
      remove_previous_parents: z.boolean().optional().default(true)
    },
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
    async ({ agent_id, spreadsheet_id, sheet_name, range, value_render_option }) => {
      const a1Range = buildSheetRange(sheet_name, range);
      const values = await getSheetValues(spreadsheet_id, a1Range, value_render_option);
      return out({ agent_id, spreadsheet_id, sheet_name, range, values });
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
    "agent_email_prepare",
    "Bereitet eine Plain-Text-E-Mail serverseitig vor und gibt eine Draft-ID zurueck. Sendet nichts; empfohlen fuer grosse Payloads vor agent_email_send_prepared.",
    {
      agent_id: agentIdSchema,
      to: z.string(),
      subject: z.string(),
      body: z.string()
    },
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
