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
      Authorization: `Bearer ${token}`
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

function assertSafeAsanaRequest(method, path, data) {
  const isStoryCreate = method === "POST" && /^\/tasks\/[^/]+\/stories\/?$/.test(path);
  const isStoryUpdate = method === "PUT" && /^\/stories\/[^/]+\/?$/.test(path);
  if (!isStoryCreate && !isStoryUpdate) return;

  if (!data || typeof data !== "object") return;

  if (typeof data.text === "string") {
    throw new Error(
      "Asana-Kommentare muessen als html_text gesendet werden. data.text ist fuer Task-Stories in diesem MCP blockiert, damit Mentions/Rich-Text nicht als Plain Text rausgehen."
    );
  }

  if (typeof data.html_text === "string") {
    const html = data.html_text.trim();
    if (!html.startsWith("<body")) {
      throw new Error("Asana html_text muss mit <body> beginnen.");
    }
    if (!html.endsWith("</body>")) {
      throw new Error("Asana html_text muss mit </body> enden.");
    }
    if (/<br\b/i.test(html)) {
      throw new Error(
        "Asana html_text darf kein <br/> enthalten. Nutze Asana-kompatible Abschnitte mit <strong>, <em>, <ul>/<ol>/<li>, <code> und echte Mentions."
      );
    }
    if (/&lt;\s*\/?\s*(body|strong|em|ul|ol|li|code|a)\b/i.test(html)) {
      throw new Error(
        "Asana html_text enthaelt bereits escaped HTML. Sende echtes Asana-Rich-Text-Markup, nicht HTML als Text."
      );
    }
    if (containsPlainMention(html)) {
      throw new Error(
        "Plain-Text-Mention blockiert. Nutze echte Asana-Mentions mit <a data-asana-gid=\"USER_GID\"/> statt @Name."
      );
    }
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

function getSmtpConfig(agentId, { requireCredentials = true } = {}) {
  const suffix = envSuffixForAgent(agentId);
  const fromAddress =
    process.env[`EMAIL_ADDRESS_${suffix}`] || AGENT_EMAIL_DEFAULTS[agentId];
  const host = getEnvWithAgentFallback("SMTP_HOST", agentId);
  const port = Number(getEnvWithAgentFallback("SMTP_PORT", agentId) || 465);
  const secure = parseBooleanEnv(getEnvWithAgentFallback("SMTP_SECURE", agentId), port === 465);
  const user = process.env[`SMTP_USER_${suffix}`] || fromAddress;
  const password =
    process.env[`SMTP_PASSWORD_${suffix}`] ||
    process.env[`EMAIL_PASSWORD_${suffix}`];

  if (!fromAddress) throw new Error(`E-Mail-Adresse fuer agent_id ${agentId} fehlt.`);
  if (requireCredentials) {
    if (!host) throw new Error(`SMTP_HOST oder SMTP_HOST_${suffix} fehlt im MCP-Environment.`);
    if (!password) {
      throw new Error(`SMTP_PASSWORD_${suffix} oder EMAIL_PASSWORD_${suffix} fehlt im MCP-Environment.`);
    }
  }

  return { fromAddress, host, port, secure, user, password };
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
      await new Promise((resolve) => waiters.push(resolve));
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
      const socket = tls.connect(
        {
          host: config.host,
          port: config.port,
          servername: config.host,
          rejectUnauthorized: !parseBooleanEnv(process.env.SMTP_TLS_ALLOW_UNAUTHORIZED, false)
        },
        () => resolve(socket)
      );
      socket.once("error", reject);
    });
  }

  return new Promise((resolve, reject) => {
    const socket = net.connect({ host: config.host, port: config.port }, () => resolve(socket));
    socket.once("error", reject);
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

async function sendSmtpEmail(config, { to, subject, body }) {
  let socket = await createSmtpSocket(config);
  let readResponse = createSmtpReader(socket);

  await expectSmtp(readResponse, [220], "connect");
  writeSmtp(socket, "EHLO vip-tools-mcp");
  await expectSmtp(readResponse, [250], "ehlo");

  if (!config.secure && config.port === 587) {
    writeSmtp(socket, "STARTTLS");
    await expectSmtp(readResponse, [220], "starttls");
    socket = await new Promise((resolve, reject) => {
      const tlsSocket = tls.connect(
        {
          socket,
          servername: config.host,
          rejectUnauthorized: !parseBooleanEnv(process.env.SMTP_TLS_ALLOW_UNAUTHORIZED, false)
        },
        () => resolve(tlsSocket)
      );
      tlsSocket.once("error", reject);
    });
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
  writeSmtp(socket, "QUIT");
  await expectSmtp(readResponse, [221], "quit");
  socket.end();
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

        const asana = getAsana(agent_id);

        assertSafeAsanaRequest(method, path, data);

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

      const config = getSmtpConfig(agent_id, { requireCredentials: true });
      await sendSmtpEmail(config, { to: draft.to, subject: draft.subject, body: draft.body });
      emailDraftCache.delete(draft_id);

      return out({
        agent_id,
        sent: true,
        draft_id,
        from: config.fromAddress,
        to: draft.to,
        subject: draft.subject,
        body_bytes: draft.body_bytes,
        body_sha256: draft.body_sha256
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
      const config = getSmtpConfig(agent_id, { requireCredentials: !dry_run });

      if (dry_run) {
        return out({
          agent_id,
          dry_run: true,
          from: config.fromAddress,
          to: validated.recipients,
          subject: validated.subject,
          body_bytes: validated.body_bytes,
          body_sha256: validated.body_sha256
        });
      }

      assertAsanaConfirmed(confirmed_by_asana, asana_task_gid, "agent_email_send");
      await sendSmtpEmail(config, { to: validated.recipients, subject: validated.subject, body });

      return out({
        agent_id,
        sent: true,
        from: config.fromAddress,
        to: validated.recipients,
        subject: validated.subject,
        body_bytes: validated.body_bytes,
        body_sha256: validated.body_sha256
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
