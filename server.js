import "dotenv/config";
import express from "express";
import axios from "axios";
import { createSign } from "crypto";
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

let googleTokenCache = { accessToken: null, expiresAt: 0 };

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
      values: z.array(z.array(z.union([z.string(), z.number(), z.boolean(), z.null()]))),
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

app.listen(PORT, () => {
  console.log("MCP läuft auf Port " + PORT);
});
