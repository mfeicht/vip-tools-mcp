import "dotenv/config";
import express from "express";
import axios from "axios";
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
