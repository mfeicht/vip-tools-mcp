import "dotenv/config";
import express from "express";
import axios from "axios";
import { z } from "zod";

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

/* ---------------- CONFIG ---------------- */

const ASANA_TOKEN = process.env.ASANA_TOKEN_VIP_AI_SALES;
if (!ASANA_TOKEN) throw new Error("ASANA_TOKEN_VIP_AI_SALES fehlt");

/* ---------------- ASANA ---------------- */

const asana = axios.create({
  baseURL: "https://app.asana.com/api/1.0",
  headers: {
    Authorization: `Bearer ${ASANA_TOKEN}`
  }
});

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
    "asana_get_my_tasks",
    "Liest offene Tasks",
    { limit: z.number().optional().default(20) },
    async ({ limit }) => {
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

      return out(res.data.data);
    }
  );
  server.tool(

      "asana_request",

      "Führt einen beliebigen Asana-API-Request mit dem aktuellen Agenten-Token aus.",

      {

        method: z.enum(["GET", "POST", "PUT", "DELETE"]),

        path: z.string(),

        params: z.record(z.any()).optional(),

        data: z.record(z.any()).optional()

      },

      async ({ method, path, params, data }) => {

        if (!path.startsWith("/")) {

          throw new Error("path muss mit / beginnen, z. B. /tasks");

        }

        const res = await asana.request({

          method,

          url: path,

          params,

          data: data ? { data } : undefined

        });

        return out(res.data);

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
