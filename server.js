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

/* ---------------- MCP SERVER ---------------- */

const server = new McpServer({
  name: "vip-tools-remote",
  version: "1.0.0"
});

function out(data) {
  return {
    content: [{ type: "text", text: JSON.stringify(data, null, 2) }]
  };
}

/* ---------------- TOOLS ---------------- */

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

/* ---------------- HTTP MCP ---------------- */

const app = express();
app.use(express.json());

const transport = new StreamableHTTPServerTransport({
  sessionIdGenerator: undefined
});

await server.connect(transport);

app.post("/mcp", async (req, res) => {
  try {
    console.log("MCP POST", req.body?.method);
    await transport.handleRequest(req, res, req.body);
  } catch (err) {
    console.error("MCP POST ERROR:", err);
    if (!res.headersSent) {
      res.status(500).json({
        error: err.message,
        stack: err.stack
      });
    }
  }
});

app.get("/mcp", async (req, res) => {
  try {
    await transport.handleRequest(req, res);
  } catch (err) {
    console.error("MCP GET ERROR:", err);
    if (!res.headersSent) res.status(500).json({ error: err.message });
  }
});

app.delete("/mcp", async (req, res) => {
  try {
    await transport.handleRequest(req, res);
  } catch (err) {
    console.error("MCP DELETE ERROR:", err);
    if (!res.headersSent) res.status(500).json({ error: err.message });
  }
});

/* ---------------- START ---------------- */

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log("MCP läuft auf Port " + PORT);
});