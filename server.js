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

/* ---------------- MCP TRANSPORT ---------------- */

const transport = new StreamableHTTPServerTransport({
  server
});

/* ---------------- EXPRESS ---------------- */

const app = express();

app.use("/mcp", (req, res) => {
  transport.handleRequest(req, res);
});

/* ---------------- START ---------------- */

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log("MCP läuft auf Port " + PORT);
});