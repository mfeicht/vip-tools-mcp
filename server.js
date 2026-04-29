import "dotenv/config";
import axios from "axios";
import fs from "fs";
import path from "path";
import express from "express";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";

const AGENT_ID = process.env.VIP_AGENT_ID || "vip-ai-sales";

function loadAgentConfig(agentId) {
  const file = path.join("agents", `${agentId}.json`);
  if (!fs.existsSync(file)) throw new Error(`Agent config nicht gefunden: ${file}`);
  return JSON.parse(fs.readFileSync(file, "utf-8"));
}

const agent = loadAgentConfig(AGENT_ID);
const ASANA_TOKEN = process.env[agent.asanaTokenEnv];

if (!ASANA_TOKEN) throw new Error(`Token fehlt für ${agent.asanaTokenEnv}`);

const asana = axios.create({
  baseURL: "https://app.asana.com/api/1.0",
  headers: { Authorization: `Bearer ${ASANA_TOKEN}` }
});

const server = new McpServer({
  name: "vip-tools-local-http",
  version: "2.0.0"
});

function out(data) {
  return {
    content: [{ type: "text", text: JSON.stringify(data, null, 2) }]
  };
}

server.tool("asana_get_my_tasks", "Liest alle offenen Aufgaben.", {
  limit: z.number().optional().default(20)
}, async ({ limit }) => {
  const user = await asana.get("/users/me");
  const workspace = user.data.data.workspaces[0].gid;

  const res = await asana.get("/tasks", {
    params: {
      assignee: "me",
      workspace,
      completed_since: "now",
      limit,
      opt_fields: "gid,name,completed,permalink_url,due_on"
    }
  });

  return out(res.data.data);
});

server.tool("asana_add_comment", "Kommentar schreiben", {
  task_gid: z.string(),
  text: z.string()
}, async ({ task_gid, text }) => {
  const res = await asana.post(`/tasks/${task_gid}/stories`, {
    data: { text }
  });

  return out(res.data.data);
});

server.tool("asana_complete_task", "Task abschließen", {
  task_gid: z.string()
}, async ({ task_gid }) => {
  const res = await asana.put(`/tasks/${task_gid}`, {
    data: { completed: true }
  });

  return out(res.data.data);
});

/* ---------------- HTTP SERVER FIX ---------------- */

const app = express();
app.use(express.json());

const transport = new StreamableHTTPServerTransport();

await server.connect(transport);

app.post("/mcp", async (req, res) => {
  try {
    await transport.handleRequest(req, res, req.body);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message });
  }
});

app.get("/mcp", async (req, res) => {
  await transport.handleRequest(req, res);
});

app.delete("/mcp", async (req, res) => {
  await transport.handleRequest(req, res);
});

app.listen(3333, () => {
  console.log("MCP läuft auf http://localhost:3333/mcp");
});
