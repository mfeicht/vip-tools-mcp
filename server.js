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
  name: "vip-tools-remote",
  version: "1.0.0"
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
      opt_fields: "gid,name,completed,permalink_url,due_on,due_at,tags.name,tags.gid,projects.name,projects.gid,modified_at"
    }
  });

  return out(res.data.data);
});

server.tool("asana_get_task", "Liest Details einer Asana-Aufgabe.", {
  task_gid: z.string()
}, async ({ task_gid }) => {
  const res = await asana.get(`/tasks/${task_gid}`, {
    params: {
      opt_fields: "gid,name,notes,completed,due_on,due_at,assignee.name,assignee.gid,permalink_url,followers.name,followers.gid,tags.name,tags.gid,projects.name,projects.gid,created_at,modified_at"
    }
  });

  return out(res.data.data);
});

server.tool("asana_get_task_stories", "Liest Kommentare und Verlauf einer Asana-Aufgabe.", {
  task_gid: z.string()
}, async ({ task_gid }) => {
  const res = await asana.get(`/tasks/${task_gid}/stories`, {
    params: {
      opt_fields: "gid,type,text,created_at,created_by.name,created_by.gid"
    }
  });

  return out(res.data.data);
});

server.tool("asana_add_comment", "Schreibt einen Kommentar in eine Asana-Aufgabe.", {
  task_gid: z.string(),
  text: z.string()
}, async ({ task_gid, text }) => {
  const res = await asana.post(`/tasks/${task_gid}/stories`, {
    data: { text }
  });

  return out(res.data.data);
});

server.tool("asana_update_task", "Aktualisiert einzelne Felder einer Asana-Aufgabe.", {
  task_gid: z.string(),
  data: z.record(z.any())
}, async ({ task_gid, data }) => {
  const res = await asana.put(`/tasks/${task_gid}`, { data });
  return out(res.data.data);
});

server.tool("asana_create_task", "Erstellt eine neue Asana-Aufgabe.", {
  data: z.record(z.any())
}, async ({ data }) => {
  const user = await asana.get("/users/me");
  const workspace = user.data.data.workspaces[0].gid;

  const res = await asana.post("/tasks", {
    data: { workspace, ...data }
  });

  return out(res.data.data);
});

server.tool("asana_complete_task", "Schließt eine Asana-Aufgabe ab.", {
  task_gid: z.string()
}, async ({ task_gid }) => {
  const res = await asana.put(`/tasks/${task_gid}`, {
    data: { completed: true }
  });

  return out(res.data.data);
});

server.tool("asana_add_followers", "Fügt Beteiligte zu einer Asana-Aufgabe hinzu.", {
  task_gid: z.string(),
  followers: z.array(z.string())
}, async ({ task_gid, followers }) => {
  const res = await asana.post(`/tasks/${task_gid}/addFollowers`, {
    data: { followers }
  });

  return out(res.data.data);
});

server.tool("asana_add_tag", "Fügt einer Asana-Aufgabe einen Tag hinzu.", {
  task_gid: z.string(),
  tag_gid: z.string()
}, async ({ task_gid, tag_gid }) => {
  const res = await asana.post(`/tasks/${task_gid}/addTag`, {
    data: { tag: tag_gid }
  });

  return out(res.data.data);
});

server.tool("asana_remove_tag", "Entfernt einen Tag von einer Asana-Aufgabe.", {
  task_gid: z.string(),
  tag_gid: z.string()
}, async ({ task_gid, tag_gid }) => {
  const res = await asana.post(`/tasks/${task_gid}/removeTag`, {
    data: { tag: tag_gid }
  });

  return out(res.data.data);
});

const app = express();
app.use(express.json());

const transport = new StreamableHTTPServerTransport({
  sessionIdGenerator: undefined
});

await server.connect(transport);

app.post("/mcp", async (req, res) => {
  try {
    await transport.handleRequest(req, res, req.body);
  } catch (error) {
    console.error(error);
    if (!res.headersSent) {
      res.status(500).json({ error: error.message });
    }
  }
});

app.get("/mcp", async (req, res) => {
  await transport.handleRequest(req, res);
});

app.delete("/mcp", async (req, res) => {
  await transport.handleRequest(req, res);
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`MCP läuft auf Port ${PORT}`);
});