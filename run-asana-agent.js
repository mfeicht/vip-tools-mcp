import "dotenv/config";
import axios from "axios";
import fs from "fs";

const agent = JSON.parse(fs.readFileSync("./agents/vip-ai-sales.json", "utf-8"));
const token = process.env[agent.asanaTokenEnv];

if (!token) throw new Error(`Token fehlt: ${agent.asanaTokenEnv}`);

const asana = axios.create({
  baseURL: "https://app.asana.com/api/1.0",
  headers: { Authorization: `Bearer ${token}` },
});

const user = await asana.get("/users/me");
const workspace = user.data.data.workspaces[0].gid;

const tasksRes = await asana.get("/tasks", {
  params: {
    assignee: "me",
    workspace,
    completed_since: "now",
    limit: 20,
    opt_fields:
      "gid,name,completed,due_on,due_at,permalink_url,tags.name,tags.gid,projects.name,projects.gid,modified_at",
  },
});

const tasks = tasksRes.data.data;

if (!tasks.length) {
  console.log(JSON.stringify({ agent: agent.name, status: "no_tasks", message: "Keine offenen Aufgaben gefunden." }, null, 2));
  process.exit(0);
}

// Erstmal: erste offene Aufgabe laden
const selected = tasks[0];

const taskRes = await asana.get(`/tasks/${selected.gid}`, {
  params: {
    opt_fields:
      "gid,name,notes,completed,due_on,due_at,assignee.name,assignee.gid,permalink_url,followers.name,followers.gid,tags.name,tags.gid,projects.name,projects.gid,created_at,modified_at",
  },
});

const storiesRes = await asana.get(`/tasks/${selected.gid}/stories`, {
  params: {
    opt_fields: "gid,type,text,created_at,created_by.name,created_by.gid",
  },
});

console.log(JSON.stringify({
  agent: agent.name,
  status: "task_loaded",
  task: taskRes.data.data,
  stories: storiesRes.data.data
}, null, 2));
