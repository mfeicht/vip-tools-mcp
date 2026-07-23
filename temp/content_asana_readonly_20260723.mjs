import "dotenv/config";
import axios from "axios";
import fs from "fs";

const agentId = "vip-ai-content";
const agent = JSON.parse(fs.readFileSync("./agents/vip-ai-content.json", "utf8"));
const token = process.env[agent.asanaTokenEnv];
if (!token) throw new Error(`Missing token for ${agent.asanaTokenEnv}`);
const asana = axios.create({
  baseURL: "https://app.asana.com/api/1.0",
  headers: { Authorization: `Bearer ${token}`, "Asana-Enable": "new_rich_text" }
});
const taskFields = [
  "gid", "name", "notes", "completed", "due_on", "due_at", "permalink_url", "created_at", "modified_at",
  "assignee.gid", "assignee.name", "followers.gid", "followers.name", "tags.gid", "tags.name",
  "projects.gid", "projects.name"
].join(",");
const storyFields = ["gid", "type", "resource_subtype", "text", "created_at", "created_by.gid", "created_by.name"].join(",");
const me = (await asana.get("/users/me")).data.data;
const workspace = me.workspaces?.[0]?.gid;
const tasks = (await asana.get("/tasks", { params: { assignee: "me", workspace, completed_since: "now", limit: 50, opt_fields: taskFields } })).data.data;
const hydrated = [];
for (const task of tasks) {
  const detail = (await asana.get(`/tasks/${task.gid}`, { params: { opt_fields: taskFields } })).data.data;
  const stories = (await asana.get(`/tasks/${task.gid}/stories`, { params: { opt_fields: storyFields, limit: 100 } })).data.data;
  const attachments = (await asana.get(`/tasks/${task.gid}/attachments`, { params: { opt_fields: "gid,name,created_at,resource_subtype,download_url,view_url", limit: 100 } })).data.data;
  hydrated.push({ task: detail, stories, attachments });
}
console.log(JSON.stringify({
  agent_id: agentId,
  whoami: { gid: me.gid, name: me.name, workspace },
  tasks: hydrated.map(({ task, stories, attachments }) => ({
    task: {
      gid: task.gid, name: task.name, completed: task.completed, due_on: task.due_on, due_at: task.due_at,
      modified_at: task.modified_at, permalink_url: task.permalink_url,
      assignee: task.assignee, followers: task.followers, tags: task.tags, projects: task.projects,
      notes_tail: String(task.notes || "").slice(-1800)
    },
    recent_stories: stories.slice(-8).map((story) => ({ gid: story.gid, created_at: story.created_at, type: story.type, resource_subtype: story.resource_subtype, created_by: story.created_by, text: String(story.text || "").slice(0, 1800) })),
    attachments
  }))
}, null, 2));
