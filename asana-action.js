import "dotenv/config";
import axios from "axios";
import fs from "fs";

const args = process.argv.slice(2);
const agentArg = args[0]?.startsWith("--agent=")
  ? args.shift().replace("--agent=", "")
  : "vip-ai-sales";
const agentFile = agentArg.endsWith(".json") ? agentArg : `./agents/${agentArg}.json`;
const agent = JSON.parse(fs.readFileSync(agentFile, "utf-8"));
const token = process.env[agent.asanaTokenEnv];

if (!token) throw new Error(`Token fehlt: ${agent.asanaTokenEnv}`);

const asana = axios.create({
  baseURL: "https://app.asana.com/api/1.0",
  headers: {
    Authorization: `Bearer ${token}`,
    "Asana-Enable": "new_rich_text",
  },
});

const [action, taskGid, ...rest] = args;

if (!action || !taskGid) {
  throw new Error("Nutzung: node asana-action.js [--agent=vip-ai-sales] comment|complete|update|append-notes TASK_GID ...");
}

function escapeHtml(value) {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function containsPlainMention(value) {
  return /(^|[\s(])@[A-Za-zÄÖÜäöüß]/.test(value);
}

function toHtmlText(value) {
  const trimmed = value.trim();
  if (trimmed.startsWith("<body")) {
    throw new Error("Rohes Asana-html_text ist in asana-action.js blockiert. Nutze das MCP-Tool asana_comment.");
  }
  return `<body>${escapeHtml(value).replace(/\n{3,}/g, "\n\n")}</body>`;
}

if (action === "comment") {
  const text = rest.join(" ");
  if (!text) throw new Error("Kommentartext fehlt");
  if (containsPlainMention(text)) {
    throw new Error(
      "Plain-Text-Mention blockiert. Asana-Kommentare mit Erwaehnung muessen html_text und echte Mentions nutzen, z. B. <a data-asana-gid=\"1108801330389276\"/>."
    );
  }

  const res = await asana.post(`/tasks/${taskGid}/stories`, {
    data: { html_text: toHtmlText(text) },
  });

  console.log(JSON.stringify({ ok: true, action, result: res.data.data }, null, 2));
}

else if (action === "complete") {
  const res = await asana.put(`/tasks/${taskGid}`, {
    data: { completed: true },
  });

  console.log(JSON.stringify({ ok: true, action, result: res.data.data }, null, 2));
}

else if (action === "update") {
  const json = rest.join(" ");
  if (!json) throw new Error("Update-JSON fehlt");

  const data = JSON.parse(json);

  const res = await asana.put(`/tasks/${taskGid}`, {
    data,
  });

  console.log(JSON.stringify({ ok: true, action, result: res.data.data }, null, 2));
}

else if (action === "append-notes") {
  const text = rest.join(" ").trim();
  if (!text) throw new Error("Notiztext fehlt");

  const task = await asana.get(`/tasks/${taskGid}`, {
    params: { opt_fields: "gid,name,notes" },
  });
  const currentNotes = task.data.data.notes || "";
  const marker = text.split("\n").find((line) => line.startsWith("Update-Key:")) || text.slice(0, 120);
  if (currentNotes.includes(marker)) {
    console.log(JSON.stringify({ ok: true, action, skipped: true, marker, task: task.data.data }, null, 2));
  } else {
    const notes = `${currentNotes.replace(/\s+$/g, "")}\n\n\n${text}`;
    const res = await asana.put(`/tasks/${taskGid}`, {
      data: { notes },
    });
    console.log(JSON.stringify({ ok: true, action, marker, result: res.data.data }, null, 2));
  }
}

else {
  throw new Error(`Unbekannte Aktion: ${action}`);
}
