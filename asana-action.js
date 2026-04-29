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

const [action, taskGid, ...rest] = process.argv.slice(2);

if (!action || !taskGid) {
  throw new Error("Nutzung: node asana-action.js comment|complete|update TASK_GID ...");
}

if (action === "comment") {
  const text = rest.join(" ");
  if (!text) throw new Error("Kommentartext fehlt");

  const res = await asana.post(`/tasks/${taskGid}/stories`, {
    data: { text },
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

else {
  throw new Error(`Unbekannte Aktion: ${action}`);
}
