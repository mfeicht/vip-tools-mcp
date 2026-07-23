import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const client = new Client({ name: "vip-ai-content-standard-intake", version: "1.0.0" });
const transport = new StreamableHTTPClientTransport(new URL(process.env.CONTENT_MCP_URL || "http://127.0.0.1:3000/mcp"));
await client.connect(transport);
const tools = await client.listTools();
const wanted = [
  "asana_whoami", "asana_get_my_tasks", "asana_search_tasks", "asana_get_task", "asana_get_task_stories",
  "asana_attachment_list", "asana_comment", "agent_email_read_unseen", "agent_lock_status", "google_drive_check_config",
  "dataforseo_check_config", "templated_check_config"
];
const present = tools.tools.filter((tool) => wanted.includes(tool.name)).map((tool) => ({ name: tool.name, inputSchema: tool.inputSchema }));
console.log(JSON.stringify({ tool_names: tools.tools.map((t) => t.name).filter((n) => /asana|email|lock|drive|dataforseo|templated/i.test(n)), selected_schemas: present }, null, 2));

async function call(name, args) {
  const result = await client.callTool({ name, arguments: args });
  const text = (result.content || []).filter((item) => item.type === "text").map((item) => item.text).join("\n");
  return { isError: result.isError || false, structuredContent: result.structuredContent ?? null, text };
}

const agent_id = "vip-ai-content";
const out = {};
out.whoami = await call("asana_whoami", { agent_id });
out.my_tasks = await call("asana_get_my_tasks", {
  agent_id,
  completed_since: "now",
  limit: 50,
  opt_fields: "gid,name,completed,due_on,due_at,permalink_url,assignee.gid,assignee.name,followers.gid,followers.name,tags.name,projects.name,projects.gid,modified_at,created_at"
});
out.email = await call("agent_email_read_unseen", { agent_id });
out.locks = await call("agent_lock_status", { agent_id });
out.drive = await call("google_drive_check_config", { agent_id });
out.dataforseo = await call("dataforseo_check_config", { agent_id });
out.templated = await call("templated_check_config", { agent_id });
console.log(JSON.stringify(out, null, 2));
await transport.close();
