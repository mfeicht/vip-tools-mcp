import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const port = 31_992;
const child = spawn(process.execPath, ["server.js"], {
  cwd: new URL("..", import.meta.url).pathname,
  env: { ...process.env, PORT: String(port) },
  stdio: ["ignore", "pipe", "pipe"]
});

let stderr = "";
child.stderr.on("data", (chunk) => {
  stderr += chunk.toString("utf8");
});

async function waitForServer() {
  const deadline = Date.now() + 10_000;
  let stdout = "";
  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString("utf8");
  });
  while (!stdout.includes(`Port ${port}`)) {
    if (child.exitCode !== null) throw new Error(`MCP server exited early: ${stderr}`);
    if (Date.now() >= deadline) throw new Error(`MCP server start timeout: ${stderr}`);
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

function parseTextResult(result) {
  const text = result?.content?.find((entry) => entry.type === "text")?.text;
  assert.ok(text, "MCP result is missing text content");
  return JSON.parse(text);
}

const client = new Client({ name: "gemini-image-selftest", version: "1.0.0" });

try {
  await waitForServer();
  const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`));
  await client.connect(transport);

  const listed = await client.listTools();
  const toolNames = new Set((listed.tools || []).map((tool) => tool.name));
  assert.ok(toolNames.has("gemini_image_check_config"));
  assert.ok(toolNames.has("gemini_image_generate"));
  assert.ok(toolNames.has("gemini_image_publish_to_cloudinary"));

  const config = parseTextResult(
    await client.callTool({
      name: "gemini_image_check_config",
      arguments: { agent_id: "vip-ai-design", fetch_models: false }
    })
  );
  assert.equal(config.default_model, "gemini-3.1-flash-image");
  assert.equal(config.fetch_models, false);

  const dryRun = parseTextResult(
    await client.callTool({
      name: "gemini_image_generate",
      arguments: {
        agent_id: "vip-ai-design",
        prompt: "A photorealistic alpine hotel at sunrise",
        upload_to_cloudinary: true,
        cloudinary_project_key: "reise-stories",
        cloudinary_asset_key: "2026-10-02-slide-1",
        dry_run: true
      }
    })
  );
  assert.equal(dryRun.dry_run, true);
  assert.equal(dryRun.payload.model, "gemini-3.1-flash-image");
  assert.equal(dryRun.payload.store, false);
  assert.equal(dryRun.payload.response_format.aspect_ratio, "16:9");
  assert.equal(dryRun.payload.response_format.image_size, "2K");
  assert.equal(dryRun.payload_summary.upload_to_cloudinary, true);
  assert.equal(dryRun.payload_summary.cloudinary_project_key, "reise-stories");
  assert.equal(dryRun.payload_summary.cloudinary_asset_key, "2026-10-02-slide-1");

  console.log("Gemini image MCP self-test passed.");
} finally {
  await client.close().catch(() => undefined);
  child.kill("SIGTERM");
  if (child.exitCode === null) await once(child, "exit").catch(() => undefined);
}
