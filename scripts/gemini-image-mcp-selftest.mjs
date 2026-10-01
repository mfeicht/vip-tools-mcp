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
  assert.ok(toolNames.has("gemini_image_batch_submit"));
  assert.ok(toolNames.has("gemini_image_batch_status"));
  assert.ok(toolNames.has("gemini_image_batch_collect"));
  assert.ok(toolNames.has("gemini_image_batch_cancel"));
  assert.ok(toolNames.has("gemini_image_cleanup_generated_assets"));

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

  const batchDryRun = parseTextResult(
    await client.callTool({
      name: "gemini_image_batch_submit",
      arguments: {
        agent_id: "vip-ai-social-media",
        project_key: "tradingpulse",
        target_publish_date: "2026-10-02",
        idempotency_key: "tradingpulse-2026-10-02-test",
        model: "gemini-3-pro-image",
        requests: [
          {
            request_key: "cover",
            prompt: "A premium editorial finance photograph without text",
            aspect_ratio: "4:5",
            image_size: "2K",
            file_name: "tradingpulse-cover.jpg",
            slide: 1
          }
        ],
        dry_run: true
      }
    })
  );
  assert.equal(batchDryRun.dry_run, true);
  assert.equal(batchDryRun.model, "gemini-3-pro-image");
  assert.equal(batchDryRun.request_count, 1);
  assert.equal(batchDryRun.requests[0].request_key, "cover");
  assert.equal(batchDryRun.requests[0].image_size, "2K");

  console.log("Gemini image MCP self-test passed.");
} finally {
  await client.close().catch(() => undefined);
  child.kill("SIGTERM");
  if (child.exitCode === null) await once(child, "exit").catch(() => undefined);
}
