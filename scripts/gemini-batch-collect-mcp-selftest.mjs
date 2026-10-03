import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import http from "node:http";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const providerPort = 31_993;
const mcpPort = 31_994;
const image = Buffer.alloc(1024 * 1024, 0x41);
image.set([
  0xff, 0xd8, 0xff, 0xe0, 0x00, 0x04, 0x00, 0x00,
  0xff, 0xc0, 0x00, 0x11, 0x08, 0x09, 0x00, 0x07, 0x40,
  0x03, 0x01, 0x11, 0x00, 0x02, 0x11, 0x00, 0x03, 0x11, 0x00
], 0);
image.set([0xff, 0xd9], image.length - 2);
const base64 = image.toString("base64");
const results = Array.from({ length: 6 }, (_, i) => ({
  metadata: {
    key: `tradingpulse-2026-10-03-r5-s${i + 1}-bg`,
    project_key: "tradingpulse",
    target_publish_date: "2026-10-03",
    aspect_ratio: "4:5",
    image_size: "2K",
    slide: i + 1,
    file_name: `slide-${i + 1}.jpg`
  },
  response: { candidates: [{ content: { parts: [{ inlineData: { mimeType: "image/jpeg", data: base64 } }] } }] }
}));
const providerBody = JSON.stringify({
  name: "batches/streamcollecttest",
  state: "JOB_STATE_SUCCEEDED",
  batchStats: { successfulRequestCount: 6, failedRequestCount: 0 },
  dest: { inlinedResponses: { inlinedResponses: results } },
  output: { inlinedResponses: [{ metadata: { key: "duplicate-output" } }] }
});
const provider = http.createServer((req, res) => {
  if (req.url !== "/v1beta/batches/streamcollecttest") {
    res.writeHead(404).end();
    return;
  }
  res.writeHead(200, { "content-type": "application/json" });
  for (let offset = 0; offset < providerBody.length; offset += 8192) {
    res.write(providerBody.slice(offset, offset + 8192));
  }
  res.end();
});
await new Promise((resolve) => provider.listen(providerPort, "127.0.0.1", resolve));

const child = spawn(process.execPath, ["server.js"], {
  cwd: new URL("..", import.meta.url).pathname,
  env: {
    ...process.env,
    PORT: String(mcpPort),
    GEMINI_API_BASE: `http://127.0.0.1:${providerPort}/v1beta`,
    GEMINI_API_KEY_VIP_AI_SOCIAL_MEDIA: "local-test-key"
  },
  stdio: ["ignore", "pipe", "pipe"]
});
let stdout = "";
let stderr = "";
child.stdout.on("data", (chunk) => { stdout += chunk.toString("utf8"); });
child.stderr.on("data", (chunk) => { stderr += chunk.toString("utf8"); });
const client = new Client({ name: "gemini-batch-collect-selftest", version: "1.0.0" });
try {
  const deadline = Date.now() + 10_000;
  while (!stdout.includes(`Port ${mcpPort}`)) {
    if (child.exitCode !== null) throw new Error(`MCP server exited: ${stderr}`);
    if (Date.now() >= deadline) throw new Error(`MCP start timeout: ${stderr}`);
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  await client.connect(new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${mcpPort}/mcp`)));
  const response = await client.callTool({
    name: "gemini_image_batch_collect",
    arguments: {
      agent_id: "vip-ai-social-media",
      batch_name: "batches/streamcollecttest",
      project_key: "tradingpulse",
      target_publish_date: "2026-10-03",
      dry_run: true
    }
  });
  const body = JSON.parse(response.content.find((entry) => entry.type === "text")?.text || "null");
  assert.equal(response.isError, undefined, JSON.stringify(body));
  assert.equal(body.dry_run, true);
  assert.equal(body.result_count, 6);
  assert.equal(body.ready_count, 6);
  assert.equal(body.results.length, 6);
  assert.equal(body.results[0].request_key, "tradingpulse-2026-10-03-r5-s1-bg");
  assert.equal(body.results[5].request_key, "tradingpulse-2026-10-03-r5-s6-bg");
  assert.ok(body.results.every((entry) => entry.bytes === image.length && entry.mime_type === "image/jpeg"));
  assert.ok(body.results.every((entry) => entry.dimensions.width === 1856 && entry.dimensions.height === 2304));
  console.log("Gemini six-result collect MCP dry-run passed.");
} finally {
  await client.close().catch(() => undefined);
  child.kill("SIGTERM");
  if (child.exitCode === null) await once(child, "exit").catch(() => undefined);
  await new Promise((resolve) => provider.close(resolve));
}
