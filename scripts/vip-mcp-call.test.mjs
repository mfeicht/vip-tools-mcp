import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { StreamableHTTPError } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { parseArgs, classifyConnectionError, isTransientConnectionError,
  connectLockReclaimable, callToolOnce } from "./vip-mcp-call.mjs";

test("CLI cannot expand the canonical retry budget or accept malformed numbers", () => {
  for (const value of ["4", "999", "3junk", "1.5", "Infinity", "0"]) {
    assert.throws(() => parseArgs(["--list-tools", "--attempts", value]));
  }
  assert.equal(parseArgs(["--list-tools", "--attempts", "3"]).attempts, 3);
  assert.throws(() => parseArgs(["--list-tools", "--delay-ms", "60001"]));
  assert.throws(() => parseArgs(["--list-tools", "--endpoint", "https://other.invalid"]));
});

test("authorization failures are not transient while 503 and challenges remain distinct", () => {
  for (const code of [401, 403]) {
    const error = new Error(`Streamable HTTP error: HTTP ${code}`);
    assert.equal(classifyConnectionError(error), "remote_auth_or_permission_error");
    assert.equal(isTransientConnectionError(error), false);
  }
  assert.equal(isTransientConnectionError(new Error("Streamable HTTP error: HTTP 503")), true);
  assert.equal(classifyConnectionError(new Error("HTTP 403 <title>Just a moment</title>")),
    "cloudflare_managed_challenge");
});

test("real SDK status codes with empty response bodies retain retry and cooldown semantics", () => {
  for (const code of [502, 503, 504]) {
    assert.equal(isTransientConnectionError(new StreamableHTTPError(code, "Error POSTing to endpoint: ")), true);
  }
  for (const code of [401, 403]) {
    assert.equal(classifyConnectionError(new StreamableHTTPError(code, "")), "remote_auth_or_permission_error");
  }
  assert.equal(classifyConnectionError(new StreamableHTTPError(429, "")), "remote_rate_limited");
  assert.equal(classifyConnectionError(new StreamableHTTPError(403, "Just a moment")),
    "cloudflare_managed_challenge");
  assert.equal(isTransientConnectionError(new StreamableHTTPError(404, "")), false);
});

test("a living or newly initializing connect holder is never reclaimed by age alone", () => {
  assert.equal(connectLockReclaimable({ pid: 123 }, 600000, () => true), false);
  assert.equal(connectLockReclaimable({ pid: 123 }, 10, () => false), true);
  assert.equal(connectLockReclaimable({}, 5), false);
  assert.equal(connectLockReclaimable({}, 300000), true);
});

test("post-connect throttling records a failure but never replays a mutation", async () => {
  let calls = 0;
  const recorded = [];
  const error = new Error("HTTP 429 Too Many Requests");
  const client = { callTool: async () => { calls++; throw error; } };
  await assert.rejects(callToolOnce(client, "asana_comment", {}, 1000,
    async (err, tool) => recorded.push({ type: classifyConnectionError(err), tool })), error);
  assert.equal(calls, 1);
  assert.deepEqual(recorded, [{ type: "remote_rate_limited", tool: "asana_comment" }]);
});

test("unknown tool timeout and failed telemetry still never replay", async () => {
  let calls = 0;
  await assert.rejects(callToolOnce({ callTool: () => { calls++; return new Promise(() => {}); } },
    "buffer_schedule_post", {}, 5, async () => { throw new Error("state unavailable"); }), /timeout/);
  assert.equal(calls, 1);
});

test("post-connect 429 persists the real shared cooldown without touching host state", (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "vip-mcp-cooldown-test-"));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const code = `
    import { callToolOnce } from ${JSON.stringify(new URL("./vip-mcp-call.mjs", import.meta.url).href)};
    let calls = 0;
    try { await callToolOnce({callTool: async () => { calls++; throw new Error("HTTP 429"); }},
      "asana_comment", {}, 1000); } catch {}
    if (calls !== 1) process.exit(1);
  `;
  const before = Date.now();
  const result = spawnSync(process.execPath, ["--input-type=module", "-"],
    { input: code, env: { ...process.env, VIP_MCP_COORDINATION_DIR: directory },
      encoding: "utf8", timeout: 10000 });
  assert.equal(result.status, 0, result.stderr);
  const state = JSON.parse(fs.readFileSync(path.join(directory, "state.json"), "utf8"));
  assert.equal(state.lastToolErrorClass, "remote_rate_limited");
  assert.ok(Date.parse(state.cooldownUntil) >= before + 60000);
  assert.equal(fs.existsSync(path.join(directory, "connect.lock")), false);
});
