import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createMcpRequestCleanup } from "../lib/mcp-request-lifecycle.js";

const calls = [];
const cleanup = createMcpRequestCleanup({
  transport: { async close() { calls.push("transport"); } },
  server: { async close() { calls.push("server"); } },
  onClosed({ reason, errors }) {
    calls.push(`closed:${reason}:${errors.length}`);
  }
});

const first = cleanup("response_close");
const second = cleanup("duplicate_close");
assert.equal(first, second);
assert.deepEqual(await first, { reason: "response_close", errors: [] });
assert.deepEqual(calls, ["transport", "server", "closed:response_close:0"]);

const failures = [];
const resilientCleanup = createMcpRequestCleanup({
  transport: { async close() { throw new Error("transport close failed"); } },
  server: { async close() { calls.push("server-after-error"); } },
  onCleanupError({ resource }) { failures.push(resource); }
});
const resilientResult = await resilientCleanup("handler_error");
assert.deepEqual(failures, ["transport"]);
assert.equal(resilientResult.errors.length, 1);
assert.ok(calls.includes("server-after-error"));

const serverSource = readFileSync(new URL("../server.js", import.meta.url), "utf8");
assert.match(serverSource, /app\.post\("\/mcp"/);
assert.match(serverSource, /app\.get\("\/mcp", rejectStatelessMcpStream\)/);
assert.match(serverSource, /app\.delete\("\/mcp", rejectStatelessMcpStream\)/);
assert.doesNotMatch(serverSource, /app\.all\("\/mcp"/);
assert.match(serverSource, /tool_name: toolName/);
assert.match(serverSource, /heap_used_bytes: memory\.heapUsed/);

console.log(JSON.stringify({ mcp_request_lifecycle: "pass", assertions: 13 }));
