import assert from "node:assert/strict";
import test from "node:test";
import { readManualNative, readManualLeaseDecision } from "./asana-manual-lease-readback.mjs";
import { processScanBinding } from "./asana-worker-process-readback.mjs";

const now = Date.now();
const run = { run_id: "manual-test", agent_id: "vip-ai-operations",
  thread_id: "thread-1", state: "active", acquired_at_ms: now - 30_000,
  agent_lease_token: "agent-token", agent_lease_fence: 1, agent_fence: 1,
  tasks: [{ task_gid: "123", acquired_at_ms: now - 29_000,
    lease_token: "task-token", lease_fence: 1, bound_lease_fence: 1 }] };

function rpc(overrides = {}) {
  return { async call(method) {
    if (method === "thread/read") return { thread: { id: run.thread_id,
      status: { type: "idle" } } };
    if (method === "thread/turns/list") return { data: [{ id: "turn-1",
      startedAt: (run.acquired_at_ms - 1000) / 1000,
      completedAt: (now - 20_000) / 1000, status: "completed" }], nextCursor: null };
    if (method === "thread/items/list") return { data: [{ item: { id: "item-1",
      type: "agentMessage", text: "Finished" } }], nextCursor: null };
    throw new Error(`Unexpected ${method}`);
  }, ...overrides };
}

test("native collector binds one complete turn and hashes its activity", async () => {
  const row = await readManualNative(run, rpc());
  assert.equal(row.turn_id, "turn-1");
  assert.equal(row.activity_item_count, 1);
  assert.match(row.activity_digest, /^[a-f0-9]{64}$/);
  await assert.rejects(readManualNative(run, rpc({ call: async (method) =>
    method === "thread/items/list" ? { data: [], nextCursor: "more" } :
      rpc().call(method) })), /pagination incomplete|repeated cursor|activity items incomplete/);
});

test("read-only driver takes two native and scoped Asana reads before deciding", async () => {
  const calls = [];
  const client = { async callTool({ name, arguments: args }) {
    calls.push([name, args.path, args.agent_id]);
    return { content: [{ type: "text", text: JSON.stringify({ response: {
      data: { gid: "user-1" } } }) }] };
  } };
  let nativeCount = 0;
  let taskCount = 0;
  const result = await readManualLeaseDecision(run, { rpc: rpc(), client,
    readNative: async () => { nativeCount += 1; return {
      observed_at_ms: now - 2500 + nativeCount * 1500,
      source: "codex_app.read_thread", host_id: "local", thread_id: run.thread_id,
      runtime_status: "idle", turn_id: "turn-1", turn_status: "completed",
      started_at_ms: run.acquired_at_ms - 1000, completed_at_ms: now - 20_000,
      activity_digest: "a".repeat(64) }; },
    readAsana: async () => { taskCount += 1; return {
      observed_at_ms: now - 2500 + taskCount * 1500,
      agent_id: run.agent_id, task: { gid: "123", completed: true,
        modified_at: new Date(now - 20_000).toISOString() },
      stories: [], next_page: null }; },
    scan: () => ({ ...processScanBinding(run), observed_at_ms: now - 100,
      raw_sha256: "b".repeat(64), scan_succeeded: true, matching_processes: [] }),
    wait: async () => {}, now: () => now });
  assert.equal(result.decision.action, "release");
  assert.equal(nativeCount, 2);
  assert.equal(taskCount, 2);
  assert.deepEqual(calls, [["asana_request", "/users/me", run.agent_id]]);
});
