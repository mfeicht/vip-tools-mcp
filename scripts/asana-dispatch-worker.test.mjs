import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";
import { cleanNoWriteDueTask, documentedDependencyNoWrite, noWriteDisposition } from "./asana-dispatch-worker.mjs";

test("MCP failure before agent start retries and releases both leases", (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "asana-dispatch-worker-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const db = path.join(dir, "store.sqlite");
  const store = new AsanaDispatchStore(db);
  store.enqueue({ id: "preflight-test", agent_id: "vip-ai-research",
    task_gid: "123456", source_version: "preflight-test", kind: "due_task",
    priority: 60, available_at_ms: Date.now() - 1000 });
  store.close();

  const result = spawnSync(process.execPath,
    [path.join(import.meta.dirname, "asana-dispatch-worker.mjs"), `--db=${db}`],
    { env: { ...process.env, WATCHER_MCP_URL: "http://127.0.0.1:1/mcp" },
      encoding: "utf8", timeout: 10_000 });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(JSON.parse(result.stdout).status, "retry_after");

  const after = new AsanaDispatchStore(db);
  assert.deepEqual(after.counts(), { retry_after: 1 });
  assert.deepEqual(after.activeLeases(), []);
  const run = after.db.prepare("SELECT state,thread_id FROM runs LIMIT 1").get();
  assert.equal(run.state, "completed");
  assert.equal(run.thread_id, null);
  after.close();
});

test("documented due-task dependency qualifies for a no-write acknowledgement", () => {
  const startedAt = Date.parse("2026-09-22T05:36:12Z");
  const story = { gid: "999001", resource_subtype: "comment_added",
    created_at: "2026-09-22T03:44:17Z", created_by: { gid: "review-user" },
    text: "Schlussabnahme wartet auf Routine 1218681595288097." };
  const before = { gid: "1218614928369934", completed: false,
    assignee: { gid: "sales-user" }, modified_at: "2026-09-22T03:44:26Z" };
  const input = { claim: { signals: [{ kind: "due_task", story_gid: null }] }, before,
    after: { ...before }, beforeStories: [story], afterStories: [story],
    linkedTask: { gid: "1218681595288097", completed: false,
      modified_at: "2026-09-21T22:04:10Z" },
    answer: { outcome: "blocked", summary: "Die dokumentierte Abhaengigkeit liegt ausschliesslich in der offenen Routine.",
      linked_task_gid: "1218681595288097", evidence_story_gid: "999001" },
    codex: { exitCode: 0, timedOut: false, threadId: "thread-1" },
    ownGid: "sales-user", startedAt };
  assert.equal(cleanNoWriteDueTask(input), true);
  assert.equal(documentedDependencyNoWrite(input), true);
  assert.equal(noWriteDisposition(input), "acknowledged");
  assert.equal(cleanNoWriteDueTask({ ...input,
    claim: { signals: [{ kind: "human_comment", story_gid: "999001" }] } }), false);
  assert.equal(noWriteDisposition({ ...input,
    claim: { signals: [{ kind: "human_comment", story_gid: "999001" }] } }), null);
  assert.equal(cleanNoWriteDueTask({ ...input,
    after: { ...before, modified_at: "2026-09-22T05:40:00Z" } }), false);
  assert.equal(cleanNoWriteDueTask({ ...input,
    codex: { ...input.codex, threadId: null } }), false);
  assert.equal(documentedDependencyNoWrite({ ...input,
    linkedTask: { ...input.linkedTask, completed: true } }), false);
  assert.equal(documentedDependencyNoWrite({ ...input,
    linkedTask: { ...input.linkedTask, modified_at: "2026-09-22T05:39:00Z" } }), false);
  assert.equal(documentedDependencyNoWrite({ ...input,
    answer: { ...input.answer, evidence_story_gid: "999002" } }), false);
  assert.equal(noWriteDisposition({ ...input,
    answer: { ...input.answer, evidence_story_gid: "999002" } }), "dead_letter");
  assert.equal(documentedDependencyNoWrite({ ...input,
    afterStories: [story, { ...story, gid: "999003", created_by: { gid: "sales-user" },
      created_at: "2026-09-22T05:40:00Z" }] }), false);
});

test("unverified clean no-write due task dead-letters without retaining an agent lease", (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "asana-dispatch-no-write-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const store = new AsanaDispatchStore(path.join(dir, "store.sqlite"));
  t.after(() => store.close());
  store.enqueue({ id: "due-1", agent_id: "vip-ai-sales", task_gid: "123456",
    source_version: "due-1", kind: "due_task", priority: 60, available_at_ms: 1000 }, 1000);
  const claim = store.claimNext("run-1", { now: 1000 });
  store.recordRun(claim, { threadId: "thread-1", state: "started", now: 1000 });
  store.settle(claim, { outcome: "dead_letter", error: "Unverified no-write dependency", now: 2000 });
  store.recordRun(claim, { threadId: "thread-1", state: "completed",
    error: "Operations review required", now: 2000 });
  assert.deepEqual(store.counts(), { dead_letter: 1 });
  assert.deepEqual(store.activeLeases(), []);
  assert.equal(store.claimNext("run-2", { now: 3000 }), null);
});
