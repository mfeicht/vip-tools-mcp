import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import test from "node:test";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";
import { quarantineFromEvidence as executeQuarantine } from "./asana-terminal-quarantine.mjs";
import { processScanBinding, matchingWorkerProcesses } from "./asana-worker-process-readback.mjs";
import { reconcileCompletedRuns } from "./asana-dispatch-reconcile.mjs";

const NOW = 2_000_000;
const quarantineFromEvidence = (store, evidence, options) => executeQuarantine(store, evidence, {
  processScanner: () => evidence.process_readback, ...options
});
function fixture(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "terminal-quarantine-"));
  const store = new AsanaDispatchStore(path.join(dir, "fixture.sqlite"));
  t.after(() => { store.close(); fs.rmSync(dir, { recursive: true, force: true }); });
  store.enqueue({ id: "signal-1", agent_id: "vip-ai-finance", task_gid: "12345",
    source_version: "version-1", kind: "continuation" }, 1000);
  const claim = store.claimNext("old-run", { now: 1000 });
  store.recordRun(claim, { threadId: "thread-old", state: "started", now: 1000 });
  const side = path.join(dir, "partial.json");
  fs.writeFileSync(side, '{"partial":true}');
  const task = { gid: "12345", completed: false, modified_at: "1970-01-01T00:00:00Z",
    assignee: { gid: "finance-user" } };
  const native = { source: "codex_app.read_thread", host_id: "local",
    thread_id: "thread-old", runtime_status: "notLoaded", turn_id: "turn-old",
    turn_status: "interrupted", started_at_ms: 1100, completed_at_ms: null,
    activity_digest: "b".repeat(64), last_event_at_ms: 1200 };
  const asana = { agent_id: "vip-ai-operations", task, stories: [], next_page: null };
  const evidence = { schema: "asana-native-terminal-quarantine-v1",
    expected_run: { ...store.db.prepare("SELECT * FROM runs WHERE run_id=?").get("old-run") },
    expected_leases: store.leasesForRun("old-run").map((r) => ({ ...r })),
    expected_signals: store.db.prepare("SELECT * FROM signals WHERE run_id=? ORDER BY id").all("old-run").map((r) => ({ ...r })),
    native_readbacks: [{ ...native, observed_at_ms: NOW - 2000 }, { ...native, observed_at_ms: NOW - 1000 }],
    asana_readbacks: [{ ...asana, observed_at_ms: NOW - 2000 }, { ...asana, observed_at_ms: NOW - 1000 }],
    process_readback: { ...processScanBinding({ run_id: "old-run", agent_id: "vip-ai-finance", thread_id: "thread-old" }),
      raw_sha256: "a".repeat(64), observed_at_ms: NOW - 500, scan_succeeded: true, matching_processes: [] },
    side_effects: [{ path: side, sha256: createHash("sha256").update(fs.readFileSync(side)).digest("hex") }] };
  return { store, evidence };
}

test("native interrupted without completedAt releases only agent and preserves task, signals and partial files", (t) => {
  const { store, evidence } = fixture(t);
  assert.equal(quarantineFromEvidence(store, evidence, { now: NOW }).status, "review_ready");
  assert.equal(store.activeLeases().length, 2);
  const result = quarantineFromEvidence(store, evidence, { apply: true, now: NOW });
  assert.equal(result.agent_released, true);
  assert.deepEqual(store.leasesForRun("old-run").map((r) => ({ ...r })),
    evidence.expected_leases.filter((r) => r.key.startsWith("task:")));
  assert.deepEqual(store.db.prepare("SELECT * FROM signals WHERE run_id=? ORDER BY id").all("old-run").map((r) => ({ ...r })),
    evidence.expected_signals);
  assert.equal(fs.readFileSync(evidence.side_effects[0].path, "utf8"), '{"partial":true}');
  assert.equal(store.runsNeedingReconciliation(NOW)[0].task_only_quarantine, 1);
  assert.equal(store.claimNext("new-same-task", { now: NOW }), null);
  store.enqueue({ id: "other", agent_id: "vip-ai-finance", task_gid: "54321",
    source_version: "other", kind: "due_task" }, NOW);
  assert.equal(store.claimNext("new-other-task", { now: NOW }).signals[0].task_gid, "54321");
});

for (const [name, change] of [
  ["active runtime", (e) => { e.native_readbacks[1].runtime_status = "active"; }],
  ["different turn", (e) => { e.native_readbacks[1].turn_id = "another"; }],
  ["unfinished turn", (e) => { e.native_readbacks[1].turn_status = "inProgress"; }],
  ["new events behind interrupted display", (e) => { e.native_readbacks[1].activity_digest = "c".repeat(64); }],
  ["advanced event timestamp", (e) => { e.native_readbacks[1].last_event_at_ms += 100; }],
  ["missing activity evidence", (e) => { delete e.native_readbacks[0].activity_digest; }],
  ["stale observation", (e) => { e.native_readbacks[0].observed_at_ms = 1; }],
  ["Asana changed", (e) => { e.asana_readbacks[1].task = { ...e.asana_readbacks[1].task, completed: true }; }],
  ["incomplete stories", (e) => { e.asana_readbacks[1].next_page = { offset: "more" }; }],
  ["running worker", (e) => { e.process_readback.matching_processes = [{ pid: 42 }]; }],
  ["foreign process host", (e) => { e.process_readback.host_id = "remote-other"; }],
  ["foreign hostname", (e) => { e.process_readback.hostname = "other-host"; }],
  ["foreign process run", (e) => { e.process_readback.run_id = "unrelated-run"; }],
  ["foreign process agent", (e) => { e.process_readback.agent_id = "vip-ai-sales"; }],
  ["foreign process thread", (e) => { e.process_readback.thread_id = "different-thread"; }],
  ["arbitrary scan expression", (e) => { e.process_readback.match_expression = "unrelated"; }],
  ["changed scan policy", (e) => { e.process_readback.scan_policy = "different"; }],
  ["changed scanner bytes", (e) => { e.process_readback.scanner_sha256 = "0".repeat(64); }],
  ["different process executable", (e) => { e.process_readback.executable = "/bin/true"; }],
  ["different process arguments", (e) => { e.process_readback.args = []; }],
  ["missing process digest", (e) => { delete e.process_readback.raw_sha256; }],
  ["completion before start", (e) => { e.native_readbacks.forEach((r) => { r.completed_at_ms = 100; }); }],
  ["side effect changed", (e) => { fs.writeFileSync(e.side_effects[0].path, "changed"); }]
]) {
  test(`rejects ${name} without releasing a lease`, (t) => {
    const { store, evidence } = fixture(t); change(evidence);
    assert.throws(() => quarantineFromEvidence(store, evidence, { apply: true, now: NOW }));
    assert.equal(store.activeLeases().length, 2);
  });
}
test("renewal after readback fails the complete lease fence", (t) => {
  const { store, evidence } = fixture(t);
  store.acquireLease("agent:vip-ai-finance", "old-run", 60_000, NOW);
  assert.throws(() => quarantineFromEvidence(store, evidence, { apply: true, now: NOW }), /differ/);
  assert.equal(store.activeLeases().length, 2);
});

test("fresh rescan rejects a worker appearing after the reviewed evidence", (t) => {
  const { store, evidence } = fixture(t);
  assert.throws(() => quarantineFromEvidence(store, evidence, { now: NOW, apply: true,
    processScanner: () => ({ ...evidence.process_readback, matching_processes: [{ pid: 42 }] })
  }), /inactivity/);
  assert.equal(store.activeLeases().length, 2);
});

test("process scan failure cannot release leases", (t) => {
  const { store, evidence } = fixture(t);
  assert.throws(() => quarantineFromEvidence(store, evidence, { now: NOW, apply: true,
    processScanner: () => { throw new Error("ps denied"); }
  }), /ps denied/);
  assert.equal(store.activeLeases().length, 2);
});

test("fixed process parser finds workers and exact identities without exposing commands", () => {
  const run = { run_id: "old-run", thread_id: "thread-old" };
  const matches = matchingWorkerProcesses(
    "1 /Applications/App/codex exec --json\n2 node /repo/asana-dispatch-worker.mjs\n" +
    "3 node custom.mjs --run=old-run\n4 worker --thread=thread-old\n5 /bin/ps -axo pid=,command=", run);
  assert.deepEqual(matches.map((r) => r.pid), [1, 2, 3, 4]);
  assert.ok(matches.every((r) => !("command" in r)));
  assert.throws(() => matchingWorkerProcesses("", run), /Incomplete/);
});
test("partial transaction failure rolls back run metadata and agent deletion", (t) => {
  const { store, evidence } = fixture(t);
  const release = store.releaseLease.bind(store);
  store.releaseLease = (lease) => { release(lease); throw new Error("fixture after delete"); };
  assert.throws(() => quarantineFromEvidence(store, evidence, { apply: true, now: NOW }), /fixture/);
  assert.equal(store.activeLeases().length, 2);
  assert.deepEqual({ ...store.db.prepare("SELECT * FROM runs WHERE run_id=?").get("old-run") }, evidence.expected_run);
});
test("automatic Asana-only reconciler cannot unlock an operator-quarantined task", async (t) => {
  const { store, evidence } = fixture(t);
  quarantineFromEvidence(store, evidence, { apply: true, now: NOW });
  const result = await reconcileCompletedRuns(store, {
    readTask: async () => ({ gid: "12345", completed: true }),
    archive: async () => { throw new Error("must not archive"); }
  });
  assert.equal(result.task_quarantined, 1);
  assert.equal(result.acknowledged, 0);
  assert.equal(result.errors.length, 0);
  assert.equal(store.leasesForRun("old-run").length, 1);
  assert.deepEqual(store.counts(), { leased: 1 });
});
