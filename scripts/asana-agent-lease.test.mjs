import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";
import { executeLeaseCommand, manualThreadContext } from "./asana-agent-lease.mjs";

const env = { CODEX_THREAD_ID: "01a13d34-b615-7451-b51d-e5d75b31dbcf" };
const other = { CODEX_THREAD_ID: "01a0483a-2d31-7ed1-bfc8-b119a64cb48c" };
function fixture(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "manual-lease-test-"));
  const dbPath = path.join(dir, "state.sqlite");
  const db = new AsanaDispatchStore(dbPath);
  t.after(() => { db.close(); fs.rmSync(dir, { recursive: true, force: true }); });
  const call = (command, args = {}, context = env) => executeLeaseCommand(db, { command, ...args }, context);
  return { db, dbPath, call };
}
const agentArgs = { agent: "vip-ai-test", run: "manual-test-1" };

test("missing or malformed host ID fails closed without acquiring a lease", t => {
  const { db, call } = fixture(t);
  for (const context of [{}, { CODEX_THREAD_ID: "fake" }, { CODEX_SESSION_ID: " " },
    { CODEX_THREAD_ID: "bad", CODEX_SESSION_ID: env.CODEX_THREAD_ID }]) {
    assert.throws(() => call("acquire-agent", agentArgs, context), /Real CODEX/);
  }
  assert.equal(db.activeLeases().length, 0);
  assert.equal(db.manualRun(agentArgs.run), null);
});

test("actual primary ID wins; missing primary uses only actual session ID", t => {
  const { db, call } = fixture(t);
  assert.deepEqual(manualThreadContext({ ...env, CODEX_SESSION_ID: other.CODEX_THREAD_ID }),
    { threadId: env.CODEX_THREAD_ID, source: "CODEX_THREAD_ID" });
  const result = call("acquire-agent", agentArgs, { CODEX_SESSION_ID: env.CODEX_THREAD_ID });
  assert.equal(result.acquired, true);
  assert.equal(db.manualRun(result.run_id).thread_id_source, "CODEX_SESSION_ID");
});

test("agent and multiple task bindings persist across database connections and release", t => {
  const { db, dbPath, call } = fixture(t);
  const { run_id: run } = call("acquire-agent", agentArgs);
  for (const task of ["123", "456"]) assert.equal(call("acquire-task", { run, task }).acquired, true);
  const reader = new AsanaDispatchStore(dbPath);
  try {
    assert.equal(reader.manualRun(run).thread_id, env.CODEX_THREAD_ID);
    assert.deepEqual(reader.db.prepare("SELECT task_gid FROM manual_run_tasks ORDER BY task_gid")
      .all().map(x => x.task_gid), ["123", "456"]);
  } finally { reader.close(); }
  assert.equal(call("renew", { run }).renewed, 3);
  assert.equal(call("release", { run }).released, 3);
  assert.equal(db.manualRun(run).state, "released");
  assert.equal(db.db.prepare("SELECT count(*) AS n FROM manual_run_tasks").get().n, 2);
  assert.equal(call("release", { run }).released, 0);
  assert.throws(() => call("acquire-agent", agentArgs), /released/);
});

test("foreign thread cannot adopt, attach tasks, renew or release a bound run", t => {
  const { db, call } = fixture(t);
  const { run_id: run } = call("acquire-agent", agentArgs);
  const before = JSON.stringify(db.activeLeases());
  for (const [command, args] of [["acquire-agent", agentArgs], ["acquire-task", { run, task: "123" }],
    ["renew", { run }], ["release", { run }]]) {
    assert.throws(() => call(command, args, other), /another thread/);
    assert.throws(() => call(command, args, {}), /Real CODEX/);
  }
  assert.equal(JSON.stringify(db.activeLeases()), before);
});

test("busy and expired foreign agent leases create no phantom binding", t => {
  const { db, call } = fixture(t);
  db.acquireLease("agent:vip-ai-test", "legacy-run", 60_000, 1);
  const before = JSON.stringify(db.activeLeases());
  assert.equal(call("acquire-agent", agentArgs).acquired, false);
  assert.equal(db.manualRun(agentArgs.run), null);
  assert.equal(JSON.stringify(db.activeLeases()), before);
});

test("failed task acquisition adds no claimed task to the manual run", t => {
  const { db, call } = fixture(t);
  const { run_id: run } = call("acquire-agent", agentArgs);
  db.acquireLease("task:123", "foreign-run", 60_000);
  assert.equal(call("acquire-task", { run, task: "123" }).acquired, false);
  assert.equal(db.db.prepare("SELECT count(*) AS n FROM manual_run_tasks").get().n, 0);
});

test("binding insert failure rolls back agent lease and fence together", t => {
  const { db, call } = fixture(t);
  db.db.exec(`CREATE TRIGGER fail_binding BEFORE INSERT ON manual_runs
    BEGIN SELECT RAISE(ABORT, 'fixture binding failure'); END;`);
  assert.throws(() => call("acquire-agent", agentArgs), /fixture binding failure/);
  assert.equal(db.activeLeases().length, 0);
  assert.equal(db.db.prepare("SELECT count(*) AS n FROM lease_fences").get().n, 0);
});

test("task binding insert failure rolls back only the new task transaction", t => {
  const { db, call } = fixture(t);
  const { run_id: run } = call("acquire-agent", agentArgs);
  const before = JSON.stringify(db.activeLeases());
  db.db.exec(`CREATE TRIGGER fail_task_binding BEFORE INSERT ON manual_run_tasks
    BEGIN SELECT RAISE(ABORT, 'fixture task binding failure'); END;`);
  assert.throws(() => call("acquire-task", { run, task: "123" }), /fixture task binding failure/);
  assert.equal(JSON.stringify(db.activeLeases()), before);
  assert.equal(db.db.prepare("SELECT * FROM lease_fences WHERE key='task:123'").get(), undefined);
});

test("existing legacy run cannot be retroactively adopted, but its lifecycle stays compatible", t => {
  const { db, call } = fixture(t);
  db.acquireLease("agent:vip-ai-test", agentArgs.run, 60_000);
  assert.throws(() => call("acquire-agent", agentArgs), /cannot be adopted/);
  const result = call("acquire-task", { run: agentArgs.run, task: "123" }, {});
  assert.equal(result.legacy_unbound, true);
  assert.equal(result.acquired, true);
  assert.equal(db.manualRun(agentArgs.run), null);
  assert.equal(call("renew", { run: agentArgs.run }, {}).renewed, 2);
  assert.equal(call("release", { run: agentArgs.run }, {}).released, 2);
});

test("dispatcher run collision and dispatch environment cannot create manual binding", t => {
  const { db, call } = fixture(t);
  db.db.prepare(`INSERT INTO runs
    (run_id,agent_id,task_gid,state,started_at_ms,updated_at_ms)
    VALUES (?,'vip-ai-test','123','completed',1,2)`).run(agentArgs.run);
  assert.throws(() => call("acquire-agent", agentArgs), /cannot be adopted/);
  assert.throws(() => call("acquire-agent", { agent: "vip-ai-other" },
    { ...env, VIP_ASANA_DISPATCH_RUN_ID: "worker-run" }), /already owns/);
  assert.equal(db.activeLeases().length, 0);
});

test("agent fence mismatch blocks all bound task and lifecycle changes", t => {
  const { db, call } = fixture(t);
  const { run_id: run } = call("acquire-agent", agentArgs);
  db.db.prepare("UPDATE leases SET fence=fence+1 WHERE run_id=?").run(run);
  for (const [command, args] of [["acquire-task", { run, task: "123" }], ["renew", { run }],
    ["release", { run }]]) assert.throws(() => call(command, args), /fence changed/);
  assert.equal(db.activeLeases().length, 1);
});

test("additive migration preserves all legacy lease and dispatcher rows", t => {
  const { db, dbPath } = fixture(t);
  db.acquireLease("agent:vip-ai-finance", "active-finance", 60_000, 100);
  db.db.prepare(`INSERT INTO runs
    (run_id,agent_id,task_gid,state,started_at_ms,updated_at_ms)
    VALUES ('dispatcher-old','vip-ai-finance','123','needs_reconciliation',1,2)`).run();
  const before = JSON.stringify({
    leases: db.db.prepare("SELECT * FROM leases").all(),
    runs: db.db.prepare("SELECT * FROM runs").all(),
  });
  db.db.exec("DROP TABLE manual_run_tasks; DROP TABLE manual_runs;");
  const reopened = new AsanaDispatchStore(dbPath);
  try {
    assert.equal(JSON.stringify({
      leases: reopened.db.prepare("SELECT * FROM leases").all(),
      runs: reopened.db.prepare("SELECT * FROM runs").all(),
    }), before);
    assert.equal(reopened.db.prepare("SELECT count(*) AS n FROM manual_runs").get().n, 0);
  } finally { reopened.close(); }
});
