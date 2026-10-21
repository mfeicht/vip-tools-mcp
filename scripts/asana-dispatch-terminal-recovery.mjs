import { createHash } from "node:crypto";
import { readDispatchNative, readManualAsanaTask, boundedReadClients } from "./asana-manual-lease-readback.mjs";
import { collectWorkerProcessReadback, processScanBinding } from "./asana-worker-process-readback.mjs";

const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const digest = value => createHash("sha256").update(JSON.stringify(value)).digest("hex");

export function dispatchRecoverySnapshot(store, runId) {
  return {
    run: store.db.prepare("SELECT * FROM runs WHERE run_id=?").get(runId),
    leases: store.leasesForRun(runId),
    signals: store.db.prepare("SELECT * FROM signals WHERE run_id=? ORDER BY id").all(runId)
  };
}

export async function readDispatchRecovery(snapshot, { rpc, client,
  readNative = readDispatchNative, readTask = readManualAsanaTask, wait = delay } = {}) {
  const { run } = snapshot;
  ({rpc,client}=boundedReadClients(rpc,client));
  const native = [], asana = [];
  for (let pass = 0; pass < 2; pass++) {
    if (pass) await wait(1100);
    native.push(await readNative(run, rpc));
    asana.push(await readTask(run, { task_gid: run.task_gid }, client));
  }
  return { native, asana };
}

export function decideDispatchRecovery(snapshot, proof, scan, now = Date.now()) {
  const hold = reason => ({ action: "hold", reason });
  const { run, leases, signals } = snapshot;
  if (!run?.thread_id || !["started", "needs_reconciliation"].includes(run.state)) {
    return hold("run_not_recoverable");
  }
  const agent = leases.find(lease => lease.key === `agent:${run.agent_id}`);
  const task = leases.find(lease => lease.key === `task:${run.task_gid}`);
  if (!task || leases.length !== (agent ? 2 : 1) || !signals.length || signals.some(signal =>
    signal.status !== "leased" || signal.run_id !== run.run_id || signal.agent_id !== run.agent_id ||
    signal.task_gid !== run.task_gid || signal.lease_token !== task.token || signal.lease_fence !== task.fence)) {
    return hold("claim_incomplete_or_extra_leases");
  }
  const fresh = row => Number.isSafeInteger(row?.observed_at_ms) &&
    row.observed_at_ms <= now + 1000 && now - row.observed_at_ms <= 300000;
  const native = proof?.native;
  if (!Array.isArray(native) || native.length !== 2 || native.some(row =>
    !fresh(row) || row.source !== "codex_app.read_thread" || row.host_id !== "local" ||
    row.thread_id !== run.thread_id || !["idle", "notLoaded"].includes(row.runtime_status) ||
    !["completed", "failed", "interrupted"].includes(row.turn_status) || !row.turn_id ||
    (run.turn_id && row.turn_id !== run.turn_id) ||
    !Number.isSafeInteger(row.started_at_ms) || row.started_at_ms < run.started_at_ms - 2000 ||
    // No general adoption of the narrow legacy exception for missing end/event times.
    !Number.isSafeInteger(row.completed_at_ms) || row.completed_at_ms < row.started_at_ms ||
    row.completed_at_ms > row.observed_at_ms ||
    !Number.isSafeInteger(row.activity_item_count) || row.activity_item_count < 1 ||
    !row.activity_last_item_id || !/^[a-f0-9]{64}$/.test(row.activity_digest || "")) ||
    native[1].observed_at_ms - native[0].observed_at_ms < 1000 ||
    !same({...native[0], observed_at_ms:null}, {...native[1], observed_at_ms:null})) {
    return hold("native_terminal_unverified");
  }
  if (!fresh(scan) || !scan.scan_succeeded ||
    Object.entries(processScanBinding(run)).some(([key,value]) => !same(scan[key],value)) ||
    !/^[a-f0-9]{64}$/.test(scan.raw_sha256 || "") ||
    !Array.isArray(scan.matching_processes) || scan.matching_processes.length) {
    return hold("worker_inactivity_unverified");
  }
  const asana = proof?.asana;
  if (!Array.isArray(asana) || asana.length !== 2 || asana.some(row =>
    !fresh(row) || row.agent_id !== run.agent_id || row.task?.gid !== run.task_gid ||
    typeof row.task.completed !== "boolean" || !Number.isFinite(Date.parse(row.task.modified_at)) ||
    !Array.isArray(row.stories) || row.next_page !== null) ||
    asana[1].observed_at_ms - asana[0].observed_at_ms < 1000 ||
    !same({...asana[0], observed_at_ms:null}, {...asana[1], observed_at_ms:null})) {
    return hold("asana_readback_unverified");
  }
  return { action: asana[1].task.completed ? "acknowledge" : "quarantine",
    release_agent: Boolean(agent), turn_id: native[1].turn_id };
}

// Local transition only; no Asana writes, thread archive or worker start.
export function applyDispatchRecovery(store, snapshot, proof, {
  scan = collectWorkerProcessReadback, now = Date.now()
} = {}) {
  const processReadback = scan(snapshot.run);
  return store.transaction(() => {
    if (!same(dispatchRecoverySnapshot(store, snapshot.run.run_id), snapshot)) {
      return { action: "hold", reason: "recovery_fence_changed" };
    }
    const decision = decideDispatchRecovery(snapshot, proof, processReadback, now);
    if (decision.action === "hold") return decision;
    const { run, leases, signals } = snapshot;
    if (decision.action === "acknowledge") {
      for (const signal of signals) store.db.prepare(`UPDATE signals SET status='acknowledged',
        run_id=NULL,lease_token=NULL,lease_fence=NULL,updated_at_ms=?,last_error=? WHERE id=?`)
        .run(now, `Stable native terminal and Asana completion; proof=${digest(proof)}`, signal.id);
      for (const lease of leases) if (!store.releaseLease(lease)) throw new Error("Recovery lease changed");
    } else {
      const agent = leases.find(lease => lease.key === `agent:${run.agent_id}`);
      if (agent && !store.releaseLease(agent)) throw new Error("Recovery agent changed");
    }
    store.db.prepare(`UPDATE runs SET state=?,turn_id=?,updated_at_ms=?,last_error=?,last_reconcile_check_at_ms=? WHERE run_id=?`)
      .run(decision.action === "acknowledge" ? "completed" : "needs_reconciliation",
        decision.turn_id, now, `Terminal recovery ${decision.action}; proof=${digest(proof)}`, now, run.run_id);
    store.db.prepare("INSERT INTO recovery_events(run_id,observed_at_ms,kind,evidence_json) VALUES (?,?,?,?)")
      .run(run.run_id,now,"dispatch",JSON.stringify({snapshot,proof,processReadback,decision}));
    return decision;
  });
}
