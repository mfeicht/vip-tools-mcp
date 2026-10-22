import { createHash } from "node:crypto";
import { readDispatchNative, readManualAsanaTask, boundedReadClients } from "./asana-manual-lease-readback.mjs";
import { collectWorkerProcessReadback, processScanBinding } from "./asana-worker-process-readback.mjs";
import { tool } from "./asana-dispatch-poller.mjs";
import { signalId } from "./asana-dispatch-signals.mjs";

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
  readNative = readDispatchNative, readTask = readManualAsanaTask, wait = delay,
  scan = collectWorkerProcessReadback } = {}) {
  const { run } = snapshot;
  ({rpc,client}=boundedReadClients(rpc,client));
  const identity = await tool(client,"asana_request",{agent_id:run.agent_id,
    method:"GET",path:"/users/me",params:{opt_fields:"gid"}});
  const ownGid=identity.response?.data?.gid;
  if(!/^\d+$/.test(ownGid || "")) throw new Error("Recovery owner identity unavailable");
  const native = [], asana = [], processes = [];
  for (let pass = 0; pass < 2; pass++) {
    if (pass) await wait(native[0].completed_at_ms === null ? 30000 : 1100);
    native.push(await readNative(run, rpc));
    processes.push(scan(run));
    asana.push(await readTask(run, { task_gid: run.task_gid }, client));
  }
  return { native, asana, processes, ownGid };
}

export function documentedOpenRecovery(run, proof) {
  const native = proof.native[1], current = proof.asana[1];
  const answer = native.answer;
  if(native.turn_status !== "completed" || native.completed_at_ms === null ||
      !["blocked","progress"].includes(answer?.outcome) ||
      !/^\d+$/.test(proof.ownGid || "") || current.task.completed !== false ||
      current.task.assignee?.gid !== proof.ownGid ||
      (answer.outcome === "progress" &&
        (typeof answer.next_step !== "string" || answer.next_step.trim().length < 12))) return null;
  const eligible = current.stories.filter(story => story.resource_subtype === "comment_added" &&
    story.created_by?.gid === proof.ownGid && /^\d+$/.test(story.gid || "") &&
    typeof story.text === "string" && story.text.trim().length >= 20 &&
    Date.parse(story.created_at) >= run.started_at_ms &&
    Date.parse(story.created_at) <= native.completed_at_ms &&
    (answer.evidence_story_gid === story.gid ||
      new RegExp(`(^|\\D)${story.gid}(\\D|$)`).test(answer.summary)));
  if(eligible.length !== 1) return null;
  const story=eligible[0];
  // A later response requires fresh work rather than settlement of the old run.
  if(current.stories.some(other => other.resource_subtype === "comment_added" &&
      other.created_by?.gid !== proof.ownGid && Date.parse(other.created_at) >= Date.parse(story.created_at))) return null;
  return {outcome:answer.outcome,evidence_story_gid:story.gid,next_step:answer.next_step};
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
    row.started_at_ms > row.observed_at_ms ||
    (row.completed_at_ms !== null && (!Number.isSafeInteger(row.completed_at_ms) ||
      row.completed_at_ms < row.started_at_ms || row.completed_at_ms > row.observed_at_ms)) ||
    !Number.isSafeInteger(row.activity_item_count) || row.activity_item_count < 1 ||
    !row.activity_last_item_id || !/^[a-f0-9]{64}$/.test(row.activity_digest || "")) ||
    native[1].observed_at_ms - native[0].observed_at_ms < 1000 ||
    !same({...native[0], observed_at_ms:null}, {...native[1], observed_at_ms:null})) {
    return hold("native_terminal_unverified");
  }
  // A missing terminal timestamp can only free the agent, never acknowledge or
  // replay the task. Require a longer stable native interval and two independent
  // process scans in addition to the fresh scan at the fenced transaction.
  const missingEnd = native[1].completed_at_ms === null;
  if (missingEnd && (!agent || native[1].observed_at_ms-native[0].observed_at_ms < 30000 ||
      !Array.isArray(proof.processes) || proof.processes.length !== 2 ||
      proof.processes.some((row,index) => !fresh(row) || !row.scan_succeeded ||
        row.observed_at_ms < native[index].observed_at_ms ||
        Object.entries(processScanBinding(run)).some(([key,value]) => !same(row[key],value)) ||
        !/^[a-f0-9]{64}$/.test(row.raw_sha256 || "") ||
        !Array.isArray(row.matching_processes) || row.matching_processes.length))) {
    return hold("missing_end_agent_quarantine_unverified");
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
  const open = !missingEnd ? documentedOpenRecovery(run,proof) : null;
  return { action: !missingEnd && (asana[1].task.completed || open) ? "acknowledge" : "quarantine",
    reason: open ? "documented_open_task" : missingEnd ? "missing_end_agent_only_quarantine" : "terminal_verified",
    open,
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
        .run(now, `Stable native terminal and ${decision.open ? "documented open-task result" : "Asana completion"}; proof=${digest(proof)}`, signal.id);
      for (const lease of leases) if (!store.releaseLease(lease)) throw new Error("Recovery lease changed");
      if(decision.open?.outcome === "progress") {
        const id=signalId(run.agent_id,run.task_gid,"continuation",run.run_id);
        store.scheduleContinuation({id,agent_id:run.agent_id,task_gid:run.task_gid,
          source_version:id,kind:"continuation",priority:55},decision.open.next_step,now);
      }
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
