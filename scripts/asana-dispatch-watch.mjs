import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { AsanaDispatchStore, DEFAULT_DB_PATH } from "./asana-dispatch-store.mjs";
import { poll } from "./asana-dispatch-poller.mjs";
import { reconcileCompletedRuns } from "./asana-dispatch-reconcile.mjs";
import { reconcileManualLeases } from "./asana-manual-lease-reconcile.mjs";
import { readMaterialCommentStoreHealth } from "./vip-mcp-call.mjs";

const HEALTH_PATH = path.join(path.dirname(DEFAULT_DB_PATH), "Asana-Dispatch-Health.json");
const LOG_PATH = path.join(path.dirname(DEFAULT_DB_PATH), "Asana-Dispatch-Workers.log");
const NODE = process.execPath;

async function saveHealth(health) {
  const temporary = `${HEALTH_PATH}.${randomUUID()}.tmp`;
  await fs.writeFile(temporary, `${JSON.stringify(health, null, 2)}\n`, { mode: 0o600 });
  await fs.rename(temporary, HEALTH_PATH);
}

export function workerCapacity(store, maximum = 2, coordinationReady = true, now = Date.now()) {
  if (!Number.isSafeInteger(maximum) || maximum < 0 || maximum > 2) throw new Error("Invalid worker limit");
  if (store.dispatchMaintenance()) return { started: 0, claimableAgents: null, slots: null, agents: [] };
  const active = store.activeLeases().filter(lease => lease.key.startsWith("agent:"));
  const slots = Math.max(0, maximum - active.length);
  const candidates = store.readyAgentCandidates(now, coordinationReady);
  return { started: 0, claimableAgents: candidates.length, slots, agents: candidates.slice(0, slots) };
}

async function launchWorkers(store, maximum = 2, coordinationReady = true) {
  const capacity = store.transaction(() => {
    if (!store.dispatchMaintenance()) store.normalizeContinuations();
    return workerCapacity(store, maximum, coordinationReady);
  });
  const agents = capacity.agents;
  if (!agents.length) return capacity;
  const log = await fs.open(LOG_PATH, "a", 0o600);
  try {
    for (const agent of agents) {
      const child = spawn(NODE, [path.join(import.meta.dirname, "asana-dispatch-worker.mjs"),`--agent=${agent}`,
        ...(coordinationReady ? [] : ["--coordination-unavailable"])], {
        cwd: path.resolve(import.meta.dirname, ".."),
        detached: true, stdio: ["ignore", log.fd, log.fd], env: { ...process.env }
      });
      child.unref();
    }
  } finally {
    await log.close();
  }
  return { ...capacity, started: agents.length };
}

export async function coordinationPrestartGate(store, readHealth = readMaterialCommentStoreHealth) {
  const candidates = store.coordinationBlockedCount();
  if (!candidates) return { ready: true, checked: false, blocked_signals: 0 };
  let health;
  try { health = await readHealth(); }
  catch { health = { ready: false, reason: "health_read_failed" }; }
  return { ...health, ready: health?.ready === true, checked: true,
    blocked_signals: health?.ready === true ? 0 : candidates };
}

export async function watch({ dispatch = false, maxWorkers = 2,
  runPoll = poll, makeStore = () => new AsanaDispatchStore(), writeHealth = saveHealth } = {}) {
  const start = Date.now();
  const health = { started_at: new Date(start).toISOString(), dispatch_enabled: dispatch,
    poll: null, workers_started: 0, ready: 0, active_agents: 0, stale_leases: 0,
    claimable_agents: null, available_worker_slots: null,
    counts: {}, errors: [], status: "starting", stalled_runs: 0, reconciliation: null };
  let pollHealthy = false;
  try {
    health.poll = await runPoll({ write: true });
    health.errors.push(...health.poll.errors.map((error) => ({ source: "poll_agent", ...error })));
    pollHealthy = health.poll.errors.length === 0;
  } catch (error) {
    health.errors.push({ source: "poll", error: String(error?.message || error).slice(0, 500) });
  }
  let store;
  try {
    store = makeStore();
    health.reconciliation = await reconcileCompletedRuns(store, { remoteEnabled: pollHealthy });
    health.errors.push(...health.reconciliation.errors.map((error) => ({ source: "reconciliation", ...error })));
    health.manual_reconciliation = pollHealthy && !health.reconciliation.errors.length
      ? await reconcileManualLeases(store)
      : { skipped: "upstream_readback_failed", errors: [] };
    health.errors.push(...health.manual_reconciliation.errors.map(error =>
      ({ source: "manual_reconciliation", ...error })));
    health.ready = store.readyCount();
    health.dispatch_maintenance = store.dispatchMaintenance();
    health.counts = store.counts();
    health.active_agents = store.activeLeases().filter((lease) =>
      lease.key.startsWith("agent:") && lease.expires_at_ms > Date.now()).length;
    health.stale_leases = store.activeLeases().filter(lease => lease.expires_at_ms <= Date.now()).length;
    health.quarantined_tasks = store.db.prepare(`SELECT COUNT(*) AS n FROM leases t
      WHERE t.key LIKE 'task:%' AND NOT EXISTS
        (SELECT 1 FROM leases a WHERE a.run_id=t.run_id AND a.key LIKE 'agent:%')`).get().n;
    health.stalled_runs = store.stalledRuns().length;
    if (dispatch && pollHealthy && !health.errors.length) {
      health.coordination_prestart = await coordinationPrestartGate(store);
      const capacity = await launchWorkers(store, maxWorkers, health.coordination_prestart.ready);
      health.workers_started = capacity.started;
      health.claimable_agents = capacity.claimableAgents;
      health.available_worker_slots = capacity.slots;
    }
  } catch (error) {
    health.errors.push({ source: "dispatch", error: String(error?.message || error).slice(0, 500) });
  } finally {
    store?.close();
  }
  health.finished_at = new Date().toISOString();
  health.duration_ms = Date.now() - start;
  health.status = health.errors.length ? "degraded" :
    health.stale_leases || health.stalled_runs || health.counts.dead_letter ||
      health.coordination_prestart?.ready === false ? "attention" : "ok";
  let metricsStore;
  try {
    metricsStore = makeStore();
    metricsStore.recordPollRun(health);
    health.history_24h = metricsStore.pollStats(start - 24 * 60 * 60_000);
  } catch (error) {
    health.errors.push({ source: "poll_history", error: String(error?.message || error).slice(0, 500) });
    health.status = "degraded";
  } finally {
    metricsStore?.close();
  }
  await writeHealth(health);
  return health;
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  watch({ dispatch: process.argv.includes("--dispatch"), maxWorkers: Number(process.env.DISPATCH_MAX_WORKERS || 2) })
    .then((health) => { console.log(JSON.stringify(health)); if (health.status !== "ok") process.exitCode = 2; })
    .catch((error) => { console.error(error); process.exitCode = 1; });
}
