import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { AsanaDispatchStore, DEFAULT_DB_PATH } from "./asana-dispatch-store.mjs";
import { poll } from "./asana-dispatch-poller.mjs";
import { reconcileCompletedRuns } from "./asana-dispatch-reconcile.mjs";

const HEALTH_PATH = path.join(path.dirname(DEFAULT_DB_PATH), "Asana-Dispatch-Health.json");
const LOG_PATH = path.join(path.dirname(DEFAULT_DB_PATH), "Asana-Dispatch-Workers.log");
const NODE = process.execPath;

async function saveHealth(health) {
  const temporary = `${HEALTH_PATH}.${randomUUID()}.tmp`;
  await fs.writeFile(temporary, `${JSON.stringify(health, null, 2)}\n`, { mode: 0o600 });
  await fs.rename(temporary, HEALTH_PATH);
}

async function launchWorkers(store, maximum = 2) {
  const active = store.activeLeases().filter((lease) =>
    lease.key.startsWith("agent:") && lease.expires_at_ms > Date.now());
  const slots = Math.max(0, maximum - active.length);
  const ready = store.readyCount();
  const count = Math.min(slots, ready);
  if (count === 0) return 0;
  const log = await fs.open(LOG_PATH, "a", 0o600);
  try {
    for (let i = 0; i < count; i += 1) {
      const child = spawn(NODE, [path.join(import.meta.dirname, "asana-dispatch-worker.mjs")], {
        cwd: path.resolve(import.meta.dirname, ".."),
        detached: true, stdio: ["ignore", log.fd, log.fd], env: { ...process.env }
      });
      child.unref();
    }
  } finally {
    await log.close();
  }
  return count;
}

export async function watch({ dispatch = false, maxWorkers = 2 } = {}) {
  const start = Date.now();
  const health = { started_at: new Date(start).toISOString(), dispatch_enabled: dispatch,
    poll: null, workers_started: 0, ready: 0, active_agents: 0, stale_leases: 0,
    counts: {}, errors: [], status: "starting", stalled_runs: 0, reconciliation: null };
  let pollHealthy = false;
  try {
    health.poll = await poll({ write: true });
    health.errors.push(...health.poll.errors);
    pollHealthy = health.poll.errors.length === 0;
  } catch (error) {
    health.errors.push({ source: "poll", error: String(error?.message || error).slice(0, 500) });
  }
  const store = new AsanaDispatchStore();
  try {
    health.reconciliation = await reconcileCompletedRuns(store);
    health.errors.push(...health.reconciliation.errors.map((error) => ({ source: "reconciliation", ...error })));
    health.ready = store.readyCount();
    health.counts = store.counts();
    health.active_agents = store.activeLeases().filter((lease) =>
      lease.key.startsWith("agent:") && lease.expires_at_ms > Date.now()).length;
    health.stale_leases = store.runsNeedingReconciliation().length;
    health.stalled_runs = store.stalledRuns().length;
    if (dispatch && pollHealthy) {
      health.workers_started = await launchWorkers(store, maxWorkers);
    }
  } catch (error) {
    health.errors.push({ source: "dispatch", error: String(error?.message || error).slice(0, 500) });
  } finally {
    store.close();
  }
  health.finished_at = new Date().toISOString();
  health.duration_ms = Date.now() - start;
  health.status = health.errors.length ? "degraded" :
    health.stale_leases || health.stalled_runs || health.counts.dead_letter ? "attention" : "ok";
  await saveHealth(health);
  return health;
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  watch({ dispatch: process.argv.includes("--dispatch"), maxWorkers: Number(process.env.DISPATCH_MAX_WORKERS || 2) })
    .then((health) => { console.log(JSON.stringify(health)); if (health.status !== "ok") process.exitCode = 2; })
    .catch((error) => { console.error(error); process.exitCode = 1; });
}
