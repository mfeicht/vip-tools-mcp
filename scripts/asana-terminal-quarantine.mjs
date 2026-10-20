import fs from "node:fs";
import { createHash } from "node:crypto";
import { AsanaDispatchStore, DEFAULT_DB_PATH } from "./asana-dispatch-store.mjs";
import { collectWorkerProcessReadback, processScanBinding } from "./asana-worker-process-readback.mjs";

const hash = (raw) => createHash("sha256").update(raw).digest("hex");
const stable = (value) => JSON.stringify(value);
const MAX_AGE_MS = 5 * 60_000;

// Operator evidence comes from native Codex tools and canonical Operations MCP.
// A timestamp in the old history projection is not a substitute for native state.
export function validateQuarantineEvidence(evidence, now = Date.now()) {
  if (evidence?.schema !== "asana-native-terminal-quarantine-v1") throw new Error("Invalid evidence schema");
  const run = evidence.expected_run;
  if (!run?.thread_id || !run.task_gid || !run.agent_id) throw new Error("Run identity missing");
  const fresh = (row) => {
    if (!Number.isSafeInteger(row?.observed_at_ms) ||
        row.observed_at_ms > now + 1_000 || now - row.observed_at_ms > MAX_AGE_MS) {
      throw new Error("Evidence stale or invalid");
    }
  };
  const native = evidence.native_readbacks;
  if (!Array.isArray(native) || native.length !== 2) throw new Error("Two native readbacks required");
  for (const row of native) {
    fresh(row);
    if (!["codex_app.read_thread", "codex_app.wait_threads"].includes(row.source) ||
        row.host_id !== "local" || row.thread_id !== run.thread_id ||
        !["notLoaded", "idle"].includes(row.runtime_status) ||
        !["completed", "failed", "interrupted"].includes(row.turn_status) ||
        !row.turn_id || (run.turn_id && row.turn_id !== run.turn_id) ||
        !Number.isFinite(row.started_at_ms) || row.started_at_ms < run.started_at_ms - 2_000 ||
        row.started_at_ms > row.observed_at_ms ||
        !/^[a-f0-9]{64}$/.test(row.activity_digest || "") ||
        !Number.isSafeInteger(row.last_event_at_ms) ||
        row.last_event_at_ms < row.started_at_ms ||
        row.last_event_at_ms > row.observed_at_ms ||
        (row.completed_at_ms != null &&
          (!Number.isFinite(row.completed_at_ms) || row.completed_at_ms < row.started_at_ms ||
            row.completed_at_ms > row.observed_at_ms + 1_000)) ||
        (row.source === "codex_app.wait_threads" && row.wake_reason !== "inactiveStatus")) {
      throw new Error("Native terminal state not verified");
    }
  }
  if (native[1].observed_at_ms - native[0].observed_at_ms < 1_000 ||
      native[0].turn_id !== native[1].turn_id ||
      native[0].turn_status !== native[1].turn_status ||
      native[0].activity_digest !== native[1].activity_digest ||
      native[0].last_event_at_ms !== native[1].last_event_at_ms ||
      native[0].started_at_ms !== native[1].started_at_ms ||
      native[0].completed_at_ms !== native[1].completed_at_ms) {
    throw new Error("Native terminal readbacks changed");
  }
  const asana = evidence.asana_readbacks;
  if (!Array.isArray(asana) || asana.length !== 2) throw new Error("Two Asana readbacks required");
  for (const row of asana) {
    fresh(row);
    if (row.agent_id !== "vip-ai-operations" || row.task?.gid !== run.task_gid ||
        row.task.completed !== false || !row.task.modified_at || !row.task.assignee?.gid ||
        !Array.isArray(row.stories) || row.next_page !== null) {
      throw new Error("Complete open-task readback required");
    }
  }
  if (asana[1].observed_at_ms - asana[0].observed_at_ms < 1_000 ||
      stable(asana[0].task) !== stable(asana[1].task) ||
      stable(asana[0].stories) !== stable(asana[1].stories)) {
    throw new Error("Asana readbacks changed");
  }
  fresh(evidence.process_readback);
  const binding = processScanBinding(run);
  if (Object.entries(binding).some(([key, value]) =>
        stable(evidence.process_readback[key]) !== stable(value)) ||
      evidence.process_readback.match_expression !== undefined ||
      !/^[a-f0-9]{64}$/.test(evidence.process_readback.raw_sha256 || "") ||
      evidence.process_readback.scan_succeeded !== true ||
      !Array.isArray(evidence.process_readback.matching_processes) ||
      evidence.process_readback.matching_processes.length) {
    throw new Error("Worker inactivity not verified");
  }
  if (!Array.isArray(evidence.side_effects) || !evidence.side_effects.length) {
    throw new Error("Side-effect inventory required");
  }
  for (const item of evidence.side_effects) {
    if (!item.path?.startsWith("/") || !/^[a-f0-9]{64}$/.test(item.sha256 || "") ||
        hash(fs.readFileSync(item.path)) !== item.sha256) {
      throw new Error("Side-effect inventory changed");
    }
  }
  return native[0].turn_id;
}

export function quarantineFromEvidence(store, evidence, {
  apply = false, now = Date.now(), processScanner = collectWorkerProcessReadback
} = {}) {
  const turnId = validateQuarantineEvidence(evidence, now);
  // Re-execute the fixed scan immediately before the fence transaction.
  // Injection exists only for fixture callers; the CLI accepts no override.
  const currentScan = processScanner(evidence.expected_run);
  validateQuarantineEvidence({ ...evidence, process_readback: currentScan }, now);
  const run = store.db.prepare("SELECT * FROM runs WHERE run_id=?").get(evidence.expected_run.run_id);
  const leases = store.leasesForRun(evidence.expected_run.run_id).map((row) => ({ ...row }));
  const signals = store.db.prepare("SELECT * FROM signals WHERE run_id=? ORDER BY id")
    .all(evidence.expected_run.run_id).map((row) => ({ ...row }));
  if (stable({ ...run }) !== stable(evidence.expected_run) ||
      stable(leases) !== stable(evidence.expected_leases) ||
      stable(signals) !== stable(evidence.expected_signals)) {
    throw new Error("Current run, leases or signals differ from reviewed evidence");
  }
  return store.quarantineTerminalRun(evidence.expected_run, evidence.expected_leases,
    evidence.expected_signals, turnId, hash(stable(evidence)), now, { dryRun: !apply });
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  const args = process.argv.slice(2);
  if (args.some((arg) => arg !== "--apply" && !arg.startsWith("--evidence=") && !arg.startsWith("--db="))) {
    throw new Error("Usage: --evidence=/absolute/file.json [--db=...] [--apply]");
  }
  const filename = args.find((arg) => arg.startsWith("--evidence="))?.slice(11);
  if (!filename?.startsWith("/") || fs.statSync(filename).size > 1024 * 1024) {
    throw new Error("Explicit bounded evidence file required");
  }
  const evidence = JSON.parse(fs.readFileSync(filename, "utf8"));
  const db = args.find((arg) => arg.startsWith("--db="))?.slice(5) || DEFAULT_DB_PATH;
  const store = new AsanaDispatchStore(db);
  try {
    console.log(JSON.stringify(quarantineFromEvidence(store, evidence,
      { apply: args.includes("--apply") })));
  } finally { store.close(); }
}
