import fs from "node:fs";
import os from "node:os";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";

const hash = (value) => createHash("sha256").update(value).digest("hex");
export const PROCESS_SCAN_POLICY = "local-all-dispatch-workers-codex-exec-and-exact-run-thread-v1";
const PS_ARGS = ["-axo", "pid=,command="];

export function processScanBinding(run) {
  if (![run?.run_id, run?.agent_id, run?.thread_id].every(
    (value) => typeof value === "string" && /^[a-zA-Z0-9_-]+$/.test(value))) {
    throw new Error("Process scan requires exact run, agent and thread identity");
  }
  return {
    source: "asana-worker-process-readback",
    host_id: "local", hostname: os.hostname(),
    run_id: run.run_id, agent_id: run.agent_id, thread_id: run.thread_id,
    scan_policy: PROCESS_SCAN_POLICY,
    scanner_sha256: hash(fs.readFileSync(new URL(import.meta.url))),
    executable: "/bin/ps", args: PS_ARGS
  };
}

export function matchingWorkerProcesses(raw, run) {
  const matches = [];
  for (const line of raw.trim().split("\n")) {
    const parsed = line.match(/^\s*(\d+)\s+(.+)$/);
    if (!parsed) throw new Error("Incomplete process scan output");
    const [, pid, command] = parsed;
    let reason;
    if (command.includes(run.run_id) || command.includes(run.thread_id)) reason = "exact-run-or-thread";
    else if (/(?:^|[\s/])asana-dispatch-worker\.mjs(?:\s|$)/.test(command)) reason = "dispatch-worker";
    else if (/(?:^|[\s/])codex(?:\s+[^\s]+)*\s+exec(?:\s|$)/.test(command)) reason = "codex-exec";
    if (reason) matches.push({ pid: Number(pid), reason });
  }
  return matches;
}

// No caller-supplied shell, filter, executable or host is accepted.
export function collectWorkerProcessReadback(run) {
  const binding = processScanBinding(run);
  const raw = execFileSync("/bin/ps", PS_ARGS, {
    encoding: "utf8", timeout: 10_000, maxBuffer: 8 * 1024 * 1024
  });
  return { ...binding, observed_at_ms: Date.now(), scan_succeeded: true,
    raw_sha256: hash(raw), matching_processes: matchingWorkerProcesses(raw, run) };
}
