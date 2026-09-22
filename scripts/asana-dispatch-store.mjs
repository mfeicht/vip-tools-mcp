import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";

const ROOT = path.resolve(import.meta.dirname, "..", "..");
export const DEFAULT_DB_PATH = path.join(
  ROOT,
  "VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/Asana-Dispatch.sqlite"
);

function assertId(value, label) {
  if (typeof value !== "string" || !/^[a-zA-Z0-9:_-]{2,180}$/.test(value)) {
    throw new Error(`${label} is invalid`);
  }
  return value;
}

function assertTime(value, label) {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${label} is invalid`);
  return value;
}

export class AsanaDispatchStore {
  constructor(filename = DEFAULT_DB_PATH) {
    fs.mkdirSync(path.dirname(filename), { recursive: true, mode: 0o700 });
    this.db = new DatabaseSync(filename);
    if (filename !== ":memory:") fs.chmodSync(filename, 0o600);
    this.db.exec("PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS signals (
        id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL,
        task_gid TEXT NOT NULL,
        story_gid TEXT,
        source_version TEXT NOT NULL,
        kind TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 0,
        available_at_ms INTEGER NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending','leased','retry_after','acknowledged','dead_letter')),
        attempts INTEGER NOT NULL DEFAULT 0,
        run_id TEXT,
        lease_token TEXT,
        lease_fence INTEGER,
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        last_error TEXT
      );
      CREATE INDEX IF NOT EXISTS signals_ready ON signals(status, available_at_ms, priority DESC, created_at_ms);
      CREATE TABLE IF NOT EXISTS leases (
        key TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        token TEXT NOT NULL,
        fence INTEGER NOT NULL,
        expires_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS lease_fences (
        key TEXT PRIMARY KEY,
        last_fence INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS task_observations (
        agent_id TEXT NOT NULL,
        task_gid TEXT NOT NULL,
        modified_at TEXT,
        latest_story_at TEXT,
        latest_story_gid TEXT,
        observed_at_ms INTEGER NOT NULL,
        PRIMARY KEY(agent_id, task_gid)
      );
      CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL,
        task_gid TEXT NOT NULL,
        thread_id TEXT,
        turn_id TEXT,
        state TEXT NOT NULL CHECK (state IN ('claimed','started','completed','needs_reconciliation')),
        started_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        last_error TEXT
      );
      CREATE TABLE IF NOT EXISTS poll_cursors (
        agent_id TEXT PRIMARY KEY,
        completed_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS dependency_watches (
        agent_id TEXT NOT NULL,
        task_gid TEXT NOT NULL,
        linked_task_gid TEXT NOT NULL,
        evidence_story_gid TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL,
        last_checked_at_ms INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(agent_id, task_gid)
      );
    `);
  }

  close() {
    this.db.close();
  }

  transaction(callback) {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = callback();
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  enqueue(signal, now = Date.now()) {
    const id = assertId(signal.id, "signal.id");
    const agentId = assertId(signal.agent_id, "signal.agent_id");
    const taskGid = assertId(signal.task_gid, "signal.task_gid");
    const sourceVersion = assertId(signal.source_version, "signal.source_version");
    const kind = assertId(signal.kind, "signal.kind");
    const storyGid = signal.story_gid == null ? null : assertId(signal.story_gid, "signal.story_gid");
    const availableAt = assertTime(signal.available_at_ms ?? now, "signal.available_at_ms");
    const priority = Number(signal.priority ?? 0);
    if (!Number.isSafeInteger(priority) || priority < 0 || priority > 100) throw new Error("signal.priority is invalid");
    return this.db.prepare(`
      INSERT OR IGNORE INTO signals
        (id,agent_id,task_gid,story_gid,source_version,kind,priority,available_at_ms,status,created_at_ms,updated_at_ms)
      VALUES (?,?,?,?,?,?,?,?, 'pending',?,?)
    `).run(id, agentId, taskGid, storyGid, sourceVersion, kind, priority, availableAt, now, now).changes === 1;
  }

  observation(agentId, taskGid) {
    return this.db.prepare("SELECT * FROM task_observations WHERE agent_id=? AND task_gid=?")
      .get(assertId(agentId, "agent_id"), assertId(taskGid, "task_gid")) || null;
  }

  pollCursor(agentId) {
    return this.db.prepare("SELECT completed_at FROM poll_cursors WHERE agent_id=?")
      .get(assertId(agentId, "agent_id"))?.completed_at || null;
  }

  dependencyWatches(agentId, now = Date.now(), intervalMs = 15 * 60_000) {
    return this.db.prepare(`SELECT * FROM dependency_watches
      WHERE agent_id=? AND last_checked_at_ms <= ? ORDER BY last_checked_at_ms, task_gid`)
      .all(assertId(agentId, "agent_id"), now - intervalMs);
  }

  markDependencyChecked(agentId, taskGid, now = Date.now()) {
    this.db.prepare(`UPDATE dependency_watches SET last_checked_at_ms=?
      WHERE agent_id=? AND task_gid=?`)
      .run(assertTime(now, "now"), assertId(agentId, "agent_id"), assertId(taskGid, "task_gid"));
  }

  clearDependencyWatch(agentId, taskGid) {
    this.db.prepare("DELETE FROM dependency_watches WHERE agent_id=? AND task_gid=?")
      .run(assertId(agentId, "agent_id"), assertId(taskGid, "task_gid"));
  }

  backfillDependencyWatch({ runId, agentId, taskGid, linkedTaskGid,
    evidenceStoryGid, now = Date.now() }) {
    assertId(runId, "run_id");
    assertId(agentId, "agent_id");
    assertId(taskGid, "task_gid");
    assertId(linkedTaskGid, "linked_task_gid");
    assertId(evidenceStoryGid, "evidence_story_gid");
    assertTime(now, "now");
    return this.transaction(() => {
      const run = this.db.prepare("SELECT agent_id,task_gid,state FROM runs WHERE run_id=?").get(runId);
      const acknowledgedDue = this.db.prepare(`SELECT 1 FROM signals
        WHERE agent_id=? AND task_gid=? AND kind='due_task' AND status='acknowledged' LIMIT 1`)
        .get(agentId, taskGid);
      const activeTaskLease = this.db.prepare("SELECT 1 FROM leases WHERE key=?")
        .get(`task:${taskGid}`);
      if (run?.agent_id !== agentId || run?.task_gid !== taskGid ||
          run?.state !== "completed" || !acknowledgedDue || activeTaskLease ||
          linkedTaskGid === taskGid) {
        throw new Error("backfill requires a completed, acknowledged due run without an active task lease");
      }
      const existing = this.db.prepare("SELECT * FROM dependency_watches WHERE agent_id=? AND task_gid=?")
        .get(agentId, taskGid);
      if (existing && (existing.linked_task_gid !== linkedTaskGid ||
          existing.evidence_story_gid !== evidenceStoryGid)) {
        throw new Error("conflicting dependency watch already exists");
      }
      this.db.prepare(`INSERT INTO dependency_watches
        (agent_id,task_gid,linked_task_gid,evidence_story_gid,created_at_ms,last_checked_at_ms)
        VALUES (?,?,?,?,?,?) ON CONFLICT(agent_id,task_gid) DO NOTHING`)
        .run(agentId, taskGid, linkedTaskGid, evidenceStoryGid, now, now);
      return this.db.prepare("SELECT * FROM dependency_watches WHERE agent_id=? AND task_gid=?")
        .get(agentId, taskGid);
    });
  }

  setPollCursor(agentId, completedAt) {
    assertId(agentId, "agent_id");
    if (!Number.isFinite(Date.parse(completedAt))) throw new Error("invalid poll cursor");
    this.db.prepare(`INSERT INTO poll_cursors(agent_id,completed_at) VALUES (?,?)
      ON CONFLICT(agent_id) DO UPDATE SET completed_at=excluded.completed_at`)
      .run(agentId, completedAt);
  }

  observeTask({ agent_id: agentId, task_gid: taskGid, modified_at: modifiedAt = null,
    latest_story_at: latestStoryAt = null, latest_story_gid: latestStoryGid = null }, now = Date.now()) {
    assertId(agentId, "agent_id");
    assertId(taskGid, "task_gid");
    assertTime(now, "now");
    this.db.prepare(`INSERT INTO task_observations
      (agent_id,task_gid,modified_at,latest_story_at,latest_story_gid,observed_at_ms)
      VALUES (?,?,?,?,?,?) ON CONFLICT(agent_id,task_gid) DO UPDATE SET
      modified_at=excluded.modified_at,
      latest_story_at=COALESCE(excluded.latest_story_at,task_observations.latest_story_at),
      latest_story_gid=COALESCE(excluded.latest_story_gid,task_observations.latest_story_gid),
      observed_at_ms=excluded.observed_at_ms`)
      .run(agentId, taskGid, modifiedAt, latestStoryAt, latestStoryGid, now);
  }

  recordRun(claim, { threadId = null, turnId = null, state = "claimed", error = null,
    now = Date.now() } = {}) {
    if (!claim?.signals?.length) throw new Error("invalid claim");
    if (!["claimed", "started", "completed", "needs_reconciliation"].includes(state)) {
      throw new Error("invalid run state");
    }
    this.db.prepare(`INSERT INTO runs
      (run_id,agent_id,task_gid,thread_id,turn_id,state,started_at_ms,updated_at_ms,last_error)
      VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET
      thread_id=COALESCE(excluded.thread_id,runs.thread_id),
      turn_id=COALESCE(excluded.turn_id,runs.turn_id),
      state=excluded.state,updated_at_ms=excluded.updated_at_ms,last_error=excluded.last_error`)
      .run(claim.run_id, claim.signals[0].agent_id, claim.signals[0].task_gid,
        threadId, turnId, state, now, now, error ? String(error).slice(0, 500) : null);
  }

  touchRun(runId, now = Date.now()) {
    this.db.prepare("UPDATE runs SET updated_at_ms=? WHERE run_id=? AND state='started'")
      .run(now, assertId(runId, "run_id"));
  }

  stalledRuns(now = Date.now(), thresholdMs = 20 * 60_000) {
    return this.db.prepare(`SELECT run_id,agent_id,task_gid,thread_id,updated_at_ms
      FROM runs WHERE state='started' AND updated_at_ms < ?`).all(now - thresholdMs);
  }

  runsNeedingReconciliation(now = Date.now()) {
    return this.db.prepare(`SELECT r.*, l.expires_at_ms FROM runs r JOIN leases l
      ON l.key='agent:'||r.agent_id AND l.run_id=r.run_id
      WHERE (l.expires_at_ms < ? AND r.state IN ('claimed','started'))
         OR r.state='needs_reconciliation'`)
      .all(now);
  }

  acquireLease(key, runId, ttlMs, now = Date.now()) {
    assertId(key, "lease.key");
    assertId(runId, "lease.run_id");
    assertTime(now, "now");
    if (!Number.isSafeInteger(ttlMs) || ttlMs < 60_000 || ttlMs > 8 * 60 * 60_000) {
      throw new Error("ttlMs must be between one minute and eight hours");
    }
    return this.transaction(() => this.acquireLeaseInTransaction(key, runId, ttlMs, now));
  }

  acquireLeaseInTransaction(key, runId, ttlMs, now) {
    const current = this.db.prepare("SELECT * FROM leases WHERE key = ?").get(key);
    // Expiry alone is not proof that a Codex turn stopped. Stale leases fail closed.
    if (current && current.run_id !== runId) return null;
    if (current) {
      this.db.prepare("UPDATE leases SET expires_at_ms = ?, updated_at_ms = ? WHERE key = ?")
        .run(now + ttlMs, now, key);
      return { key, run_id: runId, token: current.token, fence: current.fence, expires_at_ms: now + ttlMs };
    }
    const last = this.db.prepare("SELECT last_fence FROM lease_fences WHERE key = ?").get(key);
    const fence = Number(last?.last_fence || 0) + 1;
    this.db.prepare(`INSERT INTO lease_fences(key,last_fence) VALUES (?,?)
      ON CONFLICT(key) DO UPDATE SET last_fence=excluded.last_fence`).run(key, fence);
    const token = randomUUID();
    this.db.prepare("INSERT INTO leases(key,run_id,token,fence,expires_at_ms,updated_at_ms) VALUES (?,?,?,?,?,?)")
      .run(key, runId, token, fence, now + ttlMs, now);
    return { key, run_id: runId, token, fence, expires_at_ms: now + ttlMs };
  }

  releaseLease(lease) {
    if (!lease) return false;
    return this.db.prepare("DELETE FROM leases WHERE key=? AND run_id=? AND token=? AND fence=?")
      .run(lease.key, lease.run_id, lease.token, lease.fence).changes === 1;
  }

  claimNext(runId, { now = Date.now(), ttlMs = 2 * 60 * 60_000, agentId = null } = {}) {
    assertId(runId, "run_id");
    if (agentId) assertId(agentId, "agent_id");
    return this.transaction(() => {
      const candidates = this.db.prepare(`
        SELECT * FROM signals WHERE status IN ('pending','retry_after') AND available_at_ms <= ?
          AND (? IS NULL OR agent_id = ?)
          AND NOT EXISTS (SELECT 1 FROM leases WHERE key='agent:'||signals.agent_id)
          AND NOT EXISTS (SELECT 1 FROM leases WHERE key='task:'||signals.task_gid)
        ORDER BY priority DESC, available_at_ms, created_at_ms LIMIT 100
      `).all(now, agentId, agentId);
      for (const candidate of candidates) {
        const agentLease = this.acquireLeaseInTransaction(`agent:${candidate.agent_id}`, runId, ttlMs, now);
        if (!agentLease) continue;
        const taskLease = this.acquireLeaseInTransaction(`task:${candidate.task_gid}`, runId, ttlMs, now);
        if (!taskLease) {
          this.releaseLease(agentLease);
          continue;
        }
        const siblings = this.db.prepare(`
          SELECT * FROM signals WHERE agent_id=? AND task_gid=?
            AND status IN ('pending','retry_after') AND available_at_ms <= ?
          ORDER BY priority DESC, created_at_ms
        `).all(candidate.agent_id, candidate.task_gid, now);
        for (const signal of siblings) {
          this.db.prepare(`UPDATE signals SET status='leased', attempts=attempts+1,
            run_id=?, lease_token=?, lease_fence=?, updated_at_ms=? WHERE id=?`)
            .run(runId, taskLease.token, taskLease.fence, now, signal.id);
        }
        return { run_id: runId, agent_lease: agentLease, task_lease: taskLease, signals: siblings };
      }
      return null;
    });
  }

  settle(claim, { outcome, error = null, now = Date.now(), maxAttempts = 5,
    dependencyWatch = null } = {}) {
    if (!claim?.signals?.length || !["acknowledged", "retry_after", "dead_letter"].includes(outcome)) {
      throw new Error("invalid settlement");
    }
    return this.transaction(() => {
      const agentLease = this.db.prepare("SELECT * FROM leases WHERE key=?").get(claim.agent_lease.key);
      const taskLease = this.db.prepare("SELECT * FROM leases WHERE key=?").get(claim.task_lease.key);
      if (agentLease?.token !== claim.agent_lease.token || agentLease?.fence !== claim.agent_lease.fence ||
          taskLease?.token !== claim.task_lease.token || taskLease?.fence !== claim.task_lease.fence) {
        throw new Error("stale fencing token");
      }
      for (const signal of claim.signals) {
        const current = this.db.prepare("SELECT status,attempts,run_id,lease_token,lease_fence FROM signals WHERE id=?").get(signal.id);
        if (current?.status !== "leased" || current.run_id !== claim.run_id ||
            current.lease_token !== claim.task_lease.token || current.lease_fence !== claim.task_lease.fence) {
          throw new Error("signal claim changed");
        }
        const next = outcome === "retry_after" && current.attempts >= maxAttempts ? "dead_letter" : outcome;
        const delay = Math.min(60 * 60_000, 60_000 * (2 ** Math.max(0, current.attempts - 1)));
        const availableAt = next === "retry_after" ? now + delay : now;
        this.db.prepare(`UPDATE signals SET status=?, available_at_ms=?, run_id=NULL,
          lease_token=NULL, lease_fence=NULL, updated_at_ms=?, last_error=? WHERE id=?`)
          .run(next, availableAt, now, error ? String(error).slice(0, 500) : null, signal.id);
      }
      if (dependencyWatch) {
        if (outcome !== "acknowledged" ||
            dependencyWatch.task_gid !== claim.signals[0].task_gid ||
            dependencyWatch.agent_id !== claim.signals[0].agent_id ||
            dependencyWatch.linked_task_gid === dependencyWatch.task_gid) {
          throw new Error("invalid dependency watch settlement");
        }
        this.db.prepare(`INSERT INTO dependency_watches
          (agent_id,task_gid,linked_task_gid,evidence_story_gid,created_at_ms,last_checked_at_ms)
          VALUES (?,?,?,?,?,?) ON CONFLICT(agent_id,task_gid) DO UPDATE SET
          linked_task_gid=excluded.linked_task_gid,
          evidence_story_gid=excluded.evidence_story_gid,
          created_at_ms=excluded.created_at_ms,
          last_checked_at_ms=excluded.last_checked_at_ms`)
          .run(assertId(dependencyWatch.agent_id, "agent_id"),
            assertId(dependencyWatch.task_gid, "task_gid"),
            assertId(dependencyWatch.linked_task_gid, "linked_task_gid"),
            assertId(dependencyWatch.evidence_story_gid, "evidence_story_gid"), now, now);
      }
      this.releaseLease(claim.task_lease);
      this.releaseLease(claim.agent_lease);
      return claim.signals.length;
    });
  }

  reviewDeadLetters({ signalIds, agentId, taskGid, outcome = "acknowledged",
    evidence, now = Date.now() } = {}) {
    if (!Array.isArray(signalIds) || signalIds.length === 0 || signalIds.length > 20 ||
        new Set(signalIds).size !== signalIds.length) {
      throw new Error("dead-letter review requires one to twenty unique signal IDs");
    }
    signalIds.forEach((id) => assertId(id, "signal_id"));
    assertId(agentId, "agent_id");
    assertId(taskGid, "task_gid");
    assertTime(now, "now");
    if (!["acknowledged", "retry_after"].includes(outcome)) {
      throw new Error("dead-letter review outcome must be acknowledged or retry_after");
    }
    if (typeof evidence !== "string" || evidence.trim().length < 40) {
      throw new Error("dead-letter review requires concrete evidence");
    }
    return this.transaction(() => {
      const activeAgentLease = this.db.prepare("SELECT run_id FROM leases WHERE key=?")
        .get(`agent:${agentId}`);
      const activeTaskLease = this.db.prepare("SELECT run_id FROM leases WHERE key=?")
        .get(`task:${taskGid}`);
      if (activeAgentLease || activeTaskLease) {
        throw new Error("dead-letter review blocked by an active agent or task lease");
      }
      const rows = signalIds.map((id) => this.db.prepare("SELECT * FROM signals WHERE id=?").get(id));
      if (rows.some((row) => !row || row.status !== "dead_letter" ||
          row.agent_id !== agentId || row.task_gid !== taskGid || row.run_id ||
          row.lease_token || row.lease_fence)) {
        throw new Error("dead-letter signal state or scope changed");
      }
      const note = `Dead-letter review (${outcome}): ${evidence.trim()}`.slice(0, 500);
      const availableAt = outcome === "retry_after" ? now + 60_000 : now;
      const update = this.db.prepare(`UPDATE signals SET status=?, available_at_ms=?,
        updated_at_ms=?, last_error=? WHERE id=? AND status='dead_letter'`);
      for (const id of signalIds) {
        if (update.run(outcome, availableAt, now, note, id).changes !== 1) {
          throw new Error("dead-letter signal changed during review");
        }
      }
      return { agent_id: agentId, task_gid: taskGid, outcome,
        signals_resolved: signalIds.length, signal_ids: [...signalIds] };
    });
  }

  counts() {
    return Object.fromEntries(this.db.prepare("SELECT status,COUNT(*) AS n FROM signals GROUP BY status")
      .all().map((row) => [row.status, row.n]));
  }

  readyCount(now = Date.now()) {
    return this.db.prepare(`SELECT COUNT(*) AS n FROM signals
      WHERE status IN ('pending','retry_after') AND available_at_ms <= ?`).get(now).n;
  }

  activeLeases() {
    return this.db.prepare("SELECT key,run_id,fence,expires_at_ms,updated_at_ms FROM leases ORDER BY key").all();
  }

  leasesForRun(runId) {
    return this.db.prepare("SELECT * FROM leases WHERE run_id=? ORDER BY key")
      .all(assertId(runId, "run_id"));
  }

  claimForRun(runId) {
    assertId(runId, "run_id");
    const run = this.db.prepare("SELECT * FROM runs WHERE run_id=?").get(runId);
    if (!run) return null;
    const signals = this.db.prepare("SELECT * FROM signals WHERE run_id=? AND status='leased'")
      .all(runId);
    const leases = this.leasesForRun(runId);
    const agentLease = leases.find((lease) => lease.key === `agent:${run.agent_id}`);
    const taskLease = leases.find((lease) => lease.key === `task:${run.task_gid}`);
    if (!signals.length || !agentLease || !taskLease ||
        signals.some((signal) => signal.agent_id !== run.agent_id || signal.task_gid !== run.task_gid)) {
      throw new Error(`Incomplete active claim for ${runId}`);
    }
    return { run_id: runId, agent_lease: agentLease, task_lease: taskLease, signals };
  }

  releaseRun(runId) {
    assertId(runId, "run_id");
    return this.transaction(() => {
      const leases = this.leasesForRun(runId);
      for (const lease of leases) this.releaseLease(lease);
      return leases.length;
    });
  }
}
