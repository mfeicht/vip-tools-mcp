import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { collectWorkerProcessReadback } from "./asana-worker-process-readback.mjs";
import { decideManualLeaseReconciliation } from "./asana-manual-lease-decision.mjs";

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
        launch_phase TEXT,
        started_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        last_error TEXT
      );
      CREATE TABLE IF NOT EXISTS poll_cursors (
        agent_id TEXT PRIMARY KEY,
        completed_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS resource_event_cursors (
        agent_id TEXT NOT NULL,
        resource_gid TEXT NOT NULL,
        sync_token TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('needs_reconciliation','draining','ready')),
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY(agent_id, resource_gid)
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
      CREATE TABLE IF NOT EXISTS poll_runs (
        started_at_ms INTEGER PRIMARY KEY,
        finished_at_ms INTEGER NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('ok','attention','degraded')),
        dispatch_enabled INTEGER NOT NULL,
        duration_ms INTEGER NOT NULL,
        agent_count INTEGER NOT NULL,
        tasks_scanned INTEGER NOT NULL,
        stories_read INTEGER NOT NULL,
        signals_inserted INTEGER NOT NULL,
        errors_count INTEGER NOT NULL,
        workers_started INTEGER NOT NULL,
        ready INTEGER NOT NULL,
        active_agents INTEGER NOT NULL,
        stale_leases INTEGER NOT NULL,
        stalled_runs INTEGER NOT NULL,
        queue_acknowledged INTEGER NOT NULL,
        queue_pending INTEGER NOT NULL,
        queue_leased INTEGER NOT NULL,
        queue_retry_after INTEGER NOT NULL,
        queue_dead_letter INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS poll_runs_finished ON poll_runs(finished_at_ms);
      CREATE TABLE IF NOT EXISTS poll_run_errors (
        started_at_ms INTEGER NOT NULL,
        ordinal INTEGER NOT NULL,
        source TEXT NOT NULL,
        agent_id TEXT,
        error TEXT NOT NULL,
        PRIMARY KEY(started_at_ms, ordinal)
      );
      CREATE INDEX IF NOT EXISTS poll_run_errors_started ON poll_run_errors(started_at_ms);
      CREATE TABLE IF NOT EXISTS manual_runs (
        run_id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL,
        thread_id TEXT NOT NULL,
        thread_id_source TEXT NOT NULL,
        agent_fence INTEGER NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('active','released')),
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        released_at_ms INTEGER
      );
      CREATE TABLE IF NOT EXISTS manual_run_tasks (
        run_id TEXT NOT NULL,
        task_gid TEXT NOT NULL,
        lease_fence INTEGER NOT NULL,
        first_acquired_at_ms INTEGER NOT NULL,
        last_acquired_at_ms INTEGER NOT NULL,
        PRIMARY KEY(run_id, task_gid)
      );
    `);
    // Historical rows remain unclassified; never infer that Codex was not launched.
    if (!this.db.prepare("PRAGMA table_info(runs)").all()
      .some((column) => column.name === "launch_phase")) {
      this.db.exec("ALTER TABLE runs ADD COLUMN launch_phase TEXT");
    }
    if (!this.db.prepare("PRAGMA table_info(manual_runs)").all()
      .some((column) => column.name === "last_checked_at_ms")) {
      this.db.exec("ALTER TABLE manual_runs ADD COLUMN last_checked_at_ms INTEGER");
    }
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

  eventCursor(agentId, resourceGid) {
    return this.db.prepare(`SELECT * FROM resource_event_cursors
      WHERE agent_id=? AND resource_gid=?`)
      .get(assertId(agentId, "agent_id"), assertId(resourceGid, "resource_gid")) || null;
  }

  stageEventReconciliation(agentId, resourceGid, syncToken, now = Date.now()) {
    assertId(agentId, "agent_id");
    assertId(resourceGid, "resource_gid");
    if (typeof syncToken !== "string" || !syncToken || syncToken.length > 4096 ||
        /[\x00-\x1f\x7f]/.test(syncToken)) throw new Error("sync_token is invalid");
    assertTime(now, "now");
    this.db.prepare(`INSERT INTO resource_event_cursors
      (agent_id,resource_gid,sync_token,state,updated_at_ms)
      VALUES (?,?,?,'needs_reconciliation',?)
      ON CONFLICT(agent_id,resource_gid) DO UPDATE SET
        sync_token=excluded.sync_token,state='needs_reconciliation',updated_at_ms=excluded.updated_at_ms`)
      .run(agentId, resourceGid, syncToken, now);
    return this.eventCursor(agentId, resourceGid);
  }

  finishEventReconciliation(agentId, resourceGid, expectedToken, reconcile, now = Date.now()) {
    if (typeof reconcile !== "function") throw new Error("reconciliation callback is required");
    return this.transaction(() => {
      const cursor = this.eventCursor(agentId, resourceGid);
      if (cursor?.state !== "needs_reconciliation" || cursor.sync_token !== expectedToken) {
        throw new Error("event reconciliation cursor changed");
      }
      const result = reconcile();
      if (result && typeof result.then === "function") {
        throw new Error("reconciliation callback must be synchronous");
      }
      this.db.prepare(`UPDATE resource_event_cursors SET state='ready',updated_at_ms=?
        WHERE agent_id=? AND resource_gid=?`)
        .run(assertTime(now, "now"), agentId, resourceGid);
      return this.eventCursor(agentId, resourceGid);
    });
  }

  applyEventPage(agentId, resourceGid, expectedToken, nextToken, hasMore, apply,
    now = Date.now()) {
    if (typeof apply !== "function") throw new Error("event page callback is required");
    if (typeof hasMore !== "boolean") throw new Error("has_more is invalid");
    if (typeof nextToken !== "string" || !nextToken || nextToken.length > 4096 ||
        /[\x00-\x1f\x7f]/.test(nextToken) || nextToken === expectedToken) {
      throw new Error("next sync_token is invalid");
    }
    return this.transaction(() => {
      const cursor = this.eventCursor(agentId, resourceGid);
      if (!cursor || !["ready", "draining"].includes(cursor.state) ||
          cursor.sync_token !== expectedToken) {
        throw new Error("event page cursor changed or requires reconciliation");
      }
      const result = apply();
      if (result && typeof result.then === "function") {
        throw new Error("event page callback must be synchronous");
      }
      this.db.prepare(`UPDATE resource_event_cursors
        SET sync_token=?,state=?,updated_at_ms=? WHERE agent_id=? AND resource_gid=?`)
        .run(nextToken, hasMore ? "draining" : "ready", assertTime(now, "now"),
          agentId, resourceGid);
      return this.eventCursor(agentId, resourceGid);
    });
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

  beginCodexLaunch(claim, now = Date.now()) {
    assertTime(now, "now");
    return this.transaction(() => {
      const run = this.db.prepare("SELECT * FROM runs WHERE run_id=?").get(assertId(claim.run_id, "run_id"));
      const agent = this.db.prepare("SELECT * FROM leases WHERE key=?").get(claim.agent_lease.key);
      const task = this.db.prepare("SELECT * FROM leases WHERE key=?").get(claim.task_lease.key);
      const signals = this.db.prepare("SELECT * FROM signals WHERE run_id=? ORDER BY id").all(claim.run_id);
      if (run?.state !== "claimed" || run.launch_phase !== "preflight" ||
          run.thread_id || run.turn_id ||
          agent?.key !== `agent:${run.agent_id}` || task?.key !== `task:${run.task_gid}` ||
          agent?.run_id !== claim.run_id || task?.run_id !== claim.run_id ||
          agent.token !== claim.agent_lease.token || agent.fence !== claim.agent_lease.fence ||
          task.token !== claim.task_lease.token || task.fence !== claim.task_lease.fence ||
          agent.expires_at_ms <= now || task.expires_at_ms <= now ||
          signals.length !== claim.signals.length || !signals.length ||
          signals.some((signal) => signal.status !== "leased" ||
            signal.agent_id !== run.agent_id || signal.task_gid !== run.task_gid ||
            signal.lease_token !== task.token || signal.lease_fence !== task.fence ||
            !claim.signals.some((expected) => expected.id === signal.id))) return false;
      this.db.prepare("UPDATE runs SET launch_phase='launching',updated_at_ms=? WHERE run_id=?")
        .run(now, claim.run_id);
      this.acquireLeaseInTransaction(agent.key, claim.run_id, 2 * 60 * 60_000, now);
      this.acquireLeaseInTransaction(task.key, claim.run_id, 2 * 60 * 60_000, now);
      return true;
    });
  }

  recoverNeverLaunchedRun(runId, now = Date.now()) {
    assertId(runId, "run_id");
    assertTime(now, "now");
    return this.transaction(() => {
      const run = this.db.prepare("SELECT * FROM runs WHERE run_id=?").get(runId);
      if (run?.state !== "claimed" || run.launch_phase !== "preflight" ||
          run.thread_id || run.turn_id) return false;
      const leases = this.leasesForRun(runId);
      const agent = leases.find((lease) => lease.key === `agent:${run.agent_id}`);
      const task = leases.find((lease) => lease.key === `task:${run.task_gid}`);
      const signals = this.db.prepare("SELECT * FROM signals WHERE run_id=? ORDER BY id").all(runId);
      if (leases.length !== 2 || !agent || !task ||
          agent.expires_at_ms >= now || task.expires_at_ms >= now || !signals.length ||
          signals.some((signal) => signal.status !== "leased" ||
            signal.agent_id !== run.agent_id || signal.task_gid !== run.task_gid ||
            signal.lease_token !== task.token || signal.lease_fence !== task.fence)) return false;
      const note = "Dispatcher stopped in fenced preflight before Codex launch";
      for (const signal of signals) {
        const changed = this.db.prepare(`UPDATE signals SET status='retry_after',
          available_at_ms=?, attempts=MAX(0,attempts-1), run_id=NULL,
          lease_token=NULL, lease_fence=NULL, updated_at_ms=?, last_error=?
          WHERE id=? AND status='leased' AND run_id=? AND lease_token=? AND lease_fence=?`)
          .run(now + 5 * 60_000, now, note, signal.id, runId, task.token, task.fence).changes;
        if (changed !== 1) throw new Error("Preflight signal fence changed");
      }
      if (!this.releaseLease(task) || !this.releaseLease(agent)) {
        throw new Error("Preflight lease fence changed");
      }
      this.db.prepare("UPDATE runs SET state='completed',updated_at_ms=?,last_error=? WHERE run_id=?")
        .run(now, note, runId);
      return true;
    });
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
    return this.db.prepare(`SELECT r.*, a.expires_at_ms,
        CASE WHEN a.key IS NULL THEN 1 ELSE 0 END AS task_only_quarantine
      FROM runs r JOIN leases t ON t.key='task:'||r.task_gid AND t.run_id=r.run_id
      LEFT JOIN leases a ON a.key='agent:'||r.agent_id AND a.run_id=r.run_id
      WHERE (a.expires_at_ms < ? AND r.state IN ('claimed','started'))
         OR r.state='needs_reconciliation'`)
      .all(now);
  }

  quarantineTerminalRun(expectedRun, expectedLeases, expectedSignals, turnId,
    evidenceDigest, now = Date.now(), { dryRun = false } = {}) {
    assertTime(now, "now");
    assertId(turnId, "turn_id");
    if (!/^[a-f0-9]{64}$/.test(evidenceDigest || "")) throw new Error("Evidence digest required");
    return this.transaction(() => {
      const run = this.db.prepare("SELECT * FROM runs WHERE run_id=?").get(expectedRun?.run_id);
      const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
      if (!run || !same({ ...run }, expectedRun) ||
          !["started", "needs_reconciliation"].includes(run.state) ||
          !run.thread_id || (run.turn_id && run.turn_id !== turnId)) {
        throw new Error("Run changed before quarantine");
      }
      const leases = this.leasesForRun(run.run_id).map((row) => ({ ...row }));
      const signals = this.db.prepare("SELECT * FROM signals WHERE run_id=? ORDER BY id")
        .all(run.run_id).map((row) => ({ ...row }));
      if (!same(leases, expectedLeases) || !same(signals, expectedSignals) ||
          leases.length !== 2 || !signals.length ||
          signals.some((row) => row.status !== "leased" ||
            row.agent_id !== run.agent_id || row.task_gid !== run.task_gid)) {
        throw new Error("Lease or signal changed before quarantine");
      }
      const agent = leases.find((row) => row.key === `agent:${run.agent_id}`);
      const task = leases.find((row) => row.key === `task:${run.task_gid}`);
      if (!agent || !task || signals.some((row) =>
        row.lease_token !== task.token || row.lease_fence !== task.fence)) {
        throw new Error("Incomplete fenced claim");
      }
      if (dryRun) return { status: "review_ready", run_id: run.run_id,
        agent_release_only: true, task_and_signals_preserved: true };
      this.recordRun({ run_id: run.run_id, agent_lease: agent, task_lease: task, signals },
        { threadId: run.thread_id, turnId, state: "needs_reconciliation",
          error: `Task quarantined after native terminal double-read; evidence sha256=${evidenceDigest}`, now });
      if (!this.releaseLease(agent)) throw new Error("Agent release fence changed");
      return { run_id: run.run_id, agent_id: run.agent_id, task_gid: run.task_gid,
        agent_released: true, task_quarantined: true, signals_preserved: signals.length };
    });
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

  manualRun(runId) {
    return this.db.prepare("SELECT * FROM manual_runs WHERE run_id=?")
      .get(assertId(runId, "run_id")) || null;
  }

  assertManualContext(context) {
    if (!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(context?.threadId || "") ||
        !["CODEX_THREAD_ID", "CODEX_SESSION_ID"].includes(context?.source)) {
      throw new Error("Real CODEX_THREAD_ID or CODEX_SESSION_ID required");
    }
  }

  assertManualOwner(run, context) {
    this.assertManualContext(context);
    if (run.thread_id !== context.threadId || run.state !== "active") {
      throw new Error("Manual run belongs to another thread or is already released");
    }
    const agent = this.db.prepare("SELECT * FROM leases WHERE key=? AND run_id=?")
      .get(`agent:${run.agent_id}`, run.run_id);
    if (!agent || agent.fence !== run.agent_fence) throw new Error("Manual agent lease fence changed");
    return agent;
  }

  acquireManualAgent(agentId, runId, context, now = Date.now()) {
    if (!/^vip-ai-[a-z-]+$/.test(agentId || "")) throw new Error("Valid agent required");
    assertId(runId, "run_id");
    assertTime(now, "now");
    this.assertManualContext(context);
    return this.transaction(() => {
      const prior = this.manualRun(runId);
      if (prior) {
        if (prior.agent_id !== agentId) throw new Error("Manual agent identity changed");
        this.assertManualOwner(prior, context);
      } else if (this.leasesForRun(runId).length ||
          this.db.prepare("SELECT 1 FROM runs WHERE run_id=?").get(runId) ||
          this.db.prepare("SELECT 1 FROM signals WHERE run_id=?").get(runId)) {
        throw new Error("Existing unbound or dispatcher run cannot be adopted");
      }
      const lease = this.acquireLeaseInTransaction(`agent:${agentId}`, runId, 2 * 60 * 60_000, now);
      if (!lease) return null;
      if (!prior) {
        this.db.prepare(`INSERT INTO manual_runs
          (run_id,agent_id,thread_id,thread_id_source,agent_fence,state,created_at_ms,updated_at_ms)
          VALUES (?,?,?,?,?,'active',?,?)`)
          .run(runId, agentId, context.threadId, context.source, lease.fence, now, now);
      } else {
        this.db.prepare("UPDATE manual_runs SET updated_at_ms=? WHERE run_id=?").run(now, runId);
      }
      return lease;
    });
  }

  acquireManualTask(runId, taskGid, context, now = Date.now()) {
    assertId(runId, "run_id");
    if (!/^\d+$/.test(taskGid || "")) throw new Error("Numeric task required");
    assertTime(now, "now");
    return this.transaction(() => {
      const run = this.manualRun(runId);
      if (!run) throw new Error("Bound manual run required");
      this.assertManualOwner(run, context);
      const lease = this.acquireLeaseInTransaction(`task:${taskGid}`, runId, 2 * 60 * 60_000, now);
      if (!lease) return null;
      this.db.prepare(`INSERT INTO manual_run_tasks
        (run_id,task_gid,lease_fence,first_acquired_at_ms,last_acquired_at_ms)
        VALUES (?,?,?,?,?) ON CONFLICT(run_id,task_gid) DO UPDATE SET
        lease_fence=excluded.lease_fence,last_acquired_at_ms=excluded.last_acquired_at_ms`)
        .run(runId, taskGid, lease.fence, now, now);
      this.db.prepare("UPDATE manual_runs SET updated_at_ms=? WHERE run_id=?").run(now, runId);
      return lease;
    });
  }

  finishManualLeaseCommand(runId, command, context, now = Date.now()) {
    assertId(runId, "run_id");
    assertTime(now, "now");
    if (!["renew", "release"].includes(command)) throw new Error("Invalid manual lease command");
    return this.transaction(() => {
      const run = this.manualRun(runId);
      if (!run) throw new Error("Bound manual run required");
      this.assertManualContext(context);
      if (run.thread_id !== context.threadId) throw new Error("Manual run belongs to another thread");
      if (command === "release" && run.state === "released") {
        if (this.leasesForRun(runId).length) throw new Error("Released run unexpectedly owns leases");
        return 0;
      }
      this.assertManualOwner(run, context);
      const leases = this.leasesForRun(runId);
      for (const lease of leases) {
        if (command === "release") {
          if (!this.releaseLease(lease)) throw new Error("Lease release fence changed");
        } else this.acquireLeaseInTransaction(lease.key, runId, 2 * 60 * 60_000, now);
      }
      this.db.prepare(`UPDATE manual_runs SET updated_at_ms=?,state=?,released_at_ms=?
        WHERE run_id=?`).run(now, command === "release" ? "released" : "active",
        command === "release" ? now : null, runId);
      return leases.length;
    });
  }

  manualRunsForReconciliation({ now = Date.now(), limit = 2 } = {}) {
    assertTime(now, "now");
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 2) {
      throw new Error("manual reconciliation limit must be one or two");
    }
    const idleBefore = now - 10 * 60_000;
    const recheckBefore = now - 60 * 60_000;
    if (idleBefore < 0) return [];
    const runs = this.db.prepare(`SELECT r.*, r.created_at_ms AS acquired_at_ms, a.token AS agent_lease_token,
        a.fence AS agent_lease_fence, a.updated_at_ms AS agent_lease_updated_at_ms
      FROM manual_runs r
      LEFT JOIN leases a ON a.key='agent:' || r.agent_id AND a.run_id=r.run_id
      WHERE r.state='active' AND a.run_id=r.run_id AND r.created_at_ms <= ?
        AND COALESCE((SELECT MAX(l.updated_at_ms) FROM leases l
          WHERE l.run_id=r.run_id),r.created_at_ms) <= ?
        AND (r.last_checked_at_ms IS NULL OR r.last_checked_at_ms <= ?)
      ORDER BY COALESCE(r.last_checked_at_ms,r.created_at_ms),r.run_id
      LIMIT ?`).all(idleBefore, idleBefore, recheckBefore, limit);
    const tasks = this.db.prepare(`SELECT t.task_gid,t.first_acquired_at_ms AS acquired_at_ms,
        t.lease_fence AS bound_lease_fence,
        l.token AS lease_token,l.fence AS lease_fence,
        l.updated_at_ms AS lease_updated_at_ms
      FROM manual_run_tasks t
      LEFT JOIN leases l ON l.key='task:' || t.task_gid AND l.run_id=t.run_id
      WHERE t.run_id=? ORDER BY t.task_gid`);
    return runs.map((run) => ({ ...run, tasks: tasks.all(run.run_id) }));
  }

  // The caller supplies two independent Codex/Asana readbacks. A fixed local
  // process scan and every persisted fence are checked again immediately before
  // the single database transition. Unverified tasks remain quarantined.
  reconcileManualRun(run, evidence, now = Date.now()) {
    // Production entry point: no caller-supplied process proof is accepted.
    const processReadback = collectWorkerProcessReadback(run);
    return this._reconcileManualRunWithProcessReadback(run, evidence, processReadback, now);
  }

  _reconcileManualRunWithProcessReadback(run, evidence, processReadback, now = Date.now()) {
    assertTime(now, "now");
    if (!run?.run_id || !evidence?.nativeReadbacks || !evidence?.taskReadbacks) {
      throw new Error("Manual reconciliation evidence required");
    }
    return this.transaction(() => {
      const current = this.db.prepare(`SELECT r.*, r.created_at_ms AS acquired_at_ms,
          a.token AS agent_lease_token, a.fence AS agent_lease_fence
        FROM manual_runs r LEFT JOIN leases a
          ON a.key='agent:' || r.agent_id AND a.run_id=r.run_id
        WHERE r.run_id=?`).get(run.run_id);
      const tasks = this.db.prepare(`SELECT t.task_gid,
          t.first_acquired_at_ms AS acquired_at_ms,
          t.lease_fence AS bound_lease_fence,
          l.token AS lease_token,l.fence AS lease_fence
        FROM manual_run_tasks t LEFT JOIN leases l
          ON l.key='task:' || t.task_gid AND l.run_id=t.run_id
        WHERE t.run_id=? ORDER BY t.task_gid`).all(run.run_id);
      const keys = ["run_id", "agent_id", "thread_id", "state", "acquired_at_ms",
        "agent_fence", "agent_lease_token", "agent_lease_fence"];
      const taskKeys = ["task_gid", "acquired_at_ms", "bound_lease_fence", "lease_token", "lease_fence"];
      if (!current || keys.some((key) => current[key] !== run[key]) ||
          tasks.length !== run.tasks?.length || tasks.some((task, index) =>
            taskKeys.some((key) => task[key] !== run.tasks[index][key]))) {
        return { applied: false, reason: "manual_fence_changed" };
      }
      const decision = decideManualLeaseReconciliation({ run: { ...current, tasks },
        nativeReadbacks: evidence.nativeReadbacks, taskReadbacks: evidence.taskReadbacks,
        agentUserGid: evidence.agentUserGid, processReadback, now: Date.now() });
      if (!decision.release_agent || decision.action === "hold") {
        return { applied: false, reason: decision.reason };
      }
      const taskSet = new Set(tasks.map((task) => task.task_gid));
      if (decision.release_tasks.length + decision.quarantine_tasks.length !== taskSet.size ||
          decision.release_tasks.some((gid) => !taskSet.has(gid))) {
        throw new Error("Manual reconciliation task partition invalid");
      }
      for (const gid of decision.release_tasks) {
        const task = tasks.find((row) => row.task_gid === gid);
        if (!this.releaseLease({ key: `task:${gid}`, run_id: run.run_id,
          token: task.lease_token, fence: task.lease_fence })) {
          throw new Error("Manual task lease fence changed");
        }
      }
      if (!this.releaseLease({ key: `agent:${run.agent_id}`, run_id: run.run_id,
        token: current.agent_lease_token, fence: current.agent_lease_fence })) {
        throw new Error("Manual agent lease fence changed");
      }
      const complete = decision.quarantine_tasks.length === 0;
      this.db.prepare(`UPDATE manual_runs SET state=?,updated_at_ms=?,
          released_at_ms=?,last_checked_at_ms=? WHERE run_id=?`)
        .run(complete ? "released" : "active", now, complete ? now : null, now, run.run_id);
      return { applied: true, decision };
    });
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

  claimNext(runId, { now = Date.now(), ttlMs = 20 * 60_000, agentId = null } = {}) {
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
        const claim = { run_id: runId, agent_lease: agentLease, task_lease: taskLease, signals: siblings };
        this.db.prepare(`INSERT INTO runs
          (run_id,agent_id,task_gid,state,launch_phase,started_at_ms,updated_at_ms)
          VALUES (?,?,?,'claimed','preflight',?,?)`)
          .run(runId, candidate.agent_id, candidate.task_gid, now, now);
        return claim;
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

  recordPollRun(health, { now = Date.now(), retentionMs = 30 * 24 * 60 * 60_000 } = {}) {
    const startedAt = Date.parse(health?.started_at);
    const finishedAt = Date.parse(health?.finished_at);
    if (!Number.isSafeInteger(startedAt) || !Number.isSafeInteger(finishedAt) || finishedAt < startedAt) {
      throw new Error("poll run timestamps are invalid");
    }
    if (!["ok", "attention", "degraded"].includes(health?.status)) {
      throw new Error("poll run status is invalid");
    }
    if (!Number.isSafeInteger(retentionMs) || retentionMs < 24 * 60 * 60_000) {
      throw new Error("poll run retention is invalid");
    }
    assertTime(now, "now");
    const count = (value, label) => {
      const number = Number(value ?? 0);
      if (!Number.isSafeInteger(number) || number < 0) throw new Error(`${label} is invalid`);
      return number;
    };
    const agents = Array.isArray(health.poll?.agents) ? health.poll.agents : [];
    const sumAgent = (field) => agents.reduce((total, agent) => total + count(agent?.[field], field), 0);
    const counts = health.counts || {};
    const pollErrors = (Array.isArray(health.errors) ? health.errors : []).slice(0, 50)
      .map((entry, ordinal) => ({
        ordinal,
        source: String(entry?.source || "unknown").slice(0, 80),
        agent_id: entry?.agent_id == null ? null : String(entry.agent_id).slice(0, 180),
        error: String(entry?.error || entry?.message || "unknown error").slice(0, 500)
      }));
    const values = [
      startedAt, finishedAt, health.status, health.dispatch_enabled ? 1 : 0,
      count(health.duration_ms, "duration_ms"), agents.length,
      sumAgent("tasks_scanned"), sumAgent("stories_read"), sumAgent("signals_inserted"),
      count(health.errors?.length, "errors_count"), count(health.workers_started, "workers_started"),
      count(health.ready, "ready"), count(health.active_agents, "active_agents"),
      count(health.stale_leases, "stale_leases"), count(health.stalled_runs, "stalled_runs"),
      count(counts.acknowledged, "queue_acknowledged"), count(counts.pending, "queue_pending"),
      count(counts.leased, "queue_leased"), count(counts.retry_after, "queue_retry_after"),
      count(counts.dead_letter, "queue_dead_letter")
    ];
    return this.transaction(() => {
      this.db.prepare(`INSERT INTO poll_runs
        (started_at_ms,finished_at_ms,status,dispatch_enabled,duration_ms,agent_count,
         tasks_scanned,stories_read,signals_inserted,errors_count,workers_started,ready,
         active_agents,stale_leases,stalled_runs,queue_acknowledged,queue_pending,
         queue_leased,queue_retry_after,queue_dead_letter)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(started_at_ms) DO UPDATE SET
          finished_at_ms=excluded.finished_at_ms,status=excluded.status,
          dispatch_enabled=excluded.dispatch_enabled,duration_ms=excluded.duration_ms,
          agent_count=excluded.agent_count,tasks_scanned=excluded.tasks_scanned,
          stories_read=excluded.stories_read,signals_inserted=excluded.signals_inserted,
          errors_count=excluded.errors_count,workers_started=excluded.workers_started,
          ready=excluded.ready,active_agents=excluded.active_agents,
          stale_leases=excluded.stale_leases,stalled_runs=excluded.stalled_runs,
          queue_acknowledged=excluded.queue_acknowledged,queue_pending=excluded.queue_pending,
          queue_leased=excluded.queue_leased,queue_retry_after=excluded.queue_retry_after,
          queue_dead_letter=excluded.queue_dead_letter`).run(...values);
      this.db.prepare("DELETE FROM poll_run_errors WHERE started_at_ms=?").run(startedAt);
      const insertError = this.db.prepare(`INSERT INTO poll_run_errors
        (started_at_ms,ordinal,source,agent_id,error) VALUES (?,?,?,?,?)`);
      for (const item of pollErrors) {
        insertError.run(startedAt, item.ordinal, item.source, item.agent_id, item.error);
      }
      this.db.prepare("DELETE FROM poll_runs WHERE started_at_ms < ?").run(now - retentionMs);
      this.db.prepare("DELETE FROM poll_run_errors WHERE started_at_ms < ?").run(now - retentionMs);
      return this.db.prepare("SELECT * FROM poll_runs WHERE started_at_ms=?").get(startedAt);
    });
  }

  pollStats(sinceMs = Date.now() - 24 * 60 * 60_000) {
    assertTime(sinceMs, "since_ms");
    const row = this.db.prepare(`SELECT
      COUNT(*) AS runs,
      COALESCE(SUM(status='ok'),0) AS ok,
      COALESCE(SUM(status='attention'),0) AS attention,
      COALESCE(SUM(status='degraded'),0) AS degraded,
      COALESCE(SUM(errors_count),0) AS errors,
      COALESCE(SUM(workers_started),0) AS workers_started,
      ROUND(COALESCE(AVG(duration_ms),0)) AS average_duration_ms,
      COALESCE(MAX(duration_ms),0) AS maximum_duration_ms,
      COALESCE(MAX(queue_pending),0) AS maximum_pending,
      COALESCE(MAX(queue_leased),0) AS maximum_leased,
      COALESCE(MAX(queue_retry_after),0) AS maximum_retry_after,
      COALESCE(MAX(queue_dead_letter),0) AS maximum_dead_letter,
      COALESCE(MAX(stale_leases),0) AS maximum_stale_leases,
      COALESCE(MAX(stalled_runs),0) AS maximum_stalled_runs,
      MIN(started_at_ms) AS first_started_at_ms,
      MAX(finished_at_ms) AS last_finished_at_ms
      FROM poll_runs WHERE started_at_ms >= ?`).get(sinceMs);
    const errorSources = Object.fromEntries(this.db.prepare(`SELECT source,COUNT(*) AS n
      FROM poll_run_errors WHERE started_at_ms >= ? GROUP BY source ORDER BY source`)
      .all(sinceMs).map((item) => [item.source, item.n]));
    const recentErrorSamples = this.db.prepare(`SELECT started_at_ms,source,agent_id,error
      FROM poll_run_errors WHERE started_at_ms >= ?
      ORDER BY started_at_ms DESC,ordinal LIMIT 10`).all(sinceMs).map((item) => ({
      at: new Date(item.started_at_ms).toISOString(), source: item.source,
      agent_id: item.agent_id, error: item.error
    }));
    return {
      ...row,
      first_started_at: row.first_started_at_ms == null ? null : new Date(row.first_started_at_ms).toISOString(),
      last_finished_at: row.last_finished_at_ms == null ? null : new Date(row.last_finished_at_ms).toISOString(),
      error_sources: errorSources,
      recent_error_samples: recentErrorSamples
    };
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
