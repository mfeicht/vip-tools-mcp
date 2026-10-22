import { createHash } from 'node:crypto';
import { dispatchRecoverySnapshot } from './asana-dispatch-terminal-recovery.mjs';
import { decideFailedSpawnRecovery } from './asana-failed-spawn-recovery.mjs';
import { collectFailedSpawnProcessReadback } from './asana-worker-process-readback.mjs';

const same = (a,b) => JSON.stringify(a) === JSON.stringify(b);
const hash = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const stableRun = ({updated_at_ms,last_reconcile_check_at_ms,...run}) => run;

// Only a previously audited, never-started ENOENT claim is eligible. A failed
// model turn, a timeout or an unaudited legacy row cannot use this path.
export function failedSpawnResumeSnapshot(store, runId) {
  return {...dispatchRecoverySnapshot(store,runId), prior_events:store.db.prepare(
    "SELECT * FROM recovery_events WHERE run_id=? AND kind='legacy_spawn_agent_quarantine' ORDER BY observed_at_ms"
  ).all(runId)};
}

export function decideFailedSpawnResume(snapshot, proof, scan, now=Date.now()) {
  const hold=reason=>({action:'hold',reason});
  if(snapshot.prior_events?.length!==1) return hold('single_prior_spawn_audit_required');
  const event=snapshot.prior_events[0];
  let prior;
  try { prior=JSON.parse(event.evidence_json); } catch { return hold('prior_audit_invalid'); }
  const accepted=decideFailedSpawnRecovery(prior.snapshot || {},prior.proof,prior.processReadback,event.observed_at_ms);
  if(accepted.action!=='quarantine' || !same(accepted,prior.decision) ||
    !snapshot.run || !same(stableRun(snapshot.run),stableRun(prior.snapshot.run)) ||
    snapshot.run.run_id!==event.run_id || snapshot.run.thread_id!==null || snapshot.run.turn_id!==null ||
    snapshot.leases.length!==1 ||
    !same(snapshot.leases[0],prior.snapshot.leases.find(l=>l.key===`task:${snapshot.run.task_gid}`)) ||
    !same(snapshot.signals,prior.snapshot.signals)) return hold('prior_audit_or_claim_changed');
  // Reuse the strict ENOENT proof validator with the original complete claim;
  // this validates evidence only and never recreates its released agent lease.
  const current=decideFailedSpawnRecovery(prior.snapshot,proof,scan,now);
  if(current.action!=='quarantine') return current;
  if(proof.asana.some(r=>r.agent_id!=='vip-ai-operations' ||
    !Number.isFinite(Date.parse(r.task.modified_at)) || !/^\d+$/.test(r.task.assignee?.gid || '')))
    return hold('current_task_identity_missing');
  if(proof.asana[1].task.completed) return hold('completed_task_requires_completion_reconciliation');
  return {action:'retry_after',reason:'audited_spawn_never_started',delay_ms:900000,
    release_task:true,task_completed:false,preserve_attempts:true};
}

export function applyFailedSpawnResume(store,snapshot,proof,{operator,apply=false,
  scan=collectFailedSpawnProcessReadback,now=Date.now()}={}) {
  const processReadback=scan(snapshot.run);
  return store.transaction(()=>{
    if(!same(failedSpawnResumeSnapshot(store,snapshot.run.run_id),snapshot))
      return {action:'hold',reason:'recovery_fence_changed'};
    const manual=store.manualRun(operator?.run_id);
    const leases=manual ? store.leasesForRun(manual.run_id) : [];
    if(manual?.state!=='active' || manual.agent_id!=='vip-ai-operations' ||
      manual.thread_id!==operator?.thread_id || !operator.thread_id ||
      !leases.some(l=>l.key==='agent:vip-ai-operations' && l.fence===manual.agent_fence && l.expires_at_ms>now) ||
      !leases.some(l=>l.key===`task:${operator.task_gid}` && l.expires_at_ms>now))
      return {action:'hold',reason:'current_operations_lease_required'};
    const decision=decideFailedSpawnResume(snapshot,proof,processReadback,now);
    if(decision.action!=='retry_after' || !apply) return {...decision,dry_run:!apply};
    const note=`Audited ENOENT before model start; requeued without task completion; proof=${hash(proof)}`;
    for(const signal of snapshot.signals) {
      const changed=store.db.prepare(`UPDATE signals SET status='retry_after',available_at_ms=?,
        run_id=NULL,lease_token=NULL,lease_fence=NULL,updated_at_ms=?,last_error=?
        WHERE id=? AND status='leased' AND run_id=? AND lease_token=? AND lease_fence=?`)
        .run(now+decision.delay_ms,now,note,signal.id,snapshot.run.run_id,signal.lease_token,signal.lease_fence).changes;
      if(changed!==1) throw new Error('Spawn signal fence changed');
    }
    if(!store.releaseLease(snapshot.leases[0])) throw new Error('Spawn task fence changed');
    // completed is the dispatcher attempt lifecycle only; Asana stays open.
    store.db.prepare("UPDATE runs SET state='completed',updated_at_ms=?,last_error=? WHERE run_id=?")
      .run(now,note,snapshot.run.run_id);
    store.db.prepare('INSERT INTO recovery_events(run_id,observed_at_ms,kind,evidence_json) VALUES (?,?,?,?)')
      .run(snapshot.run.run_id,now,'failed_spawn_task_resume',JSON.stringify({snapshot,proof,processReadback,operator,decision}));
    return {...decision,dry_run:false};
  });
}
