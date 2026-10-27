import {createHash} from 'node:crypto';
import {validateReviewedObservations} from './asana-reviewed-intake-resume.mjs';
import {verifyPartialArtifacts} from './asana-reviewed-partial-context.mjs';
import {collectWorkerProcessReadback} from './asana-worker-process-readback.mjs';
// One reviewed historical manual run, no automatic failed-turn release rule.
export const MANUAL_RESUME = Object.freeze({
  "key": "catalyst_manual",
  "partial": true,
  "run_id": "549d3e2289fc4b6ba8e5c4c05ae260d6",
  "agent_id": "vip-ai-finance",
  "task_gid": "1219437994047037",
  "assignee": "1215003777954631",
  "thread_id": "01a167a5-b9c9-7a70-aa89-ece74cbb0200",
  "turn_id": "01a167a5-bd0f-76f1-a4e2-921ff2594570",
  "started_at": 1792740277,
  "completed_at": 1792740568,
  "activity_count": 65,
  "activity_sha256": "ddca0078c0395c4ac7323ca9107de1c8477cd5fe7e7e77a2005fc3221d918326",
  "turn_status": "failed",
  "error_message": "Selected model is at capacity. Please try a different model.",
  "modified_at": "2026-10-13T05:24:19.609Z",
  "stories_sha256": "a06b31bfaa8973c98697abbfe3e241a89d1eefb348b8f21576a5643c773390a8",
  "resource_key": "finance-due-queue-selection",
  "artifacts": [
    {
      "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Finance/due_queue_selection_automation-vip-ai-finance-due-task-worker-10_549d3e2289fc4b6ba8e5c4c05ae260d6_20261023T072736Z.json",
      "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/549d3e2289fc4b6ba8e5c4c05ae260d6/due_queue_selection_automation-vip-ai-finance-due-task-worker-10_549d3e2289fc4b6ba8e5c4c05ae260d6_20261023T072736Z.json",
      "bytes": 598,
      "sha256": "a066e2a1968218dd5a922cbc0ff5d4fe81fcdc4994222c5e85e2d98a846bb815"
    },
    {
      "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/exports/2026-10-27__system-recovery/catalyst-recovery-reviewed-context.json",
      "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/549d3e2289fc4b6ba8e5c4c05ae260d6/catalyst-recovery-reviewed-context.json",
      "bytes": 68637,
      "sha256": "f2a64bf3d58e0b111d13828e0855f4b086ecc8e37920a1d74143254294006fdb"
    }
  ]
});
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const hash=x=>createHash('sha256').update(JSON.stringify(x)).digest('hex');
export function manualResumeSnapshot(store,runId) {
 return {run:store.manualRun(runId),tasks:store.db.prepare('SELECT * FROM manual_run_tasks WHERE run_id=? ORDER BY task_gid').all(runId),
  leases:store.leasesForRun(runId),signals:store.db.prepare('SELECT * FROM signals WHERE run_id=? ORDER BY id').all(runId)};
}
export function decideReviewedManual(snapshot,proof,scan,now=Date.now()) {
 const t=MANUAL_RESUME,{run,tasks,leases,signals}=snapshot,hold=reason=>({action:'hold',reason});
 if(!run||run.run_id!==t.run_id||run.agent_id!==t.agent_id||run.thread_id!==t.thread_id||
  run.state!=='active'||run.created_at_ms!==1792740323203||run.agent_fence!==1530||
  run.agent_released_at_ms!==1792741384098||run.released_at_ms!==null||
  tasks.length!==1||tasks[0].run_id!==t.run_id||tasks[0].task_gid!==t.task_gid||
  tasks[0].resolved_at_ms!==null||tasks[0].lease_fence!==14||
  tasks[0].first_acquired_at_ms!==1792740485392||tasks[0].last_acquired_at_ms!==1792740485392||
  leases.length!==1||leases[0].key!=='task:'+t.task_gid||leases[0].run_id!==t.run_id||
  leases[0].fence!==14||signals.length!==0)return hold('reviewed_isolated_manual_claim_required');
 const checked=validateReviewedObservations(t,run,proof,scan,now);
 if(checked.action!=='retry_after')return checked;
 return {action:'release_reviewed_manual_task',task_completed:false,old_work_completed:false,
  no_effect_claim:false,signals_unchanged:true,owner_fresh_readback_required:true};
}
export function applyReviewedManual(store,snapshot,proof,{operator,apply=false,
 scan=collectWorkerProcessReadback,now=null}={}) {
 const processReadback=scan(snapshot.run);now??=Date.now();
 return store.transaction(()=>{
  if(!same(manualResumeSnapshot(store,snapshot.run.run_id),snapshot))return {action:'hold',reason:'recovery_fence_changed'};
  const own=store.manualRun(operator?.run_id),held=own?store.leasesForRun(own.run_id):[];
  if(own?.state!=='active'||own.agent_id!=='vip-ai-operations'||own.thread_id!==operator?.thread_id||
   operator.task_gid!=='1218924687084315'||
   !held.some(l=>l.key==='agent:vip-ai-operations'&&l.fence===own.agent_fence&&l.expires_at_ms>now)||
   !held.some(l=>l.key==='task:'+operator.task_gid&&l.expires_at_ms>now))return {action:'hold',reason:'current_operations_lease_required'};
  const decision=decideReviewedManual(snapshot,proof,processReadback,now);
  if(decision.action!=='release_reviewed_manual_task')return decision;
  const lease=snapshot.leases[0];
  if(store.db.prepare('SELECT last_fence FROM lease_fences WHERE key=?').get(lease.key)?.last_fence!==lease.fence)
   return {action:'hold',reason:'lease_fence_history_changed'};
  verifyPartialArtifacts(MANUAL_RESUME.artifacts);
  const acceptedContext={schema:'reviewed-manual-resume-v1',old_run_id:MANUAL_RESUME.run_id,
   task_gid:MANUAL_RESUME.task_gid,agent_id:MANUAL_RESUME.agent_id,task_completed:false,
   no_effect_claim:false,artifacts:MANUAL_RESUME.artifacts,
   next_step:'Finance prüft aktuellen Kalender-Task und Originalergebnis. Der frühere --help-Aufruf führte tatsächlich den Primary-Source-Worker aus; lokale Cache-/Queue-/Latest-Wirkungen sind nicht zu wiederholen. Keine Order und keine fachliche Kalenderaktualisierung durch Operations. Heutige Primärquellen und neue Ergebnis-/Abschlussgates bleiben beim Owner.'};
  if(!apply)return {...decision,dry_run:true,context:acceptedContext};
  if(!store.releaseLease(lease))throw new Error('Manual task fence changed');
  store.db.prepare('UPDATE manual_run_tasks SET resolved_at_ms=? WHERE run_id=? AND task_gid=?')
   .run(now,MANUAL_RESUME.run_id,MANUAL_RESUME.task_gid);
  store.db.prepare("UPDATE manual_runs SET state='released',updated_at_ms=?,released_at_ms=?,last_checked_at_ms=? WHERE run_id=?")
   .run(now,now,now,MANUAL_RESUME.run_id);
  store.db.prepare('INSERT INTO recovery_events(run_id,observed_at_ms,kind,evidence_json) VALUES (?,?,?,?)')
   .run(MANUAL_RESUME.run_id,now,'reviewed_manual_resume',JSON.stringify({snapshot,proof,operator,processReadback,decision,context:acceptedContext}));
  return {...decision,dry_run:false,evidence_sha256:hash(proof),context:acceptedContext};
 });
}
