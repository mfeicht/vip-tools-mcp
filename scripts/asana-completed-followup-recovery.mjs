import fs from 'node:fs';
import {createHash} from 'node:crypto';
import {dispatchRecoverySnapshot} from './asana-dispatch-terminal-recovery.mjs';
import {collectWorkerProcessReadback,processScanBinding} from './asana-worker-process-readback.mjs';

// One reviewed legacy incident, not a generic expired-lease deletion policy.
// Current settle() already rejects extra leases. Never infer no side effects:
// this run completed real work and verified its required native recurrence.
export const FOLLOWUP_INCIDENT=Object.freeze({
  run_id:'92a7a05404f8414186b8caefc954abdf',agent_id:'vip-ai-finance',
  thread_id:'01a15970-a6dc-7a13-9c0f-8aad173ac3ca',turn_id:'01a15970-aa38-7832-8cd0-9b25aa359105',
  source_task:'1219487335843282',followup_task:'1219685989996091',assignee:'1215003777954631',
  activity_sha256:'2d034d71c666ae4cc9f612219713ac441eda9e45199b050bffd1d4625c14ec7b',
  receipt_sha256:'e474d86248da195764ae8353bbe91ef59c7cddbfebdb2cf50140f91963eee5bd',
  // Asana's automatic due-soon story advanced modified_at after the old receipt.
  // Both subsequent mentions were also reviewed; neither changes task work.
  source_modified:'2026-10-20T13:24:08.069Z',followup_modified:'2026-10-21T10:05:00.289Z',
  completed_at:'2026-10-20T13:24:06.221Z',due_at:'2026-10-21T10:35:00.000Z'
});
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const hash=x=>createHash('sha256').update(x).digest('hex');
export function followupActivity(items) {
  return items.map(item=>{
    const row={...item};delete row.output;
    if(row.type==='reasoning'){delete row.content;delete row.summary;}
    if(row.type==='userMessage')delete row.content;
    return row;
  });
}
export function followupNativeObservation(data,observedAt=Date.now()) {
  if(data.page?.hasMore!==false||data.page.nextCursor!==null||data.turns?.length!==1||
    !Array.isArray(data.turns[0].items))throw new Error('Complete single-turn readback required');
  const turn=data.turns[0];
  return {source:'codex_app.read_thread',observed_at_ms:observedAt,thread_id:data.thread.id,
    host_id:data.thread.hostId,runtime_status:data.thread.status?.type,turn_id:turn.id,
    turn_status:turn.status,started_at:turn.startedAt,completed_at:turn.completedAt,
    activity_count:turn.items.length,activity_sha256:hash(JSON.stringify(followupActivity(turn.items)))};
}
export function decideCompletedFollowup(snapshot,proof,scan,now=Date.now()) {
  const t=FOLLOWUP_INCIDENT,hold=reason=>({action:'hold',reason});
  const {run,leases,signals}=snapshot;
  if(run?.run_id!==t.run_id||run.agent_id!==t.agent_id||run.thread_id!==t.thread_id||
    run.task_gid!==t.source_task||run.state!=='completed'||(run.turn_id&&run.turn_id!==t.turn_id)||
    signals.length!==0||leases.length!==1||leases[0].key!==`task:${t.followup_task}`||
    leases[0].run_id!==run.run_id)return hold('isolated_completed_followup_required');
  const fresh=r=>Number.isSafeInteger(r?.observed_at_ms)&&r.observed_at_ms<=now&&now-r.observed_at_ms<=300000;
  const native=proof?.native;
  if(!Array.isArray(native)||native.length!==2||native.some(r=>!fresh(r)||
    r.source!=='codex_app.read_thread'||r.thread_id!==t.thread_id||r.host_id!=='local'||
    !['idle','notLoaded'].includes(r.runtime_status)||r.turn_id!==t.turn_id||r.turn_status!=='completed'||
    r.started_at!==1792501918||r.completed_at!==1792502968||
    r.activity_count!==140||r.activity_sha256!==t.activity_sha256)||
    native[1].observed_at_ms-native[0].observed_at_ms<1000||
    !same({...native[0],observed_at_ms:null},{...native[1],observed_at_ms:null}))return hold('reviewed_native_activity_required');
  const reads=proof.asana;
  if(!Array.isArray(reads)||reads.length!==2||reads.some(r=>!fresh(r)||r.agent_id!=='vip-ai-operations'||
    r.source?.gid!==t.source_task||r.source.completed!==true||r.source.completed_at!==t.completed_at||
    r.source.modified_at!==t.source_modified||r.source.assignee?.gid!==t.assignee||
    r.followup?.gid!==t.followup_task||r.followup.completed!==false||r.followup.completed_at!==null||
    r.followup.modified_at!==t.followup_modified||r.followup.assignee?.gid!==t.assignee||r.followup.due_at!==t.due_at||
    !Array.isArray(r.stories)||r.next_page!==null||!r.stories.some(s=>s.gid==='1219726059370192'&&
      s.resource_subtype==='task_due_time_approaching_story'&&s.created_by===null&&s.created_at===t.followup_modified)||
    r.stories.some(s=>Date.parse(s.created_at)>1792502968000&&!(
      (s.gid==='1219726059370192'&&s.resource_subtype==='task_due_time_approaching_story'&&s.created_by===null)||
      (['1219788704077252','1219810459044440'].includes(s.gid)&&s.resource_subtype==='mentioned'&&s.created_by?.gid===t.assignee))))||
    reads[1].observed_at_ms-reads[0].observed_at_ms<1000||
    !same({...reads[0],observed_at_ms:null},{...reads[1],observed_at_ms:null}))return hold('stable_operations_readback_required');
  if(!fresh(scan)||!scan.scan_succeeded||Object.entries(processScanBinding(run)).some(([k,v])=>!same(scan[k],v))||
    !/^[a-f0-9]{64}$/.test(scan.raw_sha256||'')||!Array.isArray(scan.matching_processes)||scan.matching_processes.length)
    return hold('worker_inactivity_unverified');
  return {action:'release_followup_only',task_completed:false,signals_unchanged:true,old_work_preserved:true};
}

export function applyCompletedFollowup(store,snapshot,proof,{operator,receiptPath,apply=false,
  scan=collectWorkerProcessReadback,now=null}={}) {
  // A byte-bound historical receipt is evidence of past effects, never authority
  // to execute those effects again. Its unchanged current task state is mandatory.
  const info=fs.lstatSync(receiptPath);
  if(!info.isFile()||info.size>1000000||fs.realpathSync(receiptPath)!==receiptPath||
    hash(fs.readFileSync(receiptPath))!==FOLLOWUP_INCIDENT.receipt_sha256)
    return {action:'hold',reason:'reviewed_completion_receipt_required'};
  const processReadback=scan(snapshot.run);now??=Date.now();
  return store.transaction(()=>{
    if(!same(dispatchRecoverySnapshot(store,snapshot.run.run_id),snapshot))return {action:'hold',reason:'recovery_fence_changed'};
    const manual=store.manualRun(operator?.run_id),held=manual?store.leasesForRun(manual.run_id):[];
    if(manual?.state!=='active'||manual.agent_id!=='vip-ai-operations'||manual.thread_id!==operator?.thread_id||
      !operator.thread_id||operator.task_gid!=='1218924687084315'||
      !held.some(l=>l.key==='agent:vip-ai-operations'&&l.fence===manual.agent_fence&&l.expires_at_ms>now)||
      !held.some(l=>l.key===`task:${operator.task_gid}`&&l.expires_at_ms>now))return {action:'hold',reason:'current_operations_lease_required'};
    const lease=snapshot.leases[0];
    if(lease&&store.db.prepare('SELECT last_fence FROM lease_fences WHERE key=?').get(lease.key)?.last_fence!==lease.fence)
      return {action:'hold',reason:'lease_fence_history_changed'};
    const decision=decideCompletedFollowup(snapshot,proof,processReadback,now);
    if(decision.action!=='release_followup_only'||!apply)return {...decision,dry_run:!apply};
    if(!store.releaseLease(lease))throw new Error('Followup fence changed');
    store.db.prepare('INSERT INTO recovery_events(run_id,observed_at_ms,kind,evidence_json) VALUES (?,?,?,?)')
      .run(snapshot.run.run_id,now,'completed_followup_lease',JSON.stringify({snapshot,proof,operator,
        receipt_sha256:FOLLOWUP_INCIDENT.receipt_sha256,processReadback,decision}));
    return {...decision,dry_run:false};
  });
}
