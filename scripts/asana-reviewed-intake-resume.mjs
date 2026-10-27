import {createHash} from 'node:crypto';
import {dispatchRecoverySnapshot} from './asana-dispatch-terminal-recovery.mjs';
import {followupNativeObservation} from './asana-completed-followup-recovery.mjs';
import {collectWorkerProcessReadback,processScanBinding} from './asana-worker-process-readback.mjs';

// Operator-only, individually reviewed historical intake failures. This is not
// an automatic parser for "safe" shell commands and does not widen startup recovery.
// Hashes bind the entire ordered native activity projection, including tool
// arguments, exit statuses and unknown item types. Outputs are not replayed.
export const REVIEWED_INTAKES=Object.freeze([
 Object.freeze({key:'regime',run_id:'4b289fe1f93e4135bc70d4dcc2f8614e',agent_id:'vip-ai-finance',
  task_gid:'1219713644361869',assignee:'1215003777954631',
  thread_id:'01a1643d-3380-71b2-896e-b529cebd5333',turn_id:'01a1643d-35f8-7802-bbfc-3bc773ec47b3',
  started_at:1792683095,completed_at:1792683151,activity_count:18,
  activity_sha256:'7607c2762099c3411ed61f832845c363739b38ea4a8c94209ea89952702f015f',
  modified_at:'2026-10-21T07:15:00.346Z',run_started_at_ms:1792683082849,
  stories_sha256:'06f683354c8217e8d908ce1d6d667553af115f23b3d79cb53b8c76301b6477ac'}),
 Object.freeze({key:'sales',run_id:'958032d9d9fc46f5817957f633c3258c',agent_id:'vip-ai-sales',
  task_gid:'1219627999841544',assignee:'1214338261069890',
  thread_id:'01a152f8-c75e-75b3-bd65-ba316afb8a46',turn_id:'01a152f8-c9a0-77b1-90ad-2c76128b2c45',
  started_at:1792393398,completed_at:1792393834,activity_count:65,
  activity_sha256:'c1d645ddcd3068a47e85ac8ea8df99fef96ed5a06efd1f2ecb6d80a7243fddc2',
  modified_at:'2026-10-20T18:22:23.340Z',run_started_at_ms:1792393383246,
  stories_sha256:'291c1959c24c753e32ab50a1d9432db6dbc704eede26504ea83cb435f8de8f87',
  resource_key:'google-sheets:17nJW06-FijgWeqUDdv6TtRaZ7BBcBiQj0fziNMUxDWM:Akquise'})
]);
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const hash=x=>createHash('sha256').update(JSON.stringify(x)).digest('hex');
export function intakeNativeObservation(data,now=Date.now()) {
 const row=followupNativeObservation(data,now);
 return {...row,error:data.turns[0].error};
}
export function decideReviewedIntake(snapshot,proof,scan,now=Date.now()) {
 const hold=reason=>({action:'hold',reason});
 const {run,leases,signals}=snapshot;
 const t=REVIEWED_INTAKES.find(t=>t.run_id===run?.run_id);
 if(!t||run.agent_id!==t.agent_id||run.task_gid!==t.task_gid||run.thread_id!==t.thread_id||
  run.turn_id!==t.turn_id||run.started_at_ms!==t.run_started_at_ms||run.state!=='needs_reconciliation'||
  leases.length!==1||leases[0].key!=='task:'+t.task_gid||leases[0].run_id!==t.run_id||
  !signals.length||signals.some(s=>s.status!=='leased'||s.run_id!==t.run_id||s.task_gid!==t.task_gid||
   s.agent_id!==t.agent_id||s.lease_token!==leases[0].token||s.lease_fence!==leases[0].fence))
  return hold('reviewed_isolated_claim_required');
 const fresh=r=>Number.isSafeInteger(r?.observed_at_ms)&&r.observed_at_ms<=now&&now-r.observed_at_ms<=300000;
 const pair=rows=>Array.isArray(rows)&&rows.length===2&&rows.every(fresh)&&
  rows[1].observed_at_ms-rows[0].observed_at_ms>=30000&&
  same({...rows[0],observed_at_ms:null},{...rows[1],observed_at_ms:null});
 const ns=proof?.native;
 if(!pair(ns)||ns.some(n=>n.source!=='codex_app.read_thread'||n.host_id!=='local'||
  !['idle','notLoaded'].includes(n.runtime_status)||n.turn_status!=='failed'||
  n.error?.message!=='Selected model is at capacity. Please try a different model.'||
  ['thread_id','turn_id','started_at','completed_at','activity_count','activity_sha256'].some(k=>n[k]!==t[k])||
  n.completed_at*1000>n.observed_at_ms))return hold('complete_reviewed_native_activity_required');
 const as=proof.asana;
 if(!pair(as)||as.some(a=>a.agent_id!=='vip-ai-operations'||a.task?.gid!==t.task_gid||
  a.task.completed!==false||a.task.assignee?.gid!==t.assignee||a.task.modified_at!==t.modified_at||
  !Array.isArray(a.stories)||hash(a.stories)!==t.stories_sha256||a.next_page!==null))return hold('stable_operations_task_readback_required');
 // Sales has a later no-write checkpoint. Preserve it for the owner's fresh
 // intake; never mistake the old failed turn for a completed business task.
 const expectedStories=t.key==='sales'
  ?['1219607013733598','1219607013733605','1219635329205079','1219657915652807']
  :['1219713608197464','1219713608197470','1219713974704271','1219721052949223'];
 if(as.some(a=>!same(a.stories.map(s=>s.gid),expectedStories)))return hold('reviewed_story_inventory_changed');
 const remote=proof.remote;
 // Independent agents may hold unrelated remote locks. Bind the complete fresh
 // inventory, but require quiescence only for this incident's run/task/resource.
 // Remaining TTL and unrelated work can change between the two observations.
 if(!Array.isArray(remote)||remote.length!==2||remote.some(r=>!fresh(r)||
  r.agent_id!=='vip-ai-operations'||r.include_all!==true||!Array.isArray(r.active_locks)||
  r.active_locks.some(l=>!l||typeof l.key!=='string'||typeof l.run_id!=='string'||!l.run_id||
   typeof l.agent_id!=='string'||!l.agent_id||!['agent','task','resource'].includes(l.scope)||
   (l.scope==='task'&&(!/^\d+$/.test(l.task_gid||'')||l.key!=='task:'+l.task_gid))||
   (l.scope==='agent'&&l.key!=='agent:'+l.agent_id)||
   (l.scope==='resource'&&(typeof l.resource_key!=='string'||!l.resource_key||l.key!=='resource:'+l.resource_key))||
   l.run_id===t.run_id||l.task_gid===t.task_gid||l.key==='task:'+t.task_gid||
   (t.resource_key&&l.resource_key===t.resource_key)))||
  remote[1].observed_at_ms-remote[0].observed_at_ms<30000)return hold('remote_coordination_not_clear');
 const scans=[...(proof.processes||[]),scan];
 if(scans.length!==3||scans.some(r=>!fresh(r)||!r.scan_succeeded||
   Object.entries(processScanBinding(run)).some(([k,v])=>!same(r[k],v))||
   !/^[a-f0-9]{64}$/.test(r.raw_sha256||'')||!Array.isArray(r.matching_processes)||r.matching_processes.length)||
   scans[0].observed_at_ms<ns[0].observed_at_ms||scans[1].observed_at_ms<ns[1].observed_at_ms)
  return hold('worker_inactivity_unverified');
 return {action:'retry_after',reason:'reviewed_intake_without_business_writes',delay_ms:900000,
  task_completed:false,preserve_attempts:true,owner_fresh_readback_required:true};
}
export function applyReviewedIntake(store,snapshot,proof,{operator,apply=false,
 scan=collectWorkerProcessReadback,now=null}={}) {
 const processReadback=scan(snapshot.run);now??=Date.now();
 return store.transaction(()=>{
  if(!same(dispatchRecoverySnapshot(store,snapshot.run.run_id),snapshot))return {action:'hold',reason:'recovery_fence_changed'};
  const own=store.manualRun(operator?.run_id),held=own?store.leasesForRun(own.run_id):[];
  if(own?.state!=='active'||own.agent_id!=='vip-ai-operations'||own.thread_id!==operator?.thread_id||
   !operator.thread_id||operator.task_gid!=='1218924687084315'||
   !held.some(l=>l.key==='agent:vip-ai-operations'&&l.fence===own.agent_fence&&l.expires_at_ms>now)||
   !held.some(l=>l.key==='task:'+operator.task_gid&&l.expires_at_ms>now))
   return {action:'hold',reason:'current_operations_lease_required'};
  const lease=snapshot.leases[0];
  if(lease&&store.db.prepare('SELECT last_fence FROM lease_fences WHERE key=?').get(lease.key)?.last_fence!==lease.fence)
   return {action:'hold',reason:'lease_fence_history_changed'};
  const decision=decideReviewedIntake(snapshot,proof,processReadback,now);
  if(decision.action!=='retry_after'||!apply)return {...decision,dry_run:!apply};
  const note='Reviewed intake without business writes; task open; fresh owner readback required; proof='+hash(proof);
  for(const s of snapshot.signals) {
   const changes=store.db.prepare(`UPDATE signals SET status='retry_after',available_at_ms=?,run_id=NULL,
    lease_token=NULL,lease_fence=NULL,updated_at_ms=?,last_error=? WHERE id=? AND status='leased'
    AND run_id=? AND lease_token=? AND lease_fence=?`).run(now+decision.delay_ms,now,note,s.id,
     snapshot.run.run_id,s.lease_token,s.lease_fence).changes;
   if(changes!==1)throw new Error('Signal fence changed');
  }
  if(!store.releaseLease(lease))throw new Error('Task fence changed');
  store.db.prepare("UPDATE runs SET state='completed',updated_at_ms=?,last_error=? WHERE run_id=?").run(now,note,snapshot.run.run_id);
  store.db.prepare('INSERT INTO recovery_events(run_id,observed_at_ms,kind,evidence_json) VALUES (?,?,?,?)')
   .run(snapshot.run.run_id,now,'reviewed_intake_resume',JSON.stringify({snapshot,proof,operator,processReadback,decision}));
  return {...decision,dry_run:false};
 });
}
