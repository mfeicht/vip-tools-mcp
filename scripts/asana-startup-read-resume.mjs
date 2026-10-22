import {createHash} from 'node:crypto';
import path from 'node:path';
import {dispatchRecoverySnapshot} from './asana-dispatch-terminal-recovery.mjs';
import {readDispatchNative} from './asana-manual-lease-readback.mjs';
import {collectWorkerProcessReadback,processScanBinding} from './asana-worker-process-readback.mjs';

const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const hash=x=>createHash('sha256').update(JSON.stringify(x)).digest('hex');
const root=path.resolve(import.meta.dirname,'../..');

// Reuse complete, bounded native pagination and retain exactly the items that
// produced its digest. No truncated app summaries or free-text attestations.
export async function readStartupNative(run,rpc) {
  const items=[],turns=[];
  const capture={call:async(method,params,timeout)=>{
    const response=await rpc.call(method,params,timeout);
    if(method==='thread/items/list')items.push(...response.data.map(e=>e.item));
    if(method==='thread/turns/list')turns.push(...response.data);
    return response;
  }};
  const native=await readDispatchNative(run,capture);
  return {...native,items,error:turns.find(t=>t.id===native.turn_id)?.error || null};
}

export function startupReadsOnly(row,workspace=root) {
  if(!Array.isArray(row?.items)||row.items.length!==row.activity_item_count||
    hash(row.items)!==row.activity_digest||row.items[0]?.id!==row.activity_last_item_id||
    row.turn_status!=='failed'||row.completed_at_ms===null||
    row.error?.message!=='Selected model is at capacity. Please try a different model.')return false;
  let commands=0,users=0;
  for(const item of row.items) {
    if(item.type==='userMessage'){users++;continue;}
    if(item.type==='reasoning')continue;
    if(item.type==='agentMessage'&&item.phase==='commentary')continue;
    if(item.type!=='commandExecution'||++commands!==1||item.cwd!==workspace||
      item.status!=='completed'||item.exitCode!==0||typeof item.command!=='string')return false;
    // Exact observed startup grammar. No substitutions, redirections, script
    // execution, arbitrary paths, networking, additional shell statements.
    if(!/^\/bin\/zsh -lc "pwd && sed -n '1,260p' AGENTS\.md && sed -n '1,260p' VIP-AI-Memory\/10-Agenten\/VIP-AI-[A-Za-z-]+\/Start-hier\.md"$/.test(item.command))return false;
  }
  return commands===1&&users===1;
}

export function decideStartupReadResume(snapshot,proof,scan,now=Date.now(),workspace=root) {
  const hold=reason=>({action:'hold',reason});
  const {run,leases,signals}=snapshot;
  if(run?.state!=='needs_reconciliation'||!run.thread_id||!run.turn_id||leases.length!==1||
    leases[0].key!==`task:${run.task_gid}`||!signals.length||signals.some(s=>
      s.status!=='leased'||s.run_id!==run.run_id||s.agent_id!==run.agent_id||s.task_gid!==run.task_gid||
      s.lease_token!==leases[0].token||s.lease_fence!==leases[0].fence))return hold('isolated_claim_required');
  const fresh=r=>Number.isSafeInteger(r?.observed_at_ms)&&r.observed_at_ms<=now&&now-r.observed_at_ms<=300000;
  const n=proof?.native;
  if(!Array.isArray(n)||n.length!==2||n.some(r=>!fresh(r)||!startupReadsOnly(r,workspace)||
    r.source!=='codex_app.read_thread'||r.host_id!=='local'||r.thread_id!==run.thread_id||r.turn_id!==run.turn_id||
    !['idle','notLoaded'].includes(r.runtime_status)||!Number.isSafeInteger(r.started_at_ms)||
    r.started_at_ms<run.started_at_ms-2000||!Number.isSafeInteger(r.completed_at_ms)||
    r.completed_at_ms<r.started_at_ms||r.completed_at_ms>r.observed_at_ms)||
    n[1].observed_at_ms-n[0].observed_at_ms<30000||
    !same({...n[0],observed_at_ms:null},{...n[1],observed_at_ms:null}))return hold('complete_startup_only_native_proof_required');
  const scans=[...(proof.processes || []),scan];
  if(scans.length!==3||scans.some(r=>!fresh(r)||!r.scan_succeeded||
    Object.entries(processScanBinding(run)).some(([k,v])=>!same(r[k],v))||
    !/^[a-f0-9]{64}$/.test(r.raw_sha256||'')||!Array.isArray(r.matching_processes)||r.matching_processes.length)||
    scans[0].observed_at_ms<n[0].observed_at_ms||scans[1].observed_at_ms<n[1].observed_at_ms)
    return hold('worker_inactivity_unverified');
  const a=proof.asana;
  if(!Array.isArray(a)||a.length!==2||a.some(r=>!fresh(r)||r.agent_id!=='vip-ai-operations'||
    r.task?.gid!==run.task_gid||r.task.completed!==false||!/^\d+$/.test(r.task.assignee?.gid||'')||
    !Number.isFinite(Date.parse(r.task.modified_at))||!Array.isArray(r.stories)||r.next_page!==null)||
    a[1].observed_at_ms-a[0].observed_at_ms<30000||
    !same({...a[0],observed_at_ms:null},{...a[1],observed_at_ms:null}))return hold('stable_operations_task_readback_required');
  return {action:'retry_after',reason:'capacity_failure_after_verified_startup_reads_only',delay_ms:900000,
    task_completed:false,preserve_attempts:true};
}

export function applyStartupReadResume(store,snapshot,proof,{operator,apply=false,now=Date.now(),
  scan=collectWorkerProcessReadback,workspace=root}={}) {
  const processReadback=scan(snapshot.run);
  return store.transaction(()=>{
    if(!same(dispatchRecoverySnapshot(store,snapshot.run.run_id),snapshot))return {action:'hold',reason:'recovery_fence_changed'};
    const manual=store.manualRun(operator?.run_id),leases=manual?store.leasesForRun(manual.run_id):[];
    if(manual?.state!=='active'||manual.agent_id!=='vip-ai-operations'||manual.thread_id!==operator?.thread_id||
      !operator.thread_id||!leases.some(l=>l.key==='agent:vip-ai-operations'&&l.fence===manual.agent_fence&&l.expires_at_ms>now)||
      !leases.some(l=>l.key===`task:${operator.task_gid}`&&l.expires_at_ms>now))return {action:'hold',reason:'current_operations_lease_required'};
    const decision=decideStartupReadResume(snapshot,proof,processReadback,now,workspace);
    if(decision.action!=='retry_after'||!apply)return {...decision,dry_run:!apply};
    const note=`Verified startup reads only; capacity failure; task remains open; proof=${hash(proof)}`;
    for(const s of snapshot.signals) {
      const changed=store.db.prepare(`UPDATE signals SET status='retry_after',available_at_ms=?,run_id=NULL,
        lease_token=NULL,lease_fence=NULL,updated_at_ms=?,last_error=? WHERE id=? AND status='leased'
        AND run_id=? AND lease_token=? AND lease_fence=?`).run(now+decision.delay_ms,now,note,s.id,
          snapshot.run.run_id,s.lease_token,s.lease_fence).changes;
      if(changed!==1)throw new Error('Signal fence changed');
    }
    if(!store.releaseLease(snapshot.leases[0]))throw new Error('Task fence changed');
    store.db.prepare("UPDATE runs SET state='completed',updated_at_ms=?,last_error=? WHERE run_id=?")
      .run(now,note,snapshot.run.run_id);
    store.db.prepare('INSERT INTO recovery_events(run_id,observed_at_ms,kind,evidence_json) VALUES (?,?,?,?)')
      .run(snapshot.run.run_id,now,'startup_read_task_resume',JSON.stringify({snapshot,proof,operator,processReadback,decision}));
    return {...decision,dry_run:false};
  });
}
