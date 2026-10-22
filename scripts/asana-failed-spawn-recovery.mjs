import fs from 'node:fs';
import os from 'node:os';
import {createHash} from 'node:crypto';
import {dispatchRecoverySnapshot} from './asana-dispatch-terminal-recovery.mjs';
import {collectFailedSpawnProcessReadback,PROCESS_SCAN_POLICY} from './asana-worker-process-readback.mjs';
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const hash=x=>createHash('sha256').update(x).digest('hex');

export function decideFailedSpawnRecovery(snapshot,proof,scan,now=Date.now()) {
  const hold=reason=>({action:'hold',reason});
  const {run,leases,signals}=snapshot;
  if(!run || run.thread_id!==null || run.turn_id!==null || run.state!=='needs_reconciliation' ||
    !/^Error: spawn \/Applications\/(?:ChatGPT|Codex)\.app\/Contents\/Resources\/codex ENOENT$/.test(run.last_error || ''))
    return hold('spawn_failure_not_verified');
  const row=proof?.worker_record;
  if(row?.status!=='needs_reconciliation' || row.run_id!==run.run_id || row.agent_id!==run.agent_id ||
    row.task_gid!==run.task_gid || `Error: ${row.error}`!==run.last_error || row.thread_id || row.turn_id)
    return hold('independent_worker_record_missing');
  const agent=leases.find(l=>l.key===`agent:${run.agent_id}`),task=leases.find(l=>l.key===`task:${run.task_gid}`);
  if(!agent || !task || leases.length!==2 || !signals.length || signals.some(s=>s.status!=='leased' ||
    s.run_id!==run.run_id || s.agent_id!==run.agent_id || s.task_gid!==run.task_gid ||
    s.lease_token!==task.token || s.lease_fence!==task.fence)) return hold('claim_unverified');
  const fresh=r=>Number.isSafeInteger(r?.observed_at_ms) && r.observed_at_ms<=now+1000 && now-r.observed_at_ms<=300000;
  const validScan=r=>fresh(r) && r.source==='asana-failed-spawn-process-readback' && r.host_id==='local' &&
    r.hostname===os.hostname() && r.run_id===run.run_id && r.agent_id===run.agent_id && r.thread_id===null &&
    r.scan_policy===PROCESS_SCAN_POLICY && r.executable==='/bin/ps' && same(r.args,['-axo','pid=,command=']) &&
    r.scanner_sha256===hash(fs.readFileSync(new URL('./asana-worker-process-readback.mjs',import.meta.url))) &&
    r.scan_succeeded===true && /^[a-f0-9]{64}$/.test(r.raw_sha256 || '') &&
    Array.isArray(r.matching_processes) && !r.matching_processes.length;
  if(!Array.isArray(proof.processes) || proof.processes.length!==2 || !proof.processes.every(validScan) ||
    proof.processes[1].observed_at_ms-proof.processes[0].observed_at_ms<30000 || !validScan(scan))
    return hold('worker_inactivity_unverified');
  if(proof.read_agent_id!=='vip-ai-operations' || !Array.isArray(proof.asana) || proof.asana.length!==2 ||
    proof.asana.some(r=>!fresh(r) || r.task?.gid!==run.task_gid || typeof r.task.completed!=='boolean' ||
      !r.task.modified_at || !Array.isArray(r.stories) || r.next_page!==null) ||
    proof.asana[1].observed_at_ms-proof.asana[0].observed_at_ms<1000 ||
    !same({...proof.asana[0],observed_at_ms:null},{...proof.asana[1],observed_at_ms:null}))
    return hold('asana_readback_unverified');
  return {action:'quarantine',release_agent:true,release_task:false,reason:'verified_failed_spawn_agent_only'};
}

export function applyFailedSpawnRecovery(store,snapshot,proof,{scan=collectFailedSpawnProcessReadback,now=Date.now()}={}) {
  const processReadback=scan(snapshot.run);
  return store.transaction(()=>{
    if(!same(dispatchRecoverySnapshot(store,snapshot.run.run_id),snapshot))return {action:'hold',reason:'recovery_fence_changed'};
    const decision=decideFailedSpawnRecovery(snapshot,proof,processReadback,now);
    if(decision.action==='hold')return decision;
    const agent=snapshot.leases.find(l=>l.key===`agent:${snapshot.run.agent_id}`);
    if(!store.releaseLease(agent))throw new Error('Agent fence changed');
    store.db.prepare('UPDATE runs SET updated_at_ms=?,last_reconcile_check_at_ms=? WHERE run_id=?')
      .run(now,now,snapshot.run.run_id);
    store.db.prepare('INSERT INTO recovery_events(run_id,observed_at_ms,kind,evidence_json) VALUES (?,?,?,?)')
      .run(snapshot.run.run_id,now,'legacy_spawn_agent_quarantine',JSON.stringify({snapshot,proof,processReadback,decision}));
    return decision;
  });
}
