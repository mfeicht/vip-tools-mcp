import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import {createHash} from 'node:crypto';
import {AsanaDispatchStore} from './asana-dispatch-store.mjs';
import {dispatchRecoverySnapshot} from './asana-dispatch-terminal-recovery.mjs';
import {applyFailedSpawnRecovery} from './asana-failed-spawn-recovery.mjs';
import {failedSpawnResumeSnapshot,applyFailedSpawnResume,decideFailedSpawnResume} from './asana-failed-spawn-resume.mjs';
import {PROCESS_SCAN_POLICY} from './asana-worker-process-readback.mjs';
import {parseSpawnResumeArguments} from './asana-failed-spawn-resume-cli.mjs';

function fixture(t) {
 const now=Date.now(),store=new AsanaDispatchStore(':memory:');t.after(()=>store.close());
 store.enqueue({id:'signal',agent_id:'vip-ai-developer',task_gid:'123',source_version:'v1',kind:'due_task',priority:60,available_at_ms:now-900000});
 const claim=store.claimNext('old',{now:now-900000});
 store.recordRun(claim,{state:'needs_reconciliation',error:'Error: spawn /Applications/ChatGPT.app/Contents/Resources/codex ENOENT',now:now-800000});
 const scan={source:'asana-failed-spawn-process-readback',host_id:'local',hostname:os.hostname(),run_id:'old',agent_id:'vip-ai-developer',thread_id:null,
  scan_policy:PROCESS_SCAN_POLICY,executable:'/bin/ps',args:['-axo','pid=,command='],scanner_sha256:createHash('sha256').update(fs.readFileSync(new URL('./asana-worker-process-readback.mjs',import.meta.url))).digest('hex'),
  observed_at_ms:now,scan_succeeded:true,raw_sha256:'a'.repeat(64),matching_processes:[]};
 const task={gid:'123',completed:false,modified_at:new Date(now-700000).toISOString(),assignee:{gid:'234'}};
 const row={agent_id:'vip-ai-operations',task,stories:[],next_page:null};
 const proof={read_agent_id:'vip-ai-operations',worker_record:{status:'needs_reconciliation',run_id:'old',agent_id:'vip-ai-developer',task_gid:'123',error:'spawn /Applications/ChatGPT.app/Contents/Resources/codex ENOENT'},
  processes:[{...scan,observed_at_ms:now-40000},{...scan,observed_at_ms:now-5000}],
  asana:[{...row,observed_at_ms:now-39000},{...row,observed_at_ms:now-4000}]};
 applyFailedSpawnRecovery(store,dispatchRecoverySnapshot(store,'old'),proof,{scan:()=>scan,now});
 const operator={run_id:'operator',thread_id:'12345678-1234-1234-1234-123456789012',task_gid:'456'};
 store.acquireManualAgent('vip-ai-operations',operator.run_id,{threadId:operator.thread_id,source:'CODEX_THREAD_ID'},now);
 store.acquireManualTask(operator.run_id,'456',{threadId:operator.thread_id,source:'CODEX_THREAD_ID'},now);
 return {store,proof,scan,now,operator,snapshot:failedSpawnResumeSnapshot(store,'old')};
}
test('audited never-started task is delayed, attempts preserved, operator untouched and audit atomic',t=>{
 const f=fixture(t),options={operator:f.operator,scan:()=>f.scan,now:f.now};
 assert.equal(applyFailedSpawnResume(f.store,f.snapshot,f.proof,options).action,'retry_after');
 assert.equal(f.store.counts().leased,1);
 assert.equal(applyFailedSpawnResume(f.store,f.snapshot,f.proof,{...options,apply:true}).action,'retry_after');
 const signal=f.store.db.prepare("SELECT * FROM signals WHERE id='signal'").get();
 assert.equal(signal.status,'retry_after');assert.equal(signal.attempts,1);assert.equal(signal.available_at_ms,f.now+900000);
 assert.equal(f.store.leasesForRun('old').length,0);assert.equal(f.store.leasesForRun('operator').length,2);
 assert.equal(f.store.db.prepare("SELECT count(*) n FROM recovery_events WHERE kind='failed_spawn_task_resume'").get().n,1);
 assert.equal(applyFailedSpawnResume(f.store,f.snapshot,f.proof,{...options,apply:true}).reason,'recovery_fence_changed');
});
test('old proof alone, active worker, changed task and different reading identity cannot resume',t=>{
 const f=fixture(t);
 for(const alter of [p=>p.asana[1].task={...p.asana[1].task,completed:true},p=>p.asana[1].next_page='more',
  p=>p.asana[1].agent_id='vip-ai-developer',p=>p.asana[1].task={...p.asana[1].task,modified_at:'invalid'},
  p=>p.processes[1].matching_processes=[{pid:123}],p=>p.processes[1].observed_at_ms=f.now-1000000]) {
  const p=structuredClone(f.proof);alter(p);assert.equal(decideFailedSpawnResume(f.snapshot,p,f.scan,f.now).action,'hold');
 }
 for(const alter of [s=>s.prior_events=[],s=>s.run.thread_id='real-thread',s=>s.signals[0].attempts++,
  s=>s.leases[0].fence++,s=>s.leases.push({...s.leases[0],key:'task:789'})]) {
  const s=structuredClone(f.snapshot);alter(s);assert.equal(decideFailedSpawnResume(s,f.proof,f.scan,f.now).action,'hold');
 }
 assert.equal(applyFailedSpawnResume(f.store,f.snapshot,f.proof,{operator:{...f.operator,thread_id:'foreign'},scan:()=>f.scan,now:f.now,apply:true}).reason,'current_operations_lease_required');
});
test('transaction rollback preserves task, run and signal when audit cannot persist',t=>{
 const f=fixture(t);f.store.db.exec("CREATE TRIGGER fail_audit BEFORE INSERT ON recovery_events BEGIN SELECT RAISE(ABORT,'audit failed'); END");
 assert.throws(()=>applyFailedSpawnResume(f.store,f.snapshot,f.proof,{operator:f.operator,scan:()=>f.scan,now:f.now,apply:true}),/audit failed/);
 assert.deepEqual(failedSpawnResumeSnapshot(f.store,'old'),f.snapshot);
});
test('CLI refuses arbitrary DB, identity and incomplete apply arguments',()=>{
 const good=['--run='+'a'.repeat(32),'--operations-run='+'b'.repeat(32),'--operations-task=456','--output=/tmp/result.json'];
 assert.equal(parseSpawnResumeArguments(good).apply,false);
 for(const extra of ['--db=/tmp/other','--agent=vip-ai-developer','--apply','--backup=/tmp/old.sqlite','--output=/tmp/duplicate'])
  assert.throws(()=>parseSpawnResumeArguments([...good,extra]));
});
