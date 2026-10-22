import test from 'node:test';import assert from 'node:assert/strict';
import fs from 'node:fs';import os from 'node:os';import {createHash} from 'node:crypto';
import {AsanaDispatchStore} from './asana-dispatch-store.mjs';
import {dispatchRecoverySnapshot} from './asana-dispatch-terminal-recovery.mjs';
import {decideFailedSpawnRecovery,applyFailedSpawnRecovery} from './asana-failed-spawn-recovery.mjs';
import {PROCESS_SCAN_POLICY} from './asana-worker-process-readback.mjs';
function fixture(t){
 const now=Date.now(),store=new AsanaDispatchStore(':memory:');t.after(()=>store.close());
 store.enqueue({id:'s1',agent_id:'vip-ai-developer',task_gid:'123',source_version:'v1',kind:'due_task',priority:10,available_at_ms:now-900000});
 const claim=store.claimNext('run1',{now:now-900000});
 store.recordRun(claim,{state:'needs_reconciliation',error:'Error: spawn /Applications/ChatGPT.app/Contents/Resources/codex ENOENT',now:now-800000});
 const snapshot=dispatchRecoverySnapshot(store,'run1');
 const scan={source:'asana-failed-spawn-process-readback',host_id:'local',hostname:os.hostname(),
  run_id:'run1',agent_id:'vip-ai-developer',thread_id:null,scan_policy:PROCESS_SCAN_POLICY,
  executable:'/bin/ps',args:['-axo','pid=,command='],scanner_sha256:createHash('sha256').update(fs.readFileSync(new URL('./asana-worker-process-readback.mjs',import.meta.url))).digest('hex'),
  observed_at_ms:now,scan_succeeded:true,raw_sha256:'a'.repeat(64),matching_processes:[]};
 const asana={task:{gid:'123',completed:false,modified_at:new Date(now-600000).toISOString()},stories:[],next_page:null};
 const proof={read_agent_id:'vip-ai-operations',worker_record:{status:'needs_reconciliation',run_id:'run1',agent_id:'vip-ai-developer',task_gid:'123',error:snapshot.run.last_error.slice(7)},
  processes:[{...scan,observed_at_ms:now-40000},{...scan,observed_at_ms:now-5000}],
  asana:[{...asana,observed_at_ms:now-39000},{...asana,observed_at_ms:now-4000}]};
 return {store,snapshot,proof,scan,now};
}
test('proven ENOENT releases only agent and persists the complete audit atomically',t=>{
 const f=fixture(t);assert.equal(applyFailedSpawnRecovery(f.store,f.snapshot,f.proof,{scan:()=>f.scan,now:f.now}).action,'quarantine');
 assert.deepEqual(f.store.activeLeases().map(l=>l.key),['task:123']);assert.deepEqual(f.store.counts(),{leased:1});
 assert.equal(f.store.db.prepare('SELECT state FROM runs').get().state,'needs_reconciliation');
 assert.equal(f.store.db.prepare('SELECT COUNT(*) AS n FROM recovery_events').get().n,1);
 assert.equal(applyFailedSpawnRecovery(f.store,f.snapshot,f.proof,{scan:()=>f.scan,now:f.now}).reason,'recovery_fence_changed');
});
test('missing independent log, process activity, incomplete Asana and changed fences hold',t=>{
 const f=fixture(t);
 for(const alter of [p=>p.worker_record.error='different',p=>p.processes[1].matching_processes=[{pid:1}],p=>p.asana[1].next_page='more',p=>p.processes[0].scanner_sha256='b'.repeat(64)]){
  const proof=structuredClone(f.proof);alter(proof);assert.equal(decideFailedSpawnRecovery(f.snapshot,proof,f.scan,f.now).action,'hold');
 }
 assert.equal(decideFailedSpawnRecovery({...f.snapshot,run:{...f.snapshot.run,thread_id:'has-thread'}},f.proof,f.scan,f.now).action,'hold');
 f.store.acquireLease('agent:vip-ai-developer','run1',120000,f.now);
 assert.equal(applyFailedSpawnRecovery(f.store,f.snapshot,f.proof,{scan:()=>f.scan,now:f.now}).reason,'recovery_fence_changed');
});
test('failed audit write rolls back the agent release',t=>{
 const f=fixture(t);f.store.db.exec("CREATE TRIGGER crash BEFORE INSERT ON recovery_events BEGIN SELECT RAISE(ABORT,'crash'); END");
 assert.throws(()=>applyFailedSpawnRecovery(f.store,f.snapshot,f.proof,{scan:()=>f.scan,now:f.now}),/crash/);
 assert.equal(f.store.activeLeases().length,2);
});
