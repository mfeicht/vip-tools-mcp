import test from 'node:test';
import assert from 'node:assert/strict';
import {AsanaDispatchStore} from './asana-dispatch-store.mjs';
import {executeLeaseCommand} from './asana-agent-lease.mjs';
function fixture(t,agent='vip-ai-operations') {
 const now=Date.now(),store=new AsanaDispatchStore(':memory:');t.after(()=>store.close());
 const context={threadId:'12345678-1234-1234-1234-123456789012',source:'CODEX_THREAD_ID'};
 store.acquireManualAgent(agent,'operator',context,now);
 store.acquireManualTask('operator','456',context,now);
 store.enqueue({id:'s1',agent_id:'vip-ai-finance',task_gid:'123',source_version:'v1',kind:'due_task',priority:60,available_at_ms:now});
 return {store,now,context};
}
test('bounded pause blocks new atomic claims without consuming signals, then expires',t=>{
 const f=fixture(t);f.store.setDispatchMaintenance('operator',f.context,{now:f.now});
 assert.equal(f.store.claimNext('new',{now:f.now+1000}),null);
 const s=f.store.db.prepare('SELECT * FROM signals').get();assert.equal(s.attempts,0);assert.equal(s.status,'pending');
 assert.equal(f.store.dispatchMaintenance(f.now+900000),null);
 assert.ok(f.store.claimNext('after-expiry',{now:f.now+900000}));
});
test('already running claim, renewal and task settlement are untouched by admission pause',t=>{
 const f=fixture(t),claim=f.store.claimNext('running',{now:f.now});
 const before=f.store.leasesForRun('running');
 f.store.setDispatchMaintenance('operator',f.context,{now:f.now+1});
 assert.deepEqual(f.store.leasesForRun('running'),before);
 assert.ok(f.store.acquireLease(claim.agent_lease.key,'running',120000,f.now+100));
 assert.equal(f.store.db.prepare('SELECT status FROM signals').get().status,'leased');
});
test('explicit resume and normal operator release immediately reopen admission',t=>{
 const f=fixture(t);f.store.setDispatchMaintenance('operator',f.context,{now:f.now});
 f.store.setDispatchMaintenance('operator',f.context,{pause:false,now:f.now+1});
 assert.equal(f.store.dispatchMaintenance(f.now+2),null);
 f.store.setDispatchMaintenance('operator',f.context,{now:f.now+3});
 f.store.finishManualLeaseCommand('operator','release',f.context,f.now+4);
 assert.equal(f.store.dispatchMaintenance(f.now+5),null);
 assert.ok(f.store.claimNext('after-release',{now:f.now+5}));
});
test('wrong agent, foreign thread, expired owner and missing task cannot pause',t=>{
 const f=fixture(t,'vip-ai-finance');assert.throws(()=>f.store.setDispatchMaintenance('operator',f.context,{now:f.now}),/Operations task/);
 const g=fixture(t);
 assert.throws(()=>g.store.setDispatchMaintenance('operator',{...g.context,threadId:'87654321-1234-1234-1234-123456789012'},{now:g.now}),/another thread/);
 assert.throws(()=>g.store.setDispatchMaintenance('operator',g.context,{now:g.now+7200001}),/Fresh Operations/);
 g.store.releaseLease(g.store.leasesForRun('operator').find(l=>l.key==='task:456'));
 assert.throws(()=>g.store.setDispatchMaintenance('operator',g.context,{now:g.now}),/Operations task/);
 assert.equal(g.store.dispatchMaintenance(g.now),null);
});
test('audit failure rolls back admission pause and CLI rejects dispatch-owned run',t=>{
 const f=fixture(t);f.store.db.exec("CREATE TRIGGER fail BEFORE INSERT ON recovery_events BEGIN SELECT RAISE(ABORT,'audit failed'); END");
 assert.throws(()=>f.store.setDispatchMaintenance('operator',f.context,{now:f.now}),/audit failed/);
 assert.equal(f.store.dispatchMaintenance(f.now),null);
 assert.throws(()=>executeLeaseCommand(f.store,{command:'maintenance-pause',run:'operator'},
  {CODEX_THREAD_ID:f.context.threadId,VIP_ASANA_DISPATCH_RUN_ID:'dispatcher'}),/Manual Operations/);
});
