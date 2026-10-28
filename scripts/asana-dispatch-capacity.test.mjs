import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {AsanaDispatchStore} from './asana-dispatch-store.mjs';
import {workerCapacity,watch} from './asana-dispatch-watch.mjs';
const signal=(id,agent='agent-a',task='task-a',extra={})=>({id,agent_id:agent,task_gid:task,kind:'human_comment',source_version:id,priority:90,available_at_ms:1000,...extra});
function store(t){const s=new AsanaDispatchStore(':memory:');t.after(()=>s.close());return s;}
const health=(at,extra={})=>({started_at:new Date(at).toISOString(),finished_at:new Date(at+1).toISOString(),status:'ok',dispatch_enabled:true,poll:{agents:[]},errors:[],ready:1,workers_started:0,...extra});
test('old history capacity stays unknown after additive migration and reopen',t=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'capacity-migration-'));t.after(()=>fs.rmSync(root,{recursive:true,force:true}));const p=path.join(root,'db');const d=new DatabaseSync(p);
 d.exec("CREATE TABLE poll_runs(started_at_ms INTEGER PRIMARY KEY,finished_at_ms INTEGER NOT NULL,status TEXT NOT NULL);INSERT INTO poll_runs VALUES(1000,2000,'ok')");d.close();
 for(let i=0;i<2;i++){const s=new AsanaDispatchStore(p);assert.deepEqual({...s.db.prepare('SELECT claimable_agents,available_worker_slots FROM poll_runs').get()},{claimable_agents:null,available_worker_slots:null});s.close();}
});
test('two active or unresolved agent leases explain zero slots without freeing them',t=>{
 const s=store(t);s.enqueue(signal('one'),1000);s.acquireLease('agent:busy-a','run-a',60000,1000);s.acquireLease('agent:busy-b','run-b',60000,1000);const before=s.activeLeases();
 assert.deepEqual(workerCapacity(s,2,true,62000),{started:0,claimableAgents:1,slots:0,agents:[]});assert.deepEqual(s.activeLeases(),before);
});
test('capacity uses the exact claim selection including dedupe task fences future and coordination',t=>{
 const s=store(t);s.enqueue(signal('one'),1000);s.enqueue(signal('two'),1000);s.enqueue(signal('three','agent-b','task-b'),1000);s.enqueue(signal('four','agent-c','task-c'),1000);
 s.enqueue(signal('future','agent-d','task-d',{available_at_ms:4000}),1000);s.enqueue(signal('undated','agent-e','task-e',{kind:'new_assigned_task'}),1000);
 s.enqueue(signal('routine','agent-f','task-f',{kind:'due_task',requires_coordination:true}),1000);s.acquireLease('task:task-b','old-run',60000,1000);
 const before=JSON.stringify(s.db.prepare('SELECT * FROM signals ORDER BY id').all());
 const c=workerCapacity(s,2,false,2000);assert.equal(c.claimableAgents,2);assert.deepEqual(c.agents,['agent-a','agent-c']);assert.deepEqual(c.agents,s.readyAgentIds(2000,2,false));
 assert.equal(workerCapacity(s,2,true,2000).claimableAgents,3);assert.equal(JSON.stringify(s.db.prepare('SELECT * FROM signals ORDER BY id').all()),before);
});
test('maintenance is unmeasured and zero worker limit is a measured zero',t=>{
 const s=store(t);s.enqueue(signal('one'),1000);assert.deepEqual(workerCapacity(s,0,true,2000),{started:0,claimableAgents:1,slots:0,agents:[]});
 const context={threadId:'12345678-1234-1234-1234-123456789012',source:'CODEX_THREAD_ID'};
 s.acquireManualAgent('vip-ai-operations','operator',context);s.acquireManualTask('operator','456',context);s.setDispatchMaintenance('operator',context);
 assert.deepEqual(workerCapacity(s),{started:0,claimableAgents:null,slots:null,agents:[]});
 for(const n of [-1,3,NaN,1.5])assert.throws(()=>workerCapacity(s,n));
});
test('history persists paired samples and distinguishes unknown from measured idle',t=>{
 const s=store(t),day=86400000;s.recordPollRun(health(day),{now:day*4});
 s.recordPollRun(health(day*2,{claimable_agents:0,available_worker_slots:1}),{now:day*4});
 s.recordPollRun(health(day*3,{claimable_agents:4,available_worker_slots:0}),{now:day*4});
 const stats=s.pollStats(0);assert.equal(stats.capacity_samples,2);assert.equal(stats.idle_no_claim_polls,1);
 assert.equal(s.db.prepare('SELECT claimable_agents FROM poll_runs WHERE started_at_ms=?').get(day).claimable_agents,null);
 for(const extra of [{claimable_agents:0},{claimable_agents:true,available_worker_slots:0},{claimable_agents:0,available_worker_slots:'0'},{claimable_agents:-1,available_worker_slots:0}])assert.throws(()=>s.recordPollRun(health(day*4,extra),{now:day*4}));
 assert.equal(s.db.prepare('SELECT COUNT(*) AS n FROM poll_runs').get().n,3);
});
test('failed poll cannot invent capacity or start workers',async()=>{
 const h=await watch({dispatch:true,runPoll:async()=>{throw new Error('fixture');},makeStore:()=>{throw new Error('unavailable');},writeHealth:async()=>{}});
 assert.equal(h.workers_started,0);assert.equal(h.claimable_agents,null);assert.equal(h.available_worker_slots,null);
});
