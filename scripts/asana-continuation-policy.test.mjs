import test from "node:test";
import assert from "node:assert/strict";
import {AsanaDispatchStore} from "./asana-dispatch-store.mjs";

const now=200000000;
const signal=(id="continue-1",kind="continuation")=>({id,kind,agent_id:"vip-ai-test",task_gid:"123",
  source_version:id,priority:55,available_at_ms:now});
function setup(t) {const s=new AsanaDispatchStore(":memory:");t.after(()=>s.close());return s;}

test("progress waits an hour, missing next step waits a day, duplicates collapse",t=>{
  const s=setup(t);
  s.scheduleContinuation(signal(),"Read the new task result",now);
  s.scheduleContinuation(signal("continue-2"),"Review",now);
  assert.equal(s.claimNext("too-early",{now:now+3599999}),null);
  assert.equal(s.db.prepare("SELECT COUNT(*) AS n FROM signals WHERE status='pending'").get().n,1);
  assert.ok(s.claimNext("ready-run",{now:now+3600000}));
  const second={...signal("continue-3"),agent_id:"vip-ai-other",task_gid:"456"};
  s.scheduleContinuation(second,null,now);
  assert.equal(s.db.prepare("SELECT available_at_ms AS at FROM signals WHERE id='continue-3'").get().at,now+86400000);
});

test("four automatic starts defer more continuation but a human signal still runs",t=>{
  const s=setup(t);
  for(let i=0;i<4;i++) s.db.prepare("INSERT INTO continuation_starts VALUES (?,?,?,?)")
    .run(`old-${i}`,"vip-ai-test","123",now-7200000+i*1000);
  s.enqueue(signal(),now);
  assert.equal(s.claimNext("limited-run",{now}),null);
  s.enqueue(signal("human-1","human_comment"),now);
  const claim=s.claimNext("human-run",{now});
  assert.ok(claim);assert.equal(claim.signals.length,1);
  assert.equal(s.beginCodexLaunch(claim,now+1000),true);
  assert.equal(s.db.prepare("SELECT COUNT(*) AS n FROM continuation_starts").get().n,4);
  s.settle(claim,{outcome:"acknowledged",now:now+2000});
  assert.equal(s.db.prepare("SELECT status FROM signals WHERE id='continue-1'").get().status,"acknowledged");
});

test("one worker per distinct unlocked agent and no settlement with unknown extra tasks",t=>{
  const s=setup(t);
  s.enqueue(signal("human-1","human_comment"),now);
  s.enqueue({...signal("human-2","human_comment"),task_gid:"456"},now);
  s.enqueue({...signal("human-3","human_comment"),agent_id:"vip-ai-other",task_gid:"789"},now);
  assert.deepEqual(s.readyAgentIds(now,2),["vip-ai-other","vip-ai-test"].sort((a,b)=>a.localeCompare(b)));
  const claim=s.claimNext("new-run",{now,agentId:"vip-ai-test"});
  s.acquireLease("task:999",claim.run_id,60000,now);
  assert.throws(()=>s.settle(claim,{outcome:"acknowledged",now}),/Extra run leases/);
  assert.equal(s.activeLeases().length,3);
});

test("worker capacity is checked inside the claim transaction",t=>{
  const s=setup(t);
  for(let i=0;i<3;i++)s.enqueue({...signal(`human-${i}`,"human_comment"),
    agent_id:`vip-ai-agent${i}`,task_gid:`task-${i}`},now);
  assert.ok(s.claimNext("run-one",{now,maxActiveAgents:2}));
  assert.ok(s.claimNext("run-two",{now,maxActiveAgents:2}));
  assert.equal(s.claimNext("run-three",{now,maxActiveAgents:2}),null);
  assert.equal(s.activeLeases().filter(l=>l.key.startsWith("agent:")).length,2);
});

test("progress signal and lease settlement roll back together on queue failure",t=>{
  const s=setup(t);
  s.enqueue(signal("human-1","human_comment"),now);
  const claim=s.claimNext("run-1",{now});
  s.db.exec("CREATE TRIGGER queue_failure BEFORE INSERT ON signals BEGIN SELECT RAISE(ABORT,'queue failure'); END");
  assert.throws(()=>s.settle(claim,{outcome:"acknowledged",now:now+1000,
    continuation:{signal:signal("follow-up"),nextStep:"Read next result"}}),/queue failure/);
  assert.equal(s.activeLeases().length,2);
  assert.deepEqual(s.counts(),{leased:1});
});
