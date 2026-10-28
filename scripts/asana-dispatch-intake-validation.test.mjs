import assert from "node:assert/strict";
import test from "node:test";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";
import { assertSnapshotIdentities, assertTaskRows, scanAgent } from "./asana-dispatch-poller.mjs";

const task = () => ({gid:"123",name:"Fixture",completed:false,modified_at:"2026-10-20T10:00:00Z",
  assignee:{gid:"456"},due_on:"2026-10-20",due_at:null,tags:[]});
const snapshot = () => ({agent_id:"vip-ai-research",ok:true,truncated:false,
  user_gid:"456",workspace_gid:"111",tasks:[task()]});
const story = () => ({gid:"777",target:{gid:"123"},created_at:"2026-10-20T11:00:00Z",
  type:"comment",created_by:{gid:"900"},text:"Please review the open point."});
const client = (tasks=[],stories=[]) => ({async callTool({name}) {
  const value=name==="asana_search_tasks" ? {search_status:"ok",search_complete:true,result_truncated:false,tasks}
    : {response:{data:stories,next_page:null}};
  return {content:[{type:"text",text:JSON.stringify(value)}]};
}});

test("snapshot membership is exact and unique before scanning any agent",()=>{
  const a=snapshot(),b={...snapshot(),agent_id:"vip-ai-sales",user_gid:"457"};
  assert.doesNotThrow(()=>assertSnapshotIdentities({agents:[b,a]},[a.agent_id,b.agent_id]));
  for(const agents of [[a,a],[a,b,a],[a],[],[a,{...b,agent_id:"unknown"}],
    [a,{...b,truncated:undefined}],[a,{...b,ok:1}],[a,{...b,user_gid:456}],
    [a,{...b,workspace_gid:null}],[a,{...b,tasks:null}],[a,null]]){
    assert.throws(()=>assertSnapshotIdentities({agents},[a.agent_id,b.agent_id]),/identity snapshot incomplete/);
  }
  assert.throws(()=>assertSnapshotIdentities({agents:[a,a]},[a.agent_id,a.agent_id]),/identity snapshot incomplete/);
});

test("complete empty, undated, unassigned and completed task results remain valid",()=>{
  assert.doesNotThrow(()=>assertTaskRows([],"test"));
  assert.doesNotThrow(()=>assertTaskRows([{...task(),assignee:null,due_on:null,completed:true}],"test"));
});

test("malformed rows from either source never advance observations, queue or cursor",async()=>{
  const variants=[null,{...task(),gid:123},{...task(),gid:""},{...task(),completed:undefined},
    {...task(),completed:"false"},{...task(),name:undefined},{...task(),assignee:undefined},
    {...task(),assignee:{gid:456}},{...task(),modified_at:"yesterday"},
    {...task(),due_on:undefined},{...task(),due_at:undefined},{...task(),due_at:"tomorrow"},
    {...task(),due_on:"2026-02-30"},{...task(),tags:undefined},{...task(),tags:[{}]}];
  for(const source of ["assigned","search"]){
    for(const rows of [...variants.map(row=>[row]),[task(),task()]]){
      const store=new AsanaDispatchStore(":memory:");
      try{
        const before="2026-10-19T00:00:00Z";
        store.setPollCursor("vip-ai-research",before);
        store.observeTask({agent_id:"vip-ai-research",task_gid:"123",modified_at:before},1000);
        const observation=store.observation("vip-ai-research","123");
        const input=snapshot();if(source==="assigned")input.tasks=rows;
        await assert.rejects(scanAgent(client(source==="search"?rows:[],[story()]),store,input,
          new Set(["456"]),new Date("2026-10-21T10:00:00Z"),true),/Invalid or duplicate task/);
        assert.equal(store.pollCursor("vip-ai-research"),before);
        assert.deepEqual(store.observation("vip-ai-research","123"),observation);
        assert.deepEqual(store.counts(),{});
      }finally{store.close();}
    }
  }
});

test("foreign or missing story target cannot create a signal or consume an audit",async()=>{
  for(const target of [undefined,null,{gid:"999"},{gid:123}]){
    const store=new AsanaDispatchStore(":memory:");
    try{
      await assert.rejects(scanAgent(client([],[{...story(),target}]),store,snapshot(),
        new Set(["456"]),new Date("2026-10-21T10:00:00Z"),true),/Story target mismatch/);
      assert.equal(store.pollCursor("vip-ai-research"),null);
      assert.equal(store.observation("vip-ai-research","123"),null);
      assert.deepEqual(store.counts(),{});
    }finally{store.close();}
  }
});

test("valid source overlap remains one task and preserves due and comment signals",async()=>{
  const store=new AsanaDispatchStore(":memory:");
  try{
    const scan=await scanAgent(client([task()],[story()]),store,snapshot(),new Set(["456"]),
      new Date("2026-10-21T10:00:00Z"),true);
    assert.equal(scan.tasks_scanned,1);assert.equal(scan.signals_inserted,2);
  }finally{store.close();}
});
