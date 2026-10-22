import test from "node:test";
import assert from "node:assert/strict";
import {reconcileManualLeases} from "./asana-manual-lease-reconcile.mjs";

test("empty intake never opens connections or changes leases",async()=>{
  const result=await reconcileManualLeases({manualRunsForReconciliation:()=>[]},{
    connect:()=>{throw new Error("unexpected connection");},
    makeRpc:()=>{throw new Error("unexpected RPC");}
  });
  assert.equal(result.checked,0);
});

test("one connection failure is not retried and does not block an agent-only read",async()=>{
  let connections=0,closed=0,checks=0,reads=0;
  const rows=[{run_id:"with-task",tasks:[{task_gid:"123"}]},
    {run_id:"agent-only",tasks:[]}];
  const store={manualRunsForReconciliation:()=>rows,
    reconcileManualRun:()=>({applied:false,reason:"fixture_hold"}),
    markManualRunChecked:()=>checks++};
  const result=await reconcileManualLeases(store,{
    connect:async()=>{connections++;throw new Error("503");},
    makeRpc:()=>({start:async()=>{},close:async()=>{closed++;}}),
    read:async run=>{assert.equal(run.run_id,"agent-only");reads++;return {};}
  });
  assert.equal(connections,1);assert.equal(closed,1);assert.equal(checks,2);
  assert.equal(reads,1);assert.equal(result.errors.length,1);assert.equal(result.held,1);
});

test("applied partial recovery is counted separately and all clients close",async()=>{
  let closed=0;
  const store={manualRunsForReconciliation:()=>[{run_id:"run-1",tasks:[{task_gid:"123"}]}],
    reconcileManualRun:()=>({applied:true,decision:{action:"partial_quarantine"}}),
    markManualRunChecked:()=>{}};
  const result=await reconcileManualLeases(store,{
    connect:async()=>({close:async()=>{closed++;}}),
    makeRpc:()=>({start:async()=>{},close:async()=>{closed++;}}),read:async()=>({})
  });
  assert.equal(result.partial_quarantine,1);assert.equal(result.released,0);assert.equal(closed,2);
});

test("one unreadable native manual run does not block another agent",async()=>{
  const store={manualRunsForReconciliation:()=>[{run_id:'active',tasks:[]},{run_id:'done',tasks:[]}],
    reconcileManualRun:()=>({applied:true,decision:{action:'release'}}),markManualRunChecked:()=>{}};
  const result=await reconcileManualLeases(store,{
    makeRpc:()=>({start:async()=>{},close:async()=>{}}),
    read:async run=>{if(run.run_id==='active')throw new Error('Native activity items incomplete');return {};}
  });
  assert.deepEqual(result.errors,[]);assert.equal(result.held,1);assert.equal(result.released,1);
});
