import test from 'node:test';
import assert from 'node:assert/strict';
import {AsanaDispatchStore} from './asana-dispatch-store.mjs';
import {manualResumeSnapshot,decideReviewedManual} from './asana-reviewed-manual-resume.mjs';
test('manual recovery rejects unrelated, dispatcher and incomplete claims before any mutation',()=>{
 const s=new AsanaDispatchStore(':memory:');try {
  s.acquireManualAgent('vip-ai-test','unrelated',{threadId:'11111111-1111-1111-1111-111111111111',source:'CODEX_THREAD_ID'});
  s.acquireManualTask('unrelated','123',{threadId:'11111111-1111-1111-1111-111111111111',source:'CODEX_THREAD_ID'});
  const snapshot=manualResumeSnapshot(s,'unrelated'),leases=s.activeLeases();
  assert.equal(decideReviewedManual(snapshot,{},{}).action,'hold');
  assert.equal(decideReviewedManual({...snapshot,run:null},{},{}).action,'hold');
  assert.deepEqual(s.activeLeases(),leases);
 }finally{s.close();}
});
