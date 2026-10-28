import assert from 'node:assert/strict';
import test from 'node:test';
import {AsanaDispatchStore} from './asana-dispatch-store.mjs';

const current=Date.parse('2026-10-28T15:53:36.401Z');
const rollback=Date.parse('2025-10-20T16:22:35.445Z');
const ttl=2*60*60_000;
const row=(store,key)=>({...store.db.prepare('SELECT * FROM leases WHERE key=?').get(key)});

for(const key of ['agent:vip-ai-finance','task:1219517142869260']) {
 test(`clock rollback preserves live owner and time bounds: ${key}`,()=>{
  const s=new AsanaDispatchStore(':memory:');try{
   const before=s.acquireLease(key,'owner-run',ttl,current);
   const persisted=row(s,key);
   const renewed=s.acquireLease(key,'owner-run',ttl,rollback);
   assert.deepEqual(row(s,key),persisted);
   assert.deepEqual(renewed,before);
   assert.equal(s.acquireLease(key,'other-run',ttl,current+ttl+1),null);
   assert.deepEqual(row(s,key),persisted);
   const later=s.acquireLease(key,'owner-run',ttl,current+60_000);
   assert.equal(later.expires_at_ms,current+60_000+ttl);
   assert.equal(row(s,key).updated_at_ms,current+60_000);
   assert.equal(later.token,before.token);assert.equal(later.fence,before.fence);
  }finally{s.close();}
 });
}

test('shorter renewal cannot truncate existing expiry and does not invent extra TTL',()=>{
 const s=new AsanaDispatchStore(':memory:');try{
  const before=s.acquireLease('task:123','owner-run',ttl,current);
  const shortened=s.acquireLease('task:123','owner-run',60_000,current+30_000);
  assert.equal(shortened.expires_at_ms,before.expires_at_ms);
  assert.equal(row(s,'task:123').updated_at_ms,current+30_000);
  assert.equal(s.acquireLease('task:123','owner-run',ttl,current-1000).expires_at_ms,before.expires_at_ms);
 }finally{s.close();}
});

test('manual renew shares monotonic lease protection and preserves thread/task fences',()=>{
 const s=new AsanaDispatchStore(':memory:');try{
  const context={threadId:'12345678-1234-1234-1234-123456789012',source:'CODEX_THREAD_ID'};
  s.acquireManualAgent('vip-ai-operations','manual-run',context,current);
  s.acquireManualTask('manual-run','123',context,current+1);
  const before=s.leasesForRun('manual-run').map(x=>({...x}));
  assert.equal(s.finishManualLeaseCommand('manual-run','renew',context,rollback),2);
  assert.deepEqual(s.leasesForRun('manual-run').map(x=>({...x})),before);
  assert.throws(()=>s.finishManualLeaseCommand('manual-run','renew',{...context,threadId:'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'},current+2),/another thread/);
  assert.equal(s.finishManualLeaseCommand('manual-run','release',context,current+3),2);
  assert.deepEqual(s.leasesForRun('manual-run'),[]);
 }finally{s.close();}
});

test('release and reacquire still advance fences normally after a rollback',()=>{
 const s=new AsanaDispatchStore(':memory:');try{
  const first=s.acquireLease('task:123','first-run',ttl,current);
  s.acquireLease('task:123','first-run',ttl,rollback);
  assert.equal(s.releaseLease({...first,token:'wrong'}),false);
  assert.equal(s.releaseLease(first),true);
  const next=s.acquireLease('task:123','second-run',ttl,current+1);
  assert.equal(next.fence,first.fence+1);assert.notEqual(next.token,first.token);
  assert.equal(next.expires_at_ms,current+1+ttl);
 }finally{s.close();}
});
