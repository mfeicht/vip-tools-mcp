import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {AsanaDispatchStore,openDispatchDatabase} from './asana-dispatch-store.mjs';
import {migrateDispatchStorage,dispatchOpenProcesses} from './asana-dispatch-storage-migration.mjs';

function fixture(t) {
  const root=fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),'dispatch-migration-')));
  t.after(()=>fs.rmSync(root,{recursive:true,force:true}));
  return {source:path.join(root,'legacy.sqlite'),destination:path.join(root,'durable.sqlite'),
    backupPath:path.join(root,'backup.sqlite'),barrier:path.join(root,'barrier')};
}

test('storage migration preserves every table and the active lease token/fence',async t=>{
  const f=fixture(t),old=new AsanaDispatchStore(f.source);
  old.acquireLease('agent:vip-ai-finance','active-owner',120000,Date.now());
  const lease=old.leasesForRun('active-owner')[0];
  old.db.exec("CREATE TABLE migration_probe(n INTEGER, content BLOB); INSERT INTO migration_probe VALUES(9007199254740993,x'0001ff');");old.close();
  const result=await migrateDispatchStorage(f);
  assert.equal(result.status,'verified');assert.equal(result.before.sha256,result.after.sha256);
  assert.equal(fs.lstatSync(f.source).isSymbolicLink(),true);assert.equal(fs.realpathSync(f.source),f.destination);
  assert.equal(fs.existsSync(f.barrier),false);assert.equal(fs.statSync(f.destination).mode&0o777,0o600);
  const current=new AsanaDispatchStore(f.source);
  try{
    assert.deepEqual(current.leasesForRun('active-owner')[0],lease);
    assert.equal(current.acquireLease('agent:vip-ai-finance','competing-owner',120000,Date.now()),null);
    assert.equal(fs.existsSync(f.source+'-wal'),false);
    const check=new DatabaseSync(f.backupPath,{readOnly:true});
    try{assert.equal(check.prepare('SELECT token FROM leases').get().token,lease.token);}finally{check.close();}
  }finally{current.close();}
});

test('an existing connection prevents migration without touching its live lease',async t=>{
  const f=fixture(t),store=new AsanaDispatchStore(f.source);
  store.acquireLease('agent:vip-ai-finance','running',120000,Date.now());
  const lease=store.leasesForRun('running')[0];
  try{
    assert.ok(dispatchOpenProcesses(f.source).includes(process.pid));
    await assert.rejects(migrateDispatchStorage({...f,timeoutMs:0}),/did not drain/);
    assert.equal(fs.lstatSync(f.source).isFile(),true);assert.equal(fs.existsSync(f.destination),false);
    assert.equal(fs.existsSync(f.backupPath),false);assert.equal(fs.existsSync(f.barrier),false);
    assert.deepEqual(store.leasesForRun('running')[0],lease);
  }finally{store.close();}
});

test('maintenance ownership and existing destinations are never overwritten',async t=>{
  const f=fixture(t),store=new AsanaDispatchStore(f.source);store.close();
  fs.writeFileSync(f.barrier,'another-owner');
  await assert.rejects(migrateDispatchStorage(f),{code:'EEXIST'});
  assert.equal(fs.readFileSync(f.barrier,'utf8'),'another-owner');
  fs.unlinkSync(f.barrier);fs.writeFileSync(f.destination,'existing');
  await assert.rejects(migrateDispatchStorage(f),/New destination/);
  assert.equal(fs.readFileSync(f.destination,'utf8'),'existing');assert.equal(fs.existsSync(f.barrier),false);
});

test('opening after cutover uses the published store and intact values',async t=>{
  const f=fixture(t),db=new DatabaseSync(f.source);
  db.exec('CREATE TABLE values_kept(value TEXT); INSERT INTO values_kept VALUES(\'unchanged\')');db.close();
  await migrateDispatchStorage(f);
  const reopened=openDispatchDatabase(f.source,{},f);
  try{assert.equal(reopened.prepare('SELECT value FROM values_kept').get().value,'unchanged');}
  finally{reopened.close();}
});
