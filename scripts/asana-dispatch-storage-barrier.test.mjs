import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {openDispatchDatabase} from './asana-dispatch-store.mjs';

function fixture(t) {
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'dispatch-storage-barrier-'));
  t.after(()=>fs.rmSync(root,{recursive:true,force:true}));
  return {root,barrier:path.join(root,'barrier'),database:path.join(root,'store.sqlite')};
}

test('maintenance blocks new connections before a database can be created',t=>{
  const f=fixture(t);fs.writeFileSync(f.barrier,'maintenance');
  assert.throws(()=>openDispatchDatabase(f.database,{},f),{code:'ASANA_DISPATCH_STORAGE_MAINTENANCE'});
  assert.equal(fs.existsSync(f.database),false);
});

test('maintenance appearing during connection open closes it before caller writes',t=>{
  const f=fixture(t);let connection;
  assert.throws(()=>openDispatchDatabase(f.database,{}, {...f,connect:(filename,options)=>{
    connection=new DatabaseSync(filename,options);fs.writeFileSync(f.barrier,'maintenance');return connection;
  }}),{code:'ASANA_DISPATCH_STORAGE_MAINTENANCE'});
  assert.throws(()=>connection.exec('CREATE TABLE forbidden (n INTEGER)'));
  const check=new DatabaseSync(f.database);
  try{assert.equal(check.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE type='table'").get().n,0);}
  finally{check.close();}
});

test('a connection delayed across the entire maintenance window is rejected',t=>{
  const f=fixture(t);let connection;
  fs.writeFileSync(f.barrier+'.epoch','before');
  assert.throws(()=>openDispatchDatabase(f.database,{}, {...f,connect:(filename,options)=>{
    connection=new DatabaseSync(filename,options);
    fs.writeFileSync(f.barrier,'maintenance');
    fs.writeFileSync(f.barrier+'.epoch','after');
    fs.unlinkSync(f.barrier);
    return connection;
  }}),{code:'ASANA_DISPATCH_STORAGE_CHANGED'});
  assert.throws(()=>connection.exec('CREATE TABLE forbidden (n INTEGER)'));
  assert.equal(fs.existsSync(f.barrier),false);
});

test('recovery connections resolve aliases to the same WAL and fence',t=>{
  const f=fixture(t),alias=path.join(f.root,'alias.sqlite');
  const a=openDispatchDatabase(f.database,{},f);fs.symlinkSync(f.database,alias);
  const b=openDispatchDatabase(alias,{},f);
  try{
    a.exec('PRAGMA journal_mode=WAL; CREATE TABLE preserved (id TEXT PRIMARY KEY, fence INTEGER);');
    a.prepare('INSERT INTO preserved VALUES (?,?)').run('active-run',37);
    assert.equal(b.prepare('SELECT fence FROM preserved').get().fence,37);
    assert.equal(fs.existsSync(alias+'-wal'),false);
    assert.equal(fs.existsSync(alias+'-shm'),false);
  }finally{b.close();a.close();}
});

test('removing an owned maintenance barrier restores normal access without a reset',t=>{
  const f=fixture(t);let a=openDispatchDatabase(f.database,{},f);
  a.exec('CREATE TABLE preserved(n INTEGER); INSERT INTO preserved VALUES(42)');a.close();
  const before=fs.readFileSync(f.database);fs.writeFileSync(f.barrier,'maintenance');
  assert.throws(()=>openDispatchDatabase(f.database,{},f));
  assert.deepEqual(fs.readFileSync(f.database),before);fs.unlinkSync(f.barrier);
  a=openDispatchDatabase(f.database,{},f);
  try{assert.equal(a.prepare('SELECT n FROM preserved').get().n,42);}finally{a.close();}
});
