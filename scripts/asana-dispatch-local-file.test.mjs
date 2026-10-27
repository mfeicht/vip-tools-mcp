import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { AsanaDispatchStore, localDatabasePath } from "./asana-dispatch-store.mjs";
import { watch } from "./asana-dispatch-watch.mjs";

function directory(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "dispatch-local-file-"));
  t.after(() => fs.rmSync(root, {recursive:true, force:true}));
  return root;
}

test("an unavailable nonempty file is preserved before SQLite initialization", t => {
  const p=path.join(directory(t),"placeholder.sqlite");
  const fd=fs.openSync(p,"wx",0o600);fs.ftruncateSync(fd, 1024*1024);fs.closeSync(fd);
  assert.equal(fs.statSync(p).blocks,0);
  const before=fs.statSync(p);
  assert.throws(()=>new AsanaDispatchStore(p),{code:"ASANA_DISPATCH_STORE_UNAVAILABLE"});
  const after=fs.statSync(p);
  assert.equal(after.ino,before.ino);assert.equal(after.size,before.size);
  assert.equal(after.mtimeMs,before.mtimeMs);assert.equal(after.blocks,0);
  assert.equal(fs.existsSync(p+"-wal"),false);assert.equal(fs.existsSync(p+"-shm"),false);
});

test("directory and broken alias never create replacement databases", t => {
  const root=directory(t), link=path.join(root,"alias"), missing=path.join(root,"missing");
  assert.throws(()=>new AsanaDispatchStore(root),{code:"ASANA_DISPATCH_STORE_UNAVAILABLE"});
  fs.symlinkSync(missing,link);
  assert.throws(()=>new AsanaDispatchStore(link),{code:"ENOENT"});
  assert.equal(fs.existsSync(missing),false);assert.equal(fs.lstatSync(link).isSymbolicLink(),true);
});

test("materialized aliases share exactly one database and lease fence", t => {
  const root=directory(t), p=path.join(root,"store.sqlite"), link=path.join(root,"alias.sqlite");
  const a=new AsanaDispatchStore(p);fs.symlinkSync(p,link);
  const b=new AsanaDispatchStore(link);
  try {
    assert.equal(localDatabasePath(link),fs.realpathSync(p));
    const lease=a.acquireLease("agent:vip-ai-test","first",60000,Date.now());
    assert.ok(lease);assert.equal(b.acquireLease("agent:vip-ai-test","second",60000,Date.now()),null);
    assert.equal(b.leasesForRun("first")[0].token,lease.token);
    assert.equal(fs.existsSync(link+"-wal"),false);assert.equal(fs.existsSync(link+"-shm"),false);
  } finally {b.close();a.close();}
});

test("watcher publishes degraded health when both database opens fail", async () => {
  let saves=0, opens=0, saved;
  const health=await watch({dispatch:true,runPoll:async()=>({errors:[]}),
    makeStore:()=>{opens++;throw Object.assign(new Error("database not materialized"),{code:"ASANA_DISPATCH_STORE_UNAVAILABLE"});},
    writeHealth:async value=>{saves++;saved=value;}});
  assert.equal(opens,2);assert.equal(saves,1);assert.equal(saved,health);
  assert.equal(health.status,"degraded");assert.equal(health.workers_started,0);
  assert.deepEqual(health.errors.map(row=>row.source),["dispatch","poll_history"]);
  assert.ok(health.finished_at);
});
