import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { resolveCodexBinary } from "./codex-binary.mjs";
import { workOnce } from "./asana-dispatch-worker.mjs";

test("resolver uses executable candidate and rejects missing explicit override", (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "codex-binary-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const executable = path.join(dir, "codex");
  fs.writeFileSync(executable, "#!/bin/sh\nexit 0\n", { mode: 0o700 });
  assert.equal(resolveCodexBinary({ override: "", candidates: ["/missing/codex", executable] }), executable);
  assert.throws(() => resolveCodexBinary({ override: "/missing/override", candidates: [executable] }), /Configured/);
  fs.chmodSync(executable, 0o600);
  assert.throws(() => resolveCodexBinary({ override: "", candidates: [executable] }), /No executable/);
});
test("missing binary is rejected before opening a database or claiming a task", async (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "codex-preflight-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const db = path.join(dir, "must-not-exist.sqlite");
  const result = await workOnce({ db, resolveBinary: () => { throw new Error("missing fixture"); } });
  assert.equal(result.status, "preflight_blocked");
  assert.equal(fs.existsSync(db), false);
});
