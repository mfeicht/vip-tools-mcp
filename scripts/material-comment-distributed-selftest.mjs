import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import {
  mkdir,
  mkdtemp,
  readFile,
  rename,
  rm,
  unlink,
  writeFile
} from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fork } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  createAsanaMaterialCommentCoordinator,
  findMaterialCommentStoryByProbe,
  hashMaterialCommentProbe
} from "../lib/asana-material-comment-coordinator.js";

const TEST_PROBE = "Ergebnis Evidenz / Verifikation Geteilter atomarer Testbeleg.";
const TEST_HASH = hashMaterialCommentProbe(TEST_PROBE);
const TEST_KEY = "9000000000000001:9000000000000002";

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const keyHash = (value) => createHash("sha256").update(String(value), "utf8").digest("hex");

async function readJson(file, fallback = null) {
  try {
    return JSON.parse(await readFile(file, "utf8"));
  } catch (error) {
    if (error?.code === "ENOENT") return fallback;
    throw error;
  }
}

async function atomicWrite(file, value) {
  const temporary = `${file}.${process.pid}.${randomUUID()}.tmp`;
  await writeFile(temporary, JSON.stringify(value), "utf8");
  await rename(temporary, file);
}

async function withFileLock(root, name, action) {
  const lock = path.join(root, `${name}.lock`);
  const deadline = Date.now() + 5_000;
  while (true) {
    try {
      await mkdir(lock);
      break;
    } catch (error) {
      if (error?.code !== "EEXIST" || Date.now() >= deadline) throw error;
      await delay(5);
    }
  }
  try {
    return await action();
  } finally {
    await rm(lock, { recursive: true, force: true });
  }
}

function createFileMaterialCommentStore(root, { claimTtlMs = 1_000 } = {}) {
  const pathsFor = (key, payloadHash) => {
    const id = keyHash(key);
    return {
      lockName: `claim-${id}`,
      claim: path.join(root, `claim-${id}.json`),
      fence: path.join(root, `fence-${id}.json`),
      receipt: path.join(root, `receipt-${id}-${payloadHash}.json`)
    };
  };

  return {
    async acquire({ key, payloadHash, payloadProbe, token = randomUUID() }) {
      const files = pathsFor(key, payloadHash);
      return withFileLock(root, files.lockName, async () => {
        let current = await readJson(files.claim);
        if (current?.state === "claimed" && Number(current.expires_at_ms) <= Date.now()) {
          await unlink(files.claim).catch((error) => {
            if (error?.code !== "ENOENT") throw error;
          });
          current = null;
        }
        if (current) return { acquired: false, claim: current, token, fence: 0 };
        const previousFence = Number((await readJson(files.fence, { value: 0 })).value || 0);
        const fence = previousFence + 1;
        await atomicWrite(files.fence, { value: fence });
        const claim = {
          token,
          fence,
          state: "claimed",
          payload_hash: payloadHash,
          payload_probe: payloadProbe,
          acquired_at: new Date().toISOString(),
          expires_at_ms: Date.now() + claimTtlMs
        };
        await atomicWrite(files.claim, claim);
        return { acquired: true, claim, token, fence };
      });
    },

    async beginPosting({ key, payloadHash, token, fence }) {
      const files = pathsFor(key, payloadHash);
      return withFileLock(root, files.lockName, async () => {
        const claim = await readJson(files.claim);
        if (!claim) return { ok: false, status: "missing_claim" };
        if (claim.token !== token || Number(claim.fence) !== Number(fence)) {
          return { ok: false, status: "fence_mismatch" };
        }
        if (claim.state !== "claimed") return { ok: false, status: `invalid_state_${claim.state}` };
        claim.state = "posting";
        claim.posting_at = new Date().toISOString();
        delete claim.expires_at_ms;
        await atomicWrite(files.claim, claim);
        return { ok: true, status: "posting", claim };
      });
    },

    async complete({ key, payloadHash, token, fence, storyGid, completionMode = "posted" }) {
      const files = pathsFor(key, payloadHash);
      return withFileLock(root, files.lockName, async () => {
        const claim = await readJson(files.claim);
        if (!claim) return { ok: false, status: "missing_claim" };
        if (claim.token !== token || Number(claim.fence) !== Number(fence)) {
          return { ok: false, status: "fence_mismatch" };
        }
        if (claim.state !== "posting" && completionMode !== "reconcile") {
          return { ok: false, status: `invalid_state_${claim.state}` };
        }
        const receipt = {
          payload_hash: payloadHash,
          story_gid: storyGid,
          fence: claim.fence,
          completion_mode: completionMode
        };
        await atomicWrite(files.receipt, receipt);
        await unlink(files.claim);
        return { ok: true, status: "completed", receipt };
      });
    },

    async release({ key, payloadHash, token, fence }) {
      const files = pathsFor(key, payloadHash);
      return withFileLock(root, files.lockName, async () => {
        const claim = await readJson(files.claim);
        if (!claim) return { ok: true, status: "already_released" };
        if (claim.token !== token || Number(claim.fence) !== Number(fence)) {
          return { ok: false, status: "fence_mismatch" };
        }
        if (claim.state !== "claimed") return { ok: false, status: "posting_claim_is_fail_closed" };
        await unlink(files.claim);
        return { ok: true, status: "released" };
      });
    },

    async readReceipt({ key, payloadHash }) {
      return readJson(pathsFor(key, payloadHash).receipt);
    }
  };
}

async function readStories(root) {
  return readJson(path.join(root, "target-stories.json"), []);
}

async function appendStory(root, story) {
  return withFileLock(root, "target-stories", async () => {
    const stories = await readStories(root);
    await atomicWrite(path.join(root, "target-stories.json"), [...stories, story]);
  });
}

async function coordinatedAttempt(root, { crashAfterPost = false, claimTtlMs = 1_000 } = {}) {
  const store = createFileMaterialCommentStore(root, { claimTtlMs });
  const coordinator = createAsanaMaterialCommentCoordinator({
    distributedStore: store,
    distributedRequired: true
  });
  return coordinator.run(
    TEST_KEY,
    async ({ beforePost }) => {
      await beforePost();
      const story = {
        gid: `${process.pid}${Date.now()}`,
        text: `${TEST_PROBE} Prozess ${process.pid}`
      };
      await appendStory(root, story);
      if (crashAfterPost) process.exit(72);
      return { posted: { data: { data: story } } };
    },
    {
      payloadHash: TEST_HASH,
      payloadProbe: TEST_PROBE,
      readTargetStories: () => readStories(root),
      getStoryGid: (result) => result?.posted?.data?.data?.gid,
      recoverResult: (story, coordination) => ({ recovered: true, story, coordination })
    }
  );
}

async function runChild(root, mode) {
  try {
    const result = await coordinatedAttempt(root, { crashAfterPost: mode === "crash" });
    process.send?.({
      status: result?.recovered ? "recovered" : "posted",
      coordination: result?.coordination?.status || null
    });
    process.disconnect?.();
  } catch (error) {
    process.send?.({ status: "blocked", code: error?.code || "UNKNOWN" });
    process.disconnect?.();
  }
}

async function forkChild(root, mode = "attempt") {
  return new Promise((resolve, reject) => {
    const child = fork(fileURLToPath(import.meta.url), ["--child", root, mode], {
      stdio: ["ignore", "ignore", "pipe", "ipc"]
    });
    let message;
    let stderr = "";
    const timeout = setTimeout(() => {
      child.kill();
      reject(new Error("distributed fixture child timeout"));
    }, 10_000);
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("message", (value) => {
      message = value;
    });
    child.on("error", reject);
    child.on("exit", (code) => {
      clearTimeout(timeout);
      if (mode === "crash" && code === 72) return resolve({ status: "crashed_after_post" });
      if (code !== 0 || !message) return reject(new Error(`child exit ${code}: ${stderr}`));
      resolve(message);
    });
  });
}

if (process.argv[2] === "--child") {
  await runChild(process.argv[3], process.argv[4]);
} else {
  const root = await mkdtemp(path.join(tmpdir(), "vip-material-comment-test-"));
  try {
    assert.equal(findMaterialCommentStoryByProbe([
      {
        gid: "probe-story",
        text: `${TEST_PROBE} https://app.asana.com/example @VIP AI-Operations Aufwand 0 Min.`
      }
    ], { payload_probe: TEST_PROBE }).gid, "probe-story");
    await assert.rejects(
      () => createAsanaMaterialCommentCoordinator({ distributedRequired: true }).run(
        TEST_KEY,
        async () => ({ ok: true }),
        { payloadHash: TEST_HASH, payloadProbe: TEST_PROBE }
      ),
      (error) => error?.code === "MATERIAL_COMMENT_DISTRIBUTED_STORE_REQUIRED"
    );

    const concurrent = await Promise.all([forkChild(root), forkChild(root)]);
    assert.equal((await readStories(root)).length, 1);
    assert.equal(concurrent.filter((entry) => entry.status === "posted").length, 1);
    assert.equal(
      concurrent.filter((entry) => ["blocked", "recovered"].includes(entry.status)).length,
      1
    );

    await rm(root, { recursive: true, force: true });
    await mkdir(root);
    assert.deepEqual(await forkChild(root, "crash"), { status: "crashed_after_post" });
    assert.equal((await readStories(root)).length, 1);

    const recoveredAfterCrash = await coordinatedAttempt(root);
    assert.equal(recoveredAfterCrash.recovered, true);
    assert.equal(recoveredAfterCrash.coordination.status, "recovered_after_post_crash");
    assert.equal((await readStories(root)).length, 1);

    const recoveredAfterRestart = await coordinatedAttempt(root);
    assert.equal(recoveredAfterRestart.recovered, true);
    assert.equal(recoveredAfterRestart.coordination.status, "recovered_from_receipt");
    assert.equal((await readStories(root)).length, 1);

    const fenceRoot = await mkdtemp(path.join(tmpdir(), "vip-material-comment-fence-test-"));
    try {
      const fenceStore = createFileMaterialCommentStore(fenceRoot, { claimTtlMs: 20 });
      const first = await fenceStore.acquire({
        key: TEST_KEY,
        payloadHash: TEST_HASH,
        payloadProbe: TEST_PROBE,
        token: "first"
      });
      await delay(30);
      const second = await fenceStore.acquire({
        key: TEST_KEY,
        payloadHash: TEST_HASH,
        payloadProbe: TEST_PROBE,
        token: "second"
      });
      assert.equal(first.acquired, true);
      assert.equal(second.acquired, true);
      assert.ok(second.fence > first.fence);
      assert.equal((await fenceStore.beginPosting({
        key: TEST_KEY,
        payloadHash: TEST_HASH,
        token: first.token,
        fence: first.fence
      })).status, "fence_mismatch");
      assert.equal((await fenceStore.beginPosting({
        key: TEST_KEY,
        payloadHash: TEST_HASH,
        token: second.token,
        fence: second.fence
      })).ok, true);
    } finally {
      await rm(fenceRoot, { recursive: true, force: true });
    }

    const unclearRoot = await mkdtemp(path.join(tmpdir(), "vip-material-comment-unclear-test-"));
    try {
      const unclearStore = createFileMaterialCommentStore(unclearRoot);
      const claim = await unclearStore.acquire({
        key: TEST_KEY,
        payloadHash: TEST_HASH,
        payloadProbe: TEST_PROBE,
        token: "unclear"
      });
      assert.equal((await unclearStore.beginPosting({
        key: TEST_KEY,
        payloadHash: TEST_HASH,
        token: claim.token,
        fence: claim.fence
      })).ok, true);
      await assert.rejects(
        () => coordinatedAttempt(unclearRoot),
        (error) => error?.code === "MATERIAL_COMMENT_POST_OUTCOME_UNCLEAR"
      );
    } finally {
      await rm(unclearRoot, { recursive: true, force: true });
    }

    console.log(JSON.stringify({
      ok: true,
      two_process: "one_target_write",
      crash_after_post: "reconciled_from_target_readback",
      restart: "receipt_recovered_without_rewrite",
      fencing: "expired_writer_rejected",
      unclear_outcome: "persistent_fail_closed"
    }, null, 2));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}
