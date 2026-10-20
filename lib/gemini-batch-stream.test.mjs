import assert from "node:assert/strict";
import test from "node:test";
import { Readable } from "node:stream";
import { spawnSync } from "node:child_process";
import { readGeminiBatchStream } from "./gemini-batch-stream.js";

function splitBuffer(value, size) {
  const bytes = Buffer.from(value);
  return Readable.from(Array.from({ length: Math.ceil(bytes.length / size) }, (_, i) => bytes.subarray(i * size, (i + 1) * size)));
}

test("streams six inline results across tiny chunks and keeps only metadata in the returned document", async () => {
  const input = {
    name: "batches/streamtest",
    state: "BATCH_STATE_SUCCEEDED",
    dest: {
      inlinedResponses: Array.from({ length: 6 }, (_, i) => ({
        metadata: { key: `slide-${i + 1}`, project_key: "tradingpulse" },
        response: { parts: [{ inlineData: { mimeType: "image/jpeg", data: "A".repeat(200_000) } }] }
      }))
    }
  };
  const keys = [];
  const result = await readGeminiBatchStream(splitBuffer(JSON.stringify(input), 73), (entry) => {
    keys.push(entry.metadata.key);
  });
  assert.equal(result.entryCount, 6);
  assert.equal(result.document.state, "BATCH_STATE_SUCCEEDED");
  assert.deepEqual(result.document.dest.inlinedResponses, []);
  assert.deepEqual(keys, ["slide-1", "slide-2", "slide-3", "slide-4", "slide-5", "slide-6"]);
  assert.ok(result.totalBytes > 1_200_000);
});

test("uses the first provider inline list and skips a duplicate response list", async () => {
  const input = '{"state":"SUCCEEDED","dest":{"inlinedResponses":[{"metadata":{"key":"first"}}]},"output":{"inlinedResponses":[{"metadata":{"key":"duplicate"}}]}}';
  const keys = [];
  const result = await readGeminiBatchStream(splitBuffer(input, 1), (entry) => keys.push(entry.metadata.key));
  assert.deepEqual(keys, ["first"]);
  assert.equal(result.entryCount, 1);
  assert.deepEqual(result.document.output.inlinedResponses, []);
});

test("accepts the provider's nested inlinedResponses envelope", async () => {
  const input = '{"state":"BATCH_STATE_SUCCEEDED","dest":{"inlinedResponses":{"inlinedResponses":[{"metadata":{"key":"slide-1"}}]}},"output":{"inlinedResponses":[]}}';
  const keys = [];
  const result = await readGeminiBatchStream(splitBuffer(input, 7), (entry) => keys.push(entry.metadata.key));
  assert.deepEqual(keys, ["slide-1"]);
  assert.deepEqual(result.document.dest.inlinedResponses.inlinedResponses, []);
});

test("accepts the provider's metadata output envelope before a duplicate response list", async () => {
  const input = '{"metadata":{"state":"BATCH_STATE_SUCCEEDED","output":{"inlinedResponses":{"inlinedResponses":[{"metadata":{"key":"slide-1"}}]}}},"response":{"inlinedResponses":[{"metadata":{"key":"duplicate"}}]}}';
  const keys = [];
  const result = await readGeminiBatchStream(splitBuffer(input, 7), (entry) => keys.push(entry.metadata.key));
  assert.deepEqual(keys, ["slide-1"]);
  assert.equal(result.entryCount, 1);
  assert.deepEqual(result.document.metadata.output.inlinedResponses.inlinedResponses, []);
  assert.deepEqual(result.document.response.inlinedResponses, []);
});

test("rejects malformed or truncated inline results", async () => {
  await assert.rejects(readGeminiBatchStream(splitBuffer('{"dest":{"inlinedResponses":[{"metadata":{}}', 5), () => {}), /unvollständig/);
  await assert.rejects(readGeminiBatchStream(splitBuffer('{"dest":{"inlinedResponses":[true]}}', 5), () => {}), /Inline-Liste ist ungültig/);
});

test("rejects inline lists outside the known provider response path", async () => {
  await assert.rejects(readGeminiBatchStream(splitBuffer('{"other":{"inlinedResponses":[]}}', 3), () => {}), /außerhalb des erwarteten Antwortpfads/);
});

test("status accepts pending/failed envelopes without weakening collect's inline requirement", async () => {
  for (const state of ["JOB_STATE_RUNNING", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED"]) {
    const input = JSON.stringify({ name: "batches/statusonly", metadata: { state } });
    const result = await readGeminiBatchStream(splitBuffer(input, 3), () => assert.fail(),
      { allowMissingInline: true, maxEntryBytes: 16 * 1024 * 1024 });
    assert.equal(result.entryCount, 0);
    assert.equal(result.document.metadata.state, state);
    await assert.rejects(readGeminiBatchStream(splitBuffer(input, 3), () => {}), /keine Inline/);
  }
});

test("status still rejects truncation, invalid paths and oversized individual entries", async () => {
  const opts = { allowMissingInline: true, maxEntryBytes: 256 };
  await assert.rejects(readGeminiBatchStream(splitBuffer('{"metadata":', 2), () => {}, opts));
  await assert.rejects(readGeminiBatchStream(splitBuffer('{"other":{"inlinedResponses":[]}}', 2), () => {}, opts), /Antwortpfads/);
  const input = JSON.stringify({ dest: { inlinedResponses: [{ data: "A".repeat(512) }] } });
  await assert.rejects(readGeminiBatchStream(splitBuffer(input, 31), () => assert.fail(), opts), /Einzelantwort ist zu groß/);
});

test("status processes a 72 MiB batch under a 64 MiB V8 heap without retaining all images", () => {
  const code = `
    import { Readable } from "node:stream";
    import { readGeminiBatchStream } from ${JSON.stringify(new URL("./gemini-batch-stream.js", import.meta.url).href)};
    async function* body() {
      yield '{"state":"SUCCEEDED","dest":{"inlinedResponses":[';
      for (let i = 0; i < 12; i++) {
        if (i) yield ',';
        yield '{"response":{"data":"';
        for (let j = 0; j < 96; j++) yield 'A'.repeat(65536);
        yield '"}}';
      }
      yield ']}}';
    }
    const result = await readGeminiBatchStream(Readable.from(body()), () => {},
      { allowMissingInline: true, maxEntryBytes: 16 * 1024 * 1024 });
    if (result.entryCount !== 12 || JSON.stringify(result.document).length > 1024) process.exit(1);
    console.log(JSON.stringify({ entries: result.entryCount, bytes: result.totalBytes, heapLimitMiB: 64 }));
  `;
  const result = spawnSync(process.execPath, ["--max-old-space-size=64", "--input-type=module", "-"],
    { input: code, encoding: "utf8", timeout: 30000, maxBuffer: 16384 });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(JSON.parse(result.stdout).entries, 12);
});
