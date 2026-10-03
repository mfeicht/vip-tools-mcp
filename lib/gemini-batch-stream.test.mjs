import assert from "node:assert/strict";
import test from "node:test";
import { Readable } from "node:stream";
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

test("rejects malformed or truncated inline results", async () => {
  await assert.rejects(readGeminiBatchStream(splitBuffer('{"dest":{"inlinedResponses":[{"metadata":{}}', 5), () => {}), /unvollständig/);
  await assert.rejects(readGeminiBatchStream(splitBuffer('{"dest":{"inlinedResponses":[true]}}', 5), () => {}), /Inline-Liste ist ungültig/);
});

test("rejects inline lists outside the known provider response path", async () => {
  await assert.rejects(readGeminiBatchStream(splitBuffer('{"other":{"inlinedResponses":[]}}', 3), () => {}), /außerhalb des erwarteten Antwortpfads/);
});
