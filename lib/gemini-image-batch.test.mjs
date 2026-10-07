import assert from "node:assert/strict";
import test from "node:test";

import {
  buildGeminiImageBatchPayload,
  extractGeminiBatchImages,
  iterateGeminiBatchImages,
  normalizeGeminiBatchName,
  summarizeGeminiBatchJob
} from "./gemini-image-batch.js";

test("builds a deterministic private inline image batch", () => {
  const payload = buildGeminiImageBatchPayload({
    model: "gemini-3-pro-image",
    displayName: "vip-tradingpulse-2026-10-02-a1b2c3d4",
    projectKey: "tradingpulse",
    targetPublishDate: "2026-10-02",
    requests: [
      {
        requestKey: "cover",
        prompt: "A premium editorial finance photograph without text",
        aspectRatio: "4:5",
        imageSize: "2K",
        mimeType: "image/jpeg",
        fileName: "tradingpulse-cover.jpg",
        slide: 1
      }
    ]
  });

  assert.equal(payload.batch.display_name, "vip-tradingpulse-2026-10-02-a1b2c3d4");
  const entry = payload.batch.input_config.requests.requests[0];
  assert.equal(entry.metadata.key, "cover");
  assert.equal(entry.metadata.project_key, "tradingpulse");
  assert.equal(entry.metadata.target_publish_date, "2026-10-02");
  assert.equal(entry.metadata.prompt_sha256.length, 64);
  assert.deepEqual(entry.request.generation_config, {
    responseModalities: ["IMAGE"],
    imageConfig: { aspectRatio: "4:5", imageSize: "2K" }
  });
  assert.equal(entry.request.store, false);
});

test("summarizes both job and batch state prefixes", () => {
  const summary = summarizeGeminiBatchJob({
    name: "batches/abc12345",
    state: "JOB_STATE_SUCCEEDED",
    displayName: "vip-test",
    dest: { inlinedResponses: [{ response: {} }] }
  });
  assert.equal(summary.name, "batches/abc12345");
  assert.equal(summary.state, "SUCCEEDED");
  assert.equal(summary.terminal, true);
  assert.equal(summary.succeeded, true);
  assert.equal(summary.inline_response_count, 1);
  assert.equal(normalizeGeminiBatchName(summary.name), summary.name);
});

test("extracts inline image bytes and preserves request metadata", () => {
  const expected = Buffer.from("fake-png-batch-bytes");
  const results = extractGeminiBatchImages({
    state: "BATCH_STATE_SUCCEEDED",
    output: {
      inlinedResponses: {
        inlinedResponses: [
          {
            metadata: { key: "slide-1", file_name: "slide-1.png" },
            response: {
              candidates: [
                {
                  content: {
                    parts: [{ inlineData: { mimeType: "image/png", data: expected.toString("base64") } }]
                  }
                }
              ]
            }
          }
        ]
      }
    }
  });
  assert.equal(results.length, 1);
  assert.equal(results[0].requestKey, "slide-1");
  assert.deepEqual(results[0].image.bytes, expected);
  assert.equal(results[0].image.mimeType, "image/png");
  assert.equal(results[0].image.sha256.length, 64);
});

test("reports per-request errors without accepting missing image output", () => {
  const [failed] = extractGeminiBatchImages({
    dest: {
      inlinedResponses: [
        { metadata: { key: "cover" }, error: { code: 400, message: "invalid" } }
      ]
    }
  });
  assert.equal(failed.image, null);
  assert.equal(failed.error.code, 400);
});

test("filters request keys before decoding unrelated inline images", () => {
  const valid = Buffer.from("selected-image-bytes");
  const batch = {
    dest: {
      inlinedResponses: [
        { metadata: { key: "unrelated" }, response: { parts: [{ inlineData: { data: "invalid base64!" } }] } },
        { metadata: { key: "selected" }, response: { parts: [{ inlineData: { mimeType: "image/jpeg", data: valid.toString("base64") } }] } }
      ]
    }
  };
  const results = [...iterateGeminiBatchImages(batch, { requestKeys: ["selected"] })];
  assert.equal(results.length, 1);
  assert.equal(results[0].requestKey, "selected");
  assert.deepEqual(results[0].image.bytes, valid);
});

test("decodes a large inline image without exhausting the JavaScript stack", () => {
  const expected = Buffer.alloc(6 * 1024 * 1024, 0x5a);
  const [result] = extractGeminiBatchImages({
    dest: { inlinedResponses: [{
      metadata: { key: "large-image" },
      response: { parts: [{ inlineData: { mimeType: "image/jpeg", data: expected.toString("base64") } }] }
    }] }
  });
  assert.deepEqual(result.image.bytes, expected);
});

test("rejects malformed inline Base64 padding and characters", () => {
  for (const data of ["AAA=AAAA", "A===", "AAA!", "AA==AA==", "AAA"]) {
    assert.throws(() => extractGeminiBatchImages({
      dest: { inlinedResponses: [{
        metadata: { key: "invalid-image" },
        response: { parts: [{ inlineData: { data } }] }
      }] }
    }), /ungueltige Base64/);
  }
});
