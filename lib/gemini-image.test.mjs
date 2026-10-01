import assert from "node:assert/strict";
import test from "node:test";

import {
  buildGeminiImageInteractionPayload,
  extractGeminiImageResult,
  normalizeGeminiImageFileName,
  resolveGeminiImageSize
} from "./gemini-image.js";

test("builds a privacy-preserving standard image request", () => {
  const payload = buildGeminiImageInteractionPayload({ prompt: "A realistic alpine hotel at sunrise" });
  assert.equal(payload.model, "gemini-3.1-flash-image");
  assert.deepEqual(payload.input, [{ type: "text", text: "A realistic alpine hotel at sunrise" }]);
  assert.deepEqual(payload.response_format, {
    type: "image",
    mime_type: "image/png",
    aspect_ratio: "16:9",
    image_size: "2K"
  });
  assert.equal(payload.store, false);
});

test("routes default sizes by production tier", () => {
  assert.equal(resolveGeminiImageSize("gemini-3.1-flash-image"), "2K");
  assert.equal(resolveGeminiImageSize("gemini-3-pro-image"), "4K");
  assert.equal(resolveGeminiImageSize("gemini-3.1-flash-lite-image"), "1K");
  assert.throws(
    () => resolveGeminiImageSize("gemini-3.1-flash-lite-image", "2K"),
    /unterstuetzt image_size=2K nicht/
  );
});

test("extracts current Interactions API image output without leaking base64 metadata", () => {
  const expected = Buffer.from("fake-png-bytes");
  const result = extractGeminiImageResult({
    id: "int_123",
    status: "completed",
    steps: [
      {
        type: "model_output",
        content: [{ type: "image", mime_type: "image/png", data: expected.toString("base64") }]
      }
    ],
    usage: { total_tokens: 42 }
  });
  assert.deepEqual(result.bytes, expected);
  assert.equal(result.mimeType, "image/png");
  assert.equal(result.interactionId, "int_123");
  assert.equal(result.status, "completed");
  assert.equal(result.sha256.length, 64);
});

test("supports SDK-style output_image and validates filenames", () => {
  const result = extractGeminiImageResult({
    interaction: {
      output_image: { type: "image", mime_type: "image/jpeg", data: Buffer.from("jpg").toString("base64") }
    }
  });
  assert.equal(result.mimeType, "image/jpeg");
  assert.equal(normalizeGeminiImageFileName("reise.jpg", result.mimeType), "reise.jpg");
  assert.equal(normalizeGeminiImageFileName(undefined, "image/png", "gemini-test"), "gemini-test.png");
  assert.throws(() => normalizeGeminiImageFileName("../reise.png", "image/png"), /ohne Pfad/);
  assert.throws(() => normalizeGeminiImageFileName("reise.png", "image/jpeg"), /passen nicht zusammen/);
});

test("fails closed when no final image exists", () => {
  assert.throws(
    () => extractGeminiImageResult({ id: "int_failed", status: "failed", steps: [] }),
    /kein finales Bild.*failed/
  );
});
