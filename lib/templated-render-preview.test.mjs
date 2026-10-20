import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  buildTemplatedRenderPreview, TEMPLATED_PREVIEW_MAX_BYTES,
  validateTemplatedPreviewMetadata
} from "./templated-render-preview.js";

const id = "62af732c-d24c-4fe0-a6ae-90948a360285";
const url = `https://cdn.templated.media/render/${id}/test.png`;
const metadata = { id, status: "COMPLETED", removed: false, width: 1, height: 1, format: "png", url };
const png = Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a9X8AAAAASUVORK5CYII=", "base64");
function response(bytes = png, extra = {}) {
  return new Response(bytes, { status: 200, headers: { "content-type": "image/png", ...extra } });
}
function preview(options = {}) {
  return buildTemplatedRenderPreview({
    agentId: "vip-ai-operations", renderId: id,
    readRender: async () => ({ ok: true, status: 200, data: metadata }),
    fetchImpl: async () => response(), ...options
  });
}

test("account metadata precedes bounded canonical fetch; returns native MCP image", async () => {
  const calls = [];
  const result = await preview({
    readRender: async (value) => { calls.push("metadata"); assert.equal(value, id); return { ok: true, status: 200, data: metadata }; },
    fetchImpl: async (value, options) => {
      calls.push("image");
      assert.equal(value, url);
      assert.equal(options.method, "GET");
      assert.equal(options.redirect, "manual");
      assert.deepEqual(options.headers, { Accept: "image/png" });
      return response(png, { "content-length": String(png.length) });
    }
  });
  assert.deepEqual(calls, ["metadata", "image"]);
  assert.equal(result.content[1].type, "image");
  assert.equal(result.content[1].data, png.toString("base64"));
  const info = JSON.parse(result.content[0].text);
  assert.equal(info.read_only, true);
  assert.equal(info.visual_review_required, true);
  assert.deepEqual(info.dimensions, { width: 1, height: 1 });
});

for (const [name, patch] of Object.entries({
  foreignId: { id: "a".repeat(36) }, incomplete: { status: "PROCESSING" },
  removed: { removed: true }, pdf: { format: "pdf" }, webp: { format: "webp" },
  largeSide: { width: 4097 }, excessivePixels: { width: 4096, height: 4096 },
  nonInteger: { width: 1.1 }, zero: { height: 0 },
  foreignHost: { url: url.replace("cdn.templated.media", "example.com") },
  deceptiveHost: { url: url.replace("cdn.templated.media", "cdn.templated.media.example.com") },
  http: { url: url.replace("https:", "http:") },
  credentials: { url: url.replace("https://", "https://user:pass@") },
  port: { url: url.replace(".media/", ".media:8443/") },
  query: { url: url + "?target=x" }, fragment: { url: url + "#x" },
  foreignRender: { url: url.replace(id, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa") },
  encodedSlash: { url: url.replace("test.png", "other%2Ftest.png") },
  traversal: { url: url.replace("test.png", "../test.png") },
  doubleEncoding: { url: url.replace("test.png", "%252Ftest.png") }
})) {
  test(`reject ${name} before any CDN request`, async () => {
    await assert.rejects(preview({
      readRender: async () => ({ ok: true, status: 200, data: { ...metadata, ...patch } }),
      fetchImpl: async () => assert.fail("must not fetch")
    }));
  });
}

test("metadata access failure never tries CDN", async () => {
  await assert.rejects(preview({ readRender: async () => ({ ok: false, status: 403 }),
    fetchImpl: async () => assert.fail("must not fetch") }), /metadata_read_failed/);
});
test("redirects and content type substitution fail", async () => {
  await assert.rejects(preview({ fetchImpl: async () => new Response(null, { status: 302,
    headers: { location: "https://example.com" } }) }), /cdn_http_status/);
  await assert.rejects(preview({ fetchImpl: async () => response(png, { "content-type": "text/html" }) }), /content_type_mismatch/);
});
test("advertised overlimit and dishonest length fail", async () => {
  await assert.rejects(preview({ fetchImpl: async () => response(png, { "content-length": String(TEMPLATED_PREVIEW_MAX_BYTES + 1) }) }), /content_length_limit/);
  await assert.rejects(preview({ fetchImpl: async () => response(png, { "content-length": "1" }) }), /content_length_mismatch/);
});
test("stream is cancelled when chunks cross limit without content-length", async () => {
  let cancelled = false;
  const body = new ReadableStream({
    pull(controller) { controller.enqueue(new Uint8Array(1024 * 1024)); },
    cancel() { cancelled = true; }
  });
  await assert.rejects(preview({ fetchImpl: async () => new Response(body, {
    headers: { "content-type": "image/png" } }) }), /stream_limit/);
  assert.equal(cancelled, true);
});
test("signature and metadata dimensions must match", async () => {
  await assert.rejects(preview({ fetchImpl: async () => response(Buffer.from("not an image")) }));
  await assert.rejects(preview({ readRender: async () => ({ ok: true, status: 200,
    data: { ...metadata, width: 2 } }) }), /image_dimensions_mismatch/);
});
test("verified JPEG accepts the observed CDN image/jpg alias, never arbitrary types", async () => {
  const jpeg = Buffer.from([
    0xff, 0xd8, 0xff, 0xe0, 0x00, 0x04, 0x00, 0x00,
    0xff, 0xc0, 0x00, 0x11, 0x08, 0x09, 0x00, 0x07, 0x40,
    0x03, 0x01, 0x11, 0x00, 0x02, 0x11, 0x00, 0x03, 0x11, 0xff, 0xd9
  ]);
  const data = { ...metadata, width: 1856, height: 2304, format: "jpg", url: url.replace(".png", ".jpg") };
  const result = await preview({
    readRender: async () => ({ ok: true, status: 200, data }),
    fetchImpl: async () => response(jpeg, { "content-type": "image/jpg" })
  });
  assert.equal(result.content[1].mimeType, "image/jpeg");
  await assert.rejects(preview({
    readRender: async () => ({ ok: true, status: 200, data }),
    fetchImpl: async () => response(jpeg, { "content-type": "application/octet-stream" })
  }), /content_type_mismatch/);
  await assert.rejects(preview({
    readRender: async () => ({ ok: true, status: 200, data }),
    fetchImpl: async () => response(png, { "content-type": "image/jpg" })
  }), /widerspricht/);
});
test("deadline aborts a CDN fetch that has not returned headers", async (context) => {
  const originalTimer = globalThis.setTimeout;
  context.mock.method(globalThis, "setTimeout", (callback, milliseconds) => {
    assert.equal(milliseconds, 15_000);
    return originalTimer(callback, 5);
  });
  await assert.rejects(preview({ fetchImpl: async (_url, options) => new Promise((_resolve, reject) => {
    options.signal.addEventListener("abort", () => reject(new Error("aborted_by_deadline")), { once: true });
  }) }), /aborted_by_deadline/);
});
test("actual server registration uses scoped metadata GET and read-only annotations", async () => {
  const source = readFileSync(new URL("../server.js", import.meta.url), "utf8");
  const start = source.indexOf('  server.tool(\n    "templated_render_preview"');
  assert.ok(start > 0);
  const end = source.indexOf("\n  server.tool(", start + 10);
  let registration;
  const calls = [];
  const READ = { readOnlyHint: true };
  const fakeZ = { string: () => ({ uuid: () => "uuid-schema" }) };
  const scopedRequest = async (agent, args) => {
    calls.push({ agent, args }); return { ok: true, status: 200, data: metadata };
  };
  const build = (args) => buildTemplatedRenderPreview({ ...args, fetchImpl: async () => response() });
  new Function("server", "z", "TOOL_EXTERNAL_READ", "agentIdSchema",
    "buildTemplatedRenderPreview", "templatedRequest", source.slice(start, end))(
    { tool: (...args) => { registration = args; } }, fakeZ, READ, "agent-schema", build, scopedRequest);
  assert.deepEqual(Object.keys(registration[2]).sort(), ["agent_id", "render_id"]);
  assert.equal(registration[3], READ);
  const result = await registration[4]({ agent_id: "vip-ai-social-media", render_id: id });
  assert.equal(result.content[1].type, "image");
  assert.deepEqual(calls, [{ agent: "vip-ai-social-media", args: {
    method: "GET", path: `/v1/render/${id}`, timeout: 10_000,
    max_response_bytes: 256 * 1024, max_redirects: 0
  } }]);
});
test("metadata HTTP transport enforces preview limits without changing other calls", async () => {
  const source = readFileSync(new URL("../server.js", import.meta.url), "utf8");
  const start = source.indexOf("async function templatedRequest(");
  const end = source.indexOf("\nfunction getGeminiImageConfigDetails", start);
  let request;
  const fn = new Function("getTemplatedConfigDetails", "axios", "TEMPLATED_API_BASE",
    "TEMPLATED_TIMEOUT_MS", "WEB_FETCH_USER_AGENT",
    source.slice(start, end) + "\nreturn templatedRequest;")(
    () => ({ config: { apiKey: "fixture-key" } }),
    { request: async (args) => { request = args; return { status: 200, data: metadata }; } },
    "https://api.templated.io", 30000, "test"
  );
  await fn("vip-ai-social-media", { path: `/v1/render/${id}`, timeout: 10000,
    max_response_bytes: 256 * 1024, max_redirects: 0 });
  assert.equal(request.maxContentLength, 256 * 1024);
  assert.equal(request.maxRedirects, 0);
  assert.equal(request.timeout, 10000);
  await fn("vip-ai-social-media", { path: "/v1/templates" });
  assert.equal(request.maxContentLength, undefined);
  assert.equal(request.maxRedirects, undefined);
  assert.equal(request.timeout, 30000);
});
