import assert from "node:assert/strict";
import test from "node:test";
import { createHttpMaterialCommentStore } from "./asana-material-comment-http-store.js";

const payloadHash = "a".repeat(64);

test("HTTP store sends authenticated operations without exposing the token", async () => {
  const operations = [];
  const store = createHttpMaterialCommentStore({
    token: "shared-secret",
    fetchImpl: async (url, options) => {
      assert.equal(url.hostname, "ai-operations.vip-studios.de");
      assert.equal(options.headers.authorization, "Bearer shared-secret");
      assert.equal(options.redirect, "error");
      const request = JSON.parse(options.body);
      operations.push(request.operation);
      return Response.json({
        ok: true,
        result: request.operation === "health" ? { status: "ready" }
          : request.operation === "readReceipt" ? { receipt: null }
            : request.operation === "acquire" ? { acquired: true, token: request.token, fence: 1, claim: {} }
              : { ok: true, status: request.operation }
      });
    }
  });
  assert.deepEqual(await store.health(), { ready: true });
  assert.equal(await store.readReceipt({ key: "agent:task", payloadHash }), null);
  const acquired = await store.acquire({ key: "agent:task", payloadHash, payloadProbe: "result" });
  assert.equal(acquired.acquired, true);
  assert.equal(acquired.fence, 1);
  assert.equal((await store.beginPosting({ key: "agent:task", payloadHash, token: acquired.token, fence: 1 })).ok, true);
  assert.equal((await store.complete({ key: "agent:task", payloadHash, token: acquired.token, fence: 1, storyGid: "123" })).ok, true);
  assert.equal((await store.release({ key: "agent:task", payloadHash, token: acquired.token, fence: 1 })).ok, true);
  assert.deepEqual(operations, ["health", "readReceipt", "acquire", "beginPosting", "complete", "release"]);
});

test("HTTP store fails closed on bad status, malformed body, or transport error", async () => {
  for (const fetchImpl of [
    async () => Response.json({ ok: false }, { status: 503 }),
    async () => new Response("not json", { status: 200, headers: { "content-type": "text/html" } }),
    async () => { throw new Error("transport failed"); }
  ]) {
    const store = createHttpMaterialCommentStore({ token: "shared-secret", fetchImpl });
    await assert.rejects(store.readReceipt({ key: "agent:task", payloadHash }), (error) => {
      assert.equal(error.code, "MATERIAL_COMMENT_STORE_UNAVAILABLE");
      assert.equal(error.message.includes("shared-secret"), false);
      return true;
    });
  }
});
