import assert from "node:assert/strict";
import test from "node:test";
import { createHttpMaterialCommentStore } from "./asana-material-comment-http-store.js";

const payloadHash = "a".repeat(64);

test("HTTP store sends authenticated operations without exposing the token", async () => {
  const operations = [];
  const store = createHttpMaterialCommentStore({
    token: "shared-secret",
    url: "https://coordination.example.test/material-comment-store",
    fetchImpl: async (url, options) => {
      assert.equal(url.hostname, "coordination.example.test");
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
    const store = createHttpMaterialCommentStore({
      token: "shared-secret",
      url: "https://coordination.example.test/material-comment-store",
      fetchImpl
    });
    await assert.rejects(store.readReceipt({ key: "agent:task", payloadHash }), (error) => {
      assert.equal(error.code, "MATERIAL_COMMENT_STORE_UNAVAILABLE");
      assert.equal(error.message.includes("shared-secret"), false);
      return true;
    });
  }
});

test("private Sites store sends separate gate and API credentials", async () => {
  const store = createHttpMaterialCommentStore({
    token: "api-secret",
    sitesBypassToken: "sites-secret",
    url: "https://vip-asana-coordination.example.chatgpt.site/api/material-comment-store",
    fetchImpl: async (_url, options) => {
      assert.equal(options.headers.authorization, "Bearer api-secret");
      assert.equal(options.headers["OAI-Sites-Authorization"], "Bearer sites-secret");
      return Response.json({ ok: true, result: { status: "ready" } });
    }
  });
  assert.deepEqual(await store.health(), { ready: true });
});

test("HTTP store requires its own endpoint and token", () => {
  assert.throws(() => createHttpMaterialCommentStore({ token: "secret" }), /endpoint and token/);
  assert.throws(() => createHttpMaterialCommentStore({ url: "https://coordination.example.test" }), /endpoint and token/);
  assert.throws(() => createHttpMaterialCommentStore({
    url: "http://coordination.example.test", token: "secret"
  }), /plain HTTPS endpoint/);
  assert.throws(() => createHttpMaterialCommentStore({
    url: "https://coordination.example.test", token: "secret", sitesBypassToken: "sites-secret"
  }), /Sites HTTPS endpoint/);
  assert.throws(() => createHttpMaterialCommentStore({
    url: "https://coordination.chatgpt.site", token: "secret", sitesBypassToken: "bad\r\ntoken"
  }), /valid token/);
});
