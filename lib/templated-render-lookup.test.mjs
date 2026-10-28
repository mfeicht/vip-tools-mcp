import test from "node:test";
import assert from "node:assert/strict";
import { findTemplatedRenderByExternalId } from "./templated-render-lookup.js";

const templateId = "42af017d-67ab-4078-87ae-570f0e7a2b20";
const externalId = "rs-oaxaca-r10163-2026-10-29-s3-v01";

test("exact render is returned after complete pagination", async () => {
  const calls = [];
  const result = await findTemplatedRenderByExternalId({
    templateId, externalId, pageLimit: 2,
    fetchPage: async (args) => {
      calls.push(args);
      return args.page === 0
        ? [{ id: "other", externalId: "other" }, { id: "target", externalId, templateId }]
        : [];
    }
  });
  assert.equal(result.render.id, "target");
  assert.equal(result.pages_read, 2);
  assert.equal(result.complete, true);
  assert.deepEqual(calls.map((call) => call.page), [0, 1]);
});

test("empty complete provider response supports a null finding", async () => {
  const result = await findTemplatedRenderByExternalId({
    templateId, externalId, fetchPage: async () => []
  });
  assert.equal(result.render, null);
  assert.equal(result.match_count, 0);
  assert.equal(result.complete, true);
});

test("ambiguous matches fail closed", async () => {
  await assert.rejects(findTemplatedRenderByExternalId({
    templateId, externalId,
    fetchPage: async () => [{ id: "one", externalId }, { id: "two", externalId }]
  }), /ambiguous/);
});

test("incomplete pagination cannot become a null finding", async () => {
  await assert.rejects(findTemplatedRenderByExternalId({
    templateId, externalId, pageLimit: 1, maxPages: 2,
    fetchPage: async () => [{ id: "other", externalId: "other" }]
  }), /pagination_limit/);
});

test("wrong template and malformed responses fail closed", async () => {
  await assert.rejects(findTemplatedRenderByExternalId({
    templateId, externalId,
    fetchPage: async () => [{ id: "bad", externalId, templateId: "different" }]
  }), /template_mismatch/);
  await assert.rejects(findTemplatedRenderByExternalId({
    templateId, externalId,
    fetchPage: async () => ({ data: [] })
  }), /invalid_page/);
});
