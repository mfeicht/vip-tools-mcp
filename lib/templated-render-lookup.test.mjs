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
        ? [{ id: "other", externalId: "other", templateId }, { id: "target", externalId, templateId }]
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
    fetchPage: async () => [{ id: "one", externalId, templateId }, { id: "two", externalId, templateId }]
  }), /ambiguous/);
});

test("incomplete pagination cannot become a null finding", async () => {
  await assert.rejects(findTemplatedRenderByExternalId({
    templateId, externalId, pageLimit: 1, maxPages: 2,
    fetchPage: async ({page}) => [{ id: `other-${page}`, externalId: "other", templateId }]
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

for (const row of [null, {}, [], { externalId, templateId }, { id:'x', externalId }, { id:'x', templateId, externalId:7 }]) {
  test(`malformed row cannot become a negative or positive finding: ${JSON.stringify(row)}`, async () => {
    await assert.rejects(findTemplatedRenderByExternalId({templateId,externalId,fetchPage:async()=>[row]}),/invalid_/);
  });
}
test('template mismatch is rejected even on an unrelated row',async()=>{
  await assert.rejects(findTemplatedRenderByExternalId({templateId,externalId,fetchPage:async()=>[{id:'x',templateId:'foreign',externalId:'other'}]}),/template_mismatch/);
});
test('contradictory external IDs fail closed',async()=>{
  await assert.rejects(findTemplatedRenderByExternalId({templateId,externalId,fetchPage:async()=>[{id:'x',templateId,externalId,external_id:'other'}]}),/conflicting_external_id/);
});
test('duplicate IDs across pages cannot claim complete coverage',async()=>{
  await assert.rejects(findTemplatedRenderByExternalId({templateId,externalId,pageLimit:1,fetchPage:async()=>[{id:'x',templateId,externalId:'other'}]}),/repeated_render/);
});
test('valid rows with absent or null external IDs and matching snake-case ID work',async()=>{
  const result=await findTemplatedRenderByExternalId({templateId,externalId,fetchPage:async()=>[{id:'a',templateId},{id:'b',templateId,externalId:null},{id:'c',templateId,external_id:externalId}]});
  assert.equal(result.render.id,'c');assert.equal(result.complete,true);
});
test('invalid pagination budgets are rejected before fetching',async()=>{
  for (const options of [{pageLimit:0},{pageLimit:101},{pageLimit:1.5},{maxPages:0},{maxPages:21},{maxPages:Infinity}]) {
    let calls=0;
    await assert.rejects(findTemplatedRenderByExternalId({templateId,externalId,...options,fetchPage:async()=>{calls++;return []}}),/invalid_input/);
    assert.equal(calls,0);
  }
});
