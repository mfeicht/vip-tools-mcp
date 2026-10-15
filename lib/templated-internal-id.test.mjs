import assert from "node:assert/strict";
import test from "node:test";
import { findTemplatedTemplateByInternalId } from "./templated-internal-id.js";

const target = "90c73f27-c6e2-421a-91b4-a1765ea63796";

test("internal id is found without sending an externalId filter, with complete pages and layers", async () => {
  const requests = [];
  const pages = [
    [{ id: "other", externalId: target }],
    [{ id: target, pages: [{ page: "Titelseite", layers: { "FORMAT REGION-copy": { text: "SWIPE" } } }] }],
    []
  ];
  const result = await findTemplatedTemplateByInternalId({
    templateId: target,
    fetchPage: async (request) => {
      requests.push(request);
      return pages[request.page];
    }
  });
  assert.equal(result.found, true);
  assert.equal(result.template.id, target);
  assert.equal(result.template.pages[0].layers["FORMAT REGION-copy"].text, "SWIPE");
  assert.equal(result.pages_scanned, 3);
  assert.equal(result.templates_scanned, 2);
  assert.ok(requests.every((request) => !Object.hasOwn(request, "externalId") &&
    request.includeLayers && request.includePages && request.limit === 50));
});

test("an externalId match does not claim the internal id is present", async () => {
  const pages = [[{ id: "other", externalId: target }], []];
  const result = await findTemplatedTemplateByInternalId({
    templateId: target,
    fetchPage: async ({ page }) => pages[page]
  });
  assert.equal(result.found, false);
  assert.equal(result.pagination_complete, true);
});

test("duplicate internal ids and incomplete or malformed pagination fail closed", async () => {
  await assert.rejects(findTemplatedTemplateByInternalId({
    templateId: target,
    fetchPage: async ({ page }) => page < 2 ? [{ id: target }] : []
  }), /templated_internal_id_duplicate/);
  await assert.rejects(findTemplatedTemplateByInternalId({
    templateId: target,
    maxPages: 2,
    fetchPage: async () => [{ id: "other" }]
  }), /templated_template_scan_incomplete/);
  await assert.rejects(findTemplatedTemplateByInternalId({
    templateId: target,
    fetchPage: async () => ({ items: [] })
  }), /templated_template_page_invalid/);
});
