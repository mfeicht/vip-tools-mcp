import assert from "node:assert/strict";

import { buildAsanaTaskSearchParams } from "../lib/asana-search-params.js";

const modifiedSince = "2026-09-15T00:00:00.000Z";
const params = buildAsanaTaskSearchParams({
  extraParams: { "due_on.before": "2026-09-20" },
  text: "Asana-Deltafilter im Search-Wrapper reparieren",
  modifiedSince,
  involved: "1214979008788676",
  completed: false,
  sortBy: "modified_at",
  sortAscending: false,
  limit: 20,
  optFields: "gid,name,modified_at"
});

assert.equal(params.text, "Asana-Deltafilter im Search-Wrapper reparieren");
assert.equal(params["modified_at.after"], modifiedSince);
assert.equal(params["involved.any"], "1214979008788676");
assert.equal(params.modified_since, undefined);
assert.equal(params.involved_any, undefined);
assert.equal(params["due_on.before"], "2026-09-20");

const normalizedLegacyExtraParams = buildAsanaTaskSearchParams({
  extraParams: {
    text: "Legacy text",
    modified_since: "2026-09-14T00:00:00.000Z",
    involved_any: "1214979008788667"
  },
  text: "Explicit text",
  modifiedSince,
  involved: "1214979008788676",
  sortBy: "modified_at",
  limit: 10,
  optFields: "gid"
});

assert.equal(normalizedLegacyExtraParams.text, "Explicit text");
assert.equal(normalizedLegacyExtraParams["modified_at.after"], modifiedSince);
assert.equal(normalizedLegacyExtraParams["involved.any"], "1214979008788676");
assert.equal(normalizedLegacyExtraParams.modified_since, undefined);
assert.equal(normalizedLegacyExtraParams.involved_any, undefined);

console.log(JSON.stringify({
  text_filter: params.text,
  modified_filter: params["modified_at.after"],
  involved_filter: params["involved.any"],
  legacy_aliases_removed: params.modified_since === undefined && params.involved_any === undefined,
  explicit_filters_override_extra_params:
    normalizedLegacyExtraParams.text === "Explicit text" &&
    normalizedLegacyExtraParams["modified_at.after"] === modifiedSince &&
    normalizedLegacyExtraParams["involved.any"] === "1214979008788676"
}));
