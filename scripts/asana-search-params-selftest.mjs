import assert from "node:assert/strict";
import { buildAsanaTaskSearchPlan, executeAsanaInvolvedSearch, normalizeAsanaSearchUsers } from "../lib/asana-search-params.js";

const passed = [];
async function test(name, run) { await run(); passed.push(name); }
const cursor = "2026-09-15T00:00:00.000Z";
const plan = (options = {}) => buildAsanaTaskSearchPlan({ involved: "7", meGid: "7", modifiedSince: cursor, ...options });
const task = (gid, role = "assignee", createdSecond = 0, modifiedSecond = createdSecond) => ({
  gid: String(gid), created_at: new Date(Date.UTC(2026, 8, 16, 0, 0, createdSecond)).toISOString(),
  modified_at: new Date(Date.UTC(2026, 8, 16, 1, 0, modifiedSecond)).toISOString(),
  assignee: { gid: role === "assignee" ? "7" : "8" },
  created_by: { gid: role === "created_by" ? "7" : "8" }, followers: role === "followers" ? [{ gid: "7" }] : []
});
function mockSearch(data, requests = []) {
  return async (params) => {
    requests.push(structuredClone(params));
    let matches = data.filter((row) => ["assignee", "created_by", "followers"].every((role) => {
      const members = role === "followers" ? row.followers.map((user) => user.gid) : [row[role]?.gid];
      const any = params[`${role}.any`]?.split(",");
      const not = params[`${role}.not`]?.split(",");
      return (!any || members.some((gid) => any.includes(gid))) && (!not || !members.some((gid) => not.includes(gid)));
    }));
    for (const field of ["created_at", "modified_at"]) {
      if (params[`${field}.after`]) matches = matches.filter((row) => Date.parse(row[field]) > Date.parse(params[`${field}.after`]));
      if (params[`${field}.before`]) matches = matches.filter((row) => Date.parse(row[field]) < Date.parse(params[`${field}.before`]));
    }
    return matches.sort((a, b) => Date.parse(a.created_at) - Date.parse(b.created_at)).slice(0, params.limit);
  };
}

await test("canonical branches preserve shared delta/text/project/completion filters", async () => {
  const p = plan({ text: "Delta", completed: false, extraParams: { "projects.any": "12", modified_since: "old", involved_any: "8" } });
  assert.equal(p.params["modified_at.after"], cursor);
  assert.equal(p.params["involved.any"], undefined);
  assert.equal(p.params.involved_any, undefined);
  assert.equal(p.params.modified_since, undefined);
  for (const branch of p.branches) {
    assert.equal(branch.params["modified_at.after"], cursor);
    assert.equal(branch.params.text, "Delta");
    assert.equal(branch.params.completed, false);
    assert.equal(branch.params["projects.any"], "12");
    assert.equal(branch.params["involved.any"], undefined);
    assert.equal(branch.params[`${branch.role}.any`], "7");
  }
});
await test("legacy and canonical extra_params involvement use the same OR plan", async () => {
  for (const key of ["involved_any", "involved.any"]) {
    const p = plan({ involved: undefined, extraParams: { [key]: "me,8" } });
    assert.deepEqual(p.involved_user_gids, ["7", "8"]);
    assert.equal(p.params[key], undefined);
  }
});
await test("me and multiple GIDs are resolved and deduplicated", async () => {
  assert.equal(normalizeAsanaSearchUsers(" me,8,7,8 ", "7"), "7,8");
  const p = plan({ involved: "me,8", assignee: "me,8", followers: "me" });
  assert.equal(p.params["assignee.any"], "7,8");
  assert.equal(p.params["followers.any"], "7");
});
await test("invalid or empty selectors fail instead of becoming global search", async () => {
  for (const value of ["", " ", "7,", "bad", "7,bad", null]) assert.throws(() => plan({ involved: value }));
  assert.throws(() => normalizeAsanaSearchUsers("me", undefined));
  assert.throws(() => plan({ involved: Array(26).fill("7").join(",") }));
});
await test("top-level selectors override legacy and canonical extras", async () => {
  const p = plan({ assignee: "7", followers: "8", extraParams: { assignee_any: "9", "assignee.any": "10", followers_any: "9" } });
  assert.equal(p.params["assignee.any"], "7");
  assert.equal(p.params["followers.any"], "8");
});
await test("shared same-role selectors are intersected; other constraints remain", async () => {
  const p = plan({ assignee: "8", involved: "7", extraParams: { "followers.not": "9" } });
  assert.deepEqual(p.branches[0].user_gids, []);
  assert.equal(p.branches[1].params["assignee.any"], undefined);
  assert.equal(p.user_constraints["assignee.any"], "8");
  const result = await executeAsanaInvolvedSearch(p, mockSearch([task(1, "created_by")]));
  assert.deepEqual(result.tasks.map((row) => row.gid), ["1"]);
  assert.equal(result.branches[0].pages, 0);
});
await test("same-role negative selector yields empty intersection", async () => {
  const p = plan({ extraParams: { "assignee.not": "me" } });
  assert.deepEqual(p.branches[0].user_gids, []);
});
await test("regular search remains a single documented query", async () => {
  const p = plan({ involved: undefined, assignee: "me,8", text: "Task", limit: 10 });
  assert.deepEqual(p.branches, []);
  assert.equal(p.params.limit, 10);
  assert.equal(p.params["assignee.any"], "7,8");
  assert.equal(p.params.opt_fields, "gid,name");
});
await test("unsupported relevance, offset and invalid creation bounds fail explicitly", async () => {
  assert.throws(() => plan({ sortBy: "relevance" }));
  assert.throws(() => plan({ sortBy: "toString" }));
  assert.throws(() => plan({ extraParams: { offset: "abc" } }));
  assert.throws(() => plan({ extraParams: { "created_at.after": "bad" } }));
});
await test("positive assignee/creator/follower membership and negative task exclusion", async () => {
  const result = await executeAsanaInvolvedSearch(plan(), mockSearch([task(1), task(2, "created_by", 1), task(3, "followers", 2), task(4, "foreign", 3)]));
  assert.deepEqual(result.tasks.map((row) => row.gid), ["3", "2", "1"]);
  assert.equal(result.search_complete, true);
  assert.equal(result.delta_cursor_advance_allowed, true);
});
await test("multiple requested users match any selected role", async () => {
  const result = await executeAsanaInvolvedSearch(plan({ involved: "7,8" }), mockSearch([task(1), task(2, "foreign", 1)]));
  assert.deepEqual(result.tasks.map((row) => row.gid), ["2", "1"]);
});
await test("multi-user OR survives provider comma selectors behaving as intersection", async () => {
  const p = plan({ involved: "7,8" });
  const rows = [task(1), { ...task(2, "followers", 1), followers: [{ gid: "8" }], assignee: { gid: "9" }, created_by: { gid: "9" } }];
  const provider = mockSearch(rows);
  const result = await executeAsanaInvolvedSearch(p, async (params) => {
    assert(["assignee", "created_by", "followers"].every((role) => !params[`${role}.any`]?.includes(",")));
    return provider(params);
  });
  assert.deepEqual(result.tasks.map((row) => row.gid), ["2", "1"]);
  assert.equal(result.branches.length, 6);
});
await test("common multi-user AND constraints are evaluated without provider comma semantics", async () => {
  const p = plan({ involved: "7", assignee: "8,9", followers: "7,10" });
  const result = await executeAsanaInvolvedSearch(p, mockSearch([
    { ...task(1, "created_by"), assignee: { gid: "8" }, followers: [{ gid: "7" }] },
    { ...task(2, "created_by", 1), assignee: { gid: "9" }, followers: [{ gid: "10" }] },
    { ...task(3, "created_by", 2), assignee: { gid: "11" }, followers: [{ gid: "10" }] }
  ]));
  assert.deepEqual(result.tasks.map((row) => row.gid), ["2", "1"]);
  assert.equal(result.search_complete, true);
});
await test("global 30-page budget bounds multi-user fanout and marks remaining coverage partial", async () => {
  const p = plan({ involved: Array.from({ length: 11 }, (_, index) => String(index + 1)).join(",") });
  let calls = 0;
  const result = await executeAsanaInvolvedSearch(p, async () => { calls++; return []; });
  assert.equal(calls, 30);
  assert.equal(result.total_page_budget, 30);
  assert.equal(result.search_status, "partial");
  assert.equal(result.delta_cursor_advance_allowed, false);
  assert.equal(result.branches.filter((branch) => branch.status === "total_page_limit").length, 3);
});
await test("cross-branch duplicate is returned once with newest observed version", async () => {
  const duplicate = { ...task(1), created_by: { gid: "7" }, followers: [{ gid: "7" }] };
  let count = 0;
  const result = await executeAsanaInvolvedSearch(plan(), async () => [{ ...duplicate, modified_at: task(1, "assignee", 0, count++).modified_at }]);
  assert.equal(result.matched_count, 1);
  assert.equal(result.tasks[0].modified_at, task(1, "assignee", 0, 2).modified_at);
});
await test("global sort and limit apply after union; truncation blocks delta cursor", async () => {
  const result = await executeAsanaInvolvedSearch(plan({ limit: 1 }), mockSearch([task(1, "assignee", 0, 1), task(2, "created_by", 1, 9), task(3, "followers", 2, 5)]));
  assert.deepEqual(result.tasks.map((row) => row.gid), ["2"]);
  assert.equal(result.matched_count, 3);
  assert.equal(result.result_truncated, true);
  assert.equal(result.delta_cursor_advance_allowed, false);
});
await test("creation-pagination carries common cursor and overlaps boundary", async () => {
  const requests = [];
  const rows = Array.from({ length: 6 }, (_, index) => task(index + 1, "assignee", index));
  const result = await executeAsanaInvolvedSearch(plan(), mockSearch(rows, requests), { pageSize: 3 });
  assert.equal(result.matched_count, 6);
  assert.equal(result.search_complete, true);
  assert.equal(result.branches[0].pages, 3);
  assert.equal(requests[1]["created_at.after"], new Date(Date.parse(rows[2].created_at) - 1).toISOString());
  assert(requests.every((params) => params["modified_at.after"] === cursor && params.sort_by === "created_at" && params.sort_ascending === true));
});
await test("exact full final page is exhausted by an additional overlap read", async () => {
  const result = await executeAsanaInvolvedSearch(plan(), mockSearch([task(1), task(2, "assignee", 1), task(3, "assignee", 2)]), { pageSize: 3 });
  assert.equal(result.search_complete, true);
  assert.equal(result.branches[0].pages, 2);
});
await test("page budget produces explicit partial result without cursor advance", async () => {
  const result = await executeAsanaInvolvedSearch(plan(), mockSearch(Array.from({ length: 5 }, (_, index) => task(index + 1, "assignee", index))), { pageSize: 3, maxPagesPerBranch: 1 });
  assert.equal(result.search_status, "partial");
  assert.equal(result.branches[0].status, "page_limit");
  assert.equal(result.delta_cursor_advance_allowed, false);
});
await test("saturated identical creation timestamps never skip unseen members", async () => {
  const result = await executeAsanaInvolvedSearch(plan(), mockSearch([task(1), task(2), task(3), task(4)]), { pageSize: 3 });
  assert.equal(result.search_complete, false);
  assert.equal(result.branches[0].status, "ambiguous_timestamp_boundary");
  assert.equal(result.branches[0].pages, 2);
  assert.equal(result.delta_cursor_advance_allowed, false);
});
await test("original creation lower and upper bounds are preserved", async () => {
  const rows = Array.from({ length: 6 }, (_, index) => task(index + 1, "assignee", index));
  const requests = [];
  const after = rows[0].created_at;
  const before = rows[5].created_at;
  const result = await executeAsanaInvolvedSearch(plan({ extraParams: { "created_at.after": after, "created_at.before": before } }), mockSearch(rows, requests), { pageSize: 3 });
  assert.equal(result.matched_count, 4);
  assert(requests.every((params) => Date.parse(params["created_at.after"]) >= Date.parse(after) && params["created_at.before"] === before));
});
await test("foreign API member fails closed and is not returned", async () => {
  const result = await executeAsanaInvolvedSearch(plan(), async () => [task(1, "foreign")]);
  assert.equal(result.search_status, "partial");
  assert.equal(result.tasks.length, 0);
  assert(result.branches.every((branch) => branch.status === "invalid_membership_or_page"));
});
await test("missing timestamps, unsorted pages and duplicate page GIDs are partial", async () => {
  for (const rows of [[{ ...task(1), created_at: undefined }], [{ ...task(1), modified_at: undefined }], [task(2, "assignee", 2), task(1, "assignee", 1)], [task(1), task(1)]]) {
    const result = await executeAsanaInvolvedSearch(plan(), async (params) => params["assignee.any"] ? rows : []);
    assert.equal(result.search_status, "partial");
    assert.equal(result.delta_cursor_advance_allowed, false);
  }
});
await test("empty OR branches give verified empty sample", async () => {
  const result = await executeAsanaInvolvedSearch(plan(), async () => []);
  assert.equal(result.search_complete, true);
  assert.equal(result.matched_count, 0);
});
await test("API errors stop sequential branch execution without retries", async () => {
  let calls = 0;
  await assert.rejects(() => executeAsanaInvolvedSearch(plan(), async () => { calls++; throw new Error("429"); }), /429/);
  assert.equal(calls, 1);
});
await test("creation ascending and numeric GID tie-break order", async () => {
  const result = await executeAsanaInvolvedSearch(plan({ sortBy: "created_at", sortAscending: true }), mockSearch([task(10), task(2)]));
  assert.deepEqual(result.tasks.map((row) => row.gid), ["2", "10"]);
});
await test("due/completion/likes sort values are compared across all branches", async () => {
  const a = { ...task(1), due_on: "2026-09-18", completed_at: null, num_likes: 1 };
  const b = { ...task(2, "created_by", 1), due_at: "2026-09-17T10:00:00Z", completed_at: "2026-09-16T10:00:00Z", num_likes: 2 };
  for (const sortBy of ["due_date", "completed_at", "likes"]) {
    const result = await executeAsanaInvolvedSearch(plan({ sortBy, sortAscending: sortBy === "due_date" }), mockSearch([a, b]));
    assert.deepEqual(result.tasks.map((row) => row.gid), ["2", "1"]);
  }
});
console.log(JSON.stringify({ status: "PASS", tests: passed.length, passed }, null, 2));
