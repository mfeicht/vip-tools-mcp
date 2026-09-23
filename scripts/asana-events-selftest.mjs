import assert from "node:assert/strict";
import { extractAsanaEventsSyncToken, readAsanaResourceEvents } from "../lib/asana-events.js";

const passed = [];
async function test(name, run) {
  await run();
  passed.push(name);
}

function response(data) {
  return { data };
}

function errorResponse(status, data) {
  const error = new Error(`HTTP ${status}`);
  error.response = { status, data };
  return error;
}

await test("extracts documented and wrapped sync tokens", async () => {
  assert.equal(extractAsanaEventsSyncToken({ sync: "root" }), "root");
  assert.equal(extractAsanaEventsSyncToken({ data: { sync: "nested" } }), "nested");
  assert.equal(extractAsanaEventsSyncToken({ error: { sync: "error" } }), "error");
  assert.equal(extractAsanaEventsSyncToken({}), null);
});

await test("initial HTTP 412 returns a bootstrap cursor without events", async () => {
  const result = await readAsanaResourceEvents({
    resourceGid: "1214338226145911",
    request: async () => { throw errorResponse(412, { sync: "first-token" }); }
  });
  assert.equal(result.events_status, "bootstrap_required");
  assert.equal(result.sync_token, "first-token");
  assert.equal(result.event_count, 0);
  assert.equal(result.reconciliation_required, true);
});

await test("expired cursor returns an explicit reset instead of pretending success", async () => {
  const result = await readAsanaResourceEvents({
    resourceGid: "1214338226145911",
    sync: "expired-token",
    request: async () => { throw errorResponse(412, { data: { sync: "replacement-token" } }); }
  });
  assert.equal(result.events_status, "sync_reset_required");
  assert.equal(result.sync_token, "replacement-token");
  assert.deepEqual(result.events, []);
});

await test("successful event pages expose the next cursor and backlog flag", async () => {
  const requests = [];
  const events = [{ action: "changed", resource: { gid: "9", resource_type: "task" } }];
  const result = await readAsanaResourceEvents({
    resourceGid: "1214338226145911",
    sync: "current-token",
    optFields: "action,resource.gid,resource.resource_type",
    request: async (config) => {
      requests.push(config);
      return response({ data: events, sync: "next-token", has_more: true });
    }
  });
  assert.equal(result.events_status, "ok");
  assert.equal(result.sync_token, "next-token");
  assert.equal(result.has_more, true);
  assert.deepEqual(result.events, events);
  assert.deepEqual(requests[0].params, {
    resource: "1214338226145911",
    sync: "current-token",
    opt_fields: "action,resource.gid,resource.resource_type"
  });
});

await test("missing cursors and malformed inputs fail closed", async () => {
  await assert.rejects(
    () => readAsanaResourceEvents({ resourceGid: "bad", request: async () => response({}) }),
    /numerische Asana-GID/
  );
  await assert.rejects(
    () => readAsanaResourceEvents({ resourceGid: "1", sync: " ", request: async () => response({}) }),
    /sync darf nicht leer/
  );
  await assert.rejects(
    () => readAsanaResourceEvents({ resourceGid: "1", request: async () => response({ data: [] }) }),
    /ohne neuen Sync-Token/
  );
  await assert.rejects(
    () => readAsanaResourceEvents({ resourceGid: "1", request: async () => { throw errorResponse(412, {}); } }),
    /HTTP 412 ohne neuen Sync-Token/
  );
});

console.log(JSON.stringify({ passed: passed.length, tests: passed }, null, 2));
