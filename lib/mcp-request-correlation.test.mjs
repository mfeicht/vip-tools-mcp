import assert from "node:assert/strict";
import test from "node:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { createCorrelatedMcpFetch, mcpTraceId, safeMcpResponseHeaders } from "./mcp-request-correlation.js";

test("failed diagnostic sink cannot alter response, replay a request or replace its exception", async () => {
  let calls = 0;
  const broken = () => { throw new Error("diagnostics unavailable"); };
  const response = new Response("upstream failure", { status: 503 });
  const read = createCorrelatedMcpFetch(async () => { calls++; return response; }, broken);
  assert.equal(await read("https://example.test/mcp"), response);
  assert.equal(calls, 1);
  const original = new Error("network failure");
  const fail = createCorrelatedMcpFetch(async () => { calls++; throw original; }, broken);
  await assert.rejects(fail("https://example.test/mcp"), error => error === original);
  assert.equal(calls, 2);
});

test("client adds a valid trace and reports only allowlisted 5xx metadata", async () => {
  const reports = [];
  let sentHeaders;
  const fetchWithTrace = createCorrelatedMcpFetch(async (_url, init) => {
    sentHeaders = init.headers;
    return new Response("private response body", { status: 502, headers: {
      "cf-ray": "abc123-FRA", "rndr-id": "928d42b5-990c-4535",
      "x-vip-request-id": init.headers.get("x-vip-request-id"),
      "set-cookie": "secret=private"
    } });
  }, (record) => reports.push(record));
  const response = await fetchWithTrace("https://example.test/mcp", {
    method: "POST", headers: { authorization: "Bearer private" }, body: "private request body"
  });
  assert.equal(response.status, 502);
  assert.equal(sentHeaders.get("authorization"), "Bearer private");
  assert.equal(mcpTraceId(sentHeaders.get("x-vip-request-id")), sentHeaders.get("x-vip-request-id"));
  assert.equal(reports.length, 1);
  assert.equal(reports[0].trace_id, sentHeaders.get("x-vip-request-id"));
  assert.deepEqual(reports[0].response_headers,
    { "cf-ray": "abc123-FRA", "rndr-id": "928d42b5-990c-4535",
      "x-vip-request-id": sentHeaders.get("x-vip-request-id") });
  assert.equal(JSON.stringify(reports).includes("private"), false);
});

test("successful responses are silent and unsafe request or response IDs are replaced or dropped", async () => {
  const reports = [];
  const fetchWithTrace = createCorrelatedMcpFetch(async () => new Response("ok"),
    (record) => reports.push(record));
  await fetchWithTrace("https://example.test/mcp");
  assert.deepEqual(reports, []);
  assert.notEqual(mcpTraceId("injected\nlog"), "injected\nlog");
  assert.deepEqual(safeMcpResponseHeaders(new Headers({ "x-request-id": "unsafe,header" })), {});
});

test("network errors retain the same exception and only report safe metadata", async () => {
  const reports = [];
  const failure = new Error("private connection detail");
  const fetchWithTrace = createCorrelatedMcpFetch(async () => { throw failure; },
    (record) => reports.push(record));
  await assert.rejects(fetchWithTrace("https://example.test/mcp"), (error) => error === failure);
  assert.equal(reports.length, 1);
  assert.equal(reports[0].event, "mcp_http_transport_error");
  assert.equal(JSON.stringify(reports).includes("private"), false);
});

test("the real SDK initialize request carries the trace and reports one HTTP 503", async () => {
  const reports = [];
  const requests = [];
  const client = new Client({ name: "correlation-shadow", version: "1.0.0" });
  const transport = new StreamableHTTPClientTransport(
    new URL("https://example.test/mcp"),
    { fetch: createCorrelatedMcpFetch(async (_url, init) => {
      requests.push({ method: init.method, trace: init.headers.get("x-vip-request-id") });
      return new Response(null, { status: 503,
        headers: { "x-vip-request-id": init.headers.get("x-vip-request-id") } });
    }, (record) => reports.push(record)) }
  );
  await assert.rejects(client.connect(transport), (error) => error.code === 503);
  await client.close();
  assert.equal(requests.length, 1);
  assert.equal(requests[0].method, "POST");
  assert.equal(requests[0].trace, reports[0].trace_id);
  assert.equal(reports[0].status, 503);
  assert.equal(reports[0].response_headers["x-vip-request-id"], requests[0].trace);
});
