import assert from "node:assert/strict";
import axios from "axios";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

process.env.PORT = process.env.GOOGLE_DOCS_READ_TEST_PORT || "3014";
process.env.ASANA_TOKEN_VIP_AI_OPERATIONS = "local-shadow-asana";
process.env.GOOGLE_DRIVE_OAUTH_CLIENT_ID = "local-shadow-client";
process.env.GOOGLE_DRIVE_OAUTH_CLIENT_SECRET = "local-shadow-secret";
process.env.GOOGLE_DRIVE_OAUTH_REFRESH_TOKEN = "local-shadow-refresh";
process.env.GOOGLE_SERVICE_ACCOUNT_JSON = "";
process.env.GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 = "";
process.env.GOOGLE_DRIVE_AUTH_PRIORITY = "oauth";
const documentId = "linked_document_12345678901234567890";
const userGid = "1234567890123456";
let scenario = "valid";
let calls = [];
let metadataReads = 0;
// Stub every provider request before loading the production server. Only the
// local MCP transport uses real networking; no external credentials or calls.
axios.create = (config) => {
  assert.equal(config.headers.Authorization, "Bearer local-shadow-asana");
  return { request: async (request) => {
    assert.equal(request.method, "GET");
    calls.push(`asana:${request.url}`);
    if (request.url === "/users/me") return { data: { data: { gid: userGid } } };
    assert.equal(request.url, `/tasks/${userGid}`);
    return { data: { data: { notes: scenario === "unlinked" ? "No document link" : `https://docs.google.com/document/d/${documentId}/edit?tab=t.0`, assignee: { gid: scenario === "foreign" ? "other" : userGid }, followers: [] } } };
  } };
};
axios.post = async (url) => {
  assert.equal(url, "https://oauth2.googleapis.com/token");
  calls.push("google:mock_oauth_refresh");
  return { data: { access_token: "local-shadow-access", expires_in: 3600 } };
};
axios.request = async (request) => {
  assert.equal(request.method, "GET");
  assert.equal(request.headers.Authorization, "Bearer local-shadow-access");
  calls.push(`google:${request.url}`);
  if (request.url.includes("/drive/v3/files/")) {
    metadataReads += 1;
    return { data: { id: documentId, mimeType: scenario === "mime" ? "text/plain" : "application/vnd.google-apps.document", trashed: scenario === "trashed", modifiedTime: scenario === "changed" && metadataReads > 1 ? "2026-09-19T00:00:00Z" : "2026-09-18T00:00:00Z" } };
  }
  assert.equal(request.url, `https://docs.googleapis.com/v1/documents/${documentId}`);
  assert.equal(request.params.includeTabsContent, true);
  assert.equal(request.params.suggestionsViewMode, "PREVIEW_WITHOUT_SUGGESTIONS");
  return { data: { documentId, title: "Mock memo", revisionId: "mock-revision", tabs: [{ tabProperties: { tabId: "t.0", title: "Memo" }, documentTab: { body: { content: [{ paragraph: { elements: [{ textRun: { content: "Abschnitt 2\nVerifizierter Testtext\n" } }] } }] } } }] } };
};
const client = new Client({ name: "vip-linked-docs-readonly-shadow", version: "1.0.0" });
await import("../server.js");
await new Promise((resolve) => setTimeout(resolve, 300));
await client.connect(new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${process.env.PORT}/mcp`)));
try {
  const list = await client.listTools();
  const tool = list.tools.find((item) => item.name === "google_docs_read_linked");
  assert.ok(tool);
  assert.equal(tool.annotations.readOnlyHint, true);
  assert.notEqual(tool.annotations.destructiveHint, true);
  assert.ok(tool.inputSchema.required.includes("agent_id"));
  assert.ok(tool.inputSchema.required.includes("asana_task_gid"));
  assert.ok(tool.inputSchema.required.includes("document_id"));
  assert.equal(tool.inputSchema.properties.agent_id.default, undefined);
  const args = { agent_id: "vip-ai-operations", asana_task_gid: "1234567890123456", document_id: "linked_document_12345678901234567890", dry_run: true };
  for (const invalid of [
    { ...args, agent_id: undefined },
    { ...args, document_id: "../secret" },
    { ...args, asana_task_gid: "../users/me" },
    { ...args, max_chars: 50001 }
  ]) {
    const result = await client.callTool({ name: tool.name, arguments: invalid });
    assert.equal(result.isError, true);
  }
  assert.equal(calls.length, 0);
  const invoke = async (nextScenario, extra = {}) => {
    scenario = nextScenario; calls = []; metadataReads = 0;
    return client.callTool({ name: tool.name, arguments: { ...args, tab_id: "t.0", ...extra } });
  };
  const dry = await invoke("valid");
  assert.notEqual(dry.isError, true);
  assert.deepEqual(calls, [`asana:/users/me`, `asana:/tasks/${userGid}`]);
  for (const nextScenario of ["unlinked", "foreign"]) {
    const denied = await invoke(nextScenario, { dry_run: false });
    assert.equal(denied.isError, true);
    assert.equal(calls.filter((item) => item.startsWith("google:")).length, 0);
  }
  const liveFixture = await invoke("valid", { dry_run: false });
  assert.notEqual(liveFixture.isError, true);
  const body = JSON.parse(liveFixture.content[0].text);
  assert.equal(body.verification_status, "verified");
  assert.equal(body.text, "Abschnitt 2\nVerifizierter Testtext\n");
  assert.equal(body.tab_id, "t.0");
  assert.equal(metadataReads, 2);
  for (const nextScenario of ["mime", "trashed", "changed"]) {
    const denied = await invoke(nextScenario, { dry_run: false });
    assert.equal(denied.isError, true);
    if (nextScenario !== "changed") assert.equal(calls.some((item) => item.includes("docs.googleapis.com")), false);
  }
  console.log(JSON.stringify({ mcp_registration: "ok", readonly_annotations: "ok", required_explicit_identity_and_task: "ok", invalid_inputs_rejected_before_provider: "ok", task_scope_before_google: "ok", production_callback_mock_readback: "ok", wrong_mime_trash_and_concurrent_change_fail_closed: "ok", live_asana_or_google_calls: 0 }));
  process.exit(0);
} finally {
  await client.close().catch(() => {});
}
