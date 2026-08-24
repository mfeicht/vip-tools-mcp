import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const port = process.env.GOOGLE_DRIVE_UPLOAD_TEST_PORT || "3011";
process.env.PORT = port;

const client = new Client({ name: "vip-google-drive-upload-selftest", version: "1.0.0" });
const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`));
const fixturePath = process.argv[2] ? path.resolve(process.argv[2]) : null;
const csvBytes = fixturePath
  ? await readFile(fixturePath)
  : Buffer.from("Begriff;Beschreibung\nAusweis;Test\n", "latin1");
const fixtureName = fixturePath ? path.basename(fixturePath) : "shadow.csv";
const sha256 = createHash("sha256").update(csvBytes).digest("hex");

function outputText(result) {
  return (result.content || []).map((item) => item.text || "").join("\n");
}

async function callUpload(arguments_) {
  return client.callTool({
    name: "google_drive_upload_csv_to_agent_folder",
    arguments: {
      agent_id: "vip-ai-marketing",
      file_name: fixtureName,
      expected_sha256: sha256,
      ...arguments_
    }
  });
}

await import("../server.js");
await new Promise((resolve) => setTimeout(resolve, 300));
await client.connect(transport);

try {
  const toolList = await client.listTools();
  const tool = (toolList.tools || []).find((entry) => entry.name === "google_drive_upload_csv_to_agent_folder");
  assert.ok(tool, "google_drive_upload_csv_to_agent_folder is missing");
  assert.ok(tool.inputSchema?.properties?.authorization, "authorization schema is missing");
  assert.ok(tool.inputSchema?.properties?.expected_sha256, "expected_sha256 schema is missing");
  assert.ok(tool.inputSchema?.properties?.content_base64, "content_base64 schema is missing");
  assert.ok(tool.inputSchema?.properties?.content_text, "content_text schema is missing");
  assert.equal(tool.annotations?.idempotentHint, true, "tool must advertise idempotent retry semantics");

  const dryRun = await callUpload({ content_base64: csvBytes.toString("base64") });
  assert.notEqual(dryRun.isError, true, outputText(dryRun));
  assert.match(outputText(dryRun), /"dry_run"\s*:\s*true/);
  assert.match(outputText(dryRun), new RegExp(`"sha256"\\s*:\\s*"${sha256}"`));
  assert.match(outputText(dryRun), /"content_mode"\s*:\s*"base64_bytes"/);

  const textPayload = "Begriff;Beschreibung\nText;UTF-8\n";
  const textSha256 = createHash("sha256").update(textPayload, "utf8").digest("hex");
  const textDryRun = await callUpload({
    file_name: "text-shadow.csv",
    content_text: textPayload,
    expected_sha256: textSha256
  });
  assert.notEqual(textDryRun.isError, true, outputText(textDryRun));
  assert.match(outputText(textDryRun), /"content_mode"\s*:\s*"utf8_text"/);

  const wrongHash = await callUpload({
    content_base64: csvBytes.toString("base64"),
    expected_sha256: "0".repeat(64)
  });
  assert.equal(wrongHash.isError, true, "mismatched SHA-256 must be blocked");

  const dualPayload = await callUpload({
    content_text: "a;b",
    content_base64: csvBytes.toString("base64")
  });
  assert.equal(dualPayload.isError, true, "dual text/base64 payload must be blocked");

  const invalidBase64 = await callUpload({ content_base64: "not base64!" });
  assert.equal(invalidBase64.isError, true, "invalid base64 must be blocked");

  const pathName = await callUpload({
    file_name: "../shadow.csv",
    content_base64: csvBytes.toString("base64")
  });
  assert.equal(pathName.isError, true, "file path input must be blocked");

  const missingAuthorization = await callUpload({
    content_base64: csvBytes.toString("base64"),
    dry_run: false
  });
  assert.equal(missingAuthorization.isError, true, "live upload without verified authorization must be blocked");

  console.log(
    JSON.stringify({
      status: "google drive upload contract self-test passed",
      fixture: fixturePath || "embedded",
      file_name: fixtureName,
      byte_length: csvBytes.length,
      sha256
    })
  );
  process.exit(0);
} finally {
  await client.close().catch(() => {});
}
