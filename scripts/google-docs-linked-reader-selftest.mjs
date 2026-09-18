import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { assertLinkedGoogleDocScope, linkedGoogleDocReadback } from "../lib/google-docs-linked-reader.js";

const documentId = "linked_document_12345678901234567890";
const userGid = "1234567890123456";
const task = { notes: `Memo: https://docs.google.com/document/d/${documentId}/edit?tab=t.0`, assignee: { gid: userGid }, followers: [] };
assert.doesNotThrow(() => assertLinkedGoogleDocScope({ task, userGid, documentId }));
for (const role of ["created_by", "followers"]) {
  const related = { ...task, assignee: null, [role]: role === "followers" ? [{ gid: userGid }] : { gid: userGid } };
  assert.doesNotThrow(() => assertLinkedGoogleDocScope({ task: related, userGid, documentId }));
}
assert.throws(() => assertLinkedGoogleDocScope({ task, userGid: "other", documentId }), /eigene Asana-Beteiligung/);
assert.throws(() => assertLinkedGoogleDocScope({ task: { ...task, notes: `https://evil.example/?url=https://docs.google.com.evil/document/d/${documentId}` }, userGid, documentId }), /nicht.*verlinkt/);
assert.throws(() => assertLinkedGoogleDocScope({ task, userGid, documentId: documentId + "suffix" }), /nicht.*verlinkt/);
assert.throws(() => assertLinkedGoogleDocScope({ task, userGid, documentId: "../secret" }), /Ungueltige/);

const paragraph = (text) => ({ paragraph: { elements: [{ textRun: { content: text } }] } });
const tab = (tabId, title, content, childTabs = []) => ({ tabProperties: { tabId, title }, documentTab: { body: { content } }, childTabs });
const content = [paragraph("Abschnitt 2\n"), { table: { tableRows: [{ tableCells: [{ content: [paragraph("Links")] }, { content: [paragraph("Rechts")] }] }] } }, { tableOfContents: { content: [paragraph("Index\n")] } }, { paragraph: { elements: [{ inlineObjectElement: { inlineObjectId: "image" } }] } }];
const expectedText = "Abschnitt 2\nLinks\tRechts\nIndex\n";
const child = tab("t.child", "Untertab", [paragraph("Kind\n")]);
const document = { documentId, title: "Privates Memo", revisionId: "rev", tabs: [tab("t.0", "Memo", content, [child])] };
const result = linkedGoogleDocReadback(document, { documentId, tabId: "t.0" });
assert.equal(result.text, expectedText);
assert.equal(result.text_truncated, false);
assert.equal(result.total_chars, expectedText.length);
assert.equal(result.body_text_sha256, createHash("sha256").update(expectedText).digest("hex"));
assert.equal(result.tabs.length, 2);
assert.equal(linkedGoogleDocReadback(document, { documentId, tabId: "t.child" }).text, "Kind\n");
const window = linkedGoogleDocReadback(document, { documentId, tabId: "t.0", startChar: 12, maxChars: 5 });
assert.equal(window.text, expectedText.slice(12, 17));
assert.equal(window.text_truncated, true);
assert.equal(window.body_text_sha256, result.body_text_sha256);
assert.throws(() => linkedGoogleDocReadback(document, { documentId }), /nicht eindeutig/);
assert.throws(() => linkedGoogleDocReadback(document, { documentId, tabId: "missing" }), /nicht eindeutig/);
assert.throws(() => linkedGoogleDocReadback(document, { documentId: "wrong", tabId: "t.0" }), /ID stimmt/);
assert.throws(() => linkedGoogleDocReadback({ documentId, body: { content } }, { documentId }), /Tab-Topologie/);
assert.throws(() => linkedGoogleDocReadback({ documentId, tabs: [{ tabProperties: { tabId: "t.0" } }] }, { documentId }), /Body-Readback/);
assert.throws(() => linkedGoogleDocReadback(document, { documentId, tabId: "t.0", startChar: -1 }), /Textfenster/);
assert.throws(() => linkedGoogleDocReadback(document, { documentId, tabId: "t.0", maxChars: 50001 }), /Textfenster/);
assert.equal(linkedGoogleDocReadback({ ...document, tabs: [tab("t.0", "Memo", content)] }, { documentId }).text, expectedText);

const serverSource = readFileSync(new URL("../server.js", import.meta.url), "utf8");
const registration = serverSource.slice(serverSource.indexOf('    "google_docs_read_linked",'), serverSource.indexOf('    "google_drive_upload_csv_to_agent_folder",'));
assert.match(registration, /agent_id: z\.enum\(Object\.keys\(ASANA_TOKEN_ENVS\)\)/);
assert.match(registration, /TOOL_READ_ONLY/);
assert.ok(registration.indexOf("assertLinkedGoogleDocScope(") < registration.indexOf("googleRequest("));
assert.match(registration, /includeTabsContent: true/);
assert.match(registration, /after\.modifiedTime !== before\.modifiedTime/);
assert.doesNotMatch(registration, /method: "(?:POST|PATCH|PUT|DELETE)"/);
console.log(JSON.stringify({ scope_binding: "ok", native_tab_body_and_table_readback: "ok", ambiguous_and_missing_tabs_fail_closed: "ok", bounded_window_and_hash: "ok", explicit_identity_readonly_registration: "ok" }, null, 2));
