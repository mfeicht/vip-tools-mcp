import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createImapTlsTestServer } from "./imap-tls-test-fixture.mjs";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

function parse(result) {
  const value = result.content?.find((item) => item.type === "text")?.text || "{}";
  return JSON.parse(value);
}

const trashMailbox = "INBOX.Trash";
const messageId10 = "<cleanup-10@example.com>";
const messageId11 = "<cleanup-11@example.com>";
const messageIdHash10 = createHash("sha256").update(messageId10).digest("hex");
const sender = "sender@example.com";
const subject10 = "Bereits verarbeitete Nachricht";
let source10Present = true;
let trash10Present = false;
let moveCount = 0;
let loginCount = 0;
const sockets = new Set();

function header({ subject, messageId, day }) {
  return [
    `From: Sender <${sender}>`,
    `Subject: ${subject}`,
    `Date: Mon, ${day} Aug 2026 10:00:00 +0000`,
    `Message-ID: ${messageId}`,
    "",
    ""
  ].join("\r\n");
}

function writeHeaderFetch(socket, tag, uid, value, { withSnippet = false } = {}) {
  const snippet = "Diese Nachricht wurde bereits vollstaendig verarbeitet.";
  socket.write(
    `* 1 FETCH (UID ${uid} FLAGS (\\Seen) RFC822.SIZE 640 BODY[HEADER.FIELDS (FROM SUBJECT DATE MESSAGE-ID)] {${Buffer.byteLength(value)}}\r\n${value}${withSnippet ? ` BODY[TEXT]<0> {${Buffer.byteLength(snippet)}}\r\n${snippet}` : ""})\r\n${tag} OK FETCH completed\r\n`
  );
}

const { server: fakeImap } = await createImapTlsTestServer((socket) => {
  sockets.add(socket);
  socket.once("close", () => sockets.delete(socket));
  socket.setEncoding("utf8");
  socket.write("* OK fake-imap ready\r\n");
  let selectedMailbox = "";
  let buffered = "";
  socket.on("data", (chunk) => {
    buffered += chunk;
    while (buffered.includes("\r\n")) {
      const end = buffered.indexOf("\r\n");
      const line = buffered.slice(0, end);
      buffered = buffered.slice(end + 2);
      if (!line) continue;
      const match = /^(a\d+)\s+(.+)$/.exec(line);
      if (!match) continue;
      const [, tag, command] = match;
      if (/^LOGIN\b/i.test(command)) {
        loginCount += 1;
        socket.write(`${tag} OK LOGIN completed\r\n`);
      } else if (/^LIST\b/i.test(command)) {
        socket.write(
          `* LIST (\\HasNoChildren) "." "INBOX"\r\n* LIST (\\HasNoChildren \\Trash) "." "${trashMailbox}"\r\n${tag} OK LIST completed\r\n`
        );
      } else if (/^(?:EXAMINE|SELECT)\b/i.test(command)) {
        selectedMailbox = /"([^"]+)"\s*$/.exec(command)?.[1] || command.split(/\s+/).at(-1);
        socket.write(`* 2 EXISTS\r\n${tag} OK mailbox selected\r\n`);
      } else if (/^UID SEARCH BEFORE\b/i.test(command)) {
        const uids = [source10Present ? "10" : "", "11"].filter(Boolean).join(" ");
        socket.write(`* SEARCH ${uids}\r\n${tag} OK SEARCH completed\r\n`);
      } else if (/^UID SEARCH HEADER Message-ID\b/i.test(command)) {
        const found = selectedMailbox === trashMailbox && trash10Present && command.includes(messageId10);
        socket.write(`* SEARCH ${found ? "90" : ""}\r\n${tag} OK SEARCH completed\r\n`);
      } else if (/^UID FETCH\s+10\b/i.test(command)) {
        if (selectedMailbox === "INBOX" && source10Present) {
          writeHeaderFetch(socket, tag, "10", header({ subject: subject10, messageId: messageId10, day: "03" }), {
            withSnippet: /BODY\.PEEK\[TEXT\]/i.test(command)
          });
        } else {
          socket.write(`${tag} OK FETCH completed\r\n`);
        }
      } else if (/^UID FETCH\s+11\b/i.test(command)) {
        writeHeaderFetch(
          socket,
          tag,
          "11",
          header({ subject: "Noch zu pruefen", messageId: messageId11, day: "04" }),
          { withSnippet: /BODY\.PEEK\[TEXT\]/i.test(command) }
        );
      } else if (/^UID MOVE\s+10\b/i.test(command)) {
        assert.equal(selectedMailbox, "INBOX");
        source10Present = false;
        trash10Present = true;
        moveCount += 1;
        socket.write(`${tag} OK MOVE completed\r\n`);
      } else if (/^LOGOUT$/i.test(command)) {
        socket.write(`* BYE logout\r\n${tag} OK LOGOUT completed\r\n`);
        socket.end();
      } else {
        socket.write(`${tag} BAD unsupported\r\n`);
      }
    }
  });
});

await new Promise((resolve) => fakeImap.listen(0, "127.0.0.1", resolve));
const imapPort = fakeImap.address().port;
const mcpPort = process.env.EMAIL_LIFECYCLE_TEST_PORT || "3020";
process.env.PORT = mcpPort;
process.env.IMAP_HOST_VIP_AI_MARKETING = "127.0.0.1";
process.env.IMAP_PORT_VIP_AI_MARKETING = String(imapPort);
process.env.IMAP_SECURE_VIP_AI_MARKETING = "true";
process.env.IMAP_USER_VIP_AI_MARKETING = "marketing-agent@vip-studios.de";
process.env.IMAP_PASSWORD_VIP_AI_MARKETING = "test-only";

const client = new Client({ name: "vip-email-lifecycle-selftest", version: "1.0.0" });
const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${mcpPort}/mcp`));
await import("../server.js");
await new Promise((resolve) => setTimeout(resolve, 300));
await client.connect(transport);

const lifecycleArguments = {
  agent_id: "vip-ai-marketing",
  uid: "10",
  expected_message_id_hash: messageIdHash10,
  expected_from_email: sender,
  expected_subject: subject10,
  disposition_reason: "processed_and_persisted",
  processing_complete: true,
  no_pending_action: true,
  no_retention_hold: true,
  downstream_readback_verified: true,
  decision_note: "Die relevante Information ist verifiziert weiterverarbeitet."
};

try {
  const tools = await client.listTools();
  assert.equal(tools.tools.some((tool) => tool.name === "agent_email_list_cleanup_candidates"), true);
  assert.equal(tools.tools.some((tool) => tool.name === "agent_email_trash_processed"), true);

  const configArguments = { agent_id: "vip-ai-marketing", check_smtp_auth: false };
  const tlsConfig = parse(await client.callTool({ name: "agent_email_check_config", arguments: configArguments }));
  assert.equal(tlsConfig.imap_transport_policy, "implicit-tls-required-v1");
  assert.equal(tlsConfig.imap_certificate_validation_required, true);
  assert.equal(tlsConfig.imap_plaintext_fallback_allowed, false);
  assert.equal(tlsConfig.imap_ready_for_read, true);
  process.env.IMAP_SECURE_VIP_AI_MARKETING = "false";
  const blockedConfig = parse(await client.callTool({ name: "agent_email_check_config", arguments: configArguments }));
  assert.equal(blockedConfig.imap_ready_for_read, false);
  assert.deepEqual(blockedConfig.imap_candidates, []);
  const blockedRead = await client.callTool({
    name: "agent_email_read_unseen", arguments: { agent_id: "vip-ai-marketing", limit: 0 }
  });
  assert.equal(blockedRead.isError, true);
  assert.match(blockedRead.content?.find((item) => item.type === "text")?.text || "", /IMAP_TLS_REQUIRED/);
  assert.equal(loginCount, 0);
  process.env.IMAP_SECURE_VIP_AI_MARKETING = "true";

  const firstPage = parse(await client.callTool({
    name: "agent_email_list_cleanup_candidates",
    arguments: {
      agent_id: "vip-ai-marketing",
      older_than_days: 14,
      limit: 1
    }
  }));
  assert.equal(firstPage.lifecycle_scope, "own_agent_inbox_only");
  assert.equal(firstPage.returned_count, 1);
  assert.equal(firstPage.messages[0].uid, "10");
  assert.equal(firstPage.messages[0].message_id_hash, messageIdHash10);
  assert.equal(firstPage.has_more, true);
  assert.equal(firstPage.next_cursor_uid, "10");

  const secondPage = parse(await client.callTool({
    name: "agent_email_list_cleanup_candidates",
    arguments: {
      agent_id: "vip-ai-marketing",
      older_than_days: 14,
      limit: 1,
      cursor_uid: firstPage.next_cursor_uid
    }
  }));
  assert.equal(secondPage.messages[0].uid, "11");

  const missingGate = await client.callTool({
    name: "agent_email_trash_processed",
    arguments: { ...lifecycleArguments, no_pending_action: false }
  });
  assert.equal(missingGate.isError, true);
  assert.equal(moveCount, 0);

  const shadow = parse(await client.callTool({
    name: "agent_email_trash_processed",
    arguments: lifecycleArguments
  }));
  assert.equal(shadow.result.status, "ready_for_trash");
  assert.equal(shadow.permanent_expunge, false);
  assert.equal(moveCount, 0);

  const live = parse(await client.callTool({
    name: "agent_email_trash_processed",
    arguments: { ...lifecycleArguments, dry_run: false }
  }));
  assert.equal(live.result.status, "trashed");
  assert.equal(live.result.in_inbox_after_move, false);
  assert.equal(live.result.found_in_trash_after_move, true);
  assert.equal(moveCount, 1);

  const repeated = parse(await client.callTool({
    name: "agent_email_trash_processed",
    arguments: { ...lifecycleArguments, dry_run: false }
  }));
  assert.equal(repeated.result.status, "source_not_found");
  assert.equal(moveCount, 1);

  console.log("email-lifecycle-selftest: ok");
} finally {
  await client.close().catch(() => {});
  for (const socket of sockets) socket.destroy();
  await new Promise((resolve) => fakeImap.close(resolve));
}

process.exit(0);
