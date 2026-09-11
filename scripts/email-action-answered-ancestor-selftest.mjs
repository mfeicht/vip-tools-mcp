import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import net from "node:net";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

function parse(result) {
  const value = result.content?.find((item) => item.type === "text")?.text || "{}";
  return JSON.parse(value);
}

const sourceMailbox = "INBOX.E-Mail-Automatisierung.RS | Contact";
const targetMailbox = "INBOX.Trash";
const providerId = "6e4ebd68-089d-4fcf-9e73-2cf367045589";
const providerFlag = "$VR-bk69aAidT8-ecyzzZwRViQ";
const sender = "marie.lueck@berkeleypr.com";
const ancestorMessageId = "<ancestor-89@berkeleypr.com>";
const childMessageId = "<answered-child-90@berkeleypr.com>";
const ancestorMessageIdHash = createHash("sha256").update(ancestorMessageId).digest("hex");

function headerMessage({ from, subject, messageId, inReplyTo = "", references = "" }) {
  return [
    `From: Marie Lueck <${from}>`,
    "To: contact@reise-stories.de",
    `Subject: ${subject}`,
    `Message-ID: ${messageId}`,
    ...(inReplyTo ? [`In-Reply-To: ${inReplyTo}`] : []),
    ...(references ? [`References: ${references}`] : []),
    "Date: Wed, 15 Jul 2026 14:41:52 +0000",
    "",
    ""
  ].join("\r\n");
}

const ancestorHeader = headerMessage({
  from: sender,
  subject: "Beitragsvorschlag zur Reisesaison",
  messageId: ancestorMessageId
});
let childHeader = headerMessage({
  from: sender,
  subject: "Re: Beitragsvorschlag zur Reisesaison",
  messageId: childMessageId,
  inReplyTo: ancestorMessageId,
  references: ancestorMessageId
});

let sourcePresent = true;
let selectedMailbox = "";
let moveCount = 0;
const imapCommands = [];
const sockets = new Set();

function writeHeaderFetch(socket, tag, { uid, header, flags = [], size }) {
  socket.write(
    `* 1 FETCH (UID ${uid} FLAGS (${flags.join(" ")}) RFC822.SIZE ${size} BODY[HEADER.FIELDS (SUBJECT FROM TO CC BCC REPLY-TO MESSAGE-ID IN-REPLY-TO REFERENCES DATE)] {${Buffer.byteLength(header, "binary")}}\r\n${header})\r\n${tag} OK FETCH completed\r\n`
  );
}

const fakeImap = net.createServer((socket) => {
  sockets.add(socket);
  socket.once("close", () => sockets.delete(socket));
  socket.setEncoding("utf8");
  socket.write("* OK fake-imap ready\r\n");
  let buffered = "";
  socket.on("data", (chunk) => {
    buffered += chunk;
    while (buffered.includes("\r\n")) {
      const end = buffered.indexOf("\r\n");
      const line = buffered.slice(0, end);
      buffered = buffered.slice(end + 2);
      if (!line) continue;
      imapCommands.push(line);
      const match = /^(a\d+)\s+(.+)$/.exec(line);
      if (!match) continue;
      const [, tag, command] = match;
      if (/^LOGIN\b/i.test(command)) {
        socket.write(`${tag} OK LOGIN completed\r\n`);
      } else if (/^LIST\b/i.test(command)) {
        socket.write(
          `* LIST (\\HasNoChildren) "." "${sourceMailbox}"\r\n* LIST (\\HasNoChildren) "." "${targetMailbox}"\r\n${tag} OK LIST completed\r\n`
        );
      } else if (/^(?:EXAMINE|SELECT)\b/i.test(command)) {
        selectedMailbox = /"([^"]+)"\s*$/.exec(command)?.[1] || "";
        socket.write(`* 2 EXISTS\r\n${tag} OK mailbox selected\r\n`);
      } else if (/^UID SEARCH KEYWORD\b/i.test(command)) {
        const found = selectedMailbox === targetMailbox && command.includes(providerFlag) ? "90" : "";
        socket.write(`* SEARCH ${found}\r\n${tag} OK SEARCH completed\r\n`);
      } else if (/^UID SEARCH HEADER Message-ID\b/i.test(command)) {
        const found =
          selectedMailbox === targetMailbox && !sourcePresent && command.includes(ancestorMessageId)
            ? "91"
            : "";
        socket.write(`* SEARCH ${found}\r\n${tag} OK SEARCH completed\r\n`);
      } else if (/^UID FETCH\b/i.test(command)) {
        assert.equal(/BODY\.PEEK\[\]/i.test(command), false, "full message bodies must never be fetched");
        const uid = /UID FETCH\s+(\d+)/i.exec(command)?.[1];
        if (selectedMailbox === sourceMailbox && uid === "89" && sourcePresent) {
          writeHeaderFetch(socket, tag, {
            uid,
            header: ancestorHeader,
            flags: ["\\Flagged", "\\Seen"],
            size: 30 * 1024 * 1024
          });
        } else if (selectedMailbox === targetMailbox && uid === "90") {
          writeHeaderFetch(socket, tag, {
            uid,
            header: childHeader,
            flags: ["\\Seen", providerFlag, "$VIPAI-SENT-TEST"],
            size: 24_000
          });
        } else if (selectedMailbox === targetMailbox && uid === "91" && !sourcePresent) {
          writeHeaderFetch(socket, tag, {
            uid,
            header: ancestorHeader,
            flags: ["\\Flagged", "\\Seen"],
            size: 30 * 1024 * 1024
          });
        } else {
          socket.write(`${tag} OK FETCH completed\r\n`);
        }
      } else if (/^UID MOVE\s+89\b/i.test(command)) {
        assert.equal(selectedMailbox, sourceMailbox);
        sourcePresent = false;
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
const mcpPort = process.env.EMAIL_ACTION_ANSWERED_ANCESTOR_TEST_PORT || "3012";
process.env.PORT = mcpPort;
process.env.IMAP_HOST_VIP_AI_COMMUNICATION = "127.0.0.1";
process.env.IMAP_PORT_VIP_AI_COMMUNICATION = String(imapPort);
process.env.IMAP_SECURE_VIP_AI_COMMUNICATION = "false";
process.env.IMAP_USER_VIP_AI_COMMUNICATION = "communication-agent@vip-studios.de";
process.env.IMAP_PASSWORD_VIP_AI_COMMUNICATION = "test-only";

const client = new Client({ name: "vip-email-action-answered-ancestor-selftest", version: "1.0.0" });
const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${mcpPort}/mcp`));
await import("../server.js");
await new Promise((resolve) => setTimeout(resolve, 300));
await client.connect(transport);

const baseArguments = {
  agent_id: "vip-ai-communication",
  action_id: "rs-contact-de",
  ancestor_uid: "89",
  expected_ancestor_from: sender,
  expected_ancestor_message_id: ancestorMessageId,
  expected_ancestor_message_id_hash: ancestorMessageIdHash,
  answered_child_resend_provider_id: providerId,
  expected_done_mailbox: targetMailbox
};

try {
  const tools = await client.listTools();
  assert.equal(
    tools.tools.some((tool) => tool.name === "email_action_cleanup_answered_thread_ancestor"),
    true
  );

  const wrongProvider = await client.callTool({
    name: "email_action_cleanup_answered_thread_ancestor",
    arguments: {
      ...baseArguments,
      answered_child_resend_provider_id: "11111111-2222-3333-4444-555555555555",
      mode: "shadow_run"
    }
  });
  assert.equal(wrongProvider.isError, true);
  assert.match(
    wrongProvider.content?.find((item) => item.type === "text")?.text || "",
    /Resend-Provider-Marker/
  );
  assert.equal(moveCount, 0);

  const wrongTarget = await client.callTool({
    name: "email_action_cleanup_answered_thread_ancestor",
    arguments: { ...baseArguments, expected_done_mailbox: "INBOX.Spam", mode: "shadow_run" }
  });
  assert.equal(wrongTarget.isError, true);
  assert.match(
    wrongTarget.content?.find((item) => item.type === "text")?.text || "",
    /Erwarteter Zielordner/
  );
  assert.equal(moveCount, 0);

  childHeader = headerMessage({
    from: sender,
    subject: "Re: Beitragsvorschlag zur Reisesaison",
    messageId: childMessageId,
    inReplyTo: "<unrelated@example.com>",
    references: "<unrelated@example.com>"
  });
  const wrongThread = await client.callTool({
    name: "email_action_cleanup_answered_thread_ancestor",
    arguments: { ...baseArguments, mode: "shadow_run" }
  });
  assert.equal(wrongThread.isError, true);
  assert.match(
    wrongThread.content?.find((item) => item.type === "text")?.text || "",
    /References\/In-Reply-To/
  );
  assert.equal(moveCount, 0);
  childHeader = headerMessage({
    from: sender,
    subject: "Re: Beitragsvorschlag zur Reisesaison",
    messageId: childMessageId,
    inReplyTo: ancestorMessageId,
    references: ancestorMessageId
  });

  const bootstrapShadow = parse(await client.callTool({
    name: "email_action_cleanup_answered_thread_ancestor",
    arguments: {
      ...baseArguments,
      expected_ancestor_message_id: undefined,
      mode: "shadow_run"
    }
  }));
  assert.equal(bootstrapShadow.status, "ready_for_cleanup");
  assert.equal(bootstrapShadow.ancestor.message_id, ancestorMessageId);
  assert.equal(moveCount, 0);

  const shadow = parse(await client.callTool({
    name: "email_action_cleanup_answered_thread_ancestor",
    arguments: { ...baseArguments, mode: "shadow_run" }
  }));
  assert.equal(shadow.status, "ready_for_cleanup");
  assert.equal(shadow.dry_run, true);
  assert.equal(shadow.ancestor.uid, "89");
  assert.equal(shadow.ancestor.raw_bytes, 30 * 1024 * 1024);
  assert.equal(shadow.answered_child.uid, "90");
  assert.equal(shadow.provider_marker_validated, true);
  assert.equal(shadow.thread_reference_validated, true);
  assert.equal(shadow.full_body_fetched, false);
  assert.equal(shadow.sent, false);
  assert.equal(moveCount, 0);

  const live = parse(await client.callTool({
    name: "email_action_cleanup_answered_thread_ancestor",
    arguments: {
      ...baseArguments,
      mode: "live",
      confirm_cleanup: true,
      authorization: {
        source: "direct_codex",
        direct_authorized_by: "Moritz Feichtmeyer",
        direct_instruction: "Lokaler Selftest darf den Fake-IMAP-Thread-Vorfahr verschieben."
      }
    }
  }));
  assert.equal(live.status, "moved_and_verified");
  assert.equal(live.moved_now, true);
  assert.equal(live.in_source_after_move, false);
  assert.equal(live.found_in_target_after_move, true);
  assert.equal(live.safety.sends_live_email, false);
  assert.equal(moveCount, 1);

  const repeated = parse(await client.callTool({
    name: "email_action_cleanup_answered_thread_ancestor",
    arguments: {
      ...baseArguments,
      mode: "live",
      confirm_cleanup: true,
      authorization: {
        source: "direct_codex",
        direct_authorized_by: "Moritz Feichtmeyer",
        direct_instruction: "Idempotenter Selftest darf denselben Fake-Move erneut pruefen."
      }
    }
  }));
  assert.equal(repeated.status, "already_moved_and_verified");
  assert.equal(repeated.moved_now, false);
  assert.equal(moveCount, 1);
  assert.equal(imapCommands.some((command) => /^UID STORE\b/i.test(command)), false);
  console.log("email-action-answered-ancestor-selftest: ok");
} finally {
  await client.close().catch(() => {});
  for (const socket of sockets) socket.destroy();
  await new Promise((resolve) => fakeImap.close(resolve));
}

process.exit(0);
