import assert from "node:assert/strict";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { createImapTlsTestServer } from "./imap-tls-test-fixture.mjs";

const mailboxAddress = "communication-agent@vip-studios.de";
const headers = {
  "132": [
    "From: Moritz <moritz@example.com>",
    "To: recipient@example.com",
    `Cc: ${mailboxAddress}`,
    "Subject: Large human candidate",
    "Message-ID: <large-132@example.com>",
    "Date: Wed, 21 Oct 2026 11:00:00 +0200"
  ],
  "133": [
    "From: Asana <notification@example.com>",
    `To: ${mailboxAddress}`,
    "Auto-Submitted: auto-generated",
    "Precedence: bulk",
    "Subject: Large automatic notice",
    "Message-ID: <large-133@example.com>"
  ],
  "134": [
    "From: Other <other@example.com>",
    "Cc: someone-else@example.com",
    "Subject: Small message",
    "Message-ID: <small-134@example.com>"
  ],
  "135": [
    "From: Unreadable <unreadable@example.com>"
  ],
  "136": [
    "From: Oversized <oversized@example.com>",
    `Cc: ${mailboxAddress}`,
    "Subject: Incorrect declared size",
    "Message-ID: <mismatch-136@example.com>"
  ]
};
const commands = [];
const sockets = new Set();
const { server: imapServer } = await createImapTlsTestServer((socket) => {
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
      commands.push(line);
      const match = /^(a\d+)\s+(.+)$/.exec(line);
      if (!match) continue;
      const [, tag, command] = match;
      if (/^LOGIN\b/i.test(command) || /^EXAMINE\b/i.test(command)) {
        socket.write(`${tag} OK completed\r\n`);
      } else if (/^UID SEARCH ALL$/i.test(command)) {
        socket.write(`* SEARCH 132 133 134 135 136\r\n${tag} OK SEARCH completed\r\n`);
      } else if (/^UID FETCH\b/i.test(command)) {
        const uid = /UID FETCH\s+(\d+)/i.exec(command)?.[1];
        const header = `${headers[uid].join("\r\n")}\r\n\r\n`;
        if (/RFC822\.SIZE/i.test(command)) {
          if (uid === "135") {
            socket.write(`* 1 FETCH (UID ${uid} RFC822.SIZE 999999)\r\n${tag} OK FETCH completed\r\n`);
            continue;
          }
          const size = ["134", "136"].includes(uid)
            ? Buffer.byteLength(`${header}Body 134`, "binary")
            : 5 * 1024 * 1024;
          socket.write(`* 1 FETCH (UID ${uid} RFC822.SIZE ${size} BODY[HEADER.FIELDS (FROM TO CC)] {${Buffer.byteLength(header, "binary")}}\r\n${header})\r\n${tag} OK FETCH completed\r\n`);
        } else {
          const raw = `${header}${uid === "136" ? "X".repeat(60_000) : "Body 134"}`;
          socket.write(`* 1 FETCH (UID ${uid} BODY[] {${Buffer.byteLength(raw, "binary")}}\r\n${raw})\r\n${tag} OK FETCH completed\r\n`);
        }
      } else if (/^LOGOUT$/i.test(command)) {
        socket.write(`* BYE logout\r\n${tag} OK LOGOUT completed\r\n`);
        socket.end();
      } else {
        socket.write(`${tag} BAD unsupported\r\n`);
      }
    }
  });
});

await new Promise((resolve) => imapServer.listen(0, "127.0.0.1", resolve));
const mcpPort = process.env.EMAIL_CC_LARGE_TEST_PORT || "3018";
process.env.PORT = mcpPort;
process.env.IMAP_HOST_VIP_AI_COMMUNICATION = "127.0.0.1";
process.env.IMAP_PORT_VIP_AI_COMMUNICATION = String(imapServer.address().port);
process.env.IMAP_SECURE_VIP_AI_COMMUNICATION = "true";
process.env.IMAP_USER_VIP_AI_COMMUNICATION = mailboxAddress;
process.env.IMAP_PASSWORD_VIP_AI_COMMUNICATION = "test-only";

const client = new Client({ name: "vip-email-cc-large-selftest", version: "1.0.0" });
const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${mcpPort}/mcp`));
await import("../server.js");
await new Promise((resolve) => setTimeout(resolve, 300));
try {
  await client.connect(transport);
  const response = await client.callTool({
    name: "agent_email_read_bcc_learning_batch",
    arguments: { agent_id: "vip-ai-communication", after_uid: "131", limit: 5, max_email_bytes: 50_000 }
  });
  const result = JSON.parse(response.content.find((item) => item.type === "text")?.text || "{}");
  assert.equal(result.returned_count, 5);
  assert.equal(result.next_after_uid, "136");
  assert.equal(result.safety.read_only, true);
  assert.deepEqual(result.messages.map((item) => item.uid), ["132", "133", "134", "135", "136"]);
  assert.equal(result.messages[0].mailbox_visible_in_cc, true);
  assert.equal(result.messages[0].sender_class, "undetermined");
  assert.equal(result.messages[0].content_scope, "headers_only");
  assert.equal(result.messages[0].body_unavailable_reason, "message_too_large");
  assert.equal(result.messages[0].body_text, null);
  assert.equal(result.messages[1].mailbox_visible_in_cc, false);
  assert.equal(result.messages[1].sender_class, "automated");
  assert.equal(result.messages[1].content_scope, "headers_only");
  assert.equal(result.messages[2].content_scope, "body");
  assert.equal(result.messages[2].body_text.includes("Body 134"), true);
  assert.deepEqual(result.messages[3], { uid: "135", parse_error: "message_headers_unavailable" });
  assert.equal(result.messages[4].mailbox_visible_in_cc, true);
  assert.equal(result.messages[4].content_scope, "headers_only");
  assert.equal(result.messages[4].body_text, null);
  assert.deepEqual(
    commands.filter((command) => /UID FETCH .*BODY\.PEEK\[\]/i.test(command)).map((command) => /UID FETCH\s+(\d+)/i.exec(command)?.[1]),
    ["134", "136"]
  );
  assert.equal(commands.some((command) => /\bSTORE\b|\bMOVE\b|\bEXPUNGE\b/i.test(command)), false);
  console.log("email-cc-learning-large-selftest: ok");
} finally {
  await client.close().catch(() => {});
  for (const socket of sockets) socket.destroy();
  await new Promise((resolve) => imapServer.close(resolve));
}
process.exit(0);
