import assert from "node:assert/strict";
import net from "node:net";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

function parse(result) {
  const value = result.content?.find((item) => item.type === "text")?.text || "{}";
  return JSON.parse(value);
}

function rawMessage(uid) {
  const subjects = {
    "4": "VORLAGE | ACTION=rs-contact-de | FROM=contact@reise-stories.de | LANG=de",
    "5": "VORLAGE | ACTION=rs-contact-en | FROM=contact@reise-stories.de | LANG=en",
    "23": "VORLAGE | ACTION=rs-signatur-de-en | FROM=contact@reise-stories.de",
    "117": "Oldest queued message",
    "118": "Second queued message",
    "119": "Oversized queued message"
  };
  return [
    `From: Sender ${uid} <sender${uid}@example.com>`,
    "To: contact@reise-stories.de",
    `Subject: ${subjects[String(uid)] || `Message ${uid}`}`,
    `Message-ID: <message-${uid}@example.com>`,
    "Date: Thu, 03 Sep 2026 10:00:00 +0200",
    "MIME-Version: 1.0",
    "Content-Type: text/plain; charset=utf-8",
    "",
    uid === "23" ? "TEXT\r\nReise-Stories" : `Body ${uid}`
  ].join("\r\n");
}

function headerBlock(raw) {
  return `${raw.split("\r\n\r\n", 1)[0]}\r\n\r\n`;
}

const imapCommands = [];
const fullBodyUids = [];
const imapSockets = new Set();
const fakeImap = net.createServer((socket) => {
  imapSockets.add(socket);
  socket.once("close", () => imapSockets.delete(socket));
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
          `* LIST (\\HasNoChildren) "." "INBOX.E-Mail-Automatisierung.RS | Contact"\r\n${tag} OK LIST completed\r\n`
        );
      } else if (/^EXAMINE\b/i.test(command)) {
        socket.write(`* 8 EXISTS\r\n${tag} OK EXAMINE completed\r\n`);
      } else if (/^UID SEARCH ALL$/i.test(command)) {
        socket.write(`* SEARCH 1 2 4 5 23 117 118 119\r\n${tag} OK SEARCH completed\r\n`);
      } else if (/^UID SEARCH HEADER Message-ID\b/i.test(command)) {
        socket.write(`* SEARCH\r\n${tag} OK SEARCH completed\r\n`);
      } else if (/^UID FETCH\b/i.test(command)) {
        const uid = /UID FETCH\s+(\d+)/i.exec(command)?.[1];
        const raw = rawMessage(uid);
        const flags = ["4", "5", "23"].includes(uid) ? "(\\Draft)" : "()";
        if (/RFC822\.SIZE/i.test(command)) {
          const headers = headerBlock(raw);
          const size = uid === "119" ? 5 * 1024 * 1024 : Buffer.byteLength(raw, "binary");
          socket.write(
            `* 1 FETCH (UID ${uid} FLAGS ${flags} RFC822.SIZE ${size} BODY[HEADER.FIELDS (SUBJECT FROM TO CC BCC REPLY-TO MESSAGE-ID IN-REPLY-TO REFERENCES DATE)] {${Buffer.byteLength(headers, "binary")}}\r\n${headers})\r\n${tag} OK FETCH completed\r\n`
          );
        } else {
          fullBodyUids.push(uid);
          socket.write(
            `* 1 FETCH (UID ${uid} FLAGS ${flags} BODY[] {${Buffer.byteLength(raw, "binary")}}\r\n${raw})\r\n${tag} OK FETCH completed\r\n`
          );
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

await new Promise((resolve) => fakeImap.listen(0, "127.0.0.1", resolve));
const imapPort = fakeImap.address().port;
const mcpPort = process.env.EMAIL_ACTION_PAGINATION_TEST_PORT || "3011";
process.env.PORT = mcpPort;
process.env.IMAP_HOST_VIP_AI_COMMUNICATION = "127.0.0.1";
process.env.IMAP_PORT_VIP_AI_COMMUNICATION = String(imapPort);
process.env.IMAP_SECURE_VIP_AI_COMMUNICATION = "false";
process.env.IMAP_USER_VIP_AI_COMMUNICATION = "communication-agent@vip-studios.de";
process.env.IMAP_PASSWORD_VIP_AI_COMMUNICATION = "test-only";

const client = new Client({ name: "vip-email-action-pagination-selftest", version: "1.0.0" });
const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${mcpPort}/mcp`));
await import("../server.js");
await new Promise((resolve) => setTimeout(resolve, 300));
await client.connect(transport);

try {
  const template = parse(await client.callTool({
    name: "email_action_template_readback",
    arguments: {
      agent_id: "vip-ai-communication",
      action_id: "rs-contact-de",
      max_scan_messages: 1
    }
  }));
  assert.equal(template.template_readback.uid, "4");
  assert.equal(template.folder_readback.total_uid_count, 8);
  assert.equal(template.folder_readback.scanned_uid_count, 3);
  assert.deepEqual([...new Set(fullBodyUids)].sort((a, b) => Number(a) - Number(b)), ["4", "5", "23"]);
  assert.equal(imapCommands.some((command) => /UID FETCH 1\b/.test(command)), false);

  fullBodyUids.splice(0, fullBodyUids.length);
  const discovery = parse(await client.callTool({
    name: "email_action_discover_folders",
    arguments: {
      agent_id: "vip-ai-communication",
      mailbox_root: "INBOX.E-Mail-Automatisierung",
      max_folders: 1,
      max_scan_messages_per_folder: 3
    }
  }));
  const folder = discovery.folders[0];
  assert.equal(folder.scanned_uid_count, 3);
  assert.equal(folder.queue_page.full_body_fetched_count, 2);
  assert.equal(folder.queue_page.oversized_skipped_count, 1);
  assert.deepEqual(fullBodyUids.sort((a, b) => Number(a) - Number(b)), ["117", "118"]);
  assert.equal(
    folder.unrecognized_messages.find((message) => message.uid === "119")?.parse_error,
    "message_too_large"
  );
  console.log("email-action-pagination-selftest: ok");
} finally {
  await client.close().catch(() => {});
  for (const socket of imapSockets) socket.destroy();
  await new Promise((resolve) => fakeImap.close(resolve));
}

process.exit(0);
