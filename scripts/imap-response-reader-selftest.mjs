import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { getImapResponseLimits, imapResponseCommandLabel, readImapResponseUntil } from "../lib/imap-response-reader.js";

let assertions = 0;
function socket() {
  const value = new EventEmitter();
  value.destroyed = false;
  value.readableEncoding = "binary";
  value.destroy = () => { value.destroyed = true; };
  return value;
}
const options = { idleTimeoutMs: 70, totalTimeoutMs: 230, maxResponseBytes: 100 };
const match = (buffer) => buffer.endsWith("a1 OK\r\n") ? buffer : null;
function clean(value) {
  for (const event of ["data", "error", "end", "close"]) {
    assert.equal(value.listenerCount(event), 0);
    assertions += 1;
  }
}

assert.equal(imapResponseCommandLabel('LOGIN "fixture" "test-only"'), "LOGIN");
assert.equal(imapResponseCommandLabel("uid fetch 5 (BODY.PEEK[])"), "UID FETCH");
assert.equal(imapResponseCommandLabel("UID MOVE 5 INBOX.Trash"), "UID");
assertions += 3;
assert.deepEqual(getImapResponseLimits("UID FETCH"), {
  idleTimeoutMs: 30_000, totalTimeoutMs: 60_000, maxResponseBytes: 32 * 1024 * 1024
});
for (const label of ["greeting", "LOGIN", "UID", "SELECT", "LOGOUT"]) {
  assert.deepEqual(getImapResponseLimits(label), {
    idleTimeoutMs: 15_000, totalTimeoutMs: 15_000, maxResponseBytes: 32 * 1024 * 1024
  });
  assertions += 1;
}
assertions += 1;

const progressive = socket();
const progressRead = readImapResponseUntil(progressive, { buffer: "" }, match, "UID FETCH", options);
const chunks = ["* FETCH ", "literal ", "continues ", "a1 OK\r\n"];
const stream = setInterval(() => {
  progressive.emit("data", chunks.shift());
  if (!chunks.length) clearInterval(stream);
}, 40);
assert.match(await progressRead, /continues/);
assert.equal(progressive.destroyed, false);
assertions += 2;
clean(progressive);

const stalled = socket();
const stalledRead = readImapResponseUntil(stalled, { buffer: "" }, match, "UID FETCH", options);
stalled.emit("data", "partial");
await assert.rejects(stalledRead, { code: "IMAP_RESPONSE_IDLE_TIMEOUT" });
assert.equal(stalled.destroyed, true);
assertions += 2;
clean(stalled);

const trickle = socket();
const trickleRead = readImapResponseUntil(trickle, { buffer: "" }, match, "UID FETCH", options);
const trickleStream = setInterval(() => trickle.emit("data", "."), 30);
try {
  await assert.rejects(trickleRead, { code: "IMAP_RESPONSE_TOTAL_TIMEOUT" });
  assert.equal(trickle.destroyed, true);
  assertions += 2;
} finally { clearInterval(trickleStream); }
clean(trickle);

for (const event of ["close", "end"]) {
  const closed = socket();
  const closedRead = readImapResponseUntil(closed, { buffer: "" }, match, "UID FETCH", options);
  closed.emit(event);
  await assert.rejects(closedRead, { code: "IMAP_RESPONSE_CLOSED" });
  assertions += 1;
  clean(closed);
}

const tooLarge = socket();
const largeRead = readImapResponseUntil(tooLarge, { buffer: "" }, match, "UID FETCH", {
  ...options, maxResponseBytes: 5
});
tooLarge.emit("data", "123456");
await assert.rejects(largeRead, { code: "IMAP_RESPONSE_TOO_LARGE" });
assert.equal(tooLarge.destroyed, true);
assertions += 2;
clean(tooLarge);

const buffered = socket();
assert.equal(await readImapResponseUntil(buffered, { buffer: "a1 OK\r\n" }, match, "UID FETCH", options), "a1 OK\r\n");
assertions += 1;
clean(buffered);

const badMatcher = socket();
await assert.rejects(readImapResponseUntil(badMatcher, { buffer: "" }, () => {
  throw new Error("fixture matcher failure");
}, "UID FETCH", options), /fixture matcher failure/);
assert.equal(badMatcher.destroyed, true);
assertions += 2;
clean(badMatcher);

const disconnected = socket();
disconnected.destroyed = true;
await assert.rejects(readImapResponseUntil(disconnected, { buffer: "" }, match, "UID FETCH", options), { code: "IMAP_RESPONSE_CLOSED" });
assertions += 1;
clean(disconnected);

console.log(JSON.stringify({ imap_response_reader_selftest: "pass", assertions }));
