import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import tls from "node:tls";
import {
  assertImapTlsConfig,
  buildImapTlsCandidates,
  openImapTlsSocket
} from "../lib/imap-transport-security.js";
import { createImapTlsTestServer } from "./imap-tls-test-fixture.mjs";

let assertions = 0;
const config = { host: "vip-studios.vip-studios.de", port: 993, secure: true };
assert.deepEqual(buildImapTlsCandidates(config), [{ ...config, label: "primary" }]);
assertions += 1;
for (const secure of [false, undefined, "true", 1]) {
  const insecure = { ...config, port: 143, secure };
  assert.throws(() => assertImapTlsConfig(insecure), { code: "IMAP_TLS_REQUIRED" });
  assert.deepEqual(buildImapTlsCandidates(insecure, { enforce: false }), []);
  let networkCalls = 0;
  await assert.rejects(openImapTlsSocket(insecure, {
    connect: () => { networkCalls += 1; }
  }), { code: "IMAP_TLS_REQUIRED" });
  assert.equal(networkCalls, 0);
  assertions += 4;
}

function mockSocket({ encrypted = true, authorized = true, event = "secureConnect" } = {}) {
  const socket = new EventEmitter();
  Object.assign(socket, { encrypted, authorized, destroyed: false });
  socket.destroy = () => { socket.destroyed = true; };
  queueMicrotask(() => { if (event) socket.emit(event); });
  return socket;
}

process.env.IMAP_TLS_ALLOW_UNAUTHORIZED = "true";
const verified = await openImapTlsSocket(config, { connect: (options) => {
  assert.equal(options.rejectUnauthorized, true);
  assert.equal(options.minVersion, "TLSv1.2");
  assertions += 2;
  return mockSocket();
} });
assert.equal(verified.authorized, true);
assertions += 1;
for (const values of [{ encrypted: false }, { authorized: false }]) {
  const socket = mockSocket(values);
  await assert.rejects(openImapTlsSocket(config, { connect: () => socket }), { code: "IMAP_TLS_UNVERIFIED" });
  assert.equal(socket.destroyed, true);
  assertions += 2;
}
const connectedOnly = mockSocket({ event: "connect" });
await assert.rejects(openImapTlsSocket(config, {
  timeoutMs: 5, connect: () => connectedOnly
}), /connect timeout/);
assert.equal(connectedOnly.destroyed, true);
assertions += 2;
await assert.rejects(openImapTlsSocket(config, {
  connect: () => { throw new Error("fixture connection failure"); }
}), /fixture connection failure/);
assertions += 1;

let loginCount = 0;
const sockets = new Set();
const { server, certificate } = await createImapTlsTestServer((socket) => {
  sockets.add(socket);
  socket.once("close", () => sockets.delete(socket));
  socket.setEncoding("utf8");
  socket.write("* OK TLS fixture ready\r\n");
  socket.on("data", (data) => {
    if (data.includes(" LOGIN ")) {
      assert.equal(socket.encrypted, true);
      loginCount += 1;
      socket.write("a1 OK LOGIN completed\r\n");
    }
  });
}, { trustClient: false });
server.on("tlsClientError", () => {});
await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const localConfig = { host: "127.0.0.1", port: server.address().port, secure: true };
try {
  await assert.rejects(openImapTlsSocket(localConfig, { timeoutMs: 1000 }), /self.signed|certificate/i);
  assert.equal(loginCount, 0);
  assertions += 2;
  const socket = await openImapTlsSocket(localConfig, {
    timeoutMs: 1000,
    connect: (options) => tls.connect({ ...options, ca: certificate })
  });
  assert.equal(socket.encrypted && socket.authorized, true);
  const response = new Promise((resolve) => {
    socket.on("data", (data) => { if (data.toString().includes("a1 OK LOGIN")) resolve(); });
  });
  socket.write('a1 LOGIN "fixture-user" "test-only"\r\n');
  await response;
  assert.equal(loginCount, 1);
  assertions += 2;
  socket.destroy();
} finally {
  for (const socket of sockets) socket.destroy();
  await new Promise((resolve) => server.close(resolve));
}
console.log(JSON.stringify({ imap_tls_security_selftest: "pass", assertions }));
