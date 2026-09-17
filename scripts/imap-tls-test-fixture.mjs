import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import tls from "node:tls";
import { promisify } from "node:util";

export async function createImapTlsTestServer(handler, { trustClient = true } = {}) {
  const directory = await mkdtemp(path.join(tmpdir(), "vip-imap-tls-test-"));
  try {
    const keyPath = path.join(directory, "test-only-key.pem");
    const certPath = path.join(directory, "test-only-cert.pem");
    await promisify(execFile)("openssl", [
      "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
      "-subj", "/CN=localhost", "-keyout", keyPath, "-out", certPath,
      "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"
    ], { timeout: 30_000 });
    const key = await readFile(keyPath);
    const certificate = await readFile(certPath);
    const server = tls.createServer({ key, cert: certificate }, handler);
    if (trustClient) {
      const originalConnect = tls.connect;
      // The fake IMAP client trusts only this ephemeral loopback certificate.
      tls.connect = (options, ...args) => {
        assert.equal(options.host, "127.0.0.1");
        assert.equal(options.rejectUnauthorized, true);
        return originalConnect({ ...options, ca: certificate }, ...args);
      };
    }
    return { server, certificate };
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
