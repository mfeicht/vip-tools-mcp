import tls from "node:tls";
import { isIP } from "node:net";

export const IMAP_TRANSPORT_POLICY_VERSION = "implicit-tls-required-v1";

export function assertImapTlsConfig(config) {
  if (config?.secure !== true) {
    const error = new Error("IMAP_TLS_REQUIRED: Unverschluesseltes IMAP ist gesperrt. IMAP_SECURE muss true sein; Standard ist Port 993 mit TLS. Kein Plaintext- oder ungetesteter STARTTLS-Fallback.");
    error.code = "IMAP_TLS_REQUIRED";
    throw error;
  }
}

export function buildImapTlsCandidates(config, { enforce = true } = {}) {
  if (enforce) assertImapTlsConfig(config);
  return config.secure === true ? [{ ...config, label: "primary" }] : [];
}

export async function openImapTlsSocket(config, { timeoutMs = 15_000, connect = tls.connect } = {}) {
  assertImapTlsConfig(config);
  return new Promise((resolve, reject) => {
    let socket;
    let settled = false;
    const done = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (error) {
        if (socket && !socket.destroyed) socket.destroy();
        reject(error);
      } else {
        resolve(socket);
      }
    };
    const timer = setTimeout(() => {
      done(new Error(`IMAP connect timeout after ${timeoutMs}ms (${config.host}:${config.port})`));
    }, timeoutMs);

    try {
      socket = connect({
        host: config.host,
        port: config.port,
        servername: isIP(config.host) ? undefined : config.host,
        rejectUnauthorized: true,
        minVersion: "TLSv1.2"
      });
      socket.once("secureConnect", () => {
        if (!socket.encrypted || !socket.authorized) {
          const error = new Error("IMAP_TLS_UNVERIFIED: Anmeldung ohne verifizierte TLS-Verbindung ist gesperrt.");
          error.code = "IMAP_TLS_UNVERIFIED";
          done(error);
          return;
        }
        done();
      });
      socket.once("error", (error) => done(error));
      socket.once("close", () => done(new Error("IMAP TLS connection closed before verified handshake")));
    } catch (error) {
      done(error);
    }
  });
}
