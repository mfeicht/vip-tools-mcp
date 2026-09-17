export const IMAP_RESPONSE_POLICY_VERSION = "bounded-idle-response-v1";

export function getImapResponseLimits(label, timeoutMs = 15_000) {
  return {
    idleTimeoutMs: label === "UID FETCH" ? Math.max(timeoutMs, 30_000) : timeoutMs,
    totalTimeoutMs: label === "UID FETCH" ? Math.max(timeoutMs, 60_000) : timeoutMs,
    maxResponseBytes: 32 * 1024 * 1024
  };
}

export function imapResponseCommandLabel(payload) {
  const [command = "", subcommand = ""] = String(payload).trim().split(/\s+/, 2);
  return command.toUpperCase() === "UID" && subcommand.toUpperCase() === "FETCH"
    ? "UID FETCH"
    : command.toUpperCase();
}

export function readImapResponseUntil(socket, state, matcher, label, {
  idleTimeoutMs = 15_000,
  totalTimeoutMs = idleTimeoutMs,
  maxResponseBytes = 32 * 1024 * 1024
} = {}) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let idleTimer;
    let totalTimer;
    const encoding = ["binary", "latin1"].includes(socket.readableEncoding) ? "binary" : "utf8";
    let receivedBytes = Buffer.byteLength(state.buffer, encoding);
    const cleanup = () => {
      clearTimeout(idleTimer);
      clearTimeout(totalTimer);
      socket.off("data", onData);
      socket.off("error", onError);
      socket.off("end", onEnd);
      socket.off("close", onClose);
    };
    const finish = (error, value) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (error) {
        // A partial response must never be reused by a later command.
        if (!socket.destroyed) socket.destroy();
        reject(error);
      } else resolve(value);
    };
    const fail = (code, detail) => {
      const error = new Error(`IMAP ${detail} (${label}); received_bytes=${receivedBytes}`);
      error.code = code;
      finish(error);
    };
    const check = () => {
      if (receivedBytes > maxResponseBytes) {
        fail("IMAP_RESPONSE_TOO_LARGE", `response exceeds ${maxResponseBytes} bytes`);
        return;
      }
      try {
        const match = matcher(state.buffer);
        if (match) finish(null, match);
      } catch (error) {
        finish(error);
      }
    };
    const armIdleTimer = () => {
      clearTimeout(idleTimer);
      idleTimer = setTimeout(() => {
        fail("IMAP_RESPONSE_IDLE_TIMEOUT", `response idle timeout after ${idleTimeoutMs}ms`);
      }, idleTimeoutMs);
    };
    const onData = (chunk) => {
      receivedBytes += Buffer.isBuffer(chunk) ? chunk.length : Buffer.byteLength(chunk, encoding);
      state.buffer += chunk;
      check();
      if (!settled && chunk.length) armIdleTimer();
    };
    const onError = (error) => finish(error);
    const onEnd = () => fail("IMAP_RESPONSE_CLOSED", "connection ended before complete response");
    const onClose = () => fail("IMAP_RESPONSE_CLOSED", "connection closed before complete response");
    socket.on("data", onData);
    socket.once("error", onError);
    socket.once("end", onEnd);
    socket.once("close", onClose);
    totalTimer = setTimeout(() => {
      fail("IMAP_RESPONSE_TOTAL_TIMEOUT", `response total timeout after ${totalTimeoutMs}ms`);
    }, totalTimeoutMs);
    armIdleTimer();
    if (socket.destroyed || socket.readableEnded) onClose();
    else check();
  });
}
