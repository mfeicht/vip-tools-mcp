import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

const CODEX = process.env.VIP_DASHBOARD_CODEX_BIN || "/Applications/ChatGPT.app/Contents/Resources/codex";

export class CodexAppRpc {
  constructor() {
    this.nextId = 1;
    this.pending = new Map();
    this.stderr = "";
  }

  async start() {
    this.child = spawn(CODEX, ["app-server", "--listen", "stdio://"],
      { stdio: ["pipe", "pipe", "pipe"] });
    this.child.stderr.setEncoding("utf8");
    this.child.stderr.on("data", (chunk) => { this.stderr = `${this.stderr}${chunk}`.slice(-2000); });
    createInterface({ input: this.child.stdout }).on("line", (line) => {
      let message;
      try { message = JSON.parse(line); } catch { return; }
      if (message.id == null) return;
      const pending = this.pending.get(String(message.id));
      if (!pending) return;
      clearTimeout(pending.timeout);
      this.pending.delete(String(message.id));
      if (message.error) pending.reject(new Error(message.error.message || JSON.stringify(message.error)));
      else pending.resolve(message.result);
    });
    this.child.once("exit", (code) => {
      for (const pending of this.pending.values()) {
        clearTimeout(pending.timeout);
        pending.reject(new Error(`Codex app-server exited ${code}: ${this.stderr}`));
      }
      this.pending.clear();
    });
    await this.call("initialize", {
      clientInfo: { name: "vip-asana-dispatch", version: "1.0.0" },
      capabilities: { experimentalApi: true }
    });
  }

  call(method, params = {}, timeoutMs = 60_000) {
    if (!this.child?.stdin?.writable) throw new Error("Codex app-server unavailable");
    const id = String(this.nextId++);
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`${method} timed out`));
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timeout });
      this.child.stdin.write(`${JSON.stringify({ id, method, params })}\n`);
    });
  }

  async close() {
    if (!this.child) return;
    this.child.stdin.end();
    const child = this.child;
    await Promise.race([
      new Promise((resolve) => child.once("exit", resolve)),
      new Promise((resolve) => setTimeout(() => { child.kill("SIGTERM"); resolve(); }, 2_000))
    ]);
  }
}

export async function archiveCompletedThread(threadId) {
  if (!threadId) return false;
  const rpc = new CodexAppRpc();
  try {
    await rpc.start();
    await rpc.call("thread/archive", { threadId });
    return true;
  } finally {
    await rpc.close();
  }
}
