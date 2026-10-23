#!/usr/bin/env node

import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { createCorrelatedMcpFetch } from "../lib/mcp-request-correlation.js";

const MCP_URL = "https://vip-tools-mcp.onrender.com/mcp";
const DEFAULT_ATTEMPTS = 3;
const DEFAULT_DELAY_MS = 15000;
const DEFAULT_CONNECT_TIMEOUT_MS = 25000;
const DEFAULT_TOOL_TIMEOUT_MS = 120000;
const MAX_RETRY_DELAY_MS = 60000;
const RATE_LIMIT_COOLDOWN_MS = 60000;
const CONNECT_LOCK_WAIT_MS = 3 * 60 * 1000;
const CONNECT_LOCK_STALE_MS = 4 * 60 * 1000;
const MIN_CONNECT_SPACING_MS = 1500;
const RETRY_JITTER_MS = 2500;
const MAX_ERROR_OUTPUT_CHARS = 600;
const MAX_ARGS_BYTES = 2 * 1024 * 1024;
const COORDINATION_DIR =
  process.env.VIP_MCP_COORDINATION_DIR ||
  path.join(os.homedir(), "Library", "Application Support", "VIP-Studios", "mcp-transport");
const CONNECT_LOCK_PATH = path.join(COORDINATION_DIR, "connect.lock");
const TRANSPORT_STATE_PATH = path.join(COORDINATION_DIR, "state.json");

function usage() {
  console.error(`Canonical VIP Remote-MCP fallback

Usage:
  node scripts/vip-mcp-call.mjs --tool asana_list_agents --args-json '{}'
  node scripts/vip-mcp-call.mjs --tool asana_whoami --args-file /absolute/path/args.json
  node scripts/vip-mcp-call.mjs --list-tools
  node scripts/vip-mcp-call.mjs --self-test

This script always uses ${MCP_URL}. It does not accept endpoint overrides.`);
}

function parsePositiveInt(value, label) {
  const parsed = Number(value);
  if (!/^[0-9]+$/.test(String(value)) || !Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${label} muss eine positive Ganzzahl sein.`);
  }
  return parsed;
}

export function parseArgs(argv) {
  const opts = {
    tool: null,
    argsJson: null,
    argsFile: null,
    listTools: false,
    selfTest: false,
    attempts: DEFAULT_ATTEMPTS,
    delayMs: DEFAULT_DELAY_MS,
    connectTimeoutMs: DEFAULT_CONNECT_TIMEOUT_MS,
    toolTimeoutMs: DEFAULT_TOOL_TIMEOUT_MS
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = () => {
      index += 1;
      if (index >= argv.length) throw new Error(`Wert fuer ${arg} fehlt.`);
      return argv[index];
    };

    if (arg === "--tool") opts.tool = next();
    else if (arg.startsWith("--tool=")) opts.tool = arg.slice("--tool=".length);
    else if (arg === "--args-json") opts.argsJson = next();
    else if (arg.startsWith("--args-json=")) opts.argsJson = arg.slice("--args-json=".length);
    else if (arg === "--args-file") opts.argsFile = next();
    else if (arg.startsWith("--args-file=")) opts.argsFile = arg.slice("--args-file=".length);
    else if (arg === "--attempts") opts.attempts = parsePositiveInt(next(), "attempts");
    else if (arg.startsWith("--attempts=")) {
      opts.attempts = parsePositiveInt(arg.slice("--attempts=".length), "attempts");
    } else if (arg === "--delay-ms") opts.delayMs = parsePositiveInt(next(), "delay-ms");
    else if (arg.startsWith("--delay-ms=")) {
      opts.delayMs = parsePositiveInt(arg.slice("--delay-ms=".length), "delay-ms");
    } else if (arg === "--list-tools") opts.listTools = true;
    else if (arg === "--self-test") opts.selfTest = true;
    else if (arg === "--help" || arg === "-h") {
      usage();
      process.exit(0);
    } else {
      throw new Error(`Unbekanntes Argument: ${arg}`);
    }
  }

  if (!opts.listTools && !opts.selfTest && !opts.tool) {
    throw new Error("--tool, --list-tools oder --self-test ist erforderlich.");
  }
  if (opts.argsJson !== null && opts.argsFile !== null) {
    throw new Error("Nur eines von --args-json und --args-file verwenden.");
  }
  if (opts.tool && !/^[a-z][a-z0-9_]*$/i.test(opts.tool)) {
    throw new Error("Ungueltiger MCP-Toolname.");
  }
  if (opts.attempts > DEFAULT_ATTEMPTS) throw new Error("attempts darf höchstens 3 sein.");
  if (opts.delayMs > MAX_RETRY_DELAY_MS) throw new Error("delay-ms darf höchstens 60000 sein.");
  return opts;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function processAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 1) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error?.code !== "ESRCH";
  }
}

export function connectLockReclaimable(lock, ageMs, alive = processAlive) {
  // A live holder may be sleeping through a shared cooldown. Age is not proof
  // that its connect transaction ended. Give a newly created empty file time.
  if (Number.isInteger(lock?.pid) && lock.pid > 1) return !alive(lock.pid);
  return Number.isFinite(ageMs) && ageMs > CONNECT_LOCK_STALE_MS;
}

async function readJson(pathname, fallback = {}) {
  try {
    return JSON.parse(await fs.readFile(pathname, "utf8"));
  } catch {
    return fallback;
  }
}

async function writeJsonAtomic(pathname, value) {
  await fs.mkdir(path.dirname(pathname), { recursive: true, mode: 0o700 });
  const temporary = `${pathname}.${process.pid}.${Math.random().toString(16).slice(2)}.tmp`;
  await fs.writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, {
    encoding: "utf8",
    mode: 0o600
  });
  await fs.rename(temporary, pathname);
}

async function acquireConnectLock() {
  await fs.mkdir(COORDINATION_DIR, { recursive: true, mode: 0o700 });
  const token = `${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const deadline = Date.now() + CONNECT_LOCK_WAIT_MS;

  while (Date.now() < deadline) {
    try {
      const handle = await fs.open(CONNECT_LOCK_PATH, "wx", 0o600);
      await handle.writeFile(
        `${JSON.stringify({ pid: process.pid, token, acquiredAt: new Date().toISOString() })}\n`,
        "utf8"
      );
      await handle.close();
      return { token, waitedMs: CONNECT_LOCK_WAIT_MS - Math.max(0, deadline - Date.now()) };
    } catch (error) {
      if (error?.code !== "EEXIST") throw error;
      const lock = await readJson(CONNECT_LOCK_PATH, {});
      const stat = await fs.stat(CONNECT_LOCK_PATH).catch(() => null);
      const stale = connectLockReclaimable(lock, stat ? Date.now() - stat.mtimeMs : 0);
      if (stale) {
        await fs.unlink(CONNECT_LOCK_PATH).catch(() => {});
        continue;
      }
      await sleep(400 + Math.floor(Math.random() * 600));
    }
  }
  const error = new Error("MCP coordination timeout while waiting for the shared connect slot");
  error.code = "VIP_MCP_COORDINATION_TIMEOUT";
  throw error;
}

async function releaseConnectLock(lock) {
  if (!lock) return;
  const current = await readJson(CONNECT_LOCK_PATH, {});
  if (current.token === lock.token) await fs.unlink(CONNECT_LOCK_PATH).catch(() => {});
}

async function waitForSharedTransportWindow() {
  const state = await readJson(TRANSPORT_STATE_PATH, {});
  const now = Date.now();
  const cooldownUntil = Date.parse(state.cooldownUntil || "");
  if (Number.isFinite(cooldownUntil) && cooldownUntil > now) {
    const waitMs = cooldownUntil - now + Math.floor(Math.random() * RETRY_JITTER_MS);
    console.error(`[vip-mcp] shared cooldown active; waiting ${waitMs} ms`);
    await sleep(waitMs);
  }
  const lastAttemptAt = Date.parse(state.lastConnectAttemptAt || "");
  if (Number.isFinite(lastAttemptAt)) {
    const spacing = MIN_CONNECT_SPACING_MS - (Date.now() - lastAttemptAt);
    if (spacing > 0) await sleep(spacing + Math.floor(Math.random() * 500));
  }
}

async function recordTransportState(update) {
  const previous = await readJson(TRANSPORT_STATE_PATH, {});
  await writeJsonAtomic(TRANSPORT_STATE_PATH, {
    ...previous,
    version: 1,
    updatedAt: new Date().toISOString(),
    ...update
  });
}

function withTimeout(promise, timeoutMs, label) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timeout after ${timeoutMs} ms`)), timeoutMs);
    })
  ]).finally(() => clearTimeout(timer));
}

function errorText(error) {
  return String(error?.message || error || "unknown error");
}

function httpStatus(error) {
  // StreamableHTTPError keeps status in .code, not necessarily in its message.
  const value = error?.status ?? error?.response?.status ?? error?.code;
  const code = Number(value);
  return Number.isInteger(code) && code >= 100 && code <= 599 ? code : null;
}

function safeErrorText(error) {
  const message = errorText(error);
  if (/Just a moment|Cloudflare managed challenge|challenges\.cloudflare\.com|cf_chl_|Enable JavaScript and cookies/i.test(message)) {
    return "Cloudflare managed challenge on the canonical MCP endpoint";
  }
  const status = httpStatus(error);
  return `${status ? `HTTP ${status}: ` : ""}${message}`.replace(/\s+/g, " ").slice(0, MAX_ERROR_OUTPUT_CHARS);
}

export function classifyConnectionError(error) {
  const message = errorText(error);
  if (error?.code === "VIP_MCP_COORDINATION_TIMEOUT") return "mcp_coordination_timeout";
  if (/Just a moment|Cloudflare managed challenge|challenges\.cloudflare\.com|cf_chl_|Enable JavaScript and cookies/i.test(message)) {
    return "cloudflare_managed_challenge";
  }
  const status = httpStatus(error);
  if (status === 429) return "remote_rate_limited";
  if (status === 401 || status === 403) return "remote_auth_or_permission_error";
  if ([502, 503, 504].includes(status)) return "remote_mcp_transient_error";
  if (/\b429\b|too many requests|rate.?limit/i.test(message)) return "remote_rate_limited";
  if (/ENOTFOUND|EAI_AGAIN|getaddrinfo/i.test(message)) return "dns_resolution_error";
  if (/\b(401|403)\b/.test(message)) return "remote_auth_or_permission_error";
  if (/\b(502|503|504)\b/.test(message)) return "remote_mcp_transient_error";
  if (/ECONNRESET|ECONNREFUSED|ETIMEDOUT|fetch failed|timeout|timed out/i.test(message)) {
    return "network_transport_error";
  }
  return "mcp_connection_error";
}

export function isTransientConnectionError(error) {
  return [
    "dns_resolution_error",
    "remote_mcp_transient_error",
    "network_transport_error"
  ].includes(classifyConnectionError(error));
}

function isCooldownConnectionError(error) {
  return ["remote_rate_limited", "cloudflare_managed_challenge"].includes(
    classifyConnectionError(error)
  );
}

function retryDelayMs(baseDelayMs, attempt) {
  return (
    Math.min(baseDelayMs * 2 ** (attempt - 1), MAX_RETRY_DELAY_MS) +
    Math.floor(Math.random() * RETRY_JITTER_MS)
  );
}

function runSelfTest() {
  const cases = [
    [new Error("HTTP 503"), "remote_mcp_transient_error"],
    [new Error("HTTP 429 Too Many Requests"), "remote_rate_limited"],
    [new Error("<title>Just a moment...</title> challenges.cloudflare.com"), "cloudflare_managed_challenge"],
    [new Error("getaddrinfo ENOTFOUND vip-tools-mcp.onrender.com"), "dns_resolution_error"]
  ];
  for (const [error, expected] of cases) {
    const actual = classifyConnectionError(error);
    if (actual !== expected) throw new Error(`Self-test failed: expected ${expected}, got ${actual}`);
  }
  if (safeErrorText(cases[2][0]).includes("<title>")) {
    throw new Error("Self-test failed: Cloudflare HTML was not sanitized");
  }
  const delay = retryDelayMs(100, 1);
  if (delay < 100 || delay >= 100 + RETRY_JITTER_MS) {
    throw new Error("Self-test failed: retry jitter is outside the expected range");
  }
  console.log(JSON.stringify({ ok: true, checks: cases.length + 2 }));
}

async function parseToolArgs(opts) {
  let raw = "{}";
  if (opts.argsFile !== null) raw = await fs.readFile(opts.argsFile, "utf8");
  else if (opts.argsJson !== null) raw = opts.argsJson;

  if (Buffer.byteLength(raw, "utf8") > MAX_ARGS_BYTES) {
    throw new Error(`Tool-Argumente ueberschreiten ${MAX_ARGS_BYTES} Bytes.`);
  }
  const parsed = JSON.parse(raw);
  if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
    throw new Error("Tool-Argumente muessen ein JSON-Objekt sein.");
  }
  return parsed;
}

async function connectOnce(opts, attempt) {
  const client = new Client({ name: "vip-canonical-mcp-fallback", version: "1.0.0" });
  const transport = new StreamableHTTPClientTransport(new URL(MCP_URL), {
    fetch: createCorrelatedMcpFetch(fetch, (record) =>
      console.error(`[vip-mcp] ${JSON.stringify(record)}`))
  });
  try {
    await withTimeout(client.connect(transport), opts.connectTimeoutMs, "MCP connect");
    return client;
  } catch (error) {
    await client.close().catch(() => {});
    error.attempt = attempt;
    throw error;
  }
}

async function connectWithRetry(opts) {
  const lock = await acquireConnectLock();
  let lastError;
  try {
    await waitForSharedTransportWindow();
    for (let attempt = 1; attempt <= opts.attempts; attempt += 1) {
      await recordTransportState({
        lastConnectAttemptAt: new Date().toISOString(),
        lastConnectAttemptPid: process.pid
      });
      try {
        const client = await connectOnce(opts, attempt);
        await recordTransportState({
          lastConnectSuccessAt: new Date().toISOString(),
          lastErrorClass: null,
          lastError: null,
          cooldownUntil: null
        });
        return { client, attemptsUsed: attempt, coordinationWaitMs: lock.waitedMs };
      } catch (error) {
        lastError = error;
        const cooldown = isCooldownConnectionError(error);
        const retry = cooldown
          ? attempt === 1 && opts.attempts > 1
          : attempt < opts.attempts && isTransientConnectionError(error);
        const delayMs = cooldown
          ? RATE_LIMIT_COOLDOWN_MS + Math.floor(Math.random() * RETRY_JITTER_MS)
          : retryDelayMs(opts.delayMs, attempt);
        await recordTransportState({
          lastConnectFailureAt: new Date().toISOString(),
          lastErrorClass: classifyConnectionError(error),
          lastError: safeErrorText(error),
          cooldownUntil: cooldown
            ? new Date(Date.now() + RATE_LIMIT_COOLDOWN_MS).toISOString()
            : null
        });
        console.error(
          `[vip-mcp] connect attempt ${attempt}/${opts.attempts} failed ` +
            `(${classifyConnectionError(error)}): ${safeErrorText(error)}` +
            `${retry ? `; ${cooldown ? "shared cooldown" : "retrying"} in ${delayMs} ms` : ""}`
        );
        if (!retry) break;
        await sleep(delayMs);
      }
    }
    throw lastError;
  } finally {
    await releaseConnectLock(lock);
  }
}

function normalizeContent(content) {
  return (Array.isArray(content) ? content : []).map((item) => {
    if (item?.type === "text") {
      try {
        return { ...item, parsed: JSON.parse(item.text) };
      } catch {
        return item;
      }
    }
    return item;
  });
}

async function recordToolTransportFailure(error, tool) {
  const lock = await acquireConnectLock();
  try {
    const current = await readJson(TRANSPORT_STATE_PATH, {});
    const cooldown = isCooldownConnectionError(error);
    const until = Math.max(Date.parse(current.cooldownUntil || "") || 0,
      cooldown ? Date.now() + RATE_LIMIT_COOLDOWN_MS : 0);
    await recordTransportState({
      lastToolFailureAt: new Date().toISOString(),
      lastTool: tool,
      lastToolErrorClass: classifyConnectionError(error),
      lastToolError: safeErrorText(error),
      ...(cooldown ? { cooldownUntil: new Date(until).toISOString() } : {})
    });
  } finally {
    await releaseConnectLock(lock);
  }
}

// Fixed public readiness endpoint, sharing the MCP mutex and cooldown. A poll
// defers during cooldown instead of introducing another connection/retry tier.
export async function readMaterialCommentStoreHealth({ request = fetch } = {}) {
  const lock = await acquireConnectLock();
  try {
    const current = await readJson(TRANSPORT_STATE_PATH, {});
    if (Date.parse(current.cooldownUntil || "") > Date.now())
      return { ready: false, reason: "shared_cooldown", cooldown_until: current.cooldownUntil };
    await waitForSharedTransportWindow();
    await recordTransportState({ lastConnectAttemptAt: new Date().toISOString() });
    let response;
    try {
      response = await request("https://vip-tools-mcp.onrender.com/material-comment-store/health", {
        method: "GET", redirect: "error", cache: "no-store", signal: AbortSignal.timeout(38_000)
      });
      const reader = response.body.getReader();
      let size = 0;
      const chunks = [];
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          size += value.byteLength;
          if (size > 32768) throw new Error("Health response exceeds limit");
          chunks.push(Buffer.from(value));
        }
      } finally { await reader.cancel().catch(() => {}); }
      const body = Buffer.concat(chunks).toString("utf8");
      if (response.status === 429 || response.headers.get("cf-mitigated") === "challenge" ||
          /just a moment|managed challenge/i.test(body)) {
        await recordTransportState({ cooldownUntil: new Date(Date.now() + RATE_LIMIT_COOLDOWN_MS).toISOString() });
        return { ready: false, reason: "shared_cooldown", status: response.status };
      }
      if (!response.ok) return { ready: false, reason: "http_error", status: response.status };
      const data = JSON.parse(body);
      return { ready: data?.ready === true, reason: data?.ready === true ? "ready" : "store_not_ready",
        status: response.status };
    } catch {
      return { ready: false, reason: "health_read_failed", status: response?.status ?? null };
    }
  } finally { await releaseConnectLock(lock); }
}

export async function callToolOnce(client, tool, args, timeoutMs, recordFailure = recordToolTransportFailure) {
  try {
    const result = await withTimeout(client.callTool({ name: tool, arguments: args }, undefined,
      {timeout:timeoutMs,maxTotalTimeout:timeoutMs}), timeoutMs, `MCP tool ${tool}`);
    if (result?.isError) {
      const message = (result.content || []).filter((item) => item.type === "text")
        .map((item) => item.text).join("\n").slice(0, 2000);
      const error = new Error(message || "MCP tool failed");
      if (isCooldownConnectionError(error)) await recordFailure(error, tool).catch(() => {});
    }
    return result;
  } catch (error) {
    // Record post-connect throttling too, but never replay a possible mutation.
    await recordFailure(error, tool).catch(() => {});
    throw error;
  }
}

// Programmatic clients use the same single-flight connection and shared
// cooldown as the CLI. There is no endpoint override or additional retry tier.
export async function connectCanonicalMcp() {
  const { client } = await connectWithRetry({ attempts: DEFAULT_ATTEMPTS,
    delayMs: DEFAULT_DELAY_MS, connectTimeoutMs: DEFAULT_CONNECT_TIMEOUT_MS });
  return {
    async callTool(request, _schema, options = {}) {
      await waitForSharedTransportWindow();
      const remaining=options.deadlineAtMs == null ? DEFAULT_TOOL_TIMEOUT_MS : options.deadlineAtMs-Date.now();
      if(remaining<=0) throw new Error("MCP readback deadline exceeded before tool call");
      const timeout = Math.min(options.maxTotalTimeout || options.timeout ||
        DEFAULT_TOOL_TIMEOUT_MS, DEFAULT_TOOL_TIMEOUT_MS,remaining);
      return callToolOnce(client, request.name, request.arguments, timeout);
    },
    close: () => client.close()
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.selfTest) {
    runSelfTest();
    return;
  }
  const toolArgs = opts.listTools ? null : await parseToolArgs(opts);
  let client;

  try {
    const connected = await connectWithRetry(opts);
    client = connected.client;

    if (opts.listTools) {
      const result = await withTimeout(client.listTools(), opts.toolTimeoutMs, "MCP listTools");
      console.log(
        JSON.stringify({
          ok: true,
          transport: "canonical_remote_mcp_fallback",
          endpoint: MCP_URL,
          attempts_used: connected.attemptsUsed,
          coordination_wait_ms: connected.coordinationWaitMs,
          tools: (result.tools || []).map((tool) => tool.name).sort()
        })
      );
      return;
    }

    // Tool calls are intentionally not auto-retried. A transport error after a
    // mutating request is ambiguous and retrying could duplicate side effects.
    const result = await callToolOnce(client, opts.tool, toolArgs, opts.toolTimeoutMs);
    console.log(
      JSON.stringify({
        ok: !result.isError,
        transport: "canonical_remote_mcp_fallback",
        endpoint: MCP_URL,
        attempts_used: connected.attemptsUsed,
        coordination_wait_ms: connected.coordinationWaitMs,
        tool: opts.tool,
        is_error: Boolean(result.isError),
        content: normalizeContent(result.content)
      })
    );
    if (result.isError) process.exitCode = 2;
  } finally {
    await client?.close().catch(() => {});
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) main().catch((error) => {
  console.error(
    JSON.stringify({
      ok: false,
      transport: "canonical_remote_mcp_fallback",
      endpoint: MCP_URL,
      error_class: classifyConnectionError(error),
      error: safeErrorText(error),
      attempts_used: error?.attempt || null
    })
  );
  process.exitCode = 1;
});
