#!/usr/bin/env node

import fs from "node:fs/promises";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const MCP_URL = "https://vip-tools-mcp.onrender.com/mcp";
const DEFAULT_ATTEMPTS = 3;
const DEFAULT_DELAY_MS = 15000;
const DEFAULT_CONNECT_TIMEOUT_MS = 25000;
const DEFAULT_TOOL_TIMEOUT_MS = 120000;
const MAX_RETRY_DELAY_MS = 60000;
const MAX_ERROR_OUTPUT_CHARS = 600;
const MAX_ARGS_BYTES = 2 * 1024 * 1024;

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
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${label} muss eine positive Ganzzahl sein.`);
  }
  return parsed;
}

function parseArgs(argv) {
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
  return opts;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

function safeErrorText(error) {
  const message = errorText(error);
  if (/Just a moment|challenges\.cloudflare\.com|cf_chl_|Enable JavaScript and cookies/i.test(message)) {
    return "Cloudflare managed challenge on the canonical MCP endpoint";
  }
  return message.replace(/\s+/g, " ").slice(0, MAX_ERROR_OUTPUT_CHARS);
}

function classifyConnectionError(error) {
  const message = errorText(error);
  if (/Just a moment|challenges\.cloudflare\.com|cf_chl_|Enable JavaScript and cookies/i.test(message)) {
    return "cloudflare_managed_challenge";
  }
  if (/\b429\b|too many requests|rate.?limit/i.test(message)) return "remote_rate_limited";
  if (/ENOTFOUND|EAI_AGAIN|getaddrinfo/i.test(message)) return "dns_resolution_error";
  if (/\b(502|503|504)\b|Streamable HTTP error/i.test(message)) return "remote_mcp_transient_error";
  if (/ECONNRESET|ECONNREFUSED|ETIMEDOUT|fetch failed|timeout after/i.test(message)) {
    return "network_transport_error";
  }
  return "mcp_connection_error";
}

function isTransientConnectionError(error) {
  return [
    "dns_resolution_error",
    "remote_mcp_transient_error",
    "network_transport_error"
  ].includes(classifyConnectionError(error));
}

function retryDelayMs(baseDelayMs, attempt) {
  return Math.min(baseDelayMs * 2 ** (attempt - 1), MAX_RETRY_DELAY_MS);
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
  console.log(JSON.stringify({ ok: true, checks: cases.length + 1 }));
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
  const transport = new StreamableHTTPClientTransport(new URL(MCP_URL));
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
  let lastError;
  for (let attempt = 1; attempt <= opts.attempts; attempt += 1) {
    try {
      return { client: await connectOnce(opts, attempt), attemptsUsed: attempt };
    } catch (error) {
      lastError = error;
      const retry = attempt < opts.attempts && isTransientConnectionError(error);
      console.error(
        `[vip-mcp] connect attempt ${attempt}/${opts.attempts} failed ` +
          `(${classifyConnectionError(error)}): ${safeErrorText(error)}${retry ? "; retrying" : ""}`
      );
      if (!retry) break;
      await sleep(retryDelayMs(opts.delayMs, attempt));
    }
  }
  throw lastError;
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
          tools: (result.tools || []).map((tool) => tool.name).sort()
        })
      );
      return;
    }

    // Tool calls are intentionally not auto-retried. A transport error after a
    // mutating request is ambiguous and retrying could duplicate side effects.
    const result = await withTimeout(
      client.callTool({ name: opts.tool, arguments: toolArgs }),
      opts.toolTimeoutMs,
      `MCP tool ${opts.tool}`
    );
    console.log(
      JSON.stringify({
        ok: !result.isError,
        transport: "canonical_remote_mcp_fallback",
        endpoint: MCP_URL,
        attempts_used: connected.attemptsUsed,
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

main().catch((error) => {
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
