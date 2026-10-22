import { randomUUID } from "node:crypto";

const TRACE_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SAFE_HEADER_PATTERN = /^[a-zA-Z0-9._:;= /-]{1,128}$/;
const RESPONSE_HEADER_NAMES = ["cf-ray", "x-request-id", "x-render-request-id", "x-vip-request-id"];

export function mcpTraceId(value) {
  return typeof value === "string" && TRACE_ID_PATTERN.test(value) ? value.toLowerCase() : randomUUID();
}

export function safeMcpResponseHeaders(headers) {
  const result = {};
  for (const name of RESPONSE_HEADER_NAMES) {
    const value = headers?.get?.(name);
    if (typeof value === "string" && SAFE_HEADER_PATTERN.test(value)) result[name] = value;
  }
  return result;
}

export function createCorrelatedMcpFetch(fetchImpl, report = console.error) {
  // Diagnostic output must never turn an HTTP response into an ambiguous
  // failed mutation or replace the original transport exception.
  const safeReport = (record) => { try { report(record); } catch {} };
  return async (url, init = {}) => {
    const traceId = mcpTraceId();
    const startedAt = new Date().toISOString();
    const headers = new Headers(init.headers);
    headers.set("x-vip-request-id", traceId);
    try {
      const response = await fetchImpl(url, { ...init, headers });
      if (response.status >= 500) {
        safeReport({ event: "mcp_http_5xx", started_at: startedAt,
          observed_at: new Date().toISOString(), trace_id: traceId,
          status: response.status, response_headers: safeMcpResponseHeaders(response.headers) });
      }
      return response;
    } catch (error) {
      const errorName = String(error?.name || "Error");
      safeReport({ event: "mcp_http_transport_error", started_at: startedAt,
        observed_at: new Date().toISOString(), trace_id: traceId,
        error_name: /^[A-Za-z][A-Za-z0-9]{0,63}$/.test(errorName) ? errorName : "Error" });
      throw error;
    }
  };
}
