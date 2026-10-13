import { randomUUID } from "node:crypto";

function unavailable(cause) {
  const error = new Error("material_comment_store_unavailable_fail_closed");
  error.code = "MATERIAL_COMMENT_STORE_UNAVAILABLE";
  error.cause = cause;
  return error;
}

export function createHttpMaterialCommentStore({
  token,
  url,
  sitesBypassToken,
  timeoutMs = 7_000,
  retryDelayMs = 250,
  fetchImpl = fetch
} = {}) {
  const bearer = String(token || "").trim();
  const sitesBearer = String(sitesBypassToken || "").trim();
  if (!bearer || !url) throw new Error("Material-comment HTTP store needs an endpoint and token.");
  const endpoint = new URL(url);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
    throw new Error("Material-comment HTTP store needs a plain HTTPS endpoint.");
  }
  if (sitesBearer && (!endpoint.hostname.endsWith(".chatgpt.site") || /[\r\n]/.test(sitesBearer))) {
    throw new Error("Material-comment Sites access needs a Sites HTTPS endpoint and valid token.");
  }

  async function request(operation, fields = {}) {
    // A write may have committed even when its response was lost; only reads retry.
    const readOnly = operation === "health" || operation === "readReceipt";
    for (let attempt = 0; attempt < (readOnly ? 2 : 1); attempt += 1) {
      try {
        const response = await fetchImpl(endpoint, {
          method: "POST",
          headers: {
            authorization: `Bearer ${bearer}`,
            ...(sitesBearer ? { "OAI-Sites-Authorization": `Bearer ${sitesBearer}` } : {}),
            "content-type": "application/json",
            accept: "application/json"
          },
          body: JSON.stringify({ operation, ...fields }),
          cache: "no-store",
          redirect: "error",
          signal: AbortSignal.timeout(timeoutMs)
        });
        if (!response.ok || !response.headers.get("content-type")?.includes("application/json")) {
          throw new Error(`store_http_${response.status}`);
        }
        const body = await response.json();
        if (body?.ok !== true || !body.result || typeof body.result !== "object") {
          throw new Error("store_protocol_invalid");
        }
        return body.result;
      } catch (error) {
        const transientRead = readOnly && attempt === 0 &&
          (/^store_http_(?:502|503|504)$/.test(String(error?.message || "")) ||
            error?.name === "TimeoutError" || error?.name === "AbortError" ||
            (error instanceof TypeError && /fetch/i.test(error.message)));
        if (!transientRead) throw unavailable(error);
        await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
      }
    }
  }

  return {
    async health() {
      const result = await request("health");
      if (result.status !== "ready") throw unavailable(new Error("store_not_ready"));
      return { ready: true };
    },
    async readReceipt({ key, payloadHash }) {
      const result = await request("readReceipt", { key, payloadHash });
      if (!("receipt" in result)) throw unavailable(new Error("receipt_missing"));
      return result.receipt;
    },
    async acquire({ key, payloadHash, payloadProbe, token = randomUUID() }) {
      const result = await request("acquire", { key, payloadHash, payloadProbe, token });
      if (typeof result.acquired !== "boolean" || !Number.isSafeInteger(result.fence)) {
        throw unavailable(new Error("acquire_protocol_invalid"));
      }
      return result;
    },
    async beginPosting({ key, payloadHash, token, fence }) {
      const result = await request("beginPosting", { key, payloadHash, token, fence });
      if (typeof result.ok !== "boolean" || typeof result.status !== "string") {
        throw unavailable(new Error("begin_protocol_invalid"));
      }
      return result;
    },
    async complete({ key, payloadHash, token, fence, storyGid, completionMode = "posted" }) {
      const result = await request("complete", {
        key, payloadHash, token, fence, storyGid, completionMode
      });
      if (typeof result.ok !== "boolean" || typeof result.status !== "string") {
        throw unavailable(new Error("complete_protocol_invalid"));
      }
      return result;
    },
    async release({ key, payloadHash, token, fence }) {
      const result = await request("release", { key, payloadHash, token, fence });
      if (typeof result.ok !== "boolean" || typeof result.status !== "string") {
        throw unavailable(new Error("release_protocol_invalid"));
      }
      return result;
    },
    async close() {}
  };
}
