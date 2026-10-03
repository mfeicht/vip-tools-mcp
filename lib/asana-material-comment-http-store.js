import { randomUUID } from "node:crypto";

const DEFAULT_URL = "https://ai-operations.vip-studios.de/api/material-comment-store";

function unavailable(cause) {
  const error = new Error("material_comment_store_unavailable_fail_closed");
  error.code = "MATERIAL_COMMENT_STORE_UNAVAILABLE";
  error.cause = cause;
  return error;
}

export function createHttpMaterialCommentStore({
  token,
  url = DEFAULT_URL,
  timeoutMs = 7_000,
  fetchImpl = fetch
} = {}) {
  const bearer = String(token || "").trim();
  const endpoint = new URL(url);
  if (!bearer) throw new Error("Material-comment D1 store needs the shared dashboard feed token.");
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
    throw new Error("Material-comment D1 store needs a plain HTTPS endpoint.");
  }

  async function request(operation, fields = {}) {
    let response;
    try {
      response = await fetchImpl(endpoint, {
        method: "POST",
        headers: {
          authorization: `Bearer ${bearer}`,
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
      throw unavailable(error);
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
