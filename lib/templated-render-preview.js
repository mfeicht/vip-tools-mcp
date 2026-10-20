import { verifyGeneratedImageBytes } from "./generated-image-handoff.js";

export const TEMPLATED_PREVIEW_MAX_BYTES = 8 * 1024 * 1024;
export const TEMPLATED_PREVIEW_TIMEOUT_MS = 15_000;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const MIME = { jpg: "image/jpeg", jpeg: "image/jpeg", png: "image/png" };

function requireThat(condition, reason) {
  if (!condition) throw new Error(`templated_preview_${reason}`);
}

export function validateTemplatedPreviewMetadata(render, renderId) {
  requireThat(UUID.test(renderId), "invalid_render_id");
  requireThat(render?.id === renderId && render?.status === "COMPLETED"
    && render?.removed !== true, "render_not_available");
  const mimeType = MIME[render.format];
  requireThat(Boolean(mimeType), "unsupported_format");
  requireThat(Number.isInteger(render.width) && Number.isInteger(render.height)
    && render.width > 0 && render.height > 0
    && render.width <= 4096 && render.height <= 4096
    && render.width * render.height <= 16_000_000, "dimensions_limit");
  requireThat(typeof render.url === "string" && render.url.length <= 2048, "invalid_url");
  let url;
  try { url = new URL(render.url); } catch { throw new Error("templated_preview_invalid_url"); }
  const prefix = `/render/${renderId}/`;
  requireThat(url.protocol === "https:" && url.hostname === "cdn.templated.media"
    && !url.port && !url.username && !url.password && !url.search && !url.hash
    && url.href === render.url && url.pathname.startsWith(prefix), "cdn_scope");
  let filename;
  try { filename = decodeURIComponent(url.pathname.slice(prefix.length)); }
  catch { throw new Error("templated_preview_invalid_filename"); }
  requireThat(filename.length > 0 && !/[\\/\x00-\x1f\x7f%]/.test(filename)
    && ![".", ".."].includes(filename)
    && new RegExp(`\\.${render.format === "jpg" ? "jpe?g" : render.format}$`, "i").test(filename),
  "cdn_filename");
  return { url: url.href, mimeType, width: render.width, height: render.height };
}

async function readImage(url, mimeType, fetchImpl) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TEMPLATED_PREVIEW_TIMEOUT_MS);
  let response;
  let reader;
  try {
    // Never forward the account API key, accept arbitrary URLs, or follow redirects.
    response = await fetchImpl(url, {
      method: "GET", redirect: "manual", signal: controller.signal,
      headers: { Accept: mimeType }
    });
    requireThat(response.status === 200 && !response.redirected, "cdn_http_status");
    const declared = (response.headers.get("content-type") || "").split(";")[0].trim().toLowerCase();
    // Templated's canonical CDN uses image/jpg for JPEG; bytes still must match.
    const normalized = declared === "image/jpg" ? "image/jpeg" : declared;
    requireThat(normalized === mimeType, "content_type_mismatch");
    const length = response.headers.get("content-length");
    if (length !== null) {
      requireThat(/^[0-9]+$/.test(length) && Number(length) > 0
        && Number(length) <= TEMPLATED_PREVIEW_MAX_BYTES, "content_length_limit");
    }
    requireThat(Boolean(response.body?.getReader), "missing_body");
    reader = response.body.getReader();
    const chunks = [];
    let total = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      requireThat(total <= TEMPLATED_PREVIEW_MAX_BYTES, "stream_limit");
      chunks.push(Buffer.from(value));
    }
    requireThat(total > 0, "empty_body");
    // Content-Length describes encoded bytes if the CDN used compression.
    if (length !== null && !response.headers.get("content-encoding")) {
      requireThat(total === Number(length), "content_length_mismatch");
    }
    return Buffer.concat(chunks, total);
  } finally {
    controller.abort();
    clearTimeout(timer);
    if (reader) {
      await reader.cancel().catch(() => undefined);
      reader.releaseLock();
    } else if (response?.body) {
      await response.body.cancel().catch(() => undefined);
    }
  }
}

export async function buildTemplatedRenderPreview({
  agentId, renderId, readRender, fetchImpl = globalThis.fetch
}) {
  requireThat(typeof agentId === "string" && agentId.length > 0, "agent_required");
  requireThat(UUID.test(renderId), "invalid_render_id");
  // readRender must use the caller's existing scoped Templated credentials.
  const response = await readRender(renderId);
  requireThat(response?.ok === true && response?.status === 200, "metadata_read_failed");
  const metadata = validateTemplatedPreviewMetadata(response.data, renderId);
  const bytes = await readImage(metadata.url, metadata.mimeType, fetchImpl);
  const verified = verifyGeneratedImageBytes(bytes, metadata.mimeType);
  requireThat(verified.dimensions?.width === metadata.width
    && verified.dimensions?.height === metadata.height, "image_dimensions_mismatch");
  return {
    content: [
      { type: "text", text: JSON.stringify({
        agent_id: agentId, render_id: renderId, status: "COMPLETED",
        canonical_url: metadata.url, mime_type: verified.detectedMimeType,
        bytes: verified.bytes, sha256: verified.sha256, dimensions: verified.dimensions,
        read_only: true, visual_review_required: true
      }, null, 2) },
      { type: "image", data: bytes.toString("base64"), mimeType: verified.detectedMimeType }
    ]
  };
}
