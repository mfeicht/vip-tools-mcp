import { createHash } from "crypto";

import {
  GEMINI_DEFAULT_IMAGE_MODEL,
  GEMINI_IMAGE_ASPECT_RATIOS,
  GEMINI_IMAGE_MIME_TYPES,
  GEMINI_IMAGE_MODEL_PROFILES,
  normalizeGeminiImageFileName,
  resolveGeminiImageSize
} from "./gemini-image.js";

export const GEMINI_IMAGE_BATCH_MAX_REQUESTS = 12;
export const GEMINI_IMAGE_BATCH_TERMINAL_STATES = Object.freeze([
  "SUCCEEDED",
  "FAILED",
  "CANCELLED",
  "EXPIRED"
]);

function sha256(value) {
  return createHash("sha256").update(String(value), "utf8").digest("hex");
}

function normalizeMetadataText(value, maxLength = 256) {
  return String(value ?? "").trim().slice(0, maxLength);
}

export function normalizeGeminiBatchName(value) {
  const normalized = String(value || "").trim();
  if (!/^batches\/[A-Za-z0-9._-]{4,220}$/.test(normalized)) {
    throw new Error("batch_name muss dem Format batches/<ID> entsprechen.");
  }
  return normalized;
}

export function normalizeGeminiBatchRequestKey(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!/^[a-z0-9][a-z0-9_-]{0,62}$/.test(normalized)) {
    throw new Error(
      "request_key muss mit einer Zahl oder einem Kleinbuchstaben beginnen und darf nur a-z, 0-9, _ und - enthalten."
    );
  }
  return normalized;
}

export function buildGeminiImageBatchPayload({
  model = GEMINI_DEFAULT_IMAGE_MODEL,
  displayName,
  projectKey,
  targetPublishDate,
  requests
}) {
  if (!GEMINI_IMAGE_MODEL_PROFILES[model]) {
    throw new Error(`Nicht unterstuetztes Gemini-Bildmodell: ${model}`);
  }
  const normalizedDisplayName = String(displayName || "").trim();
  if (!/^[A-Za-z0-9][A-Za-z0-9._ -]{2,126}$/.test(normalizedDisplayName)) {
    throw new Error("display_name muss 3 bis 127 sichere Zeichen enthalten.");
  }
  const normalizedProjectKey = String(projectKey || "").trim().toLowerCase();
  if (!/^[a-z0-9][a-z0-9_-]{1,80}$/.test(normalizedProjectKey)) {
    throw new Error("project_key ist ungueltig.");
  }
  const normalizedTargetDate = String(targetPublishDate || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalizedTargetDate)) {
    throw new Error("target_publish_date muss YYYY-MM-DD entsprechen.");
  }
  if (!Array.isArray(requests) || !requests.length || requests.length > GEMINI_IMAGE_BATCH_MAX_REQUESTS) {
    throw new Error(`requests muss 1 bis ${GEMINI_IMAGE_BATCH_MAX_REQUESTS} Eintraege enthalten.`);
  }

  const seenKeys = new Set();
  const normalizedRequests = requests.map((item, index) => {
    const key = normalizeGeminiBatchRequestKey(item.requestKey ?? item.request_key);
    if (seenKeys.has(key)) throw new Error(`request_key ist doppelt: ${key}`);
    seenKeys.add(key);

    const prompt = String(item.prompt || "").trim();
    if (prompt.length < 3 || prompt.length > 20_000) {
      throw new Error(`Prompt fuer ${key} muss 3 bis 20.000 Zeichen enthalten.`);
    }
    const aspectRatio = item.aspectRatio ?? item.aspect_ratio ?? "4:5";
    if (!GEMINI_IMAGE_ASPECT_RATIOS.includes(aspectRatio)) {
      throw new Error(`Nicht unterstuetztes Seitenverhaeltnis fuer ${key}: ${aspectRatio}`);
    }
    const imageSize = resolveGeminiImageSize(model, item.imageSize ?? item.image_size ?? "2K");
    const mimeType = item.mimeType ?? item.mime_type ?? "image/jpeg";
    if (!GEMINI_IMAGE_MIME_TYPES.includes(mimeType)) {
      throw new Error(`Nicht unterstuetzter Bildtyp fuer ${key}: ${mimeType}`);
    }
    const fallbackFileName = `${normalizedProjectKey}-${normalizedTargetDate}-${key}${
      mimeType === "image/jpeg" ? ".jpg" : ".png"
    }`;
    const fileName = normalizeGeminiImageFileName(
      item.fileName ?? item.file_name,
      mimeType,
      fallbackFileName.replace(/\.(png|jpe?g)$/i, "")
    );
    const slide = Number(item.slide ?? index + 1);
    if (!Number.isInteger(slide) || slide < 1 || slide > 99) {
      throw new Error(`slide fuer ${key} muss zwischen 1 und 99 liegen.`);
    }

    return {
      request: {
        contents: [{ role: "user", parts: [{ text: prompt }] }],
        generation_config: {
          responseModalities: ["IMAGE"],
          imageConfig: {
            aspectRatio,
            imageSize
          }
        },
        store: false
      },
      metadata: {
        key,
        project_key: normalizedProjectKey,
        target_publish_date: normalizedTargetDate,
        prompt_sha256: sha256(prompt),
        aspect_ratio: aspectRatio,
        image_size: imageSize,
        requested_mime_type: mimeType,
        file_name: fileName,
        slide
      }
    };
  });

  return {
    batch: {
      display_name: normalizedDisplayName,
      input_config: {
        requests: {
          requests: normalizedRequests
        }
      }
    }
  };
}

function findBatchResource(value) {
  if (!value || typeof value !== "object") return {};
  if (value.state || value.dest || value.output || value.batchStats || value.batch_stats) return value;
  if (value.response && typeof value.response === "object") return findBatchResource(value.response);
  if (value.metadata && typeof value.metadata === "object") return value.metadata;
  return value;
}

export function normalizeGeminiBatchState(value) {
  const raw = String(value || "UNKNOWN").trim().toUpperCase();
  return raw.replace(/^(JOB|BATCH)_STATE_/, "");
}

export function getGeminiBatchInlineResponses(value) {
  const resource = findBatchResource(value);
  const candidates = [
    resource?.dest?.inlinedResponses,
    resource?.dest?.inlined_responses,
    resource?.output?.inlinedResponses,
    resource?.output?.inlined_responses,
    value?.response?.inlinedResponses,
    value?.response?.inlined_responses
  ];
  for (const candidate of candidates) {
    if (Array.isArray(candidate)) return candidate;
    if (Array.isArray(candidate?.inlinedResponses)) return candidate.inlinedResponses;
    if (Array.isArray(candidate?.inlined_responses)) return candidate.inlined_responses;
  }
  return [];
}

export function summarizeGeminiBatchJob(value) {
  const resource = findBatchResource(value);
  const rawState = resource?.state || value?.metadata?.state || value?.state || "UNKNOWN";
  const state = normalizeGeminiBatchState(rawState);
  const nameCandidates = [
    resource?.name,
    value?.name,
    value?.metadata?.name,
    value?.response?.name
  ].filter((candidate) => /^batches\//.test(String(candidate || "")));
  const inlineResponses = getGeminiBatchInlineResponses(value);
  return {
    name: nameCandidates[0] || null,
    model: String(resource?.model || value?.metadata?.model || "").replace(/^models\//, "") || null,
    display_name:
      resource?.displayName || resource?.display_name || value?.metadata?.displayName || value?.metadata?.display_name || null,
    state,
    raw_state: rawState,
    terminal: GEMINI_IMAGE_BATCH_TERMINAL_STATES.includes(state),
    succeeded: state === "SUCCEEDED",
    create_time: resource?.createTime || resource?.create_time || value?.metadata?.createTime || null,
    update_time: resource?.updateTime || resource?.update_time || value?.metadata?.updateTime || null,
    end_time: resource?.endTime || resource?.end_time || value?.metadata?.endTime || null,
    batch_stats: resource?.batchStats || resource?.batch_stats || value?.metadata?.batchStats || null,
    error: resource?.error || value?.error || null,
    inline_response_count: inlineResponses.length,
    done: Boolean(value?.done) || GEMINI_IMAGE_BATCH_TERMINAL_STATES.includes(state)
  };
}

function normalizeBase64(value) {
  const compact = String(value || "").replace(/\s+/g, "");
  if (!compact || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(compact)) {
    throw new Error("Batch-Antwort enthaelt ungueltige Base64-Bilddaten.");
  }
  return compact;
}

export function* iterateGeminiBatchImages(value, { requestKeys } = {}) {
  const responses = getGeminiBatchInlineResponses(value);
  const selectedKeys = requestKeys ? new Set(requestKeys.map((key) => normalizeGeminiBatchRequestKey(key))) : null;
  for (const [index, entry] of responses.entries()) {
    const metadata = entry?.metadata && typeof entry.metadata === "object" ? entry.metadata : {};
    const requestKey = normalizeMetadataText(metadata.key || `request-${index + 1}`, 63);
    if (selectedKeys && !selectedKeys.has(requestKey.toLowerCase())) continue;
    if (entry?.error) {
      yield { index, requestKey, metadata, error: entry.error, image: null };
      continue;
    }
    const parts = [];
    for (const candidate of entry?.response?.candidates || []) {
      if (Array.isArray(candidate?.content?.parts)) parts.push(...candidate.content.parts);
    }
    if (Array.isArray(entry?.response?.parts)) parts.push(...entry.response.parts);
    const imagePart = [...parts].reverse().find((part) => part?.inlineData?.data || part?.inline_data?.data);
    if (!imagePart) {
      yield {
        index,
        requestKey,
        metadata,
        error: { message: "Gemini-Batchantwort enthaelt kein Bild." },
        image: null
      };
      continue;
    }
    const inlineData = imagePart.inlineData || imagePart.inline_data;
    const bytes = Buffer.from(normalizeBase64(inlineData.data), "base64");
    const mimeType = inlineData.mimeType || inlineData.mime_type || "image/png";
    yield {
      index,
      requestKey,
      metadata,
      error: null,
      image: {
        bytes,
        mimeType,
        sha256: createHash("sha256").update(bytes).digest("hex"),
        usage: entry?.response?.usageMetadata || entry?.response?.usage_metadata || null
      }
    };
  }
}

export function extractGeminiBatchImages(value, options) {
  return [...iterateGeminiBatchImages(value, options)];
}
