import { createHash } from "crypto";

export const GEMINI_IMAGE_MODEL_PROFILES = Object.freeze({
  "gemini-3.1-flash-image": Object.freeze({
    tier: "standard",
    label: "Nano Banana 2",
    defaultImageSize: "2K",
    imageSizes: Object.freeze(["512", "1K", "2K", "4K"]),
    thinkingLevels: Object.freeze(["minimal", "high"])
  }),
  "gemini-3-pro-image": Object.freeze({
    tier: "pro",
    label: "Nano Banana Pro",
    defaultImageSize: "4K",
    imageSizes: Object.freeze(["1K", "2K", "4K"]),
    thinkingLevels: Object.freeze([])
  }),
  "gemini-3.1-flash-lite-image": Object.freeze({
    tier: "lite",
    label: "Nano Banana 2 Lite",
    defaultImageSize: "1K",
    imageSizes: Object.freeze(["1K"]),
    thinkingLevels: Object.freeze(["minimal", "high"])
  })
});

export const GEMINI_IMAGE_MODEL_IDS = Object.freeze(Object.keys(GEMINI_IMAGE_MODEL_PROFILES));
export const GEMINI_DEFAULT_IMAGE_MODEL = "gemini-3.1-flash-image";
export const GEMINI_IMAGE_ASPECT_RATIOS = Object.freeze([
  "1:1",
  "1:4",
  "4:1",
  "1:8",
  "8:1",
  "2:3",
  "3:2",
  "3:4",
  "4:3",
  "4:5",
  "5:4",
  "9:16",
  "16:9",
  "21:9"
]);
export const GEMINI_IMAGE_SIZES = Object.freeze(["512", "1K", "2K", "4K"]);
export const GEMINI_IMAGE_MIME_TYPES = Object.freeze(["image/png", "image/jpeg"]);
export const GEMINI_IMAGE_THINKING_LEVELS = Object.freeze(["minimal", "high"]);

export function resolveGeminiImageSize(model, requestedImageSize) {
  const profile = GEMINI_IMAGE_MODEL_PROFILES[model];
  if (!profile) throw new Error(`Nicht unterstuetztes Gemini-Bildmodell: ${model}`);
  const imageSize = requestedImageSize || profile.defaultImageSize;
  if (!profile.imageSizes.includes(imageSize)) {
    throw new Error(
      `${profile.label} (${model}) unterstuetzt image_size=${imageSize} nicht. Erlaubt: ${profile.imageSizes.join(", ")}.`
    );
  }
  return imageSize;
}

export function buildGeminiImageInteractionPayload({
  prompt,
  model = GEMINI_DEFAULT_IMAGE_MODEL,
  aspectRatio = "16:9",
  imageSize,
  mimeType = "image/png",
  thinkingLevel
}) {
  const normalizedPrompt = String(prompt || "").trim();
  if (!normalizedPrompt) throw new Error("Gemini-Bildprompt fehlt.");
  if (!GEMINI_IMAGE_ASPECT_RATIOS.includes(aspectRatio)) {
    throw new Error(`Nicht unterstuetztes Gemini-Bildformat: ${aspectRatio}`);
  }
  if (!GEMINI_IMAGE_MIME_TYPES.includes(mimeType)) {
    throw new Error(`Nicht unterstuetzter Gemini-Bildtyp: ${mimeType}`);
  }

  const profile = GEMINI_IMAGE_MODEL_PROFILES[model];
  const resolvedImageSize = resolveGeminiImageSize(model, imageSize);
  if (thinkingLevel && !profile.thinkingLevels.includes(thinkingLevel)) {
    throw new Error(
      `${profile.label} (${model}) akzeptiert thinking_level=${thinkingLevel} in diesem Tool nicht.`
    );
  }

  return {
    model,
    input: [{ type: "text", text: normalizedPrompt }],
    response_format: {
      type: "image",
      mime_type: mimeType,
      aspect_ratio: aspectRatio,
      image_size: resolvedImageSize
    },
    ...(thinkingLevel ? { generation_config: { thinking_level: thinkingLevel } } : {}),
    store: false
  };
}

function normalizeBase64(value) {
  const compact = String(value || "").replace(/\s+/g, "");
  if (!compact || !/^[A-Za-z0-9+/]+={0,2}$/.test(compact)) {
    throw new Error("Gemini-Antwort enthaelt keine gueltigen Base64-Bilddaten.");
  }
  return compact;
}

function collectImageBlocks(value, blocks, seen) {
  if (!value || typeof value !== "object" || seen.has(value)) return;
  seen.add(value);

  if (
    typeof value.data === "string" &&
    (value.type === "image" || String(value.mime_type || value.mimeType || "").startsWith("image/"))
  ) {
    blocks.push(value);
  }

  if (Array.isArray(value)) {
    for (const item of value) collectImageBlocks(item, blocks, seen);
    return;
  }
  for (const child of Object.values(value)) collectImageBlocks(child, blocks, seen);
}

export function extractGeminiImageResult(responseData) {
  const interaction = responseData?.interaction || responseData;
  const blocks = [];
  if (interaction?.output_image) blocks.push(interaction.output_image);
  if (interaction?.outputImage) blocks.push(interaction.outputImage);
  collectImageBlocks(interaction?.steps || interaction?.outputs || interaction, blocks, new Set());

  const imageBlock = [...blocks].reverse().find((block) => typeof block?.data === "string");
  if (!imageBlock) {
    const status = interaction?.status ? ` Status: ${interaction.status}.` : "";
    throw new Error(`Gemini hat kein finales Bild geliefert.${status}`);
  }

  const base64 = normalizeBase64(imageBlock.data);
  const bytes = Buffer.from(base64, "base64");
  if (!bytes.length) throw new Error("Gemini hat leere Bilddaten geliefert.");
  const mimeType = imageBlock.mime_type || imageBlock.mimeType || "image/png";
  if (!String(mimeType).startsWith("image/")) {
    throw new Error(`Gemini lieferte einen unerwarteten MIME-Typ: ${mimeType}`);
  }

  return {
    bytes,
    mimeType,
    sha256: createHash("sha256").update(bytes).digest("hex"),
    interactionId: interaction?.id || null,
    status: interaction?.status || null,
    usage: interaction?.usage || interaction?.usage_metadata || interaction?.usageMetadata || null,
    outputText: interaction?.output_text || interaction?.outputText || null
  };
}

export function normalizeGeminiImageFileName(fileName, mimeType, fallbackStem = "gemini-image") {
  const extension = mimeType === "image/jpeg" ? ".jpg" : ".png";
  const normalized = String(fileName || "").trim();
  if (!normalized) return `${fallbackStem}${extension}`;
  if (
    normalized !== normalized.split(/[\\/]/).pop() ||
    /[\0\r\n]/.test(normalized) ||
    !/^[A-Za-z0-9][A-Za-z0-9._ -]{0,174}\.(png|jpe?g)$/i.test(normalized)
  ) {
    throw new Error("file_name muss ein sicherer PNG- oder JPG-Dateiname ohne Pfad sein.");
  }
  if (mimeType === "image/jpeg" && !/\.jpe?g$/i.test(normalized)) {
    throw new Error("file_name-Endung und Gemini-MIME-Typ passen nicht zusammen.");
  }
  if (mimeType === "image/png" && !/\.png$/i.test(normalized)) {
    throw new Error("file_name-Endung und Gemini-MIME-Typ passen nicht zusammen.");
  }
  return normalized;
}

export function summarizeGeminiApiError(data) {
  const error = data?.error || data;
  return {
    code: error?.code || null,
    status: error?.status || null,
    message: String(error?.message || "Unbekannter Gemini-API-Fehler").slice(0, 1000)
  };
}
