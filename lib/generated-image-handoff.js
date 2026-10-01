import { createHash } from "node:crypto";
import path from "node:path";

const ALLOWED_MIME_TYPES = new Set(["image/jpeg", "image/png"]);

export function detectGeneratedImageMimeType(bytes) {
  if (!Buffer.isBuffer(bytes) || bytes.length < 8) return null;
  if (bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[bytes.length - 2] === 0xff && bytes[bytes.length - 1] === 0xd9) {
    return "image/jpeg";
  }
  if (bytes.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))) {
    return "image/png";
  }
  return null;
}

export function validateGeneratedDriveImageMetadata(file, maxBytes) {
  const provider = String(file?.appProperties?.provider || "");
  const mimeType = String(file?.mimeType || "").toLowerCase();
  const size = Number(file?.size || 0);
  const parents = Array.isArray(file?.parents) ? file.parents.filter(Boolean) : [];

  if (provider !== "google-gemini") {
    throw new Error("Drive-Datei ist nicht als Google-Gemini-Ausgabe markiert.");
  }
  if (!ALLOWED_MIME_TYPES.has(mimeType)) {
    throw new Error(`Gemini-Drive-Datei hat einen nicht erlaubten MIME-Typ: ${mimeType || "unknown"}.`);
  }
  if (!Number.isFinite(size) || size <= 0) {
    throw new Error("Gemini-Drive-Datei hat keine belastbare positive Dateigroesse.");
  }
  if (size > maxBytes) {
    throw new Error(`Gemini-Drive-Datei ist mit ${size} Bytes groesser als das Limit ${maxBytes}.`);
  }
  if (!parents.length) {
    throw new Error("Gemini-Drive-Datei liegt in keinem verifizierbaren Agentenordner.");
  }

  return { provider, mimeType, size, parents };
}

function sanitizePathPart(value, fallback) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
  return normalized || fallback;
}

export function buildGeneratedCloudinaryLocation({
  folderPrefix,
  projectKey,
  assetKey,
  fileName,
  index = 0,
  sha256
}) {
  const normalizedSha256 = String(sha256 || "").toLowerCase();
  if (!/^[a-f0-9]{64}$/.test(normalizedSha256)) {
    throw new Error("sha256 muss 64 hexadezimale Zeichen enthalten.");
  }
  const baseName = path.basename(String(fileName || "gemini-image"), path.extname(String(fileName || "")));
  const folder = [
    String(folderPrefix || "").replace(/^\/+|\/+$/g, ""),
    sanitizePathPart(projectKey, "project"),
    `generated-${sanitizePathPart(assetKey, "asset")}`
  ]
    .filter(Boolean)
    .join("/");
  const publicId = `${String(index + 1).padStart(2, "0")}-${sanitizePathPart(baseName, "gemini-image")}-${normalizedSha256.slice(0, 12)}`;
  return { folder, publicId };
}

export function verifyGeneratedImageBytes(bytes, declaredMimeType) {
  const detectedMimeType = detectGeneratedImageMimeType(bytes);
  if (!detectedMimeType) {
    throw new Error("Gemini-Datei ist kein gueltiges JPEG- oder PNG-Bild.");
  }
  if (detectedMimeType !== declaredMimeType) {
    throw new Error(
      `Gemini-Dateiinhalt (${detectedMimeType}) widerspricht dem deklarierten MIME-Typ (${declaredMimeType}).`
    );
  }
  return {
    detectedMimeType,
    sha256: createHash("sha256").update(bytes).digest("hex"),
    bytes: bytes.length
  };
}
