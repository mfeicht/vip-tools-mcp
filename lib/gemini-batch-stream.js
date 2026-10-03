import { getGeminiBatchInlineResponses } from "./gemini-image-batch.js";

// Extract inline batch results without retaining the provider's complete JSON body.
// The returned document has the single inlinedResponses array replaced by [].
export async function readGeminiBatchStream(stream, onEntry, { maxEntryBytes = 75 * 1024 * 1024, maxBodyBytes = 300 * 1024 * 1024 } = {}) {
  const outer = [];
  let outerBytes = 0;
  let totalBytes = 0;
  let entryParts = [];
  let entryBytes = 0;
  let mode = "outer";
  let inString = false;
  let escaped = false;
  let token = "";
  let lastString = null;
  let propertyKey = null;
  let arrayCount = 0;
  let entryCount = 0;
  let arrayEntryCount = 0;
  let collectArray = false;
  let arrayExpect = "value";
  let depth = 0;

  function appendOuter(part) {
    if (!part.length) return;
    outerBytes += part.length;
    if (outerBytes > 2 * 1024 * 1024) throw new Error("Gemini-Batch-Metadaten sind zu groß.");
    outer.push(part);
  }
  function appendEntry(part) {
    if (!part.length) return;
    entryBytes += part.length;
    if (entryBytes > maxEntryBytes) throw new Error("Gemini-Batch-Einzelantwort ist zu groß.");
    entryParts.push(part);
  }

  for await (const source of stream) {
    const chunk = Buffer.isBuffer(source) ? source : Buffer.from(source);
    totalBytes += chunk.length;
    if (totalBytes > maxBodyBytes) throw new Error("Gemini-Batch-Antwort überschreitet die Größenbegrenzung.");
    let segmentStart = 0;
    for (let i = 0; i < chunk.length; i++) {
      const byte = chunk[i];
      if (mode === "outer") {
        if (inString) {
          if (escaped) escaped = false;
          else if (byte === 92) escaped = true;
          else if (byte === 34) {
            inString = false;
            lastString = token;
          } else if (token.length < 128) token += String.fromCharCode(byte);
          continue;
        }
        if (byte === 34) {
          inString = true;
          token = "";
        } else if (byte === 58) {
          propertyKey = lastString;
          lastString = null;
        } else if (byte === 91 && (propertyKey === "inlinedResponses" || propertyKey === "inlined_responses")) {
          arrayCount++;
          collectArray = arrayCount === 1;
          arrayEntryCount = 0;
          appendOuter(chunk.subarray(segmentStart, i + 1));
          appendOuter(Buffer.from(collectArray ? '{"__vip_collect_target__":true}]' : "]"));
          segmentStart = i + 1;
          mode = "array";
          propertyKey = null;
          arrayExpect = "value";
        } else if (byte !== 32 && byte !== 9 && byte !== 10 && byte !== 13) {
          propertyKey = null;
          if (byte !== 44) lastString = null;
        }
      } else if (mode === "array") {
        if (byte === 32 || byte === 9 || byte === 10 || byte === 13) continue;
        if (byte === 123 && arrayExpect === "value") {
          mode = "entry";
          depth = 1;
          inString = false;
          escaped = false;
          segmentStart = i;
        } else if (byte === 44 && arrayExpect === "comma") {
          arrayExpect = "value";
        } else if (byte === 93 && arrayExpect === "comma") {
          mode = "outer";
          segmentStart = i + 1;
        } else if (byte === 93 && arrayExpect === "value" && arrayEntryCount === 0) {
          mode = "outer";
          segmentStart = i + 1;
        } else {
          throw new Error("Gemini-Batch-Inline-Liste ist ungültig.");
        }
      } else {
        if (inString) {
          if (escaped) escaped = false;
          else if (byte === 92) escaped = true;
          else if (byte === 34) inString = false;
        } else if (byte === 34) inString = true;
        else if (byte === 123 || byte === 91) depth++;
        else if (byte === 125 || byte === 93) {
          depth--;
          if (depth < 0) throw new Error("Gemini-Batch-Einzelantwort ist ungültig.");
          if (depth === 0) {
            if (collectArray) appendEntry(chunk.subarray(segmentStart, i + 1));
            const entry = collectArray ? JSON.parse(Buffer.concat(entryParts, entryBytes).toString("utf8")) : null;
            entryParts = [];
            entryBytes = 0;
            arrayEntryCount++;
            if (collectArray) {
              entryCount++;
              await onEntry(entry, entryCount - 1);
            }
            mode = "array";
            arrayExpect = "comma";
            segmentStart = i + 1;
          }
        }
      }
    }
    if (mode === "outer") appendOuter(chunk.subarray(segmentStart));
    else if (mode === "entry" && collectArray) appendEntry(chunk.subarray(segmentStart));
  }
  if (mode !== "outer" || inString || arrayCount < 1) {
    throw new Error("Gemini-Batch-Antwort ist unvollständig oder enthält keine Inline-Ergebnisse.");
  }
  const document = JSON.parse(Buffer.concat(outer, outerBytes).toString("utf8"));
  const inline = getGeminiBatchInlineResponses(document);
  if (inline.length !== 1 || inline[0]?.__vip_collect_target__ !== true) {
    throw new Error("Gemini-Batch-Inline-Liste liegt außerhalb des erwarteten Antwortpfads.");
  }
  inline.length = 0;
  return { document, entryCount, totalBytes };
}
