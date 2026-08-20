import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const source = await readFile(new URL("../server.js", import.meta.url), "utf8");
const start = source.indexOf('server.tool(\n    "accounting_drive_pdf_invoice_ocr_totals"');
const end = source.indexOf('server.tool(\n    "accounting_drive_invoice_metadata_delta"', start);

assert.notEqual(start, -1, "accounting OCR fallback tool is missing");
assert.notEqual(end, -1, "accounting OCR fallback tool boundary is missing");

const toolBlock = source.slice(start, end);
assert.match(toolBlock, /TOOL_SAFE_WRITE/);
assert.match(toolBlock, /requireMoritz:\s*true/);
assert.match(toolBlock, /assertAllowedAccountingFolder\(folder_id/);
assert.match(toolBlock, /file\.mimeType !== "application\/pdf"/);
assert.match(toolBlock, /file\.parents \|\| \[\]\)\.includes\(folder_id\)/);
assert.match(toolBlock, /deleteGoogleDriveFileWithRetry\(transientFile\.id/);
assert.match(toolBlock, /transient_cleanup_verified/);
assert.match(toolBlock, /Kein Volltext wird ausgegeben oder lokal gespeichert/);
assert.doesNotMatch(toolBlock, /ocrText\s*:/);

console.log("accounting OCR contract self-test passed");
