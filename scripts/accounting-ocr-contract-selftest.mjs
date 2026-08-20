import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const source = await readFile(new URL("../server.js", import.meta.url), "utf8");
const resolverStart = source.indexOf('server.tool(\n    "accounting_drive_resolve_month_folder"');
const resolverEnd = source.indexOf('server.tool(\n    "accounting_drive_pdf_invoice_totals"', resolverStart);
const start = source.indexOf('server.tool(\n    "accounting_drive_pdf_invoice_ocr_totals"');
const end = source.indexOf('server.tool(\n    "accounting_drive_invoice_metadata_delta"', start);

assert.notEqual(resolverStart, -1, "accounting month-folder resolver is missing");
assert.notEqual(resolverEnd, -1, "accounting month-folder resolver boundary is missing");
assert.notEqual(start, -1, "accounting OCR fallback tool is missing");
assert.notEqual(end, -1, "accounting OCR fallback tool boundary is missing");

const resolverBlock = source.slice(resolverStart, resolverEnd);
assert.match(resolverBlock, /TOOL_READ_ONLY/);
assert.match(resolverBlock, /assertAllowedAccountingFolder\(parent_folder_id/);
assert.match(resolverBlock, /resolution_status/);
assert.match(resolverBlock, /matches\.length === 1/);
assert.match(resolverBlock, /keine Monatsdateien verarbeiten/);

const toolBlock = source.slice(start, end);
assert.match(toolBlock, /TOOL_SAFE_WRITE/);
assert.match(toolBlock, /requireMoritz:\s*true/);
assert.match(toolBlock, /assertAllowedAccountingFolder\(folder_id/);
assert.match(toolBlock, /file\.mimeType !== "application\/pdf"/);
assert.match(toolBlock, /file\.parents \|\| \[\]\)\.includes\(folder_id\)/);
assert.match(toolBlock, /deleteGoogleDriveFileWithRetry\(transientFile\.id/);
assert.match(toolBlock, /transient_cleanup_verified/);
assert.match(toolBlock, /ACCOUNTING_OCR_LIVE_MAX_FILES/);
assert.match(toolBlock, /active_transient_document_count/);
assert.match(toolBlock, /Keinen parallelen oder doppelten OCR-Lauf starten/);
assert.match(toolBlock, /Kein Volltext wird ausgegeben oder lokal gespeichert/);
assert.doesNotMatch(toolBlock, /ocrText\s*:/);
assert.match(source, /function accountingAmountsPlausible\(/);
assert.match(source, /amounts_plausible:\s*amountsPlausible/);
assert.match(source, /status:\s*"conflict",\s*safe_for_write:\s*false,\s*amounts_plausible:\s*false/);
assert.match(source, /selection\.amounts_plausible/);
assert.match(source, /\(\?!\[\\d\.,\]\)/);

console.log("accounting OCR contract self-test passed");
