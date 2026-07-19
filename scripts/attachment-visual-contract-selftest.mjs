import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const source = await readFile(new URL("../server.js", import.meta.url), "utf8");
const start = source.indexOf('server.tool(\n    "asana_attachment_fetch_chunked"');
const end = source.indexOf('server.tool(\n    "asana_attachment_copy_to_drive"', start);

assert.notEqual(start, -1, "asana_attachment_fetch_chunked tool is missing");
assert.notEqual(end, -1, "asana_attachment_fetch_chunked tool boundary is missing");

const toolBlock = source.slice(start, end);

assert.match(toolBlock, /signed_download_url:\s*attachment\.download_url \|\| null/);
assert.match(toolBlock, /preferred_visual_workflow:/);
assert.match(toolBlock, /Dateityp und sha256 pruefen/);
assert.match(toolBlock, /visuellen Tool in hoher\/originaler Detailstufe/);
assert.match(toolBlock, /Nur wenn der signierte Direktdownload scheitert/);

console.log("attachment visual contract self-test passed");

