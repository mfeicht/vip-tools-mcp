import { createHash } from "node:crypto";
import path from "node:path";
import { Readable } from "node:stream";
import JSZip from "jszip";

export const XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
export const XLSX_MAX_BYTES = 5 * 1024 * 1024;
const MAX_PARTS = 250;
const MAX_INFLATED_BYTES = 32 * 1024 * 1024;
const CELL_RE = /<c\b[^>]*?(?:\/>|>[\s\S]*?<\/c>)/g;
const ROW_RE = /<row\b([^>]*)>([\s\S]*?)<\/row>/g;

function assertSimpleXml(xml) {
  // Regex-based cell editing must never interpret markup inside comments or CDATA as live cells.
  if (/<!--|<!\[CDATA\[|<!DOCTYPE\b|<!ENTITY\b/i.test(xml)) {
    throw new Error("xlsx_complex_xml_markup_refused");
  }
}

export function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function attributes(tag) {
  return Object.fromEntries([...tag.matchAll(/([A-Za-z_:][\w:.-]*)="([^"]*)"/g)].map((match) => [match[1], match[2]]));
}

function xmlEscape(value) {
  return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;").replaceAll("'", "&apos;");
}

function xmlUnescape(value) {
  return value.replace(/&(amp|lt|gt|quot|apos);/g, (_match, entity) => ({
    amp: "&", lt: "<", gt: ">", quot: '"', apos: "'"
  })[entity]);
}

export function normalizeXlsxValueCells(cells) {
  if (!Array.isArray(cells) || !cells.length || cells.length > 500) {
    throw new Error("xlsx_cells_count_out_of_range");
  }
  const result = new Map();
  for (const cell of cells) {
    const address = String(cell?.address || "").toUpperCase();
    const match = /^([A-J])([1-9][0-9]{0,2})$/.exec(address);
    if (!match || Number(match[2]) < 2 || Number(match[2]) > 999) {
      throw new Error(`xlsx_cell_outside_existing_keyword_rows:${address}`);
    }
    if (result.has(address)) throw new Error(`xlsx_duplicate_cell:${address}`);
    const value = cell?.value;
    if (typeof value === "string") {
      if (!value || value.length > 1000 || /[\u0000-\u0008\u000B\u000C\u000E-\u001F]/.test(value)) {
        throw new Error(`xlsx_cell_text_invalid:${address}`);
      }
    } else if (typeof value !== "number" || !Number.isFinite(value)) {
      throw new Error(`xlsx_cell_value_invalid:${address}`);
    }
    result.set(address, value);
  }
  return result;
}

async function readParts(bytes) {
  if (!Buffer.isBuffer(bytes) || bytes.length < 100 || bytes.length > XLSX_MAX_BYTES ||
      bytes.subarray(0, 4).toString("binary") !== "PK\u0003\u0004") {
    throw new Error("xlsx_source_size_or_zip_signature_invalid");
  }
  // JSZip's CRC option inflates every member during load. Inspect sizes first and
  // enforce a streaming cap before asking it to run the full CRC pass.
  const zip = await JSZip.loadAsync(bytes, { checkCRC32: false });
  const names = Object.keys(zip.files).filter((name) => !zip.files[name].dir).sort();
  if (!names.length || names.length > MAX_PARTS || names.some((name) => name.startsWith("/") || name.includes(".."))) {
    throw new Error("xlsx_archive_parts_invalid");
  }
  const parts = new Map();
  let inflated = 0;
  for (const name of names) {
    const declared = zip.files[name]._data?.uncompressedSize;
    if (!Number.isSafeInteger(declared) || declared < 0 || inflated + declared > MAX_INFLATED_BYTES) {
      throw new Error("xlsx_inflated_size_limit_exceeded");
    }
    const chunks = [];
    let actual = 0;
    const stream = new Readable({ read() {} }).wrap(zip.files[name].nodeStream("nodebuffer"));
    for await (const chunk of stream) {
      actual += chunk.length;
      if (inflated + actual > MAX_INFLATED_BYTES) {
        throw new Error("xlsx_inflated_size_limit_exceeded");
      }
      chunks.push(chunk);
    }
    if (actual !== declared) throw new Error("xlsx_inflated_size_mismatch");
    inflated += actual;
    parts.set(name, Buffer.concat(chunks, actual));
  }
  await JSZip.loadAsync(bytes, { checkCRC32: true });
  return { zip, parts };
}

function locateSheet(parts, sheetName) {
  const workbook = parts.get("xl/workbook.xml")?.toString("utf8");
  const relationships = parts.get("xl/_rels/workbook.xml.rels")?.toString("utf8");
  if (!workbook || !relationships) throw new Error("xlsx_workbook_or_relationships_missing");
  assertSimpleXml(workbook);
  assertSimpleXml(relationships);
  const sheetTags = [...workbook.matchAll(/<sheet\b[^>]*\/?\s*>/g)];
  const sheets = sheetTags.map((match) => attributes(match[0]));
  const matches = sheets.filter((sheet) => xmlUnescape(sheet.name || "") === sheetName);
  if (matches.length !== 1 || !matches[0]["r:id"]) throw new Error("xlsx_target_sheet_not_unique");
  const relationTags = [...relationships.matchAll(/<Relationship\b[^>]*\/?\s*>/g)];
  const relation = relationTags.map((match) => attributes(match[0]))
    .filter((item) => item.Id === matches[0]["r:id"] && /\/worksheet$/.test(item.Type || ""));
  if (relation.length !== 1) throw new Error("xlsx_sheet_relationship_invalid");
  const target = relation[0].Target || "";
  const file = target.startsWith("/") ? target.slice(1) : path.posix.normalize(path.posix.join("xl", target));
  if (!/^xl\/worksheets\/[^/]+\.xml$/.test(file) || !parts.has(file)) {
    throw new Error("xlsx_target_sheet_path_invalid");
  }
  return { sheetNames: sheets.map((sheet) => xmlUnescape(sheet.name || "")), sheetPath: file };
}

function cellOpening(cellXml) {
  return /^<c\b[^>]*?(?:\/>|>)/.exec(cellXml)?.[0] || "";
}

function collectCells(sheetXml) {
  assertSimpleXml(sheetXml);
  const found = new Map();
  for (const row of sheetXml.matchAll(ROW_RE)) {
    const rowNumber = Number(attributes(row[0].slice(0, row[0].indexOf(">") + 1)).r);
    if (!Number.isInteger(rowNumber)) throw new Error("xlsx_row_number_invalid");
    for (const cell of row[2].matchAll(CELL_RE)) {
      const address = attributes(cellOpening(cell[0])).r;
      if (!address || Number(address.match(/\d+$/)?.[0]) !== rowNumber || found.has(address)) {
        throw new Error("xlsx_cell_reference_invalid_or_duplicate");
      }
      found.set(address, cell[0]);
    }
  }
  return found;
}

function replaceValueCell(cellXml, value) {
  if (/<f(?:\s|\/|>)/.test(cellXml) || /<(?:extLst|phoneticPr)(?:\s|\/|>)/.test(cellXml)) {
    throw new Error("xlsx_formula_or_extended_cell_refused");
  }
  if (/<v>(?!<\/v>)[\s\S]+?<\/v>/.test(cellXml) || /<t(?:\s[^>]*)?>(?!<\/t>)[\s\S]+?<\/t>/.test(cellXml)) {
    throw new Error("xlsx_existing_cell_value_refused");
  }
  let opening = cellOpening(cellXml);
  if (!opening) throw new Error("xlsx_cell_opening_invalid");
  opening = opening.replace(/\s+t="[^"]*"/, "").replace(/\s*\/?>$/, "");
  if (typeof value === "number") return `${opening}><v>${value}</v></c>`;
  return `${opening} t="inlineStr"><is><t xml:space="preserve">${xmlEscape(value)}</t></is></c>`;
}

function replaceCells(sheetXml, values) {
  assertSimpleXml(sheetXml);
  const seen = new Set();
  const updated = sheetXml.replace(ROW_RE, (rowXml, rowAttributes, inner) => {
    const rowNumber = Number(attributes(`<row${rowAttributes}>`).r);
    const nextInner = inner.replace(CELL_RE, (cellXml) => {
      const address = attributes(cellOpening(cellXml)).r;
      if (!values.has(address)) return cellXml;
      if (Number(address.match(/\d+$/)?.[0]) !== rowNumber || seen.has(address)) {
        throw new Error("xlsx_target_cell_row_or_duplicate_mismatch");
      }
      seen.add(address);
      return replaceValueCell(cellXml, values.get(address));
    });
    return `<row${rowAttributes}>${nextInner}</row>`;
  });
  if (seen.size !== values.size) throw new Error("xlsx_target_cell_missing_from_template");
  return updated;
}

function maskValueCells(sheetXml, values) {
  return sheetXml.replace(CELL_RE, (cellXml) => {
    const address = attributes(cellOpening(cellXml)).r;
    if (!values.has(address)) return cellXml;
    const opening = cellOpening(cellXml).replace(/\s+t="[^"]*"/, "").replace(/\s*\/?>$/, "");
    return `${opening}/>`;
  });
}

function verifyParts(before, after, sheetPath, values) {
  const namesBefore = [...before.keys()].sort();
  const namesAfter = [...after.keys()].sort();
  if (JSON.stringify(namesBefore) !== JSON.stringify(namesAfter)) throw new Error("xlsx_archive_parts_changed");
  for (const name of namesBefore) {
    if (name === sheetPath) continue;
    if (!before.get(name).equals(after.get(name))) throw new Error(`xlsx_non_target_part_changed:${name}`);
  }
  const originalXml = before.get(sheetPath).toString("utf8");
  const updatedXml = after.get(sheetPath).toString("utf8");
  assertSimpleXml(originalXml);
  assertSimpleXml(updatedXml);
  if (maskValueCells(originalXml, values) !== maskValueCells(updatedXml, values)) {
    throw new Error("xlsx_target_sheet_structure_changed");
  }
  const updatedCells = collectCells(updatedXml);
  for (const [address, value] of values) {
    const cell = updatedCells.get(address) || "";
    if (typeof value === "number") {
      if (attributes(cellOpening(cell)).t || !cell.includes(`<v>${value}</v>`)) {
        throw new Error(`xlsx_numeric_readback_mismatch:${address}`);
      }
    } else {
      const expected = `<is><t xml:space="preserve">${xmlEscape(value)}</t></is>`;
      if (attributes(cellOpening(cell)).t !== "inlineStr" || !cell.includes(expected)) {
        throw new Error(`xlsx_text_readback_mismatch:${address}`);
      }
    }
  }
  return { unchanged_part_count: namesBefore.length - 1, target_sheet_structure_preserved: true, value_readback_count: values.size };
}

export async function inspectXlsxValueTemplate(sourceBytes, sheetName = "Keywordsammlung") {
  const { parts } = await readParts(sourceBytes);
  const { sheetNames, sheetPath } = locateSheet(parts, sheetName);
  const cells = collectCells(parts.get(sheetPath).toString("utf8"));
  const editableCells = [...cells.keys()].filter((address) => /^[A-J]([1-9][0-9]{0,2})$/.test(address) &&
    Number(address.match(/\d+$/)?.[0]) >= 2 && Number(address.match(/\d+$/)?.[0]) <= 999);
  return {
    sheet_names: sheetNames,
    target_sheet_path: sheetPath,
    archive_part_count: parts.size,
    existing_editable_cell_count: editableCells.length,
    existing_editable_cell_range: editableCells.length ? [editableCells[0], editableCells.at(-1)] : [],
    source_sha256: sha256(sourceBytes)
  };
}

export async function copyXlsxWithValues(sourceBytes, cells, sheetName = "Keywordsammlung") {
  const values = normalizeXlsxValueCells(cells);
  const { zip, parts } = await readParts(sourceBytes);
  const { sheetPath } = locateSheet(parts, sheetName);
  const updatedXml = replaceCells(parts.get(sheetPath).toString("utf8"), values);
  zip.file(sheetPath, updatedXml, { date: zip.files[sheetPath].date });
  const outputBytes = await zip.generateAsync({ type: "nodebuffer", compression: "DEFLATE", compressionOptions: { level: 6 } });
  const after = await readParts(outputBytes);
  const checks = verifyParts(parts, after.parts, sheetPath, values);
  return { bytes: outputBytes, sheetPath, ...checks, output_sha256: sha256(outputBytes) };
}

export async function verifyXlsxValueCopy(sourceBytes, outputBytes, cells, sheetName = "Keywordsammlung") {
  const values = normalizeXlsxValueCells(cells);
  const before = await readParts(sourceBytes);
  const after = await readParts(outputBytes);
  const first = locateSheet(before.parts, sheetName);
  const second = locateSheet(after.parts, sheetName);
  if (first.sheetPath !== second.sheetPath || JSON.stringify(first.sheetNames) !== JSON.stringify(second.sheetNames)) {
    throw new Error("xlsx_sheet_mapping_changed");
  }
  return verifyParts(before.parts, after.parts, first.sheetPath, values);
}
