import assert from "node:assert/strict";
import test from "node:test";
import JSZip from "jszip";
import {
  copyXlsxWithValues,
  inspectXlsxValueTemplate,
  verifyXlsxValueCopy
} from "./xlsx-value-copy.js";

async function fixture({ existingValue = false, omitB2 = false, row2Xml, noDirectoryEntries = false } = {}) {
  const zip = new JSZip();
  const add = (name, content) => zip.file(name, content, { createFolders: !noDirectoryEntries });
  add("[Content_Types].xml", "<Types><Override PartName=\"/xl/worksheets/sheet1.xml\"/></Types>");
  add("xl/workbook.xml", '<workbook><sheets><sheet name="Übersicht" sheetId="1" r:id="rId1"/><sheet name="Keywordsammlung" sheetId="2" r:id="rId2"/></sheets></workbook>');
  add("xl/_rels/workbook.xml.rels", '<Relationships><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/></Relationships>');
  add("xl/worksheets/sheet1.xml", '<worksheet><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>Do not alter</t></is></c></row></sheetData></worksheet>');
  add("xl/worksheets/sheet2.xml", `<worksheet><dimension ref="A1:J999"/><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>Keyword</t></is></c></row><row r="2" spans="1:10">${row2Xml ?? `<c r="A2" s="7"${existingValue ? ' t="inlineStr"><is><t>Old</t></is></c>' : '/>'}${omitB2 ? '' : '<c r="B2" s="9"/>'}`}</row></sheetData><conditionalFormatting sqref="A2:J999"/><dataValidations count="2"/></worksheet>`);
  add("xl/styles.xml", "<styleSheet><cellXfs count=\"10\"/></styleSheet>");
  add("xl/drawings/drawing1.xml", "<drawing><anchor>Preserve</anchor></drawing>");
  return zip.generateAsync({ type: "nodebuffer" });
}

test("writes only existing keyword values while preserving other sheets and structure", async () => {
  const source = await fixture();
  const before = await inspectXlsxValueTemplate(source);
  assert.deepEqual(before.sheet_names, ["Übersicht", "Keywordsammlung"]);
  assert.equal(before.existing_editable_cell_count, 2);
  const values = [{ address: "A2", value: "Logistics & AI <2026>" }, { address: "B2", value: 84.5 }];
  const result = await copyXlsxWithValues(source, values);
  assert.equal(result.unchanged_part_count, 6);
  assert.equal(result.value_readback_count, 2);
  const readback = await verifyXlsxValueCopy(source, result.bytes, values);
  assert.equal(readback.target_sheet_structure_preserved, true);
  const zip = await JSZip.loadAsync(result.bytes);
  const sheet = await zip.file("xl/worksheets/sheet2.xml").async("string");
  assert.match(sheet, /<c r="A2" s="7" t="inlineStr"><is><t xml:space="preserve">Logistics &amp; AI &lt;2026&gt;<\/t><\/is><\/c>/);
  assert.match(sheet, /<c r="B2" s="9"><v>84\.5<\/v><\/c>/);
});

test("repeated renders of the same source and values have identical output bytes", async () => {
  const source = await fixture({ noDirectoryEntries: true });
  const values = [{ address: "A2", value: "Stable" }, { address: "B2", value: 12 }];
  const first = await copyXlsxWithValues(source, values);
  await new Promise((resolve) => setTimeout(resolve, 2200));
  const second = await copyXlsxWithValues(source, values);
  assert.equal(second.output_sha256, first.output_sha256);
  assert.deepEqual(second.bytes, first.bytes);
  const original = await JSZip.loadAsync(source);
  const output = await JSZip.loadAsync(second.bytes);
  assert.deepEqual(Object.keys(output.files).sort(), Object.keys(original.files).sort());
});

test("refuses overwrites, missing styled cells, duplicate addresses and out-of-range rows", async () => {
  await assert.rejects(copyXlsxWithValues(await fixture({ existingValue: true }), [{ address: "A2", value: "New" }]), /xlsx_existing_cell_value_refused/);
  await assert.rejects(copyXlsxWithValues(await fixture({ omitB2: true }), [{ address: "B2", value: 9 }]), /xlsx_target_cell_missing_from_template/);
  await assert.rejects(copyXlsxWithValues(await fixture(), [{ address: "A2", value: "x" }, { address: "A2", value: "y" }]), /xlsx_duplicate_cell/);
  await assert.rejects(copyXlsxWithValues(await fixture(), [{ address: "K2", value: "x" }]), /xlsx_cell_outside_existing_keyword_rows/);
});

test("readback detects any change to another worksheet or style part", async () => {
  const source = await fixture();
  const values = [{ address: "A2", value: "One" }];
  const result = await copyXlsxWithValues(source, values);
  const zip = await JSZip.loadAsync(result.bytes);
  zip.file("xl/styles.xml", "<styleSheet/>");
  const tampered = await zip.generateAsync({ type: "nodebuffer" });
  await assert.rejects(verifyXlsxValueCopy(source, tampered, values), /xlsx_non_target_part_changed:xl\/styles.xml/);
});

test("readback detects a target cell style change", async () => {
  const source = await fixture();
  const values = [{ address: "A2", value: "One" }];
  const result = await copyXlsxWithValues(source, values);
  const zip = await JSZip.loadAsync(result.bytes);
  const sheet = await zip.file("xl/worksheets/sheet2.xml").async("string");
  zip.file("xl/worksheets/sheet2.xml", sheet.replace('<c r="A2" s="7"', '<c r="A2" s="9"'));
  const tampered = await zip.generateAsync({ type: "nodebuffer" });
  await assert.rejects(verifyXlsxValueCopy(source, tampered, values), /xlsx_target_sheet_structure_changed/);
});

test("refuses self-closing formula nodes even when the cell has no cached value", async () => {
  const source = await fixture({ row2Xml: '<c r="A2" s="7"><f/></c>' });
  await assert.rejects(copyXlsxWithValues(source, [{ address: "A2", value: 1 }]), /xlsx_formula_or_extended_cell_refused/);
});

test("refuses XML comments and CDATA that could masquerade as target cells", async () => {
  for (const markup of ['<!-- <c r="A2"/> -->', '<![CDATA[<c r="A2"/>]]>']) {
    const source = await fixture({ row2Xml: markup });
    await assert.rejects(inspectXlsxValueTemplate(source), /xlsx_complex_xml_markup_refused/);
    await assert.rejects(copyXlsxWithValues(source, [{ address: "A2", value: 1 }]), /xlsx_complex_xml_markup_refused/);
  }
});

test("rejects highly compressed oversized members before CRC inflation", async () => {
  const zip = new JSZip();
  zip.file("xl/worksheets/sheet1.xml", "x".repeat(33 * 1024 * 1024));
  const source = await zip.generateAsync({ type: "nodebuffer", compression: "DEFLATE" });
  await assert.rejects(inspectXlsxValueTemplate(source), /xlsx_inflated_size_limit_exceeded/);
});
