import assert from "node:assert/strict";
import {
  assertMcpImportAllowedByTaskNotes,
  taskRequiresLocalWordPressImporter
} from "../lib/wp-import-path-guard.js";

const localOnly = [
  "- Nutze fuer den WordPress-Import keinen MCP und keinen alternativen Importweg.",
  "- Pflichtimportweg bleibt ausschliesslich das lokale Skript: vip_ai_sales_wp_import_csv.py",
  "- Nutze für externe POST-Requests ausschließlich das lokale Skript."
];
for (const notes of localOnly) {
  assert.equal(taskRequiresLocalWordPressImporter(notes), true);
  assert.throws(() => assertMcpImportAllowedByTaskNotes(notes, "123"), /MCP-Import.*gesperrt/);
}

const remoteAllowed = [
  "WordPress-Import ueber Remote-MCP wp_import_csv nach dry_run und Asana-Readback.",
  "Das lokale Skript ist nur ein Operations-Fallback; Standard ist Remote-MCP.",
  "Keine neuen Leads ohne Partner-Gate importieren."
];
for (const notes of remoteAllowed) {
  assert.equal(taskRequiresLocalWordPressImporter(notes), false);
  assert.doesNotThrow(() => assertMcpImportAllowedByTaskNotes(notes, "123"));
}

console.log("wp-import-path-guard selftest: 6 cases passed");
