function normalizeLine(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/ß/g, "ss")
    .toLowerCase();
}

export function taskRequiresLocalWordPressImporter(notes) {
  const lines = normalizeLine(notes).split(/\r?\n/);
  return lines.some((line) =>
    /\bmcp\b/.test(line) && /\bkein(?:e|en|em|er|es)?\b/.test(line) && /\b(import|wordpress|wp|mcp)\b/.test(line)
  ) || lines.some((line) =>
    /\blokal(?:e|es|en|er|em)?\b/.test(line) && /\bskript\b/.test(line) && /\b(ausschliesslich|pflichtimportweg)\b/.test(line)
  );
}

export function assertMcpImportAllowedByTaskNotes(notes, taskGid) {
  if (taskRequiresLocalWordPressImporter(notes)) {
    throw new Error(
      `wp_import_csv: Asana-Aufgabe ${taskGid} schreibt den lokalen WordPress-Importer vor; MCP-Import ist fuer diese Aufgabe gesperrt.`
    );
  }
}
