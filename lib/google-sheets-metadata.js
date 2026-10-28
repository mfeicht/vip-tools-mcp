// Fixed field mask deliberately excludes cells, formulas, notes and data sources.
export const SHEETS_METADATA_FIELDS =
  'spreadsheetId,properties(title,locale,timeZone),sheets(properties(sheetId,title,index,sheetType,hidden,gridProperties(rowCount,columnCount)))';

export async function readSpreadsheetMetadata({ spreadsheetId, agentId, request }) {
  if (typeof spreadsheetId !== 'string' || !/^[A-Za-z0-9_-]{1,256}$/.test(spreadsheetId)) {
    throw new Error('Ungültige Spreadsheet-ID.');
  }
  if (typeof agentId !== 'string' || !agentId) throw new Error('Explizite Agentenidentität erforderlich.');
  const { data } = await request({
    method: 'GET',
    url: `https://sheets.googleapis.com/v4/spreadsheets/${encodeURIComponent(spreadsheetId)}`,
    params: { includeGridData: false, fields: SHEETS_METADATA_FIELDS },
    timeout: 30000
  }, { agent_id: agentId });
  if (data?.spreadsheetId !== spreadsheetId || typeof data.properties?.title !== 'string' ||
      !Array.isArray(data.sheets) || data.sheets.length === 0) {
    throw new Error('Unvollständiger oder falsch gebundener Spreadsheet-Metadaten-Readback.');
  }
  const ids = new Set(), titles = new Set();
  const sheets = data.sheets.map(row => {
    const p = row?.properties;
    if (!p || !Number.isSafeInteger(p.sheetId) || p.sheetId < 0 ||
        typeof p.title !== 'string' || !p.title || ids.has(p.sheetId) || titles.has(p.title)) {
      throw new Error('Unvollständige oder doppelte Tab-Identität im Metadaten-Readback.');
    }
    ids.add(p.sheetId); titles.add(p.title);
    // Return only named metadata even if an upstream implementation ignores the mask.
    return { sheet_id: p.sheetId, title: p.title, index: p.index,
      sheet_type: p.sheetType, hidden: p.hidden ?? false,
      row_count: p.gridProperties?.rowCount, column_count: p.gridProperties?.columnCount };
  });
  return { agent_id: agentId, spreadsheet_id: spreadsheetId,
    spreadsheet_title: data.properties.title, locale: data.properties.locale,
    time_zone: data.properties.timeZone, sheet_count: sheets.length, sheets };
}
