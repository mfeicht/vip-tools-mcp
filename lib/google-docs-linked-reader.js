import { createHash } from "node:crypto";

export function assertLinkedGoogleDocScope({ task, userGid, documentId }) {
  if (!/^[a-zA-Z0-9_-]{20,200}$/.test(String(documentId || ""))) throw new Error("Ungueltige Google-Doc-ID.");
  const involved = [task?.assignee?.gid, task?.created_by?.gid, ...(task?.followers || []).map((person) => person.gid)];
  if (!userGid || !involved.includes(userGid)) throw new Error("Docs-Lesezugriff braucht eigene Asana-Beteiligung.");
  const links = [...String(task?.notes || "").matchAll(/https:\/\/docs\.google\.com\/document\/d\/([a-zA-Z0-9_-]+)(?=[/?#\s]|$)/g)];
  if (!links.some((match) => match[1] === documentId)) throw new Error("Google Doc ist nicht in der aktuellen Asana-Beschreibung verlinkt.");
}

function bodyText(content = []) {
  return content.map((element) => {
    if (element.paragraph) return (element.paragraph.elements || []).map((part) => part.textRun?.content || "").join("");
    if (element.table) return (element.table.tableRows || []).map((row) => (row.tableCells || []).map((cell) => bodyText(cell.content)).join("\t")).join("\n") + "\n";
    if (element.tableOfContents) return bodyText(element.tableOfContents.content);
    return "";
  }).join("");
}

export function linkedGoogleDocReadback(document, { documentId, tabId, startChar = 0, maxChars = 20000 }) {
  if (!Number.isInteger(startChar) || startChar < 0 || !Number.isInteger(maxChars) || maxChars < 1 || maxChars > 50000) throw new Error("Ungueltiges Docs-Textfenster.");
  if (document.documentId !== documentId) throw new Error("Docs-Readback-ID stimmt nicht mit dem Auftrag ueberein.");
  const tabs = [];
  function visit(items) {
    for (const tab of items || []) { tabs.push(tab); visit(tab.childTabs); }
  }
  visit(document.tabs);
  if (!tabs.length) throw new Error("Docs-Readback enthaelt keine explizite Tab-Topologie.");
  const selected = tabId ? tabs.filter((tab) => tab.tabProperties?.tabId === tabId) : tabs;
  if (selected.length !== 1) throw new Error("Google-Doc-Tab ist nicht eindeutig; tab_id explizit waehlen.");
  const tab = selected[0];
  if (!tab.documentTab?.body) throw new Error("Google-Doc-Tab enthaelt keinen Body-Readback.");
  const text = bodyText(tab.documentTab.body.content);
  return {
    document_id: documentId,
    title: document.title,
    revision_id: document.revisionId || null,
    tab_id: tab.tabProperties.tabId,
    tab_title: tab.tabProperties.title,
    tabs: tabs.map((item) => ({ tab_id: item.tabProperties?.tabId, title: item.tabProperties?.title })),
    text: text.slice(startChar, startChar + maxChars),
    start_char: startChar,
    total_chars: text.length,
    text_truncated: startChar > 0 || startChar + maxChars < text.length,
    body_text_sha256: createHash("sha256").update(text, "utf8").digest("hex"),
    content_scope: "selected_tab_body_text_including_tables_and_toc; excludes_images_headers_footers_comments",
    source_url: `https://docs.google.com/document/d/${documentId}/edit?tab=${encodeURIComponent(tab.tabProperties.tabId)}`
  };
}
