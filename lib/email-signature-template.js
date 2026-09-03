import { load as loadHtml } from "cheerio";

function normalizedIdentity(value) {
  return String(value || "").replace(/\s+/g, " ").trim().toLowerCase();
}

export function stripTrailingIdentityFromText(value, identities = []) {
  const identitySet = new Set(identities.map(normalizedIdentity).filter(Boolean));
  const lines = String(value || "").replace(/\r\n/g, "\n").split("\n");
  while (lines.length && !lines.at(-1).trim()) lines.pop();
  if (lines.length && identitySet.has(normalizedIdentity(lines.at(-1)))) {
    lines.pop();
    while (lines.length && !lines.at(-1).trim()) lines.pop();
  }
  return lines.join("\n").trimEnd();
}

function htmlBodyFragment(value) {
  const html = String(value || "");
  const match = /<body\b[^>]*>([\s\S]*?)<\/body\s*>/i.exec(html);
  return match ? match[1] : html;
}

function htmlHeadStyles(value) {
  return Array.from(String(value || "").matchAll(/<style\b[^>]*>[\s\S]*?<\/style\s*>/gi))
    .map((match) => match[0])
    .join("");
}

export function stripTrailingIdentityFromHtml(value, identities = []) {
  const identitySet = new Set(identities.map(normalizedIdentity).filter(Boolean));
  if (!identitySet.size) return htmlBodyFragment(value);

  const $ = loadHtml(`<div id="vip-email-body-root">${htmlBodyFragment(value)}</div>`, null, false);
  const root = $("#vip-email-body-root");
  const rootNode = root.get(0);
  const textNodes = [];
  const visit = (node) => {
    if (node?.type === "text") textNodes.push(node);
    for (const child of node?.children || []) visit(child);
  };
  visit(rootNode);
  const trailingTextNode = textNodes.findLast((node) => String(node.data || "").trim());
  if (!trailingTextNode || !identitySet.has(normalizedIdentity(trailingTextNode.data))) {
    return root.html() || "";
  }

  let parent = trailingTextNode.parent || null;
  $(trailingTextNode).remove();
  while (parent && parent !== rootNode) {
    const nextParent = parent.parent || null;
    const hasVisibleText = normalizedIdentity($(parent).text());
    const hasVisibleElement = $(parent).find("img,svg,video,audio,hr").length > 0;
    if (hasVisibleText || hasVisibleElement) break;
    $(parent).remove();
    parent = nextParent;
  }
  return root.html() || "";
}

export function composeHtmlWithSignature({
  contentHtml,
  signatureHtml,
  marker,
  trailingIdentities = []
}) {
  const markerCount = String(signatureHtml || "").split(marker).length - 1;
  if (markerCount !== 1) {
    throw new Error(`HTML-Signaturvorlage muss den Einsetzpunkt ${marker} exakt einmal enthalten, gefunden: ${markerCount}.`);
  }
  const content = stripTrailingIdentityFromHtml(contentHtml, trailingIdentities);
  let merged = String(signatureHtml).replace(marker, content);
  const styles = htmlHeadStyles(contentHtml);
  if (styles) {
    merged = /<\/head\s*>/i.test(merged)
      ? merged.replace(/<\/head\s*>/i, `${styles}</head>`)
      : `${styles}${merged}`;
  }
  return {
    html: merged,
    marker_count: markerCount,
    trailing_identity_removed:
      normalizedIdentity(htmlBodyFragment(contentHtml)) !== normalizedIdentity(content)
  };
}
