/** Resolve a Templated template's internal id through the unfiltered list. */
export async function findTemplatedTemplateByInternalId({
  templateId,
  fetchPage,
  limit = 50,
  maxPages = 100
}) {
  if (!templateId || typeof fetchPage !== "function") {
    throw new TypeError("template_id_and_fetch_page_required");
  }
  let found = null;
  let scanned = 0;
  for (let page = 0; page < maxPages; page += 1) {
    const items = await fetchPage({ page, limit, includeLayers: true, includePages: true });
    if (!Array.isArray(items) || items.length > limit) {
      throw new Error("templated_template_page_invalid");
    }
    scanned += items.length;
    for (const item of items) {
      if (item?.id !== templateId) continue;
      if (found) throw new Error("templated_internal_id_duplicate");
      found = item;
    }
    // Require the terminal empty page even after a short page. A short page
    // alone does not prove that a provider has no further results.
    if (items.length === 0) {
      return {
        found: Boolean(found),
        template: found,
        pages_scanned: page + 1,
        templates_scanned: scanned,
        pagination_complete: true
      };
    }
  }
  throw new Error("templated_template_scan_incomplete");
}
