export async function findTemplatedRenderByExternalId({
  templateId,
  externalId,
  fetchPage,
  pageLimit = 100,
  maxPages = 20
}) {
  if (typeof templateId !== "string" || !templateId.trim() ||
      typeof externalId !== "string" || !externalId.trim() ||
      typeof fetchPage !== "function" ||
      !Number.isSafeInteger(pageLimit) || pageLimit < 1 || pageLimit > 100 ||
      !Number.isSafeInteger(maxPages) || maxPages < 1 || maxPages > 20) {
    throw new Error("templated_render_lookup_invalid_input");
  }

  const matches = [];
  const seenIds = new Set();
  let scannedCount = 0;
  for (let page = 0; page < maxPages; page += 1) {
    const renders = await fetchPage({ templateId, externalId, page, limit: pageLimit });
    if (!Array.isArray(renders) || renders.length > pageLimit) {
      throw new Error("templated_render_lookup_invalid_page");
    }
    scannedCount += renders.length;
    for (const render of renders) {
      // Malformed rows cannot support an exhaustive negative finding. Require
      // the provider's documented identity and template binding on every row.
      if (!render || typeof render !== "object" || Array.isArray(render) ||
          typeof render.id !== "string" || !render.id.trim() ||
          typeof render.templateId !== "string" || !render.templateId.trim()) {
        throw new Error("templated_render_lookup_invalid_render");
      }
      if (render.templateId !== templateId) {
        throw new Error("templated_render_lookup_template_mismatch");
      }
      if (seenIds.has(render.id)) throw new Error("templated_render_lookup_repeated_render");
      seenIds.add(render.id);
      for (const field of ["externalId", "external_id"]) {
        if (Object.hasOwn(render, field) && render[field] !== null &&
            typeof render[field] !== "string") {
          throw new Error("templated_render_lookup_invalid_external_id");
        }
      }
      if (Object.hasOwn(render, "externalId") && Object.hasOwn(render, "external_id") &&
          render.externalId !== render.external_id) {
        throw new Error("templated_render_lookup_conflicting_external_id");
      }
      if (render.externalId === externalId || render.external_id === externalId) {
        matches.push(render);
      }
    }
    if (renders.length < pageLimit) {
      if (matches.length > 1) throw new Error("templated_render_lookup_ambiguous");
      return {
        lookup_mode: "template_external_id_full_pagination",
        template_id: templateId,
        external_id: externalId,
        pages_read: page + 1,
        scanned_count: scannedCount,
        match_count: matches.length,
        render: matches[0] || null,
        complete: true
      };
    }
  }
  throw new Error("templated_render_lookup_pagination_limit");
}
