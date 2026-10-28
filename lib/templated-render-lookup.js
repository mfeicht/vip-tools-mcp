export async function findTemplatedRenderByExternalId({
  templateId,
  externalId,
  fetchPage,
  pageLimit = 100,
  maxPages = 20
}) {
  if (!templateId || !externalId || typeof fetchPage !== "function") {
    throw new Error("templated_render_lookup_invalid_input");
  }

  const matches = [];
  let scannedCount = 0;
  for (let page = 0; page < maxPages; page += 1) {
    const renders = await fetchPage({ templateId, externalId, page, limit: pageLimit });
    if (!Array.isArray(renders) || renders.length > pageLimit) {
      throw new Error("templated_render_lookup_invalid_page");
    }
    scannedCount += renders.length;
    for (const render of renders) {
      if (render?.externalId === externalId || render?.external_id === externalId) {
        if (render.templateId && render.templateId !== templateId) {
          throw new Error("templated_render_lookup_template_mismatch");
        }
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
