const LEGACY_ASANA_SEARCH_PARAM_ALIASES = {
  modified_since: "modified_at.after",
  assignee_any: "assignee.any",
  followers_any: "followers.any",
  involved_any: "involved.any"
};

export function buildAsanaTaskSearchParams({
  extraParams = {},
  text,
  modifiedSince,
  assignee,
  followers,
  involved,
  completed,
  sortBy,
  sortAscending,
  limit,
  optFields
}) {
  const params = { ...extraParams };

  for (const [legacyName, canonicalName] of Object.entries(LEGACY_ASANA_SEARCH_PARAM_ALIASES)) {
    if (params[canonicalName] === undefined && params[legacyName] !== undefined) {
      params[canonicalName] = params[legacyName];
    }
    delete params[legacyName];
  }

  params.limit = limit;
  params.opt_fields = optFields;
  params.sort_by = sortBy;

  if (sortAscending !== undefined) params.sort_ascending = sortAscending;
  if (completed !== undefined) params.completed = completed;
  if (text !== undefined) params.text = text;
  if (modifiedSince !== undefined) params["modified_at.after"] = modifiedSince;
  if (assignee !== undefined) params["assignee.any"] = assignee;
  if (followers !== undefined) params["followers.any"] = followers;
  if (involved !== undefined) params["involved.any"] = involved;

  return params;
}
