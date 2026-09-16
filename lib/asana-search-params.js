const ALIASES = {
  modified_since: "modified_at.after", assignee_any: "assignee.any",
  followers_any: "followers.any", created_by_any: "created_by.any", involved_any: "involved.any"
};
const ROLES = ["assignee", "created_by", "followers"];
const SORT_FIELDS = {
  modified_at: "modified_at", created_at: "created_at", completed_at: "completed_at",
  due_date: "due_at,due_on", likes: "num_likes"
};

export function normalizeAsanaSearchUsers(value, meGid, label = "user selector") {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || !value.trim()) throw new Error(`${label}: GID oder me erwartet.`);
  const gids = value.split(",").map((part) => {
    const gid = part.trim() === "me" ? meGid : part.trim();
    if (!/^\d+$/.test(gid || "")) throw new Error(`${label}: ungueltige User-GID.`);
    return gid;
  });
  if (gids.length > 25) throw new Error(`${label}: maximal 25 User-GIDs.`);
  return [...new Set(gids)].join(",");
}

export function buildAsanaTaskSearchPlan({
  extraParams = {}, text, modifiedSince, assignee, followers, involved,
  completed, sortBy = "modified_at", sortAscending, limit = 20,
  optFields = "gid,name", meGid
}) {
  const params = { ...extraParams };
  for (const [alias, canonical] of Object.entries(ALIASES)) {
    if (params[canonical] === undefined && params[alias] !== undefined) params[canonical] = params[alias];
    delete params[alias];
  }
  Object.assign(params, { limit, opt_fields: optFields, sort_by: sortBy });
  if (sortAscending !== undefined) params.sort_ascending = sortAscending;
  if (completed !== undefined) params.completed = completed;
  if (text !== undefined) params.text = text;
  if (modifiedSince !== undefined) params["modified_at.after"] = modifiedSince;
  if (assignee !== undefined) params["assignee.any"] = assignee;
  if (followers !== undefined) params["followers.any"] = followers;
  if (involved !== undefined) params["involved.any"] = involved;
  const involvedUsers = normalizeAsanaSearchUsers(params["involved.any"], meGid, "involved_any");
  delete params["involved.any"];
  for (const role of ROLES) for (const suffix of ["any", "not"]) {
    const key = `${role}.${suffix}`;
    if (params[key] !== undefined) params[key] = normalizeAsanaSearchUsers(params[key], meGid, key);
  }
  if (!involvedUsers) return { params, involved_user_gids: [], branches: [] };
  if (!Object.hasOwn(SORT_FIELDS, sortBy)) throw new Error("involved_any: sort_by=relevance oder unbekannte Sortierung ist fuer eine OR-Vereinigung nicht vergleichbar.");
  if (params.offset !== undefined) throw new Error("Asana Search unterstuetzt keine Offset-Pagination.");
  for (const key of ["created_at.after", "created_at.before"]) {
    if (params[key] !== undefined && !Number.isFinite(Date.parse(params[key]))) throw new Error(`${key}: ungueltiger Zeitstempel.`);
  }
  params.opt_fields = [...new Set([
    ...optFields.split(","), "gid", "created_at", "modified_at", "assignee.gid",
    "created_by.gid", "followers.gid", ...SORT_FIELDS[sortBy].split(",")
  ].map((field) => field.trim()).filter(Boolean))].join(",");
  const gids = involvedUsers.split(",");
  const userConstraints = Object.fromEntries(Object.entries(params).filter(([key]) => ROLES.some((role) => key === `${role}.any` || key === `${role}.not`)));
  const branches = ROLES.flatMap((role) => {
    const key = `${role}.any`;
    // Existing selectors remain AND constraints; never overwrite them with a broader branch.
    let users = params[key] === undefined ? gids : gids.filter((gid) => params[key].split(",").includes(gid));
    if (params[`${role}.not`] !== undefined) users = users.filter((gid) => !params[`${role}.not`].split(",").includes(gid));
    const branchParams = { ...params };
    for (const constraint of Object.keys(userConstraints)) delete branchParams[constraint];
    // A live control returned an intersection for comma-separated followers.any.
    // Query each user separately; the adapter owns the documented OR definition.
    return (users.length ? users.map((gid) => [gid]) : [[]]).map((userGids) => ({
      role, user_gids: userGids, params: { ...branchParams, [key]: userGids.join(",") }
    }));
  });
  return { params, involved_user_gids: gids, branches, user_constraints: userConstraints };
}

function matchesRole(task, branch) {
  const members = branch.role === "followers" ? task.followers?.map((user) => user.gid) : [task[branch.role]?.gid];
  return members?.some((gid) => branch.user_gids.includes(gid));
}

function matchesUserConstraints(task, constraints) {
  return Object.entries(constraints).every(([key, users]) => {
    const [role, suffix] = key.split(".");
    const members = role === "followers" ? task.followers.map((user) => user.gid) : [task[role]?.gid];
    const intersects = members.some((gid) => users.split(",").includes(gid));
    return suffix === "any" ? intersects : !intersects;
  });
}

function sortValue(task, sortBy) {
  if (sortBy === "likes") return task.num_likes ?? 0;
  const value = sortBy === "due_date" ? task.due_at || task.due_on : task[sortBy];
  return value == null ? null : Date.parse(value);
}

export async function executeAsanaInvolvedSearch(plan, fetchPage, { maxPagesPerBranch = 5, pageSize = 100 } = {}) {
  if (!Number.isInteger(maxPagesPerBranch) || maxPagesPerBranch < 1 || maxPagesPerBranch > 10) throw new Error("max_pages_per_branch muss 1..10 sein.");
  if (!Number.isInteger(pageSize) || pageSize < 2 || pageSize > 100) throw new Error("pageSize muss 2..100 sein.");
  const union = new Map();
  const branches = [];
  let totalPages = 0;
  const totalPageBudget = 30;
  for (const branch of plan.branches) {
    const state = { role: branch.role, user_gids: branch.user_gids, pages: 0, matched_count: 0, complete: false, status: "page_limit" };
    const seen = new Set();
    let cursor;
    if (!branch.user_gids.length) Object.assign(state, { complete: true, status: "empty_intersection" });
    else for (let page = 0; page < maxPagesPerBranch; page++) {
      if (totalPages >= totalPageBudget) { state.status = "total_page_limit"; break; }
      const params = { ...branch.params, sort_by: "created_at", sort_ascending: true, limit: pageSize };
      if (cursor !== undefined) {
        // Overlap by 1ms: saturated equal timestamps stop as partial instead of being skipped.
        const originalAfter = Date.parse(branch.params["created_at.after"]);
        params["created_at.after"] = new Date(Math.max(Number.isFinite(originalAfter) ? originalAfter : -Infinity, cursor - 1)).toISOString();
      }
      const tasks = await fetchPage(params);
      state.pages++;
      totalPages++;
      if (!Array.isArray(tasks) || tasks.length > pageSize) throw new Error("Ungueltige Asana-Search-Seite.");
      let invalid = false;
      let newest = -Infinity;
      let previous = -Infinity;
      const pageGids = new Set();
      for (const task of tasks) {
        const created = Date.parse(task.created_at);
        const sorted = sortValue(task, plan.params.sort_by);
        if (!/^\d+$/.test(task.gid || "") || pageGids.has(task.gid) || !Number.isFinite(Date.parse(task.modified_at)) || !Number.isFinite(created) || created < previous ||
            (cursor !== undefined && created < cursor) || !matchesRole(task, branch) ||
            Object.keys(plan.user_constraints).some((key) => !Object.hasOwn(task, key.split(".")[0])) ||
            (sorted !== null && !Number.isFinite(sorted))) {
          invalid = true;
          continue;
        }
        previous = created;
        pageGids.add(task.gid);
        newest = Math.max(newest, created);
        if (!matchesUserConstraints(task, plan.user_constraints)) continue;
        seen.add(task.gid);
        const existing = union.get(task.gid);
        if (!existing || Date.parse(task.modified_at) > Date.parse(existing.modified_at)) union.set(task.gid, task);
      }
      state.matched_count = seen.size;
      if (invalid) { state.status = "invalid_membership_or_page"; break; }
      if (tasks.length < pageSize) { Object.assign(state, { complete: true, status: "complete" }); break; }
      if (cursor !== undefined && newest <= cursor) { state.status = "ambiguous_timestamp_boundary"; break; }
      cursor = newest;
    }
    branches.push(state);
  }
  const sortBy = plan.params.sort_by;
  const ascending = plan.params.sort_ascending === true;
  const tasks = [...union.values()].sort((a, b) => {
    const left = sortValue(a, sortBy);
    const right = sortValue(b, sortBy);
    if (left === null && right !== null) return 1;
    if (right === null && left !== null) return -1;
    const order = left === right ? 0 : (left < right ? -1 : 1) * (ascending ? 1 : -1);
    return order || (BigInt(a.gid) < BigInt(b.gid) ? -1 : BigInt(a.gid) > BigInt(b.gid) ? 1 : 0);
  });
  const complete = branches.every((branch) => branch.complete);
  const truncated = tasks.length > plan.params.limit;
  return {
    search_status: complete ? "ok" : "partial", search_complete: complete,
    result_truncated: truncated, delta_cursor_advance_allowed: complete && !truncated,
    membership_definition: "assignee_or_creator_or_follower",
    pagination: "created_at_ascending_with_boundary_overlap",
    consistency: "eventually_consistent_search_not_atomic_snapshot",
    involved_user_gids: plan.involved_user_gids, branches,
    total_pages: totalPages, total_page_budget: totalPageBudget,
    matched_count: tasks.length, tasks: tasks.slice(0, plan.params.limit)
  };
}
