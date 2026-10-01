export const GENERATED_IMAGE_TRASH_AFTER_DAYS = 7;
export const GENERATED_IMAGE_DELETE_AFTER_TRASH_DAYS = 30;

function parseTime(value) {
  const timestamp = Date.parse(String(value || ""));
  return Number.isFinite(timestamp) ? timestamp : null;
}

export function evaluateGeneratedImageCleanup({
  file,
  post,
  projectKey,
  action,
  now = new Date(),
  trashAfterDays = GENERATED_IMAGE_TRASH_AFTER_DAYS,
  deleteAfterTrashDays = GENERATED_IMAGE_DELETE_AFTER_TRASH_DAYS
}) {
  const reasons = [];
  const nowMs = now instanceof Date ? now.getTime() : parseTime(now);
  const sentAtMs = parseTime(post?.sentAt);
  const createdAtMs = parseTime(file?.createdTime);
  const trashedAtMs = parseTime(file?.appProperties?.vip_trashed_at);
  const dayMs = 24 * 60 * 60 * 1000;

  if (!Number.isFinite(nowMs)) reasons.push("invalid_now");
  if (file?.appProperties?.provider !== "google-gemini") reasons.push("not_gemini_generated");
  if (file?.appProperties?.pipeline !== "gemini-batch-v1") reasons.push("not_batch_pipeline_asset");
  if (file?.appProperties?.project_key !== projectKey) reasons.push("project_mismatch");
  if (post?.status !== "sent") reasons.push("buffer_post_not_sent");
  if (!sentAtMs) reasons.push("buffer_sent_at_missing");
  if (!createdAtMs) reasons.push("drive_created_at_missing");
  if (sentAtMs && createdAtMs && createdAtMs > sentAtMs + dayMs) reasons.push("asset_created_after_publication");

  if (action === "trash") {
    if (file?.trashed) reasons.push("already_trashed");
    if (sentAtMs && nowMs - sentAtMs < trashAfterDays * dayMs) reasons.push("correction_window_active");
    const boundPostId = file?.appProperties?.vip_cleanup_post;
    if (boundPostId && boundPostId !== post?.id) reasons.push("bound_to_other_post");
  } else if (action === "permanent_delete") {
    if (!file?.trashed) reasons.push("not_trashed");
    if (file?.appProperties?.vip_cleanup_post !== post?.id) reasons.push("post_binding_missing_or_mismatch");
    if (file?.appProperties?.vip_cleanup_project !== projectKey) reasons.push("cleanup_project_mismatch");
    if (!trashedAtMs) reasons.push("trashed_at_missing");
    if (trashedAtMs && nowMs - trashedAtMs < deleteAfterTrashDays * dayMs) {
      reasons.push("trash_retention_window_active");
    }
  } else {
    reasons.push("unsupported_action");
  }

  return {
    eligible: reasons.length === 0,
    reasons,
    action,
    project_key: projectKey,
    file_id: file?.id || null,
    buffer_post_id: post?.id || null,
    buffer_status: post?.status || null,
    buffer_sent_at: post?.sentAt || null,
    drive_created_at: file?.createdTime || null,
    drive_trashed: Boolean(file?.trashed),
    drive_trashed_at: file?.appProperties?.vip_trashed_at || null,
    trash_after_days: trashAfterDays,
    permanent_delete_after_trash_days: deleteAfterTrashDays
  };
}

