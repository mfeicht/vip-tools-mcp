export function createAsanaMaterialCommentCoordinator({
  recentTtlMs = 5 * 60 * 1000,
  now = () => Date.now()
} = {}) {
  const tails = new Map();
  const recentStories = new Map();

  function readRecentStories(key) {
    const cutoff = now() - recentTtlMs;
    const fresh = (recentStories.get(key) || []).filter((entry) => entry.recorded_at >= cutoff);
    if (fresh.length) recentStories.set(key, fresh);
    else recentStories.delete(key);
    return fresh.map((entry) => entry.story);
  }

  function rememberStory(key, story) {
    if (!story?.gid) return;
    const retained = (recentStories.get(key) || []).filter(
      (entry) =>
        entry.recorded_at >= now() - recentTtlMs &&
        String(entry.story?.gid || "") !== String(story.gid)
    );
    recentStories.set(key, [...retained, { story, recorded_at: now() }]);
  }

  async function run(key, action) {
    const normalizedKey = String(key || "").trim();
    if (!normalizedKey) throw new Error("Material-comment coordinator needs a non-empty key.");

    const previous = tails.get(normalizedKey) || Promise.resolve();
    let release;
    const slot = new Promise((resolve) => {
      release = resolve;
    });
    const tail = previous.catch(() => undefined).then(() => slot);
    tails.set(normalizedKey, tail);

    await previous.catch(() => undefined);
    try {
      return await action({
        recentStories: readRecentStories(normalizedKey),
        rememberStory: (story) => rememberStory(normalizedKey, story)
      });
    } finally {
      release();
      if (tails.get(normalizedKey) === tail) tails.delete(normalizedKey);
    }
  }

  return {
    run,
    pendingCount: () => tails.size
  };
}
