import { createHash } from "node:crypto";

function coordinationError(code, message, cause) {
  const error = new Error(message || code);
  error.code = code;
  if (cause) error.cause = cause;
  return error;
}

export function normalizeMaterialCommentProbe(value) {
  return String(value || "")
    .replace(/<a\s+data-asana-gid="\d+"\s*\/>/gi, " ")
    .replace(/<\/li>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&#(\d+);/g, (_match, code) => String.fromCodePoint(Number(code)))
    .replace(/&quot;/g, '"')
    .replace(/&apos;|&#039;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ")
    .trim();
}

export function hashMaterialCommentProbe(value) {
  return createHash("sha256")
    .update(normalizeMaterialCommentProbe(value), "utf8")
    .digest("hex");
}

export function findMaterialCommentStoryByProbe(stories, claim) {
  const expected = normalizeMaterialCommentProbe(claim?.payload_probe);
  if (!expected) return null;
  return (stories || []).find((story) => {
    const visible = normalizeMaterialCommentProbe(story?.text || story?.html_text);
    return visible.includes(expected);
  }) || null;
}

export function createAsanaMaterialCommentCoordinator({
  recentTtlMs = 5 * 60 * 1000,
  now = () => Date.now(),
  distributedStore = null,
  distributedRequired = false
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

  async function runLocal(normalizedKey, action, context = {}) {
    return action({
      recentStories: readRecentStories(normalizedKey),
      rememberStory: (story) => rememberStory(normalizedKey, story),
      beforePost: async () => ({ status: "process_local_only" }),
      coordination: context
    });
  }

  async function runDistributed(normalizedKey, action, options) {
    const payloadHash = String(options.payloadHash || "").trim();
    const payloadProbe = normalizeMaterialCommentProbe(options.payloadProbe);
    if (!payloadHash || !payloadProbe) {
      throw coordinationError(
        "MATERIAL_COMMENT_COORDINATION_INPUT_INVALID",
        "Distributed material-comment coordination needs payloadHash and payloadProbe."
      );
    }
    if (typeof options.readTargetStories !== "function") {
      throw coordinationError(
        "MATERIAL_COMMENT_COORDINATION_INPUT_INVALID",
        "Distributed material-comment coordination needs a target-system readback."
      );
    }
    const matchTargetStory = options.matchTargetStory || findMaterialCommentStoryByProbe;
    const recoverResult = options.recoverResult || ((story) => ({ recovered_story: story }));

    const readAndMatch = async (claim) => {
      const stories = await options.readTargetStories();
      return {
        stories,
        story: matchTargetStory(stories, claim)
      };
    };

    const existingReceipt = await distributedStore.readReceipt({
      key: normalizedKey,
      payloadHash
    });
    if (existingReceipt) {
      const { story } = await readAndMatch({ payload_probe: payloadProbe });
      if (!story || String(story.gid || "") !== String(existingReceipt.story_gid || "")) {
        throw coordinationError(
          "MATERIAL_COMMENT_RECEIPT_READBACK_MISMATCH",
          "Stored material-comment receipt has no matching target-system story; fail closed."
        );
      }
      rememberStory(normalizedKey, story);
      return recoverResult(story, {
        status: "recovered_from_receipt",
        fence: existingReceipt.fence,
        receipt: existingReceipt
      });
    }

    let acquired = await distributedStore.acquire({
      key: normalizedKey,
      payloadHash,
      payloadProbe
    });

    if (!acquired.acquired) {
      const blockingClaim = acquired.claim;
      if (!blockingClaim) {
        throw coordinationError(
          "MATERIAL_COMMENT_CLAIM_UNCLEAR",
          "Material-comment claim exists but cannot be read; fail closed."
        );
      }
      const { story } = await readAndMatch(blockingClaim);
      if (story) {
        const reconciled = await distributedStore.complete({
          key: normalizedKey,
          payloadHash: blockingClaim.payload_hash,
          token: blockingClaim.token,
          fence: blockingClaim.fence,
          storyGid: story.gid,
          completionMode: "reconcile"
        });
        if (!reconciled.ok) {
          throw coordinationError(
            "MATERIAL_COMMENT_RECONCILIATION_FAILED",
            `Target story exists but shared claim reconciliation failed (${reconciled.status}); fail closed.`
          );
        }
        rememberStory(normalizedKey, story);
        if (blockingClaim.payload_hash === payloadHash) {
          return recoverResult(story, {
            status: "recovered_after_post_crash",
            fence: blockingClaim.fence,
            receipt: reconciled.receipt
          });
        }
        acquired = await distributedStore.acquire({
          key: normalizedKey,
          payloadHash,
          payloadProbe
        });
      }
      if (!story && blockingClaim.state === "posting") {
        throw coordinationError(
          "MATERIAL_COMMENT_POST_OUTCOME_UNCLEAR",
          "A shared posting claim exists without a matching target-system story. No retry is allowed until reconciled."
        );
      }
      if (!acquired.acquired) {
        throw coordinationError(
          "MATERIAL_COMMENT_CLAIM_ACTIVE",
          "Another instance owns the material-comment claim. Target-system readback ran; this write remains blocked."
        );
      }
    }

    const receiptAfterClaim = await distributedStore.readReceipt({
      key: normalizedKey,
      payloadHash
    });
    if (receiptAfterClaim) {
      const released = await distributedStore.release({
        key: normalizedKey,
        payloadHash,
        token: acquired.token,
        fence: acquired.fence
      });
      if (!released.ok) {
        throw coordinationError(
          "MATERIAL_COMMENT_RECEIPT_RACE_RELEASE_FAILED",
          `A receipt appeared during claim acquisition but the redundant claim could not be released (${released.status}); fail closed.`
        );
      }
      const { story } = await readAndMatch({ payload_probe: payloadProbe });
      if (!story || String(story.gid || "") !== String(receiptAfterClaim.story_gid || "")) {
        throw coordinationError(
          "MATERIAL_COMMENT_RECEIPT_READBACK_MISMATCH",
          "A receipt appeared during claim acquisition but no matching target-system story exists; fail closed."
        );
      }
      rememberStory(normalizedKey, story);
      return recoverResult(story, {
        status: "recovered_from_receipt_race",
        fence: receiptAfterClaim.fence,
        receipt: receiptAfterClaim
      });
    }

    let postingStarted = false;
    const beforePost = async () => {
      if (postingStarted) {
        throw coordinationError(
          "MATERIAL_COMMENT_POST_ALREADY_STARTED",
          "beforePost may only be called once."
        );
      }
      const result = await distributedStore.beginPosting({
        key: normalizedKey,
        payloadHash,
        token: acquired.token,
        fence: acquired.fence
      });
      if (!result.ok) {
        throw coordinationError(
          "MATERIAL_COMMENT_FENCE_REJECTED",
          `Shared material-comment fence rejected the writer (${result.status}); fail closed.`
        );
      }
      postingStarted = true;
      return { status: "posting", fence: acquired.fence };
    };

    let result;
    try {
      result = await action({
        recentStories: readRecentStories(normalizedKey),
        rememberStory: (story) => rememberStory(normalizedKey, story),
        beforePost,
        coordination: {
          status: "distributed_claim_acquired",
          fence: acquired.fence,
          payload_hash: payloadHash
        }
      });
    } catch (error) {
      if (!postingStarted) {
        const released = await distributedStore.release({
          key: normalizedKey,
          payloadHash,
          token: acquired.token,
          fence: acquired.fence
        });
        if (!released.ok) {
          throw coordinationError(
            "MATERIAL_COMMENT_PREPOST_RELEASE_FAILED",
            `Pre-post claim release failed (${released.status}); fail closed.`,
            error
          );
        }
        throw error;
      }
      try {
        const { story } = await readAndMatch({ payload_probe: payloadProbe });
        if (story) {
          const reconciled = await distributedStore.complete({
            key: normalizedKey,
            payloadHash,
            token: acquired.token,
            fence: acquired.fence,
            storyGid: story.gid,
            completionMode: "reconcile"
          });
          if (reconciled.ok) {
            rememberStory(normalizedKey, story);
            return recoverResult(story, {
              status: "recovered_after_unclear_post",
              fence: acquired.fence,
              receipt: reconciled.receipt
            });
          }
        }
      } catch (readbackError) {
        throw coordinationError(
          "MATERIAL_COMMENT_POST_AND_READBACK_UNCLEAR",
          "Material-comment post and mandatory target-system readback are unclear; persistent posting claim remains fail closed.",
          readbackError
        );
      }
      throw coordinationError(
        "MATERIAL_COMMENT_POST_OUTCOME_UNCLEAR",
        "Material-comment post failed and target-system readback found no matching story. Persistent posting claim remains fail closed.",
        error
      );
    }

    if (!postingStarted) {
      await distributedStore.release({
        key: normalizedKey,
        payloadHash,
        token: acquired.token,
        fence: acquired.fence
      });
      throw coordinationError(
        "MATERIAL_COMMENT_BEFORE_POST_MISSING",
        "Material-comment action returned without the mandatory shared beforePost fence."
      );
    }

    const storyGid = String(options.getStoryGid?.(result) || "").trim();
    if (!storyGid) {
      throw coordinationError(
        "MATERIAL_COMMENT_STORY_GID_MISSING",
        "Material-comment target write returned no story GID; persistent posting claim remains fail closed."
      );
    }
    const completed = await distributedStore.complete({
      key: normalizedKey,
      payloadHash,
      token: acquired.token,
      fence: acquired.fence,
      storyGid,
      completionMode: "posted"
    });
    if (!completed.ok) {
      const { story } = await readAndMatch({ payload_probe: payloadProbe });
      if (story) {
        const reconciled = await distributedStore.complete({
          key: normalizedKey,
          payloadHash,
          token: acquired.token,
          fence: acquired.fence,
          storyGid: story.gid,
          completionMode: "reconcile"
        });
        if (reconciled.ok) return result;
      }
      throw coordinationError(
        "MATERIAL_COMMENT_RECEIPT_FAILED",
        `Target write returned a story but shared receipt failed (${completed.status}); fail closed after target readback.`
      );
    }
    return result;
  }

  async function run(key, action, options = {}) {
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
      const useDistributed = options.distributed !== false;
      if (useDistributed && distributedRequired && !distributedStore) {
        throw coordinationError(
          "MATERIAL_COMMENT_DISTRIBUTED_STORE_REQUIRED",
          "Material routine comments are fail-closed because the shared claim/receipt store is not configured."
        );
      }
      if (useDistributed && distributedStore) {
        return await runDistributed(normalizedKey, action, options);
      }
      return await runLocal(normalizedKey, action, {
        status: useDistributed ? "distributed_not_required" : "dry_run_process_local"
      });
    } finally {
      release();
      if (tails.get(normalizedKey) === tail) tails.delete(normalizedKey);
    }
  }

  return {
    run,
    pendingCount: () => tails.size,
    distributedConfigured: () => Boolean(distributedStore),
    distributedRequired: () => Boolean(distributedRequired)
  };
}

export function isRoutineMaterialComment({
  routineLike = false,
  commentKind = "status",
  materialResultSignals = false
} = {}) {
  return Boolean(
    routineLike &&
      (["result", "handoff", "completion"].includes(String(commentKind || "")) ||
        materialResultSignals)
  );
}
