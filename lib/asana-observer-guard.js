function normalizeObserverText(value) {
  return String(value || "")
    .replace(/ß/g, "ss")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function classifyAsanaCommentAuthority({ task, agentUserGid }) {
  const agentGid = String(agentUserGid || "");
  const isAssignee = String(task?.assignee?.gid || "") === agentGid;
  const isCreator = String(task?.created_by?.gid || "") === agentGid;
  const isFollower = (task?.followers || []).some(
    (follower) => String(follower?.gid || "") === agentGid
  );
  return {
    role: isAssignee ? "assignee" : isCreator ? "creator" : "observer",
    observer_only: !isAssignee && !isCreator,
    is_assignee: isAssignee,
    is_creator: isCreator,
    is_follower: isFollower
  };
}

export function validateAsanaObserverCommentIntent({
  authority,
  reason,
  basis,
  authorizationReceipt,
  evidence,
  commentText,
  governanceAgent
}) {
  if (!authority?.observer_only) {
    return {
      applicable: false,
      allowed: true,
      status: "not_applicable",
      reason: null
    };
  }

  const normalizedBasis = normalizeObserverText(basis);
  const normalizedComment = normalizeObserverText(commentText);
  const verifiedClaims = Array.isArray(evidence?.verified_claims)
    ? evidence.verified_claims.filter(Boolean)
    : [];
  const unresolved = Array.isArray(evidence?.unresolved)
    ? evidence.unresolved.filter(Boolean)
    : [];

  if (!reason) {
    return {
      applicable: true,
      allowed: false,
      status: "blocked_observer_without_authority",
      reason: null
    };
  }

  if (reason === "explicit_instruction") {
    const allowed = Boolean(
      authorizationReceipt?.source === "asana" &&
        authorizationReceipt?.authorization_basis === "task_story" &&
        authorizationReceipt?.authorization_story_mentions_agent
    );
    return {
      applicable: true,
      allowed,
      status: allowed
        ? "allowed_explicit_asana_instruction"
        : "blocked_missing_explicit_asana_mention",
      reason
    };
  }

  if (reason === "direct_codex") {
    const allowed = Boolean(
      authorizationReceipt?.source === "direct_codex" &&
        authorizationReceipt?.authorized_by === "Moritz Feichtmeyer"
    );
    return {
      applicable: true,
      allowed,
      status: allowed
        ? "allowed_direct_moritz_instruction"
        : "blocked_missing_direct_moritz_instruction",
      reason
    };
  }

  if (reason === "critical_anomaly") {
    const criticalSignals = [
      "akut",
      "unmittelbar",
      "kritisch",
      "datenverlust",
      "sicherheitslucke",
      "produktionsausfall",
      "irreversibel",
      "compliance",
      "rechtsrisiko"
    ];
    const hasCriticalSignal = criticalSignals.some((signal) =>
      `${normalizedBasis} ${normalizedComment}`.includes(signal)
    );
    const allowed =
      normalizedBasis.length >= 20 &&
      verifiedClaims.length > 0 &&
      unresolved.length > 0 &&
      hasCriticalSignal;
    return {
      applicable: true,
      allowed,
      status: allowed
        ? "allowed_verified_critical_anomaly"
        : "blocked_unproven_critical_anomaly",
      reason
    };
  }

  if (reason === "governance_scope") {
    const allowed = Boolean(
      governanceAgent && normalizedBasis.length >= 20 && verifiedClaims.length > 0
    );
    return {
      applicable: true,
      allowed,
      status: allowed
        ? "allowed_documented_governance_scope"
        : "blocked_unproven_governance_scope",
      reason
    };
  }

  return {
    applicable: true,
    allowed: false,
    status: "blocked_unknown_observer_reason",
    reason
  };
}
