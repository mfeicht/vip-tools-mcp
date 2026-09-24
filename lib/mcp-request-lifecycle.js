export function createMcpRequestCleanup({ transport, server, onCleanupError, onClosed } = {}) {
  let cleanupPromise = null;

  return function cleanup(reason = "unknown") {
    if (cleanupPromise) return cleanupPromise;

    cleanupPromise = (async () => {
      const errors = [];
      for (const [resource, closeable] of [
        ["transport", transport],
        ["server", server]
      ]) {
        try {
          await closeable?.close?.();
        } catch (error) {
          const failure = { resource, error };
          errors.push(failure);
          onCleanupError?.(failure, reason);
        }
      }
      onClosed?.({ reason, errors });
      return { reason, errors };
    })();

    return cleanupPromise;
  };
}
