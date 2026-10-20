import fs from "node:fs";
import path from "node:path";

export const CODEX_CANDIDATES = [
  "/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex",
  "/Applications/Codex.app/Contents/Resources/codex",
  "/Applications/ChatGPT.app/Contents/Resources/codex"
];

export function resolveCodexBinary({ override = process.env.VIP_DASHBOARD_CODEX_BIN,
  candidates = CODEX_CANDIDATES } = {}) {
  const paths = override ? [override] : candidates;
  for (const filename of paths) {
    if (!path.isAbsolute(filename)) throw new Error("Codex binary must be an absolute path");
    try {
      if (!fs.statSync(filename).isFile()) continue;
      fs.accessSync(filename, fs.constants.X_OK);
      return filename;
    } catch { /* Missing or non-executable candidates cannot start a worker. */ }
  }
  throw new Error(override ? "Configured Codex binary is unavailable" : "No executable Codex binary found");
}
