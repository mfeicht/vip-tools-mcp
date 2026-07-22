import { execFile } from "node:child_process";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

if (process.platform === "linux") {
  console.log("Linux runtime uses pinned @sparticuz/chromium; Puppeteer Chrome download skipped.");
} else {
  const cliPath = path.join(
    process.cwd(),
    "node_modules",
    "puppeteer",
    "lib",
    "cjs",
    "puppeteer",
    "node",
    "cli.js"
  );
  await execFileAsync(process.execPath, [cliPath, "browsers", "install", "chrome"], {
    cwd: process.cwd(),
    env: process.env,
    timeout: 300_000,
    maxBuffer: 2_000_000
  });
}
