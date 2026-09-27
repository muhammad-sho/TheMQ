import { loadConfig } from "./config/env.js";
import { run } from "./app/lifecycle.js";

async function main(): Promise<void> {
  const config = loadConfig();
  await run(config);
}

process.on("unhandledRejection", (reason) => {
  // A half-alive broker is worse than a restart: exit so the supervisor
  // (Docker restart policy) brings up a clean process.
  console.error("Unhandled rejection:", reason);
  process.exit(1);
});

main().catch((err: unknown) => {
  console.error(err instanceof Error ? err.message : err);
  process.exitCode = 1;
});
