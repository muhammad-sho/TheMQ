import type { AppConfig } from "../config/schema.js";
import { buildSystem, type BuiltSystem } from "./build-app.js";

/**
 * Start the broker and wait for SIGTERM/SIGINT.
 *
 * Shutdown order: stop HTTP -> stop the background sweeper ->
 * cancel persistent consumers (pending messages requeue) ->
 * release Redis connections -> exit. Unacked messages keep their
 * visibility deadlines in Redis, so redelivery survives restarts
 * with no custom recovery.
 */
export async function run(config: AppConfig): Promise<void> {
  const system: BuiltSystem = await buildSystem(config);
  const { logger } = system;

  const shutdownRequested = new Promise<string>((resolve) => {
    for (const signal of ["SIGTERM", "SIGINT"] as const) {
      process.once(signal, () => resolve(signal));
    }
  });

  try {
    await system.fastifyApp.listen({ host: config.apiHost, port: config.apiPort });
    logger.info(
      { event: "api-listening", host: config.apiHost, port: config.apiPort },
      "TheMQ API listening",
    );
    system.sweeper.start();
    logger.info({ event: "started" }, "TheMQ started");

    const signal = await shutdownRequested;
    logger.info({ event: "shutdown-signal", signal }, "Shutdown requested");
  } finally {
    // Always release sockets, consumers, and Redis — even when listen
    // fails — so nothing holds the process (or leases) hostage.
    await system.close();
  }
  logger.info({ event: "shutdown-complete" }, "TheMQ shut down cleanly");
}
