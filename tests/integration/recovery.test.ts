import { describe, expect, it } from "vitest";
import { recoverOrphanedLeases } from "../../src/app/lifecycle.js";
import { createLogger } from "../../src/infrastructure/logging/logger.js";
import { buildTestBroker, closeTestBroker, uniquePrefix } from "./helpers.js";

describe("restart recovery", () => {
  it("requeues leases orphaned by a crashed run on the next boot", async () => {
    const prefix = uniquePrefix("restart");
    const logger = createLogger({ level: "silent" });
    // First run leases messages, then dies without cleanup (SIGKILL
    // style: no consumer cancel, no sweep — leases stay stranded).
    const crashed = await buildTestBroker(prefix);
    await crashed.broker.publish("q", { n: 1 }, { id: "msg_r1" });
    await crashed.broker.publish("q", { n: 2 }, { id: "msg_r2" });
    await crashed.broker.consume("q", { consumerId: "dead", count: 2, noExpiry: true });
    expect((await crashed.broker.getQueue("q")).unacked).toBe(2);
    crashed.sweeper.stop();
    await crashed.connections.closeAll();

    // Second boot on the same prefix recovers everything.
    const rebooted = await buildTestBroker(prefix);
    try {
      expect(await recoverOrphanedLeases(rebooted.broker, logger)).toBe(2);
      expect(await rebooted.broker.getQueue("q")).toMatchObject({ ready: 2, unacked: 0 });
      const next = await rebooted.broker.consume("q", { consumerId: "c2", count: 2 });
      expect(next.messages).toHaveLength(2);
      for (const message of next.messages) {
        expect(message.deliveryCount).toBe(2);
        expect(message.redelivered).toBe(true);
      }
    } finally {
      await closeTestBroker(rebooted);
    }
  });

  it("recovers nothing on a clean boot", async () => {
    const rebooted = await buildTestBroker(uniquePrefix("clean"));
    try {
      const logger = createLogger({ level: "silent" });
      expect(await recoverOrphanedLeases(rebooted.broker, logger)).toBe(0);
    } finally {
      await closeTestBroker(rebooted);
    }
  });
});
