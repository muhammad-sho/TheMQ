import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ApiError } from "../../src/api/errors.js";
import {
  buildTestBroker,
  closeTestBroker,
  uniquePrefix,
  waitFor,
  type TestBroker,
} from "./helpers.js";

async function expectCode(promise: Promise<unknown>, code: string): Promise<ApiError> {
  try {
    await promise;
  } catch (err) {
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).code).toBe(code);
    return err as ApiError;
  }
  throw new Error(`Expected ApiError ${code} but the call succeeded`);
}

describe("broker queues", () => {
  let system: TestBroker;
  let prefix: string;

  beforeEach(async () => {
    prefix = uniquePrefix("queues");
    system = await buildTestBroker(prefix);
  });

  afterEach(async () => {
    await closeTestBroker(system);
  });

  it("declares queues idempotently and lists them", async () => {
    const first = await system.broker.declareQueue("orders");
    expect(first).toEqual({ queue: "orders", created: true });
    const second = await system.broker.declareQueue("orders");
    expect(second.created).toBe(false);

    const queues = await system.broker.listQueues();
    expect(queues).toEqual([{ queue: "orders", ready: 0, delayed: 0, unacked: 0, consumers: 0 }]);
  });

  it("returns NOT_FOUND for unknown queues", async () => {
    await expectCode(system.broker.getQueue("missing"), "NOT_FOUND");
    await expectCode(system.broker.deleteQueue("missing"), "NOT_FOUND");
    await expectCode(system.broker.consume("missing"), "NOT_FOUND");
  });

  it("maps a dead backend to SERVICE_UNAVAILABLE, not INTERNAL_ERROR", async () => {
    await system.broker.publish("q", { n: 1 }, { id: "msg_probe" });
    await system.connections.closeAll();
    await expectCode(system.broker.listQueues(), "SERVICE_UNAVAILABLE");
    await expectCode(system.broker.getQueue("q"), "SERVICE_UNAVAILABLE");
  });

  it("publishes implicitly declare the queue", async () => {
    await system.broker.publish("auto", { message: "hello" }, { id: "msg_auto" });
    const stats = await system.broker.getQueue("auto");
    expect(stats.ready).toBe(1);
    expect(stats.published).toBe(1);
  });

  it("deletes a queue with all its messages", async () => {
    await system.broker.publish("temp", { n: 1 }, { id: "msg_t1" });
    await system.broker.publish("temp", { n: 2 }, { id: "msg_t2", ttlMs: 60_000 });
    await system.broker.consume("temp", { consumerId: "c1" });
    await system.broker.deleteQueue("temp");
    await expectCode(system.broker.getQueue("temp"), "NOT_FOUND");
    expect(await system.broker.listQueues()).toEqual([]);
    await expectCode(system.broker.consume("temp"), "NOT_FOUND");
  });

  it("supports queue names with spaces and glob characters", async () => {
    await system.broker.publish("my queue", { a: 1 }, { id: "msg_space" });
    await system.broker.publish("q*test[1]", { b: 2 }, { id: "msg_glob" });
    const names = (await system.broker.listQueues()).map((q) => q.queue).sort();
    expect(names).toEqual(["my queue", "q*test[1]"]);
    await system.broker.deleteQueue("q*test[1]");
    await expectCode(system.broker.getQueue("q*test[1]"), "NOT_FOUND");
    expect((await system.broker.getQueue("my queue")).ready).toBe(1);
  });
});

describe("broker publish/consume/ack", () => {
  let system: TestBroker;

  beforeEach(async () => {
    system = await buildTestBroker(uniquePrefix("flow"), { SWEEPER_INTERVAL_MS: "100" });
  });

  afterEach(async () => {
    await closeTestBroker(system);
  });

  it("delivers messages FIFO with the {id, data} shape", async () => {
    for (let i = 0; i < 5; i += 1) {
      await system.broker.publish("q", { message: `m${String(i)}` }, { id: `msg_m${String(i)}` });
    }
    const result = await system.broker.consume("q", { consumerId: "c1", count: 5 });
    expect(result.consumerId).toBe("c1");
    expect(result.messages.map((m) => m.data)).toEqual([
      { message: "m0" },
      { message: "m1" },
      { message: "m2" },
      { message: "m3" },
      { message: "m4" },
    ]);
    for (const m of result.messages) {
      expect(m.id).toMatch(/^msg_/);
      expect(m.deliveryCount).toBe(1);
      expect(m.redelivered).toBe(false);
    }
    expect((await system.broker.getQueue("q")).unacked).toBe(5);
  });

  it("supports explicit ids and rejects duplicates", async () => {
    await system.broker.publish("q", { message: "hello" }, { id: "msg_123" });
    const err = await expectCode(
      system.broker.publish("q", { message: "again" }, { id: "msg_123" }),
      "CONFLICT",
    );
    expect(err.resource).toMatchObject({ type: "message", id: "msg_123", queue: "q" });
  });

  it("upserts an existing ready message with new data, keeping one copy", async () => {
    const first = await system.broker.publish("q", { v: 1 }, { id: "msg_up" });
    expect(first.upserted).toBe(false);
    const second = await system.broker.publish("q", { v: 2 }, { id: "msg_up", upsert: true });
    expect(second.upserted).toBe(true);
    expect(second.createdAt).toBe(first.createdAt);

    const inspected = await system.broker.getMessage("q", "msg_up");
    expect(inspected.data).toEqual({ v: 2 });
    expect(inspected.state).toBe("ready");
    expect(inspected.deliveryCount).toBe(0);

    const consumed = await system.broker.consume("q", { consumerId: "c1", count: 5 });
    expect(consumed.messages.map((m) => m.id)).toEqual(["msg_up"]);
    expect(consumed.messages[0]?.data).toEqual({ v: 2 });
  });

  it("upsert moves messages between ready and delayed with the new TTL", async () => {
    await system.broker.publish("q", { v: 1 }, { id: "msg_move", ttlMs: 60_000 });
    expect((await system.broker.getQueue("q")).delayed).toBe(1);

    const released = await system.broker.publish(
      "q",
      { v: 2 },
      { id: "msg_move", ttlMs: 0, upsert: true },
    );
    expect(released).toMatchObject({ upserted: true, state: "ready" });
    const stats = await system.broker.getQueue("q");
    expect(stats).toMatchObject({ ready: 1, delayed: 0 });

    const hidden = await system.broker.publish(
      "q",
      { v: 3 },
      { id: "msg_move", ttlMs: 60_000, upsert: true },
    );
    expect(hidden).toMatchObject({ upserted: true, state: "delayed" });
    expect((await system.broker.consume("q", { consumerId: "c1" })).messages).toHaveLength(0);
  });

  it("upsert on a missing id creates the message", async () => {
    const created = await system.broker.publish("q", { v: 1 }, { id: "msg_new", upsert: true });
    expect(created.upserted).toBe(false);
    expect((await system.broker.getQueue("q")).ready).toBe(1);
  });

  it("upsert on a leased message conflicts like delete and TTL changes", async () => {
    await system.broker.publish("q", { v: 1 }, { id: "msg_lease" });
    await system.broker.consume("q", { consumerId: "c1" });
    await expectCode(
      system.broker.publish("q", { v: 2 }, { id: "msg_lease", upsert: true }),
      "CONFLICT",
    );
  });

  it("skips on an existing ready message without changing it", async () => {
    const first = await system.broker.publish("q", { v: 1 }, { id: "msg_skip" });
    const before = await system.broker.getQueue("q");
    const skipped = await system.broker.publish(
      "q",
      { v: 2 },
      { id: "msg_skip", onConflict: "skip" },
    );
    expect(skipped).toMatchObject({
      skipped: true,
      upserted: false,
      state: "ready",
      deliveryCount: 0,
    });
    expect(skipped.createdAt).toBe(first.createdAt);
    // Untouched: same data, same depths, no counter movement.
    expect((await system.broker.getMessage("q", "msg_skip")).data).toEqual({ v: 1 });
    expect(await system.broker.getQueue("q")).toMatchObject({
      ready: before.ready,
      published: before.published,
    });
    const consumed = await system.broker.consume("q", { consumerId: "c1", count: 5 });
    expect(consumed.messages.map((m) => m.id)).toEqual(["msg_skip"]);
  });

  it("skips on a leased message and leaves the lease intact", async () => {
    await system.broker.publish("q", { v: 1 }, { id: "msg_skipl" });
    await system.broker.consume("q", { consumerId: "c1" });
    const skipped = await system.broker.publish(
      "q",
      { v: 2 },
      { id: "msg_skipl", onConflict: "skip" },
    );
    expect(skipped).toMatchObject({ skipped: true, state: "unacked" });
    // The message still acks afterwards: nothing was disturbed.
    await system.broker.ack("q", "msg_skipl");
    expect((await system.broker.getQueue("q")).unacked).toBe(0);
  });

  it("skips on a missing id by publishing normally", async () => {
    const created = await system.broker.publish(
      "q",
      { v: 1 },
      { id: "msg_skipnew", onConflict: "skip" },
    );
    expect(created).toMatchObject({ skipped: false, upserted: false, state: "ready" });
  });

  it("shares work across competing consumers without duplicates", async () => {
    for (let i = 0; i < 10; i += 1) {
      await system.broker.publish("q", { n: i }, { id: `msg_w${String(i)}` });
    }
    const [a, b] = await Promise.all([
      system.broker.consume("q", { consumerId: "a", count: 10 }),
      system.broker.consume("q", { consumerId: "b", count: 10 }),
    ]);
    const ids = [...a.messages, ...(b?.messages ?? [])].map((m) => m.id);
    expect(new Set(ids).size).toBe(10);
    expect((await system.broker.getQueue("q")).ready).toBe(0);
  });

  it("enforces prefetch per consumer", async () => {
    for (let i = 0; i < 5; i += 1) {
      await system.broker.publish("q", { n: i }, { id: `msg_p${String(i)}` });
    }
    const first = await system.broker.consume("q", { consumerId: "c1", count: 10, prefetch: 2 });
    expect(first.messages).toHaveLength(2);
    const second = await system.broker.consume("q", { consumerId: "c1", count: 10 });
    expect(second.messages).toHaveLength(0);
    await system.broker.ack("q", first.messages[0]?.id ?? "");
    const third = await system.broker.consume("q", { consumerId: "c1", count: 10 });
    expect(third.messages).toHaveLength(1);
  });

  it("acks remove messages by queue and id alone", async () => {
    const published = await system.broker.publish("q", { message: "hello" }, { id: "msg_ack" });
    // Waiting messages cannot be settled.
    await expectCode(system.broker.ack("q", published.id), "CONFLICT");
    await system.broker.consume("q", { consumerId: "owner" });
    // No ownership check: queue + id settle any holder's lease.
    const acked = await system.broker.ack("q", published.id);
    expect(acked.deliveries).toBe(1);
    await expectCode(system.broker.ack("q", published.id), "NOT_FOUND");
    await expectCode(system.broker.getMessage("q", published.id), "NOT_FOUND");
    expect((await system.broker.getQueue("q")).acked).toBe(1);
    await expectCode(system.broker.ack("q", "msg_missing"), "NOT_FOUND");
  });

  it("requeues leased messages for redelivery", async () => {
    await system.broker.publish("q", { message: "hello" }, { id: "msg_rq" });
    const first = await system.broker.consume("q", { consumerId: "c1" });
    const id = first.messages[0]?.id ?? "";
    const requeued = await system.broker.requeue("q", id);
    expect(requeued).toMatchObject({ requeued: true, state: "ready", deliveries: 1 });
    const stats = await system.broker.getQueue("q");
    expect(stats.ready).toBe(1);
    expect(stats.unacked).toBe(0);
    const second = await system.broker.consume("q", { consumerId: "c2" });
    expect(second.messages[0]?.id).toBe(id);
    expect(second.messages[0]?.deliveryCount).toBe(2);
    expect(second.messages[0]?.redelivered).toBe(true);
  });

  it("requeues with a new payload on the same id", async () => {
    await system.broker.publish("q", { v: 1 }, { id: "msg_edit" });
    await system.broker.consume("q", { consumerId: "c1" });
    const requeued = await system.broker.requeue("q", "msg_edit", { data: { v: 2 } });
    expect(requeued).toMatchObject({ requeued: true, state: "ready", deliveries: 1 });
    expect(await system.broker.getMessage("q", "msg_edit")).toMatchObject({
      data: { v: 2 },
      state: "ready",
    });
    const second = await system.broker.consume("q", { consumerId: "c2" });
    expect(second.messages[0]).toMatchObject({
      id: "msg_edit",
      data: { v: 2 },
      deliveryCount: 2,
      redelivered: true,
    });
  });

  it("requeues with a delay as a delayed message", async () => {
    await system.broker.publish("q", { message: "later" }, { id: "msg_delay" });
    await system.broker.consume("q", { consumerId: "c1" });
    const before = Date.now();
    const requeued = await system.broker.requeue("q", "msg_delay", { ttlMs: 400 });
    expect(requeued.state).toBe("delayed");
    expect(requeued.availableAt).toBeGreaterThanOrEqual(before + 400);
    expect((await system.broker.consume("q", { consumerId: "c2" })).messages).toHaveLength(0);
    await waitFor(
      async () => (await system.broker.consume("q", { consumerId: "c2" })).messages.length === 1,
      { label: "requeue delay redelivery" },
    );
    const inspected = await system.broker.getMessage("q", "msg_delay");
    expect(inspected.deliveryCount).toBeGreaterThanOrEqual(2);
  });

  it("requeues with a new payload and a delay together", async () => {
    await system.broker.publish("q", { v: 1 }, { id: "msg_both" });
    await system.broker.consume("q", { consumerId: "c1" });
    const requeued = await system.broker.requeue("q", "msg_both", {
      data: { v: 9 },
      ttlMs: 300,
    });
    expect(requeued).toMatchObject({ requeued: true, state: "delayed" });
    let redelivered: { id: string; data: unknown } | undefined;
    await waitFor(
      async () => {
        const found = await system.broker.consume("q", { consumerId: "c2" });
        if (found.messages.length === 1) {
          redelivered = found.messages[0];
          return true;
        }
        return false;
      },
      { label: "edited delay redelivery" },
    );
    expect(redelivered).toMatchObject({ id: "msg_both", data: { v: 9 } });
  });

  it("requeue rejects missing and non-leased messages", async () => {
    await expectCode(system.broker.requeue("q", "msg_missing"), "NOT_FOUND");
    await system.broker.publish("q", { message: "waiting" }, { id: "msg_wait" });
    // Waiting messages are edited with upsert, never requeued.
    await expectCode(system.broker.requeue("q", "msg_wait"), "CONFLICT");
    await expectCode(system.broker.requeue("q", "msg_wait", { ttlMs: 100 }), "CONFLICT");
  });

  it("redelivers unacked messages after the visibility timeout", async () => {
    await system.broker.publish("q", { message: "hello" }, { id: "msg_vis" });
    const first = await system.broker.consume("q", {
      consumerId: "c1",
      visibilityTimeoutMs: 300,
    });
    const id = first.messages[0]?.id ?? "";
    await waitFor(
      async () => (await system.broker.consume("q", { consumerId: "c2" })).messages.length === 1,
      { label: "visibility expiry redelivery" },
    );
    const second = await system.broker.consume("q", { consumerId: "c2" });
    // Either the waitFor poll or this call got the redelivery.
    const redelivered = second.messages.length === 1 ? second.messages[0] : undefined;
    expect(redelivered?.id ?? id).toBe(id);
    const inspected = await system.broker.getMessage("q", id);
    expect(inspected.deliveryCount).toBeGreaterThanOrEqual(2);
    expect(inspected.state).toBe("unacked");
  });

  it("holds noExpiry leases past sweeps without redelivery", async () => {
    await system.broker.publish("q", { message: "hello" }, { id: "msg_hold" });
    const first = await system.broker.consume("q", { consumerId: "c1", noExpiry: true });
    expect(first.messages[0]?.visibleAt).toBe(0);
    // Sweep repeatedly: a deadline-free lease must never be reclaimed.
    for (let i = 0; i < 3; i += 1) {
      const swept = await system.broker.sweep("q");
      expect(swept.reclaimed).toBe(0);
    }
    expect((await system.broker.getQueue("q")).unacked).toBe(1);
    expect((await system.broker.consume("q", { consumerId: "c2" })).messages).toHaveLength(0);
    // The hold settles normally.
    await system.broker.ack("q", "msg_hold");
    expect((await system.broker.getQueue("q")).unacked).toBe(0);
  });

  it("requeueOrphanedLeases recovers every leased message, then reports none", async () => {
    await system.broker.publish("q", { n: 1 }, { id: "msg_o1" });
    await system.broker.publish("q", { n: 2 }, { id: "msg_o2" });
    await system.broker.consume("q", { consumerId: "dead-conn", count: 2, noExpiry: true });
    expect((await system.broker.getQueue("q")).unacked).toBe(2);
    const recovered = await system.broker.requeueOrphanedLeases("q");
    expect(recovered.requeued).toBe(2);
    const stats = await system.broker.getQueue("q");
    expect(stats).toMatchObject({ ready: 2, unacked: 0 });
    // Recovery is RabbitMQ-restart style: redelivered with a bumped count.
    const next = await system.broker.consume("q", { consumerId: "c2", count: 2 });
    expect(next.messages).toHaveLength(2);
    for (const message of next.messages) {
      expect(message.deliveryCount).toBe(2);
      expect(message.redelivered).toBe(true);
    }
    expect((await system.broker.getQueue("q")).unacked).toBe(2);
    // With nothing leased, recovery reports zero.
    await system.broker.ack("q", "msg_o1");
    await system.broker.ack("q", "msg_o2");
    expect((await system.broker.requeueOrphanedLeases("q")).requeued).toBe(0);
    await expectCode(system.broker.requeueOrphanedLeases("missing"), "NOT_FOUND");
  });

  it("cancelling a consumer requeues its leases", async () => {
    for (let i = 0; i < 3; i += 1) {
      await system.broker.publish("q", { n: i }, { id: `msg_c${String(i)}` });
    }
    await system.broker.consume("q", { consumerId: "c1", count: 3 });
    const cancelled = await system.broker.cancelConsumer("q", "c1");
    expect(cancelled.requeued).toBe(3);
    const stats = await system.broker.getQueue("q");
    expect(stats.ready).toBe(3);
    expect(stats.unacked).toBe(0);
    // Cancelling again is idempotent.
    expect((await system.broker.cancelConsumer("q", "c1")).requeued).toBe(0);
    const next = await system.broker.consume("q", { consumerId: "c2", count: 3 });
    expect(next.messages).toHaveLength(3);
  });
});

describe("broker delete + TTL", () => {
  let system: TestBroker;

  beforeEach(async () => {
    system = await buildTestBroker(uniquePrefix("ttl"), { SWEEPER_INTERVAL_MS: "100" });
  });

  afterEach(async () => {
    await closeTestBroker(system);
  });

  it("deletes waiting messages but not leased ones", async () => {
    const keep = await system.broker.publish("q", { n: "keep" }, { id: "msg_keep" });
    const drop = await system.broker.publish("q", { n: "drop" }, { id: "msg_drop" });
    await system.broker.deleteMessage("q", drop.id);
    await expectCode(system.broker.getMessage("q", drop.id), "NOT_FOUND");
    const consumed = await system.broker.consume("q", { consumerId: "c1", count: 5 });
    expect(consumed.messages.map((m) => m.id)).toEqual([keep.id]);
    await expectCode(system.broker.deleteMessage("q", keep.id), "CONFLICT");
    await expectCode(system.broker.deleteMessage("q", "msg_missing"), "NOT_FOUND");
  });

  it("hides published messages until their TTL passes, then delivers them", async () => {
    await system.broker.publish("q", { message: "later" }, { id: "msg_later", ttlMs: 400 });
    expect((await system.broker.consume("q", { consumerId: "c1" })).messages).toHaveLength(0);
    expect((await system.broker.getQueue("q")).delayed).toBe(1);
    await waitFor(
      async () => (await system.broker.consume("q", { consumerId: "c1" })).messages.length === 1,
      { label: "TTL expiry delivery" },
    );
    const stats = await system.broker.getQueue("q");
    expect(stats.delayed).toBe(0);
  });

  it("resets a ready message's TTL and makes it delayed", async () => {
    const published = await system.broker.publish("q", { message: "hello" }, { id: "msg_rt" });
    const changed = await system.broker.setMessageTtl("q", published.id, 60_000);
    expect(changed.state).toBe("delayed");
    expect(changed.availableAt).toBeGreaterThan(Date.now());
    expect((await system.broker.consume("q", { consumerId: "c1" })).messages).toHaveLength(0);
    const inspected = await system.broker.getMessage("q", published.id);
    expect(inspected.state).toBe("delayed");
  });

  it("a TTL of zero makes a delayed message immediately available", async () => {
    const published = await system.broker.publish(
      "q",
      { message: "hello" },
      { id: "msg_z", ttlMs: 60_000 },
    );
    const changed = await system.broker.setMessageTtl("q", published.id, 0);
    expect(changed.state).toBe("ready");
    const consumed = await system.broker.consume("q", { consumerId: "c1" });
    expect(consumed.messages.map((m) => m.id)).toEqual([published.id]);
  });

  it("rejects TTL changes on leased or missing messages", async () => {
    const published = await system.broker.publish("q", { message: "hello" }, { id: "msg_lc" });
    await system.broker.consume("q", { consumerId: "c1" });
    await expectCode(system.broker.setMessageTtl("q", published.id, 1000), "CONFLICT");
    await expectCode(system.broker.setMessageTtl("q", "msg_missing", 1000), "NOT_FOUND");
  });

  it("TTL expiry does not delete the message — it becomes consumable", async () => {
    await system.broker.publish("q", { message: "hello" }, { id: "msg_exp", ttlMs: 300 });
    await waitFor(async () => (await system.broker.getQueue("q")).delayed === 0, {
      label: "sweeper promotion",
    });
    const consumed = await system.broker.consume("q", { consumerId: "c1" });
    expect(consumed.messages).toHaveLength(1);
    expect(consumed.messages[0]?.data).toEqual({ message: "hello" });
  });
});
