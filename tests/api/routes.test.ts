import { beforeEach, describe, expect, it, vi } from "vitest";
import type { AppInstance, ApiServices } from "../../src/api/server.js";
import { buildApp } from "../../src/api/server.js";
import type { BrokerService } from "../../src/broker/broker.js";
import { SubscriptionManager } from "../../src/broker/subscriptions.js";
import { loadConfig } from "../../src/config/env.js";
import { HealthService } from "../../src/health/health-service.js";
import { createLogger } from "../../src/infrastructure/logging/logger.js";

const TOKEN = "test-token-123";

async function buildTestApp(broker: BrokerService): Promise<AppInstance> {
  const config = loadConfig({ API_TOKEN: TOKEN });
  const logger = createLogger({ level: "silent" });
  const healthService = new HealthService(logger);
  const subscriptions = new SubscriptionManager(broker, logger);
  const services: ApiServices = { config, logger, broker, healthService, subscriptions };
  const app = await buildApp(services);
  await app.ready();
  return app;
}

function mockBroker(): BrokerService {
  return {
    declareQueue: vi
      .fn()
      .mockImplementation((queue: string) => Promise.resolve({ queue, created: true })),
    listQueues: vi
      .fn()
      .mockResolvedValue([{ queue: "orders", ready: 2, delayed: 0, unacked: 0, consumers: 0 }]),
    getQueue: vi.fn().mockImplementation((queue: string) =>
      Promise.resolve({
        queue,
        ready: 1,
        delayed: 0,
        unacked: 0,
        consumers: [],
        published: 1,
        delivered: 0,
        acked: 0,
        requeued: 0,
        deleted: 0,
        createdAt: 1,
      }),
    ),
    deleteQueue: vi.fn().mockResolvedValue(undefined),
    publish: vi.fn().mockImplementation((queue: string) =>
      Promise.resolve({
        id: "msg_123",
        queue,
        state: "ready",
        availableAt: 1,
        createdAt: 1,
        upserted: false,
        skipped: false,
        deliveryCount: 0,
      }),
    ),
    getMessage: vi.fn().mockImplementation((queue: string, id: string) =>
      Promise.resolve({
        id,
        queue,
        data: { message: "hello" },
        state: "ready",
        consumerId: null,
        deliveryCount: 0,
        availableAt: 0,
        visibleAt: 0,
        createdAt: 1,
        updatedAt: 1,
      }),
    ),
    consume: vi.fn().mockImplementation((queue: string, opts: { consumerId?: string }) =>
      Promise.resolve({
        consumerId: opts.consumerId ?? "cons_abc",
        messages: [
          {
            id: "msg_123",
            queue,
            data: { message: "hello" },
            deliveryCount: 1,
            redelivered: false,
            visibleAt: 2,
          },
        ],
      }),
    ),
    ack: vi.fn().mockResolvedValue({ deliveries: 1 }),
    requeue: vi.fn().mockResolvedValue({
      id: "msg_123",
      queue: "orders",
      requeued: true,
      state: "ready",
      availableAt: 3,
      deliveries: 1,
    }),
    deleteMessage: vi.fn().mockResolvedValue({ state: "ready" }),
    setMessageTtl: vi
      .fn()
      .mockImplementation((_queue: string, _id: string) =>
        Promise.resolve({ state: "delayed", availableAt: 60001 }),
      ),
    cancelConsumer: vi
      .fn()
      .mockImplementation((_queue: string, _consumerId: string) =>
        Promise.resolve({ requeued: 2 }),
      ),
    onChange: vi.fn().mockReturnValue(() => {}),
    onDeleteQueue: vi.fn().mockReturnValue(() => {}),
  } as unknown as BrokerService;
}

describe("broker routes (mocked service, no Redis)", () => {
  let app: AppInstance;
  let broker: BrokerService;

  beforeEach(async () => {
    broker = mockBroker();
    app = await buildTestApp(broker);
  });

  const auth = { authorization: `Bearer ${TOKEN}` };

  it("rejects unauthenticated requests", async () => {
    const res = await app.inject({ method: "GET", url: "/queues" });
    expect(res.statusCode).toBe(401);
    expect(res.json()).toMatchObject({ error: { code: "UNAUTHENTICATED" } });
  });

  it("declares, lists, inspects, and deletes queues", async () => {
    let res = await app.inject({ method: "PUT", url: "/queues/orders", headers: auth });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ queue: "orders", created: true });

    res = await app.inject({ method: "GET", url: "/queues", headers: auth });
    expect(res.json<{ queues: unknown[] }>().queues).toHaveLength(1);

    res = await app.inject({ method: "GET", url: "/queues/orders", headers: auth });
    expect(res.json()).toMatchObject({ queue: "orders" });

    res = await app.inject({ method: "DELETE", url: "/queues/orders", headers: auth });
    expect(res.statusCode).toBe(204);
  });

  it("publishes, inspects, consumes, acks, and requeues messages", async () => {
    let res = await app.inject({
      method: "POST",
      url: "/queues/orders/messages",
      headers: auth,
      payload: { id: "msg_123", data: { message: "hello" } },
    });
    expect(res.statusCode).toBe(201);
    expect(res.json()).toMatchObject({ id: "msg_123" });

    res = await app.inject({
      method: "POST",
      url: "/queues/orders/consume",
      headers: auth,
      payload: { consumerId: "worker-1", count: 5 },
    });
    const consumed = res.json<{
      consumerId: string;
      messages: unknown[];
    }>();
    expect(consumed.consumerId).toBe("worker-1");
    expect(consumed.messages).toHaveLength(1);

    res = await app.inject({
      method: "POST",
      url: "/queues/orders/messages/msg_123/ack",
      headers: auth,
      payload: {},
    });
    expect(res.json()).toMatchObject({ acked: true, deliveries: 1 });

    res = await app.inject({
      method: "POST",
      url: "/queues/orders/messages/msg_123/requeue",
      headers: auth,
      payload: { data: { attempt: 2 }, ttlMs: 60000 },
    });
    expect(broker.requeue).toHaveBeenCalledWith("orders", "msg_123", {
      data: { attempt: 2 },
      ttlMs: 60000,
    });
    expect(res.json()).toMatchObject({ requeued: true, state: "ready" });
  });

  it("deletes messages, changes TTL, and cancels consumers", async () => {
    let res = await app.inject({
      method: "DELETE",
      url: "/queues/orders/messages/msg_123",
      headers: auth,
    });
    expect(res.statusCode).toBe(204);

    res = await app.inject({
      method: "PUT",
      url: "/queues/orders/messages/msg_123/ttl",
      headers: auth,
      payload: { ttl: 60000 },
    });
    expect(res.json()).toMatchObject({ state: "delayed" });

    res = await app.inject({
      method: "POST",
      url: "/queues/orders/consumers/worker-1/cancel",
      headers: auth,
    });
    expect(res.json()).toMatchObject({ cancelled: true, requeued: 2 });
  });

  it("returns 400 for invalid bodies", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/queues/orders/messages",
      headers: auth,
      payload: { execution: { type: "http" } },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json()).toMatchObject({ error: { code: "VALIDATION_ERROR" } });
  });

  it("maps empty JSON bodies to VALIDATION_ERROR, not INTERNAL_ERROR", async () => {
    const res = await app.inject({
      method: "DELETE",
      url: "/queues/orders/messages/msg_123",
      headers: { ...auth, "content-type": "application/json" },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json()).toMatchObject({ error: { code: "VALIDATION_ERROR" } });
  });

  it("labels rate-limited responses instead of INTERNAL_ERROR", async () => {
    // Fresh app per test, so the 1000/min bucket starts empty here.
    let last = await app.inject({ method: "GET", url: "/queues", headers: auth });
    for (let i = 0; i < 1000; i += 1) {
      last = await app.inject({ method: "GET", url: "/queues", headers: auth });
      if (last.statusCode === 429) break;
    }
    expect(last.statusCode).toBe(429);
    expect(last.json()).toMatchObject({ error: { code: "VALIDATION_ERROR" } });
  });
});
