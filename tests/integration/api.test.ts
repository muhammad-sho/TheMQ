import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { buildSystem, type BuiltSystem } from "../../src/app/build-app.js";
import { testConfig, uniquePrefix } from "./helpers.js";

function portOf(system: BuiltSystem): number {
  const address = system.fastifyApp.server.address();
  if (typeof address === "object" && address !== null && "port" in address) {
    return address.port;
  }
  throw new Error("Server is not listening");
}

describe("broker HTTP API", () => {
  let system: BuiltSystem;
  let base: string;

  beforeEach(async () => {
    const config = testConfig(uniquePrefix("http"), {
      API_PORT: "0",
      SWEEPER_INTERVAL_MS: "100",
    });
    system = await buildSystem(config);
    await system.fastifyApp.listen({ host: "127.0.0.1", port: 0 });
    base = `http://127.0.0.1:${String(portOf(system))}`;
  });

  afterEach(async () => {
    await system.close();
  });

  it("serves unauthenticated health probes", async () => {
    const live = await fetch(`${base}/health/live`);
    expect(live.status).toBe(200);
    const ready = await fetch(`${base}/health/ready`);
    expect(ready.status).toBe(200);
  });

  it("publishes {id, data} messages and consumes them", async () => {
    let res = await fetch(`${base}/queues/hello`, { method: "PUT" });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ queue: "hello", created: true });

    res = await fetch(`${base}/queues/hello/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ id: "msg_123", data: { message: "hello" } }),
    });
    expect(res.status).toBe(201);
    const published = (await res.json()) as { id: string; state: string };
    expect(published.id).toBe("msg_123");
    expect(published.state).toBe("ready");

    res = await fetch(`${base}/queues/hello/consume`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ consumerId: "worker-1", count: 10 }),
    });
    expect(res.status).toBe(200);
    const consumed = (await res.json()) as {
      consumerId: string;
      messages: Array<{ id: string; data: unknown; deliveryCount: number }>;
    };
    expect(consumed.consumerId).toBe("worker-1");
    expect(consumed.messages).toHaveLength(1);
    expect(consumed.messages[0]).toMatchObject({
      id: "msg_123",
      data: { message: "hello" },
      deliveryCount: 1,
    });

    res = await fetch(`${base}/queues/hello/messages/msg_123/ack`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ consumerId: "worker-1" }),
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ acked: true });
  });

  it("requires a message id and supports upsert publishing", async () => {
    await fetch(`${base}/queues/up`, { method: "PUT" });
    const post = (body: unknown): Promise<Response> =>
      fetch(`${base}/queues/up/messages`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });

    // Missing id is a validation error, not a generated id.
    let res = await post({ data: { v: 0 } });
    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({ error: { code: "VALIDATION_ERROR" } });

    res = await post({ id: "msg_up", data: { v: 1 } });
    expect(res.status).toBe(201);
    expect(await res.json()).toMatchObject({ id: "msg_up", upserted: false });

    // Same id without upsert conflicts.
    res = await post({ id: "msg_up", data: { v: 2 } });
    expect(res.status).toBe(409);

    // Same id with upsert updates in place (200, single copy).
    res = await post({ id: "msg_up", data: { v: 2 }, upsert: true });
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ id: "msg_up", upserted: true });

    res = await fetch(`${base}/queues/up/messages/msg_up`);
    expect(await res.json()).toMatchObject({ data: { v: 2 } });
    const stats = (await (await fetch(`${base}/queues/up`)).json()) as { ready: number };
    expect(stats.ready).toBe(1);
  });

  it("deletes a queued message so consumers never see it", async () => {
    await fetch(`${base}/queues/del`, { method: "PUT" });
    for (const id of ["msg_1", "msg_2"]) {
      await fetch(`${base}/queues/del/messages`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id, data: { n: id } }),
      });
    }
    const deleted = await fetch(`${base}/queues/del/messages/msg_1`, { method: "DELETE" });
    expect(deleted.status).toBe(204);
    const consumed = await fetch(`${base}/queues/del/consume`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ consumerId: "c1", count: 10 }),
    });
    const body = (await consumed.json()) as { messages: Array<{ id: string }> };
    expect(body.messages.map((m) => m.id)).toEqual(["msg_2"]);
  });

  it("changes a message TTL and delivers it once the TTL passes", async () => {
    await fetch(`${base}/queues/delayed`, { method: "PUT" });
    await fetch(`${base}/queues/delayed/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ id: "msg_ttl", data: { message: "later" } }),
    });
    let res = await fetch(`${base}/queues/delayed/messages/msg_ttl/ttl`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ttl: 60_000 }),
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ id: "msg_ttl", state: "delayed" });

    res = await fetch(`${base}/queues/delayed/messages/msg_ttl`);
    expect(((await res.json()) as { state: string }).state).toBe("delayed");

    res = await fetch(`${base}/queues/delayed/messages/msg_ttl/ttl`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ttl: 0 }),
    });
    expect(((await res.json()) as { state: string }).state).toBe("ready");

    res = await fetch(`${base}/queues/delayed/consume`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ consumerId: "c1" }),
    });
    const body = (await res.json()) as { messages: Array<{ id: string }> };
    expect(body.messages.map((m) => m.id)).toEqual(["msg_ttl"]);
  });

  it("returns stable error codes", async () => {
    let res = await fetch(`${base}/queues/nope/consume`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(404);
    expect(((await res.json()) as { error: { code: string } }).error.code).toBe("NOT_FOUND");

    res = await fetch(`${base}/queues/err/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ noData: true }),
    });
    expect(res.status).toBe(400);
    expect(((await res.json()) as { error: { code: string } }).error.code).toBe("VALIDATION_ERROR");
  });
});

describe("broker HTTP auth", () => {
  let system: BuiltSystem;
  let base: string;

  beforeEach(async () => {
    const prefix = uniquePrefix("auth");
    const { loadConfig } = await import("../../src/config/env.js");
    const { REDIS_URL } = await import("./helpers.js");
    const config = loadConfig({
      AUTH_DISABLED: "false",
      API_TOKEN: "s3cret",
      API_PORT: "0",
      REDIS_URL,
      REDIS_KEY_PREFIX: prefix,
      SWEEPER_INTERVAL_MS: "100",
    });
    system = await buildSystem(config);
    await system.fastifyApp.listen({ host: "127.0.0.1", port: 0 });
    base = `http://127.0.0.1:${String(portOf(system))}`;
  });

  afterEach(async () => {
    await system.close();
  });

  it("requires a bearer token outside /health/*", async () => {
    let res = await fetch(`${base}/queues`);
    expect(res.status).toBe(401);
    res = await fetch(`${base}/queues`, { headers: { authorization: "Bearer s3cret" } });
    expect(res.status).toBe(200);
    const live = await fetch(`${base}/health/live`);
    expect(live.status).toBe(200);
  });
});
