import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { WebSocket } from "ws";
import type { BuiltSystem } from "../../src/app/build-app.js";
import { buildSystem } from "../../src/app/build-app.js";
import { frameToString } from "../../src/api/frames.js";
import { testConfig, uniquePrefix } from "./helpers.js";

interface ServerFrame {
  type: string;
  [key: string]: unknown;
}

function portOf(system: BuiltSystem): number {
  const address = system.fastifyApp.server.address();
  if (typeof address === "object" && address !== null && "port" in address) {
    return address.port;
  }
  throw new Error("Server is not listening");
}

/** Open a raw consumer socket with frame helpers. */
function openConsumer(
  base: string,
  queue: string,
): {
  ws: WebSocket;
  nextFrame: (timeoutMs?: number) => Promise<ServerFrame>;
  closed: Promise<{ code: number; reason: string }>;
} {
  const wsUrl = base.replace("http://", "ws://");
  const ws = new WebSocket(`${wsUrl}/queues/${encodeURIComponent(queue)}/subscribe`);
  const queue_: ServerFrame[] = [];
  const waiters: Array<{
    resolve: (frame: ServerFrame) => void;
    reject: (err: Error) => void;
    timer: ReturnType<typeof setTimeout>;
  }> = [];
  ws.on("message", (raw) => {
    const frame = JSON.parse(frameToString(raw)) as ServerFrame;
    const waiter = waiters.shift();
    if (waiter) {
      clearTimeout(waiter.timer);
      waiter.resolve(frame);
    } else {
      queue_.push(frame);
    }
  });
  ws.on("error", () => {
    // surfaced via close/timeout, not thrown
  });
  const closed = new Promise<{ code: number; reason: string }>((resolve) => {
    ws.on("close", (code: number, reason: Buffer) => {
      resolve({ code, reason: reason.toString() });
    });
  });
  const nextFrame = (timeoutMs = 10000): Promise<ServerFrame> => {
    const queued = queue_.shift();
    if (queued) return Promise.resolve(queued);
    return new Promise<ServerFrame>((resolve, reject) => {
      const waiter: {
        resolve: (frame: ServerFrame) => void;
        reject: (err: Error) => void;
        timer: ReturnType<typeof setTimeout>;
      } = {
        resolve,
        reject,
        timer: setTimeout(() => {
          // A timed-out wait must not swallow a later frame.
          const index = waiters.indexOf(waiter);
          if (index >= 0) waiters.splice(index, 1);
          reject(new Error("Timed out waiting for server frame"));
        }, timeoutMs),
      };
      waiters.push(waiter);
    });
  };
  return { ws, nextFrame, closed };
}

async function waitOpen(ws: WebSocket): Promise<void> {
  if (ws.readyState === WebSocket.OPEN) return;
  await new Promise<void>((resolve, reject) => {
    ws.once("open", () => resolve());
    ws.once("error", (err) => reject(err));
  });
}

describe("persistent consumers (WebSocket subscribe)", () => {
  let system: BuiltSystem;
  let base: string;
  let prefix: string;

  beforeEach(async () => {
    prefix = uniquePrefix("sub");
    const config = testConfig(prefix, { API_PORT: "0", SWEEPER_INTERVAL_MS: "100" });
    system = await buildSystem(config);
    await system.fastifyApp.listen({ host: "127.0.0.1", port: 0 });
    base = `http://127.0.0.1:${String(portOf(system))}`;
    // buildSystem does not start the background sweeper (lifecycle.run does
    // in production); start it so TTL promotion works exactly like live.
    system.sweeper.start();
  });

  afterEach(async () => {
    await system.close();
  });

  async function declareQueue(queue: string): Promise<void> {
    const res = await fetch(`${base}/queues/${queue}`, { method: "PUT" });
    expect(res.status).toBe(200);
  }

  let publishSeq = 0;
  async function publish(queue: string, body: unknown): Promise<{ id: string }> {
    publishSeq += 1;
    const res = await fetch(`${base}/queues/${queue}/messages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      // Message ids are mandatory; the helper mints one per call.
      body: JSON.stringify({ id: `msg_sub${publishSeq}`, ...(body as Record<string, unknown>) }),
    });
    expect(res.status).toBe(201);
    return (await res.json()) as { id: string };
  }

  it("delivers published messages immediately and settles them over the socket", async () => {
    await declareQueue("live");
    const { ws, nextFrame, closed } = openConsumer(base, "live");
    await waitOpen(ws);
    ws.send(JSON.stringify({ action: "hello", consumerId: "c1", prefetch: 5 }));

    const ready = await nextFrame();
    expect(ready).toMatchObject({ type: "ready", queue: "live", consumerId: "c1", prefetch: 5 });

    const { id } = await publish("live", { data: { hello: "world" } });
    const delivered = await nextFrame();
    expect(delivered).toMatchObject({
      type: "message",
      queue: "live",
      id,
      consumerId: "c1",
      data: { hello: "world" },
      deliveryCount: 1,
      redelivered: false,
    });

    ws.send(JSON.stringify({ action: "ack", id }));
    await expect(nextFrame()).resolves.toMatchObject({ type: "acked", id, deliveries: 1 });

    const stats = (await (await fetch(`${base}/queues/live`)).json()) as {
      ready: number;
      unacked: number;
    };
    expect(stats).toMatchObject({ ready: 0, unacked: 0 });

    ws.send(JSON.stringify({ action: "cancel" }));
    await expect(nextFrame()).resolves.toMatchObject({ type: "cancelled", requeued: 0 });
    await expect(closed).resolves.toMatchObject({ code: 1000 });
  });

  it("requeues over the socket and redelivers with a bumped delivery count", async () => {
    await declareQueue("retry");
    const { ws, nextFrame } = openConsumer(base, "retry");
    await waitOpen(ws);
    ws.send(JSON.stringify({ action: "hello", consumerId: "c1", prefetch: 5 }));
    await nextFrame();

    const { id } = await publish("retry", { data: 1 });
    const first = await nextFrame();
    expect(first).toMatchObject({ id, deliveryCount: 1 });

    ws.send(JSON.stringify({ action: "requeue", id }));
    await expect(nextFrame()).resolves.toMatchObject({ type: "requeued", id });

    const second = await nextFrame();
    expect(second).toMatchObject({ id, deliveryCount: 2, redelivered: true });
    ws.close();
  });

  it("rejects unknown queues with an error frame and close code 4404", async () => {
    const { ws, nextFrame, closed } = openConsumer(base, "missing");
    await waitOpen(ws);
    ws.send(JSON.stringify({ action: "hello" }));
    await expect(nextFrame()).resolves.toMatchObject({ type: "error", code: "NOT_FOUND" });
    await expect(closed).resolves.toMatchObject({ code: 4404 });
  });

  it("answers hello with an error and closes when subscribing fails mid-handshake", async () => {
    await declareQueue("flaky");
    // Sabotage the lease step: the queue exists (hello passes validation)
    // but delivery blows up. The client must get an error frame and a
    // close — never silence followed by a client-side timeout.
    const consume = system.broker.consume.bind(system.broker);
    system.broker.consume = () => Promise.reject(new Error("backend exploded"));
    try {
      const { ws, nextFrame, closed } = openConsumer(base, "flaky");
      await waitOpen(ws);
      ws.send(JSON.stringify({ action: "hello", prefetch: 5 }));
      await expect(nextFrame()).resolves.toMatchObject({
        type: "error",
        code: "INTERNAL_ERROR",
      });
      await expect(closed).resolves.toMatchObject({ code: 1011 });
    } finally {
      system.broker.consume = consume;
    }
  });

  it("answers protocol violations with error frames and keeps the socket open", async () => {
    await declareQueue("strict");
    const { ws, nextFrame } = openConsumer(base, "strict");
    await waitOpen(ws);
    ws.send(JSON.stringify({ action: "hello", prefetch: 2 }));
    const ready = await nextFrame();
    expect(ready.type).toBe("ready");

    ws.send("not-json{{{");
    await expect(nextFrame()).resolves.toMatchObject({ type: "error", code: "PROTOCOL_ERROR" });
    ws.send(JSON.stringify({ action: "bogus" }));
    await expect(nextFrame()).resolves.toMatchObject({ type: "error", code: "PROTOCOL_ERROR" });
    ws.send(JSON.stringify({ action: "ack", id: "nope" }));
    await expect(nextFrame()).resolves.toMatchObject({
      type: "error",
      code: "NOT_FOUND",
      id: "nope",
    });
    expect(ws.readyState).toBe(WebSocket.OPEN);
    ws.close();
  });

  it("holds subscribed messages without a deadline, however short the hello lease", async () => {
    await declareQueue("hold");
    const { ws, nextFrame } = openConsumer(base, "hold");
    await waitOpen(ws);
    // 100ms is the shortest lease the hello schema accepts — the server
    // must ignore it for persistent deliveries (RabbitMQ manual-ack style).
    ws.send(
      JSON.stringify({ action: "hello", consumerId: "c1", prefetch: 5, visibilityTimeoutMs: 100 }),
    );
    await nextFrame();

    const { id } = await publish("hold", { data: "patient" });
    const delivered = await nextFrame();
    expect(delivered).toMatchObject({ id, deliveryCount: 1, redelivered: false });

    // Several sweeper intervals pass: no redelivery may arrive.
    await expect(nextFrame(700)).rejects.toThrow("Timed out waiting for server frame");
    const stats = (await (await fetch(`${base}/queues/hold`)).json()) as {
      ready: number;
      unacked: number;
    };
    expect(stats).toMatchObject({ ready: 0, unacked: 1 });

    ws.send(JSON.stringify({ action: "ack", id }));
    await expect(nextFrame()).resolves.toMatchObject({ type: "acked", id, deliveries: 1 });
    ws.close();
  });

  it("requeues pending messages when the socket drops, so work is redelivered", async () => {
    await declareQueue("drop");
    const { ws, nextFrame } = openConsumer(base, "drop");
    await waitOpen(ws);
    ws.send(JSON.stringify({ action: "hello", consumerId: "c1", prefetch: 5 }));
    await nextFrame();

    await publish("drop", { data: "work" });
    const delivered = await nextFrame();
    expect(delivered.type).toBe("message");

    // Drop the connection without acking: the lease must come back.
    ws.terminate();
    await new Promise((resolve) => setTimeout(resolve, 300));

    const res = await fetch(`${base}/queues/drop/consume`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ consumerId: "c2", count: 5 }),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { messages: Array<{ deliveryCount: number }> };
    expect(body.messages).toHaveLength(1);
    expect(body.messages[0]?.deliveryCount).toBe(2);
  });

  it("pushes TTL-delayed messages as soon as they become available", async () => {
    await declareQueue("delayed");
    const { ws, nextFrame } = openConsumer(base, "delayed");
    await waitOpen(ws);
    ws.send(JSON.stringify({ action: "hello", prefetch: 5 }));
    await nextFrame();

    await publish("delayed", { data: "later", ttlMs: 300 });
    const delivered = await nextFrame(8000);
    expect(delivered).toMatchObject({ type: "message", data: "later" });
    ws.close();
  });

  it("closes subscriptions with 4410 when the queue is deleted", async () => {
    await declareQueue("doomed");
    const { ws, nextFrame, closed } = openConsumer(base, "doomed");
    await waitOpen(ws);
    ws.send(JSON.stringify({ action: "hello", prefetch: 5 }));
    await nextFrame();

    const res = await fetch(`${base}/queues/doomed`, { method: "DELETE" });
    expect(res.status).toBe(204);
    await expect(closed).resolves.toMatchObject({ code: 4410 });
  });

  it("keeps the live subscriber when a stale socket with the same consumer id closes", async () => {
    await declareQueue("shared");
    const a = openConsumer(base, "shared");
    await waitOpen(a.ws);
    a.ws.send(JSON.stringify({ action: "hello", consumerId: "c-shared", prefetch: 5 }));
    await a.nextFrame();

    const b = openConsumer(base, "shared");
    await waitOpen(b.ws);
    b.ws.send(JSON.stringify({ action: "hello", consumerId: "c-shared", prefetch: 5 }));
    await b.nextFrame();

    // The old socket's close must not unregister the replacement.
    a.ws.close();
    await a.closed;

    const { id } = await publish("shared", { data: "still-yours" });
    const delivered = await b.nextFrame();
    expect(delivered).toMatchObject({ type: "message", id, data: "still-yours" });
    b.ws.close();
  });

  it("answers garbage before hello with an error but still accepts the hello", async () => {
    await declareQueue("strict2");
    const { ws, nextFrame } = openConsumer(base, "strict2");
    await waitOpen(ws);
    ws.send("not-json{{{");
    await expect(nextFrame()).resolves.toMatchObject({ type: "error", code: "PROTOCOL_ERROR" });
    ws.send(JSON.stringify({ action: "hello", prefetch: 2 }));
    const ready = await nextFrame();
    expect(ready).toMatchObject({ type: "ready", queue: "strict2" });
    expect(ws.readyState).toBe(WebSocket.OPEN);
    ws.close();
  });
});
