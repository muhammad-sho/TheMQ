import type * as WebSocket from "ws";
import { ApiError } from "../errors.js";
import { frameToString } from "../frames.js";
import type { OutgoingMessage, SubscriberHandle } from "../../broker/subscriptions.js";
import { queueParamsSchema } from "../schemas/common.js";
import { helloSchema } from "../schemas/subscribe.js";
import { parseWith } from "./helpers.js";
import type { ApiServices, AppInstance } from "../server.js";

/**
 * WebSocket close codes used by the subscribe endpoint (1000/1011 are
 * standard; 4xxx are application-defined).
 */
export const SUBSCRIBE_CLOSE_UNKNOWN_QUEUE = 4404;
export const SUBSCRIBE_CLOSE_PROTOCOL_ERROR = 4400;
export const SUBSCRIBE_CLOSE_QUEUE_DELETED = 4410;

const HELLO_TIMEOUT_MS = 10_000;
const HEARTBEAT_INTERVAL_MS = 30_000;

interface ServerFrame {
  type: string;
  [key: string]: unknown;
}

function sendFrame(socket: WebSocket.WebSocket, frame: ServerFrame): void {
  socket.send(JSON.stringify(frame));
}

function errorFrame(code: string, message: string, id?: string): ServerFrame {
  return {
    type: "error",
    code,
    message,
    ...(id !== undefined ? { id } : {}),
  };
}

/**
 * Persistent consumer endpoint: `GET /queues/:queue/subscribe`.
 *
 * The client opens a WebSocket, sends one `hello`, then receives
 * `message` frames as deliveries happen — no polling. It settles with
 * `ack` / `requeue` frames and ends with `cancel` or by closing.
 * Deliveries are held RabbitMQ-style: no visibility deadline, so a
 * message stays leased however long processing takes. Closing (or a
 * dropped connection) requeues pending messages, so unacknowledged
 * work is redelivered. The `visibilityTimeoutMs` hello field is
 * accepted for backward compatibility but no longer sets an expiry.
 */
export function registerSubscribeRoutes(app: AppInstance, services: ApiServices): void {
  const { broker, subscriptions, logger, config } = services;

  app.get("/queues/:queue/subscribe", { websocket: true }, (socket, request) => {
    void handleSubscription(socket, request).catch((err: unknown) => {
      logger.warn({ err, event: "subscribe-failed" }, "Subscription setup failed");
      try {
        socket.close(1011, "Internal error");
      } catch {
        // ignore — socket is already gone
      }
    });
  });

  async function handleSubscription(
    socket: WebSocket.WebSocket,
    request: { params: unknown },
  ): Promise<void> {
    const params = parseWith(queueParamsSchema, request.params, "path parameters");
    const queue = params.queue;

    // Attach listeners before the first await: a fast client hello
    // arriving during the queue check would otherwise be dropped (ws
    // does not buffer 'message' events for later listeners). Frames run
    // through a promise chain to keep their order.
    let handle: SubscriberHandle | undefined;
    let consumerId = "";
    let settled = false;
    // Set only by an accepted hello: junk frames must not disarm the
    // hello timeout, or a client spraying garbage could idle forever.
    let helloAccepted = false;
    let helloTimer: ReturnType<typeof setTimeout> | undefined = setTimeout(() => {
      helloTimer = undefined;
      if (helloAccepted) return;
      try {
        sendFrame(socket, errorFrame("PROTOCOL_ERROR", "Expected a hello frame first."));
        socket.close(SUBSCRIBE_CLOSE_PROTOCOL_ERROR, "No hello frame");
      } catch {
        // ignore — socket is already gone
      }
    }, HELLO_TIMEOUT_MS);
    let heartbeatTimer: ReturnType<typeof setInterval> | undefined;
    let awaitingPong = false;

    const cleanup = (): void => {
      if (settled) return;
      settled = true;
      if (helloTimer) {
        clearTimeout(helloTimer);
        helloTimer = undefined;
      }
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = undefined;
      }
      offDelete();
      if (handle) {
        const done = handle;
        handle = undefined;
        void dropRegistration(done);
      }
      logger.info({ event: "unsubscribed", queue, consumer: consumerId }, "Consumer disconnected");
    };

    /**
     * Forget a registration and requeue its pending messages so
     * unacknowledged work is redelivered. Shared by disconnect cleanup
     * and mid-subscribe aborts.
     */
    const dropRegistration = async (sub: SubscriberHandle): Promise<void> => {
      subscriptions.remove(sub);
      await broker.cancelConsumer(queue, sub.consumerId).catch((err: unknown) => {
        logger.warn({ err, queue, event: "cancel-failed" }, "Cancel on disconnect failed");
      });
    };

    let frameChain: Promise<void> = Promise.resolve();
    socket.on("message", (raw: WebSocket.RawData) => {
      frameChain = frameChain
        .then(() => onClientFrame(raw))
        .catch((err: unknown) => {
          logger.warn({ err, queue, event: "frame-failed" }, "Client frame handling failed");
        });
    });
    socket.on("close", cleanup);
    socket.on("error", (err: Error) => {
      logger.warn({ err, queue, event: "socket-error" }, "Consumer socket error");
    });

    const offDelete = broker.onDeleteQueue((deleted) => {
      if (deleted === queue) {
        try {
          socket.close(SUBSCRIBE_CLOSE_QUEUE_DELETED, "Queue deleted");
        } catch {
          // ignore — socket is already gone
        }
      }
    });

    try {
      await broker.getQueue(queue);
    } catch (err) {
      if (err instanceof ApiError && err.code === "NOT_FOUND") {
        sendFrame(socket, errorFrame("NOT_FOUND", `Queue '${queue}' not found.`));
        socket.close(SUBSCRIBE_CLOSE_UNKNOWN_QUEUE, "Unknown queue");
        return;
      }
      throw err;
    }
    // Frames chained during the queue check process now, in order.
    async function onClientFrame(raw: WebSocket.RawData): Promise<void> {
      let frame: unknown;
      try {
        frame = JSON.parse(frameToString(raw)) as unknown;
      } catch {
        sendFrame(socket, errorFrame("PROTOCOL_ERROR", "Frame must be JSON."));
        return;
      }
      const record = (typeof frame === "object" && frame !== null ? frame : {}) as Record<
        string,
        unknown
      >;
      if (handle === undefined) {
        const input = parseHello(record);
        if (input === undefined) return;
        helloAccepted = true;
        if (helloTimer) {
          clearTimeout(helloTimer);
          helloTimer = undefined;
        }
        const sub = await subscribeOrFail(input);
        if (sub === undefined) return;
        if (settled) {
          // Socket closed mid-subscribe: drop the fresh registration
          // instead of leaking a dead consumer.
          await dropRegistration(sub);
          return;
        }
        handle = sub;
        consumerId = sub.consumerId;
        sendFrame(socket, {
          type: "ready",
          queue,
          consumerId,
          prefetch: input.prefetch ?? config.defaultPrefetch,
          // Echoed for wire compatibility only; persistent deliveries
          // carry no deadline.
          visibilityTimeoutMs: config.defaultVisibilityTimeoutMs,
        });
        logger.info({ event: "subscribed", queue, consumer: consumerId }, "Consumer connected");
        heartbeatTimer = setInterval(() => {
          if (awaitingPong) {
            try {
              socket.terminate();
            } catch {
              // ignore — socket is already gone
            }
            return;
          }
          awaitingPong = true;
          try {
            socket.ping();
          } catch {
            // ignore — the close handler cleans up
          }
        }, HEARTBEAT_INTERVAL_MS);
        socket.on("pong", () => {
          awaitingPong = false;
        });
        return;
      }
      await onActionFrame(record, handle);
    }

    /**
     * Register the consumer. Failures reach the client as an error frame
     * plus close — a waiting hello always gets an answer.
     */
    async function subscribeOrFail(input: {
      consumerId?: string;
      prefetch?: number;
    }): Promise<SubscriberHandle | undefined> {
      try {
        return await subscriptions.add({
          queue,
          ...(input.consumerId !== undefined ? { consumerId: input.consumerId } : {}),
          prefetch: input.prefetch ?? config.defaultPrefetch,
          send: (message: OutgoingMessage) => {
            sendFrame(socket, { type: "message", ...message });
          },
        });
      } catch (err) {
        if (err instanceof ApiError) {
          sendFrame(socket, errorFrame(err.code, err.message));
        } else {
          logger.warn({ err, queue, event: "subscribe-failed" }, "Subscription setup failed");
          sendFrame(socket, errorFrame("INTERNAL_ERROR", "Failed to subscribe."));
        }
        try {
          socket.close(1011, "Subscribe failed");
        } catch {
          // ignore — socket is already gone
        }
        return undefined;
      }
    }

    function parseHello(
      record: Record<string, unknown>,
    ): { consumerId?: string; prefetch?: number } | undefined {
      const result = helloSchema.safeParse(record);
      if (!result.success) {
        sendFrame(
          socket,
          errorFrame("PROTOCOL_ERROR", 'First frame must be {"action":"hello", ...}.'),
        );
        return undefined;
      }
      // visibilityTimeoutMs validates but is ignored: persistent
      // deliveries carry no deadline.
      return {
        ...(result.data.consumerId !== undefined ? { consumerId: result.data.consumerId } : {}),
        ...(result.data.prefetch !== undefined ? { prefetch: result.data.prefetch } : {}),
      };
    }

    async function onActionFrame(
      record: Record<string, unknown>,
      sub: SubscriberHandle,
    ): Promise<void> {
      const action = record["action"];
      if (action === "ack" || action === "requeue") {
        const id = record["id"];
        if (typeof id !== "string" || id === "") {
          sendFrame(socket, errorFrame("PROTOCOL_ERROR", "Action frames need a message id."));
          return;
        }
        try {
          const settled =
            action === "ack"
              ? await broker.ack(queue, id, sub.consumerId)
              : await broker.requeue(queue, id, sub.consumerId);
          sendFrame(socket, {
            type: action === "ack" ? "acked" : "requeued",
            id,
            deliveries: settled.deliveries,
          });
        } catch (err) {
          if (err instanceof ApiError) {
            sendFrame(socket, errorFrame(err.code, err.message, id));
            return;
          }
          throw err;
        }
        // Capacity freed — top this consumer up immediately.
        await subscriptions.fillConsumer(sub).catch((err: unknown) => {
          logger.warn({ err, queue, event: "fill-failed" }, "Subscription top-up failed");
        });
        return;
      }
      if (action === "cancel") {
        try {
          const { requeued } = await broker.cancelConsumer(queue, sub.consumerId);
          sendFrame(socket, { type: "cancelled", requeued });
        } catch (err) {
          if (err instanceof ApiError) {
            sendFrame(socket, errorFrame(err.code, err.message));
          } else {
            throw err;
          }
        }
        socket.close(1000, "Cancelled by consumer");
        return;
      }
      sendFrame(socket, errorFrame("PROTOCOL_ERROR", `Unknown action '${String(action)}'.`));
    }
  }
}
