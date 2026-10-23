import { setTimeout as sleep } from "node:timers/promises";
import WebSocket from "ws";
import type {
  IDataObject,
  IExecuteResponsePromiseData,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
  IRun,
  ITriggerFunctions,
  ITriggerResponse,
} from "n8n-workflow";
import { NodeConnectionTypes, NodeOperationError } from "n8n-workflow";
import {
  describeApiError,
  errorMessage,
  isAlreadySettled,
  isFatalConnectionError,
} from "../shared/errors.js";
import {
  frameString,
  parseDeliveredMessage,
  parseFrame,
  type DeliveredMessage,
} from "../shared/frames.js";
import { normalizeBaseUrl } from "../shared/http.js";

type AcknowledgeMode =
  "immediately" | "executionFinishes" | "executionFinishesSuccessfully" | "laterMessageNode";

interface TriggerOptions {
  acknowledge?: AcknowledgeMode;
  maxConcurrentExecutions?: number;
}

interface TheMqCredentials {
  baseUrl?: string;
  apiToken?: string;
}

interface ReadyInfo {
  consumerId: string;
  /** Messages pushed before the ready frame; replay after connecting. */
  early: DeliveredMessage[];
}

type RaceResult = { kind: "hook"; real: boolean } | { kind: "run"; data: IRun };

/** Bounds mirror the server contract (prefetch cap). */
const MAX_CONCURRENT_CAP = 1000;
const IMMEDIATE_PREFETCH = 1000;
const MANUAL_PREFETCH = 1;
const CONNECT_TIMEOUT_MS = 15_000;
const HELLO_TIMEOUT_MS = 15_000;
const SETTLE_TIMEOUT_MS = 10_000;
const CLOSE_GRACE_MS = 5000;
const CLOSE_WAIT_MAX_ROUNDS = 60;
const CLOSE_WAIT_INTERVAL_MS = 1000;
const WS_NORMAL_CLOSE = 1000;
const RECONNECT_INITIAL_MS = 1000;
const RECONNECT_MAX_MS = 30_000;
const RECONNECT_JITTER_MS = 1000;
/**
 * Close codes that never trigger a reconnect: clean shutdowns plus
 * server rejections no retry can fix (auth/queue/protocol).
 */
const NO_RECONNECT_CLOSE = new Set([WS_NORMAL_CLOSE, 4400, 4404, 4410]);

/** Connect failure in plain language (raw socket errors are cryptic). */
function describeConnectionError(error: unknown, queue: string, baseUrl: string): string {
  const message = error instanceof Error ? error.message : String(error);
  if (/401|unauthorized/i.test(message)) {
    return `TheMQ rejected the connection for queue "${queue}" (unauthorized). Check the API Token in your TheMQ API credential.`;
  }
  return (
    `TheMQ is not reachable at ${baseUrl} for queue "${queue}": ${message} ` +
    "Check that TheMQ is running and the Base URL in your credential is correct."
  );
}

export class TheMqTrigger implements INodeType {
  description: INodeTypeDescription = {
    displayName: "TheMQ Trigger",
    name: "theMqTrigger",
    icon: "file:themq.png",
    group: ["trigger"],
    version: 1,
    description: "Listens to TheMQ messages",
    defaults: {
      name: "TheMQ Trigger",
    },
    triggerPanel: {
      header: "",
      executionsHelp: {
        inactive:
          "<b>While building your workflow</b>, click the 'execute step' button, then publish a message to a TheMQ queue. This will trigger an execution, which will show up in this editor.<br /> <br /><b>Once you're happy with your workflow</b>, publish it. Then every time a message arrives, the workflow will execute. These executions will show up in the <a data-key='executions'>executions list</a>, but not in the editor.",
        active:
          "<b>While building your workflow</b>, click the 'execute step' button, then publish a message to a TheMQ queue. This will trigger an execution, which will show up in this editor.<br /> <br /><b>Your workflow will also execute automatically</b>, since it's activated. Every time a message arrives, this node will trigger an execution. These executions will show up in the <a data-key='executions'>executions list</a>, but not in the editor.",
      },
      activationHint:
        "Once you've finished building your workflow, publish it to have it also listen continuously (you just won't see those executions here).",
    },
    inputs: [],
    outputs: [NodeConnectionTypes.Main],
    credentials: [
      {
        name: "theMqApi",
        required: true,
      },
    ],
    properties: [
      {
        displayName: "Queue",
        name: "queue",
        type: "string",
        default: "",
        required: true,
        placeholder: "queue-name",
        description: "The name of the queue to listen to (declared automatically if missing)",
      },
      {
        displayName: "Options",
        name: "options",
        type: "collection",
        default: {},
        placeholder: "Add option",
        options: [
          {
            displayName: "Acknowledge",
            name: "acknowledge",
            type: "options",
            options: [
              {
                name: "Execution Finishes",
                value: "executionFinishes",
                description:
                  "After the workflow execution finished, however long it takes. The message is held (never redelivered) until then, and acknowledged whether the execution was successful or not.",
              },
              {
                name: "Execution Finishes Successfully",
                value: "executionFinishesSuccessfully",
                description:
                  "After the workflow execution finished successfully, however long it takes. The message is held (never redelivered) until then. On failure it goes back to the queue and is delivered again.",
              },
              {
                name: "Immediately",
                value: "immediately",
                description:
                  "As soon as the message arrives, without a concurrency limit (RabbitMQ auto-ack). The workflow still runs, but a failure can no longer return the message.",
              },
              {
                name: "Specified Later in Workflow",
                value: "laterMessageNode",
                description:
                  "Using a TheMQ node to acknowledge the message. The message is held without a deadline until then. If the run ends without one, success acknowledges and failure returns the message.",
              },
            ],
            default: "immediately",
            description: "When to acknowledge the message (held without a deadline until then)",
          },
          {
            displayName: "Max Concurrent Executions",
            name: "maxConcurrentExecutions",
            type: "number",
            default: 1,
            displayOptions: {
              hide: {
                acknowledge: ["immediately"],
              },
            },
            description:
              "At most this many messages being processed at the same time (RabbitMQ prefetch). Further messages wait in the queue until one is acknowledged.",
          },
        ],
      },
      {
        displayName:
          "To acknowledge the message, insert a TheMQ node later in the workflow and use the 'Acknowledge' operation",
        name: "laterMessageNode",
        type: "notice",
        displayOptions: {
          show: {
            "/options.acknowledge": ["laterMessageNode"],
          },
        },
        default: "",
      },
    ],
  };

  async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
    const queue = this.getNodeParameter("queue") as string;
    const options = this.getNodeParameter("options", {}) as TriggerOptions;

    const acknowledgeMode: AcknowledgeMode = options.acknowledge ?? "immediately";
    const maxConcurrent = options.maxConcurrentExecutions ?? 1;
    if (acknowledgeMode !== "immediately") {
      if (!Number.isInteger(maxConcurrent) || maxConcurrent < 1) {
        throw new NodeOperationError(
          this.getNode(),
          "Max Concurrent Executions must be an integer greater than zero.",
        );
      }
      if (maxConcurrent > MAX_CONCURRENT_CAP) {
        throw new NodeOperationError(
          this.getNode(),
          `Max Concurrent Executions must be at most ${MAX_CONCURRENT_CAP}.`,
        );
      }
    }
    // Consumer identity is server-generated, so workflows never share a
    // lease by accident. Immediately is uncapped (server maximum);
    // otherwise the broker leases at most Max Concurrent Executions.
    // Deliveries carry no deadline: the broker holds each message until
    // it is acked, requeued, or this connection drops (RabbitMQ
    // manual-ack semantics).
    const prefetch = acknowledgeMode === "immediately" ? IMMEDIATE_PREFETCH : maxConcurrent;

    const credentials = (await this.getCredentials("theMqApi")) as unknown as TheMqCredentials;
    const baseUrl = normalizeBaseUrl(credentials.baseUrl ?? "");
    const apiToken = credentials.apiToken ?? "";
    const wsUrl = `${baseUrl.replace(/^http/, "ws")}/queues/${encodeURIComponent(queue)}/subscribe`;

    // Idempotent declare so listening on a fresh queue does not fail.
    try {
      await this.helpers.requestWithAuthentication.call(this, "theMqApi", {
        method: "PUT",
        baseURL: baseUrl,
        url: `/queues/${encodeURIComponent(queue)}`,
        body: {},
        json: true,
      });
    } catch (error) {
      throw new NodeOperationError(
        this.getNode(),
        describeApiError(error, { operation: "declare queue", queue, baseUrl }),
      );
    }

    const inflight = new Set<string>();
    const pendingSettles = new Map<string, { resolve: () => void; reject: (err: Error) => void }>();
    let closeRequested = false;
    let socket: WebSocket | undefined;
    let activeConsumerId = "";
    // Manual test runs are one-shot: never reconnect, fail visibly instead.
    const isManualRun = this.getMode() === "manual";
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    let reconnectDelayMs = RECONNECT_INITIAL_MS;

    const log = (level: "error" | "warn", message: string): void => {
      const workflow = this.getWorkflow();
      const node = this.getNode();
      const context = { node: node.name, workflowId: workflow.id };
      if (level === "error") {
        this.logger.error(
          `There was a problem with TheMQ Trigger node "${node.name}" in workflow "${workflow.id}": "${message}"`,
          context,
        );
      } else {
        this.logger.warn(
          `TheMQ Trigger node "${node.name}" in workflow "${workflow.id}": "${message}"`,
          context,
        );
      }
    };

    const openSocket = async (): Promise<WebSocket> => {
      const candidate = new WebSocket(wsUrl, {
        headers: { Authorization: `Bearer ${apiToken}` },
      });
      return new Promise<WebSocket>((resolve, reject) => {
        const timer = setTimeout(() => {
          candidate.terminate();
          reject(
            new Error(
              `Timed out connecting to TheMQ for queue "${queue}". Check that TheMQ is running and the Base URL in your credential is correct.`,
            ),
          );
        }, CONNECT_TIMEOUT_MS);
        candidate.once("open", () => {
          clearTimeout(timer);
          resolve(candidate);
        });
        candidate.once("error", (err: Error) => {
          clearTimeout(timer);
          reject(new Error(describeConnectionError(err, queue, baseUrl)));
        });
      });
    };

    const waitReady = async (candidate: WebSocket): Promise<ReadyInfo> => {
      return new Promise<ReadyInfo>((resolve, reject) => {
        const timer = setTimeout(() => {
          cleanup();
          try {
            candidate.close(WS_NORMAL_CLOSE, "Hello reply timed out");
          } catch {
            // ignore — socket is already gone
          }
          reject(
            new Error(
              `Timed out waiting for a TheMQ hello reply for queue "${queue}" (${wsUrl}). ` +
                "Is TheMQ 3.0.0+ running and reachable from n8n, and is Redis healthy? " +
                "Check TheMQ logs for subscribe/Redis errors.",
            ),
          );
        }, HELLO_TIMEOUT_MS);
        // Deliveries can arrive BEFORE the ready frame — buffer them for
        // replay once the consumer id is known, so none is ever dropped.
        const early: DeliveredMessage[] = [];
        const cleanup = (): void => {
          clearTimeout(timer);
          candidate.off("message", onFrame);
          candidate.off("close", onClose);
        };
        const onFrame = (raw: WebSocket.RawData): void => {
          const frame = parseFrame(raw);
          if (frame === undefined) return;
          if (frame["type"] === "ready" && typeof frame["consumerId"] === "string") {
            cleanup();
            resolve({ consumerId: frame["consumerId"], early });
          } else if (frame["type"] === "message") {
            const delivered = parseDeliveredMessage(frame);
            if (delivered) early.push(delivered);
          } else if (frame["type"] === "error") {
            cleanup();
            const code = frameString(frame["code"]) || "ERROR";
            const message = frameString(frame["message"]) || "subscribe failed";
            reject(new Error(`TheMQ ${code}: ${message}`));
          }
        };
        const onClose = (): void => {
          cleanup();
          reject(new Error("TheMQ connection closed before the hello reply"));
        };
        candidate.on("message", onFrame);
        candidate.once("close", onClose);
      });
    };

    /** Acknowledge/requeue over the persistent connection. */
    const sendSettle = (action: "ack" | "requeue", id: string): Promise<void> => {
      return new Promise<void>((resolve, reject) => {
        const current = socket;
        if (!current || current.readyState !== WebSocket.OPEN) {
          reject(new Error("TheMQ connection is not open"));
          return;
        }
        const timer = setTimeout(() => {
          pendingSettles.delete(id);
          reject(new Error("Timed out settling the TheMQ message"));
        }, SETTLE_TIMEOUT_MS);
        pendingSettles.set(id, {
          resolve: () => {
            clearTimeout(timer);
            resolve();
          },
          reject: (err: Error) => {
            clearTimeout(timer);
            reject(err);
          },
        });
        try {
          current.send(JSON.stringify({ action, id }));
        } catch (error) {
          pendingSettles.delete(id);
          clearTimeout(timer);
          reject(error instanceof Error ? error : new Error(String(error)));
        }
      });
    };

    const settle = async (action: "ack" | "requeue", id: string): Promise<void> => {
      try {
        await sendSettle(action, id);
      } catch (error) {
        // Already settled elsewhere — nothing left to do.
        if (!isAlreadySettled(error)) throw error;
      }
    };

    const routeReplyFrame = (frame: IDataObject): void => {
      const id = frame["id"];
      if (typeof id !== "string") return;
      const pending = pendingSettles.get(id);
      if (!pending) return;
      pendingSettles.delete(id);
      if (frame["type"] === "acked" || frame["type"] === "requeued") {
        pending.resolve();
      } else if (frame["type"] === "error") {
        const code = frameString(frame["code"]) || "ERROR";
        const message = frameString(frame["message"]) || "settle failed";
        pending.reject(new Error(`TheMQ ${code}: ${message}`));
      }
    };

    const toItem = (message: DeliveredMessage): INodeExecutionData => ({
      json: {
        queue,
        messageId: message.id,
        consumerId: activeConsumerId,
        data: message.data as IDataObject,
        deliveryCount: message.deliveryCount,
        redelivered: message.redelivered,
      },
    });

    const handleMessage = async (message: DeliveredMessage): Promise<void> => {
      if (closeRequested) return;
      inflight.add(message.id);
      try {
        const item = toItem(message);
        if (acknowledgeMode === "immediately") {
          this.emit([[item]]);
          await settle("ack", message.id);
          return;
        }
        if (acknowledgeMode === "laterMessageNode") {
          const responsePromiseHook =
            this.helpers.createDeferredPromise<IExecuteResponsePromiseData>();
          // Also await execution end, so a failed run requeues even when
          // no TheMQ node fires first.
          const responsePromise = this.helpers.createDeferredPromise<IRun>();
          this.emit([[item]], responsePromiseHook, responsePromise);
          const first = await Promise.race<RaceResult>([
            responsePromiseHook.promise.then((data): RaceResult => ({
              kind: "hook",
              real:
                data !== null && typeof data === "object" && Object.keys(data as object).length > 0,
            })),
            responsePromise.promise.then((data): RaceResult => ({ kind: "run", data })),
          ]);
          if (first.kind === "hook" && first.real) {
            await settle("ack", message.id);
          } else {
            const run = first.kind === "run" ? first.data : await responsePromise.promise;
            if (run?.data?.resultData?.error) {
              await settle("requeue", message.id);
            } else {
              await settle("ack", message.id);
            }
          }
          return;
        }
        const responsePromise = this.helpers.createDeferredPromise<IRun>();
        this.emit([[item]], undefined, responsePromise);
        const run = await responsePromise.promise;
        if (run?.data?.resultData?.error) {
          if (acknowledgeMode === "executionFinishesSuccessfully") {
            await settle("requeue", message.id);
            return;
          }
        }
        await settle("ack", message.id);
      } catch (error) {
        log("error", `message "${message.id}": ${errorMessage(error)}`);
      } finally {
        inflight.delete(message.id);
      }
    };

    const onSocketFrame = (raw: WebSocket.RawData): void => {
      const frame = parseFrame(raw);
      if (frame === undefined) return;
      if (frame["type"] === "message") {
        const delivered = parseDeliveredMessage(frame);
        if (delivered) void handleMessage(delivered);
        return;
      }
      if (frame["type"] === "acked" || frame["type"] === "requeued" || frame["type"] === "error") {
        routeReplyFrame(frame);
        if (frame["type"] === "error" && frame["id"] === undefined) {
          const code = frameString(frame["code"]) || "ERROR";
          const message = frameString(frame["message"]);
          log("error", message === "" ? `TheMQ ${code}` : `TheMQ ${code}: ${message}`);
        }
      }
    };

    const rejectPendingSettles = (message: string): void => {
      for (const [, pending] of pendingSettles) {
        pending.reject(new Error(message));
      }
      pendingSettles.clear();
    };

    const closeFunction = async (): Promise<void> => {
      closeRequested = true;
      if (reconnectTimer !== undefined) {
        clearTimeout(reconnectTimer);
        reconnectTimer = undefined;
      }
      // Bounded grace period for in-flight executions; leftovers requeue
      // server-side on close.
      let waits = 0;
      while (inflight.size > 0 && waits++ < CLOSE_WAIT_MAX_ROUNDS) {
        await sleep(CLOSE_WAIT_INTERVAL_MS);
      }
      rejectPendingSettles("TheMQ Trigger is closing");
      const current = socket;
      socket = undefined;
      if (current) {
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, CLOSE_GRACE_MS);
          current.once("close", () => {
            clearTimeout(timer);
            resolve();
          });
          try {
            current.close(WS_NORMAL_CLOSE, "Trigger deactivated");
          } catch {
            clearTimeout(timer);
            resolve();
          }
        });
      }
    };

    const connectConsumer = async (
      helloPrefetch: number,
      attachMessageHandler: boolean,
    ): Promise<DeliveredMessage[]> => {
      const candidate = await openSocket();
      socket = candidate;
      candidate.send(
        JSON.stringify({
          action: "hello",
          prefetch: helloPrefetch,
        }),
      );
      const ready = await waitReady(candidate);
      activeConsumerId = ready.consumerId;
      if (attachMessageHandler) {
        candidate.on("message", onSocketFrame);
      }
      candidate.on("error", (err: Error) => {
        log("error", err.message);
      });
      candidate.on("close", (code: number) => {
        if (socket === candidate) socket = undefined;
        rejectPendingSettles("TheMQ connection closed");
        if (closeRequested) return;
        // Manual test runs fail visibly; the live trigger rides out
        // transport drops (server restarts, deploys, blips) by
        // reconnecting — the server requeued our leases on its side.
        // Only rejections no retry can fix surface as errors.
        if (isManualRun || NO_RECONNECT_CLOSE.has(code)) {
          this.emitError(new Error(`TheMQ connection closed (code ${String(code)})`));
          return;
        }
        scheduleReconnect();
      });
      return ready.early;
    };

    /** Re-establish the consumer after a drop; back off while unreachable. */
    const establish = async (): Promise<void> => {
      const early = await connectConsumer(prefetch, true);
      if (closeRequested) {
        // Deactivated mid-handshake: drop the fresh connection at once
        // instead of leaking a live consumer nobody reads from.
        const current = socket;
        socket = undefined;
        try {
          current?.close(WS_NORMAL_CLOSE, "Trigger deactivated");
        } catch {
          // ignore — socket is already gone
        }
        return;
      }
      reconnectDelayMs = RECONNECT_INITIAL_MS;
      for (const message of early) {
        void handleMessage(message);
      }
    };

    const scheduleReconnect = (): void => {
      if (closeRequested || isManualRun || reconnectTimer !== undefined) return;
      const wait = reconnectDelayMs + Math.floor(Math.random() * RECONNECT_JITTER_MS);
      reconnectDelayMs = Math.min(reconnectDelayMs * 2, RECONNECT_MAX_MS);
      log("warn", `connection lost; reconnecting in ${(wait / 1000).toFixed(1)}s`);
      reconnectTimer = setTimeout(() => {
        reconnectTimer = undefined;
        if (closeRequested) return;
        establish().catch((error: unknown) => {
          if (closeRequested) return;
          const message = errorMessage(error);
          if (isFatalConnectionError(error)) {
            this.emitError(new Error(message));
            return;
          }
          log("warn", `reconnect failed (${message}); retrying`);
          scheduleReconnect();
        });
      }, wait);
    };

    if (isManualRun) {
      const manualTriggerFunction = async (): Promise<void> => {
        // Catch one message for the editor test run, then disconnect.
        // No execution hooks here: the test run settles it immediately.
        // Hello uses prefetch 1, so at most one message can arrive early.
        const early = await connectConsumer(MANUAL_PREFETCH, false);
        const first =
          early.length > 0
            ? early[0]
            : await new Promise<DeliveredMessage | undefined>((resolve) => {
                const current = socket;
                if (!current) {
                  resolve(undefined);
                  return;
                }
                const onFrame = (raw: WebSocket.RawData): void => {
                  const frame = parseFrame(raw);
                  if (frame === undefined) return;
                  if (frame["type"] === "message") {
                    const delivered = parseDeliveredMessage(frame);
                    if (delivered) {
                      current.off("message", onFrame);
                      resolve(delivered);
                    }
                  }
                };
                current.on("message", onFrame);
              });
        if (first) {
          this.emit([[toItem(first)]]);
          await settle("ack", first.id).catch((error: unknown) => {
            log("error", `message "${first.id}": ${errorMessage(error)}`);
          });
        }
        await closeFunction();
      };

      return {
        closeFunction,
        manualTriggerFunction,
      };
    }

    // Initial connect still throws: misconfiguration (bad URL, bad
    // token) must fail activation visibly. Only established drops
    // reconnect.
    await establish();

    return {
      closeFunction,
    };
  }
}
