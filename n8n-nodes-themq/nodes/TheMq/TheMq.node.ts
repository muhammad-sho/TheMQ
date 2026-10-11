import type {
  IDataObject,
  IExecuteFunctions,
  NodeParameterValueType,
  IHttpRequestMethods,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
} from "n8n-workflow";
import { NodeConnectionTypes, NodeOperationError } from "n8n-workflow";

/** Narrow an API value to n8n's item-data type. */
function toDataObject(value: unknown): IDataObject {
  if (typeof value === "object" && value !== null) return value as IDataObject;
  return {};
}

/** Narrow node parameters explicitly. */
function strParam(value: NodeParameterValueType | object, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function numParam(value: NodeParameterValueType | object, fallback: number): number {
  return typeof value === "number" ? value : fallback;
}

function boolParam(value: NodeParameterValueType | object, fallback = false): boolean {
  return typeof value === "boolean" ? value : fallback;
}

export class TheMq implements INodeType {
  description: INodeTypeDescription = {
    displayName: "TheMQ",
    name: "theMq",
    icon: "file:themq.png",
    group: ["transform"],
    version: 1,
    subtitle: '={{$parameter["operation"]}}',
    description: "Publish and settle messages in TheMQ queues",
    defaults: {
      name: "TheMQ",
    },
    inputs: [NodeConnectionTypes.Main],
    outputs: [NodeConnectionTypes.Main],
    credentials: [
      {
        name: "theMqApi",
        required: true,
      },
    ],
    properties: [
      {
        displayName: "Operation",
        name: "operation",
        type: "options",
        noDataExpression: true,
        options: [
          {
            name: "Publish",
            value: "publish",
            description: "Publish a message to a queue",
            action: "Publish a message",
          },
          {
            name: "Acknowledge",
            value: "ack",
            description:
              "Mark a trigger-delivered message as successfully processed (also resolves a waiting 'Specified Later in Workflow' trigger)",
            action: "Acknowledge a message",
          },
          {
            name: "Requeue",
            value: "requeue",
            description:
              "Ensure a message is queued: requeues it when leased, updates it when waiting, publishes it when missing",
            action: "Requeue a message",
          },
          {
            name: "Delete Message",
            value: "delete",
            description: "Delete a waiting message so it is never processed",
            action: "Delete a message",
          },
        ],
        default: "publish",
      },
      {
        displayName: "Queue",
        name: "queue",
        type: "string",
        default: "={{ $json.queue }}",
        required: true,
        description: "Name of the queue (taken from the trigger item when connected)",
      },
      {
        displayName: "Message ID",
        name: "messageId",
        type: "string",
        default: "={{ $json.messageId }}",
        required: true,
        description:
          "Unique id of the message. When publishing this is the upsert key; when acknowledging, requeueing, or deleting it is taken from the trigger item when connected.",
      },
      {
        displayName: "Message Data",
        name: "messageData",
        type: "json",
        default: '{\n  "message": "hello"\n}',
        required: true,
        displayOptions: { show: { operation: ["publish"] } },
        description: "Arbitrary JSON payload for the message",
      },
      {
        displayName: "Upsert",
        name: "upsert",
        type: "boolean",
        default: false,
        displayOptions: { show: { operation: ["publish"] } },
        description:
          "Update the message in place when the ID already exists (new data and delay), instead of failing with a conflict.",
      },
      {
        displayName: "On Conflict",
        name: "onConflict",
        type: "options",
        default: "error",
        displayOptions: { show: { operation: ["publish"] } },
        options: [
          {
            name: "Error",
            value: "error",
            description: "Fail with a conflict error when the ID already exists",
            action: "Fail on conflict",
          },
          {
            name: "Skip",
            value: "skip",
            description:
              "Leave the existing message untouched and return its current state with skipped: true",
            action: "Skip on conflict",
          },
        ],
        description:
          "What to do when the message ID already exists (queued or leased). Upsert above takes precedence when enabled.",
      },
      {
        displayName: "Delay (Ms)",
        name: "ttlMs",
        type: "number",
        default: 0,
        displayOptions: { show: { operation: ["publish"] } },
        description: "Wait this long before the message becomes available (0 = immediately)",
      },
      {
        displayName: "Options",
        name: "options",
        type: "collection",
        default: {},
        placeholder: "Add option",
        displayOptions: { show: { operation: ["requeue"] } },
        options: [
          {
            displayName: "Message Data",
            name: "messageData",
            type: "json",
            default: '{\n  "message": "hello"\n}',
            description: "Replacement payload for the next delivery",
          },
          {
            displayName: "Delay (Ms)",
            name: "ttlMs",
            type: "number",
            default: 60000,
            description: "Wait this long before the message becomes available again",
          },
        ],
      },
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData();
    const returnData: INodeExecutionData[] = [];
    const operation = strParam(this.getNodeParameter("operation", 0));

    for (let i = 0; i < items.length; i++) {
      const queue = strParam(this.getNodeParameter("queue", i, ""));
      const messageId = strParam(this.getNodeParameter("messageId", i, "")).trim();
      try {
        if (queue === "") {
          throw new NodeOperationError(this.getNode(), "Queue is required.", { itemIndex: i });
        }
        if (messageId === "") {
          throw new NodeOperationError(this.getNode(), "Message ID is required.", {
            itemIndex: i,
          });
        }
        if (operation === "publish") {
          const dataParam: unknown = this.getNodeParameter("messageData", i);
          let data: unknown;
          if (typeof dataParam === "string") {
            try {
              data = JSON.parse(dataParam) as unknown;
            } catch {
              throw new NodeOperationError(this.getNode(), "Message Data is not valid JSON.", {
                itemIndex: i,
              });
            }
          } else {
            data = dataParam;
          }
          const ttlMs = numParam(this.getNodeParameter("ttlMs", i, 0), 0);
          const upsert = boolParam(this.getNodeParameter("upsert", i, false));
          const onConflict = strParam(this.getNodeParameter("onConflict", i, "error"), "error");
          const body: Record<string, unknown> = { id: messageId, data };
          if (ttlMs > 0) body["ttlMs"] = ttlMs;
          if (upsert) body["upsert"] = true;
          if (onConflict === "skip") body["onConflict"] = "skip";
          const response = await request.call(
            this,
            "POST",
            `/queues/${encode(queue)}/messages`,
            body,
            { itemIndex: i, operation, queue, messageId },
          );
          returnData.push({ json: toDataObject(response) });
        } else if (operation === "ack") {
          const response = toDataObject(
            await request.call(
              this,
              "POST",
              `/queues/${encode(queue)}/messages/${encode(messageId)}/ack`,
              undefined,
              { itemIndex: i, operation, queue, messageId },
            ),
          );
          // Resolve a waiting trigger fast (best-effort: the HTTP
          // acknowledgement above already settled).
          try {
            this.sendResponse({ ...items[i]?.json, acked: true });
          } catch {
            // ignore — standalone acknowledgement already succeeded
          }
          returnData.push({ json: { queue, messageId, acked: true, ...response } });
        } else if (operation === "requeue") {
          const options = this.getNodeParameter("options", i, {}) as {
            messageData?: unknown;
            ttlMs?: number;
          };
          let replacement: unknown;
          let hasReplacement = false;
          if (options.messageData !== undefined) {
            const dataParam: unknown = options.messageData;
            if (typeof dataParam === "string") {
              try {
                replacement = JSON.parse(dataParam) as unknown;
              } catch {
                throw new NodeOperationError(this.getNode(), "Message Data is not valid JSON.", {
                  itemIndex: i,
                });
              }
            } else {
              replacement = dataParam;
            }
            hasReplacement = true;
          }
          const delayMs =
            typeof options.ttlMs === "number" && options.ttlMs > 0
              ? Math.floor(options.ttlMs)
              : undefined;
          const requeueBody: Record<string, unknown> = {
            ...(hasReplacement ? { data: replacement } : {}),
            ...(delayMs !== undefined ? { ttlMs: delayMs } : {}),
          };
          const requeuePath = `/queues/${encode(queue)}/messages/${encode(messageId)}/requeue`;
          const requestContext = { itemIndex: i, operation, queue, messageId };
          try {
            const response = await request.call(this, "POST", requeuePath, requeueBody, {
              ...requestContext,
            });
            returnData.push({ json: toDataObject(response) });
          } catch (requeueError) {
            // Forgiving requeue: the fast path above covers the normal
            // leased case in one call. Anything else (already waiting,
            // never published) is resolved into "queued" instead of
            // surfacing a state error.
            const ensured = await ensureQueued(
              this,
              queue,
              messageId,
              hasReplacement ? replacement : undefined,
              hasReplacement,
              delayMs,
              toDataObject(items[i]?.json ?? {}),
              requeueBody,
              requeuePath,
              requestContext,
              requeueError,
            );
            returnData.push({ json: ensured });
          }
        } else if (operation === "delete") {
          await request.call(
            this,
            "DELETE",
            `/queues/${encode(queue)}/messages/${encode(messageId)}`,
            undefined,
            { itemIndex: i, operation, queue, messageId },
          );
          returnData.push({ json: { queue, messageId, deleted: true } });
        } else {
          throw new NodeOperationError(this.getNode(), `Unknown operation "${operation}".`, {
            itemIndex: i,
          });
        }
      } catch (error) {
        if (this.continueOnFail()) {
          returnData.push({
            json: { error: error instanceof Error ? error.message : String(error) },
            pairedItem: { item: i },
          });
          continue;
        }
        throw error;
      }
    }
    return [returnData];
  }
}

function encode(value: string): string {
  return encodeURIComponent(value);
}

/** TheMQ error code from a formatted node error (undefined = transport/unknown). */
function apiCodeOf(error: unknown): string | undefined {
  const message = error instanceof Error ? error.message : String(error);
  return /TheMQ ([A-Z_]+):/.exec(message)?.[1];
}

/** GET a message; undefined when it does not exist. Other failures throw. */
async function fetchMessage(
  context: IExecuteFunctions,
  queue: string,
  messageId: string,
  requestContext: RequestContext,
): Promise<IDataObject | undefined> {
  try {
    return toDataObject(
      await request.call(
        context,
        "GET",
        `/queues/${encode(queue)}/messages/${encode(messageId)}`,
        undefined,
        { ...requestContext },
      ),
    );
  } catch (error) {
    if (apiCodeOf(error) === "NOT_FOUND") return undefined;
    throw error;
  }
}

/**
 * Resolve a failed requeue into "queued". Only NOT_FOUND/CONFLICT enter
 * here; anything else (auth, transport, validation) rethrows at once.
 * At most one state-changing retry follows the GET, so races settle
 * instead of looping.
 */
async function ensureQueued(
  context: IExecuteFunctions,
  queue: string,
  messageId: string,
  replacement: unknown,
  hasReplacement: boolean,
  delayMs: number | undefined,
  itemJson: IDataObject,
  requeueBody: Record<string, unknown>,
  requeuePath: string,
  requestContext: RequestContext,
  requeueError: unknown,
): Promise<IDataObject> {
  const code = apiCodeOf(requeueError);
  if (code !== "NOT_FOUND" && code !== "CONFLICT") throw requeueError;
  const current = await fetchMessage(context, queue, messageId, requestContext);
  if (current === undefined) {
    // Never published (or already gone): publish it. The trigger item
    // carries the original payload, so workflows that requeue a
    // trigger-delivered id without explicit data still work.
    const payload = hasReplacement ? replacement : itemJson["data"];
    if (payload === undefined) {
      throw new NodeOperationError(
        context.getNode(),
        `TheMQ NOT_FOUND: message "${messageId}" does not exist in queue "${queue}" and no replacement payload was provided. Add Message Data to the Requeue Options to create it.`,
        { itemIndex: requestContext.itemIndex },
      );
    }
    try {
      const published = await request.call(
        context,
        "POST",
        `/queues/${encode(queue)}/messages`,
        {
          id: messageId,
          data: payload,
          ...(delayMs !== undefined ? { ttlMs: delayMs } : {}),
          // Tolerate a race that created the message after the GET.
          upsert: true,
        },
        { ...requestContext },
      );
      return { ...toDataObject(published), queued: true };
    } catch (publishError) {
      if (apiCodeOf(publishError) === "CONFLICT") {
        // Lost the race: the message is leased now, so the requeue
        // that just failed will succeed on the new terms.
        return toDataObject(
          await request.call(context, "POST", requeuePath, requeueBody, {
            ...requestContext,
          }),
        );
      }
      throw publishError;
    }
  }
  if (current["state"] === "unacked") {
    // Changed under us between the requeue and the GET: one retry on
    // the current terms instead of reporting a stale state.
    return toDataObject(
      await request.call(context, "POST", requeuePath, requeueBody, {
        ...requestContext,
      }),
    );
  }
  // Already waiting (ready/delayed): success without moving it, unless
  // new terms were given.
  if (!hasReplacement && delayMs === undefined) {
    return {
      queue,
      messageId,
      requeued: false,
      alreadyQueued: true,
      state: current["state"],
      availableAt: current["availableAt"],
      deliveryCount: current["deliveryCount"],
      // Raw payload passthrough (arbitrary JSON, not necessarily an object).
      data: current["data"],
    };
  }
  if (hasReplacement) {
    // Waiting messages hold no lease, so upsert is the data path; it
    // resets the delivery count, which is 0 for never-delivered work.
    try {
      const published = await request.call(
        context,
        "POST",
        `/queues/${encode(queue)}/messages`,
        {
          id: messageId,
          data: replacement,
          ...(delayMs !== undefined ? { ttlMs: delayMs } : {}),
          upsert: true,
        },
        { ...requestContext },
      );
      return { ...toDataObject(published), alreadyQueued: true };
    } catch (publishError) {
      if (apiCodeOf(publishError) === "CONFLICT") {
        // Leased between the GET and the publish: requeue on the new
        // terms once instead of failing.
        return toDataObject(
          await request.call(context, "POST", requeuePath, requeueBody, {
            ...requestContext,
          }),
        );
      }
      throw publishError;
    }
  }
  // Delay-only: the TTL endpoint moves the message without touching
  // the payload or the delivery count.
  const moved = await request.call(
    context,
    "PUT",
    `/queues/${encode(queue)}/messages/${encode(messageId)}/ttl`,
    { ttl: delayMs },
    { ...requestContext },
  );
  return { queue, messageId, requeued: false, alreadyQueued: true, ...toDataObject(moved) };
}

/** Strip trailing slashes so `baseUrl + path` never doubles them. */
export function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.replace(/\/+$/, "");
}

export interface RequestContext {
  itemIndex: number;
  operation: string;
  queue?: string;
  messageId?: string;
  baseUrl?: string;
}

async function request(
  this: IExecuteFunctions,
  method: IHttpRequestMethods,
  path: string,
  body: Record<string, unknown> | undefined,
  context: RequestContext,
): Promise<IDataObject> {
  const credentials = (await this.getCredentials("theMqApi")) as unknown as {
    baseUrl?: string;
  };
  const baseUrl = normalizeBaseUrl(credentials.baseUrl ?? "");
  try {
    return toDataObject(
      await this.helpers.requestWithAuthentication.call(this, "theMqApi", {
        method,
        baseURL: baseUrl,
        url: path,
        ...(body !== undefined ? { body } : {}),
        json: true,
      }),
    );
  } catch (error) {
    throw new NodeOperationError(this.getNode(), describeApiError(error, { ...context, baseUrl }), {
      itemIndex: context.itemIndex,
    });
  }
}

/** One-line remediation per TheMQ error code (empty = the message says it all). */
function hintForCode(code: string, operation: string, message: string): string {
  switch (code) {
    case "UNAUTHENTICATED":
      return "Check the API Token in your TheMQ API credential.";
    case "NOT_FOUND":
      return "The queue or message does not exist, or was already removed.";
    case "CONFLICT":
      if (operation === "publish") {
        if (/unacked|lease/i.test(message)) {
          return "The message is leased to a consumer. Set On Conflict to Skip, or wait for the lease to settle.";
        }
        return "A message with this ID already exists. Enable Upsert to update it, or set On Conflict to Skip.";
      }
      if (operation === "ack") {
        return "The message is not leased. It may already be settled, or never picked up yet.";
      }
      if (operation === "requeue") {
        return "The message could not be returned to the queue. It may have been settled concurrently — run the node again.";
      }
      return "The message is leased (unacked). Acknowledge it first.";
    case "SERVICE_UNAVAILABLE":
      return "TheMQ cannot reach its Redis backend. Check that TheMQ and Redis are running.";
    default:
      return "";
  }
}

export function describeApiError(
  error: unknown,
  context: { operation: string; queue?: string; messageId?: string; baseUrl?: string },
): string {
  const where = [context.operation, context.queue ? `queue "${context.queue}"` : ""]
    .filter((part) => part !== "")
    .join(" ");
  const id = context.messageId ? ` message "${context.messageId}"` : "";
  const record = (typeof error === "object" && error !== null ? error : {}) as {
    message?: unknown;
    description?: unknown;
    httpCode?: unknown;
    response?: { data?: unknown };
    cause?: { response?: { data?: unknown } };
    context?: { data?: unknown };
  };
  // n8n's message is generic per-status text; the real TheMQ body
  // survives at response.data, cause.response.data, or context.data.
  const bodies = [record.response?.data, record.cause?.response?.data, record.context?.data];
  for (const body of bodies) {
    if (typeof body !== "object" || body === null || !("error" in body)) continue;
    const apiError = (body as { error?: unknown }).error;
    if (typeof apiError !== "object" || apiError === null) continue;
    const { code, message } = apiError as { code?: unknown; message?: unknown };
    if (typeof code === "string" && typeof message === "string") {
      const hint = hintForCode(code, context.operation, message);
      return `TheMQ ${code}: ${message} [${where}${id}]${hint === "" ? "" : ` ${hint}`}`;
    }
  }
  // Error status with an unfamiliar body: n8n copies the text into `description`.
  const httpCode = typeof record.httpCode === "string" ? record.httpCode : "";
  const description = typeof record.description === "string" ? record.description : "";
  if (httpCode !== "" && description !== "") {
    return `TheMQ HTTP ${httpCode}: ${description} [${where}${id}]`;
  }
  // No API body: transport-level failure (DNS, refused, timeout, proxy).
  const message = typeof record.message === "string" ? record.message : "";
  const at = context.baseUrl ? ` at ${context.baseUrl}` : "";
  return `TheMQ is not reachable${at} [${where}${id}]${message === "" ? "" : `: ${message}`} Check that TheMQ is running and the Base URL in your credential is correct.`;
}
