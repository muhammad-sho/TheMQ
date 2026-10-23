import type {
  IDataObject,
  IExecuteFunctions,
  IHttpRequestMethods,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
} from "n8n-workflow";
import { NodeConnectionTypes, NodeOperationError } from "n8n-workflow";
import { apiCodeOf, describeApiError, errorMessage } from "../shared/errors.js";
import {
  messageActionPath,
  messagePath,
  messagesPath,
  normalizeBaseUrl,
  type RequestContext,
} from "../shared/http.js";
import { buildPublishBody, buildRequeueBody, type PublishOptions } from "../shared/messages.js";
import { boolParam, numParam, strParam, toDataObject } from "../shared/params.js";

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
          const data = parseMessageData(this, this.getNodeParameter("messageData", i), i);
          const ttlMs = numParam(this.getNodeParameter("ttlMs", i, 0), 0);
          const upsert = boolParam(this.getNodeParameter("upsert", i, false));
          const onConflict = strParam(this.getNodeParameter("onConflict", i, "error"), "error");
          const response = await publishMessage(
            this,
            queue,
            messageId,
            data,
            {
              ttlMs: ttlMs > 0 ? ttlMs : undefined,
              upsert,
              onConflict: onConflict === "skip" ? "skip" : undefined,
            },
            { itemIndex: i, operation, queue, messageId },
          );
          returnData.push({ json: toDataObject(response) });
        } else if (operation === "ack") {
          const response = toDataObject(
            await request.call(
              this,
              "POST",
              messageActionPath(queue, messageId, "ack"),
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
          const hasReplacement = options.messageData !== undefined;
          const replacement = hasReplacement
            ? parseMessageData(this, options.messageData, i)
            : undefined;
          const delayMs =
            typeof options.ttlMs === "number" && options.ttlMs > 0
              ? Math.floor(options.ttlMs)
              : undefined;
          const requeueBody = buildRequeueBody(replacement, hasReplacement, delayMs);
          const requestContext = { itemIndex: i, operation, queue, messageId };
          try {
            const response = await requeueMessage(
              this,
              queue,
              messageId,
              requeueBody,
              requestContext,
            );
            returnData.push({ json: toDataObject(response) });
          } catch (requeueError) {
            // Forgiving requeue: the fast path above covers the normal
            // leased case in one call. Anything else (already waiting,
            // never published) is resolved into "queued" instead of
            // surfacing a state error.
            const ensured = await ensureQueued(this, {
              itemIndex: i,
              queue,
              messageId,
              replacement,
              hasReplacement,
              delayMs,
              itemJson: toDataObject(items[i]?.json ?? {}),
              requeueError,
            });
            returnData.push({ json: ensured });
          }
        } else if (operation === "delete") {
          await request.call(this, "DELETE", messagePath(queue, messageId), undefined, {
            itemIndex: i,
            operation,
            queue,
            messageId,
          });
          returnData.push({ json: { queue, messageId, deleted: true } });
        } else {
          throw new NodeOperationError(this.getNode(), `Unknown operation "${operation}".`, {
            itemIndex: i,
          });
        }
      } catch (error) {
        if (this.continueOnFail()) {
          returnData.push({
            json: { error: errorMessage(error) },
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

function parseMessageData(context: IExecuteFunctions, value: unknown, itemIndex: number): unknown {
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value) as unknown;
  } catch {
    throw new NodeOperationError(context.getNode(), "Message Data is not valid JSON.", {
      itemIndex,
    });
  }
}

async function publishMessage(
  context: IExecuteFunctions,
  queue: string,
  messageId: string,
  data: unknown,
  options: PublishOptions,
  requestContext: RequestContext,
): Promise<IDataObject> {
  return request.call(
    context,
    "POST",
    messagesPath(queue),
    buildPublishBody(messageId, data, options),
    { ...requestContext },
  );
}

async function requeueMessage(
  context: IExecuteFunctions,
  queue: string,
  messageId: string,
  body: Record<string, unknown>,
  requestContext: RequestContext,
): Promise<IDataObject> {
  return request.call(context, "POST", messageActionPath(queue, messageId, "requeue"), body, {
    ...requestContext,
  });
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
      await request.call(context, "GET", messagePath(queue, messageId), undefined, {
        ...requestContext,
      }),
    );
  } catch (error) {
    if (apiCodeOf(error) === "NOT_FOUND") return undefined;
    throw error;
  }
}

interface EnsureQueuedInput {
  itemIndex: number;
  queue: string;
  messageId: string;
  replacement: unknown;
  hasReplacement: boolean;
  delayMs: number | undefined;
  itemJson: IDataObject;
  requeueError: unknown;
}

/**
 * Resolve a failed requeue into "queued". Only NOT_FOUND/CONFLICT enter
 * here; anything else (auth, transport, validation) rethrows at once.
 * At most one state-changing retry follows the GET, so races settle
 * instead of looping.
 */
async function ensureQueued(
  context: IExecuteFunctions,
  input: EnsureQueuedInput,
): Promise<IDataObject> {
  const { itemIndex, queue, messageId, replacement, hasReplacement, delayMs, itemJson } = input;
  const code = apiCodeOf(input.requeueError);
  if (code !== "NOT_FOUND" && code !== "CONFLICT") throw input.requeueError;
  const requestContext: RequestContext = { itemIndex, operation: "requeue", queue, messageId };
  const requeueBody = buildRequeueBody(replacement, hasReplacement, delayMs);
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
        { itemIndex },
      );
    }
    try {
      const published = await publishMessage(
        context,
        queue,
        messageId,
        payload,
        {
          ttlMs: delayMs,
          // Tolerate a race that created the message after the GET.
          upsert: true,
        },
        requestContext,
      );
      return { ...toDataObject(published), queued: true };
    } catch (publishError) {
      if (apiCodeOf(publishError) === "CONFLICT") {
        // Lost the race: the message is leased now, so the requeue
        // that just failed will succeed on the new terms.
        return toDataObject(
          await requeueMessage(context, queue, messageId, requeueBody, requestContext),
        );
      }
      throw publishError;
    }
  }
  if (current["state"] === "unacked") {
    // Changed under us between the requeue and the GET: one retry on
    // the current terms instead of reporting a stale state.
    return toDataObject(
      await requeueMessage(context, queue, messageId, requeueBody, requestContext),
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
      const published = await publishMessage(
        context,
        queue,
        messageId,
        replacement,
        { ttlMs: delayMs, upsert: true },
        requestContext,
      );
      return { ...toDataObject(published), alreadyQueued: true };
    } catch (publishError) {
      if (apiCodeOf(publishError) === "CONFLICT") {
        // Leased between the GET and the publish: requeue on the new
        // terms once instead of failing.
        return toDataObject(
          await requeueMessage(context, queue, messageId, requeueBody, requestContext),
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
    messageActionPath(queue, messageId, "ttl"),
    { ttl: delayMs },
    { ...requestContext },
  );
  return { queue, messageId, requeued: false, alreadyQueued: true, ...toDataObject(moved) };
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
