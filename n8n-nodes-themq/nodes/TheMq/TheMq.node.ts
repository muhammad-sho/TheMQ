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
          "Unique id of the message. When publishing this is the upsert key; when acknowledging or deleting it is taken from the trigger item when connected.",
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
        displayName: "Consumer ID",
        name: "consumerId",
        type: "string",
        default: "={{ $json.consumerId }}",
        displayOptions: { show: { operation: ["ack"] } },
        description:
          "Consumer holding the lease (taken from the trigger item when connected). Only change this to settle another consumer's message.",
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
          const consumerId = strParam(this.getNodeParameter("consumerId", i, "")).trim();
          const body: Record<string, unknown> = {};
          if (consumerId !== "") body["consumerId"] = consumerId;
          const response = toDataObject(
            await request.call(
              this,
              "POST",
              `/queues/${encode(queue)}/messages/${encode(messageId)}/ack`,
              body,
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
        return "The message is not leased to this consumer. It may already be settled or held by another consumer — check the Consumer ID.";
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
