/** Everything the API error formatter needs to identify the failed call. */
export interface RequestContext {
  itemIndex: number;
  operation: string;
  queue?: string;
  messageId?: string;
  baseUrl?: string;
}

export function encode(value: string): string {
  return encodeURIComponent(value);
}

/** Strip trailing slashes so `baseUrl + path` never doubles them. */
export function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.replace(/\/+$/, "");
}

export function queuePath(queue: string): string {
  return `/queues/${encode(queue)}`;
}

export function messagesPath(queue: string): string {
  return `${queuePath(queue)}/messages`;
}

export function messagePath(queue: string, messageId: string): string {
  return `${messagesPath(queue)}/${encode(messageId)}`;
}

export function messageActionPath(
  queue: string,
  messageId: string,
  action: "ack" | "requeue" | "ttl",
): string {
  return `${messagePath(queue, messageId)}/${action}`;
}
