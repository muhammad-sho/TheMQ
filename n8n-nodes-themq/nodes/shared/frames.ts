import type { IDataObject } from "n8n-workflow";
import type WebSocket from "ws";

export interface DeliveredMessage {
  id: string;
  data: unknown;
  deliveryCount: number;
  redelivered: boolean;
}

/** Narrow an unknown frame field to a string (never stringifies objects). */
export function frameString(value: unknown): string {
  return typeof value === "string" ? value : "";
}

export function frameText(raw: WebSocket.RawData): string {
  if (typeof raw === "string") return raw;
  if (Array.isArray(raw)) return Buffer.concat(raw).toString("utf8");
  if (raw instanceof ArrayBuffer) return Buffer.from(raw).toString("utf8");
  return raw.toString("utf8");
}

/** Parse a socket frame; malformed JSON yields undefined. */
export function parseFrame(raw: WebSocket.RawData): IDataObject | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(frameText(raw)) as unknown;
  } catch {
    return undefined;
  }
  if (typeof parsed !== "object" || parsed === null) return undefined;
  return parsed as IDataObject;
}

/** Extract a delivered message from a parsed frame (undefined = not a message). */
export function parseDeliveredMessage(frame: IDataObject): DeliveredMessage | undefined {
  if (frame["type"] !== "message" || typeof frame["id"] !== "string") return undefined;
  return {
    id: frame["id"],
    data: frame["data"],
    deliveryCount: typeof frame["deliveryCount"] === "number" ? frame["deliveryCount"] : 1,
    redelivered: frame["redelivered"] === true,
  };
}
