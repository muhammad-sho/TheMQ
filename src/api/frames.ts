import type * as WebSocket from "ws";

/** Decode a WebSocket frame payload to text (shared by the route and its tests). */
export function frameToString(raw: WebSocket.RawData): string {
  if (typeof raw === "string") return raw;
  if (Array.isArray(raw)) return Buffer.concat(raw).toString("utf8");
  if (raw instanceof ArrayBuffer) return Buffer.from(raw).toString("utf8");
  return raw.toString("utf8");
}
