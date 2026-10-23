export interface PublishOptions {
  ttlMs?: number | undefined;
  upsert?: boolean | undefined;
  onConflict?: "skip" | undefined;
}

/** Build the publish body; omitted options stay absent on the wire. */
export function buildPublishBody(
  id: string,
  data: unknown,
  options: PublishOptions = {},
): Record<string, unknown> {
  const body: Record<string, unknown> = { id, data };
  if (options.ttlMs !== undefined) body["ttlMs"] = options.ttlMs;
  if (options.upsert === true) body["upsert"] = true;
  if (options.onConflict === "skip") body["onConflict"] = "skip";
  return body;
}

/** Build a requeue body; replacement data is only sent when explicitly supplied. */
export function buildRequeueBody(
  data: unknown,
  hasData: boolean,
  ttlMs: number | undefined,
): Record<string, unknown> {
  const body: Record<string, unknown> = {};
  if (hasData) body["data"] = data;
  if (ttlMs !== undefined) body["ttlMs"] = ttlMs;
  return body;
}
