export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** TheMQ error code from a formatted node error (undefined = transport/unknown). */
export function apiCodeOf(error: unknown): string | undefined {
  const message = errorMessage(error);
  const named = /TheMQ ([A-Z_]+):/.exec(message)?.[1];
  if (named !== undefined && named !== "HTTP") return named;
  // n8n versions differ in how much of the API body survives on the
  // error object; when only the status text remains ("TheMQ HTTP 404:
  // ..."), map the status back to its stable code.
  const status = /TheMQ HTTP (\d+)/.exec(message)?.[1];
  if (status === "404") return "NOT_FOUND";
  if (status === "409") return "CONFLICT";
  return undefined;
}

/** Permanent connection failures (bad credentials, gone queue): surface, don't loop. */
export function isFatalConnectionError(error: unknown): boolean {
  return /401|unauthorized|NOT_FOUND/i.test(errorMessage(error));
}

/** A message the broker failed to settle because it is already gone. */
export function isAlreadySettled(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  const message = (error as { message?: unknown }).message;
  return (
    typeof message === "string" && (message.includes("NOT_FOUND") || message.includes("CONFLICT"))
  );
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
