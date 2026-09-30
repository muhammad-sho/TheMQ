import type { TheMQErrorCode } from "../broker/types.js";

const STATUS_BY_CODE: Record<TheMQErrorCode, number> = {
  VALIDATION_ERROR: 400,
  UNAUTHENTICATED: 401,
  NOT_FOUND: 404,
  CONFLICT: 409,
  SERVICE_UNAVAILABLE: 503,
  INTERNAL_ERROR: 500,
};

export interface ApiErrorResource {
  type: string;
  id?: string;
  queue?: string;
}

export interface ApiErrorBody {
  error: {
    code: TheMQErrorCode;
    message: string;
    resource?: ApiErrorResource;
    details?: unknown;
  };
}

/**
 * Stable API error. Services throw these; the Fastify handler translates
 * them to HTTP.
 */
export class ApiError extends Error {
  readonly code: TheMQErrorCode;
  readonly statusCode: number;
  readonly resource: ApiErrorResource | undefined;
  readonly details: unknown;

  constructor(
    code: TheMQErrorCode,
    message: string,
    options: {
      statusCode?: number;
      resource?: ApiErrorResource | undefined;
      details?: unknown;
      cause?: unknown;
    } = {},
  ) {
    super(message, options.cause !== undefined ? { cause: options.cause } : {});
    this.name = "ApiError";
    this.code = code;
    this.statusCode = options.statusCode ?? STATUS_BY_CODE[code];
    this.resource = options.resource;
    this.details = options.details;
  }

  toBody(): ApiErrorBody {
    return {
      error: {
        code: this.code,
        message: this.message,
        ...(this.resource !== undefined ? { resource: this.resource } : {}),
        ...(this.details !== undefined ? { details: this.details } : {}),
      },
    };
  }

  static notFound(type: string, id: string, queue?: string): ApiError {
    const resource: ApiErrorResource = queue !== undefined ? { type, id, queue } : { type, id };
    return new ApiError("NOT_FOUND", `${type} '${id}' not found.`, {
      resource,
    });
  }

  static validation(message: string, details?: unknown): ApiError {
    return new ApiError("VALIDATION_ERROR", message, { details });
  }

  static serviceUnavailable(message: string, cause?: unknown): ApiError {
    return new ApiError("SERVICE_UNAVAILABLE", message, { cause });
  }

  static internal(message: string, cause?: unknown): ApiError {
    return new ApiError("INTERNAL_ERROR", message, { cause });
  }
}

/** Error names that mean the backend could not be reached. */
const CONNECTION_ERROR_NAMES = new Set([
  "MaxRetriesPerRequestError",
  "ConnectionClosedError",
  "ClusterAllFailedError",
  "ConnectionNotReadyError",
]);

/** Node.js syscall codes that mean the backend could not be reached. */
const CONNECTION_ERROR_CODES = new Set([
  "ECONNREFUSED",
  "ENOTFOUND",
  "EAI_AGAIN",
  "ETIMEDOUT",
  "EPIPE",
  "ECONNRESET",
  "ENETUNREACH",
  "EHOSTUNREACH",
]);

/** Backend message fragments that mean the backend is down. */
const CONNECTION_MESSAGE_PATTERNS = [
  /connection is closed/i,
  /connection lost/i,
  /stream isn't writeable/i,
  /offline queue/i,
  /max retries per request/i,
];

/** True when the error means Redis could not be reached. */
export function isConnectionError(err: unknown): boolean {
  if (err instanceof ApiError) return err.code === "SERVICE_UNAVAILABLE";
  if (typeof err !== "object" || err === null) return false;
  const record = err as { name?: unknown; code?: unknown; message?: unknown };
  if (typeof record.name === "string" && CONNECTION_ERROR_NAMES.has(record.name)) return true;
  if (typeof record.code === "string" && CONNECTION_ERROR_CODES.has(record.code)) return true;
  const message = record.message;
  if (typeof message === "string") {
    return CONNECTION_MESSAGE_PATTERNS.some((pattern) => pattern.test(message));
  }
  return false;
}

/**
 * Map a backend failure to a stable API error: unreachable -> 503,
 * anything else -> 500. The original stays as `cause` for logs.
 */
export function classifyBackendError(err: unknown, operation: string): ApiError {
  if (err instanceof ApiError) return err;
  if (isConnectionError(err)) {
    return ApiError.serviceUnavailable(`Backend temporarily unavailable during ${operation}.`, err);
  }
  return ApiError.internal(`Unexpected backend failure during ${operation}.`, err);
}
