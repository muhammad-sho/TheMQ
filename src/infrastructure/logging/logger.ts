import pino, { type DestinationStream, type Logger } from "pino";
import type { LogLevel } from "../../config/schema.js";

export interface CreateLoggerOptions {
  level?: LogLevel;
  pretty?: boolean;
  /** Optional pino destination (defaults to stdout). Useful for capturing logs in tests. */
  destination?: DestinationStream;
}

/**
 * Sensitive paths that must never appear in logs in clear text.
 * Covers API tokens, auth headers, cookies and generic credential fields.
 */
const REDACT_PATHS = [
  "apiToken",
  "token",
  "*.apiToken",
  "*.token",
  "req.headers.authorization",
  "req.headers.cookie",
  "res.headers.authorization",
  "headers.authorization",
  "headers.cookie",
  "authorization",
  "cookie",
  "body.password",
  "body.token",
  "headers.*authorization*",
];

/** Create the root structured JSON logger. */
export function createLogger(options: CreateLoggerOptions = {}): Logger {
  const { level = "info", pretty = false, destination } = options;
  return pino(
    {
      level,
      base: { service: "themq" },
      timestamp: pino.stdTimeFunctions.isoTime,
      redact: {
        paths: REDACT_PATHS,
        censor: "[Redacted]",
      },
      ...(pretty
        ? {
            transport: {
              target: "pino-pretty",
              options: { colorize: true, singleLine: true },
            },
          }
        : {}),
    },
    destination,
  );
}

export type { Logger };
