import { timingSafeEqual } from "node:crypto";
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import fastify, { type FastifyInstance, type FastifyTypeProviderDefault } from "fastify";
import helmet from "@fastify/helmet";
import rateLimit from "@fastify/rate-limit";
import websocket from "@fastify/websocket";
import { ApiError } from "./errors.js";
import type { BrokerService } from "../broker/broker.js";
import type { SubscriptionManager } from "../broker/subscriptions.js";
import type { AppConfig } from "../config/schema.js";
import type { Logger } from "../infrastructure/logging/logger.js";
import type { HealthService } from "../health/health-service.js";
import { registerHealthRoutes } from "./routes/health.js";
import { registerMessageRoutes } from "./routes/messages.js";
import { registerQueueRoutes } from "./routes/queues.js";
import { registerSubscribeRoutes } from "./routes/subscribe.js";

export interface ApiServices {
  config: AppConfig;
  logger: Logger;
  broker: BrokerService;
  healthService: HealthService;
  subscriptions: SubscriptionManager;
}

/** Fastify instance type bound to the pino logger TheMQ uses. */
export type AppInstance = FastifyInstance<
  Server<typeof IncomingMessage, typeof ServerResponse>,
  IncomingMessage,
  ServerResponse<IncomingMessage>,
  Logger,
  FastifyTypeProviderDefault
>;

/** Health probes stay unauthenticated so orchestrators can check them. */
const PUBLIC_PATHS = new Set(["/health/live", "/health/ready"]);

/** Rolling window for the per-client request ceiling (see RATE_LIMIT_MAX_PER_MINUTE). */
const RATE_LIMIT_WINDOW = "1 minute";

function isAuthorized(
  request: { url: string; headers: Record<string, string | string[] | undefined> },
  config: AppConfig,
): boolean {
  const path = request.url.split("?", 1)[0] ?? request.url;
  if (PUBLIC_PATHS.has(path)) return true;
  if (config.authDisabled) return true;
  const header = request.headers["authorization"];
  if (typeof header !== "string" || config.apiToken === undefined) return false;
  const [scheme, token] = header.split(" ", 2);
  if (scheme?.toLowerCase() !== "bearer" || !token) return false;
  const expected = Buffer.from(`Bearer ${config.apiToken}`);
  const actual = Buffer.from(header);
  return expected.length === actual.length && timingSafeEqual(expected, actual);
}

export async function buildApp(services: ApiServices): Promise<AppInstance> {
  const { config, logger } = services;
  // Keep-alive sockets cannot outlive a shutdown request; the body cap
  // follows MAX_MESSAGE_BYTES so the setting holds end to end.
  const app = fastify({
    loggerInstance: logger,
    forceCloseConnections: true,
    bodyLimit: config.maxMessageBytes,
    // No per-request access logs: at thousands of rps they cost ~35%
    // throughput and flood disks. Errors and lifecycle events still log.
    disableRequestLogging: true,
  });

  await app.register(helmet);
  await app.register(rateLimit, {
    max: config.rateLimitMaxPerMinute,
    timeWindow: RATE_LIMIT_WINDOW,
  });
  await app.register(websocket);

  app.addHook("onRequest", async (request, reply) => {
    if (!isAuthorized(request, config)) {
      return reply
        .status(401)
        .send(new ApiError("UNAUTHENTICATED", "Missing or invalid Bearer token.").toBody());
    }
  });

  app.setErrorHandler((err, request, reply) => {
    if (err instanceof ApiError) {
      if (err.statusCode >= 500) {
        request.log.error({ err, code: err.code }, "Request failed");
      }
      return reply.status(err.statusCode).send(err.toBody());
    }
    if (typeof err === "object" && err !== null && "validation" in err) {
      return reply.status(400).send(
        ApiError.validation("Invalid request.", {
          issues: (err as { validation?: unknown }).validation,
        }).toBody(),
      );
    }
    request.log.error({ err }, "Unhandled request error");
    const statusCode =
      typeof (err as { statusCode?: unknown }).statusCode === "number"
        ? (err as { statusCode: number }).statusCode
        : 500;
    const fastifyCode =
      typeof (err as { code?: unknown }).code === "string" ? (err as { code: string }).code : "";
    // Rate limiting signals the client to slow down, not a server failure.
    if (fastifyCode === "FST_ERR_RATE_LIMIT" || statusCode === 429) {
      return reply.status(429).send(
        new ApiError("VALIDATION_ERROR", "Rate limit exceeded. Slow down and retry.", {
          details: { retryable: true },
        }).toBody(),
      );
    }
    const code = fastifyCode.startsWith("FST_ERR_CTP_")
      ? "VALIDATION_ERROR"
      : statusCode === 503
        ? "SERVICE_UNAVAILABLE"
        : "INTERNAL_ERROR";
    return reply
      .status(statusCode >= 400 && statusCode < 600 ? statusCode : 500)
      .send(
        new ApiError(
          code,
          code === "VALIDATION_ERROR"
            ? "Invalid request."
            : statusCode >= 500
              ? "Internal server error."
              : "Request failed.",
        ).toBody(),
      );
  });

  app.setNotFoundHandler((request, reply) => {
    return reply.status(404).send(
      new ApiError("NOT_FOUND", `Route '${request.url}' not found.`, {
        resource: { type: "route", id: request.url },
      }).toBody(),
    );
  });

  registerHealthRoutes(app, services);
  registerQueueRoutes(app, services);
  registerMessageRoutes(app, services);
  registerSubscribeRoutes(app, services);

  return app;
}
