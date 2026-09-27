import type { AppInstance } from "../server.js";
import type { ApiServices } from "../server.js";
import {
  consumeSchema,
  leaseBodySchema,
  publishMessageSchema,
  setTtlSchema,
} from "../schemas/messages.js";
import { consumerParamsSchema, messageParamsSchema, queueParamsSchema } from "../schemas/common.js";
import { parseWith } from "./helpers.js";

export function registerMessageRoutes(app: AppInstance, services: ApiServices): void {
  const { broker } = services;

  app.post("/queues/:queue/messages", async (request, reply) => {
    const params = parseWith(queueParamsSchema, request.params, "path parameters");
    const input = parseWith(publishMessageSchema, request.body, "request body");
    const published = await broker.publish(params.queue, input.data, {
      id: input.id,
      ...(input.ttlMs !== undefined ? { ttlMs: input.ttlMs } : {}),
      ...(input.upsert === true ? { upsert: true } : {}),
    });
    // 201 for a new message, 200 when an existing id was updated.
    return reply.status(published.upserted ? 200 : 201).send({ ...published, deliveryCount: 0 });
  });

  app.post("/queues/:queue/consume", async (request) => {
    const params = parseWith(queueParamsSchema, request.params, "path parameters");
    const input = parseWith(consumeSchema, request.body ?? {}, "request body");
    return broker.consume(params.queue, {
      ...(input.consumerId !== undefined ? { consumerId: input.consumerId } : {}),
      ...(input.count !== undefined ? { count: input.count } : {}),
      ...(input.visibilityTimeoutMs !== undefined
        ? { visibilityTimeoutMs: input.visibilityTimeoutMs }
        : {}),
      ...(input.prefetch !== undefined ? { prefetch: input.prefetch } : {}),
    });
  });

  app.get("/queues/:queue/messages/:id", async (request) => {
    const params = parseWith(messageParamsSchema, request.params, "path parameters");
    return broker.getMessage(params.queue, params.id);
  });

  app.post("/queues/:queue/messages/:id/ack", async (request) => {
    const params = parseWith(messageParamsSchema, request.params, "path parameters");
    const body = parseWith(leaseBodySchema, request.body ?? {}, "request body");
    const { deliveries } = await broker.ack(params.queue, params.id, body.consumerId);
    return { id: params.id, queue: params.queue, acked: true, deliveries };
  });

  app.post("/queues/:queue/messages/:id/requeue", async (request) => {
    const params = parseWith(messageParamsSchema, request.params, "path parameters");
    const body = parseWith(leaseBodySchema, request.body ?? {}, "request body");
    const { deliveries } = await broker.requeue(params.queue, params.id, body.consumerId);
    return { id: params.id, queue: params.queue, requeued: true, state: "ready", deliveries };
  });

  app.delete("/queues/:queue/messages/:id", async (request, reply) => {
    const params = parseWith(messageParamsSchema, request.params, "path parameters");
    await broker.deleteMessage(params.queue, params.id);
    return reply.status(204).send();
  });

  app.put("/queues/:queue/messages/:id/ttl", async (request) => {
    const params = parseWith(messageParamsSchema, request.params, "path parameters");
    const input = parseWith(setTtlSchema, request.body, "request body");
    const { state, availableAt } = await broker.setMessageTtl(params.queue, params.id, input.ttl);
    return { id: params.id, queue: params.queue, state, availableAt };
  });

  app.post("/queues/:queue/consumers/:consumerId/cancel", async (request) => {
    const params = parseWith(consumerParamsSchema, request.params, "path parameters");
    const { requeued } = await broker.cancelConsumer(params.queue, params.consumerId);
    return { queue: params.queue, consumerId: params.consumerId, cancelled: true, requeued };
  });
}
