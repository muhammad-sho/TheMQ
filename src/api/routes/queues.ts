import type { AppInstance } from "../server.js";
import { parseWith } from "./helpers.js";
import { queueParamsSchema } from "../schemas/common.js";
import type { ApiServices } from "../server.js";

export function registerQueueRoutes(app: AppInstance, services: ApiServices): void {
  const { broker } = services;

  app.put("/queues/:queue", async (request) => {
    const params = parseWith(queueParamsSchema, request.params, "path parameters");
    return broker.declareQueue(params.queue);
  });

  app.get("/queues", async () => {
    const queues = await broker.listQueues();
    return { queues };
  });

  app.get("/queues/:queue", async (request) => {
    const params = parseWith(queueParamsSchema, request.params, "path parameters");
    return broker.getQueue(params.queue);
  });

  app.delete("/queues/:queue", async (request, reply) => {
    const params = parseWith(queueParamsSchema, request.params, "path parameters");
    await broker.deleteQueue(params.queue);
    return reply.status(204).send();
  });
}
