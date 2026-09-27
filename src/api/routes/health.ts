import type { AppInstance } from "../server.js";
import { ApiError } from "../errors.js";
import type { ApiServices } from "../server.js";

export function registerHealthRoutes(app: AppInstance, services: ApiServices): void {
  const { healthService } = services;

  app.get("/health/live", () => {
    return healthService.liveness();
  });

  app.get("/health/ready", async (_request, reply) => {
    try {
      return await healthService.readiness();
    } catch (err) {
      if (err instanceof ApiError) {
        return reply.status(err.statusCode).send(err.toBody());
      }
      throw err;
    }
  });
}
