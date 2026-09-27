import { describe, expect, it, vi } from "vitest";
import type { Logger } from "pino";
import {
  applyRedisServerTuning,
  type TuningClient,
} from "../../src/infrastructure/redis/server-tuning.js";

function fakeClient(): TuningClient & { calls: string[][]; bgsaves: number } {
  const calls: string[][] = [];
  return {
    calls,
    bgsaves: 0,
    config: (...args: string[]) => {
      calls.push(args);
      return Promise.resolve("OK");
    },
    bgsave: function (this: { bgsaves: number }) {
      this.bgsaves++;
      return Promise.resolve("Background saving started");
    },
  };
}

describe("applyRedisServerTuning", () => {
  it("applies durability + memory settings and snapshots once", async () => {
    const client = fakeClient();
    await applyRedisServerTuning(client, { maxmemoryMb: 256 });
    expect(client.calls).toEqual([
      ["SET", "appendonly", "yes"],
      ["SET", "appendfsync", "everysec"],
      ["SET", "save", "60 100"],
      ["SET", "maxmemory", "256mb"],
      ["SET", "maxmemory-policy", "noeviction"],
    ]);
    expect(client.bgsaves).toBe(1);
  });

  it("propagates failures so the caller can warn and continue", async () => {
    const client = fakeClient();
    client.config = vi.fn(() => Promise.reject(new Error("ERR unknown command `config`")));
    await expect(applyRedisServerTuning(client, { maxmemoryMb: 256 })).rejects.toThrow(
      /unknown command/,
    );
  });

  it("waits out a background save in progress, then snapshots", async () => {
    const client = fakeClient();
    let calls = 0;
    const realBgsave = client.bgsave.bind(client);
    client.bgsave = () => {
      calls++;
      if (calls === 1) {
        return Promise.reject(new Error("ERR Background save already in progress"));
      }
      return realBgsave();
    };
    await applyRedisServerTuning(client, { maxmemoryMb: 256 });
    expect(client.bgsaves).toBe(1);
  });

  it("warns and continues when the snapshot fails outright", async () => {
    const client = fakeClient();
    client.bgsave = () => Promise.reject(new Error("ERR permission denied"));
    const warn = vi.fn();
    const logger = { info: vi.fn(), warn } as unknown as Logger;
    await applyRedisServerTuning(client, { maxmemoryMb: 256 }, logger);
    expect(client.calls).toHaveLength(5);
    expect(warn).toHaveBeenCalledOnce();
  });
});
