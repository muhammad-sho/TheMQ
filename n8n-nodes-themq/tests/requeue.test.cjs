"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const { TheMq } = require("../dist/nodes/TheMq/TheMq.node.js");

function nodeContext(options, behavior = {}) {
  const calls = [];
  let requeueAttempts = 0;
  let ttlAttempts = 0;
  const context = {
    calls,
    getInputData: () => [{ json: { data: { v: 1 } } }],
    getNodeParameter: (name) => {
      if (name === "operation") return "requeue";
      if (name === "queue") return "q";
      if (name === "messageId") return "msg_test";
      if (name === "options") return options;
      throw new Error(`Unexpected parameter: ${name}`);
    },
    getNode: () => ({
      name: "TheMQ",
      type: "theMq",
      typeVersion: 1,
      position: [0, 0],
      parameters: {},
    }),
    getCredentials: async () => ({ baseUrl: "http://127.0.0.1:1" }),
    continueOnFail: () => false,
    sendResponse: () => {},
    helpers: {
      requestWithAuthentication: {
        call: async (_thisArg, _credential, request) => {
          calls.push(request);
          if (request.method === "POST" && request.url.endsWith("/requeue")) {
            requeueAttempts += 1;
            if (!behavior.requeueSucceedsOnRetry || requeueAttempts === 1) {
              throw new Error(
                "TheMQ CONFLICT: Cannot requeue message 'msg_test' while it is delayed.",
              );
            }
            return {
              id: "msg_test",
              queue: "q",
              requeued: true,
              state: "ready",
              availableAt: 456,
              deliveries: 0,
            };
          }
          if (request.method === "GET") {
            return {
              id: "msg_test",
              queue: "q",
              data: { v: 1 },
              state: behavior.state ?? "delayed",
              availableAt: 123,
              deliveryCount: 0,
            };
          }
          if (request.method === "PUT" && request.url.endsWith("/ttl")) {
            ttlAttempts += 1;
            if (behavior.ttlConflict && ttlAttempts === 1) {
              throw new Error(
                "TheMQ CONFLICT: Cannot change message 'msg_test' while it is unacked.",
              );
            }
            return {
              id: "msg_test",
              queue: "q",
              state: "ready",
              availableAt: 456,
            };
          }
          throw new Error(`Unexpected request: ${request.method} ${request.url}`);
        },
      },
    },
  };
  return context;
}

test("requeue with ttlMs 0 promotes a delayed message via the TTL endpoint", async () => {
  const context = nodeContext({ ttlMs: 0 });
  const result = await new TheMq().execute.call(context);

  assert.deepEqual(result, [
    [
      {
        json: {
          queue: "q",
          messageId: "msg_test",
          requeued: false,
          alreadyQueued: true,
          id: "msg_test",
          state: "ready",
          availableAt: 456,
          deliveryCount: 0,
          data: { v: 1 },
        },
      },
    ],
  ]);

  assert.deepEqual(
    context.calls.map((call) => [call.method, call.url, call.body]),
    [
      ["POST", "/queues/q/messages/msg_test/requeue", { ttlMs: 0 }],
      ["GET", "/queues/q/messages/msg_test", undefined],
      ["PUT", "/queues/q/messages/msg_test/ttl", { ttl: 0 }],
    ],
  );
});

test("requeue with ttlMs 0 retries when a consumer leases during the TTL update", async () => {
  const context = nodeContext({ ttlMs: 0 }, { ttlConflict: true, requeueSucceedsOnRetry: true });
  const result = await new TheMq().execute.call(context);

  assert.deepEqual(result, [
    [
      {
        json: {
          id: "msg_test",
          queue: "q",
          requeued: true,
          state: "ready",
          availableAt: 456,
          deliveries: 0,
        },
      },
    ],
  ]);

  assert.deepEqual(
    context.calls.map((call) => [call.method, call.url, call.body]),
    [
      ["POST", "/queues/q/messages/msg_test/requeue", { ttlMs: 0 }],
      ["GET", "/queues/q/messages/msg_test", undefined],
      ["PUT", "/queues/q/messages/msg_test/ttl", { ttl: 0 }],
      ["POST", "/queues/q/messages/msg_test/requeue", { ttlMs: 0 }],
    ],
  );
});

test("requeue with ttlMs 0 leaves an already-ready message in place", async () => {
  const context = nodeContext({ ttlMs: 0 }, { state: "ready" });
  const result = await new TheMq().execute.call(context);

  assert.deepEqual(result, [
    [
      {
        json: {
          queue: "q",
          messageId: "msg_test",
          requeued: false,
          alreadyQueued: true,
          state: "ready",
          availableAt: 123,
          deliveryCount: 0,
          data: { v: 1 },
        },
      },
    ],
  ]);

  assert.deepEqual(
    context.calls.map((call) => [call.method, call.url]),
    [
      ["POST", "/queues/q/messages/msg_test/requeue"],
      ["GET", "/queues/q/messages/msg_test"],
    ],
  );
});

test("requeue without ttlMs leaves an already-waiting message untouched", async () => {
  const context = nodeContext({});
  const result = await new TheMq().execute.call(context);

  assert.deepEqual(result, [
    [
      {
        json: {
          queue: "q",
          messageId: "msg_test",
          requeued: false,
          alreadyQueued: true,
          state: "delayed",
          availableAt: 123,
          deliveryCount: 0,
          data: { v: 1 },
        },
      },
    ],
  ]);

  assert.deepEqual(
    context.calls.map((call) => [call.method, call.url]),
    [
      ["POST", "/queues/q/messages/msg_test/requeue"],
      ["GET", "/queues/q/messages/msg_test"],
    ],
  );
});
