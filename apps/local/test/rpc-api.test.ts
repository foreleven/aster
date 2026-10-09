import { Actor, ActorSystem } from "@aster/actor";
import { AgentRunner } from "@aster/agent/agent";
import { AgentConversations, DurableHarness } from "@aster/agent/harness";
import * as ApiClient from "@aster/api/client";
import * as ApiServer from "@aster/api/server";
import {
  ApplicationError,
  ApprovalCommand,
  ContextQueries,
  ContextRegistry,
  contextView,
  defineContext,
  ExternalAgents,
  GoalActor,
  GoalSettings,
  GoalsRootActor,
  GoalsRootCommand,
  goalTimeline,
  MemoryRecall,
} from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import { NodeHttpServer, NodeSocket } from "@effect/platform-node";
import { Deferred, Effect, Exit, Layer, Queue, Schema, Scope, Stream } from "effect";
import { FetchHttpClient, HttpRouter } from "effect/http";
import { NetAddress } from "effect/net";
import { RpcClient, RpcSerialization, RpcServer } from "effect/rpc";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { test } from "node:test";
import { apiServices, rpcRequest, startTestHttp } from "./api-fixtures.js";
import { testConversations } from "./conversation-fixtures.js";

const definition = defineContext({
  view: contextView({ state: Schema.Struct({ value: Schema.Number }) }),
  state: Schema.Struct({ value: Schema.Number }),
  message: Schema.Unknown,
});
const record = {
  path: "/goals/test",
  description: "API test Goal",
  state: { value: 1 },
  messages: [],
};

test("Goal RPC acknowledges duplicate business requests without duplicating input", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const conversations = testConversations();
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            ContextQueries.layer,
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(GoalSettings, {
              definitions: [{ slug: "personal", description: "Assistant" }],
              reasoning: { model: "test" },
            }),
            Layer.succeed(AgentConversations, conversations),

            Layer.succeed(MemoryRecall, {
              search: () => Effect.succeed({ results: [] }),
              expand: () => Effect.succeed({ results: [] }),
            }),
            Layer.succeed(ExternalAgents, {}),
            Layer.succeed(
              DurableHarness,
              DurableHarness.make(() => Effect.never),
            ),
            Layer.succeed(
              AgentRunner,
              AgentRunner.make(() => Effect.die("No model expected")),
            ),
          ),
        );
        const goals = yield* system.spawn("goals", GoalsRootActor);
        yield* goals.awaitStarted;
        const api = yield* Effect.acquireRelease(
          Effect.promise(() => startTestHttp({ registry, conversations, actors: system })),
          (api) => Effect.promise(() => api.close()),
        );
        const call = (id: number, tag: string, payload: unknown) =>
          Effect.promise(async () => {
            const response = await fetch(`${api.url}/api/rpc`, {
              method: "POST",
              headers: { "content-type": "application/ndjson" },
              body: JSON.stringify({ _tag: "Request", id, tag, payload, headers: [] }) + "\n",
            });
            return JSON.parse(await response.text());
          });
        const before = yield* call(1, "GetContext", { path: "/goals/personal" });
        assert.equal(before.exit._tag, "Success");
        assert.equal(before.exit.value.path, "/goals/personal");
        const input = {
          requestId: "stable-business-request",
          slug: "personal",
          text: "Watch the release",
        };
        const first = yield* call(2, "SendGoalMessage", input);
        const duplicate = yield* call(3, "SendGoalMessage", input);
        assert.equal(first.exit._tag, "Success");
        assert.equal(first.exit.value, null);
        assert.deepEqual(duplicate.exit, first.exit);
        assert.equal(
          (yield* goalTimeline(registry, conversations, "personal", {})).messages.length,
          1,
        );
      }),
    ),
  );
});

test("ListContexts encodes cleared optional fields in public state and nested messages", async () => {
  const registry = await Effect.runPromise(makeContextRegistry());
  const path = "/goals/cleared";
  await Effect.runPromise(
    registry.register(
      path,
      defineContext({
        view: contextView({
          state: Schema.Struct({
            lastError: Schema.optional(Schema.String),
            nested: Schema.Struct({ active: Schema.Boolean }),
          }),
          message: Schema.Struct({
            role: Schema.String,
            content: Schema.Array(
              Schema.Struct({ type: Schema.Literal("text"), text: Schema.String }),
            ),
          }),
        }),
        state: Schema.Struct({ lastError: Schema.optional(Schema.String), nested: Schema.Unknown }),
        message: Schema.Unknown,
      }),
    ),
  );
  await Effect.runPromise(
    registry.commit(
      {
        path,
        description: "Cleared error",
        state: { lastError: undefined, nested: { goal: undefined, active: false } },
        messages: [
          { role: "assistant", content: [{ type: "text", text: "done", signature: undefined }] },
        ],
      },
      { expectedRevision: registry.get(path)?.revision ?? 0 },
    ),
  );
  const api = await startTestHttp({
    port: 0,
    registry,
  });
  try {
    const response = await fetch(`${api.url}/api/rpc`, {
      method: "POST",
      headers: { "content-type": "application/ndjson" },
      body:
        JSON.stringify({
          _tag: "Request",
          id: 0,
          tag: "ListContexts",
          payload: null,
          headers: [],
        }) + "\n",
    });
    const reply = JSON.parse(await response.text());
    assert.equal(reply.exit._tag, "Success", JSON.stringify(reply));
    assert.deepEqual(reply.exit.value[0], {
      path,
      revision: 1,
      description: "Cleared error",
      projection: { visibility: "public" },
      state: { nested: { active: false } },
      messages: [{ role: "assistant", content: [{ type: "text", text: "done" }] }],
    });
  } finally {
    await api.close();
  }
});

test("failed persistence and unchanged writes do not invalidate application queries", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        let fail = false;
        const registry = yield* makeContextRegistry({
          loadAll: () => [],
          save: () => {
            if (fail) throw new Error("Storage unavailable");
          },
        });
        yield* registry.register(record.path, definition);
        const changes = yield* registry.reader.subscribe;
        const received: unknown[] = [];
        yield* Stream.runForEach(changes, (change) =>
          Effect.sync(() => {
            received.push(change);
          }),
        ).pipe(Effect.forkScoped);
        yield* registry.commit(record, {
          expectedRevision: registry.get(record.path)?.revision ?? 0,
        });
        yield* registry.commit(record, {
          expectedRevision: registry.get(record.path)?.revision ?? 0,
        });
        fail = true;
        const failed = yield* Effect.exit(
          registry.commit(
            { ...record, state: { value: 2 } },
            { expectedRevision: registry.get(record.path)?.revision ?? 0 },
          ),
        );
        assert.equal(failed._tag, "Failure");
        yield* Effect.yieldNow;
        assert.equal(received.length, 1);
        assert.deepEqual(registry.get(record.path), { ...record, revision: 1 });
      }),
    ),
  );
});

for (const protocol of ["http", "websocket"] as const) {
  test(
    `${protocol}: shared client queries, Actor replies, and scoped streaming RPC`,
    { timeout: 10000 },
    async () => {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const registry = yield* makeContextRegistry();
            yield* registry.register(record.path, definition);
            yield* registry.commit(record, { expectedRevision: 0 });
            let submissions = 0;
            const Goals = Actor.define("test/RpcGoals", {
              commands: [GoalsRootCommand],
            })(
              Effect.succeed({
                receive: (command) =>
                  Effect.gen(function* () {
                    submissions++;
                    yield* command.command.replyTo.tell({
                      _tag: "Rejected",
                      error: new ApplicationError({
                        kind: "conflict",
                        message: "Rejected input",
                      }),
                    });
                  }),
              }),
            );
            const system = yield* ActorSystem.make();
            yield* (yield* system.spawn("goals", Goals)).awaitStarted;
            let active = 0;
            const released = yield* Deferred.make<void>();
            const services = apiServices({
              actors: system,
              registry: {
                ...registry,
                reader: {
                  ...registry.reader,
                  subscribe: Effect.acquireRelease(
                    Effect.sync(() => {
                      active++;
                    }),
                    () =>
                      Effect.gen(function* () {
                        active--;
                        if (!active) yield* Deferred.succeed(released, undefined);
                      }),
                  ).pipe(Effect.andThen(registry.reader.subscribe)),
                },
              },
            });
            const server = yield* NodeHttpServer.make(createServer, { host: "127.0.0.1", port: 0 });
            const url = NetAddress.formatUrlUnsafe(server.address);
            const handler = yield* HttpRouter.toHttpEffect(
              ApiServer.layer.pipe(
                Layer.provide(
                  protocol === "http"
                    ? RpcServer.layerProtocolHttp({ path: "/rpc", streamBufferSize: 4 })
                    : RpcServer.layerProtocolWebsocket({ path: "/rpc" }),
                ),
                Layer.provide(RpcSerialization.layerNdjson),
                Layer.provide(services),
              ),
            );
            yield* server.serve(handler);
            const clientProtocol =
              protocol === "http"
                ? RpcClient.layerProtocolHttp({ url: `${url}/rpc` }).pipe(
                    Layer.provide(FetchHttpClient.layer),
                  )
                : RpcClient.layerProtocolSocket().pipe(
                    Layer.provide(NodeSocket.layerWebSocket(`${url.replace("http:", "ws:")}/rpc`)),
                  );
            yield* Effect.gen(function* () {
              const client = yield* ApiClient.make;
              assert.equal((yield* client("ListContexts", undefined))[0]!.path, record.path);
              const missing = yield* client("GetContext", { path: "/missing" }).pipe(Effect.flip);
              assert.equal(missing._tag, "ApplicationError");
              const rejected = yield* client("SendGoalMessage", {
                slug: "test",
                text: "Hello",
                requestId: "one",
              }).pipe(Effect.flip);
              assert.equal(rejected._tag, "ApplicationError");
              assert.equal(submissions, 1);
              const scope = yield* Scope.make();
              const subscriptions = yield* Effect.forEach([1, 2], () =>
                client("SubscribeInvalidations", undefined, { asQueue: true }).pipe(
                  Effect.provideService(Scope.Scope, scope),
                ),
              );
              for (const queue of subscriptions)
                assert.deepEqual(yield* Queue.take(queue), {
                  _tag: "Invalidate",
                  keys: ["all-queries"],
                });
              assert.equal(active, 2);
              yield* registry.commit({ ...record, state: { value: 2 } }, { expectedRevision: 1 });
              for (const queue of subscriptions)
                assert.deepEqual(yield* Queue.take(queue), {
                  _tag: "Invalidate",
                  keys: ["contexts", "context:/goals/test", "goals", "goal-history:test"],
                });
              yield* Scope.close(scope, Exit.void);
              yield* Deferred.await(released);
              assert.equal(active, 0);
            }).pipe(
              Effect.provide(clientProtocol.pipe(Layer.provide(RpcSerialization.layerNdjson))),
            );
          }),
        ),
      );
    },
  );
}

test("RPC host rejects cross-origin and oversized input; removed endpoints return 404", async () => {
  const registry = await Effect.runPromise(makeContextRegistry());
  const server = await startTestHttp({ registry });
  try {
    const call = rpcRequest("SendGoalMessage", { slug: "personal", text: "hello" });
    assert.equal(
      (
        await fetch(`${server.url}/api/rpc`, {
          ...call,
          headers: { ...call.headers, origin: "https://other.example" },
        })
      ).status,
      403,
    );
    assert.equal(
      (
        await fetch(
          `${server.url}/api/rpc`,
          rpcRequest("SendGoalMessage", { slug: "personal", text: "x".repeat(33 * 1024) }),
        )
      ).status,
      413,
    );
    const missing = (await (await fetch(`${server.url}/api/rpc`, call)).json()) as {
      exit: { _tag: string };
    };
    assert.equal(missing.exit._tag, "Failure");
    for (const path of ["/api/goals", "/api/events", "/api/dashboard", "/api/approvals"])
      assert.equal((await fetch(`${server.url}${path}`)).status, 404);
  } finally {
    await server.close();
  }
});

test("Goal RPC paginates persisted public conversation entries", async () => {
  const registry = await Effect.runPromise(makeContextRegistry());
  const conversations = testConversations();
  await Effect.runPromise(registry.register("/goals/feed", GoalActor.contextDefinition));
  await Effect.runPromise(
    registry.commit(
      {
        path: "/goals/feed",
        description: "Feed",
        messages: [],
        state: {
          definition: { slug: "feed", description: "Feed" },
          status: "active",
          summary: "",
          inputs: [],
          receipts: [],
          tasks: [],
        },
      },
      { expectedRevision: 0 },
    ),
  );
  for (let i = 0; i < 65; i++)
    await Effect.runPromise(
      conversations.append("/goals/feed", `input-${i}`, "goal.input", {
        payload: { _tag: "UserInput", text: `Record ${i}` },
      }),
    );
  const server = await startTestHttp({ registry, conversations });
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* ApiClient.make;
          const page = yield* client("GetGoalTimeline", { slug: "feed" });
          assert.equal(page.messages.length, 30);
          assert.equal(page.messages[0]!.text, "Record 35");
          const older = yield* client("GetGoalTimeline", {
            slug: "feed",
            before: page.nextBefore!,
          });
          assert.equal(older.messages.at(-1)!.text, "Record 34");
          const invalid = yield* client("GetGoalTimeline", { slug: "feed", limit: 200 }).pipe(
            Effect.flip,
          );
          assert.equal(invalid._tag, "ApplicationError");
        }),
      ).pipe(
        Effect.provide(
          RpcClient.layerProtocolHttp({ url: `${server.url}/api/rpc` }).pipe(
            Layer.provide([FetchHttpClient.layer, RpcSerialization.layerNdjson]),
          ),
        ),
      ),
    );
  } finally {
    await server.close();
  }
});

test("Approval RPC decodes payloads before sending commands and preserves rejection", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        let submissions = 0;
        const Approvals = Actor.define("test/RpcApprovals", {
          commands: Object.values(ApprovalCommand.cases),
        })(
          Effect.succeed({
            receive: (command) =>
              Effect.gen(function* () {
                if (command._tag !== "Resolve") return;
                submissions++;
                yield* command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "conflict",
                    message: "Approval not found",
                  }),
                });
              }),
          }),
        );
        const system = yield* ActorSystem.make();
        yield* (yield* system.spawn("approvals", Approvals)).awaitStarted;
        const server = yield* Effect.acquireRelease(
          Effect.promise(() => startTestHttp({ registry, actors: system })),
          (s) => Effect.promise(() => s.close()),
        );
        const send = (response: unknown) =>
          Effect.promise(async () => {
            const r = await fetch(
              `${server.url}/api/rpc`,
              rpcRequest("RespondToApproval", { id: "one", response }),
            );
            return await r.text();
          });
        assert.match(yield* send({ decision: "invalid" }), /Failure|Defect/);
        assert.equal(submissions, 0);
        assert.match(yield* send({ decision: "approve" }), /Approval not found/);
        assert.equal(submissions, 1);
      }),
    ),
  );
});

test(
  "bounded invalidation capture reports overflow and releases the source",
  { timeout: 5000 },
  async () => {
    const registry = await Effect.runPromise(makeContextRegistry());
    let released = false;
    const server = await startTestHttp({
      registry: {
        ...registry,
        reader: {
          ...registry.reader,
          subscribe: Effect.acquireRelease(
            Effect.succeed(
              Stream.fromIterable(
                Array.from({ length: 1000 }, (_, revision) => ({
                  record: { ...record, revision },
                })),
              ),
            ),
            () =>
              Effect.sync(() => {
                released = true;
              }),
          ),
        },
      },
    });
    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const client = yield* ApiClient.make;
            const error = yield* client("SubscribeInvalidations", undefined).pipe(
              Stream.runDrain,
              Effect.flip,
            );
            assert.equal(error._tag, "ApplicationError");
            assert.match(error.message, /fell behind/);
          }),
        ).pipe(
          Effect.provide(
            RpcClient.layerProtocolHttp({ url: `${server.url}/api/rpc` }).pipe(
              Layer.provide([FetchHttpClient.layer, RpcSerialization.layerNdjson]),
            ),
          ),
        ),
      );
      assert.equal(released, true);
    } finally {
      await server.close();
    }
  },
);
