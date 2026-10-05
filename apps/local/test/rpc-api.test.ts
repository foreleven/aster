import { AgentConversations } from "@aster/agent";
import { testConversations } from "./conversation-fixtures.js";
import { AgentRunner } from "@aster/agent";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Layer, Schema, Stream } from "effect";
import { FetchHttpClient } from "effect/http";
import { RpcClient, RpcSerialization } from "effect/rpc";
import { ApplicationRpcs } from "@aster/api-contracts";
import { ActorSystem } from "@aster/actor";
import {
  ApplicationError,
  defineContext,
  contextView,
  makeApplicationApi,
  ContextRegistry,
  GoalsRootActor,
  GoalSettings,
  GoalSignals,
  MemoryRecall,
  ExternalAgents,
} from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import { startGoalApi } from "../src/http-api.js";

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
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(GoalSettings, {
              definitions: [{ slug: "personal", description: "Assistant" }],
              reasoning: { model: "test" },
            }),
            Layer.succeed(AgentConversations, conversations),
            Layer.succeed(GoalSignals, {
              applySignal: () => Effect.die("Unexpected Signal"),
              deactivate: () => Effect.void,
            }),
            Layer.succeed(MemoryRecall, {
              search: () => Effect.succeed({ results: [] }),
              expand: () => Effect.succeed({ results: [] }),
            }),
            Layer.succeed(ExternalAgents, {}),
            Layer.succeed(
              AgentRunner,
              AgentRunner.make(() => Effect.die("No model expected")),
            ),
          ),
        );
        const goals = yield* system.spawn("goals", GoalsRootActor);
        yield* goals.ask((replyTo) => ({ _tag: "AwaitReady", stage: "restored", replyTo }));
        const application = makeApplicationApi({
          conversations,
          registry,
          goals,
          inspect: Effect.succeed(null),
        });
        const api = yield* Effect.acquireRelease(
          Effect.promise(() => startGoalApi({ port: 0, application })),
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
        assert.equal((yield* application.goals.timeline("personal", {})).messages.length, 1);
      }),
    ),
  );
});

test("RPC shares typed query contracts and propagates application failures without retrying mutations", async () => {
  const registry = await Effect.runPromise(makeContextRegistry());
  await Effect.runPromise(registry.register(record.path, definition));
  await Effect.runPromise(
    registry.commit(record, { expectedRevision: registry.get(record.path)?.revision ?? 0 }),
  );
  let submissions = 0;
  const application = makeApplicationApi({
    conversations: testConversations(),
    registry,
    inspect: Effect.succeed({ phase: "ready", actors: [], events: [] }),
  });
  const api = await startGoalApi({
    port: 0,
    application: {
      ...application,
      goals: {
        ...application.goals,
        sendMessage: () =>
          Effect.gen(function* () {
            submissions++;
            return yield* new ApplicationError({ kind: "conflict", message: "Revision changed" });
          }),
      },
    },
  });
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* RpcClient.make(ApplicationRpcs, { flatten: true });
          assert.deepEqual(yield* client("ListContexts", undefined), [
            { ...record, revision: 1, projection: { version: 1, visibility: "public" } },
          ]);
          assert.deepEqual(yield* client("GetContext", { path: record.path }), {
            ...record,
            revision: 1,
            projection: { version: 1, visibility: "public" },
          });
          assert.deepEqual(yield* client("InspectRuntime", undefined), {
            phase: "ready",
            actors: [],
            events: [],
          });
          const failure = yield* Effect.result(
            client("SendGoalMessage", { slug: "test", text: "hello" }),
          );
          assert.equal(failure._tag, "Failure");
          if (failure._tag === "Failure") {
            assert.ok(failure.failure instanceof ApplicationError);
            assert.equal(failure.failure.message, "Revision changed");
          }
          assert.equal(submissions, 1);
        }),
      ).pipe(
        Effect.provide(
          RpcClient.layerProtocolHttp({ url: `${api.url}/api/rpc` }).pipe(
            Layer.provide([FetchHttpClient.layer, RpcSerialization.layerNdjson]),
          ),
        ),
      ),
    );
    const denied = await fetch(`${api.url}/api/rpc`, {
      method: "POST",
      headers: { Origin: "https://other.example", "content-type": "application/ndjson" },
      body:
        JSON.stringify({
          _tag: "Request",
          id: "1",
          tag: "SendGoalMessage",
          payload: { slug: "test", text: "no" },
          headers: [],
        }) + "\n",
    });
    assert.equal(denied.status, 403);
    assert.equal(submissions, 1);
  } finally {
    await api.close();
  }
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
  const api = await startGoalApi({
    port: 0,
    application: makeApplicationApi({
      registry,
      conversations: testConversations(),
      inspect: Effect.succeed(null),
    }),
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
      projection: { version: 1, visibility: "public" },
      state: { nested: { active: false } },
      messages: [{ role: "assistant", content: [{ type: "text", text: "done" }] }],
    });
  } finally {
    await api.close();
  }
});

test(
  "SSE subscribes before ready, broadcasts committed keys, and releases disconnected subscribers",
  { timeout: 5000 },
  async () => {
    const registry = await Effect.runPromise(makeContextRegistry());
    await Effect.runPromise(registry.register(record.path, definition));
    const application = makeApplicationApi({
      registry,
      conversations: testConversations(),
      inspect: Effect.succeed(null),
    });
    let active = 0;
    const released = Promise.withResolvers<void>();
    const api = await startGoalApi({
      port: 0,
      application: {
        ...application,
        subscribeInvalidations: Effect.acquireRelease(
          Effect.sync(() => {
            active++;
          }),
          () =>
            Effect.sync(() => {
              if (--active === 0) released.resolve();
            }),
        ).pipe(Effect.andThen(application.subscribeInvalidations)),
      },
    });
    const controllers = [new AbortController(), new AbortController()];
    try {
      const readers = await Promise.all(
        controllers.map(async (controller) => {
          const response = await fetch(`${api.url}/api/events`, { signal: controller.signal });
          const reader = response.body!.getReader();
          const first = await reader.read();
          assert.match(new TextDecoder().decode(first.value), /event: ready/);
          return reader;
        }),
      );
      assert.equal(active, 2);
      await Effect.runPromise(
        registry.commit(record, { expectedRevision: registry.get(record.path)?.revision ?? 0 }),
      );
      for (const reader of readers) {
        const frame = new TextDecoder().decode((await reader.read()).value);
        assert.match(frame, /event: invalidate/);
        assert.deepEqual(JSON.parse(frame.split("data: ")[1]!.trim()), {
          _tag: "Invalidate",
          keys: ["contexts", "context:/goals/test", "goals", "goal-history:test"],
        });
      }
      controllers.forEach((controller) => controller.abort());
      await released.promise;
      assert.equal(active, 0);
    } finally {
      controllers.forEach((controller) => controller.abort());
      await api.close();
    }
  },
);

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
        const api = makeApplicationApi({
          registry,
          conversations: testConversations(),
          inspect: Effect.succeed(null),
        });
        const changes = yield* api.subscribeInvalidations;
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

test(
  "HTTP shutdown interrupts an active RPC and awaits its cleanup",
  { timeout: 3000 },
  async () => {
    const registry = await Effect.runPromise(makeContextRegistry());
    const entered = Promise.withResolvers<void>();
    let finalized = false;
    const application = makeApplicationApi({
      registry,
      conversations: testConversations(),
      inspect: Effect.succeed(null),
    });
    const api = await startGoalApi({
      port: 0,
      application: {
        ...application,
        contexts: Effect.sync(() => entered.resolve()).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Effect.sync(() => {
              finalized = true;
            }),
          ),
        ),
      },
    });
    const controller = new AbortController();
    const response = fetch(`${api.url}/api/rpc`, {
      method: "POST",
      headers: { "content-type": "application/ndjson" },
      signal: controller.signal,
      body:
        JSON.stringify({
          _tag: "Request",
          id: "1",
          tag: "ListContexts",
          payload: null,
          headers: [],
        }) + "\n",
    })
      .then((response) => response.text())
      .catch(() => undefined);
    try {
      await entered.promise;
      await api.close();
      assert.equal(finalized, true);
    } finally {
      controller.abort();
      await api.close();
      await response;
    }
  },
);
