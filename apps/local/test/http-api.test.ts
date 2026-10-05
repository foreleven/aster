import { testConversations } from "./conversation-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { once } from "node:events";
import { Server } from "node:http";
import { createConnection } from "node:net";
import { Deferred, Effect, Option } from "effect";
import { NodeHttpServerRequest } from "@effect/platform-node";
import { HttpServerRequest } from "effect/http";
import { makeApplicationApi, ApplicationError } from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import { GoalActor, type GoalsRootCommand } from "@aster/core";
import { ActorTestKit, type ActorRef } from "@aster/actor";
import { startGoalApi } from "../src/http-api.js";

const goalRef = (commands: GoalsRootCommand[]): ActorRef<GoalsRootCommand> => ({
  path: "/user/goals",
  incarnation: "test",
  tell: (command) =>
    Effect.sync(() => {
      commands.push(command);
    }),
  ask: <Response>(build: (reply: ActorRef<Response>) => GoalsRootCommand) =>
    Effect.gen(function* () {
      const probe = yield* ActorTestKit.probe<Response>();
      const command = build(probe.ref);
      commands.push(command);
      if (command._tag === "Route")
        yield* command.command.replyTo.tell({
          _tag: "Accepted",
          receipt: { requestId: command.command.requestId, revision: 1 },
        });
      return yield* probe.take();
    }).pipe(Effect.scoped),
});

test(
  "HTTP shutdown closes requests arriving after handler removal",
  { timeout: 5000 },
  async () => {
    const registry = await Effect.runPromise(makeContextRegistry());
    const started = await Effect.runPromise(Deferred.make<Server>());
    const finalizing = await Effect.runPromise(Deferred.make<void>());
    const release = await Effect.runPromise(Deferred.make<void>());
    const api = await startGoalApi({
      port: 0,
      application: {
        ...makeApplicationApi({
          registry,
          conversations: testConversations(),
          inspect: Effect.succeed(null),
        }),
        dashboard: Effect.gen(function* () {
          const request = yield* Effect.serviceOption(HttpServerRequest.HttpServerRequest);
          assert.ok(Option.isSome(request));
          const { socket } = NodeHttpServerRequest.toIncomingMessage(request.value);
          assert.ok("server" in socket && socket.server instanceof Server);
          yield* Deferred.succeed(started, socket.server);
          return yield* Effect.never;
        }).pipe(
          Effect.ensuring(
            Deferred.succeed(finalizing, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ),
        ),
      },
    });
    const abort = new AbortController();
    const response = fetch(`${api.url}/api/dashboard`, { signal: abort.signal }).catch(
      () => undefined,
    );
    const server = await Effect.runPromise(Deferred.await(started));
    const socket = createConnection({ host: "127.0.0.1", port: Number(new URL(api.url).port) });
    let closing: Promise<void> | undefined;
    try {
      await once(socket, "connect");
      closing = api.close();
      await Effect.runPromise(Deferred.await(finalizing));
      assert.equal(server.listenerCount("request"), 0);
      // The listener is still open while admitted request finalizers drain. A browser
      // reconnect can reach this socket after the Effect request handler is removed.
      const incoming = once(server, "request");
      socket.write("GET /api/events HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
      await incoming;
      const closed = once(server, "close", { signal: AbortSignal.timeout(1000) });
      await Effect.runPromise(Deferred.succeed(release, undefined));
      await closed;
      await closing;
    } finally {
      socket.destroy();
      abort.abort();
      await Effect.runPromise(Deferred.succeed(release, undefined));
      await (closing ?? api.close());
      await response;
    }
  },
);

test(
  "HTTP shutdown interrupts active application Effects and waits for their finalizers",
  { timeout: 3000 },
  async () => {
    const registry = await Effect.runPromise(makeContextRegistry());
    const started = Promise.withResolvers<void>();
    let released = false;
    const api = await startGoalApi({
      port: 0,
      application: {
        ...makeApplicationApi({
          registry,
          conversations: testConversations(),
          inspect: Effect.succeed(null),
        }),
        dashboard: Effect.sync(() => started.resolve()).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Effect.sleep(20).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  released = true;
                }),
              ),
            ),
          ),
        ),
      },
    });
    const response = fetch(`${api.url}/api/dashboard`).catch(() => undefined);
    try {
      await started.promise;
    } finally {
      await api.close();
    }
    assert.equal(released, true);
    await response;
  },
);

test("Goal HTTP API reads public messages, routes user input, and rejects cross-origin writes", async () => {
  const registry = await Effect.runPromise(makeContextRegistry());
  await Effect.runPromise(registry.register("/goals/project", GoalActor.context));
  await Effect.runPromise(
    registry.commit(
      {
        path: "/goals/project",
        description: "Project",
        state: {
          definition: { slug: "project", description: "Project" },
          status: "active",
          summary: "",
          inputs: [],
          receipts: [],
        },
        messages: [],
      },
      { expectedRevision: registry.get("/goals/project")?.revision ?? 0 },
    ),
  );
  const commands: GoalsRootCommand[] = [];
  const api = await startGoalApi({
    application: makeApplicationApi({
      conversations: testConversations(),
      registry,
      goals: goalRef(commands),
      inspect: Effect.succeed(null),
    }),
    port: 0,
  });
  try {
    const goals = (await (await fetch(`${api.url}/api/goals`)).json()) as unknown[];
    assert.equal(goals.length, 1);
    const send = (origin: string) =>
      fetch(`${api.url}/api/goals/project/messages`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Origin: origin },
        body: JSON.stringify({ text: "Focus on the frontend" }),
      });
    assert.equal((await send("https://untrusted.example")).status, 403);
    assert.equal(commands.length, 0);
    assert.equal(
      (
        await fetch(`${api.url}/api/goals/project/messages`, {
          method: "POST",
          body: "plain text",
        })
      ).status,
      415,
    );
    assert.equal(
      (
        await fetch(`${api.url}/api/goals/project/messages`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ text: "x".repeat(33 * 1024) }),
        })
      ).status,
      413,
    );
    assert.equal(commands.length, 0);
    assert.equal((await send(api.url)).status, 202);
    assert.equal(commands.length, 1);
    const routed = commands[0];
    assert.equal(routed._tag, "Route");
    assert.ok(routed._tag === "Route" && routed.command._tag === "SubmitInput");
    assert.equal(routed.slug, "project");
    assert.deepEqual(routed.command.input, { _tag: "UserInput", text: "Focus on the frontend" });
    assert.ok(routed.command.requestId);
    assert.equal((await fetch(`${api.url}/api/context?path=/missing`)).status, 404);
  } finally {
    await api.close();
  }
});

test("approval API validates responses and preserves same-origin checks", async () => {
  const registry = await Effect.runPromise(makeContextRegistry());
  const received: unknown[] = [];
  const base = makeApplicationApi({
    registry,
    conversations: testConversations(),
    inspect: Effect.succeed(null),
  });
  const api = await startGoalApi({
    application: {
      ...base,
      approvals: {
        ...base.approvals,
        respond: (id, response) =>
          Effect.gen(function* () {
            received.push({ id, response });
            if (id === "missing")
              return yield* Effect.fail(
                new ApplicationError({ kind: "conflict", message: "Approval not found" }),
              );
          }),
      },
    },
    port: 0,
  });
  try {
    const send = (body: unknown, origin = api.url) =>
      fetch(`${api.url}/api/approvals/respond`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Origin: origin },
        body: JSON.stringify(body),
      });
    assert.equal(
      (await send({ id: "one", response: { decision: "approve" } }, "https://other.example"))
        .status,
      403,
    );
    assert.equal((await send({ id: "one", response: { decision: "invalid" } })).status, 400);
    assert.equal(received.length, 0);
    assert.equal((await send({ id: "one", response: { decision: "approve" } })).status, 202);
    assert.deepEqual(received, [{ id: "one", response: { decision: "approve" } }]);
    assert.equal((await send({ id: "missing", response: { text: "reply" } })).status, 409);
    assert.deepEqual(await (await fetch(`${api.url}/api/approvals`)).json(), []);
  } finally {
    await api.close();
  }
});

test("dashboard returns public contexts and runtime observations with origin protection", async () => {
  const registry = await Effect.runPromise(makeContextRegistry());
  await Effect.runPromise(registry.register("/goals/test", GoalActor.context));
  await Effect.runPromise(
    registry.commit(
      {
        path: "/goals/test",
        description: "Test",
        state: {
          definition: { slug: "test", description: "Test" },
          status: "active",
          summary: "",
          inputs: [],
          receipts: [],
        },
        messages: [],
      },
      { expectedRevision: registry.get("/goals/test")?.revision ?? 0 },
    ),
  );
  const api = await startGoalApi({
    application: makeApplicationApi({
      conversations: testConversations(),
      registry,
      inspect: Effect.succeed({
        actors: [{ path: "/user/goals/test", mailboxSize: 2 }],
        events: [],
      }),
    }),
    port: 0,
  });
  try {
    const result = (await (await fetch(`${api.url}/api/dashboard`)).json()) as any;
    assert.equal(result.contexts[0].path, "/goals/test");
    assert.equal(result.runtime.actors[0].mailboxSize, 2);
    assert.equal(
      (await fetch(`${api.url}/api/dashboard`, { headers: { Origin: "https://other.example" } }))
        .status,
      403,
    );
  } finally {
    await api.close();
  }
});

test("Goal feed paginates full history independently of its working messages", async () => {
  const registry = await Effect.runPromise(makeContextRegistry());
  await Effect.runPromise(registry.register("/goals/feed", GoalActor.context));
  await Effect.runPromise(
    registry.commit(
      {
        path: "/goals/feed",
        description: "Feed",
        state: {
          definition: { slug: "feed", description: "Feed" },
          status: "active",
          summary: "Older history summarized",
          inputs: [],
          receipts: [],
        },
        messages: [],
      },
      { expectedRevision: registry.get("/goals/feed")?.revision ?? 0 },
    ),
  );
  const history = testConversations();
  for (let i = 0; i < 65; i++)
    await Effect.runPromise(
      history.append("/goals/feed", `input-${i}`, "goal.input", {
        payload: { _tag: "UserInput", text: `Record ${i}` },
      }),
    );
  const api = await startGoalApi({
    application: makeApplicationApi({
      registry,
      conversations: history,
      inspect: Effect.succeed(null),
    }),
    port: 0,
  });
  try {
    const page = (await (await fetch(`${api.url}/api/goals/feed/history`)).json()) as any;
    assert.equal(page.entries.length, 30);
    assert.equal(page.entries[0].message.content, "Record 35");
    const older = (await (
      await fetch(`${api.url}/api/goals/feed/history?before=${page.nextBefore}`)
    ).json()) as any;
    assert.equal(older.entries.at(-1).message.content, "Record 34");
    assert.equal((await fetch(`${api.url}/api/goals/feed/history?limit=200`)).status, 400);
  } finally {
    await api.close();
  }
});
