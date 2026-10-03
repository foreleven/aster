import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect } from "effect";
import { makeApplicationApi, ApplicationError, makeContextRegistry } from "@aster/core";
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
      // Build the request using a typed one-shot reply port, just like ActorRef.ask.
      const probe = yield* ActorTestKit.probe<Response>();
      const command = build(probe.ref);
      commands.push(
        command._tag === "Route" && command.command._tag === "UserMessage"
          ? { ...command, command: { _tag: "UserMessage", text: command.command.text } }
          : command,
      );
      if (
        command._tag === "Route" &&
        (command.command._tag === "UserMessage" || command.command._tag === "End")
      )
        yield* command.command.replyTo!.tell({ _tag: "Accepted" });
      return yield* probe.take();
    }).pipe(Effect.scoped),
});

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
        ...makeApplicationApi({ registry, inspect: Effect.succeed(null) }),
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
          slug: "project",
          description: "Project",
          status: "active",
          progress: "",
          summary: "",
          tasks: [],
          historyThrough: 0,
          historyCount: 0,
          pendingEvaluation: false,
          receivedEvents: [],
        },
        messages: [],
      },
      { expectedRevision: registry.get("/goals/project")?.revision ?? 0 },
    ),
  );
  const commands: GoalsRootCommand[] = [];
  const api = await startGoalApi({
    application: makeApplicationApi({
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
    assert.deepEqual(commands, [
      {
        _tag: "Route",
        slug: "project",
        command: { _tag: "UserMessage", text: "Focus on the frontend" },
      },
    ]);
    assert.equal((await fetch(`${api.url}/api/context?path=/missing`)).status, 404);
  } finally {
    await api.close();
  }
});

test("approval API validates responses and preserves same-origin checks", async () => {
  const registry = await Effect.runPromise(makeContextRegistry());
  const received: unknown[] = [];
  const base = makeApplicationApi({ registry, inspect: Effect.succeed(null) });
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
          slug: "test",
          description: "Test",
          status: "active",
          progress: "",
          summary: "",
          tasks: [],
          historyThrough: 0,
          historyCount: 0,
          pendingEvaluation: false,
          receivedEvents: [],
        },
        messages: [],
      },
      { expectedRevision: registry.get("/goals/test")?.revision ?? 0 },
    ),
  );
  const api = await startGoalApi({
    application: makeApplicationApi({
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
  const { makeMemoryGoalHistory } = await import("@aster/core");
  const registry = await Effect.runPromise(makeContextRegistry());
  await Effect.runPromise(registry.register("/goals/feed", GoalActor.context));
  await Effect.runPromise(
    registry.commit(
      {
        path: "/goals/feed",
        description: "Feed",
        state: {
          slug: "feed",
          description: "Feed",
          status: "active",
          progress: "",
          summary: "Older history summarized",
          tasks: [],
          historyThrough: 60,
          historyCount: 65,
          pendingEvaluation: false,
          receivedEvents: [],
        },
        messages: [],
      },
      { expectedRevision: registry.get("/goals/feed")?.revision ?? 0 },
    ),
  );
  const history = makeMemoryGoalHistory();
  for (let i = 0; i < 65; i++)
    await Effect.runPromise(
      history.append("feed", { role: "user", content: `Record ${i}`, timestamp: i }),
    );
  const api = await startGoalApi({
    application: makeApplicationApi({ registry, history, inspect: Effect.succeed(null) }),
    port: 0,
  });
  try {
    const page = (await (await fetch(`${api.url}/api/goals/feed/history`)).json()) as any;
    assert.equal(page.entries.length, 30);
    assert.equal(page.entries[0].seq, 36);
    const older = (await (
      await fetch(`${api.url}/api/goals/feed/history?before=${page.nextBefore}`)
    ).json()) as any;
    assert.equal(older.entries.at(-1).seq, 35);
    assert.equal((await fetch(`${api.url}/api/goals/feed/history?limit=200`)).status, 400);
  } finally {
    await api.close();
  }
});
