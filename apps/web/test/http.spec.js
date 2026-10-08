import { test, expect } from "@playwright/test";
import { at } from "./fixtures.js";
test("built dashboard reads real HTTP runtime and refreshes public Context changes", async ({
  page,
}) => {
  const { createRequire } = await import("node:module");
  const { fileURLToPath } = await import("node:url");
  const requireLocal = createRequire(new URL("../../local/package.json", import.meta.url));
  const { Effect, Layer, Schema } = await import(requireLocal.resolve("effect"));
  const { AsterRuntime, ContextRegistry, ContextQueries, GoalActor } =
    await import("../../../packages/core/dist/index.js");
  const { ActorSystem, Actor } = await import("../../../packages/actor/dist/index.js");
  const { makeHttpApi } = await import("../../local/dist/http-api.js");
  const { makeContextRegistry } = await import("../../../packages/core/dist/testing/context.js");
  const registry = await Effect.runPromise(makeContextRegistry());
  await Effect.runPromise(registry.register("/goals/real-http", GoalActor.context));
  const record = {
    path: "/goals/real-http",
    revision: 1,
    description: "Live HTTP validation Goal",
    state: {
      definition: { slug: "real-http", description: "Live HTTP validation Goal" },
      status: "active",
      summary: "",
      tasks: [],
      inputs: [],
      receipts: [],
    },
    messages: [],
  };
  await Effect.runPromise(registry.commit(record, { expectedRevision: 0 }));
  const { Scope, Exit } = await import(requireLocal.resolve("effect"));
  const scope = await Effect.runPromise(Scope.make());
  const system = await Effect.runPromise(
    ActorSystem.make().pipe(Effect.provideService(Scope.Scope, scope)),
  );
  class Preview extends Actor.Service()("dashboard/Preview", {
    command: Schema.String,
  }) {}
  Preview.layer = Layer.succeed(Preview, Preview.of({ receive: () => Effect.void }));
  const ref = await Effect.runPromise(
    system.spawn("preview", Preview, {
      metadata: { contextPath: record.path },
    }),
  );
  await Effect.runPromise(ref.tell("inspect"));
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));
  page.on("console", (message) => {
    if (message.type() === "error" || message.type() === "warning") errors.push(message.text());
  });
  const { AgentConversations } = await import("../../../packages/agent/dist/harness/index.js");
  const history = await Effect.runPromise(
    AgentConversations.makeMemory().pipe(Effect.provideService(Scope.Scope, scope)),
  );
  const { NodeFileSystem } = await import(requireLocal.resolve("@effect/platform-node"));
  const bound = await Effect.runPromise(
    makeHttpApi({
      port: 0,
      webDir: fileURLToPath(new URL("../dist", import.meta.url)),
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(AsterRuntime, {
            actors: system,
            ready: Effect.void,
            inspect: system
              .inspect({ metadata: ["contextPath"] })
              .pipe(Effect.map((actors) => ({ actors, events: [], phase: "ready" }))),
          }),
          Layer.succeed(ContextRegistry, registry),
          Layer.succeed(AgentConversations, history),
          ContextQueries.layer,
          NodeFileSystem.layer,
        ),
      ),
      Effect.provideService(Scope.Scope, scope),
    ),
  );
  const api = { ...bound, close: () => Effect.runPromise(Scope.close(scope, Exit.void)) };
  try {
    await page.goto(`${api.url}/?context=%2Fgoals%2Freal-http`);
    await expect(page).toHaveTitle("Aster · Your personal assistant");
    await expect(page.getByText("Live", { exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Live HTTP validation Goal" })).toBeVisible();
    await Effect.runPromise(
      history.append("/goals/real-http", "reply", "goal.reply", {
        inputId: "input",
        text: "Live HTTP and streaming RPC updates received",
      }),
    );
    const updated = {
      ...record,
      messages: [
        {
          type: "assistant",
          text: "Live HTTP and streaming RPC updates received",
          references: [],
          at,
        },
      ],
    };
    await Effect.runPromise(
      registry.commit(updated, { expectedRevision: registry.get(updated.path)?.revision ?? 0 }),
    );
    await expect(
      page.getByText("Live HTTP and streaming RPC updates received", { exact: true }),
    ).toBeVisible();
    expect(errors).toEqual([]);
  } finally {
    try {
      if (!page.isClosed()) await page.goto("about:blank");
    } finally {
      try {
        await api.close();
      } finally {
        await Effect.runPromise(Scope.close(scope, Exit.void));
      }
    }
  }
});
