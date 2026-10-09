import { AgentRunner, Models } from "@aster/agent";
import { LocalConfig, SystemOneClientLive } from "@aster/infra";
import { LarkConfig } from "@aster/integrations";
import { makeGoalReasoner } from "@aster/core";
import {
  ImAgentQueue,
  ImSummaryGate,
  makeImSummaryGate,
  ChatSummarizer,
  LarkChatActor,
} from "@aster/integrations";
// Synthetic evidence only; real configured models, isolated actors, no external delegation.
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { ActorSystem } from "@aster/actor";
import {
  ContextRegistry,
  contextSpawnOptions,
  makeContextRegistry,
  relevantGoals,
  SystemOneClient,
  GoalSettings,
} from "@aster/core";

import { ConfigProvider, Deferred, Effect, Layer, Stream } from "effect";

const root = new URL("../../../", import.meta.url);
const env = fileURLToPath(new URL(".env", root));
const configuration = LocalConfig.layer({
  configPath: fileURLToPath(new URL("aster.config.yaml", root)),
  envPath: env,
  projectRoot: fileURLToPath(root),
});
const goal = {
  slug: "summary-verification",
  description:
    "As the Knowledge Engine frontend tech lead, continuously monitor project progress and promptly analyze blockers and risks affecting frontend delivery.",
};
const path = "/lark/im/chats/summary-verification";
const report = await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const provider = yield* ConfigProvider.ConfigProvider;
      const model = (yield* GoalSettings).reasoning?.model;
      if (!model) throw new Error("Configure config.goals.model before this verification");
      const underlying = yield* SystemOneClient;
      const client = {
        systemOne: (request) =>
          underlying
            .systemOne(request)
            .pipe(
              Effect.tap((result) =>
                Effect.sync(() =>
                  console.log(
                    JSON.stringify({ event: "verification.system-one.response", result }),
                  ),
                ),
              ),
            ),
      };
      const dir = yield* Effect.acquireRelease(
        Effect.sync(() => mkdtempSync(join(tmpdir(), "aster-summary-"))),
        (dir) => Effect.sync(() => rmSync(dir, { recursive: true, force: true })),
      );
      const registry = yield* makeContextRegistry();
      const completed = yield* Deferred.make();
      const reasoner = yield* makeGoalReasoner(model, {
        search: () => Effect.succeed({ results: [] }),
        expand: () => Effect.succeed({ results: [] }),
      }).pipe(Effect.provide(AgentRunner.layer.pipe(Layer.provide(Models.configured))));
      let systemOneMatched = false;
      const system = yield* ActorSystem.make().pipe(
        ActorSystem.provide(
          ConfigProvider.layer(provider),
          Models.configured,
          LarkConfig.layer,
          Layer.succeed(ContextRegistry, registry),
          ChatSummarizer.layer,
          ImAgentQueue.layer,
          Layer.succeed(ImSummaryGate, makeImSummaryGate(client)),
        ),
      );
      yield* Stream.runForEach(yield* registry.subscribe, (change) =>
        Effect.gen(function* () {
          if (change.record.path !== path || !change.events?.length) return;
          console.log(
            JSON.stringify({ event: "verification.summary", summary: change.record.state.summary }),
          );
          const matches = yield* relevantGoals(client, change.record, [goal]);
          systemOneMatched = matches.length > 0;
          if (!systemOneMatched)
            console.warn(
              JSON.stringify({
                event: "verification.gate.rejected",
                path,
                note: "Production routing stops here. The isolated diagnostic will test Goal reasoning separately; no execution is authorized by this result.",
              }),
            );
          if (yield* Deferred.isDone(completed)) return;
          yield* reasoner
            .plan({
              goal,
              current: {
                path: `/goals/${goal.slug}`,
                description: goal.description,
                state: {},
                messages: [],
              },
              contexts: registry.publicSnapshot(),
              signals: [],
              reason: `Independently verify Goal reasoning from the summary at ${path}`,
              durable: { sessionId: goal.slug, requestId: "verification", storageDirectory: dir },
              messages: [
                {
                  role: "user",
                  timestamp: 0,
                  content: `Read the summary at ${path} and propose a read-only risk analysis plan. Do not execute actions or send any messages.`,
                },
              ],
            })
            .pipe((effect) => Deferred.complete(completed, effect));
        }),
      ).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      const chat = yield* system.spawn("chat", LarkChatActor, contextSpawnOptions(path));
      yield* chat.tell({
        _tag: "Update",
        chat: {
          id: "summary-verification",
          name: "Knowledge Engine frontend project (synthetic validation)",
          mode: "group",
          description: "Synthetic test",
        },
        messages: [
          {
            id: "synthetic-1",
            at: "2026-09-29T09:00:00Z",
            content:
              "The new Knowledge Engine search page is scheduled to launch on Friday. Frontend API integration was originally planned to finish on Wednesday.",
            sender: {},
            url: "",
            deleted: false,
          },
          {
            id: "synthetic-2",
            at: "2026-09-29T09:10:00Z",
            content:
              "The backend team reports that the search API will be delayed until Thursday evening, leaving insufficient time for frontend integration and regression testing. Ask the frontend tech lead to analyze launch risks, dependencies, and recommendations. Do not send any external messages.",
            sender: {},
            url: "",
            deleted: false,
          },
        ],
      });
      const plan = yield* Deferred.await(completed).pipe(Effect.timeout("5 minutes"));
      const record = registry.get(path);
      return {
        systemOneMatched,
        automaticChainPassed: systemOneMatched,
        sourcePath: path,
        remainingMessages: record.messages.length,
        state: record.state,
        plan,
        delegated: false,
      };
    }),
  ).pipe(
    Effect.provide(
      Layer.mergeAll(SystemOneClientLive.layer, GoalSettings.layer).pipe(
        Layer.provideMerge(configuration),
      ),
    ),
  ),
);
console.log(JSON.stringify({ event: "verification.complete", ...report }, null, 2));
