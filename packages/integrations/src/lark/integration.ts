import { AgentRunner } from "@aster/agent/agent";
import {
  ContextCaptures,
  ContextQueries,
  ContextRegistry,
  defineIntegration,
  RuntimeConfigurationError,
  RuntimeIntegrations,
  SystemOneClient,
} from "@aster/core";
import { Context, Deferred, Effect, Fiber, Layer, Stream } from "effect";
import { LarkAccountCli, makeAccountClient } from "./account/client.js";
import { LarkRootActor } from "./account/root-actor.js";
import { LarkConfig } from "./config.js";
import { larkCaptures } from "./context-policies.js";
import { ImAgentQueue } from "./im/summary/agent-queue.js";
import { LarkChatService } from "./im/service/chat-service.js";
import { ChatSummarizer } from "./im/summary/summarizer.js";
import { ChatSummaryGate } from "./im/summary/gate.js";
import { LarkMailCli, liveCli } from "./mail/client.js";
import { larkContextViews } from "./public-views.js";
const services = Layer.effect(
  LarkMailCli,
  Effect.gen(function* () {
    const config = yield* LarkConfig;
    return liveCli(config.profile);
  }),
).pipe(
  Layer.merge(
    Layer.effect(
      LarkAccountCli,
      Effect.gen(function* () {
        const config = yield* LarkConfig;
        return makeAccountClient(config.profile);
      }),
    ),
  ),
  Layer.merge(ChatSummarizer.layer.pipe(Layer.provide(AgentRunner.layer))),
  Layer.merge(ChatSummaryGate.layer),
  Layer.merge(ImAgentQueue.layer),
  Layer.merge(LarkChatService.layer),
  Layer.provideMerge(LarkConfig.layer),
);

export const LarkIntegration = {
  services,
  layer: Layer.effectDiscard(
    Effect.gen(function* () {
      const modules = yield* RuntimeIntegrations;
      const registry = yield* ContextRegistry;
      yield* registry.views.register(larkContextViews);
      yield* (yield* ContextCaptures).register(larkCaptures);
      const im = (yield* LarkConfig).im;
      if (im !== undefined) {
        const client = yield* SystemOneClient;
        if (client.configured === false)
          return yield* Effect.fail(
            new RuntimeConfigurationError({ message: "Lark IM requires config.system-one" }),
          );
      }
      const dependencies = Context.pick(
        ContextQueries,
        ContextRegistry,
        LarkConfig,
        LarkAccountCli,
        LarkMailCli,
        ChatSummarizer,
        LarkChatService,
        ImAgentQueue,
        ChatSummaryGate,
      )(
        yield* Effect.context<
          | ContextQueries
          | ContextRegistry
          | LarkConfig
          | LarkAccountCli
          | LarkMailCli
          | ChatSummarizer
          | LarkChatService
          | ImAgentQueue
          | ChatSummaryGate
        >(),
      );
      yield* modules.register(
        defineIntegration({
          name: "lark",
          phase: "source",
          services: dependencies,
          activate: (system) =>
            Effect.gen(function* () {
              const ready = yield* Deferred.make<void>();
              // Subscribe before spawning; do not trust a previous process's persisted ready flag.
              const changes = yield* registry.subscribe;
              const observer = yield* Stream.runForEach(changes, (change) =>
                change.record.path === "/lark/im" &&
                (change.record.state as { ready?: boolean }).ready
                  ? Deferred.succeed(ready, undefined).pipe(Effect.asVoid)
                  : Effect.void,
              ).pipe(Effect.forkScoped);
              const actor = yield* system.spawn("lark", LarkRootActor);
              if (im === undefined) yield* Deferred.succeed(ready, undefined);
              return {
                ready: actor.awaitStarted.pipe(Effect.andThen(Deferred.await(ready))),
                stop: system.stop(actor).pipe(Effect.andThen(Fiber.interrupt(observer))),
              };
            }),
        }),
      );
    }),
  ).pipe(Layer.provide(services)),
};
