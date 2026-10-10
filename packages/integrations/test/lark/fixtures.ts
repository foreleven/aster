import { ActorSystem } from "@aster/actor";
import { ContextCaptures, ContextQueries, DurableContext, RuntimeIntegrations } from "@aster/core";
import { makeTestContext, type TestContext } from "@aster/core/testing";
import { Effect, Layer } from "effect";
import {
  LarkIntegration,
  LarkSettings,
  LarkAccountClient,
  LarkMailClient,
  LarkImClient,
} from "../../src/lark/index.js";
import { larkSettings, type LarkSettingsValue } from "../../src/lark/testing.js";
import { ChatSummaryGate } from "../../src/lark/im/summary/gate.js";
import { ChatSummarizer } from "../../src/lark/im/summary/summarizer.js";
import { SummaryAdmission, type AgentAdmission } from "../../src/lark/im/summary/admission.js";

const immediate: AgentAdmission = { run: (_chat, execute) => execute };

export const enabledSettings = () =>
  larkSettings({
    children: { "/mail": {}, "/im": { config: { summary: { model: "test" } } } },
  });
export const installLark = Effect.fn("test.installLark")(function* (
  options: {
    readonly settings?: LarkSettingsValue;
    readonly registry?: TestContext;
    readonly account?: LarkAccountClient["Service"];
    readonly mail?: LarkMailClient["Service"];
    readonly im?: LarkImClient["Service"];
    readonly gate?: ChatSummaryGate["Service"];
    readonly summarizer?: ChatSummarizer["Service"];
  } = {},
) {
  const registry = options.registry ?? (yield* makeTestContext());
  return yield* Effect.gen(function* () {
    yield* Layer.build(LarkIntegration.installation);
    const modules = yield* RuntimeIntegrations;
    const queries = yield* ContextQueries;
    const captures = yield* ContextCaptures;
    const module = modules.installed()[0]!;
    const system = yield* ActorSystem.make().pipe(
      ActorSystem.provide(Layer.succeedContext(module.services)),
    );
    const handle = yield* module.activate(system);
    return { registry, system, handle, queries, captures };
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        RuntimeIntegrations.layer,
        ContextQueries.layer,
        ContextCaptures.layer,
        Layer.succeed(DurableContext, registry.backend),
        Layer.succeed(LarkSettings, options.settings ?? enabledSettings()),
        Layer.succeed(LarkAccountClient, options.account ?? { getAccount: () => Effect.never }),
        Layer.succeed(
          LarkMailClient,
          options.mail ?? {
            getMailboxProfile: () => Effect.never,
            listIds: () => Effect.succeed([]),
            getMessages: () => Effect.succeed([]),
            listMessages: () => Effect.die("Unexpected mail query"),
          },
        ),
        Layer.succeed(
          LarkImClient,
          options.im ?? {
            searchMessages: () => Effect.succeed([]),
            getChatSettings: () => Effect.succeed([]),
            listMessages: () => Effect.die("Unexpected IM history query"),
          },
        ),
        Layer.succeed(ChatSummaryGate, options.gate ?? { needed: () => Effect.succeed(false) }),
        Layer.succeed(
          ChatSummarizer,
          options.summarizer ?? { summarize: () => Effect.die("Unexpected summary execution") },
        ),
        Layer.succeed(SummaryAdmission, immediate),
      ),
    ),
  );
});
