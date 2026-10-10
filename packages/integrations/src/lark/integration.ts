import { Actor, type ActorContext } from "@aster/actor";
import {
  ContextCaptures,
  ContextQueries,
  DurableContext,
  defineIntegration,
  RuntimeIntegrations,
} from "@aster/core";
import { Context, Effect, Layer } from "effect";
import { LarkAccountClient } from "./account/client.js";
import { makeLarkRootActor, type AccountCommand } from "./account/actor.js";
import { LarkSettings } from "./settings.js";
import { larkCaptures } from "./captures.js";
import { SummaryAdmission } from "./im/summary/admission.js";
import { LarkImClient } from "./im/client.js";
import { ChatSummarizer } from "./im/summary/summarizer.js";
import { ChatSummaryGate } from "./im/summary/gate.js";
import { LarkMailClient } from "./mail/client.js";
import { LarkMailActor } from "./mail/actor.js";
import { LarkImActor } from "./im/actor.js";
import { LarkCli } from "./cli.js";

/** Resolve enablement before acquiring any capability-specific resource. */
const services = Layer.unwrap(
  Effect.map(LarkSettings, (settings) =>
    Layer.mergeAll(
      LarkAccountClient.layer,
      settings.mail ? LarkMailClient.layer : Layer.empty,
      settings.im
        ? Layer.mergeAll(
            LarkImClient.layer,
            ChatSummarizer.layer,
            ChatSummaryGate.layer,
            SummaryAdmission.layer,
          )
        : Layer.empty,
    ).pipe(Layer.provide(LarkCli.layer)),
  ),
).pipe(Layer.provideMerge(LarkSettings.layer));

/** Tests may supply clients/model capabilities directly, without starting a native transport. */
const installation = Layer.effectDiscard(
  Effect.gen(function* () {
    const modules = yield* RuntimeIntegrations;
    const settings = yield* LarkSettings;
    const base = Context.make(LarkSettings, settings).pipe(
      Context.add(LarkAccountClient, yield* LarkAccountClient),
      Context.add(ContextQueries, yield* ContextQueries),
      Context.add(DurableContext, yield* DurableContext),
    );
    const mail = settings.mail
      ? LarkMailActor.pipe(
          Actor.provide(
            Layer.succeedContext(Context.add(base, LarkMailClient, yield* LarkMailClient)),
          ),
        )
      : undefined;
    const im = settings.im
      ? yield* Effect.gen(function* () {
          const gate = yield* ChatSummaryGate;
          const dependencies = base.pipe(
            Context.add(LarkImClient, yield* LarkImClient),
            Context.add(ChatSummarizer, yield* ChatSummarizer),
            Context.add(SummaryAdmission, yield* SummaryAdmission),
            Context.add(ChatSummaryGate, gate),
          );
          return LarkImActor.pipe(Actor.provide(Layer.succeedContext(dependencies)));
        })
      : undefined;
    yield* (yield* ContextCaptures).register(larkCaptures(settings.mail !== undefined));
    const root = makeLarkRootActor((context: ActorContext<AccountCommand>) =>
      Effect.gen(function* () {
        if (mail && !(yield* context.child("mail")))
          yield* (yield* context.spawn("mail", mail)).awaitStarted;
        if (im && !(yield* context.child("im")))
          yield* (yield* context.spawn("im", im)).awaitStarted;
      }),
    );
    yield* modules.register(
      defineIntegration({
        name: "lark",
        phase: "source",
        services: base,
        activate: (system) =>
          Effect.gen(function* () {
            const actor = yield* system.spawn("lark", root);
            yield* actor.awaitStarted;
            return { stop: system.stop(actor) };
          }),
      }),
    );
  }),
);

export const LarkIntegration = {
  services,
  installation,
  layer: installation.pipe(Layer.provide(services)),
};
