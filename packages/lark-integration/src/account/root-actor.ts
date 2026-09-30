import { Effect, Match, Layer, Schema } from "effect";

import { ContextActor, ContextRegistry, defineContext } from "@aster/core";
import { AccountProfile } from "./model.js";
import { LarkConfig } from "../config.js";
import { LarkMailCli } from "../mail/client.js";
import { LarkAccountCli } from "./client.js";
import { LarkEmailChannelActor } from "../mail/channel-actor.js";
import { LarkImActor } from "../im/channel-actor.js";
import { ImSearch } from "../im/client.js";
import { ImAgentQueue } from "../im/agent-queue.js";
import { ImSummaryGate } from "../im/summary-gate.js";
import { ImStorage } from "../im/storage.js";
import { ChatSummarizer } from "../im/summarizer.js";
import { profileCapture } from "../shared/profile.js";
const LarkRootCommand = Schema.TaggedStruct("AccountLoaded", {
  result: Schema.Union([
    Schema.TaggedStruct("Success", { value: AccountProfile }),
    Schema.TaggedStruct("Failure", {
      error: Schema.instanceOf(Error),
    }),
  ]),
});
type LarkRootCommand = typeof LarkRootCommand.Type;

export class LarkRootActor extends ContextActor.Service<
  LarkRootActor,
  | LarkConfig
  | LarkMailCli
  | LarkAccountCli
  | ChatSummarizer
  | ImStorage
  | ImSearch
  | ImAgentQueue
  | ImSummaryGate
>()("lark/RootActor", {
  command: LarkRootCommand,
  context: defineContext({
    identity: "Lark account",
    state: Schema.Struct({ account: Schema.optional(AccountProfile) }),
    message: Schema.Never,
    capture: (record) =>
      record.state.account === undefined
        ? undefined
        : {
            sessionId: profileCapture(record, record.state.account),
            records: [record],
          },
  }),
}) {
  static readonly layer = Layer.effect(
    LarkRootActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const config = yield* LarkConfig;
      const cli = yield* LarkAccountCli;
      return LarkRootActor.of({
        started: (context) =>
          Effect.gen(function* () {
            yield* registry.set({
              path: "/lark",
              description: config.description,
              state: {},
              messages: [],
            });
            yield* context.pipeToSelf(cli.getAccount(), (result) => ({
              _tag: "AccountLoaded",
              result,
            }));
            if ((yield* context.child("mail")) === undefined) {
              yield* context.spawn("mail", LarkEmailChannelActor);
            }
            if (config.im !== undefined && (yield* context.child("im")) === undefined)
              yield* context.spawn("im", LarkImActor);
          }),
        receive: (command) =>
          Match.value(command).pipe(
            Match.tag("AccountLoaded", ({ result }) =>
              Match.value(result).pipe(
                Match.tag("Failure", (result) => Effect.logWarning(result.error.message)),
                Match.tag("Success", (result) =>
                  registry.set({
                    path: "/lark",
                    description: config.description,
                    state: { account: result.value },
                    messages: [],
                  }),
                ),
                Match.exhaustive,
              ),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}
