import { Command } from "@aster/actor";
import { Effect, Match, Schema } from "effect";
import { accountView } from "../public-views.js";
import { LarkAccountCommands } from "./queries.js";

import { ContextActor, ContextQueryError, ContextSession } from "@aster/core";
import { LarkConfig } from "../config.js";
import { LarkImActor } from "../im/channel-actor.js";
import { LarkEmailChannelActor } from "../mail/channel-actor.js";
import { LarkAccountCli } from "./client.js";
import { AccountProfile } from "./model.js";
const LarkRootCommand = Schema.TaggedStruct("AccountLoaded", {
  result: Schema.TaggedUnion({
    Success: { value: AccountProfile },
    Failure: {
      error: Schema.instanceOf(Error),
    },
  }),
});
type LarkRootCommand = typeof LarkRootCommand.Type;

export const LarkRootActor = ContextActor.define("lark/RootActor", {
  commands: LarkAccountCommands,
  internal: LarkRootCommand,
})(
  Effect.gen(function* () {
    const config = yield* LarkConfig;
    const cli = yield* LarkAccountCli;
    const session = yield* ContextSession.make({
      path: "/lark",
      state: Schema.Struct({ account: Schema.optional(AccountProfile) }),
      message: Schema.Never,
      view: accountView,
      initial: { description: config.description, state: {} },
    }).pipe(Effect.orDie);
    return {
      started: (context) =>
        Effect.gen(function* () {
          yield* context.pipeToSelf(cli.getAccount(), (result) => ({
            _tag: "AccountLoaded",
            result,
          }));
          if ((yield* context.child("mail")) === undefined) {
            yield* (yield* context.spawn("mail", LarkEmailChannelActor)).awaitStarted;
          }
          if (config.im !== undefined && (yield* context.child("im")) === undefined)
            yield* (yield* context.spawn("im", LarkImActor)).awaitStarted;
        }),
      receive: (command) =>
        Match.value(command).pipe(
          Match.tag("profile", (request) =>
            Command.reply(
              request.replyTo,
              Effect.gen(function* () {
                const state = yield* session.state.get.pipe(Effect.orDie);
                if (!state.account)
                  return yield* new ContextQueryError({
                    kind: "unavailable",
                    message: "Account profile is not available",
                  });
                return state.account;
              }),
            ),
          ),
          Match.tag("AccountLoaded", ({ result }) =>
            Match.value(result).pipe(
              Match.tag("Failure", (result) => Effect.logWarning(result.error.message)),
              Match.tag("Success", (result) =>
                session
                  .set(
                    {
                      description: config.description,
                      state: { account: result.value },
                      messages: [],
                    },
                    {},
                  )
                  .pipe(Effect.asVoid, Effect.orDie),
              ),
              Match.exhaustive,
            ),
          ),
          Match.exhaustive,
        ),
    };
  }),
);
