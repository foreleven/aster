import { Effect, Match, Schema } from "effect";
import { accountView } from "../public-views.js";
import { LarkAccountCommands, makeLarkAccountQuery } from "../queries.js";

import { ContextActor, ContextRegistry, defineContext } from "@aster/core";
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
  context: defineContext({
    view: accountView,
    state: Schema.Struct({ account: Schema.optional(AccountProfile) }),
    message: Schema.Never,
  }),
})(
  Effect.gen(function* () {
    const query = yield* makeLarkAccountQuery;
    const registry = yield* ContextRegistry;
    const config = yield* LarkConfig;
    const cli = yield* LarkAccountCli;
    return {
      query,
      started: (context) =>
        Effect.gen(function* () {
          const previous = registry.get("/lark");
          yield* registry
            .commit(
              {
                path: "/lark",
                description: config.description,
                state: previous?.state ?? {},
                messages: [],
              },
              { expectedRevision: previous?.revision ?? 0 },
            )
            .pipe(Effect.asVoid, Effect.orDie);
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
          Match.tag("AccountLoaded", ({ result }) =>
            Match.value(result).pipe(
              Match.tag("Failure", (result) => Effect.logWarning(result.error.message)),
              Match.tag("Success", (result) =>
                registry
                  .commit(
                    {
                      path: "/lark",
                      description: config.description,
                      state: { account: result.value },
                      messages: [],
                    },
                    { expectedRevision: registry.get("/lark")?.revision ?? 0 },
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
