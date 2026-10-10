import { randomUUID } from "node:crypto";
import { Command, type ActorContext } from "@aster/actor";
import { ContextActor, DurableContext } from "@aster/core";
import { Effect, Match, Schema } from "effect";
import { LarkAccountCommands, readAccountProfile } from "./queries.js";
import { LarkSettings } from "../settings.js";
import { LarkAccountClient } from "./client.js";
import { LarkAccountError } from "../errors.js";
import { AccountProfile, AccountState, serializeAccount } from "./model.js";
const Internal = Schema.TaggedStruct("AccountLoaded", {
  generation: Schema.String,
  result: Schema.TaggedUnion({
    Success: { value: AccountProfile },
    Failure: { error: Schema.instanceOf(LarkAccountError) },
  }),
});
export type AccountCommand = typeof Internal.Type | (typeof LarkAccountCommands)[number]["Type"];
/** Children arrive already bound to enabled capabilities; the account never acquires disabled branches. */
export const makeLarkRootActor = (
  startChildren: (context: ActorContext<AccountCommand>) => Effect.Effect<void, unknown>,
) =>
  ContextActor.define("lark/RootActor", { commands: LarkAccountCommands, internal: Internal })(
    Effect.gen(function* () {
      const generation = randomUUID();
      const config = yield* LarkSettings;
      const cli = yield* LarkAccountClient;
      const session = yield* (yield* DurableContext)
        .open({
          path: "/lark",
          state: AccountState,
          message: Schema.Never,
          serialize: serializeAccount,
          initial: { description: config.description, state: {} },
        })
        .pipe(Effect.orDie);
      const refresh = cli.getAccount();
      return {
        started: (context) =>
          Effect.gen(function* () {
            yield* context.pipeToSelf(refresh, (result) => ({
              _tag: "AccountLoaded",
              generation,
              result,
            }));
            yield* startChildren(context);
          }),
        receive: (command, context) => {
          if ("generation" in command && command.generation !== generation) return Effect.void;
          return Match.value(command).pipe(
            Match.tag("profile", (request) => Command.reply(request.replyTo, readAccountProfile())),
            Match.tag("AccountLoaded", ({ result }) =>
              Match.value(result).pipe(
                Match.tag("Success", ({ value }) =>
                  session.state
                    .set({ account: value }, { description: config.description })
                    .pipe(Effect.asVoid, Effect.orDie),
                ),
                Match.tag("Failure", ({ error }) =>
                  Effect.logWarning(error.message).pipe(
                    Effect.andThen(
                      context.pipeToSelf(
                        Effect.sleep("30 seconds").pipe(Effect.andThen(refresh)),
                        (result) => ({ _tag: "AccountLoaded", generation, result }),
                      ),
                    ),
                  ),
                ),
                Match.exhaustive,
              ),
            ),
            Match.exhaustive,
          );
        },
      };
    }),
  );
