import { Effect, Match, Layer, Schema } from "effect";
import { type ActorRef } from "@aster/actor";
import {
  ContextActor,
  childActorName,
  ContextRegistry,
  defineContext,
  spawnContextChild,
} from "@aster/core";
import { LarkConfig } from "../config.js";
import { profileCapture } from "../shared/profile.js";
import { EmailData, MailboxProfile } from "./model.js";
import { LarkMailCli } from "./client.js";
import { LarkMailMessageActor, type MailMessageCommand } from "./message-actor.js";
const EmailChannelCommand = Schema.Union([
  Schema.TaggedStruct("ProfileLoaded", {
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: MailboxProfile }),
      Schema.TaggedStruct("Failure", {
        error: Schema.instanceOf(Error),
      }),
    ]),
  }),
  Schema.TaggedStruct("Poll", {}),
  Schema.TaggedStruct("Listed", {
    result: Schema.Union([
      Schema.TaggedStruct("Success", {
        value: Schema.Array(Schema.String),
      }),
      Schema.TaggedStruct("Failure", {
        error: Schema.instanceOf(Error),
      }),
    ]),
  }),
  Schema.TaggedStruct("Fetched", {
    ids: Schema.Array(Schema.String),
    result: Schema.Union([
      Schema.TaggedStruct("Success", {
        value: Schema.Array(EmailData),
      }),
      Schema.TaggedStruct("Failure", {
        error: Schema.instanceOf(Error),
      }),
    ]),
  }),
]);
type EmailChannelCommand = typeof EmailChannelCommand.Type;

export class LarkEmailChannelActor extends ContextActor.Service<
  LarkEmailChannelActor,
  LarkConfig | LarkMailCli
>()("lark/EmailChannelActor", {
  command: EmailChannelCommand,
  context: defineContext({
    identity: "Lark mailbox",
    state: Schema.Struct({
      mailbox: Schema.String,
      profile: Schema.optional(MailboxProfile),
    }),
    message: Schema.Never,
    capture: (record) =>
      record.state.profile === undefined
        ? undefined
        : {
            sessionId: profileCapture(record, record.state.profile),
            records: [record],
          },
  }),
}) {
  static readonly layer = Layer.effect(
    LarkEmailChannelActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const cli = yield* LarkMailCli;
      const config = yield* LarkConfig;
      const seen = new Set<string>();
      let initialized = false;
      const schedule = Effect.sleep(config.mail.pollIntervalMs);
      return LarkEmailChannelActor.of({
        started: (context) =>
          Effect.gen(function* () {
            yield* registry.set({
              path: "/lark/mail",
              description: config.mail.description,
              state: { mailbox: config.mail.mailbox },
              messages: [],
            });
            yield* context.pipeToSelf(cli.getMailboxProfile(config.mail.mailbox), (result) => ({
              _tag: "ProfileLoaded",
              result,
            }));
            yield* context.self.tell({ _tag: "Poll" });
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("ProfileLoaded", (command) =>
              Effect.gen(function* () {
                yield* Match.value(command.result).pipe(
                  Match.tag("Failure", (result) => Effect.logWarning(result.error.message)),
                  Match.tag("Success", (result) =>
                    registry.set({
                      path: "/lark/mail",
                      description: config.mail.description,
                      state: {
                        mailbox: config.mail.mailbox,
                        profile: result.value,
                      },
                      messages: [],
                    }),
                  ),
                  Match.exhaustive,
                );
              }),
            ),
            Match.tag("Poll", () =>
              Effect.gen(function* () {
                yield* context.pipeToSelf(cli.listRecentIds(config.mail.mailbox), (result) => ({
                  _tag: "Listed",
                  result,
                }));
              }),
            ),
            Match.tag("Listed", (command) =>
              Effect.gen(function* () {
                yield* Match.value(command.result).pipe(
                  Match.tag("Failure", (result) =>
                    Effect.gen(function* () {
                      yield* Effect.logWarning(result.error.message);
                      yield* context.pipeToSelf(schedule, () => ({
                        _tag: "Poll",
                      }));
                    }),
                  ),
                  Match.tag("Success", (result) =>
                    Effect.gen(function* () {
                      const ids = result.value;
                      if (!initialized) {
                        for (const id of ids) seen.add(id);
                        initialized = true;
                        yield* context.pipeToSelf(schedule, () => ({
                          _tag: "Poll",
                        }));
                        return;
                      }
                      const newIds = ids.filter((id) => !seen.has(id));
                      if (newIds.length === 0) {
                        yield* context.pipeToSelf(schedule, () => ({
                          _tag: "Poll",
                        }));
                        return;
                      }
                      yield* context.pipeToSelf(
                        cli.getMessages(config.mail.mailbox, newIds),
                        (result) => ({
                          _tag: "Fetched",
                          ids: newIds,
                          result,
                        }),
                      );
                    }),
                  ),
                  Match.exhaustive,
                );
              }),
            ),
            Match.tag("Fetched", (command) =>
              Effect.gen(function* () {
                yield* Match.value(command.result).pipe(
                  Match.tag("Failure", (result) => Effect.logWarning(result.error.message)),
                  Match.tag("Success", (result) =>
                    Effect.gen(function* () {
                      for (const email of result.value) {
                        const relative = `${email.mailbox}/${email.messageId}`;
                        const name = childActorName(relative);
                        const existing = yield* context.child(name);
                        const ref =
                          (existing as ActorRef<MailMessageCommand> | undefined) ??
                          (yield* spawnContextChild(context, relative, LarkMailMessageActor).pipe(
                            Effect.orDie,
                          ));
                        yield* ref.tell({ _tag: "SetEmail", email });
                      }
                      for (const id of command.ids) seen.add(id);
                    }),
                  ),
                  Match.exhaustive,
                );
                yield* context.pipeToSelf(schedule, () => ({ _tag: "Poll" }));
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}
