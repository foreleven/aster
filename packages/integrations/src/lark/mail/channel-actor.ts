import { type ActorRef } from "@aster/actor";
import {
  ContextActor,
  ContextRegistry,
  childActorName,
  defineContext,
  spawnContextChild,
} from "@aster/core";
import { Clock, Effect, Match, Schema } from "effect";
import { LarkConfig } from "../config.js";
import { mailChannelView } from "../public-views.js";
import { LarkMailCommands, makeLarkMailQuery } from "../queries.js";
import { LarkMailCli, LarkResponseError } from "./client.js";
import { LarkMailMessageActor, type MailMessageCommand } from "./message-actor.js";
import { EmailData, MailboxProfile } from "./model.js";
import { MailWindow, mailDayStart, mailWindow } from "./window.js";
const EmailChannelCommand = Schema.TaggedUnion({
  ProfileLoaded: {
    result: Schema.TaggedUnion({
      Success: { value: MailboxProfile },
      Failure: {
        error: Schema.instanceOf(Error),
      },
    }),
  },
  Poll: {},
  Polled: {
    window: MailWindow,
    result: Schema.TaggedUnion({
      Success: { value: Schema.Array(EmailData) },
      Failure: { error: Schema.instanceOf(Error) },
    }),
  },
  Published: {
    window: MailWindow,
    result: Schema.TaggedUnion({
      Success: { value: Schema.Void },
      Failure: { error: Schema.instanceOf(Error) },
    }),
  },
});
type EmailChannelCommand = typeof EmailChannelCommand.Type;

const MailboxState = Schema.Struct({
  mailbox: Schema.String,
  profile: Schema.optional(MailboxProfile),
});

export const LarkEmailChannelActor = ContextActor.define("lark/EmailChannelActor", {
  commands: LarkMailCommands,
  internal: EmailChannelCommand,
  context: defineContext({
    view: mailChannelView,
    state: MailboxState,
    message: Schema.Never,
  }),
})(
  Effect.gen(function* () {
    const query = yield* makeLarkMailQuery;
    const registry = yield* ContextRegistry;
    const cli = yield* LarkMailCli;
    const config = yield* LarkConfig;
    const sessionStart = mailDayStart(yield* Clock.currentTimeMillis);
    let cursor: number | undefined;
    let busy = false;
    const poll = Effect.fn("LarkMail.poll")(function* (window: MailWindow) {
      if (window.start >= window.through) return [];
      const ids = yield* cli.listIds(config.mail.mailbox, window.start, window.through);
      // Persisted email Contexts deduplicate both overlapping windows and today's restart replay.
      const unseen = ids.filter((id) => !registry.get(`/lark/mail/${config.mail.mailbox}/${id}`));
      if (unseen.length === 0) return [];
      const emails = yield* cli.getMessages(config.mail.mailbox, unseen);
      const received = new Set(emails.map((email) => email.messageId));
      if (
        unseen.some((id) => !received.has(id)) ||
        emails.some(
          (email) => email.mailbox !== config.mail.mailbox || !unseen.includes(email.messageId),
        )
      )
        return yield* new LarkResponseError({ cause: "Incomplete or unexpected mail messages" });
      return emails;
    });
    const schedule = Effect.sleep(config.mail.pollIntervalMs);
    return {
      query,
      started: (context) =>
        Effect.gen(function* () {
          const previous = registry.get("/lark/mail");
          const restored = previous && Schema.decodeUnknownSync(MailboxState)(previous.state);
          yield* registry
            .commit(
              {
                path: "/lark/mail",
                description: config.mail.description,
                state: {
                  ...(restored?.mailbox === config.mail.mailbox ? restored : {}),
                  mailbox: config.mail.mailbox,
                },
                messages: [],
              },
              { expectedRevision: previous?.revision ?? 0 },
            )
            .pipe(Effect.asVoid, Effect.orDie);
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
                  registry
                    .commit(
                      {
                        path: "/lark/mail",
                        description: config.mail.description,
                        state: {
                          mailbox: config.mail.mailbox,
                          profile: result.value,
                        },
                        messages: [],
                      },
                      { expectedRevision: registry.get("/lark/mail")?.revision ?? 0 },
                    )
                    .pipe(Effect.asVoid, Effect.orDie),
                ),
                Match.exhaustive,
              );
            }),
          ),
          Match.tag("Poll", () =>
            Effect.gen(function* () {
              if (busy) return;
              busy = true;
              const window = mailWindow(cursor, yield* Clock.currentTimeMillis, sessionStart);
              yield* context.pipeToSelf(poll(window), (result) => ({
                _tag: "Polled",
                window,
                result,
              }));
            }),
          ),
          Match.tag("Polled", (command) =>
            Match.value(command.result).pipe(
              Match.tag("Failure", (result) =>
                Effect.gen(function* () {
                  busy = false;
                  yield* Effect.logWarning(result.error.message);
                  yield* context.pipeToSelf(schedule, () => ({ _tag: "Poll" }));
                }),
              ),
              Match.tag("Success", (result) =>
                Effect.gen(function* () {
                  const deliveries = [];
                  for (const email of result.value) {
                    const relative = `${email.mailbox}/${email.messageId}`;
                    const existing = yield* context.child(childActorName(relative));
                    const ref =
                      (existing as ActorRef<MailMessageCommand> | undefined) ??
                      (yield* spawnContextChild(context, relative, LarkMailMessageActor).pipe(
                        Effect.orDie,
                      ));
                    deliveries.push(
                      ref.ask<void>((replyTo) => ({ _tag: "SetEmail", email, replyTo })),
                    );
                  }
                  // tell() only enqueues. Advance only after each child acknowledges its durable commit.
                  yield* context.pipeToSelf(
                    Effect.all(deliveries, { discard: true }),
                    (result) => ({
                      _tag: "Published",
                      window: command.window,
                      result,
                    }),
                  );
                }),
              ),
              Match.exhaustive,
            ),
          ),
          Match.tag("Published", (command) =>
            Effect.gen(function* () {
              busy = false;
              yield* Match.value(command.result).pipe(
                Match.tag("Failure", (result) => Effect.logWarning(result.error.message)),
                Match.tag("Success", () =>
                  Effect.sync(() => {
                    cursor = command.window.through;
                  }),
                ),
                Match.exhaustive,
              );
              if (command.result._tag === "Success" && !command.window.caughtUp) {
                yield* context.self.tell({ _tag: "Poll" });
                return;
              }
              yield* context.pipeToSelf(schedule, () => ({ _tag: "Poll" }));
            }),
          ),
          Match.exhaustive,
        ),
    };
  }),
);
