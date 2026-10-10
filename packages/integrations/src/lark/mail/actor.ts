import { CurrentContextPath } from "@aster/core";
import { randomUUID } from "node:crypto";
import { Command, CommandProcessor, type ActorRef } from "@aster/actor";
import {
  ContextActor,
  DurableContext,
  RuntimeConfigurationError,
  childActorName,
  spawnContextChild,
} from "@aster/core";
import { Clock, DateTime, Effect, Match, Schema } from "effect";
import { LarkSettings } from "../settings.js";
import { LarkMailCommands, readEmail, readMailboxProfile } from "./queries.js";
import { LarkMailClient, LarkResponseError } from "./client.js";
import { LarkCliError, queryError } from "../errors.js";
import { LarkEmailActor, type EmailCommand } from "./email-actor.js";
import { EmailData, MailboxProfile, MailboxState, serializeMailbox } from "./model.js";
import { MailWindow, mailDayStart, mailWindow, pollMail } from "./polling.js";
import { emailPath } from "../paths.js";
const ProviderFailure = Schema.Union([
  Schema.instanceOf(LarkCliError),
  Schema.instanceOf(LarkResponseError),
]);
const Internal = Schema.TaggedUnion({
  ProfileLoaded: {
    generation: Schema.String,
    result: Schema.TaggedUnion({
      Success: { value: MailboxProfile },
      Failure: { error: ProviderFailure },
    }),
  },
  Poll: { generation: Schema.String },
  Polled: {
    generation: Schema.String,
    window: MailWindow,
    result: Schema.TaggedUnion({
      Success: { value: Schema.Array(EmailData) },
      Failure: { error: ProviderFailure },
    }),
  },
  Published: {
    generation: Schema.String,
    window: MailWindow,
    result: Schema.TaggedUnion({
      Success: { value: Schema.Void },
      Failure: { error: Schema.instanceOf(LarkResponseError) },
    }),
  },
});
export const LarkMailActor = ContextActor.define("lark/MailActor", {
  commands: LarkMailCommands,
  internal: Internal,
})(
  Effect.gen(function* () {
    const generation = randomUUID();
    const processor = yield* CommandProcessor.make({ concurrency: 2 });
    const registry = yield* DurableContext;
    const cli = yield* LarkMailClient;
    const settings = (yield* LarkSettings).mail;
    if (!settings)
      return yield* new RuntimeConfigurationError({ message: "Lark Mail is not configured" });
    const session = yield* registry
      .open({
        path: yield* CurrentContextPath,
        state: MailboxState,
        message: Schema.Never,
        serialize: serializeMailbox,
        initial: { description: settings.description, state: { mailbox: settings.mailbox } },
      })
      .pipe(Effect.orDie);
    yield* session.state
      .update((state) =>
        state.mailbox === settings.mailbox ? state : { mailbox: settings.mailbox },
      )
      .pipe(Effect.orDie);
    const sessionStart = mailDayStart(yield* Clock.currentTimeMillis);
    let busy = false;
    const profile = cli.getMailboxProfile(settings.mailbox);
    return {
      started: (context) =>
        Effect.gen(function* () {
          yield* context.pipeToSelf(profile, (result) => ({
            _tag: "ProfileLoaded",
            generation,
            result,
          }));
          yield* context.self.tell({ _tag: "Poll", generation });
        }),
      receive: (command, context) => {
        if ("generation" in command && command.generation !== generation) return Effect.void;
        return Match.value(command).pipe(
          Match.tag("profile", (request) => Command.reply(request.replyTo, readMailboxProfile())),
          Match.tag("list", (request) =>
            processor.submit(
              request,
              context,
              cli.listMessages(settings.mailbox, request).pipe(
                Effect.map((page) => ({
                  ...page,
                  coverage: { source: "provider" as const, complete: !page.hasMore },
                })),
                Effect.mapError(queryError),
              ),
            ),
          ),
          Match.tag("read", (request) =>
            processor.submit(request, context, readEmail(settings.mailbox, request.messageId)),
          ),
          Match.tag("ProfileLoaded", ({ result }) =>
            Match.value(result).pipe(
              Match.tag("Success", ({ value }) =>
                session.state
                  .update((state) => ({ ...state, profile: value }), {
                    description: settings.description,
                  })
                  .pipe(Effect.asVoid, Effect.orDie),
              ),
              Match.tag("Failure", ({ error }) =>
                Effect.logWarning(error.message).pipe(
                  Effect.andThen(
                    context.pipeToSelf(
                      Effect.sleep("30 seconds").pipe(Effect.andThen(profile)),
                      (result) => ({ _tag: "ProfileLoaded", generation, result }),
                    ),
                  ),
                ),
              ),
              Match.exhaustive,
            ),
          ),
          Match.tag("Poll", () =>
            Effect.gen(function* () {
              if (busy) return;
              busy = true;
              const cursor = (yield* session.state.get.pipe(Effect.orDie)).through;
              const window = mailWindow(
                cursor ? Date.parse(cursor) : undefined,
                yield* Clock.currentTimeMillis,
                sessionStart,
              );
              yield* context.pipeToSelf(
                pollMail(
                  cli,
                  settings.mailbox,
                  window,
                  (id) => registry.get(emailPath(settings.mailbox, id)) !== undefined,
                ),
                (result) => ({ _tag: "Polled", generation, window, result }),
              );
            }),
          ),
          Match.tag("Polled", ({ result, window }) =>
            Match.value(result).pipe(
              Match.tag("Failure", ({ error }) =>
                Effect.gen(function* () {
                  busy = false;
                  yield* Effect.logWarning(error.message);
                  yield* context.pipeToSelf(Effect.sleep(settings.pollIntervalMs), () => ({
                    _tag: "Poll",
                    generation,
                  }));
                }),
              ),
              Match.tag("Success", ({ value }) =>
                Effect.gen(function* () {
                  const deliveries = [];
                  for (const email of value) {
                    const relative = `${email.mailbox}/${email.messageId}`;
                    // This channel exclusively owns these child protocols. A stopping ref fails its ask;
                    // no cursor is advanced, and the next polling interval reacquires the child.
                    const existing = yield* context.child(childActorName(relative));
                    const ref =
                      (existing as ActorRef<EmailCommand> | undefined) ??
                      (yield* spawnContextChild(context, relative, LarkEmailActor).pipe(
                        Effect.orDie,
                      ));
                    deliveries.push(
                      ref.ask<void>((replyTo) => ({ _tag: "SetEmail", email, replyTo })),
                    );
                  }
                  yield* context.pipeToSelf(
                    Effect.all(deliveries, { discard: true }).pipe(
                      Effect.mapError((cause) => new LarkResponseError({ cause })),
                    ),
                    (result) => ({ _tag: "Published", generation, window, result }),
                  );
                }),
              ),
              Match.exhaustive,
            ),
          ),
          Match.tag("Published", ({ window, result }) =>
            Effect.gen(function* () {
              busy = false;
              yield* Match.value(result).pipe(
                Match.tag("Failure", ({ error }) => Effect.logWarning(error.message)),
                Match.tag("Success", () =>
                  session.state
                    .update(
                      (state) => ({
                        ...state,
                        through: DateTime.formatIso(DateTime.makeUnsafe(window.through)),
                      }),
                      { mode: "bootstrap" },
                    )
                    .pipe(Effect.asVoid, Effect.orDie),
                ),
                Match.exhaustive,
              );
              if (result._tag === "Success" && !window.caughtUp)
                return yield* context.self.tell({ _tag: "Poll", generation });
              yield* context.pipeToSelf(Effect.sleep(settings.pollIntervalMs), () => ({
                _tag: "Poll",
                generation,
              }));
            }),
          ),
          Match.exhaustive,
        );
      },
    };
  }),
);
