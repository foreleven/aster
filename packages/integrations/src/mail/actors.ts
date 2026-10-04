import { type ActorRef, ReplyTo } from "@aster/actor";
import {
  childActorName,
  contextPath,
  ContextActor,
  ContextRegistry,
  defineContext,
  spawnContextChild,
} from "@aster/core";
import { DateTime, Effect, Layer, Match, Schema } from "effect";
import { MailFetcher } from "./client.js";
import { MailSettings } from "./config.js";
import {
  MailFetchError,
  mailFailureDetails,
  mailFailureSummary,
  type MailFailureDetails,
} from "./errors.js";
import { MailMessage } from "./model.js";
import {
  MailRootState,
  MailboxState,
  mailboxPath,
  mailMessagePath,
  mailSegment,
  mailRootView,
  mailboxView,
  mailMessageView,
} from "./contexts.js";

const SetEmail = Schema.TaggedStruct("SetEmail", { email: MailMessage, replyTo: ReplyTo<void>() });
type SetEmail = typeof SetEmail.Type;

class MailMessageActor extends ContextActor.Service<MailMessageActor>()("mail/MessageActor", {
  command: SetEmail,
  context: defineContext({
    identity: "An email in a connected mailbox",
    view: mailMessageView,
    state: MailMessage,
    message: Schema.Never,
    signalSource: true,
  }),
}) {
  static readonly layer = Layer.effect(
    MailMessageActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      return MailMessageActor.of({
        started: (context) => context.receiveTimeout("5 minutes"),
        receive: ({ email, replyTo }, context) =>
          Effect.gen(function* () {
            const path = contextPath(context);
            if (path !== mailMessagePath(email))
              return yield* Effect.die(new Error("Mail identity mismatch"));
            // The provider identity is immutable. Replay acknowledges the existing durable record.
            if (!registry.get(path)) {
              yield* registry
                .commit(
                  { path, description: "", state: email, messages: [] },
                  { expectedRevision: 0 },
                )
                .pipe(Effect.orDie);
            }
            yield* replyTo.tell(undefined);
          }),
      });
    }),
  );
}

const MailboxCommand = Schema.Union([
  Schema.TaggedStruct("Poll", {}),
  Schema.TaggedStruct("Polled", {
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.Array(MailMessage) }),
      Schema.TaggedStruct("Failure", { error: MailFetchError }),
    ]),
  }),
  Schema.TaggedStruct("Published", {
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.Void }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(Error) }),
    ]),
  }),
]);

class MailboxActor extends ContextActor.Service<MailboxActor, MailSettings | MailFetcher>()(
  "mail/MailboxActor",
  {
    command: MailboxCommand,
    context: defineContext({
      identity: "A connected mailbox",
      view: mailboxView,
      state: MailboxState,
      message: Schema.Never,
    }),
  },
) {
  static readonly layer = Layer.effect(
    MailboxActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const settings = yield* MailSettings;
      const fetcher = yield* MailFetcher;
      let busy = false;
      let failures = 0;
      let synced = false;
      let retrieved = 0;
      const retryInMs = settings.pollIntervalMs ?? 30_000;
      const save = Effect.fn("Mail.saveStatus")(function* (
        path: string,
        status: MailboxState["status"],
        lastError?: string,
        lastFailure?: MailFailureDetails,
      ) {
        const mailbox = settings.mailboxes.find((entry) => mailboxPath(entry.id) === path)!;
        const previous = registry.get(path);
        const restored = previous
          ? yield* Schema.decodeUnknownEffect(MailboxState)(previous.state).pipe(Effect.orDie)
          : undefined;
        const lastSyncedAt =
          status === "ready" ? DateTime.formatIso(yield* DateTime.now) : restored?.lastSyncedAt;
        yield* registry
          .commit(
            {
              path,
              description: `Mailbox ${mailbox.id}`,
              messages: [],
              state: {
                mailbox: mailbox.id,
                protocol: mailbox.protocol ?? "imap",
                folder: mailbox.folder ?? "INBOX",
                status,
                ...(lastSyncedAt === undefined ? {} : { lastSyncedAt }),
                ...(lastError === undefined ? {} : { lastError }),
                ...(lastFailure === undefined ? {} : { lastFailure }),
              },
            },
            { expectedRevision: previous?.revision ?? 0 },
          )
          .pipe(Effect.orDie);
      });
      const reportFailure = Effect.fn("Mail.reportFailure")(function* (
        path: string,
        details: MailFailureDetails,
      ) {
        failures++;
        const message = mailFailureSummary(details, retryInMs);
        yield* Effect.logWarning({
          event: "mail.poll.failed",
          path,
          ...details,
          attempt: failures,
          retryInMs,
        });
        yield* save(path, "error", message, details);
      });
      return MailboxActor.of({
        started: (context) =>
          Effect.gen(function* () {
            yield* save(contextPath(context), "starting");
            yield* Effect.logInfo({
              event: "mail.poll.started",
              path: contextPath(context),
              pollIntervalMs: retryInMs,
            });
            yield* context.self.tell({ _tag: "Poll" });
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("Poll", () =>
              Effect.gen(function* () {
                if (busy) return;
                busy = true;
                const mailbox = settings.mailboxes.find(
                  (entry) => mailboxPath(entry.id) === contextPath(context),
                )!;
                const pull = fetcher.pull(mailbox).pipe(
                  Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(MailMessage))),
                  Effect.flatMap((emails) =>
                    emails.every((email) => email.mailbox === mailbox.id && email.id.length > 0)
                      ? Effect.succeed(emails)
                      : Effect.fail(
                          new MailFetchError({
                            mailbox: mailbox.id,
                            message: "Unexpected mail identity",
                            cause: undefined,
                            details: mailFailureDetails(undefined, "validate"),
                          }),
                        ),
                  ),
                  Effect.catchTag("SchemaError", (cause) =>
                    Effect.fail(
                      new MailFetchError({
                        mailbox: mailbox.id,
                        message: "Invalid mail response",
                        cause,
                        details: mailFailureDetails(cause, "validate"),
                      }),
                    ),
                  ),
                );
                yield* context.pipeToSelf(pull, (result) => ({ _tag: "Polled", result }));
              }),
            ),
            Match.tag("Polled", ({ result }) =>
              Match.value(result).pipe(
                Match.tag("Failure", ({ error }) =>
                  Effect.gen(function* () {
                    busy = false;
                    yield* reportFailure(
                      contextPath(context),
                      error.details ?? mailFailureDetails(error.cause, "fetch"),
                    );
                    yield* context.pipeToSelf(Effect.sleep(retryInMs), () => ({ _tag: "Poll" }));
                  }),
                ),
                Match.tag("Success", ({ value }) =>
                  Effect.gen(function* () {
                    retrieved = value.length;
                    const deliveries = [];
                    for (const email of value) {
                      if (registry.get(mailMessagePath(email))) continue;
                      const relative = mailSegment(email.id);
                      const existing = yield* context.child(childActorName(relative));
                      const ref =
                        (existing as ActorRef<SetEmail> | undefined) ??
                        (yield* spawnContextChild(context, relative, MailMessageActor).pipe(
                          Effect.orDie,
                        ));
                      deliveries.push(
                        ref.ask<void>((replyTo) => ({ _tag: "SetEmail", email, replyTo })),
                      );
                    }
                    // Wait for child persistence acknowledgements before marking the poll complete.
                    yield* context.pipeToSelf(
                      Effect.all(deliveries, { discard: true }),
                      (result) => ({ _tag: "Published", result }),
                    );
                  }),
                ),
                Match.exhaustive,
              ),
            ),
            Match.tag("Published", ({ result }) =>
              Effect.gen(function* () {
                busy = false;
                yield* Match.value(result).pipe(
                  Match.tag("Success", () =>
                    Effect.gen(function* () {
                      yield* save(contextPath(context), "ready");
                      const log = {
                        event: failures > 0 ? "mail.poll.recovered" : "mail.poll.completed",
                        path: contextPath(context),
                        retrieved,
                        previousFailures: failures,
                      };
                      yield* failures > 0 || !synced ? Effect.logInfo(log) : Effect.logDebug(log);
                      failures = 0;
                      synced = true;
                    }),
                  ),
                  Match.tag("Failure", ({ error }) =>
                    reportFailure(contextPath(context), mailFailureDetails(error, "publish")),
                  ),
                  Match.exhaustive,
                );
                yield* context.pipeToSelf(Effect.sleep(retryInMs), () => ({
                  _tag: "Poll",
                }));
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}

export class MailRootActor extends ContextActor.Service<
  MailRootActor,
  MailSettings | MailFetcher
>()("mail/RootActor", {
  command: Schema.Never,
  context: defineContext({
    identity: "Connected mailboxes",
    view: mailRootView,
    state: MailRootState,
    message: Schema.Never,
  }),
}) {
  static readonly layer = Layer.effect(
    MailRootActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const settings = yield* MailSettings;
      return MailRootActor.of({
        started: (context) =>
          Effect.gen(function* () {
            yield* registry
              .commit(
                {
                  path: "/mail",
                  description: settings.description ?? "Connected mailboxes",
                  state: {
                    mailboxes: settings.mailboxes.map((mailbox) => mailboxPath(mailbox.id)),
                  },
                  messages: [],
                },
                { expectedRevision: registry.get("/mail")?.revision ?? 0 },
              )
              .pipe(Effect.orDie);
            for (const mailbox of settings.mailboxes) {
              const name = mailSegment(mailbox.id);
              if (!(yield* context.child(childActorName(name))))
                yield* spawnContextChild(context, name, MailboxActor);
            }
          }),
        receive: () => Effect.void,
      });
    }),
  );
}
