import { randomUUID } from "node:crypto";
import { type ActorRef, ReplyTo } from "@aster/actor";
import {
  ContextQueries,
  childActorName,
  contextPath,
  ContextActor,
  ContextRegistry,
  defineContext,
  spawnContextChild,
} from "@aster/core";
import { Context, DateTime, Effect, Layer, Match, Schema, Scope, Semaphore } from "effect";
import { MailFetcher } from "./client.js";
import { MailSettings } from "./config.js";
import { MailFetchError, mailFailureDetails, type MailFailureDetails } from "./errors.js";
import { MailboxState } from "./state/model.js";
import { MailboxSnapshot } from "./state/snapshot.js";
import { makeMailboxQuery, mailboxQueryDefinition } from "./queries.js";
import { mailDay } from "./dates.js";
import { MailBatch, MailboxWindow, type Mailbox } from "./model.js";
import { MailMessage } from "./model.js";
import {
  MailRootState,
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
    view: mailMessageView,
    state: MailMessage,
    message: Schema.Never,
    changes: "durable-state",
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
                  {
                    path,
                    description: `Email from ${email.from}: ${email.subject}`,
                    state: email,
                    messages: [],
                  },
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
  Schema.TaggedStruct("Rotate", {}),
  Schema.TaggedStruct("Baseline", {
    generation: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.Array(Schema.String) }),
      Schema.TaggedStruct("Failure", { error: MailFetchError }),
    ]),
  }),
  Schema.TaggedStruct("Polled", {
    generation: Schema.String,
    window: MailboxWindow,
    caughtUp: Schema.Boolean,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: MailBatch }),
      Schema.TaggedStruct("Failure", { error: MailFetchError }),
    ]),
  }),
  Schema.TaggedStruct("Published", {
    generation: Schema.String,
    window: MailboxWindow,
    caughtUp: Schema.Boolean,
    batch: MailBatch,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.Void }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(Error) }),
    ]),
  }),
]);

class MailboxActor extends ContextActor.Service<
  MailboxActor,
  MailSettings | MailFetcher | ContextQueries
>()("mail/MailboxActor", {
  command: MailboxCommand,
  context: defineContext({ view: mailboxView, state: MailboxSnapshot, message: Schema.Never }),
}) {
  static readonly layer = Layer.effect(
    MailboxActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const settings = yield* MailSettings;
      const fetcher = yield* MailFetcher;
      const queries = yield* ContextQueries;
      const scope = yield* Scope.Scope;
      // POP3 commonly allows only one authenticated session per mailbox.
      const mailboxPermit = yield* Semaphore.make(1);
      const generation = randomUUID();
      let state: MailboxState["Service"];
      let mailbox: Mailbox;
      let busy = false;
      let failures = 0;
      let synced = false;
      const interval = settings.pollIntervalMs ?? 30_000;
      const failed = Effect.fnUntraced(function* (details: MailFailureDetails) {
        busy = false;
        failures++;
        yield* state.failSync(details);
        yield* Effect.logWarning({
          event: "mail.poll.failed",
          path: mailboxPath(mailbox.id),
          ...details,
          attempt: failures,
          retryInMs: interval,
        });
      });
      return MailboxActor.of({
        started: (actor) =>
          Effect.gen(function* () {
            const path = contextPath(actor);
            mailbox = settings.mailboxes.find((entry) => mailboxPath(entry.id) === path)!;
            state = Context.get(
              yield* Layer.buildWithScope(MailboxState.layer(path, mailbox), scope),
              MailboxState,
            );
            const query = yield* makeMailboxQuery(mailbox);
            yield* queries
              .register(path, mailboxQueryDefinition, (input) =>
                mailboxPermit.withPermit(query(input)),
              )
              .pipe(Effect.provideService(Scope.Scope, scope), Effect.orDie);
            yield* Effect.logInfo({ event: "mail.poll.started", path, pollIntervalMs: interval });
            yield* actor.self.tell({ _tag: "Rotate" });
            yield* actor.self.tell({ _tag: "Poll" });
          }),
        receive: (command, actor) =>
          Effect.gen(function* () {
            if ("generation" in command && command.generation !== generation) return;
            yield* Match.value(command).pipe(
              Match.tag("Rotate", () =>
                Effect.gen(function* () {
                  const now = yield* DateTime.now;
                  yield* state.rollDay(now);
                  const next = DateTime.add(mailDay(now, mailbox.timeZone ?? "Asia/Shanghai"), {
                    days: 1,
                  });
                  yield* actor.pipeToSelf(
                    Effect.sleep(
                      Math.max(1, DateTime.toEpochMillis(next) - DateTime.toEpochMillis(now)),
                    ),
                    () => ({ _tag: "Rotate" }),
                  );
                  // Polling has its own single timer chain; rollover only changes the index.
                }),
              ),
              Match.tag("Poll", () =>
                Effect.gen(function* () {
                  if (busy) return;
                  busy = true;
                  const saved = yield* state.snapshot;
                  if (saved.known === undefined) {
                    yield* actor.pipeToSelf(
                      mailboxPermit.withPermit(fetcher.inventory(mailbox)).pipe(
                        Effect.flatMap(
                          Schema.decodeUnknownEffect(Schema.Array(Schema.NonEmptyString)),
                        ),
                        Effect.catchTag("SchemaError", (cause) =>
                          Effect.fail(
                            new MailFetchError({
                              mailbox: mailbox.id,
                              message: "Invalid mailbox inventory",
                              cause,
                              details: mailFailureDetails(cause, "validate"),
                            }),
                          ),
                        ),
                      ),
                      (result) => ({ _tag: "Baseline", generation, result }),
                    );
                    return;
                  }
                  const now = yield* DateTime.now;
                  const window = yield* state.nextWindow(now);
                  const caughtUp = Date.parse(window.through) >= DateTime.toEpochMillis(now);
                  const pull = mailboxPermit
                    .withPermit(fetcher.pull(mailbox, window, saved.known))
                    .pipe(
                      Effect.flatMap(Schema.decodeUnknownEffect(MailBatch)),
                      Effect.flatMap((batch) =>
                        batch.messages.every(
                          (email) =>
                            email.mailbox === mailbox.id &&
                            email.id.length > 0 &&
                            batch.ids.includes(email.id),
                        )
                          ? Effect.succeed(batch)
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
                  yield* actor.pipeToSelf(pull, (result) => ({
                    _tag: "Polled",
                    generation,
                    window,
                    caughtUp,
                    result,
                  }));
                }),
              ),
              Match.tag("Baseline", ({ result }) =>
                Effect.gen(function* () {
                  busy = false;
                  if (result._tag === "Failure") {
                    yield* failed(
                      result.error.details ?? mailFailureDetails(result.error.cause, "fetch"),
                    );
                    yield* actor.pipeToSelf(Effect.sleep(interval), () => ({ _tag: "Poll" }));
                  } else {
                    yield* state.baseline(result.value);
                    yield* actor.self.tell({ _tag: "Poll" });
                  }
                }),
              ),
              Match.tag("Polled", ({ result, window, caughtUp }) =>
                Effect.gen(function* () {
                  if (result._tag === "Failure") {
                    yield* failed(
                      result.error.details ?? mailFailureDetails(result.error.cause, "fetch"),
                    );
                    yield* actor.pipeToSelf(Effect.sleep(interval), () => ({ _tag: "Poll" }));
                    return;
                  }
                  const batch = result.value;
                  const deliveries = [];
                  for (const email of batch.messages) {
                    if (registry.get(mailMessagePath(email))) continue;
                    const name = mailSegment(email.id);
                    const existing = yield* actor.child(childActorName(name));
                    const ref =
                      (existing as ActorRef<SetEmail> | undefined) ??
                      (yield* spawnContextChild(actor, name, MailMessageActor).pipe(Effect.orDie));
                    deliveries.push(
                      ref.ask<void>((replyTo) => ({ _tag: "SetEmail", email, replyTo })),
                    );
                  }
                  yield* actor.pipeToSelf(
                    Effect.all(deliveries, { concurrency: 4, discard: true }),
                    (result) => ({
                      _tag: "Published",
                      generation,
                      window,
                      caughtUp,
                      batch,
                      result,
                    }),
                  );
                }),
              ),
              Match.tag("Published", ({ result, batch, window, caughtUp }) =>
                Effect.gen(function* () {
                  busy = false;
                  if (result._tag === "Failure")
                    yield* failed(mailFailureDetails(result.error, "publish"));
                  else {
                    yield* state.rollDay(yield* DateTime.now);
                    yield* state.completeSync(window, batch, caughtUp);
                    const log = {
                      event: failures ? "mail.poll.recovered" : "mail.poll.completed",
                      path: contextPath(actor),
                      retrieved: batch.messages.length,
                      previousFailures: failures,
                      through: window.through,
                    };
                    yield* failures || !synced ? Effect.logInfo(log) : Effect.logDebug(log);
                    failures = 0;
                    synced = true;
                  }
                  if (result._tag === "Success" && !caughtUp)
                    yield* actor.self.tell({ _tag: "Poll" });
                  else yield* actor.pipeToSelf(Effect.sleep(interval), () => ({ _tag: "Poll" }));
                }),
              ),
              Match.exhaustive,
            );
          }),
      });
    }),
  );
}

export class MailRootActor extends ContextActor.Service<
  MailRootActor,
  MailSettings | MailFetcher | ContextQueries
>()("mail/RootActor", {
  command: Schema.Never,
  context: defineContext({
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
