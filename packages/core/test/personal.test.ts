import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { PersonalMessage, PersonalState, type PersonalReceipt } from "@aster/api-contracts";
import { Deferred, Effect, Fiber, Layer, Schema, Stream } from "effect";
import {
  ContextRegistry,
  PersonalAgentActor,
  PersonalProcessor,
  PersonalActions,
  PersonalProcessingError,
  defineContext,
  contextView,
  makeApplicationApi,
  makeContextRegistry,
  type ContextRecord,
} from "../src/index.js";

const input = {
  requestId: "request-1",
  causationId: "user-1",
  expectedRevision: 1,
  text: "Monitor the release",
};

test("Personal processes one durable input at a time and commits only its own reply and cursor", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const secondEntered = yield* Deferred.make<void>();
        const secondRelease = yield* Deferred.make<void>();
        const seen: string[] = [];
        const processor = Layer.succeed(PersonalProcessor, {
          enabled: true,
          run: (message, reads) =>
            Effect.gen(function* () {
              const current = yield* reads.read("/personal").pipe(Effect.orDie);
              const run = Schema.decodeUnknownSync(PersonalState)(current.state).runs?.find(
                (run) => run.requestId === message.requestId,
              );
              assert.equal(run?.status, "running", "handoff must follow run persistence");
              assert.ok(
                (yield* reads.list.pipe(Effect.orDie)).some(
                  (record) => record.path === "/personal",
                ),
              );
              seen.push(message.requestId);
              if (message.requestId === input.requestId) {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
              } else {
                yield* Deferred.succeed(secondEntered, undefined);
                yield* Deferred.await(secondRelease);
              }
              return { text: `Answer: ${message.payload.text}` };
            }),
        });
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            PersonalActions.unavailable,
            Layer.succeed(ContextRegistry, registry),
            processor,
          ),
        );
        const personal = yield* system.spawn("personal", PersonalAgentActor);
        const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
        const firstReceipt = yield* api.personal.sendMessage(input);
        yield* Deferred.await(entered);
        const current = yield* api.personal.get;
        const secondReceipt = yield* api.personal.sendMessage({
          ...input,
          requestId: "second",
          text: "Another question",
          expectedRevision: current.revision!,
        });
        assert.equal(secondReceipt.sequence, 2);
        const changes = yield* registry.subscribe;
        yield* Deferred.succeed(release, undefined);
        yield* Deferred.await(secondEntered);
        const afterFirst = yield* api.personal.get;
        const firstState = Schema.decodeUnknownSync(PersonalState)(afterFirst.state);
        assert.equal(firstState.processedThrough, firstReceipt.sequence);
        assert.deepEqual(firstState.pendingRequestIds, ["second"]);
        assert.equal(afterFirst.messages.length, 3);
        assert.deepEqual(yield* api.personal.sendMessage(input), firstReceipt);
        yield* Deferred.succeed(secondRelease, undefined);
        yield* changes.pipe(
          Stream.filter(
            (change) =>
              change.path === "/personal" &&
              Schema.decodeUnknownSync(PersonalState)(change.record.state).processedThrough ===
                secondReceipt.sequence,
          ),
          Stream.take(1),
          Stream.runDrain,
        );
        const done = yield* api.personal.get;
        assert.equal(done.messages.length, 4);
        assert.deepEqual(seen, [input.requestId, "second"]);
        assert.deepEqual(Schema.decodeUnknownSync(PersonalState)(done.state).pendingRequestIds, []);
        const replies = Schema.decodeUnknownSync(Schema.Array(PersonalMessage))(
          done.messages,
        ).filter((message) => message.source === "/personal");
        assert.deepEqual(
          replies.map((message) => message.payload.text),
          [`Answer: ${input.text}`, "Answer: Another question"],
        );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Personal commits a model reply and proposed Goal operations before starting delivery", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const records = new Map<string, ContextRecord>();
        const registry = yield* makeContextRegistry({
          loadAll: () => [],
          save: (record) => {
            records.set(record.path, structuredClone(record));
          },
        });
        const sent = yield* Deferred.make<void>();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(PersonalProcessor, {
              enabled: true,
              run: () =>
                Effect.succeed({
                  text: "Queued a Goal message",
                  goalMessages: [
                    { goalSlug: "project", goalRevision: 7, text: "Investigate the blocker" },
                  ],
                }),
            }),
            Layer.succeed(PersonalActions, {
              executors: Effect.succeed([]),
              applySignal: () => Effect.die(new Error("Unexpected Signal command")),
              resumeRun: () => Effect.die(new Error("Unexpected Run command")),
              startTask: () => Effect.die(new Error("Unexpected Task command")),
              requestApproval: () => Effect.die(new Error("Unexpected approval request")),
              respondApproval: () => Effect.die(new Error("Unexpected Approval command")),
              bind: () => Effect.succeed(true),
              sendGoalMessage: (delivery) =>
                Effect.gen(function* () {
                  const persisted = records.get("/personal")!;
                  const state = Schema.decodeUnknownSync(PersonalState)(persisted.state);
                  const messages = Schema.decodeUnknownSync(Schema.Array(PersonalMessage))(
                    persisted.messages,
                  );
                  assert.equal(state.runs?.[0]?.status, "completed");
                  assert.equal(messages[1]?.payload.text, "Queued a Goal message");
                  assert.equal(state.outbox?.[0]?.input.requestId, delivery.requestId);
                  assert.equal(delivery.causationId, input.causationId);
                  assert.equal(delivery.target, "/goals/project");
                  assert.equal(delivery.expectedRevision, 7);
                  yield* Deferred.succeed(sent, undefined);
                  return { requestId: delivery.requestId, revision: 8 };
                }),
            }),
          ),
        );
        const personal = yield* system.spawn("personal", PersonalAgentActor);
        const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
        const changes = yield* registry.subscribe;
        yield* api.personal.sendMessage(input);
        yield* Deferred.await(sent);
        yield* changes.pipe(
          Stream.filter(
            (change) =>
              Schema.decodeUnknownSync(PersonalState)(change.record.state).outbox?.[0]?.status ===
              "delivered",
          ),
          Stream.take(1),
          Stream.runDrain,
        );
        assert.equal(
          Schema.decodeUnknownSync(PersonalState)((yield* api.personal.get).state).outbox?.[0]
            ?.receipt?.revision,
          8,
        );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Personal processing failure is visible and is not resubmitted by duplicate acceptance or restart", async () => {
  const records = new Map<string, ContextRecord>();
  let calls = 0;
  for (let restart = 0; restart < 2; restart++) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry({
            loadAll: () => [...records.values()],
            save: (record) => {
              records.set(record.path, structuredClone(record));
            },
          });
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              PersonalActions.unavailable,
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(PersonalProcessor, {
                enabled: true,
                run: () =>
                  Effect.suspend(() => {
                    calls++;
                    return Effect.fail(
                      new PersonalProcessingError({ message: "Provider unavailable" }),
                    );
                  }),
              }),
            ),
          );
          const personal = yield* system.spawn("personal", PersonalAgentActor);
          const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
          const changes = yield* registry.subscribe;
          yield* api.personal.sendMessage(input);
          if (restart === 0)
            yield* changes.pipe(
              Stream.filter(
                (change) =>
                  Schema.decodeUnknownSync(PersonalState)(change.record.state).runs?.[0]?.status ===
                  "failed",
              ),
              Stream.take(1),
              Stream.runDrain,
            );
          const snapshot = yield* api.personal.get;
          const state = Schema.decodeUnknownSync(PersonalState)(snapshot.state);
          assert.equal(state.runs?.[0]?.error, "Provider unavailable");
          assert.deepEqual(state.pendingRequestIds, [input.requestId]);
          assert.equal(snapshot.messages.length, 1);
          assert.equal(calls, 1);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
});

test("Personal recovers a persisted run after the model result cannot be applied", async () => {
  const records = new Map<string, ContextRecord>();
  const results = new Map<string, string>();
  let loseApply = true;
  let invocations = 0;
  let executions = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry({
          loadAll: () => [...records.values()],
          save: (record) => {
            if (record.messages.length === 2 && loseApply) {
              loseApply = false;
              throw new Error("Injected reply commit failure");
            }
            records.set(record.path, structuredClone(record));
          },
        });
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            PersonalActions.unavailable,
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(PersonalProcessor, {
              enabled: true,
              run: (message) =>
                Effect.sync(() => {
                  invocations++;
                  assert.equal(
                    Schema.decodeUnknownSync(PersonalState)(records.get("/personal")!.state)
                      .runs?.[0]?.status,
                    "running",
                  );
                  if (!results.has(message.requestId)) {
                    executions++;
                    results.set(message.requestId, "Saved model reply");
                  }
                  return { text: results.get(message.requestId)! };
                }),
            }),
          ),
        );
        const personal = yield* system.spawn("personal", PersonalAgentActor);
        const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
        const changes = yield* registry.subscribe;
        const receipt = yield* api.personal.sendMessage(input);
        yield* changes.pipe(
          Stream.filter((change) => change.record.messages.length === 2),
          Stream.take(1),
          Stream.runDrain,
        );
        assert.equal(executions, 1);
        assert.equal(invocations, 2);
        const done = yield* api.personal.get;
        assert.equal(done.messages.length, 2);
        assert.equal(
          Schema.decodeUnknownSync(PersonalState)(done.state).processedThrough,
          receipt.sequence,
        );
        assert.deepEqual(yield* api.personal.sendMessage(input), receipt);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("an explicit Personal retry has its own durable identity and a duplicate does not start another attempt", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const attempts: string[] = [];
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            PersonalActions.unavailable,
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(PersonalProcessor, {
              enabled: true,
              run: (_input, _reads, executionId) =>
                Effect.suspend(() => {
                  attempts.push(executionId!);
                  return attempts.length === 1
                    ? Effect.fail(new PersonalProcessingError({ message: "Provider failed" }))
                    : Effect.succeed({ text: "Recovered reply" });
                }),
            }),
          ),
        );
        const personal = yield* system.spawn("personal", PersonalAgentActor);
        const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
        const changes = yield* registry.subscribe;
        yield* api.personal.sendMessage(input);
        yield* changes.pipe(
          Stream.filter(
            (change) =>
              Schema.decodeUnknownSync(PersonalState)(change.record.state).runs?.[0]?.status ===
              "failed",
          ),
          Stream.take(1),
          Stream.runDrain,
        );
        const failed = yield* api.personal.get;
        const retry = {
          requestId: "retry-one",
          inputRequestId: input.requestId,
          expectedRevision: failed.revision!,
        };
        const receipt = yield* api.personal.retry(retry);
        assert.deepEqual(yield* api.personal.retry(retry), receipt);
        yield* changes.pipe(
          Stream.filter((change) => change.record.messages.length === 2),
          Stream.take(1),
          Stream.runDrain,
        );
        assert.deepEqual(attempts, ["input:1", "retry:retry-one"]);
        const completed = yield* api.personal.get;
        assert.deepEqual(
          Schema.decodeUnknownSync(PersonalState)(completed.state).runs?.map((run) => run.status),
          ["failed", "completed"],
        );
        assert.equal(completed.messages.length, 2);
        assert.deepEqual(yield* api.personal.retry(retry), receipt);
        assert.deepEqual(attempts, ["input:1", "retry:retry-one"]);
        const conflict = yield* api.personal
          .retry({ ...retry, inputRequestId: "other" })
          .pipe(Effect.result);
        assert.ok(conflict._tag === "Failure" && conflict.failure.kind === "conflict");
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Personal input and retry receipt survive a complete runtime restart without another message", async () => {
  const records = new Map<string, ContextRecord>();
  let receipt: PersonalReceipt | undefined;
  for (let start = 0; start < 2; start++) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry({
            loadAll: () => [...records.values()],
            save: (record) => {
              records.set(record.path, structuredClone(record));
            },
          });
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              PersonalActions.unavailable,
              Layer.succeed(ContextRegistry, registry),
              PersonalProcessor.disabled,
            ),
          );
          const personal = yield* system.spawn("personal", PersonalAgentActor);
          const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
          const accepted = yield* api.personal.sendMessage(input);
          assert.deepEqual(accepted, { requestId: input.requestId, revision: 2, sequence: 1 });
          if (receipt) assert.deepEqual(accepted, receipt);
          receipt = accepted;
          const record = yield* api.personal.get;
          assert.equal(record.revision, 2);
          assert.deepEqual(record, registry.project(records.get("/personal")!));
          assert.equal(record.messages.length, 1);
          const message = Schema.decodeUnknownSync(PersonalMessage)(record.messages[0]);
          assert.equal(message.payload.text, input.text);
          assert.equal(message.source, "user");
          assert.equal(message.target, "/personal");
          assert.equal(message.causationId, input.causationId);
          assert.deepEqual(
            Schema.decodeUnknownSync(PersonalState)(record.state).pendingRequestIds,
            [input.requestId],
          );
          const changed = yield* api.personal
            .sendMessage({ ...input, text: "Different request" })
            .pipe(Effect.result);
          assert.ok(changed._tag === "Failure" && changed.failure.kind === "conflict");
          assert.deepEqual(yield* api.personal.get, record);
        }),
      ),
    );
  }
});

test("Personal acknowledgement waits for the state and ordered message commit", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const gated = {
          ...registry,
          commit: (record: ContextRecord, options: Parameters<typeof registry.commit>[1]) =>
            Effect.gen(function* () {
              if (record.messages.length) {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
              }
              return yield* registry.commit(record, options);
            }),
        };
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            PersonalActions.unavailable,
            Layer.succeed(ContextRegistry, gated),
            PersonalProcessor.disabled,
          ),
        );
        const personal = yield* system.spawn("personal", PersonalAgentActor);
        const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
        const sending = yield* api.personal.sendMessage(input).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        assert.equal(sending.pollUnsafe(), undefined);
        assert.equal(registry.get("/personal")?.messages.length, 0);
        yield* Deferred.succeed(release, undefined);
        const receipt = yield* Fiber.join(sending);
        assert.equal(receipt.revision, registry.get("/personal")?.revision);
        assert.equal(registry.get("/personal")?.messages.length, 1);
      }),
    ),
  );
});

test("Personal mailbox serializes new inputs and rejects stale or invalid commands without mutation", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            PersonalActions.unavailable,
            Layer.succeed(ContextRegistry, registry),
            PersonalProcessor.disabled,
          ),
        );
        const personal = yield* system.spawn("personal", PersonalAgentActor);
        const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
        const outcomes = yield* Effect.all(
          [
            api.personal.sendMessage(input).pipe(Effect.result),
            api.personal.sendMessage({ ...input, requestId: "request-2" }).pipe(Effect.result),
          ],
          { concurrency: "unbounded" },
        );
        assert.equal(outcomes.filter((outcome) => outcome._tag === "Success").length, 1);
        const losing = outcomes.find((outcome) => outcome._tag === "Failure");
        assert.ok(losing?._tag === "Failure" && losing.failure.kind === "conflict");
        const before = yield* api.personal.get;
        const invalid = yield* api.personal
          .sendMessage({ ...input, requestId: "blank", expectedRevision: 2, text: "  " })
          .pipe(Effect.result);
        assert.ok(invalid._tag === "Failure" && invalid.failure.kind === "invalid-input");
        assert.deepEqual(yield* api.personal.get, before);
      }),
    ),
  );
});

test("Personal reads public snapshots by command and does not ingest ContextChange", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            PersonalActions.unavailable,
            Layer.succeed(ContextRegistry, registry),
            PersonalProcessor.disabled,
          ),
        );
        const personal = yield* system.spawn("personal", PersonalAgentActor);
        const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
        const before = yield* api.personal.get;
        yield* registry.register(
          "/source",
          defineContext({
            identity: "Public source",
            view: contextView({
              state: Schema.Struct({ value: Schema.Number }),
              message: Schema.String,
            }),
            state: Schema.Struct({ value: Schema.Number }),
            message: Schema.String,
          }),
        );
        yield* registry.commit(
          {
            path: "/source",
            description: "Source",
            state: { value: 1, privateField: "removed" },
            messages: ["one"],
          },
          { expectedRevision: 0 },
        );
        const source = yield* api.personal.readContext("/source");
        assert.deepEqual(source.state, { value: 1 });
        assert.equal(source.revision, 1);
        const listed = yield* api.personal.listContexts;
        assert.deepEqual(
          listed.find((record) => record.path === "/source"),
          source,
        );
        const missing = yield* api.personal.readContext("/missing").pipe(Effect.result);
        assert.ok(missing._tag === "Failure" && missing.failure.kind === "not-found");
        assert.deepEqual(yield* api.personal.get, before);
      }),
    ),
  );
});

test("a Personal commit with a lost acknowledgement is reconciled after owner restart", async () => {
  const records = new Map<string, ContextRecord>();
  let loseAcknowledgement = true;
  const store = {
    loadAll: () => [...records.values()],
    save: (record: ContextRecord) => {
      records.set(record.path, structuredClone(record));
      if (record.messages.length && loseAcknowledgement) {
        loseAcknowledgement = false;
        throw new Error("Commit persisted before storage acknowledgement failed");
      }
    },
  };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry(store);
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            PersonalActions.unavailable,
            Layer.succeed(ContextRegistry, registry),
            PersonalProcessor.disabled,
          ),
        );
        const personal = yield* system.spawn("personal", PersonalAgentActor);
        const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
        yield* api.personal.get;
        const restarting = yield* Deferred.make<void>();
        yield* Stream.runForEach(system.events, (event) =>
          event._tag === "ActorRestarting" && event.path === "/user/personal"
            ? Deferred.succeed(restarting, undefined).pipe(Effect.asVoid)
            : Effect.void,
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const sending = yield* api.personal.sendMessage(input).pipe(Effect.forkScoped);
        yield* Deferred.await(restarting);
        assert.equal(
          sending.pollUnsafe(),
          undefined,
          "no success receipt precedes commit acknowledgement",
        );
        assert.equal(records.get("/personal")?.messages.length, 1);
      }),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry(store);
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            PersonalActions.unavailable,
            Layer.succeed(ContextRegistry, registry),
            PersonalProcessor.disabled,
          ),
        );
        const personal = yield* system.spawn("personal", PersonalAgentActor);
        const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
        assert.deepEqual(yield* api.personal.sendMessage(input), {
          requestId: input.requestId,
          revision: 2,
          sequence: 1,
        });
        assert.equal((yield* api.personal.get).messages.length, 1);
      }),
    ),
  );
});
