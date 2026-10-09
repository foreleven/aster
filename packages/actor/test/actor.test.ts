import { Effect, Fiber, Layer, Match, Schema, Stream } from "effect";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import {
  Actor,
  ActorPersistence,
  ActorSystem,
  InMemoryActorPersistence,
  PersistenceError,
  PersistentActor,
  ReplyTo,
  SqliteActorPersistence,
  type ActorRef,
} from "../src/index.js";

const CounterCommand = Schema.TaggedUnion({
  Add: { value: Schema.Number },
  Read: { replyTo: ReplyTo<number>() },
  Fail: {},
});
type CounterCommand = typeof CounterCommand.Type;

const CounterActor = Actor.define("test/CounterActor", {
  commands: Object.values(CounterCommand.cases),
})(
  Effect.sync(() => {
    let value = 0;
    return {
      receive: (command) =>
        Match.value(command).pipe(
          Match.tag("Add", (command) =>
            Effect.sync(() => {
              value += command.value;
            }),
          ),
          Match.tag("Fail", (_command) => Effect.die(new Error("counter failure"))),
          Match.tag("Read", (command) => command.replyTo.tell(value)),
          Match.exhaustive,
        ),
    };
  }),
);

test("serializes commands and supports ask", async () => {
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const system = yield* ActorSystem.make();
      const counter = yield* system.spawn("counter", CounterActor);
      yield* counter.tell({ _tag: "Add", value: 2 });
      yield* counter.tell({ _tag: "Add", value: 3 });
      return yield* counter.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds");
    }).pipe(Effect.scoped),
  );
  assert.equal(result, 5);
});

test("restarts an ordinary actor and retains commands queued after a failure", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make();
        const counter = yield* system.spawn("counter", CounterActor);
        yield* counter.tell({ _tag: "Add", value: 9 });
        yield* counter.tell({ _tag: "Fail" });
        return yield* counter.ask((replyTo) => ({ _tag: "Read", replyTo }));
      }),
    ),
  );
  assert.equal(result, 0);
});

const DoubleReplyActor = Actor.define("test/DoubleReplyActor", {
  commands: [Schema.TaggedStruct("Ask", { replyTo: ReplyTo<number>() })],
})(
  Effect.succeed({
    receive: (command) =>
      Match.value(command).pipe(
        Match.tag("Ask", (command) =>
          Effect.gen(function* () {
            yield* command.replyTo.tell(1);
            yield* command.replyTo.tell(2);
          }),
        ),
        Match.exhaustive,
      ),
  }),
);

test("ask accepts one reply and reports a duplicate as a redacted DeadLetter", async () => {
  const observed = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make();
        const dead = yield* Stream.runHead(
          Stream.filter(system.events, (event) => event._tag === "DeadLetter"),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const actor = yield* system.spawn("double", DoubleReplyActor);
        const value = yield* actor.ask((replyTo) => ({ _tag: "Ask", replyTo }), "2 seconds");
        const event = yield* Fiber.join(dead).pipe(Effect.timeout("2 seconds"));
        return { value, event };
      }),
    ),
  );
  assert.equal(observed.value, 1);
  assert.equal(observed.event._tag, "Some");
  if (observed.event._tag === "Some" && observed.event.value._tag === "DeadLetter") {
    assert.match(
      observed.event.value.target,
      /^\/system\/ask\/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    assert.equal("payload" in observed.event.value, false);
  }
});

const LateCommand = Schema.TaggedUnion({
  Ask: { replyTo: ReplyTo<number>() },
  Release: {},
});
type LateCommand = typeof LateCommand.Type;

const LateReplyActor = Actor.define("test/LateReplyActor", {
  commands: Object.values(LateCommand.cases),
})(
  Effect.sync(() => {
    let pending: ActorRef<number> | undefined;
    return {
      receive: (command) =>
        Match.value(command).pipe(
          Match.tag("Ask", (command) =>
            Effect.sync(() => {
              pending = command.replyTo;
            }),
          ),
          Match.tag("Release", (_command) => pending?.tell(9) ?? Effect.void),
          Match.exhaustive,
        ),
    };
  }),
);

test("a reply after ask timeout becomes a DeadLetter", async () => {
  const observed = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make();
        const dead = yield* Stream.runHead(
          Stream.filter(system.events, (event) => event._tag === "DeadLetter"),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const actor = yield* system.spawn("late", LateReplyActor);
        const result = yield* Effect.exit(actor.ask((replyTo) => ({ _tag: "Ask", replyTo }), 1));
        yield* actor.tell({ _tag: "Release" });
        return { result, event: yield* Fiber.join(dead).pipe(Effect.timeout("2 seconds")) };
      }),
    ),
  );
  assert.equal(observed.result._tag, "Failure");
  assert.equal(observed.event._tag, "Some");
  if (observed.event._tag === "Some" && observed.event.value._tag === "DeadLetter") {
    assert.equal(observed.event.value.reason, "ask closed");
  }
});

const PersistentCommand = Schema.TaggedUnion({
  Add: { value: Schema.Number },
  AddBatch: { values: Schema.Array(Schema.Number) },
  Snapshot: {},
  Stop: {},
  Read: { replyTo: ReplyTo<number>() },
});
type PersistentCommand = typeof PersistentCommand.Type;

const SchemaCounterCommand = Schema.TaggedUnion({
  Add: { value: Schema.Number },
  Read: { replyTo: ReplyTo<number>() },
});

const SchemaCounter = PersistentActor.define("test/SchemaCounter", {
  commands: Object.values(SchemaCounterCommand.cases),
  event: Schema.Number,
  state: Schema.Number,
})(
  Effect.succeed({
    initialState: 0,
    applyEvent: (state, event) => state + event,
    receive: (command, context) =>
      Match.value(command).pipe(
        Match.tag("Add", (command) => context.persist(command.value)),
        Match.tag("Read", (command) => command.replyTo.tell(context.state)),
        Match.exhaustive,
      ),
  }),
);

const SchemaEcho = Actor.define("test/SchemaEcho", {
  commands: [Schema.TaggedStruct("Read", { replyTo: ReplyTo<number>() })],
})(
  Effect.succeed({
    receive: (command) =>
      Match.value(command).pipe(
        Match.tag("Read", (command) => command.replyTo.tell(42)),
        Match.exhaustive,
      ),
  }),
);

const schemaInferenceChecks = SchemaCounter.of({
  eventSchema: Schema.Number,
  stateSchema: Schema.Number,
  initialState: 0,
  applyEvent: (state, event) => {
    // @ts-expect-error Event is inferred as number.
    const invalidEvent: string = event;
    void invalidEvent;
    return state + event;
  },
  receive: (command, context) => {
    // @ts-expect-error Command is inferred from its Schema.
    const invalidCommand: { readonly _tag: "Other" } = command;
    // @ts-expect-error State is inferred as number.
    const invalidState: string = context.state;
    // @ts-expect-error Persisted Event is inferred as number.
    void context.persist("invalid");
    void invalidCommand;
    void invalidState;
    return Effect.void;
  },
});
void schemaInferenceChecks;

const commandSchemaRequiredChecks = () => {
  // @ts-expect-error Ordinary Actor definitions require a Command Schema.
  void Actor.define("test/MissingCommandSchema");
  // @ts-expect-error Persistent Actor definitions require Command, Event, and State Schemas.
  void PersistentActor.define("test/MissingPersistentSchemas");
};
void commandSchemaRequiredChecks;

test("define infers command, event, and state from Schemas", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(InMemoryActorPersistence.layer),
        );
        const counter = yield* system.spawn("schema-counter", SchemaCounter);
        const echo = yield* system.spawn("schema-echo", SchemaEcho);
        yield* counter.tell({ _tag: "Add", value: 3 });
        return {
          count: yield* counter.ask((replyTo) => ({ _tag: "Read", replyTo })),
          echo: yield* echo.ask((replyTo) => ({ _tag: "Read", replyTo })),
        };
      }),
    ),
  );
  assert.deepEqual(result, { count: 3, echo: 42 });
});

const PersistentCounter = PersistentActor.define("test/PersistentCounter", {
  commands: Object.values(PersistentCommand.cases),
  event: Schema.Number,
  state: Schema.Number,
})(
  Effect.succeed({
    initialState: 0,
    applyEvent: (state, event) => state + event,
    receive: (command, context) =>
      Match.value(command).pipe(
        Match.tag("Add", (command) => context.persist(command.value)),
        Match.tag("AddBatch", (command) => context.persistAll(command.values)),
        Match.tag("Snapshot", (_command) => context.saveSnapshot()),
        Match.tag("Stop", (_command) => context.stopSelf()),
        Match.tag("Read", (command) => command.replyTo.tell(context.state)),
        Match.exhaustive,
      ),
  }),
);

test("recovers SQLite state across system lifetimes and appends after a snapshot", async () => {
  const directory = await mkdtemp(join(tmpdir(), "actor-test-"));
  const databasePath = join(directory, "journal.sqlite");
  const run = (commands: ReadonlyArray<PersistentCommand>) =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(SqliteActorPersistence.layer({ path: databasePath })),
          );
          const counter = yield* system.spawn("counter", PersistentCounter);
          for (const command of commands) yield* counter.tell(command);
          return yield* counter.ask((replyTo) => ({ _tag: "Read", replyTo }));
        }),
      ),
    );
  try {
    assert.equal(await run([{ _tag: "AddBatch", values: [2, 3] }, { _tag: "Snapshot" }]), 5);
    assert.equal(await run([{ _tag: "Add", value: 7 }]), 12);
    const database = new DatabaseSync(databasePath);
    try {
      const events = database
        .prepare("SELECT sequence_number FROM actor_events WHERE id = ? ORDER BY sequence_number")
        .all("/user/counter");
      const stream = database
        .prepare("SELECT sequence_number FROM actor_streams WHERE id = ?")
        .get("/user/counter");
      assert.deepEqual(
        events.map((event) => event.sequence_number),
        [3],
      );
      assert.equal(stream?.sequence_number, 3);
    } finally {
      database.close();
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("a new incarnation at the same path recovers from in-memory persistence", async () => {
  const value = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(InMemoryActorPersistence.layer),
        );
        const stopped = yield* Stream.runHead(
          Stream.filter(system.events, (event) => event._tag === "ActorStopped"),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const first = yield* system.spawn("counter", PersistentCounter);
        yield* first.tell({ _tag: "Add", value: 5 });
        yield* first.tell({ _tag: "Stop" });
        yield* Fiber.join(stopped).pipe(Effect.timeout("2 seconds"));
        const second = yield* system.spawn("counter", PersistentCounter);
        return yield* second.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds");
      }),
    ),
  );
  assert.equal(value, 5);
});

const SharedIdentityCounter = PersistentActor.define("test/SharedIdentityCounter", {
  commands: Object.values(PersistentCommand.cases),
  event: Schema.Number,
  state: Schema.Number,
})(
  Effect.succeed({
    initialState: 0,
    persistenceId: () => "shared-counter",
    applyEvent: (state, event) => state + event,
    receive: (command, context) =>
      Match.value(command).pipe(
        Match.tag("Add", (command) => context.persist(command.value)),
        Match.tag("Stop", (_command) => context.stopSelf()),
        Match.tag("Read", (command) => command.replyTo.tell(context.state)),
        Match.tag("AddBatch", "Snapshot", (_command) => Effect.void),
        Match.exhaustive,
      ),
  }),
);

test("a PersistentActor can use a stable identity independent of its path", async () => {
  const value = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(InMemoryActorPersistence.layer),
        );
        const stopped = yield* Stream.runHead(
          Stream.filter(system.events, (event) => event._tag === "ActorStopped"),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const first = yield* system.spawn("first", SharedIdentityCounter);
        yield* first.tell({ _tag: "Add", value: 8 });
        yield* first.tell({ _tag: "Stop" });
        yield* Fiber.join(stopped).pipe(Effect.timeout("2 seconds"));
        const second = yield* system.spawn("second", SharedIdentityCounter);
        return yield* second.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds");
      }),
    ),
  );
  assert.equal(value, 8);
});

const ObjectCommand = Schema.TaggedUnion({
  Add: { value: Schema.Number },
  AttemptMutation: {},
  Read: { replyTo: ReplyTo<number>() },
});
type ObjectCommand = typeof ObjectCommand.Type;

const ObjectCounter = PersistentActor.define("test/ObjectCounter", {
  commands: Object.values(ObjectCommand.cases),
  event: Schema.Number,
  state: Schema.Struct({ count: Schema.Number }),
})(
  Effect.succeed({
    initialState: { count: 0 },
    applyEvent: (state, event) => ({ count: state.count + event }),
    receive: (command, context) =>
      Match.value(command).pipe(
        Match.tag("Add", (command) => context.persist(command.value)),
        Match.tag("AttemptMutation", (_command) =>
          Effect.sync(() => {
            (context.state as { count: number }).count = 999;
          }),
        ),
        Match.tag("Read", (command) => command.replyTo.tell(context.state.count)),
        Match.exhaustive,
      ),
  }),
);

test("persistent state reads cannot mutate the runtime-owned state", async () => {
  const value = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(InMemoryActorPersistence.layer),
        );
        const actor = yield* system.spawn("object", ObjectCounter);
        yield* actor.tell({ _tag: "Add", value: 2 });
        yield* actor.tell({ _tag: "AttemptMutation" });
        return yield* actor.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds");
      }),
    ),
  );
  assert.equal(value, 2);
});

test("recovery retries cleanup after a snapshot was saved", async () => {
  let cleanupCalls = 0;
  const failOnce = Layer.effect(
    ActorPersistence,
    Effect.gen(function* () {
      const real = yield* ActorPersistence;
      return ActorPersistence.of({
        ...real,
        cleanup: (id, sequenceNumber) => {
          cleanupCalls++;
          return cleanupCalls === 1
            ? Effect.fail(
                new PersistenceError({ operation: "cleanup", id, message: "cleanup failed" }),
              )
            : real.cleanup(id, sequenceNumber);
        },
      });
    }),
  );
  const value = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(InMemoryActorPersistence.layer, failOnce),
        );
        const actor = yield* system.spawn("counter", PersistentCounter);
        yield* actor.tell({ _tag: "Add", value: 3 });
        yield* actor.tell({ _tag: "Snapshot" });
        return yield* actor.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds");
      }),
    ),
  );
  assert.equal(value, 3);
  assert.equal(cleanupCalls, 2);
});

const BigIntCommand = Schema.TaggedUnion({
  Add: { value: Schema.BigInt },
  Snapshot: {},
  Read: { replyTo: ReplyTo<bigint>() },
});
type BigIntCommand = typeof BigIntCommand.Type;

const BigIntCounter = PersistentActor.define("test/BigIntCounter", {
  commands: Object.values(BigIntCommand.cases),
  event: Schema.BigInt,
  state: Schema.BigInt,
})(
  Effect.succeed({
    initialState: 0n,
    applyEvent: (state, event) => state + event,
    receive: (command, context) =>
      Match.value(command).pipe(
        Match.tag("Add", (command) => context.persist(command.value)),
        Match.tag("Snapshot", (_command) => context.saveSnapshot()),
        Match.tag("Read", (command) => command.replyTo.tell(context.state)),
        Match.exhaustive,
      ),
  }),
);

test("SQLite recovery preserves Schema encoded bigint events and state", async () => {
  const directory = await mkdtemp(join(tmpdir(), "actor-bigint-test-"));
  const path = join(directory, "journal.sqlite");
  const run = (commands: ReadonlyArray<BigIntCommand>) =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(SqliteActorPersistence.layer({ path })),
          );
          const actor = yield* system.spawn("bigint", BigIntCounter);
          for (const command of commands) yield* actor.tell(command);
          return yield* actor.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds");
        }),
      ),
    );
  try {
    assert.equal(await run([{ _tag: "Add", value: 5n }, { _tag: "Snapshot" }]), 5n);
    assert.equal(await run([{ _tag: "Add", value: 7n }]), 12n);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
