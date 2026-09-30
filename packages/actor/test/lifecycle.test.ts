import assert from "node:assert/strict";
import { test } from "node:test";
import { Clock, Context, Deferred, Effect, Match, Fiber, Layer, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import {
  ReplyTo,
  Actor,
  ActorSystem,
  ActorTestKit,
  PersistentActor,
  type ActorRef,
} from "../src/index.js";

const ChildCommand = Schema.Union([
  Schema.TaggedStruct("Add", { value: Schema.Number }),
  Schema.TaggedStruct("Read", { replyTo: ReplyTo<number>() }),
  Schema.TaggedStruct("Stop", {}),
]);
type ChildCommand = typeof ChildCommand.Type;

class ChildActor extends Actor.Service<ChildActor>()("test/ChildActor", { command: ChildCommand }) {
  static readonly layer = Layer.effect(
    ChildActor,
    Effect.sync(() => {
      let value = 0;
      return ChildActor.of({
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("Add", (command) =>
              Effect.sync(() => {
                value += command.value;
              }),
            ),
            Match.tag("Stop", (_command) => context.stopSelf()),
            Match.tag("Read", (command) => command.replyTo.tell(value)),
            Match.exhaustive,
          ),
      });
    }),
  );
}

const ParentCommand = Schema.Union([
  Schema.TaggedStruct("Add", { value: Schema.Number }),
  Schema.TaggedStruct("Read", { replyTo: ReplyTo<number>() }),
  Schema.TaggedStruct("StopChild", { replyTo: ReplyTo<string>() }),
  Schema.TaggedStruct("Fail", {}),
]);
type ParentCommand = typeof ParentCommand.Type;

class ParentActor extends Actor.Service<ParentActor>()("test/ParentActor", {
  command: ParentCommand,
}) {
  static readonly layer = Layer.effect(
    ParentActor,
    Effect.sync(() => {
      let child: ActorRef<ChildCommand>;
      let stoppedReply: ActorRef<string> | undefined;
      return ParentActor.of({
        started: (context) =>
          Effect.gen(function* () {
            child =
              ((yield* context.child("child")) as ActorRef<ChildCommand> | undefined) ??
              (yield* context.spawn("child", ChildActor));
            yield* context.watch(child);
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("Fail", (_command) => Effect.die(new Error("parent failure"))),
            Match.tag("Add", "Read", (command) => child.tell(command)),
            Match.tag("StopChild", (command) => {
              stoppedReply = command.replyTo;
              return context.stop(child);
            }),
            Match.exhaustive,
          ),
        receiveSignal: (signal) =>
          Effect.gen(function* () {
            if (signal._tag === "Terminated" && stoppedReply !== undefined) {
              yield* stoppedReply.tell(signal.ref.path);
            }
          }),
      });
    }),
  );
}

test("a parent restart keeps its child, and watch receives Terminated", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make();
        const parent = yield* system.spawn("parent", ParentActor);
        yield* parent.tell({ _tag: "Add", value: 4 });
        yield* parent.tell({ _tag: "Fail" });
        const value = yield* parent.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds");
        const stoppedPath = yield* parent.ask(
          (replyTo) => ({ _tag: "StopChild", replyTo }),
          "2 seconds",
        );
        return { value, stoppedPath };
      }),
    ),
  );
  assert.deepEqual(result, { value: 4, stoppedPath: "/user/parent/child" });
});

test("stopping a root awaits its children and leaves sibling roots available", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make();
        const parent = yield* system.spawn("parent", ParentActor);
        const sibling = yield* system.spawn("sibling", ChildActor);
        yield* parent.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds");
        yield* system.stop(parent);
        assert.deepEqual(
          (yield* system.inspect()).map((cell) => cell.path),
          ["/user/sibling"],
        );
        yield* sibling.tell({ _tag: "Add", value: 7 });
        assert.equal(yield* sibling.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds"), 7);
        yield* system.stop(parent);
      }),
    ),
  );
});

const AsyncCommand = Schema.Union([
  Schema.TaggedStruct("Run", {
    replyTo: ReplyTo<number>(),
  }),
  Schema.TaggedStruct("RunFailure", {
    replyTo: ReplyTo<number>(),
  }),
  Schema.TaggedStruct("Completed", {
    replyTo: ReplyTo<number>(),
    value: Schema.Number,
  }),
]);
type AsyncCommand = typeof AsyncCommand.Type;

class AsyncActor extends Actor.Service<AsyncActor>()("test/AsyncActor", { command: AsyncCommand }) {
  static readonly layer = Layer.effect(
    AsyncActor,
    Effect.succeed(
      AsyncActor.of({
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("Run", (command) =>
              context.pipeToSelf(Effect.succeed(7), (outcome) => ({
                _tag: "Completed",
                replyTo: command.replyTo,
                value: Match.value(outcome).pipe(
                  Match.tag("Success", (result) => result.value),
                  Match.tag("Failure", () => -1),
                  Match.exhaustive,
                ),
              })),
            ),
            Match.tag("RunFailure", (command) =>
              context.pipeToSelf(Effect.fail("expected"), (outcome) => ({
                _tag: "Completed",
                replyTo: command.replyTo,
                value: Match.value(outcome).pipe(
                  Match.tag("Success", (result) => result.value),
                  Match.tag("Failure", () => -1),
                  Match.exhaustive,
                ),
              })),
            ),
            Match.tag("Completed", (command) => command.replyTo.tell(command.value)),
            Match.exhaustive,
          ),
      }),
    ),
  );
}

test("pipeToSelf sends an asynchronous result back through the mailbox", async () => {
  const values = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make();
        const actor = yield* system.spawn("async", AsyncActor);
        const success = yield* actor.ask((replyTo) => ({ _tag: "Run", replyTo }), "2 seconds");
        const expectedFailure = yield* actor.ask(
          (replyTo) => ({ _tag: "RunFailure", replyTo }),
          "2 seconds",
        );
        return { success, expectedFailure };
      }),
    ),
  );
  assert.deepEqual(values, { success: 7, expectedFailure: -1 });
});

const TimeoutCommand = Schema.TaggedStruct("Ping", {});
type TimeoutCommand = typeof TimeoutCommand.Type;
class TimeoutActor extends Actor.Service<TimeoutActor>()("test/TimeoutActor", {
  command: TimeoutCommand,
}) {
  static readonly layer = Layer.effect(
    TimeoutActor,
    Effect.succeed(
      TimeoutActor.of({
        started: (context) => context.receiveTimeout(15),
        receive: (command) =>
          Match.value(command).pipe(
            Match.tag("Ping", (_command) => Effect.void),
            Match.exhaustive,
          ),
      }),
    ),
  );
}

const LongTimeoutCommand = Schema.TaggedStruct("Ping", {
  replyTo: ReplyTo<void>(),
});
type LongTimeoutCommand = typeof LongTimeoutCommand.Type;
class LongTimeoutActor extends Actor.Service<LongTimeoutActor>()("test/LongTimeoutActor", {
  command: LongTimeoutCommand,
}) {
  static readonly layer = Layer.effect(
    LongTimeoutActor,
    Effect.succeed(
      LongTimeoutActor.of({
        started: (context) => context.receiveTimeout("1 hour"),
        receive: (command) =>
          Match.value(command).pipe(
            Match.tag("Ping", (command) => command.replyTo.tell(undefined)),
            Match.exhaustive,
          ),
      }),
    ),
  );
}

test("ReceiveTimeout stops an idle actor and stale delivery becomes a redacted DeadLetter", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make();
        const stopped = yield* Stream.runHead(
          Stream.filter(system.events, (event) => event._tag === "ActorStopped"),
        ).pipe(Effect.forkScoped);
        const dead = yield* Stream.runHead(
          Stream.filter(system.events, (event) => event._tag === "DeadLetter"),
        ).pipe(Effect.forkScoped);
        yield* Effect.sleep(1);
        const actor = yield* system.spawn("timeout", TimeoutActor);
        const stopEvent = yield* Fiber.join(stopped).pipe(Effect.timeout("2 seconds"));
        yield* actor.tell({ _tag: "Ping" });
        const deadEvent = yield* Fiber.join(dead).pipe(Effect.timeout("2 seconds"));
        return { stopEvent, deadEvent };
      }),
    ),
  );
  assert.equal(result.stopEvent._tag, "Some");
  assert.equal(result.deadEvent._tag, "Some");
  if (result.deadEvent._tag === "Some") {
    assert.equal(result.deadEvent.value._tag, "DeadLetter");
    assert.equal("payload" in result.deadEvent.value, false);
    if (result.deadEvent.value._tag === "DeadLetter") {
      assert.equal(result.deadEvent.value.commandTag, "Ping");
    }
  }
});

class NumberService extends Context.Service<NumberService, { readonly value: number }>()(
  "test/NumberService",
) {}

class ServiceActor extends Actor.Service<ServiceActor, NumberService>()("test/ServiceActor", {
  command: Schema.TaggedStruct("Read", { replyTo: ReplyTo<number>() }),
}) {
  static readonly layer = Layer.effect(
    ServiceActor,
    Effect.gen(function* () {
      const service = yield* NumberService;
      return ServiceActor.of({
        receive: (command) =>
          Match.value(command).pipe(
            Match.tag("Read", (command) => command.replyTo.tell(service.value)),
            Match.exhaustive,
          ),
      });
    }),
  );
}

class DependentService extends Context.Service<DependentService, { readonly doubled: number }>()(
  "test/DependentService",
) {}
const dependentLayer = Layer.effect(
  DependentService,
  Effect.gen(function* () {
    const number = yield* NumberService;
    return { doubled: number.value * 2 };
  }),
);

class DependentActor extends Actor.Service<DependentActor, DependentService>()(
  "test/DependentActor",
  { command: Schema.TaggedStruct("Read", { replyTo: ReplyTo<number>() }) },
) {
  static readonly layer = Layer.effect(
    DependentActor,
    Effect.gen(function* () {
      const service = yield* DependentService;
      return DependentActor.of({
        receive: (command) =>
          Match.value(command).pipe(
            Match.tag("Read", (command) => command.replyTo.tell(service.doubled)),
            Match.exhaustive,
          ),
      });
    }),
  );
}

test("ActorSystem provides services to actor layers and terminates idempotently", async () => {
  const value = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(NumberService, { value: 42 })),
        );
        const actor = yield* system.spawn("service", ServiceActor);
        const result = yield* actor.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds");
        yield* Effect.all([system.terminate(), system.terminate()], { concurrency: 2 });
        return result;
      }),
    ),
  );
  assert.equal(value, 42);
});

test("ActorSystem.provide builds dependent Layers in order", async () => {
  const value = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(NumberService, { value: 21 }), dependentLayer),
        );
        const actor = yield* system.spawn("dependent", DependentActor);
        return yield* actor.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds");
      }),
    ),
  );
  assert.equal(value, 42);
});

test("terminate waits for provided Layer finalizers", async () => {
  let closed = false;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const resource = Layer.effect(
          NumberService,
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                closed = true;
              }),
            );
            return NumberService.of({ value: 1 });
          }),
        );
        const system = yield* ActorSystem.make().pipe(ActorSystem.provide(resource));
        yield* system.terminate();
        assert.equal(closed, true);
      }),
    ),
  );
});

test("TestClock controls ReceiveTimeout and a scoped probe records replies", async () => {
  const observed = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(Clock.Clock, clock)),
        );
        const stopped = yield* Stream.runHead(
          Stream.filter(system.events, (event) => event._tag === "ActorStopped"),
        ).pipe(Effect.forkScoped);
        yield* Effect.sleep(1);
        const actor = yield* system.spawn("timeout", LongTimeoutActor);
        yield* actor.ask((replyTo) => ({ _tag: "Ping", replyTo }));
        const probe = yield* ActorTestKit.probe<TimeoutCommand>();
        yield* probe.ref.tell({ _tag: "Ping" });
        const received = yield* probe.take();
        yield* clock.adjust("1 hour");
        const stoppedEvent = yield* Fiber.join(stopped).pipe(Effect.timeout("2 seconds"));
        return { received, stoppedEvent, actorPath: actor.path };
      }),
    ),
  );
  assert.deepEqual(observed.received, { _tag: "Ping" });
  assert.equal(observed.stoppedEvent._tag, "Some");
  assert.equal(observed.actorPath, "/user/timeout");
});

test("terminate wakes an actor waiting with a long ReceiveTimeout", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make();
        const actor = yield* system.spawn("long-timeout", LongTimeoutActor);
        yield* actor.ask((replyTo) => ({ _tag: "Ping", replyTo }), "2 seconds");
        yield* system.terminate().pipe(Effect.timeout("2 seconds"));
      }),
    ),
  );
});

class GateService extends Context.Service<
  GateService,
  {
    readonly entered: Deferred.Deferred<void>;
    readonly release: Deferred.Deferred<void>;
    readonly completed: () => void;
  }
>()("test/GateService") {}

const GateCommand = Schema.Union([
  Schema.TaggedStruct("Wait", {}),
  Schema.TaggedStruct("Queued", {}),
]);
type GateCommand = typeof GateCommand.Type;

class GateActor extends Actor.Service<GateActor, GateService>()("test/GateActor", {
  command: GateCommand,
}) {
  static readonly layer = Layer.effect(
    GateActor,
    Effect.gen(function* () {
      const gate = yield* GateService;
      return GateActor.of({
        receive: (command) =>
          Match.value(command).pipe(
            Match.tag("Queued", (_command) => Effect.void),
            Match.tag("Wait", (_command) =>
              Effect.gen(function* () {
                yield* Deferred.succeed(gate.entered, undefined);
                yield* Deferred.await(gate.release);
                gate.completed();
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}

test("terminate waits for the current handler and drops queued commands", async () => {
  let completed = false;
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(GateService, {
              entered,
              release,
              completed: () => {
                completed = true;
              },
            }),
          ),
        );
        const dead = yield* Stream.runHead(
          Stream.filter(system.events, (event) => event._tag === "DeadLetter"),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const actor = yield* system.spawn("gate", GateActor);
        yield* actor.tell({ _tag: "Wait" });
        yield* Deferred.await(entered);
        yield* actor.tell({ _tag: "Queued" });
        const termination = yield* system.terminate().pipe(Effect.forkScoped);
        const discarded = yield* Fiber.join(dead).pipe(Effect.timeout("2 seconds"));
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(termination).pipe(Effect.timeout("2 seconds"));
        return discarded;
      }),
    ),
  );
  assert.equal(completed, true);
  assert.equal(result._tag, "Some");
  if (result._tag === "Some" && result.value._tag === "DeadLetter") {
    assert.equal(result.value.commandTag, "Queued");
  }
});

test("closing the ActorSystem scope interrupts a blocked handler", async () => {
  let completed = false;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const nested = Effect.scoped(
          Effect.gen(function* () {
            const system = yield* ActorSystem.make().pipe(
              ActorSystem.provide(
                Layer.succeed(GateService, {
                  entered,
                  release,
                  completed: () => {
                    completed = true;
                  },
                }),
              ),
            );
            const actor = yield* system.spawn("gate", GateActor);
            yield* actor.tell({ _tag: "Wait" });
            yield* Deferred.await(entered);
            return yield* Effect.never;
          }),
        );
        const fiber = yield* nested.pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber).pipe(Effect.timeout("2 seconds"));
      }),
    ),
  );
  assert.equal(completed, false);
});

test("a stopped path can be reused without reviving its stale ActorRef", async () => {
  const observed = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make();
        const stopped = yield* Stream.runHead(
          Stream.filter(system.events, (event) => event._tag === "ActorStopped"),
        ).pipe(Effect.forkScoped);
        const dead = yield* Stream.runHead(
          Stream.filter(system.events, (event) => event._tag === "DeadLetter"),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const oldRef = yield* system.spawn("worker", ChildActor);
        yield* oldRef.tell({ _tag: "Stop" });
        yield* Fiber.join(stopped).pipe(Effect.timeout("2 seconds"));
        const newRef = yield* system.spawn("worker", ChildActor);
        const reply = yield* ActorTestKit.probe<number>();
        yield* oldRef.tell({ _tag: "Read", replyTo: reply.ref });
        const deadEvent = yield* Fiber.join(dead).pipe(Effect.timeout("2 seconds"));
        yield* reply.expectNoMessage(1);
        const value = yield* newRef.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds");
        return { oldRef, newRef, deadEvent, value };
      }),
    ),
  );
  assert.equal(observed.oldRef.path, observed.newRef.path);
  assert.notEqual(observed.oldRef.incarnation, observed.newRef.incarnation);
  assert.equal(observed.value, 0);
  assert.equal(observed.deadEvent._tag, "Some");
});

class AlwaysFails extends Actor.Service<AlwaysFails>()("test/AlwaysFails", {
  command: Schema.TaggedStruct("Noop", {}),
}) {
  static readonly layer = Layer.effect(
    AlwaysFails,
    Effect.succeed(
      AlwaysFails.of({
        started: () => Effect.die(new Error("cannot start")),
        receive: (command) =>
          Match.value(command).pipe(
            Match.tag("Noop", (_command) => Effect.void),
            Match.exhaustive,
          ),
      }),
    ),
  );
}

test("default supervision stops after five restarts within one minute", async () => {
  const events = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(Clock.Clock, clock)),
        );
        const collected = yield* Stream.runCollect(Stream.take(system.events, 6)).pipe(
          Effect.forkScoped,
        );
        const first = yield* Stream.runHead(
          Stream.filter(system.events, (event) => event._tag === "ActorRestarting"),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* system.spawn("broken", AlwaysFails);
        yield* Fiber.join(first);
        yield* clock.adjust("10 seconds");
        return [...(yield* Fiber.join(collected))];
      }),
    ),
  );
  assert.equal(events.filter((event) => event._tag === "ActorRestarting").length, 5);
  const stopped = events.find((event) => event._tag === "ActorStopped");
  assert.equal(stopped?._tag, "ActorStopped");
  if (stopped?._tag === "ActorStopped") {
    assert.match(stopped.cause?.message ?? "", /cannot start/);
  }
});

interface InstanceCounts {
  built: number;
  closed: number;
}
class CountsService extends Context.Service<CountsService, InstanceCounts>()(
  "test/CountsService",
) {}

const ScopedCommand = Schema.Union([
  Schema.TaggedStruct("Fail", {}),
  Schema.TaggedStruct("Read", { replyTo: ReplyTo<InstanceCounts>() }),
]);
type ScopedCommand = typeof ScopedCommand.Type;

class ScopedActor extends Actor.Service<ScopedActor, CountsService>()("test/ScopedActor", {
  command: ScopedCommand,
}) {
  static readonly layer = Layer.effect(
    ScopedActor,
    Effect.gen(function* () {
      const counts = yield* CountsService;
      counts.built++;
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          counts.closed++;
        }),
      );
      return ScopedActor.of({
        receive: (command) =>
          Match.value(command).pipe(
            Match.tag("Fail", (_command) => Effect.die(new Error("restart"))),
            Match.tag("Read", (command) => command.replyTo.tell({ ...counts })),
            Match.exhaustive,
          ),
      });
    }),
  );
}

test("restart closes the old Behavior Layer scope before building a new one", async () => {
  const counts: InstanceCounts = { built: 0, closed: 0 };
  const during = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(CountsService, counts)),
        );
        const actor = yield* system.spawn("scoped", ScopedActor);
        yield* actor.tell({ _tag: "Fail" });
        return yield* actor.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds");
      }),
    ),
  );
  assert.deepEqual(during, { built: 2, closed: 1 });
  assert.deepEqual(counts, { built: 2, closed: 2 });
});

const PipeFailureCommand = Schema.Union([
  Schema.TaggedStruct("Start", {}),
  Schema.TaggedStruct("Read", { replyTo: ReplyTo<number>() }),
]);
type PipeFailureCommand = typeof PipeFailureCommand.Type;

class DefectivePipeActor extends Actor.Service<DefectivePipeActor, CountsService>()(
  "test/DefectivePipeActor",
  { command: PipeFailureCommand },
) {
  static readonly layer = Layer.effect(
    DefectivePipeActor,
    Effect.gen(function* () {
      const counts = yield* CountsService;
      counts.built++;
      return DefectivePipeActor.of({
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("Start", (_command) =>
              context.pipeToSelf(Effect.die(new Error("background defect")), () => ({
                _tag: "Start",
              })),
            ),
            Match.tag("Read", (command) => command.replyTo.tell(counts.built)),
            Match.exhaustive,
          ),
      });
    }),
  );
}

test("a pipeToSelf defect fails the actor and invokes supervision", async () => {
  const counts: InstanceCounts = { built: 0, closed: 0 };
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(CountsService, counts)),
        );
        const restarting = yield* Stream.runHead(
          Stream.filter(system.events, (event) => event._tag === "ActorRestarting"),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const actor = yield* system.spawn("pipe", DefectivePipeActor);
        yield* actor.tell({ _tag: "Start" });
        const event = yield* Fiber.join(restarting).pipe(Effect.timeout("2 seconds"));
        const built = yield* actor.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds");
        return { event, built };
      }),
    ),
  );
  assert.equal(result.event._tag, "Some");
  assert.equal(result.built, 2);
});

test("a custom stop directive ends a failed actor without restarting it", async () => {
  const stopped = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make();
        const event = yield* Stream.runHead(
          Stream.filter(system.events, (value) => value._tag === "ActorStopped"),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* system.spawn("failing", AlwaysFails, { supervision: () => "stop" });
        return yield* Fiber.join(event).pipe(Effect.timeout("2 seconds"));
      }),
    ),
  );
  assert.equal(stopped._tag, "Some");
  if (stopped._tag === "Some" && stopped.value._tag === "ActorStopped") {
    assert.match(stopped.value.cause?.message ?? "", /cannot start/);
  }
});

class WatchReport extends Context.Service<WatchReport, { readonly report: ActorRef<string> }>()(
  "test/WatchReport",
) {}

class WatchingParent extends Actor.Service<WatchingParent, WatchReport>()("test/WatchingParent", {
  command: Schema.TaggedStruct("Noop", {}),
}) {
  static readonly layer = Layer.effect(
    WatchingParent,
    Effect.gen(function* () {
      const report = yield* WatchReport;
      return WatchingParent.of({
        started: (context) =>
          Effect.gen(function* () {
            const child = yield* context.spawn("broken", AlwaysFails, {
              supervision: () => "stop",
            });
            yield* context.watch(child);
          }),
        receive: (command) =>
          Match.value(command).pipe(
            Match.tag("Noop", (_command) => Effect.void),
            Match.exhaustive,
          ),
        receiveSignal: (signal) => report.report.tell(signal.cause?.message ?? "missing cause"),
      });
    }),
  );
}

test("DeathWatch carries the terminal failure cause", async () => {
  const message = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const report = yield* ActorTestKit.probe<string>();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(WatchReport, { report: report.ref })),
        );
        yield* system.spawn("watching", WatchingParent);
        return yield* report.take().pipe(Effect.timeout("2 seconds"));
      }),
    ),
  );
  assert.match(message, /cannot start/);
});

class BoomChild extends Actor.Service<BoomChild>()("test/BoomChild", {
  command: Schema.TaggedStruct("Boom", {}),
}) {
  static readonly layer = Layer.effect(
    BoomChild,
    Effect.succeed(
      BoomChild.of({
        receive: (command) =>
          Match.value(command).pipe(
            Match.tag("Boom", (_command) => Effect.die(new Error("child defect"))),
            Match.exhaustive,
          ),
      }),
    ),
  );
}

const EscalationCommand = Schema.Union([
  Schema.TaggedStruct("Trigger", {}),
  Schema.TaggedStruct("Read", { replyTo: ReplyTo<number>() }),
]);
type EscalationCommand = typeof EscalationCommand.Type;

class EscalatingParent extends Actor.Service<EscalatingParent, CountsService>()(
  "test/EscalatingParent",
  { command: EscalationCommand },
) {
  static readonly layer = Layer.effect(
    EscalatingParent,
    Effect.gen(function* () {
      const counts = yield* CountsService;
      counts.built++;
      let child: ActorRef<{ readonly _tag: "Boom" }>;
      return EscalatingParent.of({
        started: (context) =>
          Effect.gen(function* () {
            child =
              ((yield* context.child("child")) as typeof child | undefined) ??
              (yield* context.spawn("child", BoomChild, { supervision: () => "escalate" }));
          }),
        receive: (command) =>
          Match.value(command).pipe(
            Match.tag("Trigger", (_command) => child.tell({ _tag: "Boom" })),
            Match.tag("Read", (command) => command.replyTo.tell(counts.built)),
            Match.exhaustive,
          ),
      });
    }),
  );
}

test("escalate forwards a child defect into its parent's supervision", async () => {
  const counts: InstanceCounts = { built: 0, closed: 0 };
  const built = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(CountsService, counts)),
        );
        const parentRestart = yield* Stream.runHead(
          Stream.filter(
            system.events,
            (event) => event._tag === "ActorRestarting" && event.path === "/user/escalate",
          ),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const parent = yield* system.spawn("escalate", EscalatingParent);
        yield* parent.tell({ _tag: "Trigger" });
        yield* Fiber.join(parentRestart).pipe(Effect.timeout("2 seconds"));
        return yield* parent.ask((replyTo) => ({ _tag: "Read", replyTo }), "2 seconds");
      }),
    ),
  );
  assert.equal(built, 2);
});

class NeedsPersistence extends PersistentActor.Service<NeedsPersistence>()(
  "test/NeedsPersistence",
  {
    command: Schema.TaggedStruct("Noop", {}),
    event: Schema.Number,
    state: Schema.Number,
  },
) {
  static readonly layer = Layer.effect(
    NeedsPersistence,
    Effect.succeed(
      NeedsPersistence.of({
        initialState: 0,
        applyEvent: (state, event) => state + event,
        receive: (command) =>
          Match.value(command).pipe(
            Match.tag("Noop", (_command) => Effect.void),
            Match.exhaustive,
          ),
      }),
    ),
  );
}

class UndeclaredServiceActor extends Actor.Service<UndeclaredServiceActor>()(
  "test/UndeclaredServiceActor",
  { command: Schema.TaggedStruct("Noop", {}) },
) {
  static readonly layer = Layer.effect(
    UndeclaredServiceActor,
    Effect.gen(function* () {
      yield* NumberService;
      return UndeclaredServiceActor.of({
        receive: (command) =>
          Match.value(command).pipe(
            Match.tag("Noop", (_command) => Effect.void),
            Match.exhaustive,
          ),
      });
    }),
  );
}

class MissingChildServices extends Actor.Service<MissingChildServices>()(
  "test/MissingChildServices",
  { command: Schema.TaggedStruct("Noop", {}) },
) {
  static readonly layer = Layer.effect(
    MissingChildServices,
    Effect.succeed(
      MissingChildServices.of({
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("Noop", (_command) => {
              // @ts-expect-error The parent's Services declaration cannot satisfy this child's dependency.
              void context.spawn("child", ServiceActor);
              return Effect.void;
            }),
            Match.exhaustive,
          ),
      }),
    ),
  );
}

// Keep the fixture type-checked without spawning it at runtime.
void MissingChildServices;

const compileTimeSpawnChecks = Effect.scoped(
  Effect.gen(function* () {
    const system = yield* ActorSystem.make();
    // @ts-expect-error A persistent actor requires ActorPersistence in the system environment.
    yield* system.spawn("persistent", NeedsPersistence);
    // @ts-expect-error A layer's direct input service must be present.
    yield* system.spawn("service", ServiceActor);
    // @ts-expect-error Layer inputs are checked even if an actor omits them from its Services declaration.
    yield* system.spawn("undeclared", UndeclaredServiceActor);
    // @ts-expect-error Services can only be provided while acquiring a system.
    void ActorSystem.provide(Layer.succeed(NumberService, { value: 1 }))(Effect.succeed(system));
    // @ts-expect-error A Layer's input must be supplied by an earlier Layer.
    void ActorSystem.make().pipe(ActorSystem.provide(dependentLayer));
  }),
);
void compileTimeSpawnChecks;
