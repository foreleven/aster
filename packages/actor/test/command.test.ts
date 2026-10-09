import { Cause, Context, Effect, Exit, Layer, Match, Schema } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Actor, ActorSystem, Command, type ActorRef, type ServicesOf } from "../src/index.js";

class LookupError extends Schema.TaggedError<LookupError>()("LookupError", { id: Schema.String }) {}
class Lookup extends Command.Class<Lookup>()("Lookup", {
  payload: { id: Schema.NonEmptyString },
  success: Schema.Number,
  error: LookupError,
  description: "Read a number by id.",
}) {}
class Stop extends Command.Class<Stop>()("Stop", { payload: {} }) {}
class Multiplier extends Context.Service<Multiplier, number>()("test/command/Multiplier") {}
const Completed = Schema.TaggedStruct("Completed", { value: Schema.Number });
const Reader = Actor.define("test/command/Reader", {
  commands: [Lookup, Stop],
  internal: Completed,
})(
  Effect.gen(function* () {
    const multiplier = yield* Multiplier;
    return {
      receive: (message, actor) =>
        Match.value(message).pipe(
          Match.tag("Lookup", ({ id, replyTo }) =>
            replyTo.tell(
              id === "known"
                ? { _tag: "Success", value: multiplier * 2 }
                : { _tag: "Failure", error: new LookupError({ id }) },
            ),
          ),
          Match.tag("Completed", () => Effect.void),
          Match.tag("Stop", () => actor.stopSelf()),
          Match.exhaustive,
        ),
    };
  }),
);
const Parent = Actor.define("test/command/Parent", { commands: [Stop] })(
  Effect.succeed({
    started: (actor) => actor.spawn("reader", Reader).pipe(Effect.asVoid),
    receive: (_, actor) => actor.stopSelf(),
  }),
);

// Compile-time assertions complement the real runtime tests below.
const typeChecks = (
  ref: ActorRef<Lookup | Stop>,
  system: ActorSystem,
  service: ServicesOf<typeof Parent>,
) => {
  const valid = ref.tell(new Stop());
  // @ts-expect-error Internal mailbox messages are not public commands.
  const internal = ref.tell({ _tag: "Completed", value: 1 });
  // @ts-expect-error Child acquisition dependencies propagate into the parent definition.
  const missing = system.spawn("parent", Parent);
  const dependency: Multiplier = service;
  return { dependency, valid, internal, missing };
};
void typeChecks;

test("Command classes retain payload validation, tags and typed replies without a command map", async () => {
  assert.equal(Lookup._tag, "Lookup");
  assert.equal(Command.isContract(Lookup), true);
  assert.deepEqual(Object.keys(Lookup.payloadSchema.fields), ["id"]);
  assert.equal(Schema.decodeUnknownOption(Lookup.payloadSchema)({ id: "" })._tag, "None");
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(Multiplier, 3)),
        );
        const ref = yield* system.spawn("reader", Reader);
        yield* ref.awaitStarted;
        assert.deepEqual(
          yield* ref.ask<Command.Reply<typeof Lookup>>(
            (replyTo) => new Lookup({ id: "known", replyTo }),
          ),
          { _tag: "Success", value: 6 },
        );
        const missing = yield* ref.ask<Command.Reply<typeof Lookup>>(
          (replyTo) => new Lookup({ id: "missing", replyTo }),
        );
        assert.equal(missing._tag, "Failure");
        if (missing._tag === "Failure") assert.equal(missing.error.id, "missing");
        yield* (yield* system.spawn("parent", Parent)).awaitStarted;
      }),
    ),
  );
});

test("duplicate public tags and public/internal collisions fail Actor startup", async () => {
  class OtherLookup extends Command.Class<OtherLookup>()("Lookup", { payload: {} }) {}
  for (const protocol of [
    { commands: [Lookup, OtherLookup] },
    { commands: [Lookup], internal: Schema.TaggedStruct("Lookup", {}) },
  ]) {
    const Invalid = Actor.define(
      "test/command/Invalid",
      protocol,
    )(Effect.succeed({ receive: () => Effect.void }));
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const system = yield* ActorSystem.make();
          const ref = yield* system.spawn("invalid", Invalid);
          const exit = yield* Effect.exit(ref.awaitStarted);
          assert.ok(Exit.isFailure(exit));
          assert.match(String(Cause.squash(exit.cause)), /Duplicate Actor command tag: Lookup/);
        }),
      ),
    );
  }
});
