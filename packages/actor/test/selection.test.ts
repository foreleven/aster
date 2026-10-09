import { Effect, Match, Schema } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Actor, actorSelectionPath, ActorSystem, ReplyTo } from "../src/index.js";
const Command = Schema.TaggedUnion({
  Read: { replyTo: ReplyTo<string>() },
  Sibling: { replyTo: ReplyTo<string>() },
});
const Selected = Actor.define("test/Selected", {
  commands: Object.values(Command.cases),
})(
  Effect.succeed({
    receive: (command, context) =>
      Match.value(command).pipe(
        Match.tag("Read", ({ replyTo }) => replyTo.tell(context.self.incarnation)),
        Match.tag("Sibling", ({ replyTo }) =>
          context
            .select("../target")
            .resolve()
            .pipe(
              Effect.flatMap((ref) => replyTo.tell(ref.path)),
              Effect.orDie,
            ),
        ),
        Match.exhaustive,
      ),
  }),
);
test("selection resolves a stable address to a new incarnation after system recreation", async () => {
  const first = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make();
        const ref = yield* system.spawn("target", Selected);
        const selection = system.select("/user/target");
        assert.equal((yield* selection.resolve()).incarnation, ref.incarnation);
        const sibling = yield* system.spawn("sibling", Selected);
        assert.equal(
          yield* sibling.ask<string>((replyTo) => ({ _tag: "Sibling", replyTo })),
          "/user/target",
        );
        return { path: selection.path, incarnation: ref.incarnation };
      }),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const system = yield* ActorSystem.make();
        const selection = system.select(first.path);
        assert.equal((yield* Effect.result(selection.resolve()))._tag, "Failure");
        const ref = yield* system.spawn("target", Selected);
        assert.equal((yield* selection.resolve()).incarnation, ref.incarnation);
        assert.notEqual(ref.incarnation, first.incarnation);
      }),
    ),
  );
  assert.equal(actorSelectionPath("../target", "/user/sibling"), "/user/target");
  assert.throws(() => actorSelectionPath("../../target", "/user"));
  assert.throws(() => actorSelectionPath("/user/*"));
});
