import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Schema } from "effect";
import { ContextCommitError, makeDurableContext, type StoredContext } from "../src/index.js";
const initial = {
  path: "/local",
  revision: 1,
  description: "Local",
  state: { value: 1 },
  text: "1",
  messages: [],
};
const definition = {
  path: "/local",
  state: Schema.Struct({ value: Schema.Int }),
  message: Schema.String,
  serialize: (state: { value: number }) => String(state.value),
};

test("recovery rejects missing, regressed and invalid owner snapshots", async () => {
  for (const restored of [
    [],
    [{ snapshot: { ...initial, revision: 0 }, events: [] }],
    [{ snapshot: { ...initial, revision: 2, state: { value: "invalid" } }, events: [] }],
  ]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          let stored: readonly StoredContext[] = [{ snapshot: initial, events: [] }];
          const durable = yield* makeDurableContext({
            load: Effect.sync(() => stored),
            save: () =>
              Effect.fail(
                new ContextCommitError({ path: "/local", cause: new Error("Unknown outcome") }),
              ),
          });
          yield* Effect.scoped(
            Effect.gen(function* () {
              const owner = yield* durable.open(definition);
              assert.equal(
                (yield* owner.state.update(() => ({ value: 2 })).pipe(Effect.flip))._tag,
                "ContextCommitError",
              );
            }),
          );
          // StoredContext validates structure; owner schemas validate domain types during open.
          stored = Schema.decodeUnknownSync(
            Schema.Array(
              Schema.Struct({
                snapshot: Schema.Struct({ ...initialFields }),
                events: Schema.Array(Schema.Never),
              }),
            ),
          )(restored);
          assert.equal(
            (yield* Effect.scoped(durable.open(definition)).pipe(Effect.flip))._tag,
            "ContextRecoveryError",
          );
          assert.equal(durable.get("/local")?.revision, 1);
        }),
      ),
    );
  }
});
const initialFields = {
  path: Schema.String,
  revision: Schema.Int,
  description: Schema.String,
  state: Schema.ObjectKeyword,
  text: Schema.String,
  messages: Schema.Array(Schema.Unknown),
};

test("typed owners validate before invoking the injected persistence adapter", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const writes: StoredContext[] = [];
        const durable = yield* makeDurableContext({
          load: Effect.succeed([]),
          save: (record) =>
            Effect.sync(() => {
              writes.push(record);
            }),
        });
        const owner = yield* durable.open({
          ...definition,
          initial: { description: "Local", state: { value: 1 } },
        });
        assert.equal((yield* owner.messages.append(["accepted"])).revision, 2);
        assert.equal(
          (yield* owner.state
            .set(
              Schema.decodeUnknownSync(Schema.Struct({ value: Schema.Unknown }))({
                value: "bad",
              }) as { value: number },
            )
            .pipe(Effect.flip))._tag,
          "ContextValidationError",
        );
        assert.equal(writes.length, 2);
        assert.deepEqual(writes[1]?.snapshot.messages, ["accepted"]);
        assert.deepEqual(durable.get("/local"), {
          path: "/local",
          revision: 2,
          description: "Local",
          text: "1",
        });
      }),
    ),
  );
});

test("DurableContext rejects duplicate identities and invalid stored paths", async () => {
  for (const values of [
    [initial, initial],
    [{ ...initial, path: "/../outside" }],
    [{ ...initial, path: "/trailing/" }],
  ]) {
    const error = await Effect.runPromise(
      Effect.scoped(
        makeDurableContext({
          load: Effect.succeed(values.map((snapshot) => ({ snapshot, events: [] }))),
          save: () => Effect.void,
        }).pipe(Effect.flip),
      ),
    );
    assert.equal(error._tag, "ContextRecoveryError");
  }
});
