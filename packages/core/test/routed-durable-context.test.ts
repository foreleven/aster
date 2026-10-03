import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Fiber, Stream } from "effect";
import {
  LocalDurableContext,
  RoutedDurableContext,
  contextBackendFor,
  type ContextRecord,
} from "../src/index.js";

const record: ContextRecord = {
  path: "/personal",
  description: "Personal",
  state: { value: 1 },
  messages: [],
};
const seeded = (records: readonly ContextRecord[]) =>
  LocalDurableContext.fromStore({ loadAll: () => [...records], save: () => {} });
const routes = [{ prefix: "/personal", backend: "pi" as const }];

test("Context routes use longest segment prefixes", () => {
  const rules = [...routes, { prefix: "/personal/local", backend: "local" as const }];
  assert.equal(contextBackendFor("/personal", rules), "pi");
  assert.equal(contextBackendFor("/personal/child", rules), "pi");
  assert.equal(contextBackendFor("/personal/local/child", rules), "local");
  assert.equal(contextBackendFor("/personal-archive", rules), "local");
});

test("router delegates CAS and publishes only selected backend changes", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const local = yield* seeded([]);
        const pi = yield* seeded([]);
        const router = yield* RoutedDurableContext.make({ local, pi }, routes);
        const stream = yield* router.subscribe;
        const observed = yield* stream.pipe(Stream.take(2), Stream.runCollect, Effect.forkScoped);
        yield* local.commit(record, { expectedRevision: 0 }); // A retained copy is not authoritative.
        yield* router.commit(record, { expectedRevision: 0 });
        const conflict = yield* router
          .commit({ ...record, state: { value: 2 } }, { expectedRevision: 0 })
          .pipe(Effect.flip);
        assert.equal(conflict._tag, "ContextConflict");
        yield* router.commit({ ...record, path: "/goals/demo" }, { expectedRevision: 0 });
        const changes = yield* Fiber.join(observed);
        assert.deepEqual(changes.map((change) => change.path).sort(), ["/goals/demo", "/personal"]);
        assert.equal(local.get("/personal")?.revision, 1);
        assert.equal(pi.get("/goals/demo"), undefined);
        assert.deepEqual(Object.keys(router.snapshot()).sort(), ["/goals/demo", "/personal"]);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("router refuses missing, regressed or divergent selected state and invalid routes", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const local = yield* seeded([{ ...record, revision: 2 }]);
      for (const records of [
        [],
        [{ ...record, revision: 1 }],
        [{ ...record, revision: 2, messages: ["different"] }],
      ]) {
        const pi = yield* seeded(records);
        const error = yield* RoutedDurableContext.make({ local, pi }, routes).pipe(Effect.flip);
        assert.equal(error._tag, "ContextRecoveryError");
      }
      for (const rules of [routes, [...routes, ...routes]]) {
        const error = yield* RoutedDurableContext.make({ local }, rules).pipe(Effect.flip);
        assert.equal(error._tag, "ContextRecoveryError");
      }
      const pi = yield* seeded([{ ...record, revision: 3 }]);
      const router = yield* RoutedDurableContext.make({ local, pi }, routes);
      assert.equal(router.get(record.path)?.revision, 3);
      const legacy = yield* seeded([record]);
      const zero = yield* seeded([{ ...record, revision: 0 }]);
      yield* RoutedDurableContext.make({ local: legacy, pi: zero }, routes);
    }),
  );
});
