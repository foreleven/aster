import { CurrentActors } from "../src/services/actors.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Deferred, Effect, Exit, Fiber, Schema, Scope } from "effect";
import { contextTools } from "../src/tools/catalogues.js";
import { toolSystem } from "./tool-fixtures.js";

test("Context discovery lists capabilities without state and descriptions match strict execution schemas", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const { system, queries } = yield* toolSystem();
        for (let i = 0; i < 31; i++)
          yield* queries.register(
            `/mail/${i}`,
            {
              description: `Mailbox ${i}`,
              commands: {
                read: {
                  description: "Read one email",
                  schema: Schema.Struct({ id: Schema.String }),
                },
              },
            },
            (input) => Effect.succeed({ ...input, queriedAt: "now", data: "PRIVATE BODY" }),
          );
        const [list, describe] = contextTools();
        const found = yield* list!
          .execute("list", { parent: "/mail" })
          .pipe(Effect.provideService(CurrentActors, system));
        const page = JSON.parse(
          found.content.map((c) => (c.type === "text" ? c.text : "")).join(""),
        );
        assert.equal(page.total, 31);
        assert.equal(page.items.length, 20);
        assert.equal(page.nextOffset, 20);
        assert.doesNotMatch(JSON.stringify(page), /PRIVATE|state/);
        const description = yield* describe!
          .execute("describe", { path: "/mail/0" })
          .pipe(Effect.provideService(CurrentActors, system));
        assert.match(JSON.stringify(description), /Read one email/);
        assert.doesNotMatch(JSON.stringify(description), /PRIVATE/);
        const invalidInputs: import("../src/context/contracts.js").ContextQueryInput[] = [
          { path: "/mail/0", command: "state", args: {} },
          { path: "/mail/0", command: "read", args: { id: 3 } },
          { path: "/mail/0", command: "read", args: { id: "1", extra: true } },
        ];
        for (const input of invalidInputs)
          assert.equal((yield* Effect.flip(queries.query(input))).kind, "invalid-input");
        assert.equal(
          (yield* queries.query({ path: "/mail/0", command: "read", args: { id: "1" } })).data,
          "PRIVATE BODY",
        );
        assert.equal((yield* queries.list("/mai")).total, 0);
      }),
    ),
  );
});

test("Context query registration owns active handlers and cancels them on shutdown", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const { queries } = yield* toolSystem();
        const owner = yield* Scope.make();
        const entered = yield* Deferred.make<void>();
        const cancelled = yield* Deferred.make<void>();
        yield* queries
          .register(
            "/source",
            {
              description: "Source",
              commands: { read: { description: "Read", schema: Schema.Struct({}) } },
            },
            () =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(cancelled, undefined)),
              ),
          )
          .pipe(Effect.provideService(Scope.Scope, owner));
        const caller = yield* queries
          .query({ path: "/source", command: "read", args: {} })
          .pipe(Effect.flip, Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* Scope.close(owner, Exit.void);
        yield* Deferred.await(cancelled);
        assert.equal((yield* Fiber.join(caller)).kind, "unavailable");
        assert.equal((yield* queries.list()).total, 0);
        assert.equal((yield* Effect.flip(queries.describe("/source"))).kind, "unavailable");
      }),
    ).pipe(Effect.timeout("10 seconds")),
  );
});
