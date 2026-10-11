import { ContextQueryError } from "../src/queries/contracts.js";
import { ContextQueries } from "../src/queries/registry.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Deferred, Effect, Exit, Fiber, Schema, Scope } from "effect";
import { listContexts, describeContext } from "../src/tools/task/contexts.js";
import { toolSystem } from "./tool-fixtures.js";

test("Context discovery lists capabilities without state and descriptions match strict execution schemas", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const { queries } = yield* toolSystem();
        for (let i = 0; i < 31; i++)
          yield* queries.register(
            `/mail/${i}`,
            {
              description: `Mailbox ${i}`,
              commands: {
                read: {
                  description: "Read one email",
                  schema: Schema.Struct({ id: Schema.String }),
                  success: Schema.Json,
                  error: Schema.Never,
                },
              },
            },
            (input) => Effect.succeed({ ...input, queriedAt: "now", data: "PRIVATE BODY" }),
          );
        const list = listContexts();
        const describe = describeContext();
        const found = yield* list!
          .execute("list", { parent: "/mail" })
          .pipe(Effect.provideService(ContextQueries, queries));
        const page = JSON.parse(
          found.content.map((c) => (c.type === "text" ? c.text : "")).join(""),
        );
        assert.equal(page.total, 31);
        assert.equal(page.items.length, 20);
        assert.equal(page.nextOffset, 20);
        assert.doesNotMatch(JSON.stringify(page), /PRIVATE|state/);
        const description = yield* describe!
          .execute("describe", { path: "/mail/0" })
          .pipe(Effect.provideService(ContextQueries, queries));
        assert.match(JSON.stringify(description), /Read one email/);
        assert.doesNotMatch(JSON.stringify(description), /PRIVATE/);
        const invalidInputs: import("../src/queries/contracts.js").ContextQueryInput[] = [
          { path: "/mail/0", command: "state", args: {} },
          { path: "/mail/0", command: "read", args: { id: 3 } },
          { path: "/mail/0", command: "read", args: { id: "1", extra: true } },
        ];
        for (const input of invalidInputs)
          assert.equal(
            Schema.decodeUnknownSync(ContextQueryError)(yield* Effect.flip(queries.query(input)))
              .kind,
            "invalid-input",
          );
        assert.equal(
          Schema.decodeUnknownSync(Schema.Struct({ data: Schema.String }))(
            yield* queries.query({ path: "/mail/0", command: "read", args: { id: "1" } }),
          ).data,
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
              commands: {
                read: {
                  description: "Read",
                  schema: Schema.Struct({}),
                  success: Schema.Json,
                  error: Schema.Never,
                },
              },
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
        assert.equal(
          Schema.decodeUnknownSync(ContextQueryError)(yield* Fiber.join(caller)).kind,
          "unavailable",
        );
        assert.equal((yield* queries.list()).total, 0);
        assert.equal(
          Schema.decodeUnknownSync(ContextQueryError)(
            yield* Effect.flip(queries.describe("/source")),
          ).kind,
          "unavailable",
        );
      }),
    ).pipe(Effect.timeout("10 seconds")),
  );
});

test("Context argument diagnostics report declared fields and types without rejected values or keys", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const { queries } = yield* toolSystem();
        yield* queries.register(
          "/apps/safe",
          {
            description: "Test",
            commands: {
              read: {
                description: "Read",
                schema: Schema.Struct({ query: Schema.String, limit: Schema.optional(Schema.Int) }),
                success: Schema.String,
                error: Schema.Never,
              },
            },
          },
          () => Effect.die("Invalid arguments must not execute"),
        );
        const request = (args: import("../src/queries/contracts.js").ContextQueryInput["args"]) =>
          queries.query({ path: "/apps/safe", command: "read", args });
        const missing = Schema.decodeUnknownSync(ContextQueryError)(
          yield* Effect.flip(request({})),
        );
        assert.match(missing.message, /query \(required\)/);
        const invalid = Schema.decodeUnknownSync(ContextQueryError)(
          yield* Effect.flip(
            request({ query: "PRIVATE_SOURCE_CONTENT", limit: "PRIVATE_REJECTED_VALUE" }),
          ),
        );
        assert.match(invalid.message, /limit \(expected integer\)/);
        assert.doesNotMatch(JSON.stringify(invalid), /PRIVATE/);
        const excess = Schema.decodeUnknownSync(ContextQueryError)(
          yield* Effect.flip(
            request({
              query: "PRIVATE_SOURCE_CONTENT",
              PRIVATE_UNKNOWN_KEY: "PRIVATE_REJECTED_VALUE",
            }),
          ),
        );
        assert.match(excess.message, /arguments \(unknown field\)/);
        assert.doesNotMatch(JSON.stringify(excess), /PRIVATE/);
      }),
    ),
  );
});
