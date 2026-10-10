import assert from "node:assert/strict";
import { test } from "node:test";
import { Deferred, Effect, Fiber, Logger } from "effect";
import { TestClock } from "effect/testing";
import { choice } from "@aster/core";
import { systemOne } from "./http-fixtures.js";
const config = {
  url: "https://system-one.test?ak=PRIVATE_QUERY",
  model: "test",
  apiKey: "PRIVATE_KEY",
};
const request = {
  state: "PRIVATE_EVIDENCE",
  questions: { intent: choice("Question", { yes: "Yes" }) },
};
const success = () => Response.json({ answers: { intent: { type: "choice", choice: "yes" } } });
test("Effect HTTP retries transient decisions and retains isolated, credential-safe logs", async () => {
  const logs: unknown[] = [];
  let attempts = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const client = yield* systemOne(config, async () =>
          ++attempts < 2
            ? Response.json({}, { status: 503, headers: { "retry-after-ms": "0" } })
            : success(),
        );
        assert.equal((yield* client.systemOne(request)).answers.intent?.choice, "yes");
      }).pipe(
        Effect.provide(
          Logger.layer([
            Logger.make((entry) => {
              logs.push(entry.message);
            }),
          ]),
        ),
      ),
    ),
  );
  assert.equal(attempts, 2);
  assert.doesNotMatch(JSON.stringify(logs), /PRIVATE_QUERY|PRIVATE_KEY|PRIVATE_EVIDENCE/);
});

test("network failures retain safe codes while hiding native diagnostic payloads", async () => {
  let attempts = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const client = yield* systemOne(config, async () => {
          attempts++;
          throw Object.assign(new Error("PRIVATE_QUERY PRIVATE_KEY PRIVATE_EVIDENCE"), {
            cause: { code: "ECONNREFUSED" },
          });
        });
        const requestFiber = yield* client.systemOne(request).pipe(Effect.flip, Effect.forkScoped);
        yield* TestClock.adjust(10000);
        const error = yield* Fiber.join(requestFiber);
        assert.match(error.message, /ECONNREFUSED/);
        assert.doesNotMatch(error.message, /PRIVATE_/);
        assert.equal(attempts, 3);
      }).pipe(Effect.provide(TestClock.layer())),
    ),
  );
});
test("malformed answers and ordinary HTTP failures are not retried", async () => {
  for (const response of [
    () => Response.json({ answers: {} }),
    () => Response.json({}, { status: 401 }),
  ]) {
    let attempts = 0;
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* systemOne(config, async () => {
          attempts++;
          return response();
        });
        assert.equal((yield* client.systemOne(request).pipe(Effect.result))._tag, "Failure");
      }),
    );
    assert.equal(attempts, 1);
  }
});
test("interrupting an HTTP decision aborts transport without resubmitting", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        let aborted = false;
        let attempts = 0;
        const client = yield* systemOne(config, (_input, init) => {
          attempts++;
          Deferred.doneUnsafe(entered, Effect.void);
          return new Promise<Response>((_, reject) =>
            init?.signal?.addEventListener(
              "abort",
              () => {
                aborted = true;
                reject(init.signal?.reason);
              },
              { once: true },
            ),
          );
        });
        const fiber = yield* client.systemOne(request).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
        assert.equal(aborted, true);
        assert.equal(attempts, 1);
      }),
    ),
  );
});
