import assert from "node:assert/strict";
import { test } from "node:test";
import { Cause, Deferred, Effect, Exit, Fiber } from "effect";
import { ExternalAgentError } from "@aster/core";
import { adaptExternalAgent, type ExternalAgentDriver } from "../src/external-agent.js";

const driver = (overrides: Partial<ExternalAgentDriver>): ExternalAgentDriver => ({
  followUp: async (session) => session,
  capabilities: "Test executor",
  submit: async () => ({ sessionId: "original" }),
  status: async () => ({ state: "running" }),
  resume: async (session) => session,
  wait: async () => ({ state: "completed", result: { text: "Done" } }),
  respond: async () => {},
  ...overrides,
});

test("execution effects are lazy and preserve operation and cause without retrying ambiguous submissions", async () => {
  let submissions = 0;
  const cause = new Error("Receipt lost after acceptance");
  const agent = adaptExternalAgent(
    driver({
      submit: async () => {
        submissions++;
        throw cause;
      },
    }),
  );
  const submission = agent.submit({ instructions: "Review", input: [] });
  assert.equal(submissions, 0);
  const error = await Effect.runPromise(Effect.flip(submission));
  assert.ok(error instanceof ExternalAgentError);
  assert.equal(error.operation, "submit");
  assert.equal(error.cause, cause);
  assert.equal(submissions, 1);
});

test("interrupting execution forwards cancellation to transport and preserves Fiber interruption", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        let signal: AbortSignal | undefined;
        const agent = adaptExternalAgent(
          driver({
            wait: (_session, currentSignal) => {
              signal = currentSignal;
              Deferred.doneUnsafe(entered, Exit.void);
              return new Promise((_resolve, reject) =>
                currentSignal.addEventListener("abort", () => reject(currentSignal.reason), {
                  once: true,
                }),
              );
            },
          }),
        );
        const waiting = yield* agent.wait({ sessionId: "original" }).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(waiting);
        const exit = yield* Fiber.await(waiting);
        assert.ok(signal?.aborted);
        assert.ok(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause));
      }),
    ),
  );
});

test("external adapter forwards stable submission identity and explicit cancellation confirmation", async () => {
  const session = { sessionId: "original" };
  const identity = { requestId: "task-identity" };
  const agent = adaptExternalAgent(
    driver({
      submit: async (_task, signal, submission) => {
        assert.equal(signal.aborted, false);
        assert.deepEqual(submission, identity);
        return session;
      },
      cancel: async (current, signal) => {
        assert.deepEqual(current, session);
        assert.equal(signal.aborted, false);
        return true;
      },
    }),
  );
  assert.deepEqual(
    await Effect.runPromise(agent.submit({ instructions: "Review", input: [] }, identity)),
    session,
  );
  assert.equal(await Effect.runPromise(agent.cancel!(session)), true);
  assert.equal(adaptExternalAgent(driver({})).cancel, undefined);
});
