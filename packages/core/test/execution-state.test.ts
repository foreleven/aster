import { taskInput } from "./task-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Fiber, Layer, Schema, Stream } from "effect";
import { ActorSystem, ActorTestKit } from "@aster/actor";
import {
  ContextRegistry,
  DelegationActor,
  type DelegationUpdate,
  DelegationState,
  ExternalAgents,
  RunState,
  TaskRunActor,
  contextSpawnOptions,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { fakeAgent } from "./fixtures.js";

const task = { instructions: "Read evidence", input: [] };
const run = {
  admission: { input: taskInput(), receipt: { requestId: "task", revision: 1 } },
  executorPrompt: "Test policy",
};
const delegation = {
  request: { runPath: "/runs/test", agent: "test", task },
  requests: {},
  responses: {},
};

test("every Run requires a frozen admitted Task and accepts only execution phases", () => {
  const decode = Schema.decodeUnknownSync(RunState);
  for (const status of ["awaiting-confirmation", "ready", "running", "completed", "cancelled"])
    assert.deepEqual(decode({ ...run, status }).admission.input.task, taskInput().task);
  for (const status of ["preparing", "checking", "typo"])
    assert.throws(() => decode({ ...run, status }));
  assert.throws(() => decode({ status: "completed" }));
});

test("Delegation distinguishes ambiguous submission from sessions and completed results", () => {
  const decode = Schema.decodeUnknownSync(DelegationState);
  assert.equal(
    decode({ ...delegation, status: "uncertain", error: "Connection lost" }).session,
    undefined,
  );
  for (const status of ["running", "waiting_input", "failed", "completed"])
    assert.throws(() => decode({ ...delegation, status, result: "Done", error: "Failed" }));
  const session = { sessionId: "s" };
  assert.throws(() => decode({ ...delegation, status: "completed", session }));
  assert.equal(
    decode({ ...delegation, status: "completed", session, result: "Done" }).result,
    "Done",
  );
});

test("malformed restored Run and Delegation state stop before external execution and remain intact", async () => {
  for (const kind of ["run", "delegation"]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const record = {
            path: "/restored",
            description: "Invalid persisted state",
            messages: [],
            state:
              kind === "run"
                ? { ...run, status: "checking" }
                : { ...delegation, status: "running" },
          };
          let saves = 0,
            calls = 0;
          const registry = yield* makeContextRegistry({
            loadAll: () => [record],
            save: () => {
              saves++;
            },
          });
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),

              Layer.succeed(ExternalAgents, {
                test: fakeAgent({
                  submit: () =>
                    Effect.sync(() => {
                      calls++;
                      return { sessionId: "unexpected" };
                    }),
                  status: () =>
                    Effect.sync(() => {
                      calls++;
                      return { state: "running" };
                    }),
                }),
              }),
            ),
          );
          const stopped = yield* Stream.runHead(
            Stream.filter(
              system.events,
              (e) => e._tag === "ActorStopped" && e.path === "/user/restored",
            ),
          ).pipe(Effect.forkScoped);
          yield* Effect.yieldNow;
          const options = contextSpawnOptions(record.path, { supervision: () => "stop" });
          if (kind === "run") yield* system.spawn("restored", TaskRunActor, options);
          else yield* system.spawn("restored", DelegationActor, options);
          const event = yield* Fiber.join(stopped).pipe(Effect.timeout("2 seconds"));
          assert.equal(event._tag, "Some");
          if (event._tag === "Some" && event.value._tag === "ActorStopped")
            assert.ok(event.value.cause);
          assert.equal(calls, 0);
          assert.equal(saves, 0);
          assert.deepEqual(registry.get(record.path), { ...record, revision: 0 });
        }),
      ),
    );
  }
});

test("restored terminal Delegations replay results without a configured executor, including parent reattachment", async () => {
  for (const status of ["completed", "failed", "cancelled", "unknown"] as const) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const saved = {
            path: "/delegations/test",
            description: "Finished execution",
            messages: [],
            state: {
              ...delegation,
              replyPath: "/retired-parent",
              session: { sessionId: "original" },
              status,
              ...(status === "completed"
                ? { result: "Original result" }
                : { error: `Original ${status}` }),
            },
          };
          let saves = 0;
          const registry = yield* makeContextRegistry({
            loadAll: () => [saved],
            save: () => {
              saves++;
            },
          });
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(ExternalAgents, {}),
            ),
          );
          const actor = yield* system.spawn(
            "restored",
            DelegationActor,
            contextSpawnOptions(saved.path),
          );
          for (let attachment = 0; attachment < 2; attachment++) {
            const parent = yield* ActorTestKit.probe<DelegationUpdate>();
            yield* actor.tell({
              _tag: "Start",
              request: delegation.request,
              replyTo: parent.ref,
              recovering: true,
            });
            const update = yield* parent.take().pipe(Effect.timeout("2 seconds"));
            assert.equal(update._tag, "Finished");
            if (status === "completed")
              assert.deepEqual(update.outcome, { _tag: "Completed", text: "Original result" });
            else {
              assert.equal(
                update.outcome._tag,
                { failed: "Failed", cancelled: "Cancelled", unknown: "Uncertain" }[status],
              );
              assert.equal(update.outcome.text, `Original ${status}`);
            }
          }
          assert.equal(saves, 0);
          assert.deepEqual(registry.get(saved.path), { ...saved, revision: 0 });
        }),
      ),
    );
  }
});

test("Delegation commits authoritative failure before acknowledging its parent", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ExternalAgents, {
              test: fakeAgent({
                status: () => Effect.succeed({ state: "failed", error: "Executor failed" }),
              }),
            }),
          ),
        );
        const parent = yield* ActorTestKit.probe<DelegationUpdate>();
        const actor = yield* system.spawn(
          "delegation",
          DelegationActor,
          contextSpawnOptions("/delegations/test"),
        );
        yield* actor.tell({
          _tag: "Start",
          request: delegation.request,
          replyTo: {
            path: parent.ref.path,
            incarnation: parent.ref.incarnation,
            ask: (command, timeout) => parent.ref.ask(command, timeout),
            tell: (update) =>
              Effect.gen(function* () {
                if (update._tag === "Finished")
                  assert.equal(
                    Schema.decodeUnknownSync(DelegationState)(
                      registry.get("/delegations/test")!.state,
                    ).status,
                    "failed",
                  );
                yield* parent.ref.tell(update);
              }),
          },
        });
        assert.equal((yield* parent.take().pipe(Effect.timeout("2 seconds")))._tag, "Submitted");
        const result = yield* parent.take().pipe(Effect.timeout("2 seconds"));
        assert.equal(result._tag, "Finished");
        assert.equal(result.outcome._tag, "Failed");
      }),
    ),
  );
});
