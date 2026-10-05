import { personalDisabled } from "./workflow-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem, ActorTestKit } from "@aster/actor";
import { Effect, Layer, Option, Schema } from "effect";
import {
  ContextRegistry,
  DelegationActor,
  DelegationState,
  ExternalAgents,
  ExternalAgentError,
  PersonalActions,
  PersonalAgentActor,
  contextSpawnOptions,
  makeApplicationApi,
  type ContextRecord,
  type DelegationUpdate,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { fakeAgent } from "./fixtures.js";

const request = {
  runPath: "/signals/watch/runs/execution",
  agent: "test",
  task: { instructions: "Read evidence", input: [{ content: "Evidence", sources: ["/source"] }] },
};
const retained = (): ContextRecord => ({
  path: "/delegations/execution",
  description: "Retained execution",
  revision: 2,
  messages: [],
  state: {
    request,
    status: "uncertain",
    error: "Handle acknowledgement was lost",
    requests: {},
    responses: {},
  },
});

for (const result of ["found", "missing", "unsupported", "failed"] as const) {
  test(`Delegation recovery with ${result} admission never submits a replacement`, async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const records = new Map([[retained().path, retained()]]);
          const registry = yield* makeContextRegistry({
            loadAll: () => [...records.values()],
            save: (record) => {
              records.set(record.path, structuredClone(record));
            },
          });
          let lookups = 0;
          const agent = fakeAgent({
            submit: () => Effect.die(new Error("Recovery must not submit")),
            resume: () => Effect.die(new Error("Inspection must not resume failed work")),
            ...(result === "unsupported"
              ? {}
              : {
                  lookupSubmission: (
                    task: typeof request.task,
                    submission: { requestId: string },
                  ) =>
                    Effect.gen(function* () {
                      lookups++;
                      assert.deepEqual(task, request.task);
                      assert.equal(submission.requestId, "/delegations/execution");
                      if (result === "failed")
                        return yield* new ExternalAgentError({
                          operation: "lookup",
                          message: "Admission unavailable",
                        });
                      return result === "found"
                        ? Option.some({ sessionId: "retained-session", runId: "retained-run" })
                        : Option.none();
                    }),
                }),
            status: () =>
              Effect.succeed({ state: "completed", result: { text: "Existing result" } }),
          });
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(ExternalAgents, { test: agent }),
            ),
          );
          const parent = yield* ActorTestKit.probe<DelegationUpdate>();
          const actor = yield* system.spawn(
            "delegation",
            DelegationActor,
            contextSpawnOptions(retained().path),
          );
          yield* actor.tell({ _tag: "Start", request, replyTo: parent.ref, recovering: true });
          const first = yield* parent.take();
          const saved = () =>
            Schema.decodeUnknownSync(DelegationState)(records.get(retained().path)!.state);
          if (result === "found") {
            assert.equal(first._tag, "Submitted");
            assert.equal(
              saved().session?.sessionId,
              "retained-session",
              "the recovered handle commits before notification",
            );
            assert.equal(
              saved().error,
              undefined,
              "resolved uncertainty does not remain a current error",
            );
            const finished = yield* parent.take();
            assert.deepEqual(finished, {
              _tag: "Finished",
              outcome: { _tag: "Completed", text: "Existing result" },
            });
            assert.equal(saved().status, "completed");
          } else {
            assert.equal(first._tag, "Finished");
            if (first._tag === "Finished") assert.equal(first.outcome._tag, "Uncertain");
            assert.equal(saved().status, "uncertain");
            assert.equal(saved().session, undefined);
          }
          assert.equal(lookups, result === "unsupported" ? 0 : 1);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
}

test("Personal inspects retained Delegation business data without provider metadata or executor calls", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const record: ContextRecord = {
          ...retained(),
          state: {
            ...retained().state,
            status: "waiting_input",
            session: { sessionId: "id", metadata: { credential: "private-token" } },
            requests: {
              approval: {
                id: "provider-request",
                kind: "approval",
                prompt: "Allow work?",
                metadata: { token: "private-token" },
              },
            },
          },
          messages: [{ type: "native-frame", token: "private-token" }],
        };
        const registry = yield* makeContextRegistry({ loadAll: () => [record], save: () => {} });
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            PersonalActions.unavailable,
            personalDisabled,
          ),
        );
        const personal = yield* system.spawn("personal", PersonalAgentActor);
        const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
        const view = yield* api.personal.inspectDelegation(record.path);
        assert.deepEqual(view, {
          path: record.path,
          revision: 2,
          runPath: request.runPath,
          agent: "test",
          status: "waiting_input",
          instructions: "Read evidence",
          sources: ["/source"],
          hasExecution: true,
          error: "Handle acknowledgement was lost",
          requests: [
            { id: "approval", kind: "approval", prompt: "Allow work?", responseStatus: "pending" },
          ],
        });
        assert.doesNotMatch(JSON.stringify(view), /private-token|metadata|native-frame/);
        assert.equal(
          (yield* api.personal.inspectDelegation("/personal").pipe(Effect.flip)).kind,
          "invalid-input",
        );
        assert.equal(
          (yield* api.personal.inspectDelegation("/delegations/missing").pipe(Effect.flip)).kind,
          "not-found",
        );
        assert.equal(registry.get(record.path)?.revision, 2);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
