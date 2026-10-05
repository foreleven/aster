import { taskExecutionLayer, personalDisabled } from "./workflow-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { ApplicationError, PersonalState, type PersonalResumeRunInput } from "@aster/api-contracts";
import { Deferred, Effect, Layer, Schema, Stream } from "effect";
import {
  ApprovalQueueActor,
  ContextRegistry,
  ExternalAgents,
  ExternalAgentError,
  PersonalActions,
  PersonalAgentActor,
  RunRootActor,
  makeApplicationApi,
  type ContextRecord,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { personalTaskIntent } from "../src/tasks/admission.js";
import { RunState } from "../src/tasks/run-state.js";
import { DelegationState } from "../src/delegation/state.js";
import { fakeAgent } from "./fixtures.js";

const task = {
  instructions: "Continue release analysis",
  input: [{ content: "Frozen evidence", sources: ["/source"] }],
};
const intent = personalTaskIntent(
  { agent: "test", task },
  { requestId: "initial-task", causationId: "user-task", createdAt: "2026-10-02T00:00:00Z" },
);
const runPath = intent.target;
const delegationPath = `/delegations/${runPath.split("/").at(-1)}`;
const session = { sessionId: "original-session", runId: "original-run" };
const resumeInput = (revision = 4): PersonalResumeRunInput => ({
  requestId: "resume-1",
  causationId: "user-resume",
  expectedRevision: 1,
  runPath,
  runRevision: revision,
});
const seed = () =>
  new Map<string, ContextRecord>([
    [
      runPath,
      {
        path: runPath,
        description: "Retained failed Run",
        revision: 4,
        messages: [],
        state: {
          admission: { input: intent, receipt: { requestId: intent.requestId, revision: 1 } },
          signalSlug: runPath.split("/").at(-1),
          definition: {
            slug: runPath.split("/").at(-1),
            when: "Explicit Task",
            task: task.instructions,
            agent: "test",
            mode: "confirm",
          },
          sourcePath: "/personal",
          source: { path: "/personal", description: "Personal", state: {}, messages: [] },
          task,
          approvals: [`${runPath}:confirm`],
          status: "failed",
          outcomeText: "Executor interrupted",
        },
      },
    ],
    [
      delegationPath,
      {
        path: delegationPath,
        description: "Retained execution",
        revision: 3,
        messages: [],
        state: {
          request: { runPath, agent: "test", task },
          replyPath: `/user${runPath}`,
          session,
          status: "failed",
          error: "Executor interrupted",
          requests: {},
          responses: {},
        },
      },
    ],
  ]);
const fixture = (
  records: Map<string, ContextRecord>,
  agent: ReturnType<typeof fakeAgent>,
  loseAck = false,
) =>
  Effect.gen(function* () {
    const registry = yield* makeContextRegistry({
      loadAll: () => [...records.values()],
      save: (record) => {
        records.set(record.path, structuredClone(record));
      },
    });
    const agents = Layer.succeed(ExternalAgents, { test: agent });
    const actions = yield* PersonalActions.pipe(
      Effect.provide(PersonalActions.layer.pipe(Layer.provide(agents))),
    );
    const system = yield* ActorSystem.make().pipe(
      ActorSystem.provide(
        agents,
        Layer.succeed(ContextRegistry, registry),
        personalDisabled,
        taskExecutionLayer({
          prepare: () => Effect.die("Must not re-prepare"),
          ready: () => Effect.die("Must not repeat readiness"),
        }),
        Layer.succeed(PersonalActions, {
          ...actions,
          resumeRun: (input) =>
            actions.resumeRun(input).pipe(
              Effect.flatMap((receipt) =>
                loseAck
                  ? Effect.fail(
                      new ApplicationError({
                        kind: "unavailable",
                        message: "Lost Run admission acknowledgement",
                      }),
                    )
                  : Effect.succeed(receipt),
              ),
            ),
        }),
      ),
    );
    const approvals = yield* system.spawn("approvals", ApprovalQueueActor);
    const runs = yield* system.spawn("runs", RunRootActor);
    const personal = yield* system.spawn("personal", PersonalAgentActor);
    yield* actions.bind(undefined, undefined, approvals, runs);
    const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
    const run = () => Schema.decodeUnknownSync(RunState)(registry.get(runPath)!.state);
    const waitRun = (status: string) => {
      if (run().status === status) return Effect.void;
      return registry.changes.pipe(
        Stream.filter(
          (change) =>
            change.record.path === runPath &&
            Schema.decodeUnknownSync(RunState)(change.record.state).status === status,
        ),
        Stream.take(1),
        Stream.runDrain,
      );
    };
    return { api, registry, run, waitRun };
  });

test("Personal resumes the same failed execution with durable receipts; lost acknowledgement and restart never resume twice", async () => {
  const records = seed();
  // The Run may lag a child update; creating the child for a manual resume must
  // not activate its old automatic recovery path before command admission.
  records.set(delegationPath, {
    ...records.get(delegationPath)!,
    state: { ...records.get(delegationPath)!.state, status: "uncertain" },
  });
  let resumes = 0;
  const agent = fakeAgent({
    submit: () => Effect.die("Must not submit a replacement"),
    status: (handle) =>
      Effect.sync(() => {
        assert.ok(
          Schema.decodeUnknownSync(DelegationState)(records.get(delegationPath)!.state).resumptions
            ?.length,
          "manual recovery must be admitted before status lookup",
        );
        if (resumes) assert.equal(handle.runId, "resumed-run");
        return resumes
          ? { state: "completed", result: { text: "Original execution completed" } }
          : { state: "failed", resumable: true, error: "Interrupted" };
      }),
    resume: (handle) =>
      Effect.sync(() => {
        assert.deepEqual(handle, session);
        const saved = Schema.decodeUnknownSync(DelegationState)(records.get(delegationPath)!.state);
        assert.equal(
          saved.resumptions?.[0]?.status,
          "resuming",
          "external operation follows its durable marker",
        );
        const run = Schema.decodeUnknownSync(RunState)(records.get(runPath)!.state);
        assert.deepEqual(run.task, task);
        assert.equal(run.resumptions?.[0]?.receipt.revision, 5);
        resumes++;
        return { ...handle, runId: "resumed-run" };
      }),
  });
  for (let restart = 0; restart < 2; restart++) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* fixture(records, agent, restart === 0);
          const receipt = yield* env.api.personal.resumeRun(resumeInput());
          assert.equal(receipt.requestId, "resume-1");
          yield* env.waitRun("completed");
          assert.equal(resumes, 1);
          assert.equal(env.run().outcomeText, "Original execution completed");
          const original = env.run().resumptions![0]!;
          assert.equal(original.receipt.revision, 5);
          assert.equal(env.registry.get("/signals"), undefined);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
});

for (const interrupted of [false, true]) {
  test(`a ${interrupted ? "crash during" : "lost response to"} resume remains uncertain across restart and a new request`, async () => {
    const records = seed();
    let resumes = 0;
    const entered = Deferred.makeUnsafe<void>();
    const agent = fakeAgent({
      submit: () => Effect.die("No replacement submission"),
      status: () => Effect.succeed({ state: "failed", resumable: true, error: "Interrupted" }),
      resume: () =>
        Effect.gen(function* () {
          resumes++;
          yield* Deferred.succeed(entered, undefined);
          if (interrupted) return yield* Effect.never;
          return yield* new ExternalAgentError({ operation: "resume", message: "Response lost" });
        }),
    });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* fixture(records, agent);
          yield* env.api.personal.resumeRun(resumeInput());
          yield* Deferred.await(entered);
          if (!interrupted) yield* env.waitRun("uncertain");
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* fixture(records, agent);
          if (!env.run().outcomeText?.includes("unknown"))
            yield* env.registry.changes.pipe(
              Stream.filter(
                (change) =>
                  change.record.path === runPath &&
                  Schema.decodeUnknownSync(RunState)(change.record.state).outcomeText?.includes(
                    "unknown",
                  ) === true,
              ),
              Stream.take(1),
              Stream.runDrain,
            );
          assert.equal(resumes, 1);
          assert.match(env.run().outcomeText!, /unknown/);
          const source = yield* env.api.personal.get;
          yield* env.api.personal.resumeRun({
            ...resumeInput(env.registry.get(runPath)!.revision),
            requestId: "resume-2",
            expectedRevision: source.revision!,
          });
          yield* env.registry.changes.pipe(
            Stream.filter(
              (change) =>
                change.record.path === delegationPath &&
                Schema.decodeUnknownSync(DelegationState)(change.record.state).resumptions?.at(-1)
                  ?.status === "unknown",
            ),
            Stream.take(1),
            Stream.runDrain,
          );
          assert.equal(resumes, 1, "a new request cannot repeat an unknown external resume");
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
}

test("Run resumption rejects stale revisions and completed/confirmation states without executor effects", async () => {
  for (const status of ["failed", "completed", "awaiting-confirmation"]) {
    const records = seed();
    records.set(runPath, {
      ...records.get(runPath)!,
      state: { ...records.get(runPath)!.state, status },
    });
    const agent = fakeAgent({
      submit: () => Effect.die("Must not submit"),
      resume: () => Effect.die("Must not resume"),
      status: () => Effect.die("Must not inspect execution"),
    });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* fixture(records, agent);
          yield* env.api.personal.resumeRun(resumeInput(status === "failed" ? 3 : 4));
          const done = (record: ContextRecord) =>
            Schema.decodeUnknownSync(PersonalState)(record.state).outbox?.[0]?.status ===
            "rejected";
          if (!done(env.registry.get("/personal")!))
            yield* env.registry.changes.pipe(
              Stream.filter((change) => change.record.path === "/personal" && done(change.record)),
              Stream.take(1),
              Stream.runDrain,
            );
          assert.equal(env.run().resumptions, undefined);
          assert.equal(env.registry.get(runPath)!.revision, 4);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
});

test("a retained resume receipt restores child completion or monitoring after the Run lost the outcome", async () => {
  for (const [delivery, phase] of [
    ["pending", "completed"],
    ["delivered", "completed"],
    ["pending", "running"],
    ["delivered", "running"],
  ] as const) {
    const records = seed();
    const input = {
      operation: "resumeRun",
      source: "/personal",
      target: runPath,
      requestId: "retained-resume",
      causationId: "user-resume",
      expectedRevision: 4,
      createdAt: "2026-10-02T00:00:00Z",
    };
    records.set(runPath, {
      ...records.get(runPath)!,
      revision: 5,
      state: {
        ...records.get(runPath)!.state,
        resumptions: [
          { input, receipt: { requestId: input.requestId, revision: 5 }, status: delivery },
        ],
      },
    });
    records.set(delegationPath, {
      ...records.get(delegationPath)!,
      state: {
        ...records.get(delegationPath)!.state,
        status: phase,
        result: "Completed before parent crash",
        resumptions: [
          { input, receipt: { requestId: input.requestId, revision: 4 }, status: "done" },
        ],
      },
    });
    const agent = fakeAgent({
      submit: () => Effect.die("No new execution"),
      resume: () => Effect.die("No repeated resume"),
      status: () =>
        phase === "completed"
          ? Effect.die("Durable completion needs no provider")
          : Effect.succeed({
              state: "completed",
              result: { text: "Completed before parent crash" },
            }),
    });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* fixture(records, agent);
          yield* env.waitRun("completed");
          assert.equal(env.run().outcomeText, "Completed before parent crash");
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
});
