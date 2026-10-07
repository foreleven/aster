import { AgentConversations, AgentError, AgentRunner } from "@aster/agent";
import { ApplicationError, TaskDeliveryInput, type PreparedTask } from "@aster/api-contracts";
import { Context, Deferred, Effect, Layer, Option, Ref, Schedule, Schema, Semaphore } from "effect";
import { GoalSettings } from "../../config/settings.js";
import { CurrentActors } from "../../services/actors.js";
import { sendApproval } from "../../approvals/actor.js";
import {
  TaskInput,
  type TaskInputRef,
  type TaskOutcome,
  type TaskWork,
} from "../state/snapshot.js";
import { StoredTaskInput } from "../state/snapshot.js";
import { ExternalAgents, ExternalAgentError, type ExecutionStatus } from "./contracts.js";
import { executionCheckpoint } from "./checkpoint.js";
import { DEFAULT_EXECUTOR_PROMPT, taskPrompt } from "./external.js";
import { executeTask } from "./agent.js";

const makeExecution = Effect.fn("TaskExecution.make")(function* (path: string) {
  const messages = yield* AgentConversations;
  const agents = yield* ExternalAgents;
  const runner = yield* AgentRunner;
  const settings = yield* GoalSettings;
  const actors = yield* CurrentActors;
  const journal = yield* executionCheckpoint(path, DEFAULT_EXECUTOR_PROMPT);
  const writer = yield* Semaphore.make(1);
  const changed = yield* Ref.make(yield* Deferred.make<void>());
  const save = Effect.fnUntraced(function* (patch: Parameters<typeof journal.save>[0]) {
    yield* journal.save(patch);
    const previous = yield* Ref.getAndSet(changed, yield* Deferred.make<void>());
    yield* Deferred.succeed(previous, undefined);
  });
  const active = yield* Ref.make<{ work: TaskWork; admission: TaskDeliveryInput } | undefined>(
    undefined,
  );
  const initial = (work: TaskWork) =>
    messages.get(path, work.admissionEntryId).pipe(
      Effect.flatMap((entry) => Schema.decodeUnknownEffect(TaskDeliveryInput)(entry.data)),
      Effect.orDie,
    );
  const resolve = (ref: Pick<TaskInputRef, "requestId" | "entryId">) =>
    messages.get(path, ref.entryId).pipe(
      Effect.map((entry): TaskInput =>
        entry.kind === "task.admission"
          ? { _tag: "Initial", input: Schema.decodeUnknownSync(TaskDeliveryInput)(entry.data) }
          : Schema.decodeUnknownSync(StoredTaskInput)(entry.data).input,
      ),
      Effect.orDie,
    );
  const executor = (agent: string) =>
    agents[agent]
      ? Effect.succeed(agents[agent]!)
      : Effect.fail(
          new ExternalAgentError({
            operation: "submit",
            message: `Task executor unavailable: ${agent}`,
          }),
        );
  const prepare = Effect.fnUntraced(function* (
    admission: TaskDeliveryInput,
  ): Effect.fn.Return<PreparedTask> {
    const checkpoint = yield* journal.read;
    return {
      instructions: `${admission.agent === "internal" ? "" : `${checkpoint.prompt}\n\n`}${admission.task.instructions}`,
      input: [
        ...admission.task.input,
        ...(admission.evidence
          ? [{ content: JSON.stringify(admission.evidence), sources: [admission.evidence.path] }]
          : []),
      ],
    };
  });
  const mark = Effect.fnUntraced(function* (
    id: string,
    roundId: string,
    kind: "instruction" | "answer" | "control",
    status: "sending" | "accepted" | "rejected" | "unknown",
  ) {
    const saved = yield* journal.read;
    yield* save({
      deliveries: [
        ...saved.deliveries.filter((item) => item.requestId !== id),
        { requestId: id, roundId, kind, status },
      ],
    });
  });
  const acknowledge = (requestId: string) =>
    sendApproval(actors, { _tag: "Acknowledge", id: requestId, target: `/user${path}` });
  const outcome = Effect.fnUntraced(function* (
    work: TaskWork,
    status: TaskOutcome["status"],
    text: string,
    requests?: TaskOutcome["requests"],
  ) {
    const checkpoint = yield* journal.read;
    return {
      roundId: work.roundId,
      status,
      text,
      covered: checkpoint.deliveries
        .filter(
          (item) => item.roundId === work.roundId && ["accepted", "rejected"].includes(item.status),
        )
        .map((item) => item.requestId),
      ...(requests ? { requests } : {}),
    } satisfies TaskOutcome;
  });
  const uncertain = (work: TaskWork) =>
    outcome(
      work,
      "uncertain",
      "An execution input has an unconfirmed delivery outcome. Check the original execution before continuing.",
    );
  const failure = (work: TaskWork, error: AgentError | ExternalAgentError) =>
    outcome(
      work,
      (error instanceof AgentError ? error.outcome === "failed" : error.outcome === "rejected")
        ? "failed"
        : "uncertain",
      error.message,
    );
  const finish = Effect.fnUntraced(function* (result: TaskOutcome) {
    yield* save({ outcome: result });
    yield* Ref.set(active, undefined);
    return result;
  });
  const observed = Effect.fnUntraced(function* (work: TaskWork, status: ExecutionStatus) {
    if (status.state === "waiting_input")
      return yield* outcome(
        work,
        "waiting_input",
        status.requests?.map((request) => request.prompt).join("\n") ??
          "More information is needed",
        (status.requests ?? []).map((request) => ({
          id: `${path}:input:${request.id}`,
          kind: request.kind,
          request,
        })),
      );
    if (status.state === "running") return undefined;
    return yield* outcome(
      work,
      status.state === "unknown" ? "uncertain" : status.state,
      status.result?.text ?? status.error ?? "Task finished",
    );
  });
  /** Called under the execution writer, never on the Task mailbox. */
  const deliver = Effect.fnUntraced(function* (
    ref: Pick<TaskInputRef, "requestId" | "entryId">,
    work: TaskWork,
    admission: TaskDeliveryInput,
  ) {
    const input = yield* resolve(ref);
    const saved = yield* journal.read;
    const previous = saved.deliveries.find((item) => item.requestId === ref.requestId);
    if (previous) {
      if (previous.kind === "answer" && previous.status === "accepted")
        yield* acknowledge(ref.requestId);
      return;
    }
    if (input._tag === "Check" || input._tag === "Retry") return;
    if (input._tag === "Answer") {
      if (input.requestId === `${path}:confirm`) {
        yield* save({ approved: input.response.decision === "approve" });
        yield* mark(ref.requestId, work.roundId, "answer", "accepted");
        yield* acknowledge(ref.requestId);
        return;
      }
      const request = saved.outcome?.requests?.find((item) => item.id === input.requestId)?.request;
      if (!request || !saved.session) return;
      const agent = yield* executor(admission.agent);
      yield* mark(ref.requestId, work.roundId, "answer", "sending");
      const result = yield* agent
        .respond(saved.session, request, input.response)
        .pipe(Effect.result);
      yield* mark(
        ref.requestId,
        work.roundId,
        "answer",
        result._tag === "Success" ? "accepted" : "unknown",
      );
      if (result._tag === "Failure") return yield* result.failure;
      yield* acknowledge(ref.requestId);
      return;
    }
    if (admission.agent === "internal") {
      if (input._tag !== "Message") return;
      // Native steering owns its durable admission before the checkpoint mirrors it.
      if (yield* messages.steer(path, ref.requestId, input.input.text).pipe(Effect.orDie))
        yield* mark(ref.requestId, work.roundId, "instruction", "accepted");
      return;
    }
    if (!saved.approved) return;
    const agent = yield* executor(admission.agent);
    yield* mark(ref.requestId, work.roundId, "instruction", "sending");
    const task =
      input._tag === "Initial"
        ? yield* prepare(admission)
        : { instructions: input.input.text, input: [] };
    const result = yield* (
      saved.session
        ? agent.followUp(saved.session, { requestId: ref.requestId, text: task.instructions })
        : agent.submit(task, { requestId: path })
    ).pipe(Effect.result);
    if (result._tag === "Failure") {
      yield* mark(
        ref.requestId,
        work.roundId,
        "instruction",
        result.failure.outcome === "rejected" ? "rejected" : "unknown",
      );
      return yield* result.failure;
    }
    // Session and acceptance are one checkpoint, so a recovered handle never proves another input.
    yield* save({
      session: result.success,
      deliveries: (yield* journal.read).deliveries.map((item) =>
        item.requestId === ref.requestId ? { ...item, status: "accepted" as const } : item,
      ),
    });
  });
  const reconcile = Effect.fnUntraced(function* (work: TaskWork, admission: TaskDeliveryInput) {
    const controls = [];
    for (const ref of work.inputs) {
      const input = yield* resolve(ref);
      if (input._tag === "Check" || input._tag === "Retry") controls.push({ ref, input });
    }
    const control = controls.at(-1);
    if (!control) return undefined;
    const saved = yield* journal.read;
    const previous = saved.deliveries.find((item) => item.requestId === control.ref.requestId);
    if (previous && ["sending", "unknown"].includes(previous.status)) return yield* uncertain(work);
    if (admission.agent === "internal") return control;
    const unresolved = saved.deliveries.filter((item) =>
      ["sending", "unknown"].includes(item.status),
    );
    if (
      unresolved.some((item) => item.kind === "answer" || item.requestId !== admission.requestId)
    ) {
      yield* mark(control.ref.requestId, work.roundId, "control", "accepted");
      return yield* uncertain(work);
    }
    if (control.input._tag === "Check")
      yield* mark(control.ref.requestId, work.roundId, "control", "accepted");
    const agent = yield* executor(admission.agent);
    if (
      !saved.session &&
      saved.deliveries.some(
        (item) => item.requestId === admission.requestId && item.status === "rejected",
      )
    ) {
      if (control.input._tag === "Check")
        return yield* outcome(work, "failed", "The executor rejected the original submission");
      yield* save({
        deliveries: saved.deliveries.filter((item) => item.requestId !== admission.requestId),
      });
      yield* mark(control.ref.requestId, work.roundId, "control", "accepted");
      return undefined;
    }
    const rejectedInputs = saved.deliveries.filter(
      (item) =>
        item.roundId === work.roundId && item.kind === "instruction" && item.status === "rejected",
    );
    if (rejectedInputs.length) {
      if (control.input._tag === "Check")
        return yield* outcome(work, "failed", "The executor rejected an instruction");
      const entries = yield* messages.read(path).pipe(Effect.orDie);
      yield* save({
        deliveries: saved.deliveries.filter((item) => !rejectedInputs.includes(item)),
      });
      for (const input of rejectedInputs) {
        const entry = entries.find(
          (entry) => entry.requestId === input.requestId && entry.kind === "task.input",
        )!;
        yield* deliver({ requestId: input.requestId, entryId: entry.id }, work, admission);
      }
      yield* mark(control.ref.requestId, work.roundId, "control", "accepted");
      return undefined;
    }
    let session = saved.session;
    if (!session && agent.lookupSubmission) {
      const found = yield* agent.lookupSubmission(yield* prepare(admission), { requestId: path });
      if (Option.isSome(found)) session = found.value;
    }
    if (!session) {
      yield* mark(control.ref.requestId, work.roundId, "control", "accepted");
      return yield* uncertain(work);
    }
    const status = yield* agent.status(session);
    if (control.input._tag === "Retry") {
      if (status.state !== "failed" || !status.resumable)
        return yield* outcome(work, "uncertain", "Executor cannot authoritatively retry this work");
      yield* mark(control.ref.requestId, work.roundId, "control", "sending");
      session = yield* agent.resume(session);
    }
    yield* save({
      session,
      deliveries: (yield* journal.read).deliveries.map((item) =>
        unresolved.some((pending) => pending.requestId === item.requestId) &&
        status.state !== "unknown"
          ? { ...item, status: "accepted" as const }
          : item,
      ),
    });
    yield* mark(control.ref.requestId, work.roundId, "control", "accepted");
    if (control.input._tag === "Check") return yield* observed(work, status);
    return undefined;
  });
  const poll = Effect.fnUntraced(function* (work: TaskWork, admission: TaskDeliveryInput) {
    const [before, wake] = yield* writer.withPermit(Effect.all([journal.read, Ref.get(changed)]));
    if (before.deliveries.some((item) => ["sending", "unknown"].includes(item.status)))
      return Option.some(yield* writer.withPermit(uncertain(work).pipe(Effect.flatMap(finish))));
    if (
      before.deliveries.some((item) => item.roundId === work.roundId && item.status === "rejected")
    )
      return Option.some(
        yield* writer.withPermit(
          outcome(work, "failed", "The executor rejected an instruction").pipe(
            Effect.flatMap(finish),
          ),
        ),
      );
    const agent = yield* executor(admission.agent);
    if (!before.session)
      return Option.some(yield* writer.withPermit(uncertain(work).pipe(Effect.flatMap(finish))));
    const observation = yield* Effect.raceFirst(
      agent.wait(before.session).pipe(Effect.map(Option.some)),
      Deferred.await(wake).pipe(Effect.as(Option.none<ExecutionStatus>())),
    );
    if (Option.isNone(observation)) return Option.none<TaskOutcome>();
    const status = observation.value;
    return yield* writer.withPermit(
      Effect.gen(function* () {
        // A follow-up may have replaced the provider round while wait was in flight.
        if ((yield* journal.read).revision !== before.revision) return Option.none<TaskOutcome>();
        const result = yield* observed(work, status);
        return result ? Option.some(yield* finish(result)) : Option.none<TaskOutcome>();
      }),
    );
  });
  const run = Effect.fn("TaskExecution.run")(function* (work: TaskWork) {
    const admission = yield* initial(work);
    const prepared = yield* writer.withPermit(
      Effect.gen(function* () {
        yield* Ref.set(active, { work, admission });
        let saved = yield* journal.read;
        if (saved.revision === 0) {
          yield* save({
            prompt: agents[admission.agent]?.executorPrompt ?? DEFAULT_EXECUTOR_PROMPT,
          });
          saved = yield* journal.read;
        }
        if (
          saved.outcome?.roundId === work.roundId &&
          saved.outcome.status !== "waiting_input" &&
          work.inputs.every((input) => saved.outcome!.covered.includes(input.requestId))
        )
          return yield* finish(saved.outcome);
        // An acknowledged Pi steer can precede the executor checkpoint during interruption.
        if (admission.agent === "internal") {
          for (const entry of yield* messages.read(path).pipe(Effect.orDie)) {
            if (entry.kind !== "task.steer") continue;
            const requestId = Schema.decodeUnknownSync(Schema.Struct({ requestId: Schema.String }))(
              entry.data,
            ).requestId;
            if (
              work.inputs.some((input) => input.requestId === requestId) &&
              !saved.deliveries.some((item) => item.requestId === requestId)
            )
              yield* mark(requestId, work.roundId, "instruction", "accepted");
          }
        }
        const checked = yield* reconcile(work, admission);
        if (checked && "status" in checked) return yield* finish(checked);
        for (const ref of work.inputs) {
          if ((yield* resolve(ref))._tag === "Answer") yield* deliver(ref, work, admission);
        }
        saved = yield* journal.read;
        if (admission.agent !== "internal" && !saved.approved) {
          const rejected = saved.deliveries.some(
            (item) => item.requestId === `${path}:confirm` && item.status === "accepted",
          );
          if (rejected)
            return yield* finish(
              yield* outcome(work, "cancelled", "The user rejected this execution"),
            );
          const id = `${path}:confirm`;
          return yield* finish(
            yield* outcome(work, "waiting_input", "Please confirm this execution", [
              {
                id,
                kind: "confirmation",
                request: { id, kind: "approval", prompt: taskPrompt(yield* prepare(admission)) },
              },
            ]),
          );
        }
        if (
          admission.agent !== "internal" &&
          saved.deliveries.some((item) => ["sending", "unknown"].includes(item.status))
        )
          return yield* finish(yield* uncertain(work));
        if (admission.agent === "internal") return undefined;
        if (!(yield* journal.read).session)
          yield* deliver(
            { requestId: admission.requestId, entryId: work.admissionEntryId },
            work,
            admission,
          );
        for (const ref of work.inputs) yield* deliver(ref, work, admission);
        return undefined;
      }),
    );
    if (prepared) return prepared;
    if (admission.agent !== "internal") {
      const result = yield* poll(work, admission).pipe(
        Effect.repeat({ schedule: Schedule.spaced("1 second"), while: Option.isNone }),
      );
      return Option.getOrThrow(result);
    }
    const invocation = yield* writer.withPermit(
      Effect.gen(function* () {
        const saved = yield* journal.read;
        const resolved = yield* Effect.forEach(work.inputs, (ref) =>
          resolve(ref).pipe(Effect.map((input) => ({ ref, input }))),
        );
        const control = resolved.findLast(
          ({ input }) => input._tag === "Check" || input._tag === "Retry",
        );
        const interrupted = saved.deliveries.find(
          (item) => item.kind === "instruction" && ["sending", "unknown"].includes(item.status),
        );
        const next =
          control ??
          resolved.find(({ ref }) => ref.requestId === interrupted?.requestId) ??
          resolved.find(
            ({ ref }) => !saved.deliveries.some((item) => item.requestId === ref.requestId),
          );
        if (!next) return yield* Effect.die(new Error("Internal execution has no input"));
        const { ref, input } = next;
        let instruction = input;
        let originalId = interrupted?.requestId;
        if (input._tag === "Retry" || input._tag === "Check") {
          const original = saved.deliveries.findLast((item) => item.kind === "instruction");
          originalId ??= original?.requestId;
          // Retry markers point at a prior instruction through its retained control input.
          const entries = yield* messages.read(path).pipe(Effect.orDie);
          const entry = entries.findLast(
            (entry) =>
              saved.deliveries.some(
                (item) => item.kind === "instruction" && item.requestId === entry.requestId,
              ) &&
              (entry.kind === "task.admission" ||
                (entry.kind === "task.input" &&
                  Schema.decodeUnknownSync(StoredTaskInput)(entry.data).input._tag === "Message")),
          );
          if (entry)
            instruction =
              entry.kind === "task.admission"
                ? { _tag: "Initial", input: admission }
                : Schema.decodeUnknownSync(StoredTaskInput)(entry.data).input;
        }
        const task =
          instruction._tag === "Message"
            ? { instructions: instruction.input.text, input: [] }
            : yield* prepare(admission);
        const requestId = input._tag === "Check" ? (originalId ?? ref.requestId) : ref.requestId;
        yield* mark(requestId, work.roundId, "instruction", "sending");
        return {
          requestId,
          task,
          reconcile: input._tag === "Check" || (!!interrupted && input._tag !== "Retry"),
        };
      }),
    );
    const result = yield* executeTask({
      ...invocation,
      path,
      model: settings.reasoning!.model,
    }).pipe(
      Effect.provideService(AgentRunner, runner),
      Effect.provideService(CurrentActors, actors),
      Effect.result,
    );
    return yield* writer.withPermit(
      Effect.gen(function* () {
        yield* mark(
          invocation.requestId,
          work.roundId,
          "instruction",
          result._tag === "Success" || result.failure.outcome === "failed" ? "accepted" : "unknown",
        );
        for (const ref of work.inputs) {
          const input = yield* resolve(ref);
          if (
            (input._tag === "Check" || input._tag === "Retry") &&
            ref.requestId !== invocation.requestId
          )
            yield* mark(ref.requestId, work.roundId, "control", "accepted");
        }
        return yield* finish(
          result._tag === "Success"
            ? yield* outcome(work, "completed", result.success)
            : yield* failure(work, result.failure),
        );
      }),
    );
  });
  return {
    run: (work: TaskWork) =>
      run(work).pipe(
        Effect.catchTag("ExternalAgentError", (error) =>
          writer.withPermit(failure(work, error).pipe(Effect.flatMap(finish))),
        ),
        Effect.ensuring(Ref.set(active, undefined)),
      ),
    send: Effect.fn("TaskExecution.send")(function* (input: TaskInputRef) {
      yield* writer.withPermit(
        Effect.gen(function* () {
          const checkpoint = yield* journal.read;
          const previous = checkpoint.deliveries.find((item) => item.requestId === input.requestId);
          if (previous?.kind === "answer" && previous.status === "accepted")
            return yield* acknowledge(input.requestId);
          const current = yield* Ref.get(active);
          if (current) yield* deliver(input, current.work, current.admission);
        }),
      );
    }),
    cancel: Effect.fn("TaskExecution.cancel")(function* () {
      return yield* writer.withPermit(
        Effect.gen(function* () {
          const current = yield* Ref.get(active);
          if (!current) return false;
          if (current.admission.agent === "internal") return true;
          const agent = yield* executor(current.admission.agent);
          const saved = yield* journal.read;
          if (!saved.session || !agent.cancel)
            return yield* new ApplicationError({
              kind: "conflict",
              message: "This executor cannot confirm cancellation of running work",
            });
          return yield* agent.cancel(saved.session);
        }),
      );
    }),
  };
});
export class TaskExecution extends Context.Service<
  TaskExecution,
  Effect.Success<ReturnType<typeof makeExecution>>
>()("tasks/Execution") {
  static readonly layer = (path: string) => Layer.effect(TaskExecution, makeExecution(path));
}
