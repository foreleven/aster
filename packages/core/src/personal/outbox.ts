import { personalTaskIntent } from "../tasks/admission.js";
import type { ActorContext } from "@aster/actor";
import {
  ApplicationError,
  PersonalGoalMessageInput,
  PersonalSignalCommandInput,
  PersonalApprovalResponseInput,
  PersonalApprovalRequestInput,
  PersonalStartTaskInput,
  PersonalResumeRunInput,
  type PersonalOutboxItem,
  PersonalState,
  type CommandReceipt,
} from "@aster/api-contracts";
import { Clock, Effect, Match, Schema, Struct } from "effect";
import { isDeepStrictEqual } from "node:util";
import { randomUUID } from "node:crypto";
import type { ContextRegistry } from "../context/registry.js";
import type { PersonalActions } from "./actions.js";
import type { PersonalCommand } from "./actor.js";

/** All mutations run in Personal's mailbox; remote acknowledgements return through pipeToSelf. */
export const personalOutbox = (
  registry: ContextRegistry["Service"],
  actions: PersonalActions["Service"],
) => {
  let generation: string | undefined;
  const queue = Effect.fn("PersonalOutbox.queue")(function* (
    rawInput: PersonalOutboxItem["input"],
    expectedRevision: number,
  ): Effect.fn.Return<CommandReceipt, ApplicationError> {
    const current = registry.get("/personal")!;
    const state = Schema.decodeUnknownSync(PersonalState)(current.state);
    const outbox = state.outbox ?? [];
    const previous = outbox.find((item) => item.input.requestId === rawInput.requestId);
    const input = {
      ...rawInput,
      causal: previous?.input.causal ??
        rawInput.causal ?? {
          rootRequestId: rawInput.requestId,
          remainingAgentTurns: 4,
        },
    };
    if (previous) {
      if (!isDeepStrictEqual({ ...previous.input, createdAt: input.createdAt }, input))
        return yield* new ApplicationError({
          kind: "conflict",
          message: "Personal operation ID belongs to another command",
        });
      if (
        previous.status === "unknown" ||
        (input.operation === "resumeRun" && previous.status === "delivered")
      )
        yield* registry
          .commit(
            {
              ...current,
              state: {
                ...state,
                outbox: outbox.map((item) =>
                  item === previous ? { ...Struct.omit(item, ["error"]), status: "pending" } : item,
                ),
              },
            },
            { expectedRevision: current.revision ?? 0 },
          )
          .pipe(Effect.orDie);
      return { requestId: input.requestId, revision: previous.acceptedRevision };
    }
    const revision = (current.revision ?? 0) + 1;
    yield* registry
      .commit(
        {
          ...current,
          state: {
            ...state,
            outbox: [...outbox, { input, acceptedRevision: revision, status: "pending" }],
          },
        },
        { expectedRevision },
      )
      .pipe(
        Effect.catchTag("ContextConflict", () =>
          Effect.fail(
            new ApplicationError({
              kind: "conflict",
              message: "Personal Context revision changed",
            }),
          ),
        ),
        Effect.catchTag("ContextCommitError", Effect.die),
        Effect.catchTag("ContextValidationError", Effect.die),
      );
    return { requestId: input.requestId, revision };
  });
  const enqueue = Effect.fn("PersonalOutbox.enqueueGoal")(function* (
    raw: PersonalGoalMessageInput,
  ) {
    const input = yield* Schema.decodeUnknownEffect(PersonalGoalMessageInput)(raw).pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "invalid-input", message: "Invalid Goal message" }),
      ),
    );
    if (!input.text.trim())
      return yield* new ApplicationError({
        kind: "invalid-input",
        message: "Message text is required",
      });
    return yield* queue(
      {
        requestId: input.requestId,
        causationId: input.causationId,
        source: "/personal",
        target: `/goals/${input.goalSlug}`,
        expectedRevision: input.goalRevision,
        text: input.text,
        createdAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
      },
      input.expectedRevision,
    );
  });
  const enqueueSignal = Effect.fn("PersonalOutbox.enqueueSignal")(function* (
    raw: PersonalSignalCommandInput,
  ) {
    const input = yield* Schema.decodeUnknownEffect(PersonalSignalCommandInput)(raw).pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "invalid-input", message: "Invalid Signal command" }),
      ),
    );
    return yield* queue(
      {
        operation: input.operation,
        requestId: input.requestId,
        causationId: input.causationId,
        source: "/personal",
        target: `/signals/${input.signalSlug}`,
        expectedRevision: input.signalRevision,
        definition: input.definition,
        active: input.active,
        createdAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
      },
      input.expectedRevision,
    );
  });
  const enqueueApprovalRequest = Effect.fn("PersonalOutbox.enqueueApprovalRequest")(function* (
    raw: PersonalApprovalRequestInput,
  ) {
    const input = yield* Schema.decodeUnknownEffect(PersonalApprovalRequestInput)(raw).pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "invalid-input", message: "Invalid approval request" }),
      ),
    );
    return yield* queue(
      {
        operation: "requestApproval",
        requestId: input.requestId,
        causationId: input.causationId,
        source: "/personal",
        target: "/approvals",
        expectedRevision: input.approvalsRevision,
        contextPath: input.contextPath,
        contextRevision: input.contextRevision,
        approvalId: input.approvalId,
        createdAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
      },
      input.expectedRevision,
    );
  });
  const enqueueApproval = Effect.fn("PersonalOutbox.enqueueApproval")(function* (
    raw: PersonalApprovalResponseInput,
  ) {
    const input = yield* Schema.decodeUnknownEffect(PersonalApprovalResponseInput)(raw).pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "invalid-input", message: "Invalid approval response" }),
      ),
    );
    return yield* queue(
      {
        operation: "respondApproval",
        requestId: input.requestId,
        causationId: input.causationId,
        source: "/personal",
        target: "/approvals",
        expectedRevision: input.approvalsRevision,
        approvalId: input.approvalId,
        response: input.response,
        createdAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
      },
      input.expectedRevision,
    );
  });
  const enqueueResume = Effect.fn("PersonalOutbox.enqueueResume")(function* (
    raw: PersonalResumeRunInput,
  ) {
    const input = yield* Schema.decodeUnknownEffect(PersonalResumeRunInput)(raw).pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "invalid-input", message: "Invalid Run resumption" }),
      ),
    );
    return yield* queue(
      {
        operation: "resumeRun",
        source: "/personal",
        target: input.runPath,
        requestId: input.requestId,
        causationId: input.causationId,
        expectedRevision: input.runRevision,
        createdAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
      },
      input.expectedRevision,
    );
  });
  const enqueueTask = Effect.fn("PersonalOutbox.enqueueTask")(function* (
    raw: PersonalStartTaskInput,
  ) {
    const input = yield* Schema.decodeUnknownEffect(PersonalStartTaskInput)(raw).pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "invalid-input", message: "Invalid Task input" }),
      ),
    );
    return yield* queue(
      personalTaskIntent(
        { agent: input.agent, task: input.task },
        {
          requestId: input.requestId,
          causationId: input.causationId,
          createdAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
        },
      ),
      input.expectedRevision,
    );
  });
  const deliver = Effect.fn("PersonalOutbox.deliver")(function* (
    actor: ActorContext<PersonalCommand>,
    recover = false,
  ) {
    if (generation) return;
    const current = registry.get("/personal")!;
    let state = Schema.decodeUnknownSync(PersonalState)(current.state);
    if (recover && state.outbox?.some((item) => item.status === "unknown")) {
      state = {
        ...state,
        outbox: state.outbox.map((item) =>
          item.status === "unknown"
            ? {
                ...Struct.omit(item, ["error"]),
                status: "pending" as const,
              }
            : item,
        ),
      };
      yield* registry
        .commit({ ...current, state }, { expectedRevision: current.revision ?? 0 })
        .pipe(Effect.orDie);
    }
    const next = state.outbox?.find((item) => item.status === "pending");
    if (!next) return;
    const activeGeneration = randomUUID();
    const latest = registry.get("/personal")!;
    const lastAttemptAt = new Date(yield* Clock.currentTimeMillis).toISOString();
    yield* registry
      .commit(
        {
          ...latest,
          state: {
            ...state,
            outbox: state.outbox?.map((item) =>
              item === next ? { ...item, attempts: (item.attempts ?? 0) + 1, lastAttemptAt } : item,
            ),
          },
        },
        { expectedRevision: latest.revision ?? 0 },
      )
      .pipe(Effect.orDie);
    generation = activeGeneration;
    const input = next.input;
    const operation = Match.value(input).pipe(
      Match.when({ operation: "respondApproval" }, actions.respondApproval),
      Match.when({ operation: "requestApproval" }, actions.requestApproval),
      Match.when({ operation: "startTask" }, actions.startTask),
      Match.when({ operation: "resumeRun" }, actions.resumeRun),
      Match.when({ operation: "createSignal" }, actions.applySignal),
      Match.when({ operation: "updateSignal" }, actions.applySignal),
      Match.orElse(actions.sendGoalMessage),
    );
    yield* actor.pipeToSelf(operation, (result) => ({
      _tag: "OutboxResult",
      generation: activeGeneration,
      requestId: next.input.requestId,
      result,
    }));
  });
  const settled = Effect.fn("PersonalOutbox.settled")(function* (
    command: Extract<PersonalCommand, { _tag: "OutboxResult" }>,
    actor: ActorContext<PersonalCommand>,
  ) {
    if (generation !== command.generation) return;
    const current = registry.get("/personal")!;
    const state = Schema.decodeUnknownSync(PersonalState)(current.state);
    const result = command.result;
    const outbox = state.outbox?.map((item) => {
      if (item.input.requestId !== command.requestId) return item;
      if (result._tag === "Success")
        return {
          ...Struct.omit(item, ["error"]),
          status: "delivered",
          receipt: result.value,
        };
      return {
        ...item,
        status: result.error.kind === "unavailable" ? "unknown" : "rejected",
        error: result.error.message,
      };
    });
    yield* registry
      .commit(
        { ...current, state: { ...state, outbox } },
        { expectedRevision: current.revision ?? 0 },
      )
      .pipe(Effect.orDie);
    generation = undefined;
    yield* actor.self.tell({ _tag: "DeliverOutbox" });
  });
  return {
    enqueue,
    enqueueSignal,
    enqueueApproval,
    enqueueApprovalRequest,
    enqueueTask,
    enqueueResume,
    deliver,
    settled,
  };
};
