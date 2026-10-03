import { acceptBusinessNotification } from "../notifications/inbox.js";
import { makePersonalApi } from "./api.js";
import { personalTaskIntent } from "../tasks/admission.js";
import { inspectDelegation } from "../delegation/inspection.js";
import { ReplyTo, type ActorContext } from "@aster/actor";
import { randomUUID } from "node:crypto";
import {
  ApplicationError,
  BusinessNotification,
  PersonalInput,
  PersonalStartTaskInput,
  PersonalResumeRunInput,
  PersonalMessage,
  PersonalReceipt,
  PersonalRetryInput,
  PersonalState,
  PersonalResult,
  PersonalGoalMessageInput,
  PersonalSignalCommandInput,
  PersonalApprovalResponseInput,
  PersonalApprovalRequestInput,
  type PersonalOutboxItem,
  CommandReceipt,
  PublicContext,
  DelegationInspection,
} from "@aster/api-contracts";
import { Clock, Effect, Layer, Match, Schema } from "effect";
import { ContextActor, contextPath } from "../context/actor.js";
import { defineContext } from "../context/model.js";
import { ContextRegistry } from "../context/registry.js";
import { PersonalProcessor, PersonalProcessingError } from "./processor.js";
import { PersonalActions } from "./actions.js";
import { personalOutbox } from "./outbox.js";

export const PersonalReply = Schema.Union([
  Schema.TaggedStruct("Delegation", { inspection: DelegationInspection }),
  Schema.TaggedStruct("Accepted", { receipt: PersonalReceipt }),
  Schema.TaggedStruct("Queued", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Snapshot", { record: PublicContext }),
  Schema.TaggedStruct("Contexts", { records: Schema.Array(PublicContext) }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type PersonalReply = typeof PersonalReply.Type;

export const PersonalCommand = Schema.Union([
  Schema.TaggedStruct("Notify", { input: BusinessNotification, replyTo: ReplyTo<PersonalReply>() }),
  Schema.TaggedStruct("RequestApproval", {
    input: PersonalApprovalRequestInput,
    replyTo: ReplyTo<PersonalReply>(),
  }),
  Schema.TaggedStruct("ResumeRun", {
    input: PersonalResumeRunInput,
    replyTo: ReplyTo<PersonalReply>(),
  }),
  Schema.TaggedStruct("StartTask", {
    input: PersonalStartTaskInput,
    replyTo: ReplyTo<PersonalReply>(),
  }),
  Schema.TaggedStruct("InspectDelegation", {
    path: Schema.String,
    replyTo: ReplyTo<PersonalReply>(),
  }),
  Schema.TaggedStruct("RespondApproval", {
    input: PersonalApprovalResponseInput,
    replyTo: ReplyTo<PersonalReply>(),
  }),
  Schema.TaggedStruct("ApplySignal", {
    input: PersonalSignalCommandInput,
    replyTo: ReplyTo<PersonalReply>(),
  }),
  Schema.TaggedStruct("DeliverOutbox", { recover: Schema.optional(Schema.Boolean) }),
  Schema.TaggedStruct("OutboxResult", {
    generation: Schema.String,
    requestId: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: CommandReceipt }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(ApplicationError) }),
    ]),
  }),
  Schema.TaggedStruct("SendGoalMessage", {
    input: PersonalGoalMessageInput,
    replyTo: ReplyTo<PersonalReply>(),
  }),
  Schema.TaggedStruct("Process", {}),
  Schema.TaggedStruct("Processed", {
    generation: Schema.String,
    requestId: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: PersonalResult }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(PersonalProcessingError) }),
    ]),
  }),
  Schema.TaggedStruct("Get", { replyTo: ReplyTo<PersonalReply>() }),
  Schema.TaggedStruct("ReadContext", { path: Schema.String, replyTo: ReplyTo<PersonalReply>() }),
  Schema.TaggedStruct("ListContexts", { replyTo: ReplyTo<PersonalReply>() }),
  Schema.TaggedStruct("Accept", { input: PersonalInput, replyTo: ReplyTo<PersonalReply>() }),
  Schema.TaggedStruct("Retry", { input: PersonalRetryInput, replyTo: ReplyTo<PersonalReply>() }),
]);
export type PersonalCommand = typeof PersonalCommand.Type;

const receipt = (message: PersonalMessage): PersonalReceipt => ({
  requestId: message.requestId,
  revision: message.revision,
  sequence: message.sequence,
});

/** The mailbox owns acceptance and ordering. This Actor never subscribes to ContextChange. */
export class PersonalAgentActor extends ContextActor.Service<PersonalAgentActor>()(
  "personal/Actor",
  {
    command: PersonalCommand,
    context: defineContext({
      identity: "Personal Agent",
      signalSource: false,
      state: PersonalState,
      message: PersonalMessage,
    }),
  },
) {
  static readonly layer = Layer.effect(
    PersonalAgentActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const processor = yield* PersonalProcessor;
      const actions = yield* PersonalActions;
      const outbox = personalOutbox(registry, actions);
      let generation: string | undefined;
      const snapshot = () => registry.get("/personal")!;
      const readRecord = (path: string): Effect.Effect<PublicContext, ApplicationError> =>
        Effect.suspend(() => {
          const record = registry.get(path);
          return record
            ? Effect.succeed(record)
            : Effect.fail(
                new ApplicationError({ kind: "not-found", message: "Context not found" }),
              );
        });
      const read = (path: string) =>
        readRecord(path).pipe(
          Effect.map((record): PersonalReply => ({
            _tag: "Snapshot",
            record: registry.project(record),
          })),
        );
      const accept = Effect.fn("PersonalAgent.accept")(function* (
        raw: PersonalInput,
      ): Effect.fn.Return<PersonalReply, ApplicationError> {
        // Local Actor commands are not automatically decoded by the generic mailbox.
        const input = yield* Schema.decodeUnknownEffect(PersonalInput)(raw).pipe(
          Effect.mapError(
            () =>
              new ApplicationError({ kind: "invalid-input", message: "Invalid Personal input" }),
          ),
        );
        if (!input.text.trim())
          return yield* new ApplicationError({
            kind: "invalid-input",
            message: "Message text is required",
          });
        const current = snapshot();
        const state = Schema.decodeUnknownSync(PersonalState)(current.state);
        const messages = Schema.decodeUnknownSync(Schema.Array(PersonalMessage))(current.messages);
        const previous = messages.find(
          (message) => message.source === "user" && message.requestId === input.requestId,
        );
        // A replay precedes revision checking: its original revision is necessarily stale.
        if (previous) {
          if (previous.payload.text !== input.text || previous.causationId !== input.causationId)
            return yield* new ApplicationError({
              kind: "conflict",
              message: "Request ID already belongs to another input",
            });
          return { _tag: "Accepted", receipt: receipt(previous) };
        }
        const message: PersonalMessage = {
          requestId: input.requestId,
          causationId: input.causationId,
          source: "user",
          causal: { rootRequestId: input.requestId, remainingAgentTurns: 4 },
          target: "/personal",
          revision: (current.revision ?? 0) + 1,
          sequence: messages.length + 1,
          createdAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
          payload: { _tag: "UserInput", text: input.text },
        };
        yield* registry
          .commit(
            {
              ...current,
              state: { ...state, pendingRequestIds: [...state.pendingRequestIds, input.requestId] },
              messages: [...messages, message],
            },
            { expectedRevision: input.expectedRevision },
          )
          .pipe(
            Effect.catchTag("ContextConflict", () =>
              Effect.fail(
                new ApplicationError({
                  kind: "conflict",
                  message: "Personal Context revision changed; reload before sending a new input",
                }),
              ),
            ),
            // A possibly committed storage failure enters supervision. The caller must retry
            // the same identity after recovery, never infer rejection from a missing reply.
            Effect.catchTag("ContextCommitError", Effect.die),
            Effect.catchTag("ContextValidationError", Effect.die),
          );
        return { _tag: "Accepted", receipt: receipt(message) };
      });
      const process = Effect.fn("PersonalAgent.process")(function* (
        actor: ActorContext<PersonalCommand>,
      ) {
        if (!processor.enabled || generation) return;
        const current = snapshot();
        const state = Schema.decodeUnknownSync(PersonalState)(current.state);
        const requestId = state.pendingRequestIds[0];
        if (!requestId) return;
        const runs = state.runs ?? [];
        const existing = runs.findLast((run) => run.requestId === requestId);
        // Failed or uncertain outcomes require explicit reconciliation; restart
        // must not turn them into an unconditional new submission.
        if (existing?.status === "failed") return;
        const messages = Schema.decodeUnknownSync(Schema.Array(PersonalMessage))(current.messages);
        const input = messages.find(
          (message) => message.target === "/personal" && message.requestId === requestId,
        );
        if (!input) return yield* Effect.die(new Error("Personal pending input is missing"));
        if (!existing)
          yield* registry
            .commit(
              {
                ...current,
                state: {
                  ...state,
                  runs: [
                    ...runs,
                    {
                      requestId,
                      executionId: `input:${input.sequence}`,
                      revision: (current.revision ?? 0) + 1,
                      inputSequence: input.sequence,
                      status: "running",
                      startedAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
                    },
                  ],
                },
              },
              { expectedRevision: current.revision ?? 0 },
            )
            .pipe(Effect.orDie);
        const activeGeneration = randomUUID();
        generation = activeGeneration;
        const readApi = makePersonalApi(actor.self);
        yield* actor.pipeToSelf(
          processor.run(
            input,
            {
              read: readApi.readContext,
              list: readApi.listContexts,
              inspectDelegation: readApi.inspectDelegation,
              executors: actions.executors,
            },
            existing?.executionId ?? (existing ? input.requestId : `input:${input.sequence}`),
          ),
          (result) => ({
            _tag: "Processed",
            generation: activeGeneration,
            requestId,
            result,
          }),
        );
      });
      const processed = Effect.fn("PersonalAgent.processed")(function* (
        command: Extract<PersonalCommand, { _tag: "Processed" }>,
        actor: ActorContext<PersonalCommand>,
      ) {
        if (generation !== command.generation) return;
        const current = snapshot();
        const state = Schema.decodeUnknownSync(PersonalState)(current.state);
        const messages = Schema.decodeUnknownSync(Schema.Array(PersonalMessage))(current.messages);
        const input = messages.find(
          (message) => message.target === "/personal" && message.requestId === command.requestId,
        );
        if (!input || state.pendingRequestIds[0] !== command.requestId)
          return yield* Effect.die(new Error("Personal result does not match the accepted input"));
        const result = command.result;
        const causal = {
          rootRequestId: input.causal?.rootRequestId ?? input.requestId,
          remainingAgentTurns: Math.max(0, (input.causal?.remainingAgentTurns ?? 4) - 1),
        };
        const createdAt = new Date(yield* Clock.currentTimeMillis).toISOString();
        const queued: PersonalOutboxItem[] =
          result._tag === "Success"
            ? (result.value.goalMessages ?? []).map((message) => ({
                input: {
                  requestId: `personal:${randomUUID()}`,
                  causationId: input.causationId,
                  causal,
                  source: "/personal" as const,
                  target: `/goals/${message.goalSlug}`,
                  expectedRevision: message.goalRevision,
                  createdAt,
                  text: message.text,
                },
                acceptedRevision: (current.revision ?? 0) + 1,
                status: "pending" as const,
              }))
            : [];
        if (result._tag === "Success")
          for (const proposal of result.value.signalCommands ?? [])
            queued.push({
              input: {
                operation: proposal.operation,
                requestId: `personal:${randomUUID()}`,
                causationId: input.causationId,
                causal,
                source: "/personal",
                target: `/signals/${proposal.signalSlug}`,
                expectedRevision: proposal.signalRevision,
                createdAt,
                definition: proposal.definition,
                active: proposal.active,
              },
              acceptedRevision: (current.revision ?? 0) + 1,
              status: "pending",
            });
        if (result._tag === "Success")
          for (const proposal of result.value.tasks ?? [])
            queued.push({
              input: personalTaskIntent(proposal, {
                requestId: `personal:${randomUUID()}`,
                causationId: input.causationId,
                causal,
                createdAt,
              }),
              acceptedRevision: (current.revision ?? 0) + 1,
              status: "pending",
            });
        if (result._tag === "Success")
          for (const proposal of result.value.approvalRequests ?? [])
            queued.push({
              input: {
                operation: "requestApproval",
                requestId: `personal:${randomUUID()}`,
                causationId: input.causationId,
                causal,
                source: "/personal",
                target: "/approvals",
                expectedRevision: proposal.approvalsRevision,
                contextPath: proposal.contextPath,
                contextRevision: proposal.contextRevision,
                approvalId: proposal.approvalId,
                createdAt,
              },
              acceptedRevision: (current.revision ?? 0) + 1,
              status: "pending",
            });
        const activeRun = state.runs?.findLast((run) => run.requestId === command.requestId);
        const runs = (state.runs ?? []).map((run) =>
          run === activeRun
            ? {
                ...run,
                status: result._tag === "Success" ? "completed" : "failed",
                ...(result._tag === "Failure" ? { error: result.error.message } : {}),
              }
            : run,
        );
        const reply: PersonalMessage[] =
          result._tag === "Success"
            ? [
                {
                  requestId: `reply:${input.sequence}`,
                  causationId: input.causationId,
                  causal,
                  source: "/personal",
                  target: "user",
                  revision: (current.revision ?? 0) + 1,
                  sequence: messages.length + 1,
                  createdAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
                  payload: {
                    _tag: "AgentReply",
                    text: result.value.text,
                    inputRequestId: input.requestId,
                  },
                },
              ]
            : [];
        yield* registry
          .commit(
            {
              ...current,
              state: {
                ...state,
                outbox: [...(state.outbox ?? []), ...queued],
                runs,
                pendingRequestIds:
                  result._tag === "Success"
                    ? state.pendingRequestIds.slice(1)
                    : state.pendingRequestIds,
                processedThrough:
                  result._tag === "Success" ? input.sequence : state.processedThrough,
              },
              messages: [...messages, ...reply],
            },
            { expectedRevision: current.revision ?? 0 },
          )
          .pipe(Effect.orDie);
        generation = undefined;
        if (result._tag === "Success") yield* actor.self.tell({ _tag: "Process" });
        if (queued.length) yield* actor.self.tell({ _tag: "DeliverOutbox" });
      });
      const retry = Effect.fn("PersonalAgent.retry")(function* (
        raw: PersonalRetryInput,
      ): Effect.fn.Return<PersonalReply, ApplicationError> {
        const input = yield* Schema.decodeUnknownEffect(PersonalRetryInput)(raw).pipe(
          Effect.mapError(
            () =>
              new ApplicationError({ kind: "invalid-input", message: "Invalid Personal retry" }),
          ),
        );
        const current = snapshot();
        const state = Schema.decodeUnknownSync(PersonalState)(current.state);
        const runs = state.runs ?? [];
        const executionId = `retry:${input.requestId}`;
        const duplicate = runs.find((run) => run.executionId === executionId);
        if (duplicate) {
          if (duplicate.requestId !== input.inputRequestId)
            return yield* new ApplicationError({
              kind: "conflict",
              message: "Retry ID belongs to another input",
            });
          return {
            _tag: "Accepted",
            receipt: {
              requestId: input.requestId,
              sequence: duplicate.inputSequence,
              revision: duplicate.revision!,
            },
          };
        }
        const previous = runs.findLast((run) => run.requestId === input.inputRequestId);
        if (
          generation ||
          !previous ||
          previous.status !== "failed" ||
          state.pendingRequestIds[0] !== input.inputRequestId
        )
          return yield* new ApplicationError({
            kind: "conflict",
            message: "Only the failed pending Personal input can be retried",
          });
        const revision = (current.revision ?? 0) + 1;
        // Processing has read-only tools. A requested new attempt cannot replay
        // an external action; future business outputs are applied by the outbox.
        yield* registry
          .commit(
            {
              ...current,
              state: {
                ...state,
                runs: [
                  ...runs,
                  {
                    requestId: input.inputRequestId,
                    executionId,
                    revision,
                    inputSequence: previous.inputSequence,
                    status: "running",
                    startedAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
                  },
                ],
              },
            },
            { expectedRevision: input.expectedRevision },
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
        return {
          _tag: "Accepted",
          receipt: { requestId: input.requestId, revision, sequence: previous.inputSequence },
        };
      });
      return PersonalAgentActor.of({
        started: (actor) =>
          Effect.gen(function* () {
            if (contextPath(actor) !== "/personal")
              return yield* Effect.die(new Error("Personal Agent must own /personal"));
            const current = registry.get("/personal");
            if (current) {
              Schema.decodeUnknownSync(PersonalState)(current.state);
              Schema.decodeUnknownSync(Schema.Array(PersonalMessage))(current.messages);
              yield* actor.self.tell({ _tag: "Process" });
              yield* actor.self.tell({ _tag: "DeliverOutbox", recover: true });
              return;
            }
            yield* registry.commit(
              {
                path: "/personal",
                description: "Personal Agent conversations and pending work",
                state: {
                  owner: { kind: "ownerless", id: "personal" },
                  pendingRequestIds: [],
                  processedThrough: 0,
                },
                messages: [],
              },
              { expectedRevision: 0 },
            );
            yield* actor.self.tell({ _tag: "Process" });
            yield* actor.self.tell({ _tag: "DeliverOutbox", recover: true });
          }),
        receive: (command, actor) => {
          if (command._tag === "DeliverOutbox") return outbox.deliver(actor, command.recover);
          if (command._tag === "OutboxResult") return outbox.settled(command, actor);
          if (command._tag === "Process") return process(actor);
          if (command._tag === "Processed") return processed(command, actor);
          return Match.value(command).pipe(
            Match.tag("Get", () => read("/personal")),
            Match.tag("InspectDelegation", ({ path }) =>
              inspectDelegation(path, readRecord).pipe(
                Effect.map((inspection): PersonalReply => ({ _tag: "Delegation", inspection })),
              ),
            ),
            Match.tag("ReadContext", ({ path }) => read(path)),
            Match.tag("ListContexts", () =>
              Effect.sync((): PersonalReply => ({
                _tag: "Contexts",
                records: Object.values(registry.publicSnapshot()),
              })),
            ),
            Match.tag("Accept", ({ input }) => accept(input)),
            Match.tag("Notify", ({ input }) =>
              acceptBusinessNotification(registry, input).pipe(
                Effect.map((receipt): PersonalReply => ({ _tag: "Accepted", receipt })),
              ),
            ),
            Match.tag("ResumeRun", ({ input }) =>
              outbox
                .enqueueResume(input)
                .pipe(Effect.map((receipt): PersonalReply => ({ _tag: "Queued", receipt }))),
            ),
            Match.tag("StartTask", ({ input }) =>
              outbox
                .enqueueTask(input)
                .pipe(Effect.map((receipt): PersonalReply => ({ _tag: "Queued", receipt }))),
            ),
            Match.tag("ApplySignal", ({ input }) =>
              outbox
                .enqueueSignal(input)
                .pipe(Effect.map((receipt): PersonalReply => ({ _tag: "Queued", receipt }))),
            ),
            Match.tag("RequestApproval", ({ input }) =>
              outbox
                .enqueueApprovalRequest(input)
                .pipe(Effect.map((receipt): PersonalReply => ({ _tag: "Queued", receipt }))),
            ),
            Match.tag("RespondApproval", ({ input }) =>
              outbox
                .enqueueApproval(input)
                .pipe(Effect.map((receipt): PersonalReply => ({ _tag: "Queued", receipt }))),
            ),
            Match.tag("SendGoalMessage", ({ input }) =>
              outbox
                .enqueue(input)
                .pipe(Effect.map((receipt): PersonalReply => ({ _tag: "Queued", receipt }))),
            ),
            Match.tag("Retry", ({ input }) => retry(input)),
            Match.exhaustive,
            Effect.catchTag("ApplicationError", (error) =>
              Effect.succeed({ _tag: "Rejected" as const, error }),
            ),
            Effect.flatMap((response) => command.replyTo.tell(response)),
            Effect.andThen(
              command._tag === "SendGoalMessage" ||
                command._tag === "ResumeRun" ||
                command._tag === "StartTask" ||
                command._tag === "ApplySignal" ||
                command._tag === "RespondApproval" ||
                command._tag === "RequestApproval"
                ? actor.self.tell({ _tag: "DeliverOutbox" })
                : Effect.void,
            ),
            Effect.andThen(
              command._tag === "Accept" || command._tag === "Notify" || command._tag === "Retry"
                ? actor.self.tell({ _tag: "Process" })
                : Effect.void,
            ),
          );
        },
      });
    }),
  );
}
