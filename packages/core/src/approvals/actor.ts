import { requestedApproval } from "./request.js";
import {
  ApplicationError,
  ApprovalDelivery,
  ApprovalDeliveryInput,
  ApprovalRequestDelivery,
  ApprovalRequestDeliveryInput,
  ApprovalEntry,
  CommandReceipt,
} from "@aster/api-contracts";
import { isDeepStrictEqual } from "node:util";
export { ApprovalEntry } from "@aster/api-contracts";
import { ContextActor } from "../context/actor.js";
import { defineContext } from "../context/definition.js";
import { ContextRegistry } from "../context/registry.js";
import { ApprovalResponse, InputRequest } from "../tasks/model.js";
import { ReplyTo, type ActorContext } from "@aster/actor";
import { Clock, Data, Effect, Layer, Match, Schema } from "effect";

export const ApprovalCommandReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type ApprovalCommandReply = typeof ApprovalCommandReply.Type;
const ApprovalState = Schema.Struct({
  entries: Schema.Array(ApprovalEntry),
  revokedIds: Schema.optional(Schema.Array(Schema.String)),
  commandReceipts: Schema.optional(
    Schema.Array(Schema.Union([ApprovalDelivery, ApprovalRequestDelivery])),
  ),
});

export const ApprovalResolved = Schema.TaggedStruct("ApprovalResolved", {
  requestId: Schema.String,
  response: ApprovalResponse,
});
export const ApprovalCommand = Schema.Union([
  Schema.TaggedStruct("RequestPersonal", {
    input: ApprovalRequestDeliveryInput,
    replyTo: ReplyTo<ApprovalCommandReply>(),
  }),
  Schema.TaggedStruct("RespondPersonal", {
    input: ApprovalDeliveryInput,
    replyTo: ReplyTo<ApprovalCommandReply>(),
  }),
  Schema.TaggedStruct("Revoke", { id: Schema.String }),
  Schema.TaggedStruct("Enqueue", { entry: ApprovalEntry }),
  Schema.TaggedStruct("Resolve", {
    id: Schema.String,
    response: ApprovalResponse,
    replyTo: ReplyTo<{ error?: string }>(),
  }),
  Schema.TaggedStruct("Acknowledge", { id: Schema.String, target: Schema.String }),
  Schema.TaggedStruct("Deliver", {}),
]);
export type ApprovalCommand = typeof ApprovalCommand.Type;
export const approvalEntries = (registry: ContextRegistry["Service"]): readonly ApprovalEntry[] =>
  (registry.get("/approvals")?.state as { entries?: readonly ApprovalEntry[] } | undefined)
    ?.entries ?? [];
export const sendApproval = (context: ActorContext<any, any>, command: ApprovalCommand) =>
  context
    .select("/user/approvals")
    .resolve()
    .pipe(
      Effect.flatMap((ref) => ref.tell(command)),
      Effect.orDie,
    );

const hasAnswer = (values: readonly string[] = []): boolean =>
  values.some((value) => value.trim().length > 0);

class ApprovalValidationError extends Data.TaggedError("ApprovalValidationError")<{
  readonly message: string;
}> {}

const validateInputResponse = Effect.fnUntraced(function* (
  request: InputRequest,
  response: ApprovalResponse,
) {
  const hasText = Boolean(response.text?.trim());
  const answers = { ...response.answers };
  const questions = request.questions ?? [];
  if (!hasText && !Object.values(answers).some(hasAnswer))
    return yield* new ApprovalValidationError({ message: "Input is required" });
  for (const question of questions) {
    const values = Match.value({
      keyed: answers[question.id] ?? [],
      singleText: questions.length === 1 && hasText,
    }).pipe(
      Match.when(
        ({ keyed }) => hasAnswer(keyed),
        ({ keyed }) => keyed,
      ),
      Match.when({ singleText: true }, () => [response.text!]),
      Match.orElse(() => []),
    );
    yield* Match.value({
      answered: hasAnswer(values),
      invalidChoice:
        !!question.options?.length &&
        question.allowOther !== true &&
        values.some((value) => !question.options!.includes(value)),
      invalidCount: question.multiple === false && values.length !== 1,
    }).pipe(
      Match.when({ answered: false }, () =>
        Effect.fail(new ApprovalValidationError({ message: "Every question requires an answer" })),
      ),
      Match.when({ invalidChoice: true }, () =>
        Effect.fail(
          new ApprovalValidationError({ message: "Answer must match an offered option" }),
        ),
      ),
      Match.when({ invalidCount: true }, () =>
        Effect.fail(
          new ApprovalValidationError({ message: "Single-choice question requires one answer" }),
        ),
      ),
      Match.orElse(() => Effect.void),
    );
    answers[question.id] = values;
  }
  return questions.length ? { ...response, answers } : response;
});

const validateApprovalResponse = (
  entry: ApprovalEntry | undefined,
  response: ApprovalResponse,
): Effect.Effect<ApprovalResponse, ApprovalValidationError> =>
  Effect.suspend(() =>
    Match.value({ entry, decision: response.decision }).pipe(
      Match.when({ entry: undefined }, () =>
        Effect.fail(new ApprovalValidationError({ message: "Approval not found" })),
      ),
      Match.not({ entry: { status: "pending" } }, () =>
        Effect.fail(new ApprovalValidationError({ message: "Approval already resolved" })),
      ),
      Match.when({ entry: { kind: "input" } }, ({ entry }) =>
        validateInputResponse(entry.request, response),
      ),
      Match.when({ decision: undefined }, () =>
        Effect.fail(new ApprovalValidationError({ message: "Approval decision is required" })),
      ),
      Match.orElse(() => Effect.succeed(response)),
    ),
  );

export class ApprovalQueueActor extends ContextActor.Service<ApprovalQueueActor>()(
  "approvals/Queue",
  {
    command: ApprovalCommand,
    context: defineContext({
      changes: "none",
      state: ApprovalState,
      message: Schema.Unknown,
    }),
  },
) {
  static readonly layer = Layer.effect(
    ApprovalQueueActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const save = (
        entries: readonly ApprovalEntry[],
        event: object,
        revokedIds?: readonly string[],
      ) => {
        const current = registry.get("/approvals")!;
        return registry
          .commit(
            {
              ...current,
              state: { ...current.state, entries, ...(revokedIds ? { revokedIds } : {}) },
              messages: [...current.messages, { ...event, at: new Date().toISOString() }],
            },
            { expectedRevision: current.revision ?? 0 },
          )
          .pipe(Effect.asVoid, Effect.orDie);
      };
      const respondPersonal = Effect.fn("ApprovalQueue.respondPersonal")(function* (
        raw: ApprovalDeliveryInput,
      ) {
        const input = yield* Schema.decodeUnknownEffect(ApprovalDeliveryInput)(raw).pipe(
          Effect.mapError(
            () =>
              new ApplicationError({ kind: "invalid-input", message: "Invalid approval command" }),
          ),
        );
        const current = registry.get("/approvals")!;
        const state = Schema.decodeUnknownSync(ApprovalState)(current.state);
        const previous = state.commandReceipts?.find(
          (item) => item.input.requestId === input.requestId,
        );
        if (previous) {
          if (!isDeepStrictEqual(previous.input, input))
            return yield* new ApplicationError({
              kind: "conflict",
              message: "Approval command ID belongs to another response",
            });
          return previous.receipt;
        }
        const entry = state.entries.find((item) => item.id === input.approvalId);
        const response = yield* validateApprovalResponse(entry, input.response).pipe(
          Effect.catchTag("ApprovalValidationError", ({ message }) =>
            Effect.fail(new ApplicationError({ kind: "conflict", message })),
          ),
        );
        const receipt = { requestId: input.requestId, revision: (current.revision ?? 0) + 1 };
        const at = new Date(yield* Clock.currentTimeMillis).toISOString();
        yield* registry
          .commit(
            {
              ...current,
              state: {
                ...state,
                entries: state.entries.map((item) =>
                  item.id === input.approvalId ? { ...item, response, status: "resolved" } : item,
                ),
                commandReceipts: [...(state.commandReceipts ?? []), { input, receipt }],
              },
              messages: [
                ...current.messages,
                {
                  type: "Resolved",
                  requestId: input.approvalId,
                  commandId: input.requestId,
                  causationId: input.causationId,
                  source: input.source,
                  target: input.target,
                  revision: receipt.revision,
                  response,
                  at,
                },
              ],
            },
            { expectedRevision: input.expectedRevision },
          )
          .pipe(
            Effect.catchTag("ContextConflict", () =>
              Effect.fail(
                new ApplicationError({
                  kind: "conflict",
                  message:
                    "Approval Context revision changed; refresh before issuing a new response",
                }),
              ),
            ),
            Effect.catchTag("ContextCommitError", Effect.die),
            Effect.catchTag("ContextValidationError", Effect.die),
          );
        return receipt;
      });
      const requestPersonal = Effect.fn("ApprovalQueue.requestPersonal")(function* (
        raw: ApprovalRequestDeliveryInput,
      ) {
        const input = yield* Schema.decodeUnknownEffect(ApprovalRequestDeliveryInput)(raw).pipe(
          Effect.mapError(
            () =>
              new ApplicationError({
                kind: "invalid-input",
                message: "Invalid approval request command",
              }),
          ),
        );
        const current = registry.get("/approvals")!;
        const state = Schema.decodeUnknownSync(ApprovalState)(current.state);
        const previous = state.commandReceipts?.find(
          (item) => item.input.requestId === input.requestId,
        );
        if (previous) {
          if (!isDeepStrictEqual(previous.input, input))
            return yield* new ApplicationError({
              kind: "conflict",
              message: "Approval command ID belongs to another command",
            });
          return previous.receipt;
        }
        if (state.revokedIds?.includes(input.approvalId))
          return yield* new ApplicationError({
            kind: "conflict",
            message: "Approval demand was revoked",
          });
        const entry = yield* requestedApproval(registry, input);
        const existing = state.entries.find((item) => item.id === entry.id);
        if (existing && !isDeepStrictEqual(existing, entry))
          return yield* new ApplicationError({
            kind: "conflict",
            message: "Approval already resolved, revoked, or bound to another demand",
          });
        const receipt = { requestId: input.requestId, revision: (current.revision ?? 0) + 1 };
        yield* registry
          .commit(
            {
              ...current,
              state: {
                ...state,
                entries: existing ? state.entries : [...state.entries, entry],
                commandReceipts: [...(state.commandReceipts ?? []), { input, receipt }],
              },
              messages: [
                ...current.messages,
                {
                  type: "RequestAdmitted",
                  approvalId: entry.id,
                  requestId: input.requestId,
                  causationId: input.causationId,
                  source: input.source,
                  contextPath: input.contextPath,
                  contextRevision: input.contextRevision,
                  revision: receipt.revision,
                  at: input.createdAt,
                },
              ],
            },
            { expectedRevision: input.expectedRevision },
          )
          .pipe(
            Effect.catchTag("ContextConflict", () =>
              Effect.fail(
                new ApplicationError({
                  kind: "conflict",
                  message: "Approval queue revision changed",
                }),
              ),
            ),
            Effect.catchTag("ContextCommitError", Effect.die),
            Effect.catchTag("ContextValidationError", Effect.die),
          );
        return receipt;
      });
      return ApprovalQueueActor.of({
        started: (context) =>
          Effect.gen(function* () {
            if (!registry.get("/approvals"))
              yield* registry
                .commit(
                  {
                    path: "/approvals",
                    description: "Task requests awaiting my confirmation or additional information",
                    state: { entries: [] },
                    messages: [],
                  },
                  { expectedRevision: 0 },
                )
                .pipe(Effect.asVoid, Effect.orDie);
            yield* context.self.tell({ _tag: "Deliver" });
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("RequestPersonal", ({ input, replyTo }) =>
              requestPersonal(input).pipe(
                Effect.map((receipt): ApprovalCommandReply => ({ _tag: "Accepted", receipt })),
                Effect.catchTag("ApplicationError", (error) =>
                  Effect.succeed({ _tag: "Rejected" as const, error }),
                ),
                Effect.flatMap((reply) => replyTo.tell(reply)),
              ),
            ),
            Match.tag("RespondPersonal", ({ input, replyTo }) =>
              respondPersonal(input).pipe(
                Effect.map((receipt): ApprovalCommandReply => ({ _tag: "Accepted", receipt })),
                Effect.catchTag("ApplicationError", (error) =>
                  Effect.succeed({ _tag: "Rejected" as const, error }),
                ),
                Effect.flatMap((reply) => replyTo.tell(reply)),
              ),
            ),
            Match.tag("Enqueue", ({ entry }) =>
              Effect.gen(function* () {
                if (
                  Schema.decodeUnknownSync(ApprovalState)(
                    registry.get("/approvals")!.state,
                  ).revokedIds?.includes(entry.id)
                )
                  return;
                const existing = approvalEntries(registry).find((item) => item.id === entry.id);
                if (existing) return;
                if (!entry.target.startsWith("/user/") || entry.status !== "pending")
                  return yield* Effect.die(new Error("Invalid approval destination/state"));
                yield* save([...approvalEntries(registry), entry], {
                  type: "Requested",
                  requestId: entry.id,
                });
              }),
            ),
            Match.tag("Revoke", ({ id }) =>
              Effect.gen(function* () {
                const state = Schema.decodeUnknownSync(ApprovalState)(
                  registry.get("/approvals")!.state,
                );
                if (
                  state.revokedIds?.includes(id) ||
                  state.entries.some((entry) => entry.id === id && entry.status === "acknowledged")
                )
                  return;
                yield* save(
                  approvalEntries(registry).map((e) =>
                    e.id === id ? { ...e, status: "revoked" } : e,
                  ),
                  { type: "Revoked", requestId: id },
                  [...(state.revokedIds ?? []), id],
                );
              }),
            ),
            Match.tag("Resolve", ({ id, response, replyTo }) =>
              Effect.gen(function* () {
                const entry = approvalEntries(registry).find((item) => item.id === id);
                const accepted = yield* validateApprovalResponse(entry, response);
                yield* save(
                  approvalEntries(registry).map((item) =>
                    item.id === id ? { ...item, response: accepted, status: "resolved" } : item,
                  ),
                  { type: "Resolved", requestId: id, response: accepted },
                );
                yield* replyTo.tell({});
              }).pipe(
                // Only expected validation failures become replies; defects still reach supervision.
                Effect.catchTag("ApprovalValidationError", ({ message }) =>
                  replyTo.tell({ error: message }),
                ),
              ),
            ),
            Match.tag("Acknowledge", ({ id, target }) =>
              Effect.gen(function* () {
                if (
                  !approvalEntries(registry).some(
                    (item) =>
                      item.id === id && item.target === target && item.status === "resolved",
                  )
                )
                  return;
                yield* save(
                  approvalEntries(registry).map((item) =>
                    item.id === id ? { ...item, status: "acknowledged" } : item,
                  ),
                  { type: "Acknowledged", requestId: id },
                );
              }),
            ),
            Match.tag("Deliver", () =>
              Effect.gen(function* () {
                for (const entry of approvalEntries(registry)) {
                  if (entry.status !== "resolved" || !entry.response) continue;
                  yield* context
                    .select(entry.target)
                    .resolve()
                    .pipe(
                      Effect.flatMap((ref) =>
                        ref.tell({
                          _tag: "ApprovalResolved",
                          requestId: entry.id,
                          response: entry.response,
                        }),
                      ),
                      Effect.catch(() => Effect.void),
                    );
                }
                yield* context.pipeToSelf(Effect.sleep("1 second"), () => ({ _tag: "Deliver" }));
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}
