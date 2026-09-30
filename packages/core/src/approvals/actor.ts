import { ApprovalEntry } from "@aster/api-contracts";
export { ApprovalEntry } from "@aster/api-contracts";
import { ContextActor } from "../context/actor.js";
import { defineContext } from "../context/model.js";
import { ContextRegistry } from "../context/registry.js";
import { ApprovalResponse, InputRequest } from "../tasks/model.js";
import { ReplyTo, type ActorContext } from "@aster/actor";
import { Data, Effect, Layer, Match, Schema } from "effect";

export const ApprovalResolved = Schema.TaggedStruct("ApprovalResolved", {
  requestId: Schema.String,
  response: ApprovalResponse,
});
export const ApprovalCommand = Schema.Union([
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
      identity: "Unified task approval queue",
      signalSource: false,
      state: Schema.Struct({ entries: Schema.Array(ApprovalEntry) }),
      message: Schema.Unknown,
    }),
  },
) {
  static readonly layer = Layer.effect(
    ApprovalQueueActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const save = (entries: readonly ApprovalEntry[], event: object) => {
        const current = registry.get("/approvals")!;
        return registry.set({
          ...current,
          state: { entries },
          messages: [...current.messages, { ...event, at: new Date().toISOString() }],
        });
      };
      return ApprovalQueueActor.of({
        started: (context) =>
          Effect.gen(function* () {
            if (!registry.get("/approvals"))
              yield* registry.set({
                path: "/approvals",
                description: "Task requests awaiting my confirmation or additional information",
                state: { entries: [] },
                messages: [],
              });
            yield* context.self.tell({ _tag: "Deliver" });
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("Enqueue", ({ entry }) =>
              Effect.gen(function* () {
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
                if (
                  !approvalEntries(registry).some(
                    (e) => e.id === id && e.status !== "acknowledged" && e.status !== "revoked",
                  )
                )
                  return;
                yield* save(
                  approvalEntries(registry).map((e) =>
                    e.id === id ? { ...e, status: "revoked" } : e,
                  ),
                  { type: "Revoked", requestId: id },
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
