import { isDeepStrictEqual } from "node:util";
import { ApprovalEntry, ApprovalResponse, InputRequest } from "./contracts.js";
import { Clock, Context, Data, Effect, Layer, Match, Ref, Schema } from "effect";
import { ContextRegistry } from "../context/registry.js";

export const ApprovalSnapshot = Schema.Struct({
  entries: Schema.Array(ApprovalEntry),
  revokedIds: Schema.Array(Schema.String),
});
export const ApprovalEvent = Schema.Struct({
  type: Schema.Literals(["Requested", "Revoked", "Resolved", "Acknowledged"]),
  requestId: Schema.String,
  at: Schema.String,
  response: Schema.optional(ApprovalResponse),
});
export const approvalEntries = (registry: ContextRegistry["Service"]): readonly ApprovalEntry[] => {
  const record = registry.get("/approvals");
  return record ? Schema.decodeUnknownSync(ApprovalSnapshot)(record.state).entries : [];
};

const hasAnswer = (values: readonly string[] = []): boolean =>
  values.some((value) => value.trim().length > 0);

export class ApprovalValidationError extends Data.TaggedError("ApprovalValidationError")<{
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

const makeApprovalState = Effect.gen(function* () {
  const registry = yield* ContextRegistry;
  const snapshot = yield* Ref.make<typeof ApprovalSnapshot.Type | undefined>(undefined);
  const read = Ref.get(snapshot).pipe(
    Effect.flatMap((state) =>
      state ? Effect.succeed(state) : Effect.die(new Error("Approval state not restored")),
    ),
  );
  const commit = Effect.fn("Approvals.commit")(function* (
    state: typeof ApprovalSnapshot.Type,
    event?: Omit<typeof ApprovalEvent.Type, "at">,
  ) {
    const previous = registry.get("/approvals");
    const messages = [...(previous?.messages ?? [])];
    if (event)
      messages.push({ ...event, at: new Date(yield* Clock.currentTimeMillis).toISOString() });
    yield* registry
      .commit(
        {
          path: "/approvals",
          description: "Task requests awaiting confirmation or additional information",
          state,
          messages,
        },
        { expectedRevision: previous?.revision ?? 0 },
      )
      .pipe(Effect.orDie);
    yield* Ref.set(snapshot, state);
  }, Effect.uninterruptible);
  return {
    restore: Effect.gen(function* () {
      const initial = registry.get("/approvals");
      if (initial)
        yield* Ref.set(snapshot, Schema.decodeUnknownSync(ApprovalSnapshot)(initial.state));
      else yield* commit({ entries: [], revokedIds: [] });
    }),
    pending: read.pipe(
      Effect.map((state) => state.entries.filter((entry) => entry.status === "resolved")),
    ),
    enqueue: Effect.fn("Approvals.enqueue")(function* (entry: ApprovalEntry) {
      const state = yield* read;
      if (state.revokedIds.includes(entry.id)) return;
      const previous = state.entries.find((item) => item.id === entry.id);
      if (previous) {
        if (!isDeepStrictEqual(previous.request, entry.request) || previous.target !== entry.target)
          return yield* Effect.die(new Error("Approval identity collision"));
        return;
      }
      if (!entry.target.startsWith("/user/") || entry.status !== "pending")
        return yield* Effect.die(new Error("Invalid approval destination/state"));
      yield* commit(
        { ...state, entries: [...state.entries, entry] },
        { type: "Requested", requestId: entry.id },
      );
    }),
    revoke: Effect.fn("Approvals.revoke")(function* (id: string) {
      const state = yield* read;
      if (
        state.revokedIds.includes(id) ||
        state.entries.some((entry) => entry.id === id && entry.status === "acknowledged")
      )
        return;
      yield* commit(
        {
          entries: state.entries.map((entry) =>
            entry.id === id ? { ...entry, status: "revoked" } : entry,
          ),
          revokedIds: [...state.revokedIds, id],
        },
        { type: "Revoked", requestId: id },
      );
    }),
    resolve: Effect.fn("Approvals.resolve")(function* (id: string, response: ApprovalResponse) {
      const state = yield* read;
      const entry = state.entries.find((item) => item.id === id);
      if (entry?.response && (entry.status === "resolved" || entry.status === "acknowledged")) {
        const accepted = yield* validateApprovalResponse({ ...entry, status: "pending" }, response);
        if (isDeepStrictEqual(entry.response, accepted)) return;
      }
      const accepted = yield* validateApprovalResponse(entry, response);
      yield* commit(
        {
          ...state,
          entries: state.entries.map((entry) =>
            entry.id === id ? { ...entry, response: accepted, status: "resolved" } : entry,
          ),
        },
        { type: "Resolved", requestId: id, response: accepted },
      );
    }),
    acknowledge: Effect.fn("Approvals.acknowledge")(function* (id: string, target: string) {
      const state = yield* read;
      if (
        !state.entries.some(
          (entry) => entry.id === id && entry.target === target && entry.status === "resolved",
        )
      )
        return;
      yield* commit(
        {
          ...state,
          entries: state.entries.map((entry) =>
            entry.id === id ? { ...entry, status: "acknowledged" } : entry,
          ),
        },
        { type: "Acknowledged", requestId: id },
      );
    }),
  };
});

/** Actor-local business state; the mailbox is its sole writer. */
export class ApprovalState extends Context.Service<
  ApprovalState,
  Effect.Success<typeof makeApprovalState>
>()("approvals/State") {
  static readonly layer = Layer.effect(ApprovalState, makeApprovalState);
}
