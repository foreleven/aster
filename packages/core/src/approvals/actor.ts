import { ApprovalEntry, ApprovalResponse, ApplicationError } from "@aster/api-contracts";
export { ApprovalEntry } from "@aster/api-contracts";
export { approvalEntries } from "./state.js";
import { ApprovalState, ApprovalSnapshot, ApprovalEvent } from "./state.js";
import { ContextActor } from "../context/actor.js";
import { defineContext } from "../context/definition.js";
import { ReplyTo, type ActorContext } from "@aster/actor";
import { Effect, Layer, Match, Schema } from "effect";

export const ApprovalResolved = Schema.TaggedStruct("ApprovalResolved", {
  requestId: Schema.String,
  response: ApprovalResponse,
});
export const ApprovalReply = Schema.Union([
  Schema.TaggedStruct("Accepted", {}),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type ApprovalReply = typeof ApprovalReply.Type;
export const ApprovalCommand = Schema.Union([
  Schema.TaggedStruct("Revoke", { id: Schema.String }),
  Schema.TaggedStruct("Enqueue", { entry: ApprovalEntry }),
  Schema.TaggedStruct("Resolve", {
    id: Schema.String,
    response: ApprovalResponse,
    replyTo: ReplyTo<ApprovalReply>(),
  }),
  Schema.TaggedStruct("Acknowledge", { id: Schema.String, target: Schema.String }),
  Schema.TaggedStruct("Deliver", {}),
]);
export type ApprovalCommand = typeof ApprovalCommand.Type;
export const sendApproval = (
  context: Pick<ActorContext<unknown>, "select">,
  command: ApprovalCommand,
) =>
  context
    .select("/user/approvals")
    .resolve()
    .pipe(
      Effect.flatMap((ref) => ref.tell(command)),
      Effect.orDie,
    );

export class ApprovalQueueActor extends ContextActor.Service<ApprovalQueueActor>()(
  "approvals/Queue",
  {
    command: ApprovalCommand,
    context: defineContext({ changes: "none", state: ApprovalSnapshot, message: ApprovalEvent }),
  },
) {
  static readonly layer = Layer.effect(
    ApprovalQueueActor,
    Effect.gen(function* () {
      const state = yield* ApprovalState;
      const deliver = Effect.fn("Approvals.deliver")(function* (
        context: ActorContext<ApprovalCommand>,
      ) {
        for (const entry of yield* state.pending) {
          if (!entry.response) continue;
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
              Effect.catchTag("ActorNotFound", () => Effect.void),
            );
        }
      });
      return ApprovalQueueActor.of({
        started: (context) =>
          state.restore.pipe(Effect.andThen(context.self.tell({ _tag: "Deliver" }))),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("Enqueue", ({ entry }) => state.enqueue(entry)),
            Match.tag("Revoke", ({ id }) => state.revoke(id)),
            Match.tag("Acknowledge", ({ id, target }) => state.acknowledge(id, target)),
            Match.tag("Resolve", ({ id, response, replyTo }) =>
              Effect.gen(function* () {
                const result = yield* state.resolve(id, response).pipe(Effect.result);
                if (result._tag === "Failure")
                  return yield* replyTo.tell({
                    _tag: "Rejected",
                    error: new ApplicationError({
                      kind: "conflict",
                      message: result.failure.message,
                    }),
                  });
                yield* replyTo.tell({ _tag: "Accepted" });
                yield* deliver(context);
              }),
            ),
            Match.tag("Deliver", () =>
              deliver(context).pipe(
                Effect.andThen(
                  context.pipeToSelf(Effect.sleep("1 second"), () => ({ _tag: "Deliver" })),
                ),
              ),
            ),
            Match.exhaustive,
          ),
      });
    }),
  ).pipe(Layer.provide(ApprovalState.layer));
}
