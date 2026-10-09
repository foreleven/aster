import { Command as ActorCommand, type ActorContext } from "@aster/actor";
import { Effect, Match, Schema } from "effect";
import { ContextActor } from "../context/actor.js";
import { ApplicationError } from "../operations.js";
import { ApprovalEntry, ApprovalResponse } from "./contracts.js";
import { ApprovalState } from "./state.js";
export { ApprovalEntry } from "./contracts.js";
export { approvalEntries } from "./state.js";

export class ApprovalResolved extends ActorCommand.Class<ApprovalResolved>()("ApprovalResolved", {
  payload: {
    requestId: Schema.String,
    response: ApprovalResponse,
  },
}) {}
export const ApprovalReply = Schema.TaggedUnion({
  Accepted: {},
  Rejected: { error: ApplicationError },
});
export type ApprovalReply = typeof ApprovalReply.Type;
export class Revoke extends ActorCommand.Class<Revoke>()("Revoke", {
  payload: { id: Schema.String },
}) {}
export class Enqueue extends ActorCommand.Class<Enqueue>()("Enqueue", {
  payload: { entry: ApprovalEntry },
}) {}
export class Resolve extends ActorCommand.Class<Resolve>()("Resolve", {
  payload: { id: Schema.String, response: ApprovalResponse },
  reply: ApprovalReply,
}) {}
export class Acknowledge extends ActorCommand.Class<Acknowledge>()("Acknowledge", {
  payload: { id: Schema.String, target: Schema.String },
}) {}
const Deliver = Schema.TaggedStruct("Deliver", {});
export const ApprovalCommand = Schema.TaggedUnion({
  Revoke: Revoke.fields,
  Enqueue: Enqueue.fields,
  Resolve: Resolve.fields,
  Acknowledge: Acknowledge.fields,
});
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

export const ApprovalQueueActor = ContextActor.define("approvals/Queue", {
  commands: [Revoke, Enqueue, Resolve, Acknowledge],
  internal: Deliver,
})(
  Effect.gen(function* () {
    const state = yield* ApprovalState;
    const deliver = Effect.fn("Approvals.deliver")(function* (
      context: Pick<ActorContext<unknown>, "select">,
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
    return {
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
    };
  }),
).pipe(ContextActor.provide(ApprovalState.layer));
