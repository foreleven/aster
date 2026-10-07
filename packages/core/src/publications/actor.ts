import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { ReplyTo, type ActorContext, type ActorRef } from "@aster/actor";
import { AgentConversations } from "@aster/agent";
import {
  ApplicationError,
  WritebackRequest,
  WritebackOperation,
  writebackApprovalId,
  writebackPrompt,
} from "@aster/api-contracts";
import { Clock, Effect, Layer, Match, Option, Schedule, Schema } from "effect";
import { ContextActor } from "../context/actor.js";
import { ContextRegistry } from "../context/registry.js";
import { defineContext } from "../context/definition.js";
import { contextView } from "../context/definition.js";
import { ApprovalResolved, approvalEntries, sendApproval } from "../approvals/actor.js";
import { ChannelWrites, ChannelWriteError } from "./contracts.js";

const PublicationState = Schema.Struct({ operations: Schema.Array(WritebackOperation) });
const Publish = Schema.TaggedStruct("Publish", {
  request: WritebackRequest,
  replyTo: ReplyTo<void>(),
});
const Command = Schema.Union([
  Publish,
  ApprovalResolved,
  Schema.TaggedStruct("Published", {
    generation: Schema.String,
    requestId: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.Struct({ externalId: Schema.String }) }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(ChannelWriteError) }),
    ]),
  }),
]);
export const publicationView = contextView({
  matches: (path) => path === "/publications",
  state: PublicationState,
});
export const publications = (registry: ContextRegistry["Service"]) => {
  const record = registry.get("/publications");
  return record ? Schema.decodeUnknownSync(PublicationState)(record.state).operations : [];
};
export const requestPublication = Effect.fn("Publications.request")(
  function* (actor: Pick<ActorContext<unknown>, "select">, request: WritebackRequest) {
    const root = yield* actor
      .select("/user/publications")
      .resolve()
      .pipe(
        Effect.mapError(
          () =>
            new ApplicationError({ kind: "unavailable", message: "Publication owner unavailable" }),
        ),
      );
    yield* (root as ActorRef<typeof Command.Type>)
      .ask<void>((replyTo) => ({ _tag: "Publish", request, replyTo }))
      .pipe(
        Effect.mapError(
          () =>
            new ApplicationError({
              kind: "unavailable",
              message: "Publication handoff unconfirmed",
            }),
        ),
      );
  },
  Effect.retry({
    schedule: Schedule.spaced("3 seconds"),
    while: (error) => error.kind === "unavailable",
  }),
);

/** Publication has a separate lifetime and approval; Task completion never depends on transport. */
export class PublicationsActor extends ContextActor.Service<
  PublicationsActor,
  AgentConversations
>()("publications/Actor", {
  command: Command,
  context: defineContext({ state: PublicationState, message: Schema.Never }),
}) {
  static readonly layer = Layer.effect(
    PublicationsActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const messages = yield* AgentConversations;
      const channel = yield* Effect.serviceOption(ChannelWrites);
      const generation = randomUUID();
      const at = Clock.currentTimeMillis.pipe(Effect.map((now) => new Date(now).toISOString()));
      const save = Effect.fnUntraced(function* (operation: WritebackOperation) {
        yield* messages
          .append(
            `/publications/${operation.request.requestId}`,
            operation.status,
            "publication.status",
            operation,
          )
          .pipe(Effect.orDie);
        yield* commit(operation);
      });
      const commit = Effect.fnUntraced(function* (operation: WritebackOperation) {
        const record = registry.get("/publications")!;
        const operations = publications(registry);
        yield* registry
          .commit(
            {
              ...record,
              state: {
                operations: [
                  ...operations.filter(
                    (item) => item.request.requestId !== operation.request.requestId,
                  ),
                  operation,
                ],
              },
            },
            { expectedRevision: record.revision! },
          )
          .pipe(Effect.orDie);
      });
      type Owner = ActorContext<typeof Command.Type>;
      const approval = (operation: WritebackOperation, owner: Owner) => ({
        id: writebackApprovalId(operation.request),
        contextPath: operation.request.source,
        target: owner.path,
        kind: "approval" as const,
        status: "pending" as const,
        request: {
          id: writebackApprovalId(operation.request),
          kind: "approval" as const,
          prompt: writebackPrompt(operation.request),
        },
      });
      const dispatch = Effect.fnUntraced(function* (operation: WritebackOperation, owner: Owner) {
        if (operation.status !== "authorized" || !operation.authorization) return;
        yield* save({ ...operation, status: "sending", submittedAt: yield* at });
        yield* owner.pipeToSelf(
          Option.isSome(channel)
            ? channel.value.publish(operation.request, operation.authorization)
            : Effect.fail(
                new ChannelWriteError({
                  outcome: "rejected",
                  message: "No Channel write adapter is installed",
                }),
              ),
          (result) => ({
            _tag: "Published",
            generation,
            requestId: operation.request.requestId,
            result,
          }),
        );
      });
      const recover = Effect.fnUntraced(function* (operation: WritebackOperation, owner: Owner) {
        if (operation.status === "sending")
          return yield* save({
            ...operation,
            status: "unknown",
            error:
              "Publication was interrupted after submission intent; inspect the destination before further action",
          });
        if (operation.status === "waiting-approval")
          yield* sendApproval(owner, { _tag: "Enqueue", entry: approval(operation, owner) });
        if (operation.authorization || operation.status === "rejected")
          yield* sendApproval(owner, {
            _tag: "Acknowledge",
            id: writebackApprovalId(operation.request),
            target: owner.path,
          });
        yield* dispatch(operation, owner);
      });
      return PublicationsActor.of({
        started: (owner) =>
          Effect.gen(function* () {
            if (!registry.get("/publications"))
              yield* registry
                .commit(
                  {
                    path: "/publications",
                    description: "External publications",
                    state: { operations: [] },
                    messages: [],
                  },
                  { expectedRevision: 0 },
                )
                .pipe(Effect.orDie);
            for (let operation of publications(registry)) {
              // Pi may have committed a submission intent or result before the Context handoff.
              const entries = yield* messages
                .read(`/publications/${operation.request.requestId}`)
                .pipe(Effect.orDie);
              const latest = entries.findLast((entry) => entry.kind === "publication.status");
              if (latest) {
                const retained = Schema.decodeUnknownSync(WritebackOperation)(latest.data);
                if (!isDeepStrictEqual(operation, retained)) {
                  yield* commit(retained);
                  operation = retained;
                }
              }
              yield* recover(operation, owner);
            }
          }),
        receive: (command, owner) =>
          Match.value(command).pipe(
            Match.tag("Publish", ({ request, replyTo }) =>
              Effect.gen(function* () {
                const existing = publications(registry).find(
                  (item) => item.request.requestId === request.requestId,
                );
                // A Task has one publication. Later rounds cannot replace its reviewed content.
                if (existing) return yield* replyTo.tell(undefined);
                const operation: WritebackOperation = { status: "waiting-approval", request };
                yield* save(operation);
                yield* replyTo.tell(undefined);
                yield* recover(operation, owner);
              }),
            ),
            Match.tag("ApprovalResolved", ({ requestId, response }) =>
              Effect.gen(function* () {
                let operation = publications(registry).find(
                  (item) => writebackApprovalId(item.request) === requestId,
                );
                if (!operation) return;
                const entry = approvalEntries(registry).find((item) => item.id === requestId);
                if (
                  !entry ||
                  entry.target !== owner.path ||
                  !["resolved", "acknowledged"].includes(entry.status) ||
                  !isDeepStrictEqual(entry.request, approval(operation, owner).request) ||
                  !isDeepStrictEqual(entry.response, response)
                )
                  return;
                if (operation.status === "waiting-approval") {
                  operation =
                    response.decision === "approve"
                      ? {
                          ...operation,
                          status: "authorized",
                          authorization: {
                            approvalId: requestId,
                            approvalsRevision: registry.get("/approvals")!.revision!,
                            approvedAt: yield* at,
                          },
                        }
                      : {
                          ...operation,
                          status: "rejected",
                          error: "Publication rejected by the user",
                        };
                  yield* save(operation);
                }
                yield* sendApproval(owner, {
                  _tag: "Acknowledge",
                  id: requestId,
                  target: owner.path,
                });
                yield* dispatch(operation, owner);
              }),
            ),
            Match.tag("Published", ({ generation: token, requestId, result }) =>
              Effect.gen(function* () {
                const operation = publications(registry).find(
                  (item) => item.request.requestId === requestId,
                );
                if (token !== generation || operation?.status !== "sending") return;
                yield* save(
                  result._tag === "Success"
                    ? { ...operation, status: "published", externalId: result.value.externalId }
                    : { ...operation, status: result.error.outcome, error: result.error.message },
                );
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}
