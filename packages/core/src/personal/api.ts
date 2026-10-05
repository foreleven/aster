import type { ActorRef, ReplyTo } from "@aster/actor";
import {
  ApplicationError,
  type PersonalInput,
  type PersonalStartTaskInput,
  type PersonalResumeRunInput,
  type PersonalRetryInput,
  type PersonalGoalMessageInput,
  type PersonalSignalCommandInput,
  type PersonalApprovalResponseInput,
  type PersonalApprovalRequestInput,
} from "@aster/api-contracts";
import { Effect, Schema } from "effect";
import type { PersonalCommand, PersonalReply } from "./actor.js";
import { publicJson } from "../context/json.js";
import { PublicContext as ContextRecord } from "@aster/api-contracts";

export const makePersonalApi = (actor?: ActorRef<PersonalCommand>) => {
  const ask = Effect.fn("PersonalApi.ask")(function* (
    command: (replyTo: ReplyTo<PersonalReply>) => PersonalCommand,
  ) {
    if (!actor)
      return yield* new ApplicationError({
        kind: "unavailable",
        message: "Personal Agent unavailable",
      });
    const reply = yield* actor.ask(command).pipe(
      Effect.catchTag("AskTimeoutError", () =>
        Effect.fail(
          new ApplicationError({
            kind: "unavailable",
            message:
              "Personal Agent acknowledgement timed out; retry the same request ID to reconcile acceptance",
          }),
        ),
      ),
    );
    if (reply._tag === "Rejected") return yield* reply.error;
    return reply;
  });
  const wireContext = (record: ContextRecord) =>
    Schema.decodeUnknownSync(ContextRecord)(publicJson(record));
  const snapshot = (command: (replyTo: ReplyTo<PersonalReply>) => PersonalCommand) =>
    ask(command).pipe(
      Effect.flatMap((reply) =>
        reply._tag === "Snapshot"
          ? Effect.succeed(wireContext(reply.record))
          : Effect.die(new Error("Invalid Personal snapshot response")),
      ),
    );
  const applySignal = (input: PersonalSignalCommandInput) =>
    ask((replyTo) => ({ _tag: "ApplySignal", input, replyTo })).pipe(
      Effect.flatMap((reply) =>
        reply._tag === "Queued"
          ? Effect.succeed(reply.receipt)
          : Effect.die(new Error("Invalid Signal command receipt")),
      ),
    );
  return {
    resumeRun: (input: PersonalResumeRunInput) =>
      ask((replyTo) => ({ _tag: "ResumeRun", input, replyTo })).pipe(
        Effect.flatMap((reply) =>
          reply._tag === "Queued"
            ? Effect.succeed(reply.receipt)
            : Effect.die(new Error("Invalid Run resumption receipt")),
        ),
      ),
    startTask: (input: PersonalStartTaskInput) =>
      ask((replyTo) => ({ _tag: "StartTask", input, replyTo })).pipe(
        Effect.flatMap((reply) =>
          reply._tag === "Queued"
            ? Effect.succeed(reply.receipt)
            : Effect.die(new Error("Invalid Task admission receipt")),
        ),
      ),
    inspectDelegation: (path: string) =>
      ask((replyTo) => ({ _tag: "InspectDelegation", path, replyTo })).pipe(
        Effect.flatMap((reply) =>
          reply._tag === "Delegation"
            ? Effect.succeed(reply.inspection)
            : Effect.die(new Error("Invalid Delegation inspection")),
        ),
      ),
    requestApproval: (input: PersonalApprovalRequestInput) =>
      ask((replyTo) => ({ _tag: "RequestApproval", input, replyTo })).pipe(
        Effect.flatMap((reply) =>
          reply._tag === "Queued"
            ? Effect.succeed(reply.receipt)
            : Effect.die(new Error("Invalid approval request receipt")),
        ),
      ),
    respondApproval: (input: PersonalApprovalResponseInput) =>
      ask((replyTo) => ({ _tag: "RespondApproval", input, replyTo })).pipe(
        Effect.flatMap((reply) =>
          reply._tag === "Queued"
            ? Effect.succeed(reply.receipt)
            : Effect.die(new Error("Invalid approval command receipt")),
        ),
      ),
    applySignal,
    createSignal: (input: Omit<PersonalSignalCommandInput, "operation">) =>
      applySignal({ ...input, operation: "createSignal" }),
    updateSignal: (input: Omit<PersonalSignalCommandInput, "operation">) =>
      applySignal({ ...input, operation: "updateSignal" }),
    sendGoalMessage: (input: PersonalGoalMessageInput) =>
      ask((replyTo) => ({ _tag: "SendGoalMessage", input, replyTo })).pipe(
        Effect.flatMap((reply) =>
          reply._tag === "Queued"
            ? Effect.succeed(reply.receipt)
            : Effect.die(new Error("Invalid Personal outbox receipt")),
        ),
      ),
    get: snapshot((replyTo) => ({ _tag: "Get", replyTo })),
    readContext: (path: string) => snapshot((replyTo) => ({ _tag: "ReadContext", path, replyTo })),
    listContexts: ask((replyTo) => ({ _tag: "ListContexts", replyTo })).pipe(
      Effect.flatMap((reply) =>
        reply._tag === "Contexts"
          ? Effect.succeed(reply.records.map(wireContext))
          : Effect.die(new Error("Invalid Personal Context list response")),
      ),
    ),
    sendMessage: (input: PersonalInput) =>
      ask((replyTo) => ({ _tag: "Accept", input, replyTo })).pipe(
        Effect.flatMap((reply) =>
          reply._tag === "Accepted"
            ? Effect.succeed(reply.receipt)
            : Effect.die(new Error("Invalid Personal acceptance response")),
        ),
      ),
    retry: (input: PersonalRetryInput) =>
      ask((replyTo) => ({ _tag: "Retry", input, replyTo })).pipe(
        Effect.flatMap((reply) =>
          reply._tag === "Accepted"
            ? Effect.succeed(reply.receipt)
            : Effect.die(new Error("Invalid Personal retry response")),
        ),
      ),
  };
};
