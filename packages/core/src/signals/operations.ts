import { Effect, Match, Schema } from "effect";
import { GoalPath } from "../goals/contracts.js";
import { ApplicationError, type CommandReceipt } from "../operations.js";
import { askOwner, readOwner } from "../runtime/addressing.js";
import { prepareInternalTask, prepareExternalTask } from "../tasks/contracts.js";
import {
  type CreateSignalInput,
  type SignalActionInput,
  type SignalTriggerInput,
  type SignalAction,
  type SignalTrigger,
} from "./contracts.js";
import { SignalChangeInput, type SignalCommandReply, type SignalRootCommand } from "./protocol.js";
import { type SignalSnapshot } from "./state/snapshot.js";
import { SignalRecord } from "./queries.js";
import type { AgentConversations } from "@aster/agent/harness";
import type { DurableContext } from "../context/store.js";
import { readSignalHistory } from "./state/store.js";

/** Signal command receipts retain source-qualified identities in the existing owner journal. */
export const signalOperationReceipt: (
  registry: DurableContext["Service"],
  messages: AgentConversations["Service"],
  owner: string,
  operationId: string,
) => Effect.Effect<CommandReceipt | undefined> = Effect.fn("Signals.operationReceipt")(function* (
  registry: DurableContext["Service"],
  messages: AgentConversations["Service"],
  owner: string,
  operationId: string,
) {
  const prefix = `/signals/${owner.slice("/goals/".length)}--`;
  for (const entry of registry.directory()) {
    if (!entry.path.startsWith(prefix)) continue;
    const history = yield* readSignalHistory(messages, entry.path);
    const received = history.receipts.find(
      (item) =>
        item._tag === "Command" &&
        item.input.source === owner &&
        item.input.requestId === operationId,
    );
    if (received) return received.receipt;
  }
  return undefined;
});

/** Model descriptions become concrete actions once, before durable rule admission. */
const action = (input: SignalActionInput, owner: string): SignalAction =>
  Match.value(input).pipe(
    Match.when({ type: "start_task" }, (work) => ({
      _tag: "StartTask" as const,
      task: { _tag: "Agent" as const, task: prepareInternalTask(work), replyTo: owner },
    })),
    Match.when({ type: "delegate_task" }, (work) => ({
      _tag: "StartTask" as const,
      task: {
        _tag: "Delegate" as const,
        agent: work.executor,
        task: prepareExternalTask(work),
        replyTo: owner,
      },
    })),
    Match.when({ type: "send_goal_message" }, ({ goalPath, text }) => ({
      _tag: "SendGoalMessage" as const,
      goalPath,
      text,
    })),
    Match.exhaustive,
  );
const trigger = (input: SignalTriggerInput): SignalTrigger =>
  Match.value(input).pipe(
    Match.when({ type: "context" }, ({ when }) => ({ _tag: "Context" as const, when })),
    Match.when({ type: "once" }, ({ at }) => ({
      _tag: "Schedule" as const,
      schedule: { type: "once" as const, at },
    })),
    Match.when({ type: "cron" }, ({ expression, timeZone }) => ({
      _tag: "Schedule" as const,
      schedule: { type: "cron" as const, expression, timeZone },
    })),
    Match.exhaustive,
  );
export const signalDefinition = (input: CreateSignalInput, owner: string) => ({
  trigger: trigger(input.trigger),
  action: action(input.action, owner),
});
export const ownedSignalPath = (owner: string, id: string) =>
  `/signals/${owner.slice("/goals/".length)}--${id}`;

export const changeSignal = Effect.fn("Signals.change")(function* (input: SignalChangeInput) {
  const decoded = yield* Schema.decodeUnknownEffect(SignalChangeInput)(input).pipe(
    Effect.mapError(
      () => new ApplicationError({ kind: "invalid-input", message: "Invalid Signal command" }),
    ),
  );
  const reply = yield* askOwner<SignalRootCommand, SignalCommandReply>(
    "/user/signals",
    (replyTo) => ({ _tag: "Change", input: decoded, replyTo }),
    decoded.requestId,
  );
  if (reply._tag === "Rejected") return yield* reply.error;
  return reply.receipt;
});
const presentAction = (input: SignalAction): SignalActionInput =>
  Match.value(input).pipe(
    Match.tag("SendGoalMessage", ({ goalPath, text }) => ({
      type: "send_goal_message" as const,
      goalPath,
      text,
    })),
    Match.tag("StartTask", ({ task }) =>
      Match.value(task).pipe(
        Match.tag("Agent", ({ task: work }) => ({
          type: "start_task" as const,
          title: work.title,
          instructions: work.instructions,
          contextPaths: work.contextPaths,
          evidence: work.input,
        })),
        Match.tag("Delegate", ({ agent, task: work }) => ({
          type: "delegate_task" as const,
          title: work.title,
          executor: agent,
          instructions: work.instructions,
          evidence: work.input,
        })),
        Match.exhaustive,
      ),
    ),
    Match.exhaustive,
  );
const presentTrigger = (input: SignalTrigger): SignalTriggerInput =>
  Match.value(input).pipe(
    Match.tag("Context", ({ when }) => ({ type: "context" as const, when })),
    Match.tag("Schedule", ({ schedule }) => schedule),
    Match.exhaustive,
  );
export const presentSignal = (path: string, state: SignalSnapshot, owner: string) => ({
  id: path.slice(ownedSignalPath(owner, "").length),
  path,
  status: state.status,
  version: state.version,
  trigger: presentTrigger(state.trigger),
  action: presentAction(state.action),
  ...(state.nextDue === undefined ? {} : { nextDue: state.nextDue }),
});
export const listOwnedSignals = Effect.fn("Signals.listOwned")(function* (owner: string) {
  const source = yield* Schema.decodeUnknownEffect(GoalPath)(owner).pipe(
    Effect.mapError(
      () => new ApplicationError({ kind: "invalid-input", message: "Signal owner must be a Goal" }),
    ),
  );
  const reply = yield* readOwner<SignalRootCommand, import("../services/actors.js").QueryReply>(
    "/user/signals",
    (replyTo) => ({ _tag: "ListByOwner", owner: source, replyTo }),
  );
  if (reply._tag === "Failure") return yield* reply.error;
  const records = yield* Schema.decodeUnknownEffect(Schema.Array(SignalRecord))(reply.value).pipe(
    Effect.orDie,
  );
  return records.map((record) => presentSignal(record.path, record.state, owner));
});
