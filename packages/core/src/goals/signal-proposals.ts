import { createHash } from "node:crypto";
import { Effect, Match, Schema } from "effect";
import { SignalDefinition, validateSignalTime } from "../config/schema.js";
import type { ContextRegistry } from "../context/registry.js";
import { GoalSignalInput, type GoalSignalOperation } from "../signals/goal-command.js";
import { goalOutputCause, type GoalState } from "./state.js";
import { GoalSignalChange, GoalToolError, type GoalTask } from "./tasks.js";

/** Freeze receiver commands without writes; the caller commits these with the entire result. */
export const planSignalChanges = Effect.fn("Goal.planSignalChanges")(function* (options: {
  readonly state: GoalState;
  readonly tasks: readonly GoalTask[];
  readonly changes: readonly GoalSignalChange[];
  readonly registry: ContextRegistry["Service"];
  readonly agents: readonly string[];
  readonly evaluationId: string;
  readonly at: string;
  readonly completed: boolean;
}) {
  const changes = yield* Schema.decodeUnknownEffect(
    Schema.Array(GoalSignalChange).check(Schema.isMaxLength(16)),
  )(options.changes).pipe(
    Effect.mapError(() => new GoalToolError({ message: "Invalid Signal proposals" })),
  );
  const outbox: GoalSignalOperation[] = [];
  const targets = new Set<string>();
  for (const [index, change] of changes.entries()) {
    const fail = (message: string) => new GoalToolError({ message });
    if (!/^[a-z0-9][a-z0-9-]*$/.test(change.id)) return yield* fail("Invalid Signal ID");
    const goal = options.state.slug;
    const slug = change.id.startsWith(`${goal}--`) ? change.id : `${goal}--${change.id}`;
    const target = `/signals/${slug}`;
    if (targets.has(target)) return yield* fail("Consolidate changes into one proposal per Signal");
    targets.add(target);
    if (
      options.state.signalOutbox?.some(
        (operation) =>
          operation.input.target === target &&
          ["pending", "sending", "unknown"].includes(operation.status),
      )
    )
      return yield* fail("Reconcile the pending Signal operation before proposing another change");
    const current = options.registry.get(target);
    const state = current?.state as
      { goal?: string; owner?: string; revision?: number; deleted?: boolean } | undefined;
    const creating = change.operation === "signal_create";
    const deleting = change.operation === "signal_delete";
    if (options.completed && !deleting)
      return yield* fail("A completed Goal can only delete Signals");
    if (current && (state?.goal !== goal || state.owner !== undefined))
      return yield* fail("Signal belongs to another owner");
    if (creating && current) return yield* fail("Signal already exists");
    if (!creating && (!current || state?.deleted || change.revision !== state?.revision))
      return yield* fail("Signal missing, deleted or revision changed");
    const previous = current && Schema.decodeUnknownSync(SignalDefinition)(current.state);
    const patch = deleting ? {} : change.definition;
    const raw: Record<string, unknown> = {
      ...current?.state,
      ...patch,
      slug,
      when: patch.when ?? previous?.when ?? "Check the latest Goal progress on schedule",
      task:
        patch.task ?? previous?.task ?? "Evaluate changes, record conclusions, or advance tasks",
      agent: previous?.agent ?? "doubao-delegate",
      mode: "confirm",
    };
    for (const key of ["taskId", "schedule", "notBefore"]) if (raw[key] === null) delete raw[key];
    const definition = yield* Schema.decodeUnknownEffect(SignalDefinition)(raw).pipe(
      Effect.mapError(() => fail("Invalid Signal definition")),
    );
    yield* Effect.try({
      try: () => validateSignalTime(definition),
      catch: () => fail("Invalid Signal timing"),
    });
    if (!definition.when.trim() || !definition.task.trim())
      return yield* fail("Signal condition and task must not be empty");
    if (!deleting && !options.agents.includes(definition.agent))
      return yield* fail("Signal executor is not configured");
    if (
      !deleting &&
      definition.taskId &&
      !options.tasks.some((task) => task.id === definition.taskId && task.status !== "deleted")
    )
      return yield* fail("Signal references a missing or deleted Goal Task");
    const input = yield* Schema.decodeUnknownEffect(GoalSignalInput)({
      requestId: createHash("sha256")
        .update(JSON.stringify(["goal.signal", options.evaluationId, index]))
        .digest("hex"),
      evaluationId: options.evaluationId,
      source: `/goals/${goal}`,
      target,
      expectedRevision: current?.revision ?? 0,
      operation: Match.value(change).pipe(
        Match.when({ operation: "signal_create" }, () => "create" as const),
        Match.when({ operation: "signal_update" }, () => "update" as const),
        Match.when({ operation: "signal_delete" }, () => "delete" as const),
        Match.exhaustive,
      ),
      definition,
      causal: goalOutputCause(options.state) ?? {
        rootRequestId: options.evaluationId,
        remainingAgentTurns: 0,
      },
      createdAt: options.at,
    }).pipe(Effect.mapError(() => fail("Invalid frozen Signal command")));
    outbox.push({ input, status: "pending", attempts: 0 });
  }
  return outbox;
});
