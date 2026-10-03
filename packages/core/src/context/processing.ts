import type {
  ContextDescriptionError,
  ContextConflict,
  ContextCommitError,
  ContextValidationError,
} from "./errors.js";
import type { DescriptionInitializer } from "./description.js";
import { type ContextChange, type ContextRecord, type ContextCapture } from "./model.js";
import { ContextRegistry } from "./registry.js";
import { Cause, Effect } from "effect";

/** Common follow-up policy; integrations only update their own public Context. */
export const makeContextProcessor = <E>(
  registry: ContextRegistry["Service"],
  captureMemory: (input: ContextCapture) => Effect.Effect<void>,
  evaluate: (
    record: ContextRecord,
    snapshot: Readonly<Record<string, ContextRecord>>,
  ) => Effect.Effect<void, E>,
  describe: DescriptionInitializer,
) => {
  const captures = new Set<string>();
  return (
    change: ContextChange,
  ): Effect.Effect<
    void,
    ContextDescriptionError | ContextConflict | ContextCommitError | ContextValidationError | E
  > =>
    Effect.gen(function* () {
      let record = change.record;
      const definition = registry.definition(record.path);
      if (!definition) return;
      if (!record.description) {
        // Initialize identity without losing the original state-change event.
        const existingDescription = registry.get(record.path)?.description;
        if (existingDescription) record = { ...record, description: existingDescription };
        else {
          let ancestor = record.path.slice(0, record.path.lastIndexOf("/"));
          while (ancestor && !registry.get(ancestor))
            ancestor = ancestor.slice(0, ancestor.lastIndexOf("/"));
          const description = yield* describe({
            path: record.path,
            identity: definition.identity,
            parentDescription: registry.get(ancestor)?.description ?? "",
          });
          const latest = registry.get(record.path);
          if (latest) yield* registry.describe(record.path, description, latest.revision ?? 0);
          record = { ...record, description };
        }
      }
      const capture = definition.capture?.(record);
      if (capture && !captures.has(capture.sessionId)) {
        yield* captureMemory({ ...capture, records: capture.records.map(registry.project) });
        // Deduplicate only accepted captures. A failed handoff must remain eligible
        // when a later Context change offers the same session again.
        captures.add(capture.sessionId);
      }
      if (definition.signalSource && change.stateChanged && change.evaluate !== false) {
        // A queued notification must evaluate its own source snapshot, even if newer data exists.
        const source = registry.project(record);
        yield* evaluate(source, { ...registry.publicSnapshot(), [record.path]: source });
      }
    });
};

/** A failed item must not complete the long-lived subscription. Cancellation still propagates. */
export const isolateContextChange =
  <E, R>(handle: (change: ContextChange) => Effect.Effect<void, E, R>) =>
  (change: ContextChange) =>
    handle(change).pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterrupts(cause)
          ? Effect.failCause(cause)
          : Effect.logError(
              JSON.stringify({
                event: "context.processing.failed",
                path: change.path,
                error: String(Cause.squash(cause)),
              }),
            ),
      ),
    );
