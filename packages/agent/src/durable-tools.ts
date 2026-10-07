import { Schema } from "effect";
import type {
  ConversationId,
  Cursor,
  EntryRecord,
  ToolRegistration,
  Tx,
  Harness,
  GenerationHooks,
  CompactionHooks,
} from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";

const Diagnostics = Schema.Struct({
  diagnostics: Schema.Array(Schema.Struct({ code: Schema.optional(Schema.String) })),
});
const Rejected = Schema.Struct({ aster: Schema.Struct({ outcome: Schema.Literal("rejected") }) });

/** Pi reports ordinary hook errors and continues. Carry the validation promise
 * across the native callback boundary so the provider must observe its outcome.
 * The invocation signal identifies this attempt; retained entries remain the
 * authority, and reopening rebuilds the guard from those entries. */
export const generationFence = (owner: () => Harness, unsafe: ReadonlySet<string>) => {
  const requests = new WeakMap<AbortSignal, Promise<unknown>>();
  const beforeRequest: GenerationHooks["beforeRequest"] = async (_request, api, context) => {
    const checked = (async () => {
      const unknown = await owner().commit(
        async (tx) => hasUnknownToolOutcome(await entriesFor(tx, api.conversationId), unsafe),
        context,
      );
      if (unknown)
        throw new Error("Tool outcome is unknown; reconcile before another model request.");
      return undefined;
    })();
    if (context.abortSignal) requests.set(context.abortSignal, checked);
    return checked;
  };
  const beforeCompact: CompactionHooks["beforeCompact"] = async (_request, api, context) => {
    const unknown = await owner().commit(
      async (tx) => hasUnknownToolOutcome(await entriesFor(tx, api.conversationId), unsafe),
      context,
    );
    return unknown ? { decline: true } : undefined;
  };
  return {
    beforeRequest,
    beforeCompact,
    beforeModel: async (signal?: AbortSignal) => {
      const checked = signal && requests.get(signal);
      if (checked) {
        await checked;
        return;
      }
      // Pi's recovered compaction summarize/retry phases skip beforeCompact and
      // do not expose a conversation ID to the provider. Fail closed across this
      // owner for such unattributed requests; never guess from prompt content.
      await owner().commit(
        async (tx) => {
          let cursor: Cursor | undefined;
          do {
            const page = await tx.scanConversations({}, 100, cursor);
            for (const conversation of page.items) {
              if (hasUnknownToolOutcome(await entriesFor(tx, conversation.id), unsafe))
                throw new Error(
                  "Tool outcome is unknown; reconcile before compaction or another unattributed model request.",
                );
            }
            cursor = page.next;
          } while (cursor);
        },
        signal ? withAbortSignal(signal, BACKGROUND_CONTEXT) : BACKGROUND_CONTEXT,
      );
    },
  };
};

/** Native Pi transaction boundary; reads retained evidence without resuming work. */
export const entriesFor = async (tx: Tx, conversationId: ConversationId) => {
  const entries: EntryRecord[] = [];
  let cursor: Cursor | undefined;
  do {
    const page = await tx.scanEntries({ conversationId }, 100, cursor);
    entries.push(...page.items);
    cursor = page.next;
  } while (cursor);
  return entries;
};

export const hasUnknownToolOutcome = (
  entries: readonly EntryRecord[],
  unsafe: ReadonlySet<string>,
) =>
  entries.some((entry) => {
    if (entry.kind !== "pi.tool-result") return false;
    const codes = Schema.decodeUnknownSync(Diagnostics)(entry.data).diagnostics.map(
      (item) => item.code,
    );
    if (codes.some((code) => code === "interrupted" || code === "aster.unknown")) return true;
    // Pi emits these before invoking execute; they cannot describe a partial write.
    if (
      codes.some(
        (code) => code === "invalid_arguments" || code === "blocked" || code === "tool_unavailable",
      )
    )
      return false;
    return (
      entry.model?.some(
        (message) =>
          message.role === "toolResult" &&
          message.isError &&
          unsafe.has(message.toolName) &&
          !Schema.is(Rejected)(message.details),
      ) ?? false
    );
  });

/** Only a host tool handler may declare a rejected operation: no side effect was accepted. */
export const rejectedToolResult = (message: string) => ({
  isError: true,
  content: [{ type: "text" as const, text: message }],
  details: { aster: { outcome: "rejected" as const } },
});

const uncertainResult = (message: string) => ({
  isError: true,
  content: [{ type: "text" as const, text: message }],
  diagnostics: [
    {
      severity: "error" as const,
      code: "aster.unknown",
      message: "Tool outcome is unknown; reconcile the retained operation before further work.",
    },
  ],
  control: { terminate: true as const },
});

/** An unsafe call makes its entire tool round sequential. Its unknown outcome
 * commits before another call may enter, including sibling calls in one answer. */
export const fenceTools = (
  tools: readonly ToolRegistration[],
  unsafe: ReadonlySet<string>,
): ToolRegistration[] =>
  tools.map((tool) => ({
    ...tool,
    ...(unsafe.has(tool.name) ? { executionMode: "sequential" as const } : {}),
    execute: async (args, api, context) => {
      const unknown = await api.commit(
        async (tx) => hasUnknownToolOutcome(await entriesFor(tx, api.conversationId), unsafe),
        context,
      );
      if (unknown) return uncertainResult("An earlier tool has an uncertain outcome.");
      try {
        const result = await tool.execute(args, api, context);
        if (unsafe.has(tool.name) && result.isError && !Schema.is(Rejected)(result.details))
          return {
            ...result,
            diagnostics: [...(result.diagnostics ?? []), ...uncertainResult("").diagnostics],
            control: { terminate: true as const },
          };
        return result;
      } catch (error) {
        // Interruption leaves Pi's durable intent for its unsafe recovery policy.
        if (!unsafe.has(tool.name) || context.abortSignal?.aborted) throw error;
        return uncertainResult(error instanceof Error ? error.message : String(error));
      }
    },
  }));
