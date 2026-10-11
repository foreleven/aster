import type { StreamFn } from "@earendil-works/pi-agent-core";
import {
  lazyStream,
  type AssistantMessageEvent,
  type Model,
  type ToolCall,
} from "@earendil-works/pi-ai";
import { Option, Schema } from "effect";

const ToolStart = Schema.Struct({
  type: Schema.Literal("content_block_start"),
  index: Schema.Number,
  content_block: Schema.Struct({
    type: Schema.Literal("tool_use"),
    id: Schema.String,
    input: Schema.optional(Schema.Unknown),
  }),
});
const ToolDelta = Schema.Struct({
  type: Schema.Literal("content_block_delta"),
  index: Schema.Number,
  delta: Schema.Struct({
    type: Schema.Literal("input_json_delta"),
    partial_json: Schema.String,
  }),
});
const Arguments = Schema.Record(Schema.String, Schema.MutableJson);

/** Pi 1.1.0 drops initial tool input and salvages malformed deltas at block end.
 * Keep its transport/formatting, but finalize arguments from the original wire events.
 * This buffer belongs to one provider response, never to a conversation or domain.
 */
export const anthropicStream: StreamFn = (model, context, options) =>
  lazyStream(model, async () => {
    const calls = new Map<number, { id: string; initial: unknown; json?: string }>();
    const observe = (event: unknown) => {
      const start = Schema.decodeUnknownOption(ToolStart)(event);
      if (Option.isSome(start)) {
        calls.set(start.value.index, {
          id: start.value.content_block.id,
          initial: structuredClone(
            start.value.content_block.input === undefined ? {} : start.value.content_block.input,
          ),
        });
        return;
      }
      const delta = Schema.decodeUnknownOption(ToolDelta)(event);
      if (Option.isSome(delta)) {
        const call = calls.get(delta.value.index);
        if (!call) throw new Error("Provider tool delta has no initial block");
        call.json = (call.json ?? "") + delta.value.delta.partial_json;
      }
    };
    const finalize = (toolCall: ToolCall) => {
      const call = [...calls.values()].find((value) => value.id === toolCall.id);
      if (!call) throw new Error("Provider tool call has no initial block");
      // An absent delta preserves complete initial input; an empty delta is invalid JSON.
      try {
        const input: unknown = call.json === undefined ? call.initial : JSON.parse(call.json);
        toolCall.arguments = Schema.decodeUnknownSync(Arguments)(input);
      } catch {
        // The upstream error envelope must never echo private argument content.
        throw new Error("Provider returned malformed tool arguments");
      }
    };
    const provider = await import("@earendil-works/pi-ai/api/anthropic-messages");
    if (model.api !== "anthropic-messages") throw new Error("Expected Anthropic Messages model");
    // The SDK's generic API/compat relationship is selected at this transport boundary.
    const source = provider.streamSimple(model as Model<"anthropic-messages">, context, {
      ...options,
      onProviderStreamEvent: async (event, currentModel) => {
        observe(event);
        await options?.onProviderStreamEvent?.(event, currentModel);
      },
    });
    return (async function* (): AsyncGenerator<AssistantMessageEvent> {
      for await (const event of source) {
        if (event.type === "toolcall_end") finalize(event.toolCall);
        if (event.type === "done")
          for (const block of event.message.content) if (block.type === "toolCall") finalize(block);
        yield event;
      }
    })();
  });
