import { isJsonValue, type JsonValue } from "@earendil-works/chord";

import { defineTool, type ToolRegistration } from "@earendil-works/pi-durable";
import type { AgentTool, AgentToolResult } from "@earendil-works/pi-agent-core";

const jsonDetails = (value: unknown): JsonValue | undefined => {
  if (value === undefined) return undefined;
  return isJsonValue(value) ? value : undefined;
};

const toolResult = (
  result: AgentToolResult,
  api: { output: (chunk: string | Uint8Array) => void },
) => {
  for (const item of result.content) if (item.type === "text") api.output(item.text);
  const details = jsonDetails(result.details);
  return {
    content: result.content,
    ...(details === undefined ? {} : { details }),
    ...(result.isError === undefined ? {} : { isError: result.isError }),
    ...(result.usage === undefined ? {} : { usage: result.usage }),
    ...(result.terminate ? { control: { terminate: true as const } } : {}),
  };
};

export const durableTool = (tool: AgentTool): ToolRegistration =>
  defineTool({
    name: tool.name,
    description: tool.description,
    parameters: tool.parameters,
    replay: tool.replay === "safe" ? "safe" : "unsafe",
    prepareArguments: tool.prepareArguments,
    execute: async (args, api, context) => {
      const result = await tool.execute(api.callId, args, context.abortSignal, (partial) => {
        for (const item of partial.content) if (item.type === "text") api.output(item.text);
      });
      return toolResult(result, api);
    },
  });
