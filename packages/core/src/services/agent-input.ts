import type { AgentMessage, AssistantMessage } from "@aster/agent";

/** Goal and Task evidence uses one stable envelope across durable admission and replay. */
export const conversationInput = (messages: readonly AgentMessage[]) => {
  const system = messages.find((message) => message.role === "system");
  let instructions = "";
  if (system)
    instructions =
      typeof system.content === "string" ? system.content : JSON.stringify(system.content);
  return {
    instructions,
    input: JSON.stringify({
      evidence: messages.filter((message) => message.role !== "system"),
      instruction:
        "Process this newly committed input according to your instructions. Use the available tools and submit any required structured result.",
    }),
  };
};

/** Durable callers read the native answer, never rebuild a run transcript. */
export const answerText = (answer: AssistantMessage | undefined) =>
  answer && !answer.content.some((part) => part.type === "toolCall")
    ? answer.content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("\n\n")
    : "";
