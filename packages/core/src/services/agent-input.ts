import type { AssistantMessage } from "@aster/agent";

/** Durable callers read the native answer, never rebuild a run transcript. */
export const answerText = (answer: AssistantMessage | undefined) =>
  answer && !answer.content.some((part) => part.type === "toolCall")
    ? answer.content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("\n\n")
    : "";
