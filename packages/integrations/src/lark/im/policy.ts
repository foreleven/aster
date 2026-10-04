import { object } from "../shared/response.js";
export const parseImPolicy = (entry: unknown) => {
  const config = object(object(entry).config);
  const summary = object(config.summary);
  const positive = (value: unknown, fallback: number, name: string) => {
    const result = value ?? fallback;
    if (typeof result !== "number" || !Number.isSafeInteger(result) || result <= 0)
      throw new Error(`Lark IM ${name} must be a positive integer`);
    return result;
  };
  return {
    pollIntervalMs: positive(config.pollIntervalMs, 15 * 60_000, "pollIntervalMs"),
    catchUpWindowMs: positive(config.catchUpWindowMs, 60 * 60_000, "catchUpWindowMs"),
    agentStartIntervalMs: positive(
      summary.agentStartIntervalMs,
      10_000,
      "summary.agentStartIntervalMs",
    ),
    agentConcurrency: positive(summary.agentConcurrency, 2, "summary.agentConcurrency"),
  };
};
