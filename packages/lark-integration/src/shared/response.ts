export const object = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

export const string = (value: unknown): string => (typeof value === "string" ? value : "");

export const parseCliOutput = (stdout: string): Record<string, unknown> => {
  const envelope = object(JSON.parse(stdout));
  if (envelope.ok === false)
    throw new Error(string(object(envelope.error).message) || "lark-cli request failed");
  return object(envelope.data ?? envelope);
};
