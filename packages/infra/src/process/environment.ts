import type { ProcessEnvironment } from "@aster/core";

/** Filter the host's captured sources without rereading files or live process state. */
export const agentEnvironment = (source: ProcessEnvironment["Service"]): NodeJS.ProcessEnv => {
  const privateKeys = new Set([
    ...source.privateKeys,
    "TYPESAFE_API_KEY",
    "LAYA_API_KEY",
    "AGENTMEMORY_SECRET",
  ]);
  return Object.fromEntries(
    Object.entries(source.values).filter(
      ([key]) => !key.startsWith("ASTER_") && !privateKeys.has(key),
    ),
  );
};
