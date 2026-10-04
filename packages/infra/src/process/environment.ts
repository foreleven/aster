import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { parseEnv } from "node:util";

export const agentEnvironment = (
  source: NodeJS.ProcessEnv = process.env,
  envPath = resolve(".env"),
): NodeJS.ProcessEnv => {
  const env = { ...source };
  const projectVariables = existsSync(envPath)
    ? Object.keys(parseEnv(readFileSync(envPath, "utf8")))
    : [];
  for (const key of [
    ...projectVariables,
    ...Object.keys(source).filter((name) => name.startsWith("ASTER_")),
    "TYPESAFE_API_KEY",
    "LAYA_API_KEY",
    "AGENTMEMORY_SECRET",
  ])
    delete env[key];
  return env;
};
