import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { Config, Effect, Schema } from "effect";
import { ConfigLocation } from "@aster/core";

/** Context files and screening journals share the host's locked storage root. */
export const storageSettings = Effect.gen(function* () {
  const { baseDir } = yield* ConfigLocation;
  const configuredRoot = yield* Config.schema(Schema.optional(Schema.NonEmptyString), [
    "config",
    "durable",
    "root",
  ]);
  const root = configuredRoot ? resolve(baseDir, configuredRoot) : join(homedir(), ".aster");
  return { root, contextDirectory: join(root, "actors") };
});
