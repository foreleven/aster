import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { Config, Data, Effect, Schema } from "effect";
import { ContextRoute } from "./routed-durable.js";
import { ConfigLocation } from "@aster/core";
import { makeFileContextStore } from "./file-context-store.js";

export class StorageRoutingError extends Data.TaggedError("StorageRoutingError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export const StorageAuthority = Schema.Struct({
  version: Schema.Literal(1),
  localDirectory: Schema.NonEmptyString,
  pi: Schema.optional(
    Schema.Struct({ directory: Schema.NonEmptyString, ownerId: Schema.NonEmptyString }),
  ),
  routes: Schema.Array(ContextRoute),
});
export type StorageAuthority = typeof StorageAuthority.Type;

const Settings = Schema.Struct({
  root: Schema.optional(Schema.NonEmptyString),
  pi: Schema.optional(
    Schema.Struct({
      directory: Schema.optional(Schema.NonEmptyString),
      ownerId: Schema.optional(Schema.NonEmptyString),
    }),
  ),
  routes: Schema.optional(Schema.Array(ContextRoute)),
});

/** Keep the process lease and every local journal under the same root. */
export const validateStorageAuthority = Effect.fn("StorageAuthority.validate")(function* (
  root: string,
  input: StorageAuthority,
) {
  const authority = yield* Schema.decodeUnknownEffect(StorageAuthority)(input).pipe(
    Effect.mapError(
      (cause) => new StorageRoutingError({ message: "Invalid storage authority", cause }),
    ),
  );
  if (authority.localDirectory !== join(resolve(root), "actors"))
    return yield* new StorageRoutingError({
      message: "Local Context storage must be the locked root's actors directory",
    });
  if (new Set(authority.routes.map((route) => route.prefix)).size !== authority.routes.length)
    return yield* new StorageRoutingError({ message: "Duplicate Context route prefix" });
  if (!authority.pi && authority.routes.some((route) => route.backend === "pi"))
    return yield* new StorageRoutingError({ message: "Pi routes require config.durable.pi" });
  if (authority.pi) {
    const directory = authority.pi.directory;
    if (resolve(directory) !== directory)
      return yield* new StorageRoutingError({
        message: "Pi storage directory must be absolute and normalized",
      });
    const reserved = ["actors", "storage-authority", "goals", "evaluations"].map((name) =>
      join(resolve(root), name),
    );
    if (
      reserved.some(
        (path) =>
          directory === path ||
          directory.startsWith(`${path}/`) ||
          path.startsWith(`${directory}/`),
      )
    )
      return yield* new StorageRoutingError({
        message:
          "Pi storage must not overlap Local Context, journal or routing authority directories",
      });
  }
  return authority;
});

/** Module-owned settings read from the host's captured ConfigProvider. */
export const storageSettings = Effect.gen(function* () {
  const { baseDir } = yield* ConfigLocation;
  const settings = yield* Config.schema(Schema.optional(Settings), ["config", "durable"]);
  const root = settings?.root ? resolve(baseDir, settings.root) : join(homedir(), ".aster");
  const routes = [...(settings?.routes ?? [])].sort((a, b) => a.prefix.localeCompare(b.prefix));
  const authority: StorageAuthority = {
    version: 1,
    localDirectory: join(root, "actors"),
    ...(settings?.pi
      ? {
          pi: {
            directory: settings.pi.directory
              ? resolve(baseDir, settings.pi.directory)
              : join(root, "pi"),
            ownerId: settings.pi.ownerId ?? "aster-executions",
          },
        }
      : {}),
    routes,
  };
  yield* validateStorageAuthority(root, authority);
  return { root, authority };
});

/** Reuse the fsync/pending-file transaction protocol in a private namespace.
 * This authority record is never a public Context or an Agent-readable file. */
export const routingAuthorityStore = Effect.fn("StorageAuthority.make")(function* (root: string) {
  const store = yield* Effect.try({
    try: () => makeFileContextStore(join(root, "storage-authority")),
    catch: (cause) => new StorageRoutingError({ message: "Cannot open storage authority", cause }),
  });
  const read = Effect.try({
    try: () => {
      const records = store.loadAll();
      if (records.length === 0) return undefined;
      if (records.length !== 1 || records[0].snapshot.path !== "/routing")
        throw new Error("Invalid storage authority records");
      return {
        messages: records[0].snapshot.messages,
        revision: records[0].snapshot.revision ?? 0,
        value: Schema.decodeUnknownSync(StorageAuthority)(records[0].snapshot.state),
      };
    },
    catch: (cause) => new StorageRoutingError({ message: "Cannot read storage authority", cause }),
  });
  const publish = (value: StorageAuthority, expectedRevision: number) =>
    Effect.gen(function* () {
      const previous = yield* read;
      if ((previous?.revision ?? 0) !== expectedRevision)
        return yield* new StorageRoutingError({
          message: "Storage authority changed during migration",
        });
      yield* Effect.try({
        try: () =>
          store.save({
            events: [],
            snapshot: {
              path: "/routing",
              description: "Authoritative Context backend routes",
              revision: expectedRevision + 1,
              state: Schema.decodeUnknownSync(StorageAuthority)(value),
              messages: [
                ...(previous?.messages ?? []),
                { generation: expectedRevision + 1, from: previous?.value ?? null, to: value },
              ],
            },
          }),
        catch: (cause) =>
          new StorageRoutingError({
            message: "Storage authority publication outcome is unknown; inspect before retry",
            cause,
          }),
      });
    });
  const verify = (requested: StorageAuthority) =>
    read.pipe(
      Effect.flatMap((current) =>
        current && !isDeepStrictEqual(current.value, requested)
          ? Effect.fail(
              new StorageRoutingError({
                message:
                  "Context routing differs from persisted authority. Stop Aster and run storage migrate before starting with this configuration.",
              }),
            )
          : Effect.succeed(current),
      ),
    );
  return { read, publish, verify };
});
