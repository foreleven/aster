import { Effect, FileSystem, Path, Schema, Redacted } from "effect";
import { MemoryConnection } from "./protocol.js";
import { MemoryConnectionError } from "./errors.js";
import { MemorySettings } from "./settings.js";
import { openProvenance } from "./provenance.js";
import { makeMemoryClient } from "./client.js";

export const openMemoryReader = Effect.fn("Memory.reader")(function* (file: string) {
  const fs = yield* FileSystem.FileSystem;
  const connection = yield* fs.readFileString(file).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(MemoryConnection))),
    Effect.mapError(
      (cause) =>
        new MemoryConnectionError({
          message: "Cannot open managed memory connection; start Aster first",
          cause: Redacted.make(cause),
        }),
    ),
  );
  const provenance = yield* openProvenance(connection.dataDir, true);
  const client = yield* makeMemoryClient(connection, provenance);
  return {
    search: (query: string, options?: { readonly limit?: number }) =>
      client.search(query, options?.limit),
    expand: client.expand,
  };
});
export const configuredMemoryReader = Effect.gen(function* () {
  const settings = yield* MemorySettings;
  const path = yield* Path.Path;
  return yield* openMemoryReader(path.join(settings.dataDir, "connection.json"));
}).pipe(Effect.provide(MemorySettings.layer));
