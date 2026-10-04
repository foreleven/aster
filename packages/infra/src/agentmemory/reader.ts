import { MemoryConnectionError } from "./errors.js";
import { readFile } from "node:fs/promises";
import { Effect, Schema } from "effect";
import { makeMemoryReader } from "./client.js";

const Connection = Schema.Struct({
  url: Schema.String,
  secret: Schema.String,
  dataDir: Schema.String,
  project: Schema.String,
  cwd: Schema.String,
});

/** Owns connection-file decoding and the reader's SQLite resource. */
export const openMemoryReader = (path: string) =>
  Effect.acquireRelease(
    Effect.tryPromise({
      try: async () =>
        makeMemoryReader(
          Schema.decodeUnknownSync(Connection)(JSON.parse(await readFile(path, "utf8"))),
        ),
      catch: (cause) =>
        (cause as NodeJS.ErrnoException).code === "ENOENT"
          ? new MemoryConnectionError({
              message: "Managed memory is not running; start Aster first",
              cause,
            })
          : new MemoryConnectionError({ message: "Cannot open managed memory connection", cause }),
    }),
    (client) => Effect.sync(client.close),
  );
