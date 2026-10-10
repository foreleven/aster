import { DatabaseSync } from "node:sqlite";
import { Effect, Path, Schema } from "effect";
import { MemoryProtocolError } from "./errors.js";
import { Row } from "./protocol.js";

/** Synchronous SQLite is a native storage boundary; its lifetime belongs to the adapter Scope. */
export const openProvenance = Effect.fn("Memory.provenance")(function* (
  dataDir: string,
  readOnly: boolean = false,
) {
  const path = yield* Path.Path;
  const failure = (cause: unknown) =>
    new MemoryProtocolError({ message: "Cannot access memory provenance", cause });
  const db = yield* Effect.acquireRelease(
    Effect.try({
      try: () => new DatabaseSync(path.join(dataDir, "signals-sources.sqlite"), { readOnly }),
      catch: failure,
    }),
    (db) => Effect.sync(() => db.close()),
  );
  yield* Effect.try({
    try: () => {
      if (!readOnly)
        db.exec(
          "PRAGMA journal_mode = WAL; CREATE TABLE IF NOT EXISTS sources (session_id TEXT NOT NULL, observation_id TEXT NOT NULL, paths TEXT NOT NULL, PRIMARY KEY (session_id, observation_id))",
        );
    },
    catch: failure,
  });
  const Rows = Schema.Array(Schema.Struct({ paths: Schema.String }));
  const lookup = (field: "observation_id" | "session_id", id: string) =>
    Effect.try({
      try: () => db.prepare(`SELECT paths FROM sources WHERE ${field} = ?`).all(id),
      catch: failure,
    }).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Rows)),
      Effect.flatMap((rows) =>
        Effect.forEach(rows, (row) =>
          Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Array(Schema.String)))(row.paths),
        ),
      ),
      Effect.map((rows) => rows.flat()),
      Effect.mapError(failure),
    );
  const strings = (value: unknown): readonly string[] =>
    Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
  return {
    insert: (sessionId: string, observationId: string, paths: readonly string[]) =>
      Effect.try({
        try: () => {
          if (readOnly) throw new Error("Read-only provenance");
          db.prepare("INSERT OR REPLACE INTO sources VALUES (?, ?, ?)").run(
            sessionId,
            observationId,
            JSON.stringify(paths),
          );
        },
        catch: failure,
      }),
    attach: Effect.fn("Memory.attachSources")(function* (
      hit: Row,
    ): Effect.fn.Return<Row & { sourcePaths: string[] }, MemoryProtocolError> {
      const observation = yield* Schema.decodeUnknownEffect(Schema.optional(Row))(
        hit.observation,
      ).pipe(Effect.mapError(failure));
      const ids = [hit.obsId, ...strings(observation?.sourceObservationIds)].filter(
        (id): id is string => typeof id === "string",
      );
      let paths = (yield* Effect.forEach(ids, (id) => lookup("observation_id", id))).flat();
      if (!paths.length) {
        const sessions = [hit.sessionId, ...strings(observation?.sessionIds)].filter(
          (id): id is string => typeof id === "string",
        );
        paths = (yield* Effect.forEach(sessions, (id) => lookup("session_id", id))).flat();
      }
      return { ...hit, sourcePaths: [...new Set(paths)] };
    }),
  };
});
export type Provenance = Effect.Success<ReturnType<typeof openProvenance>>;
