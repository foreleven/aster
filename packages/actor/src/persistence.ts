import { DatabaseSync } from "node:sqlite";
import { Context, Data, Effect, Layer } from "effect";

export class PersistenceError extends Data.TaggedError("PersistenceError")<{
  readonly operation: "open" | "load" | "append" | "snapshot" | "cleanup";
  readonly id?: string;
  readonly message: string;
  readonly cause?: unknown;
}> {}

export class PersistenceConflict extends Data.TaggedError("PersistenceConflict")<{
  readonly id: string;
  readonly operation: "append" | "snapshot";
  readonly expected: number;
  readonly actual: number;
}> {
  override get message() {
    return `${this.operation === "append" ? "Persistence" : "Snapshot"} sequence conflict for ${this.id}`;
  }
}

export type PersistenceFailure = PersistenceError | PersistenceConflict;

export interface PersistedEvent {
  readonly sequenceNumber: number;
  readonly payload: string;
}

export interface PersistedSnapshot {
  readonly sequenceNumber: number;
  readonly state: string;
}

export interface RecoveredStream {
  readonly sequenceNumber: number;
  readonly snapshot: PersistedSnapshot | undefined;
  readonly events: ReadonlyArray<PersistedEvent>;
}

export interface PersistenceStore {
  readonly load: (id: string) => Effect.Effect<RecoveredStream, PersistenceFailure>;
  readonly append: (
    id: string,
    expectedSequenceNumber: number,
    payloads: ReadonlyArray<string>,
  ) => Effect.Effect<number, PersistenceFailure>;
  readonly saveSnapshot: (
    id: string,
    sequenceNumber: number,
    state: string,
  ) => Effect.Effect<void, PersistenceFailure>;
  readonly cleanup: (
    id: string,
    throughSequenceNumber: number,
  ) => Effect.Effect<void, PersistenceFailure>;
}

export class ActorPersistence extends Context.Service<ActorPersistence, PersistenceStore>()(
  "@aster/actor/ActorPersistence",
) {}

interface MemoryStream {
  sequenceNumber: number;
  snapshot: PersistedSnapshot | undefined;
  events: Array<PersistedEvent>;
}

const emptyStream = (): MemoryStream => ({ sequenceNumber: 0, snapshot: undefined, events: [] });

export const InMemoryActorPersistence = {
  layer: Layer.effect(
    ActorPersistence,
    Effect.sync((): PersistenceStore => {
      const streams = new Map<string, MemoryStream>();
      const get = (id: string): MemoryStream => {
        let stream = streams.get(id);
        if (stream === undefined) {
          stream = emptyStream();
          streams.set(id, stream);
        }
        return stream;
      };

      return {
        load: (id) =>
          Effect.sync(() => {
            const stream = get(id);
            return {
              sequenceNumber: stream.sequenceNumber,
              snapshot: stream.snapshot,
              events: stream.events.filter(
                (event) => event.sequenceNumber > (stream.snapshot?.sequenceNumber ?? 0),
              ),
            };
          }),
        append: (id, expected, payloads) =>
          Effect.try({
            try: () => {
              const stream = get(id);
              if (stream.sequenceNumber !== expected) {
                throw new PersistenceConflict({
                  id,
                  operation: "append",
                  expected,
                  actual: stream.sequenceNumber,
                });
              }
              for (const payload of payloads) {
                stream.events.push({ sequenceNumber: ++stream.sequenceNumber, payload });
              }
              return stream.sequenceNumber;
            },
            catch: toError("append", id),
          }),
        saveSnapshot: (id, sequenceNumber, state) =>
          Effect.try({
            try: () => {
              const stream = get(id);
              if (stream.sequenceNumber !== sequenceNumber) {
                throw new PersistenceConflict({
                  id,
                  operation: "snapshot",
                  expected: sequenceNumber,
                  actual: stream.sequenceNumber,
                });
              }
              stream.snapshot = { sequenceNumber, state };
            },
            catch: toError("snapshot", id),
          }),
        cleanup: (id, throughSequenceNumber) =>
          Effect.sync(() => {
            const stream = get(id);
            stream.events = stream.events.filter(
              (event) => event.sequenceNumber > throughSequenceNumber,
            );
          }),
      };
    }),
  ),
} as const;

const toError =
  (operation: PersistenceError["operation"], id?: string) =>
  (cause: unknown): PersistenceFailure =>
    cause instanceof PersistenceConflict || cause instanceof PersistenceError
      ? cause
      : new PersistenceError({
          operation,
          ...(id === undefined ? {} : { id }),
          cause,
          message: cause instanceof Error ? cause.message : String(cause),
        });

export const SqliteActorPersistence = {
  layer: ({ path }: { readonly path: string }) =>
    Layer.effect(
      ActorPersistence,
      Effect.acquireRelease(
        Effect.try({
          try: () => {
            const database = new DatabaseSync(path);
            try {
              database.exec(`
              PRAGMA journal_mode = WAL;
              CREATE TABLE IF NOT EXISTS actor_streams (
                id TEXT PRIMARY KEY,
                sequence_number INTEGER NOT NULL
              );
              CREATE TABLE IF NOT EXISTS actor_events (
                id TEXT NOT NULL,
                sequence_number INTEGER NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (id, sequence_number)
              );
              CREATE TABLE IF NOT EXISTS actor_snapshots (
                id TEXT PRIMARY KEY,
                sequence_number INTEGER NOT NULL,
                state TEXT NOT NULL
              );
            `);
              return database;
            } catch (error) {
              database.close();
              throw error;
            }
          },
          catch: toError("open"),
        }),
        (database) => Effect.sync(() => database.close()),
      ).pipe(
        Effect.map((database): PersistenceStore => ({
          load: (id) =>
            Effect.try({
              try: () => {
                const stream = database
                  .prepare("SELECT sequence_number FROM actor_streams WHERE id = ?")
                  .get(id);
                const snapshot = database
                  .prepare("SELECT sequence_number, state FROM actor_snapshots WHERE id = ?")
                  .get(id);
                const events = database
                  .prepare(
                    "SELECT sequence_number, payload FROM actor_events WHERE id = ? AND sequence_number > ? ORDER BY sequence_number",
                  )
                  .all(id, Number(snapshot?.sequence_number ?? 0));
                return {
                  sequenceNumber: Number(stream?.sequence_number ?? 0),
                  snapshot:
                    snapshot === undefined
                      ? undefined
                      : {
                          sequenceNumber: Number(snapshot.sequence_number),
                          state: String(snapshot.state),
                        },
                  events: events.map((event) => ({
                    sequenceNumber: Number(event.sequence_number),
                    payload: String(event.payload),
                  })),
                };
              },
              catch: toError("load", id),
            }),
          append: (id, expected, payloads) =>
            Effect.try({
              try: () => {
                database.exec("BEGIN IMMEDIATE");
                try {
                  const row = database
                    .prepare("SELECT sequence_number FROM actor_streams WHERE id = ?")
                    .get(id);
                  const current = Number(row?.sequence_number ?? 0);
                  if (current !== expected)
                    throw new PersistenceConflict({
                      id,
                      operation: "append",
                      expected,
                      actual: current,
                    });
                  let next = current;
                  const insert = database.prepare(
                    "INSERT INTO actor_events (id, sequence_number, payload) VALUES (?, ?, ?)",
                  );
                  for (const payload of payloads) insert.run(id, ++next, payload);
                  database
                    .prepare(
                      "INSERT INTO actor_streams (id, sequence_number) VALUES (?, ?) " +
                        "ON CONFLICT(id) DO UPDATE SET sequence_number = excluded.sequence_number",
                    )
                    .run(id, next);
                  database.exec("COMMIT");
                  return next;
                } catch (error) {
                  database.exec("ROLLBACK");
                  throw error;
                }
              },
              catch: toError("append", id),
            }),
          saveSnapshot: (id, sequenceNumber, state) =>
            Effect.try({
              try: () => {
                database.exec("BEGIN IMMEDIATE");
                try {
                  const row = database
                    .prepare("SELECT sequence_number FROM actor_streams WHERE id = ?")
                    .get(id);
                  if (Number(row?.sequence_number ?? 0) !== sequenceNumber) {
                    throw new PersistenceConflict({
                      id,
                      operation: "snapshot",
                      expected: sequenceNumber,
                      actual: Number(row?.sequence_number ?? 0),
                    });
                  }
                  database
                    .prepare(
                      "INSERT INTO actor_snapshots (id, sequence_number, state) VALUES (?, ?, ?) " +
                        "ON CONFLICT(id) DO UPDATE SET sequence_number = excluded.sequence_number, state = excluded.state",
                    )
                    .run(id, sequenceNumber, state);
                  database.exec("COMMIT");
                } catch (error) {
                  database.exec("ROLLBACK");
                  throw error;
                }
              },
              catch: toError("snapshot", id),
            }),
          cleanup: (id, throughSequenceNumber) =>
            Effect.try({
              try: () => {
                database
                  .prepare("DELETE FROM actor_events WHERE id = ? AND sequence_number <= ?")
                  .run(id, throughSequenceNumber);
              },
              catch: toError("cleanup", id),
            }),
        })),
      ),
    ),
} as const;
