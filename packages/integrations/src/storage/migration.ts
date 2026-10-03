import { isDeepStrictEqual } from "node:util";
import { PiStorageLease } from "@aster/agent";
import { Effect, Schema } from "effect";
import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import { createSession } from "@earendil-works/pi-durable";
import { openNodeJsonlStorage } from "@earendil-works/pi-durable/storage/jsonl/node";
import {
  contextBackendFor,
  normalizeContextRevision,
  DurableContextSnapshot,
  type ContextRecord,
} from "@aster/core";
import { makeFileContextStore } from "./file-context-store.js";
import { acquireActorStoreLock } from "./actor-store-lock.js";
import { readPiContextSnapshots, writePiContextSnapshot } from "./pi-durable-context.js";
import {
  routingAuthorityStore,
  StorageAuthority,
  StorageRoutingError,
  validateStorageAuthority,
} from "./routing.js";

const failure = (message: string) => (cause: unknown) =>
  new StorageRoutingError({ message, cause });
const openPiArchive = (
  pi: NonNullable<StorageAuthority["pi"]>,
  quarantine: Effect.Effect<void> = Effect.void,
) =>
  Effect.acquireRelease(
    Effect.tryPromise({
      try: async (signal) =>
        createSession(
          await openNodeJsonlStorage(pi.directory, withAbortSignal(signal, BACKGROUND_CONTEXT), {
            fsync: true,
          }),
        ),
      catch: failure("Cannot open Pi archive for offline migration"),
    }),
    (session) =>
      Effect.tryPromise({
        try: () => session.close(BACKGROUND_CONTEXT),
        catch: failure("Pi archive close is uncertain"),
      }).pipe(
        Effect.tapError(() => quarantine),
        Effect.orDie,
      ),
  );
const readPi = (session: Effect.Success<ReturnType<typeof openPiArchive>>, ownerId: string) =>
  Effect.tryPromise({
    try: (signal) =>
      readPiContextSnapshots(session, ownerId, withAbortSignal(signal, BACKGROUND_CONTEXT), true),
    catch: failure("Cannot validate Pi Context archive"),
  });

/** Offline, model-free migration. The old authority remains valid until every
 * selected snapshot has been copied and verified from reopened storage. */
export const migrateContextStorage = Effect.fn("ContextStorage.migrate")(function* (options: {
  readonly root: string;
  readonly authority: StorageAuthority;
}) {
  const target = yield* validateStorageAuthority(options.root, options.authority);
  yield* Effect.acquireRelease(
    Effect.try({
      try: () => acquireActorStoreLock(options.root, { recoverStale: false }),
      catch: failure("Stop Aster before migrating its Context storage"),
    }),
    (release) => Effect.sync(release),
  );
  const authority = yield* routingAuthorityStore(options.root);
  const previous = yield* authority.read;
  const source: StorageAuthority = previous?.value ?? { ...target, routes: [] };
  if (
    source.localDirectory !== target.localDirectory ||
    (source.pi && !isDeepStrictEqual(source.pi, target.pi))
  )
    return yield* new StorageRoutingError({
      message:
        "Keep existing storage directories and owner identity when changing routes; directory relocation is a separate operation",
    });
  const pi = target.pi;
  const lease = pi
    ? yield* PiStorageLease.acquire(pi.directory, pi.ownerId).pipe(
        Effect.mapError(failure("Pi storage is already owned or unavailable")),
      )
    : undefined;
  const local = yield* Effect.try({
    try: () => makeFileContextStore(target.localDirectory),
    catch: failure("Cannot open Local Context archive"),
  });
  const loadLocal = Effect.try({
    try: () =>
      Schema.decodeUnknownSync(Schema.Array(DurableContextSnapshot))(local.loadAll()).map(
        normalizeContextRevision,
      ),
    catch: failure("Cannot validate Local Context archive"),
  });
  const snapshot = yield* Effect.scoped(
    Effect.gen(function* () {
      const localRecords = yield* loadLocal;
      const session = pi ? yield* openPiArchive(pi, lease?.quarantine) : undefined;
      const restored = session && pi ? yield* readPi(session, pi.ownerId) : undefined;
      const stores = {
        local: new Map(localRecords.map((record) => [record.path, record])),
        pi: new Map(
          restored?.records.map((record) => [record.path, normalizeContextRevision(record)]) ?? [],
        ),
      };
      const paths = [...new Set([...stores.local.keys(), ...stores.pi.keys()])].sort();
      const canonical: ContextRecord[] = [];
      let copied = 0;
      // Validate the complete plan before starting the first destination write.
      for (const path of paths) {
        const record = stores[contextBackendFor(path, source.routes)].get(path);
        if (!record)
          return yield* new StorageRoutingError({
            message: `Authoritative Context is missing: ${path}`,
          });
        for (const store of Object.values(stores)) {
          const copy = store.get(path);
          if (
            copy &&
            ((copy.revision ?? 0) > (record.revision ?? 0) ||
              (copy.revision === record.revision && !isDeepStrictEqual(copy, record)))
          )
            return yield* new StorageRoutingError({
              message: `Stored copy diverges from the authoritative Context: ${path}`,
            });
        }
        canonical.push(record);
      }
      for (const record of canonical) {
        const backend = contextBackendFor(record.path, target.routes);
        const existing = stores[backend].get(record.path);
        if (existing && isDeepStrictEqual(record, existing)) continue;
        if (backend === "local") {
          yield* Effect.try({
            try: () => local.save(record),
            catch: failure(`Cannot import Local Context: ${record.path}`),
          });
        } else {
          if (!session || !pi || !restored)
            return yield* new StorageRoutingError({ message: "Pi destination is not configured" });
          const id = yield* Effect.tryPromise({
            try: (signal) =>
              writePiContextSnapshot(
                session,
                pi.ownerId,
                restored.conversations,
                record,
                withAbortSignal(signal, BACKGROUND_CONTEXT),
                { expectedStoredRevision: existing?.revision },
              ),
            catch: failure(`Pi Context import outcome requires reconciliation: ${record.path}`),
          });
          restored.conversations.set(record.path, id);
        }
        stores[backend].set(record.path, record);
        copied++;
      }
      return { records: canonical, copied };
    }),
  );
  // The lease remains held while all Pi handles close and reopen for disk replay.
  yield* Effect.scoped(
    Effect.gen(function* () {
      const localRecords = new Map((yield* loadLocal).map((record) => [record.path, record]));
      const session = pi ? yield* openPiArchive(pi, lease?.quarantine) : undefined;
      const restored = session && pi ? yield* readPi(session, pi.ownerId) : undefined;
      const stores = {
        local: localRecords,
        pi: new Map(
          restored?.records.map((record) => [record.path, normalizeContextRevision(record)]) ?? [],
        ),
      };
      for (const record of snapshot.records)
        if (
          !isDeepStrictEqual(
            stores[contextBackendFor(record.path, target.routes)].get(record.path),
            record,
          )
        )
          return yield* new StorageRoutingError({
            message: `Migrated Context failed disk replay validation: ${record.path}`,
          });
    }),
  );
  if (!previous || !isDeepStrictEqual(previous.value, target))
    yield* authority.publish(target, previous?.revision ?? 0);
  return { checked: snapshot.records.length, copied: snapshot.copied, routes: target.routes };
});
