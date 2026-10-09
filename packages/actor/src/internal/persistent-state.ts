import { Effect, Schema } from "effect";
import { deserialize, serialize } from "node:v8";
import type { ActorContext, PersistentActorBehavior, PersistentActorContext } from "../actor.js";
import type { ActorPersistence } from "../persistence.js";

type Behavior = PersistentActorBehavior<any, any, any, any>;
const encodePayload = (value: unknown): string => serialize(value).toString("base64");
const decodePayload = (payload: string): unknown => deserialize(Buffer.from(payload, "base64"));

/** State belongs to one behavior instance; mailbox and supervision never mutate it directly. */
export class PersistentState {
  private state: unknown;
  private sequenceNumber = 0;

  private constructor(
    private readonly behavior: Behavior,
    private readonly store: ActorPersistence["Service"],
    private readonly id: string,
  ) {}

  static recover(behavior: Behavior, store: ActorPersistence["Service"], path: string) {
    return Effect.gen(function* () {
      const journal = new PersistentState(behavior, store, behavior.persistenceId?.(path) ?? path);
      const recovered = yield* store.load(journal.id).pipe(Effect.orDie);
      journal.state =
        recovered.snapshot === undefined
          ? journal.clone(behavior.initialState)
          : Schema.decodeSync(behavior.stateSchema)(decodePayload(recovered.snapshot.state));
      journal.sequenceNumber = recovered.snapshot?.sequenceNumber ?? 0;
      for (const event of recovered.events) {
        if (event.sequenceNumber !== journal.sequenceNumber + 1)
          return yield* Effect.die(new Error(`Journal gap for ${journal.id}`));
        journal.apply(Schema.decodeSync(behavior.eventSchema)(decodePayload(event.payload)));
        journal.sequenceNumber = event.sequenceNumber;
      }
      if (journal.sequenceNumber !== recovered.sequenceNumber)
        return yield* Effect.die(new Error(`Journal sequence mismatch for ${journal.id}`));
      // Saving a snapshot and pruning its journal are separate durable operations.
      // Recovery retries pruning if the preceding process stopped between them.
      if (recovered.snapshot !== undefined)
        yield* store.cleanup(journal.id, recovered.snapshot.sequenceNumber).pipe(Effect.orDie);
      return journal;
    });
  }

  private clone(state: unknown): any {
    const encoded = Schema.encodeSync(this.behavior.stateSchema)(state);
    return Schema.decodeSync(this.behavior.stateSchema)(structuredClone(encoded));
  }

  private apply(event: unknown): void {
    this.state = this.clone(this.behavior.applyEvent(this.clone(this.state), event));
  }

  private persistAll(events: ReadonlyArray<unknown>): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      if (!events.length) return;
      const payloads = events.map((event) =>
        encodePayload(Schema.encodeSync(this.behavior.eventSchema)(event)),
      );
      // Commit first. A reducer defect must recover the committed events, not retry the command.
      const next = yield* this.store
        .append(this.id, this.sequenceNumber, payloads)
        .pipe(Effect.orDie);
      for (const event of events) this.apply(event);
      this.sequenceNumber = next;
    });
  }

  context(base: ActorContext<any>): PersistentActorContext<any, any, any> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias -- The getter reads the current journal state.
    const journal = this;
    return {
      ...base,
      // Detached, schema-round-tripped views protect state even from unchecked JS callers.
      get state() {
        return journal.clone(journal.state);
      },
      persist: (event) => this.persistAll([event]),
      persistAll: (events) => this.persistAll(events),
      saveSnapshot: () =>
        Effect.gen({ self: this }, function* () {
          const encoded = encodePayload(Schema.encodeSync(this.behavior.stateSchema)(this.state));
          yield* this.store.saveSnapshot(this.id, this.sequenceNumber, encoded).pipe(Effect.orDie);
          yield* this.store.cleanup(this.id, this.sequenceNumber).pipe(Effect.orDie);
        }),
    };
  }
}
