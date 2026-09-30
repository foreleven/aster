import { Context, Effect, Layer, Schema, type Duration } from "effect";
import type { ActorPersistence } from "./persistence.js";

export type ActorPath = string;
export type SupervisionDirective = "restart" | "stop" | "escalate";
export type ReadonlyDeep<Value> = Value extends (...args: any[]) => any
  ? Value
  : Value extends ReadonlyArray<infer Item>
    ? ReadonlyArray<ReadonlyDeep<Item>>
    : Value extends Map<infer Key, infer Item>
      ? ReadonlyMap<ReadonlyDeep<Key>, ReadonlyDeep<Item>>
      : Value extends Set<infer Item>
        ? ReadonlySet<ReadonlyDeep<Item>>
        : Value extends object
          ? { readonly [Key in keyof Value]: ReadonlyDeep<Value[Key]> }
          : Value;

export interface FailureSummary {
  readonly message: string;
  readonly stack?: string;
}

export interface Terminated {
  readonly _tag: "Terminated";
  readonly ref: ActorRef<any>;
  readonly cause?: FailureSummary;
}

export type ActorSignal = Terminated;

export interface SpawnOptions {
  readonly supervision?: (cause: FailureSummary) => SupervisionDirective;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export class SpawnError extends Error {
  readonly _tag = "SpawnError";
  constructor(message: string) {
    super(message);
  }
}

export class AskTimeoutError extends Error {
  readonly _tag = "AskTimeoutError";
  constructor(path: ActorPath) {
    super(`Ask timed out: ${path}`);
  }
}

export interface ActorRef<in Command> {
  readonly path: ActorPath;
  readonly incarnation: string;
  /** Enqueue only; stopped targets emit a redacted DeadLetter instead of failing the sender. */
  tell(command: Command): Effect.Effect<void>;
  /** First reply wins. Timeout/cancellation closes the reply ref, not the receiver's work. */
  ask<Response>(
    makeCommand: (replyTo: ActorRef<Response>) => Command,
    timeout?: Duration.Input,
  ): Effect.Effect<Response, AskTimeoutError>;
}

export class ActorNotFound extends Error {
  readonly _tag = "ActorNotFound";
  constructor(readonly path: string) {
    super(`Actor not found: ${path}`);
  }
}
export interface ActorSelection {
  readonly path: ActorPath;
  resolve(): Effect.Effect<ActorRef<unknown>, ActorNotFound>;
}

/** Exact local paths only. Persist the normalized absolute path, never a runtime ref. */
export const actorSelectionPath = (path: string, base = "/"): string => {
  // eslint-disable-next-line no-control-regex -- Actor paths must reject control characters.
  if (!path || /[\x00-\x1f\x7f*?#]/.test(path) || path.includes(":"))
    throw new Error("Invalid local Actor selection");
  const parts = path.startsWith("/") ? [] : base.split("/").filter(Boolean);
  for (const segment of path.split("/")) {
    if (!segment || segment === ".") continue;
    if (segment === "..") {
      if (!parts.length) throw new Error("Actor selection escapes root");
      parts.pop();
    } else parts.push(segment);
  }
  return "/" + parts.join("/");
};

export type ReplyTo<Response> = ActorRef<Response>;

export const ReplyTo = <Response>() =>
  Schema.declare<ReplyTo<Response>>(
    (value): value is ReplyTo<Response> =>
      typeof value === "object" &&
      value !== null &&
      "path" in value &&
      typeof value.path === "string" &&
      "incarnation" in value &&
      typeof value.incarnation === "string" &&
      "tell" in value &&
      typeof value.tell === "function" &&
      "ask" in value &&
      typeof value.ask === "function",
  );

export interface ActorContext<Command, Services = never> {
  select(path: string): ActorSelection;
  readonly self: ActorRef<Command>;
  readonly path: ActorPath;
  readonly metadata: Readonly<Record<string, unknown>>;
  /** Register a child immediately; Layer acquisition, recovery and started run asynchronously. */
  spawn<Definition extends AnyActorDefinition>(
    name: string,
    definition: Definition & RequireServices<Definition, Services>,
    options?: SpawnOptions,
  ): Effect.Effect<ActorRef<CommandOf<Definition>>, SpawnError>;
  /** Request a direct child's stop without awaiting it; watch observes final termination. */
  stop<Child>(ref: ActorRef<Child>): Effect.Effect<void>;
  /** Finish this handler, discard queued work, then stop descendants and close resources. */
  stopSelf(): Effect.Effect<void>;
  watch<Watched>(ref: ActorRef<Watched>): Effect.Effect<void>;
  child(name: string): Effect.Effect<ActorRef<unknown> | undefined>;
  children(): Effect.Effect<ReadonlyArray<ActorRef<unknown>>>;
  receiveTimeout(duration: Duration.Input): Effect.Effect<void>;
  /** Behavior-scoped work: expected errors become commands; defects enter supervision. */
  pipeToSelf<A, E>(
    effect: Effect.Effect<A, E, never>,
    toCommand: (
      result:
        | { readonly _tag: "Success"; readonly value: A }
        | { readonly _tag: "Failure"; readonly error: E },
    ) => Command,
  ): Effect.Effect<void>;
}

export interface ActorBehavior<Command, Services = never> {
  readonly receive: (
    command: Command,
    context: ActorContext<Command, Services>,
  ) => Effect.Effect<void>;
  readonly started?: (
    context: ActorContext<Command, Services>,
  ) => Effect.Effect<void, unknown, Services>;
  readonly receiveSignal?: (
    signal: ActorSignal,
    context: ActorContext<Command, Services>,
  ) => Effect.Effect<void>;
}

export interface PersistentActorContext<
  Command,
  Event,
  State,
  Services = never,
> extends ActorContext<Command, Services | ActorPersistence> {
  /** Detached view of the latest committed state; read again after persisting to see updates. */
  readonly state: ReadonlyDeep<State>;
  persist(event: Event): Effect.Effect<void>;
  persistAll(events: ReadonlyArray<Event>): Effect.Effect<void>;
  saveSnapshot(): Effect.Effect<void>;
}

export interface PersistentActorBehavior<Command, Event, State, Services = never> {
  readonly initialState: State;
  readonly eventSchema: Schema.Codec<Event, any>;
  readonly stateSchema: Schema.Codec<State, any>;
  readonly receive: (
    command: Command,
    context: PersistentActorContext<Command, Event, State, Services>,
  ) => Effect.Effect<void>;
  readonly applyEvent: (state: ReadonlyDeep<State>, event: Event) => State;
  readonly started?: (
    context: PersistentActorContext<Command, Event, State, Services>,
  ) => Effect.Effect<void, unknown>;
  readonly receiveSignal?: (
    signal: ActorSignal,
    context: PersistentActorContext<Command, Event, State, Services>,
  ) => Effect.Effect<void>;
  readonly persistenceId?: (path: ActorPath) => string;
}

export interface AnyActorDefinition {
  readonly actorKind: "actor" | "persistent";
  readonly layer: Layer.Layer<never, any, any>;
  readonly __command: unknown;
  readonly __services: unknown;
}

export type CommandOf<Definition> = Definition extends { readonly __command: infer Command }
  ? Command
  : never;
export type ServicesOf<Definition> = Definition extends { readonly __services: infer Services }
  ? Services
  : never;
export type RequireServices<Definition, Available> = [
  | ServicesOf<Definition>
  | (Definition extends { readonly layer: infer L }
      ? L extends Layer.Layer<never, any, any>
        ? Layer.Services<L>
        : never
      : never),
] extends [Available]
  ? unknown
  : never;

type ActorDefinition<Self, Command, Services> = ReturnType<
  ReturnType<typeof Context.Service<Self, ActorBehavior<Command, Services>>>
> & {
  readonly actorKind: "actor";
  readonly __command: Command;
  readonly __services: Services;
};

type PersistentActorDefinition<Self, Command, Event, State, Services> = ReturnType<
  ReturnType<typeof Context.Service<Self, PersistentActorBehavior<Command, Event, State, Services>>>
> & {
  readonly actorKind: "persistent";
  readonly __command: Command;
  readonly __services: Services | ActorPersistence;
};

type SchemaPersistentActorDefinition<Self, Command, Event, State, Services> =
  PersistentActorDefinition<Self, Command, Event, State, Services> & {
    readonly of: (
      behavior: Omit<
        PersistentActorBehavior<Command, Event, State, Services>,
        "eventSchema" | "stateSchema"
      >,
    ) => PersistentActorBehavior<Command, Event, State, Services>;
  };

function actorService<Self, Services = never>(): <CommandSchema extends Schema.Schema<any>>(
  key: string,
  schemas: { readonly command: CommandSchema },
) => ActorDefinition<Self, Schema.Schema.Type<CommandSchema>, Services>;
function actorService(): any {
  // Local Commands may contain live ActorRefs. Their Schema supplies types only;
  // validation/encoding belongs at external boundaries, or to persistent events/state.
  return (key: string, _schemas: { readonly command: Schema.Schema<any> }) =>
    Object.assign(Context.Service<any, any>()(key), { actorKind: "actor" as const });
}

function persistentActorService<Self, Services = never>(): <
  CommandSchema extends Schema.Schema<any>,
  EventSchema extends Schema.Codec<any, any>,
  StateSchema extends Schema.Codec<any, any>,
>(
  key: string,
  schemas: {
    readonly command: CommandSchema;
    readonly event: EventSchema;
    readonly state: StateSchema;
  },
) => SchemaPersistentActorDefinition<
  Self,
  Schema.Schema.Type<CommandSchema>,
  Schema.Schema.Type<EventSchema>,
  Schema.Schema.Type<StateSchema>,
  Services
>;
function persistentActorService(): any {
  return (
    key: string,
    schemas: {
      readonly command: Schema.Schema<any>;
      readonly event: Schema.Codec<any, any>;
      readonly state: Schema.Codec<any, any>;
    },
  ) => {
    const definition = Context.Service<any, any>()(key);
    const of = definition.of;
    return Object.assign(definition, {
      of: (behavior: object) =>
        of({ ...behavior, eventSchema: schemas.event, stateSchema: schemas.state }),
      actorKind: "persistent" as const,
    });
  };
}

export const Actor = { Service: actorService } as const;
export const PersistentActor = { Service: persistentActorService } as const;

export type ActorSystemEvent =
  | {
      readonly _tag: "CommandProcessed";
      readonly path: ActorPath;
      readonly incarnation: string;
      readonly commandTag?: string;
      readonly success: boolean;
      readonly timestamp: string;
    }
  | {
      readonly _tag: "DeadLetter";
      readonly target: ActorPath;
      readonly incarnation: string;
      readonly commandTag?: string;
      readonly reason: string;
      readonly timestamp: string;
    }
  | {
      readonly _tag: "ActorRestarting";
      readonly path: ActorPath;
      readonly incarnation: string;
      readonly cause: FailureSummary;
      readonly timestamp: string;
    }
  | {
      readonly _tag: "ActorStopped";
      readonly path: ActorPath;
      readonly incarnation: string;
      readonly cause?: FailureSummary;
      readonly timestamp: string;
    };
