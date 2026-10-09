import { Context, Data, Effect, Layer, Schema, SchemaAST, Scope, type Duration } from "effect";
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

export class ActorStartupError extends Data.TaggedError("ActorStartupError")<{
  readonly path: ActorPath;
  readonly cause: unknown;
}> {
  override get message() {
    return `Actor startup failed: ${this.path}`;
  }
}

export interface ActorRef<in Command> {
  readonly path: ActorPath;
  readonly incarnation: string;
  /** First initialization outcome, including Layer acquisition, recovery and started.
   * Not a processing barrier or health check. Cancelling a waiter does not stop the Actor.
   */
  readonly awaitStarted: Effect.Effect<void, ActorStartupError>;
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
      "awaitStarted" in value &&
      Effect.isEffect(value.awaitStarted) &&
      "tell" in value &&
      typeof value.tell === "function" &&
      "ask" in value &&
      typeof value.ask === "function",
  );

export interface ActorContext<Command> {
  select(path: string): ActorSelection;
  readonly self: ActorRef<Command>;
  readonly path: ActorPath;
  readonly metadata: Readonly<Record<string, unknown>>;
  /** Register a child immediately; Layer acquisition, recovery and started run asynchronously. */
  spawn<Definition extends AnyActorDefinition>(
    name: string,
    definition: Definition,
    options?: SpawnOptions,
  ): Effect.Effect<ActorRef<CommandOf<Definition>>, SpawnError, ServicesOf<Definition>>;
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
    context: ActorContext<Command>,
  ) => Effect.Effect<void, never, Services>;
  readonly started?: (context: ActorContext<Command>) => Effect.Effect<void, unknown, Services>;
  readonly receiveSignal?: (
    signal: ActorSignal,
    context: ActorContext<Command>,
  ) => Effect.Effect<void, never, Services>;
}

export interface PersistentActorContext<Command, Event, State> extends ActorContext<Command> {
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
    context: PersistentActorContext<Command, Event, State>,
  ) => Effect.Effect<void, never, Services>;
  readonly applyEvent: (state: ReadonlyDeep<State>, event: Event) => State;
  readonly started?: (
    context: PersistentActorContext<Command, Event, State>,
  ) => Effect.Effect<void, unknown, Services>;
  readonly receiveSignal?: (
    signal: ActorSignal,
    context: PersistentActorContext<Command, Event, State>,
  ) => Effect.Effect<void, never, Services>;
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

/** Protocol declarations are evaluated before behavior inference so handler parameters stay precise. */
export interface Protocol<Commands extends readonly Schema.Top[], Internal extends Schema.Top> {
  readonly commands: Commands;
  readonly internal?: Internal;
}
export type MailboxOf<C extends readonly Schema.Top[], I extends Schema.Top> =
  C[number]["Type"] | I["Type"];

export interface Definition<Public, Behavior, E, R> extends Context.Service<Behavior, Behavior> {
  readonly actorKind: "actor" | "persistent";
  readonly layer: Layer.Layer<Behavior, E, R>;
  readonly commands: readonly Schema.Top[];
  readonly __command: Public;
  readonly __services: R;
}

const protocolTags = (ast: SchemaAST.AST): readonly string[] => {
  if (SchemaAST.isUnion(ast)) return ast.types.flatMap(protocolTags);
  if (!SchemaAST.isObjects(ast)) return [];
  const tag = ast.propertySignatures.find((field) => field.name === "_tag")?.type;
  return tag && SchemaAST.isLiteral(tag) && typeof tag.literal === "string" ? [tag.literal] : [];
};
const validateProtocol = (protocol: Protocol<readonly Schema.Top[], Schema.Top>) =>
  Effect.gen(function* () {
    const tags = new Set<string>();
    for (const schema of [...protocol.commands, protocol.internal ?? Schema.Never]) {
      for (const tag of protocolTags(Schema.toEncoded(schema).ast)) {
        if (tags.has(tag))
          return yield* Effect.die(new Error(`Duplicate Actor command tag: ${tag}`));
        tags.add(tag);
      }
    }
  });

const actorDefine =
  <const C extends readonly Schema.Top[], I extends Schema.Top = typeof Schema.Never>(
    key: string,
    protocol: Protocol<C, I>,
  ) =>
  <E, R, H = never>(
    acquire: Effect.Effect<ActorBehavior<MailboxOf<C, I>, H>, E, R>,
  ): Definition<
    C[number]["Type"],
    ActorBehavior<MailboxOf<C, I>, H>,
    E,
    Exclude<R | H, Scope.Scope>
  > => {
    const service = Context.Service<ActorBehavior<MailboxOf<C, I>, H>>(key);
    const layer = Layer.effect(
      service,
      Effect.gen(function* () {
        yield* validateProtocol(protocol);
        const environment = yield* Effect.context<H>();
        const behavior = yield* acquire;
        return {
          ...behavior,
          receive: (command: MailboxOf<C, I>, actor: ActorContext<MailboxOf<C, I>>) =>
            behavior.receive(command, actor).pipe(Effect.provideContext(environment)),
          ...(behavior.started
            ? {
                started: (actor: ActorContext<MailboxOf<C, I>>) =>
                  behavior.started!(actor).pipe(Effect.provideContext(environment)),
              }
            : {}),
          ...(behavior.receiveSignal
            ? {
                receiveSignal: (signal: ActorSignal, actor: ActorContext<MailboxOf<C, I>>) =>
                  behavior.receiveSignal!(signal, actor).pipe(Effect.provideContext(environment)),
              }
            : {}),
        };
      }),
    );
    // Phantom protocol/dependency members have no runtime value. The service key and Layer
    // remain intact for the runtime's heterogeneous registry and per-instance acquisition.
    return Object.assign(service, {
      actorKind: "actor" as const,
      commands: protocol.commands,
      layer,
      __command: undefined as never,
      __services: undefined as never,
    });
  };

const persistentDefine =
  <
    const C extends readonly Schema.Top[],
    Event extends Schema.Codec<any, any>,
    State extends Schema.Codec<any, any>,
    I extends Schema.Top = typeof Schema.Never,
  >(
    key: string,
    protocol: Protocol<C, I> & { readonly event: Event; readonly state: State },
  ) =>
  <E, R, H = never>(
    acquire: Effect.Effect<
      Omit<
        PersistentActorBehavior<MailboxOf<C, I>, Event["Type"], State["Type"], H>,
        "eventSchema" | "stateSchema"
      >,
      E,
      R
    >,
  ): Definition<
    C[number]["Type"],
    PersistentActorBehavior<MailboxOf<C, I>, Event["Type"], State["Type"], H>,
    E,
    Exclude<R | H, Scope.Scope> | ActorPersistence
  > => {
    type Behavior = PersistentActorBehavior<MailboxOf<C, I>, Event["Type"], State["Type"], H>;
    type Owner = PersistentActorContext<MailboxOf<C, I>, Event["Type"], State["Type"]>;
    const service = Context.Service<Behavior>(key);
    const layer = Layer.effect(
      service,
      Effect.gen(function* () {
        yield* validateProtocol(protocol);
        const environment = yield* Effect.context<H>();
        const behavior = yield* acquire;
        return {
          ...behavior,
          eventSchema: protocol.event,
          stateSchema: protocol.state,
          receive: (command: MailboxOf<C, I>, actor: Owner) =>
            behavior.receive(command, actor).pipe(Effect.provideContext(environment)),
          ...(behavior.started
            ? {
                started: (actor: Owner) =>
                  behavior.started!(actor).pipe(Effect.provideContext(environment)),
              }
            : {}),
          ...(behavior.receiveSignal
            ? {
                receiveSignal: (signal: ActorSignal, actor: Owner) =>
                  behavior.receiveSignal!(signal, actor).pipe(Effect.provideContext(environment)),
              }
            : {}),
        };
      }),
    );
    return Object.assign(service, {
      actorKind: "persistent" as const,
      commands: protocol.commands,
      layer,
      __command: undefined as never,
      __services: undefined as never,
    });
  };

/** Supply behavior-local dependencies without hiding the remaining system requirements. */
const provide =
  <Out, E2, R2>(dependency: Layer.Layer<Out, E2, R2>) =>
  <P, B, E, R>(
    definition: Definition<P, B, E, R>,
  ): Definition<P, B, E | E2, Exclude<R, Out> | R2> =>
    Object.assign(Context.Service<B>(definition.key), {
      actorKind: definition.actorKind,
      commands: definition.commands,
      layer: definition.layer.pipe(Layer.provide(dependency)),
      __command: undefined as never,
      __services: undefined as never,
    });

export const Actor = { define: actorDefine, provide } as const;
export const PersistentActor = { define: persistentDefine, provide } as const;

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
