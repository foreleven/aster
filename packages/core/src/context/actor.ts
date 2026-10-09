import {
  Actor,
  ReplyTo,
  type ActorBehavior,
  type ActorContext,
  type ActorRef,
  type AnyActorDefinition,
  type CommandOf,
  type Protocol,
  type ServicesOf,
  type SpawnError,
  type SpawnOptions,
} from "@aster/actor";
import { Deferred, Effect, Predicate, Schema, Scope } from "effect";
import { randomUUID } from "node:crypto";
import { ContextQueryError, type ContextQueryResult } from "./contracts.js";
import type { ContextDefinition } from "./definition.js";
import {
  QueryInvocation,
  QueryReply,
  isQueryCommand,
  type QueryCommand,
} from "./queries/protocol.js";
import { ContextQueries } from "./queries/routes.js";
import { ContextRegistry } from "./registry.js";

export const childContextPath = (parentPath: string, relativePath: string): string => {
  const segments = relativePath.split("/");
  if (segments.some((segment) => !segment || segment === "." || segment === "..")) {
    throw new Error(`Invalid relative Context path: ${relativePath}`);
  }
  return `${parentPath}/${relativePath}`;
};

export const childActorName = (relativePath: string): string => {
  childContextPath("", relativePath);
  return relativePath.includes("/")
    ? `~${Buffer.from(relativePath).toString("base64url")}`
    : relativePath;
};

/** Public paths are independent of the runtime's /user guardian and encoded child names. */
export const contextPath = (context: Pick<ActorContext<unknown>, "path" | "metadata">): string => {
  const explicit = context.metadata.contextPath;
  if (explicit !== undefined) {
    if (typeof explicit !== "string") throw new Error("Context path metadata must be a string");
    childContextPath("", explicit.slice(1));
    if (!explicit.startsWith("/")) throw new Error(`Invalid Context path: ${explicit}`);
    return explicit;
  }
  const segments = context.path.split("/").slice(1);
  if (segments[0] === "user") segments.shift();
  return childContextPath(
    "",
    segments
      .map((part) =>
        part.startsWith("~") ? Buffer.from(part.slice(1), "base64url").toString("utf8") : part,
      )
      .join("/"),
  );
};

export const contextSpawnOptions = (path: string, options: SpawnOptions = {}): SpawnOptions => ({
  ...options,
  metadata: { ...options.metadata, contextPath: path },
});

type LocalMailbox<C extends readonly Schema.Top[], I extends Schema.Top> =
  Exclude<C[number], QueryCommand>["Type"] | I["Type"];
interface ContextBehavior<
  C extends readonly Schema.Top[],
  I extends Schema.Top,
  H,
> extends ActorBehavior<LocalMailbox<C, I>, H> {
  readonly query?: (
    command: Extract<C[number], QueryCommand>["Type"],
    actor: ActorContext<LocalMailbox<C, I>>,
  ) => Effect.Effect<ContextQueryResult, ContextQueryError, H>;
  readonly description?: string;
}

/** Context ownership and public capability registration follow the Behavior scope. */
export const ContextActor = {
  define:
    <const C extends readonly Schema.Top[], I extends Schema.Top = typeof Schema.Never>(
      key: string,
      options: Protocol<C, I> & { readonly context: ContextDefinition },
    ) =>
    <E, R, H = never>(acquire: Effect.Effect<ContextBehavior<C, I, H>, E, R>) => {
      const contracts = options.commands.filter(isQueryCommand);
      const internal = Schema.Union([options.internal ?? Schema.Never, QueryInvocation]);
      return Object.assign(
        Actor.define(key, { commands: options.commands, internal })(
          Effect.gen(function* () {
            const registry = yield* ContextRegistry;
            const scope = yield* Effect.scope;
            const environment = yield* Effect.context<H>();
            // The runtime filter and conditional requirement use the same descriptor marker.
            // Widened schema arrays conservatively retain the query-registry requirement.
            const routes = yield* (
              contracts.length ? ContextQueries : Effect.void
            ) as Effect.Effect<
              ContextQueries["Service"] | undefined,
              never,
              QueryCommand extends C[number]
                ? ContextQueries
                : Extract<C[number], QueryCommand> extends never
                  ? never
                  : ContextQueries
            >;
            const behavior = yield* acquire;
            type Owner = ActorContext<LocalMailbox<C, I>>;
            const withContextPath = <M>(actor: ActorContext<M>): typeof actor => ({
              ...actor,
              spawn: (name, child, spawnOptions) =>
                actor.spawn(
                  name,
                  child,
                  contextSpawnOptions(
                    typeof spawnOptions?.metadata?.contextPath === "string"
                      ? spawnOptions.metadata.contextPath
                      : childContextPath(
                          contextPath(actor),
                          name.startsWith("~")
                            ? Buffer.from(name.slice(1), "base64url").toString("utf8")
                            : name,
                        ),
                    spawnOptions,
                  ),
                ),
            });
            const pending = new Map<string, ReplyTo<typeof QueryReply.Type>>();
            const run = (tag: string, raw: unknown, actor: Owner) =>
              Effect.gen(function* () {
                const contract = contracts.find((candidate) => candidate._tag === tag);
                if (!contract || !behavior.query)
                  return yield* new ContextQueryError({
                    kind: "invalid-input",
                    message: "Unsupported Context command",
                  });
                const request = yield* Schema.decodeUnknownEffect(contract)(raw).pipe(
                  Effect.mapError(
                    () =>
                      new ContextQueryError({
                        kind: "invalid-input",
                        message: "Invalid command arguments",
                      }),
                  ),
                );
                // The registry erases individual schema types. Decoding with a member of C
                // reestablishes the union before handing it to the domain query behavior.
                return yield* behavior
                  .query(request as Extract<C[number], QueryCommand>["Type"], actor)
                  .pipe(Effect.provideContext(environment));
              });
            yield* Effect.addFinalizer(() =>
              Effect.forEach(
                pending.values(),
                (replyTo) =>
                  replyTo.tell({
                    _tag: "Failure",
                    error: new ContextQueryError({
                      kind: "unavailable",
                      message: "Context query owner stopped",
                    }),
                  }),
                { discard: true },
              ),
            );
            return {
              receive: (command, actor) =>
                Effect.gen(function* () {
                  if (Schema.is(QueryInvocation)(command)) {
                    if (command._tag === "ContextQueryCompleted") {
                      const replyTo = pending.get(command.id);
                      pending.delete(command.id);
                      if (replyTo) yield* replyTo.tell(command.result);
                      return;
                    }
                    if (yield* Deferred.isDone(command.cancelled)) return;
                    // Fresh ids isolate shared reply refs and queued completions from older Behaviors.
                    const id = randomUUID();
                    pending.set(id, command.replyTo);
                    yield* actor.pipeToSelf(
                      Effect.raceFirst(
                        run(
                          command.input.command,
                          {
                            ...command.input.args,
                            _tag: command.input.command,
                            replyTo: command.replyTo,
                          },
                          withContextPath(actor),
                        ),
                        Deferred.await(command.cancelled).pipe(
                          Effect.andThen(
                            Effect.fail(
                              new ContextQueryError({
                                kind: "unavailable",
                                message: "Context query cancelled",
                              }),
                            ),
                          ),
                        ),
                      ),
                      (result) => ({
                        _tag: "ContextQueryCompleted",
                        id,
                        result,
                      }),
                    );
                    return;
                  }
                  for (const contract of contracts) {
                    if (Predicate.hasProperty(command, "_tag") && command._tag === contract._tag) {
                      const request = yield* Schema.decodeUnknownEffect(
                        Schema.Struct({ replyTo: ReplyTo<typeof QueryReply.Type>() }),
                      )(command).pipe(Effect.orDie);
                      // Fresh ids isolate shared reply refs and queued completions from older Behaviors.
                      const id = randomUUID();
                      pending.set(id, request.replyTo);
                      yield* actor.pipeToSelf(
                        run(contract._tag, command, withContextPath(actor)),
                        (result) => ({
                          _tag: "ContextQueryCompleted",
                          id,
                          result,
                        }),
                      );
                      return;
                    }
                  }
                  // The generic registry erases individual contracts. Only the framework's
                  // query messages are intercepted; the remaining original mailbox is unchanged.
                  yield* behavior.receive(command as LocalMailbox<C, I>, withContextPath(actor));
                }),
              ...(behavior.receiveSignal
                ? {
                    receiveSignal: (signal: import("@aster/actor").ActorSignal, actor: Owner) =>
                      behavior.receiveSignal!(signal, withContextPath(actor)),
                  }
                : {}),
              started: (actor) =>
                Effect.gen(function* () {
                  const path = contextPath(actor);
                  yield* registry.register(path, options.context);
                  if (behavior.started) yield* behavior.started(withContextPath(actor));
                  if (routes) {
                    if (!behavior.query)
                      return yield* Effect.die(new Error(`Missing query behavior for ${key}`));
                    yield* routes
                      .register(
                        path,
                        {
                          description:
                            behavior.description ?? registry.get(path)?.description ?? path,
                          commands: Object.fromEntries(
                            contracts.map((contract) => [
                              contract._tag,
                              {
                                description: contract.description,
                                schema: contract.payloadSchema,
                              },
                            ]),
                          ),
                        },
                        (input) =>
                          Effect.acquireUseRelease(
                            Deferred.make<void>(),
                            (cancelled) =>
                              actor.self
                                .ask<typeof QueryReply.Type>(
                                  (replyTo) => ({
                                    _tag: "ContextQueryRequested",
                                    input,
                                    replyTo,
                                    cancelled,
                                  }),
                                  "100 seconds",
                                )
                                .pipe(
                                  Effect.flatMap((reply) =>
                                    reply._tag === "Success"
                                      ? Effect.succeed(reply.value)
                                      : Effect.fail(reply.error),
                                  ),
                                  Effect.catchTag("AskTimeoutError", () =>
                                    Effect.fail(
                                      new ContextQueryError({
                                        kind: "timeout",
                                        message: "Context query acknowledgement timed out",
                                      }),
                                    ),
                                  ),
                                ),
                            (cancelled) => Deferred.succeed(cancelled, undefined),
                          ),
                      )
                      .pipe(Effect.provideService(Scope.Scope, scope), Effect.orDie);
                  }
                }),
            };
          }),
        ),
        { contextDefinition: options.context },
      );
    },
  provide:
    <Out, E2, R2>(dependency: import("effect").Layer.Layer<Out, E2, R2>) =>
    <P, B, E, R>(
      definition: import("@aster/actor").Definition<P, B, E, R> & {
        readonly contextDefinition: ContextDefinition;
      },
    ) =>
      Object.assign(Actor.provide(dependency)(definition), {
        contextDefinition: definition.contextDefinition,
      }),
} as const;

export const spawnContextChild = <Command, Definition extends AnyActorDefinition>(
  context: ActorContext<Command>,
  relativePath: string,
  definition: Definition,
  options?: SpawnOptions,
): Effect.Effect<ActorRef<CommandOf<Definition>>, SpawnError, ServicesOf<Definition>> =>
  context.spawn(
    childActorName(relativePath),
    definition,
    contextSpawnOptions(
      typeof options?.metadata?.contextPath === "string"
        ? options.metadata.contextPath
        : childContextPath(contextPath(context), relativePath),
      options,
    ),
  );
