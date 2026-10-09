import {
  Actor,
  ReplyTo,
  type ActorBehavior,
  type ActorContext,
  type ActorRef,
  type AnyActorDefinition,
  type CommandOf,
  type Protocol,
  type MailboxOf,
  type ServicesOf,
  type SpawnError,
  type SpawnOptions,
} from "@aster/actor";
import { Effect, Match, Predicate, Ref, Schema, Scope } from "effect";
import { randomUUID } from "node:crypto";
import { ContextQueryError } from "./contracts.js";
import { QueryReply, isQueryCommand, type QueryCommand } from "./queries/protocol.js";
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

interface ContextBehavior<
  C extends readonly Schema.Top[],
  I extends Schema.Top,
  H,
> extends ActorBehavior<MailboxOf<C, I>, H> {
  readonly description?: string;
}

/** Context ownership and public capability registration follow the Behavior scope. */
export const ContextActor = {
  define:
    <const C extends readonly Schema.Top[], I extends Schema.Top = typeof Schema.Never>(
      key: string,
      options: Protocol<C, I>,
    ) =>
    <E, R, H = never>(
      acquire:
        | Effect.Effect<ContextBehavior<C, I, H>, E, R>
        | ((actor: ActorContext<MailboxOf<C, I>>) => Effect.Effect<ContextBehavior<C, I, H>, E, R>),
    ) => {
      const contracts = options.commands.filter(isQueryCommand);
      return Actor.define(
        key,
        options,
      )((owner) =>
        Effect.gen(function* () {
          const registry = yield* ContextRegistry;
          const scope = yield* Effect.scope;
          // The runtime filter and conditional requirement use the same descriptor marker.
          // Widened schema arrays conservatively retain the query-registry requirement.
          const routes = yield* (contracts.length ? ContextQueries : Effect.void) as Effect.Effect<
            ContextQueries["Service"] | undefined,
            never,
            QueryCommand extends C[number]
              ? ContextQueries
              : Extract<C[number], QueryCommand> extends never
                ? never
                : ContextQueries
          >;
          type Owner = ActorContext<MailboxOf<C, I>>;
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
          const behavior = yield* typeof acquire === "function"
            ? acquire(withContextPath(owner))
            : acquire;
          const pending = yield* Ref.make(
            new Map<
              string,
              {
                replyTo: ReplyTo<typeof QueryReply.Type>;
                contract: QueryCommand;
              }
            >(),
          );
          const retire = (id: string) =>
            Ref.update(pending, (current) => {
              const next = new Map(current);
              next.delete(id);
              return next;
            });
          yield* Effect.addFinalizer(() =>
            Ref.get(pending).pipe(
              Effect.flatMap((requests) =>
                Effect.forEach(
                  requests.values(),
                  ({ replyTo, contract }) => {
                    const error = new ContextQueryError({
                      kind: "unavailable",
                      message: "Context query owner stopped",
                    });
                    // Local replies must stay within the command's declared error protocol.
                    // Remote queries also observe retirement through their route Scope.
                    return Schema.is(contract.errorSchema)(error)
                      ? replyTo.tell({
                          _tag: "Failure",
                          error,
                        })
                      : Effect.void;
                  },
                  { discard: true },
                ),
              ),
            ),
          );
          return {
            receive: (command, actor) =>
              Effect.gen(function* () {
                const contract = contracts.find(
                  (candidate) =>
                    Predicate.hasProperty(command, "_tag") && command._tag === candidate._tag,
                );
                if (!contract) return yield* behavior.receive(command, withContextPath(actor));
                const original = yield* Schema.decodeUnknownEffect(
                  Schema.Struct({ replyTo: ReplyTo<typeof QueryReply.Type>() }),
                )(command).pipe(Effect.orDie);
                if (original.replyTo.scope?.state._tag === "Closed") return;
                // Correlation belongs to an invocation, not a possibly shared reply Actor.
                const id = randomUUID();
                yield* Ref.update(pending, (current) =>
                  new Map(current).set(id, {
                    replyTo: original.replyTo,
                    contract,
                  }),
                );
                if (original.replyTo.scope)
                  yield* Scope.addFinalizer(original.replyTo.scope, retire(id));
                const replyTo: ReplyTo<typeof QueryReply.Type> = {
                  path: original.replyTo.path,
                  incarnation: original.replyTo.incarnation,
                  awaitStarted: original.replyTo.awaitStarted,
                  scope: original.replyTo.scope,
                  ask: (makeCommand, options) => original.replyTo.ask(makeCommand, options),
                  tell: (result) =>
                    Ref.modify(pending, (current) => {
                      const next = new Map(current);
                      const active = next.delete(id);
                      return [active, next] as const;
                    }).pipe(
                      Effect.flatMap((active) =>
                        active ? original.replyTo.tell(result) : Effect.void,
                      ),
                    ),
                };
                yield* Schema.decodeUnknownEffect(contract)(
                  Object.assign({}, command, { replyTo }),
                ).pipe(
                  Effect.matchEffect({
                    onFailure: () => {
                      const error = new ContextQueryError({
                        kind: "invalid-input",
                        message: "Invalid command arguments",
                      });
                      return Schema.is(contract.errorSchema)(error)
                        ? replyTo.tell({
                            _tag: "Failure",
                            error,
                          })
                        : Effect.die(error);
                    },
                    // The decoded member of the declared protocol restores the erased catalogue type.
                    onSuccess: (decoded) =>
                      behavior.receive(decoded as MailboxOf<C, I>, withContextPath(actor)),
                  }),
                );
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
                if (behavior.started) yield* behavior.started(withContextPath(actor));
                if (routes) {
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
                              success: contract.successSchema,
                              error: contract.errorSchema,
                              text: (value: unknown) => contract.text(value),
                            },
                          ]),
                        ),
                      },
                      (input) =>
                        actor.self
                          .ask<typeof QueryReply.Type>(
                            (replyTo) =>
                              ({ ...input.args, _tag: input.command, replyTo }) as MailboxOf<C, I>,
                            { timeout: "100 seconds", scope },
                          )
                          .pipe(
                            Effect.catchTag("AskTimeoutError", () =>
                              Effect.fail(
                                new ContextQueryError({
                                  kind: "timeout",
                                  message: "Context query acknowledgement timed out",
                                }),
                              ),
                            ),
                            Effect.flatMap((reply) =>
                              Match.value(reply).pipe(
                                Match.tag("Success", ({ value }) => Effect.succeed(value)),
                                Match.tag("Failure", ({ error }) => Effect.fail(error)),
                                Match.exhaustive,
                              ),
                            ),
                          ),
                    )
                    .pipe(Effect.provideService(Scope.Scope, scope), Effect.orDie);
                }
              }),
          };
        }),
      );
    },
  provide: Actor.provide,
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
