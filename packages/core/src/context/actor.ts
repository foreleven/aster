import { Effect, Schema } from "effect";
import { ContextRegistry } from "./registry.js";
import type { ContextDefinition } from "./definition.js";
import {
  Actor,
  type ActorBehavior,
  type ActorContext,
  type ActorRef,
  type AnyActorDefinition,
  type CommandOf,
  type RequireServices,
  type SpawnError,
  type SpawnOptions,
} from "@aster/actor";

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

/** Registration belongs to the Context lifecycle, before started and any mailbox Command. */
export const ContextActor = {
  Service:
    <Self, Services = never>() =>
    <CommandSchema extends Schema.Schema<any>>(
      key: string,
      options: { readonly command: CommandSchema; readonly context: ContextDefinition },
    ) => {
      const definition = Actor.Service<Self, Services | ContextRegistry>()(key, {
        command: options.command,
      });
      const of = definition.of;
      const withContextPath = (
        actor: ActorContext<Schema.Schema.Type<CommandSchema>, Services | ContextRegistry>,
      ): typeof actor => ({
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
      return Object.assign(definition, {
        context: options.context,
        of: (
          behavior: ActorBehavior<Schema.Schema.Type<CommandSchema>, Services | ContextRegistry>,
        ) =>
          of({
            ...behavior,
            receive: (command, actor) => behavior.receive(command, withContextPath(actor)),
            ...(behavior.receiveSignal
              ? {
                  receiveSignal: (signal, actor) =>
                    behavior.receiveSignal!(signal, withContextPath(actor)),
                }
              : {}),
            started: (actor) =>
              Effect.gen(function* () {
                const registry = yield* ContextRegistry;
                yield* registry.register(contextPath(actor), options.context);
                if (behavior.started) yield* behavior.started(withContextPath(actor));
              }),
          }),
      });
    },
} as const;

export const spawnContextChild = <Command, Services, Definition extends AnyActorDefinition>(
  context: ActorContext<Command, Services>,
  relativePath: string,
  definition: Definition & RequireServices<Definition, Services>,
  options?: SpawnOptions,
): Effect.Effect<ActorRef<CommandOf<Definition>>, SpawnError> =>
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
