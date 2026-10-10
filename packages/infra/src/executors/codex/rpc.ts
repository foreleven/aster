import {
  Data,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Option,
  Queue,
  Ref,
  Scope,
  Schema,
  Semaphore,
  Stream,
  Redacted,
} from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { Frame } from "./protocol.js";
export class CodexRpcError extends Data.TaggedError("CodexRpcError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}
export const makeCodexRpc = Effect.fn("Codex.rpc")(function* (
  environment: Readonly<Record<string, string | undefined>>,
  executable: string = "codex",
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const closed = yield* Ref.make(false);
  const transportScope = yield* Effect.acquireRelease(Scope.make(), (scope, exit) =>
    Ref.set(closed, true).pipe(Effect.andThen(Scope.close(scope, exit))),
  );
  const gate = yield* Semaphore.make(1);
  const sendGate = yield* Semaphore.make(1);
  const events = yield* Queue.unbounded<Frame | { readonly method: "disconnected" }>();
  const initialization = yield* Ref.make<
    Option.Option<Fiber.Fiber<ChildProcessSpawner.ChildProcessHandle, CodexRpcError>>
  >(Option.none());
  const pending = new Map<number, Deferred.Deferred<unknown, CodexRpcError>>();
  let sequence = 0;
  let handle: ChildProcessSpawner.ChildProcessHandle | undefined;
  const failure = (cause: unknown) =>
    new CodexRpcError({
      message: "Codex RPC transport failed; external outcome may be unknown",
      cause: Redacted.make(cause),
    });
  const write = (child: ChildProcessSpawner.ChildProcessHandle, packet: object) =>
    sendGate
      .withPermit(
        Stream.make(new TextEncoder().encode(JSON.stringify(packet) + "\n")).pipe(
          Stream.run(child.stdin),
        ),
      )
      .pipe(Effect.mapError(failure));
  const callDirect = Effect.fn("Codex.rpcCall")(function* (
    child: ChildProcessSpawner.ChildProcessHandle,
    method: string,
    params: object,
  ) {
    const id = ++sequence;
    const reply = yield* Deferred.make<unknown, CodexRpcError>();
    pending.set(id, reply);
    return yield* write(child, { id, method, params }).pipe(
      Effect.andThen(Deferred.await(reply)),
      Effect.timeout(60000),
      Effect.mapError(failure),
      Effect.ensuring(Effect.sync(() => pending.delete(id))),
    );
  });
  const open = Effect.gen(function* () {
    const child = yield* spawner.spawn(
      ChildProcess.make(executable, ["app-server"], {
        env: environment,
        extendEnv: false,
        stdin: { stream: "pipe", endOnDone: false },
        forceKillAfter: "2 seconds",
      }),
    );
    handle = child;
    const disconnected = Effect.gen(function* () {
      const error = new CodexRpcError({ message: "Codex app-server disconnected" });
      for (const reply of pending.values()) yield* Deferred.fail(reply, error);
      pending.clear();
      yield* Ref.set(initialization, Option.none());
      yield* Queue.offer(events, { method: "disconnected" });
    });
    const consume = child.stdout.pipe(
      Stream.decodeText(),
      Stream.splitLines,
      Stream.runForEach((line) =>
        Effect.gen(function* () {
          const message = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Frame))(
            line,
          ).pipe(Effect.mapError(failure));
          if (typeof message.id === "number" && message.method === undefined) {
            const reply = pending.get(message.id);
            if (reply) {
              pending.delete(message.id);
              if (message.error)
                yield* Deferred.fail(
                  reply,
                  new CodexRpcError({ message: "Codex request rejected" }),
                );
              else yield* Deferred.succeed(reply, message.result);
            }
          } else yield* Queue.offer(events, message);
        }),
      ),
      Effect.ensuring(disconnected),
    );
    yield* consume.pipe(Effect.forkIn(transportScope));
    yield* child.stderr.pipe(Stream.runDrain, Effect.forkIn(transportScope));
    yield* callDirect(child, "initialize", {
      clientInfo: { name: "aster", version: "0.0.0" },
      capabilities: { experimentalApi: true },
    });
    yield* write(child, { method: "initialized", params: {} });
    return child;
  }).pipe(Effect.mapError(failure), Scope.provide(transportScope));
  const ensure = gate
    .withPermit(
      Effect.gen(function* () {
        if (yield* Ref.get(closed))
          return yield* new CodexRpcError({ message: "Codex RPC transport is closed" });
        const current = yield* Ref.get(initialization);
        if (Option.isSome(current)) return current.value;
        const fiber = yield* open.pipe(Effect.forkIn(transportScope));
        yield* Ref.set(initialization, Option.some(fiber));
        return fiber;
      }),
    )
    .pipe(Effect.flatMap(Fiber.join));
  return {
    call: (method: string, params: object) =>
      Effect.yieldNow
        .pipe(Effect.andThen(ensure))
        .pipe(Effect.flatMap((child) => callDirect(child, method, params))),
    reply: (id: string | number, result: object) =>
      ensure.pipe(Effect.flatMap((child) => write(child, { id, result }))),
    events: Stream.fromQueue(events),
    processId: Effect.sync(() => handle?.pid),
    close: Ref.set(closed, true).pipe(Effect.andThen(Scope.close(transportScope, Exit.void))),
  };
});
