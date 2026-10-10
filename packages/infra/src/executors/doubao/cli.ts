import { Data, Effect, Fiber, Redacted, Ref, Schema, Scope, Semaphore, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
export class DoubaoCliError extends Data.TaggedError("DoubaoCliError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}
export class DoubaoWaitTimeout extends Data.TaggedError("DoubaoWaitTimeout") {}
const Envelope = Schema.Record(Schema.String, Schema.Unknown);
export const makeDoubaoCli = Effect.fn("Doubao.cli")(function* (
  environment: Readonly<Record<string, string | undefined>>,
  executable: string = "doubao",
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const gate = yield* Semaphore.make(1);
  const owner = yield* Effect.scope;
  const closed = yield* Ref.make(false);
  yield* Scope.addFinalizer(owner, Ref.set(closed, true));
  const run = (args: readonly string[]) =>
    gate.withPermit(
      Effect.scoped(
        Effect.gen(function* () {
          if (yield* Ref.get(closed))
            return yield* new DoubaoCliError({ message: "Doubao CLI is closed" });
          const child = yield* spawner.spawn(
            ChildProcess.make(executable, ["--app", "work", ...args, "--json"], {
              env: environment,
              extendEnv: false,
              stdin: "ignore",
              forceKillAfter: "2 seconds",
            }),
          );
          let bytes = 0;
          const stdout = child.stdout.pipe(
            Stream.mapEffect((chunk) => {
              bytes += chunk.byteLength;
              return bytes <= 16 * 1024 * 1024
                ? Effect.succeed(chunk)
                : Effect.fail(new DoubaoCliError({ message: "Doubao output exceeded 16 MiB" }));
            }),
            Stream.decodeText(),
            Stream.mkString,
          );
          const [text, , code] = yield* Effect.all(
            [stdout, Stream.runDrain(child.stderr), child.exitCode],
            { concurrency: "unbounded" },
          );
          const value = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Envelope))(
            text,
          ).pipe(
            Effect.mapError(
              (cause) =>
                new DoubaoCliError({
                  message: "Doubao returned invalid JSON",
                  cause: Redacted.make(cause),
                }),
            ),
          );
          if (code !== 0 && typeof value.status !== "string")
            return yield* new DoubaoCliError({
              message: `Doubao command failed with exit code ${code}`,
            });
          return value;
        }),
      ).pipe(
        Effect.timeout(90000),
        Effect.catchTag(
          "TimeoutError",
          (cause): Effect.Effect<never, DoubaoWaitTimeout | DoubaoCliError> =>
            args[0] === "sessions" && args[1] === "wait"
              ? Effect.fail(new DoubaoWaitTimeout())
              : Effect.fail(
                  new DoubaoCliError({
                    message: "Doubao command timed out",
                    cause: Redacted.make(cause),
                  }),
                ),
        ),
        Effect.mapError((cause) =>
          cause instanceof DoubaoWaitTimeout || cause instanceof DoubaoCliError
            ? cause
            : new DoubaoCliError({
                message: "Doubao CLI transport failed",
                cause: Redacted.make(cause),
              }),
        ),
      ),
    );
  return (args: readonly string[]) =>
    Effect.gen(function* () {
      if (yield* Ref.get(closed))
        return yield* new DoubaoCliError({ message: "Doubao CLI is closed" });
      const fiber = yield* run(args).pipe(Effect.forkIn(owner));
      return yield* Fiber.join(fiber).pipe(Effect.onInterrupt(() => Fiber.interrupt(fiber)));
    });
});
export type DoubaoCommand = Effect.Success<ReturnType<typeof makeDoubaoCli>>;
