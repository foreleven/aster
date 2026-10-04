import { Context, Effect, Layer, Schema, Semaphore, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { ContextQueryError, ProcessEnvironment } from "@aster/core";
import { openCliFailure } from "./errors.js";

// Display only: execution still passes argv directly to the process without a shell.
const shellArgument = (value: string) =>
  /^[a-zA-Z0-9_./:=+-]+$/.test(value) ? value : "'" + value.replaceAll("'", "'\"'\"'") + "'";

export class OpenCli extends Context.Service<
  OpenCli,
  {
    readonly run: (argv: readonly string[]) => Effect.Effect<Schema.Json, ContextQueryError>;
  }
>()("apps/OpenCli") {
  static readonly layer = Layer.effect(
    OpenCli,
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const source = yield* ProcessEnvironment;
      const privateValues = Object.entries(source.values)
        .filter(
          ([key]) =>
            source.privateKeys.includes(key) || /(?:API_KEY|SECRET|PASSWORD|TOKEN)$/.test(key),
        )
        .flatMap(([, value]) => (value ? [value] : []));
      const env: NodeJS.ProcessEnv = { ...source.values, NO_COLOR: "1" };
      for (const key of Object.keys(env)) {
        if (
          source.privateKeys.includes(key) ||
          key.startsWith("ASTER_") ||
          /(?:API_KEY|SECRET|PASSWORD)$/.test(key)
        )
          delete env[key];
      }
      // One browser session is shared by both adapters. Cancelled queued calls release their wait.
      const gate = yield* Semaphore.make(1);
      return {
        run: Effect.fn("OpenCli.run")((argv: readonly string[]) =>
          Effect.scoped(
            Effect.gen(function* () {
              yield* Effect.logInfo({
                event: "apps.query.command",
                command: ["opencli", ...argv].map(shellArgument).join(" "),
              });
              const handle = yield* spawner.spawn(
                ChildProcess.make("opencli", argv, {
                  env,
                  extendEnv: false,
                  shell: false,
                  stdin: "ignore",
                  forceKillAfter: "2 seconds",
                }),
              );
              let bytes = 0;
              const stdout = handle.stdout.pipe(
                Stream.mapEffect((chunk) => {
                  bytes += chunk.byteLength;
                  return bytes <= 128 * 1024
                    ? Effect.succeed(chunk)
                    : Effect.fail(
                        new ContextQueryError({
                          kind: "failed",
                          message: "OpenCLI output exceeded 128 KiB; narrow the query",
                        }),
                      );
                }),
                Stream.decodeText(),
                Stream.mkString,
              );
              // Keep a bounded diagnostic prefix while draining the entire pipe to avoid child-process deadlock.
              const stderr = handle.stderr.pipe(
                Stream.decodeText(),
                Stream.runFold(
                  () => "",
                  (text, chunk) => text + chunk.slice(0, Math.max(0, 16 * 1024 - text.length)),
                ),
              );
              const [output, diagnostic, code] = yield* Effect.all(
                [stdout, stderr, handle.exitCode],
                { concurrency: "unbounded" },
              );
              if (code !== 0)
                return yield* openCliFailure(code, output, diagnostic, [
                  ...privateValues,
                  ...argv.slice(2).filter((value) => !value.startsWith("-") && value.length > 0),
                ]);
              return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Json))(
                output,
              ).pipe(
                Effect.mapError(
                  () =>
                    new ContextQueryError({
                      kind: "failed",
                      message: "OpenCLI returned invalid JSON",
                    }),
                ),
              );
            }),
          ).pipe(
            gate.withPermits(1),
            Effect.timeout("90 seconds"),
            Effect.catchTag("TimeoutError", () =>
              Effect.fail(
                new ContextQueryError({
                  kind: "timeout",
                  message: "OpenCLI query timed out after 90 seconds",
                }),
              ),
            ),
            Effect.catchTag("PlatformError", () =>
              Effect.fail(
                new ContextQueryError({
                  kind: "unavailable",
                  message:
                    "Cannot run OpenCLI; check the executable on PATH and local browser setup",
                }),
              ),
            ),
          ),
        ),
      };
    }),
  );
}
