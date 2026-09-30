import { Effect } from "effect";

/** Keep handlers installed through scoped cleanup, including forwarded duplicate signals. */
export const waitForShutdown = Effect.acquireRelease(
  Effect.sync(() => {
    let resolve!: () => void;
    const ready = new Promise<void>((resume) => {
      resolve = resume;
    });
    const stop = () => resolve();
    process.on("SIGINT", stop);
    process.on("SIGTERM", stop);
    return { ready, stop };
  }),
  ({ stop }) =>
    Effect.sync(() => {
      process.off("SIGINT", stop);
      process.off("SIGTERM", stop);
    }),
).pipe(Effect.flatMap(({ ready }) => Effect.promise(() => ready)));
