import { Context, Redacted } from "effect";

/** Captured launch environment, private to native process adapters. */
export class ProcessEnvironment extends Context.Service<
  ProcessEnvironment,
  {
    readonly values: Redacted.Redacted<Readonly<Record<string, string | undefined>>>;
    readonly privateKeys: readonly string[];
  }
>()("core/process/Environment") {}
