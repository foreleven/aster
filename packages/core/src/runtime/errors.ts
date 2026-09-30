import { Data } from "effect";

export class IntegrationError extends Data.TaggedError("IntegrationError")<{
  readonly integration: string;
  readonly message: string;
  readonly cause?: unknown;
}> {}

export class RuntimeConfigurationError extends Data.TaggedError("RuntimeConfigurationError")<{
  readonly message: string;
}> {}
