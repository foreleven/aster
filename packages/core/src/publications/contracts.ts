import { WritebackRequest, WritebackAuthorization } from "@aster/api-contracts";
import { Context, Effect, Schema } from "effect";

export class ChannelWriteError extends Schema.TaggedError<ChannelWriteError>()(
  "ChannelWriteError",
  {
    outcome: Schema.Literals(["rejected", "unknown"]),
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

/** Concrete Channel adapters own credentials. Rejection proves no write was
 * accepted; transport failures after submission must be reported as unknown. */
export class ChannelWrites extends Context.Service<
  ChannelWrites,
  {
    readonly publish: (
      request: WritebackRequest,
      authorization: typeof WritebackAuthorization.Type,
    ) => Effect.Effect<{ readonly externalId: string }, ChannelWriteError>;
  }
>()("publications/ChannelWrites") {}
