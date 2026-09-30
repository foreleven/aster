# Close the Actor behavior error channel

`receive` and `receiveSignal` return `Effect<void>` without a typed error channel. Since `tell` only confirms enqueueing, there is no caller waiting to handle an asynchronous Command failure. An Actor turns expected domain or operational failures into its own protocol, such as a reply or persisted Event; an uncaught failure enters parent supervision. This supersedes ADR-0021's proposal to expose typed Command errors from Context handlers.
