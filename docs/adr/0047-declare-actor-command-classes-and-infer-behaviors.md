# Declare Actor command classes and infer Behaviors

Status: accepted.

Actor implementations previously repeated their mailbox unions, service requirements, query catalogue metadata and registration code. The repeated declarations could disagree, and public Actor references exposed internal completion messages.

Use `Command.Class` for application commands. Each class owns its tag, payload and response contract. Preserve explicit reply references and existing durable admission responses. A list such as `commands: [Search]` determines the public message union without another name mapping. Internal completion messages and polling messages remain in an optional internal schema; only the Behavior and its self reference accept them.

Declare the public Command tuple and internal Schema independently and pass them directly to the Actor definition. Derive a complete mailbox type with `MailboxOf<typeof Commands, typeof Internal>` when a helper needs it. Do not build a combined runtime Schema solely to extract its cases and reconstruct the internal protocol at the Actor definition.

Use function definitions, `Actor.define(key, protocol)(acquire)` and `PersistentActor.define(key, protocol)(acquire)`. Supplying the protocol first preserves contextual inference for generator-returned handlers. Infer service requirements from acquisition and handler Effects, including child Actor acquisition. Capture handler services in the Behavior scope. Keep an explicit provision operation for dependencies satisfied inside a definition.

Core specializes query commands with `ContextCommand.Class`. Its metadata provides the discoverable command name, argument schema and description. `ContextActor.define` registers those routes after initialization, dispatches decoded command instances through the owner, and unregisters on stop or restart. Its query callback receives the declared query-command union. The external Context query envelope stays compatible. Plain local commands are not automatically exposed through Agent query tools.

Automatic registration belongs to core; `packages/actor` remains independent of Contexts, Goals and integrations. Query work is scoped and cancellable, and mailbox completion messages retain supervision semantics. Existing domain persistence, generation checks and acknowledgement ordering remain in their owning Actors.

The migration covers all production Actor definitions, their command declarations, and the runtime fixtures. Plain Schema commands remain supported for fixtures and internal protocols. Duplicate public tags and public/internal collisions are rejected during acquisition.
