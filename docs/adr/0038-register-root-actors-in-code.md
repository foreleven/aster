# Register root Actors in code and let them own configuration and children

Status: accepted. Supersedes the string Context type registry in [0016](0016-one-open-context-type-per-context.md) and the flat child configuration examples in earlier design notes.

The application starts explicitly registered root Actor implementations through the ActorSystem API. It supplies optional root configuration entries without traversing or interpreting their children. Each integration consumes and validates its own subtree, supplies defaults, and creates its own child Actors. A configuration entry is not required for an Actor to start if the implementation has suitable defaults. YAML does not select implementations by a `type` string.

Lark owns the `/lark` subtree, including `children./mail`, and creates the mail Actor at `/lark/mail`. The mail Actor discovers email children at `/lark/mail/me/{message_id}`. Root code neither registers these descendants nor sends initialization Commands to them. Configuration files supply parameters to registered code rather than define a global Actor inventory.

Public Context data contains only `path`, fixed `description`, object `state`, and ordered `messages`. An Actor registers its Context path with its public Schemas and private processing callbacks. The common processor uses these callbacks for memory selection and Signal evaluation eligibility; it has no Context type or path switch. These functions and Schema objects are never serialized into Agent snapshots or memory. Dynamic descriptions receive a fixed identity hint from the implementation during initialization.

The Actor module remains domain-neutral. Its service identifiers and TypeScript Command types continue to provide typed Actor APIs. Context implementations remain separate code definitions without requiring a second string identity in their data.
