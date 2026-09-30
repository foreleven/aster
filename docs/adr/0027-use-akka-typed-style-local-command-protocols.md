# Use Akka Typed-style local Command protocols

Each Context Type defines a closed TypeScript Command union and exposes an `ActorRef<Command>` that accepts only that protocol. Commands are transient and may carry local runtime values such as a reply actor reference, so they do not require Effect Schema. `ask` is implemented as an interaction pattern with a temporary `replyTo` and has no durable completion or restart recovery. Persisted Context Messages/events, exact state, and checkpoints do require Effect Schema for SQLite encoding and recovery.
