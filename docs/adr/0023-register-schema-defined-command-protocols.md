---
status: superseded by ADR-0026; Command typing to be redesigned
---

# Register a Schema-defined Command protocol per Context Type

Every Context Type registers a closed tagged union of write Commands defined with Effect Schema, together with its handlers. Callers use a typed `ActorRef<Command>` for that Context Type; the actor runtime erases the concrete type only to store inbox rows, then selects the registered type and decodes the Command during activation and processing. Commands cannot contain arbitrary closures or unserializable runtime objects. This preserves plugin openness and durable recovery without a generic Cluster RPC payload.
