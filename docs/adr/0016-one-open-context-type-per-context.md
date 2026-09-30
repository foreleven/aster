# Assign one open Context Type to each Context

Status: superseded by [0038](0038-register-root-actors-in-code.md). Actor implementations remain distinct; the string type tag and central catalog are removed.

Every `ContextEntity` has exactly one Context Type selected from an open registry. The entity provides the common actor shell—path identity, mailbox, persistence, exact-state boundary, and Message history—while the registered type defines its state schema, accepted Commands, transitions, and any standard interfaces it implements. Contexts do not assemble multiple Context Types. Shared Providers are separate dependencies and may supply several type implementations, such as Lark IM and Lark Mail Channel types, allowing shared clients and credentials without weakening per-Context invariants.
