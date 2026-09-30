# Isolate a domain-neutral actor module

Build the Actor Model runtime as an independent module with no knowledge of Contexts, Channels, Signals, or their path conventions. It owns typed actor references, actor paths and hierarchy, mailboxes, supervision, ReceiveTimeout, lifecycle scopes, interaction patterns, and persistence extension points. The Context layer later maps domain Context paths onto actor paths and supplies concrete actor implementations. This adds an abstraction boundary now so the runtime can be tested and reasoned about without domain behavior.
