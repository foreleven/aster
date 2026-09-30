# Passivate Context actors with ReceiveTimeout

The local actor runtime supports ReceiveTimeout. When an actor has received no Commands for its implementation-defined duration, has no Command in progress, its in-memory mailbox is empty, and it has no active children, it stops its scoped fiber, releases runtime resources, and is removed from the active hierarchy. Its Context path and durable state remain in SQLite, and the next Command addressed to that path activates a fresh actor and restores its state. Actor removal and concurrent delivery must be coordinated so a Command cannot be sent to a stopped instance.

Each registered Context Type decides in code whether to enable ReceiveTimeout and which duration to use. It is not user-editable Context configuration.

A Context implementation that holds an enabled Channel runtime normally does not enable ReceiveTimeout: it must remain alive to poll or listen for downstream data. The Channel sends that data to child Context actors, whose implementations may enable ReceiveTimeout and passivate normally.
