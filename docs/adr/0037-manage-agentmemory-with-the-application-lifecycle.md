# Manage agentmemory with the application lifecycle

Aster launches and owns a local agentmemory runtime by default, using a pinned project dependency and a non-interactive CLI invocation. It waits for readiness before starting integrations and gracefully stops its memory runtime on shutdown, preserving the data directory. This provides a single application startup while retaining the upstream memory service's process and storage boundaries, at the cost of managing its worker and iii engine lifecycle.
