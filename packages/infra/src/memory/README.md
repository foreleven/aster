# Managed agentmemory backend

The Layer provides core MemoryBackend; core owns capture eligibility, its durable pending queue, retry policy and business receipts. This adapter owns the pinned agentmemory 0.9.29 / iii 0.11.2 protocol, process resources and durable provenance.

Settings resolve once. Data defaults to the configured durable root's `memory/` directory, with explicit dataDir overrides. Environment translation masks upstream provider auto-detection and supplies only the selected provider/model/endpoint credentials. Credentials remain Redacted until process or HTTP invocation.

`daemon.ts` owns a scoped upstream global lease, generated worker template, native child/engine identities, readiness and joined shutdown. The worker template removes the unrelated execution worker and configures the state persistence interval at 100 ms. Because iii has no shutdown flush, the engine remains alive for several ticks after the worker exits. This does not remove upstream's abrupt-crash persistence window.

`capture.ts` submits session/start and observe requests, persists source provenance, waits for observation processing and then ends the session. Admitted fibers outlive caller observation and are drained before daemon/provenance release. Errors retain the durable core queue for business recovery; the adapter adds no second durable queue or detached Promise registry.

`client.ts` implements cancellable progressive recall with injected Effect HTTP. Search returns compact candidates; expansion retrieves selected IDs only, caps upstream batches at 20 and resolves consolidated-memory fallback sequentially. `protocol.ts` validates external data before use. `provenance.ts` owns the private SQLite observation/session-to-Context mapping needed when upstream narratives omit original source identity.

`reader.ts` acquires read-only provenance and recall capabilities from a connection file. It never starts or adopts a daemon. The connection file is private and is published only after readiness, then removed on release. CLI and backend readers share the same HTTP and source-mapping implementation.

Native process identification and SQLite operations are narrow transport/storage boundaries. All surrounding lifecycle, polling, retries, filesystem access and synchronization are Effects. Tests use fake HTTP/process services and temporary native databases; validation never invokes a real memory model.
