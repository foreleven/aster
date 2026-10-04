# agentmemory integration

This package owns the pinned upstream CLI lifecycle, config/environment translation, REST observation protocol, durable source provenance, and the implementation of core’s `MemoryBackend`. It has no Lark or Signal detection dependencies.

- `parseMemoryConfig` validates the `/memory` YAML entry and resolves its data directory.
- `memorySettings` reads the typed `/memory` Config declaration with config-relative path resolution.
- `AgentMemoryBackend.layer` provides `MemoryBackend`, owns the daemon/connection file and resolves credentials through the captured ConfigProvider.
- Core internally provides `MemoryRecall`/`ContextCaptureSink` and owns the Memory Actor. `AsterRuntime` installs it before source activation; the host supplies only `AgentMemoryBackend.layer`.
- `managedMemory` acquires the worker and engine in an Effect Scope; `launchMemory` exposes the same lifecycle for integration checks.
- `makeMemoryClient` captures a batch in one session, waits for each observation's compression, and ends the session. Recall follows upstream progressive disclosure: `search(query, { limit: 10 })` returns `{ mode: "compact", results }`; `expand(selectedIds)` returns `{ mode: "expanded", results, truncated }` with full details only for selected records.
- `openMemoryReader` owns scoped connection-file decoding and `makeMemoryReader`; `configuredMemoryReader` also resolves the module settings. Readers expose search and expansion with a read-only provenance database for the CLI. Both retain available durable Context paths. Consolidated memory hits resolve their original observation IDs to Context paths when expanded, without fetching their content during search. Expansion accepts observation IDs or `{ obsId, sessionId }` references and follows upstream's 20-record batch cap.
- Core’s `MemoryActor` acknowledges capture only after queue persistence and routes backend results through its mailbox with `pipeToSelf`. No Actor or capture retry policy is implemented in this adapter.

Core Context reactions decide capture eligibility. This package never subscribes to every Context or captures source activity by itself.

The pinned CLI uses global PID files. An Aster ownership lock and preflight prevent adopting other daemons. A generated engine template sets all ports and data paths, removes the template's extra exec worker, and binds the worker manager to loopback. iii 0.11.2 saves KV files periodically; the template uses a 100 ms interval, and graceful shutdown keeps the engine alive for several ticks after the worker saves its search index. Abrupt process or machine failure still has that upstream persistence window.

Aster retains the legacy memory agent identity `signals`, ownership lock, engine template, and provenance database filenames for compatibility with existing memories and running-process detection. These are storage identifiers, not the product name.

The core-facing `MemoryRecall` port is Effect-native. `makeMemoryRecall` converts backend search/expand promises to typed `MemoryRecallError` failures and forwards cancellation into the transport. The Promise client accepts `search(query, { signal })` and `expand(references, signal)`; each request combines that signal with its 15-second timeout and rechecks cancellation before continuing expansion fallbacks. `MemoryRecallError` is defined in core and re-exported here as the same class. Capture/drain retain their existing durable queue and shutdown behavior.

Memory Actor commits now pass the revision from the same snapshot used to merge pending/captured state. Restored unversioned state starts at revision zero; restart and capture acknowledgements preserve the persisted queue through the explicit Context commit boundary.

The `/memory` public view exposes readiness, retrieval mode and model identity. Pending capture payloads and internal capture receipts remain private. Both new captures and recovered pending batches are projected through the source owners' read policies before being sent to the memory backend; retained canonical source records are not rewritten.
