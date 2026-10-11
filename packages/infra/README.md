# Infrastructure

Concrete infrastructure capabilities for Aster. Local chooses platform implementations and independent Layers; core owns domain contracts, Actor orchestration, accepted state and business recovery. Infra and integrations are sibling packages without imports or re-exports between them. Core's Agent Runtime receives native pi-ai Models/provider/auth capabilities from Infra.

## Public capabilities

- `LocalConfig.layer`, `ConfigLocation`: captured YAML/dotenv/environment sources and launch locations, with Effect FileSystem/Path. No module reads live environment values.
- `StorageSettings.layer`, `withStorageLease`: resolve and exclusively own the shared data root. The SQLite lock's permanent inode remains the ownership identity; failed writer cleanup retains ownership until process exit.
- `ContextStorage.layer`: native Pi Context Documents under `context-sessions/`. Core owns owner validation, CAS and publication; the adapter owns atomic document persistence, validated recovery and fsync.
- `ConfiguredModels.layer`: decode native pi-ai model registrations and supply the provider catalog, alias lookup and authentication capability used by Core Agent Assemblies.
- `ExternalExecutors.layer`: scoped Codex and Doubao capabilities. Task workspaces default to `<root>/tasks`. Core freezes reviewed executor instructions; adapters submit those instructions without prepending policy again.
- `SystemOneTransport.layer`: injected Effect HTTP decision protocol, typed responses, bounded retry/timeout/cancellation and credential-safe logs. Business question builders apply output-language instructions.
- `AgentMemoryBackend.layer`: managed agentmemory process, transport, durable provenance and admitted capture lifetime. Default data lives under `<root>/memory`; explicit dataDir overrides it.
- `openMemoryReader`, `configuredMemoryReader`: read-only CLI access from an explicit or configuration-resolved connection file, without starting a daemon.

The package root exports these host capabilities explicitly. Native clients, protocol schemas, status decoders, renderer functions and resource factories remain private; tests import implementations relatively.

System One optionally accepts `config.system-one.proxy`, an HTTP/HTTPS proxy origin such as `http://127.0.0.1:7890` or an exact `${SYSTEM_ONE_PROXY}` reference from the captured environment. URL user/password credentials are supported and kept Redacted. HTTPS targets use CONNECT. The owning Layer releases its private dispatcher on shutdown; other transports retain their injected HTTP client. Without an explicit proxy, System One uses that injected client too. Settings are captured at acquisition and require a restart to change.

## Resource ownership

The host holds the root lease around the complete runtime and adapter graph. Runtime stops domain admission/producers and Actors before releasing adapters. Cancelled observation does not prove that an external submission or memory observation was cancelled; external submissions are not automatically retried after unknown outcomes.

Memory capture fibers belong to the backend's private work scope. Caller interruption stops observation, while admitted work remains available for drain. The adapter drains captures, stops worker/engine processes and closes SQLite resources before leases release. The upstream daemon's global control files retain a separate scoped lease; uncertain cleanup retains that lease too.

Codex owns a lazy app-server connection and correlated JSON-RPC requests. Protocol decoding is separate from execution and control mapping. Doubao owns serialized CLI commands and explicit native renderer approval/clarification responses. Unsupported follow-up remains a typed rejection. Native response scripts are self-contained because the external renderer has no Aster Effect runtime.

Pi callbacks, SQLite kernel ownership, native detached PID control and renderer code are explicit native boundaries. Ordinary HTTP, filesystem reads, CLI operations, delays and coordination use Effect services. Public model/transport errors preserve private causes as Redacted values.

Independent Goal screening audit files and their Store capability have been removed. Durable routing and Goal inputs remain authoritative, accompanied by structured operational logs; no complete calibration dataset is collected.

See [infra design](../../docs/infra-design-review.md), [configuration](../../docs/configuration-design.md) and [runtime](../../docs/runtime-design.md).
