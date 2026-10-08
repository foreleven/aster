# Infrastructure

Concrete infrastructure adapters for Aster. The local host imports `@aster/infra` alongside `@aster/core` and `@aster/integrations`; core never imports concrete adapters. Infra and integrations are independent sibling packages.

This package owns:

- `LocalConfig.layer`: captured YAML, dotenv and process sources, Effect ConfigProvider, source locations and explicit child-process environment. Modules validate their own settings through this provider.
- `makeFileContextStore`, the file persistence adapter and the actor-store process lock.
- The TypeSafe-compatible System One HTTP client, behind core's decision interface.
- Infrastructure Layers (`ConfiguredDurableInfrastructure`, `FileGoalScreening`, `SystemOneClientLive`); Goal runtime assembly and reasoning belong to core.
- Codex app-server execution, persistent thread/turn recovery, native approval replies and environment isolation.
- Doubao session/run status and waiting via CLI, native pending-control replies, and executor capability descriptions.

The agentmemory adapter lives in `agentmemory/` and provides core’s `MemoryBackend`. Core owns its Memory Actor, durable capture queue, retries, and runtime assembly. Lark and generic mail belong to `@aster/integrations`; Models is imported directly from `@aster/agent`.

External adapters receive explicit child environments filtered once from captured values and private key names. Dotenv/service keys and `ASTER_*` variables are removed without rereading files. Doubao CLI and native response use the same captured endpoint. Internal reasoning no longer uses Codex CLI or an app CLI path. Captures, reasoning, extraction and delegation are injected into core through typed callbacks/interfaces; no core module reaches into this package's compiled output.

The local app retains CLI commands, HTTP/SSE and web serving, adapter selection and process signals. Core `AsterRuntime` owns startup order and domain shutdown. Infra does not start an application or managed memory merely by being imported.

Source is grouped by capability: `config/`, `storage/`, `system-one/`, `codex/`, `doubao/`, `agentmemory/`, and `process/`. Consumers use the package root exports rather than internal paths.

System One request failures emit the Effect error event `system-one.request.failed` with the model, question count and error message, independently of SDK log-level filtering. Logs omit credentials and request state; Goal screening retains its normalized input separately in the screening audit store.

Doubao native response supports local command allow/reject, native local `interaction.ask` input, and pre-tool safety confirmation. It verifies the request against the original run before calling the observed desktop protocol; it never substitutes ordinary chat messages for approval. Unsupported action types, changed module contracts and missing bindings fail explicitly, with the saved response retained in Delegation history. No globally installed CLI is modified. Native contracts were checked against Doubao Work 2.31.6 and CLI 0.12.0.

Execution factories return managed Effect adapters. `external-agent.ts` contains the Promise driver boundary: it maps transport failures to core's `ExternalAgentError`, forwards Fiber cancellation as an AbortSignal and performs no submission retry. Only infrastructure sees the additional `close(): Effect<void>` capability; `makeExternalAgents` acquires and releases adapters inside `ConfiguredDurableInfrastructure.layer`, after domain Actors stop. The host provides this Layer together with configuration/environment services. Codex JSON-RPC and Doubao CLI/native transports remain Promise implementations inside their adapters. Cancelling a local wait does not mean the external run was cancelled.

`ConfiguredDurableInfrastructure.layer` selects core's `DurableContext`; `LocalDurableContext.fromStore` adapts the native file driver to core's persistence contract. The host selects this authoritative backend; domain Actors use ContextRegistry and never open files. File commits fsync both data and directory entries, validate pending/state files before recovery writes, and replay a complete pending record after partial append or rename failure. The existing synchronous driver is a native I/O boundary, not the domain commit API. One host process still holds the actor-store ownership lock for the entire runtime lifetime.

## Local Context storage

`ConfiguredDurableInfrastructure.layer` supplies Local Context persistence and the Codex/Doubao external adapters. `config.durable.root` defaults to `~/.aster`; relative paths resolve beside the configuration file. Context snapshots live in `actors/` and screening journals in `evaluations/`. Pi execution and durable Agent conversations belong to `@aster/agent`; infra has no Pi storage backend, routing table or migration command.

The root store lock uses a SQLite exclusive transaction on permanent `.actors-lock.sqlite`. `actors.pid` is diagnostic only. Startup never unlinks a lock to recover ownership; process death releases it. Keep the root lock through adapter cleanup, including interrupted memory startup, before allowing another writer. The host uses `withActorStoreLock` to retain ownership until process exit if runtime shutdown ends with a defect. Contexts, Agent conversations and screening journals share this resolved root.
