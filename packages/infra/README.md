# Infrastructure

Concrete infrastructure adapters for Aster. The local host imports `@aster/infra` alongside `@aster/core` and `@aster/integrations`; core never imports concrete adapters. Infra and integrations are independent sibling packages.

This package owns:

- `LocalConfig.layer`: captured YAML, dotenv and process sources, Effect ConfigProvider, source locations and explicit child-process environment. `loadConfig` remains a standalone file-validation utility.
- `makeFileContextStore`, the `FileDurableContext` backend Layer, and the actor-store process lock.
- The TypeSafe-compatible System One HTTP client, behind core's decision interface.
- Infrastructure Layers (`FileDurableContext`, `FileGoalHistory`, `SystemOneClientLive`, `ExternalAgentsLive`); Goal runtime assembly and reasoning belong to core.
- Codex app-server execution, persistent thread/turn recovery, native approval replies and environment isolation.
- Doubao session/run status and waiting via CLI, native pending-control replies, and executor capability descriptions.

The agentmemory adapter lives in `agentmemory/` and provides core’s `MemoryBackend`. Core owns its Memory Actor, durable capture queue, retries, and runtime assembly. Lark and generic mail belong to `@aster/integrations`; Models is imported directly from `@aster/agent`.

External adapters receive the captured environment and `.env` path explicitly; they strip dotenv/service keys and `ASTER_*` variables from child environments. Internal reasoning no longer uses Codex CLI or an app CLI path. Captures, reasoning, extraction and delegation are injected into core through typed callbacks/interfaces; no core module reaches into this package's compiled output.

The local app retains CLI commands, HTTP/SSE and web serving, adapter selection and process signals. Core `AsterRuntime` owns startup order and domain shutdown. Infra does not start an application or managed memory merely by being imported.

Source is grouped by capability: `config/`, `storage/`, `system-one/`, `codex/`, `doubao/`, `pi/`, `agentmemory/`, and `process/`. Consumers use the package root exports rather than internal paths.

System One request failures emit the Effect error event `system-one.request.failed` with the model, question count and error message, independently of SDK log-level filtering. Logs omit credentials and request state; Goal screening retains its normalized input separately in the screening audit store.

Doubao native response supports local command allow/reject, native local `interaction.ask` input, and pre-tool safety confirmation. It verifies the request against the original run before calling the observed desktop protocol; it never substitutes ordinary chat messages for approval. Unsupported action types, changed module contracts and missing bindings fail explicitly, with the saved response retained in Delegation history. No globally installed CLI is modified. Native contracts were checked against Doubao Work 2.31.6 and CLI 0.12.0.

Execution factories return managed Effect adapters. `external-agent.ts` contains the Promise driver boundary: it maps transport failures to core's `ExternalAgentError`, forwards Fiber cancellation as an AbortSignal and performs no submission retry. Only infrastructure sees the additional `close(): Effect<void>` capability; `ExternalAgentsLive.layer` acquires and releases the adapters after domain Actors stop. The host continues to provide this single Layer together with configuration/environment services. Codex JSON-RPC and Doubao CLI/native transports remain Promise implementations inside their adapters. Cancelling a local wait does not mean the external run was cancelled.

`FileDurableContext.layer` provides core's `DurableContext` through `LocalDurableContext` and the file driver. The host selects this authoritative backend; domain Actors use ContextRegistry and never open files. File commits fsync both data and directory entries, validate pending/state files before recovery writes, and replay a complete pending record after partial append or rename failure. The existing synchronous driver is a native compatibility boundary, not the domain commit API. One host process still holds the actor-store ownership lock for the entire runtime lifetime.

## Pi Context storage

`PiDurableContext.layer({ directory, shardId })` is an injectable, model-free backend. It owns one scoped Pi Session and opens JSONL storage with `fsync: true`. A single `Session.commit` writes `app.aster.context.commit`, the conversation snapshot document, and the session index. The index records mapping version, shard, Context path, conversation ID, revision, and last commit entry ID. Receipts/outbox state and ordered messages are part of the same snapshot. Full snapshot entries preserve replacement/compaction, and contain no model frames.

Local and Pi share the canonical commit/recovery kernel. Pi additionally verifies stored revisions and validates index/document/entry agreement when opening. Any uncertain commit fences writes and recovery closes/reopens the Session before reading authoritative data. Cancellation drains admitted persistence before closing storage. The directory lock outlives the Session, including reopen. Pi uses a kernel lock that remains exclusive until the owning process closes it or exits; stale diagnostic metadata does not prevent recovery. Distributed fencing is not implemented. `make({ shardId, openStorage })` supports injected drivers; callers must hold exclusive ownership and return a fresh handle on each open.

The local host selects ConfiguredDurableInfrastructure, which defaults to Local and can route configured path prefixes to Pi. The model-free storage adapter does not open a Harness or execute tools. When Pi execution is configured on the same store, the combined backend supplies both capabilities.

## Pi external executor

Set `agents.pi.model` to a configured model name to register the `pi` ExternalAgent; Signals select it with `agent: pi`. Optional `agents.pi.storageDirectory` defaults to `~/.aster/executions/pi`. Task execution input construction supplies a self-contained Task, and Delegation passes its stable public path as the submission request ID. The adapter submits the frozen Task prompt, returns Pi conversation/task IDs with owner/request mapping, and reads persisted status/results. Delegation retains business history and acknowledges completion after persisting it. Public Contexts contain no native transcript or model credentials.

The initial executor performs read-only analysis through `SandboxManager` and the native Pi read tool. Policy `aster.prepared-evidence.v1` exposes immutable `/task/input.md` and `/task/instructions.md` from the admitted Task. This is a virtual evidence namespace: it never delegates to the host filesystem. All file mutations, host path reads, process execution, network access and credential access are denied. Changing cwd or using absolute/traversal paths cannot grant host access. Credentials remain inside the model provider adapter. The environment policy is persisted with admission and pending recovery rejects a changed policy before resuming work.

This capability boundary does not run untrusted JavaScript or provide an operating-system process sandbox. Installed tool implementations remain trusted host code; the production catalogue installs only the read tool. Enabling process execution or external writes requires a separate enforcing backend and explicit action policy. Run approval cannot expand this environment. There is no interactive Pi approval channel: `respond` rejects instead of fabricating acknowledgement. The executor directory uses fsync and the same scoped kernel lease as PiDurableContext. Standalone executor configuration still uses its own directory; `PiDurableBackend` provides combined Context/execution ownership for hosts that select the shared backend.

## Shared Pi backend

`PiDurableBackend.make({ model, directory, shardId })` acquires one lease and one PiDurableAgentRuntime, attaches PiDurableContext through `fromRuntime`, and returns the core Context and ExternalAgent capabilities. `layer` provides `DurableContext` and an `ExternalAgents` registry containing `pi`; it is an explicit host composition and does not merge unrelated executor registries. No second Session or storage handle is opened for Contexts.

Context and execution entries/documents share Pi's mutation line. Context reads load only Aster's Context index, leaving task transcript frames private. A failed Context commit reconciles through the runtime owner: close/join the previous Harness, reopen, validate stored Context mapping and restore the canonical revision. Execution-side recovery uses the same owner and leaves Context records available. Long-running execution observers do not block new Context commits. Retired observers report an error; the durable execution handle remains usable with the new owner.

The local host uses `ConfiguredDurableInfrastructure.layer`. It provides routed DurableContext and ExternalAgents; `config.durable.pi` plus `agents.pi.model` select one shared backend. Without an execution model, Pi Context storage stays model-free. If `agents.pi.storageDirectory` is supplied in shared mode, it must resolve to the same directory.

## Offline storage routing and rollback

`config.durable.root` defaults to `~/.aster` and contains `actors`, `goals`, `evaluations` and private `storage-authority` journals. Relative paths resolve beside the configuration file. `config.durable.pi` enables a Pi store (default `<root>/pi`, owner `aster-executions`); `routes` select `local` or `pi` by longest segment prefix. Unmatched paths stay Local. `/goals` includes `/goals/demo`, but not `/goals-archive`. Live commits and observations use only the selected store.

The host validates both stores before accepting writes. Missing selected snapshots, newer unselected snapshots, and unequal snapshots at the same revision refuse startup. Configured routes must match the persisted authority. An empty installation can publish its initial authority on startup; existing records require offline migration before first Pi activation.

Stop Aster, set the desired routes, and run `pnpm aster storage migrate --config FILE`. This command holds the same root lock as startup and the Pi directory lease. It opens no model, Harness or integration. It validates all copies before writing, imports complete snapshots without advancing their revision, closes/reopens Pi and rereads Local to validate disk replay, then publishes authority with a generation audit. Unversioned legacy records become revision zero. Retrying a partially copied plan reconciles equal copies without duplicate entries. Unknown authority publication must be inspected or retried with the same target; never assume failure means the old routes remain active.

Rollback uses the same command with reverse routes (or `routes: []` for Local). It copies the latest authoritative records, including writes made after activation. Keep the existing Pi directory and owner in configuration even after returning all routes to Local. This preserves comparison evidence and execution identities. Storage relocation, ownerless Personal/Goal conversation migration and execution-session migration are outside this command. Existing runtime data is never migrated by startup.

Pi directory acquisition uses the same scoped kernel lease as Goal/Personal execution in `@aster/agent`. Context-only and shared execution recovery retain that lease across close/reopen, and quarantine it when SDK close is uncertain. Competing owners fail before Pi reads/writes. Process death releases the kernel lock without deleting a PID marker; the permanent SQLite lock file must not be removed. This is local-filesystem ownership, not distributed failover. Existing offline archive code uses this same lease when opening Pi, but no migration is required or performed by the new lease protocol.
