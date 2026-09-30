# Integrations

Concrete infrastructure adapters and local integration composition for Aster. The app imports this package alongside `@aster/core`; core never imports integrations.

This package owns:

- `LocalConfig.layer`: captured YAML, dotenv and process sources, Effect ConfigProvider, source locations and explicit child-process environment. `loadConfig` remains a standalone file-validation utility.
- `makeFileContextStore` and the actor-store process lock.
- The TypeSafe-compatible System One HTTP client, behind core's decision interface.
- Infrastructure Layers (`FileContextStore`, `FileGoalHistory`, `SystemOneClientLive`, `ExternalAgentsLive`); Goal runtime assembly and reasoning belong to core.
- Codex app-server execution, persistent thread/turn recovery, native approval replies and environment isolation.
- Doubao session/run status and waiting via CLI, native pending-control replies, and executor capability descriptions.

Lark and memory keep their own packages and implementations. They are exported here for application assembly, alongside `Models`. Integrations depends on these implementations; they depend on core's contracts, never on this composition package.

External adapters receive the captured environment and `.env` path explicitly; they strip dotenv/service keys and `ASTER_*` variables from child environments. Internal reasoning no longer uses Codex CLI or an app CLI path. Captures, reasoning, extraction and delegation are injected into core through typed callbacks/interfaces; no core module reaches into this package's compiled output.

The local app retains CLI commands, HTTP/SSE and web serving, adapter selection and process signals. Core `AsterRuntime` owns startup order and domain shutdown. Integrations does not start an application or managed memory merely by being imported.

Source is grouped by capability: `config/, storage/, system-one/, codex/, doubao/, process/`. Consumers use the package root exports rather than internal paths.

Doubao native response supports local command allow/reject, native local `interaction.ask` input, and pre-tool safety confirmation. It verifies the request against the original run before calling the observed desktop protocol; it never substitutes ordinary chat messages for approval. Unsupported action types, changed module contracts and missing bindings fail explicitly, with the saved response retained in Delegation history. No globally installed CLI is modified. Native contracts were checked against Doubao Work 2.31.6 and CLI 0.12.0.

Execution factories return managed Effect adapters. `external-agent.ts` contains the Promise driver boundary: it maps transport failures to core's `ExternalAgentError`, forwards Fiber cancellation as an AbortSignal and performs no submission retry. Only infrastructure sees the additional `close(): Effect<void>` capability; `ExternalAgentsLive.layer` acquires and releases the adapters after domain Actors stop. The host continues to provide this single Layer together with configuration/environment services. Codex JSON-RPC and Doubao CLI/native transports remain Promise implementations inside their adapters. Cancelling a local wait does not mean the external run was cancelled.
