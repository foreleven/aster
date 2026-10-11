# Split Agent Assembly from Agent Runtime

Status: proposed; implementation pending the core refactor.

The current `Core agent/runtime` package combines pi execution adapters, model resolution contracts, durable conversation handling, and callback lifecycle, while `@aster/core` owns Goal and Task tools and prompts. This makes the package boundary obscure: business Agent behavior is separated from the tools and instructions that define it, while the runtime adapter also carries application-specific seams.

Remove the standalone `Core agent/runtime` package. Keep Agent Assembly and the pi-agent-core/pi-durable execution runtime in Core, organized by domain Agent (for example `agent/goal/instructions.md`, `agent/goal/extensions.ts`, and `agent/task/...`). Infra supplies pi-ai's native Models, provider catalog, authentication and application configuration. Each Agent Assembly builds the runtime it needs through Core's shared runtime modules; the runtime remains reusable without becoming a second package boundary.

Each Assembly is self-contained by Agent type: its instructions, extensions, tools, input shaping, and result contract live together under `agent/goal/`, `agent/task/`, or `agent/screening/`. Each Assembly exposes an Effect-style `make` operation (for example `GoalAgent.make`) rather than a global AgentFactory. Shared tool-definition and runtime glue may be reused through explicit internal modules, but domain tools do not remain in a cross-domain `core/tools` bucket.

Long instructions remain Markdown assets under the Assembly directory. A shared Core loader resolves these assets during build or Layer acquisition; `make` receives the loaded instruction text and does not embed large templates in TypeScript.

An Assembly's `extensions.ts` describes only its runtime extensions and hooks. Ordinary and durable runtime adapters select the compatible pi hooks; business tools are defined once by the Assembly.

Effect Schema to pi-native tool adaptation remains shared runtime infrastructure under `agent/shared/define-tool.ts`. Goal and Task tools use it from their own Assembly directories. Integration-owned Assemblies, such as Lark summarization, remain in their integration context and reuse Core runtime/shared contracts rather than moving integration business behavior into Core.

Only genuinely cross-Assembly tool infrastructure belongs under `agent/shared/`: schema/tool adaptation, common result formatting, invocation boundaries, and runtime-facing helpers. Goal and Task business tools remain owned by their respective Assemblies.

Model configuration reuses pi-ai's native `Models`, provider catalog, and authentication capabilities. Aster does not maintain a second `ResolvedModel`/stream/API-key facade; infrastructure decodes application configuration and supplies the native model runtime to the Agent Runtime.

The existing Aster model configuration is allowed a breaking migration to pi-ai-native configuration. The application-level model alias remains the selection key, while provider, model, and authentication fields are decoded and registered by Infra without preserving the old `ModelConfig` facade.

The durable runtime is intentionally thin. Core keeps an `AgentSession` wrapper around pi-durable's native session/checkpoint/replay/recovery behavior and removes the current adapter-owned failure ledger, submission reconciliation, and duplicate state machine. Goal and Task retain their own business receipts and state transitions.

Goal and internal Task Assemblies use durable sessions. Goal Screening and integration-owned Lark summarization use ordinary runs. Session storage is injected through an `openStorage(owner)` capability; Infra/Local own JSONL or SQLite, paths, and leases, while Core never creates directories or reads local files.

Core owns and implements the Agent Runtime modules around pi-agent-core and pi-durable. Assemblies depend on these shared modules and the native pi-ai Models capability supplied by Infra; Core does not import Infra.

This preserves the domain boundary while making the construction of each Agent discoverable. It also lets model configuration reuse pi's provider and model capabilities without making Goal or Task code understand provider transport details.
