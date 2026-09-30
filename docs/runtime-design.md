# Aster runtime, integrations and configuration

Status: implemented. See [ADR 0040](adr/0040-compose-aster-runtime-with-effect-layers.md). API names below describe responsibilities; exported signatures are kept with their implementations.

## Ownership

| Owner                   | Responsibilities                                                                                                       |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| local                   | CLI arguments, configuration locations, adapter selection, process signals, HTTP/SSE and static assets                 |
| core runtime            | shared Layer graph, domain roots, integration installation/activation, subscriptions, readiness coordination, shutdown |
| core domain modules     | Context reactions, Signal eligibility, Goal operations/history, internal reasoning and Task preparation                |
| integrations            | private settings and clients, root/child Actors, initial readiness and cleanup                                         |
| infrastructure adapters | file stores, model and decision transport, memory backend and external executors                                       |
| ActorSystem             | generic typed Actors, mailboxes, supervision and scoped Actor services                                                 |

Local must not select Signal candidates, inspect Goal state, construct mailbox Commands for HTTP, inspect integration child configuration, bind a synthetic Signal ActorRef, or call an integration start function. Runtime must not import concrete Lark or agentmemory implementations.

## Effect composition

External Layers supply storage, history, Models, SystemOneClient, memory backend and external agents. Internal Layers construct ContextRegistry, domain settings, internal reasoning, TaskPreparation, Goal services and Context reactions. ConfigProvider is installed over the complete acquisition graph, including adapters and integrations.

Integration Layers acquire dependencies and return runtime-managed installation capabilities. Multiple modules must coexist without overwriting a shared service tag. The runtime uses one graph/memoization scope and passes acquired services into Actors without acquiring duplicate clients or copying Scope. Integration metadata is not a second configuration-driven dependency injection system.

The local composition is intentionally small:

```ts
const RuntimeLive = AsterRuntime.layer({
  integrations: [MemoryIntegration.layer, LarkIntegration.layer],
}).pipe(
  Layer.provide(
    Layer.mergeAll(
      FileContextStore.layer,
      FileGoalHistory.layer,
      Models.configured,
      SystemOneClientLive.layer,
      AgentMemoryBackend.layer,
      ExternalAgentsLive.layer,
    ),
  ),
  Layer.provide(LocalConfig.layer({ configPath, envPath, projectRoot })),
);
```

| External capability                                | Local implementation      | Internal consumer                                                      |
| -------------------------------------------------- | ------------------------- | ---------------------------------------------------------------------- |
| ConfigProvider, ConfigLocation, ProcessEnvironment | LocalConfig.layer         | Module Config declarations and adapters                                |
| ContextStore                                       | FileContextStore.layer    | ContextRegistry.layer                                                  |
| GoalHistoryStore                                   | FileGoalHistory.layer     | Goal runtime and application history API                               |
| Models                                             | Models.configured         | Internal Agent, Goal reasoner, Lark summarizer                         |
| SystemOneClient                                    | SystemOneClientLive.layer | Signal/Goal policies and Lark summary gate                             |
| MemoryRuntime                                      | AgentMemoryBackend.layer  | MemoryIntegration supplies MemoryRecall and ContextCaptureSink to core |
| ExternalAgents                                     | ExternalAgentsLive.layer  | Task preparation and Delegation Actors                                 |

`ConfigProvider` is an Effect reference service with a default; local explicitly overrides it for the entire Layer acquisition graph. Other unsatisfied capabilities remain in the returned Layer's input type. A host without MemoryIntegration can supply MemoryRecall and ContextCaptureSink directly. Local never supplies the internal registry, command endpoint, reasoner or TaskPreparation separately.

Startup phases: acquire and validate dependencies; prepare roots and command endpoints; subscribe consumers; activate integrations; asynchronously await required initial readiness; initialize Goals once. Lark readiness means initial retrieval catch-up, preserving current behavior; it does not mean all summaries have completed. Without IM it completes immediately. Sources continue to be selected in code.

Shutdown phases: close admission/stop sources; stop startup coordination and reaction producers; finish or durably retain pending work according to existing domain contracts; stop Actors; release adapter resources. Store lock and signal handlers outlive all finalizers. Interrupted startup must release every resource already acquired.

Readiness is completed with the startup Exit, not only its typed error channel. A defect fails waiting callers, and shutdown interrupts pending readiness even if the startup Fiber never began. Cleanup phases compose with Effect finalizers: a failing integration stop does not skip remaining integrations, Actor termination or capture draining. Multiple cleanup defects remain observable to the host.

Internal domain operations also preserve the caller's Effect execution. Goal Signal reconciliation/edits/deactivation return Effects rather than starting independent Promise runtimes. Mailbox handlers use `pipeToSelf` for remote acknowledgements; per-Goal Signal operations remain serialized without preventing the Goal mailbox from accepting End or UserMessage. The shared Run workflow lives in `core/tasks`, below both Goals and Signals.

Goal reasoning, compaction and tool/transcript callbacks return Effects. Evaluation performs history reads and generation-tagged mailbox acknowledgements directly. Only the Agent adapter bridges SDK Promise callbacks, preserving the calling Context and cancelling pending callbacks before Agent cleanup waits for idle. Goal End signals cancellation through a Deferred; restart and shutdown use the Behavior scope. Expected reasoning failures remain tagged, while defects enter supervision. MemoryRecall, Signal extraction and Context description also expose Effect ports. MemoryIntegration owns the Promise-to-Effect adapter and forwards cancellation to the backend HTTP request, including expansion fallbacks. Goal and InternalAgent share the SDK callback lifecycle helper; no additional host Layer is needed.

## Configuration

Use the installed Effect version, currently `4.0.0-rc.117`, and the [v4 configuration guide](https://effect.website/docs/v4/configuration). Config describes decoding, ConfigProvider supplies values, and module Settings Services retain the resolved result. Do not retain a globally parsed application configuration as a service locator.

Precedence: explicit configuration overrides > `ASTER_*` process values > `.env` application values > YAML > module defaults. Provider fallback handles absent paths only; validation errors must not fall back. Structural overlays merge YAML/override record keys, while environment trie keys never rename or truncate those records. Environment values override declared dynamic entries; new Signal/Goal keys must be declared in YAML or structured overrides. Numeric model indices can override array entries. Missing optional `.env` is allowed, but read errors and malformed YAML are not hidden. `preserveEmptyStrings: true` lets field schemas distinguish invalid explicit emptiness from absence.

Keep current YAML keys, including `contexts./lark.children./im`, models, signals and goals. Read path segments with `Config.schema(schema, path)` or supported nesting APIs. In rc.117 `Config.nested` accepts one string, while `Config.schema` accepts a path array. Relative paths resolve beside the config file, not the process working directory.

Each module owns defaults and validation. A settings acquisition may consult another service for cross-module checks (for example, a selected model alias). Validate required settings before starting producers. Disabled capabilities must not demand their unused credentials. Keep configuration and credentials out of public Context snapshots.

Use one credential resolver for exact `${ENV_VAR}` references and memory `apiKeyEnv`. Resolve against a captured process/.env source and retain secrets as Redacted until adapter invocation. Never interpolate arbitrary task text. Dotenv is parsed once without general interpolation. No process.loadEnvFile or ad-hoc process.env lookup in domain/model configuration. Child-process environments are explicit adapter inputs and retain credential filtering.

## Application interface

Runtime exposes Context reads/change notifications, normalized query invalidations, Goal list/history/sendMessage/end, approvals list/respond, and Schema-backed runtime diagnostics. Browser-safe RPC contracts live in `packages/api-contracts`. HTTP validates transport inputs and maps application errors to responses; it does not inspect domain paths to identify entities, compute history pages or send Actor Commands.

## Refactoring and acceptance

1. Introduce module-owned Config declarations, source/provider loading and credential resolution, preserving YAML behavior and standalone recall.
2. Introduce domain-owned service Layers, Context reactions, Signal policy and application queries; remove stale constructor dependencies.
3. Introduce runtime-managed integration installation, readiness and lifecycle; reduce local to composition and transport.
4. Move HTTP and CLI onto application/adapter capabilities and update package documentation.
5. Verify with isolated providers/adapters, lifecycle and domain boundary tests, build, type checks, lint, formatting and existing backend tests.

Acceptance is about change ownership: changing Signal rules touches core; changing IM readiness touches Lark; adding an executor touches its adapter and registration; adding a client reuses application use cases. No live external integrations are necessary for acceptance tests.

External execution follows the same boundary: core's `ExternalAgent` port exposes Effect operations and tagged `ExternalAgentError`; the infrastructure Layer owns adapter release. Fiber cancellation reaches transport requests without treating it as proof of external cancellation or automatically retrying submission. Delegation persists terminal results before notifying Run and replays them on restart or parent reattachment, even when that executor is no longer configured. The host still supplies `ExternalAgentsLive.layer`; no new internal Layer needs application assembly.
