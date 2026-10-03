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
      ConfiguredDurableInfrastructure.layer,
      FileGoalHistory.layer,
      Models.configured,
      SystemOneClientLive.layer,
      AgentMemoryBackend.layer,
    ),
  ),
  Layer.provide(LocalConfig.layer({ configPath, envPath, projectRoot })),
);
```

| External capability                                | Local implementation                  | Internal consumer                                                      |
| -------------------------------------------------- | ------------------------------------- | ---------------------------------------------------------------------- |
| ConfigProvider, ConfigLocation, ProcessEnvironment | LocalConfig.layer                     | Module Config declarations and adapters                                |
| DurableContext                                     | ConfiguredDurableInfrastructure.layer | ContextRegistry.layer                                                  |
| GoalHistoryStore                                   | FileGoalHistory.layer                 | Goal runtime and application history API                               |
| Models                                             | Models.configured                     | Internal Agent, Goal reasoner, Lark summarizer                         |
| SystemOneClient                                    | SystemOneClientLive.layer             | Signal/Goal policies and Lark summary gate                             |
| MemoryRuntime                                      | AgentMemoryBackend.layer              | MemoryIntegration supplies MemoryRecall and ContextCaptureSink to core |
| ExternalAgents                                     | ConfiguredDurableInfrastructure.layer | Task preparation and Delegation Actors                                 |

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

## Shared Pi infrastructure owner

A host selecting `PiDurableBackend.layer` receives both DurableContext and the Pi ExternalAgent from one storage lease and one scoped Harness. The core graph retains the same ports and Actor ownership. PiDurableContext attaches through the runtime's infrastructure-only Session bridge; it does not create or close another writer. Shared short operations serialize with owner recovery, while observing task completion does not hold that permit. Recovery joins the retired Harness before reopening, keeps the storage lease, validates persisted mappings, and reuses business execution handles. Interrupted unsafe tool outcomes remain unknown. The local host selects this combined owner when both Pi storage and execution are configured. Otherwise it uses model-free Pi Context storage or Local according to the configured routes.

The process captures LocalConfig once before resolving and locking `config.durable.root`. That lock outlives runtime resources and covers Context storage, Goal history, screening logs and private routing authority. Longest segment-prefix routes select one authoritative writer per Context; other copies are startup validation evidence. Activation refuses missing selected data, newer shadow data or same-revision divergence. A configured route change requires the offline `storage migrate` command, which takes the same root/Pi locks, validates the entire plan, copies full snapshots at their existing revisions, reopens storage for verification and publishes authority last. Reverse migration carries post-activation writes back to Local. The command opens no Harness or models, and does not relocate execution sessions.

## Local Pi process ownership

Every production Pi directory opener uses `PiStorageLease` from the Agent adapter, including ownerless Goal/Personal runs and the shared Context/execution backend. The host holds a SQLite exclusive transaction on a permanent lock file in the canonical directory. This local-filesystem kernel lock survives symlink aliases and competing recovery processes; a paused live owner cannot be evicted, while process death releases the lock. No stale PID file is deleted to acquire Pi ownership. This is not a distributed lease protocol.

The lease outlives all Pi writers and callbacks in its Scope. SDK close failure quarantines ownership until process exit; accepted mutations and cancellation cleanup drain before a normal release. Session/Harness reopen retains the same lease and rejects a retired or quarantined owner. Logical owner identity is still validated against Pi documents; acquiring an OS lock does not authorize adopting another owner's conversation. Runtime inspection includes redacted storage owner identities, lease identities and held/quarantined status. Lock descriptors on disk are diagnostics only, and lock database files are never removed by acquisition or release.
