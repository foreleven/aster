# Agent architecture refactor

Status: proposed; implementation starts only after the design is confirmed.

This refactor removes the standalone `Core agent/runtime` package and makes Agent construction visible where the business Agent is defined. It also replaces the custom model facade with pi-ai's native model/provider/auth capabilities and reduces the durable adapter to a thin pi-durable wrapper.

## Decisions from the design interview

- `Core agent/runtime` is removed.
- Core owns the pi-agent-core/pi-durable runtime adapter because moving it to Infra would add a heavy second boundary without enough value.
- Infra owns pi-ai model configuration, provider registration, authentication and credential resolution.
- Core owns Agent Assemblies for Goal, Task and Screening.
- Integration-owned Agents remain in their integration context. Lark summary stays under `integrations/lark`.
- Each Assembly is self-contained and exposes an Effect-style `make` operation. There is no global AgentFactory.
- Shared tool infrastructure lives under `core/agent/shared/`; Goal and Task business tools live beside their Assemblies.
- Instructions remain Markdown assets, loaded through one shared Core loader.
- `extensions.ts` describes Assembly extensions and hooks. It does not duplicate business tools for ordinary and durable execution.
- Goal and internal Task use durable sessions. Screening and Lark summary use ordinary runs.
- The durable layer delegates checkpointing, replay and recovery to pi-durable. Aster removes its adapter-owned failure ledger and duplicate submission state machine.
- Session storage is injected through `openStorage(owner)`. Infra/Local owns JSONL or SQLite, paths and leases.
- The existing model configuration may break. The application-level model alias remains, while provider/model/auth fields move to pi-ai-native registration.
- The first implementation does not add workspace path isolation. A later pi-durable extension may enforce it.

## Target layout

```text
packages/core/src/agent/
  runtime/
    ordinary.ts       # one-shot pi-agent-core execution
    session.ts        # thin pi-durable session wrapper
    storage.ts        # injected openStorage(owner) capability
    callbacks.ts      # Effect callback bridge and scope ownership
    errors.ts         # runtime-level typed failures
    instructions.ts   # Markdown asset loader
  shared/
    define-tool.ts    # Effect Schema to pi-native AgentTool adapter
    results.ts        # common tool result and structured details helpers
    invocation.ts     # current invocation capability
    types.ts          # shared Agent Assembly/runtime types
  goal/
    agent.ts
    instructions.md
    extensions.ts
    input.ts
    tools/
      catalogue.ts
      state.ts
      tasks.ts
      messages.ts
      signals.ts
  task/
    agent.ts
    instructions.md
    extensions.ts
    tools/
      catalogue.ts
      contexts.ts
      memory.ts
  screening/
    agent.ts
    instructions.md
    extensions.ts
    tools/
      submit-result.ts

packages/infra/src/models/
  config.ts          # native pi-ai-facing configuration schema
  layer.ts           # provider catalog, auth and alias resolution
  credentials.ts     # captured Redacted credential resolution

packages/integrations/src/lark/im/summary/
  agent.ts
  instructions.md
  extensions.ts
  tools.ts
```

The existing `packages/core/src/tools/goal` and `packages/core/src/tools/task` directories disappear. Their domain operations move with the Agent Assembly that owns them. `agent/shared` contains only reusable adaptation mechanics, never Goal or Task policy.

## Runtime contract

The runtime is deliberately small and explicit. It is implemented in Core around the native pi packages and receives the native pi-ai `Models` capability from Infra.

```ts
type AgentAssembly = {
  readonly model: string; // application alias
  readonly instructions: string;
  readonly tools: readonly AgentTool[];
  readonly extensions?: readonly AgentExtension[];
};

const runOrdinary = (assembly, input) => Effect.Effect<AgentRunResult, AgentRuntimeError>;

const openSession = (assembly, owner, use) => Effect.Effect<A, AgentRuntimeError | E, R>;
```

`runOrdinary` creates a fresh pi-agent-core Agent for Screening or an integration summary. `openSession` opens one pi-durable conversation for Goal or Task, installs the selected Assembly configuration, and delegates persistence/replay/recovery to pi-durable. Both operations share native model resolution, tool declarations and callback ownership.

There is no `AgentRuntime` or `AgentSession` Context service in the target design. Assemblies call the runtime operations from their own `make` functions and provide the business dependencies they need. The runtime still owns the native Promise boundary, Effect interruption, scope cleanup and pi lifecycle.

## Assembly construction

Each Agent owns its construction:

```ts
export class GoalAgent extends Context.Service<GoalAgent, GoalAgentService>()(
  "agent/goal/GoalAgent",
) {
  static readonly make = Effect.fn("GoalAgent.make")(function* () {
    const runtime = yield* AgentRuntime;
    const instructions = yield* loadInstructions("goal");
    return GoalAgent.of({
      converse: /* runtime.openSession with Goal tools/extensions */,
      screen: /* runtime.runOrdinary with screening Assembly */,
    });
  });
}
```

The exact service shape can differ for Task and Screening. The invariant is that each `make` keeps its own prompt, extensions, tools, input shaping, result validation and runtime mode together.

## Models and configuration

Infra stops exporting Aster's `ModelConfig`, `ResolvedModel`, custom stream function and API-key getter. It uses pi-ai's native `Models`, provider factories/catalogs and auth resolution.

The new configuration keeps an application alias but adopts native provider fields. A representative shape is:

```yaml
config:
  models:
    - name: reasoning
      provider: anthropic
      model: claude-sonnet
      api: anthropic-messages
      auth:
        apiKey: ${ANTHROPIC_API_KEY}
```

Provider-specific options stay in Infra and are passed to the native provider registration. Core receives only the alias and native Models capability. Missing credentials, unavailable models and provider errors remain typed at the infrastructure/runtime boundary.

## Durable semantics

The current Agent adapter contains more durability policy than the Assembly needs. The rewrite removes its extra failure evidence, terminal failure ledger, submission reconciliation and duplicated native state. `AgentSession` delegates those concerns to pi-durable.

Goal and Task continue to persist their own accepted business inputs, delivery receipts, outcomes and actor state. Removing the Agent-level ledger does not remove domain receipts. The refactor must explicitly test the new boundary for duplicate input handling, interrupted sessions, native replay, cancellation and process restart.

Storage remains injected:

```ts
type AgentStorage = {
  readonly openStorage: (owner: string, context: NativeContext) => Promise<Storage>;
};
```

Local Infra chooses JSONL or SQLite, resolves the durable root and owns the process lease. Core never imports Node filesystem APIs, constructs local directories or interprets local workspace paths.

## Workspace behavior

The first workspace implementation continues to provide an infrastructure-created `ExecutionEnv` with an opaque workspace identity as `cwd`. Read/write/edit tools are available only to Assemblies that request a workspace. Root isolation, traversal checks and symlink enforcement are deferred to a dedicated pi-durable extension.

## Migration sequence

The migration is intentionally one large breaking refactor, with compile/test checkpoints inside the change:

1. Add `core/agent/runtime`, `core/agent/shared`, the Markdown loader and Core Agent Assembly contracts.
2. Move the ordinary pi-agent-core adapter and the thin pi-durable session adapter into Core. Delete the custom failure/reconciliation machinery while preserving domain-owned receipts.
3. Move `goalTools` and `taskTools` into their Agent Assembly directories and update imports to `agent/shared/define-tool`.
4. Split `GoalAgent` into its Assembly `make`; move screening construction into `agent/screening` and Task execution construction into `agent/task`.
5. Move Lark's summarizer construction to its integration-owned Assembly while depending on Core runtime/shared contracts.
6. Replace `Core agent/runtime` Models with Infra's native pi-ai Models/provider/auth layer. Update the model configuration schema and all alias consumers.
7. Replace runtime Layer assembly and test fixtures. Remove `AgentRuntime`, `AgentSession`, `AgentConversations` and old package imports in one pass.
8. Delete `packages/agent`, its package entry, dependencies and README. Rewrite `docs/agent-design.md`, `packages/agent/README.md` references, runtime docs and package instructions.
9. Update workspace Agent integration to use the new session runtime and opaque workspace identity.
10. Run build, typecheck, focused runtime/assembly tests, integration tests and the full suite; resolve behavior changes before declaring the refactor complete.

## Verification and acceptance

- No package imports `Core agent/runtime` after the refactor.
- Core contains no Node filesystem access for Agent runtime or workspace creation.
- No custom `ResolvedModel`, stream or API-key facade remains.
- Every Agent Assembly has colocated instructions, extensions, tools, input shaping and result validation.
- Task prompts use readable text sections and never serialize Context metadata as JSON strings.
- Goal and Task durable sessions recover through pi-durable native state without an Aster Agent failure ledger.
- Screening and Lark summary use isolated ordinary runs.
- Model tests exercise native provider registration, auth resolution, alias lookup and unavailable-model errors.
- Assembly tests cover tool schemas, Effect dependencies, instructions loading, extension selection and result contracts.
- Runtime tests cover interruption, scope cleanup, native replay and storage reopening.
