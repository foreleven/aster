# Compose the Aster runtime and configuration with Effect Layers

Status: accepted. Implementation is tracked in [runtime design](../runtime-design.md). Refines [0038](0038-register-root-actors-in-code.md); root implementations remain registered in code.

`apps/local` selects implementations, configuration sources and HTTP/CLI entry points. `AsterRuntime` owns installation of integrations, the shared domain service graph, Actor startup, Context subscriptions, first Goal initialization and shutdown. Integrations own their configuration, children, readiness semantics and resource cleanup. The generic ActorSystem stays domain-neutral.

Use Effect 4 `Context.Service`, `Layer` and `Scope`. Infrastructure supplies Context storage, Goal history, models, System One transport, memory and external executors. Domain Layers own internal reasoning, Task preparation, Signal eligibility, Goal operations and Context reactions. The runtime reuses acquired service instances when injecting Actors; it never copies an application Scope into an Actor environment.

Integrations are acquired through Layers and activated by the runtime after consumers are ready. Layer merge order is not startup order. Runtime startup phases are explicit Effects. Lark owns the interpretation of IM configuration and first-catch-up readiness; neither local nor core inspect `/lark/im.state.ready`. Readiness must be level-triggered or subscribed before activation, so a fast producer cannot lose its notification. Readiness waiting must not prevent HTTP from exposing startup progress.

Configuration separates source, declaration and resolved settings. Local installs a ConfigProvider for explicit overrides, `ASTER_*` process environment, `.env`, then parsed YAML. Module Config declarations own defaults and validation; Settings Layers resolve once per runtime. Missing values can default, invalid values must fail. Preserve explicit empty strings for validation. Preserve the current YAML shape and config-relative paths. Credentials use Redacted and a shared explicit reference resolver; `${ENV_VAR}` in YAML is not expanded by Effect automatically. Arbitrary prompts are never interpolated. Loading `.env` must not mutate process.env.

HTTP consumes application use cases, not Actor Commands or persistence implementations. Domain operations retain Effect errors and cancellation; Promise conversion stays at the transport/SDK boundary. Shutdown stops admission and producers before consumers, then releases underlying services. Partial acquisition and cancellation use the same scoped finalizers.

Validation includes configuration precedence and errors, single acquisition of shared services, integration activation/readiness, multiple integrations, cancellation during startup, teardown ordering, Context reaction eligibility/deduplication, HTTP contracts and the existing workspace tests. Tests inject providers and adapters; no live Lark, model or external task submission is required.
