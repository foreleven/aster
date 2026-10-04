# Separate business integrations from infrastructure

Status: accepted and implemented.

Reserve `packages/integrations` for connections to external business systems, such as Lark and mail. Move the existing Lark implementation into its `lark` source folder. Pi and System One provide internal execution, persistence, and decision infrastructure; they do not belong in this package merely because they use external SDKs or services. Memory is likewise an internal capability, currently implemented with agentmemory.

Core owns the domain-facing contracts and consumes supplied capabilities without importing their concrete implementations. This separation replaces the broad infrastructure-and-re-exports responsibility currently assigned to `@aster/integrations`; this decision does not authorize moving domain policy into infrastructure.

Core also owns the backend-independent Memory Actor and capture workflow, including the durable pending queue, deduplication, retries, and recovery. The agentmemory adapter owns its protocol, process and connection lifecycle, configuration translation, and backend-specific provenance mapping. Replacing agentmemory must not require reimplementing Aster's capture workflow. Moving these responsibilities must preserve durable capture handoff and drain semantics, with `AsterRuntime` retaining startup and shutdown ownership.

Use one `@aster/infra` workspace package with implementation folders rather than a separate package for each backend. Its source folders include `pi/`, `system-one/`, `agentmemory/`, and `storage/`. Move the concrete Memory implementation into `agentmemory/` and remove the former `@aster/memory` package after its domain workflow moves to core. Move Lark into `packages/integrations/src/lark/` and remove `@aster/lark-integration`; generic mail remains in `integrations/src/mail/`. Update consumers to the new package boundaries rather than retaining forwarding packages.

Keep `@aster/agent` as the existing lower-level Agent SDK wrapper, independent of core. The Pi adapters in infra implement core's capabilities using that wrapper. This preserves the dependency direction while avoiding per-backend package configuration and re-export maintenance. Codex and Doubao execution adapters also move to `infra/src/codex/` and `infra/src/doubao/`: like Pi execution, they implement core's `ExternalAgent` contract. Classify adapters by their Aster capability rather than by whether their provider is an external product.

This refines the package boundary described in [core design](../core-design.md) while preserving the runtime lifecycle ownership in [ADR 0040](0040-compose-aster-runtime-with-effect-layers.md). Package documentation tracks the implemented ownership.

## Migration plan

The complete plan was confirmed for implementation:

- Move the existing configuration-source loaders and executor environment helpers to `infra/src/config/` and `infra/src/process/`. Local continues selecting source paths and implementations; modules retain their own typed settings. Source capture and provider precedence remain unchanged.
- Keep infra and integrations as sibling packages without imports or re-exports between them. Both consume core contracts and may use the existing Actor/Agent packages. Local imports their public capabilities separately. Lark receives decision/model services through Effect dependencies rather than importing concrete infra implementations.
- Make Memory orchestration part of core's runtime assembly, with an injected backend contract. Local supplies the agentmemory backend and selects Lark/mail integrations; it does not assemble Memory internals. Preserve consumer readiness before producers and the source-stop, capture-drain, Actor-stop, resource-release ownership constraints during normal shutdown, startup failure, and interruption.
- Move adapter tests with their implementations and place backend-independent Memory workflow tests in core. Retain local cross-package tests. Update package manifests, exports, TypeScript configuration, build/test ordering, the lockfile, architecture instructions, and maintained documentation. Remove obsolete package references after all consumers migrate.
- Keep YAML keys, Context paths, persistent formats, storage locations, external executor identifiers, and application API behavior compatible. Do not migrate runtime data or change credentials. Existing Pi storage/execution sharing and recovery ownership remain intact.
- Verify workspace build, focused tests during iteration, the full backend test suite, `pnpm check`, and Effect diagnostics for affected packages. Cover Memory persistence-before-delivery, deduplication, restart recovery, failure propagation, capture drain, and runtime lifecycle using fake services and temporary storage. Do not invoke live integrations or models.

## Verification

- `pnpm build` and `pnpm test` pass; the backend suite contains 435 passing tests across Agent, Actor, core, infra, integrations, and local.
- `pnpm check` passes lint, formatting, and all workspace type checks. ESLint enforces the core/infra/integrations dependency boundaries and keeps Agent independent of core.
- Effect diagnostics for core, infra, integrations, and local report zero errors and warnings against the installed `4.0.0-rc.117`. The locally available reference snapshot was rc.118, so matching upstream tests/examples were read from the rc.117 release commit `14a3f140095fdebbff9162944fe7d4ea83e054e6` without modifying the reference directory.
- Core Memory tests cover persisted admission, duplicate suppression, typed failure recovery, Clock-controlled retries, private-state projection, interruption, and defects reaching Actor supervision. Infra tests verify that interrupted observers leave admitted agentmemory operations available to drain. Local runtime tests verify source shutdown, observer interruption, backend drain, and resource release ordering, including failed and interrupted startup.
- Third-party lockfile resolutions are unchanged. No runtime data, credentials, live models, or external business services were used or modified. Browser source and public API behavior are unchanged; the web production build passes as part of the workspace build.
