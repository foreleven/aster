# Configuration loading and consumption

Status: accepted.

Configuration follows one direction: the host captures launch inputs, infrastructure loads sources, module Layers resolve typed Settings, and business code consumes those Settings. This refines [ADR 0040](adr/0040-compose-aster-runtime-with-effect-layers.md).

## Ownership

- `apps/local` selects configuration paths, captures the process environment and home directory once, and supplies platform implementations before source acquisition.
- `packages/infra/config` loads YAML and dotenv through Effect `FileSystem` and `Path`. It supplies `ConfigProvider`, source locations and the captured child-process environment. The source loader does not know module schemas. The configured model adapter in this directory owns model input decoding and credential resolution.
- Each module owns its configuration paths, Schema, defaults, cross-field validation, reference resolution and normalized Settings. Core does not import concrete adapters or integration settings.
- Business services and Actors consume resolved Settings. They do not read live environment variables or reload configuration during execution.

The filesystem provider remains infrastructure even when its implementation has no Node dependencies. There is no global AppConfig, settings registry or generic settings factory.

## Sources and precedence

`LocalConfig.layer` accepts explicit configuration and dotenv paths, project root, home directory, environment snapshot and optional overrides. It requires Effect `FileSystem` and `Path`. No default reads from `process.env` occur in infrastructure.

Application precedence is explicit overrides, `ASTER_*` process variables, `ASTER_*` dotenv variables, YAML, then module defaults. Credential reference precedence is process variables before dotenv variables. Loading dotenv never mutates the host environment.

Read and decode each source once per acquired source Layer. YAML must exist and contain a mapping. A missing default dotenv file is allowed; permission and other I/O failures are errors. Preserve explicit empty strings for validation.

Preserve YAML and override structural names, including camelCase, slash-prefixed children, slugs and arrays. Environment overrides apply to leaves of that structure. Overriding one array element must not truncate remaining YAML elements. Do not replace structural composition with plain `ConfigProvider.orElse` without proving these semantics.

Dotenv decoding produces one raw dictionary shared by the provider and child environment capture. Native Effect dotenv constructors return only a provider; use a portable parser where the raw dictionary is necessary, rather than duplicating parsing or implementing a dotenv grammar.

`ConfigLocation` belongs to infrastructure and contains resolved source and launch directories. `ProcessEnvironment` is a narrow core contract shared by infrastructure and integrations. It uses portable types and Redacted values. Native adapters unwrap values only at subprocess boundaries and filter private keys without rereading sources.

## Module definitions

Use `*Config` for external input and `*Settings` for resolved internal values. A module's `settings.ts` owns its Config declarations and Settings Layer; split larger implementations only when responsibilities warrant it.

Use `Config.schema` and Schema constraints for shapes, nonblank strings, ranges, enums and defaults. Cross-field rules use Schema checks or Effects with typed errors. Do not throw expected validation failures or wrap synchronous validators in generic catch-all helpers. Missing values may default; invalid supplied values must fail.

An optional capability can be absent. Once its section exists, required nested fields must be validated: optional enclosing schemas must not hide invalid or incomplete input. Preserve existing enablement semantics.

Resolved fields with defaults are required in the internal type. Paths are resolved using Effect `Path` and captured location metadata. Avoid repeated normalization in consumers.

Context directory metadata is independent of Goal settings. Its reader collects only explicit Context paths and descriptions, never integration credentials or module configuration.

## Acquisition and consumption

Settings Layers resolve once during construction of the owning runtime, integration or adapter graph, before that capability starts work. Reuse acquired Settings in Actor environments. Layer merge order does not define startup order.

Shared language Settings default to `zh-CN`. Goals, Signals and Reactions own their settings. Integrations resolve their own settings. Infrastructure resolves model configuration and credentials at acquisition; the agent registry consumes resolved entries; unknown model lookups remain runtime errors. System One resolves its URL and optional credential during client acquisition. Memory resolves backend parameters, paths and credentials before starting its daemon.

After all business consumers use Settings, remove ambient ConfigProvider injection from Actor environments. Configuration sources remain available to acquiring Layers and CLI configuration readers. This design does not introduce hot reload.

## Credentials

Only fields explicitly supporting references expand `${VAR}`. API keys support exact references. System One owns embedded URL reference expansion and query encoding. Prompts and descriptions remain literal. Do not recursively interpolate the configuration tree.

Use Redacted for resolved credentials and environment snapshots. Error messages and logs must not expose rejected credential values. The credential resolver belongs to `infra/config/credentials.ts`. Infrastructure resolves model configuration through `ConfiguredModels.layer` and supplies Redacted credentials to `Models.layer`. Agent exposes model execution and registration contracts without ConfigProvider lookup or application reference syntax. Do not introduce agent-to-infrastructure dependencies or forward this helper through core.

## Validation

Use injected ConfigProviders and FileSystem/Path Layers. Verify source snapshotting, precedence, structural keys and arrays, missing versus invalid sections, paths, credential masking and subprocess filtering. Verify runtime and integration acquisition with fake transports. Never invoke real models, external agents or messaging to validate configuration changes.

## File responsibilities

| Location                           | Responsibility                                                      |
| ---------------------------------- | ------------------------------------------------------------------- |
| `apps/local/src/configuration.ts`  | Capture launch inputs and provide platform/source/storage Layers    |
| `apps/local/src/settings.ts`       | Local HTTP configuration declaration                                |
| `infra/config/provider.ts`         | Public source Layer and provider assembly                           |
| `infra/config/sources.ts`          | File reads and YAML/dotenv decoding                                 |
| `infra/config/overlay.ts`          | Private structural overlay semantics                                |
| `infra/config/location.ts`         | Source-location contract                                            |
| `core/process/environment.ts`      | Portable, redacted subprocess environment contract                  |
| `core/config/language.ts`          | Shared language Settings and prompt instruction                     |
| `core/config/context-directory.ts` | Explicit Context metadata Settings                                  |
| `core/goals/settings.ts`           | Goal definitions and normalized reasoning budgets                   |
| `core/signals/settings.ts`         | Configured Signal definitions                                       |
| `core/reactions/settings.ts`       | Matching and delivery concurrency                                   |
| `infra/config/models.ts`           | Model configuration loading and credential resolution               |
| `infra/config/credentials.ts`      | Private credential reference resolution for infrastructure adapters |
| `infra/storage/settings.ts`        | Shared resolved storage paths                                       |
| `infra/agentmemory/settings.ts`    | Memory input validation, paths and native environment preparation   |
| `infra/system-one/settings.ts`     | Optional client configuration and credential/URL resolution         |
| `infra/executor-settings.ts`       | External executor prompt declaration                                |
| `integrations/*/settings.ts`       | Module-owned enablement, transport and business settings            |

Shared Settings Layers use the same Layer identity within a composition graph so Effect memoization reuses their acquisition. Adapters that need shared language settings can provide that Layer when constructing their capability. They must not resolve it inside business invocations or Actor initialization.
